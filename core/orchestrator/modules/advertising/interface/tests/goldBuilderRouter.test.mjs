import test from 'node:test';
import assert from 'node:assert/strict';

process.env.PGUSER = process.env.PGUSER || 'test';
process.env.PGPASSWORD = process.env.PGPASSWORD || 'test';
process.env.PGDATABASE = process.env.PGDATABASE || 'test';

const { goldBuilderTestkit } = await import('../server/goldBuilderRouter.mjs');

const {
  materializeGoldDefinition,
  _testInsertRefreshRun,
  _testPublishDefinition,
  _testUpsertDefinitionDraft
} = goldBuilderTestkit;

const TEST_CONFIG = {
  contracts_schema: 'ao_system',
  contracts_table: 'table_data_contract_versions',
  gold_schema: 'ao_system',
  gold_definitions_table: 'gold_definitions_store',
  gold_definition_versions_table: 'gold_definition_versions_store',
  gold_external_sources_table: 'gold_external_sources_store',
  gold_refresh_runs_table: 'gold_refresh_runs_store'
};

function normalizeSql(sql = '') {
  return String(sql).replace(/\s+/g, ' ').trim();
}

function createGoldClient() {
  const state = {
    definitionId: 100,
    definitions: new Map(),
    versions: [],
    refreshRuns: [],
    contracts: [],
    createdTables: new Set()
  };

  return {
    state,
    async query(sql, params = []) {
      const text = normalizeSql(sql);

      if (text.startsWith('SELECT setting_key, setting_value FROM "ao_system"."table_settings_store"')) {
        return { rows: [], rowCount: 0 };
      }
      if (text.startsWith('INSERT INTO "ao_system"."table_settings_store"')) {
        return { rows: [], rowCount: 1 };
      }
      if (
        /^INSERT INTO "ao_system"\."(?:table_templates_store|api_configs_store|parser_configs_store|write_configs_store|node_registry_store|table_server_writes_store|workflow_desks_store)"/.test(
          text
        )
      ) {
        return { rows: [], rowCount: 1 };
      }
      if (text.startsWith('WITH ranked AS')) {
        return { rows: [], rowCount: 0 };
      }
      if (text.startsWith('SELECT column_name AS name, data_type AS type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2')) {
        const schema = String(params?.[0] || '');
        const table = String(params?.[1] || '');
        if (schema === 'ao_system' && table === 'table_data_contract_versions') {
          const rows = [
            ['schema_name', 'text'],
            ['table_name', 'text'],
            ['contract_name', 'text'],
            ['version', 'integer'],
            ['lifecycle_state', 'text'],
            ['deleted_at', 'timestamp with time zone'],
            ['description', 'text'],
            ['columns', 'jsonb'],
            ['change_reason', 'text'],
            ['changed_by', 'text'],
            ['created_at', 'timestamp with time zone']
          ].map(([name, type]) => ({ name, type }));
          return { rows, rowCount: rows.length };
        }
        if (schema === 'ao_system' && table === 'table_settings_store') {
          const rows = [
            ['setting_key', 'text'],
            ['setting_value', 'jsonb'],
            ['scope', 'text'],
            ['description', 'text'],
            ['is_active', 'boolean'],
            ['updated_at', 'timestamp with time zone'],
            ['updated_by', 'text']
          ].map(([name, type]) => ({ name, type }));
          return { rows, rowCount: rows.length };
        }
        return { rows: [], rowCount: 0 };
      }
      if (
        text.startsWith('CREATE SCHEMA IF NOT EXISTS') ||
        text.startsWith('CREATE TABLE IF NOT EXISTS') ||
        text.startsWith('CREATE TABLE "') ||
        text.startsWith('CREATE SEQUENCE IF NOT EXISTS') ||
        text.startsWith('CREATE INDEX IF NOT EXISTS') ||
        text.startsWith('CREATE UNIQUE INDEX IF NOT EXISTS') ||
        text.startsWith('ALTER SEQUENCE') ||
        text.startsWith('ALTER TABLE') ||
        text.startsWith('COMMENT ON TABLE') ||
        text.startsWith('DO $$') ||
        text.startsWith('SELECT setval(')
      ) {
        return { rows: [], rowCount: 0 };
      }
      if (
        text.startsWith('UPDATE "ao_system"."table_templates_store" SET data_level = COALESCE') ||
        text.startsWith('WITH dup AS (') ||
        text.startsWith('UPDATE "ao_system"."table_data_contract_versions" SET lifecycle_state = \'inactive\'')
      ) {
        return { rows: [], rowCount: 0 };
      }
      if (
        /^UPDATE "ao_system"\."(?:table_templates_store|api_configs_store|parser_configs_store|write_configs_store|node_registry_store|table_server_writes_store|workflow_desks_store)"/.test(
          text
        )
      ) {
        return { rows: [], rowCount: 0 };
      }
      if (/^UPDATE "ao_system"\."[a-z_]+_store" SET id = nextval\(/.test(text)) {
        return { rows: [], rowCount: 0 };
      }
      if (text.startsWith('SELECT id FROM "ao_system"."gold_definitions_store" WHERE lower(gold_code) = lower($1)')) {
        const code = String(params?.[0] || '').toLowerCase();
        const excludeId = Number(params?.[1] || 0);
        const conflict = [...state.definitions.values()].find((item) => item.gold_code.toLowerCase() === code && item.id !== excludeId);
        return { rows: conflict ? [{ id: conflict.id }] : [], rowCount: conflict ? 1 : 0 };
      }
      if (text.startsWith('INSERT INTO "ao_system"."gold_definitions_store"')) {
        const row = {
          id: state.definitionId++,
          gold_code: params[0],
          gold_name: params[1],
          description: params[2],
          owner_name: params[3],
          tags: JSON.parse(params[4]),
          status: 'draft',
          published: false,
          active_version: 0,
          definition_json: JSON.parse(params[5]),
          updated_at: new Date().toISOString(),
          updated_by: params[6]
        };
        state.definitions.set(row.id, row);
        return { rows: [row], rowCount: 1 };
      }
      if (text.startsWith('UPDATE "ao_system"."gold_definitions_store" SET gold_code = $1')) {
        const id = Number(params[9]);
        const row = state.definitions.get(id);
        if (!row) return { rows: [], rowCount: 0 };
        row.gold_code = params[0];
        row.gold_name = params[1];
        row.description = params[2];
        row.owner_name = params[3];
        row.tags = JSON.parse(params[4]);
        row.definition_json = JSON.parse(params[5]);
        row.updated_at = new Date().toISOString();
        row.updated_by = params[6];
        return { rows: [row], rowCount: 1 };
      }
      if (text.startsWith('SELECT * FROM "ao_system"."gold_definitions_store" WHERE id = $1 LIMIT 1')) {
        const row = state.definitions.get(Number(params[0]));
        return { rows: row ? [row] : [], rowCount: row ? 1 : 0 };
      }
      if (text.startsWith('SELECT c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1')) {
        return { rows: [{ relkind: 'r' }], rowCount: 1 };
      }
      if (text.startsWith('SELECT * FROM "silver_adv"."wb_ads_daily" LIMIT')) {
        return {
          rows: [
            { event_date: '2026-04-01', revenue: 10, clicks: 2 },
            { event_date: '2026-04-01', revenue: 15, clicks: 3 }
          ],
          rowCount: 2
        };
      }
      if (text.startsWith('UPDATE "ao_system"."gold_definition_versions_store" SET lifecycle_state = \'inactive\'')) {
        state.versions = state.versions.map((item) =>
          item.gold_definition_id === Number(params[0]) ? { ...item, lifecycle_state: 'inactive' } : item
        );
        return { rows: [], rowCount: 0 };
      }
      if (text.startsWith('INSERT INTO "ao_system"."gold_definition_versions_store"')) {
        state.versions.push({
          gold_definition_id: Number(params[0]),
          gold_code: params[1],
          version: Number(params[2]),
          lifecycle_state: 'active',
          definition_json: JSON.parse(params[4]),
          status_snapshot: JSON.parse(params[6])
        });
        return { rows: [], rowCount: 1 };
      }
      if (text.startsWith('UPDATE "ao_system"."gold_definitions_store" SET published = true')) {
        const row = state.definitions.get(Number(params[0]));
        row.published = true;
        row.active_version = Number(params[1]);
        row.definition_json = JSON.parse(params[2]);
        row.status = 'published';
        row.updated_by = params[3];
        return { rows: [row], rowCount: 1 };
      }
      if (text.startsWith('TRUNCATE TABLE')) {
        return { rows: [], rowCount: 0 };
      }
      if (text.startsWith('INSERT INTO "gold_showcase"."roi_daily"')) return { rows: [], rowCount: 1 };
      if (text.startsWith('SELECT c.column_name AS field_name') || text.includes('FROM information_schema.columns c')) {
        const schema = String(params?.[0] || '');
        const table = String(params?.[1] || '');
        if (schema === 'gold_showcase' && table === 'roi_daily') {
          return {
            rows: [
              { field_name: 'event_date', field_type: 'date', description: '' },
              { field_name: 'revenue_sum', field_type: 'numeric', description: '' }
            ],
            rowCount: 2
          };
        }
        return {
          rows: [
            { field_name: 'event_date', field_type: 'date', description: '' },
            { field_name: 'revenue_sum', field_type: 'numeric', description: '' }
          ],
          rowCount: 2
        };
      }
      if (text.startsWith('SELECT COALESCE(obj_description(c.oid), \'\') AS description')) {
        return { rows: [{ description: 'gold showcase' }], rowCount: 1 };
      }
      if (text.startsWith('SELECT COALESCE(MAX(version), 0) AS max_v FROM "ao_system"."table_data_contract_versions"')) {
        return { rows: [{ max_v: 0 }], rowCount: 1 };
      }
      if (text.startsWith('SELECT 1 FROM "ao_system"."table_data_contract_versions" WHERE schema_name = $1 AND table_name = $2 AND lifecycle_state = \'active\' LIMIT 1')) {
        return { rows: [], rowCount: 0 };
      }
      if (text.startsWith('INSERT INTO "ao_system"."table_data_contract_versions"')) {
        state.contracts.push({ schema_name: params[0], table_name: params[1], contract_name: params[2], version: Number(params[3]) });
        return { rows: [], rowCount: 1 };
      }
      if (text.startsWith('INSERT INTO "ao_system"."gold_refresh_runs_store"')) {
        const row = {
          gold_definition_id: Number(params[0]),
          gold_code: params[1],
          run_uid: params[3],
          status: params[4],
          row_count: Number(params[8] || 0)
        };
        state.refreshRuns.push(row);
        return { rows: [row], rowCount: 1 };
      }

      throw new Error(`Unhandled SQL: ${text}`);
    }
  };
}

test('gold router: draft upsert creates definition row', async () => {
  const client = createGoldClient();
  const definition = await _testUpsertDefinitionDraft(
    client,
    TEST_CONFIG,
    {
      metadata: { code: 'roi_daily', name: 'ROI daily', description: 'showcase' },
      sources: [{ source_key: 'process:1', source_kind: 'process', schema_name: 'silver_adv', table_name: 'wb_ads_daily', selected_fields: [{ field_name: 'event_date' }] }]
    },
    { updatedBy: 'tester' }
  );

  assert.equal(definition.code, 'roi_daily');
  assert.equal(definition.name, 'ROI daily');
  assert.equal(client.state.definitions.size, 1);
});

test('gold router: publish writes active version from hydrated sources', async () => {
  const client = createGoldClient();
  const created = await _testUpsertDefinitionDraft(
    client,
    TEST_CONFIG,
    {
      metadata: { code: 'roi_daily', name: 'ROI daily' },
      sources: [
        {
          source_key: 'process:24:start_1:silver_adv.wb_ads_daily',
          source_kind: 'process',
          schema_name: 'silver_adv',
          table_name: 'wb_ads_daily',
          selected_fields: [
            { field_name: 'event_date', field_type: 'date' },
            { field_name: 'revenue', field_type: 'numeric' }
          ]
        }
      ],
      transformations: {
        groupings: [{ field_name: 'event_date', alias: 'event_date' }],
        aggregations: [{ alias: 'revenue_sum', field_name: 'revenue', aggregator: 'sum', field_type: 'numeric' }]
      }
    },
    { updatedBy: 'tester' }
  );

  const published = await _testPublishDefinition(client, TEST_CONFIG, created.id, {
    publishedBy: 'tester',
    sourceCatalogByKey: {
      'process:24:start_1:silver_adv.wb_ads_daily': {
        source_key: 'process:24:start_1:silver_adv.wb_ads_daily',
        source_kind: 'process',
        source_name: 'WB daily',
        schema_name: 'silver_adv',
        table_name: 'wb_ads_daily',
        contract_version: 3,
        fields: [
          { name: 'event_date', type: 'date' },
          { name: 'revenue', type: 'numeric' }
        ]
      }
    }
  });

  assert.equal(published.version, 1);
  assert.equal(client.state.versions.length, 1);
  assert.equal(client.state.definitions.get(created.id).published, true);
  assert.equal(published.previewModel.dataset.rows.length, 1);
  assert.equal(published.previewModel.dataset.rows[0].revenue_sum, 25);
});

test('gold router: materialization and refresh log keep real columns', async () => {
  const client = createGoldClient();
  const previewModel = {
    dataset: {
      rows: [{ event_date: '2026-04-01', revenue_sum: 25 }],
      output_fields: [
        { name: 'event_date', type: 'date' },
        { name: 'revenue_sum', type: 'numeric' }
      ]
    }
  };

  const materialized = await materializeGoldDefinition(
    client,
    {
      metadata: { code: 'roi_daily', description: 'ROI daily showcase' },
      materialization: { mode: 'snapshot_table', target_schema: 'gold_showcase', target_name: 'roi_daily' }
    },
    previewModel
  );

  const refreshRow = await _testInsertRefreshRun(client, TEST_CONFIG, {
    gold_definition_id: 100,
    gold_code: 'roi_daily',
    definition_version: 1,
    run_uid: 'gold_refresh_1',
    status: 'completed',
    materialization_mode: materialized.mode,
    target_schema: materialized.schema_name,
    target_name: materialized.target_name,
    row_count: materialized.row_count,
    error_text: '',
    source_versions_json: {},
    preview_columns: previewModel.dataset.output_fields,
    sample_rows: previewModel.dataset.rows,
    quality_json: { machine_status: 'healthy' },
    finished_at: new Date().toISOString(),
    triggered_by: 'tester'
  });

  assert.equal(materialized.schema_name, 'gold_showcase');
  assert.equal(materialized.target_name, 'roi_daily');
  assert.ok(
    client.state.contracts.some((item) => item.schema_name === 'gold_showcase' && item.table_name === 'roi_daily'),
    'gold snapshot contract must be created'
  );
  assert.equal(refreshRow.status, 'completed');
  assert.equal(refreshRow.row_count, 1);
});
