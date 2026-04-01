import test from 'node:test';
import assert from 'node:assert/strict';

process.env.PGUSER = process.env.PGUSER || 'test';
process.env.PGPASSWORD = process.env.PGPASSWORD || 'test';
process.env.PGDATABASE = process.env.PGDATABASE || 'test';

const { buildWriteNodeTargetColumns, createWriteNodeTargetTable } = await import('../server/tableBuilder.mjs');
const { previewWriteConfig } = await import('../server/writeRuntime.mjs');

const TEST_RUNTIME = {
  config: {
    contracts_schema: 'ao_system',
    contracts_table: 'table_data_contract_versions',
    templates_schema: 'ao_system',
    templates_table: 'table_templates_store'
  },
  templatesQn: '"ao_system"."table_templates_store"',
  contractsQn: '"ao_system"."table_data_contract_versions"',
  skipEnsure: true
};

function createWriteNodeCreateClient({ existingTemplate = false, existingTable = false } = {}) {
  const calls = [];
  const createdContracts = new Map();
  const client = {
    calls,
    async query(sql, params = []) {
      const text = String(sql || '').replace(/\s+/g, ' ').trim();
      calls.push({ sql: text, params });

      if (text === 'BEGIN' || text === 'COMMIT' || text === 'ROLLBACK') {
        return { rows: [], rowCount: 0 };
      }

      if (text.includes('FROM "ao_system"."table_templates_store"') && text.includes('lower(template_name) = lower($1)')) {
        return existingTemplate
          ? { rows: [{ id: 91, template_name: 'dup_template', schema_name: 'ao_data', table_name: 'dup_table' }], rowCount: 1 }
          : { rows: [], rowCount: 0 };
      }

      if (text.includes('FROM information_schema.tables')) {
        return existingTable ? { rows: [{ '?column?': 1 }], rowCount: 1 } : { rows: [], rowCount: 0 };
      }

      if (text.includes('FROM "ao_system"."table_data_contract_versions"') && text.includes("lifecycle_state = 'active'")) {
        const schema = String(params?.[0] || '');
        const table = String(params?.[1] || '');
        if (schema === 'ao_system' && table === 'table_templates_store') {
          return {
            rows: [{ contract_name: 'ao_system__table_templates_store__contract', version: 4 }],
            rowCount: 1
          };
        }
        const created = createdContracts.get(`${schema}.${table}`);
        return created ? { rows: [created], rowCount: 1 } : { rows: [], rowCount: 0 };
      }

      if (text.includes('FROM "ao_system"."table_data_contract_versions"') && text.includes("lifecycle_state <> 'deleted_by_user'")) {
        const schema = String(params?.[0] || '');
        const table = String(params?.[1] || '');
        const created = createdContracts.get(`${schema}.${table}`);
        return created ? { rows: [created], rowCount: 1 } : { rows: [], rowCount: 0 };
      }

      if (text.startsWith('INSERT INTO "ao_system"."table_templates_store"')) {
        return { rows: [{ id: 77 }], rowCount: 1 };
      }

      if (
        text.startsWith('CREATE SCHEMA') ||
        text.startsWith('CREATE TABLE') ||
        text.startsWith('COMMENT ON TABLE') ||
        text.startsWith('COMMENT ON COLUMN')
      ) {
        return { rows: [], rowCount: 0 };
      }

      if (text.startsWith('SELECT COALESCE(MAX(version), 0) AS max_v FROM "ao_system"."table_data_contract_versions"')) {
        return { rows: [{ max_v: 0 }], rowCount: 1 };
      }

      if (text.startsWith('UPDATE "ao_system"."table_data_contract_versions" SET lifecycle_state = \'inactive\'')) {
        return { rows: [], rowCount: 0 };
      }

      if (text.startsWith('INSERT INTO "ao_system"."table_data_contract_versions"')) {
        const schema = String(params?.[0] || '');
        const table = String(params?.[1] || '');
        const contract_name = String(params?.[2] || '');
        const version = Number(params?.[3] || 1) || 1;
        createdContracts.set(`${schema}.${table}`, { contract_name, version });
        return { rows: [], rowCount: 1 };
      }

      throw new Error(`Unhandled SQL in create client: ${text}`);
    }
  };
  return client;
}

function createPreviewClient(columns) {
  const names = Array.isArray(columns) ? columns : [];
  return {
    async query(sql, params = []) {
      const text = String(sql || '');
      if (text.includes('information_schema.tables')) return { rows: [{ '?column?': 1 }], rowCount: 1 };
      if (text.includes('information_schema.columns')) {
        return {
          rows: names.map((name) => ({ column_name: name, data_type: 'text' })),
          rowCount: names.length
        };
      }
      if (text.startsWith('SELECT 1 FROM')) return { rows: [], rowCount: 0 };
      if (text.startsWith('INSERT INTO')) return { rows: [], rowCount: Array.isArray(params) ? params.length : 0 };
      if (text.startsWith('UPDATE ')) return { rows: [], rowCount: 0 };
      return { rows: [], rowCount: 0 };
    }
  };
}

test('table builder write create: buildWriteNodeTargetColumns adds ao fields once', () => {
  const columns = buildWriteNodeTargetColumns([
    { alias: 'id', type: 'text', path: 'list.id' },
    { alias: 'title', type: 'text', path: 'list.title' },
    { alias: 'ao_source', type: 'text', path: 'ao_source' }
  ]);

  const fieldNames = columns.map((item) => item.field_name);
  assert.deepEqual(fieldNames.slice(0, 3), ['id', 'title', 'ao_source']);
  assert.equal(fieldNames.filter((name) => name === 'ao_source').length, 1);
  assert.equal(fieldNames.includes('ao_contract_version'), true);
});

test('table builder write create: createWriteNodeTargetTable creates template, table and active contract', async () => {
  const client = createWriteNodeCreateClient();
  const result = await createWriteNodeTargetTable(
    client,
    {
      schema_name: 'ao_data',
      table_name: 'silver_ads_new',
      template_name: 'write_silver_ads_new',
      data_level: 'silver',
      table_class: 'silver_table',
      description: 'write-node create test',
      upstream_fields: [
        { alias: 'id', type: 'text', path: 'list.id' },
        { alias: 'title', type: 'text', path: 'list.title' },
        { alias: 'state', type: 'text', path: 'list.state' }
      ]
    },
    TEST_RUNTIME
  );

  assert.equal(result.ok, true);
  assert.equal(result.schema_name, 'ao_data');
  assert.equal(result.table_name, 'silver_ads_new');
  assert.equal(result.template_name, 'write_silver_ads_new');
  assert.equal(result.template_id, 77);
  assert.equal(result.contract.contract_name, 'ao_data.silver_ads_new');
  assert.equal(result.contract.version, 1);
  assert.equal(result.columns.some((item) => item.field_name === 'id'), true);
  assert.equal(result.columns.some((item) => item.field_name === 'title'), true);
  assert.equal(result.columns.some((item) => item.field_name === 'state'), true);
  assert.equal(result.columns.some((item) => item.field_name === 'ao_source'), true);
  assert.equal(result.columns.some((item) => item.field_name === 'ao_contract_version'), true);
  assert.equal(client.calls.some((item) => item.sql.startsWith('INSERT INTO "ao_system"."table_templates_store"')), true);
  assert.equal(client.calls.some((item) => item.sql.startsWith('CREATE TABLE "ao_data"."silver_ads_new"')), true);
  assert.equal(client.calls.some((item) => item.sql.startsWith('INSERT INTO "ao_system"."table_data_contract_versions"')), true);
  assert.equal(client.calls.at(-1)?.sql, 'COMMIT');
});

test('table builder write create: rejects duplicate schema.table', async () => {
  const client = createWriteNodeCreateClient({ existingTable: true });
  await assert.rejects(
    () =>
      createWriteNodeTargetTable(
        client,
        {
          schema_name: 'ao_data',
          table_name: 'silver_ads_new',
          template_name: 'write_silver_ads_new',
          upstream_fields: [{ alias: 'id', type: 'text' }]
        },
        TEST_RUNTIME
      ),
    /table_name_conflict/
  );
  assert.equal(client.calls.some((item) => item.sql === 'ROLLBACK'), true);
});

test('table builder write create: rejects duplicate template_name', async () => {
  const client = createWriteNodeCreateClient({ existingTemplate: true });
  await assert.rejects(
    () =>
      createWriteNodeTargetTable(
        client,
        {
          schema_name: 'ao_data',
          table_name: 'silver_ads_new',
          template_name: 'write_silver_ads_new',
          upstream_fields: [{ alias: 'id', type: 'text' }]
        },
        TEST_RUNTIME
      ),
    /template_name_conflict/
  );
  assert.equal(client.calls.some((item) => item.sql === 'ROLLBACK'), true);
});

test('table builder write create: created binding is immediately usable by write preview', async () => {
  const client = createWriteNodeCreateClient();
  const created = await createWriteNodeTargetTable(
    client,
    {
      schema_name: 'ao_data',
      table_name: 'silver_ads_bound',
      template_name: 'write_silver_ads_bound',
      upstream_fields: [
        { alias: 'id', type: 'text', path: 'list.id' },
        { alias: 'title', type: 'text', path: 'list.title' }
      ]
    },
    TEST_RUNTIME
  );

  const preview = await previewWriteConfig(
    createPreviewClient(created.columns.map((item) => item.field_name)),
    {
      sourceMode: 'node',
      targetSchema: created.schema_name,
      targetTable: created.table_name,
      writeMode: 'insert'
    },
    {
      inputValue: {
        contract_version: 'node_io_v1',
        rows: [{ id: '1', title: 'Alpha' }]
      }
    }
  );

  assert.equal(preview.target_schema, 'ao_data');
  assert.equal(preview.target_table, 'silver_ads_bound');
  assert.equal(preview.mapping_summary.rows_ready, 1);
  assert.equal(preview.mapping_rows.some((row) => row.sourceField === 'id' && row.targetField === 'id'), true);
});
