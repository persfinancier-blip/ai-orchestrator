import express from 'express';

import { pool } from './db.mjs';
import { createContractVersionFromTable, fieldTypeToSql, getTableColumnsSnapshot } from './tableBuilder.mjs';
import {
  buildGoldPreviewModel,
  buildNamedSourceCatalog,
  buildProcessGoldSources
} from './goldBuilderCore.mjs';
import {
  analyzeGoldImpact,
  buildGoldDependencyGraph,
  normalizeGoldDefinition,
  planGoldMaterialization,
  planGoldRefresh,
  resolveConsumerDependencies,
  validateGoldDefinition
} from '../shared/goldDefinitionCore.mjs';

export const goldBuilderRouter = express.Router();
goldBuilderRouter.use(express.json({ limit: '4mb' }));

const DEFAULT_RUNTIME = Object.freeze({
  settings_schema: 'ao_system',
  settings_table: 'table_settings_store',
  templates_schema: 'ao_system',
  templates_table: 'table_templates_store',
  contracts_schema: 'ao_system',
  contracts_table: 'table_data_contract_versions',
  workflow_desks_schema: 'ao_system',
  workflow_desks_table: 'workflow_desks_store',
  workflow_desk_versions_table: 'workflow_desk_versions_store',
  gold_schema: 'ao_system',
  gold_definitions_table: 'gold_definitions_store',
  gold_definition_versions_table: 'gold_definition_versions_store',
  gold_external_sources_table: 'gold_external_sources_store',
  gold_refresh_runs_table: 'gold_refresh_runs_store'
});

const MAX_PREVIEW_ROWS = 200;
const MAX_REFRESH_ROWS_PER_SOURCE = 50_000;

function requireDataAdmin(req, res, next) {
  const role = String(req.header('X-AO-ROLE') || '').trim();
  if (role !== 'data_admin') {
    return res.status(403).json({ error: 'forbidden', details: 'data_admin role required' });
  }
  next();
}

function asText(value) {
  return String(value || '').trim();
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function asObject(value) {
  return value && typeof value === 'object' && !Array.isArray(value) ? value : {};
}

function isIdent(value) {
  return typeof value === 'string' && /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(value);
}

function qi(value) {
  if (!isIdent(value)) throw new Error(`invalid_identifier:${value}`);
  return `"${value}"`;
}

function qname(schema, table) {
  return `${qi(schema)}.${qi(table)}`;
}

function parseJsonSafe(value, fallback) {
  if (value && typeof value === 'object') return value;
  const raw = asText(value);
  if (!raw) return fallback;
  try {
    const parsed = JSON.parse(raw);
    return parsed ?? fallback;
  } catch {
    return fallback;
  }
}

function parseSettingStorage(value, fallbackSchema, fallbackTable) {
  if (typeof value === 'string') {
    const raw = value.trim();
    const dot = raw.indexOf('.');
    if (dot > 0 && dot < raw.length - 1) {
      return { schema: raw.slice(0, dot).trim(), table: raw.slice(dot + 1).trim() };
    }
  }
  const obj = asObject(value);
  return {
    schema: asText(obj.schema || obj.schema_name || fallbackSchema) || fallbackSchema,
    table: asText(obj.table || obj.table_name || fallbackTable) || fallbackTable
  };
}

function buildTableKey(schema, table) {
  return `${asText(schema).toLowerCase()}.${asText(table).toLowerCase()}`;
}

async function loadGoldRuntimeConfig(client) {
  const next = { ...DEFAULT_RUNTIME };
  try {
    const qn = qname(DEFAULT_RUNTIME.settings_schema, DEFAULT_RUNTIME.settings_table);
    const rows = await client.query(
      `
      SELECT setting_key, setting_value
      FROM ${qn}
      WHERE is_active = true
        AND setting_key IN (
          'templates_storage',
          'contracts_storage',
          'workflow_desks_storage',
          'workflow_desk_versions_storage'
        )
      `
    );
    for (const row of rows.rows || []) {
      const key = asText(row.setting_key);
      const value = parseJsonSafe(row.setting_value, row.setting_value);
      if (key === 'templates_storage') {
        const parsed = parseSettingStorage(value, next.templates_schema, next.templates_table);
        next.templates_schema = parsed.schema;
        next.templates_table = parsed.table;
      }
      if (key === 'contracts_storage') {
        const parsed = parseSettingStorage(value, next.contracts_schema, next.contracts_table);
        next.contracts_schema = parsed.schema;
        next.contracts_table = parsed.table;
      }
      if (key === 'workflow_desks_storage') {
        const parsed = parseSettingStorage(value, next.workflow_desks_schema, next.workflow_desks_table);
        next.workflow_desks_schema = parsed.schema;
        next.workflow_desks_table = parsed.table;
      }
      if (key === 'workflow_desk_versions_storage') {
        const parsed = parseSettingStorage(value, next.workflow_desks_schema, next.workflow_desk_versions_table);
        next.workflow_desks_schema = parsed.schema;
        next.workflow_desk_versions_table = parsed.table;
      }
    }
  } catch {
    // keep defaults
  }
  return next;
}

function definitionsQn(config) {
  return qname(config.gold_schema, config.gold_definitions_table);
}

function versionsQn(config) {
  return qname(config.gold_schema, config.gold_definition_versions_table);
}

function externalSourcesQn(config) {
  return qname(config.gold_schema, config.gold_external_sources_table);
}

function refreshRunsQn(config) {
  return qname(config.gold_schema, config.gold_refresh_runs_table);
}

function contractsQn(config) {
  return qname(config.contracts_schema, config.contracts_table);
}

function templatesQn(config) {
  return qname(config.templates_schema, config.templates_table);
}

function desksQn(config) {
  return qname(config.workflow_desks_schema, config.workflow_desks_table);
}

function deskVersionsQn(config) {
  return qname(config.workflow_desks_schema, config.workflow_desk_versions_table);
}

async function relationKind(client, schema, name) {
  const r = await client.query(
    `
    SELECT c.relkind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = $1
      AND c.relname = $2
    LIMIT 1
    `,
    [schema, name]
  );
  return asText(r.rows?.[0]?.relkind);
}

async function ensureGoldDefinitionsTable(client, config) {
  await client.query(`CREATE SCHEMA IF NOT EXISTS ${qi(config.gold_schema)}`);
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${definitionsQn(config)} (
      id bigserial PRIMARY KEY,
      gold_code text NOT NULL UNIQUE,
      gold_name text NOT NULL,
      description text NOT NULL DEFAULT '',
      owner_name text NOT NULL DEFAULT '',
      tags jsonb NOT NULL DEFAULT '[]'::jsonb,
      status text NOT NULL DEFAULT 'draft',
      published boolean NOT NULL DEFAULT false,
      active_version integer NOT NULL DEFAULT 0,
      definition_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      updated_at timestamptz NOT NULL DEFAULT now(),
      updated_by text NOT NULL DEFAULT 'ui',
      ao_source text,
      ao_run_id text,
      ao_created_at timestamptz,
      ao_updated_at timestamptz,
      ao_contract_schema text,
      ao_contract_name text,
      ao_contract_version integer
    )
  `);
}

async function ensureGoldDefinitionVersionsTable(client, config) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${versionsQn(config)} (
      id bigserial PRIMARY KEY,
      gold_definition_id bigint NOT NULL REFERENCES ${definitionsQn(config)}(id) ON DELETE CASCADE,
      gold_code text NOT NULL,
      version integer NOT NULL,
      lifecycle_state text NOT NULL DEFAULT 'active',
      description text NOT NULL DEFAULT '',
      definition_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      published_at timestamptz NOT NULL DEFAULT now(),
      published_by text NOT NULL DEFAULT 'ui',
      status_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
      ao_source text,
      ao_run_id text,
      ao_created_at timestamptz,
      ao_updated_at timestamptz,
      ao_contract_schema text,
      ao_contract_name text,
      ao_contract_version integer,
      UNIQUE(gold_definition_id, version)
    )
  `);
}

async function ensureGoldExternalSourcesTable(client, config) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${externalSourcesQn(config)} (
      id bigserial PRIMARY KEY,
      source_key text NOT NULL UNIQUE,
      source_kind text NOT NULL DEFAULT 'external',
      source_name text NOT NULL,
      description text NOT NULL DEFAULT '',
      schema_name text NOT NULL,
      table_name text NOT NULL,
      contract_version integer NOT NULL DEFAULT 0,
      expected_freshness_minutes integer NOT NULL DEFAULT 0,
      required_fields jsonb NOT NULL DEFAULT '[]'::jsonb,
      optional_fields jsonb NOT NULL DEFAULT '[]'::jsonb,
      tags jsonb NOT NULL DEFAULT '[]'::jsonb,
      is_active boolean NOT NULL DEFAULT true,
      last_seen_at timestamptz,
      schema_version text NOT NULL DEFAULT '',
      updated_at timestamptz NOT NULL DEFAULT now(),
      updated_by text NOT NULL DEFAULT 'ui',
      ao_source text,
      ao_run_id text,
      ao_created_at timestamptz,
      ao_updated_at timestamptz,
      ao_contract_schema text,
      ao_contract_name text,
      ao_contract_version integer
    )
  `);
}

async function ensureGoldRefreshRunsTable(client, config) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${refreshRunsQn(config)} (
      id bigserial PRIMARY KEY,
      gold_definition_id bigint NOT NULL REFERENCES ${definitionsQn(config)}(id) ON DELETE CASCADE,
      gold_code text NOT NULL,
      definition_version integer NOT NULL DEFAULT 0,
      run_uid text NOT NULL,
      status text NOT NULL DEFAULT 'queued',
      materialization_mode text NOT NULL DEFAULT 'snapshot_table',
      target_schema text NOT NULL DEFAULT '',
      target_name text NOT NULL DEFAULT '',
      row_count integer NOT NULL DEFAULT 0,
      error_text text NOT NULL DEFAULT '',
      source_versions_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      preview_columns jsonb NOT NULL DEFAULT '[]'::jsonb,
      sample_rows jsonb NOT NULL DEFAULT '[]'::jsonb,
      quality_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      started_at timestamptz NOT NULL DEFAULT now(),
      finished_at timestamptz,
      triggered_by text NOT NULL DEFAULT 'ui',
      ao_source text,
      ao_run_id text,
      ao_created_at timestamptz,
      ao_updated_at timestamptz,
      ao_contract_schema text,
      ao_contract_name text,
      ao_contract_version integer
    )
  `);
}

async function ensureGoldContracts(client, config) {
  const qn = contractsQn(config);
  for (const table of [
    config.gold_definitions_table,
    config.gold_definition_versions_table,
    config.gold_external_sources_table,
    config.gold_refresh_runs_table
  ]) {
    const existing = await client.query(
      `
      SELECT 1
      FROM ${qn}
      WHERE schema_name = $1
        AND table_name = $2
        AND lifecycle_state = 'active'
      LIMIT 1
      `,
      [config.gold_schema, table]
    );
    if (existing.rows?.length) continue;
    await createContractVersionFromTable(client, config.gold_schema, table, `gold_bootstrap:${table}`, 'gold_builder');
  }
}

function parseDefinitionRow(row = {}) {
  const definition = normalizeGoldDefinition(parseJsonSafe(row.definition_json, {}));
  const activeMart = definition.active_mart;
  const activeScenario = definition.active_scenario;
  definition.metadata.id = String(Number(row.id || 0) || '');
  definition.metadata.code = asText(row.gold_code || definition.metadata.code);
  definition.metadata.name = asText(row.gold_name || definition.metadata.name);
  definition.metadata.description = asText(row.description || definition.metadata.description);
  definition.metadata.owner = asText(row.owner_name || definition.metadata.owner);
  definition.metadata.tags = asArray(parseJsonSafe(row.tags, definition.metadata.tags));
  definition.metadata.status = asText(row.status || definition.metadata.status || 'draft');
  definition.metadata.published = Boolean(row.published);
  definition.metadata.version = Math.max(1, Number(row.active_version || definition.metadata.version || 1));
  return {
    id: Number(row.id || 0),
    code: definition.metadata.code,
    name: definition.metadata.name,
    description: definition.metadata.description,
    published: definition.metadata.published,
    active_version: Math.max(0, Number(row.active_version || 0)),
    status: definition.metadata.status,
    updated_at: asText(row.updated_at),
    updated_by: asText(row.updated_by),
    group_name: asText(definition.metadata.group_name),
    mart_count: asArray(definition.marts).length,
    scenario_count: asArray(activeMart?.scenarios).length,
    active_mart_id: asText(activeMart?.id),
    active_mart_name: asText(activeMart?.name),
    active_scenario_id: asText(activeScenario?.id),
    active_scenario_name: asText(activeScenario?.name),
    definition
  };
}

function parseRefreshRow(row = {}) {
  return {
    id: Number(row.id || 0),
    gold_definition_id: Number(row.gold_definition_id || 0),
    gold_code: asText(row.gold_code),
    definition_version: Math.max(0, Number(row.definition_version || 0)),
    run_uid: asText(row.run_uid),
    status: asText(row.status),
    materialization_mode: asText(row.materialization_mode),
    target_schema: asText(row.target_schema),
    target_name: asText(row.target_name),
    row_count: Math.max(0, Number(row.row_count || 0)),
    error_text: asText(row.error_text),
    preview_columns: parseJsonSafe(row.preview_columns, []),
    sample_rows: parseJsonSafe(row.sample_rows, []),
    quality_json: parseJsonSafe(row.quality_json, {}),
    started_at: asText(row.started_at),
    finished_at: asText(row.finished_at)
  };
}

async function loadActiveContractRows(client, config) {
  const rows = await client.query(
    `
    SELECT schema_name, table_name, contract_name, version, columns
    FROM ${contractsQn(config)}
    WHERE lifecycle_state = 'active'
    ORDER BY schema_name, table_name
    `
  );
  return rows.rows || [];
}

async function loadTemplateRows(client, config) {
  try {
    const rows = await client.query(
      `
      SELECT template_name, schema_name, table_name, data_level, template_kind, table_class
      FROM ${templatesQn(config)}
      `
    );
    return rows.rows || [];
  } catch {
    return [];
  }
}

async function loadPublishedDesks(client, config) {
  const rows = await client.query(
    `
    SELECT
      d.id AS desk_id,
      d.desk_name,
      v.id AS desk_version_id,
      v.version_no,
      v.graph_json
    FROM ${desksQn(config)} d
    JOIN ${deskVersionsQn(config)} v
      ON v.desk_id = d.id
    WHERE d.is_active = true
      AND v.is_published = true
    ORDER BY d.id DESC
    `
  );
  return rows.rows || [];
}

async function loadExternalSourceRows(client, config) {
  const rows = await client.query(
    `
    SELECT *
    FROM ${externalSourcesQn(config)}
    ORDER BY source_name, id
    `
  );
  return rows.rows || [];
}

async function buildSourceCatalog(client, config) {
  const [publishedDesks, contractRows, templateRows, registryRows] = await Promise.all([
    loadPublishedDesks(client, config),
    loadActiveContractRows(client, config),
    loadTemplateRows(client, config),
    loadExternalSourceRows(client, config)
  ]);
  const process_sources = buildProcessGoldSources({ publishedDesks, contractRows, templateRows });
  const named = buildNamedSourceCatalog({ registryRows, contractRows });
  return {
    process_sources,
    external_sources: named.filter((item) => item.source_kind === 'external'),
    reference_sources: named.filter((item) => item.source_kind === 'reference'),
    source_map: Object.fromEntries([...process_sources, ...named].map((item) => [item.source_key, item]))
  };
}

async function loadDefinitionRows(client, config) {
  const rows = await client.query(
    `
    SELECT *
    FROM ${definitionsQn(config)}
    ORDER BY updated_at DESC, id DESC
    `
  );
  return (rows.rows || []).map(parseDefinitionRow);
}

async function loadDefinitionById(client, config, definitionId) {
  const row = await client.query(`SELECT * FROM ${definitionsQn(config)} WHERE id = $1 LIMIT 1`, [definitionId]);
  const item = row.rows?.[0];
  return item ? parseDefinitionRow(item) : null;
}

async function loadLatestRefreshMap(client, config) {
  const rows = await client.query(
    `
    SELECT DISTINCT ON (gold_definition_id) *
    FROM ${refreshRunsQn(config)}
    ORDER BY gold_definition_id, started_at DESC, id DESC
    `
  );
  const map = new Map();
  (rows.rows || []).forEach((row) => {
    map.set(Number(row.gold_definition_id || 0), parseRefreshRow(row));
  });
  return map;
}

async function loadActiveVersionRow(client, config, definitionId) {
  const row = await client.query(
    `
    SELECT *
    FROM ${versionsQn(config)}
    WHERE gold_definition_id = $1
      AND lifecycle_state = 'active'
    ORDER BY version DESC, id DESC
    LIMIT 1
    `,
    [definitionId]
  );
  return row.rows?.[0] || null;
}

async function loadSourceRowsForDefinition(client, definition, limit) {
  const rowsByKey = {};
  for (const source of definition.sources) {
    const schema = asText(source.schema_name);
    const table = asText(source.table_name);
    if (!schema || !table) {
      rowsByKey[source.source_key] = [];
      continue;
    }
    const relation = await relationKind(client, schema, table);
    if (!relation) throw new Error(`gold_source_relation_not_found:${schema}.${table}`);
    const safeLimit = Math.max(1, Math.min(MAX_REFRESH_ROWS_PER_SOURCE, Math.trunc(Number(limit || MAX_PREVIEW_ROWS))));
    const result = await client.query(`SELECT * FROM ${qname(schema, table)} LIMIT ${safeLimit}`);
    rowsByKey[source.source_key] = asArray(result.rows);
  }
  return rowsByKey;
}

function inferSourceVersions(definition, sourceCatalogByKey) {
  const result = {};
  definition.sources.forEach((source) => {
    const catalog = asObject(sourceCatalogByKey?.[source.source_key]);
    result[source.source_key] = {
      schema_name: asText(catalog.schema_name || source.schema_name),
      table_name: asText(catalog.table_name || source.table_name),
      contract_version: Number(catalog.contract_version || source.contract_version || 0)
    };
  });
  return result;
}

function definitionPayload(definition, published) {
  const normalized = normalizeGoldDefinition(definition);
  normalized.metadata.published = Boolean(published);
  return normalized;
}

async function insertRefreshRun(client, config, payload) {
  const row = await client.query(
    `
    INSERT INTO ${refreshRunsQn(config)}
      (
        gold_definition_id, gold_code, definition_version, run_uid, status, materialization_mode, target_schema, target_name,
        row_count, error_text, source_versions_json, preview_columns, sample_rows, quality_json,
        started_at, finished_at, triggered_by,
        ao_source, ao_run_id, ao_created_at, ao_updated_at, ao_contract_schema, ao_contract_name, ao_contract_version
      )
    VALUES
      ($1, $2, $3, $4, $5, $6, $7, $8,
       $9, $10, $11::jsonb, $12::jsonb, $13::jsonb, $14::jsonb,
       now(), $15, $16,
       'gold_refresh', $4, now(), now(), $17, $18, 1)
    RETURNING *
    `,
    [
      payload.gold_definition_id,
      payload.gold_code,
      payload.definition_version,
      payload.run_uid,
      payload.status,
      payload.materialization_mode,
      payload.target_schema,
      payload.target_name,
      payload.row_count,
      payload.error_text,
      JSON.stringify(payload.source_versions_json || {}),
      JSON.stringify(payload.preview_columns || []),
      JSON.stringify(payload.sample_rows || []),
      JSON.stringify(payload.quality_json || {}),
      payload.finished_at || null,
      payload.triggered_by || 'ui',
      config.contracts_schema,
      `${config.gold_schema}.${config.gold_refresh_runs_table}`
    ]
  );
  return parseRefreshRow(row.rows?.[0] || {});
}

function jsonbRecordsetTypes(outputFields = []) {
  return outputFields.map((field) => `${qi(field.name)} ${fieldTypeToSql(field.type)}`).join(', ');
}

async function ensureSnapshotTableShape(client, schema, name, outputFields, description) {
  await client.query(`CREATE SCHEMA IF NOT EXISTS ${qi(schema)}`);
  const relation = await relationKind(client, schema, name);
  if (!relation) {
    const defs = outputFields.map((field) => `${qi(field.name)} ${fieldTypeToSql(field.type)}`).join(', ');
    await client.query(`CREATE TABLE ${qname(schema, name)} (${defs || `${qi('id')} bigint`})`);
  } else if (!['r', 'p'].includes(relation)) {
    throw new Error(`gold_target_relation_type_invalid:${schema}.${name}:${relation}`);
  }
  const existingColumns = await getTableColumnsSnapshot(client, schema, name);
  const existingNames = new Set(existingColumns.map((item) => asText(item.field_name).toLowerCase()));
  for (const field of outputFields) {
    if (existingNames.has(asText(field.name).toLowerCase())) continue;
    await client.query(`ALTER TABLE ${qname(schema, name)} ADD COLUMN IF NOT EXISTS ${qi(field.name)} ${fieldTypeToSql(field.type)}`);
  }
  if (description) {
    const safe = description.replace(/'/g, "''");
    await client.query(`COMMENT ON TABLE ${qname(schema, name)} IS '${safe}'`);
  }
}

async function writeSnapshotRows(client, schema, name, outputFields, rows) {
  await client.query(`TRUNCATE TABLE ${qname(schema, name)}`);
  if (!rows.length || !outputFields.length) return;
  const cols = outputFields.map((field) => qi(field.name)).join(', ');
  const types = jsonbRecordsetTypes(outputFields);
  await client.query(
    `INSERT INTO ${qname(schema, name)} (${cols}) SELECT ${cols} FROM jsonb_to_recordset($1::jsonb) AS src(${types})`,
    [JSON.stringify(rows)]
  );
}

function jsonLiteral(value) {
  return `'${JSON.stringify(value).replace(/'/g, "''")}'::jsonb`;
}

async function materializeViewLike(client, mode, schema, name, outputFields, rows, description) {
  await client.query(`CREATE SCHEMA IF NOT EXISTS ${qi(schema)}`);
  const relation = await relationKind(client, schema, name);
  if (relation === 'm') await client.query(`DROP MATERIALIZED VIEW IF EXISTS ${qname(schema, name)} CASCADE`);
  if (relation === 'v') await client.query(`DROP VIEW IF EXISTS ${qname(schema, name)} CASCADE`);
  if (relation && !['m', 'v'].includes(relation)) {
    await client.query(`DROP TABLE IF EXISTS ${qname(schema, name)} CASCADE`);
  }
  const types = jsonbRecordsetTypes(outputFields);
  const datasetLiteral = jsonLiteral(rows);
  if (mode === 'materialized_view') {
    await client.query(`CREATE MATERIALIZED VIEW ${qname(schema, name)} AS SELECT * FROM jsonb_to_recordset(${datasetLiteral}) AS src(${types})`);
  } else {
    await client.query(`CREATE OR REPLACE VIEW ${qname(schema, name)} AS SELECT * FROM jsonb_to_recordset(${datasetLiteral}) AS src(${types})`);
  }
  if (description) {
    const safe = description.replace(/'/g, "''");
    const relKeyword = mode === 'materialized_view' ? 'MATERIALIZED VIEW' : 'VIEW';
    await client.query(`COMMENT ON ${relKeyword} ${qname(schema, name)} IS '${safe}'`);
  }
}

async function materializeGoldDefinition(client, definition, previewModel) {
  const plan = planGoldMaterialization(definition);
  const schema = asText(plan.target.schema_name);
  const name = asText(plan.target.name);
  if (!schema || !name) throw new Error('gold_target_required');
  if (plan.mode === 'snapshot_table') {
    await ensureSnapshotTableShape(client, schema, name, previewModel.dataset.output_fields, definition.metadata.description);
    await writeSnapshotRows(client, schema, name, previewModel.dataset.output_fields, previewModel.dataset.rows);
  } else {
    await materializeViewLike(
      client,
      plan.mode,
      schema,
      name,
      previewModel.dataset.output_fields,
      previewModel.dataset.rows,
      definition.metadata.description
    );
  }
  await createContractVersionFromTable(client, schema, name, `gold_refresh:${definition.metadata.code}`, 'gold_builder');
  return {
    schema_name: schema,
    target_name: name,
    mode: plan.mode,
    row_count: previewModel.dataset.rows.length
  };
}

function selectedFieldsFromCatalog(catalog = {}) {
  return asArray(catalog.fields)
    .map((field) => ({
      field_name: asText(field?.name || field?.field_name),
      alias: asText(field?.name || field?.field_name),
      field_type: asText(field?.type || field?.field_type || 'text') || 'text',
      required: false,
      description: asText(field?.description),
      path: asText(field?.path)
    }))
    .filter((field) => field.field_name);
}

function hydrateDefinitionSources(definition, sourceCatalogByKey = {}) {
  const normalized = normalizeGoldDefinition(definition);
  const next = typeof structuredClone === 'function' ? structuredClone(normalized) : JSON.parse(JSON.stringify(normalized));
  const mart = next.marts.find((item) => item.id === next.active_mart_id) || next.marts[0];
  const scenario = mart?.scenarios?.find((item) => item.id === mart.active_scenario_id) || mart?.scenarios?.[0];
  if (!scenario) return normalizeGoldDefinition(next);
  scenario.sources = normalized.sources.map((source) => {
    const catalog = asObject(sourceCatalogByKey?.[source.source_key]);
    return {
      ...catalog,
      ...source,
      source_kind: asText(source.source_kind || catalog.source_kind || 'process'),
      source_name: asText(source.source_name || catalog.source_name || source.source_key),
      process_code: asText(source.process_code || catalog.process_code),
      desk_id: Number(source.desk_id || catalog.desk_id || 0),
      start_node_id: asText(source.start_node_id || catalog.start_node_id),
      schema_name: asText(source.schema_name || catalog.schema_name),
      table_name: asText(source.table_name || catalog.table_name),
      contract_version: Number(source.contract_version || catalog.contract_version || 0),
      required_fields: asArray(source.required_fields).length ? source.required_fields : asArray(catalog.required_fields),
      optional_fields: asArray(source.optional_fields).length ? source.optional_fields : asArray(catalog.optional_fields),
      freshness_expectation_minutes:
        Number(source.freshness_expectation_minutes || 0) > 0
          ? Number(source.freshness_expectation_minutes || 0)
          : Number(catalog.freshness_expectation_minutes || 0),
      selected_fields: asArray(source.selected_fields).length ? source.selected_fields : selectedFieldsFromCatalog(catalog)
    };
  });
  return normalizeGoldDefinition(next);
}

function buildExternalSourceStates(registryRows = []) {
  const states = {};
  asArray(registryRows).forEach((row) => {
    const sourceKey = asText(row.source_key);
    if (!sourceKey) return;
    states[sourceKey] = {
      last_seen_at: asText(row.last_seen_at),
      schema_version: asText(row.schema_version)
    };
  });
  return states;
}

function stableJson(value) {
  return JSON.stringify(normalizeGoldDefinition(value));
}

async function loadActiveVersionMap(client, config) {
  const rows = await client.query(
    `
    SELECT DISTINCT ON (gold_definition_id) *
    FROM ${versionsQn(config)}
    WHERE lifecycle_state = 'active'
    ORDER BY gold_definition_id, version DESC, id DESC
    `
  );
  const map = new Map();
  (rows.rows || []).forEach((row) => {
    map.set(Number(row.gold_definition_id || 0), row);
  });
  return map;
}

async function upsertDefinitionDraft(client, config, definition, { definitionId = 0, updatedBy = 'ui' } = {}) {
  const normalized = definitionPayload(definition, false);
  const code = asText(normalized.metadata.code);
  const name = asText(normalized.metadata.name);
  if (!code) throw new Error('gold_code_required');
  if (!name) throw new Error('gold_name_required');

  const conflict = await client.query(
    `
    SELECT id
    FROM ${definitionsQn(config)}
    WHERE lower(gold_code) = lower($1)
      AND ($2::bigint = 0 OR id <> $2::bigint)
    LIMIT 1
    `,
    [code, Number(definitionId || 0)]
  );
  if (conflict.rows?.length) throw new Error('gold_code_conflict');

  const params = [
    code,
    name,
    asText(normalized.metadata.description),
    asText(normalized.metadata.owner),
    JSON.stringify(asArray(normalized.metadata.tags)),
    JSON.stringify(normalized),
    updatedBy
  ];

  if (Number(definitionId || 0) > 0) {
    const updated = await client.query(
      `
      UPDATE ${definitionsQn(config)}
      SET
        gold_code = $1,
        gold_name = $2,
        description = $3,
        owner_name = $4,
        tags = $5::jsonb,
        definition_json = $6::jsonb,
        updated_at = now(),
        updated_by = $7,
        status = CASE WHEN published THEN status ELSE 'draft' END,
        ao_source = 'gold_builder',
        ao_run_id = $1,
        ao_updated_at = now(),
        ao_contract_schema = $8,
        ao_contract_name = $9,
        ao_contract_version = 1
      WHERE id = $10
      RETURNING *
      `,
      [
        ...params,
        config.gold_schema,
        `${config.gold_schema}.${config.gold_definitions_table}`,
        Number(definitionId || 0)
      ]
    );
    const row = updated.rows?.[0];
    if (!row) throw new Error('gold_definition_not_found');
    return parseDefinitionRow(row);
  }

  const inserted = await client.query(
    `
    INSERT INTO ${definitionsQn(config)}
      (
        gold_code, gold_name, description, owner_name, tags, status, published, active_version, definition_json,
        updated_at, updated_by,
        ao_source, ao_run_id, ao_created_at, ao_updated_at, ao_contract_schema, ao_contract_name, ao_contract_version
      )
    VALUES
      ($1, $2, $3, $4, $5::jsonb, 'draft', false, 0, $6::jsonb,
       now(), $7,
       'gold_builder', $1, now(), now(), $8, $9, 1)
    RETURNING *
    `,
    [...params, config.gold_schema, `${config.gold_schema}.${config.gold_definitions_table}`]
  );
  return parseDefinitionRow(inserted.rows?.[0] || {});
}

async function publishDefinition(client, config, definitionId, { publishedBy = 'ui', sourceCatalogByKey = {} } = {}) {
  const current = await loadDefinitionById(client, config, definitionId);
  if (!current) throw new Error('gold_definition_not_found');
  const hydrated = hydrateDefinitionSources(current.definition, sourceCatalogByKey);
  const validation = validateGoldDefinition(hydrated);
  if (!validation.ok) throw new Error(`gold_definition_invalid:${validation.errors[0]?.code || 'validation_failed'}`);

  const sourceRowsByKey = await loadSourceRowsForDefinition(client, hydrated, MAX_PREVIEW_ROWS);
  const previewModel = buildGoldPreviewModel(hydrated, {
    sourceCatalogByKey,
    sourceRowsByKey
  });
  const nextVersion = Math.max(0, Number(current.active_version || 0)) + 1;

  await client.query(
    `UPDATE ${versionsQn(config)} SET lifecycle_state = 'inactive' WHERE gold_definition_id = $1 AND lifecycle_state = 'active'`,
    [definitionId]
  );
  await client.query(
    `
    INSERT INTO ${versionsQn(config)}
      (
        gold_definition_id, gold_code, version, lifecycle_state, description, definition_json, published_at, published_by,
        status_snapshot, ao_source, ao_run_id, ao_created_at, ao_updated_at, ao_contract_schema, ao_contract_name, ao_contract_version
      )
    VALUES
      ($1, $2, $3, 'active', $4, $5::jsonb, now(), $6,
       $7::jsonb, 'gold_publish', $2, now(), now(), $8, $9, 1)
    `,
    [
      definitionId,
      hydrated.metadata.code,
      nextVersion,
      hydrated.metadata.description,
      JSON.stringify(hydrated),
      publishedBy,
      JSON.stringify(previewModel.status || {}),
      config.gold_schema,
      `${config.gold_schema}.${config.gold_definition_versions_table}`
    ]
  );
  const updated = await client.query(
    `
    UPDATE ${definitionsQn(config)}
    SET
      published = true,
      active_version = $2,
      definition_json = $3::jsonb,
      status = 'published',
      updated_at = now(),
      updated_by = $4,
      ao_source = 'gold_publish',
      ao_run_id = $5,
      ao_updated_at = now(),
      ao_contract_schema = $6,
      ao_contract_name = $7,
      ao_contract_version = 1
    WHERE id = $1
    RETURNING *
    `,
    [
      definitionId,
      nextVersion,
      JSON.stringify(hydrated),
      publishedBy,
      hydrated.metadata.code,
      config.gold_schema,
      `${config.gold_schema}.${config.gold_definitions_table}`
    ]
  );
  return {
    definition: parseDefinitionRow(updated.rows?.[0] || {}),
    version: nextVersion,
    previewModel
  };
}

function parseChangedSources(req) {
  const fromQuery = String(req.query.changed_sources || req.query.changed_source || '').trim();
  if (!fromQuery) return [];
  return fromQuery
    .split(',')
    .map((item) => asText(item))
    .filter(Boolean);
}

function buildDefinitionSummary(definitionRow, { latestRefresh = null, activeVersionRow = null } = {}) {
  const activeDefinition = activeVersionRow ? normalizeGoldDefinition(parseJsonSafe(activeVersionRow.definition_json, {})) : null;
  const hasDraftChanges = Boolean(
    definitionRow?.published &&
      activeDefinition &&
      stableJson(activeDefinition) !== stableJson(definitionRow.definition)
  );
  return {
    ...definitionRow,
    has_draft_changes: hasDraftChanges,
    latest_refresh: latestRefresh,
    active_definition: activeDefinition
  };
}

goldBuilderRouter.get('/gold-sources/catalog', requireDataAdmin, async (_req, res) => {
  const client = await pool.connect();
  try {
    const config = await loadGoldRuntimeConfig(client);
    await ensureGoldDefinitionsTable(client, config);
    await ensureGoldDefinitionVersionsTable(client, config);
    await ensureGoldExternalSourcesTable(client, config);
    await ensureGoldRefreshRunsTable(client, config);
    await ensureGoldContracts(client, config);
    const catalog = await buildSourceCatalog(client, config);
    return res.json({ ok: true, ...catalog });
  } catch (e) {
    return res.status(500).json({ error: 'gold_catalog_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

goldBuilderRouter.post('/gold-sources/upsert', requireDataAdmin, async (req, res) => {
  const client = await pool.connect();
  try {
    const config = await loadGoldRuntimeConfig(client);
    await ensureGoldExternalSourcesTable(client, config);
    const body = asObject(req.body);
    const source_key = asText(body.source_key);
    const source_kind = ['external', 'reference'].includes(asText(body.source_kind).toLowerCase())
      ? asText(body.source_kind).toLowerCase()
      : '';
    const source_name = asText(body.source_name);
    const schema_name = asText(body.schema_name);
    const table_name = asText(body.table_name);
    if (!source_key || !source_kind || !source_name) {
      return res.status(400).json({ error: 'bad_request', details: 'source_key, source_kind and source_name are required' });
    }
    if ((schema_name && !isIdent(schema_name)) || (table_name && !isIdent(table_name))) {
      return res.status(400).json({ error: 'bad_request', details: 'invalid schema/table identifiers' });
    }
    const contractRows = await loadActiveContractRows(client, config);
    const contract = contractRows.find(
      (row) => buildTableKey(row.schema_name, row.table_name) === buildTableKey(schema_name, table_name)
    );
    const params = [
      source_key,
      source_kind,
      source_name,
      asText(body.description),
      schema_name,
      table_name,
      Number(contract?.version || body.contract_version || 0),
      Math.max(0, Number(body.expected_freshness_minutes || 0)),
      JSON.stringify(asArray(body.required_fields)),
      JSON.stringify(asArray(body.optional_fields)),
      JSON.stringify(asArray(body.tags)),
      Boolean(body.is_active !== false),
      asText(body.last_seen_at),
      asText(body.schema_version),
      asText(req.header('X-AO-USER') || 'ui')
    ];
    await client.query(
      `
      INSERT INTO ${externalSourcesQn(config)}
        (
          source_key, source_kind, source_name, description, schema_name, table_name, contract_version,
          expected_freshness_minutes, required_fields, optional_fields, tags, is_active, last_seen_at, schema_version,
          updated_at, updated_by,
          ao_source, ao_run_id, ao_created_at, ao_updated_at, ao_contract_schema, ao_contract_name, ao_contract_version
        )
      VALUES
        ($1, $2, $3, $4, $5, $6, $7,
         $8, $9::jsonb, $10::jsonb, $11::jsonb, $12, NULLIF($13, '')::timestamptz, $14,
         now(), $15,
         'gold_source_registry', $1, now(), now(), $16, $17, 1)
      ON CONFLICT (source_key) DO UPDATE SET
        source_kind = EXCLUDED.source_kind,
        source_name = EXCLUDED.source_name,
        description = EXCLUDED.description,
        schema_name = EXCLUDED.schema_name,
        table_name = EXCLUDED.table_name,
        contract_version = EXCLUDED.contract_version,
        expected_freshness_minutes = EXCLUDED.expected_freshness_minutes,
        required_fields = EXCLUDED.required_fields,
        optional_fields = EXCLUDED.optional_fields,
        tags = EXCLUDED.tags,
        is_active = EXCLUDED.is_active,
        last_seen_at = EXCLUDED.last_seen_at,
        schema_version = EXCLUDED.schema_version,
        updated_at = now(),
        updated_by = EXCLUDED.updated_by,
        ao_source = EXCLUDED.ao_source,
        ao_run_id = EXCLUDED.ao_run_id,
        ao_updated_at = now(),
        ao_contract_schema = EXCLUDED.ao_contract_schema,
        ao_contract_name = EXCLUDED.ao_contract_name,
        ao_contract_version = EXCLUDED.ao_contract_version
      `,
      [...params, config.gold_schema, `${config.gold_schema}.${config.gold_external_sources_table}`]
    );
    const catalog = await buildSourceCatalog(client, config);
    return res.json({ ok: true, ...catalog });
  } catch (e) {
    return res.status(500).json({ error: 'gold_source_upsert_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

goldBuilderRouter.post('/gold-sources/delete', requireDataAdmin, async (req, res) => {
  const client = await pool.connect();
  try {
    const config = await loadGoldRuntimeConfig(client);
    const source_key = asText(req.body?.source_key);
    if (!source_key) return res.status(400).json({ error: 'bad_request', details: 'source_key required' });
    await client.query(`DELETE FROM ${externalSourcesQn(config)} WHERE source_key = $1`, [source_key]);
    return res.json({ ok: true, source_key });
  } catch (e) {
    return res.status(500).json({ error: 'gold_source_delete_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

goldBuilderRouter.get('/gold-definitions', requireDataAdmin, async (_req, res) => {
  const client = await pool.connect();
  try {
    const config = await loadGoldRuntimeConfig(client);
    await ensureGoldDefinitionsTable(client, config);
    await ensureGoldDefinitionVersionsTable(client, config);
    await ensureGoldExternalSourcesTable(client, config);
    await ensureGoldRefreshRunsTable(client, config);
    await ensureGoldContracts(client, config);
    const [definitions, latestRefreshMap, activeVersionMap] = await Promise.all([
      loadDefinitionRows(client, config),
      loadLatestRefreshMap(client, config),
      loadActiveVersionMap(client, config)
    ]);
    return res.json({
      ok: true,
      definitions: definitions.map((item) =>
        buildDefinitionSummary(item, {
          latestRefresh: latestRefreshMap.get(item.id) || null,
          activeVersionRow: activeVersionMap.get(item.id) || null
        })
      )
    });
  } catch (e) {
    return res.status(500).json({ error: 'gold_definitions_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

goldBuilderRouter.get('/gold-definitions/:id', requireDataAdmin, async (req, res) => {
  const client = await pool.connect();
  try {
    const config = await loadGoldRuntimeConfig(client);
    const definition = await loadDefinitionById(client, config, Number(req.params.id || 0));
    if (!definition) return res.status(404).json({ error: 'not_found', details: 'gold definition not found' });
    const [latestRefreshMap, activeVersionRow, catalog] = await Promise.all([
      loadLatestRefreshMap(client, config),
      loadActiveVersionRow(client, config, definition.id),
      buildSourceCatalog(client, config)
    ]);
    return res.json({
      ok: true,
      definition: buildDefinitionSummary(definition, {
        latestRefresh: latestRefreshMap.get(definition.id) || null,
        activeVersionRow
      }),
      source_catalog: catalog,
      previewable_definition: hydrateDefinitionSources(definition.definition, catalog.source_map)
    });
  } catch (e) {
    return res.status(500).json({ error: 'gold_definition_load_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

goldBuilderRouter.post('/gold-definitions/create', requireDataAdmin, async (req, res) => {
  const client = await pool.connect();
  try {
    const config = await loadGoldRuntimeConfig(client);
    await ensureGoldDefinitionsTable(client, config);
    const created = await upsertDefinitionDraft(client, config, req.body?.definition || req.body || {}, {
      updatedBy: asText(req.header('X-AO-USER') || 'ui')
    });
    return res.json({ ok: true, definition: created });
  } catch (e) {
    return res.status(500).json({ error: 'gold_definition_create_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

goldBuilderRouter.post('/gold-definitions/:id/update-draft', requireDataAdmin, async (req, res) => {
  const client = await pool.connect();
  try {
    const config = await loadGoldRuntimeConfig(client);
    const updated = await upsertDefinitionDraft(client, config, req.body?.definition || req.body || {}, {
      definitionId: Number(req.params.id || 0),
      updatedBy: asText(req.header('X-AO-USER') || 'ui')
    });
    return res.json({ ok: true, definition: updated });
  } catch (e) {
    return res.status(500).json({ error: 'gold_definition_update_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

goldBuilderRouter.post('/gold-definitions/:id/validate', requireDataAdmin, async (req, res) => {
  const client = await pool.connect();
  try {
    const config = await loadGoldRuntimeConfig(client);
    const current = await loadDefinitionById(client, config, Number(req.params.id || 0));
    if (!current) return res.status(404).json({ error: 'not_found', details: 'gold definition not found' });
    const catalog = await buildSourceCatalog(client, config);
    const registryRows = await loadExternalSourceRows(client, config);
    const definition = hydrateDefinitionSources(req.body?.definition || current.definition, catalog.source_map);
    const sourceRowsByKey = await loadSourceRowsForDefinition(client, definition, MAX_PREVIEW_ROWS);
    const previewModel = buildGoldPreviewModel(definition, {
      sourceCatalogByKey: catalog.source_map,
      sourceRowsByKey,
      externalSourceStatesByKey: buildExternalSourceStates(registryRows)
    });
    return res.json({
      ok: true,
      validation: previewModel.validation,
      compatibility: previewModel.compatibility,
      status: previewModel.status
    });
  } catch (e) {
    return res.status(500).json({ error: 'gold_definition_validate_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

goldBuilderRouter.post('/gold-definitions/:id/publish', requireDataAdmin, async (req, res) => {
  const client = await pool.connect();
  try {
    const config = await loadGoldRuntimeConfig(client);
    const catalog = await buildSourceCatalog(client, config);
    await client.query('BEGIN');
    const published = await publishDefinition(client, config, Number(req.params.id || 0), {
      publishedBy: asText(req.header('X-AO-USER') || 'ui'),
      sourceCatalogByKey: catalog.source_map
    });
    await client.query('COMMIT');
    return res.json({ ok: true, ...published });
  } catch (e) {
    try {
      await client.query('ROLLBACK');
    } catch {}
    return res.status(500).json({ error: 'gold_definition_publish_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

goldBuilderRouter.get('/gold-definitions/:id/active', requireDataAdmin, async (req, res) => {
  const client = await pool.connect();
  try {
    const config = await loadGoldRuntimeConfig(client);
    const row = await loadActiveVersionRow(client, config, Number(req.params.id || 0));
    if (!row) return res.status(404).json({ error: 'not_found', details: 'active gold version not found' });
    return res.json({
      ok: true,
      active_version: {
        id: Number(row.id || 0),
        gold_definition_id: Number(row.gold_definition_id || 0),
        version: Number(row.version || 0),
        lifecycle_state: asText(row.lifecycle_state),
        description: asText(row.description),
        published_at: asText(row.published_at),
        published_by: asText(row.published_by),
        definition: normalizeGoldDefinition(parseJsonSafe(row.definition_json, {})),
        status_snapshot: parseJsonSafe(row.status_snapshot, {})
      }
    });
  } catch (e) {
    return res.status(500).json({ error: 'gold_definition_active_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

goldBuilderRouter.post('/gold-definitions/:id/preview', requireDataAdmin, async (req, res) => {
  const client = await pool.connect();
  try {
    const config = await loadGoldRuntimeConfig(client);
    const current = await loadDefinitionById(client, config, Number(req.params.id || 0));
    if (!current) return res.status(404).json({ error: 'not_found', details: 'gold definition not found' });
    const [catalog, registryRows, latestRefreshMap] = await Promise.all([
      buildSourceCatalog(client, config),
      loadExternalSourceRows(client, config),
      loadLatestRefreshMap(client, config)
    ]);
    const definition = hydrateDefinitionSources(req.body?.definition || current.definition, catalog.source_map);
    const limit = Math.max(1, Math.min(MAX_PREVIEW_ROWS, Math.trunc(Number(req.body?.limit || MAX_PREVIEW_ROWS))));
    const sourceRowsByKey = await loadSourceRowsForDefinition(client, definition, limit);
    const previewUid = `gold_preview_${Date.now()}_${Math.random().toString(16).slice(2, 8)}`;
    const previewModel = buildGoldPreviewModel(definition, {
      sourceCatalogByKey: catalog.source_map,
      sourceRowsByKey,
      externalSourceStatesByKey: buildExternalSourceStates(registryRows),
      lastRefresh: latestRefreshMap.get(current.id) || null,
      previewUid
    });
    return res.json({ ok: true, preview: previewModel });
  } catch (e) {
    return res.status(500).json({ error: 'gold_preview_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

goldBuilderRouter.post('/gold-definitions/:id/refresh', requireDataAdmin, async (req, res) => {
  const client = await pool.connect();
  const definitionId = Number(req.params.id || 0);
  const run_uid = `gold_refresh_${Date.now()}_${Math.random().toString(16).slice(2, 8)}`;
  try {
    const config = await loadGoldRuntimeConfig(client);
    const current = await loadDefinitionById(client, config, definitionId);
    if (!current) return res.status(404).json({ error: 'not_found', details: 'gold definition not found' });
    const activeRow = await loadActiveVersionRow(client, config, definitionId);
    if (!activeRow) {
      return res.status(409).json({ error: 'gold_not_published', details: 'publish gold definition before refresh' });
    }
    const [catalog, registryRows] = await Promise.all([
      buildSourceCatalog(client, config),
      loadExternalSourceRows(client, config)
    ]);
    const definition = hydrateDefinitionSources(parseJsonSafe(activeRow.definition_json, {}), catalog.source_map);
    const sourceRowsByKey = await loadSourceRowsForDefinition(client, definition, MAX_REFRESH_ROWS_PER_SOURCE);
    const previewModel = buildGoldPreviewModel(definition, {
      sourceCatalogByKey: catalog.source_map,
      sourceRowsByKey,
      externalSourceStatesByKey: buildExternalSourceStates(registryRows),
      previewUid: run_uid
    });
    if (!previewModel.validation.ok) {
      throw new Error(`gold_definition_invalid:${previewModel.validation.errors[0]?.code || 'validation_failed'}`);
    }
    await client.query('BEGIN');
    const materialized = await materializeGoldDefinition(client, definition, previewModel);
    const refreshRow = await insertRefreshRun(client, config, {
      gold_definition_id: definitionId,
      gold_code: definition.metadata.code,
      definition_version: Number(activeRow.version || current.active_version || 0),
      run_uid,
      status: 'completed',
      materialization_mode: materialized.mode,
      target_schema: materialized.schema_name,
      target_name: materialized.target_name,
      row_count: materialized.row_count,
      error_text: '',
      source_versions_json: inferSourceVersions(definition, catalog.source_map),
      preview_columns: previewModel.dataset.output_fields,
      sample_rows: previewModel.dataset.rows.slice(0, 50),
      quality_json: previewModel.status,
      finished_at: new Date().toISOString(),
      triggered_by: asText(req.header('X-AO-USER') || 'ui')
    });
    await client.query(
      `
      UPDATE ${definitionsQn(config)}
      SET
        status = $2,
        updated_at = now(),
        updated_by = $3,
        ao_source = 'gold_refresh',
        ao_run_id = $4,
        ao_updated_at = now()
      WHERE id = $1
      `,
      [definitionId, asText(previewModel.status.health_code || 'healthy'), asText(req.header('X-AO-USER') || 'ui'), run_uid]
    );
    await client.query('COMMIT');
    return res.json({ ok: true, refresh_run: refreshRow, materialization: materialized, preview: previewModel });
  } catch (e) {
    try {
      await client.query('ROLLBACK');
    } catch {}
    try {
      const config = await loadGoldRuntimeConfig(client);
      await insertRefreshRun(client, config, {
        gold_definition_id: definitionId,
        gold_code: '',
        definition_version: 0,
        run_uid,
        status: 'failed',
        materialization_mode: 'snapshot_table',
        target_schema: '',
        target_name: '',
        row_count: 0,
        error_text: String(e?.message || e),
        source_versions_json: {},
        preview_columns: [],
        sample_rows: [],
        quality_json: {},
        finished_at: new Date().toISOString(),
        triggered_by: asText(req.header('X-AO-USER') || 'ui')
      });
    } catch {}
    return res.status(500).json({ error: 'gold_refresh_failed', details: String(e?.message || e), run_uid });
  } finally {
    client.release();
  }
});

goldBuilderRouter.get('/gold-definitions/:id/lineage', requireDataAdmin, async (req, res) => {
  const client = await pool.connect();
  try {
    const config = await loadGoldRuntimeConfig(client);
    const current = await loadDefinitionById(client, config, Number(req.params.id || 0));
    if (!current) return res.status(404).json({ error: 'not_found', details: 'gold definition not found' });
    return res.json({ ok: true, lineage: buildGoldDependencyGraph(current.definition) });
  } catch (e) {
    return res.status(500).json({ error: 'gold_lineage_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

goldBuilderRouter.get('/gold-definitions/:id/impact', requireDataAdmin, async (req, res) => {
  const client = await pool.connect();
  try {
    const config = await loadGoldRuntimeConfig(client);
    const current = await loadDefinitionById(client, config, Number(req.params.id || 0));
    if (!current) return res.status(404).json({ error: 'not_found', details: 'gold definition not found' });
    const catalog = await buildSourceCatalog(client, config);
    const definition = hydrateDefinitionSources(current.definition, catalog.source_map);
    const compatibility = buildGoldPreviewModel(definition, { sourceCatalogByKey: catalog.source_map, sourceRowsByKey: {} }).compatibility;
    const changedSources = parseChangedSources(req);
    const inferredChanged = compatibility.sources.filter((item) => !item.compatible).map((item) => item.source_key);
    return res.json({
      ok: true,
      impact: analyzeGoldImpact(definition, {
        changedSources: changedSources.length ? changedSources : inferredChanged
      })
    });
  } catch (e) {
    return res.status(500).json({ error: 'gold_impact_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

goldBuilderRouter.get('/gold-definitions/:id/health', requireDataAdmin, async (req, res) => {
  const client = await pool.connect();
  try {
    const config = await loadGoldRuntimeConfig(client);
    const current = await loadDefinitionById(client, config, Number(req.params.id || 0));
    if (!current) return res.status(404).json({ error: 'not_found', details: 'gold definition not found' });
    const [catalog, registryRows, latestRefreshMap] = await Promise.all([
      buildSourceCatalog(client, config),
      loadExternalSourceRows(client, config),
      loadLatestRefreshMap(client, config)
    ]);
    const definition = hydrateDefinitionSources(current.definition, catalog.source_map);
    const previewModel = buildGoldPreviewModel(definition, {
      sourceCatalogByKey: catalog.source_map,
      sourceRowsByKey: {},
      externalSourceStatesByKey: buildExternalSourceStates(registryRows),
      lastRefresh: latestRefreshMap.get(current.id) || null
    });
    return res.json({
      ok: true,
      status: previewModel.status,
      refresh_plan: planGoldRefresh(definition, {
        lastRefreshAt: asText(latestRefreshMap.get(current.id)?.finished_at)
      }),
      materialization_plan: planGoldMaterialization(definition),
      validation: previewModel.validation,
      compatibility: previewModel.compatibility
    });
  } catch (e) {
    return res.status(500).json({ error: 'gold_health_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

goldBuilderRouter.get('/gold-definitions/:id/consumers', requireDataAdmin, async (req, res) => {
  const client = await pool.connect();
  try {
    const config = await loadGoldRuntimeConfig(client);
    const current = await loadDefinitionById(client, config, Number(req.params.id || 0));
    if (!current) return res.status(404).json({ error: 'not_found', details: 'gold definition not found' });
    return res.json({ ok: true, consumers: resolveConsumerDependencies(current.definition) });
  } catch (e) {
    return res.status(500).json({ error: 'gold_consumers_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

export async function bootstrapGoldBuilder() {
  const client = await pool.connect();
  try {
    const config = await loadGoldRuntimeConfig(client);
    await ensureGoldDefinitionsTable(client, config);
    await ensureGoldDefinitionVersionsTable(client, config);
    await ensureGoldExternalSourcesTable(client, config);
    await ensureGoldRefreshRunsTable(client, config);
    await ensureGoldContracts(client, config);
    return { ok: true, effective: config };
  } finally {
    client.release();
  }
}

export const goldBuilderTestkit = {
  loadGoldRuntimeConfig,
  parseDefinitionRow,
  parseRefreshRow,
  buildSourceCatalog,
  loadSourceRowsForDefinition,
  inferSourceVersions,
  hydrateDefinitionSources,
  buildExternalSourceStates,
  stableJson,
  materializeGoldDefinition,
  _testEnsureGoldDefinitionsTable: ensureGoldDefinitionsTable,
  _testEnsureGoldDefinitionVersionsTable: ensureGoldDefinitionVersionsTable,
  _testEnsureGoldExternalSourcesTable: ensureGoldExternalSourcesTable,
  _testEnsureGoldRefreshRunsTable: ensureGoldRefreshRunsTable,
  _testEnsureGoldContracts: ensureGoldContracts,
  _testUpsertDefinitionDraft: upsertDefinitionDraft,
  _testPublishDefinition: publishDefinition,
  _testInsertRefreshRun: insertRefreshRun,
  _testBuildDefinitionSummary: buildDefinitionSummary
};
