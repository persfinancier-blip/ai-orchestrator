import express from 'express';
import { pool } from './db.mjs';

export const tableBuilderRouter = express.Router();
tableBuilderRouter.use(express.json({ limit: '4mb' }));

function requireDataAdmin(req, res, next) {
  const role = (req.header('X-AO-ROLE') || '').trim();
  if (role !== 'data_admin') {
    return res.status(403).json({ error: 'forbidden', details: 'data_admin role required' });
  }
  next();
}

function isIdent(s) {
  return typeof s === 'string' && /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(s);
}

function qi(ident) {
  if (!isIdent(ident)) throw new Error(`invalid_identifier:${ident}`);
  return `"${ident}"`;
}

function qname(schema, table) {
  return `${qi(schema)}.${qi(table)}`;
}

// SQL literal for COMMENT text etc.
function qlit(v) {
  if (v === null || v === undefined) return 'NULL';
  const s = String(v).replace(/'/g, "''");
  return `'${s}'`;
}

const KNOWN_TYPES = new Set(['text', 'int', 'bigint', 'numeric', 'boolean', 'date', 'timestamptz', 'jsonb', 'uuid']);
const SETTINGS_SCHEMA = 'ao_system';
const SETTINGS_TABLE = 'table_settings_store';
const SETTINGS_QNAME = `${qi(SETTINGS_SCHEMA)}.${qi(SETTINGS_TABLE)}`;
const DEFAULT_CONFIG = Object.freeze({
  contracts_schema: 'ao_system',
  contracts_table: 'table_data_contract_versions',
  templates_schema: 'ao_system',
  templates_table: 'table_templates_store',
  api_configs_schema: 'ao_system',
  api_configs_table: 'api_configs_store',
  server_writes_schema: 'ao_system',
  server_writes_table: 'table_server_writes_store'
});
const SETTINGS_CACHE_MS = Number(process.env.AO_SETTINGS_CACHE_MS || 5000);
let settingsCache = { at: 0, value: { ...DEFAULT_CONFIG } };
let PROTECTED_SYSTEM_TABLES = new Set([
  `${SETTINGS_SCHEMA}.${SETTINGS_TABLE}`,
  `${DEFAULT_CONFIG.server_writes_schema}.${DEFAULT_CONFIG.server_writes_table}`
]);
const SETTINGS_REQUIRED_COLUMNS = [
  { name: 'setting_key', types: ['text', 'character varying', 'varchar'] },
  { name: 'setting_value', types: ['jsonb', 'json'] },
  { name: 'scope', types: ['text', 'character varying', 'varchar'] },
  { name: 'description', types: ['text', 'character varying', 'varchar'] },
  { name: 'is_active', types: ['boolean'] },
  { name: 'updated_at', types: ['timestamp with time zone', 'timestamptz', 'timestamp'] },
  { name: 'updated_by', types: ['text', 'character varying', 'varchar'] }
];
const CONTRACTS_REQUIRED_COLUMNS = [
  { name: 'schema_name', types: ['text', 'character varying', 'varchar'] },
  { name: 'table_name', types: ['text', 'character varying', 'varchar'] },
  { name: 'contract_name', types: ['text', 'character varying', 'varchar'] },
  { name: 'version', types: ['integer', 'bigint', 'int'] },
  { name: 'lifecycle_state', types: ['text', 'character varying', 'varchar'] },
  { name: 'deleted_at', types: ['timestamp with time zone', 'timestamptz', 'timestamp'] },
  { name: 'description', types: ['text', 'character varying', 'varchar'] },
  { name: 'columns', types: ['jsonb', 'json'] },
  { name: 'change_reason', types: ['text', 'character varying', 'varchar'] },
  { name: 'changed_by', types: ['text', 'character varying', 'varchar'] },
  { name: 'created_at', types: ['timestamp with time zone', 'timestamptz', 'timestamp'] }
];
const TEMPLATES_REQUIRED_COLUMNS = [
  { name: 'template_name', types: ['text', 'character varying', 'varchar'] },
  { name: 'schema_name', types: ['text', 'character varying', 'varchar'] },
  { name: 'table_name', types: ['text', 'character varying', 'varchar'] },
  { name: 'table_class', types: ['text', 'character varying', 'varchar'] },
  { name: 'description', types: ['text', 'character varying', 'varchar'] },
  { name: 'columns', types: ['jsonb', 'json'] },
  { name: 'partition_enabled', types: ['boolean'] },
  { name: 'partition_column', types: ['text', 'character varying', 'varchar'] },
  { name: 'partition_interval', types: ['text', 'character varying', 'varchar'] }
];
const SERVER_WRITES_REQUIRED_COLUMNS = [
  { name: 'rule_key', types: ['text', 'character varying', 'varchar'] },
  { name: 'target_schema', types: ['text', 'character varying', 'varchar'] },
  { name: 'target_table', types: ['text', 'character varying', 'varchar'] },
  { name: 'operation', types: ['text', 'character varying', 'varchar'] },
  { name: 'payload', types: ['jsonb', 'json'] },
  { name: 'scope', types: ['text', 'character varying', 'varchar'] },
  { name: 'description', types: ['text', 'character varying', 'varchar'] },
  { name: 'is_active', types: ['boolean'] },
  { name: 'updated_at', types: ['timestamp with time zone', 'timestamptz', 'timestamp'] },
  { name: 'updated_by', types: ['text', 'character varying', 'varchar'] }
];
const API_CONFIGS_REQUIRED_COLUMNS = [
  { name: 'api_name', types: ['text', 'character varying', 'varchar'] },
  { name: 'method', types: ['text', 'character varying', 'varchar'] },
  { name: 'base_url', types: ['text', 'character varying', 'varchar'] },
  { name: 'path', types: ['text', 'character varying', 'varchar'] },
  { name: 'headers_json', types: ['jsonb', 'json'] },
  { name: 'query_json', types: ['jsonb', 'json'] },
  { name: 'body_json', types: ['jsonb', 'json'] },
  { name: 'pagination_json', types: ['jsonb', 'json'] },
  { name: 'target_schema', types: ['text', 'character varying', 'varchar'] },
  { name: 'target_table', types: ['text', 'character varying', 'varchar'] },
  { name: 'mapping_json', types: ['jsonb', 'json'] },
  { name: 'description', types: ['text', 'character varying', 'varchar'] },
  { name: 'is_active', types: ['boolean'] },
  { name: 'updated_at', types: ['timestamp with time zone', 'timestamptz', 'timestamp'] },
  { name: 'updated_by', types: ['text', 'character varying', 'varchar'] }
];
const SYSTEM_CONTRACT_COLUMNS = [
  { name: 'ao_source', type: 'text' },
  { name: 'ao_run_id', type: 'text' },
  { name: 'ao_created_at', type: 'timestamptz' },
  { name: 'ao_updated_at', type: 'timestamptz' },
  { name: 'ao_contract_schema', type: 'text' },
  { name: 'ao_contract_name', type: 'text' },
  { name: 'ao_contract_version', type: 'integer' }
];

function normalizeSettingIdent(value, fallback) {
  const v = String(value || '').trim();
  return isIdent(v) ? v : fallback;
}

function contractsQname(config) {
  return `${qi(config.contracts_schema)}.${qi(config.contracts_table)}`;
}

function apiConfigsQname(config) {
  return `${qi(config.api_configs_schema)}.${qi(config.api_configs_table)}`;
}

function syncProtectedSystemTables(config) {
  const next = new Set([`${SETTINGS_SCHEMA}.${SETTINGS_TABLE}`]);
  if (config?.api_configs_schema && config?.api_configs_table) {
    next.add(`${config.api_configs_schema}.${config.api_configs_table}`);
  } else {
    next.add(`${DEFAULT_CONFIG.api_configs_schema}.${DEFAULT_CONFIG.api_configs_table}`);
  }
  if (config?.server_writes_schema && config?.server_writes_table) {
    next.add(`${config.server_writes_schema}.${config.server_writes_table}`);
  } else {
    next.add(`${DEFAULT_CONFIG.server_writes_schema}.${DEFAULT_CONFIG.server_writes_table}`);
  }
  PROTECTED_SYSTEM_TABLES = next;
}

function isProtectedSystemTable(schema, table) {
  return PROTECTED_SYSTEM_TABLES.has(`${String(schema || '').trim()}.${String(table || '').trim()}`);
}

function normalizeTypeName(type) {
  return String(type || '').toLowerCase().trim();
}

function normalizeContractFieldType(type) {
  const t = normalizeTypeName(type);
  if (!t) return '';
  if (t === 'text' || t.includes('character varying') || t === 'varchar') return 'text';
  if (t === 'int' || t === 'integer' || t === 'int4') return 'integer';
  if (t === 'bigint' || t === 'int8') return 'bigint';
  if (t.startsWith('numeric') || t.startsWith('decimal')) return 'numeric';
  if (t === 'boolean' || t === 'bool') return 'boolean';
  if (t === 'date') return 'date';
  if (t === 'timestamptz' || t.includes('timestamp with time zone')) return 'timestamptz';
  if (t === 'jsonb' || t === 'json') return 'jsonb';
  if (t === 'uuid') return 'uuid';
  return '';
}

function normalizeContractColumnsPayload(columns) {
  const list = Array.isArray(columns) ? columns : [];
  const out = [];
  const seen = new Set();
  for (const raw of list) {
    const field_name = String(raw?.field_name || raw?.name || '').trim();
    const field_type = normalizeContractFieldType(raw?.field_type || raw?.type || '');
    const description = String(raw?.description || '').trim();
    if (!isIdent(field_name)) continue;
    if (!field_type) continue;
    const key = field_name.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({ field_name, field_type, description });
  }
  return out;
}

function parseStorageSettingValue(value) {
  if (typeof value === 'string') {
    const raw = value.trim();
    if (!raw) return { schema: '', table: '' };
    const dot = raw.indexOf('.');
    if (dot > 0 && dot < raw.length - 1) {
      return { schema: raw.slice(0, dot).trim(), table: raw.slice(dot + 1).trim() };
    }
    return { schema: '', table: '' };
  }
  const obj = value && typeof value === 'object' && !Array.isArray(value) ? value : {};
  return {
    schema: String(obj.schema ?? obj.schema_name ?? '').trim(),
    table: String(obj.table ?? obj.table_name ?? '').trim()
  };
}

async function hasRequiredColumns(client, schema, table, required) {
  const r = await client.query(
    `
    SELECT column_name AS name, data_type AS type
    FROM information_schema.columns
    WHERE table_schema = $1 AND table_name = $2
    `,
    [schema, table]
  );
  const cols = Array.isArray(r.rows) ? r.rows : [];
  if (!cols.length) return false;
  const map = new Map(cols.map((c) => [String(c.name || '').toLowerCase(), normalizeTypeName(c.type)]));
  for (const need of required) {
    const actual = map.get(String(need.name || '').toLowerCase());
    if (!actual || !need.types.some((t) => actual.includes(String(t)))) return false;
  }
  return true;
}

async function ensureSettingsTable(client) {
  await client.query(`CREATE SCHEMA IF NOT EXISTS ${qi(SETTINGS_SCHEMA)}`);
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${SETTINGS_QNAME} (
      id bigserial PRIMARY KEY,
      setting_key text NOT NULL UNIQUE,
      setting_value jsonb NOT NULL,
      scope text NOT NULL DEFAULT 'global',
      description text NOT NULL DEFAULT '',
      is_active boolean NOT NULL DEFAULT true,
      updated_at timestamptz NOT NULL DEFAULT now(),
      updated_by text NOT NULL DEFAULT 'system'
    )
  `);

  const ok = await hasRequiredColumns(client, SETTINGS_SCHEMA, SETTINGS_TABLE, SETTINGS_REQUIRED_COLUMNS);
  if (!ok) {
    const backupTable = `${SETTINGS_TABLE}__broken_${Date.now()}`;
    await client.query(`ALTER TABLE ${SETTINGS_QNAME} RENAME TO ${qi(backupTable)}`);
    await client.query(`
      CREATE TABLE ${SETTINGS_QNAME} (
        id bigserial PRIMARY KEY,
        setting_key text NOT NULL UNIQUE,
        setting_value jsonb NOT NULL,
        scope text NOT NULL DEFAULT 'global',
        description text NOT NULL DEFAULT '',
        is_active boolean NOT NULL DEFAULT true,
        updated_at timestamptz NOT NULL DEFAULT now(),
        updated_by text NOT NULL DEFAULT 'system'
      )
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS ao_table_settings_store_active_idx
      ON ${SETTINGS_QNAME} (is_active, setting_key)
    `);
  }
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_table_settings_store_active_idx
    ON ${SETTINGS_QNAME} (is_active, setting_key)
  `);
  await ensureSystemContractColumns(client, SETTINGS_QNAME);
}

async function ensureDefaultSettingsRows(client) {
  const defaults = [
    {
      key: 'contracts_storage',
      value: { schema: DEFAULT_CONFIG.contracts_schema, table: DEFAULT_CONFIG.contracts_table },
      description: 'Хранилище версий контрактов данных'
    },
    {
      key: 'templates_storage',
      value: { schema: DEFAULT_CONFIG.templates_schema, table: DEFAULT_CONFIG.templates_table },
      description: 'Хранилище шаблонов таблиц'
    },
    {
      key: 'server_writes_storage',
      value: { schema: DEFAULT_CONFIG.server_writes_schema, table: DEFAULT_CONFIG.server_writes_table },
      description: 'Хранилище правил серверных записей'
    },
    {
      key: 'api_configs_storage',
      value: { schema: DEFAULT_CONFIG.api_configs_schema, table: DEFAULT_CONFIG.api_configs_table },
      description: 'Хранилище преднастроенных API'
    }
  ];

  for (const row of defaults) {
    await client.query(
      `
      INSERT INTO ${SETTINGS_QNAME}
        (setting_key, setting_value, scope, description, is_active, updated_at, updated_by,
         ao_source, ao_run_id, ao_created_at, ao_updated_at, ao_contract_schema, ao_contract_name, ao_contract_version)
      VALUES
        ($1, $2::jsonb, 'global', $3, true, now(), 'system_bootstrap',
         'system_bootstrap', 'settings_bootstrap', now(), now(), $4, $5, 1)
      ON CONFLICT (setting_key) DO NOTHING
      `,
      [
        row.key,
        JSON.stringify(row.value),
        row.description,
        DEFAULT_CONFIG.contracts_schema,
        defaultContractName(SETTINGS_SCHEMA, SETTINGS_TABLE)
      ]
    );
  }
}

function applySettingValue(target, key, value) {
  if (key === 'contracts_storage') {
    const parsed = parseStorageSettingValue(value);
    target.contracts_schema = normalizeSettingIdent(parsed.schema, target.contracts_schema);
    target.contracts_table = normalizeSettingIdent(parsed.table, target.contracts_table);
    return;
  }
  if (key === 'contracts_storage_schema') {
    target.contracts_schema = normalizeSettingIdent(value, target.contracts_schema);
    return;
  }
  if (key === 'contracts_storage_table') {
    target.contracts_table = normalizeSettingIdent(value, target.contracts_table);
    return;
  }
  if (key === 'templates_storage') {
    const parsed = parseStorageSettingValue(value);
    target.templates_schema = normalizeSettingIdent(parsed.schema, target.templates_schema);
    target.templates_table = normalizeSettingIdent(parsed.table, target.templates_table);
    return;
  }
  if (key === 'templates_storage_schema') {
    target.templates_schema = normalizeSettingIdent(value, target.templates_schema);
    return;
  }
  if (key === 'templates_storage_table') {
    target.templates_table = normalizeSettingIdent(value, target.templates_table);
    return;
  }
  if (key === 'server_writes_storage') {
    const parsed = parseStorageSettingValue(value);
    target.server_writes_schema = normalizeSettingIdent(parsed.schema, target.server_writes_schema);
    target.server_writes_table = normalizeSettingIdent(parsed.table, target.server_writes_table);
    return;
  }
  if (key === 'server_writes_storage_schema') {
    target.server_writes_schema = normalizeSettingIdent(value, target.server_writes_schema);
    return;
  }
  if (key === 'server_writes_storage_table') {
    target.server_writes_table = normalizeSettingIdent(value, target.server_writes_table);
    return;
  }
  if (key === 'api_configs_storage') {
    const parsed = parseStorageSettingValue(value);
    target.api_configs_schema = normalizeSettingIdent(parsed.schema, target.api_configs_schema);
    target.api_configs_table = normalizeSettingIdent(parsed.table, target.api_configs_table);
    return;
  }
  if (key === 'api_configs_storage_schema') {
    target.api_configs_schema = normalizeSettingIdent(value, target.api_configs_schema);
    return;
  }
  if (key === 'api_configs_storage_table') {
    target.api_configs_table = normalizeSettingIdent(value, target.api_configs_table);
  }
}

async function loadRuntimeConfig(client, { force = false } = {}) {
  const now = Date.now();
  if (!force && now - settingsCache.at < SETTINGS_CACHE_MS) {
    return settingsCache.value;
  }

  await ensureSettingsTable(client);
  await ensureDefaultSettingsRows(client);
  const next = { ...DEFAULT_CONFIG };
  const r = await client.query(
    `
    SELECT setting_key, setting_value
    FROM ${SETTINGS_QNAME}
    WHERE is_active = true
    `
  );
  for (const row of r.rows || []) {
    const settingKey = String(row?.setting_key || '').trim();
    if (!settingKey) continue;
    applySettingValue(next, settingKey, row?.setting_value);
  }

  const contractsOk = await hasRequiredColumns(
    client,
    next.contracts_schema,
    next.contracts_table,
    CONTRACTS_REQUIRED_COLUMNS
  );
  if (!contractsOk) {
    next.contracts_schema = DEFAULT_CONFIG.contracts_schema;
    next.contracts_table = DEFAULT_CONFIG.contracts_table;
  }

  const templatesOk = await hasRequiredColumns(
    client,
    next.templates_schema,
    next.templates_table,
    TEMPLATES_REQUIRED_COLUMNS
  );
  if (!templatesOk) {
    next.templates_schema = DEFAULT_CONFIG.templates_schema;
    next.templates_table = DEFAULT_CONFIG.templates_table;
  }

  const serverWritesOk = await hasRequiredColumns(
    client,
    next.server_writes_schema,
    next.server_writes_table,
    SERVER_WRITES_REQUIRED_COLUMNS
  );
  if (!serverWritesOk) {
    next.server_writes_schema = DEFAULT_CONFIG.server_writes_schema;
    next.server_writes_table = DEFAULT_CONFIG.server_writes_table;
  }

  const apiConfigsOk = await hasRequiredColumns(
    client,
    next.api_configs_schema,
    next.api_configs_table,
    API_CONFIGS_REQUIRED_COLUMNS
  );
  if (!apiConfigsOk) {
    next.api_configs_schema = DEFAULT_CONFIG.api_configs_schema;
    next.api_configs_table = DEFAULT_CONFIG.api_configs_table;
  }

  const contractsQn = await ensureContractsTable(client, next);
  const templatesQn = await ensureTemplatesStorageTable(client, next);
  await ensureApiConfigsTable(client, next);
  const serverWritesQn = await ensureServerWritesTable(client, next);
  await ensureDefaultServerWriteRules(client, next, serverWritesQn);

  await ensureContractVersionForTable(
    client,
    contractsQn,
    SETTINGS_SCHEMA,
    SETTINGS_TABLE,
    'system_bootstrap:settings_storage',
    'system_bootstrap',
    next.contracts_schema
  );
  await ensureContractVersionForTable(
    client,
    contractsQn,
    next.contracts_schema,
    next.contracts_table,
    'system_bootstrap:contracts_storage',
    'system_bootstrap',
    next.contracts_schema
  );
  await ensureContractVersionForTable(
    client,
    contractsQn,
    next.templates_schema,
    next.templates_table,
    'system_bootstrap:templates_storage',
    'system_bootstrap',
    next.contracts_schema
  );
  await ensureContractVersionForTable(
    client,
    contractsQn,
    next.server_writes_schema,
    next.server_writes_table,
    'system_bootstrap:server_writes_storage',
    'system_bootstrap',
    next.contracts_schema
  );
  await ensureContractVersionForTable(
    client,
    contractsQn,
    next.api_configs_schema,
    next.api_configs_table,
    'system_bootstrap:api_configs_storage',
    'system_bootstrap',
    next.contracts_schema
  );

  syncProtectedSystemTables(next);
  settingsCache = { at: now, value: next };
  return next;
}

async function ensureContractsTable(client, config) {
  const contractsSchema = normalizeSettingIdent(config?.contracts_schema, DEFAULT_CONFIG.contracts_schema);
  const contractsTable = normalizeSettingIdent(config?.contracts_table, DEFAULT_CONFIG.contracts_table);
  const contractsQn = `${qi(contractsSchema)}.${qi(contractsTable)}`;

  await client.query(`CREATE SCHEMA IF NOT EXISTS ${qi(contractsSchema)}`);
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${contractsQn} (
      id bigserial PRIMARY KEY,
      schema_name text NOT NULL,
      table_name text NOT NULL,
      contract_name text NOT NULL,
      version int NOT NULL,
      lifecycle_state text NOT NULL DEFAULT 'active',
      deleted_at timestamptz NULL,
      description text NOT NULL DEFAULT '',
      columns jsonb NOT NULL DEFAULT '[]'::jsonb,
      change_reason text NOT NULL DEFAULT '',
      changed_by text NOT NULL DEFAULT 'ui',
      created_at timestamptz NOT NULL DEFAULT now(),
      UNIQUE(schema_name, table_name, version)
    )
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_table_data_contract_versions_lookup_idx
    ON ${contractsQn} (schema_name, table_name, version DESC)
  `);
  // Normalize legacy data: keep only latest active row per table before enabling unique partial index.
  await client.query(`
    WITH ranked AS (
      SELECT
        id,
        ROW_NUMBER() OVER (
          PARTITION BY schema_name, table_name
          ORDER BY version DESC, created_at DESC, id DESC
        ) AS rn
      FROM ${contractsQn}
      WHERE lifecycle_state = 'active'
    )
    UPDATE ${contractsQn} c
    SET lifecycle_state = 'inactive'
    FROM ranked r
    WHERE c.id = r.id
      AND r.rn > 1
  `);
  await client.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS ao_table_data_contract_versions_one_active_idx
    ON ${contractsQn} (schema_name, table_name)
    WHERE lifecycle_state = 'active'
  `);
  await ensureSystemContractColumns(client, contractsQn);

  const ok = await hasRequiredColumns(client, contractsSchema, contractsTable, CONTRACTS_REQUIRED_COLUMNS);
  if (ok) return contractsQn;

  const isDefault =
    contractsSchema === DEFAULT_CONFIG.contracts_schema &&
    contractsTable === DEFAULT_CONFIG.contracts_table;
  if (isDefault) {
    throw new Error('contracts_storage_invalid_structure');
  }

  return ensureContractsTable(client, DEFAULT_CONFIG);
}

async function ensureTemplatesStorageTable(client, config) {
  const templatesSchema = normalizeSettingIdent(config?.templates_schema, DEFAULT_CONFIG.templates_schema);
  const templatesTable = normalizeSettingIdent(config?.templates_table, DEFAULT_CONFIG.templates_table);
  const templatesQn = `${qi(templatesSchema)}.${qi(templatesTable)}`;

  await client.query(`CREATE SCHEMA IF NOT EXISTS ${qi(templatesSchema)}`);
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${templatesQn} (
      id bigserial PRIMARY KEY,
      template_name text NOT NULL,
      schema_name text NOT NULL,
      table_name text NOT NULL,
      table_class text NOT NULL DEFAULT 'custom',
      description text NOT NULL DEFAULT '',
      columns jsonb NOT NULL DEFAULT '[]'::jsonb,
      partition_enabled boolean NOT NULL DEFAULT false,
      partition_column text NOT NULL DEFAULT '',
      partition_interval text NOT NULL DEFAULT 'day'
    )
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_table_templates_store_name_idx
    ON ${templatesQn} (template_name)
  `);
  await ensureSystemContractColumns(client, templatesQn);
  return templatesQn;
}

async function ensureApiConfigsTable(client, config) {
  const schema = normalizeSettingIdent(config?.api_configs_schema, DEFAULT_CONFIG.api_configs_schema);
  const table = normalizeSettingIdent(config?.api_configs_table, DEFAULT_CONFIG.api_configs_table);
  const qn = `${qi(schema)}.${qi(table)}`;

  await client.query(`CREATE SCHEMA IF NOT EXISTS ${qi(schema)}`);
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${qn} (
      id bigserial PRIMARY KEY,
      api_name text NOT NULL,
      method text NOT NULL DEFAULT 'GET',
      base_url text NOT NULL DEFAULT '',
      path text NOT NULL DEFAULT '',
      headers_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      query_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      body_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      pagination_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      target_schema text NOT NULL DEFAULT '',
      target_table text NOT NULL DEFAULT '',
      mapping_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      description text NOT NULL DEFAULT '',
      is_active boolean NOT NULL DEFAULT true,
      updated_at timestamptz NOT NULL DEFAULT now(),
      updated_by text NOT NULL DEFAULT 'system'
    )
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_api_configs_store_name_idx
    ON ${qn} (api_name)
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_api_configs_store_active_idx
    ON ${qn} (is_active, api_name)
  `);
  await ensureSystemContractColumns(client, qn);
  return qn;
}

async function ensureServerWritesTable(client, config) {
  const schema = normalizeSettingIdent(config?.server_writes_schema, DEFAULT_CONFIG.server_writes_schema);
  const table = normalizeSettingIdent(config?.server_writes_table, DEFAULT_CONFIG.server_writes_table);
  const qn = `${qi(schema)}.${qi(table)}`;

  await client.query(`CREATE SCHEMA IF NOT EXISTS ${qi(schema)}`);
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${qn} (
      id bigserial PRIMARY KEY,
      rule_key text NOT NULL UNIQUE,
      target_schema text NOT NULL,
      target_table text NOT NULL,
      operation text NOT NULL,
      payload jsonb NOT NULL DEFAULT '{}'::jsonb,
      scope text NOT NULL DEFAULT 'global',
      description text NOT NULL DEFAULT '',
      is_active boolean NOT NULL DEFAULT true,
      updated_at timestamptz NOT NULL DEFAULT now(),
      updated_by text NOT NULL DEFAULT 'system'
    )
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_table_server_writes_store_active_idx
    ON ${qn} (is_active, rule_key)
  `);
  await ensureSystemContractColumns(client, qn);
  return qn;
}

async function ensureDefaultServerWriteRules(client, config, serverWritesQn) {
  const contractsSchema = normalizeSettingIdent(config?.contracts_schema, DEFAULT_CONFIG.contracts_schema);
  const contractsTable = normalizeSettingIdent(config?.contracts_table, DEFAULT_CONFIG.contracts_table);
  const templatesSchema = normalizeSettingIdent(config?.templates_schema, DEFAULT_CONFIG.templates_schema);
  const templatesTable = normalizeSettingIdent(config?.templates_table, DEFAULT_CONFIG.templates_table);
  const apiSchema = normalizeSettingIdent(config?.api_configs_schema, DEFAULT_CONFIG.api_configs_schema);
  const apiTable = normalizeSettingIdent(config?.api_configs_table, DEFAULT_CONFIG.api_configs_table);
  const writesSchema = normalizeSettingIdent(config?.server_writes_schema, DEFAULT_CONFIG.server_writes_schema);
  const writesTable = normalizeSettingIdent(config?.server_writes_table, DEFAULT_CONFIG.server_writes_table);
  const contractName = defaultContractName(writesSchema, writesTable);

  const defaults = [
    {
      rule_key: 'settings_defaults_bootstrap',
      target_schema: SETTINGS_SCHEMA,
      target_table: SETTINGS_TABLE,
      operation: 'upsert_settings_defaults',
      payload: { setting_keys: ['contracts_storage', 'templates_storage', 'api_configs_storage', 'server_writes_storage'] },
      description: 'Сервер поддерживает обязательные ключи в таблице настроек'
    },
    {
      rule_key: 'contracts_auto_create',
      target_schema: contractsSchema,
      target_table: contractsTable,
      operation: 'insert_contract_version',
      payload: { trigger: ['table_create', 'column_add', 'column_drop', 'table_rename', 'system_bootstrap'] },
      description: 'Сервер автоматически создает версии контрактов данных'
    },
    {
      rule_key: 'templates_storage_sync',
      target_schema: templatesSchema,
      target_table: templatesTable,
      operation: 'upsert_template',
      payload: { trigger: ['template_add', 'template_save', 'template_delete'] },
      description: 'Сервер сохраняет шаблоны таблиц в подключенное хранилище'
    },
    {
      rule_key: 'api_configs_storage_sync',
      target_schema: apiSchema,
      target_table: apiTable,
      operation: 'upsert_api_config',
      payload: { trigger: ['api_add', 'api_save', 'api_delete'] },
      description: 'Сервер сохраняет преднастроенные API в подключенное хранилище'
    },
    {
      rule_key: 'server_writes_rules_self',
      target_schema: writesSchema,
      target_table: writesTable,
      operation: 'upsert_rule',
      payload: { trigger: ['bootstrap', 'settings_reload'] },
      description: 'Сервер поддерживает актуальные правила записей'
    }
  ];

  for (const row of defaults) {
    await client.query(
      `
      INSERT INTO ${serverWritesQn}
        (rule_key, target_schema, target_table, operation, payload, scope, description, is_active, updated_at, updated_by,
         ao_source, ao_run_id, ao_created_at, ao_updated_at, ao_contract_schema, ao_contract_name, ao_contract_version)
      VALUES
        ($1, $2, $3, $4, $5::jsonb, 'global', $6, true, now(), 'system_bootstrap',
         'system_bootstrap', 'server_rules_bootstrap', now(), now(), $7, $8, 1)
      ON CONFLICT (rule_key)
      DO UPDATE SET
        target_schema = EXCLUDED.target_schema,
        target_table = EXCLUDED.target_table,
        operation = EXCLUDED.operation,
        payload = EXCLUDED.payload,
        description = EXCLUDED.description,
        is_active = true,
        updated_at = now(),
        updated_by = 'system_bootstrap',
        ao_source = EXCLUDED.ao_source,
        ao_run_id = EXCLUDED.ao_run_id,
        ao_updated_at = now(),
        ao_contract_schema = EXCLUDED.ao_contract_schema,
        ao_contract_name = EXCLUDED.ao_contract_name,
        ao_contract_version = EXCLUDED.ao_contract_version
      `,
      [
        row.rule_key,
        row.target_schema,
        row.target_table,
        row.operation,
        JSON.stringify(row.payload),
        row.description,
        contractsSchema,
        contractName
      ]
    );
  }
}

async function ensureSystemContractColumns(client, tableQname) {
  for (const c of SYSTEM_CONTRACT_COLUMNS) {
    await client.query(`ALTER TABLE ${tableQname} ADD COLUMN IF NOT EXISTS ${qi(c.name)} ${c.type}`);
  }
}

function defaultContractName(schema, table) {
  return `${schema}.${table}`;
}

async function getTableColumnsSnapshot(client, schema, table) {
  const r = await client.query(
    `
    SELECT
      c.column_name AS field_name,
      c.data_type AS field_type,
      COALESCE(d.description, '') AS description
    FROM information_schema.columns c
    LEFT JOIN pg_catalog.pg_namespace n
      ON n.nspname = c.table_schema
    LEFT JOIN pg_catalog.pg_class cls
      ON cls.relnamespace = n.oid
     AND cls.relname = c.table_name
    LEFT JOIN pg_catalog.pg_attribute a
      ON a.attrelid = cls.oid
     AND a.attname = c.column_name
    LEFT JOIN pg_catalog.pg_description d
      ON d.objoid = cls.oid
     AND d.objsubid = a.attnum
    WHERE c.table_schema = $1
      AND c.table_name = $2
    ORDER BY c.ordinal_position
    `,
    [schema, table]
  );
  return (r.rows || []).map((x) => ({
    field_name: String(x.field_name || ''),
    field_type: String(x.field_type || ''),
    description: String(x.description || '')
  }));
}

async function getTableDescription(client, schema, table) {
  const r = await client.query(
    `
    SELECT COALESCE(obj_description(c.oid), '') AS description
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = $1 AND c.relname = $2
    LIMIT 1
    `,
    [schema, table]
  );
  return String(r.rows?.[0]?.description || '');
}

async function getNextContractVersion(client, schema, table) {
  const config = await loadRuntimeConfig(client);
  const contractsQn = await ensureContractsTable(client, config);
  return getNextContractVersionWithQn(client, contractsQn, schema, table);
}

async function getNextContractVersionWithQn(client, contractsQn, schema, table) {
  const r = await client.query(
    `SELECT COALESCE(MAX(version), 0) AS max_v FROM ${contractsQn} WHERE schema_name = $1 AND table_name = $2`,
    [schema, table]
  );
  const maxV = Number(r.rows?.[0]?.max_v || 0);
  return Number.isFinite(maxV) ? maxV + 1 : 1;
}

async function createContractVersion(client, { schema, table, description, columns, reason, by }) {
  const config = await loadRuntimeConfig(client);
  const contractsQn = await ensureContractsTable(client, config);
  return createContractVersionWithQn(client, contractsQn, {
    schema,
    table,
    description,
    columns,
    reason,
    by,
    contracts_schema: config.contracts_schema
  });
}

async function createContractVersionWithQn(
  client,
  contractsQn,
  { schema, table, description, columns, reason, by, contracts_schema = DEFAULT_CONFIG.contracts_schema }
) {
  const version = await getNextContractVersionWithQn(client, contractsQn, schema, table);
  await client.query(
    `
    UPDATE ${contractsQn}
    SET lifecycle_state = 'inactive'
    WHERE schema_name = $1
      AND table_name = $2
      AND lifecycle_state = 'active'
    `,
    [schema, table]
  );
  await client.query(
    `
    INSERT INTO ${contractsQn}
      (schema_name, table_name, contract_name, version, lifecycle_state, deleted_at, description, columns, change_reason, changed_by,
       ao_source, ao_run_id, ao_created_at, ao_updated_at, ao_contract_schema, ao_contract_name, ao_contract_version)
    VALUES
      ($1, $2, $3, $4, 'active', NULL, $5, $6::jsonb, $7, $8,
       'table_builder', $9, now(), now(), $10, $3, $4)
    `,
    [
      schema,
      table,
      defaultContractName(schema, table),
      version,
      String(description || ''),
      JSON.stringify(Array.isArray(columns) ? columns : []),
      String(reason || ''),
      String(by || 'ui'),
      `${String(reason || 'contract_update').replace(/[^a-zA-Z0-9_:-]/g, '_')}_${Date.now()}`,
      String(contracts_schema || DEFAULT_CONFIG.contracts_schema)
    ]
  );
  return version;
}

async function createContractVersionFromTable(client, schema, table, reason, by) {
  const cols = await getTableColumnsSnapshot(client, schema, table);
  const description = await getTableDescription(client, schema, table);
  return createContractVersion(client, { schema, table, description, columns: cols, reason, by });
}

async function hasActiveContractVersion(client, contractsQn, schema, table) {
  const r = await client.query(
    `
    SELECT 1
    FROM ${contractsQn}
    WHERE schema_name = $1
      AND table_name = $2
      AND lifecycle_state = 'active'
    LIMIT 1
    `,
    [schema, table]
  );
  return Boolean(r.rows?.length);
}

async function ensureContractVersionForTable(client, contractsQn, schema, table, reason, by, contractsSchema) {
  const exists = await hasActiveContractVersion(client, contractsQn, schema, table);
  if (exists) return false;
  const cols = await getTableColumnsSnapshot(client, schema, table);
  const description = await getTableDescription(client, schema, table);
  await createContractVersionWithQn(client, contractsQn, {
    schema,
    table,
    description,
    columns: cols,
    reason,
    by,
    contracts_schema: contractsSchema
  });
  return true;
}

async function markContractsDeletedByTableDrop(client, schema, table) {
  const config = await loadRuntimeConfig(client);
  const contractsQn = await ensureContractsTable(client, config);
  await client.query(
    `
    UPDATE ${contractsQn}
    SET lifecycle_state = 'table_deleted', deleted_at = now()
    WHERE schema_name = $1
      AND table_name = $2
      AND lifecycle_state <> 'deleted_by_user'
    `,
    [schema, table]
  );
}

async function getLatestContractMeta(client, schema, table) {
  const config = await loadRuntimeConfig(client);
  const contractsQn = await ensureContractsTable(client, config);
  const r = await client.query(
    `
    SELECT contract_name, version
    FROM ${contractsQn}
    WHERE schema_name = $1
      AND table_name = $2
      AND lifecycle_state = 'active'
    ORDER BY version DESC, created_at DESC, id DESC
    LIMIT 1
    `,
    [schema, table]
  );
  let row = r.rows?.[0] || null;
  if (!row) {
    const fallback = await client.query(
      `
      SELECT contract_name, version
      FROM ${contractsQn}
      WHERE schema_name = $1
        AND table_name = $2
        AND lifecycle_state <> 'deleted_by_user'
      ORDER BY version DESC, created_at DESC, id DESC
      LIMIT 1
      `,
      [schema, table]
    );
    row = fallback.rows?.[0] || null;
  }
  if (!row) return null;
  return {
    contract_name: String(row.contract_name || defaultContractName(schema, table)),
    version: Number(row.version || 1),
    contracts_schema: config.contracts_schema
  };
}

async function enrichRowWithContractFields(client, schema, table, sourceRow) {
  const row = sourceRow && typeof sourceRow === 'object' && !Array.isArray(sourceRow) ? { ...sourceRow } : {};
  const cols = await getTableColumnsSnapshot(client, schema, table);
  const colSet = new Set(cols.map((c) => String(c.field_name || '').toLowerCase()));
  const meta = await getLatestContractMeta(client, schema, table);
  const nowIso = new Date().toISOString();
  const autoRunId = `run_${Date.now()}`;

  // Always populate technical lineage fields when such columns exist.
  if (colSet.has('ao_source') && row.ao_source == null) {
    row.ao_source = 'table_builder';
  }
  if (colSet.has('ao_run_id') && row.ao_run_id == null) {
    row.ao_run_id = autoRunId;
  }
  if (colSet.has('ao_created_at') && row.ao_created_at == null) {
    row.ao_created_at = nowIso;
  }
  if (colSet.has('ao_updated_at') && row.ao_updated_at == null) {
    row.ao_updated_at = nowIso;
  }

  if (!meta) return row;

  if (colSet.has('ao_contract_schema') && row.ao_contract_schema == null) {
    row.ao_contract_schema = meta.contracts_schema || DEFAULT_CONFIG.contracts_schema;
  }
  if (colSet.has('ao_contract_name') && row.ao_contract_name == null) {
    row.ao_contract_name = meta.contract_name;
  }
  if (colSet.has('ao_contract_version') && row.ao_contract_version == null) {
    row.ao_contract_version = Number.isFinite(meta.version) ? Math.trunc(meta.version) : 1;
  }
  return row;
}

function normalizeColumns(columns) {
  const cols = Array.isArray(columns) ? columns : [];
  const out = [];
  for (const c of cols) {
    const field_name = String(c?.field_name || '').trim();
    const field_type = String(c?.field_type || '').trim();
    const description = String(c?.description || '').trim();
    if (!field_name) continue;
    if (!isIdent(field_name)) throw new Error(`invalid_field_name:${field_name}`);
    if (!KNOWN_TYPES.has(field_type)) throw new Error(`invalid_field_type:${field_type}`);
    out.push({ field_name, field_type, description });
  }
  if (!out.length) throw new Error('no_columns');
  return out;
}

async function listExistingTables(client) {
  const r = await client.query(`
    SELECT n.nspname AS schema_name, c.relname AS table_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r','p')
      AND n.nspname NOT IN ('pg_catalog','information_schema')
    ORDER BY 1,2
  `);
  return r.rows || [];
}

tableBuilderRouter.get('/tables', async (_req, res) => {
  const client = await pool.connect();
  try {
    const existing_tables = await listExistingTables(client);
    return res.json({ drafts: [], existing_tables });
  } catch (e) {
    return res.status(500).json({ error: 'tables_list_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

tableBuilderRouter.get('/settings', requireDataAdmin, async (_req, res) => {
  const client = await pool.connect();
  try {
    await ensureSettingsTable(client);
    const rows = await client.query(
      `
      SELECT setting_key, setting_value, scope, description, is_active, updated_at, updated_by
      FROM ${SETTINGS_QNAME}
      ORDER BY setting_key
      `
    );
    const effective = await loadRuntimeConfig(client, { force: true });
    return res.json({ ok: true, defaults: DEFAULT_CONFIG, effective, rows: rows.rows || [] });
  } catch (e) {
    return res.status(500).json({ error: 'settings_list_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

tableBuilderRouter.get('/settings/effective', async (_req, res) => {
  const client = await pool.connect();
  try {
    const effective = await loadRuntimeConfig(client, { force: true });
    return res.json({ ok: true, effective });
  } catch (e) {
    return res.status(500).json({ error: 'settings_effective_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

tableBuilderRouter.post('/settings/upsert', requireDataAdmin, async (req, res) => {
  const setting_key = String(req.body?.setting_key || '').trim();
  const scope = String(req.body?.scope || 'global').trim() || 'global';
  const description = String(req.body?.description || '').trim();
  const updated_by = String(req.body?.updated_by || req.header('X-AO-ROLE') || 'ui').trim();
  const is_active = req.body?.is_active === undefined ? true : Boolean(req.body?.is_active);
  const setting_value = req.body?.setting_value;

  if (!setting_key) {
    return res.status(400).json({ error: 'bad_request', details: 'setting_key is required' });
  }
  if (setting_value === undefined) {
    return res.status(400).json({ error: 'bad_request', details: 'setting_value is required' });
  }

  const client = await pool.connect();
  try {
    await ensureSettingsTable(client);
    await client.query(
      `
      INSERT INTO ${SETTINGS_QNAME}
        (setting_key, setting_value, scope, description, is_active, updated_by, updated_at,
         ao_source, ao_run_id, ao_created_at, ao_updated_at, ao_contract_schema, ao_contract_name, ao_contract_version)
      VALUES
        ($1, $2::jsonb, $3, $4, $5, $6, now(),
         'settings_api', $7, now(), now(), $8, $9, 1)
      ON CONFLICT (setting_key)
      DO UPDATE SET
        setting_value = EXCLUDED.setting_value,
        scope = EXCLUDED.scope,
        description = EXCLUDED.description,
        is_active = EXCLUDED.is_active,
        updated_by = EXCLUDED.updated_by,
        updated_at = now(),
        ao_source = EXCLUDED.ao_source,
        ao_run_id = EXCLUDED.ao_run_id,
        ao_updated_at = now(),
        ao_contract_schema = EXCLUDED.ao_contract_schema,
        ao_contract_name = EXCLUDED.ao_contract_name,
        ao_contract_version = EXCLUDED.ao_contract_version
      `,
      [
        setting_key,
        JSON.stringify(setting_value),
        scope,
        description,
        is_active,
        updated_by,
        `settings_upsert_${Date.now()}`,
        DEFAULT_CONFIG.contracts_schema,
        defaultContractName(SETTINGS_SCHEMA, SETTINGS_TABLE)
      ]
    );

    const effective = await loadRuntimeConfig(client, { force: true });
    await ensureContractsTable(client, effective);
    await ensureTemplatesStorageTable(client, effective);
    await ensureApiConfigsTable(client, effective);
    await ensureServerWritesTable(client, effective);
    return res.json({ ok: true, setting_key, effective });
  } catch (e) {
    return res.status(500).json({ error: 'settings_upsert_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

tableBuilderRouter.post('/settings/reload', requireDataAdmin, async (_req, res) => {
  const client = await pool.connect();
  try {
    const effective = await loadRuntimeConfig(client, { force: true });
    await ensureContractsTable(client, effective);
    await ensureTemplatesStorageTable(client, effective);
    await ensureApiConfigsTable(client, effective);
    await ensureServerWritesTable(client, effective);
    return res.json({ ok: true, effective });
  } catch (e) {
    return res.status(500).json({ error: 'settings_reload_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

tableBuilderRouter.get('/contracts', requireDataAdmin, async (req, res) => {
  const schema = String(req.query.schema || '').trim();
  const table = String(req.query.table || '').trim();
  if (!isIdent(schema) || !isIdent(table)) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid schema/table' });
  }

  const client = await pool.connect();
  try {
    const config = await loadRuntimeConfig(client);
    const contractsQn = await ensureContractsTable(client, config);
    const r = await client.query(
      `
      SELECT
        schema_name,
        table_name,
        contract_name,
        version,
        lifecycle_state,
        deleted_at,
        description,
        columns,
        change_reason,
        changed_by,
        created_at
      FROM ${contractsQn}
      WHERE schema_name = $1 AND table_name = $2
      ORDER BY version DESC
      `,
      [schema, table]
    );
    return res.json({ contracts: r.rows || [] });
  } catch (e) {
    return res.status(500).json({ error: 'contracts_list_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

tableBuilderRouter.post('/contracts/version/delete', requireDataAdmin, async (req, res) => {
  const schema = String(req.body?.schema || '').trim();
  const table = String(req.body?.table || '').trim();
  const version = Number(req.body?.version || 0);
  if (!isIdent(schema) || !isIdent(table) || !Number.isFinite(version) || version <= 0) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid schema/table/version' });
  }

  const client = await pool.connect();
  try {
    const config = await loadRuntimeConfig(client);
    const contractsQn = await ensureContractsTable(client, config);
    const r = await client.query(
      `
      UPDATE ${contractsQn}
      SET lifecycle_state = 'deleted_by_user', deleted_at = now()
      WHERE schema_name = $1 AND table_name = $2 AND version = $3
      `,
      [schema, table, Math.trunc(version)]
    );
    return res.json({ ok: true, schema, table, version: Math.trunc(version), affected: r.rowCount || 0 });
  } catch (e) {
    return res.status(500).json({ error: 'contract_delete_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

tableBuilderRouter.get('/api-configs', requireDataAdmin, async (_req, res) => {
  const client = await pool.connect();
  try {
    const config = await loadRuntimeConfig(client);
    const qn = apiConfigsQname(config);
    await ensureApiConfigsTable(client, config);
    const r = await client.query(
      `
      SELECT
        id,
        api_name,
        method,
        base_url,
        path,
        headers_json,
        query_json,
        body_json,
        pagination_json,
        target_schema,
        target_table,
        mapping_json,
        description,
        is_active,
        updated_at,
        updated_by
      FROM ${qn}
      WHERE is_active = true
      ORDER BY updated_at DESC, id DESC
      `
    );
    return res.json({ api_configs: r.rows || [] });
  } catch (e) {
    return res.status(500).json({ error: 'api_configs_list_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

tableBuilderRouter.post('/api-configs/upsert', requireDataAdmin, async (req, res) => {
  const idRaw = req.body?.id;
  const id = Number(idRaw || 0);
  const api_name = String(req.body?.api_name || '').trim();
  const method = String(req.body?.method || 'GET').trim().toUpperCase();
  const base_url = String(req.body?.base_url || '').trim();
  const path = String(req.body?.path || '').trim();
  const target_schema = String(req.body?.target_schema || '').trim();
  const target_table = String(req.body?.target_table || '').trim();
  const description = String(req.body?.description || '').trim();
  const is_active = req.body?.is_active === undefined ? true : Boolean(req.body?.is_active);
  const updated_by = String(req.body?.updated_by || req.header('X-AO-ROLE') || 'ui').trim();

  if (!api_name) return res.status(400).json({ error: 'bad_request', details: 'api_name is required' });
  if (!['GET', 'POST', 'PUT', 'PATCH', 'DELETE'].includes(method)) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid method' });
  }

  const toJson = (v) => (v === undefined ? {} : v);
  const headers_json = toJson(req.body?.headers_json);
  const query_json = toJson(req.body?.query_json);
  const body_json = toJson(req.body?.body_json);
  const pagination_json = toJson(req.body?.pagination_json);
  const mapping_json = toJson(req.body?.mapping_json);

  const client = await pool.connect();
  try {
    const config = await loadRuntimeConfig(client);
    const qn = apiConfigsQname(config);
    await ensureApiConfigsTable(client, config);
    const runId = `api_config_upsert_${Date.now()}`;
    const contractName = defaultContractName(config.api_configs_schema, config.api_configs_table);

    if (Number.isFinite(id) && id > 0) {
      const r = await client.query(
        `
        UPDATE ${qn}
        SET
          api_name = $2,
          method = $3,
          base_url = $4,
          path = $5,
          headers_json = $6::jsonb,
          query_json = $7::jsonb,
          body_json = $8::jsonb,
          pagination_json = $9::jsonb,
          target_schema = $10,
          target_table = $11,
          mapping_json = $12::jsonb,
          description = $13,
          is_active = $14,
          updated_at = now(),
          updated_by = $15,
          ao_source = 'api_configs_api',
          ao_run_id = $16,
          ao_updated_at = now(),
          ao_contract_schema = $17,
          ao_contract_name = $18,
          ao_contract_version = 1
        WHERE id = $1
        RETURNING id
        `,
        [
          Math.trunc(id),
          api_name,
          method,
          base_url,
          path,
          JSON.stringify(headers_json),
          JSON.stringify(query_json),
          JSON.stringify(body_json),
          JSON.stringify(pagination_json),
          target_schema,
          target_table,
          JSON.stringify(mapping_json),
          description,
          is_active,
          updated_by,
          runId,
          config.contracts_schema,
          contractName
        ]
      );
      if (!r.rows?.length) return res.status(404).json({ error: 'not_found', details: 'api_config id not found' });
      return res.json({ ok: true, id: Number(r.rows[0].id || 0), updated: true });
    }

    const r = await client.query(
      `
      INSERT INTO ${qn}
        (api_name, method, base_url, path, headers_json, query_json, body_json, pagination_json, target_schema, target_table,
         mapping_json, description, is_active, updated_at, updated_by,
         ao_source, ao_run_id, ao_created_at, ao_updated_at, ao_contract_schema, ao_contract_name, ao_contract_version)
      VALUES
        ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7::jsonb, $8::jsonb, $9, $10, $11::jsonb, $12, $13, now(), $14,
         'api_configs_api', $15, now(), now(), $16, $17, 1)
      RETURNING id
      `,
      [
        api_name,
        method,
        base_url,
        path,
        JSON.stringify(headers_json),
        JSON.stringify(query_json),
        JSON.stringify(body_json),
        JSON.stringify(pagination_json),
        target_schema,
        target_table,
        JSON.stringify(mapping_json),
        description,
        is_active,
        updated_by,
        runId,
        config.contracts_schema,
        contractName
      ]
    );
    return res.json({ ok: true, id: Number(r.rows?.[0]?.id || 0), created: true });
  } catch (e) {
    return res.status(500).json({ error: 'api_config_upsert_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

tableBuilderRouter.post('/api-configs/delete', requireDataAdmin, async (req, res) => {
  const id = Number(req.body?.id || 0);
  if (!Number.isFinite(id) || id <= 0) return res.status(400).json({ error: 'bad_request', details: 'invalid id' });

  const client = await pool.connect();
  try {
    const config = await loadRuntimeConfig(client);
    const qn = apiConfigsQname(config);
    await ensureApiConfigsTable(client, config);
    const r = await client.query(
      `
      UPDATE ${qn}
      SET is_active = false, updated_at = now(), updated_by = $2, ao_updated_at = now()
      WHERE id = $1
      RETURNING id
      `,
      [Math.trunc(id), String(req.header('X-AO-ROLE') || 'ui')]
    );
    if (!r.rows?.length) return res.status(404).json({ error: 'not_found', details: 'api_config id not found' });
    return res.json({ ok: true, id: Number(r.rows[0].id || 0), deleted: true });
  } catch (e) {
    return res.status(500).json({ error: 'api_config_delete_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

tableBuilderRouter.post('/contracts/apply-version', requireDataAdmin, async (req, res) => {
  const schema = String(req.body?.schema || '').trim();
  const table = String(req.body?.table || '').trim();
  const targetVersion = Number(req.body?.target_version || 0);
  if (!isIdent(schema) || !isIdent(table) || !Number.isFinite(targetVersion) || targetVersion <= 0) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid schema/table/target_version' });
  }
  if (isProtectedSystemTable(schema, table)) {
    return res.status(403).json({ error: 'forbidden', details: 'protected_system_table' });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const config = await loadRuntimeConfig(client);
    const contractsQn = await ensureContractsTable(client, config);

    const targetRes = await client.query(
      `
      SELECT version, description, columns
      FROM ${contractsQn}
      WHERE schema_name = $1
        AND table_name = $2
        AND version = $3
        AND lifecycle_state <> 'deleted_by_user'
      LIMIT 1
      `,
      [schema, table, Math.trunc(targetVersion)]
    );
    const target = targetRes.rows?.[0] || null;
    if (!target) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'contract_not_found', details: 'target contract version not found' });
    }

    const targetColumns = normalizeContractColumnsPayload(target.columns);
    const currentColumns = await getTableColumnsSnapshot(client, schema, table);

    const currentMap = new Map(currentColumns.map((c) => [String(c.field_name || '').toLowerCase(), c]));
    const targetMap = new Map(targetColumns.map((c) => [String(c.field_name || '').toLowerCase(), c]));

    const toAdd = [];
    const toDrop = [];
    const toDescribe = [];

    for (const tc of targetColumns) {
      const key = tc.field_name.toLowerCase();
      const cc = currentMap.get(key);
      if (!cc) {
        toAdd.push(tc);
        continue;
      }
      const currDesc = String(cc.description || '').trim();
      const nextDesc = String(tc.description || '').trim();
      if (currDesc !== nextDesc) {
        toDescribe.push({ name: tc.field_name, description: nextDesc });
      }
    }

    for (const cc of currentColumns) {
      const key = String(cc.field_name || '').toLowerCase();
      if (!targetMap.has(key)) {
        toDrop.push(cc.field_name);
      }
    }

    for (const c of toAdd) {
      await client.query(`ALTER TABLE ${qname(schema, table)} ADD COLUMN IF NOT EXISTS ${qi(c.field_name)} ${c.field_type}`);
      await client.query(`COMMENT ON COLUMN ${qname(schema, table)}.${qi(c.field_name)} IS ${qlit(c.description)}`);
    }

    for (const name of toDrop) {
      await client.query(`ALTER TABLE ${qname(schema, table)} DROP COLUMN IF EXISTS ${qi(name)}`);
    }

    for (const d of toDescribe) {
      await client.query(`COMMENT ON COLUMN ${qname(schema, table)}.${qi(d.name)} IS ${qlit(d.description)}`);
    }

    const mark = `aligned_to_contract:v${Math.trunc(targetVersion)}`;
    const baseDescription = String(target.description || '').trim();
    const nextTableDescription = baseDescription ? `${baseDescription} | ${mark}` : mark;
    await client.query(`COMMENT ON TABLE ${qname(schema, table)} IS ${qlit(nextTableDescription)}`);

    const newVersion = await createContractVersionFromTable(
      client,
      schema,
      table,
      mark,
      req.header('X-AO-ROLE') || 'ui'
    );

    await client.query('COMMIT');
    return res.json({
      ok: true,
      schema,
      table,
      target_version: Math.trunc(targetVersion),
      new_version: Number(newVersion || 0),
      added_columns: toAdd.map((x) => x.field_name),
      dropped_columns: toDrop,
      described_columns: toDescribe.map((x) => x.name)
    });
  } catch (e) {
    try {
      await client.query('ROLLBACK');
    } catch {}
    return res.status(500).json({ error: 'apply_contract_version_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

tableBuilderRouter.get('/columns', async (req, res) => {
  const schema = String(req.query.schema || '').trim();
  const table = String(req.query.table || '').trim();

  if (!isIdent(schema) || !isIdent(table)) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid schema/table' });
  }

  const client = await pool.connect();
  try {
    const q = `
      SELECT
        c.column_name AS name,
        c.data_type AS type,
        d.description AS description,
        (c.is_nullable = 'YES') AS is_nullable
      FROM information_schema.columns c
      LEFT JOIN pg_catalog.pg_namespace n
        ON n.nspname = c.table_schema
      LEFT JOIN pg_catalog.pg_class cls
        ON cls.relnamespace = n.oid
       AND cls.relname = c.table_name
      LEFT JOIN pg_catalog.pg_attribute a
        ON a.attrelid = cls.oid
       AND a.attname = c.column_name
      LEFT JOIN pg_catalog.pg_description d
        ON d.objoid = cls.oid
       AND d.objsubid = a.attnum
      WHERE c.table_schema = $1
        AND c.table_name = $2
      ORDER BY c.ordinal_position
    `;
    const r = await client.query(q, [schema, table]);
    return res.json({ columns: r.rows || [] });
  } catch (e) {
    return res.status(500).json({ error: 'columns_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

tableBuilderRouter.get('/preview', async (req, res) => {
  const schema = String(req.query.schema || '').trim();
  const table = String(req.query.table || '').trim();
  const limit = Math.max(1, Math.min(50, Number(req.query.limit || 5)));

  if (!isIdent(schema) || !isIdent(table)) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid schema/table' });
  }

  const client = await pool.connect();
  try {
    const q = `SELECT ctid::text AS __ctid, * FROM ${qname(schema, table)} LIMIT ${limit}`;
    const r = await client.query(q);
    return res.json({ rows: r.rows || [] });
  } catch (e) {
    return res.status(500).json({ error: 'preview_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

// CREATE TABLE immediately (no drafts)
tableBuilderRouter.post('/tables/create', requireDataAdmin, async (req, res) => {
  const schema_name = String(req.body?.schema_name || '').trim();
  const table_name = String(req.body?.table_name || '').trim();
  const table_class = String(req.body?.table_class || 'custom').trim();
  const description = String(req.body?.description || '').trim();
  const created_by = String(req.body?.created_by || 'ui').trim();

  let partitioning = req.body?.partitioning || { enabled: false };
  const partition_enabled = !!partitioning?.enabled;
  const partition_column = String(partitioning?.column || '').trim();
  const partition_interval = String(partitioning?.interval || 'day').trim();

  let test_row = req.body?.test_row ?? null;

  if (!isIdent(schema_name)) return res.status(400).json({ error: 'bad_request', details: 'invalid schema_name' });
  if (!isIdent(table_name)) return res.status(400).json({ error: 'bad_request', details: 'invalid table_name' });

  const columns = normalizeColumns(req.body?.columns);

  if (partition_enabled) {
    if (!isIdent(partition_column)) return res.status(400).json({ error: 'bad_request', details: 'invalid partition column' });
    if (!['day', 'month'].includes(partition_interval)) {
      return res.status(400).json({ error: 'bad_request', details: 'invalid partition interval' });
    }
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // schema
    await client.query(`CREATE SCHEMA IF NOT EXISTS ${qi(schema_name)}`);

    // create table
    const colDDL = columns
      .map((c) => `${qi(c.field_name)} ${c.field_type}${c.field_name === partition_column ? ' NOT NULL' : ''}`)
      .join(',\n  ');

    if (partition_enabled) {
      await client.query(
        `CREATE TABLE IF NOT EXISTS ${qname(schema_name, table_name)} (
          ${colDDL}
        ) PARTITION BY RANGE (${qi(partition_column)});`
      );

      // NOTE: COMMENT IS must be literal/NULL (no $1)
      const tableComment = description
        ? `${description} | partition:${partition_column}:${partition_interval} | by:${created_by} | class:${table_class}`
        : `partition:${partition_column}:${partition_interval} | by:${created_by} | class:${table_class}`;
      await client.query(`COMMENT ON TABLE ${qname(schema_name, table_name)} IS ${qlit(tableComment)}`);

      for (const c of columns) {
        if (c.description) {
          await client.query(
            `COMMENT ON COLUMN ${qname(schema_name, table_name)}.${qi(c.field_name)} IS ${qlit(c.description)}`
          );
        }
      }

      // create partitions
      // day => today + next day; month => current month + next month
      const base = new Date();
      const yyyy = base.getUTCFullYear();
      const mm = String(base.getUTCMonth() + 1).padStart(2, '0');
      const dd = String(base.getUTCDate()).padStart(2, '0');

      if (partition_interval === 'day') {
        const from = `${yyyy}-${mm}-${dd}`;
        const toDate = new Date(Date.UTC(yyyy, base.getUTCMonth(), base.getUTCDate() + 1));
        const to = `${toDate.getUTCFullYear()}-${String(toDate.getUTCMonth() + 1).padStart(2, '0')}-${String(toDate.getUTCDate()).padStart(2, '0')}`;

        const pToday = `${table_name}__p_${yyyy}${mm}${dd}`;
        const pDefault = `${table_name}__p_default`;

        await client.query(
          `CREATE TABLE IF NOT EXISTS ${qname(schema_name, pToday)}
           PARTITION OF ${qname(schema_name, table_name)}
           FOR VALUES FROM (${qlit(from)}) TO (${qlit(to)});`
        );

        await client.query(
          `CREATE TABLE IF NOT EXISTS ${qname(schema_name, pDefault)}
           PARTITION OF ${qname(schema_name, table_name)} DEFAULT;`
        );
      } else {
        // month
        const from = `${yyyy}-${mm}-01`;
        const nextMonth = new Date(Date.UTC(yyyy, base.getUTCMonth() + 1, 1));
        const to = `${nextMonth.getUTCFullYear()}-${String(nextMonth.getUTCMonth() + 1).padStart(2, '0')}-01`;

        const pMonth = `${table_name}__p_${yyyy}${mm}`;
        const pDefault = `${table_name}__p_default`;

        await client.query(
          `CREATE TABLE IF NOT EXISTS ${qname(schema_name, pMonth)}
           PARTITION OF ${qname(schema_name, table_name)}
           FOR VALUES FROM (${qlit(from)}) TO (${qlit(to)});`
        );

        await client.query(
          `CREATE TABLE IF NOT EXISTS ${qname(schema_name, pDefault)}
           PARTITION OF ${qname(schema_name, table_name)} DEFAULT;`
        );
      }
    } else {
      await client.query(
        `CREATE TABLE IF NOT EXISTS ${qname(schema_name, table_name)} (
          ${colDDL}
        );`
      );

      const tableComment = description
        ? `${description} | by:${created_by} | class:${table_class}`
        : `by:${created_by} | class:${table_class}`;
      await client.query(`COMMENT ON TABLE ${qname(schema_name, table_name)} IS ${qlit(tableComment)}`);

      for (const c of columns) {
        if (c.description) {
          await client.query(
            `COMMENT ON COLUMN ${qname(schema_name, table_name)}.${qi(c.field_name)} IS ${qlit(c.description)}`
          );
        }
      }
    }

    // optional: insert test row
    if (test_row && typeof test_row === 'object' && !Array.isArray(test_row)) {
      const enrichedTestRow = await enrichRowWithContractFields(client, schema_name, table_name, test_row);
      const keys = Object.keys(enrichedTestRow).filter((k) => isIdent(k));
      if (keys.length) {
        const cols = keys.map((k) => qi(k)).join(', ');
        const vals = keys.map((_, i) => `$${i + 1}`).join(', ');
        const params = keys.map((k) => enrichedTestRow[k]);
        await client.query(`INSERT INTO ${qname(schema_name, table_name)} (${cols}) VALUES (${vals})`, params);
      }
    }

    await createContractVersion(client, {
      schema: schema_name,
      table: table_name,
      description,
      columns,
      reason: 'table_created',
      by: created_by
    });

    await client.query('COMMIT');

    return res.json({ ok: true, schema_name, table_name });
  } catch (e) {
    try {
      await client.query('ROLLBACK');
    } catch {}
    return res.status(500).json({ error: 'create_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

tableBuilderRouter.post('/tables/drop', requireDataAdmin, async (req, res) => {
  const schema = String(req.body?.schema || '').trim();
  const table = String(req.body?.table || '').trim();

  if (!isIdent(schema) || !isIdent(table)) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid schema/table' });
  }
  if (isProtectedSystemTable(schema, table)) {
    return res.status(403).json({ error: 'forbidden', details: 'protected_system_table' });
  }

  const client = await pool.connect();
  try {
    await markContractsDeletedByTableDrop(client, schema, table);
    await client.query(`DROP TABLE IF EXISTS ${qname(schema, table)} CASCADE`);
    return res.json({ ok: true, schema, table });
  } catch (e) {
    return res.status(500).json({ error: 'drop_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

tableBuilderRouter.post('/tables/rename', requireDataAdmin, async (req, res) => {
  const schema = String(req.body?.schema || '').trim();
  const table = String(req.body?.table || '').trim();
  const new_table = String(req.body?.new_table || '').trim();

  if (!isIdent(schema) || !isIdent(table) || !isIdent(new_table)) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid schema/table/new_table' });
  }
  if (isProtectedSystemTable(schema, table)) {
    return res.status(403).json({ error: 'forbidden', details: 'protected_system_table' });
  }
  if (table === new_table) {
    return res.json({ ok: true, schema, table, new_table, renamed: false });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`ALTER TABLE ${qname(schema, table)} RENAME TO ${qi(new_table)}`);
    const config = await loadRuntimeConfig(client);
    const contractsQn = await ensureContractsTable(client, config);
    await client.query(
      `
      UPDATE ${contractsQn}
      SET table_name = $3, contract_name = $4
      WHERE schema_name = $1 AND table_name = $2
      `,
      [schema, table, new_table, defaultContractName(schema, new_table)]
    );
    await createContractVersionFromTable(client, schema, new_table, 'table_renamed', req.header('X-AO-ROLE') || 'ui');
    await client.query('COMMIT');
    return res.json({ ok: true, schema, table, new_table, renamed: true });
  } catch (e) {
    try {
      await client.query('ROLLBACK');
    } catch {}
    const code = e?.code ? String(e.code) : '';
    if (code === '42P07') {
      return res.status(409).json({ error: 'rename_failed', details: 'table_with_new_name_already_exists' });
    }
    return res.status(500).json({ error: 'rename_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

tableBuilderRouter.get('/tables/meta', async (req, res) => {
  const schema = String(req.query.schema || '').trim();
  const table = String(req.query.table || '').trim();
  if (!isIdent(schema) || !isIdent(table)) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid schema/table' });
  }

  const client = await pool.connect();
  try {
    const description = await getTableDescription(client, schema, table);
    return res.json({ ok: true, schema, table, description });
  } catch (e) {
    return res.status(500).json({ error: 'table_meta_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

tableBuilderRouter.post('/tables/update-meta', requireDataAdmin, async (req, res) => {
  const schema = String(req.body?.schema || '').trim();
  const table = String(req.body?.table || '').trim();
  const new_schema = String(req.body?.new_schema || schema).trim();
  const new_table = String(req.body?.new_table || table).trim();
  const description = String(req.body?.description || '').trim();

  if (!isIdent(schema) || !isIdent(table) || !isIdent(new_schema) || !isIdent(new_table)) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid schema/table/new_schema/new_table' });
  }
  if (isProtectedSystemTable(schema, table)) {
    return res.status(403).json({ error: 'forbidden', details: 'protected_system_table' });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    let currentSchema = schema;
    let currentTable = table;

    if (new_schema !== currentSchema) {
      await client.query(`CREATE SCHEMA IF NOT EXISTS ${qi(new_schema)}`);
      await client.query(`ALTER TABLE ${qname(currentSchema, currentTable)} SET SCHEMA ${qi(new_schema)}`);
      currentSchema = new_schema;
    }

    if (new_table !== currentTable) {
      await client.query(`ALTER TABLE ${qname(currentSchema, currentTable)} RENAME TO ${qi(new_table)}`);
      currentTable = new_table;
    }

    await client.query(`COMMENT ON TABLE ${qname(currentSchema, currentTable)} IS ${qlit(description)}`);

    const config = await loadRuntimeConfig(client);
    const contractsQn = await ensureContractsTable(client, config);
    await client.query(
      `
      UPDATE ${contractsQn}
      SET schema_name = $3, table_name = $4, contract_name = $5
      WHERE schema_name = $1 AND table_name = $2
      `,
      [schema, table, currentSchema, currentTable, defaultContractName(currentSchema, currentTable)]
    );

    await createContractVersionFromTable(
      client,
      currentSchema,
      currentTable,
      'table_meta_updated',
      req.header('X-AO-ROLE') || 'ui'
    );

    await client.query('COMMIT');
    return res.json({
      ok: true,
      schema,
      table,
      new_schema: currentSchema,
      new_table: currentTable,
      description
    });
  } catch (e) {
    try {
      await client.query('ROLLBACK');
    } catch {}
    return res.status(500).json({ error: 'table_meta_update_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

tableBuilderRouter.post('/columns/drop', requireDataAdmin, async (req, res) => {
  const schema = String(req.body?.schema || '').trim();
  const table = String(req.body?.table || '').trim();
  const column = String(req.body?.column || '').trim();

  if (!isIdent(schema) || !isIdent(table) || !isIdent(column)) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid schema/table/column' });
  }
  if (isProtectedSystemTable(schema, table)) {
    return res.status(403).json({ error: 'forbidden', details: 'protected_system_table' });
  }

  const client = await pool.connect();
  try {
    await client.query(`ALTER TABLE ${qname(schema, table)} DROP COLUMN IF EXISTS ${qi(column)}`);
    await createContractVersionFromTable(client, schema, table, `column_dropped:${column}`, req.header('X-AO-ROLE') || 'ui');
    return res.json({ ok: true, schema, table, column });
  } catch (e) {
    return res.status(500).json({ error: 'drop_column_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

tableBuilderRouter.post('/columns/add', requireDataAdmin, async (req, res) => {
  const schema = String(req.body?.schema || '').trim();
  const table = String(req.body?.table || '').trim();
  const colName = String(req.body?.column?.name || '').trim();
  const colType = String(req.body?.column?.type || '').trim();
  const colDescription = String(req.body?.column?.description || '').trim();

  if (!isIdent(schema) || !isIdent(table) || !isIdent(colName)) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid schema/table/column' });
  }
  if (isProtectedSystemTable(schema, table)) {
    return res.status(403).json({ error: 'forbidden', details: 'protected_system_table' });
  }
  if (!KNOWN_TYPES.has(colType)) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid column type' });
  }

  const client = await pool.connect();
  try {
    await client.query(
      `ALTER TABLE ${qname(schema, table)} ADD COLUMN IF NOT EXISTS ${qi(colName)} ${colType}`
    );
    if (colDescription) {
      await client.query(
        `COMMENT ON COLUMN ${qname(schema, table)}.${qi(colName)} IS ${qlit(colDescription)}`
      );
    }
    await createContractVersionFromTable(client, schema, table, `column_added:${colName}`, req.header('X-AO-ROLE') || 'ui');
    return res.json({ ok: true, schema, table, column: { name: colName, type: colType, description: colDescription } });
  } catch (e) {
    return res.status(500).json({ error: 'add_column_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

tableBuilderRouter.post('/columns/describe', requireDataAdmin, async (req, res) => {
  const schema = String(req.body?.schema || '').trim();
  const table = String(req.body?.table || '').trim();
  const column = String(req.body?.column || '').trim();
  const description = String(req.body?.description || '').trim();

  if (!isIdent(schema) || !isIdent(table) || !isIdent(column)) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid schema/table/column' });
  }
  if (isProtectedSystemTable(schema, table)) {
    return res.status(403).json({ error: 'forbidden', details: 'protected_system_table' });
  }

  const client = await pool.connect();
  try {
    await client.query(`COMMENT ON COLUMN ${qname(schema, table)}.${qi(column)} IS ${qlit(description)}`);
    await createContractVersionFromTable(
      client,
      schema,
      table,
      `column_description_updated:${column}`,
      req.header('X-AO-ROLE') || 'ui'
    );
    return res.json({ ok: true, schema, table, column, description });
  } catch (e) {
    return res.status(500).json({ error: 'column_describe_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

tableBuilderRouter.post('/rows/add', requireDataAdmin, async (req, res) => {
  const schema = String(req.body?.schema || '').trim();
  const table = String(req.body?.table || '').trim();
  const row = req.body?.row;

  if (!isIdent(schema) || !isIdent(table)) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid schema/table' });
  }
  if (isProtectedSystemTable(schema, table)) {
    return res.status(403).json({ error: 'forbidden', details: 'protected_system_table' });
  }
  if (!row || typeof row !== 'object' || Array.isArray(row)) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid row payload' });
  }

  const client = await pool.connect();
  try {
    const enrichedRow = await enrichRowWithContractFields(client, schema, table, row);
    const keys = Object.keys(enrichedRow).filter((k) => isIdent(k));
    if (!keys.length) {
      return res.status(400).json({ error: 'bad_request', details: 'no valid row fields' });
    }
    const cols = keys.map((k) => qi(k)).join(', ');
    const vals = keys.map((_, i) => `$${i + 1}`).join(', ');
    const params = keys.map((k) => enrichedRow[k]);

    await client.query(
      `INSERT INTO ${qname(schema, table)} (${cols}) VALUES (${vals})`,
      params
    );
    return res.json({ ok: true, schema, table, inserted: 1 });
  } catch (e) {
    return res.status(500).json({ error: 'add_row_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

tableBuilderRouter.post('/rows/delete', requireDataAdmin, async (req, res) => {
  const schema = String(req.body?.schema || '').trim();
  const table = String(req.body?.table || '').trim();
  const ctid = String(req.body?.ctid || '').trim();

  if (!isIdent(schema) || !isIdent(table)) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid schema/table' });
  }
  if (isProtectedSystemTable(schema, table)) {
    return res.status(403).json({ error: 'forbidden', details: 'protected_system_table' });
  }
  if (!ctid || !/^\(\d+,\d+\)$/.test(ctid)) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid ctid' });
  }

  const client = await pool.connect();
  try {
    const r = await client.query(
      `DELETE FROM ${qname(schema, table)} WHERE ctid = $1::tid`,
      [ctid]
    );
    return res.json({ ok: true, schema, table, ctid, deleted: r.rowCount || 0 });
  } catch (e) {
    return res.status(500).json({ error: 'delete_row_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

export async function bootstrapTableBuilder() {
  const client = await pool.connect();
  try {
    const effective = await loadRuntimeConfig(client, { force: true });
    await ensureSettingsTable(client);
    await ensureContractsTable(client, effective);
    await ensureTemplatesStorageTable(client, effective);
    await ensureApiConfigsTable(client, effective);
    await ensureServerWritesTable(client, effective);
    return { ok: true, effective };
  } finally {
    client.release();
  }
}

