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
  templates_table: 'table_templates_store'
});
const SETTINGS_CACHE_MS = Number(process.env.AO_SETTINGS_CACHE_MS || 5000);
let settingsCache = { at: 0, value: { ...DEFAULT_CONFIG } };

function normalizeSettingIdent(value, fallback) {
  const v = String(value || '').trim();
  return isIdent(v) ? v : fallback;
}

function contractsQname(config) {
  return `${qi(config.contracts_schema)}.${qi(config.contracts_table)}`;
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
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_table_settings_store_active_idx
    ON ${SETTINGS_QNAME} (is_active, setting_key)
  `);
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
    }
  ];

  for (const row of defaults) {
    await client.query(
      `
      INSERT INTO ${SETTINGS_QNAME}
        (setting_key, setting_value, scope, description, is_active, updated_at, updated_by)
      VALUES
        ($1, $2::jsonb, 'global', $3, true, now(), 'system_bootstrap')
      ON CONFLICT (setting_key) DO NOTHING
      `,
      [row.key, JSON.stringify(row.value), row.description]
    );
  }
}

function applySettingValue(target, key, value) {
  if (key === 'contracts_storage') {
    const obj = value && typeof value === 'object' && !Array.isArray(value) ? value : {};
    target.contracts_schema = normalizeSettingIdent(obj.schema, target.contracts_schema);
    target.contracts_table = normalizeSettingIdent(obj.table, target.contracts_table);
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
    const obj = value && typeof value === 'object' && !Array.isArray(value) ? value : {};
    target.templates_schema = normalizeSettingIdent(obj.schema, target.templates_schema);
    target.templates_table = normalizeSettingIdent(obj.table, target.templates_table);
    return;
  }
  if (key === 'templates_storage_schema') {
    target.templates_schema = normalizeSettingIdent(value, target.templates_schema);
    return;
  }
  if (key === 'templates_storage_table') {
    target.templates_table = normalizeSettingIdent(value, target.templates_table);
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
  return contractsQn;
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
  const version = await getNextContractVersion(client, schema, table);
  await client.query(
    `
    INSERT INTO ${contractsQn}
      (schema_name, table_name, contract_name, version, lifecycle_state, deleted_at, description, columns, change_reason, changed_by)
    VALUES
      ($1, $2, $3, $4, 'active', NULL, $5, $6::jsonb, $7, $8)
    `,
    [
      schema,
      table,
      defaultContractName(schema, table),
      version,
      String(description || ''),
      JSON.stringify(Array.isArray(columns) ? columns : []),
      String(reason || ''),
      String(by || 'ui')
    ]
  );
  return version;
}

async function createContractVersionFromTable(client, schema, table, reason, by) {
  const cols = await getTableColumnsSnapshot(client, schema, table);
  const description = await getTableDescription(client, schema, table);
  return createContractVersion(client, { schema, table, description, columns: cols, reason, by });
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
      AND lifecycle_state <> 'deleted_by_user'
    ORDER BY version DESC
    LIMIT 1
    `,
    [schema, table]
  );
  const row = r.rows?.[0] || null;
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
        (setting_key, setting_value, scope, description, is_active, updated_by, updated_at)
      VALUES
        ($1, $2::jsonb, $3, $4, $5, $6, now())
      ON CONFLICT (setting_key)
      DO UPDATE SET
        setting_value = EXCLUDED.setting_value,
        scope = EXCLUDED.scope,
        description = EXCLUDED.description,
        is_active = EXCLUDED.is_active,
        updated_by = EXCLUDED.updated_by,
        updated_at = now()
      `,
      [setting_key, JSON.stringify(setting_value), scope, description, is_active, updated_by]
    );

    const effective = await loadRuntimeConfig(client, { force: true });
    await ensureContractsTable(client, effective);
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

tableBuilderRouter.post('/columns/drop', requireDataAdmin, async (req, res) => {
  const schema = String(req.body?.schema || '').trim();
  const table = String(req.body?.table || '').trim();
  const column = String(req.body?.column || '').trim();

  if (!isIdent(schema) || !isIdent(table) || !isIdent(column)) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid schema/table/column' });
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

tableBuilderRouter.post('/rows/add', requireDataAdmin, async (req, res) => {
  const schema = String(req.body?.schema || '').trim();
  const table = String(req.body?.table || '').trim();
  const row = req.body?.row;

  if (!isIdent(schema) || !isIdent(table)) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid schema/table' });
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
    return { ok: true, effective };
  } finally {
    client.release();
  }
}
