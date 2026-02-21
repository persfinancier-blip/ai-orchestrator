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
const CONTRACTS_SCHEMA = 'ao_system';
const CONTRACTS_TABLE = 'table_data_contract_versions';
const CONTRACTS_QNAME = `${qi(CONTRACTS_SCHEMA)}.${qi(CONTRACTS_TABLE)}`;

async function ensureContractsTable(client) {
  await client.query(`CREATE SCHEMA IF NOT EXISTS ${qi(CONTRACTS_SCHEMA)}`);
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${CONTRACTS_QNAME} (
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
    ON ${CONTRACTS_QNAME} (schema_name, table_name, version DESC)
  `);
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
  await ensureContractsTable(client);
  const r = await client.query(
    `SELECT COALESCE(MAX(version), 0) AS max_v FROM ${CONTRACTS_QNAME} WHERE schema_name = $1 AND table_name = $2`,
    [schema, table]
  );
  const maxV = Number(r.rows?.[0]?.max_v || 0);
  return Number.isFinite(maxV) ? maxV + 1 : 1;
}

async function createContractVersion(client, { schema, table, description, columns, reason, by }) {
  const version = await getNextContractVersion(client, schema, table);
  await client.query(
    `
    INSERT INTO ${CONTRACTS_QNAME}
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
  await ensureContractsTable(client);
  await client.query(
    `
    UPDATE ${CONTRACTS_QNAME}
    SET lifecycle_state = 'table_deleted', deleted_at = now()
    WHERE schema_name = $1
      AND table_name = $2
      AND lifecycle_state <> 'deleted_by_user'
    `,
    [schema, table]
  );
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

tableBuilderRouter.get('/contracts', requireDataAdmin, async (req, res) => {
  const schema = String(req.query.schema || '').trim();
  const table = String(req.query.table || '').trim();
  if (!isIdent(schema) || !isIdent(table)) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid schema/table' });
  }

  const client = await pool.connect();
  try {
    await ensureContractsTable(client);
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
      FROM ${CONTRACTS_QNAME}
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
    await ensureContractsTable(client);
    const r = await client.query(
      `
      UPDATE ${CONTRACTS_QNAME}
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
      const keys = Object.keys(test_row).filter((k) => isIdent(k));
      if (keys.length) {
        const cols = keys.map((k) => qi(k)).join(', ');
        const vals = keys.map((_, i) => `$${i + 1}`).join(', ');
        const params = keys.map((k) => test_row[k]);
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
    await ensureContractsTable(client);
    await client.query(
      `
      UPDATE ${CONTRACTS_QNAME}
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

  const keys = Object.keys(row).filter((k) => isIdent(k));
  if (!keys.length) {
    return res.status(400).json({ error: 'bad_request', details: 'no valid row fields' });
  }

  const cols = keys.map((k) => qi(k)).join(', ');
  const vals = keys.map((_, i) => `$${i + 1}`).join(', ');
  const params = keys.map((k) => row[k]);

  const client = await pool.connect();
  try {
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
