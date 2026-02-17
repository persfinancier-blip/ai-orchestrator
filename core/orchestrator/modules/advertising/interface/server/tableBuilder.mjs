// core/orchestrator/modules/advertising/interface/server/tableBuilder.mjs
import express from 'express';
import { pool } from './db.mjs';

const tableBuilderRouter = express.Router();
tableBuilderRouter.use(express.json({ limit: '4mb' }));

/**
 * Very small RBAC for MVP.
 * Header: X-AO-ROLE = viewer|operator|data_admin
 */
function requireDataAdmin(req, res, next) {
  const role = String(req.header('X-AO-ROLE') || 'viewer');
  if (role !== 'data_admin') return res.status(403).json({ error: 'forbidden', details: 'requires_data_admin' });
  return next();
}

/** allow only safe SQL identifiers */
const IDENT_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

function assertIdent(name, what) {
  if (!IDENT_RE.test(name)) throw new Error(`${what}_invalid:${name}`);
  return name;
}

function qIdent(name) {
  // escape double quotes
  return `"${String(name).replace(/"/g, '""')}"`;
}

function qFull(schema, table) {
  return `${qIdent(schema)}.${qIdent(table)}`;
}

function normalizeType(t) {
  const v = String(t || '').toLowerCase().trim();
  const allowed = new Set([
    'text',
    'int',
    'integer',
    'bigint',
    'numeric',
    'boolean',
    'date',
    'timestamptz',
    'timestamp with time zone',
    'jsonb',
    'uuid',
  ]);
  if (!allowed.has(v)) throw new Error(`field_type_invalid:${t}`);
  if (v === 'int') return 'integer';
  if (v === 'timestamptz') return 'timestamp with time zone';
  return v;
}

function buildColumnSql(columns) {
  if (!Array.isArray(columns) || columns.length === 0) throw new Error('columns_required');
  const uniq = new Set();

  const sqlParts = [];
  for (const c of columns) {
    const field_name = assertIdent(String(c.field_name || '').trim(), 'field_name');
    if (uniq.has(field_name)) throw new Error(`field_name_duplicate:${field_name}`);
    uniq.add(field_name);

    const field_type = normalizeType(c.field_type);
    sqlParts.push(`${qIdent(field_name)} ${field_type}`);
  }
  return sqlParts.join(',\n          ');
}

async function listExistingTables(client) {
  const r = await client.query(
    `
    SELECT table_schema AS schema_name, table_name
    FROM information_schema.tables
    WHERE table_type='BASE TABLE'
      AND table_schema NOT IN ('pg_catalog','information_schema')
    ORDER BY table_schema, table_name;
    `
  );
  return r.rows;
}

async function listTableColumns(client, schema, table) {
  const r = await client.query(
    `
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema=$1 AND table_name=$2
    ORDER BY ordinal_position;
    `,
    [schema, table]
  );
  return r.rows;
}

/**
 * GET /ai-orchestrator/api/tables
 * Returns existing tables in DB (all schemas) - used for UI "Current tables".
 */
tableBuilderRouter.get('/tables', async (_req, res) => {
  const client = await pool.connect();
  try {
    const existing_tables = await listExistingTables(client);
    res.json({ drafts: [], existing_tables });
  } catch (e) {
    res.status(500).json({ error: 'tables_list_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

/**
 * GET /ai-orchestrator/api/preview?schema=...&table=...&limit=5
 * Returns columns + rows (limit).
 */
tableBuilderRouter.get('/preview', async (req, res) => {
  const schema = String(req.query.schema || '').trim();
  const table = String(req.query.table || '').trim();
  const limit = Math.min(Math.max(Number(req.query.limit || 5), 1), 50);

  try {
    assertIdent(schema, 'schema');
    assertIdent(table, 'table');
  } catch (e) {
    return res.status(400).json({ error: 'bad_request', details: String(e?.message || e) });
  }

  const client = await pool.connect();
  try {
    const columns = await listTableColumns(client, schema, table);
    const sql = `SELECT * FROM ${qFull(schema, table)} ORDER BY 1 DESC NULLS LAST LIMIT $1`;
    const r = await client.query(sql, [limit]);
    res.json({ columns, rows: r.rows });
  } catch (e) {
    res.status(500).json({ error: 'preview_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

/**
 * POST /ai-orchestrator/api/tables/create
 * Body:
 * {
 *   schema_name, table_name,
 *   columns: [{field_name, field_type}],
 *   partitioning?: { enabled: boolean, column?: string, interval?: 'day'|'month' }
 * }
 *
 * Creates schema if missing, creates table, optionally partitions.
 * Adds system columns:
 *   id uuid pk default gen_random_uuid()
 *   created_at timestamptz default now()
 *   updated_at timestamptz default now()
 */
tableBuilderRouter.post('/tables/create', requireDataAdmin, async (req, res) => {
  const body = req.body || {};
  const schema_name = String(body.schema_name || '').trim();
  const table_name = String(body.table_name || '').trim();
  const description = String(body.description || '').trim();
  const columns = Array.isArray(body.columns) ? body.columns : [];
  const partitioning = body.partitioning || { enabled: false };

  try {
    assertIdent(schema_name, 'schema');
    assertIdent(table_name, 'table');
  } catch (e) {
    return res.status(400).json({ error: 'bad_request', details: String(e?.message || e) });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // schema
    await client.query(`CREATE SCHEMA IF NOT EXISTS ${qIdent(schema_name)};`);

    // system cols first
    const sysCols = [
      `${qIdent('id')} uuid PRIMARY KEY DEFAULT gen_random_uuid()`,
      `${qIdent('created_at')} timestamp with time zone NOT NULL DEFAULT now()`,
      `${qIdent('updated_at')} timestamp with time zone NOT NULL DEFAULT now()`,
    ];
    const userColsSql = buildColumnSql(columns);
    const colsSql = [...sysCols, userColsSql].join(',\n          ');

    const fullName = qFull(schema_name, table_name);

    const partEnabled = !!partitioning?.enabled;
    const partition_column = String(partitioning?.column || 'event_date').trim();
    const partition_interval = String(partitioning?.interval || 'day').trim();

    if (!partEnabled) {
      await client.query(
        `CREATE TABLE IF NOT EXISTS ${fullName} (
          ${colsSql}
        );`
      );
    } else {
      assertIdent(partition_column, 'partition_column');

      // ensure partition column exists in user columns
      const has = columns.some((c) => String(c?.field_name || '').trim() === partition_column);
      if (!has) throw new Error(`partition_column_not_found:${partition_column}`);

      await client.query(
        `CREATE TABLE IF NOT EXISTS ${fullName} (
          ${colsSql}
        ) PARTITION BY RANGE (${qIdent(partition_column)});`
      );

      const partToday = qFull(schema_name, `${table_name}__p_today`);
      const partMonth = qFull(schema_name, `${table_name}__p_month`);
      const partDefault = qFull(schema_name, `${table_name}__p_default`);

      if (partition_interval === 'day') {
        await client.query(
          `CREATE TABLE IF NOT EXISTS ${partToday} PARTITION OF ${fullName}
           FOR VALUES FROM (CURRENT_DATE) TO (CURRENT_DATE + INTERVAL '1 day');`
        );
      } else if (partition_interval === 'month') {
        await client.query(
          `CREATE TABLE IF NOT EXISTS ${partMonth} PARTITION OF ${fullName}
           FOR VALUES FROM (date_trunc('month', CURRENT_DATE)::date)
           TO (date_trunc('month', CURRENT_DATE)::date + INTERVAL '1 month');`
        );
      } else {
        throw new Error('partition_interval_invalid');
      }

      await client.query(`CREATE TABLE IF NOT EXISTS ${partDefault} PARTITION OF ${fullName} DEFAULT;`);
    }

    if (description) {
      await client.query(`COMMENT ON TABLE ${fullName} IS $1;`, [description]);
    }

    await client.query('COMMIT');
    res.json({ ok: true, schema_name, table_name });
  } catch (e) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: 'table_create_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

/**
 * POST /ai-orchestrator/api/tables/column/add
 * Body: { schema_name, table_name, column: { field_name, field_type, description? } }
 */
tableBuilderRouter.post('/tables/column/add', requireDataAdmin, async (req, res) => {
  const body = req.body || {};
  const schema_name = String(body.schema_name || '').trim();
  const table_name = String(body.table_name || '').trim();
  const column = body.column || {};

  try {
    assertIdent(schema_name, 'schema');
    assertIdent(table_name, 'table');
    const field_name = assertIdent(String(column.field_name || '').trim(), 'field_name');
    const field_type = normalizeType(column.field_type);
    const description = String(column.description || '').trim();

    const client = await pool.connect();
    try {
      const fullName = qFull(schema_name, table_name);
      await client.query(`ALTER TABLE ${fullName} ADD COLUMN IF NOT EXISTS ${qIdent(field_name)} ${field_type};`);
      if (description) {
        await client.query(`COMMENT ON COLUMN ${fullName}.${qIdent(field_name)} IS $1;`, [description]);
      }
      res.json({ ok: true });
    } finally {
      client.release();
    }
  } catch (e) {
    res.status(500).json({ error: 'column_add_failed', details: String(e?.message || e) });
  }
});

/**
 * POST /ai-orchestrator/api/tables/column/drop
 * Body: { schema_name, table_name, field_name }
 */
tableBuilderRouter.post('/tables/column/drop', requireDataAdmin, async (req, res) => {
  const body = req.body || {};
  const schema_name = String(body.schema_name || '').trim();
  const table_name = String(body.table_name || '').trim();
  const field_name = String(body.field_name || '').trim();

  try {
    assertIdent(schema_name, 'schema');
    assertIdent(table_name, 'table');
    assertIdent(field_name, 'field_name');

    if (['id', 'created_at', 'updated_at'].includes(field_name)) {
      return res.status(400).json({ error: 'bad_request', details: 'system_column_cannot_be_dropped' });
    }

    const client = await pool.connect();
    try {
      const fullName = qFull(schema_name, table_name);
      await client.query(`ALTER TABLE ${fullName} DROP COLUMN IF EXISTS ${qIdent(field_name)};`);
      res.json({ ok: true });
    } finally {
      client.release();
    }
  } catch (e) {
    res.status(500).json({ error: 'column_drop_failed', details: String(e?.message || e) });
  }
});

/**
 * POST /ai-orchestrator/api/tables/row/insert
 * Body: { schema_name, table_name, row: { ... } }
 * Notes:
 * - id/created_at/updated_at are optional (id auto).
 */
tableBuilderRouter.post('/tables/row/insert', requireDataAdmin, async (req, res) => {
  const body = req.body || {};
  const schema_name = String(body.schema_name || '').trim();
  const table_name = String(body.table_name || '').trim();
  const row = body.row;

  try {
    assertIdent(schema_name, 'schema');
    assertIdent(table_name, 'table');
    if (!row || typeof row !== 'object' || Array.isArray(row)) throw new Error('row_must_be_object');

    const keys = Object.keys(row).filter((k) => row[k] !== undefined);
    if (keys.length === 0) throw new Error('row_empty');

    for (const k of keys) assertIdent(k, 'field_name');

    const cols = keys.map(qIdent).join(', ');
    const params = keys.map((_, i) => `$${i + 1}`).join(', ');
    const values = keys.map((k) => row[k]);

    const client = await pool.connect();
    try {
      const fullName = qFull(schema_name, table_name);
      const sql = `INSERT INTO ${fullName} (${cols}) VALUES (${params}) RETURNING *;`;
      const r = await client.query(sql, values);
      res.json({ ok: true, row: r.rows[0] });
    } finally {
      client.release();
    }
  } catch (e) {
    res.status(500).json({ error: 'row_insert_failed', details: String(e?.message || e) });
  }
});

/**
 * POST /ai-orchestrator/api/tables/row/delete
 * Body: { schema_name, table_name, id }
 */
tableBuilderRouter.post('/tables/row/delete', requireDataAdmin, async (req, res) => {
  const body = req.body || {};
  const schema_name = String(body.schema_name || '').trim();
  const table_name = String(body.table_name || '').trim();
  const id = String(body.id || '').trim();

  try {
    assertIdent(schema_name, 'schema');
    assertIdent(table_name, 'table');
    if (!id) throw new Error('id_required');

    const client = await pool.connect();
    try {
      const fullName = qFull(schema_name, table_name);
      const r = await client.query(`DELETE FROM ${fullName} WHERE id = $1 RETURNING id;`, [id]);
      res.json({ ok: true, deleted: r.rowCount });
    } finally {
      client.release();
    }
  } catch (e) {
    res.status(500).json({ error: 'row_delete_failed', details: String(e?.message || e) });
  }
});

/**
 * POST /ai-orchestrator/api/tables/drop
 * Body: { schema_name, table_name, cascade?: boolean }
 */
tableBuilderRouter.post('/tables/drop', requireDataAdmin, async (req, res) => {
  const body = req.body || {};
  const schema_name = String(body.schema_name || '').trim();
  const table_name = String(body.table_name || '').trim();
  const cascade = !!body.cascade;

  try {
    assertIdent(schema_name, 'schema');
    assertIdent(table_name, 'table');

    const client = await pool.connect();
    try {
      const fullName = qFull(schema_name, table_name);
      await client.query(`DROP TABLE IF EXISTS ${fullName} ${cascade ? 'CASCADE' : ''};`);
      res.json({ ok: true });
    } finally {
      client.release();
    }
  } catch (e) {
    res.status(500).json({ error: 'table_drop_failed', details: String(e?.message || e) });
  }
});

export { tableBuilderRouter };
