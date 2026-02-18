// core/orchestrator/modules/advertising/interface/server/tableBuilder.mjs
import express from 'express';
import { pool } from './db.mjs';

export const tableBuilderRouter = express.Router();

// JSON body (UI sends JSON)
tableBuilderRouter.use(express.json({ limit: '4mb' }));

/** -------- helpers -------- */

function requireDataAdmin(req) {
  const role = String(req.header('X-AO-ROLE') || '').trim();
  if (role !== 'data_admin') {
    const err = new Error('forbidden');
    err.status = 403;
    err.details = 'Роль должна быть data_admin';
    throw err;
  }
}

function qi(s) {
  return `"${String(s).replace(/"/g, '""')}"`;
}

// quote literal (for COMMENT). Parameters ($1) do NOT work reliably in COMMENT statements.
function ql(v) {
  if (v === null || v === undefined) return 'NULL';
  const s = String(v);
  return `'${s.replace(/'/g, "''")}'`;
}

function qname(schema, table) {
  return `${qi(schema)}.${qi(table)}`;
}

function assertIdent(name, what = 'identifier') {
  const s = String(name || '').trim();
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(s)) {
    const err = new Error('invalid_identifier');
    err.status = 400;
    err.details = `Некорректный ${what}: ${name}`;
    throw err;
  }
  return s;
}

const TYPE_WHITELIST = new Set([
  'text',
  'int',
  'bigint',
  'numeric',
  'boolean',
  'date',
  'timestamptz',
  'jsonb',
  'uuid'
]);

function assertType(t) {
  const s = String(t || '').trim().toLowerCase();
  if (!TYPE_WHITELIST.has(s)) {
    const err = new Error('invalid_type');
    err.status = 400;
    err.details = `Некорректный тип: ${t}`;
    throw err;
  }
  return s;
}

function normalizeColumns(columns) {
  const cols = Array.isArray(columns) ? columns : [];
  const out = [];
  for (const c of cols) {
    const name = String(c?.field_name || c?.name || '').trim();
    const type = String(c?.field_type || c?.type || '').trim().toLowerCase();
    const description = String(c?.description || '').trim();

    if (!name) continue;
    assertIdent(name, 'имя поля');
    assertType(type);

    out.push({ name, type, description });
  }
  if (!out.length) {
    const err = new Error('no_columns');
    err.status = 400;
    err.details = 'Нужно минимум одно поле';
    throw err;
  }
  return out;
}

async function ensureSchema(client, schema) {
  await client.query(`CREATE SCHEMA IF NOT EXISTS ${qi(schema)};`);
}

async function listExistingTables(client) {
  const q = `
    SELECT
      n.nspname AS schema_name,
      c.relname AS table_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r','p')
      AND n.nspname NOT IN ('pg_catalog','information_schema')
    ORDER BY n.nspname, c.relname;
  `;
  const r = await client.query(q);
  return r.rows || [];
}

async function previewRows(client, schema, table, limit = 5) {
  const q = `SELECT * FROM ${qname(schema, table)} LIMIT $1;`;
  const r = await client.query(q, [Number(limit) || 5]);
  return r.rows || [];
}

async function columnExists(client, schema, table, column) {
  const q = `
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
    LIMIT 1;
  `;
  const r = await client.query(q, [schema, table, column]);
  return (r.rows || []).length > 0;
}

/** -------- routes -------- */

// list tables + drafts (drafts currently empty but kept for compatibility)
tableBuilderRouter.get('/tables', async (_req, res) => {
  const client = await pool.connect();
  try {
    const existing_tables = await listExistingTables(client);
    res.json({ drafts: [], existing_tables });
  } catch (e) {
    res.status(500).json({
      error: 'tables_list_failed',
      details: String(e?.message || e)
    });
  } finally {
    client.release();
  }
});

// preview 5 rows
tableBuilderRouter.get('/preview', async (req, res) => {
  const schema = String(req.query.schema || '').trim();
  const table = String(req.query.table || '').trim();
  const limit = Number(req.query.limit || 5);

  if (!schema || !table) {
    return res.status(400).json({ error: 'bad_request', details: 'schema и table обязательны' });
  }

  const client = await pool.connect();
  try {
    const rows = await previewRows(client, schema, table, limit);
    res.json({ rows });
  } catch (e) {
    res.status(500).json({
      error: 'preview_failed',
      details: String(e?.message || e)
    });
  } finally {
    client.release();
  }
});

// create draft (in-memory emulation: returns an id with payload)
tableBuilderRouter.post('/tables/draft', async (req, res) => {
  try {
    requireDataAdmin(req);

    const schema = assertIdent(req.body?.schema_name, 'schema');
    const table = assertIdent(req.body?.table_name, 'table');
    const table_class = String(req.body?.table_class || 'custom').trim();
    const description = String(req.body?.description || '').trim();
    const columns = normalizeColumns(req.body?.columns);

    const partitioning = req.body?.partitioning || { enabled: false };
    const partition_enabled = Boolean(partitioning?.enabled);
    const partition_column = partition_enabled ? assertIdent(partitioning?.column || 'event_date', 'partition column') : null;
    const partition_interval = partition_enabled ? String(partitioning?.interval || 'day') : null;

    const test_row = req.body?.test_row || null;

    // draft id (client will immediately apply)
    const id = `${schema}.${table}.${Date.now()}`;

    res.json({
      id,
      schema_name: schema,
      table_name: table,
      table_class,
      description,
      columns,
      partitioning: partition_enabled
        ? { enabled: true, column: partition_column, interval: partition_interval }
        : { enabled: false },
      test_row
    });
  } catch (e) {
    res.status(e?.status || 500).json({
      error: 'draft_create_failed',
      details: String(e?.details || e?.message || e)
    });
  }
});

// apply draft (create schema/table/columns + optional partition + optional insert test row)
tableBuilderRouter.post('/tables/apply/:id', async (req, res) => {
  const draft = req.body?.draft || null;

  // also allow passing draft via body-less call where id contains schema/table,
  // but normal flow: UI first calls /draft, then /apply/{id} with empty body.
  // We rebuild the needed payload from cached data on client side -> here we expect it in body? not reliable.
  // So the simplest: UI keeps using /draft which returns everything; /apply can accept it via body.
  // If not passed, we reject.
  if (!draft && !req.body?.schema_name) {
    return res.status(400).json({
      error: 'apply_failed',
      details: 'apply ожидает данные черновика в body (schema_name, table_name, columns, ...)'
    });
  }

  const schema = assertIdent((draft?.schema_name || req.body?.schema_name), 'schema');
  const table = assertIdent((draft?.table_name || req.body?.table_name), 'table');
  const table_class = String(draft?.table_class || req.body?.table_class || 'custom').trim();
  const description = String(draft?.description || req.body?.description || '').trim();

  const columns = normalizeColumns(draft?.columns || req.body?.columns);

  const partitioning = (draft?.partitioning || req.body?.partitioning || { enabled: false });
  const partition_enabled = Boolean(partitioning?.enabled);
  const partition_column = partition_enabled ? assertIdent(partitioning?.column || 'event_date', 'partition column') : null;
  const partition_interval = partition_enabled ? String(partitioning?.interval || 'day') : null;

  const test_row = draft?.test_row ?? req.body?.test_row ?? null;

  const client = await pool.connect();
  try {
    requireDataAdmin(req);

    await client.query('BEGIN');

    await ensureSchema(client, schema);

    // Build column definitions
    const colDefs = columns.map((c) => `${qi(c.name)} ${c.type}`).join(',\n  ');

    if (!partition_enabled) {
      // Normal table
      await client.query(`CREATE TABLE IF NOT EXISTS ${qname(schema, table)} (\n  ${colDefs}\n);`);

      if (description) {
        // FIX: no $1 in COMMENT ON
        await client.query(`COMMENT ON TABLE ${qname(schema, table)} IS ${ql(description)};`);
      }
    } else {
      // Partitioned table (by RANGE)
      if (!(await columnExists(client, schema, table, partition_column))) {
        // Ensure partition column exists in columns
        const exists = columns.some((c) => c.name === partition_column);
        if (!exists) {
          const err = new Error('partition_column_missing');
          err.status = 400;
          err.details = `Колонка партиции "${partition_column}" не найдена в списке полей`;
          throw err;
        }
      }

      await client.query(
        `CREATE TABLE IF NOT EXISTS ${qname(schema, table)} (\n  ${colDefs}\n) PARTITION BY RANGE (${qi(partition_column)});`
      );

      // create default partition
      await client.query(`CREATE TABLE IF NOT EXISTS ${qname(schema, `${table}__p_default`)} PARTITION OF ${qname(schema, table)} DEFAULT;`);

      // create today partition (day/month)
      const partName = `${table}__p_today`;
      if (partition_interval === 'month') {
        await client.query(
          `CREATE TABLE IF NOT EXISTS ${qname(schema, partName)} PARTITION OF ${qname(schema, table)}
           FOR VALUES FROM (date_trunc('month', now())::date) TO ((date_trunc('month', now()) + interval '1 month')::date);`
        );
      } else {
        await client.query(
          `CREATE TABLE IF NOT EXISTS ${qname(schema, partName)} PARTITION OF ${qname(schema, table)}
           FOR VALUES FROM (current_date) TO (current_date + 1);`
        );
      }

      // FIX: no $1 in COMMENT ON
      const __tbl_comment = description ? `${description} | partitioned by ${partition_column} (${partition_interval})` : `partitioned by ${partition_column} (${partition_interval})`;
      await client.query(`COMMENT ON TABLE ${qname(schema, table)} IS ${ql(__tbl_comment)};`);
    }

    // column comments (FIX: no $1)
    for (const c of columns) {
      if (c.description) {
        await client.query(`COMMENT ON COLUMN ${qname(schema, table)}.${qi(c.name)} IS ${ql(c.description)};`);
      }
    }

    // optional insert test row
    if (test_row && typeof test_row === 'object' && !Array.isArray(test_row)) {
      const keys = Object.keys(test_row);
      if (keys.length) {
        // Only insert into columns that exist
        const allowed = new Set(columns.map((c) => c.name));
        const k2 = keys.filter((k) => allowed.has(k));
        if (k2.length) {
          const colsSql = k2.map((k) => qi(k)).join(', ');
          const valsSql = k2.map((_, i) => `$${i + 1}`).join(', ');
          const params = k2.map((k) => test_row[k]);

          await client.query(`INSERT INTO ${qname(schema, table)} (${colsSql}) VALUES (${valsSql});`, params);
        }
      }
    }

    await client.query('COMMIT');

    res.json({
      ok: true,
      created: `${schema}.${table}`,
      table_class
    });
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch {}
    res.status(e?.status || 500).json({
      error: 'apply_failed',
      details: String(e?.details || e?.message || e)
    });
  } finally {
    client.release();
  }
});

// add column
tableBuilderRouter.post('/tables/column/add', async (req, res) => {
  const schema = assertIdent(req.body?.schema, 'schema');
  const table = assertIdent(req.body?.table, 'table');
  const name = assertIdent(req.body?.name, 'column name');
  const type = assertType(req.body?.type);
  const description = String(req.body?.description || '').trim();

  const client = await pool.connect();
  try {
    requireDataAdmin(req);

    await client.query('BEGIN');
    await client.query(`ALTER TABLE ${qname(schema, table)} ADD COLUMN ${qi(name)} ${type};`);
    if (description) {
      // FIX: no $1 in COMMENT ON
      await client.query(`COMMENT ON COLUMN ${qname(schema, table)}.${qi(name)} IS ${ql(description)};`);
    }
    await client.query('COMMIT');

    res.json({ ok: true });
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch {}
    res.status(e?.status || 500).json({
      error: 'column_add_failed',
      details: String(e?.details || e?.message || e)
    });
  } finally {
    client.release();
  }
});

// drop column
tableBuilderRouter.post('/tables/column/drop', async (req, res) => {
  const schema = assertIdent(req.body?.schema, 'schema');
  const table = assertIdent(req.body?.table, 'table');
  const name = assertIdent(req.body?.name, 'column name');

  const client = await pool.connect();
  try {
    requireDataAdmin(req);

    await client.query('BEGIN');
    await client.query(`ALTER TABLE ${qname(schema, table)} DROP COLUMN ${qi(name)};`);
    await client.query('COMMIT');

    res.json({ ok: true });
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch {}
    res.status(e?.status || 500).json({
      error: 'column_drop_failed',
      details: String(e?.details || e?.message || e)
    });
  } finally {
    client.release();
  }
});

// insert row (JSON)
tableBuilderRouter.post('/tables/row/insert', async (req, res) => {
  const schema = assertIdent(req.body?.schema, 'schema');
  const table = assertIdent(req.body?.table, 'table');
  const row = req.body?.row;

  if (!row || typeof row !== 'object' || Array.isArray(row)) {
    return res.status(400).json({ error: 'bad_request', details: 'row должен быть JSON объектом' });
  }

  const client = await pool.connect();
  try {
    requireDataAdmin(req);

    const keys = Object.keys(row);
    if (!keys.length) return res.status(400).json({ error: 'bad_request', details: 'row пустой' });

    const colsSql = keys.map((k) => qi(assertIdent(k, 'column'))).join(', ');
    const valsSql = keys.map((_, i) => `$${i + 1}`).join(', ');
    const params = keys.map((k) => row[k]);

    await client.query(`INSERT INTO ${qname(schema, table)} (${colsSql}) VALUES (${valsSql});`, params);
    res.json({ ok: true });
  } catch (e) {
    res.status(e?.status || 500).json({
      error: 'row_insert_failed',
      details: String(e?.details || e?.message || e)
    });
  } finally {
    client.release();
  }
});

// delete table (dangerous)
tableBuilderRouter.post('/tables/drop', async (req, res) => {
  const schema = assertIdent(req.body?.schema, 'schema');
  const table = assertIdent(req.body?.table, 'table');

  const client = await pool.connect();
  try {
    requireDataAdmin(req);

    await client.query(`DROP TABLE ${qname(schema, table)} CASCADE;`);
    res.json({ ok: true });
  } catch (e) {
    res.status(e?.status || 500).json({
      error: 'table_drop_failed',
      details: String(e?.details || e?.message || e)
    });
  } finally {
    client.release();
  }
});
