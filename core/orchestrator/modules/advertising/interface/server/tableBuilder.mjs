import express from 'express';
import { pool } from './db.mjs';

export const tableBuilderRouter = express.Router();

tableBuilderRouter.use(express.json({ limit: '4mb' }));

function requireDataAdmin(req, res, next) {
  const role = String(req.header('X-AO-ROLE') || 'viewer');
  if (role !== 'data_admin') {
    return res.status(403).json({ error: 'forbidden', details: 'Требуется роль data_admin' });
  }
  next();
}

function isIdent(s) {
  return typeof s === 'string' && /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(s);
}

function qIdent(s) {
  // safe because validated
  return `"${s.replaceAll('"', '""')}"`;
}

function qPath(schema, table) {
  return `${qIdent(schema)}.${qIdent(table)}`;
}

function mapType(t) {
  const allowed = new Set(['text', 'int', 'bigint', 'numeric', 'boolean', 'date', 'timestamptz', 'jsonb', 'uuid']);
  if (!allowed.has(t)) throw new Error(`Некорректный тип: ${t}`);
  if (t === 'int') return 'integer';
  return t;
}

async function ensureSchema(schema) {
  await pool.query(`CREATE SCHEMA IF NOT EXISTS ${qIdent(schema)};`);
}

async function commentColumn(schema, table, col, desc) {
  if (!desc) return;
  await pool.query(`COMMENT ON COLUMN ${qPath(schema, table)}.${qIdent(col)} IS $1;`, [desc]);
}

async function commentTable(schema, table, desc) {
  if (!desc) return;
  await pool.query(`COMMENT ON TABLE ${qPath(schema, table)} IS $1;`, [desc]);
}

// -------- LIST TABLES --------
tableBuilderRouter.get('/tables', async (_req, res) => {
  try {
    const r = await pool.query(`
      SELECT table_schema AS schema_name, table_name
      FROM information_schema.tables
      WHERE table_type='BASE TABLE'
        AND table_schema NOT IN ('pg_catalog','information_schema')
      ORDER BY table_schema, table_name
    `);
    res.json({ drafts: [], existing_tables: r.rows });
  } catch (e) {
    res.status(500).json({ error: 'tables_list_failed', details: String(e?.message || e) });
  }
});

// -------- CREATE TABLE NOW --------
tableBuilderRouter.post('/table/create', requireDataAdmin, async (req, res) => {
  try {
    const {
      schema_name,
      table_name,
      description,
      columns,
      partitioning,
      test_row
    } = req.body || {};

    if (!isIdent(schema_name)) throw new Error('Некорректная схема');
    if (!isIdent(table_name)) throw new Error('Некорректная таблица');

    if (!Array.isArray(columns) || columns.length === 0) throw new Error('Нужны columns');

    const cols = columns.map((c) => {
      const name = String(c.field_name || '').trim();
      const type = String(c.field_type || '').trim();
      const desc = String(c.description || '').trim();
      if (!isIdent(name)) throw new Error(`Некорректное имя колонки: ${name}`);
      return { name, type: mapType(type), desc };
    });

    // partitioning
    const partEnabled = !!partitioning?.enabled;
    let partColumn = null;
    let partInterval = null;
    if (partEnabled) {
      partColumn = String(partitioning?.column || '').trim();
      partInterval = String(partitioning?.interval || 'day');
      if (!isIdent(partColumn)) throw new Error('Некорректная колонка партиций');
      if (!['day', 'month'].includes(partInterval)) throw new Error('Некорректный интервал партиций (day|month)');
      // must exist in columns
      if (!cols.some((c) => c.name === partColumn)) {
        throw new Error(`Колонка партиций "${partColumn}" должна быть среди колонок таблицы`);
      }
    }

    await ensureSchema(schema_name);

    // CREATE TABLE
    const colsSql = cols.map((c) => `${qIdent(c.name)} ${c.type}`).join(',\n  ');

    const baseCreate = `
      CREATE TABLE IF NOT EXISTS ${qPath(schema_name, table_name)} (
        ${colsSql}
      )
    `.trim();

    if (partEnabled) {
      await pool.query(`${baseCreate} PARTITION BY RANGE (${qIdent(partColumn)});`);
    } else {
      await pool.query(`${baseCreate};`);
    }

    await commentTable(schema_name, table_name, String(description || '').trim());
    for (const c of cols) {
      await commentColumn(schema_name, table_name, c.name, c.desc);
    }

    // create "today" partition (optional) – to avoid confusion, create current partition only
    if (partEnabled) {
      // partition name: <table>__p_YYYYMMDD or __p_YYYYMM
      const now = new Date();
      const yyyy = now.getUTCFullYear();
      const mm = String(now.getUTCMonth() + 1).padStart(2, '0');
      const dd = String(now.getUTCDate()).padStart(2, '0');

      let pName, from, to;
      if (partInterval === 'day') {
        pName = `${table_name}__p_${yyyy}${mm}${dd}`;
        from = `${yyyy}-${mm}-${dd}`;
        const next = new Date(Date.UTC(yyyy, Number(mm) - 1, Number(dd) + 1));
        const nY = next.getUTCFullYear();
        const nM = String(next.getUTCMonth() + 1).padStart(2, '0');
        const nD = String(next.getUTCDate()).padStart(2, '0');
        to = `${nY}-${nM}-${nD}`;
      } else {
        pName = `${table_name}__p_${yyyy}${mm}`;
        from = `${yyyy}-${mm}-01`;
        const next = new Date(Date.UTC(yyyy, Number(mm), 1));
        const nY = next.getUTCFullYear();
        const nM = String(next.getUTCMonth() + 1).padStart(2, '0');
        to = `${nY}-${nM}-01`;
      }

      if (!isIdent(pName)) {
        // fallback just in case
        pName = `${table_name}__p_cur`;
      }

      await pool.query(`
        CREATE TABLE IF NOT EXISTS ${qIdent(schema_name)}.${qIdent(pName)}
        PARTITION OF ${qPath(schema_name, table_name)}
        FOR VALUES FROM ($1) TO ($2);
      `, [from, to]);
    }

    // insert test row
    if (test_row && typeof test_row === 'object' && !Array.isArray(test_row)) {
      const keys = Object.keys(test_row);
      if (keys.length > 0) {
        // validate keys are existing columns
        for (const k of keys) {
          if (!isIdent(k)) throw new Error(`Некорректный ключ в test_row: ${k}`);
          if (!cols.some((c) => c.name === k)) throw new Error(`test_row содержит неизвестную колонку: ${k}`);
        }
        const colsList = keys.map(qIdent).join(', ');
        const vals = keys.map((_, i) => `$${i + 1}`).join(', ');
        const params = keys.map((k) => test_row[k]);
        await pool.query(`INSERT INTO ${qPath(schema_name, table_name)} (${colsList}) VALUES (${vals});`, params);
      }
    }

    res.json({ ok: true, schema: schema_name, table: table_name });
  } catch (e) {
    res.status(400).json({ error: 'create_failed', details: String(e?.message || e) });
  }
});

// -------- COLUMNS --------
tableBuilderRouter.get('/table/columns', async (req, res) => {
  try {
    const schema = String(req.query.schema || '');
    const table = String(req.query.table || '');
    if (!isIdent(schema) || !isIdent(table)) throw new Error('Некорректная схема/таблица');

    const r = await pool.query(
      `
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_schema=$1 AND table_name=$2
      ORDER BY ordinal_position
      `,
      [schema, table]
    );

    res.json({ columns: r.rows });
  } catch (e) {
    res.status(400).json({ error: 'columns_failed', details: String(e?.message || e) });
  }
});

// -------- ROWS (with __ctid) --------
tableBuilderRouter.get('/table/rows', async (req, res) => {
  try {
    const schema = String(req.query.schema || '');
    const table = String(req.query.table || '');
    const limit = Math.min(Number(req.query.limit || 5), 200);
    const offset = Math.max(Number(req.query.offset || 0), 0);

    if (!isIdent(schema) || !isIdent(table)) throw new Error('Некорректная схема/таблица');

    // include ctid as __ctid for delete
    const r = await pool.query(
      `SELECT ctid::text AS "__ctid", * FROM ${qPath(schema, table)} LIMIT $1 OFFSET $2;`,
      [limit, offset]
    );

    res.json({ rows: r.rows, limit, offset });
  } catch (e) {
    res.status(400).json({ error: 'rows_failed', details: String(e?.message || e) });
  }
});

// -------- ADD COLUMN --------
tableBuilderRouter.post('/table/column/add', requireDataAdmin, async (req, res) => {
  try {
    const schema = String(req.body?.schema || '');
    const table = String(req.body?.table || '');
    const column_name = String(req.body?.column_name || '');
    const data_type = String(req.body?.data_type || '');
    const description = String(req.body?.description || '').trim();

    if (!isIdent(schema) || !isIdent(table) || !isIdent(column_name)) throw new Error('Некорректные идентификаторы');

    const sqlType = mapType(data_type);

    await pool.query(`ALTER TABLE ${qPath(schema, table)} ADD COLUMN IF NOT EXISTS ${qIdent(column_name)} ${sqlType};`);
    await commentColumn(schema, table, column_name, description);

    res.json({ ok: true });
  } catch (e) {
    res.status(400).json({ error: 'add_column_failed', details: String(e?.message || e) });
  }
});

// -------- DROP COLUMN --------
tableBuilderRouter.post('/table/column/drop', requireDataAdmin, async (req, res) => {
  try {
    const schema = String(req.body?.schema || '');
    const table = String(req.body?.table || '');
    const column_name = String(req.body?.column_name || '');

    if (!isIdent(schema) || !isIdent(table) || !isIdent(column_name)) throw new Error('Некорректные идентификаторы');

    await pool.query(`ALTER TABLE ${qPath(schema, table)} DROP COLUMN IF EXISTS ${qIdent(column_name)} CASCADE;`);
    res.json({ ok: true });
  } catch (e) {
    res.status(400).json({ error: 'drop_column_failed', details: String(e?.message || e) });
  }
});

// -------- INSERT ROW --------
tableBuilderRouter.post('/table/row/insert', requireDataAdmin, async (req, res) => {
  try {
    const schema = String(req.body?.schema || '');
    const table = String(req.body?.table || '');
    const row = req.body?.row;

    if (!isIdent(schema) || !isIdent(table)) throw new Error('Некорректная схема/таблица');
    if (!row || typeof row !== 'object' || Array.isArray(row)) throw new Error('row должен быть объектом');

    const keys = Object.keys(row);
    if (!keys.length) throw new Error('row пустой');

    for (const k of keys) {
      if (!isIdent(k)) throw new Error(`Некорректная колонка: ${k}`);
    }

    const colsList = keys.map(qIdent).join(', ');
    const vals = keys.map((_, i) => `$${i + 1}`).join(', ');
    const params = keys.map((k) => row[k]);

    await pool.query(`INSERT INTO ${qPath(schema, table)} (${colsList}) VALUES (${vals});`, params);

    res.json({ ok: true });
  } catch (e) {
    res.status(400).json({ error: 'insert_failed', details: String(e?.message || e) });
  }
});

// -------- DELETE ROW by CTID --------
tableBuilderRouter.post('/table/row/delete', requireDataAdmin, async (req, res) => {
  try {
    const schema = String(req.body?.schema || '');
    const table = String(req.body?.table || '');
    const ctid = String(req.body?.ctid || '');

    if (!isIdent(schema) || !isIdent(table)) throw new Error('Некорректная схема/таблица');
    if (!ctid || !/^\(\d+,\d+\)$/.test(ctid)) throw new Error('Некорректный ctid');

    await pool.query(`DELETE FROM ${qPath(schema, table)} WHERE ctid = $1;`, [ctid]);

    res.json({ ok: true });
  } catch (e) {
    res.status(400).json({ error: 'delete_row_failed', details: String(e?.message || e) });
  }
});

// -------- DROP TABLE --------
tableBuilderRouter.post('/table/drop', requireDataAdmin, async (req, res) => {
  try {
    const schema = String(req.body?.schema || '');
    const table = String(req.body?.table || '');

    if (!isIdent(schema) || !isIdent(table)) throw new Error('Некорректная схема/таблица');

    await pool.query(`DROP TABLE IF EXISTS ${qPath(schema, table)} CASCADE;`);
    res.json({ ok: true });
  } catch (e) {
    res.status(400).json({ error: 'drop_table_failed', details: String(e?.message || e) });
  }
});
