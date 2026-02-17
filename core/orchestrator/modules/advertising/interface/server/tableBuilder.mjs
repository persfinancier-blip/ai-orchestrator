// core/orchestrator/modules/advertising/interface/server/tableBuilder.mjs
import express from 'express';
import { pool } from './db.mjs';

export const tableBuilderRouter = express.Router();

tableBuilderRouter.use(express.json({ limit: '8mb' }));

/**
 * Примитивная роль: UI кладёт X-AO-ROLE.
 * Сейчас это больше "переключатель", чем настоящая безопасность.
 */
function requireDataAdmin(req, res, next) {
  const role = (req.header('X-AO-ROLE') || '').toLowerCase();
  if (role !== 'data_admin') {
    return res.status(403).json({ error: 'forbidden', details: 'Требуется роль data_admin' });
  }
  return next();
}

function qIdent(name) {
  if (!name || typeof name !== 'string') throw new Error('invalid_identifier');
  const s = name.trim();
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(s)) throw new Error(`invalid_identifier:${s}`);
  return `"${s.replace(/"/g, '""')}"`;
}

function qLiteral(v) {
  if (v === null || v === undefined) return 'NULL';
  const s = String(v);
  return `'${s.replace(/'/g, "''")}'`;
}

function normalizeColumns(columns) {
  if (!Array.isArray(columns) || columns.length === 0) throw new Error('columns_required');

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
    'uuid'
  ]);

  const out = [];
  for (const c of columns) {
    const field_name = String(c?.field_name || '').trim();
    const field_type_raw = String(c?.field_type || '').trim().toLowerCase();
    const description = c?.description ? String(c.description).trim() : '';

    if (!field_name) throw new Error('field_name_required');
    if (!field_type_raw) throw new Error('field_type_required');

    if (!allowed.has(field_type_raw)) throw new Error(`unsupported_type:${field_type_raw}`);

    const field_type =
      field_type_raw === 'int'
        ? 'integer'
        : field_type_raw === 'timestamp with time zone'
          ? 'timestamptz'
          : field_type_raw;

    out.push({ field_name, field_type, description });
  }

  // уникальность имен
  const seen = new Set();
  for (const c of out) {
    const k = c.field_name.toLowerCase();
    if (seen.has(k)) throw new Error(`duplicate_field:${c.field_name}`);
    seen.add(k);
  }

  return out;
}

async function listExistingTables() {
  const q = `
    SELECT table_schema AS schema_name, table_name
    FROM information_schema.tables
    WHERE table_type='BASE TABLE'
      AND table_schema NOT IN ('pg_catalog','information_schema')
    ORDER BY table_schema, table_name;
  `;
  const r = await pool.query(q);
  return r.rows || [];
}

async function listColumns(schema, table) {
  const q = `
    SELECT
      column_name,
      data_type,
      udt_name,
      is_nullable,
      column_default
    FROM information_schema.columns
    WHERE table_schema=$1 AND table_name=$2
    ORDER BY ordinal_position;
  `;
  const r = await pool.query(q, [schema, table]);
  return r.rows || [];
}

function mapTypeForUI(col) {
  // информационная выдача для UI
  const dt = (col.data_type || '').toLowerCase();
  const udt = (col.udt_name || '').toLowerCase();

  if (dt === 'timestamp with time zone') return 'timestamptz';
  if (dt === 'jsonb') return 'jsonb';
  if (dt === 'uuid') return 'uuid';
  if (dt === 'date') return 'date';
  if (dt === 'boolean') return 'boolean';
  if (dt === 'integer') return 'int';
  if (dt === 'bigint') return 'bigint';
  if (dt === 'numeric') return 'numeric';
  if (dt === 'text' || udt === 'text') return 'text';

  // fallback
  return dt || udt || 'text';
}

/**
 * GET /tables
 * возвращает таблицы, которые реально есть в БД
 */
tableBuilderRouter.get('/tables', async (_req, res) => {
  try {
    const existing_tables = await listExistingTables();
    return res.json({ drafts: [], existing_tables });
  } catch (e) {
    return res.status(500).json({ error: 'tables_list_failed', details: String(e?.message || e) });
  }
});

/**
 * GET /meta?schema=...&table=...
 * отдает колонки для UI (для построения таблицы ввода строки/кнопок)
 */
tableBuilderRouter.get('/meta', async (req, res) => {
  try {
    const schema = String(req.query.schema || '').trim();
    const table = String(req.query.table || '').trim();
    if (!schema || !table) return res.status(400).json({ error: 'bad_request' });

    // validate identifiers
    qIdent(schema);
    qIdent(table);

    const cols = await listColumns(schema, table);
    const columns = cols.map((c) => ({
      name: c.column_name,
      type: mapTypeForUI(c),
      nullable: String(c.is_nullable || '') === 'YES',
      default: c.column_default || null
    }));

    return res.json({ columns });
  } catch (e) {
    return res.status(500).json({ error: 'meta_failed', details: String(e?.message || e) });
  }
});

/**
 * Preview: 5 строк из указанной таблицы
 * GET /preview?schema=...&table=...&limit=5
 * ВАЖНО: добавляем __ctid чтобы можно было удалять строки
 */
tableBuilderRouter.get('/preview', async (req, res) => {
  try {
    const schema = String(req.query.schema || '').trim();
    const table = String(req.query.table || '').trim();
    const limit = Math.min(Math.max(Number(req.query.limit || 5), 1), 50);

    if (!schema || !table) return res.status(400).json({ error: 'bad_request' });

    qIdent(schema);
    qIdent(table);

    const fullName = `${qIdent(schema)}.${qIdent(table)}`;
    const r = await pool.query(`SELECT ctid::text AS "__ctid", * FROM ${fullName} LIMIT $1`, [limit]);

    return res.json({ rows: r.rows || [] });
  } catch (e) {
    return res.status(500).json({ error: 'preview_failed', details: String(e?.message || e) });
  }
});

/**
 * POST /tables/create
 * Создаёт схему/таблицу/поля/партиции (если включены) + вставляет тестовую строку (если задана).
 */
tableBuilderRouter.post('/tables/create', requireDataAdmin, async (req, res) => {
  const client = await pool.connect();
  try {
    const schema_name = String(req.body?.schema_name || '').trim();
    const table_name = String(req.body?.table_name || '').trim();
    const table_class = String(req.body?.table_class || 'custom').trim();
    const description = req.body?.description ? String(req.body.description).trim() : '';

    qIdent(schema_name);
    qIdent(table_name);

    const columns = normalizeColumns(req.body?.columns || []);

    const partitioning = req.body?.partitioning || { enabled: false };
    const partition_enabled = Boolean(partitioning?.enabled);
    const partition_column = String(partitioning?.column || '').trim();
    const partition_interval = String(partitioning?.interval || 'day').trim(); // day|month

    const test_row = req.body?.test_row || null; // object or null

    await client.query('BEGIN');

    // 1) schema
    await client.query(`CREATE SCHEMA IF NOT EXISTS ${qIdent(schema_name)}`);

    // 2) create table ddl
    const fullName = `${qIdent(schema_name)}.${qIdent(table_name)}`;

    const colsSql = columns.map((c) => `${qIdent(c.field_name)} ${c.field_type}`).join(',\n      ');

    if (partition_enabled) {
      if (!partition_column) throw new Error('partition_column_required');
      qIdent(partition_column);

      const has = columns.some((c) => c.field_name === partition_column);
      if (!has) throw new Error(`partition_column_not_found:${partition_column}`);

      await client.query(`
        CREATE TABLE IF NOT EXISTS ${fullName} (
          ${colsSql}
        ) PARTITION BY RANGE (${qIdent(partition_column)});
      `);

      // авто-партиции: today/month + default
      const partToday = `${qIdent(schema_name)}.${qIdent(`${table_name}__p_today`)}`;
      const partMonth = `${qIdent(schema_name)}.${qIdent(`${table_name}__p_month`)}`;
      const partDefault = `${qIdent(schema_name)}.${qIdent(`${table_name}__p_default`)}`;

      if (partition_interval === 'day') {
        await client.query(
          `CREATE TABLE IF NOT EXISTS ${partToday} PARTITION OF ${fullName}
           FOR VALUES FROM (CURRENT_DATE) TO (CURRENT_DATE + INTERVAL '1 day')`
        );
      } else if (partition_interval === 'month') {
        await client.query(
          `CREATE TABLE IF NOT EXISTS ${partMonth} PARTITION OF ${fullName}
           FOR VALUES FROM (date_trunc('month', CURRENT_DATE)::date)
           TO (date_trunc('month', CURRENT_DATE)::date + INTERVAL '1 month')`
        );
      } else {
        throw new Error('partition_interval_invalid');
      }

      await client.query(`CREATE TABLE IF NOT EXISTS ${partDefault} PARTITION OF ${fullName} DEFAULT`);
    } else {
      await client.query(`
        CREATE TABLE IF NOT EXISTS ${fullName} (
          ${colsSql}
        );
      `);
    }

    // 3) comments (COMMENT ON нельзя параметризовать $1)
    if (description) {
      await client.query(`COMMENT ON TABLE ${fullName} IS ${qLiteral(description)}`);
    }
    for (const c of columns) {
      if (c.description) {
        await client.query(
          `COMMENT ON COLUMN ${fullName}.${qIdent(c.field_name)} IS ${qLiteral(c.description)}`
        );
      }
    }

    // 4) тестовая запись
    if (test_row) {
      if (typeof test_row !== 'object' || Array.isArray(test_row)) throw new Error('test_row_must_be_object');

      const keys = Object.keys(test_row);
      if (keys.length) {
        const allowed = new Set(columns.map((c) => c.field_name));
        const insertKeys = keys.filter((k) => allowed.has(k));
        if (!insertKeys.length) throw new Error('test_row_has_no_known_fields');

        const insertCols = insertKeys.map((k) => qIdent(k)).join(', ');
        const values = insertKeys.map((k) => test_row[k]);
        const params = values.map((_, i) => `$${i + 1}`).join(', ');

        await client.query(`INSERT INTO ${fullName} (${insertCols}) VALUES (${params})`, values);
      }
    }

    await client.query('COMMIT');

    return res.json({
      ok: true,
      created: { schema_name, table_name, table_class, partition_enabled },
      message: 'Таблица создана'
    });
  } catch (e) {
    try {
      await client.query('ROLLBACK');
    } catch {
      // ignore
    }
    return res.status(500).json({ error: 'create_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

/**
 * POST /tables/column/add
 * body: { schema, table, column_name, column_type, description? }
 */
tableBuilderRouter.post('/tables/column/add', requireDataAdmin, async (req, res) => {
  const client = await pool.connect();
  try {
    const schema = String(req.body?.schema || '').trim();
    const table = String(req.body?.table || '').trim();
    const column_name = String(req.body?.column_name || '').trim();
    const column_type_raw = String(req.body?.column_type || '').trim().toLowerCase();
    const description = req.body?.description ? String(req.body.description).trim() : '';

    qIdent(schema);
    qIdent(table);
    qIdent(column_name);

    const allowed = new Set(['text', 'int', 'integer', 'bigint', 'numeric', 'boolean', 'date', 'timestamptz', 'jsonb', 'uuid']);
    if (!allowed.has(column_type_raw)) throw new Error(`unsupported_type:${column_type_raw}`);

    const column_type = column_type_raw === 'int' ? 'integer' : column_type_raw;

    const fullName = `${qIdent(schema)}.${qIdent(table)}`;

    await client.query('BEGIN');
    await client.query(`ALTER TABLE ${fullName} ADD COLUMN ${qIdent(column_name)} ${column_type}`);

    if (description) {
      await client.query(`COMMENT ON COLUMN ${fullName}.${qIdent(column_name)} IS ${qLiteral(description)}`);
    }

    await client.query('COMMIT');
    return res.json({ ok: true });
  } catch (e) {
    try {
      await client.query('ROLLBACK');
    } catch {
      // ignore
    }
    return res.status(500).json({ error: 'add_column_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

/**
 * POST /tables/column/drop
 * body: { schema, table, column_name }
 */
tableBuilderRouter.post('/tables/column/drop', requireDataAdmin, async (req, res) => {
  const client = await pool.connect();
  try {
    const schema = String(req.body?.schema || '').trim();
    const table = String(req.body?.table || '').trim();
    const column_name = String(req.body?.column_name || '').trim();

    qIdent(schema);
    qIdent(table);
    qIdent(column_name);

    const fullName = `${qIdent(schema)}.${qIdent(table)}`;

    await client.query('BEGIN');
    await client.query(`ALTER TABLE ${fullName} DROP COLUMN ${qIdent(column_name)} CASCADE`);
    await client.query('COMMIT');

    return res.json({ ok: true });
  } catch (e) {
    try {
      await client.query('ROLLBACK');
    } catch {
      // ignore
    }
    return res.status(500).json({ error: 'drop_column_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

/**
 * POST /tables/row/add
 * body: { schema, table, row: {col:val,...} }
 */
tableBuilderRouter.post('/tables/row/add', requireDataAdmin, async (req, res) => {
  const client = await pool.connect();
  try {
    const schema = String(req.body?.schema || '').trim();
    const table = String(req.body?.table || '').trim();
    const row = req.body?.row;

    qIdent(schema);
    qIdent(table);

    if (!row || typeof row !== 'object' || Array.isArray(row)) {
      throw new Error('row_must_be_object');
    }

    const cols = await listColumns(schema, table);
    const allowed = new Set(cols.map((c) => c.column_name));

    const keys = Object.keys(row).filter((k) => allowed.has(k));
    if (!keys.length) throw new Error('row_has_no_known_fields');

    const fullName = `${qIdent(schema)}.${qIdent(table)}`;

    const insertCols = keys.map((k) => qIdent(k)).join(', ');
    const values = keys.map((k) => row[k]);
    const params = values.map((_, i) => `$${i + 1}`).join(', ');

    await client.query('BEGIN');
    const r = await client.query(
      `INSERT INTO ${fullName} (${insertCols}) VALUES (${params}) RETURNING ctid::text AS "__ctid", *`,
      values
    );
    await client.query('COMMIT');

    return res.json({ ok: true, row: (r.rows || [])[0] || null });
  } catch (e) {
    try {
      await client.query('ROLLBACK');
    } catch {
      // ignore
    }
    return res.status(500).json({ error: 'row_add_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

/**
 * POST /tables/row/delete
 * body: { schema, table, ctid }
 */
tableBuilderRouter.post('/tables/row/delete', requireDataAdmin, async (req, res) => {
  const client = await pool.connect();
  try {
    const schema = String(req.body?.schema || '').trim();
    const table = String(req.body?.table || '').trim();
    const ctid = String(req.body?.ctid || '').trim();

    qIdent(schema);
    qIdent(table);

    if (!ctid) throw new Error('ctid_required');

    const fullName = `${qIdent(schema)}.${qIdent(table)}`;

    await client.query('BEGIN');
    const r = await client.query(`DELETE FROM ${fullName} WHERE ctid::text = $1`, [ctid]);
    await client.query('COMMIT');

    return res.json({ ok: true, deleted: r.rowCount || 0 });
  } catch (e) {
    try {
      await client.query('ROLLBACK');
    } catch {
      // ignore
    }
    return res.status(500).json({ error: 'row_delete_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

/**
 * POST /tables/drop
 * body: { schema, table }
 */
tableBuilderRouter.post('/tables/drop', requireDataAdmin, async (req, res) => {
  const client = await pool.connect();
  try {
    const schema = String(req.body?.schema || '').trim();
    const table = String(req.body?.table || '').trim();

    qIdent(schema);
    qIdent(table);

    const fullName = `${qIdent(schema)}.${qIdent(table)}`;

    await client.query('BEGIN');
    await client.query(`DROP TABLE IF EXISTS ${fullName} CASCADE`);
    await client.query('COMMIT');

    return res.json({ ok: true });
  } catch (e) {
    try {
      await client.query('ROLLBACK');
    } catch {
      // ignore
    }
    return res.status(500).json({ error: 'drop_table_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});
