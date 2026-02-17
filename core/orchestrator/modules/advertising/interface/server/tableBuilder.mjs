// core/orchestrator/modules/advertising/interface/server/tableBuilder.mjs
import express from 'express';
import { pool } from './db.mjs';

export const tableBuilderRouter = express.Router();

tableBuilderRouter.use(express.json({ limit: '4mb' }));

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
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
    throw new Error(`invalid_identifier:${name}`);
  }
  return `"${name.replace(/"/g, '""')}"`;
}

function qLiteral(v) {
  if (v === null || v === undefined) return 'NULL';
  const s = String(v);
  return `'${s.replace(/'/g, "''")}'`;
}

function normalizeColumns(columns) {
  if (!Array.isArray(columns) || columns.length === 0) throw new Error('columns_required');

  const out = [];
  for (const c of columns) {
    const field_name = String(c?.field_name || '').trim();
    const field_type = String(c?.field_type || '').trim().toLowerCase();
    const description = c?.description ? String(c.description).trim() : '';

    if (!field_name) throw new Error('field_name_required');
    if (!field_type) throw new Error('field_type_required');

    // минимальный allow-list типов (расширишь позже)
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
    if (!allowed.has(field_type)) throw new Error(`unsupported_type:${field_type}`);

    // normalize
    const ft =
      field_type === 'int' ? 'integer' : field_type === 'timestamptz' ? 'timestamptz' : field_type;

    out.push({ field_name, field_type: ft, description });
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

/**
 * Preview: 5 строк из указанной таблицы
 * GET /preview?schema=...&table=...&limit=5
 */
tableBuilderRouter.get('/preview', async (req, res) => {
  try {
    const schema = String(req.query.schema || '').trim();
    const table = String(req.query.table || '').trim();
    const limit = Math.min(Math.max(Number(req.query.limit || 5), 1), 50);

    if (!schema || !table) return res.status(400).json({ error: 'bad_request' });

    const fullName = `${qIdent(schema)}.${qIdent(table)}`;
    const r = await pool.query(`SELECT * FROM ${fullName} LIMIT $1`, [limit]);
    return res.json({ rows: r.rows || [] });
  } catch (e) {
    return res.status(500).json({ error: 'preview_failed', details: String(e?.message || e) });
  }
});

/**
 * GET /tables
 * возвращает таблицы, которые реально есть в БД (и может расшириться до "drafts", если захочешь)
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

    const columns = normalizeColumns(req.body?.columns || []);

    const partitioning = req.body?.partitioning || { enabled: false };
    const partition_enabled = Boolean(partitioning?.enabled);
    const partition_column = String(partitioning?.column || '').trim();
    const partition_interval = String(partitioning?.interval || 'day').trim(); // day|month

    const test_row = req.body?.test_row || null; // object or null

    if (!schema_name) throw new Error('schema_name_required');
    if (!table_name) throw new Error('table_name_required');

    await client.query('BEGIN');

    // 1) schema
    await client.query(`CREATE SCHEMA IF NOT EXISTS ${qIdent(schema_name)}`);

    // 2) create table ddl
    const fullName = `${qIdent(schema_name)}.${qIdent(table_name)}`;

    const colsSql = columns
      .map((c) => `${qIdent(c.field_name)} ${c.field_type}`)
      .join(',\n      ');

    if (partition_enabled) {
      if (!partition_column) throw new Error('partition_column_required');
      // проверяем, что колонка существует
      const has = columns.some((c) => c.field_name === partition_column);
      if (!has) throw new Error(`partition_column_not_found:${partition_column}`);

      await client.query(`
        CREATE TABLE IF NOT EXISTS ${fullName} (
          ${colsSql}
        ) PARTITION BY RANGE (${qIdent(partition_column)});
      `);

      // 2.1) авто-партиции: today/month + default
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

    // 3) comments (ВАЖНО: COMMENT ON нельзя параметризовать $1)
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
      if (typeof test_row !== 'object' || Array.isArray(test_row)) {
        throw new Error('test_row_must_be_object');
      }

      const keys = Object.keys(test_row);
      if (keys.length) {
        // разрешаем вставлять только те поля, которые есть в схеме
        const allowed = new Set(columns.map((c) => c.field_name));
        const insertKeys = keys.filter((k) => allowed.has(k));

        if (!insertKeys.length) {
          throw new Error('test_row_has_no_known_fields');
        }

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
