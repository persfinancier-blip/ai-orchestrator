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
  const role = String(req.header('X-AO-ROLE') || '').trim();
  if (role !== 'data_admin') {
    return res.status(403).json({ error: 'forbidden', details: 'Требуется роль data_admin' });
  }
  next();
}

function qIdent(name) {
  // простое экранирование идентификаторов Postgres
  const s = String(name ?? '');
  if (!s) throw new Error('empty_identifier');
  return `"${s.replaceAll('"', '""')}"`;
}

function normalizeType(t) {
  const x = String(t || '').trim().toLowerCase();
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
  if (!allowed.has(x)) throw new Error(`unsupported_type:${t}`);
  if (x === 'int') return 'integer';
  if (x === 'timestamptz') return 'timestamp with time zone';
  return x;
}

function validateNamePart(label, v) {
  const s = String(v || '').trim();
  if (!s) throw new Error(`${label}_required`);
  // мягкая валидация: буквы/цифры/подчеркивание, без точек
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(s)) {
    throw new Error(`${label}_invalid: используйте латиницу/цифры/_ и без точек`);
  }
  return s;
}

/**
 * GET /ai-orchestrator/api/tables
 * Возвращает:
 * - drafts: (пока пусто/или история если вы используете)
 * - existing_tables: реальные таблицы/представления из базы
 */
tableBuilderRouter.get('/tables', async (_req, res) => {
  try {
    const existing = await pool.query(`
      SELECT table_schema AS schema_name, table_name
      FROM information_schema.tables
      WHERE table_type='BASE TABLE'
        AND table_schema NOT IN ('pg_catalog','information_schema')
      ORDER BY table_schema, table_name
    `);

    // черновики: если таблицы не созданы — просто отдаём пусто
    let drafts = [];
    try {
      const d = await pool.query(`
        SELECT
          id::text,
          schema_name,
          table_name,
          table_class,
          status,
          description,
          created_at,
          applied_at,
          last_error,
          options,
          fields
        FROM public.ao_table_definitions
        ORDER BY created_at DESC
        LIMIT 50
      `);
      drafts = d.rows || [];
    } catch {
      drafts = [];
    }

    res.json({
      drafts,
      existing_tables: existing.rows || []
    });
  } catch (e) {
    res.status(500).json({ error: 'tables_list_failed', details: String(e?.message || e) });
  }
});

/**
 * GET /ai-orchestrator/api/preview?schema=...&table=...&limit=5
 * Возвращает 5 строк для предпросмотра.
 */
tableBuilderRouter.get('/preview', async (req, res) => {
  try {
    const schema = validateNamePart('schema', req.query.schema);
    const table = validateNamePart('table', req.query.table);
    const limit = Math.max(1, Math.min(50, Number(req.query.limit || 5)));

    const fullName = `${qIdent(schema)}.${qIdent(table)}`;
    const r = await pool.query(`SELECT * FROM ${fullName} LIMIT $1`, [limit]);
    res.json({ rows: r.rows || [] });
  } catch (e) {
    res.status(400).json({ error: 'preview_failed', details: String(e?.message || e) });
  }
});

/**
 * POST /ai-orchestrator/api/tables/create
 * Создаёт схему/таблицу/поля (+ партиции опционально) и опционально вставляет 1 тестовую строку.
 *
 * body:
 * {
 *   schema_name, table_name, table_class?, description?,
 *   columns: [{field_name, field_type, description?}],
 *   partitioning?: { enabled: true/false, column?: string, interval?: 'day'|'month' },
 *   test_row?: object|null
 * }
 */
tableBuilderRouter.post('/tables/create', requireDataAdmin, async (req, res) => {
  const client = await pool.connect();
  try {
    const schema_name = validateNamePart('schema_name', req.body?.schema_name);
    const table_name = validateNamePart('table_name', req.body?.table_name);

    const description = String(req.body?.description || '').trim();
    const table_class = String(req.body?.table_class || 'custom').trim();

    const columns = Array.isArray(req.body?.columns) ? req.body.columns : [];
    if (!columns.length) throw new Error('columns_required');

    // колонки
    const seen = new Set();
    const normalizedColumns = columns.map((c, idx) => {
      const field_name = validateNamePart(`columns[${idx}].field_name`, c.field_name);
      if (seen.has(field_name)) throw new Error(`duplicate_field:${field_name}`);
      seen.add(field_name);

      const field_type = normalizeType(c.field_type);
      const colDesc = String(c.description || '').trim();

      return { field_name, field_type, description: colDesc };
    });

    const partitioning = req.body?.partitioning || { enabled: false };
    const partitionEnabled = !!partitioning?.enabled;
    const partitionColumn = partitionEnabled ? validateNamePart('partitioning.column', partitioning.column || '') : null;
    const partInterval = partitionEnabled ? String(partitioning.interval || 'day') : 'day';
    if (partitionEnabled && !['day', 'month'].includes(partInterval)) {
      throw new Error('partitioning.interval must be day|month');
    }

    const test_row = req.body?.test_row ?? null;
    if (test_row !== null && (typeof test_row !== 'object' || Array.isArray(test_row))) {
      throw new Error('test_row must be object or null');
    }

    const fullName = `${qIdent(schema_name)}.${qIdent(table_name)}`;

    // строим SQL колонок
    const colsSql = normalizedColumns
      .map((c) => `${qIdent(c.field_name)} ${c.field_type}`)
      .join(', ');

    await client.query('BEGIN');

    // 1) schema
    await client.query(`CREATE SCHEMA IF NOT EXISTS ${qIdent(schema_name)}`);

    // 2) table
    if (!partitionEnabled) {
      await client.query(`CREATE TABLE IF NOT EXISTS ${fullName} (${colsSql})`);
    } else {
      // база партиционированная
      const pcol = partitionColumn;
      await client.query(`CREATE TABLE IF NOT EXISTS ${fullName} (${colsSql}) PARTITION BY RANGE (${qIdent(pcol)})`);

      // минимальная авто-настройка: создаём "текущую" партицию + default
      // day: [today, tomorrow)
      // month: [monthStart, nextMonthStart)
      const partToday = `${qIdent(schema_name)}.${qIdent(`${table_name}__p_today`)}`;
      const partMonth = `${qIdent(schema_name)}.${qIdent(`${table_name}__p_month`)}`;
      const partDefault = `${qIdent(schema_name)}.${qIdent(`${table_name}__p_default`)}`;

      if (partInterval === 'day') {
        await client.query(
          `CREATE TABLE IF NOT EXISTS ${partToday} PARTITION OF ${fullName}
           FOR VALUES FROM (CURRENT_DATE) TO (CURRENT_DATE + INTERVAL '1 day')`
        );
      } else {
        await client.query(
          `CREATE TABLE IF NOT EXISTS ${partMonth} PARTITION OF ${fullName}
           FOR VALUES FROM (date_trunc('month', CURRENT_DATE)::date)
           TO (date_trunc('month', CURRENT_DATE)::date + INTERVAL '1 month')`
        );
      }

      await client.query(`CREATE TABLE IF NOT EXISTS ${partDefault} PARTITION OF ${fullName} DEFAULT`);
    }

    // 3) комментарии (по желанию)
    if (description) {
      await client.query(`COMMENT ON TABLE ${fullName} IS $1`, [description]);
    }
    for (const c of normalizedColumns) {
      if (c.description) {
        await client.query(`COMMENT ON COLUMN ${fullName}.${qIdent(c.field_name)} IS $1`, [c.description]);
      }
    }

    // 4) тестовая запись (если дали)
    if (test_row) {
      const keys = Object.keys(test_row);
      if (!keys.length) throw new Error('test_row_empty');
      const cols = keys.map((k) => qIdent(k)).join(', ');
      const vals = keys.map((_, i) => `$${i + 1}`).join(', ');

      const params = keys.map((k) => test_row[k]);
      await client.query(`INSERT INTO ${fullName} (${cols}) VALUES (${vals})`, params);
    }

    await client.query('COMMIT');

    res.json({
      ok: true,
      created: { schema_name, table_name, full_name: `${schema_name}.${table_name}` },
      table_class
    });
  } catch (e) {
    try {
      await pool.query('ROLLBACK');
    } catch {}
    res.status(400).json({ error: 'create_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});
