// core/orchestrator/modules/advertising/interface/server/tableBuilder.mjs
import express from 'express';
import { pool } from './db.mjs';

export const tableBuilderRouter = express.Router();

tableBuilderRouter.use(express.json({ limit: '4mb' }));

function requireDataAdmin(req, res, next) {
  const role = String(req.header('X-AO-ROLE') || '').toLowerCase();
  if (role !== 'data_admin') {
    return res.status(403).json({ error: 'forbidden', details: 'Нужна роль data_admin (X-AO-ROLE: data_admin)' });
  }
  next();
}

function isSafeIdent(s) {
  return /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(s);
}

function qIdent(s) {
  // двойные кавычки для безопасной работы с регистром/ключевыми словами
  return `"${String(s).replace(/"/g, '""')}"`;
}

function mapType(t) {
  const v = String(t || '').toLowerCase();
  const allowed = new Set(['text', 'int', 'bigint', 'numeric', 'boolean', 'date', 'timestamptz', 'jsonb', 'uuid']);
  if (!allowed.has(v)) throw new Error(`unsupported_type: ${t}`);
  if (v === 'int') return 'integer';
  if (v === 'timestamptz') return 'timestamptz';
  return v;
}

async function listExistingTables() {
  // Берём реальные таблицы из information_schema (кроме системных)
  const r = await pool.query(
    `
    SELECT table_schema AS schema_name, table_name
    FROM information_schema.tables
    WHERE table_type='BASE TABLE'
      AND table_schema NOT IN ('pg_catalog','information_schema')
    ORDER BY table_schema, table_name
  `
  );
  return r.rows || [];
}

/**
 * GET /ai-orchestrator/api/tables
 * Возвращает:
 * - drafts (если используются)
 * - existing_tables (реальные таблицы, которые есть в базе)
 */
tableBuilderRouter.get('/tables', async (_req, res) => {
  try {
    const existing_tables = await listExistingTables();

    // drafts — не обязательны, но пусть история остаётся, если таблицы meta существуют
    let drafts = [];
    try {
      const d = await pool.query(
        `
        SELECT
          d.id::text,
          d.schema_name,
          d.table_name,
          d.table_class,
          d.status,
          d.description,
          d.created_at,
          d.applied_at,
          d.last_error,
          d.options
        FROM public.ao_table_definitions d
        WHERE d.status IN ('draft','error')
        ORDER BY d.created_at DESC
        LIMIT 50
      `
      );

      const ids = d.rows.map((x) => x.id);
      const fieldsBy = new Map();

      if (ids.length) {
        const f = await pool.query(
          `
          SELECT
            table_definition_id::text AS id,
            field_name,
            field_type,
            description
          FROM public.ao_table_fields
          WHERE table_definition_id = ANY($1::uuid[])
          ORDER BY created_at ASC
        `,
          [ids]
        );

        for (const row of f.rows) {
          if (!fieldsBy.has(row.id)) fieldsBy.set(row.id, []);
          fieldsBy.get(row.id).push({
            field_name: row.field_name,
            field_type: row.field_type,
            description: row.description
          });
        }
      }

      drafts = d.rows.map((x) => ({ ...x, fields: fieldsBy.get(x.id) || [] }));
    } catch {
      drafts = [];
    }

    return res.json({ drafts, existing_tables });
  } catch (e) {
    return res.status(500).json({ error: 'tables_list_failed', details: String(e?.message || e) });
  }
});

/**
 * GET /ai-orchestrator/api/preview?schema=...&table=...&limit=5
 * Предпросмотр 5 строк
 */
tableBuilderRouter.get('/preview', async (req, res) => {
  try {
    const schema = String(req.query.schema || '');
    const table = String(req.query.table || '');
    const limit = Math.max(1, Math.min(50, Number(req.query.limit || 5)));

    if (!isSafeIdent(schema) || !isSafeIdent(table)) {
      return res.status(400).json({ error: 'invalid_ident', details: 'Некорректное имя схемы/таблицы' });
    }

    const sql = `SELECT * FROM ${qIdent(schema)}.${qIdent(table)} LIMIT ${limit}`;
    const r = await pool.query(sql);
    return res.json({ rows: r.rows || [] });
  } catch (e) {
    return res.status(500).json({ error: 'preview_failed', details: String(e?.message || e) });
  }
});

/**
 * POST /ai-orchestrator/api/tables/create
 * Создаёт схему → таблицу → колонки (сразу, без черновиков).
 * body:
 * {
 *   schema_name, table_name, columns:[{field_name, field_type, description?}],
 *   partitioning:{enabled:boolean, column?, interval?},
 *   test_row?: object
 * }
 */
tableBuilderRouter.post('/tables/create', requireDataAdmin, async (req, res) => {
  const body = req.body || {};
  const schema_name = String(body.schema_name || '').trim();
  const table_name = String(body.table_name || '').trim();
  const columns = Array.isArray(body.columns) ? body.columns : [];
  const partitioning = body.partitioning || { enabled: false };
  const test_row = body.test_row || null;

  try {
    if (!isSafeIdent(schema_name)) throw new Error('invalid_schema_name');
    if (!isSafeIdent(table_name)) throw new Error('invalid_table_name');

    const cleanCols = columns
      .map((c) => ({
        field_name: String(c.field_name || '').trim(),
        field_type: mapType(c.field_type),
        description: String(c.description || '')
      }))
      .filter((c) => c.field_name);

    if (!cleanCols.length) throw new Error('no_columns');

    for (const c of cleanCols) {
      if (!isSafeIdent(c.field_name)) throw new Error(`invalid_column_name: ${c.field_name}`);
    }

    const partEnabled = !!partitioning?.enabled;
    const partColumn = String(partitioning?.column || '').trim();
    const partInterval = String(partitioning?.interval || 'day').toLowerCase();

    if (partEnabled) {
      if (!partColumn || !isSafeIdent(partColumn)) throw new Error('invalid_partition_column');
      if (!['day', 'month'].includes(partInterval)) throw new Error('invalid_partition_interval');
      const exists = cleanCols.some((c) => c.field_name === partColumn);
      if (!exists) throw new Error('partition_column_not_in_columns');
    }

    // 1) schema
    await pool.query(`CREATE SCHEMA IF NOT EXISTS ${qIdent(schema_name)}`);

    // 2) table DDL
    const colSql = cleanCols.map((c) => `${qIdent(c.field_name)} ${c.field_type}`).join(',\n  ');

    const fullName = `${qIdent(schema_name)}.${qIdent(table_name)}`;

    if (!partEnabled) {
      await pool.query(`CREATE TABLE IF NOT EXISTS ${fullName} (\n  ${colSql}\n)`);
    } else {
      // partitioned table
      await pool.query(
        `CREATE TABLE IF NOT EXISTS ${fullName} (\n  ${colSql}\n) PARTITION BY RANGE (${qIdent(partColumn)})`
      );

      // минимальная авто-настройка: создаём "текущую" партицию + default
      // day: [today, tomorrow)
      // month: [monthStart, nextMonthStart)
      if (partInterval === 'day') {
        await pool.query(
          `CREATE TABLE IF NOT EXISTS ${fullName}__p_today PARTITION OF ${fullName}
           FOR VALUES FROM (CURRENT_DATE) TO (CURRENT_DATE + INTERVAL '1 day')`
        );
      } else {
        await pool.query(
          `CREATE TABLE IF NOT EXISTS ${fullName}__p_month PARTITION OF ${fullName}
           FOR VALUES FROM (date_trunc('month', CURRENT_DATE)::date)
           TO (date_trunc('month', CURRENT_DATE)::date + INTERVAL '1 month')`
        );
      }

      await pool.query(`CREATE TABLE IF NOT EXISTS ${fullName}__p_default PARTITION OF ${fullName} DEFAULT`);
    }

    // 3) comments (опционально)
    for (const c of cleanCols) {
      if (c.description) {
        await pool.query(
          `COMMENT ON COLUMN ${fullName}.${qIdent(c.field_name)} IS $1`,
          [c.description]
        );
      }
    }

    // 4) test row (опционально)
    if (test_row) {
      if (typeof test_row !== 'object' || Array.isArray(test_row)) throw new Error('test_row_invalid');

      const keys = Object.keys(test_row);
      if (!keys.length) throw new Error('test_row_empty');

      // берём только те ключи, которые реально есть в колонках
      const allowed = new Set(cleanCols.map((c) => c.field_name));
      const useKeys = keys.filter((k) => allowed.has(k));

      if (!useKeys.length) throw new Error('test_row_no_matching_columns');

      const colsList = useKeys.map((k) => qIdent(k)).join(',');
      const params = useKeys.map((_, i) => `$${i + 1}`).join(',');
      const values = useKeys.map((k) => test_row[k]);

      await pool.query(`INSERT INTO ${fullName} (${colsList}) VALUES (${params})`, values);
    }

    return res.json({
      ok: true,
      created: `${schema_name}.${table_name}`,
      partitioning: partEnabled ? { enabled: true, column: partColumn, interval: partInterval } : { enabled: false }
    });
  } catch (e) {
    return res.status(500).json({ error: 'create_failed', details: String(e?.message || e) });
  }
});
