// core/orchestrator/modules/advertising/interface/server/tableBuilder.mjs
import express from 'express';
import { pool } from './db.mjs';

const router = express.Router();
router.use(express.json({ limit: '4mb' }));

function requireDataAdmin(req, res, next) {
  const role = String(req.header('X-AO-ROLE') || '').toLowerCase();
  if (role !== 'data_admin') return res.status(403).json({ error: 'forbidden: data_admin only' });
  return next();
}

function isValidIdent(name) {
  return typeof name === 'string' && /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name);
}

function qIdent(name) {
  if (!isValidIdent(name)) throw new Error(`Invalid identifier: ${name}`);
  return `"${name}"`;
}

const ALLOWED_TYPES = new Set([
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

function normalizeType(t) {
  const v = String(t || '').trim().toLowerCase();
  if (v === 'integer') return 'int';
  if (v === 'timestamp with time zone') return 'timestamptz';
  return v;
}

async function listExistingTables() {
  // pg_catalog точнее, чем information_schema, и не даёт странных "showcase.advertising" в public
  const sql = `
    SELECT n.nspname AS schema_name, c.relname AS table_name
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
      AND n.nspname NOT IN ('pg_catalog','information_schema')
    ORDER BY n.nspname, c.relname
  `;
  const r = await pool.query(sql);
  return r.rows;
}

router.get('/tables', async (_req, res) => {
  try {
    const defs = await pool.query(
      `SELECT id, schema_name, table_name, table_class, status, description, created_by, created_at, applied_at, applied_by, last_error, options
       FROM ao_table_definitions
       ORDER BY created_at DESC`
    );

    const ids = defs.rows.map((x) => x.id);
    let fieldsByDef = {};
    if (ids.length) {
      const fields = await pool.query(
        `SELECT table_definition_id, field_name, field_type, description, created_at
         FROM ao_table_fields
         WHERE table_definition_id = ANY($1::uuid[])
         ORDER BY created_at ASC`,
        [ids]
      );
      fieldsByDef = fields.rows.reduce((acc, row) => {
        (acc[row.table_definition_id] ||= []).push({
          field_name: row.field_name,
          field_type: row.field_type,
          description: row.description,
          created_at: row.created_at,
        });
        return acc;
      }, {});
    }

    const existingTables = await listExistingTables();

    return res.json({
      drafts: defs.rows.map((d) => ({ ...d, fields: fieldsByDef[d.id] || [] })),
      existing_tables: existingTables,
    });
  } catch (e) {
    return res.status(500).json({ error: 'tables_list_failed', details: String(e?.message || e) });
  }
});

router.get('/columns', async (req, res) => {
  try {
    const schema = String(req.query.schema || '');
    const table = String(req.query.table || '');
    if (!isValidIdent(schema) || !isValidIdent(table)) return res.status(400).json({ error: 'invalid schema/table' });

    const r = await pool.query(
      `
      SELECT column_name, data_type, udt_name, is_nullable
      FROM information_schema.columns
      WHERE table_schema=$1 AND table_name=$2
      ORDER BY ordinal_position
      `,
      [schema, table]
    );

    return res.json({ columns: r.rows });
  } catch (e) {
    return res.status(500).json({ error: 'columns_failed', details: String(e?.message || e) });
  }
});

router.get('/preview', async (req, res) => {
  try {
    const schema = String(req.query.schema || '');
    const table = String(req.query.table || '');
    const limit = Math.min(Math.max(Number(req.query.limit || 5), 1), 50);

    if (!isValidIdent(schema) || !isValidIdent(table)) return res.status(400).json({ error: 'invalid schema/table' });

    const sql = `SELECT * FROM ${qIdent(schema)}.${qIdent(table)} ORDER BY 1 DESC LIMIT ${limit}`;
    const r = await pool.query(sql);
    return res.json({ rows: r.rows, limit });
  } catch (e) {
    return res.status(500).json({ error: 'preview_failed', details: String(e?.message || e) });
  }
});

router.post('/tables/draft', requireDataAdmin, async (req, res) => {
  const body = req.body || {};
  const schema_name = body.schema_name;
  const table_name = body.table_name;
  const table_class = body.table_class || 'custom';
  const description = body.description || '';
  const created_by = body.created_by || 'ui';
  const columns = Array.isArray(body.columns) ? body.columns : [];

  // новые опции
  const partitioning = body.partitioning || { enabled: false };
  const test_row = body.test_row ?? null;

  try {
    if (!isValidIdent(schema_name)) return res.status(400).json({ error: 'invalid schema_name' });
    if (!isValidIdent(table_name)) return res.status(400).json({ error: 'invalid table_name' });
    if (!columns.length) return res.status(400).json({ error: 'columns required' });

    for (const c of columns) {
      if (!isValidIdent(c.field_name)) return res.status(400).json({ error: `invalid field_name: ${c.field_name}` });
      const t = normalizeType(c.field_type);
      if (!ALLOWED_TYPES.has(t)) return res.status(400).json({ error: `invalid field_type: ${c.field_type}` });
    }

    if (partitioning?.enabled) {
      const col = String(partitioning.column || '');
      const interval = String(partitioning.interval || 'day');
      if (!isValidIdent(col)) return res.status(400).json({ error: 'invalid partitioning.column' });
      if (!['day', 'month'].includes(interval)) return res.status(400).json({ error: 'invalid partitioning.interval' });
    }

    if (test_row !== null && (typeof test_row !== 'object' || Array.isArray(test_row))) {
      return res.status(400).json({ error: 'test_row must be object or null' });
    }

    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      const def = await client.query(
        `INSERT INTO ao_table_definitions (schema_name, table_name, table_class, status, description, created_by, options)
         VALUES ($1,$2,$3,'draft',$4,$5,$6::jsonb)
         RETURNING id`,
        [schema_name, table_name, table_class, description, created_by, JSON.stringify({ partitioning, test_row })]
      );

      const id = def.rows[0].id;

      for (const c of columns) {
        await client.query(
          `INSERT INTO ao_table_fields (table_definition_id, field_name, field_type, description)
           VALUES ($1,$2,$3,$4)`,
          [id, c.field_name, normalizeType(c.field_type), c.description || '']
        );
      }

      await client.query('COMMIT');
      return res.json({ ok: true, id });
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  } catch (e) {
    return res.status(500).json({ error: 'draft_create_failed', details: String(e?.message || e) });
  }
});

function buildCreateTableSql({ schema_name, table_name, fields, partitioning }) {
  const schema = qIdent(schema_name);
  const table = qIdent(table_name);

  const userCols = fields.map((f) => `${qIdent(f.field_name)} ${normalizeType(f.field_type)}`).join(',\n  ');

  // системные поля всегда добавляем (они нужны всем уровням)
  const baseCols = `
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at timestamptz NOT NULL DEFAULT now(),
  ${userCols}
`.trim();

  if (partitioning?.enabled) {
    const pcol = qIdent(partitioning.column);
    return `
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA IF NOT EXISTS ${schema};

CREATE TABLE IF NOT EXISTS ${schema}.${table} (
  ${baseCols}
) PARTITION BY RANGE (${pcol});
`.trim();
  }

  return `
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA IF NOT EXISTS ${schema};

CREATE TABLE IF NOT EXISTS ${schema}.${table} (
  ${baseCols}
);
`.trim();
}

async function ensurePartitions({ schema_name, table_name, partitioning }) {
  if (!partitioning?.enabled) return;

  const schema = qIdent(schema_name);
  const table = qIdent(table_name);
  const pcol = qIdent(partitioning.column);
  const interval = partitioning.interval || 'day';

  // создаём партиции вперёд на 30 дней (day) или 12 месяцев (month)
  const count = interval === 'day' ? 30 : 12;

  for (let i = 0; i < count; i++) {
    const fromExpr = interval === 'day'
      ? `date_trunc('day', now()) + interval '${i} day'`
      : `date_trunc('month', now()) + interval '${i} month'`;

    const toExpr = interval === 'day'
      ? `date_trunc('day', now()) + interval '${i + 1} day'`
      : `date_trunc('month', now()) + interval '${i + 1} month'`;

    const partName = `${table_name}_${interval}_${String(i).padStart(3, '0')}`;
    const part = qIdent(partName);

    const sql = `
CREATE TABLE IF NOT EXISTS ${schema}.${part}
PARTITION OF ${schema}.${table}
FOR VALUES FROM (${fromExpr}) TO (${toExpr});
`.trim();

    // eslint-disable-next-line no-await-in-loop
    await pool.query(sql);
  }
}

router.post('/tables/apply/:id', requireDataAdmin, async (req, res) => {
  const id = req.params.id;
  const applied_by = (req.body && req.body.applied_by) || 'ui';

  try {
    const def = await pool.query(
      `SELECT id, schema_name, table_name, table_class, status, options
       FROM ao_table_definitions
       WHERE id = $1::uuid`,
      [id]
    );
    if (!def.rows.length) return res.status(404).json({ error: 'draft_not_found' });

    const d = def.rows[0];
    const options = d.options || {};
    const partitioning = options.partitioning || { enabled: false };
    const test_row = options.test_row ?? null;

    const fields = await pool.query(
      `SELECT field_name, field_type
       FROM ao_table_fields
       WHERE table_definition_id = $1::uuid
       ORDER BY created_at ASC`,
      [id]
    );
    if (!fields.rows.length) return res.status(400).json({ error: 'no_fields' });

    const createSql = buildCreateTableSql({
      schema_name: d.schema_name,
      table_name: d.table_name,
      fields: fields.rows,
      partitioning,
    });

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(createSql);

      // партиции (если включено)
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }

    // создаём партиции отдельно (простая логика, но рабочая)
    try {
      await ensurePartitions({ schema_name: d.schema_name, table_name: d.table_name, partitioning });
    } catch (e) {
      // не валим apply, просто пишем ошибку в last_error
      await pool.query(
        `UPDATE ao_table_definitions SET last_error=$2 WHERE id=$1::uuid`,
        [id, `partitioning_failed: ${String(e?.message || e)}`]
      );
    }

    // тестовая запись (если задана)
    if (test_row) {
      try {
        const cols = Object.keys(test_row).filter(isValidIdent);
        const vals = cols.map((k) => test_row[k]);
        const colSql = cols.map(qIdent).join(', ');
        const params = cols.map((_, i) => `$${i + 1}`).join(', ');

        const sql = `INSERT INTO ${qIdent(d.schema_name)}.${qIdent(d.table_name)} (${colSql}) VALUES (${params})`;
        await pool.query(sql, vals);
      } catch (e) {
        await pool.query(
          `UPDATE ao_table_definitions SET last_error=$2 WHERE id=$1::uuid`,
          [id, `test_row_failed: ${String(e?.message || e)}`]
        );
      }
    }

    await pool.query(
      `UPDATE ao_table_definitions
       SET status='applied', applied_at=now(), applied_by=$2
       WHERE id=$1::uuid`,
      [id, applied_by]
    );

    return res.json({ ok: true, applied: true, sql: createSql, partitioning, test_row_inserted: !!test_row });
  } catch (e) {
    await pool.query(
      `UPDATE ao_table_definitions SET last_error=$2 WHERE id=$1::uuid`,
      [id, String(e?.message || e)]
    );
    return res.status(500).json({ error: 'apply_failed', details: String(e?.message || e) });
  }
});

export { router as tableBuilderRouter };
