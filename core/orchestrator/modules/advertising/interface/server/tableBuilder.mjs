// core/orchestrator/modules/advertising/interface/server/tableBuilder.mjs
import express from 'express';
import { pool } from './db.mjs';

const router = express.Router();

// нужно, иначе req.body будет undefined
router.use(express.json({ limit: '2mb' }));

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
  const sql = `
    SELECT table_schema AS schema_name, table_name
    FROM information_schema.tables
    WHERE table_type='BASE TABLE'
      AND table_schema NOT IN ('pg_catalog','information_schema')
    ORDER BY table_schema, table_name
  `;
  const r = await pool.query(sql);
  return r.rows;
}

router.get('/tables', async (_req, res) => {
  try {
    const defs = await pool.query(
      `SELECT id, schema_name, table_name, table_class, status, description, created_by, created_at, applied_at, applied_by, last_error
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

router.post('/tables/draft', requireDataAdmin, async (req, res) => {
  const body = req.body || {};
  const schema_name = body.schema_name;
  const table_name = body.table_name;
  const table_class = body.table_class || 'custom';
  const description = body.description || '';
  const created_by = body.created_by || 'ui';
  const columns = Array.isArray(body.columns) ? body.columns : [];

  try {
    if (!isValidIdent(schema_name)) return res.status(400).json({ error: 'invalid schema_name' });
    if (!isValidIdent(table_name)) return res.status(400).json({ error: 'invalid table_name' });
    if (!columns.length) return res.status(400).json({ error: 'columns required' });

    for (const c of columns) {
      if (!isValidIdent(c.field_name)) return res.status(400).json({ error: `invalid field_name: ${c.field_name}` });
      const t = normalizeType(c.field_type);
      if (!ALLOWED_TYPES.has(t)) return res.status(400).json({ error: `invalid field_type: ${c.field_type}` });
    }

    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      const def = await client.query(
        `INSERT INTO ao_table_definitions (schema_name, table_name, table_class, status, description, created_by)
         VALUES ($1,$2,$3,'draft',$4,$5)
         RETURNING id`,
        [schema_name, table_name, table_class, description, created_by]
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

router.post('/tables/apply/:id', requireDataAdmin, async (req, res) => {
  const id = req.params.id;
  const applied_by = (req.body && req.body.applied_by) || 'ui';

  try {
    const def = await pool.query(
      `SELECT id, schema_name, table_name, table_class, status
       FROM ao_table_definitions
       WHERE id = $1::uuid`,
      [id]
    );
    if (!def.rows.length) return res.status(404).json({ error: 'draft_not_found' });

    const d = def.rows[0];

    const fields = await pool.query(
      `SELECT field_name, field_type
       FROM ao_table_fields
       WHERE table_definition_id = $1::uuid
       ORDER BY created_at ASC`,
      [id]
    );
    if (!fields.rows.length) return res.status(400).json({ error: 'no_fields' });

    const schema = qIdent(d.schema_name);
    const table = qIdent(d.table_name);

    const userCols = fields.rows
      .map((f) => `${qIdent(f.field_name)} ${normalizeType(f.field_type)}`)
      .join(',\n        ');

    const createSql = `
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA IF NOT EXISTS ${schema};

CREATE TABLE IF NOT EXISTS ${schema}.${table} (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at timestamptz NOT NULL DEFAULT now(),
  ${userCols}
);
`.trim();

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(`CREATE EXTENSION IF NOT EXISTS pgcrypto;`);
      await client.query(`CREATE SCHEMA IF NOT EXISTS ${schema};`);
      await client.query(createSql);

      await client.query(
        `UPDATE ao_table_definitions
         SET status='applied', applied_at=now(), applied_by=$2, last_error=NULL
         WHERE id=$1::uuid`,
        [id, applied_by]
      );

      await client.query('COMMIT');
      return res.json({ ok: true, applied: true, sql: createSql });
    } catch (e) {
      await client.query('ROLLBACK');
      await pool.query(
        `UPDATE ao_table_definitions
         SET last_error=$2
         WHERE id=$1::uuid`,
        [id, String(e?.message || e)]
      );
      return res.status(500).json({ error: 'apply_failed', details: String(e?.message || e) });
    } finally {
      client.release();
    }
  } catch (e) {
    return res.status(500).json({ error: 'apply_failed_outer', details: String(e?.message || e) });
  }
});

export { router as tableBuilderRouter };
