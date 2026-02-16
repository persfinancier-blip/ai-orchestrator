// core/orchestrator/modules/advertising/interface/server/tableBuilder.mjs
import express from 'express';
import { pool } from './db.mjs';

const router = express.Router();

/**
 * Super-minimal RBAC (MVP):
 * - Any read: allowed
 * - Write/apply: requires header X-AO-ROLE=data_admin
 */
function requireDataAdmin(req, res, next) {
  const role = String(req.header('X-AO-ROLE') || '').toLowerCase();
  if (role !== 'data_admin') {
    return res.status(403).json({ error: 'forbidden: data_admin only' });
  }
  return next();
}

router.get('/tables', async (_req, res) => {
  const drafts = await pool.query(
    `SELECT id, schema_name, table_name, table_class, description, status, created_at, created_by, applied_at, applied_by, last_error
     FROM ao_table_drafts
     ORDER BY created_at DESC`
  );

  const existing = await pool.query(`SELECT schema_name, table_name FROM ao_user_tables`);

  res.json({
    drafts: drafts.rows,
    existing_tables: existing.rows,
  });
});

router.post('/tables/draft', requireDataAdmin, async (req, res) => {
  const {
    schema_name,
    table_name,
    table_class = 'custom',
    description = '',
    columns = [],
  } = req.body || {};

  if (!schema_name || !table_name) {
    return res.status(400).json({ error: 'schema_name and table_name are required' });
  }

  if (!Array.isArray(columns) || columns.length === 0) {
    return res.status(400).json({ error: 'columns[] required' });
  }

  // Basic validation
  for (const [i, c] of columns.entries()) {
    if (!c?.column_name || !c?.data_type) {
      return res.status(400).json({ error: `columns[${i}] must have column_name and data_type` });
    }
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const ins = await client.query(
      `INSERT INTO ao_table_drafts (schema_name, table_name, table_class, description, status, created_by)
       VALUES ($1,$2,$3,$4,'draft','ui')
       ON CONFLICT (schema_name, table_name)
       DO UPDATE SET table_class=EXCLUDED.table_class, description=EXCLUDED.description, status='draft', last_error=NULL
       RETURNING id`,
      [schema_name, table_name, table_class, description]
    );

    const draftId = ins.rows[0].id;

    await client.query(`DELETE FROM ao_table_draft_columns WHERE draft_id=$1`, [draftId]);

    for (let idx = 0; idx < columns.length; idx += 1) {
      const c = columns[idx];
      await client.query(
        `INSERT INTO ao_table_draft_columns
          (draft_id, ordinal, column_name, data_type, is_nullable, default_expr, description)
         VALUES ($1,$2,$3,$4,$5,$6,$7)`,
        [
          draftId,
          idx + 1,
          c.column_name,
          c.data_type,
          c.is_nullable ?? true,
          c.default_expr ?? null,
          c.description ?? '',
        ]
      );
    }

    await client.query('COMMIT');
    return res.json({ draft_id: draftId });
  } catch (e) {
    await client.query('ROLLBACK');
    return res.status(500).json({ error: String(e?.message || e) });
  } finally {
    client.release();
  }
});

router.get('/tables/:draftId/sql', async (req, res) => {
  const { draftId } = req.params;

  const d = await pool.query(
    `SELECT id, schema_name, table_name FROM ao_table_drafts WHERE id=$1`,
    [draftId]
  );
  if (d.rowCount === 0) return res.status(404).json({ error: 'draft not found' });

  const cols = await pool.query(
    `SELECT column_name, data_type, is_nullable, default_expr, description
     FROM ao_table_draft_columns WHERE draft_id=$1 ORDER BY ordinal`,
    [draftId]
  );

  const { schema_name, table_name } = d.rows[0];
  const ddl = buildCreateTableDDL(schema_name, table_name, cols.rows);

  res.json({ draft_id: draftId, ddl });
});

router.post('/tables/:draftId/apply', requireDataAdmin, async (req, res) => {
  const { draftId } = req.params;

  const d = await pool.query(
    `SELECT id, schema_name, table_name FROM ao_table_drafts WHERE id=$1`,
    [draftId]
  );
  if (d.rowCount === 0) return res.status(404).json({ error: 'draft not found' });

  const cols = await pool.query(
    `SELECT column_name, data_type, is_nullable, default_expr, description
     FROM ao_table_draft_columns WHERE draft_id=$1 ORDER BY ordinal`,
    [draftId]
  );

  const { schema_name, table_name } = d.rows[0];
  const ddl = buildCreateTableDDL(schema_name, table_name, cols.rows);

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // ensure schema
    await client.query(`CREATE SCHEMA IF NOT EXISTS "${schema_name}"`);

    await client.query(ddl);

    await client.query(
      `UPDATE ao_table_drafts
       SET status='applied', applied_at=now(), applied_by='ui', last_error=NULL
       WHERE id=$1`,
      [draftId]
    );

    await client.query('COMMIT');
    return res.json({ ok: true });
  } catch (e) {
    await client.query('ROLLBACK');
    await pool.query(
      `UPDATE ao_table_drafts SET status='failed', last_error=$2 WHERE id=$1`,
      [draftId, String(e?.message || e)]
    );
    return res.status(500).json({ error: String(e?.message || e) });
  } finally {
    client.release();
  }
});

function buildCreateTableDDL(schemaName, tableName, columns) {
  // System columns (always)
  const sys = [
    `"id" uuid PRIMARY KEY DEFAULT gen_random_uuid()`,
    `"created_at" timestamptz NOT NULL DEFAULT now()`,
    `"updated_at" timestamptz NOT NULL DEFAULT now()`,
  ];

  const cols = columns.map((c) => {
    const nn = c.is_nullable ? '' : ' NOT NULL';
    const def = c.default_expr ? ` DEFAULT ${c.default_expr}` : '';
    return `"${c.column_name}" ${c.data_type}${def}${nn}`;
  });

  // Trigger for updated_at
  const func = `
CREATE OR REPLACE FUNCTION "${schemaName}".ao_set_updated_at()
RETURNS trigger AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;`;

  const trg = `
DROP TRIGGER IF EXISTS ao_set_updated_at ON "${schemaName}"."${tableName}";
CREATE TRIGGER ao_set_updated_at
BEFORE UPDATE ON "${schemaName}"."${tableName}"
FOR EACH ROW EXECUTE FUNCTION "${schemaName}".ao_set_updated_at();`;

  return `
${func}
CREATE TABLE IF NOT EXISTS "${schemaName}"."${tableName}" (
  ${[...sys, ...cols].join(',\n  ')}
);
${trg}
`.trim();
}

export const tableBuilderRouter = router;
