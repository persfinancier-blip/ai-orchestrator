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
    const e = new Error('forbidden');
    e.statusCode = 403;
    throw e;
  }
}

function isIdent(s) {
  return /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(String(s || ''));
}

function qi(s) {
  return `"${String(s).replace(/"/g, '""')}"`;
}

function qname(schema, table) {
  return `${qi(schema)}.${qi(table)}`;
}

function asInt(v, def) {
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
}

function normalizeType(t) {
  const tt = String(t || '').trim().toLowerCase();
  const allowed = new Set(['text', 'int', 'bigint', 'numeric', 'boolean', 'date', 'timestamptz', 'jsonb', 'uuid']);
  if (!allowed.has(tt)) throw new Error(`invalid_field_type:${tt}`);
  if (tt === 'int') return 'integer';
  return tt;
}

function safeError(err) {
  return String(err?.message || err);
}

/**
 * Extract PG column comments.
 */
async function getTableColumns(schema, table) {
  const sql = `
    SELECT
      c.column_name,
      c.data_type,
      c.is_nullable,
      pgd.description
    FROM information_schema.columns c
    JOIN pg_catalog.pg_class cls ON cls.relname = c.table_name
    JOIN pg_catalog.pg_namespace nsp ON nsp.oid = cls.relnamespace AND nsp.nspname = c.table_schema
    LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = cls.oid AND a.attname = c.column_name
    LEFT JOIN pg_catalog.pg_description pgd ON pgd.objoid = cls.oid AND pgd.objsubid = a.attnum
    WHERE c.table_schema = $1 AND c.table_name = $2
    ORDER BY c.ordinal_position;
  `;
  const r = await pool.query(sql, [schema, table]);

  return r.rows.map((x) => ({
    name: x.column_name,
    type:
      x.data_type === 'integer'
        ? 'int'
        : x.data_type === 'timestamp with time zone'
          ? 'timestamptz'
          : x.data_type === 'uuid'
            ? 'uuid'
            : x.data_type === 'jsonb'
              ? 'jsonb'
              : x.data_type === 'numeric'
                ? 'numeric'
                : x.data_type === 'boolean'
                  ? 'boolean'
                  : x.data_type === 'date'
                    ? 'date'
                    : 'text',
    description: x.description || '',
    is_nullable: x.is_nullable === 'YES'
  }));
}

/** -------- routes -------- */

// List existing tables
tableBuilderRouter.get('/tables', async (_req, res) => {
  try {
    const sql = `
      SELECT n.nspname AS schema_name, c.relname AS table_name
      FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relkind IN ('r','p')
        AND n.nspname NOT IN ('pg_catalog','information_schema')
      ORDER BY n.nspname, c.relname;
    `;
    const r = await pool.query(sql);
    res.json({ drafts: [], existing_tables: r.rows });
  } catch (e) {
    res.status(500).json({ error: 'tables_list_failed', details: safeError(e) });
  }
});

// Columns metadata for a table
tableBuilderRouter.get('/columns', async (req, res) => {
  const schema = String(req.query.schema || '').trim();
  const table = String(req.query.table || '').trim();
  if (!schema || !table) return res.status(400).json({ error: 'missing_schema_or_table' });

  try {
    const cols = await getTableColumns(schema, table);
    res.json({ schema, table, columns: cols });
  } catch (e) {
    res.status(500).json({ error: 'columns_failed', details: safeError(e) });
  }
});

// Preview rows (returns __ctid)
tableBuilderRouter.get('/preview', async (req, res) => {
  const schema = String(req.query.schema || '').trim();
  const table = String(req.query.table || '').trim();
  const limit = Math.min(100, Math.max(1, asInt(req.query.limit, 5)));

  if (!schema || !table) return res.status(400).json({ error: 'missing_schema_or_table' });
  if (!isIdent(schema) || !isIdent(table)) return res.status(400).json({ error: 'invalid_schema_or_table' });

  try {
    const sql = `SELECT ctid::text AS "__ctid", * FROM ${qname(schema, table)} LIMIT $1`;
    const r = await pool.query(sql, [limit]);
    res.json({ rows: r.rows });
  } catch (e) {
    res.status(500).json({ error: 'preview_failed', details: safeError(e) });
  }
});

// Create table NOW (no drafts)
tableBuilderRouter.post('/tables/create', async (req, res) => {
  try {
    requireDataAdmin(req);

    const schema = String(req.body?.schema_name || '').trim();
    const table = String(req.body?.table_name || '').trim();
    const description = String(req.body?.description || '').trim();
    const columns = Array.isArray(req.body?.columns) ? req.body.columns : [];
    const partitioning = req.body?.partitioning || { enabled: false };
    const testRow = req.body?.test_row || null;

    if (!schema || !table) return res.status(400).json({ error: 'missing_schema_or_table' });
    if (!isIdent(schema) || !isIdent(table)) return res.status(400).json({ error: 'invalid_schema_or_table' });

    const cols = columns
      .map((c) => ({
        name: String(c?.field_name || '').trim(),
        type: normalizeType(c?.field_type || 'text'),
        description: String(c?.description || '').trim()
      }))
      .filter((c) => c.name);

    if (!cols.length) return res.status(400).json({ error: 'no_columns' });
    for (const c of cols) {
      if (!isIdent(c.name)) return res.status(400).json({ error: 'invalid_column_name', details: c.name });
    }

    const partitionEnabled = !!partitioning?.enabled;
    const partitionColumn = String(partitioning?.column || '').trim();
    const partitionInterval = String(partitioning?.interval || 'day').trim();

    if (partitionEnabled && (!partitionColumn || !isIdent(partitionColumn))) {
      return res.status(400).json({ error: 'invalid_partition_column' });
    }

    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      await client.query(`CREATE SCHEMA IF NOT EXISTS ${qi(schema)}`);

      const colDDL = cols.map((c) => `${qi(c.name)} ${c.type}`).join(',\n  ');

      if (partitionEnabled) {
        // IMPORTANT: create DEFAULT partition so inserts work immediately
        await client.query(`
          CREATE TABLE IF NOT EXISTS ${qname(schema, table)} (
            ${colDDL}
          ) PARTITION BY RANGE (${qi(partitionColumn)});
        `);

        let pName = `${table}__p_default`;
        if (pName.length > 60) pName = pName.slice(0, 60);

        await client.query(`
          CREATE TABLE IF NOT EXISTS ${qname(schema, pName)}
          PARTITION OF ${qname(schema, table)}
          DEFAULT;
        `);

        await client.query(
          `COMMENT ON TABLE ${qname(schema, table)} IS $1`,
          [description ? `${description} | partition:${partitionColumn}:${partitionInterval}` : `partition:${partitionColumn}:${partitionInterval}`]
        );
      } else {
        await client.query(`
          CREATE TABLE IF NOT EXISTS ${qname(schema, table)} (
            ${colDDL}
          );
        `);

        if (description) {
          await client.query(`COMMENT ON TABLE ${qname(schema, table)} IS $1`, [description]);
        }
      }

      for (const c of cols) {
        if (c.description) {
          await client.query(`COMMENT ON COLUMN ${qname(schema, table)}.${qi(c.name)} IS $1`, [c.description]);
        }
      }

      if (testRow && typeof testRow === 'object' && !Array.isArray(testRow)) {
        const keys = Object.keys(testRow);
        if (keys.length) {
          const known = new Map(cols.map((c) => [c.name, c.type]));
          const useKeys = keys.filter((k) => known.has(k));
          if (useKeys.length) {
            const colList = useKeys.map((k) => qi(k)).join(', ');
            const vals = useKeys.map((k) => testRow[k]);
            const ph = useKeys.map((k, i) => `$${i + 1}::${known.get(k)}`).join(', ');
            await client.query(`INSERT INTO ${qname(schema, table)} (${colList}) VALUES (${ph})`, vals);
          }
        }
      }

      await client.query('COMMIT');
      res.json({ ok: true, schema_name: schema, table_name: table });
    } catch (e) {
      await client.query('ROLLBACK');
      res.status(500).json({ error: 'tables_create_failed', details: safeError(e) });
    } finally {
      client.release();
    }
  } catch (e) {
    res.status(e?.statusCode || 500).json({ error: 'tables_create_failed', details: safeError(e) });
  }
});

// Add column
tableBuilderRouter.post('/columns/add', async (req, res) => {
  try {
    requireDataAdmin(req);

    const schema = String(req.body?.schema || '').trim();
    const table = String(req.body?.table || '').trim();
    const name = String(req.body?.column?.name || '').trim();
    const type = normalizeType(req.body?.column?.type || 'text');
    const description = String(req.body?.column?.description || '').trim();

    if (!schema || !table || !name) return res.status(400).json({ error: 'missing_fields' });
    if (!isIdent(schema) || !isIdent(table) || !isIdent(name)) return res.status(400).json({ error: 'invalid_identifier' });

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(`ALTER TABLE ${qname(schema, table)} ADD COLUMN IF NOT EXISTS ${qi(name)} ${type}`);
      if (description) {
        await client.query(`COMMENT ON COLUMN ${qname(schema, table)}.${qi(name)} IS $1`, [description]);
      }
      await client.query('COMMIT');
      res.json({ ok: true });
    } catch (e) {
      await client.query('ROLLBACK');
      res.status(500).json({ error: 'add_column_failed', details: safeError(e) });
    } finally {
      client.release();
    }
  } catch (e) {
    res.status(e?.statusCode || 500).json({ error: 'add_column_failed', details: safeError(e) });
  }
});

// Drop column
tableBuilderRouter.post('/columns/drop', async (req, res) => {
  try {
    requireDataAdmin(req);

    const schema = String(req.body?.schema || '').trim();
    const table = String(req.body?.table || '').trim();
    const name = String(req.body?.column_name || '').trim();

    if (!schema || !table || !name) return res.status(400).json({ error: 'missing_fields' });
    if (!isIdent(schema) || !isIdent(table) || !isIdent(name)) return res.status(400).json({ error: 'invalid_identifier' });

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(`ALTER TABLE ${qname(schema, table)} DROP COLUMN IF EXISTS ${qi(name)} CASCADE`);
      await client.query('COMMIT');
      res.json({ ok: true });
    } catch (e) {
      await client.query('ROLLBACK');
      res.status(500).json({ error: 'drop_column_failed', details: safeError(e) });
    } finally {
      client.release();
    }
  } catch (e) {
    res.status(e?.statusCode || 500).json({ error: 'drop_column_failed', details: safeError(e) });
  }
});

// Insert row
tableBuilderRouter.post('/rows/add', async (req, res) => {
  try {
    requireDataAdmin(req);

    const schema = String(req.body?.schema || '').trim();
    const table = String(req.body?.table || '').trim();
    const row = req.body?.row || {};

    if (!schema || !table) return res.status(400).json({ error: 'missing_schema_or_table' });
    if (!isIdent(schema) || !isIdent(table)) return res.status(400).json({ error: 'invalid_identifier' });
    if (!row || typeof row !== 'object' || Array.isArray(row)) return res.status(400).json({ error: 'row_must_be_object' });

    const cols = await getTableColumns(schema, table);
    const typeByName = new Map(cols.map((c) => [c.name, normalizeType(c.type)]));

    const keys = Object.keys(row).filter((k) => typeByName.has(k));
    if (!keys.length) return res.status(400).json({ error: 'no_known_columns' });

    const colList = keys.map((k) => qi(k)).join(', ');
    const vals = keys.map((k) => row[k]);
    const ph = keys.map((k, i) => `$${i + 1}::${typeByName.get(k)}`).join(', ');

    const sql = `INSERT INTO ${qname(schema, table)} (${colList}) VALUES (${ph}) RETURNING ctid::text AS "__ctid", *`;
    const r = await pool.query(sql, vals);
    res.json({ ok: true, row: r.rows[0] });
  } catch (e) {
    res.status(e?.statusCode || 500).json({ error: 'add_row_failed', details: safeError(e) });
  }
});

// Delete row (by __ctid)
tableBuilderRouter.post('/rows/delete', async (req, res) => {
  try {
    requireDataAdmin(req);

    const schema = String(req.body?.schema || '').trim();
    const table = String(req.body?.table || '').trim();
    const ctid = String(req.body?.__ctid || '').trim();

    if (!schema || !table || !ctid) return res.status(400).json({ error: 'missing_fields' });
    if (!isIdent(schema) || !isIdent(table)) return res.status(400).json({ error: 'invalid_identifier' });

    const sql = `DELETE FROM ${qname(schema, table)} WHERE ctid::text = $1`;
    const r = await pool.query(sql, [ctid]);
    res.json({ ok: true, deleted: r.rowCount });
  } catch (e) {
    res.status(e?.statusCode || 500).json({ error: 'delete_row_failed', details: safeError(e) });
  }
});

// Drop table
tableBuilderRouter.post('/tables/drop', async (req, res) => {
  try {
    requireDataAdmin(req);

    const schema = String(req.body?.schema || '').trim();
    const table = String(req.body?.table || '').trim();

    if (!schema || !table) return res.status(400).json({ error: 'missing_schema_or_table' });
    if (!isIdent(schema) || !isIdent(table)) return res.status(400).json({ error: 'invalid_identifier' });

    await pool.query(`DROP TABLE IF EXISTS ${qname(schema, table)} CASCADE`);
    res.json({ ok: true });
  } catch (e) {
    res.status(e?.statusCode || 500).json({ error: 'drop_table_failed', details: safeError(e) });
  }
});
