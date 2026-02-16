import express from 'express';
import { withClient, withTx } from './db.mjs';

export const tableBuilderRouter = express.Router();

const IDENT_RE = /^[a-z][a-z0-9_]{1,62}$/;
const SCHEMA_ALLOWLIST = new Set(['platform', 'adv', 'content', 'strategy']);
const TABLE_CLASS_ALLOWLIST = new Set(['bronze_raw', 'silver_table', 'showcase_table']);

const TYPE_ALLOWLIST = new Set([
  'uuid',
  'text',
  'int4',
  'int8',
  'bool',
  'date',
  'timestamptz',
  'jsonb',
  'numeric(18,2)',
  'numeric(18,4)',
  'numeric(12,2)',
]);

function requireRole(role) {
  return (req, res, next) => {
    const current = String(req.headers['x-ao-role'] || 'viewer');
    if (current !== role) return res.status(403).json({ error: 'forbidden' });
    next();
  };
}

function assertIdent(value, kind) {
  const v = String(value || '');
  if (!IDENT_RE.test(v)) throw new Error(`Invalid ${kind}: ${v}`);
  return v;
}

function assertSchema(value) {
  const v = String(value || '');
  if (!SCHEMA_ALLOWLIST.has(v)) throw new Error(`Schema not allowed: ${v}`);
  return v;
}

function assertType(value) {
  const v = String(value || '');
  if (!TYPE_ALLOWLIST.has(v)) throw new Error(`Type not allowed: ${v}`);
  return v;
}

function qIdent(name) {
  return `"${String(name).replaceAll('"', '""')}"`;
}

function systemColumnsSql(tableClass) {
  const base = [
    'id uuid primary key default gen_random_uuid()',
    'created_at timestamptz not null default now()',
    'run_id uuid not null',
    'source_id uuid null',
    'dataset_name text not null',
    'contract_version text not null',
  ];

  if (tableClass === 'bronze_raw') {
    return [
      ...base,
      'ingested_at timestamptz not null default now()',
      'entity_keys jsonb null',
      'request_meta jsonb null',
      'payload_hash text null',
      'payload jsonb not null',
      "ingest_status text not null default 'ok'",
      'error_code text null',
      'error_message text null',
    ];
  }

  return [...base, 'updated_at timestamptz not null default now()'];
}

function userColumnsSql(fields) {
  return fields.map((f) => `${qIdent(f.field_name)} ${f.field_type}`);
}

function bronzeParentTableSql(schemaName, tableName, fields) {
  const cols = [...systemColumnsSql('bronze_raw'), ...userColumnsSql(fields)].join(',\n  ');
  return `create table if not exists ${qIdent(schemaName)}.${qIdent(tableName)} (\n  ${cols}\n) partition by range (ingested_at);`;
}

function bronzeDefaultPartitionSql(schemaName, tableName) {
  const partName = `${tableName}__default`;
  return `create table if not exists ${qIdent(schemaName)}.${qIdent(partName)} partition of ${qIdent(schemaName)}.${qIdent(tableName)} default;`;
}

function bronzePartitionSql(schemaName, tableName, fromTs, toTs) {
  const fromTag = fromTs.toISOString().slice(0, 10).replaceAll('-', '_');
  const partName = `${tableName}__${fromTag}`;
  const fromIso = fromTs.toISOString();
  const toIso = toTs.toISOString();
  return `create table if not exists ${qIdent(schemaName)}.${qIdent(partName)} partition of ${qIdent(schemaName)}.${qIdent(tableName)} for values from ('${fromIso}') to ('${toIso}');`;
}

function bronzeIndexesSql(schemaName, tableName) {
  const base = `${qIdent(schemaName)}.${qIdent(tableName)}`;
  return [
    `create index if not exists ${qIdent(`idx_${tableName}_dataset_ingested_at`)} on ${base} (dataset_name, ingested_at);`,
    `create index if not exists ${qIdent(`idx_${tableName}_run_id`)} on ${base} (run_id);`,
    `create index if not exists ${qIdent(`idx_${tableName}_source_ingested_at`)} on ${base} (source_id, ingested_at);`,
    `create index if not exists ${qIdent(`brin_${tableName}_ingested_at`)} on ${base} using brin (ingested_at);`,
  ].join('\n');
}

function generateBronzeSql(def, fields) {
  const schemaName = def.schema_name;
  const tableName = def.table_name;
  const options = def.options || {};

  const partitionsAheadDays = Number(options.partitions_ahead_days ?? 30);
  const partitionIntervalDays = Number(options.partition_interval_days ?? 1);

  const now = new Date();
  const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));

  const statements = [];
  statements.push(`create schema if not exists ${qIdent(schemaName)};`);
  statements.push(bronzeParentTableSql(schemaName, tableName, fields));
  statements.push(bronzeDefaultPartitionSql(schemaName, tableName));

  for (let day = 0; day <= partitionsAheadDays; day += partitionIntervalDays) {
    const from = new Date(start.getTime() + day * 24 * 3600 * 1000);
    const to = new Date(from.getTime() + partitionIntervalDays * 24 * 3600 * 1000);
    statements.push(bronzePartitionSql(schemaName, tableName, from, to));
  }

  statements.push(bronzeIndexesSql(schemaName, tableName));
  return {
    upSql: statements.join('\n\n'),
    downSql: `drop table if exists ${qIdent(schemaName)}.${qIdent(tableName)} cascade;`,
  };
}

async function loadDefinition(client, id) {
  const defRes = await client.query('select * from ao_table_definitions where id = $1', [id]);
  if (!defRes.rows.length) return null;
  const def = defRes.rows[0];
  const fieldsRes = await client.query(
    `select field_name, field_type, description
     from ao_table_fields
     where table_definition_id = $1
     order by created_at asc`,
    [id],
  );
  return { def, fields: fieldsRes.rows };
}

tableBuilderRouter.get('/tables', async (req, res) => {
  try {
    const { rows } = await withClient((c) =>
      c.query(
        `select id, schema_name, table_name, table_class, status, options, created_by, created_at, applied_at
         from ao_table_definitions
         order by created_at desc
         limit 200`,
      ),
    );
    res.json({ items: rows });
  } catch {
    res.status(500).json({ error: 'failed_to_list_tables' });
  }
});

tableBuilderRouter.get('/tables/:id', async (req, res) => {
  try {
    const out = await withClient((c) => loadDefinition(c, req.params.id));
    if (!out) return res.status(404).json({ error: 'not_found' });
    res.json(out);
  } catch {
    res.status(500).json({ error: 'failed_to_get_table' });
  }
});

tableBuilderRouter.post('/tables', express.json({ limit: '2mb' }), async (req, res) => {
  try {
    const schemaName = assertSchema(req.body.schema_name);
    const tableName = assertIdent(req.body.table_name, 'table_name');
    const tableClass = String(req.body.table_class || '');
    if (!TABLE_CLASS_ALLOWLIST.has(tableClass)) throw new Error(`Invalid table_class: ${tableClass}`);

    const createdBy = String(req.headers['x-ao-user'] || 'ui');
    const options = req.body.options && typeof req.body.options === 'object' ? req.body.options : {};
    const fields = Array.isArray(req.body.fields) ? req.body.fields : [];

    const normalized = fields.map((f) => {
      const field_name = assertIdent(f.field_name, 'field_name');
      const field_type = assertType(f.field_type);
      const description = String(f.description || '').trim();
      if (!description) throw new Error(`Field description required: ${field_name}`);
      return { field_name, field_type, description };
    });

    const id = await withTx(async (client) => {
      const defRes = await client.query(
        `insert into ao_table_definitions(schema_name, table_name, table_class, status, options, created_by)
         values($1,$2,$3,'draft',$4,$5)
         returning id`,
        [schemaName, tableName, tableClass, options, createdBy],
      );
      const defId = defRes.rows[0].id;
      for (const f of normalized) {
        await client.query(
          `insert into ao_table_fields(table_definition_id, field_name, field_type, description)
           values($1,$2,$3,$4)`,
          [defId, f.field_name, f.field_type, f.description],
        );
      }
      return defId;
    });

    res.json({ id });
  } catch (e) {
    res.status(400).json({ error: String(e.message || e) });
  }
});

tableBuilderRouter.post('/tables/:id/generate-sql', async (req, res) => {
  try {
    const out = await withClient((c) => loadDefinition(c, req.params.id));
    if (!out) return res.status(404).json({ error: 'not_found' });

    const { def, fields } = out;
    if (def.table_class !== 'bronze_raw') {
      return res.status(400).json({ error: 'only_bronze_raw_supported_in_v1' });
    }

    res.json(generateBronzeSql(def, fields));
  } catch {
    res.status(500).json({ error: 'failed_to_generate_sql' });
  }
});

tableBuilderRouter.post('/tables/:id/apply', requireRole('data_admin'), async (req, res) => {
  try {
    const appliedBy = String(req.headers['x-ao-user'] || 'admin');
    const out = await withClient((c) => loadDefinition(c, req.params.id));
    if (!out) return res.status(404).json({ error: 'not_found' });

    const { def, fields } = out;
    if (def.status === 'applied') return res.json({ ok: true, already: true });
    if (def.table_class !== 'bronze_raw') return res.status(400).json({ error: 'only_bronze_raw_supported_in_v1' });

    const { upSql } = generateBronzeSql(def, fields);

    await withTx(async (client) => {
      await client.query(upSql);
      await client.query(
        `insert into ao_migrations_applied(table_definition_id, schema_name, table_name, migration_sql, applied_by)
         values($1,$2,$3,$4,$5)`,
        [def.id, def.schema_name, def.table_name, upSql, appliedBy],
      );
      await client.query(`update ao_table_definitions set status='applied', applied_at=now() where id=$1`, [def.id]);
    });

    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: 'failed_to_apply', details: String(e.message || e) });
  }
});
