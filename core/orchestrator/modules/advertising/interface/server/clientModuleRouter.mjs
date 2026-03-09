import express from 'express';
import { pool } from './db.mjs';
import { fieldTypeToSql } from './tableBuilder.mjs';
import { CLIENT_MODULE_SCHEMA, CLIENT_MODULE_TEMPLATES, getClientModuleTemplateByKey } from '../shared/clientModuleTemplates.mjs';
import {
  buildClientCode,
  buildClientListItem,
  buildClientModuleSources,
  buildClientSummaryView,
  CLIENT_SECTION_DEFINITIONS,
  sanitizeRecordForTemplate
} from './clientModuleCore.mjs';

export const clientModuleRouter = express.Router();
clientModuleRouter.use(express.json({ limit: '2mb' }));

const SETTINGS_SCHEMA = 'ao_system';
const TEMPLATES_TABLE = 'table_templates_store';
const CONTRACTS_TABLE = 'table_data_contract_versions';
const SYSTEM_CONTRACT_COLUMNS = [
  { name: 'ao_source', type: 'text' },
  { name: 'ao_run_id', type: 'text' },
  { name: 'ao_created_at', type: 'timestamptz' },
  { name: 'ao_updated_at', type: 'timestamptz' },
  { name: 'ao_contract_schema', type: 'text' },
  { name: 'ao_contract_name', type: 'text' },
  { name: 'ao_contract_version', type: 'integer' }
];

function requireDataAdmin(req, res, next) {
  const role = (req.header('X-AO-ROLE') || '').trim();
  if (role !== 'data_admin') {
    return res.status(403).json({ error: 'forbidden', details: 'data_admin role required' });
  }
  next();
}

function isIdent(s) {
  return typeof s === 'string' && /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(s);
}

function qi(ident) {
  if (!isIdent(ident)) throw new Error(`invalid_identifier:${ident}`);
  return `"${ident}"`;
}

function qname(schema, table) {
  return `${qi(schema)}.${qi(table)}`;
}

function qlit(v) {
  if (v === null || v === undefined) return 'NULL';
  return `'${String(v).replace(/'/g, "''")}'`;
}

function defaultContractName(schema, table) {
  return `${schema}.${table}`;
}

function templatesQname() {
  return qname(SETTINGS_SCHEMA, TEMPLATES_TABLE);
}

function contractsQname() {
  return qname(SETTINGS_SCHEMA, CONTRACTS_TABLE);
}

function sourceMetaForTemplate(template) {
  return {
    template_name: template.template_name,
    schema_name: template.schema_name,
    table_name: template.table_name
  };
}

function normalizeTemplateColumns(template) {
  const business = Array.isArray(template?.columns) ? template.columns : [];
  return [
    ...SYSTEM_CONTRACT_COLUMNS.map((item) => ({
      field_name: item.name,
      field_type: item.type,
      description: 'Системное поле контракта.'
    })),
    ...business
  ];
}

async function ensureTemplatesStorageTable(client) {
  const qn = templatesQname();
  await client.query(`CREATE SCHEMA IF NOT EXISTS ${qi(SETTINGS_SCHEMA)}`);
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${qn} (
      id bigserial PRIMARY KEY,
      template_name text NOT NULL UNIQUE,
      schema_name text NOT NULL,
      table_name text NOT NULL,
      data_level text NOT NULL DEFAULT 'bronze',
      template_kind text NOT NULL DEFAULT 'data',
      table_class text NOT NULL DEFAULT 'custom',
      description text NOT NULL DEFAULT '',
      columns jsonb NOT NULL DEFAULT '[]'::jsonb,
      partition_enabled boolean NOT NULL DEFAULT false,
      partition_column text NOT NULL DEFAULT '',
      partition_interval text NOT NULL DEFAULT 'day',
      ao_source text,
      ao_run_id text,
      ao_created_at timestamptz,
      ao_updated_at timestamptz,
      ao_contract_schema text,
      ao_contract_name text,
      ao_contract_version integer
    )
  `);
}

async function ensureContractsTable(client) {
  const qn = contractsQname();
  await client.query(`CREATE SCHEMA IF NOT EXISTS ${qi(SETTINGS_SCHEMA)}`);
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${qn} (
      id bigserial PRIMARY KEY,
      schema_name text NOT NULL,
      table_name text NOT NULL,
      contract_name text NOT NULL,
      version integer NOT NULL,
      lifecycle_state text NOT NULL DEFAULT 'active',
      deleted_at timestamptz NULL,
      description text NOT NULL DEFAULT '',
      columns jsonb NOT NULL DEFAULT '[]'::jsonb,
      change_reason text NOT NULL DEFAULT '',
      changed_by text NOT NULL DEFAULT 'system',
      created_at timestamptz NOT NULL DEFAULT now(),
      ao_source text,
      ao_run_id text,
      ao_created_at timestamptz,
      ao_updated_at timestamptz,
      ao_contract_schema text,
      ao_contract_name text,
      ao_contract_version integer,
      UNIQUE(schema_name, table_name, version)
    )
  `);
}

async function ensureSystemContractColumns(client, qn) {
  for (const column of SYSTEM_CONTRACT_COLUMNS) {
    await client.query(`ALTER TABLE ${qn} ADD COLUMN IF NOT EXISTS ${qi(column.name)} ${column.type}`);
  }
}

async function ensureBuiltinTemplateRow(client, template) {
  const qn = templatesQname();
  const runId = `clients_template_${template.key}_${Date.now()}`;
  const params = [
    template.template_name,
    template.schema_name,
    template.table_name,
    template.data_level,
    template.template_kind,
    template.table_class,
    template.description,
    JSON.stringify(normalizeTemplateColumns(template)),
    Boolean(template.partition_enabled),
    String(template.partition_column || ''),
    String(template.partition_interval || 'day'),
    runId,
    SETTINGS_SCHEMA,
    defaultContractName(SETTINGS_SCHEMA, TEMPLATES_TABLE)
  ];
  const updated = await client.query(
    `
    UPDATE ${qn}
    SET
      schema_name = $2,
      table_name = $3,
      data_level = $4,
      template_kind = $5,
      table_class = $6,
      description = $7,
      columns = $8::jsonb,
      partition_enabled = $9,
      partition_column = $10,
      partition_interval = $11,
      ao_source = 'clients_module_bootstrap',
      ao_run_id = $12,
      ao_updated_at = now(),
      ao_contract_schema = $13,
      ao_contract_name = $14,
      ao_contract_version = 1
    WHERE lower(template_name) = lower($1)
    `,
    params
  );
  if ((updated.rowCount || 0) > 0) return;
  await client.query(
    `
    INSERT INTO ${qn}
      (
        template_name, schema_name, table_name, data_level, template_kind, table_class, description,
        columns, partition_enabled, partition_column, partition_interval,
        ao_source, ao_run_id, ao_created_at, ao_updated_at, ao_contract_schema, ao_contract_name, ao_contract_version
      )
    VALUES
      ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11,
       'clients_module_bootstrap', $12, now(), now(), $13, $14, 1)
    `,
    params
  );
}

async function ensureClientTable(client, template) {
  const schema = template.schema_name;
  const table = template.table_name;
  const qn = qname(schema, table);
  const columns = normalizeTemplateColumns(template);
  await client.query(`CREATE SCHEMA IF NOT EXISTS ${qi(schema)}`);
  const createDefs = columns
    .map((column) => {
      if (column.field_name === 'id') return `${qi('id')} bigserial PRIMARY KEY`;
      return `${qi(column.field_name)} ${fieldTypeToSql(column.field_type)}`;
    })
    .join(',\n      ');
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${qn} (
      ${createDefs}
    )
  `);
  for (const column of columns) {
    if (column.field_name === 'id') continue;
    await client.query(`ALTER TABLE ${qn} ADD COLUMN IF NOT EXISTS ${qi(column.field_name)} ${fieldTypeToSql(column.field_type)}`);
  }
  await ensureSystemContractColumns(client, qn);
  for (const column of columns) {
    if (column.description) {
      await client.query(`COMMENT ON COLUMN ${qn}.${qi(column.field_name)} IS ${qlit(column.description)}`);
    }
  }
  await client.query(`COMMENT ON TABLE ${qn} IS ${qlit(template.description)}`);

  const indexes = Array.isArray(template.indexes) ? template.indexes : [];
  for (const field of indexes) {
    if (!isIdent(field)) continue;
    await client.query(`CREATE INDEX IF NOT EXISTS ${qi(`${table}_${field}_idx`)} ON ${qn} (${qi(field)})`);
  }
  const uniqueIndexes = Array.isArray(template.unique_indexes) ? template.unique_indexes : [];
  for (const fields of uniqueIndexes) {
    const validFields = (Array.isArray(fields) ? fields : []).filter((field) => isIdent(field));
    if (!validFields.length) continue;
    const indexName = `${table}_${validFields.join('_')}_uniq`;
    await client.query(`CREATE UNIQUE INDEX IF NOT EXISTS ${qi(indexName)} ON ${qn} (${validFields.map((field) => qi(field)).join(', ')})`);
  }
}

async function getTableColumnsSnapshot(client, schema, table) {
  const r = await client.query(
    `
    SELECT
      c.column_name AS field_name,
      c.data_type AS field_type,
      COALESCE(d.description, '') AS description
    FROM information_schema.columns c
    LEFT JOIN pg_catalog.pg_namespace n
      ON n.nspname = c.table_schema
    LEFT JOIN pg_catalog.pg_class cls
      ON cls.relnamespace = n.oid
     AND cls.relname = c.table_name
    LEFT JOIN pg_catalog.pg_attribute a
      ON a.attrelid = cls.oid
     AND a.attname = c.column_name
    LEFT JOIN pg_catalog.pg_description d
      ON d.objoid = cls.oid
     AND d.objsubid = a.attnum
    WHERE c.table_schema = $1
      AND c.table_name = $2
    ORDER BY c.ordinal_position
    `,
    [schema, table]
  );
  return (r.rows || []).map((item) => ({
    field_name: String(item.field_name || ''),
    field_type: String(item.field_type || ''),
    description: String(item.description || '')
  }));
}

async function getTableDescription(client, schema, table) {
  const r = await client.query(
    `
    SELECT COALESCE(obj_description(c.oid), '') AS description
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = $1 AND c.relname = $2
    LIMIT 1
    `,
    [schema, table]
  );
  return String(r.rows?.[0]?.description || '');
}

async function ensureContractVersionForTable(client, schema, table, reason) {
  const qn = contractsQname();
  const exists = await client.query(
    `SELECT 1 FROM ${qn} WHERE schema_name = $1 AND table_name = $2 AND lifecycle_state = 'active' LIMIT 1`,
    [schema, table]
  );
  if (exists.rows?.length) return;
  const max = await client.query(
    `SELECT COALESCE(MAX(version), 0) AS max_v FROM ${qn} WHERE schema_name = $1 AND table_name = $2`,
    [schema, table]
  );
  const nextVersion = Number(max.rows?.[0]?.max_v || 0) + 1;
  const columns = await getTableColumnsSnapshot(client, schema, table);
  const description = await getTableDescription(client, schema, table);
  await client.query(
    `
    INSERT INTO ${qn}
      (schema_name, table_name, contract_name, version, lifecycle_state, deleted_at, description, columns, change_reason, changed_by,
       ao_source, ao_run_id, ao_created_at, ao_updated_at, ao_contract_schema, ao_contract_name, ao_contract_version)
    VALUES
      ($1, $2, $3, $4, 'active', NULL, $5, $6::jsonb, $7, 'clients_module_bootstrap',
       'clients_module_bootstrap', $8, now(), now(), $9, $3, $4)
    `,
    [
      schema,
      table,
      defaultContractName(schema, table),
      nextVersion,
      description,
      JSON.stringify(columns),
      reason,
      `clients_contract_${table}_${Date.now()}`,
      SETTINGS_SCHEMA
    ]
  );
}

async function ensureClientModuleBootstrap(client) {
  await ensureTemplatesStorageTable(client);
  await ensureContractsTable(client);
  for (const template of CLIENT_MODULE_TEMPLATES) {
    await ensureBuiltinTemplateRow(client, template);
    await ensureClientTable(client, template);
    await ensureContractVersionForTable(client, template.schema_name, template.table_name, `client_module_bootstrap:${template.key}`);
  }
}

function sectionTemplate(section) {
  const def = CLIENT_SECTION_DEFINITIONS[String(section || '').trim()];
  return def ? getClientModuleTemplateByKey(def.key) : null;
}

function recordMeta(tableKey) {
  return getClientModuleTemplateByKey(tableKey);
}

function prepareSystemRow(schema, table, row, isInsert = false) {
  const now = new Date().toISOString();
  return {
    ...row,
    ao_source: 'clients_module',
    ao_run_id: `clients_module_${Date.now()}`,
    ao_updated_at: now,
    ao_contract_schema: SETTINGS_SCHEMA,
    ao_contract_name: defaultContractName(schema, table),
    ao_contract_version: 1,
    ...(isInsert ? { ao_created_at: now } : {}),
    ...(isInsert && row.created_at === undefined ? { created_at: now } : {}),
    ...(row.updated_at === undefined ? { updated_at: now } : {})
  };
}

async function insertRow(client, schema, table, row) {
  const keys = Object.keys(row).filter((key) => isIdent(key));
  const cols = keys.map((key) => qi(key)).join(', ');
  const vals = keys.map((_, idx) => `$${idx + 1}`).join(', ');
  const params = keys.map((key) => row[key]);
  const qn = qname(schema, table);
  const r = await client.query(`INSERT INTO ${qn} (${cols}) VALUES (${vals}) RETURNING *`, params);
  return r.rows?.[0] || null;
}

async function updateRowById(client, schema, table, id, patch) {
  const keys = Object.keys(patch).filter((key) => isIdent(key) && key !== 'id');
  const assignments = keys.map((key, idx) => `${qi(key)} = $${idx + 1}`);
  const params = keys.map((key) => patch[key]);
  params.push(id);
  const qn = qname(schema, table);
  const r = await client.query(`UPDATE ${qn} SET ${assignments.join(', ')} WHERE id = $${params.length} RETURNING *`, params);
  return r.rows?.[0] || null;
}

async function upsertSingleByClientId(client, template, clientId, row) {
  const qn = qname(template.schema_name, template.table_name);
  const prepared = prepareSystemRow(template.schema_name, template.table_name, { ...row, client_id: clientId }, true);
  const keys = Object.keys(prepared).filter((key) => isIdent(key) && key !== 'id');
  const cols = keys.map((key) => qi(key)).join(', ');
  const vals = keys.map((_, idx) => `$${idx + 1}`).join(', ');
  const updates = keys
    .filter((key) => key !== 'client_id' && key !== 'created_at' && !key.startsWith('ao_created_'))
    .map((key) => `${qi(key)} = EXCLUDED.${qi(key)}`)
    .join(', ');
  const params = keys.map((key) => prepared[key]);
  const r = await client.query(
    `INSERT INTO ${qn} (${cols}) VALUES (${vals})
     ON CONFLICT (client_id) DO UPDATE SET ${updates}
     RETURNING *`,
    params
  );
  return r.rows?.[0] || null;
}

async function queryRows(client, template, whereClause = '', params = []) {
  const qn = qname(template.schema_name, template.table_name);
  const r = await client.query(`SELECT * FROM ${qn} ${whereClause}`, params);
  return Array.isArray(r.rows) ? r.rows : [];
}

async function fetchClientDetail(client, clientId) {
  const sources = buildClientModuleSources();
  const clientsTemplate = getClientModuleTemplateByKey('clients');
  const mainTemplate = getClientModuleTemplateByKey('client_main_data');
  const clientRows = await queryRows(client, clientsTemplate, 'WHERE id = $1 LIMIT 1', [clientId]);
  const clientRow = clientRows[0] || null;
  if (!clientRow) return null;

  const [mainRows, legalEntities, contracts, paymentTerms, paymentSchedule, goals, kpis, accesses, constraints, metricsRows, actionItems] =
    await Promise.all([
      queryRows(client, mainTemplate, 'WHERE client_id = $1 ORDER BY id DESC LIMIT 1', [clientId]),
      queryRows(client, getClientModuleTemplateByKey('client_legal_entities'), 'WHERE client_id = $1 ORDER BY id', [clientId]),
      queryRows(client, getClientModuleTemplateByKey('client_contracts'), 'WHERE client_id = $1 ORDER BY id', [clientId]),
      queryRows(client, getClientModuleTemplateByKey('client_payment_terms'), 'WHERE client_id = $1 ORDER BY id', [clientId]),
      queryRows(client, getClientModuleTemplateByKey('client_payment_schedule'), 'WHERE client_id = $1 ORDER BY id', [clientId]),
      queryRows(client, getClientModuleTemplateByKey('client_goals'), 'WHERE client_id = $1 ORDER BY priority DESC, id', [clientId]),
      queryRows(client, getClientModuleTemplateByKey('client_kpis'), 'WHERE client_id = $1 ORDER BY priority DESC, id', [clientId]),
      queryRows(client, getClientModuleTemplateByKey('client_accesses'), 'WHERE client_id = $1 ORDER BY id', [clientId]),
      queryRows(client, getClientModuleTemplateByKey('client_constraints'), 'WHERE client_id = $1 ORDER BY id', [clientId]),
      queryRows(client, getClientModuleTemplateByKey('client_summary_metrics'), 'WHERE client_id = $1 ORDER BY id DESC LIMIT 1', [clientId]),
      queryRows(client, getClientModuleTemplateByKey('client_action_items'), 'WHERE client_id = $1 ORDER BY due_at NULLS LAST, id', [clientId])
    ]);

  const mainData = mainRows[0] || {
    client_id: clientId,
    client_code: clientRow.client_code || '',
    client_name: clientRow.client_display_name || '',
    client_display_name: clientRow.client_display_name || '',
    status: clientRow.status || 'active',
    comment: clientRow.comment || ''
  };
  const summaryMetrics = metricsRows[0] || {
    client_id: clientId,
    budget_plan: null,
    budget_fact: null,
    revenue_plan: null,
    revenue_fact: null,
    orders_plan: null,
    orders_fact: null,
    drr_plan: null,
    drr_fact: null,
    roas_plan: null,
    roas_fact: null
  };
  const summary = buildClientSummaryView({
    goals,
    kpis,
    accesses,
    constraints,
    summaryMetrics,
    actionItems
  });

  return {
    client: buildClientListItem(
      {
        ...clientRow,
        client_display_name: mainData.client_display_name || clientRow.client_display_name,
        status: mainData.status || clientRow.status
      },
      { goals, kpis, accesses, actionItems }
    ),
    mainData,
    legalEntities,
    contracts,
    paymentTerms,
    paymentSchedule,
    goals,
    kpis,
    accesses,
    constraints,
    summaryMetrics,
    actionItems,
    sources,
    summary,
    options: {
      legalEntities: legalEntities.map((item) => ({ value: String(item.id), label: item.legal_entity_name || item.legal_entity_code || `Юрлицо ${item.id}` })),
      contracts: contracts.map((item) => ({ value: String(item.id), label: item.contract_name || item.contract_number || `Договор ${item.id}` })),
      paymentTerms: paymentTerms.map((item) => ({ value: String(item.id), label: item.fee_type || `Условие ${item.id}` })),
      goals: goals.map((item) => ({ value: String(item.id), label: item.goal_name || `Цель ${item.id}` }))
    }
  };
}

clientModuleRouter.get('/clients/module/bootstrap', requireDataAdmin, async (_req, res) => {
  const client = await pool.connect();
  try {
    await ensureClientModuleBootstrap(client);
    return res.json({
      ok: true,
      sources: buildClientModuleSources(),
      templates: CLIENT_MODULE_TEMPLATES.map(sourceMetaForTemplate)
    });
  } catch (e) {
    return res.status(500).json({ error: 'client_module_bootstrap_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

clientModuleRouter.get('/clients/module/list', requireDataAdmin, async (req, res) => {
  const search = String(req.query.search || '').trim().toLowerCase();
  const client = await pool.connect();
  try {
    await ensureClientModuleBootstrap(client);
    const clientsTemplate = getClientModuleTemplateByKey('clients');
    const mainTemplate = getClientModuleTemplateByKey('client_main_data');
    const accessesTemplate = getClientModuleTemplateByKey('client_accesses');
    const goalsTemplate = getClientModuleTemplateByKey('client_goals');
    const kpisTemplate = getClientModuleTemplateByKey('client_kpis');
    const actionTemplate = getClientModuleTemplateByKey('client_action_items');
    const q = `
      SELECT
        c.id,
        COALESCE(m.client_display_name, c.client_display_name) AS client_display_name,
        COALESCE(m.client_code, c.client_code) AS client_code,
        COALESCE(m.status, c.status) AS status,
        COALESCE(c.comment, m.comment, '') AS comment,
        COALESCE(access_summary.platform_summary, '') AS platform_summary,
        COALESCE(goal_summary.active_goals, 0) AS active_goal_count,
        COALESCE(kpi_summary.active_kpis, 0) AS active_kpi_count,
        COALESCE(access_summary.active_accesses, 0) AS active_access_count,
        COALESCE(action_summary.open_items, 0) AS warning_count
      FROM ${qname(clientsTemplate.schema_name, clientsTemplate.table_name)} c
      LEFT JOIN ${qname(mainTemplate.schema_name, mainTemplate.table_name)} m
        ON m.client_id = c.id
      LEFT JOIN LATERAL (
        SELECT
          COUNT(*) FILTER (WHERE COALESCE(is_active, true)) AS active_accesses,
          string_agg(DISTINCT COALESCE(platform_name, platform_code), ' ') FILTER (WHERE COALESCE(is_active, true)) AS platform_summary
        FROM ${qname(accessesTemplate.schema_name, accessesTemplate.table_name)}
        WHERE client_id = c.id
      ) access_summary ON true
      LEFT JOIN LATERAL (
        SELECT COUNT(*) FILTER (WHERE COALESCE(is_active, true)) AS active_goals
        FROM ${qname(goalsTemplate.schema_name, goalsTemplate.table_name)}
        WHERE client_id = c.id
      ) goal_summary ON true
      LEFT JOIN LATERAL (
        SELECT COUNT(*) FILTER (WHERE COALESCE(is_active, true)) AS active_kpis
        FROM ${qname(kpisTemplate.schema_name, kpisTemplate.table_name)}
        WHERE client_id = c.id
      ) kpi_summary ON true
      LEFT JOIN LATERAL (
        SELECT COUNT(*) FILTER (WHERE COALESCE(is_active, true) AND COALESCE(status, '') NOT IN ('done', 'closed', 'resolved')) AS open_items
        FROM ${qname(actionTemplate.schema_name, actionTemplate.table_name)}
        WHERE client_id = c.id
      ) action_summary ON true
      WHERE ($1 = '' OR lower(COALESCE(m.client_display_name, c.client_display_name)) LIKE '%' || $1 || '%' OR lower(COALESCE(m.client_code, c.client_code)) LIKE '%' || $1 || '%')
      ORDER BY COALESCE(m.client_display_name, c.client_display_name), c.id
    `;
    const r = await client.query(q, [search]);
    return res.json({
      clients: (r.rows || []).map((item) => ({
        id: Number(item.id || 0),
        client_display_name: String(item.client_display_name || '').trim(),
        client_code: String(item.client_code || '').trim(),
        status: String(item.status || '').trim(),
        comment: String(item.comment || '').trim(),
        platform_summary: String(item.platform_summary || '').trim(),
        active_goal_count: Number(item.active_goal_count || 0),
        active_kpi_count: Number(item.active_kpi_count || 0),
        active_access_count: Number(item.active_access_count || 0),
        warning_count: Number(item.warning_count || 0)
      })),
      source: sourceMetaForTemplate(clientsTemplate)
    });
  } catch (e) {
    return res.status(500).json({ error: 'clients_list_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

clientModuleRouter.post('/clients/module/client/create', requireDataAdmin, async (req, res) => {
  const displayName = String(req.body?.client_display_name || '').trim();
  const requestedCode = String(req.body?.client_code || '').trim().toLowerCase();
  const requestedStatus = String(req.body?.status || 'draft').trim() || 'draft';
  if (!displayName) {
    return res.status(400).json({ error: 'bad_request', details: 'client_display_name_required' });
  }
  const client = await pool.connect();
  try {
    await ensureClientModuleBootstrap(client);
    await client.query('BEGIN');
    const clientsTemplate = getClientModuleTemplateByKey('clients');
    const codes = await client.query(`SELECT client_code FROM ${qname(clientsTemplate.schema_name, clientsTemplate.table_name)}`);
    const clientCode = requestedCode || buildClientCode(displayName, (codes.rows || []).map((item) => item.client_code));
    const clientRow = prepareSystemRow(
      clientsTemplate.schema_name,
      clientsTemplate.table_name,
      {
        client_code: clientCode,
        client_display_name: displayName,
        status: requestedStatus,
        comment: ''
      },
      true
    );
    const insertedClient = await insertRow(client, clientsTemplate.schema_name, clientsTemplate.table_name, clientRow);
    const mainTemplate = getClientModuleTemplateByKey('client_main_data');
    await upsertSingleByClientId(client, mainTemplate, insertedClient.id, {
      client_code: clientCode,
      client_name: displayName,
      client_display_name: displayName,
      status: requestedStatus,
      comment: ''
    });
    const metricsTemplate = getClientModuleTemplateByKey('client_summary_metrics');
    await upsertSingleByClientId(client, metricsTemplate, insertedClient.id, {});
    await client.query('COMMIT');
    const detail = await fetchClientDetail(client, insertedClient.id);
    return res.json({ ok: true, client_id: Number(insertedClient.id || 0), detail });
  } catch (e) {
    try {
      await client.query('ROLLBACK');
    } catch {}
    return res.status(500).json({ error: 'client_create_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

clientModuleRouter.get('/clients/module/client/:id', requireDataAdmin, async (req, res) => {
  const clientId = Math.trunc(Number(req.params.id || 0));
  if (!(clientId > 0)) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid_client_id' });
  }
  const client = await pool.connect();
  try {
    await ensureClientModuleBootstrap(client);
    const detail = await fetchClientDetail(client, clientId);
    if (!detail) return res.status(404).json({ error: 'not_found', details: 'client_not_found' });
    return res.json(detail);
  } catch (e) {
    return res.status(500).json({ error: 'client_detail_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

clientModuleRouter.post('/clients/module/main/upsert', requireDataAdmin, async (req, res) => {
  const clientId = Math.trunc(Number(req.body?.client_id || 0));
  if (!(clientId > 0)) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid_client_id' });
  }
  const data = req.body?.data;
  const client = await pool.connect();
  try {
    await ensureClientModuleBootstrap(client);
    await client.query('BEGIN');
    const mainTemplate = getClientModuleTemplateByKey('client_main_data');
    const preparedMain = sanitizeRecordForTemplate(mainTemplate.key, data, { preserveClientId: false });
    await upsertSingleByClientId(client, mainTemplate, clientId, preparedMain);

    const clientsTemplate = getClientModuleTemplateByKey('clients');
    const clientPatch = prepareSystemRow(
      clientsTemplate.schema_name,
      clientsTemplate.table_name,
      {
        client_code: preparedMain.client_code || null,
        client_display_name: preparedMain.client_display_name || preparedMain.client_name || null,
        status: preparedMain.status || null,
        comment: preparedMain.comment || null
      },
      false
    );
    await updateRowById(client, clientsTemplate.schema_name, clientsTemplate.table_name, clientId, clientPatch);
    await client.query('COMMIT');
    const detail = await fetchClientDetail(client, clientId);
    return res.json({ ok: true, detail });
  } catch (e) {
    try {
      await client.query('ROLLBACK');
    } catch {}
    return res.status(500).json({ error: 'client_main_upsert_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

clientModuleRouter.post('/clients/module/section/upsert', requireDataAdmin, async (req, res) => {
  const section = String(req.body?.section || '').trim();
  const clientId = Math.trunc(Number(req.body?.client_id || 0));
  const record = req.body?.record;
  const def = CLIENT_SECTION_DEFINITIONS[section];
  const template = def ? getClientModuleTemplateByKey(def.key) : null;
  if (!template || !['legal_entities', 'contracts', 'payment_terms', 'payment_schedule', 'goals', 'kpis', 'accesses', 'constraints', 'summary_metrics', 'action_items'].includes(section)) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid_section' });
  }
  if (!(clientId > 0)) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid_client_id' });
  }
  const client = await pool.connect();
  try {
    await ensureClientModuleBootstrap(client);
    const prepared = sanitizeRecordForTemplate(template.key, record, { preserveId: true, preserveClientId: false });
    let saved = null;
    if (section === 'summary_metrics') {
      saved = await upsertSingleByClientId(client, template, clientId, prepared);
    } else if (prepared.id) {
      saved = await updateRowById(
        client,
        template.schema_name,
        template.table_name,
        Math.trunc(Number(prepared.id || 0)),
        prepareSystemRow(template.schema_name, template.table_name, { ...prepared, client_id: clientId }, false)
      );
    } else {
      saved = await insertRow(
        client,
        template.schema_name,
        template.table_name,
        prepareSystemRow(template.schema_name, template.table_name, { ...prepared, client_id: clientId }, true)
      );
    }
    const detail = await fetchClientDetail(client, clientId);
    return res.json({ ok: true, saved, detail });
  } catch (e) {
    return res.status(500).json({ error: 'client_section_upsert_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

clientModuleRouter.post('/clients/module/section/delete', requireDataAdmin, async (req, res) => {
  const section = String(req.body?.section || '').trim();
  const clientId = Math.trunc(Number(req.body?.client_id || 0));
  const recordId = Math.trunc(Number(req.body?.id || 0));
  const def = CLIENT_SECTION_DEFINITIONS[section];
  const template = def ? getClientModuleTemplateByKey(def.key) : null;
  if (!template || !(clientId > 0) || !(recordId > 0)) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid_section_or_record' });
  }
  const client = await pool.connect();
  try {
    await ensureClientModuleBootstrap(client);
    await client.query(`DELETE FROM ${qname(template.schema_name, template.table_name)} WHERE id = $1`, [recordId]);
    const detail = await fetchClientDetail(client, clientId);
    return res.json({ ok: true, detail });
  } catch (e) {
    return res.status(500).json({ error: 'client_section_delete_failed', details: String(e?.message || e) });
  } finally {
    client.release();
  }
});

export async function bootstrapClientModule() {
  const client = await pool.connect();
  try {
    await ensureClientModuleBootstrap(client);
    return {
      ok: true,
      schema: CLIENT_MODULE_SCHEMA,
      tables: CLIENT_MODULE_TEMPLATES.map((item) => item.table_name)
    };
  } finally {
    client.release();
  }
}
