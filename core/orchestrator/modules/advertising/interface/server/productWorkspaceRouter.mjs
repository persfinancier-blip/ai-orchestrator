import express from 'express';
import { pool } from './db.mjs';
import { buildNormalizationPlan, monteCarloForecast, normalizeContainerKind, rankCorrelations } from './productAnalyticsCore.mjs';

const SCHEMA = 'ao_product';
const COOKIE = 'ao_container_id';
const COOKIE_ATTRS = 'Path=/; HttpOnly; SameSite=Strict; Max-Age=2592000';

function ident(value) {
  const raw = String(value || '').trim();
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(raw)) throw new Error(`invalid_identifier:${raw}`);
  return `"${raw}"`;
}

function tableName(schema, table) {
  return `${ident(schema)}.${ident(table)}`;
}

function positiveId(value) {
  const id = Math.trunc(Number(value || 0));
  return id > 0 ? id : 0;
}

function readCookie(req, name) {
  for (const part of String(req?.headers?.cookie || '').split(';')) {
    const [key, value] = part.trim().split('=');
    if (key === name) return decodeURIComponent(value || '');
  }
  return '';
}

export function readContainerId(req) {
  return positiveId(readCookie(req, COOKIE));
}

async function ensureTables(client) {
  await client.query(`CREATE SCHEMA IF NOT EXISTS ${ident(SCHEMA)}`);
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${ident(SCHEMA)}.containers_store (
      id bigserial PRIMARY KEY,
      name text NOT NULL,
      kind text NOT NULL DEFAULT 'project',
      client_id bigint,
      description text NOT NULL DEFAULT '',
      status text NOT NULL DEFAULT 'active',
      config_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    )
  `);
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${ident(SCHEMA)}.container_workflows_store (
      container_id bigint NOT NULL,
      desk_id bigint NOT NULL,
      role text NOT NULL DEFAULT 'process',
      created_at timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (container_id, desk_id)
    )
  `);
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${ident(SCHEMA)}.data_products_store (
      id bigserial PRIMARY KEY,
      container_id bigint NOT NULL,
      name text NOT NULL,
      source_schema text NOT NULL,
      source_table text NOT NULL,
      stage text NOT NULL DEFAULT 'raw',
      semantic_schema_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      normalization_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      status text NOT NULL DEFAULT 'draft',
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    )
  `);
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${ident(SCHEMA)}.analysis_models_store (
      id bigserial PRIMARY KEY,
      container_id bigint NOT NULL,
      data_product_id bigint,
      name text NOT NULL,
      target_metric text NOT NULL DEFAULT '',
      model_type text NOT NULL DEFAULT 'auto',
      config_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      last_result_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      status text NOT NULL DEFAULT 'draft',
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    )
  `);
  await client.query(`CREATE INDEX IF NOT EXISTS ao_product_containers_client_idx ON ${ident(SCHEMA)}.containers_store (client_id, status)`);
  await client.query(`CREATE INDEX IF NOT EXISTS ao_product_data_container_idx ON ${ident(SCHEMA)}.data_products_store (container_id, status)`);
  await client.query(`CREATE INDEX IF NOT EXISTS ao_product_models_container_idx ON ${ident(SCHEMA)}.analysis_models_store (container_id, status)`);
}

async function inspectSource(client, schema, table, limit = 80) {
  const meta = await client.query(
    `SELECT column_name AS name, data_type AS type, (is_nullable = 'YES') AS is_nullable
       FROM information_schema.columns
      WHERE table_schema = $1 AND table_name = $2
      ORDER BY ordinal_position`,
    [schema, table]
  );
  if (!meta.rows?.length) throw new Error('source_table_not_found');
  const sample = await client.query(`SELECT * FROM ${tableName(schema, table)} LIMIT $1`, [Math.max(1, Math.min(200, Number(limit || 80)))]);
  return { columns: meta.rows || [], rows: sample.rows || [] };
}

export const productWorkspaceRouter = express.Router();
productWorkspaceRouter.use(express.json({ limit: '2mb' }));

productWorkspaceRouter.get('/context/container', (req, res) => {
  res.json({ container_id: readContainerId(req) || null });
});

productWorkspaceRouter.post('/context/container', (req, res) => {
  const id = positiveId(req.body?.container_id);
  if (!id) return res.status(400).json({ error: 'invalid_container_id' });
  res.setHeader('Set-Cookie', `${COOKIE}=${id}; ${COOKIE_ATTRS}`);
  return res.json({ ok: true, container_id: id });
});

productWorkspaceRouter.delete('/context/container', (_req, res) => {
  res.setHeader('Set-Cookie', `${COOKIE}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0`);
  res.json({ ok: true });
});

productWorkspaceRouter.get('/product/containers', async (_req, res) => {
  const client = await pool.connect();
  try {
    await ensureTables(client);
    const result = await client.query(`SELECT * FROM ${ident(SCHEMA)}.containers_store WHERE status <> 'archived' ORDER BY updated_at DESC, id DESC`);
    return res.json({ containers: result.rows || [] });
  } catch (error) {
    return res.status(500).json({ error: 'containers_list_failed', details: String(error?.message || error) });
  } finally { client.release(); }
});

productWorkspaceRouter.post('/product/containers/upsert', async (req, res) => {
  const id = positiveId(req.body?.id);
  const name = String(req.body?.name || '').trim();
  const kind = normalizeContainerKind(req.body?.kind);
  const clientId = positiveId(req.body?.client_id) || null;
  const description = String(req.body?.description || '').trim();
  const status = ['active', 'paused', 'archived'].includes(String(req.body?.status || 'active')) ? String(req.body.status || 'active') : 'active';
  const config = req.body?.config_json && typeof req.body.config_json === 'object' ? req.body.config_json : {};
  if (!name) return res.status(400).json({ error: 'container_name_required' });
  const client = await pool.connect();
  try {
    await ensureTables(client);
    const result = id
      ? await client.query(`UPDATE ${ident(SCHEMA)}.containers_store SET name=$2,kind=$3,client_id=$4,description=$5,status=$6,config_json=$7::jsonb,updated_at=now() WHERE id=$1 RETURNING *`, [id, name, kind, clientId, description, status, JSON.stringify(config)])
      : await client.query(`INSERT INTO ${ident(SCHEMA)}.containers_store (name,kind,client_id,description,status,config_json) VALUES ($1,$2,$3,$4,$5,$6::jsonb) RETURNING *`, [name, kind, clientId, description, status, JSON.stringify(config)]);
    if (!result.rows?.length) return res.status(404).json({ error: 'container_not_found' });
    return res.json({ ok: true, container: result.rows[0] });
  } catch (error) {
    return res.status(500).json({ error: 'container_upsert_failed', details: String(error?.message || error) });
  } finally { client.release(); }
});

productWorkspaceRouter.get('/product/container/:id', async (req, res) => {
  const id = positiveId(req.params?.id);
  if (!id) return res.status(400).json({ error: 'invalid_container_id' });
  const client = await pool.connect();
  try {
    await ensureTables(client);
    const [container, workflows, dataProducts, models] = await Promise.all([
      client.query(`SELECT * FROM ${ident(SCHEMA)}.containers_store WHERE id=$1`, [id]),
      client.query(`SELECT cw.*, d.desk_name, d.desk_type, d.updated_at FROM ${ident(SCHEMA)}.container_workflows_store cw LEFT JOIN ao_system.workflow_desks_store d ON d.id=cw.desk_id WHERE cw.container_id=$1 ORDER BY cw.created_at DESC`, [id]),
      client.query(`SELECT * FROM ${ident(SCHEMA)}.data_products_store WHERE container_id=$1 AND status <> 'archived' ORDER BY updated_at DESC`, [id]),
      client.query(`SELECT * FROM ${ident(SCHEMA)}.analysis_models_store WHERE container_id=$1 AND status <> 'archived' ORDER BY updated_at DESC`, [id])
    ]);
    if (!container.rows?.length) return res.status(404).json({ error: 'container_not_found' });
    return res.json({ container: container.rows[0], workflows: workflows.rows || [], data_products: dataProducts.rows || [], models: models.rows || [] });
  } catch (error) {
    return res.status(500).json({ error: 'container_read_failed', details: String(error?.message || error) });
  } finally { client.release(); }
});

productWorkspaceRouter.post('/product/container-workflows/link', async (req, res) => {
  const containerId = positiveId(req.body?.container_id) || readContainerId(req);
  const deskId = positiveId(req.body?.desk_id);
  if (!containerId || !deskId) return res.status(400).json({ error: 'container_and_desk_required' });
  const client = await pool.connect();
  try {
    await ensureTables(client);
    await client.query(`INSERT INTO ${ident(SCHEMA)}.container_workflows_store (container_id,desk_id,role) VALUES ($1,$2,$3) ON CONFLICT (container_id,desk_id) DO UPDATE SET role=EXCLUDED.role`, [containerId, deskId, String(req.body?.role || 'process').trim() || 'process']);
    return res.json({ ok: true, container_id: containerId, desk_id: deskId });
  } catch (error) {
    return res.status(500).json({ error: 'workflow_link_failed', details: String(error?.message || error) });
  } finally { client.release(); }
});

productWorkspaceRouter.post('/product/normalization/inspect', async (req, res) => {
  const schema = String(req.body?.schema || '').trim();
  const table = String(req.body?.table || '').trim();
  try { ident(schema); ident(table); } catch { return res.status(400).json({ error: 'invalid_source' }); }
  const client = await pool.connect();
  try {
    const source = await inspectSource(client, schema, table);
    return res.json({ source: { schema, table, rows_sampled: source.rows.length }, plan: buildNormalizationPlan(source.columns, source.rows) });
  } catch (error) {
    const details = String(error?.message || error);
    return res.status(details === 'source_table_not_found' ? 404 : 500).json({ error: 'normalization_inspect_failed', details });
  } finally { client.release(); }
});

productWorkspaceRouter.post('/product/data-products/upsert', async (req, res) => {
  const id = positiveId(req.body?.id);
  const containerId = positiveId(req.body?.container_id) || readContainerId(req);
  const name = String(req.body?.name || '').trim();
  const schema = String(req.body?.source_schema || '').trim();
  const table = String(req.body?.source_table || '').trim();
  if (!containerId || !name) return res.status(400).json({ error: 'container_and_name_required' });
  try { ident(schema); ident(table); } catch { return res.status(400).json({ error: 'invalid_source' }); }
  const semantic = req.body?.semantic_schema_json && typeof req.body.semantic_schema_json === 'object' ? req.body.semantic_schema_json : {};
  const normalization = req.body?.normalization_json && typeof req.body.normalization_json === 'object' ? req.body.normalization_json : {};
  const stage = ['raw', 'clean', 'business'].includes(String(req.body?.stage || 'raw')) ? String(req.body.stage || 'raw') : 'raw';
  const status = ['draft', 'ready', 'archived'].includes(String(req.body?.status || 'draft')) ? String(req.body.status || 'draft') : 'draft';
  const client = await pool.connect();
  try {
    await ensureTables(client);
    const result = id
      ? await client.query(`UPDATE ${ident(SCHEMA)}.data_products_store SET container_id=$2,name=$3,source_schema=$4,source_table=$5,stage=$6,semantic_schema_json=$7::jsonb,normalization_json=$8::jsonb,status=$9,updated_at=now() WHERE id=$1 RETURNING *`, [id, containerId, name, schema, table, stage, JSON.stringify(semantic), JSON.stringify(normalization), status])
      : await client.query(`INSERT INTO ${ident(SCHEMA)}.data_products_store (container_id,name,source_schema,source_table,stage,semantic_schema_json,normalization_json,status) VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8) RETURNING *`, [containerId, name, schema, table, stage, JSON.stringify(semantic), JSON.stringify(normalization), status]);
    return res.json({ ok: true, data_product: result.rows?.[0] || null });
  } catch (error) {
    return res.status(500).json({ error: 'data_product_upsert_failed', details: String(error?.message || error) });
  } finally { client.release(); }
});

productWorkspaceRouter.post('/product/insights/scan', async (req, res) => {
  const schema = String(req.body?.schema || '').trim();
  const table = String(req.body?.table || '').trim();
  const limit = Math.max(50, Math.min(5000, Math.trunc(Number(req.body?.limit || 1500))));
  try { ident(schema); ident(table); } catch { return res.status(400).json({ error: 'invalid_source' }); }
  const client = await pool.connect();
  try {
    const source = await inspectSource(client, schema, table, Math.min(limit, 200));
    const plan = buildNormalizationPlan(source.columns, source.rows);
    const rows = await client.query(`SELECT * FROM ${tableName(schema, table)} LIMIT $1`, [limit]);
    const metrics = plan.fields.filter((f) => f.role === 'metric').map((f) => f.source);
    const dimensions = plan.fields.filter((f) => ['entity', 'dimension', 'scope'].includes(f.role)).map((f) => f.source);
    const time = plan.fields.find((f) => f.role === 'time')?.source || '';
    return res.json({ source: { schema, table, rows: rows.rows?.length || 0 }, plan, relationships: rankCorrelations(rows.rows || [], metrics, 16), fields: { metrics, dimensions, time }, rows: (rows.rows || []).slice(0, 3000) });
  } catch (error) {
    return res.status(500).json({ error: 'insights_scan_failed', details: String(error?.message || error) });
  } finally { client.release(); }
});

productWorkspaceRouter.post('/product/forecast/run', async (req, res) => {
  const schema = String(req.body?.schema || '').trim();
  const table = String(req.body?.table || '').trim();
  const valueField = String(req.body?.value_field || '').trim();
  const orderField = String(req.body?.order_field || '').trim();
  try { ident(schema); ident(table); ident(valueField); if (orderField) ident(orderField); } catch { return res.status(400).json({ error: 'invalid_forecast_source' }); }
  const client = await pool.connect();
  try {
    const q = `SELECT ${ident(valueField)} AS value FROM ${tableName(schema, table)} WHERE ${ident(valueField)} IS NOT NULL ${orderField ? `ORDER BY ${ident(orderField)} ASC` : ''} LIMIT 5000`;
    const result = await client.query(q);
    const forecast = monteCarloForecast((result.rows || []).map((row) => row.value), { horizon: req.body?.horizon, simulations: req.body?.simulations, seed: req.body?.seed, target: req.body?.target });
    return res.json({ source: { schema, table, value_field: valueField, order_field: orderField, observations: result.rows?.length || 0 }, forecast });
  } catch (error) {
    const details = String(error?.message || error);
    return res.status(details === 'forecast_series_too_short' ? 400 : 500).json({ error: 'forecast_failed', details });
  } finally { client.release(); }
});

export async function bootstrapProductWorkspace() {
  const client = await pool.connect();
  try { await ensureTables(client); return { ok: true, schema: SCHEMA }; }
  finally { client.release(); }
}
