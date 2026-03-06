import express from 'express';
import crypto from 'node:crypto';
import { pool } from './db.mjs';
import {
  shouldRetryStatus,
  retryDelayMs,
  groupRowsByAliases,
  evaluateStopRuleValue
} from '../desk/tabs/apiBuilderRuntimeCore.js';

const SETTINGS_SCHEMA = 'ao_system';
const SETTINGS_TABLE = 'table_settings_store';
const DEFAULT_CONFIG = Object.freeze({
  api_configs_schema: 'ao_system',
  api_configs_table: 'api_configs_store',
  workflow_desks_schema: 'ao_system',
  workflow_desks_table: 'workflow_desks_store',
  workflow_desk_versions_table: 'workflow_desk_versions_store',
  workflow_runs_schema: 'ao_system',
  workflow_runs_table: 'workflow_runs_store',
  workflow_run_steps_table: 'workflow_run_steps_store',
  workflow_process_overrides_table: 'workflow_process_overrides_store',
  workflow_process_locks_table: 'workflow_process_locks_store',
  workflow_process_bus_table: 'workflow_process_bus_store',
  workflow_job_queue_table: 'workflow_job_queue_store',
  workflow_incremental_state_table: 'workflow_incremental_state_store',
  workflow_dependencies_table: 'workflow_process_dependencies_store',
  workflow_rate_limit_policies_table: 'workflow_rate_limit_policies_store',
  workflow_tenant_policies_table: 'workflow_tenant_policies_store',
  workflow_run_aggregation_table: 'workflow_run_aggregation_store',
  workflow_dead_jobs_table: 'workflow_dead_jobs_store',
  workflow_scheduler_leases_table: 'workflow_scheduler_leases_store',
  workflow_worker_leases_table: 'workflow_worker_leases_store',
  workflow_provider_registry_table: 'workflow_provider_registry_store',
  workflow_chunk_logs_table: 'workflow_chunk_logs_store'
});

const PARAMETER_TOKEN_RE = /\{\{\s*([^{}]+?)\s*\}\}/g;
const PARAMETER_TOKEN_EXACT_RE = /^\{\{\s*([^{}]+?)\s*\}\}$/;
const MAX_REQUESTS_PREVIEW = 20;
const MAX_RESPONSES_PREVIEW = 20;
const ABSOLUTE_PAGE_SAFETY_LIMIT = 1000;
const SCHEDULER_TICK_MS = Math.max(1000, Number(process.env.AO_WORKFLOW_SCHEDULER_TICK_MS || 10_000));
const SCHEDULER_ENABLED = String(process.env.AO_WORKFLOW_SCHEDULER_ENABLED || 'true').trim().toLowerCase() !== 'false';
const RUN_TIMEOUT_MS = Math.max(1000, Number(process.env.AO_WORKFLOW_HTTP_TIMEOUT_MS || 30_000));
const RETRY_ATTEMPTS = Math.max(1, Number(process.env.AO_WORKFLOW_RETRY_ATTEMPTS || 3));
const RETRY_BASE_MS = Math.max(0, Number(process.env.AO_WORKFLOW_RETRY_BASE_MS || 250));
const RETRY_MAX_MS = Math.max(RETRY_BASE_MS, Number(process.env.AO_WORKFLOW_RETRY_MAX_MS || 5000));
const MAX_PARALLEL_RUNS = Math.max(1, Number(process.env.AO_WORKFLOW_MAX_PARALLEL_RUNS || 3));
const PROCESS_LOCK_TTL_MS = Math.max(30_000, Number(process.env.AO_WORKFLOW_LOCK_TTL_MS || 5 * 60_000));
const SCHEDULER_LOCK_KEY = 89423011;
const JOB_CLAIM_BATCH = Math.max(1, Number(process.env.AO_WORKFLOW_JOB_CLAIM_BATCH || 20));
const JOB_LOCK_TTL_MS = Math.max(10_000, Number(process.env.AO_WORKFLOW_JOB_LOCK_TTL_MS || 120_000));
const JOB_DEFAULT_MAX_ATTEMPTS = Math.max(1, Number(process.env.AO_WORKFLOW_JOB_MAX_ATTEMPTS || 5));
const JOB_DEFAULT_PRIORITY = Math.max(1, Number(process.env.AO_WORKFLOW_JOB_DEFAULT_PRIORITY || 100));
const CHUNK_SIZE_DEFAULT = Math.max(1, Number(process.env.AO_WORKFLOW_CHUNK_SIZE_DEFAULT || 200));
const WORKER_ID = String(process.env.AO_WORKFLOW_WORKER_ID || `worker_${process.pid}`).trim();
const SCHEDULER_ID = String(process.env.AO_WORKFLOW_SCHEDULER_ID || `scheduler_${process.pid}`).trim();
const DEFAULT_TENANT_ID = String(process.env.AO_WORKFLOW_DEFAULT_TENANT_ID || 'default').trim() || 'default';
const DEFAULT_TENANT_SQL = DEFAULT_TENANT_ID.replace(/'/g, "''");

const schedulerState = {
  enabled: SCHEDULER_ENABLED,
  tickMs: SCHEDULER_TICK_MS,
  timer: null,
  running: false,
  lastTickAt: null,
  lastError: '',
  lastDiscoveryCount: 0,
  lastScheduledCount: 0,
  activeRuns: new Map(),
  workerLastTickAt: null,
  workerLastError: '',
  workerClaimedJobs: 0,
  workerProcessedJobs: 0
};

export const workflowAutomationRouter = express.Router();
workflowAutomationRouter.use(express.json({ limit: '4mb' }));

function requireDataAdmin(req, res, next) {
  const role = String(req.header('X-AO-ROLE') || '').trim();
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

function tryObject(value) {
  if (value && typeof value === 'object' && !Array.isArray(value)) return value;
  if (typeof value !== 'string') return {};
  const raw = String(value || '').trim();
  if (!raw) return {};
  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function parseStorageSettingValue(value) {
  if (typeof value === 'string') {
    const raw = value.trim();
    if (!raw) return { schema: '', table: '' };
    const dot = raw.indexOf('.');
    if (dot > 0 && dot < raw.length - 1) {
      return { schema: raw.slice(0, dot).trim(), table: raw.slice(dot + 1).trim() };
    }
    return { schema: '', table: '' };
  }
  const obj = value && typeof value === 'object' && !Array.isArray(value) ? value : {};
  return {
    schema: String(obj.schema ?? obj.schema_name ?? '').trim(),
    table: String(obj.table ?? obj.table_name ?? '').trim()
  };
}

function normalizeSettingIdent(value, fallback) {
  const v = String(value || '').trim();
  return isIdent(v) ? v : fallback;
}

function buildRunUid(prefix = 'wf') {
  return `${prefix}_${Date.now()}_${Math.random().toString(16).slice(2, 10)}`;
}

function uniqueAliasList(values) {
  return [...new Set((values || []).map((v) => String(v || '').trim()).filter(Boolean))];
}

function parsePathParts(path) {
  const raw = String(path || '').trim();
  if (!raw) return [];
  const normalized = raw.replace(/\[(\d+)\]/g, '.$1');
  return normalized
    .split('.')
    .map((p) => p.trim())
    .filter(Boolean)
    .map((p) => (/^\d+$/.test(p) ? Number(p) : p));
}

function getByPath(obj, path) {
  if (!path) return obj;
  const parts = parsePathParts(path);
  let cur = obj;
  for (const part of parts) {
    if (cur == null) return undefined;
    cur = cur[part];
  }
  return cur;
}

function setByPath(obj, path, value) {
  const parts = parsePathParts(path);
  if (!parts.length) return;
  let cur = obj;
  for (let i = 0; i < parts.length - 1; i += 1) {
    const key = parts[i];
    const nextKey = parts[i + 1];
    if (cur[key] === undefined || cur[key] === null || typeof cur[key] !== 'object') {
      cur[key] = typeof nextKey === 'number' ? [] : {};
    }
    cur = cur[key];
  }
  cur[parts[parts.length - 1]] = value;
}

function deepClone(value) {
  return JSON.parse(JSON.stringify(value));
}

function stableJsonString(value) {
  try {
    return JSON.stringify(value ?? {});
  } catch {
    return '{}';
  }
}

function sha1(value) {
  return crypto.createHash('sha1').update(String(value || '')).digest('hex');
}

function dependencyDispatchMode(raw) {
  const mode = String(raw || '').trim().toLowerCase();
  if (mode === 'once_per_source_run' || mode === 'once_after_all_chunks' || mode === 'for_each_chunk') return mode;
  return 'once_per_source_run';
}

function dependencyDedupeMode(raw) {
  const mode = String(raw || '').trim().toLowerCase();
  if (mode === 'skip_if_target_running' || mode === 'deduplicate_by_payload') return mode;
  return 'none';
}

function findAliasInMap(map, rawAlias) {
  const alias = String(rawAlias || '').trim();
  if (!alias || !map || typeof map !== 'object') return { found: false, key: '' };
  if (Object.prototype.hasOwnProperty.call(map, alias)) {
    return { found: true, key: alias, value: map[alias] };
  }
  const lower = alias.toLowerCase();
  for (const key of Object.keys(map)) {
    if (key.toLowerCase() === lower) return { found: true, key, value: map[key] };
  }
  return { found: false, key: '' };
}

function collectParameterTokens(value, out) {
  if (typeof value === 'string') {
    const matches = value.matchAll(new RegExp(PARAMETER_TOKEN_RE.source, 'g'));
    for (const m of matches) {
      const alias = String(m?.[1] || '').trim();
      if (alias) out.add(alias);
    }
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((item) => collectParameterTokens(item, out));
    return;
  }
  if (value && typeof value === 'object') {
    Object.values(value).forEach((item) => collectParameterTokens(item, out));
  }
}

function replaceParameterTokens(str, map) {
  if (!str || !map || !Object.keys(map).length) return str;
  return str.replace(PARAMETER_TOKEN_RE, (match, rawAlias) => {
    const alias = String(rawAlias || '').trim();
    const resolved = findAliasInMap(map, alias);
    if (!alias || !resolved.found) return match;
    const value = resolved.value;
    if (value === undefined || value === null) return match;
    if (typeof value === 'string') return value;
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
  });
}

function applyParametersToValue(value, map) {
  if (!map || !Object.keys(map).length) return value;
  if (typeof value === 'string') {
    const exact = value.match(PARAMETER_TOKEN_EXACT_RE);
    if (exact?.[1]) {
      const alias = String(exact[1] || '').trim();
      const resolved = findAliasInMap(map, alias);
      if (alias && resolved.found) {
        const raw = resolved.value;
        if (raw !== undefined && raw !== null) return raw;
      }
    }
    return replaceParameterTokens(value, map);
  }
  if (Array.isArray(value)) return value.map((item) => applyParametersToValue(item, map));
  if (value && typeof value === 'object') {
    Object.entries(value).forEach(([key, val]) => {
      value[key] = applyParametersToValue(val, map);
    });
    return value;
  }
  return value;
}

function isExactTemplateAlias(value, aliasesLower) {
  if (typeof value !== 'string' || !aliasesLower.size) return false;
  const exact = value.match(PARAMETER_TOKEN_EXACT_RE);
  if (!exact?.[1]) return false;
  return aliasesLower.has(String(exact[1] || '').trim().toLowerCase());
}

function stripUnresolvedTemplateTokens(value, aliasesLower) {
  if (!value || !aliasesLower.size) return;
  if (Array.isArray(value)) {
    for (let i = value.length - 1; i >= 0; i -= 1) {
      const item = value[i];
      if (isExactTemplateAlias(item, aliasesLower)) {
        value.splice(i, 1);
        continue;
      }
      stripUnresolvedTemplateTokens(item, aliasesLower);
    }
    return;
  }
  if (typeof value !== 'object') return;
  Object.keys(value).forEach((key) => {
    const val = value[key];
    if (isExactTemplateAlias(val, aliasesLower)) {
      delete value[key];
      return;
    }
    stripUnresolvedTemplateTokens(val, aliasesLower);
  });
}

async function delayMs(ms) {
  const n = Number(ms || 0);
  if (!Number.isFinite(n) || n <= 0) return;
  await new Promise((resolve) => setTimeout(resolve, n));
}

function tryParseJson(raw, fallback = {}) {
  if (raw && typeof raw === 'object') return raw;
  if (typeof raw !== 'string') return fallback;
  const txt = String(raw || '').trim();
  if (!txt) return fallback;
  try {
    const parsed = JSON.parse(txt);
    return parsed && typeof parsed === 'object' ? parsed : fallback;
  } catch {
    return fallback;
  }
}

const STORAGE_OBJECT_KEYS = Object.freeze({
  api_configs_storage: ['api_configs_schema', 'api_configs_table'],
  workflow_desks_storage: ['workflow_desks_schema', 'workflow_desks_table'],
  workflow_desk_versions_storage: ['workflow_desks_schema', 'workflow_desk_versions_table'],
  workflow_runs_storage: ['workflow_runs_schema', 'workflow_runs_table'],
  workflow_run_steps_storage: ['workflow_runs_schema', 'workflow_run_steps_table'],
  workflow_process_overrides_storage: ['workflow_runs_schema', 'workflow_process_overrides_table'],
  workflow_process_locks_storage: ['workflow_runs_schema', 'workflow_process_locks_table'],
  workflow_process_bus_storage: ['workflow_runs_schema', 'workflow_process_bus_table'],
  workflow_job_queue_storage: ['workflow_runs_schema', 'workflow_job_queue_table'],
  workflow_incremental_state_storage: ['workflow_runs_schema', 'workflow_incremental_state_table'],
  workflow_dependencies_storage: ['workflow_runs_schema', 'workflow_dependencies_table'],
  workflow_rate_limit_policies_storage: ['workflow_runs_schema', 'workflow_rate_limit_policies_table'],
  workflow_tenant_policies_storage: ['workflow_runs_schema', 'workflow_tenant_policies_table'],
  workflow_run_aggregation_storage: ['workflow_runs_schema', 'workflow_run_aggregation_table'],
  workflow_dead_jobs_storage: ['workflow_runs_schema', 'workflow_dead_jobs_table'],
  workflow_scheduler_leases_storage: ['workflow_runs_schema', 'workflow_scheduler_leases_table'],
  workflow_worker_leases_storage: ['workflow_runs_schema', 'workflow_worker_leases_table'],
  workflow_provider_registry_storage: ['workflow_runs_schema', 'workflow_provider_registry_table'],
  workflow_chunk_logs_storage: ['workflow_runs_schema', 'workflow_chunk_logs_table']
});

const STORAGE_SCHEMA_KEYS = Object.freeze({
  api_configs_storage_schema: 'api_configs_schema',
  workflow_desks_storage_schema: 'workflow_desks_schema',
  workflow_desk_versions_storage_schema: 'workflow_desks_schema',
  workflow_runs_storage_schema: 'workflow_runs_schema',
  workflow_run_steps_storage_schema: 'workflow_runs_schema',
  workflow_process_overrides_storage_schema: 'workflow_runs_schema',
  workflow_process_locks_storage_schema: 'workflow_runs_schema',
  workflow_process_bus_storage_schema: 'workflow_runs_schema',
  workflow_job_queue_storage_schema: 'workflow_runs_schema',
  workflow_incremental_state_storage_schema: 'workflow_runs_schema',
  workflow_dependencies_storage_schema: 'workflow_runs_schema',
  workflow_rate_limit_policies_storage_schema: 'workflow_runs_schema',
  workflow_tenant_policies_storage_schema: 'workflow_runs_schema',
  workflow_run_aggregation_storage_schema: 'workflow_runs_schema',
  workflow_dead_jobs_storage_schema: 'workflow_runs_schema',
  workflow_scheduler_leases_storage_schema: 'workflow_runs_schema',
  workflow_worker_leases_storage_schema: 'workflow_runs_schema',
  workflow_provider_registry_storage_schema: 'workflow_runs_schema',
  workflow_chunk_logs_storage_schema: 'workflow_runs_schema'
});

const STORAGE_TABLE_KEYS = Object.freeze({
  api_configs_storage_table: 'api_configs_table',
  workflow_desks_storage_table: 'workflow_desks_table',
  workflow_desk_versions_storage_table: 'workflow_desk_versions_table',
  workflow_runs_storage_table: 'workflow_runs_table',
  workflow_run_steps_storage_table: 'workflow_run_steps_table',
  workflow_process_overrides_storage_table: 'workflow_process_overrides_table',
  workflow_process_locks_storage_table: 'workflow_process_locks_table',
  workflow_process_bus_storage_table: 'workflow_process_bus_table',
  workflow_job_queue_storage_table: 'workflow_job_queue_table',
  workflow_incremental_state_storage_table: 'workflow_incremental_state_table',
  workflow_dependencies_storage_table: 'workflow_dependencies_table',
  workflow_rate_limit_policies_storage_table: 'workflow_rate_limit_policies_table',
  workflow_tenant_policies_storage_table: 'workflow_tenant_policies_table',
  workflow_run_aggregation_storage_table: 'workflow_run_aggregation_table',
  workflow_dead_jobs_storage_table: 'workflow_dead_jobs_table',
  workflow_scheduler_leases_storage_table: 'workflow_scheduler_leases_table',
  workflow_worker_leases_storage_table: 'workflow_worker_leases_table',
  workflow_provider_registry_storage_table: 'workflow_provider_registry_table',
  workflow_chunk_logs_storage_table: 'workflow_chunk_logs_table'
});

function applyStorageSetting(next, key, value) {
  if (Object.prototype.hasOwnProperty.call(STORAGE_OBJECT_KEYS, key)) {
    const [schemaKey, tableKey] = STORAGE_OBJECT_KEYS[key];
    const parsed = parseStorageSettingValue(value);
    next[schemaKey] = normalizeSettingIdent(parsed.schema, next[schemaKey]);
    next[tableKey] = normalizeSettingIdent(parsed.table, next[tableKey]);
    return true;
  }
  if (Object.prototype.hasOwnProperty.call(STORAGE_SCHEMA_KEYS, key)) {
    const schemaKey = STORAGE_SCHEMA_KEYS[key];
    next[schemaKey] = normalizeSettingIdent(value, next[schemaKey]);
    return true;
  }
  if (Object.prototype.hasOwnProperty.call(STORAGE_TABLE_KEYS, key)) {
    const tableKey = STORAGE_TABLE_KEYS[key];
    next[tableKey] = normalizeSettingIdent(value, next[tableKey]);
    return true;
  }
  return false;
}

async function tableExists(client, schema, table) {
  const r = await client.query(
    `
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = $1 AND table_name = $2
    LIMIT 1
    `,
    [schema, table]
  );
  return Boolean(r.rows?.length);
}

async function loadRuntimeStorageConfig(client) {
  const next = { ...DEFAULT_CONFIG };
  const hasSettings = await tableExists(client, SETTINGS_SCHEMA, SETTINGS_TABLE);
  if (!hasSettings) return next;

  const r = await client.query(
    `
    SELECT setting_key, setting_value
    FROM ${qname(SETTINGS_SCHEMA, SETTINGS_TABLE)}
    WHERE is_active = true
      AND scope = 'global'
    `
  );
  const rows = Array.isArray(r.rows) ? r.rows : [];
  for (const row of rows) {
    const key = String(row?.setting_key || '').trim();
    const value = row?.setting_value;
    if (applyStorageSetting(next, key, value)) continue;
    if (key === 'api_configs_storage') {
      const parsed = parseStorageSettingValue(value);
      next.api_configs_schema = normalizeSettingIdent(parsed.schema, next.api_configs_schema);
      next.api_configs_table = normalizeSettingIdent(parsed.table, next.api_configs_table);
      continue;
    }
    if (key === 'api_configs_storage_schema') {
      next.api_configs_schema = normalizeSettingIdent(value, next.api_configs_schema);
      continue;
    }
    if (key === 'api_configs_storage_table') {
      next.api_configs_table = normalizeSettingIdent(value, next.api_configs_table);
      continue;
    }
    if (key === 'workflow_desks_storage') {
      const parsed = parseStorageSettingValue(value);
      next.workflow_desks_schema = normalizeSettingIdent(parsed.schema, next.workflow_desks_schema);
      next.workflow_desks_table = normalizeSettingIdent(parsed.table, next.workflow_desks_table);
      continue;
    }
    if (key === 'workflow_desks_storage_schema') {
      next.workflow_desks_schema = normalizeSettingIdent(value, next.workflow_desks_schema);
      continue;
    }
    if (key === 'workflow_desks_storage_table') {
      next.workflow_desks_table = normalizeSettingIdent(value, next.workflow_desks_table);
      continue;
    }
    if (key === 'workflow_desk_versions_storage') {
      const parsed = parseStorageSettingValue(value);
      next.workflow_desks_schema = normalizeSettingIdent(parsed.schema, next.workflow_desks_schema);
      next.workflow_desk_versions_table = normalizeSettingIdent(parsed.table, next.workflow_desk_versions_table);
      continue;
    }
    if (key === 'workflow_desk_versions_storage_schema') {
      next.workflow_desks_schema = normalizeSettingIdent(value, next.workflow_desks_schema);
      continue;
    }
    if (key === 'workflow_desk_versions_storage_table') {
      next.workflow_desk_versions_table = normalizeSettingIdent(value, next.workflow_desk_versions_table);
      continue;
    }
  }
  return next;
}

function workflowRunsQname(config) {
  return qname(config.workflow_runs_schema, config.workflow_runs_table);
}

function workflowRunStepsQname(config) {
  return qname(config.workflow_runs_schema, config.workflow_run_steps_table);
}

function workflowDesksQname(config) {
  return qname(config.workflow_desks_schema, config.workflow_desks_table);
}

function workflowDeskVersionsQname(config) {
  return qname(config.workflow_desks_schema, config.workflow_desk_versions_table);
}

function apiConfigsQname(config) {
  return qname(config.api_configs_schema, config.api_configs_table);
}

function workflowProcessOverridesQname(config) {
  return qname(config.workflow_runs_schema, config.workflow_process_overrides_table);
}

function workflowProcessLocksQname(config) {
  return qname(config.workflow_runs_schema, config.workflow_process_locks_table);
}

function workflowProcessBusQname(config) {
  return qname(config.workflow_runs_schema, config.workflow_process_bus_table);
}

function workflowJobQueueQname(config) {
  return qname(config.workflow_runs_schema, config.workflow_job_queue_table);
}

function workflowIncrementalStateQname(config) {
  return qname(config.workflow_runs_schema, config.workflow_incremental_state_table);
}

function workflowDependenciesQname(config) {
  return qname(config.workflow_runs_schema, config.workflow_dependencies_table);
}

function workflowRateLimitPoliciesQname(config) {
  return qname(config.workflow_runs_schema, config.workflow_rate_limit_policies_table);
}

function workflowTenantPoliciesQname(config) {
  return qname(config.workflow_runs_schema, config.workflow_tenant_policies_table);
}

function workflowRunAggregationQname(config) {
  return qname(config.workflow_runs_schema, config.workflow_run_aggregation_table);
}

function workflowDeadJobsQname(config) {
  return qname(config.workflow_runs_schema, config.workflow_dead_jobs_table);
}

function workflowSchedulerLeasesQname(config) {
  return qname(config.workflow_runs_schema, config.workflow_scheduler_leases_table);
}

function workflowWorkerLeasesQname(config) {
  return qname(config.workflow_runs_schema, config.workflow_worker_leases_table);
}

function workflowProviderRegistryQname(config) {
  return qname(config.workflow_runs_schema, config.workflow_provider_registry_table);
}

function workflowChunkLogsQname(config) {
  return qname(config.workflow_runs_schema, config.workflow_chunk_logs_table);
}

async function ensureWorkflowAutomationTables(client, config) {
  await client.query(`CREATE SCHEMA IF NOT EXISTS ${qi(config.workflow_desks_schema)}`);
  await client.query(`CREATE SCHEMA IF NOT EXISTS ${qi(config.workflow_runs_schema)}`);

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${workflowDeskVersionsQname(config)} (
      id bigserial PRIMARY KEY,
      desk_id bigint NOT NULL,
      version_no integer NOT NULL,
      graph_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      source_revision integer NOT NULL DEFAULT 0,
      is_published boolean NOT NULL DEFAULT false,
      created_at timestamptz NOT NULL DEFAULT now(),
      created_by text NOT NULL DEFAULT 'system',
      published_at timestamptz,
      published_by text NOT NULL DEFAULT '',
      UNIQUE(desk_id, version_no)
    )
  `);
  await client.query(`
    ALTER TABLE ${workflowDeskVersionsQname(config)}
      ADD COLUMN IF NOT EXISTS graph_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      ADD COLUMN IF NOT EXISTS source_revision integer NOT NULL DEFAULT 0,
      ADD COLUMN IF NOT EXISTS is_published boolean NOT NULL DEFAULT false,
      ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
      ADD COLUMN IF NOT EXISTS created_by text NOT NULL DEFAULT 'system',
      ADD COLUMN IF NOT EXISTS published_at timestamptz,
      ADD COLUMN IF NOT EXISTS published_by text NOT NULL DEFAULT ''
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_workflow_desk_versions_lookup_idx
    ON ${workflowDeskVersionsQname(config)} (desk_id, version_no DESC)
  `);
  await client.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS ao_workflow_desk_versions_one_published_idx
    ON ${workflowDeskVersionsQname(config)} (desk_id)
    WHERE is_published = true
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${workflowProcessOverridesQname(config)} (
      desk_id bigint NOT NULL,
      start_node_id text NOT NULL,
      is_enabled boolean NOT NULL DEFAULT true,
      trigger_type text NOT NULL DEFAULT '',
      schedule_value text NOT NULL DEFAULT '',
      timezone text NOT NULL DEFAULT '',
      run_policy text NOT NULL DEFAULT '',
      process_code text NOT NULL DEFAULT '',
      input_scope text NOT NULL DEFAULT '',
      output_scope text NOT NULL DEFAULT '',
      updated_at timestamptz NOT NULL DEFAULT now(),
      updated_by text NOT NULL DEFAULT 'system',
      PRIMARY KEY (desk_id, start_node_id)
    )
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_workflow_process_overrides_lookup_idx
    ON ${workflowProcessOverridesQname(config)} (desk_id, updated_at DESC)
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${workflowProcessLocksQname(config)} (
      desk_id bigint NOT NULL,
      start_node_id text NOT NULL,
      lock_key text NOT NULL DEFAULT '',
      locked_at timestamptz NOT NULL DEFAULT now(),
      locked_until timestamptz NOT NULL DEFAULT now(),
      owner_id text NOT NULL DEFAULT '',
      PRIMARY KEY (desk_id, start_node_id)
    )
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_workflow_process_locks_until_idx
    ON ${workflowProcessLocksQname(config)} (locked_until)
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${workflowProcessBusQname(config)} (
      id bigserial PRIMARY KEY,
      desk_id bigint NOT NULL,
      run_uid text NOT NULL DEFAULT '',
      process_code text NOT NULL DEFAULT '',
      channel text NOT NULL DEFAULT '',
      payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      status text NOT NULL DEFAULT 'new',
      created_at timestamptz NOT NULL DEFAULT now(),
      consumed_at timestamptz,
      consumed_by text NOT NULL DEFAULT ''
    )
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_workflow_process_bus_lookup_idx
    ON ${workflowProcessBusQname(config)} (desk_id, status, channel, created_at)
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${workflowRunsQname(config)} (
      id bigserial PRIMARY KEY,
      run_uid text NOT NULL UNIQUE,
      desk_id bigint NOT NULL,
      desk_name text NOT NULL DEFAULT '',
      desk_version_id bigint NOT NULL DEFAULT 0,
      start_node_id text NOT NULL DEFAULT '',
      process_code text NOT NULL DEFAULT '',
      run_policy text NOT NULL DEFAULT 'single_instance',
      trigger_source text NOT NULL DEFAULT 'manual',
      trigger_type text NOT NULL DEFAULT 'manual',
      trigger_key text NOT NULL DEFAULT '',
      trigger_meta jsonb NOT NULL DEFAULT '{}'::jsonb,
      status text NOT NULL DEFAULT 'running',
      started_at timestamptz NOT NULL DEFAULT now(),
      finished_at timestamptz,
      duration_ms integer NOT NULL DEFAULT 0,
      summary_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      error_text text NOT NULL DEFAULT '',
      created_by text NOT NULL DEFAULT 'system',
      updated_at timestamptz NOT NULL DEFAULT now()
    )
  `);
  await client.query(`
    ALTER TABLE ${workflowRunsQname(config)}
      ADD COLUMN IF NOT EXISTS desk_version_id bigint NOT NULL DEFAULT 0,
      ADD COLUMN IF NOT EXISTS start_node_id text NOT NULL DEFAULT '',
      ADD COLUMN IF NOT EXISTS process_code text NOT NULL DEFAULT '',
      ADD COLUMN IF NOT EXISTS run_policy text NOT NULL DEFAULT 'single_instance',
      ADD COLUMN IF NOT EXISTS trigger_type text NOT NULL DEFAULT 'manual'
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_workflow_runs_lookup_idx
    ON ${workflowRunsQname(config)} (desk_id, trigger_source, trigger_key, started_at DESC)
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_workflow_runs_status_idx
    ON ${workflowRunsQname(config)} (status, started_at DESC)
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_workflow_runs_process_lookup_idx
    ON ${workflowRunsQname(config)} (desk_id, desk_version_id, start_node_id, started_at DESC)
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${workflowRunStepsQname(config)} (
      id bigserial PRIMARY KEY,
      run_uid text NOT NULL,
      step_no integer NOT NULL,
      step_order integer NOT NULL DEFAULT 1,
      node_id text NOT NULL DEFAULT '',
      node_name text NOT NULL DEFAULT '',
      node_type text NOT NULL DEFAULT '',
      status text NOT NULL DEFAULT 'ok',
      started_at timestamptz NOT NULL DEFAULT now(),
      finished_at timestamptz NOT NULL DEFAULT now(),
      duration_ms integer NOT NULL DEFAULT 0,
      input_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      output_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      request_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
      response_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
      metrics_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      error_text text NOT NULL DEFAULT ''
    )
  `);
  await client.query(`
    ALTER TABLE ${workflowRunStepsQname(config)}
      ADD COLUMN IF NOT EXISTS step_order integer NOT NULL DEFAULT 1,
      ADD COLUMN IF NOT EXISTS input_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      ADD COLUMN IF NOT EXISTS output_json jsonb NOT NULL DEFAULT '{}'::jsonb
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_workflow_run_steps_lookup_idx
    ON ${workflowRunStepsQname(config)} (run_uid, step_no)
  `);

  await client.query(`
    ALTER TABLE ${workflowProcessOverridesQname(config)}
      ADD COLUMN IF NOT EXISTS tenant_id text NOT NULL DEFAULT 'default',
      ADD COLUMN IF NOT EXISTS client_id text NOT NULL DEFAULT ''
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_workflow_process_overrides_tenant_idx
    ON ${workflowProcessOverridesQname(config)} (tenant_id, desk_id, start_node_id)
  `);

  await client.query(`
    ALTER TABLE ${workflowRunsQname(config)}
      ADD COLUMN IF NOT EXISTS tenant_id text NOT NULL DEFAULT 'default',
      ADD COLUMN IF NOT EXISTS client_id text NOT NULL DEFAULT '',
      ADD COLUMN IF NOT EXISTS orchestration_mode text NOT NULL DEFAULT 'queue_chunks'
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_workflow_runs_tenant_idx
    ON ${workflowRunsQname(config)} (tenant_id, status, started_at DESC)
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${workflowJobQueueQname(config)} (
      job_id bigserial PRIMARY KEY,
      tenant_id text NOT NULL DEFAULT 'default',
      client_id text NOT NULL DEFAULT '',
      desk_id bigint NOT NULL DEFAULT 0,
      desk_version_id bigint NOT NULL DEFAULT 0,
      start_node_id text NOT NULL DEFAULT '',
      process_code text NOT NULL DEFAULT '',
      parent_run_uid text NOT NULL DEFAULT '',
      parent_job_id bigint NOT NULL DEFAULT 0,
      job_type text NOT NULL DEFAULT '',
      payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      dedupe_key text NOT NULL DEFAULT '',
      priority integer NOT NULL DEFAULT ${JOB_DEFAULT_PRIORITY},
      status text NOT NULL DEFAULT 'queued',
      available_at timestamptz NOT NULL DEFAULT now(),
      attempt_no integer NOT NULL DEFAULT 0,
      max_attempts integer NOT NULL DEFAULT ${JOB_DEFAULT_MAX_ATTEMPTS},
      locked_by text NOT NULL DEFAULT '',
      locked_until timestamptz,
      last_error text NOT NULL DEFAULT '',
      last_response_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    )
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_workflow_job_queue_pick_idx
    ON ${workflowJobQueueQname(config)} (status, available_at, priority, job_id)
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_workflow_job_queue_tenant_idx
    ON ${workflowJobQueueQname(config)} (tenant_id, status, available_at)
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_workflow_job_queue_run_idx
    ON ${workflowJobQueueQname(config)} (parent_run_uid, status, job_id)
  `);
  await client.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS ao_workflow_job_queue_dedupe_idx
    ON ${workflowJobQueueQname(config)} (dedupe_key)
    WHERE dedupe_key <> '' AND status IN ('queued', 'locked', 'running')
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${workflowIncrementalStateQname(config)} (
      id bigserial PRIMARY KEY,
      tenant_id text NOT NULL DEFAULT 'default',
      desk_id bigint NOT NULL DEFAULT 0,
      desk_version_id bigint NOT NULL DEFAULT 0,
      start_node_id text NOT NULL DEFAULT '',
      process_code text NOT NULL DEFAULT '',
      state_key text NOT NULL DEFAULT 'default',
      cursor_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      watermark_ts timestamptz,
      last_successful_sync_at timestamptz,
      last_attempt_at timestamptz,
      slice_start text NOT NULL DEFAULT '',
      slice_end text NOT NULL DEFAULT '',
      etag text NOT NULL DEFAULT '',
      page_marker text NOT NULL DEFAULT '',
      sequence_value bigint NOT NULL DEFAULT 0,
      extra_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      updated_at timestamptz NOT NULL DEFAULT now(),
      UNIQUE(tenant_id, desk_id, start_node_id, process_code, state_key)
    )
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_workflow_incremental_state_lookup_idx
    ON ${workflowIncrementalStateQname(config)} (tenant_id, desk_id, process_code, updated_at DESC)
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${workflowDependenciesQname(config)} (
      id bigserial PRIMARY KEY,
      tenant_id text NOT NULL DEFAULT 'default',
      desk_id bigint NOT NULL DEFAULT 0,
      source_start_node_id text NOT NULL DEFAULT '',
      target_start_node_id text NOT NULL DEFAULT '',
      trigger_event_type text NOT NULL DEFAULT '',
      trigger_status text NOT NULL DEFAULT '',
      condition_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      dispatch_mode text NOT NULL DEFAULT 'enqueue',
      dedupe_policy text NOT NULL DEFAULT 'event_once',
      is_enabled boolean NOT NULL DEFAULT true,
      updated_at timestamptz NOT NULL DEFAULT now(),
      updated_by text NOT NULL DEFAULT 'system'
    )
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_workflow_dependencies_source_idx
    ON ${workflowDependenciesQname(config)} (tenant_id, desk_id, source_start_node_id, is_enabled)
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${workflowRateLimitPoliciesQname(config)} (
      id bigserial PRIMARY KEY,
      tenant_id text NOT NULL DEFAULT '*',
      provider_code text NOT NULL DEFAULT '*',
      endpoint_code text NOT NULL DEFAULT '*',
      max_rps numeric NOT NULL DEFAULT 0,
      max_parallel_jobs integer NOT NULL DEFAULT 0,
      burst_limit integer NOT NULL DEFAULT 0,
      cooldown_ms integer NOT NULL DEFAULT 0,
      retry_policy_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      is_enabled boolean NOT NULL DEFAULT true,
      updated_at timestamptz NOT NULL DEFAULT now(),
      UNIQUE(tenant_id, provider_code, endpoint_code)
    )
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${workflowTenantPoliciesQname(config)} (
      tenant_id text PRIMARY KEY,
      max_parallel_runs integer NOT NULL DEFAULT ${MAX_PARALLEL_RUNS},
      max_parallel_jobs integer NOT NULL DEFAULT ${MAX_PARALLEL_RUNS * 4},
      max_queue_depth integer NOT NULL DEFAULT 50000,
      default_priority integer NOT NULL DEFAULT ${JOB_DEFAULT_PRIORITY},
      weight integer NOT NULL DEFAULT 1,
      is_enabled boolean NOT NULL DEFAULT true,
      updated_at timestamptz NOT NULL DEFAULT now()
    )
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${workflowRunAggregationQname(config)} (
      run_uid text PRIMARY KEY,
      tenant_id text NOT NULL DEFAULT 'default',
      desk_id bigint NOT NULL DEFAULT 0,
      desk_version_id bigint NOT NULL DEFAULT 0,
      start_node_id text NOT NULL DEFAULT '',
      process_code text NOT NULL DEFAULT '',
      total_jobs integer NOT NULL DEFAULT 0,
      queued_jobs integer NOT NULL DEFAULT 0,
      running_jobs integer NOT NULL DEFAULT 0,
      completed_jobs integer NOT NULL DEFAULT 0,
      failed_jobs integer NOT NULL DEFAULT 0,
      dead_letter_jobs integer NOT NULL DEFAULT 0,
      skipped_jobs integer NOT NULL DEFAULT 0,
      progress_percent numeric NOT NULL DEFAULT 0,
      final_status text NOT NULL DEFAULT 'running',
      last_job_at timestamptz,
      updated_at timestamptz NOT NULL DEFAULT now()
    )
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_workflow_run_aggregation_lookup_idx
    ON ${workflowRunAggregationQname(config)} (tenant_id, desk_id, final_status, updated_at DESC)
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${workflowDeadJobsQname(config)} (
      id bigserial PRIMARY KEY,
      job_id bigint NOT NULL,
      run_uid text NOT NULL DEFAULT '',
      tenant_id text NOT NULL DEFAULT 'default',
      desk_id bigint NOT NULL DEFAULT 0,
      desk_version_id bigint NOT NULL DEFAULT 0,
      start_node_id text NOT NULL DEFAULT '',
      process_code text NOT NULL DEFAULT '',
      job_type text NOT NULL DEFAULT '',
      dedupe_key text NOT NULL DEFAULT '',
      payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      attempt_no integer NOT NULL DEFAULT 0,
      max_attempts integer NOT NULL DEFAULT 0,
      error_text text NOT NULL DEFAULT '',
      failed_at timestamptz NOT NULL DEFAULT now()
    )
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_workflow_dead_jobs_lookup_idx
    ON ${workflowDeadJobsQname(config)} (tenant_id, desk_id, failed_at DESC)
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${workflowSchedulerLeasesQname(config)} (
      lease_key text PRIMARY KEY,
      owner_id text NOT NULL DEFAULT '',
      locked_at timestamptz NOT NULL DEFAULT now(),
      locked_until timestamptz NOT NULL DEFAULT now(),
      meta_json jsonb NOT NULL DEFAULT '{}'::jsonb
    )
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${workflowWorkerLeasesQname(config)} (
      worker_id text PRIMARY KEY,
      owner_id text NOT NULL DEFAULT '',
      tenant_id text NOT NULL DEFAULT 'default',
      status text NOT NULL DEFAULT 'idle',
      heartbeat_at timestamptz NOT NULL DEFAULT now(),
      lease_until timestamptz NOT NULL DEFAULT now(),
      active_jobs integer NOT NULL DEFAULT 0,
      meta_json jsonb NOT NULL DEFAULT '{}'::jsonb
    )
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_workflow_worker_leases_lookup_idx
    ON ${workflowWorkerLeasesQname(config)} (status, lease_until, heartbeat_at DESC)
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${workflowProviderRegistryQname(config)} (
      id bigserial PRIMARY KEY,
      provider_code text NOT NULL DEFAULT '',
      endpoint_code text NOT NULL DEFAULT '',
      base_url text NOT NULL DEFAULT '',
      endpoint_path text NOT NULL DEFAULT '',
      source_type text NOT NULL DEFAULT 'http',
      auth_mode text NOT NULL DEFAULT '',
      metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      is_enabled boolean NOT NULL DEFAULT true,
      updated_at timestamptz NOT NULL DEFAULT now(),
      UNIQUE(provider_code, endpoint_code)
    )
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${workflowChunkLogsQname(config)} (
      id bigserial PRIMARY KEY,
      job_id bigint NOT NULL DEFAULT 0,
      run_uid text NOT NULL DEFAULT '',
      tenant_id text NOT NULL DEFAULT 'default',
      desk_id bigint NOT NULL DEFAULT 0,
      desk_version_id bigint NOT NULL DEFAULT 0,
      start_node_id text NOT NULL DEFAULT '',
      process_code text NOT NULL DEFAULT '',
      provider_code text NOT NULL DEFAULT '',
      endpoint_code text NOT NULL DEFAULT '',
      chunk_key text NOT NULL DEFAULT '',
      chunk_no integer NOT NULL DEFAULT 1,
      status text NOT NULL DEFAULT 'ok',
      started_at timestamptz NOT NULL DEFAULT now(),
      finished_at timestamptz NOT NULL DEFAULT now(),
      duration_ms integer NOT NULL DEFAULT 0,
      input_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      output_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      metrics_json jsonb NOT NULL DEFAULT '{}'::jsonb,
      error_text text NOT NULL DEFAULT ''
    )
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_workflow_chunk_logs_lookup_idx
    ON ${workflowChunkLogsQname(config)} (tenant_id, run_uid, started_at DESC)
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS ao_workflow_chunk_logs_rate_idx
    ON ${workflowChunkLogsQname(config)} (tenant_id, provider_code, endpoint_code, finished_at DESC)
  `);
}

function parseJsonb(v, fallback = {}) {
  if (v && typeof v === 'object') return v;
  if (typeof v !== 'string') return fallback;
  try {
    const parsed = JSON.parse(v);
    return parsed && typeof parsed === 'object' ? parsed : fallback;
  } catch {
    return fallback;
  }
}

function isTerminalRunStatus(statusRaw) {
  const s = String(statusRaw || '').trim().toLowerCase();
  return s === 'completed' || s === 'completed_with_errors' || s === 'failed' || s === 'cancelled' || s === 'skipped';
}

async function upsertSchedulerLease(client, config, patch = {}) {
  const qn = workflowSchedulerLeasesQname(config);
  const ttlMs = Math.max(5_000, Number(patch?.ttl_ms || schedulerState.tickMs * 3 || 30_000));
  await client.query(
    `
    INSERT INTO ${qn} (lease_key, owner_id, locked_at, locked_until, meta_json)
    VALUES ('main_scheduler', $1, now(), now() + ($2 || ' milliseconds')::interval, $3::jsonb)
    ON CONFLICT (lease_key) DO UPDATE
    SET owner_id = EXCLUDED.owner_id,
        locked_at = EXCLUDED.locked_at,
        locked_until = EXCLUDED.locked_until,
        meta_json = EXCLUDED.meta_json
    `,
    [String(patch?.owner_id || SCHEDULER_ID).trim(), String(ttlMs), stableJsonString(patch?.meta_json || {})]
  );
}

async function upsertWorkerLease(client, config, patch = {}) {
  const qn = workflowWorkerLeasesQname(config);
  const ttlMs = Math.max(5_000, Number(patch?.ttl_ms || schedulerState.tickMs * 3 || 30_000));
  await client.query(
    `
    INSERT INTO ${qn}
      (worker_id, owner_id, tenant_id, status, heartbeat_at, lease_until, active_jobs, meta_json)
    VALUES
      ($1, $2, $3, $4, now(), now() + ($5 || ' milliseconds')::interval, $6, $7::jsonb)
    ON CONFLICT (worker_id) DO UPDATE
    SET owner_id = EXCLUDED.owner_id,
        tenant_id = EXCLUDED.tenant_id,
        status = EXCLUDED.status,
        heartbeat_at = EXCLUDED.heartbeat_at,
        lease_until = EXCLUDED.lease_until,
        active_jobs = EXCLUDED.active_jobs,
        meta_json = EXCLUDED.meta_json
    `,
    [
      String(patch?.worker_id || WORKER_ID).trim(),
      String(patch?.owner_id || WORKER_ID).trim(),
      String(patch?.tenant_id || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID,
      String(patch?.status || 'running').trim(),
      String(ttlMs),
      Math.max(0, Math.trunc(Number(patch?.active_jobs || 0))),
      stableJsonString(patch?.meta_json || {})
    ]
  );
}

async function upsertRunAggregationRow(client, config, payload = {}) {
  const qn = workflowRunAggregationQname(config);
  await client.query(
    `
    INSERT INTO ${qn}
      (run_uid, tenant_id, desk_id, desk_version_id, start_node_id, process_code, final_status, updated_at)
    VALUES
      ($1, $2, $3, $4, $5, $6, 'running', now())
    ON CONFLICT (run_uid) DO UPDATE
    SET tenant_id = EXCLUDED.tenant_id,
        desk_id = EXCLUDED.desk_id,
        desk_version_id = EXCLUDED.desk_version_id,
        start_node_id = EXCLUDED.start_node_id,
        process_code = EXCLUDED.process_code,
        updated_at = now()
    `,
    [
      String(payload?.run_uid || '').trim(),
      String(payload?.tenant_id || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID,
      Math.trunc(Number(payload?.desk_id || 0)),
      Math.trunc(Number(payload?.desk_version_id || 0)),
      String(payload?.start_node_id || '').trim(),
      String(payload?.process_code || '').trim()
    ]
  );
}

async function recomputeRunAggregation(client, config, runUid) {
  const queueQn = workflowJobQueueQname(config);
  const aggQn = workflowRunAggregationQname(config);
  const runQn = workflowRunsQname(config);
  const runId = String(runUid || '').trim();
  if (!runId) return null;

  const countRes = await client.query(
    `
    SELECT
      COUNT(*)::int AS total_jobs,
      COUNT(*) FILTER (WHERE status = 'queued')::int AS queued_jobs,
      COUNT(*) FILTER (WHERE status IN ('locked', 'running'))::int AS running_jobs,
      COUNT(*) FILTER (WHERE status = 'completed')::int AS completed_jobs,
      COUNT(*) FILTER (WHERE status = 'failed')::int AS failed_jobs,
      COUNT(*) FILTER (WHERE status = 'dead_letter')::int AS dead_letter_jobs,
      COUNT(*) FILTER (WHERE status = 'cancelled')::int AS skipped_jobs
    FROM ${queueQn}
    WHERE parent_run_uid = $1
    `,
    [runId]
  );
  const c = countRes.rows?.[0] || {};
  const totalJobs = Math.max(0, Number(c.total_jobs || 0));
  const queuedJobs = Math.max(0, Number(c.queued_jobs || 0));
  const runningJobs = Math.max(0, Number(c.running_jobs || 0));
  const completedJobs = Math.max(0, Number(c.completed_jobs || 0));
  const failedJobs = Math.max(0, Number(c.failed_jobs || 0));
  const deadJobs = Math.max(0, Number(c.dead_letter_jobs || 0));
  const skippedJobs = Math.max(0, Number(c.skipped_jobs || 0));
  const doneJobs = completedJobs + failedJobs + deadJobs + skippedJobs;
  const progressPercent = totalJobs > 0 ? Math.min(100, (doneJobs / totalJobs) * 100) : 0;

  let finalStatus = 'running';
  if (totalJobs > 0 && queuedJobs === 0 && runningJobs === 0) {
    if (deadJobs > 0 || failedJobs > 0) finalStatus = 'failed';
    else finalStatus = 'completed';
  }

  await client.query(
    `
    UPDATE ${aggQn}
    SET
      total_jobs = $2,
      queued_jobs = $3,
      running_jobs = $4,
      completed_jobs = $5,
      failed_jobs = $6,
      dead_letter_jobs = $7,
      skipped_jobs = $8,
      progress_percent = $9,
      final_status = $10,
      last_job_at = now(),
      updated_at = now()
    WHERE run_uid = $1
    `,
    [runId, totalJobs, queuedJobs, runningJobs, completedJobs, failedJobs, deadJobs, skippedJobs, progressPercent, finalStatus]
  );

  if (finalStatus !== 'running') {
    const runRes = await client.query(
      `
      SELECT status
      FROM ${runQn}
      WHERE run_uid = $1
      LIMIT 1
      `,
      [runId]
    );
    const currentStatus = String(runRes.rows?.[0]?.status || '').trim();
    if (!isTerminalRunStatus(currentStatus)) {
      const summary = {
        total_jobs: totalJobs,
        completed_jobs: completedJobs,
        failed_jobs: failedJobs,
        dead_letter_jobs: deadJobs,
        skipped_jobs: skippedJobs,
        progress_percent: progressPercent
      };
      await updateRunRow(client, config, runId, {
        status: finalStatus === 'completed' ? 'completed' : 'failed',
        finished_at: new Date().toISOString(),
        duration_ms: 0,
        summary_json: summary,
        error_text: finalStatus === 'completed' ? '' : 'chunk_jobs_failed'
      });
    }
  }

  return {
    run_uid: runId,
    total_jobs: totalJobs,
    queued_jobs: queuedJobs,
    running_jobs: runningJobs,
    completed_jobs: completedJobs,
    failed_jobs: failedJobs,
    dead_letter_jobs: deadJobs,
    skipped_jobs: skippedJobs,
    progress_percent: progressPercent,
    final_status: finalStatus
  };
}

async function enqueueWorkflowJob(client, config, job = {}) {
  const qn = workflowJobQueueQname(config);
  const dedupeKey = String(job?.dedupe_key || '').trim();
  const r = await client.query(
    `
    INSERT INTO ${qn}
      (
        tenant_id,
        client_id,
        desk_id,
        desk_version_id,
        start_node_id,
        process_code,
        parent_run_uid,
        parent_job_id,
        job_type,
        payload_json,
        dedupe_key,
        priority,
        status,
        available_at,
        attempt_no,
        max_attempts,
        locked_by,
        locked_until,
        last_error,
        last_response_json,
        created_at,
        updated_at
      )
    VALUES
      ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $12, 'queued', COALESCE($13::timestamptz, now()), 0, $14, '', null, '', '{}'::jsonb, now(), now())
    ON CONFLICT (dedupe_key) WHERE dedupe_key <> '' AND status IN ('queued', 'locked', 'running')
    DO NOTHING
    RETURNING job_id
    `,
    [
      String(job?.tenant_id || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID,
      String(job?.client_id || '').trim(),
      Math.trunc(Number(job?.desk_id || 0)),
      Math.trunc(Number(job?.desk_version_id || 0)),
      String(job?.start_node_id || '').trim(),
      String(job?.process_code || '').trim(),
      String(job?.parent_run_uid || '').trim(),
      Math.trunc(Number(job?.parent_job_id || 0)),
      String(job?.job_type || '').trim(),
      stableJsonString(job?.payload_json || {}),
      dedupeKey,
      Math.max(1, Math.trunc(Number(job?.priority || JOB_DEFAULT_PRIORITY))),
      job?.available_at ? String(job.available_at) : null,
      Math.max(1, Math.trunc(Number(job?.max_attempts || JOB_DEFAULT_MAX_ATTEMPTS)))
    ]
  );
  return Number(r.rows?.[0]?.job_id || 0);
}

async function claimWorkflowJobs(client, config, limit = JOB_CLAIM_BATCH) {
  const qn = workflowJobQueueQname(config);
  const r = await client.query(
    `
    WITH pick AS (
      SELECT job_id
      FROM ${qn}
      WHERE status = 'queued'
        AND available_at <= now()
      ORDER BY priority ASC, available_at ASC, job_id ASC
      LIMIT $1
      FOR UPDATE SKIP LOCKED
    )
    UPDATE ${qn} q
    SET
      status = 'running',
      locked_by = $2,
      locked_until = now() + ($3 || ' milliseconds')::interval,
      attempt_no = q.attempt_no + 1,
      updated_at = now()
    FROM pick
    WHERE q.job_id = pick.job_id
    RETURNING q.*
    `,
    [Math.max(1, Math.min(500, Math.trunc(Number(limit || JOB_CLAIM_BATCH)))), WORKER_ID, String(JOB_LOCK_TTL_MS)]
  );
  return Array.isArray(r.rows) ? r.rows : [];
}

async function completeWorkflowJob(client, config, jobId, response = {}) {
  const qn = workflowJobQueueQname(config);
  await client.query(
    `
    UPDATE ${qn}
    SET
      status = 'completed',
      locked_until = now(),
      last_response_json = $2::jsonb,
      updated_at = now()
    WHERE job_id = $1
    `,
    [Math.trunc(Number(jobId || 0)), stableJsonString(response || {})]
  );
}

async function requeueWorkflowJob(client, config, jobId, patch = {}) {
  const qn = workflowJobQueueQname(config);
  const preserveAttempt = Boolean(patch?.preserve_attempt);
  await client.query(
    `
    UPDATE ${qn}
    SET
      status = 'queued',
      available_at = COALESCE($2::timestamptz, now()),
      locked_until = null,
      last_error = $3,
      attempt_no = CASE WHEN $4 THEN GREATEST(0, attempt_no - 1) ELSE attempt_no END,
      updated_at = now()
    WHERE job_id = $1
    `,
    [
      Math.trunc(Number(jobId || 0)),
      patch?.available_at ? String(patch.available_at) : null,
      String(patch?.error_text || '').trim(),
      preserveAttempt
    ]
  );
}

async function failWorkflowJob(client, config, job, errorText = '') {
  const qn = workflowJobQueueQname(config);
  const deadQn = workflowDeadJobsQname(config);
  const jobId = Math.trunc(Number(job?.job_id || 0));
  const attempts = Math.trunc(Number(job?.attempt_no || 0));
  const maxAttempts = Math.max(1, Math.trunc(Number(job?.max_attempts || JOB_DEFAULT_MAX_ATTEMPTS)));
  if (!jobId) return;
  if (attempts < maxAttempts) {
    const backoffMs = Math.min(60_000, retryDelayMs(attempts, RETRY_BASE_MS, RETRY_MAX_MS));
    const availableAt = new Date(Date.now() + backoffMs).toISOString();
    await requeueWorkflowJob(client, config, jobId, { available_at: availableAt, error_text: errorText, preserve_attempt: false });
    return;
  }
  await client.query(
    `
    UPDATE ${qn}
    SET
      status = 'dead_letter',
      locked_until = now(),
      last_error = $2,
      updated_at = now()
    WHERE job_id = $1
    `,
    [jobId, String(errorText || '').trim()]
  );
  await client.query(
    `
    INSERT INTO ${deadQn}
      (job_id, run_uid, tenant_id, desk_id, desk_version_id, start_node_id, process_code, job_type, dedupe_key, payload_json, attempt_no, max_attempts, error_text, failed_at)
    VALUES
      ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $12, $13, now())
    `,
    [
      jobId,
      String(job?.parent_run_uid || '').trim(),
      String(job?.tenant_id || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID,
      Math.trunc(Number(job?.desk_id || 0)),
      Math.trunc(Number(job?.desk_version_id || 0)),
      String(job?.start_node_id || '').trim(),
      String(job?.process_code || '').trim(),
      String(job?.job_type || '').trim(),
      String(job?.dedupe_key || '').trim(),
      stableJsonString(job?.payload_json || {}),
      attempts,
      maxAttempts,
      String(errorText || '').trim()
    ]
  );
}

async function emitWorkflowEvent(client, config, payload = {}) {
  const eventType = String(payload?.event_type || '').trim().toLowerCase();
  if (!eventType) return 0;
  const busId = await writeBusMessage(client, config, {
    desk_id: Math.trunc(Number(payload?.desk_id || 0)),
    run_uid: String(payload?.run_uid || '').trim(),
    process_code: String(payload?.process_code || '').trim(),
    channel: 'workflow_event',
    payload_json: {
      event_type: eventType,
      source_run_uid: String(payload?.source_run_uid || payload?.run_uid || '').trim(),
      tenant_id: String(payload?.tenant_id || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID,
      client_id: String(payload?.client_id || '').trim(),
      desk_id: Math.trunc(Number(payload?.desk_id || 0)),
      desk_version_id: Math.trunc(Number(payload?.desk_version_id || 0)),
      start_node_id: String(payload?.start_node_id || '').trim(),
      process_code: String(payload?.process_code || '').trim(),
      status: String(payload?.status || '').trim(),
      trigger_type: String(payload?.trigger_type || '').trim(),
      chunk_key: String(payload?.chunk_key || '').trim(),
      chunk_no: Math.max(1, Math.trunc(Number(payload?.chunk_no || 1))),
      all_chunks_done: Boolean(payload?.all_chunks_done),
      metrics: payload?.metrics || {},
      payload: payload?.payload || {},
      created_at: new Date().toISOString()
    }
  });
  return busId;
}

async function claimWorkflowEvents(client, config, limit = 100) {
  const qn = workflowProcessBusQname(config);
  const r = await client.query(
    `
    WITH pick AS (
      SELECT id
      FROM ${qn}
      WHERE status = 'new'
        AND channel = 'workflow_event'
      ORDER BY id ASC
      LIMIT $1
      FOR UPDATE SKIP LOCKED
    )
    UPDATE ${qn} b
    SET status = 'processing',
        consumed_by = $2,
        consumed_at = now()
    FROM pick
    WHERE b.id = pick.id
    RETURNING b.id, b.desk_id, b.run_uid, b.process_code, b.payload_json, b.created_at
    `,
    [Math.max(1, Math.min(1000, Math.trunc(Number(limit || 100)))), WORKER_ID]
  );
  return Array.isArray(r.rows) ? r.rows : [];
}

async function finishWorkflowEvent(client, config, eventId, status = 'consumed') {
  const qn = workflowProcessBusQname(config);
  await client.query(
    `
    UPDATE ${qn}
    SET status = $2,
        consumed_at = now(),
        consumed_by = $3
    WHERE id = $1
    `,
    [Math.trunc(Number(eventId || 0)), String(status || 'consumed').trim(), WORKER_ID]
  );
}

async function listDependencyRulesForEvent(client, config, eventPayload = {}) {
  const qn = workflowDependenciesQname(config);
  const sourceStartNodeId = String(eventPayload?.start_node_id || '').trim();
  const eventType = String(eventPayload?.event_type || '').trim().toLowerCase();
  if (!sourceStartNodeId || !eventType) return [];
  const r = await client.query(
    `
    SELECT
      id,
      tenant_id,
      desk_id,
      source_start_node_id,
      target_start_node_id,
      trigger_event_type,
      trigger_status,
      condition_json,
      dispatch_mode,
      dedupe_policy,
      is_enabled
    FROM ${qn}
    WHERE is_enabled = true
      AND source_start_node_id = $1
      AND trigger_event_type = $2
      AND (tenant_id = $3 OR tenant_id = '*' OR tenant_id = '')
      AND (desk_id = 0 OR desk_id = $4)
    ORDER BY id ASC
    `,
    [
      sourceStartNodeId,
      eventType,
      String(eventPayload?.tenant_id || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID,
      Math.trunc(Number(eventPayload?.desk_id || 0))
    ]
  );
  return Array.isArray(r.rows) ? r.rows : [];
}

async function isTargetProcessBusy(client, config, tenantId, deskId, targetStartNodeId) {
  const qn = workflowRunsQname(config);
  const r = await client.query(
    `
    SELECT 1
    FROM ${qn}
    WHERE tenant_id = $1
      AND desk_id = $2
      AND start_node_id = $3
      AND status IN ('queued', 'running')
    LIMIT 1
    `,
    [String(tenantId || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID, Math.trunc(Number(deskId || 0)), String(targetStartNodeId || '').trim()]
  );
  return Boolean(r.rows?.length);
}

function dependencyShouldDispatch(rule, eventPayload = {}) {
  const eventType = String(eventPayload?.event_type || '').trim().toLowerCase();
  const eventStatus = String(eventPayload?.status || '').trim().toLowerCase();
  const ruleStatus = String(rule?.trigger_status || '').trim().toLowerCase();
  if (ruleStatus && eventStatus && ruleStatus !== eventStatus) return false;

  const mode = dependencyDispatchMode(rule?.dispatch_mode);
  if (mode === 'once_after_all_chunks') {
    if (eventType !== 'process_completed') return false;
    if (!Boolean(eventPayload?.all_chunks_done)) return false;
  }
  if (mode === 'for_each_chunk') {
    if (eventType !== 'chunk_completed' && eventType !== 'chunk_failed') return false;
  }
  return true;
}

function buildDependencyDispatchDedupeKey(rule, eventPayload = {}) {
  const mode = dependencyDispatchMode(rule?.dispatch_mode);
  const dedupeMode = dependencyDedupeMode(rule?.dedupe_policy);
  const sourceRunUid = String(eventPayload?.source_run_uid || eventPayload?.run_uid || '').trim();
  const ruleId = Math.trunc(Number(rule?.id || 0));
  const targetStartNodeId = String(rule?.target_start_node_id || '').trim();
  const chunkKey = String(eventPayload?.chunk_key || '').trim();
  let key = `dep:${ruleId}:${sourceRunUid}:${targetStartNodeId}:${mode}`;
  if (mode === 'for_each_chunk') {
    key += `:${chunkKey || Math.trunc(Number(eventPayload?.chunk_no || 1))}`;
  }
  if (dedupeMode === 'deduplicate_by_payload') {
    key += `:${sha1(stableJsonString(eventPayload?.payload || {}))}`;
  }
  return key;
}

async function enqueueDependencyDispatchJobs(client, config, eventPayload = {}) {
  const rules = await listDependencyRulesForEvent(client, config, eventPayload);
  let enqueued = 0;
  for (const rule of rules) {
    if (!dependencyShouldDispatch(rule, eventPayload)) continue;

    const tenantId = String(rule?.tenant_id || eventPayload?.tenant_id || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID;
    const deskId = Math.trunc(Number(rule?.desk_id || eventPayload?.desk_id || 0));
    const targetStartNodeId = String(rule?.target_start_node_id || '').trim();
    if (!deskId || !targetStartNodeId) continue;
    const tenantPolicy = await loadTenantExecutionPolicy(client, config, tenantId);
    if (!tenantPolicy.is_enabled) continue;
    const queueDepth = await countTenantQueueDepth(client, config, tenantId);
    if (queueDepth >= tenantPolicy.max_queue_depth) continue;

    const dedupeMode = dependencyDedupeMode(rule?.dedupe_policy);
    if (dedupeMode === 'skip_if_target_running') {
      const busy = await isTargetProcessBusy(client, config, tenantId, deskId, targetStartNodeId);
      if (busy) continue;
    }

    const dedupeKey = buildDependencyDispatchDedupeKey(rule, eventPayload);
    const jobId = await enqueueWorkflowJob(client, config, {
      tenant_id: tenantId,
      client_id: String(eventPayload?.client_id || '').trim(),
      desk_id: deskId,
      desk_version_id: Math.trunc(Number(eventPayload?.desk_version_id || 0)),
      start_node_id: targetStartNodeId,
      process_code: String(eventPayload?.process_code || '').trim(),
      parent_run_uid: String(eventPayload?.source_run_uid || eventPayload?.run_uid || '').trim(),
      parent_job_id: 0,
      job_type: 'dependency_dispatch',
      payload_json: {
        dependency_rule_id: Math.trunc(Number(rule?.id || 0)),
        source_event: eventPayload,
        target_start_node_id: targetStartNodeId,
        dispatch_mode: dependencyDispatchMode(rule?.dispatch_mode),
        dedupe_policy: dedupeMode
      },
      dedupe_key: dedupeKey,
      priority: Math.max(1, JOB_DEFAULT_PRIORITY - 20),
      max_attempts: JOB_DEFAULT_MAX_ATTEMPTS
    });
    if (jobId > 0) {
      enqueued += 1;
      await emitWorkflowEvent(client, config, {
        event_type: 'chunk_enqueued',
        tenant_id: tenantId,
        desk_id: deskId,
        start_node_id: targetStartNodeId,
        process_code: `dependency:${Math.trunc(Number(rule?.id || 0))}`,
        run_uid: String(eventPayload?.source_run_uid || eventPayload?.run_uid || '').trim(),
        source_run_uid: String(eventPayload?.source_run_uid || eventPayload?.run_uid || '').trim(),
        payload: {
          dependency_rule_id: Math.trunc(Number(rule?.id || 0)),
          dispatch_job_id: jobId,
          dispatch_mode: dependencyDispatchMode(rule?.dispatch_mode),
          dedupe_policy: dedupeMode
        }
      });
    }
    if (enqueued >= 1000) break;
  }
  return enqueued;
}

async function processDependencyEvents(client, config) {
  const events = await claimWorkflowEvents(client, config, 100);
  if (!events.length) return 0;
  let processed = 0;
  for (const eventRow of events) {
    const payload = parseJsonb(eventRow?.payload_json, {});
    try {
      await enqueueDependencyDispatchJobs(client, config, payload);
      await finishWorkflowEvent(client, config, Number(eventRow?.id || 0), 'consumed');
      processed += 1;
    } catch (e) {
      await finishWorkflowEvent(client, config, Number(eventRow?.id || 0), 'new');
      schedulerState.workerLastError = String(e?.message || e || 'dependency_event_failed');
    }
  }
  return processed;
}

async function loadTenantExecutionPolicy(client, config, tenantId) {
  const qn = workflowTenantPoliciesQname(config);
  const tenant = String(tenantId || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID;
  const r = await client.query(
    `
    SELECT tenant_id, max_parallel_runs, max_parallel_jobs, max_queue_depth, default_priority, weight, is_enabled
    FROM ${qn}
    WHERE tenant_id = $1
    LIMIT 1
    `,
    [tenant]
  );
  const row = r.rows?.[0];
  if (!row) {
    return {
      tenant_id: tenant,
      max_parallel_runs: MAX_PARALLEL_RUNS,
      max_parallel_jobs: MAX_PARALLEL_RUNS * 4,
      max_queue_depth: 50_000,
      default_priority: JOB_DEFAULT_PRIORITY,
      weight: 1,
      is_enabled: true
    };
  }
  return {
    tenant_id: String(row.tenant_id || tenant).trim() || tenant,
    max_parallel_runs: Math.max(1, Math.trunc(Number(row.max_parallel_runs || MAX_PARALLEL_RUNS))),
    max_parallel_jobs: Math.max(1, Math.trunc(Number(row.max_parallel_jobs || MAX_PARALLEL_RUNS * 4))),
    max_queue_depth: Math.max(1, Math.trunc(Number(row.max_queue_depth || 50_000))),
    default_priority: Math.max(1, Math.trunc(Number(row.default_priority || JOB_DEFAULT_PRIORITY))),
    weight: Math.max(1, Math.trunc(Number(row.weight || 1))),
    is_enabled: Boolean(row.is_enabled)
  };
}

async function countTenantRunningJobs(client, config, tenantId) {
  const qn = workflowJobQueueQname(config);
  const r = await client.query(
    `
    SELECT COUNT(*)::int AS c
    FROM ${qn}
    WHERE tenant_id = $1
      AND status IN ('locked', 'running')
    `,
    [String(tenantId || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID]
  );
  return Math.max(0, Number(r.rows?.[0]?.c || 0));
}

async function countTenantQueueDepth(client, config, tenantId) {
  const qn = workflowJobQueueQname(config);
  const r = await client.query(
    `
    SELECT COUNT(*)::int AS c
    FROM ${qn}
    WHERE tenant_id = $1
      AND status IN ('queued', 'locked', 'running')
    `,
    [String(tenantId || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID]
  );
  return Math.max(0, Number(r.rows?.[0]?.c || 0));
}

async function loadRateLimitPolicy(client, config, tenantId, providerCode, endpointCode) {
  const qn = workflowRateLimitPoliciesQname(config);
  const tenant = String(tenantId || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID;
  const provider = String(providerCode || '*').trim() || '*';
  const endpoint = String(endpointCode || '*').trim() || '*';
  const r = await client.query(
    `
    SELECT tenant_id, provider_code, endpoint_code, max_rps, max_parallel_jobs, burst_limit, cooldown_ms, retry_policy_json
    FROM ${qn}
    WHERE is_enabled = true
      AND tenant_id IN ($1, '*')
      AND provider_code IN ($2, '*')
      AND endpoint_code IN ($3, '*')
    ORDER BY
      CASE WHEN tenant_id = $1 THEN 0 ELSE 1 END,
      CASE WHEN provider_code = $2 THEN 0 ELSE 1 END,
      CASE WHEN endpoint_code = $3 THEN 0 ELSE 1 END,
      id ASC
    LIMIT 1
    `,
    [tenant, provider, endpoint]
  );
  const row = r.rows?.[0];
  if (!row) {
    return {
      tenant_id: tenant,
      provider_code: provider,
      endpoint_code: endpoint,
      max_rps: 0,
      max_parallel_jobs: 0,
      burst_limit: 0,
      cooldown_ms: 0,
      retry_policy_json: {}
    };
  }
  return {
    tenant_id: String(row.tenant_id || tenant).trim() || tenant,
    provider_code: String(row.provider_code || provider).trim() || provider,
    endpoint_code: String(row.endpoint_code || endpoint).trim() || endpoint,
    max_rps: Number(row.max_rps || 0),
    max_parallel_jobs: Math.max(0, Math.trunc(Number(row.max_parallel_jobs || 0))),
    burst_limit: Math.max(0, Math.trunc(Number(row.burst_limit || 0))),
    cooldown_ms: Math.max(0, Math.trunc(Number(row.cooldown_ms || 0))),
    retry_policy_json: parseJsonb(row.retry_policy_json, {})
  };
}

function providerEndpointFromTemplate(template) {
  const urlRaw = String(template?.url || '').trim();
  let provider = String(template?.provider_code || '').trim();
  let endpoint = String(template?.endpoint_code || '').trim();
  if (urlRaw) {
    try {
      const u = new URL(urlRaw);
      if (!provider) provider = String(u.hostname || '').trim().toLowerCase();
      if (!endpoint) endpoint = String(u.pathname || '').trim().toLowerCase();
    } catch {
      // ignore invalid url
    }
  }
  if (!provider) provider = 'generic_http';
  if (!endpoint) endpoint = String(template?.name || template?.id || 'default_endpoint').trim().toLowerCase();
  return { provider_code: provider, endpoint_code: endpoint };
}

async function upsertProviderRegistry(client, config, template, providerCode, endpointCode) {
  const qn = workflowProviderRegistryQname(config);
  const urlRaw = String(template?.url || '').trim();
  let baseUrl = '';
  let endpointPath = '';
  if (urlRaw) {
    try {
      const u = new URL(urlRaw);
      baseUrl = `${u.protocol}//${u.host}`;
      endpointPath = String(u.pathname || '').trim();
    } catch {
      // ignore invalid url
    }
  }
  await client.query(
    `
    INSERT INTO ${qn}
      (provider_code, endpoint_code, base_url, endpoint_path, source_type, auth_mode, metadata_json, is_enabled, updated_at)
    VALUES
      ($1, $2, $3, $4, 'http', $5, $6::jsonb, true, now())
    ON CONFLICT (provider_code, endpoint_code) DO UPDATE
    SET
      base_url = EXCLUDED.base_url,
      endpoint_path = EXCLUDED.endpoint_path,
      auth_mode = EXCLUDED.auth_mode,
      metadata_json = EXCLUDED.metadata_json,
      updated_at = now()
    `,
    [
      String(providerCode || 'generic_http').trim(),
      String(endpointCode || 'default_endpoint').trim(),
      baseUrl,
      endpointPath,
      String(template?.auth_mode || '').trim(),
      stableJsonString({
        method: String(template?.method || '').trim(),
        source_id: Number(template?.id || 0) || undefined,
        source_name: String(template?.name || '').trim()
      })
    ]
  );
}

async function getLatestChunkLogTs(client, config, tenantId, providerCode, endpointCode) {
  const qn = workflowChunkLogsQname(config);
  const r = await client.query(
    `
    SELECT finished_at
    FROM ${qn}
    WHERE tenant_id = $1
      AND provider_code = $2
      AND endpoint_code = $3
    ORDER BY finished_at DESC
    LIMIT 1
    `,
    [String(tenantId || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID, String(providerCode || '').trim(), String(endpointCode || '').trim()]
  );
  return r.rows?.[0]?.finished_at || null;
}

async function getRunningEndpointJobs(client, config, tenantId, providerCode, endpointCode) {
  const qn = workflowJobQueueQname(config);
  const r = await client.query(
    `
    SELECT COUNT(*)::int AS c
    FROM ${qn}
    WHERE tenant_id = $1
      AND status IN ('running', 'locked')
      AND payload_json->>'provider_code' = $2
      AND payload_json->>'endpoint_code' = $3
    `,
    [String(tenantId || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID, String(providerCode || '').trim(), String(endpointCode || '').trim()]
  );
  return Math.max(0, Number(r.rows?.[0]?.c || 0));
}

async function shouldDelayByRatePolicy(client, config, tenantId, providerCode, endpointCode) {
  const policy = await loadRateLimitPolicy(client, config, tenantId, providerCode, endpointCode);
  if (!policy) return 0;
  if (policy.max_parallel_jobs > 0) {
    const running = await getRunningEndpointJobs(client, config, tenantId, providerCode, endpointCode);
    if (running >= policy.max_parallel_jobs) return Math.max(500, policy.cooldown_ms || 1000);
  }
  if (policy.cooldown_ms > 0) {
    const lastTs = await getLatestChunkLogTs(client, config, tenantId, providerCode, endpointCode);
    if (lastTs) {
      const ageMs = Date.now() - new Date(lastTs).getTime();
      if (ageMs < policy.cooldown_ms) return Math.max(100, policy.cooldown_ms - ageMs);
    }
  }
  return 0;
}

async function writeChunkLog(client, config, payload = {}) {
  const qn = workflowChunkLogsQname(config);
  await client.query(
    `
    INSERT INTO ${qn}
      (
        job_id,
        run_uid,
        tenant_id,
        desk_id,
        desk_version_id,
        start_node_id,
        process_code,
        provider_code,
        endpoint_code,
        chunk_key,
        chunk_no,
        status,
        started_at,
        finished_at,
        duration_ms,
        input_json,
        output_json,
        metrics_json,
        error_text
      )
    VALUES
      ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::timestamptz, $14::timestamptz, $15, $16::jsonb, $17::jsonb, $18::jsonb, $19)
    `,
    [
      Math.trunc(Number(payload?.job_id || 0)),
      String(payload?.run_uid || '').trim(),
      String(payload?.tenant_id || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID,
      Math.trunc(Number(payload?.desk_id || 0)),
      Math.trunc(Number(payload?.desk_version_id || 0)),
      String(payload?.start_node_id || '').trim(),
      String(payload?.process_code || '').trim(),
      String(payload?.provider_code || '').trim(),
      String(payload?.endpoint_code || '').trim(),
      String(payload?.chunk_key || '').trim(),
      Math.max(1, Math.trunc(Number(payload?.chunk_no || 1))),
      String(payload?.status || 'ok').trim(),
      payload?.started_at ? String(payload.started_at) : new Date().toISOString(),
      payload?.finished_at ? String(payload.finished_at) : new Date().toISOString(),
      Math.max(0, Math.trunc(Number(payload?.duration_ms || 0))),
      stableJsonString(payload?.input_json || {}),
      stableJsonString(payload?.output_json || {}),
      stableJsonString(payload?.metrics_json || {}),
      String(payload?.error_text || '').trim()
    ]
  );
}

async function loadRunRow(client, config, runUid) {
  const qn = workflowRunsQname(config);
  const r = await client.query(
    `
    SELECT run_uid, tenant_id, client_id, desk_id, desk_name, desk_version_id, start_node_id, process_code, run_policy, orchestration_mode, trigger_source, trigger_type, trigger_key, trigger_meta, status
    FROM ${qn}
    WHERE run_uid = $1
    LIMIT 1
    `,
    [String(runUid || '').trim()]
  );
  return r.rows?.[0] || null;
}

async function findProcessByStartNode(client, config, deskRow, startNodeId) {
  const processes = await discoverDeskProcesses(client, config, deskRow);
  return processes.find((p) => String(p?.start_node_id || '').trim() === String(startNodeId || '').trim()) || null;
}

async function enqueueProcessRunQueued(client, config, runInput = {}) {
  const deskRow = runInput?.desk_row || {};
  const process = runInput?.process || {};
  const runUid = String(runInput?.run_uid || buildRunUid('wf_run')).trim() || buildRunUid('wf_run');
  const tenantId = String(process?.tenant_id || runInput?.tenant_id || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID;
  const clientId = String(process?.client_id || runInput?.client_id || '').trim();
  const runPolicy = normalizeRunPolicy(process?.run_policy);
  const triggerType = String(runInput?.trigger_type || 'manual').trim() || 'manual';
  const triggerMeta = runInput?.trigger_meta && typeof runInput.trigger_meta === 'object' ? runInput.trigger_meta : {};
  let lockAcquired = false;

  if (runPolicy === 'single_instance') {
    const busy = await isTargetProcessBusy(client, config, tenantId, Number(deskRow?.desk_id || 0), String(process?.start_node_id || ''));
    if (busy) {
      return { accepted: false, run_uid: '', reason: 'single_instance_busy' };
    }
    lockAcquired = await acquireProcessLock(client, config, {
      desk_id: Number(deskRow?.desk_id || 0),
      start_node_id: String(process?.start_node_id || ''),
      lock_key: String(process?.process_key || `${deskRow?.desk_id}:${process?.start_node_id}`).trim(),
      owner_id: runUid,
      ttl_ms: PROCESS_LOCK_TTL_MS
    });
    if (!lockAcquired) {
      return { accepted: false, run_uid: '', reason: 'single_instance_lock_conflict' };
    }
  }

  const tenantPolicy = await loadTenantExecutionPolicy(client, config, tenantId);
  if (!tenantPolicy.is_enabled) {
    return { accepted: false, run_uid: '', reason: 'tenant_disabled' };
  }
  const queueDepth = await countTenantQueueDepth(client, config, tenantId);
  if (queueDepth >= tenantPolicy.max_queue_depth) {
    return { accepted: false, run_uid: '', reason: 'tenant_queue_depth_limit' };
  }

  await insertProcessRunRow(client, config, {
    run_uid: runUid,
    tenant_id: tenantId,
    client_id: clientId,
    desk_id: Number(deskRow?.desk_id || 0),
    desk_name: String(deskRow?.desk_name || ''),
    desk_version_id: Number(deskRow?.desk_version_id || 0),
    start_node_id: String(process?.start_node_id || ''),
    process_code: String(process?.process_code || ''),
    run_policy: runPolicy,
    orchestration_mode: 'queue_chunks',
    trigger_source: String(runInput?.trigger_source || 'manual'),
    trigger_type: triggerType,
    trigger_key: String(runInput?.trigger_key || ''),
    trigger_meta: triggerMeta,
    status: 'queued',
    created_by: String(runInput?.created_by || 'scheduler')
  });

  await upsertRunAggregationRow(client, config, {
    run_uid: runUid,
    tenant_id: tenantId,
    desk_id: Number(deskRow?.desk_id || 0),
    desk_version_id: Number(deskRow?.desk_version_id || 0),
    start_node_id: String(process?.start_node_id || ''),
    process_code: String(process?.process_code || '')
  });

  const dedupeKey = `run_plan:${runUid}`;
  const planJobId = await enqueueWorkflowJob(client, config, {
    tenant_id: tenantId,
    client_id: clientId,
    desk_id: Number(deskRow?.desk_id || 0),
    desk_version_id: Number(deskRow?.desk_version_id || 0),
    start_node_id: String(process?.start_node_id || ''),
    process_code: String(process?.process_code || ''),
    parent_run_uid: runUid,
    parent_job_id: 0,
    job_type: 'process_plan',
    payload_json: {
      desk_row: deskRow,
      process,
      run_uid: runUid,
      trigger_type: triggerType
    },
    dedupe_key: dedupeKey,
    priority: Math.max(1, Math.trunc(Number(tenantPolicy.default_priority || JOB_DEFAULT_PRIORITY))),
    max_attempts: JOB_DEFAULT_MAX_ATTEMPTS
  });

  if (!planJobId) {
    await updateRunRow(client, config, runUid, {
      status: 'failed',
      finished_at: new Date().toISOString(),
      duration_ms: 0,
      summary_json: { error: 'plan_job_deduped_or_not_created' },
      error_text: 'plan_job_not_created'
    });
    if (lockAcquired) {
      await releaseProcessLock(client, config, {
        desk_id: Number(deskRow?.desk_id || 0),
        start_node_id: String(process?.start_node_id || ''),
        owner_id: runUid
      });
    }
    return { accepted: false, run_uid: runUid, reason: 'plan_job_not_created' };
  }

  await emitWorkflowEvent(client, config, {
    event_type: 'process_started',
    tenant_id: tenantId,
    client_id: clientId,
    desk_id: Number(deskRow?.desk_id || 0),
    desk_version_id: Number(deskRow?.desk_version_id || 0),
    start_node_id: String(process?.start_node_id || ''),
    process_code: String(process?.process_code || ''),
    run_uid: runUid,
    trigger_type: triggerType,
    payload: {
      mode: 'queued',
      plan_job_id: planJobId
    }
  });
  await recomputeRunAggregation(client, config, runUid);
  return { accepted: true, run_uid: runUid, plan_job_id: planJobId };
}

async function finalizeRunAfterQueue(client, config, runUid, status, errorText = '') {
  const run = await loadRunRow(client, config, runUid);
  if (!run) return;
  if (isTerminalRunStatus(run.status)) return;
  const agg = await recomputeRunAggregation(client, config, runUid);
  await updateRunRow(client, config, runUid, {
    status: String(status || 'completed').trim(),
    finished_at: new Date().toISOString(),
    duration_ms: 0,
    summary_json: agg || {},
    error_text: String(errorText || '').trim()
  });
  if (run.run_policy === 'single_instance') {
    await releaseProcessLock(client, config, {
      desk_id: Number(run.desk_id || 0),
      start_node_id: String(run.start_node_id || ''),
      owner_id: runUid
    });
  }
  await emitWorkflowEvent(client, config, {
    event_type: status === 'completed' ? 'process_completed' : 'process_failed',
    tenant_id: String(run.tenant_id || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID,
    client_id: String(run.client_id || '').trim(),
    desk_id: Number(run.desk_id || 0),
    desk_version_id: Number(run.desk_version_id || 0),
    start_node_id: String(run.start_node_id || ''),
    process_code: String(run.process_code || ''),
    run_uid: String(run.run_uid || ''),
    source_run_uid: String(run.run_uid || ''),
    status: status,
    trigger_type: String(run.trigger_type || ''),
    all_chunks_done: true,
    payload: {
      error_text: String(errorText || '').trim(),
      aggregation: agg || {}
    }
  });
}

async function handleDependencyDispatchJob(client, config, job, payload = {}) {
  const sourceEvent = payload?.source_event && typeof payload.source_event === 'object' ? payload.source_event : {};
  const deskId = Math.trunc(Number(job?.desk_id || sourceEvent?.desk_id || 0));
  const targetStartNodeId = String(payload?.target_start_node_id || job?.start_node_id || '').trim();
  if (!deskId || !targetStartNodeId) return { skipped: true, reason: 'invalid_dependency_target' };

  const desk = await loadPublishedDeskById(client, config, deskId);
  if (!desk) return { skipped: true, reason: 'published_desk_not_found' };
  const process = await findProcessByStartNode(client, config, desk, targetStartNodeId);
  if (!process) return { skipped: true, reason: 'target_process_not_found' };

  const ruleId = Math.trunc(Number(payload?.dependency_rule_id || 0));
  const triggerMeta = {
    source_run_uid: String(sourceEvent?.source_run_uid || sourceEvent?.run_uid || '').trim(),
    source_start_node_id: String(sourceEvent?.start_node_id || '').trim(),
    source_event_type: String(sourceEvent?.event_type || '').trim(),
    source_event_payload: sourceEvent?.payload || {},
    dependency_rule_id: ruleId,
    dispatch_mode: String(payload?.dispatch_mode || '').trim(),
    dedupe_policy: String(payload?.dedupe_policy || '').trim()
  };
  const queued = await enqueueProcessRunQueued(client, config, {
    desk_row: desk,
    process,
    trigger_source: 'dependency_event',
    trigger_type: 'dependency',
    trigger_key: `dependency:${ruleId}:${triggerMeta.source_run_uid}`,
    trigger_meta: triggerMeta,
    created_by: 'dependency_engine',
    tenant_id: String(job?.tenant_id || sourceEvent?.tenant_id || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID,
    client_id: String(job?.client_id || sourceEvent?.client_id || '').trim()
  });
  return queued.accepted
    ? { queued: true, run_uid: queued.run_uid, source_run_uid: triggerMeta.source_run_uid }
    : { skipped: true, reason: queued.reason || 'not_enqueued' };
}

async function handleProcessPlanJob(client, config, job, payload = {}) {
  const runUid = String(payload?.run_uid || job?.parent_run_uid || '').trim();
  const process = payload?.process && typeof payload.process === 'object' ? payload.process : {};
  const subgraph = process?.subgraph && typeof process.subgraph === 'object' ? process.subgraph : {};
  const order = Array.isArray(subgraph?.order) ? subgraph.order : [];
  if (!runUid || !order.length) {
    throw new Error('process_plan_invalid_payload');
  }
  const firstNode = order[0];
  const nodeJobId = await enqueueWorkflowJob(client, config, {
    tenant_id: String(job?.tenant_id || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID,
    client_id: String(job?.client_id || '').trim(),
    desk_id: Number(job?.desk_id || 0),
    desk_version_id: Number(job?.desk_version_id || 0),
    start_node_id: String(job?.start_node_id || process?.start_node_id || '').trim(),
    process_code: String(job?.process_code || process?.process_code || '').trim(),
    parent_run_uid: runUid,
    parent_job_id: Math.trunc(Number(job?.job_id || 0)),
    job_type: 'process_node',
    payload_json: {
      run_uid: runUid,
      desk_row: payload?.desk_row || {},
      process,
      node_index: 0,
      node: firstNode,
      input_value: {}
    },
    dedupe_key: `run_node:${runUid}:0`,
    priority: Math.max(1, Math.trunc(Number(job?.priority || JOB_DEFAULT_PRIORITY))),
    max_attempts: JOB_DEFAULT_MAX_ATTEMPTS
  });
  if (!nodeJobId) throw new Error('first_node_job_not_created');
  const runQn = workflowRunsQname(config);
  await client.query(`UPDATE ${runQn} SET status = 'running', updated_at = now() WHERE run_uid = $1`, [runUid]);
  await emitWorkflowEvent(client, config, {
    event_type: 'chunk_enqueued',
    tenant_id: String(job?.tenant_id || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID,
    client_id: String(job?.client_id || '').trim(),
    desk_id: Number(job?.desk_id || 0),
    desk_version_id: Number(job?.desk_version_id || 0),
    start_node_id: String(job?.start_node_id || process?.start_node_id || ''),
    process_code: String(job?.process_code || process?.process_code || ''),
    run_uid: runUid,
    source_run_uid: runUid,
    chunk_key: `node_0`,
    chunk_no: 1,
    payload: { job_id: nodeJobId, node_id: String(firstNode?.id || '') }
  });
  return { enqueued_node_job_id: nodeJobId };
}

async function handleProcessNodeJob(client, config, job, payload = {}) {
  const runUid = String(payload?.run_uid || job?.parent_run_uid || '').trim();
  const process = payload?.process && typeof payload.process === 'object' ? payload.process : {};
  const subgraph = process?.subgraph && typeof process.subgraph === 'object' ? process.subgraph : {};
  const order = Array.isArray(subgraph?.order) ? subgraph.order : [];
  const nodeIndex = Math.max(0, Math.trunc(Number(payload?.node_index || 0)));
  const node = order[nodeIndex] || payload?.node;
  if (!runUid || !node) throw new Error('process_node_invalid_payload');

  const processCtx = {
    run_uid: runUid,
    desk_id: Math.trunc(Number(job?.desk_id || payload?.desk_row?.desk_id || 0)),
    desk_version_id: Math.trunc(Number(job?.desk_version_id || payload?.desk_row?.desk_version_id || 0)),
    start_node_id: String(job?.start_node_id || process?.start_node_id || '').trim(),
    process_code: String(job?.process_code || process?.process_code || '').trim(),
    tenant_id: String(job?.tenant_id || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID,
    client_id: String(job?.client_id || '').trim()
  };
  const templates = await loadActiveApiConfigs(client, config);
  const stepStarted = Date.now();
  let status = 'ok';
  let output = {};
  let metrics = {};
  let reqPayload = {};
  let respPayload = {};
  let errorText = '';
  let providerCode = '';
  let endpointCode = '';
  try {
    if (isApiNode(node) || isApiToolNode(node)) {
      const refs = nodeTemplateRefs(node);
      const template = templates.find((tpl) => refs.some((ref) => sourceMatchesTemplateRef(tpl, ref)));
      if (template) {
        const pe = providerEndpointFromTemplate(template);
        providerCode = pe.provider_code;
        endpointCode = pe.endpoint_code;
        await upsertProviderRegistry(client, config, template, providerCode, endpointCode);
        const delay = await shouldDelayByRatePolicy(client, config, processCtx.tenant_id, providerCode, endpointCode);
        if (delay > 0) {
          const retryAt = new Date(Date.now() + delay).toISOString();
          await requeueWorkflowJob(client, config, Number(job?.job_id || 0), {
            available_at: retryAt,
            error_text: `rate_limit_delay_${delay}ms`,
            preserve_attempt: true
          });
          return { deferred: true, delay_ms: delay };
        }
      }
    }
    const exec = await executeProcessNode(client, config, processCtx, node, templates, payload?.input_value ?? {});
    output = exec.output ?? {};
    metrics = exec.metrics || {};
    reqPayload = exec.request_payload || {};
    respPayload = exec.response_payload || {};
    if (Number(metrics?.failed || 0) > 0) status = 'warn';
  } catch (e) {
    status = 'error';
    errorText = String(e?.message || e || 'process_node_failed');
    output = { error: errorText };
    metrics = { failed: true };
    respPayload = { error: errorText };
  }
  const durationMs = Date.now() - stepStarted;
  const stepOrder = nodeIndex + 1;
  await insertProcessStepRow(client, config, {
    run_uid: runUid,
    step_order: stepOrder,
    node_id: String(node?.id || ''),
    node_name: String(node?.config?.name || node?.id || '').trim(),
    node_type: String(node?.config?.toolType || node?.type || '').trim(),
    status,
    started_at: new Date(stepStarted).toISOString(),
    finished_at: new Date().toISOString(),
    duration_ms: durationMs,
    input_json: reqPayload,
    output_json: output,
    request_payload: reqPayload,
    response_payload: respPayload,
    metrics_json: metrics,
    error_text: errorText
  });

  await writeChunkLog(client, config, {
    job_id: Number(job?.job_id || 0),
    run_uid: runUid,
    tenant_id: processCtx.tenant_id,
    desk_id: processCtx.desk_id,
    desk_version_id: processCtx.desk_version_id,
    start_node_id: processCtx.start_node_id,
    process_code: processCtx.process_code,
    provider_code: providerCode,
    endpoint_code: endpointCode,
    chunk_key: `node_${nodeIndex}`,
    chunk_no: stepOrder,
    status: status === 'error' ? 'failed' : 'ok',
    started_at: new Date(stepStarted).toISOString(),
    finished_at: new Date().toISOString(),
    duration_ms: durationMs,
    input_json: reqPayload,
    output_json: output,
    metrics_json: metrics,
    error_text: errorText
  });

  await emitWorkflowEvent(client, config, {
    event_type: status === 'error' ? 'chunk_failed' : 'chunk_completed',
    tenant_id: processCtx.tenant_id,
    client_id: processCtx.client_id,
    desk_id: processCtx.desk_id,
    desk_version_id: processCtx.desk_version_id,
    start_node_id: processCtx.start_node_id,
    process_code: processCtx.process_code,
    run_uid: runUid,
    source_run_uid: runUid,
    status: status,
    chunk_key: `node_${nodeIndex}`,
    chunk_no: stepOrder,
    payload: {
      node_id: String(node?.id || ''),
      node_name: String(node?.config?.name || node?.id || ''),
      node_type: String(node?.config?.toolType || node?.type || ''),
      output
    },
    metrics
  });

  if (status === 'error') {
    await finalizeRunAfterQueue(client, config, runUid, 'failed', errorText || 'process_node_failed');
    return { failed: true, error_text: errorText };
  }

  if (nodeIndex >= order.length - 1 || String(node?.config?.toolType || '').trim().toLowerCase() === 'end_process') {
    await finalizeRunAfterQueue(client, config, runUid, 'completed', '');
    return { completed: true };
  }

  const nextIndex = nodeIndex + 1;
  const nextNode = order[nextIndex];
  const nextJobId = await enqueueWorkflowJob(client, config, {
    tenant_id: processCtx.tenant_id,
    client_id: processCtx.client_id,
    desk_id: processCtx.desk_id,
    desk_version_id: processCtx.desk_version_id,
    start_node_id: processCtx.start_node_id,
    process_code: processCtx.process_code,
    parent_run_uid: runUid,
    parent_job_id: Math.trunc(Number(job?.job_id || 0)),
    job_type: 'process_node',
    payload_json: {
      run_uid: runUid,
      desk_row: payload?.desk_row || {},
      process,
      node_index: nextIndex,
      node: nextNode,
      input_value: output
    },
    dedupe_key: `run_node:${runUid}:${nextIndex}`,
    priority: Math.max(1, Math.trunc(Number(job?.priority || JOB_DEFAULT_PRIORITY))),
    max_attempts: JOB_DEFAULT_MAX_ATTEMPTS
  });
  if (!nextJobId) {
    throw new Error(`next_node_job_not_created:${nextIndex}`);
  }
  await emitWorkflowEvent(client, config, {
    event_type: 'chunk_enqueued',
    tenant_id: processCtx.tenant_id,
    client_id: processCtx.client_id,
    desk_id: processCtx.desk_id,
    desk_version_id: processCtx.desk_version_id,
    start_node_id: processCtx.start_node_id,
    process_code: processCtx.process_code,
    run_uid: runUid,
    source_run_uid: runUid,
    chunk_key: `node_${nextIndex}`,
    chunk_no: nextIndex + 1,
    payload: { job_id: nextJobId, node_id: String(nextNode?.id || '') }
  });

  const cursorCandidate = getByPath({ response: output }, 'response.cursor');
  if (cursorCandidate !== undefined) {
    const incQn = workflowIncrementalStateQname(config);
    await client.query(
      `
      INSERT INTO ${incQn}
        (tenant_id, desk_id, desk_version_id, start_node_id, process_code, state_key, cursor_json, last_successful_sync_at, last_attempt_at, updated_at)
      VALUES
        ($1, $2, $3, $4, $5, 'default', $6::jsonb, now(), now(), now())
      ON CONFLICT (tenant_id, desk_id, start_node_id, process_code, state_key) DO UPDATE
      SET cursor_json = EXCLUDED.cursor_json,
          last_successful_sync_at = EXCLUDED.last_successful_sync_at,
          last_attempt_at = EXCLUDED.last_attempt_at,
          updated_at = now()
      `,
      [processCtx.tenant_id, processCtx.desk_id, processCtx.desk_version_id, processCtx.start_node_id, processCtx.process_code, stableJsonString(cursorCandidate)]
    );
  }
  return { next_job_id: nextJobId };
}

async function executeWorkflowJobByType(client, config, job) {
  const payload = parseJsonb(job?.payload_json, {});
  const type = String(job?.job_type || '').trim().toLowerCase();
  if (type === 'process_plan') return handleProcessPlanJob(client, config, job, payload);
  if (type === 'process_node') return handleProcessNodeJob(client, config, job, payload);
  if (type === 'dependency_dispatch') return handleDependencyDispatchJob(client, config, job, payload);
  return { skipped: true, reason: `unsupported_job_type:${type}` };
}

async function processQueuedJobsTick(client, config) {
  const claimed = await claimWorkflowJobs(client, config, JOB_CLAIM_BATCH);
  schedulerState.workerClaimedJobs = claimed.length;
  let processed = 0;
  for (const job of claimed) {
    const tenantId = String(job?.tenant_id || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID;
    const tenantPolicy = await loadTenantExecutionPolicy(client, config, tenantId);
    const runningTenantJobs = await countTenantRunningJobs(client, config, tenantId);
    if (runningTenantJobs > tenantPolicy.max_parallel_jobs) {
      await requeueWorkflowJob(client, config, Number(job?.job_id || 0), {
        available_at: new Date(Date.now() + 1500).toISOString(),
        error_text: 'tenant_parallel_limit',
        preserve_attempt: true
      });
      continue;
    }
    try {
      const out = await executeWorkflowJobByType(client, config, job);
      if (!out?.deferred) {
        await completeWorkflowJob(client, config, Number(job?.job_id || 0), out || {});
      }
    } catch (e) {
      const err = String(e?.message || e || 'job_failed');
      await failWorkflowJob(client, config, job, err);
    } finally {
      await recomputeRunAggregation(client, config, String(job?.parent_run_uid || '').trim());
    }
    processed += 1;
  }
  schedulerState.workerProcessedJobs = processed;
  return processed;
}

async function waitRunsToFinish(client, config, runUids, timeoutMs = 120_000) {
  const runIds = Array.isArray(runUids) ? runUids.map((x) => String(x || '').trim()).filter(Boolean) : [];
  if (!runIds.length) return [];
  const qn = workflowRunsQname(config);
  const started = Date.now();
  while (Date.now() - started < Math.max(1_000, Number(timeoutMs || 120_000))) {
    const r = await client.query(
      `
      SELECT run_uid, status, summary_json, error_text, started_at, finished_at, trigger_type, trigger_meta
      FROM ${qn}
      WHERE run_uid = ANY($1::text[])
      `,
      [runIds]
    );
    const rows = Array.isArray(r.rows) ? r.rows : [];
    if (rows.length && rows.every((x) => isTerminalRunStatus(x?.status))) return rows;
    await delayMs(500);
  }
  const r = await client.query(
    `
    SELECT run_uid, status, summary_json, error_text, started_at, finished_at, trigger_type, trigger_meta
    FROM ${qn}
    WHERE run_uid = ANY($1::text[])
    `,
    [runIds]
  );
  return Array.isArray(r.rows) ? r.rows : [];
}

function normalizeStopRuleOperator(raw) {
  const op = String(raw || '').trim().toLowerCase();
  if (
    op === 'equals' ||
    op === 'not_equals' ||
    op === 'gt' ||
    op === 'gte' ||
    op === 'lt' ||
    op === 'lte' ||
    op === 'contains' ||
    op === 'not_contains' ||
    op === 'is_empty' ||
    op === 'not_empty'
  ) {
    return op;
  }
  if (op === '=' || op === '==') return 'equals';
  if (op === '!=' || op === '<>') return 'not_equals';
  if (op === '>') return 'gt';
  if (op === '>=') return 'gte';
  if (op === '<') return 'lt';
  if (op === '<=') return 'lte';
  if (op === 'empty') return 'is_empty';
  if (op === 'exists') return 'not_empty';
  return 'equals';
}

function parsePaginationStopRules(raw) {
  const src = Array.isArray(raw) ? raw : [];
  return src
    .map((rule, idx) => ({
      id: String(rule?.id || `stop_${idx + 1}`),
      responsePath: String(rule?.response_path || rule?.responsePath || rule?.path || '').trim(),
      operator: normalizeStopRuleOperator(rule?.operator),
      compareValue: String(rule?.compare_value || rule?.compareValue || rule?.value || '').trim()
    }))
    .filter((rule) => Boolean(rule.responsePath));
}

function paginationModeFromTemplateRaw(rawRow) {
  const pagination = tryObject(rawRow?.pagination_json);
  const strategy = String(rawRow?.pagination_strategy || pagination?.strategy || 'none').trim().toLowerCase();
  if (strategy === 'page_number') return 'page_number';
  if (strategy === 'offset_limit') return 'offset_limit';
  if (strategy === 'cursor_fields' || strategy === 'cursor') return 'cursor';
  return 'none';
}

function paginationFromTemplateRaw(rawRow) {
  const pagination = tryObject(rawRow?.pagination_json);
  const stopConditions = tryObject(pagination?.stop_conditions);
  const enabledRaw = rawRow?.pagination_enabled;
  const enabled = enabledRaw === undefined || enabledRaw === null ? Boolean(pagination?.enabled) : Boolean(enabledRaw);
  const mode = paginationModeFromTemplateRaw(rawRow);
  const targetRaw = String(rawRow?.pagination_target || pagination?.target || 'query').trim().toLowerCase();
  const target = targetRaw === 'body' ? 'body' : 'query';
  const maxPages = Number.isFinite(Number(rawRow?.pagination_max_pages))
    ? Number(rawRow?.pagination_max_pages)
    : Number.isFinite(Number(pagination?.max_pages))
    ? Number(pagination?.max_pages)
    : 10;
  const pauseMs = Number.isFinite(Number(rawRow?.pagination_delay_ms))
    ? Number(rawRow?.pagination_delay_ms)
    : Number.isFinite(Number(pagination?.delay_ms))
    ? Number(pagination?.delay_ms)
    : 0;
  const useMaxPagesRaw = rawRow?.pagination_use_max_pages ?? pagination?.use_max_pages ?? pagination?.max_pages_enabled;
  const useDelayRaw = rawRow?.pagination_use_delay ?? pagination?.use_delay ?? pagination?.delay_enabled;
  const stopOnMissingRaw = rawRow?.pagination_stop_on_missing_value ?? stopConditions?.on_missing_pagination_value;
  const stopOnHttpErrorRaw = rawRow?.pagination_stop_on_http_error ?? stopConditions?.on_http_error;
  const stopOnSameRaw = rawRow?.pagination_stop_on_same_response ?? stopConditions?.on_same_response;
  const sameResponseLimitRaw = rawRow?.pagination_same_response_limit ?? stopConditions?.same_response_limit;
  return {
    enabled: Boolean(enabled),
    mode,
    target,
    useMaxPages: useMaxPagesRaw === undefined || useMaxPagesRaw === null ? true : Boolean(useMaxPagesRaw),
    maxPages: Math.max(1, Number(maxPages || 1)),
    useDelay: useDelayRaw === undefined || useDelayRaw === null ? Math.max(0, Number(pauseMs || 0)) > 0 : Boolean(useDelayRaw),
    pauseMs: Math.max(0, Number(pauseMs || 0)),
    dataPath: String(rawRow?.pagination_data_path || pagination?.data_path || '').trim(),
    stopOnMissingValue: stopOnMissingRaw === undefined || stopOnMissingRaw === null ? true : Boolean(stopOnMissingRaw),
    stopOnHttpError: stopOnHttpErrorRaw === undefined || stopOnHttpErrorRaw === null ? true : Boolean(stopOnHttpErrorRaw),
    stopOnSameResponse: stopOnSameRaw === undefined || stopOnSameRaw === null ? true : Boolean(stopOnSameRaw),
    sameResponseLimit: Math.max(2, Math.min(50, Number(sameResponseLimitRaw || 5))),
    stopRules: parsePaginationStopRules(stopConditions?.response_rules || stopConditions?.rules || []),
    pageParam: String(rawRow?.pagination_page_param || pagination?.page_param || 'page').trim() || 'page',
    startPage: Math.max(1, Number(rawRow?.pagination_start_page ?? pagination?.start_page ?? 1)),
    limitParam: String(rawRow?.pagination_limit_param || pagination?.limit_param || 'limit').trim() || 'limit',
    limitValue: Math.max(1, Number(rawRow?.pagination_limit_value ?? pagination?.limit_value ?? 100)),
    offsetParam: String(rawRow?.pagination_offset_param || pagination?.offset_param || 'offset').trim() || 'offset',
    startOffset: Math.max(0, Number(rawRow?.pagination_start_offset ?? pagination?.start_offset ?? 0)),
    cursorReqPath: String(rawRow?.pagination_cursor_req_path_1 || pagination?.cursor_req_path_1 || 'cursor').trim() || 'cursor',
    cursorResPath:
      String(rawRow?.pagination_cursor_res_path_1 || pagination?.cursor_res_path_1 || 'response.cursor').trim() ||
      'response.cursor'
  };
}

function parseScalarValue(raw) {
  if (raw === null || raw === undefined) return raw;
  if (typeof raw !== 'string') return raw;
  const txt = String(raw || '').trim();
  if (!txt) return '';
  if (txt.toLowerCase() === 'null') return null;
  if (txt.toLowerCase() === 'true') return true;
  if (txt.toLowerCase() === 'false') return false;
  if (/^-?\d+(\.\d+)?$/.test(txt)) return Number(txt);
  if ((txt.startsWith('{') && txt.endsWith('}')) || (txt.startsWith('[') && txt.endsWith(']'))) {
    try {
      return JSON.parse(txt);
    } catch {
      return txt;
    }
  }
  return txt;
}

function normalizeTemplateParameterDefinitions(row) {
  const mapping = tryObject(row?.mapping_json);
  const definitionsRaw = Array.isArray(row?.parameter_definitions)
    ? row.parameter_definitions
    : Array.isArray(mapping?.parameter_definitions)
    ? mapping.parameter_definitions
    : [];
  return definitionsRaw
    .map((pd) => ({
      alias: String(pd?.alias || '').trim(),
      definition: String(pd?.definition || '').trim(),
      sourceSchema: String(pd?.source_schema || pd?.sourceSchema || '').trim(),
      sourceTable: String(pd?.source_table || pd?.sourceTable || '').trim(),
      sourceField: String(pd?.source_field || pd?.sourceField || '').trim(),
      conditions: Array.isArray(pd?.conditions)
        ? pd.conditions.map((cond) => ({
            id: String(cond?.id || '').trim(),
            schema: String(cond?.schema || '').trim(),
            table: String(cond?.table || '').trim(),
            field: String(cond?.field || '').trim(),
            operator: String(cond?.operator || '').trim(),
            compareMode: String(cond?.compare_mode || cond?.compareMode || 'value').trim(),
            compareValue: String(cond?.compare_value || cond?.compareValue || '').trim(),
            compareColumn: String(cond?.compare_column || cond?.compareColumn || '').trim()
          }))
        : []
    }))
    .filter((pd) => Boolean(pd.alias));
}

function normalizeTemplatePaginationParameters(row) {
  const pagination = tryObject(row?.pagination_json);
  const mapping = tryObject(row?.mapping_json);
  const raw = Array.isArray(pagination?.parameters)
    ? pagination.parameters
    : Array.isArray(mapping?.pagination_parameters)
    ? mapping.pagination_parameters
    : [];
  return raw
    .map((p) => ({
      alias: String(p?.alias || '').trim(),
      firstValue: parseScalarValue(p?.first_value ?? p?.firstValue ?? '')
    }))
    .filter((p) => Boolean(p.alias));
}

function normalizeTemplateDataModel(row) {
  const execution = tryObject(row?.execution_json);
  const mapping = tryObject(row?.mapping_json);
  const source = tryObject(execution?.data_model || mapping?.data_model || row?.data_model_json);
  const tables = (Array.isArray(source?.tables) ? source.tables : [])
    .map((t, idx) => ({
      id: String(t?.id || `t${idx}`).trim(),
      schema: String(t?.schema || '').trim(),
      table: String(t?.table || '').trim(),
      alias: String(t?.alias || t?.table || `table_${idx + 1}`).trim()
    }))
    .filter((t) => Boolean(t.id && isIdent(t.schema) && isIdent(t.table)));
  const tableIds = new Set(tables.map((t) => t.id));
  const joins = (Array.isArray(source?.joins) ? source.joins : [])
    .map((j, idx) => ({
      id: String(j?.id || `j${idx}`).trim(),
      left_table_id: String(j?.left_table_id || j?.leftTableId || '').trim(),
      left_field: String(j?.left_field || j?.leftField || '').trim(),
      right_table_id: String(j?.right_table_id || j?.rightTableId || '').trim(),
      right_field: String(j?.right_field || j?.rightField || '').trim(),
      join_type: String(j?.join_type || j?.joinType || 'inner').toLowerCase() === 'left' ? 'left' : 'inner'
    }))
    .filter(
      (j) =>
        Boolean(j.left_table_id && j.right_table_id && j.left_field && j.right_field) &&
        tableIds.has(j.left_table_id) &&
        tableIds.has(j.right_table_id)
    );
  const fields = (Array.isArray(source?.fields) ? source.fields : [])
    .map((f, idx) => ({
      id: String(f?.id || `f${idx}`).trim(),
      table_id: String(f?.table_id || f?.tableId || '').trim(),
      field: String(f?.field || '').trim(),
      alias: String(f?.alias || f?.field || `field_${idx + 1}`).trim()
    }))
    .filter((f) => Boolean(f.table_id && f.field && f.alias && tableIds.has(f.table_id)));
  const filters = (Array.isArray(source?.filters) ? source.filters : [])
    .map((f, idx) => ({
      id: String(f?.id || `flt_${idx}`).trim(),
      table_id: String(f?.table_id || f?.tableId || '').trim(),
      field: String(f?.field || '').trim(),
      operator: String(f?.operator || '').trim(),
      compare_value: String(f?.compare_value || f?.compareValue || '').trim()
    }))
    .filter((f) => Boolean(f.table_id && f.field && f.operator && tableIds.has(f.table_id)));
  return { tables, joins, fields, filters };
}

function extractPositiveIntFromObject(obj, candidateKeys = []) {
  if (!obj || typeof obj !== 'object') return 0;
  const lowered = new Map();
  Object.keys(obj).forEach((k) => lowered.set(String(k || '').toLowerCase(), obj[k]));
  for (const key of candidateKeys) {
    const raw = lowered.get(String(key || '').toLowerCase());
    const n = Number(String(raw ?? '').trim());
    if (Number.isFinite(n) && n > 0) return Math.trunc(n);
  }
  return 0;
}

function materializeApiConfigRow(row) {
  const cfg = tryObject(row?.config_json);
  const hasCfg = Object.keys(cfg).length > 0;
  const src = hasCfg ? cfg : row || {};
  const id =
    extractPositiveIntFromObject(row, ['id', 'api_config_id', 'config_id', 'api_id', 'store_id']) ||
    extractPositiveIntFromObject(src, ['id', 'api_config_id', 'config_id', 'api_id', 'store_id']) ||
    0;
  const normalized = {
    ...src,
    id,
    api_name: String(row?.api_name || src?.api_name || '').trim(),
    description: String(row?.description ?? src?.description ?? '').trim(),
    is_active: row?.is_active === undefined ? Boolean(src?.is_active ?? true) : Boolean(row?.is_active),
    updated_at: row?.updated_at || src?.updated_at || null,
    updated_by: String(row?.updated_by || src?.updated_by || '').trim(),
    schema_version: Number(row?.schema_version || src?.schema_version || 1) || 1,
    revision: Number(row?.revision || src?.revision || 1) || 1,
    config_json: hasCfg ? cfg : {}
  };
  return {
    ...normalized,
    parameterDefinitions: normalizeTemplateParameterDefinitions(normalized),
    paginationParameters: normalizeTemplatePaginationParameters(normalized),
    dataModel: normalizeTemplateDataModel(normalized)
  };
}

async function loadActiveApiConfigs(client, config) {
  const qn = apiConfigsQname(config);
  const r = await client.query(
    `
    SELECT *
    FROM ${qn}
    WHERE is_active = true
    ORDER BY updated_at DESC, id DESC
    `
  );
  const rows = Array.isArray(r.rows) ? r.rows : [];
  return rows.map(materializeApiConfigRow);
}

function sourceMatchesTemplateRef(src, refRaw) {
  const ref = String(refRaw || '').trim();
  if (!ref) return false;
  const lower = ref.toLowerCase();
  const storeId = Number(src?.id || 0);
  const storeIdStr = storeId > 0 ? String(storeId) : '';
  if (lower === storeIdStr.toLowerCase()) return true;
  if (storeIdStr && lower === `api_template:${storeIdStr}`.toLowerCase()) return true;
  if (storeIdStr && lower === `api_tpl_${storeIdStr}`.toLowerCase()) return true;
  if (lower === String(src?.datasetId || '').toLowerCase()) return true;
  if (lower === String(src?.templateId || '').toLowerCase()) return true;
  return false;
}

function nodeTemplateRefs(node) {
  if (!node || typeof node !== 'object') return [];
  const refs = [];
  if (node.type === 'data' && String(node?.config?.group || '').trim() === 'api_requests') {
    refs.push(String(node?.config?.templateId || '').trim());
    refs.push(String(node?.config?.templateStoreId || '').trim());
    refs.push(String(node?.config?.id || '').trim());
    refs.push(String(node?.config?.datasetId || '').trim());
  } else if (node.type === 'tool' && String(node?.config?.toolType || '').trim() === 'api_request') {
    const settings = node?.config?.settings && typeof node.config.settings === 'object' ? node.config.settings : {};
    refs.push(String(settings?.templateId || '').trim());
    refs.push(String(settings?.templateStoreId || '').trim());
  }
  return uniqueAliasList(refs.filter(Boolean));
}

function parseParameterSourceRef(raw, param) {
  const src = String(raw || '').trim().replace(/^['"]|['"]$/g, '');
  if (!src) return null;
  const parts = src
    .split('.')
    .map((x) => String(x || '').trim())
    .filter(Boolean);
  if (parts.length >= 3) {
    const [schema, table, ...fieldParts] = parts;
    const field = fieldParts.join('.');
    return schema && table && field ? { schema, table, field } : null;
  }
  if (parts.length === 2) {
    const [table, field] = parts;
    const schema = String(param?.sourceSchema || '').trim();
    return schema && table && field ? { schema, table, field } : null;
  }
  if (parts.length === 1) {
    const field = parts[0];
    const schema = String(param?.sourceSchema || '').trim();
    const table = String(param?.sourceTable || '').trim();
    return schema && table && field ? { schema, table, field } : null;
  }
  return null;
}

function resolveParameterSource(param) {
  const directSchema = String(param?.sourceSchema || '').trim();
  const directTable = String(param?.sourceTable || '').trim();
  const directField = String(param?.sourceField || '').trim();
  if (directSchema && directTable && directField) return { schema: directSchema, table: directTable, field: directField };
  const definition = String(param?.definition || '').trim();
  if (!definition) return null;
  try {
    const parsed = JSON.parse(definition);
    if (parsed && typeof parsed === 'object') {
      const source = String(parsed?.source || '').trim();
      if (source) {
        const fromSource = parseParameterSourceRef(source, param);
        if (fromSource) return fromSource;
      }
      const schema = String(parsed?.source_schema || parsed?.schema || '').trim();
      const table = String(parsed?.source_table || parsed?.table || '').trim();
      const field = String(parsed?.source_field || parsed?.field || '').trim();
      if (schema && table && field) return { schema, table, field };
    }
  } catch {
    // continue with text format
  }
  const fieldFn = definition.match(/FIELD\(\s*['"]([^'"]+)['"]\s*\)/i);
  if (fieldFn?.[1]) {
    const fromField = parseParameterSourceRef(fieldFn[1], param);
    if (fromField) return fromField;
  }
  if (/^[A-Za-z0-9_.-]+$/.test(definition)) {
    const fromRaw = parseParameterSourceRef(definition, param);
    if (fromRaw) return fromRaw;
  }
  return null;
}

const PARAM_CONDITION_OPERATORS = {
  equals: '=',
  contains: 'contains',
  starts_with: 'starts_with',
  ends_with: 'ends_with',
  gt: '>',
  lt: '<',
  before: '<',
  after: '>',
  not_equals: '<>'
};

function conditionClause(cond, params) {
  const field = String(cond.field || '').trim();
  if (!isIdent(field)) return null;
  const operatorKey = String(cond.operator || 'equals');
  const op = PARAM_CONDITION_OPERATORS[operatorKey];
  if (!op) return null;
  const column = qi(field);
  const mode = cond.compareMode === 'column' ? 'column' : 'value';
  if (mode === 'column') {
    const target = String(cond.compareColumn || '').trim();
    const parts = target.split('.');
    const compareField = parts.length ? parts[parts.length - 1] : '';
    if (!isIdent(compareField)) return null;
    const cmpColumn = qi(compareField);
    if (op === 'contains' || op === 'starts_with' || op === 'ends_with') return null;
    return `${column} ${op} ${cmpColumn}`;
  }
  const rawValue = String(cond.compareValue || '').trim();
  if (!rawValue && op !== 'contains' && op !== 'starts_with' && op !== 'ends_with') return null;
  if (op === 'contains') {
    params.push(rawValue);
    return `${column} ILIKE '%' || $${params.length} || '%'`;
  }
  if (op === 'starts_with') {
    params.push(rawValue);
    return `${column} ILIKE $${params.length} || '%'`;
  }
  if (op === 'ends_with') {
    params.push(rawValue);
    return `${column} ILIKE '%' || $${params.length}`;
  }
  params.push(rawValue);
  return `${column} ${op} $${params.length}`;
}

async function queryParameterValue(client, param) {
  const source = resolveParameterSource(param);
  if (!source) return null;
  if (!isIdent(source.schema) || !isIdent(source.table) || !isIdent(source.field)) return null;
  const conditions = Array.isArray(param?.conditions) ? param.conditions : [];
  const params = [];
  const clauses = conditions.map((cond) => conditionClause(cond, params)).filter(Boolean);
  const whereSql = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
  const r = await client.query(
    `SELECT ${qi(source.field)} AS value FROM ${qname(source.schema, source.table)} ${whereSql} LIMIT 1`,
    params
  );
  return r.rows?.[0]?.value ?? null;
}

function scalarConditionClause(columnExpr, operatorKey, rawValue, params) {
  const op = PARAM_CONDITION_OPERATORS[String(operatorKey || 'equals')];
  if (!op) return null;
  const value = String(rawValue ?? '').trim();
  if (!value && op !== 'contains' && op !== 'starts_with' && op !== 'ends_with') return null;
  if (op === 'contains') {
    params.push(value);
    return `${columnExpr} ILIKE '%' || $${params.length} || '%'`;
  }
  if (op === 'starts_with') {
    params.push(value);
    return `${columnExpr} ILIKE $${params.length} || '%'`;
  }
  if (op === 'ends_with') {
    params.push(value);
    return `${columnExpr} ILIKE '%' || $${params.length}`;
  }
  params.push(value);
  return `${columnExpr} ${op} $${params.length}`;
}

async function queryRowsByDataModel(client, model, aliases, limit = 5000) {
  const requestedAliases = uniqueAliasList(aliases);
  if (!requestedAliases.length) return [];
  const tablesRaw = Array.isArray(model?.tables) ? model.tables : [];
  const joinsRaw = Array.isArray(model?.joins) ? model.joins : [];
  const fieldsRaw = Array.isArray(model?.fields) ? model.fields : [];
  const filtersRaw = Array.isArray(model?.filters) ? model.filters : [];

  const aliasSet = new Set(requestedAliases.map((x) => String(x || '').trim().toLowerCase()));
  const fields = fieldsRaw.filter((f) => aliasSet.has(String(f?.alias || '').trim().toLowerCase()));
  if (!fields.length) return [];

  const usedTableIds = new Set(fields.map((f) => String(f?.table_id || '').trim()).filter(Boolean));
  const tables = tablesRaw
    .filter((t) => usedTableIds.has(String(t?.id || '').trim()))
    .map((t, idx) => ({
      id: String(t?.id || `t${idx}`).trim(),
      schema: String(t?.schema || '').trim(),
      table: String(t?.table || '').trim(),
      sqlAlias: `t${idx}`
    }))
    .filter((t) => t.id && isIdent(t.schema) && isIdent(t.table));
  if (!tables.length) return [];

  const tableById = new Map();
  for (const t of tables) tableById.set(t.id, t);

  const joins = joinsRaw
    .map((j, idx) => ({
      id: String(j?.id || `j${idx}`).trim(),
      leftTableId: String(j?.left_table_id || j?.leftTableId || '').trim(),
      leftField: String(j?.left_field || j?.leftField || '').trim(),
      rightTableId: String(j?.right_table_id || j?.rightTableId || '').trim(),
      rightField: String(j?.right_field || j?.rightField || '').trim(),
      joinType: String(j?.join_type || j?.joinType || 'inner').trim().toLowerCase() === 'left' ? 'left' : 'inner'
    }))
    .filter(
      (j) =>
        j.leftTableId &&
        j.rightTableId &&
        j.leftTableId !== j.rightTableId &&
        tableById.has(j.leftTableId) &&
        tableById.has(j.rightTableId) &&
        isIdent(j.leftField) &&
        isIdent(j.rightField)
    );
  if (tables.length > 1 && !joins.length) {
    throw new Error('joins are required for multi-table selection');
  }

  const selectFields = fields
    .map((f) => ({
      tableId: String(f?.table_id || '').trim(),
      field: String(f?.field || '').trim(),
      alias: String(f?.alias || '').trim()
    }))
    .filter((f) => f.tableId && f.field && f.alias && tableById.has(f.tableId) && isIdent(f.field) && isIdent(f.alias));
  if (!selectFields.length) return [];

  const base = tables[0];
  const connected = new Set([base.id]);
  const pending = [...joins];
  const joinClauses = [];
  const extraJoinConditions = [];
  let guard = 0;
  while (pending.length && guard < 1000) {
    let progressed = false;
    for (let i = pending.length - 1; i >= 0; i -= 1) {
      const j = pending[i];
      const leftConnected = connected.has(j.leftTableId);
      const rightConnected = connected.has(j.rightTableId);
      if (!leftConnected && !rightConnected) continue;
      if (leftConnected && rightConnected) {
        const l = tableById.get(j.leftTableId);
        const r = tableById.get(j.rightTableId);
        extraJoinConditions.push(`${l.sqlAlias}.${qi(j.leftField)} = ${r.sqlAlias}.${qi(j.rightField)}`);
        pending.splice(i, 1);
        progressed = true;
        continue;
      }
      const sourceId = leftConnected ? j.leftTableId : j.rightTableId;
      const targetId = leftConnected ? j.rightTableId : j.leftTableId;
      const sourceField = leftConnected ? j.leftField : j.rightField;
      const targetField = leftConnected ? j.rightField : j.leftField;
      const source = tableById.get(sourceId);
      const target = tableById.get(targetId);
      const joinKeyword = j.joinType === 'left' ? 'LEFT JOIN' : 'INNER JOIN';
      joinClauses.push(
        `${joinKeyword} ${qname(target.schema, target.table)} ${target.sqlAlias} ON ${source.sqlAlias}.${qi(sourceField)} = ${target.sqlAlias}.${qi(
          targetField
        )}`
      );
      connected.add(targetId);
      pending.splice(i, 1);
      progressed = true;
    }
    if (!progressed) break;
    guard += 1;
  }
  if (connected.size !== tables.length) {
    throw new Error('unable to connect all selected tables with joins');
  }

  const params = [];
  const whereClauses = [...extraJoinConditions];
  const filters = filtersRaw
    .map((f, idx) => ({
      id: String(f?.id || `f${idx}`).trim(),
      tableId: String(f?.table_id || f?.tableId || '').trim(),
      field: String(f?.field || '').trim(),
      operator: String(f?.operator || 'equals').trim(),
      compareValue: String(f?.compare_value || f?.compareValue || '').trim()
    }))
    .filter((f) => f.tableId && tableById.has(f.tableId) && isIdent(f.field));
  for (const f of filters) {
    const tableRef = tableById.get(f.tableId);
    const colExpr = `${tableRef.sqlAlias}.${qi(f.field)}`;
    const clause = scalarConditionClause(colExpr, f.operator, f.compareValue, params);
    if (clause) whereClauses.push(clause);
  }
  params.push(Math.max(1, Math.min(5000, Number(limit || 1000))));
  const limitIdx = params.length;
  const selectCols = selectFields.map((f) => `${tableById.get(f.tableId).sqlAlias}.${qi(f.field)} AS ${qi(f.alias)}`).join(', ');
  const whereSql = whereClauses.length ? `WHERE ${whereClauses.join(' AND ')}` : '';
  const sql = `
    SELECT ${selectCols}
    FROM ${qname(base.schema, base.table)} ${base.sqlAlias}
    ${joinClauses.join('\n')}
    ${whereSql}
    LIMIT $${limitIdx}
  `;
  const r = await client.query(sql, params);
  return Array.isArray(r.rows) ? r.rows : [];
}

function isApiNode(node) {
  return Boolean(node && node.type === 'data' && String(node?.config?.group || '').trim() === 'api_requests');
}

function isApiToolNode(node) {
  return Boolean(node && node.type === 'tool' && String(node?.config?.toolType || '').trim() === 'api_request');
}

function normalizeApiRequestFromTemplateRow(template) {
  const method = String(template?.method || 'GET').trim().toUpperCase() || 'GET';
  const baseUrl = String(template?.base_url || '').trim();
  const pathRaw = String(template?.path || '').trim();
  const path = pathRaw ? (pathRaw.startsWith('/') ? pathRaw : `/${pathRaw}`) : '/';
  return {
    method,
    url: `${baseUrl}${path}`,
    authMode: String(template?.auth_mode || 'manual').trim() || 'manual',
    headers: template?.headers_json && typeof template.headers_json === 'object' ? deepClone(template.headers_json) : {},
    query: template?.query_json && typeof template.query_json === 'object' ? deepClone(template.query_json) : {},
    body: template?.body_json !== undefined ? deepClone(template.body_json) : {}
  };
}

function getNodeParameterOverrides(node) {
  if (!node) return {};
  if (isApiNode(node)) {
    const raw = node?.config?.parameterOverrides;
    return raw && typeof raw === 'object' && !Array.isArray(raw) ? raw : tryParseJson(raw, {});
  }
  if (isApiToolNode(node)) {
    const raw = node?.config?.settings?.parameterOverrides;
    return raw && typeof raw === 'object' && !Array.isArray(raw) ? raw : tryParseJson(raw, {});
  }
  return {};
}

function templateDispatchConfig(template) {
  const mapping = tryObject(template?.mapping_json);
  const execution = tryObject(template?.execution_json || mapping?.execution);
  const dispatchModeRaw = String(template?.dispatch_mode || execution?.dispatch_mode || 'single').trim().toLowerCase();
  const dispatchMode = dispatchModeRaw === 'group_by' ? 'group_by' : 'single';
  const groupByRaw = Array.isArray(template?.group_by_aliases)
    ? template.group_by_aliases
    : Array.isArray(execution?.group_by_aliases)
    ? execution.group_by_aliases
    : [];
  const groupByAliases = uniqueAliasList(groupByRaw.map((x) => String(x || '').trim()));
  return { dispatchMode, groupByAliases };
}

function resolveAliasesFromPaginationFirstValues(template, aliases) {
  const map = {};
  const requested = new Set(uniqueAliasList(aliases).map((a) => a.toLowerCase()));
  (template?.paginationParameters || []).forEach((p) => {
    const alias = String(p?.alias || '').trim();
    if (!alias || !requested.has(alias.toLowerCase())) return;
    if (p.firstValue === undefined || p.firstValue === null || p.firstValue === '') return;
    map[alias] = p.firstValue;
  });
  return map;
}

async function resolveAliasesFromParameterDefinitions(client, definitions, aliases) {
  const map = {};
  const issues = {};
  const requested = uniqueAliasList(aliases);
  if (!requested.length) return { map, issues };
  for (const alias of requested) {
    const param = (Array.isArray(definitions) ? definitions : []).find(
      (d) => String(d?.alias || '').trim().toLowerCase() === alias.toLowerCase()
    );
    if (!param) continue;
    try {
      const value = await queryParameterValue(client, param);
      if (value !== undefined && value !== null && String(value) !== '') {
        map[alias] = value;
      } else {
        issues[alias] = 'источник параметра вернул пустое значение';
      }
    } catch (e) {
      issues[alias] = String(e?.message || e || 'ошибка получения значения');
    }
  }
  return { map, issues };
}

async function resolveAliasRowsFromDataModel(client, template, aliases) {
  const issues = {};
  const requested = uniqueAliasList(aliases);
  if (!requested.length) return { rows: [], issues };
  const model = template?.dataModel;
  if (!model || !model.tables?.length || !model.fields?.length) return { rows: [], issues };
  try {
    const rawRows = await queryRowsByDataModel(client, model, requested, 5000);
    const rows = rawRows.map((row) => {
      const out = {};
      requested.forEach((alias) => {
        const found = findAliasInMap(row, alias);
        if (found.found) out[alias] = found.value;
      });
      return out;
    });
    return { rows, issues };
  } catch (e) {
    const msg = String(e?.message || e || 'ошибка чтения конструктора данных');
    requested.forEach((alias) => {
      issues[alias] = msg;
    });
    return { rows: [], issues };
  }
}

function resolveStopRuleCompareValue(raw, resolvedParams, requestCtx = {}) {
  const source = String(raw || '').trim();
  if (!source) return '';
  const bodyRef = source.match(/^\{\{\s*body\.([^{}]+)\s*\}\}$/i);
  if (bodyRef?.[1]) return getByPath(requestCtx?.body || {}, String(bodyRef[1] || '').trim());
  const queryRef = source.match(/^\{\{\s*query\.([^{}]+)\s*\}\}$/i);
  if (queryRef?.[1]) return getByPath(requestCtx?.query || {}, String(queryRef[1] || '').trim());
  const headersRef = source.match(/^\{\{\s*headers\.([^{}]+)\s*\}\}$/i);
  if (headersRef?.[1]) return getByPath(requestCtx?.headers || {}, String(headersRef[1] || '').trim());
  return applyParametersToValue(source, resolvedParams);
}

function evaluateNodeStopRuleOperator(operatorRaw, actual, expected) {
  const op = normalizeStopRuleOperator(operatorRaw);
  if (op === 'equals') return evaluateStopRuleValue(actual, '=', expected);
  if (op === 'not_equals') return evaluateStopRuleValue(actual, '!=', expected);
  if (op === 'gt') return evaluateStopRuleValue(actual, '>', expected);
  if (op === 'gte') return evaluateStopRuleValue(actual, '>=', expected);
  if (op === 'lt') return evaluateStopRuleValue(actual, '<', expected);
  if (op === 'lte') return evaluateStopRuleValue(actual, '<=', expected);
  if (op === 'contains') return evaluateStopRuleValue(actual, 'contains', expected);
  if (op === 'not_contains') return evaluateStopRuleValue(actual, 'not_contains', expected);
  if (op === 'is_empty') return evaluateStopRuleValue(actual, 'empty', expected);
  if (op === 'not_empty') return evaluateStopRuleValue(actual, 'exists', expected);
  return false;
}

function findTriggeredStopRule(responseBody, rules, resolvedParams, requestCtx = {}) {
  const list = Array.isArray(rules) ? rules : [];
  for (const rule of list) {
    const path = String(rule?.responsePath || '').trim();
    if (!path) continue;
    const actual = getByPath({ response: responseBody }, path);
    const expected = resolveStopRuleCompareValue(String(rule?.compareValue || ''), resolvedParams, requestCtx);
    if (evaluateNodeStopRuleOperator(String(rule?.operator || ''), actual, expected)) {
      return {
        id: String(rule?.id || ''),
        path,
        operator: normalizeStopRuleOperator(rule?.operator),
        actual,
        expected
      };
    }
  }
  return null;
}

function parseHttpHeaders(rawHeaders) {
  const blockedHeaders = new Set(['host', 'connection', 'content-length', 'accept-encoding']);
  const out = {};
  const src = rawHeaders && typeof rawHeaders === 'object' && !Array.isArray(rawHeaders) ? rawHeaders : {};
  Object.entries(src).forEach(([k, v]) => {
    const key = String(k || '').trim();
    if (!key || blockedHeaders.has(key.toLowerCase())) return;
    if (v === undefined || v === null) return;
    out[key] = typeof v === 'string' ? v : String(v);
  });
  return out;
}

async function executeHttpWithRetry({ method, url, headers, body }) {
  let attempt = 0;
  let lastResult = null;
  while (attempt < RETRY_ATTEMPTS) {
    attempt += 1;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), RUN_TIMEOUT_MS);
    try {
      const resp = await fetch(url, {
        method,
        headers,
        body: body === undefined ? undefined : JSON.stringify(body),
        redirect: 'follow',
        signal: controller.signal
      });
      const text = await resp.text();
      let bodyJson = null;
      try {
        bodyJson = text ? JSON.parse(text) : null;
      } catch {
        bodyJson = null;
      }
      const result = {
        ok: resp.ok,
        status: resp.status,
        body: bodyJson === null ? text : bodyJson,
        raw_text: text
      };
      lastResult = result;
      if (shouldRetryStatus(resp.status) && attempt < RETRY_ATTEMPTS) {
        const waitMs = retryDelayMs(attempt, RETRY_BASE_MS, RETRY_MAX_MS);
        await delayMs(waitMs);
        continue;
      }
      return result;
    } catch (e) {
      lastResult = {
        ok: false,
        status: 0,
        body: { error: String(e?.message || e || 'request_failed') },
        raw_text: ''
      };
      if (attempt < RETRY_ATTEMPTS) {
        const waitMs = retryDelayMs(attempt, RETRY_BASE_MS, RETRY_MAX_MS);
        await delayMs(waitMs);
        continue;
      }
      return lastResult;
    } finally {
      clearTimeout(timeout);
    }
  }
  return (
    lastResult || {
      ok: false,
      status: 0,
      body: { error: 'request_failed' },
      raw_text: ''
    }
  );
}

async function executeApiNode(client, node, template) {
  const request = normalizeApiRequestFromTemplateRow(template);
  const method = String(request.method || 'GET').trim().toUpperCase() || 'GET';
  const urlRaw = String(request.url || '').trim();
  if (!urlRaw) {
    throw new Error('У шаблона API пустой URL');
  }
  const pagination = paginationFromTemplateRaw(template);
  const headersObj = request.headers && typeof request.headers === 'object' ? deepClone(request.headers) : {};
  const baseQueryObj = request.query && typeof request.query === 'object' ? deepClone(request.query) : {};
  const bodyRaw = request.body !== undefined ? deepClone(request.body) : {};
  const baseBodyObj = bodyRaw && typeof bodyRaw === 'object' ? bodyRaw : {};

  const requestedAliasesSet = new Set();
  collectParameterTokens(urlRaw, requestedAliasesSet);
  collectParameterTokens(headersObj, requestedAliasesSet);
  collectParameterTokens(baseQueryObj, requestedAliasesSet);
  collectParameterTokens(baseBodyObj, requestedAliasesSet);
  const requestedAliases = Array.from(requestedAliasesSet);

  let resolveIssues = {};
  let entityParameterMaps = [{}];
  if (requestedAliases.length) {
    const dispatchCfg = templateDispatchConfig(template);
    const paginationSeed = resolveAliasesFromPaginationFirstValues(template, requestedAliases);
    const unresolvedAfterPagination = requestedAliases.filter((alias) => !findAliasInMap(paginationSeed, alias).found);

    let dataRows = [];
    if (unresolvedAfterPagination.length) {
      const fromRows = await resolveAliasRowsFromDataModel(client, template, unresolvedAfterPagination);
      dataRows = Array.isArray(fromRows.rows) ? fromRows.rows : [];
      Object.entries(fromRows.issues || {}).forEach(([alias, reason]) => {
        if (!resolveIssues[alias]) resolveIssues[alias] = String(reason || '');
      });
    }

    const fromDefinitions = await resolveAliasesFromParameterDefinitions(
      client,
      template.parameterDefinitions || [],
      unresolvedAfterPagination
    );
    Object.entries(fromDefinitions.issues || {}).forEach(([alias, reason]) => {
      if (!resolveIssues[alias]) resolveIssues[alias] = String(reason || '');
    });

    const rowsBase = dataRows.length ? dataRows : [{}];
    let mergedRows = rowsBase.map((row) => ({
      ...row,
      ...fromDefinitions.map,
      ...paginationSeed
    }));
    if (dispatchCfg.dispatchMode === 'group_by' && dispatchCfg.groupByAliases.length && mergedRows.length > 1) {
      const grouped = groupRowsByAliases(mergedRows, dispatchCfg.groupByAliases);
      mergedRows = Array.from(grouped.values()).map((g) => ({
        ...(g?.rows?.[0] || {}),
        ...(g?.key || {})
      }));
    }
    entityParameterMaps = mergedRows.length ? mergedRows : [{ ...fromDefinitions.map, ...paginationSeed }];
  }

  const nodeOverrides = getNodeParameterOverrides(node);
  const finalEntityMaps = (entityParameterMaps.length ? entityParameterMaps : [{}]).map((m) => ({ ...m, ...nodeOverrides }));
  const paginationAliasesLower = new Set(
    (template?.paginationParameters || [])
      .map((p) => String(p?.alias || '').trim().toLowerCase())
      .filter(Boolean)
  );

  const requestsPreview = [];
  const responsesPreview = [];
  const stopReasons = [];
  let requestCount = 0;
  let success = 0;
  let failed = 0;
  let payloadCount = 0;
  let lastStatus = 0;

  for (let entityIdx = 0; entityIdx < finalEntityMaps.length; entityIdx += 1) {
    const resolvedParameters = finalEntityMaps[entityIdx] || {};
    const headersForEntity = deepClone(headersObj || {});
    const queryForEntity = deepClone(baseQueryObj || {});
    const bodyForEntity = deepClone(baseBodyObj || {});
    const resolvedUrlRaw = replaceParameterTokens(urlRaw, resolvedParameters);

    applyParametersToValue(headersForEntity, resolvedParameters);
    applyParametersToValue(queryForEntity, resolvedParameters);
    applyParametersToValue(bodyForEntity, resolvedParameters);

    const unresolvedBeforeStrip = new Set();
    collectParameterTokens(resolvedUrlRaw, unresolvedBeforeStrip);
    collectParameterTokens(headersForEntity, unresolvedBeforeStrip);
    collectParameterTokens(queryForEntity, unresolvedBeforeStrip);
    collectParameterTokens(bodyForEntity, unresolvedBeforeStrip);
    const unresolvedPaginationAliasesLower = new Set(
      Array.from(unresolvedBeforeStrip)
        .map((a) => String(a || '').trim().toLowerCase())
        .filter((a) => paginationAliasesLower.has(a))
    );
    if (unresolvedPaginationAliasesLower.size) {
      stripUnresolvedTemplateTokens(queryForEntity, unresolvedPaginationAliasesLower);
      if (bodyForEntity && typeof bodyForEntity === 'object') {
        stripUnresolvedTemplateTokens(bodyForEntity, unresolvedPaginationAliasesLower);
      }
    }

    const unresolvedTokens = new Set();
    collectParameterTokens(resolvedUrlRaw, unresolvedTokens);
    collectParameterTokens(headersForEntity, unresolvedTokens);
    collectParameterTokens(queryForEntity, unresolvedTokens);
    collectParameterTokens(bodyForEntity, unresolvedTokens);
    if (!Object.keys(resolveIssues).length && unresolvedTokens.size) {
      Array.from(unresolvedTokens).forEach((alias) => {
        resolveIssues[alias] = 'alias не найден в источниках шаблона (конструктор/параметры/первое значение пагинации)';
      });
    }
    if (unresolvedTokens.size) {
      const unresolvedList = Array.from(unresolvedTokens);
      const details = unresolvedList
        .map((alias) => {
          const issue = findAliasInMap(resolveIssues, alias);
          return issue.found && issue.value ? `${alias} (${issue.value})` : alias;
        })
        .join(', ');
      throw new Error(`Не удалось подставить параметры (сущность ${entityIdx + 1}): ${details}`);
    }

    let entityStopReason = '';
    let pageNo = 1;
    let pageNumberValue = Math.max(1, Number(pagination.startPage || 1));
    let offsetValue = Math.max(0, Number(pagination.startOffset || 0));
    let cursorValue;
    let lastResponseFingerprint = '';
    let sameResponseStreak = 0;

    while (true) {
      if (pageNo > ABSOLUTE_PAGE_SAFETY_LIMIT) {
        entityStopReason = `Достигнут защитный лимит страниц (${ABSOLUTE_PAGE_SAFETY_LIMIT})`;
        break;
      }

      const queryObj = deepClone(queryForEntity || {});
      const bodyObjRaw = deepClone(bodyForEntity || {});
      const bodyObj = bodyObjRaw && typeof bodyObjRaw === 'object' ? bodyObjRaw : {};
      const targetObj = pagination.target === 'body' ? bodyObj : queryObj;
      if (pagination.enabled) {
        if (pagination.mode === 'page_number') {
          setByPath(targetObj, pagination.pageParam, pageNumberValue);
          if (pagination.limitParam) setByPath(targetObj, pagination.limitParam, Math.max(1, Number(pagination.limitValue || 1)));
        } else if (pagination.mode === 'offset_limit') {
          setByPath(targetObj, pagination.offsetParam, offsetValue);
          if (pagination.limitParam) setByPath(targetObj, pagination.limitParam, Math.max(1, Number(pagination.limitValue || 1)));
        } else if (pagination.mode === 'cursor') {
          if (pageNo > 1) {
            if ((cursorValue === undefined || cursorValue === null || cursorValue === '') && pagination.stopOnMissingValue) {
              entityStopReason = 'Остановка: курсор не найден в предыдущем ответе';
              break;
            }
            setByPath(targetObj, pagination.cursorReqPath, cursorValue);
          }
        }
      }

      const url = new URL(resolvedUrlRaw);
      Object.entries(queryObj || {}).forEach(([k, v]) => {
        if (v !== undefined && v !== null) url.searchParams.set(k, String(v));
      });
      const headers = parseHttpHeaders(headersForEntity);
      const reqPayload = {
        entity_index: entityIdx + 1,
        page: pageNo,
        method,
        url: url.toString(),
        headers,
        query: queryObj,
        body: method === 'GET' || method === 'DELETE' ? undefined : bodyObj
      };
      requestCount += 1;
      if (requestsPreview.length < MAX_REQUESTS_PREVIEW) requestsPreview.push(reqPayload);

      const resp = await executeHttpWithRetry({
        method,
        url: reqPayload.url,
        headers,
        body: method === 'GET' || method === 'DELETE' ? undefined : bodyObj
      });
      lastStatus = Number(resp?.status || 0);
      if (resp?.ok) success += 1;
      else failed += 1;

      const responseBody = resp?.body;
      if (responsesPreview.length < MAX_RESPONSES_PREVIEW) {
        responsesPreview.push({
          entity_index: entityIdx + 1,
          page: pageNo,
          status: lastStatus,
          response: responseBody
        });
      }

      if (!resp?.ok && pagination.stopOnHttpError) {
        entityStopReason = `HTTP ошибка ${lastStatus || 0}`;
        break;
      }
      if (!pagination.enabled || pagination.mode === 'none') break;

      if (pagination.useMaxPages) {
        const maxPages = Math.max(1, Number(pagination.maxPages || 1));
        if (pageNo >= maxPages) {
          entityStopReason = `Достигнут лимит страниц (${maxPages})`;
          break;
        }
      }

      const stopRuleHit = findTriggeredStopRule(responseBody, pagination.stopRules || [], resolvedParameters, {
        body: bodyObj,
        query: queryObj,
        headers
      });
      if (stopRuleHit) {
        entityStopReason = `Сработало условие остановки: ${stopRuleHit.path}`;
        break;
      }

      if (pagination.stopOnSameResponse) {
        const fingerprint =
          typeof responseBody === 'string'
            ? responseBody
            : (() => {
                try {
                  return JSON.stringify(responseBody);
                } catch {
                  return String(responseBody);
                }
              })();
        if (fingerprint === lastResponseFingerprint) sameResponseStreak += 1;
        else sameResponseStreak = 1;
        lastResponseFingerprint = fingerprint;
        if (sameResponseStreak >= Math.max(2, Math.min(50, Number(pagination.sameResponseLimit || 5)))) {
          entityStopReason = `Получено ${sameResponseStreak} одинаковых ответов подряд`;
          break;
        }
      }

      const dataArr = pagination.dataPath ? getByPath({ response: responseBody }, pagination.dataPath) : undefined;
      if (Array.isArray(dataArr)) payloadCount += dataArr.length;
      const isMissingData = pagination.dataPath
        ? dataArr === undefined || dataArr === null || (Array.isArray(dataArr) && dataArr.length === 0)
        : false;
      if (pagination.mode === 'page_number') {
        if (pagination.stopOnMissingValue && isMissingData) {
          entityStopReason = 'Остановка: массив данных пустой или отсутствует';
          break;
        }
        pageNumberValue += 1;
      } else if (pagination.mode === 'offset_limit') {
        if (pagination.stopOnMissingValue && isMissingData) {
          entityStopReason = 'Остановка: массив данных пустой или отсутствует';
          break;
        }
        offsetValue += Math.max(1, Number(pagination.limitValue || 1));
      } else if (pagination.mode === 'cursor') {
        cursorValue = getByPath({ response: responseBody }, pagination.cursorResPath);
        if (pagination.stopOnMissingValue && isMissingData) {
          entityStopReason = 'Остановка: массив данных пустой или отсутствует';
          break;
        }
        if (pagination.stopOnMissingValue && (cursorValue === undefined || cursorValue === null || cursorValue === '')) {
          entityStopReason = 'Остановка: курсор пустой';
          break;
        }
      }

      pageNo += 1;
      if (pagination.useDelay) await delayMs(Math.max(0, Number(pagination.pauseMs || 0)));
    }

    if (entityStopReason) {
      stopReasons.push({ entity_index: entityIdx + 1, stop_reason: entityStopReason });
    }
  }

  return {
    requestPreview: {
      total_requests: requestCount,
      shown_requests: requestsPreview.length,
      requests: requestsPreview,
      template_store_id: Number(template?.id || 0) || undefined,
      resolved_parameters:
        finalEntityMaps.length === 1
          ? finalEntityMaps[0]
          : finalEntityMaps.slice(0, 5).map((values, idx) => ({ entity_index: idx + 1, values })),
      resolve_issues: Object.keys(resolveIssues).length ? resolveIssues : undefined
    },
    responsePreview: {
      total_requests: requestCount,
      request_entities: finalEntityMaps.length,
      success,
      failed,
      stop_reasons: stopReasons.length ? stopReasons : undefined,
      responses: responsesPreview,
      template_store_id: Number(template?.id || 0) || undefined,
      resolved_parameters:
        finalEntityMaps.length === 1
          ? finalEntityMaps[0]
          : finalEntityMaps.slice(0, 5).map((values, idx) => ({ entity_index: idx + 1, values })),
      resolve_issues: Object.keys(resolveIssues).length ? resolveIssues : undefined
    },
    metrics: {
      requestCount,
      success,
      failed,
      payloadCount,
      stopReasons,
      status: lastStatus
    }
  };
}

function topoSort(nodes, edges) {
  const indeg = new Map((nodes || []).map((n) => [n.id, 0]));
  const byId = new Map((nodes || []).map((n) => [n.id, n]));
  (edges || []).forEach((e) => indeg.set(e.to, Number(indeg.get(e.to) || 0) + 1));
  const q = Array.from(indeg.entries())
    .filter(([, d]) => d === 0)
    .map(([id]) => id);
  const out = [];
  while (q.length) {
    const id = String(q.shift());
    const n = byId.get(id);
    if (!n) continue;
    out.push(n);
    (edges || [])
      .filter((e) => e.from === id)
      .forEach((e) => {
        const d = Number(indeg.get(e.to) || 0) - 1;
        indeg.set(e.to, d);
        if (d === 0) q.push(e.to);
      });
  }
  return out.length === (nodes || []).length ? out : null;
}

async function insertRunRow(client, config, payload) {
  const qn = workflowRunsQname(config);
  await client.query(
    `
    INSERT INTO ${qn}
      (run_uid, desk_id, desk_name, trigger_source, trigger_key, trigger_meta, status, started_at, created_by, updated_at)
    VALUES
      ($1, $2, $3, $4, $5, $6::jsonb, 'running', now(), $7, now())
    `,
    [
      String(payload.run_uid || '').trim(),
      Math.trunc(Number(payload.desk_id || 0)),
      String(payload.desk_name || '').trim(),
      String(payload.trigger_source || 'manual').trim(),
      String(payload.trigger_key || '').trim(),
      JSON.stringify(payload.trigger_meta || {}),
      String(payload.created_by || 'system').trim()
    ]
  );
}

async function updateRunRow(client, config, runUid, patch) {
  const qn = workflowRunsQname(config);
  await client.query(
    `
    UPDATE ${qn}
    SET
      status = $2,
      finished_at = $3::timestamptz,
      duration_ms = $4,
      summary_json = $5::jsonb,
      error_text = $6,
      updated_at = now()
    WHERE run_uid = $1
    `,
    [
      String(runUid || '').trim(),
      String(patch?.status || 'failed').trim(),
      patch?.finished_at ? String(patch.finished_at) : new Date().toISOString(),
      Math.max(0, Math.trunc(Number(patch?.duration_ms || 0))),
      JSON.stringify(patch?.summary_json || {}),
      String(patch?.error_text || '').trim()
    ]
  );
}

async function insertStepRow(client, config, payload) {
  const qn = workflowRunStepsQname(config);
  await client.query(
    `
    INSERT INTO ${qn}
      (run_uid, step_no, node_id, node_name, node_type, status, started_at, finished_at, duration_ms, request_payload, response_payload, metrics_json, error_text)
    VALUES
      ($1, $2, $3, $4, $5, $6, $7::timestamptz, $8::timestamptz, $9, $10::jsonb, $11::jsonb, $12::jsonb, $13)
    `,
    [
      String(payload?.run_uid || '').trim(),
      Math.max(1, Math.trunc(Number(payload?.step_no || 1))),
      String(payload?.node_id || '').trim(),
      String(payload?.node_name || '').trim(),
      String(payload?.node_type || '').trim(),
      String(payload?.status || 'ok').trim(),
      payload?.started_at ? String(payload.started_at) : new Date().toISOString(),
      payload?.finished_at ? String(payload.finished_at) : new Date().toISOString(),
      Math.max(0, Math.trunc(Number(payload?.duration_ms || 0))),
      JSON.stringify(payload?.request_payload || {}),
      JSON.stringify(payload?.response_payload || {}),
      JSON.stringify(payload?.metrics_json || {}),
      String(payload?.error_text || '').trim()
    ]
  );
}

function parseIntervalToMs(valueRaw, unitRaw) {
  const value = Math.max(1, Math.trunc(Number(valueRaw || 1)));
  const unit = String(unitRaw || 'minutes').trim().toLowerCase();
  if (unit === 'seconds') return value * 1000;
  if (unit === 'hours') return value * 60 * 60 * 1000;
  return value * 60 * 1000;
}

function parseCronPart(part, min, max) {
  const raw = String(part || '').trim();
  if (!raw || raw === '*') return null;
  if (raw.startsWith('*/')) {
    const n = Number(raw.slice(2));
    if (!Number.isFinite(n) || n <= 0) return null;
    return { step: Math.trunc(n) };
  }
  const values = raw
    .split(',')
    .map((x) => Number(String(x || '').trim()))
    .filter((x) => Number.isFinite(x) && x >= min && x <= max)
    .map((x) => Math.trunc(x));
  if (!values.length) return null;
  return { set: new Set(values) };
}

function cronMatchesNow(cronExpr, now = new Date()) {
  const expr = String(cronExpr || '').trim();
  if (!expr) return false;
  const parts = expr.split(/\s+/).filter(Boolean);
  if (parts.length !== 5) return false;
  const minute = parseCronPart(parts[0], 0, 59);
  const hour = parseCronPart(parts[1], 0, 23);
  const day = parseCronPart(parts[2], 1, 31);
  const month = parseCronPart(parts[3], 1, 12);
  const week = parseCronPart(parts[4], 0, 6);
  const checks = [
    [minute, now.getMinutes()],
    [hour, now.getHours()],
    [day, now.getDate()],
    [month, now.getMonth() + 1],
    [week, now.getDay()]
  ];
  for (const [rule, value] of checks) {
    if (!rule) continue;
    if (rule.step && value % rule.step !== 0) return false;
    if (rule.set && !rule.set.has(value)) return false;
  }
  return true;
}

function activeStartTriggers(nodes) {
  return (Array.isArray(nodes) ? nodes : [])
    .filter((n) => n?.type === 'tool' && String(n?.config?.toolType || '').trim() === 'start_process')
    .map((n) => {
      const settings = n?.config?.settings && typeof n.config.settings === 'object' ? n.config.settings : {};
      return {
        nodeId: String(n?.id || '').trim(),
        nodeName: String(n?.config?.name || 'Start').trim() || 'Start',
        triggerType: String(settings?.triggerType || 'interval').trim().toLowerCase(),
        intervalValue: Number(settings?.intervalValue || 1),
        intervalUnit: String(settings?.intervalUnit || 'minutes').trim().toLowerCase()
      };
    })
    .filter((x) => Boolean(x.nodeId));
}

function activeScheduleTriggers(nodes) {
  return (Array.isArray(nodes) ? nodes : [])
    .filter((n) => n?.type === 'tool' && String(n?.config?.toolType || '').trim() === 'schedule_process')
    .map((n) => {
      const settings = n?.config?.settings && typeof n.config.settings === 'object' ? n.config.settings : {};
      return {
        nodeId: String(n?.id || '').trim(),
        nodeName: String(n?.config?.name || 'Schedule').trim() || 'Schedule',
        cron: String(settings?.cron || '').trim()
      };
    })
    .filter((x) => Boolean(x.nodeId && x.cron));
}

async function lastRunForTrigger(client, config, deskId, triggerSource, triggerKey) {
  const qn = workflowRunsQname(config);
  const r = await client.query(
    `
    SELECT started_at
    FROM ${qn}
    WHERE desk_id = $1
      AND trigger_source = $2
      AND trigger_key = $3
    ORDER BY started_at DESC
    LIMIT 1
    `,
    [Math.trunc(Number(deskId || 0)), String(triggerSource || '').trim(), String(triggerKey || '').trim()]
  );
  return r.rows?.[0]?.started_at || null;
}

function requestSignature(node) {
  if (!node || typeof node !== 'object') return '';
  return `${String(node.id || '')}:${String(node?.config?.name || '')}:${String(node?.type || '')}`;
}

async function executeWorkflowRun(client, config, deskRow, runMeta) {
  const runUid = String(runMeta?.run_uid || '').trim();
  const startTs = Date.now();
  const cfg = deskRow?.config_json && typeof deskRow.config_json === 'object' ? deskRow.config_json : {};
  const nodesRaw = Array.isArray(cfg?.nodes) ? cfg.nodes : [];
  const edgesRaw = Array.isArray(cfg?.edges) ? cfg.edges : [];
  const nodes = nodesRaw
    .map((n) => {
      if (!n || typeof n !== 'object') return null;
      const id = String(n?.id || '').trim();
      if (!id) return null;
      return {
        id,
        type: String(n?.type || 'data').trim(),
        config: n?.config && typeof n.config === 'object' ? n.config : {}
      };
    })
    .filter(Boolean);
  const nodeIds = new Set(nodes.map((n) => n.id));
  const edges = edgesRaw
    .map((e) => ({
      from: String(e?.from || '').trim(),
      to: String(e?.to || '').trim()
    }))
    .filter((e) => e.from && e.to && nodeIds.has(e.from) && nodeIds.has(e.to));

  const order = topoSort(nodes, edges);
  if (!order) throw new Error('workflow содержит цикл');
  const templates = await loadActiveApiConfigs(client, config);
  const apiNodeCount = order.filter((n) => isApiNode(n) || isApiToolNode(n)).length;
  let stepNo = 0;
  let totalRequests = 0;
  let totalSuccess = 0;
  let totalFailed = 0;
  let totalPayload = 0;
  let stepErrors = 0;

  for (const node of order) {
    stepNo += 1;
    const stepStarted = Date.now();
    let status = 'ok';
    let requestPayload = {};
    let responsePayload = {};
    let metrics = {};
    let errorText = '';
    try {
      if (isApiNode(node) || isApiToolNode(node)) {
        const refs = nodeTemplateRefs(node);
        const template = templates.find((tpl) => refs.some((ref) => sourceMatchesTemplateRef(tpl, ref)));
        if (!template) {
          throw new Error(`Не найден API-шаблон для ноды (${requestSignature(node)})`);
        }
        const exec = await executeApiNode(client, node, template);
        requestPayload = exec.requestPreview || {};
        responsePayload = exec.responsePreview || {};
        metrics = exec.metrics || {};
        totalRequests += Number(exec.metrics?.requestCount || 0);
        totalSuccess += Number(exec.metrics?.success || 0);
        totalFailed += Number(exec.metrics?.failed || 0);
        totalPayload += Number(exec.metrics?.payloadCount || 0);
        if (Number(exec.metrics?.failed || 0) > 0) status = 'warn';
      } else {
        requestPayload = { node: node.id, type: node.type };
        responsePayload = { ok: true };
        metrics = { handled: true };
      }
    } catch (e) {
      status = 'error';
      errorText = String(e?.message || e || 'step_failed');
      responsePayload = { error: errorText };
      metrics = { failed: true };
      stepErrors += 1;
    }
    const stepDuration = Date.now() - stepStarted;
    await insertStepRow(client, config, {
      run_uid: runUid,
      step_no: stepNo,
      node_id: String(node?.id || ''),
      node_name: String(node?.config?.name || node?.id || '').trim(),
      node_type: String(node?.type || ''),
      status,
      started_at: new Date(stepStarted).toISOString(),
      finished_at: new Date(stepStarted + stepDuration).toISOString(),
      duration_ms: stepDuration,
      request_payload: requestPayload,
      response_payload: responsePayload,
      metrics_json: metrics,
      error_text: errorText
    });
  }

  const durationMs = Date.now() - startTs;
  const hasErrors = totalFailed > 0 || stepErrors > 0;
  return {
    status: hasErrors ? 'completed_with_errors' : 'completed',
    summary_json: {
      desk_id: Number(deskRow?.id || 0),
      desk_name: String(deskRow?.desk_name || ''),
      nodes_total: order.length,
      api_nodes: apiNodeCount,
      total_requests: totalRequests,
      success: totalSuccess,
      failed: totalFailed,
      payload_count: totalPayload,
      step_errors: stepErrors
    },
    error_text: '',
    duration_ms: durationMs
  };
}

async function runDeskById(deskId, triggerMeta = {}, options = {}) {
  const createdBy = String(options?.created_by || 'scheduler').trim() || 'scheduler';
  const triggerSource = String(triggerMeta?.trigger_source || 'manual').trim() || 'manual';
  const triggerKey = String(triggerMeta?.trigger_key || '').trim();
  const runUid = String(options?.run_uid || buildRunUid('wf_run')).trim() || buildRunUid('wf_run');

  const client = await pool.connect();
  let config = { ...DEFAULT_CONFIG };
  try {
    config = await loadRuntimeStorageConfig(client);
    await ensureWorkflowAutomationTables(client, config);
    const deskQn = workflowDesksQname(config);
    const deskRes = await client.query(
      `
      SELECT id, desk_name, desk_type, config_json, revision, updated_at
      FROM ${deskQn}
      WHERE id = $1 AND is_active = true
      LIMIT 1
      `,
      [Math.trunc(Number(deskId || 0))]
    );
    const deskRow = deskRes.rows?.[0];
    if (!deskRow) throw new Error('workflow desk not found');

    await insertRunRow(client, config, {
      run_uid: runUid,
      desk_id: Number(deskRow.id || 0),
      desk_name: String(deskRow.desk_name || ''),
      trigger_source: triggerSource,
      trigger_key: triggerKey,
      trigger_meta:
        triggerMeta?.trigger_meta && typeof triggerMeta.trigger_meta === 'object'
          ? triggerMeta.trigger_meta
          : triggerMeta,
      created_by: createdBy
    });

    const result = await executeWorkflowRun(client, config, deskRow, { run_uid: runUid });
    await updateRunRow(client, config, runUid, {
      status: result.status,
      finished_at: new Date().toISOString(),
      duration_ms: Number(result.duration_ms || 0),
      summary_json: result.summary_json || {},
      error_text: result.error_text || ''
    });
    return { run_uid: runUid, ...result, desk_id: Number(deskRow.id || 0), desk_name: String(deskRow.desk_name || '') };
  } catch (e) {
    const errorText = String(e?.message || e || 'run_failed');
    try {
      await updateRunRow(client, config, runUid, {
        status: 'failed',
        finished_at: new Date().toISOString(),
        duration_ms: 0,
        summary_json: {},
        error_text: errorText
      });
    } catch {
      // ignore update failure
    }
    throw new Error(errorText);
  } finally {
    client.release();
  }
}

function reserveRunSlot(deskId, runUid) {
  if (schedulerState.activeRuns.size >= MAX_PARALLEL_RUNS) return false;
  const key = String(deskId || '').trim();
  for (const item of schedulerState.activeRuns.values()) {
    if (String(item?.deskId || '') === key) return false;
  }
  schedulerState.activeRuns.set(runUid, { deskId: key, startedAt: new Date().toISOString() });
  return true;
}

function releaseRunSlot(runUid) {
  schedulerState.activeRuns.delete(String(runUid || '').trim());
}

function launchRun(deskId, triggerMeta = {}, options = {}) {
  const runUid = buildRunUid('wf_run');
  if (!reserveRunSlot(deskId, runUid)) {
    return {
      accepted: false,
      run_uid: '',
      promise: Promise.resolve({
        run_uid: '',
        status: 'skipped',
        summary_json: { reason: 'parallel_limit_or_desk_busy' },
        duration_ms: 0,
        error_text: ''
      })
    };
  }

  const promise = runDeskById(deskId, triggerMeta, { ...options, run_uid: runUid })
    .catch((e) => ({
      run_uid: runUid,
      status: 'failed',
      summary_json: {},
      duration_ms: 0,
      error_text: String(e?.message || e || 'run_failed')
    }))
    .finally(() => {
      releaseRunSlot(runUid);
    });

  return { accepted: true, run_uid: runUid, promise };
}

async function withSchedulerLock(client, fn) {
  const lock = await client.query('SELECT pg_try_advisory_lock($1) AS locked', [SCHEDULER_LOCK_KEY]);
  if (!Boolean(lock.rows?.[0]?.locked)) return false;
  try {
    await fn();
    return true;
  } finally {
    await client.query('SELECT pg_advisory_unlock($1)', [SCHEDULER_LOCK_KEY]);
  }
}

async function schedulerTick() {
  await schedulerTickPublishedProcesses();
}

export async function bootstrapWorkflowAutomation() {
  const client = await pool.connect();
  try {
    const config = await loadRuntimeStorageConfig(client);
    await ensureWorkflowAutomationTables(client, config);
    return { ok: true, config };
  } finally {
    client.release();
  }
}

export function startWorkflowScheduler() {
  if (!schedulerState.enabled) {
    return {
      started: false,
      reason: 'disabled_by_env',
      tick_ms: schedulerState.tickMs
    };
  }
  if (schedulerState.timer) {
    return {
      started: true,
      reason: 'already_started',
      tick_ms: schedulerState.tickMs
    };
  }
  schedulerState.timer = setInterval(() => {
    void schedulerTick();
  }, schedulerState.tickMs);
  void schedulerTick();
  return {
    started: true,
    reason: 'ok',
    tick_ms: schedulerState.tickMs
  };
}

export function stopWorkflowScheduler() {
  if (schedulerState.timer) {
    clearInterval(schedulerState.timer);
    schedulerState.timer = null;
  }
  return { stopped: true };
}

workflowAutomationRouter.get('/workflow-scheduler/state', requireDataAdmin, async (_req, res) => {
  let queueDepth = 0;
  let queueRunning = 0;
  let queueDead = 0;
  let dependencyBacklog = 0;
  let activeWorkers = 0;
  try {
    const client = await pool.connect();
    try {
      const config = await loadRuntimeStorageConfig(client);
      await ensureWorkflowAutomationTables(client, config);
      const queueQn = workflowJobQueueQname(config);
      const busQn = workflowProcessBusQname(config);
      const workerQn = workflowWorkerLeasesQname(config);
      const q = await client.query(
        `
        SELECT
          COUNT(*) FILTER (WHERE status = 'queued')::int AS queued,
          COUNT(*) FILTER (WHERE status IN ('running', 'locked'))::int AS running,
          COUNT(*) FILTER (WHERE status = 'dead_letter')::int AS dead
        FROM ${queueQn}
        `
      );
      queueDepth = Math.max(0, Number(q.rows?.[0]?.queued || 0));
      queueRunning = Math.max(0, Number(q.rows?.[0]?.running || 0));
      queueDead = Math.max(0, Number(q.rows?.[0]?.dead || 0));
      const b = await client.query(
        `
        SELECT COUNT(*)::int AS c
        FROM ${busQn}
        WHERE status = 'new'
          AND channel = 'workflow_event'
        `
      );
      dependencyBacklog = Math.max(0, Number(b.rows?.[0]?.c || 0));
      const w = await client.query(
        `
        SELECT COUNT(*)::int AS c
        FROM ${workerQn}
        WHERE lease_until >= now()
        `
      );
      activeWorkers = Math.max(0, Number(w.rows?.[0]?.c || 0));
    } finally {
      client.release();
    }
  } catch {
    // ignore metric read errors for state endpoint
  }
  return res.json({
    ok: true,
    enabled: schedulerState.enabled,
    tick_ms: schedulerState.tickMs,
    running: schedulerState.running,
    active_runs: schedulerState.activeRuns.size,
    active_run_desks: Array.from(schedulerState.activeRuns.entries()).map(([run_uid, x]) => ({
      run_uid,
      desk_id: x?.deskId || '',
      start_node_id: x?.startNodeId || '',
      process_key: x?.processKey || '',
      started_at: x?.startedAt || ''
    })),
    last_tick_at: schedulerState.lastTickAt,
    worker_last_tick_at: schedulerState.workerLastTickAt,
    last_discovery_count: schedulerState.lastDiscoveryCount,
    last_scheduled_count: schedulerState.lastScheduledCount,
    last_error: schedulerState.lastError,
    worker_last_error: schedulerState.workerLastError,
    worker_claimed_jobs: schedulerState.workerClaimedJobs,
    worker_processed_jobs: schedulerState.workerProcessedJobs,
    metrics: {
      queue_depth: queueDepth,
      queue_running: queueRunning,
      queue_dead_letter: queueDead,
      dependency_event_backlog: dependencyBacklog,
      active_workers: activeWorkers
    }
  });
});

workflowAutomationRouter.post('/workflow-runs/trigger', requireDataAdmin, async (req, res) => {
  return triggerProcessRunsHandler(req, res);
});

workflowAutomationRouter.get('/workflow-runs', requireDataAdmin, async (req, res) => {
  return listProcessRunsHandler(req, res);
});

workflowAutomationRouter.get('/workflow-runs/:run_uid', requireDataAdmin, async (req, res) => {
  return getProcessRunHandler(req, res);
});

function boolFromAny(value, fallback = true) {
  if (value === undefined || value === null || value === '') return fallback;
  if (typeof value === 'boolean') return value;
  const txt = String(value).trim().toLowerCase();
  if (txt === '1' || txt === 'true' || txt === 'yes' || txt === 'on') return true;
  if (txt === '0' || txt === 'false' || txt === 'no' || txt === 'off') return false;
  return fallback;
}

function normalizeTriggerType(raw) {
  const v = String(raw || '').trim().toLowerCase();
  if (v === 'manual' || v === 'interval' || v === 'cron') return v;
  return 'interval';
}

function normalizeRunPolicy(raw) {
  const v = String(raw || '').trim().toLowerCase();
  if (v === 'allow_parallel' || v === 'parallel') return 'allow_parallel';
  return 'single_instance';
}

function normalizeGraphNode(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const id = String(raw.id || '').trim();
  if (!id) return null;
  return {
    id,
    type: String(raw.type || '').trim() || 'tool',
    config: raw.config && typeof raw.config === 'object' ? raw.config : {}
  };
}

function normalizeGraphEdge(raw, validNodeIds) {
  if (!raw || typeof raw !== 'object') return null;
  const from = String(raw.from || '').trim();
  const to = String(raw.to || '').trim();
  if (!from || !to || !validNodeIds.has(from) || !validNodeIds.has(to)) return null;
  return { from, to };
}

function parseGraph(graphJson) {
  const src = graphJson && typeof graphJson === 'object' ? graphJson : {};
  const nodes = Array.isArray(src.nodes) ? src.nodes.map(normalizeGraphNode).filter(Boolean) : [];
  const nodeIds = new Set(nodes.map((n) => n.id));
  const edges = Array.isArray(src.edges) ? src.edges.map((e) => normalizeGraphEdge(e, nodeIds)).filter(Boolean) : [];
  return { nodes, edges };
}

function buildReachableSubgraph(nodes, edges, startNodeId) {
  const byFrom = new Map();
  (edges || []).forEach((e) => {
    if (!byFrom.has(e.from)) byFrom.set(e.from, []);
    byFrom.get(e.from).push(e.to);
  });
  const queue = [startNodeId];
  const visited = new Set([startNodeId]);
  while (queue.length) {
    const cur = String(queue.shift() || '');
    const next = byFrom.get(cur) || [];
    next.forEach((to) => {
      if (visited.has(to)) return;
      visited.add(to);
      queue.push(to);
    });
  }
  const subNodes = (nodes || []).filter((n) => visited.has(n.id));
  const subEdges = (edges || []).filter((e) => visited.has(e.from) && visited.has(e.to));
  const order = topoSort(subNodes, subEdges);
  if (!order) throw new Error(`process_subgraph_cycle:${startNodeId}`);
  return { nodes: subNodes, edges: subEdges, order };
}

function processConfigFromStartNode(startNode, deskId, override = null) {
  const settings = startNode?.config?.settings && typeof startNode.config.settings === 'object' ? startNode.config.settings : {};
  const triggerType = normalizeTriggerType(override?.trigger_type || settings?.triggerType || settings?.trigger_type || 'interval');
  const intervalValue = Math.max(1, Math.trunc(Number(settings?.intervalValue || settings?.interval_value || 1)));
  const intervalUnit = String(settings?.intervalUnit || settings?.interval_unit || 'minutes').trim().toLowerCase() || 'minutes';
  const cronExpr = String(override?.schedule_value || settings?.cron || settings?.scheduleValue || '').trim();
  const scheduleValue = triggerType === 'cron' ? cronExpr || '0 * * * *' : `${intervalValue} ${intervalUnit}`;
  const processCode = String(
    override?.process_code || settings?.processCode || settings?.process_code || `desk_${deskId}_start_${startNode.id}`
  ).trim();
  return {
    start_node_id: String(startNode.id || '').trim(),
    name: String(startNode?.config?.name || 'Start').trim() || 'Start',
    tenant_id: String(override?.tenant_id || settings?.tenantId || settings?.tenant_id || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID,
    client_id: String(override?.client_id || settings?.clientId || settings?.client_id || '').trim(),
    is_enabled: boolFromAny(override?.is_enabled, boolFromAny(settings?.isEnabled ?? settings?.enabled ?? settings?.is_enabled, true)),
    trigger_type: triggerType,
    schedule_value: scheduleValue,
    timezone: String(override?.timezone || settings?.timezone || 'UTC').trim() || 'UTC',
    run_policy: normalizeRunPolicy(override?.run_policy || settings?.runPolicy || settings?.run_policy || 'single_instance'),
    process_code: processCode || `desk_${deskId}_start_${startNode.id}`,
    input_scope: String(override?.input_scope || settings?.inputScope || settings?.input_scope || '').trim(),
    output_scope: String(override?.output_scope || settings?.outputScope || settings?.output_scope || ''),
    interval_value: intervalValue,
    interval_unit: intervalUnit,
    cron_expr: cronExpr || ''
  };
}

function buildOverridesMap(rows) {
  const map = new Map();
  (Array.isArray(rows) ? rows : []).forEach((r) => {
    const nodeId = String(r?.start_node_id || '').trim();
    if (!nodeId) return;
    map.set(nodeId, {
      start_node_id: nodeId,
      tenant_id: String(r?.tenant_id || '').trim(),
      client_id: String(r?.client_id || '').trim(),
      is_enabled: r?.is_enabled,
      trigger_type: String(r?.trigger_type || '').trim(),
      schedule_value: String(r?.schedule_value || '').trim(),
      timezone: String(r?.timezone || '').trim(),
      run_policy: String(r?.run_policy || '').trim(),
      process_code: String(r?.process_code || '').trim(),
      input_scope: String(r?.input_scope || '').trim(),
      output_scope: String(r?.output_scope || '').trim()
    });
  });
  return map;
}

function discoverProcessesFromGraph({ deskId, deskVersionId, versionNo, graphJson, overrideRows }) {
  const { nodes, edges } = parseGraph(graphJson);
  const overrides = buildOverridesMap(overrideRows);
  const starts = nodes.filter(
    (n) => n?.type === 'tool' && String(n?.config?.toolType || '').trim().toLowerCase() === 'start_process'
  );
  const processes = [];
  for (const startNode of starts) {
    const processCfg = processConfigFromStartNode(startNode, deskId, overrides.get(startNode.id) || null);
    const subgraph = buildReachableSubgraph(nodes, edges, startNode.id);
    processes.push({
      desk_id: Math.trunc(Number(deskId || 0)),
      desk_version_id: Math.trunc(Number(deskVersionId || 0)),
      version_no: Math.trunc(Number(versionNo || 0)),
      process_key: `${Math.trunc(Number(deskId || 0))}:${String(startNode.id || '').trim()}`,
      ...processCfg,
      subgraph
    });
  }
  return processes;
}

async function loadProcessOverrides(client, config, deskId) {
  const qn = workflowProcessOverridesQname(config);
  const r = await client.query(
    `
    SELECT desk_id, start_node_id, tenant_id, client_id, is_enabled, trigger_type, schedule_value, timezone, run_policy, process_code, input_scope, output_scope
    FROM ${qn}
    WHERE desk_id = $1
    `,
    [Math.trunc(Number(deskId || 0))]
  );
  return Array.isArray(r.rows) ? r.rows : [];
}

async function loadPublishedDeskById(client, config, deskId) {
  const desksQn = workflowDesksQname(config);
  const versionsQn = workflowDeskVersionsQname(config);
  const r = await client.query(
    `
    SELECT
      d.id AS desk_id,
      d.desk_name,
      d.desk_type,
      d.revision AS desk_revision,
      v.id AS desk_version_id,
      v.version_no,
      v.graph_json
    FROM ${desksQn} d
    JOIN ${versionsQn} v
      ON v.desk_id = d.id
    WHERE d.id = $1
      AND d.is_active = true
      AND d.desk_type = 'data'
      AND v.is_published = true
    ORDER BY v.version_no DESC
    LIMIT 1
    `,
    [Math.trunc(Number(deskId || 0))]
  );
  return r.rows?.[0] || null;
}

async function listPublishedDesks(client, config, limit = 500) {
  const desksQn = workflowDesksQname(config);
  const versionsQn = workflowDeskVersionsQname(config);
  const r = await client.query(
    `
    SELECT
      d.id AS desk_id,
      d.desk_name,
      d.desk_type,
      d.revision AS desk_revision,
      v.id AS desk_version_id,
      v.version_no,
      v.graph_json
    FROM ${desksQn} d
    JOIN ${versionsQn} v
      ON v.desk_id = d.id
    WHERE d.is_active = true
      AND d.desk_type = 'data'
      AND v.is_published = true
    ORDER BY d.id ASC
    LIMIT $1
    `,
    [Math.max(1, Math.min(2000, Math.trunc(Number(limit || 500))))]
  );
  return Array.isArray(r.rows) ? r.rows : [];
}

async function lastProcessRunStartedAt(client, config, deskId, startNodeId, triggerType) {
  const qn = workflowRunsQname(config);
  const r = await client.query(
    `
    SELECT started_at
    FROM ${qn}
    WHERE desk_id = $1
      AND start_node_id = $2
      AND trigger_type = $3
    ORDER BY started_at DESC
    LIMIT 1
    `,
    [Math.trunc(Number(deskId || 0)), String(startNodeId || '').trim(), String(triggerType || '').trim()]
  );
  return r.rows?.[0]?.started_at || null;
}

async function insertProcessRunRow(client, config, payload) {
  const qn = workflowRunsQname(config);
  await client.query(
    `
    INSERT INTO ${qn}
      (
        run_uid,
        tenant_id,
        client_id,
        desk_id,
        desk_name,
        desk_version_id,
        start_node_id,
        process_code,
        run_policy,
        orchestration_mode,
        trigger_source,
        trigger_type,
        trigger_key,
        trigger_meta,
        status,
        started_at,
        created_by,
        updated_at
      )
    VALUES
      ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb, $15, now(), $16, now())
    `,
    [
      String(payload.run_uid || '').trim(),
      String(payload.tenant_id || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID,
      String(payload.client_id || '').trim(),
      Math.trunc(Number(payload.desk_id || 0)),
      String(payload.desk_name || '').trim(),
      Math.trunc(Number(payload.desk_version_id || 0)),
      String(payload.start_node_id || '').trim(),
      String(payload.process_code || '').trim(),
      String(payload.run_policy || 'single_instance').trim(),
      String(payload.orchestration_mode || 'queue_chunks').trim(),
      String(payload.trigger_source || 'manual').trim(),
      String(payload.trigger_type || 'manual').trim(),
      String(payload.trigger_key || '').trim(),
      JSON.stringify(payload.trigger_meta || {}),
      String(payload.status || 'running').trim(),
      String(payload.created_by || 'system').trim()
    ]
  );
}

async function insertProcessStepRow(client, config, payload) {
  const qn = workflowRunStepsQname(config);
  await client.query(
    `
    INSERT INTO ${qn}
      (
        run_uid,
        step_no,
        step_order,
        node_id,
        node_name,
        node_type,
        status,
        started_at,
        finished_at,
        duration_ms,
        input_json,
        output_json,
        request_payload,
        response_payload,
        metrics_json,
        error_text
      )
    VALUES
      ($1, $2, $3, $4, $5, $6, $7, $8::timestamptz, $9::timestamptz, $10, $11::jsonb, $12::jsonb, $13::jsonb, $14::jsonb, $15::jsonb, $16)
    `,
    [
      String(payload?.run_uid || '').trim(),
      Math.max(1, Math.trunc(Number(payload?.step_order || 1))),
      Math.max(1, Math.trunc(Number(payload?.step_order || 1))),
      String(payload?.node_id || '').trim(),
      String(payload?.node_name || '').trim(),
      String(payload?.node_type || '').trim(),
      String(payload?.status || 'ok').trim(),
      payload?.started_at ? String(payload.started_at) : new Date().toISOString(),
      payload?.finished_at ? String(payload.finished_at) : new Date().toISOString(),
      Math.max(0, Math.trunc(Number(payload?.duration_ms || 0))),
      JSON.stringify(payload?.input_json || {}),
      JSON.stringify(payload?.output_json || {}),
      JSON.stringify(payload?.request_payload || {}),
      JSON.stringify(payload?.response_payload || {}),
      JSON.stringify(payload?.metrics_json || {}),
      String(payload?.error_text || '').trim()
    ]
  );
}

async function acquireProcessLock(client, config, lockData) {
  const qn = workflowProcessLocksQname(config);
  const deskId = Math.trunc(Number(lockData?.desk_id || 0));
  const startNodeId = String(lockData?.start_node_id || '').trim();
  const lockKey = String(lockData?.lock_key || `${deskId}:${startNodeId}`).trim();
  const ownerId = String(lockData?.owner_id || '').trim();
  if (!deskId || !startNodeId || !ownerId) return false;
  const lockUntil = new Date(Date.now() + Math.max(30_000, Number(lockData?.ttl_ms || PROCESS_LOCK_TTL_MS))).toISOString();
  const r = await client.query(
    `
    INSERT INTO ${qn} (desk_id, start_node_id, lock_key, locked_at, locked_until, owner_id)
    VALUES ($1, $2, $3, now(), $4::timestamptz, $5)
    ON CONFLICT (desk_id, start_node_id) DO UPDATE
    SET lock_key = EXCLUDED.lock_key,
        locked_at = EXCLUDED.locked_at,
        locked_until = EXCLUDED.locked_until,
        owner_id = EXCLUDED.owner_id
    WHERE ${qn}.locked_until <= now() OR ${qn}.owner_id = EXCLUDED.owner_id
    RETURNING desk_id
    `,
    [deskId, startNodeId, lockKey, lockUntil, ownerId]
  );
  return Boolean(r.rows?.length);
}

async function releaseProcessLock(client, config, lockData) {
  const qn = workflowProcessLocksQname(config);
  await client.query(
    `
    UPDATE ${qn}
    SET lock_key = '',
        locked_at = now(),
        locked_until = now(),
        owner_id = ''
    WHERE desk_id = $1
      AND start_node_id = $2
      AND owner_id = $3
    `,
    [
      Math.trunc(Number(lockData?.desk_id || 0)),
      String(lockData?.start_node_id || '').trim(),
      String(lockData?.owner_id || '').trim()
    ]
  );
}

async function writeBusMessage(client, config, payload) {
  const qn = workflowProcessBusQname(config);
  const r = await client.query(
    `
    INSERT INTO ${qn} (desk_id, run_uid, process_code, channel, payload_json, status, created_at)
    VALUES ($1, $2, $3, $4, $5::jsonb, 'new', now())
    RETURNING id
    `,
    [
      Math.trunc(Number(payload?.desk_id || 0)),
      String(payload?.run_uid || '').trim(),
      String(payload?.process_code || '').trim(),
      String(payload?.channel || '').trim(),
      JSON.stringify(payload?.payload_json || {})
    ]
  );
  return Number(r.rows?.[0]?.id || 0);
}

async function readBusMessages(client, config, { desk_id, channel, run_uid, limit = 100, consume = true }) {
  const qn = workflowProcessBusQname(config);
  const rowsRes = await client.query(
    `
    SELECT id, payload_json, run_uid, process_code, channel, created_at
    FROM ${qn}
    WHERE desk_id = $1
      AND status = 'new'
      AND ($2 = '' OR channel = $2)
    ORDER BY id ASC
    LIMIT $3
    `,
    [Math.trunc(Number(desk_id || 0)), String(channel || '').trim(), Math.max(1, Math.min(5000, Math.trunc(Number(limit || 100))))]
  );
  const rows = Array.isArray(rowsRes.rows) ? rowsRes.rows : [];
  if (consume && rows.length) {
    const ids = rows.map((x) => Number(x.id || 0)).filter((x) => Number.isFinite(x) && x > 0);
    if (ids.length) {
      await client.query(
        `
        UPDATE ${qn}
        SET status = 'consumed',
            consumed_at = now(),
            consumed_by = $2
        WHERE id = ANY($1::bigint[])
        `,
        [ids, String(run_uid || '').trim()]
      );
    }
  }
  return rows;
}

async function executeTableParserNode(client, config, processCtx, node) {
  const settings = node?.config?.settings && typeof node.config.settings === 'object' ? node.config.settings : {};
  const sourceSchema = String(settings.sourceSchema || settings.source_schema || '').trim();
  const sourceTable = String(settings.sourceTable || settings.source_table || '').trim();
  const channel = String(settings.channel || sourceTable || processCtx.process_code || '').trim();
  const limit = Math.max(1, Math.min(5000, Math.trunc(Number(settings.limit || 100))));
  if (isIdent(sourceSchema) && isIdent(sourceTable)) {
    const qn = `${qi(sourceSchema)}.${qi(sourceTable)}`;
    const r = await client.query(`SELECT * FROM ${qn} LIMIT ${limit}`);
    return {
      output: { rows: r.rows || [], source: `${sourceSchema}.${sourceTable}` },
      metrics: { rows: Number(r.rows?.length || 0), source_type: 'table' }
    };
  }
  const consume = boolFromAny(settings.consume, true);
  const messages = await readBusMessages(client, config, {
    desk_id: processCtx.desk_id,
    channel,
    run_uid: processCtx.run_uid,
    limit,
    consume
  });
  return {
    output: {
      rows: messages.map((m) => m?.payload_json).filter((x) => x !== undefined),
      source: 'process_bus',
      channel
    },
    metrics: { rows: messages.length, source_type: 'process_bus', channel }
  };
}

async function executeDbWriteNode(client, config, processCtx, node, inputValue) {
  const settings = node?.config?.settings && typeof node.config.settings === 'object' ? node.config.settings : {};
  const targetSchema = String(settings.targetSchema || settings.target_schema || '').trim();
  const targetTable = String(settings.targetTable || settings.target_table || '').trim();
  const channel = String(settings.channel || targetTable || processCtx.process_code || '').trim();
  if (isIdent(targetSchema) && isIdent(targetTable)) {
    const exists = await tableExists(client, targetSchema, targetTable);
    if (exists) {
      const qn = `${qi(targetSchema)}.${qi(targetTable)}`;
      try {
        await client.query(
          `
          INSERT INTO ${qn} (payload_json)
          VALUES ($1::jsonb)
          `,
          [JSON.stringify(inputValue === undefined ? {} : inputValue)]
        );
        return {
          output: { wrote: 1, target: `${targetSchema}.${targetTable}` },
          metrics: { wrote: 1, target_type: 'table' }
        };
      } catch {
        // fallback to process_bus when target table does not support payload_json
      }
    }
  }
  const id = await writeBusMessage(client, config, {
    desk_id: processCtx.desk_id,
    run_uid: processCtx.run_uid,
    process_code: processCtx.process_code,
    channel,
    payload_json: inputValue === undefined ? {} : inputValue
  });
  return {
    output: { wrote: 1, target: 'process_bus', message_id: id, channel },
    metrics: { wrote: 1, target_type: 'process_bus', channel }
  };
}

async function executeProcessNode(client, config, processCtx, node, templates, inputValue) {
  if (isApiNode(node) || isApiToolNode(node)) {
    const refs = nodeTemplateRefs(node);
    const template = templates.find((tpl) => refs.some((ref) => sourceMatchesTemplateRef(tpl, ref)));
    if (!template) throw new Error(`api_template_not_found:${requestSignature(node)}`);
    const exec = await executeApiNode(client, node, template);
    return {
      output: exec.responsePreview || {},
      metrics: exec.metrics || {},
      request_payload: exec.requestPreview || {},
      response_payload: exec.responsePreview || {}
    };
  }
  const toolType = String(node?.config?.toolType || '').trim().toLowerCase();
  if (toolType === 'start_process') {
    return {
      output: {
        desk_id: processCtx.desk_id,
        desk_version_id: processCtx.desk_version_id,
        start_node_id: processCtx.start_node_id,
        process_code: processCtx.process_code,
        run_uid: processCtx.run_uid
      },
      metrics: { start: true },
      request_payload: {},
      response_payload: {}
    };
  }
  if (toolType === 'table_parser') {
    const parser = await executeTableParserNode(client, config, processCtx, node);
    return {
      output: parser.output,
      metrics: parser.metrics,
      request_payload: { mode: 'table_parser' },
      response_payload: parser.output
    };
  }
  if (toolType === 'db_write') {
    const write = await executeDbWriteNode(client, config, processCtx, node, inputValue);
    return {
      output: write.output,
      metrics: write.metrics,
      request_payload: { mode: 'db_write' },
      response_payload: write.output
    };
  }
  if (toolType === 'end_process') {
    return {
      output: { end: true, result: inputValue ?? null },
      metrics: { end: true },
      request_payload: {},
      response_payload: { end: true }
    };
  }
  return {
    output: inputValue ?? {},
    metrics: { skipped: true, node_type: toolType || node?.type || 'unknown' },
    request_payload: {},
    response_payload: { skipped: true }
  };
}

async function executeProcessRunV2(client, config, deskRow, processDef, runMeta) {
  const runUid = String(runMeta?.run_uid || '').trim();
  const processCtx = {
    run_uid: runUid,
    desk_id: Math.trunc(Number(deskRow?.desk_id || 0)),
    desk_version_id: Math.trunc(Number(deskRow?.desk_version_id || 0)),
    start_node_id: String(processDef?.start_node_id || ''),
    process_code: String(processDef?.process_code || '')
  };
  const startTs = Date.now();
  const templates = await loadActiveApiConfigs(client, config);
  const order = Array.isArray(processDef?.subgraph?.order) ? processDef.subgraph.order : [];
  if (!order.length) throw new Error('process_subgraph_empty');

  let stepNo = 0;
  let totalRequests = 0;
  let totalSuccess = 0;
  let totalFailed = 0;
  let totalPayload = 0;
  let stepErrors = 0;
  let lastOutput = null;

  for (const node of order) {
    stepNo += 1;
    const stepStarted = Date.now();
    let status = 'ok';
    let reqPayload = {};
    let respPayload = {};
    let outputValue = {};
    let metrics = {};
    let errorText = '';
    try {
      const exec = await executeProcessNode(client, config, processCtx, node, templates, lastOutput);
      reqPayload = exec.request_payload || {};
      respPayload = exec.response_payload || {};
      outputValue = exec.output ?? {};
      metrics = exec.metrics || {};
      totalRequests += Number(metrics?.requestCount || 0);
      totalSuccess += Number(metrics?.success || 0);
      totalFailed += Number(metrics?.failed || 0);
      totalPayload += Number(metrics?.payloadCount || 0);
      if (Number(metrics?.failed || 0) > 0) status = 'warn';
      lastOutput = outputValue;
    } catch (e) {
      status = 'error';
      errorText = String(e?.message || e || 'step_failed');
      respPayload = { error: errorText };
      outputValue = { error: errorText };
      metrics = { failed: true };
      stepErrors += 1;
      lastOutput = outputValue;
    }
    const stepDuration = Date.now() - stepStarted;
    await insertProcessStepRow(client, config, {
      run_uid: runUid,
      step_order: stepNo,
      node_id: String(node?.id || ''),
      node_name: String(node?.config?.name || node?.id || '').trim(),
      node_type: String(node?.config?.toolType || node?.type || '').trim(),
      status,
      started_at: new Date(stepStarted).toISOString(),
      finished_at: new Date(stepStarted + stepDuration).toISOString(),
      duration_ms: stepDuration,
      input_json: reqPayload,
      output_json: outputValue,
      request_payload: reqPayload,
      response_payload: respPayload,
      metrics_json: metrics,
      error_text: errorText
    });
    if (status === 'error') break;
  }

  const durationMs = Date.now() - startTs;
  const hasErrors = totalFailed > 0 || stepErrors > 0;
  return {
    status: hasErrors ? 'completed_with_errors' : 'completed',
    summary_json: {
      desk_id: Number(deskRow?.desk_id || 0),
      desk_version_id: Number(deskRow?.desk_version_id || 0),
      start_node_id: String(processDef?.start_node_id || ''),
      process_code: String(processDef?.process_code || ''),
      nodes_total: order.length,
      total_requests: totalRequests,
      success: totalSuccess,
      failed: totalFailed,
      payload_count: totalPayload,
      step_errors: stepErrors
    },
    error_text: '',
    duration_ms: durationMs
  };
}

function reserveRunSlotForProcess(processKey, runUid, meta = {}) {
  if (schedulerState.activeRuns.size >= MAX_PARALLEL_RUNS) return false;
  const allowParallel = normalizeRunPolicy(meta?.run_policy) === 'allow_parallel';
  const pKey = String(processKey || '').trim();
  if (!allowParallel) {
    for (const item of schedulerState.activeRuns.values()) {
      if (String(item?.processKey || '') === pKey) return false;
    }
  }
  schedulerState.activeRuns.set(runUid, {
    processKey: pKey,
    deskId: String(meta?.desk_id || ''),
    startNodeId: String(meta?.start_node_id || ''),
    startedAt: new Date().toISOString()
  });
  return true;
}

function releaseRunSlotForProcess(runUid) {
  schedulerState.activeRuns.delete(String(runUid || '').trim());
}

async function runProcessByDefinitionV2(runInput) {
  const runUid = String(runInput?.run_uid || buildRunUid('wf_run')).trim() || buildRunUid('wf_run');
  const client = await pool.connect();
  let config = { ...DEFAULT_CONFIG };
  let lockAcquired = false;
  let runInserted = false;
  try {
    config = await loadRuntimeStorageConfig(client);
    await ensureWorkflowAutomationTables(client, config);
    const deskRow = runInput?.desk_row || {};
    const process = runInput?.process || {};
    const runPolicy = normalizeRunPolicy(process?.run_policy);
    if (runPolicy === 'single_instance') {
      lockAcquired = await acquireProcessLock(client, config, {
        desk_id: Number(deskRow?.desk_id || 0),
        start_node_id: String(process?.start_node_id || ''),
        lock_key: String(process?.process_key || `${deskRow?.desk_id}:${process?.start_node_id}`),
        owner_id: runUid,
        ttl_ms: PROCESS_LOCK_TTL_MS
      });
      if (!lockAcquired) {
        return {
          run_uid: runUid,
          status: 'skipped',
          summary_json: { reason: 'single_instance_lock' },
          error_text: '',
          duration_ms: 0
        };
      }
    }

    await insertProcessRunRow(client, config, {
      run_uid: runUid,
      desk_id: Number(deskRow?.desk_id || 0),
      desk_name: String(deskRow?.desk_name || ''),
      desk_version_id: Number(deskRow?.desk_version_id || 0),
      start_node_id: String(process?.start_node_id || ''),
      process_code: String(process?.process_code || ''),
      run_policy: runPolicy,
      trigger_source: String(runInput?.trigger_source || 'manual'),
      trigger_type: String(runInput?.trigger_type || 'manual'),
      trigger_key: String(runInput?.trigger_key || ''),
      trigger_meta: runInput?.trigger_meta || {},
      created_by: String(runInput?.created_by || 'system')
    });
    runInserted = true;

    const result = await executeProcessRunV2(client, config, deskRow, process, { run_uid: runUid });
    await updateRunRow(client, config, runUid, {
      status: result.status,
      finished_at: new Date().toISOString(),
      duration_ms: Number(result.duration_ms || 0),
      summary_json: result.summary_json || {},
      error_text: result.error_text || ''
    });
    return { run_uid: runUid, ...result };
  } catch (e) {
    const errorText = String(e?.message || e || 'run_failed');
    if (runInserted) {
      try {
        await updateRunRow(client, config, runUid, {
          status: 'failed',
          finished_at: new Date().toISOString(),
          duration_ms: 0,
          summary_json: {},
          error_text: errorText
        });
      } catch {
        // ignore update failure
      }
    }
    return {
      run_uid: runUid,
      status: 'failed',
      summary_json: {},
      error_text: errorText,
      duration_ms: 0
    };
  } finally {
    try {
      if (lockAcquired) {
        await releaseProcessLock(client, config, {
          desk_id: Number(runInput?.desk_row?.desk_id || 0),
          start_node_id: String(runInput?.process?.start_node_id || ''),
          owner_id: runUid
        });
      }
    } catch {
      // ignore unlock errors
    }
    client.release();
  }
}

function launchProcessRunV2(runInput) {
  const runUid = String(runInput?.run_uid || buildRunUid('wf_run')).trim() || buildRunUid('wf_run');
  const process = runInput?.process || {};
  const deskRow = runInput?.desk_row || {};
  const processKey = String(process?.process_key || `${deskRow?.desk_id}:${process?.start_node_id}`);
  if (
    !reserveRunSlotForProcess(processKey, runUid, {
      run_policy: process?.run_policy,
      desk_id: deskRow?.desk_id,
      start_node_id: process?.start_node_id
    })
  ) {
    return {
      accepted: false,
      run_uid: '',
      promise: Promise.resolve({
        run_uid: '',
        status: 'skipped',
        summary_json: { reason: 'parallel_limit_or_process_busy' },
        error_text: '',
        duration_ms: 0
      })
    };
  }
  const promise = runProcessByDefinitionV2({ ...runInput, run_uid: runUid }).finally(() => {
    releaseRunSlotForProcess(runUid);
  });
  return { accepted: true, run_uid: runUid, promise };
}

async function discoverDeskProcesses(client, config, deskRow) {
  const overrides = await loadProcessOverrides(client, config, Number(deskRow?.desk_id || 0));
  return discoverProcessesFromGraph({
    deskId: Number(deskRow?.desk_id || 0),
    deskVersionId: Number(deskRow?.desk_version_id || 0),
    versionNo: Number(deskRow?.version_no || 0),
    graphJson: deskRow?.graph_json || {},
    overrideRows: overrides
  });
}

async function schedulerTickPublishedProcesses() {
  if (!schedulerState.enabled || schedulerState.running) return;
  schedulerState.running = true;
  schedulerState.lastTickAt = new Date().toISOString();
  schedulerState.lastError = '';
  schedulerState.lastDiscoveryCount = 0;
  schedulerState.lastScheduledCount = 0;

  let client = null;
  try {
    client = await pool.connect();
    await withSchedulerLock(client, async () => {
      const config = await loadRuntimeStorageConfig(client);
      await ensureWorkflowAutomationTables(client, config);
      await upsertSchedulerLease(client, config, {
        owner_id: SCHEDULER_ID,
        ttl_ms: Math.max(schedulerState.tickMs * 3, 30_000),
        meta_json: { tick_ms: schedulerState.tickMs, phase: 'scheduling' }
      });
      const desks = await listPublishedDesks(client, config, 500);
      let discovered = 0;
      let scheduled = 0;
      const now = new Date();
      const minuteSlot = new Date(now);
      minuteSlot.setSeconds(0, 0);
      for (const desk of desks) {
        const processes = await discoverDeskProcesses(client, config, desk);
        discovered += processes.length;
        for (const process of processes) {
          if (!process?.is_enabled) continue;
          const triggerType = normalizeTriggerType(process?.trigger_type);
          if (triggerType === 'manual') continue;
          let due = false;
          if (triggerType === 'interval') {
            const last = await lastProcessRunStartedAt(client, config, desk.desk_id, process.start_node_id, 'interval');
            const lastTs = last ? new Date(last).getTime() : 0;
            const intervalMs = parseIntervalToMs(process.interval_value, process.interval_unit);
            due = !lastTs || Date.now() - lastTs >= intervalMs;
          } else if (triggerType === 'cron') {
            if (cronMatchesNow(process.cron_expr || process.schedule_value || '', now)) {
              const last = await lastProcessRunStartedAt(client, config, desk.desk_id, process.start_node_id, 'cron');
              const lastSlot = last ? new Date(last) : null;
              due = !lastSlot || lastSlot.getTime() < minuteSlot.getTime();
            }
          }
          if (!due) continue;
          const queued = await enqueueProcessRunQueued(client, config, {
            desk_row: desk,
            process,
            trigger_source: triggerType === 'interval' ? 'schedule_interval' : 'schedule_cron',
            trigger_type: triggerType,
            trigger_key: `${triggerType}:${desk.desk_id}:${process.start_node_id}`,
            trigger_meta: {
              desk_id: Number(desk?.desk_id || 0),
              desk_version_id: Number(desk?.desk_version_id || 0),
              start_node_id: String(process?.start_node_id || ''),
              process_code: String(process?.process_code || ''),
              schedule_value: String(process?.schedule_value || '')
            },
            created_by: 'scheduler',
            tenant_id: String(process?.tenant_id || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID,
            client_id: String(process?.client_id || '').trim()
          });
          if (queued.accepted) scheduled += 1;
        }
      }
      const dependencyEventsProcessed = await processDependencyEvents(client, config);
      const workerProcessed = await processQueuedJobsTick(client, config);
      await upsertWorkerLease(client, config, {
        worker_id: WORKER_ID,
        owner_id: WORKER_ID,
        status: 'running',
        ttl_ms: Math.max(schedulerState.tickMs * 3, 30_000),
        active_jobs: 0,
        meta_json: {
          processed_jobs: workerProcessed,
          dependency_events: dependencyEventsProcessed
        }
      });
      schedulerState.workerLastTickAt = new Date().toISOString();
      schedulerState.lastDiscoveryCount = discovered;
      schedulerState.lastScheduledCount = scheduled;
    });
  } catch (e) {
    schedulerState.lastError = String(e?.message || e || 'scheduler_tick_failed');
    schedulerState.workerLastError = schedulerState.lastError;
  } finally {
    schedulerState.running = false;
    if (client) client.release();
  }
}

async function createPublishedDeskVersion(client, config, deskId, publishedBy = 'data_admin') {
  const desksQn = workflowDesksQname(config);
  const versionsQn = workflowDeskVersionsQname(config);
  const deskRes = await client.query(
    `
    SELECT id, desk_name, desk_type, revision, config_json, is_active
    FROM ${desksQn}
    WHERE id = $1
    LIMIT 1
    `,
    [Math.trunc(Number(deskId || 0))]
  );
  const deskRow = deskRes.rows?.[0];
  if (!deskRow || !deskRow.is_active || String(deskRow?.desk_type || '') !== 'data') {
    throw new Error('desk_not_found_or_inactive');
  }
  const nextVerRes = await client.query(
    `
    SELECT COALESCE(MAX(version_no), 0) + 1 AS next_version
    FROM ${versionsQn}
    WHERE desk_id = $1
    `,
    [Math.trunc(Number(deskId || 0))]
  );
  const versionNo = Math.max(1, Math.trunc(Number(nextVerRes.rows?.[0]?.next_version || 1)));
  const ins = await client.query(
    `
    INSERT INTO ${versionsQn}
      (desk_id, version_no, graph_json, source_revision, is_published, created_at, created_by, published_at, published_by)
    VALUES
      ($1, $2, $3::jsonb, $4, false, now(), $5, null, '')
    RETURNING id, desk_id, version_no
    `,
    [
      Math.trunc(Number(deskId || 0)),
      versionNo,
      JSON.stringify(deskRow?.config_json || {}),
      Math.max(1, Math.trunc(Number(deskRow?.revision || 1))),
      String(publishedBy || 'data_admin')
    ]
  );
  const newId = Number(ins.rows?.[0]?.id || 0);
  await client.query(
    `
    UPDATE ${versionsQn}
    SET is_published = false, published_at = null, published_by = ''
    WHERE desk_id = $1 AND id <> $2
    `,
    [Math.trunc(Number(deskId || 0)), Math.trunc(newId)]
  );
  await client.query(
    `
    UPDATE ${versionsQn}
    SET is_published = true, published_at = now(), published_by = $2
    WHERE id = $1
    `,
    [Math.trunc(newId), String(publishedBy || 'data_admin')]
  );
  return {
    desk_id: Math.trunc(Number(deskId || 0)),
    desk_name: String(deskRow?.desk_name || ''),
    desk_version_id: Math.trunc(newId),
    version_no: versionNo,
    graph_json: deskRow?.config_json || {}
  };
}

async function upsertProcessEnabled(client, config, deskId, startNodeId, enabled, updatedBy = 'data_admin') {
  const qn = workflowProcessOverridesQname(config);
  await client.query(
    `
    INSERT INTO ${qn} (desk_id, start_node_id, is_enabled, updated_at, updated_by)
    VALUES ($1, $2, $3, now(), $4)
    ON CONFLICT (desk_id, start_node_id) DO UPDATE
    SET is_enabled = EXCLUDED.is_enabled,
        updated_at = now(),
        updated_by = EXCLUDED.updated_by
    `,
    [Math.trunc(Number(deskId || 0)), String(startNodeId || '').trim(), Boolean(enabled), String(updatedBy || 'data_admin').trim()]
  );
}

async function listProcessRunsHandler(req, res) {
  const deskId = Number(req.query?.desk_id || 0);
  const startNodeId = String(req.query?.start_node_id || '').trim();
  const status = String(req.query?.status || '').trim();
  const limit = Math.max(1, Math.min(200, Number(req.query?.limit || 50)));
  let client = null;
  try {
    client = await pool.connect();
    const config = await loadRuntimeStorageConfig(client);
    await ensureWorkflowAutomationTables(client, config);
    const qn = workflowRunsQname(config);
    const params = [];
    let whereSql = 'WHERE 1=1';
    if (Number.isFinite(deskId) && deskId > 0) {
      params.push(Math.trunc(deskId));
      whereSql += ` AND desk_id = $${params.length}`;
    }
    if (startNodeId) {
      params.push(startNodeId);
      whereSql += ` AND start_node_id = $${params.length}`;
    }
    if (status) {
      params.push(status);
      whereSql += ` AND status = $${params.length}`;
    }
    params.push(limit);
    const r = await client.query(
      `
      SELECT
        run_uid,
        tenant_id,
        client_id,
        desk_id,
        desk_name,
        desk_version_id,
        start_node_id,
        process_code,
        run_policy,
        orchestration_mode,
        trigger_source,
        trigger_type,
        trigger_key,
        trigger_meta,
        status,
        started_at,
        finished_at,
        duration_ms,
        summary_json,
        error_text
      FROM ${qn}
      ${whereSql}
      ORDER BY started_at DESC
      LIMIT $${params.length}
      `,
      params
    );
    return res.json({ runs: r.rows || [] });
  } catch (e) {
    return res.status(500).json({ error: 'process_runs_list_failed', details: String(e?.message || e) });
  } finally {
    if (client) client.release();
  }
}

async function getProcessRunHandler(req, res) {
  const runUid = String(req.params?.run_uid || '').trim();
  if (!runUid) return res.status(400).json({ error: 'bad_request', details: 'run_uid is required' });
  let client = null;
  try {
    client = await pool.connect();
    const config = await loadRuntimeStorageConfig(client);
    await ensureWorkflowAutomationTables(client, config);
    const runQn = workflowRunsQname(config);
    const stepQn = workflowRunStepsQname(config);
    const runRes = await client.query(
      `
      SELECT
        run_uid,
        tenant_id,
        client_id,
        desk_id,
        desk_name,
        desk_version_id,
        start_node_id,
        process_code,
        run_policy,
        orchestration_mode,
        trigger_source,
        trigger_type,
        trigger_key,
        trigger_meta,
        status,
        started_at,
        finished_at,
        duration_ms,
        summary_json,
        error_text
      FROM ${runQn}
      WHERE run_uid = $1
      LIMIT 1
      `,
      [runUid]
    );
    if (!runRes.rows?.length) return res.status(404).json({ error: 'not_found', details: 'run not found' });
    const stepRes = await client.query(
      `
      SELECT
        id,
        run_uid,
        step_order,
        node_id,
        node_name,
        node_type,
        status,
        started_at,
        finished_at,
        duration_ms,
        input_json,
        output_json,
        metrics_json,
        error_text
      FROM ${stepQn}
      WHERE run_uid = $1
      ORDER BY step_order ASC, id ASC
      `,
      [runUid]
    );
    return res.json({ run: runRes.rows[0], steps: stepRes.rows || [] });
  } catch (e) {
    return res.status(500).json({ error: 'process_run_get_failed', details: String(e?.message || e) });
  } finally {
    if (client) client.release();
  }
}

async function triggerProcessRunsHandler(req, res) {
  const deskId = Number(req.body?.desk_id || 0);
  const startNodeId = String(req.body?.start_node_id || '').trim();
  const wait = req.body?.wait === undefined ? true : Boolean(req.body?.wait);
  if (!Number.isFinite(deskId) || deskId <= 0) {
    return res.status(400).json({ error: 'bad_request', details: 'desk_id is required' });
  }
  let client = null;
  try {
    client = await pool.connect();
    const config = await loadRuntimeStorageConfig(client);
    await ensureWorkflowAutomationTables(client, config);
    const desk = await loadPublishedDeskById(client, config, deskId);
    if (!desk) {
      return res.status(404).json({ error: 'not_found', details: 'published desk version not found' });
    }
    const processes = await discoverDeskProcesses(client, config, desk);
    const selected = startNodeId
      ? processes.filter((p) => String(p?.start_node_id || '') === startNodeId)
      : processes.filter((p) => normalizeTriggerType(p?.trigger_type) === 'manual');
    if (!selected.length) {
      return res.status(404).json({ error: 'not_found', details: 'no matching process for trigger' });
    }
    const accepted = [];
    for (const process of selected.filter((p) => p?.is_enabled)) {
      const queued = await enqueueProcessRunQueued(client, config, {
        desk_row: desk,
        process,
        trigger_source: 'manual',
        trigger_type: 'manual',
        trigger_key: `manual:${deskId}:${process.start_node_id}`,
        trigger_meta: {
          note: String(req.body?.note || '').trim(),
          requested_start_node_id: startNodeId || '',
          desk_id: Math.trunc(Number(deskId || 0)),
          desk_version_id: Number(desk?.desk_version_id || 0)
        },
        created_by: String(req.body?.created_by || 'manual').trim() || 'manual',
        tenant_id: String(process?.tenant_id || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID,
        client_id: String(process?.client_id || '').trim()
      });
      if (queued.accepted) accepted.push(queued);
    }
    if (!accepted.length) {
      return res.status(429).json({ error: 'busy', details: 'parallel_limit_or_process_busy' });
    }
    if (!wait) {
      return res.json({
        ok: true,
        accepted: true,
        runs: accepted.map((x) => ({ run_uid: x.run_uid, status: 'queued' }))
      });
    }
    const results = await waitRunsToFinish(
      client,
      config,
      accepted.map((x) => x.run_uid),
      Math.max(5_000, Math.trunc(Number(req.body?.wait_timeout_ms || 120_000)))
    );
    return res.json({
      ok: true,
      accepted: true,
      runs: results
    });
  } catch (e) {
    return res.status(500).json({ error: 'process_trigger_failed', details: String(e?.message || e) });
  } finally {
    if (client) client.release();
  }
}

async function publishDeskHandler(req, res) {
  const deskId = Number(req.params?.desk_id || 0);
  if (!Number.isFinite(deskId) || deskId <= 0) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid desk_id' });
  }
  let client = null;
  try {
    client = await pool.connect();
    const config = await loadRuntimeStorageConfig(client);
    await ensureWorkflowAutomationTables(client, config);
    const published = await createPublishedDeskVersion(
      client,
      config,
      Math.trunc(deskId),
      String(req.body?.published_by || req.header('X-AO-ROLE') || 'data_admin').trim()
    );
    const processes = await discoverDeskProcesses(client, config, published);
    return res.json({
      ok: true,
      desk_id: Number(published.desk_id || 0),
      desk_version_id: Number(published.desk_version_id || 0),
      version_no: Number(published.version_no || 1),
      processes_total: processes.length,
      starts: processes.map((p) => ({
        start_node_id: p.start_node_id,
        process_code: p.process_code,
        is_enabled: p.is_enabled,
        trigger_type: p.trigger_type
      }))
    });
  } catch (e) {
    return res.status(500).json({ error: 'desk_publish_failed', details: String(e?.message || e) });
  } finally {
    if (client) client.release();
  }
}

async function setProcessEnabledHandler(req, res, enabled) {
  const deskId = Number(req.params?.desk_id || 0);
  const startNodeId = String(req.params?.start_node_id || '').trim();
  if (!Number.isFinite(deskId) || deskId <= 0 || !startNodeId) {
    return res.status(400).json({ error: 'bad_request', details: 'invalid desk_id/start_node_id' });
  }
  let client = null;
  try {
    client = await pool.connect();
    const config = await loadRuntimeStorageConfig(client);
    await ensureWorkflowAutomationTables(client, config);
    const published = await loadPublishedDeskById(client, config, deskId);
    if (!published) {
      return res.status(404).json({ error: 'not_found', details: 'published desk version not found' });
    }
    const processes = await discoverDeskProcesses(client, config, published);
    if (!processes.some((p) => String(p?.start_node_id || '') === startNodeId)) {
      return res.status(404).json({ error: 'not_found', details: 'start node not found in published desk version' });
    }
    await upsertProcessEnabled(
      client,
      config,
      Math.trunc(deskId),
      startNodeId,
      Boolean(enabled),
      String(req.body?.updated_by || req.header('X-AO-ROLE') || 'data_admin').trim()
    );
    return res.json({
      ok: true,
      desk_id: Math.trunc(deskId),
      start_node_id: startNodeId,
      is_enabled: Boolean(enabled)
    });
  } catch (e) {
    return res.status(500).json({ error: 'process_toggle_failed', details: String(e?.message || e) });
  } finally {
    if (client) client.release();
  }
}

async function listWorkflowJobsHandler(req, res) {
  const status = String(req.query?.status || '').trim();
  const runUid = String(req.query?.run_uid || '').trim();
  const tenantId = String(req.query?.tenant_id || '').trim();
  const limit = Math.max(1, Math.min(500, Math.trunc(Number(req.query?.limit || 100))));
  let client = null;
  try {
    client = await pool.connect();
    const config = await loadRuntimeStorageConfig(client);
    await ensureWorkflowAutomationTables(client, config);
    const qn = workflowJobQueueQname(config);
    const params = [];
    let whereSql = 'WHERE 1=1';
    if (status) {
      params.push(status);
      whereSql += ` AND status = $${params.length}`;
    }
    if (runUid) {
      params.push(runUid);
      whereSql += ` AND parent_run_uid = $${params.length}`;
    }
    if (tenantId) {
      params.push(tenantId);
      whereSql += ` AND tenant_id = $${params.length}`;
    }
    params.push(limit);
    const r = await client.query(
      `
      SELECT
        job_id,
        tenant_id,
        client_id,
        desk_id,
        desk_version_id,
        start_node_id,
        process_code,
        parent_run_uid,
        parent_job_id,
        job_type,
        dedupe_key,
        priority,
        status,
        available_at,
        attempt_no,
        max_attempts,
        locked_by,
        locked_until,
        last_error,
        created_at,
        updated_at
      FROM ${qn}
      ${whereSql}
      ORDER BY job_id DESC
      LIMIT $${params.length}
      `,
      params
    );
    return res.json({ jobs: r.rows || [] });
  } catch (e) {
    return res.status(500).json({ error: 'workflow_jobs_list_failed', details: String(e?.message || e) });
  } finally {
    if (client) client.release();
  }
}

async function getWorkflowJobHandler(req, res) {
  const jobId = Math.trunc(Number(req.params?.job_id || 0));
  if (!jobId) return res.status(400).json({ error: 'bad_request', details: 'job_id is required' });
  let client = null;
  try {
    client = await pool.connect();
    const config = await loadRuntimeStorageConfig(client);
    await ensureWorkflowAutomationTables(client, config);
    const qn = workflowJobQueueQname(config);
    const r = await client.query(`SELECT * FROM ${qn} WHERE job_id = $1 LIMIT 1`, [jobId]);
    if (!r.rows?.length) return res.status(404).json({ error: 'not_found', details: 'job not found' });
    return res.json({ job: r.rows[0] });
  } catch (e) {
    return res.status(500).json({ error: 'workflow_job_get_failed', details: String(e?.message || e) });
  } finally {
    if (client) client.release();
  }
}

async function retryDeadJobHandler(req, res) {
  const jobId = Math.trunc(Number(req.body?.job_id || 0));
  if (!jobId) return res.status(400).json({ error: 'bad_request', details: 'job_id is required' });
  let client = null;
  try {
    client = await pool.connect();
    const config = await loadRuntimeStorageConfig(client);
    await ensureWorkflowAutomationTables(client, config);
    const qn = workflowJobQueueQname(config);
    const r = await client.query(
      `
      UPDATE ${qn}
      SET status = 'queued',
          attempt_no = 0,
          available_at = now(),
          locked_by = '',
          locked_until = null,
          last_error = '',
          updated_at = now()
      WHERE job_id = $1
        AND status IN ('dead_letter', 'failed')
      RETURNING job_id
      `,
      [jobId]
    );
    if (!r.rows?.length) return res.status(404).json({ error: 'not_found', details: 'dead/failed job not found' });
    return res.json({ ok: true, job_id: jobId, status: 'queued' });
  } catch (e) {
    return res.status(500).json({ error: 'workflow_job_retry_failed', details: String(e?.message || e) });
  } finally {
    if (client) client.release();
  }
}

async function listDependenciesHandler(req, res) {
  const deskId = Math.trunc(Number(req.query?.desk_id || 0));
  let client = null;
  try {
    client = await pool.connect();
    const config = await loadRuntimeStorageConfig(client);
    await ensureWorkflowAutomationTables(client, config);
    const qn = workflowDependenciesQname(config);
    const params = [];
    let whereSql = 'WHERE 1=1';
    if (deskId > 0) {
      params.push(deskId);
      whereSql += ` AND desk_id = $${params.length}`;
    }
    const r = await client.query(
      `
      SELECT
        id,
        tenant_id,
        desk_id,
        source_start_node_id,
        target_start_node_id,
        trigger_event_type,
        trigger_status,
        condition_json,
        dispatch_mode,
        dedupe_policy,
        is_enabled,
        updated_at,
        updated_by
      FROM ${qn}
      ${whereSql}
      ORDER BY id DESC
      `,
      params
    );
    return res.json({ dependencies: r.rows || [] });
  } catch (e) {
    return res.status(500).json({ error: 'dependencies_list_failed', details: String(e?.message || e) });
  } finally {
    if (client) client.release();
  }
}

async function upsertDependencyHandler(req, res) {
  const body = req.body && typeof req.body === 'object' ? req.body : {};
  const depId = Math.trunc(Number(body?.id || 0));
  const tenantId = String(body?.tenant_id || DEFAULT_TENANT_ID).trim() || DEFAULT_TENANT_ID;
  const deskId = Math.trunc(Number(body?.desk_id || 0));
  const sourceStart = String(body?.source_start_node_id || '').trim();
  const targetStart = String(body?.target_start_node_id || '').trim();
  const triggerEvent = String(body?.trigger_event_type || '').trim().toLowerCase();
  const triggerStatus = String(body?.trigger_status || '').trim().toLowerCase();
  const dispatchMode = dependencyDispatchMode(body?.dispatch_mode);
  const dedupePolicy = dependencyDedupeMode(body?.dedupe_policy);
  const conditionJson = body?.condition_json && typeof body.condition_json === 'object' ? body.condition_json : {};
  const isEnabled = body?.is_enabled === undefined ? true : Boolean(body?.is_enabled);
  const updatedBy = String(body?.updated_by || req.header('X-AO-ROLE') || 'data_admin').trim() || 'data_admin';

  if (!deskId || !sourceStart || !targetStart || !triggerEvent) {
    return res.status(400).json({ error: 'bad_request', details: 'desk_id/source_start_node_id/target_start_node_id/trigger_event_type are required' });
  }
  let client = null;
  try {
    client = await pool.connect();
    const config = await loadRuntimeStorageConfig(client);
    await ensureWorkflowAutomationTables(client, config);
    const qn = workflowDependenciesQname(config);
    if (depId > 0) {
      const r = await client.query(
        `
        UPDATE ${qn}
        SET
          tenant_id = $2,
          desk_id = $3,
          source_start_node_id = $4,
          target_start_node_id = $5,
          trigger_event_type = $6,
          trigger_status = $7,
          condition_json = $8::jsonb,
          dispatch_mode = $9,
          dedupe_policy = $10,
          is_enabled = $11,
          updated_at = now(),
          updated_by = $12
        WHERE id = $1
        RETURNING id
        `,
        [depId, tenantId, deskId, sourceStart, targetStart, triggerEvent, triggerStatus, stableJsonString(conditionJson), dispatchMode, dedupePolicy, isEnabled, updatedBy]
      );
      if (!r.rows?.length) return res.status(404).json({ error: 'not_found', details: 'dependency not found' });
      return res.json({ ok: true, id: depId, updated: true });
    }
    const r = await client.query(
      `
      INSERT INTO ${qn}
        (tenant_id, desk_id, source_start_node_id, target_start_node_id, trigger_event_type, trigger_status, condition_json, dispatch_mode, dedupe_policy, is_enabled, updated_at, updated_by)
      VALUES
        ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, now(), $11)
      RETURNING id
      `,
      [tenantId, deskId, sourceStart, targetStart, triggerEvent, triggerStatus, stableJsonString(conditionJson), dispatchMode, dedupePolicy, isEnabled, updatedBy]
    );
    return res.json({ ok: true, id: Number(r.rows?.[0]?.id || 0), created: true });
  } catch (e) {
    return res.status(500).json({ error: 'dependency_upsert_failed', details: String(e?.message || e) });
  } finally {
    if (client) client.release();
  }
}

async function deleteDependencyHandler(req, res) {
  const depId = Math.trunc(Number(req.body?.id || 0));
  if (!depId) return res.status(400).json({ error: 'bad_request', details: 'id is required' });
  let client = null;
  try {
    client = await pool.connect();
    const config = await loadRuntimeStorageConfig(client);
    await ensureWorkflowAutomationTables(client, config);
    const qn = workflowDependenciesQname(config);
    const r = await client.query(`DELETE FROM ${qn} WHERE id = $1 RETURNING id`, [depId]);
    if (!r.rows?.length) return res.status(404).json({ error: 'not_found', details: 'dependency not found' });
    return res.json({ ok: true, id: depId, deleted: true });
  } catch (e) {
    return res.status(500).json({ error: 'dependency_delete_failed', details: String(e?.message || e) });
  } finally {
    if (client) client.release();
  }
}

async function listRunAggregationHandler(req, res) {
  const runUid = String(req.query?.run_uid || '').trim();
  const deskId = Math.trunc(Number(req.query?.desk_id || 0));
  const limit = Math.max(1, Math.min(200, Math.trunc(Number(req.query?.limit || 50))));
  let client = null;
  try {
    client = await pool.connect();
    const config = await loadRuntimeStorageConfig(client);
    await ensureWorkflowAutomationTables(client, config);
    const qn = workflowRunAggregationQname(config);
    const params = [];
    let whereSql = 'WHERE 1=1';
    if (runUid) {
      params.push(runUid);
      whereSql += ` AND run_uid = $${params.length}`;
    }
    if (deskId > 0) {
      params.push(deskId);
      whereSql += ` AND desk_id = $${params.length}`;
    }
    params.push(limit);
    const r = await client.query(
      `
      SELECT *
      FROM ${qn}
      ${whereSql}
      ORDER BY updated_at DESC
      LIMIT $${params.length}
      `,
      params
    );
    return res.json({ aggregation: r.rows || [] });
  } catch (e) {
    return res.status(500).json({ error: 'run_aggregation_list_failed', details: String(e?.message || e) });
  } finally {
    if (client) client.release();
  }
}

async function listIncrementalStateHandler(req, res) {
  const deskId = Math.trunc(Number(req.query?.desk_id || 0));
  const startNodeId = String(req.query?.start_node_id || '').trim();
  const tenantId = String(req.query?.tenant_id || '').trim();
  const limit = Math.max(1, Math.min(500, Math.trunc(Number(req.query?.limit || 100))));
  let client = null;
  try {
    client = await pool.connect();
    const config = await loadRuntimeStorageConfig(client);
    await ensureWorkflowAutomationTables(client, config);
    const qn = workflowIncrementalStateQname(config);
    const params = [];
    let whereSql = 'WHERE 1=1';
    if (tenantId) {
      params.push(tenantId);
      whereSql += ` AND tenant_id = $${params.length}`;
    }
    if (deskId > 0) {
      params.push(deskId);
      whereSql += ` AND desk_id = $${params.length}`;
    }
    if (startNodeId) {
      params.push(startNodeId);
      whereSql += ` AND start_node_id = $${params.length}`;
    }
    params.push(limit);
    const r = await client.query(
      `
      SELECT *
      FROM ${qn}
      ${whereSql}
      ORDER BY updated_at DESC
      LIMIT $${params.length}
      `,
      params
    );
    return res.json({ incremental_state: r.rows || [] });
  } catch (e) {
    return res.status(500).json({ error: 'incremental_state_list_failed', details: String(e?.message || e) });
  } finally {
    if (client) client.release();
  }
}

workflowAutomationRouter.get('/process-runs', requireDataAdmin, listProcessRunsHandler);
workflowAutomationRouter.get('/process-runs/:run_uid', requireDataAdmin, getProcessRunHandler);
workflowAutomationRouter.post('/process-runs/trigger', requireDataAdmin, triggerProcessRunsHandler);
workflowAutomationRouter.post('/desks/:desk_id/publish', requireDataAdmin, publishDeskHandler);
workflowAutomationRouter.post('/processes/:desk_id/:start_node_id/enable', requireDataAdmin, async (req, res) =>
  setProcessEnabledHandler(req, res, true)
);
workflowAutomationRouter.post('/processes/:desk_id/:start_node_id/disable', requireDataAdmin, async (req, res) =>
  setProcessEnabledHandler(req, res, false)
);
workflowAutomationRouter.get('/workflow-jobs', requireDataAdmin, listWorkflowJobsHandler);
workflowAutomationRouter.get('/workflow-jobs/:job_id', requireDataAdmin, getWorkflowJobHandler);
workflowAutomationRouter.post('/workflow-jobs/retry', requireDataAdmin, retryDeadJobHandler);
workflowAutomationRouter.get('/process-dependencies', requireDataAdmin, listDependenciesHandler);
workflowAutomationRouter.post('/process-dependencies/upsert', requireDataAdmin, upsertDependencyHandler);
workflowAutomationRouter.post('/process-dependencies/delete', requireDataAdmin, deleteDependencyHandler);
workflowAutomationRouter.get('/process-runs/aggregation', requireDataAdmin, listRunAggregationHandler);
workflowAutomationRouter.get('/process-state/incremental', requireDataAdmin, listIncrementalStateHandler);
