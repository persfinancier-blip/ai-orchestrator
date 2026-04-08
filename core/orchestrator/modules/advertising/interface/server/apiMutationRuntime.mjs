import { retryDelayMs, shouldRetryStatus } from '../desk/tabs/apiBuilderRuntimeCore.js';

const DEFAULT_BATCH_SIZE = 50;
const DEFAULT_MAX_ROWS = 500;
const DEFAULT_PREVIEW_LIMIT = 20;
const DEFAULT_TIMEOUT_MS = 15000;

function str(value) {
  return String(value ?? '').trim();
}

function parseJsonSafe(raw, fallback) {
  if (raw && typeof raw === 'object') return raw;
  const txt = str(raw);
  if (!txt) return fallback;
  try {
    return JSON.parse(txt);
  } catch {
    return fallback;
  }
}

function parseJsonObjectSafe(raw, fallback = {}) {
  const parsed = parseJsonSafe(raw, fallback);
  return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : fallback;
}

function parseJsonArraySafe(raw, fallback = []) {
  const parsed = parseJsonSafe(raw, fallback);
  return Array.isArray(parsed) ? parsed : fallback;
}

function rowsFromNodeIo(value) {
  if (value && typeof value === 'object' && String(value.contract_version || '').trim() === 'node_io_v1' && Array.isArray(value.rows)) {
    return value.rows.filter((row) => row && typeof row === 'object' && !Array.isArray(row));
  }
  if (Array.isArray(value)) {
    return value.filter((row) => row && typeof row === 'object' && !Array.isArray(row));
  }
  if (value && typeof value === 'object') return [value];
  return [];
}

function columnsFromRows(rows = [], fallback = []) {
  const detected = [
    ...new Set(
      (Array.isArray(rows) ? rows : []).flatMap((row) =>
        row && typeof row === 'object' && !Array.isArray(row) ? Object.keys(row) : []
      )
    )
  ];
  return detected.length ? detected : [...new Set((Array.isArray(fallback) ? fallback : []).map((item) => str(item)).filter(Boolean))];
}

function parsePathParts(path) {
  const raw = str(path);
  if (!raw) return [];
  return raw
    .replace(/\[(\d+)\]/g, '.$1')
    .split('.')
    .map((item) => item.trim())
    .filter(Boolean)
    .map((item) => (/^\d+$/.test(item) ? Number(item) : item));
}

function getByPath(obj, path) {
  if (!path) return obj;
  const parts = parsePathParts(path);
  let current = obj;
  for (const part of parts) {
    if (current == null) return undefined;
    current = current[part];
  }
  return current;
}

function setByPath(target, path, value) {
  const parts = parsePathParts(path);
  if (!parts.length) return;
  let cursor = target;
  for (let idx = 0; idx < parts.length; idx += 1) {
    const part = parts[idx];
    const isLast = idx === parts.length - 1;
    if (isLast) {
      cursor[part] = value;
      return;
    }
    const nextPart = parts[idx + 1];
    if (cursor[part] == null || typeof cursor[part] !== 'object') {
      cursor[part] = typeof nextPart === 'number' ? [] : {};
    }
    cursor = cursor[part];
  }
}

function replaceTokens(value, row) {
  if (typeof value !== 'string') return value;
  return value.replace(/\{([^{}]+)\}/g, (_match, key) => {
    const resolved = row?.[str(key)];
    return resolved === undefined || resolved === null ? '' : String(resolved);
  });
}

function deepApplyTokens(value, row) {
  if (Array.isArray(value)) return value.map((item) => deepApplyTokens(item, row));
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, deepApplyTokens(item, row)]));
  }
  return replaceTokens(value, row);
}

function buildHttpRequestBody(method, headers, bodyObj) {
  const upperMethod = str(method).toUpperCase() || 'POST';
  if (upperMethod === 'GET' || upperMethod === 'DELETE') return undefined;
  const contentTypeKey = Object.keys(headers || {}).find((key) => String(key).toLowerCase() === 'content-type');
  const contentType = String(contentTypeKey ? headers?.[contentTypeKey] : 'application/json').toLowerCase();
  if (contentType.includes('application/x-www-form-urlencoded')) {
    return new URLSearchParams(
      Object.entries(bodyObj || {}).reduce((acc, [key, value]) => {
        if (value === undefined || value === null) return acc;
        acc[key] = String(value);
        return acc;
      }, {})
    ).toString();
  }
  if (contentType.includes('text/plain')) return typeof bodyObj === 'string' ? bodyObj : JSON.stringify(bodyObj || {});
  return JSON.stringify(bodyObj || {});
}

function normalizeBindingRule(item = {}, index = 0) {
  const target = str(item.target || 'body_item').toLowerCase();
  return {
    id: str(item.id || `binding_${index + 1}`),
    sourceField: str(item.sourceField || item.alias || item.source_field),
    target: ['header', 'query', 'body', 'body_item'].includes(target) ? target : 'body_item',
    path: str(item.path || item.targetPath || item.target_path)
  };
}

function normalizeResponseMapping(item = {}, index = 0) {
  return {
    id: str(item.id || `mapping_${index + 1}`),
    responsePath: str(item.responsePath || item.response_path || item.path),
    alias: str(item.alias || item.outputName || item.output_name || `response_${index + 1}`)
  };
}

export function normalizeApiMutationSettings(raw = {}) {
  const src = raw && typeof raw === 'object' ? raw : {};
  const requestModeRaw = str(src.requestMode || src.request_mode || 'row_per_request').toLowerCase();
  const stopPolicyRaw = str(src.stopPolicy || src.stop_policy || 'stop_on_error').toLowerCase();
  const authModeRaw = str(src.authMode || src.auth_mode || 'manual').toLowerCase();
  return {
    endpointUrl: str(src.endpointUrl || src.endpoint_url || src.apiUrl || src.api_url),
    httpMethod: str(src.httpMethod || src.http_method || src.apiMethod || src.api_method || 'POST').toUpperCase() || 'POST',
    authMode: authModeRaw === 'bearer_token' ? 'bearer_token' : 'manual',
    authJson: parseJsonObjectSafe(src.authJson || src.auth_json, {}),
    headersJson: parseJsonObjectSafe(src.headersJson || src.headers_json || src.apiHeaders || src.api_headers, {}),
    queryJson: parseJsonObjectSafe(src.queryJson || src.query_json || src.apiQuery || src.api_query, {}),
    bodyJson: parseJsonObjectSafe(src.bodyJson || src.body_json || src.apiBody || src.api_body, {}),
    bodyItemsPath: str(src.bodyItemsPath || src.body_items_path || 'items') || 'items',
    bindingRules: parseJsonArraySafe(src.bindingRules || src.bindingRulesJson || src.binding_rules, []).map(normalizeBindingRule),
    responseMappings: parseJsonArraySafe(src.responseMappings || src.responseMappingsJson || src.response_mappings, []).map(normalizeResponseMapping),
    requestMode: requestModeRaw === 'batch_request' ? 'batch_request' : 'row_per_request',
    batchSize: Math.max(1, Math.min(500, Math.trunc(Number(src.batchSize || src.batch_size || DEFAULT_BATCH_SIZE)))),
    dryRun: String(src.dryRun ?? src.dry_run ?? 'true').trim().toLowerCase() === 'true',
    maxRowsPerRun: Math.max(1, Math.min(10000, Math.trunc(Number(src.maxRowsPerRun || src.max_rows_per_run || DEFAULT_MAX_ROWS)))),
    retryCount: Math.max(0, Math.min(10, Math.trunc(Number(src.retryCount || src.retry_count || 1)))),
    timeoutMs: Math.max(100, Math.min(120000, Math.trunc(Number(src.timeoutMs || src.timeout_ms || DEFAULT_TIMEOUT_MS)))),
    stopPolicy: stopPolicyRaw === 'continue' ? 'continue' : 'stop_on_error',
    previewLimit: Math.max(1, Math.min(200, Math.trunc(Number(src.previewLimit || src.preview_limit || DEFAULT_PREVIEW_LIMIT)))),
    channel: str(src.channel)
  };
}

function mergeAuthHeaders(cfg, headers) {
  const next = { ...(headers || {}) };
  if (cfg.authMode === 'bearer_token') {
    const token = str(cfg.authJson?.token || cfg.authJson?.access_token);
    if (token) next.Authorization = `Bearer ${token}`;
    return next;
  }
  Object.entries(cfg.authJson || {}).forEach(([key, value]) => {
    if (value === undefined || value === null) return;
    next[key] = value;
  });
  return next;
}

function buildItemsForBatch(groupRows, itemRules) {
  return groupRows.map((row) => {
    const item = {};
    for (const rule of itemRules) {
      if (!rule.sourceField || !rule.path) continue;
      setByPath(item, rule.path, row?.[rule.sourceField]);
    }
    return item;
  });
}

function buildRequestForGroup(cfg, groupRows) {
  const queryObj = deepApplyTokens(cfg.queryJson, groupRows[0] || {});
  const headersObj = mergeAuthHeaders(cfg, deepApplyTokens(cfg.headersJson, groupRows[0] || {}));
  const bodyObj = deepApplyTokens(cfg.bodyJson, groupRows[0] || {});
  const scalarRules = cfg.bindingRules.filter((rule) => rule.target !== 'body_item');
  const itemRules = cfg.bindingRules.filter((rule) => rule.target === 'body_item');

  if (cfg.requestMode === 'batch_request') {
    const items = buildItemsForBatch(groupRows, itemRules);
    if (items.length) setByPath(bodyObj, cfg.bodyItemsPath, items);
    for (const rule of scalarRules) {
      if (!rule.sourceField || !rule.path) continue;
      const sourceRow = groupRows[0] || {};
      const value = sourceRow?.[rule.sourceField];
      if (rule.target === 'header') headersObj[rule.path] = value;
      if (rule.target === 'query') queryObj[rule.path] = value;
      if (rule.target === 'body') setByPath(bodyObj, rule.path, value);
    }
  } else {
    const sourceRow = groupRows[0] || {};
    for (const rule of cfg.bindingRules) {
      if (!rule.sourceField || !rule.path) continue;
      const value = sourceRow?.[rule.sourceField];
      if (rule.target === 'header') headersObj[rule.path] = value;
      if (rule.target === 'query') queryObj[rule.path] = value;
      if (rule.target === 'body' || rule.target === 'body_item') setByPath(bodyObj, rule.path, value);
    }
  }

  const url = new URL(cfg.endpointUrl);
  Object.entries(queryObj || {}).forEach(([key, value]) => {
    if (value === undefined || value === null) return;
    url.searchParams.set(key, String(value));
  });
  return {
    method: cfg.httpMethod,
    url: url.toString(),
    headers: headersObj,
    bodyObject: bodyObj,
    body: buildHttpRequestBody(cfg.httpMethod, headersObj, bodyObj)
  };
}

function chunkRows(rows, size) {
  const out = [];
  for (let index = 0; index < rows.length; index += size) {
    out.push(rows.slice(index, index + size));
  }
  return out;
}

async function executeHttpRequest(request, cfg) {
  let lastResponse = null;
  let lastError = null;
  const attempts = Math.max(1, cfg.retryCount + 1);
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), cfg.timeoutMs);
    try {
      const response = await fetch(request.url, {
        method: request.method,
        headers: request.headers,
        body: request.body,
        signal: controller.signal
      });
      clearTimeout(timer);
      const contentType = String(response.headers.get('content-type') || '').toLowerCase();
      const rawText = await response.text();
      const body =
        contentType.includes('json')
          ? (() => {
              try {
                return rawText ? JSON.parse(rawText) : {};
              } catch {
                return rawText;
              }
            })()
          : rawText;
      lastResponse = {
        ok: response.ok,
        status: response.status,
        body
      };
      if (response.ok || !shouldRetryStatus(response.status) || attempt >= attempts) return lastResponse;
      await new Promise((resolve) => setTimeout(resolve, retryDelayMs(attempt)));
    } catch (error) {
      clearTimeout(timer);
      lastError = error;
      if (attempt >= attempts) break;
      await new Promise((resolve) => setTimeout(resolve, retryDelayMs(attempt)));
    }
  }
  if (lastResponse) return lastResponse;
  throw lastError || new Error('api_mutation_request_failed');
}

function mapResponseFields(responseBody, mappings) {
  const out = {};
  for (const mapping of mappings) {
    if (!mapping.alias) continue;
    out[mapping.alias] = mapping.responsePath ? getByPath(responseBody, mapping.responsePath) : responseBody;
  }
  return out;
}

async function runApiMutation(client, rawSettings = {}, options = {}) {
  void client;
  const cfg = normalizeApiMutationSettings(rawSettings);
  const inputRows = rowsFromNodeIo(options?.inputValue).slice(0, cfg.maxRowsPerRun);
  if (!cfg.endpointUrl) throw new Error('Сначала укажи endpoint API-изменения.');
  const previewMode = Boolean(options?.preview);
  const effectiveDryRun = options?.dryRunOverride === undefined ? previewMode || cfg.dryRun : Boolean(options.dryRunOverride);
  const groups =
    cfg.requestMode === 'batch_request' ? chunkRows(inputRows, cfg.batchSize) : inputRows.map((row) => [row]);
  const warnings = [];
  const requestSamples = [];
  const outputRows = [];
  let requestCount = 0;
  let successCount = 0;
  let failedCount = 0;

  for (let index = 0; index < groups.length; index += 1) {
    const groupRows = groups[index];
    const request = buildRequestForGroup(cfg, groupRows);
    requestCount += 1;
    if (requestSamples.length < cfg.previewLimit) {
      requestSamples.push({
        request_index: index + 1,
        batch_index: index + 1,
        source_rows: groupRows.length,
        url: request.url,
        method: request.method,
        body: request.bodyObject
      });
    }

    let responseStatus = 0;
    let responseBody = { dry_run: true };
    let ok = true;
    let errorText = '';
    if (!effectiveDryRun) {
      try {
        const response = await executeHttpRequest(request, cfg);
        responseStatus = Number(response?.status || 0);
        responseBody = response?.body;
        ok = Boolean(response?.ok);
      } catch (error) {
        ok = false;
        errorText = String(error?.message || error);
      }
    }

    if (ok) successCount += 1;
    else failedCount += 1;

    const mappedResponse = mapResponseFields(responseBody, cfg.responseMappings);
    if (cfg.requestMode === 'row_per_request') {
      const sourceRow = groupRows[0] || {};
      outputRows.push({
        ...sourceRow,
        mutation_request_index: index + 1,
        mutation_batch_index: index + 1,
        mutation_source_rows: groupRows.length,
        mutation_dry_run: effectiveDryRun,
        mutation_executed: !effectiveDryRun,
        mutation_ok: ok,
        mutation_status: responseStatus,
        mutation_error: errorText,
        ...mappedResponse
      });
    } else {
      outputRows.push({
        mutation_request_index: index + 1,
        mutation_batch_index: index + 1,
        mutation_source_rows: groupRows.length,
        mutation_dry_run: effectiveDryRun,
        mutation_executed: !effectiveDryRun,
        mutation_ok: ok,
        mutation_status: responseStatus,
        mutation_error: errorText,
        ...mappedResponse
      });
    }

    if (!ok && cfg.stopPolicy === 'stop_on_error') {
      warnings.push(`Выполнение остановлено на запросе ${index + 1}: ${errorText || `HTTP ${responseStatus || 0}`}`);
      break;
    }
  }

  return {
    rows: outputRows,
    columns: columnsFromRows(outputRows, [
      'mutation_request_index',
      'mutation_batch_index',
      'mutation_source_rows',
      'mutation_dry_run',
      'mutation_executed',
      'mutation_ok',
      'mutation_status',
      'mutation_error'
    ]),
    input_rows: inputRows,
    input_columns: columnsFromRows(inputRows),
    request_preview: requestSamples,
    stats: {
      input_rows: inputRows.length,
      request_mode: cfg.requestMode,
      request_count: requestCount,
      success_count: successCount,
      failed_count: failedCount,
      dry_run: effectiveDryRun,
      batch_size: cfg.batchSize
    },
    warnings,
    meta: {
      source_type: 'api_mutation',
      request_mode: cfg.requestMode,
      dry_run: effectiveDryRun
    }
  };
}

export async function previewApiMutationConfig(client, rawSettings = {}, options = {}) {
  const cfg = normalizeApiMutationSettings(rawSettings);
  const result = await runApiMutation(client, rawSettings, { ...options, preview: true, dryRunOverride: true });
  return {
    source_type: 'api_mutation',
    source_ref: str(rawSettings?.endpointUrl || rawSettings?.endpoint_url),
    input_row_count: result.input_rows.length,
    input_column_count: result.input_columns.length,
    input_columns: result.input_columns,
    input_sample_rows: result.input_rows.slice(0, cfg.previewLimit),
    row_count: result.rows.length,
    column_count: result.columns.length,
    columns: result.columns,
    sample_rows: result.rows.slice(0, cfg.previewLimit),
    request_preview: result.request_preview,
    stats: result.stats,
    warnings: result.warnings
  };
}

export async function executeApiMutationConfig(client, rawSettings = {}, options = {}) {
  return runApiMutation(client, rawSettings, options);
}
