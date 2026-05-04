const HTTP_METHODS = new Set(['GET', 'POST', 'PUT', 'PATCH', 'DELETE']);

const PATCH_WHITELIST = Object.freeze({
  method: 'string',
  baseUrl: 'string',
  path: 'string',
  authMode: 'string',
  authJson: 'object',
  oauth2TokenUrl: 'string',
  oauth2RequestUrl: 'string',
  headersJson: 'object',
  queryJson: 'object',
  bodyJson: 'any',
  paginationEnabled: 'boolean',
  paginationStrategy: 'string',
  paginationTarget: 'string',
  paginationDataPath: 'string',
  paginationPageParam: 'string',
  paginationStartPage: 'number',
  paginationLimitParam: 'string',
  paginationLimitValue: 'number',
  paginationOffsetParam: 'string',
  paginationStartOffset: 'number',
  paginationCursorReqPath1: 'string',
  paginationCursorResPath1: 'string',
  paginationNextUrlPath: 'string',
  paginationUseDelay: 'boolean',
  paginationDelayMs: 'number',
  paginationUseMaxPages: 'boolean',
  paginationMaxPages: 'number',
  paginationStopOnHttpError: 'boolean',
  paginationStopOnMissingValue: 'boolean',
  responseLogEnabled: 'boolean',
  responseLogMode: 'string',
  responseLogOnlyErrors: 'boolean',
  responseLogWriteRequestPayload: 'boolean',
  responseLogWriteResponsePayload: 'boolean',
  responseLogWritePaginationValues: 'boolean',
  executionDelayMs: 'number',
  outputParameters: 'array',
  bodyItemsPath: 'string'
});

const FIELD_LABELS = Object.freeze({
  method: 'HTTP method',
  baseUrl: 'Base URL',
  path: 'Endpoint/path',
  authMode: 'Авторизация',
  authJson: 'Auth hints',
  oauth2TokenUrl: 'OAuth2 token URL',
  oauth2RequestUrl: 'OAuth2 token request',
  headersJson: 'Headers',
  queryJson: 'Query params',
  bodyJson: 'Body',
  paginationEnabled: 'Пагинация',
  paginationStrategy: 'Стратегия пагинации',
  paginationTarget: 'Куда писать пагинацию',
  paginationDataPath: 'Путь записей в ответе',
  paginationPageParam: 'Page param',
  paginationStartPage: 'Start page',
  paginationLimitParam: 'Limit param',
  paginationLimitValue: 'Limit value',
  paginationOffsetParam: 'Offset param',
  paginationStartOffset: 'Start offset',
  paginationCursorReqPath1: 'Cursor request path',
  paginationCursorResPath1: 'Cursor response path',
  paginationNextUrlPath: 'Next URL path',
  paginationUseDelay: 'Пауза между запросами',
  paginationDelayMs: 'Пауза, мс',
  paginationUseMaxPages: 'Лимит страниц',
  paginationMaxPages: 'Max pages',
  paginationStopOnHttpError: 'Остановка на HTTP ошибке',
  paginationStopOnMissingValue: 'Остановка без значения пагинации',
  responseLogEnabled: 'API logging',
  responseLogMode: 'Режим API logging',
  responseLogOnlyErrors: 'Логировать только ошибки',
  responseLogWriteRequestPayload: 'Логировать request payload',
  responseLogWriteResponsePayload: 'Логировать response payload',
  responseLogWritePaginationValues: 'Логировать значения пагинации',
  executionDelayMs: 'Пауза выполнения',
  outputParameters: 'Выходные параметры',
  bodyItemsPath: 'Body items path',
  documentation_url: 'URL документации',
  rate_limit: 'Rate limit / retry',
  retry_policy: 'Retry hints'
});

function isPlainObject(value) {
  return value && typeof value === 'object' && !Array.isArray(value);
}

function toPlainObject(value) {
  return isPlainObject(value) ? value : {};
}

function cloneJson(value) {
  if (value === undefined) return undefined;
  try {
    return JSON.parse(JSON.stringify(value));
  } catch {
    return value;
  }
}

function uniqueStrings(items) {
  return Array.from(new Set((Array.isArray(items) ? items : []).map((item) => String(item || '').trim()).filter(Boolean)));
}

function safeJsonParse(raw) {
  const text = String(raw || '').trim();
  if (!text) return { ok: false, value: null };
  try {
    return { ok: true, value: JSON.parse(text) };
  } catch {
    return { ok: false, value: null };
  }
}

function tryParseJsonSnippet(raw) {
  const text = unwrapCodeFence(raw);
  const direct = safeJsonParse(text);
  if (direct.ok) return direct;
  const startObj = text.indexOf('{');
  const endObj = text.lastIndexOf('}');
  if (startObj >= 0 && endObj > startObj) {
    const parsed = safeJsonParse(text.slice(startObj, endObj + 1));
    if (parsed.ok) return parsed;
  }
  const startArr = text.indexOf('[');
  const endArr = text.lastIndexOf(']');
  if (startArr >= 0 && endArr > startArr) {
    const parsed = safeJsonParse(text.slice(startArr, endArr + 1));
    if (parsed.ok) return parsed;
  }
  return { ok: false, value: null };
}

function unwrapCodeFence(raw) {
  const text = String(raw || '').trim();
  const match = text.match(/```(?:json|bash|sh|http|curl)?\s*([\s\S]*?)```/i);
  return (match?.[1] || text).trim();
}

function cleanUrl(raw) {
  return String(raw || '')
    .trim()
    .replace(/[),.;]+$/g, '')
    .replace(/^['"]|['"]$/g, '');
}

function decodePath(pathname) {
  const raw = String(pathname || '/').trim() || '/';
  try {
    return decodeURIComponent(raw);
  } catch {
    return raw;
  }
}

function parseUrlParts(rawUrl) {
  const text = cleanUrl(rawUrl);
  if (!text) return null;
  try {
    const u = new URL(text);
    const query = {};
    u.searchParams.forEach((value, key) => {
      query[key] = value;
    });
    return {
      raw: text,
      baseUrl: `${u.protocol}//${u.host}`,
      path: decodePath(u.pathname || '/'),
      query
    };
  } catch {
    return null;
  }
}

function looksLikeDocumentationUrl(url) {
  const lower = String(url || '').toLowerCase();
  return /\/(?:docs|doc|documentation|manual|reference|swagger|openapi)(?:\/|$|[?#])/.test(lower);
}

function isProbablyEndpointUrl(url) {
  const parsed = parseUrlParts(url);
  if (!parsed) return false;
  if (looksLikeDocumentationUrl(parsed.raw)) return false;
  return /\/(?:api|v\d+|rest|graphql|openapi|wb|ozon|campaign|advert|stats|report|orders|items|products|cards|analytics)(?:\/|$)/i.test(parsed.path)
    || Object.keys(parsed.query || {}).length > 0;
}

function tokenizeShell(input) {
  const out = [];
  let cur = '';
  let quote = '';
  let escaped = false;
  for (const ch of String(input || '')) {
    if (escaped) {
      cur += ch;
      escaped = false;
      continue;
    }
    if (ch === '\\') {
      escaped = true;
      continue;
    }
    if (quote) {
      if (ch === quote) quote = '';
      else cur += ch;
      continue;
    }
    if (ch === "'" || ch === '"') {
      quote = ch;
      continue;
    }
    if (/\s/.test(ch)) {
      if (cur) {
        out.push(cur);
        cur = '';
      }
      continue;
    }
    cur += ch;
  }
  if (cur) out.push(cur);
  return out;
}

function fromHeaderLines(lines) {
  const out = {};
  for (const line of Array.isArray(lines) ? lines : String(lines || '').split(/\r?\n/)) {
    const text = String(line || '').trim();
    if (!text || !text.includes(':')) continue;
    if (/^(https?:\/\/|body:|request:|response:)/i.test(text)) continue;
    const idx = text.indexOf(':');
    const key = text.slice(0, idx).trim();
    const value = text.slice(idx + 1).trim();
    if (!key || /\s/.test(key) || key.length > 80) continue;
    out[key] = value;
  }
  return out;
}

function splitAuthHeaders(headersObj) {
  const auth = {};
  const headersOut = {};
  for (const [rawKey, rawValue] of Object.entries(toPlainObject(headersObj))) {
    const key = String(rawKey || '').trim();
    if (!key) continue;
    const value = typeof rawValue === 'string' ? rawValue : String(rawValue ?? '');
    const lower = key.toLowerCase();
    const isAuth =
      lower === 'authorization' ||
      lower === 'x-api-key' ||
      lower === 'api-key' ||
      lower === 'apikey' ||
      lower.includes('token') ||
      lower.includes('secret');
    if (isAuth) auth[key] = value;
    else headersOut[key] = value;
  }
  return { auth, headersOut };
}

function normalizeMethod(raw) {
  const method = String(raw || '').trim().toUpperCase();
  return HTTP_METHODS.has(method) ? method : '';
}

function parseCurl(text) {
  if (!/\bcurl\b/i.test(text)) return null;
  const tokens = tokenizeShell(String(text || '').replace(/\\\r?\n/g, ' '));
  const curlIdx = tokens.findIndex((token) => token.toLowerCase() === 'curl');
  if (curlIdx < 0) return null;
  let method = '';
  let url = '';
  const headerLines = [];
  const dataParts = [];
  let hasData = false;
  for (let i = curlIdx + 1; i < tokens.length; i += 1) {
    const token = tokens[i];
    const lower = token.toLowerCase();
    if (lower === '-x' || lower === '--request') {
      method = normalizeMethod(tokens[i + 1]);
      i += 1;
      continue;
    }
    if (lower.startsWith('-x') && lower.length > 2) {
      method = normalizeMethod(lower.slice(2));
      continue;
    }
    if (lower === '-h' || lower === '--header') {
      if (tokens[i + 1]) headerLines.push(tokens[i + 1]);
      i += 1;
      continue;
    }
    if (lower.startsWith('-h') && lower.length > 2) {
      headerLines.push(token.slice(2));
      continue;
    }
    if (lower === '--url') {
      url = tokens[i + 1] || url;
      i += 1;
      continue;
    }
    if (['-d', '--data', '--data-raw', '--data-binary', '--data-ascii', '--form'].includes(lower)) {
      hasData = true;
      if (tokens[i + 1]) dataParts.push(tokens[i + 1]);
      i += 1;
      continue;
    }
    if ((lower === '-u' || lower === '--user') && tokens[i + 1]) {
      headerLines.push(`Authorization: Basic ${tokens[i + 1]}`);
      i += 1;
      continue;
    }
    if (!url && /^https?:\/\//i.test(token)) url = token;
  }
  if (!method) method = hasData ? 'POST' : 'GET';
  if (!url) {
    const urlMatch = text.match(/https?:\/\/[^\s"'\\]+/i);
    url = urlMatch?.[0] || '';
  }
  return url ? { method, url, headerLines, bodyRaw: dataParts.join('&') } : null;
}

function parseHttpRequest(text) {
  const lines = String(text || '').split(/\r?\n/);
  let requestLineIdx = -1;
  let method = '';
  let target = '';
  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i].trim();
    const match = line.match(/^(GET|POST|PUT|PATCH|DELETE)\s+(\S+)(?:\s+HTTP\/\d(?:\.\d)?)?/i);
    if (match) {
      requestLineIdx = i;
      method = normalizeMethod(match[1]);
      target = match[2];
      break;
    }
  }
  if (requestLineIdx < 0) return null;
  const headerLines = [];
  let bodyLines = [];
  let inBody = false;
  for (let i = requestLineIdx + 1; i < lines.length; i += 1) {
    const line = lines[i];
    if (!inBody && !line.trim()) {
      inBody = true;
      continue;
    }
    if (inBody) bodyLines.push(line);
    else headerLines.push(line);
  }
  const headers = fromHeaderLines(headerLines);
  const host = headers.Host || headers.host || '';
  let url = target;
  if (!/^https?:\/\//i.test(url) && host) {
    url = `https://${host}${target.startsWith('/') ? target : `/${target}`}`;
  }
  return { method, url, headers, bodyRaw: bodyLines.join('\n').trim() };
}

function parseObjectFromMaybeText(value) {
  if (isPlainObject(value)) return value;
  if (typeof value === 'string') {
    const parsed = safeJsonParse(value);
    if (parsed.ok && isPlainObject(parsed.value)) return parsed.value;
  }
  return {};
}

function extractUrlFromObject(root) {
  const direct = String(root?.url || root?.endpoint || root?.endpoint_url || root?.request_url || '').trim();
  if (direct) return direct;
  const host = String(root?.baseUrl || root?.base_url || root?.host || '').trim();
  const path = String(root?.path || root?.pathname || '').trim();
  if (host && path) return `${host.replace(/\/$/, '')}${path.startsWith('/') ? path : `/${path}`}`;
  return '';
}

function firstOpenApiOperation(doc) {
  const paths = toPlainObject(doc?.paths);
  for (const [path, item] of Object.entries(paths)) {
    const ops = toPlainObject(item);
    for (const method of ['get', 'post', 'put', 'patch', 'delete']) {
      if (ops[method]) return { method: method.toUpperCase(), path, operation: ops[method] };
    }
  }
  return null;
}

function schemaExample(schema) {
  const src = toPlainObject(schema);
  if (src.example !== undefined) return src.example;
  if (src.default !== undefined) return src.default;
  if (src.type === 'array') return [schemaExample(src.items || {})];
  if (src.type === 'object' || src.properties) {
    const out = {};
    for (const [key, child] of Object.entries(toPlainObject(src.properties))) out[key] = schemaExample(child);
    return out;
  }
  if (src.type === 'integer' || src.type === 'number') return 0;
  if (src.type === 'boolean') return false;
  return '';
}

function parseOpenApi(doc) {
  if (!isPlainObject(doc) || (!doc.openapi && !doc.swagger && !doc.paths)) return null;
  const op = firstOpenApiOperation(doc);
  if (!op) return null;
  const servers = Array.isArray(doc.servers) ? doc.servers : [];
  const serverUrl = String(servers[0]?.url || doc.host || '').trim();
  const patch = { method: op.method, path: op.path };
  if (serverUrl) {
    const parsed = parseUrlParts(serverUrl);
    if (parsed) patch.baseUrl = parsed.baseUrl;
    else if (/^https?:\/\//i.test(serverUrl)) patch.baseUrl = serverUrl.replace(/\/$/, '');
  }
  const query = {};
  const headers = {};
  for (const param of Array.isArray(op.operation?.parameters) ? op.operation.parameters : []) {
    const where = String(param?.in || '').trim();
    const name = String(param?.name || '').trim();
    if (!name) continue;
    const value = param?.example ?? param?.default ?? schemaExample(param?.schema || {});
    if (where === 'query') query[name] = value;
    if (where === 'header') headers[name] = value;
  }
  if (Object.keys(query).length) patch.queryJson = query;
  if (Object.keys(headers).length) {
    const split = splitAuthHeaders(headers);
    if (Object.keys(split.headersOut).length) patch.headersJson = split.headersOut;
    if (Object.keys(split.auth).length) patch.authJson = split.auth;
  }
  const requestBody = op.operation?.requestBody?.content;
  if (isPlainObject(requestBody)) {
    const firstContent = Object.values(requestBody)[0];
    const bodyExample = firstContent?.example ?? firstContent?.examples?.default?.value ?? schemaExample(firstContent?.schema || {});
    if (bodyExample !== undefined) patch.bodyJson = bodyExample;
  }
  const responseContent = op.operation?.responses?.['200']?.content || op.operation?.responses?.default?.content;
  if (isPlainObject(responseContent)) {
    const firstContent = Object.values(responseContent)[0];
    const responseExample = firstContent?.example ?? firstContent?.examples?.default?.value ?? schemaExample(firstContent?.schema || {});
    const recordPath = findArrayPath(responseExample);
    if (recordPath) {
      patch.paginationDataPath = recordPath;
      patch.outputParameters = buildOutputParametersFromSample(getByDottedPath(responseExample, recordPath), recordPath);
    }
  }
  return patch;
}

function getByDottedPath(value, path) {
  if (!path) return value;
  const parts = String(path).split('.').filter(Boolean);
  let current = value;
  for (const part of parts) {
    if (current == null) return undefined;
    current = current[part];
  }
  return current;
}

function findArrayPath(value, prefix = '') {
  if (Array.isArray(value)) return prefix;
  if (!isPlainObject(value)) return '';
  const preferred = ['data', 'items', 'records', 'results', 'rows', 'list', 'objects', 'payload'];
  for (const key of preferred) {
    if (Array.isArray(value[key])) return prefix ? `${prefix}.${key}` : key;
  }
  for (const [key, child] of Object.entries(value)) {
    const childPath = findArrayPath(child, prefix ? `${prefix}.${key}` : key);
    if (childPath) return childPath;
  }
  return '';
}

function normalizeOutputAlias(path) {
  const leaf = String(path || '').split('.').filter(Boolean).slice(-1)[0] || 'value';
  return leaf
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replace(/[^a-zA-Z0-9а-яА-ЯёЁ_]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .toLowerCase() || 'value';
}

function buildOutputParametersFromSample(value, rootPath = '') {
  const sample = Array.isArray(value) ? value[0] : value;
  if (!isPlainObject(sample)) return [];
  return Object.entries(sample)
    .filter(([, child]) => child === null || typeof child !== 'object')
    .slice(0, 12)
    .map(([key, child], idx) => ({
      id: `api_out_${idx + 1}_${normalizeOutputAlias(key)}`,
      rootPath: String(rootPath || '').trim(),
      path: key,
      alias: normalizeOutputAlias(key),
      valueType:
        typeof child === 'number' ? 'Число' : typeof child === 'boolean' ? 'Булево' : /^\d{4}-\d{2}-\d{2}/.test(String(child || '')) ? 'Дата' : 'Текст'
    }));
}

function detectRequestBodyFromText(text) {
  const bodyMatch = String(text || '').match(/\b(?:body|payload|data|тело(?:\s+запроса)?)\s*[:=]\s*([\s\S]+)$/i);
  if (!bodyMatch) return undefined;
  const parsed = tryParseJsonSnippet(bodyMatch[1]);
  return parsed.ok ? parsed.value : undefined;
}

function cleanPathHint(raw) {
  return String(raw || '')
    .trim()
    .replace(/\[(\d+)\]/g, '.$1')
    .replace(/[.,;:]+$/g, '');
}

function detectBodyItemsPath(text) {
  const match = String(text || '').match(/\b(?:body_items_path|items path|путь элементов body)\s*[:=]\s*([A-Za-z0-9_.[\]-]+)/i);
  return match?.[1] ? cleanPathHint(match[1]) : '';
}

function detectResponsePath(text) {
  const match = String(text || '').match(
    /\b(?:records path|response path|data path|items path|output path|путь\s+(?:к\s+)?(?:запис(?:ям|и)|данным|ответу))\s*[:=]\s*([A-Za-z0-9_.[\]-]+)/i
  );
  return match?.[1] ? cleanPathHint(match[1]) : '';
}

function detectPaginationHints(text, query) {
  const lower = String(text || '').toLowerCase();
  const queryKeys = new Set(Object.keys(toPlainObject(query)).map((key) => key.toLowerCase()));
  const patch = {};
  const mentionsPagination = /pagination|page|offset|cursor|next[_\s-]?url|страниц|пагинац|курсор|смещени/i.test(text);
  if (!mentionsPagination) return patch;
  patch.paginationEnabled = true;
  patch.paginationTarget = /body\s+(?:param|parameter|поле)|в\s+body|request body/i.test(text) ? 'body' : 'query';
  const dataPath = detectResponsePath(text);
  if (dataPath) patch.paginationDataPath = dataPath;
  if (/next[_\s-]?url|next\s+link|следующ(?:ая|ий)\s+ссылк/i.test(lower)) {
    patch.paginationStrategy = 'next_url';
    const match = text.match(/\b(?:next_url|next url|next link|next)\s*[:=]\s*([A-Za-z0-9_.[\]-]+)/i);
    patch.paginationNextUrlPath = match?.[1] || 'next';
  } else if (/cursor|next_cursor|курсор/i.test(lower) || queryKeys.has('cursor')) {
    patch.paginationStrategy = 'cursor_fields';
    const req = text.match(/\b(?:cursor param|cursor request|request cursor|cursor)\s*[:=]\s*([A-Za-z0-9_.[\]-]+)/i);
    const res = text.match(/\b(?:next_cursor|cursor response|response cursor|cursor path)\s*[:=]\s*([A-Za-z0-9_.[\]-]+)/i);
    patch.paginationCursorReqPath1 = req?.[1] || (queryKeys.has('cursor') ? 'cursor' : 'cursor');
    patch.paginationCursorResPath1 = res?.[1] || 'next_cursor';
  } else if (/offset|смещени/i.test(lower) || queryKeys.has('offset')) {
    patch.paginationStrategy = 'offset_limit';
    const off = text.match(/\b(?:offset param|offset)\s*[:=]\s*([A-Za-z0-9_-]+)/i);
    patch.paginationOffsetParam = off?.[1] && !/^\d+$/.test(off[1]) ? off[1] : queryKeys.has('offset') ? 'offset' : 'offset';
    patch.paginationStartOffset = 0;
  } else {
    patch.paginationStrategy = 'page_number';
    const page = text.match(/\b(?:page param|page|страница)\s*[:=]\s*([A-Za-z0-9_-]+)/i);
    patch.paginationPageParam = page?.[1] && !/^\d+$/.test(page[1]) ? page[1] : queryKeys.has('page') ? 'page' : 'page';
    patch.paginationStartPage = 1;
  }
  const limitMatch = text.match(/\b(?:limit param|limit|per_page|page_size|размер\s+страницы)\s*[:=]\s*([A-Za-z0-9_-]+)/i);
  const limitValueMatch = text.match(/\b(?:limit|per_page|page_size|размер\s+страницы)\s*(?:=|:|\s)\s*(\d{1,5})\b/i);
  if (limitMatch?.[1] && !/^\d+$/.test(limitMatch[1])) patch.paginationLimitParam = limitMatch[1];
  else if (queryKeys.has('limit')) patch.paginationLimitParam = 'limit';
  else if (queryKeys.has('per_page')) patch.paginationLimitParam = 'per_page';
  if (limitValueMatch?.[1]) patch.paginationLimitValue = Math.max(1, Number(limitValueMatch[1]));
  patch.paginationStopOnHttpError = true;
  patch.paginationStopOnMissingValue = true;
  return patch;
}

function detectRateLimitHints(text) {
  const lower = String(text || '').toLowerCase();
  const patch = {};
  const detected = {};
  if (!/(rate.?limit|429|too many requests|retry-after|лимит|ретра|повтор)/i.test(text)) return { patch, detected };
  detected.rate_limit = 'Найдены hints по лимитам или retry';
  patch.paginationStopOnHttpError = true;
  const retryAfter = text.match(/retry-after\s*[:=]\s*(\d{1,6})/i);
  const perMinute = text.match(/(\d{1,6})\s*(?:requests|req|запрос(?:ов|а)?)\s*(?:per|\/|в)\s*(?:minute|min|минут)/i);
  if (retryAfter?.[1]) {
    const delayMs = Math.max(0, Number(retryAfter[1]) * 1000);
    patch.paginationUseDelay = true;
    patch.paginationDelayMs = delayMs;
    patch.executionDelayMs = delayMs;
  } else if (perMinute?.[1]) {
    const per = Math.max(1, Number(perMinute[1]));
    const delayMs = Math.ceil(60000 / per);
    patch.paginationUseDelay = true;
    patch.paginationDelayMs = delayMs;
    patch.executionDelayMs = delayMs;
  } else if (/429|too many requests|retry/i.test(lower)) {
    detected.retry_policy = 'HTTP 429/5xx будут обрабатываться существующей retry-логикой выполнения';
  }
  return { patch, detected };
}

function detectLoggingHints(text) {
  const lower = String(text || '').toLowerCase();
  const patch = {};
  if (!/(api\s*log|audit|logging|журнал|логир|лог\s+api|response_log)/i.test(text)) return patch;
  patch.responseLogEnabled = true;
  if (/debug|full|request\s+and\s+response|запрос\s+и\s+ответ|полный/i.test(lower)) {
    patch.responseLogMode = 'debug';
    patch.responseLogWriteRequestPayload = true;
    patch.responseLogWriteResponsePayload = true;
    patch.responseLogWritePaginationValues = true;
  } else if (/minimal|только\s+статус|без\s+payload/i.test(lower)) {
    patch.responseLogMode = 'minimal';
    patch.responseLogWriteRequestPayload = false;
    patch.responseLogWriteResponsePayload = false;
    patch.responseLogWritePaginationValues = false;
  } else {
    patch.responseLogMode = 'standard';
  }
  if (/errors?\s+only|только\s+ошиб/i.test(lower)) patch.responseLogOnlyErrors = true;
  return patch;
}

function detectOAuthHints(text) {
  const lower = String(text || '').toLowerCase();
  const patch = {};
  if (!/oauth2|client_credentials|access_token|token_url|token url/i.test(lower)) return patch;
  patch.authMode = 'oauth2_client_credentials';
  const tokenUrl = text.match(/(?:token_url|token url|token endpoint|oauth2 token|токен)\s*[:=]?\s*(https?:\/\/[^\s"'<>]+)/i);
  if (tokenUrl?.[1]) {
    patch.oauth2TokenUrl = cleanUrl(tokenUrl[1]);
    patch.oauth2RequestUrl = cleanUrl(tokenUrl[1]);
  }
  return patch;
}

function hasRequestLikeKeys(root) {
  if (!isPlainObject(root)) return false;
  return [
    'request',
    'method',
    'url',
    'endpoint',
    'endpoint_url',
    'path',
    'baseUrl',
    'base_url',
    'headers',
    'query',
    'params',
    'body',
    'data'
  ].some((key) => Object.prototype.hasOwnProperty.call(root, key));
}

function parseJsonRequest(parsed) {
  const openApiPatch = parseOpenApi(parsed);
  if (openApiPatch) return { patch: openApiPatch, mode: 'openapi' };
  const root = isPlainObject(parsed?.request) ? parsed.request : parsed;
  if (!isPlainObject(root)) return { patch: {}, mode: 'json_value' };
  const patch = {};
  const method = normalizeMethod(root.method || root.http_method);
  if (method) patch.method = method;
  const urlRaw = extractUrlFromObject(root);
  const parsedUrl = parseUrlParts(urlRaw);
  if (parsedUrl) {
    patch.baseUrl = parsedUrl.baseUrl;
    patch.path = parsedUrl.path;
    if (Object.keys(parsedUrl.query).length) patch.queryJson = parsedUrl.query;
  } else {
    const baseUrl = String(root.baseUrl || root.base_url || '').trim();
    const path = String(root.path || '').trim();
    if (baseUrl) patch.baseUrl = baseUrl.replace(/\/$/, '');
    if (path) patch.path = path.startsWith('/') ? path : `/${path}`;
  }
  const headersSrc = parseObjectFromMaybeText(root.headers || root.header);
  const { auth, headersOut } = splitAuthHeaders(headersSrc);
  if (Object.keys(headersOut).length) patch.headersJson = headersOut;
  const authSrc = parseObjectFromMaybeText(root.auth || root.authorization || root.auth_json);
  if (Object.keys(authSrc).length) patch.authJson = authSrc;
  else if (Object.keys(auth).length) patch.authJson = auth;
  const querySrc = parseObjectFromMaybeText(root.query || root.params || root.query_params || root.query_json);
  if (Object.keys(querySrc).length) patch.queryJson = { ...(patch.queryJson || {}), ...querySrc };
  const hasExplicitBody =
    Object.prototype.hasOwnProperty.call(root, 'body') ||
    Object.prototype.hasOwnProperty.call(root, 'data') ||
    Object.prototype.hasOwnProperty.call(root, 'body_json');
  if (hasExplicitBody) patch.bodyJson = root.body ?? root.data ?? root.body_json ?? {};
  if (root.pagination || root.pagination_json) Object.assign(patch, normalizePaginationObject(root.pagination || root.pagination_json));
  if (root.output || root.response || root.response_json) {
    const sample = root.output || root.response || root.response_json;
    const recordPath = findArrayPath(sample);
    if (recordPath) {
      patch.paginationDataPath = patch.paginationDataPath || recordPath;
      patch.outputParameters = buildOutputParametersFromSample(getByDottedPath(sample, recordPath), recordPath);
    }
  }
  if (!hasRequestLikeKeys(parsed) && !hasExplicitBody) {
    const recordPath = findArrayPath(parsed);
    if (recordPath) {
      patch.paginationDataPath = recordPath;
      patch.outputParameters = buildOutputParametersFromSample(getByDottedPath(parsed, recordPath), recordPath);
      return { patch, mode: 'json_response_sample' };
    }
    patch.bodyJson = parsed;
    return { patch, mode: 'json_body' };
  }
  return { patch, mode: 'json_request' };
}

function normalizePaginationObject(raw) {
  const src = toPlainObject(raw);
  const patch = {};
  const strategy = String(src.strategy || src.mode || '').trim().toLowerCase();
  if (src.enabled !== undefined || strategy) patch.paginationEnabled = src.enabled === undefined ? true : Boolean(src.enabled);
  if (['page_number', 'offset_limit', 'next_url', 'cursor_fields', 'cursor'].includes(strategy)) {
    patch.paginationStrategy = strategy === 'cursor' ? 'cursor_fields' : strategy;
  }
  if (src.target === 'query' || src.target === 'body') patch.paginationTarget = src.target;
  if (src.data_path || src.records_path || src.response_path) patch.paginationDataPath = String(src.data_path || src.records_path || src.response_path).trim();
  if (src.page_param) patch.paginationPageParam = String(src.page_param).trim();
  if (src.start_page !== undefined) patch.paginationStartPage = Number(src.start_page);
  if (src.limit_param) patch.paginationLimitParam = String(src.limit_param).trim();
  if (src.limit_value !== undefined) patch.paginationLimitValue = Number(src.limit_value);
  if (src.offset_param) patch.paginationOffsetParam = String(src.offset_param).trim();
  if (src.start_offset !== undefined) patch.paginationStartOffset = Number(src.start_offset);
  if (src.cursor_req_path_1 || src.cursor_param) patch.paginationCursorReqPath1 = String(src.cursor_req_path_1 || src.cursor_param).trim();
  if (src.cursor_res_path_1 || src.cursor_response_path) patch.paginationCursorResPath1 = String(src.cursor_res_path_1 || src.cursor_response_path).trim();
  if (src.next_url_path) patch.paginationNextUrlPath = String(src.next_url_path).trim();
  if (src.delay_ms !== undefined) {
    patch.paginationUseDelay = Number(src.delay_ms) > 0;
    patch.paginationDelayMs = Number(src.delay_ms);
  }
  if (src.max_pages !== undefined) {
    patch.paginationUseMaxPages = true;
    patch.paginationMaxPages = Number(src.max_pages);
  }
  return patch;
}

function addDetected(detected, key, value, confidence = 'medium', source = '') {
  if (value === undefined || value === null || value === '') return;
  detected.push({
    key,
    label: FIELD_LABELS[key] || key,
    value: cloneJson(value),
    confidence,
    source
  });
}

function mergePatch(target, source) {
  const src = normalizeApiExampleApplyPatch(source);
  for (const [key, value] of Object.entries(src)) {
    if (value === undefined) continue;
    if ((key === 'headersJson' || key === 'queryJson' || key === 'authJson') && isPlainObject(value) && isPlainObject(target[key])) {
      target[key] = { ...target[key], ...value };
    } else {
      target[key] = value;
    }
  }
}

function patchToDetectedFields(patch, detected, source) {
  for (const [key, value] of Object.entries(normalizeApiExampleApplyPatch(patch))) {
    const confidence = key === 'method' || key === 'baseUrl' || key === 'path' ? 'high' : 'medium';
    addDetected(detected, key, value, confidence, source);
  }
}

function patchToRecommendations(patch) {
  return Object.entries(normalizeApiExampleApplyPatch(patch)).map(([field, value]) => ({
    field,
    label: FIELD_LABELS[field] || field,
    value: cloneJson(value),
    action: ['headersJson', 'queryJson', 'authJson', 'outputParameters'].includes(field) ? 'merge' : 'overwrite_safe',
    confidence: field === 'method' || field === 'baseUrl' || field === 'path' ? 'high' : 'medium'
  }));
}

function parseEndpointFromText(text) {
  const methodEndpoint = String(text || '').match(/\b(GET|POST|PUT|PATCH|DELETE)\s+(https?:\/\/[^\s"'<>]+|\/[^\s"'<>]+)/i);
  const methodLabel = String(text || '').match(/\bmethod\s*[:=]\s*(GET|POST|PUT|PATCH|DELETE)\b/i);
  const endpointLabel = String(text || '').match(/\b(?:endpoint|url|path|request url|ручка|эндпоинт)\s*[:=]\s*(https?:\/\/[^\s"'<>]+|\/[^\s"'<>]+)/i);
  const method = normalizeMethod(methodEndpoint?.[1] || methodLabel?.[1]);
  const endpoint = cleanUrl(methodEndpoint?.[2] || endpointLabel?.[1] || '');
  return { method, endpoint };
}

function extractUrls(text) {
  return uniqueStrings(String(text || '').match(/https?:\/\/[^\s"'<>]+/gi) || []).map(cleanUrl);
}

function buildSummary(patch, detected, unresolved, warnings) {
  const parts = [];
  if (patch.method || patch.path) {
    parts.push(`Настрой API как ${patch.method || 'метод из формы'} ${patch.path || 'найденный endpoint'}.`);
  } else if (detected.some((item) => item.key === 'documentation_url')) {
    parts.push('Найден URL документации. Для автозаполнения endpoint нужен фрагмент ручки, curl, HTTP request или OpenAPI JSON.');
  } else {
    parts.push('Автозаполнение ограничено: endpoint или method не распознаны.');
  }
  if (patch.authJson || patch.authMode) parts.push('Проверь авторизацию перед сохранением, секреты не валидируются автоматически.');
  if (patch.paginationEnabled) parts.push('Проверь путь записей и стратегию пагинации на реальном ответе API.');
  if (patch.responseLogEnabled) parts.push('API logging будет включен через существующий блок response_log.');
  if (warnings.length) parts.push(`Есть предупреждения: ${warnings.length}.`);
  if (unresolved.length) parts.push(`Требуют ручной проверки: ${unresolved.length}.`);
  return parts.join(' ');
}

export function normalizeApiExampleApplyPatch(rawPatch) {
  const src = isPlainObject(rawPatch) ? rawPatch : {};
  const out = {};
  for (const [key, kind] of Object.entries(PATCH_WHITELIST)) {
    if (!Object.prototype.hasOwnProperty.call(src, key)) continue;
    const value = src[key];
    if (value === undefined || value === null) continue;
    if (kind === 'string') {
      const text = String(value).trim();
      if (text) out[key] = key === 'method' ? normalizeMethod(text) || undefined : text;
    } else if (kind === 'number') {
      const n = Number(value);
      if (Number.isFinite(n)) out[key] = n;
    } else if (kind === 'boolean') {
      out[key] = Boolean(value);
    } else if (kind === 'object') {
      if (isPlainObject(value)) out[key] = cloneJson(value);
    } else if (kind === 'array') {
      if (Array.isArray(value)) out[key] = cloneJson(value);
    } else {
      out[key] = cloneJson(value);
    }
  }
  if (out.method === undefined) delete out.method;
  if (out.path && !String(out.path).startsWith('/')) out.path = `/${out.path}`;
  if (out.paginationStrategy && !['none', 'cursor_fields', 'page_number', 'offset_limit', 'next_url', 'custom'].includes(out.paginationStrategy)) {
    delete out.paginationStrategy;
  }
  if (out.paginationTarget && !['query', 'body'].includes(out.paginationTarget)) delete out.paginationTarget;
  if (out.responseLogMode && !['standard', 'minimal', 'debug'].includes(out.responseLogMode)) delete out.responseLogMode;
  if (out.authMode && !['manual', 'oauth2_client_credentials', 'custom'].includes(out.authMode)) delete out.authMode;
  return out;
}

export function parseApiExample(raw, options = {}) {
  void options;
  const text = unwrapCodeFence(raw);
  const detected = [];
  const unresolved = [];
  const warnings = [];
  const patch = {};
  if (!text) {
    return {
      summary: '',
      detected_fields: [],
      unresolved_items: ['Поле примера пустое.'],
      warnings: [],
      recommended_changes: [],
      apply_patch: {}
    };
  }

  const urls = extractUrls(text);
  for (const url of urls) {
    if (looksLikeDocumentationUrl(url) && !isProbablyEndpointUrl(url)) {
      addDetected(detected, 'documentation_url', url, 'high', 'text');
      warnings.push('URL документации распознан, но внешний документ не загружался. Вставьте фрагмент ручки или OpenAPI JSON для точного автозаполнения.');
    }
  }

  const curl = parseCurl(text);
  if (curl) {
    const parsedUrl = parseUrlParts(curl.url);
    const nextPatch = {};
    if (curl.method) nextPatch.method = curl.method;
    if (parsedUrl) {
      nextPatch.baseUrl = parsedUrl.baseUrl;
      nextPatch.path = parsedUrl.path;
      if (Object.keys(parsedUrl.query).length) nextPatch.queryJson = parsedUrl.query;
    }
    const headers = fromHeaderLines(curl.headerLines);
    const { auth, headersOut } = splitAuthHeaders(headers);
    if (Object.keys(headersOut).length) nextPatch.headersJson = headersOut;
    if (Object.keys(auth).length) nextPatch.authJson = auth;
    if (curl.bodyRaw) {
      const parsedBody = safeJsonParse(curl.bodyRaw);
      nextPatch.bodyJson = parsedBody.ok ? parsedBody.value : { raw: curl.bodyRaw };
    }
    mergePatch(patch, nextPatch);
    patchToDetectedFields(nextPatch, detected, 'curl');
  }

  const http = parseHttpRequest(text);
  if (http?.url) {
    const parsedUrl = parseUrlParts(http.url);
    const nextPatch = {};
    if (http.method) nextPatch.method = http.method;
    if (parsedUrl) {
      nextPatch.baseUrl = parsedUrl.baseUrl;
      nextPatch.path = parsedUrl.path;
      if (Object.keys(parsedUrl.query).length) nextPatch.queryJson = { ...(patch.queryJson || {}), ...parsedUrl.query };
    }
    const { auth, headersOut } = splitAuthHeaders(http.headers);
    if (Object.keys(headersOut).length) nextPatch.headersJson = headersOut;
    if (Object.keys(auth).length) nextPatch.authJson = auth;
    if (http.bodyRaw) {
      const parsedBody = safeJsonParse(http.bodyRaw);
      nextPatch.bodyJson = parsedBody.ok ? parsedBody.value : { raw: http.bodyRaw };
    }
    mergePatch(patch, nextPatch);
    patchToDetectedFields(nextPatch, detected, 'http');
  }

  const json = tryParseJsonSnippet(text);
  if (json.ok) {
    const parsedJson = parseJsonRequest(json.value);
    mergePatch(patch, parsedJson.patch);
    patchToDetectedFields(parsedJson.patch, detected, parsedJson.mode);
    if (parsedJson.mode === 'json_response_sample') {
      warnings.push('JSON похож на пример ответа: настройки запроса не менялись, использованы только output/records hints.');
    }
  }

  const endpoint = parseEndpointFromText(text);
  if (endpoint.endpoint && !patch.baseUrl && !patch.path) {
    const nextPatch = {};
    if (endpoint.method) nextPatch.method = endpoint.method;
    const parsedUrl = parseUrlParts(endpoint.endpoint);
    if (parsedUrl) {
      nextPatch.baseUrl = parsedUrl.baseUrl;
      nextPatch.path = parsedUrl.path;
      if (Object.keys(parsedUrl.query).length) nextPatch.queryJson = parsedUrl.query;
    } else if (endpoint.endpoint.startsWith('/')) {
      nextPatch.path = endpoint.endpoint;
    }
    mergePatch(patch, nextPatch);
    patchToDetectedFields(nextPatch, detected, 'text');
  }

  const endpointUrl = urls.find((url) => isProbablyEndpointUrl(url));
  if (endpointUrl && !patch.baseUrl) {
    const parsedUrl = parseUrlParts(endpointUrl);
    if (parsedUrl) {
      const nextPatch = {
        baseUrl: parsedUrl.baseUrl,
        path: parsedUrl.path
      };
      if (Object.keys(parsedUrl.query).length) nextPatch.queryJson = parsedUrl.query;
      mergePatch(patch, nextPatch);
      patchToDetectedFields(nextPatch, detected, 'url');
    }
  }

  const headerLines = fromHeaderLines(text);
  const split = splitAuthHeaders(headerLines);
  if (Object.keys(split.headersOut).length && !patch.headersJson) {
    mergePatch(patch, { headersJson: split.headersOut });
    addDetected(detected, 'headersJson', split.headersOut, 'medium', 'text');
  }
  if (Object.keys(split.auth).length && !patch.authJson) {
    mergePatch(patch, { authJson: split.auth });
    addDetected(detected, 'authJson', split.auth, 'medium', 'text');
  }

  const bodyFromText = detectRequestBodyFromText(text);
  if (bodyFromText !== undefined && patch.bodyJson === undefined) {
    mergePatch(patch, { bodyJson: bodyFromText });
    addDetected(detected, 'bodyJson', bodyFromText, 'medium', 'text');
  }

  const bodyItemsPath = detectBodyItemsPath(text);
  if (bodyItemsPath) {
    mergePatch(patch, { bodyItemsPath });
    addDetected(detected, 'bodyItemsPath', bodyItemsPath, 'medium', 'text');
  }

  const paginationPatch = detectPaginationHints(text, patch.queryJson || {});
  if (Object.keys(paginationPatch).length) {
    mergePatch(patch, paginationPatch);
    patchToDetectedFields(paginationPatch, detected, 'pagination_hints');
  }

  const responsePath = detectResponsePath(text);
  if (responsePath && !patch.paginationDataPath) {
    mergePatch(patch, { paginationDataPath: responsePath });
    addDetected(detected, 'paginationDataPath', responsePath, 'medium', 'text');
  }

  const rate = detectRateLimitHints(text);
  if (Object.keys(rate.patch).length) {
    mergePatch(patch, rate.patch);
    patchToDetectedFields(rate.patch, detected, 'rate_limit');
  }
  for (const [key, value] of Object.entries(rate.detected)) addDetected(detected, key, value, 'medium', 'text');

  const loggingPatch = detectLoggingHints(text);
  if (Object.keys(loggingPatch).length) {
    mergePatch(patch, loggingPatch);
    patchToDetectedFields(loggingPatch, detected, 'logging_hints');
  }

  const oauthPatch = detectOAuthHints(text);
  if (Object.keys(oauthPatch).length) {
    mergePatch(patch, oauthPatch);
    patchToDetectedFields(oauthPatch, detected, 'auth_hints');
  }

  if (!patch.method && /method|метод/i.test(text)) unresolved.push('Метод упомянут, но не распознан как GET/POST/PUT/PATCH/DELETE.');
  if (!patch.baseUrl && !patch.path) unresolved.push('Endpoint/path не найден.');
  if (/auth|authorization|token|api.?key|oauth|авторизац|токен/i.test(text) && !patch.authJson && !patch.authMode) {
    unresolved.push('Авторизация упомянута, но не распознана достаточно точно.');
  }
  if (/pagination|page|cursor|offset|пагинац|страниц/i.test(text) && !patch.paginationEnabled) {
    unresolved.push('Пагинация упомянута, но стратегия не распознана.');
  }

  const normalizedPatch = normalizeApiExampleApplyPatch(patch);
  const recommended = patchToRecommendations(normalizedPatch);
  const summary = buildSummary(normalizedPatch, detected, unresolved, warnings);
  return {
    summary,
    detected_fields: detected,
    unresolved_items: uniqueStrings(unresolved),
    warnings: uniqueStrings(warnings),
    recommended_changes: recommended,
    apply_patch: normalizedPatch
  };
}

function parseDraftObjectField(value) {
  if (isPlainObject(value)) return value;
  const text = String(value || '').trim();
  if (!text) return {};
  const parsed = safeJsonParse(text);
  return parsed.ok && isPlainObject(parsed.value) ? parsed.value : {};
}

function parseDraftAnyField(value) {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== 'string') return value;
  const text = value.trim();
  if (!text) return undefined;
  const parsed = safeJsonParse(text);
  return parsed.ok ? parsed.value : value;
}

function prettyJson(value) {
  return JSON.stringify(value ?? {}, null, 2);
}

function hasMeaningfulJsonText(value) {
  const text = String(value || '').trim();
  if (!text || text === '{}' || text === '[]') return false;
  return true;
}

function valuesEqual(a, b) {
  return JSON.stringify(a) === JSON.stringify(b);
}

function mergeJsonObjectField(currentRaw, patchObj, fieldLabel, warnings, appliedChanges) {
  const current = parseDraftObjectField(currentRaw);
  const incoming = toPlainObject(patchObj);
  const next = { ...current };
  for (const [key, value] of Object.entries(incoming)) {
    if (Object.prototype.hasOwnProperty.call(current, key) && !valuesEqual(current[key], value)) {
      warnings.push(`${fieldLabel}: поле "${key}" уже заполнено вручную, значение парсера не применено.`);
      continue;
    }
    next[key] = value;
  }
  if (!valuesEqual(current, next)) appliedChanges.push(fieldLabel);
  return prettyJson(next);
}

function setScalar(next, field, value, label, appliedChanges) {
  if (value === undefined || value === null || value === '') return;
  if (!valuesEqual(next[field], value)) {
    next[field] = value;
    appliedChanges.push(label || FIELD_LABELS[field] || field);
  }
}

function setScalarPreserveManual(next, field, value, label, warnings, appliedChanges, manualPredicate) {
  if (value === undefined || value === null || value === '') return;
  const current = next[field];
  if (manualPredicate(current) && !valuesEqual(current, value)) {
    warnings.push(`${label || FIELD_LABELS[field] || field}: существующее значение сохранено, рекомендация конфликтует с ручной настройкой.`);
    return;
  }
  setScalar(next, field, value, label, appliedChanges);
}

function normalizeOutputParameterList(items) {
  if (!Array.isArray(items)) return [];
  return items
    .map((item, idx) => ({
      id: String(item?.id || `api_out_${idx + 1}`).trim(),
      rootPath: String(item?.rootPath || item?.root_path || '').trim(),
      path: String(item?.path || item?.response_path || '').trim(),
      alias: String(item?.alias || normalizeOutputAlias(item?.path || item?.response_path || item?.rootPath || '')).trim(),
      valueType: String(item?.valueType || item?.value_type || 'Текст').trim() || 'Текст'
    }))
    .filter((item) => item.alias && (item.rootPath || item.path));
}

export function applyApiExamplePatchToDraft(draft, resultOrPatch, options = {}) {
  void options;
  const rawPatch = isPlainObject(resultOrPatch?.apply_patch) ? resultOrPatch.apply_patch : resultOrPatch;
  const patch = normalizeApiExampleApplyPatch(rawPatch);
  const next = { ...(isPlainObject(draft) ? draft : {}) };
  const warnings = [];
  const appliedChanges = [];

  setScalar(next, 'method', patch.method, FIELD_LABELS.method, appliedChanges);
  setScalar(next, 'baseUrl', patch.baseUrl, FIELD_LABELS.baseUrl, appliedChanges);
  setScalar(next, 'path', patch.path, FIELD_LABELS.path, appliedChanges);
  if (patch.headersJson) {
    next.headersJson = mergeJsonObjectField(next.headersJson, patch.headersJson, FIELD_LABELS.headersJson, warnings, appliedChanges);
  }
  if (patch.queryJson) {
    next.queryJson = mergeJsonObjectField(next.queryJson, patch.queryJson, FIELD_LABELS.queryJson, warnings, appliedChanges);
  }
  if (patch.authJson) {
    const hasManualAuth = hasMeaningfulJsonText(next.authJson);
    if (hasManualAuth) {
      next.authJson = mergeJsonObjectField(next.authJson, patch.authJson, FIELD_LABELS.authJson, warnings, appliedChanges);
    } else {
      next.authJson = prettyJson(patch.authJson);
      appliedChanges.push(FIELD_LABELS.authJson);
    }
  }
  if (patch.authMode) {
    setScalarPreserveManual(
      next,
      'authMode',
      patch.authMode,
      FIELD_LABELS.authMode,
      warnings,
      appliedChanges,
      (current) => current && current !== 'manual'
    );
  }
  setScalarPreserveManual(next, 'oauth2TokenUrl', patch.oauth2TokenUrl, FIELD_LABELS.oauth2TokenUrl, warnings, appliedChanges, hasMeaningfulJsonText);
  setScalarPreserveManual(next, 'oauth2RequestUrl', patch.oauth2RequestUrl, FIELD_LABELS.oauth2RequestUrl, warnings, appliedChanges, hasMeaningfulJsonText);
  if (Object.prototype.hasOwnProperty.call(patch, 'bodyJson')) {
    const currentBody = parseDraftAnyField(next.bodyJson);
    const hasManualBody = hasMeaningfulJsonText(next.bodyJson);
    if (hasManualBody && !valuesEqual(currentBody, patch.bodyJson)) {
      warnings.push(`${FIELD_LABELS.bodyJson}: существующее body сохранено, рекомендация конфликтует с ручной настройкой.`);
    } else {
      next.bodyJson = prettyJson(patch.bodyJson);
      appliedChanges.push(FIELD_LABELS.bodyJson);
    }
  }

  const paginationFields = [
    'paginationEnabled',
    'paginationStrategy',
    'paginationTarget',
    'paginationDataPath',
    'paginationPageParam',
    'paginationStartPage',
    'paginationLimitParam',
    'paginationLimitValue',
    'paginationOffsetParam',
    'paginationStartOffset',
    'paginationCursorReqPath1',
    'paginationCursorResPath1',
    'paginationNextUrlPath',
    'paginationUseDelay',
    'paginationDelayMs',
    'paginationUseMaxPages',
    'paginationMaxPages',
    'paginationStopOnHttpError',
    'paginationStopOnMissingValue'
  ];
  const currentPaginationManual =
    Boolean(next.paginationEnabled) &&
    (String(next.paginationStrategy || 'none') !== 'none' || String(next.paginationDataPath || '').trim());
  for (const field of paginationFields) {
    if (!Object.prototype.hasOwnProperty.call(patch, field)) continue;
    const isConflict =
      currentPaginationManual &&
      field !== 'paginationEnabled' &&
      next[field] !== undefined &&
      next[field] !== '' &&
      !valuesEqual(next[field], patch[field]);
    if (isConflict) {
      warnings.push(`${FIELD_LABELS[field] || field}: существующая пагинация сохранена, рекомендация требует ручной проверки.`);
      continue;
    }
    setScalar(next, field, patch[field], FIELD_LABELS[field], appliedChanges);
  }

  const currentLogManual = Boolean(next.responseLogEnabled);
  for (const field of [
    'responseLogEnabled',
    'responseLogMode',
    'responseLogOnlyErrors',
    'responseLogWriteRequestPayload',
    'responseLogWriteResponsePayload',
    'responseLogWritePaginationValues'
  ]) {
    if (!Object.prototype.hasOwnProperty.call(patch, field)) continue;
    const isConflict = currentLogManual && field !== 'responseLogEnabled' && !valuesEqual(next[field], patch[field]);
    if (isConflict) {
      warnings.push(`${FIELD_LABELS[field] || field}: существующий API logging сохранен, рекомендация требует ручной проверки.`);
      continue;
    }
    setScalar(next, field, patch[field], FIELD_LABELS[field], appliedChanges);
  }

  setScalar(next, 'executionDelayMs', patch.executionDelayMs, FIELD_LABELS.executionDelayMs, appliedChanges);
  setScalar(next, 'bodyItemsPath', patch.bodyItemsPath, FIELD_LABELS.bodyItemsPath, appliedChanges);

  if (Array.isArray(patch.outputParameters) && patch.outputParameters.length) {
    const current = Array.isArray(next.outputParameters) ? next.outputParameters : [];
    const existingKeys = new Set(current.map((item) => `${String(item?.rootPath || '').trim()}|${String(item?.path || '').trim()}|${String(item?.alias || '').trim()}`));
    const additions = normalizeOutputParameterList(patch.outputParameters).filter((item) => {
      const key = `${item.rootPath}|${item.path}|${item.alias}`;
      if (existingKeys.has(key)) return false;
      existingKeys.add(key);
      return true;
    });
    if (additions.length) {
      next.outputParameters = [...current, ...additions];
      appliedChanges.push(FIELD_LABELS.outputParameters);
    }
  }

  return {
    draft: next,
    applied_changes: uniqueStrings(appliedChanges),
    warnings: uniqueStrings(warnings)
  };
}

export function clearApiExampleInputDraft(draft) {
  return {
    ...(isPlainObject(draft) ? draft : {}),
    exampleRequest: ''
  };
}
