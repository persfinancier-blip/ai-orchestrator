const URL_RE = /\bhttps?:\/\/[^\s<>"')]+/gi;
const METHOD_RE = /\b(GET|POST|PUT|PATCH|DELETE)\b/i;

const DEFAULT_GROUPS = [
  { key: 'main', label: 'Основные настройки' },
  { key: 'request', label: 'Основной запрос' },
  { key: 'auth', label: 'Авторизация' },
  { key: 'headers', label: 'Headers' },
  { key: 'query', label: 'Query' },
  { key: 'body', label: 'Body' },
  { key: 'pagination', label: 'Pagination' },
  { key: 'logging', label: 'Logging' },
  { key: 'output', label: 'Output mapping' }
];

function field(field_key, label, type, group, semantic_key, extra = {}) {
  return {
    field_key,
    label,
    type,
    group,
    semantic_key,
    sensitivity: 'normal',
    supports_ai: true,
    merge_strategy: 'preserve_manual',
    ...extra
  };
}

export const DEFAULT_NODE_EDITOR_SCHEMAS = Object.freeze({
  http_request: {
    schema_version: 1,
    supports_ai: true,
    groups: DEFAULT_GROUPS,
    fields: [
      field('apiMethod', 'Метод', 'select', 'request', 'http_method', {
        draft_path: 'settings.apiMethod',
        enum_values: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE'],
        merge_strategy: 'overwrite_safe',
        required: true
      }),
      field('apiUrl', 'URL', 'url', 'request', 'full_url', {
        draft_path: 'settings.apiUrl',
        merge_strategy: 'overwrite_safe',
        required: true
      }),
      field('apiHeaders', 'Headers JSON', 'json_object', 'headers', 'headers_json', {
        draft_path: 'settings.apiHeaders',
        merge_strategy: 'merge_object'
      }),
      field('apiQuery', 'Query JSON', 'json_object', 'query', 'query_json', {
        draft_path: 'settings.apiQuery',
        merge_strategy: 'merge_object'
      }),
      field('apiBody', 'Body JSON', 'json', 'body', 'body_json', {
        draft_path: 'settings.apiBody',
        merge_strategy: 'preserve_manual'
      })
    ]
  },
  api_request: {
    schema_version: 1,
    supports_ai: true,
    groups: DEFAULT_GROUPS,
    fields: [
      field('method', 'Метод', 'select', 'request', 'http_method', {
        draft_path_by_node_kind: { data: 'apiRequest.method', tool: 'settings.apiMethod' },
        enum_values: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE'],
        merge_strategy: 'overwrite_safe',
        required: true
      }),
      field('url', 'URL', 'url', 'request', 'full_url', {
        draft_path_by_node_kind: { data: 'apiRequest.url', tool: 'settings.apiUrl' },
        merge_strategy: 'overwrite_safe',
        required: true
      }),
      field('authMode', 'Режим авторизации', 'select', 'auth', 'auth_mode', {
        draft_path_by_node_kind: { data: 'apiRequest.authMode', tool: 'settings.apiAuthMode' },
        enum_values: ['manual', 'oauth2_client_credentials', 'custom'],
        sensitivity: 'auth',
        merge_strategy: 'preserve_manual'
      }),
      field('headersText', 'Headers JSON', 'json_object', 'headers', 'headers_json', {
        draft_path_by_node_kind: { data: 'apiRequest.headersText', tool: 'settings.apiHeaders' },
        merge_strategy: 'merge_object'
      }),
      field('queryText', 'Query JSON', 'json_object', 'query', 'query_json', {
        draft_path_by_node_kind: { data: 'apiRequest.queryText', tool: 'settings.apiQuery' },
        merge_strategy: 'merge_object'
      }),
      field('bodyText', 'Body JSON', 'json', 'body', 'body_json', {
        draft_path_by_node_kind: { data: 'apiRequest.bodyText', tool: 'settings.apiBody' },
        merge_strategy: 'preserve_manual'
      }),
      field('paginationDataPath', 'Путь к массиву записей', 'text', 'output', 'records_path', {
        draft_path_by_node_kind: { data: 'apiPagination.dataPath', tool: 'settings.paginationDataPath' },
        merge_strategy: 'preserve_manual'
      })
    ]
  },
  start_process: {
    schema_version: 1,
    supports_ai: true,
    groups: [
      { key: 'main', label: 'Основные настройки' },
      { key: 'schedule', label: 'Запуск' },
      { key: 'scope', label: 'Контекст' }
    ],
    fields: [
      field('name', 'Название процесса', 'text', 'main', 'node_name', { draft_path: 'name', merge_strategy: 'preserve_manual' }),
      field('isEnabled', 'Активность процесса', 'boolean', 'main', 'enabled', {
        draft_path: 'settings.isEnabled',
        enum_values: ['true', 'false'],
        merge_strategy: 'overwrite_safe'
      }),
      field('triggerType', 'Тип запуска', 'select', 'schedule', 'trigger_type', {
        draft_path: 'settings.triggerType',
        enum_values: ['interval', 'cron', 'manual'],
        merge_strategy: 'overwrite_safe'
      }),
      field('intervalValue', 'Интервал', 'number', 'schedule', 'interval_value', {
        draft_path: 'settings.intervalValue',
        merge_strategy: 'overwrite_safe'
      }),
      field('intervalUnit', 'Единица интервала', 'select', 'schedule', 'interval_unit', {
        draft_path: 'settings.intervalUnit',
        enum_values: ['seconds', 'minutes', 'hours'],
        merge_strategy: 'overwrite_safe'
      }),
      field('cron', 'Cron', 'text', 'schedule', 'cron_expression', { draft_path: 'settings.cron', merge_strategy: 'preserve_manual' }),
      field('timezone', 'Часовой пояс', 'text', 'schedule', 'timezone', { draft_path: 'settings.timezone', merge_strategy: 'preserve_manual' }),
      field('runPolicy', 'Правило повторного запуска', 'select', 'schedule', 'run_policy', {
        draft_path: 'settings.runPolicy',
        enum_values: ['single_instance', 'allow_parallel'],
        merge_strategy: 'overwrite_safe'
      }),
      field('executionScopeMode', 'Для кого запускать процесс', 'select', 'scope', 'execution_scope', {
        draft_path: 'settings.executionScopeMode',
        enum_values: ['single_global', 'for_each_tenant', 'for_each_source_account', 'for_each_segment', 'for_each_partition'],
        merge_strategy: 'preserve_manual'
      })
    ]
  },
  condition_if: {
    schema_version: 1,
    supports_ai: true,
    groups: [{ key: 'condition', label: 'Условие' }],
    fields: [
      field('conditionField', 'Поле', 'text', 'condition', 'condition_field', {
        draft_path: 'settings.conditionField',
        required: true,
        merge_strategy: 'overwrite_safe'
      }),
      field('conditionOperator', 'Оператор', 'select', 'condition', 'condition_operator', {
        draft_path: 'settings.conditionOperator',
        enum_values: ['equals', 'not_equals', 'gt', 'gte', 'lt', 'lte', 'contains', 'not_contains', 'is_empty', 'not_empty'],
        merge_strategy: 'overwrite_safe'
      }),
      field('conditionValue', 'Значение', 'text', 'condition', 'condition_value', {
        draft_path: 'settings.conditionValue',
        merge_strategy: 'overwrite_safe'
      })
    ]
  },
  condition_switch: {
    schema_version: 1,
    supports_ai: true,
    groups: [{ key: 'condition', label: 'Переключатель' }],
    fields: [
      field('switchField', 'Поле', 'text', 'condition', 'condition_field', {
        draft_path: 'settings.switchField',
        required: true,
        merge_strategy: 'overwrite_safe'
      }),
      field('switchDefaultPort', 'Порт по умолчанию', 'text', 'condition', 'default_port', {
        draft_path: 'settings.switchDefaultPort',
        merge_strategy: 'preserve_manual'
      })
    ]
  },
  split_data: {
    schema_version: 1,
    supports_ai: true,
    groups: [{ key: 'main', label: 'Разделение' }],
    fields: [
      field('splitMode', 'Режим', 'select', 'main', 'split_mode', {
        draft_path: 'settings.splitMode',
        enum_values: ['duplicate', 'split'],
        merge_strategy: 'overwrite_safe'
      }),
      field('splitMultiplier', 'Коэффициент', 'number', 'main', 'split_multiplier', {
        draft_path: 'settings.splitMultiplier',
        merge_strategy: 'overwrite_safe'
      }),
      field('splitKeyField', 'Техническое поле', 'text', 'main', 'field_name', {
        draft_path: 'settings.splitKeyField',
        merge_strategy: 'preserve_manual'
      }),
      field('splitPrefix', 'Префикс', 'text', 'main', 'prefix', {
        draft_path: 'settings.splitPrefix',
        merge_strategy: 'preserve_manual'
      })
    ]
  },
  merge_data: {
    schema_version: 1,
    supports_ai: true,
    groups: [{ key: 'main', label: 'Объединение' }],
    fields: [
      field('mergeMode', 'Режим', 'select', 'main', 'merge_mode', {
        draft_path: 'settings.mergeMode',
        enum_values: ['dedupe', 'passthrough'],
        merge_strategy: 'overwrite_safe'
      }),
      field('dedupeBy', 'Поля дедупликации', 'text', 'main', 'dedupe_fields', {
        draft_path: 'settings.dedupeBy',
        merge_strategy: 'preserve_manual'
      }),
      field('mergeKeep', 'Какой дубль оставлять', 'select', 'main', 'merge_keep', {
        draft_path: 'settings.mergeKeep',
        enum_values: ['first', 'last'],
        merge_strategy: 'overwrite_safe'
      })
    ]
  },
  code_node: {
    schema_version: 1,
    supports_ai: true,
    groups: [{ key: 'main', label: 'Код' }],
    fields: [
      field('codeTimeoutMs', 'Таймаут, мс', 'number', 'main', 'timeout_ms', {
        draft_path: 'settings.codeTimeoutMs',
        merge_strategy: 'overwrite_safe'
      }),
      field('scriptCode', 'Код', 'code', 'main', 'script_code', {
        draft_path: 'settings.scriptCode',
        sensitivity: 'code',
        supports_ai: false,
        merge_strategy: 'manual_only'
      })
    ]
  }
});

function cleanText(value) {
  return String(value ?? '').trim();
}

function compactWhitespace(value) {
  return cleanText(value).replace(/\s+/g, ' ');
}

function isPlainObject(value) {
  return Boolean(value && typeof value === 'object' && !Array.isArray(value));
}

function cloneJson(value) {
  if (value === undefined) return undefined;
  return JSON.parse(JSON.stringify(value));
}

function confidenceLabel(value) {
  const n = Number(value || 0);
  if (n >= 0.85) return 'high';
  if (n >= 0.6) return 'medium';
  return 'low';
}

function toArray(value) {
  return Array.isArray(value) ? value : [];
}

function normalizeGroups(rawGroups) {
  const groups = toArray(rawGroups)
    .map((item) => ({ key: cleanText(item?.key), label: cleanText(item?.label || item?.key) }))
    .filter((item) => item.key);
  return groups.length ? groups : DEFAULT_GROUPS;
}

export function normalizeNodeEditorSchema(schema, registryRow = {}) {
  const raw = schema && typeof schema === 'object' ? schema : {};
  const fields = toArray(raw.fields)
    .map((item) => {
      const field_key = cleanText(item?.field_key || item?.key);
      if (!field_key) return null;
      return {
        field_key,
        label: cleanText(item?.label || field_key),
        type: cleanText(item?.type || 'text') || 'text',
        group: cleanText(item?.group || 'main') || 'main',
        sensitivity: cleanText(item?.sensitivity || 'normal') || 'normal',
        supports_ai: item?.supports_ai !== false,
        merge_strategy: cleanText(item?.merge_strategy || 'preserve_manual') || 'preserve_manual',
        semantic_key: cleanText(item?.semantic_key || item?.semantic || ''),
        required: Boolean(item?.required),
        enum_values: toArray(item?.enum_values).map((v) => cleanText(v)).filter(Boolean),
        draft_path: cleanText(item?.draft_path || ''),
        draft_path_by_node_kind: isPlainObject(item?.draft_path_by_node_kind) ? cloneJson(item.draft_path_by_node_kind) : undefined
      };
    })
    .filter(Boolean);
  return {
    schema_version: Math.max(1, Number(raw.schema_version || 1) || 1),
    supports_ai: raw.supports_ai !== false && fields.some((item) => item.supports_ai),
    node_type_code: cleanText(registryRow?.node_type_code || raw.node_type_code || ''),
    editor_type_code: cleanText(registryRow?.editor_type_code || raw.editor_type_code || ''),
    groups: normalizeGroups(raw.groups),
    fields
  };
}

function extractUrls(text) {
  return [...String(text || '').matchAll(URL_RE)].map((m) => String(m[0] || '').replace(/[.,;:]+$/, ''));
}

export function classifyNodeAssistantUrl(url) {
  try {
    const parsed = new URL(url);
    const host = parsed.hostname.toLowerCase();
    const path = parsed.pathname.toLowerCase();
    const full = `${host}${path}${parsed.hash || ''}${parsed.search || ''}`.toLowerCase();
    if (
      host.startsWith('docs.') ||
      host.includes('.docs.') ||
      full.includes('/docs') ||
      full.includes('/documentation') ||
      full.includes('/manual') ||
      full.includes('/reference') ||
      full.includes('swagger') ||
      full.includes('openapi') ||
      full.includes('redoc') ||
      full.includes('#tag/')
    ) {
      return 'documentation';
    }
    if (/\/(?:api|v\d+|rest|graphql)(?:\/|$)/i.test(parsed.pathname)) return 'api_endpoint';
    if (METHOD_RE.test(url)) return 'api_endpoint';
    return 'unknown';
  } catch {
    return 'unknown';
  }
}

export function analyzeNodeAssistantSources(text) {
  const urls = extractUrls(text);
  const analyzed = urls.map((url) => ({ url, type: classifyNodeAssistantUrl(url) }));
  return {
    urls: analyzed,
    has_documentation: analyzed.some((item) => item.type === 'documentation'),
    has_endpoint: analyzed.some((item) => item.type === 'api_endpoint'),
    documentation_urls: analyzed.filter((item) => item.type === 'documentation').map((item) => item.url),
    endpoint_urls: analyzed.filter((item) => item.type === 'api_endpoint').map((item) => item.url),
    unknown_urls: analyzed.filter((item) => item.type === 'unknown').map((item) => item.url)
  };
}

function firstLatinSourceToken(text, sourceAnalysis) {
  const hostSource = sourceAnalysis.urls
    .map((item) => {
      try {
        const host = new URL(item.url).hostname.toLowerCase().replace(/^www\./, '').replace(/^docs\./, '');
        const parts = host.split('.').filter(Boolean);
        return parts.length > 1 ? parts[0] : '';
      } catch {
        return '';
      }
    })
    .find(Boolean);
  if (hostSource) return hostSource;
  const raw = String(text || '').toLowerCase();
  const afterSource = raw.match(/(?:для|из|from|source|marketplace|маркетплейс)\s+([a-z][a-z0-9_-]{2,})/i);
  if (afterSource?.[1]) return afterSource[1];
  const token = raw.match(/\b([a-z][a-z0-9_-]{2,})\b/i)?.[1] || '';
  return ['get', 'post', 'put', 'patch', 'delete', 'api', 'http', 'json', 'url', 'docs'].includes(token) ? '' : token;
}

function extractAction(text) {
  const raw = String(text || '').toLowerCase();
  const actionRules = [
    { action: 'get', operation_prefix: 'List', match: /(получ|выгруз|спис|загруз|list|get|read|fetch)/i },
    { action: 'create', operation_prefix: 'Create', match: /(созда|create|add|insert)/i },
    { action: 'update', operation_prefix: 'Update', match: /(обнов|измен|update|patch)/i },
    { action: 'delete', operation_prefix: 'Delete', match: /(удал|delete|remove)/i },
    { action: 'filter', operation_prefix: 'Filter', match: /(если|услов|filter|where)/i },
    { action: 'split', operation_prefix: 'Split', match: /(раздел|split|дублир|duplicate)/i },
    { action: 'merge', operation_prefix: 'Merge', match: /(объедин|дедуп|merge|dedupe)/i },
    { action: 'schedule', operation_prefix: 'Schedule', match: /(кажд|распис|cron|manual|вручн|schedule|every)/i }
  ];
  return actionRules.find((item) => item.match.test(raw)) || null;
}

function extractOperation(text, actionRule) {
  const raw = String(text || '');
  const explicit = raw.match(/\b([A-Z][A-Za-z0-9_]{3,})\b/)?.[1] || '';
  if (explicit && !['HTTP', 'JSON', 'API', 'URL'].includes(explicit.toUpperCase())) return explicit;
  const object = raw
    .toLowerCase()
    .match(/(?:получить|выгрузить|список|get|list|fetch|create|update|delete)\s+([a-zа-я0-9_-]{3,})/i)?.[1];
  if (!object || !actionRule?.operation_prefix) return '';
  return `${actionRule.operation_prefix}${object.charAt(0).toUpperCase()}${object.slice(1)}`;
}

export function extractNodeAssistantIntent(text, sourceAnalysis = analyzeNodeAssistantSources(text)) {
  const raw = String(text || '');
  const actionRule = extractAction(raw);
  const source = firstLatinSourceToken(raw, sourceAnalysis);
  const operation = extractOperation(raw, actionRule);
  const confidence = actionRule && (source || operation || sourceAnalysis.urls.length) ? 0.82 : actionRule ? 0.7 : source || operation ? 0.58 : 0.2;
  return {
    identified: confidence >= 0.6,
    action: actionRule?.action || '',
    source,
    operation,
    confidence,
    explanation:
      confidence >= 0.6
        ? 'Intent выделен из цели пользователя, URL и структуры запроса.'
        : 'Недостаточно признаков для уверенного intent. Нужна цель ноды или операция.'
  };
}

function parseJsonSnippet(text) {
  const raw = String(text || '');
  const start = raw.indexOf('{');
  const end = raw.lastIndexOf('}');
  if (start < 0 || end <= start) return undefined;
  try {
    return JSON.parse(raw.slice(start, end + 1));
  } catch {
    return undefined;
  }
}

function parseEndpointText(text) {
  const raw = String(text || '');
  const methodUrl = raw.match(/\b(GET|POST|PUT|PATCH|DELETE)\s+(https?:\/\/[^\s'")]+|\/[A-Za-z0-9_./{}:-]+(?:\?[^\s'")]+)?)/i);
  if (methodUrl) return { method: methodUrl[1].toUpperCase(), endpoint: methodUrl[2] };
  const curlUrl = raw.match(/curl[\s\S]{0,500}?(?:-X\s+(GET|POST|PUT|PATCH|DELETE))?[\s\S]{0,500}?['"](https?:\/\/[^'"]+)['"]/i);
  if (curlUrl) return { method: (curlUrl[1] || '').toUpperCase() || '', endpoint: curlUrl[2] };
  return { method: '', endpoint: '' };
}

function urlParts(endpoint) {
  try {
    const parsed = new URL(endpoint);
    const query = {};
    for (const [key, value] of parsed.searchParams.entries()) query[key] = value;
    return { base_url: parsed.origin, path: parsed.pathname || '/', query };
  } catch {
    return null;
  }
}

function fact(value, confidence, source_type, excerpt) {
  if (value === undefined || value === null || value === '') return null;
  return { value, confidence, source_type, excerpt: compactWhitespace(excerpt).slice(0, 260) };
}

function addFact(target, key, nextFact) {
  if (!nextFact) return;
  const current = target[key];
  if (!current || Number(nextFact.confidence || 0) > Number(current.confidence || 0)) target[key] = nextFact;
}

function parseHeaders(text, sourceType) {
  const headers = {};
  const raw = String(text || '');
  const headerLines = [...raw.matchAll(/(?:-H\s+['"]([^:'"]+)\s*:\s*([^'"]+)['"]|^([A-Za-z0-9-]+)\s*:\s*([^\n\r]+))/gim)];
  for (const match of headerLines) {
    const key = cleanText(match[1] || match[3]);
    const value = cleanText(match[2] || match[4]);
    if (!key || !value || /^(GET|POST|PUT|PATCH|DELETE)$/i.test(key)) continue;
    headers[key] = value;
  }
  return Object.keys(headers).length ? fact(headers, 0.78, sourceType, 'headers') : null;
}

function parseInterval(text) {
  const raw = String(text || '').toLowerCase();
  const hit = raw.match(/(?:кажд[а-яё]*|every)\s+(\d+)\s*(сек|second|мин|minute|час|hour)/i);
  if (!hit) return null;
  const unitRaw = hit[2].toLowerCase();
  const unit = unitRaw.startsWith('сек') || unitRaw.startsWith('second') ? 'seconds' : unitRaw.startsWith('час') || unitRaw.startsWith('hour') ? 'hours' : 'minutes';
  return { value: hit[1], unit };
}

function parseCondition(text) {
  const raw = String(text || '');
  const hit = raw.match(/(?:если|if)\s+([A-Za-zА-Яа-я0-9_.-]+)\s*(>=|<=|!=|=|>|<|содержит|contains)\s*([^\n,.;]+)/i);
  if (!hit) return null;
  const opMap = {
    '>': 'gt',
    '>=': 'gte',
    '<': 'lt',
    '<=': 'lte',
    '=': 'equals',
    '!=': 'not_equals',
    contains: 'contains',
    содержит: 'contains'
  };
  return { field: hit[1], operator: opMap[String(hit[2]).toLowerCase()] || 'equals', value: cleanText(hit[3]) };
}

function factsFromText(text, sourceType = 'user_text') {
  const facts = {};
  const raw = String(text || '');
  const endpoint = parseEndpointText(raw);
  if (endpoint.endpoint) {
    if (endpoint.method) addFact(facts, 'method', fact(endpoint.method, 0.9, sourceType, endpoint.method));
    const parts = urlParts(endpoint.endpoint);
    if (parts) {
      const urlType = classifyNodeAssistantUrl(endpoint.endpoint);
      if (urlType === 'api_endpoint') {
        addFact(facts, 'base_url', fact(parts.base_url, 0.9, 'api_endpoint', endpoint.endpoint));
        addFact(facts, 'path', fact(parts.path, 0.9, 'api_endpoint', endpoint.endpoint));
        if (Object.keys(parts.query).length) addFact(facts, 'query_json', fact(parts.query, 0.82, 'api_endpoint', endpoint.endpoint));
      }
    } else if (String(endpoint.endpoint || '').startsWith('/')) {
      addFact(facts, 'path', fact(endpoint.endpoint, 0.82, sourceType, endpoint.endpoint));
    }
  }

  const headersFact = parseHeaders(raw, sourceType);
  addFact(facts, 'headers_json', headersFact);

  const json = parseJsonSnippet(raw);
  if (json !== undefined) addFact(facts, 'body_json', fact(json, 0.66, 'text_json', 'JSON-фрагмент из текста'));

  const recordsPath = raw.match(/(?:records|items|data|result|response)[\s._-]*(?:path|путь)\s*[:=]\s*([A-Za-z0-9_.[\]-]+)/i)?.[1];
  if (recordsPath) addFact(facts, 'records_path', fact(recordsPath, 0.78, sourceType, recordsPath));

  const interval = parseInterval(raw);
  if (interval) {
    addFact(facts, 'trigger_type', fact('interval', 0.86, sourceType, `каждые ${interval.value}`));
    addFact(facts, 'interval_value', fact(interval.value, 0.9, sourceType, `каждые ${interval.value}`));
    addFact(facts, 'interval_unit', fact(interval.unit, 0.9, sourceType, interval.unit));
  } else if (/ручн|manual/i.test(raw)) {
    addFact(facts, 'trigger_type', fact('manual', 0.84, sourceType, 'ручной запуск'));
  }

  const condition = parseCondition(raw);
  if (condition) {
    addFact(facts, 'condition_field', fact(condition.field, 0.86, sourceType, condition.field));
    addFact(facts, 'condition_operator', fact(condition.operator, 0.86, sourceType, condition.operator));
    addFact(facts, 'condition_value', fact(condition.value, 0.82, sourceType, condition.value));
  }

  if (/дедуп|дублик|dedupe/i.test(raw)) addFact(facts, 'merge_mode', fact('dedupe', 0.82, sourceType, 'дедупликация'));
  if (/без изменений|passthrough/i.test(raw)) addFact(facts, 'merge_mode', fact('passthrough', 0.82, sourceType, 'без изменений'));
  const split = raw.match(/(?:дублир|duplicate|размнож)\D+(\d+)/i);
  if (split) {
    addFact(facts, 'split_mode', fact('duplicate', 0.82, sourceType, split[0]));
    addFact(facts, 'split_multiplier', fact(split[1], 0.86, sourceType, split[0]));
  }
  return facts;
}

function factsFromDocumentation(text, sourceUrl = '') {
  const facts = factsFromText(text, 'documentation');
  const raw = String(text || '');
  const baseHits = [
    ...raw.matchAll(/(?:base\s*url|baseUrl|host|сервер|базов\w*\s+url)\s*[:=-]?\s*(https?:\/\/[^\s<>"')]+)/gi)
  ];
  for (const hit of baseHits) {
    const value = cleanText(hit[1]).replace(/[.,;]+$/, '');
    addFact(facts, 'base_url', fact(value, 0.84, 'documentation', hit[0]));
  }
  const methodPath = raw.match(/\b(GET|POST|PUT|PATCH|DELETE)\s+(\/[A-Za-z0-9_./{}:-]+(?:\?[^\s'")<]+)?)/i);
  if (methodPath) {
    addFact(facts, 'method', fact(methodPath[1].toUpperCase(), 0.86, 'documentation', methodPath[0]));
    addFact(facts, 'path', fact(methodPath[2], 0.86, 'documentation', methodPath[0]));
  }
  const authHint = raw.match(/\b(?:Authorization|Bearer|OAuth|API[-\s]?Key|Client[-\s]?Id|token)\b/i);
  if (authHint) addFact(facts, 'auth_mode', fact('manual', 0.62, 'documentation', authHint[0]));
  if (sourceUrl) {
    for (const key of Object.keys(facts)) {
      facts[key].source_url = sourceUrl;
    }
  }
  return facts;
}

function factsFromClarificationAnswers(answers) {
  const facts = {};
  const source = answers && typeof answers === 'object' ? answers : {};
  if (source.full_url) {
    const parts = urlParts(cleanText(source.full_url));
    if (parts) {
      addFact(facts, 'base_url', fact(parts.base_url, 0.96, 'clarification', 'Ответ пользователя'));
      addFact(facts, 'path', fact(parts.path, 0.96, 'clarification', 'Ответ пользователя'));
      if (Object.keys(parts.query).length) addFact(facts, 'query_json', fact(parts.query, 0.9, 'clarification', 'Ответ пользователя'));
    }
  }
  if (source.base_url) addFact(facts, 'base_url', fact(cleanText(source.base_url), 0.96, 'clarification', 'Ответ пользователя'));
  if (source.endpoint_path) addFact(facts, 'path', fact(cleanText(source.endpoint_path), 0.96, 'clarification', 'Ответ пользователя'));
  if (source.method) addFact(facts, 'method', fact(cleanText(source.method).toUpperCase(), 0.96, 'clarification', 'Ответ пользователя'));
  if (source.records_path) addFact(facts, 'records_path', fact(cleanText(source.records_path), 0.92, 'clarification', 'Ответ пользователя'));
  if (source.intent_goal) return mergeFacts(facts, factsFromText(source.intent_goal, 'clarification'));
  return facts;
}

function mergeFacts(...items) {
  const out = {};
  for (const item of items) {
    for (const [key, itemFact] of Object.entries(item || {})) addFact(out, key, itemFact);
  }
  return out;
}

function composeFullUrl(facts) {
  const base = cleanText(facts.base_url?.value);
  const path = cleanText(facts.path?.value);
  if (!base || !path) return null;
  return `${base.replace(/\/+$/, '')}/${path.replace(/^\/+/, '')}`;
}

function factForSemantic(semantic, facts) {
  if (!semantic) return null;
  if (semantic === 'full_url') {
    const full = composeFullUrl(facts);
    if (!full) return null;
    const confidence = Math.min(Number(facts.base_url?.confidence || 0), Number(facts.path?.confidence || 0));
    const sourceFact = Number(facts.path?.confidence || 0) >= Number(facts.base_url?.confidence || 0) ? facts.path : facts.base_url;
    return fact(full, confidence || 0.75, sourceFact?.source_type || 'knowledge', sourceFact?.excerpt || full);
  }
  if (semantic === 'http_method') return facts.method || null;
  return facts[semantic] || null;
}

function valueIsEmpty(value) {
  if (value === undefined || value === null) return true;
  if (typeof value === 'string') return value.trim() === '' || value.trim() === '{}' || value.trim() === '[]';
  if (Array.isArray(value)) return value.length === 0;
  if (isPlainObject(value)) return Object.keys(value).length === 0;
  return false;
}

function currentValueForField(currentValues, field) {
  if (!currentValues || typeof currentValues !== 'object') return '';
  if (Object.prototype.hasOwnProperty.call(currentValues, field.field_key)) return currentValues[field.field_key];
  return '';
}

function fieldDraftPath(field, nodeKind = '') {
  const byKind = field?.draft_path_by_node_kind && typeof field.draft_path_by_node_kind === 'object' ? field.draft_path_by_node_kind : {};
  return cleanText(byKind[nodeKind] || field?.draft_path || '');
}

function recommendationForField(field, fieldFact, currentValue, sourceAnalysis, nodeKind) {
  if (!fieldFact) return null;
  const confidence = Number(fieldFact.confidence || 0);
  const currentEmpty = valueIsEmpty(currentValue);
  const mergeMode = field.merge_strategy || 'preserve_manual';
  const sensitive = ['secret', 'token', 'auth', 'credential', 'code'].includes(String(field.sensitivity || '').toLowerCase());
  let apply_allowed = Boolean(field.supports_ai) && confidence >= 0.8 && !sensitive;
  let warning = '';
  if (!field.supports_ai) {
    apply_allowed = false;
    warning = 'Поле не поддерживает AI-подстановку по schema.';
  } else if (sensitive) {
    apply_allowed = false;
    warning = 'Чувствительное поле требует ручной проверки.';
  } else if (!currentEmpty && mergeMode === 'preserve_manual') {
    apply_allowed = false;
    warning = 'В поле уже есть ручное значение; автоподстановка отключена.';
  } else if (confidence < 0.8) {
    apply_allowed = false;
    warning = 'Недостаточная уверенность для автоподстановки.';
  }
  if (field.semantic_key === 'full_url' && sourceAnalysis.has_documentation && !sourceAnalysis.has_endpoint && fieldFact.source_type !== 'clarification' && fieldFact.source_type !== 'documentation') {
    apply_allowed = false;
    warning = 'URL документации не используется как API endpoint.';
  }
  return {
    field_key: field.field_key,
    label: field.label,
    group: field.group,
    semantic_key: field.semantic_key,
    draft_path: fieldDraftPath(field, nodeKind),
    current_value: cloneJson(currentValue),
    suggested_value: cloneJson(fieldFact.value),
    confidence: confidenceLabel(confidence),
    confidence_score: confidence,
    source_type: fieldFact.source_type || 'unknown',
    source_url: fieldFact.source_url || '',
    source_excerpt: cleanText(fieldFact.excerpt || ''),
    merge_mode: mergeMode,
    apply_allowed,
    warning,
    explanation: apply_allowed
      ? 'Значение сопоставлено со schema поля и будет подставлено только в draft UI.'
      : warning || 'Поле требует ручной проверки.'
  };
}

function buildClarificationQuestions({ intent, sourceAnalysis, schema, facts, recommendations }) {
  const questions = [];
  if (!intent.identified) {
    questions.push({
      id: 'intent_goal',
      prompt: 'Не удалось уверенно определить цель. Опишите, что должна сделать эта нода.',
      answer_key: 'intent_goal',
      answer_type: 'text',
      examples: ['получить список кампаний из маркетплейса', 'запускать процесс каждые 15 минут', 'если metrics.drr > 20']
    });
  }

  const needsUrl = schema.fields.some((item) => item.supports_ai && item.required && item.semantic_key === 'full_url');
  if (needsUrl && sourceAnalysis.has_documentation && !facts.base_url) {
    questions.push({
      id: 'missing_base_url',
      prompt: 'В тексте есть ссылка на документацию, но base URL API не найден с достаточной уверенностью. Укажите base URL или пришлите фрагмент документации с базовым адресом.',
      field_key: schema.fields.find((item) => item.semantic_key === 'full_url')?.field_key || '',
      answer_key: 'base_url',
      answer_type: 'text',
      examples: ['https://api.example.com']
    });
  }
  if (needsUrl && !facts.path) {
    questions.push({
      id: 'missing_endpoint_path',
      prompt: 'Не найден endpoint/path операции. Укажите путь запроса или уточните операцию.',
      field_key: schema.fields.find((item) => item.semantic_key === 'full_url')?.field_key || '',
      answer_key: 'endpoint_path',
      answer_type: 'text',
      examples: ['/v1/campaigns', 'GET /api/client/campaign']
    });
  }

  const requiredMissing = schema.fields.filter(
    (fieldItem) =>
      fieldItem.required &&
      fieldItem.supports_ai &&
      !recommendations.some((rec) => rec.field_key === fieldItem.field_key && rec.apply_allowed)
  );
  for (const fieldItem of requiredMissing.slice(0, 4)) {
    const id = `required_${fieldItem.field_key}`;
    if (questions.some((q) => q.id === id || q.field_key === fieldItem.field_key)) continue;
    questions.push({
      id,
      field_key: fieldItem.field_key,
      prompt: `Не хватает надежного значения для поля "${fieldItem.label}". Укажите его вручную или уточните задачу.`,
      answer_key: fieldItem.semantic_key === 'full_url' ? 'full_url' : fieldItem.field_key,
      answer_type: 'text'
    });
  }
  return questions;
}

function buildSearchQuery(intent, userText, schema) {
  const parts = [];
  if (intent.source) parts.push(intent.source);
  if (intent.operation) parts.push(intent.operation);
  if (intent.action) parts.push(intent.action);
  if (!parts.length) parts.push(compactWhitespace(userText).slice(0, 80));
  if (schema.fields.some((item) => item.semantic_key === 'full_url')) parts.push('api documentation endpoint');
  return parts.filter(Boolean).join(' ');
}

function normalizeKnowledgeInput(input) {
  const docs = toArray(input.documentation_texts || input.documentation_sources)
    .map((item) => {
      if (typeof item === 'string') return { url: '', text: item };
      return { url: cleanText(item?.url), text: cleanText(item?.text || item?.content) };
    })
    .filter((item) => item.text);
  const webResults = toArray(input.web_results)
    .map((item) => ({
      title: cleanText(item?.title),
      url: cleanText(item?.url),
      snippet: cleanText(item?.snippet || item?.text)
    }))
    .filter((item) => item.title || item.snippet || item.url);
  return { docs, webResults };
}

function resolveKnowledge({ userText, sourceAnalysis, intent, schema, clarificationAnswers, knowledgeInput }) {
  const docFacts = knowledgeInput.docs.map((doc) => factsFromDocumentation(doc.text, doc.url));
  const webFacts = knowledgeInput.webResults.map((result) => factsFromDocumentation(`${result.title}\n${result.url}\n${result.snippet}`, result.url));
  const facts = mergeFacts(
    factsFromText(userText, 'user_text'),
    ...docFacts,
    ...webFacts,
    factsFromClarificationAnswers(clarificationAnswers)
  );
  return {
    facts,
    sources: {
      user_text: Boolean(userText),
      documentation: knowledgeInput.docs.map((doc) => ({ url: doc.url, chars: doc.text.length })),
      web_results: knowledgeInput.webResults.map((item) => ({ title: item.title, url: item.url, snippet: item.snippet.slice(0, 180) })),
      search_query: sourceAnalysis.urls.length ? '' : buildSearchQuery(intent, userText, schema)
    }
  };
}

function buildApplyPatch(recommendations) {
  const fields = recommendations
    .filter((rec) => rec.apply_allowed)
    .map((rec) => ({
      field_key: rec.field_key,
      draft_path: rec.draft_path,
      value: cloneJson(rec.suggested_value),
      merge_mode: rec.merge_mode,
      source_type: rec.source_type,
      confidence: rec.confidence,
      confidence_score: rec.confidence_score
    }));
  return {
    fields,
    by_field: Object.fromEntries(fields.map((item) => [item.field_key, cloneJson(item.value)]))
  };
}

export async function runNodeAssistantFlow(input = {}) {
  const userText = cleanText(input.user_text || input.text || '');
  const registryRow = input.registry_row && typeof input.registry_row === 'object' ? input.registry_row : {};
  const rawSchema = input.editor_schema || registryRow.editor_schema_json || registryRow.editor_schema || null;
  const schema = normalizeNodeEditorSchema(rawSchema, registryRow);
  const nodeTypeCode = cleanText(input.node_type_code || registryRow.node_type_code || schema.node_type_code);
  const editorTypeCode = cleanText(input.editor_type_code || registryRow.editor_type_code || schema.editor_type_code);
  const nodeKind = cleanText(input.node_kind || '');
  const currentValues = input.current_values && typeof input.current_values === 'object' ? input.current_values : {};
  const clarificationAnswers = input.clarification_answers && typeof input.clarification_answers === 'object' ? input.clarification_answers : {};
  const warnings = [];
  const unresolved = [];

  const sourceAnalysis = analyzeNodeAssistantSources(userText);
  const intent = extractNodeAssistantIntent(`${userText}\n${clarificationAnswers.intent_goal || ''}`, sourceAnalysis);

  const nodeContext = {
    node_type_code: nodeTypeCode,
    editor_type_code: editorTypeCode,
    node_kind: nodeKind,
    schema
  };

  if (!schema.supports_ai) {
    return {
      status: 'unsupported',
      summary: 'AI не поддерживается для этой ноды: в node_registry_store нет editor_schema_json с AI-полями.',
      node_context: nodeContext,
      intent,
      source_analysis: sourceAnalysis,
      knowledge_resolution: { facts: {}, sources: {} },
      recognized: [],
      detected_fields: [],
      unresolved_items: ['Нет schema полей с supports_ai=true.'],
      warnings: [],
      recommendations: [],
      recommended_changes: [],
      clarification_questions: [],
      apply_patch: { fields: [], by_field: {} },
      pipeline: ['node_context', 'intent_extraction', 'source_analysis']
    };
  }

  if (sourceAnalysis.has_documentation) {
    warnings.push('URL документации распознан отдельно от API endpoint. Сама ссылка на manual не будет подставлена как base_url или URL запроса.');
  }
  if (!sourceAnalysis.urls.length && input.web_search_attempted && !toArray(input.web_results).length) {
    warnings.push('Веб-поиск не вернул надежных источников. Нужна ссылка на документацию или пример запроса.');
  }

  const knowledgeInput = normalizeKnowledgeInput(input);
  const knowledge = resolveKnowledge({ userText, sourceAnalysis, intent, schema, clarificationAnswers, knowledgeInput });
  const facts = knowledge.facts;

  const recommendations = [];
  const recognized = [];
  for (const schemaField of schema.fields) {
    const semanticFact = factForSemantic(schemaField.semantic_key, facts);
    const currentValue = currentValueForField(currentValues, schemaField);
    const rec = recommendationForField(schemaField, semanticFact, currentValue, sourceAnalysis, nodeKind);
    if (rec) {
      recommendations.push(rec);
      recognized.push({
        field_key: schemaField.field_key,
        key: schemaField.field_key,
        label: schemaField.label,
        group: schemaField.group,
        value: cloneJson(rec.suggested_value),
        confidence: rec.confidence,
        source_type: rec.source_type
      });
    } else if (schemaField.required && schemaField.supports_ai) {
      unresolved.push(`Не найдено надежное значение для обязательного поля "${schemaField.label}".`);
    }
  }

  for (const rec of recommendations) {
    if (rec.warning) warnings.push(`${rec.label}: ${rec.warning}`);
  }

  const questions = buildClarificationQuestions({ intent, sourceAnalysis, schema, facts, recommendations });
  const applyPatch = buildApplyPatch(recommendations);
  const status = questions.length ? 'clarification_required' : applyPatch.fields.length ? 'ready' : 'no_safe_recommendations';

  return {
    status,
    summary:
      status === 'ready'
        ? 'Рекомендации готовы. Применение подставит значения только в draft UI ноды; сохранение выполняется вручную.'
        : status === 'clarification_required'
        ? 'Нужны уточнения перед подстановкой значений. Ответьте на вопросы или добавьте ссылку/пример запроса.'
        : 'Нет безопасных рекомендаций для автоподстановки. Данные не будут придуманы.',
    node_context: nodeContext,
    intent,
    source_analysis: sourceAnalysis,
    knowledge_resolution: {
      facts: Object.fromEntries(
        Object.entries(facts).map(([key, value]) => [
          key,
          {
            value: cloneJson(value.value),
            confidence: confidenceLabel(value.confidence),
            confidence_score: value.confidence,
            source_type: value.source_type,
            source_url: value.source_url || '',
            source_excerpt: value.excerpt || ''
          }
        ])
      ),
      sources: knowledge.sources
    },
    recognized,
    detected_fields: recognized,
    unresolved_items: [...new Set(unresolved)],
    warnings: [...new Set(warnings)],
    recommendations,
    recommended_changes: recommendations
      .filter((rec) => rec.apply_allowed)
      .map((rec) => ({ field: rec.field_key, label: rec.label, value: cloneJson(rec.suggested_value), action: rec.merge_mode })),
    clarification_questions: questions,
    apply_patch: applyPatch,
    pipeline: [
      'node_context',
      'intent_extraction',
      'source_analysis',
      'knowledge_resolution',
      'field_mapping',
      'recommendation_generation',
      questions.length ? 'clarification_loop' : 'draft_apply_ready'
    ]
  };
}

export function recommendNodeSettings(input = {}) {
  return runNodeAssistantFlow(input);
}
