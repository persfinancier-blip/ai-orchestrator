import { tableColumnsDetailed } from './writeRuntime.mjs';

const DEFAULT_BATCH_SIZE = 200;
const DEFAULT_PREVIEW_LIMIT = 20;

function isIdent(value) {
  return typeof value === 'string' && /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(value);
}

function qi(ident) {
  if (!isIdent(ident)) throw new Error(`invalid_identifier:${ident}`);
  return `"${ident}"`;
}

function qname(schema, table) {
  return `${qi(schema)}.${qi(table)}`;
}

function parseJsonSafe(raw, fallback) {
  if (raw && typeof raw === 'object') return raw;
  const txt = String(raw || '').trim();
  if (!txt) return fallback;
  try {
    return JSON.parse(txt);
  } catch {
    return fallback;
  }
}

function parseJsonArraySafe(raw, fallback = []) {
  const parsed = parseJsonSafe(raw, fallback);
  return Array.isArray(parsed) ? parsed : fallback;
}

function parseJsonObjectSafe(raw, fallback = {}) {
  const parsed = parseJsonSafe(raw, fallback);
  return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : fallback;
}

function parseCsvList(raw) {
  return String(raw || '')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

function deepClone(value) {
  return value && typeof value === 'object' ? JSON.parse(JSON.stringify(value)) : value;
}

function columnsFromRows(rows = []) {
  return [
    ...new Set(
      (Array.isArray(rows) ? rows : []).flatMap((row) =>
        row && typeof row === 'object' && !Array.isArray(row) ? Object.keys(row) : []
      )
    )
  ];
}

function parsePathParts(path) {
  const raw = String(path || '').trim();
  if (!raw) return [];
  const normalized = raw.replace(/\[(\d+)\]/g, '.$1');
  return normalized
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

function normalizeObjectRow(value) {
  return value && typeof value === 'object' && !Array.isArray(value) ? value : { value };
}

function defaultOutputName(alias, fieldName) {
  const cleanField = String(fieldName || '').trim();
  const cleanAlias = String(alias || '').trim() || 'src';
  if (!cleanField) return cleanAlias;
  return cleanAlias === 'base' ? cleanField : `${cleanAlias}_${cleanField}`;
}

function convertFieldType(value, typeName) {
  const type = String(typeName || '').trim().toLowerCase();
  if (!type) return value;
  if (value === undefined || value === null || value === '') return value;
  if (type === 'int' || type === 'integer' || type === 'bigint') {
    const n = Number(value);
    return Number.isFinite(n) ? Math.trunc(n) : value;
  }
  if (type === 'numeric' || type === 'float' || type === 'number') {
    const n = Number(value);
    return Number.isFinite(n) ? n : value;
  }
  if (type === 'boolean' || type === 'bool') {
    if (typeof value === 'boolean') return value;
    return ['1', 'true', 'yes', 'on', 'enabled'].includes(String(value || '').trim().toLowerCase());
  }
  if (type === 'json' || type === 'jsonb') {
    if (typeof value === 'string') {
      try {
        return JSON.parse(value);
      } catch {
        return value;
      }
    }
    return value;
  }
  if (type === 'text' || type === 'string') return String(value);
  if (type === 'date' || type === 'timestamp' || type === 'timestamptz') {
    const dt = new Date(value);
    return Number.isFinite(dt.getTime()) ? dt.toISOString() : value;
  }
  return value;
}

function parseDateValue(value) {
  if (value === undefined || value === null || value === '') return null;
  const dt = new Date(value);
  return Number.isFinite(dt.getTime()) ? dt : null;
}

function buildExpressionFunctions() {
  const numericArgs = (args) =>
    args
      .map((value) => Number(value))
      .filter((value) => Number.isFinite(value));
  return {
    _if: (cond, whenTrue, whenFalse) => (cond ? whenTrue : whenFalse),
    _and: (...args) => args.every(Boolean),
    _or: (...args) => args.some(Boolean),
    _not: (value) => !value,
    _empty: (value) => value === undefined || value === null || String(value) === '',
    _today: () => new Date().toISOString().slice(0, 10),
    _now: () => new Date().toISOString(),
    _year: (value) => {
      const dt = parseDateValue(value);
      return dt ? dt.getUTCFullYear() : null;
    },
    _month: (value) => {
      const dt = parseDateValue(value);
      return dt ? dt.getUTCMonth() + 1 : null;
    },
    _day: (value) => {
      const dt = parseDateValue(value);
      return dt ? dt.getUTCDate() : null;
    },
    _date: (year, month, day) => {
      const y = Number(year);
      const m = Number(month);
      const d = Number(day);
      if (!Number.isFinite(y) || !Number.isFinite(m) || !Number.isFinite(d)) return null;
      return new Date(Date.UTC(Math.trunc(y), Math.max(0, Math.trunc(m) - 1), Math.trunc(d))).toISOString().slice(0, 10);
    },
    _dateDiff: (left, right, unit = 'day') => {
      const a = parseDateValue(left);
      const b = parseDateValue(right);
      if (!a || !b) return null;
      const diffMs = a.getTime() - b.getTime();
      const normalizedUnit = String(unit || 'day').trim().toLowerCase();
      if (normalizedUnit === 'hour') return Math.trunc(diffMs / 3600000);
      if (normalizedUnit === 'minute') return Math.trunc(diffMs / 60000);
      return Math.trunc(diffMs / 86400000);
    },
    _length: (value) => (value === undefined || value === null ? 0 : String(value).length),
    _substring: (value, start, len) => {
      const text = String(value ?? '');
      const from = Math.max(0, Math.trunc(Number(start || 0)));
      if (len === undefined || len === null || len === '') return text.slice(from);
      const size = Math.max(0, Math.trunc(Number(len || 0)));
      return text.slice(from, from + size);
    },
    _upper: (value) => String(value ?? '').toUpperCase(),
    _lower: (value) => String(value ?? '').toLowerCase(),
    _replace: (value, from, to) => String(value ?? '').split(String(from ?? '')).join(String(to ?? '')),
    _min: (...args) => {
      const values = numericArgs(args);
      return values.length ? Math.min(...values) : null;
    },
    _max: (...args) => {
      const values = numericArgs(args);
      return values.length ? Math.max(...values) : null;
    },
    _round: (value, digits = 0) => {
      const num = Number(value);
      const scale = Number(digits);
      if (!Number.isFinite(num)) return null;
      if (!Number.isFinite(scale)) return num;
      const factor = 10 ** Math.max(0, Math.trunc(scale));
      return Math.round(num * factor) / factor;
    },
    _concat: (...args) => args.map((value) => String(value ?? '')).join(''),
    _contains: (value, search) => String(value ?? '').includes(String(search ?? '')),
    _notContains: (value, search) => !String(value ?? '').includes(String(search ?? ''))
  };
}

const COMPUTED_FN_REPLACEMENTS = [
  [/\u0435\u0441\u043b\u0438\s*\(/gi, 'fn._if('],
  [/\u0438\s*\(/gi, 'fn._and('],
  [/\u0438\u043b\u0438\s*\(/gi, 'fn._or('],
  [/\u043d\u0435\s*\(/gi, 'fn._not('],
  [/\u043f\u0443\u0441\u0442\u043e\s*\(/gi, 'fn._empty('],
  [/\u0441\u0435\u0433\u043e\u0434\u043d\u044f\s*\(/gi, 'fn._today('],
  [/\u0441\u0435\u0439\u0447\u0430\u0441\s*\(/gi, 'fn._now('],
  [/\u0433\u043e\u0434\s*\(/gi, 'fn._year('],
  [/\u043c\u0435\u0441\u044f\u0446\s*\(/gi, 'fn._month('],
  [/\u0434\u0435\u043d\u044c\s*\(/gi, 'fn._day('],
  [/\u0434\u0430\u0442\u0430_\u0440\u0430\u0437\u043d\u0438\u0446\u0430\s*\(/gi, 'fn._dateDiff('],
  [/\u0434\u0430\u0442\u0430\s*\(/gi, 'fn._date('],
  [/\u0434\u043b\u0438\u043d\u0430\s*\(/gi, 'fn._length('],
  [/\u043f\u043e\u0434\u0441\u0442\u0440\u043e\u043a\u0430\s*\(/gi, 'fn._substring('],
  [/\u0432\u0435\u0440\u0445\u043d\u0438\u0439_\u0440\u0435\u0433\u0438\u0441\u0442\u0440\s*\(/gi, 'fn._upper('],
  [/\u043d\u0438\u0436\u043d\u0438\u0439_\u0440\u0435\u0433\u0438\u0441\u0442\u0440\s*\(/gi, 'fn._lower('],
  [/\u0437\u0430\u043c\u0435\u043d\u0438\u0442\u044c\s*\(/gi, 'fn._replace('],
  [/\u043c\u0438\u043d\u0438\u043c\u0443\u043c\s*\(/gi, 'fn._min('],
  [/\u043c\u0430\u043a\u0441\u0438\u043c\u0443\u043c\s*\(/gi, 'fn._max('],
  [/\u043e\u043a\u0440\u0443\u0433\u043b\u0438\u0442\u044c\s*\(/gi, 'fn._round('],
  [/\u0441\u043a\u043b\u0435\u0438\u0442\u044c\s*\(/gi, 'fn._concat('],
  [/\u0441\u043e\u0435\u0434\u0438\u043d\u0438\u0442\u044c_\u0441\u0442\u0440\u043e\u043a\u0438\s*\(/gi, 'fn._concat('],
  [/\u0441\u043e\u0434\u0435\u0440\u0436\u0438\u0442\s*\(/gi, 'fn._contains('],
  [/\u043d\u0435_\u0441\u043e\u0434\u0435\u0440\u0436\u0438\u0442\s*\(/gi, 'fn._notContains(']
];

function sanitizeComputedExpression(expression) {
  let jsExpr = String(expression || '').trim();
  if (!jsExpr) return '';
  jsExpr = jsExpr.replace(/\{([^{}]+)\}/g, (_m, fieldPath) => `field(${JSON.stringify(String(fieldPath || '').trim())})`);
  for (const [pattern, replacement] of COMPUTED_FN_REPLACEMENTS) jsExpr = jsExpr.replace(pattern, replacement);
  if (/[;\[\]`\\]/.test(jsExpr)) throw new Error('table_node_expression_unsafe_chars');
  if (/\b(?:globalThis|window|process|Function|constructor|require|import|eval|this|new)\b/.test(jsExpr)) {
    throw new Error('table_node_expression_unsafe_identifier');
  }
  if (!/^[\w\s"'.,()+\-*/%<>=!&|:?]+$/.test(jsExpr)) throw new Error('table_node_expression_invalid_chars');
  return jsExpr;
}

function evaluateComputedExpression(expression, row) {
  const jsExpr = sanitizeComputedExpression(expression);
  if (!jsExpr) return undefined;
  const field = (path) => {
    const key = String(path || '').trim();
    if (!key) return undefined;
    if (Object.prototype.hasOwnProperty.call(row, key)) return row[key];
    return getByPath(row, key);
  };
  const fn = buildExpressionFunctions();
  const evaluator = new Function('field', 'fn', `"use strict"; return (${jsExpr});`);
  return evaluator(field, fn);
}

function compareByOperator(left, operator, right) {
  const op = String(operator || '').trim().toLowerCase();
  if (!op || op === '=' || op === 'eq' || op === 'equals') return String(left ?? '') === String(right ?? '');
  if (op === '!=' || op === '<>' || op === 'neq' || op === 'not_equals') return String(left ?? '') !== String(right ?? '');
  if (op === '>' || op === 'gt') return Number(left) > Number(right);
  if (op === '>=' || op === 'gte') return Number(left) >= Number(right);
  if (op === '<' || op === 'lt') return Number(left) < Number(right);
  if (op === '<=' || op === 'lte') return Number(left) <= Number(right);
  if (op === 'contains') return String(left ?? '').includes(String(right ?? ''));
  if (op === 'not_contains') return !String(left ?? '').includes(String(right ?? ''));
  if (op === 'empty' || op === 'is_empty') return left === undefined || left === null || String(left) === '';
  if (op === 'not_empty' || op === 'exists') return !(left === undefined || left === null || String(left) === '');
  if (op === 'between') {
    const range = Array.isArray(right) ? right : String(right ?? '').split('|');
    const [from, to] = range;
    return Number(left) >= Number(from) && Number(left) <= Number(to);
  }
  if (op === 'in_list' || op === 'in') {
    const list = Array.isArray(right) ? right : String(right ?? '').split(',').map((item) => item.trim());
    return list.some((item) => String(item ?? '') === String(left ?? ''));
  }
  return String(left ?? '') === String(right ?? '');
}

function normalizeJoinMode(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'left' || raw === 'all_source' || raw === 'all_current') return 'left';
  if (raw === 'right' || raw === 'all_lookup') return 'right';
  if (raw === 'full' || raw === 'all_both') return 'full';
  return 'inner';
}

function normalizeConflictMode(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'replace') return 'replace';
  if (raw === 'skip') return 'skip';
  return 'suffix';
}

function normalizeOutputMode(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'aggregated_values' || raw === 'named_output_params') return raw;
  return 'rows';
}

function normalizeNodeIoEnvelopeLike(inputValue, inputEnvelope = {}) {
  if (inputEnvelope && typeof inputEnvelope === 'object' && String(inputEnvelope.contract_version || '').trim() === 'node_io_v1') {
    return {
      contract_version: 'node_io_v1',
      rows: Array.isArray(inputEnvelope.rows) ? inputEnvelope.rows : [],
      row_count: Number(inputEnvelope.row_count || (Array.isArray(inputEnvelope.rows) ? inputEnvelope.rows.length : 0) || 0),
      meta: inputEnvelope.meta && typeof inputEnvelope.meta === 'object' ? inputEnvelope.meta : {}
    };
  }
  if (inputValue && typeof inputValue === 'object' && String(inputValue.contract_version || '').trim() === 'node_io_v1') {
    return {
      contract_version: 'node_io_v1',
      rows: Array.isArray(inputValue.rows) ? inputValue.rows : [],
      row_count: Number(inputValue.row_count || (Array.isArray(inputValue.rows) ? inputValue.rows.length : 0) || 0),
      meta: inputValue.meta && typeof inputValue.meta === 'object' ? inputValue.meta : {}
    };
  }
  const rows = Array.isArray(inputValue) ? inputValue : inputValue === undefined || inputValue === null ? [] : [inputValue];
  return {
    contract_version: 'node_io_v1',
    rows,
    row_count: rows.length,
    meta: inputEnvelope && typeof inputEnvelope.meta === 'object' ? inputEnvelope.meta : {}
  };
}

function normalizeSelectedField(item, index, baseAlias) {
  const sourceAlias = String(item?.sourceAlias ?? item?.source_alias ?? '').trim() || baseAlias;
  const fieldName = String(item?.fieldName ?? item?.field_name ?? '').trim();
  const outputName =
    String(item?.outputName ?? item?.output_name ?? '').trim() || defaultOutputName(sourceAlias, fieldName || `field_${index + 1}`);
  return { sourceAlias, fieldName, outputName };
}

function normalizeComputedField(item) {
  return {
    name: String(item?.name || '').trim(),
    expression: String(item?.expression || '').trim(),
    type: String(item?.type || '').trim()
  };
}

function normalizeFilterRule(item) {
  return {
    field: String(item?.field || '').trim(),
    expression: String(item?.expression || '').trim(),
    operator: String(item?.operator || '').trim(),
    value: item?.value ?? '',
    secondValue: item?.secondValue ?? item?.second_value ?? '',
    caseSensitive: Boolean(item?.caseSensitive ?? item?.case_sensitive)
  };
}

function normalizeJoinedSource(item, index) {
  return {
    id: String(item?.id || `join_${index + 1}`).trim(),
    sourceType: String(item?.sourceType ?? item?.source_type ?? '').trim().toLowerCase() === 'input_param' ? 'input_param' : 'table',
    sourceSchema: String(item?.sourceSchema ?? item?.source_schema ?? '').trim(),
    sourceTable: String(item?.sourceTable ?? item?.source_table ?? '').trim(),
    parameterName: String(item?.parameterName ?? item?.parameter_name ?? '').trim(),
    alias: String(item?.alias || '').trim() || `src_${index + 1}`,
    joinMode: normalizeJoinMode(item?.joinMode ?? item?.join_mode),
    joinKeys: parseJsonArraySafe(item?.joinKeys ?? item?.join_keys, [])
      .map((rule) => ({
        leftField: String(rule?.leftField ?? rule?.left_field ?? '').trim(),
        rightField: String(rule?.rightField ?? rule?.right_field ?? '').trim()
      }))
      .filter((rule) => rule.leftField && rule.rightField),
    selectedFields: parseJsonArraySafe(item?.selectedFields ?? item?.selected_fields, [])
      .map((field) => String(field || '').trim())
      .filter(Boolean),
    prefix: String(item?.prefix || '').trim(),
    conflictMode: normalizeConflictMode(item?.conflictMode ?? item?.conflict_mode)
  };
}

function normalizeInputSource(item, index) {
  return {
    id: String(item?.id || `param_${index + 1}`).trim(),
    parameterName: String(item?.parameterName ?? item?.parameter_name ?? '').trim(),
    bindMode: String(item?.bindMode ?? item?.bind_mode ?? '').trim().toLowerCase() === 'rows' ? 'rows' : 'broadcast_fields',
    fieldMapping: parseJsonArraySafe(item?.fieldMapping ?? item?.field_mapping, [])
      .map((rule) => ({
        sourceField: String(rule?.sourceField ?? rule?.source_field ?? '').trim(),
        targetField: String(rule?.targetField ?? rule?.target_field ?? '').trim()
      }))
      .filter((rule) => rule.targetField)
  };
}

function normalizeOutputMapping(item, index) {
  return {
    outputParamName: String(item?.outputParamName ?? item?.output_param_name ?? '').trim() || `output_${index + 1}`,
    sourceField: String(item?.sourceField ?? item?.source_field ?? '').trim(),
    expression: String(item?.expression || '').trim(),
    mode: String(item?.mode || '').trim().toLowerCase() === 'array' ? 'array' : 'scalar'
  };
}

export function normalizeTableNodeSettings(raw = {}, overrides = {}) {
  const settings = raw && typeof raw === 'object' ? raw : {};
  const baseAlias = String(overrides.baseAlias ?? settings.baseAlias ?? settings.base_alias ?? 'base').trim() || 'base';
  return {
    baseSchema: String(overrides.baseSchema ?? settings.baseSchema ?? settings.base_schema ?? '').trim(),
    baseTable: String(overrides.baseTable ?? settings.baseTable ?? settings.base_table ?? '').trim(),
    baseAlias,
    joinedSources: parseJsonArraySafe(
      overrides.joinedSources ?? settings.joinedSourcesJson ?? settings.joinedSources ?? settings.joined_sources,
      []
    ).map(normalizeJoinedSource),
    inputSources: parseJsonArraySafe(
      overrides.inputSources ?? settings.inputSourcesJson ?? settings.inputSources ?? settings.input_sources,
      []
    ).map(normalizeInputSource),
    selectedFields: parseJsonArraySafe(
      overrides.selectedFields ?? settings.selectedFieldsJson ?? settings.selectedFields ?? settings.selected_fields,
      []
    ).map((item, index) => normalizeSelectedField(item, index, baseAlias)),
    computedFields: parseJsonArraySafe(
      overrides.computedFields ?? settings.computedFieldsJson ?? settings.computedFields ?? settings.computed_fields,
      []
    ).map(normalizeComputedField).filter((item) => item.name && item.expression),
    filterRules: parseJsonArraySafe(
      overrides.filterRules ?? settings.filterRulesJson ?? settings.filterRules ?? settings.filter_rules,
      []
    ).map(normalizeFilterRule).filter((item) => item.field || item.expression),
    filterLogic: String(overrides.filterLogic ?? settings.filterLogic ?? settings.filter_logic ?? 'and').trim().toLowerCase() === 'or' ? 'or' : 'and',
    outputMode: normalizeOutputMode(overrides.outputMode ?? settings.outputMode ?? settings.output_mode),
    outputParamsMapping: parseJsonArraySafe(
      overrides.outputParamsMapping ?? settings.outputParamsMappingJson ?? settings.outputParamsMapping ?? settings.output_params_mapping,
      []
    ).map(normalizeOutputMapping).filter((item) => item.outputParamName && (item.sourceField || item.expression)),
    previewInputParams: parseJsonObjectSafe(
      overrides.previewInputParams ?? settings.previewInputParamsJson ?? settings.previewInputParams ?? settings.preview_input_params,
      {}
    ),
    batchSize: Math.max(1, Math.min(5000, Math.trunc(Number(overrides.batchSize ?? settings.batchSize ?? settings.batch_size ?? DEFAULT_BATCH_SIZE)))),
    previewLimit: Math.max(1, Math.min(200, Math.trunc(Number(overrides.previewLimit ?? settings.previewLimit ?? settings.preview_limit ?? DEFAULT_PREVIEW_LIMIT)))),
    dedupeMode: String(settings.dedupeMode ?? settings.dedupe_mode ?? '').trim(),
    dedupeFields: String(settings.dedupeFields ?? settings.dedupe_fields ?? '').trim(),
    dedupeKeep: String(settings.dedupeKeep ?? settings.dedupe_keep ?? 'first').trim(),
    groupByFields: String(settings.groupByFields ?? settings.group_by_fields ?? '').trim(),
    aggregateRules: parseJsonArraySafe(
      overrides.aggregateRules ?? settings.aggregateRulesJson ?? settings.aggregateRules ?? settings.aggregate_rules,
      []
    )
  };
}

async function readTableBatch(client, schema, table, { limit = 100, offset = 0, columns = [] } = {}) {
  const safeLimit = Math.max(1, Math.min(5000, Math.trunc(Number(limit || 100))));
  const safeOffset = Math.max(0, Math.trunc(Number(offset || 0)));
  const selectedColumns = (Array.isArray(columns) ? columns : []).filter((col) => isIdent(col));
  const selectSql = selectedColumns.length ? selectedColumns.map((col) => qi(col)).join(', ') : '*';
  const sql = `SELECT ${selectSql} FROM ${qname(schema, table)} LIMIT ${safeLimit + 1} OFFSET ${safeOffset}`;
  const result = await client.query(sql);
  const rawRows = Array.isArray(result.rows) ? result.rows : [];
  return {
    rows: rawRows.slice(0, safeLimit).map((row) => normalizeObjectRow(row)),
    hasMore: rawRows.length > safeLimit
  };
}

function normalizeParameterRows(value) {
  if (Array.isArray(value)) return value.map((item) => normalizeObjectRow(item));
  if (value && typeof value === 'object') return [normalizeObjectRow(value)];
  return [];
}

function flattenRuntimeParams(inputValue, inputEnvelope, previewParams = {}) {
  const envelope = normalizeNodeIoEnvelopeLike(inputValue, inputEnvelope);
  const metaParams = envelope.meta && typeof envelope.meta.output_params === 'object' ? envelope.meta.output_params : {};
  const baseObject =
    inputValue && typeof inputValue === 'object' && !Array.isArray(inputValue) && String(inputValue.contract_version || '').trim() !== 'node_io_v1'
      ? inputValue
      : {};
  return {
    ...baseObject,
    ...metaParams,
    ...parseJsonObjectSafe(previewParams, {})
  };
}

function attachBaseRows(rows, alias) {
  return rows.map((row) => ({ ...row, __sources: { [alias]: deepClone(row) } }));
}

function applyInputSources(rows, cfg, runtimeParams, diagnostics) {
  if (!cfg.inputSources.length) return rows;
  return rows.map((row) => {
    const next = { ...row, __sources: { ...(row.__sources || {}) } };
    for (const source of cfg.inputSources) {
      const value = runtimeParams?.[source.parameterName];
      if (source.bindMode === 'rows') {
        diagnostics.warnings.push(`Параметр "${source.parameterName}" подключён как набор строк через блок дополнительных источников.`);
        continue;
      }
      const obj =
        value && typeof value === 'object' && !Array.isArray(value)
          ? value
          : value === undefined || value === null
          ? {}
          : { value };
      for (const mapping of source.fieldMapping) {
        const sourceField = String(mapping.sourceField || '').trim();
        next[mapping.targetField] = sourceField ? getByPath(obj, sourceField) : value;
      }
      next.__sources[source.parameterName || source.id] = deepClone(obj);
    }
    return next;
  });
}

function buildJoinKey(row, rules, side = 'left') {
  return JSON.stringify(
    rules.map((rule) => {
      const path = side === 'left' ? rule.leftField : rule.rightField;
      return String(getByPath(row, path) ?? '').trim();
    })
  );
}

function resolveCollisionKey(baseKey, targetRow, mode) {
  const initial = String(baseKey || '').trim();
  if (!initial) return '';
  if (!Object.prototype.hasOwnProperty.call(targetRow, initial)) return initial;
  if (mode === 'replace') return initial;
  if (mode === 'skip') return '';
  let idx = 2;
  let candidate = `${initial}_${idx}`;
  while (Object.prototype.hasOwnProperty.call(targetRow, candidate)) {
    idx += 1;
    candidate = `${initial}_${idx}`;
  }
  return candidate;
}

function mergeJoinedFields(targetRow, joinedRow, source) {
  const next = { ...targetRow, __sources: { ...(targetRow.__sources || {}) } };
  next.__sources[source.alias] = deepClone(joinedRow);
  let added = 0;
  for (const field of source.selectedFields) {
    const baseKey = source.prefix ? `${source.prefix}${field}` : defaultOutputName(source.alias, field);
    const finalKey = resolveCollisionKey(baseKey, next, source.conflictMode);
    if (!finalKey) continue;
    next[finalKey] = joinedRow?.[field];
    added += 1;
  }
  return { row: next, added };
}

async function loadJoinedSourceRows(client, source, runtimeParams) {
  if (source.sourceType === 'input_param') {
    return normalizeParameterRows(runtimeParams?.[source.parameterName]);
  }
  if (!isIdent(source.sourceSchema) || !isIdent(source.sourceTable)) return [];
  const neededColumns = [...new Set([...source.selectedFields, ...source.joinKeys.map((rule) => rule.rightField)].filter(isIdent))];
  const batch = await readTableBatch(client, source.sourceSchema, source.sourceTable, { limit: 500, offset: 0, columns: neededColumns });
  return batch.rows;
}

async function applyJoinedSources(client, cfg, rows, runtimeParams, diagnostics) {
  let currentRows = rows;
  const joinSummaries = [];
  for (const source of cfg.joinedSources) {
    if (!source.joinKeys.length) {
      diagnostics.warnings.push(`Источник "${source.alias}" пропущен: не настроены поля связи.`);
      continue;
    }
    const lookupRows = await loadJoinedSourceRows(client, source, runtimeParams);
    const joinMap = new Map();
    for (const item of lookupRows) {
      const key = buildJoinKey(item, source.joinKeys, 'right');
      const bucket = joinMap.get(key) || [];
      bucket.push(item);
      joinMap.set(key, bucket);
    }

    const nextRows = [];
    const usedLookup = new Set();
    let matchedRows = 0;
    let unmatchedRows = 0;
    let addedFieldsCount = 0;

    for (const row of currentRows) {
      const key = buildJoinKey(row, source.joinKeys, 'left');
      const hits = joinMap.get(key) || [];
      if (!hits.length) {
        unmatchedRows += 1;
        if (source.joinMode === 'left' || source.joinMode === 'full') nextRows.push(row);
        continue;
      }
      matchedRows += 1;
      const selectedHits = source.joinMode === 'left' ? [hits[0]] : hits;
      for (const hit of selectedHits) {
        usedLookup.add(hit);
        const merged = mergeJoinedFields(row, hit, source);
        addedFieldsCount = Math.max(addedFieldsCount, merged.added);
        nextRows.push(merged.row);
      }
    }

    if (source.joinMode === 'right' || source.joinMode === 'full') {
      for (const hit of lookupRows) {
        if (usedLookup.has(hit)) continue;
        const merged = mergeJoinedFields({ __sources: {} }, hit, source);
        nextRows.push(merged.row);
      }
    }

    currentRows = nextRows;
    joinSummaries.push({
      alias: source.alias,
      source_type: source.sourceType,
      join_mode: source.joinMode,
      matched_rows: matchedRows,
      unmatched_rows: unmatchedRows,
      added_fields_count: addedFieldsCount
    });
  }
  return { rows: currentRows, summaries: joinSummaries };
}

function projectRows(rows, cfg) {
  if (!cfg.selectedFields.length) {
    return rows.map((row) => {
      const next = { ...row };
      delete next.__sources;
      return next;
    });
  }
  return rows.map((row) => {
    const next = {};
    for (const field of cfg.selectedFields) {
      const sourceRow = row?.__sources?.[field.sourceAlias];
      let value =
        sourceRow && field.fieldName
          ? getByPath(sourceRow, field.fieldName)
          : field.fieldName
          ? row?.[field.fieldName]
          : undefined;
      if (value === undefined && field.fieldName) value = row?.[field.fieldName];
      next[field.outputName] = value;
    }
    return next;
  });
}

function parameterFieldMap(runtimeParams = {}) {
  const out = {};
  for (const [key, value] of Object.entries(runtimeParams || {})) {
    const rows =
      Array.isArray(value)
        ? value
        : value && typeof value === 'object'
        ? [value]
        : value === undefined || value === null
        ? []
        : [{ value }];
    out[key] = [
      ...new Set(
        rows.flatMap((row) => (row && typeof row === 'object' && !Array.isArray(row) ? Object.keys(row) : ['value']))
      )
    ];
  }
  return out;
}

function joinedOutputFieldName(source, fieldName) {
  return source.prefix ? `${source.prefix}${fieldName}` : defaultOutputName(source.alias, fieldName);
}

function availableWorkingFieldsBeforeJoin(cfg, baseColumns = [], sourceColumns = new Map(), runtimeParamFields = {}, sourceIndex = 0) {
  const out = new Set((Array.isArray(baseColumns) ? baseColumns : []).filter(isIdent));
  for (const source of cfg.inputSources || []) {
    for (const mapping of source.fieldMapping || []) {
      const targetField = String(mapping?.targetField || '').trim();
      if (isIdent(targetField)) out.add(targetField);
    }
  }
  for (const source of (cfg.joinedSources || []).slice(0, Math.max(0, Math.trunc(Number(sourceIndex || 0))))) {
    for (const fieldName of source.selectedFields || []) {
      const outputName = joinedOutputFieldName(source, fieldName);
      if (isIdent(outputName)) out.add(outputName);
    }
  }
  return out;
}

function buildSampleValidationRow(availableFields = []) {
  const row = {};
  for (const fieldName of availableFields) row[fieldName] = 1;
  return row;
}

function validateTableNodeConfig(cfg, context = {}) {
  const baseColumns = Array.isArray(context.baseColumns) ? context.baseColumns : [];
  const runtimeParamFields = context.runtimeParamFields && typeof context.runtimeParamFields === 'object' ? context.runtimeParamFields : {};
  const sourceColumns = context.sourceColumns instanceof Map ? context.sourceColumns : new Map();

  for (let sourceIndex = 0; sourceIndex < cfg.joinedSources.length; sourceIndex += 1) {
    const source = cfg.joinedSources[sourceIndex];
    if (source.sourceType === 'table') {
      if (!isIdent(source.sourceSchema) || !isIdent(source.sourceTable)) {
        throw new Error(`Источник "${source.alias}": сначала выбери схему и таблицу.`);
      }
    } else if (!source.parameterName) {
      throw new Error(`Источник "${source.alias}": сначала выбери входной параметр workflow.`);
    }
    if (!source.joinKeys.length) {
      throw new Error(`Источник "${source.alias}": добавь хотя бы одно условие связи.`);
    }
    const leftFields = availableWorkingFieldsBeforeJoin(cfg, baseColumns, sourceColumns, runtimeParamFields, sourceIndex);
    const rightFields =
      source.sourceType === 'table'
        ? new Set(sourceColumns.get(`${source.sourceSchema}.${source.sourceTable}`) || [])
        : new Set(runtimeParamFields[source.parameterName] || []);
    for (const rule of source.joinKeys) {
      if (!leftFields.has(rule.leftField)) {
        throw new Error(`Источник "${source.alias}": поле связи "${rule.leftField}" не найдено в текущем наборе данных.`);
      }
      if (rightFields.size && !rightFields.has(rule.rightField)) {
        throw new Error(`Источник "${source.alias}": поле "${rule.rightField}" не найдено в подключаемом источнике.`);
      }
    }
  }

  const sampleRow = buildSampleValidationRow([
    ...baseColumns,
    ...cfg.inputSources.flatMap((source) => (source.fieldMapping || []).map((rule) => String(rule?.targetField || '').trim())),
    ...cfg.joinedSources.flatMap((source) => (source.selectedFields || []).map((fieldName) => joinedOutputFieldName(source, fieldName))),
    ...cfg.selectedFields.map((field) => String(field?.outputName || '').trim())
  ].filter(isIdent));

  for (const field of cfg.computedFields) {
    try {
      evaluateComputedExpression(field.expression, sampleRow);
    } catch (e) {
      throw new Error(`Вычисляемое поле "${field.name}": ${String(e?.message || e)}`);
    }
  }
  for (let ruleIndex = 0; ruleIndex < cfg.filterRules.length; ruleIndex += 1) {
    const rule = cfg.filterRules[ruleIndex];
    if (rule.expression) {
      try {
        evaluateComputedExpression(rule.expression, sampleRow);
      } catch (e) {
        throw new Error(`Фильтр ${ruleIndex + 1}: ${String(e?.message || e)}`);
      }
    }
  }
  for (let mappingIndex = 0; mappingIndex < cfg.outputParamsMapping.length; mappingIndex += 1) {
    const mapping = cfg.outputParamsMapping[mappingIndex];
    if (mapping.expression) {
      try {
        evaluateComputedExpression(mapping.expression, sampleRow);
      } catch (e) {
        throw new Error(`Исходящий параметр "${mapping.outputParamName}": ${String(e?.message || e)}`);
      }
    }
  }
}

function applyComputedFields(rows, cfg, diagnostics) {
  if (!cfg.computedFields.length) return rows;
  return rows.map((row) => {
    const next = { ...row };
    for (const field of cfg.computedFields) {
      try {
        const value = evaluateComputedExpression(field.expression, next);
        next[field.name] = field.type ? convertFieldType(value, field.type) : value;
      } catch (e) {
        diagnostics.warnings.push(`Не удалось вычислить поле "${field.name}": ${String(e?.message || e)}`);
        next[field.name] = null;
      }
    }
    return next;
  });
}

function evaluateFilterValue(rule, row) {
  if (rule.expression) return evaluateComputedExpression(rule.expression, row);
  return getByPath(row, rule.field);
}

function applyFilters(rows, cfg) {
  if (!cfg.filterRules.length) return rows;
  return rows.filter((row) => {
    const results = cfg.filterRules.map((rule) => {
      const left = evaluateFilterValue(rule, row);
      const right =
        String(rule.operator || '').trim().toLowerCase() === 'between'
          ? [rule.value, rule.secondValue]
          : String(rule.operator || '').trim().toLowerCase() === 'in_list'
          ? parseCsvList(rule.value)
          : rule.value;
      const normalizedLeft = rule.caseSensitive || typeof left !== 'string' ? left : String(left ?? '').toLowerCase();
      const normalizedRight =
        rule.caseSensitive || typeof right !== 'string' || Array.isArray(right) ? right : String(right ?? '').toLowerCase();
      return compareByOperator(normalizedLeft, rule.operator, normalizedRight);
    });
    return cfg.filterLogic === 'or' ? results.some(Boolean) : results.every(Boolean);
  });
}

function applyDedupe(rows, cfg) {
  const dedupeMode = String(cfg.dedupeMode || '').trim().toLowerCase();
  const dedupeFields = parseCsvList(cfg.dedupeFields || '');
  if (dedupeMode !== 'full_row' && dedupeMode !== 'by_fields') return rows;
  const keepLast = String(cfg.dedupeKeep || '').trim().toLowerCase() === 'last';
  const source = keepLast ? [...rows].reverse() : rows;
  const seen = new Set();
  const out = [];
  for (const row of source) {
    const key =
      dedupeMode === 'by_fields' && dedupeFields.length
        ? JSON.stringify(dedupeFields.map((field) => row?.[field]))
        : JSON.stringify(row);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(row);
  }
  return keepLast ? out.reverse() : out;
}

function aggregateValue(rows, rule) {
  const op = String(rule?.op || '').trim().toLowerCase();
  const field = String(rule?.field || '').trim();
  const values = rows.map((row) => row?.[field]).filter((value) => value !== undefined && value !== null && value !== '');
  if (op === 'count') return rows.length;
  if (op === 'sum') return values.reduce((sum, value) => sum + Number(value || 0), 0);
  if (op === 'min') return values.length ? values.reduce((acc, value) => (acc < value ? acc : value)) : null;
  if (op === 'max') return values.length ? values.reduce((acc, value) => (acc > value ? acc : value)) : null;
  if (op === 'avg' || op === 'average') return values.length ? values.reduce((sum, value) => sum + Number(value || 0), 0) / values.length : null;
  return null;
}

function applyGrouping(rows, cfg) {
  const groupByFields = parseCsvList(cfg.groupByFields || '');
  const aggregateRules = parseJsonArraySafe(cfg.aggregateRules, [])
    .map((rule) => ({
      field: String(rule?.field || '').trim(),
      op: String(rule?.op || rule?.operator || '').trim().toLowerCase(),
      as: String(rule?.as || rule?.alias || '').trim()
    }))
    .filter((rule) => rule.op && (rule.field || rule.op === 'count'));
  if (!groupByFields.length || !aggregateRules.length) return rows;

  const groups = new Map();
  for (const row of rows) {
    const key = JSON.stringify(groupByFields.map((field) => row?.[field]));
    const bucket = groups.get(key) || [];
    bucket.push(row);
    groups.set(key, bucket);
  }

  const out = [];
  for (const bucket of groups.values()) {
    const first = bucket[0] || {};
    const next = {};
    for (const field of groupByFields) next[field] = first?.[field];
    for (const rule of aggregateRules) next[String(rule.as || `${rule.op}_${rule.field || 'rows'}`).trim()] = aggregateValue(bucket, rule);
    out.push(next);
  }
  return out;
}

function buildOutputParams(rows, cfg) {
  const out = {};
  for (const mapping of cfg.outputParamsMapping) {
    const getter = (row) => {
      if (mapping.expression) return evaluateComputedExpression(mapping.expression, row);
      return getByPath(row, mapping.sourceField);
    };
    out[mapping.outputParamName] = mapping.mode === 'array' ? rows.map(getter) : rows.length ? getter(rows[0]) : null;
  }
  return out;
}

function previewSummary(result) {
  return {
    source_type: 'table_node',
    source_ref: result.source_ref,
    base_schema: result.base_schema,
    base_table: result.base_table,
    input_row_count: result.input_row_count,
    input_column_count: result.input_column_count,
    input_columns: result.input_columns,
    input_sample_rows: result.input_sample_rows,
    row_count: result.rows.length,
    column_count: result.columns.length,
    columns: result.columns,
    sample_rows: result.rows.slice(0, result.preview_limit),
    stats: result.stats,
    warnings: result.warnings,
    join_summaries: result.join_summaries,
    batch: result.batch,
    output_params: result.output_params
  };
}

async function runTableNodePipeline(client, rawSettings = {}, options = {}) {
  const cfg = normalizeTableNodeSettings(rawSettings, options?.overrides || {});
  if (!isIdent(cfg.baseSchema) || !isIdent(cfg.baseTable)) throw new Error('Сначала выбери основную таблицу.');

  const cursor = options?.cursor && typeof options.cursor === 'object' ? options.cursor : {};
  const offset = Math.max(0, Math.trunc(Number(cursor.offset || 0)));
  const limit = options?.preview ? cfg.previewLimit : cfg.batchSize;
  const runtimeParams = flattenRuntimeParams(options?.inputValue, options?.inputEnvelope, cfg.previewInputParams);
  const diagnostics = { warnings: [], applied_steps: [] };

  const baseColumns = await tableColumnsDetailed(client, cfg.baseSchema, cfg.baseTable);
  const runtimeParamFields = parameterFieldMap(runtimeParams);
  const sourceColumns = new Map();
  for (const source of cfg.joinedSources) {
    if (source.sourceType !== 'table' || !isIdent(source.sourceSchema) || !isIdent(source.sourceTable)) continue;
    const key = `${source.sourceSchema}.${source.sourceTable}`;
    if (sourceColumns.has(key)) continue;
    sourceColumns.set(
      key,
      (await tableColumnsDetailed(client, source.sourceSchema, source.sourceTable)).map((column) => String(column?.name || '').trim()).filter(isIdent)
    );
  }
  validateTableNodeConfig(cfg, {
    baseColumns: baseColumns.map((column) => String(column?.name || '').trim()).filter(isIdent),
    runtimeParamFields,
    sourceColumns
  });

  const baseBatch = await readTableBatch(client, cfg.baseSchema, cfg.baseTable, { limit, offset });
  let rows = attachBaseRows(baseBatch.rows, cfg.baseAlias);
  diagnostics.applied_steps.push(`Основная таблица: ${cfg.baseSchema}.${cfg.baseTable}`);

  rows = applyInputSources(rows, cfg, runtimeParams, diagnostics);
  if (cfg.inputSources.length) diagnostics.applied_steps.push(`Параметры workflow: ${cfg.inputSources.length}`);

  const joined = await applyJoinedSources(client, cfg, rows, runtimeParams, diagnostics);
  rows = joined.rows;
  const inputPreviewRows = rows.map((row) => {
    const next = { ...row };
    delete next.__sources;
    return next;
  });
  if (joined.summaries.length) diagnostics.applied_steps.push(`Соединения: ${joined.summaries.length}`);

  rows = projectRows(rows, cfg);
  diagnostics.applied_steps.push(cfg.selectedFields.length ? `Поля результата: ${cfg.selectedFields.length}` : 'Поля результата: все');

  rows = applyComputedFields(rows, cfg, diagnostics);
  if (cfg.computedFields.length) diagnostics.applied_steps.push(`Вычисляемые поля: ${cfg.computedFields.length}`);

  rows = applyFilters(rows, cfg);
  if (cfg.filterRules.length) diagnostics.applied_steps.push(`Фильтры: ${cfg.filterRules.length}`);

  rows = applyDedupe(rows, cfg);
  rows = applyGrouping(rows, cfg);

  const outputParams = buildOutputParams(rows, cfg);

  return {
    rows,
    columns: columnsFromRows(rows),
    base_schema: cfg.baseSchema,
    base_table: cfg.baseTable,
    source_ref: `${cfg.baseSchema}.${cfg.baseTable}`,
    preview_limit: cfg.previewLimit,
    input_row_count: inputPreviewRows.length,
    input_column_count: columnsFromRows(inputPreviewRows).length,
    input_columns: columnsFromRows(inputPreviewRows),
    input_sample_rows: inputPreviewRows.slice(0, cfg.previewLimit),
    batch: {
      offset,
      batch_size: limit,
      returned_rows: rows.length,
      has_more: baseBatch.hasMore,
      next_cursor: baseBatch.hasMore ? { offset: offset + limit } : null
    },
    stats: {
      base_rows: baseBatch.rows.length,
      result_rows: rows.length,
      base_columns: baseColumns.length,
      selected_fields: cfg.selectedFields.length,
      computed_fields: cfg.computedFields.length,
      filters: cfg.filterRules.length,
      joins: cfg.joinedSources.length,
      input_sources: cfg.inputSources.length,
      output_params: Object.keys(outputParams).length,
      applied_steps: diagnostics.applied_steps
    },
    warnings: diagnostics.warnings,
    join_summaries: joined.summaries,
    output_params: outputParams
  };
}

export async function previewTableNodeConfig(client, rawSettings = {}, options = {}) {
  const result = await runTableNodePipeline(client, rawSettings, { ...options, preview: true });
  return previewSummary(result);
}

export async function executeTableNodeConfig(client, rawSettings = {}, options = {}) {
  const result = await runTableNodePipeline(client, rawSettings, options);
  return {
    rows: result.rows,
    columns: result.columns,
    batch: result.batch,
    stats: result.stats,
    warnings: result.warnings,
    joinSummaries: result.join_summaries,
    outputParams: result.output_params,
    meta: {
      source_type: 'table_node',
      source_ref: result.source_ref,
      table_batch: result.batch,
      output_params: result.output_params
    }
  };
}

export const tableNodeRuntimeTestkit = {
  normalizeTableNodeSettings,
  previewSummary,
  compareByOperator,
  evaluateComputedExpression,
  parseJsonArraySafe,
  parseJsonObjectSafe,
  parseCsvList,
  buildOutputParams
};

