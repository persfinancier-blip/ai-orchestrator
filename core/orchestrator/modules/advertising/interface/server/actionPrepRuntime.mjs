import { tableNodeRuntimeTestkit } from './tableNodeRuntime.mjs';

const { compareByOperator, evaluateComputedExpression } = tableNodeRuntimeTestkit;

const DEFAULT_BATCH_SIZE = 500;
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

function parseJsonArraySafe(raw, fallback = []) {
  const parsed = parseJsonSafe(raw, fallback);
  return Array.isArray(parsed) ? parsed : fallback;
}

function parseCsvList(raw) {
  if (Array.isArray(raw)) return raw.map((item) => str(item)).filter(Boolean);
  return String(raw || '')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
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

function normalizeObjectRow(value) {
  return value && typeof value === 'object' && !Array.isArray(value) ? value : { value };
}

function rowsFromNodeIo(value) {
  if (value && typeof value === 'object' && String(value.contract_version || '').trim() === 'node_io_v1' && Array.isArray(value.rows)) {
    return value.rows.filter((row) => row && typeof row === 'object' && !Array.isArray(row));
  }
  if (Array.isArray(value)) {
    return value.filter((row) => row && typeof row === 'object' && !Array.isArray(row)).map((row) => normalizeObjectRow(row));
  }
  if (value && typeof value === 'object') return [normalizeObjectRow(value)];
  return [];
}

function normalizeActionValueType(value, typeName) {
  const type = str(typeName).toLowerCase();
  if (!type) return value;
  if (value === undefined || value === null || value === '') return value;
  if (type === 'integer' || type === 'int' || type === 'bigint') {
    const n = Number(value);
    return Number.isFinite(n) ? Math.trunc(n) : value;
  }
  if (type === 'numeric' || type === 'float' || type === 'number') {
    const n = Number(value);
    return Number.isFinite(n) ? n : value;
  }
  if (type === 'boolean' || type === 'bool') {
    if (typeof value === 'boolean') return value;
    const raw = str(value).toLowerCase();
    if (!raw) return false;
    return ['1', 'true', 'yes', 'on', 'enabled'].includes(raw);
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
  return value;
}

function templateString(text, row) {
  return String(text ?? '').replace(/\{([^{}]+)\}/g, (_match, key) => {
    const value = row?.[String(key || '').trim()];
    return value === undefined || value === null ? '' : String(value);
  });
}

function resolveScalarValue(rawValue, row) {
  if (rawValue === undefined || rawValue === null) return rawValue;
  if (typeof rawValue === 'number' || typeof rawValue === 'boolean') return rawValue;
  const text = String(rawValue);
  if (text.includes('{')) return templateString(text, row);
  return rawValue;
}

function normalizeFilterRule(item = {}, index = 0) {
  return {
    id: str(item.id || `filter_${index + 1}`),
    field: str(item.field),
    operator: str(item.operator || '='),
    value: item.value ?? '',
    secondValue: item.secondValue ?? item.second_value ?? '',
    expression: str(item.expression),
    caseSensitive: Boolean(item.caseSensitive ?? item.case_sensitive)
  };
}

function normalizeActionColumn(item = {}, index = 0) {
  const modeRaw = str(item.mode || item.sourceMode || item.actionType).toLowerCase();
  const mode = [
    'source_field',
    'constant',
    'formula',
    'string_template',
    'if_else',
    'percent_change',
    'absolute_change',
    'replace_text',
    'override_value'
  ].includes(modeRaw)
    ? modeRaw
    : 'source_field';
  return {
    id: str(item.id || `action_${index + 1}`),
    name: str(item.name || item.outputName || item.output_name || `action_${index + 1}`),
    mode,
    sourceField: str(item.sourceField || item.source_field),
    baseField: str(item.baseField || item.base_field),
    constantValue: item.constantValue ?? item.constant_value ?? '',
    expression: str(item.expression),
    template: String(item.template ?? ''),
    conditionExpression: str(item.conditionExpression || item.condition_expression),
    trueValue: item.trueValue ?? item.true_value ?? '',
    falseValue: item.falseValue ?? item.false_value ?? '',
    percentValue: item.percentValue ?? item.percent_value ?? '',
    deltaValue: item.deltaValue ?? item.delta_value ?? '',
    replaceFrom: String(item.replaceFrom ?? item.replace_from ?? ''),
    replaceTo: String(item.replaceTo ?? item.replace_to ?? ''),
    overrideValue: item.overrideValue ?? item.override_value ?? '',
    type: str(item.type)
  };
}

export function normalizeActionPrepSettings(raw = {}) {
  const src = raw && typeof raw === 'object' ? raw : {};
  return {
    sourceMode: str(src.sourceMode || src.source_mode || 'node').toLowerCase() === 'table' ? 'table' : 'node',
    sourceSchema: str(src.sourceSchema || src.source_schema),
    sourceTable: str(src.sourceTable || src.source_table),
    filterRules: parseJsonArraySafe(src.filterRules ?? src.filterRulesJson ?? src.filter_rules, []).map(normalizeFilterRule),
    filterLogic: str(src.filterLogic || src.filter_logic || 'and').toLowerCase() === 'or' ? 'or' : 'and',
    actionColumns: parseJsonArraySafe(src.actionColumns ?? src.actionColumnsJson ?? src.action_columns, []).map(normalizeActionColumn),
    batchSize: Math.max(1, Math.min(5000, Math.trunc(Number(src.batchSize || src.batch_size || DEFAULT_BATCH_SIZE)))),
    previewLimit: Math.max(1, Math.min(200, Math.trunc(Number(src.previewLimit || src.preview_limit || DEFAULT_PREVIEW_LIMIT)))),
    channel: str(src.channel)
  };
}

async function readTableRows(client, schema, table, limit) {
  if (!isIdent(schema) || !isIdent(table)) throw new Error('Сначала выбери схему и таблицу источника.');
  const safeLimit = Math.max(1, Math.min(5000, Math.trunc(Number(limit || DEFAULT_BATCH_SIZE))));
  const sql = `SELECT * FROM ${qname(schema, table)} LIMIT ${safeLimit}`;
  const result = await client.query(sql);
  return (Array.isArray(result.rows) ? result.rows : []).map((row) => normalizeObjectRow(row));
}

async function resolveSourceRows(client, cfg, options = {}) {
  if (cfg.sourceMode === 'table') {
    const rows = await readTableRows(client, cfg.sourceSchema, cfg.sourceTable, options?.preview ? cfg.previewLimit : cfg.batchSize);
    return {
      rows,
      source_type: 'table',
      source_ref: `${cfg.sourceSchema}.${cfg.sourceTable}`
    };
  }
  return {
    rows: rowsFromNodeIo(options?.inputValue),
    source_type: 'node',
    source_ref: 'upstream'
  };
}

function applyFilters(rows, cfg) {
  if (!cfg.filterRules.length) return rows;
  return rows.filter((row) => {
    const results = cfg.filterRules.map((rule) => {
      const left = rule.expression ? evaluateComputedExpression(rule.expression, row) : row?.[rule.field];
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

function evaluateActionColumn(rule, row) {
  if (rule.mode === 'source_field') return row?.[rule.sourceField];
  if (rule.mode === 'constant') return resolveScalarValue(rule.constantValue, row);
  if (rule.mode === 'formula') return evaluateComputedExpression(rule.expression, row);
  if (rule.mode === 'string_template') return templateString(rule.template, row);
  if (rule.mode === 'if_else') {
    const conditionMet = Boolean(evaluateComputedExpression(rule.conditionExpression, row));
    return resolveScalarValue(conditionMet ? rule.trueValue : rule.falseValue, row);
  }
  if (rule.mode === 'percent_change') {
    const base = Number(row?.[rule.baseField]);
    const delta = Number(rule.percentValue || 0);
    if (!Number.isFinite(base)) return null;
    return base * (1 + delta / 100);
  }
  if (rule.mode === 'absolute_change') {
    const base = Number(row?.[rule.baseField]);
    const delta = Number(rule.deltaValue || 0);
    if (!Number.isFinite(base)) return null;
    return base + delta;
  }
  if (rule.mode === 'replace_text') {
    return String(row?.[rule.baseField] ?? '').split(String(rule.replaceFrom ?? '')).join(String(rule.replaceTo ?? ''));
  }
  if (rule.mode === 'override_value') return resolveScalarValue(rule.overrideValue, row);
  return null;
}

function applyActionColumns(rows, cfg, warnings) {
  if (!cfg.actionColumns.length) return rows;
  return rows.map((row) => {
    const next = { ...row };
    for (const rule of cfg.actionColumns) {
      try {
        const value = evaluateActionColumn(rule, next);
        next[rule.name] = normalizeActionValueType(value, rule.type);
      } catch (error) {
        warnings.push(`Не удалось вычислить action-колонку "${rule.name}": ${String(error?.message || error)}`);
        next[rule.name] = null;
      }
    }
    return next;
  });
}

async function runActionPrepPipeline(client, rawSettings = {}, options = {}) {
  const cfg = normalizeActionPrepSettings(rawSettings);
  const warnings = [];
  const source = await resolveSourceRows(client, cfg, options);
  const inputRows = Array.isArray(source.rows) ? source.rows : [];
  const filteredRows = applyFilters(inputRows, cfg);
  const resultRows = applyActionColumns(filteredRows, cfg, warnings);
  return {
    rows: resultRows,
    columns: columnsFromRows(resultRows),
    input_rows: inputRows,
    input_columns: columnsFromRows(inputRows),
    warnings,
    stats: {
      source_rows: inputRows.length,
      filtered_rows: filteredRows.length,
      result_rows: resultRows.length,
      action_columns: cfg.actionColumns.length,
      filters: cfg.filterRules.length
    },
    batch: {
      batch_size: options?.preview ? cfg.previewLimit : cfg.batchSize,
      returned_rows: resultRows.length,
      has_more: false,
      next_cursor: null
    },
    source_type: source.source_type,
    source_ref: source.source_ref
  };
}

export async function previewActionPrepConfig(client, rawSettings = {}, options = {}) {
  const cfg = normalizeActionPrepSettings(rawSettings);
  const result = await runActionPrepPipeline(client, rawSettings, { ...options, preview: true });
  return {
    source_type: 'action_prep',
    source_ref: result.source_ref,
    input_row_count: result.input_rows.length,
    input_column_count: result.input_columns.length,
    input_columns: result.input_columns,
    input_sample_rows: result.input_rows.slice(0, cfg.previewLimit),
    row_count: result.rows.length,
    column_count: result.columns.length,
    columns: result.columns,
    sample_rows: result.rows.slice(0, cfg.previewLimit),
    stats: result.stats,
    warnings: result.warnings,
    batch: result.batch
  };
}

export async function executeActionPrepConfig(client, rawSettings = {}, options = {}) {
  const result = await runActionPrepPipeline(client, rawSettings, options);
  return {
    rows: result.rows,
    columns: result.columns,
    batch: result.batch,
    stats: result.stats,
    warnings: result.warnings,
    meta: {
      source_type: 'action_prep',
      source_ref: result.source_ref
    }
  };
}
