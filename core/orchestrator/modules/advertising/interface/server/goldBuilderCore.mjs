import vm from 'node:vm';

import { discoverProcessesFromGraph } from './workflowAutomation.mjs';
import {
  analyzeGoldImpact,
  buildGoldDependencyGraph,
  checkSourceCompatibility,
  collectGoldOutputFields,
  getActiveDataMart,
  getActiveScenario,
  normalizeGoldDefinition,
  resolveConsumerDependencies,
  resolveExternalSourceFreshness,
  resolveGoldStatus,
  validateGoldDefinition
} from '../shared/goldDefinitionCore.mjs';

function asText(value) {
  return String(value || '').trim();
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function asObject(value) {
  return value && typeof value === 'object' && !Array.isArray(value) ? value : {};
}

function buildTableKey(schema, table) {
  return `${asText(schema).toLowerCase()}.${asText(table).toLowerCase()}`;
}

function parseJsonSafe(value, fallback) {
  if (value && typeof value === 'object') return value;
  const raw = asText(value);
  if (!raw) return fallback;
  try {
    const parsed = JSON.parse(raw);
    return parsed ?? fallback;
  } catch {
    return fallback;
  }
}

function parseColumns(value) {
  return asArray(parseJsonSafe(value, [])).map((item) => ({
    name: asText(item?.field_name || item?.name),
    type: asText(item?.field_type || item?.type || 'text') || 'text',
    description: asText(item?.description)
  })).filter((item) => item.name);
}

function normalizeTemplateRow(row = {}) {
  return {
    template_name: asText(row.template_name),
    schema_name: asText(row.schema_name),
    table_name: asText(row.table_name),
    data_level: asText(row.data_level || 'bronze') || 'bronze',
    template_kind: asText(row.template_kind || 'data') || 'data',
    table_class: asText(row.table_class || 'custom') || 'custom'
  };
}

function parseContractRow(row = {}) {
  return {
    schema_name: asText(row.schema_name),
    table_name: asText(row.table_name),
    contract_name: asText(row.contract_name),
    version: Math.max(0, Number(row.version || 0)),
    columns: parseColumns(row.columns)
  };
}

function buildContractsMap(contractRows = []) {
  const map = new Map();
  asArray(contractRows).forEach((row) => {
    const parsed = parseContractRow(row);
    if (!parsed.schema_name || !parsed.table_name) return;
    map.set(buildTableKey(parsed.schema_name, parsed.table_name), parsed);
  });
  return map;
}

function buildTemplatesMap(templateRows = []) {
  const map = new Map();
  asArray(templateRows).forEach((row) => {
    const parsed = normalizeTemplateRow(row);
    if (!parsed.schema_name || !parsed.table_name) return;
    map.set(buildTableKey(parsed.schema_name, parsed.table_name), parsed);
  });
  return map;
}

function writeNodeTargetsFromProcess(process) {
  const nodes = asArray(process?.subgraph?.nodes);
  return nodes
    .filter((node) => node?.type === 'tool' && asText(node?.config?.toolType).toLowerCase() === 'db_write')
    .map((node) => {
      const settings = asObject(node?.config?.settings);
      return {
        node_id: asText(node?.id),
        node_name: asText(node?.config?.name || node?.id || 'Write'),
        schema_name: asText(settings.targetSchema || settings.target_schema),
        table_name: asText(settings.targetTable || settings.target_table)
      };
    })
    .filter((item) => item.schema_name && item.table_name);
}

export function buildProcessGoldSources({ publishedDesks = [], contractRows = [], templateRows = [] } = {}) {
  const contracts = buildContractsMap(contractRows);
  const templates = buildTemplatesMap(templateRows);
  const out = [];
  const seen = new Set();

  asArray(publishedDesks).forEach((desk) => {
    const graphJson = parseJsonSafe(desk?.graph_json, {});
    const processes = discoverProcessesFromGraph({
      deskId: Number(desk?.desk_id || 0),
      deskVersionId: Number(desk?.desk_version_id || 0),
      versionNo: Number(desk?.version_no || 0),
      graphJson,
      overrideRows: []
    });
    processes.forEach((process) => {
      writeNodeTargetsFromProcess(process).forEach((target) => {
        const tableKey = buildTableKey(target.schema_name, target.table_name);
        const template = templates.get(tableKey);
        if (template && template.template_kind !== 'data') return;
        const contract = contracts.get(tableKey);
        const sourceKey = `process:${Number(desk?.desk_id || 0)}:${asText(process.start_node_id)}:${target.schema_name}.${target.table_name}`;
        if (seen.has(sourceKey)) return;
        seen.add(sourceKey);
        out.push({
          source_key: sourceKey,
          source_kind: 'process',
          source_name: `${asText(process.name || process.process_code || 'Процесс')} -> ${target.schema_name}.${target.table_name}`,
          process_code: asText(process.process_code),
          desk_id: Number(desk?.desk_id || 0),
          desk_name: asText(desk?.desk_name),
          start_node_id: asText(process.start_node_id),
          schema_name: target.schema_name,
          table_name: target.table_name,
          data_level: asText(template?.data_level || 'silver') || 'silver',
          table_class: asText(template?.table_class || 'custom'),
          contract_name: asText(contract?.contract_name),
          contract_version: Number(contract?.version || 0),
          fields: asArray(contract?.columns)
        });
      });
    });
  });

  return out;
}

export function buildNamedSourceCatalog({ registryRows = [], contractRows = [] } = {}) {
  const contracts = buildContractsMap(contractRows);
  return asArray(registryRows)
    .filter((row) => row?.is_active !== false)
    .map((row) => {
      const schema_name = asText(row.schema_name);
      const table_name = asText(row.table_name);
      const contract = contracts.get(buildTableKey(schema_name, table_name));
      return {
        source_key: asText(row.source_key),
        source_kind: asText(row.source_kind || 'external') || 'external',
        source_name: asText(row.source_name || row.source_key),
        description: asText(row.description),
        schema_name,
        table_name,
        contract_name: asText(contract?.contract_name),
        contract_version: Number(contract?.version || row.contract_version || 0),
        freshness_expectation_minutes: Math.max(0, Number(row.expected_freshness_minutes || 0)),
        required_fields: asArray(parseJsonSafe(row.required_fields, [])).map((item) => asText(item)).filter(Boolean),
        optional_fields: asArray(parseJsonSafe(row.optional_fields, [])).map((item) => asText(item)).filter(Boolean),
        fields: asArray(contract?.columns)
      };
    })
    .filter((item) => item.source_key);
}

function sourceFieldValue(sources, sourceKey, fieldName) {
  return asObject(sources?.[sourceKey])?.[fieldName];
}

function safeFormula(formula, ctx) {
  const code = asText(formula);
  if (!code) return null;
  if (/[;`\\]/.test(code)) throw new Error('gold_formula_unsafe_chars');
  const sandbox = {
    row: ctx.row,
    sources: ctx.sources,
    Math,
    Number,
    String,
    Boolean,
    Date,
    coalesce: (...values) => values.find((value) => value !== null && value !== undefined && value !== '') ?? null,
    round: (value, precision = 0) => {
      const num = Number(value || 0);
      const factor = 10 ** Math.max(0, Number(precision || 0));
      return Math.round(num * factor) / factor;
    },
    source: (sourceKey, fieldName) => sourceFieldValue(ctx.sources, asText(sourceKey), asText(fieldName))
  };
  return vm.runInNewContext(code, sandbox, { timeout: 50 });
}

function compareValue(actual, operator, expected) {
  if (operator === 'eq') return actual === expected;
  if (operator === 'ne') return actual !== expected;
  if (operator === 'gt') return Number(actual) > Number(expected);
  if (operator === 'gte') return Number(actual) >= Number(expected);
  if (operator === 'lt') return Number(actual) < Number(expected);
  if (operator === 'lte') return Number(actual) <= Number(expected);
  if (operator === 'contains') return asText(actual).toLowerCase().includes(asText(expected).toLowerCase());
  if (operator === 'not_contains') return !asText(actual).toLowerCase().includes(asText(expected).toLowerCase());
  if (operator === 'is_empty') return actual === null || actual === undefined || actual === '';
  if (operator === 'not_empty') return !(actual === null || actual === undefined || actual === '');
  if (operator === 'in') return asArray(expected).map((item) => asText(item)).includes(asText(actual));
  return true;
}

function sourceRows(sourceRowsByKey, sourceKey) {
  return asArray(sourceRowsByKey?.[sourceKey]);
}

function primarySource(definition) {
  const normalized = normalizeGoldDefinition(definition);
  const scenario = getActiveScenario(normalized);
  const sources = asArray(scenario?.sources);
  return sources.find((item) => item.source_role === 'primary' || item.source_role === 'fact') || sources[0] || null;
}

function buildInitialContexts(definition, sourceRowsByKey) {
  const primary = primarySource(definition);
  if (!primary) return [];
  return sourceRows(sourceRowsByKey, primary.source_key).map((row) => ({
    __sources: { [primary.source_key]: row },
    __warnings: []
  }));
}

function relationTypeLabel(type) {
  const key = asText(type).toLowerCase();
  if (key === 'left_join') return 'left join';
  if (key === 'inner_join') return 'inner join';
  if (key === 'full_join') return 'full join';
  if (key === 'right_join') return 'right join';
  if (key === 'union') return 'union';
  if (key === 'append') return 'append';
  return 'lookup enrich';
}

function relationAllowsManyMatches(relation) {
  const cardinality = asText(relation?.cardinality);
  const relationType = asText(relation?.relation_type).toLowerCase();
  if (relationType === 'lookup_enrich') return false;
  if (cardinality === '1:N' || cardinality === 'N:N') return true;
  return false;
}

function valueWithTypePolicy(leftValue, rightValue, typePolicy) {
  if (typePolicy === 'cast') return [leftValue == null ? null : String(leftValue), rightValue == null ? null : String(rightValue)];
  if (typePolicy === 'normalize_text') return [asText(leftValue).toLowerCase(), asText(rightValue).toLowerCase()];
  return [leftValue, rightValue];
}

function normalizeJoinValue(value, typePolicy) {
  if (typePolicy === 'cast') return value == null ? null : String(value);
  if (typePolicy === 'normalize_text') return asText(value).toLowerCase();
  return value ?? null;
}

function canUseIndexedJoin(relation) {
  const joinKeys = asArray(relation?.join_keys);
  if (!joinKeys.length) return false;
  return joinKeys.every((key) => !asText(key?.operator) || asText(key?.operator).toLowerCase() === 'eq');
}

function joinSignature(row, joinKeys, fieldKey, typePolicy) {
  return JSON.stringify(
    asArray(joinKeys).map((key) => normalizeJoinValue(asObject(row)?.[key?.[fieldKey]], typePolicy))
  );
}

function buildRightLookup(rows, relation) {
  if (!canUseIndexedJoin(relation)) return null;
  const map = new Map();
  rows.forEach((row) => {
    const signature = joinSignature(row, relation.join_keys, 'right_field', relation.type_policy);
    const bucket = map.get(signature);
    if (bucket) bucket.push(row);
    else map.set(signature, [row]);
  });
  return map;
}

function joinMatches(leftRow, rightRow, joinKeys, typePolicy) {
  return asArray(joinKeys).every((key) => {
    const [leftValue, rightValue] = valueWithTypePolicy(leftRow?.[key.left_field], rightRow?.[key.right_field], typePolicy);
    return leftValue === rightValue;
  });
}

function relationForSource(scenario, sourceKey) {
  return asArray(scenario?.relations).find((item) => item.right_source_key === sourceKey || item.left_source_key === sourceKey) || null;
}

function resolveConflictKey(row, source, field, relation) {
  const baseKey = asText(field.alias || field.field_name);
  if (!baseKey) return '';
  if (!(baseKey in row)) return baseKey;
  const policy = asText(relation?.conflict_policy || 'keep_left');
  if (policy === 'keep_left') return '';
  if (policy === 'keep_right') return baseKey;
  if (policy === 'rename_with_alias' && asText(field.alias) && asText(field.alias) !== asText(field.field_name)) return asText(field.alias);
  const prefix = asText(relation?.rename_prefix || source?.source_role || source?.source_key).replace(/[^0-9a-zA-Z_а-яА-ЯёЁ]+/g, '_');
  return `${prefix}_${baseKey}`;
}

function applyRelations(contexts, definition, sourceRowsByKey) {
  const normalized = normalizeGoldDefinition(definition);
  const mart = getActiveDataMart(normalized);
  const scenario = getActiveScenario(normalized);
  const sources = asArray(scenario?.sources);
  const relations = asArray(scenario?.relations?.length ? scenario.relations : scenario?.transformations?.joins);
  const assembly = {
    desk_name: normalized.metadata.name,
    desk_code: normalized.metadata.code,
    mart_name: mart?.name || '',
    mart_code: mart?.code || '',
    scenario_name: scenario?.name || '',
    scenario_id: scenario?.id || '',
    primary_source_key: primarySource(normalized)?.source_key || '',
    row_count_before_merge: contexts.length,
    row_count_after_merge: contexts.length,
    sources: sources.map((source) => ({
      source_key: source.source_key,
      source_name: source.source_name,
      source_role: source.source_role,
      row_count: sourceRows(sourceRowsByKey, source.source_key).length
    })),
    relations: [],
    warnings: []
  };
  let current = contexts;
  relations.forEach((relation) => {
    const beforeCount = current.length;
    if (!relation.left_source_key || !relation.right_source_key) {
      assembly.relations.push({
        relation_key: relation.relation_key,
        relation_name: relation.relation_name,
        relation_type: relation.relation_type,
        before_count: beforeCount,
        after_count: beforeCount,
        skipped: true
      });
      return;
    }
    const rightRows = sourceRows(sourceRowsByKey, relation.right_source_key);
    const rightLookup = buildRightLookup(rightRows, relation);
    if (relation.relation_type === 'append' || relation.relation_type === 'union') {
      const appended = rightRows.map((row) => ({
        __sources: { [relation.right_source_key]: row },
        __warnings: []
      }));
      current = [...current, ...appended];
      assembly.relations.push({
        relation_key: relation.relation_key,
        relation_name: relation.relation_name,
        relation_type: relation.relation_type,
        relation_type_label: relationTypeLabel(relation.relation_type),
        before_count: beforeCount,
        after_count: current.length,
        join_keys: []
      });
      return;
    }

    const next = [];
    let matchedRows = 0;
    let droppedRows = 0;
    let warningCount = 0;
    current.forEach((ctx) => {
      const leftRow = asObject(ctx.__sources?.[relation.left_source_key]);
      const matches = rightLookup
        ? asArray(rightLookup.get(joinSignature(leftRow, relation.join_keys, 'left_field', relation.type_policy)))
        : rightRows.filter((row) => joinMatches(leftRow, row, relation.join_keys, relation.type_policy));
      if (matches.length) {
        matchedRows += matches.length;
        const allowManyMatches = relationAllowsManyMatches(relation);
        if (!allowManyMatches && matches.length > 1) warningCount += 1;
        const rowsToAttach = allowManyMatches ? matches : [matches[0]];
        rowsToAttach.forEach((match) => {
          next.push({
            __sources: {
              ...ctx.__sources,
              [relation.right_source_key]: match
            },
            __warnings: asArray(ctx.__warnings)
          });
        });
        return;
      }
      if (relation.mismatch_policy === 'drop_row' || relation.relation_type === 'inner_join') {
        droppedRows += 1;
        return;
      }
      if (relation.mismatch_policy === 'warning' || relation.mismatch_policy === 'error') warningCount += 1;
      next.push({
        __sources: {
          ...ctx.__sources,
          [relation.right_source_key]: null
        },
        __warnings: [...asArray(ctx.__warnings), `mismatch:${relation.relation_key}`]
      });
    });
    current = next;
    assembly.relations.push({
      relation_key: relation.relation_key,
      relation_name: relation.relation_name,
      relation_type: relation.relation_type,
      relation_type_label: relationTypeLabel(relation.relation_type),
      before_count: beforeCount,
      after_count: current.length,
      matched_rows: matchedRows,
      dropped_rows: droppedRows,
      warnings_count: warningCount,
      join_keys: asArray(relation.join_keys).map((item) => `${item.left_field} = ${item.right_field}`),
      cardinality: relation.cardinality,
      mismatch_policy: relation.mismatch_policy,
      conflict_policy: relation.conflict_policy,
      type_policy: relation.type_policy
    });
    if (warningCount > 0) {
      assembly.warnings.push({
        code: 'relation_cardinality_warning',
        message: `Связь ${relation.relation_name} нашла множественные совпадения при кардинальности ${relation.cardinality}.`
      });
    }
    if (droppedRows > 0) {
      assembly.warnings.push({
        code: 'rows_dropped_on_relation',
        message: `Связь ${relation.relation_name} отбросила ${droppedRows} строк.`
      });
    }
  });
  assembly.row_count_after_merge = current.length;
  if (sources.length > 1 && !relations.length) {
    assembly.warnings.push({
      code: 'relations_missing',
      message: 'Дополнительные источники не участвуют в сборке без явных relations.'
    });
  }
  return { contexts: current, assembly };
}

function flattenContext(definition, ctx) {
  const normalized = normalizeGoldDefinition(definition);
  const scenario = getActiveScenario(normalized);
  const row = {};
  asArray(scenario?.sources).forEach((source) => {
    const sourceRow = asObject(ctx.__sources?.[source.source_key]);
    const relation = relationForSource(scenario, source.source_key);
    source.selected_fields.forEach((field) => {
      const targetKey = resolveConflictKey(row, source, field, relation);
      if (!targetKey) return;
      row[targetKey] = sourceRow?.[field.field_name] ?? null;
    });
  });
  return row;
}

function applyFilters(rows, definition, contexts) {
  return rows.filter((row, index) => {
    return asArray(definition.transformations?.filters).every((filter) => {
      if (filter.formula) {
        return Boolean(safeFormula(filter.formula, { row, sources: contexts[index]?.__sources || {} }));
      }
      return compareValue(row?.[filter.field_name], filter.operator, filter.value);
    });
  });
}

function applyDerivedFields(rows, definition, contexts) {
  return rows.map((row, index) => {
    const next = { ...row };
    asArray(definition.transformations?.derived_fields).forEach((field) => {
      next[field.field_name] = field.formula
        ? safeFormula(field.formula, { row: next, sources: contexts[index]?.__sources || {} })
        : null;
    });
    ['mathematical_blocks', 'forecasting_blocks', 'inference_blocks'].forEach((section) => {
      asArray(definition.model_enrichment?.[section]).forEach((block) => {
        block.output_fields.forEach((field) => {
          next[field.field_name] = field.formula
            ? safeFormula(field.formula, { row: next, sources: contexts[index]?.__sources || {} })
            : null;
        });
      });
    });
    return next;
  });
}

function aggregateRows(rows, definition) {
  const groups = [...asArray(definition.transformations?.groupings), ...asArray(definition.transformations?.dimensions)];
  const metrics = [...asArray(definition.transformations?.aggregations), ...asArray(definition.transformations?.metrics)];
  if (!groups.length && !metrics.length) return rows;
  const grouped = new Map();
  rows.forEach((row) => {
    const keyValues = groups.map((group) => row?.[group.field_name]);
    const groupKey = JSON.stringify(keyValues);
    if (!grouped.has(groupKey)) grouped.set(groupKey, []);
    grouped.get(groupKey).push(row);
  });
  const result = [];
  grouped.forEach((bucket, groupKey) => {
    const groupValues = JSON.parse(groupKey);
    const next = {};
    groups.forEach((group, index) => {
      next[group.alias || group.field_name] = groupValues[index];
    });
    metrics.forEach((metric) => {
      const values = metric.field_name ? bucket.map((row) => row?.[metric.field_name]).filter((value) => value !== null && value !== undefined && value !== '') : [];
      if (metric.aggregator === 'count') next[metric.alias] = bucket.length;
      else if (metric.aggregator === 'count_distinct') next[metric.alias] = new Set(values.map((value) => JSON.stringify(value))).size;
      else if (metric.aggregator === 'avg') next[metric.alias] = values.length ? values.reduce((sum, value) => sum + Number(value || 0), 0) / values.length : 0;
      else if (metric.aggregator === 'min') next[metric.alias] = values.length ? values.reduce((min, value) => (Number(value) < Number(min) ? value : min), values[0]) : null;
      else if (metric.aggregator === 'max') next[metric.alias] = values.length ? values.reduce((max, value) => (Number(value) > Number(max) ? value : max), values[0]) : null;
      else next[metric.alias] = values.reduce((sum, value) => sum + Number(value || 0), 0);
    });
    result.push(next);
  });
  return result;
}

function inferColumns(rows, fallbackOutputFields) {
  if (rows.length) return [...new Set(rows.flatMap((row) => Object.keys(asObject(row))))];
  return asArray(fallbackOutputFields).map((field) => asText(field.name)).filter(Boolean);
}

function sampleAggregates(rows, outputFields) {
  const numericFields = asArray(outputFields).filter((field) => ['int', 'bigint', 'numeric'].includes(asText(field.type)));
  return numericFields.map((field) => ({
    field_name: field.name,
    sum: rows.reduce((sum, row) => sum + Number(row?.[field.name] || 0), 0),
    non_null_count: rows.filter((row) => row?.[field.name] !== null && row?.[field.name] !== undefined && row?.[field.name] !== '').length
  }));
}

export function buildGoldDataset(definition, { sourceRowsByKey = {} } = {}) {
  const normalized = normalizeGoldDefinition(definition);
  let contexts = buildInitialContexts(normalized, sourceRowsByKey);
  const relationResult = applyRelations(contexts, normalized, sourceRowsByKey);
  contexts = relationResult.contexts;
  let rows = contexts.map((ctx) => flattenContext(normalized, ctx));
  rows = applyFilters(rows, normalized, contexts);
  rows = applyDerivedFields(rows, normalized, contexts);
  rows = aggregateRows(rows, normalized);
  const outputFields = collectGoldOutputFields(normalized);
  return {
    rows,
    columns: inferColumns(rows, outputFields),
    output_fields: outputFields,
    sample_aggregates: sampleAggregates(rows, outputFields),
    warnings: asArray(relationResult.assembly?.warnings),
    assembly: relationResult.assembly
  };
}

export function buildGoldPreviewModel(
  definition,
  {
    sourceCatalogByKey = {},
    sourceRowsByKey = {},
    externalSourceStatesByKey = {},
    lastRefresh = null,
    publishedDefinition = null,
    nowMs = Date.now(),
    previewUid = `gold_preview_${Date.now()}`
  } = {}
) {
  const normalized = normalizeGoldDefinition(definition);
  const validation = validateGoldDefinition(normalized);
  const compatibility = checkSourceCompatibility(normalized, { sourceCatalogByKey });
  const freshness = resolveExternalSourceFreshness(normalized, { externalSourceStatesByKey, nowMs });
  const dataset = buildGoldDataset(normalized, { sourceRowsByKey });
  const status = resolveGoldStatus({
    definition: normalized,
    publishedDefinition,
    lastRefresh,
    compatibility,
    sourceFreshness: freshness
  });

  const quality_warnings = [];
  if (!dataset.rows.length) {
    quality_warnings.push({ code: 'preview_empty', message: 'Preview не вернул строк.' });
  }
  normalized.quality.required_output_fields.forEach((fieldName) => {
    if (!dataset.columns.includes(fieldName)) {
      quality_warnings.push({
        code: 'required_output_field_missing',
        message: `В preview нет обязательного поля ${fieldName}.`
      });
    }
  });

  return {
    definition: normalized,
    validation,
    compatibility,
    freshness,
    status,
    dataset,
    preview_uid: previewUid,
    lineage: buildGoldDependencyGraph(normalized),
    impact: analyzeGoldImpact(normalized, { changedSources: [] }),
    consumers: resolveConsumerDependencies(normalized),
    quality_warnings
  };
}
