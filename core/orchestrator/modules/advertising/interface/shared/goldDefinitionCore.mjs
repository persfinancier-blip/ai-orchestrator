const SOURCE_KINDS = new Set(['process', 'external', 'reference']);
const MATERIALIZATION_MODES = new Set(['live_view', 'materialized_view', 'snapshot_table']);
const REFRESH_MODES = new Set(['manual', 'interval', 'cron', 'dependency']);
const GOLD_MACHINE_STATUSES = ['draft', 'published', 'healthy', 'stale', 'incompatible', 'partial', 'broken', 'disabled'];

function asText(value) {
  return String(value || '').trim();
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function asObject(value) {
  return value && typeof value === 'object' && !Array.isArray(value) ? value : {};
}

function uniqueStrings(values = []) {
  return [...new Set(asArray(values).map((item) => asText(item)).filter(Boolean))];
}

function normalizeFieldName(value, fallback = '') {
  const raw = asText(value || fallback);
  return raw.replace(/\s+/g, '_');
}

function normalizeFieldType(value) {
  const raw = asText(value).toLowerCase();
  if (raw === 'int' || raw === 'integer') return 'int';
  if (raw === 'bigint') return 'bigint';
  if (raw === 'numeric' || raw === 'decimal') return 'numeric';
  if (raw === 'boolean' || raw === 'bool') return 'boolean';
  if (raw === 'date') return 'date';
  if (raw === 'timestamptz' || raw === 'timestamp with time zone') return 'timestamptz';
  if (raw === 'jsonb' || raw === 'json') return 'jsonb';
  if (raw === 'uuid') return 'uuid';
  return raw || 'text';
}

function parseScheduleToMs(raw) {
  const value = asText(raw).toLowerCase();
  if (!value) return 0;
  const match = value.match(/^(\d+)\s*(ms|s|m|h|d)$/);
  if (!match) return 0;
  const amount = Number(match[1] || 0);
  const unit = match[2] || 'm';
  if (!(amount > 0)) return 0;
  if (unit === 'ms') return amount;
  if (unit === 's') return amount * 1000;
  if (unit === 'm') return amount * 60_000;
  if (unit === 'h') return amount * 3_600_000;
  return amount * 86_400_000;
}

function toTs(value) {
  const raw = asText(value);
  if (!raw) return 0;
  const ts = Date.parse(raw);
  return Number.isFinite(ts) ? ts : 0;
}

function ensureSection(value, defaults = {}) {
  return { ...defaults, ...asObject(value) };
}

function normalizeSelectedField(raw = {}) {
  return {
    field_name: normalizeFieldName(raw.field_name || raw.name),
    alias: normalizeFieldName(raw.alias || raw.output_name || raw.field_name || raw.name),
    field_type: normalizeFieldType(raw.field_type || raw.type || 'text'),
    required: Boolean(raw.required),
    description: asText(raw.description),
    path: asText(raw.path)
  };
}

function normalizeSource(raw = {}, index = 0) {
  const source_kind = SOURCE_KINDS.has(asText(raw.source_kind).toLowerCase()) ? asText(raw.source_kind).toLowerCase() : 'process';
  const selectedFields = asArray(raw.selected_fields).map(normalizeSelectedField).filter((item) => item.field_name);
  return {
    source_key: asText(raw.source_key || raw.id || `${source_kind}_${index + 1}`),
    source_kind,
    source_name: asText(raw.source_name || raw.name || `Источник ${index + 1}`),
    process_code: asText(raw.process_code),
    desk_id: Math.max(0, Number(raw.desk_id || 0)),
    start_node_id: asText(raw.start_node_id),
    schema_name: asText(raw.schema_name),
    table_name: asText(raw.table_name),
    contract_version: Math.max(0, Number(raw.contract_version || 0)),
    required_fields: uniqueStrings(raw.required_fields),
    optional_fields: uniqueStrings(raw.optional_fields),
    selected_fields: selectedFields,
    freshness_expectation_minutes: Math.max(0, Number(raw.freshness_expectation_minutes || 0)),
    output_aliases: { ...asObject(raw.output_aliases) }
  };
}

function normalizeDerivedField(raw = {}, index = 0) {
  return {
    field_name: normalizeFieldName(raw.field_name || raw.name || `derived_${index + 1}`),
    field_type: normalizeFieldType(raw.field_type || raw.type || 'text'),
    formula: asText(raw.formula),
    description: asText(raw.description)
  };
}

function normalizeJoin(raw = {}, index = 0) {
  return {
    join_key: asText(raw.join_key || raw.id || `join_${index + 1}`),
    left_source_key: asText(raw.left_source_key),
    right_source_key: asText(raw.right_source_key),
    left_field: normalizeFieldName(raw.left_field),
    right_field: normalizeFieldName(raw.right_field),
    join_type: ['left', 'inner', 'full'].includes(asText(raw.join_type).toLowerCase()) ? asText(raw.join_type).toLowerCase() : 'left'
  };
}

function normalizeFilter(raw = {}, index = 0) {
  return {
    filter_key: asText(raw.filter_key || raw.id || `filter_${index + 1}`),
    field_name: normalizeFieldName(raw.field_name || raw.field),
    operator: asText(raw.operator || 'eq').toLowerCase() || 'eq',
    value: raw.value,
    formula: asText(raw.formula)
  };
}

function normalizeGrouping(raw = {}, index = 0) {
  return {
    grouping_key: asText(raw.grouping_key || raw.id || `group_${index + 1}`),
    field_name: normalizeFieldName(raw.field_name || raw.field),
    alias: normalizeFieldName(raw.alias || raw.field_name || raw.field || `group_${index + 1}`)
  };
}

function normalizeAggregation(raw = {}, index = 0) {
  return {
    metric_key: asText(raw.metric_key || raw.id || `metric_${index + 1}`),
    alias: normalizeFieldName(raw.alias || raw.field_name || `metric_${index + 1}`),
    field_name: normalizeFieldName(raw.field_name || raw.field),
    aggregator: asText(raw.aggregator || 'sum').toLowerCase() || 'sum',
    field_type: normalizeFieldType(raw.field_type || raw.type || 'numeric'),
    description: asText(raw.description)
  };
}

function normalizeModelBlock(raw = {}, index = 0) {
  const outputs = asArray(raw.output_fields).map((item, itemIndex) => ({
    field_name: normalizeFieldName(item.field_name || item.name || `model_output_${itemIndex + 1}`),
    field_type: normalizeFieldType(item.field_type || item.type || 'numeric'),
    formula: asText(item.formula),
    description: asText(item.description)
  }));
  return {
    block_key: asText(raw.block_key || raw.id || `model_${index + 1}`),
    block_name: asText(raw.block_name || raw.name || `Модель ${index + 1}`),
    model_key: asText(raw.model_key || raw.block_key || raw.id),
    model_version: asText(raw.model_version || raw.version || 'draft'),
    required_input_features: uniqueStrings(raw.required_input_features),
    output_fields: outputs,
    retraining_policy: asText(raw.retraining_policy || ''),
    block_type: asText(raw.block_type || 'custom').toLowerCase() || 'custom'
  };
}

function normalizeConsumer(raw = {}, index = 0) {
  return {
    consumer_key: asText(raw.consumer_key || raw.id || `consumer_${index + 1}`),
    consumer_kind: asText(raw.consumer_kind || raw.kind || 'report').toLowerCase() || 'report',
    name: asText(raw.name || raw.consumer_key || `Потребитель ${index + 1}`),
    description: asText(raw.description)
  };
}

export function normalizeGoldDefinition(raw = {}) {
  const metadata = ensureSection(raw.metadata, raw);
  const transformations = ensureSection(raw.transformations);
  const model_enrichment = ensureSection(raw.model_enrichment);
  const materialization = ensureSection(raw.materialization);
  const refresh_policy = ensureSection(raw.refresh_policy);
  const quality = ensureSection(raw.quality);

  return {
    metadata: {
      id: asText(metadata.id),
      code: normalizeFieldName(metadata.code || raw.code),
      name: asText(metadata.name || raw.name),
      description: asText(metadata.description || raw.description),
      owner: asText(metadata.owner || raw.owner),
      tags: uniqueStrings(metadata.tags || raw.tags),
      status: asText(metadata.status || raw.status || 'draft'),
      version: Math.max(1, Number(metadata.version || raw.version || 1)),
      published: Boolean(metadata.published ?? raw.published)
    },
    sources: asArray(raw.sources).map(normalizeSource),
    source_contracts: asArray(raw.source_contracts).map((item) => ({
      source_key: asText(item.source_key),
      source_table: asObject(item.source_table),
      source_contract_version: Math.max(0, Number(item.source_contract_version || 0)),
      required_fields: uniqueStrings(item.required_fields),
      optional_fields: uniqueStrings(item.optional_fields),
      freshness_expectation_minutes: Math.max(0, Number(item.freshness_expectation_minutes || 0))
    })),
    transformations: {
      joins: asArray(transformations.joins).map(normalizeJoin),
      filters: asArray(transformations.filters).map(normalizeFilter),
      derived_fields: asArray(transformations.derived_fields).map(normalizeDerivedField),
      metrics: asArray(transformations.metrics).map(normalizeAggregation),
      dimensions: asArray(transformations.dimensions).map(normalizeGrouping),
      windows: asArray(transformations.windows).map((item) => ({ ...asObject(item) })),
      groupings: asArray(transformations.groupings).map(normalizeGrouping),
      aggregations: asArray(transformations.aggregations).map(normalizeAggregation)
    },
    model_enrichment: {
      mathematical_blocks: asArray(model_enrichment.mathematical_blocks || model_enrichment.math_blocks).map(normalizeModelBlock),
      forecasting_blocks: asArray(model_enrichment.forecasting_blocks || model_enrichment.forecast_blocks).map(normalizeModelBlock),
      inference_blocks: asArray(model_enrichment.inference_blocks).map(normalizeModelBlock)
    },
    materialization: {
      mode: MATERIALIZATION_MODES.has(asText(materialization.mode).toLowerCase())
        ? asText(materialization.mode).toLowerCase()
        : 'snapshot_table',
      target_schema: asText(materialization.target_schema || 'gold_showcase'),
      target_name: normalizeFieldName(materialization.target_name || metadata.code || raw.code),
      table_class: asText(materialization.table_class || 'gold_showcase'),
      data_level: asText(materialization.data_level || 'gold')
    },
    refresh_policy: {
      mode: REFRESH_MODES.has(asText(refresh_policy.mode).toLowerCase()) ? asText(refresh_policy.mode).toLowerCase() : 'manual',
      schedule_value: asText(refresh_policy.schedule_value),
      timezone: asText(refresh_policy.timezone || 'UTC'),
      dependency_sources: uniqueStrings(refresh_policy.dependency_sources)
    },
    quality: {
      required_output_fields: uniqueStrings(quality.required_output_fields || quality.required_fields),
      freshness_expectation_minutes: Math.max(0, Number(quality.freshness_expectation_minutes || 0)),
      completeness_threshold_pct: Math.max(0, Math.min(100, Number(quality.completeness_threshold_pct || 0)))
    },
    consumers: asArray(raw.consumers).map(normalizeConsumer)
  };
}

export function collectGoldOutputFields(definition) {
  const normalized = normalizeGoldDefinition(definition);
  const output = [];
  const seen = new Set();
  const pushField = (name, type = 'text', description = '') => {
    const key = normalizeFieldName(name);
    if (!key || seen.has(key)) return;
    seen.add(key);
    output.push({ name: key, type: normalizeFieldType(type), description: asText(description) });
  };

  normalized.sources.forEach((source) => {
    source.selected_fields.forEach((field) => pushField(field.alias || field.field_name, field.field_type, field.description));
  });
  normalized.transformations.derived_fields.forEach((field) => pushField(field.field_name, field.field_type, field.description));
  normalized.transformations.groupings.forEach((field) => pushField(field.alias || field.field_name, 'text'));
  normalized.transformations.aggregations.forEach((field) => pushField(field.alias, field.field_type, field.description));
  normalized.transformations.metrics.forEach((field) => pushField(field.alias, field.field_type, field.description));
  ['mathematical_blocks', 'forecasting_blocks', 'inference_blocks'].forEach((section) => {
    normalized.model_enrichment[section].forEach((block) => {
      block.output_fields.forEach((field) => pushField(field.field_name, field.field_type, field.description));
    });
  });
  return output;
}

export function validateGoldDefinition(definition) {
  const normalized = normalizeGoldDefinition(definition);
  const errors = [];
  const warnings = [];
  const pushError = (code, message, context = {}) => errors.push({ code, message, context });
  const pushWarning = (code, message, context = {}) => warnings.push({ code, message, context });

  if (!normalized.metadata.code) pushError('metadata_code_required', 'У витрины должен быть код.');
  if (!normalized.metadata.name) pushError('metadata_name_required', 'У витрины должно быть название.');
  if (!normalized.sources.length) pushError('sources_required', 'Нужно указать хотя бы один источник.');

  normalized.sources.forEach((source, index) => {
    if (!source.source_key) pushError('source_key_required', 'Источник должен иметь source_key.', { index });
    if (!SOURCE_KINDS.has(source.source_kind)) pushError('source_kind_invalid', 'У источника недопустимый тип.', { index });
    if (!source.selected_fields.length) pushWarning('source_selected_fields_empty', 'Источник не публикует поля в витрину.', { index });
  });

  const outputFields = collectGoldOutputFields(normalized);
  if (!outputFields.length) pushError('output_fields_required', 'Нужно определить хотя бы одно выходное поле витрины.');

  if (normalized.materialization.mode === 'snapshot_table') {
    if (!normalized.materialization.target_schema || !normalized.materialization.target_name) {
      pushError('materialization_target_required', 'Для snapshot table нужно указать схему и имя объекта.');
    }
  }

  if (normalized.refresh_policy.mode === 'interval' && !parseScheduleToMs(normalized.refresh_policy.schedule_value)) {
    pushError('refresh_schedule_required', 'Для interval refresh нужно указать интервал вида 15m / 1h / 1d.');
  }
  if (normalized.refresh_policy.mode === 'cron' && !normalized.refresh_policy.schedule_value) {
    pushError('refresh_schedule_required', 'Для cron refresh нужно указать cron-выражение.');
  }
  if (normalized.refresh_policy.mode === 'dependency' && !normalized.refresh_policy.dependency_sources.length) {
    pushError('refresh_dependency_required', 'Для dependency refresh нужно выбрать зависимые источники.');
  }

  const modelBinding = validateModelBindings(normalized);
  errors.push(...modelBinding.errors);
  warnings.push(...modelBinding.warnings);

  return { ok: errors.length === 0, errors, warnings, output_fields: outputFields };
}

export function checkSourceCompatibility(definition, { sourceCatalogByKey = {} } = {}) {
  const normalized = normalizeGoldDefinition(definition);
  const sources = normalized.sources.map((source) => {
    const catalog = asObject(sourceCatalogByKey?.[source.source_key]);
    const contractFields = uniqueStrings(asArray(catalog.fields).map((field) => asText(field?.name || field?.field_name)));
    const missingRequired = source.required_fields.filter((field) => !contractFields.includes(field));
    const missingSelected = source.selected_fields
      .map((field) => field.field_name)
      .filter((field) => field && !contractFields.includes(field));
    return {
      source_key: source.source_key,
      source_name: source.source_name || asText(catalog.source_name),
      compatible: !missingRequired.length && !missingSelected.length && Boolean(contractFields.length),
      missing_required_fields: missingRequired,
      missing_selected_fields: missingSelected,
      available_fields: contractFields
    };
  });
  return { ok: sources.every((item) => item.compatible), sources };
}

export function validateModelBindings(definition) {
  const normalized = normalizeGoldDefinition(definition);
  const errors = [];
  const warnings = [];
  const availableFields = new Set(
    collectGoldOutputFields({
      ...normalized,
      model_enrichment: { mathematical_blocks: [], forecasting_blocks: [], inference_blocks: [] }
    }).map((field) => field.name)
  );

  const allBlocks = [
    ...normalized.model_enrichment.mathematical_blocks,
    ...normalized.model_enrichment.forecasting_blocks,
    ...normalized.model_enrichment.inference_blocks
  ];
  allBlocks.forEach((block) => {
    block.required_input_features.forEach((feature) => {
      if (!availableFields.has(normalizeFieldName(feature))) {
        errors.push({
          code: 'model_missing_input_feature',
          message: `Модель ${block.block_name} требует вход ${feature}, которого нет в витрине.`,
          context: { block_key: block.block_key, feature }
        });
      }
    });
    const seen = new Set();
    block.output_fields.forEach((field) => {
      const key = normalizeFieldName(field.field_name);
      if (seen.has(key)) {
        errors.push({
          code: 'model_duplicate_output_field',
          message: `Модель ${block.block_name} публикует дублирующееся поле ${key}.`,
          context: { block_key: block.block_key, field_name: key }
        });
      }
      seen.add(key);
      if (!field.formula) {
        warnings.push({
          code: 'model_output_formula_missing',
          message: `У поля ${key} модели ${block.block_name} нет формулы preview/inference.`,
          context: { block_key: block.block_key, field_name: key }
        });
      }
    });
  });
  return { ok: errors.length === 0, errors, warnings };
}

export function buildGoldDependencyGraph(definition) {
  const normalized = normalizeGoldDefinition(definition);
  const goldKey = `gold:${normalized.metadata.code || normalized.metadata.id || 'draft'}`;
  const nodes = [
    {
      key: goldKey,
      node_type: 'gold_definition',
      label: normalized.metadata.name || normalized.metadata.code || 'Gold'
    }
  ];
  const edges = [];

  normalized.sources.forEach((source) => {
    nodes.push({
      key: source.source_key,
      node_type: `${source.source_kind}_source`,
      label: source.source_name || source.source_key
    });
    edges.push({ from: source.source_key, to: goldKey, edge_type: 'feeds' });
  });

  normalized.consumers.forEach((consumer) => {
    nodes.push({
      key: consumer.consumer_key,
      node_type: `${consumer.consumer_kind}_consumer`,
      label: consumer.name || consumer.consumer_key
    });
    edges.push({ from: goldKey, to: consumer.consumer_key, edge_type: 'consumed_by' });
  });

  return { gold_key: goldKey, nodes, edges };
}

export function analyzeGoldImpact(definition, { changedSources = [] } = {}) {
  const normalized = normalizeGoldDefinition(definition);
  const bySource = new Map(asArray(changedSources).map((item) => [asText(item?.source_key), asObject(item)]));
  const affected_sources = [];
  normalized.sources.forEach((source) => {
    const change = bySource.get(source.source_key);
    if (!change) return;
    const removed = uniqueStrings(change.removed_fields);
    const touched = source.required_fields.filter((field) => removed.includes(field));
    const selectedBroken = source.selected_fields.map((field) => field.field_name).filter((field) => removed.includes(field));
    if (!touched.length && !selectedBroken.length && !change.compatibility_broken) return;
    affected_sources.push({
      source_key: source.source_key,
      severity: touched.length ? 'high' : 'medium',
      removed_required_fields: touched,
      removed_selected_fields: selectedBroken
    });
  });
  const affected_consumers = affected_sources.length ? normalized.consumers.map((consumer) => ({ ...consumer })) : [];
  return {
    has_impact: affected_sources.length > 0,
    affected_sources,
    affected_consumers
  };
}

export function planGoldRefresh(definition, { nowMs = Date.now(), lastRefreshAt = '', dependencyEvents = [] } = {}) {
  const normalized = normalizeGoldDefinition(definition);
  const mode = normalized.refresh_policy.mode;
  const lastTs = toTs(lastRefreshAt);
  if (mode === 'manual') {
    return { mode, needs_refresh: false, reason_code: 'manual_only', next_due_at: '' };
  }
  if (mode === 'interval') {
    const everyMs = parseScheduleToMs(normalized.refresh_policy.schedule_value);
    if (!everyMs) return { mode, needs_refresh: false, reason_code: 'interval_invalid', next_due_at: '' };
    const dueTs = lastTs > 0 ? lastTs + everyMs : nowMs;
    return {
      mode,
      needs_refresh: dueTs <= nowMs,
      reason_code: dueTs <= nowMs ? 'interval_due' : 'interval_waiting',
      next_due_at: new Date(dueTs).toISOString()
    };
  }
  if (mode === 'dependency') {
    const changed = asArray(dependencyEvents).find((event) => {
      const sourceKey = asText(event?.source_key);
      return normalized.refresh_policy.dependency_sources.includes(sourceKey) && toTs(event?.changed_at) > lastTs;
    });
    return {
      mode,
      needs_refresh: Boolean(changed),
      reason_code: changed ? 'dependency_changed' : 'dependency_waiting',
      next_due_at: ''
    };
  }
  return {
    mode,
    needs_refresh: false,
    reason_code: normalized.refresh_policy.schedule_value ? 'cron_waiting' : 'cron_not_evaluated',
    next_due_at: ''
  };
}

export function planGoldMaterialization(definition) {
  const normalized = normalizeGoldDefinition(definition);
  return {
    mode: normalized.materialization.mode,
    target: {
      schema_name: normalized.materialization.target_schema,
      name: normalized.materialization.target_name,
      table_class: normalized.materialization.table_class,
      data_level: normalized.materialization.data_level
    },
    output_fields: collectGoldOutputFields(normalized)
  };
}

export function resolveExternalSourceFreshness(definition, { externalSourceStatesByKey = {}, nowMs = Date.now() } = {}) {
  const normalized = normalizeGoldDefinition(definition);
  const sources = normalized.sources
    .filter((source) => source.source_kind === 'external' || source.source_kind === 'reference')
    .map((source) => {
      const state = asObject(externalSourceStatesByKey?.[source.source_key]);
      const lastSeenTs = toTs(state.last_seen_at);
      const freshnessMs = Math.max(0, Number(source.freshness_expectation_minutes || 0)) * 60_000;
      const stale = freshnessMs > 0 && lastSeenTs > 0 ? nowMs - lastSeenTs > freshnessMs : false;
      const missing = !lastSeenTs;
      return {
        source_key: source.source_key,
        source_name: source.source_name,
        stale,
        missing,
        freshness_expectation_minutes: source.freshness_expectation_minutes,
        last_seen_at: asText(state.last_seen_at),
        schema_version: asText(state.schema_version)
      };
    });
  return {
    sources,
    summary: {
      stale_count: sources.filter((item) => item.stale).length,
      missing_count: sources.filter((item) => item.missing).length
    }
  };
}

export function resolveConsumerDependencies(definition) {
  const normalized = normalizeGoldDefinition(definition);
  const counts = {};
  normalized.consumers.forEach((consumer) => {
    counts[consumer.consumer_kind] = (counts[consumer.consumer_kind] || 0) + 1;
  });
  return { consumers: normalized.consumers, counts };
}

export function resolveGoldStatus({
  definition,
  publishedDefinition = null,
  lastRefresh = null,
  compatibility = null,
  sourceFreshness = null
} = {}) {
  const normalized = normalizeGoldDefinition(definition);
  const machine_statuses = [];
  const add = (code) => {
    if (GOLD_MACHINE_STATUSES.includes(code) && !machine_statuses.includes(code)) machine_statuses.push(code);
  };

  add(normalized.metadata.published || publishedDefinition ? 'published' : 'draft');
  if (normalized.metadata.status === 'disabled') add('disabled');

  const refreshStatus = asText(lastRefresh?.status).toLowerCase();
  if (refreshStatus === 'failed' || refreshStatus === 'error') add('broken');
  if (refreshStatus === 'completed_with_warnings') add('partial');
  if (compatibility && compatibility.ok === false) add('incompatible');
  if (sourceFreshness?.summary?.stale_count || sourceFreshness?.summary?.missing_count) add('stale');
  if (!machine_statuses.includes('broken') && !machine_statuses.includes('incompatible') && !machine_statuses.includes('stale')) {
    add('healthy');
  }

  let health_code = 'healthy';
  if (machine_statuses.includes('broken')) health_code = 'broken';
  else if (machine_statuses.includes('incompatible')) health_code = 'incompatible';
  else if (machine_statuses.includes('stale')) health_code = 'stale';
  else if (machine_statuses.includes('partial')) health_code = 'partial';
  else if (machine_statuses.includes('disabled')) health_code = 'disabled';

  return {
    code: normalized.metadata.code,
    name: normalized.metadata.name,
    published: machine_statuses.includes('published'),
    machine_statuses,
    health_code,
    last_refresh_at: asText(lastRefresh?.finished_at || lastRefresh?.started_at),
    last_refresh_status: refreshStatus || 'never'
  };
}
