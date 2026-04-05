const SOURCE_KINDS = new Set(['process', 'external', 'reference']);
const SOURCE_ROLES = new Set(['primary', 'lookup', 'reference', 'fact', 'append']);
const RELATION_TYPES = new Set(['left_join', 'inner_join', 'full_join', 'right_join', 'union', 'append', 'lookup_enrich']);
const CARDINALITIES = new Set(['1:1', '1:N', 'N:1', 'N:N']);
const MISMATCH_POLICIES = new Set(['keep_primary', 'drop_row', 'null', 'warning', 'error']);
const CONFLICT_POLICIES = new Set(['keep_left', 'keep_right', 'rename_with_prefix', 'rename_with_alias', 'explicit']);
const TYPE_POLICIES = new Set(['strict', 'cast', 'normalize_text']);
const MATERIALIZATION_MODES = new Set(['live_view', 'materialized_view', 'snapshot_table']);
const REFRESH_MODES = new Set(['manual', 'interval', 'cron', 'dependency']);
const GOLD_MACHINE_STATUSES = ['draft', 'published', 'healthy', 'stale', 'incompatible', 'partial', 'broken', 'disabled'];

function asText(value) {
  return String(value ?? '').trim();
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function asObject(value) {
  return value && typeof value === 'object' && !Array.isArray(value) ? value : {};
}

function cloneJson(value) {
  return JSON.parse(JSON.stringify(value ?? null));
}

function uniqueStrings(values = []) {
  return [...new Set(asArray(values).map((item) => asText(item)).filter(Boolean))];
}

function slugify(value, fallback = '') {
  const raw = asText(value || fallback)
    .replace(/\s+/g, '_')
    .replace(/[^0-9a-zA-Z_а-яА-ЯёЁ]+/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '');
  return raw || asText(fallback);
}

function normalizeFieldName(value, fallback = '') {
  return slugify(value, fallback);
}

function normalizeFieldType(value) {
  const raw = asText(value).toLowerCase();
  if (raw === 'int' || raw === 'integer') return 'int';
  if (raw === 'bigint') return 'bigint';
  if (raw === 'numeric' || raw === 'decimal' || raw === 'number') return 'numeric';
  if (raw === 'boolean' || raw === 'bool') return 'boolean';
  if (raw === 'date') return 'date';
  if (raw === 'timestamp' || raw === 'timestamp with time zone' || raw === 'timestamptz') return 'timestamptz';
  if (raw === 'json' || raw === 'jsonb') return 'jsonb';
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

function inferSourceRole(rawRole, index) {
  const role = asText(rawRole).toLowerCase();
  if (SOURCE_ROLES.has(role)) return role;
  return index === 0 ? 'primary' : 'lookup';
}

function normalizeSelectedField(raw = {}) {
  const fieldName = normalizeFieldName(raw.field_name || raw.name, raw.field_name || raw.name);
  return {
    field_name: fieldName,
    alias: normalizeFieldName(raw.alias || raw.output_name || fieldName, fieldName),
    field_type: normalizeFieldType(raw.field_type || raw.type || 'text'),
    required: Boolean(raw.required),
    description: asText(raw.description),
    path: asText(raw.path)
  };
}

function normalizeJoinKey(raw = {}, index = 0) {
  return {
    key_id: asText(raw.key_id || raw.id || `key_${index + 1}`),
    left_field: normalizeFieldName(raw.left_field || raw.source_a_field || raw.field_a),
    right_field: normalizeFieldName(raw.right_field || raw.source_b_field || raw.field_b),
    operator: asText(raw.operator || 'eq').toLowerCase() || 'eq'
  };
}

function normalizeSource(raw = {}, index = 0) {
  const source_kind = SOURCE_KINDS.has(asText(raw.source_kind).toLowerCase()) ? asText(raw.source_kind).toLowerCase() : 'process';
  return {
    source_key: asText(raw.source_key || raw.id || `${source_kind}_${index + 1}`),
    source_kind,
    source_role: inferSourceRole(raw.source_role || raw.role, index),
    source_name: asText(raw.source_name || raw.name || `Источник ${index + 1}`),
    process_code: asText(raw.process_code),
    desk_id: Math.max(0, Number(raw.desk_id || 0)),
    start_node_id: asText(raw.start_node_id),
    schema_name: asText(raw.schema_name),
    table_name: asText(raw.table_name),
    contract_version: Math.max(0, Number(raw.contract_version || 0)),
    required_fields: uniqueStrings(raw.required_fields),
    optional_fields: uniqueStrings(raw.optional_fields),
    selected_fields: asArray(raw.selected_fields).map(normalizeSelectedField).filter((item) => item.field_name),
    freshness_expectation_minutes: Math.max(0, Number(raw.freshness_expectation_minutes || 0)),
    output_aliases: { ...asObject(raw.output_aliases) }
  };
}

function normalizeRelation(raw = {}, index = 0) {
  const joinType = asText(raw.join_type).toLowerCase();
  const rawType = asText(raw.relation_type || raw.type).toLowerCase();
  let relationType = rawType;
  if (!RELATION_TYPES.has(relationType)) {
    if (joinType === 'left') relationType = 'left_join';
    else if (joinType === 'inner') relationType = 'inner_join';
    else if (joinType === 'full') relationType = 'full_join';
    else relationType = index === 0 ? 'left_join' : 'lookup_enrich';
  }
  const join_keys = asArray(raw.join_keys).map(normalizeJoinKey).filter((item) => item.left_field && item.right_field);
  if (!join_keys.length && (asText(raw.left_field) || asText(raw.right_field))) {
    const single = normalizeJoinKey(raw, 0);
    if (single.left_field && single.right_field) join_keys.push(single);
  }
  return {
    relation_key: asText(raw.relation_key || raw.join_key || raw.id || `relation_${index + 1}`),
    relation_name: asText(raw.relation_name || raw.name || `Связь ${index + 1}`),
    relation_type: relationType,
    left_source_key: asText(raw.left_source_key),
    right_source_key: asText(raw.right_source_key),
    join_keys,
    cardinality: CARDINALITIES.has(asText(raw.cardinality)) ? asText(raw.cardinality) : 'N:1',
    mismatch_policy: MISMATCH_POLICIES.has(asText(raw.mismatch_policy)) ? asText(raw.mismatch_policy) : 'keep_primary',
    conflict_policy: CONFLICT_POLICIES.has(asText(raw.conflict_policy)) ? asText(raw.conflict_policy) : 'keep_left',
    type_policy: TYPE_POLICIES.has(asText(raw.type_policy)) ? asText(raw.type_policy) : 'strict',
    rename_prefix: normalizeFieldName(raw.rename_prefix || raw.right_prefix),
    alias_map: { ...asObject(raw.alias_map) }
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

function normalizeDerivedField(raw = {}, index = 0) {
  return {
    field_name: normalizeFieldName(raw.field_name || raw.name || `derived_${index + 1}`),
    field_type: normalizeFieldType(raw.field_type || raw.type || 'text'),
    formula: asText(raw.formula),
    description: asText(raw.description)
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
  return {
    block_key: asText(raw.block_key || raw.id || `model_${index + 1}`),
    block_name: asText(raw.block_name || raw.name || `Модель ${index + 1}`),
    model_key: asText(raw.model_key || raw.block_key || raw.id),
    model_version: asText(raw.model_version || raw.version || 'draft'),
    retraining_policy: asText(raw.retraining_policy),
    block_type: asText(raw.block_type || 'custom').toLowerCase() || 'custom',
    required_input_features: uniqueStrings(raw.required_input_features),
    output_fields: asArray(raw.output_fields)
      .map((item, itemIndex) => ({
        field_name: normalizeFieldName(item.field_name || item.name || `model_output_${itemIndex + 1}`),
        field_type: normalizeFieldType(item.field_type || item.type || 'numeric'),
        formula: asText(item.formula),
        description: asText(item.description)
      }))
      .filter((item) => item.field_name)
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

function normalizeSourceContract(raw = {}) {
  return {
    source_key: asText(raw.source_key),
    source_table: asObject(raw.source_table),
    source_contract_version: Math.max(0, Number(raw.source_contract_version || 0)),
    required_fields: uniqueStrings(raw.required_fields),
    optional_fields: uniqueStrings(raw.optional_fields),
    freshness_expectation_minutes: Math.max(0, Number(raw.freshness_expectation_minutes || 0))
  };
}

function normalizePreviewState(raw = {}) {
  const obj = asObject(raw);
  return {
    status: asText(obj.status || 'idle') || 'idle',
    preview_uid: asText(obj.preview_uid),
    updated_at: asText(obj.updated_at),
    signature: asText(obj.signature),
    stale_reason: asText(obj.stale_reason)
  };
}

function normalizePublishState(raw = {}) {
  const obj = asObject(raw);
  return {
    status: asText(obj.status || 'draft') || 'draft',
    published_version: Math.max(0, Number(obj.published_version || 0)),
    published_at: asText(obj.published_at),
    published_by: asText(obj.published_by)
  };
}

function normalizeMaterialization(raw = {}, fallbackCode = '') {
  return {
    mode: MATERIALIZATION_MODES.has(asText(raw.mode).toLowerCase()) ? asText(raw.mode).toLowerCase() : 'snapshot_table',
    target_schema: asText(raw.target_schema || 'gold_showcase'),
    target_name: normalizeFieldName(raw.target_name || fallbackCode, fallbackCode),
    table_class: asText(raw.table_class || 'gold_showcase'),
    data_level: asText(raw.data_level || 'gold')
  };
}

function normalizeRefreshPolicy(raw = {}) {
  return {
    mode: REFRESH_MODES.has(asText(raw.mode).toLowerCase()) ? asText(raw.mode).toLowerCase() : 'manual',
    schedule_value: asText(raw.schedule_value),
    timezone: asText(raw.timezone || 'UTC'),
    dependency_sources: uniqueStrings(raw.dependency_sources)
  };
}

function normalizeQuality(raw = {}) {
  return {
    required_output_fields: uniqueStrings(raw.required_output_fields || raw.required_fields),
    freshness_expectation_minutes: Math.max(0, Number(raw.freshness_expectation_minutes || 0)),
    completeness_threshold_pct: Math.max(0, Math.min(100, Number(raw.completeness_threshold_pct || 0)))
  };
}

function createBaseScenario(overrides = {}, index = 0, mart = {}) {
  const martCode = normalizeFieldName(mart.code || mart.name || 'mart');
  const name = asText(overrides.name || `Сценарий ${index + 1}`);
  const id = asText(overrides.id || `scenario_${index + 1}`);
  return {
    id,
    name,
    description: asText(overrides.description),
    enabled: overrides.enabled !== false,
    scenario_type: asText(overrides.scenario_type || 'default') || 'default',
    preview_state: normalizePreviewState(overrides.preview_state),
    publish_state: normalizePublishState(overrides.publish_state),
    sources: asArray(overrides.sources).map(normalizeSource),
    source_contracts: asArray(overrides.source_contracts).map(normalizeSourceContract),
    relations: asArray(overrides.relations).map(normalizeRelation),
    transformations: {
      joins: asArray(overrides?.transformations?.joins).map(normalizeRelation),
      filters: asArray(overrides?.transformations?.filters).map(normalizeFilter),
      derived_fields: asArray(overrides?.transformations?.derived_fields).map(normalizeDerivedField),
      metrics: asArray(overrides?.transformations?.metrics).map(normalizeAggregation),
      dimensions: asArray(overrides?.transformations?.dimensions).map(normalizeGrouping),
      windows: asArray(overrides?.transformations?.windows).map((item) => ({ ...asObject(item) })),
      groupings: asArray(overrides?.transformations?.groupings).map(normalizeGrouping),
      aggregations: asArray(overrides?.transformations?.aggregations).map(normalizeAggregation)
    },
    model_enrichment: {
      mathematical_blocks: asArray(overrides?.model_enrichment?.mathematical_blocks || overrides?.model_enrichment?.math_blocks).map(normalizeModelBlock),
      forecasting_blocks: asArray(overrides?.model_enrichment?.forecasting_blocks || overrides?.model_enrichment?.forecast_blocks).map(normalizeModelBlock),
      inference_blocks: asArray(overrides?.model_enrichment?.inference_blocks).map(normalizeModelBlock)
    },
    materialization: normalizeMaterialization(overrides.materialization, `${martCode}_${slugify(name, `scenario_${index + 1}`)}`),
    refresh_policy: normalizeRefreshPolicy(overrides.refresh_policy),
    quality: normalizeQuality(overrides.quality),
    consumers: asArray(overrides.consumers).map(normalizeConsumer)
  };
}

function createBaseMart(overrides = {}, index = 0) {
  const name = asText(overrides.name || `Витрина ${index + 1}`);
  const code = normalizeFieldName(overrides.code || name || `mart_${index + 1}`, `mart_${index + 1}`);
  const martId = asText(overrides.id || code || `mart_${index + 1}`);
  const scenariosInput = asArray(overrides.scenarios);
  const scenarios = scenariosInput.length
    ? scenariosInput.map((item, scenarioIndex) => createBaseScenario(item, scenarioIndex, { code, name }))
    : [createBaseScenario({}, 0, { code, name })];
  const activeScenarioId = asText(overrides.active_scenario_id || scenarios[0]?.id || 'scenario_1');
  return {
    id: martId,
    code,
    name,
    description: asText(overrides.description),
    group_name: asText(overrides.group_name),
    status: asText(overrides.status || 'draft') || 'draft',
    active_version: Math.max(1, Number(overrides.active_version || 1)),
    materialization_mode: asText(overrides.materialization_mode || scenarios[0]?.materialization?.mode || 'snapshot_table'),
    scenarios,
    active_scenario_id: scenarios.some((item) => item.id === activeScenarioId) ? activeScenarioId : scenarios[0].id
  };
}

function legacyToDesk(raw = {}) {
  const metadata = ensureSection(raw.metadata, raw);
  const martName = asText(raw.mart_name || metadata.name || 'Витрина');
  const martCode = normalizeFieldName(raw.mart_code || metadata.code || martName || 'mart_1', 'mart_1');
  const scenarioName = asText(raw.scenario_name || 'Основной');
  const scenario = createBaseScenario(
    {
      id: asText(raw.active_scenario_id || 'scenario_main'),
      name: scenarioName,
      description: asText(raw.scenario_description),
      enabled: raw.enabled !== false,
      scenario_type: asText(raw.scenario_type || 'default'),
      preview_state: raw.preview_state,
      publish_state: raw.publish_state,
      sources: raw.sources,
      source_contracts: raw.source_contracts,
      relations: raw.relations,
      transformations: raw.transformations,
      model_enrichment: raw.model_enrichment,
      materialization: raw.materialization,
      refresh_policy: raw.refresh_policy,
      quality: raw.quality,
      consumers: raw.consumers
    },
    0,
    { code: martCode, name: martName }
  );
  const mart = createBaseMart(
    {
      id: asText(raw.active_mart_id || 'mart_main'),
      code: martCode,
      name: martName,
      description: asText(raw.mart_description || metadata.description),
      group_name: asText(raw.group_name || metadata.group_name),
      status: asText(raw.mart_status || metadata.status || 'draft'),
      active_version: Math.max(1, Number(metadata.version || raw.version || 1)),
      scenarios: [scenario],
      active_scenario_id: scenario.id,
      materialization_mode: scenario.materialization.mode
    },
    0
  );
  return {
    metadata: {
      id: asText(metadata.id),
      code: normalizeFieldName(metadata.code || raw.code, metadata.code || raw.code),
      name: asText(metadata.name || raw.name || 'Рабочий стол витрин'),
      description: asText(metadata.description || raw.description),
      group_name: asText(metadata.group_name || raw.group_name),
      owner: asText(metadata.owner || raw.owner),
      tags: uniqueStrings(metadata.tags || raw.tags),
      status: asText(metadata.status || raw.status || 'draft') || 'draft',
      version: Math.max(1, Number(metadata.version || raw.version || 1)),
      published: Boolean(metadata.published ?? raw.published)
    },
    marts: [mart],
    active_mart_id: mart.id,
    active_scenario_id: mart.active_scenario_id
  };
}

export function createDataMartScenario(overrides = {}, index = 0, mart = {}) {
  return createBaseScenario(overrides, index, mart);
}

export function createDataMart(overrides = {}, index = 0) {
  return createBaseMart(overrides, index);
}

export function createEmptyGoldDefinition(overrides = {}) {
  const metadata = ensureSection(overrides.metadata, overrides);
  const name = asText(metadata.name || 'Рабочий стол витрин');
  const code = normalizeFieldName(metadata.code || name || `gold_${Date.now()}`, `gold_${Date.now()}`);
  const mart = createBaseMart(
    {
      id: 'mart_1',
      code: 'mart_1',
      name: 'Витрина 1',
      description: '',
      group_name: asText(metadata.group_name),
      scenarios: [
        createBaseScenario(
          {
            id: 'scenario_1',
            name: 'Основной',
            description: '',
            enabled: true
          },
          0,
          { code: 'mart_1', name: 'Витрина 1' }
        )
      ],
      active_scenario_id: 'scenario_1'
    },
    0
  );
  return normalizeGoldDefinition({
    metadata: {
      id: asText(metadata.id),
      code,
      name,
      description: asText(metadata.description),
      group_name: asText(metadata.group_name),
      owner: asText(metadata.owner),
      tags: uniqueStrings(metadata.tags),
      status: asText(metadata.status || 'draft') || 'draft',
      version: Math.max(1, Number(metadata.version || 1)),
      published: Boolean(metadata.published)
    },
    marts: [mart],
    active_mart_id: mart.id,
    active_scenario_id: mart.active_scenario_id
  });
}

export function getActiveDataMart(definition) {
  const normalized = normalizeGoldDefinition(definition);
  return normalized.marts.find((item) => item.id === normalized.active_mart_id) || normalized.marts[0] || null;
}

export function getScenarioById(definition, martId, scenarioId) {
  const normalized = normalizeGoldDefinition(definition);
  const mart = normalized.marts.find((item) => item.id === martId);
  if (!mart) return null;
  return mart.scenarios.find((item) => item.id === scenarioId) || null;
}

export function getActiveScenario(definition) {
  const normalized = normalizeGoldDefinition(definition);
  const mart = getActiveDataMart(normalized);
  if (!mart) return null;
  return mart.scenarios.find((item) => item.id === mart.active_scenario_id) || mart.scenarios[0] || null;
}

export function setActiveDataMart(definition, martId) {
  const normalized = normalizeGoldDefinition(definition);
  const next = cloneJson(normalized);
  const mart = next.marts.find((item) => item.id === martId) || next.marts[0];
  next.active_mart_id = mart?.id || '';
  next.active_scenario_id = mart?.active_scenario_id || mart?.scenarios?.[0]?.id || '';
  return normalizeGoldDefinition(next);
}

export function setActiveScenario(definition, martId, scenarioId) {
  const normalized = normalizeGoldDefinition(definition);
  const next = cloneJson(normalized);
  const mart = next.marts.find((item) => item.id === martId) || next.marts[0];
  if (!mart) return normalizeGoldDefinition(next);
  const scenario = mart.scenarios.find((item) => item.id === scenarioId) || mart.scenarios[0];
  mart.active_scenario_id = scenario?.id || mart.active_scenario_id;
  next.active_mart_id = mart.id;
  next.active_scenario_id = mart.active_scenario_id;
  return normalizeGoldDefinition(next);
}

export function addMartToGoldDefinition(definition, overrides = {}) {
  const normalized = normalizeGoldDefinition(definition);
  const next = cloneJson(normalized);
  const mart = createDataMart(overrides, next.marts.length);
  next.marts.push(mart);
  next.active_mart_id = mart.id;
  next.active_scenario_id = mart.active_scenario_id;
  return normalizeGoldDefinition(next);
}

export function copyMartInGoldDefinition(definition, martId) {
  const normalized = normalizeGoldDefinition(definition);
  const next = cloneJson(normalized);
  const source = next.marts.find((item) => item.id === martId);
  if (!source) return normalizeGoldDefinition(next);
  const copyIndex = next.marts.length;
  const copy = createDataMart({
    ...source,
    id: `${source.id}_copy_${copyIndex + 1}`,
    code: `${normalizeFieldName(source.code, source.id)}_copy_${copyIndex + 1}`,
    name: `${source.name} копия`,
    scenarios: source.scenarios.map((scenario, scenarioIndex) => ({
      ...scenario,
      id: `${scenario.id}_copy_${copyIndex + 1}_${scenarioIndex + 1}`,
      preview_state: { status: 'stale', stale_reason: 'copied' },
      publish_state: { status: 'draft' }
    }))
  });
  next.marts.push(copy);
  next.active_mart_id = copy.id;
  next.active_scenario_id = copy.active_scenario_id;
  return normalizeGoldDefinition(next);
}

export function removeMartFromGoldDefinition(definition, martId) {
  const normalized = normalizeGoldDefinition(definition);
  const next = cloneJson(normalized);
  if (next.marts.length <= 1) return normalizeGoldDefinition(next);
  next.marts = next.marts.filter((item) => item.id !== martId);
  const activeMart = next.marts.find((item) => item.id === next.active_mart_id) || next.marts[0];
  next.active_mart_id = activeMart?.id || '';
  next.active_scenario_id = activeMart?.active_scenario_id || activeMart?.scenarios?.[0]?.id || '';
  return normalizeGoldDefinition(next);
}

export function addScenarioToMart(definition, martId, overrides = {}) {
  const normalized = normalizeGoldDefinition(definition);
  const next = cloneJson(normalized);
  const mart = next.marts.find((item) => item.id === martId);
  if (!mart) return normalizeGoldDefinition(next);
  const scenario = createDataMartScenario(overrides, mart.scenarios.length, mart);
  mart.scenarios.push(scenario);
  mart.active_scenario_id = scenario.id;
  next.active_mart_id = mart.id;
  next.active_scenario_id = scenario.id;
  return normalizeGoldDefinition(next);
}

export function copyScenarioInMart(definition, martId, scenarioId) {
  const normalized = normalizeGoldDefinition(definition);
  const next = cloneJson(normalized);
  const mart = next.marts.find((item) => item.id === martId);
  if (!mart) return normalizeGoldDefinition(next);
  const source = mart.scenarios.find((item) => item.id === scenarioId);
  if (!source) return normalizeGoldDefinition(next);
  const copy = createDataMartScenario(
    {
      ...source,
      id: `${source.id}_copy_${mart.scenarios.length + 1}`,
      name: `${source.name} копия`,
      preview_state: { status: 'stale', stale_reason: 'copied' },
      publish_state: { status: 'draft' }
    },
    mart.scenarios.length,
    mart
  );
  mart.scenarios.push(copy);
  mart.active_scenario_id = copy.id;
  next.active_mart_id = mart.id;
  next.active_scenario_id = copy.id;
  return normalizeGoldDefinition(next);
}

export function removeScenarioFromMart(definition, martId, scenarioId) {
  const normalized = normalizeGoldDefinition(definition);
  const next = cloneJson(normalized);
  const mart = next.marts.find((item) => item.id === martId);
  if (!mart || mart.scenarios.length <= 1) return normalizeGoldDefinition(next);
  mart.scenarios = mart.scenarios.filter((item) => item.id !== scenarioId);
  const activeScenario = mart.scenarios.find((item) => item.id === mart.active_scenario_id) || mart.scenarios[0];
  mart.active_scenario_id = activeScenario?.id || '';
  next.active_mart_id = mart.id;
  next.active_scenario_id = mart.active_scenario_id;
  return normalizeGoldDefinition(next);
}

export function markActiveScenarioPreviewStale(definition, staleReason = 'definition_changed') {
  const normalized = normalizeGoldDefinition(definition);
  const next = cloneJson(normalized);
  const mart = next.marts.find((item) => item.id === next.active_mart_id);
  const scenario = mart?.scenarios?.find((item) => item.id === next.active_scenario_id);
  if (scenario) {
    scenario.preview_state = {
      ...scenario.preview_state,
      status: 'stale',
      stale_reason: asText(staleReason)
    };
  }
  return normalizeGoldDefinition(next);
}

export function normalizeGoldDefinition(raw = {}) {
  const looksLikeDesk =
    Array.isArray(raw?.marts) ||
    Boolean(raw?.active_mart_id) ||
    Boolean(raw?.metadata?.group_name) ||
    Boolean(raw?.active_scenario_id);
  const base = looksLikeDesk ? raw : legacyToDesk(raw);
  const metadata = ensureSection(base.metadata, base);
  const martsInput = asArray(base.marts);
  const marts = martsInput.length ? martsInput.map((item, index) => createBaseMart(item, index)) : [createBaseMart({}, 0)];
  const activeMart = marts.find((item) => item.id === asText(base.active_mart_id)) || marts[0];
  const activeScenario = activeMart.scenarios.find((item) => item.id === asText(base.active_scenario_id || activeMart.active_scenario_id)) || activeMart.scenarios[0];

  const normalized = {
    metadata: {
      id: asText(metadata.id),
      code: normalizeFieldName(metadata.code || raw.code, metadata.code || raw.code),
      name: asText(metadata.name || raw.name || 'Рабочий стол витрин'),
      description: asText(metadata.description || raw.description),
      group_name: asText(metadata.group_name || raw.group_name),
      owner: asText(metadata.owner || raw.owner),
      tags: uniqueStrings(metadata.tags || raw.tags),
      status: asText(metadata.status || raw.status || 'draft') || 'draft',
      version: Math.max(1, Number(metadata.version || raw.version || 1)),
      published: Boolean(metadata.published ?? raw.published)
    },
    marts,
    active_mart_id: activeMart.id,
    active_scenario_id: activeScenario.id,
    active_mart: activeMart,
    active_scenario: activeScenario,
    sources: activeScenario.sources,
    source_contracts: activeScenario.source_contracts,
    relations: activeScenario.relations.length ? activeScenario.relations : activeScenario.transformations.joins,
    transformations: activeScenario.transformations,
    model_enrichment: activeScenario.model_enrichment,
    materialization: activeScenario.materialization,
    refresh_policy: activeScenario.refresh_policy,
    quality: activeScenario.quality,
    consumers: activeScenario.consumers
  };

  if (!normalized.metadata.code) {
    normalized.metadata.code = normalizeFieldName(normalized.metadata.name, `gold_${Date.now()}`);
  }
  return normalized;
}

function fieldsForScenario(definition) {
  const normalized = normalizeGoldDefinition(definition);
  return normalized.active_scenario || getActiveScenario(normalized);
}

function pushUniqueField(output, seen, name, type = 'text', description = '') {
  const key = normalizeFieldName(name);
  if (!key || seen.has(key)) return;
  seen.add(key);
  output.push({ name: key, type: normalizeFieldType(type), description: asText(description) });
}

export function collectGoldOutputFields(definition) {
  const normalized = normalizeGoldDefinition(definition);
  const scenario = fieldsForScenario(normalized);
  const output = [];
  const seen = new Set();
  asArray(scenario?.sources).forEach((source) => {
    source.selected_fields.forEach((field) => pushUniqueField(output, seen, field.alias || field.field_name, field.field_type, field.description));
  });
  asArray(scenario?.transformations?.derived_fields).forEach((field) =>
    pushUniqueField(output, seen, field.field_name, field.field_type, field.description)
  );
  [...asArray(scenario?.transformations?.groupings), ...asArray(scenario?.transformations?.dimensions)].forEach((field) =>
    pushUniqueField(output, seen, field.alias || field.field_name, 'text')
  );
  [...asArray(scenario?.transformations?.aggregations), ...asArray(scenario?.transformations?.metrics)].forEach((field) =>
    pushUniqueField(output, seen, field.alias, field.field_type, field.description)
  );
  ['mathematical_blocks', 'forecasting_blocks', 'inference_blocks'].forEach((section) => {
    asArray(scenario?.model_enrichment?.[section]).forEach((block) => {
      block.output_fields.forEach((field) => pushUniqueField(output, seen, field.field_name, field.field_type, field.description));
    });
  });
  return output;
}

export function validateModelBindings(definition) {
  const normalized = normalizeGoldDefinition(definition);
  const scenario = fieldsForScenario(normalized);
  const errors = [];
  const warnings = [];
  const availableFields = new Set(
    collectGoldOutputFields({
      ...normalized,
      active_scenario: {
        ...scenario,
        model_enrichment: { mathematical_blocks: [], forecasting_blocks: [], inference_blocks: [] }
      },
      model_enrichment: { mathematical_blocks: [], forecasting_blocks: [], inference_blocks: [] }
    }).map((field) => field.name)
  );

  const allBlocks = [
    ...asArray(scenario?.model_enrichment?.mathematical_blocks),
    ...asArray(scenario?.model_enrichment?.forecasting_blocks),
    ...asArray(scenario?.model_enrichment?.inference_blocks)
  ];
  allBlocks.forEach((block) => {
    block.required_input_features.forEach((feature) => {
      const key = normalizeFieldName(feature);
      if (!availableFields.has(key)) {
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

function validateRelations(scenario, errors, warnings) {
  const sourceKeys = new Set(asArray(scenario?.sources).map((item) => item.source_key));
  const relations = asArray(scenario?.relations?.length ? scenario.relations : scenario?.transformations?.joins);
  if (asArray(scenario?.sources).length > 1 && !relations.length) {
    warnings.push({
      code: 'relations_missing',
      message: 'Для нескольких источников не задана явная модель объединения.'
    });
  }
  relations.forEach((relation) => {
    if (!sourceKeys.has(relation.left_source_key)) {
      errors.push({
        code: 'relation_left_source_missing',
        message: `Связь ${relation.relation_name} ссылается на несуществующий левый источник.`,
        context: { relation_key: relation.relation_key }
      });
    }
    if (!sourceKeys.has(relation.right_source_key)) {
      errors.push({
        code: 'relation_right_source_missing',
        message: `Связь ${relation.relation_name} ссылается на несуществующий правый источник.`,
        context: { relation_key: relation.relation_key }
      });
    }
    if ((relation.relation_type.endsWith('_join') || relation.relation_type === 'lookup_enrich') && !relation.join_keys.length) {
      errors.push({
        code: 'relation_join_keys_required',
        message: `Для связи ${relation.relation_name} нужно указать ключи объединения.`,
        context: { relation_key: relation.relation_key }
      });
    }
  });
}

export function validateGoldDefinition(definition) {
  const normalized = normalizeGoldDefinition(definition);
  const scenario = fieldsForScenario(normalized);
  const errors = [];
  const warnings = [];
  const pushError = (code, message, context = {}) => errors.push({ code, message, context });
  const pushWarning = (code, message, context = {}) => warnings.push({ code, message, context });

  if (!normalized.metadata.code) pushError('metadata_code_required', 'У рабочего стола витрин должен быть код.');
  if (!normalized.metadata.name) pushError('metadata_name_required', 'У рабочего стола витрин должно быть название.');
  if (!normalized.marts.length) pushError('marts_required', 'Нужна хотя бы одна витрина.');
  if (!scenario) pushError('scenario_required', 'Нужен активный сценарий витрины.');
  if (scenario && !scenario.enabled) pushWarning('scenario_disabled', 'Активный сценарий выключен.');
  if (scenario && !scenario.sources.length) pushError('sources_required', 'Нужно указать хотя бы один источник.');

  asArray(scenario?.sources).forEach((source, index) => {
    if (!source.source_key) pushError('source_key_required', 'Источник должен иметь source_key.', { index });
    if (!SOURCE_KINDS.has(source.source_kind)) pushError('source_kind_invalid', 'У источника недопустимый тип.', { index });
    if (!source.selected_fields.length) pushWarning('source_selected_fields_empty', 'Источник не публикует поля в витрину.', { index });
  });

  const primaryCount = asArray(scenario?.sources).filter((item) => item.source_role === 'primary' || item.source_role === 'fact').length;
  if (scenario?.sources?.length && primaryCount === 0) {
    pushWarning('primary_source_missing', 'Не выбран primary/fact источник. Будет использован первый источник.');
  }
  if (primaryCount > 1) {
    pushError('primary_source_ambiguous', 'В сценарии не может быть больше одного primary/fact источника.');
  }

  validateRelations(scenario, errors, warnings);

  const outputFields = collectGoldOutputFields(normalized);
  if (!outputFields.length) pushError('output_fields_required', 'Нужно определить хотя бы одно выходное поле витрины.');

  if (scenario?.materialization?.mode === 'snapshot_table') {
    if (!scenario.materialization.target_schema || !scenario.materialization.target_name) {
      pushError('materialization_target_required', 'Для snapshot table нужно указать схему и имя объекта.');
    }
  }

  if (scenario?.refresh_policy?.mode === 'interval' && !parseScheduleToMs(scenario.refresh_policy.schedule_value)) {
    pushError('refresh_schedule_required', 'Для interval refresh нужно указать интервал вида 15m / 1h / 1d.');
  }
  if (scenario?.refresh_policy?.mode === 'cron' && !scenario.refresh_policy.schedule_value) {
    pushError('refresh_schedule_required', 'Для cron refresh нужно указать cron-выражение.');
  }
  if (scenario?.refresh_policy?.mode === 'dependency' && !scenario.refresh_policy.dependency_sources.length) {
    pushError('refresh_dependency_required', 'Для dependency refresh нужно выбрать зависимые источники.');
  }

  const modelBinding = validateModelBindings(normalized);
  errors.push(...modelBinding.errors);
  warnings.push(...modelBinding.warnings);

  return { ok: errors.length === 0, errors, warnings, output_fields: outputFields };
}

export function checkSourceCompatibility(definition, { sourceCatalogByKey = {} } = {}) {
  const normalized = normalizeGoldDefinition(definition);
  const scenario = fieldsForScenario(normalized);
  const sources = asArray(scenario?.sources).map((source) => {
    const catalog = asObject(sourceCatalogByKey?.[source.source_key]);
    const contractFields = uniqueStrings(asArray(catalog.fields).map((field) => asText(field?.name || field?.field_name)));
    const missingRequired = source.required_fields.filter((field) => !contractFields.includes(field));
    const missingSelected = source.selected_fields
      .map((field) => field.field_name)
      .filter((field) => field && !contractFields.includes(field));
    return {
      source_key: source.source_key,
      source_name: source.source_name || asText(catalog.source_name),
      source_role: source.source_role,
      compatible: !missingRequired.length && !missingSelected.length && Boolean(contractFields.length),
      missing_required_fields: missingRequired,
      missing_selected_fields: missingSelected,
      available_fields: contractFields
    };
  });
  return { ok: sources.every((item) => item.compatible), sources };
}

export function buildGoldDependencyGraph(definition) {
  const normalized = normalizeGoldDefinition(definition);
  const mart = getActiveDataMart(normalized);
  const scenario = fieldsForScenario(normalized);
  const deskKey = `gold_desk:${normalized.metadata.code || normalized.metadata.id || 'draft'}`;
  const martKey = `gold_mart:${mart?.code || mart?.id || 'mart'}`;
  const scenarioKey = `gold_scenario:${scenario?.id || 'scenario'}`;
  const nodes = [
    { key: deskKey, node_type: 'gold_desk', label: normalized.metadata.name || normalized.metadata.code || 'Gold desk' },
    { key: martKey, node_type: 'gold_mart', label: mart?.name || mart?.code || 'Mart' },
    { key: scenarioKey, node_type: 'gold_scenario', label: scenario?.name || scenario?.id || 'Scenario' }
  ];
  const edges = [
    { from: deskKey, to: martKey, edge_type: 'contains' },
    { from: martKey, to: scenarioKey, edge_type: 'activates' }
  ];

  asArray(scenario?.sources).forEach((source) => {
    nodes.push({
      key: source.source_key,
      node_type: `${source.source_kind}_source`,
      label: source.source_name || source.source_key
    });
    edges.push({ from: source.source_key, to: scenarioKey, edge_type: 'feeds' });
  });

  asArray(scenario?.consumers).forEach((consumer) => {
    nodes.push({
      key: consumer.consumer_key,
      node_type: `${consumer.consumer_kind}_consumer`,
      label: consumer.name || consumer.consumer_key
    });
    edges.push({ from: scenarioKey, to: consumer.consumer_key, edge_type: 'consumed_by' });
  });

  return { gold_key: deskKey, desk_key: deskKey, mart_key: martKey, scenario_key: scenarioKey, nodes, edges };
}

export function analyzeGoldImpact(definition, { changedSources = [] } = {}) {
  const normalized = normalizeGoldDefinition(definition);
  const scenario = fieldsForScenario(normalized);
  const bySource = new Map(asArray(changedSources).map((item) => [asText(item?.source_key), asObject(item)]));
  const affected_sources = [];
  asArray(scenario?.sources).forEach((source) => {
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
  const affected_output_fields = affected_sources.length ? collectGoldOutputFields(normalized).map((field) => field.name) : [];
  const affected_consumers = affected_sources.length ? asArray(scenario?.consumers).map((consumer) => ({ ...consumer })) : [];
  return {
    has_impact: affected_sources.length > 0,
    affected_sources,
    affected_output_fields,
    affected_consumers
  };
}

export function planGoldRefresh(definition, { nowMs = Date.now(), lastRefreshAt = '', dependencyEvents = [] } = {}) {
  const normalized = normalizeGoldDefinition(definition);
  const scenario = fieldsForScenario(normalized);
  const mode = asText(scenario?.refresh_policy?.mode || 'manual');
  const lastTs = toTs(lastRefreshAt);
  if (mode === 'manual') {
    return { mode, needs_refresh: false, reason_code: 'manual_only', next_due_at: '' };
  }
  if (mode === 'interval') {
    const everyMs = parseScheduleToMs(scenario.refresh_policy.schedule_value);
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
      return asArray(scenario?.refresh_policy?.dependency_sources).includes(sourceKey) && toTs(event?.changed_at) > lastTs;
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
    reason_code: scenario?.refresh_policy?.schedule_value ? 'cron_waiting' : 'cron_not_evaluated',
    next_due_at: ''
  };
}

export function planGoldMaterialization(definition) {
  const normalized = normalizeGoldDefinition(definition);
  const scenario = fieldsForScenario(normalized);
  return {
    mode: scenario?.materialization?.mode || 'snapshot_table',
    target: {
      schema_name: scenario?.materialization?.target_schema || '',
      name: scenario?.materialization?.target_name || '',
      table_class: scenario?.materialization?.table_class || '',
      data_level: scenario?.materialization?.data_level || ''
    },
    output_fields: collectGoldOutputFields(normalized)
  };
}

export function resolveExternalSourceFreshness(definition, { externalSourceStatesByKey = {}, nowMs = Date.now() } = {}) {
  const normalized = normalizeGoldDefinition(definition);
  const scenario = fieldsForScenario(normalized);
  const sources = asArray(scenario?.sources)
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
  const scenario = fieldsForScenario(normalized);
  const counts = {};
  asArray(scenario?.consumers).forEach((consumer) => {
    counts[consumer.consumer_kind] = (counts[consumer.consumer_kind] || 0) + 1;
  });
  return { consumers: asArray(scenario?.consumers), counts };
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
