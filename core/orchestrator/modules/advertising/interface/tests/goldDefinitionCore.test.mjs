import test from 'node:test';
import assert from 'node:assert/strict';

import {
  addMartToGoldDefinition,
  addScenarioToMart,
  analyzeGoldImpact,
  buildGoldDependencyGraph,
  checkSourceCompatibility,
  collectGoldOutputFields,
  copyMartInGoldDefinition,
  copyScenarioInMart,
  createEmptyGoldDefinition,
  getActiveDataMart,
  getActiveScenario,
  markActiveScenarioPreviewStale,
  normalizeGoldDefinition,
  planGoldMaterialization,
  planGoldRefresh,
  removeMartFromGoldDefinition,
  removeScenarioFromMart,
  resolveConsumerDependencies,
  resolveExternalSourceFreshness,
  resolveGoldStatus,
  setActiveDataMart,
  setActiveScenario,
  validateGoldDefinition,
  validateModelBindings
} from '../shared/goldDefinitionCore.mjs';

function sourceCatalogEntry(overrides = {}) {
  return {
    source_key: 'process:24:start_1:silver_adv.wb_ads_daily',
    source_kind: 'process',
    source_name: 'WB daily',
    process_code: 'wb_sync',
    desk_id: 24,
    start_node_id: 'start_1',
    schema_name: 'silver_adv',
    table_name: 'wb_ads_daily',
    contract_version: 3,
    fields: [
      { name: 'event_date', type: 'date' },
      { name: 'campaign_id', type: 'text' },
      { name: 'spend', type: 'numeric' },
      { name: 'clicks', type: 'bigint' }
    ],
    ...overrides
  };
}

test('gold core: empty definition now starts with desk, mart and scenario', () => {
  const definition = createEmptyGoldDefinition({ metadata: { code: 'roi_daily', name: 'ROI daily desk' } });

  assert.equal(definition.metadata.code, 'roi_daily');
  assert.equal(definition.marts.length, 1);
  assert.equal(definition.active_mart_id, definition.marts[0].id);
  assert.equal(definition.marts[0].scenarios.length, 1);
  assert.equal(definition.active_scenario_id, definition.marts[0].scenarios[0].id);
});

test('gold core: active mart and scenario selection works inside desk', () => {
  let definition = createEmptyGoldDefinition({ metadata: { code: 'roi_daily', name: 'ROI daily desk' } });
  definition = addMartToGoldDefinition(definition, { id: 'mart_regions', code: 'mart_regions', name: 'По регионам' });
  definition = addScenarioToMart(definition, 'mart_regions', { id: 'scenario_weekly', name: 'Weekly' });
  definition = setActiveDataMart(definition, 'mart_1');
  definition = setActiveScenario(definition, 'mart_1', 'scenario_1');

  assert.equal(getActiveDataMart(definition).id, 'mart_1');
  assert.equal(getActiveScenario(definition).id, 'scenario_1');

  definition = setActiveDataMart(definition, 'mart_regions');
  definition = setActiveScenario(definition, 'mart_regions', 'scenario_weekly');
  assert.equal(getActiveDataMart(definition).id, 'mart_regions');
  assert.equal(getActiveScenario(definition).id, 'scenario_weekly');
});

test('gold core: mart and scenario copy/remove keep state consistent', () => {
  let definition = createEmptyGoldDefinition({ metadata: { code: 'roi_daily', name: 'ROI daily desk' } });
  definition = copyMartInGoldDefinition(definition, 'mart_1');
  assert.equal(definition.marts.length, 2);
  definition = removeMartFromGoldDefinition(definition, 'mart_1');
  assert.equal(definition.marts.length, 1);

  definition = copyScenarioInMart(definition, definition.active_mart_id, definition.active_scenario_id);
  assert.equal(getActiveDataMart(definition).scenarios.length, 2);
  definition = removeScenarioFromMart(definition, definition.active_mart_id, getActiveDataMart(definition).scenarios[0].id);
  assert.equal(getActiveDataMart(definition).scenarios.length, 1);
});

test('gold core: relation change marks preview stale', () => {
  const definition = markActiveScenarioPreviewStale(
    normalizeGoldDefinition({
      metadata: { code: 'roi_daily', name: 'ROI daily desk' },
      marts: [
        {
          id: 'mart_1',
          code: 'mart_1',
          name: 'Main mart',
          scenarios: [{ id: 'scenario_1', name: 'Main', preview_state: { status: 'ready' } }]
        }
      ],
      active_mart_id: 'mart_1',
      active_scenario_id: 'scenario_1'
    }),
    'relation_changed'
  );

  assert.equal(getActiveScenario(definition).preview_state.status, 'stale');
  assert.equal(getActiveScenario(definition).preview_state.stale_reason, 'relation_changed');
});

test('gold core: validation flags missing fields, relation issues and unsupported configuration', () => {
  const definition = normalizeGoldDefinition({
    metadata: { code: '', name: '' },
    marts: [
      {
        id: 'mart_1',
        code: 'mart_1',
        name: 'Main mart',
        scenarios: [
          {
            id: 'scenario_1',
            name: 'Main',
            sources: [
              { source_key: 'a', source_kind: 'process', source_role: 'primary', selected_fields: [] },
              { source_key: 'b', source_kind: 'process', source_role: 'lookup', selected_fields: [] }
            ],
            relations: [{ relation_key: 'r1', relation_type: 'left_join', left_source_key: 'a', right_source_key: 'missing', join_keys: [] }],
            materialization: { mode: 'snapshot_table', target_schema: '', target_name: '' },
            refresh_policy: { mode: 'interval', schedule_value: '' }
          }
        ]
      }
    ],
    active_mart_id: 'mart_1',
    active_scenario_id: 'scenario_1'
  });
  const validation = validateGoldDefinition(definition);

  assert.equal(validation.ok, false);
  assert.equal(definition.metadata.name.length > 0, true);
  assert.equal(validation.errors.some((item) => item.code === 'relation_right_source_missing'), true);
  assert.equal(validation.errors.some((item) => item.code === 'relation_join_keys_required'), true);
  assert.equal(validation.errors.some((item) => item.code === 'refresh_schedule_required'), true);
});

test('gold core: compatibility checker finds missing required source fields', () => {
  const definition = normalizeGoldDefinition({
    code: 'roi_daily',
    name: 'ROI daily',
    sources: [
      {
        source_key: 'process:24:start_1:silver_adv.wb_ads_daily',
        source_kind: 'process',
        required_fields: ['event_date', 'orders'],
        optional_fields: ['spend'],
        selected_fields: [{ field_name: 'event_date' }, { field_name: 'spend' }]
      }
    ]
  });

  const result = checkSourceCompatibility(definition, {
    sourceCatalogByKey: {
      'process:24:start_1:silver_adv.wb_ads_daily': sourceCatalogEntry()
    }
  });

  assert.equal(result.ok, false);
  assert.equal(result.sources[0].compatible, false);
  assert.deepEqual(result.sources[0].missing_required_fields, ['orders']);
});

test('gold core: model binding validation rejects missing input features and duplicate outputs', () => {
  const definition = normalizeGoldDefinition({
    code: 'roi_daily',
    name: 'ROI daily',
    sources: [{ source_key: 'process:24:start_1:silver_adv.wb_ads_daily', source_kind: 'process', selected_fields: [{ field_name: 'event_date' }, { field_name: 'spend' }] }],
    model_enrichment: {
      inference_blocks: [
        {
          block_key: 'score_model',
          block_name: 'Score',
          model_version: 'v1',
          required_input_features: ['spend', 'orders'],
          output_fields: [{ field_name: 'score' }, { field_name: 'score' }]
        }
      ]
    }
  });

  const result = validateModelBindings(definition);
  assert.equal(result.ok, false);
  assert.equal(result.errors.some((item) => item.code === 'model_missing_input_feature'), true);
  assert.equal(result.errors.some((item) => item.code === 'model_duplicate_output_field'), true);
});

test('gold core: dependency graph and consumers are normalized for desk, mart and scenario', () => {
  const definition = normalizeGoldDefinition({
    metadata: { code: 'roi_daily', name: 'ROI daily desk' },
    marts: [
      {
        id: 'mart_roi',
        code: 'mart_roi',
        name: 'ROI daily',
        scenarios: [
          {
            id: 'scenario_main',
            name: 'Основной',
            sources: [{ source_key: 'process:24:start_1:silver_adv.wb_ads_daily', source_kind: 'process', source_name: 'WB daily', selected_fields: [{ field_name: 'event_date' }] }],
            consumers: [
              { consumer_key: 'dashboard:roi_main', consumer_kind: 'dashboard', name: 'ROI dashboard' },
              { consumer_key: 'action:rebid', consumer_kind: 'action_rule', name: 'Rebid action' }
            ]
          }
        ]
      }
    ],
    active_mart_id: 'mart_roi',
    active_scenario_id: 'scenario_main'
  });

  const graph = buildGoldDependencyGraph(definition);
  const consumers = resolveConsumerDependencies(definition);

  assert.equal(graph.nodes.some((node) => node.node_type === 'gold_desk' && node.key === 'gold_desk:roi_daily'), true);
  assert.equal(graph.edges.some((edge) => edge.from === 'process:24:start_1:silver_adv.wb_ads_daily' && edge.to === 'gold_scenario:scenario_main'), true);
  assert.equal(graph.edges.some((edge) => edge.from === 'gold_scenario:scenario_main' && edge.to === 'dashboard:roi_main'), true);
  assert.equal(consumers.counts.dashboard, 1);
  assert.equal(consumers.counts.action_rule, 1);
});

test('gold core: impact analysis reports incompatible sources and affected consumers', () => {
  const definition = normalizeGoldDefinition({
    code: 'roi_daily',
    name: 'ROI daily',
    sources: [
      {
        source_key: 'process:24:start_1:silver_adv.wb_ads_daily',
        source_kind: 'process',
        required_fields: ['event_date', 'spend'],
        selected_fields: [{ field_name: 'event_date' }, { field_name: 'spend' }]
      }
    ],
    consumers: [{ consumer_key: 'dashboard:roi_main', consumer_kind: 'dashboard', name: 'ROI dashboard' }]
  });

  const impact = analyzeGoldImpact(definition, {
    changedSources: [{ source_key: 'process:24:start_1:silver_adv.wb_ads_daily', removed_fields: ['spend'] }]
  });

  assert.equal(impact.has_impact, true);
  assert.equal(impact.affected_sources[0].severity, 'high');
  assert.equal(impact.affected_consumers[0].consumer_key, 'dashboard:roi_main');
  assert.equal(impact.affected_output_fields.includes('event_date'), true);
});

test('gold core: refresh planner resolves interval and dependency triggers', () => {
  const intervalDefinition = normalizeGoldDefinition({
    code: 'roi_daily',
    name: 'ROI daily',
    sources: [{ source_key: 'process:24:start_1:silver_adv.wb_ads_daily', source_kind: 'process' }],
    refresh_policy: { mode: 'interval', schedule_value: '15m' }
  });
  const intervalPlan = planGoldRefresh(intervalDefinition, {
    nowMs: Date.parse('2026-04-05T10:30:00.000Z'),
    lastRefreshAt: '2026-04-05T10:00:00.000Z'
  });
  assert.equal(intervalPlan.needs_refresh, true);
  assert.equal(intervalPlan.reason_code, 'interval_due');

  const dependencyDefinition = normalizeGoldDefinition({
    code: 'roi_daily',
    name: 'ROI daily',
    sources: [{ source_key: 'process:24:start_1:silver_adv.wb_ads_daily', source_kind: 'process' }],
    refresh_policy: { mode: 'dependency', dependency_sources: ['process:24:start_1:silver_adv.wb_ads_daily'] }
  });
  const dependencyPlan = planGoldRefresh(dependencyDefinition, {
    dependencyEvents: [{ source_key: 'process:24:start_1:silver_adv.wb_ads_daily', changed_at: '2026-04-05T10:20:00.000Z' }],
    lastRefreshAt: '2026-04-05T10:00:00.000Z'
  });
  assert.equal(dependencyPlan.needs_refresh, true);
  assert.equal(dependencyPlan.reason_code, 'dependency_changed');
});

test('gold core: materialization planner keeps selected scenario target metadata', () => {
  const definition = normalizeGoldDefinition({
    code: 'roi_daily',
    name: 'ROI daily',
    marts: [
      {
        id: 'mart_main',
        code: 'mart_main',
        name: 'Main',
        scenarios: [
          {
            id: 'scenario_mv',
            name: 'MV',
            materialization: {
              mode: 'materialized_view',
              target_schema: 'gold_adv',
              target_name: 'roi_daily_mv',
              table_class: 'gold_showcase'
            }
          }
        ]
      }
    ],
    active_mart_id: 'mart_main',
    active_scenario_id: 'scenario_mv'
  });
  const plan = planGoldMaterialization(definition);
  assert.equal(plan.mode, 'materialized_view');
  assert.equal(plan.target.schema_name, 'gold_adv');
  assert.equal(plan.target.name, 'roi_daily_mv');
});

test('gold core: external freshness and status resolve to stale/incompatible correctly', () => {
  const definition = normalizeGoldDefinition({
    code: 'roi_daily',
    name: 'ROI daily',
    published: true,
    version: 3,
    sources: [{ source_key: 'external:pricing_feed', source_kind: 'external', source_name: 'Pricing feed', freshness_expectation_minutes: 30 }]
  });
  const freshness = resolveExternalSourceFreshness(definition, {
    externalSourceStatesByKey: {
      'external:pricing_feed': { last_seen_at: '2026-04-05T09:00:00.000Z', schema_version: 'v2' }
    },
    nowMs: Date.parse('2026-04-05T10:00:00.000Z')
  });
  const status = resolveGoldStatus({
    definition,
    publishedDefinition: definition,
    lastRefresh: { status: 'completed', finished_at: '2026-04-05T09:15:00.000Z', row_count: 120 },
    compatibility: { ok: false, sources: [{ source_key: 'external:pricing_feed', compatible: false, missing_required_fields: ['price'] }] },
    sourceFreshness: freshness
  });

  assert.equal(freshness.summary.stale_count, 1);
  assert.equal(status.health_code, 'incompatible');
  assert.equal(status.machine_statuses.includes('stale'), true);
  assert.equal(status.machine_statuses.includes('incompatible'), true);
});
