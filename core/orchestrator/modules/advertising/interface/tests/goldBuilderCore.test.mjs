import test from 'node:test';
import assert from 'node:assert/strict';

process.env.PGUSER = process.env.PGUSER || 'test';
process.env.PGPASSWORD = process.env.PGPASSWORD || 'test';
process.env.PGDATABASE = process.env.PGDATABASE || 'test';

const {
  buildGoldDataset,
  buildGoldPreviewModel,
  buildNamedSourceCatalog,
  buildProcessGoldSources
} = await import('../server/goldBuilderCore.mjs');

test('gold builder core: process catalog reuses published write outputs only', () => {
  const publishedDesks = [
    {
      desk_id: 24,
      desk_name: 'Desk 24',
      desk_version_id: 14,
      version_no: 14,
      graph_json: {
        nodes: [
          { id: 'start_1', type: 'tool', config: { name: 'Start', toolType: 'start_process', settings: {} } },
          {
            id: 'write_1',
            type: 'tool',
            config: {
              name: 'Write silver',
              toolType: 'db_write',
              settings: { targetSchema: 'silver_adv', targetTable: 'wb_ads_daily' }
            }
          }
        ],
        edges: [{ id: 'e1', from: 'start_1', from_port: 'out', to: 'write_1', to_port: 'in' }]
      }
    }
  ];

  const sources = buildProcessGoldSources({
    publishedDesks,
    contractRows: [
      {
        schema_name: 'silver_adv',
        table_name: 'wb_ads_daily',
        contract_name: 'silver_adv.wb_ads_daily',
        version: 3,
        columns: JSON.stringify([
          { field_name: 'event_date', field_type: 'date' },
          { field_name: 'spend', field_type: 'numeric' }
        ])
      }
    ],
    templateRows: [
      {
        template_name: 'Silver',
        schema_name: 'silver_adv',
        table_name: 'wb_ads_daily',
        data_level: 'silver',
        template_kind: 'data',
        table_class: 'silver_table'
      }
    ]
  });

  assert.equal(sources.length, 1);
  assert.equal(sources[0].source_kind, 'process');
  assert.equal(sources[0].schema_name, 'silver_adv');
  assert.equal(sources[0].fields.length, 2);
});

test('gold builder core: external/reference catalog preserves registry semantics', () => {
  const sources = buildNamedSourceCatalog({
    registryRows: [
      {
        source_key: 'external:pricing_feed',
        source_kind: 'external',
        source_name: 'Pricing feed',
        schema_name: 'ext_data',
        table_name: 'pricing_daily',
        expected_freshness_minutes: 60,
        required_fields: JSON.stringify(['sku', 'price']),
        is_active: true
      }
    ],
    contractRows: [
      {
        schema_name: 'ext_data',
        table_name: 'pricing_daily',
        contract_name: 'ext_data.pricing_daily',
        version: 2,
        columns: JSON.stringify([
          { field_name: 'sku', field_type: 'text' },
          { field_name: 'price', field_type: 'numeric' }
        ])
      }
    ]
  });

  assert.equal(sources.length, 1);
  assert.equal(sources[0].contract_version, 2);
  assert.deepEqual(sources[0].required_fields, ['sku', 'price']);
});

test('gold builder core: dataset preview keeps real rows and derived fields', () => {
  const definition = {
    code: 'roi_daily',
    name: 'ROI daily',
    sources: [
      {
        source_key: 'process:24:start_1:silver_adv.wb_ads_daily',
        source_kind: 'process',
        selected_fields: [
          { field_name: 'event_date', field_type: 'date' },
          { field_name: 'spend', alias: 'fact_spend', field_type: 'numeric' }
        ]
      }
    ],
    transformations: {
      derived_fields: [{ field_name: 'spend_bucket', formula: "row.fact_spend > 100 ? 'high' : 'low'" }]
    }
  };

  const preview = buildGoldDataset(definition, {
    sourceRowsByKey: {
      'process:24:start_1:silver_adv.wb_ads_daily': [
        { event_date: '2026-04-01', spend: 120 },
        { event_date: '2026-04-02', spend: 80 }
      ]
    }
  });

  assert.equal(preview.rows.length, 2);
  assert.deepEqual(preview.columns, ['event_date', 'fact_spend', 'spend_bucket']);
  assert.equal(preview.rows[0].spend_bucket, 'high');
  assert.equal(preview.rows[1].spend_bucket, 'low');
});

test('gold builder core: preview model aggregates metrics and keeps status data', () => {
  const definition = {
    code: 'roi_daily',
    name: 'ROI daily',
    published: true,
    sources: [
      {
        source_key: 'process:24:start_1:silver_adv.wb_ads_daily',
        source_kind: 'process',
        required_fields: ['event_date', 'spend'],
        selected_fields: [
          { field_name: 'event_date', field_type: 'date' },
          { field_name: 'spend', field_type: 'numeric' }
        ]
      }
    ],
    transformations: {
      groupings: [{ field_name: 'event_date', alias: 'event_date' }],
      aggregations: [{ alias: 'spend_total', field_name: 'spend', aggregator: 'sum', field_type: 'numeric' }]
    },
    quality: {
      required_output_fields: ['event_date', 'spend_total']
    }
  };

  const preview = buildGoldPreviewModel(definition, {
    sourceCatalogByKey: {
      'process:24:start_1:silver_adv.wb_ads_daily': {
        source_key: 'process:24:start_1:silver_adv.wb_ads_daily',
        source_name: 'WB daily',
        fields: [
          { name: 'event_date', type: 'date' },
          { name: 'spend', type: 'numeric' }
        ]
      }
    },
    sourceRowsByKey: {
      'process:24:start_1:silver_adv.wb_ads_daily': [
        { event_date: '2026-04-01', spend: 10 },
        { event_date: '2026-04-01', spend: 20 },
        { event_date: '2026-04-02', spend: 5 }
      ]
    },
    lastRefresh: { status: 'completed', finished_at: '2026-04-05T10:00:00.000Z' }
  });

  assert.equal(preview.validation.ok, true);
  assert.equal(preview.compatibility.ok, true);
  assert.equal(preview.dataset.rows.length, 2);
  assert.equal(preview.dataset.rows[0].spend_total, 30);
  assert.equal(preview.status.health_code, 'healthy');
  assert.equal(preview.quality_warnings.length, 0);
});

test('gold builder core: explicit relation model explains left join assembly', () => {
  const definition = {
    metadata: { code: 'gold_joined', name: 'Joined desk' },
    marts: [
      {
        id: 'mart_main',
        code: 'mart_main',
        name: 'Main mart',
        scenarios: [
          {
            id: 'scenario_main',
            name: 'Main scenario',
            sources: [
              {
                source_key: 'process:fact',
                source_kind: 'process',
                source_role: 'primary',
                selected_fields: [
                  { field_name: 'offer_id', field_type: 'text' },
                  { field_name: 'marketplace', field_type: 'text' },
                  { field_name: 'spend', field_type: 'numeric' }
                ]
              },
              {
                source_key: 'reference:costs',
                source_kind: 'reference',
                source_role: 'lookup',
                selected_fields: [
                  { field_name: 'offer_id', alias: 'cost_offer_id', field_type: 'text' },
                  { field_name: 'marketplace', alias: 'cost_marketplace', field_type: 'text' },
                  { field_name: 'cost', alias: 'product_cost', field_type: 'numeric' }
                ]
              }
            ],
            relations: [
              {
                relation_key: 'rel_costs',
                relation_name: 'Стоимость товара',
                relation_type: 'left_join',
                left_source_key: 'process:fact',
                right_source_key: 'reference:costs',
                join_keys: [
                  { left_field: 'offer_id', right_field: 'offer_id' },
                  { left_field: 'marketplace', right_field: 'marketplace' }
                ],
                cardinality: 'N:1',
                mismatch_policy: 'keep_primary',
                conflict_policy: 'rename_with_prefix',
                type_policy: 'strict'
              }
            ]
          }
        ]
      }
    ],
    active_mart_id: 'mart_main',
    active_scenario_id: 'scenario_main'
  };

  const preview = buildGoldPreviewModel(definition, {
    sourceCatalogByKey: {
      'process:fact': { source_key: 'process:fact', source_name: 'Fact', fields: [{ name: 'offer_id' }, { name: 'marketplace' }, { name: 'spend' }] },
      'reference:costs': { source_key: 'reference:costs', source_name: 'Costs', fields: [{ name: 'offer_id' }, { name: 'marketplace' }, { name: 'cost' }] }
    },
    sourceRowsByKey: {
      'process:fact': [
        { offer_id: 'sku-1', marketplace: 'wb', spend: 10 },
        { offer_id: 'sku-2', marketplace: 'wb', spend: 20 }
      ],
      'reference:costs': [{ offer_id: 'sku-1', marketplace: 'wb', cost: 4 }]
    }
  });

  assert.equal(preview.dataset.rows.length, 2);
  assert.equal(preview.dataset.rows[0].product_cost, 4);
  assert.equal(preview.dataset.rows[1].product_cost, null);
  assert.equal(preview.dataset.assembly.primary_source_key, 'process:fact');
  assert.equal(preview.dataset.assembly.row_count_before_merge, 2);
  assert.equal(preview.dataset.assembly.row_count_after_merge, 2);
  assert.equal(preview.dataset.assembly.relations[0].relation_type, 'left_join');
  assert.deepEqual(preview.dataset.assembly.relations[0].join_keys, ['offer_id = offer_id', 'marketplace = marketplace']);
});

test('gold builder core: N:1 relation keeps one row per left record even when right side has duplicates', () => {
  const definition = {
    metadata: { code: 'gold_cardinality', name: 'Cardinality desk' },
    marts: [
      {
        id: 'mart_main',
        code: 'mart_main',
        name: 'Main mart',
        scenarios: [
          {
            id: 'scenario_main',
            name: 'Main scenario',
            sources: [
              {
                source_key: 'process:fact',
                source_kind: 'process',
                source_role: 'primary',
                selected_fields: [
                  { field_name: 'id', field_type: 'text' },
                  { field_name: 'title', field_type: 'text' }
                ]
              },
              {
                source_key: 'external:lookup',
                source_kind: 'external',
                source_role: 'lookup',
                selected_fields: [
                  { field_name: 'id', alias: 'lookup_id', field_type: 'text' },
                  { field_name: 'title', alias: 'lookup_title', field_type: 'text' }
                ]
              }
            ],
            relations: [
              {
                relation_key: 'rel_lookup',
                relation_name: 'Lookup relation',
                relation_type: 'left_join',
                left_source_key: 'process:fact',
                right_source_key: 'external:lookup',
                join_keys: [{ left_field: 'id', right_field: 'id' }],
                cardinality: 'N:1',
                mismatch_policy: 'keep_primary',
                conflict_policy: 'rename_with_alias',
                type_policy: 'strict'
              }
            ]
          }
        ]
      }
    ],
    active_mart_id: 'mart_main',
    active_scenario_id: 'scenario_main'
  };

  const preview = buildGoldPreviewModel(definition, {
    sourceCatalogByKey: {
      'process:fact': { source_key: 'process:fact', source_name: 'Fact', fields: [{ name: 'id' }, { name: 'title' }] },
      'external:lookup': { source_key: 'external:lookup', source_name: 'Lookup', fields: [{ name: 'id' }, { name: 'title' }] }
    },
    sourceRowsByKey: {
      'process:fact': [{ id: '1', title: 'left-title' }],
      'external:lookup': [
        { id: '1', title: 'right-title-1' },
        { id: '1', title: 'right-title-2' }
      ]
    }
  });

  assert.equal(preview.dataset.rows.length, 1);
  assert.equal(preview.dataset.rows[0].lookup_title, 'right-title-1');
  assert.equal(preview.dataset.assembly.relations[0].matched_rows, 2);
  assert.equal(preview.dataset.assembly.relations[0].after_count, 1);
  assert.equal(preview.dataset.warnings.some((item) => item.code === 'relation_cardinality_warning'), true);
});
