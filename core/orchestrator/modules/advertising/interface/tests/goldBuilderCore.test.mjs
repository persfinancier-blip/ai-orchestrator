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
