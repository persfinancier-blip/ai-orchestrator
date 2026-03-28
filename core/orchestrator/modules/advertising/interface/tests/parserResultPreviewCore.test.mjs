import test from 'node:test';
import assert from 'node:assert/strict';

import {
  buildParserFlowPreviewState,
  buildParserPreviewDataFromRuntimeValue,
  buildParserPreviewDataFromRuntimeStep,
  buildParserDraftPreviewUxState,
  buildParserResultPreviewState,
  buildParserRuntimeResultState
} from '../desk/tabs/parserResultPreviewCore.js';

test('parser preview data from runtime value: canonical input envelope becomes preview payload', () => {
  const preview = buildParserPreviewDataFromRuntimeValue(
    {
      contract_version: 'node_io_v1',
      row_count: 1,
      rows: [{ id: 101, title: 'Campaign A', state: 'active' }],
      meta: {
        source_type: 'api_request',
        source_ref: 'upstream_api',
        source_format: 'json'
      }
    },
    { previewLimit: 20 }
  );

  assert.ok(preview);
  assert.equal(preview.row_count, 1);
  assert.deepEqual(preview.columns, ['id', 'title', 'state']);
  assert.deepEqual(preview.sample_rows[0], { id: 101, title: 'Campaign A', state: 'active' });
  assert.equal(preview.source_type, 'api_request');
  assert.equal(preview.source_ref, 'upstream_api');
  assert.equal(preview.source_format, 'json');
});

test('parser preview data from runtime step: output envelope becomes draft preview payload', () => {
  const preview = buildParserPreviewDataFromRuntimeStep(
    {
      output_json: {
        contract_version: 'node_io_v1',
        row_count: 2,
        rows: [
          { adv_id: 7, adv_title: 'Alpha' },
          { adv_id: 9, adv_title: 'Beta' }
        ],
        meta: {
          source_type: 'table_parser',
          source_ref: 'workflow_preview',
          source_format: 'json',
          parser_batch: {
            offset: 0,
            batch_size: 20,
            returned_rows: 2,
            has_more: false,
            next_cursor: null
          }
        }
      },
      metrics_json: { rows: 2 }
    },
    { previewLimit: 20 }
  );

  assert.ok(preview);
  assert.equal(preview.row_count, 2);
  assert.deepEqual(preview.columns, ['adv_id', 'adv_title']);
  assert.deepEqual(preview.sample_rows[0], { adv_id: 7, adv_title: 'Alpha' });
  assert.equal(preview.source_type, 'table_parser');
  assert.equal(preview.source_ref, 'workflow_preview');
  assert.equal(preview.source_format, 'json');
  assert.equal(preview.batch.returned_rows, 2);
});

test('parser flow preview state: input view uses canonical input rows from the same preview run', () => {
  const state = buildParserFlowPreviewState({
    viewKind: 'input',
    previewData: {
      row_count: 1,
      columns: ['id', 'title', 'state'],
      sample_rows: [{ id: 101, title: 'Campaign A', state: 'active' }],
      source_format: 'json'
    },
    publishedDescriptorFields: [
      { alias: 'id', type: 'numeric', path: 'id' },
      { alias: 'title', type: 'text', path: 'title' },
      { alias: 'state', type: 'text', path: 'state' }
    ],
    currentConfigSignature: 'cfg:input:1',
    previewLastAttemptSignature: 'cfg:input:1',
    previewLastSuccessSignature: 'cfg:input:1',
    currentInputSource: {
      key: 'server_preview_run',
      label: 'РСЃС‚РѕС‡РЅРёРє РІС…РѕРґР° РґР»СЏ preview: server preview-run',
      description: 'РћРґРёРЅ Рё С‚РѕС‚ Р¶Рµ server preview-run РґР»СЏ input/output.',
      available: true
    }
  });

  assert.equal(state.mode, 'rows');
  assert.deepEqual(state.columns, ['id', 'title', 'state']);
  assert.deepEqual(state.rows[0], { id: 101, title: 'Campaign A', state: 'active' });
  assert.match(state.statusTitle, /входн/i);
});

test('parser flow preview state: input mapping failure keeps raw rows visible and reports consume alignment diagnostics', () => {
  const state = buildParserFlowPreviewState({
    viewKind: 'input',
    previewData: {
      row_count: 1,
      columns: ['legacy_key'],
      sample_rows: [{ legacy_key: 'value' }],
      source_format: 'json'
    },
    publishedDescriptorFields: [{ alias: 'id', type: 'text', path: 'id' }],
    currentConfigSignature: 'cfg:input:map-fail',
    previewLastAttemptSignature: 'cfg:input:map-fail',
    previewLastSuccessSignature: 'cfg:input:map-fail',
    currentInputSource: {
      key: 'server_preview_run',
      label: 'РСЃС‚РѕС‡РЅРёРє РІС…РѕРґР° РґР»СЏ preview: server preview-run',
      description: 'РћРґРёРЅ Рё С‚РѕС‚ Р¶Рµ server preview-run РґР»СЏ input/output.',
      available: true
    }
  });

  assert.equal(state.mode, 'rows');
  assert.deepEqual(state.columns, ['legacy_key']);
  assert.deepEqual(state.rows, [{ legacy_key: 'value' }]);
  assert.equal(state.alignmentTone, 'warn');
  assert.deepEqual(state.matchedColumns, []);
  assert.deepEqual(state.unmatchedRawColumns, ['legacy_key']);
  assert.deepEqual(state.missingContractColumns, ['id']);
});

test('parser result preview state: fresh preview rows stay visible as raw runtime rows with separate publish alignment', () => {
  const state = buildParserResultPreviewState({
    previewData: {
      row_count: 2,
      columns: ['meta.id', 'price'],
      sample_rows: [
        { meta: { id: 7 }, price: 12.5 },
        { meta: { id: 100 }, price: 0 }
      ],
      source_format: 'json'
    },
    publishedDescriptorFields: [
      { alias: 'product_key', type: 'numeric', path: 'meta.id' },
      { alias: 'price', type: 'numeric', path: 'price' }
    ],
    currentConfigSignature: 'cfg:1',
    previewLastAttemptSignature: 'cfg:1',
    previewLastSuccessSignature: 'cfg:1',
    currentInputSource: {
      key: 'upstream_sample',
      label: 'РСЃС‚РѕС‡РЅРёРє РІС…РѕРґР° РґР»СЏ preview: upstream sample',
      description: 'Р”Р»СЏ draft preview Р±СѓРґРµС‚ РёСЃРїРѕР»СЊР·РѕРІР°РЅ sample snapshot upstream.',
      available: true
    }
  });

  assert.equal(state.mode, 'rows');
  assert.equal(state.liveRowsCount, 2);
  assert.equal(state.preparedRowsCount, 2);
  assert.ok(state.columns.includes('meta.id'));
  assert.ok(state.columns.includes('price'));
  assert.equal(state.rows[0]['meta.id'], 7);
  assert.equal(state.rows[0].price, 12.5);
  assert.equal(state.debug.previewSuccess, true);
  assert.deepEqual(state.debug.columnsWithoutMappedValues, []);
  assert.deepEqual(state.matchedColumns, ['product_key', 'price']);
});
test('parser result preview state: no_preview_yet mode exists before preview run and can still show structure', () => {
  const state = buildParserResultPreviewState({
    previewData: null,
    publishedDescriptorFields: [
      { alias: 'product_key', type: 'numeric', path: 'meta.id' },
      { alias: 'product_name', type: 'text', path: 'product.name' }
    ],
    currentConfigSignature: 'cfg:2',
    previewLastAttemptSignature: '',
    previewLastSuccessSignature: ''
  });

  assert.equal(state.mode, 'no_preview_yet');
  assert.equal(state.showStructure, true);
  assert.deepEqual(
    state.structureFields.map((field) => field.name),
    ['product_key', 'product_name']
  );
  assert.match(state.statusDescription, /ещё не запрашивались|не запрашивались/i);
});

test('parser result preview state: stale preview keeps last rows visible and falls back to last preview columns when current publish alias no longer maps', () => {
  const state = buildParserResultPreviewState({
    previewData: {
      row_count: 1,
      columns: ['old_name'],
      sample_rows: [{ old_name: 'legacy' }]
    },
    publishedDescriptorFields: [{ alias: 'new_name', type: 'text', path: 'product.name' }],
    currentConfigSignature: 'cfg:3:new',
    previewLastAttemptSignature: 'cfg:3:old',
    previewLastSuccessSignature: 'cfg:3:old',
    lastResolvedInputSource: {
      key: 'upstream_sample',
      label: 'РСЃС‚РѕС‡РЅРёРє РІС…РѕРґР° РґР»СЏ preview: upstream sample',
      description: 'Р”Р»СЏ draft preview Р±СѓРґРµС‚ РёСЃРїРѕР»СЊР·РѕРІР°РЅ sample snapshot upstream.',
      available: true
    }
  });

  assert.equal(state.mode, 'rows');
  assert.equal(state.isStalePreview, true);
  assert.deepEqual(state.columns, ['old_name']);
  assert.deepEqual(state.rows, [{ old_name: 'legacy' }]);
  assert.deepEqual(state.debug.effectivePublishColumns, ['new_name']);
});

test('parser result preview state: raw rows stay visible even when publish alignment is missing', () => {
  const state = buildParserResultPreviewState({
    previewData: {
      row_count: 1,
      columns: ['old_name'],
      sample_rows: [{ old_name: 'legacy' }]
    },
    publishedDescriptorFields: [{ alias: 'new_name', type: 'text', path: 'missing.path' }],
    currentConfigSignature: 'cfg:4',
    previewLastAttemptSignature: 'cfg:4',
    previewLastSuccessSignature: 'cfg:4',
    currentInputSource: {
      key: 'upstream_sample',
      label: 'РСЃС‚РѕС‡РЅРёРє РІС…РѕРґР° РґР»СЏ preview: upstream sample',
      description: 'Р”Р»СЏ draft preview Р±СѓРґРµС‚ РёСЃРїРѕР»СЊР·РѕРІР°РЅ sample snapshot upstream.',
      available: true
    }
  });

  assert.equal(state.mode, 'rows');
  assert.equal(state.showStructure, false);
  assert.equal(state.preparedRowsCount, 1);
  assert.equal(state.debug.previewResponseRowCount, 1);
  assert.deepEqual(state.columns, ['old_name']);
  assert.deepEqual(state.rows, [{ old_name: 'legacy' }]);
  assert.equal(state.alignmentTone, 'warn');
  assert.deepEqual(state.matchedColumns, []);
  assert.deepEqual(state.unmatchedRawColumns, ['old_name']);
  assert.deepEqual(state.missingContractColumns, ['new_name']);
});

test('parser flow preview state: output mapping failure keeps raw rows visible and reports publish alignment diagnostics', () => {
  const state = buildParserFlowPreviewState({
    viewKind: 'output',
    previewData: {
      row_count: 1,
      columns: ['legacy_key'],
      sample_rows: [{ legacy_key: 'value' }],
      source_format: 'json'
    },
    publishedDescriptorFields: [{ alias: 'adv_id', type: 'text', path: 'missing.path' }],
    currentConfigSignature: 'cfg:output:map-fail',
    previewLastAttemptSignature: 'cfg:output:map-fail',
    previewLastSuccessSignature: 'cfg:output:map-fail',
    currentInputSource: {
      key: 'server_preview_run',
      label: 'РСЃС‚РѕС‡РЅРёРє РІС…РѕРґР° РґР»СЏ preview: server preview-run',
      description: 'РћРґРёРЅ Рё С‚РѕС‚ Р¶Рµ server preview-run РґР»СЏ input/output.',
      available: true
    }
  });

  assert.equal(state.mode, 'rows');
  assert.deepEqual(state.columns, ['legacy_key']);
  assert.deepEqual(state.rows, [{ legacy_key: 'value' }]);
  assert.equal(state.alignmentTone, 'warn');
  assert.deepEqual(state.matchedColumns, []);
  assert.deepEqual(state.unmatchedRawColumns, ['legacy_key']);
  assert.deepEqual(state.missingContractColumns, ['adv_id']);
});

test('parser flow preview state: output view keeps raw nested runtime keys visible while alignment uses publish contract separately', () => {
  const state = buildParserFlowPreviewState({
    viewKind: 'output',
    previewData: {
      row_count: 1,
      columns: ['list.id', 'list.title'],
      sample_rows: [{ list: { id: 101, title: 'Campaign A' } }],
      source_format: 'json'
    },
    publishedDescriptorFields: [
      { alias: 'id', type: 'text', path: 'list.id' },
      { alias: 'title', type: 'text', path: 'list.title' }
    ],
    currentConfigSignature: 'cfg:output:canonical',
    previewLastAttemptSignature: 'cfg:output:canonical',
    previewLastSuccessSignature: 'cfg:output:canonical',
    currentInputSource: {
      key: 'server_preview_run',
      label: 'РСЃС‚РѕС‡РЅРёРє РІС…РѕРґР° РґР»СЏ preview: server preview-run',
      description: 'РћРґРёРЅ Рё С‚РѕС‚ Р¶Рµ server preview-run РґР»СЏ input/output.',
      available: true
    }
  });

  assert.equal(state.mode, 'rows');
  assert.ok(state.columns.includes('list.id'));
  assert.ok(state.columns.includes('list.title'));
  assert.equal(state.rows[0]['list.id'], 101);
  assert.equal(state.rows[0]['list.title'], 'Campaign A');
  assert.deepEqual(state.matchedColumns, ['id', 'title']);
  assert.deepEqual(state.unmatchedRawColumns, ['list']);
  assert.deepEqual(state.missingContractColumns, []);
});
test('parser result preview state: successful preview keeps the actually used source label even if current source is now missing', () => {
  const state = buildParserResultPreviewState({
    previewData: {
      row_count: 1,
      columns: ['product_key'],
      sample_rows: [{ product_key: 7 }]
    },
    publishedDescriptorFields: [{ alias: 'product_key', type: 'numeric', path: 'meta.id' }],
    currentConfigSignature: 'cfg:5',
    previewLastAttemptSignature: 'cfg:5',
    previewLastSuccessSignature: 'cfg:5',
    currentInputSource: {
      key: 'missing',
      label: 'РСЃС‚РѕС‡РЅРёРє РІС…РѕРґР° РґР»СЏ preview РЅРµ РЅР°Р№РґРµРЅ',
      description: 'РќРµС‚ current source.',
      available: false
    },
    lastResolvedInputSource: {
      key: 'last_runtime_input',
      label: 'РСЃС‚РѕС‡РЅРёРє РІС…РѕРґР° РґР»СЏ preview: last runtime input',
      description: 'Р”Р»СЏ draft preview Р±СѓРґРµС‚ РёСЃРїРѕР»СЊР·РѕРІР°РЅ РєР°РЅРѕРЅРёС‡РµСЃРєРёР№ input СЌС‚РѕР№ РЅРѕРґС‹ РёР· РїРѕСЃР»РµРґРЅРµРіРѕ server run.',
      available: true
    }
  });

  const ux = buildParserDraftPreviewUxState({
    previewState: state,
    inputSource: state.effectiveInputSource
  });

  assert.equal(state.mode, 'rows');
  assert.equal(state.effectiveInputSource.key, 'last_runtime_input');
  assert.equal(ux.sourceLabel, 'РСЃС‚РѕС‡РЅРёРє РІС…РѕРґР° РґР»СЏ preview: last runtime input');
});

test('parser runtime result state: rows mode uses last runtime output and keeps publish columns primary', () => {
  const state = buildParserRuntimeResultState({
    runtimeStep: {
      run_uid: 'run_rt_1',
      run_status: 'completed',
      status: 'ok',
      input_json: {
        contract_version: 'node_io_v1',
        rows: [{ id: 7, name: 'Alpha' }],
        row_count: 1
      },
      output_json: {
        contract_version: 'node_io_v1',
        rows: [{ product_key: 7, product_name: 'Alpha' }],
        row_count: 1
      },
      previous_step: {
        node_name: 'API',
        output_json: {
          contract_version: 'node_io_v1',
          rows: [{ id: 7, name: 'Alpha' }],
          row_count: 1
        }
      }
    },
    publishedDescriptorFields: [
      { alias: 'product_key', type: 'numeric', path: 'id' },
      { alias: 'product_name', type: 'text', path: 'name' }
    ]
  });

  assert.equal(state.mode, 'rows');
  assert.deepEqual(state.columns, ['product_key', 'product_name']);
  assert.equal(state.rowCount, 1);
  assert.equal(state.handoffMatchesUpstream, true);
});

test('parser runtime result state: shape_only mode stays honest when runtime step has no rows', () => {
  const state = buildParserRuntimeResultState({
    runtimeStep: {
      run_uid: 'run_rt_2',
      run_status: 'completed',
      status: 'ok',
      input_json: {
        contract_version: 'node_io_v1',
        rows: [{ id: 7 }],
        row_count: 1
      },
      output_json: {
        contract_version: 'node_io_v1',
        rows: [],
        row_count: 0
      }
    },
    publishedDescriptorFields: [{ alias: 'product_key', type: 'numeric', path: 'id' }]
  });

  assert.equal(state.mode, 'shape_only');
  assert.equal(state.showStructure, true);
  assert.deepEqual(state.structureColumns, ['product_key']);
});

test('parser draft preview ux state: exposes no-preview, stale, running, ready and failed states', () => {
  const source = {
    label: 'РСЃС‚РѕС‡РЅРёРє РІС…РѕРґР° РґР»СЏ preview: upstream sample',
    description: 'Preview Р±СѓРґРµС‚ СЃРѕР±СЂР°РЅ РїРѕ sample snapshot upstream.'
  };

  const noPreview = buildParserDraftPreviewUxState({
    previewState: { mode: 'no_preview_yet', isStalePreview: false },
    inputSource: source
  });
  assert.equal(noPreview.state, 'no_preview_yet');
  assert.equal(noPreview.actionLabel, 'Запустить предпросмотр результата');

  const stale = buildParserDraftPreviewUxState({
    previewState: { mode: 'rows', isStalePreview: true },
    inputSource: source
  });
  assert.equal(stale.state, 'stale_preview');
  assert.equal(stale.actionLabel, 'Обновить предпросмотр');

  const running = buildParserDraftPreviewUxState({
    previewState: { mode: 'rows', isStalePreview: true },
    previewLoading: true,
    inputSource: source
  });
  assert.equal(running.state, 'preview_running');
  assert.equal(running.actionDisabled, true);

  const ready = buildParserDraftPreviewUxState({
    previewState: { mode: 'rows', isStalePreview: false },
    inputSource: source
  });
  assert.equal(ready.state, 'preview_ready');
  assert.equal(ready.actionLabel, 'Обновить предпросмотр');

  const failed = buildParserDraftPreviewUxState({
    previewState: { mode: 'preview_failed', statusDescription: 'preview failed', isStalePreview: false },
    inputSource: source
  });
  assert.equal(failed.state, 'preview_failed');
  assert.equal(failed.actionLabel, 'Повторить предпросмотр');
});
