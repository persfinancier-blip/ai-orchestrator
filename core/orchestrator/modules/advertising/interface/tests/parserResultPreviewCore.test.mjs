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
      label: 'Источник входа для preview: server preview-run',
      description: 'Один и тот же server preview-run для input/output.',
      available: true
    }
  });

  assert.equal(state.mode, 'rows');
  assert.deepEqual(state.columns, ['id', 'title', 'state']);
  assert.deepEqual(state.rows[0], { id: 101, title: 'Campaign A', state: 'active' });
  assert.match(state.statusTitle, /входн/i);
});

test('parser result preview state: fresh preview rows are normalized to publish aliases', () => {
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
      label: 'Источник входа для preview: upstream sample',
      description: 'Для draft preview будет использован sample snapshot upstream.',
      available: true
    }
  });

  assert.equal(state.mode, 'rows');
  assert.equal(state.liveRowsCount, 2);
  assert.equal(state.preparedRowsCount, 2);
  assert.deepEqual(state.columns, ['product_key', 'price']);
  assert.deepEqual(state.rows[0], { product_key: 7, price: 12.5 });
  assert.equal(state.debug.previewSuccess, true);
  assert.deepEqual(state.debug.columnsWithoutMappedValues, []);
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
  assert.match(state.statusDescription, /ещё не запускался|не запрашивались/i);
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
      label: 'Источник входа для preview: upstream sample',
      description: 'Для draft preview будет использован sample snapshot upstream.',
      available: true
    }
  });

  assert.equal(state.mode, 'rows');
  assert.equal(state.isStalePreview, true);
  assert.deepEqual(state.columns, ['old_name']);
  assert.deepEqual(state.rows, [{ old_name: 'legacy' }]);
  assert.deepEqual(state.debug.effectivePublishColumns, ['new_name']);
});

test('parser result preview state: fresh preview does not show success when rows cannot be mapped to publish columns', () => {
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
      label: 'Источник входа для preview: upstream sample',
      description: 'Для draft preview будет использован sample snapshot upstream.',
      available: true
    }
  });

  assert.equal(state.mode, 'preview_failed');
  assert.equal(state.showStructure, true);
  assert.equal(state.preparedRowsCount, 0);
  assert.equal(state.debug.previewResponseRowCount, 1);
  assert.match(state.statusDescription, /publish columns|подготовить/i);
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
      label: 'Источник входа для preview не найден',
      description: 'Нет current source.',
      available: false
    },
    lastResolvedInputSource: {
      key: 'last_runtime_input',
      label: 'Источник входа для preview: last runtime input',
      description: 'Для draft preview будет использован канонический input этой ноды из последнего server run.',
      available: true
    }
  });

  const ux = buildParserDraftPreviewUxState({
    previewState: state,
    inputSource: state.effectiveInputSource
  });

  assert.equal(state.mode, 'rows');
  assert.equal(state.effectiveInputSource.key, 'last_runtime_input');
  assert.equal(ux.sourceLabel, 'Источник входа для preview: last runtime input');
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
    label: 'Источник входа для preview: upstream sample',
    description: 'Preview будет собран по sample snapshot upstream.'
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
