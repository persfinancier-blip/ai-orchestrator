import test from 'node:test';
import assert from 'node:assert/strict';

import { buildParserResultPreviewState } from '../desk/tabs/parserResultPreviewCore.js';

test('parser result preview state: rows mode uses fresh preview rows and keeps publish contract columns primary', () => {
  const state = buildParserResultPreviewState({
    previewData: {
      row_count: 2,
      columns: ['product_key', 'price', 'runtime_only'],
      sample_rows: [
        { product_key: 7, price: 12.5, runtime_only: 'x' },
        { product_key: 100, price: 0, runtime_only: 'y' }
      ],
      source_format: 'json'
    },
    publishedDescriptorFields: [
      { alias: 'product_key', type: 'numeric', path: 'meta.id' },
      { alias: 'price', type: 'numeric', path: 'price' },
      { alias: 'missing_in_sample', type: 'text', path: 'missing' }
    ],
    currentConfigSignature: 'cfg:1',
    previewLastAttemptSignature: 'cfg:1',
    previewLastSuccessSignature: 'cfg:1'
  });

  assert.equal(state.mode, 'rows');
  assert.equal(state.liveRowsCount, 2);
  assert.deepEqual(state.columns, ['product_key', 'price', 'missing_in_sample']);
  assert.equal(state.showStructure, false);
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
  assert.match(state.statusDescription, /ещё не запрашивались/i);
});

test('parser result preview state: shape_only mode marks stale preview as non-live', () => {
  const state = buildParserResultPreviewState({
    previewData: {
      row_count: 1,
      columns: ['old_name'],
      sample_rows: [{ old_name: 'legacy' }]
    },
    publishedDescriptorFields: [{ alias: 'new_name', type: 'text', path: 'product.name' }],
    currentConfigSignature: 'cfg:3:new',
    previewLastAttemptSignature: 'cfg:3:old',
    previewLastSuccessSignature: 'cfg:3:old'
  });

  assert.equal(state.mode, 'shape_only');
  assert.equal(state.isStalePreview, true);
  assert.deepEqual(state.rows, []);
  assert.deepEqual(state.structureColumns, ['new_name']);
});

test('parser result preview state: preview_failed mode keeps error honest and can still show structure', () => {
  const state = buildParserResultPreviewState({
    previewData: null,
    previewError: 'parser_preview_failed:test',
    publishedDescriptorFields: [{ alias: 'product_key', type: 'numeric', path: 'meta.id' }],
    currentConfigSignature: 'cfg:4',
    previewLastAttemptSignature: 'cfg:4',
    previewLastSuccessSignature: ''
  });

  assert.equal(state.mode, 'preview_failed');
  assert.equal(state.showStructure, true);
  assert.match(state.statusDescription, /parser_preview_failed:test/);
});

test('parser result preview state: no_preview_yet mode stays empty when there is no shape or live preview', () => {
  const state = buildParserResultPreviewState({
    previewData: null,
    publishedDescriptorFields: [],
    currentConfigSignature: 'cfg:5',
    previewLastAttemptSignature: '',
    previewLastSuccessSignature: ''
  });

  assert.equal(state.mode, 'no_preview_yet');
  assert.equal(state.showStructure, false);
  assert.deepEqual(state.rows, []);
  assert.deepEqual(state.columns, []);
});
