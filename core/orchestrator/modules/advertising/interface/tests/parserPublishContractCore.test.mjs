import test from 'node:test';
import assert from 'node:assert/strict';

import {
  buildParserPublishReadCandidates,
  buildParserPublishRuntimeEntries,
  buildParserPublishDescriptorFields,
  buildParserPublishRows,
  serializeParserPublishRows
} from '../shared/parserPublishContractCore.js';

test('parser publish rows: deduplicates by canonical path and keeps complex field manual-only', () => {
  const incomingDescriptors = [
    {
      fields: [
        { path: 'items.id', alias: 'id', type: 'integer' },
        { path: 'items.payload', alias: 'payload', type: 'object' }
      ]
    }
  ];

  const state = buildParserPublishRows({
    incomingDescriptors,
    settings: {
      selectFields: 'items.id, items.payload, items.id',
      renameMap: { 'items.id': 'product_id' },
      defaultValues: { 'items.id': '0' }
    }
  });

  assert.equal(state.rows.length, 2);
  assert.deepEqual(state.duplicateSourcePaths, ['items.id']);
  assert.equal(state.rows[0].path, 'items.id');
  assert.equal(state.rows[0].alias, 'product_id');
  assert.equal(state.rows[0].status, 'changed');
  assert.equal(state.rows[0].isValidForPublish, true);
  assert.equal(state.rows[1].path, 'items.payload');
  assert.equal(state.rows[1].status, 'manual');
  assert.equal(state.rows[1].sourceSupported, false);
});

test('parser publish rows: marks stale settings as missing upstream instead of dropping them silently', () => {
  const state = buildParserPublishRows({
    incomingDescriptors: [{ fields: [{ path: 'items.id', alias: 'id', type: 'integer' }] }],
    settings: {
      selectFields: 'items.legacy',
      renameMap: { 'items.legacy': 'legacy_id' },
      typeMap: { 'items.legacy': 'integer' }
    }
  });

  assert.equal(state.rows.length, 1);
  assert.equal(state.rows[0].path, 'items.legacy');
  assert.equal(state.rows[0].status, 'missing upstream');
  assert.equal(state.rows[0].isValidForPublish, false);
});

test('parser publish serialization: stores overrides only and preserves canonical source path', () => {
  const incomingDescriptors = [
    {
      fields: [
        { path: 'items.id', alias: 'id', type: 'integer' },
        { path: 'items.name', alias: 'name', type: 'text' }
      ]
    }
  ];

  const serialized = serializeParserPublishRows(
    [
      { path: 'items.id', alias: 'id', type: 'integer', defaultValue: '' },
      { path: 'items.name', alias: 'title', type: 'text', defaultValue: 'unknown' }
    ],
    incomingDescriptors
  );

  assert.deepEqual(serialized.selectFields, ['items.id', 'items.name']);
  assert.deepEqual(serialized.renameMap, { 'items.name': 'title' });
  assert.deepEqual(serialized.typeMap, {});
  assert.deepEqual(serialized.defaultValues, { 'items.name': 'unknown' });
});

test('parser publish descriptor fields: builds publish contract only from valid rows', () => {
  const fields = buildParserPublishDescriptorFields({
    incomingDescriptors: [
      {
        fields: [
          { path: 'items.id', alias: 'id', type: 'integer' },
          { path: 'items.name', alias: 'name', type: 'text' }
        ]
      }
    ],
    settings: {
      selectFields: 'items.id, items.name',
      renameMap: { 'items.name': 'title' }
    }
  });

  assert.deepEqual(
    fields.map((field) => ({ name: field.name, alias: field.alias, type: field.type, path: field.path })),
    [
      { name: 'id', alias: 'id', type: 'integer', path: 'items.id' },
      { name: 'title', alias: 'title', type: 'text', path: 'items.name' }
    ]
  );
});

test('parser publish runtime entries: keep output aliases and add read candidates relative to working set', () => {
  const entries = buildParserPublishRuntimeEntries({
    settings: {
      selectFields: 'list.id, list.title',
      renameMap: { 'list.id': 'id', 'list.title': 'title' },
      defaultValues: { 'list.title': 'unknown' },
      typeMap: { 'list.id': 'integer' }
    },
    workingSetPath: '[].list'
  });

  assert.deepEqual(
    entries.map((entry) => ({
      sourcePath: entry.sourcePath,
      outputName: entry.outputName,
      readCandidates: entry.readCandidates,
      defaultValue: entry.defaultValue,
      explicitType: entry.explicitType
    })),
    [
      {
        sourcePath: 'list.id',
        outputName: 'id',
        readCandidates: ['list.id', 'id'],
        defaultValue: undefined,
        explicitType: 'integer'
      },
      {
        sourcePath: 'list.title',
        outputName: 'title',
        readCandidates: ['list.title', 'title'],
        defaultValue: 'unknown',
        explicitType: ''
      }
    ]
  );

  assert.deepEqual(buildParserPublishReadCandidates('items.meta.id', 'items.meta'), ['items.meta.id', 'id']);
  assert.deepEqual(buildParserPublishReadCandidates('list.advObjectType', 'list'), ['list.advObjectType', 'advObjectType', 'adv_object_type']);
});

test('parser publish runtime entries: default publish aliases normalize camelCase leafs to canonical snake_case', () => {
  const entries = buildParserPublishRuntimeEntries({
    settings: {
      selectFields: 'list.advObjectType, list.fromDate, list.PaymentType'
    },
    workingSetPath: 'list'
  });

  assert.deepEqual(
    entries.map((entry) => ({ sourcePath: entry.sourcePath, outputName: entry.outputName, readCandidates: entry.readCandidates })),
    [
      {
        sourcePath: 'list.advObjectType',
        outputName: 'adv_object_type',
        readCandidates: ['list.advObjectType', 'advObjectType', 'adv_object_type']
      },
      {
        sourcePath: 'list.fromDate',
        outputName: 'from_date',
        readCandidates: ['list.fromDate', 'fromDate', 'from_date']
      },
      {
        sourcePath: 'list.PaymentType',
        outputName: 'payment_type',
        readCandidates: ['list.PaymentType', 'PaymentType', 'payment_type']
      }
    ]
  );
});
