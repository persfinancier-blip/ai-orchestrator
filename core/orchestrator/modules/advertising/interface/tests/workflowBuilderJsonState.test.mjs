import test from 'node:test';
import assert from 'node:assert/strict';

import {
  normalizeJsonTextSetting,
  safeJsonArray
} from '../desk/data/workflowBuilderJsonState.js';

test('workflow builder json state: serialized JSON text is not double-stringified', () => {
  assert.equal(normalizeJsonTextSetting('[]', '[]'), '[]');
  assert.equal(normalizeJsonTextSetting('{}', '{}'), '{}');
  assert.equal(normalizeJsonTextSetting('{"a":1}', '{}'), '{\n  "a": 1\n}');
});

test('workflow builder json state: objects and arrays normalize to editable pretty JSON', () => {
  assert.equal(normalizeJsonTextSetting([{ field: 'id' }], '[]'), '[\n  {\n    "field": "id"\n  }\n]');
  assert.equal(
    normalizeJsonTextSetting({ Authorization: 'Bearer {{token}}' }, '{}'),
    '{\n  "Authorization": "Bearer {{token}}"\n}'
  );
});

test('workflow builder json state: safeJsonArray protects builder tabs from broken config shapes', () => {
  assert.deepEqual(safeJsonArray(['a', 'b']), ['a', 'b']);
  assert.deepEqual(safeJsonArray({ field: 'id' }), []);
  assert.deepEqual(safeJsonArray('[]'), []);
  assert.deepEqual(safeJsonArray(null), []);
});
