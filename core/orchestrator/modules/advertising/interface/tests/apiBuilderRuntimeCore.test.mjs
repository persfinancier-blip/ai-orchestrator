import test from 'node:test';
import assert from 'node:assert/strict';
import {
  shouldRetryStatus,
  retryDelayMs,
  groupRowsByAliases,
  buildExecutionOrder,
  evaluateStopRuleValue
} from '../desk/tabs/apiBuilderRuntimeCore.js';

test('retry policy: retries only for 429/5xx', () => {
  assert.equal(shouldRetryStatus(429), true);
  assert.equal(shouldRetryStatus(500), true);
  assert.equal(shouldRetryStatus(503), true);
  assert.equal(shouldRetryStatus(400), false);
  assert.equal(shouldRetryStatus(401), false);
});

test('retry policy: backoff grows and is capped', () => {
  const d1 = retryDelayMs(1, 100, 500, () => 0);
  const d2 = retryDelayMs(2, 100, 500, () => 0);
  const d3 = retryDelayMs(3, 100, 500, () => 0);
  const d4 = retryDelayMs(10, 100, 500, () => 0);
  assert.equal(d1, 100);
  assert.equal(d2, 200);
  assert.equal(d3, 400);
  assert.equal(d4, 500);
});

test('grouping contract: explicit key keeps client/entity split', () => {
  const rows = [
    { TKN: 'A', nmID: 1 },
    { TKN: 'A', nmID: 2 },
    { TKN: 'B', nmID: 10 }
  ];
  const groups = groupRowsByAliases(rows, ['TKN']);
  assert.equal(groups.size, 2);
  const a = groups.get(JSON.stringify({ TKN: 'A' }));
  const b = groups.get(JSON.stringify({ TKN: 'B' }));
  assert.equal(a?.rows?.length, 2);
  assert.equal(b?.rows?.length, 1);
});

test('sync planner contract: entity_to_stop processes entity fully', () => {
  const order = buildExecutionOrder('sync', 'entity_to_stop', [3, 2]);
  assert.deepEqual(order, [
    { entity: 0, step: 1 },
    { entity: 0, step: 2 },
    { entity: 0, step: 3 },
    { entity: 1, step: 1 },
    { entity: 1, step: 2 }
  ]);
});

test('sync planner contract: by_wave interleaves entities by step', () => {
  const order = buildExecutionOrder('sync', 'by_wave', [3, 2]);
  assert.deepEqual(order, [
    { entity: 0, step: 1 },
    { entity: 1, step: 1 },
    { entity: 0, step: 2 },
    { entity: 1, step: 2 },
    { entity: 0, step: 3 }
  ]);
});

test('async contract: sequence inside each entity is always ordered', () => {
  const order = buildExecutionOrder('async', 'entity_to_stop', [4, 3, 2]);
  const byEntity = new Map();
  for (const item of order) {
    const seq = byEntity.get(item.entity) || [];
    seq.push(item.step);
    byEntity.set(item.entity, seq);
  }
  assert.deepEqual(byEntity.get(0), [1, 2, 3, 4]);
  assert.deepEqual(byEntity.get(1), [1, 2, 3]);
  assert.deepEqual(byEntity.get(2), [1, 2]);
});

test('stop-rules contract: numeric/text/empty operators', () => {
  assert.equal(evaluateStopRuleValue(16, '<=', 100), true);
  assert.equal(evaluateStopRuleValue(150, '<=', 100), false);
  assert.equal(evaluateStopRuleValue('cursor', 'contains', 'cur'), true);
  assert.equal(evaluateStopRuleValue('', 'empty', ''), true);
  assert.equal(evaluateStopRuleValue('abc', 'exists', ''), true);
});
