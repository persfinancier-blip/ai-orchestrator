import test from 'node:test';
import assert from 'node:assert/strict';
import { buildNormalizationPlan, monteCarloForecast, normalizeContainerKind, pearson, rankCorrelations } from '../server/productAnalyticsCore.mjs';

test('normalization detects time, entity and metrics', () => {
  const plan = buildNormalizationPlan([
    { name: 'date', type: 'date' },
    { name: 'sku', type: 'text' },
    { name: 'revenue', type: 'numeric' },
    { name: 'spend', type: 'numeric' }
  ], [{ date: '2026-08-01', sku: 'A', revenue: 100, spend: 20 }]);
  assert.equal(plan.roles.time, 1);
  assert.equal(plan.roles.entity, 1);
  assert.equal(plan.roles.metric, 2);
  assert.equal(plan.ready_for_analysis, true);
});

test('correlation ranks strongest metric pairs', () => {
  const rows = Array.from({ length: 8 }, (_, i) => ({ a: i, b: i * 2, c: i % 2 }));
  assert.ok(Math.abs(pearson(rows.map((r) => r.a), rows.map((r) => r.b)) - 1) < 1e-9);
  const ranked = rankCorrelations(rows, ['a', 'b', 'c']);
  assert.equal(ranked[0].x, 'a');
  assert.equal(ranked[0].y, 'b');
});

test('monte carlo forecast is deterministic with a seed', () => {
  const series = [10, 12, 13, 15, 16, 18, 19, 21];
  const a = monteCarloForecast(series, { horizon: 7, simulations: 500, seed: 42, target: 24 });
  const b = monteCarloForecast(series, { horizon: 7, simulations: 500, seed: 42, target: 24 });
  assert.deepEqual(a.final, b.final);
  assert.equal(a.forecast.length, 7);
  assert.ok(a.final.p90 >= a.final.median);
  assert.ok(a.final.median >= a.final.p10);
  assert.ok(a.final.target_probability >= 0 && a.final.target_probability <= 1);
});

test('container kind falls back to project', () => {
  assert.equal(normalizeContainerKind('client'), 'client');
  assert.equal(normalizeContainerKind('something'), 'project');
});
