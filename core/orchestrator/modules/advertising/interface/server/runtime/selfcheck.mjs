import assert from 'node:assert/strict';
import { buildIdempotencyKey, stableStringify } from './idempotency.mjs';
import { computeRetryDelayMs, isRetryableFailure } from './retry.mjs';
import { RUNTIME_CAPABILITIES } from './capabilities.mjs';

const first = { payload: { b: 2, a: 1 }, tenant: 't1' };
const second = { tenant: 't1', payload: { a: 1, b: 2 } };
assert.equal(stableStringify(first), stableStringify(second));
assert.equal(buildIdempotencyKey('job', first), buildIdempotencyKey('job', second));
assert.equal(computeRetryDelayMs({ attempt: 1, baseMs: 100, jitter: 0 }), 100);
assert.equal(computeRetryDelayMs({ attempt: 4, baseMs: 100, maxMs: 500, jitter: 0 }), 500);
assert.equal(isRetryableFailure({ status: 429 }), true);
assert.equal(isRetryableFailure({ status: 400 }), false);
assert.deepEqual(RUNTIME_CAPABILITIES.external_runtime_dependencies, []);

console.log('runtime self-check: ok');
