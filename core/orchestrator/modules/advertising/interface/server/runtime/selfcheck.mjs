import assert from 'node:assert/strict';
import { buildIdempotencyKey, stableStringify } from './idempotency.mjs';
import { computeRetryDelayMs, isRetryableFailure } from './retry.mjs';
import { buildControlAuth } from './security.mjs';
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

const response = () => ({
  statusCode: 200,
  status(code) { this.statusCode = code; return this; },
  json() { return this; }
});
let authorized = false;
const req = { headers: {}, header: () => 'Bearer abc' };
buildControlAuth({ token: 'abc', allowInsecure: false })(req, response(), () => { authorized = true; });
assert.equal(authorized, true);
assert.equal(req.headers['x-ao-role'], 'data_admin');

const denied = response();
buildControlAuth({ token: '', allowInsecure: false })({ headers: {}, header: () => '' }, denied, () => {});
assert.equal(denied.statusCode, 503);

console.log('runtime self-check: ok');
