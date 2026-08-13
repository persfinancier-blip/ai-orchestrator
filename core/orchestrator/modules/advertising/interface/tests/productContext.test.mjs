import test from 'node:test';
import assert from 'node:assert/strict';
import { createSession, verifySession } from '../server/runtime/session.mjs';
import { readClientId } from '../server/clientContextRouter.mjs';

test('product session is signed and expires', () => {
  const secret = 'test-secret';
  const token = createSession(secret, 60);
  const session = verifySession(token, secret);
  assert.equal(session?.role, 'data_admin');
  assert.equal(Boolean(session?.sid), true);
  assert.equal(verifySession(token, 'wrong-secret'), null);
  assert.equal(verifySession(token, secret, Number(session.exp) + 1), null);
});

test('client context reads only positive integer cookie value', () => {
  assert.equal(readClientId({ headers: { cookie: 'ao_client_id=42' } }), 42);
  assert.equal(readClientId({ headers: { cookie: 'x=1; ao_client_id=7; y=2' } }), 7);
  assert.equal(readClientId({ headers: { cookie: 'ao_client_id=-3' } }), 0);
  assert.equal(readClientId({ headers: { cookie: 'ao_client_id=abc' } }), 0);
  assert.equal(readClientId({ headers: {} }), 0);
});
