import test from 'node:test';
import assert from 'node:assert/strict';
import { maskClientSecrets, clientSecretsTestkit } from '../server/clientSecretsMiddleware.mjs';

test('client access secrets are masked in detail responses', () => {
  const result = maskClientSecrets({
    client: { id: 1 },
    accesses: [{ id: 4, token_value: 'token', password_value: 'pass', api_key: 'key', login_value: 'user' }]
  });
  assert.equal(result.accesses[0].token_value, clientSecretsTestkit.MASK);
  assert.equal(result.accesses[0].password_value, clientSecretsTestkit.MASK);
  assert.equal(result.accesses[0].api_key, clientSecretsTestkit.MASK);
  assert.equal(result.accesses[0].login_value, 'user');
});

test('nested detail and saved records are masked too', () => {
  const result = maskClientSecrets({ detail: { accesses: [{ token_value: 'secret' }] }, saved: { api_key: 'secret' } });
  assert.equal(result.detail.accesses[0].token_value, clientSecretsTestkit.MASK);
  assert.equal(result.saved.api_key, clientSecretsTestkit.MASK);
});
