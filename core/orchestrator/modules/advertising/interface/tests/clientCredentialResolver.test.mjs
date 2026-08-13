import test from 'node:test';
import assert from 'node:assert/strict';
import { encryptClientSecret } from '../server/runtime/clientSecretCrypto.mjs';
import {
  normalizeProviderCode,
  resolveClientCredentialAliases
} from '../server/clientCredentialResolver.mjs';
import { maskRuntimeValues } from '../server/runtime/runtimeValueMask.mjs';

const KEY = 'resolver-test-key';
process.env.AO_CLIENT_SECRET_KEY = KEY;

function fakeClient(rows) {
  return {
    async query(_sql, params) {
      const [clientId, providerCode, cabinetId] = params;
      const filtered = rows.filter((row) =>
        Number(row.client_id) === Number(clientId) &&
        normalizeProviderCode(row.platform_code) === normalizeProviderCode(providerCode) &&
        row.is_active !== false &&
        (!cabinetId || String(row.cabinet_id || '') === String(cabinetId))
      );
      return { rows: filtered.slice(0, 3) };
    }
  };
}

function access(overrides = {}) {
  return {
    id: 1,
    client_id: 42,
    platform_code: 'ozon',
    platform_name: 'Ozon',
    cabinet_name: 'Main',
    cabinet_id: 'cab-1',
    auth_type: 'bearer',
    token_value: encryptClientSecret('ozon-secret-token', KEY),
    login_value: '',
    password_value: '',
    api_key: '',
    is_active: true,
    expires_at: null,
    ...overrides
  };
}

test('provider hostname normalizes to client platform code', () => {
  assert.equal(normalizeProviderCode('api-performance.ozon.ru'), 'ozon');
  assert.equal(normalizeProviderCode('WB'), 'wildberries');
  assert.equal(normalizeProviderCode('yandex_market'), 'yandex_market');
});

test('resolver decrypts one client access into ephemeral aliases', async () => {
  const result = await resolveClientCredentialAliases(fakeClient([access()]), {
    clientId: 42,
    providerCode: 'api-performance.ozon.ru'
  });
  assert.equal(result.bound, true);
  assert.equal(result.aliases.ozon_performance_token, 'ozon-secret-token');
  assert.equal(result.aliases.client_access_token, 'ozon-secret-token');
  assert.equal(result.aliases.client_cabinet_id, 'cab-1');
  assert.equal(result.access.provider_code, 'ozon');
});

test('resolver rejects ambiguous client cabinets without cabinet id', async () => {
  const client = fakeClient([access(), access({ id: 2, cabinet_id: 'cab-2' })]);
  await assert.rejects(
    () => resolveClientCredentialAliases(client, { clientId: 42, providerCode: 'ozon' }),
    /client_credential_ambiguous/
  );
});

test('explicit cabinet id resolves one access', async () => {
  const client = fakeClient([access(), access({ id: 2, cabinet_id: 'cab-2', token_value: encryptClientSecret('second-token', KEY) })]);
  const result = await resolveClientCredentialAliases(client, { clientId: 42, providerCode: 'ozon', cabinetId: 'cab-2' });
  assert.equal(result.aliases.ozon_performance_token, 'second-token');
  assert.equal(result.access.cabinet_id, 'cab-2');
});

test('runtime preview masking removes resolved values recursively', () => {
  const payload = {
    headers: { Authorization: 'Bearer ozon-secret-token' },
    url: 'https://example.test/?token=ozon-secret-token',
    resolved_parameters: { ozon_performance_token: 'ozon-secret-token', safe: 1 }
  };
  const masked = maskRuntimeValues(payload, ['ozon-secret-token']);
  assert.equal(JSON.stringify(masked).includes('ozon-secret-token'), false);
  assert.equal(masked.resolved_parameters.safe, 1);
});
