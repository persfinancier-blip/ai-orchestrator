import test from 'node:test';
import assert from 'node:assert/strict';
import { integrationDraftFromAssistant } from '../product/integrationDraft.js';

test('assistant draft stays inactive and splits endpoint', () => {
  const result = integrationDraftFromAssistant({
    summary: 'Campaign list',
    apply_patch: { by_field: { method: 'GET', url: 'https://api.example.test/v1/campaigns?limit=10' } }
  });
  assert.equal(result.ok, true);
  assert.equal(result.payload.base_url, 'https://api.example.test');
  assert.equal(result.payload.path, '/v1/campaigns?limit=10');
  assert.equal(result.payload.is_active, false);
});

test('assistant draft rejects missing or unsupported endpoint', () => {
  assert.equal(integrationDraftFromAssistant({ apply_patch: { by_field: {} } }).reason, 'missing_endpoint');
  assert.equal(integrationDraftFromAssistant({ apply_patch: { by_field: { url: 'file:///tmp/x' } } }).reason, 'invalid_protocol');
});

test('assistant draft rejects unsupported method', () => {
  const result = integrationDraftFromAssistant({ apply_patch: { by_field: { method: 'TRACE', url: 'https://example.test/x' } } });
  assert.equal(result.ok, false);
  assert.equal(result.reason, 'invalid_method');
});

test('assistant draft strips credentials from headers query and body', () => {
  const result = integrationDraftFromAssistant({
    apply_patch: {
      by_field: {
        method: 'POST',
        url: 'https://example.test/v1/items',
        headersText: JSON.stringify({ Authorization: 'Bearer secret', 'Content-Type': 'application/json' }),
        queryText: JSON.stringify({ limit: 10, api_key: 'secret' }),
        bodyText: JSON.stringify({ name: 'safe', credentials: { client_secret: 'secret' } })
      }
    }
  });
  assert.equal(result.ok, true);
  assert.equal(result.payload.headers_json.Authorization, undefined);
  assert.equal(result.payload.headers_json['Content-Type'], 'application/json');
  assert.equal(result.payload.query_json.api_key, undefined);
  assert.equal(result.payload.query_json.limit, 10);
  assert.equal(result.payload.body_json.credentials.client_secret, undefined);
  assert.equal(result.payload.body_json.name, 'safe');
  assert.equal(result.warnings.length, 3);
});

test('assistant draft rejects credentials embedded in URL', () => {
  const result = integrationDraftFromAssistant({ apply_patch: { by_field: { method: 'GET', url: 'https://user:pass@example.test/v1/items' } } });
  assert.equal(result.ok, false);
  assert.equal(result.reason, 'embedded_credentials');
});
