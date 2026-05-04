import test from 'node:test';
import assert from 'node:assert/strict';

import {
  applyApiExamplePatchToDraft,
  clearApiExampleInputDraft,
  parseApiExample
} from '../shared/apiExampleParserCore.mjs';

function baseDraft(patch = {}) {
  return {
    method: 'GET',
    baseUrl: '',
    path: '/',
    authMode: 'manual',
    authJson: '',
    headersJson: '',
    queryJson: '',
    bodyJson: '',
    paginationEnabled: false,
    paginationStrategy: 'none',
    paginationTarget: 'query',
    paginationDataPath: '',
    paginationPageParam: 'page',
    paginationStartPage: 1,
    paginationLimitParam: 'limit',
    paginationLimitValue: 100,
    responseLogEnabled: false,
    responseLogMode: 'standard',
    responseLogWriteRequestPayload: true,
    responseLogWriteResponsePayload: true,
    responseLogWritePaginationValues: true,
    responseLogOnlyErrors: false,
    outputParameters: [],
    exampleRequest: '',
    ...patch
  };
}

test('api example parser: curl/docs sample returns normalized preview and patch', () => {
  const input = `
Нужно получить список кампаний.
curl -X GET 'https://api.example.com/v1/campaigns?limit=100&page=1' \\
  -H 'Authorization: Bearer {{token}}' \\
  -H 'Accept: application/json'

Pagination: page param page, limit 100. Records path: data.items.
Rate limit: 120 requests per minute, retry on 429.
Включить API log debug request and response.
`;

  const result = parseApiExample(input);

  assert.equal(result.apply_patch.method, 'GET');
  assert.equal(result.apply_patch.baseUrl, 'https://api.example.com');
  assert.equal(result.apply_patch.path, '/v1/campaigns');
  assert.deepEqual(result.apply_patch.queryJson, { limit: '100', page: '1' });
  assert.deepEqual(result.apply_patch.authJson, { Authorization: 'Bearer {{token}}' });
  assert.equal(result.apply_patch.headersJson.Accept, 'application/json');
  assert.equal(result.apply_patch.paginationEnabled, true);
  assert.equal(result.apply_patch.paginationStrategy, 'page_number');
  assert.equal(result.apply_patch.paginationDataPath, 'data.items');
  assert.equal(result.apply_patch.responseLogEnabled, true);
  assert.equal(result.apply_patch.responseLogMode, 'debug');
  assert.ok(result.recommended_changes.length > 0);
  assert.ok(result.detected_fields.some((item) => item.key === 'rate_limit'));
});

test('api example parser: constructor and node modal use identical normalized result', () => {
  const input = `POST https://api.example.com/v2/reports?date=2026-05-01
Content-Type: application/json
Authorization: Bearer {{token}}

{"metrics":["orders"],"limit":50}`;

  const constructorResult = parseApiExample(input);
  const nodeModalResult = parseApiExample(input);

  assert.deepEqual(constructorResult, nodeModalResult);
});

test('api example apply: merge preserves manual auth, body and conflicting query values', () => {
  const result = parseApiExample(`
curl -X POST 'https://api.example.com/v1/items?limit=100&date=2026-05-01' \\
  -H 'Authorization: Bearer parsed' \\
  -H 'Accept: application/json' \\
  --data '{"parsed":true}'
`);
  const draft = baseDraft({
    authJson: '{"Authorization":"Bearer manual"}',
    queryJson: '{"limit":50}',
    bodyJson: '{"manual":true}'
  });

  const applied = applyApiExamplePatchToDraft(draft, result);
  const query = JSON.parse(applied.draft.queryJson);
  const auth = JSON.parse(applied.draft.authJson);
  const body = JSON.parse(applied.draft.bodyJson);

  assert.equal(applied.draft.method, 'POST');
  assert.equal(applied.draft.baseUrl, 'https://api.example.com');
  assert.equal(query.limit, 50);
  assert.equal(query.date, '2026-05-01');
  assert.equal(auth.Authorization, 'Bearer manual');
  assert.deepEqual(body, { manual: true });
  assert.ok(applied.warnings.some((item) => item.includes('Authorization')));
  assert.ok(applied.warnings.some((item) => item.includes('Body')));
  assert.ok(applied.warnings.some((item) => item.includes('limit')));
});

test('api example clear: clears only input field and keeps applied settings', () => {
  const draft = baseDraft({
    method: 'POST',
    baseUrl: 'https://api.example.com',
    path: '/v1/items',
    queryJson: '{"limit":100}',
    exampleRequest: 'curl https://api.example.com/v1/items'
  });

  const cleared = clearApiExampleInputDraft(draft);

  assert.equal(cleared.exampleRequest, '');
  assert.equal(cleared.method, 'POST');
  assert.equal(cleared.baseUrl, 'https://api.example.com');
  assert.equal(cleared.path, '/v1/items');
  assert.equal(cleared.queryJson, '{"limit":100}');
});

test('api example apply: produces fields that map to api template and desk node state', () => {
  const result = parseApiExample(`
GET https://api.example.com/v1/orders?limit=25&offset=0
Пагинация offset/limit, records path: data.orders.
`);
  const applied = applyApiExamplePatchToDraft(baseDraft(), result);
  const nodeRequest = {
    method: applied.draft.method,
    url: `${applied.draft.baseUrl}${applied.draft.path}`,
    authMode: applied.draft.authMode,
    headersText: applied.draft.headersJson || '{}',
    queryText: applied.draft.queryJson || '{}',
    bodyText: applied.draft.bodyJson || '{}'
  };
  const pagination = {
    enabled: applied.draft.paginationEnabled,
    mode: applied.draft.paginationStrategy === 'cursor_fields' ? 'cursor' : applied.draft.paginationStrategy,
    target: applied.draft.paginationTarget,
    dataPath: applied.draft.paginationDataPath,
    limitParam: applied.draft.paginationLimitParam,
    offsetParam: applied.draft.paginationOffsetParam
  };

  assert.deepEqual(nodeRequest, {
    method: 'GET',
    url: 'https://api.example.com/v1/orders',
    authMode: 'manual',
    headersText: '{}',
    queryText: '{\n  "limit": "25",\n  "offset": "0"\n}',
    bodyText: '{}'
  });
  assert.deepEqual(pagination, {
    enabled: true,
    mode: 'offset_limit',
    target: 'query',
    dataPath: 'data.orders',
    limitParam: 'limit',
    offsetParam: 'offset'
  });
});
