import test from 'node:test';
import assert from 'node:assert/strict';

import { buildApiResponsePreviewState, extractApiBusinessPayload } from '../desk/tabs/apiResponseTreeCore.js';

test('api response tree: unwraps business payload from runtime wrapper', () => {
  const wrapper = {
    total_requests: 1,
    success: 1,
    failed: 0,
    responses: [
      {
        page: 1,
        status: 200,
        response: {
          list: [{ id: '23131134', title: 'Оплата за заказ', state: 'running' }],
          total: '375'
        }
      }
    ],
    payloads_only: [
      {
        list: [{ id: '23131134', title: 'Оплата за заказ', state: 'running' }],
        total: '375'
      }
    ]
  };

  const payload = extractApiBusinessPayload(wrapper);
  assert.deepEqual(payload, {
    list: [{ id: '23131134', title: 'Оплата за заказ', state: 'running' }],
    total: '375'
  });
});

test('api response tree: parses JSON preview from truncated wrapper when it is valid JSON', () => {
  const wrapper = {
    total_requests: 1,
    responses: [
      {
        response: {
          __truncated: true,
          preview: '{"list":[{"id":"1","title":"Test","state":"active"}],"total":"1"}',
          total_chars: 1200
        }
      }
    ]
  };

  const payload = extractApiBusinessPayload(wrapper);
  assert.deepEqual(payload, {
    list: [{ id: '1', title: 'Test', state: 'active' }],
    total: '1'
  });
});

test('api response tree: does not fallback to runtime wrapper when payload is unavailable', () => {
  const wrapper = {
    total_requests: 1,
    success: 1,
    failed: 0,
    responses: [
      {
        response: {
          __truncated: true,
          preview: '{"list":[{"id":"1"',
          total_chars: 1200
        }
      }
    ]
  };

  const payload = extractApiBusinessPayload(wrapper);
  assert.equal(payload, undefined);
});

test('api response tree: preview state shows business payload instead of wrapper metadata', () => {
  const wrapper = {
    total_requests: 1,
    success: 1,
    failed: 0,
    payloads_only: [
      {
        list: [{ id: '1', title: 'Test', state: 'active' }],
        total: '1'
      }
    ]
  };

  const preview = buildApiResponsePreviewState(wrapper, wrapper);
  assert.equal(preview.isJson, true);
  assert.deepEqual(preview.treePayload, {
    list: [{ id: '1', title: 'Test', state: 'active' }],
    total: '1'
  });
  assert.match(preview.text, /"list"/);
  assert.doesNotMatch(preview.text, /"total_requests"/);
});
