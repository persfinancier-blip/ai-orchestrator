import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';

const { executeActionPrepConfig } = await import('../server/actionPrepRuntime.mjs');
const { previewApiMutationConfig, executeApiMutationConfig } = await import('../server/apiMutationRuntime.mjs');

function createServer(handler) {
  return new Promise((resolve) => {
    const server = http.createServer(handler);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      resolve({ server, port: address.port });
    });
  });
}

test('api mutation runtime: dry-run keeps canonical input and builds request preview without network calls', async () => {
  const preview = await previewApiMutationConfig(
    null,
    {
      endpointUrl: 'https://example.test/mutate',
      httpMethod: 'POST',
      bodyJson: '{}',
      bindingRulesJson: JSON.stringify([
        { sourceField: 'campaign_id', target: 'body', path: 'campaign.id' },
        { sourceField: 'new_bid', target: 'body', path: 'campaign.new_bid' }
      ])
    },
    {
      inputValue: {
        contract_version: 'node_io_v1',
        rows: [{ campaign_id: 10, new_bid: 123 }]
      }
    }
  );

  assert.equal(preview.row_count, 1);
  assert.equal(preview.sample_rows[0].mutation_dry_run, true);
  assert.equal(preview.request_preview[0].body.campaign.id, 10);
  assert.equal(preview.request_preview[0].body.campaign.new_bid, 123);
});

test('api mutation runtime: batch mode groups rows and maps response fields', async () => {
  const requests = [];
  const { server, port } = await createServer(async (req, res) => {
    let body = '';
    for await (const chunk of req) body += chunk;
    requests.push({ url: req.url, method: req.method, body: body ? JSON.parse(body) : null });
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ result: { ok: true, accepted: Array.isArray(requests[requests.length - 1].body?.items) ? requests[requests.length - 1].body.items.length : 0 } }));
  });
  try {
    const result = await executeApiMutationConfig(
      null,
      {
        endpointUrl: `http://127.0.0.1:${port}/mutate`,
        httpMethod: 'POST',
        requestMode: 'batch_request',
        batchSize: '2',
        dryRun: 'false',
        bodyJson: '{}',
        bodyItemsPath: 'items',
        bindingRulesJson: JSON.stringify([
          { sourceField: 'campaign_id', target: 'body_item', path: 'id' },
          { sourceField: 'new_bid', target: 'body_item', path: 'bid' }
        ]),
        responseMappingsJson: JSON.stringify([{ responsePath: 'result.accepted', alias: 'accepted_count' }])
      },
      {
        inputValue: {
          contract_version: 'node_io_v1',
          rows: [
            { campaign_id: 1, new_bid: 101 },
            { campaign_id: 2, new_bid: 202 },
            { campaign_id: 3, new_bid: 303 }
          ]
        }
      }
    );

    assert.equal(requests.length, 2);
    assert.equal(requests[0].body.items.length, 2);
    assert.equal(requests[1].body.items.length, 1);
    assert.equal(result.rows.length, 2);
    assert.equal(result.rows[0].accepted_count, 2);
    assert.equal(result.rows[1].accepted_count, 1);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test('action prep -> api mutation handoff keeps action-ready rows as canonical input', async () => {
  const prepared = await executeActionPrepConfig(
    { query: async () => ({ rows: [] }) },
    {
      sourceMode: 'node',
      actionColumnsJson: JSON.stringify([
        { name: 'action_type', mode: 'constant', constantValue: 'update_bid' },
        { name: 'new_bid', mode: 'formula', expression: '{current_bid} * 1.05', type: 'numeric' }
      ])
    },
    {
      inputValue: {
        contract_version: 'node_io_v1',
        rows: [{ campaign_id: 55, current_bid: 100 }]
      }
    }
  );

  const preview = await previewApiMutationConfig(
    null,
    {
      endpointUrl: 'https://example.test/mutate',
      httpMethod: 'POST',
      bodyJson: '{}',
      bindingRulesJson: JSON.stringify([
        { sourceField: 'campaign_id', target: 'body', path: 'campaign.id' },
        { sourceField: 'action_type', target: 'body', path: 'action.type' },
        { sourceField: 'new_bid', target: 'body', path: 'action.new_bid' }
      ])
    },
    {
      inputValue: {
        contract_version: 'node_io_v1',
        rows: prepared.rows
      }
    }
  );

  assert.equal(preview.input_sample_rows[0].action_type, 'update_bid');
  assert.equal(preview.request_preview[0].body.action.new_bid, 105);
});
