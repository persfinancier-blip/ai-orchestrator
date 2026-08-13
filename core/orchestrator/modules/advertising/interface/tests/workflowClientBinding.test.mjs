import test from 'node:test';
import assert from 'node:assert/strict';
import { bindWorkflowClient } from '../server/workflowClientBindingMiddleware.mjs';

function graph(clientId = '') {
  return {
    nodes: [
      {
        id: 'start_1',
        type: 'tool',
        config: {
          name: 'Start',
          toolType: 'start_process',
          settings: clientId ? { clientId } : {}
        }
      },
      { id: 'end_1', type: 'tool', config: { toolType: 'end_process', settings: {} } }
    ],
    edges: [{ from: 'start_1', to: 'end_1' }]
  };
}

test('selected client is written into empty start node', () => {
  const result = bindWorkflowClient(graph(), 42);
  assert.equal(result.ok, true);
  assert.equal(result.changed, true);
  assert.equal(result.config_json.nodes[0].config.settings.clientId, '42');
});

test('existing same client remains stable', () => {
  const result = bindWorkflowClient(graph('42'), 42);
  assert.equal(result.ok, true);
  assert.equal(result.changed, false);
});

test('workflow cannot be silently rebound to another client', () => {
  const result = bindWorkflowClient(graph('7'), 42);
  assert.equal(result.ok, false);
  assert.equal(result.conflicts.length, 1);
  assert.equal(result.conflicts[0].client_id, '7');
});

test('missing selected client leaves graph global', () => {
  const source = graph();
  const result = bindWorkflowClient(source, 0);
  assert.equal(result.ok, true);
  assert.equal(result.changed, false);
  assert.equal(result.config_json, source);
});
