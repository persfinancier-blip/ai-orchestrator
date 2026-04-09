import test from 'node:test';
import assert from 'node:assert/strict';

import {
  canOpenWorkflowNodeSettings,
  openableWorkflowToolTypes
} from '../desk/data/workflowNodeSettingsAccess.js';

test('workflow node settings access: new action nodes are openable', () => {
  assert.equal(
    canOpenWorkflowNodeSettings({ id: 'n1', type: 'tool', config: { toolType: 'action_prep' } }),
    true
  );
  assert.equal(
    canOpenWorkflowNodeSettings({ id: 'n2', type: 'tool', config: { toolType: 'api_mutation' } }),
    true
  );
});

test('workflow node settings access: existing editable nodes stay openable', () => {
  assert.equal(
    canOpenWorkflowNodeSettings({ id: 'api', type: 'data', config: { group: 'api_requests' } }),
    true
  );
  assert.equal(
    canOpenWorkflowNodeSettings({ id: 'p', type: 'tool', config: { toolType: 'table_parser' } }),
    true
  );
  assert.equal(
    canOpenWorkflowNodeSettings({ id: 'w', type: 'tool', config: { toolType: 'db_write' } }),
    true
  );
});

test('workflow node settings access: unknown tools stay blocked and exported registry includes new nodes', () => {
  const openable = openableWorkflowToolTypes();
  assert.equal(
    canOpenWorkflowNodeSettings({ id: 'x', type: 'tool', config: { toolType: 'unknown_future_tool' } }),
    false
  );
  assert.ok(openable.includes('action_prep'));
  assert.ok(openable.includes('api_mutation'));
});
