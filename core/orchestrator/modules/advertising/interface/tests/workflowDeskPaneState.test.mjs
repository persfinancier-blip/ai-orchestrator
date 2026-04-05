import test from 'node:test';
import assert from 'node:assert/strict';

import {
  WORKFLOW_DESK_VISIBLE_PANES,
  resolveWorkflowDeskPane,
  sanitizeWorkflowDeskParams,
  shouldRefreshWorkflowDeskTables
} from '../desk/data/workflowDeskPaneState.js';

test('workflow desk pane state: parser tab is not visible anymore', () => {
  assert.deepEqual(WORKFLOW_DESK_VISIBLE_PANES, ['workspace', 'api', 'clients']);
  assert.ok(!WORKFLOW_DESK_VISIBLE_PANES.includes('parser'));
});

test('workflow desk pane state: old parser pane falls back to workspace', () => {
  assert.equal(resolveWorkflowDeskPane({ route: 'desk/data', paneParam: 'parser', apiStoreId: null }), 'workspace');

  const params = new URLSearchParams('desk_id=24&pane=parser&from_node=node-1');
  const normalized = sanitizeWorkflowDeskParams({ route: 'desk/data', paneParam: 'parser', params });
  assert.equal(normalized.route, 'desk/data');
  assert.equal(normalized.changed, true);
  assert.equal(normalized.params.get('pane'), null);
  assert.equal(normalized.params.get('from_node'), null);
  assert.equal(normalized.params.get('desk_id'), '24');
});

test('workflow desk pane state: api and clients panes keep working', () => {
  assert.equal(resolveWorkflowDeskPane({ route: 'desk/data', paneParam: 'api', apiStoreId: null }), 'api');
  assert.equal(resolveWorkflowDeskPane({ route: 'desk/data', paneParam: 'clients', apiStoreId: null }), 'clients');
  assert.equal(resolveWorkflowDeskPane({ route: 'desk/workflow', paneParam: '', apiStoreId: null }), 'api');
  assert.equal(resolveWorkflowDeskPane({ route: 'desk/data', paneParam: '', apiStoreId: 17 }), 'api');
  assert.equal(shouldRefreshWorkflowDeskTables('api'), true);
  assert.equal(shouldRefreshWorkflowDeskTables('clients'), true);
  assert.equal(shouldRefreshWorkflowDeskTables('workspace'), false);
});
