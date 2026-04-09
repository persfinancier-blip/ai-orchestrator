import test from 'node:test';
import assert from 'node:assert/strict';

import {
  actionPrepEditorMountKey,
  apiMutationEditorMountKey,
  mergeWorkflowToolSettings,
  safeWorkflowToolSettings
} from '../desk/data/workflowNodeEditorState.js';

test('workflow node editor state: missing tool settings stay safe for new nodes', () => {
  const actionNode = { id: 'action', type: 'tool', config: { toolType: 'action_prep' } };
  const mutationNode = { id: 'mutation', type: 'tool', config: { toolType: 'api_mutation', settings: null } };

  assert.deepEqual(safeWorkflowToolSettings(actionNode), {});
  assert.deepEqual(safeWorkflowToolSettings(mutationNode), {});
  assert.equal(actionPrepEditorMountKey(actionNode), 'action:node::');
  assert.equal(apiMutationEditorMountKey(mutationNode), 'mutation::row_per_request');
});

test('workflow node editor state: mergeWorkflowToolSettings backfills defaults without dropping explicit values', () => {
  const actionDefaults = { sourceMode: 'node', sourceSchema: '', sourceTable: '', previewLimit: '20' };
  const actionNode = {
    id: 'action',
    type: 'tool',
    config: {
      toolType: 'action_prep',
      settings: {
        sourceMode: 'table',
        sourceSchema: 'silver',
        sourceTable: 'ads_actions'
      }
    }
  };
  const mutationDefaults = { endpointUrl: '', requestMode: 'row_per_request', batchSize: '50' };
  const mutationNode = {
    id: 'mutation',
    type: 'tool',
    config: {
      toolType: 'api_mutation',
      settings: {
        endpointUrl: 'https://example.test/api',
        requestMode: 'batch_request'
      }
    }
  };

  assert.deepEqual(mergeWorkflowToolSettings(actionDefaults, actionNode), {
    sourceMode: 'table',
    sourceSchema: 'silver',
    sourceTable: 'ads_actions',
    previewLimit: '20'
  });
  assert.deepEqual(mergeWorkflowToolSettings(mutationDefaults, mutationNode), {
    endpointUrl: 'https://example.test/api',
    requestMode: 'batch_request',
    batchSize: '50'
  });
});

test('workflow node editor state: old-new-old editor sequence keeps mount key resolution stable', () => {
  const parserNode = {
    id: 'parser',
    type: 'tool',
    config: {
      toolType: 'table_parser',
      settings: {
        templateStoreId: '1',
        templateId: 'parser_template'
      }
    }
  };
  const actionNode = { id: 'action', type: 'tool', config: { toolType: 'action_prep' } };
  const apiMutationNode = { id: 'mutation', type: 'tool', config: { toolType: 'api_mutation', settings: undefined } };

  assert.deepEqual(safeWorkflowToolSettings(parserNode), {
    templateStoreId: '1',
    templateId: 'parser_template'
  });
  assert.equal(actionPrepEditorMountKey(actionNode), 'action:node::');
  assert.equal(apiMutationEditorMountKey(apiMutationNode), 'mutation::row_per_request');
  assert.deepEqual(safeWorkflowToolSettings(parserNode), {
    templateStoreId: '1',
    templateId: 'parser_template'
  });
});
