import test from 'node:test';
import assert from 'node:assert/strict';

import {
  buildDeskExecutionStateSummary,
  buildNodeProcessVisualState,
  buildProcessStatusModel,
  pickProcessActiveJob,
  pickProcessFocusRun
} from '../desk/data/workflowProcessStatusCore.js';

function isoMinutesAgo(minutes) {
  return new Date(Date.now() - minutes * 60 * 1000).toISOString();
}

test('process status: draft desk stays unpublished until publish exists', () => {
  const model = buildProcessStatusModel({
    draftProcess: {
      start_node_id: 'start_1',
      process_code: 'sync_cards',
      name: 'Синхронизация карточек',
      trigger_type: 'manual'
    },
    publishedProcess: null,
    publishedDeskReady: false,
    deskDirty: true
  });

  assert.equal(model.statusCode, 'draft_unpublished');
  assert.equal(model.statusLabel, 'Черновик, не опубликован');
  assert.equal(model.hasUnpublishedChanges, true);
  assert.equal(model.actionDisabled, true);
});

test('process status: manual published process waits for manual trigger', () => {
  const model = buildProcessStatusModel({
    draftProcess: {
      start_node_id: 'start_1',
      process_code: 'sync_cards',
      name: 'Синхронизация карточек',
      trigger_type: 'manual'
    },
    publishedProcess: {
      start_node_id: 'start_1',
      process_code: 'sync_cards',
      name: 'Синхронизация карточек',
      is_enabled: true,
      trigger_type: 'manual',
      run_policy: 'single_instance'
    },
    publishedDeskReady: true
  });

  assert.equal(model.statusCode, 'manual_ready');
  assert.equal(model.statusLabel, 'Ждёт ручного запуска');
  assert.equal(model.actionLabel, 'Запустить процесс');
  assert.equal(model.actionDisabled, false);
});

test('process status: active running process picks current node from active job detail', () => {
  const run = {
    run_uid: 'wf_run_1',
    status: 'running',
    started_at: isoMinutesAgo(5),
    summary_json: {
      total_jobs: 3,
      queued_jobs: 0,
      running_jobs: 1,
      completed_jobs: 1,
      total_steps: 3,
      ok_steps: 1,
      progress_percent: 33.3
    }
  };
  const activeJob = {
    job_id: 101,
    parent_run_uid: 'wf_run_1',
    job_type: 'process_node',
    status: 'running'
  };
  const model = buildProcessStatusModel({
    draftProcess: {
      start_node_id: 'start_1',
      process_code: 'sync_cards',
      name: 'Синхронизация карточек',
      trigger_type: 'manual'
    },
    publishedProcess: {
      start_node_id: 'start_1',
      process_code: 'sync_cards',
      name: 'Синхронизация карточек',
      is_enabled: true,
      trigger_type: 'manual'
    },
    publishedDeskReady: true,
    processRuns: [run],
    workflowJobs: [activeJob],
    runDetail: {
      run,
      steps: [{ step_order: 1, node_id: 'api_1', node_name: 'API-запрос', status: 'ok' }]
    },
    activeJobDetail: {
      job_id: 101,
      payload_json: {
        node: {
          id: 'parser_1',
          config: { name: 'Парсер данных' }
        }
      }
    }
  });

  assert.equal(model.statusCode, 'running');
  assert.equal(model.currentStep.nodeId, 'parser_1');
  assert.equal(model.currentStep.nodeName, 'Парсер данных');
  assert.equal(model.successfulNodeIds.includes('api_1'), true);
});

test('process status: failed run exposes failed and last successful steps', () => {
  const run = {
    run_uid: 'wf_run_2',
    status: 'failed',
    started_at: '2026-04-04T10:00:00.000Z',
    finished_at: '2026-04-04T10:01:00.000Z'
  };
  const model = buildProcessStatusModel({
    draftProcess: {
      start_node_id: 'start_1',
      process_code: 'sync_cards',
      name: 'Синхронизация карточек',
      trigger_type: 'manual'
    },
    publishedProcess: {
      start_node_id: 'start_1',
      process_code: 'sync_cards',
      name: 'Синхронизация карточек',
      is_enabled: true,
      trigger_type: 'manual'
    },
    publishedDeskReady: true,
    processRuns: [run],
    runDetail: {
      run,
      steps: [
        { step_order: 1, node_id: 'api_1', node_name: 'API-запрос', status: 'ok' },
        { step_order: 2, node_id: 'parser_1', node_name: 'Парсер данных', status: 'failed' }
      ]
    }
  });

  assert.equal(model.statusCode, 'failed');
  assert.equal(model.failedStep.nodeId, 'parser_1');
  assert.equal(model.lastSuccessfulStep.nodeId, 'api_1');
  assert.match(model.primaryMessage, /Парсер данных/);
});

test('process helper: focus run prefers active run and active job prefers process node', () => {
  const focus = pickProcessFocusRun({
    processRuns: [
      { run_uid: 'wf_old', status: 'completed', started_at: '2026-04-04T09:00:00.000Z' },
      { run_uid: 'wf_active', status: 'running', started_at: '2026-04-04T10:00:00.000Z' }
    ]
  });
  const job = pickProcessActiveJob(
    [
      { job_id: 1, parent_run_uid: 'wf_active', job_type: 'process_plan', status: 'queued' },
      { job_id: 2, parent_run_uid: 'wf_active', job_type: 'process_node', status: 'running' }
    ],
    'wf_active'
  );

  assert.equal(focus?.run_uid, 'wf_active');
  assert.equal(job?.job_id, 2);
});

test('node indicator: start/current/failed states derive from process model', () => {
  const runningModel = buildProcessStatusModel({
    draftProcess: {
      start_node_id: 'start_1',
      process_code: 'sync_cards',
      name: 'Синхронизация карточек',
      trigger_type: 'manual'
    },
    publishedProcess: {
      start_node_id: 'start_1',
      process_code: 'sync_cards',
      name: 'Синхронизация карточек',
      is_enabled: true,
      trigger_type: 'manual'
    },
    publishedDeskReady: true,
    processRuns: [{ run_uid: 'wf_run_1', status: 'running', started_at: isoMinutesAgo(3) }],
    runDetail: {
      run: { run_uid: 'wf_run_1', status: 'running' },
      steps: [{ step_order: 1, node_id: 'api_1', node_name: 'API-запрос', status: 'ok' }]
    },
    workflowJobs: [{ job_id: 10, parent_run_uid: 'wf_run_1', job_type: 'process_node', status: 'running' }],
    activeJobDetail: {
      payload_json: {
        node: {
          id: 'parser_1',
          config: { name: 'Парсер данных' }
        }
      }
    }
  });

  const startState = buildNodeProcessVisualState({ nodeId: 'start_1', isStartNode: true, processModel: runningModel });
  const completedState = buildNodeProcessVisualState({ nodeId: 'api_1', processModel: runningModel });
  const currentState = buildNodeProcessVisualState({ nodeId: 'parser_1', processModel: runningModel });

  assert.equal(startState?.tone, 'running');
  assert.equal(completedState?.tone, 'success');
  assert.equal(currentState?.tone, 'running');
});

test('desk execution summary prefers active, queued, failed, manual in that order', () => {
  const queued = { statusCode: 'queued', name: 'Q', currentStep: { nodeName: '' } };
  const failed = { statusCode: 'failed', name: 'F', primaryMessage: 'Ошибка' };
  const summary = buildDeskExecutionStateSummary({
    deskId: 24,
    publishedDeskReady: true,
    processModels: [failed, queued]
  });

  assert.equal(summary.mode, 'queued');
  assert.equal(summary.label, 'Запуск в очереди');
});
