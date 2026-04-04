const RUN_ACTIVE_STATUSES = new Set(['queued', 'running', 'locked']);
const RUN_SUCCESS_STATUSES = new Set(['completed', 'completed_with_errors', 'ok']);
const RUN_FAILURE_STATUSES = new Set(['failed', 'cancelled', 'dead_letter', 'error']);
const JOB_ACTIVE_STATUSES = new Set(['queued', 'locked', 'running']);
const STEP_SUCCESS_STATUSES = new Set(['ok', 'warn', 'completed']);
const STEP_FAILURE_STATUSES = new Set(['failed', 'error']);
const ATTENTION_STALE_MS = 30 * 60 * 1000;

function toText(value) {
  return String(value || '').trim();
}

function toTs(value) {
  const txt = toText(value);
  if (!txt) return 0;
  const ts = Date.parse(txt);
  return Number.isFinite(ts) ? ts : 0;
}

function sortByStartedAtDesc(left, right) {
  return toTs(right?.started_at) - toTs(left?.started_at);
}

function summaryFromRun(run) {
  const summary = run?.summary_json;
  return summary && typeof summary === 'object' && !Array.isArray(summary) ? summary : {};
}

function nodeNameFromJobDetail(jobDetail) {
  const payload = jobDetail?.payload_json && typeof jobDetail.payload_json === 'object' ? jobDetail.payload_json : {};
  const node = payload?.node && typeof payload.node === 'object' ? payload.node : {};
  const config = node?.config && typeof node.config === 'object' ? node.config : {};
  return toText(config?.name || payload?.node_name || node?.id || payload?.node_id);
}

function nodeIdFromJobDetail(jobDetail) {
  const payload = jobDetail?.payload_json && typeof jobDetail.payload_json === 'object' ? jobDetail.payload_json : {};
  const node = payload?.node && typeof payload.node === 'object' ? payload.node : {};
  return toText(node?.id || payload?.node_id);
}

function terminalStatus(status) {
  const normalized = toText(status).toLowerCase();
  return RUN_SUCCESS_STATUSES.has(normalized) || RUN_FAILURE_STATUSES.has(normalized);
}

export function isActiveRunStatus(status) {
  return RUN_ACTIVE_STATUSES.has(toText(status).toLowerCase());
}

export function isTerminalRunStatus(status) {
  return terminalStatus(status);
}

export function isSuccessfulRunStatus(status) {
  return RUN_SUCCESS_STATUSES.has(toText(status).toLowerCase());
}

export function isFailedRunStatus(status) {
  return RUN_FAILURE_STATUSES.has(toText(status).toLowerCase());
}

export function pickProcessFocusRun({ publishedProcess = null, processRuns = [] } = {}) {
  const runs = (Array.isArray(processRuns) ? [...processRuns] : []).sort(sortByStartedAtDesc);
  const active = runs.find((run) => isActiveRunStatus(run?.status));
  if (active) return active;
  if (runs.length) return runs[0];
  const lastRun = publishedProcess?.last_run;
  return lastRun && typeof lastRun === 'object' ? lastRun : null;
}

export function pickProcessActiveJob(workflowJobs = [], runUid = '') {
  const targetRunUid = toText(runUid);
  if (!targetRunUid) return null;
  const relevant = (Array.isArray(workflowJobs) ? workflowJobs : [])
    .filter((job) => toText(job?.parent_run_uid) === targetRunUid && JOB_ACTIVE_STATUSES.has(toText(job?.status).toLowerCase()))
    .sort((left, right) => Number(right?.job_id || 0) - Number(left?.job_id || 0));
  if (!relevant.length) return null;
  const runningNodeJob = relevant.find(
    (job) => toText(job?.job_type).toLowerCase() === 'process_node' && toText(job?.status).toLowerCase() !== 'queued'
  );
  if (runningNodeJob) return runningNodeJob;
  const nodeJob = relevant.find((job) => toText(job?.job_type).toLowerCase() === 'process_node');
  return nodeJob || relevant[0];
}

export function buildProcessStatusModel({
  draftProcess = null,
  publishedProcess = null,
  publishedDeskReady = false,
  publishedDeskVersionId = 0,
  deskDirty = false,
  schedulerEnabled = false,
  processRuns = [],
  workflowJobs = [],
  runDetail = null,
  activeJobDetail = null,
  nowMs = Date.now()
} = {}) {
  const startNodeId = toText(draftProcess?.start_node_id || publishedProcess?.start_node_id);
  const processCode = toText(draftProcess?.process_code || publishedProcess?.process_code);
  const name = toText(draftProcess?.name || publishedProcess?.name || startNodeId || processCode || 'Процесс');
  const triggerType = toText(publishedProcess?.trigger_type || draftProcess?.trigger_type || 'manual') || 'manual';
  const runPolicy = toText(publishedProcess?.run_policy || '');
  const isPublished = Boolean(publishedDeskReady && publishedProcess);
  const isEnabled = Boolean(publishedProcess?.is_enabled);
  const focusRun = pickProcessFocusRun({ publishedProcess, processRuns });
  const latestRuns = (Array.isArray(processRuns) ? [...processRuns] : []).sort(sortByStartedAtDesc);
  const activeRun = latestRuns.find((run) => isActiveRunStatus(run?.status)) || null;
  const lastRun = focusRun || null;
  const summary = summaryFromRun(lastRun);
  const activeJob = activeRun ? pickProcessActiveJob(workflowJobs, activeRun.run_uid) : null;
  const effectiveJobDetail = activeJobDetail && typeof activeJobDetail === 'object' ? activeJobDetail : null;
  const steps = Array.isArray(runDetail?.steps) ? runDetail.steps : [];
  const successfulSteps = steps.filter((step) => STEP_SUCCESS_STATUSES.has(toText(step?.status).toLowerCase()));
  const failedStep = steps.find((step) => STEP_FAILURE_STATUSES.has(toText(step?.status).toLowerCase())) || null;
  const lastSuccessfulStep = successfulSteps.length ? successfulSteps[successfulSteps.length - 1] : null;
  const currentNodeId = nodeIdFromJobDetail(effectiveJobDetail);
  const currentNodeName = nodeNameFromJobDetail(effectiveJobDetail);
  const activeRunStartedAtTs = toTs(activeRun?.started_at);
  const activeRunAgeMs = activeRunStartedAtTs > 0 ? Math.max(0, nowMs - activeRunStartedAtTs) : 0;
  const requiresAttention =
    (Boolean(activeRun) && activeRunAgeMs >= ATTENTION_STALE_MS) ||
    (isPublished && isEnabled && triggerType !== 'manual' && !schedulerEnabled && !activeRun);

  let statusCode = 'idle';
  let statusLabel = 'Не определён';
  let statusTone = 'neutral';

  if (!isPublished) {
    statusCode = 'draft_unpublished';
    statusLabel = 'Черновик, не опубликован';
    statusTone = 'draft';
  } else if (!isEnabled) {
    statusCode = 'published_disabled';
    statusLabel = 'Опубликован, выключен';
    statusTone = 'disabled';
  } else if (activeRun && toText(activeRun?.status).toLowerCase() === 'queued') {
    statusCode = requiresAttention ? 'needs_attention' : 'queued';
    statusLabel = requiresAttention ? 'Нужна проверка' : 'В очереди';
    statusTone = requiresAttention ? 'attention' : 'queued';
  } else if (activeRun) {
    statusCode = requiresAttention ? 'needs_attention' : 'running';
    statusLabel = requiresAttention ? 'Нужна проверка' : 'Выполняется';
    statusTone = requiresAttention ? 'attention' : 'running';
  } else if (lastRun && (isFailedRunStatus(lastRun?.status) || isFailedRunStatus(summary?.final_status))) {
    statusCode = 'failed';
    statusLabel = 'Завершён с ошибкой';
    statusTone = 'failed';
  } else if (lastRun && (isSuccessfulRunStatus(lastRun?.status) || isSuccessfulRunStatus(summary?.final_status))) {
    statusCode = 'completed';
    statusLabel = 'Завершён успешно';
    statusTone = 'success';
  } else if (triggerType === 'manual') {
    statusCode = 'manual_ready';
    statusLabel = 'Ждёт ручного запуска';
    statusTone = 'manual';
  } else if (!schedulerEnabled) {
    statusCode = 'needs_attention';
    statusLabel = 'Нужна проверка';
    statusTone = 'attention';
  } else {
    statusCode = 'waiting';
    statusLabel = 'Опубликован, ожидает запуска';
    statusTone = 'idle';
  }

  const runUid = toText(activeRun?.run_uid || lastRun?.run_uid);
  const currentStepLabel = currentNodeName || '';
  const failedStepName = toText(failedStep?.node_name || failedStep?.node_id);
  const lastSuccessfulStepName = toText(lastSuccessfulStep?.node_name || lastSuccessfulStep?.node_id);
  const successfulNodeIds = [...new Set(successfulSteps.map((step) => toText(step?.node_id)).filter(Boolean))];

  let primaryMessage = '';
  if (statusCode === 'draft_unpublished') {
    primaryMessage = 'Опубликуй рабочий стол, чтобы сервер увидел этот процесс.';
  } else if (statusCode === 'published_disabled') {
    primaryMessage = 'Процесс опубликован, но выключен и не будет запускаться.';
  } else if (statusCode === 'queued') {
    primaryMessage = runUid ? `Run ${runUid} уже в очереди.` : 'Запуск поставлен в очередь.';
  } else if (statusCode === 'running') {
    primaryMessage = currentStepLabel ? `Сейчас выполняется шаг: ${currentStepLabel}.` : 'Run уже выполняется на сервере.';
  } else if (statusCode === 'needs_attention') {
    if (failedStepName) primaryMessage = `Run требует проверки. Последняя ошибка на шаге: ${failedStepName}.`;
    else if (activeRun && activeRunAgeMs >= ATTENTION_STALE_MS) primaryMessage = 'Активный run не меняется слишком долго и требует проверки.';
    else if (triggerType !== 'manual' && !schedulerEnabled) primaryMessage = 'Процесс включён, но планировщик сейчас выключен.';
    else primaryMessage = 'Серверный статус процесса требует проверки.';
  } else if (statusCode === 'failed') {
    primaryMessage = failedStepName
      ? `Упал на шаге: ${failedStepName}.`
      : `Последний run завершился с ошибкой${runUid ? ` (${runUid})` : ''}.`;
  } else if (statusCode === 'completed') {
    primaryMessage = lastSuccessfulStepName
      ? `Последний run завершился успешно. Последний шаг: ${lastSuccessfulStepName}.`
      : 'Последний run завершился успешно.';
  } else if (statusCode === 'manual_ready') {
    primaryMessage = 'Процесс опубликован и ждёт ручного запуска.';
  } else if (statusCode === 'waiting') {
    primaryMessage = 'Процесс опубликован и ждёт следующего запуска.';
  }

  const actionLabel = activeRun ? (toText(activeRun?.status).toLowerCase() === 'queued' ? 'В очереди...' : 'Выполняется...') : lastRun ? 'Запустить повторно' : 'Запустить процесс';
  const actionDisabled = !isPublished || !isEnabled || Boolean(activeRun);
  const enableActionLabel = !isPublished ? 'Не опубликован' : isEnabled ? 'Выключен' : 'Включить';

  return {
    startNodeId,
    processCode,
    name,
    triggerType,
    runPolicy,
    isPublished,
    isEnabled,
    hasUnpublishedChanges: Boolean(deskDirty),
    publishedDeskVersionId: Math.max(0, Number(publishedDeskVersionId || 0)),
    statusCode,
    statusLabel,
    statusTone,
    primaryMessage,
    actionLabel,
    actionDisabled,
    enableActionLabel,
    enableActionDisabled: !isPublished,
    runUid,
    runStatus: toText(activeRun?.status || lastRun?.status),
    runStartedAt: toText(activeRun?.started_at || lastRun?.started_at),
    runFinishedAt: toText(activeRun?.finished_at || lastRun?.finished_at),
    runDurationMs: Number(lastRun?.duration_ms || 0) || (activeRunStartedAtTs > 0 ? activeRunAgeMs : 0),
    triggerMeta: {
      triggerType,
      runPolicy
    },
    summary: {
      totalJobs: Math.max(0, Number(summary?.total_jobs || 0)),
      queuedJobs: Math.max(0, Number(summary?.queued_jobs || 0)),
      runningJobs: Math.max(0, Number(summary?.running_jobs || 0)),
      completedJobs: Math.max(0, Number(summary?.completed_jobs || 0)),
      failedJobs: Math.max(0, Number(summary?.failed_jobs || 0)),
      totalSteps: Math.max(0, Number(summary?.total_steps || 0)),
      okSteps: Math.max(0, Number(summary?.ok_steps || 0)),
      warnSteps: Math.max(0, Number(summary?.warn_steps || 0)),
      errorSteps: Math.max(0, Number(summary?.error_steps || 0)),
      progressPercent: Number(summary?.progress_percent || 0) || 0
    },
    currentStep: {
      nodeId: currentNodeId,
      nodeName: currentNodeName
    },
    failedStep: {
      nodeId: toText(failedStep?.node_id),
      nodeName: failedStepName
    },
    lastSuccessfulStep: {
      nodeId: toText(lastSuccessfulStep?.node_id),
      nodeName: lastSuccessfulStepName
    },
    successfulNodeIds,
    focusRun: lastRun,
    activeRun,
    activeJob,
    activeJobDetail: effectiveJobDetail,
    runDetail,
    requiresAttention
  };
}

export function buildDeskExecutionStateSummary({
  deskId = 0,
  publishedDeskReady = false,
  processModels = []
} = {}) {
  if (!deskId) {
    return {
      mode: 'idle',
      label: 'Рабочий стол ещё не создан',
      hint: 'Сначала сохрани рабочий стол, чтобы сервер мог работать с его процессами.'
    };
  }
  if (!publishedDeskReady) {
    return {
      mode: 'idle',
      label: 'Черновик, не опубликован',
      hint: 'Сначала опубликуй рабочий стол, чтобы сервер начал видеть процессы.'
    };
  }
  const items = Array.isArray(processModels) ? processModels : [];
  const anyRunning = items.find((item) => item.statusCode === 'running');
  if (anyRunning) {
    return {
      mode: 'active',
      label: 'Есть активный запуск',
      hint: anyRunning.currentStep?.nodeName
        ? `Сейчас выполняется процесс "${anyRunning.name}", шаг "${anyRunning.currentStep.nodeName}".`
        : `Сейчас выполняется процесс "${anyRunning.name}".`
    };
  }
  const anyQueued = items.find((item) => item.statusCode === 'queued');
  if (anyQueued) {
    return {
      mode: 'queued',
      label: 'Запуск в очереди',
      hint: `Процесс "${anyQueued.name}" поставлен в очередь и ждёт исполнения.`
    };
  }
  const anyAttention = items.find((item) => item.statusCode === 'needs_attention');
  if (anyAttention) {
    return {
      mode: 'blocked',
      label: 'Нужна проверка',
      hint: anyAttention.primaryMessage || `Проверь процесс "${anyAttention.name}".`
    };
  }
  const anyFailed = items.find((item) => item.statusCode === 'failed');
  if (anyFailed) {
    return {
      mode: 'blocked',
      label: 'Последний запуск с ошибкой',
      hint: anyFailed.primaryMessage || `Последний run процесса "${anyFailed.name}" завершился с ошибкой.`
    };
  }
  const anyManual = items.find((item) => item.statusCode === 'manual_ready');
  if (anyManual) {
    return {
      mode: 'manual',
      label: 'Ждёт ручного запуска',
      hint: `Опубликованный процесс "${anyManual.name}" ждёт ручного запуска.`
    };
  }
  const anyCompleted = items.find((item) => item.statusCode === 'completed');
  if (anyCompleted) {
    return {
      mode: 'success',
      label: 'Последний запуск завершён',
      hint: `Последний run процесса "${anyCompleted.name}" завершился успешно.`
    };
  }
  const anyEnabled = items.find((item) => item.isPublished && item.isEnabled);
  if (anyEnabled) {
    return {
      mode: 'idle',
      label: 'Опубликовано, ожидает запуска',
      hint: 'Процессы опубликованы и готовы к следующему запуску.'
    };
  }
  return {
    mode: 'idle',
    label: 'Опубликовано, но процессов нет',
    hint: 'На опубликованном рабочем столе нет включённых процессов.'
  };
}

function priorityForProcessModel(model) {
  const code = toText(model?.statusCode);
  if (code === 'running') return 100;
  if (code === 'queued') return 90;
  if (code === 'needs_attention') return 80;
  if (code === 'failed') return 70;
  if (code === 'completed') return 60;
  if (code === 'manual_ready') return 50;
  if (code === 'waiting') return 40;
  if (code === 'published_disabled') return 30;
  if (code === 'draft_unpublished') return 20;
  return 10;
}

export function pickDominantProcessModel(models = []) {
  const items = (Array.isArray(models) ? models : []).filter(Boolean);
  if (!items.length) return null;
  return [...items].sort((left, right) => priorityForProcessModel(right) - priorityForProcessModel(left))[0] || null;
}

export function buildNodeProcessVisualState({
  nodeId = '',
  isStartNode = false,
  processModel = null
} = {}) {
  const safeNodeId = toText(nodeId);
  if (!safeNodeId || !processModel) return null;

  const successfulNodeIds = new Set(processModel.successfulNodeIds || []);
  const currentNodeId = toText(processModel.currentStep?.nodeId);
  const failedNodeId = toText(processModel.failedStep?.nodeId);
  const statusCode = toText(processModel.statusCode);

  if (isStartNode) {
    if (statusCode === 'running') return { tone: 'running', label: 'Выполняется', hint: processModel.primaryMessage };
    if (statusCode === 'queued') return { tone: 'queued', label: 'В очереди', hint: processModel.primaryMessage };
    if (statusCode === 'failed' || statusCode === 'needs_attention') return { tone: 'failed', label: 'Ошибка', hint: processModel.primaryMessage };
    if (statusCode === 'completed') return { tone: 'success', label: 'Успешно', hint: processModel.primaryMessage };
    if (statusCode === 'manual_ready' || statusCode === 'waiting') return { tone: 'pending', label: 'Готов', hint: processModel.primaryMessage };
    if (statusCode === 'published_disabled') return { tone: 'disabled', label: 'Выкл', hint: processModel.primaryMessage };
    return { tone: 'draft', label: 'Черновик', hint: processModel.primaryMessage };
  }

  if (failedNodeId && failedNodeId === safeNodeId) {
    return { tone: 'failed', label: 'Ошибка', hint: processModel.primaryMessage };
  }
  if (currentNodeId && currentNodeId === safeNodeId) {
    return { tone: 'running', label: 'Сейчас', hint: processModel.primaryMessage };
  }
  if (successfulNodeIds.has(safeNodeId)) {
    return { tone: 'success', label: 'Пройден', hint: 'Шаг уже успешно выполнен в последнем run.' };
  }
  if (processModel.runUid && (statusCode === 'running' || statusCode === 'queued' || statusCode === 'failed' || statusCode === 'completed')) {
    return { tone: 'pending', label: 'Ожидание', hint: 'Этот шаг ещё не был выполнен в текущем/последнем run.' };
  }
  return null;
}
