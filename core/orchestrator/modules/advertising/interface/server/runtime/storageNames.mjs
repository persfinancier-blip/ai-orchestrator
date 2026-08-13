export const RUNTIME_STORAGE = Object.freeze({
  schema: 'ao_system',
  runs: 'workflow_runs_store',
  queue: 'workflow_job_queue_store',
  dead: 'workflow_dead_jobs_store',
  aggregation: 'workflow_run_aggregation_store',
  events: 'workflow_runtime_events_store',
  idempotency: 'workflow_runtime_idempotency_store',
  waitpoints: 'workflow_waitpoints_store'
});
