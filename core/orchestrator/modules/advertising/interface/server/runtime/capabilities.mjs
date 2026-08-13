export const RUNTIME_CAPABILITIES = Object.freeze({
  runtime: 'ai-orchestrator-postgres-durable',
  contract_version: 'runtime_v2',
  external_runtime_dependencies: [],
  persistence: 'postgresql',
  mechanics: [
    'published_workflow_versions',
    'postgres_skip_locked_queue',
    'scheduler_and_worker_leases',
    'dead_letter_retry',
    'dependency_dispatch',
    'tenant_flow_control',
    'incremental_state',
    'step_observability',
    'chunk_observability',
    'provider_registry',
    'secure_control_auth'
  ]
});

export function runtimeCapabilitiesHandler(_req, res) {
  return res.json({ ok: true, ...RUNTIME_CAPABILITIES });
}
