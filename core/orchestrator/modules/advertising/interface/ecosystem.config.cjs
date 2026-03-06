// core/orchestrator/modules/advertising/interface/ecosystem.config.cjs
module.exports = {
  apps: [
    {
      name: 'ai-orchestrator-adv-api',
      cwd: __dirname,
      script: 'server/spaceServer.mjs',
      interpreter: 'node',
      env: {
        SPACE_API_PORT: '8787',
        AO_WORKFLOW_SCHEDULER_ENABLED: 'true',
        AO_WORKFLOW_SCHEDULER_TICK_MS: '10000',
        AO_WORKFLOW_MAX_PARALLEL_RUNS: '3',
        AO_WORKFLOW_HTTP_TIMEOUT_MS: '30000',
        AO_WORKFLOW_RETRY_ATTEMPTS: '3',
        AO_WORKFLOW_RETRY_BASE_MS: '250',
        AO_WORKFLOW_RETRY_MAX_MS: '5000',
        AO_WORKFLOW_LOCK_TTL_MS: '300000',
      },
      max_restarts: 20,
      restart_delay: 2000,
      autorestart: true,
      time: true,
    },
  ],
};
