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
      },
      max_restarts: 20,
      restart_delay: 2000,
      autorestart: true,
      time: true,
    },
  ],
};
