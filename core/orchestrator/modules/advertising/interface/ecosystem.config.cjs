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

        // Эти переменные должны быть заданы на сервере (или добавим позже в secrets и прокинем)
        // PGHOST: '127.0.0.1',
        // PGPORT: '5432',
        // PGUSER: '...',
        // PGPASSWORD: '...',
        // PGDATABASE: '...',
      },
      max_restarts: 20,
      restart_delay: 2000,
      autorestart: true,
      time: true,
    },
  ],
};
