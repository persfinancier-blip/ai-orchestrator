import express from 'express';
import { clientModuleRouter } from './clientModuleRouter.mjs';
import { goldBuilderRouter } from './goldBuilderRouter.mjs';
import { tableBuilderRouter } from './tableBuilder.mjs';
import { workflowAutomationRouter } from './workflowAutomation.mjs';
import { createAppAuth } from './runtime/appAuth.mjs';
import { showcaseRouter } from './showcaseRouter.mjs';

export function createProductApp() {
  const app = express();
  const auth = createAppAuth();

  app.use(express.json({ limit: '4mb' }));
  app.get('/ai-orchestrator/api/health', (_req, res) => {
    res.json({ ok: true, product: 'ai-orchestrator', auth_configured: auth.configured });
  });
  app.use('/ai-orchestrator/api', auth.router);
  app.use('/ai-orchestrator/api', auth.gate);
  app.use('/ai-orchestrator/api', tableBuilderRouter);
  app.use('/ai-orchestrator/api', workflowAutomationRouter);
  app.use('/ai-orchestrator/api', clientModuleRouter);
  app.use('/ai-orchestrator/api', goldBuilderRouter);
  app.use('/ai-orchestrator/api', showcaseRouter);

  return app;
}
