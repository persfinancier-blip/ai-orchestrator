import express from 'express';
import { pool } from './db.mjs';
import { bootstrapClientModule, clientModuleRouter } from './clientModuleRouter.mjs';
import { clientSecretsMiddleware } from './clientSecretsMiddleware.mjs';
import { bootstrapGoldBuilder, goldBuilderRouter } from './goldBuilderRouter.mjs';
import { bootstrapTableBuilder, tableBuilderRouter } from './tableBuilder.mjs';
import { bootstrapWorkflowAutomation, startWorkflowScheduler, workflowAutomationRouter } from './workflowAutomation.mjs';
import { clientContextRouter, injectClientContext } from './clientContextRouter.mjs';
import { createAppAuth } from './runtime/appAuth.mjs';

const app = express();
const port = Number(process.env.SPACE_API_PORT || 8787);
const host = String(process.env.AO_API_HOST || '127.0.0.1');
const auth = createAppAuth({ allowInsecure: false });

app.use(express.json({ limit: '4mb' }));

app.get('/ai-orchestrator/api/health', (_req, res) => {
  res.json({ ok: true, product: 'ai-orchestrator', auth_configured: auth.configured, port });
});

app.get('/ai-orchestrator/api/ready', async (_req, res) => {
  let database = false;
  try {
    await pool.query('SELECT 1');
    database = true;
  } catch {}
  const ready = database && auth.configured;
  res.status(ready ? 200 : 503).json({ ready, database, auth_configured: auth.configured });
});

app.use('/ai-orchestrator/api', auth.router);
app.use('/ai-orchestrator/api', auth.gate);
app.use('/ai-orchestrator/api', clientContextRouter);
app.use('/ai-orchestrator/api', injectClientContext);
app.use('/ai-orchestrator/api', clientSecretsMiddleware);
app.use('/ai-orchestrator/api', tableBuilderRouter);
app.use('/ai-orchestrator/api', workflowAutomationRouter);
app.use('/ai-orchestrator/api', clientModuleRouter);
app.use('/ai-orchestrator/api', goldBuilderRouter);

async function handleSpace(req, res) {
  const limit = Math.max(1, Math.min(Number(req.query.limit ?? 500), 5000));
  const offset = Math.max(0, Number(req.query.offset ?? 0));
  try {
    const result = await pool.query('SELECT * FROM showcase.advertising LIMIT $1::int OFFSET $2::int', [limit, offset]);
    return res.json({ points: result.rows, limit, offset });
  } catch (error) {
    console.error('showcase.advertising read error:', error);
    return res.status(500).json({ error: 'showcase_read_failed' });
  }
}

app.get('/ai-orchestrator/api/space', handleSpace);
app.get('/ai-orchestrator/api/space/', handleSpace);

for (const [name, run] of [
  ['Table Builder', bootstrapTableBuilder],
  ['Workflow automation', bootstrapWorkflowAutomation],
  ['Client module', bootstrapClientModule],
  ['Gold builder', bootstrapGoldBuilder]
]) {
  try {
    console.log(`${name} bootstrap:`, await run());
  } catch (error) {
    console.error(`${name} bootstrap failed:`, error);
  }
}

try {
  console.log('Workflow scheduler:', startWorkflowScheduler());
} catch (error) {
  console.error('Workflow scheduler start failed:', error);
}

app.listen(port, host, () => {
  console.log(`AI Orchestrator API running on http://${host}:${port}`);
});
