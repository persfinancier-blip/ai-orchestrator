import express from 'express';
import { workflowAutomationRouter } from './workflowAutomation.mjs';
import { buildControlAuth } from './runtime/security.mjs';
import { runtimeCapabilitiesHandler } from './runtime/capabilities.mjs';

const app = express();
const host = String(process.env.AO_CONTROL_HOST || '127.0.0.1').trim() || '127.0.0.1';
const port = Math.max(1, Number(process.env.AO_CONTROL_PORT || 8788));
const controlAuth = buildControlAuth();

app.use(express.json({ limit: '4mb' }));
app.get('/ai-orchestrator/control/health', (_req, res) => {
  res.json({ ok: true, service: 'workflow-control-plane', host, port });
});
app.get('/ai-orchestrator/control/capabilities', runtimeCapabilitiesHandler);
app.use('/ai-orchestrator/control', controlAuth, workflowAutomationRouter);

app.listen(port, host, () => {
  // eslint-disable-next-line no-console
  console.log(`Workflow control plane running on http://${host}:${port}`);
});
