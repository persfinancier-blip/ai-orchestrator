// core/orchestrator/modules/advertising/interface/server/spaceServer.mjs
import express from 'express';
import { tableBuilderRouter } from './tableBuilder.mjs';

const app = express();
const port = Number(process.env.SPACE_API_PORT || 8787);

// на всякий случай (если будут новые POST без router.use(json))
app.use(express.json({ limit: '4mb' }));

// всё API висит тут:
app.use('/ai-orchestrator/api', tableBuilderRouter);

// health (для проверок)
app.get('/ai-orchestrator/api/health', (_req, res) => {
  res.json({ ok: true });
});

app.listen(port, () => {
  // eslint-disable-next-line no-console
  console.log(`Space API running on :${port}`);
});
