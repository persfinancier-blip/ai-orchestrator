// core/orchestrator/modules/advertising/interface/server/spaceServer.mjs

import express from 'express';
import { pool } from './db.mjs';
import { tableBuilderRouter } from './tableBuilder.mjs';

const app = express();
const port = Number(process.env.SPACE_API_PORT || 8787);

app.use(express.json({ limit: '2mb' }));

// Table Builder routes:
// - GET  /ai-orchestrator/api/tables
// - POST /ai-orchestrator/api/tables/create
// - GET  /ai-orchestrator/api/preview
app.use('/ai-orchestrator/api', tableBuilderRouter);

/**
 * GOLD витрина Advertising: showcase.advertising
 * Таблица может быть пустой -> вернём [] (это ок)
 *
 * Query:
 * - limit (default 500, max 5000)
 * - offset (default 0)
 */
async function handleSpace(req, res) {
  const limit = Math.max(1, Math.min(Number(req.query.limit ?? 500), 5000));
  const offset = Math.max(0, Number(req.query.offset ?? 0));

  const sql = `
    SELECT *
    FROM showcase.advertising
    LIMIT $1::int OFFSET $2::int
  `;

  try {
    const result = await pool.query(sql, [limit, offset]);
    return res.json({ points: result.rows, limit, offset });
  } catch (error) {
    // чтобы увидеть реальную причину в pm2 logs
    // eslint-disable-next-line no-console
    console.error('showcase.advertising read error:', error);
    return res.status(500).json({ error: 'Ошибка чтения витрины' });
  }
}

// поддержка /space и /space/
app.get('/ai-orchestrator/api/space', handleSpace);
app.get('/ai-orchestrator/api/space/', handleSpace);

// здоровье API (workflow может проверять это)
app.get('/ai-orchestrator/api/health', (_req, res) => {
  res.json({ ok: true, port });
});

app.listen(port, () => {
  // eslint-disable-next-line no-console
  console.log(`Space API running on :${port}`);
});
