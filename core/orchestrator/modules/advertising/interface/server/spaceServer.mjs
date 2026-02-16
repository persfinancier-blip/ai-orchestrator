// core/orchestrator/modules/advertising/interface/server/spaceServer.mjs

import express from 'express';
import { pool } from './db.mjs';
import { tableBuilderRouter } from './tableBuilder.mjs';

const app = express();
const port = Number(process.env.SPACE_API_PORT || 8787);

// Нужно для POST/PUT (Table Builder и будущие конфиги)
app.use(express.json({ limit: '2mb' }));

// Table Builder API будет жить тут: /ai-orchestrator/api/tables ...
app.use('/ai-orchestrator/api', tableBuilderRouter);

/**
 * GOLD витрина Advertising: showcase.advertising
 * Таблица пустая — будет возвращать [] и это ок.
 *
 * Query:
 * - limit (default 500, max 5000)
 * - offset (default 0)
 */
app.get('/ai-orchestrator/api/space', async (req, res) => {
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
    // why: чтобы увидеть реальную причину в pm2 logs
    // eslint-disable-next-line no-console
    console.error('gold showcase.advertising read error:', error);
    return res.status(500).json({ error: 'Ошибка чтения витрины' });
  }
});

app.get('/ai-orchestrator/api/health', (_req, res) => {
  res.json({ ok: true, port });
});

app.listen(port, () => {
  // eslint-disable-next-line no-console
  console.log(`Space API running on :${port}`);
});
