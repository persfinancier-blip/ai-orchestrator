// core/orchestrator/modules/advertising/interface/server/spaceServer.mjs
import express from 'express';
import { pool } from './db.mjs';
import { tableBuilderRouter } from './tableBuilder.mjs';

const app = express();
const port = Number(process.env.SPACE_API_PORT || 8787);

// ✅ ВАЖНО: это должно быть ДО роутов и ДО tableBuilderRouter
app.use(express.json({ limit: '2mb' }));

// Table Builder API (будет обслуживать /ai-orchestrator/api/tables, /tables/draft, etc.)
app.use('/ai-orchestrator/api', tableBuilderRouter);

// Простая витрина (пример)
app.get('/ai-orchestrator/api/space', async (req, res) => {
  const period = Number(req.query.period || 30);

  const query = `
    WITH date_series AS (
      SELECT generate_series(
        CURRENT_DATE - $1::int,
        CURRENT_DATE,
        '1 day'::interval
      )::date AS date
    ),
    aggregated AS (
      SELECT
        date_trunc('day', created_at)::date AS date,
        count(*) AS count
      FROM ao_events
      WHERE created_at >= CURRENT_DATE - $1::int
      GROUP BY 1
    )
    SELECT
      ds.date,
      COALESCE(a.count, 0) AS count
    FROM date_series ds
    LEFT JOIN aggregated a ON a.date = ds.date
    ORDER BY ds.date;
  `;

  try {
    const result = await pool.query(query, [period]);
    return res.json({ points: result.rows });
  } catch (error) {
    // почему: чтобы в pm2 logs была реальная причина 500
    // eslint-disable-next-line no-console
    console.error('space endpoint error:', error);
    return res.status(500).json({ error: 'Ошибка чтения витрины' });
  }
});

// Healthcheck (удобно для деплоя)
app.get('/ai-orchestrator/api/health', (_req, res) => {
  res.json({ ok: true });
});

app.listen(port, () => {
  // eslint-disable-next-line no-console
  console.log(`Space API running on :${port}`);
});
