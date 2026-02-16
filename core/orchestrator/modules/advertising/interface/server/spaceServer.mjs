// core/orchestrator/modules/advertising/interface/server/spaceServer.mjs
import express from 'express';
import { pool } from './db.mjs';
import { tableBuilderRouter } from './tableBuilder.mjs';

const app = express();
const port = Number(process.env.SPACE_API_PORT || 8787);

app.use('/ai-orchestrator/api', tableBuilderRouter);

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
    res.json({ points: result.rows });
  } catch (error) {
    res.status(500).json({ error: 'Ошибка чтения витрины' });
  }
});

app.listen(port, () => {
  // eslint-disable-next-line no-console
  console.log(`Space API running on :${port}`);
});
