// core/orchestrator/modules/advertising/interface/server/spaceServer.mjs
import express from 'express';
import { pool } from './db.mjs';
import { tableBuilderRouter } from './tableBuilder.mjs';

const app = express();
const port = Number(process.env.SPACE_API_PORT || 8787);

app.use(express.json({ limit: '2mb' }));

// Table Builder API: /ai-orchestrator/api/tables...
app.use('/ai-orchestrator/api', tableBuilderRouter);

/**
 * GOLD витрина Advertising: showcase.advertising
 *
 * UI читает ТОЛЬКО Gold.
 * “полностью” = полная модель витрины, но отдача всегда порциями/фильтрами.
 *
 * Query:
 * - limit (default 500, max 5000)
 * - offset (default 0)
 * - date_from (optional, YYYY-MM-DD)
 * - date_to   (optional, YYYY-MM-DD)
 * - entity_type (optional)
 * - q (optional search in entity_key/name)
 * - order_by (optional): revenue|spend|orders|drr|roi|date (default date desc)
 * - order_dir (optional): asc|desc (default desc)
 */
app.get('/ai-orchestrator/api/space', async (req, res) => {
  const limit = Math.max(1, Math.min(Number(req.query.limit ?? 500), 5000));
  const offset = Math.max(0, Number(req.query.offset ?? 0));

  const dateFrom = (req.query.date_from ? String(req.query.date_from) : '').trim();
  const dateTo = (req.query.date_to ? String(req.query.date_to) : '').trim();
  const entityType = (req.query.entity_type ? String(req.query.entity_type) : '').trim();
  const q = (req.query.q ? String(req.query.q) : '').trim();

  const allowedOrder = new Set(['revenue', 'spend', 'orders', 'drr', 'roi', 'date']);
  const orderBy = allowedOrder.has(String(req.query.order_by || 'date')) ? String(req.query.order_by || 'date') : 'date';
  const orderDir = String(req.query.order_dir || 'desc').toLowerCase() === 'asc' ? 'ASC' : 'DESC';

  // ВАЖНО: orderBy вставляем как идентификатор только из allowlist
  const sql = `
    SELECT *
    FROM showcase.advertising
    WHERE
      ($1::text = '' OR date >= $1::date)
      AND ($2::text = '' OR date <= $2::date)
      AND ($3::text = '' OR entity_type = $3::text)
      AND (
        $4::text = ''
        OR entity_key ILIKE ('%' || $4::text || '%')
        OR name ILIKE ('%' || $4::text || '%')
      )
    ORDER BY "${orderBy}" ${orderDir} NULLS LAST
    LIMIT $5::int OFFSET $6::int
  `;

  try {
    const result = await pool.query(sql, [dateFrom, dateTo, entityType, q, limit, offset]);
    return res.json({ points: result.rows, limit, offset });
  } catch (error) {
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
