import express from 'express';
import pg from 'pg';

const { Pool } = pg;
const app = express();
const port = Number(process.env.SPACE_API_PORT || 8787);

const pool = new Pool({
  host: process.env.PGHOST || '127.0.0.1',
  port: Number(process.env.PGPORT || 5432),
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
});

app.get('/ai-orchestrator/api/space', async (req, res) => {
  const period = Number(req.query.period || 30);
  const query = `
    with latest as (
      select e.id as entity_id, e.entity_type, e.entity_key, e.name,
             m.date, m.revenue, m.orders, m.spend, m.drr, m.roi, m.cr0, m.cr1, m.cr2,
             m.position, m.stock_days, m.search_share, m.shelf_share,
             row_number() over(partition by e.id order by m.date desc) rn
      from ao_entities e
      join ao_metrics_daily m on m.entity_id = e.id
      where m.date >= current_date - ($1::int || ' days')::interval
    )
    select entity_id as id, entity_type, entity_key, name, to_char(date, 'YYYY-MM-DD') as date,
           revenue, orders, spend, drr, roi, cr0, cr1, cr2, position, stock_days, search_share, shelf_share
    from latest
    where rn = 1
    order by entity_type, entity_key;
  `;

  try {
    const result = await pool.query(query, [period]);
    res.json({ points: result.rows });
  } catch (error) {
    res.status(500).json({ error: 'Ошибка чтения витрины' });
  }
});

app.listen(port, () => {
  console.log(`Space API запущен: http://localhost:${port}/ai-orchestrator/api/space`);
});
