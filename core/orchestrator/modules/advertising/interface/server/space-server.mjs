import express from 'express';
import { pool } from './db.mjs';

const app = express();
const port = Number(process.env.SPACE_API_PORT || 8787);

const allowedTextOps = new Set(['равно', 'не равно', 'содержит', 'не содержит', 'в списке']);
const allowedNumOps = new Set(['>', '<', '≥', '≤', 'между']);

app.get('/ai-orchestrator/api/space', async (req, res) => {
  try {
    const entityField = String(req.query.entityField || 'sku');
    const x = String(req.query.x || 'revenue');
    const y = String(req.query.y || 'spend');
    const z = String(req.query.z || 'roi');
    const period = String(req.query.period || '30 дней');
    const filters = JSON.parse(String(req.query.filters || '[]'));

    const params = [];
    const where = [];

    if (entityField === 'sku') where.push(`e.entity_type = 'sku'`);
    if (entityField === 'campaign_id') where.push(`e.entity_type = 'campaign'`);

    if (period === '7 дней') where.push(`m.date >= current_date - interval '7 day'`);
    if (period === '14 дней') where.push(`m.date >= current_date - interval '14 day'`);
    if (period === '30 дней') where.push(`m.date >= current_date - interval '30 day'`);
    if (period === 'Даты') {
      if (req.query.fromDate) {
        params.push(String(req.query.fromDate));
        where.push(`m.date >= $${params.length}::date`);
      }
      if (req.query.toDate) {
        params.push(String(req.query.toDate));
        where.push(`m.date <= $${params.length}::date`);
      }
    }

    for (const filter of filters) {
      if (filter.filterType === 'date' && filter.valueA && filter.valueB) {
        params.push(filter.valueA, filter.valueB);
        where.push(`m.date between $${params.length - 1}::date and $${params.length}::date`);
      }
      if (filter.filterType === 'text' && allowedTextOps.has(filter.operator)) {
        const col = filter.fieldCode === 'campaign_id' ? 'e.entity_key' : filter.fieldCode;
        if (filter.operator === 'равно') { params.push(filter.valueA); where.push(`${col} = $${params.length}`); }
        if (filter.operator === 'не равно') { params.push(filter.valueA); where.push(`${col} != $${params.length}`); }
        if (filter.operator === 'содержит') { params.push(`%${filter.valueA}%`); where.push(`${col} ilike $${params.length}`); }
        if (filter.operator === 'не содержит') { params.push(`%${filter.valueA}%`); where.push(`${col} not ilike $${params.length}`); }
        if (filter.operator === 'в списке') {
          params.push(filter.valueA.split(',').map((v) => v.trim()));
          where.push(`${col} = any($${params.length})`);
        }
      }
      if (filter.filterType === 'number' && allowedNumOps.has(filter.operator)) {
        const col = `m.${filter.fieldCode}`;
        if (filter.operator !== 'между') {
          params.push(Number(filter.valueA));
          const op = filter.operator === '≥' ? '>=' : filter.operator === '≤' ? '<=' : filter.operator;
          where.push(`${col} ${op} $${params.length}`);
        } else {
          params.push(Number(filter.valueA), Number(filter.valueB));
          where.push(`${col} between $${params.length - 1} and $${params.length}`);
        }
      }
    }

    const sql = `
      select
        e.id,
        e.entity_type,
        e.entity_key,
        e.name,
        max(m.date)::date as date,
        max(case when e.entity_type='sku' then e.entity_key else '' end) as sku,
        max(case when e.entity_type='campaign' then e.entity_key else '' end) as campaign_id,
        max(case when e.entity_type='sku' then 'Товар' else 'РК' end) as subject,
        max(case when e.entity_type='sku' then 'SKU-категория' else 'РК-категория' end) as category,
        max(case when e.entity_type='sku' then 'Бренд A' else 'Бренд B' end) as brand,
        max(case when e.entity_type='sku' then 'Поиск' else 'Категория' end) as entry_point,
        max(case when e.entity_type='sku' then 'Кластер SKU' else 'Кластер РК' end) as keyword_cluster,
        max(case when e.entity_type='sku' then 'sku запрос' else 'рк запрос' end) as search_query,
        avg(m.revenue)::float as revenue,
        avg(m.orders)::float as orders,
        avg(m.spend)::float as spend,
        avg(m.drr)::float as drr,
        avg(m.roi)::float as roi,
        avg(m.cr0)::float as cr0,
        avg(m.cr1)::float as cr1,
        avg(m.cr2)::float as cr2,
        avg(m.position)::float as position,
        avg(m.stock_days)::float as stock_days,
        avg(m.search_share)::float as search_share,
        avg(m.shelf_share)::float as shelf_share
      from ao_entities e
      join ao_metrics_daily m on m.entity_id = e.id
      ${where.length ? `where ${where.join(' and ')}` : ''}
      group by e.id
      order by e.id
      limit 20
    `;

    const { rows } = await pool.query(sql, params);

    res.json({
      points: rows.map((r) => ({
        id: String(r.id),
        entityType: r.entity_type,
        entityKey: r.entity_key,
        name: r.name,
        date: String(r.date),
        sku: r.sku,
        campaign_id: r.campaign_id,
        subject: r.subject,
        category: r.category,
        brand: r.brand,
        entry_point: r.entry_point,
        keyword_cluster: r.keyword_cluster,
        search_query: r.search_query,
        metrics: { revenue: r.revenue, orders: r.orders, spend: r.spend, drr: r.drr, roi: r.roi, cr0: r.cr0, cr1: r.cr1, cr2: r.cr2, position: r.position, stock_days: r.stock_days, search_share: r.search_share, shelf_share: r.shelf_share, [x]: r[x], [y]: r[y], [z]: r[z] },
      })),
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'space_api_error' });
  }
});

app.listen(port, () => console.log(`Space API на порту ${port}`));
