insert into ao_entities (entity_type, entity_key, name)
select 'sku', format('SKU %s', to_char(g, 'FM0000')), format('Артикул %s', to_char(g, 'FM0000'))
from generate_series(1, 10) g
on conflict (entity_key) do nothing;

insert into ao_entities (entity_type, entity_key, name)
select 'campaign', format('РК %s', 1000 + g), format('Рекламная кампания %s', 1000 + g)
from generate_series(1, 10) g
on conflict (entity_key) do nothing;

with entities as (
  select id, entity_type,
         row_number() over (partition by entity_type order by id) as idx
  from ao_entities
),
days as (
  select (current_date - interval '29 day' + (n || ' day')::interval)::date as dt, n
  from generate_series(0, 29) n
)
insert into ao_metrics_daily (
  entity_id, date, revenue, orders, spend, drr, roi, cr0, cr1, cr2, position, stock_days, search_share, shelf_share
)
select
  e.id,
  d.dt,
  round((base_rev + d.n * growth + ((random() - 0.5) * 2000))::numeric, 2),
  (base_ord + d.n * ord_growth + floor(random() * 8))::int,
  round((base_spend + d.n * spend_growth + ((random() - 0.5) * 500))::numeric, 2),
  round((20 + grp * 6 + random() * 4)::numeric, 2),
  round((1.2 + grp * 0.8 + random() * 0.7)::numeric, 2),
  round((2 + grp * 1.1 + random() * 2)::numeric, 2),
  round((3 + grp * 1.2 + random() * 2)::numeric, 2),
  round((4 + grp * 1.3 + random() * 2)::numeric, 2),
  (10 + grp * 12 + floor(random() * 6))::int,
  (7 + grp * 9 + floor(random() * 5))::int,
  round((30 + grp * 20 + random() * 10)::numeric, 2),
  round((25 + grp * 18 + random() * 10)::numeric, 2)
from (
  select *,
    case when idx <= 3 then 0 when idx <= 7 then 1 else 2 end as grp,
    case when idx <= 3 then 30000 when idx <= 7 then 60000 else 90000 end as base_rev,
    case when idx <= 3 then 100 when idx <= 7 then 180 else 260 end as base_ord,
    case when idx <= 3 then 12000 when idx <= 7 then 22000 else 34000 end as base_spend,
    case when idx <= 3 then 420 when idx <= 7 then 620 else 780 end as growth,
    case when idx <= 3 then 2 when idx <= 7 then 3 else 4 end as ord_growth,
    case when idx <= 3 then 110 when idx <= 7 then 170 else 250 end as spend_growth
  from entities
) e
cross join days d
on conflict (entity_id, date) do update
set revenue = excluded.revenue,
    orders = excluded.orders,
    spend = excluded.spend,
    drr = excluded.drr,
    roi = excluded.roi,
    cr0 = excluded.cr0,
    cr1 = excluded.cr1,
    cr2 = excluded.cr2,
    position = excluded.position,
    stock_days = excluded.stock_days,
    search_share = excluded.search_share,
    shelf_share = excluded.shelf_share;
