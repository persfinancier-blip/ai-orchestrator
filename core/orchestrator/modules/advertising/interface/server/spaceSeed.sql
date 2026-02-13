truncate table ao_metrics_daily restart identity;
truncate table ao_entities restart identity cascade;

insert into ao_entities(entity_type, entity_key, name)
select 'sku', format('SKU %s', to_char(i, 'FM0000')), format('Артикул %s', to_char(i, 'FM0000'))
from generate_series(1, 10) i;

insert into ao_entities(entity_type, entity_key, name)
select 'campaign', format('РК %s', 1000 + i), format('Рекламная кампания %s', 1000 + i)
from generate_series(1, 10) i;

with base as (
  select id,
         case
           when id <= 4 then 1
           when id <= 8 then 2
           else 3
         end as cluster_id
  from ao_entities
),
series as (
  select generate_series(current_date - interval '29 day', current_date, interval '1 day')::date as d
)
insert into ao_metrics_daily(entity_id, date, revenue, orders, spend, drr, roi, cr0, cr1, cr2, position, stock_days, search_share, shelf_share)
select
  b.id,
  s.d,
  round((case b.cluster_id when 1 then 180000 when 2 then 120000 else 70000 end + random()*15000)::numeric, 2) as revenue,
  (case b.cluster_id when 1 then 420 when 2 then 280 else 160 end + (random()*30)::int) as orders,
  round((case b.cluster_id when 1 then 42000 when 2 then 28000 else 26000 end + random()*6000)::numeric, 2) as spend,
  round((8 + random()*18)::numeric, 2) as drr,
  round((1.1 + random()*2.4)::numeric, 2) as roi,
  round((1.5 + random()*4)::numeric, 2) as cr0,
  round((0.9 + random()*2.4)::numeric, 2) as cr1,
  round((0.4 + random()*1.8)::numeric, 2) as cr2,
  (3 + (random()*25)::int) as position,
  (10 + (random()*45)::int) as stock_days,
  round((0.25 + random()*0.6)::numeric, 4) as search_share,
  round((0.15 + random()*0.6)::numeric, 4) as shelf_share
from base b
cross join series s;
