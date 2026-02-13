create table if not exists ao_entities (
  id serial primary key,
  entity_type text not null check (entity_type in ('sku', 'campaign')),
  entity_key text not null unique,
  name text not null
);

create table if not exists ao_metrics_daily (
  id serial primary key,
  entity_id int not null references ao_entities(id) on delete cascade,
  date date not null,
  revenue numeric,
  orders int,
  spend numeric,
  drr numeric,
  roi numeric,
  cr0 numeric,
  cr1 numeric,
  cr2 numeric,
  position int,
  stock_days int,
  search_share numeric,
  shelf_share numeric,
  unique (entity_id, date)
);
