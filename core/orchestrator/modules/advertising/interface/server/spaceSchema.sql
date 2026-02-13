create table if not exists ao_entities (
  id serial primary key,
  entity_type text not null,
  entity_key text not null,
  name text not null
);

create table if not exists ao_metrics_daily (
  id serial primary key,
  entity_id int references ao_entities(id),
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
  shelf_share numeric
);
