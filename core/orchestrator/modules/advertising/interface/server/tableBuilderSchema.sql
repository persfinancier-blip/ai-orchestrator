-- core/orchestrator/modules/advertising/interface/server/tableBuilderSchema.sql

CREATE EXTENSION IF NOT EXISTS pgcrypto;

create table if not exists ao_table_definitions (
  id uuid primary key default gen_random_uuid(),
  schema_name text not null,
  table_name text not null,
  table_class text not null, -- bronze_raw | silver_table | showcase_table
  status text not null default 'draft', -- draft | applied
  options jsonb not null default '{}'::jsonb,
  created_by text not null default 'unknown',
  created_at timestamptz not null default now(),
  applied_at timestamptz null
);

create table if not exists ao_table_fields (
  id uuid primary key default gen_random_uuid(),
  table_definition_id uuid not null references ao_table_definitions(id) on delete cascade,
  field_name text not null,
  field_type text not null,
  description text not null,
  created_at timestamptz not null default now()
);

create table if not exists ao_migrations_applied (
  id uuid primary key default gen_random_uuid(),
  table_definition_id uuid not null references ao_table_definitions(id) on delete restrict,
  schema_name text not null,
  table_name text not null,
  migration_sql text not null,
  applied_by text not null default 'unknown',
  applied_at timestamptz not null default now()
);

create index if not exists idx_ao_table_definitions_schema_table
  on ao_table_definitions(schema_name, table_name);

create index if not exists idx_ao_table_fields_definition
  on ao_table_fields(table_definition_id);
