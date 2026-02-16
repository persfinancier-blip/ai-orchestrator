-- core/orchestrator/modules/advertising/interface/server/tableBuilderSchema.sql
-- Table Builder registry (MVP)
-- Run once:
--   create extension if not exists pgcrypto;
--   psql -d ai_orchestrator -f tableBuilderSchema.sql

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS ao_table_drafts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  schema_name text NOT NULL,
  table_name  text NOT NULL,
  table_class text NOT NULL DEFAULT 'custom',
  description text NOT NULL DEFAULT '',
  status      text NOT NULL DEFAULT 'draft', -- draft/applied/failed
  created_at  timestamptz NOT NULL DEFAULT now(),
  created_by  text NOT NULL DEFAULT 'ui',
  applied_at  timestamptz NULL,
  applied_by  text NULL,
  last_error  text NULL,
  UNIQUE (schema_name, table_name)
);

CREATE TABLE IF NOT EXISTS ao_table_draft_columns (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  draft_id uuid NOT NULL REFERENCES ao_table_drafts(id) ON DELETE CASCADE,
  ordinal int NOT NULL,
  column_name text NOT NULL,
  data_type text NOT NULL,         -- e.g. text, bigint, timestamptz, jsonb, numeric(18,2)
  is_nullable boolean NOT NULL DEFAULT true,
  default_expr text NULL,          -- e.g. now(), '{}'::jsonb
  description text NOT NULL DEFAULT '',
  UNIQUE (draft_id, column_name),
  UNIQUE (draft_id, ordinal)
);

-- Helpful view: non-system tables
CREATE OR REPLACE VIEW ao_user_tables AS
SELECT
  n.nspname AS schema_name,
  c.relname AS table_name
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY 1, 2;
