-- core/orchestrator/modules/advertising/interface/server/tableBuilderSchema.sql
-- Idempotent "upgrade" schema: safely adds missing columns/indexes.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 1) Ensure base table exists
CREATE TABLE IF NOT EXISTS ao_table_definitions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  schema_name text NOT NULL,
  table_name text NOT NULL,
  table_class text NOT NULL,
  status text NOT NULL DEFAULT 'draft'
);

-- 2) Add missing columns (upgrade old schema)
ALTER TABLE ao_table_definitions
  ADD COLUMN IF NOT EXISTS description text NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS options jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS created_by text NOT NULL DEFAULT 'unknown',
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS applied_at timestamptz NULL,
  ADD COLUMN IF NOT EXISTS applied_by text NULL,
  ADD COLUMN IF NOT EXISTS last_error text NULL;

CREATE INDEX IF NOT EXISTS ao_table_definitions_created_at_idx
  ON ao_table_definitions (created_at DESC);

-- 3) Ensure fields table exists
CREATE TABLE IF NOT EXISTS ao_table_fields (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  table_definition_id uuid NOT NULL REFERENCES ao_table_definitions(id) ON DELETE CASCADE,
  field_name text NOT NULL,
  field_type text NOT NULL
);

-- 4) Add missing columns (upgrade old schema)
ALTER TABLE ao_table_fields
  ADD COLUMN IF NOT EXISTS description text NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS ao_table_fields_table_definition_id_idx
  ON ao_table_fields (table_definition_id);

CREATE UNIQUE INDEX IF NOT EXISTS ao_table_fields_unique_name_per_table
  ON ao_table_fields (table_definition_id, field_name);
