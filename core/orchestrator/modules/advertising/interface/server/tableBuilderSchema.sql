-- core/orchestrator/modules/advertising/interface/server/tableBuilderSchema.sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS ao_table_definitions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  schema_name text NOT NULL,
  table_name text NOT NULL,
  table_class text NOT NULL,
  status text NOT NULL DEFAULT 'draft',
  description text NOT NULL DEFAULT '',
  options jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_by text NOT NULL DEFAULT 'unknown',
  created_at timestamptz NOT NULL DEFAULT now(),
  applied_at timestamptz NULL,
  applied_by text NULL,
  last_error text NULL
);

CREATE INDEX IF NOT EXISTS ao_table_definitions_created_at_idx
  ON ao_table_definitions (created_at DESC);

CREATE TABLE IF NOT EXISTS ao_table_fields (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  table_definition_id uuid NOT NULL REFERENCES ao_table_definitions(id) ON DELETE CASCADE,
  field_name text NOT NULL,
  field_type text NOT NULL,
  description text NOT NULL DEFAULT '',
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ao_table_fields_table_definition_id_idx
  ON ao_table_fields (table_definition_id);

CREATE UNIQUE INDEX IF NOT EXISTS ao_table_fields_unique_name_per_table
  ON ao_table_fields (table_definition_id, field_name);
