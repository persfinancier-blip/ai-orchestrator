-- Verify staging table compatibility and load status

-- 1) table exists
SELECT
  EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'system'
      AND table_name = 'api_configs_store_stg_wb_work_products'
  ) AS staging_exists;

-- 2) required columns for API builder compatibility
WITH required(name) AS (
  VALUES
    ('id'),
    ('api_name'),
    ('config_json'),
    ('schema_version'),
    ('revision'),
    ('description'),
    ('is_active'),
    ('updated_at'),
    ('updated_by')
)
SELECT
  r.name AS required_column,
  CASE WHEN c.column_name IS NULL THEN false ELSE true END AS present
FROM required r
LEFT JOIN information_schema.columns c
  ON c.table_schema = 'system'
 AND c.table_name = 'api_configs_store_stg_wb_work_products'
 AND c.column_name = r.name
ORDER BY r.name;

-- 3) loaded rows
SELECT count(*)::int AS loaded_rows
FROM "system"."api_configs_store_stg_wb_work_products"
WHERE is_active = true;

-- 4) quick sanity sample
SELECT
  id,
  api_name,
  config_json->>'method' AS method,
  config_json->>'base_url' AS base_url,
  config_json->>'path' AS path,
  updated_at
FROM "system"."api_configs_store_stg_wb_work_products"
ORDER BY updated_at DESC, id DESC
LIMIT 20;

