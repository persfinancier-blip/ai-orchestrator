-- Safe transfer from staging to main API templates storage
-- Source:  system.api_configs_store_stg_wb_work_products
-- Target:  system.api_configs_store
-- Rule:    upsert by (config_json.method, config_json.base_url, config_json.path)

DO $$
DECLARE
  rec RECORD;
  matched_id bigint;
BEGIN
  FOR rec IN
    SELECT
      s.api_name,
      s.config_json,
      s.description,
      COALESCE(s.schema_version, 1) AS schema_version,
      COALESCE(s.is_active, true) AS is_active
    FROM "system"."api_configs_store_stg_wb_work_products" s
    WHERE COALESCE(s.is_active, true) = true
    ORDER BY s.id
  LOOP
    SELECT t.id INTO matched_id
    FROM "system"."api_configs_store" t
    WHERE UPPER(COALESCE(t.config_json->>'method', '')) = UPPER(COALESCE(rec.config_json->>'method', ''))
      AND COALESCE(t.config_json->>'base_url', '') = COALESCE(rec.config_json->>'base_url', '')
      AND COALESCE(t.config_json->>'path', '') = COALESCE(rec.config_json->>'path', '')
    ORDER BY t.updated_at DESC NULLS LAST, t.id DESC
    LIMIT 1;

    IF matched_id IS NULL THEN
      INSERT INTO "system"."api_configs_store"
        (api_name, config_json, schema_version, revision, description, is_active, updated_at, updated_by)
      VALUES
        (
          rec.api_name,
          rec.config_json,
          rec.schema_version,
          1,
          rec.description,
          rec.is_active,
          now(),
          'wb_staging_transfer'
        );
    ELSE
      UPDATE "system"."api_configs_store"
      SET
        api_name = rec.api_name,
        config_json = rec.config_json,
        schema_version = rec.schema_version,
        revision = CASE WHEN COALESCE(revision, 0) < 1 THEN 1 ELSE revision + 1 END,
        description = rec.description,
        is_active = rec.is_active,
        updated_at = now(),
        updated_by = 'wb_staging_transfer'
      WHERE id = matched_id;
    END IF;
  END LOOP;
END $$;

