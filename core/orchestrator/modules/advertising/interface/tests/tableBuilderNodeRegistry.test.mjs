import test from 'node:test';
import assert from 'node:assert/strict';

process.env.PGUSER = process.env.PGUSER || 'test';
process.env.PGPASSWORD = process.env.PGPASSWORD || 'test';
process.env.PGDATABASE = process.env.PGDATABASE || 'test';

const {
  DEFAULT_API_CONFIG_ROWS,
  DEFAULT_NODE_REGISTRY_ROWS,
  fieldTypeToSql,
  materializeNodeRegistryRow
} = await import('../server/tableBuilder.mjs');

test('table builder: new practical field types map to safe SQL types', () => {
  assert.equal(fieldTypeToSql('label'), 'text');
  assert.equal(fieldTypeToSql('csv_text'), 'text');
  assert.equal(fieldTypeToSql('zip_archive'), 'bytea');
  assert.equal(fieldTypeToSql('url'), 'text');
  assert.equal(fieldTypeToSql('table_ref'), 'text');
  assert.equal(fieldTypeToSql('record_ref'), 'text');
  assert.equal(fieldTypeToSql('file_ref'), 'text');
  assert.equal(fieldTypeToSql('external_source_ref'), 'text');
  assert.equal(fieldTypeToSql('json_payload'), 'jsonb');
  assert.equal(fieldTypeToSql('text_payload'), 'text');
  assert.equal(fieldTypeToSql('integer'), 'integer');
  assert.equal(fieldTypeToSql('bytea'), 'bytea');
});

test('table builder: node registry seed rows keep canonical order and russian labels', () => {
  const rows = DEFAULT_NODE_REGISTRY_ROWS;
  assert.equal(rows.length >= 13, true);
  assert.deepEqual(
    rows.filter((row) => !row.hidden_in_palette).map((row) => row.node_type_code),
    [
      'start_process',
      'api_request',
      'http_request',
      'api_mutation',
      'condition_if',
      'condition_switch',
      'table_parser',
      'split_data',
      'merge_data',
      'table_node',
      'action_prep',
      'code_node',
      'db_write',
      'end_process'
    ]
  );
  assert.deepEqual(
    rows.filter((row) => !row.hidden_in_palette).map((row) => row.section_name_ru),
    ['Старт', 'Запросы', 'Запросы', 'Запросы', 'Логика', 'Логика', 'Работа с данными', 'Работа с данными', 'Работа с данными', 'Работа с данными', 'Работа с данными', 'Инструменты', 'Запись', 'Завершение']
  );
  assert.equal(rows.find((row) => row.node_type_code === 'table_node')?.node_name_ru, 'Табличный набор');
  assert.equal(rows.find((row) => row.node_type_code === 'table_parser')?.node_name_ru, 'Парсер данных');
  assert.equal(rows.find((row) => row.node_type_code === 'api_request')?.node_label_ru, 'API');
  assert.equal(rows.find((row) => row.node_type_code === 'api_mutation')?.node_name_ru, 'API-изменение');
  assert.equal(rows.find((row) => row.node_type_code === 'http_request')?.node_name_ru, 'HTTP-запрос');
  assert.equal(rows.find((row) => row.node_type_code === 'condition_if')?.node_name_ru, 'Если');
  assert.equal(rows.find((row) => row.node_type_code === 'condition_switch')?.node_name_ru, 'Переключатель');
  assert.equal(rows.find((row) => row.node_type_code === 'split_data')?.node_name_ru, 'Разделить данные');
  assert.equal(rows.find((row) => row.node_type_code === 'merge_data')?.node_name_ru, 'Объединить данные');
  assert.equal(rows.find((row) => row.node_type_code === 'action_prep')?.node_name_ru, 'Подготовка действий');
  assert.equal(rows.find((row) => row.node_type_code === 'code_node')?.node_name_ru, 'Код');
});

test('table builder: node registry materializer normalizes row payload', () => {
  const materialized = materializeNodeRegistryRow({
    id: '12',
    node_type_code: ' table_parser ',
    node_name_ru: ' Парсер данных ',
    description_ru: ' Тестовое описание ',
    section_code: ' data_processing ',
    section_name_ru: ' Работа с данными ',
    section_order: '30',
    node_order: '15',
    is_enabled: 1,
    is_system: true,
    hidden_in_palette: 0,
    node_label_ru: ' Парсер ',
    icon_key: ' table_parser ',
    visual_preset_key: ' data ',
    editor_type_code: ' parser_builder ',
    runtime_handler_code: ' table_parser ',
    updated_at: '2026-03-08T10:00:00.000Z',
    updated_by: ' system '
  });

  assert.deepEqual(materialized, {
    id: 12,
    node_type_code: 'table_parser',
    node_name_ru: 'Парсер данных',
    description_ru: 'Тестовое описание',
    section_code: 'data_processing',
    section_name_ru: 'Работа с данными',
    section_order: 30,
    node_order: 15,
    is_enabled: true,
    is_system: true,
    hidden_in_palette: false,
    node_label_ru: 'Парсер',
    icon_key: 'table_parser',
    visual_preset_key: 'data',
    editor_type_code: 'parser_builder',
    runtime_handler_code: 'table_parser',
    updated_at: '2026-03-08T10:00:00.000Z',
    updated_by: 'system'
  });
});

test('table builder: built-in Ozon Performance Campaign API templates are executable request configs', () => {
  const rows = DEFAULT_API_CONFIG_ROWS.filter(
    (row) => row.config_json?.mapping_json?.source?.source_slug === 'ozon_performance_campaign'
  );
  assert.equal(rows.length, 5);
  assert.deepEqual(
    rows.map((row) => `${row.config_json.method} ${row.config_json.path}`),
    [
      'GET /api/client/campaign',
      'GET /campaign/available',
      'GET /api/client/campaign/all_sku_promo/activate',
      'POST /api/client/campaign/search_promo/products',
      'POST /api/client/search_promo/product/enable'
    ]
  );
  for (const row of rows) {
    const cfg = row.config_json;
    assert.equal(cfg.base_url, 'https://api-performance.ozon.ru');
    assert.equal(cfg.auth_mode, 'manual');
    assert.equal(cfg.headers_json.Authorization, 'Bearer {{ozon_performance_token}}');
    assert.equal(cfg.pagination_json.enabled, false);
    assert.equal(cfg.execution_json.dispatch_mode, 'single');
    assert.equal(cfg.execution_json.execution_mode, 'sync');
    assert.equal(cfg.mapping_json.tag, 'Campaign');
    assert.equal(cfg.is_active, true);
  }
});
