import test from 'node:test';
import assert from 'node:assert/strict';

process.env.PGUSER = process.env.PGUSER || 'test';
process.env.PGPASSWORD = process.env.PGPASSWORD || 'test';
process.env.PGDATABASE = process.env.PGDATABASE || 'test';

const {
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
  assert.equal(rows.length >= 5, true);
  assert.deepEqual(
    rows.filter((row) => !row.hidden_in_palette).map((row) => row.node_type_code),
    ['start_process', 'api_request', 'table_parser', 'db_write', 'end_process']
  );
  assert.deepEqual(
    rows.filter((row) => !row.hidden_in_palette).map((row) => row.section_name_ru),
    ['Старт', 'Запросы', 'Работа с данными', 'Запись', 'Завершение']
  );
  assert.equal(rows.find((row) => row.node_type_code === 'table_parser')?.node_name_ru, 'Парсер данных');
  assert.equal(rows.find((row) => row.node_type_code === 'api_request')?.node_label_ru, 'API');
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

