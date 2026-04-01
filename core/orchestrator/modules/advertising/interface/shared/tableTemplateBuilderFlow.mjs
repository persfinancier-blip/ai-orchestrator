import { CLIENT_MODULE_TEMPLATES } from './clientModuleTemplates.mjs';

export const TABLE_FIELD_TYPE_OPTIONS = [
  { value: 'text', label: 'Текст' },
  { value: 'label', label: 'Ярлык / метка' },
  { value: 'int', label: 'Целое число' },
  { value: 'bigint', label: 'Большое целое число' },
  { value: 'numeric', label: 'Число с дробной частью' },
  { value: 'boolean', label: 'Да / нет' },
  { value: 'date', label: 'Дата' },
  { value: 'timestamptz', label: 'Дата и время с часовым поясом' },
  { value: 'jsonb', label: 'JSON-объект' },
  { value: 'json_payload', label: 'JSON-пакет' },
  { value: 'text_payload', label: 'Текстовый пакет' },
  { value: 'csv_text', label: 'CSV-данные' },
  { value: 'zip_archive', label: 'ZIP-архив' },
  { value: 'url', label: 'Ссылка / URL' },
  { value: 'file_ref', label: 'Ссылка на файл' },
  { value: 'table_ref', label: 'Ссылка на таблицу' },
  { value: 'record_ref', label: 'Ссылка на запись' },
  { value: 'external_source_ref', label: 'Ссылка на внешний источник' },
  { value: 'uuid', label: 'UUID' },
  { value: 'bytea', label: 'Бинарные данные' }
];

export const STORAGE_DEFAULT_SCHEMA = 'ao_system';
export const STORAGE_DEFAULT_TABLE = 'table_templates_store';
export const STORAGE_CONTRACT_NAME = 'Хранилище шаблонов таблиц';

export const REQUIRED_TABLE_FIELDS = [
  { field_name: 'ao_source', field_type: 'text', description: 'источник данных (техническое поле)' },
  { field_name: 'ao_run_id', field_type: 'text', description: 'идентификатор запуска (техническое поле)' },
  { field_name: 'ao_created_at', field_type: 'timestamptz', description: 'время создания записи (техническое поле)' },
  { field_name: 'ao_updated_at', field_type: 'timestamptz', description: 'время обновления записи (техническое поле)' },
  { field_name: 'ao_contract_schema', field_type: 'text', description: 'схема таблицы контракта данных' },
  { field_name: 'ao_contract_name', field_type: 'text', description: 'имя контракта данных' },
  { field_name: 'ao_contract_version', field_type: 'int', description: 'версия контракта данных' }
];

const FIELD_TYPE_TO_DB_TYPES = {
  text: ['text', 'character varying', 'varchar'],
  label: ['text', 'character varying', 'varchar'],
  int: ['integer', 'int4', 'int'],
  bigint: ['bigint', 'int8'],
  numeric: ['numeric', 'decimal'],
  boolean: ['boolean', 'bool'],
  date: ['date'],
  timestamptz: ['timestamp with time zone', 'timestamptz'],
  jsonb: ['jsonb', 'json'],
  json_payload: ['jsonb', 'json'],
  text_payload: ['text', 'character varying', 'varchar'],
  csv_text: ['text', 'character varying', 'varchar'],
  zip_archive: ['bytea'],
  url: ['text', 'character varying', 'varchar'],
  table_ref: ['text', 'character varying', 'varchar'],
  record_ref: ['text', 'character varying', 'varchar'],
  file_ref: ['text', 'character varying', 'varchar'],
  external_source_ref: ['text', 'character varying', 'varchar'],
  uuid: ['uuid'],
  bytea: ['bytea']
};

const normalizeTypeName = (type) => String(type || '').toLowerCase().trim();

export function normalizeFieldType(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return 'text';
  if (TABLE_FIELD_TYPE_OPTIONS.some((item) => item.value === raw)) return raw;
  if (raw === 'int' || raw === 'integer' || raw === 'int4') return 'int';
  if (raw === 'bigint' || raw === 'int8' || raw === 'bigserial') return 'bigint';
  if (raw.startsWith('numeric') || raw.startsWith('decimal')) return 'numeric';
  if (raw === 'boolean' || raw === 'bool') return 'boolean';
  if (raw === 'date') return 'date';
  if (raw === 'timestamptz' || raw.includes('timestamp with time zone')) return 'timestamptz';
  if (raw === 'json' || raw === 'jsonb') return 'jsonb';
  if (raw === 'text' || raw === 'varchar' || raw.includes('character varying')) return 'text';
  if (raw === 'uuid') return 'uuid';
  if (raw === 'bytea') return 'bytea';
  return 'text';
}

export function normalizeColumns(cols = []) {
  return (Array.isArray(cols) ? cols : [])
    .map((c) => ({
      field_name: String(c?.field_name || '').trim(),
      field_type: normalizeFieldType(c?.field_type),
      description: String(c?.description || '').trim()
    }))
    .filter((c) => c.field_name.length > 0);
}

export function withRequiredTableFields(cols = []) {
  const base = (Array.isArray(cols) ? cols : []).map((c) => ({
    field_name: String(c?.field_name || '').trim(),
    field_type: String(c?.field_type || 'text').trim(),
    description: String(c?.description || '').trim()
  }));
  const byName = new Map(base.map((c) => [c.field_name.toLowerCase(), c]));
  const requiredFirst = REQUIRED_TABLE_FIELDS.map((sys) => {
    const hit = byName.get(sys.field_name.toLowerCase());
    return hit ? { ...sys, ...hit } : { ...sys };
  });
  const nonSystem = base.filter(
    (c) => !REQUIRED_TABLE_FIELDS.some((f) => f.field_name.toLowerCase() === c.field_name.toLowerCase())
  );
  return [...requiredFirst, ...nonSystem];
}

export function isRequiredTableField(fieldName) {
  const key = String(fieldName || '').trim().toLowerCase();
  return REQUIRED_TABLE_FIELDS.some((field) => field.field_name.toLowerCase() === key);
}

export function normalizeDataLevel(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'silver') return 'silver';
  if (raw === 'gold') return 'gold';
  return 'bronze';
}

export function normalizeTemplateKind(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'system_log') return 'system_log';
  if (raw === 'workflow_log') return 'workflow_log';
  if (raw === 'system_storage') return 'system_storage';
  return 'data';
}

export function inferDataLevel(schema, tableClass) {
  const cls = String(tableClass || '').trim().toLowerCase();
  const sch = String(schema || '').trim().toLowerCase();
  if (cls.includes('silver') || sch.startsWith('silver')) return 'silver';
  if (cls.includes('gold') || cls.includes('showcase') || sch.startsWith('gold') || sch.startsWith('showcase')) {
    return 'gold';
  }
  return 'bronze';
}

export function inferTemplateKindFromRow(row) {
  const explicit = normalizeTemplateKind(row?.template_kind);
  if (explicit !== 'data') return explicit;
  const tableClass = String(row?.table_class || '').trim().toLowerCase();
  if (tableClass.includes('system_log')) return 'system_log';
  return 'data';
}

export function isBuiltinTemplate(template) {
  return String(template?.id || '').startsWith('builtin_');
}

export function isSystemTemplate(template) {
  return normalizeTemplateKind(template?.template_kind) !== 'data';
}

export function isWorkflowLogTemplate(template) {
  return normalizeTemplateKind(template?.template_kind) === 'workflow_log';
}

export function templateBadgeText(template) {
  if (isSystemTemplate(template)) return 'System';
  if (isBuiltinTemplate(template)) return 'Base';
  return '';
}

function bronzeDataTemplate() {
  return {
    id: 'builtin_bronze_data',
    name: 'Bronze данные',
    schema_name: 'bronze',
    table_name: 'wb_entities_raw',
    table_class: 'bronze_raw',
    data_level: 'bronze',
    template_kind: 'data',
    description: 'Сырые сущности Bronze-слоя. Используется как обычная таблица данных.',
    columns: withRequiredTableFields([
      { field_name: 'entity_key', field_type: 'text', description: 'Технический ключ сущности.' },
      { field_name: 'entity_label', field_type: 'text', description: 'Понятная подпись сущности.' },
      { field_name: 'source_name', field_type: 'text', description: 'Источник, из которого получены данные.' },
      { field_name: 'payload', field_type: 'jsonb', description: 'Сырой JSON сущности.' },
      { field_name: 'received_at', field_type: 'timestamptz', description: 'Когда сущность получена.' },
      { field_name: 'created_at', field_type: 'timestamptz', description: 'Когда строка создана.' },
      { field_name: 'updated_at', field_type: 'timestamptz', description: 'Когда строка обновлена.' }
    ]),
    partition_enabled: true,
    partition_column: 'received_at',
    partition_interval: 'day',
    contract_version: 1,
    contract_mode: 'safe_add_only'
  };
}

function bronzeApiLogSystemTemplate() {
  return {
    id: 'builtin_bronze_system_api_log',
    name: 'Bronze системный лог API',
    schema_name: 'bronze',
    table_name: 'api_step_log',
    table_class: 'bronze_system_log',
    data_level: 'bronze',
    template_kind: 'system_log',
    description: 'Системный журнал шагов API: что отправили, что получили и почему продолжили или остановили запуск.',
    columns: withRequiredTableFields([
      { field_name: 'run_id', field_type: 'text', description: 'ID одного запуска API. По нему собирается прогон целиком.' },
      { field_name: 'api_name', field_type: 'text', description: 'Название API-конфига из конструктора.' },
      { field_name: 'execution_mode', field_type: 'text', description: 'Режим запуска: sync или async.' },
      { field_name: 'sync_planner', field_type: 'text', description: 'Планировщик sync-режима.' },
      { field_name: 'dispatch_mode', field_type: 'text', description: 'Одиночная отправка или группировка.' },
      { field_name: 'entity_key', field_type: 'text', description: 'Технический ключ сущности (нейтральный идентификатор).' },
      { field_name: 'entity_label', field_type: 'text', description: 'Читаемая подпись сущности.' },
      { field_name: 'row_index', field_type: 'int', description: 'Номер строки источника данных.' },
      { field_name: 'wave_no', field_type: 'int', description: 'Номер волны выполнения.' },
      { field_name: 'page_no', field_type: 'int', description: 'Номер шага/страницы в рамках сущности.' },
      { field_name: 'iteration_reason', field_type: 'text', description: 'Причина запуска итерации (initial_request, pagination_updated и т.д.).' },
      { field_name: 'decision', field_type: 'text', description: 'Решение после шага: continue / stop / fail.' },
      { field_name: 'stop_reason', field_type: 'text', description: 'Причина остановки, если шаг завершил цепочку.' },
      { field_name: 'error_message', field_type: 'text', description: 'Текст ошибки шага, если она была.' },
      { field_name: 'status_code', field_type: 'int', description: 'HTTP-статус ответа.' },
      { field_name: 'request_payload', field_type: 'jsonb', description: 'Фактически отправленный запрос.' },
      { field_name: 'response_payload', field_type: 'jsonb', description: 'Фактически полученный ответ.' },
      { field_name: 'pagination_values', field_type: 'jsonb', description: 'Извлеченные значения параметров пагинации.' },
      { field_name: 'duration_ms', field_type: 'int', description: 'Время выполнения шага в миллисекундах.' },
      { field_name: 'created_at', field_type: 'timestamptz', description: 'Когда шаг записан в лог.' },
      { field_name: 'updated_at', field_type: 'timestamptz', description: 'Когда строка лога обновлена.' }
    ]),
    partition_enabled: true,
    partition_column: 'created_at',
    partition_interval: 'day',
    contract_version: 1,
    contract_mode: 'safe_add_only'
  };
}

function bronzeWorkflowLogSystemTemplate() {
  return {
    id: 'builtin_bronze_system_workflow_log',
    name: 'Bronze системный лог workflow',
    schema_name: 'ao_system',
    table_name: 'workflow_execution_log',
    table_class: 'bronze_system_log',
    data_level: 'bronze',
    template_kind: 'workflow_log',
    description:
      'Системный источник журнала выполнения рабочего стола. Это логическая привязка к существующему workflow log, а не шаблон для создания второй таблицы.',
    columns: withRequiredTableFields([
      { field_name: 'run_uid', field_type: 'text', description: 'Уникальный идентификатор запуска процесса.' },
      { field_name: 'desk_id', field_type: 'bigint', description: 'Идентификатор рабочего стола.' },
      { field_name: 'desk_name', field_type: 'text', description: 'Название рабочего стола.' },
      { field_name: 'desk_version_id', field_type: 'bigint', description: 'Опубликованная версия рабочего стола.' },
      { field_name: 'start_node_id', field_type: 'text', description: 'Старт-нода процесса.' },
      { field_name: 'process_code', field_type: 'text', description: 'Внутренний код процесса.' },
      { field_name: 'scope_type', field_type: 'text', description: 'Тип области выполнения.' },
      { field_name: 'scope_ref', field_type: 'text', description: 'Ссылка на область выполнения.' },
      { field_name: 'trigger_source', field_type: 'text', description: 'Кто инициировал запуск.' },
      { field_name: 'trigger_type', field_type: 'text', description: 'Тип запуска: manual / interval / cron / dependency.' },
      { field_name: 'status', field_type: 'text', description: 'Текущий или финальный статус запуска.' },
      { field_name: 'started_at', field_type: 'timestamptz', description: 'Время старта процесса.' },
      { field_name: 'finished_at', field_type: 'timestamptz', description: 'Время завершения процесса.' },
      { field_name: 'duration_ms', field_type: 'int', description: 'Длительность запуска.' },
      { field_name: 'summary_json', field_type: 'jsonb', description: 'Сводка запуска и агрегаты выполнения.' },
      { field_name: 'error_text', field_type: 'text', description: 'Ошибка запуска, если она была.' }
    ]),
    partition_enabled: false,
    partition_column: '',
    partition_interval: 'day',
    contract_version: 1,
    contract_mode: 'safe_add_only'
  };
}

function silverTemplate() {
  return {
    id: 'builtin_silver',
    name: 'Silver',
    schema_name: 'silver_adv',
    table_name: 'wb_ads_daily',
    table_class: 'silver_table',
    data_level: 'silver',
    template_kind: 'data',
    description: 'Дневная агрегированная таблица рекламы',
    columns: withRequiredTableFields([
      { field_name: 'event_date', field_type: 'date', description: 'дата метрики' },
      { field_name: 'campaign_id', field_type: 'text', description: 'идентификатор кампании' },
      { field_name: 'impressions', field_type: 'bigint', description: 'показы' },
      { field_name: 'clicks', field_type: 'bigint', description: 'клики' },
      { field_name: 'spend', field_type: 'numeric', description: 'расход' }
    ]),
    partition_enabled: true,
    partition_column: 'event_date',
    partition_interval: 'day',
    contract_version: 1,
    contract_mode: 'safe_add_only'
  };
}

function storageSystemTemplate() {
  return {
    id: 'builtin_storage_contracts',
    name: STORAGE_CONTRACT_NAME,
    schema_name: STORAGE_DEFAULT_SCHEMA,
    table_name: STORAGE_DEFAULT_TABLE,
    table_class: 'custom',
    data_level: 'bronze',
    template_kind: 'system_storage',
    description: 'Служебная таблица для хранения шаблонов блока «Создание таблиц»',
    columns: withRequiredTableFields([
      { field_name: 'template_name', field_type: 'text', description: 'имя шаблона' },
      { field_name: 'schema_name', field_type: 'text', description: 'схема таблицы' },
      { field_name: 'table_name', field_type: 'text', description: 'имя таблицы' },
      { field_name: 'data_level', field_type: 'text', description: 'уровень данных шаблона: bronze / silver / gold' },
      { field_name: 'template_kind', field_type: 'text', description: 'тип шаблона: data / system_log / workflow_log / system_storage' },
      { field_name: 'table_class', field_type: 'text', description: 'класс таблицы' },
      { field_name: 'description', field_type: 'text', description: 'описание' },
      { field_name: 'columns', field_type: 'jsonb', description: 'json список полей' },
      { field_name: 'partition_enabled', field_type: 'boolean', description: 'включено ли партиционирование' },
      { field_name: 'partition_column', field_type: 'text', description: 'колонка партиционирования' },
      { field_name: 'partition_interval', field_type: 'text', description: 'интервал партиционирования' }
    ]),
    partition_enabled: false,
    partition_column: '',
    partition_interval: 'day',
    contract_version: 1,
    contract_mode: 'safe_add_only'
  };
}

function contractsSystemTemplate() {
  return {
    id: 'builtin_data_contracts_table',
    name: 'Хранилище контрактов данных',
    schema_name: 'ao_system',
    table_name: 'table_data_contract_versions',
    table_class: 'custom',
    data_level: 'bronze',
    template_kind: 'system_storage',
    description: 'Системная таблица версий контрактов данных',
    columns: withRequiredTableFields([
      { field_name: 'id', field_type: 'bigint', description: 'идентификатор версии' },
      { field_name: 'schema_name', field_type: 'text', description: 'схема таблицы' },
      { field_name: 'table_name', field_type: 'text', description: 'имя таблицы' },
      { field_name: 'contract_name', field_type: 'text', description: 'имя контракта' },
      { field_name: 'version', field_type: 'int', description: 'версия контракта' },
      { field_name: 'lifecycle_state', field_type: 'text', description: 'состояние версии' },
      { field_name: 'deleted_at', field_type: 'timestamptz', description: 'время удаления/архивации' },
      { field_name: 'description', field_type: 'text', description: 'описание таблицы' },
      { field_name: 'columns', field_type: 'jsonb', description: 'список колонок' },
      { field_name: 'change_reason', field_type: 'text', description: 'причина изменения' },
      { field_name: 'changed_by', field_type: 'text', description: 'кто изменил' },
      { field_name: 'created_at', field_type: 'timestamptz', description: 'время создания версии' }
    ]),
    partition_enabled: false,
    partition_column: '',
    partition_interval: 'day',
    contract_version: 1,
    contract_mode: 'safe_add_only'
  };
}

function settingsSystemTemplate() {
  return {
    id: 'builtin_settings_table',
    name: 'Таблица настроек',
    schema_name: 'ao_system',
    table_name: 'table_settings_store',
    table_class: 'custom',
    data_level: 'bronze',
    template_kind: 'system_storage',
    description: 'Системная таблица настроек сервера, API и подключений к БД',
    columns: withRequiredTableFields([
      { field_name: 'setting_key', field_type: 'text', description: 'уникальный ключ настройки' },
      { field_name: 'setting_value', field_type: 'jsonb', description: 'значение настройки (json)' },
      { field_name: 'scope', field_type: 'text', description: 'область применения (global/module/env)' },
      { field_name: 'description', field_type: 'text', description: 'описание настройки' },
      { field_name: 'is_active', field_type: 'boolean', description: 'включена ли настройка' },
      { field_name: 'updated_at', field_type: 'timestamptz', description: 'время обновления' },
      { field_name: 'updated_by', field_type: 'text', description: 'кто обновил' }
    ]),
    partition_enabled: false,
    partition_column: '',
    partition_interval: 'day',
    contract_version: 1,
    contract_mode: 'safe_add_only'
  };
}

function serverWritesSystemTemplate() {
  return {
    id: 'builtin_server_writes_table',
    name: 'Таблица серверных записей',
    schema_name: 'ao_system',
    table_name: 'table_server_writes_store',
    table_class: 'custom',
    data_level: 'bronze',
    template_kind: 'system_storage',
    description: 'Системная таблица правил серверных записей в БД',
    columns: withRequiredTableFields([
      { field_name: 'rule_key', field_type: 'text', description: 'уникальный ключ правила' },
      { field_name: 'target_schema', field_type: 'text', description: 'схема таблицы назначения' },
      { field_name: 'target_table', field_type: 'text', description: 'имя таблицы назначения' },
      { field_name: 'operation', field_type: 'text', description: 'тип серверной операции' },
      { field_name: 'payload', field_type: 'jsonb', description: 'параметры операции (json)' },
      { field_name: 'scope', field_type: 'text', description: 'область действия (global/module)' },
      { field_name: 'description', field_type: 'text', description: 'описание правила' },
      { field_name: 'is_active', field_type: 'boolean', description: 'включено ли правило' },
      { field_name: 'updated_at', field_type: 'timestamptz', description: 'время обновления' },
      { field_name: 'updated_by', field_type: 'text', description: 'кто обновил' }
    ]),
    partition_enabled: false,
    partition_column: '',
    partition_interval: 'day',
    contract_version: 1,
    contract_mode: 'safe_add_only'
  };
}

function apiConfigsSystemTemplate() {
  return {
    id: 'builtin_api_configs_table',
    name: 'Хранилище API',
    schema_name: 'ao_system',
    table_name: 'api_configs_store',
    table_class: 'custom',
    data_level: 'bronze',
    template_kind: 'system_storage',
    description: 'Системная таблица API-шаблонов (единый JSON-конфиг + служебные поля)',
    columns: withRequiredTableFields([
      { field_name: 'id', field_type: 'bigint', description: 'идентификатор API-шаблона (автонумерация через sequence на сервере)' },
      { field_name: 'api_name', field_type: 'text', description: 'человекочитаемое название API-шаблона' },
      { field_name: 'config_json', field_type: 'jsonb', description: 'полное описание API-шаблона в JSON (метод, URL, параметры, пагинация, маппинги и т.д.)' },
      { field_name: 'schema_version', field_type: 'int', description: 'версия структуры config_json для миграций' },
      { field_name: 'revision', field_type: 'int', description: 'номер ревизии для защиты от одновременной перезаписи' },
      { field_name: 'description', field_type: 'text', description: 'краткое пояснение назначения шаблона' },
      { field_name: 'is_active', field_type: 'boolean', description: 'активен ли шаблон (мягкое удаление через false)' },
      { field_name: 'updated_at', field_type: 'timestamptz', description: 'время последнего обновления' },
      { field_name: 'updated_by', field_type: 'text', description: 'кто последним обновил шаблон' }
    ]),
    partition_enabled: false,
    partition_column: '',
    partition_interval: 'day',
    contract_version: 1,
    contract_mode: 'safe_add_only'
  };
}

function workflowDesksSystemTemplate() {
  return {
    id: 'builtin_workflow_desks_table',
    name: 'Хранилище рабочих столов',
    schema_name: 'ao_system',
    table_name: 'workflow_desks_store',
    table_class: 'custom',
    data_level: 'bronze',
    template_kind: 'system_storage',
    description: 'Системная таблица рабочих столов: процессы и данные (единый JSON-конфиг)',
    columns: withRequiredTableFields([
      { field_name: 'id', field_type: 'bigint', description: 'идентификатор рабочего стола (автонумерация через sequence на сервере)' },
      { field_name: 'desk_name', field_type: 'text', description: 'название рабочего стола' },
      { field_name: 'desk_type', field_type: 'text', description: 'тип рабочего стола (data/process)' },
      { field_name: 'config_json', field_type: 'jsonb', description: 'полное описание canvas: ноды, связи, viewport, настройки' },
      { field_name: 'schema_version', field_type: 'int', description: 'версия структуры config_json для миграций' },
      { field_name: 'revision', field_type: 'int', description: 'номер ревизии для защиты от одновременной перезаписи' },
      { field_name: 'description', field_type: 'text', description: 'описание рабочего стола' },
      { field_name: 'is_active', field_type: 'boolean', description: 'активен ли рабочий стол (мягкое удаление через false)' },
      { field_name: 'updated_at', field_type: 'timestamptz', description: 'время последнего обновления' },
      { field_name: 'updated_by', field_type: 'text', description: 'кто последним обновил рабочий стол' }
    ]),
    partition_enabled: false,
    partition_column: '',
    partition_interval: 'day',
    contract_version: 1,
    contract_mode: 'safe_add_only'
  };
}

function nodeRegistrySystemTemplate() {
  return {
    id: 'builtin_node_registry_table',
    name: 'Реестр системных нод',
    schema_name: 'ao_system',
    table_name: 'node_registry_store',
    table_class: 'system_registry',
    data_level: 'bronze',
    template_kind: 'system_storage',
    description: 'Системный реестр нод и разделов workflow: названия, порядок, безопасные метаданные показа и служебные признаки.',
    columns: withRequiredTableFields([
      { field_name: 'node_type_code', field_type: 'text', description: 'Служебный код типа ноды.' },
      { field_name: 'node_name_ru', field_type: 'label', description: 'Основное русское название ноды.' },
      { field_name: 'description_ru', field_type: 'text', description: 'Краткое русское описание ноды.' },
      { field_name: 'section_code', field_type: 'text', description: 'Служебный код раздела палитры.' },
      { field_name: 'section_name_ru', field_type: 'label', description: 'Русское название раздела палитры.' },
      { field_name: 'section_order', field_type: 'int', description: 'Порядок раздела в палитре.' },
      { field_name: 'node_order', field_type: 'int', description: 'Порядок ноды внутри раздела.' },
      { field_name: 'is_enabled', field_type: 'boolean', description: 'Показывать ли ноду как доступную.' },
      { field_name: 'is_system', field_type: 'boolean', description: 'Системная ли это нода.' },
      { field_name: 'hidden_in_palette', field_type: 'boolean', description: 'Скрывать ли ноду в палитре.' },
      { field_name: 'node_label_ru', field_type: 'label', description: 'Короткий ярлык ноды.' },
      { field_name: 'icon_key', field_type: 'text', description: 'Ключ иконки из безопасного набора интерфейса.' },
      { field_name: 'visual_preset_key', field_type: 'text', description: 'Ключ визуального пресета ноды.' },
      { field_name: 'editor_type_code', field_type: 'text', description: 'Код редактора или конструктора, который открывается для ноды.' },
      { field_name: 'runtime_handler_code', field_type: 'text', description: 'Код обработчика выполнения на сервере.' },
      { field_name: 'updated_at', field_type: 'timestamptz', description: 'Когда запись в реестре обновили.' },
      { field_name: 'updated_by', field_type: 'text', description: 'Кто обновил запись.' }
    ]),
    partition_enabled: false,
    partition_column: '',
    partition_interval: 'day',
    contract_version: 1,
    contract_mode: 'safe_add_only'
  };
}

export function builtinTableTemplates() {
  return [
    bronzeDataTemplate(),
    bronzeApiLogSystemTemplate(),
    bronzeWorkflowLogSystemTemplate(),
    silverTemplate(),
    storageSystemTemplate(),
    contractsSystemTemplate(),
    settingsSystemTemplate(),
    apiConfigsSystemTemplate(),
    nodeRegistrySystemTemplate(),
    workflowDesksSystemTemplate(),
    serverWritesSystemTemplate(),
    ...CLIENT_MODULE_TEMPLATES.map((item) => ({
      id: item.id,
      name: item.template_name,
      schema_name: item.schema_name,
      table_name: item.table_name,
      table_class: item.table_class,
      data_level: item.data_level,
      template_kind: item.template_kind,
      description: item.description,
      columns: withRequiredTableFields(item.columns),
      partition_enabled: Boolean(item.partition_enabled),
      partition_column: String(item.partition_column || ''),
      partition_interval: String(item.partition_interval || 'day') === 'month' ? 'month' : 'day',
      contract_version: 1,
      contract_mode: 'safe_add_only'
    }))
  ];
}

export function mergeTemplates(builtin, custom) {
  const seen = new Set();
  const out = [];
  for (const item of [...(Array.isArray(builtin) ? builtin : []), ...(Array.isArray(custom) ? custom : [])]) {
    const key = String(item?.name || '').trim().toLowerCase();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
}

export function storageRequiredColumnsFromSystemTemplate() {
  return storageSystemTemplate().columns.map((c) => ({
    name: String(c.field_name || '').trim().toLowerCase(),
    types: FIELD_TYPE_TO_DB_TYPES[String(c.field_type || '').trim()] || [normalizeTypeName(c.field_type || '')]
  }));
}

export function storageInstruction(prefix) {
  return `${prefix} Выберите системный шаблон «${STORAGE_CONTRACT_NAME}», создайте таблицу и подключите ее здесь.`;
}

export async function resolveTemplateStorage(apiBase, apiJson) {
  let schema = STORAGE_DEFAULT_SCHEMA;
  let table = STORAGE_DEFAULT_TABLE;
  try {
    const payload = await apiJson(`${apiBase}/settings/effective`);
    const effective = payload?.effective || {};
    const nextSchema = String(effective?.templates_schema || '').trim();
    const nextTable = String(effective?.templates_table || '').trim();
    if (nextSchema) schema = nextSchema;
    if (nextTable) table = nextTable;
  } catch {}
  return { schema, table };
}

export async function checkTemplateStorageTable(apiBase, apiJson, schema, table) {
  try {
    const result = await apiJson(
      `${apiBase}/columns?schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`
    );
    const cols = Array.isArray(result?.columns) ? result.columns : [];
    if (!cols.length) {
      return {
        ok: false,
        status: 'missing',
        message: storageInstruction('Таблица хранения не найдена или пуста.')
      };
    }
    const map = new Map(cols.map((c) => [String(c.name || '').toLowerCase(), normalizeTypeName(c.type)]));
    const required = storageRequiredColumnsFromSystemTemplate();
    for (const need of required) {
      const actual = map.get(need.name);
      if (!actual || !need.types.some((type) => actual.includes(type))) {
        return {
          ok: false,
          status: 'invalid',
          message: `Структура ${schema}.${table} не подходит: колонка ${need.name} отсутствует или имеет неверный тип.`
        };
      }
    }
    return {
      ok: true,
      status: 'ok',
      message: `Хранилище шаблонов подключено: ${schema}.${table}`
    };
  } catch (error) {
    const message = String(error?.message || '');
    return {
      ok: false,
      status: message.includes('404') ? 'missing' : 'error',
      message: message.includes('404')
        ? storageInstruction(`Таблица ${schema}.${table} не найдена.`)
        : storageInstruction(`Не удалось проверить ${schema}.${table}.`)
    };
  }
}

export function parseStoredTemplateRows(rows) {
  const custom = [];
  for (const row of Array.isArray(rows) ? rows : []) {
    const parsedColumns = Array.isArray(row?.columns)
      ? row.columns
      : (() => {
          try {
            const parsed = JSON.parse(String(row?.columns || '[]'));
            return Array.isArray(parsed) ? parsed : [];
          } catch {
            return [];
          }
        })();
    const name = String(row?.template_name || '').trim();
    if (!name) continue;
    const rawMode = String(row?.contract_mode || 'safe_add_only').trim();
    custom.push({
      id: `stored_${name.toLowerCase()}`,
      name,
      schema_name: String(row?.schema_name || ''),
      table_name: String(row?.table_name || ''),
      table_class: String(row?.table_class || 'custom'),
      data_level: normalizeDataLevel(row?.data_level),
      template_kind: inferTemplateKindFromRow(row),
      description: String(row?.description || ''),
      columns: parsedColumns.map((column) => ({
        field_name: String(column?.field_name || ''),
        field_type: normalizeFieldType(column?.field_type || column?.type || 'text'),
        description: String(column?.description || '')
      })),
      partition_enabled: Boolean(row?.partition_enabled),
      partition_column: String(row?.partition_column || 'event_date'),
      partition_interval: String(row?.partition_interval || 'day') === 'month' ? 'month' : 'day',
      contract_version: Number(row?.contract_version || 1) > 0 ? Number(row?.contract_version || 1) : 1,
      contract_mode: rawMode === 'strict_sync' ? 'strict_sync' : 'safe_add_only',
      storage_ctids: row?.__ctid ? [String(row.__ctid)] : []
    });
  }
  return custom;
}

export async function loadTableTemplatesCatalog(apiBase, apiJson) {
  const storage = await resolveTemplateStorage(apiBase, apiJson);
  const check = await checkTemplateStorageTable(apiBase, apiJson, storage.schema, storage.table);
  if (!check.ok) {
    return {
      templates: builtinTableTemplates(),
      storage_schema: storage.schema,
      storage_table: storage.table,
      storage_status: check.status,
      storage_status_message: check.message
    };
  }
  try {
    const payload = await apiJson(
      `${apiBase}/preview?schema=${encodeURIComponent(storage.schema)}&table=${encodeURIComponent(storage.table)}&limit=5000`
    );
    const rows = Array.isArray(payload?.rows) ? payload.rows : [];
    return {
      templates: mergeTemplates(builtinTableTemplates(), parseStoredTemplateRows(rows)),
      storage_schema: storage.schema,
      storage_table: storage.table,
      storage_status: check.status,
      storage_status_message: check.message
    };
  } catch {
    return {
      templates: builtinTableTemplates(),
      storage_schema: storage.schema,
      storage_table: storage.table,
      storage_status: 'error',
      storage_status_message: storageInstruction('Ошибка загрузки шаблонов из таблицы.')
    };
  }
}

export function buildTemplateColumnsWithUpstreamFields(baseColumns, upstreamFields) {
  const next = withRequiredTableFields(normalizeColumns(baseColumns));
  const seen = new Set(next.map((column) => String(column.field_name || '').trim().toLowerCase()));
  const appended = [];
  for (const field of Array.isArray(upstreamFields) ? upstreamFields : []) {
    const field_name = String(field?.alias || field?.name || '').trim();
    if (!field_name) continue;
    const key = field_name.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    appended.push({
      field_name,
      field_type: normalizeFieldType(field?.type || field?.field_type || 'text'),
      description: String(field?.description || '').trim() || (String(field?.path || '').trim() ? `Источник: ${String(field.path).trim()}` : '')
    });
  }
  return [...next, ...appended];
}
