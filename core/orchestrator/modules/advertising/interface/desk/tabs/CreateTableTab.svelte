<!-- File: core/orchestrator/modules/advertising/interface/desk/tabs/CreateTableTab.svelte -->
<script lang="ts">
  import { onMount, tick } from 'svelte';

  export type Role = 'viewer' | 'operator' | 'data_admin';
  export type ExistingTable = { schema_name: string; table_name: string };

  export let apiBase: string;
  export let role: Role;
  export let loading: boolean;

  export let refreshTables: () => Promise<void>;
  export let hardReloadConstructor: (() => Promise<void>) | undefined = undefined;
  export let onCreated: (schema: string, table: string) => void;

  export let headers: () => Record<string, string>;
  export let apiJson: <T = any>(url: string, init?: RequestInit) => Promise<T>;
  export let existingTables: ExistingTable[] = [];

  type ColumnDef = { field_name: string; field_type: string; description?: string };
  type DataContract = {
    id: string;
    name: string;
    schema_name: string;
    table_name: string;
    table_class: string;
    description: string;
    columns: ColumnDef[];
    partition_enabled: boolean;
    partition_column: string;
    partition_interval: 'day' | 'month';
    contract_version: number;
    contract_mode: 'safe_add_only' | 'strict_sync';
    storage_ctids?: string[];
  };

  const typeOptions = ['text', 'int', 'bigint', 'numeric', 'boolean', 'date', 'timestamptz', 'jsonb', 'uuid'];
  const CREATE_TIMEOUT_MS = 30000;
  const IDENT_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/;
  const CREATE_BUTTON_LABEL = 'Создать таблицу';
  const CREATE_WAIT_LABEL = 'Запрос создания отправлен. Ожидаем ответ сервера...';
  const DATA_CONTRACTS_KEY = 'ao_data_contracts_v1';
  const DATA_CONTRACTS_STORAGE_KEY = 'ao_data_contracts_storage_v1';
  const TABLE_TEMPLATES_KEY = 'ao_create_table_templates_v1'; // legacy fallback
  const TABLE_TEMPLATES_STORAGE_KEY = 'ao_create_table_templates_storage_v1'; // legacy fallback
  const STORAGE_DEFAULT_SCHEMA = 'ao_system';
  const STORAGE_DEFAULT_TABLE = 'table_templates_store';
  const STORAGE_CONTRACT_NAME = 'Хранилище шаблонов таблиц';
  const SETTINGS_SYSTEM_TABLE = 'ao_system.table_settings_store';
  const API_CONFIGS_SYSTEM_TABLE = 'ao_system.api_configs_store';
  const SERVER_WRITES_SYSTEM_TABLE = 'ao_system.table_server_writes_store';
  const REQUIRED_TABLE_FIELDS: ColumnDef[] = [
    { field_name: 'ao_source', field_type: 'text', description: 'источник данных (техническое поле)' },
    { field_name: 'ao_run_id', field_type: 'text', description: 'идентификатор запуска (техническое поле)' },
    { field_name: 'ao_created_at', field_type: 'timestamptz', description: 'время создания записи (техническое поле)' },
    { field_name: 'ao_updated_at', field_type: 'timestamptz', description: 'время обновления записи (техническое поле)' },
    { field_name: 'ao_contract_schema', field_type: 'text', description: 'схема таблицы контракта данных' },
    { field_name: 'ao_contract_name', field_type: 'text', description: 'имя контракта данных' },
    { field_name: 'ao_contract_version', field_type: 'int', description: 'версия контракта данных' }
  ];

  let error = '';
  let schema_name = '';
  let table_name = '';
  let description = '';
  let descriptionEl: HTMLTextAreaElement | null = null;
  let table_class = 'custom';
  let columns: ColumnDef[] = REQUIRED_TABLE_FIELDS.map((c) => ({ ...c }));
  let partition_enabled = false;
  let partition_column = 'event_date';
  let partition_interval: 'day' | 'month' = 'day';

  let tableTemplates: DataContract[] = [];
  let selectedTemplateId = '';
  let templateNameDraft = '';
  let storage_schema = STORAGE_DEFAULT_SCHEMA;
  let storage_table = STORAGE_DEFAULT_TABLE;
  let storage_picker_open = false;
  let storage_pick_value = '';
  let storage_status: 'checking' | 'ok' | 'invalid' | 'missing' | 'error' = 'checking';
  let storage_status_message = '';

  let result_modal_open = false;
  let result_modal_title = '';
  let result_modal_text = '';
  let result_created_schema = '';
  let result_created_table = '';
  let result_is_success = false;
  let creating = false;
  let refreshingTables = false;
  let refreshingTemplates = false;
  let tablesSignature = '';

  function uid() {
    return `${Date.now()}_${Math.random().toString(16).slice(2)}`;
  }

  function canWrite(): boolean {
    return role === 'data_admin';
  }

  function isSystemTable(schema: string, table: string): boolean {
    const qn = `${String(schema || '').trim()}.${String(table || '').trim()}`;
    return qn === SETTINGS_SYSTEM_TABLE || qn === API_CONFIGS_SYSTEM_TABLE || qn === SERVER_WRITES_SYSTEM_TABLE;
  }

  function sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async function forceRefreshTables() {
    refreshingTables = true;
    try {
      if (hardReloadConstructor) {
        await hardReloadConstructor();
      } else {
        await refreshTables();
        await sleep(450);
        await refreshTables();
      }
    } finally {
      refreshingTables = false;
    }
  }

  async function refreshTemplatesPanel() {
    refreshingTemplates = true;
    try {
      await loadTableTemplates();
    } finally {
      refreshingTemplates = false;
    }
  }

  function normalizeColumns(cols: ColumnDef[]) {
    return cols
      .map((c) => ({
        field_name: (c.field_name || '').trim(),
        field_type: (c.field_type || '').trim(),
        description: (c.description || '').trim()
      }))
      .filter((c) => c.field_name.length > 0);
  }

  function withRequiredTableFields(cols: ColumnDef[]) {
    const base = (Array.isArray(cols) ? cols : []).map((c) => ({
      field_name: String(c?.field_name || '').trim(),
      field_type: String(c?.field_type || 'text').trim(),
      description: String(c?.description || '').trim()
    }));
    const byName = new Map(base.map((c) => [c.field_name.toLowerCase(), c]));

    // Keep system fields first (and preserve user overrides if they exist),
    // then append non-system fields in their original order.
    const requiredFirst = REQUIRED_TABLE_FIELDS.map((sys) => {
      const hit = byName.get(sys.field_name.toLowerCase());
      return hit ? { ...sys, ...hit } : { ...sys };
    });

    const nonSystem = base.filter(
      (c) => !REQUIRED_TABLE_FIELDS.some((f) => f.field_name.toLowerCase() === c.field_name.toLowerCase())
    );

    return [...requiredFirst, ...nonSystem];
  }

  function bronzeTemplate(): DataContract {
    return {
      id: 'builtin_bronze',
      name: 'Bronze',
      schema_name: 'bronze',
      table_name: 'wb_ads_raw',
      table_class: 'bronze_raw',
      description: 'Сырые ответы API (append-only JSON)',
      columns: withRequiredTableFields([
        { field_name: 'dataset', field_type: 'text', description: 'dataset name' },
        { field_name: 'endpoint', field_type: 'text', description: 'endpoint name' },
        { field_name: 'request_id', field_type: 'text', description: 'request id' },
        { field_name: 'ingested_at', field_type: 'timestamptz', description: 'ingest timestamp' },
        { field_name: 'payload', field_type: 'jsonb', description: 'raw payload' }
      ]),
      partition_enabled: true,
      partition_column: 'ingested_at',
      partition_interval: 'day',
      contract_version: 1,
      contract_mode: 'safe_add_only'
    };
  }

  function silverTemplate(): DataContract {
    return {
      id: 'builtin_silver',
      name: 'Silver',
      schema_name: 'silver_adv',
      table_name: 'wb_ads_daily',
      table_class: 'silver_table',
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

  function storageSystemTemplate(): DataContract {
    return {
      id: 'builtin_storage_contracts',
      name: STORAGE_CONTRACT_NAME,
      schema_name: STORAGE_DEFAULT_SCHEMA,
      table_name: STORAGE_DEFAULT_TABLE,
      table_class: 'custom',
      description: 'Служебная таблица для хранения шаблонов блока «Создание таблиц»',
      columns: withRequiredTableFields([
        { field_name: 'template_name', field_type: 'text', description: 'имя шаблона' },
        { field_name: 'schema_name', field_type: 'text', description: 'схема таблицы' },
        { field_name: 'table_name', field_type: 'text', description: 'имя таблицы' },
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

  function contractsSystemTemplate(): DataContract {
    return {
      id: 'builtin_data_contracts_table',
      name: 'Хранилище контрактов данных',
      schema_name: 'ao_system',
      table_name: 'table_data_contract_versions',
      table_class: 'custom',
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

  function settingsSystemTemplate(): DataContract {
    return {
      id: 'builtin_settings_table',
      name: 'Таблица настроек',
      schema_name: 'ao_system',
      table_name: 'table_settings_store',
      table_class: 'custom',
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

  function serverWritesSystemTemplate(): DataContract {
    return {
      id: 'builtin_server_writes_table',
      name: 'Таблица серверных записей',
      schema_name: 'ao_system',
      table_name: 'table_server_writes_store',
      table_class: 'custom',
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

  function apiConfigsSystemTemplate(): DataContract {
    return {
      id: 'builtin_api_configs_table',
      name: 'Хранилище API',
      schema_name: 'ao_system',
      table_name: 'api_configs_store',
      table_class: 'custom',
      description: 'Системная таблица преднастроенных API для workflow',
      columns: withRequiredTableFields([
        { field_name: 'api_name', field_type: 'text', description: 'имя API-конфига' },
        { field_name: 'method', field_type: 'text', description: 'HTTP-метод' },
        { field_name: 'base_url', field_type: 'text', description: 'базовый URL API' },
        { field_name: 'path', field_type: 'text', description: 'путь запроса' },
        { field_name: 'headers_json', field_type: 'jsonb', description: 'заголовки (json)' },
        { field_name: 'query_json', field_type: 'jsonb', description: 'query-параметры (json)' },
        { field_name: 'body_json', field_type: 'jsonb', description: 'тело запроса (json)' },
        { field_name: 'pagination_json', field_type: 'jsonb', description: 'настройки пагинации (json)' },
        { field_name: 'target_schema', field_type: 'text', description: 'схема таблицы назначения' },
        { field_name: 'target_table', field_type: 'text', description: 'таблица назначения' },
        { field_name: 'mapping_json', field_type: 'jsonb', description: 'маппинг полей (json)' },
        { field_name: 'description', field_type: 'text', description: 'описание конфига' },
        { field_name: 'is_active', field_type: 'boolean', description: 'активен ли API-конфиг' },
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

  const FIELD_TYPE_TO_DB_TYPES: Record<string, string[]> = {
    text: ['text', 'character varying', 'varchar'],
    int: ['integer', 'int4', 'int'],
    bigint: ['bigint', 'int8'],
    numeric: ['numeric', 'decimal'],
    boolean: ['boolean', 'bool'],
    date: ['date'],
    timestamptz: ['timestamp with time zone', 'timestamptz'],
    jsonb: ['jsonb', 'json'],
    uuid: ['uuid']
  };

  function normalizeTypeName(type: string) {
    return String(type || '').toLowerCase().trim();
  }

  function storageRequiredColumnsFromSystemTemplate() {
    return storageSystemTemplate().columns.map((c) => ({
      name: String(c.field_name || '').trim().toLowerCase(),
      types: FIELD_TYPE_TO_DB_TYPES[String(c.field_type || '').trim()] || [normalizeTypeName(c.field_type || '')]
    }));
  }

  function storageInstruction(prefix: string) {
    return `${prefix} Выберите системный шаблон «${STORAGE_CONTRACT_NAME}», создайте таблицу и подключите ее здесь.`;
  }

  async function parseStorageTableConfig() {
    storage_schema = STORAGE_DEFAULT_SCHEMA;
    storage_table = STORAGE_DEFAULT_TABLE;
    try {
      const j = await apiJson<{ effective?: any }>(`${apiBase}/settings/effective`);
      const eff = j?.effective || {};
      const nextSchema = String(eff?.templates_schema || '').trim();
      const nextTable = String(eff?.templates_table || '').trim();
      if (nextSchema) storage_schema = nextSchema;
      if (nextTable) storage_table = nextTable;
    } catch {
      storage_schema = STORAGE_DEFAULT_SCHEMA;
      storage_table = STORAGE_DEFAULT_TABLE;
    }
  }

  async function checkStorageTable(schema: string, table: string) {
    storage_status = 'checking';
    storage_status_message = 'Проверяем таблицу хранения...';
    try {
      const res = await apiJson<{ columns: Array<{ name: string; type: string }> }>(
        `${apiBase}/columns?schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`
      );
      const cols = Array.isArray(res?.columns) ? res.columns : [];
      if (!cols.length) {
        storage_status = 'missing';
        storage_status_message = storageInstruction('Таблица хранения не найдена или пуста.');
        return false;
      }

      const map = new Map(cols.map((c) => [String(c.name || '').toLowerCase(), normalizeTypeName(c.type)]));
      const required = storageRequiredColumnsFromSystemTemplate();
      for (const need of required) {
        const actual = map.get(need.name);
        if (!actual || !need.types.some((t) => actual.includes(t))) {
          storage_status = 'invalid';
          storage_status_message = `Структура ${schema}.${table} не подходит: колонка ${need.name} отсутствует или имеет неверный тип.`;
          return false;
        }
      }

      storage_status = 'ok';
      storage_status_message = `Хранилище шаблонов подключено: ${schema}.${table}`;
      return true;
    } catch (e: any) {
      const msg = String(e?.message || '');
      storage_status = msg.includes('404') ? 'missing' : 'error';
      storage_status_message = msg.includes('404')
        ? storageInstruction(`Таблица ${schema}.${table} не найдена.`)
        : storageInstruction(`Не удалось проверить ${schema}.${table}.`);
      return false;
    }
  }

  async function loadTemplatesFromStorage() {
    const isValid = await checkStorageTable(storage_schema, storage_table);
    if (!isValid) {
      loadTableTemplatesFallback();
      return;
    }
    try {
      const j = await apiJson<{ rows: any[] }>(
        `${apiBase}/preview?schema=${encodeURIComponent(storage_schema)}&table=${encodeURIComponent(storage_table)}&limit=5000`
      );
      const rows = Array.isArray(j?.rows) ? j.rows : [];
      const custom: DataContract[] = [];
      for (const r of rows) {
        const parsedColumns = Array.isArray(r?.columns)
          ? r.columns
          : (() => {
              try {
                const x = JSON.parse(String(r?.columns || '[]'));
                return Array.isArray(x) ? x : [];
              } catch {
                return [];
              }
            })();
        const name = String(r?.template_name || '').trim();
        if (!name) continue;
        const rawMode = String(r?.contract_mode || 'safe_add_only').trim();
        custom.push({
          id: uid(),
          name,
          schema_name: String(r?.schema_name || ''),
          table_name: String(r?.table_name || ''),
          table_class: String(r?.table_class || 'custom'),
          description: String(r?.description || ''),
          columns: parsedColumns.map((c: any) => ({
            field_name: String(c?.field_name || ''),
            field_type: String(c?.field_type || 'text'),
            description: String(c?.description || '')
          })),
          partition_enabled: Boolean(r?.partition_enabled),
          partition_column: String(r?.partition_column || 'event_date'),
          partition_interval: String(r?.partition_interval || 'day') === 'month' ? 'month' : 'day',
          contract_version: Number(r?.contract_version || 1) > 0 ? Number(r?.contract_version || 1) : 1,
          contract_mode: rawMode === 'strict_sync' ? 'strict_sync' : 'safe_add_only',
          storage_ctids: r?.__ctid ? [String(r.__ctid)] : []
        });
      }
      tableTemplates = [
        bronzeTemplate(),
        silverTemplate(),
        storageSystemTemplate(),
        contractsSystemTemplate(),
        settingsSystemTemplate(),
        apiConfigsSystemTemplate(),
        serverWritesSystemTemplate(),
        ...custom
      ];
      error = '';
    } catch (e: any) {
      storage_status = 'error';
      storage_status_message = storageInstruction('Ошибка загрузки шаблонов из таблицы.');
      loadTableTemplatesFallback();
      error = e?.message || String(e);
    }
  }

  function loadTableTemplatesFallback() {
    tableTemplates = [
      bronzeTemplate(),
      silverTemplate(),
      storageSystemTemplate(),
      contractsSystemTemplate(),
      settingsSystemTemplate(),
      apiConfigsSystemTemplate(),
      serverWritesSystemTemplate()
    ];
  }

  function applyTemplate(t: DataContract) {
    schema_name = t.schema_name;
    table_name = t.table_name;
    table_class = t.table_class;
    description = t.description;
    columns = withRequiredTableFields((t.columns || []).map((c) => ({ ...c })));
    partition_enabled = !!t.partition_enabled;
    partition_column = t.partition_column || 'event_date';
    partition_interval = t.partition_interval || 'day';
    selectedTemplateId = t.id;
    templateNameDraft = t.name;
  }

  function pickTemplateBronze() {
    applyTemplate(bronzeTemplate());
  }

  function pickTemplateSilver() {
    applyTemplate(silverTemplate());
  }

  async function loadTableTemplates() {
    await parseStorageTableConfig();
    await loadTemplatesFromStorage();
  }

  function saveTableTemplatesLocal() {
    // local storage disabled: source of truth is server DB
  }

  async function saveTemplateToStorage(t: DataContract) {
    const isValid = await checkStorageTable(storage_schema, storage_table);
    if (!isValid) throw new Error(storage_status_message || 'Таблица хранения недоступна');
    const rows = await apiJson<{ rows: any[] }>(
      `${apiBase}/preview?schema=${encodeURIComponent(storage_schema)}&table=${encodeURIComponent(storage_table)}&limit=5000`
    );
    const found = (Array.isArray(rows?.rows) ? rows.rows : []).filter(
      (r) => String(r?.template_name || '').trim().toLowerCase() === t.name.toLowerCase()
    );
    for (const r of found) {
      if (r?.__ctid) {
        await apiJson(`${apiBase}/rows/delete`, {
          method: 'POST',
          headers: headers(),
          body: JSON.stringify({ schema: storage_schema, table: storage_table, ctid: String(r.__ctid) })
        });
      }
    }
    const row: Record<string, any> = {
      schema_name: t.schema_name,
      table_name: t.table_name,
      table_class: t.table_class,
      description: t.description,
      columns: JSON.stringify(t.columns || []),
      partition_enabled: !!t.partition_enabled,
      partition_column: t.partition_column || '',
      partition_interval: t.partition_interval || 'day'
    };
    row.template_name = t.name;

    await apiJson(`${apiBase}/rows/add`, {
      method: 'POST',
      headers: headers(),
      body: JSON.stringify({
        schema: storage_schema,
        table: storage_table,
        row
      })
    });
  }

  async function deleteTemplateFromStorage(templateName: string) {
    const isValid = await checkStorageTable(storage_schema, storage_table);
    if (!isValid) throw new Error(storage_status_message || 'Таблица хранения недоступна');
    const rows = await apiJson<{ rows: any[] }>(
      `${apiBase}/preview?schema=${encodeURIComponent(storage_schema)}&table=${encodeURIComponent(storage_table)}&limit=5000`
    );
    const found = (Array.isArray(rows?.rows) ? rows.rows : []).filter(
      (r) => String(r?.template_name || '').trim().toLowerCase() === templateName.toLowerCase()
    );
    for (const r of found) {
      if (r?.__ctid) {
        await apiJson(`${apiBase}/rows/delete`, {
          method: 'POST',
          headers: headers(),
          body: JSON.stringify({ schema: storage_schema, table: storage_table, ctid: String(r.__ctid) })
        });
      }
    }
  }

  function applySelectedTemplate(id: string) {
    selectedTemplateId = id;
    const t = tableTemplates.find((x) => x.id === id);
    if (!t) return;
    applyTemplate(t);
  }

  function validateTemplateDraftForSave() {
    const name = String(templateNameDraft || '').trim();
    const nextSchema = schema_name.trim();
    const nextTable = table_name.trim();
    const nextClass = table_class.trim() || 'custom';
    const nextDescription = description.trim();
    const cols = withRequiredTableFields(normalizeColumns(columns));

    if (!name) throw new Error('Укажи название шаблона');
    if (!nextSchema) throw new Error('Для шаблона заполни поле «Схема»');
    if (!IDENT_RE.test(nextSchema)) throw new Error('Для шаблона схема должна содержать только латиницу, цифры и "_"');
    if (!nextTable) throw new Error('Для шаблона заполни поле «Название таблицы»');
    if (!IDENT_RE.test(nextTable))
      throw new Error('Для шаблона имя таблицы должно содержать только латиницу, цифры и "_"');
    if (!nextClass) throw new Error('Для шаблона заполни поле «Класс»');
    if (!cols.length) throw new Error('Добавь хотя бы одно поле');
    if (partition_enabled && !partition_column.trim()) {
      throw new Error('Укажи поле партиционирования для шаблона');
    }
    if (partition_enabled && partition_interval !== 'day' && partition_interval !== 'month') {
      throw new Error('Интервал партиционирования должен быть day или month');
    }

    return {
      name,
      schema_name: nextSchema,
      table_name: nextTable,
      table_class: nextClass,
      description: nextDescription,
      columns: cols,
      partition_enabled,
      partition_column: partition_column.trim(),
      partition_interval
    };
  }

  async function startNewTemplate() {
    const draft = validateTemplateDraftForSave();
    if (storage_status !== 'ok') throw new Error(storage_status_message || 'Сначала подключи таблицу хранения шаблонов');
    if (
      tableTemplates.some(
        (t) => !t.id.startsWith('builtin_') && t.name.trim().toLowerCase() === draft.name.toLowerCase()
      )
    ) {
      throw new Error('Шаблон с таким названием уже существует');
    }

    const newTemplate: DataContract = {
      id: uid(),
      ...draft,
      contract_version: 1,
      contract_mode: 'safe_add_only'
    };

    await saveTemplateToStorage(newTemplate);
    await loadTemplatesFromStorage();
    const actual = tableTemplates.find((x) => x.name.trim().toLowerCase() === draft.name.toLowerCase());
    selectedTemplateId = actual?.id || '';
    templateNameDraft = actual?.name || draft.name;
    error = '';
  }

  async function saveCurrentTemplate() {
    if (!selectedTemplateId) throw new Error('Сначала добавь или выбери шаблон');
    if (selectedTemplateId.startsWith('builtin_')) {
      throw new Error('Встроенный шаблон нельзя сохранить. Нажми «Добавить»');
    }

    const idx = tableTemplates.findIndex((x) => x.id === selectedTemplateId);
    if (idx < 0) throw new Error('Активный шаблон не найден');
    if (storage_status !== 'ok') throw new Error(storage_status_message || 'Сначала подключи таблицу хранения шаблонов');

    const draft = validateTemplateDraftForSave();

    const updated = {
      ...tableTemplates[idx],
      ...draft,
      contract_version: Number(tableTemplates[idx].contract_version || 1),
      contract_mode: tableTemplates[idx].contract_mode || 'safe_add_only'
    } as DataContract;

    const previousName = String(tableTemplates[idx].name || '').trim();
    if (previousName && previousName.toLowerCase() !== draft.name.toLowerCase()) {
      await deleteTemplateFromStorage(previousName);
    }
    await saveTemplateToStorage(updated);
    await loadTemplatesFromStorage();
    const actual = tableTemplates.find((x) => x.name.trim().toLowerCase() === draft.name.toLowerCase());
    selectedTemplateId = actual?.id || '';
    templateNameDraft = actual?.name || draft.name;
    error = '';
  }

  async function deleteTemplateById(id: string) {
    if (!id) throw new Error('Сначала выбери шаблон');
    if (id.startsWith('builtin_')) throw new Error('Встроенный шаблон удалить нельзя');
    if (storage_status !== 'ok') throw new Error(storage_status_message || 'Сначала подключи таблицу хранения шаблонов');

    const t = tableTemplates.find((x) => x.id === id);
    if (t) {
      await deleteTemplateFromStorage(t.name);
      await loadTemplatesFromStorage();
    }
    if (selectedTemplateId === id) {
      selectedTemplateId = '';
      templateNameDraft = '';
    }
    error = '';
  }

  async function onSaveTemplateClick() {
    try {
      await saveCurrentTemplate();
    } catch (e: any) {
      error = e?.message || String(e);
    }
  }

  async function onDeleteTemplateClick(id: string) {
    try {
      await deleteTemplateById(id);
    } catch (e: any) {
      error = e?.message || String(e);
    }
  }

  async function onAddTemplateClick() {
    try {
      await startNewTemplate();
    } catch (e: any) {
      error = e?.message || String(e);
    }
  }

  async function applyStorageTableChoice() {
    if (!storage_pick_value) return;
    const [schema, table] = storage_pick_value.split('.');
    if (!schema || !table) return;
    const ok = await checkStorageTable(schema, table);
    if (!ok) return;
    storage_schema = schema;
    storage_table = table;
    try {
      await apiJson(`${apiBase}/settings/upsert`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({
          setting_key: 'templates_storage',
          setting_value: { schema, table },
          description: 'Хранилище шаблонов таблиц',
          scope: 'global',
          is_active: true
        })
      });
    } catch (e: any) {
      error = e?.message ?? String(e);
    }
    storage_picker_open = false;
    await loadTemplatesFromStorage();
  }

  function addField() {
    columns = [...columns, { field_name: '', field_type: 'text', description: '' }];
  }

  function isRequiredField(name: string) {
    const n = String(name || '').trim().toLowerCase();
    return REQUIRED_TABLE_FIELDS.some((f) => f.field_name.toLowerCase() === n);
  }

  function removeField(ix: number) {
    const fieldName = String(columns[ix]?.field_name || '');
    if (isRequiredField(fieldName)) return;
    columns = columns.filter((_, i) => i !== ix);
    if (columns.length === 0) columns = REQUIRED_TABLE_FIELDS.map((c) => ({ ...c }));
  }

  function validate() {
    if (!schema_name.trim()) throw new Error('Укажи схему');
    if (!table_name.trim()) throw new Error('Укажи имя таблицы');
    if (!table_class.trim()) throw new Error('Укажи класс');
    if (!IDENT_RE.test(schema_name.trim())) {
      throw new Error('Схема: только латиница, цифры и _, первый символ — буква или _');
    }
    if (!IDENT_RE.test(table_name.trim())) {
      throw new Error('Имя таблицы: только латиница, цифры и _, первый символ — буква или _');
    }

    const cols = normalizeColumns(columns);
    if (cols.length === 0) throw new Error('Добавь хотя бы одно поле');
    for (const c of cols) {
      if (!c.field_type) throw new Error('Укажи тип для каждого поля');
      if (!IDENT_RE.test(c.field_name)) {
        throw new Error(`Поле «${c.field_name}»: только латиница, цифры и _, первый символ — буква или _`);
      }
    }

    if (partition_enabled) {
      if (!partition_column.trim()) throw new Error('Партиционирование включено: укажи колонку');
      if (!partition_interval) throw new Error('Партиционирование включено: укажи интервал');
    }
  }

  async function createTableNow() {
    error = '';
    creating = true;

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), CREATE_TIMEOUT_MS);

    try {
      if (!canWrite()) throw new Error('Недостаточно прав (нужна роль data_admin)');

      validate();
      const cols = withRequiredTableFields(normalizeColumns(columns));

      const request = apiJson(`${apiBase}/tables/create`, {
        method: 'POST',
        headers: headers(),
        signal: controller.signal,
        body: JSON.stringify({
          schema_name: schema_name.trim(),
          table_name: table_name.trim(),
          table_class,
          description: description.trim(),
          columns: cols,
          partitioning: partition_enabled
            ? { enabled: true, column: partition_column.trim(), interval: partition_interval }
            : { enabled: false }
        })
      });
      const timeoutPromise = new Promise((_, reject) =>
        setTimeout(() => reject(new Error('Сервер не ответил вовремя. Проверьте логи API и БД.')), CREATE_TIMEOUT_MS)
      );
      const response = await Promise.race([request, timeoutPromise]) as any;

      result_created_schema = schema_name.trim();
      result_created_table = table_name.trim();
      result_is_success = true;
      result_modal_title = 'Таблица создана';
      result_modal_text = response ? JSON.stringify(response, null, 2) : 'Операция выполнена успешно.';
      result_modal_open = true;
    } catch (e: any) {
      error = e?.name === 'AbortError'
        ? 'Сервер не ответил вовремя. Проверьте статус базы и повторите попытку.'
        : (e?.message ?? String(e));
      result_is_success = false;
      result_modal_title = 'Ошибка создания';
      result_modal_text = error;
      result_modal_open = true;
    } finally {
      clearTimeout(timeoutId);
      creating = false;
    }
  }

  async function deleteTableNow(schema: string, table: string) {
    error = '';
    try {
      if (!canWrite()) throw new Error('Недостаточно прав (нужна роль data_admin)');
      const ok = confirm(`Удалить таблицу ${schema}.${table}?`);
      if (!ok) return;

      await apiJson(`${apiBase}/tables/drop`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({ schema, table })
      });
      await forceRefreshTables();
    } catch (e: any) {
      error = e?.message ?? String(e);
    }
  }

  function closeResultModal() {
    result_modal_open = false;
    if (result_is_success && result_created_schema && result_created_table) {
      onCreated?.(result_created_schema, result_created_table);
    }
  }

  onMount(() => {
    loadTableTemplates();
    tick().then(syncDescriptionHeight);
  });

  function syncDescriptionHeight() {
    if (!descriptionEl) return;
    descriptionEl.style.height = 'auto';
    descriptionEl.style.height = `${Math.max(descriptionEl.scrollHeight, 72)}px`;
  }

  $: description, tick().then(syncDescriptionHeight);
  $: tablesSignature = existingTables.map((t) => `${t.schema_name}.${t.table_name}`).join('|');
</script>

{#if error}
  <div class="alert">
    <div class="alert-title">Ошибка</div>
    <pre>{error}</pre>
  </div>
{/if}

<section class="panel">
  <div class="panel-head">
    <h2>Создание таблиц</h2>
  </div>

  <div class="layout">
    <aside class="aside">
      <div class="aside-head">
        <div class="aside-title">Текущие таблицы</div>
        <button class="icon-btn refresh-btn" on:click={forceRefreshTables} disabled={loading || refreshingTables} title="Обновить список">↻</button>
      </div>
      <div class="storage-meta">
        <span>Подключено к базе:</span>
        <span class="plain-value">{apiBase}</span>
      </div>
      {#if existingTables.length === 0}
        <div class="hint">Пока нет данных.</div>
      {:else}
        {#key tablesSignature}
          <div class="list tables-list">
            {#each existingTables as t}
              <div class="row-item">
                <div class="row-name">{t.schema_name}.{t.table_name}</div>
                <div class="row-actions">
                  {#if isSystemTable(t.schema_name, t.table_name)}
                    <span class="system-badge">System</span>
                  {:else}
                    <button class="danger icon-btn" on:click={() => deleteTableNow(t.schema_name, t.table_name)} title="Удалить таблицу">x</button>
                  {/if}
                </div>
              </div>
            {/each}
          </div>
        {/key}
      {/if}
    </aside>

    <div class="main">
      <div class="card">
        <div class="form create-form">
          <label class="w20">
            Класс (для себя)
            <select bind:value={table_class}>
              <option value="custom">custom</option>
              <option value="bronze_raw">bronze_raw</option>
              <option value="silver_table">silver_table</option>
              <option value="showcase_table">showcase_table</option>
            </select>
          </label>

          <label class="w20">
            Схема
            <input bind:value={schema_name} placeholder="например: showcase / bronze / silver_adv" />
          </label>

          <label class="w60">
            Название таблицы
            <input bind:value={table_name} placeholder="например: advertising" />
          </label>

          <label class="w100">
            Описание таблицы
            <textarea
              bind:this={descriptionEl}
              bind:value={description}
              rows="2"
              placeholder="что это за таблица"
              on:input={syncDescriptionHeight}
            ></textarea>
          </label>

          <div class="w100 partition-toggle">
            <button
              type="button"
              class="partition-btn"
              class:active={partition_enabled}
              on:click={() => (partition_enabled = !partition_enabled)}
            >
              {#if partition_enabled}
                <span class="partition-indicator">●</span>
              {/if}
              <span>Партиционирование</span>
            </button>
          </div>
        </div>

        {#if partition_enabled}
          <div class="subcard">
            <h3>Партиционирование</h3>
            <div class="form" style="margin-top:10px;">
              <label>
                Колонка для партиций
                <input bind:value={partition_column} placeholder="event_date / ingested_at / ..." />
              </label>
              <label>
                Интервал
                <select bind:value={partition_interval}>
                  <option value="day">day</option>
                  <option value="month">month</option>
                </select>
              </label>
            </div>
          </div>
        {/if}

        <div class="subcard">
          <h3>Поля</h3>
          {#each columns as c, ix}
            <div class="field-row">
              <input placeholder="имя поля" bind:value={c.field_name} />
              <select bind:value={c.field_type}>
                {#each typeOptions as t}
                  <option value={t}>{t}</option>
                {/each}
              </select>
              <input placeholder="описание" bind:value={c.description} />
              <button
                class="danger icon-btn"
                on:click={() => removeField(ix)}
                disabled={isRequiredField(c.field_name)}
                title={isRequiredField(c.field_name) ? 'Обязательное поле' : 'Удалить поле'}
              >x</button>
            </div>
          {/each}
          <div class="fields-footer">
            <button class="add-field-icon" on:click={addField} title="Добавить поле" aria-label="Добавить поле">+</button>
          </div>
        </div>

        <div class="actions">
          <button class="primary" on:click={createTableNow} disabled={loading || creating || !canWrite()}>
            {CREATE_BUTTON_LABEL}
          </button>
          {#if creating}
            <span class="hint">{CREATE_WAIT_LABEL}</span>
          {/if}
        </div>

        {#if !canWrite()}
          <p class="hint">Кнопка активна только при роли <b>data_admin</b>.</p>
        {/if}
      </div>
    </div>

    <aside class="aside">
      <div class="aside-head">
        <div class="aside-title">Шаблоны таблиц</div>
        <button
          class="icon-btn refresh-btn"
          on:click={refreshTemplatesPanel}
          disabled={refreshingTemplates}
          title="Обновить шаблоны"
        >↻</button>
      </div>
      <div class="storage-meta templates-meta">
        <span>Хранятся в таблице:</span>
        <button class="link-btn" on:click={() => { storage_picker_open = !storage_picker_open; storage_pick_value = `${storage_schema}.${storage_table}`; }}>
          {storage_schema}.{storage_table}
        </button>
      </div>
      {#if storage_picker_open}
        <div class="storage-picker">
          <select bind:value={storage_pick_value}>
            {#each existingTables as t}
              <option value={`${t.schema_name}.${t.table_name}`}>{t.schema_name}.{t.table_name}</option>
            {/each}
          </select>
          <button on:click={applyStorageTableChoice} disabled={!storage_pick_value}>Подключить</button>
        </div>
      {/if}
      {#if storage_status !== 'ok' && storage_status_message}
        <p class="hint">{storage_status_message}</p>
      {/if}
      <div class="template-controls">
        <input class="template-name" bind:value={templateNameDraft} placeholder="Название шаблона" />
        <div class="inline-actions">
          <button on:click={onAddTemplateClick}>Добавить</button>
          <button on:click={onSaveTemplateClick}>Сохранить</button>
        </div>
      </div>
      <div class="list templates-list">
        {#each tableTemplates as t}
          <div class="row-item" class:activeitem={selectedTemplateId === t.id}>
            <button class="item-button" on:click={() => applySelectedTemplate(t.id)}>{t.name}</button>
            <div class="row-actions">
              {#if t.id.startsWith('builtin_')}
                <span class="system-badge">System</span>
              {:else}
                <button class="danger icon-btn" on:click={() => onDeleteTemplateClick(t.id)} title="Удалить шаблон">x</button>
              {/if}
            </div>
          </div>
        {/each}
      </div>
    </aside>
  </div>
</section>

{#if result_modal_open}
  <div
    class="modal-bg"
    role="button"
    tabindex="0"
    aria-label="Закрыть окно результата"
    on:click={closeResultModal}
    on:keydown={(e) => (e.key === 'Escape' || e.key === 'Enter' || e.key === ' ') && closeResultModal()}
  >
    <div class="modal" on:click|stopPropagation on:keydown|stopPropagation>
      <h3>{result_modal_title}</h3>
      <pre>{result_modal_text}</pre>
      <div class="modal-actions">
        <button class="primary" on:click={closeResultModal}>OK</button>
      </div>
    </div>
  </div>
{/if}

<style>
  .panel { background:#fff; border:1px solid #e6eaf2; border-radius:18px; padding:14px; box-shadow:0 6px 20px rgba(15,23,42,.05); margin-top:12px; }
  .panel-head { display:flex; align-items:center; justify-content:space-between; gap:12px; }
  .panel-head h2 { margin:0; font-size:18px; }
  .layout { display:grid; grid-template-columns: 320px 1fr 360px; gap:12px; margin-top:12px; align-items:start; }
  @media (max-width: 1300px) { .layout { grid-template-columns: 320px 1fr; } }
  @media (max-width: 1100px) { .layout { grid-template-columns: 1fr; } }

  .aside { border:1px solid #e6eaf2; border-radius:16px; padding:12px; background:#f8fafc; }
  .aside-head { display:flex; align-items:center; justify-content:space-between; gap:8px; margin-bottom:8px; }
  .aside-title { font-weight:700; font-size:14px; line-height:1.3; margin-bottom:0; }
  .list { display:flex; flex-direction:column; gap:8px; overflow:visible; max-height:none; }
  .row-item { display:grid; grid-template-columns: 1fr auto; gap:8px; align-items:center; border:1px solid #e6eaf2; border-radius:14px; background:#fff; padding:8px 10px; }
  .row-actions { display:flex; align-items:center; justify-content:flex-end; min-width:54px; }
  .system-badge { font-size:11px; line-height:1; padding:4px 8px; border-radius:999px; border:1px solid #cbd5e1; color:#334155; background:#f8fafc; font-weight:600; }
  .row-name { font-weight:400; font-size:13px; line-height:1.25; word-break:break-word; }
  .item-button { text-align:left; border:0; background:transparent; padding:0; font-weight:400; font-size:13px; line-height:1.25; color:inherit; }
  .activeitem { border-color:#0f172a; background:#0f172a; color:#fff; }
  .activeitem .item-button { color:#fff; }
  .tables-list .row-item { background:#0f172a; border-color:#0f172a; }
  .tables-list .row-name { color:#fff; }
  .templates-list .row-item { background:#0f172a; border-color:#0f172a; }
  .templates-list .item-button { color:#fff; }
  .tables-list .system-badge { border-color:#334155; color:#e2e8f0; background:#1e293b; }
  .templates-list .system-badge { border-color:#334155; color:#e2e8f0; background:#1e293b; }
  .templates-list .activeitem { background:#fff; border-color:#e6eaf2; color:#0f172a; }
  .templates-list .activeitem .system-badge { border-color:#cbd5e1; color:#334155; background:#f8fafc; }
  .templates-list .activeitem .item-button { color:#0f172a; font-size:15px; font-weight:600; letter-spacing:.01em; }
  .templates-list .activeitem .item-button::before { content:'●'; margin-right:8px; font-size:11px; color:#0f172a; vertical-align:middle; }

  .main { min-width:0; }
  .card { border:1px solid #e6eaf2; border-radius:16px; padding:12px; background:#fff; }
  .subcard { margin-top:10px; border:1px dashed #e6eaf2; border-radius:14px; padding:10px; }
  .subcard h3 { margin:0 0 8px 0; font-size:14px; }

  .form { display:grid; grid-template-columns: 1fr 1fr; gap:10px; }
  .create-form { grid-template-columns: 1fr 1fr 3fr; }
  .create-form .w20, .create-form .w60, .create-form .w100 { min-width:0; }
  .create-form .w100 { grid-column: 1 / -1; }
  @media (max-width: 1100px) { .form { grid-template-columns: 1fr; } }
  label { display:flex; flex-direction:column; gap:6px; font-size:13px; }
  input, select, textarea { border-radius:14px; border:1px solid #e6eaf2; padding:10px 12px; outline:none; background:#fff; box-sizing:border-box; }
  .create-form textarea { min-height:72px; resize:none; overflow:hidden; }

  .field-row { display:grid; grid-template-columns: 1.2fr .8fr 1.6fr auto; gap:8px; margin-top:10px; }
  @media (max-width: 1100px) { .field-row { grid-template-columns: 1fr; } }
  .fields-footer { margin-top:12px; }
  .add-field-icon { width:44px; min-width:44px; padding:10px 0; font-size:20px; line-height:1; border-color:transparent; background:transparent; }

  .partition-toggle { display:flex; align-items:flex-start; justify-content:flex-start; }
  .partition-btn {
    display:inline-flex;
    align-items:center;
    gap:8px;
    background:#0f172a;
    border-color:#0f172a;
    color:#fff;
    font-size:14px;
    font-weight:500;
    line-height:1.2;
    padding:10px 14px;
  }
  .partition-btn.active {
    background:#fff;
    border-color:#e6eaf2;
    color:#0f172a;
    font-weight:600;
  }
  .partition-indicator { font-size:11px; line-height:1; }
  .actions { margin-top:14px; display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
  .template-controls { display:flex; flex-direction:column; gap:8px; margin-bottom:8px; }
  .inline-actions { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
  .inline-actions button { flex:1 1 0; width:50%; }
  .storage-meta { margin-top:0; margin-bottom:8px; display:flex; align-items:center; gap:6px; font-size:12px; color:#64748b; }
  .templates-meta { margin-top:0; }
  .link-btn { border:0; background:transparent; color:#0f172a; padding:0; text-decoration:underline; font-size:12px; font-weight:500; }
  .plain-value { color:#0f172a; font-size:12px; font-weight:500; }
  .storage-picker { display:flex; gap:8px; align-items:center; margin-bottom:8px; }
  .storage-picker select { flex:1; min-width:0; }

  button { border-radius:14px; border:1px solid #e6eaf2; padding:10px 12px; background:#fff; cursor:pointer; }
  button:disabled { opacity:.6; cursor:not-allowed; }
  .primary { background:#0f172a; color:#fff; border-color:#0f172a; }
  .danger { border-color:#f3c0c0; color:#b91c1c; }
  .icon-btn { width:44px; min-width:44px; padding:10px 0; text-transform:uppercase; border-color:transparent; background:transparent; color:#b91c1c; }
  .danger.icon-btn { border-color:transparent; background:transparent; color:#b91c1c; }
  .tables-list .icon-btn { color:#fff; }
  .templates-list .icon-btn { color:#fff; }
  .tables-list .icon-btn, .templates-list .icon-btn { width:34px; min-width:34px; padding:6px 0; font-size:14px; }
  .activeitem .icon-btn { color:#fff; }
  .templates-list .activeitem .icon-btn { color:#b91c1c; }
  .refresh-btn { color:#16a34a; }

  .hint { margin:10px 0 0; color:#64748b; font-size:13px; }
  .alert { margin: 12px 0; padding: 10px 12px; border-radius: 14px; border: 1px solid #f3c0c0; background: #fff5f5; }
  .alert-title { font-weight: 700; margin-bottom: 6px; }

  .modal-bg { position: fixed; inset: 0; background: rgba(15,23,42,.35); display:flex; align-items:center; justify-content:center; padding: 18px; z-index: 1000; }
  .modal { width: min(560px, 100%); background:#fff; border-radius:18px; border:1px solid #e6eaf2; padding:14px; box-shadow:0 20px 60px rgba(15,23,42,.25); }
  .modal h3 { margin: 0 0 10px 0; }
  .modal-actions { display:flex; justify-content:flex-end; margin-top:12px; }
  pre { margin:0; white-space: pre-wrap; word-break: break-word; }
</style>
