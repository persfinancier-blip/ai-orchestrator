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
  type TableTemplate = {
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
    storage_ctids?: string[];
  };

  const typeOptions = ['text', 'int', 'bigint', 'numeric', 'boolean', 'date', 'timestamptz', 'jsonb', 'uuid'];
  const CREATE_TIMEOUT_MS = 30000;
  const CREATE_BUTTON_LABEL = 'Создать таблицу';
  const CREATE_WAIT_LABEL = 'Запрос создания отправлен. Ожидаем ответ сервера...';
  const TABLE_TEMPLATES_KEY = 'ao_create_table_templates_v1';
  const TABLE_TEMPLATES_STORAGE_KEY = 'ao_create_table_templates_storage_v1';
  const STORAGE_DEFAULT_SCHEMA = 'ao_system';
  const STORAGE_DEFAULT_TABLE = 'table_templates_store';
  const STORAGE_TEMPLATE_NAME = 'System: Хранилище шаблонов таблиц';

  let error = '';
  let schema_name = '';
  let table_name = '';
  let description = '';
  let descriptionEl: HTMLTextAreaElement | null = null;
  let table_class = 'custom';
  let columns: ColumnDef[] = [{ field_name: '', field_type: 'text', description: '' }];
  let partition_enabled = false;
  let partition_column = 'event_date';
  let partition_interval: 'day' | 'month' = 'day';

  let tableTemplates: TableTemplate[] = [];
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
  let tablesSignature = '';

  function uid() {
    return `${Date.now()}_${Math.random().toString(16).slice(2)}`;
  }

  function canWrite(): boolean {
    return role === 'data_admin';
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

  function normalizeColumns(cols: ColumnDef[]) {
    return cols
      .map((c) => ({
        field_name: (c.field_name || '').trim(),
        field_type: (c.field_type || '').trim(),
        description: (c.description || '').trim()
      }))
      .filter((c) => c.field_name.length > 0);
  }

  function bronzeTemplate(): TableTemplate {
    return {
      id: 'builtin_bronze',
      name: 'Bronze',
      schema_name: 'bronze',
      table_name: 'wb_ads_raw1',
      table_class: 'bronze_raw',
      description: 'Сырые ответы API (append-only JSON)',
      columns: [
        { field_name: 'dataset', field_type: 'text', description: 'dataset name' },
        { field_name: 'endpoint', field_type: 'text', description: 'endpoint name' },
        { field_name: 'request_id', field_type: 'text', description: 'request id' },
        { field_name: 'ingested_at', field_type: 'timestamptz', description: 'ingest timestamp' },
        { field_name: 'payload', field_type: 'jsonb', description: 'raw payload' }
      ],
      partition_enabled: true,
      partition_column: 'ingested_at',
      partition_interval: 'day'
    };
  }

  function silverTemplate(): TableTemplate {
    return {
      id: 'builtin_silver',
      name: 'Silver',
      schema_name: 'silver_adv',
      table_name: 'wb_ads_daily',
      table_class: 'silver_table',
      description: 'Дневная агрегированная таблица рекламы',
      columns: [
        { field_name: 'event_date', field_type: 'date', description: 'дата метрики' },
        { field_name: 'campaign_id', field_type: 'text', description: 'идентификатор кампании' },
        { field_name: 'impressions', field_type: 'bigint', description: 'показы' },
        { field_name: 'clicks', field_type: 'bigint', description: 'клики' },
        { field_name: 'spend', field_type: 'numeric', description: 'расход' }
      ],
      partition_enabled: true,
      partition_column: 'event_date',
      partition_interval: 'day'
    };
  }

  function storageSystemTemplate(): TableTemplate {
    return {
      id: 'builtin_storage_templates',
      name: STORAGE_TEMPLATE_NAME,
      schema_name: STORAGE_DEFAULT_SCHEMA,
      table_name: STORAGE_DEFAULT_TABLE,
      table_class: 'custom',
      description: 'Служебная таблица для хранения шаблонов блока «Создание таблиц»',
      columns: [
        { field_name: 'template_name', field_type: 'text', description: 'имя шаблона' },
        { field_name: 'schema_name', field_type: 'text', description: 'схема таблицы' },
        { field_name: 'table_name', field_type: 'text', description: 'имя таблицы' },
        { field_name: 'table_class', field_type: 'text', description: 'класс таблицы' },
        { field_name: 'description', field_type: 'text', description: 'описание' },
        { field_name: 'columns', field_type: 'jsonb', description: 'json список полей' },
        { field_name: 'partition_enabled', field_type: 'boolean', description: 'включено ли партиционирование' },
        { field_name: 'partition_column', field_type: 'text', description: 'колонка партиционирования' },
        { field_name: 'partition_interval', field_type: 'text', description: 'интервал партиционирования' }
      ],
      partition_enabled: false,
      partition_column: '',
      partition_interval: 'day'
    };
  }

  const STORAGE_REQUIRED_COLUMNS = [
    { name: 'template_name', types: ['text', 'character varying', 'varchar'] },
    { name: 'schema_name', types: ['text', 'character varying', 'varchar'] },
    { name: 'table_name', types: ['text', 'character varying', 'varchar'] },
    { name: 'table_class', types: ['text', 'character varying', 'varchar'] },
    { name: 'description', types: ['text', 'character varying', 'varchar'] },
    { name: 'columns', types: ['jsonb', 'json'] },
    { name: 'partition_enabled', types: ['boolean', 'bool'] },
    { name: 'partition_column', types: ['text', 'character varying', 'varchar'] },
    { name: 'partition_interval', types: ['text', 'character varying', 'varchar'] }
  ];

  function normalizeTypeName(type: string) {
    return String(type || '').toLowerCase().trim();
  }

  function storageInstruction(prefix: string) {
    return `${prefix} Выберите системный шаблон «${STORAGE_TEMPLATE_NAME}», создайте таблицу и подключите ее здесь.`;
  }

  function parseStorageTableConfig() {
    try {
      const raw = JSON.parse(localStorage.getItem(TABLE_TEMPLATES_STORAGE_KEY) || '{}');
      if (raw && typeof raw.schema === 'string' && typeof raw.table === 'string') {
        storage_schema = raw.schema.trim() || STORAGE_DEFAULT_SCHEMA;
        storage_table = raw.table.trim() || STORAGE_DEFAULT_TABLE;
      }
    } catch {
      storage_schema = STORAGE_DEFAULT_SCHEMA;
      storage_table = STORAGE_DEFAULT_TABLE;
    }
  }

  function saveStorageTableConfig() {
    localStorage.setItem(TABLE_TEMPLATES_STORAGE_KEY, JSON.stringify({ schema: storage_schema, table: storage_table }));
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
      for (const need of STORAGE_REQUIRED_COLUMNS) {
        const actual = map.get(need.name);
        if (!actual || !need.types.some((t) => actual.includes(t))) {
          storage_status = 'invalid';
          storage_status_message = storageInstruction(
            `Структура ${schema}.${table} не подходит: колонка ${need.name} отсутствует или имеет неверный тип.`
          );
          return false;
        }
      }

      storage_status = 'ok';
      storage_status_message = `Хранилище подключено: ${schema}.${table}`;
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
      loadTableTemplatesLocalOnly();
      return;
    }
    try {
      const j = await apiJson<{ rows: any[] }>(
        `${apiBase}/preview?schema=${encodeURIComponent(storage_schema)}&table=${encodeURIComponent(storage_table)}&limit=5000`
      );
      const rows = Array.isArray(j?.rows) ? j.rows : [];
      const custom: TableTemplate[] = [];
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
          storage_ctids: r?.ctid ? [String(r.ctid)] : []
        });
      }
      tableTemplates = [bronzeTemplate(), silverTemplate(), storageSystemTemplate(), ...custom];
      error = '';
    } catch (e: any) {
      storage_status = 'error';
      storage_status_message = storageInstruction('Ошибка загрузки шаблонов из таблицы.');
      loadTableTemplatesLocalOnly();
      error = e?.message || String(e);
    }
  }

  function loadTableTemplatesLocalOnly() {
    try {
      const raw = JSON.parse(localStorage.getItem(TABLE_TEMPLATES_KEY) || '[]');
      const custom = Array.isArray(raw)
        ? raw.map((x: any) => ({
            id: String(x?.id || uid()),
            name: String(x?.name || ''),
            schema_name: String(x?.schema_name || ''),
            table_name: String(x?.table_name || ''),
            table_class: String(x?.table_class || 'custom'),
            description: String(x?.description || ''),
            columns: Array.isArray(x?.columns)
              ? x.columns.map((c: any) => ({
                  field_name: String(c?.field_name || ''),
                  field_type: String(c?.field_type || 'text'),
                  description: String(c?.description || '')
                }))
              : [],
            partition_enabled: Boolean(x?.partition_enabled),
            partition_column: String(x?.partition_column || 'event_date'),
            partition_interval: x?.partition_interval === 'month' ? 'month' : 'day'
          }))
        : [];
      tableTemplates = [bronzeTemplate(), silverTemplate(), storageSystemTemplate(), ...custom];
    } catch {
      tableTemplates = [bronzeTemplate(), silverTemplate(), storageSystemTemplate()];
    }
  }

  function applyTemplate(t: TableTemplate) {
    schema_name = t.schema_name;
    table_name = t.table_name;
    table_class = t.table_class;
    description = t.description;
    columns = (t.columns || []).map((c) => ({ ...c }));
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
    parseStorageTableConfig();
    await loadTemplatesFromStorage();
  }

  function saveTableTemplatesLocal() {
    const custom = tableTemplates.filter((t) => !t.id.startsWith('builtin_'));
    localStorage.setItem(TABLE_TEMPLATES_KEY, JSON.stringify(custom.slice(0, 300)));
  }

  async function saveTemplateToStorage(t: TableTemplate) {
    const isValid = await checkStorageTable(storage_schema, storage_table);
    if (!isValid) throw new Error(storage_status_message || 'Таблица хранения недоступна');
    const rows = await apiJson<{ rows: any[] }>(
      `${apiBase}/preview?schema=${encodeURIComponent(storage_schema)}&table=${encodeURIComponent(storage_table)}&limit=5000`
    );
    const found = (Array.isArray(rows?.rows) ? rows.rows : []).filter(
      (r) => String(r?.template_name || '').trim().toLowerCase() === t.name.toLowerCase()
    );
    for (const r of found) {
      if (r?.ctid) {
        await apiJson(`${apiBase}/rows/delete`, {
          method: 'POST',
          headers: headers(),
          body: JSON.stringify({ schema: storage_schema, table: storage_table, ctid: String(r.ctid) })
        });
      }
    }
    await apiJson(`${apiBase}/rows/add`, {
      method: 'POST',
      headers: headers(),
      body: JSON.stringify({
        schema: storage_schema,
        table: storage_table,
        row: {
          template_name: t.name,
          schema_name: t.schema_name,
          table_name: t.table_name,
          table_class: t.table_class,
          description: t.description,
          columns: JSON.stringify(t.columns || []),
          partition_enabled: !!t.partition_enabled,
          partition_column: t.partition_column || '',
          partition_interval: t.partition_interval || 'day'
        }
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
      if (r?.ctid) {
        await apiJson(`${apiBase}/rows/delete`, {
          method: 'POST',
          headers: headers(),
          body: JSON.stringify({ schema: storage_schema, table: storage_table, ctid: String(r.ctid) })
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

  async function startNewTemplate() {
    const name = String(templateNameDraft || '').trim();
    const cols = normalizeColumns(columns);
    if (!name) throw new Error('Укажи название шаблона');
    if (!cols.length) throw new Error('Добавь хотя бы одно поле');
    if (storage_status !== 'ok') throw new Error(storage_status_message || 'Сначала подключи таблицу хранения шаблонов');
    if (tableTemplates.some((t) => !t.id.startsWith('builtin_') && t.name.trim().toLowerCase() === name.toLowerCase())) {
      throw new Error('Шаблон с таким названием уже существует');
    }

    const newTemplate: TableTemplate = {
      id: uid(),
      name,
      schema_name: schema_name.trim(),
      table_name: table_name.trim(),
      table_class: table_class.trim() || 'custom',
      description: description.trim(),
      columns: cols,
      partition_enabled,
      partition_column: partition_column.trim(),
      partition_interval
    };

    await saveTemplateToStorage(newTemplate);
    await loadTemplatesFromStorage();
    const actual = tableTemplates.find((x) => x.name.trim().toLowerCase() === name.toLowerCase());
    selectedTemplateId = actual?.id || '';
    templateNameDraft = actual?.name || name;
    error = '';
  }

  async function saveCurrentTemplate() {
    if (!selectedTemplateId) throw new Error('Сначала добавь или выбери шаблон');
    if (selectedTemplateId.startsWith('builtin_')) {
      throw new Error('Встроенный шаблон нельзя сохранить. Нажми «Добавить шаблон»');
    }

    const idx = tableTemplates.findIndex((x) => x.id === selectedTemplateId);
    if (idx < 0) throw new Error('Активный шаблон не найден');
    if (storage_status !== 'ok') throw new Error(storage_status_message || 'Сначала подключи таблицу хранения шаблонов');

    const name = String(templateNameDraft || '').trim();
    const cols = normalizeColumns(columns);
    if (!name) throw new Error('Укажи название шаблона');
    if (!cols.length) throw new Error('Добавь хотя бы одно поле');

    const updated = {
      ...tableTemplates[idx],
      name,
      schema_name: schema_name.trim(),
      table_name: table_name.trim(),
      table_class: table_class.trim() || 'custom',
      description: description.trim(),
      columns: cols,
      partition_enabled,
      partition_column: partition_column.trim(),
      partition_interval
    } as TableTemplate;

    await saveTemplateToStorage(updated);
    await loadTemplatesFromStorage();
    const actual = tableTemplates.find((x) => x.name.trim().toLowerCase() === name.toLowerCase());
    selectedTemplateId = actual?.id || '';
    templateNameDraft = actual?.name || name;
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
    saveStorageTableConfig();
    storage_picker_open = false;
    await loadTemplatesFromStorage();
  }

  function addField() {
    columns = [...columns, { field_name: '', field_type: 'text', description: '' }];
  }

  function removeField(ix: number) {
    columns = columns.filter((_, i) => i !== ix);
    if (columns.length === 0) columns = [{ field_name: '', field_type: 'text', description: '' }];
  }

  function validate() {
    if (!schema_name.trim()) throw new Error('Укажи схему');
    if (!table_name.trim()) throw new Error('Укажи имя таблицы');
    if (!table_class.trim()) throw new Error('Укажи класс');

    const cols = normalizeColumns(columns);
    if (cols.length === 0) throw new Error('Добавь хотя бы одно поле');
    for (const c of cols) if (!c.field_type) throw new Error('Укажи тип для каждого поля');

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
      const cols = normalizeColumns(columns);

      const response = await apiJson(`${apiBase}/tables/create`, {
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

      result_created_schema = schema_name.trim();
      result_created_table = table_name.trim();
      result_is_success = true;
      result_modal_title = 'Таблица создана';
      result_modal_text = response ? JSON.stringify(response, null, 2) : 'Операция выполнена успешно.';
      result_modal_open = true;

      forceRefreshTables().catch(() => {
        // Ошибка обновления списка не должна блокировать результат создания
      });
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
                <button class="danger icon-btn" on:click={() => deleteTableNow(t.schema_name, t.table_name)} title="Удалить таблицу">x</button>
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
              <button class="danger icon-btn" on:click={() => removeField(ix)} title="Удалить поле">x</button>
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
      <div class="aside-title">Шаблоны таблиц</div>
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
      <div class="template-controls">
        <input class="template-name" bind:value={templateNameDraft} placeholder="Название шаблона" />
        <div class="inline-actions">
          <button on:click={onAddTemplateClick}>Добавить шаблон</button>
          <button on:click={onSaveTemplateClick}>Сохранить шаблон</button>
        </div>
      </div>
      <div class="list templates-list">
        {#each tableTemplates as t}
          <div class="row-item" class:activeitem={selectedTemplateId === t.id}>
            <button class="item-button" on:click={() => applySelectedTemplate(t.id)}>{t.name}</button>
            <button class="danger icon-btn" on:click={() => onDeleteTemplateClick(t.id)} title="Удалить шаблон">x</button>
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
  .aside-title { font-weight:700; font-size:14px; line-height:1.3; margin-bottom:8px; }
  .list { display:flex; flex-direction:column; gap:8px; overflow:visible; max-height:none; }
  .row-item { display:grid; grid-template-columns: 1fr auto; gap:8px; align-items:center; border:1px solid #e6eaf2; border-radius:14px; background:#fff; padding:10px 12px; }
  .row-name { font-weight:400; font-size:14px; line-height:1.3; word-break:break-word; }
  .item-button { text-align:left; border:0; background:transparent; padding:0; font-weight:400; font-size:14px; line-height:1.3; color:inherit; }
  .activeitem { border-color:#0f172a; background:#0f172a; color:#fff; }
  .activeitem .item-button { color:#fff; }
  .tables-list .row-item { background:#0f172a; border-color:#0f172a; }
  .tables-list .row-name { color:#fff; }
  .templates-list .row-item { background:#0f172a; border-color:#0f172a; }
  .templates-list .item-button { color:#fff; }
  .templates-list .activeitem { background:#fff; border-color:#e6eaf2; color:#0f172a; }
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
  .storage-meta { margin-top:-2px; margin-bottom:8px; display:flex; align-items:center; gap:6px; font-size:12px; color:#64748b; }
  .templates-meta { margin-top:6px; }
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
