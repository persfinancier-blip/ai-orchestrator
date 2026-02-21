<!-- File: core/orchestrator/modules/advertising/interface/desk/tabs/CreateTableTab.svelte -->
<script lang="ts">
  import { onMount, tick } from 'svelte';

  export type Role = 'viewer' | 'operator' | 'data_admin';
  export type ExistingTable = { schema_name: string; table_name: string };

  export let apiBase: string;
  export let role: Role;
  export let loading: boolean;
  export let dbStatus: 'checking' | 'ok' | 'error' = 'checking';
  export let dbStatusMessage = '';

  export let refreshTables: () => Promise<void>;
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
  };

  const typeOptions = ['text', 'int', 'bigint', 'numeric', 'boolean', 'date', 'timestamptz', 'jsonb', 'uuid'];
  const CREATE_TIMEOUT_MS = 30000;
  const CREATE_BUTTON_LABEL = 'Создать таблицу';
  const CREATE_WAIT_LABEL = 'Запрос создания отправлен. Ожидаем ответ сервера...';
  const DEFAULT_DB_STATUS_LABEL = 'Статус подключения к базе: нет данных.';
  const TABLE_TEMPLATES_KEY = 'ao_create_table_templates_v1';

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

  let result_modal_open = false;
  let result_modal_title = '';
  let result_modal_text = '';
  let result_created_schema = '';
  let result_created_table = '';
  let result_is_success = false;
  let creating = false;

  function uid() {
    return `${Date.now()}_${Math.random().toString(16).slice(2)}`;
  }

  function canWrite(): boolean {
    return role === 'data_admin';
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

  function loadTableTemplates() {
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
      tableTemplates = [bronzeTemplate(), silverTemplate(), ...custom];
    } catch {
      tableTemplates = [bronzeTemplate(), silverTemplate()];
    }
  }

  function saveTableTemplates() {
    const custom = tableTemplates.filter((t) => !t.id.startsWith('builtin_'));
    localStorage.setItem(TABLE_TEMPLATES_KEY, JSON.stringify(custom.slice(0, 300)));
  }

  function applySelectedTemplate(id: string) {
    selectedTemplateId = id;
    const t = tableTemplates.find((x) => x.id === id);
    if (!t) return;
    applyTemplate(t);
  }

  function startNewTemplate() {
    const name = String(templateNameDraft || '').trim();
    const cols = normalizeColumns(columns);
    if (!name) throw new Error('Укажи название шаблона');
    if (!cols.length) throw new Error('Добавь хотя бы одно поле');

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

    tableTemplates = [newTemplate, ...tableTemplates];
    selectedTemplateId = newTemplate.id;
    templateNameDraft = newTemplate.name;
    saveTableTemplates();
    error = '';
  }

  function saveCurrentTemplate() {
    if (!selectedTemplateId) throw new Error('Сначала добавь или выбери шаблон');
    if (selectedTemplateId.startsWith('builtin_')) {
      throw new Error('Встроенный шаблон нельзя сохранить. Нажми «Добавить шаблон»');
    }

    const idx = tableTemplates.findIndex((x) => x.id === selectedTemplateId);
    if (idx < 0) throw new Error('Активный шаблон не найден');

    const name = String(templateNameDraft || '').trim();
    const cols = normalizeColumns(columns);
    if (!name) throw new Error('Укажи название шаблона');
    if (!cols.length) throw new Error('Добавь хотя бы одно поле');

    tableTemplates[idx] = {
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
    };

    saveTableTemplates();
    error = '';
  }

  function deleteTemplateById(id: string) {
    if (!id) throw new Error('Сначала выбери шаблон');
    if (id.startsWith('builtin_')) throw new Error('Встроенный шаблон удалить нельзя');

    tableTemplates = tableTemplates.filter((x) => x.id !== id);
    if (selectedTemplateId === id) {
      selectedTemplateId = '';
      templateNameDraft = '';
    }
    saveTableTemplates();
    error = '';
  }

  function onSaveTemplateClick() {
    try {
      saveCurrentTemplate();
    } catch (e: any) {
      error = e?.message || String(e);
    }
  }

  function onDeleteTemplateClick(id: string) {
    try {
      deleteTemplateById(id);
    } catch (e: any) {
      error = e?.message || String(e);
    }
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

      refreshTables().catch(() => {
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
      await refreshTables();
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
    <div class="muted">{dbStatusMessage || DEFAULT_DB_STATUS_LABEL}</div>
  </div>

  <div class="layout">
    <aside class="aside">
      <div class="aside-head">
        <div class="aside-title">Текущие таблицы</div>
        <button class="icon-btn refresh-btn" on:click={refreshTables} disabled={loading} title="Обновить список">↻</button>
      </div>
      {#if existingTables.length === 0}
        <div class="hint">Пока нет данных.</div>
      {:else}
        <div class="list tables-list">
          {#each existingTables as t}
            <div class="row-item">
              <div class="row-name">{t.schema_name}.{t.table_name}</div>
              <button class="danger icon-btn" on:click={() => deleteTableNow(t.schema_name, t.table_name)} title="Удалить таблицу">x</button>
            </div>
          {/each}
        </div>
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

          <div class="w100 partition-toggle">
            <label class="inline-check">
              <input type="checkbox" bind:checked={partition_enabled} />
              <span>Партиционирование</span>
            </label>
          </div>

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
            <button on:click={addField}>+ Добавить поле</button>
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
      <div class="template-controls">
        <input class="template-name" bind:value={templateNameDraft} placeholder="Название шаблона" />
        <div class="inline-actions">
          <button on:click={startNewTemplate}>Добавить шаблон</button>
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
  .muted { color:#64748b; font-size:13px; }

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

  .partition-toggle { display:flex; align-items:flex-start; justify-content:flex-start; }
  .inline-check { display:flex; align-items:center; gap:8px; font-size:13px; color:#334155; margin:0; }
  .actions { margin-top:14px; display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
  .template-controls { display:flex; flex-direction:column; gap:8px; margin-bottom:8px; }
  .inline-actions { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }

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
