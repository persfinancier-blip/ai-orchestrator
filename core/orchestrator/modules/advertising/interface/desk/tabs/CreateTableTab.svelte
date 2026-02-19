<!-- File: core/orchestrator/modules/advertising/interface/desk/tabs/CreateTableTab.svelte -->
<script lang="ts">
  export type Role = 'viewer' | 'operator' | 'data_admin';

  export let apiBase: string;
  export let role: Role;
  export let loading: boolean;
  export let dbStatus: 'checking' | 'ok' | 'error' = 'checking';
  export let dbStatusMessage = '';

  export let refreshTables: () => Promise<void>;
  export let onCreated: (schema: string, table: string) => void;

  export let headers: () => Record<string, string>;
  export let apiJson: <T = any>(url: string, init?: RequestInit) => Promise<T>;

  let error = '';

  let schema_name = '';
  let table_name = '';
  let description = '';
  let table_class = 'custom';

  const typeOptions = ['text', 'int', 'bigint', 'numeric', 'boolean', 'date', 'timestamptz', 'jsonb', 'uuid'];
  const CREATE_TIMEOUT_MS = 30000;
  const CREATE_BUTTON_LABEL = '\u0421\u043e\u0437\u0434\u0430\u0442\u044c \u0442\u0430\u0431\u043b\u0438\u0446\u0443';
  const CREATE_WAIT_LABEL = '\u0417\u0430\u043f\u0440\u043e\u0441 \u0441\u043e\u0437\u0434\u0430\u043d\u0438\u044f \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d. \u041e\u0436\u0438\u0434\u0430\u0435\u043c \u043e\u0442\u0432\u0435\u0442 \u0441\u0435\u0440\u0432\u0435\u0440\u0430...';
  const DEFAULT_DB_STATUS_LABEL = '\u0421\u0442\u0430\u0442\u0443\u0441 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u044f \u043a \u0431\u0430\u0437\u0435: \u043d\u0435\u0442 \u0434\u0430\u043d\u043d\u044b\u0445.';

  type ColumnDef = { field_name: string; field_type: string; description?: string };
  let columns: ColumnDef[] = [{ field_name: '', field_type: 'text', description: '' }];

  let partition_enabled = false;
  let partition_column = 'event_date';
  let partition_interval: 'day' | 'month' = 'day';

  let result_modal_open = false;
  let result_modal_title = '';
  let result_modal_text = '';
  let result_created_schema = '';
  let result_created_table = '';
  let result_is_success = false;
  let creating = false;

  function canWrite(): boolean {
    return role === 'data_admin';
  }

  function pickTemplateBronze() {
    schema_name = 'bronze';
    table_name = 'wb_ads_raw1';
    table_class = 'bronze_raw';
    description = 'Сырые ответы API (append-only JSON)';
    columns = [
      { field_name: 'dataset', field_type: 'text', description: 'dataset name' },
      { field_name: 'endpoint', field_type: 'text', description: 'endpoint name' },
      { field_name: 'request_id', field_type: 'text', description: 'request id' },
      { field_name: 'ingested_at', field_type: 'timestamptz', description: 'ingest timestamp' },
      { field_name: 'payload', field_type: 'jsonb', description: 'raw payload' }
    ];
    partition_enabled = true;
    partition_column = 'ingested_at';
    partition_interval = 'day';
  }

  function addField() {
    columns = [...columns, { field_name: '', field_type: 'text', description: '' }];
  }

  function removeField(ix: number) {
    columns = columns.filter((_, i) => i !== ix);
    if (columns.length === 0) columns = [{ field_name: '', field_type: 'text', description: '' }];
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

  function validate() {
    if (!schema_name.trim()) throw new Error('Укажи схему');
    if (!table_name.trim()) throw new Error('Укажи имя таблицы');
    if (!table_class.trim()) throw new Error('Укажи класс');
    const cols = normalizeColumns(columns);
    if (cols.length === 0) throw new Error('Добавь хотя бы одно поле');
    for (const c of cols) if (!c.field_type) throw new Error("Укажи тип для каждого поля");

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

  function closeResultModal() {
    result_modal_open = false;
    if (result_is_success && result_created_schema && result_created_table) {
      onCreated?.(result_created_schema, result_created_table);
    }
  }
</script>

{#if error}
  <div class="alert">
    <div class="alert-title">Ошибка создания</div>
    <pre>{error}</pre>
  </div>
{/if}

<section class="grid single">
  <div class="panel">
    <div class="panel-head">
      <h2>Создать таблицу</h2>
      <div class="quick">
        <button on:click={pickTemplateBronze}>Заполнить: Bronze</button>
        <button on:click={refreshTables} disabled={loading}>{loading ? 'Загрузка…' : 'Обновить список'}</button>
      </div>
    </div>

    <div class="form">
      <label>
        Схема
        <input bind:value={schema_name} placeholder="например: showcase / bronze / silver_adv" />
      </label>

      <label>
        Таблица
        <input bind:value={table_name} placeholder="например: advertising" />
      </label>

      <label>
        Класс (для себя)
        <select bind:value={table_class}>
          <option value="custom">custom</option>
          <option value="bronze_raw">bronze_raw</option>
          <option value="silver_table">silver_table</option>
          <option value="showcase_table">showcase_table</option>
        </select>
      </label>

      <label>
        Описание
        <input bind:value={description} placeholder="что это за таблица" />
      </label>
    </div>

    <div class="fields">
      <div class="fields-head">
        <h3>Поля</h3>
      </div>

      {#each columns as c, ix}
        <div class="field-row">
          <input placeholder="имя поля" bind:value={c.field_name} />
          <select bind:value={c.field_type}>
            {#each typeOptions as t}
              <option value={t}>{t}</option>
            {/each}
          </select>
          <input placeholder="описание" bind:value={c.description} />
          <button class="danger" on:click={() => removeField(ix)} title="Удалить поле">Удалить</button>
        </div>
      {/each}

      <div class="fields-footer">
        <button on:click={addField}>+ Добавить поле</button>
      </div>
    </div>

    <div class="panel2">
      <h3>Партиционирование</h3>
      <label class="row">
        <input type="checkbox" bind:checked={partition_enabled} />
        <span>Включить партиции</span>
      </label>

      {#if partition_enabled}
        <div class="form">
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
      {/if}
    </div>

    <div class="actions">
      <button class="primary" on:click={createTableNow} disabled={loading || creating || !canWrite()}>
        {CREATE_BUTTON_LABEL}
      </button>
      {#if creating}
        <span class="hint">{CREATE_WAIT_LABEL}</span>
      {/if}
    </div>

    <div class="statusline" class:ok={dbStatus === 'ok'} class:error={dbStatus === 'error'}>
      {dbStatusMessage || DEFAULT_DB_STATUS_LABEL}
    </div>

    {#if !canWrite()}
      <p class="hint">Кнопка активна только при роли <b>data_admin</b>.</p>
    {/if}
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
  .grid { display:grid; gap: 12px; margin-top: 12px; }
  .grid.single { grid-template-columns: 1fr; }

  .panel { background:#fff; border:1px solid #e6eaf2; border-radius:18px; padding:14px; box-shadow:0 6px 20px rgba(15,23,42,.05); }
  .panel-head { display:flex; align-items:center; justify-content:space-between; gap:12px; }
  .panel-head h2 { margin:0; font-size:18px; }
  .quick { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }

  .form { display:grid; grid-template-columns: 1fr 1fr; gap:10px; margin-top:12px; }
  @media (max-width: 1100px) { .form { grid-template-columns: 1fr; } }
  .form label { display:flex; flex-direction:column; gap:6px; font-size:13px; }
  .form input, .form select { border-radius:14px; border:1px solid #e6eaf2; padding:10px 12px; outline:none; background:#fff; }

  .fields { margin-top:14px; border-top:1px dashed #e6eaf2; padding-top:14px; }
  .fields-head h3 { margin:0; font-size:16px; }
  .field-row { display:grid; grid-template-columns: 1.2fr .8fr 1.6fr auto; gap:8px; margin-top:10px; }
  @media (max-width: 1100px) { .field-row { grid-template-columns: 1fr; } }
  .field-row input, .field-row select { border-radius:14px; border:1px solid #e6eaf2; padding:10px 12px; }
  .fields-footer { margin-top:12px; }

  .panel2 { margin-top:14px; border-top:1px dashed #e6eaf2; padding-top:14px; }
  .panel2 h3 { margin:0 0 10px 0; font-size:16px; }
  .row { display:flex; align-items:center; gap:10px; }

  .actions { margin-top:14px; display:flex; gap:10px; align-items:center; }

  button { border-radius:14px; border:1px solid #e6eaf2; padding:10px 12px; background:#fff; cursor:pointer; }
  button:disabled { opacity:.6; cursor:not-allowed; }
  .primary { background:#0f172a; color:#fff; border-color:#0f172a; }
  .danger { border-color:#f3c0c0; color:#b91c1c; }

  .hint { margin:10px 0 0; color:#64748b; font-size:13px; }
  .statusline { margin-top:12px; border:1px solid #e6eaf2; border-radius:12px; padding:10px 12px; background:#f8fafc; color:#334155; font-size:13px; }
  .statusline.ok { border-color:#bbf7d0; background:#f0fdf4; color:#166534; }
  .statusline.error { border-color:#fecaca; background:#fef2f2; color:#991b1b; }

  .alert { margin: 12px 0; padding: 10px 12px; border-radius: 14px; border: 1px solid #f3c0c0; background: #fff5f5; }
  .alert-title { font-weight: 700; margin-bottom: 6px; }
  pre { margin:0; white-space: pre-wrap; word-break: break-word; }
  .modal-bg { position: fixed; inset: 0; background: rgba(15,23,42,.35); display:flex; align-items:center; justify-content:center; padding: 18px; z-index: 1000; }
  .modal { width: min(560px, 100%); background:#fff; border-radius:18px; border:1px solid #e6eaf2; padding:14px; box-shadow:0 20px 60px rgba(15,23,42,.25); }
  .modal h3 { margin: 0 0 10px 0; }
  .modal-actions { display:flex; justify-content:flex-end; margin-top:12px; }
</style>
