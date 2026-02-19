<!-- File: core/orchestrator/modules/advertising/interface/desk/tabs/CreateTableTab.svelte -->
<script lang="ts">
  export type Role = 'viewer' | 'operator' | 'data_admin';

  export let apiBase: string;
  export let role: Role;
  export let loading: boolean;

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

  type ColumnDef = { field_name: string; field_type: string; description?: string };
  let columns: ColumnDef[] = [{ field_name: '', field_type: 'text', description: '' }];

  let partition_enabled = false;
  let partition_column = 'event_date';
  let partition_interval: 'day' | 'month' = 'day';

  let result_modal_open = false;
  let result_modal_title = '';
  let result_modal_text = '';

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
        name: (c.field_name || '').trim(),
        type: (c.field_type || '').trim(),
        description: (c.description || '').trim()
      }))
      .filter((c) => c.name.length > 0);
  }

  function validate() {
    if (!schema_name.trim()) throw new Error('Укажи схему');
    if (!table_name.trim()) throw new Error('Укажи имя таблицы');
    if (!table_class.trim()) throw new Error('Укажи класс');
    const cols = normalizeColumns(columns);
    if (cols.length === 0) throw new Error('Добавь хотя бы одно поле');
    for (const c of cols) if (!c.type) throw new Error(`Для поля "${c.name}" не указан тип`);

    if (partition_enabled) {
      if (!partition_column.trim()) throw new Error('Партиционирование включено: укажи колонку');
      if (!partition_interval) throw new Error('Партиционирование включено: укажи интервал');
    }
  }

  async function createTableNow() {
    error = '';
    try {
      if (!canWrite()) throw new Error('Недостаточно прав (нужна роль data_admin)');

      validate();

      const cols = normalizeColumns(columns);
      const test_row = parseTestRow();

      const response = await apiJson(`${apiBase}/tables/create`, {
        method: 'POST',
        headers: headers(),
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

      await refreshTables();
      onCreated?.(schema_name.trim(), table_name.trim());
      result_modal_title = 'Таблица создана';
      result_modal_text = response ? JSON.stringify(response, null, 2) : 'Операция выполнена успешно.';
      result_modal_open = true;
    } catch (e: any) {
      error = e?.message ?? String(e);
      result_modal_title = 'Ошибка создания';
      result_modal_text = error;
      result_modal_open = true;
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
      <button class="primary" on:click={createTableNow} disabled={loading || !canWrite()}>
        Создать таблицу
      </button>
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
    on:click={() => (result_modal_open = false)}
    on:keydown={(e) => (e.key === 'Escape' || e.key === 'Enter' || e.key === ' ') && (result_modal_open = false)}
  >
    <div class="modal" on:click|stopPropagation on:keydown|stopPropagation>
      <h3>{result_modal_title}</h3>
      <pre>{result_modal_text}</pre>
      <div class="modal-actions">
        <button class="primary" on:click={() => (result_modal_open = false)}>OK</button>
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

  .alert { margin: 12px 0; padding: 10px 12px; border-radius: 14px; border: 1px solid #f3c0c0; background: #fff5f5; }
  .alert-title { font-weight: 700; margin-bottom: 6px; }
  pre { margin:0; white-space: pre-wrap; word-break: break-word; }
  .modal-bg { position: fixed; inset: 0; background: rgba(15,23,42,.35); display:flex; align-items:center; justify-content:center; padding: 18px; z-index: 1000; }
  .modal { width: min(560px, 100%); background:#fff; border-radius:18px; border:1px solid #e6eaf2; padding:14px; box-shadow:0 20px 60px rgba(15,23,42,.25); }
  .modal h3 { margin: 0 0 10px 0; }
  .modal-actions { display:flex; justify-content:flex-end; margin-top:12px; }
</style>
