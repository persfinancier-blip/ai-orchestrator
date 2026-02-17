<!-- core/orchestrator/modules/advertising/interface/desk/DataDesk.svelte -->
<script lang="ts">
  import { onMount } from 'svelte';

  type ColumnDef = { field_name: string; field_type: string; description?: string };
  type ExistingTable = { schema_name: string; table_name: string };

  let role: 'viewer' | 'operator' | 'data_admin' = 'data_admin';

  let loading = false;
  let error = '';

  let existingTables: ExistingTable[] = [];

  // Конструктор: пользователь вводит сам
  let schema_name = '';
  let table_name = '';
  let table_class = 'custom';
  let description = '';

  const typeOptions = ['text', 'integer', 'bigint', 'numeric', 'boolean', 'date', 'timestamptz', 'jsonb', 'uuid'];
  let columns: ColumnDef[] = [{ field_name: '', field_type: 'text', description: '' }];

  // Партиционирование
  let partition_enabled = false;
  let partition_column = 'event_date';
  let partition_interval: 'day' | 'month' = 'day';

  // Тестовая запись (JSON)
  let test_row_text = '';

  // Предпросмотр
  let preview_schema = '';
  let preview_table = '';
  let preview_rows: any[] = [];
  let preview_error = '';
  let preview_loading = false;

  // Меню
  let showAdvanced = false;

  function setErr(e: any) {
    error = String(e?.details || e?.message || e);
  }

  function isValidIdentifier(s: string) {
    return /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(s);
  }

  function canCreateTable() {
    if (role !== 'data_admin') return false;
    if (!schema_name.trim() || !table_name.trim()) return false;
    if (!isValidIdentifier(schema_name.trim())) return false;
    if (!isValidIdentifier(table_name.trim())) return false;

    const clean = columns.filter((c) => c.field_name.trim());
    if (clean.length === 0) return false;

    for (const c of clean) {
      if (!isValidIdentifier(c.field_name.trim())) return false;
      if (!c.field_type) return false;
    }

    if (partition_enabled) {
      if (!partition_column.trim()) return false;
      if (!clean.some((c) => c.field_name.trim() === partition_column.trim())) return false;
    }

    return true;
  }

  async function refresh() {
    loading = true;
    error = '';
    try {
      const r = await fetch('/ai-orchestrator/api/tables');
      const j = await r.json();
      if (!r.ok) throw new Error(j.error || 'failed_to_list');

      existingTables = j.existing_tables || [];

      if (!preview_schema && existingTables.length) {
        preview_schema = existingTables[0].schema_name;
        preview_table = existingTables[0].table_name;
      }
    } catch (e: any) {
      setErr(e);
    } finally {
      loading = false;
    }
  }

  function addColumn() {
    columns = [...columns, { field_name: '', field_type: 'text', description: '' }];
  }

  function removeColumn(ix: number) {
    columns = columns.filter((_, i) => i !== ix);
    if (!columns.length) columns = [{ field_name: '', field_type: 'text', description: '' }];
  }

  function parseTestRow(): any | null {
    const t = test_row_text.trim();
    if (!t) return null;
    const parsed = JSON.parse(t);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new Error('Тестовая запись должна быть JSON объектом (например {"a":1})');
    }
    return parsed;
  }

  async function createTableNow() {
    error = '';
    try {
      const test_row = parseTestRow();

      const cleanCols = columns
        .map((c) => ({
          field_name: String(c.field_name || '').trim(),
          field_type: String(c.field_type || '').trim(),
          description: c.description ? String(c.description).trim() : ''
        }))
        .filter((c) => c.field_name);

      const r = await fetch('/ai-orchestrator/api/tables/create', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({
          schema_name: schema_name.trim(),
          table_name: table_name.trim(),
          table_class,
          description: description.trim(),
          columns: cleanCols,
          partitioning: partition_enabled
            ? { enabled: true, column: partition_column.trim(), interval: partition_interval }
            : { enabled: false },
          test_row
        })
      });

      const j = await r.json();
      if (!r.ok) throw new Error(j.details || j.error || 'create_failed');

      await refresh();
      alert('Таблица создана');
    } catch (e: any) {
      setErr(e);
    }
  }

  async function loadPreview() {
    preview_loading = true;
    preview_error = '';
    preview_rows = [];
    try {
      const url =
        `/ai-orchestrator/api/preview?schema=${encodeURIComponent(preview_schema)}` +
        `&table=${encodeURIComponent(preview_table)}&limit=5`;
      const r = await fetch(url);
      const j = await r.json();
      if (!r.ok) throw new Error(j.details || j.error || 'preview_failed');
      preview_rows = j.rows || [];
    } catch (e: any) {
      preview_error = String(e?.message || e);
    } finally {
      preview_loading = false;
    }
  }

  function pickTemplateBronze() {
    schema_name = 'bronze';
    table_name = 'wb_ads_raw';
    table_class = 'bronze_raw';
    description = 'WB raw ingestion (append-only JSON)';
    columns = [
      { field_name: 'dataset', field_type: 'text', description: 'dataset name' },
      { field_name: 'endpoint', field_type: 'text', description: 'endpoint name' },
      { field_name: 'ingested_at', field_type: 'timestamptz', description: 'ingest timestamp' },
      { field_name: 'event_date', field_type: 'date', description: 'event/business date' },
      { field_name: 'idempotency_key', field_type: 'text', description: 'dedupe key' },
      { field_name: 'payload', field_type: 'jsonb', description: 'raw json' }
    ];
    partition_enabled = true;
    partition_column = 'event_date';
    partition_interval = 'day';
    test_row_text = '';
  }

  onMount(refresh);
</script>

<main class="root">
  <header class="header">
    <div>
      <h1>Конструктор таблиц</h1>
      <p class="sub">
        Создаёт схему/таблицу/поля по твоему вводу + опции (тестовая запись, партиции). Показывает текущие таблицы и
        предпросмотр 5 строк.
      </p>
    </div>

    <div class="role">
      <span>Роль:</span>
      <select bind:value={role}>
        <option value="viewer">viewer</option>
        <option value="operator">operator</option>
        <option value="data_admin">data_admin</option>
      </select>
    </div>
  </header>

  {#if error}
    <div class="alert">
      <div class="alert-title">Ошибка</div>
      <pre>{error}</pre>
    </div>
  {/if}

  <section class="grid">
    <!-- LEFT -->
    <div class="panel">
      <div class="panel-head">
        <h2>Создать таблицу</h2>
        <div class="quick">
          <button on:click={refresh}>Обновить список</button>
          <button on:click={() => (showAdvanced = !showAdvanced)}>
            {showAdvanced ? 'Скрыть шаблоны' : 'Шаблоны / подсказки'}
          </button>
        </div>
      </div>

      {#if showAdvanced}
        <div class="panel2">
          <h3>Шаблоны</h3>
          <p class="hint">Это просто авто-заполнение полей формы, чтобы быстрее стартовать.</p>
          <div class="actions">
            <button on:click={pickTemplateBronze}>Заполнить: Bronze WB Raw</button>
          </div>
        </div>
      {/if}

      <div class="form">
        <label>
          Схема
          <input bind:value={schema_name} placeholder="например: bronze / silver_adv / showcase" />
          <span class="hint2">Только латиница/цифры/_, без пробелов (пример: bronze)</span>
        </label>

        <label>
          Таблица
          <input bind:value={table_name} placeholder="например: wb_ads_raw" />
          <span class="hint2">Только латиница/цифры/_, без пробелов (пример: wb_ads_raw)</span>
        </label>

        <label>
          Класс
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
            <input placeholder="показатель (field_name)" bind:value={c.field_name} />
            <select bind:value={c.field_type}>
              {#each typeOptions as t}
                <option value={t}>{t}</option>
              {/each}
            </select>
            <input placeholder="описание" bind:value={c.description} />
            <button class="danger" on:click={() => removeColumn(ix)}>Удалить</button>
          </div>
        {/each}

        <!-- Кнопка добавления ВНИЗУ, как просил -->
        <div class="actions" style="margin-top:10px;">
          <button on:click={addColumn}>+ Добавить поле</button>
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
              <input bind:value={partition_column} placeholder="event_date / ingested_at / date ..." />
            </label>
            <label>
              Интервал
              <select bind:value={partition_interval}>
                <option value="day">day</option>
                <option value="month">month</option>
              </select>
            </label>
          </div>
          <p class="hint2">Создаст партицию today/month + default (для остальных дат).</p>
        {/if}
      </div>

      <div class="panel2">
        <h3>Тестовая запись (опционально)</h3>
        <p class="hint">Если заполнить JSON — одна строка будет вставлена после создания таблицы.</p>
        <textarea
          bind:value={test_row_text}
          placeholder='{"dataset":"ads","event_date":"2026-02-17","payload":{"a":1}}'
        />
      </div>

      <div class="actions">
        <button class="primary" on:click={createTableNow} disabled={!canCreateTable()}>
          Создать таблицу
        </button>
      </div>

      {#if role !== 'data_admin'}
        <p class="hint2">Кнопка недоступна: выбери роль <b>data_admin</b>.</p>
      {/if}
      {#if role === 'data_admin' && !canCreateTable()}
        <p class="hint2">
          Проверь: заполнены схема/таблица, корректные имена (латиница/цифры/_), и хотя бы одно поле.
        </p>
      {/if}
    </div>

    <!-- RIGHT -->
    <div class="panel">
      <h2>Текущие таблицы в базе</h2>

      {#if loading}
        <p>Загрузка…</p>
      {:else if existingTables.length === 0}
        <p class="hint">Пока нет таблиц (или нет доступа).</p>
      {:else}
        <div class="list">
          {#each existingTables as t}
            <div class="item2">{t.schema_name}.{t.table_name}</div>
          {/each}
        </div>
      {/if}

      <h2 style="margin-top:16px;">Предпросмотр (5 строк)</h2>

      {#if preview_error}
        <div class="alert">
          <div class="alert-title">Ошибка предпросмотра</div>
          <pre>{preview_error}</pre>
        </div>
      {/if}

      <div class="form">
        <label>
          Схема
          <select bind:value={preview_schema} on:change={() => (preview_rows = [])}>
            {#each Array.from(new Set(existingTables.map((t) => t.schema_name))) as s}
              <option value={s}>{s}</option>
            {/each}
          </select>
        </label>

        <label>
          Таблица
          <select bind:value={preview_table} on:change={() => (preview_rows = [])}>
            {#each existingTables.filter((t) => t.schema_name === preview_schema) as t}
              <option value={t.table_name}>{t.schema_name}.{t.table_name}</option>
            {/each}
          </select>
        </label>
      </div>

      <div class="actions">
        <button on:click={loadPreview} disabled={preview_loading}>
          {preview_loading ? 'Загрузка…' : 'Показать 5 строк'}
        </button>
      </div>

      {#if preview_rows.length}
        <div class="preview">
          <table>
            <thead>
              <tr>
                {#each Object.keys(preview_rows[0]) as k}
                  <th>{k}</th>
                {/each}
              </tr>
            </thead>
            <tbody>
              {#each preview_rows as r}
                <tr>
                  {#each Object.keys(preview_rows[0]) as k}
                    <td>{typeof r[k] === 'object' ? JSON.stringify(r[k]) : String(r[k])}</td>
                  {/each}
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      {:else}
        <p class="hint">Нет данных для предпросмотра (таблица может быть пустой).</p>
      {/if}
    </div>
  </section>
</main>

<style>
  .root { padding: 18px; }
  .header { display:flex; justify-content:space-between; gap:12px; align-items:flex-start; }
  h1 { margin:0; font-size: 20px; font-weight: 800; }
  .sub { margin: 6px 0 0; font-size: 12px; color:#64748b; }
  .role { display:flex; gap:8px; align-items:center; font-size: 12px; color:#334155; }

  .grid { display:grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-top: 12px; }
  @media (max-width: 1100px) { .grid { grid-template-columns: 1fr; } }

  .panel { background:#fff; border:1px solid #eef2f7; border-radius: 16px; padding: 14px; }
  .panel2 { margin-top: 12px; border:1px solid #eef2f7; border-radius: 14px; padding: 12px; }
  .panel-head { display:flex; justify-content:space-between; align-items:center; gap:8px; }
  .quick { display:flex; gap: 8px; }

  .form { display:grid; gap: 10px; margin-top: 12px; }
  label { display:grid; gap: 6px; font-size: 12px; color:#334155; }
  input, select, textarea { padding: 10px 12px; border-radius: 12px; border:1px solid #e2e8f0; outline: none; }
  textarea { min-height: 90px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }

  .fields { margin-top: 14px; }
  .fields-head { display:flex; align-items:center; justify-content:space-between; gap: 8px; }
  .field-row { display:grid; grid-template-columns: 1.2fr 0.8fr 1.6fr auto; gap: 8px; margin-top: 8px; }

  .row { display:flex; gap:8px; align-items:center; font-size: 12px; color:#334155; }

  .actions { display:flex; gap: 8px; margin-top: 12px; }
  button { padding: 10px 12px; border-radius: 12px; border:1px solid #e2e8f0; background:#fff; cursor:pointer; }
  button.primary { background:#0f172a; color:#fff; border-color:#0f172a; }
  button:disabled { opacity:0.5; cursor:not-allowed; }
  .danger { border:1px solid #fecaca; color:#b91c1c; }

  .hint { margin-top: 10px; font-size: 12px; color:#64748b; }
  .hint2 { margin-top: 6px; font-size: 12px; color:#64748b; }

  .alert { background:#fff1f2; border:1px solid #fecdd3; border-radius: 14px; padding: 12px; margin: 12px 0; }
  .alert-title { font-weight: 800; margin-bottom: 6px; }
  pre { white-space: pre-wrap; margin:0; font-size: 12px; }

  .preview { margin-top: 10px; overflow:auto; border:1px solid #eef2f7; border-radius: 14px; }
  table { border-collapse: collapse; width: 100%; font-size: 12px; }
  th, td { border-bottom:1px solid #eef2f7; padding: 8px 10px; text-align:left; vertical-align:top; }
  th { position: sticky; top: 0; background:#fff; }

  .list { margin-top: 10px; display:grid; gap:6px; }
  .item2 { padding: 8px 10px; border:1px solid #eef2f7; border-radius: 12px; font-size: 12px; color:#0f172a; }
</style>
