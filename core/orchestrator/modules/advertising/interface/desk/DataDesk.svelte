<script lang="ts">
  import { onMount } from 'svelte';

  type Role = 'viewer' | 'operator' | 'data_admin';

  type ColumnDef = { field_name: string; field_type: string; description?: string };
  type ExistingTable = { schema_name: string; table_name: string };

  const API_BASE = '/ai-orchestrator/api';

  let role: Role = 'data_admin';

  let loading = false;
  let error = '';

  let existingTables: ExistingTable[] = [];

  // CREATE TABLE form
  let schema_name = '';
  let table_name = '';
  let description = '';
  let table_class = 'custom';

  const typeOptions = ['text', 'int', 'bigint', 'numeric', 'boolean', 'date', 'timestamptz', 'jsonb', 'uuid'];
  let columns: ColumnDef[] = [{ field_name: '', field_type: 'text', description: '' }];

  let partition_enabled = false;
  let partition_column = 'event_date';
  let partition_interval: 'day' | 'month' = 'day';

  let test_row_text = '';

  const TEST_ROW_PLACEHOLDER =
    '{"dataset":"ads","event_date":"2026-02-17","payload":{"a":1}}';

  // Preview + CRUD
  let preview_schema = '';
  let preview_table = '';

  let preview_rows: any[] = [];
  let preview_columns: { column_name: string; data_type: string }[] = [];

  let preview_error = '';
  let preview_loading = false;

  // Add column modal (simple inline)
  let add_col_name = '';
  let add_col_type = 'text';
  let add_col_desc = '';

  // Add row
  let add_row_text = '';
  const ADD_ROW_PLACEHOLDER = '{"col1":"value","col2":123}';

  function canWrite() {
    return role === 'data_admin';
  }

  function setErr(e: any) {
    error = String(e?.details || e?.message || e);
  }

  async function apiJson(url: string, init?: RequestInit) {
    const r = await fetch(url, init);
    let j: any = null;
    try {
      j = await r.json();
    } catch {
      // ignore
    }
    if (!r.ok) {
      const msg = j?.details || j?.error || `${r.status} ${r.statusText}`;
      throw new Error(msg);
    }
    return j;
  }

  function normalizeColumns(cols: ColumnDef[]): ColumnDef[] {
    return cols
      .map((c) => ({
        field_name: (c.field_name || '').trim(),
        field_type: (c.field_type || '').trim(),
        description: (c.description || '').trim()
      }))
      .filter((c) => c.field_name.length > 0);
  }

  function parseJsonObject(text: string, errMsg: string): any | null {
    const t = (text || '').trim();
    if (!t) return null;
    const parsed = JSON.parse(t);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new Error(errMsg);
    }
    return parsed;
  }

  function validateCreate() {
    const s = schema_name.trim();
    const t = table_name.trim();
    const cols = normalizeColumns(columns);

    if (!s) throw new Error('Укажи схему');
    if (!t) throw new Error('Укажи таблицу');
    if (!cols.length) throw new Error('Добавь хотя бы одно поле');

    for (const c of cols) {
      if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(c.field_name)) {
        throw new Error(`Некорректное имя поля: ${c.field_name}`);
      }
      if (!typeOptions.includes(c.field_type)) {
        throw new Error(`Некорректный тип поля: ${c.field_type}`);
      }
    }

    if (partition_enabled) {
      const pc = partition_column.trim();
      if (!pc) throw new Error('Укажи колонку для партиций');
      if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(pc)) throw new Error('Некорректное имя колонки для партиций');
    }
  }

  async function refresh() {
    loading = true;
    error = '';
    try {
      const j = await apiJson(`${API_BASE}/tables`, { method: 'GET' });
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

  // Создать таблицу СРАЗУ
  async function createTableNow() {
    error = '';
    try {
      if (!canWrite()) throw new Error('Недостаточно прав (нужна роль data_admin)');
      validateCreate();

      const cols = normalizeColumns(columns);
      const test_row = parseJsonObject(
        test_row_text,
        'Тестовая запись должна быть JSON-объектом (например {"a":1})'
      );

      await apiJson(`${API_BASE}/table/create`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({
          schema_name: schema_name.trim(),
          table_name: table_name.trim(),
          table_class,
          description: description.trim(),
          columns: cols,
          partitioning: partition_enabled
            ? { enabled: true, column: partition_column.trim(), interval: partition_interval }
            : { enabled: false },
          test_row
        })
      });

      await refresh();

      preview_schema = schema_name.trim();
      preview_table = table_name.trim();
      await loadPreview();

      alert(`Таблица создана: ${preview_schema}.${preview_table}`);
    } catch (e: any) {
      setErr(e);
    }
  }

  async function loadPreview() {
    preview_loading = true;
    preview_error = '';
    preview_rows = [];
    preview_columns = [];
    try {
      if (!preview_schema || !preview_table) throw new Error('Выбери схему и таблицу');

      const cols = await apiJson(
        `${API_BASE}/table/columns?schema=${encodeURIComponent(preview_schema)}&table=${encodeURIComponent(preview_table)}`,
        { method: 'GET' }
      );
      preview_columns = cols.columns || [];

      const rows = await apiJson(
        `${API_BASE}/table/rows?schema=${encodeURIComponent(preview_schema)}&table=${encodeURIComponent(preview_table)}&limit=5&offset=0`,
        { method: 'GET' }
      );
      preview_rows = rows.rows || [];
    } catch (e: any) {
      preview_error = String(e?.message || e);
    } finally {
      preview_loading = false;
    }
  }

  async function dropTable() {
    error = '';
    try {
      if (!canWrite()) throw new Error('Недостаточно прав (нужна роль data_admin)');
      if (!preview_schema || !preview_table) throw new Error('Выбери таблицу');

      const ok = confirm(`Удалить таблицу ${preview_schema}.${preview_table}? Это необратимо.`);
      if (!ok) return;

      await apiJson(`${API_BASE}/table/drop`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({ schema: preview_schema, table: preview_table })
      });

      await refresh();
      preview_rows = [];
      preview_columns = [];
      alert('Таблица удалена');
    } catch (e: any) {
      setErr(e);
    }
  }

  async function addDbColumn() {
    error = '';
    try {
      if (!canWrite()) throw new Error('Недостаточно прав (нужна роль data_admin)');
      if (!preview_schema || !preview_table) throw new Error('Выбери таблицу');

      const name = add_col_name.trim();
      if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) throw new Error('Некорректное имя колонки');
      if (!typeOptions.includes(add_col_type)) throw new Error('Некорректный тип колонки');

      await apiJson(`${API_BASE}/table/column/add`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({
          schema: preview_schema,
          table: preview_table,
          column_name: name,
          data_type: add_col_type,
          description: add_col_desc.trim()
        })
      });

      add_col_name = '';
      add_col_type = 'text';
      add_col_desc = '';
      await loadPreview();
    } catch (e: any) {
      setErr(e);
    }
  }

  async function dropDbColumn(col: string) {
    error = '';
    try {
      if (!canWrite()) throw new Error('Недостаточно прав (нужна роль data_admin)');
      if (!preview_schema || !preview_table) throw new Error('Выбери таблицу');

      const ok = confirm(`Удалить колонку "${col}" из ${preview_schema}.${preview_table}?`);
      if (!ok) return;

      await apiJson(`${API_BASE}/table/column/drop`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({ schema: preview_schema, table: preview_table, column_name: col })
      });

      await loadPreview();
    } catch (e: any) {
      setErr(e);
    }
  }

  async function insertRow() {
    error = '';
    try {
      if (!canWrite()) throw new Error('Недостаточно прав (нужна роль data_admin)');
      if (!preview_schema || !preview_table) throw new Error('Выбери таблицу');

      const row = parseJsonObject(add_row_text, 'Строка должна быть JSON-объектом (пример {"a":1})');
      if (!row) throw new Error('Заполни JSON строки');

      await apiJson(`${API_BASE}/table/row/insert`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({ schema: preview_schema, table: preview_table, row })
      });

      add_row_text = '';
      await loadPreview();
    } catch (e: any) {
      setErr(e);
    }
  }

  async function deleteRowByCtid(ctid: string) {
    error = '';
    try {
      if (!canWrite()) throw new Error('Недостаточно прав (нужна роль data_admin)');
      if (!preview_schema || !preview_table) throw new Error('Выбери таблицу');

      const ok = confirm('Удалить эту строку?');
      if (!ok) return;

      await apiJson(`${API_BASE}/table/row/delete`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({ schema: preview_schema, table: preview_table, ctid })
      });

      await loadPreview();
    } catch (e: any) {
      setErr(e);
    }
  }

  function pickTemplateBronze() {
    schema_name = 'bronze';
    table_name = 'wb_ads_raw';
    table_class = 'bronze_raw';
    description = 'Сырые ответы API (append-only JSON)';
    columns = [
      { field_name: 'dataset', field_type: 'text', description: 'dataset name' },
      { field_name: 'endpoint', field_type: 'text', description: 'endpoint name' },
      { field_name: 'ingested_at', field_type: 'timestamptz', description: 'время ingest' },
      { field_name: 'event_date', field_type: 'date', description: 'бизнес-дата' },
      { field_name: 'idempotency_key', field_type: 'text', description: 'ключ дедупликации' },
      { field_name: 'payload', field_type: 'jsonb', description: 'сырой JSON' }
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
        Создаёт схему/таблицу/поля по твоему вводу. Справа: текущие таблицы + предпросмотр и кнопки управления.
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
          <button on:click={pickTemplateBronze}>Заполнить шаблоном Bronze</button>
          <button on:click={refresh}>Обновить список</button>
        </div>
      </div>

      <div class="form">
        <label>
          Схема
          <input bind:value={schema_name} placeholder="например: bronze / silver_adv / showcase" />
        </label>

        <label>
          Таблица
          <input bind:value={table_name} placeholder="например: wb_ads_raw" />
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
            <button class="danger" on:click={() => removeColumn(ix)} title="Удалить поле">Удалить</button>
          </div>
        {/each}

        <div class="fields-footer">
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
        {/if}
      </div>

      <div class="panel2">
        <h3>Тестовая запись (опционально)</h3>
        <p class="hint">Если заполнить JSON — одна строка будет вставлена сразу после создания таблицы.</p>
        <textarea bind:value={test_row_text} placeholder={TEST_ROW_PLACEHOLDER} />
      </div>

      <div class="actions">
        <button class="primary" on:click={createTableNow} disabled={!canWrite()}>
          Создать таблицу
        </button>
      </div>

      {#if !canWrite()}
        <p class="hint">Кнопка активна только для роли <b>data_admin</b>.</p>
      {/if}
    </div>

    <!-- RIGHT -->
    <div class="panel">
      <div class="panel-head">
        <h2>Текущие таблицы</h2>
        <div class="quick">
          <button on:click={refresh} disabled={loading}>{loading ? 'Загрузка…' : 'Обновить'}</button>
        </div>
      </div>

      {#if loading}
        <p>Загрузка…</p>
      {:else if existingTables.length === 0}
        <p class="hint">Пока таблиц не найдено.</p>
      {:else}
        <div class="tables-list">
          {#each existingTables as t}
            <button
              class="chip"
              on:click={() => {
                preview_schema = t.schema_name;
                preview_table = t.table_name;
                preview_rows = [];
                preview_columns = [];
                preview_error = '';
              }}
              title="Выбрать таблицу"
            >
              {t.schema_name}.{t.table_name}
            </button>
          {/each}
        </div>
      {/if}

      <div class="preview-head">
        <h2>Предпросмотр (5 строк)</h2>
        <div class="preview-actions">
          <button on:click={loadPreview} disabled={preview_loading}>
            {preview_loading ? 'Загрузка…' : 'Показать 5 строк'}
          </button>
          <button class="danger" on:click={dropTable} disabled={!canWrite() || !preview_schema || !preview_table}>
            Удалить таблицу
          </button>
        </div>
      </div>

      {#if preview_error}
        <div class="alert">
          <div class="alert-title">Ошибка предпросмотра</div>
          <pre>{preview_error}</pre>
        </div>
      {/if}

      <div class="form">
        <label>
          Схема
          <select bind:value={preview_schema} on:change={() => { preview_rows = []; preview_columns=[]; }}>
            {#each Array.from(new Set(existingTables.map((t) => t.schema_name))) as s}
              <option value={s}>{s}</option>
            {/each}
          </select>
        </label>

        <label>
          Таблица
          <select bind:value={preview_table} on:change={() => { preview_rows = []; preview_columns=[]; }}>
            {#each existingTables.filter((t) => t.schema_name === preview_schema) as t}
              <option value={t.table_name}>{t.schema_name}.{t.table_name}</option>
            {/each}
          </select>
        </label>
      </div>

      <!-- Column controls -->
      <div class="panel2">
        <h3>Колонки</h3>
        <div class="col-add">
          <input placeholder="имя колонки" bind:value={add_col_name} />
          <select bind:value={add_col_type}>
            {#each typeOptions as t}
              <option value={t}>{t}</option>
            {/each}
          </select>
          <input placeholder="описание (опц.)" bind:value={add_col_desc} />
          <button class="primary" on:click={addDbColumn} disabled={!canWrite() || !preview_schema || !preview_table}>
            + Добавить колонку
          </button>
        </div>

        {#if preview_columns.length}
          <div class="cols-list">
            {#each preview_columns as c}
              <div class="col-item">
                <div class="col-name">{c.column_name}</div>
                <div class="col-type">{c.data_type}</div>
                <button class="danger" on:click={() => dropDbColumn(c.column_name)} disabled={!canWrite()}>
                  ✕
                </button>
              </div>
            {/each}
          </div>
        {:else}
          <p class="hint">Нажми “Показать 5 строк”, чтобы подтянуть список колонок.</p>
        {/if}
      </div>

      <!-- Row controls -->
      <div class="panel2">
        <h3>Строки</h3>
        <p class="hint">Добавление строки: вставь JSON-объект (ключи = имена колонок).</p>
        <textarea bind:value={add_row_text} placeholder={ADD_ROW_PLACEHOLDER} />
        <div class="actions">
          <button class="primary" on:click={insertRow} disabled={!canWrite() || !preview_schema || !preview_table}>
            + Добавить строку
          </button>
        </div>
      </div>

      <!-- Preview table -->
      {#if preview_rows.length > 0}
        <div class="preview">
          <table>
            <thead>
              <tr>
                {#each Object.keys(preview_rows[0]) as k}
                  <th>{k}</th>
                {/each}
                <th style="width:1%;">Действия</th>
              </tr>
            </thead>
            <tbody>
              {#each preview_rows as r}
                <tr>
                  {#each Object.keys(preview_rows[0]) as k}
                    <td>{typeof r[k] === 'object' ? JSON.stringify(r[k]) : String(r[k])}</td>
                  {/each}
                  <td>
                    <button
                      class="danger"
                      on:click={() => deleteRowByCtid(String(r.__ctid))}
                      disabled={!canWrite() || !r.__ctid}
                      title="Удалить строку"
                    >
                      ✕
                    </button>
                  </td>
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
  .quick { display:flex; gap: 8px; flex-wrap: wrap; }

  .form { display:grid; gap: 10px; margin-top: 12px; }
  label { display:grid; gap: 6px; font-size: 12px; color:#334155; }

  input, select, textarea { padding: 10px 12px; border-radius: 12px; border:1px solid #e2e8f0; outline: none; }
  textarea { min-height: 90px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }

  .fields { margin-top: 14px; }
  .fields-head { display:flex; align-items:center; justify-content:space-between; gap: 8px; }
  .field-row { display:grid; grid-template-columns: 1.2fr 0.8fr 1.6fr auto; gap: 8px; margin-top: 8px; }
  .fields-footer { margin-top: 10px; display:flex; justify-content:flex-end; }

  .row { display:flex; gap:8px; align-items:center; font-size: 12px; color:#334155; }

  .actions { display:flex; gap: 8px; margin-top: 12px; align-items:center; flex-wrap: wrap; }

  button { padding: 10px 12px; border-radius: 12px; border:1px solid #e2e8f0; background:#fff; cursor:pointer; }
  button.primary { background:#0f172a; color:#fff; border-color:#0f172a; }
  button:disabled { opacity:0.5; cursor:not-allowed; }
  .danger { border:1px solid #fecaca; color:#b91c1c; }

  .hint { margin-top: 10px; font-size: 12px; color:#64748b; }

  .alert { background:#fff1f2; border:1px solid #fecdd3; border-radius: 14px; padding: 12px; margin: 12px 0; }
  .alert-title { font-weight: 800; margin-bottom: 6px; }
  pre { white-space: pre-wrap; margin:0; font-size: 12px; }

  .tables-list { display:flex; gap:8px; flex-wrap:wrap; margin-top: 12px; }
  .chip { padding: 6px 10px; border-radius: 999px; border:1px solid #e2e8f0; font-size: 12px; color:#334155; background:#fff; cursor:pointer; }

  .preview-head { display:flex; align-items:center; justify-content:space-between; gap: 8px; margin-top: 16px; }
  .preview-actions { display:flex; gap: 8px; flex-wrap: wrap; }

  .preview { margin-top: 10px; overflow:auto; border:1px solid #eef2f7; border-radius: 14px; }
  table { border-collapse: collapse; width: 100%; font-size: 12px; }
  th, td { border-bottom:1px solid #eef2f7; padding: 8px 10px; text-align:left; vertical-align:top; }
  th { position: sticky; top: 0; background:#fff; }

  .col-add { display:grid; grid-template-columns: 1fr 180px 1.2fr auto; gap: 8px; margin-top: 10px; }
  @media (max-width: 1100px) { .col-add { grid-template-columns: 1fr; } }

  .cols-list { display:grid; gap: 8px; margin-top: 10px; }
  .col-item { display:grid; grid-template-columns: 1fr 200px auto; gap: 8px; align-items:center; padding: 8px; border:1px solid #eef2f7; border-radius: 12px; }
  .col-name { font-weight: 700; }
  .col-type { color:#64748b; font-size: 12px; }
</style>
