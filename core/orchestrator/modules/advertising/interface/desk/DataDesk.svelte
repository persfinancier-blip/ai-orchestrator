<script lang="ts">
  import { onMount } from 'svelte';

  type ColumnDef = { field_name: string; field_type: string; description?: string };
  type ExistingTable = { schema_name: string; table_name: string };
  type PreviewColumn = { column_name: string; data_type: string };

  let role: 'viewer' | 'operator' | 'data_admin' = 'data_admin';

  let loading = false;
  let error = '';

  let existingTables: ExistingTable[] = [];

  // ---- Создание таблицы
  let schema_name = '';
  let table_name = '';
  let description = '';

  const typeOptions = ['text', 'int', 'bigint', 'numeric', 'boolean', 'date', 'timestamptz', 'jsonb', 'uuid'];
  let columns: ColumnDef[] = [{ field_name: '', field_type: 'text', description: '' }];

  // Партиционирование
  let partition_enabled = false;
  let partition_column = 'event_date';
  let partition_interval: 'day' | 'month' = 'day';

  // ---- Предпросмотр / текущие таблицы
  let preview_schema = '';
  let preview_table = '';
  let preview_columns: PreviewColumn[] = [];
  let preview_rows: any[] = [];
  let preview_error = '';
  let preview_loading = false;

  // ---- UI: добавить колонку в существующую таблицу
  let addcol_name = '';
  let addcol_type = 'text';
  let addcol_desc = '';

  // ---- UI: добавить строку
  const rowPlaceholder = '{"event_date":"2026-02-17","payload":{"a":1}}';
  let new_row_text = '';

  function setErr(e: any) {
    error = String(e?.details || e?.message || e);
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

  function addColumnRow() {
    columns = [...columns, { field_name: '', field_type: 'text', description: '' }];
  }

  function removeColumnRow(ix: number) {
    columns = columns.filter((_, i) => i !== ix);
    if (!columns.length) columns = [{ field_name: '', field_type: 'text', description: '' }];
  }

  function isValidIdent(v: string) {
    return /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(v);
  }

  function canCreateTable() {
    if (role !== 'data_admin') return false;
    if (!schema_name.trim() || !table_name.trim()) return false;
    if (!isValidIdent(schema_name.trim()) || !isValidIdent(table_name.trim())) return false;
    const good = columns.filter((c) => c.field_name.trim());
    if (good.length === 0) return false;
    for (const c of good) {
      if (!isValidIdent(c.field_name.trim())) return false;
      if (!c.field_type) return false;
    }
    if (partition_enabled && !isValidIdent(partition_column.trim())) return false;
    return true;
  }

  async function createTableNow() {
    error = '';
    try {
      const payload = {
        schema_name: schema_name.trim(),
        table_name: table_name.trim(),
        description: description.trim(),
        columns: columns
          .map((c) => ({
            field_name: c.field_name.trim(),
            field_type: c.field_type,
            description: (c.description || '').trim()
          }))
          .filter((c) => c.field_name),
        partitioning: partition_enabled
          ? { enabled: true, column: partition_column.trim(), interval: partition_interval }
          : { enabled: false }
      };

      const r = await fetch('/ai-orchestrator/api/tables/create', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify(payload)
      });
      const j = await r.json();
      if (!r.ok) throw new Error(j.details || j.error || 'create_failed');

      await refresh();
      preview_schema = payload.schema_name;
      preview_table = payload.table_name;
      await loadPreview();
      alert('Таблица создана');
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
      const url =
        `/ai-orchestrator/api/preview?schema=${encodeURIComponent(preview_schema)}` +
        `&table=${encodeURIComponent(preview_table)}&limit=5`;
      const r = await fetch(url);
      const j = await r.json();
      if (!r.ok) throw new Error(j.details || j.error || 'preview_failed');
      preview_rows = j.rows || [];
      preview_columns = j.columns || [];
    } catch (e: any) {
      preview_error = String(e?.message || e);
    } finally {
      preview_loading = false;
    }
  }

  async function deleteTable() {
    if (role !== 'data_admin') return;
    if (!confirm(`Удалить таблицу ${preview_schema}.${preview_table}?`)) return;
    error = '';
    try {
      const r = await fetch('/ai-orchestrator/api/tables/drop', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({ schema_name: preview_schema, table_name: preview_table })
      });
      const j = await r.json();
      if (!r.ok) throw new Error(j.details || j.error || 'drop_failed');
      await refresh();
      preview_rows = [];
      preview_columns = [];
      alert('Таблица удалена');
    } catch (e: any) {
      setErr(e);
    }
  }

  async function addColumnToExisting() {
    if (role !== 'data_admin') return;
    error = '';
    try {
      const r = await fetch('/ai-orchestrator/api/tables/column/add', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({
          schema_name: preview_schema,
          table_name: preview_table,
          column: { field_name: addcol_name.trim(), field_type: addcol_type, description: addcol_desc.trim() }
        })
      });
      const j = await r.json();
      if (!r.ok) throw new Error(j.details || j.error || 'add_column_failed');

      addcol_name = '';
      addcol_desc = '';
      await loadPreview();
      await refresh();
    } catch (e: any) {
      setErr(e);
    }
  }

  async function dropColumn(colName: string) {
    if (role !== 'data_admin') return;
    if (!confirm(`Удалить столбец "${colName}"?`)) return;
    error = '';
    try {
      const r = await fetch('/ai-orchestrator/api/tables/column/drop', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({ schema_name: preview_schema, table_name: preview_table, field_name: colName })
      });
      const j = await r.json();
      if (!r.ok) throw new Error(j.details || j.error || 'drop_column_failed');
      await loadPreview();
      await refresh();
    } catch (e: any) {
      setErr(e);
    }
  }

  function parseRowJson(): any {
    const t = new_row_text.trim();
    if (!t) throw new Error('Введите JSON строки');
    const parsed = JSON.parse(t);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new Error('Строка должна быть JSON объектом (например {"a":1})');
    }
    return parsed;
  }

  async function insertRow() {
    if (role !== 'data_admin') return;
    error = '';
    try {
      const row = parseRowJson();
      const r = await fetch('/ai-orchestrator/api/tables/row/insert', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({ schema_name: preview_schema, table_name: preview_table, row })
      });
      const j = await r.json();
      if (!r.ok) throw new Error(j.details || j.error || 'insert_row_failed');
      new_row_text = '';
      await loadPreview();
    } catch (e: any) {
      setErr(e);
    }
  }

  async function deleteRow(id: string) {
    if (role !== 'data_admin') return;
    if (!confirm(`Удалить запись id=${id}?`)) return;
    error = '';
    try {
      const r = await fetch('/ai-orchestrator/api/tables/row/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({ schema_name: preview_schema, table_name: preview_table, id })
      });
      const j = await r.json();
      if (!r.ok) throw new Error(j.details || j.error || 'delete_row_failed');
      await loadPreview();
    } catch (e: any) {
      setErr(e);
    }
  }

  // Шаблон (быстро заполнить поля создания)
  function applyTemplateBronze() {
    schema_name = 'bronze';
    table_name = 'wb_ads_raw';
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
  }

  onMount(async () => {
    await refresh();
    if (preview_schema && preview_table) await loadPreview();
  });
</script>

<main class="root">
  <header class="header">
    <div>
      <h1>Конструктор таблиц</h1>
      <p class="sub">Создание схемы/таблицы/полей + управление существующими таблицами (столбцы, строки, удаление).</p>
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
    <!-- ---------- Создание таблицы ---------- -->
    <div class="panel">
      <div class="panel-head">
        <h2>Создать таблицу</h2>
        <div class="quick">
          <button on:click={applyTemplateBronze}>Заполнить шаблон Bronze</button>
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
          Описание
          <input bind:value={description} placeholder="что это за таблица" />
        </label>
      </div>

      <div class="fields">
        <div class="fields-head">
          <h3>Поля</h3>
          <p class="hint" style="margin:0;">Системные поля добавятся автоматически: id, created_at, updated_at</p>
        </div>

        {#each columns as c, ix}
          <div class="field-row">
            <input placeholder="имя поля (латиница)" bind:value={c.field_name} />
            <select bind:value={c.field_type}>
              {#each typeOptions as t}
                <option value={t}>{t}</option>
              {/each}
            </select>
            <input placeholder="описание (опционально)" bind:value={c.description} />
            <button class="danger" on:click={() => removeColumnRow(ix)} title="Удалить поле">✕</button>
          </div>
        {/each}

        <!-- Кнопка добавления поля ВНИЗУ блока -->
        <div class="actions" style="justify-content:flex-start;">
          <button on:click={addColumnRow}>+ Добавить поле</button>
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
                <option value="day">день</option>
                <option value="month">месяц</option>
              </select>
            </label>
          </div>
        {/if}
      </div>

      <div class="actions">
        <button class="primary" on:click={createTableNow} disabled={!canCreateTable()}>
          Создать таблицу
        </button>
      </div>

      {#if role !== 'data_admin'}
        <p class="hint">Кнопка активна только для роли <b>data_admin</b>.</p>
      {/if}
    </div>

    <!-- ---------- Текущие таблицы + управление ---------- -->
    <div class="panel">
      <div class="panel-head">
        <h2>Текущие таблицы</h2>
        <div class="quick">
          <button class="danger" on:click={deleteTable} disabled={role !== 'data_admin' || !preview_schema || !preview_table}>
            Удалить таблицу
          </button>
        </div>
      </div>

      {#if loading}
        <p>Загрузка…</p>
      {:else}
        <div class="form">
          <label>
            Схема
            <select
              bind:value={preview_schema}
              on:change={() => {
                preview_rows = [];
                preview_columns = [];
                const first = existingTables.find((t) => t.schema_name === preview_schema);
                if (first) preview_table = first.table_name;
              }}
            >
              {#each Array.from(new Set(existingTables.map((t) => t.schema_name))) as s}
                <option value={s}>{s}</option>
              {/each}
            </select>
          </label>

          <label>
            Таблица
            <select bind:value={preview_table} on:change={() => ((preview_rows = []), (preview_columns = []))}>
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

        {#if preview_error}
          <div class="alert">
            <div class="alert-title">Ошибка предпросмотра</div>
            <pre>{preview_error}</pre>
          </div>
        {/if}

        <!-- Управление столбцами -->
        <div class="panel2">
          <div class="panel-head" style="padding:0;border:none;">
            <h3 style="margin:0;">Столбцы</h3>
          </div>

          <div class="field-row" style="grid-template-columns: 1.2fr 0.9fr 1.6fr auto; margin-top:10px;">
            <input placeholder="новый столбец" bind:value={addcol_name} />
            <select bind:value={addcol_type}>
              {#each typeOptions as t}
                <option value={t}>{t}</option>
              {/each}
            </select>
            <input placeholder="описание" bind:value={addcol_desc} />
            <button class="primary" on:click={addColumnToExisting} disabled={role !== 'data_admin' || !addcol_name.trim()}>
              +
            </button>
          </div>

          {#if preview_columns.length}
            <div class="cols-list">
              {#each preview_columns as c}
                <div class="col-item">
                  <div class="col-meta">
                    <div class="col-name">{c.column_name}</div>
                    <div class="col-type">{c.data_type}</div>
                  </div>
                  <button
                    class="danger"
                    title="Удалить столбец"
                    on:click={() => dropColumn(c.column_name)}
                    disabled={role !== 'data_admin' || ['id','created_at','updated_at'].includes(c.column_name)}
                  >
                    ✕
                  </button>
                </div>
              {/each}
            </div>
          {:else}
            <p class="hint">Нажми “Показать 5 строк”, чтобы увидеть список столбцов.</p>
          {/if}
        </div>

        <!-- Управление строками -->
        <div class="panel2">
          <h3 style="margin-top:0;">Строки (добавить/удалить)</h3>
          <p class="hint" style="margin-top:0;">
            Добавление: введи JSON объекта (ключи = имена столбцов). Удаление: по <b>id</b>.
          </p>

          <textarea bind:value={new_row_text} placeholder={rowPlaceholder} />

          <div class="actions">
            <button class="primary" on:click={insertRow} disabled={role !== 'data_admin'}>
              Добавить строку
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
                    <th style="width:1%; white-space:nowrap;">Действия</th>
                  </tr>
                </thead>
                <tbody>
                  {#each preview_rows as r}
                    <tr>
                      {#each Object.keys(preview_rows[0]) as k}
                        <td>{typeof r[k] === 'object' ? JSON.stringify(r[k]) : String(r[k])}</td>
                      {/each}
                      <td style="white-space:nowrap;">
                        <button class="danger" on:click={() => deleteRow(String(r.id || ''))} disabled={role !== 'data_admin' || !r.id}>
                          Удалить
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
        {/if}
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
  .fields-head { display:grid; gap:6px; }
  .field-row { display:grid; grid-template-columns: 1.2fr 0.8fr 1.6fr auto; gap: 8px; margin-top: 8px; }

  .row { display:flex; gap:8px; align-items:center; font-size: 12px; color:#334155; }

  .actions { display:flex; gap: 8px; margin-top: 12px; }
  button { padding: 10px 12px; border-radius: 12px; border:1px solid #e2e8f0; background:#fff; cursor:pointer; }
  button.primary { background:#0f172a; color:#fff; border-color:#0f172a; }
  button:disabled { opacity:0.5; cursor:not-allowed; }
  button.danger { border:1px solid #fecaca; color:#b91c1c; background:#fff; }

  .hint { margin-top: 10px; font-size: 12px; color:#64748b; }

  .alert { background:#fff1f2; border:1px solid #fecdd3; border-radius: 14px; padding: 12px; margin: 12px 0; }
  .alert-title { font-weight: 800; margin-bottom: 6px; }
  pre { white-space: pre-wrap; margin:0; font-size: 12px; }

  .preview { margin-top: 10px; overflow:auto; border:1px solid #eef2f7; border-radius: 14px; }
  table { border-collapse: collapse; width: 100%; font-size: 12px; }
  th, td { border-bottom:1px solid #eef2f7; padding: 8px 10px; text-align:left; vertical-align:top; }
  th { position: sticky; top: 0; background:#fff; }

  .cols-list { display:grid; gap: 8px; margin-top: 10px; }
  .col-item { display:flex; justify-content:space-between; gap: 10px; align-items:center; border:1px solid #eef2f7; border-radius: 12px; padding: 10px; }
  .col-meta { display:flex; gap:10px; align-items:baseline; flex-wrap:wrap; }
  .col-name { font-weight: 800; }
  .col-type { color:#64748b; font-size: 12px; }
</style>
