<!-- core/orchestrator/modules/advertising/interface/desk/DataDesk.svelte -->
<script lang="ts">
  import { onMount } from 'svelte';

  type ColumnDef = { field_name: string; field_type: string; description?: string };
  type ExistingTable = { schema_name: string; table_name: string };
  type Draft = {
    id: string;
    schema_name: string;
    table_name: string;
    table_class: string;
    status: string;
    description?: string;
    created_at: string;
    applied_at?: string | null;
    last_error?: string | null;
    options?: any;
    fields?: ColumnDef[];
  };

  const testRowPlaceholder = '{"dataset":"ads","event_date":"2026-02-17","payload":{"a":1}}';

  let role: 'viewer' | 'operator' | 'data_admin' = 'data_admin';

  let loading = false;
  let error = '';

  let drafts: Draft[] = [];
  let existingTables: ExistingTable[] = [];

  // Конструктор: пользователь вводит сам
  let schema_name = '';
  let table_name = '';
  let table_class = 'custom';
  let description = '';

  const typeOptions = ['text', 'int', 'bigint', 'numeric', 'boolean', 'date', 'timestamptz', 'jsonb', 'uuid'];
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

  function setErr(e: any) {
    error = String(e?.details || e?.message || e);
  }

  function trimOrEmpty(v: string) {
    return (v || '').trim();
  }

  function validateForm(): string | null {
    if (!trimOrEmpty(schema_name)) return 'Заполни поле: Схема';
    if (!trimOrEmpty(table_name)) return 'Заполни поле: Таблица';

    const cleanCols = columns
      .map((c) => ({
        field_name: trimOrEmpty(c.field_name),
        field_type: c.field_type,
        description: trimOrEmpty(c.description || '')
      }))
      .filter((c) => c.field_name);

    if (!cleanCols.length) return 'Добавь хотя бы одно поле (показатель)';

    const seen = new Set<string>();
    for (const c of cleanCols) {
      const k = c.field_name.toLowerCase();
      if (seen.has(k)) return `Дублируется поле: ${c.field_name}`;
      seen.add(k);
    }

    if (partition_enabled && !trimOrEmpty(partition_column)) return 'Заполни колонку для партиций';

    return null;
  }

  function normalizedColumns(): ColumnDef[] {
    return columns
      .map((c) => ({
        field_name: trimOrEmpty(c.field_name),
        field_type: c.field_type,
        description: trimOrEmpty(c.description || '')
      }))
      .filter((c) => c.field_name);
  }

  async function refresh() {
    loading = true;
    error = '';
    try {
      const r = await fetch('/ai-orchestrator/api/tables');
      const j = await r.json();
      if (!r.ok) throw new Error(j.error || 'failed_to_list');

      drafts = j.drafts || [];
      existingTables = j.existing_tables || [];

      if ((!preview_schema || !preview_table) && existingTables.length) {
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
    const t = trimOrEmpty(test_row_text);
    if (!t) return null;
    const parsed = JSON.parse(t);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new Error('Тестовая запись должна быть JSON объектом (например {"a":1})');
    }
    return parsed;
  }

  // Возвращает id черновика (или null при ошибке)
  async function createDraft(): Promise<string | null> {
    error = '';

    const validationError = validateForm();
    if (validationError) {
      error = validationError;
      return null;
    }

    try {
      const test_row = parseTestRow();

      const r = await fetch('/ai-orchestrator/api/tables/draft', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({
          schema_name: trimOrEmpty(schema_name),
          table_name: trimOrEmpty(table_name),
          table_class,
          description: trimOrEmpty(description),
          created_by: 'ui',
          columns: normalizedColumns(),
          partitioning: partition_enabled
            ? { enabled: true, column: trimOrEmpty(partition_column), interval: partition_interval }
            : { enabled: false },
          test_row
        })
      });

      const j = await r.json();
      if (!r.ok) throw new Error(j.details || j.error || 'draft_create_failed');

      await refresh();
      return String(j.id);
    } catch (e: any) {
      setErr(e);
      return null;
    }
  }

  async function applyDraft(id: string) {
    error = '';
    try {
      const r = await fetch(`/ai-orchestrator/api/tables/apply/${id}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({ applied_by: 'ui' })
      });
      const j = await r.json();
      if (!r.ok) throw new Error(j.details || j.error || 'apply_failed');
      await refresh();
      alert('Таблица создана (применение выполнено)');
    } catch (e: any) {
      setErr(e);
    }
  }

  // Один клик: создать черновик + применить
  async function createAndApply() {
    if (role !== 'data_admin') return;
    const id = await createDraft();
    if (!id) return;
    await applyDraft(id);
  }

  async function loadPreview() {
    preview_loading = true;
    preview_error = '';
    preview_rows = [];
    try {
      if (!trimOrEmpty(preview_schema) || !trimOrEmpty(preview_table)) {
        throw new Error('Выбери схему и таблицу для предпросмотра');
      }

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

  $: canCreateDraft = role === 'data_admin' && !validateForm();
  $: canCreateTable = canCreateDraft;

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
    <div class="panel">
      <div class="panel-head">
        <h2>Создать таблицу</h2>
        <div class="quick">
          <button on:click={pickTemplateBronze}>Шаблон: Bronze</button>
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
          <button on:click={addColumn}>+ Добавить поле</button>
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
        <p class="hint">Если заполнить JSON — одна строка будет вставлена после “Применить”.</p>
        <textarea bind:value={test_row_text} placeholder={testRowPlaceholder} />
      </div>

      <div class="actions">
        <button class="primary" on:click={createDraft} disabled={!canCreateDraft}>
          Сохранить черновик
        </button>
        <button class="primary" on:click={createAndApply} disabled={!canCreateTable}>
          Создать таблицу
        </button>
      </div>

      <p class="hint">Можно: сохранить как черновик, либо “Создать таблицу” сразу одним кликом.</p>
    </div>

    <div class="panel">
      <h2>Черновики</h2>
      {#if loading}
        <p>Загрузка…</p>
      {:else if drafts.length === 0}
        <p class="hint">Черновиков пока нет.</p>
      {:else}
        <div class="list">
          {#each drafts as d}
            <div class="item">
              <div class="meta">
                <div class="title">{d.schema_name}.{d.table_name}</div>
                <div class="sub">{d.table_class} · {d.status}</div>
                <div class="sub">создано: {new Date(d.created_at).toLocaleString()}</div>
                {#if d.applied_at}
                  <div class="sub">применено: {new Date(d.applied_at).toLocaleString()}</div>
                {/if}
                {#if d.last_error}
                  <div class="sub err">ошибка: {d.last_error}</div>
                {/if}
              </div>
              <div class="btns">
                <button class="primary" on:click={() => applyDraft(d.id)} disabled={role !== 'data_admin'}>
                  Применить
                </button>
              </div>
            </div>
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

  .list { display:grid; gap: 10px; margin-top: 12px; }
  .item { display:flex; justify-content:space-between; gap: 12px; border:1px solid #eef2f7; border-radius: 14px; padding: 12px; }
  .title { font-weight: 800; }
  .sub { font-size: 12px; color:#64748b; margin-top: 2px; }
  .sub.err { color:#b91c1c; }

  .alert { background:#fff1f2; border:1px solid #fecdd3; border-radius: 14px; padding: 12px; margin: 12px 0; }
  .alert-title { font-weight: 800; margin-bottom: 6px; }
  pre { white-space: pre-wrap; margin:0; font-size: 12px; }

  .preview { margin-top: 10px; overflow:auto; border:1px solid #eef2f7; border-radius: 14px; }
  table { border-collapse: collapse; width: 100%; font-size: 12px; }
  th, td { border-bottom:1px solid #eef2f7; padding: 8px 10px; text-align:left; vertical-align:top; }
  th { position: sticky; top: 0; background:#fff; }
</style>
