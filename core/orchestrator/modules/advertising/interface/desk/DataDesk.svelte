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

  // Роль (временно оставляем как раньше)
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

  // UI: меню справа (чтобы “шаблоны/черновики” не мешали)
  let rightTab: 'preview' | 'menu' = 'preview';

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

      drafts = j.drafts || [];
      existingTables = j.existing_tables || [];

      // Автовыбор для предпросмотра
      if (!preview_schema && existingTables.length) {
        preview_schema = existingTables[0].schema_name;
        preview_table = existingTables[0].table_name;
      } else if (preview_schema && existingTables.length) {
        // если текущая выбранная таблица пропала, выбираем первую доступную
        const exists = existingTables.some(
          (t) => t.schema_name === preview_schema && t.table_name === preview_table
        );
        if (!exists) {
          preview_schema = existingTables[0].schema_name;
          preview_table = existingTables[0].table_name;
        }
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

  async function createDraft() {
    error = '';
    try {
      const test_row = parseTestRow();

      const r = await fetch('/ai-orchestrator/api/tables/draft', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({
          schema_name,
          table_name,
          table_class,
          description,
          created_by: 'ui',
          columns,
          partitioning: partition_enabled
            ? { enabled: true, column: partition_column, interval: partition_interval }
            : { enabled: false },
          test_row
        })
      });

      const j = await r.json();
      if (!r.ok) throw new Error(j.details || j.error || 'draft_create_failed');

      await refresh();
      rightTab = 'menu';
      alert(`Черновик создан: ${j.id}`);
    } catch (e: any) {
      setErr(e);
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

  // Шаблон (временно оставляем один пример)
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
        Создаёт схему/таблицу/поля по твоему вводу + опции (тестовая запись, партиции). Справа — предпросмотр и меню
        шаблонов/черновиков.
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
    <!-- ЛЕВО: СОЗДАНИЕ -->
    <div class="panel">
      <div class="panel-head">
        <h2>Создать таблицу</h2>
        <div class="quick">
          <button on:click={refresh} disabled={loading}>{loading ? 'Обновляю…' : 'Обновить список'}</button>
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

        <!-- КНОПКА ВНИЗУ СПИСКА (как ты просил) -->
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
        <p class="hint">Если заполнить JSON — одна строка будет вставлена после “Применить”.</p>
        <textarea
          bind:value={test_row_text}
          placeholder="{&quot;dataset&quot;:&quot;ads&quot;,&quot;event_date&quot;:&quot;2026-02-17&quot;,&quot;payload&quot;:{&quot;a&quot;:1}}"
        />
      </div>

      <div class="actions">
        <!-- Это и есть “создать” в текущей логике: создаём черновик,
             а “Применить” (создать таблицу) — в меню справа -->
        <button class="primary" on:click={createDraft} disabled={role !== 'data_admin'}>
          Создать (сохранить черновик)
        </button>

        <button on:click={() => (rightTab = 'menu')}>Открыть меню справа</button>
      </div>

      <p class="hint">
        Создание таблицы делается в 2 шага: <b>Создать (черновик)</b> → <b>Применить</b> (в меню справа).
      </p>
    </div>

    <!-- ПРАВО: ТАБЫ -->
    <div class="panel">
      <div class="right-head">
        <div class="right-tabs">
          <button class:active={rightTab === 'preview'} on:click={() => (rightTab = 'preview')}>Предпросмотр</button>
          <button class:active={rightTab === 'menu'} on:click={() => (rightTab = 'menu')}>
            Меню (шаблоны/черновики)
          </button>
        </div>
      </div>

      {#if rightTab === 'preview'}
        <h2>Предпросмотр (5 строк)</h2>

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
      {:else}
        <h2>Меню</h2>

        <div class="menu-card">
          <div class="menu-title">Шаблоны</div>
          <div class="menu-sub">
            Шаблон — это готовый набор полей (чтобы не заполнять руками). Можно использовать или игнорировать.
          </div>
          <div class="actions">
            <button on:click={pickTemplateBronze}>Применить шаблон: Bronze</button>
          </div>
        </div>

        <div class="menu-card">
          <div class="menu-title">Черновики</div>
          <div class="menu-sub">
            Черновик — это “план создания таблицы”. После “Создать (черновик)” — жми “Применить”, и таблица реально
            создастся в базе.
          </div>

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
                      Применить (создать таблицу)
                    </button>
                  </div>
                </div>
              {/each}
            </div>
          {/if}
        </div>
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

  .grid { display:grid; grid-template-columns: 1.2fr 0.8fr; gap: 12px; margin-top: 12px; }
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
  .fields-footer { display:flex; justify-content:flex-start; margin-top: 10px; }

  .row { display:flex; gap:8px; align-items:center; font-size: 12px; color:#334155; }

  .actions { display:flex; gap: 8px; margin-top: 12px; flex-wrap: wrap; }
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

  .right-head { display:flex; justify-content:space-between; align-items:center; gap:10px; }
  .right-tabs { display:flex; gap:8px; flex-wrap: wrap; }
  .right-tabs button.active { background:#0f172a; color:#fff; border-color:#0f172a; }

  .menu-card { border:1px solid #eef2f7; border-radius: 14px; padding: 12px; margin-top: 12px; }
  .menu-title { font-weight: 900; margin-bottom: 6px; }
  .menu-sub { font-size: 12px; color:#64748b; }
</style>
