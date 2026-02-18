<script lang="ts">
  import { onMount } from 'svelte';

  type Role = 'viewer' | 'operator' | 'data_admin';

  type ColumnDef = {
    field_name: string;
    field_type: string;
    description?: string;
  };

  type ExistingTable = {
    schema_name: string;
    table_name: string;
  };

  const API_BASE = '/ai-orchestrator/api';

  // Роль (пока клиентская, сервер проверяет через X-AO-ROLE)
  let role: Role = 'data_admin';

  // Списки
  let loading = false;
  let error = '';
  let existingTables: ExistingTable[] = [];

  // Создание таблицы
  let schema_name = '';
  let table_name = '';
  let description = '';
  let table_class = 'custom';

  const typeOptions = ['text', 'int', 'bigint', 'numeric', 'boolean', 'date', 'timestamptz', 'jsonb', 'uuid'];
  let columns: ColumnDef[] = [{ field_name: '', field_type: 'text', description: '' }];

  // Партиционирование
  let partition_enabled = false;
  let partition_column = 'event_date';
  let partition_interval: 'day' | 'month' = 'day';

  // Тестовая строка (JSON)
  let test_row_text = '';
  const TEST_ROW_PLACEHOLDER = `{"dataset":"ads","event_date":"2026-02-17","payload":{"a":1}}`;

  // UI: вкладки
  let tab: 'constructor' | 'tables' = 'constructor';

  function setErr(e: any) {
    error = String(e?.details || e?.message || e);
  }

  function canWrite(): boolean {
    return role === 'data_admin';
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

  async function refresh() {
    loading = true;
    error = '';
    try {
      const j = await apiJson(`${API_BASE}/tables`, { method: 'GET' });
      existingTables = j.existing_tables || [];
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

  function normalizeColumns(cols: ColumnDef[]): ColumnDef[] {
    return cols
      .map((c) => ({
        field_name: (c.field_name || '').trim(),
        field_type: (c.field_type || '').trim(),
        description: (c.description || '').trim()
      }))
      .filter((c) => c.field_name.length > 0);
  }

  function parseTestRow(): any | null {
    const t = (test_row_text || '').trim();
    if (!t) return null;

    const parsed = JSON.parse(t);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new Error('Тестовая запись должна быть JSON-объектом (например {"a":1})');
    }
    return parsed;
  }

  function validateCreate() {
    const s = schema_name.trim();
    const t = table_name.trim();
    const cols = normalizeColumns(columns);

    if (!s) throw new Error('Укажи схему (schema)');
    if (!t) throw new Error('Укажи таблицу (table)');
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
    }
  }

  // Создаём таблицу сразу (без "черновиков")
  async function createTableNow() {
    error = '';
    try {
      if (!canWrite()) throw new Error('Недостаточно прав (нужна роль data_admin)');
      validateCreate();

      const cols = normalizeColumns(columns);
      const test_row = parseTestRow();

      await apiJson(`${API_BASE}/tables/create`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({
          schema_name: schema_name.trim(),
          table_name: table_name.trim(),
          table_class,
          description: description.trim(),
          created_by: 'ui',
          columns: cols,
          partitioning: partition_enabled
            ? { enabled: true, column: partition_column.trim(), interval: partition_interval }
            : { enabled: false },
          test_row
        })
      });

      await refresh();
      alert(`Таблица создана: ${schema_name.trim()}.${table_name.trim()}`);
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
        Создание таблицы по твоему вводу. Для просмотра/редактирования существующих таблиц используй вкладку «Таблицы и данные».
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

  <nav class="tabs">
    <button class:active={tab === 'constructor'} on:click={() => (tab = 'constructor')}>Создание</button>
    <button class:active={tab === 'tables'} on:click={() => (tab = 'tables')}>Таблицы и данные</button>
  </nav>

  {#if error}
    <div class="alert">
      <div class="alert-title">Ошибка</div>
      <pre>{error}</pre>
    </div>
  {/if}

  {#if tab === 'constructor'}
    <section class="grid">
      <div class="panel">
        <div class="panel-head">
          <h2>Создать таблицу</h2>
          <div class="quick">
            <button on:click={pickTemplateBronze}>Заполнить: Bronze</button>
            <button on:click={refresh} disabled={loading}>{loading ? 'Загрузка…' : 'Обновить список'}</button>
          </div>
        </div>

        <div class="form">
          <label>
            Схема
            <input bind:value={schema_name} placeholder="например: bronze / showcase" />
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
              <input placeholder="имя поля (field_name)" bind:value={c.field_name} />
              <select bind:value={c.field_type}>
                {#each typeOptions as t}
                  <option value={t}>{t}</option>
                {/each}
              </select>
              <input placeholder="описание" bind:value={c.description} />
              <button class="danger" on:click={() => removeColumn(ix)} title="Удалить поле">Удалить</button>
            </div>
          {/each}

          <!-- кнопка внизу блока -->
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
                <input bind:value={partition_column} placeholder="event_date / ingested_at ..." />
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
    </section>
  {:else}
    <!-- Вкладку “Таблицы и данные” НЕ ТРОГАЛ: она живёт в текущей версии репозитория.
         Здесь оставлен минимальный контейнер, чтобы навигация работала. -->
    <section class="panel">
      <h2>Таблицы и данные</h2>
      <p class="hint">Эта вкладка остаётся как в твоей текущей версии (без изменений).</p>
    </section>
  {/if}
</main>

<style>
  .root { padding: 18px; }

  .header { display:flex; justify-content:space-between; gap:12px; align-items:flex-start; }
  h1 { margin:0; font-size: 20px; font-weight: 800; }
  .sub { margin: 6px 0 0; font-size: 12px; color:#64748b; }
  .role { display:flex; gap:8px; align-items:center; font-size: 12px; color:#334155; }

  .tabs { display:flex; gap:8px; margin-top: 12px; }
  .tabs button { padding: 8px 10px; border-radius: 12px; border:1px solid #e2e8f0; background:#fff; cursor:pointer; }
  .tabs button.active { background:#0f172a; color:#fff; border-color:#0f172a; }

  .grid { display:grid; grid-template-columns: 1fr; gap: 12px; margin-top: 12px; }

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
  .fields-footer { margin-top: 10px; display:flex; justify-content:flex-end; }

  .row { display:flex; gap:8px; align-items:center; font-size: 12px; color:#334155; }

  .actions { display:flex; gap: 8px; margin-top: 12px; }
  button { padding: 10px 12px; border-radius: 12px; border:1px solid #e2e8f0; background:#fff; cursor:pointer; }
  button.primary { background:#0f172a; color:#fff; border-color:#0f172a; }
  button:disabled { opacity:0.5; cursor:not-allowed; }
  .danger { border:1px solid #fecaca; color:#b91c1c; }

  .hint { margin-top: 10px; font-size: 12px; color:#64748b; }

  .alert { background:#fff1f2; border:1px solid #fecdd3; border-radius: 14px; padding: 12px; margin: 12px 0; }
  .alert-title { font-weight: 800; margin-bottom: 6px; }
  pre { white-space: pre-wrap; margin:0; font-size: 12px; }
</style>
