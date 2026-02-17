<script lang="ts">
  import { onMount } from 'svelte';

  type Role = 'viewer' | 'operator' | 'data_admin';

  type ExistingTable = { schema_name: string; table_name: string };

  type ColumnMeta = {
    name: string;
    type: string;
    nullable: boolean;
    default: string | null;
  };

  const API_BASE = '/ai-orchestrator/api';

  let role: Role = 'data_admin';

  let loading = false;
  let error = '';

  let existingTables: ExistingTable[] = [];

  // Создание таблицы
  let schema_name = '';
  let table_name = '';
  let description = '';
  let table_class = 'custom';

  const typeOptions = ['text', 'int', 'bigint', 'numeric', 'boolean', 'date', 'timestamptz', 'jsonb', 'uuid'];

  let columns: { field_name: string; field_type: string; description?: string }[] = [
    { field_name: '', field_type: 'text', description: '' }
  ];

  // Партиционирование
  let partition_enabled = false;
  let partition_column = 'event_date';
  let partition_interval: 'day' | 'month' = 'day';

  // Тестовая строка (JSON)
  let test_row_text = '';
  const testRowPlaceholder = `{"dataset":"ads","event_date":"2026-02-17","payload":{"a":1}}`;

  // Предпросмотр
  let preview_schema = '';
  let preview_table = '';
  let preview_rows: any[] = [];
  let preview_columns: ColumnMeta[] = [];
  let preview_error = '';
  let preview_loading = false;

  // Ввод новой строки (inline в таблице)
  let newRow: Record<string, any> = {};
  let rowActionError = '';
  let rowActionLoading = false;

  // Модалка: добавить столбец
  let showAddColumnModal = false;
  let addColName = '';
  let addColType = 'text';
  let addColDesc = '';
  let addColError = '';
  let addColLoading = false;

  // Модалка: подтверждение удаления таблицы/колонки
  let confirmText = '';
  let showConfirm = false;
  let confirmLoading = false;
  let confirmError = '';
  let confirmAction: null | (() => Promise<void>) = null;

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

  async function refresh() {
    loading = true;
    error = '';
    try {
      const j = await apiJson(`${API_BASE}/tables`);
      existingTables = j.existing_tables || [];

      if (!preview_schema && existingTables.length) {
        preview_schema = existingTables[0].schema_name;
        preview_table = existingTables[0].table_name;
        await loadMeta();
        await loadPreview();
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

  function normalizeColumns(cols: any[]) {
    return (cols || [])
      .map((c) => ({
        field_name: String(c?.field_name || '').trim(),
        field_type: String(c?.field_type || '').trim(),
        description: String(c?.description || '').trim()
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

    if (!s) throw new Error('Укажи схему');
    if (!t) throw new Error('Укажи таблицу');
    if (!cols.length) throw new Error('Добавь хотя бы одно поле');

    for (const c of cols) {
      if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(c.field_name)) throw new Error(`Некорректное имя поля: ${c.field_name}`);
      if (!typeOptions.includes(c.field_type)) throw new Error(`Некорректный тип поля: ${c.field_type}`);
    }

    if (partition_enabled) {
      const pc = partition_column.trim();
      if (!pc) throw new Error('Укажи колонку для партиций');
    }
  }

  // СОЗДАТЬ ТАБЛИЦУ СРАЗУ (без черновиков)
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
          columns: cols,
          partitioning: partition_enabled
            ? { enabled: true, column: partition_column.trim(), interval: partition_interval }
            : { enabled: false },
          test_row
        })
      });

      await refresh();

      // открыть предпросмотр на созданную
      preview_schema = schema_name.trim();
      preview_table = table_name.trim();
      await loadMeta();
      await loadPreview();

      alert(`Таблица создана: ${preview_schema}.${preview_table}`);
    } catch (e: any) {
      setErr(e);
    }
  }

  async function loadMeta() {
    preview_error = '';
    try {
      if (!preview_schema || !preview_table) return;
      const j = await apiJson(
        `${API_BASE}/meta?schema=${encodeURIComponent(preview_schema)}&table=${encodeURIComponent(preview_table)}`
      );
      preview_columns = j.columns || [];
      // подготовим объект новой строки
      const nr: Record<string, any> = {};
      for (const c of preview_columns) nr[c.name] = '';
      newRow = nr;
    } catch (e: any) {
      preview_error = String(e?.message || e);
    }
  }

  async function loadPreview() {
    preview_loading = true;
    preview_error = '';
    preview_rows = [];
    try {
      if (!preview_schema || !preview_table) throw new Error('Выбери схему и таблицу');

      const j = await apiJson(
        `${API_BASE}/preview?schema=${encodeURIComponent(preview_schema)}&table=${encodeURIComponent(preview_table)}&limit=5`
      );
      preview_rows = j.rows || [];
    } catch (e: any) {
      preview_error = String(e?.message || e);
    } finally {
      preview_loading = false;
    }
  }

  // UI: выбрать таблицу (клик по “чипу”)
  async function pickTable(s: string, t: string) {
    preview_schema = s;
    preview_table = t;
    preview_rows = [];
    await loadMeta();
    await loadPreview();
  }

  // ---------- КОЛОНКИ (из предпросмотра) ----------

  function openAddColumnModal() {
    addColName = '';
    addColType = 'text';
    addColDesc = '';
    addColError = '';
    showAddColumnModal = true;
  }

  async function addColumnToTable() {
    addColError = '';
    try {
      if (!canWrite()) throw new Error('Нужна роль data_admin');
      const name = addColName.trim();
      if (!name) throw new Error('Укажи имя столбца');
      if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) throw new Error('Некорректное имя столбца');
      if (!typeOptions.includes(addColType)) throw new Error('Некорректный тип');

      addColLoading = true;

      await apiJson(`${API_BASE}/tables/column/add`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({
          schema: preview_schema,
          table: preview_table,
          column_name: name,
          column_type: addColType,
          description: addColDesc.trim()
        })
      });

      showAddColumnModal = false;
      await loadMeta();
      await loadPreview();
    } catch (e: any) {
      addColError = String(e?.message || e);
    } finally {
      addColLoading = false;
    }
  }

  function confirmDropColumn(col: string) {
    confirmText = `Удалить столбец "${col}"? Это удалит данные этого столбца.`;
    confirmError = '';
    showConfirm = true;
    confirmAction = async () => {
      if (!canWrite()) throw new Error('Нужна роль data_admin');
      await apiJson(`${API_BASE}/tables/column/drop`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({ schema: preview_schema, table: preview_table, column_name: col })
      });
      await loadMeta();
      await loadPreview();
    };
  }

  // ---------- СТРОКИ (из предпросмотра) ----------

  function coerceValueByType(type: string, v: any) {
    const s = String(v ?? '').trim();
    if (s === '') return null;

    switch (type) {
      case 'int':
      case 'bigint':
        return Number(s);
      case 'numeric':
        return Number(s);
      case 'boolean':
        return s === 'true' || s === '1' || s === 'yes';
      case 'jsonb':
        // разрешаем вводить JSON строкой
        return JSON.parse(s);
      case 'date':
        return s; // yyyy-mm-dd
      case 'timestamptz':
        return s; // ISO
      default:
        return s;
    }
  }

  async function addRowInline() {
    rowActionError = '';
    try {
      if (!canWrite()) throw new Error('Нужна роль data_admin');

      rowActionLoading = true;

      const payload: Record<string, any> = {};
      for (const c of preview_columns) {
        // __ctid не колонка таблицы
        if (c.name === '__ctid') continue;
        if (c.name in newRow) {
          payload[c.name] = coerceValueByType(c.type, newRow[c.name]);
        }
      }

      await apiJson(`${API_BASE}/tables/row/add`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({ schema: preview_schema, table: preview_table, row: payload })
      });

      await loadPreview();

      // очистим поля ввода
      const nr: Record<string, any> = {};
      for (const c of preview_columns) nr[c.name] = '';
      newRow = nr;
    } catch (e: any) {
      rowActionError = String(e?.message || e);
    } finally {
      rowActionLoading = false;
    }
  }

  function confirmDeleteRow(ctid: string) {
    confirmText = `Удалить эту строку?`;
    confirmError = '';
    showConfirm = true;
    confirmAction = async () => {
      if (!canWrite()) throw new Error('Нужна роль data_admin');
      await apiJson(`${API_BASE}/tables/row/delete`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({ schema: preview_schema, table: preview_table, ctid })
      });
      await loadPreview();
    };
  }

  // ---------- УДАЛИТЬ ТАБЛИЦУ ----------

  function confirmDropTable() {
    confirmText = `Удалить таблицу ${preview_schema}.${preview_table}? (CASCADE)`;
    confirmError = '';
    showConfirm = true;
    confirmAction = async () => {
      if (!canWrite()) throw new Error('Нужна роль data_admin');
      await apiJson(`${API_BASE}/tables/drop`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({ schema: preview_schema, table: preview_table })
      });
      await refresh();
      preview_rows = [];
      preview_columns = [];
      preview_schema = '';
      preview_table = '';
    };
  }

  async function runConfirm() {
    if (!confirmAction) return;
    confirmLoading = true;
    confirmError = '';
    try {
      await confirmAction();
      showConfirm = false;
    } catch (e: any) {
      confirmError = String(e?.message || e);
    } finally {
      confirmLoading = false;
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
        Создаёт схему/таблицу/поля. Справа — предпросмотр 5 строк + действия (столбцы/строки/удаление таблицы).
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
          <button on:click={pickTemplateBronze}>Заполнить шаблон Bronze</button>
          <button on:click={refresh} disabled={loading}>{loading ? 'Загрузка…' : 'Обновить список'}</button>
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

        <!-- КНОПКА ВНИЗУ БЛОКА -->
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
        <textarea bind:value={test_row_text} placeholder={testRowPlaceholder} />
      </div>

      <div class="actions">
        <button class="primary" on:click={createTableNow} disabled={!canWrite()}>
          Создать таблицу
        </button>
      </div>

      {#if !canWrite()}
        <p class="hint">Кнопка активна только при роли <b>data_admin</b>.</p>
      {/if}
    </div>

    <!-- ПРАВО: ТЕКУЩИЕ + ПРЕДПРОСМОТР С КНОПКАМИ -->
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
            <button class="chip" on:click={() => pickTable(t.schema_name, t.table_name)}>
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
          <button class="danger" on:click={confirmDropTable} disabled={!canWrite() || !preview_schema || !preview_table}>
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
          <select
            bind:value={preview_schema}
            on:change={async () => {
              preview_rows = [];
              await loadMeta();
            }}
          >
            {#each Array.from(new Set(existingTables.map((t) => t.schema_name))) as s}
              <option value={s}>{s}</option>
            {/each}
          </select>
        </label>

        <label>
          Таблица
          <select
            bind:value={preview_table}
            on:change={async () => {
              preview_rows = [];
              await loadMeta();
            }}
          >
            {#each existingTables.filter((t) => t.schema_name === preview_schema) as t}
              <option value={t.table_name}>{t.schema_name}.{t.table_name}</option>
            {/each}
          </select>
        </label>
      </div>

      {#if rowActionError}
        <div class="alert">
          <div class="alert-title">Ошибка действия</div>
          <pre>{rowActionError}</pre>
        </div>
      {/if}

      {#if preview_schema && preview_table}
        <div class="preview">
          <table>
            <thead>
              <tr>
                {#each preview_columns as c}
                  {#if c.name !== '__ctid'}
                    <th>
                      <div class="th-wrap">
                        <span>{c.name}</span>
                        <button
                          class="icon danger"
                          title="Удалить столбец"
                          on:click={() => confirmDropColumn(c.name)}
                          disabled={!canWrite()}
                        >
                          ×
                        </button>
                      </div>
                    </th>
                  {/if}
                {/each}

                <!-- последняя “пустая колонка” = ДОБАВИТЬ СТОЛБЕЦ -->
                <th class="th-add">
                  <button class="icon" title="Добавить столбец" on:click={openAddColumnModal} disabled={!canWrite()}>
                    +
                  </button>
                </th>

                <!-- колонка действий строк -->
                <th class="th-actions">Действия</th>
              </tr>
            </thead>

            <tbody>
              {#if preview_rows.length > 0}
                {#each preview_rows as r}
                  <tr>
                    {#each preview_columns as c}
                      {#if c.name !== '__ctid'}
                        <td>{typeof r[c.name] === 'object' ? JSON.stringify(r[c.name]) : String(r[c.name] ?? '')}</td>
                      {/if}
                    {/each}

                    <!-- “пустая” колонка add-col -->
                    <td></td>

                    <!-- действия по строке -->
                    <td class="td-actions">
                      <button
                        class="icon danger"
                        title="Удалить строку"
                        on:click={() => confirmDeleteRow(String(r.__ctid || ''))}
                        disabled={!canWrite() || !r.__ctid}
                      >
                        🗑
                      </button>
                    </td>
                  </tr>
                {/each}
              {/if}

              <!-- ПОСЛЕДНЯЯ СТРОКА: ДОБАВИТЬ СТРОКУ (как продолжение таблицы) -->
              <tr class="add-row">
                {#each preview_columns as c}
                  {#if c.name !== '__ctid'}
                    <td>
                      <input
                        class="cell-input"
                        placeholder={c.type}
                        bind:value={newRow[c.name]}
                        disabled={!canWrite()}
                      />
                    </td>
                  {/if}
                {/each}

                <td></td>

                <td class="td-actions">
                  <button class="icon" title="Добавить строку" on:click={addRowInline} disabled={!canWrite() || rowActionLoading}>
                    +
                  </button>
                </td>
              </tr>
            </tbody>
          </table>

          {#if preview_rows.length === 0}
            <p class="hint" style="padding:10px 12px;">Нет данных (таблица может быть пустой). Добавь строку снизу.</p>
          {/if}
        </div>
      {:else}
        <p class="hint">Выбери схему и таблицу.</p>
      {/if}
    </div>
  </section>

  <!-- МОДАЛКА: добавить столбец -->
  {#if showAddColumnModal}
    <div class="modal-backdrop" on:click={() => (showAddColumnModal = false)}>
      <div class="modal" on:click|stopPropagation>
        <h3>Добавить столбец</h3>

        {#if addColError}
          <div class="alert">
            <div class="alert-title">Ошибка</div>
            <pre>{addColError}</pre>
          </div>
        {/if}

        <div class="form">
          <label>
            Имя столбца
            <input bind:value={addColName} placeholder="например: campaign_id" />
          </label>

          <label>
            Тип
            <select bind:value={addColType}>
              {#each typeOptions as t}
                <option value={t}>{t}</option>
              {/each}
            </select>
          </label>

          <label>
            Описание (опционально)
            <input bind:value={addColDesc} placeholder="что это за поле" />
          </label>
        </div>

        <div class="actions">
          <button on:click={() => (showAddColumnModal = false)}>Отмена</button>
          <button class="primary" on:click={addColumnToTable} disabled={!canWrite() || addColLoading}>
            {addColLoading ? 'Добавляю…' : 'Добавить'}
          </button>
        </div>
      </div>
    </div>
  {/if}

  <!-- МОДАЛКА: подтверждение (удалить столбец/строку/таблицу) -->
  {#if showConfirm}
    <div class="modal-backdrop" on:click={() => (showConfirm = false)}>
      <div class="modal" on:click|stopPropagation>
        <h3>Подтверждение</h3>
        <p class="hint">{confirmText}</p>

        {#if confirmError}
          <div class="alert">
            <div class="alert-title">Ошибка</div>
            <pre>{confirmError}</pre>
          </div>
        {/if}

        <div class="actions">
          <button on:click={() => (showConfirm = false)} disabled={confirmLoading}>Отмена</button>
          <button class="danger" on:click={runConfirm} disabled={confirmLoading}>
            {confirmLoading ? 'Выполняю…' : 'Да, удалить'}
          </button>
        </div>
      </div>
    </div>
  {/if}
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
  .actions { display:flex; gap: 8px; margin-top: 12px; }

  button { padding: 10px 12px; border-radius: 12px; border:1px solid #e2e8f0; background:#fff; cursor:pointer; }
  button.primary { background:#0f172a; color:#fff; border-color:#0f172a; }
  button:disabled { opacity:0.5; cursor:not-allowed; }
  button.danger { border:1px solid #fecaca; color:#b91c1c; }

  .hint { margin-top: 10px; font-size: 12px; color:#64748b; }

  .alert { background:#fff1f2; border:1px solid #fecdd3; border-radius: 14px; padding: 12px; margin: 12px 0; }
  .alert-title { font-weight: 800; margin-bottom: 6px; }
  pre { white-space: pre-wrap; margin:0; font-size: 12px; }

  .tables-list { display:flex; gap:8px; flex-wrap:wrap; margin-top: 12px; }
  .chip { padding: 6px 10px; border-radius: 999px; border:1px solid #e2e8f0; font-size: 12px; color:#334155; background:#fff; cursor:pointer; }

  .preview-head { display:flex; justify-content:space-between; align-items:center; gap: 10px; margin-top: 16px; }
  .preview-actions { display:flex; gap: 8px; flex-wrap: wrap; }

  .preview { margin-top: 10px; overflow:auto; border:1px solid #eef2f7; border-radius: 14px; }
  table { border-collapse: collapse; width: 100%; font-size: 12px; }
  th, td { border-bottom:1px solid #eef2f7; padding: 8px 10px; text-align:left; vertical-align:top; }
  th { position: sticky; top: 0; background:#fff; z-index: 1; }

  .th-wrap { display:flex; align-items:center; justify-content:space-between; gap: 8px; }
  .icon { padding: 4px 8px; border-radius: 10px; border:1px solid #e2e8f0; background:#fff; cursor:pointer; line-height: 1; }
  .icon.danger { border-color:#fecaca; color:#b91c1c; }

  .th-add { width: 56px; text-align:center; }
  .th-actions { width: 90px; text-align:center; }

  .td-actions { text-align:center; }

  .cell-input { width: 100%; padding: 8px 10px; border-radius: 10px; border:1px solid #e2e8f0; }

  .add-row td { background: #fbfdff; }

  /* Модалки */
  .modal-backdrop {
    position: fixed; inset: 0;
    background: rgba(15, 23, 42, 0.35);
    display:flex; align-items:center; justify-content:center;
    padding: 16px;
  }
  .modal {
    width: min(520px, 100%);
    background:#fff;
    border:1px solid #eef2f7;
    border-radius: 16px;
    padding: 14px;
    box-shadow: 0 10px 30px rgba(0,0,0,0.12);
  }
  .modal h3 { margin: 0 0 8px; }
</style>
