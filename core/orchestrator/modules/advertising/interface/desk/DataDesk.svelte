<script lang="ts">
  import { onMount } from 'svelte';

  type Role = 'viewer' | 'operator' | 'data_admin';
  type ExistingTable = { schema_name: string; table_name: string };
  type ColumnMeta = { name: string; type: string; description?: string; is_nullable?: boolean };

  const API_BASE = '/ai-orchestrator/api';

  // role (client-side). Server checks header X-AO-ROLE.
  let role: Role = 'data_admin';

  // global state
  let loading = false;
  let error = '';

  // tables list
  let existingTables: ExistingTable[] = [];

  // Create table form
  let schema_name = '';
  let table_name = '';
  let description = '';
  let table_class = 'custom';

  const typeOptions = ['text', 'int', 'bigint', 'numeric', 'boolean', 'date', 'timestamptz', 'jsonb', 'uuid'];

  type ColumnDef = { field_name: string; field_type: string; description?: string };
  let columns: ColumnDef[] = [{ field_name: '', field_type: 'text', description: '' }];

  // Partitioning
  let partition_enabled = false;
  let partition_column = 'event_date';
  let partition_interval: 'day' | 'month' = 'day';

  // Test row (IMPORTANT: do NOT inline JSON with { } into attribute string in Svelte)
  const TEST_ROW_PLACEHOLDER = `{"dataset":"ads","event_date":"2026-02-17","payload":{"a":1}}`;
  let test_row_text = '';

  // Preview table
  let preview_schema = '';
  let preview_table = '';
  let preview_columns: ColumnMeta[] = [];
  let preview_rows: any[] = [];
  let preview_loading = false;
  let preview_error = '';

  // CRUD UI state
  let modal = '' as '' | 'addColumn' | 'confirmDropColumn' | 'confirmDropTable' | 'confirmDeleteRow';
  let modal_error = '';

  // add-column modal inputs
  let new_col_name = '';
  let new_col_type = 'text';
  let new_col_desc = '';

  // confirm delete column
  let pending_drop_column = '';

  // confirm delete row
  let pending_delete_ctid = '';

  // add-row inline editor (tfoot inputs)
  let newRow: Record<string, string> = {};

  // Tabs
  let tab: 'constructor' | 'preview' = 'constructor';

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

  async function refreshTables() {
    loading = true;
    error = '';
    try {
      const j = await apiJson(`${API_BASE}/tables`);
      existingTables = j.existing_tables || [];

      if (!preview_schema && existingTables.length) {
        preview_schema = existingTables[0].schema_name;
        preview_table = existingTables[0].table_name;
      }

      if (preview_schema && preview_table) {
        await loadColumns();
      }
    } catch (e: any) {
      setErr(e);
    } finally {
      loading = false;
    }
  }

  function addColumnForm() {
    columns = [...columns, { field_name: '', field_type: 'text', description: '' }];
  }

  function removeColumnForm(ix: number) {
    columns = columns.filter((_, i) => i !== ix);
    if (!columns.length) columns = [{ field_name: '', field_type: 'text', description: '' }];
  }

  function normalizeColumns(cols: ColumnDef[]): ColumnDef[] {
    return cols
      .map((c) => ({
        field_name: (c.field_name || '').trim(),
        field_type: (c.field_type || 'text').trim(),
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
      if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(pc)) throw new Error(`Некорректная колонка партиций: ${pc}`);
    }
  }

  // NO drafts: create table immediately
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

      await refreshTables();

      preview_schema = schema_name.trim();
      preview_table = table_name.trim();
      tab = 'preview';
      await loadColumns();
      await loadPreview();

      alert(`Таблица создана: ${preview_schema}.${preview_table}`);
    } catch (e: any) {
      setErr(e);
    }
  }

  async function loadColumns() {
    preview_error = '';
    preview_columns = [];
    try {
      if (!preview_schema || !preview_table) return;
      const j = await apiJson(
        `${API_BASE}/columns?schema=${encodeURIComponent(preview_schema)}&table=${encodeURIComponent(preview_table)}`
      );
      preview_columns = j.columns || [];
      const obj: Record<string, string> = {};
      for (const c of preview_columns) obj[c.name] = '';
      newRow = obj;
    } catch (e: any) {
      preview_error = String(e?.message || e);
    }
  }

  async function loadPreview() {
    preview_loading = true;
    preview_error = '';
    preview_rows = [];
    try {
      if (!preview_schema || !preview_table) throw new Error('Выбери схему и таблицу для предпросмотра');

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

  function openAddColumnModal() {
    modal_error = '';
    new_col_name = '';
    new_col_type = 'text';
    new_col_desc = '';
    modal = 'addColumn';
  }

  async function addColumnToTable() {
    modal_error = '';
    try {
      if (!canWrite()) throw new Error('Недостаточно прав (нужна роль data_admin)');
      const name = new_col_name.trim();
      if (!name) throw new Error('Укажи имя столбца');
      if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) throw new Error('Имя столбца некорректное (только a-zA-Z0-9_)');

      await apiJson(`${API_BASE}/columns/add`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({
          schema: preview_schema,
          table: preview_table,
          column: { name, type: new_col_type, description: new_col_desc.trim() }
        })
      });

      modal = '';
      await loadColumns();
      await loadPreview();
    } catch (e: any) {
      modal_error = String(e?.message || e);
    }
  }

  function confirmDropColumn(name: string) {
    pending_drop_column = name;
    modal_error = '';
    modal = 'confirmDropColumn';
  }

  async function dropColumnNow() {
    modal_error = '';
    try {
      if (!canWrite()) throw new Error('Недостаточно прав (нужна роль data_admin)');

      await apiJson(`${API_BASE}/columns/drop`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({ schema: preview_schema, table: preview_table, column_name: pending_drop_column })
      });

      modal = '';
      pending_drop_column = '';
      await loadColumns();
      await loadPreview();
    } catch (e: any) {
      modal_error = String(e?.message || e);
    }
  }

  function confirmDropTable() {
    modal_error = '';
    modal = 'confirmDropTable';
  }

  async function dropTableNow() {
    modal_error = '';
    try {
      if (!canWrite()) throw new Error('Недостаточно прав (нужна роль data_admin)');

      await apiJson(`${API_BASE}/tables/drop`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({ schema: preview_schema, table: preview_table })
      });

      modal = '';
      preview_rows = [];
      preview_columns = [];
      newRow = {};

      await refreshTables();
      alert('Таблица удалена');
    } catch (e: any) {
      modal_error = String(e?.message || e);
    }
  }

  function coerceValue(type: string, raw: string): any {
    const v = raw.trim();
    if (v === '') return null;

    if (type === 'int' || type === 'bigint') {
      const n = Number(v);
      if (!Number.isFinite(n)) throw new Error(`Ожидалось число (${type})`);
      return Math.trunc(n);
    }
    if (type === 'numeric') {
      const n = Number(v);
      if (!Number.isFinite(n)) throw new Error('Ожидалось число (numeric)');
      return n;
    }
    if (type === 'boolean') {
      if (v === 'true' || v === '1') return true;
      if (v === 'false' || v === '0') return false;
      throw new Error('boolean: введи true/false');
    }
    if (type === 'jsonb') {
      return JSON.parse(v);
    }
    return v; // text/date/timestamptz/uuid
  }

  async function addRowNow() {
    error = '';
    try {
      if (!canWrite()) throw new Error('Недостаточно прав (нужна роль data_admin)');

      const row: any = {};
      for (const c of preview_columns) {
        const raw = newRow[c.name] ?? '';
        const val = coerceValue(c.type, raw);
        if (val !== null) row[c.name] = val;
      }

      if (!Object.keys(row).length) throw new Error('Заполни хотя бы одно значение');

      await apiJson(`${API_BASE}/rows/add`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({ schema: preview_schema, table: preview_table, row })
      });

      const obj: Record<string, string> = {};
      for (const c of preview_columns) obj[c.name] = '';
      newRow = obj;

      await loadPreview();
    } catch (e: any) {
      setErr(e);
    }
  }

  function confirmDeleteRow(ctid: string) {
    pending_delete_ctid = ctid;
    modal_error = '';
    modal = 'confirmDeleteRow';
  }

  async function deleteRowNow() {
    modal_error = '';
    try {
      if (!canWrite()) throw new Error('Недостаточно прав (нужна роль data_admin)');
      await apiJson(`${API_BASE}/rows/delete`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-AO-ROLE': role },
        body: JSON.stringify({ schema: preview_schema, table: preview_table, __ctid: pending_delete_ctid })
      });
      modal = '';
      pending_delete_ctid = '';
      await loadPreview();
    } catch (e: any) {
      modal_error = String(e?.message || e);
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

  function pickExisting(t: ExistingTable) {
    preview_schema = t.schema_name;
    preview_table = t.table_name;
    tab = 'preview';
    preview_rows = [];
    loadColumns();
  }

  $: createDisabled = !canWrite();

  onMount(refreshTables);
</script>

<main class="root">
  <header class="header">
    <div>
      <h1>Конструктор таблиц</h1>
      <p class="sub">
        Слева — создание таблицы. Справа — текущие таблицы и “живой” предпросмотр (редактирование: столбцы/строки/удаление).
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
    <button class:active={tab === 'preview'} on:click={() => (tab = 'preview')}>Таблицы и данные</button>
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
              <button class="danger" on:click={() => removeColumnForm(ix)} title="Удалить поле">Удалить</button>
            </div>
          {/each}

          <div class="fields-footer">
            <button on:click={addColumnForm}>+ Добавить поле</button>
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

        <div class="panel2">
          <h3>Тестовая запись (опционально)</h3>
          <p class="hint">Если заполнить JSON — одна строка будет вставлена сразу после создания таблицы.</p>
          <textarea bind:value={test_row_text} placeholder={TEST_ROW_PLACEHOLDER} />
        </div>

        <div class="actions">
          <button class="primary" on:click={createTableNow} disabled={createDisabled}>
            Создать таблицу
          </button>
        </div>

        {#if !canWrite()}
          <p class="hint">Кнопка активна только при роли <b>data_admin</b>.</p>
        {/if}
      </div>

      <div class="panel">
        <div class="panel-head">
          <h2>Текущие таблицы</h2>
          <div class="quick">
            <button on:click={refreshTables} disabled={loading}>{loading ? 'Загрузка…' : 'Обновить'}</button>
          </div>
        </div>

        {#if loading}
          <p>Загрузка…</p>
        {:else if existingTables.length === 0}
          <p class="hint">Таблицы не найдены.</p>
        {:else}
          <div class="tables-list">
            {#each existingTables as t}
              <button class="chip" on:click={() => pickExisting(t)} title="Открыть">
                {t.schema_name}.{t.table_name}
              </button>
            {/each}
          </div>
          <p class="hint">Нажми на таблицу, чтобы перейти во вкладку “Таблицы и данные”.</p>
        {/if}
      </div>
    </section>
  {:else}
    <section class="panel">
      <div class="panel-head">
        <h2>Таблицы и данные</h2>
        <div class="quick">
          <button on:click={refreshTables} disabled={loading}>{loading ? 'Загрузка…' : 'Обновить список'}</button>
        </div>
      </div>

      <div class="two">
        <div class="left">
          <h3>Текущие таблицы</h3>
          {#if existingTables.length === 0}
            <p class="hint">Таблиц нет.</p>
          {:else}
            <div class="tables-list">
              {#each existingTables as t}
                <button
                  class="chip"
                  class:activechip={t.schema_name === preview_schema && t.table_name === preview_table}
                  on:click={() => pickExisting(t)}
                >
                  {t.schema_name}.{t.table_name}
                </button>
              {/each}
            </div>
          {/if}
        </div>

        <div class="right">
          <div class="panel-head" style="margin-top:0;">
            <div>
              <h3 style="margin:0;">Предпросмотр (5 строк)</h3>
              <div class="hint" style="margin:4px 0 0;">
                {#if preview_schema && preview_table}
                  {preview_schema}.{preview_table}
                {:else}
                  Выбери таблицу слева
                {/if}
              </div>
            </div>

            <div class="quick">
              <button on:click={loadColumns} disabled={!preview_schema || !preview_table}>Колонки</button>
              <button on:click={loadPreview} disabled={preview_loading || !preview_schema || !preview_table}>
                {preview_loading ? 'Загрузка…' : 'Обновить 5 строк'}
              </button>
              <button class="danger" on:click={confirmDropTable} disabled={!canWrite() || !preview_schema || !preview_table}>
                Удалить таблицу
              </button>
            </div>
          </div>

          {#if preview_error}
            <div class="alert">
              <div class="alert-title">Ошибка</div>
              <pre>{preview_error}</pre>
            </div>
          {/if}

          {#if !preview_schema || !preview_table}
            <p class="hint">Выбери таблицу слева.</p>
          {:else}
            {#if preview_columns.length === 0}
              <p class="hint">Колонки не загружены. Нажми “Колонки”.</p>
            {:else}
              <div class="preview">
                <table>
                  <thead>
                    <tr>
                      {#each preview_columns as c}
                        <th>
                          <div class="thwrap">
                            <span class="thname" title={c.description || ''}>{c.name}</span>
                            <button
                              class="xbtn"
                              on:click={() => confirmDropColumn(c.name)}
                              disabled={!canWrite()}
                              title="Удалить столбец"
                            >
                              ×
                            </button>
                          </div>
                        </th>
                      {/each}

                      <!-- add-column cell (part of table header) -->
                      <th class="thadd">
                        <button class="plusbtn" on:click={openAddColumnModal} disabled={!canWrite()} title="Добавить столбец">
                          +
                        </button>
                      </th>
                    </tr>
                  </thead>

                  <tbody>
                    {#if preview_rows.length === 0}
                      <tr>
                        <td colspan={preview_columns.length + 1} class="muted">
                          Нет строк (таблица может быть пустой).
                        </td>
                      </tr>
                    {:else}
                      {#each preview_rows as r}
                        <tr>
                          {#each preview_columns as c}
                            <td>{typeof r[c.name] === 'object' ? JSON.stringify(r[c.name]) : String(r[c.name] ?? '')}</td>
                          {/each}
                          <td class="rowactions">
                            <button
                              class="trash"
                              on:click={() => confirmDeleteRow(r.__ctid)}
                              disabled={!canWrite()}
                              title="Удалить строку"
                            >
                              🗑
                            </button>
                          </td>
                        </tr>
                      {/each}
                    {/if}
                  </tbody>

                  <!-- ADD ROW прямо в конце таблицы (как ты просил) -->
                  <tfoot>
                    <tr>
                      {#each preview_columns as c}
                        <td>
                          <input
                            class="cellinput"
                            bind:value={newRow[c.name]}
                            placeholder={c.type}
                            disabled={!canWrite()}
                          />
                        </td>
                      {/each}
                      <td class="rowactions">
                        <button class="addrow" on:click={addRowNow} disabled={!canWrite()} title="Добавить строку">
                          Добавить
                        </button>
                      </td>
                    </tr>
                  </tfoot>
                </table>
              </div>

              {#if !canWrite()}
                <p class="hint">Редактирование доступно только при роли <b>data_admin</b>.</p>
              {/if}
            {/if}
          {/if}
        </div>
      </div>
    </section>
  {/if}

  <!-- MODALS -->
  {#if modal !== ''}
    <div class="modal-bg" on:click={() => (modal = '')}>
      <div class="modal" on:click|stopPropagation>
        {#if modal === 'addColumn'}
          <h3 style="margin-top:0;">Добавить столбец</h3>
          <div class="form">
            <label>
              Имя столбца
              <input bind:value={new_col_name} placeholder="например: clicks" />
            </label>
            <label>
              Тип
              <select bind:value={new_col_type}>
                {#each typeOptions as t}
                  <option value={t}>{t}</option>
                {/each}
              </select>
            </label>
            <label>
              Описание
              <input bind:value={new_col_desc} placeholder="опционально" />
            </label>
          </div>

          {#if modal_error}
            <div class="alert" style="margin-top:12px;">
              <div class="alert-title">Ошибка</div>
              <pre>{modal_error}</pre>
            </div>
          {/if}

          <div class="actions">
            <button on:click={() => (modal = '')}>Отмена</button>
            <button class="primary" on:click={addColumnToTable} disabled={!canWrite()}>Добавить</button>
          </div>
        {/if}

        {#if modal === 'confirmDropColumn'}
          <h3 style="margin-top:0;">Подтверждение</h3>
          <p>Удалить столбец <b>{pending_drop_column}</b>? Это удалит данные этого столбца.</p>

          {#if modal_error}
            <div class="alert" style="margin-top:12px;">
              <div class="alert-title">Ошибка</div>
              <pre>{modal_error}</pre>
            </div>
          {/if}

          <div class="actions">
            <button on:click={() => (modal = '')}>Отмена</button>
            <button class="danger" on:click={dropColumnNow} disabled={!canWrite()}>Да, удалить</button>
          </div>
        {/if}

        {#if modal === 'confirmDropTable'}
          <h3 style="margin-top:0;">Подтверждение</h3>
          <p>Удалить таблицу <b>{preview_schema}.{preview_table}</b>? Это действие необратимо.</p>

          {#if modal_error}
            <div class="alert" style="margin-top:12px;">
              <div class="alert-title">Ошибка</div>
              <pre>{modal_error}</pre>
            </div>
          {/if}

          <div class="actions">
            <button on:click={() => (modal = '')}>Отмена</button>
            <button class="danger" on:click={dropTableNow} disabled={!canWrite()}>Да, удалить таблицу</button>
          </div>
        {/if}

        {#if modal === 'confirmDeleteRow'}
          <h3 style="margin-top:0;">Подтверждение</h3>
          <p>Удалить эту строку?</p>

          {#if modal_error}
            <div class="alert" style="margin-top:12px;">
              <div class="alert-title">Ошибка</div>
              <pre>{modal_error}</pre>
            </div>
          {/if}

          <div class="actions">
            <button on:click={() => (modal = '')}>Отмена</button>
            <button class="danger" on:click={deleteRowNow} disabled={!canWrite()}>Да, удалить</button>
          </div>
        {/if}
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

  .tabs { display:flex; gap:8px; margin-top: 12px; }
  .tabs button { padding: 8px 10px; border-radius: 12px; border:1px solid #e2e8f0; background:#fff; cursor:pointer; }
  .tabs button.active { background:#0f172a; color:#fff; border-color:#0f172a; }

  .grid { display:grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-top: 12px; }
  @media (max-width: 1100px) { .grid { grid-template-columns: 1fr; } }

  .panel { background:#fff; border:1px solid #eef2f7; border-radius: 16px; padding: 14px; }
  .panel2 { margin-top: 12px; border:1px solid #eef2f7; border-radius: 14px; padding: 12px; }

  .panel-head { display:flex; justify-content:space-between; align-items:center; gap:8px; }
  .quick { display:flex; gap: 8px; flex-wrap:wrap; }

  .form { display:grid; gap: 10px; margin-top: 12px; }
  label { display:grid; gap: 6px; font-size: 12px; color:#334155; }
  input, select, textarea { padding: 10px 12px; border-radius: 12px; border:1px solid #e2e8f0; outline: none; }
  textarea { min-height: 90px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }

  .fields { margin-top: 14px; }
  .field-row { display:grid; grid-template-columns: 1.2fr 0.8fr 1.6fr auto; gap: 8px; margin-top: 8px; }
  .fields-footer { margin-top: 10px; display:flex; justify-content:flex-end; }

  .row { display:flex; gap:8px; align-items:center; font-size: 12px; color:#334155; }

  .actions { display:flex; gap: 8px; margin-top: 12px; }
  button { padding: 10px 12px; border-radius: 12px; border:1px solid #e2e8f0; background:#fff; cursor:pointer; }
  button.primary { background:#0f172a; color:#fff; border-color:#0f172a; }
  button:disabled { opacity:0.5; cursor:not-allowed; }
  .danger { border:1px solid #fecaca; color:#b91c1c; background:#fff; }

  .hint { margin-top: 10px; font-size: 12px; color:#64748b; }

  .alert { background:#fff1f2; border:1px solid #fecdd3; border-radius: 14px; padding: 12px; margin: 12px 0; }
  .alert-title { font-weight: 800; margin-bottom: 6px; }
  pre { white-space: pre-wrap; margin:0; font-size: 12px; }

  .tables-list { display:flex; gap:8px; flex-wrap:wrap; margin-top: 10px; }
  .chip { padding: 6px 10px; border-radius: 999px; border:1px solid #e2e8f0; font-size: 12px; color:#334155; background:#fff; cursor:pointer; }
  .chip.activechip { background:#0f172a; color:#fff; border-color:#0f172a; }

  .two { display:grid; grid-template-columns: 340px 1fr; gap: 12px; margin-top: 12px; }
  @media (max-width: 1100px) { .two { grid-template-columns: 1fr; } }

  .preview { margin-top: 10px; overflow:auto; border:1px solid #eef2f7; border-radius: 14px; }
  table { border-collapse: collapse; width: 100%; font-size: 12px; }
  th, td { border-bottom:1px solid #eef2f7; padding: 8px 10px; text-align:left; vertical-align:top; }
  th { position: sticky; top: 0; background:#fff; z-index: 1; }
  tfoot td { background:#fbfdff; position: sticky; bottom: 0; }

  .thwrap { display:flex; align-items:center; justify-content:space-between; gap: 8px; }
  .thname { font-weight: 700; }
  .xbtn { width: 24px; height: 24px; border-radius: 999px; padding: 0; line-height: 22px; text-align:center; border:1px solid #fecaca; color:#b91c1c; background:#fff; }
  .xbtn:disabled { opacity: .35; }

  .thadd { width: 56px; }
  .plusbtn { width: 100%; height: 28px; border-radius: 12px; padding: 0; font-weight: 800; }
  .plusbtn:disabled { opacity: .35; }

  .rowactions { width: 90px; text-align:center; }
  .trash { width: 36px; height: 28px; border-radius: 12px; padding: 0; }
  .addrow { width: 100%; border-radius: 12px; }

  .cellinput { width: 100%; padding: 8px 10px; border-radius: 12px; border:1px solid #e2e8f0; }
  .muted { color:#64748b; font-size: 12px; }

  /* Modal */
  .modal-bg { position: fixed; inset:0; background: rgba(15,23,42,.35); display:flex; align-items:center; justify-content:center; padding: 20px; z-index: 50; }
  .modal { width: min(520px, 95vw); background:#fff; border-radius: 18px; border:1px solid #e2e8f0; padding: 16px; box-shadow: 0 12px 30px rgba(0,0,0,.15); }
</style>
