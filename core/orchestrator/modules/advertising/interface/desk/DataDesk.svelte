/* File: core/orchestrator/modules/advertising/interface/desk/DataDesk.svelte */
<script lang="ts">
  import { onMount } from 'svelte';

  type Role = 'viewer' | 'operator' | 'data_admin';
  type ExistingTable = { schema_name: string; table_name: string };
  type ColumnMeta = { name: string; type: string; description?: string; is_nullable?: boolean };

  const API_BASE = '/ai-orchestrator/api';

  // role (client-side). Server checks header X-AO-ROLE.
  let role: Role = 'data_admin';

  type Tab = 'constructor' | 'tables';
  let tab: Tab = 'constructor';

  // Constructor form state
  let schema_name = 'bronze';
  let table_name = 'wb_ads_raw1';
  let table_class = 'bronze_raw';
  let description = 'Сырые ответы API (append-only JSON)';

  type ColumnForm = { field_name: string; field_type: string; description: string };
  const typeOptions = [
    'text',
    'int',
    'bigint',
    'float',
    'double',
    'numeric',
    'bool',
    'timestamp',
    'date',
    'json',
    'jsonb',
    'uuid'
  ];

  let columns: ColumnForm[] = [
    { field_name: 'dataset', field_type: 'text', description: 'dataset name' },
    { field_name: 'endpoint', field_type: 'text', description: 'endpoint name' }
  ];

  // Partitioning
  let partition_enabled = false;
  let partition_column = '';
  let partition_interval = 'day';

  // Existing tables / data tab
  let existingTables: ExistingTable[] = [];
  let loading = false;
  let errorMsg = '';
  let okMsg = '';

  // Tables & Data tab state
  let selectedTable: ExistingTable | null = null;
  let selectedTableColumns: ColumnMeta[] = [];
  let tableData: Record<string, unknown>[] = [];
  let dataLoading = false;
  let dataError = '';

  // Data pagination / query
  let limit = 50;
  let offset = 0;
  let order_by = '';
  let order_dir: 'asc' | 'desc' = 'desc';
  let where_json = '';

  // Inline editing state
  let editMode = false;
  let editedRowIndex: number | null = null;
  let editedRow: Record<string, unknown> = {};

  // Modals
  type Modal = '' | 'confirm_delete_table' | 'confirm_delete_row' | 'confirm_delete_column' | 'add_column';
  let modal: Modal = '';
  let modalPayload: any = null;

  // New column modal fields
  let newColumnName = '';
  let newColumnType = 'text';
  let newColumnDesc = '';
  let newColumnNullable = true;

  function canWrite(): boolean {
    return role === 'data_admin';
  }

  function resetMessages(): void {
    errorMsg = '';
    okMsg = '';
    dataError = '';
  }

  function setError(msg: string): void {
    errorMsg = msg;
    okMsg = '';
  }

  function setOk(msg: string): void {
    okMsg = msg;
    errorMsg = '';
  }

  function addColumnForm(): void {
    columns = [...columns, { field_name: '', field_type: 'text', description: '' }];
  }

  function removeColumnForm(ix: number): void {
    columns = columns.filter((_, i) => i !== ix);
  }

  function pickTemplateBronze(): void {
    schema_name = 'bronze';
    table_name = 'wb_ads_raw1';
    table_class = 'bronze_raw';
    description = 'Сырые ответы API (append-only JSON)';
    columns = [
      { field_name: 'dataset', field_type: 'text', description: 'dataset name' },
      { field_name: 'endpoint', field_type: 'text', description: 'endpoint name' },
      { field_name: 'request_id', field_type: 'text', description: 'request id' },
      { field_name: 'ingested_at', field_type: 'timestamp', description: 'ingest timestamp' },
      { field_name: 'payload', field_type: 'jsonb', description: 'raw payload' }
    ];
    partition_enabled = true;
    partition_column = 'ingested_at';
    partition_interval = 'day';
  }

  function headers(): Record<string, string> {
    return {
      'Content-Type': 'application/json',
      'X-AO-ROLE': role
    };
  }

  async function api<T>(path: string, init?: RequestInit): Promise<T> {
    const res = await fetch(`${API_BASE}${path}`, {
      ...init,
      headers: { ...headers(), ...(init?.headers ?? {}) }
    });

    if (!res.ok) {
      const text = await res.text().catch(() => '');
      throw new Error(text || `HTTP ${res.status}`);
    }

    const contentType = res.headers.get('content-type') || '';
    if (contentType.includes('application/json')) return (await res.json()) as T;
    return (await res.text()) as unknown as T;
  }

  async function refreshTables(): Promise<void> {
    resetMessages();
    loading = true;
    try {
      const list = await api<ExistingTable[]>('/data/tables', { method: 'GET' });
      existingTables = list ?? [];
      setOk('Список таблиц обновлён.');
    } catch (e: any) {
      setError(`Не удалось загрузить таблицы: ${e?.message ?? String(e)}`);
    } finally {
      loading = false;
    }
  }

  function validateCreateForm(): string | null {
    if (!schema_name.trim()) return 'Укажи схему (schema).';
    if (!table_name.trim()) return 'Укажи имя таблицы.';
    if (!table_class.trim()) return 'Укажи класс.';
    if (columns.length === 0) return 'Добавь хотя бы одно поле.';
    for (const [i, c] of columns.entries()) {
      if (!c.field_name.trim()) return `Поле #${i + 1}: имя не заполнено.`;
      if (!c.field_type.trim()) return `Поле #${i + 1}: тип не заполнен.`;
    }
    if (partition_enabled) {
      if (!partition_column.trim()) return 'Партиции включены: укажи колонку для партиционирования.';
      if (!partition_interval.trim()) return 'Партиции включены: укажи интервал.';
    }
    return null;
  }

  function buildCreatePayload(): any {
    return {
      schema_name: schema_name.trim(),
      table_name: table_name.trim(),
      table_class: table_class.trim(),
      description: description?.trim() ?? '',
      columns: columns.map((c) => ({
        name: c.field_name.trim(),
        type: c.field_type.trim(),
        description: c.description?.trim() ?? ''
      })),
      partitioning: partition_enabled
        ? { enabled: true, column: partition_column.trim(), interval: partition_interval }
        : { enabled: false }
    };
  }

  async function createTable(): Promise<void> {
    resetMessages();
    if (!canWrite()) {
      setError('Недостаточно прав. Нужна роль data_admin.');
      return;
    }
    const err = validateCreateForm();
    if (err) {
      setError(err);
      return;
    }

    loading = true;
    try {
      await api('/data/table', {
        method: 'POST',
        body: JSON.stringify(buildCreatePayload())
      });
      setOk(`Таблица ${schema_name}.${table_name} создана.`);
      await refreshTables();
    } catch (e: any) {
      setError(`Ошибка создания: ${e?.message ?? String(e)}`);
    } finally {
      loading = false;
    }
  }

  function gotoTablesTab(): void {
    tab = 'tables';
  }

  async function pickExisting(t: ExistingTable): Promise<void> {
    selectedTable = t;
    tab = 'tables';
    offset = 0;
    await loadSelectedTableMetaAndData();
  }

  function selectedQualifiedName(): string {
    if (!selectedTable) return '';
    return `${selectedTable.schema_name}.${selectedTable.table_name}`;
  }

  async function loadSelectedTableMetaAndData(): Promise<void> {
    if (!selectedTable) return;
    resetMessages();
    dataLoading = true;
    try {
      const cols = await api<ColumnMeta[]>(
        `/data/table/columns?schema_name=${encodeURIComponent(selectedTable.schema_name)}&table_name=${encodeURIComponent(
          selectedTable.table_name
        )}`,
        { method: 'GET' }
      );
      selectedTableColumns = cols ?? [];

      await loadData();
    } catch (e: any) {
      dataError = `Ошибка загрузки метаданных: ${e?.message ?? String(e)}`;
    } finally {
      dataLoading = false;
    }
  }

  async function loadData(): Promise<void> {
    if (!selectedTable) return;
    dataError = '';
    dataLoading = true;
    try {
      const params = new URLSearchParams();
      params.set('schema_name', selectedTable.schema_name);
      params.set('table_name', selectedTable.table_name);
      params.set('limit', String(limit));
      params.set('offset', String(offset));
      if (order_by.trim()) params.set('order_by', order_by.trim());
      if (order_dir) params.set('order_dir', order_dir);
      if (where_json.trim()) params.set('where_json', where_json.trim());

      const rows = await api<Record<string, unknown>[]>(`/data/table/rows?${params.toString()}`, { method: 'GET' });
      tableData = rows ?? [];
      setOk('Данные загружены.');
    } catch (e: any) {
      dataError = `Ошибка загрузки данных: ${e?.message ?? String(e)}`;
    } finally {
      dataLoading = false;
    }
  }

  function canEditData(): boolean {
    return canWrite();
  }

  function startEditRow(rowIndex: number): void {
    if (!canEditData()) return;
    editMode = true;
    editedRowIndex = rowIndex;
    editedRow = { ...(tableData[rowIndex] ?? {}) };
  }

  function cancelEditRow(): void {
    editMode = false;
    editedRowIndex = null;
    editedRow = {};
  }

  async function saveEditRow(): Promise<void> {
    if (!selectedTable) return;
    if (!canEditData()) return;
    if (editedRowIndex === null) return;

    resetMessages();
    dataLoading = true;
    try {
      await api('/data/table/row', {
        method: 'PUT',
        body: JSON.stringify({
          schema_name: selectedTable.schema_name,
          table_name: selectedTable.table_name,
          row_index: editedRowIndex,
          row: editedRow
        })
      });
      setOk('Строка сохранена.');
      await loadData();
      cancelEditRow();
    } catch (e: any) {
      dataError = `Ошибка сохранения: ${e?.message ?? String(e)}`;
    } finally {
      dataLoading = false;
    }
  }

  function openDeleteRow(rowIndex: number): void {
    if (!selectedTable) return;
    if (!canEditData()) return;
    modal = 'confirm_delete_row';
    modalPayload = { rowIndex };
  }

  async function confirmDeleteRow(): Promise<void> {
    if (!selectedTable) return;
    if (!canEditData()) return;

    const rowIndex = modalPayload?.rowIndex as number;
    modal = '';
    modalPayload = null;

    resetMessages();
    dataLoading = true;
    try {
      await api('/data/table/row', {
        method: 'DELETE',
        body: JSON.stringify({
          schema_name: selectedTable.schema_name,
          table_name: selectedTable.table_name,
          row_index: rowIndex
        })
      });
      setOk('Строка удалена.');
      await loadData();
    } catch (e: any) {
      dataError = `Ошибка удаления строки: ${e?.message ?? String(e)}`;
    } finally {
      dataLoading = false;
    }
  }

  function openDeleteTable(): void {
    if (!selectedTable) return;
    if (!canWrite()) return;
    modal = 'confirm_delete_table';
    modalPayload = { ...selectedTable };
  }

  async function confirmDeleteTable(): Promise<void> {
    if (!canWrite()) return;
    const payload = modalPayload as ExistingTable;
    modal = '';
    modalPayload = null;

    resetMessages();
    loading = true;
    try {
      await api('/data/table', {
        method: 'DELETE',
        body: JSON.stringify({
          schema_name: payload.schema_name,
          table_name: payload.table_name
        })
      });
      setOk(`Таблица ${payload.schema_name}.${payload.table_name} удалена.`);
      if (selectedTable?.schema_name === payload.schema_name && selectedTable?.table_name === payload.table_name) {
        selectedTable = null;
        selectedTableColumns = [];
        tableData = [];
      }
      await refreshTables();
    } catch (e: any) {
      setError(`Ошибка удаления таблицы: ${e?.message ?? String(e)}`);
    } finally {
      loading = false;
    }
  }

  function openDeleteColumn(col: ColumnMeta): void {
    if (!selectedTable) return;
    if (!canWrite()) return;
    modal = 'confirm_delete_column';
    modalPayload = { col };
  }

  async function confirmDeleteColumn(): Promise<void> {
    if (!selectedTable) return;
    if (!canWrite()) return;

    const col = modalPayload?.col as ColumnMeta;
    modal = '';
    modalPayload = null;

    resetMessages();
    dataLoading = true;
    try {
      await api('/data/table/column', {
        method: 'DELETE',
        body: JSON.stringify({
          schema_name: selectedTable.schema_name,
          table_name: selectedTable.table_name,
          column_name: col.name
        })
      });
      setOk(`Колонка ${col.name} удалена.`);
      await loadSelectedTableMetaAndData();
    } catch (e: any) {
      dataError = `Ошибка удаления колонки: ${e?.message ?? String(e)}`;
    } finally {
      dataLoading = false;
    }
  }

  function openAddColumn(): void {
    if (!selectedTable) return;
    if (!canWrite()) return;
    newColumnName = '';
    newColumnType = 'text';
    newColumnDesc = '';
    newColumnNullable = true;
    modal = 'add_column';
    modalPayload = {};
  }

  async function confirmAddColumn(): Promise<void> {
    if (!selectedTable) return;
    if (!canWrite()) return;

    if (!newColumnName.trim()) {
      dataError = 'Укажи имя новой колонки.';
      return;
    }

    modal = '';
    modalPayload = null;

    resetMessages();
    dataLoading = true;
    try {
      await api('/data/table/column', {
        method: 'POST',
        body: JSON.stringify({
          schema_name: selectedTable.schema_name,
          table_name: selectedTable.table_name,
          column: {
            name: newColumnName.trim(),
            type: newColumnType.trim(),
            description: newColumnDesc.trim(),
            is_nullable: !!newColumnNullable
          }
        })
      });
      setOk(`Колонка ${newColumnName} добавлена.`);
      await loadSelectedTableMetaAndData();
    } catch (e: any) {
      dataError = `Ошибка добавления колонки: ${e?.message ?? String(e)}`;
    } finally {
      dataLoading = false;
    }
  }

  function createDisabled(): boolean {
    return loading || !canWrite();
  }

  function onRoleChange(e: Event): void {
    const v = (e.target as HTMLSelectElement).value as Role;
    role = v;
    resetMessages();
  }

  onMount(async () => {
    await refreshTables();
  });
</script>

<div class="page">
  <div class="topbar">
    <div class="tabs">
      <button class:active={tab === 'constructor'} on:click={() => (tab = 'constructor')}>Создание</button>
      <button class:active={tab === 'tables'} on:click={() => (tab = 'tables')}>Таблицы и данные</button>
    </div>

    <div class="role">
      <span>Роль:</span>
      <select value={role} on:change={onRoleChange}>
        <option value="viewer">viewer</option>
        <option value="operator">operator</option>
        <option value="data_admin">data_admin</option>
      </select>
    </div>
  </div>

  {#if errorMsg}
    <div class="msg error">{errorMsg}</div>
  {/if}
  {#if okMsg}
    <div class="msg ok">{okMsg}</div>
  {/if}

  {#if tab === 'constructor'}
    <section class="grid" class:single={tab === 'constructor'}>
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
                  <option value="week">week</option>
                  <option value="month">month</option>
                </select>
              </label>
            </div>
          {/if}
        </div>

        <div class="actions">
          <button class="primary" on:click={createTable} disabled={createDisabled()}>
            Создать таблицу
          </button>
        </div>

        {#if !canWrite()}
          <p class="hint">Кнопка активна только при роли <b>data_admin</b>.</p>
        {/if}
      </div>
    </section>
  {/if}

  {#if tab === 'tables'}
    <section class="tables">
      <div class="left">
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
                <button
                  class="chip"
                  class:active={selectedTable?.schema_name === t.schema_name && selectedTable?.table_name === t.table_name}
                  on:click={() => pickExisting(t)}
                  title="Открыть"
                >
                  {t.schema_name}.{t.table_name}
                </button>
              {/each}
            </div>
            <p class="hint">Нажми на таблицу, чтобы открыть данные.</p>
          {/if}
        </div>

        <div class="panel">
          <div class="panel-head">
            <h2>Запрос</h2>
          </div>

          <div class="form">
            <label>
              limit
              <input type="number" min="1" max="1000" bind:value={limit} />
            </label>
            <label>
              offset
              <input type="number" min="0" bind:value={offset} />
            </label>
            <label>
              order_by
              <input bind:value={order_by} placeholder="например: ingested_at" />
            </label>
            <label>
              order_dir
              <select bind:value={order_dir}>
                <option value="desc">desc</option>
                <option value="asc">asc</option>
              </select>
            </label>
            <label class="wide">
              where_json (JSON фильтр)
              <textarea bind:value={where_json} placeholder='например: {"dataset":"ads"}'></textarea>
            </label>
          </div>

          <div class="actions">
            <button class="primary" on:click={loadData} disabled={!selectedTable || dataLoading}>
              {dataLoading ? 'Загрузка…' : 'Загрузить'}
            </button>
            <button on:click={() => ((offset = Math.max(0, offset - limit)), loadData())} disabled={!selectedTable || dataLoading || offset === 0}>
              ← Назад
            </button>
            <button on:click={() => ((offset = offset + limit), loadData())} disabled={!selectedTable || dataLoading}>
              Вперёд →
            </button>
          </div>
        </div>
      </div>

      <div class="right">
        <div class="panel">
          <div class="panel-head">
            <h2>Таблица</h2>
            <div class="quick">
              {#if selectedTable}
                <span class="muted">{selectedQualifiedName()}</span>
              {/if}
            </div>
          </div>

          {#if dataError}
            <div class="msg error">{dataError}</div>
          {/if}

          {#if !selectedTable}
            <p class="hint">Выбери таблицу слева.</p>
          {:else}
            <div class="toolbar">
              <button on:click={loadSelectedTableMetaAndData} disabled={dataLoading}>
                {dataLoading ? 'Загрузка…' : 'Обновить'}
              </button>

              <button on:click={openAddColumn} disabled={!canWrite()}>+ Колонка</button>
              <button class="danger" on:click={openDeleteTable} disabled={!canWrite()}>Удалить таблицу</button>
            </div>

            <div class="cols">
              <h3>Колонки</h3>
              {#if selectedTableColumns.length === 0}
                <p class="hint">Колонки не найдены.</p>
              {:else}
                <div class="cols-list">
                  {#each selectedTableColumns as c}
                    <div class="col-item">
                      <div class="col-main">
                        <b>{c.name}</b>
                        <span class="muted">{c.type}</span>
                        {#if c.is_nullable === false}
                          <span class="badge">NOT NULL</span>
                        {/if}
                      </div>
                      <div class="col-actions">
                        <button class="danger" on:click={() => openDeleteColumn(c)} disabled={!canWrite()}>
                          Удалить
                        </button>
                      </div>
                      {#if c.description}
                        <div class="col-desc">{c.description}</div>
                      {/if}
                    </div>
                  {/each}
                </div>
              {/if}
            </div>

            <div class="data">
              <h3>Данные</h3>

              {#if dataLoading}
                <p>Загрузка…</p>
              {:else if tableData.length === 0}
                <p class="hint">Нет данных (или фильтр ничего не вернул).</p>
              {:else}
                <div class="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        {#each Object.keys(tableData[0] ?? {}) as k}
                          <th>{k}</th>
                        {/each}
                        <th class="actions-col">Действия</th>
                      </tr>
                    </thead>

                    <tbody>
                      {#each tableData as row, ri}
                        <tr class:editing={editMode && editedRowIndex === ri}>
                          {#each Object.keys(tableData[0] ?? {}) as k}
                            <td>
                              {#if editMode && editedRowIndex === ri}
                                <input
                                  value={String((editedRow as any)[k] ?? '')}
                                  on:input={(e) => ((editedRow as any)[k] = (e.target as HTMLInputElement).value)}
                                />
                              {:else}
                                <span class="cell">{String((row as any)[k] ?? '')}</span>
                              {/if}
                            </td>
                          {/each}

                          <td class="actions-col">
                            {#if editMode && editedRowIndex === ri}
                              <button class="primary" on:click={saveEditRow} disabled={dataLoading}>Сохранить</button>
                              <button on:click={cancelEditRow} disabled={dataLoading}>Отмена</button>
                            {:else}
                              <button on:click={() => startEditRow(ri)} disabled={!canEditData()}>Редактировать</button>
                              <button class="danger" on:click={() => openDeleteRow(ri)} disabled={!canEditData()}>
                                Удалить
                              </button>
                            {/if}
                          </td>
                        </tr>
                      {/each}
                    </tbody>
                  </table>
                </div>
              {/if}
            </div>
          {/if}
        </div>
      </div>
    </section>
  {/if}

  <!-- MODALS -->
  {#if modal !== ''}
    <div class="modal-bg" on:click={() => (modal = '')}>
      <div class="modal" on:click|stopPropagation>
        {#if modal === 'confirm_delete_table'}
          <h3>Удалить таблицу?</h3>
          <p class="hint">Действие необратимо.</p>
          <div class="modal-actions">
            <button class="danger" on:click={confirmDeleteTable}>Удалить</button>
            <button on:click={() => (modal = '')}>Отмена</button>
          </div>
        {:else if modal === 'confirm_delete_row'}
          <h3>Удалить строку?</h3>
          <p class="hint">Действие необратимо.</p>
          <div class="modal-actions">
            <button class="danger" on:click={confirmDeleteRow}>Удалить</button>
            <button on:click={() => (modal = '')}>Отмена</button>
          </div>
        {:else if modal === 'confirm_delete_column'}
          <h3>Удалить колонку?</h3>
          <p class="hint">Данные в колонке будут потеряны.</p>
          <div class="modal-actions">
            <button class="danger" on:click={confirmDeleteColumn}>Удалить</button>
            <button on:click={() => (modal = '')}>Отмена</button>
          </div>
        {:else if modal === 'add_column'}
          <h3>Добавить колонку</h3>

          <div class="form">
            <label>
              Имя
              <input bind:value={newColumnName} placeholder="например: campaign_id" />
            </label>

            <label>
              Тип
              <select bind:value={newColumnType}>
                {#each typeOptions as t}
                  <option value={t}>{t}</option>
                {/each}
              </select>
            </label>

            <label>
              Описание
              <input bind:value={newColumnDesc} placeholder="опционально" />
            </label>

            <label class="row">
              <input type="checkbox" bind:checked={newColumnNullable} />
              <span>Nullable</span>
            </label>
          </div>

          <div class="modal-actions">
            <button class="primary" on:click={confirmAddColumn}>Добавить</button>
            <button on:click={() => (modal = '')}>Отмена</button>
          </div>
        {/if}
      </div>
    </div>
  {/if}
</div>

<style>
  .page { padding: 14px; }

  .topbar { display:flex; align-items:center; justify-content:space-between; gap: 12px; }
  .tabs { display:flex; gap: 8px; }
  .tabs button { padding: 8px 12px; border-radius: 12px; border: 1px solid #e6eaf2; background: #fff; cursor: pointer; }
  .tabs button.active { background: #0f172a; color: #fff; border-color: #0f172a; }

  .role { display:flex; align-items:center; gap: 8px; }
  .role select { border-radius: 12px; border: 1px solid #e6eaf2; padding: 8px 10px; background: #fff; }

  .msg { margin-top: 12px; padding: 10px 12px; border-radius: 14px; border: 1px solid #e6eaf2; background: #fff; }
  .msg.error { border-color: #f3c0c0; background: #fff5f5; }
  .msg.ok { border-color: #b8e7c2; background: #f2fff5; }

  .grid { display:grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-top: 12px; }
  .grid.single { grid-template-columns: 1fr; }
  @media (max-width: 1100px) { .grid { grid-template-columns: 1fr; } }

  .panel { background: #fff; border: 1px solid #e6eaf2; border-radius: 18px; padding: 14px; box-shadow: 0 6px 20px rgba(15, 23, 42, .05); }
  .panel-head { display:flex; align-items:center; justify-content:space-between; gap: 12px; }
  .panel-head h2 { margin: 0; font-size: 18px; }
  .quick { display:flex; gap: 8px; align-items:center; flex-wrap: wrap; }
  .quick .muted { color: #64748b; font-size: 13px; }

  .form { display:grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-top: 12px; }
  .form label { display:flex; flex-direction:column; gap: 6px; font-size: 13px; color: #0f172a; }
  .form input, .form select, .form textarea {
    border-radius: 14px; border: 1px solid #e6eaf2; padding: 10px 12px; background: #fff; outline: none;
  }
  .form textarea { min-height: 80px; resize: vertical; }
  .form .wide { grid-column: 1 / -1; }

  .fields { margin-top: 14px; border-top: 1px dashed #e6eaf2; padding-top: 14px; }
  .fields-head { display:flex; align-items:center; justify-content:space-between; }
  .fields-head h3 { margin: 0; font-size: 16px; }
  .field-row { display:grid; grid-template-columns: 1.2fr 0.7fr 1.6fr auto; gap: 8px; margin-top: 10px; }
  .field-row input, .field-row select { border-radius: 14px; border: 1px solid #e6eaf2; padding: 10px 12px; }
  .fields-footer { margin-top: 12px; display:flex; justify-content:flex-start; }

  .panel2 { margin-top: 14px; border-top: 1px dashed #e6eaf2; padding-top: 14px; }
  .panel2 h3 { margin: 0 0 10px 0; font-size: 16px; }
  .row { display:flex; align-items:center; gap: 10px; }

  .actions { margin-top: 14px; display:flex; gap: 10px; align-items:center; }

  button { border-radius: 14px; border: 1px solid #e6eaf2; padding: 10px 12px; background: #fff; cursor: pointer; }
  button:hover { border-color: #cfd7e6; }
  button:disabled { opacity: 0.6; cursor: not-allowed; }
  .primary { background: #0f172a; color: #fff; border-color: #0f172a; }
  .danger { background: #fff; border-color: #f3c0c0; color: #b91c1c; }

  .hint { margin: 10px 0 0 0; color: #64748b; font-size: 13px; }

  /* Tables & Data */
  .tables { display:grid; grid-template-columns: 360px 1fr; gap: 12px; margin-top: 12px; }
  @media (max-width: 1100px) { .tables { grid-template-columns: 1fr; } }

  .left, .right { display:flex; flex-direction:column; gap: 12px; }

  .tables-list { display:flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }
  .chip { padding: 8px 10px; border-radius: 999px; }
  .chip.active { background: #0f172a; color: #fff; border-color: #0f172a; }

  .toolbar { display:flex; gap: 8px; align-items:center; flex-wrap: wrap; margin-top: 10px; }

  .cols { margin-top: 16px; border-top: 1px dashed #e6eaf2; padding-top: 14px; }
  .cols h3 { margin: 0 0 10px 0; font-size: 16px; }
  .cols-list { display:flex; flex-direction:column; gap: 8px; }
  .col-item { border: 1px solid #e6eaf2; border-radius: 16px; padding: 10px 12px; }
  .col-main { display:flex; align-items:center; gap: 8px; flex-wrap: wrap; }
  .badge { font-size: 12px; border: 1px solid #e6eaf2; padding: 2px 8px; border-radius: 999px; color: #0f172a; background: #f8fafc; }
  .col-actions { margin-left: auto; }
  .col-desc { margin-top: 6px; color: #64748b; font-size: 13px; }

  .data { margin-top: 16px; border-top: 1px dashed #e6eaf2; padding-top: 14px; }
  .data h3 { margin: 0 0 10px 0; font-size: 16px; }

  .table-wrap { overflow:auto; border: 1px solid #e6eaf2; border-radius: 16px; }
  table { width: 100%; border-collapse: collapse; min-width: 760px; }
  th, td { border-bottom: 1px solid #eef2f7; padding: 10px 10px; text-align:left; vertical-align: top; }
  th { position: sticky; top: 0; background: #fff; z-index: 1; }
  .actions-col { white-space: nowrap; }
  .cell { display:block; max-width: 420px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  tr.editing { background: #f8fafc; }

  /* Modal */
  .modal-bg { position: fixed; inset: 0; background: rgba(15,23,42,.35); display:flex; align-items:center; justify-content:center; padding: 18px; }
  .modal { width: min(560px, 100%); background: #fff; border-radius: 18px; border: 1px solid #e6eaf2; padding: 14px; box-shadow: 0 20px 60px rgba(15, 23, 42, .25); }
  .modal h3 { margin: 0 0 8px 0; }
  .modal-actions { display:flex; gap: 10px; justify-content:flex-end; margin-top: 12px; }
</style>
