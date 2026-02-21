<!-- File: core/orchestrator/modules/advertising/interface/desk/tabs/TablesAndDataTab.svelte -->
<script lang="ts">
  export type Role = 'viewer' | 'operator' | 'data_admin';
  export type ExistingTable = { schema_name: string; table_name: string };
  export type ColumnMeta = { name: string; type: string; description?: string; is_nullable?: boolean };

  export let apiBase: string;
  export let role: Role;
  export let loading: boolean;

  export let existingTables: ExistingTable[];
  export let refreshTables: () => Promise<void>;

  export let headers: () => Record<string, string>;
  export let apiJson: <T = any>(url: string, init?: RequestInit) => Promise<T>;

  let preview_schema = '';
  let preview_table = '';
  let preview_columns: ColumnMeta[] = [];
  let preview_rows: any[] = [];
  let preview_loading = false;
  let preview_error = '';

  let modal = '' as '' | 'addColumn' | 'confirmDropColumn' | 'confirmDropTable' | 'confirmDeleteRow';
  let modal_error = '';

  let new_col_name = '';
  let new_col_type = 'text';
  let new_col_desc = '';

  let pending_drop_column = '';
  let pending_delete_row_ctid = '';

  let newRow: Record<string, string> = {};

  function canWrite(): boolean {
    return role === 'data_admin';
  }

  function pickExisting(t: ExistingTable) {
    preview_schema = t.schema_name;
    preview_table = t.table_name;
    loadColumns();
    loadPreview();
  }

  async function loadColumns() {
    preview_error = '';
    try {
      if (!preview_schema || !preview_table) return;

      const j = await apiJson<{ columns: ColumnMeta[] }>(
        `${apiBase}/columns?schema=${encodeURIComponent(preview_schema)}&table=${encodeURIComponent(preview_table)}`
      );
      preview_columns = j.columns || [];
      newRow = Object.fromEntries(preview_columns.map((c) => [c.name, '']));
    } catch (e: any) {
      preview_error = e?.message ?? String(e);
    }
  }

  async function loadPreview() {
    preview_loading = true;
    preview_error = '';
    try {
      if (!preview_schema || !preview_table) return;
      const j = await apiJson<{ rows: any[] }>(
        `${apiBase}/preview?schema=${encodeURIComponent(preview_schema)}&table=${encodeURIComponent(preview_table)}&limit=5`
      );
      preview_rows = j.rows || [];
    } catch (e: any) {
      preview_error = e?.message ?? String(e);
    } finally {
      preview_loading = false;
    }
  }

  async function refreshPreviewAll() {
    await loadColumns();
    await loadPreview();
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
      if (!preview_schema || !preview_table) throw new Error('Таблица не выбрана');
      if (!new_col_name.trim()) throw new Error('Укажи имя столбца');

      await apiJson(`${apiBase}/columns/add`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({
          schema: preview_schema,
          table: preview_table,
          column: { name: new_col_name.trim(), type: new_col_type, description: new_col_desc.trim() }
        })
      });

      modal = '';
      await loadColumns();
      await loadPreview();
      await refreshTables();
    } catch (e: any) {
      modal_error = e?.message ?? String(e);
    }
  }

  function confirmDropColumn(name: string) {
    modal_error = '';
    pending_drop_column = name;
    modal = 'confirmDropColumn';
  }

  async function dropColumnNow() {
    modal_error = '';
    try {
      if (!canWrite()) throw new Error('Недостаточно прав (нужна роль data_admin)');
      if (!preview_schema || !preview_table) throw new Error('Таблица не выбрана');
      if (!pending_drop_column) throw new Error('Колонка не выбрана');

      await apiJson(`${apiBase}/columns/drop`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({ schema: preview_schema, table: preview_table, column: pending_drop_column })
      });

      modal = '';
      await loadColumns();
      await loadPreview();
      await refreshTables();
    } catch (e: any) {
      modal_error = e?.message ?? String(e);
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
      if (!preview_schema || !preview_table) throw new Error('Таблица не выбрана');

      await apiJson(`${apiBase}/tables/drop`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({ schema: preview_schema, table: preview_table })
      });

      modal = '';
      preview_columns = [];
      preview_rows = [];
      newRow = {};
      preview_schema = '';
      preview_table = '';

      await refreshTables();
    } catch (e: any) {
      modal_error = e?.message ?? String(e);
    }
  }

  async function addRowNow() {
    preview_error = '';
    try {
      if (!canWrite()) throw new Error('Недостаточно прав (нужна роль data_admin)');
      if (!preview_schema || !preview_table) throw new Error('Таблица не выбрана');

      const row: Record<string, any> = {};
      for (const c of preview_columns) row[c.name] = newRow[c.name];

      await apiJson(`${apiBase}/rows/add`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({ schema: preview_schema, table: preview_table, row })
      });

      await loadPreview();
      newRow = Object.fromEntries(preview_columns.map((c) => [c.name, '']));
    } catch (e: any) {
      preview_error = e?.message ?? String(e);
    }
  }

  function confirmDeleteRow(ctid: string) {
    modal_error = '';
    pending_delete_row_ctid = ctid;
    modal = 'confirmDeleteRow';
  }

  async function deleteRowNow() {
    modal_error = '';
    try {
      if (!canWrite()) throw new Error('Недостаточно прав (нужна роль data_admin)');
      if (!preview_schema || !preview_table) throw new Error('Таблица не выбрана');
      if (!pending_delete_row_ctid) throw new Error('CTID не задан');

      await apiJson(`${apiBase}/rows/delete`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({ schema: preview_schema, table: preview_table, ctid: pending_delete_row_ctid })
      });

      modal = '';
      await loadPreview();
    } catch (e: any) {
      modal_error = e?.message ?? String(e);
    }
  }
</script>

<section class="panel">
  <div class="panel-head">
    <h2>Таблицы и данные</h2>
  </div>

  <div class="layout">
    <aside class="aside">
      <div class="aside-head">
        <div class="aside-title">Текущие таблицы</div>
        <button class="icon-btn refresh-btn" on:click={refreshTables} disabled={loading} title="Обновить список">↻</button>
      </div>
      <div class="storage-meta">
        <span>Подключено к базе:</span>
        <span class="plain-value">{apiBase}</span>
      </div>
      <p class="hint">Создание таблиц выполняется на вкладке «Создание».</p>

      {#if existingTables.length === 0}
        <p class="hint">Таблиц нет.</p>
      {:else}
        <div class="list tables-list">
          {#each existingTables as t}
            <div class="row-item" class:activeitem={t.schema_name === preview_schema && t.table_name === preview_table}>
              <button class="item-button" on:click={() => pickExisting(t)}>
                {t.schema_name}.{t.table_name}
              </button>
            </div>
          {/each}
        </div>
      {/if}
    </aside>

    <div class="main">
      <div class="card">
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
            <button on:click={refreshPreviewAll} disabled={preview_loading || !preview_schema || !preview_table}>
              {preview_loading ? 'Загрузка…' : 'Обновить предпросмотр'}
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

        {#if preview_schema && preview_table}
          {#if preview_columns.length === 0}
            <p class="hint">Колонки не загружены. Нажми «Обновить предпросмотр».</p>
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
                          x
                        </button>
                      </div>
                    </th>
                  {/each}
                  <th class="thadd">
                    <button class="plusbtn" on:click={openAddColumnModal} disabled={!canWrite()} title="Добавить столбец">+</button>
                  </th>
                </tr>
              </thead>

              <tbody>
                {#if preview_rows.length === 0}
                  <tr>
                    <td colspan={preview_columns.length + 1} class="muted">Нет строк (таблица может быть пустой).</td>
                  </tr>
                {:else}
                  {#each preview_rows as r}
                    <tr>
                      {#each preview_columns as c}
                        <td>{typeof r[c.name] === 'object' ? JSON.stringify(r[c.name]) : String(r[c.name] ?? '')}</td>
                      {/each}
                      <td class="rowactions">
                        <button class="trash" on:click={() => confirmDeleteRow(r.__ctid)} disabled={!canWrite()} title="Удалить строку">🗑</button>
                      </td>
                    </tr>
                  {/each}
                {/if}
              </tbody>

              <tfoot>
                <tr>
                  {#each preview_columns as c}
                    <td>
                      <input class="cellinput" bind:value={newRow[c.name]} placeholder={c.type} disabled={!canWrite()} />
                    </td>
                  {/each}
                  <td class="rowactions">
                    <button class="addrow" on:click={addRowNow} disabled={!canWrite()} title="Добавить строку">Добавить</button>
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

    <aside class="aside">
      <div class="aside-head">
        <div class="aside-title">Шаблоны таблиц</div>
        <button class="icon-btn refresh-btn" on:click={refreshTables} disabled={loading} title="Обновить список">↻</button>
      </div>
      <div class="storage-meta">
        <span>Хранятся в таблице:</span>
        <span class="plain-value">ao_system.table_templates_store</span>
      </div>
      <p class="hint">Управление шаблонами выполняется на вкладке «Создание».</p>
    </aside>
  </div>
</section>

{#if modal !== ''}
  <div
    class="modal-bg"
    role="button"
    tabindex="0"
    aria-label="Закрыть модальное окно"
    on:click={() => (modal = '')}
    on:keydown={(e) => (e.key === 'Escape' || e.key === 'Enter' || e.key === ' ') && (modal = '')}
  >
    <div class="modal" on:click|stopPropagation on:keydown|stopPropagation>
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
              <option value="text">text</option>
              <option value="int">int</option>
              <option value="bigint">bigint</option>
              <option value="numeric">numeric</option>
              <option value="boolean">boolean</option>
              <option value="date">date</option>
              <option value="timestamptz">timestamptz</option>
              <option value="jsonb">jsonb</option>
              <option value="uuid">uuid</option>
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

        <div class="modal-actions">
          <button class="primary" on:click={addColumnToTable} disabled={!canWrite()}>Добавить</button>
          <button on:click={() => (modal = '')}>Отмена</button>
        </div>

      {:else if modal === 'confirmDropColumn'}
        <h3 style="margin-top:0;">Удалить столбец «{pending_drop_column}»?</h3>
        <p class="hint">Данные в столбце будут потеряны.</p>

        {#if modal_error}
          <div class="alert" style="margin-top:12px;">
            <div class="alert-title">Ошибка</div>
            <pre>{modal_error}</pre>
          </div>
        {/if}

        <div class="modal-actions">
          <button class="danger" on:click={dropColumnNow} disabled={!canWrite()}>Удалить</button>
          <button on:click={() => (modal = '')}>Отмена</button>
        </div>

      {:else if modal === 'confirmDropTable'}
        <h3 style="margin-top:0;">Удалить таблицу?</h3>
        <p class="hint">Действие необратимо.</p>

        {#if modal_error}
          <div class="alert" style="margin-top:12px;">
            <div class="alert-title">Ошибка</div>
            <pre>{modal_error}</pre>
          </div>
        {/if}

        <div class="modal-actions">
          <button class="danger" on:click={dropTableNow} disabled={!canWrite()}>Удалить</button>
          <button on:click={() => (modal = '')}>Отмена</button>
        </div>

      {:else if modal === 'confirmDeleteRow'}
        <h3 style="margin-top:0;">Удалить строку?</h3>
        <p class="hint">Удаление по CTID: {pending_delete_row_ctid}</p>

        {#if modal_error}
          <div class="alert" style="margin-top:12px;">
            <div class="alert-title">Ошибка</div>
            <pre>{modal_error}</pre>
          </div>
        {/if}

        <div class="modal-actions">
          <button class="danger" on:click={deleteRowNow} disabled={!canWrite()}>Удалить</button>
          <button on:click={() => (modal = '')}>Отмена</button>
        </div>
      {/if}
    </div>
  </div>
{/if}

<style>
  .panel { background:#fff; border:1px solid #e6eaf2; border-radius:18px; padding:14px; box-shadow:0 6px 20px rgba(15,23,42,.05); margin-top: 12px; }
  .panel-head { display:flex; align-items:center; justify-content:space-between; gap:12px; }
  .panel-head h2 { margin:0; font-size:18px; }
  .quick { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }

  .layout { display:grid; grid-template-columns: 320px 1fr 360px; gap:12px; margin-top:12px; align-items:start; }
  @media (max-width: 1300px) { .layout { grid-template-columns: 320px 1fr; } }
  @media (max-width: 1100px) { .layout { grid-template-columns: 1fr; } }

  .aside { border:1px solid #e6eaf2; border-radius:16px; padding:12px; background:#f8fafc; }
  .aside-head { display:flex; align-items:center; justify-content:space-between; gap:8px; margin-bottom:8px; }
  .aside-title { font-weight:700; font-size:14px; line-height:1.3; margin-bottom:0; }
  .list { display:flex; flex-direction:column; gap:8px; overflow:visible; max-height:none; }
  .row-item { display:grid; grid-template-columns: 1fr; gap:8px; align-items:center; border:1px solid #e6eaf2; border-radius:14px; background:#fff; padding:10px 12px; }
  .item-button { text-align:left; border:0; background:transparent; padding:0; font-weight:400; font-size:14px; line-height:1.3; color:inherit; }
  .activeitem { border-color:#0f172a; background:#0f172a; color:#fff; }
  .activeitem .item-button { color:#fff; }
  .tables-list .row-item { background:#0f172a; border-color:#0f172a; }
  .tables-list .item-button { color:#fff; }

  .main { min-width:0; }
  .card { border:1px solid #e6eaf2; border-radius:16px; padding:12px; background:#fff; }

  .storage-meta { margin-top:0; margin-bottom:8px; display:flex; align-items:center; gap:6px; font-size:12px; color:#64748b; }
  .plain-value { color:#0f172a; font-size:12px; font-weight:500; }

  .hint { margin:10px 0 0; color:#64748b; font-size:13px; }
  .muted { color:#64748b; font-size:13px; }

  .preview { margin-top: 10px; overflow:auto; border:1px solid #e6eaf2; border-radius:16px; }
  table { width:100%; border-collapse:collapse; min-width: 740px; }
  th, td { border-bottom:1px solid #eef2f7; padding:10px; vertical-align:top; text-align:left; }
  th { position: sticky; top:0; background:#fff; z-index:1; }

  .thwrap { display:flex; align-items:center; justify-content:space-between; gap:10px; }
  .thname { font-weight:700; }
  .xbtn { border-color:transparent; background:transparent; color:#b91c1c; border-radius:10px; padding:2px 8px; cursor:pointer; text-transform:lowercase; }
  .thadd { width: 1%; white-space: nowrap; }
  .plusbtn { border-radius:12px; border:1px solid #e6eaf2; padding:6px 10px; background:#fff; cursor:pointer; }

  .rowactions { width: 1%; white-space: nowrap; }
  .trash { border-color:transparent; background:transparent; color:#b91c1c; border-radius:12px; padding:6px 10px; cursor:pointer; }
  .addrow { border-radius:12px; border:1px solid #0f172a; padding:8px 10px; background:#0f172a; color:#fff; cursor:pointer; }

  .cellinput { width: 100%; border-radius: 12px; border:1px solid #e6eaf2; padding:8px 10px; }

  button { border-radius:14px; border:1px solid #e6eaf2; padding:10px 12px; background:#fff; cursor:pointer; }
  button:disabled { opacity:.6; cursor:not-allowed; }
  .icon-btn { width:44px; min-width:44px; padding:10px 0; text-transform:uppercase; border-color:transparent; background:transparent; }
  .refresh-btn { color:#16a34a; }
  .danger { border-color:#f3c0c0; color:#b91c1c; }
  .primary { background:#0f172a; color:#fff; border-color:#0f172a; }

  .alert { margin: 12px 0; padding: 10px 12px; border-radius: 14px; border: 1px solid #f3c0c0; background: #fff5f5; }
  .alert-title { font-weight: 700; margin-bottom: 6px; }
  pre { margin:0; white-space: pre-wrap; word-break: break-word; }

  .modal-bg { position: fixed; inset: 0; background: rgba(15,23,42,.35); display:flex; align-items:center; justify-content:center; padding: 18px; }
  .modal { width: min(560px, 100%); background:#fff; border-radius:18px; border:1px solid #e6eaf2; padding:14px; box-shadow:0 20px 60px rgba(15,23,42,.25); }
  .modal-actions { display:flex; gap:10px; justify-content:flex-end; margin-top: 12px; }

  .form { display:grid; grid-template-columns: 1fr 1fr; gap:10px; margin-top:12px; }
  @media (max-width: 900px) { .form { grid-template-columns: 1fr; } }
  .form label { display:flex; flex-direction:column; gap:6px; font-size:13px; }
  .form input, .form select { border-radius:14px; border:1px solid #e6eaf2; padding:10px 12px; }
</style>
