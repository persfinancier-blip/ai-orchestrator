<!-- File: core/orchestrator/modules/advertising/interface/desk/tabs/TablesAndDataTab.svelte -->
<script lang="ts">
  import { onMount, tick } from 'svelte';

  export type Role = 'viewer' | 'operator' | 'data_admin';
  export type ExistingTable = { schema_name: string; table_name: string };
  export type ColumnMeta = { name: string; type: string; description?: string; is_nullable?: boolean };
  export type ContractVersion = {
    id?: string;
    __ctid?: string;
    schema_name: string;
    table_name: string;
    contract_name: string;
    version: number;
    lifecycle_state: string;
    deleted_at?: string | null;
    description?: string;
    created_at?: string;
    columns?: Array<{ field_name: string; field_type: string; description?: string }>;
    change_reason?: string;
    changed_by?: string;
  };
  type ContractColumn = { field_name: string; field_type: string; description: string };

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

  let modal = '' as '' | 'addColumn' | 'confirmDropColumn' | 'confirmDeleteRow' | 'confirmApplyContractVersion';
  let modal_error = '';

  let new_col_name = '';
  let new_col_type = 'text';
  let new_col_desc = '';

  let pending_drop_column = '';
  let pending_delete_row_ctid = '';

  let newRow: Record<string, string> = {};
  let edit_schema_name = '';
  let edit_table_name = '';
  let table_description = '';
  let tableDescriptionEl: HTMLTextAreaElement | null = null;
  let columnDescriptionDrafts: Record<string, string> = {};
  let contracts_loading = false;
  let contracts_error = '';
  let contractVersions: ContractVersion[] = [];
  let selectedContractId = '';
  let compare_versions_open = false;
  let compare_top_id = '';
  let compare_bottom_id = '';
  let compareTop: ContractVersion | null = null;
  let compareBottom: ContractVersion | null = null;
  let compareRows: Array<{ key: string; top: ContractColumn | null; bottom: ContractColumn | null }> = [];
  let applyTargetContract: ContractVersion | null = null;
  let applyPlan: { toAdd: ContractColumn[]; toDrop: Array<{ field_name: string }>; toDescribe: ContractColumn[] } = {
    toAdd: [],
    toDrop: [],
    toDescribe: []
  };
  let contracts_storage_schema = 'ao_system';
  let contracts_storage_table = 'table_data_contract_versions';
  let contracts_storage_picker_open = false;
  let contracts_storage_pick_value = '';
  const SETTINGS_SYSTEM_TABLE = 'ao_system.table_settings_store';
  const SERVER_WRITES_SYSTEM_TABLE = 'ao_system.table_server_writes_store';

  const CONTRACTS_REQUIRED_COLUMNS = [
    { name: 'schema_name', types: ['text', 'character varying', 'varchar'] },
    { name: 'table_name', types: ['text', 'character varying', 'varchar'] },
    { name: 'contract_name', types: ['text', 'character varying', 'varchar'] },
    { name: 'version', types: ['integer', 'int', 'bigint'] },
    { name: 'lifecycle_state', types: ['text', 'character varying', 'varchar'] },
    { name: 'deleted_at', types: ['timestamp with time zone', 'timestamptz', 'timestamp'] },
    { name: 'description', types: ['text', 'character varying', 'varchar'] },
    { name: 'columns', types: ['jsonb', 'json'] },
    { name: 'change_reason', types: ['text', 'character varying', 'varchar'] },
    { name: 'changed_by', types: ['text', 'character varying', 'varchar'] },
    { name: 'created_at', types: ['timestamp with time zone', 'timestamptz', 'timestamp'] }
  ];

  function canWrite(): boolean {
    return role === 'data_admin';
  }

  function isSystemTable(schema: string, table: string): boolean {
    const qn = `${String(schema || '').trim()}.${String(table || '').trim()}`;
    return qn === SETTINGS_SYSTEM_TABLE || qn === SERVER_WRITES_SYSTEM_TABLE;
  }

  function canEditSelectedTable(): boolean {
    return canWrite();
  }

  async function parseContractsStorageConfig() {
    contracts_storage_schema = 'ao_system';
    contracts_storage_table = 'table_data_contract_versions';
    try {
      const j = await apiJson<{ effective?: any }>(`${apiBase}/settings/effective`);
      const eff = j?.effective || {};
      const nextSchema = String(eff?.contracts_schema || '').trim();
      const nextTable = String(eff?.contracts_table || '').trim();
      if (nextSchema) contracts_storage_schema = nextSchema;
      if (nextTable) contracts_storage_table = nextTable;
    } catch {
      // ignore and keep defaults
    }
    contracts_storage_pick_value = `${contracts_storage_schema}.${contracts_storage_table}`;
  }

  function normalizeTypeName(type: string) {
    return String(type || '').toLowerCase().trim();
  }

  async function checkContractsStorageTable(schema: string, table: string) {
    try {
      const j = await apiJson<{ columns: Array<{ name: string; type: string }> }>(
        `${apiBase}/columns?schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`
      );
      const cols = Array.isArray(j?.columns) ? j.columns : [];
      if (!cols.length) {
        contracts_error = `Таблица ${schema}.${table} не найдена или пуста. Выбери таблицу хранилища контрактов.`;
        return false;
      }
      const map = new Map(cols.map((c) => [String(c.name || '').toLowerCase(), normalizeTypeName(c.type)]));
      for (const need of CONTRACTS_REQUIRED_COLUMNS) {
        const actual = map.get(need.name);
        if (!actual || !need.types.some((t) => actual.includes(t))) {
          contracts_error = `Структура ${schema}.${table} не подходит: колонка ${need.name} отсутствует или имеет неверный тип.`;
          return false;
        }
      }
      return true;
    } catch (e: any) {
      contracts_error = e?.message ?? String(e);
      return false;
    }
  }

  function pickExisting(t: ExistingTable) {
    preview_schema = t.schema_name;
    preview_table = t.table_name;
    edit_schema_name = t.schema_name;
    edit_table_name = t.table_name;
    loadColumns();
    loadPreview();
    loadTableMeta();
    loadContractsPanel();
  }

  async function loadTableMeta() {
    preview_error = '';
    try {
      if (!preview_schema || !preview_table) return;
      const j = await apiJson<{ description?: string }>(
        `${apiBase}/tables/meta?schema=${encodeURIComponent(preview_schema)}&table=${encodeURIComponent(preview_table)}`
      );
      table_description = String(j?.description || '');
      await tick();
      syncTableDescriptionHeight();
    } catch (e: any) {
      preview_error = e?.message ?? String(e);
    }
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
      columnDescriptionDrafts = Object.fromEntries(
        preview_columns.map((c) => [c.name, String(c.description || '')])
      );
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
    await loadTableMeta();
  }

  async function loadContractsPanel() {
    contracts_loading = true;
    contracts_error = '';
    try {
      if (!preview_schema || !preview_table) {
        contractVersions = [];
        return;
      }
      const ok = await checkContractsStorageTable(contracts_storage_schema, contracts_storage_table);
      if (!ok) {
        contractVersions = [];
        return;
      }
      const j = await apiJson<{ contracts: any[] }>(
        `${apiBase}/contracts?schema=${encodeURIComponent(preview_schema)}&table=${encodeURIComponent(preview_table)}`,
        { headers: headers() }
      );
      const rows = Array.isArray(j?.contracts) ? j.contracts : [];
      contractVersions = rows
        .filter((r: any) =>
          String(r?.schema_name || '').trim() === preview_schema &&
          String(r?.table_name || '').trim() === preview_table &&
          String(r?.lifecycle_state || '').trim() !== 'deleted_by_user'
        )
        .map((r: any, idx: number) => ({
          id: String(`${preview_schema}.${preview_table}.v${Number(r?.version || 0)}.${String(r?.created_at || '')}.${idx}`),
          __ctid: '',
          schema_name: String(r?.schema_name || ''),
          table_name: String(r?.table_name || ''),
          contract_name: String(r?.contract_name || ''),
          version: Number(r?.version || 0),
          lifecycle_state: String(r?.lifecycle_state || ''),
          deleted_at: r?.deleted_at || null,
          description: String(r?.description || ''),
          created_at: String(r?.created_at || ''),
          columns: normalizeContractColumns(r?.columns),
          change_reason: String(r?.change_reason || ''),
          changed_by: String(r?.changed_by || '')
        }))
        .sort((a, b) => b.version - a.version);
      if (selectedContractId && !contractVersions.some((x) => String(x.id || '') === selectedContractId)) {
        selectedContractId = '';
      }
      if (compare_top_id && !contractVersions.some((x) => String(x.id || '') === compare_top_id)) {
        compare_top_id = '';
      }
      if (compare_bottom_id && !contractVersions.some((x) => String(x.id || '') === compare_bottom_id)) {
        compare_bottom_id = '';
      }
    } catch (e: any) {
      contracts_error = e?.message ?? String(e);
      contractVersions = [];
      selectedContractId = '';
      compare_top_id = '';
      compare_bottom_id = '';
    } finally {
      contracts_loading = false;
    }
  }

  function applySelectedContract(id: string) {
    selectedContractId = id;
  }

  function normalizeContractColumns(columns: any): ContractColumn[] {
    const src = Array.isArray(columns) ? columns : [];
    const out: ContractColumn[] = [];
    const seen = new Set<string>();
    for (const row of src) {
      const field_name = String(row?.field_name || row?.name || '').trim();
      const field_type = String(row?.field_type || row?.type || '').trim();
      const description = String(row?.description || '').trim();
      if (!field_name || !field_type) continue;
      const key = field_name.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      out.push({ field_name, field_type, description });
    }
    return out;
  }

  function getContractById(id: string): ContractVersion | null {
    if (!id) return null;
    return contractVersions.find((x) => String(x.id || '') === id) || null;
  }

  function getActiveContract(): ContractVersion | null {
    return contractVersions.find((x) => isActiveContract(x)) || contractVersions[0] || null;
  }

  function getDefaultCompareBottomId(topId: string) {
    const top = getContractById(topId);
    if (!top) return '';
    const candidates = contractVersions.filter((c) => String(c.id || '') !== String(top.id || ''));
    if (!candidates.length) return '';
    return String(candidates[0].id || '');
  }

  function openCompareVersions() {
    if (!contractVersions.length) return;
    const active = getActiveContract();
    compare_top_id = String(active?.id || contractVersions[0]?.id || '');
    compare_bottom_id = getDefaultCompareBottomId(compare_top_id);
    compare_versions_open = true;
  }

  function buildApplyPlan(target: ContractVersion | null) {
    const targetCols = normalizeContractColumns(target?.columns);
    const currCols = preview_columns.map((c) => ({
      field_name: String(c.name || '').trim(),
      field_type: String(c.type || '').trim(),
      description: String(c.description || '').trim()
    }));
    const currMap = new Map(currCols.map((c) => [c.field_name.toLowerCase(), c]));
    const targetMap = new Map(targetCols.map((c) => [c.field_name.toLowerCase(), c]));
    const toAdd = targetCols.filter((c) => !currMap.has(c.field_name.toLowerCase()));
    const toDrop = currCols.filter((c) => !targetMap.has(c.field_name.toLowerCase()));
    const toDescribe = targetCols.filter((c) => {
      const cur = currMap.get(c.field_name.toLowerCase());
      return !!cur && String(cur.description || '').trim() !== String(c.description || '').trim();
    });
    return { toAdd, toDrop, toDescribe };
  }

  function openApplySelectedVersion() {
    if (!preview_schema || !preview_table) {
      preview_error = 'Таблица не выбрана';
      return;
    }
    if (!selectedContractId) {
      contracts_error = 'Выбери версию контракта справа';
      return;
    }
    const target = getContractById(selectedContractId);
    if (!target) {
      contracts_error = 'Выбранная версия контракта не найдена';
      return;
    }
    modal_error = '';
    modal = 'confirmApplyContractVersion';
  }

  async function applySelectedContractVersionNow() {
    modal_error = '';
    try {
      if (!canEditSelectedTable()) throw new Error('Системную таблицу нельзя редактировать');
      if (!preview_schema || !preview_table) throw new Error('Таблица не выбрана');
      const target = getContractById(selectedContractId);
      if (!target) throw new Error('Выбери версию контракта для применения');
      const version = Number(target.version || 0);
      if (!Number.isFinite(version) || version <= 0) throw new Error('Некорректная версия контракта');

      await apiJson(`${apiBase}/contracts/apply-version`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({
          schema: preview_schema,
          table: preview_table,
          target_version: Math.trunc(version)
        })
      });

      modal = '';
      await refreshPreviewAll();
      await loadContractsPanel();
      await refreshTables();
    } catch (e: any) {
      modal_error = e?.message ?? String(e);
    }
  }

  function isActiveContract(c: ContractVersion) {
    return String(c?.lifecycle_state || '').trim() === 'active';
  }

  async function deleteContractVersion(contract: ContractVersion) {
    try {
      if (!canWrite()) throw new Error('Недостаточно прав (нужна роль data_admin)');
      if (!preview_schema || !preview_table) throw new Error('Таблица не выбрана');
      const version = Number(contract?.version || 0);
      const ok = confirm(`Удалить версию контракта v${version}?`);
      if (!ok) return;
      if (!Number.isFinite(version) || version <= 0) throw new Error('Некорректная версия контракта');
      await apiJson(`${apiBase}/contracts/version/delete`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({ schema: preview_schema, table: preview_table, version: Math.trunc(version) })
      });
      await loadContractsPanel();
    } catch (e: any) {
      contracts_error = e?.message ?? String(e);
    }
  }


  function openAddColumnModal() {
    if (!canEditSelectedTable()) return;
    modal_error = '';
    new_col_name = '';
    new_col_type = 'text';
    new_col_desc = '';
    modal = 'addColumn';
  }

  async function addColumnToTable() {
    modal_error = '';
    try {
      if (!canEditSelectedTable()) throw new Error('Системную таблицу нельзя редактировать');
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
      await loadContractsPanel();
      await refreshTables();
    } catch (e: any) {
      modal_error = e?.message ?? String(e);
    }
  }

  function confirmDropColumn(name: string) {
    if (!canEditSelectedTable()) return;
    modal_error = '';
    pending_drop_column = name;
    modal = 'confirmDropColumn';
  }

  async function dropColumnNow() {
    modal_error = '';
    try {
      if (!canEditSelectedTable()) throw new Error('Системную таблицу нельзя редактировать');
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
      await loadContractsPanel();
      await refreshTables();
    } catch (e: any) {
      modal_error = e?.message ?? String(e);
    }
  }

  async function dropTableFromList(schema: string, table: string) {
    try {
      if (!canWrite()) throw new Error('Недостаточно прав (нужна роль data_admin)');
      const ok = confirm(`Удалить таблицу ${schema}.${table}?`);
      if (!ok) return;

      await apiJson(`${apiBase}/tables/drop`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({ schema, table })
      });

      if (preview_schema === schema && preview_table === table) {
        preview_schema = '';
        preview_table = '';
        preview_columns = [];
        preview_rows = [];
        contractVersions = [];
        newRow = {};
        columnDescriptionDrafts = {};
        edit_schema_name = '';
        edit_table_name = '';
        table_description = '';
      }

      await refreshTables();
    } catch (e: any) {
      preview_error = e?.message ?? String(e);
    }
  }

  async function addRowNow() {
    preview_error = '';
    try {
      if (!canEditSelectedTable()) throw new Error('Системную таблицу нельзя редактировать');
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

  async function saveTableMeta() {
    preview_error = '';
    try {
      if (!canEditSelectedTable()) throw new Error('Системную таблицу нельзя редактировать');
      if (!preview_schema || !preview_table) throw new Error('Таблица не выбрана');
      const nextSchema = edit_schema_name.trim();
      const nextTable = edit_table_name.trim();
      if (!nextSchema) throw new Error('Укажи схему');
      if (!nextTable) throw new Error('Укажи новое название таблицы');

      await apiJson(`${apiBase}/tables/update-meta`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({
          schema: preview_schema,
          table: preview_table,
          new_schema: nextSchema,
          new_table: nextTable,
          description: table_description
        })
      });

      preview_schema = nextSchema;
      preview_table = nextTable;
      edit_schema_name = nextSchema;
      edit_table_name = nextTable;
      await refreshTables();
      await loadColumns();
      await loadPreview();
      await loadTableMeta();
      await loadContractsPanel();
    } catch (e: any) {
      preview_error = e?.message ?? String(e);
    }
  }

  async function saveColumnDescription(name: string) {
    preview_error = '';
    try {
      if (!canEditSelectedTable()) throw new Error('Системную таблицу нельзя редактировать');
      if (!preview_schema || !preview_table) throw new Error('Таблица не выбрана');
      const nextDescription = String(columnDescriptionDrafts[name] || '').trim();

      await apiJson(`${apiBase}/columns/describe`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({
          schema: preview_schema,
          table: preview_table,
          column: name,
          description: nextDescription
        })
      });
      preview_columns = preview_columns.map((c) => (c.name === name ? { ...c, description: nextDescription } : c));
      await loadContractsPanel();
    } catch (e: any) {
      preview_error = e?.message ?? String(e);
    }
  }

  function confirmDeleteRow(ctid: string) {
    if (!canEditSelectedTable()) return;
    modal_error = '';
    pending_delete_row_ctid = ctid;
    modal = 'confirmDeleteRow';
  }

  async function deleteRowNow() {
    modal_error = '';
    try {
      if (!canEditSelectedTable()) throw new Error('Системную таблицу нельзя редактировать');
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

  onMount(async () => {
    await parseContractsStorageConfig();
    await loadContractsPanel();
  });

  async function applyContractsStorageChoice() {
    if (!contracts_storage_pick_value) return;
    const [schema, table] = contracts_storage_pick_value.split('.');
    if (!schema || !table) return;
    const ok = await checkContractsStorageTable(schema, table);
    if (!ok) return;
    contracts_storage_schema = schema;
    contracts_storage_table = table;
    try {
      await apiJson(`${apiBase}/settings/upsert`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({
          setting_key: 'contracts_storage',
          setting_value: { schema, table },
          description: 'Хранилище версий контрактов данных',
          scope: 'global',
          is_active: true
        })
      });
    } catch (e: any) {
      contracts_error = e?.message ?? String(e);
    }
    contracts_storage_picker_open = false;
    await loadContractsPanel();
  }

  function syncTableDescriptionHeight() {
    if (!tableDescriptionEl) return;
    tableDescriptionEl.style.height = 'auto';
    tableDescriptionEl.style.height = `${Math.max(tableDescriptionEl.scrollHeight, 56)}px`;
  }

  function buildCompareRows(top: ContractVersion | null, bottom: ContractVersion | null) {
    const topCols = normalizeContractColumns(top?.columns);
    const bottomCols = normalizeContractColumns(bottom?.columns);
    const order: string[] = [];
    const seen = new Set<string>();
    for (const c of topCols) {
      const k = c.field_name.toLowerCase();
      if (seen.has(k)) continue;
      seen.add(k);
      order.push(k);
    }
    for (const c of bottomCols) {
      const k = c.field_name.toLowerCase();
      if (seen.has(k)) continue;
      seen.add(k);
      order.push(k);
    }
    const topMap = new Map(topCols.map((c) => [c.field_name.toLowerCase(), c]));
    const bottomMap = new Map(bottomCols.map((c) => [c.field_name.toLowerCase(), c]));
    return order.map((k) => ({
      key: k,
      top: topMap.get(k) || null,
      bottom: bottomMap.get(k) || null
    }));
  }

  $: compareTop = getContractById(compare_top_id);
  $: compareBottom = getContractById(compare_bottom_id);
  $: compareRows = buildCompareRows(compareTop, compareBottom);
  $: applyTargetContract = getContractById(selectedContractId);
  $: applyPlan = buildApplyPlan(applyTargetContract);

  $: table_description, tick().then(syncTableDescriptionHeight);
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
              <div class="row-actions">
                {#if isSystemTable(t.schema_name, t.table_name)}
                  <span class="system-badge">System</span>
                {:else}
                  <button
                    class="danger icon-btn"
                    on:click={() => dropTableFromList(t.schema_name, t.table_name)}
                    disabled={!canWrite()}
                    title="Удалить таблицу"
                  >x</button>
                {/if}
              </div>
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
            <button
              class="icon-btn refresh-btn"
              on:click={refreshPreviewAll}
              disabled={preview_loading || !preview_schema || !preview_table}
              title="Обновить предпросмотр"
            >↻</button>
          </div>
        </div>

        {#if preview_schema && preview_table}
          <div class="rename-row">
            <input
              class="rename-input schema-input"
              bind:value={edit_schema_name}
              placeholder="Схема"
              disabled={!canEditSelectedTable()}
            />
            <input
              class="rename-input"
              bind:value={edit_table_name}
              placeholder="Название таблицы"
              disabled={!canEditSelectedTable()}
            />
            <button class="primary save-meta-btn" on:click={saveTableMeta} disabled={!canEditSelectedTable()}>Сохранить</button>
          </div>
          <textarea
            class="table-desc-input"
            bind:this={tableDescriptionEl}
            bind:value={table_description}
            rows="2"
            placeholder="Описание таблицы"
            disabled={!canEditSelectedTable()}
            on:input={syncTableDescriptionHeight}
          ></textarea>
        {/if}
        {#if compare_versions_open}
          <div class="compare-card">
            <div class="compare-head">
              <h4>Сравнение версий контракта</h4>
              <button class="icon-btn" title="Закрыть сравнение" on:click={() => (compare_versions_open = false)}>x</button>
            </div>
            <div class="compare-selectors">
              <select bind:value={compare_top_id}>
                {#each contractVersions as c}
                  <option value={String(c.id || '')}>Версия v{c.version}</option>
                {/each}
              </select>
              <select bind:value={compare_bottom_id}>
                {#each contractVersions as c}
                  <option value={String(c.id || '')}>Версия v{c.version}</option>
                {/each}
              </select>
            </div>
            {#if compareRows.length === 0}
              <p class="hint">Для сравнения выбери две версии контракта.</p>
            {:else}
              <div class="compare-stack">
                <div class="compare-version">
                  <div class="hint">Верхняя версия: v{compareTop?.version ?? '-'}</div>
                  <table class="compare-table">
                    <thead>
                      <tr><th>Колонка</th><th>Тип</th><th>Описание</th></tr>
                    </thead>
                    <tbody>
                      {#each compareRows as row}
                        <tr class:missing-row={!row.top}>
                          <td>{row.top?.field_name || ''}</td>
                          <td>{row.top?.field_type || ''}</td>
                          <td>{row.top?.description || ''}</td>
                        </tr>
                      {/each}
                    </tbody>
                  </table>
                </div>
                <div class="compare-version">
                  <div class="hint">Нижняя версия: v{compareBottom?.version ?? '-'}</div>
                  <table class="compare-table">
                    <thead>
                      <tr><th>Колонка</th><th>Тип</th><th>Описание</th></tr>
                    </thead>
                    <tbody>
                      {#each compareRows as row}
                        <tr class:missing-row={!row.bottom}>
                          <td>{row.bottom?.field_name || ''}</td>
                          <td>{row.bottom?.field_type || ''}</td>
                          <td>{row.bottom?.description || ''}</td>
                        </tr>
                      {/each}
                    </tbody>
                  </table>
                </div>
              </div>
            {/if}
          </div>
        {/if}

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
                          disabled={!canEditSelectedTable()}
                          title="Удалить столбец"
                        >
                          x
                        </button>
                      </div>
                      <div class="thdesc-edit">
                        <textarea
                          class="thdesc-input"
                          bind:value={columnDescriptionDrafts[c.name]}
                          placeholder="Описание поля"
                          disabled={!canEditSelectedTable()}
                          rows="3"
                          on:blur={() => saveColumnDescription(c.name)}
                        ></textarea>
                      </div>
                    </th>
                  {/each}
                  <th class="thadd">
                    <button class="plusbtn" on:click={openAddColumnModal} disabled={!canEditSelectedTable()} title="Добавить столбец">+</button>
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
                        <button class="trash" on:click={() => confirmDeleteRow(r.__ctid)} disabled={!canEditSelectedTable()} title="Удалить строку">x</button>
                      </td>
                    </tr>
                  {/each}
                {/if}
              </tbody>

              <tfoot>
                <tr>
                  {#each preview_columns as c}
                    <td>
                      <input class="cellinput" bind:value={newRow[c.name]} placeholder={c.type} disabled={!canEditSelectedTable()} />
                    </td>
                  {/each}
                  <td class="rowactions">
                    <button class="addrow-icon" on:click={addRowNow} disabled={!canEditSelectedTable()} title="Добавить строку">+</button>
                  </td>
                </tr>
              </tfoot>
            </table>
            </div>

          {/if}
        {/if}
      </div>
    </div>

    <aside class="aside">
      <div class="aside-head">
        <div class="aside-title">Контракты данных</div>
        <button
          class="icon-btn refresh-btn"
          on:click={loadContractsPanel}
          disabled={contracts_loading}
          title="Обновить контракты"
        >↻</button>
      </div>
      <div class="storage-meta">
        <span>Хранятся в таблице:</span>
        <button
          class="link-btn"
          on:click={() => {
            contracts_storage_picker_open = !contracts_storage_picker_open;
            contracts_storage_pick_value = `${contracts_storage_schema}.${contracts_storage_table}`;
          }}
        >
          {contracts_storage_schema}.{contracts_storage_table}
        </button>
      </div>
      {#if contracts_storage_picker_open}
        <div class="storage-picker">
          <select bind:value={contracts_storage_pick_value}>
            {#each existingTables as t}
              <option value={`${t.schema_name}.${t.table_name}`}>{t.schema_name}.{t.table_name}</option>
            {/each}
          </select>
          <button on:click={applyContractsStorageChoice} disabled={!contracts_storage_pick_value}>Подключить</button>
        </div>
      {/if}
      <p class="hint">
        {#if preview_schema && preview_table}
          Для таблицы: {preview_schema}.{preview_table}
        {:else}
          Выбери таблицу слева
        {/if}
      </p>
      <div class="contract-actions">
        <button on:click={openCompareVersions} disabled={!preview_schema || !preview_table || contractVersions.length < 2}>
          Сравнить версии
        </button>
        <button on:click={openApplySelectedVersion} disabled={!preview_schema || !preview_table || !selectedContractId || !canEditSelectedTable()}>
          Привести к выбранной версии
        </button>
      </div>
      {#if contractVersions.length === 0}
        <p class="hint">Версии контракта не найдены.</p>
      {:else}
        <div class="list contracts-list">
          {#each contractVersions as c (c.id)}
            <div class="row-item" class:activeitem={selectedContractId === c.id}>
              <button class="item-button" type="button" on:click={() => applySelectedContract(String(c.id || ''))}>
                v{c.version}
                {#if c.lifecycle_state === 'table_deleted'} · таблица удалена{/if}
              </button>
              <div class="row-actions">
                {#if isActiveContract(c)}
                  <span class="system-badge">Active</span>
                {:else}
                  <button
                    class="danger icon-btn"
                    on:click={() => deleteContractVersion(c)}
                    disabled={!canWrite()}
                    title="Удалить версию контракта"
                  >x</button>
                {/if}
              </div>
            </div>
          {/each}
        </div>
      {/if}
      {#if contracts_error}
        <p class="hint">{contracts_error}</p>
      {/if}
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
          <button class="primary" on:click={addColumnToTable} disabled={!canEditSelectedTable()}>Добавить</button>
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
          <button class="danger" on:click={dropColumnNow} disabled={!canEditSelectedTable()}>Удалить</button>
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
          <button class="danger" on:click={deleteRowNow} disabled={!canEditSelectedTable()}>Удалить</button>
          <button on:click={() => (modal = '')}>Отмена</button>
        </div>
      {:else if modal === 'confirmApplyContractVersion'}
        <h3 style="margin-top:0;">Привести таблицу к версии v{applyTargetContract?.version ?? '-'}</h3>
        <p class="hint">Будет применено к таблице {preview_schema}.{preview_table}</p>
        <div class="hint" style="margin-top:8px;">
          Добавить столбцы: {applyPlan.toAdd.length} · Удалить столбцы: {applyPlan.toDrop.length} · Обновить описания: {applyPlan.toDescribe.length}
        </div>
        {#if applyPlan.toAdd.length > 0}
          <p class="hint">Добавятся: {applyPlan.toAdd.map((x) => x.field_name).join(', ')}</p>
        {/if}
        {#if applyPlan.toDrop.length > 0}
          <p class="hint">Удалятся: {applyPlan.toDrop.map((x) => x.field_name).join(', ')}</p>
        {/if}
        {#if applyPlan.toDescribe.length > 0}
          <p class="hint">Обновятся описания: {applyPlan.toDescribe.map((x) => x.field_name).join(', ')}</p>
        {/if}

        {#if modal_error}
          <div class="alert" style="margin-top:12px;">
            <div class="alert-title">Ошибка</div>
            <pre>{modal_error}</pre>
          </div>
        {/if}

        <div class="modal-actions">
          <button class="primary" on:click={applySelectedContractVersionNow} disabled={!canEditSelectedTable()}>Подтвердить</button>
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
  .rename-row { margin-top:10px; display:grid; grid-template-columns: 5fr 12fr 3fr; gap:8px; align-items:center; }
  .schema-input { min-width:0; }
  .rename-input { width:100%; box-sizing:border-box; border-radius:12px; border:1px solid #e6eaf2; padding:10px 12px; }
  .save-meta-btn { width:100%; }
  .table-desc-input { width:100%; box-sizing:border-box; margin-top:8px; border-radius:12px; border:1px solid #e6eaf2; padding:10px 12px; min-height:56px; resize:none; overflow:hidden; }

  .layout { display:grid; grid-template-columns: 320px 1fr 360px; gap:12px; margin-top:12px; align-items:start; }
  @media (max-width: 1300px) { .layout { grid-template-columns: 320px 1fr; } }
  @media (max-width: 1100px) { .layout { grid-template-columns: 1fr; } }

  .aside { border:1px solid #e6eaf2; border-radius:16px; padding:12px; background:#f8fafc; }
  .aside-head { display:flex; align-items:center; justify-content:space-between; gap:8px; margin-bottom:8px; }
  .aside-title { font-weight:700; font-size:14px; line-height:1.3; margin-bottom:0; }
  .list { display:flex; flex-direction:column; gap:8px; overflow:visible; max-height:none; }
  .row-item { display:grid; grid-template-columns: minmax(0, 1fr) auto; gap:8px; align-items:center; border:1px solid #e6eaf2; border-radius:14px; background:#fff; padding:8px 10px; }
  .row-actions { display:flex; align-items:center; justify-content:flex-end; min-width:54px; flex-shrink:0; }
  .system-badge { font-size:11px; line-height:1; padding:4px 8px; border-radius:999px; border:1px solid #cbd5e1; color:#334155; background:#f8fafc; font-weight:600; }
  .item-button { text-align:left; border:0; background:transparent; padding:0; font-weight:400; font-size:14px; line-height:1.3; color:inherit; min-width:0; width:100%; overflow-wrap:anywhere; word-break:break-word; }
  .tables-list .row-item { background:#0f172a; border-color:#0f172a; }
  .tables-list .item-button { color:#fff; }
  .tables-list .system-badge { border-color:#334155; color:#e2e8f0; background:#1e293b; }
  .contracts-list .row-item { background:#0f172a; border-color:#0f172a; }
  .contracts-list .item-button { color:#fff; font-size:13px; font-weight:400; line-height:1.25; letter-spacing:0; }
  .contracts-list .icon-btn { color:#fff; }
  .tables-list .activeitem { background:#fff; border-color:#e6eaf2; color:#0f172a; }
  .tables-list .activeitem .item-button { color:#0f172a !important; font-size:15px !important; font-weight:600 !important; letter-spacing:.01em !important; line-height:1.25; }
  .tables-list .activeitem .item-button::before { content:'●'; margin-right:8px; font-size:11px; color:#0f172a; vertical-align:middle; }
  .contracts-list .activeitem { background:#fff; border-color:#e6eaf2; color:#0f172a; }
  .contracts-list .activeitem .item-button { color:#0f172a !important; font-size:15px !important; font-weight:600 !important; letter-spacing:.01em !important; line-height:1.25; }
  .contracts-list .activeitem .item-button::before { content:'●'; margin-right:8px; font-size:11px; color:#0f172a; vertical-align:middle; }
  .contracts-list .activeitem .icon-btn { color:#b91c1c; }

  .main { min-width:0; }
  .card { border:1px solid #e6eaf2; border-radius:16px; padding:12px; background:#fff; }

  .storage-meta { margin-top:0; margin-bottom:8px; display:flex; align-items:center; gap:6px; font-size:12px; color:#64748b; }
  .plain-value { color:#0f172a; font-size:12px; font-weight:500; }
  .link-btn { border:0; background:transparent; color:#0f172a; padding:0; text-decoration:underline; font-size:12px; font-weight:500; }
  .storage-picker { display:flex; gap:8px; align-items:center; margin-bottom:8px; }
  .storage-picker select { flex:1; min-width:0; }

  .hint { margin:10px 0 0; color:#64748b; font-size:13px; }
  .muted { color:#64748b; font-size:13px; }
  .contract-actions { margin-top:10px; display:grid; grid-template-columns:1fr 1fr; gap:8px; }

  .compare-card { margin-top:10px; border:1px solid #e6eaf2; border-radius:14px; padding:10px; background:#fff; }
  .compare-head { display:flex; align-items:center; justify-content:space-between; gap:8px; }
  .compare-head h4 { margin:0; font-size:14px; }
  .compare-selectors { margin-top:8px; display:grid; grid-template-columns:1fr 1fr; gap:8px; }
  .compare-selectors select { width:100%; min-width:0; }
  .compare-stack { margin-top:10px; display:grid; grid-template-columns:1fr; gap:10px; }
  .compare-version { border:1px solid #eef2f7; border-radius:12px; padding:8px; }
  .compare-table { width:100%; border-collapse:collapse; min-width:0; }
  .compare-table th, .compare-table td { border-bottom:1px solid #eef2f7; padding:8px; font-size:12px; }
  .compare-table th { position:static; background:transparent; }
  .missing-row td { background:#f1f5f9; color:#94a3b8; }

  .preview { margin-top: 10px; overflow:auto; border:1px solid #e6eaf2; border-radius:16px; }
  table { width:100%; border-collapse:collapse; min-width: 740px; }
  th, td { border-bottom:1px solid #eef2f7; padding:10px; vertical-align:top; text-align:left; }
  th { position: sticky; top:0; background:#fff; z-index:1; }

  .thwrap { display:flex; align-items:center; justify-content:space-between; gap:10px; }
  .thname { font-weight:700; }
  .thdesc-edit { margin-top:6px; display:block; }
  .thdesc-input { width:100%; box-sizing:border-box; border-radius:10px; border:1px solid #e6eaf2; padding:6px 8px; font-size:11px; line-height:1.25; font-weight:400; color:#334155; resize:vertical; min-height:54px; }
  .xbtn { border-color:transparent; background:transparent; color:#b91c1c; border-radius:10px; width:34px; min-width:34px; padding:6px 0; cursor:pointer; text-transform:lowercase; font-size:14px; font-weight:400; line-height:1; }
  .thadd { width: 1%; white-space: nowrap; }
  .plusbtn { border-radius:12px; border-color:transparent; background:transparent; width:34px; min-width:34px; padding:6px 0; font-size:20px; line-height:1; font-weight:400; cursor:pointer; }

  .rowactions { width:44px; min-width:44px; white-space: nowrap; text-align:center; }
  .trash { border-color:transparent; background:transparent; color:#b91c1c; border-radius:12px; padding:6px 10px; cursor:pointer; }
  .addrow-icon { border-radius:12px; border-color:transparent; background:transparent; width:34px; min-width:34px; padding:6px 0; font-size:20px; line-height:1; font-weight:400; cursor:pointer; }

  .cellinput { display:block; width:100%; min-width:120px; box-sizing:border-box; border-radius:12px; border:1px solid #e6eaf2; padding:8px 10px; }

  button { border-radius:14px; border:1px solid #e6eaf2; padding:10px 12px; background:#fff; cursor:pointer; }
  button:disabled { opacity:.6; cursor:not-allowed; }
  .icon-btn { width:34px; min-width:34px; padding:6px 0; text-transform:uppercase; border-color:transparent; background:transparent; }
  .refresh-btn { color:#16a34a; }
  .danger { border-color:#f3c0c0; color:#b91c1c; }
  .danger.icon-btn { border-color:transparent; background:transparent; color:#b91c1c; }
  .tables-list .icon-btn { color:#fff; }
  .tables-list .row-item:not(.activeitem) .danger.icon-btn { color:#fff !important; }
  .tables-list .activeitem .danger.icon-btn { color:#b91c1c !important; }
  .contracts-list .row-item:not(.activeitem) .danger.icon-btn { color:#fff !important; }
  .contracts-list .activeitem .danger.icon-btn { color:#b91c1c !important; }
  .tables-list .activeitem .icon-btn { color:#b91c1c; }
  .primary { background:#0f172a; color:#fff; border-color:#0f172a; }

  .alert { margin: 12px 0; padding: 10px 12px; border-radius: 14px; border: 1px solid #f3c0c0; background: #fff5f5; }
  .alert-title { font-weight: 700; margin-bottom: 6px; }
  pre { margin:0; white-space: pre-wrap; word-break: break-word; }

  .modal-bg { position: fixed; inset: 0; background: rgba(15,23,42,.35); display:flex; align-items:center; justify-content:center; padding: 18px; }
  .modal { width: min(560px, 100%); background:#fff; border-radius:18px; border:1px solid #e6eaf2; padding:14px; box-shadow:0 20px 60px rgba(15,23,42,.25); }
  .modal-actions { display:flex; gap:10px; justify-content:flex-end; margin-top: 12px; }

  .form { display:grid; grid-template-columns: 1fr 1fr; gap:10px; margin-top:12px; }
  @media (max-width: 900px) { .form { grid-template-columns: 1fr; } }
  @media (max-width: 900px) { .contract-actions { grid-template-columns:1fr; } }
  @media (max-width: 900px) { .compare-selectors { grid-template-columns:1fr; } }
  @media (max-width: 900px) { .rename-row { grid-template-columns: 1fr; } }
  .form label { display:flex; flex-direction:column; gap:6px; font-size:13px; }
  .form input, .form select { border-radius:14px; border:1px solid #e6eaf2; padding:10px 12px; }
</style>
