<!-- File: core/orchestrator/modules/advertising/interface/desk/DataDesk.svelte -->
<script lang="ts">
  import { onMount } from 'svelte';

  import CreateTableTab from './tabs/CreateTableTab.svelte';
  import TablesAndDataTab from './tabs/TablesAndDataTab.svelte';
  import ApiBuilderTab from './tabs/ApiBuilderTab.svelte';
  import DataManagementTab from './tabs/DataManagementTab.svelte';

  export type Role = 'viewer' | 'operator' | 'data_admin';
  export type ExistingTable = { schema_name: string; table_name: string };

  const API_BASE = '/ai-orchestrator/api';

  let role: Role = 'data_admin';

  type Tab = 'constructor' | 'tables' | 'api_builder' | 'data_management';
  let tab: Tab = 'constructor';

  let loading = false;
  let error = '';
  let existingTables: ExistingTable[] = [];
  let dbStatus: 'checking' | 'ok' | 'error' = 'checking';
  let dbStatusMessage = 'Проверка подключения к базе...';

  async function apiJson<T = any>(url: string, init?: RequestInit): Promise<T> {
    const res = await fetch(url, init);

    let payload: any = null;
    try {
      payload = await res.json();
    } catch {
      // ignore
    }

    if (!res.ok) {
      const msg = payload?.details || payload?.error || `${res.status} ${res.statusText}`;
      throw new Error(msg);
    }

    return payload as T;
  }

  function headers(): Record<string, string> {
    return {
      'Content-Type': 'application/json',
      'X-AO-ROLE': role
    };
  }

  async function refreshTables(): Promise<void> {
    loading = true;
    error = '';
    dbStatus = 'checking';
    dbStatusMessage = 'Проверка подключения к базе...';
    try {
      const j = await apiJson<{ existing_tables: ExistingTable[] }>(`${API_BASE}/tables`);
      existingTables = j.existing_tables || [];
      dbStatus = 'ok';
      dbStatusMessage = `Подключение к базе: OK. Таблиц доступно: ${existingTables.length}.`;
    } catch (e: any) {
      error = e?.message ?? String(e);
      dbStatus = 'error';
      dbStatusMessage = `Подключение к базе: ошибка. ${error}`;
    } finally {
      loading = false;
    }
  }

  function onCreated(schema: string, table: string) {
    tab = 'tables';
    refreshTables();
  }

  onMount(refreshTables);
</script>

<div class="page">
  <header class="top">
    <div>
      <h1>Конструктор таблиц</h1>
      <p class="sub">
        Создание/редактирование таблиц. Отдельно: будущий конструктор API и будущий workflow “Управление данными”.
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
    <button class:active={tab === 'api_builder'} on:click={() => (tab = 'api_builder')}>API</button>
    <button class:active={tab === 'data_management'} on:click={() => (tab = 'data_management')}>Управление данными</button>
  </nav>

  <div class="db-status" class:ok={dbStatus === 'ok'} class:error={dbStatus === 'error'}>
    {dbStatusMessage}
  </div>

  {#if error}
    <div class="alert">
      <div class="alert-title">Ошибка</div>
      <pre>{error}</pre>
    </div>
  {/if}

  {#if tab === 'constructor'}
    <CreateTableTab
      apiBase={API_BASE}
      {role}
      {loading}
      {dbStatus}
      {dbStatusMessage}
      {headers}
      {apiJson}
      {refreshTables}
      {existingTables}
      onCreated={onCreated}
    />
  {:else if tab === 'tables'}
    <TablesAndDataTab
      apiBase={API_BASE}
      {role}
      {loading}
      {headers}
      {apiJson}
      {refreshTables}
      {existingTables}
    />
  {:else if tab === 'api_builder'}
    <ApiBuilderTab
      apiBase={API_BASE}
      {apiJson}
      {headers}
      {existingTables}
      {refreshTables}
    />
  {:else}
    <DataManagementTab />
  {/if}
</div>

<style>
  .page { padding: 14px; }
  .top { display:flex; align-items:flex-start; justify-content:space-between; gap:12px; }
  h1 { margin:0; font-size:22px; }
  .sub { margin:6px 0 0; color:#64748b; font-size:13px; max-width: 820px; }

  .role { display:flex; align-items:center; gap:8px; }
  .role select { border-radius:12px; border:1px solid #e6eaf2; padding:8px 10px; background:#fff; }

  .tabs { display:flex; gap:8px; margin-top:12px; flex-wrap:wrap; }
  .tabs button { padding:8px 12px; border-radius:12px; border:1px solid #e6eaf2; background:#fff; cursor:pointer; }
  .tabs button.active { background:#0f172a; color:#fff; border-color:#0f172a; }
  .db-status { margin-top:12px; border:1px solid #e6eaf2; border-radius:12px; padding:10px 12px; background:#f8fafc; color:#334155; font-size:13px; }
  .db-status.ok { border-color:#bbf7d0; background:#f0fdf4; color:#166534; }
  .db-status.error { border-color:#fecaca; background:#fef2f2; color:#991b1b; }

  .alert { margin: 12px 0; padding: 10px 12px; border-radius: 14px; border: 1px solid #f3c0c0; background: #fff5f5; }
  .alert-title { font-weight: 700; margin-bottom: 6px; }
  pre { margin:0; white-space: pre-wrap; word-break: break-word; }
</style>
