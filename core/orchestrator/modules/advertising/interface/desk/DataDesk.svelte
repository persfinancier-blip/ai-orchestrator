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
  const TABLES_TIMEOUT_MS = 15000;

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

  async function fetchTablesFromServer(currentRole: Role): Promise<ExistingTable[]> {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), TABLES_TIMEOUT_MS);
    try {
      const url = `${API_BASE}/tables?_ts=${Date.now()}`;
      const res = await fetch(url, {
        method: 'GET',
        cache: 'no-store',
        headers: {
          Accept: 'application/json',
          'X-AO-ROLE': currentRole
        },
        signal: controller.signal
      });

      const raw = await res.text();
      let payload: any = null;
      try {
        payload = raw ? JSON.parse(raw) : {};
      } catch {
        throw new Error('Сервер вернул некорректный JSON при загрузке таблиц.');
      }

      if (!res.ok) {
        const msg = payload?.details || payload?.error || `${res.status} ${res.statusText}`;
        throw new Error(msg);
      }

      if (!Array.isArray(payload?.existing_tables)) {
        throw new Error('Сервер вернул неожиданный формат списка таблиц.');
      }

      return [...(payload.existing_tables as ExistingTable[])];
    } catch (e: any) {
      if (e?.name === 'AbortError') {
        throw new Error('Таймаут проверки подключения к базе.');
      }
      throw new Error(e?.message ?? String(e));
    } finally {
      clearTimeout(timeoutId);
    }
  }

  async function refreshTables(): Promise<void> {
    loading = true;
    error = '';
    dbStatus = 'checking';
    dbStatusMessage = 'Проверяем подключение к базе...';
    try {
      const tables = await fetchTablesFromServer(role);
      existingTables = tables;
      dbStatus = 'ok';
      dbStatusMessage = `Подключение к базе: OK. Таблиц доступно: ${tables.length}.`;
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
        Создание и редактирование таблиц. Отдельно доступны конструктор API и блок управления данными.
      </p>
    </div>

    <div class="role">
      <span>Роль:</span>
      <select bind:value={role} on:change={refreshTables}>
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

  .alert { margin: 12px 0; padding: 10px 12px; border-radius: 14px; border: 1px solid #f3c0c0; background: #fff5f5; }
  .alert-title { font-weight: 700; margin-bottom: 6px; }
  pre { margin:0; white-space: pre-wrap; word-break: break-word; }
</style>
