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

  type Tab = 'constructor' | 'preview' | 'api_builder' | 'data_management';
  let tab: Tab = 'constructor';

  let loading = false;
  let error = '';
  let existingTables: ExistingTable[] = [];

  async function apiJson<T = any>(url: string, init?: RequestInit): Promise<T> {
    const res = await fetch(url, init);
    let j: any = null;

    try {
      j = await res.json();
    } catch {
      // ignore
    }

    if (!res.ok) {
      const msg = j?.details || j?.error || `${res.status} ${res.statusText}`;
      throw new Error(msg);
    }

    return j as T;
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
    try {
      const j = await apiJson<{ existing_tables: ExistingTable[] }>(`${API_BASE}/tables`);
      existingTables = j.existing_tables || [];
    } catch (e: any) {
      error = e?.message ?? String(e);
    } finally {
      loading = false;
    }
  }

  function onCreated(schema: string, table: string) {
    tab = 'preview';
    // после создания обновим список и выберем созданную таблицу
    refreshTables().then(() => {
      // ничего, выбор таблицы происходит уже внутри preview-вкладки
    });
  }

  onMount(async () => {
    await refreshTables();
  });
</script>

<div class="page">
  <header class="top">
    <div class="head">
      <h1>Конструктор таблиц</h1>
      <p class="sub">
        Создание таблиц, редактирование структуры и данных. Плюс будущие вкладки: API-конструктор и workflow “Управление данными”.
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
      onCreated={onCreated}
      refreshTables={refreshTables}
      headers={headers}
      apiJson={apiJson}
    />
  {:else if tab === 'preview'}
    <TablesAndDataTab
      apiBase={API_BASE}
      {role}
      {loading}
      existingTables={existingTables}
      refreshTables={refreshTables}
      headers={headers}
      apiJson={apiJson}
    />
  {:else if tab === 'api_builder'}
    <ApiBuilderTab />
  {:else}
    <DataManagementTab />
  {/if}
</div>

<style>
  .page { padding: 14px; }

  .top { display:flex; align-items:flex-start; justify-content:space-between; gap:12px; }
  .head h1 { margin:0; font-size:22px; }
  .sub { margin:6px 0 0; color:#64748b; font-size:13px; max-width: 820px; }

  .role { display:flex; align-items:center; gap:8px; }
  .role select { border-radius:12px; border:1px solid #e6eaf2; padding:8px 10px; background:#fff; }

  .tabs { display:flex; gap:8px; margin-top:12px; flex-wrap: wrap; }
  .tabs button {
    padding: 8px 12px; border-radius: 12px; border: 1px solid #e6eaf2; background: #fff; cursor: pointer;
  }
  .tabs button.active { background:#0f172a; color:#fff; border-color:#0f172a; }

  .alert { margin: 12px 0; padding: 10px 12px; border-radius: 14px; border: 1px solid #f3c0c0; background: #fff5f5; }
  .alert-title { font-weight: 700; margin-bottom: 6px; }
  pre { margin:0; white-space: pre-wrap; word-break: break-word; }
</style>
