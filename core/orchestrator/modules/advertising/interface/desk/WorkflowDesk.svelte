<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import WorkflowCanvas from './components/WorkflowCanvas.svelte';
  import ApiBuilderTab from './tabs/ApiBuilderTab.svelte';

  type ExistingTable = { schema_name: string; table_name: string };
  type Pane = 'workspace' | 'api';

  const API_BASE = '/ai-orchestrator/api';
  const API_ROLE = 'data_admin';
  const TABLES_TIMEOUT_MS = 15000;

  let pane: Pane = 'workspace';
  let existingTables: ExistingTable[] = [];
  let tablesLoading = false;
  let tablesError = '';
  let tablesStatus = '';
  let handoffInfo = '';
  let initialApiStoreId: number | null = null;
  let apiBuilderRenderKey = 0;

  function parseHashState() {
    const raw = String(window.location.hash || '').replace(/^#/, '');
    const [routeRaw, queryRaw = ''] = raw.split('?');
    const route = String(routeRaw || '').trim() || 'desk/data';
    return {
      route,
      params: new URLSearchParams(queryRaw)
    };
  }

  function buildHash(route: string, params: URLSearchParams) {
    const q = params.toString();
    return `#${route}${q ? `?${q}` : ''}`;
  }

  async function apiJson<T = any>(url: string, init?: RequestInit): Promise<T> {
    const res = await fetch(url, init);
    let payload: any = null;
    try {
      payload = await res.json();
    } catch {
      payload = null;
    }
    if (!res.ok) {
      const msg = payload?.details || payload?.error || `${res.status} ${res.statusText}`;
      throw new Error(String(msg || 'Ошибка API'));
    }
    return payload as T;
  }

  function headers(): Record<string, string> {
    return {
      'Content-Type': 'application/json',
      'X-AO-ROLE': API_ROLE
    };
  }

  async function refreshApiBuilderTables(): Promise<void> {
    if (tablesLoading) return;
    tablesLoading = true;
    tablesError = '';
    tablesStatus = 'Проверяем подключение к базе...';
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), TABLES_TIMEOUT_MS);
    try {
      const res = await fetch(`${API_BASE}/tables?_ts=${Date.now()}`, {
        method: 'GET',
        cache: 'no-store',
        headers: {
          Accept: 'application/json',
          'X-AO-ROLE': API_ROLE
        },
        signal: controller.signal
      });
      const rawText = await res.text();
      let payload: any = {};
      try {
        payload = rawText ? JSON.parse(rawText) : {};
      } catch {
        throw new Error('Сервер вернул некорректный JSON при загрузке таблиц.');
      }
      if (!res.ok) {
        const msg = payload?.details || payload?.error || `${res.status} ${res.statusText}`;
        throw new Error(String(msg || 'Ошибка загрузки таблиц'));
      }
      if (!Array.isArray(payload?.existing_tables)) {
        throw new Error('Сервер вернул неожиданный формат списка таблиц.');
      }
      existingTables = [...(payload.existing_tables as ExistingTable[])];
      tablesStatus = `Подключение к базе: OK. Таблиц доступно: ${existingTables.length}.`;
    } catch (e: any) {
      const msg = e?.name === 'AbortError' ? 'Таймаут проверки подключения к базе.' : String(e?.message || e);
      tablesError = msg;
      tablesStatus = `Подключение к базе: ошибка. ${msg}`;
    } finally {
      clearTimeout(timeoutId);
      tablesLoading = false;
    }
  }

  function setPane(next: Pane) {
    const { params } = parseHashState();
    if (next === 'api') {
      params.set('pane', 'api');
    } else {
      params.delete('pane');
      params.delete('api_store_id');
      params.delete('from_node');
      handoffInfo = '';
    }
    const target = buildHash('desk/data', params);
    if (window.location.hash !== target) {
      window.location.hash = target;
      return;
    }
    pane = next;
    if (pane === 'api') void refreshApiBuilderTables();
  }

  function applyHashState() {
    const { route, params } = parseHashState();
    const fromLegacyWorkflowRoute = route === 'desk/workflow';
    const paneParam = String(params.get('pane') || '').trim().toLowerCase();
    const apiStoreIdRaw = Number(params.get('api_store_id') || 0);
    const apiStoreId = Number.isFinite(apiStoreIdRaw) && apiStoreIdRaw > 0 ? Math.trunc(apiStoreIdRaw) : null;
    const fromNode = String(params.get('from_node') || '').trim();

    const nextPane: Pane = fromLegacyWorkflowRoute || paneParam === 'api' || apiStoreId !== null ? 'api' : 'workspace';
    pane = nextPane;

    if (initialApiStoreId !== apiStoreId) {
      initialApiStoreId = apiStoreId;
      apiBuilderRenderKey += 1;
    }

    handoffInfo = apiStoreId
      ? fromNode
        ? `Открыт из workflow-узла: ${fromNode}. Шаблон API ID: ${apiStoreId}`
        : `Открыт из workflow-узла API. Шаблон ID: ${apiStoreId}`
      : '';

    if (nextPane === 'api') {
      void refreshApiBuilderTables();
    }

    if (fromLegacyWorkflowRoute) {
      const nextParams = new URLSearchParams(params);
      nextParams.set('pane', 'api');
      const target = buildHash('desk/data', nextParams);
      if (window.location.hash !== target) {
        window.location.hash = target;
      }
    }
  }

  const onHashChange = () => applyHashState();

  onMount(() => {
    applyHashState();
    window.addEventListener('hashchange', onHashChange);
  });

  onDestroy(() => {
    window.removeEventListener('hashchange', onHashChange);
  });
</script>

<main class="page">
  <header class="top">
    <div>
      <h1>Рабочий стол данных</h1>
      <p class="sub">Управление процессами и API-шаблонами в едином рабочем пространстве.</p>
    </div>
  </header>

  <nav class="tabs">
    <button class:active={pane === 'workspace'} on:click={() => setPane('workspace')}>Рабочий стол</button>
    <button class:active={pane === 'api'} on:click={() => setPane('api')}>API</button>
  </nav>

  {#if pane === 'workspace'}
    <WorkflowCanvas />
  {:else}
    {#if handoffInfo}
      <div class="okbox">{handoffInfo}</div>
    {/if}
    {#if tablesStatus}
      <p class="hint">{tablesStatus}</p>
    {/if}
    {#if tablesError}
      <div class="alert">
        <div class="alert-title">Ошибка</div>
        <pre>{tablesError}</pre>
      </div>
    {/if}
    {#key `${apiBuilderRenderKey}:${initialApiStoreId || 0}`}
      <ApiBuilderTab
        apiBase={API_BASE}
        {apiJson}
        {headers}
        {existingTables}
        refreshTables={refreshApiBuilderTables}
        {initialApiStoreId}
      />
    {/key}
  {/if}
</main>

<style>
  .page {
    padding: 14px;
    min-height: 100%;
    box-sizing: border-box;
  }
  .top {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 12px;
  }

  h1 {
    margin: 0;
    font-size: 22px;
  }

  .sub {
    margin: 6px 0 0;
    color: #64748b;
    font-size: 13px;
    max-width: 820px;
  }

  .tabs {
    display: flex;
    gap: 8px;
    margin-top: 12px;
    flex-wrap: wrap;
  }

  .tabs button {
    padding: 8px 12px;
    border-radius: 12px;
    border: 1px solid #e6eaf2;
    background: #fff;
    cursor: pointer;
  }

  .tabs button.active {
    background: #0f172a;
    color: #fff;
    border-color: #0f172a;
  }

  .hint {
    margin: 0;
    color: #64748b;
    font-size: 12px;
  }

  .alert {
    margin: 12px 0;
    padding: 10px 12px;
    border-radius: 14px;
    border: 1px solid #f3c0c0;
    background: #fff5f5;
  }

  .alert-title {
    font-weight: 700;
    margin-bottom: 6px;
  }

  .okbox {
    margin: 10px 0;
    border: 1px solid #b8e7c8;
    border-radius: 12px;
    padding: 9px 12px;
    background: #effcf3;
    color: #14532d;
    font-size: 13px;
  }

  pre {
    margin: 0;
    white-space: pre-wrap;
    word-break: break-word;
  }
</style>
