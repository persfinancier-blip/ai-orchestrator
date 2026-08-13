<script>
  import { onDestroy, onMount } from 'svelte';
  import ProductHome from './product/ProductHome.svelte';
  import ProductLogin from './product/ProductLogin.svelte';
  import AdvertisingDashboard from './components/layout/AdvertisingDashboard.svelte';
  import AdvertisingDesk from './desk/AdvertisingDesk.svelte';
  import DataDesk from './desk/DataDesk.svelte';
  import WorkflowDesk from './desk/WorkflowDesk.svelte';

  const nav = [
    ['home', '#home', 'Обзор', 'Работа'],
    ['clients', '#desk/data?pane=clients', 'Клиенты', 'Работа'],
    ['advertising', '#legacy', 'Реклама', 'Работа'],
    ['automation', '#desk/data', 'Сценарии', 'Автоматизация'],
    ['integrations', '#desk/data?pane=api', 'Интеграции', 'Автоматизация'],
    ['data', '#desk/tables', 'Данные', 'Автоматизация'],
    ['space', '#desk', 'Пространство', 'Аналитика']
  ];

  function readState() {
    const raw = String(window.location.hash || '').replace(/^#/, '') || 'home';
    const [route, query = ''] = raw.split('?');
    const pane = String(new URLSearchParams(query).get('pane') || '').toLowerCase();
    let section = 'home';
    if (route === 'legacy') section = 'advertising';
    else if (route === 'desk') section = 'space';
    else if (route === 'desk/tables') section = 'data';
    else if (route === 'desk/data' || route === 'desk/workflow') section = pane === 'clients' ? 'clients' : pane === 'api' ? 'integrations' : 'automation';
    return { route, pane, section };
  }

  let state = readState();
  let authChecking = true;
  let authenticated = false;
  let authError = '';
  const onHash = () => (state = readState());
  window.addEventListener('hashchange', onHash);
  onDestroy(() => window.removeEventListener('hashchange', onHash));

  async function checkSession() {
    authChecking = true;
    authError = '';
    try {
      const res = await fetch('/ai-orchestrator/api/auth/session', { cache: 'no-store' });
      const payload = await res.json().catch(() => ({}));
      authenticated = res.ok && Boolean(payload?.authenticated);
    } catch (e) {
      authenticated = false;
      authError = String(e?.message || e || 'API недоступен');
    } finally {
      authChecking = false;
    }
  }

  async function logout() {
    try { await fetch('/ai-orchestrator/api/auth/logout', { method: 'POST' }); } catch {}
    authenticated = false;
  }

  onMount(checkSession);
  $: groups = [...new Set(nav.map((item) => item[3]))];
  $: title = nav.find((item) => item[0] === state.section)?.[2] || 'Обзор';
</script>

{#if authChecking}
  <div class="boot">AI Orchestrator · подключение…</div>
{:else if !authenticated}
  <ProductLogin on:authenticated={() => (authenticated = true)} />
  {#if authError}<div class="api-error">{authError}</div>{/if}
{:else}
  <div class="shell">
    <aside>
      <a class="brand" href="#home"><b>AO</b><span><strong>AI Orchestrator</strong><small>Marketplace OS</small></span></a>
      <nav>
        {#each groups as group}
          <section><small>{group}</small>{#each nav.filter((item) => item[3] === group) as item}<a href={item[1]} class:active={state.section === item[0]}>{item[2]}</a>{/each}</section>
        {/each}
      </nav>
      <div class="status"><i></i> Self-hosted</div>
    </aside>

    <div class="workspace">
      <header><span><small>Рабочее пространство</small><strong>{title}</strong></span><button on:click={logout}>Выйти</button></header>
      <main>
        {#if state.section === 'home'}<ProductHome />
        {:else if state.section === 'advertising'}<AdvertisingDashboard />
        {:else if state.section === 'data'}<DataDesk />
        {:else if state.section === 'space'}<AdvertisingDesk />
        {:else}<WorkflowDesk />{/if}
      </main>
    </div>
  </div>
{/if}

<style>
  :global(html), :global(body), :global(#app) { margin: 0; min-height: 100%; }
  :global(body) { background: #f6f8fb; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
  .boot { min-height: 100vh; display: grid; place-items: center; color: #64748b; font: 12px system-ui; }.api-error { position: fixed; bottom: 14px; left: 50%; transform: translateX(-50%); padding: 8px 12px; background: #fff1f2; color: #be123c; border-radius: 8px; font: 11px system-ui; }
  .shell { min-height: 100vh; display: grid; grid-template-columns: 218px minmax(0, 1fr); color: #172033; }
  aside { position: sticky; top: 0; height: 100vh; padding: 18px 13px; box-sizing: border-box; background: #fff; border-right: 1px solid #e4e9f1; display: flex; flex-direction: column; }
  .brand { display: flex; align-items: center; gap: 9px; padding: 4px 7px 20px; color: inherit; text-decoration: none; }.brand > b { width: 33px; height: 33px; display: grid; place-items: center; border-radius: 10px; background: #172033; color: #fff; font-size: 11px; }.brand span { display: flex; flex-direction: column; }.brand strong { font-size: 12px; }.brand small { color: #94a3b8; font-size: 9px; }
  nav { display: flex; flex-direction: column; gap: 16px; } nav section { display: flex; flex-direction: column; gap: 3px; } nav section > small { padding: 0 8px 4px; color: #a0aabc; font-size: 8px; font-weight: 800; text-transform: uppercase; letter-spacing: .12em; } nav a { padding: 9px; border-radius: 9px; color: #64748b; text-decoration: none; font-size: 12px; font-weight: 650; } nav a:hover { background: #f4f6f9; color: #172033; } nav a.active { background: #172033; color: #fff; }
  .status { margin-top: auto; padding: 9px; color: #94a3b8; font-size: 9px; display: flex; gap: 7px; align-items: center; }.status i { width: 7px; height: 7px; border-radius: 50%; background: #22c55e; }
  .workspace { min-width: 0; min-height: 100vh; }.workspace > header { position: sticky; top: 0; z-index: 20; height: 56px; padding: 9px 17px; box-sizing: border-box; border-bottom: 1px solid #e4e9f1; background: rgba(255,255,255,.94); display: flex; align-items: center; justify-content: space-between; } header span { display: flex; flex-direction: column; } header small { color: #94a3b8; font-size: 9px; } header strong { font-size: 13px; } header button { border: 1px solid #dfe5ed; border-radius: 8px; padding: 6px 9px; background: #fff; color: #64748b; font-size: 10px; cursor: pointer; }
  main { min-width: 0; min-height: calc(100vh - 56px); overflow: auto; }
  @media (max-width: 760px) { .shell { grid-template-columns: 1fr; } aside { height: auto; z-index: 30; padding: 8px; border-right: 0; border-bottom: 1px solid #e4e9f1; } .brand, nav section > small, .status { display: none; } nav { flex-direction: row; gap: 4px; overflow-x: auto; } nav section { flex-direction: row; gap: 4px; } nav a { white-space: nowrap; padding: 7px 9px; } .workspace > header { top: 43px; } }
</style>
