<script>
  import AdvertisingDashboard from './components/layout/AdvertisingDashboard.svelte';
  import AdvertisingDesk from './desk/AdvertisingDesk.svelte';
  import DataDesk from './desk/DataDesk.svelte';
  import WorkflowDesk from './desk/WorkflowDesk.svelte';

  function hashRoute() {
    const raw = window.location.hash.replace(/^#/, '') || 'desk/data';
    const routeOnly = raw.split('?')[0];
    return routeOnly || 'desk/data';
  }

  function hashPane() {
    const raw = window.location.hash.replace(/^#/, '') || '';
    const idx = raw.indexOf('?');
    if (idx < 0) return '';
    const params = new URLSearchParams(raw.slice(idx + 1));
    return String(params.get('pane') || '').trim().toLowerCase();
  }

  let route = hashRoute();
  let pane = hashPane();

  const onHash = () => {
    route = hashRoute();
    pane = hashPane();
  };

  window.addEventListener('hashchange', onHash);
</script>

<nav class="top-nav">
  <a href="#desk" class:active={route === 'desk'}>Пространство</a>
  <a href="#desk/data" class:active={route === 'desk/data' && pane !== 'api'}>Данные</a>
  <a href="#desk/data?pane=api" class:active={(route === 'desk/data' && pane === 'api') || route === 'desk/workflow'}>API</a>
  <a href="#desk/tables" class:active={route === 'desk/tables'}>Таблицы</a>
  <a href="#legacy" class:active={route === 'legacy'}>Старый дашборд</a>
</nav>

<div class="app">
  {#if route === 'legacy'}
    <AdvertisingDashboard />
  {:else if route === 'desk/tables'}
    <DataDesk />
  {:else if route === 'desk'}
    <AdvertisingDesk />
  {:else}
    <WorkflowDesk />
  {/if}
</div>

<style>
  .top-nav {
    position: sticky;
    top: 0;
    z-index: 5;
    display: flex;
    gap: 10px;
    padding: 10px 18px;
    background: rgba(248, 250, 252, 0.85);
    backdrop-filter: blur(6px);
    border-bottom: 1px solid #e5eaf1;
  }

  .top-nav a {
    text-decoration: none;
    color: #64748b;
    font-weight: 600;
    font-size: 13px;
    padding: 6px 10px;
    border-radius: 999px;
  }

  .top-nav a.active {
    background: #0f172a;
    color: #fff;
  }

  .app {
    height: calc(100vh - 52px);
    min-height: 0;
    overflow: auto;
  }
</style>
