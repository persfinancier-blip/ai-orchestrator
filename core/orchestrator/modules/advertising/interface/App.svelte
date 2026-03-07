<script>
  import AdvertisingDashboard from './components/layout/AdvertisingDashboard.svelte';
  import DataDesk from './desk/DataDesk.svelte';
  import AdvertisingDesk from './desk/AdvertisingDesk.svelte';
  import WorkflowDesk from './desk/WorkflowDesk.svelte';

  function hashRoute() {
    const raw = window.location.hash.replace(/^#/, '') || 'desk';
    const routeOnly = raw.split('?')[0];
    return routeOnly || 'desk';
  }

  let route = hashRoute();

  const onHash = () => {
    route = hashRoute();
  };

  window.addEventListener('hashchange', onHash);
</script>

<nav class="top-nav">
  <a href="#desk" class:active={route === 'desk'}>Пространство</a>
  <a href="#desk/data" class:active={route === 'desk/data'}>Данные</a>
  <a href="#desk/workflow" class:active={route === 'desk/workflow'}>Конструктор</a>
  <a href="#legacy" class:active={route === 'legacy'}>Старый дашборд</a>
</nav>

<div class="app">
  {#if route === 'legacy'}
    <AdvertisingDashboard />
  {:else if route === 'desk/data'}
    <WorkflowDesk />
  {:else if route === 'desk/workflow'}
    <DataDesk />
  {:else}
    <AdvertisingDesk />
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
