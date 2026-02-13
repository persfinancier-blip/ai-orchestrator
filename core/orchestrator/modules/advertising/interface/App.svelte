<script>
  import AdvertisingDashboard from './components/layout/AdvertisingDashboard.svelte';
  import AdvertisingDesk from './desk/AdvertisingDesk.svelte';

  let route = window.location.hash.replace('#', '') || 'desk';
  const onHash = () => (route = window.location.hash.replace('#', '') || 'desk');
  window.addEventListener('hashchange', onHash);
</script>

<nav class="top-nav">
  <a href="#desk" class:active={route === 'desk'}>Пространство</a>
  <a href="#legacy" class:active={route === 'legacy'}>Старый дашборд</a>
</nav>

{#if route === 'legacy'}
  <div class="app"><AdvertisingDashboard /></div>
{:else}
  <AdvertisingDesk />
{/if}

<style>
  .top-nav { position: sticky; top:0; z-index:5; display:flex; gap:10px; padding:10px 18px; background:rgba(248,250,252,.85); backdrop-filter: blur(6px); border-bottom:1px solid #e5eaf1; }
  .top-nav a { text-decoration:none; color:#64748b; font-weight:600; font-size:13px; padding:6px 10px; border-radius:999px; }
  .top-nav a.active { background:#0f172a; color:#fff; }
  .app { height: calc(100vh - 52px); overflow: hidden; }
</style>
