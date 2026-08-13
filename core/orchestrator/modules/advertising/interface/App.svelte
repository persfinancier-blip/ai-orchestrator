<script>
  import { onDestroy, onMount } from 'svelte';
  import ProductShellV2 from './product/ProductShellV2.svelte';
  import ProductHome from './product/ProductHome.svelte';
  import ProductLogin from './product/ProductLogin.svelte';
  import CurrentClientCard from './product/CurrentClientCard.svelte';
  import IntegrationAssistant from './product/IntegrationAssistant.svelte';
  import AdvertisingDashboard from './components/layout/AdvertisingDashboard.svelte';
  import DataDesk from './desk/DataDesk.svelte';
  import WorkflowDesk from './desk/WorkflowDesk.svelte';

  const titles = { home: 'Обзор', clients: 'Клиенты', advertising: 'Реклама', assistant: 'Ассистент', automation: 'Сценарии', integrations: 'API вручную', data: 'Данные', space: 'Аналитика' };

  function routeState() {
    const raw = String(window.location.hash || '').replace(/^#/, '') || 'home';
    const [route, query = ''] = raw.split('?');
    const pane = String(new URLSearchParams(query).get('pane') || '').toLowerCase();
    let section = 'home';
    if (route === 'assistant') section = 'assistant';
    else if (route === 'legacy') section = 'advertising';
    else if (route === 'desk') section = 'space';
    else if (route === 'desk/tables') section = 'data';
    else if (route === 'desk/data' || route === 'desk/workflow') section = pane === 'clients' ? 'clients' : pane === 'api' ? 'integrations' : 'automation';
    return { section };
  }

  let state = routeState();
  let clientEpoch = 0;
  let checking = true;
  let authenticated = false;
  let authError = '';
  const onHash = () => {
    state = routeState();
    window.dispatchEvent(new CustomEvent('ao:client-context-refresh'));
  };
  const onClient = () => (clientEpoch += 1);
  const onSessionExpired = () => {
    authenticated = false;
    authError = 'Сессия завершена. Войдите снова.';
  };

  async function checkSession() {
    checking = true;
    authError = '';
    try {
      const res = await fetch('/ai-orchestrator/api/auth/session', { cache: 'no-store' });
      const payload = await res.json().catch(() => ({}));
      authenticated = res.ok && Boolean(payload?.authenticated);
    } catch (error) {
      authenticated = false;
      authError = String(error?.message || error || 'API недоступен');
    } finally {
      checking = false;
    }
  }

  async function logout() {
    try { await fetch('/ai-orchestrator/api/context/client', { method: 'DELETE' }); } catch {}
    try { await fetch('/ai-orchestrator/api/auth/logout', { method: 'POST' }); } catch {}
    authenticated = false;
  }

  onMount(() => {
    window.addEventListener('hashchange', onHash);
    window.addEventListener('ao:client-context-changed', onClient);
    window.addEventListener('ao:session-expired', onSessionExpired);
    checkSession();
  });
  onDestroy(() => {
    window.removeEventListener('hashchange', onHash);
    window.removeEventListener('ao:client-context-changed', onClient);
    window.removeEventListener('ao:session-expired', onSessionExpired);
  });
  $: title = titles[state.section] || 'Обзор';
</script>

{#if checking}
  <div class="boot">AI Orchestrator · подключение…</div>
{:else if !authenticated}
  <ProductLogin on:authenticated={() => { authenticated = true; authError = ''; }} />
  {#if authError}<div class="api-error">{authError}</div>{/if}
{:else}
  <ProductShellV2 section={state.section} {title} on:logout={logout}>
    {#if state.section === 'home'}
      {#key clientEpoch}<div class="client"><CurrentClientCard /></div>{/key}<ProductHome />
    {:else if state.section === 'assistant'}<IntegrationAssistant />
    {:else if state.section === 'advertising'}<AdvertisingDashboard />
    {:else if state.section === 'data'}<DataDesk />
    {:else if state.section === 'space'}
      <section class="space-off"><small>Аналитика</small><h1>Пространство отключено от production-пути</h1><p>Старый 3D-прототип использовал сгенерированные точки. Он будет возвращён только после подключения к проверенной Gold/Showcase-витрине.</p><a href="#desk/tables">Открыть данные →</a></section>
    {:else}<WorkflowDesk />{/if}
  </ProductShellV2>
{/if}

<style>
  :global(html), :global(body), :global(#app) { margin: 0; min-height: 100%; }
  :global(body) { background: #f6f8fb; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
  .boot { min-height: 100vh; display: grid; place-items: center; color: #64748b; font: 12px system-ui; }.api-error { position: fixed; bottom: 14px; left: 50%; transform: translateX(-50%); padding: 8px 12px; background: #fff1f2; color: #be123c; border-radius: 8px; font: 11px system-ui; }
  .client { max-width: 1180px; margin: 0 auto -24px; padding: 28px 32px 0; box-sizing: border-box; }
  .space-off { max-width: 760px; margin: 0 auto; padding: 56px 30px; color: #172033; }.space-off small { color: #94a3b8; font-size: 9px; font-weight: 800; letter-spacing: .12em; text-transform: uppercase; }.space-off h1 { margin: 7px 0 10px; font-size: 32px; letter-spacing: -.04em; }.space-off p { margin: 0 0 18px; color: #64748b; font-size: 12px; line-height: 1.65; }.space-off a { padding: 9px 12px; border-radius: 9px; background: #172033; color: #fff; text-decoration: none; font-size: 10px; font-weight: 800; }
  @media (max-width: 560px) { .client { padding: 20px 18px 0; margin-bottom: -12px; } }
</style>
