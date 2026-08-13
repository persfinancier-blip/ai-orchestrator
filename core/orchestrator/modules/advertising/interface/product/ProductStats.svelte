<script lang="ts">
  import { onMount } from 'svelte';

  let clients = [];
  let runs = [];
  let apiOk = false;
  let loading = true;

  async function read(url) {
    const res = await fetch(url, { cache: 'no-store', headers: { Accept: 'application/json' } });
    if (!res.ok) throw new Error(String(res.status));
    return res.json();
  }

  async function refresh() {
    loading = true;
    const [health, clientList, runList] = await Promise.allSettled([
      read('/ai-orchestrator/api/health'),
      read('/ai-orchestrator/api/clients/module/list'),
      read('/ai-orchestrator/api/process-runs/aggregation?limit=20')
    ]);
    apiOk = health.status === 'fulfilled' && Boolean(health.value?.ok);
    clients = clientList.status === 'fulfilled' && Array.isArray(clientList.value?.clients) ? clientList.value.clients : [];
    runs = runList.status === 'fulfilled' && Array.isArray(runList.value?.aggregation) ? runList.value.aggregation : [];
    loading = false;
  }

  onMount(refresh);

  $: activeClients = clients.filter((item) => ['active', 'setup'].includes(String(item.status || '').toLowerCase())).length;
  $: accesses = clients.reduce((sum, item) => sum + Number(item.active_access_count || 0), 0);
  $: warnings = clients.reduce((sum, item) => sum + Number(item.warning_count || 0), 0);
  $: failedRuns = runs.filter((item) => ['failed', 'completed_with_errors'].includes(String(item.final_status || '').toLowerCase()) || Number(item.dead_letter_jobs || 0) > 0).length;
</script>

<div class="stats">
  <a href="#desk/data?pane=clients"><span>Клиенты</span><strong>{loading ? '…' : activeClients}</strong><small>{clients.length} всего</small></a>
  <a href="#desk/data?pane=clients"><span>Подключения</span><strong>{loading ? '…' : accesses}</strong><small>активных кабинетов</small></a>
  <a href="#desk/data"><span>Последние процессы</span><strong>{loading ? '…' : runs.length}</strong><small>{failedRuns} с ошибками</small></a>
  <a href="#desk/data?pane=clients" class:attention={warnings + failedRuns > 0}><span>Требует внимания</span><strong>{loading ? '…' : warnings + failedRuns}</strong><small>{apiOk ? 'API подключён' : 'проверьте API'}</small></a>
</div>

<style>
  .stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 14px; }
  a { min-height: 105px; padding: 16px; border: 1px solid #e4e9f1; border-radius: 16px; background: #fff; color: #172033; text-decoration: none; display: flex; flex-direction: column; box-sizing: border-box; }
  span { color: #64748b; font-size: 11px; font-weight: 700; } strong { margin-top: auto; font-size: 27px; letter-spacing: -.04em; } small { color: #94a3b8; font-size: 10px; }
  a.attention strong { color: #b91c1c; }
  @media (max-width: 800px) { .stats { grid-template-columns: repeat(2, 1fr); } }
  @media (max-width: 520px) { .stats { grid-template-columns: 1fr; } }
</style>
