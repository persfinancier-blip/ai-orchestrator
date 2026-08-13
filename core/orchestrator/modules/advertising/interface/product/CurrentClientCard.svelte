<script lang="ts">
  import { onMount } from 'svelte';
  let detail: any = null;
  let loading = true;

  async function read(url: string) {
    const res = await fetch(url, { cache: 'no-store', headers: { Accept: 'application/json' } });
    if (!res.ok) return null;
    return res.json();
  }

  async function load() {
    loading = true;
    const context = await read('/ai-orchestrator/api/context/client');
    const id = Number(context?.client_id || 0);
    detail = id ? await read(`/ai-orchestrator/api/clients/module/client/${id}`) : null;
    loading = false;
  }

  onMount(load);
  $: name = detail?.client?.client_display_name || detail?.client?.client_code || 'Клиент не выбран';
  $: accesses = Number(detail?.client?.active_access_count || 0);
  $: goals = Number(detail?.client?.active_goal_count || 0);
  $: kpis = Number(detail?.client?.active_kpi_count || 0);
  $: warnings = Number(detail?.client?.warning_count || 0);
</script>

<section>
  <div><small>Текущий клиент</small><strong>{loading ? 'Загрузка…' : name}</strong></div>
  <dl><span><b>{accesses}</b><small>кабинетов</small></span><span><b>{goals}</b><small>целей</small></span><span><b>{kpis}</b><small>KPI</small></span><span class:warn={warnings > 0}><b>{warnings}</b><small>проблем</small></span></dl>
  <a href="#desk/data?pane=clients">Открыть карточку →</a>
</section>

<style>
  section { margin-bottom: 14px; padding: 18px 20px; border: 1px solid #e4e9f1; border-radius: 16px; background: #fff; display: grid; grid-template-columns: minmax(180px,1fr) auto auto; gap: 20px; align-items: center; }
  div { display: flex; flex-direction: column; } small { color: #94a3b8; font-size: 9px; } strong { margin-top: 4px; font-size: 17px; } dl { margin: 0; display: flex; gap: 22px; } dl span { display: flex; flex-direction: column; } b { font-size: 17px; } .warn b { color: #b91c1c; } a { color: #475569; font-size: 11px; font-weight: 700; text-decoration: none; }
  @media (max-width: 720px) { section { grid-template-columns: 1fr; } dl { justify-content: space-between; } }
</style>
