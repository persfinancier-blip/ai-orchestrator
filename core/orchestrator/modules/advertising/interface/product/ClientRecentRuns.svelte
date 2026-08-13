<script>
  import { onDestroy, onMount } from 'svelte';
  import { productGet } from './productApi.js';

  let rows = [];
  let loading = true;
  let error = '';

  async function load() {
    loading = true;
    error = '';
    try {
      const context = await productGet('/context/client');
      const clientId = Number(context?.client_id || 0);
      const payload = await productGet('/process-runs?limit=100');
      rows = (Array.isArray(payload?.runs) ? payload.runs : [])
        .filter((item) => Number(item?.client_id || 0) === clientId)
        .slice(0, 6);
    } catch (e) {
      rows = [];
      error = String(e?.message || e || 'История недоступна');
    } finally {
      loading = false;
    }
  }

  const refresh = () => load();
  onMount(() => {
    window.addEventListener('ao:client-context-changed', refresh);
    load();
  });
  onDestroy(() => window.removeEventListener('ao:client-context-changed', refresh));

  function started(value) {
    return value ? new Date(value).toLocaleString('ru-RU') : '—';
  }
</script>

<section>
  <header><strong>Последние процессы клиента</strong><a href="#desk/data">Открыть сценарии →</a></header>
  {#if loading}<p>Загрузка…</p>
  {:else if error}<p class="error">{error}</p>
  {:else if !rows.length}<p>У выбранного клиента пока нет запусков.</p>
  {:else}
    {#each rows as row}
      <div class="run">
        <span><b>{row.process_code || row.desk_name || 'process'}</b><small>{started(row.started_at)}</small></span>
        <i class={String(row.status || '').toLowerCase()}>{row.status || 'unknown'}</i>
      </div>
    {/each}
  {/if}
</section>

<style>
  section{margin-bottom:14px;border:1px solid #e4e9f1;border-radius:16px;background:#fff;overflow:hidden}header,.run{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:12px 15px}header{background:#f8fafc}header strong{font-size:11px}header a{color:#64748b;font-size:9px;text-decoration:none}.run{border-top:1px solid #eef1f5}.run span{display:flex;flex-direction:column;min-width:0}.run b{font-size:10px}.run small{margin-top:2px;color:#94a3b8;font-size:9px}.run i{padding:4px 7px;border-radius:999px;background:#f1f5f9;color:#475569;font-size:8px;font-style:normal}.run i.completed{background:#ecfdf5;color:#166534}.run i.failed,.run i.completed_with_errors{background:#fff1f2;color:#be123c}.run i.running,.run i.queued{background:#eff6ff;color:#1d4ed8}p{margin:0;padding:15px;color:#94a3b8;font-size:10px}p.error{color:#be123c}
</style>
