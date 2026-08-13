<script>
  import { onMount } from 'svelte';
  import GraphPanel from '../../desk/components/GraphPanel.svelte';
  import { setShowcaseData } from '../../desk/data/showcaseStore';
  let loading = true;
  let error = '';
  let relationships = [];
  async function load() {
    loading = true; error = '';
    try {
      const res = await fetch('/ai-orchestrator/api/product/insights/scan', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({ schema:'showcase', table:'advertising', limit:2500 }) });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data?.details || data?.error || `HTTP ${res.status}`);
      relationships = data?.relationships || [];
      setShowcaseData(data?.rows || [], data?.plan?.fields || []);
    } catch (e) { error = String(e?.message || e); setShowcaseData([], []); }
    finally { loading = false; }
  }
  onMount(load);
</script>

<section class="page">
  <header><small>Аналитика</small><h1>Пространство зависимостей</h1><p>Реальная Gold-витрина → автоматический поиск связей → 3D-исследование.</p></header>
  {#if loading}<p>Анализируем данные…</p>{:else if error}<p class="error">{error}</p>{/if}
  {#if relationships.length}<div class="relations">{#each relationships.slice(0,8) as item}<span><b>{item.x} ↔ {item.y}</b> r={Number(item.correlation).toFixed(2)}</span>{/each}</div>{/if}
  <div class="graph"><GraphPanel /></div>
  <a class="forecast" href="#home">Прогнозы уже доступны в product API; отдельный экран подключается следующим шагом →</a>
</section>

<style>
  .page{max-width:1380px;margin:auto;padding:28px;color:#172033}header{max-width:760px}small{color:#94a3b8;font-size:9px;text-transform:uppercase;font-weight:800}h1{margin:6px 0;font-size:32px}p{color:#64748b;font-size:10px}.error{color:#b42318}.relations{display:flex;flex-wrap:wrap;gap:6px;margin:12px 0}.relations span{padding:7px 9px;background:#fff;border:1px solid #e4e9f1;border-radius:8px;color:#64748b;font-size:9px}.graph{min-height:560px;background:#fff;border:1px solid #e4e9f1;border-radius:14px;padding:10px}.forecast{display:inline-block;margin-top:12px;color:#172033;font-size:10px;font-weight:800;text-decoration:none}
</style>
