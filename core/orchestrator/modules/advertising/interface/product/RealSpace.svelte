<script>
  import { onMount } from 'svelte';
  import GraphPanel from '../desk/components/GraphPanel.svelte';
  import { loadRealShowcase } from '../desk/data/showcaseApi';
  let loading = true;
  let error = '';
  let count = 0;
  async function refresh() {
    loading = true; error = '';
    try { count = await loadRealShowcase(5000); }
    catch (e) { count = 0; error = String(e?.message || e || 'Витрина недоступна'); }
    loading = false;
  }
  onMount(refresh);
</script>

<section class="page">
  <header><div><small>Аналитика</small><h1>Пространство</h1><p>3D-анализ данных из реальной витрины. Демонстрационная генерация отключена.</p></div><button on:click={refresh} disabled={loading}>{loading ? 'Загрузка…' : 'Обновить'}</button></header>
  {#if error}<div class="state error">{error}. Подготовьте витрину в разделе «Данные».</div>
  {:else if loading}<div class="state">Загружаем витрину…</div>
  {:else if count === 0}<div class="empty"><strong>Витрина пуста</strong><span>После обновления Gold/Showcase реальные точки появятся здесь автоматически.</span><a href="#desk/tables">Открыть данные →</a></div>
  {:else}<div class="state">Загружено строк: {count.toLocaleString('ru-RU')}</div><GraphPanel />{/if}
</section>

<style>
  .page{min-height:100%;padding:24px;box-sizing:border-box;color:#172033}.page>header{display:flex;justify-content:space-between;gap:16px;align-items:flex-start;margin-bottom:12px}.page>header small{color:#94a3b8;font-size:9px;font-weight:800;text-transform:uppercase;letter-spacing:.12em}h1{margin:4px 0;font-size:26px}p{margin:0;color:#64748b;font-size:11px}button{border:1px solid #dbe4f0;border-radius:9px;padding:8px 11px;background:#fff;color:#475569;font-size:10px;font-weight:700}.state{margin-bottom:12px;padding:10px 12px;border:1px solid #e4e9f1;border-radius:10px;background:#fff;color:#64748b;font-size:10px}.state.error{background:#fff1f2;border-color:#fecdd3;color:#9f1239}.empty{min-height:260px;display:grid;place-items:center;align-content:center;gap:7px;border:1px dashed #cbd5e1;border-radius:16px;background:#fff;text-align:center}.empty span{color:#64748b;font-size:11px}.empty a{color:#334155;font-size:10px;font-weight:800;text-decoration:none}@media(max-width:620px){.page{padding:16px}.page>header{flex-direction:column}}
</style>
