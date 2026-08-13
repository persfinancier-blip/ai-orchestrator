<script>
  import { onMount } from 'svelte';
  import GraphPanel from '../../desk/components/GraphPanel.svelte';
  import { setShowcaseData } from '../../desk/data/showcaseStore';
  let loading = true;
  let error = '';
  let relationships = [];
  let anomalies = [];
  let metric = 'revenue';
  let orderField = 'date';
  let horizon = 30;
  let target = '';
  let forecast = null;

  function orderFields(fields, axes) {
    const order = new Map((Array.isArray(axes) ? axes : []).map((name, index) => [String(name), index]));
    return [...(Array.isArray(fields) ? fields : [])].sort((a, b) => {
      const ar = order.has(String(a?.source || '')) ? order.get(String(a.source)) : 1000;
      const br = order.has(String(b?.source || '')) ? order.get(String(b.source)) : 1000;
      return ar - br;
    });
  }

  async function load() {
    loading = true; error = '';
    try {
      const res = await fetch('/ai-orchestrator/api/product/insights/scan', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({ schema:'showcase', table:'advertising', limit:2500 }) });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data?.details || data?.error || `HTTP ${res.status}`);
      relationships = data?.relationships || [];
      anomalies = data?.anomalies || [];
      setShowcaseData(data?.rows || [], orderFields(data?.plan?.fields || [], data?.suggested_axes || []));
      metric = data?.suggested_axes?.[0] || data?.fields?.metrics?.[0] || metric;
      orderField = data?.fields?.time || orderField;
    } catch (e) { error = String(e?.message || e); setShowcaseData([], []); anomalies = []; }
    finally { loading = false; }
  }

  async function predict() {
    loading = true; error = ''; forecast = null;
    try {
      const res = await fetch('/ai-orchestrator/api/product/forecast/run', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({ schema:'showcase', table:'advertising', value_field:metric, order_field:orderField, horizon:Number(horizon||30), simulations:3000, target:target === '' ? undefined : Number(target) }) });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data?.details || data?.error || `HTTP ${res.status}`);
      forecast = data?.forecast || null;
    } catch (e) { error = String(e?.message || e); }
    finally { loading = false; }
  }
  onMount(load);
</script>

<section class="page">
  <header><small>Аналитика</small><h1>Зависимости и прогноз</h1><p>Реальная Gold-витрина → связи и аномалии → 3D → прогноз результата.</p></header>
  {#if loading}<p>Анализируем данные…</p>{:else if error}<p class="error">{error}</p>{/if}
  {#if relationships.length}<div class="relations">{#each relationships.slice(0,8) as item}<span><b>{item.x} ↔ {item.y}</b> {item.relation_type === 'monotonic' ? 'monotonic' : 'linear'} {Number(item.correlation).toFixed(2)}</span>{/each}</div>{/if}
  {#if anomalies.length}
    <section class="anomalies"><div><small>Что выбивается</small><h2>Аномальные объекты</h2></div><div class="anomaly-list">{#each anomalies.slice(0,8) as item}<article><b>{item.entity ?? `Строка ${item.row_index + 1}`}</b><strong>{Number(item.score).toFixed(1)}</strong><span>{item.findings?.slice(0,3).map((finding) => finding.field).join(', ')}</span></article>{/each}</div></section>
  {/if}
  <div class="graph"><GraphPanel /></div>
  <section class="forecast">
    <div><small>Что будет дальше</small><h2>Прогноз</h2><p>Модель выбирается системой. Сейчас встроен Monte Carlo по агрегированному временному ряду.</p></div>
    <form on:submit|preventDefault={predict}><label>Метрика<input bind:value={metric}/></label><label>Дата / порядок<input bind:value={orderField}/></label><label>Горизонт<input type="number" min="1" max="365" bind:value={horizon}/></label><label>Цель<input type="number" bind:value={target} placeholder="необязательно"/></label><button disabled={loading}>Построить</button></form>
    {#if forecast}<div class="cards"><article><small>Медиана</small><strong>{Number(forecast.final?.median||0).toLocaleString('ru-RU')}</strong></article><article><small>Диапазон 80%</small><strong>{Number(forecast.final?.p10||0).toLocaleString('ru-RU')} — {Number(forecast.final?.p90||0).toLocaleString('ru-RU')}</strong></article><article><small>Вероятность цели</small><strong>{forecast.final?.target_probability == null ? '—' : `${Math.round(forecast.final.target_probability*100)}%`}</strong></article><article><small>Тренд / шаг</small><strong>{Number(forecast.trend_per_step||0).toLocaleString('ru-RU')}</strong></article></div>{/if}
  </section>
</section>

<style>
  .page{max-width:1380px;margin:auto;padding:28px;color:#172033}header{max-width:760px}small{color:#94a3b8;font-size:9px;text-transform:uppercase;font-weight:800}h1{margin:6px 0;font-size:32px}h2{margin:5px 0;font-size:20px}p{color:#64748b;font-size:10px;line-height:1.55}.error{color:#b42318}.relations{display:flex;flex-wrap:wrap;gap:6px;margin:12px 0}.relations span{padding:7px 9px;background:#fff;border:1px solid #e4e9f1;border-radius:8px;color:#64748b;font-size:9px}.anomalies{margin:10px 0;padding:12px;background:#fff;border:1px solid #e4e9f1;border-radius:12px}.anomaly-list{display:grid;grid-template-columns:repeat(4,1fr);gap:7px;margin-top:8px}.anomaly-list article{display:grid;grid-template-columns:1fr auto;gap:4px;padding:9px;border:1px solid #edf0f4;border-radius:8px}.anomaly-list b{font-size:9px}.anomaly-list strong{font-size:13px}.anomaly-list span{grid-column:1/-1;color:#667085;font-size:8px}.graph{min-height:560px;background:#fff;border:1px solid #e4e9f1;border-radius:14px;padding:10px}.forecast{margin-top:12px;padding:14px;background:#fff;border:1px solid #e4e9f1;border-radius:14px}.forecast form{display:flex;gap:7px;align-items:end;flex-wrap:wrap}.forecast label{display:flex;flex-direction:column;gap:4px;color:#64748b;font-size:9px}.forecast input{border:1px solid #dfe5ed;border-radius:8px;padding:8px;font-size:10px}.forecast button{border:0;border-radius:8px;background:#172033;color:#fff;padding:9px 12px;font-size:10px}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-top:10px}.cards article{padding:11px;border:1px solid #edf0f4;border-radius:9px}.cards strong{display:block;margin-top:5px;font-size:16px}@media(max-width:900px){.anomaly-list,.cards{grid-template-columns:1fr 1fr}}@media(max-width:520px){.page{padding:20px 14px}.anomaly-list,.cards{grid-template-columns:1fr}}
</style>
