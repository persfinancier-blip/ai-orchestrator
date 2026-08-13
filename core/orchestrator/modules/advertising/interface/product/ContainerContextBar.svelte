<script>
  import { onDestroy, onMount } from 'svelte';
  import { productGet, productPost } from './productApi.js';

  let containers = [];
  let selected = '';
  let loading = false;
  let error = '';

  async function load() {
    loading = true;
    error = '';
    try {
      const [list, context] = await Promise.all([
        productGet('/product/containers'),
        productGet('/context/container').catch(() => ({ container_id: null }))
      ]);
      containers = Array.isArray(list?.containers) ? list.containers : [];
      selected = context?.container_id ? String(context.container_id) : '';
      if (!selected && containers.length === 1) {
        await choose(String(containers[0].id));
      }
    } catch (e) {
      error = String(e?.message || e || 'Не удалось загрузить контейнеры');
    } finally {
      loading = false;
    }
  }

  async function choose(value) {
    const id = Number(value || 0);
    selected = id > 0 ? String(id) : '';
    if (!(id > 0)) return;
    try {
      await productPost('/context/container', { container_id: id });
      window.dispatchEvent(new CustomEvent('ao:container-context-changed', { detail: { container_id: id } }));
    } catch (e) {
      error = String(e?.message || e || 'Не удалось выбрать контейнер');
    }
  }

  const refresh = () => load();
  onMount(() => {
    load();
    window.addEventListener('ao:container-context-refresh', refresh);
  });
  onDestroy(() => window.removeEventListener('ao:container-context-refresh', refresh));
</script>

<div class="bar" title={error || 'Рабочий контейнер'}>
  <span>Контейнер</span>
  <select value={selected} on:change={(e) => choose(e.currentTarget.value)} disabled={loading || !containers.length}>
    {#if !selected}<option value="">{loading ? 'Загрузка…' : 'Не выбран'}</option>{/if}
    {#each containers as item (item.id)}
      <option value={String(item.id)}>{item.name}</option>
    {/each}
  </select>
  <a href="#containers" aria-label="Открыть контейнеры">↗</a>
</div>

<style>
  .bar { display:flex; align-items:center; gap:6px; min-width:0; }
  span { color:#94a3b8; font-size:9px; text-transform:uppercase; letter-spacing:.08em; font-weight:800; }
  select { max-width:220px; min-width:120px; border:1px solid #dfe5ed; border-radius:8px; padding:6px 8px; background:#fff; color:#172033; font-size:10px; }
  a { display:grid; place-items:center; width:26px; height:26px; border:1px solid #dfe5ed; border-radius:8px; color:#64748b; text-decoration:none; font-size:11px; }
  @media (max-width:760px) { span { display:none; } select { max-width:140px; } }
</style>
