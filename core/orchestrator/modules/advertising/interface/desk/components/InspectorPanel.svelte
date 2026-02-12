<script lang="ts">
  import type { SimilarityEntity } from '../data/mockGraph';

  export let selectedEntity: SimilarityEntity | null = null;
  export let nearest: SimilarityEntity[] = [];
  export let farthest: SimilarityEntity[] = [];
  export let onCreateCloud: () => void = () => {};
</script>

<aside class="panel inspector">
  {#if selectedEntity}
    <h3>{selectedEntity.name}</h3>
    <p class="muted">{selectedEntity.type === 'item' ? 'Товар' : 'Рекламная кампания'} · {selectedEntity.id}</p>

    <div class="grid">
      <div>Расход <strong>{selectedEntity.metrics.spend.toLocaleString('ru-RU')} ₽</strong></div>
      <div>Выручка <strong>{selectedEntity.metrics.sales.toLocaleString('ru-RU')} ₽</strong></div>
      <div>Заказы <strong>{selectedEntity.metrics.orders}</strong></div>
      <div>ДРР <strong>{selectedEntity.metrics.DRR}%</strong></div>
      <div>ROI <strong>{selectedEntity.metrics.ROI}</strong></div>
      <div>CR2 <strong>{selectedEntity.metrics.CR2}%</strong></div>
    </div>

    {#if selectedEntity.type === 'item'}
      <h4>Топ-10 ближайших РК</h4>
      <ul>{#each nearest as n}<li>{n.name}</li>{/each}</ul>
      <h4>Топ-10 дальних РК</h4>
      <ul>{#each farthest as n}<li>{n.name}</li>{/each}</ul>
    {:else}
      <div class="campaign-hit">
        <div>Попадание <strong>{selectedEntity.matchScore ?? 0}/100</strong></div>
        <div>Куда попали <strong>{selectedEntity.targetCluster ?? selectedEntity.clusterName}</strong></div>
      </div>
      <h4>Топ-10 ближайших товаров</h4>
      <ul>{#each nearest as n}<li>{n.name}</li>{/each}</ul>
    {/if}

    <button on:click={onCreateCloud}>Создать облако данных из выделения</button>
  {:else}
    <h3>Инспектор</h3>
    <p class="muted">Нажмите точку на карте сходства, чтобы увидеть карточку и списки близости.</p>
  {/if}
</aside>

<style>
  .inspector { min-height: 540px; }
  .muted { color:#64748b; font-size:12px; }
  .grid { display:grid; grid-template-columns:1fr 1fr; gap:8px; font-size:12px; margin:12px 0; }
  h4 { margin:8px 0 6px; font-size:12px; color:#64748b; }
  ul { margin:0; padding-left:16px; font-size:12px; color:#334155; max-height:130px; overflow:auto; }
  button { margin-top:10px; border:1px solid #cbd5e1; background:#fff; border-radius:10px; padding:8px 10px; }
  .campaign-hit { margin-top:10px; padding:8px; border:1px solid #e2e8f0; border-radius:10px; font-size:12px; color:#334155; }
</style>
