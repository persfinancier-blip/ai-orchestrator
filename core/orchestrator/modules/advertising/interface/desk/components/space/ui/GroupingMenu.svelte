<script lang="ts">
  import type { ShowcaseField } from '../../data/showcaseStore';
  import type { GroupingConfig, GroupingPrinciple, RecomputeMode } from '../cluster/types';

  export let cfg: GroupingConfig;

  export let numberFields: ShowcaseField[] = [];
  export let textFields: ShowcaseField[] = [];

  export let onRecompute: () => void;

  const principles: Array<{ id: GroupingPrinciple; name: string }> = [
    { id: 'efficiency', name: 'По эффективности' },
    { id: 'behavior', name: 'По поведению' },
    { id: 'proximity', name: 'По близости' }
  ];

  const recomputeModes: Array<{ id: RecomputeMode; name: string }> = [
    { id: 'auto', name: 'Авто' },
    { id: 'fixed', name: 'Фиксировать группы' },
    { id: 'manual', name: 'Ручной' }
  ];

  $: availableFields = cfg.principle === 'efficiency' ? numberFields : cfg.principle === 'behavior' ? textFields : [];
  $: canPickFields = cfg.principle !== 'proximity';

  function toggleField(code: string): void {
    if (!canPickFields) return;

    const cur = new Set(cfg.featureFields);
    if (cur.has(code)) cur.delete(code);
    else {
      if (cur.size >= 3) return; // ✅ ограничение до 3
      cur.add(code);
    }
    cfg = { ...cfg, featureFields: [...cur] };
  }

  function setPrinciple(p: GroupingPrinciple): void {
    cfg = { ...cfg, principle: p, featureFields: [] };
  }
</script>

<div class="menu-pop pick" style="top: 56px; right: 368px; width: 340px;">
  <div class="menu-title">Формирование групп</div>

  <div class="row">
    <label style="display:flex; gap:10px; align-items:center; pointer-events:auto;">
      <input type="checkbox" bind:checked={cfg.enabled} />
      <span style="font-size:12px; font-weight:650;">Включить группировку</span>
    </label>
  </div>

  <div class="sep" />

  <div class="sub">Принцип объединения</div>
  <div class="row" style="flex-wrap:wrap;">
    {#each principles as p}
      <button
        class="btn"
        style={`padding:8px 12px; opacity:${cfg.principle === p.id ? 1 : 0.65};`}
        on:click={() => setPrinciple(p.id)}
      >
        {p.name}
      </button>
    {/each}
  </div>

  <div class="sub">Детализация групп</div>
  <div class="row">
    <input class="input" type="range" min="0" max="1" step="0.01" bind:value={cfg.detail} />
  </div>

  {#if canPickFields}
    <div class="sub">
      Признаки для похожести (до 3)
      <span class="hint">({cfg.featureFields.length}/3)</span>
    </div>

    <div class="list">
      {#each availableFields as f (f.code)}
        <button class="item" on:click={() => toggleField(f.code)} style="opacity:1;">
          <span class="name">{f.name}</span>
          <span class="tag">{cfg.featureFields.includes(f.code) ? 'выбрано' : ' '}</span>
        </button>
      {/each}
    </div>
  {/if}

  {#if cfg.principle === 'proximity'}
    <div class="sub">Приоритет измерений</div>
    <div class="row">
      <label style="display:flex; gap:10px; align-items:center; pointer-events:auto;">
        <input type="checkbox" bind:checked={cfg.customWeights} />
        <span style="font-size:12px;">Свой режим</span>
      </label>
    </div>

    {#if cfg.customWeights}
      <div class="row two">
        <input class="input" type="number" min="0.2" max="5" step="0.1" bind:value={cfg.wX} />
        <input class="input" type="number" min="0.2" max="5" step="0.1" bind:value={cfg.wY} />
      </div>
      <div class="row">
        <input class="input" type="number" min="0.2" max="5" step="0.1" bind:value={cfg.wZ} />
      </div>
    {/if}
  {/if}

  <div class="sub">Минимальный размер группы</div>
  <div class="row">
    <input class="input" type="number" min="1" step="1" bind:value={cfg.minClusterSize} />
  </div>

  <div class="sub">Режим пересчёта</div>
  <div class="row">
    <select class="select" bind:value={cfg.recompute}>
      {#each recomputeModes as m}
        <option value={m.id}>{m.name}</option>
      {/each}
    </select>
  </div>

  {#if cfg.recompute === 'manual'}
    <div class="row">
      <button class="btn btn-primary wide" on:click={onRecompute}>Пересчитать группы</button>
    </div>
  {/if}
</div>
