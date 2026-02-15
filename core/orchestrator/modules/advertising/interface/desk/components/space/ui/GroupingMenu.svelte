<!-- core/orchestrator/modules/advertising/interface/desk/components/space/ui/GroupingMenu.svelte -->
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
    { id: 'fixed', name: 'Фикс' },
    { id: 'manual', name: 'Вручную' }
  ];

  $: canPickFields = cfg.principle !== 'proximity';

  function toggleFeatureField(code: string): void {
    if (!code) return;

    if (cfg.featureFields.includes(code)) {
      cfg.featureFields = cfg.featureFields.filter((x) => x !== code);
      return;
    }

    if (cfg.featureFields.length >= 3) return;
    cfg.featureFields = [...cfg.featureFields, code];
  }
</script>

<div class="menu">
  <div class="head">
    <div class="title">Группировка</div>
  </div>

  <div class="row">
    <label style="display:flex; gap:10px; align-items:center; pointer-events:auto;">
      <input type="checkbox" bind:checked={cfg.enabled} disabled />
      <span style="font-size:12px; font-weight:650;">Включить группировку</span>
    </label>
  </div>

  <div class="sep" />

  <div class="sub">Принцип объединения</div>
  <div class="chips">
    {#each principles as p}
      <button
        type="button"
        class="chip {cfg.principle === p.id ? 'active' : ''}"
        on:click={() => (cfg.principle = p.id)}
      >
        {p.name}
      </button>
    {/each}
  </div>

  <div class="sep" />

  <div class="sub">Детализация групп (0 = без группировки)</div>
  <div class="row">
    <input class="input" type="range" min="0" max="1" step="0.01" bind:value={cfg.detail} />
  </div>

  {#if canPickFields}
    <div class="sub">
      Признаки для похожести (до 3)
      <span class="hint">({cfg.featureFields.length}/3)</span>
    </div>

    <div class="list" style="pointer-events:auto;">
      {#each (cfg.principle === 'behavior' ? textFields : numberFields) as f}
        <label class="item">
          <input type="checkbox" checked={cfg.featureFields.includes(f.code)} on:change={() => toggleFeatureField(f.code)} />
          <span>{f.name}</span>
        </label>
      {/each}
    </div>
  {/if}

  <div class="sep" />

  <div class="sub">Размер кластера (min)</div>
  <div class="row">
    <input class="input" type="number" min="2" max="999" step="1" bind:value={cfg.minClusterSize} />
  </div>

  <div class="sep" />

  <div class="sub">Режим пересчёта</div>
  <div class="chips">
    {#each recomputeModes as m}
      <button
        type="button"
        class="chip {cfg.recompute === m.id ? 'active' : ''}"
        on:click={() => (cfg.recompute = m.id)}
      >
        {m.name}
      </button>
    {/each}
  </div>

  <div class="sep" />

  <div class="row" style="justify-content:flex-end;">
    <button type="button" class="btn" on:click={onRecompute}>Пересчитать</button>
  </div>
</div>

<style>
  .menu {
    position: absolute;
    right: 12px;
    top: 52px;
    width: 320px;
    background: rgba(255, 255, 255, 0.98);
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 12px;
    box-shadow: 0 10px 30px rgba(2, 6, 23, 0.10);
    z-index: 10;
  }
  .head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }
  .title { font-size: 13px; font-weight: 800; }
  .row { display: flex; gap: 10px; align-items: center; margin: 8px 0; }
  .sub { font-size: 12px; font-weight: 700; margin-top: 8px; }
  .hint { font-size: 11px; opacity: 0.7; margin-left: 6px; }
  .sep { height: 1px; background: #e2e8f0; margin: 10px 0; }
  .chips { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 8px; }
  .chip {
    padding: 6px 10px;
    border-radius: 999px;
    border: 1px solid #e2e8f0;
    background: #fff;
    cursor: pointer;
    font-size: 12px;
  }
  .chip.active { border-color: #94a3b8; background: #f1f5f9; }
  .input { width: 100%; }
  .list { display: flex; flex-direction: column; gap: 6px; margin-top: 8px; max-height: 160px; overflow: auto; }
  .item { display: flex; align-items: center; gap: 8px; font-size: 12px; }
  .btn { padding: 8px 12px; border-radius: 10px; border: 1px solid #e2e8f0; background: #fff; cursor: pointer; }
</style>
