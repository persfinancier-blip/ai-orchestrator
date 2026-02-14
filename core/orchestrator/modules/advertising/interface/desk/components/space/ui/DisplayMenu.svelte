<script lang="ts">
  export let visualBg: string;
  export let visualEdge: string;
  export let selectedVisualId: string;
  export let selectedDatasetPresetId: string;

  export let visualSchemes: Array<{ id: string; name: string }>;
  export let datasetPresets: Array<{ id: string; name: string }>;

  export let onResetVisual: () => void;
  export let onOpenSaveVisual: () => void;

  export let onClearAllDataSelection: () => void;
  export let onOpenSaveDataset: () => void;

  export let onApplyVisual: (id: string) => void;
  export let onApplyDataset: (id: string) => void;

  export let onResetView: () => void;
</script>

<div class="menu-pop display">
  <div class="menu-title">Визуал</div>

  <div class="row">
    <div class="label">Фон</div>
    <div class="color-row">
      <input class="color-input" type="color" bind:value={visualBg} />
      <input class="hex" spellcheck="false" bind:value={visualBg} />
    </div>
  </div>

  <div class="row">
    <div class="label">Рёбра</div>
    <div class="color-row">
      <input class="color-input" type="color" bind:value={visualEdge} />
      <input class="hex" spellcheck="false" bind:value={visualEdge} />
    </div>
  </div>

  <div class="sep" />

  <div class="row two">
    <button class="btn" on:click={onResetVisual}>Сбросить</button>
    <button class="btn btn-primary" on:click={onOpenSaveVisual}>Сохранить схему</button>
  </div>

  <div class="row">
    <select class="select" bind:value={selectedVisualId} on:change={() => onApplyVisual(selectedVisualId)}>
      <option value="">Выберите схему</option>
      {#each visualSchemes as v}
        <option value={v.id}>{v.name}</option>
      {/each}
    </select>
  </div>

  <div class="sep" />

  <div class="menu-title">Набор данных</div>
  <div class="row two">
    <button class="btn" on:click={onClearAllDataSelection}>Заводские</button>
    <button class="btn btn-primary" on:click={onOpenSaveDataset}>Сохранить набор</button>
  </div>

  <div class="row">
    <select class="select" bind:value={selectedDatasetPresetId} on:change={() => onApplyDataset(selectedDatasetPresetId)}>
      <option value="">Выберите набор</option>
      {#each datasetPresets as p}
        <option value={p.id}>{p.name}</option>
      {/each}
    </select>
  </div>

  <div class="row">
    <button class="btn wide" on:click={onResetView}>Сбросить камеру</button>
  </div>
</div>
