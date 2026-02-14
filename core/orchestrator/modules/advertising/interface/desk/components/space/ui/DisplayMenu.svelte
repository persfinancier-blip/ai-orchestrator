<script lang="ts">
  import type { DatasetPreset, VisualScheme } from '../types';
  import ColorPickerPopover from './ColorPickerPopover.svelte';

  export let visualBg: string;
  export let visualEdge: string;

  export let visualSchemes: VisualScheme[] = [];
  export let selectedVisualId = '';

  export let datasetPresets: DatasetPreset[] = [];
  export let selectedDatasetPresetId = '';

  export let onResetVisual: () => void;
  export let onOpenSaveVisual: () => void;

  export let onClearAllDataSelection: () => void;
  export let onOpenSaveDataset: () => void;

  export let onApplyVisual: (id: string) => void;
  export let onApplyDataset: (id: string) => void;

  export let onResetView: () => void;

  type PickerKind = 'bg' | 'cube' | null;
  let picker: PickerKind = null;

  function open(k: PickerKind): void {
    picker = k;
  }

  function close(): void {
    picker = null;
  }
</script>

<div class="menu-pop display">
  <div class="menu-title">Визуал</div>

  <div class="row">
    <div class="label">Фон</div>
    <div class="color-row">
      <button class="swatch-btn" type="button" on:click={() => open('bg')} aria-label="Выбрать цвет фона">
        <span class="swatch" style={`background:${visualBg};`} />
      </button>
      <input class="hex" spellcheck="false" bind:value={visualBg} />
    </div>

    {#if picker === 'bg'}
      <div class="picker-slot">
        <ColorPickerPopover title="Цвет фона" bind:value={visualBg} onClose={close} />
      </div>
    {/if}
  </div>

  <div class="row">
    <div class="label">Куб</div>
    <div class="color-row">
      <button class="swatch-btn" type="button" on:click={() => open('cube')} aria-label="Выбрать цвет куба">
        <span class="swatch" style={`background:${visualEdge};`} />
      </button>
      <input class="hex" spellcheck="false" bind:value={visualEdge} />
    </div>

    {#if picker === 'cube'}
      <div class="picker-slot">
        <ColorPickerPopover title="Цвет куба" bind:value={visualEdge} onClose={close} />
      </div>
    {/if}
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

<style>
  .row { position: relative; }

  .color-row {
    display: flex;
    gap: 10px;
    align-items: center;
    width: 100%;
  }

  .swatch-btn {
    border: 0;
    padding: 0;
    background: transparent;
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    justify-content: center;
  }

  .swatch {
    width: 34px;
    height: 34px;
    border-radius: 12px;
    box-shadow: 0 12px 30px rgba(15, 23, 42, 0.12);
    border: 1px solid rgba(15, 23, 42, 0.08);
  }

  .picker-slot {
    position: absolute;
    right: 0;
    top: 46px;
    width: 0;
    height: 0;
  }
</style>
