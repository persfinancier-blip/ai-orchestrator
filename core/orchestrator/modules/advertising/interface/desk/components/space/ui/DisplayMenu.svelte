<script lang="ts">
  import type { DatasetPreset, VisualScheme } from '../types';

  export let visualBg: string;
  export let visualEdge: string;
  export let pointColor: string;

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

  let bgPickerEl: HTMLInputElement | null = null;
  let edgePickerEl: HTMLInputElement | null = null;
  let pointPickerEl: HTMLInputElement | null = null;

  function openPicker(kind: 'bg' | 'edge' | 'point'): void {
    if (kind === 'bg') bgPickerEl?.click();
    if (kind === 'edge') edgePickerEl?.click();
    if (kind === 'point') pointPickerEl?.click();
  }

  function normalizeHex(input: string): string {
    const s = String(input ?? '').trim().toLowerCase();
    const m = s.match(/^#?([0-9a-f]{6})$/);
    if (m) return `#${m[1]}`;
    const m3 = s.match(/^#?([0-9a-f]{3})$/);
    if (m3) {
      const a = m3[1];
      return `#${a[0]}${a[0]}${a[1]}${a[1]}${a[2]}${a[2]}`;
    }
    return input; // если человек печатает — не ломаем ввод на лету
  }

  function onHex(kind: 'bg' | 'edge' | 'point', e: Event): void {
    const v = normalizeHex((e.currentTarget as HTMLInputElement).value);
    if (kind === 'bg') visualBg = v;
    if (kind === 'edge') visualEdge = v;
    if (kind === 'point') pointColor = v;
  }

  function onPicker(kind: 'bg' | 'edge' | 'point', e: Event): void {
    const v = (e.currentTarget as HTMLInputElement).value;
    if (kind === 'bg') visualBg = v;
    if (kind === 'edge') visualEdge = v;
    if (kind === 'point') pointColor = v;
  }
</script>

<div class="menu-pop display">
  <div class="menu-title">Визуал</div>

  <div class="row">
    <div class="label">Фон</div>

    <div class="color-row">
      <button class="swatch-btn" type="button" on:click={() => openPicker('bg')} aria-label="Выбрать цвет фона">
        <span class="swatch" style={`background:${visualBg};`} />
      </button>

      <input
        bind:this={bgPickerEl}
        class="color-hidden"
        type="color"
        value={visualBg}
        on:input={(e) => onPicker('bg', e)}
        tabindex="-1"
        aria-hidden="true"
      />

      <input class="hex" spellcheck="false" value={visualBg} on:input={(e) => onHex('bg', e)} />
    </div>
  </div>

  <div class="row">
    <div class="label">Рёбра</div>

    <div class="color-row">
      <button class="swatch-btn" type="button" on:click={() => openPicker('edge')} aria-label="Выбрать цвет рёбер">
        <span class="swatch" style={`background:${visualEdge};`} />
      </button>

      <input
        bind:this={edgePickerEl}
        class="color-hidden"
        type="color"
        value={visualEdge}
        on:input={(e) => onPicker('edge', e)}
        tabindex="-1"
        aria-hidden="true"
      />

      <input class="hex" spellcheck="false" value={visualEdge} on:input={(e) => onHex('edge', e)} />
    </div>
  </div>

  <div class="row">
    <div class="label">Точки</div>

    <div class="color-row">
      <button class="swatch-btn" type="button" on:click={() => openPicker('point')} aria-label="Выбрать цвет точек">
        <span class="swatch" style={`background:${pointColor};`} />
      </button>

      <input
        bind:this={pointPickerEl}
        class="color-hidden"
        type="color"
        value={pointColor}
        on:input={(e) => onPicker('point', e)}
        tabindex="-1"
        aria-hidden="true"
      />

      <input class="hex" spellcheck="false" value={pointColor} on:input={(e) => onHex('point', e)} />
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

<style>
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
    border-radius: 10px;
    box-shadow: 0 10px 26px rgba(15, 23, 42, 0.10);
    border: 1px solid rgba(15, 23, 42, 0.08);
  }

  .color-hidden {
    position: absolute;
    width: 1px;
    height: 1px;
    opacity: 0;
    pointer-events: none;
  }
</style>
