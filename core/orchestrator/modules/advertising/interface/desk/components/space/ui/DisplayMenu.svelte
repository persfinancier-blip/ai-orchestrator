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

  type Rgb = { r: number; g: number; b: number };

  function clampByte(n: number): number {
    if (!Number.isFinite(n)) return 0;
    return Math.max(0, Math.min(255, Math.round(n)));
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
    return '#000000';
  }

  function hexToRgb(hex: string): Rgb {
    const h = normalizeHex(hex).slice(1);
    const r = parseInt(h.slice(0, 2), 16);
    const g = parseInt(h.slice(2, 4), 16);
    const b = parseInt(h.slice(4, 6), 16);
    return { r: clampByte(r), g: clampByte(g), b: clampByte(b) };
  }

  function rgbToHex(rgb: Rgb): string {
    const r = clampByte(rgb.r).toString(16).padStart(2, '0');
    const g = clampByte(rgb.g).toString(16).padStart(2, '0');
    const b = clampByte(rgb.b).toString(16).padStart(2, '0');
    return `#${r}${g}${b}`;
  }

  function makeGradient(channel: 'r' | 'g' | 'b', rgb: Rgb): string {
    const a: Rgb = { ...rgb, [channel]: 0 } as Rgb;
    const z: Rgb = { ...rgb, [channel]: 255 } as Rgb;
    return `linear-gradient(90deg, ${rgbToHex(a)}, ${rgbToHex(z)})`;
  }

  // ---- local state (so sliders feel smooth)
  let bgRgb: Rgb = hexToRgb(visualBg);
  let edgeRgb: Rgb = hexToRgb(visualEdge);
  let pointRgb: Rgb = hexToRgb(pointColor);

  // keep in sync if parent changes (apply scheme/reset)
  $: bgRgb = hexToRgb(visualBg);
  $: edgeRgb = hexToRgb(visualEdge);
  $: pointRgb = hexToRgb(pointColor);

  function setColorFromRgb(kind: 'bg' | 'edge' | 'point', next: Rgb): void {
    const hex = rgbToHex(next);
    if (kind === 'bg') visualBg = hex;
    if (kind === 'edge') visualEdge = hex;
    if (kind === 'point') pointColor = hex;
  }

  function setColorFromHex(kind: 'bg' | 'edge' | 'point', hex: string): void {
    const normalized = normalizeHex(hex);
    if (kind === 'bg') visualBg = normalized;
    if (kind === 'edge') visualEdge = normalized;
    if (kind === 'point') pointColor = normalized;
  }
</script>

<div class="menu-pop display">
  <div class="menu-title">Визуал</div>

  <!-- Фон -->
  <div class="color-block">
    <div class="row">
      <div class="label">Фон</div>
      <div class="color-head">
        <div class="swatch" style={`background:${visualBg};`} />
        <input class="hex" spellcheck="false" value={visualBg} on:input={(e) => setColorFromHex('bg', (e.target as HTMLInputElement).value)} />
      </div>
    </div>

    <div class="rgb">
      <div class="rgb-row">
        <div class="c">R</div>
        <input
          class="slider"
          style={`background:${makeGradient('r', bgRgb)};`}
          type="range"
          min="0"
          max="255"
          value={bgRgb.r}
          on:input={(e) => setColorFromRgb('bg', { ...bgRgb, r: Number((e.target as HTMLInputElement).value) })}
        />
        <div class="v">{bgRgb.r}</div>
      </div>
      <div class="rgb-row">
        <div class="c">G</div>
        <input
          class="slider"
          style={`background:${makeGradient('g', bgRgb)};`}
          type="range"
          min="0"
          max="255"
          value={bgRgb.g}
          on:input={(e) => setColorFromRgb('bg', { ...bgRgb, g: Number((e.target as HTMLInputElement).value) })}
        />
        <div class="v">{bgRgb.g}</div>
      </div>
      <div class="rgb-row">
        <div class="c">B</div>
        <input
          class="slider"
          style={`background:${makeGradient('b', bgRgb)};`}
          type="range"
          min="0"
          max="255"
          value={bgRgb.b}
          on:input={(e) => setColorFromRgb('bg', { ...bgRgb, b: Number((e.target as HTMLInputElement).value) })}
        />
        <div class="v">{bgRgb.b}</div>
      </div>
    </div>
  </div>

  <!-- Рёбра -->
  <div class="color-block">
    <div class="row">
      <div class="label">Рёбра</div>
      <div class="color-head">
        <div class="swatch" style={`background:${visualEdge};`} />
        <input class="hex" spellcheck="false" value={visualEdge} on:input={(e) => setColorFromHex('edge', (e.target as HTMLInputElement).value)} />
      </div>
    </div>

    <div class="rgb">
      <div class="rgb-row">
        <div class="c">R</div>
        <input
          class="slider"
          style={`background:${makeGradient('r', edgeRgb)};`}
          type="range"
          min="0"
          max="255"
          value={edgeRgb.r}
          on:input={(e) => setColorFromRgb('edge', { ...edgeRgb, r: Number((e.target as HTMLInputElement).value) })}
        />
        <div class="v">{edgeRgb.r}</div>
      </div>
      <div class="rgb-row">
        <div class="c">G</div>
        <input
          class="slider"
          style={`background:${makeGradient('g', edgeRgb)};`}
          type="range"
          min="0"
          max="255"
          value={edgeRgb.g}
          on:input={(e) => setColorFromRgb('edge', { ...edgeRgb, g: Number((e.target as HTMLInputElement).value) })}
        />
        <div class="v">{edgeRgb.g}</div>
      </div>
      <div class="rgb-row">
        <div class="c">B</div>
        <input
          class="slider"
          style={`background:${makeGradient('b', edgeRgb)};`}
          type="range"
          min="0"
          max="255"
          value={edgeRgb.b}
          on:input={(e) => setColorFromRgb('edge', { ...edgeRgb, b: Number((e.target as HTMLInputElement).value) })}
        />
        <div class="v">{edgeRgb.b}</div>
      </div>
    </div>
  </div>

  <!-- Точки -->
  <div class="color-block">
    <div class="row">
      <div class="label">Точки</div>
      <div class="color-head">
        <div class="swatch" style={`background:${pointColor};`} />
        <input class="hex" spellcheck="false" value={pointColor} on:input={(e) => setColorFromHex('point', (e.target as HTMLInputElement).value)} />
      </div>
    </div>

    <div class="rgb">
      <div class="rgb-row">
        <div class="c">R</div>
        <input
          class="slider"
          style={`background:${makeGradient('r', pointRgb)};`}
          type="range"
          min="0"
          max="255"
          value={pointRgb.r}
          on:input={(e) => setColorFromRgb('point', { ...pointRgb, r: Number((e.target as HTMLInputElement).value) })}
        />
        <div class="v">{pointRgb.r}</div>
      </div>
      <div class="rgb-row">
        <div class="c">G</div>
        <input
          class="slider"
          style={`background:${makeGradient('g', pointRgb)};`}
          type="range"
          min="0"
          max="255"
          value={pointRgb.g}
          on:input={(e) => setColorFromRgb('point', { ...pointRgb, g: Number((e.target as HTMLInputElement).value) })}
        />
        <div class="v">{pointRgb.g}</div>
      </div>
      <div class="rgb-row">
        <div class="c">B</div>
        <input
          class="slider"
          style={`background:${makeGradient('b', pointRgb)};`}
          type="range"
          min="0"
          max="255"
          value={pointRgb.b}
          on:input={(e) => setColorFromRgb('point', { ...pointRgb, b: Number((e.target as HTMLInputElement).value) })}
        />
        <div class="v">{pointRgb.b}</div>
      </div>
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
  .color-block {
    margin-top: 10px;
    padding: 10px;
    border-radius: 14px;
    background: rgba(248, 251, 255, 0.65);
  }

  .color-head {
    display: flex;
    align-items: center;
    gap: 10px;
    width: 100%;
  }

  .swatch {
    width: 22px;
    height: 22px;
    border-radius: 999px;
    box-shadow: 0 10px 22px rgba(15, 23, 42, 0.12);
    border: 1px solid rgba(15, 23, 42, 0.08);
    flex: 0 0 auto;
  }

  .rgb {
    margin-top: 8px;
    display: flex;
    flex-direction: column;
    gap: 8px;
  }

  .rgb-row {
    display: grid;
    grid-template-columns: 16px 1fr 34px;
    align-items: center;
    gap: 10px;
  }

  .c {
    font-size: 11px;
    font-weight: 800;
    color: rgba(15, 23, 42, 0.75);
  }

  .v {
    font-size: 11px;
    font-weight: 700;
    color: rgba(15, 23, 42, 0.75);
    text-align: right;
  }

  .slider {
    -webkit-appearance: none;
    appearance: none;
    height: 10px;
    border-radius: 999px;
    outline: none;
    border: 1px solid rgba(15, 23, 42, 0.06);
    box-shadow: inset 0 1px 2px rgba(15, 23, 42, 0.06);
  }

  .slider::-webkit-slider-thumb {
    -webkit-appearance: none;
    appearance: none;
    width: 16px;
    height: 16px;
    border-radius: 999px;
    background: rgba(255, 255, 255, 0.95);
    border: 1px solid rgba(15, 23, 42, 0.15);
    box-shadow: 0 10px 22px rgba(15, 23, 42, 0.18);
    cursor: pointer;
  }

  .slider::-moz-range-thumb {
    width: 16px;
    height: 16px;
    border-radius: 999px;
    background: rgba(255, 255, 255, 0.95);
    border: 1px solid rgba(15, 23, 42, 0.15);
    box-shadow: 0 10px 22px rgba(15, 23, 42, 0.18);
    cursor: pointer;
  }
</style>
