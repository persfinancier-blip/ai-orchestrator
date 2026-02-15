<script lang="ts">
  import { onDestroy, tick } from 'svelte';

  export let value: string; // hex #rrggbb
  export let title: string;
  export let onClose: () => void;

  let rootEl: HTMLDivElement | null = null;
  let svEl: HTMLDivElement | null = null;

  type Hsv = { h: number; s: number; v: number };

  function clamp(n: number, a: number, b: number): number {
    return Math.max(a, Math.min(b, n));
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
    return value;
  }

  function hexToRgb(hex: string): { r: number; g: number; b: number } {
    const h = normalizeHex(hex).slice(1);
    const r = parseInt(h.slice(0, 2), 16);
    const g = parseInt(h.slice(2, 4), 16);
    const b = parseInt(h.slice(4, 6), 16);
    return { r, g, b };
  }

  function rgbToHex(r: number, g: number, b: number): string {
    const to = (x: number) => clamp(Math.round(x), 0, 255).toString(16).padStart(2, '0');
    return `#${to(r)}${to(g)}${to(b)}`;
  }

  function rgbToHsv(r: number, g: number, b: number): Hsv {
    const rr = r / 255;
    const gg = g / 255;
    const bb = b / 255;

    const max = Math.max(rr, gg, bb);
    const min = Math.min(rr, gg, bb);
    const d = max - min;

    let h = 0;
    if (d !== 0) {
      if (max === rr) h = ((gg - bb) / d) % 6;
      else if (max === gg) h = (bb - rr) / d + 2;
      else h = (rr - gg) / d + 4;
      h *= 60;
      if (h < 0) h += 360;
    }

    const s = max === 0 ? 0 : d / max;
    const v = max;

    return { h, s, v };
  }

  function hsvToRgb(h: number, s: number, v: number): { r: number; g: number; b: number } {
    const hh = ((h % 360) + 360) % 360;
    const c = v * s;
    const x = c * (1 - Math.abs(((hh / 60) % 2) - 1));
    const m = v - c;

    let r1 = 0;
    let g1 = 0;
    let b1 = 0;

    if (hh < 60) [r1, g1, b1] = [c, x, 0];
    else if (hh < 120) [r1, g1, b1] = [x, c, 0];
    else if (hh < 180) [r1, g1, b1] = [0, c, x];
    else if (hh < 240) [r1, g1, b1] = [0, x, c];
    else if (hh < 300) [r1, g1, b1] = [x, 0, c];
    else [r1, g1, b1] = [c, 0, x];

    return { r: (r1 + m) * 255, g: (g1 + m) * 255, b: (b1 + m) * 255 };
  }

  let hsv: Hsv = { h: 0, s: 1, v: 1 };

  $: {
    const { r, g, b } = hexToRgb(value);
    hsv = rgbToHsv(r, g, b);
  }

  function hueHex(h: number): string {
    const { r, g, b } = hsvToRgb(h, 1, 1);
    return rgbToHex(r, g, b);
  }

  function setFromSvByPointer(clientX: number, clientY: number): void {
    if (!svEl) return;
    const rect = svEl.getBoundingClientRect();
    const x = clamp((clientX - rect.left) / rect.width, 0, 1);
    const y = clamp((clientY - rect.top) / rect.height, 0, 1);

    const s = x;
    const v = 1 - y;

    const { r, g, b } = hsvToRgb(hsv.h, s, v);
    value = rgbToHex(r, g, b);
  }

  function onSvDown(e: PointerEvent): void {
    (e.currentTarget as HTMLElement).setPointerCapture(e.pointerId);
    setFromSvByPointer(e.clientX, e.clientY);
  }

  function onSvMove(e: PointerEvent): void {
    if (!(e.buttons & 1)) return;
    setFromSvByPointer(e.clientX, e.clientY);
  }

  function onHue(e: Event): void {
    const h = Number((e.currentTarget as HTMLInputElement).value);
    const { r, g, b } = hsvToRgb(h, hsv.s, hsv.v);
    value = rgbToHex(r, g, b);
  }

  function onHex(e: Event): void {
    value = normalizeHex((e.currentTarget as HTMLInputElement).value);
  }

  function onDocDown(e: MouseEvent): void {
    if (!rootEl) return;
    const t = e.target as Node | null;
    if (!t) return;
    if (!rootEl.contains(t)) onClose();
  }

  function onKey(e: KeyboardEvent): void {
    if (e.key === 'Escape') onClose();
  }

  (async () => {
    await tick();
    document.addEventListener('mousedown', onDocDown, true);
    document.addEventListener('keydown', onKey);
  })();

  onDestroy(() => {
    document.removeEventListener('mousedown', onDocDown, true);
    document.removeEventListener('keydown', onKey);
  });
</script>

<div class="picker" bind:this={rootEl}>
  <div class="head">
    <div class="t">{title}</div>
    <button class="x" type="button" on:click={onClose}>×</button>
  </div>

  <div class="body">
    <div class="sv" bind:this={svEl} style={`--h:${hsv.h};`} on:pointerdown={onSvDown} on:pointermove={onSvMove}>
      <div class="sv-white" />
      <div class="sv-black" />
      <div class="thumb" style={`left:${hsv.s * 100}%; top:${(1 - hsv.v) * 100}%;`} />
    </div>

    <div class="right">
      <div class="preview" style={`background:${value};`} />
      <input class="hex" spellcheck="false" value={value} on:input={onHex} />

      <div class="hue-wrap">
        <div class="hue-label">Тон</div>
        <div class="hue-pad">
          <input
            class="hue"
            type="range"
            min="0"
            max="360"
            step="1"
            value={hsv.h}
            on:input={onHue}
            style={`background:linear-gradient(90deg,
              ${hueHex(0)}, ${hueHex(60)}, ${hueHex(120)}, ${hueHex(180)}, ${hueHex(240)}, ${hueHex(300)}, ${hueHex(360)}
            );`}
          />
        </div>
      </div>
    </div>
  </div>
</div>

<style>
  .picker {
    position: absolute;
    top: 52px;
    right: 0;
    width: 320px;
    padding: 12px;
    border-radius: 18px;
    background: rgba(255, 255, 255, 0.94);
    box-shadow: 0 22px 60px rgba(15, 23, 42, 0.18);
    backdrop-filter: blur(14px);
    z-index: 30;
    box-sizing: border-box;
  }

  .head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 10px;
  }

  .t {
    font-size: 13px;
    font-weight: 800;
    color: rgba(15, 23, 42, 0.92);
  }

  .x {
    border: 0;
    width: 28px;
    height: 28px;
    border-radius: 10px;
    background: rgba(15, 23, 42, 0.06);
    cursor: pointer;
    font-weight: 900;
    color: rgba(15, 23, 42, 0.7);
  }

  .body {
    display: grid;
    grid-template-columns: 1fr 110px; /* как было */
    gap: 10px;
    align-items: start;
  }

  .sv {
    position: relative;
    width: 100%;
    aspect-ratio: 1 / 1;
    border-radius: 16px;
    overflow: hidden;
    background: hsl(var(--h) 100% 50%);
    box-shadow: inset 0 1px 2px rgba(15, 23, 42, 0.06);
    border: 1px solid rgba(15, 23, 42, 0.06);
    touch-action: none;
    box-sizing: border-box;
  }

  .sv-white {
    position: absolute;
    inset: 0;
    background: linear-gradient(90deg, rgba(255,255,255,1), rgba(255,255,255,0));
  }

  .sv-black {
    position: absolute;
    inset: 0;
    background: linear-gradient(0deg, rgba(0,0,0,1), rgba(0,0,0,0));
  }

  .thumb {
    position: absolute;
    width: 16px;
    height: 16px;
    border-radius: 999px;
    border: 2px solid rgba(255,255,255,0.95);
    box-shadow: 0 10px 22px rgba(15, 23, 42, 0.22);
    transform: translate(-50%, -50%);
    pointer-events: none;
  }

  .right {
    display: flex;
    flex-direction: column;
    gap: 8px;
    min-width: 0;
  }

  /* ✅ одинаковая ширина: preview + hex + hue */
  .preview {
    width: 100%;
    height: 36px;
    border-radius: 14px;
    border: 1px solid rgba(15, 23, 42, 0.08);
    box-shadow: 0 10px 22px rgba(15, 23, 42, 0.10);
    box-sizing: border-box;
  }

  .hex {
    width: 100%;
    border: 0;
    outline: none;
    background: rgba(248, 251, 255, 0.92);
    border-radius: 12px;
    padding: 10px 10px;
    font-size: 12px;
    color: rgba(15, 23, 42, 0.9);
    box-sizing: border-box;
  }

  .hue-wrap { margin-top: 2px; }
  .hue-label {
    font-size: 11px;
    font-weight: 750;
    color: rgba(15,23,42,.7);
    margin-bottom: 6px;
  }

  /* ✅ отступы, чтобы thumb не упирался в край */
  .hue-pad { padding: 0 2px; }

  .hue {
    -webkit-appearance: none;
    appearance: none;
    width: 100%;
    height: 10px;
    border-radius: 999px;
    border: 1px solid rgba(15, 23, 42, 0.06);
    outline: none;
    display: block;
    box-sizing: border-box;
  }

  .hue::-webkit-slider-thumb {
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

  .hue::-moz-range-thumb {
    width: 16px;
    height: 16px;
    border-radius: 999px;
    background: rgba(255, 255, 255, 0.95);
    border: 1px solid rgba(15, 23, 42, 0.15);
    box-shadow: 0 10px 22px rgba(15, 23, 42, 0.18);
    cursor: pointer;
  }
</style>
