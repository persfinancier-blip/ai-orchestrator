<!-- core/orchestrator/modules/advertising/interface/desk/components/space/ui/PickDataMenu.svelte -->
<script lang="ts">
  import type { PeriodMode } from '../types';
  import ColorPickerPopover from './ColorPickerPopover.svelte';

  export let textFields: Array<{ code: string; name: string; kind: 'text' }>;
  export let coordFields: Array<{ code: string; name: string; kind: 'number' | 'date' }>;

  export let selectedEntityFields: string[] = [];
  export let axisX = '';
  export let axisY = '';
  export let axisZ = '';

  export let search = '';

  export let period: PeriodMode = '30 дней';
  export let fromDate = '';
  export let toDate = '';

  export let onAddEntity: (code: string) => void;
  export let onAddCoord: (code: string) => void;
  export let onClose: () => void;

  export let pointsColor = '#3b82f6';
  let isPointsColorOpen = false;

  type AnyField =
    | { code: string; name: string; kind: 'text' }
    | { code: string; name: string; kind: 'number' | 'date' };

  const kindLabel: Record<string, string> = { text: 'текст', number: 'число', date: 'дата' };

  $: selectedCoordCount = [axisX, axisY, axisZ].filter(Boolean).length;
  $: canAddCoord = selectedCoordCount < 3;

  $: allFields = ([...(textFields ?? []), ...(coordFields ?? [])] as AnyField[]);
  $: q = search.trim().toLowerCase();
  $: filteredFields =
    !q ? allFields : allFields.filter((f) => (f.name ?? '').toLowerCase().includes(q) || (f.code ?? '').toLowerCase().includes(q));
  $: nameByCode = new Map(allFields.map((f) => [f.code, f.name] as const));

  function toggleText(code: string): void {
    const cleaned = String(code ?? '').trim();
    if (!cleaned) return;
    selectedEntityFields = selectedEntityFields.includes(cleaned)
      ? selectedEntityFields.filter((x) => x !== cleaned)
      : [...selectedEntityFields, cleaned];
  }

  function selectedAxis(code: string): 'x' | 'y' | 'z' | null {
    if (axisX === code) return 'x';
    if (axisY === code) return 'y';
    if (axisZ === code) return 'z';
    return null;
  }

  function removeCoord(code: string): void {
    const ax = selectedAxis(code);
    if (ax === 'x') axisX = '';
    if (ax === 'y') axisY = '';
    if (ax === 'z') axisZ = '';
  }

  function addCoord(code: string): void {
    const cleaned = String(code ?? '').trim();
    if (!cleaned) return;
    if (axisX === cleaned || axisY === cleaned || axisZ === cleaned) return;
    if (!canAddCoord) return;

    if (!axisX) axisX = cleaned;
    else if (!axisY) axisY = cleaned;
    else if (!axisZ) axisZ = cleaned;
  }

  function toggleCoord(code: string): void {
    const cleaned = String(code ?? '').trim();
    if (!cleaned) return;
    if (selectedAxis(cleaned)) removeCoord(cleaned);
    else addCoord(cleaned);
  }

  function isDisabledField(f: AnyField): boolean {
    if (f.kind === 'text') return false;
    if (selectedAxis(f.code)) return false;
    return !canAddCoord;
  }

  function onPick(f: AnyField): void {
    if (isDisabledField(f)) return;
    if (f.kind === 'text') toggleText(f.code);
    else toggleCoord(f.code);
  }

  const chipLabel = (code: string): string => nameByCode.get(code) ?? code;

  function setPointsColor(v: string): void {
    const cleaned = String(v ?? '').trim();
    if (!cleaned) return;
    pointsColor = cleaned;
  }

  function onHexInput(e: Event): void {
    const el = e.currentTarget as HTMLInputElement | null;
    if (!el) return;
    setPointsColor(el.value);
  }

  function togglePointsColor(): void {
    isPointsColorOpen = !isPointsColorOpen;
  }

  function closePointsColor(): void {
    isPointsColorOpen = false;
  }
</script>

<!-- ... -->

<div class="row two">
  <div class="color-wrap">
    <label class="label">Цвет</label>

  <button
    type="button"
    class="color"
    aria-label="Цвет точек"
    style={`background:${pointsColor};`}
    on:click={togglePointsColor}
  ></button>

    {#if isPointsColorOpen}
      <div class="picker-overlay" on:click={closePointsColor} />
      <div class="picker-layer" aria-label="Цвет точек">
        <ColorPickerPopover bind:value={pointsColor} title="Цвет точек" onClose={closePointsColor} />
      </div>
    {/if}
  </div>

  <input class="hex" placeholder="#3b82f6" value={pointsColor} on:input={onHexInput} />
</div>

<style>
  /* ... твои стили ... */

  .picker-overlay{
    position: fixed;
    inset: 0;
    background: rgba(15, 23, 42, 0.25);
    backdrop-filter: blur(2px);
    z-index: 9998;
  }

  .picker-layer{
    position: fixed;
    inset: 0;
    z-index: 9999;
    pointer-events: none;
  }

  .picker-layer :global(.picker){
    position: fixed;
    top: 50%;
    left: 50%;
    right: auto;
    transform: translate(-50%, -50%);
    pointer-events: auto;
    max-width: min(92vw, 360px);
  }

  @media (max-height: 560px){
    .picker-layer :global(.picker){
      top: 12px;
      transform: translate(-50%, 0);
    }
  }
</style>
