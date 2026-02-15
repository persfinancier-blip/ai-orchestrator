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
    !q
      ? allFields
      : allFields.filter(
          (f) => (f.name ?? '').toLowerCase().includes(q) || (f.code ?? '').toLowerCase().includes(q),
        );

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

<!-- ВАЖНО: .menu-pop.pick НЕ трогаем (позиционирование задаётся общими стилями) -->
<div class="menu-pop pick">
  <!-- ✅ внутренний якорь только для overlay/layer -->
  <div class="pick-inner">
    <div class="menu-title">Выбор данных</div>

    <div class="selected-bar">
      <div class="selected-block">
        <div class="selected-title">Выбраны поля</div>
        <div class="chips">
          {#if (selectedEntityFields?.length ?? 0) === 0}
            <span class="empty">ничего</span>
          {:else}
            {#each selectedEntityFields as c (c)}
              <span class="chip">{chipLabel(c)}</span>
            {/each}
          {/if}
        </div>
      </div>

      <div class="selected-block">
        <div class="selected-title">Оси</div>
        <div class="chips">
          <span class="chip axis">X: {axisX ? chipLabel(axisX) : '—'}</span>
          <span class="chip axis">Y: {axisY ? chipLabel(axisY) : '—'}</span>
          <span class="chip axis">Z: {axisZ ? chipLabel(axisZ) : '—'}</span>
        </div>
      </div>
    </div>

    <!-- ✅ выбор цвета точек -->
    <div class="row two">
      <div class="color-wrap">
        <label class="label">Цвет</label>

        <button
          type="button"
          class="color"
          aria-label="Цвет точек"
          style={`background:${pointsColor};`}
          on:click|stopPropagation={togglePointsColor}
        ></button>
      </div>

      <input class="hex" placeholder="#3b82f6" value={pointsColor} on:input={onHexInput} />
    </div>

    <div class="row">
      <input class="input" placeholder="Поиск по полям (Ctrl+K)" bind:value={search} />
    </div>

    <div class="sep" />

    <div class="sub">Поля</div>

    <div class="list">
      {#each filteredFields as f (f.code)}
        <button class="item" disabled={isDisabledField(f)} on:click={() => onPick(f)}>
          <span class="name">{f.name}</span>

          <span class="right">
            {#if f.kind !== 'text' && selectedAxis(f.code)}
              <span class="pill">{selectedAxis(f.code)?.toUpperCase()}</span>
            {/if}
            <span class="tag">{kindLabel[f.kind]}</span>
          </span>
        </button>
      {/each}
    </div>

    {#if !canAddCoord}
      <div class="limit">
        Уже выбрано 3 координаты (X/Y/Z). Удалите одну прямо на ребре куба (×), чтобы добавить другую.
      </div>
    {/if}

    <!-- ✅ модалка выбора цвета (живет внутри pick-inner, НЕ влияет на позицию меню) -->
    {#if isPointsColorOpen}
      <div class="picker-overlay" on:click={closePointsColor}></div>

      <div class="picker-layer" aria-label="Цвет точек">
        <ColorPickerPopover bind:value={pointsColor} title="Цвет точек" onClose={closePointsColor} />
      </div>
    {/if}
  </div>
</div>

<style>
  /* ✅ вот тут мы якорим только внутренние абсолюты */
  .pick-inner {
    position: relative;
  }

  .row {
    display: flex;
    gap: 10px;
    align-items: center;
    margin-top: 10px;
  }

  .row.two {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 10px;
  }

  .sep {
    height: 1px;
    background: var(--divider, rgba(226, 232, 240, 0.7));
    margin: 12px 0 8px;
  }

  .sub {
    margin-top: 2px;
    font-size: 12px;
    font-weight: 650;
    color: rgba(15, 23, 42, 0.78);
  }

  .selected-bar {
    margin: 8px 0 10px;
    padding: 10px;
    border-radius: 14px;
    background: rgba(15, 23, 42, 0.04);
    border: 1px solid rgba(15, 23, 42, 0.08);
    display: flex;
    flex-direction: column;
    gap: 10px;
  }

  .selected-block {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }

  .selected-title {
    font-size: 11px;
    font-weight: 800;
    color: rgba(15, 23, 42, 0.7);
  }

  .chips {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    align-items: center;
  }

  .empty {
    font-size: 12px;
    color: rgba(100, 116, 139, 0.9);
  }

  .chip {
    font-size: 11px;
    font-weight: 700;
    color: rgba(15, 23, 42, 0.86);
    background: rgba(255, 255, 255, 0.9);
    border: 1px solid rgba(15, 23, 42, 0.1);
    padding: 6px 10px;
    border-radius: 999px;
    line-height: 1;
    max-width: 100%;
  }

  .chip.axis {
    background: rgba(248, 251, 255, 0.92);
  }

  .label {
    font-size: 12px;
    color: rgba(15, 23, 42, 0.78);
    width: 52px;
  }

  .color-wrap {
    display: flex;
    align-items: center;
    gap: 10px;
    min-width: 0;
  }

  .color {
    width: 44px;
    height: 34px;
    padding: 0;
    border: 1px solid var(--stroke-soft, rgba(15, 23, 42, 0.08));
    border-radius: 10px;
    background: #ffffff;
    cursor: pointer;
  }

  .hex {
    width: 100%;
    border: 1px solid var(--stroke-soft, rgba(15, 23, 42, 0.08));
    background: #ffffff;
    border-radius: 12px;
    padding: 10px 12px;
    font-size: 12px;
    outline: none;
    color: rgba(15, 23, 42, 0.9);
    box-sizing: border-box;
  }

  .hex:focus {
    box-shadow: var(--focus-ring, 0 0 0 4px rgba(15, 23, 42, 0.1));
  }

  .list {
    margin-top: 8px;
    display: flex;
    flex-direction: column;
    gap: 6px;
    max-height: 320px;
    overflow: auto;
    padding-right: 2px;
  }

  .item {
    width: 100%;
    text-align: left;
    border: 1px solid var(--stroke-soft, rgba(15, 23, 42, 0.08));
    background: #ffffff;
    border-radius: 14px;
    padding: 10px 10px;
    display: flex;
    justify-content: space-between;
    gap: 10px;
    cursor: pointer;
    box-sizing: border-box;
    transition:
      transform 120ms ease,
      box-shadow 120ms ease,
      border-color 120ms ease,
      background 120ms ease;
  }

  .item:hover {
    transform: translateY(-0.5px);
    box-shadow: var(--shadow-btn, 0 10px 26px rgba(15, 23, 42, 0.1));
    border-color: var(--stroke-mid, rgba(15, 23, 42, 0.12));
  }

  .item:disabled {
    opacity: 0.55;
    cursor: not-allowed;
    box-shadow: none;
    transform: none;
  }

  .name {
    font-size: 12px;
    font-weight: 650;
    color: rgba(15, 23, 42, 0.88);
  }

  .right {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    flex-shrink: 0;
  }

  .tag {
    font-size: 11px;
    color: rgba(100, 116, 139, 0.9);
  }

  .pill {
    font-size: 11px;
    font-weight: 800;
    color: rgba(15, 23, 42, 0.82);
    background: rgba(15, 23, 42, 0.06);
    border: 1px solid rgba(15, 23, 42, 0.1);
    padding: 4px 8px;
    border-radius: 999px;
    line-height: 1;
  }

  .limit {
    margin-top: 8px;
    font-size: 12px;
    color: rgba(100, 116, 139, 0.95);
    background: #ffffff;
    border: 1px solid var(--stroke-soft, rgba(15, 23, 42, 0.08));
    border-radius: 14px;
    padding: 10px 12px;
    box-sizing: border-box;
  }

  /* overlay только внутри меню */
  .picker-overlay {
    position: absolute;
    inset: 0;
    background: rgba(15, 23, 42, 0.25);
    backdrop-filter: blur(2px);
    z-index: 200;
  }

  .picker-layer {
    position: absolute;
    inset: 0;
    z-index: 201;
    pointer-events: none;
  }

  .picker-layer :global(.picker) {
    position: absolute !important;
    top: 50% !important;
    left: 50% !important;
    right: auto !important;
    transform: translate(-50%, -50%) !important;
    pointer-events: auto;
    max-width: min(92vw, 360px);
  }

  @media (max-height: 560px) {
    .picker-layer :global(.picker) {
      top: 12px !important;
      transform: translate(-50%, 0) !important;
    }
  }
</style>
