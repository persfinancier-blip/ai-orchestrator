<!-- core/orchestrator/modules/advertising/interface/desk/components/space/ui/PickDataMenu.svelte -->
<script lang="ts">
  import type { PeriodMode } from '../types';

  export let textFields: Array<{ code: string; name: string; kind: 'text' }>;
  export let coordFields: Array<{ code: string; name: string; kind: 'number' | 'date' }>;

  // bind'ы из GraphPanel
  export let selectedEntityFields: string[] = [];
  export let axisX = '';
  export let axisY = '';
  export let axisZ = '';

  export let search = '';

  // оставляем, чтобы не ломать GraphPanel bind'ы (UI для периода убран)
  export let period: PeriodMode = '30 дней';
  export let fromDate = '';
  export let toDate = '';

  // оставляем, чтобы не ломать интерфейс (используем только для add)
  export let onAddEntity: (code: string) => void;
  export let onAddCoord: (code: string) => void;
  export let onClose: () => void;

  type AnyField =
    | { code: string; name: string; kind: 'text' }
    | { code: string; name: string; kind: 'number' | 'date' };

  const kindLabel: Record<string, string> = {
    text: 'текст',
    number: 'число',
    date: 'дата'
  };

  $: selectedCoordCount = [axisX, axisY, axisZ].filter(Boolean).length;
  $: canAddCoord = selectedCoordCount < 3;

  $: allFields = ([...(textFields ?? []), ...(coordFields ?? [])] as AnyField[]);

  $: q = search.trim().toLowerCase();
  $: filteredFields =
    !q
      ? allFields
      : allFields.filter((f) => (f.name ?? '').toLowerCase().includes(q) || (f.code ?? '').toLowerCase().includes(q));

  function isSelectedText(code: string): boolean {
    return selectedEntityFields.includes(code);
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
    if (axisX === code || axisY === code || axisZ === code) return;
    if (!canAddCoord) return;

    // тот же приоритет что и в GraphPanel: X -> Y -> Z
    if (!axisX) axisX = code;
    else if (!axisY) axisY = code;
    else if (!axisZ) axisZ = code;

    onAddCoord?.(code);
  }

  function toggleText(code: string): void {
    if (!code) return;
    if (isSelectedText(code)) {
      selectedEntityFields = selectedEntityFields.filter((x) => x !== code);
      return;
    }
    selectedEntityFields = [...selectedEntityFields, code];
    onAddEntity?.(code);
  }

  function toggleCoord(code: string): void {
    if (!code) return;
    if (selectedAxis(code)) {
      removeCoord(code);
      return;
    }
    addCoord(code);
  }

  function isDisabledField(f: AnyField): boolean {
    // выбранные всегда кликабельны (для снятия выбора)
    if (f.kind === 'text') return false;
    if (selectedAxis(f.code)) return false;

    // новые координаты запрещаем, когда уже 3/3
    if (f.kind !== 'text' && !canAddCoord) return true;

    return false;
  }

  function onPick(f: AnyField): void {
    if (isDisabledField(f)) return;

    if (f.kind === 'text') toggleText(f.code);
    else toggleCoord(f.code);
  }
</script>

<div class="menu-pop pick">
  <div class="menu-title">Выбор данных</div>

  <div class="row">
    <input class="input" placeholder="Поиск по полям (Ctrl+K)" bind:value={search} />
  </div>

  <div class="sep" />

  <div class="sub">Поля</div>

  <div class="list">
    {#each filteredFields as f (f.code)}
      <button class="item" disabled={isDisabledField(f)} on:click={() => onPick(f)}>
        <span class="name">{f.name}</span>
        <span class="tag">{kindLabel[f.kind]}</span>
      </button>
    {/each}
  </div>

  {#if !canAddCoord}
    <div class="limit">
      Уже выбрано 3 координаты (X/Y/Z). Удалите одну прямо на ребре куба (×), чтобы добавить другую.
    </div>
  {/if}
</div>

<style>
  .row { display: flex; gap: 10px; align-items: center; margin-top: 10px; }

  .sep {
    height: 1px;
    background: rgba(226, 232, 240, 0.7);
    margin: 12px 0 8px;
  }

  .sub {
    margin-top: 2px;
    font-size: 12px;
    font-weight: 650;
    color: rgba(15, 23, 42, 0.78);
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
    border: 0;
    background: rgba(248, 251, 255, 0.92);
    border-radius: 14px;
    padding: 10px 10px;
    display: flex;
    justify-content: space-between;
    gap: 10px;
    cursor: pointer;
  }

  .item:disabled { opacity: .45; cursor: not-allowed; }

  .name { font-size: 12px; font-weight: 650; color: rgba(15,23,42,.88); }
  .tag { font-size: 11px; color: rgba(100,116,139,.9); }

  .limit {
    margin-top: 8px;
    font-size: 12px;
    color: rgba(100, 116, 139, 0.95);
    background: rgba(248, 251, 255, 0.92);
    border-radius: 14px;
    padding: 10px 12px;
  }
</style>
