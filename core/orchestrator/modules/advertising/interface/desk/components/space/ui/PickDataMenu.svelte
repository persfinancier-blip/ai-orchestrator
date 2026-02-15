<!-- core/orchestrator/modules/advertising/interface/desk/components/space/ui/PickDataMenu.svelte -->
<script lang="ts">
  import type { PeriodMode } from '../types';

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

  $: allFields = [
    ...(textFields ?? []),
    ...(coordFields ?? [])
  ] as AnyField[];

  $: q = search.trim().toLowerCase();

  $: filteredFields =
    !q
      ? allFields
      : allFields.filter((f) => (f.name ?? '').toLowerCase().includes(q) || (f.code ?? '').toLowerCase().includes(q));

  function isSelectedText(code: string): boolean {
    return selectedEntityFields.includes(code);
  }

  function isSelectedCoord(code: string): boolean {
    return axisX === code || axisY === code || axisZ === code;
  }

  function isDisabledField(f: AnyField): boolean {
    if (f.kind === 'text') return isSelectedText(f.code);
    if (isSelectedCoord(f.code)) return true;
    return !canAddCoord;
  }

  function onPick(f: AnyField): void {
    if (isDisabledField(f)) return;

    if (f.kind === 'text') onAddEntity(f.code);
    else onAddCoord(f.code);
  }
</script>

<div class="menu-pop pick">
  <div class="menu-title">Выбор данных</div>

  <div class="row">
    <input class="input" placeholder="Поиск по полям (Ctrl+K)" bind:value={search} />
  </div>

  <div class="sub">
    Поля
    <span class="hint">
      (точки: {selectedEntityFields.length} · оси: {selectedCoordCount}/3)
    </span>
  </div>

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

  <div class="sep" />

  <div class="row">
    <select class="select" bind:value={period}>
      <option>7 дней</option>
      <option>14 дней</option>
      <option>30 дней</option>
      <option>Даты</option>
    </select>
  </div>

  {#if period === 'Даты'}
    <div class="row two">
      <input class="input" type="date" bind:value={fromDate} />
      <input class="input" type="date" bind:value={toDate} />
    </div>
  {/if}

  <div class="row two">
    <button class="btn" on:click={onClose}>Закрыть</button>
    <button class="btn btn-primary" on:click={onClose}>Готово</button>
  </div>
</div>
