<script lang="ts">
  export type FieldItem = { code: string; name: string; kind: 'text' | 'number' | 'date' };

  export let textFields: FieldItem[] = [];
  export let coordFields: FieldItem[] = [];

  export let selectedEntityFields: string[] = [];
  export let axisX = '';
  export let axisY = '';
  export let axisZ = '';

  export let search = '';
  export let period: '7 дней' | '14 дней' | '30 дней' | 'Даты';
  export let fromDate = '';
  export let toDate = '';

  export let onAddEntity: (code: string) => void;
  export let onAddCoord: (code: string) => void;
  export let onClose: () => void;

  $: selectedCoordCount = [axisX, axisY, axisZ].filter(Boolean).length;
  $: canAddCoord = selectedCoordCount < 3;
</script>

<div class="menu-pop pick">
  <div class="menu-title">Выбор данных</div>

  <div class="sub">Текстовые → точки</div>
  <div class="list">
    {#each textFields as f (f.code)}
      <button class="item" disabled={selectedEntityFields.includes(f.code)} on:click={() => onAddEntity(f.code)}>
        <span class="name">{f.name}</span>
        <span class="tag">текст</span>
      </button>
    {/each}
  </div>

  <div class="sub">
    Координаты → оси (числа/даты)
    <span class="hint">({selectedCoordCount}/3)</span>
  </div>

  {#if canAddCoord}
    <div class="list">
      {#each coordFields as f (f.code)}
        <button class="item" disabled={axisX === f.code || axisY === f.code || axisZ === f.code} on:click={() => onAddCoord(f.code)}>
          <span class="name">{f.name}</span>
          <span class="tag">{f.kind === 'date' ? 'дата' : 'число'}</span>
        </button>
      {/each}
    </div>
  {:else}
    <div class="limit">
      Уже выбрано 3 координаты (X/Y/Z). Удалите одну прямо на ребре куба (×), чтобы добавить другую.
    </div>
  {/if}

  <div class="sep" />

  <div class="row">
    <input class="input" placeholder="Поиск по точкам (Ctrl+K)" bind:value={search} />
  </div>

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
