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

  // ✅ фикс рассинхрона: одинаково нормализуем коды
  const norm = (s: string): string => String(s ?? '').trim().toLowerCase();

  $: selectedCoordCount = [axisX, axisY, axisZ].filter(Boolean).length;
  $: canAddCoord = selectedCoordCount < 3;

  $: allFields = ([...(textFields ?? []), ...(coordFields ?? [])] as AnyField[]);

  $: q = search.trim().toLowerCase();
  $: filteredFields =
    !q
      ? allFields
      : allFields.filter((f) => (f.name ?? '').toLowerCase().includes(q) || (f.code ?? '').toLowerCase().includes(q));

  function isSelectedText(code: string): boolean {
    const c = norm(code);
    return selectedEntityFields.some((x) => norm(x) === c);
  }

  function selectedAxis(code: string): 'x' | 'y' | 'z' | null {
    const c = norm(code);
    if (norm(axisX) === c) return 'x';
    if (norm(axisY) === c) return 'y';
    if (norm(axisZ) === c) return 'z';
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

    if (selectedAxis(cleaned)) return;
    if (!canAddCoord) return;

    // X -> Y -> Z
    if (!axisX) axisX = cleaned;
    else if (!axisY) axisY = cleaned;
    else if (!axisZ) axisZ = cleaned;

    onAddCoord?.(cleaned);
  }

  function toggleText(code: string): void {
    const cleaned = String(code ?? '').trim();
    if (!cleaned) return;

    if (isSelectedText(cleaned)) {
      const c = norm(cleaned);
      selectedEntityFields = selectedEntityFields.filter((x) => norm(x) !== c);
      return;
    }

    selectedEntityFields = [...selectedEntityFields, cleaned];
    onAddEntity?.(cleaned);
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

  function isActiveField(f: AnyField): boolean {
    if (f.kind === 'text') return isSelectedText(f.code);
    return Boolean(selectedAxis(f.code));
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
      {@const ax = f.kind === 'text' ? null : selectedAxis(f.code)}
      {@const active = isActiveField(f)}

      <button
        class="item"
        class:active={active}
        class:disabledish={isDisabledField(f)}
        disabled={isDisabledField(f)}
        on:click={() => onPick(f)}
      >
        <span class="name">{f.name}</span>

        <span class="right">
          {#if ax}
            <span class="pill">{ax.toUpperCase()}</span>
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
</div>

<style>
  .row { display: flex; gap: 10px; align-items: center; margin-top: 10px; }

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
    transition: transform 120ms ease, box-shadow 120ms ease, border-color 120ms ease, background 120ms ease;
  }

  .item:hover {
    transform: translateY(-0.5px);
    box-shadow: var(--shadow-btn, 0 10px 26px rgba(15, 23, 42, 0.10));
    border-color: var(--stroke-mid, rgba(15, 23, 42, 0.12));
  }

  /* ✅ выбранное поле (усилили селектор, чтобы не перебивалось глобалом) */
  .list .item.active {
    background: rgba(248, 251, 255, 0.92);
    border-color: var(--stroke-hard, rgba(15, 23, 42, 0.18));
    box-shadow: var(--shadow-btn-strong, 0 12px 30px rgba(15, 23, 42, 0.12));
  }

  .item:disabled {
    opacity: 0.55;
    cursor: not-allowed;
    box-shadow: none;
    transform: none;
  }

  .name { font-size: 12px; font-weight: 650; color: rgba(15,23,42,.88); }

  .right {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    flex-shrink: 0;
  }

  .tag { font-size: 11px; color: rgba(100,116,139,.9); }

  /* ✅ бейдж X/Y/Z */
  .pill {
    font-size: 11px;
    font-weight: 800;
    color: rgba(15, 23, 42, 0.82);
    background: rgba(15, 23, 42, 0.06);
    border: 1px solid rgba(15, 23, 42, 0.10);
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
</style>
