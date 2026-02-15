<!-- core/orchestrator/modules/advertising/interface/desk/components/GraphPanel.svelte -->
<script lang="ts">
  import { onDestroy, onMount, tick } from 'svelte';
  import { get } from 'svelte/store';

  import { showcaseStore, fieldName, type DatasetId, type ShowcaseField } from '../data/showcaseStore';

  import type { DatasetPreset, PeriodMode, TooltipState, VisualScheme, ShowcaseSafe, SpacePoint } from './space/types';
  import { applyRowsFilter, buildPoints, calcMax } from './space/pipeline';
  import { loadDatasetPresets, loadVisualSchemes, saveDatasetPresets, saveVisualSchemes } from './space/presetsStorage';
  import { SpaceScene } from './space/three/SpaceScene';

  import DisplayMenu from './space/ui/DisplayMenu.svelte';
  import PickDataMenu from './space/ui/PickDataMenu.svelte';
  import SaveModal from './space/ui/SaveModal.svelte';
  import Tooltip from './space/ui/Tooltip.svelte';
  import Crumbs from './space/ui/Crumbs.svelte';
  import InfoCard from './space/ui/InfoCard.svelte';

  const STORAGE_KEY_VISUALS = 'desk-space-visual-schemes-v2';
  const STORAGE_KEY_DATASETS = 'desk-space-datasets-v2';

  const EMPTY_SHOWCASE: ShowcaseSafe = { fields: [], rows: [], datasets: [] };

  let showcase: ShowcaseSafe = ((get(showcaseStore) as any) ?? EMPTY_SHOWCASE) as ShowcaseSafe;
  const unsub = showcaseStore.subscribe((value) => {
    showcase = (((value as any) ?? EMPTY_SHOWCASE) as ShowcaseSafe) ?? EMPTY_SHOWCASE;
  });

  let container3d: HTMLDivElement;

  let activeLayers: DatasetId[] = ['sales_fact', 'ads'];

  let selectedEntityFields: string[] = ['sku', 'campaign_id'];
  let axisX = '';
  let axisY = '';
  let axisZ = '';

  let period: PeriodMode = '30 дней';
  let fromDate = '';
  let toDate = '';
  let search = '';

  let visualBg = '#ffffff';
  let visualEdge = '#334155';

  // ✅ per-field colors: code -> hex (используем для точек)
  let entityFieldColors: Record<string, string> = {};
  const DEFAULT_ENTITY_COLOR = '#3b82f6';

  // ✅ LOD state (по умолчанию включено, средняя детализация, min=5)
  let lodEnabled = true;
  let lodDetail = 0.5; // 0..1
  let lodMinCount = 5;

  // ✅ НОРМАЛИЗАЦИЯ КЛЮЧЕЙ (должна совпадать с PickDataMenu)
  const norm = (s: string): string => String(s ?? '').trim().toLowerCase();
  const tail = (s: string): string => {
    const n = norm(s);
    return n.split(/[:./]/g).filter(Boolean).pop() ?? n;
  };
  const keyForColor = (code: string): string => tail(code);

  function setEntityColor(code: string, color: string): void {
    const k = keyForColor(code);
    const c = String(color ?? '').trim();
    if (!k || !c) return;
    entityFieldColors = { ...(entityFieldColors ?? {}), [k]: c };
  }

  function deleteEntityColor(code: string): void {
    const k = keyForColor(code);
    if (!entityFieldColors?.[k]) return;
    const next = { ...(entityFieldColors ?? {}) };
    delete next[k];
    entityFieldColors = next;
  }

  let visualSchemes: VisualScheme[] = [];
  let selectedVisualId = '';

  let datasetPresets: DatasetPreset[] = [];
  let selectedDatasetPresetId = '';

  let showDisplayMenu = false;
  let showPickDataMenu = false;

  let showSaveVisualModal = false;
  let saveVisualName = '';

  let showSaveDatasetModal = false;
  let saveDatasetName = '';

  let tooltip: TooltipState = { visible: false, x: 0, y: 0, lines: [] };

  let renderedCount = 0;
  let bboxLabel = '—';

  let scene: SpaceScene | null = null;

  function fieldsAll(): ShowcaseField[] {
    return (showcase?.fields ?? []) as ShowcaseField[];
  }

  function fieldsFor(kind: ShowcaseField['kind']): ShowcaseField[] {
    const all = fieldsAll();
    return all.filter((f) => f.kind === kind && (f.datasetIds ?? []).some((id) => activeLayers.includes(id)));
  }

  $: textFieldsAll = fieldsFor('text');
  $: numberFieldsAll = fieldsFor('number');
  $: dateFieldsAll = fieldsFor('date');
  $: coordFieldsAll = [...numberFieldsAll, ...dateFieldsAll];

  $: if (coordFieldsAll.length) ensureDefaults();

  function ensureDefaults(): void {
    const coords = coordFieldsAll;
    if (!axisX && coords[0]) axisX = coords[0].code;
    if (!axisY && coords[1]) axisY = coords[1].code;
    if (!axisZ && coords[2]) axisZ = coords[2].code;
  }

  $: filteredRows = applyRowsFilter({
    rows: showcase?.rows ?? [],
    period,
    fromDate,
    toDate,
    search,
    selectedEntityFields
  });

  function safeBuildPoints(opts: { lodEnabled: boolean; lodDetail: number; lodMinCount: number }): SpacePoint[] {
    try {
      return buildPoints({
        rows: filteredRows,
        entityFields: selectedEntityFields,
        axisX,
        axisY,
        axisZ,
        numberFields: numberFieldsAll,
        dateFields: dateFieldsAll,
        lodEnabled: opts.lodEnabled,
        lodDetail: opts.lodDetail,
        lodMinCount: opts.lodMinCount
      });
    } catch (e) {
      // чтобы UI не умирал и кнопки не “умирали” вместе с компонентом
      console.error('[GraphPanel] buildPoints failed', e);
      return [];
    }
  }

  // ✅ raw points (без LOD) — только для max значений осей
  $: pointsRaw = safeBuildPoints({ lodEnabled: false, lodDetail, lodMinCount });

  // ✅ render points (с LOD) — то, что реально рисуем
  $: points = safeBuildPoints({ lodEnabled, lodDetail, lodMinCount });

  // ✅ max по осям считаем по raw (не зависят от LOD)
  $: axisMaxOverride = ({
    xMax: axisX ? calcMax(pointsRaw, axisX) : Number.NaN,
    yMax: axisY ? calcMax(pointsRaw, axisY) : Number.NaN,
    zMax: axisZ ? calcMax(pointsRaw, axisZ) : Number.NaN
  });

  function colorForEntityField(code: string, colors: Record<string, string>): string {
    return colors?.[keyForColor(code)] ?? DEFAULT_ENTITY_COLOR;
  }

  // ✅ когда “формирование групп” убрали — всегда красим по полю
  function applyColoring(input: SpacePoint[], colors: Record<string, string>): SpacePoint[] {
    return (input ?? []).map((p) => ({
      ...p,
      color: colorForEntityField(p.sourceField, colors)
    }));
  }

  $: colored = applyColoring(points, entityFieldColors);

  $: if (scene) {
    scene.setTheme({ bg: visualBg, edge: visualEdge });

    // ✅ важно: сюда передаём axisMaxOverride, иначе подписи будут “прыгать” от LOD
    scene.setAxisCodes(
      { x: axisX, y: axisY, z: axisZ },
      { axisMaxOverride }
    );

    const info = scene.setPoints(colored, { axisMaxOverride });

    renderedCount = info.renderedCount;
    bboxLabel = info.bboxLabel;
  }

  function toggleDisplayMenu(): void {
    showDisplayMenu = !showDisplayMenu;
    if (showDisplayMenu) showPickDataMenu = false;
  }

  function togglePickMenu(): void {
    showPickDataMenu = !showPickDataMenu;
    if (showPickDataMenu) showDisplayMenu = false;
  }

  function closeAllMenus(): void {
    showDisplayMenu = false;
    showPickDataMenu = false;
  }

  function addEntityField(code: string): void {
    if (!code || selectedEntityFields.includes(code)) return;
    selectedEntityFields = [...selectedEntityFields, code];

    const k = keyForColor(code);
    if (!entityFieldColors?.[k]) setEntityColor(code, DEFAULT_ENTITY_COLOR);
  }

  function removeEntityField(code: string): void {
    selectedEntityFields = selectedEntityFields.filter((x) => x !== code);
    deleteEntityColor(code);
  }

  function addCoordField(code: string): void {
    if (!code) return;
    if (axisX === code || axisY === code || axisZ === code) return;

    if (!axisX) axisX = code;
    else if (!axisY) axisY = code;
    else if (!axisZ) axisZ = code;
  }

  function clearAllDataSelection(): void {
    selectedEntityFields = [];
    axisX = '';
    axisY = '';
    axisZ = '';
    search = '';
    entityFieldColors = {};

    lodEnabled = true;
    lodDetail = 0.5;
    lodMinCount = 5;
  }

  function loadStorages(): void {
    const loaded = loadVisualSchemes(STORAGE_KEY_VISUALS) as any[];
    visualSchemes = (loaded ?? []).map((v) => ({
      id: String(v.id),
      name: String(v.name ?? ''),
      bg: String(v.bg ?? '#ffffff'),
      edge: String(v.edge ?? '#334155')
    }));

    datasetPresets = loadDatasetPresets(STORAGE_KEY_DATASETS);
  }

  function saveVisualScheme(name: string): void {
    const trimmed = name.trim();
    if (!trimmed) return;

    const item: VisualScheme = {
      id: `${Date.now()}-${Math.random()}`,
      name: trimmed,
      bg: visualBg,
      edge: visualEdge
    };

    visualSchemes = [item, ...visualSchemes].slice(0, 30);
    selectedVisualId = item.id;
    saveVisualSchemes(STORAGE_KEY_VISUALS, visualSchemes);
  }

  function applyVisualScheme(id: string): void {
    const v = visualSchemes.find((x) => x.id === id);
    if (!v) return;
    visualBg = v.bg;
    visualEdge = v.edge;
  }

  function resetVisual(): void {
    visualBg = '#ffffff';
    visualEdge = '#334155';
    selectedVisualId = '';
  }

  function saveDatasetPreset(name: string): void {
    const trimmed = name.trim();
    if (!trimmed) return;

    const item: DatasetPreset = {
      id: `${Date.now()}-${Math.random()}`,
      name: trimmed,
      value: { selectedEntityFields, axisX, axisY, axisZ, period, fromDate, toDate, search }
    };

    datasetPresets = [item, ...datasetPresets].slice(0, 30);
    selectedDatasetPresetId = item.id;
    saveDatasetPresets(STORAGE_KEY_DATASETS, datasetPresets);
  }

  function applyDatasetPreset(id: string): void {
    const p = datasetPresets.find((x) => x.id === id);
    if (!p) return;

    selectedEntityFields = p.value.selectedEntityFields ?? [];
    axisX = p.value.axisX ?? '';
    axisY = p.value.axisY ?? '';
    axisZ = p.value.axisZ ?? '';
    period = p.value.period ?? '30 дней';
    fromDate = p.value.fromDate ?? '';
    toDate = p.value.toDate ?? '';
    search = p.value.search ?? '';
  }

  function onAxisRemove(axis: 'x' | 'y' | 'z'): void {
    if (axis === 'x') axisX = '';
    if (axis === 'y') axisY = '';
    if (axis === 'z') axisZ = '';
  }

  function onGlobalClick(e: MouseEvent): void {
    const t = e.target as Node | null;
    if (!t) return;

    const menus = Array.from(document.querySelectorAll('.menu-pop')) as HTMLElement[];
    const inMenu = menus.some((m) => m.contains(t));
    const inBtns = (document.querySelector('.hud')?.contains(t) ?? false);

    if (!inMenu && !inBtns) closeAllMenus();
  }

  function onKey(e: KeyboardEvent): void {
    if (e.key === 'Escape') {
      closeAllMenus();
      showSaveVisualModal = false;
      showSaveDatasetModal = false;
    }
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') {
      e.preventDefault();
      showPickDataMenu = true;
      showDisplayMenu = false;
    }
  }

  function onResize(): void {
    scene?.resize(560);
  }

  onMount(async () => {
    loadStorages();
    await tick();

    scene = new SpaceScene(
      { fieldName, getFields: () => fieldsAll() },
      { onTooltip: (t) => (tooltip = t), onAxisRemove }
    );

    scene.init(container3d, { height: 560 });
    ensureDefaults();

    window.addEventListener('resize', onResize);
    window.addEventListener('keydown', onKey);
    window.addEventListener('click', onGlobalClick, true);

    await tick();

    // ✅ на старте — тоже с axisMaxOverride
    scene.setAxisCodes({ x: axisX, y: axisY, z: axisZ }, { axisMaxOverride });
    const info = scene.setPoints(colored, { axisMaxOverride });
    renderedCount = info.renderedCount;
    bboxLabel = info.bboxLabel;
  });

  onDestroy(() => {
    unsub();

    window.removeEventListener('resize', onResize);
    window.removeEventListener('keydown', onKey);
    window.removeEventListener('click', onGlobalClick, true);

    scene?.dispose();
    scene = null;
  });

  $: selectedCrumbs = selectedEntityFields.map((c) => ({ code: c, name: fieldName(c) }));
  $: axesLabel = `Оси: ${fieldName(axisX || '—')} / ${fieldName(axisY || '—')} / ${fieldName(axisZ || '—')}`;
</script>

<section class="graph-root">
  <div class="stage">
    <div bind:this={container3d} class="scene" />

    <div class="hud top-right">
      <div class="hud-actions">
        <button class="btn" on:click={toggleDisplayMenu}>Настройка отображения</button>
        <button class="btn btn-primary" on:click={togglePickMenu}>Выбрать данные</button>
      </div>

      <Crumbs crumbs={selectedCrumbs} onRemove={removeEntityField} />
    </div>

    <InfoCard pointsCount={renderedCount} axesLabel={axesLabel} bboxLabel={bboxLabel} />

    {#if showDisplayMenu}
      <DisplayMenu
        bind:visualBg
        bind:visualEdge
        bind:selectedVisualId
        bind:selectedDatasetPresetId
        {visualSchemes}
        {datasetPresets}
        onResetVisual={resetVisual}
        onOpenSaveVisual={() => {
          saveVisualName = '';
          showSaveVisualModal = true;
          closeAllMenus();
        }}
        onClearAllDataSelection={clearAllDataSelection}
        onOpenSaveDataset={() => {
          saveDatasetName = '';
          showSaveDatasetModal = true;
          closeAllMenus();
        }}
        onApplyVisual={applyVisualScheme}
        onApplyDataset={applyDatasetPreset}
        onResetView={() => scene?.resetView()}
      />
    {/if}

    {#if showPickDataMenu}
      <PickDataMenu
        textFields={textFieldsAll}
        coordFields={coordFieldsAll}
        bind:selectedEntityFields
        bind:axisX
        bind:axisY
        bind:axisZ
        bind:search
        bind:period
        bind:fromDate
        bind:toDate
        bind:entityFieldColors
        defaultEntityColor={DEFAULT_ENTITY_COLOR}
        bind:lodEnabled
        bind:lodDetail
        bind:lodMinCount
        onAddEntity={addEntityField}
        onAddCoord={addCoordField}
        onClose={closeAllMenus}
      />
    {/if}

    <Tooltip {tooltip} />
  </div>

  {#if showSaveVisualModal}
    <SaveModal
      title="Сохранить визуальную схему"
      placeholder="Например: Светлый фон"
      bind:value={saveVisualName}
      onCancel={() => (showSaveVisualModal = false)}
      onSave={(v) => {
        saveVisualScheme(v);
        showSaveVisualModal = false;
      }}
    />
  {/if}

  {#if showSaveDatasetModal}
    <SaveModal
      title="Сохранить набор данных"
      placeholder="Например: Артикул + Выручка/Заказы/Расход"
      bind:value={saveDatasetName}
      onCancel={() => (showSaveDatasetModal = false)}
      onSave={(v) => {
        saveDatasetPreset(v);
        showSaveDatasetModal = false;
      }}
    />
  {/if}
</section>

<style>
  /* твои стили без изменений */
</style>

<style>
  /* стили без изменений */
  :global(:root) {
    --ink-900: 15 23 42;
    --ink-600: 100 116 139;
    --ink-200: 226 232 240;

    --stroke-soft: rgba(var(--ink-900) / 0.08);
    --stroke-mid: rgba(var(--ink-900) / 0.12);
    --stroke-hard: rgba(var(--ink-900) / 0.18);
    --divider: rgba(var(--ink-200) / 0.70);

    --shadow-btn: 0 10px 26px rgba(var(--ink-900) / 0.10);
    --shadow-btn-strong: 0 12px 30px rgba(var(--ink-900) / 0.12);
    --shadow-card: 0 16px 38px rgba(var(--ink-900) / 0.14);
    --shadow-pop: 0 22px 60px rgba(var(--ink-900) / 0.18);
    --shadow-modal: 0 30px 80px rgba(var(--ink-900) / 0.24);

    --focus-ring: 0 0 0 4px rgba(var(--ink-900) / 0.10);

    --field-bg: #ffffff;
    --field-bg-soft: rgba(248, 251, 255, 0.9);
  }

  :global(.graph-root) { width: 100%; }
  :global(.stage) { position: relative; height: 560px; border-radius: 18px; overflow: hidden; background: #ffffff; }
  :global(.scene) { position: absolute; inset: 0; }

  :global(.hud) { position: absolute; pointer-events: none; display: flex; flex-direction: column; gap: 10px; z-index: 5; }
  :global(.hud.top-right) { right: 14px; top: 12px; align-items: flex-end; }
  :global(.hud.bottom-left) { left: 14px; bottom: 12px; align-items: flex-start; }
  :global(.hud-actions) { display: flex; gap: 10px; pointer-events: auto; }

  :global(.btn) {
    border: 0; border-radius: 999px; padding: 10px 14px; font-size: 13px; font-weight: 650;
    cursor: pointer; line-height: 1; background: #f8fbff; color: rgba(var(--ink-900) / 0.90);
    box-shadow: var(--shadow-btn); pointer-events: auto;
  }
  :global(.btn:hover) { transform: translateY(-0.5px); }

  :global(.btn.btn-primary) { background: #f8fbff; box-shadow: var(--shadow-btn-strong); position: relative; }
  :global(.btn.wide) { width: 100%; }

  :global(.menu-pop) {
    position: absolute; top: 56px; right: 14px; width: 340px;
    background: rgba(255, 255, 255, 0.92);
    border-radius: 18px; padding: 12px;
    box-shadow: 0 22px 60px rgba(15, 23, 42, 0.18);
    backdrop-filter: blur(14px);
    pointer-events: auto; z-index: 2000;
    max-height: calc(100vh - 110px); overflow: auto;
  }

  :global(.menu-title) { font-weight: 800; font-size: 13px; color: rgba(var(--ink-900) / 0.90); margin-bottom: 10px; }
  :global(.sub) { margin-top: 10px; font-size: 12px; font-weight: 650; color: rgba(var(--ink-900) / 0.78); display: flex; align-items: baseline; gap: 6px; }
  :global(.hint) { font-weight: 600; color: rgba(var(--ink-600) / 0.90); }

  :global(.row) { display: flex; gap: 10px; align-items: center; margin-top: 10px; }
  :global(.row.two) { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
  :global(.label) { font-size: 12px; color: rgba(var(--ink-900) / 0.78); width: 52px; }

  :global(.sep) { height: 1px; background: var(--divider); margin: 12px 0; }

  :global(.select), :global(.input), :global(.hex) {
    width: 100%;
    border: 1px solid var(--stroke-soft);
    background: var(--field-bg, #ffffff);
    border-radius: 12px;
    padding: 10px 12px;
    font-size: 12px;
    outline: none;
    color: rgba(var(--ink-900) / 0.90);
    box-sizing: border-box;
  }

  :global(.select:focus), :global(.input:focus), :global(.hex:focus) {
    box-shadow: var(--focus-ring);
    border-color: var(--stroke-mid);
  }

  :global(.list) { margin-top: 8px; display: flex; flex-direction: column; gap: 6px; max-height: 190px; overflow: auto; padding-right: 2px; }

  :global(.item) {
    width: 100%;
    text-align: left;
    border: 1px solid var(--stroke-soft);
    background: rgba(248, 251, 255, 0.92);
    border-radius: 14px;
    padding: 10px 10px;
    display: flex;
    justify-content: space-between;
    gap: 10px;
    cursor: pointer;
    box-sizing: border-box;
  }
  :global(.item:disabled) { opacity: .45; cursor: not-allowed; }

  :global(.name) { font-size: 12px; font-weight: 650; color: rgba(var(--ink-900) / 0.88); }
  :global(.tag) { font-size: 11px; color: rgba(var(--ink-600) / 0.90); }

  :global(.limit) {
    margin-top: 8px;
    font-size: 12px;
    color: rgba(var(--ink-600) / 0.95);
    background: rgba(248, 251, 255, 0.92);
    border: 1px solid var(--stroke-soft);
    border-radius: 14px;
    padding: 10px 12px;
    box-sizing: border-box;
  }

  :global(.crumbs) { display: flex; flex-wrap: wrap; gap: 8px; justify-content: flex-end; max-width: 520px; pointer-events: auto; }
  :global(.crumb) {
    display: inline-flex; align-items: center; gap: 8px;
    padding: 8px 10px;
    border-radius: 999px;
    border: 1px solid var(--stroke-soft);
    background: rgba(248, 251, 255, 0.95);
    color: rgba(var(--ink-900) / 0.90);
    box-shadow: var(--shadow-btn);
    cursor: pointer;
  }

  :global(.info-card) {
    pointer-events: none;
    background: rgba(248, 251, 255, 0.88);
    border-radius: 14px;
    padding: 10px 12px;
    box-shadow: var(--shadow-card);
    max-width: 420px;
    border: 1px solid var(--stroke-soft);
    box-sizing: border-box;
  }

  :global(.info-title) { font-size: 12px; font-weight: 750; color: rgba(var(--ink-900) / 0.90); margin-bottom: 4px; }
  :global(.info-sub) { font-size: 11px; color: rgba(var(--ink-600) / 0.92); line-height: 1.35; }

  :global(.tooltip) {
    position: absolute;
    pointer-events: none;
    background: rgba(15, 23, 42, 0.92);
    color: #f8fafc;
    font-size: 12px;
    padding: 10px;
    border-radius: 12px;
    max-width: 360px;
    z-index: 6;
  }
</style>
