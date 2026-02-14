<!-- core/orchestrator/modules/advertising/interface/desk/components/GraphPanel.svelte -->
<script lang="ts">
  import { onDestroy, onMount, tick } from 'svelte';
  import { get } from 'svelte/store';

  import { showcaseStore, fieldName, type DatasetId, type ShowcaseField } from '../data/showcaseStore';

  import type { DatasetPreset, PeriodMode, TooltipState, VisualScheme, ShowcaseSafe, SpacePoint } from './space/types';
  import { applyRowsFilter, buildPoints } from './space/pipeline';
  import { loadDatasetPresets, loadVisualSchemes, saveDatasetPresets, saveVisualSchemes } from './space/presetsStorage';
  import { SpaceScene } from './space/three/SpaceScene';

  import DisplayMenu from './space/ui/DisplayMenu.svelte';
  import PickDataMenu from './space/ui/PickDataMenu.svelte';
  import SaveModal from './space/ui/SaveModal.svelte';
  import Tooltip from './space/ui/Tooltip.svelte';
  import Crumbs from './space/ui/Crumbs.svelte';
  import InfoCard from './space/ui/InfoCard.svelte';

  import GroupingMenu from './space/ui/GroupingMenu.svelte';

  import type { GroupingConfig } from './space/cluster/types';
  import { dbscan3 } from './space/cluster/dbscan3';
  import { buildFeatureVec3, dbscanParamsFromDetail, weightsForCfg } from './space/cluster/features';

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

  let showGroupingMenu = false;

  let grouping: GroupingConfig = {
    enabled: false,
    principle: 'proximity',
    featureFields: [],
    detail: 0.45,
    customWeights: false,
    wX: 1,
    wY: 1,
    wZ: 1,
    minClusterSize: 5,
    recompute: 'auto'
  };

  let clusterSeed = 0;

  // ---- fields helpers
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

  $: groupingNumberFields = numberFieldsAll;
  $: groupingTextFields = textFieldsAll;

  $: if (coordFieldsAll.length) ensureDefaults();

  function ensureDefaults(): void {
    const coords = coordFieldsAll;
    if (!axisX && coords[0]) axisX = coords[0].code;
    if (!axisY && coords[1]) axisY = coords[1].code;
    if (!axisZ && coords[2]) axisZ = coords[2].code;
  }

  // ---- data -> points
  $: filteredRows = applyRowsFilter({
    rows: showcase?.rows ?? [],
    period,
    fromDate,
    toDate,
    search,
    selectedEntityFields
  });

  $: points = buildPoints({
    rows: filteredRows,
    entityFields: selectedEntityFields,
    axisX,
    axisY,
    axisZ,
    numberFields: numberFieldsAll,
    dateFields: dateFieldsAll
  });

  // ---- clustering cache
  let cachedLabels: Int32Array | null = null;
  let cachedCounts: Map<number, number> | null = null;
  let cachedKey = '';
  let cachedAxisKey = '';

  function colorForCluster(id: number): string {
    if (id < 0) return '#94a3b8';
    const hue = (id * 47) % 360;
    return `hsl(${hue} 70% 45%)`;
  }

  function makeClusterKey(cfg: GroupingConfig): string {
    return [
      cfg.enabled ? '1' : '0',
      cfg.principle,
      cfg.featureFields.slice().sort().join('|'),
      cfg.detail.toFixed(3),
      cfg.customWeights ? '1' : '0',
      cfg.wX.toFixed(2),
      cfg.wY.toFixed(2),
      cfg.wZ.toFixed(2),
      String(cfg.minClusterSize),
      cfg.recompute,
      String(clusterSeed) // manual trigger
    ].join('::');
  }

  function getTextValue(p: SpacePoint, field: string): string {
    // для behavior: берём “label” и мета-ключи максимально стабильно
    // metrics для text полей обычно пустые => используем label / id / sourceField как fallback
    const m = (p as any)?.metrics ?? {};
    const v = m?.[field];
    if (typeof v === 'string') return v;
    // если вдруг кто-то положил текст прямо в metrics как number/string
    if (v != null) return String(v);

    // fallback: если выбран field совпадает с sourceField, label содержит значение группы
    if (p.sourceField === field) return p.label;

    return '';
  }

  function computeClusters(input: SpacePoint[]): { labels: Int32Array; counts: Map<number, number> } {
    const vecs = buildFeatureVec3({
      points: input,
      cfg: grouping,
      axis: { x: axisX, y: axisY, z: axisZ },
      getTextValue
    });

    const { eps, minPts } = dbscanParamsFromDetail(grouping.detail);
    const w = weightsForCfg(grouping);

    const labels = dbscan3(vecs, { eps, minPts }, w);

    const counts = new Map<number, number>();
    for (let i = 0; i < labels.length; i += 1) {
      const id = labels[i];
      counts.set(id, (counts.get(id) ?? 0) + 1);
    }

    return { labels, counts };
  }

  function applyClustering(input: SpacePoint[]): SpacePoint[] {
    if (!grouping.enabled) {
      cachedLabels = null;
      cachedCounts = null;
      cachedKey = '';
      cachedAxisKey = '';
      return input.map((p) => ({ ...p, color: undefined }));
    }

    const key = makeClusterKey(grouping);
    const axisKey = `${axisX}::${axisY}::${axisZ}`;

    const needRecomputeAuto = grouping.recompute === 'auto';
    const needRecomputeManual = grouping.recompute === 'manual';
    const needRecomputeFixed = grouping.recompute === 'fixed';

    // fixed: не пересчитывать при смене осей/поиска/периода, только если:
    // - кэш пуст (первое включение)
    // - или пользователь нажал "пересчитать" (clusterSeed изменился)
    const fixedShouldRecompute = needRecomputeFixed && (!cachedLabels || key !== cachedKey);

    // manual: пересчёт только по кнопке (clusterSeed меняет key)
    const manualShouldRecompute = needRecomputeManual && (!cachedLabels || key !== cachedKey);

    // auto: пересчёт при любом изменении key или если длина изменилась
    const autoShouldRecompute =
      needRecomputeAuto &&
      (!cachedLabels || key !== cachedKey || axisKey !== cachedAxisKey || cachedLabels.length !== input.length);

    const shouldRecompute = autoShouldRecompute || manualShouldRecompute || fixedShouldRecompute;

    if (shouldRecompute) {
      const res = computeClusters(input);
      cachedLabels = res.labels;
      cachedCounts = res.counts;
      cachedKey = key;
      cachedAxisKey = axisKey;
    }

    const labels = cachedLabels;
    const counts = cachedCounts;

    if (!labels || !counts || labels.length !== input.length) {
      return input.map((p) => ({ ...p, color: '#94a3b8' }));
    }

    return input.map((p, i) => {
      const id = labels[i];
      const size = counts.get(id) ?? 0;
      const effectiveId = id >= 0 && size < grouping.minClusterSize ? -1 : id;
      return { ...p, color: colorForCluster(effectiveId) };
    });
  }

  function recomputeClusters(): void {
    clusterSeed += 1;
  }

  // ---- render reactive
  $: if (scene) {
    scene.setTheme({ bg: visualBg, edge: visualEdge });
    scene.setAxisCodes({ x: axisX, y: axisY, z: axisZ });

    const clustered = applyClustering(points);
    const info = scene.setPoints(clustered);

    renderedCount = info.renderedCount;
    bboxLabel = info.bboxLabel;
  }

  // ---- menus
  function toggleDisplayMenu(): void {
    showDisplayMenu = !showDisplayMenu;
    if (showDisplayMenu) {
      showPickDataMenu = false;
      showGroupingMenu = false;
    }
  }

  function togglePickMenu(): void {
    showPickDataMenu = !showPickDataMenu;
    if (showPickDataMenu) {
      showDisplayMenu = false;
      showGroupingMenu = false;
    }
  }

  function toggleGroupingMenu(): void {
    showGroupingMenu = !showGroupingMenu;
    if (showGroupingMenu) {
      showDisplayMenu = false;
      showPickDataMenu = false;
    }
  }

  function closeAllMenus(): void {
    showDisplayMenu = false;
    showPickDataMenu = false;
    showGroupingMenu = false;
  }

  // ---- selection helpers
  function addEntityField(code: string): void {
    if (!code || selectedEntityFields.includes(code)) return;
    selectedEntityFields = [...selectedEntityFields, code];
  }

  function removeEntityField(code: string): void {
    selectedEntityFields = selectedEntityFields.filter((x) => x !== code);
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
  }

  // ---- storage
  function loadStorages(): void {
    visualSchemes = loadVisualSchemes(STORAGE_KEY_VISUALS);
    datasetPresets = loadDatasetPresets(STORAGE_KEY_DATASETS);
  }

  function saveVisualScheme(name: string): void {
    const trimmed = name.trim();
    if (!trimmed) return;

    const item: VisualScheme = { id: `${Date.now()}-${Math.random()}`, name: trimmed, bg: visualBg, edge: visualEdge };
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

  // ---- axis remove
  function onAxisRemove(axis: 'x' | 'y' | 'z'): void {
    if (axis === 'x') axisX = '';
    if (axis === 'y') axisY = '';
    if (axis === 'z') axisZ = '';
  }

  // ---- global close / esc
  function onGlobalClick(e: MouseEvent): void {
    const t = e.target as Node | null;
    if (!t) return;

    // любой .menu-pop (display/pick/grouping) считаем меню
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
      showGroupingMenu = false;
    }
  }

  function onResize(): void {
    scene?.resize(560);
  }

  // ---- mount/unmount
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

    const clustered = applyClustering(points);
    const info = scene.setPoints(clustered);
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

  // ---- breadcrumbs/labels
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
        <button class="btn" on:click={toggleGroupingMenu}>Формирование групп</button>
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
        onAddEntity={addEntityField}
        onAddCoord={addCoordField}
        onClose={closeAllMenus}
      />
    {/if}

    {#if showGroupingMenu}
      <GroupingMenu bind:cfg={grouping} numberFields={groupingNumberFields} textFields={groupingTextFields} onRecompute={recomputeClusters} />
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
  :global(.graph-root) { width: 100%; }

  :global(.stage) {
    position: relative;
    height: 560px;
    border-radius: 18px;
    overflow: hidden;
    background: #ffffff;
  }

  :global(.scene) { position: absolute; inset: 0; }

  :global(.hud) {
    position: absolute;
    pointer-events: none;
    display: flex;
    flex-direction: column;
    gap: 10px;
    z-index: 5;
  }

  :global(.hud.top-right) { right: 14px; top: 12px; align-items: flex-end; }
  :global(.hud.bottom-left) { left: 14px; bottom: 12px; align-items: flex-start; }

  :global(.hud-actions) { display: flex; gap: 10px; pointer-events: auto; }

  :global(.btn) {
    border: 0;
    border-radius: 999px;
    padding: 10px 14px;
    font-size: 13px;
    font-weight: 650;
    cursor: pointer;
    line-height: 1;
    background: #f8fbff;
    color: rgba(15, 23, 42, 0.9);
    box-shadow: 0 10px 26px rgba(15, 23, 42, 0.10);
    pointer-events: auto;
  }
  :global(.btn:hover) { transform: translateY(-0.5px); }

  :global(.btn.btn-primary) {
    background: #f8fbff;
    box-shadow: 0 12px 30px rgba(15, 23, 42, 0.12);
    position: relative;
  }
  :global(.btn.btn-primary::after) {
    content: '';
    position: absolute;
    left: 12px;
    right: 12px;
    bottom: 8px;
    height: 2px;
    border-radius: 2px;
    background: rgba(34, 197, 94, 0.9);
    opacity: .55;
  }

  :global(.btn.wide) { width: 100%; }

  :global(.crumbs) {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    justify-content: flex-end;
    max-width: 520px;
    pointer-events: auto;
  }

  :global(.crumb) {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 8px 10px;
    border-radius: 999px;
    border: 0;
    background: rgba(248, 251, 255, 0.95);
    color: rgba(15, 23, 42, 0.9);
    box-shadow: 0 10px 22px rgba(15, 23, 42, 0.10);
    cursor: pointer;
  }
  :global(.crumb .t) { font-size: 12px; font-weight: 650; }
  :global(.crumb .x) {
    width: 18px;
    height: 18px;
    display: grid;
    place-items: center;
    border-radius: 999px;
    background: rgba(15,23,42,.06);
    font-weight: 900;
    line-height: 1;
  }
  :global(.crumb:hover .x) { background: rgba(34,197,94,.18); }

  :global(.info-card) {
    pointer-events: none;
    background: rgba(248, 251, 255, 0.88);
    border-radius: 14px;
    padding: 10px 12px;
    box-shadow: 0 16px 38px rgba(15, 23, 42, 0.14);
    max-width: 420px;
  }
  :global(.info-title) { font-size: 12px; font-weight: 750; color: rgba(15,23,42,.9); margin-bottom: 4px; }
  :global(.info-sub) { font-size: 11px; color: rgba(100,116,139,.92); line-height: 1.35; }

  :global(.menu-pop) {
    position: absolute;
    top: 56px;
    right: 14px;
    width: 340px;
    background: rgba(255, 255, 255, 0.92);
    border-radius: 18px;
    padding: 12px;
    box-shadow: 0 22px 60px rgba(15, 23, 42, 0.18);
    backdrop-filter: blur(14px);
    pointer-events: auto;
    z-index: 10;
  }

  :global(.menu-title) {
    font-weight: 800;
    font-size: 13px;
    color: rgba(15, 23, 42, 0.9);
    margin-bottom: 10px;
  }

  :global(.sub) {
    margin-top: 10px;
    font-size: 12px;
    font-weight: 650;
    color: rgba(15, 23, 42, 0.78);
    display: flex;
    align-items: baseline;
    gap: 6px;
  }
  :global(.hint) { font-weight: 600; color: rgba(100, 116, 139, 0.9); }

  :global(.row) { display: flex; gap: 10px; align-items: center; margin-top: 10px; }
  :global(.row.two) { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }

  :global(.label) { font-size: 12px; color: rgba(15,23,42,.78); width: 52px; }

  :global(.sep) {
    height: 1px;
    background: rgba(226, 232, 240, 0.7);
    margin: 12px 0;
  }

  :global(.color-row) { display: flex; gap: 10px; align-items: center; width: 100%; }

  :global(.color-input) {
    width: 34px;
    height: 34px;
    border: 0;
    padding: 0;
    background: transparent;
    cursor: pointer;
    border-radius: 10px;
    overflow: hidden;
  }
  :global(.color-input::-webkit-color-swatch-wrapper) { padding: 0; }
  :global(.color-input::-webkit-color-swatch) { border: 0; border-radius: 10px; }

  :global(.hex) {
    flex: 1;
    border: 0;
    outline: none;
    background: rgba(248, 251, 255, 0.9);
    border-radius: 12px;
    padding: 10px 12px;
    font-size: 12px;
    color: rgba(15, 23, 42, 0.9);
  }

  :global(.select), :global(.input) {
    width: 100%;
    border: 0;
    background: rgba(248, 251, 255, 0.9);
    border-radius: 12px;
    padding: 10px 12px;
    font-size: 12px;
    outline: none;
    color: rgba(15,23,42,.9);
  }

  :global(.select:focus), :global(.input:focus) {
    box-shadow: 0 0 0 4px rgba(34, 197, 94, 0.14);
  }

  :global(.list) {
    margin-top: 8px;
    display: flex;
    flex-direction: column;
    gap: 6px;
    max-height: 190px;
    overflow: auto;
    padding-right: 2px;
  }

  :global(.item) {
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
  :global(.item:disabled) { opacity: .45; cursor: not-allowed; }

  :global(.name) { font-size: 12px; font-weight: 650; color: rgba(15,23,42,.88); }
  :global(.tag) { font-size: 11px; color: rgba(100,116,139,.9); }

  :global(.limit) {
    margin-top: 8px;
    font-size: 12px;
    color: rgba(100, 116, 139, 0.95);
    background: rgba(248, 251, 255, 0.92);
    border-radius: 14px;
    padding: 10px 12px;
  }

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

  :global(.modal-backdrop) {
    position: fixed;
    inset: 0;
    background: rgba(15, 23, 42, 0.35);
    display: grid;
    place-items: center;
    z-index: 50;
  }

  :global(.modal) {
    width: min(520px, calc(100vw - 24px));
    background: rgba(255,  255, 255, 0.92);
    border-radius: 18px;
    padding: 14px;
    box-shadow: 0 30px 80px rgba(15, 23, 42, 0.24);
    backdrop-filter: blur(14px);
  }

  :global(.modal-title) {
    font-weight: 800;
    font-size: 14px;
    color: rgba(15, 23, 42, 0.92);
    margin-bottom: 10px;
  }

  :global(.chip3d) {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 6px 10px;
    border-radius: 999px;
    background: rgba(255, 255, 255, 0.92);
    box-shadow: 0 10px 28px rgba(15, 23, 42, 0.14);
    font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial;
    font-size: 12px;
    font-weight: 720;
    color: rgba(15, 23, 42, 0.92);
    white-space: nowrap;
    transform: translate(-50%, -50%);
  }

  :global(.chip3d.accent) {
    background: rgba(248, 251, 255, 0.95);
    color: rgba(30, 64, 175, 0.98);
  }

  :global(.chip3d .x) {
    border: 0;
    width: 20px;
    height: 20px;
    border-radius: 999px;
    background: rgba(15,23,42,.06);
    color: rgba(15,23,42,.75);
    cursor: pointer;
    font-weight: 900;
    line-height: 1;
    display: grid;
    place-items: center;
  }
  :global(.chip3d .x:hover) { background: rgba(34,197,94,.18); color: rgba(5,46,22,.9); }
</style>
