<!-- core/orchestrator/modules/advertising/interface/desk/components/GraphPanel.svelte -->
<script lang="ts">
  import { onDestroy, onMount, tick } from 'svelte';
  import { get } from 'svelte/store';

  import {
    showcaseStore,
    fieldName,
    type DatasetId,
    type ShowcaseField
  } from '../data/showcaseStore';

  import type {
    DatasetPreset,
    PeriodMode,
    TooltipState,
    VisualScheme,
    ShowcaseSafe,
    SpacePoint
  } from './space/types';

  import { applyRowsFilter, buildPoints } from './space/pipeline';
  import { loadDatasetPresets, loadVisualSchemes } from './space/presetsStorage';
  import { SpaceScene } from './space/three/SpaceScene';

  import DisplayMenu from './space/ui/DisplayMenu.svelte';
  import PickDataMenu from './space/ui/PickDataMenu.svelte';
  import SaveModal from './space/ui/SaveModal.svelte';
  import GroupingMenu from './space/ui/GroupingMenu.svelte';
  import Tooltip from './space/ui/Tooltip.svelte';

  import InfoCard from './space/ui/InfoCard.svelte';
  import Crumbs from './space/ui/Crumbs.svelte';

  import type { GroupingConfig } from './space/cluster/types';

  let showcase: ShowcaseSafe | null = null;

  let activeLayers: DatasetId[] = [];
  let selectedEntityFields: string[] = [];

  let axisX = '';
  let axisY = '';
  let axisZ = '';

  let period: PeriodMode = '30 дней';
  let fromDate = '';
  let toDate = '';
  let search = '';

  let tooltip: TooltipState = { visible: false, x: 0, y: 0, lines: [] };

  let schemes: VisualScheme[] = [];
  let activeSchemeId = 'default';

  let presets: DatasetPreset[] = [];

  let showDisplayMenu = false;
  let showPickDataMenu = false;
  let showSaveVisualModal = false;
  let showGroupingMenu = false;

  // ✅ группировка включена по умолчанию
  let grouping: GroupingConfig = {
    enabled: true,
    principle: 'proximity',
    featureFields: [],
    detail: 0.45,          // 0..1 (0 = без группировки)
    customWeights: false,
    wX: 1,
    wY: 1,
    wZ: 1,
    minClusterSize: 5,
    recompute: 'auto'
  };

  let clusterSeed = 0;

  let scene: SpaceScene | null = null;
  let elScene: HTMLDivElement | null = null;

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

  $: filteredRows = applyRowsFilter({
    rows: showcase?.rows ?? [],
    period,
    fromDate,
    toDate,
    search,
    selectedEntityFields
  });

  // ✅ ВАЖНО: points пересчитываются реактивно при любом изменении данных/осей/слоёв/ползунка
  $: points = buildPoints({
    rows: filteredRows,
    entityFields: selectedEntityFields,
    axisX,
    axisY,
    axisZ,
    numberFields: numberFieldsAll,
    dateFields: dateFieldsAll,

    // ✅ Воксель-LOD: подключили “Детализация групп” прямо к buildPoints()
    // выключение = detail = 0
    lodEnabled: grouping.enabled && grouping.detail > 0,
    lodDetail: grouping.detail,
    lodMinCount: Math.max(2, grouping.minClusterSize)
  });

  function closeAllMenus(): void {
    showDisplayMenu = false;
    showPickDataMenu = false;
    showSaveVisualModal = false;
    showGroupingMenu = false;
  }

  function openDisplayMenu(): void {
    closeAllMenus();
    showDisplayMenu = true;
  }

  function openPickDataMenu(): void {
    closeAllMenus();
    showPickDataMenu = true;
  }

  function openSaveVisualModal(): void {
    closeAllMenus();
    showSaveVisualModal = true;
  }

  function openGroupingMenu(): void {
    closeAllMenus();
    showGroupingMenu = true;
  }

  function addEntityField(code: string): void {
    if (!code) return;
    if (selectedEntityFields.includes(code)) return;
    selectedEntityFields = [...selectedEntityFields, code];
  }

  function addCoordField(axis: 'x' | 'y' | 'z', code: string): void {
    if (axis === 'x') axisX = code;
    if (axis === 'y') axisY = code;
    if (axis === 'z') axisZ = code;
  }

  function removeAxis(axis: 'x' | 'y' | 'z'): void {
    if (axis === 'x') axisX = '';
    if (axis === 'y') axisY = '';
    if (axis === 'z') axisZ = '';
  }

  function recomputeClusters(): void {
    // если режим "вручную" — можно дёргать seed (на будущее)
    clusterSeed += 1;
  }

  function toggleLayer(id: DatasetId): void {
    if (activeLayers.includes(id)) activeLayers = activeLayers.filter((x) => x !== id);
    else activeLayers = [...activeLayers, id];
  }

  function onTooltip(payload: TooltipState): void {
    tooltip = payload;
  }

  function resetView(): void {
    scene?.resetView();
  }

  onMount(async () => {
    showcase = get(showcaseStore) as ShowcaseSafe;
    schemes = loadVisualSchemes();
    presets = loadDatasetPresets();

    await tick();

    if (elScene) {
      scene = new SpaceScene(
        { fieldName, getFields: () => (showcase?.fields ?? []) as ShowcaseField[] },
        { onTooltip, onAxisRemove: removeAxis }
      );
      scene.init(elScene, { height: 560 });
    }
  });

  onDestroy(() => {
    scene?.dispose();
    scene = null;
  });

  $: if (scene) {
    const scheme =
      schemes.find((s) => s.id === activeSchemeId) ??
      schemes[0] ??
      { id: 'default', name: 'Default', bg: '#ffffff', edge: '#334155' };

    scene.setTheme({ bg: scheme.bg, edge: scheme.edge });
    scene.setAxisCodes({ x: axisX, y: axisY, z: axisZ });

    // ✅ вот здесь и происходит реальная перерисовка при любом изменении points
    scene.setPoints(points);
  }
</script>

<div class="graph-panel">
  <div class="topbar">
    <div class="left">
      <button class="btn" type="button" on:click={openPickDataMenu}>Выбрать данные</button>
      <button class="btn" type="button" on:click={openDisplayMenu}>Настройка отображения</button>
      <button class="btn" type="button" on:click={openGroupingMenu}>Формирование групп</button>
    </div>

    <div class="right">
      <button class="btn" type="button" on:click={openSaveVisualModal}>Сохранить вид</button>
    </div>
  </div>

  <div class="scene-wrap">
    <div class="scene" bind:this={elScene} />

    <InfoCard
      pointsCount={points.length}
      axisLabel={`Оси: ${axisX || '—'} / ${axisY || '—'} / ${axisZ || '—'}`}
    />

    <Crumbs items={selectedEntityFields} />

    {#if showDisplayMenu}
      <DisplayMenu
        {schemes}
        bind:activeSchemeId
        onClose={closeAllMenus}
      />
    {/if}

    {#if showPickDataMenu}
      <PickDataMenu
        {activeLayers}
        {selectedEntityFields}
        {axisX}
        {axisY}
        {axisZ}
        {period}
        {fromDate}
        {toDate}
        {search}
        {textFieldsAll}
        {coordFieldsAll}
        onToggleLayer={toggleLayer}
        onAddEntity={addEntityField}
        onAddCoord={addCoordField}
        onClose={closeAllMenus}
      />
    {/if}

    {#if showGroupingMenu}
      <GroupingMenu
        bind:cfg={grouping}
        numberFields={numberFieldsAll}
        textFields={textFieldsAll}
        onRecompute={recomputeClusters}
      />
    {/if}

    <Tooltip {tooltip} />
  </div>

  {#if showSaveVisualModal}
    <SaveModal
      title="Сохранить визуальную схему"
      bind:show={showSaveVisualModal}
      onSave={(name) => {
        // если у тебя есть сохранение — вставь сюда
        closeAllMenus();
      }}
    />
  {/if}
</div>

<style>
  .graph-panel { display: flex; flex-direction: column; gap: 10px; }
  .topbar { display: flex; justify-content: space-between; align-items: center; gap: 10px; }
  .btn { padding: 8px 12px; border-radius: 8px; border: 1px solid #e2e8f0; background: #fff; cursor: pointer; }
  .scene-wrap { position: relative; }
  .scene { width: 100%; }
</style>
