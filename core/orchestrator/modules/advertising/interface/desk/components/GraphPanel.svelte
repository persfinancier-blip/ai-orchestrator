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
  import GroupingMenu from './space/ui/GroupingMenu.svelte';
  import Tooltip from './space/ui/Tooltip.svelte';

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

  // ✅ ВАЖНО: по умолчанию группировка включена (как ты просил).
  // Выключение делается ползунком: detail=0.
  let grouping: GroupingConfig = {
    enabled: true,
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

  $: filteredRows = applyRowsFilter({
    rows: showcase?.rows ?? [],
    period,
    fromDate,
    toDate,
    search,
    selectedEntityFields
  });

  // ✅ ВОТ ГЛАВНОЕ: ползунок "Детализация групп" реально управляет voxel/LOD.
  // detail=0 => группировка выключена, точки обычные.
  $: points = buildPoints({
    rows: filteredRows,
    entityFields: selectedEntityFields,
    axisX,
    axisY,
    axisZ,
    numberFields: numberFieldsAll,
    dateFields: dateFieldsAll,

    // ✅ VOXEL/LOD (управляется ползунком "Детализация групп")
    lodEnabled: grouping.enabled && grouping.detail > 0,
    lodDetail: grouping.detail,
    lodMinCount: Math.max(2, grouping.minClusterSize)
  });

  let scene: SpaceScene | null = null;
  let elScene: HTMLDivElement | null = null;

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
    // хук под будущую логику (если понадобится)
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

  function calcAxisMaxOverride(): { xMax: number; yMax: number; zMax: number } | null {
    if (!axisX && !axisY && !axisZ) return null;

    const list = buildPoints({
      rows: filteredRows,
      entityFields: selectedEntityFields,
      axisX,
      axisY,
      axisZ,
      numberFields: numberFieldsAll,
      dateFields: dateFieldsAll,
      lodEnabled: false
    });

    const xMax = axisX ? Math.max(...list.map((p) => p.metrics[axisX]).filter((n) => Number.isFinite(n))) : Number.NaN;
    const yMax = axisY ? Math.max(...list.map((p) => p.metrics[axisY]).filter((n) => Number.isFinite(n))) : Number.NaN;
    const zMax = axisZ ? Math.max(...list.map((p) => p.metrics[axisZ]).filter((n) => Number.isFinite(n))) : Number.NaN;

    return {
      xMax: Number.isFinite(xMax) ? xMax : Number.NaN,
      yMax: Number.isFinite(yMax) ? yMax : Number.NaN,
      zMax: Number.isFinite(zMax) ? zMax : Number.NaN
    };
  }

  $: axisMaxOverride = calcAxisMaxOverride();

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
    scene.setPoints(points, { axisMaxOverride: axisMaxOverride ?? undefined });
  }
</script>

<div class="graph-panel">
  <div class="topbar">
    <div class="left">
      <button class="btn" type="button" on:click={openPickDataMenu}>Данные</button>
      <button class="btn" type="button" on:click={openDisplayMenu}>Вид</button>
      <button class="btn" type="button" on:click={openGroupingMenu}>Группировка</button>
      <button class="btn" type="button" on:click={resetView}>Сбросить вид</button>
    </div>
    <div class="right">
      <button class="btn" type="button" on:click={openSaveVisualModal}>Сохранить вид</button>
    </div>
  </div>

  <div class="scene-wrap">
    <div class="scene" bind:this={elScene} />

    {#if showDisplayMenu}
      <DisplayMenu {schemes} bind:activeSchemeId onClose={closeAllMenus} />
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
        numberFields={groupingNumberFields}
        textFields={groupingTextFields}
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
        // тут у тебя логика сохранения
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
