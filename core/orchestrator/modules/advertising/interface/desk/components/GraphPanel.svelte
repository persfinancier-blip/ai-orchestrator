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

  // ✅ per-field colors (code -> hex)
  let entityFieldColors: Record<string, string> = {};
  const DEFAULT_ENTITY_COLOR = '#3b82f6';

  let visualSchemes: VisualScheme[] = [];
  let selectedVisualId = '';

  let datasetPresets: DatasetPreset[] = [];
  let selectedDatasetPresetId = '';

  let showDisplayMenu = false;
  let showPickDataMenu = false;
  let showGroupingMenu = false;

  let showSaveVisualModal = false;
  let saveVisualName = '';

  let showSaveDatasetModal = false;
  let saveDatasetName = '';

  let tooltip: TooltipState = { visible: false, x: 0, y: 0, lines: [] };

  let renderedCount = 0;
  let bboxLabel = '—';

  let scene: SpaceScene | null = null;

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

  // ---- FIXED caching: Map(point.id -> clusterId)
  let fixedAssign: Map<string, number> = new Map();
  let fixedCounts: Map<number, number> = new Map();
  let fixedConfigKey = '';

  function colorForCluster(id: number): string {
    if (id < 0) return '#94a3b8';
    const hue = (id * 47) % 360;
    return `hsl(${hue} 70% 45%)`;
  }

  function colorForEntityField(code: string): string {
    return entityFieldColors?.[code] ?? DEFAULT_ENTITY_COLOR;
  }

  function getTextValue(p: SpacePoint, field: string): string {
    if (p.sourceField === field) return p.label ?? '';
    return '';
  }

  function makeFixedConfigKey(cfg: GroupingConfig): string {
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
      cfg.recompute
    ].join('::');
  }

  function computeClustersNow(input: SpacePoint[]): { labels: Int32Array; counts: Map<number, number> } {
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

  function rebuildFixedAssignments(input: SpacePoint[]): void {
    const res = computeClustersNow(input);

    const assign = new Map<string, number>();
    for (let i = 0; i < input.length; i += 1) assign.set(input[i].id, res.labels[i]);

    fixedAssign = assign;
    fixedCounts = res.counts;
    fixedConfigKey = makeFixedConfigKey(grouping);
  }

  function applyClustering(input: SpacePoint[]): SpacePoint[] {
    // ✅ when grouping off: color by entity field (per-field color)
    if (!grouping.enabled) {
      return input.map((p) => ({
        ...p,
        color: colorForEntityField(p.sourceField)
      }));
    }

    if (grouping.recompute === 'fixed') {
      const cfgKey = makeFixedConfigKey(grouping);

      if (cfgKey !== fixedConfigKey) {
        fixedAssign = new Map();
        fixedCounts = new Map();
        fixedConfigKey = cfgKey;
      }

      if (fixedAssign.size === 0 && input.length) rebuildFixedAssignments(input);

      return input.map((p) => {
        const id = fixedAssign.get(p.id);
        const clusterId = typeof id === 'number' ? id : -1;

        const size = fixedCounts.get(clusterId) ?? 0;
        const effectiveId = clusterId >= 0 && size < grouping.minClusterSize ? -1 : clusterId;

        return { ...p, color: colorForCluster(effectiveId) };
      });
    }

    if (grouping.recompute === 'manual') {
      if (clusterSeed > 0) {
        rebuildFixedAssignments(input);
        clusterSeed = 0;
      }

      return input.map((p) => {
        const id = fixedAssign.get(p.id);
        const clusterId = typeof id === 'number' ? id : -1;

        const size = fixedCounts.get(clusterId) ?? 0;
        const effectiveId = clusterId >= 0 && size < grouping.minClusterSize ? -1 : clusterId;

        return { ...p, color: colorForCluster(effectiveId) };
      });
    }

    const res = computeClustersNow(input);

    return input.map((p, i) => {
      const clusterId = res.labels[i];
      const size = res.counts.get(clusterId) ?? 0;
      const effectiveId = clusterId >= 0 && size < grouping.minClusterSize ? -1 : clusterId;
      return { ...p, color: colorForCluster(effectiveId) };
    });
  }

  function recomputeClusters(): void {
    if (grouping.recompute === 'fixed') {
      rebuildFixedAssignments(points);
      return;
    }
    if (grouping.recompute === 'manual') {
      clusterSeed += 1;
      return;
    }
  }

  $: if (scene) {
    scene.setTheme({ bg: visualBg, edge: visualEdge });
    scene.setAxisCodes({ x: axisX, y: axisY, z: axisZ });

    const clustered = applyClustering(points);
    const info = scene.setPoints(clustered);

    renderedCount = info.renderedCount;
    bboxLabel = info.bboxLabel;
  }

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
      showGroupingMenu = false;
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
        bind:entityFieldColors
        defaultEntityColor={DEFAULT_ENTITY_COLOR}
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
  /* твой CSS без изменений */
</style>
