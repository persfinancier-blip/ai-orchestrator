<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import * as THREE from 'three';
  import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
  import type { DatasetId, FieldDef } from '../data/showcase';
  import { DATASETS, FIELDS, fieldLabel } from '../data/showcase';
  import { generateBuyerCloud, type BuyerPoint } from '../data/mockGraph';
  import { loadSpacePoints, type PanelFilter, type SpacePoint } from '../data/spaceApi';

  type PeriodMode = '7 дней' | '14 дней' | '30 дней' | 'Даты';
  type DateOperator = 'с … по …';
  type TextOperator = 'равно' | 'не равно' | 'содержит' | 'не содержит' | 'в списке';
  type NumberOperator = '>' | '<' | '≥' | '≤' | 'между';
  type FilterTypeLabel = 'Дата' | 'Текст' | 'Число';

  type PanelSettings = {
    id: string;
    name: string;
    entityField: string;
    layers: DatasetId[];
    axisX: string;
    axisY: string;
    axisZ: string;
    period: PeriodMode;
    fromDate: string;
    toDate: string;
    filters: PanelFilter[];
  };

  const STORAGE_KEY = 'desk-space-settings-v1';
  const buyers = generateBuyerCloud(120);

  let settingsList: PanelSettings[] = [];
  let selectedSettingId = '';
  let showLayerMenu = false;
  let showFilterTypeMenu = false;

  let entityFieldCode = 'sku';
  let activeLayers: DatasetId[] = ['sales_fact', 'ads'];
  let axisX = 'revenue';
  let axisY = 'spend';
  let axisZ = 'roi';
  let period: PeriodMode = '30 дней';
  let fromDate = '2026-01-01';
  let toDate = '2026-01-30';
  let filters: PanelFilter[] = [];
  let search = '';

  let points: SpacePoint[] = [];
  let projected: Array<SpacePoint & { px: number; py: number; pz: number }> = [];
  let isLoading = false;
  let errorMessage = '';

  let container3d: HTMLDivElement;
  let tooltip = { visible: false, x: 0, y: 0, lines: [] as string[] };

  const allEntityFields = filterFields('entity', 'dimension', 'string');
  let layerFieldCodes = new Set<string>();
  let entityFields: FieldDef[] = [];
  let axisFields: FieldDef[] = [];
  let textFilterFields: FieldDef[] = [];
  let numberFilterFields: FieldDef[] = [];
  let dateFilterFields: FieldDef[] = [];

  $: layerFieldCodes = new Set(FIELDS.filter((f) => f.datasetIds.some((id) => activeLayers.includes(id))).map((f) => f.code));
  $: entityFields = allEntityFields.filter((f) => layerFieldCodes.has(f.code));
  $: axisFields = filterFields('axis', 'measure', 'number').filter((f) => layerFieldCodes.has(f.code));
  $: textFilterFields = filterFields('filter', 'dimension', 'string').filter((f) => layerFieldCodes.has(f.code));
  $: numberFilterFields = filterFields('filter', 'measure', 'number').filter((f) => layerFieldCodes.has(f.code));
  $: dateFilterFields = filterFields('filter', 'time', 'date').filter((f) => layerFieldCodes.has(f.code));

  $: if (!entityFields.find((f) => f.code === entityFieldCode)) entityFieldCode = entityFields[0]?.code ?? 'sku';
  $: if (!axisFields.find((f) => f.code === axisX)) axisX = axisFields[0]?.code ?? 'revenue';
  $: if (!axisFields.find((f) => f.code === axisY)) axisY = axisFields[1]?.code ?? axisFields[0]?.code ?? 'spend';
  $: if (!axisFields.find((f) => f.code === axisZ)) axisZ = axisFields[2]?.code ?? axisFields[0]?.code ?? 'roi';

  $: projected = projectEntities(applyClientFilters(points));
  $: rebuild3d();

  function filterFields(role: 'entity' | 'axis' | 'filter', fieldType: FieldDef['fieldType'], valueType: FieldDef['valueType']): FieldDef[] {
    return FIELDS.filter((f) => f.allowedRoles.includes(role) && f.fieldType === fieldType && f.valueType === valueType);
  }

  function entityValue(point: SpacePoint, code: string): string {
    if (code === 'sku') return point.sku;
    if (code === 'campaign_id') return point.campaign_id;
    if (code === 'subject') return point.subject;
    if (code === 'category') return point.category;
    if (code === 'brand') return point.brand;
    if (code === 'entry_point') return point.entry_point;
    if (code === 'keyword_cluster') return point.keyword_cluster;
    if (code === 'search_query') return point.search_query;
    return point.name;
  }

  function applyClientFilters(list: SpacePoint[]): SpacePoint[] {
    const query = search.toLowerCase().trim();
    return list.filter((item) => {
      if (!query) return true;
      return entityValue(item, entityFieldCode).toLowerCase().includes(query);
    });
  }

  async function reloadFromApi(): Promise<void> {
    isLoading = true;
    errorMessage = '';
    try {
      points = await loadSpacePoints({ entityField: entityFieldCode, datasets: activeLayers, x: axisX, y: axisY, z: axisZ, period, fromDate, toDate, filters });
    } catch (error) {
      points = [];
      errorMessage = 'Нет данных. Проверьте подключение к витрине.';
      console.error(error);
    } finally {
      isLoading = false;
    }
  }

  $: {
    entityFieldCode; activeLayers; axisX; axisY; axisZ; period; fromDate; toDate; filters;
    reloadFromApi();
  }

  function projectEntities(list: SpacePoint[]): Array<SpacePoint & { px: number; py: number; pz: number }> {
    if (!list.length) return [];
    const xs = list.map((i) => i.metrics[axisX] ?? 0);
    const ys = list.map((i) => i.metrics[axisY] ?? 0);
    const zs = list.map((i) => i.metrics[axisZ] ?? 0);
    const [minX, maxX] = [Math.min(...xs), Math.max(...xs)];
    const [minY, maxY] = [Math.min(...ys), Math.max(...ys)];
    const [minZ, maxZ] = [Math.min(...zs), Math.max(...zs)];

    return list.map((i) => ({
      ...i,
      px: normalize(i.metrics[axisX] ?? 0, minX, maxX),
      py: normalize(i.metrics[axisY] ?? 0, minY, maxY),
      pz: normalize(i.metrics[axisZ] ?? 0, minZ, maxZ),
    }));
  }

  const normalize = (v: number, min: number, max: number): number => ((v - min) / Math.max(max - min, 0.0001) - 0.5) * 180;
  const formatDate = (d: string): string => `${d.slice(8, 10)}.${d.slice(5, 7)}.${d.slice(0, 4)}`;

  function fmt(v: number): string {
    return v.toLocaleString('ru-RU', { maximumFractionDigits: 2 });
  }

  function formatTooltip(point: SpacePoint): string[] {
    return [
      point.name,
      `${fieldLabel(axisX)}: ${fmt(point.metrics[axisX] ?? 0)} ₽`,
      `${fieldLabel(axisY)}: ${fmt(point.metrics[axisY] ?? 0)} ₽`,
      `${fieldLabel(axisZ)}: ${fmt(point.metrics[axisZ] ?? 0)}`,
      `Дата: ${formatDate(point.date)}`,
    ];
  }

  let renderer: THREE.WebGLRenderer;
  let scene: THREE.Scene;
  let camera: THREE.PerspectiveCamera;
  let controls: OrbitControls;
  let buyersMesh: THREE.InstancedMesh;
  let entitiesMesh: THREE.InstancedMesh;
  let anim = 0;
  const raycaster = new THREE.Raycaster();
  const ndc = new THREE.Vector2();
  let renderPoints: BuyerPoint[] = [];
  let renderEntities: Array<SpacePoint & { px: number; py: number; pz: number }> = [];

  function init3d(): void {
    const width = container3d.clientWidth;
    const height = 560;
    scene = new THREE.Scene();
    scene.background = new THREE.Color('#f8fbff');
    camera = new THREE.PerspectiveCamera(52, width / height, 0.1, 1800);
    camera.position.set(0, 80, 210);
    renderer = new THREE.WebGLRenderer({ antialias: true });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    renderer.setSize(width, height);
    container3d.innerHTML = '';
    container3d.appendChild(renderer.domElement);
    controls = new OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    scene.add(new THREE.AmbientLight('#ffffff', 0.95));
    scene.add(new THREE.GridHelper(260, 16, '#d6deea', '#eaf0f7'));
    renderer.domElement.addEventListener('mousemove', onMove3d);
    animate();
  }

  function clearMesh(mesh?: THREE.InstancedMesh): void {
    if (!mesh) return;
    scene.remove(mesh);
    mesh.geometry.dispose();
    (mesh.material as THREE.Material).dispose();
  }

  function rebuild3d(): void {
    if (!scene) return;
    clearMesh(buyersMesh);
    clearMesh(entitiesMesh);
    renderPoints = activeLayers.includes('sales_fact') ? buyers.points : [];
    renderEntities = projected;

    if (renderPoints.length) {
      buyersMesh = new THREE.InstancedMesh(new THREE.SphereGeometry(0.7, 6, 6), new THREE.MeshBasicMaterial({ color: '#60a5fa', transparent: true, opacity: 0.2 }), renderPoints.length);
      const d = new THREE.Object3D();
      renderPoints.forEach((p, i) => { d.position.set(p.x, p.y, p.z); d.updateMatrix(); buyersMesh.setMatrixAt(i, d.matrix); });
      scene.add(buyersMesh);
    }

    if (renderEntities.length) {
      entitiesMesh = new THREE.InstancedMesh(new THREE.OctahedronGeometry(1.05, 0), new THREE.MeshBasicMaterial({ vertexColors: true }), renderEntities.length);
      entitiesMesh.instanceColor = new THREE.InstancedBufferAttribute(new Float32Array(renderEntities.length * 3), 3);
      const d = new THREE.Object3D();
      renderEntities.forEach((e, i) => {
        d.position.set(e.px, e.py, e.pz);
        d.scale.set(e.entityType === 'campaign' ? 1.7 : 1.25, e.entityType === 'campaign' ? 1.7 : 1.25, e.entityType === 'campaign' ? 1.7 : 1.25);
        d.updateMatrix();
        entitiesMesh.setMatrixAt(i, d.matrix);
        entitiesMesh.setColorAt(i, e.entityType === 'campaign' ? new THREE.Color('#1e40af') : new THREE.Color('#22c55e'));
      });
      scene.add(entitiesMesh);
    }
  }

  function onMove3d(event: MouseEvent): void {
    if (!entitiesMesh) return;
    const rect = renderer.domElement.getBoundingClientRect();
    ndc.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
    ndc.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;
    raycaster.setFromCamera(ndc, camera);
    const hit = raycaster.intersectObject(entitiesMesh, true)[0];
    tooltip = hit && hit.instanceId !== undefined
      ? { visible: true, x: event.clientX - rect.left + 12, y: event.clientY - rect.top + 12, lines: formatTooltip(renderEntities[hit.instanceId]) }
      : { ...tooltip, visible: false };
  }

  function animate(): void { controls?.update(); renderer?.render(scene, camera); anim = requestAnimationFrame(animate); }
  function onResize(): void { if (!renderer || !camera || !container3d) return; const width = container3d.clientWidth; renderer.setSize(width, 560); camera.aspect = width / 560; camera.updateProjectionMatrix(); }

  function addLayer(layerId: DatasetId): void {
    if (activeLayers.includes(layerId)) return;
    activeLayers = [...activeLayers, layerId];
    showLayerMenu = false;
  }
  const removeLayer = (id: DatasetId): void => { if (activeLayers.length > 1) activeLayers = activeLayers.filter((x) => x !== id); };

  function addFilter(typeLabel: FilterTypeLabel): void {
    const id = `${Date.now()}-${Math.random()}`;
    if (typeLabel === 'Дата') filters = [...filters, { id, filterType: 'date', fieldCode: 'date', operator: 'с … по …', valueA: fromDate, valueB: toDate }];
    if (typeLabel === 'Текст') filters = [...filters, { id, filterType: 'text', fieldCode: textFilterFields[0]?.code ?? 'subject', operator: 'содержит', valueA: '', valueB: '' }];
    if (typeLabel === 'Число') filters = [...filters, { id, filterType: 'number', fieldCode: numberFilterFields[0]?.code ?? 'revenue', operator: '>', valueA: '', valueB: '' }];
    showFilterTypeMenu = false;
  }
  const updateFilter = (id: string, patch: Partial<PanelFilter>): void => (filters = filters.map((f) => (f.id === id ? { ...f, ...patch } : f)));
  const removeFilter = (id: string): void => (filters = filters.filter((f) => f.id !== id));

  function readSettings(): void { try { settingsList = JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]'); } catch { settingsList = []; } }
  function writeSettings(): void { localStorage.setItem(STORAGE_KEY, JSON.stringify(settingsList)); }
  function saveCurrent(): void {
    const name = window.prompt('Название настройки', `Настройка ${settingsList.length + 1}`)?.trim() || `Настройка ${settingsList.length + 1}`;
    const item: PanelSettings = { id: `${Date.now()}-${Math.random()}`, name, entityField: entityFieldCode, layers: [...activeLayers], axisX, axisY, axisZ, period, fromDate, toDate, filters: structuredClone(filters) };
    settingsList = [item, ...settingsList].slice(0, 20);
    selectedSettingId = item.id;
    writeSettings();
  }
  function onSettingChange(): void {
    const i = settingsList.find((x) => x.id === selectedSettingId);
    if (!i) return;
    entityFieldCode = i.entityField; activeLayers = i.layers; axisX = i.axisX; axisY = i.axisY; axisZ = i.axisZ; period = i.period; fromDate = i.fromDate; toDate = i.toDate; filters = structuredClone(i.filters);
  }
  function removeSetting(): void { settingsList = settingsList.filter((x) => x.id !== selectedSettingId); selectedSettingId = ''; writeSettings(); }
  function resetPanel(): void { entityFieldCode = 'sku'; activeLayers = ['sales_fact', 'ads']; axisX = 'revenue'; axisY = 'spend'; axisZ = 'roi'; period = '30 дней'; fromDate = '2026-01-01'; toDate = '2026-01-30'; filters = []; search = ''; }

  onMount(() => { init3d(); readSettings(); window.addEventListener('resize', onResize); });
  onDestroy(() => { cancelAnimationFrame(anim); window.removeEventListener('resize', onResize); renderer?.dispose(); controls?.dispose(); });
</script>

<section class="graph-root panel">
  <div class="module-head">
    <h2>Пространство</h2>
    <div class="status">Точек: {projected.length} · Слои: {activeLayers.map((layer) => DATASETS.find((item) => item.id === layer)?.name).join(', ')} · Оси: {fieldLabel(axisX)} / {fieldLabel(axisY)} / {fieldLabel(axisZ)}</div>
  </div>
  <div class="content">
    <div class="stage">
      <div bind:this={container3d} class="scene" />
      {#if isLoading}<div class="overlay">Загрузка данных…</div>{/if}
      {#if errorMessage}<div class="overlay error">{errorMessage}</div>{/if}
      {#if tooltip.visible}<div class="tooltip" style={`left:${tooltip.x}px;top:${tooltip.y}px`}>{#each tooltip.lines as line}<div>{line}</div>{/each}</div>{/if}
    </div>

    <aside class="control-panel">
      <section class="section">
        <h4>Настройки</h4>
        <small>Сохранение и загрузка конфигурации панели.</small>
        <select bind:value={selectedSettingId} on:change={onSettingChange}><option value="">Выберите сохранённую настройку</option>{#each settingsList as s}<option value={s.id}>{s.name}</option>{/each}</select>
        <div class="row-buttons"><button on:click={saveCurrent}>Сохранить текущую</button><button on:click={removeSetting} disabled={!selectedSettingId}>Удалить</button><button on:click={resetPanel}>Сбросить</button></div>
      </section>

      <section class="section"><h4>Точки на графе</h4><small>Что является точками в пространстве.</small><select bind:value={entityFieldCode}>{#each entityFields as f}<option value={f.code}>{f.name}</option>{/each}</select></section>

      <section class="section">
        <h4>Набор данных</h4><small>Из каких источников берутся метрики.</small>
        <div class="layer-list">{#each activeLayers as layer}<div class="layer-item"><span>{DATASETS.find((d) => d.id === layer)?.name}</span><button on:click={() => removeLayer(layer)}>✕</button></div>{/each}</div>
        <div class="menu-wrap"><button on:click={() => (showLayerMenu = !showLayerMenu)}>+ Добавить слой</button>{#if showLayerMenu}<div class="dropdown">{#each DATASETS.filter((d) => !activeLayers.includes(d.id)) as d}<button on:click={() => addLayer(d.id)}>{d.name}</button>{/each}</div>{/if}</div>
      </section>

      <section class="section">
        <h4>Координаты</h4><small>Какие числовые метрики формируют оси X/Y/Z.</small>
        <label>Ось X<select bind:value={axisX}>{#each axisFields as f}<option value={f.code}>{f.name}</option>{/each}</select></label>
        <label>Ось Y<select bind:value={axisY}>{#each axisFields as f}<option value={f.code}>{f.name}</option>{/each}</select></label>
        <label>Ось Z<select bind:value={axisZ}>{#each axisFields as f}<option value={f.code}>{f.name}</option>{/each}</select></label>
      </section>

      <section class="section">
        <h4>Период и фильтры</h4><small>Ограничивает данные для расчёта.</small>
        <label>Период<select bind:value={period}><option>7 дней</option><option>14 дней</option><option>30 дней</option><option>Даты</option></select></label>
        {#if period === 'Даты'}<div class="dates"><input type="date" bind:value={fromDate} /><input type="date" bind:value={toDate} /></div>{/if}
        <input bind:value={search} placeholder="Поиск SKU / РК" />
        <div class="menu-wrap"><button on:click={() => (showFilterTypeMenu = !showFilterTypeMenu)}>+ Добавить фильтр</button>{#if showFilterTypeMenu}<div class="dropdown"><button on:click={() => addFilter('Дата')}>Дата</button><button on:click={() => addFilter('Текст')}>Текст</button><button on:click={() => addFilter('Число')}>Число</button></div>{/if}</div>

        {#each filters as filter}
          <div class="filter-item">
            {#if filter.filterType === 'date'}
              <select bind:value={filter.fieldCode}>{#each dateFilterFields as f}<option value={f.code}>{f.name}</option>{/each}</select>
              <span>с … по …</span>
              <input type="date" bind:value={filter.valueA} on:input={(e) => updateFilter(filter.id, { valueA: e.currentTarget.value })} />
              <input type="date" bind:value={filter.valueB} on:input={(e) => updateFilter(filter.id, { valueB: e.currentTarget.value })} />
            {:else if filter.filterType === 'text'}
              <select bind:value={filter.fieldCode}>{#each textFilterFields as f}<option value={f.code}>{f.name}</option>{/each}</select>
              <select bind:value={filter.operator}>{#each ['равно','не равно','содержит','не содержит','в списке'] as op}<option>{op}</option>{/each}</select>
              <input type="text" bind:value={filter.valueA} on:input={(e) => updateFilter(filter.id, { valueA: e.currentTarget.value })} />
            {:else}
              <select bind:value={filter.fieldCode}>{#each numberFilterFields as f}<option value={f.code}>{f.name}</option>{/each}</select>
              <select bind:value={filter.operator}>{#each ['>','<','≥','≤','между'] as op}<option>{op}</option>{/each}</select>
              <input type="number" bind:value={filter.valueA} on:input={(e) => updateFilter(filter.id, { valueA: e.currentTarget.value })} />
              {#if filter.operator === 'между'}<input type="number" bind:value={filter.valueB} on:input={(e) => updateFilter(filter.id, { valueB: e.currentTarget.value })} />{/if}
            {/if}
            <button on:click={() => removeFilter(filter.id)}>Удалить</button>
          </div>
        {/each}
      </section>
    </aside>
  </div>
</section>

<style>
  .graph-root { display:flex; flex-direction:column; gap:10px; }
  .module-head { display:flex; justify-content:space-between; align-items:center; gap:10px; }
  .module-head h2 { margin:0; font-size:24px; }
  .status { font-size:12px; color:#64748b; text-align:right; }
  .content { display:flex; align-items:stretch; gap:12px; }
  .stage { position:relative; flex:1; min-width:0; }
  .scene { width:100%; height:560px; border:1px solid #e2e8f0; border-radius:16px; background:#f8fbff; }
  .control-panel { width:360px; height:560px; border:1px solid #e2e8f0; border-radius:14px; padding:10px; background:#fbfdff; display:flex; flex-direction:column; gap:10px; overflow:auto; }
  .section { border:1px solid #e7edf6; border-radius:10px; padding:8px; display:flex; flex-direction:column; gap:6px; }
  .section h4 { margin:0; font-size:13px; }
  .section label { display:flex; flex-direction:column; gap:4px; font-size:12px; }
  .section select, .section input, .section button { border:1px solid #dbe4f0; border-radius:10px; padding:6px 8px; background:#fff; font-size:12px; }
  .row-buttons, .dates { display:flex; gap:6px; }
  .layer-item { display:flex; justify-content:space-between; align-items:center; font-size:12px; }
  .menu-wrap { position:relative; }
  .dropdown { position:absolute; top:100%; left:0; right:0; z-index:4; display:flex; flex-direction:column; gap:4px; background:#fff; border:1px solid #dbe4f0; border-radius:10px; padding:6px; }
  .filter-item { border:1px solid #e7edf6; border-radius:8px; padding:6px; display:flex; flex-direction:column; gap:6px; }
  .tooltip { position:absolute; pointer-events:none; background:rgba(15,23,42,.94); color:#f8fafc; font-size:12px; padding:10px; border-radius:10px; max-width:360px; }
  .overlay { position:absolute; inset:0; display:flex; align-items:center; justify-content:center; background:rgba(248,251,255,.78); border-radius:16px; font-weight:600; }
  .overlay.error { color:#b91c1c; }
  small { color:#64748b; font-size:11px; }
</style>
