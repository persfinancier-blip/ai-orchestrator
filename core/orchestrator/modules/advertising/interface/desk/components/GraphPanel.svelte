<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import * as THREE from 'three';
  import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
  import type { DatasetId, FieldDef } from '../data/showcase';
  import { DATASETS, FIELDS, fieldLabel } from '../data/showcase';
  import type { BuyerPoint, MeasureField, SimilarityEntity } from '../data/mockGraph';
  import { generateBuyerCloud, generateSimilarityData, getMeasure } from '../data/mockGraph';

  export let entities: SimilarityEntity[] = [];

  type PeriodMode = '7 дней' | '14 дней' | '30 дней' | 'Даты';
  type FilterType = 'date' | 'text' | 'number';
  type DateOperator = 'с' | 'по';
  type TextOperator = 'равно' | 'не равно' | 'содержит' | 'не содержит' | 'в списке';
  type NumberOperator = '>' | '<' | '≥' | '≤' | 'между';

  type PanelFilter = {
    id: string;
    filterType: FilterType;
    fieldCode: string;
    operator: DateOperator | TextOperator | NumberOperator;
    valueA: string;
    valueB: string;
  };

  type PanelSettings = {
    id: string;
    name: string;
    entityField: string;
    layers: DatasetId[];
    axisX: MeasureField;
    axisY: MeasureField;
    axisZ: MeasureField;
    period: PeriodMode;
    fromDate: string;
    toDate: string;
    filters: PanelFilter[];
    demoMode: boolean;
  };

  const STORAGE_KEY = 'desk-space-settings-v1';

  let settingsList: PanelSettings[] = [];
  let selectedSettingId = '';

  let entityFieldCode = 'sku';
  let activeLayers: DatasetId[] = ['sales_fact', 'ads'];

  let axisX: MeasureField = 'revenue';
  let axisY: MeasureField = 'spend';
  let axisZ: MeasureField = 'roi';

  let period: PeriodMode = '30 дней';
  let fromDate = '2026-02-01';
  let toDate = '2026-02-28';

  let filters: PanelFilter[] = [];
  let demoMode = true;
  let search = '';

  let container3d: HTMLDivElement;
  let searchInput: HTMLInputElement;

  const buyersFull = generateBuyerCloud(220);
  const buyersDemo = generateBuyerCloud(40);

  let sourceEntities: SimilarityEntity[] = [];
  let filteredEntities: SimilarityEntity[] = [];
  let projected: Array<SimilarityEntity & { px: number; py: number; pz: number }> = [];

  let tooltip = { visible: false, x: 0, y: 0, lines: [] as string[] };

  const allEntityFields = filterFields('entity', 'dimension', 'string');

  let layerFieldCodes = new Set<string>();
  let entityFields: FieldDef[] = [];
  let axisFields: FieldDef[] = [];
  let textFilterFields: FieldDef[] = [];
  let numberFilterFields: FieldDef[] = [];
  let dateFilterFields: FieldDef[] = [];

  $: layerFieldCodes = new Set(FIELDS.filter((field) => field.datasetIds.some((datasetId) => activeLayers.includes(datasetId))).map((field) => field.code));
  $: entityFields = allEntityFields.filter((field) => layerFieldCodes.has(field.code));
  $: axisFields = filterFields('axis', 'measure', 'number').filter((field) => layerFieldCodes.has(field.code));
  $: textFilterFields = filterFields('filter', 'dimension', 'string').filter((field) => layerFieldCodes.has(field.code));
  $: numberFilterFields = filterFields('filter', 'measure', 'number').filter((field) => layerFieldCodes.has(field.code));
  $: dateFilterFields = filterFields('filter', 'time', 'date').filter((field) => layerFieldCodes.has(field.code));

  $: if (!axisFields.find((f) => f.code === axisX) && axisFields[0]) axisX = axisFields[0].code as MeasureField;
  $: if (!axisFields.find((f) => f.code === axisY) && axisFields[1]) axisY = axisFields[1].code as MeasureField;
  $: if (!axisFields.find((f) => f.code === axisZ) && axisFields[2]) axisZ = axisFields[2].code as MeasureField;
  $: if (!entityFields.find((field) => field.code === entityFieldCode)) entityFieldCode = entityFields[0]?.code ?? 'sku';

  $: sourceEntities = demoMode
    ? [...entities.filter((entity) => entity.type === 'item').slice(0, 10), ...entities.filter((entity) => entity.type === 'campaign').slice(0, 10)]
    : entities;

  $: filteredEntities = applyFilters(sourceEntities);
  $: projected = projectEntities(filteredEntities);
  $: rebuild3d();

  function filterFields(role: 'entity' | 'axis' | 'filter', fieldType: FieldDef['fieldType'], valueType: FieldDef['valueType']): FieldDef[] {
    return FIELDS.filter(
      (field) => field.allowedRoles.includes(role) && field.fieldType === fieldType && field.valueType === valueType
    );
  }

  function entityValue(entity: SimilarityEntity, code: string): string {
    if (code === 'sku') return entity.sku || entity.id;
    if (code === 'subject') return entity.subject;
    if (code === 'category') return entity.category;
    if (code === 'brand') return entity.brand;
    if (code === 'campaign_id') return entity.campaign_id || entity.id;
    if (code === 'entry_point') return entity.entry_point;
    if (code === 'keyword_cluster') return entity.keyword_cluster;
    if (code === 'search_query') return entity.search_query;
    return entity.name;
  }

  function inPeriod(date: string): boolean {
    if (period === 'Даты') return date >= fromDate && date <= toDate;
    const day = Number(date.slice(-2));
    if (period === '7 дней') return day >= 22;
    if (period === '14 дней') return day >= 15;
    return true;
  }

  function allowsEntityByLayer(entity: SimilarityEntity): boolean {
    const wantsSales = activeLayers.includes('sales_fact');
    const wantsAds = activeLayers.includes('ads');
    if (wantsSales && wantsAds) return true;
    if (wantsSales && entity.type === 'item') return true;
    if (wantsAds && entity.type === 'campaign') return true;
    if (wantsSales && wantsAds === false && entity.type === 'campaign' && entityFieldCode !== 'campaign_id') return false;
    return false;
  }

  function measureValue(entity: SimilarityEntity, code: string): number {
    return getMeasure(entity, code as MeasureField);
  }

  function checkFilter(entity: SimilarityEntity, filter: PanelFilter): boolean {
    if (filter.filterType === 'date') {
      const value = entity.date;
      const start = filter.valueA;
      if (!start) return true;
      if (filter.operator === 'с') return value >= start;
      return value <= start;
    }

    if (filter.filterType === 'text') {
      const raw = entityValue(entity, filter.fieldCode).toLowerCase();
      const valueA = filter.valueA.toLowerCase();
      const list = filter.valueA
        .split(',')
        .map((item) => item.trim().toLowerCase())
        .filter(Boolean);

      if (filter.operator === 'равно') return raw === valueA;
      if (filter.operator === 'не равно') return raw !== valueA;
      if (filter.operator === 'содержит') return raw.includes(valueA);
      if (filter.operator === 'не содержит') return !raw.includes(valueA);
      return list.includes(raw);
    }

    const value = measureValue(entity, filter.fieldCode);
    const numberA = Number(filter.valueA);
    const numberB = Number(filter.valueB);
    if (Number.isNaN(numberA)) return true;

    if (filter.operator === '>') return value > numberA;
    if (filter.operator === '<') return value < numberA;
    if (filter.operator === '≥') return value >= numberA;
    if (filter.operator === '≤') return value <= numberA;
    if (Number.isNaN(numberB)) return true;
    return value >= Math.min(numberA, numberB) && value <= Math.max(numberA, numberB);
  }

  function applyFilters(list: SimilarityEntity[]): SimilarityEntity[] {
    const query = search.trim().toLowerCase();
    return list.filter((entity) => {
      if (!allowsEntityByLayer(entity)) return false;
      if (!inPeriod(entity.date)) return false;
      if (!checkEntityMode(entity)) return false;
      if (query && !entity.name.toLowerCase().includes(query) && !entity.id.toLowerCase().includes(query)) return false;
      if (filters.some((filter) => !checkFilter(entity, filter))) return false;
      return true;
    });
  }

  function checkEntityMode(entity: SimilarityEntity): boolean {
    if (entityFieldCode === 'campaign_id') return entity.type === 'campaign';
    return true;
  }

  function projectEntities(list: SimilarityEntity[]): Array<SimilarityEntity & { px: number; py: number; pz: number }> {
    if (list.length === 0) return [];
    const xVals = list.map((e) => measureValue(e, axisX));
    const yVals = list.map((e) => measureValue(e, axisY));
    const zVals = list.map((e) => measureValue(e, axisZ));

    const minX = Math.min(...xVals);
    const maxX = Math.max(...xVals);
    const minY = Math.min(...yVals);
    const maxY = Math.max(...yVals);
    const minZ = Math.min(...zVals);
    const maxZ = Math.max(...zVals);

    return list.map((entity) => ({
      ...entity,
      px: normalize(measureValue(entity, axisX), minX, maxX),
      py: normalize(measureValue(entity, axisY), minY, maxY),
      pz: normalize(measureValue(entity, axisZ), minZ, maxZ),
    }));
  }

  function normalize(value: number, min: number, max: number): number {
    return ((value - min) / Math.max(max - min, 0.0001) - 0.5) * 180;
  }

  function formatDate(value: string): string {
    const [year, month, day] = value.split('-');
    if (!year || !month || !day) return value;
    return `${day}.${month}.${year}`;
  }

  function formatTooltip(entity: SimilarityEntity): string[] {
    return [
      entity.name,
      `Тип: ${entity.type === 'campaign' ? 'РК' : 'Товар'}`,
      `ДРР ${entity.measures.drr}% · ROI ${entity.measures.roi} · CR2 ${entity.measures.cr2}%`,
      `Выручка ${entity.measures.revenue.toLocaleString('ru-RU')} ₽ · Расход ${entity.measures.spend.toLocaleString('ru-RU')} ₽`,
      `Дата: ${formatDate(entity.date)}`,
    ];
  }

  let renderer: THREE.WebGLRenderer;
  let scene: THREE.Scene;
  let camera: THREE.PerspectiveCamera;
  let controls: OrbitControls;
  let buyersMesh: THREE.InstancedMesh;
  let entitiesMesh: THREE.InstancedMesh;
  let vectors = new THREE.Group();
  let anim = 0;
  const raycaster = new THREE.Raycaster();
  const ndc = new THREE.Vector2();
  let renderPoints: BuyerPoint[] = [];
  let renderEntities: Array<SimilarityEntity & { px: number; py: number; pz: number }> = [];

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
    controls.dampingFactor = 0.08;

    scene.add(new THREE.AmbientLight('#ffffff', 0.95));
    const light = new THREE.DirectionalLight('#ffffff', 0.35);
    light.position.set(80, 120, 75);
    scene.add(light);
    scene.add(new THREE.GridHelper(260, 16, '#d6deea', '#eaf0f7'));
    scene.add(vectors);

    renderer.domElement.addEventListener('mousemove', onMove3d);
    renderer.domElement.addEventListener('click', onClick3d);

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
    vectors.clear();

    renderPoints = activeLayers.includes('sales_fact') ? (demoMode ? buyersDemo.points : buyersFull.points) : [];
    renderEntities = projected;

    if (renderPoints.length > 0) {
      buyersMesh = new THREE.InstancedMesh(
        new THREE.SphereGeometry(0.7, 6, 6),
        new THREE.MeshBasicMaterial({ color: '#60a5fa', transparent: true, opacity: 0.2 }),
        renderPoints.length
      );
      const d = new THREE.Object3D();
      renderPoints.forEach((point, index) => {
        d.position.set(point.x, point.y, point.z);
        d.updateMatrix();
        buyersMesh.setMatrixAt(index, d.matrix);
      });
      scene.add(buyersMesh);
    }

    if (renderEntities.length > 0) {
      entitiesMesh = new THREE.InstancedMesh(
        new THREE.OctahedronGeometry(1.05, 0),
        new THREE.MeshBasicMaterial({ vertexColors: true }),
        renderEntities.length
      );
      entitiesMesh.instanceColor = new THREE.InstancedBufferAttribute(new Float32Array(renderEntities.length * 3), 3);
      const d = new THREE.Object3D();

      renderEntities.forEach((entity, index) => {
        d.position.set(entity.px, entity.py, entity.pz);
        d.scale.set(entity.type === 'campaign' ? 1.7 : 1.25, entity.type === 'campaign' ? 1.7 : 1.25, entity.type === 'campaign' ? 1.7 : 1.25);
        d.rotation.set(0.2, 0.5, 0.1);
        d.updateMatrix();
        entitiesMesh.setMatrixAt(index, d.matrix);

        const color = entity.type === 'campaign' ? new THREE.Color('#1e40af') : new THREE.Color('#22c55e');
        entitiesMesh.setColorAt(index, color);

        const targetCluster = (demoMode ? buyersDemo.clusters : buyersFull.clusters).find((cluster) => cluster.name === entity.targetCluster);
        if (targetCluster && activeLayers.includes('ads')) {
          const start = new THREE.Vector3(entity.px, entity.py, entity.pz);
          const end = new THREE.Vector3(targetCluster.center.x, targetCluster.center.y, targetCluster.center.z);
          const dist = start.distanceTo(end);
          const tone = Math.min(1, dist / 130);
          const lineColor = new THREE.Color(`hsl(${(1 - tone) * 120}, 70%, 45%)`);
          vectors.add(new THREE.Line(new THREE.BufferGeometry().setFromPoints([start, end]), new THREE.LineBasicMaterial({ color: lineColor, transparent: true, opacity: 0.65 })));
        }
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
    if (hit && hit.instanceId !== undefined) {
      tooltip = {
        visible: true,
        x: event.clientX - rect.left + 12,
        y: event.clientY - rect.top + 12,
        lines: formatTooltip(renderEntities[hit.instanceId]),
      };
    } else {
      tooltip.visible = false;
    }
  }

  function onClick3d(event: MouseEvent): void {
    if (!entitiesMesh) return;
    const rect = renderer.domElement.getBoundingClientRect();
    ndc.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
    ndc.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;
    raycaster.setFromCamera(ndc, camera);
    const hit = raycaster.intersectObject(entitiesMesh, true)[0];
    if (hit && hit.instanceId !== undefined) {
      const entity = renderEntities[hit.instanceId];
      console.info('Выбрана точка', { id: entity.id, name: entity.name, тип: entity.type === 'campaign' ? 'РК' : 'Товар' });
    }
  }

  function animate(): void {
    controls?.update();
    if (buyersMesh) {
      const dist = camera.position.length();
      if (dist > 300) buyersMesh.count = Math.floor(renderPoints.length * 0.3);
      else if (dist > 230) buyersMesh.count = Math.floor(renderPoints.length * 0.6);
      else buyersMesh.count = renderPoints.length;
    }
    renderer?.render(scene, camera);
    anim = requestAnimationFrame(animate);
  }

  function onResize(): void {
    if (!renderer || !camera || !container3d) return;
    const width = container3d.clientWidth;
    const height = 560;
    renderer.setSize(width, height);
    camera.aspect = width / height;
    camera.updateProjectionMatrix();
  }

  function resetView(): void {
    camera.position.set(0, 80, 210);
    controls.target.set(0, 0, 0);
    controls.update();
  }

  function addLayer(): void {
    const next = DATASETS.find((dataset) => !activeLayers.includes(dataset.id));
    if (!next) return;
    activeLayers = [...activeLayers, next.id];
  }

  function removeLayer(layerId: DatasetId): void {
    if (activeLayers.length <= 1) return;
    activeLayers = activeLayers.filter((id) => id !== layerId);
  }

  function addFilter(filterType: FilterType): void {
    const id = `${Date.now()}-${Math.random()}`;
    if (filterType === 'text') {
      filters = [...filters, { id, filterType, fieldCode: textFilterFields[0]?.code ?? 'subject', operator: 'содержит', valueA: '', valueB: '' }];
      return;
    }
    if (filterType === 'number') {
      filters = [...filters, { id, filterType, fieldCode: numberFilterFields[0]?.code ?? 'revenue', operator: '>', valueA: '', valueB: '' }];
      return;
    }
    filters = [...filters, { id, filterType, fieldCode: dateFilterFields[0]?.code ?? 'date', operator: 'с', valueA: fromDate, valueB: '' }];
  }

  function updateFilter(id: string, patch: Partial<PanelFilter>): void {
    filters = filters.map((filter) => (filter.id === id ? { ...filter, ...patch } : filter));
  }

  function removeFilter(id: string): void {
    filters = filters.filter((filter) => filter.id !== id);
  }

  function onFilterFieldChange(id: string, event: Event): void {
    updateFilter(id, { fieldCode: (event.currentTarget as HTMLSelectElement).value });
  }

  function onFilterDateOperatorChange(id: string, event: Event): void {
    updateFilter(id, { operator: (event.currentTarget as HTMLSelectElement).value as DateOperator });
  }

  function onFilterTextOperatorChange(id: string, event: Event): void {
    updateFilter(id, { operator: (event.currentTarget as HTMLSelectElement).value as TextOperator });
  }

  function onFilterNumberOperatorChange(id: string, event: Event): void {
    updateFilter(id, { operator: (event.currentTarget as HTMLSelectElement).value as NumberOperator });
  }

  function onFilterValueAChange(id: string, event: Event): void {
    updateFilter(id, { valueA: (event.currentTarget as HTMLInputElement).value });
  }

  function onFilterValueBChange(id: string, event: Event): void {
    updateFilter(id, { valueB: (event.currentTarget as HTMLInputElement).value });
  }

  function readSettings(): void {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      settingsList = raw ? (JSON.parse(raw) as PanelSettings[]) : [];
    } catch {
      settingsList = [];
    }
  }

  function writeSettings(): void {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(settingsList));
  }

  function snapshot(name: string): PanelSettings {
    return {
      id: `${Date.now()}-${Math.random()}`,
      name,
      entityField: entityFieldCode,
      layers: [...activeLayers],
      axisX,
      axisY,
      axisZ,
      period,
      fromDate,
      toDate,
      filters: structuredClone(filters),
      demoMode,
    };
  }

  function saveCurrent(): void {
    const typedName = window.prompt('Название настройки', `Настройка ${settingsList.length + 1}`)?.trim();
    const name = typedName || `Настройка ${settingsList.length + 1}`;
    const item = snapshot(name);
    settingsList = [item, ...settingsList].slice(0, 20);
    selectedSettingId = item.id;
    writeSettings();
  }

  function applySettings(item: PanelSettings): void {
    entityFieldCode = item.entityField;
    activeLayers = item.layers;
    axisX = item.axisX;
    axisY = item.axisY;
    axisZ = item.axisZ;
    period = item.period;
    fromDate = item.fromDate;
    toDate = item.toDate;
    filters = structuredClone(item.filters);
    demoMode = item.demoMode;
  }

  function onSettingChange(): void {
    const found = settingsList.find((item) => item.id === selectedSettingId);
    if (found) applySettings(found);
  }

  function removeSetting(): void {
    if (!selectedSettingId) return;
    settingsList = settingsList.filter((item) => item.id !== selectedSettingId);
    selectedSettingId = '';
    writeSettings();
  }

  function resetPanel(): void {
    entityFieldCode = 'sku';
    activeLayers = ['sales_fact', 'ads'];
    axisX = 'revenue';
    axisY = 'spend';
    axisZ = 'roi';
    period = '30 дней';
    fromDate = '2026-02-01';
    toDate = '2026-02-28';
    filters = [];
    demoMode = true;
    search = '';
    resetView();
  }

  onMount(() => {
    if (!entities || entities.length === 0) {
      entities = generateSimilarityData(200, 80);
    }
    init3d();
    readSettings();
    window.addEventListener('resize', onResize);
    window.addEventListener('keydown', onHotKey);
  });

  function onHotKey(event: KeyboardEvent): void {
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'k') {
      event.preventDefault();
      searchInput?.focus();
    }
  }

  onDestroy(() => {
    cancelAnimationFrame(anim);
    window.removeEventListener('resize', onResize);
    window.removeEventListener('keydown', onHotKey);
    renderer?.dispose();
    controls?.dispose();
  });
</script>

<section class="graph-root panel">
  <div class="header">
    <h2>Пространство</h2>
  </div>

  <div class="content">
    <div class="graph-wrap">
      <div class="status">
        Точек: {projected.length} · Слои: {activeLayers.map((layer) => DATASETS.find((item) => item.id === layer)?.name).join(', ')} ·
        Оси: {fieldLabel(axisX)} / {fieldLabel(axisY)} / {fieldLabel(axisZ)}
      </div>
      <div class="stage">
        <div bind:this={container3d} class="scene" />
        {#if tooltip.visible}
          <div class="tooltip" style={`left:${tooltip.x}px;top:${tooltip.y}px`}>
            {#each tooltip.lines as line}<div>{line}</div>{/each}
          </div>
        {/if}
      </div>
    </div>

    <aside class="control-panel">
      <section class="section">
        <h4>Настройки</h4>
        <small>Позволяет сохранить текущую конфигурацию панели.</small>
        <select bind:value={selectedSettingId} on:change={onSettingChange}>
          <option value="">Выберите сохранённую настройку</option>
          {#each settingsList as setting}
            <option value={setting.id}>{setting.name}</option>
          {/each}
        </select>
        <div class="row-buttons">
          <button on:click={saveCurrent}>Сохранить текущую</button>
          <button on:click={removeSetting} disabled={!selectedSettingId}>Удалить</button>
          <button on:click={resetPanel}>Сбросить</button>
        </div>
        <label class="demo-toggle"><input type="checkbox" bind:checked={demoMode} /> Демо-режим</label>
      </section>

      <section class="section">
        <h4>Точки на графе</h4>
        <small>Определяет, что является отдельной точкой в пространстве.</small>
        <select bind:value={entityFieldCode}>
          {#each entityFields as field}
            <option value={field.code}>{field.name}</option>
          {/each}
        </select>
      </section>

      <section class="section">
        <h4>Набор данных</h4>
        <small>Определяет, из каких источников берутся метрики.</small>
        <div class="layer-list">
          {#each activeLayers as layer}
            <div class="layer-item">
              <span>{DATASETS.find((dataset) => dataset.id === layer)?.name}</span>
              <button on:click={() => removeLayer(layer)} disabled={activeLayers.length <= 1}>Удалить</button>
            </div>
          {/each}
        </div>
        <button on:click={addLayer} disabled={activeLayers.length === DATASETS.length}>+ Добавить слой</button>
      </section>

      <section class="section">
        <h4>Координаты</h4>
        <small>Определяет, какие числовые показатели формируют 3D-пространство.</small>
        <label>Ось X
          <select bind:value={axisX}>{#each axisFields as field}<option value={field.code}>{field.name}</option>{/each}</select>
        </label>
        <label>Ось Y
          <select bind:value={axisY}>{#each axisFields as field}<option value={field.code}>{field.name}</option>{/each}</select>
        </label>
        <label>Ось Z
          <select bind:value={axisZ}>{#each axisFields as field}<option value={field.code}>{field.name}</option>{/each}</select>
        </label>
        <small>Оси принимают только числовые метрики.</small>
      </section>

      <section class="section">
        <h4>Период и фильтры</h4>
        <small>Ограничивает данные, которые участвуют в расчётах.</small>

        <label>Период
          <select bind:value={period}>
            <option>7 дней</option>
            <option>14 дней</option>
            <option>30 дней</option>
            <option>Даты</option>
          </select>
        </label>

        {#if period === 'Даты'}
          <div class="dates">
            <input type="date" bind:value={fromDate} />
            <input type="date" bind:value={toDate} />
          </div>
        {/if}

        <input bind:this={searchInput} bind:value={search} placeholder="Поиск SKU / РК" />

        <div class="row-buttons">
          <button on:click={() => addFilter('date')}>+ Добавить фильтр даты</button>
          <button on:click={() => addFilter('text')}>+ Добавить текстовый фильтр</button>
          <button on:click={() => addFilter('number')}>+ Добавить числовой фильтр</button>
        </div>

        {#each filters as filter}
          <div class="filter-item">
            <div class="filter-head">Фильтр: {filter.filterType === 'date' ? 'Дата' : filter.filterType === 'text' ? 'Текст' : 'Число'}</div>
            {#if filter.filterType === 'date'}
              <select bind:value={filter.fieldCode} on:change={(event) => onFilterFieldChange(filter.id, event)}>
                {#each dateFilterFields as field}<option value={field.code}>{field.name}</option>{/each}
              </select>
              <select bind:value={filter.operator} on:change={(event) => onFilterDateOperatorChange(filter.id, event)}>
                <option>с</option>
                <option>по</option>
              </select>
              <input type="date" bind:value={filter.valueA} on:input={(event) => onFilterValueAChange(filter.id, event)} />
            {:else if filter.filterType === 'text'}
              <select bind:value={filter.fieldCode} on:change={(event) => onFilterFieldChange(filter.id, event)}>
                {#each textFilterFields as field}<option value={field.code}>{field.name}</option>{/each}
              </select>
              <select bind:value={filter.operator} on:change={(event) => onFilterTextOperatorChange(filter.id, event)}>
                <option>равно</option>
                <option>не равно</option>
                <option>содержит</option>
                <option>не содержит</option>
                <option>в списке</option>
              </select>
              <input type="text" bind:value={filter.valueA} placeholder="Значение" on:input={(event) => onFilterValueAChange(filter.id, event)} />
            {:else}
              <select bind:value={filter.fieldCode} on:change={(event) => onFilterFieldChange(filter.id, event)}>
                {#each numberFilterFields as field}<option value={field.code}>{field.name}</option>{/each}
              </select>
              <select bind:value={filter.operator} on:change={(event) => onFilterNumberOperatorChange(filter.id, event)}>
                <option>&gt;</option>
                <option>&lt;</option>
                <option>≥</option>
                <option>≤</option>
                <option>между</option>
              </select>
              <input type="number" bind:value={filter.valueA} placeholder="Значение" on:input={(event) => onFilterValueAChange(filter.id, event)} />
              {#if filter.operator === 'между'}
                <input type="number" bind:value={filter.valueB} placeholder="И" on:input={(event) => onFilterValueBChange(filter.id, event)} />
              {/if}
            {/if}
            <button on:click={() => removeFilter(filter.id)}>Удалить фильтр</button>
          </div>
        {/each}

        <button on:click={resetView}>Сбросить вид</button>
      </section>
    </aside>
  </div>
</section>

<style>
  .graph-root { display:flex; flex-direction:column; gap:10px; }
  .header h2 { margin:0; font-size:24px; }
  .content { display:grid; grid-template-columns:minmax(0,1fr) 360px; gap:12px; align-items:stretch; }
  .graph-wrap { min-width:0; }
  .status { font-size:12px; color:#64748b; margin-bottom:6px; }
  .stage { position:relative; }
  .scene { width:100%; height:560px; border:1px solid #e2e8f0; border-radius:16px; background:#f8fbff; }
  .control-panel { height:560px; border:1px solid #e2e8f0; border-radius:14px; padding:10px; background:#fbfdff; display:flex; flex-direction:column; gap:10px; overflow:auto; }
  .section { border:1px solid #e7edf6; border-radius:10px; padding:8px; display:flex; flex-direction:column; gap:6px; }
  .section h4 { margin:0; font-size:13px; color:#334155; }
  .section label { display:flex; flex-direction:column; gap:4px; font-size:12px; color:#334155; }
  .section select, .section input, .section button { border:1px solid #dbe4f0; border-radius:10px; padding:6px 8px; background:#fff; font-size:12px; }
  .section button { cursor:pointer; }
  .section button:disabled { opacity:.5; cursor:not-allowed; }
  .dates { display:flex; gap:6px; }
  .tooltip { position:absolute; pointer-events:none; background:rgba(15,23,42,.94); color:#f8fafc; font-size:12px; padding:10px; border-radius:10px; max-width:360px; }
  .row-buttons { display:flex; flex-wrap:wrap; gap:6px; }
  .layer-list, .filter-item { display:flex; flex-direction:column; gap:6px; }
  .layer-item { display:flex; justify-content:space-between; align-items:center; gap:8px; font-size:12px; }
  .filter-head { font-size:11px; color:#64748b; }
  .demo-toggle { display:flex; flex-direction:row; align-items:center; gap:6px; }
  small { color:#64748b; font-size:11px; line-height:1.3; }
</style>
