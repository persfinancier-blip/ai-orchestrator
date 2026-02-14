<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import * as THREE from 'three';
  import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
  import { get } from 'svelte/store';
  import { showcaseStore, fieldName, type DatasetId, type ShowcaseField, type ShowcaseRow } from '../data/showcaseStore';

  type FilterType = 'date' | 'text' | 'number';
  type PeriodMode = '7 дней' | '14 дней' | '30 дней' | 'Даты';
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

  type SpacePoint = {
    id: string;
    label: string;
    sourceField: string;
    metrics: Record<string, number>;
    x: number;
    y: number;
    z: number;
  };

  const STORAGE_KEY = 'desk-space-settings-v2';

  let activeLayers: DatasetId[] = ['sales_fact', 'ads'];
  let selectedEntityFields: string[] = ['sku', 'campaign_id'];
  let axisX = '';
  let axisY = '';
  let axisZ = '';
  let period: PeriodMode = '30 дней';
  let fromDate = '';
  let toDate = '';
  let search = '';
  let filters: PanelFilter[] = [];

  let panelSettingsList: Array<{ id: string; name: string; value: unknown }> = [];
  let selectedSettingId = '';

  let showcase = get(showcaseStore);
  const unsub = showcaseStore.subscribe((value) => {
    showcase = value;
    ensureDefaults();
  });

  let container3d: HTMLDivElement;
  let searchInput: HTMLInputElement;

  let tooltip = { visible: false, x: 0, y: 0, lines: [] as string[] };
  let points: SpacePoint[] = [];
  let debugCalculatedCount = 0;
  let debugRenderedCount = 0;
  let debugBBox = '—';
  let debugLastError = '—';

  type BBox = { minX: number; maxX: number; minY: number; maxY: number; minZ: number; maxZ: number };

  let renderer: THREE.WebGLRenderer;
  let scene: THREE.Scene;
  let camera: THREE.PerspectiveCamera;
  let controls: OrbitControls;
  let circleMesh: THREE.InstancedMesh | undefined;
  let diamondMesh: THREE.InstancedMesh | undefined;
  let axesHelper: THREE.AxesHelper | undefined;
  let circlePoints: SpacePoint[] = [];
  let diamondPoints: SpacePoint[] = [];
  let anim = 0;
  const raycaster = new THREE.Raycaster();
  const ndc = new THREE.Vector2();

  function ensureDefaults(): void {
    const numberFields = availableFields('number', 'axis');
    if (!axisX && numberFields[0]) axisX = numberFields[0].code;
    if (!axisY && numberFields[1]) axisY = numberFields[1].code;
    if (!axisZ && numberFields[2]) axisZ = numberFields[2].code;
    if (!selectedEntityFields.length) selectedEntityFields = ['sku'];
  }

  function availableFields(kind: ShowcaseField['kind'], role: 'entity' | 'axis' | 'filter'): ShowcaseField[] {
    return showcase.fields.filter(
      (field) => field.kind === kind && field.roles.includes(role) && field.datasetIds.some((id) => activeLayers.includes(id))
    );
  }

  $: textFields = availableFields('text', 'entity');
  $: axisFields = availableFields('number', 'axis');
  $: dateFields = availableFields('date', 'filter');
  $: textFilterFields = availableFields('text', 'filter');
  $: numberFilterFields = availableFields('number', 'filter');

  $: filteredRows = applyRowsFilter(showcase.rows);
  $: points = buildPoints(filteredRows);
  $: rebuildScene();

  function hashToJitter(input: string): number {
    let hash = 0;
    for (let i = 0; i < input.length; i += 1) hash = (hash * 31 + input.charCodeAt(i)) >>> 0;
    return ((hash % 1000) / 1000 - 0.5) * 10;
  }

  function applyRowsFilter(rows: ShowcaseRow[]): ShowcaseRow[] {
    return rows.filter((row) => {
      if (!inPeriod(row.date)) return false;
      if (filters.some((filter) => !matchFilter(row, filter))) return false;
      if (!search.trim()) return true;
      const q = search.trim().toLowerCase();
      return selectedEntityFields.some((field) => String((row as Record<string, unknown>)[field] ?? '').toLowerCase().includes(q));
    });
  }

  function inPeriod(date: string): boolean {
    if (period === 'Даты') {
      if (fromDate && date < fromDate) return false;
      if (toDate && date > toDate) return false;
      return true;
    }
    const days = period === '7 дней' ? 7 : period === '14 дней' ? 14 : 30;
    const border = new Date();
    border.setDate(border.getDate() - (days - 1));
    return new Date(date) >= border;
  }

  function matchFilter(row: ShowcaseRow, filter: PanelFilter): boolean {
    const value = (row as Record<string, unknown>)[filter.fieldCode];
    if (filter.filterType === 'date') {
      const v = String(value ?? '');
      if (!filter.valueA) return true;
      return filter.operator === 'с' ? v >= filter.valueA : v <= filter.valueA;
    }
    if (filter.filterType === 'text') {
      const v = String(value ?? '').toLowerCase();
      const q = filter.valueA.toLowerCase();
      if (filter.operator === 'равно') return v === q;
      if (filter.operator === 'не равно') return v !== q;
      if (filter.operator === 'содержит') return v.includes(q);
      if (filter.operator === 'не содержит') return !v.includes(q);
      return q.split(',').map((i) => i.trim()).filter(Boolean).includes(v);
    }
    const n = Number(value ?? 0);
    const a = Number(filter.valueA);
    const b = Number(filter.valueB);
    if (Number.isNaN(a)) return true;
    if (filter.operator === '>') return n > a;
    if (filter.operator === '<') return n < a;
    if (filter.operator === '≥') return n >= a;
    if (filter.operator === '≤') return n <= a;
    if (Number.isNaN(b)) return true;
    return n >= Math.min(a, b) && n <= Math.max(a, b);
  }

  function buildPoints(rows: ShowcaseRow[]): SpacePoint[] {
    const result: SpacePoint[] = [];
    const numberCodes = showcase.fields.filter((f) => f.kind === 'number').map((f) => f.code);

    selectedEntityFields.forEach((entityField) => {
      const groups = new Map<string, ShowcaseRow[]>();
      rows.forEach((row) => {
        const key = String((row as Record<string, unknown>)[entityField] ?? '').trim();
        if (!key) return;
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key)?.push(row);
      });

      groups.forEach((groupRows, key) => {
        const metrics: Record<string, number> = {};
        numberCodes.forEach((code) => {
          metrics[code] = groupRows.reduce((sum, row) => sum + Number((row as Record<string, unknown>)[code] ?? 0), 0) / Math.max(groupRows.length, 1);
        });
        metrics.revenue = groupRows.reduce((sum, row) => sum + row.revenue, 0);
        metrics.spend = groupRows.reduce((sum, row) => sum + row.spend, 0);
        metrics.orders = groupRows.reduce((sum, row) => sum + row.orders, 0);
        metrics.drr = metrics.revenue > 0 ? (metrics.spend / metrics.revenue) * 100 : 0;
        metrics.roi = metrics.spend > 0 ? metrics.revenue / metrics.spend : 0;

        result.push({
          id: `${entityField}:${key}`,
          label: key,
          sourceField: entityField,
          metrics,
          x: 0,
          y: 0,
          z: 0,
        });
      });
    });

    const coords = projectPoints(result);
    const uniqueTotal = selectedEntityFields.reduce((sum, field) => sum + new Set(rows.map((r) => String((r as Record<string, unknown>)[field] ?? '')).filter(Boolean)).size, 0);
    console.log('[Пространство] пересчет', { поля: selectedEntityFields, уникальных: uniqueTotal, первыеТочки: coords.slice(0, 3).map((p) => ({ id: p.id, x: p.x, y: p.y, z: p.z })) });
    return coords;
  }

  function projectPoints(list: SpacePoint[]): SpacePoint[] {
    if (!list.length) return [];
    const useX = axisX && axisFields.some((f) => f.code === axisX);
    const useY = axisY && axisFields.some((f) => f.code === axisY);
    const useZ = axisZ && axisFields.some((f) => f.code === axisZ);

    const xValues = useX ? list.map((p) => p.metrics[axisX] ?? 0) : [0, 1];
    const yValues = useY ? list.map((p) => p.metrics[axisY] ?? 0) : [0, 1];
    const zValues = useZ ? list.map((p) => p.metrics[axisZ] ?? 0) : [0, 1];

    const minX = Math.min(...xValues);
    const maxX = Math.max(...xValues);
    const minY = Math.min(...yValues);
    const maxY = Math.max(...yValues);
    const minZ = Math.min(...zValues);
    const maxZ = Math.max(...zValues);

    return list.map((point, index) => {
      const jitter = hashToJitter(point.id);
      const x = useX ? normalize(point.metrics[axisX] ?? 0, minX, maxX) : jitter;
      const y = useY ? normalize(point.metrics[axisY] ?? 0, minY, maxY) : -jitter;
      const z = useZ ? normalize(point.metrics[axisZ] ?? 0, minZ, maxZ) : (index % 9) - 4 + jitter * 0.2;
      return { ...point, x, y, z };
    });
  }

  function normalize(value: number, min: number, max: number): number {
    return ((value - min) / Math.max(max - min, 0.0001) - 0.5) * 180;
  }


  function sanitizeCoord(value: number, id: string, axis: 'x' | 'y' | 'z'): number {
    if (!Number.isFinite(value)) return hashToJitter(`${id}:${axis}`) * 0.05;
    return value + hashToJitter(`${id}:${axis}`) * 0.05;
  }

  function fallbackPoints(count = 24): SpacePoint[] {
    return Array.from({ length: count }).map((_, i) => {
      const id = `fallback-${i + 1}`;
      return {
        id,
        label: `Тестовая точка ${i + 1}`,
        sourceField: i % 2 === 0 ? 'sku' : 'campaign_id',
        metrics: { revenue: 0, spend: 0, drr: 0, roi: 0, orders: 0 },
        x: hashToJitter(`${id}:x`) * 1.2,
        y: hashToJitter(`${id}:y`) * 1.2,
        z: hashToJitter(`${id}:z`) * 1.2,
      };
    });
  }

  function buildBBox(list: SpacePoint[]): BBox {
    const xs = list.map((p) => p.x);
    const ys = list.map((p) => p.y);
    const zs = list.map((p) => p.z);
    return {
      minX: Math.min(...xs),
      maxX: Math.max(...xs),
      minY: Math.min(...ys),
      maxY: Math.max(...ys),
      minZ: Math.min(...zs),
      maxZ: Math.max(...zs),
    };
  }

  function fitCameraToPoints(list: SpacePoint[]): void {
    if (!camera || !controls || !list.length) return;
    const bbox = buildBBox(list);
    debugBBox = `x ${bbox.minX.toFixed(2)}..${bbox.maxX.toFixed(2)} | y ${bbox.minY.toFixed(2)}..${bbox.maxY.toFixed(2)} | z ${bbox.minZ.toFixed(2)}..${bbox.maxZ.toFixed(2)}`;

    const center = new THREE.Vector3(
      (bbox.minX + bbox.maxX) / 2,
      (bbox.minY + bbox.maxY) / 2,
      (bbox.minZ + bbox.maxZ) / 2
    );
    const spanX = Math.max(0.001, bbox.maxX - bbox.minX);
    const spanY = Math.max(0.001, bbox.maxY - bbox.minY);
    const spanZ = Math.max(0.001, bbox.maxZ - bbox.minZ);
    const maxSpan = Math.max(spanX, spanY, spanZ);

    if (maxSpan < 0.5) {
      camera.position.set(center.x + 1.5, center.y + 1.5, center.z + 5);
      controls.target.copy(center);
      controls.update();
      return;
    }

    const distance = Math.max(8, maxSpan * 1.55);
    camera.position.set(center.x + distance * 0.55, center.y + distance * 0.45, center.z + distance);
    controls.target.copy(center);
    controls.update();
  }

  function ensureRenderablePoints(input: SpacePoint[]): SpacePoint[] {
    const sanitized = input.map((point) => ({
      ...point,
      x: sanitizeCoord(point.x, point.id, 'x'),
      y: sanitizeCoord(point.y, point.id, 'y'),
      z: sanitizeCoord(point.z, point.id, 'z'),
    }));
    const hasFinite = sanitized.some((p) => Number.isFinite(p.x) && Number.isFinite(p.y) && Number.isFinite(p.z));
    if (!sanitized.length || !hasFinite) {
      debugLastError = 'Включен гарантированный режим: точки подставлены автоматически';
      return fallbackPoints(24);
    }
    debugLastError = '—';
    return sanitized;
  }

  function init3d(): void {
    const width = container3d.clientWidth;
    const height = 560;
    scene = new THREE.Scene();
    scene.background = new THREE.Color('#f8fbff');
    camera = new THREE.PerspectiveCamera(52, width / height, 0.1, 1500);
    camera.position.set(0, 80, 220);
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
    axesHelper = new THREE.AxesHelper(60);
    scene.add(axesHelper);

    renderer.domElement.addEventListener('mousemove', onMove3d);
    animate();
  }

  function clearMesh(mesh?: THREE.InstancedMesh): void {
    if (!mesh) return;
    scene.remove(mesh);
    mesh.geometry.dispose();
    (mesh.material as THREE.Material).dispose();
  }

  function rebuildScene(): void {
    if (!scene) return;
    clearMesh(circleMesh);
    clearMesh(diamondMesh);

    debugCalculatedCount = points.length;
    const renderablePoints = ensureRenderablePoints(points);
    debugRenderedCount = renderablePoints.length;

    circlePoints = renderablePoints.filter((p) => p.sourceField !== 'campaign_id');
    diamondPoints = renderablePoints.filter((p) => p.sourceField === 'campaign_id');

    if (circlePoints.length) {
      circleMesh = new THREE.InstancedMesh(
        new THREE.SphereGeometry(1.45, 10, 10),
        new THREE.MeshBasicMaterial({ color: '#22c55e' }),
        circlePoints.length
      );
      const o = new THREE.Object3D();
      circlePoints.forEach((point, idx) => {
        o.position.set(point.x, point.y, point.z);
        o.updateMatrix();
        circleMesh?.setMatrixAt(idx, o.matrix);
      });
      scene.add(circleMesh);
    }

    if (diamondPoints.length) {
      diamondMesh = new THREE.InstancedMesh(
        new THREE.OctahedronGeometry(1.9, 0),
        new THREE.MeshBasicMaterial({ color: '#1d4ed8' }),
        diamondPoints.length
      );
      const o = new THREE.Object3D();
      diamondPoints.forEach((point, idx) => {
        o.position.set(point.x, point.y, point.z);
        o.rotation.set(0.2, 0.5, 0.1);
        o.updateMatrix();
        diamondMesh?.setMatrixAt(idx, o.matrix);
      });
      scene.add(diamondMesh);
    }

    fitCameraToPoints(renderablePoints);
  }

  function formatDate(value: string): string {
    const [y, m, d] = value.split('-');
    return y && m && d ? `${d}.${m}.${y}` : value;
  }

  function onMove3d(event: MouseEvent): void {
    if (!renderer) return;
    const rect = renderer.domElement.getBoundingClientRect();
    ndc.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
    ndc.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;
    raycaster.setFromCamera(ndc, camera);

    const hitCircle = circleMesh ? raycaster.intersectObject(circleMesh, true)[0] : undefined;
    const hitDiamond = diamondMesh ? raycaster.intersectObject(diamondMesh, true)[0] : undefined;

    const hit = hitCircle || hitDiamond;
    if (!hit || hit.instanceId === undefined) {
      tooltip.visible = false;
      return;
    }

    const point = hitCircle ? circlePoints[hit.instanceId] : diamondPoints[hit.instanceId];
    if (!point) return;

    tooltip = {
      visible: true,
      x: event.clientX - rect.left + 12,
      y: event.clientY - rect.top + 12,
      lines: [
        `${point.label}`,
        `Поле: ${fieldName(point.sourceField)}`,
        `Выручка: ${Math.round(point.metrics.revenue).toLocaleString('ru-RU')} ₽`,
        `Расход: ${Math.round(point.metrics.spend).toLocaleString('ru-RU')} ₽`,
        `ДРР: ${point.metrics.drr.toFixed(2)}% · ROI: ${point.metrics.roi.toFixed(2)}`,
        `Период: ${formatDate(fromDate || showcase.rows[0]?.date || '')} — ${formatDate(toDate || showcase.rows[showcase.rows.length - 1]?.date || '')}`,
      ],
    };
  }

  function animate(): void {
    controls?.update();
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
    camera.position.set(0, 80, 220);
    controls.target.set(0, 0, 0);
    controls.update();
  }

  function addLayer(): void {
    const next = showcase.datasets.find((dataset) => !activeLayers.includes(dataset.id));
    if (next) activeLayers = [...activeLayers, next.id];
  }

  function removeLayer(layerId: DatasetId): void {
    if (activeLayers.length <= 1) return;
    activeLayers = activeLayers.filter((id) => id !== layerId);
  }

  function onAddEntityFieldChange(event: Event): void {
    const code = (event.currentTarget as HTMLSelectElement).value;
    addEntityField(code);
    (event.currentTarget as HTMLSelectElement).value = '';
  }

  function addEntityField(code: string): void {
    if (!code || selectedEntityFields.includes(code)) return;
    selectedEntityFields = [...selectedEntityFields, code];
  }

  function removeEntityField(code: string): void {
    selectedEntityFields = selectedEntityFields.filter((item) => item !== code);
  }

  function addFilter(type: FilterType): void {
    const id = `${Date.now()}-${Math.random()}`;
    if (type === 'date') {
      filters = [...filters, { id, filterType: type, fieldCode: dateFields[0]?.code ?? 'date', operator: 'с', valueA: '', valueB: '' }];
      return;
    }
    if (type === 'number') {
      filters = [...filters, { id, filterType: type, fieldCode: numberFilterFields[0]?.code ?? 'revenue', operator: '>', valueA: '', valueB: '' }];
      return;
    }
    filters = [...filters, { id, filterType: type, fieldCode: textFilterFields[0]?.code ?? 'sku', operator: 'содержит', valueA: '', valueB: '' }];
  }

  function updateFilter(id: string, patch: Partial<PanelFilter>): void {
    filters = filters.map((f) => (f.id === id ? { ...f, ...patch } : f));
  }

  function removeFilter(id: string): void {
    filters = filters.filter((f) => f.id !== id);
  }

  function onFilterTypeChange(id: string, event: Event): void {
    const t = (event.currentTarget as HTMLSelectElement).value as FilterType;
    if (t === 'date') updateFilter(id, { filterType: t, fieldCode: dateFields[0]?.code ?? 'date', operator: 'с', valueA: '', valueB: '' });
    if (t === 'text') updateFilter(id, { filterType: t, fieldCode: textFilterFields[0]?.code ?? 'sku', operator: 'содержит', valueA: '', valueB: '' });
    if (t === 'number') updateFilter(id, { filterType: t, fieldCode: numberFilterFields[0]?.code ?? 'revenue', operator: '>', valueA: '', valueB: '' });
  }

  function saveCurrent(): void {
    const name = window.prompt('Название настройки', `Настройка ${panelSettingsList.length + 1}`)?.trim();
    if (!name) return;
    const item = {
      id: `${Date.now()}-${Math.random()}`,
      name,
      value: {
        activeLayers,
        selectedEntityFields,
        axisX,
        axisY,
        axisZ,
        period,
        fromDate,
        toDate,
        filters,
      },
    };
    panelSettingsList = [item, ...panelSettingsList].slice(0, 20);
    selectedSettingId = item.id;
    localStorage.setItem(STORAGE_KEY, JSON.stringify(panelSettingsList));
  }

  function loadSettings(): void {
    const found = panelSettingsList.find((item) => item.id === selectedSettingId);
    if (!found) return;
    const value = found.value as Record<string, unknown>;
    activeLayers = (value.activeLayers as DatasetId[]) ?? ['sales_fact', 'ads'];
    selectedEntityFields = (value.selectedEntityFields as string[]) ?? ['sku'];
    axisX = String(value.axisX ?? '');
    axisY = String(value.axisY ?? '');
    axisZ = String(value.axisZ ?? '');
    period = (value.period as PeriodMode) ?? '30 дней';
    fromDate = String(value.fromDate ?? '');
    toDate = String(value.toDate ?? '');
    filters = (value.filters as PanelFilter[]) ?? [];
  }

  function removeSetting(): void {
    if (!selectedSettingId) return;
    panelSettingsList = panelSettingsList.filter((item) => item.id !== selectedSettingId);
    selectedSettingId = '';
    localStorage.setItem(STORAGE_KEY, JSON.stringify(panelSettingsList));
  }

  function resetPanel(): void {
    activeLayers = ['sales_fact', 'ads'];
    selectedEntityFields = ['sku', 'campaign_id'];
    axisX = '';
    axisY = '';
    axisZ = '';
    period = '30 дней';
    fromDate = '';
    toDate = '';
    filters = [];
    search = '';
    resetView();
  }

  function onHotKey(event: KeyboardEvent): void {
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'k') {
      event.preventDefault();
      searchInput?.focus();
    }
  }

  onMount(() => {
    init3d();
    ensureDefaults();
    rebuildScene();
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      panelSettingsList = raw ? JSON.parse(raw) : [];
    } catch {
      panelSettingsList = [];
    }
    window.addEventListener('resize', onResize);
    window.addEventListener('keydown', onHotKey);
  });

  onDestroy(() => {
    unsub();
    cancelAnimationFrame(anim);
    window.removeEventListener('resize', onResize);
    window.removeEventListener('keydown', onHotKey);
    renderer?.dispose();
    controls?.dispose();
  });
</script>

<section class="graph-root panel">
  <div class="title-row">
    <h2>Пространство</h2>
    <div class="status">Точек: {points.length} · Слои: {activeLayers.map((id) => showcase.datasets.find((d) => d.id === id)?.name).join(', ')} · Оси: {fieldName(axisX || '—')} / {fieldName(axisY || '—')} / {fieldName(axisZ || '—')}</div>
  </div>

  <div class="content">
    <div class="graph-wrap">
      <div class="stage">
        <div bind:this={container3d} class="scene" />
        <div class="debug">
          <div>Точек (расчёт): {debugCalculatedCount}</div>
          <div>Точек (отрисовка): {debugRenderedCount}</div>
          <div>X/Y/Z: {fieldName(axisX || '—')} / {fieldName(axisY || '—')} / {fieldName(axisZ || '—')}</div>
          <div>bbox: {debugBBox}</div>
          <div>последняя ошибка: {debugLastError}</div>
        </div>
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
        <small>Сохранение и загрузка конфигурации панели.</small>
        <select bind:value={selectedSettingId} on:change={loadSettings}>
          <option value="">Выберите сохранённую настройку</option>
          {#each panelSettingsList as setting}
            <option value={setting.id}>{setting.name}</option>
          {/each}
        </select>
        <div class="row-buttons">
          <button on:click={saveCurrent}>Сохранить текущую</button>
          <button on:click={removeSetting} disabled={!selectedSettingId}>Удалить</button>
          <button on:click={resetPanel}>Сбросить</button>
        </div>
      </section>

      <section class="section">
        <h4>Точки на графе</h4>
        <small>Добавьте одно или несколько текстовых полей. Каждое уникальное значение станет точкой.</small>
        <div class="chips">
          {#each selectedEntityFields as code}
            <span class="chip">{fieldName(code)} <button on:click={() => removeEntityField(code)}>×</button></span>
          {/each}
        </div>
        <label>+ Добавить поле
          <select on:change={onAddEntityFieldChange}>
            <option value="">Выберите поле</option>
            {#each textFields as field}
              <option value={field.code}>{field.name}</option>
            {/each}
          </select>
        </label>
      </section>

      <section class="section">
        <h4>Набор данных</h4>
        <small>Из каких источников берутся метрики.</small>
        {#each activeLayers as layer}
          <div class="layer-item">
            <span>{showcase.datasets.find((dataset) => dataset.id === layer)?.name}</span>
            <button on:click={() => removeLayer(layer)} disabled={activeLayers.length <= 1}>Удалить</button>
          </div>
        {/each}
        <button on:click={addLayer} disabled={activeLayers.length === showcase.datasets.length}>+ Добавить слой</button>
      </section>

      <section class="section">
        <h4>Координаты</h4>
        <small>Какие числовые метрики формируют оси X/Y/Z.</small>
        <label>Ось X
          <select bind:value={axisX}><option value="">Не выбрано</option>{#each axisFields as field}<option value={field.code}>{field.name}</option>{/each}</select>
        </label>
        <label>Ось Y
          <select bind:value={axisY}><option value="">Не выбрано</option>{#each axisFields as field}<option value={field.code}>{field.name}</option>{/each}</select>
        </label>
        <label>Ось Z
          <select bind:value={axisZ}><option value="">Не выбрано</option>{#each axisFields as field}<option value={field.code}>{field.name}</option>{/each}</select>
        </label>
      </section>

      <section class="section">
        <h4>Период и фильтры</h4>
        <small>Ограничивает данные для расчёта.</small>
        <label>Период
          <select bind:value={period}>
            <option>7 дней</option><option>14 дней</option><option>30 дней</option><option>Даты</option>
          </select>
        </label>
        {#if period === 'Даты'}
          <div class="dates"><input type="date" bind:value={fromDate} /><input type="date" bind:value={toDate} /></div>
        {/if}
        <input bind:this={searchInput} bind:value={search} placeholder="Поиск по выбранным полям" />
        <div class="row-buttons">
          <button on:click={() => addFilter('date')}>+ Дата</button>
          <button on:click={() => addFilter('text')}>+ Текст</button>
          <button on:click={() => addFilter('number')}>+ Число</button>
        </div>
        {#each filters as filter}
          <div class="filter-item">
            <select bind:value={filter.filterType} on:change={(event) => onFilterTypeChange(filter.id, event)}>
              <option value="date">Дата</option>
              <option value="text">Текст</option>
              <option value="number">Число</option>
            </select>
            {#if filter.filterType === 'date'}
              <select bind:value={filter.fieldCode}>{#each dateFields as field}<option value={field.code}>{field.name}</option>{/each}</select>
              <select bind:value={filter.operator}><option>с</option><option>по</option></select>
              <input type="date" bind:value={filter.valueA} />
            {:else if filter.filterType === 'text'}
              <select bind:value={filter.fieldCode}>{#each textFilterFields as field}<option value={field.code}>{field.name}</option>{/each}</select>
              <select bind:value={filter.operator}><option>равно</option><option>не равно</option><option>содержит</option><option>не содержит</option><option>в списке</option></select>
              <input type="text" bind:value={filter.valueA} placeholder="Значение" />
            {:else}
              <select bind:value={filter.fieldCode}>{#each numberFilterFields as field}<option value={field.code}>{field.name}</option>{/each}</select>
              <select bind:value={filter.operator}><option>&gt;</option><option>&lt;</option><option>≥</option><option>≤</option><option>между</option></select>
              <input type="number" bind:value={filter.valueA} placeholder="От" />
              {#if filter.operator === 'между'}<input type="number" bind:value={filter.valueB} placeholder="До" />{/if}
            {/if}
            <button on:click={() => removeFilter(filter.id)}>Удалить</button>
          </div>
        {/each}
        <button on:click={resetView}>Сбросить вид</button>
      </section>
    </aside>
  </div>
</section>

<style>
  .graph-root { display:flex; flex-direction:column; gap:8px; }
  .title-row { display:flex; justify-content:space-between; align-items:center; gap:12px; }
  .title-row h2 { margin:0; font-size:24px; }
  .status { font-size:12px; color:#64748b; text-align:right; }
  .content { display:grid; grid-template-columns:minmax(0,1fr) 360px; gap:12px; align-items:stretch; }
  .graph-wrap { min-width:0; }
  .stage { position:relative; min-height:560px; }
  .scene { width:100%; height:100%; min-height:560px; border:1px solid #e2e8f0; border-radius:16px; background:#f8fbff; }
  .debug { position:absolute; left:10px; top:10px; z-index:3; background:rgba(15,23,42,.82); color:#f8fafc; font-size:11px; border-radius:10px; padding:8px 10px; line-height:1.4; pointer-events:none; max-width:400px; }
  .control-panel { height:560px; border:1px solid #e2e8f0; border-radius:14px; padding:10px; background:#fbfdff; display:flex; flex-direction:column; gap:10px; overflow:auto; }
  .section { border:1px solid #e7edf6; border-radius:10px; padding:8px; display:flex; flex-direction:column; gap:6px; }
  .section h4 { margin:0; font-size:13px; color:#334155; }
  .section label { display:flex; flex-direction:column; gap:4px; font-size:12px; color:#334155; }
  .section select, .section input, .section button { border:1px solid #dbe4f0; border-radius:10px; padding:6px 8px; background:#fff; font-size:12px; }
  .section button { cursor:pointer; }
  .section button:disabled { opacity:.5; cursor:not-allowed; }
  .row-buttons { display:flex; flex-wrap:wrap; gap:6px; }
  .dates { display:flex; gap:6px; }
  .chips { display:flex; flex-wrap:wrap; gap:6px; }
  .chip { display:inline-flex; align-items:center; gap:6px; border:1px solid #dbe4f0; border-radius:999px; padding:4px 8px; background:#f8fbff; font-size:12px; }
  .chip button { border:none; background:transparent; cursor:pointer; padding:0; line-height:1; }
  .layer-item { display:flex; justify-content:space-between; align-items:center; gap:8px; font-size:12px; }
  .filter-item { display:flex; flex-direction:column; gap:6px; border:1px solid #e2e8f0; border-radius:10px; padding:6px; }
  .tooltip { position:absolute; pointer-events:none; background:rgba(15,23,42,.94); color:#f8fafc; font-size:12px; padding:10px; border-radius:10px; max-width:360px; }
  small { color:#64748b; font-size:11px; line-height:1.3; }
</style>
