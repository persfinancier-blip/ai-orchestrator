<!-- core/orchestrator/modules/advertising/interface/desk/components/GraphPanel.svelte -->
<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import * as THREE from 'three';
  import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
  import { CSS2DRenderer, CSS2DObject } from 'three/examples/jsm/renderers/CSS2DRenderer.js';
  import { get } from 'svelte/store';
  import { showcaseStore, fieldName, type DatasetId, type ShowcaseField, type ShowcaseRow } from '../data/showcaseStore';

  type PeriodMode = '7 дней' | '14 дней' | '30 дней' | 'Даты';

  type SpacePoint = {
    id: string;
    label: string;
    sourceField: string;
    metrics: Record<string, number>;
    x: number;
    y: number;
    z: number;
  };

  type BBox = { minX: number; maxX: number; minY: number; maxY: number; minZ: number; maxZ: number };

  // ---- storage
  const STORAGE_KEY_VISUALS = 'desk-space-visual-schemes-v2';
  const STORAGE_KEY_DATASETS = 'desk-space-datasets-v2';

  // ---- data
  let activeLayers: DatasetId[] = ['sales_fact', 'ads'];

  // “Выбрать данные”
  let selectedEntityFields: string[] = ['sku', 'campaign_id']; // text => точки
  let axisX = ''; // number|date => координата
  let axisY = '';
  let axisZ = '';

  // период
  let period: PeriodMode = '30 дней';
  let fromDate = '';
  let toDate = '';
  let search = '';

  // visuals
  type VisualScheme = { id: string; name: string; bg: string; edge: string };
  let visualBg = '#ffffff'; // ✅ по умолчанию белый
  let visualEdge = '#334155';
  let visualSchemes: VisualScheme[] = [];
  let selectedVisualId = '';

  // dataset presets
  type DatasetPreset = {
    id: string;
    name: string;
    value: {
      selectedEntityFields: string[];
      axisX: string;
      axisY: string;
      axisZ: string;
      period: PeriodMode;
      fromDate: string;
      toDate: string;
      search: string;
    };
  };
  let datasetPresets: DatasetPreset[] = [];
  let selectedDatasetPresetId = '';

  // UI popovers / modals
  let showDisplayMenu = false;
  let showPickDataMenu = false;

  let showSaveVisualModal = false;
  let saveVisualName = '';

  let showSaveDatasetModal = false;
  let saveDatasetName = '';

  // ---- store safe
  type ShowcaseSafe = { fields: ShowcaseField[]; rows: ShowcaseRow[]; datasets: any[] };
  const EMPTY_SHOWCASE: ShowcaseSafe = { fields: [], rows: [], datasets: [] };

  let showcase: ShowcaseSafe = ((get(showcaseStore) as any) ?? EMPTY_SHOWCASE) as ShowcaseSafe;

  const unsub = showcaseStore.subscribe((value) => {
    showcase = ((value as any) ?? EMPTY_SHOWCASE) as ShowcaseSafe;
    ensureDefaults(); // ✅ только когда поля реально есть
  });

  // ---- 3D
  let container3d: HTMLDivElement;

  let tooltip = { visible: false, x: 0, y: 0, lines: [] as string[] };
  let points: SpacePoint[] = [];
  let debugRenderedCount = 0;
  let debugBBox = '—';

  let renderer: THREE.WebGLRenderer;
  let scene: THREE.Scene;
  let camera: THREE.PerspectiveCamera;
  let controls: OrbitControls;
  let circleMesh: THREE.InstancedMesh | undefined;
  let diamondMesh: THREE.InstancedMesh | undefined;
  let labelRenderer: CSS2DRenderer | undefined;

  const planeGroup = new THREE.Group();
  const labelGroup = new THREE.Group();
  let cornerFrame: THREE.LineSegments<THREE.BufferGeometry, THREE.LineBasicMaterial> | null = null;
  let maxLabels: CSS2DObject[] = [];

  let circlePoints: SpacePoint[] = [];
  let diamondPoints: SpacePoint[] = [];
  let anim = 0;

  const raycaster = new THREE.Raycaster();
  const ndc = new THREE.Vector2();

  // ---- fields (без ролей — чтобы не падало на undefined)
  function availableFieldsByKind(kind: ShowcaseField['kind']): ShowcaseField[] {
    const fields = showcase?.fields ?? [];
    return fields.filter((f) => {
      if (f.kind !== kind) return false;
      const ids = (f as any).datasetIds ?? [];
      if (!Array.isArray(ids) || !ids.length) return true;
      return ids.some((id: DatasetId) => activeLayers.includes(id));
    });
  }

  $: textFieldsAll = availableFieldsByKind('text');
  $: numberFieldsAll = availableFieldsByKind('number');
  $: dateFieldsAll = availableFieldsByKind('date');
  $: coordFieldsAll = [...numberFieldsAll, ...dateFieldsAll];

  $: selectedCoordCount = [axisX, axisY, axisZ].filter(Boolean).length;
  $: canAddCoord = selectedCoordCount < 3;

  // ---- rows/points
  $: filteredRows = applyRowsFilter(showcase?.rows ?? []);
  $: points = buildPoints(filteredRows, selectedEntityFields, axisX, axisY, axisZ);
  $: rebuildScene(points);

  function ensureDefaults(): void {
    const coords = coordFieldsAll ?? [];
    // ✅ если данных ещё нет — ничего не трогаем
    if (!coords.length) return;

    if (!axisX && coords[0]) axisX = coords[0].code;
    if (!axisY && coords[1]) axisY = coords[1].code;
    if (!axisZ && coords[2]) axisZ = coords[2].code;
  }

  function hashToJitter(input: string): number {
    let hash = 0;
    for (let i = 0; i < input.length; i += 1) hash = (hash * 31 + input.charCodeAt(i)) >>> 0;
    return ((hash % 1000) / 1000 - 0.5) * 10;
  }

  function inPeriod(date: string): boolean {
    if (!date) return true;
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

  function applyRowsFilter(rows: ShowcaseRow[]): ShowcaseRow[] {
    return rows.filter((row) => {
      const anyRow = row as any;
      if (!inPeriod(String(anyRow.date ?? ''))) return false;

      if (!search.trim()) return true;
      const q = search.trim().toLowerCase();
      return selectedEntityFields.some((field) => String((row as any)?.[field] ?? '').toLowerCase().includes(q));
    });
  }

  function parseDateToNumber(v: unknown): number {
    const s = String(v ?? '').trim();
    if (!s) return NaN;
    const t = Date.parse(s);
    return Number.isFinite(t) ? t : NaN;
  }

  function buildPoints(rows: ShowcaseRow[], entityFields: string[], xCode: string, yCode: string, zCode: string): SpacePoint[] {
    if (!entityFields.length) return [];

    const result: SpacePoint[] = [];
    const numberCodes = (numberFieldsAll ?? []).map((f) => f.code);
    const dateCodes = (dateFieldsAll ?? []).map((f) => f.code);

    for (const entityField of entityFields) {
      const groups = new Map<string, ShowcaseRow[]>();

      for (const row of rows) {
        const key = String((row as any)?.[entityField] ?? '').trim();
        if (!key) continue;
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key)!.push(row);
      }

      groups.forEach((groupRows, key) => {
        const metrics: Record<string, number> = {};

        // numbers avg
        for (const code of numberCodes) {
          let sum = 0;
          for (const r of groupRows) sum += Number((r as any)?.[code] ?? 0);
          metrics[code] = sum / Math.max(groupRows.length, 1);
        }

        // dates avg timestamp
        for (const code of dateCodes) {
          let sum = 0;
          let cnt = 0;
          for (const r of groupRows) {
            const n = parseDateToNumber((r as any)?.[code]);
            if (Number.isFinite(n)) {
              sum += n;
              cnt += 1;
            }
          }
          metrics[code] = cnt ? sum / cnt : NaN;
        }

        // expected fields
        metrics.revenue = groupRows.reduce((s, r) => s + Number((r as any)?.revenue ?? 0), 0);
        metrics.spend = groupRows.reduce((s, r) => s + Number((r as any)?.spend ?? 0), 0);
        metrics.orders = groupRows.reduce((s, r) => s + Number((r as any)?.orders ?? 0), 0);
        metrics.drr = metrics.revenue > 0 ? (metrics.spend / metrics.revenue) * 100 : 0;
        metrics.roi = metrics.spend > 0 ? metrics.revenue / metrics.spend : 0;

        result.push({
          id: `${entityField}:${key}`,
          label: key,
          sourceField: entityField,
          metrics,
          x: 0,
          y: 0,
          z: 0
        });
      });
    }

    return projectPoints(result, xCode, yCode, zCode);
  }

  function normalize(value: number, min: number, max: number): number {
    return ((value - min) / Math.max(max - min, 0.0001) - 0.5) * 180;
  }

  function projectPoints(list: SpacePoint[], xCode: string, yCode: string, zCode: string): SpacePoint[] {
    if (!list.length) return [];

    const useX = !!xCode;
    const useY = !!yCode;
    const useZ = !!zCode;

    const xValues = useX ? list.map((p) => p.metrics[xCode]).filter((n) => Number.isFinite(n)) : [0, 1];
    const yValues = useY ? list.map((p) => p.metrics[yCode]).filter((n) => Number.isFinite(n)) : [0, 1];
    const zValues = useZ ? list.map((p) => p.metrics[zCode]).filter((n) => Number.isFinite(n)) : [0, 1];

    const minX = Math.min(...xValues);
    const maxX = Math.max(...xValues);
    const minY = Math.min(...yValues);
    const maxY = Math.max(...yValues);
    const minZ = Math.min(...zValues);
    const maxZ = Math.max(...zValues);

    return list.map((point) => {
      const x = useX && Number.isFinite(point.metrics[xCode]) ? normalize(point.metrics[xCode], minX, maxX) : hashToJitter(`${point.id}:x`);
      const y = useY && Number.isFinite(point.metrics[yCode]) ? normalize(point.metrics[yCode], minY, maxY) : hashToJitter(`${point.id}:y`);
      const z = useZ && Number.isFinite(point.metrics[zCode]) ? normalize(point.metrics[zCode], minZ, maxZ) : hashToJitter(`${point.id}:z`);
      return { ...point, x, y, z };
    });
  }

  function sanitizeCoord(value: number, id: string, axis: 'x' | 'y' | 'z'): number {
    if (!Number.isFinite(value)) return hashToJitter(`${id}:${axis}`) * 0.1;
    return value;
  }

  function sanitizePoints(input: SpacePoint[]): SpacePoint[] {
    return input.map((point) => ({
      ...point,
      x: sanitizeCoord(point.x, point.id, 'x') + hashToJitter(`${point.id}:x`) * 0.05,
      y: sanitizeCoord(point.y, point.id, 'y') + hashToJitter(`${point.id}:y`) * 0.05,
      z: sanitizeCoord(point.z, point.id, 'z') + hashToJitter(`${point.id}:z`) * 0.05
    }));
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
      maxZ: Math.max(...zs)
    };
  }

  function normalizeBBox(list: SpacePoint[]): BBox {
    if (!list.length) return { minX: -1, maxX: 1, minY: -1, maxY: 1, minZ: -1, maxZ: 1 };
    const bbox = buildBBox(list);
    const spanX = bbox.maxX - bbox.minX;
    const spanY = bbox.maxY - bbox.minY;
    const spanZ = bbox.maxZ - bbox.minZ;
    if (spanX < 0.001 && spanY < 0.001 && spanZ < 0.001) {
      const cx = (bbox.minX + bbox.maxX) / 2;
      const cy = (bbox.minY + bbox.maxY) / 2;
      const cz = (bbox.minZ + bbox.maxZ) / 2;
      return { minX: cx - 1, maxX: cx + 1, minY: cy - 1, maxY: cy + 1, minZ: cz - 1, maxZ: cz + 1 };
    }
    return bbox;
  }

  function fitCameraToPoints(list: SpacePoint[]): void {
    if (!camera || !controls) return;

    if (!list.length) {
      debugBBox = '—';
      camera.position.set(0, 0, 40);
      controls.target.set(0, 0, 0);
      controls.update();
      return;
    }

    const bbox = buildBBox(list);
    debugBBox = `x ${bbox.minX.toFixed(2)}..${bbox.maxX.toFixed(2)} | y ${bbox.minY.toFixed(2)}..${bbox.maxY.toFixed(2)} | z ${bbox.minZ.toFixed(2)}..${bbox.maxZ.toFixed(2)}`;

    const center = new THREE.Vector3((bbox.minX + bbox.maxX) / 2, (bbox.minY + bbox.maxY) / 2, (bbox.minZ + bbox.maxZ) / 2);
    const spanX = Math.max(0.001, bbox.maxX - bbox.minX);
    const spanY = Math.max(0.001, bbox.maxY - bbox.minY);
    const spanZ = Math.max(0.001, bbox.maxZ - bbox.minZ);
    const maxSpan = Math.max(spanX, spanY, spanZ);

    const distance = Math.max(10, maxSpan * 1.55);
    camera.position.set(center.x + distance * 0.55, center.y + distance * 0.45, center.z + distance);
    controls.target.copy(center);
    controls.update();
  }

  function calcMax(pointsList: SpacePoint[], metricKey: string): number {
    if (!metricKey) return NaN;
    let max = -Infinity;
    for (const p of pointsList) {
      const v = p?.metrics?.[metricKey];
      if (Number.isFinite(v)) max = Math.max(max, v);
    }
    return max === -Infinity ? NaN : max;
  }

  function formatNumberHuman(n: number): string {
    if (!Number.isFinite(n)) return 'нет данных';
    const abs = Math.abs(n);
    if (abs >= 1_000_000_000) return (n / 1_000_000_000).toFixed(1).replace('.', ',') + ' млрд';
    if (abs >= 1_000_000) return (n / 1_000_000).toFixed(1).replace('.', ',') + ' млн';
    if (abs >= 1_000) return (n / 1_000).toFixed(1).replace('.', ',') + ' тыс';
    return Math.round(n).toString();
  }

  function formatValueByMetric(metricCode: string, maxValue: number): string {
    if (!metricCode) return '—';
    if (!Number.isFinite(maxValue)) return 'нет данных';

    const f = (showcase?.fields ?? []).find((x) => x.code === metricCode);
    if (f?.kind === 'date') {
      const d = new Date(maxValue);
      if (Number.isNaN(d.getTime())) return 'нет данных';
      const dd = String(d.getDate()).padStart(2, '0');
      const mm = String(d.getMonth() + 1).padStart(2, '0');
      const yy = d.getFullYear();
      return `${dd}.${mm}.${yy}`;
    }

    const money = new Set(['revenue', 'spend', 'margin', 'profit']);
    if (money.has(metricCode)) return `${formatNumberHuman(maxValue)} ₽`;

    const percent = new Set(['drr', 'cr0', 'cr1', 'cr2', 'roi_pct']);
    if (percent.has(metricCode)) {
      const v = maxValue <= 1 ? maxValue * 100 : maxValue;
      return `${v.toFixed(1).replace('.', ',')}%`;
    }

    if (metricCode === 'roi') return maxValue.toFixed(2).replace('.', ',');

    return formatNumberHuman(maxValue);
  }

  // ---- wireframe & max labels
  function disposeCornerFrame(): void {
    if (!cornerFrame) return;
    planeGroup.remove(cornerFrame);
    cornerFrame.geometry.dispose();
    cornerFrame.material.dispose();
    cornerFrame = null;
  }

  function disposeMaxLabels(): void {
    for (const l of maxLabels) labelGroup.remove(l);
    maxLabels = [];
  }

  function createCloud(text: string): CSS2DObject {
    const el = document.createElement('div');
    el.className = 'cloud';
    el.textContent = text;
    return new CSS2DObject(el);
  }

  function rebuildPlanes(list: SpacePoint[]): void {
    planeGroup.clear();
    disposeCornerFrame();
    disposeMaxLabels();

    const bbox = normalizeBBox(list);

    const A = new THREE.Vector3(bbox.minX, bbox.minY, bbox.minZ);
    const Bx = new THREE.Vector3(bbox.maxX, bbox.minY, bbox.minZ);
    const By = new THREE.Vector3(bbox.minX, bbox.maxY, bbox.minZ);
    const Bz = new THREE.Vector3(bbox.minX, bbox.minY, bbox.maxZ);

    const Cxy = new THREE.Vector3(bbox.maxX, bbox.maxY, bbox.minZ);
    const Cxz = new THREE.Vector3(bbox.maxX, bbox.minY, bbox.maxZ);
    const Cyz = new THREE.Vector3(bbox.minX, bbox.maxY, bbox.maxZ);

    // 3 плоскости-угол (без верхних граней)
    const segments: Array<[THREE.Vector3, THREE.Vector3]> = [
      // XY (z=min)
      [A, Bx],
      [Bx, Cxy],
      [Cxy, By],
      [By, A],

      // XZ (y=min)
      [A, Bx],
      [Bx, Cxz],
      [Cxz, Bz],
      [Bz, A],

      // YZ (x=min)
      [A, By],
      [By, Cyz],
      [Cyz, Bz],
      [Bz, A]
    ];

    const vertices: number[] = [];
    for (const [p1, p2] of segments) vertices.push(p1.x, p1.y, p1.z, p2.x, p2.y, p2.z);

    const geom = new THREE.BufferGeometry();
    geom.setAttribute('position', new THREE.Float32BufferAttribute(vertices, 3));
    const mat = new THREE.LineBasicMaterial({ color: new THREE.Color(visualEdge) });

    cornerFrame = new THREE.LineSegments(geom, mat);
    planeGroup.add(cornerFrame);

    // max values at ends
    const xMax = axisX ? calcMax(list, axisX) : NaN;
    const yMax = axisY ? calcMax(list, axisY) : NaN;
    const zMax = axisZ ? calcMax(list, axisZ) : NaN;

    const mx = createCloud(formatValueByMetric(axisX, xMax));
    mx.position.copy(Bx);

    const my = createCloud(formatValueByMetric(axisY, yMax));
    my.position.copy(By);

    const mz = createCloud(formatValueByMetric(axisZ, zMax));
    mz.position.copy(Bz);

    maxLabels = [mx, my, mz];
    labelGroup.add(...maxLabels);
  }

  // ---- init / render
  function init3d(): void {
    const width = container3d.clientWidth;
    const height = 560;

    scene = new THREE.Scene();
    scene.background = new THREE.Color(visualBg);

    camera = new THREE.PerspectiveCamera(52, width / height, 0.1, 1500);
    camera.position.set(0, 80, 220);

    renderer = new THREE.WebGLRenderer({ antialias: true, alpha: false });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    renderer.setSize(width, height);

    container3d.innerHTML = '';
    container3d.appendChild(renderer.domElement);

    controls = new OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.08;
    controls.rotateSpeed = 0.6;
    controls.panSpeed = 0.7;
    controls.zoomSpeed = 0.9;

    scene.add(new THREE.AmbientLight('#ffffff', 0.95));
    const light = new THREE.DirectionalLight('#ffffff', 0.35);
    light.position.set(80, 120, 75);
    scene.add(light);

    scene.add(planeGroup);
    scene.add(labelGroup);

    labelRenderer = new CSS2DRenderer();
    labelRenderer.setSize(width, height);
    labelRenderer.domElement.style.position = 'absolute';
    labelRenderer.domElement.style.left = '0';
    labelRenderer.domElement.style.top = '0';
    labelRenderer.domElement.style.pointerEvents = 'none';
    labelRenderer.domElement.className = 'label-layer';
    container3d.appendChild(labelRenderer.domElement);

    renderer.domElement.addEventListener('mousemove', onMove3d);
    animate();
  }

  function clearMesh(mesh?: THREE.InstancedMesh): void {
    if (!mesh || !scene) return;
    scene.remove(mesh);
    mesh.geometry.dispose();
    (mesh.material as THREE.Material).dispose();
  }

  function rebuildScene(sourcePoints: SpacePoint[]): void {
    if (!scene) return;

    scene.background = new THREE.Color(visualBg);

    if (cornerFrame) {
      const m = cornerFrame.material as THREE.LineBasicMaterial;
      m.color = new THREE.Color(visualEdge);
      m.needsUpdate = true;
    }

    clearMesh(circleMesh);
    clearMesh(diamondMesh);

    const renderablePoints = sanitizePoints(sourcePoints);
    debugRenderedCount = renderablePoints.length;

    circlePoints = renderablePoints.filter((p) => p.sourceField !== 'campaign_id');
    diamondPoints = renderablePoints.filter((p) => p.sourceField === 'campaign_id');

    // ✅ фикс “зелёной хрени”: не раздуваем точки, размер стабильный
    if (circlePoints.length) {
      circleMesh = new THREE.InstancedMesh(
        new THREE.SphereGeometry(1.35, 10, 10),
        new THREE.MeshBasicMaterial({ color: '#22c55e' }),
        circlePoints.length
      );
      const o = new THREE.Object3D();
      circlePoints.forEach((point, idx) => {
        o.position.set(point.x, point.y, point.z);
        o.updateMatrix();
        circleMesh!.setMatrixAt(idx, o.matrix);
      });
      scene.add(circleMesh);
    }

    if (diamondPoints.length) {
      diamondMesh = new THREE.InstancedMesh(
        new THREE.OctahedronGeometry(1.75, 0),
        new THREE.MeshBasicMaterial({ color: '#1d4ed8' }),
        diamondPoints.length
      );
      const o = new THREE.Object3D();
      diamondPoints.forEach((point, idx) => {
        o.position.set(point.x, point.y, point.z);
        o.rotation.set(0.2, 0.5, 0.1);
        o.updateMatrix();
        diamondMesh!.setMatrixAt(idx, o.matrix);
      });
      scene.add(diamondMesh);
    }

    rebuildPlanes(renderablePoints);
    fitCameraToPoints(renderablePoints);
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
        `Выручка: ${Math.round(point.metrics.revenue ?? 0).toLocaleString('ru-RU')} ₽`,
        `Расход: ${Math.round(point.metrics.spend ?? 0).toLocaleString('ru-RU')} ₽`,
        `ДРР: ${(point.metrics.drr ?? 0).toFixed(2)}% · ROI: ${(point.metrics.roi ?? 0).toFixed(2)}`
      ]
    };
  }

  function animate(): void {
    controls?.update();
    renderer?.render(scene, camera);
    labelRenderer?.render(scene, camera);
    anim = requestAnimationFrame(animate);
  }

  function onResize(): void {
    if (!renderer || !camera || !container3d) return;
    const width = container3d.clientWidth;
    const height = 560;
    renderer.setSize(width, height);
    labelRenderer?.setSize(width, height);
    camera.aspect = width / height;
    camera.updateProjectionMatrix();
  }

  function resetView(): void {
    camera.position.set(0, 80, 220);
    controls.target.set(0, 0, 0);
    controls.update();
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

  function clearAxis(slot: 'x' | 'y' | 'z'): void {
    if (slot === 'x') axisX = '';
    if (slot === 'y') axisY = '';
    if (slot === 'z') axisZ = '';
  }

  // ---- menus
  function toggleDisplayMenu(): void {
    showDisplayMenu = !showDisplayMenu;
    if (showDisplayMenu) showPickDataMenu = false;
  }
  function togglePickDataMenu(): void {
    showPickDataMenu = !showPickDataMenu;
    if (showPickDataMenu) showDisplayMenu = false;
  }
  function closeAllMenus(): void {
    showDisplayMenu = false;
    showPickDataMenu = false;
  }

  function onGlobalClick(e: MouseEvent): void {
    const t = e.target as Node | null;
    if (!t) return;
    const inMenu = document.querySelector('.menu-pop')?.contains(t) ?? false;
    const inBtn = document.querySelector('.hud')?.contains(t) ?? false;
    if (!inMenu && !inBtn) closeAllMenus();
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

  // ---- visuals persistence
  function loadVisualSchemes(): void {
    try {
      const raw = localStorage.getItem(STORAGE_KEY_VISUALS);
      visualSchemes = raw ? JSON.parse(raw) : [];
    } catch {
      visualSchemes = [];
    }
  }

  function saveVisualScheme(name: string): void {
    const trimmed = name.trim();
    if (!trimmed) return;
    const item: VisualScheme = { id: `${Date.now()}-${Math.random()}`, name: trimmed, bg: visualBg, edge: visualEdge };
    visualSchemes = [item, ...visualSchemes].slice(0, 30);
    selectedVisualId = item.id;
    localStorage.setItem(STORAGE_KEY_VISUALS, JSON.stringify(visualSchemes));
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

  // ---- dataset presets persistence
  function loadDatasetPresets(): void {
    try {
      const raw = localStorage.getItem(STORAGE_KEY_DATASETS);
      datasetPresets = raw ? JSON.parse(raw) : [];
    } catch {
      datasetPresets = [];
    }
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
    localStorage.setItem(STORAGE_KEY_DATASETS, JSON.stringify(datasetPresets));
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

  // ---- breadcrumbs (под кнопками)
  type Crumb = { kind: 'text' | 'axis'; label: string; code: string; slot?: 'x' | 'y' | 'z' };
  $: crumbs = buildCrumbs();

  function buildCrumbs(): Crumb[] {
    const out: Crumb[] = [];

    // axes first (X/Y/Z)
    if (axisX) out.push({ kind: 'axis', label: `X: ${fieldName(axisX)}`, code: axisX, slot: 'x' });
    if (axisY) out.push({ kind: 'axis', label: `Y: ${fieldName(axisY)}`, code: axisY, slot: 'y' });
    if (axisZ) out.push({ kind: 'axis', label: `Z: ${fieldName(axisZ)}`, code: axisZ, slot: 'z' });

    // then text fields
    for (const code of selectedEntityFields) {
      out.push({ kind: 'text', label: fieldName(code), code });
    }
    return out;
  }

  function removeCrumb(c: Crumb): void {
    if (c.kind === 'text') removeEntityField(c.code);
    else if (c.slot) clearAxis(c.slot);
  }

  // ---- mount/unmount
  onMount(() => {
    loadVisualSchemes();
    loadDatasetPresets();

    init3d();
    ensureDefaults();
    rebuildScene(points);

    window.addEventListener('resize', onResize);
    window.addEventListener('keydown', onKey);
    window.addEventListener('click', onGlobalClick, true);
  });

  onDestroy(() => {
    unsub();
    cancelAnimationFrame(anim);

    window.removeEventListener('resize', onResize);
    window.removeEventListener('keydown', onKey);
    window.removeEventListener('click', onGlobalClick, true);

    disposeMaxLabels();
    disposeCornerFrame();

    renderer?.domElement?.removeEventListener('mousemove', onMove3d);
    renderer?.dispose();
    controls?.dispose();
    labelRenderer?.domElement?.remove();
  });
</script>

<section class="graph-root">
  <div class="stage">
    <div bind:this={container3d} class="scene" />

    <!-- HUD: кнопки справа сверху + хлебные крошки под ними -->
    <div class="hud">
      <div class="hud-right">
        <button class="btn ghost" on:click={toggleDisplayMenu}>Настройка отображения</button>
        <button class="btn primary" on:click={togglePickDataMenu}>Выбрать данные</button>

        <div class="crumbs">
          {#each crumbs as c (c.kind + ':' + c.code + ':' + (c.slot ?? ''))}
            <button class="crumb" type="button" on:click={() => removeCrumb(c)} title="Убрать">
              <span>{c.label}</span>
              <span class="x">×</span>
            </button>
          {/each}
        </div>
      </div>
    </div>

    <!-- Меню “Настройка отображения” -->
    {#if showDisplayMenu}
      <div class="menu-pop">
        <div class="menu-title">Визуал</div>

        <div class="row">
          <div class="label">Фон</div>
          <div class="color-wrap">
            <input class="color" type="color" bind:value={visualBg} aria-label="Цвет фона" />
            <div class="swatch" style={`background:${visualBg}`} />
          </div>
        </div>

        <div class="row">
          <div class="label">Рёбра</div>
          <div class="color-wrap">
            <input class="color" type="color" bind:value={visualEdge} aria-label="Цвет рёбер" />
            <div class="swatch" style={`background:${visualEdge}`} />
          </div>
        </div>

        <div class="hr" />

        <div class="row two">
          <button class="btn ghost" on:click={resetVisual}>Сбросить</button>
          <button
            class="btn primary"
            on:click={() => {
              saveVisualName = '';
              showSaveVisualModal = true;
              closeAllMenus();
            }}
          >
            Сохранить схему
          </button>
        </div>

        <div class="row">
          <select class="select" bind:value={selectedVisualId} on:change={() => applyVisualScheme(selectedVisualId)}>
            <option value="">Выберите схему</option>
            {#each visualSchemes as v}
              <option value={v.id}>{v.name}</option>
            {/each}
          </select>
        </div>

        <div class="hr" />

        <div class="menu-title">Наборы данных</div>

        <div class="row two">
          <button
            class="btn ghost"
            on:click={() => {
              selectedEntityFields = [];
              axisX = '';
              axisY = '';
              axisZ = '';
              search = '';
            }}
          >
            Заводские
          </button>

          <button
            class="btn primary"
            on:click={() => {
              saveDatasetName = '';
              showSaveDatasetModal = true;
              closeAllMenus();
            }}
          >
            Сохранить набор
          </button>
        </div>

        <div class="row">
          <select class="select" bind:value={selectedDatasetPresetId} on:change={() => applyDatasetPreset(selectedDatasetPresetId)}>
            <option value="">Выберите набор</option>
            {#each datasetPresets as p}
              <option value={p.id}>{p.name}</option>
            {/each}
          </select>
        </div>

        <div class="row">
          <button class="btn ghost wide" on:click={resetView}>Сбросить камеру</button>
        </div>
      </div>
    {/if}

    <!-- Меню “Выбрать данные” -->
    {#if showPickDataMenu}
      <div class="menu-pop">
        <div class="menu-title">Выбор данных</div>

        <div class="sub">Текстовые → точки</div>
        <div class="list">
          {#each textFieldsAll as f}
            <button class="item" disabled={selectedEntityFields.includes(f.code)} on:click={() => addEntityField(f.code)}>
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
            {#each coordFieldsAll as f}
              <button class="item" disabled={axisX === f.code || axisY === f.code || axisZ === f.code} on:click={() => addCoordField(f.code)}>
                <span class="name">{f.name}</span>
                <span class="tag">{f.kind === 'date' ? 'дата' : 'число'}</span>
              </button>
            {/each}
          </div>
        {:else}
          <div class="limit">Уже выбрано 3 координаты (X/Y/Z). Удалите одну в хлебных крошках (×), чтобы добавить другую.</div>
        {/if}

        <div class="hr" />

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
          <button class="btn ghost" on:click={closeAllMenus}>Закрыть</button>
          <button class="btn primary" on:click={closeAllMenus}>Готово</button>
        </div>
      </div>
    {/if}

    <!-- Tooltip -->
    {#if tooltip.visible}
      <div class="tooltip" style={`left:${tooltip.x}px;top:${tooltip.y}px`}>
        {#each tooltip.lines as line}<div>{line}</div>{/each}
      </div>
    {/if}

    <!-- Info: слева снизу -->
    <div class="info">
      <div class="info-title">Точек: {debugRenderedCount}</div>
      <div class="info-sub">Оси: {fieldName(axisX || '—')} / {fieldName(axisY || '—')} / {fieldName(axisZ || '—')}</div>
      <div class="info-sub">{debugBBox}</div>
    </div>
  </div>

  <!-- Modal: save visual -->
  {#if showSaveVisualModal}
    <div class="modal-backdrop" on:click={() => (showSaveVisualModal = false)}>
      <div class="modal" on:click|stopPropagation>
        <div class="modal-title">Сохранить визуальную схему</div>
        <input class="input" placeholder="Например: Светлый фон" bind:value={saveVisualName} />
        <div class="row two">
          <button class="btn ghost" on:click={() => (showSaveVisualModal = false)}>Отмена</button>
          <button
            class="btn primary"
            on:click={() => {
              saveVisualScheme(saveVisualName);
              showSaveVisualModal = false;
            }}
          >
            Сохранить
          </button>
        </div>
      </div>
    </div>
  {/if}

  <!-- Modal: save dataset -->
  {#if showSaveDatasetModal}
    <div class="modal-backdrop" on:click={() => (showSaveDatasetModal = false)}>
      <div class="modal" on:click|stopPropagation>
        <div class="modal-title">Сохранить набор данных</div>
        <input class="input" placeholder="Например: Артикул + Выручка/Заказы/Расход" bind:value={saveDatasetName} />
        <div class="row two">
          <button class="btn ghost" on:click={() => (showSaveDatasetModal = false)}>Отмена</button>
          <button
            class="btn primary"
            on:click={() => {
              saveDatasetPreset(saveDatasetName);
              showSaveDatasetModal = false;
            }}
          >
            Сохранить
          </button>
        </div>
      </div>
    </div>
  {/if}
</section>

<style>
  .graph-root { width: 100%; }

  .stage {
    position: relative;
    height: 560px;
    border-radius: 18px;
    overflow: hidden;
    background: #ffffff; /* ✅ белый дефолт */
  }

  .scene { position: absolute; inset: 0; }

  /* HUD: справа сверху */
  .hud {
    position: absolute;
    top: 12px;
    right: 14px;
    display: flex;
    justify-content: flex-end;
    pointer-events: none;
    z-index: 5;
  }

  .hud-right {
    display: flex;
    flex-direction: column;
    align-items: flex-end;
    gap: 10px;
    pointer-events: auto;
  }

  .crumbs {
    display: flex;
    flex-wrap: wrap;
    justify-content: flex-end;
    gap: 8px;
    max-width: min(680px, calc(100vw - 40px));
  }

  .crumb {
    border: 0;
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 8px 10px;
    border-radius: 999px;
    background: rgba(255, 255, 255, 0.78);
    color: rgba(15, 23, 42, 0.9);
    box-shadow: 0 10px 30px rgba(15, 23, 42, 0.10);
    backdrop-filter: blur(10px);
    font-size: 12px;
    font-weight: 650;
    white-space: nowrap;
  }
  .crumb .x {
    width: 18px;
    height: 18px;
    border-radius: 999px;
    display: grid;
    place-items: center;
    background: rgba(15, 23, 42, 0.06);
    font-weight: 900;
    line-height: 1;
  }
  .crumb:hover .x { background: rgba(37, 99, 235, 0.16); }

  /* buttons: “как в заголовке таблицы” — синий акцент */
  .btn {
    border: 0;
    border-radius: 999px;
    padding: 10px 14px;
    font-size: 13px;
    font-weight: 650;
    cursor: pointer;
    line-height: 1;
    background: rgba(255, 255, 255, 0.78);
    color: rgba(15, 23, 42, 0.9);
    backdrop-filter: blur(10px);
    box-shadow: 0 10px 30px rgba(15, 23, 42, 0.10);
  }
  .btn:hover { transform: translateY(-0.5px); }

  .btn.primary {
    background: rgba(37, 99, 235, 0.92);
    color: #ffffff;
    box-shadow: 0 12px 34px rgba(37, 99, 235, 0.22);
  }
  .btn.ghost { background: rgba(255, 255, 255, 0.78); }
  .btn.wide { width: 100%; }

  /* меню */
  .menu-pop {
    position: absolute;
    top: 58px;
    right: 14px;
    width: 340px;
    background: rgba(255, 255, 255, 0.92);
    border-radius: 18px;
    padding: 12px;
    box-shadow: 0 18px 50px rgba(15, 23, 42, 0.16);
    backdrop-filter: blur(14px);
    pointer-events: auto;
    z-index: 10;
  }

  .menu-title {
    font-weight: 750;
    font-size: 13px;
    color: rgba(15, 23, 42, 0.9);
    margin-bottom: 10px;
  }

  .sub {
    margin-top: 10px;
    font-size: 12px;
    font-weight: 650;
    color: rgba(15, 23, 42, 0.78);
    display: flex;
    align-items: baseline;
    gap: 6px;
  }

  .hint { font-weight: 600; color: rgba(100, 116, 139, 0.9); }

  .row {
    display: flex;
    gap: 10px;
    align-items: center;
    margin-top: 10px;
  }
  .row.two { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
  .label { font-size: 12px; color: rgba(15,23,42,.78); width: 52px; }

  .hr {
    height: 1px;
    background: rgba(226, 232, 240, 0.7);
    margin: 12px 0;
  }

  /* ✅ современный “без рамок” выбор цвета */
  .color-wrap {
    display: flex;
    align-items: center;
    gap: 10px;
  }
  .color {
    width: 30px;
    height: 30px;
    padding: 0;
    border: 0;             /* ✅ убрать границы */
    background: transparent;
    appearance: none;
    cursor: pointer;
  }
  .color::-webkit-color-swatch-wrapper { padding: 0; border: 0; }
  .color::-webkit-color-swatch { border: 0; border-radius: 10px; } /* ✅ без рамок (красное на скрине) */
  .swatch {
    width: 44px;
    height: 30px;
    border-radius: 12px;
    box-shadow: inset 0 0 0 1px rgba(226,232,240,.55);
  }

  .select, .input {
    width: 100%;
    border: 0;
    background: rgba(248, 250, 252, 0.9);
    border-radius: 12px;
    padding: 10px 12px;
    font-size: 12px;
    outline: none;
    box-shadow: inset 0 0 0 1px rgba(226, 232, 240, 0.85);
  }

  .select:focus, .input:focus {
    box-shadow: inset 0 0 0 1px rgba(37, 99, 235, 0.55), 0 0 0 4px rgba(37, 99, 235, 0.12);
  }

  .list {
    margin-top: 8px;
    display: flex;
    flex-direction: column;
    gap: 6px;
    max-height: 190px;
    overflow: auto;
    padding-right: 2px;
  }

  .item {
    width: 100%;
    text-align: left;
    border: 0;
    background: rgba(248, 250, 252, 0.9);
    border-radius: 14px;
    padding: 10px 10px;
    display: flex;
    justify-content: space-between;
    gap: 10px;
    cursor: pointer;
    box-shadow: inset 0 0 0 1px rgba(226, 232, 240, 0.85);
  }
  .item:disabled { opacity: .45; cursor: not-allowed; }
  .name { font-size: 12px; font-weight: 650; color: rgba(15,23,42,.88); }
  .tag { font-size: 11px; color: rgba(100,116,139,.9); }

  .limit {
    margin-top: 8px;
    font-size: 12px;
    color: rgba(100, 116, 139, 0.95);
    background: rgba(241, 245, 249, 0.9);
    border-radius: 14px;
    padding: 10px 12px;
    box-shadow: inset 0 0 0 1px rgba(226, 232, 240, 0.85);
  }

  /* tooltip */
  .tooltip {
    position: absolute;
    pointer-events: none;
    background: rgba(15, 23, 42, 0.92);
    color: #f8fafc;
    font-size: 12px;
    padding: 10px;
    border-radius: 12px;
    max-width: 360px;
    z-index: 20;
  }

  /* info: слева снизу */
  .info {
    position: absolute;
    left: 14px;
    bottom: 12px;
    background: rgba(255,255,255,.74);
    border-radius: 14px;
    padding: 10px 12px;
    box-shadow: 0 18px 50px rgba(15, 23, 42, 0.12);
    backdrop-filter: blur(12px);
    pointer-events: none;
    z-index: 6;
    max-width: 380px;
  }
  .info-title { font-size: 12px; font-weight: 750; color: rgba(15,23,42,.9); }
  .info-sub { font-size: 11px; color: rgba(100,116,139,.92); margin-top: 2px; }

  /* modal */
  .modal-backdrop {
    position: fixed;
    inset: 0;
    background: rgba(15, 23, 42, 0.35);
    display: grid;
    place-items: center;
    z-index: 50;
  }

  .modal {
    width: min(520px, calc(100vw - 24px));
    background: rgba(255, 255, 255, 0.92);
    border-radius: 18px;
    padding: 14px;
    box-shadow: 0 30px 80px rgba(15, 23, 42, 0.24);
    backdrop-filter: blur(14px);
  }

  .modal-title {
    font-weight: 800;
    font-size: 14px;
    color: rgba(15, 23, 42, 0.92);
    margin-bottom: 10px;
  }

  /* clouds (max labels) */
  :global(.cloud) {
    transform: translate(-50%, -50%);
    pointer-events: none;
    user-select: none;
    white-space: nowrap;
    font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial;
    font-size: 12px;
    font-weight: 750;
    padding: 6px 10px;
    border-radius: 999px;
    background: rgba(239, 246, 255, 0.92);
    color: rgba(30, 64, 175, 0.98);
    box-shadow: 0 12px 30px rgba(15, 23, 42, 0.14);
  }
</style>
