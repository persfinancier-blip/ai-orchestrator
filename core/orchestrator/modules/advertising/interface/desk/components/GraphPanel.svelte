<!-- core/orchestrator/modules/advertising/interface/desk/components/GraphPanel.svelte -->
<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import * as THREE from 'three';
  import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
  import { CSS2DRenderer, CSS2DObject } from 'three/examples/jsm/renderers/CSS2DRenderer.js';
  import { get } from 'svelte/store';
  import { showcaseStore, fieldName, type DatasetId, type ShowcaseField, type ShowcaseRow } from '../data/showcaseStore';

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

  // ───────────────────────────────────────────────────────────
  // storage
  const STORAGE_DATASETS = 'desk-space-dataset-presets-v1';
  const STORAGE_VISUALS = 'desk-space-visual-schemes-v1';
  const STORAGE_VISUAL_CURRENT = 'desk-space-visual-current-v1';

  // ───────────────────────────────────────────────────────────
  // data layers
  let activeLayers: DatasetId[] = ['sales_fact', 'ads'];

  // selection
  let selectedTextFields: string[] = ['sku', 'campaign_id']; // точки
  let selectedCoordFields: string[] = ['revenue', 'orders', 'spend']; // координаты (числа/даты), max 3

  // derived axes (первые 3 координаты)
  let axisX = '';
  let axisY = '';
  let axisZ = '';

  // UI add field
  let addMenuOpen = false;

  // UI display settings
  let displayMenuOpen = false;

  // modals
  let saveVisualModalOpen = false;
  let saveVisualName = '';

  let saveDatasetModalOpen = false;
  let saveDatasetName = '';

  // presets
  type DatasetPreset = { id: string; name: string; value: { selectedTextFields: string[]; selectedCoordFields: string[] } };
  type VisualScheme = { id: string; name: string; value: { bgColor: string; edgeColor: string } };

  let datasetPresets: DatasetPreset[] = [];
  let selectedDatasetPresetId = '';

  let visualSchemes: VisualScheme[] = [];
  let selectedVisualSchemeId = '';

  // visual settings
  const DEFAULT_BG = '#f8fbff';
  const DEFAULT_EDGE = '#334155';

  let bgColor = DEFAULT_BG;
  let edgeColor = DEFAULT_EDGE;

  // store
  let showcase = get(showcaseStore);
  const unsub = showcaseStore.subscribe((value) => {
    showcase = value;
  });

  // DOM/3D
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
  const edgeLabelGroup = new THREE.Group();
  let axisLabels: CSS2DObject[] = [];
  let cornerFrame: THREE.LineSegments<THREE.BufferGeometry, THREE.LineBasicMaterial> | null = null;

  let circlePoints: SpacePoint[] = [];
  let diamondPoints: SpacePoint[] = [];
  let anim = 0;

  const raycaster = new THREE.Raycaster();
  const ndc = new THREE.Vector2();

  // one global handler so we can remove it
  const onWindowClick = (e: MouseEvent) => {
    const target = e.target as HTMLElement | null;
    if (!target) return;

    // close Add menu if click outside
    if (addMenuOpen && !target.closest('.add-menu') && !target.closest('.add-btn')) addMenuOpen = false;

    // close Display menu if click outside
    if (displayMenuOpen && !target.closest('.display-menu') && !target.closest('.display-btn')) displayMenuOpen = false;
  };

  // ───────────────────────────────────────────────────────────
  // helpers
  function fieldKind(code: string): ShowcaseField['kind'] | null {
    const f = showcase.fields.find((x) => x.code === code);
    return f?.kind ?? null;
  }

  function ensureAxisFromCoords(): void {
    axisX = selectedCoordFields[0] ?? '';
    axisY = selectedCoordFields[1] ?? '';
    axisZ = selectedCoordFields[2] ?? '';
  }

  function coordLimitReached(): boolean {
    return selectedCoordFields.length >= 3;
  }

  function hashToJitter(input: string): number {
    let hash = 0;
    for (let i = 0; i < input.length; i += 1) hash = (hash * 31 + input.charCodeAt(i)) >>> 0;
    return ((hash % 1000) / 1000 - 0.5) * 10;
  }

  function normalize(value: number, min: number, max: number): number {
    return ((value - min) / Math.max(max - min, 0.0001) - 0.5) * 180;
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

  function toNumberByField(row: ShowcaseRow, code: string): number {
    const k = fieldKind(code);
    const v = (row as Record<string, unknown>)[code];

    if (k === 'number') {
      const n = Number(v ?? 0);
      return Number.isFinite(n) ? n : 0;
    }

    if (k === 'date') {
      const s = String(v ?? '');
      const t = Date.parse(s);
      return Number.isFinite(t) ? t : 0;
    }

    const n = Number(v ?? 0);
    return Number.isFinite(n) ? n : 0;
  }

  // ───────────────────────────────────────────────────────────
  // available fields
  function allFieldsByKind(kind: ShowcaseField['kind']): ShowcaseField[] {
    return showcase.fields.filter((f) => f.kind === kind && f.datasetIds.some((id) => activeLayers.includes(id)));
  }

  $: textFieldsAll = allFieldsByKind('text');
  $: numberFieldsAll = allFieldsByKind('number');
  $: dateFieldsAll = allFieldsByKind('date');

  function allowedTextFields(): ShowcaseField[] {
    return textFieldsAll.filter((f) => !selectedTextFields.includes(f.code));
  }

  function allowedCoordFields(): ShowcaseField[] {
    if (coordLimitReached()) return [];
    return [...numberFieldsAll, ...dateFieldsAll].filter((f) => !selectedCoordFields.includes(f.code));
  }

  // ───────────────────────────────────────────────────────────
  // build points
  $: points = buildPoints(showcase.rows, selectedTextFields, axisX, axisY, axisZ);
  $: rebuildScene(points, selectedTextFields);

  function buildPoints(rowsInput: ShowcaseRow[], textFields: string[], xCode: string, yCode: string, zCode: string): SpacePoint[] {
    if (!textFields.length) return [];

    const coordCodes = [xCode, yCode, zCode].filter(Boolean);
    const result: SpacePoint[] = [];

    for (const entityField of textFields) {
      const groups = new Map<string, ShowcaseRow[]>();

      for (const row of rowsInput) {
        const key = String((row as Record<string, unknown>)[entityField] ?? '').trim();
        if (!key) continue;
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key)!.push(row);
      }

      for (const [key, groupRows] of groups.entries()) {
        const metrics: Record<string, number> = {};

        for (const c of coordCodes) {
          const sum = groupRows.reduce((acc, r) => acc + toNumberByField(r, c), 0);
          metrics[c] = sum / Math.max(groupRows.length, 1);
        }

        // optional known metrics for tooltip
        metrics.revenue = groupRows.reduce((sum, row) => sum + Number((row as any).revenue ?? 0), 0);
        metrics.spend = groupRows.reduce((sum, row) => sum + Number((row as any).spend ?? 0), 0);
        metrics.orders = groupRows.reduce((sum, row) => sum + Number((row as any).orders ?? 0), 0);
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
      }
    }

    return projectPoints(result, xCode, yCode, zCode);
  }

  function projectPoints(list: SpacePoint[], xCode: string, yCode: string, zCode: string): SpacePoint[] {
    if (!list.length) return [];

    const useX = !!xCode;
    const useY = !!yCode;
    const useZ = !!zCode;

    const xValues = useX ? list.map((p) => p.metrics[xCode] ?? 0) : [0, 1];
    const yValues = useY ? list.map((p) => p.metrics[yCode] ?? 0) : [0, 1];
    const zValues = useZ ? list.map((p) => p.metrics[zCode] ?? 0) : [0, 1];

    const minX = Math.min(...xValues);
    const maxX = Math.max(...xValues);
    const minY = Math.min(...yValues);
    const maxY = Math.max(...yValues);
    const minZ = Math.min(...zValues);
    const maxZ = Math.max(...zValues);

    return list.map((point) => {
      const x = useX ? normalize(point.metrics[xCode] ?? 0, minX, maxX) : hashToJitter(`${point.id}:x`);
      const y = useY ? normalize(point.metrics[yCode] ?? 0, minY, maxY) : hashToJitter(`${point.id}:y`);
      const z = useZ ? normalize(point.metrics[zCode] ?? 0, minZ, maxZ) : hashToJitter(`${point.id}:z`);
      return { ...point, x, y, z };
    });
  }

  // ───────────────────────────────────────────────────────────
  // bbox/camera
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

    const distance = Math.max(8, maxSpan * 1.55);
    camera.position.set(center.x + distance * 0.55, center.y + distance * 0.45, center.z + distance);
    controls.target.copy(center);
    controls.update();
  }

  // ───────────────────────────────────────────────────────────
  // labels/wireframe (corner 3 planes)
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

    const k = fieldKind(metricCode);
    if (k === 'date') {
      const d = new Date(maxValue);
      if (Number.isFinite(d.getTime())) {
        const dd = String(d.getDate()).padStart(2, '0');
        const mm = String(d.getMonth() + 1).padStart(2, '0');
        const yy = d.getFullYear();
        return `${dd}.${mm}.${yy}`;
      }
      return 'нет данных';
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

  function disposeCornerFrame(): void {
    if (!cornerFrame) return;
    planeGroup.remove(cornerFrame);
    cornerFrame.geometry.dispose();
    cornerFrame.material.dispose();
    cornerFrame = null;
  }

  function disposeAxisLabels(): void {
    for (const l of axisLabels) edgeLabelGroup.remove(l);
    axisLabels = [];
  }

  function createAxisLabel(text: string, isMax = false): CSS2DObject {
    const el = document.createElement('div');

    el.style.pointerEvents = 'none';
    el.style.userSelect = 'none';
    el.style.whiteSpace = 'nowrap';
    el.style.fontFamily = 'Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial';
    el.style.fontSize = '12px';
    el.style.fontWeight = '700';
    el.style.padding = '4px 8px';
    el.style.borderRadius = '10px';
    el.style.boxShadow = '0 8px 24px rgba(15, 23, 42, 0.12)';
    el.style.background = 'rgba(255, 255, 255, 0.92)';
    el.style.border = '1px solid rgba(226, 232, 240, 0.95)';
    el.style.color = 'rgba(15, 23, 42, 0.92)';

    if (isMax) {
      el.style.color = 'rgba(30, 64, 175, 0.98)';
      el.style.borderColor = 'rgba(191, 219, 254, 0.98)';
      el.style.background = 'rgba(239, 246, 255, 0.95)';
    }

    el.textContent = text;
    return new CSS2DObject(el);
  }

  function rebuildPlanes(list: SpacePoint[]): void {
    planeGroup.clear();
    disposeCornerFrame();
    disposeAxisLabels();

    const bbox = normalizeBBox(list);

    const A = new THREE.Vector3(bbox.minX, bbox.minY, bbox.minZ);
    const Bx = new THREE.Vector3(bbox.maxX, bbox.minY, bbox.minZ);
    const By = new THREE.Vector3(bbox.minX, bbox.maxY, bbox.minZ);
    const Bz = new THREE.Vector3(bbox.minX, bbox.minY, bbox.maxZ);

    const Cxy = new THREE.Vector3(bbox.maxX, bbox.maxY, bbox.minZ);
    const Cxz = new THREE.Vector3(bbox.maxX, bbox.minY, bbox.maxZ);
    const Cyz = new THREE.Vector3(bbox.minX, bbox.maxY, bbox.maxZ);

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

    const mat = new THREE.LineBasicMaterial({ color: new THREE.Color(edgeColor) });
    cornerFrame = new THREE.LineSegments(geom, mat);
    planeGroup.add(cornerFrame);

    const xName = axisX ? fieldName(axisX) : '—';
    const yName = axisY ? fieldName(axisY) : '—';
    const zName = axisZ ? fieldName(axisZ) : '—';

    const midX = A.clone().lerp(Bx, 0.5);
    const midY = A.clone().lerp(By, 0.5);
    const midZ = A.clone().lerp(Bz, 0.5);

    const lx = createAxisLabel(xName);
    lx.position.copy(midX);

    const ly = createAxisLabel(yName);
    ly.position.copy(midY);

    const lz = createAxisLabel(zName);
    lz.position.copy(midZ);

    const xMax = axisX ? calcMax(list, axisX) : NaN;
    const yMax = axisY ? calcMax(list, axisY) : NaN;
    const zMax = axisZ ? calcMax(list, axisZ) : NaN;

    const mx = createAxisLabel(formatValueByMetric(axisX, xMax), true);
    mx.position.copy(Bx);

    const my = createAxisLabel(formatValueByMetric(axisY, yMax), true);
    my.position.copy(By);

    const mz = createAxisLabel(formatValueByMetric(axisZ, zMax), true);
    mz.position.copy(Bz);

    axisLabels = [lx, ly, lz, mx, my, mz];
    edgeLabelGroup.add(...axisLabels);
  }

  function applyVisual(): void {
    if (!scene) return;
    scene.background = new THREE.Color(bgColor);
    if (cornerFrame) {
      cornerFrame.material.color = new THREE.Color(edgeColor);
      cornerFrame.material.needsUpdate = true;
    }
  }

  $: applyVisual();

  // ───────────────────────────────────────────────────────────
  // 3D init/render
  function init3d(): void {
    const width = container3d.clientWidth;
    const height = container3d.clientHeight || 720;

    scene = new THREE.Scene();
    scene.background = new THREE.Color(bgColor);

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

    scene.add(planeGroup);
    scene.add(edgeLabelGroup);

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

  function rebuildScene(sourcePoints: SpacePoint[], textFields: string[]): void {
    if (!scene) return;

    clearMesh(circleMesh);
    clearMesh(diamondMesh);

    const renderablePoints = sanitizePoints(sourcePoints);
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
        circleMesh!.setMatrixAt(idx, o.matrix);
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
        diamondMesh!.setMatrixAt(idx, o.matrix);
      });
      scene.add(diamondMesh);
    }

    if (textFields.length === 0) {
      debugRenderedCount = 0;
      rebuildPlanes([]);
      fitCameraToPoints([]);
      return;
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
        `Выручка: ${Math.round(point.metrics.revenue).toLocaleString('ru-RU')} ₽`,
        `Расход: ${Math.round(point.metrics.spend).toLocaleString('ru-RU')} ₽`,
        `ДРР: ${point.metrics.drr.toFixed(2)}% · ROI: ${point.metrics.roi.toFixed(2)}`
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
    const height = container3d.clientHeight || 720;
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

  // ───────────────────────────────────────────────────────────
  // add/remove fields
  function removeField(code: string): void {
    if (selectedTextFields.includes(code)) {
      selectedTextFields = selectedTextFields.filter((x) => x !== code);
      return;
    }
    if (selectedCoordFields.includes(code)) {
      selectedCoordFields = selectedCoordFields.filter((x) => x !== code);
      ensureAxisFromCoords();
      return;
    }
  }

  function addField(code: string): void {
    const k = fieldKind(code);
    if (!k) return;

    if (k === 'text') {
      if (selectedTextFields.includes(code)) return;
      selectedTextFields = [...selectedTextFields, code];
      return;
    }

    if (k === 'number' || k === 'date') {
      if (selectedCoordFields.includes(code)) return;
      if (coordLimitReached()) return;
      selectedCoordFields = [...selectedCoordFields, code];
      ensureAxisFromCoords();
      return;
    }
  }

  // ───────────────────────────────────────────────────────────
  // visual menu actions
  function resetVisualDefaults(): void {
    bgColor = DEFAULT_BG;
    edgeColor = DEFAULT_EDGE;
    selectedVisualSchemeId = '';
    persistCurrentVisual();
    rebuildPlanes(sanitizePoints(points));
  }

  function persistCurrentVisual(): void {
    localStorage.setItem(STORAGE_VISUAL_CURRENT, JSON.stringify({ bgColor, edgeColor }));
  }

  function openSaveVisualModal(): void {
    saveVisualName = '';
    saveVisualModalOpen = true;
  }

  function confirmSaveVisual(): void {
    const name = saveVisualName.trim();
    if (!name) return;

    const item: VisualScheme = {
      id: `${Date.now()}-${Math.random()}`,
      name,
      value: { bgColor, edgeColor }
    };

    visualSchemes = [item, ...visualSchemes].slice(0, 30);
    selectedVisualSchemeId = item.id;
    localStorage.setItem(STORAGE_VISUALS, JSON.stringify(visualSchemes));
    persistCurrentVisual();
    saveVisualModalOpen = false;
  }

  function loadVisualScheme(): void {
    const found = visualSchemes.find((s) => s.id === selectedVisualSchemeId);
    if (!found) return;
    bgColor = found.value.bgColor;
    edgeColor = found.value.edgeColor;
    persistCurrentVisual();
    rebuildPlanes(sanitizePoints(points));
  }

  // ───────────────────────────────────────────────────────────
  // dataset presets actions
  function openSaveDatasetModal(): void {
    saveDatasetName = '';
    saveDatasetModalOpen = true;
  }

  function confirmSaveDataset(): void {
    const name = saveDatasetName.trim();
    if (!name) return;

    const item: DatasetPreset = {
      id: `${Date.now()}-${Math.random()}`,
      name,
      value: { selectedTextFields, selectedCoordFields }
    };

    datasetPresets = [item, ...datasetPresets].slice(0, 30);
    selectedDatasetPresetId = item.id;
    localStorage.setItem(STORAGE_DATASETS, JSON.stringify(datasetPresets));
    saveDatasetModalOpen = false;
  }

  function loadDatasetPreset(): void {
    const found = datasetPresets.find((d) => d.id === selectedDatasetPresetId);
    if (!found) return;

    selectedTextFields = found.value.selectedTextFields ?? [];
    selectedCoordFields = (found.value.selectedCoordFields ?? []).slice(0, 3);
    ensureAxisFromCoords();
    resetView();
  }

  function resetDatasetDefaults(): void {
    selectedDatasetPresetId = '';
    selectedTextFields = ['sku', 'campaign_id'];
    selectedCoordFields = ['revenue', 'orders', 'spend'];
    ensureAxisFromCoords();
    resetView();
  }

  // ───────────────────────────────────────────────────────────
  // react to color changes: persist + update lines
  $: if (typeof window !== 'undefined') {
    persistCurrentVisual();
    applyVisual();
    if (cornerFrame) {
      cornerFrame.material.color = new THREE.Color(edgeColor);
      cornerFrame.material.needsUpdate = true;
    }
  }

  // ───────────────────────────────────────────────────────────
  // mount/unmount
  onMount(() => {
    ensureAxisFromCoords();

    // load presets
    try {
      const rawD = localStorage.getItem(STORAGE_DATASETS);
      datasetPresets = rawD ? JSON.parse(rawD) : [];
    } catch {
      datasetPresets = [];
    }

    try {
      const rawV = localStorage.getItem(STORAGE_VISUALS);
      visualSchemes = rawV ? JSON.parse(rawV) : [];
    } catch {
      visualSchemes = [];
    }

    // load current visual (if any)
    try {
      const rawC = localStorage.getItem(STORAGE_VISUAL_CURRENT);
      const v = rawC ? (JSON.parse(rawC) as any) : null;
      if (v?.bgColor) bgColor = v.bgColor;
      if (v?.edgeColor) edgeColor = v.edgeColor;
    } catch {
      // ignore
    }

    init3d();
    window.addEventListener('resize', onResize);
    window.addEventListener('click', onWindowClick);
  });

  onDestroy(() => {
    unsub();
    cancelAnimationFrame(anim);

    window.removeEventListener('resize', onResize);
    window.removeEventListener('click', onWindowClick);

    disposeAxisLabels();
    disposeCornerFrame();

    renderer?.domElement?.removeEventListener('mousemove', onMove3d);

    renderer?.dispose();
    controls?.dispose();
    labelRenderer?.domElement?.remove();
  });
</script>

<section class="graph-root">
  <div class="stage">
    <div bind:this={container3d} class="scene"></div>

    <div class="overlay">
      <!-- LEFT: display settings -->
      <div class="left">
        <button class="display-btn" on:click={() => (displayMenuOpen = !displayMenuOpen)}>
          Настройка отображения
        </button>

        {#if displayMenuOpen}
          <div class="display-menu">
            <div class="menu-title">Визуал</div>

            <div class="row">
              <label>Цвет фона</label>
              <input type="color" bind:value={bgColor} />
            </div>

            <div class="row">
              <label>Цвет рёбер</label>
              <input type="color" bind:value={edgeColor} />
            </div>

            <div class="row-buttons">
              <button class="btn ghost" on:click={resetVisualDefaults}>Сбросить визуал</button>
              <button class="btn" on:click={openSaveVisualModal}>Сохранить схему</button>
            </div>

            <div class="row">
              <label>Визуальная схема</label>
              <select bind:value={selectedVisualSchemeId} on:change={loadVisualScheme}>
                <option value="">Выберите схему</option>
                {#each visualSchemes as s}
                  <option value={s.id}>{s.name}</option>
                {/each}
              </select>
            </div>

            <div class="divider"></div>

            <div class="menu-title">Набор данных</div>

            <div class="row-buttons">
              <button class="btn ghost" on:click={resetDatasetDefaults}>Заводские поля</button>
              <button class="btn" on:click={openSaveDatasetModal}>Сохранить набор</button>
            </div>

            <div class="row">
              <label>Набор</label>
              <select bind:value={selectedDatasetPresetId} on:change={loadDatasetPreset}>
                <option value="">Выберите набор</option>
                {#each datasetPresets as d}
                  <option value={d.id}>{d.name}</option>
                {/each}
              </select>
            </div>

            <div class="row-buttons">
              <button class="btn ghost" on:click={resetView}>Сбросить камеру</button>
            </div>
          </div>
        {/if}
      </div>

      <!-- RIGHT: fields -->
      <div class="right">
        <button class="add-btn" on:click={() => (addMenuOpen = !addMenuOpen)}>
          + Добавить поле
        </button>

        {#if addMenuOpen}
          <div class="add-menu">
            <div class="menu-title">Точки (текст)</div>
            <div class="list">
              {#each allowedTextFields() as f}
                <button class="item" on:click={() => (addField(f.code), (addMenuOpen = false))}>{f.name}</button>
              {/each}
              {#if allowedTextFields().length === 0}
                <div class="empty">Нет доступных</div>
              {/if}
            </div>

            <div class="menu-title">
              Координаты (числа/даты)
              {#if coordLimitReached()}<span class="limit"> · максимум 3</span>{/if}
            </div>

            <div class="list">
              {#each allowedCoordFields() as f}
                <button class="item" on:click={() => (addField(f.code), (addMenuOpen = false))}>{f.name}</button>
              {/each}
              {#if allowedCoordFields().length === 0}
                <div class="empty">{coordLimitReached() ? 'Лимит координат достигнут' : 'Нет доступных'}</div>
              {/if}
            </div>
          </div>
        {/if}

        <div class="chips">
          {#each selectedTextFields as code}
            <span class="chip text">
              {fieldName(code)}
              <button class="x" on:click={() => removeField(code)}>×</button>
            </span>
          {/each}

          {#each selectedCoordFields as code}
            <span class="chip coord">
              {fieldName(code)}
              <button class="x" on:click={() => removeField(code)}>×</button>
            </span>
          {/each}
        </div>

        <div class="mini">
          Точек: {points.length} ·
          На графе: {debugRenderedCount} ·
          Оси: {axisX ? fieldName(axisX) : '—'} / {axisY ? fieldName(axisY) : '—'} / {axisZ ? fieldName(axisZ) : '—'}
        </div>
      </div>
    </div>

    {#if tooltip.visible}
      <div class="tooltip" style={`left:${tooltip.x}px;top:${tooltip.y}px`}>
        {#each tooltip.lines as line}<div>{line}</div>{/each}
      </div>
    {/if}

    <!-- Save visual modal -->
    {#if saveVisualModalOpen}
      <div class="modal-backdrop" on:click={() => (saveVisualModalOpen = false)}>
        <div class="modal" on:click|stopPropagation>
          <div class="modal-title">Сохранить визуальную схему</div>
          <input class="modal-input" bind:value={saveVisualName} placeholder="Например: Тёмные рёбра" />
          <div class="modal-actions">
            <button class="btn ghost" on:click={() => (saveVisualModalOpen = false)}>Отмена</button>
            <button class="btn" on:click={confirmSaveVisual} disabled={!saveVisualName.trim()}>Сохранить</button>
          </div>
        </div>
      </div>
    {/if}

    <!-- Save dataset modal -->
    {#if saveDatasetModalOpen}
      <div class="modal-backdrop" on:click={() => (saveDatasetModalOpen = false)}>
        <div class="modal" on:click|stopPropagation>
          <div class="modal-title">Сохранить набор данных</div>
          <input class="modal-input" bind:value={saveDatasetName} placeholder="Например: SKU + Выручка/Заказы/Расход" />
          <div class="modal-actions">
            <button class="btn ghost" on:click={() => (saveDatasetModalOpen = false)}>Отмена</button>
            <button class="btn" on:click={confirmSaveDataset} disabled={!saveDatasetName.trim()}>Сохранить</button>
          </div>
        </div>
      </div>
    {/if}
  </div>
</section>

<style>
  .graph-root {
    width: 100%;
  }

  /* граф на всё пространство */
  .stage {
    position: relative;
    width: 100%;
    height: 720px;
    border-radius: 18px;
    background: #f8fbff;
    overflow: hidden;
  }

  .scene {
    position: absolute;
    inset: 0;
  }

  .overlay {
    position: absolute;
    inset: 14px;
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 12px;
    pointer-events: none;
  }

  .left,
  .right {
    pointer-events: auto;
    display: flex;
    flex-direction: column;
    gap: 10px;
    max-width: 420px;
  }

  /* buttons */
  .display-btn,
  .add-btn,
  .btn {
    border: 1px solid rgba(226, 232, 240, 0.95);
    background: rgba(255, 255, 255, 0.92);
    border-radius: 14px;
    padding: 10px 12px;
    font-size: 13px;
    font-weight: 800;
    color: rgba(15, 23, 42, 0.92);
    cursor: pointer;
    box-shadow: 0 10px 30px rgba(15, 23, 42, 0.08);
  }

  .btn.ghost {
    background: rgba(248, 250, 252, 0.92);
    color: rgba(51, 65, 85, 0.92);
  }

  .btn:disabled {
    opacity: 0.55;
    cursor: not-allowed;
  }

  /* menus */
  .display-menu,
  .add-menu {
    width: 360px;
    max-height: 520px;
    overflow: auto;
    border-radius: 16px;
    background: rgba(255, 255, 255, 0.96);
    border: 1px solid rgba(226, 232, 240, 0.95);
    box-shadow: 0 18px 45px rgba(15, 23, 42, 0.14);
    padding: 10px;
  }

  .menu-title {
    font-size: 12px;
    font-weight: 900;
    color: rgba(51, 65, 85, 0.95);
    margin: 8px 0 6px;
    display: flex;
    align-items: center;
    gap: 6px;
  }

  .divider {
    height: 1px;
    background: rgba(226, 232, 240, 0.95);
    margin: 10px 0;
  }

  .row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    padding: 6px 0;
  }

  .row label {
    font-size: 12px;
    font-weight: 800;
    color: rgba(51, 65, 85, 0.95);
  }

  .row input[type='color'] {
    width: 44px;
    height: 30px;
    padding: 0;
    border: none;
    background: transparent;
    cursor: pointer;
  }

  select {
    width: 100%;
    border: 1px solid rgba(226, 232, 240, 0.95);
    background: rgba(248, 250, 252, 0.92);
    border-radius: 12px;
    padding: 8px 10px;
    font-size: 12px;
    font-weight: 700;
    color: rgba(15, 23, 42, 0.92);
    outline: none;
  }

  .row-buttons {
    display: flex;
    gap: 8px;
    padding: 6px 0;
  }

  .row-buttons .btn {
    flex: 1;
    padding: 9px 10px;
    font-weight: 900;
  }

  /* add list */
  .list {
    display: flex;
    flex-direction: column;
    gap: 6px;
    margin-bottom: 8px;
  }

  .item {
    text-align: left;
    border: 1px solid rgba(226, 232, 240, 0.95);
    background: rgba(248, 250, 252, 0.95);
    border-radius: 12px;
    padding: 8px 10px;
    font-size: 12px;
    font-weight: 700;
    color: rgba(15, 23, 42, 0.92);
    cursor: pointer;
  }

  .empty {
    font-size: 12px;
    color: rgba(100, 116, 139, 0.95);
    padding: 8px 10px;
  }

  .limit {
    font-weight: 900;
    color: rgba(30, 64, 175, 0.95);
  }

  /* chips */
  .chips {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    justify-content: flex-end;
  }

  .chip {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    border-radius: 999px;
    padding: 6px 10px;
    font-size: 12px;
    font-weight: 800;
    border: 1px solid rgba(226, 232, 240, 0.95);
    background: rgba(255, 255, 255, 0.92);
    color: rgba(15, 23, 42, 0.9);
    box-shadow: 0 10px 30px rgba(15, 23, 42, 0.06);
  }

  .chip.text {
    background: rgba(240, 253, 244, 0.92);
    border-color: rgba(187, 247, 208, 0.95);
  }

  .chip.coord {
    background: rgba(239, 246, 255, 0.92);
    border-color: rgba(191, 219, 254, 0.95);
  }

  .x {
    border: none;
    background: transparent;
    cursor: pointer;
    font-size: 14px;
    line-height: 1;
    padding: 0;
    color: rgba(51, 65, 85, 0.9);
  }

  .mini {
    align-self: flex-end;
    font-size: 12px;
    color: rgba(71, 85, 105, 0.92);
    background: rgba(255, 255, 255, 0.7);
    border: 1px solid rgba(226, 232, 240, 0.85);
    border-radius: 12px;
    padding: 8px 10px;
    box-shadow: 0 10px 30px rgba(15, 23, 42, 0.06);
  }

  /* tooltip */
  .tooltip {
    position: absolute;
    pointer-events: none;
    background: rgba(15, 23, 42, 0.94);
    color: #f8fafc;
    font-size: 12px;
    padding: 10px;
    border-radius: 12px;
    max-width: 360px;
    z-index: 30;
  }

  /* modal */
  .modal-backdrop {
    position: absolute;
    inset: 0;
    background: rgba(15, 23, 42, 0.24);
    display: grid;
    place-items: center;
    z-index: 50;
  }

  .modal {
    width: min(520px, calc(100% - 24px));
    background: rgba(255, 255, 255, 0.98);
    border: 1px solid rgba(226, 232, 240, 0.95);
    border-radius: 18px;
    box-shadow: 0 24px 70px rgba(15, 23, 42, 0.22);
    padding: 14px;
  }

  .modal-title {
    font-size: 14px;
    font-weight: 900;
    color: rgba(15, 23, 42, 0.92);
    margin-bottom: 10px;
  }

  .modal-input {
    width: 100%;
    border: 1px solid rgba(226, 232, 240, 0.95);
    border-radius: 14px;
    padding: 10px 12px;
    font-size: 13px;
    font-weight: 800;
    outline: none;
    background: rgba(248, 250, 252, 0.92);
  }

  .modal-actions {
    margin-top: 12px;
    display: flex;
    gap: 10px;
    justify-content: flex-end;
  }
</style>
