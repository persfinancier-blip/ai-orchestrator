<script lang="ts">
  /**
   * GraphPanel.svelte (полная рабочая версия)
   *
   * Что делает:
   * - Берёт витрину showcaseStore (rows + fields)
   * - Фильтрует строки по периоду/фильтрам/поиску
   * - Группирует по выбранным текстовым полям (sku, campaign_id и т.д.)
   * - Считает метрики и строит точки в 3D (THREE.js)
   * - Рисует 3 плоскости XY/XZ/YZ (опционально)
   * - Показывает подсказку при наведении
   * - Рисует подписи рёбер куба (ось X / Y / Z) + 0—MAX
   */

  import { onDestroy, onMount } from 'svelte';
  import * as THREE from 'three';
  import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
  import { CSS2DRenderer, CSS2DObject } from 'three/examples/jsm/renderers/CSS2DRenderer.js';
  import { get } from 'svelte/store';
  import { showcaseStore, fieldName, type DatasetId, type ShowcaseField, type ShowcaseRow } from '../data/showcaseStore';

  // -----------------------------
  // Типы UI фильтров
  // -----------------------------
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

  // -----------------------------
  // Точка в пространстве
  // -----------------------------
  type SpacePoint = {
    id: string;                  // уникальный id (например "sku:12345")
    label: string;               // подпись (например "12345")
    sourceField: string;         // по какому полю создана (sku/campaign_id/...)
    metrics: Record<string, number>; // все метрики (revenue/spend/drr/roi + любые number-поля)
    x: number;
    y: number;
    z: number;
  };

  const STORAGE_KEY = 'desk-space-settings-v2';

  // -----------------------------
  // Состояние панели
  // -----------------------------
  let activeLayers: DatasetId[] = ['sales_fact', 'ads'];
  let selectedEntityFields: string[] = ['sku', 'campaign_id'];

  let axisX = '';
  let axisY = '';
  let axisZ = '';

  let showPlanes = true;

  let period: PeriodMode = '30 дней';
  let fromDate = '';
  let toDate = '';
  let search = '';
  let filters: PanelFilter[] = [];

  let panelSettingsList: Array<{ id: string; name: string; value: unknown }> = [];
  let selectedSettingId = '';

  // -----------------------------
  // Данные витрины
  // -----------------------------
  let showcase = get(showcaseStore);
  const unsub = showcaseStore.subscribe((value) => {
    showcase = value;
    ensureDefaults();
  });

  // -----------------------------
  // DOM
  // -----------------------------
  let container3d: HTMLDivElement;
  let searchInput: HTMLInputElement;

  // -----------------------------
  // Tooltip
  // -----------------------------
  let tooltip = { visible: false, x: 0, y: 0, lines: [] as string[] };

  // -----------------------------
  // Точки и debug
  // -----------------------------
  let points: SpacePoint[] = [];
  let debugCalculatedCount = 0;
  let debugRenderedCount = 0;
  let debugBBox = '—';

  type BBox = { minX: number; maxX: number; minY: number; maxY: number; minZ: number; maxZ: number };

  // -----------------------------
  // THREE.js сущности
  // -----------------------------
  let renderer: THREE.WebGLRenderer;
  let scene: THREE.Scene;
  let camera: THREE.PerspectiveCamera;
  let controls: OrbitControls;

  let circleMesh: THREE.InstancedMesh | undefined;
  let diamondMesh: THREE.InstancedMesh | undefined;

  // Оси/сетка сейчас НЕ добавляем (по вашей просьбе).
  // Оставлю переменную, чтобы не ломать будущие правки.
  let axesHelper: THREE.AxesHelper | undefined;

  let labelRenderer: CSS2DRenderer | undefined;

  // Плоскости XY/XZ/YZ
  const planeGroup = new THREE.Group();

  // Для подсветки / tooltip
  let circlePoints: SpacePoint[] = [];
  let diamondPoints: SpacePoint[] = [];

  // Анимация
  let anim = 0;

  // Raycast
  const raycaster = new THREE.Raycaster();
  const ndc = new THREE.Vector2();

  // Подписи рёбер куба (спрайты)
  let edgeLabelGroup: THREE.Group | undefined;
  let xLabelSprite: THREE.Sprite | undefined;
  let yLabelSprite: THREE.Sprite | undefined;
  let zLabelSprite: THREE.Sprite | undefined;

  // Куб (размер в мировых координатах для позиционирования подписей)
  const CUBE_SIZE = 240;

  // =============================
  // 1) Выбор доступных полей
  // =============================
  function ensureDefaults(): void {
    const numberFields = availableFields('number', 'axis');
    if (!axisX && numberFields[0]) axisX = numberFields[0].code;
    if (!axisY && numberFields[1]) axisY = numberFields[1].code;
    if (!axisZ && numberFields[2]) axisZ = numberFields[2].code;
  }

  function availableFields(kind: ShowcaseField['kind'], role: 'entity' | 'axis' | 'filter'): ShowcaseField[] {
    return showcase.fields.filter(
      (field) => field.kind === kind && field.roles.includes(role) && field.datasetIds.some((id) => activeLayers.includes(id))
    );
  }

  // Реактивные списки для UI
  $: textFields = availableFields('text', 'entity');
  $: axisFields = availableFields('number', 'axis');
  $: dateFields = availableFields('date', 'filter');
  $: textFilterFields = availableFields('text', 'filter');
  $: numberFilterFields = availableFields('number', 'filter');

  // Реактивные данные + пересчёт точек
  $: filteredRows = applyRowsFilter(showcase.rows);
  $: points = buildPoints(filteredRows, selectedEntityFields, axisX, axisY, axisZ);
  $: rebuildScene(points, selectedEntityFields);

  // =============================
  // 2) Фильтрация строк витрины
  // =============================
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
      return selectedEntityFields.some((field) =>
        String((row as Record<string, unknown>)[field] ?? '').toLowerCase().includes(q)
      );
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

    // date
    if (filter.filterType === 'date') {
      const v = String(value ?? '');
      if (!filter.valueA) return true;
      return filter.operator === 'с' ? v >= filter.valueA : v <= filter.valueA;
    }

    // text
    if (filter.filterType === 'text') {
      const v = String(value ?? '').toLowerCase();
      const q = filter.valueA.toLowerCase();
      if (filter.operator === 'равно') return v === q;
      if (filter.operator === 'не равно') return v !== q;
      if (filter.operator === 'содержит') return v.includes(q);
      if (filter.operator === 'не содержит') return !v.includes(q);
      return q
        .split(',')
        .map((i) => i.trim())
        .filter(Boolean)
        .includes(v);
    }

    // number
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

  // =============================
  // 3) Построение точек (группировка + метрики)
  // =============================
  function buildPoints(rows: ShowcaseRow[], entityFields: string[], xCode: string, yCode: string, zCode: string): SpacePoint[] {
    if (!entityFields.length) return [];

    const result: SpacePoint[] = [];

    // какие числовые поля вообще есть в витрине
    const numberCodes = showcase.fields.filter((f) => f.kind === 'number').map((f) => f.code);

    // Для каждого выбранного поля "сущности" создаём отдельный набор точек
    entityFields.forEach((entityField) => {
      // группировка строк по значению entityField
      const groups = new Map<string, ShowcaseRow[]>();

      rows.forEach((row) => {
        const key = String((row as Record<string, unknown>)[entityField] ?? '').trim();
        if (!key) return;
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key)?.push(row);
      });

      // по каждой группе делаем точку
      groups.forEach((groupRows, key) => {
        const metrics: Record<string, number> = {};

        // средние значения по всем number-полям
        numberCodes.forEach((code) => {
          metrics[code] =
            groupRows.reduce((sum, row) => sum + Number((row as Record<string, unknown>)[code] ?? 0), 0) /
            Math.max(groupRows.length, 1);
        });

        // агрегаты "как принято" (суммы)
        metrics.revenue = groupRows.reduce((sum, row) => sum + (row as any).revenue, 0);
        metrics.spend = groupRows.reduce((sum, row) => sum + (row as any).spend, 0);
        metrics.orders = groupRows.reduce((sum, row) => sum + (row as any).orders, 0);

        // производные метрики
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

    const coords = projectPoints(result, xCode, yCode, zCode);

    // debug
    const uniqueTotal = entityFields.reduce(
      (sum, field) => sum + new Set(rows.map((r) => String((r as Record<string, unknown>)[field] ?? '')).filter(Boolean)).size,
      0
    );
    console.log('[Пространство] пересчет', {
      поля: entityFields,
      уникальных: uniqueTotal,
      первыеТочки: coords.slice(0, 3).map((p) => ({ id: p.id, x: p.x, y: p.y, z: p.z })),
    });

    return coords;
  }

  // =============================
  // 4) Проекция в координаты X/Y/Z
  // =============================
  function projectPoints(list: SpacePoint[], xCode: string, yCode: string, zCode: string): SpacePoint[] {
    if (!list.length) return [];

    const useX = !!xCode && axisFields.some((f) => f.code === xCode);
    const useY = !!yCode && axisFields.some((f) => f.code === yCode);
    const useZ = !!zCode && axisFields.some((f) => f.code === zCode);

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
      // Если ось не выбрана — рандомный "джиттер", чтобы точки не слипались
      const x = useX ? normalize(point.metrics[xCode] ?? 0, minX, maxX) : hashToJitter(`${point.id}:x`);
      const y = useY ? normalize(point.metrics[yCode] ?? 0, minY, maxY) : hashToJitter(`${point.id}:y`);
      const z = useZ ? normalize(point.metrics[zCode] ?? 0, minZ, maxZ) : hashToJitter(`${point.id}:z`);
      return { ...point, x, y, z };
    });
  }

  // Нормализация значений в диапазон ~[-90..+90] (180 ширина)
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
      z: sanitizeCoord(point.z, point.id, 'z') + hashToJitter(`${point.id}:z`) * 0.05,
    }));
  }

  // =============================
  // 5) BBox + камера
  // =============================
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

  function normalizeBBox(list: SpacePoint[]): BBox {
    if (!list.length) return { minX: -1, maxX: 1, minY: -1, maxY: 1, minZ: -1, maxZ: 1 };

    const bbox = buildBBox(list);
    const spanX = bbox.maxX - bbox.minX;
    const spanY = bbox.maxY - bbox.minY;
    const spanZ = bbox.maxZ - bbox.minZ;

    // Если всё в одной точке — делаем "рамку"
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
    debugBBox = `x ${bbox.minX.toFixed(2)}..${bbox.maxX.toFixed(2)} | y ${bbox.minY.toFixed(2)}..${bbox.maxY.toFixed(
      2
    )} | z ${bbox.minZ.toFixed(2)}..${bbox.maxZ.toFixed(2)}`;

    const center = new THREE.Vector3((bbox.minX + bbox.maxX) / 2, (bbox.minY + bbox.maxY) / 2, (bbox.minZ + bbox.maxZ) / 2);

    const spanX = Math.max(0.001, bbox.maxX - bbox.minX);
    const spanY = Math.max(0.001, bbox.maxY - bbox.minY);
    const spanZ = Math.max(0.001, bbox.maxZ - bbox.minZ);
    const maxSpan = Math.max(spanX, spanY, spanZ);

    // если всё слишком скучено
    if (maxSpan < 0.5) {
      camera.position.set(center.x + 1.5, center.y + 1.5, center.z + 5);
      controls.target.copy(center);
      controls.update();
      return;
    }

    // типовая дистанция
    const distance = Math.max(8, maxSpan * 1.55);
    camera.position.set(center.x + distance * 0.55, center.y + distance * 0.45, center.z + distance);
    controls.target.copy(center);
    controls.update();
  }

  // =============================
  // 6) Плоскости XY/XZ/YZ
  // =============================
  function makePlaneLabel(text: string, x: number, y: number, z: number): CSS2DObject {
    const el = document.createElement('div');
    el.className = 'plane-label';
    el.textContent = text;
    const obj = new CSS2DObject(el);
    obj.position.set(x, y, z);
    return obj;
  }

  function rebuildPlanes(list: SpacePoint[]): void {
    planeGroup.clear();
    if (!showPlanes) return;

    const bbox = normalizeBBox(list);
    const widthX = Math.max(2, bbox.maxX - bbox.minX);
    const widthY = Math.max(2, bbox.maxY - bbox.minY);
    const widthZ = Math.max(2, bbox.maxZ - bbox.minZ);

    const centerX = (bbox.minX + bbox.maxX) / 2;
    const centerY = (bbox.minY + bbox.maxY) / 2;
    const centerZ = (bbox.minZ + bbox.maxZ) / 2;

    const matXY = new THREE.MeshBasicMaterial({
      color: '#94a3b8',
      transparent: true,
      opacity: 0.1,
      side: THREE.DoubleSide,
      depthWrite: false,
    });
    const matXZ = new THREE.MeshBasicMaterial({
      color: '#9ca3af',
      transparent: true,
      opacity: 0.1,
      side: THREE.DoubleSide,
      depthWrite: false,
    });
    const matYZ = new THREE.MeshBasicMaterial({
      color: '#a5b4fc',
      transparent: true,
      opacity: 0.1,
      side: THREE.DoubleSide,
      depthWrite: false,
    });

    // XY на "дне" по Z
    const planeXY = new THREE.Mesh(new THREE.PlaneGeometry(widthX, widthY), matXY);
    planeXY.position.set(centerX, centerY, bbox.minZ);
    planeGroup.add(planeXY);

    // XZ на "дне" по Y
    const planeXZ = new THREE.Mesh(new THREE.PlaneGeometry(widthX, widthZ), matXZ);
    planeXZ.rotation.x = -Math.PI / 2;
    planeXZ.position.set(centerX, bbox.minY, centerZ);
    planeGroup.add(planeXZ);

    // YZ на "стенке" по X
    const planeYZ = new THREE.Mesh(new THREE.PlaneGeometry(widthY, widthZ), matYZ);
    planeYZ.rotation.y = Math.PI / 2;
    planeYZ.position.set(bbox.minX, centerY, centerZ);
    planeGroup.add(planeYZ);

    const xName = fieldName(axisX || '—');
    const yName = fieldName(axisY || '—');
    const zName = fieldName(axisZ || '—');

    // подписи плоскостей
    planeGroup.add(makePlaneLabel(`${xName} × ${yName}`, centerX, centerY, bbox.minZ + 0.25));
    planeGroup.add(makePlaneLabel(`${xName} × ${zName}`, centerX, bbox.minY + 0.25, centerZ));
    planeGroup.add(makePlaneLabel(`${yName} × ${zName}`, bbox.minX + 0.25, centerY, centerZ));
  }

  // =============================
  // 7) Подписи рёбер куба (Ось + 0—MAX)
  // =============================

  // Текстовый спрайт на canvas
  function makeTextSprite(text: string, { fontSize = 36, padding = 16 } = {}): THREE.Sprite {
    const canvas = document.createElement('canvas');
    const ctx = canvas.getContext('2d');
    if (!ctx) {
      // запасной вариант, но обычно ctx есть
      const mat = new THREE.SpriteMaterial({ color: '#111827' as any });
      return new THREE.Sprite(mat);
    }

    ctx.font = `${fontSize}px Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial`;
    const metrics = ctx.measureText(text);
    const w = Math.ceil(metrics.width) + padding * 2;
    const h = fontSize + padding * 2;

    canvas.width = w;
    canvas.height = h;

    // прозрачный фон
    ctx.clearRect(0, 0, w, h);

    // текст
    ctx.font = `${fontSize}px Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial`;
    ctx.fillStyle = '#1f2a37';
    ctx.textBaseline = 'middle';
    ctx.fillText(text, padding, h / 2);

    const texture = new THREE.CanvasTexture(canvas);
    texture.needsUpdate = true;

    const material = new THREE.SpriteMaterial({ map: texture, transparent: true, depthTest: false });
    const sprite = new THREE.Sprite(material);

    // размер в мировых координатах
    const scale = 0.06;
    sprite.scale.set(w * scale, h * scale, 1);

    // чтобы всегда читалось (не перекрывалось)
    sprite.renderOrder = 9999;
    return sprite;
  }

  // Максимум по метрике среди точек
  function calcMax(list: SpacePoint[], metricKey: string): number {
    if (!metricKey) return NaN;
    let max = -Infinity;
    for (const p of list) {
      const v = p?.metrics?.[metricKey];
      if (Number.isFinite(v)) max = Math.max(max, v);
    }
    return max === -Infinity ? NaN : max;
  }

  // Человекочитаемый формат
  function formatNumberHuman(n: number): string {
    if (!Number.isFinite(n)) return 'нет данных';
    const abs = Math.abs(n);
    if (abs >= 1_000_000_000) return (n / 1_000_000_000).toFixed(1).replace('.', ',') + ' млрд';
    if (abs >= 1_000_000) return (n / 1_000_000).toFixed(1).replace('.', ',') + ' млн';
    if (abs >= 1_000) return (n / 1_000).toFixed(1).replace('.', ',') + ' тыс';
    return Math.round(n).toString();
  }

  function formatValueByMetric(metricCode: string, maxValue: number): string {
    if (!Number.isFinite(maxValue)) return 'нет данных';

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

  /**
   * ВАЖНО:
   * Это обычная функция, а НЕ "updateEdgeLabels(...) { ... }" (вот это у тебя и ломало сборку!)
   */
  function updateEdgeLabels(params: {
    points: SpacePoint[];
    xMetric: string;
    yMetric: string;
    zMetric: string;
    xName: string;
    yName: string;
    zName: string;
    cubeSize: number;
  }): void {
    if (!edgeLabelGroup) return;

    const { points, xMetric, yMetric, zMetric, xName, yName, zName, cubeSize } = params;

    // очистка старых спрайтов
    edgeLabelGroup.clear();

    const xMax = calcMax(points, xMetric);
    const yMax = calcMax(points, yMetric);
    const zMax = calcMax(points, zMetric);

    const xText = xMetric ? `${xName} · 0 — ${formatValueByMetric(xMetric, xMax)}` : 'Ось X не выбрана';
    const yText = yMetric ? `${yName} · 0 — ${formatValueByMetric(yMetric, yMax)}` : 'Ось Y не выбрана';
    const zText = zMetric ? `${zName} · 0 — ${formatValueByMetric(zMetric, zMax)}` : 'Ось Z не выбрана';

    xLabelSprite = makeTextSprite(xText);
    yLabelSprite = makeTextSprite(yText);
    zLabelSprite = makeTextSprite(zText);

    const half = cubeSize / 2;

    /**
     * Позиции подписей:
     * Мы ставим их "возле" рёбер куба.
     * Куб предполагаем центр в (0,0,0).
     *
     * X: нижнее переднее ребро (X)
     * Y: нижнее левое ребро (Z-направление) — просто визуальная подпись "оси Y"
     * Z: правое переднее вертикальное ребро (Y-направление)
     */
    xLabelSprite.position.set(0, -half - 14, +half + 8);
    yLabelSprite.position.set(-half - 18, -half - 14, 0);
    zLabelSprite.position.set(+half + 18, 0, +half + 8);

    edgeLabelGroup.add(xLabelSprite, yLabelSprite, zLabelSprite);
  }

  // =============================
  // 8) Инициализация 3D сцены
  // =============================
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

    // Группа под подписи рёбер
    edgeLabelGroup = new THREE.Group();
    scene.add(edgeLabelGroup);

    // Управление камерой
    controls = new OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.08;

    // Свет
    scene.add(new THREE.AmbientLight('#ffffff', 0.95));
    const light = new THREE.DirectionalLight('#ffffff', 0.35);
    light.position.set(80, 120, 75);
    scene.add(light);

    // Плоскости
    scene.add(planeGroup);

    // 2D-лейблы (CSS2D) для плоскостей
    labelRenderer = new CSS2DRenderer();
    labelRenderer.setSize(width, height);
    labelRenderer.domElement.style.position = 'absolute';
    labelRenderer.domElement.style.left = '0';
    labelRenderer.domElement.style.top = '0';
    labelRenderer.domElement.style.pointerEvents = 'none';
    labelRenderer.domElement.className = 'label-layer';
    container3d.appendChild(labelRenderer.domElement);

    // Наведение
    renderer.domElement.addEventListener('mousemove', onMove3d);

    animate();
  }

  // =============================
  // 9) Рендер точек + обновление плоскостей/подписей
  // =============================
  function clearMesh(mesh?: THREE.InstancedMesh): void {
    if (!mesh) return;
    scene.remove(mesh);
    mesh.geometry.dispose();
    (mesh.material as THREE.Material).dispose();
  }

  function rebuildScene(sourcePoints: SpacePoint[], entityFields: string[]): void {
    if (!scene) return;

    clearMesh(circleMesh);
    clearMesh(diamondMesh);

    debugCalculatedCount = sourcePoints.length;

    const renderablePoints = sanitizePoints(sourcePoints);
    debugRenderedCount = renderablePoints.length;

    // Разные формы: круги = НЕ campaign_id, ромбы = campaign_id
    circlePoints = renderablePoints.filter((p) => p.sourceField !== 'campaign_id');
    diamondPoints = renderablePoints.filter((p) => p.sourceField === 'campaign_id');

    // Круги
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

    // Ромбы
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

    // Если точек нет — чистим плоскости/камеру/подписи
    if (entityFields.length === 0) {
      debugRenderedCount = 0;
      rebuildPlanes([]);
      fitCameraToPoints([]);
      updateEdgeLabels({
        points: [],
        xMetric: axisX,
        yMetric: axisY,
        zMetric: axisZ,
        xName: fieldName(axisX || '—'),
        yName: fieldName(axisY || '—'),
        zName: fieldName(axisZ || '—'),
        cubeSize: CUBE_SIZE,
      });
      return;
    }

    // Плоскости и камера
    rebuildPlanes(renderablePoints);
    fitCameraToPoints(renderablePoints);

    // Подписи рёбер куба по выбранным осям
    updateEdgeLabels({
      points: renderablePoints,
      xMetric: axisX,
      yMetric: axisY,
      zMetric: axisZ,
      xName: fieldName(axisX || '—'),
      yName: fieldName(axisY || '—'),
      zName: fieldName(axisZ || '—'),
      cubeSize: CUBE_SIZE,
    });
  }

  // =============================
  // 10) Tooltip (hover)
  // =============================
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

  // =============================
  // 11) Render loop + resize
  // =============================
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

  // =============================
  // 12) UI: слои/поля/фильтры/сейвы
  // =============================
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
      filters = [
        ...filters,
        { id, filterType: type, fieldCode: numberFilterFields[0]?.code ?? 'revenue', operator: '>', valueA: '', valueB: '' },
      ];
      return;
    }
    filters = [
      ...filters,
      { id, filterType: type, fieldCode: textFilterFields[0]?.code ?? 'sku', operator: 'содержит', valueA: '', valueB: '' },
    ];
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
    if (t === 'text')
      updateFilter(id, { filterType: t, fieldCode: textFilterFields[0]?.code ?? 'sku', operator: 'содержит', valueA: '', valueB: '' });
    if (t === 'number')
      updateFilter(id, { filterType: t, fieldCode: numberFilterFields[0]?.code ?? 'revenue', operator: '>', valueA: '', valueB: '' });
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

  // =============================
  // 13) Lifecycle
  // =============================
  onMount(() => {
    init3d();
    ensureDefaults();

    // Важно: после ensureDefaults у тебя могут поменяться axisX/Y/Z
    // rebuildScene вызовется реактивно, но можно оставить и так:
    rebuildScene(points, selectedEntityFields);

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

    // CSS2D слой
    labelRenderer?.domElement?.remove();
  });
</script>

<section class="graph-root panel">
  <div class="title-row">
    <h2>Пространство</h2>
    <div class="status">
      Точек: {points.length} · Слои: {activeLayers.map((id) => showcase.datasets.find((d) => d.id === id)?.name).join(', ')} · Оси:
      {fieldName(axisX || '—')} / {fieldName(axisY || '—')} / {fieldName(axisZ || '—')}
    </div>
  </div>

  <div class="content">
    <div class="graph-wrap">
      <div class="stage">
        <div bind:this={container3d} class="scene" />

        <div class="debug">
          <div>Выбрано полей: {selectedEntityFields.length} ({selectedEntityFields.join(', ') || '—'})</div>
          <div>Точек на графе: {debugRenderedCount}</div>
          <div>Источник: Витрина данных (rows={filteredRows.length})</div>
          <div>Группировка по: {selectedEntityFields[0] || '—'}</div>
          <div>X/Y/Z: {fieldName(axisX || '—')} / {fieldName(axisY || '—')} / {fieldName(axisZ || '—')}</div>
          <div>bbox: {debugBBox}</div>
        </div>

        {#if selectedEntityFields.length === 0}
          <div class="empty-hint">Выберите поля в ‘Точки на графе’</div>
        {/if}

        {#if tooltip.visible}
          <div class="tooltip" style={`left:${tooltip.x}px;top:${tooltip.y}px`}>
            {#each tooltip.lines as line}
              <div>{line}</div>
            {/each}
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
        <label
          >+ Добавить поле
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
        <label
          >Ось X
          <select bind:value={axisX}>
            <option value="">Не выбрано</option>
            {#each axisFields as field}<option value={field.code}>{field.name}</option>{/each}
          </select>
        </label>
        <label
          >Ось Y
          <select bind:value={axisY}>
            <option value="">Не выбрано</option>
            {#each axisFields as field}<option value={field.code}>{field.name}</option>{/each}
          </select>
        </label>
        <label
          >Ось Z
          <select bind:value={axisZ}>
            <option value="">Не выбрано</option>
            {#each axisFields as field}<option value={field.code}>{field.name}</option>{/each}
          </select>
        </label>
        <label class="toggle"><input type="checkbox" bind:checked={showPlanes} /> Плоскости</label>
      </section>

      <section class="section">
        <h4>Период и фильтры</h4>
        <small>Ограничивает данные для расчёта.</small>
        <label
          >Период
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
              <select bind:value={filter.fieldCode}>
                {#each dateFields as field}<option value={field.code}>{field.name}</option>{/each}
              </select>
              <select bind:value={filter.operator}><option>с</option><option>по</option></select>
              <input type="date" bind:value={filter.valueA} />
            {:else if filter.filterType === 'text'}
              <select bind:value={filter.fieldCode}>
                {#each textFilterFields as field}<option value={field.code}>{field.name}</option>{/each}
              </select>
              <select bind:value={filter.operator}>
                <option>равно</option><option>не равно</option><option>содержит</option><option>не содержит</option><option>в списке</option>
              </select>
              <input type="text" bind:value={filter.valueA} placeholder="Значение" />
            {:else}
              <select bind:value={filter.fieldCode}>
                {#each numberFilterFields as field}<option value={field.code}>{field.name}</option>{/each}
              </select>
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

  .scene { position:relative; width:100%; height:100%; min-height:560px; border:1px solid #e2e8f0; border-radius:16px; background:#f8fbff; overflow:hidden; }

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

  .empty-hint { position:absolute; left:50%; top:50%; transform:translate(-50%,-50%); background:#fff; border:1px dashed #94a3b8; color:#334155; border-radius:10px; padding:10px 12px; font-size:13px; }

  :global(.plane-label) { background:rgba(15,23,42,.7); color:#f8fafc; border-radius:8px; padding:3px 7px; font-size:11px; white-space:nowrap; }

  .toggle { flex-direction:row; align-items:center; gap:6px; }

  small { color:#64748b; font-size:11px; line-height:1.3; }
</style>
