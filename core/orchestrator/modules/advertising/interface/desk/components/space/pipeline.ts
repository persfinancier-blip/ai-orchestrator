import type { ShowcaseField, ShowcaseRow } from '../data/showcaseStore';
import type { BBox, PeriodMode, SpacePoint } from './types';

export function hashToJitter(input: string): number {
  let hash = 0;
  for (let i = 0; i < input.length; i += 1) hash = (hash * 31 + input.charCodeAt(i)) >>> 0;
  return ((hash % 1000) / 1000 - 0.5) * 10;
}

export function parseDateToNumber(v: unknown): number {
  const s = String(v ?? '').trim();
  if (!s) return Number.NaN;
  const t = Date.parse(s);
  return Number.isFinite(t) ? t : Number.NaN;
}

export function inPeriod(date: string, period: PeriodMode, fromDate: string, toDate: string): boolean {
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

export function applyRowsFilter(args: {
  rows: ShowcaseRow[];
  period: PeriodMode;
  fromDate: string;
  toDate: string;
  search: string;
  selectedEntityFields: string[];
}): ShowcaseRow[] {
  const { rows, period, fromDate, toDate, search, selectedEntityFields } = args;

  return rows.filter((row) => {
    const anyRow = row as any;
    if (!inPeriod(String(anyRow?.date ?? ''), period, fromDate, toDate)) return false;

    if (!search.trim()) return true;
    const q = search.trim().toLowerCase();

    return selectedEntityFields.some((field) =>
      String((row as Record<string, unknown>)[field] ?? '').toLowerCase().includes(q)
    );
  });
}

export function normalize(value: number, min: number, max: number): number {
  return ((value - min) / Math.max(max - min, 0.0001) - 0.5) * 180;
}

export function projectPoints(list: SpacePoint[], xCode: string, yCode: string, zCode: string): SpacePoint[] {
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
    const x =
      useX && Number.isFinite(point.metrics[xCode])
        ? normalize(point.metrics[xCode], minX, maxX)
        : hashToJitter(`${point.id}:x`);

    const y =
      useY && Number.isFinite(point.metrics[yCode])
        ? normalize(point.metrics[yCode], minY, maxY)
        : hashToJitter(`${point.id}:y`);

    const z =
      useZ && Number.isFinite(point.metrics[zCode])
        ? normalize(point.metrics[zCode], minZ, maxZ)
        : hashToJitter(`${point.id}:z`);

    return { ...point, x, y, z };
  });
}

function sanitizeCoord(value: number, id: string, axis: 'x' | 'y' | 'z'): number {
  if (!Number.isFinite(value)) return hashToJitter(`${id}:${axis}`) * 0.1;
  return value;
}

export function sanitizePoints(input: SpacePoint[]): SpacePoint[] {
  return input.map((point) => ({
    ...point,
    x: sanitizeCoord(point.x, point.id, 'x') + hashToJitter(`${point.id}:x`) * 0.05,
    y: sanitizeCoord(point.y, point.id, 'y') + hashToJitter(`${point.id}:y`) * 0.05,
    z: sanitizeCoord(point.z, point.id, 'z') + hashToJitter(`${point.id}:z`) * 0.05
  }));
}

export function buildBBox(points: SpacePoint[]): BBox {
  const xs = points.map((p) => p.x);
  const ys = points.map((p) => p.y);
  const zs = points.map((p) => p.z);
  return {
    minX: Math.min(...xs),
    maxX: Math.max(...xs),
    minY: Math.min(...ys),
    maxY: Math.max(...ys),
    minZ: Math.min(...zs),
    maxZ: Math.max(...zs)
  };
}

/**
 * ВАЖНО: SpaceScene (твой) вызывает normalizeBBox(bbox), поэтому оставляем именно такую сигнатуру.
 */
export function normalizeBBox(bbox: BBox): BBox {
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

/**
 * GraphPanel ожидает: calcMax(pointsRaw, axisCode) -> number
 */
export function calcMax(pointsList: SpacePoint[], metricKey: string): number {
  if (!metricKey) return Number.NaN;
  let max = -Infinity;

  for (const p of pointsList) {
    const v = p?.metrics?.[metricKey];
    if (Number.isFinite(v)) max = Math.max(max, v);
  }

  return Number.isFinite(max) ? max : Number.NaN;
}

export function formatValueByMetric(metric: string, value: number): string {
  if (!Number.isFinite(value)) return '—';
  if (metric === 'drr') return `${value.toFixed(1)}%`;
  if (metric === 'roi') return value.toFixed(2);
  if (metric.includes('date') || metric.includes('Date')) return new Date(value).toISOString().slice(0, 10);

  const abs = Math.abs(value);
  if (abs >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(2)}B`;
  if (abs >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
  if (abs >= 1_000) return `${(value / 1_000).toFixed(2)}K`;
  return value.toFixed(2);
}

// ==============================
// ✅ VOXEL LOD + HULL
// ==============================

function clamp01(v: number): number {
  return Math.min(1, Math.max(0, v));
}

/**
 * - detail=0 -> "почти по точкам"
 * - detail=1 -> size >= 2*maxSpan => все точки в 1 ячейке
 */
function voxelSizeFromDetailAndBBox(detail01: number, bbox: BBox): number {
  const d = clamp01(detail01);
  const spanX = Math.max(0.001, bbox.maxX - bbox.minX);
  const spanY = Math.max(0.001, bbox.maxY - bbox.minY);
  const spanZ = Math.max(0.001, bbox.maxZ - bbox.minZ);
  const maxSpan = Math.max(spanX, spanY, spanZ);

  const minSize = Math.max(1e-9, maxSpan / 10_000);
  const maxSize = maxSpan * 2;
  return minSize + (maxSize - minSize) * d;
}

function voxelKey(p: SpacePoint, bbox: BBox, size: number): string {
  const ix = Math.floor((p.x - bbox.minX) / size);
  const iy = Math.floor((p.y - bbox.minY) / size);
  const iz = Math.floor((p.z - bbox.minZ) / size);
  return `${ix}|${iy}|${iz}`;
}

function effectiveMinCount(detail01: number, total: number, baseMinCount: number): number {
  const d = clamp01(detail01);
  if (total <= 1) return 2;

  const noAgg = total + 1;
  if (d <= 0) return noAgg;
  if (d >= 1) return 1;

  if (d < 0.85) {
    const mid = Math.max(2, Math.round(noAgg * (1 - d) + baseMinCount * d));
    return Math.min(noAgg, mid);
  }

  const u = (d - 0.85) / 0.15;
  const mid = Math.max(2, Math.round(noAgg * (1 - d) + baseMinCount * d));
  return Math.max(1, Math.round(mid * (1 - u) + 1 * u));
}

function pickHullSamples(cellPoints: SpacePoint[], centroid: { x: number; y: number; z: number }): [number, number, number][] {
  const n = cellPoints.length;
  if (!n) return [];

  let minX = cellPoints[0], maxX = cellPoints[0];
  let minY = cellPoints[0], maxY = cellPoints[0];
  let minZ = cellPoints[0], maxZ = cellPoints[0];

  for (const p of cellPoints) {
    if (p.x < minX.x) minX = p;
    if (p.x > maxX.x) maxX = p;
    if (p.y < minY.y) minY = p;
    if (p.y > maxY.y) maxY = p;
    if (p.z < minZ.z) minZ = p;
    if (p.z > maxZ.z) maxZ = p;
  }

  const raw: [number, number, number][] = [
    [minX.x, minX.y, minX.z],
    [maxX.x, maxX.y, maxX.z],
    [minY.x, minY.y, minY.z],
    [maxY.x, maxY.y, maxY.z],
    [minZ.x, minZ.y, minZ.z],
    [maxZ.x, maxZ.y, maxZ.z]
  ];

  const target = 24;
  if (n > raw.length) {
    const stride = Math.max(1, Math.ceil(n / (target - raw.length)));
    for (let i = 0; i < n && raw.length < target; i += stride) {
      const p = cellPoints[i];
      raw.push([p.x, p.y, p.z]);
    }
  }

  const uniq: [number, number, number][] = [];
  const seen = new Set<string>();
  for (const v of raw) {
    const k = `${v[0].toFixed(6)}|${v[1].toFixed(6)}|${v[2].toFixed(6)}`;
    if (seen.has(k)) continue;
    seen.add(k);
    uniq.push(v);
  }

  if (uniq.length < 4) {
    const j = 0.001;
    uniq.push([centroid.x + j, centroid.y, centroid.z]);
    uniq.push([centroid.x, centroid.y + j, centroid.z]);
    uniq.push([centroid.x, centroid.y, centroid.z + j]);
  }

  return uniq;
}

function voxelAggregateHomogeneous(points: SpacePoint[], opts: { minCount: number; detail: number }): SpacePoint[] {
  const { minCount, detail } = opts;
  if (!points.length) return [];

  const byField = new Map<string, SpacePoint[]>();
  for (const p of points) {
    const k = String(p.sourceField ?? '');
    const arr = byField.get(k);
    if (arr) arr.push(p);
    else byField.set(k, [p]);
  }

  const out: SpacePoint[] = [];

  for (const [field, list] of byField.entries()) {
    const bbox = buildBBox(list);
    const size = voxelSizeFromDetailAndBBox(detail, bbox);
    const minCountEff = effectiveMinCount(detail, list.length, minCount);

    const grid = new Map<string, SpacePoint[]>();
    for (const p of list) {
      const k = voxelKey(p, bbox, size);
      const arr = grid.get(k);
      if (arr) arr.push(p);
      else grid.set(k, [p]);
    }

    for (const [cell, cellPoints] of grid.entries()) {
      if (cellPoints.length < minCountEff) {
        out.push(...cellPoints);
        continue;
      }

      let minX = Infinity, minY = Infinity, minZ = Infinity;
      let maxX = -Infinity, maxY = -Infinity, maxZ = -Infinity;

      let sx = 0, sy = 0, sz = 0;

      const sums: Record<string, number> = {};
      const counts: Record<string, number> = {};

      for (const p of cellPoints) {
        sx += p.x; sy += p.y; sz += p.z;

        minX = Math.min(minX, p.x); minY = Math.min(minY, p.y); minZ = Math.min(minZ, p.z);
        maxX = Math.max(maxX, p.x); maxY = Math.max(maxY, p.y); maxZ = Math.max(maxZ, p.z);

        const m = p.metrics ?? {};
        for (const k of Object.keys(m)) {
          const v = Number(m[k]);
          if (!Number.isFinite(v)) continue;
          sums[k] = (sums[k] ?? 0) + v;
          counts[k] = (counts[k] ?? 0) + 1;
        }
      }

      const n = cellPoints.length;
      const metrics: Record<string, number> = {};
      for (const k of Object.keys(sums)) {
        const c = counts[k] ?? 0;
        metrics[k] = c ? sums[k] / c : Number.NaN;
      }

      const cx = sx / n;
      const cy = sy / n;
      const cz = sz / n;

      const span = {
        x: Math.max(0.001, maxX - minX),
        y: Math.max(0.001, maxY - minY),
        z: Math.max(0.001, maxZ - minZ)
      };

      const hull = pickHullSamples(cellPoints, { x: cx, y: cy, z: cz });

      out.push({
        id: `cluster:${field}:${cell}`,
        label: `Кластер (${n})`,
        sourceField: field,
        metrics,
        x: cx,
        y: cy,
        z: cz,
        isCluster: true,
        clusterCount: n,
        span,
        hull
      });
    }
  }

  return out;
}

// ==============================
// ✅ buildPoints
// ==============================

export function buildPoints(args: {
  rows: ShowcaseRow[];
  entityFields: string[];
  axisX: string;
  axisY: string;
  axisZ: string;
  numberFields: ShowcaseField[];
  dateFields: ShowcaseField[];
  lodEnabled?: boolean;
  lodDetail?: number;
  lodMinCount?: number;
}): SpacePoint[] {
  const {
    rows,
    entityFields,
    axisX,
    axisY,
    axisZ,
    numberFields,
    dateFields,
    lodEnabled = true,
    lodDetail = 0.5,
    lodMinCount = 5
  } = args;

  if (!entityFields.length) return [];

  const numberCodes = numberFields.map((f) => f.code);
  const dateCodes = dateFields.map((f) => f.code);

  const result: SpacePoint[] = [];

  entityFields.forEach((entityField) => {
    const groups = new Map<string, ShowcaseRow[]>();

    rows.forEach((row) => {
      const key = String((row as Record<string, unknown>)[entityField] ?? '').trim();
      if (!key) return;
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key)!.push(row);
    });

    groups.forEach((groupRows, key) => {
      const metrics: Record<string, number> = {};

      for (const code of numberCodes) {
        let sum = 0;
        for (const r of groupRows) sum += Number((r as any)[code] ?? 0);
        metrics[code] = sum / Math.max(groupRows.length, 1);
      }

      for (const code of dateCodes) {
        let sum = 0;
        let cnt = 0;
        for (const r of groupRows) {
          const n = parseDateToNumber((r as any)[code]);
          if (Number.isFinite(n)) {
            sum += n;
            cnt += 1;
          }
        }
        metrics[code] = cnt ? sum / cnt : Number.NaN;
      }

      metrics.revenue = groupRows.reduce((sum, r) => sum + Number((r as any).revenue ?? 0), 0);
      metrics.spend = groupRows.reduce((sum, r) => sum + Number((r as any).spend ?? 0), 0);
      metrics.orders = groupRows.reduce((sum, r) => sum + Number((r as any).orders ?? 0), 0);
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
  });

  const projected = projectPoints(result, axisX, axisY, axisZ);
  return lodEnabled ? voxelAggregateHomogeneous(projected, { minCount: lodMinCount, detail: lodDetail }) : projected;
}
