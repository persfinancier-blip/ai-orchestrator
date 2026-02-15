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

  if (period === 'Месяц') {
    if (!fromDate) return true;
    return date.slice(0, 7) === fromDate.slice(0, 7);
  }

  if (period === 'Год') {
    if (!fromDate) return true;
    return date.slice(0, 4) === fromDate.slice(0, 4);
  }

  return true;
}

function sanitizeCoord(value: number, id: string, axis: 'x' | 'y' | 'z'): number {
  if (!Number.isFinite(value)) return hashToJitter(`${id}:${axis}`) * 0.1;
  return value;
}

/**
 * sanitizePoints
 * - НЕ добавляет jitter всем подряд.
 * - добавляет jitter только если координата невалидна ИЛИ есть полные дубликаты (x,y,z).
 */
export function sanitizePoints(input: SpacePoint[]): SpacePoint[] {
  const base = input.map((p) => ({
    ...p,
    x: sanitizeCoord(p.x, p.id, 'x'),
    y: sanitizeCoord(p.y, p.id, 'y'),
    z: sanitizeCoord(p.z, p.id, 'z')
  }));

  const keyToIndices = new Map<string, number[]>();
  for (let i = 0; i < base.length; i += 1) {
    const p = base[i];
    const k = `${p.x}|${p.y}|${p.z}`;
    const arr = keyToIndices.get(k);
    if (arr) arr.push(i);
    else keyToIndices.set(k, [i]);
  }

  // jitter only duplicates (leave unique points untouched)
  for (const indices of keyToIndices.values()) {
    if (indices.length <= 1) continue;
    for (let j = 0; j < indices.length; j += 1) {
      const idx = indices[j];
      const p = base[idx];
      const s = 0.02 + j * 0.002;
      base[idx] = {
        ...p,
        x: p.x + hashToJitter(`${p.id}:x`) * s,
        y: p.y + hashToJitter(`${p.id}:y`) * s,
        z: p.z + hashToJitter(`${p.id}:z`) * s
      };
    }
  }

  return base;
}

export function buildBBox(points: SpacePoint[]): BBox {
  let minX = Infinity;
  let minY = Infinity;
  let minZ = Infinity;
  let maxX = -Infinity;
  let maxY = -Infinity;
  let maxZ = -Infinity;

  for (const p of points) {
    if (!Number.isFinite(p.x) || !Number.isFinite(p.y) || !Number.isFinite(p.z)) continue;
    if (p.x < minX) minX = p.x;
    if (p.y < minY) minY = p.y;
    if (p.z < minZ) minZ = p.z;
    if (p.x > maxX) maxX = p.x;
    if (p.y > maxY) maxY = p.y;
    if (p.z > maxZ) maxZ = p.z;
  }

  if (!Number.isFinite(minX)) {
    return { minX: 0, minY: 0, minZ: 0, maxX: 1, maxY: 1, maxZ: 1 };
  }

  return { minX, minY, minZ, maxX, maxY, maxZ };
}

export function normalizeBBox(bbox: BBox): BBox {
  const spanX = Math.max(0.001, bbox.maxX - bbox.minX);
  const spanY = Math.max(0.001, bbox.maxY - bbox.minY);
  const spanZ = Math.max(0.001, bbox.maxZ - bbox.minZ);

  // делаем чуть “толще”, чтобы не схлопывалось в плоскость
  const pad = 0.05;
  return {
    minX: bbox.minX - spanX * pad,
    maxX: bbox.maxX + spanX * pad,
    minY: bbox.minY - spanY * pad,
    maxY: bbox.maxY + spanY * pad,
    minZ: bbox.minZ - spanZ * pad,
    maxZ: bbox.maxZ + spanZ * pad
  };
}

export function formatValueByMetric(metric: string, value: number): string {
  if (!Number.isFinite(value)) return '—';
  if (metric === 'drr') return `${value.toFixed(1)}%`;
  if (metric === 'roi') return value.toFixed(2);
  if (metric.includes('date') || metric.includes('Date')) return new Date(value).toISOString().slice(0, 10);

  if (Math.abs(value) >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
  if (Math.abs(value) >= 1_000) return `${(value / 1_000).toFixed(2)}K`;
  return value.toFixed(2);
}

function clamp01(v: number): number {
  return Math.min(1, Math.max(0, v));
}

/**
 * ✅ Гарантированный LOD:
 * - detail=0 -> не агрегируем (через minCountEff = N+1)
 * - detail=1 -> все точки попадают в одну ячейку (size >= 2*maxSpan)
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
  if (n <= 0) return [];

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

  // + семплы по ряду, чтобы hull был похож на форму, а не только 6 точек
  const target = 24;
  if (n > 6) {
    const stride = Math.max(1, Math.ceil(n / (target - raw.length)));
    for (let i = 0; i < n && raw.length < target; i += stride) {
      const p = cellPoints[i];
      raw.push([p.x, p.y, p.z]);
    }
  }

  // уникализация + защита от полностью одинаковых
  const uniq: [number, number, number][] = [];
  const seen = new Set<string>();
  for (const v of raw) {
    const k = `${v[0].toFixed(6)}|${v[1].toFixed(6)}|${v[2].toFixed(6)}`;
    if (seen.has(k)) continue;
    seen.add(k);
    uniq.push(v);
  }

  // если совсем мало уникальных — добавим 2 точки около центроида (почти нулевые), чтобы ConvexGeometry не падал
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

      let sumRevenue = 0;
      let sumSpend = 0;
      let sumOrders = 0;

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

        sumRevenue += Number(m.revenue ?? 0);
        sumSpend += Number(m.spend ?? 0);
        sumOrders += Number(m.orders ?? 0);
      }

      const n = cellPoints.length;

      const metrics: Record<string, number> = {};
      for (const k of Object.keys(sums)) {
        const c = counts[k] ?? 0;
        metrics[k] = c ? sums[k] / c : Number.NaN;
      }

      metrics.revenue = sumRevenue;
      metrics.spend = sumSpend;
      metrics.orders = sumOrders;
      metrics.drr = metrics.revenue > 0 ? (metrics.spend / metrics.revenue) * 100 : 0;
      metrics.roi = metrics.spend > 0 ? metrics.revenue / metrics.spend : 0;

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
        // @ts-expect-error: расширяем SpacePoint динамически; при желании добавь hull?: [number,number,number][] в types.ts
        hull
      });
    }
  }

  return out;
}

function projectPoints(points: SpacePoint[], axisX: string, axisY: string, axisZ: string): SpacePoint[] {
  const get = (p: SpacePoint, code: string): number => {
    if (code === 'x') return p.x;
    if (code === 'y') return p.y;
    if (code === 'z') return p.z;
    return Number((p.metrics ?? {})[code]);
  };

  return points.map((p) => ({
    ...p,
    x: get(p, axisX),
    y: get(p, axisY),
    z: get(p, axisZ)
  }));
}

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

  if (lodEnabled) {
    return voxelAggregateHomogeneous(projected, { minCount: lodMinCount, detail: lodDetail });
  }

  return projected;
}
