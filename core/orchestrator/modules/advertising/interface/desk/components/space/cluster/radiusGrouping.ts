import type { SpacePoint } from '../types';

type Vec3 = [number, number, number];

type RadiusGroupingArgs = {
  points: SpacePoint[];
  detail: number; // 0..1: 0 = max merge, 1 = min merge
  weights: Vec3;
};

export type RadiusGroupingResult = {
  labels: Int32Array;
  counts: Map<number, number>;
};

function clamp01(v: number): number {
  return Math.min(1, Math.max(0, v));
}

function smoothstep01(t: number): number {
  const x = clamp01(t);
  return x * x * (3 - 2 * x);
}

function radiusFromDetail(detail: number): number {
  const d = clamp01(detail);

  // detail=0 => максимальный радиус (грубая группировка)
  // detail=1 => минимальный радиус (детальная картинка)
  const rMin = 0.0025;
  const rMax = 1.05;

  // Плавное распределение чувствительности по всему ходу ползунка.
  const u = smoothstep01(1 - d);
  return rMin * Math.pow(rMax / rMin, u);
}

function cellKey(ix: number, iy: number, iz: number): string {
  return `${ix}|${iy}|${iz}`;
}

function find(parent: Int32Array, x: number): number {
  let v = x;
  while (parent[v] !== v) {
    parent[v] = parent[parent[v]];
    v = parent[v];
  }
  return v;
}

function unite(parent: Int32Array, size: Int32Array, a: number, b: number): void {
  let ra = find(parent, a);
  let rb = find(parent, b);
  if (ra === rb) return;
  if (size[ra] < size[rb]) {
    const t = ra;
    ra = rb;
    rb = t;
  }
  parent[rb] = ra;
  size[ra] += size[rb];
}

function normalizedWeighted(points: SpacePoint[], weights: Vec3): Float32Array {
  const n = points.length;
  const out = new Float32Array(n * 3);
  if (!n) return out;

  let minX = Infinity;
  let minY = Infinity;
  let minZ = Infinity;
  let maxX = -Infinity;
  let maxY = -Infinity;
  let maxZ = -Infinity;

  for (let i = 0; i < n; i += 1) {
    const p = points[i];
    const x = Number(p.x) * weights[0];
    const y = Number(p.y) * weights[1];
    const z = Number(p.z) * weights[2];

    out[i * 3] = x;
    out[i * 3 + 1] = y;
    out[i * 3 + 2] = z;

    if (x < minX) minX = x;
    if (y < minY) minY = y;
    if (z < minZ) minZ = z;
    if (x > maxX) maxX = x;
    if (y > maxY) maxY = y;
    if (z > maxZ) maxZ = z;
  }

  const spanX = Math.max(1e-9, maxX - minX);
  const spanY = Math.max(1e-9, maxY - minY);
  const spanZ = Math.max(1e-9, maxZ - minZ);

  for (let i = 0; i < n; i += 1) {
    out[i * 3] = (out[i * 3] - minX) / spanX;
    out[i * 3 + 1] = (out[i * 3 + 1] - minY) / spanY;
    out[i * 3 + 2] = (out[i * 3 + 2] - minZ) / spanZ;
  }

  return out;
}

export function groupByRadius(args: RadiusGroupingArgs): RadiusGroupingResult {
  const { points, detail, weights } = args;
  const n = points.length;

  if (!n) return { labels: new Int32Array(0), counts: new Map() };

  const r = radiusFromDetail(detail);
  const threshold = 2 * r;

  if (threshold >= 1.999) {
    return { labels: new Int32Array(n).fill(0), counts: new Map<number, number>([[0, n]]) };
  }

  if (threshold <= 1e-6) {
    const labels = new Int32Array(n);
    const counts = new Map<number, number>();
    for (let i = 0; i < n; i += 1) {
      labels[i] = i;
      counts.set(i, 1);
    }
    return { labels, counts };
  }

  const coords = normalizedWeighted(points, weights);
  const cellSize = threshold;
  const grid = new Map<string, number[]>();

  for (let i = 0; i < n; i += 1) {
    const ix = Math.floor(coords[i * 3] / cellSize);
    const iy = Math.floor(coords[i * 3 + 1] / cellSize);
    const iz = Math.floor(coords[i * 3 + 2] / cellSize);
    const key = cellKey(ix, iy, iz);
    const arr = grid.get(key);
    if (arr) arr.push(i);
    else grid.set(key, [i]);
  }

  const parent = new Int32Array(n);
  const size = new Int32Array(n);
  for (let i = 0; i < n; i += 1) {
    parent[i] = i;
    size[i] = 1;
  }

  const t2 = threshold * threshold;

  for (let i = 0; i < n; i += 1) {
    const xi = coords[i * 3];
    const yi = coords[i * 3 + 1];
    const zi = coords[i * 3 + 2];

    const ix = Math.floor(xi / cellSize);
    const iy = Math.floor(yi / cellSize);
    const iz = Math.floor(zi / cellSize);

    for (let dx = -1; dx <= 1; dx += 1) {
      for (let dy = -1; dy <= 1; dy += 1) {
        for (let dz = -1; dz <= 1; dz += 1) {
          const bucket = grid.get(cellKey(ix + dx, iy + dy, iz + dz));
          if (!bucket) continue;
          for (let q = 0; q < bucket.length; q += 1) {
            const j = bucket[q];
            if (j <= i) continue;
            const dxv = xi - coords[j * 3];
            const dyv = yi - coords[j * 3 + 1];
            const dzv = zi - coords[j * 3 + 2];
            const dist2 = dxv * dxv + dyv * dyv + dzv * dzv;
            if (dist2 <= t2) unite(parent, size, i, j);
          }
        }
      }
    }
  }

  const rootToId = new Map<number, number>();
  const labels = new Int32Array(n).fill(-1);
  const counts = new Map<number, number>();
  let nextId = 0;

  for (let i = 0; i < n; i += 1) {
    const root = find(parent, i);
    let cid = rootToId.get(root);
    if (cid == null) {
      cid = nextId;
      nextId += 1;
      rootToId.set(root, cid);
    }
    labels[i] = cid;
    counts.set(cid, (counts.get(cid) ?? 0) + 1);
  }

  return { labels, counts };
}
