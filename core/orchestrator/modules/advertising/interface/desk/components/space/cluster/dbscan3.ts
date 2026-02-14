import type { DbscanParams } from './types';

export type Vec3 = [number, number, number];

function dist2(a: Vec3, b: Vec3, w: Vec3): number {
  const dx = (a[0] - b[0]) * w[0];
  const dy = (a[1] - b[1]) * w[1];
  const dz = (a[2] - b[2]) * w[2];
  return dx * dx + dy * dy + dz * dz;
}

function cellKey(ix: number, iy: number, iz: number): string {
  return `${ix},${iy},${iz}`;
}

function buildGrid(points: Vec3[], cellSize: number): Map<string, number[]> {
  const grid = new Map<string, number[]>();
  for (let i = 0; i < points.length; i += 1) {
    const p = points[i];
    const ix = Math.floor(p[0] / cellSize);
    const iy = Math.floor(p[1] / cellSize);
    const iz = Math.floor(p[2] / cellSize);
    const key = cellKey(ix, iy, iz);
    const arr = grid.get(key);
    if (arr) arr.push(i);
    else grid.set(key, [i]);
  }
  return grid;
}

function regionQuery(args: {
  points: Vec3[];
  grid: Map<string, number[]>;
  idx: number;
  eps2: number;
  cellSize: number;
  w: Vec3;
}): number[] {
  const { points, grid, idx, eps2, cellSize, w } = args;
  const p = points[idx];
  const ix = Math.floor(p[0] / cellSize);
  const iy = Math.floor(p[1] / cellSize);
  const iz = Math.floor(p[2] / cellSize);

  const out: number[] = [];
  for (let dx = -1; dx <= 1; dx += 1) {
    for (let dy = -1; dy <= 1; dy += 1) {
      for (let dz = -1; dz <= 1; dz += 1) {
        const key = cellKey(ix + dx, iy + dy, iz + dz);
        const bucket = grid.get(key);
        if (!bucket) continue;
        for (const j of bucket) {
          if (dist2(p, points[j], w) <= eps2) out.push(j);
        }
      }
    }
  }
  return out;
}

/**
 * DBSCAN в 3D, сложность ~ O(n * avg_neighbors).
 * Возвращает labels: -1 шум, 0..k-1 кластеры.
 */
export function dbscan3(points: Vec3[], params: DbscanParams, w: Vec3): Int32Array {
  const { eps, minPts } = params;
  const eps2 = eps * eps;
  const cellSize = Math.max(eps, 1e-6);

  const grid = buildGrid(points, cellSize);

  const labels = new Int32Array(points.length).fill(-2); // -2 = unvisited, -1 = noise
  let clusterId = 0;

  for (let i = 0; i < points.length; i += 1) {
    if (labels[i] !== -2) continue;

    const neighbors = regionQuery({ points, grid, idx: i, eps2, cellSize, w });
    if (neighbors.length < minPts) {
      labels[i] = -1;
      continue;
    }

    labels[i] = clusterId;

    const queue = [...neighbors];
    for (let qi = 0; qi < queue.length; qi += 1) {
      const j = queue[qi];

      if (labels[j] === -1) labels[j] = clusterId; // border point
      if (labels[j] !== -2) continue;

      labels[j] = clusterId;

      const n2 = regionQuery({ points, grid, idx: j, eps2, cellSize, w });
      if (n2.length >= minPts) queue.push(...n2);
    }

    clusterId += 1;
  }

  // replace -2 -> -1 (shouldn't happen, but safe)
  for (let i = 0; i < labels.length; i += 1) if (labels[i] === -2) labels[i] = -1;

  return labels;
}
