import type { SpacePoint } from '../types';
import type { GroupingConfig } from './types';
import type { Vec3 } from './dbscan3';
import { textToVec3 } from './hash3';

function minMax01(values: number[]): { min: number; max: number } {
  let min = Infinity;
  let max = -Infinity;
  for (const v of values) {
    if (!Number.isFinite(v)) continue;
    min = Math.min(min, v);
    max = Math.max(max, v);
  }
  if (!Number.isFinite(min) || !Number.isFinite(max) || Math.abs(max - min) < 1e-9) return { min: 0, max: 1 };
  return { min, max };
}

function norm01(v: number, min: number, max: number): number {
  return (v - min) / (max - min);
}

export function buildFeatureVec3(args: {
  points: SpacePoint[];
  cfg: GroupingConfig;
  axis: { x: string; y: string; z: string };
  getTextValue: (p: SpacePoint, field: string) => string;
}): Vec3[] {
  const { points, cfg, axis, getTextValue } = args;

  if (cfg.principle === 'proximity') {
    // x/y/z в points уже "проекция", но мы нормализуем в 0..1 чтобы eps был стабильным
    const xs = points.map((p) => p.x);
    const ys = points.map((p) => p.y);
    const zs = points.map((p) => p.z);
    const rx = minMax01(xs);
    const ry = minMax01(ys);
    const rz = minMax01(zs);

    return points.map((p) => [norm01(p.x, rx.min, rx.max), norm01(p.y, ry.min, ry.max), norm01(p.z, rz.min, rz.max)]);
  }

  if (cfg.principle === 'efficiency') {
    // number поля -> до 3 признаков
    const fields = cfg.featureFields.slice(0, 3);
    const valsByField = fields.map((f) => points.map((p) => Number(p.metrics?.[f])));

    const ranges = valsByField.map((vals) => minMax01(vals));

    return points.map((p) => {
      const v = fields.map((f, idx) => {
        const raw = Number(p.metrics?.[f]);
        if (!Number.isFinite(raw)) return 0.5;
        const r = ranges[idx];
        return norm01(raw, r.min, r.max);
      });

      while (v.length < 3) v.push(0.5);
      return [v[0], v[1], v[2]];
    });
  }

  // behavior: text поля -> stable hash -> vec3 0..1
  const tFields = cfg.featureFields.slice(0, 3);

  return points.map((p) => {
    const parts = tFields.map((f) => getTextValue(p, f));
    return textToVec3(parts);
  });
}

export function weightsForCfg(cfg: GroupingConfig): [number, number, number] {
  if (cfg.principle !== 'proximity') return [1, 1, 1];
  if (!cfg.customWeights) return [1, 1, 1];
  return [cfg.wX, cfg.wY, cfg.wZ];
}

// detail 0..1 -> eps/minPts (подобрано для 0..1 пространства)
export function dbscanParamsFromDetail(detail01: number): { eps: number; minPts: number } {
  const d = Math.min(1, Math.max(0, detail01));
  // меньше detail => меньше eps => больше мелких групп
  const eps = 0.03 + (0.18 - 0.03) * d;     // 0.03..0.18
  const minPts = Math.round(3 + (12 - 3) * d); // 3..12
  return { eps, minPts };
}
