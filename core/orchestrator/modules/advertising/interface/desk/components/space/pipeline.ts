function clamp01(v: number): number {
  return Math.min(1, Math.max(0, v));
}

/**
 * Гарантия:
 * - detail=0 -> вообще не агрегируем (кластеров = точек)
 * - detail=1 -> всё в одну ячейку (1 кластер)
 */
function voxelSizeFromDetailAndBBox(detail01: number, bbox: BBox): number {
  const d = clamp01(detail01);

  const spanX = Math.max(0.001, bbox.maxX - bbox.minX);
  const spanY = Math.max(0.001, bbox.maxY - bbox.minY);
  const spanZ = Math.max(0.001, bbox.maxZ - bbox.minZ);
  const maxSpan = Math.max(spanX, spanY, spanZ);

  const minSize = Math.max(1e-9, maxSpan / 10_000); // почти "по точкам"
  const maxSize = maxSpan * 2; // гарантированно "одна ячейка"
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

  // detail=0  -> total+1 (никогда не агрегирует)
  // detail=1  -> 1       (агрегирует всё, и при maxSize будет 1 ячейка)
  const noAgg = total + 1;
  if (d <= 0) return noAgg;
  if (d >= 1) return 1;

  // до 0.85 — плавно идем к baseMinCount
  if (d < 0.85) {
    const mid = Math.max(2, Math.round(noAgg * (1 - d) + baseMinCount * d));
    return Math.min(noAgg, mid);
  }

  // 0.85..1 — дожимаем minCount до 1
  const u = (d - 0.85) / 0.15; // 0..1
  const mid = Math.max(2, Math.round(noAgg * (1 - d) + baseMinCount * d));
  return Math.max(1, Math.round(mid * (1 - u) + 1 * u));
}

function voxelAggregateHomogeneous(points: SpacePoint[], opts: { minCount: number; detail: number }): SpacePoint[] {
  const { minCount, detail } = opts;
  if (!points.length) return [];

  const out: SpacePoint[] = [];

  const byField = new Map<string, SpacePoint[]>();
  for (const p of points) {
    const key = p.sourceField;
    const arr = byField.get(key);
    if (arr) arr.push(p);
    else byField.set(key, [p]);
  }

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

      let sx = 0;
      let sy = 0;
      let sz = 0;

      let minX = Infinity;
      let minY = Infinity;
      let minZ = Infinity;
      let maxX = -Infinity;
      let maxY = -Infinity;
      let maxZ = -Infinity;

      const metrics: Record<string, number> = {};
      const n = cellPoints.length;

      for (const p of cellPoints) {
        sx += p.x;
        sy += p.y;
        sz += p.z;

        if (p.x < minX) minX = p.x;
        if (p.y < minY) minY = p.y;
        if (p.z < minZ) minZ = p.z;
        if (p.x > maxX) maxX = p.x;
        if (p.y > maxY) maxY = p.y;
        if (p.z > maxZ) maxZ = p.z;

        for (const [k, v] of Object.entries(p.metrics ?? {})) {
          metrics[k] = (metrics[k] ?? 0) + v;
        }
      }

      for (const k of Object.keys(metrics)) metrics[k] /= n;

      out.push({
        id: `cluster:${field}:${cell}`,
        label: `Кластер (${n})`,
        sourceField: field,
        metrics,
        x: sx / n,
        y: sy / n,
        z: sz / n,
        isCluster: true,
        clusterCount: n,
        span: { x: maxX - minX, y: maxY - minY, z: maxZ - minZ }
      });
    }
  }

  return out;
}
