export type EntityType = 'item' | 'campaign';
export type LevelMode = 'SKU' | 'Предмет';
export type PeriodMode = '7 дней' | '14 дней' | '30 дней';
export type Mode3D = '2D' | '3D';
export type ColorMetric = 'DRR' | 'ROI' | 'CR2' | 'Spend' | 'EntryPoint(SearchShare)' | 'SegmentShare';
export type SizeMetric = 'Sales' | 'Spend' | 'Orders';

export type Metrics = {
  spend: number;
  clicks: number;
  orders: number;
  sales: number;
  DRR: number;
  ROI: number;
  CR2: number;
  entry_search_share: number;
  entry_shelf_share: number;
  segmentA_share: number;
  segmentB_share: number;
  segmentC_share: number;
};

export type SimilarityEntity = {
  id: string;
  name: string;
  type: EntityType;
  levelKey: string;
  clusterName: string;
  subjectId?: string;
  campaignId?: string;
  x: number;
  y: number;
  z: number;
  matchScore?: number;
  targetCluster?: string;
  metrics: Metrics;
};

export type BuyerCluster = {
  id: string;
  name: string;
  center: { x: number; y: number; z: number };
};

export type BuyerPoint = {
  id: string;
  x: number;
  y: number;
  z: number;
  clusterId: string;
  clusterName: string;
};

const subjects = ['Кроссовки', 'Джинсы', 'Аксессуары', 'Верхняя одежда', 'База'];

const buyerClusters: BuyerCluster[] = [
  { id: 'c1', name: 'Рациональные покупатели', center: { x: -58, y: 36, z: 20 } },
  { id: 'c2', name: 'Импульсные покупатели', center: { x: 42, y: -22, z: 10 } },
  { id: 'c3', name: 'Охотники за скидками', center: { x: -12, y: -52, z: -26 } },
  { id: 'c4', name: 'Премиум-аудитория', center: { x: 58, y: 42, z: -12 } },
  { id: 'c5', name: 'Лояльные повторные', center: { x: 10, y: 20, z: 44 } },
];

const rnd = (a: number, b: number): number => a + Math.random() * (b - a);
const clamp = (v: number, min: number, max: number): number => Math.max(min, Math.min(max, v));

function distance3(a: { x: number; y: number; z: number }, b: { x: number; y: number; z: number }): number {
  const dx = a.x - b.x;
  const dy = a.y - b.y;
  const dz = a.z - b.z;
  return Math.sqrt(dx * dx + dy * dy + dz * dz);
}

function metricProfile(distanceToCluster: number): Metrics {
  const clusterFactor = clamp(1 - distanceToCluster / 180, 0.1, 1);
  const spend = clamp(12000 + clusterFactor * 52000 + rnd(-5000, 5000), 1200, 95000);
  const orders = clamp(60 + clusterFactor * 580 + rnd(-45, 45), 2, 1200);
  const sales = clamp(spend * rnd(1.4, 3.8), 1500, 380000);
  const clicks = clamp(orders * rnd(8, 28), 40, 24000);
  const roi = clamp((sales / Math.max(spend, 1)) * rnd(0.7, 1.25), 0.2, 7.2);
  const drr = clamp((spend / Math.max(sales, 1)) * 100 * rnd(0.85, 1.1), 2.5, 58);
  const cr2 = clamp((orders / Math.max(clicks, 1)) * 100 * rnd(0.85, 1.2), 0.15, 18);

  const entrySearch = clamp(0.2 + clusterFactor * 0.75 + rnd(-0.08, 0.08), 0, 1);
  const entryShelf = clamp(1 - entrySearch + rnd(-0.05, 0.05), 0, 1);

  const a = clamp(0.15 + clusterFactor * 0.5 + rnd(-0.06, 0.06), 0.01, 0.95);
  const b = clamp(0.25 + (1 - clusterFactor) * 0.5 + rnd(-0.06, 0.06), 0.01, 0.95);
  const c = clamp(1 - a - b, 0.01, 0.95);
  const sum = a + b + c;

  return {
    spend: Math.round(spend),
    clicks: Math.round(clicks),
    orders: Math.round(orders),
    sales: Math.round(sales),
    DRR: Number(drr.toFixed(2)),
    ROI: Number(roi.toFixed(2)),
    CR2: Number(cr2.toFixed(2)),
    entry_search_share: Number(entrySearch.toFixed(3)),
    entry_shelf_share: Number(entryShelf.toFixed(3)),
    segmentA_share: Number((a / sum).toFixed(3)),
    segmentB_share: Number((b / sum).toFixed(3)),
    segmentC_share: Number((c / sum).toFixed(3)),
  };
}

function nearestCluster(position: { x: number; y: number; z: number }): BuyerCluster {
  return buyerClusters
    .map((cluster) => ({ cluster, dist: distance3(position, cluster.center) }))
    .sort((a, b) => a.dist - b.dist)[0].cluster;
}

function buildItem(i: number): SimilarityEntity {
  const cluster = buyerClusters[i % buyerClusters.length];
  const pos = {
    x: cluster.center.x + rnd(-15, 15),
    y: cluster.center.y + rnd(-15, 15),
    z: cluster.center.z + rnd(-15, 15),
  };
  const dist = distance3(pos, cluster.center);
  return {
    id: `SKU-${100000 + i}`,
    name: `SKU-${100000 + i}`,
    type: 'item',
    levelKey: `SKU-${100000 + i}`,
    clusterName: cluster.name,
    subjectId: subjects[i % subjects.length],
    x: pos.x,
    y: pos.y,
    z: pos.z,
    metrics: metricProfile(dist),
  };
}

function buildCampaign(i: number): SimilarityEntity {
  const nearTarget = Math.random() < 0.6;
  const target = buyerClusters[i % buyerClusters.length];
  const pos = nearTarget
    ? {
        x: target.center.x + rnd(-18, 18),
        y: target.center.y + rnd(-18, 18),
        z: target.center.z + rnd(-18, 18),
      }
    : {
        x: target.center.x + rnd(55, 92) * (Math.random() > 0.5 ? 1 : -1),
        y: target.center.y + rnd(55, 92) * (Math.random() > 0.5 ? 1 : -1),
        z: target.center.z + rnd(45, 86) * (Math.random() > 0.5 ? 1 : -1),
      };

  const nearest = nearestCluster(pos);
  const dist = distance3(pos, nearest.center);
  const score = Math.round(clamp(100 - dist * 1.12, 0, 100));

  return {
    id: `CMP-${1000 + i}`,
    name: `РК ${1000 + i}`,
    type: 'campaign',
    levelKey: `CMP-${1000 + i}`,
    clusterName: nearest.name,
    campaignId: `CMP-${1000 + i}`,
    x: pos.x,
    y: pos.y,
    z: pos.z,
    matchScore: score,
    targetCluster: nearest.name,
    metrics: metricProfile(dist + (nearTarget ? 0 : 22)),
  };
}

export function generateSimilarityData(itemCount = 200, campaignCount = 80): SimilarityEntity[] {
  const items = Array.from({ length: itemCount }).map((_, i) => buildItem(i));
  const campaigns = Array.from({ length: campaignCount }).map((_, i) => buildCampaign(i));
  return [...items, ...campaigns];
}

export function generateBuyerCloud(pointsPerCluster = 240): { clusters: BuyerCluster[]; points: BuyerPoint[] } {
  const points: BuyerPoint[] = [];
  buyerClusters.forEach((cluster) => {
    for (let i = 0; i < pointsPerCluster; i += 1) {
      points.push({
        id: `${cluster.id}-${i}`,
        clusterId: cluster.id,
        clusterName: cluster.name,
        x: cluster.center.x + rnd(-26, 26),
        y: cluster.center.y + rnd(-26, 26),
        z: cluster.center.z + rnd(-26, 26),
      });
    }
  });
  return { clusters: buyerClusters, points };
}

export function aggregateItemsByLevel(entities: SimilarityEntity[], level: LevelMode): SimilarityEntity[] {
  const campaigns = entities.filter((e) => e.type === 'campaign');
  const items = entities.filter((e) => e.type === 'item');
  if (level === 'SKU') return [...items, ...campaigns];

  const groups = new Map<string, SimilarityEntity[]>();
  items.forEach((item) => {
    const key = `ПР:${item.subjectId}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key)?.push(item);
  });

  const aggregated = Array.from(groups.entries()).map(([key, arr]) => {
    const avg = (field: (e: SimilarityEntity) => number): number => arr.reduce((a, b) => a + field(b), 0) / arr.length;
    return {
      id: key,
      name: `${arr[0].subjectId} (${arr.length})`,
      type: 'item' as const,
      levelKey: key,
      clusterName: arr[0].clusterName,
      subjectId: arr[0].subjectId,
      x: avg((e) => e.x),
      y: avg((e) => e.y),
      z: avg((e) => e.z),
      metrics: {
        spend: Math.round(avg((e) => e.metrics.spend)),
        clicks: Math.round(avg((e) => e.metrics.clicks)),
        orders: Math.round(avg((e) => e.metrics.orders)),
        sales: Math.round(avg((e) => e.metrics.sales)),
        DRR: Number(avg((e) => e.metrics.DRR).toFixed(2)),
        ROI: Number(avg((e) => e.metrics.ROI).toFixed(2)),
        CR2: Number(avg((e) => e.metrics.CR2).toFixed(2)),
        entry_search_share: Number(avg((e) => e.metrics.entry_search_share).toFixed(3)),
        entry_shelf_share: Number(avg((e) => e.metrics.entry_shelf_share).toFixed(3)),
        segmentA_share: Number(avg((e) => e.metrics.segmentA_share).toFixed(3)),
        segmentB_share: Number(avg((e) => e.metrics.segmentB_share).toFixed(3)),
        segmentC_share: Number(avg((e) => e.metrics.segmentC_share).toFixed(3)),
      },
    };
  });

  return [...aggregated, ...campaigns];
}

export function metricValue(entity: SimilarityEntity, metric: ColorMetric | SizeMetric): number {
  if (metric === 'DRR') return entity.metrics.DRR;
  if (metric === 'ROI') return entity.metrics.ROI;
  if (metric === 'CR2') return entity.metrics.CR2;
  if (metric === 'Spend') return entity.metrics.spend;
  if (metric === 'Sales') return entity.metrics.sales;
  if (metric === 'Orders') return entity.metrics.orders;
  if (metric === 'EntryPoint(SearchShare)') return entity.metrics.entry_search_share * 100;
  return ((entity.metrics.segmentA_share + entity.metrics.segmentB_share + entity.metrics.segmentC_share) / 3) * 100;
}

export function formatMetricLabel(metric: ColorMetric | SizeMetric): string {
  if (metric === 'DRR') return 'ДРР';
  if (metric === 'ROI') return 'ROI';
  if (metric === 'CR2') return 'CR2';
  if (metric === 'Spend') return 'Расход';
  if (metric === 'Sales') return 'Выручка';
  if (metric === 'Orders') return 'Заказы';
  if (metric === 'EntryPoint(SearchShare)') return 'Доля поиска';
  return 'Доля сегментов';
}

function distanceEntity(a: SimilarityEntity, b: SimilarityEntity): number {
  return distance3(a, b);
}

export function nearestOpposite(selected: SimilarityEntity, entities: SimilarityEntity[], limit = 10, far = false): SimilarityEntity[] {
  return entities
    .filter((e) => e.type !== selected.type)
    .map((e) => ({ e, d: distanceEntity(selected, e) }))
    .sort((a, b) => (far ? b.d - a.d : a.d - b.d))
    .slice(0, limit)
    .map((x) => x.e);
}
