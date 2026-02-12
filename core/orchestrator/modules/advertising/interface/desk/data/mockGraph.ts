export type EntityType = 'item' | 'campaign';
export type EntityMode = 'Артикулы (SKU)' | 'Предметы' | 'Категории' | 'Бренды' | 'Рекламные кампании (РК)';
export type PeriodMode = '7 дней' | '14 дней' | '30 дней' | 'Выбрать даты';
export type Mode3D = '2D' | '3D';

export type DimensionField = 'sku' | 'subject' | 'campaign_id' | 'entry_point' | 'segment' | 'brand' | 'category';
export type MeasureField =
  | 'revenue'
  | 'orders'
  | 'spend'
  | 'drr'
  | 'roi'
  | 'cr0'
  | 'cr1'
  | 'cr2'
  | 'clicks'
  | 'position'
  | 'search_share'
  | 'shelf_share'
  | 'stock_days'
  | 'entry_gap';

export type ColorField = MeasureField | 'entry_point';

export type DataField = {
  key: DimensionField | MeasureField | 'date';
  title: string;
  kind: 'dimension' | 'measure' | 'time';
};

export const DATA_FIELDS: DataField[] = [
  { key: 'sku', title: 'Артикул (SKU)', kind: 'dimension' },
  { key: 'subject', title: 'Предмет', kind: 'dimension' },
  { key: 'campaign_id', title: 'ID РК', kind: 'dimension' },
  { key: 'entry_point', title: 'Точка входа', kind: 'dimension' },
  { key: 'segment', title: 'Сегмент', kind: 'dimension' },
  { key: 'brand', title: 'Бренд', kind: 'dimension' },
  { key: 'category', title: 'Категория', kind: 'dimension' },
  { key: 'revenue', title: 'Выручка', kind: 'measure' },
  { key: 'orders', title: 'Заказы', kind: 'measure' },
  { key: 'spend', title: 'Расход', kind: 'measure' },
  { key: 'drr', title: 'ДРР', kind: 'measure' },
  { key: 'roi', title: 'ROI', kind: 'measure' },
  { key: 'cr0', title: 'CR0', kind: 'measure' },
  { key: 'cr1', title: 'CR1', kind: 'measure' },
  { key: 'cr2', title: 'CR2', kind: 'measure' },
  { key: 'clicks', title: 'Клики', kind: 'measure' },
  { key: 'search_share', title: 'Доля поиска', kind: 'measure' },
  { key: 'shelf_share', title: 'Доля полки', kind: 'measure' },
  { key: 'stock_days', title: 'Остатки, дней', kind: 'measure' },
  { key: 'entry_gap', title: 'Расхождение долей входов', kind: 'measure' },
  { key: 'date', title: 'Дата', kind: 'time' },
];

export const MEASURE_FIELDS = DATA_FIELDS.filter((field) => field.kind === 'measure') as Array<DataField & { kind: 'measure' }>;

export type SimilarityEntity = {
  id: string;
  name: string;
  type: EntityType;
  sku: string;
  subject: string;
  campaign_id: string;
  keyword_cluster: string;
  search_query: string;
  entry_point: 'Поиск' | 'Полка' | 'Категория';
  segment: 'Премиум' | 'Чувствительные к цене' | 'Импульсные';
  brand: string;
  category: string;
  rkType: 'Поиск' | 'Каталог' | 'Авто';
  clusterName: string;
  date: string;
  targetCluster: string;
  matchScore: number;
  measures: Record<MeasureField, number>;
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
  clusterName: string;
};

const subjects = ['Кроссовки', 'Джинсы', 'Аксессуары', 'Верхняя одежда', 'База'];
const brands = ['Север', 'Лайм', 'Пульс', 'Орион', 'Вектор'];
const categories = ['Одежда', 'Обувь', 'Спорт', 'Дом'];
const segments = ['Премиум', 'Чувствительные к цене', 'Импульсные'] as const;
const entryPoints = ['Поиск', 'Полка', 'Категория'] as const;
const rkTypes = ['Поиск', 'Каталог', 'Авто'] as const;
const keywordClusters = ['Брендовый спрос', 'Конкурентный спрос', 'Высокая маржинальность', 'Широкий охват'];

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

function nearestCluster(position: { x: number; y: number; z: number }): BuyerCluster {
  return buyerClusters.map((cluster) => ({ cluster, dist: distance3(position, cluster.center) })).sort((a, b) => a.dist - b.dist)[0].cluster;
}

function metricProfile(distanceToCluster: number): Record<MeasureField, number> {
  const clusterFactor = clamp(1 - distanceToCluster / 180, 0.1, 1);
  const spend = clamp(12000 + clusterFactor * 52000 + rnd(-5000, 5000), 1200, 95000);
  const orders = clamp(60 + clusterFactor * 580 + rnd(-45, 45), 2, 1200);
  const revenue = clamp(spend * rnd(1.4, 3.8), 1500, 380000);
  const clicks = clamp(orders * rnd(8, 28), 40, 24000);
  const roi = clamp(revenue / Math.max(spend, 1), 0.2, 7.2);
  const drr = clamp((spend / Math.max(revenue, 1)) * 100, 2.5, 58);
  const cr2 = clamp((orders / Math.max(clicks, 1)) * 100, 0.15, 18);
  const cr1 = clamp(cr2 * rnd(1.1, 1.45), 0.2, 24);
  const cr0 = clamp(cr1 * rnd(1.2, 1.6), 0.4, 35);
  const searchShare = clamp(0.2 + clusterFactor * 0.75 + rnd(-0.08, 0.08), 0, 1);
  const shelfShare = clamp(1 - searchShare + rnd(-0.05, 0.05), 0, 1);
  const position = clamp(3 + (1 - clusterFactor) * 42 + rnd(-4, 4), 1, 60);
  const stockDays = Math.round(clamp(8 + (1 - clusterFactor) * 70 + rnd(-5, 8), 3, 120));
  const entryGap = Math.abs(searchShare - shelfShare) * 100;

  return {
    revenue: Math.round(revenue),
    orders: Math.round(orders),
    spend: Math.round(spend),
    drr: Number(drr.toFixed(2)),
    roi: Number(roi.toFixed(2)),
    cr0: Number(cr0.toFixed(2)),
    cr1: Number(cr1.toFixed(2)),
    cr2: Number(cr2.toFixed(2)),
    clicks: Math.round(clicks),
    position: Number(position.toFixed(2)),
    search_share: Number((searchShare * 100).toFixed(2)),
    shelf_share: Number((shelfShare * 100).toFixed(2)),
    stock_days: stockDays,
    entry_gap: Number(entryGap.toFixed(2)),
  };
}

function buildItem(i: number): SimilarityEntity {
  const cluster = buyerClusters[i % buyerClusters.length];
  const pos = { x: cluster.center.x + rnd(-15, 15), y: cluster.center.y + rnd(-15, 15), z: cluster.center.z + rnd(-15, 15) };
  const dist = distance3(pos, cluster.center);
  return {
    id: `SKU-${100000 + i}`,
    name: `SKU-${100000 + i}`,
    type: 'item',
    sku: `SKU-${100000 + i}`,
    subject: subjects[i % subjects.length],
    campaign_id: '',
    keyword_cluster: keywordClusters[i % keywordClusters.length],
    search_query: `запрос ${1 + (i % 30)}`,
    entry_point: entryPoints[i % entryPoints.length],
    segment: segments[i % segments.length],
    brand: brands[i % brands.length],
    category: categories[i % categories.length],
    rkType: rkTypes[i % rkTypes.length],
    clusterName: cluster.name,
    date: `2026-02-${String(1 + (i % 28)).padStart(2, '0')}`,
    targetCluster: cluster.name,
    matchScore: Math.round(clamp(100 - dist, 0, 100)),
    measures: metricProfile(dist),
  };
}

function buildCampaign(i: number): SimilarityEntity {
  const nearTarget = Math.random() < 0.6;
  const target = buyerClusters[i % buyerClusters.length];
  const pos = nearTarget
    ? { x: target.center.x + rnd(-18, 18), y: target.center.y + rnd(-18, 18), z: target.center.z + rnd(-18, 18) }
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
    sku: '',
    subject: subjects[i % subjects.length],
    campaign_id: `CMP-${1000 + i}`,
    keyword_cluster: keywordClusters[i % keywordClusters.length],
    search_query: `рк запрос ${1 + (i % 25)}`,
    entry_point: entryPoints[i % entryPoints.length],
    segment: segments[i % segments.length],
    brand: brands[i % brands.length],
    category: categories[i % categories.length],
    rkType: rkTypes[i % rkTypes.length],
    clusterName: nearest.name,
    date: `2026-02-${String(1 + ((i + 7) % 28)).padStart(2, '0')}`,
    targetCluster: nearest.name,
    matchScore: score,
    measures: metricProfile(dist + (nearTarget ? 0 : 18)),
  };
}

export function generateSimilarityData(itemCount = 200, campaignCount = 80): SimilarityEntity[] {
  const items = Array.from({ length: itemCount }).map((_, i) => buildItem(i));
  const campaigns = Array.from({ length: campaignCount }).map((_, i) => buildCampaign(i));
  return [...items, ...campaigns];
}

export function generateBuyerCloud(pointsPerCluster = 220): { clusters: BuyerCluster[]; points: BuyerPoint[] } {
  const points: BuyerPoint[] = [];
  buyerClusters.forEach((cluster) => {
    for (let i = 0; i < pointsPerCluster; i += 1) {
      points.push({
        id: `${cluster.id}-${i}`,
        clusterName: cluster.name,
        x: cluster.center.x + rnd(-26, 26),
        y: cluster.center.y + rnd(-26, 26),
        z: cluster.center.z + rnd(-26, 26),
      });
    }
  });
  return { clusters: buyerClusters, points };
}

export function aggregateBySubject(entities: SimilarityEntity[]): SimilarityEntity[] {
  const items = entities.filter((e) => e.type === 'item');
  const campaigns = entities.filter((e) => e.type === 'campaign');
  const groups = new Map<string, SimilarityEntity[]>();
  items.forEach((item) => {
    if (!groups.has(item.subject)) groups.set(item.subject, []);
    groups.get(item.subject)?.push(item);
  });

  const aggregated = Array.from(groups.entries()).map(([subject, group], idx) => {
    const avg = (key: MeasureField): number => group.reduce((sum, e) => sum + e.measures[key], 0) / group.length;
    return {
      id: `SUB-${idx + 1}`,
      name: `${subject} (${group.length})`,
      type: 'item' as const,
      sku: '',
      subject,
      campaign_id: '',
      entry_point: group[0].entry_point,
      segment: group[0].segment,
      brand: group[0].brand,
      category: group[0].category,
      rkType: group[0].rkType,
      clusterName: group[0].clusterName,
      date: group[0].date,
      targetCluster: group[0].targetCluster,
      matchScore: Math.round(avg('entry_gap')),
      measures: {
        revenue: Math.round(avg('revenue')),
        orders: Math.round(avg('orders')),
        spend: Math.round(avg('spend')),
        drr: Number(avg('drr').toFixed(2)),
        roi: Number(avg('roi').toFixed(2)),
        cr0: Number(avg('cr0').toFixed(2)),
        cr1: Number(avg('cr1').toFixed(2)),
        cr2: Number(avg('cr2').toFixed(2)),
        clicks: Math.round(avg('clicks')),
        position: Number(avg('position').toFixed(2)),
        search_share: Number(avg('search_share').toFixed(2)),
        shelf_share: Number(avg('shelf_share').toFixed(2)),
        stock_days: Math.round(avg('stock_days')),
        entry_gap: Number(avg('entry_gap').toFixed(2)),
      },
    };
  });

  return [...aggregated, ...campaigns];
}

export function getMeasure(entity: SimilarityEntity, field: MeasureField): number {
  return entity.measures[field];
}

export function formatFieldTitle(field: MeasureField | ColorField): string {
  const found = DATA_FIELDS.find((item) => item.key === field);
  return found?.title ?? String(field);
}

export function nearestOpposite(selected: SimilarityEntity, entities: SimilarityEntity[], limit = 10): SimilarityEntity[] {
  return entities
    .filter((e) => e.type !== selected.type)
    .map((e) => ({ e, d: Math.abs(e.measures.revenue - selected.measures.revenue) + Math.abs(e.measures.spend - selected.measures.spend) }))
    .sort((a, b) => a.d - b.d)
    .slice(0, limit)
    .map((x) => x.e);
}
