import type { SimilarityEntity } from './mockGraph';

export type SpaceApiPoint = {
  id: number;
  entity_type: 'sku' | 'campaign';
  entity_key: string;
  name: string;
  date: string;
  revenue: number;
  orders: number;
  spend: number;
  drr: number;
  roi: number;
  cr0: number;
  cr1: number;
  cr2: number;
  position: number;
  stock_days: number;
  search_share: number;
  shelf_share: number;
};

export function mapApiPoint(point: SpaceApiPoint, index: number): SimilarityEntity {
  const entryPoint = point.search_share >= point.shelf_share ? 'Поиск' : 'Полка';
  return {
    id: point.entity_key,
    name: point.name,
    type: point.entity_type === 'campaign' ? 'campaign' : 'item',
    sku: point.entity_type === 'sku' ? point.entity_key : '',
    subject: `Предмет ${1 + (index % 4)}`,
    campaign_id: point.entity_type === 'campaign' ? point.entity_key : '',
    keyword_cluster: `Кластер ${1 + (index % 3)}`,
    search_query: `запрос ${1 + (index % 20)}`,
    entry_point: entryPoint,
    segment: ['Премиум', 'Чувствительные к цене', 'Импульсные'][index % 3] as SimilarityEntity['segment'],
    brand: `Бренд ${1 + (index % 5)}`,
    category: ['Одежда', 'Обувь', 'Дом'][index % 3],
    rkType: ['Поиск', 'Каталог', 'Авто'][index % 3] as SimilarityEntity['rkType'],
    clusterName: `Кластер ${1 + (index % 3)}`,
    date: point.date,
    targetCluster: `Кластер ${1 + (index % 3)}`,
    matchScore: Math.max(0, Math.min(100, Math.round(100 - Math.abs(point.drr - 15) * 2))),
    measures: {
      revenue: Number(point.revenue ?? 0),
      orders: Number(point.orders ?? 0),
      spend: Number(point.spend ?? 0),
      drr: Number(point.drr ?? 0),
      roi: Number(point.roi ?? 0),
      cr0: Number(point.cr0 ?? 0),
      cr1: Number(point.cr1 ?? 0),
      cr2: Number(point.cr2 ?? 0),
      clicks: Math.round(Number(point.orders ?? 0) * 11),
      position: Number(point.position ?? 0),
      search_share: Number(point.search_share ?? 0),
      shelf_share: Number(point.shelf_share ?? 0),
      stock_days: Number(point.stock_days ?? 0),
      entry_gap: Math.abs(Number(point.search_share ?? 0) - Number(point.shelf_share ?? 0)),
    },
  };
}
