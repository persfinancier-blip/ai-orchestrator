import { writable } from 'svelte/store';

export type DatasetId = 'sales_fact' | 'ads';
export type FieldKind = 'text' | 'number' | 'date';
export type FieldRole = 'entity' | 'axis' | 'filter';

export interface ShowcaseField {
  code: string;
  name: string;
  kind: FieldKind;
  roles: FieldRole[];
  datasetIds: DatasetId[];
}

export interface ShowcaseRow {
  id: string;
  sku: string;
  campaign_id: string;
  subject: string;
  brand: string;
  category: string;
  entry_point: string;
  keyword_cluster: string;
  search_query: string;
  revenue: number;
  orders: number;
  spend: number;
  drr: number;
  roi: number;
  cr2: number;
  position: number;
  search_share: number;
  shelf_share: number;
  date: string;
}

export interface ShowcaseState {
  datasets: Array<{ id: DatasetId; name: string }>;
  fields: ShowcaseField[];
  rows: ShowcaseRow[];
}

const datasets: ShowcaseState['datasets'] = [
  { id: 'sales_fact', name: 'Факт продаж' },
  { id: 'ads', name: 'Реклама' },
];

const fields: ShowcaseField[] = [
  { code: 'sku', name: 'Артикул', kind: 'text', roles: ['entity', 'filter'], datasetIds: ['sales_fact'] },
  { code: 'campaign_id', name: 'РК', kind: 'text', roles: ['entity', 'filter'], datasetIds: ['ads'] },
  { code: 'subject', name: 'Предмет', kind: 'text', roles: ['entity', 'filter'], datasetIds: ['sales_fact', 'ads'] },
  { code: 'brand', name: 'Бренд', kind: 'text', roles: ['entity', 'filter'], datasetIds: ['sales_fact', 'ads'] },
  { code: 'category', name: 'Категория', kind: 'text', roles: ['entity', 'filter'], datasetIds: ['sales_fact', 'ads'] },
  { code: 'entry_point', name: 'Точка входа', kind: 'text', roles: ['entity', 'filter'], datasetIds: ['sales_fact', 'ads'] },
  { code: 'keyword_cluster', name: 'Кластер РК', kind: 'text', roles: ['entity', 'filter'], datasetIds: ['ads'] },
  { code: 'search_query', name: 'Поисковый запрос', kind: 'text', roles: ['entity', 'filter'], datasetIds: ['ads'] },

  { code: 'revenue', name: 'Выручка', kind: 'number', roles: ['axis', 'filter'], datasetIds: ['sales_fact', 'ads'] },
  { code: 'orders', name: 'Заказы', kind: 'number', roles: ['axis', 'filter'], datasetIds: ['sales_fact', 'ads'] },
  { code: 'spend', name: 'Расход', kind: 'number', roles: ['axis', 'filter'], datasetIds: ['sales_fact', 'ads'] },
  { code: 'drr', name: 'ДРР', kind: 'number', roles: ['axis', 'filter'], datasetIds: ['sales_fact', 'ads'] },
  { code: 'roi', name: 'ROI', kind: 'number', roles: ['axis', 'filter'], datasetIds: ['sales_fact', 'ads'] },
  { code: 'cr2', name: 'CR2', kind: 'number', roles: ['axis', 'filter'], datasetIds: ['ads'] },
  { code: 'position', name: 'Место в выдаче', kind: 'number', roles: ['axis', 'filter'], datasetIds: ['ads'] },
  { code: 'search_share', name: 'Доля поиска', kind: 'number', roles: ['axis', 'filter'], datasetIds: ['ads'] },
  { code: 'shelf_share', name: 'Доля полок', kind: 'number', roles: ['axis', 'filter'], datasetIds: ['ads'] },

  { code: 'date', name: 'Дата', kind: 'date', roles: ['filter'], datasetIds: ['sales_fact', 'ads'] },
];

const subjects = ['Кроссовки', 'Куртки', 'Аксессуары', 'Джинсы'];
const brands = ['Север', 'Лайм', 'Пульс', 'Орион'];
const categories = ['Одежда', 'Обувь', 'Дом'];
const entryPoints = ['Поиск', 'Полки', 'Категория'];
const keywordClusters = ['Бренд', 'Конкуренты', 'Широкий спрос'];

function rnd(min: number, max: number): number {
  return min + Math.random() * (max - min);
}

function dateAgo(daysAgo: number): string {
  const d = new Date();
  d.setDate(d.getDate() - daysAgo);
  return d.toISOString().slice(0, 10);
}

export function generateShowcaseRows(count = 200): ShowcaseRow[] {
  const rows: ShowcaseRow[] = [];
  const skuPool = 16 + Math.floor(Math.random() * 14);
  const campaignPool = 16 + Math.floor(Math.random() * 14);
  const archetypes = [
    { revenue: 280000, spend: 52000, orders: 620, cr2: 7.1, position: 5, searchShare: 0.82, shelfShare: 0.66 },
    { revenue: 170000, spend: 70000, orders: 300, cr2: 3.8, position: 14, searchShare: 0.58, shelfShare: 0.44 },
    { revenue: 90000, spend: 26000, orders: 210, cr2: 2.1, position: 26, searchShare: 0.33, shelfShare: 0.24 },
    { revenue: 52000, spend: 38000, orders: 95, cr2: 0.9, position: 39, searchShare: 0.16, shelfShare: 0.12 }
  ];

  for (let i = 0; i < count; i += 1) {
    const cluster = i % archetypes.length;
    const base = archetypes[cluster];

    const revenue = Math.max(5000, Math.round(base.revenue + rnd(-42000, 48000)));
    const spend = Math.max(1500, Math.round(base.spend + rnd(-14000, 16000)));
    const orders = Math.max(1, Math.round(base.orders + rnd(-95, 110)));

    const roi = Number((revenue / Math.max(spend, 1)).toFixed(2));
    const drr = Number(((spend / Math.max(revenue, 1)) * 100).toFixed(2));

    rows.push({
      id: `row-${i + 1}`,
      sku: `SKU ${String(1 + (i % skuPool)).padStart(4, '0')}`,
      campaign_id: `РК ${1000 + (i % campaignPool)}`,
      subject: subjects[i % subjects.length],
      brand: brands[i % brands.length],
      category: categories[i % categories.length],
      entry_point: entryPoints[i % entryPoints.length],
      keyword_cluster: keywordClusters[i % keywordClusters.length],
      search_query: `запрос ${1 + (i % 35)}`,
      revenue,
      orders,
      spend,
      drr,
      roi,
      cr2: Number((Math.max(0.2, base.cr2 + rnd(-1.4, 1.8))).toFixed(2)),
      position: Math.max(1, Math.round(base.position + rnd(-6, 7))),
      search_share: Number((Math.max(0.02, Math.min(0.98, base.searchShare + rnd(-0.12, 0.12)))).toFixed(3)),
      shelf_share: Number((Math.max(0.02, Math.min(0.95, base.shelfShare + rnd(-0.10, 0.10)))).toFixed(3)),
      date: dateAgo(i % 30),
    });
  }
  return rows;
}

function createState(): ShowcaseState {
  return {
    datasets,
    fields,
    rows: generateShowcaseRows(440),
  };
}

export const showcaseStore = writable<ShowcaseState>(createState());

export function regenerateShowcase(): void {
  showcaseStore.update((prev) => ({ ...prev, rows: generateShowcaseRows(440) }));
}

export function fieldName(code: string): string {
  return fields.find((field) => field.code === code)?.name ?? code;
}
