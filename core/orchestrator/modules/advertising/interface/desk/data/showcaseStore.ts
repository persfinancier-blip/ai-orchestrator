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
  for (let i = 0; i < count; i += 1) {
    const cluster = i % 3;
    const revenueBase = cluster === 0 ? 180000 : cluster === 1 ? 70000 : 120000;
    const spendBase = cluster === 0 ? 36000 : cluster === 1 ? 32000 : 42000;
    const revenue = Math.round(revenueBase + rnd(-15000, 18000));
    const spend = Math.round(spendBase + rnd(-6000, 6000));
    const orders = Math.round((cluster === 0 ? 430 : cluster === 1 ? 180 : 280) + rnd(-35, 40));

    const roi = Number((revenue / Math.max(spend, 1)).toFixed(2));
    const drr = Number(((spend / Math.max(revenue, 1)) * 100).toFixed(2));

    rows.push({
      id: `row-${i + 1}`,
      sku: `SKU ${String(1 + (i % 20)).padStart(4, '0')}`,
      campaign_id: `РК ${1000 + (i % 20)}`,
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
      cr2: Number((rnd(0.5, 8.5)).toFixed(2)),
      position: Math.round(rnd(2, 45)),
      search_share: Number(rnd(0.2, 0.9).toFixed(3)),
      shelf_share: Number(rnd(0.1, 0.7).toFixed(3)),
      date: dateAgo(i % 30),
    });
  }
  return rows;
}

function createState(): ShowcaseState {
  return {
    datasets,
    fields,
    rows: generateShowcaseRows(220),
  };
}

export const showcaseStore = writable<ShowcaseState>(createState());

export function regenerateShowcase(): void {
  showcaseStore.update((prev) => ({ ...prev, rows: generateShowcaseRows(220) }));
}

export function fieldName(code: string): string {
  return fields.find((field) => field.code === code)?.name ?? code;
}
