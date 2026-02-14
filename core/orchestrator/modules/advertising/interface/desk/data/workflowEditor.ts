export type SourceType = 'graph' | 'datamart' | 'cohort';
export type ToolType = 'filter' | 'join' | 'aggregate' | 'score' | 'export';

export type CohortDefinition = {
  metric: string;
  operator: string;
  value: number;
  period: string;
  entity: string;
};

export type SourceItem = {
  id: string;
  name: string;
  sourceType: SourceType;
  datasetId: string;
  schema: string[];
  size: number;
  preview: Array<Record<string, string | number>>;
  dynamic?: boolean;
  definition?: CohortDefinition;
};

export type ToolItem = {
  id: string;
  name: string;
  toolType: ToolType;
};

const mkPreview = (prefix: string): Array<Record<string, string | number>> =>
  Array.from({ length: 10 }).map((_, idx) => ({
    entity: `${prefix}-${idx + 1}`,
    revenue: Math.round(1000 + Math.random() * 25000),
    spend: Math.round(500 + Math.random() * 12000),
    ctr: Number((1 + Math.random() * 9).toFixed(2)),
  }));

export const graphSources: SourceItem[] = [
  {
    id: 'g1',
    name: 'Selected points (выбранные точки)',
    sourceType: 'graph',
    datasetId: 'graph:selected_points',
    schema: ['sku', 'revenue', 'spend', 'ctr'],
    size: 124,
    preview: mkPreview('point'),
  },
  {
    id: 'g2',
    name: 'Group / Cluster (группа / кластер)',
    sourceType: 'graph',
    datasetId: 'graph:cluster',
    schema: ['cluster', 'sku', 'orders'],
    size: 68,
    preview: mkPreview('cluster'),
  },
  {
    id: 'g3',
    name: 'Entity set (набор сущностей)',
    sourceType: 'graph',
    datasetId: 'graph:entity_set',
    schema: ['sku', 'subject', 'campaign_id'],
    size: 412,
    preview: mkPreview('entity'),
  },
  {
    id: 'g4',
    name: 'Campaign set (набор РК)',
    sourceType: 'graph',
    datasetId: 'graph:campaign_set',
    schema: ['campaign_id', 'spend', 'roi'],
    size: 37,
    preview: mkPreview('campaign'),
  },
];

export const datamartSources: SourceItem[] = [
  {
    id: 'd1',
    name: 'SKU metrics',
    sourceType: 'datamart',
    datasetId: 'dm:sku_metrics',
    schema: ['sku', 'revenue', 'spend', 'drr'],
    size: 1800,
    preview: mkPreview('sku'),
  },
  {
    id: 'd2',
    name: 'Campaign metrics',
    sourceType: 'datamart',
    datasetId: 'dm:campaign_metrics',
    schema: ['campaign_id', 'spend', 'clicks'],
    size: 230,
    preview: mkPreview('campaign'),
  },
  {
    id: 'd3',
    name: 'Search queries',
    sourceType: 'datamart',
    datasetId: 'dm:queries',
    schema: ['query', 'ctr', 'orders'],
    size: 5400,
    preview: mkPreview('query'),
  },
  {
    id: 'd4',
    name: 'Orders / Sales',
    sourceType: 'datamart',
    datasetId: 'dm:sales',
    schema: ['order_id', 'sku', 'revenue'],
    size: 9200,
    preview: mkPreview('sales'),
  },
  {
    id: 'd5',
    name: 'Traffic sources',
    sourceType: 'datamart',
    datasetId: 'dm:traffic',
    schema: ['source', 'sessions', 'cvr'],
    size: 130,
    preview: mkPreview('traffic'),
  },
];

export const cohortSources: SourceItem[] = [
  {
    id: 'c1',
    name: 'Когорта: Падение CTR (7 дней)',
    sourceType: 'cohort',
    datasetId: 'cohort:ctr_drop_7d',
    schema: ['sku', 'ctr_delta', 'clicks'],
    size: 211,
    preview: mkPreview('ctr-drop'),
    dynamic: true,
    definition: { metric: 'CTR', operator: '<', value: -15, period: '7d', entity: 'sku' },
  },
  {
    id: 'c2',
    name: 'Когорта: Рост расхода > 20%',
    sourceType: 'cohort',
    datasetId: 'cohort:spend_growth',
    schema: ['sku', 'spend_delta', 'campaign_id'],
    size: 144,
    preview: mkPreview('spend-growth'),
    dynamic: true,
    definition: { metric: 'SPEND', operator: '>', value: 20, period: '7d', entity: 'sku' },
  },
  {
    id: 'c3',
    name: 'Когорта: Новые SKU (14 дней)',
    sourceType: 'cohort',
    datasetId: 'cohort:new_sku',
    schema: ['sku', 'created_at', 'subject'],
    size: 93,
    preview: mkPreview('new-sku'),
    dynamic: false,
    definition: { metric: 'SKU_CREATED', operator: '>=', value: 1, period: '14d', entity: 'sku' },
  },
  {
    id: 'c4',
    name: 'Когорта: ДРР > 25%',
    sourceType: 'cohort',
    datasetId: 'cohort:drr_gt_25',
    schema: ['sku', 'drr', 'revenue'],
    size: 287,
    preview: mkPreview('drr'),
    dynamic: true,
    definition: { metric: 'ДРР', operator: '>', value: 25, period: '7d', entity: 'sku' },
  },
  {
    id: 'c5',
    name: 'Когорта: Поиск → низкая конверсия',
    sourceType: 'cohort',
    datasetId: 'cohort:search_low_cvr',
    schema: ['query', 'cvr', 'orders'],
    size: 166,
    preview: mkPreview('low-cvr'),
    dynamic: true,
    definition: { metric: 'CVR', operator: '<', value: -10, period: '7d', entity: 'query' },
  },
];

export const tools: ToolItem[] = [
  { id: 't1', name: 'Filter', toolType: 'filter' },
  { id: 't2', name: 'Join/Match', toolType: 'join' },
  { id: 't3', name: 'Aggregate', toolType: 'aggregate' },
  { id: 't4', name: 'Score/Similarity', toolType: 'score' },
  { id: 't5', name: 'Export', toolType: 'export' },
];

export const toolPorts: Record<ToolType, { inputs: string[]; outputs: string[] }> = {
  filter: { inputs: ['in'], outputs: ['out'] },
  join: { inputs: ['left', 'right'], outputs: ['out'] },
  aggregate: { inputs: ['in'], outputs: ['out'] },
  score: { inputs: ['in'], outputs: ['out'] },
  export: { inputs: ['in'], outputs: [] },
};
