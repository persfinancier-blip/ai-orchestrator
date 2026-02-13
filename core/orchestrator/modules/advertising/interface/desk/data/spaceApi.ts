import type { DatasetId } from './showcase';

export type FilterType = 'date' | 'text' | 'number';

export interface PanelFilter {
  id: string;
  filterType: FilterType;
  fieldCode: string;
  operator: string;
  valueA: string;
  valueB: string;
}

export interface SpacePoint {
  id: string;
  entityType: 'sku' | 'campaign';
  entityKey: string;
  name: string;
  date: string;
  sku: string;
  campaign_id: string;
  subject: string;
  category: string;
  brand: string;
  entry_point: string;
  keyword_cluster: string;
  search_query: string;
  metrics: Record<string, number>;
}

interface SpaceResponse {
  points: SpacePoint[];
}

export async function loadSpacePoints(params: {
  entityField: string;
  datasets: DatasetId[];
  x: string;
  y: string;
  z: string;
  period: string;
  fromDate?: string;
  toDate?: string;
  filters: PanelFilter[];
}): Promise<SpacePoint[]> {
  const q = new URLSearchParams({
    entityField: params.entityField,
    datasets: params.datasets.join(','),
    x: params.x,
    y: params.y,
    z: params.z,
    period: params.period,
    filters: JSON.stringify(params.filters),
  });
  if (params.fromDate) q.set('fromDate', params.fromDate);
  if (params.toDate) q.set('toDate', params.toDate);

  const response = await fetch(`/ai-orchestrator/api/space?${q.toString()}`);
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  const payload = (await response.json()) as SpaceResponse;
  return payload.points;
}
