export type FilterRule = { id: string; field: string; operator: string; value: string };

export type WorkflowState = {
  workflowName: string;
  dataCloud: string;
  filters: FilterRule[];
  conditionDrr: number;
  conditionRoi: number;
  conditionStockDays: number;
  action: 'Create SPS/SPM' | 'Change bids' | 'Change budget' | 'Pause campaign';
  schedule: '15m' | '1h' | '6h' | '24h';
};

export type CommandLogItem = {
  id: string;
  time: string;
  workflowName: string;
  action: string;
  affectedCount: number;
  status: 'SIMULATED';
};

export const dataCloudOptions = ['ad_mart_daily', 'customer_typology_v2', 'entry_point_graph', 'sku_ads_kpi'];

export const initialWorkflow: WorkflowState = {
  workflowName: 'Desk Flow A',
  dataCloud: dataCloudOptions[0],
  filters: [
    { id: 'f1', field: 'subject', operator: 'IN', value: 'Кроссовки' },
    { id: 'f2', field: 'stockDays', operator: '>', value: '30' },
  ],
  conditionDrr: 18,
  conditionRoi: 1.2,
  conditionStockDays: 30,
  action: 'Change bids',
  schedule: '1h',
};

export function generatePivotRows(count = 20): Array<Record<string, string | number>> {
  return Array.from({ length: count }).map((_, i) => ({
    sku: `SKU-${100200 + i}`,
    spend: Math.round(1200 + Math.random() * 35000),
    orders: Math.round(4 + Math.random() * 120),
    drr: Number((5 + Math.random() * 30).toFixed(2)),
    roi: Number((0.6 + Math.random() * 2.8).toFixed(2)),
    stockDays: Math.round(8 + Math.random() * 80),
    entryPoint: ['Search', 'Shelf', 'Category'][i % 3],
    targetSegment: ['premium', 'price-sensitive', 'спорт'][i % 3],
  }));
}

export function newLog(workflowName: string, action: string, affectedCount: number): CommandLogItem {
  return {
    id: String(Date.now() + Math.random()),
    time: new Date().toLocaleTimeString(),
    workflowName,
    action,
    affectedCount,
    status: 'SIMULATED',
  };
}
