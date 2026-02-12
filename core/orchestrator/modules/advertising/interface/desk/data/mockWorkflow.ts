export type FilterRule = { id: string; field: string; operator: string; value: string };

export type WorkflowState = {
  workflowName: string;
  dataCloud: string;
  filters: FilterRule[];
  conditionDrr: number;
  conditionRoi: number;
  conditionStockDays: number;
  action: 'Создать SPS/SPM' | 'Изменить ставки' | 'Изменить бюджет' | 'Остановить РК';
  schedule: '15m' | '1h' | '6h' | '24h';
};

export type CommandLogItem = {
  id: string;
  time: string;
  workflowName: string;
  action: string;
  affectedCount: number;
  status: 'СИМУЛЯЦИЯ';
};

export const dataCloudOptions = ['Витрина рекламы за день', 'Типология покупателей', 'Граф точек входа', 'KPI по рекламе SKU'];

export const initialWorkflow: WorkflowState = {
  workflowName: 'Сценарий A',
  dataCloud: dataCloudOptions[0],
  filters: [
    { id: 'f1', field: 'subject', operator: 'IN', value: 'Кроссовки' },
    { id: 'f2', field: 'stockDays', operator: '>', value: '30' },
  ],
  conditionDrr: 18,
  conditionRoi: 1.2,
  conditionStockDays: 30,
  action: 'Изменить ставки',
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
    entryPoint: ['Поиск', 'Полка', 'Категория'][i % 3],
    targetSegment: ['Премиум', 'Чувствительные к цене', 'Спорт'][i % 3],
  }));
}

export function newLog(workflowName: string, action: string, affectedCount: number): CommandLogItem {
  return {
    id: String(Date.now() + Math.random()),
    time: new Date().toLocaleDateString('ru-RU') + ' ' + new Date().toLocaleTimeString('ru-RU'),
    workflowName,
    action,
    affectedCount,
    status: 'СИМУЛЯЦИЯ',
  };
}
