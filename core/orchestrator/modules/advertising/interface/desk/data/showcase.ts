export type DatasetId = 'sales_fact' | 'ads';
export type FieldType = 'dimension' | 'measure' | 'time';
export type ValueType = 'string' | 'number' | 'date';
export type FieldRole = 'entity' | 'axis' | 'filter';

export interface DatasetDef {
  id: DatasetId;
  name: string;
}

export interface FieldDef {
  code: string;
  name: string;
  fieldType: FieldType;
  valueType: ValueType;
  allowedRoles: FieldRole[];
  datasetIds: DatasetId[];
}

export const DATASETS: DatasetDef[] = [
  { id: 'sales_fact', name: 'Факт продаж' },
  { id: 'ads', name: 'Реклама' },
];

export const FIELDS: FieldDef[] = [
  { code: 'sku', name: 'Артикул', fieldType: 'dimension', valueType: 'string', allowedRoles: ['entity', 'filter'], datasetIds: ['sales_fact'] },
  { code: 'subject', name: 'Предмет', fieldType: 'dimension', valueType: 'string', allowedRoles: ['entity', 'filter'], datasetIds: ['sales_fact', 'ads'] },
  { code: 'category', name: 'Категория', fieldType: 'dimension', valueType: 'string', allowedRoles: ['entity', 'filter'], datasetIds: ['sales_fact', 'ads'] },
  { code: 'brand', name: 'Бренд', fieldType: 'dimension', valueType: 'string', allowedRoles: ['entity', 'filter'], datasetIds: ['sales_fact', 'ads'] },
  { code: 'campaign_id', name: 'Рекламная кампания (РК)', fieldType: 'dimension', valueType: 'string', allowedRoles: ['entity', 'filter'], datasetIds: ['ads'] },
  { code: 'entry_point', name: 'Точка входа', fieldType: 'dimension', valueType: 'string', allowedRoles: ['filter'], datasetIds: ['sales_fact', 'ads'] },
  { code: 'keyword_cluster', name: 'Кластер РК', fieldType: 'dimension', valueType: 'string', allowedRoles: ['filter'], datasetIds: ['ads'] },
  { code: 'search_query', name: 'Поисковый запрос', fieldType: 'dimension', valueType: 'string', allowedRoles: ['filter'], datasetIds: ['ads'] },

  { code: 'revenue', name: 'Выручка', fieldType: 'measure', valueType: 'number', allowedRoles: ['axis', 'filter'], datasetIds: ['sales_fact', 'ads'] },
  { code: 'orders', name: 'Заказы', fieldType: 'measure', valueType: 'number', allowedRoles: ['axis', 'filter'], datasetIds: ['sales_fact', 'ads'] },
  { code: 'spend', name: 'Расход', fieldType: 'measure', valueType: 'number', allowedRoles: ['axis', 'filter'], datasetIds: ['sales_fact', 'ads'] },
  { code: 'drr', name: 'ДРР', fieldType: 'measure', valueType: 'number', allowedRoles: ['axis', 'filter'], datasetIds: ['sales_fact', 'ads'] },
  { code: 'roi', name: 'ROI', fieldType: 'measure', valueType: 'number', allowedRoles: ['axis', 'filter'], datasetIds: ['sales_fact', 'ads'] },
  { code: 'cr0', name: 'CR0', fieldType: 'measure', valueType: 'number', allowedRoles: ['axis', 'filter'], datasetIds: ['ads'] },
  { code: 'cr1', name: 'CR1', fieldType: 'measure', valueType: 'number', allowedRoles: ['axis', 'filter'], datasetIds: ['ads'] },
  { code: 'cr2', name: 'CR2', fieldType: 'measure', valueType: 'number', allowedRoles: ['axis', 'filter'], datasetIds: ['sales_fact', 'ads'] },
  { code: 'position', name: 'Место в выдаче', fieldType: 'measure', valueType: 'number', allowedRoles: ['axis', 'filter'], datasetIds: ['ads'] },
  { code: 'stock_days', name: 'Остаток, дни', fieldType: 'measure', valueType: 'number', allowedRoles: ['axis', 'filter'], datasetIds: ['sales_fact'] },
  { code: 'search_share', name: 'Доля поиска', fieldType: 'measure', valueType: 'number', allowedRoles: ['axis', 'filter'], datasetIds: ['ads'] },
  { code: 'shelf_share', name: 'Доля полок', fieldType: 'measure', valueType: 'number', allowedRoles: ['axis', 'filter'], datasetIds: ['ads'] },

  { code: 'date', name: 'Дата', fieldType: 'time', valueType: 'date', allowedRoles: ['filter'], datasetIds: ['sales_fact', 'ads'] },
];

export function fieldLabel(code: string): string {
  return FIELDS.find((field) => field.code === code)?.name ?? code;
}
