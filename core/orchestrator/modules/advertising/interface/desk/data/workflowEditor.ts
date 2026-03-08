export type SourceGroup = 'api_requests' | 'data_tables' | 'math_calculations';
export type ToolType = 'start_process' | 'schedule_process' | 'api_request' | 'table_parser' | 'db_write' | 'end_process';

export type ApiRequestTemplate = {
  method: string;
  url: string;
  authMode: string;
  headers: Record<string, any>;
  query: Record<string, any>;
  body: any;
};

export type SourceItem = {
  id: string;
  name: string;
  group: SourceGroup;
  datasetId: string;
  schema: string[];
  size: number;
  preview: Array<Record<string, string | number>>;
  description: string;
  requestTemplate?: ApiRequestTemplate;
};

export type ToolItem = {
  id: string;
  name: string;
  toolType: ToolType;
  description: string;
};

const mkPreview = (prefix: string, rows = 10): Array<Record<string, string | number>> =>
  Array.from({ length: rows }).map((_, idx) => ({
    entity_id: `${prefix}_${idx + 1}`,
    revenue: 1000 + idx * 230,
    spend: 400 + idx * 120,
    ctr: Number((1.5 + idx * 0.12).toFixed(2))
  }));

export const sourceGroups: Array<{ key: SourceGroup; label: string }> = [
  { key: 'api_requests', label: 'API запросы' },
  { key: 'data_tables', label: 'Текущие таблицы данных' },
  { key: 'math_calculations', label: 'Математические расчеты' }
];

export const sourceItemsByGroup: Record<SourceGroup, SourceItem[]> = {
  api_requests: [
    {
      id: 'api_1',
      name: 'WB: карточки товаров',
      group: 'api_requests',
      datasetId: 'api:wildberries.cards',
      schema: ['nmID', 'updatedAt', 'subjectName'],
      size: 120,
      preview: mkPreview('wb_cards'),
      description: 'Ответ API по карточкам товаров'
    },
    {
      id: 'api_2',
      name: 'Ozon: рекламные кампании',
      group: 'api_requests',
      datasetId: 'api:ozon.campaigns',
      schema: ['campaign_id', 'status', 'budget'],
      size: 48,
      preview: mkPreview('ozon_adv'),
      description: 'Ответ API по рекламным кампаниям'
    },
    {
      id: 'api_3',
      name: 'Внешний справочник',
      group: 'api_requests',
      datasetId: 'api:external.dictionary',
      schema: ['key', 'value', 'updated_at'],
      size: 560,
      preview: mkPreview('dict'),
      description: 'Справочные данные из внешнего API'
    }
  ],
  data_tables: [
    {
      id: 'tb_1',
      name: 'bronze_client',
      group: 'data_tables',
      datasetId: 'db:bronze_client',
      schema: ['client_id', 'token', 'shop_name'],
      size: 2000,
      preview: mkPreview('client'),
      description: 'Текущая таблица клиентов и токенов'
    },
    {
      id: 'tb_2',
      name: 'bronze_adv',
      group: 'data_tables',
      datasetId: 'db:bronze_adv',
      schema: ['campaign_id', 'client_id', 'status'],
      size: 4300,
      preview: mkPreview('adv'),
      description: 'Текущая таблица рекламных кампаний'
    },
    {
      id: 'tb_3',
      name: 'bronze_sku',
      group: 'data_tables',
      datasetId: 'db:bronze_sku',
      schema: ['sku', 'campaign_id', 'client_id'],
      size: 18200,
      preview: mkPreview('sku'),
      description: 'Текущая таблица SKU'
    }
  ],
  math_calculations: [
    {
      id: 'mt_1',
      name: 'Расчет DRR',
      group: 'math_calculations',
      datasetId: 'calc:drr',
      schema: ['entity_id', 'spend', 'revenue', 'drr'],
      size: 1000,
      preview: mkPreview('drr'),
      description: 'Подготовленный набор для расчета DRR'
    },
    {
      id: 'mt_2',
      name: 'Расчет unit-экономики',
      group: 'math_calculations',
      datasetId: 'calc:unit_economics',
      schema: ['entity_id', 'margin', 'logistic_cost', 'profit'],
      size: 750,
      preview: mkPreview('unit'),
      description: 'Подготовленный набор для расчета маржи и прибыли'
    },
    {
      id: 'mt_3',
      name: 'Расчет сезонных индексов',
      group: 'math_calculations',
      datasetId: 'calc:seasonality',
      schema: ['entity_id', 'period', 'season_index'],
      size: 365,
      preview: mkPreview('season'),
      description: 'Подготовленные коэффициенты сезонности'
    }
  ]
};

export const tools: ToolItem[] = [
  {
    id: 'tool_start',
    name: 'Старт процесса',
    toolType: 'start_process',
    description: 'Точка запуска цепочки'
  },
  {
    id: 'tool_schedule',
    name: 'Расписание процесса',
    toolType: 'schedule_process',
    description: 'Настройка частоты и окна запуска'
  },
  {
    id: 'tool_api_request',
    name: 'API Request',
    toolType: 'api_request',
    description: 'HTTP-запрос к внешнему API'
  },
  {
    id: 'tool_parser',
    name: 'Парсер данных',
    toolType: 'table_parser',
    description: 'Парсинг, нормализация и подготовка строк для следующей ноды'
  },
  {
    id: 'tool_db_write',
    name: 'Запись в БД',
    toolType: 'db_write',
    description: 'Сохранение результата в таблицу БД'
  },
  {
    id: 'tool_end',
    name: 'Конец процесса',
    toolType: 'end_process',
    description: 'Точка завершения цепочки'
  }
];

export const toolPorts: Record<ToolType, { inputs: string[]; outputs: string[] }> = {
  start_process: { inputs: ['in'], outputs: ['out'] },
  schedule_process: { inputs: ['in'], outputs: ['out'] },
  api_request: { inputs: ['in'], outputs: ['out'] },
  table_parser: { inputs: ['in'], outputs: ['out'] },
  db_write: { inputs: ['in'], outputs: ['out'] },
  end_process: { inputs: ['in'], outputs: [] }
};
