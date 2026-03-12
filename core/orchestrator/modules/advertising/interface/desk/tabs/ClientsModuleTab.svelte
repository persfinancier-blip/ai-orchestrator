<script lang="ts">
  import { onMount } from 'svelte';
  import ClientRowsEditor, { type ClientFieldConfig, type OptionItem } from '../components/ClientRowsEditor.svelte';
  import { CLIENT_MODULE_SECTION_DEFINITIONS, clientModuleSourceMeta } from '../../shared/clientModuleTemplates.mjs';

  type ExistingTable = { schema_name: string; table_name: string };
  type ClientListItem = {
    id: number;
    client_display_name: string;
    client_code: string;
    status: string;
    comment: string;
    platform_summary: string;
    active_goal_count: number;
    active_kpi_count: number;
    active_access_count: number;
    warning_count: number;
  };
  type SourceMeta = { key?: string; template_name: string; schema_name: string; table_name: string };
  type ClientDetailPayload = {
    client: ClientListItem;
    mainData: Record<string, any>;
    legalEntities: Array<Record<string, any>>;
    contracts: Array<Record<string, any>>;
    paymentTerms: Array<Record<string, any>>;
    paymentSchedule: Array<Record<string, any>>;
    goals: Array<Record<string, any>>;
    kpis: Array<Record<string, any>>;
    accesses: Array<Record<string, any>>;
    constraints: Array<Record<string, any>>;
    summaryMetrics: Record<string, any>;
    actionItems: Array<Record<string, any>>;
    sources: Record<string, any>;
    summary: Record<string, any>;
    options: Record<string, OptionItem[]>;
  };
  type TabKey =
    | 'main_data'
    | 'legal_entities'
    | 'contracts'
    | 'payment_terms'
    | 'payment_schedule'
    | 'goals'
    | 'kpis'
    | 'accesses';

  export let apiBase: string;
  export let apiJson: <T = any>(url: string, init?: RequestInit) => Promise<T>;
  export let headers: () => Record<string, string>;
  export let existingTables: ExistingTable[] = [];

  const FALLBACK_SOURCES = clientModuleSourceMeta();
  const TABS: Array<{ key: TabKey; title: string; sourceKey: string }> = [
    { key: 'main_data', title: 'Основные данные', sourceKey: 'main_data' },
    { key: 'legal_entities', title: 'Юр. лица', sourceKey: 'legal_entities' },
    { key: 'contracts', title: 'Договоры', sourceKey: 'contracts' },
    { key: 'payment_terms', title: 'Условия оплаты', sourceKey: 'payment_terms' },
    { key: 'payment_schedule', title: 'График платежей', sourceKey: 'payment_schedule' },
    { key: 'goals', title: 'Цели', sourceKey: 'goals' },
    { key: 'kpis', title: 'KPI', sourceKey: 'kpis' },
    { key: 'accesses', title: 'Платформы и доступы', sourceKey: 'accesses' }
  ];

  const STATUS_OPTIONS: OptionItem[] = [
    { value: 'draft', label: 'Черновик' },
    { value: 'active', label: 'Активен' },
    { value: 'setup', label: 'Требует настройки' },
    { value: 'paused', label: 'На паузе' },
    { value: 'archived', label: 'Архив' }
  ];
  const FEE_TYPE_OPTIONS: OptionItem[] = [
    { value: 'fixed', label: 'Фикс' },
    { value: 'percent', label: 'Процент' },
    { value: 'mixed', label: 'Фикс + процент' }
  ];
  const PERCENT_BASE_OPTIONS: OptionItem[] = [
    { value: 'ad_budget', label: 'Рекламный бюджет' },
    { value: 'revenue', label: 'Выручка' },
    { value: 'ad_revenue', label: 'Рекламная выручка' },
    { value: 'gross_profit', label: 'Валовая прибыль' },
    { value: 'orders', label: 'Заказы' },
    { value: 'other', label: 'Другое' }
  ];
  const CALC_PERIOD_OPTIONS: OptionItem[] = [
    { value: 'weekly', label: 'Неделя' },
    { value: 'monthly', label: 'Месяц' },
    { value: 'quarterly', label: 'Квартал' }
  ];
  const PAYMENT_FREQUENCY_OPTIONS: OptionItem[] = [
    { value: 'once', label: 'Разово' },
    { value: 'weekly', label: 'Еженедельно' },
    { value: 'monthly', label: 'Ежемесячно' },
    { value: 'quarterly', label: 'Ежеквартально' }
  ];
  const PRIORITY_OPTIONS: OptionItem[] = [
    { value: '1', label: '1 — высокий' },
    { value: '2', label: '2 — выше среднего' },
    { value: '3', label: '3 — средний' },
    { value: '4', label: '4 — ниже среднего' },
    { value: '5', label: '5 — низкий' }
  ];
  const KPI_TYPE_OPTIONS: OptionItem[] = [
    { value: 'revenue', label: 'Выручка' },
    { value: 'orders', label: 'Заказы' },
    { value: 'drr', label: 'ДРР' },
    { value: 'roas', label: 'ROAS' },
    { value: 'profit', label: 'Прибыль' }
  ];
  const PLATFORM_OPTIONS: OptionItem[] = [
    { value: 'wildberries', label: 'Wildberries' },
    { value: 'ozon', label: 'Ozon' },
    { value: 'yandex_market', label: 'Яндекс Маркет' },
    { value: 'ozon_performance', label: 'Ozon Performance' },
    { value: 'mpstats', label: 'MPStats' },
    { value: 'mpguru', label: 'MPGuru' }
  ];
  const AUTH_TYPE_OPTIONS: OptionItem[] = [
    { value: 'token', label: 'Токен' },
    { value: 'login_password', label: 'Логин и пароль' },
    { value: 'api_key', label: 'API-ключ' }
  ];
  const ACCESS_MODE_OPTIONS: OptionItem[] = [
    { value: 'read', label: 'Только чтение' },
    { value: 'write', label: 'Чтение и запись' },
    { value: 'admin', label: 'Полный доступ' }
  ];
  const MAIN_DATA_FIELDS: ClientFieldConfig[] = [
    { key: 'client_code', label: 'Код партнера', placeholder: 'fresh_market' },
    { key: 'client_display_name', label: 'Название партнера', placeholder: 'Свежий Рынок' },
    { key: 'status', label: 'Статус', type: 'select', options: STATUS_OPTIONS },
    { key: 'comment', label: 'Описание', type: 'textarea', rows: 4, placeholder: 'Описание' }
  ];
  const LEGAL_ENTITY_FIELDS: ClientFieldConfig[] = [
    { key: 'legal_entity_code', label: 'Код юр. лица', readOnly: true, disabled: true },
    { key: 'tax_id', label: 'ИНН' },
    {
      key: 'is_active',
      label: 'Статус',
      type: 'select',
      selectValueType: 'boolean',
      options: [
        { value: 'true', label: 'Активно' },
        { value: 'false', label: 'Неактивно' }
      ]
    },
    { key: 'legal_entity_name', label: 'Название юр. лица', colSpan: 2 },
    { key: 'country', label: 'Страна' },
    { key: 'comment', label: 'Описание', type: 'textarea', rows: 3, colSpan: 3 }
  ];
  const CONTRACT_FIELDS: ClientFieldConfig[] = [
    { key: 'contract_name', label: 'Название договора', colSpan: 2 },
    { key: 'contract_number', label: 'Номер договора' },
    { key: 'status', label: 'Статус', type: 'select', options: STATUS_OPTIONS },
    { key: 'contract_date', label: 'Дата договора', type: 'date' },
    { key: 'date_start', label: 'Начало действия', type: 'date' },
    { key: 'date_end', label: 'Дата окончания', type: 'date' },
    { key: 'legal_entity_id', label: 'Юр.лицо заказчик', type: 'select', optionsKey: 'legalEntities' },
    { key: 'file_url', label: 'Ссылка на договор' },
    { key: 'comment', label: 'Описание', type: 'textarea', rows: 3, colSpan: 3 }
  ];
  const PAYMENT_TERM_FIELDS: ClientFieldConfig[] = [
    { key: 'contract_id', label: 'Договор', type: 'select', optionsKey: 'contracts' },
    { key: 'fee_type', label: 'Тип оплаты', type: 'select', options: FEE_TYPE_OPTIONS },
    { key: 'fixed_amount', label: 'Фиксированная сумма', type: 'number', step: '0.01' },
    { key: 'percent_value', label: 'Процент', type: 'number', step: '0.01' },
    { key: 'percent_base', label: 'База процента', type: 'select', options: PERCENT_BASE_OPTIONS },
    { key: 'min_amount', label: 'Минимальная сумма', type: 'number', step: '0.01' },
    { key: 'max_amount', label: 'Максимальная сумма', type: 'number', step: '0.01' },
    { key: 'calc_period', label: 'Период расчёта', type: 'select', options: CALC_PERIOD_OPTIONS },
    { key: 'is_active', label: 'Активно', type: 'checkbox' },
    { key: 'date_start', label: 'Начало действия', type: 'date' },
    { key: 'date_end', label: 'Окончание действия', type: 'date' },
    { key: 'comment', label: 'Комментарий', type: 'textarea', rows: 3 }
  ];
  const PAYMENT_SCHEDULE_FIELDS: ClientFieldConfig[] = [
    { key: 'payment_term_id', label: 'Условие оплаты', type: 'select', optionsKey: 'paymentTerms' },
    { key: 'payment_type', label: 'Тип платежа' },
    { key: 'frequency', label: 'Частота', type: 'select', options: PAYMENT_FREQUENCY_OPTIONS },
    { key: 'due_day', label: 'День оплаты', type: 'number', min: 1, step: 1 },
    { key: 'amount', label: 'Сумма', type: 'number', step: '0.01' },
    { key: 'calc_rule', label: 'Правило расчёта' },
    { key: 'status', label: 'Статус', type: 'select', options: STATUS_OPTIONS },
    { key: 'comment', label: 'Комментарий', type: 'textarea', rows: 3 }
  ];
  const GOAL_FIELDS: ClientFieldConfig[] = [
    { key: 'goal_name', label: 'Название цели' },
    { key: 'goal_type', label: 'Тип цели' },
    { key: 'priority', label: 'Приоритет', type: 'select', options: PRIORITY_OPTIONS },
    { key: 'date_start', label: 'Начало', type: 'date' },
    { key: 'date_end', label: 'Окончание', type: 'date' },
    { key: 'is_active', label: 'Активна', type: 'checkbox' },
    { key: 'comment', label: 'Комментарий', type: 'textarea', rows: 3 }
  ];
  const KPI_FIELDS: ClientFieldConfig[] = [
    { key: 'goal_id', label: 'Цель', type: 'select', optionsKey: 'goals' },
    { key: 'kpi_name', label: 'Название KPI' },
    { key: 'kpi_type', label: 'Тип KPI', type: 'select', options: KPI_TYPE_OPTIONS },
    { key: 'target_value', label: 'Цель', type: 'number', step: '0.01' },
    { key: 'min_value', label: 'Минимум', type: 'number', step: '0.01' },
    { key: 'max_value', label: 'Максимум', type: 'number', step: '0.01' },
    { key: 'unit', label: 'Единица' },
    { key: 'evaluation_period', label: 'Период оценки' },
    { key: 'priority', label: 'Приоритет', type: 'select', options: PRIORITY_OPTIONS },
    { key: 'is_active', label: 'Активен', type: 'checkbox' },
    { key: 'comment', label: 'Комментарий', type: 'textarea', rows: 3 }
  ];
  const ACCESS_FIELDS: ClientFieldConfig[] = [
    { key: 'platform_code', label: 'Код платформы', type: 'select', options: PLATFORM_OPTIONS },
    { key: 'platform_name', label: 'Название платформы' },
    { key: 'system_name', label: 'Название в системе' },
    { key: 'external_id', label: 'Внешний ID' },
    { key: 'cabinet_name', label: 'Кабинет' },
    { key: 'cabinet_id', label: 'ID кабинета' },
    { key: 'auth_type', label: 'Тип авторизации', type: 'select', options: AUTH_TYPE_OPTIONS },
    { key: 'token_value', label: 'Токен / ключ' },
    { key: 'login_value', label: 'Логин' },
    { key: 'password_value', label: 'Пароль' },
    { key: 'api_key', label: 'API-ключ' },
    { key: 'access_scope', label: 'Область доступа' },
    { key: 'access_mode', label: 'Режим доступа', type: 'select', options: ACCESS_MODE_OPTIONS },
    { key: 'is_active', label: 'Активен', type: 'checkbox' },
    { key: 'check_status', label: 'Статус проверки' },
    { key: 'last_checked_at', label: 'Последняя проверка', type: 'datetime' },
    { key: 'expires_at', label: 'Истекает', type: 'datetime' },
    { key: 'last_error_text', label: 'Последняя ошибка', type: 'textarea', rows: 3 },
    { key: 'data_table_ref', label: 'Таблица данных' },
    { key: 'sync_table_ref', label: 'Таблица синхронизации' },
    { key: 'log_table_ref', label: 'Таблица логов' },
    { key: 'comment', label: 'Комментарий', type: 'textarea', rows: 3 }
  ];
  const MULTI_SECTION_FIELDS: Record<string, ClientFieldConfig[]> = {
    legal_entities: LEGAL_ENTITY_FIELDS,
    contracts: CONTRACT_FIELDS,
    payment_terms: PAYMENT_TERM_FIELDS,
    payment_schedule: PAYMENT_SCHEDULE_FIELDS,
    goals: GOAL_FIELDS,
    kpis: KPI_FIELDS,
    accesses: ACCESS_FIELDS
  };

  let bootstrapLoading = false;
  let listLoading = false;
  let detailLoading = false;
  let createLoading = false;
  let savingMain = false;
  let busyRowKey = '';
  let error = '';
  let info = '';
  let search = '';
  let clients: ClientListItem[] = [];
  let selectedClientId = 0;
  let deletingClientId = 0;
  let listSource: SourceMeta | null = null;
  let detail: ClientDetailPayload | null = null;
  let activeTab: TabKey = 'main_data';
  let currentTabMeta = TABS[0];
  let currentTabSource: any = null;
  let currentRows: Array<Record<string, any>> = [];
  let currentOptionSets: Record<string, OptionItem[]> = {};

  function defaultClientDetail(): ClientDetailPayload {
    return {
      client: {
        id: 0,
        client_display_name: '',
        client_code: '',
        status: 'draft',
        comment: '',
        platform_summary: '',
        active_goal_count: 0,
        active_kpi_count: 0,
        active_access_count: 0,
        warning_count: 0
      },
      mainData: { client_code: '', client_name: '', client_display_name: '', status: 'draft', comment: '' },
      legalEntities: [],
      contracts: [],
      paymentTerms: [],
      paymentSchedule: [],
      goals: [],
      kpis: [],
      accesses: [],
      constraints: [],
      summaryMetrics: {},
      actionItems: [],
      sources: {},
      summary: { goals: [], kpis: [], constraints: [], metrics: {}, actionItems: [], counts: {} },
      options: { legalEntities: [], contracts: [], paymentTerms: [], goals: [] }
    };
  }

  function normalizeDetail(payload: any): ClientDetailPayload {
    const base = defaultClientDetail();
    return {
      ...base,
      ...(payload || {}),
      client: { ...base.client, ...(payload?.client || {}) },
      mainData: { ...base.mainData, ...(payload?.mainData || {}) },
      legalEntities: Array.isArray(payload?.legalEntities) ? payload.legalEntities : [],
      contracts: Array.isArray(payload?.contracts) ? payload.contracts : [],
      paymentTerms: Array.isArray(payload?.paymentTerms) ? payload.paymentTerms : [],
      paymentSchedule: Array.isArray(payload?.paymentSchedule) ? payload.paymentSchedule : [],
      goals: Array.isArray(payload?.goals) ? payload.goals : [],
      kpis: Array.isArray(payload?.kpis) ? payload.kpis : [],
      accesses: Array.isArray(payload?.accesses) ? payload.accesses : [],
      constraints: Array.isArray(payload?.constraints) ? payload.constraints : [],
      summaryMetrics: payload?.summaryMetrics && typeof payload.summaryMetrics === 'object' ? payload.summaryMetrics : {},
      actionItems: Array.isArray(payload?.actionItems) ? payload.actionItems : [],
      sources: payload?.sources && typeof payload.sources === 'object' ? payload.sources : {},
      summary: payload?.summary && typeof payload.summary === 'object' ? payload.summary : base.summary,
      options: payload?.options && typeof payload.options === 'object' ? payload.options : base.options
    };
  }

  function uniqueLocalId(prefix: string) {
    return `${prefix}_${Date.now()}_${Math.random().toString(16).slice(2, 8)}`;
  }

  function sourceForSection(sourceKey: string) {
    const dynamic = detail?.sources?.[sourceKey];
    if (Array.isArray(dynamic)) return dynamic;
    if (dynamic && typeof dynamic === 'object') return dynamic;
    const key = (CLIENT_MODULE_SECTION_DEFINITIONS as any)?.[sourceKey]?.key || sourceKey;
    return (FALLBACK_SOURCES as any)[key] || null;
  }

  function sourceLabel(source: SourceMeta | null | undefined) {
    if (!source) return 'Источник не определён';
    return `Шаблон таблицы: ${source.template_name} • Таблица: ${source.table_name} • Схема: ${source.schema_name}`;
  }

  function optionSets() {
    return {
      legalEntities: Array.isArray(detail?.options?.legalEntities) ? detail.options.legalEntities : [],
      contracts: Array.isArray(detail?.options?.contracts) ? detail.options.contracts : [],
      paymentTerms: Array.isArray(detail?.options?.paymentTerms) ? detail.options.paymentTerms : [],
      goals: Array.isArray(detail?.options?.goals) ? detail.options.goals : []
    };
  }

  function rowKey(section: string, row: Record<string, any>, index: number) {
    return `${section}:${String(row?.id || row?.__localId || index)}`;
  }

  function sectionKey(section: TabKey): keyof ClientDetailPayload {
    const map: Record<TabKey, keyof ClientDetailPayload> = {
      main_data: 'mainData',
      legal_entities: 'legalEntities',
      contracts: 'contracts',
      payment_terms: 'paymentTerms',
      payment_schedule: 'paymentSchedule',
      goals: 'goals',
      kpis: 'kpis',
      accesses: 'accesses'
    };
    return map[section];
  }

  function sourceRows(section: TabKey) {
    if (!detail) return [];
    if (section === 'main_data') return [];
    if (section === 'legal_entities') return detail.legalEntities;
    if (section === 'contracts') return detail.contracts;
    if (section === 'payment_terms') return detail.paymentTerms;
    if (section === 'payment_schedule') return detail.paymentSchedule;
    if (section === 'goals') return detail.goals;
    if (section === 'kpis') return detail.kpis;
    if (section === 'accesses') return detail.accesses;
    return [];
  }

  function patchDetail(section: keyof ClientDetailPayload, nextRows: any) {
    if (!detail) return;
    detail = { ...detail, [section]: nextRows };
  }

  function updateMultiSection(section: TabKey, index: number, field: string, value: any) {
    const targetKey = sectionKey(section);
    const rows = Array.isArray((detail as any)?.[targetKey]) ? structuredClone((detail as any)[targetKey]) : [];
    if (!rows[index]) return;
    rows[index][field] = value;
    patchDetail(targetKey, rows);
  }

  function newRowDefaults(section: TabKey) {
    const base = { __localId: uniqueLocalId(section), is_active: true, status: 'active' };
    if (section === 'legal_entities') return { ...base, country: 'Россия', role: 'Основное юр. лицо' };
    if (section === 'contracts') return { ...base, status: 'draft' };
    if (section === 'payment_terms') return { ...base, fee_type: 'fixed', percent_base: 'ad_budget', calc_period: 'monthly' };
    if (section === 'payment_schedule') return { ...base, frequency: 'monthly', due_day: 10 };
    if (section === 'goals') return { ...base, priority: 3 };
    if (section === 'kpis') return { ...base, priority: 3, evaluation_period: 'monthly' };
    if (section === 'accesses') return { ...base, platform_code: 'wildberries', platform_name: 'Wildberries', auth_type: 'token', access_mode: 'read' };
    return { ...base };
  }

  function addRow(section: TabKey) {
    if (!detail) return;
    const targetKey = sectionKey(section);
    const rows = Array.isArray((detail as any)[targetKey]) ? structuredClone((detail as any)[targetKey]) : [];
    rows.push(newRowDefaults(section));
    patchDetail(targetKey, rows);
  }

  function duplicateRow(section: TabKey, index: number) {
    if (!detail) return;
    const targetKey = sectionKey(section);
    const rows = Array.isArray((detail as any)[targetKey]) ? structuredClone((detail as any)[targetKey]) : [];
    const row = rows[index];
    if (!row) return;
    const clone = { ...row, id: undefined, __localId: uniqueLocalId(section) };
    rows.splice(index + 1, 0, clone);
    patchDetail(targetKey, rows);
  }

  async function saveRow(section: TabKey, index: number) {
    if (!detail?.client?.id) return;
    const targetKey = sectionKey(section);
    const rows = Array.isArray((detail as any)[targetKey]) ? structuredClone((detail as any)[targetKey]) : [];
    const row = rows[index];
    if (!row) return;
    busyRowKey = rowKey(section, row, index);
    error = '';
    info = '';
    try {
      const payload = await apiJson<{ detail?: any }>(`${apiBase}/clients/module/section/upsert`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({ section, client_id: detail.client.id, record: row })
      });
      detail = normalizeDetail(payload?.detail);
      info = 'Запись сохранена в таблицу.';
      await loadClients(false);
    } catch (e: any) {
      error = String(e?.message || e || 'Не удалось сохранить запись.');
    } finally {
      busyRowKey = '';
    }
  }

  async function removeRow(section: TabKey, index: number) {
    if (!detail?.client?.id) return;
    const targetKey = sectionKey(section);
    const rows = Array.isArray((detail as any)[targetKey]) ? structuredClone((detail as any)[targetKey]) : [];
    const row = rows[index];
    if (!row) return;
    if (!row.id) {
      rows.splice(index, 1);
      patchDetail(targetKey, rows);
      return;
    }
    busyRowKey = rowKey(section, row, index);
    error = '';
    info = '';
    try {
      const payload = await apiJson<{ detail?: any }>(`${apiBase}/clients/module/section/delete`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({ section, client_id: detail.client.id, id: row.id })
      });
      detail = normalizeDetail(payload?.detail);
      info = 'Запись удалена.';
      await loadClients(false);
    } catch (e: any) {
      error = String(e?.message || e || 'Не удалось удалить запись.');
    } finally {
      busyRowKey = '';
    }
  }

  async function saveMainData() {
    if (!detail?.client?.id) return;
    savingMain = true;
    error = '';
    info = '';
    try {
      const payload = await apiJson<{ detail?: any }>(`${apiBase}/clients/module/main/upsert`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({ client_id: detail.client.id, data: detail.mainData })
      });
      detail = normalizeDetail(payload?.detail);
      info = 'Основные данные клиента сохранены.';
      await loadClients(false);
    } catch (e: any) {
      error = String(e?.message || e || 'Не удалось сохранить основные данные клиента.');
    } finally {
      savingMain = false;
    }
  }

  async function bootstrapModule() {
    bootstrapLoading = true;
    error = '';
    try {
      await apiJson(`${apiBase}/clients/module/bootstrap`, {
        method: 'GET',
        headers: { Accept: 'application/json', ...headers() }
      });
    } catch (e: any) {
      error = String(e?.message || e || 'Не удалось подготовить модуль клиентов.');
    } finally {
      bootstrapLoading = false;
    }
  }

  async function loadClients(selectFirstIfNeeded = true) {
    listLoading = true;
    error = '';
    try {
      const payload = await apiJson<{ clients?: ClientListItem[]; source?: SourceMeta }>(
        `${apiBase}/clients/module/list?search=${encodeURIComponent(search)}`,
        { method: 'GET', headers: { Accept: 'application/json', ...headers() } }
      );
      clients = Array.isArray(payload?.clients) ? payload.clients : [];
      listSource = payload?.source || null;
      const stillExists = clients.some((item) => item.id === selectedClientId);
      if (selectFirstIfNeeded && clients.length && (!selectedClientId || !stillExists)) {
        await selectClient(clients[0].id);
      } else if (!clients.length) {
        selectedClientId = 0;
        detail = null;
      }
    } catch (e: any) {
      error = String(e?.message || e || 'Не удалось загрузить список клиентов.');
    } finally {
      listLoading = false;
    }
  }

  async function selectClient(clientId: number) {
    if (!(clientId > 0)) return;
    detailLoading = true;
    error = '';
    selectedClientId = clientId;
    try {
      const payload = await apiJson<ClientDetailPayload>(`${apiBase}/clients/module/client/${clientId}`, {
        method: 'GET',
        headers: { Accept: 'application/json', ...headers() }
      });
      detail = normalizeDetail(payload);
    } catch (e: any) {
      error = String(e?.message || e || 'Не удалось загрузить карточку клиента.');
    } finally {
      detailLoading = false;
    }
  }

  async function deleteClient(clientId: number) {
    if (!(clientId > 0)) return;
    const source = listSource || FALLBACK_SOURCES.clients;
    if (!source?.schema_name || !source?.table_name) {
      error = 'Не удалось определить таблицу списка клиентов для удаления.';
      return;
    }
    deletingClientId = clientId;
    error = '';
    info = '';
    try {
      const preview = await apiJson<{ rows?: Array<Record<string, any>> }>(
        `${apiBase}/preview?schema=${encodeURIComponent(source.schema_name)}&table=${encodeURIComponent(source.table_name)}&limit=2000`,
        { headers: headers() }
      );
      const rows = Array.isArray(preview?.rows) ? preview.rows : [];
      const row = rows.find((item) => Number(item?.id || 0) === clientId);
      const ctid = row?.__ctid || row?.ctid;
      if (!ctid) throw new Error('ctid_not_found');
      await apiJson(`${apiBase}/rows/delete`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({ schema: source.schema_name, table: source.table_name, ctid })
      });
      if (selectedClientId === clientId) {
        selectedClientId = 0;
        detail = null;
      }
      await loadClients(true);
      info = 'Клиент удалён.';
    } catch (e: any) {
      error = e?.message || String(e || 'Не удалось удалить клиента.');
    } finally {
      deletingClientId = 0;
    }
  }

  async function createClient() {
    createLoading = true;
    error = '';
    info = '';
    try {
      const displayName = `Новый клиент ${new Date().toLocaleString('ru-RU', { dateStyle: 'short', timeStyle: 'short' })}`;
      const payload = await apiJson<{ client_id?: number; detail?: any }>(`${apiBase}/clients/module/client/create`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({ client_display_name: displayName })
      });
      detail = normalizeDetail(payload?.detail);
      selectedClientId = Number(payload?.client_id || detail?.client?.id || 0);
      info = 'Клиент создан.';
      await loadClients(false);
    } catch (e: any) {
      error = String(e?.message || e || 'Не удалось создать клиента.');
    } finally {
      createLoading = false;
    }
  }
  $: currentTabMeta = TABS.find((item) => item.key === activeTab) || TABS[0];
  $: {
    const dynamic = detail?.sources?.[currentTabMeta.sourceKey];
    if (Array.isArray(dynamic) || (dynamic && typeof dynamic === 'object')) {
      currentTabSource = dynamic;
    } else {
      const key = (CLIENT_MODULE_SECTION_DEFINITIONS as any)?.[currentTabMeta.sourceKey]?.key || currentTabMeta.sourceKey;
      currentTabSource = (FALLBACK_SOURCES as any)[key] || null;
    }
  }
  $: {
    if (!detail) {
      currentRows = [];
    } else if (activeTab === 'legal_entities') {
      currentRows = detail.legalEntities;
    } else if (activeTab === 'contracts') {
      currentRows = detail.contracts;
    } else if (activeTab === 'payment_terms') {
      currentRows = detail.paymentTerms;
    } else if (activeTab === 'payment_schedule') {
      currentRows = detail.paymentSchedule;
    } else if (activeTab === 'goals') {
      currentRows = detail.goals;
    } else if (activeTab === 'kpis') {
      currentRows = detail.kpis;
    } else if (activeTab === 'accesses') {
      currentRows = detail.accesses;
    } else {
      currentRows = [];
    }
  }
  $: currentOptionSets = {
    legalEntities: Array.isArray(detail?.options?.legalEntities) ? detail.options.legalEntities : [],
    contracts: Array.isArray(detail?.options?.contracts) ? detail.options.contracts : [],
    paymentTerms: Array.isArray(detail?.options?.paymentTerms) ? detail.options.paymentTerms : [],
    goals: Array.isArray(detail?.options?.goals) ? detail.options.goals : []
  };

  function summarySourceList() {
    const src = detail?.sources?.summary;
    if (Array.isArray(src) && src.length) return src;
    return [FALLBACK_SOURCES.client_summary_metrics, FALLBACK_SOURCES.client_constraints, FALLBACK_SOURCES.client_action_items];
  }

  function metricsRows() {
    const metrics = detail?.summaryMetrics || {};
    return [
      { label: 'Бюджет', plan: metrics.budget_plan, fact: metrics.budget_fact },
      { label: 'Выручка', plan: metrics.revenue_plan, fact: metrics.revenue_fact },
      { label: 'Заказы', plan: metrics.orders_plan, fact: metrics.orders_fact },
      { label: 'ДРР', plan: metrics.drr_plan, fact: metrics.drr_fact },
      { label: 'ROAS', plan: metrics.roas_plan, fact: metrics.roas_fact }
    ];
  }

  function formatMetric(value: any) {
    if (value === undefined || value === null || value === '') return '—';
    const num = Number(value);
    return Number.isFinite(num) ? num.toLocaleString('ru-RU') : String(value);
  }

  function elementValue(event: Event) {
    const target = event.currentTarget as HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement | null;
    return target ? target.value : '';
  }

  function patchMainField(field: string, value: any) {
    if (!detail) return;
    detail = { ...detail, mainData: { ...detail.mainData, [field]: value } };
  }

  onMount(async () => {
    await bootstrapModule();
    await loadClients(true);
  });
</script>

<div class="clients-layout">
  <aside class="panel left-col">
    <div class="panel-head">
      <div>
        <h2>Клиенты</h2>
        <p>Список всегда читается из таблицы. Здесь выбирается активная карточка клиента.</p>
      </div>
      <button class="primary-btn" type="button" on:click={createClient} disabled={createLoading || bootstrapLoading}>
        {createLoading ? 'Создаём...' : 'Новый клиент'}
      </button>
    </div>
    <div class="source-box">{sourceLabel(listSource || FALLBACK_SOURCES.clients)}</div>
    <div class="search-row">
      <input type="search" bind:value={search} placeholder="Поиск клиентов" on:input={() => loadClients(false)} />
      <button class="icon-btn refresh-btn" type="button" on:click={() => loadClients(false)} disabled={listLoading} title="Обновить список">↻</button>
    </div>
    {#if error}<div class="alert-box">{error}</div>{/if}
    {#if info}<div class="ok-box">{info}</div>{/if}
    <div class="client-list">
      {#if listLoading}
        <div class="empty-box">Загрузка списка клиентов...</div>
      {:else if clients.length === 0}
        <div class="empty-box">Клиенты пока не созданы. Нажми «Новый клиент».</div>
      {:else}
        {#each clients as client}
          <button type="button" class:active={client.id === selectedClientId} class="client-card" on:click={() => selectClient(client.id)}>
            <div class="client-card-head">
              <strong>{client.client_display_name || client.client_code || `Клиент #${client.id}`}</strong>
              <div class="client-card-actions">
                <span class="badge">{client.status || '—'}</span>
                <button
                  class="danger icon-btn"
                  type="button"
                  title="Удалить клиента"
                  disabled={deletingClientId === client.id}
                  on:click|stopPropagation={() => deleteClient(client.id)}
                >x</button>
              </div>
            </div>
            <div class="client-meta">Код: {client.client_code || '—'}</div>
            <div class="client-meta">{client.platform_summary || 'Платформы пока не заданы'}</div>
            <div class="client-stats">
              <span>Цели: {client.active_goal_count}</span>
              <span>KPI: {client.active_kpi_count}</span>
              <span>Доступы: {client.active_access_count}</span>
              <span>Предупреждения: {client.warning_count}</span>
            </div>
          </button>
        {/each}
      {/if}
    </div>
  </aside>

  <section class="panel center-col">
    {#if detailLoading}
      <div class="empty-box">Загрузка карточки клиента...</div>
    {:else if !detail?.client?.id}
      <div class="empty-box">Выбери клиента слева или создай нового, чтобы открыть карточку.</div>
    {:else}
      <div class="panel-head">
        <div>
          <h2>{detail.mainData.client_display_name || detail.client.client_display_name || 'Клиент'}</h2>
          <p>Код клиента: {detail.mainData.client_code || detail.client.client_code || '—'} • Статус: {detail.mainData.status || detail.client.status || '—'}</p>
        </div>
      </div>
      <nav class="tabs">
        {#each TABS as tab}
          <button class:active={activeTab === tab.key} type="button" on:click={() => (activeTab = tab.key)}>{tab.title}</button>
        {/each}
      </nav>
      <div class="source-box">{sourceLabel(currentTabSource)}</div>
      {#if activeTab === 'main_data'}
        <section class="section-block">
          <div class="section-head">
            <div>
              <h3>Основные данные клиента</h3>
              <p>Все изменения пишутся в таблицу `client_main_data` и синхронизируют базовый список клиентов.</p>
            </div>
            <button class="primary-btn" type="button" on:click={saveMainData} disabled={savingMain}>{savingMain ? 'Сохраняем...' : 'Сохранить'}</button>
          </div>
          <div class="form-grid">
            {#each MAIN_DATA_FIELDS as field}
              {@const isReadonly = field.key === 'client_code'}
              <label class:wide={field.key === 'comment'}>
                <span>{field.label}</span>
                {#if field.type === 'textarea'}
                  <textarea rows={field.rows || 4} value={String(detail.mainData?.[field.key] ?? '')} placeholder={field.placeholder || ''} on:input={(e) => patchMainField(field.key, elementValue(e))}></textarea>
                {:else if field.type === 'select'}
                  <select value={String(detail.mainData?.[field.key] ?? '')} on:change={(e) => patchMainField(field.key, elementValue(e))}>
                    <option value="">Выбери значение</option>
                    {#each field.options || [] as option}
                      <option value={option.value}>{option.label}</option>
                    {/each}
                  </select>
                {:else}
                  <input
                    type="text"
                    value={String(detail.mainData?.[field.key] ?? '')}
                    placeholder={field.placeholder || ''}
                    readonly={isReadonly}
                    disabled={isReadonly}
                    on:input={(e) => patchMainField(field.key, elementValue(e))}
                  />
                {/if}
              </label>
            {/each}
          </div>
        </section>
      {:else}
        <ClientRowsEditor
          title={currentTabMeta.title}
          rows={currentRows}
          fields={MULTI_SECTION_FIELDS[activeTab]}
          optionSets={currentOptionSets}
          addLabel={`Добавить: ${currentTabMeta.title}`}
          emptyText="Данные пока не заполнены."
          {busyRowKey}
          columns={activeTab === 'legal_entities' || activeTab === 'contracts' ? 3 : 2}
          gridTemplate={activeTab === 'contracts' ? '3fr 1fr 1fr' : ''}
          addRowHandler={() => addRow(activeTab)}
          changeRowHandler={(index, field, value) => updateMultiSection(activeTab, index, field, value)}
          saveRowHandler={(index) => saveRow(activeTab, index)}
          removeRowHandler={(index) => removeRow(activeTab, index)}
          duplicateRowHandler={(index) => duplicateRow(activeTab, index)}
        />
      {/if}
    {/if}
  </section>

  <aside class="panel right-col">
    <div class="panel-head">
      <div>
        <h2>Сводка</h2>
        <p>Сводка строится только на таблицах целей, KPI, ограничений, план/факт и ближайших действий.</p>
      </div>
    </div>
    {#each summarySourceList() as source}
      <div class="source-box compact">{sourceLabel(source)}</div>
    {/each}
    {#if !detail?.client?.id}
      <div class="empty-box">Выбери клиента, чтобы увидеть сводку.</div>
    {:else}
      <section class="summary-block">
        <h3>Цели</h3>
        {#if detail.summary?.goals?.length}
          <ul>{#each detail.summary.goals as goal}<li>{goal.goal_name || goal.goal_type || `Цель #${goal.id}`}</li>{/each}</ul>
        {:else}<div class="empty-mini">Активных целей пока нет.</div>{/if}
      </section>
      <section class="summary-block">
        <h3>KPI</h3>
        {#if detail.summary?.kpis?.length}
          <ul>{#each detail.summary.kpis as kpi}<li>{kpi.kpi_name || kpi.kpi_type || `KPI #${kpi.id}`}</li>{/each}</ul>
        {:else}<div class="empty-mini">Активных KPI пока нет.</div>{/if}
      </section>
      <section class="summary-block">
        <h3>Ограничения</h3>
        {#if detail.summary?.constraints?.length}
          <ul>{#each detail.summary.constraints as item}<li>{item.constraint_type}: {formatMetric(item.value)} {item.unit || ''}</li>{/each}</ul>
        {:else}<div class="empty-mini">Ограничения не заданы.</div>{/if}
      </section>
      <section class="summary-block">
        <h3>План / факт</h3>
        <div class="metrics-grid">{#each metricsRows() as row}<div class="metric-card"><strong>{row.label}</strong><div>План: {formatMetric(row.plan)}</div><div>Факт: {formatMetric(row.fact)}</div></div>{/each}</div>
      </section>
      <section class="summary-block">
        <h3>Ближайшие действия и предупреждения</h3>
        {#if detail.summary?.actionItems?.length}
          <ul>{#each detail.summary.actionItems as item}<li><strong>{item.title || item.action_type || 'Событие'}</strong><div>{item.message || item.comment || '—'}</div></li>{/each}</ul>
        {:else}<div class="empty-mini">Активных предупреждений и действий нет.</div>{/if}
      </section>
    {/if}
  </aside>
</div>

<style>
  .clients-layout { display: grid; grid-template-columns: minmax(260px, 320px) minmax(0, 1fr) minmax(280px, 360px); gap: 16px; min-width: 0; }
  .panel { background: #fff; border: 1px solid #e2e8f0; border-radius: 18px; padding: 16px; box-sizing: border-box; min-width: 0; display: flex; flex-direction: column; gap: 14px; }
  .panel-head { display: flex; justify-content: space-between; gap: 12px; align-items: flex-start; flex-wrap: wrap; }
  .panel-head h2 { margin: 0; font-size: 22px; }
  .panel-head p { margin: 4px 0 0; color: #64748b; font-size: 12px; max-width: 700px; }
  .source-box { border: 1px solid #dbe4f0; border-radius: 12px; padding: 10px 12px; background: #f8fafc; color: #334155; font-size: 12px; }
  .source-box.compact { padding: 8px 10px; font-size: 11px; }
  .search-row { display: grid; grid-template-columns: 1fr auto; gap: 8px; }
  .search-row input, .form-grid input, .form-grid textarea, .form-grid select { width: 100%; box-sizing: border-box; border: 1px solid #cbd5e1; border-radius: 10px; padding: 10px 12px; font: inherit; background: #fff; }
  .client-list { display: flex; flex-direction: column; gap: 10px; min-width: 0; }
  .client-card { border: 1px solid #dbe4f0; border-radius: 14px; background: #fff; padding: 12px; cursor: pointer; text-align: left; display: flex; flex-direction: column; gap: 8px; }
  .client-card.active { border-color: #0f172a; box-shadow: inset 0 0 0 1px #0f172a; }
  .client-card-head { display: flex; justify-content: space-between; gap: 8px; align-items: flex-start; }
  .client-card-actions { display: inline-flex; align-items: center; gap: 8px; }
  .client-meta { font-size: 12px; color: #475569; }
  .client-stats { display: flex; gap: 8px; flex-wrap: wrap; font-size: 11px; color: #64748b; }
  .badge { display: inline-flex; align-items: center; padding: 4px 8px; border-radius: 999px; background: #eef2ff; color: #3730a3; font-size: 11px; white-space: nowrap; }
  .tabs { display: flex; gap: 8px; flex-wrap: wrap; border-bottom: 1px solid #e2e8f0; padding-bottom: 10px; }
  .tabs button { border: 1px solid #dbe4f0; background: #fff; border-radius: 12px; padding: 8px 12px; cursor: pointer; }
  .tabs button.active { background: #0f172a; color: #fff; border-color: #0f172a; }
  .section-block { display: flex; flex-direction: column; gap: 14px; }
  .section-head { display: flex; justify-content: space-between; gap: 12px; align-items: flex-start; flex-wrap: wrap; }
  .section-head h3 { margin: 0; font-size: 16px; }
  .section-head p { margin: 4px 0 0; color: #64748b; font-size: 12px; }
  .form-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; }
  .form-grid label { display: flex; flex-direction: column; gap: 6px; font-size: 12px; color: #334155; }
  .form-grid label.wide { grid-column: 1 / -1; }
  .form-grid span { font-weight: 600; }
  .right-col { gap: 12px; }
  .summary-block { border: 1px solid #e2e8f0; border-radius: 14px; padding: 12px; display: flex; flex-direction: column; gap: 10px; }
  .summary-block h3 { margin: 0; font-size: 15px; }
  .summary-block ul { list-style: none; padding: 0; margin: 0; display: flex; flex-direction: column; gap: 8px; }
  .summary-block li { border: 1px solid #edf2f7; border-radius: 10px; padding: 8px 10px; font-size: 12px; color: #334155; }
  .metrics-grid { display: grid; grid-template-columns: 1fr; gap: 8px; }
  .metric-card { border: 1px solid #edf2f7; border-radius: 10px; padding: 8px 10px; font-size: 12px; color: #334155; }
  .metric-card strong { display: block; margin-bottom: 4px; }
  .primary-btn, .mini-btn, .icon-btn { border-radius: 10px; border: 1px solid #dbe4f0; background: #fff; padding: 8px 12px; cursor: pointer; font-size: 12px; }
  .primary-btn { background: #0f172a; color: #fff; border-color: #0f172a; }
  .refresh-btn { color: #16a34a; }
  .danger.icon-btn { color: #dc2626; border-color: #fecaca; }
  .empty-box, .empty-mini, .alert-box, .ok-box { border-radius: 12px; padding: 12px; font-size: 13px; }
  .empty-box, .empty-mini { border: 1px dashed #cbd5e1; color: #64748b; background: #f8fafc; }
  .alert-box { border: 1px solid #fecaca; background: #fff1f2; color: #b91c1c; }
  .ok-box { border: 1px solid #bbf7d0; background: #f0fdf4; color: #166534; }
  @media (max-width: 1480px) { .clients-layout { grid-template-columns: 280px minmax(0, 1fr); } .right-col { grid-column: 1 / -1; } }
  @media (max-width: 1024px) { .clients-layout { grid-template-columns: 1fr; } .form-grid { grid-template-columns: 1fr; } }
</style>

