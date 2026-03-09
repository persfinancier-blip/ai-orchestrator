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
    { key: 'main_data', title: 'РћСЃРЅРѕРІРЅС‹Рµ РґР°РЅРЅС‹Рµ', sourceKey: 'main_data' },
    { key: 'legal_entities', title: 'Р®СЂ. Р»РёС†Р°', sourceKey: 'legal_entities' },
    { key: 'contracts', title: 'Р”РѕРіРѕРІРѕСЂС‹', sourceKey: 'contracts' },
    { key: 'payment_terms', title: 'РЈСЃР»РѕРІРёСЏ РѕРїР»Р°С‚С‹', sourceKey: 'payment_terms' },
    { key: 'payment_schedule', title: 'Р“СЂР°С„РёРє РїР»Р°С‚РµР¶РµР№', sourceKey: 'payment_schedule' },
    { key: 'goals', title: 'Р¦РµР»Рё', sourceKey: 'goals' },
    { key: 'kpis', title: 'KPI', sourceKey: 'kpis' },
    { key: 'accesses', title: 'РџР»Р°С‚С„РѕСЂРјС‹ Рё РґРѕСЃС‚СѓРїС‹', sourceKey: 'accesses' }
  ];

  const STATUS_OPTIONS: OptionItem[] = [
    { value: 'draft', label: 'Р§РµСЂРЅРѕРІРёРє' },
    { value: 'active', label: 'РђРєС‚РёРІРµРЅ' },
    { value: 'setup', label: 'РўСЂРµР±СѓРµС‚ РЅР°СЃС‚СЂРѕР№РєРё' },
    { value: 'paused', label: 'РќР° РїР°СѓР·Рµ' },
    { value: 'archived', label: 'РђСЂС…РёРІ' }
  ];
  const FEE_TYPE_OPTIONS: OptionItem[] = [
    { value: 'fixed', label: 'Р¤РёРєСЃ' },
    { value: 'percent', label: 'РџСЂРѕС†РµРЅС‚' },
    { value: 'mixed', label: 'Р¤РёРєСЃ + РїСЂРѕС†РµРЅС‚' }
  ];
  const PERCENT_BASE_OPTIONS: OptionItem[] = [
    { value: 'ad_budget', label: 'Р РµРєР»Р°РјРЅС‹Р№ Р±СЋРґР¶РµС‚' },
    { value: 'revenue', label: 'Р’С‹СЂСѓС‡РєР°' },
    { value: 'ad_revenue', label: 'Р РµРєР»Р°РјРЅР°СЏ РІС‹СЂСѓС‡РєР°' },
    { value: 'gross_profit', label: 'Р’Р°Р»РѕРІР°СЏ РїСЂРёР±С‹Р»СЊ' },
    { value: 'orders', label: 'Р—Р°РєР°Р·С‹' },
    { value: 'other', label: 'Р”СЂСѓРіРѕРµ' }
  ];
  const CALC_PERIOD_OPTIONS: OptionItem[] = [
    { value: 'weekly', label: 'РќРµРґРµР»СЏ' },
    { value: 'monthly', label: 'РњРµСЃСЏС†' },
    { value: 'quarterly', label: 'РљРІР°СЂС‚Р°Р»' }
  ];
  const PAYMENT_FREQUENCY_OPTIONS: OptionItem[] = [
    { value: 'once', label: 'Р Р°Р·РѕРІРѕ' },
    { value: 'weekly', label: 'Р•Р¶РµРЅРµРґРµР»СЊРЅРѕ' },
    { value: 'monthly', label: 'Р•Р¶РµРјРµСЃСЏС‡РЅРѕ' },
    { value: 'quarterly', label: 'Р•Р¶РµРєРІР°СЂС‚Р°Р»СЊРЅРѕ' }
  ];
  const PRIORITY_OPTIONS: OptionItem[] = [
    { value: '1', label: '1 вЂ” РІС‹СЃРѕРєРёР№' },
    { value: '2', label: '2 вЂ” РІС‹С€Рµ СЃСЂРµРґРЅРµРіРѕ' },
    { value: '3', label: '3 вЂ” СЃСЂРµРґРЅРёР№' },
    { value: '4', label: '4 вЂ” РЅРёР¶Рµ СЃСЂРµРґРЅРµРіРѕ' },
    { value: '5', label: '5 вЂ” РЅРёР·РєРёР№' }
  ];
  const KPI_TYPE_OPTIONS: OptionItem[] = [
    { value: 'revenue', label: 'Р’С‹СЂСѓС‡РєР°' },
    { value: 'orders', label: 'Р—Р°РєР°Р·С‹' },
    { value: 'drr', label: 'Р”Р Р ' },
    { value: 'roas', label: 'ROAS' },
    { value: 'profit', label: 'РџСЂРёР±С‹Р»СЊ' }
  ];
  const PLATFORM_OPTIONS: OptionItem[] = [
    { value: 'wildberries', label: 'Wildberries' },
    { value: 'ozon', label: 'Ozon' },
    { value: 'yandex_market', label: 'РЇРЅРґРµРєСЃ РњР°СЂРєРµС‚' },
    { value: 'ozon_performance', label: 'Ozon Performance' },
    { value: 'mpstats', label: 'MPStats' },
    { value: 'mpguru', label: 'MPGuru' }
  ];
  const AUTH_TYPE_OPTIONS: OptionItem[] = [
    { value: 'token', label: 'РўРѕРєРµРЅ' },
    { value: 'login_password', label: 'Р›РѕРіРёРЅ Рё РїР°СЂРѕР»СЊ' },
    { value: 'api_key', label: 'API-РєР»СЋС‡' }
  ];
  const ACCESS_MODE_OPTIONS: OptionItem[] = [
    { value: 'read', label: 'РўРѕР»СЊРєРѕ С‡С‚РµРЅРёРµ' },
    { value: 'write', label: 'Р§С‚РµРЅРёРµ Рё Р·Р°РїРёСЃСЊ' },
    { value: 'admin', label: 'РџРѕР»РЅС‹Р№ РґРѕСЃС‚СѓРї' }
  ];
  const MAIN_DATA_FIELDS: ClientFieldConfig[] = [
    { key: 'client_code', label: 'РљРѕРґ РєР»РёРµРЅС‚Р°', placeholder: 'fresh_market' },
    { key: 'client_name', label: 'Р®СЂРёРґРёС‡РµСЃРєРѕРµ РёРјСЏ', placeholder: 'РћРћРћ РЎРІРµР¶РёР№ Р С‹РЅРѕРє' },
    { key: 'client_display_name', label: 'РћС‚РѕР±СЂР°Р¶Р°РµРјРѕРµ РёРјСЏ', placeholder: 'РЎРІРµР¶РёР№ Р С‹РЅРѕРє' },
    { key: 'status', label: 'РЎС‚Р°С‚СѓСЃ', type: 'select', options: STATUS_OPTIONS },
    { key: 'comment', label: 'РљРѕРјРјРµРЅС‚Р°СЂРёР№', type: 'textarea', rows: 4, placeholder: 'РљРѕРјРјРµРЅС‚Р°СЂРёР№ РїРѕ РєР»РёРµРЅС‚Сѓ' }
  ];
  const LEGAL_ENTITY_FIELDS: ClientFieldConfig[] = [
    { key: 'legal_entity_name', label: 'РќР°Р·РІР°РЅРёРµ СЋСЂ. Р»РёС†Р°' },
    { key: 'legal_entity_code', label: 'РљРѕРґ СЋСЂ. Р»РёС†Р°' },
    { key: 'tax_id', label: 'РРќРќ / РЅР°Р»РѕРіРѕРІС‹Р№ РЅРѕРјРµСЂ' },
    { key: 'country', label: 'РЎС‚СЂР°РЅР°' },
    { key: 'role', label: 'Р РѕР»СЊ' },
    { key: 'is_active', label: 'РђРєС‚РёРІРЅРѕ', type: 'checkbox' },
    { key: 'comment', label: 'РљРѕРјРјРµРЅС‚Р°СЂРёР№', type: 'textarea', rows: 3 }
  ];
  const CONTRACT_FIELDS: ClientFieldConfig[] = [
    { key: 'legal_entity_id', label: 'Р®СЂ. Р»РёС†Рѕ', type: 'select', optionsKey: 'legalEntities' },
    { key: 'contract_number', label: 'РќРѕРјРµСЂ РґРѕРіРѕРІРѕСЂР°' },
    { key: 'contract_name', label: 'РќР°Р·РІР°РЅРёРµ РґРѕРіРѕРІРѕСЂР°' },
    { key: 'contract_date', label: 'Р”Р°С‚Р° РґРѕРіРѕРІРѕСЂР°', type: 'date' },
    { key: 'date_start', label: 'РќР°С‡Р°Р»Рѕ РґРµР№СЃС‚РІРёСЏ', type: 'date' },
    { key: 'date_end', label: 'РћРєРѕРЅС‡Р°РЅРёРµ РґРµР№СЃС‚РІРёСЏ', type: 'date' },
    { key: 'status', label: 'РЎС‚Р°С‚СѓСЃ', type: 'select', options: STATUS_OPTIONS },
    { key: 'file_url', label: 'РЎСЃС‹Р»РєР° РЅР° С„Р°Р№Р»' },
    { key: 'comment', label: 'РљРѕРјРјРµРЅС‚Р°СЂРёР№', type: 'textarea', rows: 3 }
  ];
  const PAYMENT_TERM_FIELDS: ClientFieldConfig[] = [
    { key: 'contract_id', label: 'Р”РѕРіРѕРІРѕСЂ', type: 'select', optionsKey: 'contracts' },
    { key: 'fee_type', label: 'РўРёРї РѕРїР»Р°С‚С‹', type: 'select', options: FEE_TYPE_OPTIONS },
    { key: 'fixed_amount', label: 'Р¤РёРєСЃРёСЂРѕРІР°РЅРЅР°СЏ СЃСѓРјРјР°', type: 'number', step: '0.01' },
    { key: 'percent_value', label: 'РџСЂРѕС†РµРЅС‚', type: 'number', step: '0.01' },
    { key: 'percent_base', label: 'Р‘Р°Р·Р° РїСЂРѕС†РµРЅС‚Р°', type: 'select', options: PERCENT_BASE_OPTIONS },
    { key: 'min_amount', label: 'РњРёРЅРёРјР°Р»СЊРЅР°СЏ СЃСѓРјРјР°', type: 'number', step: '0.01' },
    { key: 'max_amount', label: 'РњР°РєСЃРёРјР°Р»СЊРЅР°СЏ СЃСѓРјРјР°', type: 'number', step: '0.01' },
    { key: 'calc_period', label: 'РџРµСЂРёРѕРґ СЂР°СЃС‡С‘С‚Р°', type: 'select', options: CALC_PERIOD_OPTIONS },
    { key: 'is_active', label: 'РђРєС‚РёРІРЅРѕ', type: 'checkbox' },
    { key: 'date_start', label: 'РќР°С‡Р°Р»Рѕ РґРµР№СЃС‚РІРёСЏ', type: 'date' },
    { key: 'date_end', label: 'РћРєРѕРЅС‡Р°РЅРёРµ РґРµР№СЃС‚РІРёСЏ', type: 'date' },
    { key: 'comment', label: 'РљРѕРјРјРµРЅС‚Р°СЂРёР№', type: 'textarea', rows: 3 }
  ];
  const PAYMENT_SCHEDULE_FIELDS: ClientFieldConfig[] = [
    { key: 'payment_term_id', label: 'РЈСЃР»РѕРІРёРµ РѕРїР»Р°С‚С‹', type: 'select', optionsKey: 'paymentTerms' },
    { key: 'payment_type', label: 'РўРёРї РїР»Р°С‚РµР¶Р°' },
    { key: 'frequency', label: 'Р§Р°СЃС‚РѕС‚Р°', type: 'select', options: PAYMENT_FREQUENCY_OPTIONS },
    { key: 'due_day', label: 'Р”РµРЅСЊ РѕРїР»Р°С‚С‹', type: 'number', min: 1, step: 1 },
    { key: 'amount', label: 'РЎСѓРјРјР°', type: 'number', step: '0.01' },
    { key: 'calc_rule', label: 'РџСЂР°РІРёР»Рѕ СЂР°СЃС‡С‘С‚Р°' },
    { key: 'status', label: 'РЎС‚Р°С‚СѓСЃ', type: 'select', options: STATUS_OPTIONS },
    { key: 'comment', label: 'РљРѕРјРјРµРЅС‚Р°СЂРёР№', type: 'textarea', rows: 3 }
  ];
  const GOAL_FIELDS: ClientFieldConfig[] = [
    { key: 'goal_name', label: 'РќР°Р·РІР°РЅРёРµ С†РµР»Рё' },
    { key: 'goal_type', label: 'РўРёРї С†РµР»Рё' },
    { key: 'priority', label: 'РџСЂРёРѕСЂРёС‚РµС‚', type: 'select', options: PRIORITY_OPTIONS },
    { key: 'date_start', label: 'РќР°С‡Р°Р»Рѕ', type: 'date' },
    { key: 'date_end', label: 'РћРєРѕРЅС‡Р°РЅРёРµ', type: 'date' },
    { key: 'is_active', label: 'РђРєС‚РёРІРЅР°', type: 'checkbox' },
    { key: 'comment', label: 'РљРѕРјРјРµРЅС‚Р°СЂРёР№', type: 'textarea', rows: 3 }
  ];
  const KPI_FIELDS: ClientFieldConfig[] = [
    { key: 'goal_id', label: 'Р¦РµР»СЊ', type: 'select', optionsKey: 'goals' },
    { key: 'kpi_name', label: 'РќР°Р·РІР°РЅРёРµ KPI' },
    { key: 'kpi_type', label: 'РўРёРї KPI', type: 'select', options: KPI_TYPE_OPTIONS },
    { key: 'target_value', label: 'Р¦РµР»СЊ', type: 'number', step: '0.01' },
    { key: 'min_value', label: 'РњРёРЅРёРјСѓРј', type: 'number', step: '0.01' },
    { key: 'max_value', label: 'РњР°РєСЃРёРјСѓРј', type: 'number', step: '0.01' },
    { key: 'unit', label: 'Р•РґРёРЅРёС†Р°' },
    { key: 'evaluation_period', label: 'РџРµСЂРёРѕРґ РѕС†РµРЅРєРё' },
    { key: 'priority', label: 'РџСЂРёРѕСЂРёС‚РµС‚', type: 'select', options: PRIORITY_OPTIONS },
    { key: 'is_active', label: 'РђРєС‚РёРІРµРЅ', type: 'checkbox' },
    { key: 'comment', label: 'РљРѕРјРјРµРЅС‚Р°СЂРёР№', type: 'textarea', rows: 3 }
  ];
  const ACCESS_FIELDS: ClientFieldConfig[] = [
    { key: 'platform_code', label: 'РљРѕРґ РїР»Р°С‚С„РѕСЂРјС‹', type: 'select', options: PLATFORM_OPTIONS },
    { key: 'platform_name', label: 'РќР°Р·РІР°РЅРёРµ РїР»Р°С‚С„РѕСЂРјС‹' },
    { key: 'system_name', label: 'РќР°Р·РІР°РЅРёРµ РІ СЃРёСЃС‚РµРјРµ' },
    { key: 'external_id', label: 'Р’РЅРµС€РЅРёР№ ID' },
    { key: 'cabinet_name', label: 'РљР°Р±РёРЅРµС‚' },
    { key: 'cabinet_id', label: 'ID РєР°Р±РёРЅРµС‚Р°' },
    { key: 'auth_type', label: 'РўРёРї Р°РІС‚РѕСЂРёР·Р°С†РёРё', type: 'select', options: AUTH_TYPE_OPTIONS },
    { key: 'token_value', label: 'РўРѕРєРµРЅ / РєР»СЋС‡' },
    { key: 'login_value', label: 'Р›РѕРіРёРЅ' },
    { key: 'password_value', label: 'РџР°СЂРѕР»СЊ' },
    { key: 'api_key', label: 'API-РєР»СЋС‡' },
    { key: 'access_scope', label: 'РћР±Р»Р°СЃС‚СЊ РґРѕСЃС‚СѓРїР°' },
    { key: 'access_mode', label: 'Р РµР¶РёРј РґРѕСЃС‚СѓРїР°', type: 'select', options: ACCESS_MODE_OPTIONS },
    { key: 'is_active', label: 'РђРєС‚РёРІРµРЅ', type: 'checkbox' },
    { key: 'check_status', label: 'РЎС‚Р°С‚СѓСЃ РїСЂРѕРІРµСЂРєРё' },
    { key: 'last_checked_at', label: 'РџРѕСЃР»РµРґРЅСЏСЏ РїСЂРѕРІРµСЂРєР°', type: 'datetime' },
    { key: 'expires_at', label: 'РСЃС‚РµРєР°РµС‚', type: 'datetime' },
    { key: 'last_error_text', label: 'РџРѕСЃР»РµРґРЅСЏСЏ РѕС€РёР±РєР°', type: 'textarea', rows: 3 },
    { key: 'data_table_ref', label: 'РўР°Р±Р»РёС†Р° РґР°РЅРЅС‹С…' },
    { key: 'sync_table_ref', label: 'РўР°Р±Р»РёС†Р° СЃРёРЅС…СЂРѕРЅРёР·Р°С†РёРё' },
    { key: 'log_table_ref', label: 'РўР°Р±Р»РёС†Р° Р»РѕРіРѕРІ' },
    { key: 'comment', label: 'РљРѕРјРјРµРЅС‚Р°СЂРёР№', type: 'textarea', rows: 3 }
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
  let listSource: SourceMeta | null = null;
  let detail: ClientDetailPayload | null = null;
  let activeTab: TabKey = 'main_data';

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
    if (!source) return 'РСЃС‚РѕС‡РЅРёРє РЅРµ РѕРїСЂРµРґРµР»С‘РЅ';
    return `РЁР°Р±Р»РѕРЅ С‚Р°Р±Р»РёС†С‹: ${source.template_name} вЂў РўР°Р±Р»РёС†Р°: ${source.table_name} вЂў РЎС…РµРјР°: ${source.schema_name}`;
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
    if (section === 'legal_entities') return { ...base, country: 'Р РѕСЃСЃРёСЏ', role: 'РћСЃРЅРѕРІРЅРѕРµ СЋСЂ. Р»РёС†Рѕ' };
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
      info = 'Р—Р°РїРёСЃСЊ СЃРѕС…СЂР°РЅРµРЅР° РІ С‚Р°Р±Р»РёС†Сѓ.';
      await loadClients(false);
    } catch (e: any) {
      error = String(e?.message || e || 'РќРµ СѓРґР°Р»РѕСЃСЊ СЃРѕС…СЂР°РЅРёС‚СЊ Р·Р°РїРёСЃСЊ.');
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
      info = 'Р—Р°РїРёСЃСЊ СѓРґР°Р»РµРЅР°.';
      await loadClients(false);
    } catch (e: any) {
      error = String(e?.message || e || 'РќРµ СѓРґР°Р»РѕСЃСЊ СѓРґР°Р»РёС‚СЊ Р·Р°РїРёСЃСЊ.');
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
      info = 'РћСЃРЅРѕРІРЅС‹Рµ РґР°РЅРЅС‹Рµ РєР»РёРµРЅС‚Р° СЃРѕС…СЂР°РЅРµРЅС‹.';
      await loadClients(false);
    } catch (e: any) {
      error = String(e?.message || e || 'РќРµ СѓРґР°Р»РѕСЃСЊ СЃРѕС…СЂР°РЅРёС‚СЊ РѕСЃРЅРѕРІРЅС‹Рµ РґР°РЅРЅС‹Рµ РєР»РёРµРЅС‚Р°.');
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
      error = String(e?.message || e || 'РќРµ СѓРґР°Р»РѕСЃСЊ РїРѕРґРіРѕС‚РѕРІРёС‚СЊ РјРѕРґСѓР»СЊ РєР»РёРµРЅС‚РѕРІ.');
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
      error = String(e?.message || e || 'РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РіСЂСѓР·РёС‚СЊ СЃРїРёСЃРѕРє РєР»РёРµРЅС‚РѕРІ.');
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
      error = String(e?.message || e || 'РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РіСЂСѓР·РёС‚СЊ РєР°СЂС‚РѕС‡РєСѓ РєР»РёРµРЅС‚Р°.');
    } finally {
      detailLoading = false;
    }
  }

  async function createClient() {
    createLoading = true;
    error = '';
    info = '';
    try {
      const displayName = `РќРѕРІС‹Р№ РєР»РёРµРЅС‚ ${new Date().toLocaleString('ru-RU', { dateStyle: 'short', timeStyle: 'short' })}`;
      const payload = await apiJson<{ client_id?: number; detail?: any }>(`${apiBase}/clients/module/client/create`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({ client_display_name: displayName })
      });
      detail = normalizeDetail(payload?.detail);
      selectedClientId = Number(payload?.client_id || detail?.client?.id || 0);
      info = 'РљР»РёРµРЅС‚ СЃРѕР·РґР°РЅ.';
      await loadClients(false);
    } catch (e: any) {
      error = String(e?.message || e || 'РќРµ СѓРґР°Р»РѕСЃСЊ СЃРѕР·РґР°С‚СЊ РєР»РёРµРЅС‚Р°.');
    } finally {
      createLoading = false;
    }
  }

  function activeTabMeta() {
    return TABS.find((item) => item.key === activeTab) || TABS[0];
  }

  function summarySourceList() {
    const src = detail?.sources?.summary;
    if (Array.isArray(src) && src.length) return src;
    return [FALLBACK_SOURCES.client_summary_metrics, FALLBACK_SOURCES.client_constraints, FALLBACK_SOURCES.client_action_items];
  }

  function metricsRows() {
    const metrics = detail?.summaryMetrics || {};
    return [
      { label: 'Р‘СЋРґР¶РµС‚', plan: metrics.budget_plan, fact: metrics.budget_fact },
      { label: 'Р’С‹СЂСѓС‡РєР°', plan: metrics.revenue_plan, fact: metrics.revenue_fact },
      { label: 'Р—Р°РєР°Р·С‹', plan: metrics.orders_plan, fact: metrics.orders_fact },
      { label: 'Р”Р Р ', plan: metrics.drr_plan, fact: metrics.drr_fact },
      { label: 'ROAS', plan: metrics.roas_plan, fact: metrics.roas_fact }
    ];
  }

  function formatMetric(value: any) {
    if (value === undefined || value === null || value === '') return 'вЂ”';
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
        <h2>РљР»РёРµРЅС‚С‹</h2>
        <p>РЎРїРёСЃРѕРє РІСЃРµРіРґР° С‡РёС‚Р°РµС‚СЃСЏ РёР· С‚Р°Р±Р»РёС†С‹. Р—РґРµСЃСЊ РІС‹Р±РёСЂР°РµС‚СЃСЏ Р°РєС‚РёРІРЅР°СЏ РєР°СЂС‚РѕС‡РєР° РєР»РёРµРЅС‚Р°.</p>
      </div>
      <button class="primary-btn" type="button" on:click={createClient} disabled={createLoading || bootstrapLoading}>
        {createLoading ? 'РЎРѕР·РґР°С‘Рј...' : 'РќРѕРІС‹Р№ РєР»РёРµРЅС‚'}
      </button>
    </div>
    <div class="source-box">{sourceLabel(listSource || FALLBACK_SOURCES.clients)}</div>
    <div class="search-row">
      <input type="search" bind:value={search} placeholder="РџРѕРёСЃРє РєР»РёРµРЅС‚РѕРІ" on:input={() => loadClients(false)} />
      <button class="mini-btn" type="button" on:click={() => loadClients(false)} disabled={listLoading}>РћР±РЅРѕРІРёС‚СЊ</button>
    </div>
    {#if error}<div class="alert-box">{error}</div>{/if}
    {#if info}<div class="ok-box">{info}</div>{/if}
    <div class="client-list">
      {#if listLoading}
        <div class="empty-box">Р—Р°РіСЂСѓР·РєР° СЃРїРёСЃРєР° РєР»РёРµРЅС‚РѕРІ...</div>
      {:else if clients.length === 0}
        <div class="empty-box">РљР»РёРµРЅС‚С‹ РїРѕРєР° РЅРµ СЃРѕР·РґР°РЅС‹. РќР°Р¶РјРё В«РќРѕРІС‹Р№ РєР»РёРµРЅС‚В».</div>
      {:else}
        {#each clients as client}
          <button type="button" class:active={client.id === selectedClientId} class="client-card" on:click={() => selectClient(client.id)}>
            <div class="client-card-head">
              <strong>{client.client_display_name || client.client_code || `РљР»РёРµРЅС‚ #${client.id}`}</strong>
              <span class="badge">{client.status || 'вЂ”'}</span>
            </div>
            <div class="client-meta">РљРѕРґ: {client.client_code || 'вЂ”'}</div>
            <div class="client-meta">{client.platform_summary || 'РџР»Р°С‚С„РѕСЂРјС‹ РїРѕРєР° РЅРµ Р·Р°РґР°РЅС‹'}</div>
            <div class="client-stats">
              <span>Р¦РµР»Рё: {client.active_goal_count}</span>
              <span>KPI: {client.active_kpi_count}</span>
              <span>Р”РѕСЃС‚СѓРїС‹: {client.active_access_count}</span>
              <span>РџСЂРµРґСѓРїСЂРµР¶РґРµРЅРёСЏ: {client.warning_count}</span>
            </div>
          </button>
        {/each}
      {/if}
    </div>
  </aside>

  <section class="panel center-col">
    {#if detailLoading}
      <div class="empty-box">Р—Р°РіСЂСѓР·РєР° РєР°СЂС‚РѕС‡РєРё РєР»РёРµРЅС‚Р°...</div>
    {:else if !detail?.client?.id}
      <div class="empty-box">Р’С‹Р±РµСЂРё РєР»РёРµРЅС‚Р° СЃР»РµРІР° РёР»Рё СЃРѕР·РґР°Р№ РЅРѕРІРѕРіРѕ, С‡С‚РѕР±С‹ РѕС‚РєСЂС‹С‚СЊ РєР°СЂС‚РѕС‡РєСѓ.</div>
    {:else}
      <div class="panel-head">
        <div>
          <h2>{detail.mainData.client_display_name || detail.client.client_display_name || 'РљР»РёРµРЅС‚'}</h2>
          <p>РљРѕРґ РєР»РёРµРЅС‚Р°: {detail.mainData.client_code || detail.client.client_code || 'вЂ”'} вЂў РЎС‚Р°С‚СѓСЃ: {detail.mainData.status || detail.client.status || 'вЂ”'}</p>
        </div>
      </div>
      <nav class="tabs">
        {#each TABS as tab}
          <button class:active={activeTab === tab.key} type="button" on:click={() => (activeTab = tab.key)}>{tab.title}</button>
        {/each}
      </nav>
      <div class="source-box">{sourceLabel(sourceForSection(activeTabMeta().sourceKey))}</div>
      {#if activeTab === 'main_data'}
        <section class="section-block">
          <div class="section-head">
            <div>
              <h3>РћСЃРЅРѕРІРЅС‹Рµ РґР°РЅРЅС‹Рµ РєР»РёРµРЅС‚Р°</h3>
              <p>Р’СЃРµ РёР·РјРµРЅРµРЅРёСЏ РїРёС€СѓС‚СЃСЏ РІ С‚Р°Р±Р»РёС†Сѓ `client_main_data` Рё СЃРёРЅС…СЂРѕРЅРёР·РёСЂСѓСЋС‚ Р±Р°Р·РѕРІС‹Р№ СЃРїРёСЃРѕРє РєР»РёРµРЅС‚РѕРІ.</p>
            </div>
            <button class="primary-btn" type="button" on:click={saveMainData} disabled={savingMain}>{savingMain ? 'РЎРѕС…СЂР°РЅСЏРµРј...' : 'РЎРѕС…СЂР°РЅРёС‚СЊ'}</button>
          </div>
          <div class="form-grid">
            {#each MAIN_DATA_FIELDS as field}
              <label class:wide={field.key === 'comment'}>
                <span>{field.label}</span>
                {#if field.type === 'textarea'}
                  <textarea rows={field.rows || 4} value={String(detail.mainData?.[field.key] ?? '')} placeholder={field.placeholder || ''} on:input={(e) => patchMainField(field.key, elementValue(e))}></textarea>
                {:else if field.type === 'select'}
                  <select value={String(detail.mainData?.[field.key] ?? '')} on:change={(e) => patchMainField(field.key, elementValue(e))}>
                    <option value="">Р’С‹Р±РµСЂРё Р·РЅР°С‡РµРЅРёРµ</option>
                    {#each field.options || [] as option}
                      <option value={option.value}>{option.label}</option>
                    {/each}
                  </select>
                {:else}
                  <input type="text" value={String(detail.mainData?.[field.key] ?? '')} placeholder={field.placeholder || ''} on:input={(e) => patchMainField(field.key, elementValue(e))} />
                {/if}
              </label>
            {/each}
          </div>
        </section>
      {:else}
        <ClientRowsEditor
          title={activeTabMeta().title}
          rows={sourceRows(activeTab)}
          fields={MULTI_SECTION_FIELDS[activeTab]}
          optionSets={optionSets()}
          addLabel={`Р”РѕР±Р°РІРёС‚СЊ: ${activeTabMeta().title}`}
          emptyText="Р”Р°РЅРЅС‹Рµ РїРѕРєР° РЅРµ Р·Р°РїРѕР»РЅРµРЅС‹."
          {busyRowKey}
          on:add={() => addRow(activeTab)}
          on:change={(event) => updateMultiSection(activeTab, event.detail.index, event.detail.field, event.detail.value)}
          on:save={(event) => saveRow(activeTab, event.detail.index)}
          on:remove={(event) => removeRow(activeTab, event.detail.index)}
          on:duplicate={(event) => duplicateRow(activeTab, event.detail.index)}
        />
      {/if}
    {/if}
  </section>

  <aside class="panel right-col">
    <div class="panel-head">
      <div>
        <h2>РЎРІРѕРґРєР°</h2>
        <p>РЎРІРѕРґРєР° СЃС‚СЂРѕРёС‚СЃСЏ С‚РѕР»СЊРєРѕ РЅР° С‚Р°Р±Р»РёС†Р°С… С†РµР»РµР№, KPI, РѕРіСЂР°РЅРёС‡РµРЅРёР№, РїР»Р°РЅ/С„Р°РєС‚ Рё Р±Р»РёР¶Р°Р№С€РёС… РґРµР№СЃС‚РІРёР№.</p>
      </div>
    </div>
    {#each summarySourceList() as source}
      <div class="source-box compact">{sourceLabel(source)}</div>
    {/each}
    {#if !detail?.client?.id}
      <div class="empty-box">Р’С‹Р±РµСЂРё РєР»РёРµРЅС‚Р°, С‡С‚РѕР±С‹ СѓРІРёРґРµС‚СЊ СЃРІРѕРґРєСѓ.</div>
    {:else}
      <section class="summary-block">
        <h3>Р¦РµР»Рё</h3>
        {#if detail.summary?.goals?.length}
          <ul>{#each detail.summary.goals as goal}<li>{goal.goal_name || goal.goal_type || `Р¦РµР»СЊ #${goal.id}`}</li>{/each}</ul>
        {:else}<div class="empty-mini">РђРєС‚РёРІРЅС‹С… С†РµР»РµР№ РїРѕРєР° РЅРµС‚.</div>{/if}
      </section>
      <section class="summary-block">
        <h3>KPI</h3>
        {#if detail.summary?.kpis?.length}
          <ul>{#each detail.summary.kpis as kpi}<li>{kpi.kpi_name || kpi.kpi_type || `KPI #${kpi.id}`}</li>{/each}</ul>
        {:else}<div class="empty-mini">РђРєС‚РёРІРЅС‹С… KPI РїРѕРєР° РЅРµС‚.</div>{/if}
      </section>
      <section class="summary-block">
        <h3>РћРіСЂР°РЅРёС‡РµРЅРёСЏ</h3>
        {#if detail.summary?.constraints?.length}
          <ul>{#each detail.summary.constraints as item}<li>{item.constraint_type}: {formatMetric(item.value)} {item.unit || ''}</li>{/each}</ul>
        {:else}<div class="empty-mini">РћРіСЂР°РЅРёС‡РµРЅРёСЏ РЅРµ Р·Р°РґР°РЅС‹.</div>{/if}
      </section>
      <section class="summary-block">
        <h3>РџР»Р°РЅ / С„Р°РєС‚</h3>
        <div class="metrics-grid">{#each metricsRows() as row}<div class="metric-card"><strong>{row.label}</strong><div>РџР»Р°РЅ: {formatMetric(row.plan)}</div><div>Р¤Р°РєС‚: {formatMetric(row.fact)}</div></div>{/each}</div>
      </section>
      <section class="summary-block">
        <h3>Р‘Р»РёР¶Р°Р№С€РёРµ РґРµР№СЃС‚РІРёСЏ Рё РїСЂРµРґСѓРїСЂРµР¶РґРµРЅРёСЏ</h3>
        {#if detail.summary?.actionItems?.length}
          <ul>{#each detail.summary.actionItems as item}<li><strong>{item.title || item.action_type || 'РЎРѕР±С‹С‚РёРµ'}</strong><div>{item.message || item.comment || 'вЂ”'}</div></li>{/each}</ul>
        {:else}<div class="empty-mini">РђРєС‚РёРІРЅС‹С… РїСЂРµРґСѓРїСЂРµР¶РґРµРЅРёР№ Рё РґРµР№СЃС‚РІРёР№ РЅРµС‚.</div>{/if}
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
  .form-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
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
  .primary-btn, .mini-btn { border-radius: 10px; border: 1px solid #dbe4f0; background: #fff; padding: 8px 12px; cursor: pointer; font-size: 12px; }
  .primary-btn { background: #0f172a; color: #fff; border-color: #0f172a; }
  .empty-box, .empty-mini, .alert-box, .ok-box { border-radius: 12px; padding: 12px; font-size: 13px; }
  .empty-box, .empty-mini { border: 1px dashed #cbd5e1; color: #64748b; background: #f8fafc; }
  .alert-box { border: 1px solid #fecaca; background: #fff1f2; color: #b91c1c; }
  .ok-box { border: 1px solid #bbf7d0; background: #f0fdf4; color: #166534; }
  @media (max-width: 1480px) { .clients-layout { grid-template-columns: 280px minmax(0, 1fr); } .right-col { grid-column: 1 / -1; } }
  @media (max-width: 1024px) { .clients-layout { grid-template-columns: 1fr; } .form-grid { grid-template-columns: 1fr; } }
</style>

