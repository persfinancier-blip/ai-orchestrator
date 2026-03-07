<script lang="ts">
  import { onDestroy, tick } from 'svelte';
  import JsonTreeView from '../components/JsonTreeView.svelte';
  import { shouldRetryStatus, retryDelayMs, groupRowsByAliases } from './apiBuilderRuntimeCore.js';
  export type ExistingTable = { schema_name: string; table_name: string };
  type HttpMethod = 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE';
  type DispatchMode = 'single' | 'group_by';
  type BindingTarget = 'header' | 'query' | 'body' | 'body_item';
  type PaginationParameterTarget = 'body' | 'query' | 'header' | 'auth';
  export let apiBase: string;
  export let apiJson: <T = any>(url: string, init?: RequestInit) => Promise<T>;
  export let headers: () => Record<string, string>;
  export let existingTables: ExistingTable[] = [];
  export let refreshTables: () => Promise<void>;
  export let initialApiStoreId: number | null = null;
  export let embeddedMode = false;
  const EMBEDDED_LAYOUT_BREAKPOINT = 1420;
  let compactLayoutByContainer = false;
  $: effectiveEmbeddedMode = embeddedMode || compactLayoutByContainer;

  type BindingRule = {
    id: string;
    alias: string;
    target: BindingTarget;
    path: string;
  };

  type DataModelTable = {
    id: string;
    schema: string;
    table: string;
    alias: string;
  };

  type DataModelJoin = {
    id: string;
    leftTableId: string;
    leftField: string;
    rightTableId: string;
    rightField: string;
    joinType: 'inner' | 'left';
  };

  type DataModelField = {
    id: string;
    tableId: string;
    field: string;
    alias: string;
    grouped?: boolean;
    dateMode?: 'raw' | 'process';
    dateAnchorPreset?: DateAnchorPreset;
    dateAddDays?: number;
    dateAddMonths?: number;
    dateFormatPreset?: DateFormatPreset;
  };

  type DataModelFilter = {
    id: string;
    tableId: string;
    field: string;
    operator: string;
    compareValue: string;
  };

  type DateBasePreset = 'today' | 'custom';
  type DateAnchorPreset =
    | 'raw'
    | 'start_of_day'
    | 'end_of_day'
    | 'start_of_week'
    | 'end_of_week'
    | 'start_of_month'
    | 'end_of_month'
    | 'start_of_year'
    | 'end_of_year';
  type DateFormatPreset = 'yyyy_mm_dd' | 'datetime_utc_z' | 'datetime_msk' | 'rfc3339_msk' | 'iso8601';

  type DataDateParameter = {
    id: string;
    alias: string;
    basePreset: DateBasePreset;
    customDate: string;
    anchorPreset: DateAnchorPreset;
    addDays: number;
    addMonths: number;
    formatPreset: DateFormatPreset;
    grouped?: boolean;
  };

  type DataApiParameterSourceType = 'system' | 'input' | 'output';

  type DataApiParameter = {
    id: string;
    sourceApiRef: string;
    sourceApiStoreId?: number;
    sourceType: DataApiParameterSourceType;
    sourceKey: string;
    alias: string;
    grouped?: boolean;
  };

  type PaginationParameter = {
    id: string;
    alias: string;
    responsePath: string;
    firstValue: string;
    requestTarget: PaginationParameterTarget;
    requestPath: string;
    applyForAllResponses: boolean;
  };

  type PaginationStopOperator =
    | 'equals'
    | 'not_equals'
    | 'gt'
    | 'gte'
    | 'lt'
    | 'lte'
    | 'contains'
    | 'not_contains'
    | 'is_empty'
    | 'not_empty';

  type PaginationStopRule = {
    id: string;
    responsePath: string;
    operator: PaginationStopOperator;
    compareValue: string;
  };

  type ResponseLogMode = 'minimal' | 'standard' | 'debug';

  type OAuth2ResponseMapping = {
    id: string;
    responsePath: string;
    alias: string;
  };

  type ApiDraft = {
    localId: string;
    storeId?: number;
    name: string;
    method: HttpMethod;
    baseUrl: string;
    path: string;
    headersJson: string;
    authJson: string;
    authMode: 'manual' | 'oauth2_client_credentials' | 'custom';
    oauth2TokenUrl: string;
    oauth2ClientId: string;
    oauth2ClientSecret: string;
    oauth2GrantType: string;
    oauth2TokenField: string;
    oauth2ExpiresField: string;
    oauth2TokenTypeField: string;
    oauth2RequestMethod: HttpMethod;
    oauth2RequestUrl: string;
    oauth2RequestHost: string;
    oauth2RequestPath: string;
    oauth2RequestHeadersJson: string;
    oauth2RequestQueryJson: string;
    oauth2RequestBodyJson: string;
    oauth2ResponseMappings: OAuth2ResponseMapping[];
    queryJson: string;
    bodyJson: string;
    paginationEnabled: boolean;
    paginationStrategy: 'none' | 'cursor_fields' | 'page_number' | 'offset_limit' | 'next_url' | 'custom';
    paginationTarget: 'query' | 'body';
    paginationDataPath: string;
    paginationPageParam: string;
    paginationStartPage: number;
    paginationLimitParam: string;
    paginationLimitValue: number;
    paginationCustomStrategy: string;
    paginationCursorReqPath1: string;
    paginationCursorReqPath2: string;
    paginationCursorResPath1: string;
    paginationCursorResPath2: string;
    paginationNextUrlPath: string;
    paginationUseMaxPages: boolean;
    paginationMaxPages: number;
    paginationUseDelay: boolean;
    paginationDelayMs: number;
    paginationStopOnMissingValue: boolean;
    paginationStopOnSameResponse: boolean;
    paginationSameResponseLimit: number;
    paginationStopOnHttpError: boolean;
    paginationStopRules: PaginationStopRule[];
    paginationParameters: PaginationParameter[];
    pickedPaths: string[];
    responseTargets: Array<{
      id: string;
      schema: string;
      table: string;
      fields: Array<{ id: string; responsePath: string; targetField: string }>;
    }>;
    description: string;
    sectionName: string;
    templateCode: string;
    exampleRequest: string;
    parameterDefinitions: ParameterDefinition[];
    dispatchMode: DispatchMode;
    executionMode: 'sync' | 'async';
    syncPlanner: 'entity_to_stop' | 'by_wave';
    asyncConcurrency: number;
    executionDelayMs: number;
    responseLogEnabled: boolean;
    responseLogMode: ResponseLogMode;
    responseLogWriteRequestPayload: boolean;
    responseLogWriteResponsePayload: boolean;
    responseLogWritePaginationValues: boolean;
    responseLogOnlyErrors: boolean;
    responseLogResponsePayloadLimit: number;
    responseLogSchema: string;
    responseLogTable: string;
    groupByAliases: string[];
    bodyItemsPath: string;
    previewRequestLimit: number;
    bindingRules: BindingRule[];
    dataTables: DataModelTable[];
    dataJoins: DataModelJoin[];
    dataFields: DataModelField[];
    dataFilters: DataModelFilter[];
    dataDateParams: DataDateParameter[];
    dataApiParams: DataApiParameter[];
  };

  type ParameterCondition = {
    id: string;
    schema: string;
    table: string;
    field: string;
    operator: string;
    compareMode: 'value' | 'column';
    compareValue: string;
    compareColumn?: string;
  };
  type ParameterDefinition = {
    id: string;
    alias: string;
    definition: string;
    sourceSchema: string;
    sourceTable: string;
    sourceField: string;
    conditions: ParameterCondition[];
  };
  const AUTH_MODE_OAUTH2 = 'oauth2_client_credentials';
  const DATA_JOIN_TYPES: Array<{ value: 'inner' | 'left'; label: string }> = [
    { value: 'inner', label: 'Только совпавшие' },
    { value: 'left', label: 'Оставить все из основной' }
  ];
  const DATA_FILTER_OPERATORS_BY_KIND: Record<string, Array<{ value: string; label: string }>> = {
    text: [
      { value: 'equals', label: 'равно' },
      { value: 'not_equals', label: 'не равно' },
      { value: 'contains', label: 'содержит' },
      { value: 'starts_with', label: 'начинается с' },
      { value: 'ends_with', label: 'заканчивается на' }
    ],
    number: [
      { value: 'equals', label: '=' },
      { value: 'not_equals', label: '!=' },
      { value: 'gt', label: '>' },
      { value: 'lt', label: '<' }
    ],
    boolean: [
      { value: 'equals', label: 'равно' },
      { value: 'not_equals', label: 'не равно' }
    ],
    date: [
      { value: 'equals', label: 'в дату' },
      { value: 'not_equals', label: 'не в дату' },
      { value: 'before', label: 'раньше' },
      { value: 'after', label: 'позже' }
    ]
  };
  const DATE_BASE_PRESETS: Array<{ value: DateBasePreset; label: string }> = [
    { value: 'today', label: 'Сегодня' },
    { value: 'custom', label: 'Свободная дата' }
  ];
  const DATE_ANCHOR_PRESETS: Array<{ value: DateAnchorPreset; label: string }> = [
    { value: 'raw', label: 'Как есть' },
    { value: 'start_of_day', label: 'Начало дня' },
    { value: 'end_of_day', label: 'Конец дня' },
    { value: 'start_of_week', label: 'Начало недели' },
    { value: 'end_of_week', label: 'Конец недели' },
    { value: 'start_of_month', label: 'Начало месяца' },
    { value: 'end_of_month', label: 'Конец месяца' },
    { value: 'start_of_year', label: 'Начало года' },
    { value: 'end_of_year', label: 'Конец года' }
  ];
  const DATE_FORMAT_PRESETS: Array<{ value: DateFormatPreset; label: string; example: string }> = [
    { value: 'yyyy_mm_dd', label: 'YYYY-MM-DD', example: '2024-03-01' },
    { value: 'datetime_utc_z', label: 'YYYY-MM-DDTHH:MM:SSZ (UTC)', example: '2024-08-01T23:59:59Z' },
    { value: 'datetime_msk', label: 'YYYY-MM-DDTHH:MM:SS (МСК)', example: '2024-08-01T23:59:59' },
    { value: 'rfc3339_msk', label: 'RFC3339 (+03:00)', example: '2019-06-20T00:00:00+03:00' },
    { value: 'iso8601', label: 'ISO 8601 (с миллисекундами)', example: '2024-08-01T23:59:59.000Z' }
  ];
  const PAGINATION_STOP_OPERATORS: Array<{ value: PaginationStopOperator; label: string }> = [
    { value: 'equals', label: 'равно' },
    { value: 'not_equals', label: 'не равно' },
    { value: 'gt', label: 'больше' },
    { value: 'gte', label: 'больше или равно' },
    { value: 'lt', label: 'меньше' },
    { value: 'lte', label: 'меньше или равно' },
    { value: 'contains', label: 'содержит' },
    { value: 'not_contains', label: 'не содержит' },
    { value: 'is_empty', label: 'пусто' },
    { value: 'not_empty', label: 'не пусто' }
  ];

  const API_STORAGE_REQUIRED_COLUMNS: Array<{ name: string; types: string[] }> = [
    { name: 'id', types: ['bigint', 'int8', 'bigserial', 'integer', 'int4', 'int'] },
    { name: 'api_name', types: ['text', 'character varying', 'varchar'] },
    { name: 'config_json', types: ['jsonb', 'json'] },
    { name: 'schema_version', types: ['integer', 'int', 'int4'] },
    { name: 'revision', types: ['integer', 'int', 'int4'] },
    { name: 'description', types: ['text', 'character varying', 'varchar'] },
    { name: 'is_active', types: ['boolean'] },
    { name: 'updated_at', types: ['timestamp with time zone', 'timestamptz', 'timestamp'] },
    { name: 'updated_by', types: ['text', 'character varying', 'varchar'] }
  ];

  const API_LOG_TEMPLATE_REQUIRED_COLUMNS: Array<{ name: string; types: string[] }> = [
    { name: 'ao_source', types: ['text', 'character varying', 'varchar'] },
    { name: 'ao_run_id', types: ['text', 'character varying', 'varchar'] },
    { name: 'ao_created_at', types: ['timestamp with time zone', 'timestamptz', 'timestamp'] },
    { name: 'ao_updated_at', types: ['timestamp with time zone', 'timestamptz', 'timestamp'] },
    { name: 'ao_contract_schema', types: ['text', 'character varying', 'varchar'] },
    { name: 'ao_contract_name', types: ['text', 'character varying', 'varchar'] },
    { name: 'ao_contract_version', types: ['integer', 'int', 'int4', 'bigint', 'int8'] },
    { name: 'run_id', types: ['text', 'character varying', 'varchar'] },
    { name: 'api_name', types: ['text', 'character varying', 'varchar'] },
    { name: 'execution_mode', types: ['text', 'character varying', 'varchar'] },
    { name: 'sync_planner', types: ['text', 'character varying', 'varchar'] },
    { name: 'dispatch_mode', types: ['text', 'character varying', 'varchar'] },
    { name: 'entity_key', types: ['text', 'character varying', 'varchar'] },
    { name: 'entity_label', types: ['text', 'character varying', 'varchar'] },
    { name: 'row_index', types: ['integer', 'int', 'int4', 'bigint', 'int8'] },
    { name: 'wave_no', types: ['integer', 'int', 'int4', 'bigint', 'int8'] },
    { name: 'page_no', types: ['integer', 'int', 'int4', 'bigint', 'int8'] },
    { name: 'iteration_reason', types: ['text', 'character varying', 'varchar'] },
    { name: 'decision', types: ['text', 'character varying', 'varchar'] },
    { name: 'stop_reason', types: ['text', 'character varying', 'varchar'] },
    { name: 'error_message', types: ['text', 'character varying', 'varchar'] },
    { name: 'status_code', types: ['integer', 'int', 'int4', 'bigint', 'int8'] },
    { name: 'request_payload', types: ['jsonb', 'json'] },
    { name: 'response_payload', types: ['jsonb', 'json'] },
    { name: 'pagination_values', types: ['jsonb', 'json'] },
    { name: 'duration_ms', types: ['integer', 'int', 'int4', 'bigint', 'int8', 'numeric', 'decimal'] },
    { name: 'created_at', types: ['timestamp with time zone', 'timestamptz', 'timestamp'] },
    { name: 'updated_at', types: ['timestamp with time zone', 'timestamptz', 'timestamp'] }
  ];

  const CONDITION_OPERATORS: Record<'text' | 'number' | 'date' | 'boolean', Array<{ value: string; label: string }>> = {
    text: [
      { value: 'equals', label: 'равно' },
      { value: 'contains', label: 'содержит' },
      { value: 'starts_with', label: 'начинается с' },
      { value: 'ends_with', label: 'заканчивается на' }
    ],
    number: [
      { value: 'equals', label: '=' },
      { value: 'gt', label: '>' },
      { value: 'lt', label: '<' },
      { value: 'between', label: 'между' }
    ],
    date: [
      { value: 'equals', label: '=' },
      { value: 'before', label: 'раньше' },
      { value: 'after', label: 'позже' }
    ],
    boolean: [
      { value: 'equals', label: '=' },
      { value: 'not_equals', label: '≠' }
    ]
  };

  let drafts: ApiDraft[] = [];
  let selectedRef = '';
  let nameDraft = '';

  let api_storage_schema = 'ao_system';
  let api_storage_table = 'api_configs_store';
  let api_storage_picker_open = false;
  let api_storage_pick_value = '';
  let response_log_picker_open = false;
  let response_log_pick_value = '';
  let responseLogTableCompatibility: Record<string, { ok: boolean; reason: string }> = {};
  let responseLogTablesRefreshing = false;

  let loading = false;
  let saving = false;
  let checking = false;
  let err = '';
  let ok = '';
  let warn = '';
  let selectedBaselineSignature = '';
  let canSaveSelected = false;
  let canAddTemplate = false;
  let initialApiStoreIdApplied = 0;
  let nameDuplicateHint = '';

  let requestInput = '';
  let responseStatus = 0;
  let responseText = '';
  let responseJson: any = null;
  let responseIsJson = false;
  let responseTimeMs = 0;
  let responsePayloadCount = 0;
  let responsePagesCount = 0;
  let responsePayloadSize = 0;
  let responseViewMode: 'tree' | 'raw' = 'tree';
  let exampleJson: any = null;
  let exampleIsJson = false;
  let exampleViewMode: 'tree' | 'raw' = 'raw';
  let myPreviewJson: any = null;
  let myPreviewIsJson = false;
  let myPreviewViewMode: 'tree' | 'raw' = 'tree';
  let myPreviewModeLabel = '-';
  let myPreviewSourceLabel = '-';
  let myPreviewTotalRequests: number | null = null;
  let myPreviewShownRequests: number | null = null;
  let myApiPreviewDraft = '';
  let myPreviewDirty = false;
  let myPreviewApplyMessage = '';
  let authJsonTree: any = null;
  let authJsonValid = false;
  let authViewMode: 'tree' | 'raw' = 'raw';
  let tokenHeadersJsonTree: any = null;
  let tokenHeadersJsonValid = false;
  let tokenHeadersViewMode: 'tree' | 'raw' = 'raw';
  let tokenQueryJsonTree: any = null;
  let tokenQueryJsonValid = false;
  let tokenQueryViewMode: 'tree' | 'raw' = 'raw';
  let tokenBodyJsonTree: any = null;
  let tokenBodyJsonValid = false;
  let tokenBodyViewMode: 'tree' | 'raw' = 'raw';
  let headersJsonTree: any = null;
  let headersJsonValid = false;
  let headersViewMode: 'tree' | 'raw' = 'raw';
  let queryJsonTree: any = null;
  let queryJsonValid = false;
  let queryViewMode: 'tree' | 'raw' = 'raw';
  let bodyJsonTree: any = null;
  let bodyJsonValid = false;
  let bodyViewMode: 'tree' | 'raw' = 'raw';
  let selected: ApiDraft | null = null;
  let myApiPreview = '';
  let lastSelectedRef = '';
  let responsePreviewEl: HTMLTextAreaElement | null = null;
  let exampleApiEl: HTMLTextAreaElement | null = null;
  let myPreviewEl: HTMLTextAreaElement | null = null;
  let descriptionEl: HTMLTextAreaElement | null = null;
  let authEl: HTMLTextAreaElement | null = null;
  let tokenHeadersEl: HTMLTextAreaElement | null = null;
  let tokenQueryEl: HTMLTextAreaElement | null = null;
  let tokenBodyEl: HTMLTextAreaElement | null = null;
  let headersEl: HTMLTextAreaElement | null = null;
  let queryEl: HTMLTextAreaElement | null = null;
  let bodyEl: HTMLTextAreaElement | null = null;
  let templateParseMessage = '';
  let activeResponseFieldRef = '';
  type ColumnMeta = { name: string; type?: string };
  let columnsCache: Record<string, ColumnMeta[]> = {};
  let responsePathOptions: string[] = [];
  let responsePathPickerOpen = false;
  let responsePathPick = '';
  let paginationCursorResponsePathOptions: string[] = [];
  let paginationCursorResponsePick = '';
  let activePaginationParameterId = '';
  let activePaginationParameter: PaginationParameter | null = null;
  let oauthTokenCache: Record<string, { token: string; tokenType: string; expiresAt: number }> = {};
  let selectedParameterId: string | null = null;
  let aliasParamEl: HTMLTextAreaElement | null = null;
  let definitionParamEl: HTMLTextAreaElement | null = null;
  let definitionDraft = '';
  let definitionError = '';
  let definitionDirty = false;
  let definitionUpdatingFromFields = false;
  let parameterPreviewRows: any[] = [];
  let parameterPreviewLoading = false;
  let parameterPreviewError = '';
  let definitionTree: any = null;
  let definitionTreeDisplay: any = null;
  let definitionViewMode: 'text' | 'tree' = 'text';
  let datasetPreviewRows: Array<Record<string, any>> = [];
  let datasetPreviewColumns: string[] = [];
  let datasetPreviewHasMore = false;
  let datasetPreviewLoading = false;
  let datasetPreviewError = '';
  let datasetPreviewTimer: ReturnType<typeof setTimeout> | null = null;
  let datasetPreviewRequestSeq = 0;
  let datasetPreviewSignature = '';
  let apiResponseCache: Record<string, any> = {};
  let activeDataTableId = '';
  let activeDataJoinId = '';
  let activeDataFilterId = '';
  let groupByAliasCandidates: string[] = [];
  type TemplateScopeFilter = 'all' | 'recent' | 'favorites';
  type TemplateMethodFilter = 'all' | HttpMethod;
  type TemplateListItem = {
    ref: string;
    draft: ApiDraft;
    section: string;
    method: HttpMethod;
    path: string;
    templateCode: string;
    searchText: string;
  };
  type TemplateSectionGroup = { section: string; items: TemplateListItem[] };
  const TEMPLATE_RECENT_LIMIT = 20;
  const TEMPLATE_FAVORITES_LIMIT = 200;
  let templateSearch = '';
  let templateScopeFilter: TemplateScopeFilter = 'all';
  let templateMethodFilter: TemplateMethodFilter = 'all';
  let templateSectionFilter = 'all';
  let templateFavoriteRefs: string[] = [];
  let templateRecentRefs: string[] = [];
  let collapsedTemplateSections: Record<string, boolean> = {};
  let templateListItems: TemplateListItem[] = [];
  let templateSectionOptions: string[] = [];
  let templateFilteredItems: TemplateListItem[] = [];
  let templateGroupedItems: TemplateSectionGroup[] = [];
  let templateScopeCounts = { all: 0, recent: 0, favorites: 0 };
  let templateListEmptyHint = 'Список шаблонов пуст.';
  const PARAMETER_PREVIEW_LIMIT = 5;
  const REQUEST_PREVIEW_MAX = 20;
  const DATA_MODEL_ROW_HARD_LIMIT = 100000;
  const PARAMETER_ROW_HARD_LIMIT = 100000;
  const MERGE_ROW_CAP = 5000;
  const EXECUTION_RESPONSE_PREVIEW_MAX = 200;
  const EXECUTION_AUDIT_FLUSH_BATCH = 200;
  const EXECUTION_RESPONSE_ENTRY_MAX_CHARS = 120000;
  const RETRY_MAX_ATTEMPTS_DEFAULT = 3;
  const RETRY_BASE_DELAY_MS_DEFAULT = 500;
  const RETRY_MAX_DELAY_MS_DEFAULT = 5000;
  const FILTER_SCOPE_ALIAS = '__alias__';
  const PARAMETER_TOKEN_RE = /\{\{\s*([^{}]+?)\s*\}\}/g;
  const PARAMETER_TOKEN_EXACT_RE = /^\{\{\s*([^{}]+?)\s*\}\}$/;

  function uid() {
    return `${Date.now()}_${Math.random().toString(16).slice(2)}`;
  }

  function normalizeTypeName(type: string) {
    return String(type || '').toLowerCase().trim();
  }

  function toHttpMethod(v: string): HttpMethod {
    return v === 'GET' || v === 'POST' || v === 'PUT' || v === 'PATCH' || v === 'DELETE' ? v : 'GET';
  }

  function toDispatchMode(v: string): DispatchMode {
    return v === 'group_by' ? 'group_by' : 'single';
  }

  function toBindingTarget(v: string): BindingTarget {
    return v === 'header' || v === 'query' || v === 'body' || v === 'body_item' ? v : 'body_item';
  }

  function toPaginationParameterTarget(v: string): PaginationParameterTarget {
    return v === 'query' || v === 'header' || v === 'auth' ? (v as PaginationParameterTarget) : 'body';
  }

  function toPaginationStopOperator(v: string): PaginationStopOperator {
    return [
      'equals',
      'not_equals',
      'gt',
      'gte',
      'lt',
      'lte',
      'contains',
      'not_contains',
      'is_empty',
      'not_empty'
    ].includes(v)
      ? (v as PaginationStopOperator)
      : 'equals';
  }

  function paginationStopOperatorNeedsValue(op: PaginationStopOperator) {
    return op !== 'is_empty' && op !== 'not_empty';
  }

  function tryObj(v: any): any {
    if (v && typeof v === 'object') return v;
    if (typeof v !== 'string') return {};
    try {
      const p = JSON.parse(v);
      return p && typeof p === 'object' ? p : {};
    } catch {
      return {};
    }
  }

  function toPrettyJson(v: any): string {
    return JSON.stringify(v && typeof v === 'object' ? v : {}, null, 2);
  }

  function parseObj(text: string): any {
    const s = String(text || '').trim();
    if (!s) return {};
    return JSON.parse(s);
  }

  function parseLooseObject(text: string): Record<string, any> {
    const src = String(text || '').trim();
    if (!src) return {};
    const normalized = src.replace(/^\s*\{/, '').replace(/\}\s*$/, '').trim();
    if (!normalized) return {};

    const out: Record<string, any> = {};
    const parts = normalized
      .split(/\r?\n|,/)
      .map((x) => x.trim())
      .filter(Boolean);
    for (const part of parts) {
      const idx = part.indexOf(':');
      if (idx <= 0) continue;
      const rawKey = part.slice(0, idx).trim();
      const rawVal = part.slice(idx + 1).trim();
      const key = rawKey.replace(/^['"]|['"]$/g, '').trim();
      let value = rawVal.replace(/^['"]|['"]$/g, '').trim();
      if (!key) continue;
      if (value === 'true') out[key] = true;
      else if (value === 'false') out[key] = false;
      else if (value === 'null') out[key] = null;
      else if (/^-?\d+(?:\.\d+)?$/.test(value)) out[key] = Number(value);
      else out[key] = value;
    }
    return out;
  }

  function quoteUnquotedTemplateTokensInJson(src: string): string {
    const text = String(src || '');
    if (!text.includes('{{')) return text;
    // Allow JSON placeholders without quotes: { "nmID": {{nmID}} } -> { "nmID": "{{nmID}}" }
    return text.replace(/(:\s*|\[\s*|,\s*)(\{\{\s*[^{}]+\s*\}\})(\s*(?=[,\}\]]))/g, '$1"$2"$3');
  }

  function parseJsonObjectField(label: string, text: string): Record<string, any> {
    const src = String(text || '').trim();
    if (!src) return {};
    let parsed: any;
    try {
      parsed = JSON.parse(src);
    } catch {
      try {
        parsed = JSON.parse(quoteUnquotedTemplateTokensInJson(src));
      } catch {
        const loose = parseLooseObject(src);
        if (Object.keys(loose).length) return loose;
        throw new Error(`${label}: некорректный JSON`);
      }
    }
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new Error(`${label}: ожидается JSON-объект`);
    }
    return parsed;
  }

  function parseJsonAnyField(label: string, text: string): any {
    const src = String(text || '').trim();
    if (!src) return {};
    try {
      return JSON.parse(src);
    } catch {
      try {
        return JSON.parse(quoteUnquotedTemplateTokensInJson(src));
      } catch {
        throw new Error(`${label}: некорректный JSON`);
      }
    }
  }

function safePreviewObj(text: string): any {
  try {
    return parseJsonObjectField('preview', text);
  } catch {
    const src = String(text || '').trim();
    return { __raw: src, __error: 'Некорректный JSON' };
  }
}

function countPayloadItems(payload: any) {
  if (Array.isArray(payload)) return payload.length;
  if (payload && typeof payload === 'object') return 1;
  return 0;
}

function formatBytes(bytes: number) {
  if (!Number.isFinite(bytes) || bytes <= 0) return '-';
  if (bytes < 1024) return `${Math.round(bytes)} B`;
  return `${(bytes / 1024).toFixed(1)} КБ`;
}

  function refOf(d: ApiDraft): string {
    const sid = parsePositiveInt(d?.storeId);
    return sid > 0 ? `db:${sid}` : `tmp:${d.localId}`;
  }

  function byRef(ref: string): ApiDraft | null {
    const v = String(ref || '').trim();
    if (!v) return null;
    if (v.startsWith('db:')) {
      const id = Number(v.slice(3));
      if (Number.isFinite(id) && id > 0) return drafts.find((x) => Number(x.storeId || 0) === id) || null;
      return null;
    }
    if (v.startsWith('tmp:')) {
      return drafts.find((x) => x.localId === v.slice(4)) || null;
    }
    return null;
  }

  function baseDraft(): ApiDraft {
    return {
      localId: uid(),
      name: 'API',
      method: 'GET',
      baseUrl: '',
      path: '/',
      headersJson: '',
      authJson: '',
      authMode: 'manual',
      oauth2TokenUrl: '',
      oauth2ClientId: '',
      oauth2ClientSecret: '',
      oauth2GrantType: 'client_credentials',
      oauth2TokenField: 'access_token',
      oauth2ExpiresField: 'expires_in',
      oauth2TokenTypeField: 'token_type',
      oauth2RequestMethod: 'POST',
      oauth2RequestUrl: '',
      oauth2RequestHost: '',
      oauth2RequestPath: '',
      oauth2RequestHeadersJson: toPrettyJson({
        'Content-Type': 'application/json',
        Accept: 'application/json'
      }),
      oauth2RequestQueryJson: toPrettyJson({}),
      oauth2RequestBodyJson: toPrettyJson({
        client_id: '{{client_id}}',
        client_secret: '{{client_secret}}',
        grant_type: 'client_credentials'
      }),
      oauth2ResponseMappings: [
        { id: uid(), responsePath: 'access_token', alias: 'access_token' },
        { id: uid(), responsePath: 'expires_in', alias: 'expires_in' },
        { id: uid(), responsePath: 'token_type', alias: 'token_type' }
      ],
      queryJson: '',
      bodyJson: '',
      paginationEnabled: false,
      paginationStrategy: 'none',
      paginationTarget: 'body',
      paginationDataPath: '',
      paginationPageParam: 'page',
      paginationStartPage: 1,
      paginationLimitParam: 'limit',
      paginationLimitValue: 100,
      paginationCursorReqPath1: '',
      paginationCursorReqPath2: '',
      paginationCursorResPath1: '',
      paginationCursorResPath2: '',
      paginationNextUrlPath: 'next',
      paginationUseMaxPages: true,
      paginationMaxPages: 3,
      paginationUseDelay: false,
      paginationDelayMs: 0,
      paginationStopOnMissingValue: true,
      paginationStopOnSameResponse: true,
      paginationSameResponseLimit: 5,
      paginationStopOnHttpError: true,
      paginationStopRules: [],
      paginationCustomStrategy: '',
      paginationParameters: [],
      pickedPaths: [],
      responseTargets: [],
      description: '',
      sectionName: '',
      templateCode: '',
      exampleRequest: '',
      parameterDefinitions: [],
      dispatchMode: 'single',
      executionMode: 'sync',
      syncPlanner: 'entity_to_stop',
      asyncConcurrency: 3,
      executionDelayMs: 0,
      responseLogEnabled: false,
      responseLogMode: 'standard',
      responseLogWriteRequestPayload: true,
      responseLogWriteResponsePayload: true,
      responseLogWritePaginationValues: true,
      responseLogOnlyErrors: false,
      responseLogResponsePayloadLimit: 120000,
      responseLogSchema: 'bronze',
      responseLogTable: 'api_step_log',
      groupByAliases: [],
      bodyItemsPath: 'items',
      previewRequestLimit: 5,
      bindingRules: [],
      dataTables: [],
      dataJoins: [],
      dataFields: [],
      dataFilters: [],
      dataDateParams: [],
      dataApiParams: []
    };
  }

  function fromRow(row: any): ApiDraft {
    const d = baseDraft();
    const storeIdNum = extractStoreIdFromRow(row);
    const config = tryObj(row?.config_json);

    const mapping = tryObj(row?.mapping_json);
    const legacy = tryObj(mapping?.source ?? mapping?.config ?? mapping?.payload);
    const pagination = tryObj(row?.pagination_json || mapping?.pagination || legacy?.pagination);
    const oauth2 = tryObj(mapping?.oauth2 || legacy?.oauth2);
    const oauth2Flow = tryObj(
      mapping?.oauth2_flow ||
        mapping?.oauth2Flow ||
        config?.oauth2_flow ||
        legacy?.oauth2_flow ||
        legacy?.oauth2Flow
    );
    const parameterDefinitionsRaw = Array.isArray(row?.parameter_definitions)
      ? row.parameter_definitions
      : Array.isArray(mapping?.parameter_definitions)
      ? mapping.parameter_definitions
      : Array.isArray(legacy?.parameter_definitions)
      ? legacy.parameter_definitions
      : [];
    const parsedTargets = Array.isArray(mapping?.response_targets) ? mapping.response_targets : [];
    const responseTargetsSource = Array.isArray(row?.response_targets) ? row.response_targets : parsedTargets;
    const normalizedTargets = responseTargetsSource
      .map((t: any) => ({
        id: uid(),
        schema: String(t?.schema || ''),
        table: String(t?.table || ''),
        fields: Array.isArray(t?.fields)
          ? t.fields.map((f: any) => ({
              id: uid(),
              responsePath: String(f?.responsePath || ''),
              targetField: String(f?.targetField || '')
            }))
          : []
      }))
      .filter((t: any) => t.schema || t.table || t.fields.length);
    if (!normalizedTargets.length) {
      const fallbackSchema = String(row?.target_schema || legacy?.targetSchema || '');
      const fallbackTable = String(row?.target_table || legacy?.targetTable || '');
      if (fallbackSchema || fallbackTable) {
        normalizedTargets.push({
          id: uid(),
          schema: fallbackSchema,
          table: fallbackTable,
          fields: [{ id: uid(), responsePath: '', targetField: '' }]
        });
      }
    }

    const normalizedDefinitions = parameterDefinitionsRaw.map((pd: any) => ({
      id: String(pd?.id || uid()),
      alias: String(pd?.alias || `param_${uid()}`),
      definition: String(pd?.definition || ''),
      sourceSchema: String(pd?.source_schema || pd?.sourceSchema || ''),
      sourceTable: String(pd?.source_table || pd?.sourceTable || ''),
      sourceField: String(pd?.source_field || pd?.sourceField || ''),
      conditions: Array.isArray(pd?.conditions)
        ? pd.conditions.map((cond: any) => ({
            id: String(cond?.id || uid()),
            schema: String(cond?.schema || ''),
            table: String(cond?.table || ''),
            field: String(cond?.field || ''),
            operator: String(cond?.operator || 'equals'),
            compareMode: String(cond?.compare_mode || cond?.compareMode || 'value') === 'column' ? 'column' : 'value',
            compareValue: String(cond?.compare_value || cond?.compareValue || ''),
            compareColumn: cond?.compare_column || cond?.compareColumn || ''
          }))
        : []
    }));

    const pickedPathsSource = Array.isArray(row?.picked_paths)
      ? row.picked_paths
      : Array.isArray(mapping?.picked_paths)
      ? mapping.picked_paths
      : [];
    const executionCfg = tryObj(mapping?.execution || row?.execution_json || legacy?.execution);
    const bindingRulesRaw = Array.isArray(row?.binding_rules)
      ? row.binding_rules
      : Array.isArray(executionCfg?.binding_rules)
      ? executionCfg.binding_rules
      : [];
    const normalizedBindingRules = bindingRulesRaw
      .map((rule: any) => ({
        id: String(rule?.id || uid()),
        alias: String(rule?.alias || '').trim(),
        target: toBindingTarget(String(rule?.target || 'body_item')),
        path: String(rule?.path || '').trim()
      }))
      .filter((rule: BindingRule) => rule.alias && rule.path);
    const groupByRaw = Array.isArray(row?.group_by_aliases)
      ? row.group_by_aliases
      : Array.isArray(executionCfg?.group_by_aliases)
      ? executionCfg.group_by_aliases
      : [];
    const normalizedGroupBy = [...new Set(groupByRaw.map((x: any) => String(x || '').trim()).filter(Boolean))];
    const normalizedGroupBySet = new Set(normalizedGroupBy.map((x) => x.toLowerCase()));
    const dataModelCfg = tryObj(executionCfg?.data_model || mapping?.data_model || row?.data_model_json);
    const normalizedDataTables = (Array.isArray(dataModelCfg?.tables) ? dataModelCfg.tables : [])
      .map((t: any, idx: number) => ({
        id: String(t?.id || `t${idx}`),
        schema: String(t?.schema || '').trim(),
        table: String(t?.table || '').trim(),
        alias: String(t?.alias || t?.table || `table_${idx + 1}`).trim()
      }))
      .filter((t: DataModelTable) => t.id && t.schema && t.table);
    const dataTableIdSet = new Set(normalizedDataTables.map((t: DataModelTable) => t.id));
    const normalizedDataJoins = (Array.isArray(dataModelCfg?.joins) ? dataModelCfg.joins : [])
      .map((j: any, idx: number) => ({
        id: String(j?.id || `j${idx}`),
        leftTableId: String(j?.left_table_id || j?.leftTableId || '').trim(),
        leftField: String(j?.left_field || j?.leftField || '').trim(),
        rightTableId: String(j?.right_table_id || j?.rightTableId || '').trim(),
        rightField: String(j?.right_field || j?.rightField || '').trim(),
        joinType: String(j?.join_type || j?.joinType || 'inner').toLowerCase() === 'left' ? 'left' : 'inner'
      }))
      .filter(
        (j: DataModelJoin) =>
          j.leftTableId &&
          j.rightTableId &&
          j.leftField &&
          j.rightField &&
          dataTableIdSet.has(j.leftTableId) &&
          dataTableIdSet.has(j.rightTableId)
      );
    const normalizedDataFields = (Array.isArray(dataModelCfg?.fields) ? dataModelCfg.fields : [])
      .map((f: any, idx: number) => ({
        id: String(f?.id || `f${idx}`),
        tableId: String(f?.table_id || f?.tableId || '').trim(),
        field: String(f?.field || '').trim(),
        alias: String(f?.alias || f?.field || `field_${idx + 1}`).trim(),
        grouped:
          Boolean(f?.group_by ?? f?.groupBy) ||
          normalizedGroupBySet.has(String(f?.alias || '').trim().toLowerCase()),
        dateMode:
          String((f?.date_mode ?? f?.dateMode ?? (f?.date_transform?.enabled ? 'process' : 'raw')) || 'raw').toLowerCase() ===
          'process'
            ? 'process'
            : 'raw',
        dateAnchorPreset: toDateAnchorPreset(
          String(f?.date_anchor_preset ?? f?.dateAnchorPreset ?? f?.date_transform?.anchor_preset ?? f?.date_transform?.anchor ?? 'raw')
        ),
        dateAddDays: Number.isFinite(Number(f?.date_add_days ?? f?.dateAddDays ?? f?.date_transform?.add_days))
          ? Number(f?.date_add_days ?? f?.dateAddDays ?? f?.date_transform?.add_days)
          : 0,
        dateAddMonths: Number.isFinite(Number(f?.date_add_months ?? f?.dateAddMonths ?? f?.date_transform?.add_months))
          ? Number(f?.date_add_months ?? f?.dateAddMonths ?? f?.date_transform?.add_months)
          : 0,
        dateFormatPreset: toDateFormatPreset(
          String(f?.date_format_preset ?? f?.dateFormatPreset ?? f?.date_transform?.format_preset ?? f?.date_transform?.format ?? 'yyyy_mm_dd')
        )
      }))
      .filter((f: DataModelField) => f.tableId && f.field && f.alias && dataTableIdSet.has(f.tableId));
    const normalizedDataFilters = (Array.isArray(dataModelCfg?.filters) ? dataModelCfg.filters : [])
      .map((f: any, idx: number) => ({
        id: String(f?.id || `flt_${idx}`),
        tableId: String(f?.table_id || f?.tableId || '').trim(),
        field: String(f?.field || '').trim(),
        operator: String(f?.operator || 'equals').trim(),
        compareValue: String(f?.compare_value || f?.compareValue || '').trim()
      }))
      .filter((f: DataModelFilter) => f.tableId && f.field && (f.tableId === FILTER_SCOPE_ALIAS || dataTableIdSet.has(f.tableId)));
    const dataDateParamsRaw = Array.isArray(dataModelCfg?.date_parameters)
      ? dataModelCfg.date_parameters
      : Array.isArray((mapping as any)?.data_date_parameters)
      ? (mapping as any).data_date_parameters
      : Array.isArray((row as any)?.data_date_parameters)
      ? (row as any).data_date_parameters
      : [];
    const normalizedDataDateParams = dataDateParamsRaw
      .map((p: any, idx: number) =>
        normalizeDataDateParameter(
          {
            id: String(p?.id || ''),
            alias: String(p?.alias || `date_${idx + 1}`),
            basePreset: String(p?.base_preset || p?.basePreset || p?.base || 'today') as DateBasePreset,
            customDate: String(p?.custom_date || p?.customDate || ''),
            anchorPreset: String(p?.anchor_preset || p?.anchorPreset || p?.anchor || 'raw') as DateAnchorPreset,
            addDays: Number(p?.add_days ?? p?.addDays ?? 0),
            addMonths: Number(p?.add_months ?? p?.addMonths ?? 0),
            formatPreset: String(p?.format_preset || p?.formatPreset || p?.format || 'yyyy_mm_dd') as DateFormatPreset,
            grouped: Boolean(p?.grouped)
          },
          idx + 1
        )
      )
      .filter((p: DataDateParameter) => String(p.alias || '').trim());
    const dataApiParamsRaw = Array.isArray(dataModelCfg?.api_parameters)
      ? dataModelCfg.api_parameters
      : Array.isArray((mapping as any)?.data_api_parameters)
      ? (mapping as any).data_api_parameters
      : Array.isArray((row as any)?.data_api_parameters)
      ? (row as any).data_api_parameters
      : [];
    const normalizedDataApiParams = dataApiParamsRaw
      .map((p: any, idx: number) =>
        normalizeDataApiParameter(
          {
            id: String(p?.id || ''),
            sourceApiRef: String(p?.source_api_ref || p?.sourceApiRef || ''),
            sourceApiStoreId: Number(p?.source_api_id ?? p?.sourceApiStoreId ?? 0),
            sourceType: String(p?.source_type || p?.sourceType || 'system') as DataApiParameterSourceType,
            sourceKey: String(p?.source_key || p?.sourceKey || ''),
            alias: String(p?.alias || `api_param_${idx + 1}`),
            grouped: Boolean(p?.grouped)
          },
          idx + 1
        )
      )
      .filter((p: DataApiParameter) => String(p.alias || '').trim());
    const dispatchMode = toDispatchMode(String(row?.dispatch_mode || executionCfg?.dispatch_mode || 'single'));
    const executionModeRaw = String(row?.execution_mode || executionCfg?.execution_mode || 'sync').trim().toLowerCase();
    const executionMode: 'sync' | 'async' = executionModeRaw === 'async' ? 'async' : 'sync';
    const syncPlannerRaw = String(row?.sync_planner || executionCfg?.sync_planner || 'entity_to_stop').trim().toLowerCase();
    const syncPlanner: 'entity_to_stop' | 'by_wave' = syncPlannerRaw === 'by_wave' ? 'by_wave' : 'entity_to_stop';
    const asyncConcurrency = Math.max(
      1,
      Math.min(
        20,
        Number.isFinite(Number(row?.async_concurrency))
          ? Number(row?.async_concurrency)
          : Number(executionCfg?.async_concurrency || 3)
      )
    );
    const executionDelayMs = Math.max(
      0,
      Number.isFinite(Number(row?.execution_delay_ms))
        ? Number(row?.execution_delay_ms)
        : Number(executionCfg?.execution_delay_ms || 0)
    );
    const responseLogCfg = executionCfg?.response_log && typeof executionCfg.response_log === 'object'
      ? executionCfg.response_log
      : {};
    const responseLogEnabled =
      (row as any)?.response_log_enabled === undefined
        ? Boolean((responseLogCfg as any)?.enabled)
        : Boolean((row as any)?.response_log_enabled);
    const responseLogModeRaw = String((responseLogCfg as any)?.mode || 'standard').trim().toLowerCase();
    const responseLogMode: ResponseLogMode =
      responseLogModeRaw === 'minimal' || responseLogModeRaw === 'debug' ? (responseLogModeRaw as ResponseLogMode) : 'standard';
    const responseLogWriteRequestPayload =
      (responseLogCfg as any)?.write_request_payload === undefined
        ? true
        : Boolean((responseLogCfg as any)?.write_request_payload);
    const responseLogWriteResponsePayload =
      (responseLogCfg as any)?.write_response_payload === undefined
        ? true
        : Boolean((responseLogCfg as any)?.write_response_payload);
    const responseLogWritePaginationValues =
      (responseLogCfg as any)?.write_pagination_values === undefined
        ? true
        : Boolean((responseLogCfg as any)?.write_pagination_values);
    const responseLogOnlyErrors = Boolean((responseLogCfg as any)?.only_errors);
    const responseLogResponsePayloadLimit = Math.max(
      0,
      Number.isFinite(Number((responseLogCfg as any)?.response_payload_limit))
        ? Number((responseLogCfg as any)?.response_payload_limit)
        : Number((responseLogCfg as any)?.response_payload_max_chars || 120000)
    );
    const responseLogSchema = String((row as any)?.response_log_schema || (responseLogCfg as any)?.schema || 'bronze').trim() || 'bronze';
    const responseLogTable = String((row as any)?.response_log_table || (responseLogCfg as any)?.table || 'api_step_log').trim() || 'api_step_log';
    const bodyItemsPath = String(row?.body_items_path || executionCfg?.body_items_path || 'items').trim() || 'items';
    const previewRequestLimit = Number.isFinite(Number(row?.preview_request_limit))
      ? Number(row?.preview_request_limit)
      : Number(executionCfg?.preview_request_limit || 5);

    const authJsonSource = tryObj(
      row?.auth_json || mapping?.auth_json || legacy?.auth_json || legacy?.authJson
    );

    const authModeRaw = String(row?.auth_mode || oauth2?.mode || 'manual').trim();
    const authMode = authModeRaw === 'oauth2_client_credentials' ? 'oauth2_client_credentials' : 'manual';
    const oauth2TokenUrl = String(row?.oauth2_token_url || oauth2?.token_url || '');
    const oauth2ClientId = String(row?.oauth2_client_id || oauth2?.client_id || '');
    const oauth2ClientSecret = String(row?.oauth2_client_secret || oauth2?.client_secret || '');
    const oauth2GrantType = String(row?.oauth2_grant_type || oauth2?.grant_type || 'client_credentials');
    const oauth2TokenField = String(row?.oauth2_token_field || oauth2?.token_field || 'access_token');
    const oauth2ExpiresField = String(row?.oauth2_expires_field || oauth2?.expires_field || 'expires_in');
    const oauth2TokenTypeField = String(row?.oauth2_token_type_field || oauth2?.token_type_field || 'token_type');
    const oauth2RequestRaw = tryObj(oauth2Flow?.request || oauth2Flow?.token_request);
    const oauth2RequestMethod = toHttpMethod(String(oauth2RequestRaw?.method || 'POST').toUpperCase());
    const oauth2RequestUrl = String(oauth2RequestRaw?.url || oauth2Flow?.token_url || oauth2TokenUrl || '').trim();
    let oauth2RequestHost = String(oauth2RequestRaw?.host || '').trim();
    let oauth2RequestPath = String(oauth2RequestRaw?.path || '').trim();
    if (oauth2RequestUrl && (!oauth2RequestHost || !oauth2RequestPath)) {
      try {
        const parsed = /^https?:\/\//i.test(oauth2RequestUrl)
          ? new URL(oauth2RequestUrl)
          : oauth2TokenUrl && /^https?:\/\//i.test(oauth2TokenUrl)
          ? new URL(resolveAbsoluteUrl(oauth2RequestUrl, { ...d, baseUrl: new URL(oauth2TokenUrl).origin } as ApiDraft))
          : null;
        if (parsed) {
          if (!oauth2RequestHost) oauth2RequestHost = `${parsed.protocol}//${parsed.host}`;
          if (!oauth2RequestPath) oauth2RequestPath = decodeUrlPathname(parsed.pathname);
        }
      } catch {
        // ignore parse errors
      }
    }
    if (!oauth2RequestPath && oauth2RequestUrl && !/^https?:\/\//i.test(oauth2RequestUrl)) {
      oauth2RequestPath = oauth2RequestUrl.startsWith('/') ? oauth2RequestUrl : `/${oauth2RequestUrl}`;
    }
    const oauth2RequestHeadersObj = tryObj(
      oauth2RequestRaw?.headers_json || oauth2RequestRaw?.headers || {
        'Content-Type': 'application/json',
        Accept: 'application/json'
      }
    );
    const oauth2RequestQueryObj = tryObj(oauth2RequestRaw?.query_json || oauth2RequestRaw?.query || {});
    const oauth2RequestBodyObj = tryObj(
      oauth2RequestRaw?.body_json || oauth2RequestRaw?.body || {
        client_id: oauth2ClientId || '{{client_id}}',
        client_secret: oauth2ClientSecret || '{{client_secret}}',
        grant_type: oauth2GrantType || 'client_credentials'
      }
    );
    const oauth2MappingsRaw = Array.isArray(oauth2Flow?.response_mappings)
      ? oauth2Flow.response_mappings
      : Array.isArray(oauth2Flow?.responseMappings)
      ? oauth2Flow.responseMappings
      : [];
    const oauth2ResponseMappings: OAuth2ResponseMapping[] = oauth2MappingsRaw
      .map((m: any) => ({
        id: String(m?.id || uid()),
        responsePath: String(m?.response_path || m?.responsePath || '').trim(),
        alias: String(m?.alias || '').trim()
      }))
      .filter((m: OAuth2ResponseMapping) => m.responsePath && m.alias);
    if (!oauth2ResponseMappings.length) {
      oauth2ResponseMappings.push(
        { id: uid(), responsePath: oauth2TokenField || 'access_token', alias: 'access_token' },
        { id: uid(), responsePath: oauth2ExpiresField || 'expires_in', alias: 'expires_in' },
        { id: uid(), responsePath: oauth2TokenTypeField || 'token_type', alias: 'token_type' }
      );
    }
    const exampleRequestValue = String(row?.example_request || mapping?.exampleRequest || legacy?.exampleRequest || '');
    const paginationEnabledValue =
      row?.pagination_enabled ?? Boolean(pagination?.enabled) ?? false;
    const paginationStrategyValue = String(row?.pagination_strategy || pagination?.strategy || 'none').trim();
    const paginationTargetValue = String(row?.pagination_target || pagination?.target || 'body').trim();
    const paginationDataPathValue = String(row?.pagination_data_path || pagination?.data_path || '');
    const paginationPageParamValue = String(row?.pagination_page_param || pagination?.page_param || 'page');
    const paginationStartPageValue = Number.isFinite(Number(row?.pagination_start_page))
      ? Number(row?.pagination_start_page)
      : Number(pagination?.start_page || 1);
    const paginationLimitParamValue = String(row?.pagination_limit_param || pagination?.limit_param || 'limit');
    const paginationLimitValueValue = Number.isFinite(Number(row?.pagination_limit_value))
      ? Number(row?.pagination_limit_value)
      : Number(pagination?.limit_value || 100);
    const paginationCursorReqPath1Value = String(row?.pagination_cursor_req_path_1 || pagination?.cursor_req_path_1 || '');
    const paginationCursorReqPath2Value = String(row?.pagination_cursor_req_path_2 || pagination?.cursor_req_path_2 || '');
    const paginationCursorResPath1Value = String(row?.pagination_cursor_res_path_1 || pagination?.cursor_res_path_1 || '');
    const paginationCursorResPath2Value = String(row?.pagination_cursor_res_path_2 || pagination?.cursor_res_path_2 || '');
    const paginationNextUrlPathValue = String(row?.pagination_next_url_path || pagination?.next_url_path || 'next');
    const paginationUseMaxPagesRaw =
      (row as any)?.pagination_use_max_pages ??
      (pagination as any)?.use_max_pages ??
      (pagination as any)?.max_pages_enabled;
    const paginationUseMaxPages =
      paginationUseMaxPagesRaw === undefined || paginationUseMaxPagesRaw === null
        ? true
        : Boolean(paginationUseMaxPagesRaw);
    const paginationMaxPagesValue = Number.isFinite(Number(row?.pagination_max_pages))
      ? Number(row?.pagination_max_pages)
      : Number(pagination?.max_pages || 3);
    const paginationDelayMsValue = Number.isFinite(Number(row?.pagination_delay_ms))
      ? Number(row?.pagination_delay_ms)
      : Number(pagination?.delay_ms || 0);
    const paginationUseDelayRaw =
      (row as any)?.pagination_use_delay ??
      (pagination as any)?.use_delay ??
      (pagination as any)?.delay_enabled;
    const paginationUseDelay =
      paginationUseDelayRaw === undefined || paginationUseDelayRaw === null
        ? Number(paginationDelayMsValue || 0) > 0
        : Boolean(paginationUseDelayRaw);
    const paginationCustomStrategyValue = String(
      row?.pagination_custom_strategy || pagination?.custom_strategy || ''
    );
    const stopConditions = pagination?.stop_conditions && typeof pagination.stop_conditions === 'object'
      ? pagination.stop_conditions
      : {};
    const paginationStopOnMissingRaw =
      (row as any)?.pagination_stop_on_missing_value ?? (stopConditions as any)?.on_missing_pagination_value;
    const paginationStopOnSameRaw =
      (row as any)?.pagination_stop_on_same_response ?? (stopConditions as any)?.on_same_response;
    const paginationSameResponseLimitRaw =
      (row as any)?.pagination_same_response_limit ?? (stopConditions as any)?.same_response_limit;
    const paginationStopOnHttpErrorRaw =
      (row as any)?.pagination_stop_on_http_error ?? (stopConditions as any)?.on_http_error;
    const paginationStopOnMissingValue =
      paginationStopOnMissingRaw === undefined || paginationStopOnMissingRaw === null
        ? true
        : Boolean(paginationStopOnMissingRaw);
    const paginationStopOnSameResponse =
      paginationStopOnSameRaw === undefined || paginationStopOnSameRaw === null
        ? true
        : Boolean(paginationStopOnSameRaw);
    const paginationSameResponseLimit = Math.max(
      2,
      Math.min(
        50,
        Number.isFinite(Number(paginationSameResponseLimitRaw))
          ? Number(paginationSameResponseLimitRaw)
          : 5
      )
    );
    const paginationStopOnHttpError =
      paginationStopOnHttpErrorRaw === undefined || paginationStopOnHttpErrorRaw === null
        ? true
        : Boolean(paginationStopOnHttpErrorRaw);
    const paginationStopRulesRaw = Array.isArray((stopConditions as any)?.response_rules)
      ? (stopConditions as any).response_rules
      : Array.isArray((stopConditions as any)?.rules)
      ? (stopConditions as any).rules
      : [];
    const normalizedPaginationStopRules: PaginationStopRule[] = paginationStopRulesRaw
      .map((rule: any) =>
        normalizePaginationStopRule(
          {
            id: String(rule?.id || ''),
            responsePath: String(rule?.response_path || rule?.responsePath || rule?.path || ''),
            operator: String(rule?.operator || 'equals'),
            compareValue: String(rule?.compare_value || rule?.compareValue || rule?.value || '')
          }
        )
      )
      .filter((rule: PaginationStopRule) => rule.responsePath);
    const paginationParamsRaw = Array.isArray(pagination?.parameters)
      ? pagination.parameters
      : Array.isArray((mapping as any)?.pagination_parameters)
      ? (mapping as any).pagination_parameters
      : [];
    const normalizedPaginationParameters: PaginationParameter[] = paginationParamsRaw
      .map((p: any, idx: number) => ({
        id: String(p?.id || uid()),
        alias: String(p?.alias || `pg_${idx + 1}`).trim(),
        responsePath: String(p?.response_path || p?.responsePath || '').trim(),
        firstValue: String(p?.first_value || p?.firstValue || '').trim(),
        requestTarget: ['body', 'query', 'header', 'auth'].includes(String(p?.request_target || p?.requestTarget || '').trim())
          ? (String(p?.request_target || p?.requestTarget || '').trim() as PaginationParameterTarget)
          : 'body',
        requestPath: String(p?.request_path || p?.requestPath || '').trim(),
        applyForAllResponses: true
      }))
      .filter((p: PaginationParameter) => p.alias);
    if (!normalizedPaginationParameters.length) {
      const legacyTarget: PaginationParameterTarget = paginationTargetValue === 'query' ? 'query' : 'body';
      if (paginationCursorResPath1Value || paginationCursorReqPath1Value) {
        normalizedPaginationParameters.push({
          id: uid(),
          alias: 'cursor_1',
          responsePath: paginationCursorResPath1Value,
          firstValue: '',
          requestTarget: legacyTarget,
          requestPath: paginationCursorReqPath1Value,
          applyForAllResponses: true
        });
      }
      if (paginationCursorResPath2Value || paginationCursorReqPath2Value) {
        normalizedPaginationParameters.push({
          id: uid(),
          alias: 'cursor_2',
          responsePath: paginationCursorResPath2Value,
          firstValue: '',
          requestTarget: legacyTarget,
          requestPath: paginationCursorReqPath2Value,
          applyForAllResponses: true
        });
      }
    }

    return {
      ...d,
      localId: uid(),
      storeId: storeIdNum > 0 ? storeIdNum : undefined,
      name: String(row?.api_name || legacy?.name || 'API'),
      method: toHttpMethod(String(row?.method || legacy?.method || 'GET').toUpperCase()),
      baseUrl: String(row?.base_url || legacy?.base_url || legacy?.baseUrl || ''),
      path: String(row?.path || legacy?.path || '/'),
      headersJson: toPrettyJson(tryObj(row?.headers_json || legacy?.headers_json || legacy?.headersJson || legacy?.headers)),
      authJson: toPrettyJson(authJsonSource),
      authMode,
      oauth2TokenUrl,
      oauth2ClientId,
      oauth2ClientSecret,
      oauth2GrantType,
      oauth2TokenField,
      oauth2ExpiresField,
      oauth2TokenTypeField,
      oauth2RequestMethod,
      oauth2RequestUrl,
      oauth2RequestHost,
      oauth2RequestPath,
      oauth2RequestHeadersJson: toPrettyJson(oauth2RequestHeadersObj),
      oauth2RequestQueryJson: toPrettyJson(oauth2RequestQueryObj),
      oauth2RequestBodyJson: toPrettyJson(oauth2RequestBodyObj),
      oauth2ResponseMappings,
      queryJson: toPrettyJson(tryObj(row?.query_json || legacy?.query_json || legacy?.queryJson || legacy?.query || legacy?.params)),
      bodyJson: toPrettyJson(tryObj(row?.body_json || legacy?.body_json || legacy?.bodyJson || legacy?.body || legacy?.data)),
      paginationEnabled: paginationEnabledValue,
      paginationStrategy:
        ['cursor_fields', 'page_number', 'offset_limit', 'next_url'].includes(paginationStrategyValue)
          ? (paginationStrategyValue as ApiDraft['paginationStrategy'])
          : 'none',
      paginationTarget: paginationTargetValue === 'query' ? 'query' : 'body',
      paginationDataPath: paginationDataPathValue,
      paginationPageParam: paginationPageParamValue,
      paginationStartPage: paginationStartPageValue,
      paginationLimitParam: paginationLimitParamValue,
      paginationLimitValue: paginationLimitValueValue,
      paginationCursorReqPath1: paginationCursorReqPath1Value,
      paginationCursorReqPath2: paginationCursorReqPath2Value,
      paginationCursorResPath1: paginationCursorResPath1Value,
      paginationCursorResPath2: paginationCursorResPath2Value,
      paginationNextUrlPath: paginationNextUrlPathValue,
      paginationUseMaxPages,
      paginationMaxPages: paginationMaxPagesValue,
      paginationUseDelay,
      paginationDelayMs: paginationDelayMsValue,
      paginationStopOnMissingValue,
      paginationStopOnSameResponse,
      paginationSameResponseLimit,
      paginationStopOnHttpError,
      paginationStopRules: normalizedPaginationStopRules,
      paginationCustomStrategy: paginationCustomStrategyValue,
      paginationParameters: normalizedPaginationParameters,
      pickedPaths: pickedPathsSource.map((x: any) => String(x || '').trim()).filter(Boolean),
      responseTargets: normalizedTargets,
      description: String(row?.description || legacy?.description || ''),
      sectionName: String(
        row?.section_name ||
          config?.section ||
          config?.section_name ||
          config?.category ||
          legacy?.section ||
          ''
      ).trim(),
      templateCode: String(
        row?.template_code ||
          config?.template_code ||
          config?.code ||
          legacy?.template_code ||
          ''
      ).trim(),
      exampleRequest: exampleRequestValue,
      parameterDefinitions: normalizedDefinitions,
      dispatchMode,
      executionMode,
      syncPlanner,
      asyncConcurrency,
      executionDelayMs,
      responseLogEnabled,
      responseLogMode,
      responseLogWriteRequestPayload,
      responseLogWriteResponsePayload,
      responseLogWritePaginationValues,
      responseLogOnlyErrors,
      responseLogResponsePayloadLimit,
      responseLogSchema,
      responseLogTable,
      groupByAliases: normalizedGroupBy,
      bodyItemsPath,
      previewRequestLimit: Math.max(1, Math.min(50, Number(previewRequestLimit || 5))),
      bindingRules: normalizedBindingRules,
      dataTables: normalizedDataTables,
      dataJoins: normalizedDataJoins,
      dataFields: normalizedDataFields,
      dataFilters: normalizedDataFilters,
      dataDateParams: normalizedDataDateParams,
      dataApiParams: normalizedDataApiParams
    };
  }

  function toPayload(
    d: ApiDraft,
    parsed?: { headersJson: Record<string, any>; queryJson: Record<string, any>; bodyJson: any; authJson: Record<string, any> }
  ) {
    const sanitizedAliases = sanitizeAliasReferences(d);
    const firstTarget = d.responseTargets.find((t) => t.schema && t.table);
    const parameterDefinitionsPayload = d.parameterDefinitions.map((pd) => ({
      id: pd.id,
      alias: pd.alias,
      definition: pd.definition,
      source_schema: pd.sourceSchema,
      source_table: pd.sourceTable,
      source_field: pd.sourceField,
      conditions: pd.conditions.map((cond) => ({
        id: cond.id,
        schema: cond.schema,
        table: cond.table,
        field: cond.field,
        operator: cond.operator,
        compare_mode: cond.compareMode,
        compare_value: cond.compareValue,
        compare_column: cond.compareColumn
      }))
    }));
    const paginationParametersPayload = paginationParametersForDraft(d).map((p) => ({
      id: p.id,
      alias: p.alias,
      response_path: p.responsePath,
      first_value: String(p.firstValue || ''),
      request_target: p.requestTarget,
      request_path: p.requestPath,
      apply_for_all_responses: true
    }));
    const dataDateParametersPayload = dataDateParametersForDraft(d).map((p) => ({
      id: p.id,
      alias: p.alias,
      base_preset: p.basePreset,
      custom_date: p.customDate,
      anchor_preset: p.anchorPreset,
      add_days: Number(p.addDays || 0),
      add_months: Number(p.addMonths || 0),
      format_preset: p.formatPreset,
      grouped: Boolean(p.grouped)
    }));
    const dataApiParametersPayload = dataApiParametersForDraft(d).map((p) => ({
      id: p.id,
      source_api_ref: p.sourceApiRef,
      source_api_id: Number(p.sourceApiStoreId || 0) || null,
      source_type: p.sourceType,
      source_key: p.sourceKey,
      alias: p.alias,
      grouped: Boolean(p.grouped)
    }));
    const paginationStopRulesPayload = paginationStopRulesForDraft(d)
      .filter((rule) => String(rule.responsePath || '').trim())
      .map((rule) => ({
        id: rule.id,
        response_path: rule.responsePath,
        operator: rule.operator,
        compare_value: rule.compareValue
      }));
    const legacyCursorParams = paginationParametersPayload.filter((p) => p.response_path || p.request_path);
    const cursor1 = legacyCursorParams[0];
    const cursor2 = legacyCursorParams[1];
    const paginationTargetLegacy =
      cursor1?.request_target === 'query' || cursor2?.request_target === 'query'
        ? 'query'
        : d.paginationTarget;
    const paginationStrategyLegacy =
      d.paginationEnabled && paginationParametersPayload.length
        ? 'cursor_fields'
        : d.paginationStrategy || 'cursor_fields';
    const dispatchModePayload: DispatchMode = sanitizedAliases.groupByAliases.length ? 'group_by' : 'single';
    const bindingRulesPayload = sanitizedAliases.bindingRules.map((rule) => ({
      id: rule.id,
      alias: rule.alias,
      target: rule.target,
      path: rule.path
    }));
    const dataModelPayload = {
      tables: d.dataTables.map((t) => ({
        id: t.id,
        schema: t.schema,
        table: t.table,
        alias: t.alias
      })),
      joins: d.dataJoins.map((j) => ({
        id: j.id,
        left_table_id: j.leftTableId,
        left_field: j.leftField,
        right_table_id: j.rightTableId,
        right_field: j.rightField,
        join_type: j.joinType
      })),
      fields: d.dataFields.map((f) => ({
        id: f.id,
        table_id: f.tableId,
        field: f.field,
        alias: f.alias,
        group_by: Boolean(f.grouped),
        date_mode: f.dateMode === 'process' ? 'process' : 'raw',
        date_anchor_preset: toDateAnchorPreset(String(f.dateAnchorPreset || 'raw')),
        date_add_days: Number(f.dateAddDays || 0),
        date_add_months: Number(f.dateAddMonths || 0),
        date_format_preset: toDateFormatPreset(String(f.dateFormatPreset || 'yyyy_mm_dd')),
        date_transform:
          f.dateMode === 'process'
            ? {
                enabled: true,
                anchor_preset: toDateAnchorPreset(String(f.dateAnchorPreset || 'raw')),
                add_days: Number(f.dateAddDays || 0),
                add_months: Number(f.dateAddMonths || 0),
                format_preset: toDateFormatPreset(String(f.dateFormatPreset || 'yyyy_mm_dd'))
              }
            : { enabled: false }
      })),
      filters: d.dataFilters.map((f) => ({
        id: f.id,
        table_id: f.tableId,
        field: f.field,
        operator: f.operator,
        compare_value: f.compareValue
      })),
      date_parameters: dataDateParametersPayload,
      api_parameters: dataApiParametersPayload
    };
    const executionPayload = {
      dispatch_mode: dispatchModePayload,
      execution_mode: d.executionMode || 'sync',
      sync_planner: d.syncPlanner || 'entity_to_stop',
      async_concurrency: Math.max(1, Math.min(20, Number(d.asyncConcurrency || 3))),
      execution_delay_ms: Math.max(0, Number(d.executionDelayMs || 0)),
      response_log: {
        enabled: Boolean(d.responseLogEnabled),
        mode: d.responseLogMode || 'standard',
        write_request_payload: Boolean(d.responseLogWriteRequestPayload),
        write_response_payload: Boolean(d.responseLogWriteResponsePayload),
        write_pagination_values: Boolean(d.responseLogWritePaginationValues),
        only_errors: Boolean(d.responseLogOnlyErrors),
        response_payload_limit: Math.max(0, Number(d.responseLogResponsePayloadLimit || 0)),
        schema: String(d.responseLogSchema || '').trim(),
        table: String(d.responseLogTable || '').trim()
      },
      group_by_aliases: sanitizedAliases.groupByAliases,
      body_items_path: d.bodyItemsPath,
      preview_request_limit: d.previewRequestLimit,
      data_model: dataModelPayload,
      binding_rules: bindingRulesPayload
    };
    const oauth2FlowPayload =
      d.authMode === 'oauth2_client_credentials'
        ? {
            mode: 'manual_subrequest',
            request: {
              method: d.oauth2RequestMethod || 'POST',
              url: String(d.oauth2RequestUrl || '').trim(),
              host: String(d.oauth2RequestHost || '').trim(),
              path: String(d.oauth2RequestPath || '').trim(),
              headers_json: tryObj(d.oauth2RequestHeadersJson),
              query_json: tryObj(d.oauth2RequestQueryJson),
              body_json: tryObj(d.oauth2RequestBodyJson)
            },
            response_mappings: (Array.isArray(d.oauth2ResponseMappings) ? d.oauth2ResponseMappings : [])
              .map((m) => ({
                id: String(m?.id || uid()),
                response_path: String(m?.responsePath || '').trim(),
                alias: String(m?.alias || '').trim()
              }))
              .filter((m) => m.response_path && m.alias)
          }
        : { mode: 'manual' };

    return {
      id: d.storeId || undefined,
      api_name: d.name,
      method: d.method,
      base_url: d.baseUrl,
      path: d.path,
      headers_json: parsed?.headersJson ?? tryObj(d.headersJson),
      query_json: parsed?.queryJson ?? tryObj(d.queryJson),
      body_json: parsed?.bodyJson ?? tryObj(d.bodyJson),
      pagination_json: {
        enabled: d.paginationEnabled,
        strategy: paginationStrategyLegacy,
        target: paginationTargetLegacy,
        data_path: d.paginationDataPath,
        page_param: d.paginationPageParam,
        start_page: d.paginationStartPage,
        limit_param: d.paginationLimitParam,
        limit_value: d.paginationLimitValue,
        cursor_req_path_1: cursor1?.request_path || d.paginationCursorReqPath1,
        cursor_req_path_2: cursor2?.request_path || d.paginationCursorReqPath2,
        cursor_res_path_1: cursor1?.response_path || d.paginationCursorResPath1,
        cursor_res_path_2: cursor2?.response_path || d.paginationCursorResPath2,
        next_url_path: d.paginationNextUrlPath,
        use_max_pages: Boolean(d.paginationUseMaxPages),
        max_pages: d.paginationMaxPages,
        use_delay: Boolean(d.paginationUseDelay),
        delay_ms: d.paginationDelayMs,
        stop_conditions: {
          on_missing_pagination_value: Boolean(d.paginationStopOnMissingValue),
          on_same_response: Boolean(d.paginationStopOnSameResponse),
          same_response_limit: Math.max(2, Number(d.paginationSameResponseLimit || 5)),
          on_http_error: Boolean(d.paginationStopOnHttpError),
          response_rules: paginationStopRulesPayload
        },
        custom_strategy: d.paginationCustomStrategy,
        parameters: paginationParametersPayload
      },
      execution_json: executionPayload,
      data_model_json: dataModelPayload,
      target_schema: firstTarget?.schema || '',
      target_table: firstTarget?.table || '',
      mapping_json: {
        exampleRequest: d.exampleRequest,
        response_targets: d.responseTargets,
        picked_paths: d.pickedPaths,
        pagination_parameters: paginationParametersPayload,
        oauth2_flow: oauth2FlowPayload,
        oauth2:
          d.authMode === 'oauth2_client_credentials'
            ? {
                mode: d.authMode,
                token_url: d.oauth2RequestUrl || d.oauth2TokenUrl,
                client_id: d.oauth2ClientId,
                client_secret: d.oauth2ClientSecret,
                grant_type: d.oauth2GrantType || 'client_credentials',
                token_field: d.oauth2TokenField || 'access_token',
                expires_field: d.oauth2ExpiresField || 'expires_in',
                token_type_field: d.oauth2TokenTypeField || 'token_type'
            }
            : { mode: 'manual' },
        auth_json: parsed?.authJson ?? tryObj(d.authJson),
        parameter_definitions: parameterDefinitionsPayload,
        execution: executionPayload,
      },
      auth_mode: d.authMode,
      auth_json: parsed?.authJson ?? tryObj(d.authJson),
      oauth2_token_url: d.oauth2RequestUrl || d.oauth2TokenUrl,
      oauth2_client_id: d.oauth2ClientId,
      oauth2_client_secret: d.oauth2ClientSecret,
      oauth2_grant_type: d.oauth2GrantType,
      oauth2_token_field: d.oauth2TokenField,
      oauth2_expires_field: d.oauth2ExpiresField,
      oauth2_token_type_field: d.oauth2TokenTypeField,
      parameter_definitions: parameterDefinitionsPayload,
      dispatch_mode: dispatchModePayload,
      execution_mode: d.executionMode || 'sync',
      sync_planner: d.syncPlanner || 'entity_to_stop',
      async_concurrency: Math.max(1, Math.min(20, Number(d.asyncConcurrency || 3))),
      execution_delay_ms: Math.max(0, Number(d.executionDelayMs || 0)),
      response_log_enabled: Boolean(d.responseLogEnabled),
      response_log_schema: String(d.responseLogSchema || '').trim(),
      response_log_table: String(d.responseLogTable || '').trim(),
      group_by_aliases: sanitizedAliases.groupByAliases,
      body_items_path: d.bodyItemsPath,
      preview_request_limit: d.previewRequestLimit,
      binding_rules: bindingRulesPayload,
      response_targets: d.responseTargets,
      picked_paths: d.pickedPaths,
      example_request: d.exampleRequest,
      pagination_enabled: d.paginationEnabled,
      pagination_strategy: paginationStrategyLegacy,
      pagination_target: paginationTargetLegacy,
      pagination_data_path: d.paginationDataPath,
      pagination_page_param: d.paginationPageParam,
      pagination_start_page: d.paginationStartPage,
      pagination_limit_param: d.paginationLimitParam,
      pagination_limit_value: d.paginationLimitValue,
      pagination_cursor_req_path_1: cursor1?.request_path || d.paginationCursorReqPath1,
      pagination_cursor_req_path_2: cursor2?.request_path || d.paginationCursorReqPath2,
      pagination_cursor_res_path_1: cursor1?.response_path || d.paginationCursorResPath1,
      pagination_cursor_res_path_2: cursor2?.response_path || d.paginationCursorResPath2,
      pagination_next_url_path: d.paginationNextUrlPath,
      pagination_use_max_pages: Boolean(d.paginationUseMaxPages),
      pagination_max_pages: d.paginationMaxPages,
      pagination_use_delay: Boolean(d.paginationUseDelay),
      pagination_delay_ms: d.paginationDelayMs,
      pagination_stop_on_missing_value: Boolean(d.paginationStopOnMissingValue),
      pagination_stop_on_same_response: Boolean(d.paginationStopOnSameResponse),
      pagination_same_response_limit: Math.max(2, Number(d.paginationSameResponseLimit || 5)),
      pagination_stop_on_http_error: Boolean(d.paginationStopOnHttpError),
      pagination_custom_strategy: d.paginationCustomStrategy,
      data_date_parameters: dataDateParametersPayload,
      data_api_parameters: dataApiParametersPayload,
      description: d.description,
      is_active: true
    };
  }

  function parseQualifiedTable(value: string): { schema: string; table: string } {
    const [schema, table] = String(value || '').split('.');
    return { schema: schema || '', table: table || '' };
  }

  function formatQualifiedTable(schema: string, table: string): string {
    return schema && table ? `${schema}.${table}` : '';
  }

  function responseLogQualifiedTable(draft: ApiDraft | null): string {
    if (!draft) return '';
    return formatQualifiedTable(String(draft.responseLogSchema || '').trim(), String(draft.responseLogTable || '').trim());
  }

  function responseLogTableKey(schema: string, table: string) {
    return `${String(schema || '').trim()}.${String(table || '').trim()}`;
  }

  async function checkResponseLogTableCompatibility(schema: string, table: string) {
    const s = String(schema || '').trim();
    const t = String(table || '').trim();
    if (!s || !t) return { ok: false, reason: 'Не выбрана таблица лога' };
    const key = responseLogTableKey(s, t);
    if (responseLogTableCompatibility[key]) return responseLogTableCompatibility[key];
    try {
      const res = await apiJson<{ columns: Array<{ name: string; type: string }> }>(
        `${apiBase}/columns?schema=${encodeURIComponent(s)}&table=${encodeURIComponent(t)}`,
        { headers: headers() }
      );
      const cols = Array.isArray(res?.columns) ? res.columns : [];
      if (!cols.length) {
        const miss = { ok: false, reason: 'Таблица не найдена или не содержит колонок' };
        responseLogTableCompatibility = { ...responseLogTableCompatibility, [key]: miss };
        return miss;
      }
      const map = new Map(cols.map((c) => [String(c?.name || '').toLowerCase(), normalizeTypeName(String(c?.type || ''))]));
      for (const need of API_LOG_TEMPLATE_REQUIRED_COLUMNS) {
        const actual = map.get(need.name);
        if (!actual || !need.types.some((type) => actual.includes(normalizeTypeName(type)))) {
          const bad = { ok: false, reason: `Нет обязательного поля: ${need.name}` };
          responseLogTableCompatibility = { ...responseLogTableCompatibility, [key]: bad };
          return bad;
        }
      }
      const good = { ok: true, reason: '' };
      responseLogTableCompatibility = { ...responseLogTableCompatibility, [key]: good };
      return good;
    } catch (e: any) {
      const fail = { ok: false, reason: String(e?.message || 'Не удалось проверить таблицу') };
      responseLogTableCompatibility = { ...responseLogTableCompatibility, [key]: fail };
      return fail;
    }
  }

  async function refreshResponseLogTableCompatibility() {
    if (responseLogTablesRefreshing) return;
    responseLogTablesRefreshing = true;
    try {
      const list = Array.isArray(existingTables) ? existingTables : [];
      for (const item of list) {
        const schema = String(item?.schema_name || '').trim();
        const table = String(item?.table_name || '').trim();
        if (!schema || !table) continue;
        await checkResponseLogTableCompatibility(schema, table);
      }
    } finally {
      responseLogTablesRefreshing = false;
    }
  }

  function responseLogCompatibleTables() {
    return (Array.isArray(existingTables) ? existingTables : []).filter((item) => {
      const schema = String(item?.schema_name || '').trim();
      const table = String(item?.table_name || '').trim();
      const key = responseLogTableKey(schema, table);
      return Boolean(responseLogTableCompatibility[key]?.ok);
    });
  }

  function firstCompatibleResponseLogTable() {
    return responseLogCompatibleTables()[0] || null;
  }

  function toggleResponseLogPicker() {
    response_log_picker_open = !response_log_picker_open;
    response_log_pick_value = responseLogQualifiedTable(selected);
  }

  function toggleResponseLogEnabled() {
    mutateSelected((d) => {
      d.responseLogEnabled = !Boolean(d.responseLogEnabled);
      if (d.responseLogEnabled) {
        const currentSchema = String(d.responseLogSchema || '').trim();
        const currentTable = String(d.responseLogTable || '').trim();
        if (!currentSchema || !currentTable) {
          const fallback = firstCompatibleResponseLogTable();
          if (fallback) {
            d.responseLogSchema = String(fallback.schema_name || '').trim();
            d.responseLogTable = String(fallback.table_name || '').trim();
          }
        }
      }
    });
  }

  function setResponseLogMode(mode: string) {
    mutateSelected((d) => {
      const normalized: ResponseLogMode = mode === 'minimal' || mode === 'debug' ? (mode as ResponseLogMode) : 'standard';
      d.responseLogMode = normalized;
      if (normalized === 'minimal') {
        d.responseLogWriteRequestPayload = false;
        d.responseLogWriteResponsePayload = false;
        d.responseLogWritePaginationValues = false;
      } else if (normalized === 'debug') {
        d.responseLogWriteRequestPayload = true;
        d.responseLogWriteResponsePayload = true;
        d.responseLogWritePaginationValues = true;
      }
    });
  }

  function toggleResponseLogWriteRequestPayload() {
    mutateSelected((d) => {
      d.responseLogWriteRequestPayload = !Boolean(d.responseLogWriteRequestPayload);
    });
  }

  function toggleResponseLogWriteResponsePayload() {
    mutateSelected((d) => {
      d.responseLogWriteResponsePayload = !Boolean(d.responseLogWriteResponsePayload);
    });
  }

  function toggleResponseLogWritePaginationValues() {
    mutateSelected((d) => {
      d.responseLogWritePaginationValues = !Boolean(d.responseLogWritePaginationValues);
    });
  }

  function toggleResponseLogOnlyErrors() {
    mutateSelected((d) => {
      d.responseLogOnlyErrors = !Boolean(d.responseLogOnlyErrors);
    });
  }

  function togglePaginationEnabled() {
    mutateSelected((d) => {
      d.paginationEnabled = !Boolean(d.paginationEnabled);
    });
  }

  function togglePaginationUseMaxPages() {
    mutateSelected((d) => {
      d.paginationUseMaxPages = !Boolean(d.paginationUseMaxPages);
    });
  }

  function togglePaginationUseDelay() {
    mutateSelected((d) => {
      d.paginationUseDelay = !Boolean(d.paginationUseDelay);
    });
  }

  function togglePaginationStopOnMissingValue() {
    mutateSelected((d) => {
      d.paginationStopOnMissingValue = !Boolean(d.paginationStopOnMissingValue);
    });
  }

  function togglePaginationStopOnHttpError() {
    mutateSelected((d) => {
      d.paginationStopOnHttpError = !Boolean(d.paginationStopOnHttpError);
    });
  }

  function togglePaginationStopOnSameResponse() {
    mutateSelected((d) => {
      d.paginationStopOnSameResponse = !Boolean(d.paginationStopOnSameResponse);
    });
  }

  function isConnectedTable(schema: string, table: string) {
    const s = String(schema || '').trim();
    const t = String(table || '').trim();
    if (!s || !t) return false;
    return existingTables.some(
      (item) => String(item?.schema_name || '').trim() === s && String(item?.table_name || '').trim() === t
    );
  }

  function applyResponseLogChoice() {
    err = '';
    const [schema, table] = String(response_log_pick_value || '').split('.');
    const schemaTrimmed = String(schema || '').trim();
    const tableTrimmed = String(table || '').trim();
    if (!schemaTrimmed || !tableTrimmed) {
      err = 'Выбери таблицу лога из списка подключённых.';
      return;
    }
    const hasTarget = isConnectedTable(schemaTrimmed, tableTrimmed);
    if (!hasTarget) {
      err = 'Таблица лога должна быть из списка подключённых таблиц.';
      return;
    }
    checkResponseLogTableCompatibility(schemaTrimmed, tableTrimmed).then((compat) => {
      if (!compat.ok) {
        err = `Эта таблица не подходит для лога API: ${compat.reason}`;
        return;
      }
      mutateSelected((d) => {
        d.responseLogSchema = schemaTrimmed;
        d.responseLogTable = tableTrimmed;
      });
      response_log_picker_open = false;
    });
  }

  function tableCacheKey(schema: string, table: string) {
    return `${schema}.${table}`;
  }

  function mappingRowsOf(d: ApiDraft | null) {
    if (!d) return [] as Array<{ targetId: string; fieldId: string; schema: string; table: string; responsePath: string; targetField: string }>;
    const rows: Array<{ targetId: string; fieldId: string; schema: string; table: string; responsePath: string; targetField: string }> = [];
    for (const t of d.responseTargets) {
      for (const f of t.fields) {
        rows.push({
          targetId: t.id,
          fieldId: f.id,
          schema: t.schema,
          table: t.table,
          responsePath: f.responsePath,
          targetField: f.targetField
        });
      }
    }
    return rows;
  }

  async function ensureColumnsFor(schema: string, table: string) {
    if (!schema || !table) return;
    const key = tableCacheKey(schema, table);
    if (Array.isArray(columnsCache[key])) return;
    try {
      const j = await apiJson<{ columns: Array<{ name: string }> }>(
        `${apiBase}/columns?schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`,
        { headers: headers() }
      );
      const cols = Array.isArray(j?.columns)
        ? j.columns
            .map((c) => ({
              name: String(c?.name || '').trim(),
              type: String(c?.type || '').trim().toLowerCase() || undefined
            }))
            .filter((c) => c.name)
        : [];
      columnsCache = { ...columnsCache, [key]: cols };
    } catch {
      columnsCache = { ...columnsCache, [key]: [] };
    }
  }

  function columnOptionsFor(schema: string, table: string) {
    return (columnsCache[tableCacheKey(schema, table)] || []).map((c) => c.name);
  }

  function columnMeta(schema: string, table: string, column: string) {
    const meta = columnsCache[tableCacheKey(schema, table)];
    return meta?.find((c) => c.name === column);
  }

  function responsePathOptionsFor(current: string) {
    const base = (selected?.pickedPaths || []).filter(Boolean);
    const all = current && !base.includes(current) ? [current, ...base] : base;
    return [...new Set(all)];
  }

  function tableFieldOptionsFor(schema: string, table: string, current: string) {
    const cols = columnOptionsFor(schema, table).filter(Boolean);
    const all = current && !cols.includes(current) ? [current, ...cols] : cols;
    return [...new Set(all)];
  }

  const CONDITION_COMPARE_MODES = [
    { value: 'value', label: 'Значение' },
    { value: 'column', label: 'Колонка' }
  ];

  function getDefaultCondition(parameter?: ParameterDefinition): ParameterCondition {
    const schema = parameter?.sourceSchema || existingTables[0]?.schema_name || '';
    const table = parameter?.sourceTable || existingTables[0]?.table_name || '';
    return {
      id: uid(),
      schema,
      table,
      field: '',
      operator: 'equals',
      compareMode: 'value',
      compareValue: '',
      compareColumn: undefined
    };
  }

  function addParameterDefinition() {
    if (!selected) return;
    const source = existingTables[0];
    const newParam: ParameterDefinition = {
      id: uid(),
      alias: `param_${selected.parameterDefinitions.length + 1}`,
      definition: '',
      sourceSchema: source?.schema_name || '',
      sourceTable: source?.table_name || '',
      sourceField: '',
      conditions: [getDefaultCondition()]
    };
    mutateSelected((d) => {
      d.parameterDefinitions = [...d.parameterDefinitions, newParam];
    });
    selectedParameterId = newParam.id;
    ensureColumnsFor(newParam.sourceSchema, newParam.sourceTable);
  }

  function updateParameterDefinition(id: string, updates: Partial<ParameterDefinition>) {
    if (!selected) return;
    mutateSelected((d) => {
      d.parameterDefinitions = d.parameterDefinitions.map((param) =>
        param.id === id ? { ...param, ...updates } : param
      );
    });
  }

  function removeParameterDefinition(id: string) {
    if (!selected) return;
    mutateSelected((d) => {
      d.parameterDefinitions = d.parameterDefinitions.filter((param) => param.id !== id);
    });
    if (selectedParameterId === id) selectedParameterId = null;
  }

  function addCondition(parameterId: string) {
    if (!selected) return;
    const param = selected.parameterDefinitions.find((p) => p.id === parameterId);
    const newCondition = getDefaultCondition(param);
    ensureColumnsFor(newCondition.schema, newCondition.table);
    mutateSelected((d) => {
      d.parameterDefinitions = d.parameterDefinitions.map((param) => {
        if (param.id !== parameterId) return param;
        return { ...param, conditions: [...param.conditions, newCondition] };
      });
    });
  }

  function updateCondition(parameterId: string, conditionId: string, updates: Partial<ParameterCondition>) {
    if (!selected) return;
    mutateSelected((d) => {
      d.parameterDefinitions = d.parameterDefinitions.map((param) => {
        if (param.id !== parameterId) return param;
        return {
          ...param,
          conditions: param.conditions.map((cond) => (cond.id === conditionId ? { ...cond, ...updates } : cond))
        };
      });
    });
  }

  function removeCondition(parameterId: string, conditionId: string) {
    if (!selected) return;
    mutateSelected((d) => {
      d.parameterDefinitions = d.parameterDefinitions.map((param) => {
        if (param.id !== parameterId) return param;
        return { ...param, conditions: param.conditions.filter((cond) => cond.id !== conditionId) };
      });
    });
  }

  function getOperatorsForColumn(column: string, schema: string, table: string) {
    const meta = columnMeta(schema, table, column);
    const type = meta?.type;
    if (type?.includes('int') || type?.includes('numeric') || type?.includes('double') || type?.includes('decimal')) return CONDITION_OPERATORS.number;
    if (type?.includes('date') || type?.includes('timestamp')) return CONDITION_OPERATORS.date;
    if (type === 'boolean') return CONDITION_OPERATORS.boolean;
    return CONDITION_OPERATORS.text;
  }

  function parameterConditionSummary(cond: ParameterCondition) {
    const target = cond.compareMode === 'column' ? cond.compareColumn : cond.compareValue;
    return `${cond.schema}.${cond.table}.${cond.field} ${cond.operator} ${target || '?'}`;
  }

  let activeParameter: ParameterDefinition | null = null;
  $: activeParameter =
    selected && selectedParameterId
      ? selected.parameterDefinitions.find((param) => param.id === selectedParameterId) ?? null
      : null;
  $: groupByAliasCandidates = selected
    ? uniqueAliasList([
        ...bindingAliasOptions(selected),
        ...(Array.isArray(selected.paginationParameters)
          ? selected.paginationParameters.map((p) => String(p?.alias || '').trim()).filter(Boolean)
          : [])
      ])
    : [];
  $: if (selected) {
    const groupedTable = (selected.dataFields || [])
      .filter((f) => Boolean(f?.grouped))
      .map((f) => String(f?.alias || '').trim())
      .filter(Boolean);
    const groupedDates = dataDateParametersForDraft(selected)
      .filter((p) => Boolean(p?.grouped))
      .map((p) => String(p?.alias || '').trim())
      .filter(Boolean);
    const groupedApi = dataApiParametersForDraft(selected)
      .filter((p) => Boolean(p?.grouped))
      .map((p) => String(p?.alias || '').trim())
      .filter(Boolean);
    const groupedFromFields = uniqueAliasList([...groupedTable, ...groupedDates, ...groupedApi]);
    const groupedStored = uniqueAliasList((selected.groupByAliases || []).map((x) => String(x || '').trim()).filter(Boolean));
    if (JSON.stringify(groupedFromFields) !== JSON.stringify(groupedStored)) {
      mutateSelected((d) => {
        d.groupByAliases = groupedFromFields;
      });
    }
  }
  $: if (selected?.dataFilters?.length) {
    let changed = false;
    const normalized = selected.dataFilters.map((filter) => {
      const ops = dataFilterOperatorsFor(filter);
      if (!ops.length) return filter;
      if (ops.some((op) => op.value === filter.operator)) return filter;
      changed = true;
      return { ...filter, operator: ops[0].value };
    });
    if (changed) {
      mutateSelected((d) => {
        d.dataFilters = normalized;
      });
    }
  }
  $: if (selected?.dataTables?.length) {
    if (!selected.dataTables.some((t) => t.id === activeDataTableId)) {
      activeDataTableId = selected.dataTables[0].id;
    }
  } else {
    activeDataTableId = '';
  }
  $: if (selected?.dataJoins?.length) {
    if (!selected.dataJoins.some((j) => j.id === activeDataJoinId)) {
      activeDataJoinId = selected.dataJoins[0].id;
    }
  } else {
    activeDataJoinId = '';
  }
  $: if (selected?.dataFilters?.length) {
    if (!selected.dataFilters.some((f) => f.id === activeDataFilterId)) {
      activeDataFilterId = selected.dataFilters[0].id;
    }
  } else {
    activeDataFilterId = '';
  }
  $: if (selected?.paginationParameters?.length) {
    if (!selected.paginationParameters.some((param) => param.id === activePaginationParameterId)) {
      activePaginationParameterId = selected.paginationParameters[0].id;
    }
  } else {
    activePaginationParameterId = '';
  }
  $: activePaginationParameter =
    selected && activePaginationParameterId
      ? selected.paginationParameters.find((param) => param.id === activePaginationParameterId) ?? null
      : null;
  $: if (selected?.authMode === AUTH_MODE_OAUTH2) {
    const mappings = Array.isArray(selected.oauth2ResponseMappings) ? selected.oauth2ResponseMappings : [];
    if (!mappings.length) {
      mutateSelected((d) => {
        d.oauth2ResponseMappings = [{ id: uid(), responsePath: 'access_token', alias: 'access_token' }];
      });
    }
  }

  $: if (selected && selectedParameterId && !selected.parameterDefinitions.some((param) => param.id === selectedParameterId)) {
    selectedParameterId = null;
  }

  let lastParameterId: string | null = null;
  $: if (activeParameter && activeParameter.id !== lastParameterId) {
    lastParameterId = activeParameter.id;
    definitionDirty = false;
  } else if (!activeParameter) {
    lastParameterId = null;
  }

  $: if (activeParameter && !definitionDirty && !definitionUpdatingFromFields) {
    definitionUpdatingFromFields = true;
    definitionDraft = serializeParameterDefinition(activeParameter);
    definitionUpdatingFromFields = false;
  }

  $: if (activeParameter) {
    ensureColumnsFor(activeParameter.sourceSchema, activeParameter.sourceTable);
  }

  function addMappingRow() {
    mutateSelected((d) => {
      const firstTable = existingTables[0];
      d.responseTargets = [
        ...d.responseTargets,
        {
          id: uid(),
          schema: firstTable?.schema_name || '',
          table: firstTable?.table_name || '',
          fields: [{ id: uid(), responsePath: '', targetField: '' }]
        }
      ];
    });
  }

  function removeMappingRow(targetId: string, fieldId: string) {
    mutateSelected((d) => {
      d.responseTargets = d.responseTargets
        .map((t) => (t.id === targetId ? { ...t, fields: t.fields.filter((f) => f.id !== fieldId) } : t))
        .filter((t) => t.fields.length > 0);
    });
  }

  async function setMappingRowTable(targetId: string, value: string) {
    const parsed = parseQualifiedTable(value);
    mutateSelected((d) => {
      d.responseTargets = d.responseTargets.map((t) =>
        t.id === targetId ? { ...t, schema: parsed.schema, table: parsed.table } : t
      );
    });
    await ensureColumnsFor(parsed.schema, parsed.table);
  }

  function setMappingRowResponsePath(targetId: string, fieldId: string, value: string) {
    setTargetFieldValue(targetId, fieldId, 'responsePath', value);
  }

  function setMappingRowColumn(targetId: string, fieldId: string, value: string) {
    setTargetFieldValue(targetId, fieldId, 'targetField', value);
  }

  function removePickedPath(path: string) {
    mutateSelected((d) => {
      d.pickedPaths = d.pickedPaths.filter((x) => x !== path);
    });
  }

  function addOAuth2ResponseMapping() {
    mutateSelected((d) => {
      d.oauth2ResponseMappings = [
        ...(Array.isArray(d.oauth2ResponseMappings) ? d.oauth2ResponseMappings : []),
        { id: uid(), responsePath: '', alias: '' }
      ];
    });
  }

  function removeOAuth2ResponseMapping(id: string) {
    mutateSelected((d) => {
      d.oauth2ResponseMappings = (Array.isArray(d.oauth2ResponseMappings) ? d.oauth2ResponseMappings : []).filter((m) => m.id !== id);
      if (!d.oauth2ResponseMappings.length) {
        d.oauth2ResponseMappings = [{ id: uid(), responsePath: 'access_token', alias: 'access_token' }];
      }
    });
  }

  function updateOAuth2ResponseMapping(id: string, patch: Partial<OAuth2ResponseMapping>) {
    mutateSelected((d) => {
      d.oauth2ResponseMappings = (Array.isArray(d.oauth2ResponseMappings) ? d.oauth2ResponseMappings : []).map((m) =>
        m.id === id ? { ...m, ...patch } : m
      );
    });
  }

  function collectResponsePaths(node: any, base: string, out: string[]) {
    if (out.length >= 5000) return;
    if (node && typeof node === 'object') {
      if (base) out.push(base);
      if (Array.isArray(node)) {
        node.forEach((v, i) => {
          const next = base ? `${base}[${i}]` : `[${i}]`;
          collectResponsePaths(v, next, out);
        });
        return;
      }
      Object.entries(node).forEach(([k, v]) => {
        const next = base ? `${base}.${k}` : k;
        collectResponsePaths(v, next, out);
      });
      return;
    }
    if (base) out.push(base);
  }

  function isScalarValue(value: any) {
    const t = typeof value;
    return value === null || t === 'string' || t === 'number' || t === 'boolean';
  }

  function collectScalarResponsePaths(node: any, base: string, out: Set<string>) {
    if (isScalarValue(node)) {
      if (base) out.add(base);
      return;
    }
    if (Array.isArray(node)) {
      node.slice(0, 3).forEach((item, idx) => {
        const next = base ? `${base}[${idx}]` : `[${idx}]`;
        collectScalarResponsePaths(item, next, out);
      });
      return;
    }
    if (node && typeof node === 'object') {
      Object.entries(node).forEach(([k, v]) => {
        const next = base ? `${base}.${k}` : k;
        collectScalarResponsePaths(v, next, out);
      });
    }
  }

  function setActivePaginationParameter(id: string) {
    activePaginationParameterId = id;
  }

  function addPaginationParameter() {
    if (!selected) return;
    const next = normalizePaginationParameter(
      {
        id: uid(),
        alias: `cursor_${(selected.paginationParameters?.length || 0) + 1}`,
        firstValue: '',
        requestTarget: 'body',
        requestPath: '',
        responsePath: '',
        applyForAllResponses: true
      },
      `cursor_${(selected.paginationParameters?.length || 0) + 1}`
    );
    mutateSelected((d) => {
      d.paginationEnabled = true;
      d.paginationStrategy = 'cursor_fields';
      d.paginationTarget = 'body';
      d.paginationParameters = [...(Array.isArray(d.paginationParameters) ? d.paginationParameters : []), next];
    });
    activePaginationParameterId = next.id;
  }

  function removePaginationParameter(id: string) {
    if (!selected) return;
    mutateSelected((d) => {
      d.paginationParameters = (Array.isArray(d.paginationParameters) ? d.paginationParameters : []).filter((p) => p.id !== id);
    });
  }

  function updatePaginationParameter(id: string, updates: Partial<PaginationParameter>) {
    if (!selected) return;
    mutateSelected((d) => {
      d.paginationParameters = (Array.isArray(d.paginationParameters) ? d.paginationParameters : []).map((p) =>
        p.id === id ? normalizePaginationParameter({ ...p, ...updates }, p.alias || 'cursor') : p
      );
    });
  }

  function setPaginationResponsePick(value: string) {
    const path = String(value || '').trim();
    paginationCursorResponsePick = path;
    if (!path || !activePaginationParameterId) return;
    updatePaginationParameter(activePaginationParameterId, { responsePath: path });
  }

  function paginationStopRulePathOptionsFor(current: string) {
    const fromPreview = Array.isArray(paginationCursorResponsePathOptions) ? paginationCursorResponsePathOptions : [];
    const fromAllResponsePaths = Array.isArray(responsePathOptions) ? responsePathOptions : [];
    const fromPicked = Array.isArray(selected?.pickedPaths) ? selected?.pickedPaths : [];
    const base = uniqueAliasList([...fromPreview, ...fromAllResponsePaths, ...fromPicked].map((path) => String(path || '').trim()).filter(Boolean));
    if (current && !base.includes(current)) return [current, ...base];
    return base;
  }

  function addPaginationStopRule() {
    if (!selected) return;
    const defaultPath = paginationStopRulePathOptionsFor('')[0] || '';
    const next = normalizePaginationStopRule(
      {
        id: uid(),
        responsePath: defaultPath,
        operator: 'equals',
        compareValue: ''
      }
    );
    mutateSelected((d) => {
      d.paginationEnabled = true;
      d.paginationStopRules = [...(Array.isArray(d.paginationStopRules) ? d.paginationStopRules : []), next];
    });
  }

  function updatePaginationStopRule(id: string, updates: Partial<PaginationStopRule>) {
    if (!selected) return;
    mutateSelected((d) => {
      d.paginationStopRules = (Array.isArray(d.paginationStopRules) ? d.paginationStopRules : []).map((rule) =>
        rule.id === id ? normalizePaginationStopRule({ ...rule, ...updates }) : rule
      );
    });
  }

  function removePaginationStopRule(id: string) {
    if (!selected) return;
    mutateSelected((d) => {
      d.paginationStopRules = (Array.isArray(d.paginationStopRules) ? d.paginationStopRules : []).filter((rule) => rule.id !== id);
    });
  }

  function addPickedPathFromPicker() {
    const path = String(responsePathPick || '').trim();
    if (!path) return;
    mutateSelected((d) => {
      if (!d.pickedPaths.includes(path)) d.pickedPaths = [...d.pickedPaths, path];
    });
    responsePathPickerOpen = false;
    ok = 'Путь добавлен в витрину';
  }

  function setActiveParameter(id: string) {
    selectedParameterId = selectedParameterId === id ? null : id;
  }

  function parameterCardDragStart(param: ParameterDefinition, event: DragEvent) {
    if (!event.dataTransfer) return;
    const payload = `{{${param.alias}}}`;
    event.dataTransfer.setData('text/plain', payload);
    event.dataTransfer.effectAllowed = 'copy';
  }

  function parseCompareModeValue(value: string): 'value' | 'column' {
    return value === 'column' ? 'column' : 'value';
  }

  function compareColumnOptions(cond: ParameterCondition) {
    const [schema, table] = compareColumnTableValue(cond).split('.');
    if (!schema || !table) return [];
    return columnOptionsFor(schema, table);
  }

  function splitQualified(value: string) {
    const parts = String(value || '').split('.');
    return {
      schema: parts[0] || '',
      table: parts[1] || '',
      field: parts[2] || ''
    };
  }

  function compareColumnTableValue(cond: ParameterCondition) {
    const parts = splitQualified(cond.compareColumn || '');
    return parts.schema && parts.table ? `${parts.schema}.${parts.table}` : '';
  }

  function compareColumnFieldValue(cond: ParameterCondition) {
    return splitQualified(cond.compareColumn || '').field;
  }

  function onPathChipDragStart(event: DragEvent, path: string) {
    if (!event.dataTransfer) return;
    event.dataTransfer.setData('text/plain', path);
    event.dataTransfer.effectAllowed = 'copy';
  }

  function dropPathToMapping(event: DragEvent, targetId: string, fieldId: string) {
    event.preventDefault();
    const path = String(event.dataTransfer?.getData('text/plain') || '').trim();
    if (!path) return;
    setActiveResponseField(targetId, fieldId);
    setMappingRowResponsePath(targetId, fieldId, path);
  }

  function addResponseTarget() {
    mutateSelected((d) => {
      const firstTable = existingTables[0];
      d.responseTargets = [
        ...d.responseTargets,
        {
          id: uid(),
          schema: firstTable?.schema_name || '',
          table: firstTable?.table_name || '',
          fields: [{ id: uid(), responsePath: '', targetField: '' }]
        }
      ];
    });
  }

  function removeResponseTarget(targetId: string) {
    mutateSelected((d) => {
      d.responseTargets = d.responseTargets.filter((t) => t.id !== targetId);
    });
  }

  function setResponseTargetTable(targetId: string, value: string) {
    const parsed = parseQualifiedTable(value);
    mutateSelected((d) => {
      d.responseTargets = d.responseTargets.map((t) =>
        t.id === targetId ? { ...t, schema: parsed.schema, table: parsed.table } : t
      );
    });
  }

  function addTargetField(targetId: string) {
    mutateSelected((d) => {
      d.responseTargets = d.responseTargets.map((t) =>
        t.id === targetId
          ? { ...t, fields: [...t.fields, { id: uid(), responsePath: '', targetField: '' }] }
          : t
      );
    });
  }

  function removeTargetField(targetId: string, fieldId: string) {
    mutateSelected((d) => {
      d.responseTargets = d.responseTargets.map((t) =>
        t.id === targetId ? { ...t, fields: t.fields.filter((f) => f.id !== fieldId) } : t
      );
    });
  }

  function setTargetFieldValue(targetId: string, fieldId: string, key: 'responsePath' | 'targetField', value: string) {
    mutateSelected((d) => {
      d.responseTargets = d.responseTargets.map((t) =>
        t.id === targetId
          ? {
              ...t,
              fields: t.fields.map((f) => (f.id === fieldId ? { ...f, [key]: value } : f))
            }
          : t
      );
    });
  }

  function setActiveResponseField(targetId: string, fieldId: string) {
    activeResponseFieldRef = `${targetId}:${fieldId}`;
  }

  function isActiveResponseField(targetId: string, fieldId: string) {
    return activeResponseFieldRef === `${targetId}:${fieldId}`;
  }

  function applyPickedResponsePath(rawPath: string) {
    const path = String(rawPath || '').trim();
    if (!path || !selected) return;
    mutateSelected((d) => {
      if (!d.pickedPaths.includes(path)) d.pickedPaths = [...d.pickedPaths, path];
    });

    const [activeTargetId, activeFieldId] = String(activeResponseFieldRef || '').split(':');
    if (activeTargetId && activeFieldId) {
      let found = false;
      mutateSelected((d) => {
        d.responseTargets = d.responseTargets.map((t) => {
          if (t.id !== activeTargetId) return t;
          return {
            ...t,
            fields: t.fields.map((f) => {
              if (f.id !== activeFieldId) return f;
              found = true;
              return { ...f, responsePath: path };
            })
          };
        });
      });
      if (found) {
        ok = 'Путь добавлен в активное поле ответа';
        return;
      }
    }

    const firstTarget = selected.responseTargets[0];
    if (firstTarget) {
      const firstField = firstTarget.fields[0];
      if (firstField) {
        mutateSelected((d) => {
          d.responseTargets = d.responseTargets.map((t) =>
            t.id === firstTarget.id
              ? {
                  ...t,
                  fields: t.fields.map((f) => (f.id === firstField.id ? { ...f, responsePath: path } : f))
                }
              : t
          );
        });
        setActiveResponseField(firstTarget.id, firstField.id);
        ok = 'Путь добавлен в первое поле ответа';
        return;
      }

      const newFieldId = uid();
      mutateSelected((d) => {
        d.responseTargets = d.responseTargets.map((t) =>
          t.id === firstTarget.id
            ? { ...t, fields: [...t.fields, { id: newFieldId, responsePath: path, targetField: '' }] }
            : t
        );
      });
      setActiveResponseField(firstTarget.id, newFieldId);
      ok = 'Путь добавлен в новое поле ответа';
      return;
    }

    const targetId = uid();
    const fieldId = uid();
    const firstTable = existingTables[0];
    mutateSelected((d) => {
      d.responseTargets = [
        ...d.responseTargets,
        {
          id: targetId,
          schema: firstTable?.schema_name || '',
          table: firstTable?.table_name || '',
          fields: [{ id: fieldId, responsePath: path, targetField: '' }]
        }
      ];
    });
    setActiveResponseField(targetId, fieldId);
    ok = 'Путь добавлен, создана новая строка маппинга';
  }

  function fullUrl(d: ApiDraft) {
    const base = String(d.baseUrl || '').trim();
    const path = String(d.path || '').trim() || '/';
    const p = path.startsWith('/') ? path : `/${path}`;
    const u = new URL(`${base.replace(/\/$/, '')}${p}`);
    const q = parseJsonObjectField('Query JSON', d.queryJson);
    for (const [k, v] of Object.entries(q || {})) u.searchParams.set(k, String(v));
    return u.toString();
  }

  function parsePathParts(path: string): Array<string | number> {
    const raw = String(path || '').trim();
    if (!raw) return [];
    const out: Array<string | number> = [];
    const dotParts = raw.split('.');
    for (const part of dotParts) {
      if (!part) continue;
      const re = /([^[\]]+)|\[(\d+)\]/g;
      let m: RegExpExecArray | null;
      while ((m = re.exec(part))) {
        if (m[1]) out.push(m[1]);
        else if (m[2]) out.push(Number(m[2]));
      }
    }
    return out;
  }

  function getByPath(obj: any, path: string): any {
    const parts = parsePathParts(path);
    let cur = obj;
    for (const p of parts) {
      if (cur == null) return undefined;
      cur = cur[p as any];
    }
    return cur;
  }

  function setByPath(obj: any, path: string, value: any) {
    const parts = parsePathParts(path);
    if (!parts.length) return;
    let cur = obj;
    for (let i = 0; i < parts.length - 1; i++) {
      const p = parts[i];
      const next = parts[i + 1];
      if (cur[p as any] == null) {
        cur[p as any] = typeof next === 'number' ? [] : {};
      }
      cur = cur[p as any];
    }
    cur[parts[parts.length - 1] as any] = value;
  }

  function stripGroupedResponsePrefix(path: string) {
    const raw = String(path || '').trim();
    if (!raw) return '';
    return raw
      .replace(/^responses\[\d+\]\.response\.?/, '')
      .replace(/^responses\[\d+\]\.?/, '')
      .replace(/^response\./, '');
  }

  function readPaginationValueFromResponse(payload: any, param: PaginationParameter) {
    const rawPath = String(param?.responsePath || '').trim();
    if (!rawPath) return undefined;
    const direct = getByPath(payload, rawPath);
    if (direct !== undefined && direct !== null) return direct;

    const strippedPath = stripGroupedResponsePrefix(rawPath);
    if (strippedPath && strippedPath !== rawPath) {
      const strippedValue = getByPath(payload, strippedPath);
      if (strippedValue !== undefined && strippedValue !== null) return strippedValue;
    }

    const responses = Array.isArray(payload?.responses) ? payload.responses : [];
    if (!responses.length) return direct;
    const candidatePaths = uniqueAliasList(
      [strippedPath, rawPath]
        .map((path) => String(path || '').trim())
        .filter(Boolean)
        .map((path) => stripGroupedResponsePrefix(path))
        .filter(Boolean)
    );
    for (const entry of responses) {
      const source = entry && typeof entry === 'object' && 'response' in entry ? entry.response : entry;
      for (const path of candidatePaths) {
        const value = path ? getByPath(source, path) : source;
        if (value !== undefined && value !== null) return value;
      }
    }
    return direct;
  }

  function applyPaginationValueToRequest(
    param: PaginationParameter,
    value: any,
    queryObj: Record<string, any>,
    bodyObj: any,
    headersObj: Record<string, any>
  ) {
    const path = String(param?.requestPath || '').trim();
    if (!path || value === undefined || value === null) return;
    const target = toPaginationParameterTarget(String(param?.requestTarget || 'body'));
    if (target === 'query') {
      setByPath(queryObj, path, value);
      return;
    }
    if (target === 'body') {
      if (bodyObj && typeof bodyObj === 'object') {
        setByPath(bodyObj, path, value);
      }
      return;
    }
    headersObj[path] = value;
  }

  function readPaginationStopRuleValueFromResponse(payload: any, rule: PaginationStopRule) {
    const rawPath = String(rule?.responsePath || '').trim();
    if (!rawPath) return undefined;
    const direct = getByPath(payload, rawPath);
    if (direct !== undefined) return direct;

    const strippedPath = stripGroupedResponsePrefix(rawPath);
    if (strippedPath && strippedPath !== rawPath) {
      const strippedValue = getByPath(payload, strippedPath);
      if (strippedValue !== undefined) return strippedValue;
    }

    const responses = Array.isArray(payload?.responses) ? payload.responses : [];
    if (!responses.length) return direct;
    const candidatePaths = uniqueAliasList(
      [strippedPath, rawPath]
        .map((path) => String(path || '').trim())
        .filter(Boolean)
        .map((path) => stripGroupedResponsePrefix(path))
        .filter(Boolean)
    );
    for (const entry of responses) {
      const source = entry && typeof entry === 'object' && 'response' in entry ? entry.response : entry;
      for (const path of candidatePaths) {
        const value = path ? getByPath(source, path) : source;
        if (value !== undefined) return value;
      }
    }
    return direct;
  }

  function isPaginationStopValueEmpty(value: any) {
    if (value === undefined || value === null) return true;
    if (typeof value === 'string') return !value.trim();
    if (Array.isArray(value)) return value.length === 0;
    if (typeof value === 'object') return Object.keys(value).length === 0;
    return false;
  }

  function toNumberOrNull(value: any): number | null {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    const txt = String(value ?? '').trim();
    if (!txt) return null;
    const n = Number(txt);
    return Number.isFinite(n) ? n : null;
  }

  function parseStopRuleCompareValue(raw: string): any {
    const txt = String(raw || '').trim();
    if (!txt) return '';
    if (txt.toLowerCase() === 'null') return null;
    if (txt.toLowerCase() === 'true') return true;
    if (txt.toLowerCase() === 'false') return false;
    if (/^-?\d+(\.\d+)?$/.test(txt)) return Number(txt);
    return txt;
  }

  function stringValueForContains(value: any) {
    if (value === undefined || value === null) return '';
    if (typeof value === 'string') return value;
    if (typeof value === 'number' || typeof value === 'boolean') return String(value);
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }

  type PaginationStopRuleEvalContext = {
    values: Record<string, any>;
    queryObj: Record<string, any>;
    bodyObj: any;
    headersObj: Record<string, any>;
  };

  function resolveStopRuleTokenValue(tokenRaw: string, context: PaginationStopRuleEvalContext) {
    const token = String(tokenRaw || '').trim();
    if (!token) return undefined;
    const lower = token.toLowerCase();

    if (lower.startsWith('request.body.')) return getByPath(context.bodyObj, token.slice('request.body.'.length));
    if (lower.startsWith('body.')) return getByPath(context.bodyObj, token.slice('body.'.length));
    if (lower.startsWith('request.query.')) return getByPath(context.queryObj, token.slice('request.query.'.length));
    if (lower.startsWith('query.')) return getByPath(context.queryObj, token.slice('query.'.length));
    if (lower.startsWith('request.headers.')) return getByPath(context.headersObj, token.slice('request.headers.'.length));
    if (lower.startsWith('request.header.')) return getByPath(context.headersObj, token.slice('request.header.'.length));
    if (lower.startsWith('headers.')) return getByPath(context.headersObj, token.slice('headers.'.length));
    if (lower.startsWith('header.')) return getByPath(context.headersObj, token.slice('header.'.length));
    if (lower.startsWith('group.')) return getByPath(context.values, token.slice('group.'.length));

    const fromAlias = findAliasInMap(context.values, token);
    if (fromAlias.found) return fromAlias.value;
    const fromPath = getByPath(context.values, token);
    if (fromPath !== undefined) return fromPath;
    return undefined;
  }

  function resolveStopRuleCompareValue(rule: PaginationStopRule, context: PaginationStopRuleEvalContext): any {
    const raw = String(rule.compareValue || '');
    if (!raw.trim()) return '';

    const exact = raw.match(PARAMETER_TOKEN_EXACT_RE);
    if (exact?.[1]) {
      const resolved = resolveStopRuleTokenValue(String(exact[1] || ''), context);
      if (resolved !== undefined) return resolved;
    }

    const replaced = raw.replace(PARAMETER_TOKEN_RE, (match, aliasRaw) => {
      const resolved = resolveStopRuleTokenValue(String(aliasRaw || ''), context);
      if (resolved === undefined || resolved === null) return match;
      if (typeof resolved === 'string') return resolved;
      if (typeof resolved === 'object') return JSON.stringify(resolved);
      return String(resolved);
    });
    return parseStopRuleCompareValue(replaced);
  }

  function paginationStopOperatorLabel(op: PaginationStopOperator) {
    const found = PAGINATION_STOP_OPERATORS.find((item) => item.value === op);
    return found?.label || op;
  }

  function evaluatePaginationStopRule(
    rule: PaginationStopRule,
    actualValue: any,
    context: PaginationStopRuleEvalContext
  ) {
    const op = rule.operator;
    const expected = resolveStopRuleCompareValue(rule, context);

    if (op === 'is_empty') return isPaginationStopValueEmpty(actualValue);
    if (op === 'not_empty') return !isPaginationStopValueEmpty(actualValue);

    if (op === 'contains' || op === 'not_contains') {
      const needle = String(expected ?? '').trim().toLowerCase();
      let contains = false;
      if (needle) {
        if (Array.isArray(actualValue)) {
          contains = actualValue.some((item) => stringValueForContains(item).toLowerCase().includes(needle));
        } else {
          contains = stringValueForContains(actualValue).toLowerCase().includes(needle);
        }
      }
      return op === 'contains' ? contains : !contains;
    }

    if (op === 'gt' || op === 'gte' || op === 'lt' || op === 'lte') {
      const left = toNumberOrNull(actualValue);
      const right = toNumberOrNull(expected);
      if (left === null || right === null) return false;
      if (op === 'gt') return left > right;
      if (op === 'gte') return left >= right;
      if (op === 'lt') return left < right;
      return left <= right;
    }

    if (op === 'equals' || op === 'not_equals') {
      let equal = false;
      if (actualValue === null || actualValue === undefined) {
        equal = expected === null || expected === '';
      } else if (typeof actualValue === 'number' && typeof expected === 'number') {
        equal = actualValue === expected;
      } else if (typeof actualValue === 'boolean' && typeof expected === 'boolean') {
        equal = actualValue === expected;
      } else {
        equal = String(actualValue) === String(expected);
      }
      return op === 'equals' ? equal : !equal;
    }

    return false;
  }

  function formatPaginationStopRuleReason(rule: PaginationStopRule, actualValue: any, resolvedCompareValue: any) {
    const left = String(rule.responsePath || '?');
    const opLabel = paginationStopOperatorLabel(rule.operator);
    if (paginationStopOperatorNeedsValue(rule.operator)) {
      const originalRight = String(rule.compareValue || '');
      const resolvedRight = stringValueForContains(resolvedCompareValue);
      const rightPart =
        originalRight.includes('{{') && resolvedRight
          ? `${originalRight} => ${resolvedRight}`
          : originalRight || resolvedRight;
      return `Сработало условие: ${left} ${opLabel} ${rightPart} (текущее: ${stringValueForContains(actualValue)})`;
    }
    return `Сработало условие: ${left} ${opLabel}`;
  }

  function findMatchedPaginationStopRule(payload: any, rules: PaginationStopRule[], context: PaginationStopRuleEvalContext) {
    for (const rule of rules) {
      const path = String(rule?.responsePath || '').trim();
      if (!path) continue;
      const actualValue = readPaginationStopRuleValueFromResponse(payload, rule);
      const expectedValue = resolveStopRuleCompareValue(rule, context);
      if (evaluatePaginationStopRule(rule, actualValue, context)) {
        return { rule, actualValue, expectedValue };
      }
    }
    return null;
  }

  function selectedDraftSignature(draft: ApiDraft | null, nameOverride = '') {
    if (!draft) return '';
    return JSON.stringify({
      ...draft,
      name: String(nameOverride || draft.name || '').trim()
    });
  }

  function templatesStorageScopeKey() {
    return `${String(api_storage_schema || '').trim().toLowerCase()}.${String(api_storage_table || '').trim().toLowerCase()}`;
  }

  function templateFavoritesStorageKey() {
    return `ao_api_builder_favorites:${templatesStorageScopeKey()}`;
  }

  function templateRecentStorageKey() {
    return `ao_api_builder_recent:${templatesStorageScopeKey()}`;
  }

  function safeReadTemplateRefList(key: string, limit: number) {
    if (typeof window === 'undefined') return [];
    try {
      const raw = window.localStorage.getItem(key);
      const parsed = raw ? JSON.parse(raw) : [];
      if (!Array.isArray(parsed)) return [];
      const uniq: string[] = [];
      for (const item of parsed) {
        const v = String(item || '').trim();
        if (!v || uniq.includes(v)) continue;
        uniq.push(v);
      }
      return uniq.slice(0, limit);
    } catch {
      return [];
    }
  }

  function persistTemplateRefList(key: string, refs: string[], limit: number) {
    if (typeof window === 'undefined') return;
    const uniq: string[] = [];
    for (const item of refs) {
      const v = String(item || '').trim();
      if (!v || uniq.includes(v)) continue;
      uniq.push(v);
      if (uniq.length >= limit) break;
    }
    try {
      window.localStorage.setItem(key, JSON.stringify(uniq));
    } catch {
      // ignore storage quota/runtime errors
    }
  }

  function loadTemplateUiPrefs() {
    templateFavoriteRefs = safeReadTemplateRefList(templateFavoritesStorageKey(), TEMPLATE_FAVORITES_LIMIT);
    templateRecentRefs = safeReadTemplateRefList(templateRecentStorageKey(), TEMPLATE_RECENT_LIMIT);
  }

  function persistTemplateUiPrefs() {
    persistTemplateRefList(templateFavoritesStorageKey(), templateFavoriteRefs, TEMPLATE_FAVORITES_LIMIT);
    persistTemplateRefList(templateRecentStorageKey(), templateRecentRefs, TEMPLATE_RECENT_LIMIT);
  }

  function normalizeTemplateSectionText(text: any) {
    const src = String(text || '')
      .replace(/^\s*\d+\s*[·.\-–—]\s*/g, '')
      .replace(/\s+/g, ' ')
      .trim();
    return src;
  }

  function deriveTemplateSectionFromName(name: string) {
    const src = normalizeTemplateSectionText(name);
    if (!src) return 'Без раздела';
    const partsByDash = src.split(/\s+-\s+/).map((x) => x.trim()).filter(Boolean);
    if (partsByDash.length >= 2) return partsByDash[0];
    const partsByDot = src.split('·').map((x) => x.trim()).filter(Boolean);
    if (partsByDot.length >= 2) {
      const first = partsByDot[0];
      const second = partsByDot[1];
      if (/^(wb|wildberries|ozon|yandex|market)/i.test(first)) {
        return `${first} ${second}`.trim();
      }
      return first;
    }
    return src;
  }

  function sectionForDraft(draft: ApiDraft) {
    const fromCfg = normalizeTemplateSectionText((draft as any)?.sectionName || '');
    if (fromCfg) return fromCfg;
    return deriveTemplateSectionFromName(String(draft?.name || ''));
  }

  function templateCodeForDraft(draft: ApiDraft) {
    const own = String((draft as any)?.templateCode || '').trim();
    if (own) return own;
    const token = String(draft?.name || '').match(/\b[A-Z]{2,5}-\d{2}-\d{3}\b/i);
    return token ? String(token[0]).toUpperCase() : '';
  }

  function shortPathForDraft(draft: ApiDraft) {
    const base = String(draft?.baseUrl || '').trim();
    const path = String(draft?.path || '').trim() || '/';
    if (!base) return path;
    try {
      const u = new URL(base);
      return `${u.hostname}${path}`;
    } catch {
      const shortBase = base.replace(/^https?:\/\//i, '').replace(/\/+$/, '');
      return `${shortBase}${path.startsWith('/') ? path : `/${path}`}`;
    }
  }

  function isTemplateFavorite(ref: string) {
    return templateFavoriteRefs.includes(String(ref || '').trim());
  }

  function toggleTemplateFavorite(ref: string) {
    const key = String(ref || '').trim();
    if (!key) return;
    if (templateFavoriteRefs.includes(key)) {
      templateFavoriteRefs = templateFavoriteRefs.filter((x) => x !== key);
    } else {
      templateFavoriteRefs = [key, ...templateFavoriteRefs].slice(0, TEMPLATE_FAVORITES_LIMIT);
    }
    persistTemplateUiPrefs();
  }

  function markTemplateRecent(ref: string) {
    const key = String(ref || '').trim();
    if (!key) return;
    templateRecentRefs = [key, ...templateRecentRefs.filter((x) => x !== key)].slice(0, TEMPLATE_RECENT_LIMIT);
    persistTemplateUiPrefs();
  }

  function toggleTemplateSection(section: string) {
    const key = String(section || '').trim();
    if (!key) return;
    collapsedTemplateSections = {
      ...collapsedTemplateSections,
      [key]: !Boolean(collapsedTemplateSections[key])
    };
  }

  function isTemplateSectionCollapsed(section: string) {
    return Boolean(collapsedTemplateSections[String(section || '').trim()]);
  }

  function applyTemplateSelection(ref: string) {
    const key = String(ref || '').trim();
    if (!key) return;
    selectedRef = key;
    markTemplateRecent(key);
  }

  function cleanupTemplateUiRefs() {
    const valid = new Set(drafts.map((d) => refOf(d)));
    templateFavoriteRefs = templateFavoriteRefs.filter((x) => valid.has(x));
    templateRecentRefs = templateRecentRefs.filter((x) => valid.has(x));
    persistTemplateUiPrefs();
  }

  function orderTemplateItemsByRefList(items: TemplateListItem[], refList: string[]) {
    const orderMap = new Map(refList.map((ref, idx) => [ref, idx]));
    return [...items].sort((a, b) => {
      const ai = orderMap.has(a.ref) ? Number(orderMap.get(a.ref)) : Number.MAX_SAFE_INTEGER;
      const bi = orderMap.has(b.ref) ? Number(orderMap.get(b.ref)) : Number.MAX_SAFE_INTEGER;
      if (ai === bi) return 0;
      return ai - bi;
    });
  }

  function parsePositiveInt(value: any): number {
    const n = Number(String(value ?? '').trim());
    return Number.isFinite(n) && n > 0 ? Math.trunc(n) : 0;
  }

  function storeIdFromRef(ref: string): number {
    const raw = String(ref || '').trim();
    if (!raw.startsWith('db:')) return 0;
    return parsePositiveInt(raw.slice(3));
  }

  function resolveDraftStoreId(draft?: ApiDraft | null, ref = ''): number {
    const fromDraft = parsePositiveInt(draft?.storeId);
    if (fromDraft > 0) return fromDraft;
    return storeIdFromRef(ref);
  }

  function normalizeTemplateName(value: any): string {
    return String(value || '')
      .trim()
      .toLowerCase()
      .replace(/\s+/g, ' ');
  }

  function normalizePathLike(value: any): string {
    const raw = String(value || '').trim();
    if (!raw) return '/';
    return raw.startsWith('/') ? raw : `/${raw}`;
  }

  function normalizeBaseUrlLike(value: any): string {
    return String(value || '').trim().replace(/\/+$/, '');
  }

  function duplicateNameMatches(name: string, current?: ApiDraft | null): ApiDraft[] {
    const key = normalizeTemplateName(name);
    if (!key) return [];
    const currentStoreId = resolveDraftStoreId(current || null, selectedRef);
    const currentLocalId = String(current?.localId || '').trim();
    return drafts.filter((d) => {
      if (normalizeTemplateName(d.name) !== key) return false;
      const dStoreId = resolveDraftStoreId(d, refOf(d));
      if (currentStoreId > 0 && dStoreId > 0) return dStoreId !== currentStoreId;
      if (!currentStoreId && currentLocalId) return String(d.localId || '').trim() !== currentLocalId;
      return true;
    });
  }

  function fingerprintForDraft(draft: ApiDraft | null | undefined) {
    return {
      name: normalizeTemplateName(draft?.name),
      method: String(draft?.method || 'GET').trim().toUpperCase(),
      baseUrl: normalizeBaseUrlLike(draft?.baseUrl),
      path: normalizePathLike(draft?.path)
    };
  }

  function fingerprintFromRow(row: any) {
    const cfg = tryObj(row?.config_json);
    return {
      id: extractStoreIdFromRow(row),
      name: normalizeTemplateName(row?.api_name ?? cfg?.api_name ?? ''),
      method: String(row?.method ?? cfg?.method ?? 'GET').trim().toUpperCase(),
      baseUrl: normalizeBaseUrlLike(row?.base_url ?? cfg?.base_url ?? ''),
      path: normalizePathLike(row?.path ?? cfg?.path ?? '/')
    };
  }

  async function resolveServerStoreIdForDraft(draft: ApiDraft | null | undefined): Promise<number> {
    if (!draft) return 0;
    const fp = fingerprintForDraft(draft);
    if (!fp.name) return 0;
    try {
      const j = await apiJson<{ api_configs: any[] }>(`${apiBase}/api-configs`, { headers: headers() });
      const rows = Array.isArray(j?.api_configs) ? j.api_configs : [];
      const matches = rows
        .map((row) => fingerprintFromRow(row))
        .filter((x) => x.id > 0)
        .filter(
          (x) =>
            x.name === fp.name &&
            x.method === fp.method &&
            x.baseUrl === fp.baseUrl &&
            x.path === fp.path
        );
      return matches.length === 1 ? matches[0].id : 0;
    } catch {
      return 0;
    }
  }

  function extractStoreIdFromRow(row: any): number {
    const cfg = tryObj(row?.config_json);
    const candidates = [
      row?.id,
      row?.api_config_id,
      row?.api_id,
      row?.config_id,
      row?.store_id,
      cfg?.id,
      cfg?.api_config_id,
      cfg?.api_id,
      cfg?.store_id
    ];
    for (const candidate of candidates) {
      const parsed = parsePositiveInt(candidate);
      if (parsed > 0) return parsed;
    }
    return 0;
  }

  function stableJsonStringify(value: any): string {
    if (value === null || typeof value !== 'object') return JSON.stringify(value);
    if (Array.isArray(value)) return `[${value.map((item) => stableJsonStringify(item)).join(',')}]`;
    const keys = Object.keys(value).sort();
    return `{${keys.map((k) => `${JSON.stringify(k)}:${stableJsonStringify(value[k])}`).join(',')}}`;
  }

  function responseFingerprint(value: any): string {
    if (typeof value === 'string') return value;
    try {
      return stableJsonStringify(value);
    } catch {
      try {
        return JSON.stringify(value);
      } catch {
        return String(value);
      }
    }
  }

  function resolveAbsoluteUrl(urlText: string, draft: ApiDraft) {
    const raw = String(urlText || '').trim();
    if (!raw) return fullUrl(draft);
    if (/^https?:\/\//i.test(raw)) return raw;
    const base = String(draft.baseUrl || '').replace(/\/$/, '');
    const path = raw.startsWith('/') ? raw : `/${raw}`;
    return `${base}${path}`;
  }

  function decodeUrlPathname(pathname: string) {
    const raw = String(pathname || '/');
    if (!raw) return '/';
    try {
      return decodeURIComponent(raw) || '/';
    } catch {
      return raw || '/';
    }
  }

  function normalizeRequestHeaders(input?: HeadersInit): Record<string, string> {
    const out: Record<string, string> = {};
    if (!input) return out;
    if (input instanceof Headers) {
      input.forEach((v, k) => {
        out[k] = v;
      });
      return out;
    }
    if (Array.isArray(input)) {
      input.forEach(([k, v]) => {
        out[String(k)] = String(v);
      });
      return out;
    }
    Object.entries(input as Record<string, any>).forEach(([k, v]) => {
      if (v === undefined || v === null) return;
      out[String(k)] = String(v);
    });
    return out;
  }

  function findHeaderValue(headers: Record<string, any>, headerName: string) {
    const target = String(headerName || '').trim().toLowerCase();
    if (!target || !headers || typeof headers !== 'object') return '';
    for (const [k, v] of Object.entries(headers)) {
      if (String(k || '').trim().toLowerCase() === target) return String(v ?? '').trim();
    }
    return '';
  }

  function ensureHeaderValue(headers: Record<string, any>, headerName: string, headerValue: string) {
    const existing = findHeaderValue(headers, headerName);
    if (existing) return;
    headers[headerName] = headerValue;
  }

  function toFormUrlEncoded(body: any) {
    if (body === undefined || body === null) return '';
    if (typeof body === 'string') return body;
    if (typeof body !== 'object') return String(body);
    const query = new URLSearchParams();
    Object.entries(body).forEach(([k, v]) => {
      if (v === undefined || v === null) return;
      if (Array.isArray(v)) {
        v.forEach((item) => query.append(k, item === undefined || item === null ? '' : String(item)));
        return;
      }
      if (typeof v === 'object') {
        query.append(k, JSON.stringify(v));
        return;
      }
      query.append(k, String(v));
    });
    return query.toString();
  }

  function buildHttpRequestBodyByContentType(method: string, headers: Record<string, any>, body: any) {
    const upperMethod = String(method || 'GET').toUpperCase();
    if (upperMethod === 'GET' || upperMethod === 'HEAD' || upperMethod === 'DELETE') return undefined;
    if (body === undefined || body === null) return undefined;
    if (typeof body === 'string') return body;
    const contentType = findHeaderValue(headers, 'Content-Type').toLowerCase();
    if (contentType.includes('application/x-www-form-urlencoded')) {
      return toFormUrlEncoded(body);
    }
    if (typeof body === 'object') {
      if (!contentType) ensureHeaderValue(headers, 'Content-Type', 'application/json');
      return JSON.stringify(body);
    }
    return String(body);
  }

  async function runHttpRequestViaServer(url: string, init: RequestInit, timeoutMs = 30_000) {
    let requestBody: any = null;
    if (typeof init.body === 'string') requestBody = init.body;
    else if (init.body !== undefined && init.body !== null) requestBody = String(init.body);
    return apiJson<{
      ok: boolean;
      status: number;
      status_text?: string;
      headers?: Record<string, string>;
      body_text?: string;
      body_json?: any;
    }>(`${apiBase}/http-request`, {
      method: 'POST',
      headers: headers(),
      body: JSON.stringify({
        method: String(init.method || 'GET').toUpperCase(),
        url,
        headers: normalizeRequestHeaders(init.headers),
        body: requestBody,
        timeout_ms: timeoutMs
      })
    });
  }

  async function runHttpRequestViaServerWithRetry(
    url: string,
    init: RequestInit,
    timeoutMs = 30_000,
    retryConfig?: { attempts?: number; baseDelayMs?: number; maxDelayMs?: number }
  ) {
    const attemptsMax = Math.max(1, Math.min(10, Number(retryConfig?.attempts || RETRY_MAX_ATTEMPTS_DEFAULT)));
    const baseDelayMs = Math.max(50, Number(retryConfig?.baseDelayMs || RETRY_BASE_DELAY_MS_DEFAULT));
    const maxDelayMs = Math.max(baseDelayMs, Number(retryConfig?.maxDelayMs || RETRY_MAX_DELAY_MS_DEFAULT));
    let attempt = 0;
    let lastError: any = null;

    while (attempt < attemptsMax) {
      attempt += 1;
      try {
        const proxied = await runHttpRequestViaServer(url, init, timeoutMs);
        const status = Number(proxied?.status || 0);
        if (attempt < attemptsMax && shouldRetryStatus(status)) {
          const delay = retryDelayMs(attempt, baseDelayMs, maxDelayMs);
          await new Promise((resolve) => setTimeout(resolve, delay));
          continue;
        }
        return { proxied, attempts: attempt };
      } catch (error: any) {
        lastError = error;
        if (attempt >= attemptsMax) break;
        const delay = retryDelayMs(attempt, baseDelayMs, maxDelayMs);
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
    throw lastError || new Error('Ошибка запроса после повторов');
  }

  function oauth2MappingsForDraft(d: ApiDraft) {
    const normalized = (Array.isArray(d.oauth2ResponseMappings) ? d.oauth2ResponseMappings : [])
      .map((m) => ({
        id: String(m?.id || uid()),
        responsePath: String(m?.responsePath || '').trim(),
        alias: String(m?.alias || '').trim()
      }))
      .filter((m) => m.responsePath && m.alias);
    if (normalized.length) return normalized;
    return [
      { id: uid(), responsePath: d.oauth2TokenField || 'access_token', alias: 'access_token' },
      { id: uid(), responsePath: d.oauth2ExpiresField || 'expires_in', alias: 'expires_in' },
      { id: uid(), responsePath: d.oauth2TokenTypeField || 'token_type', alias: 'token_type' }
    ];
  }

  function resolveTokenRequestUrl(d: ApiDraft) {
    const directUrl = String(d.oauth2RequestUrl || d.oauth2TokenUrl || '').trim();
    if (directUrl) return resolveAbsoluteUrl(directUrl, d);
    const host = String(d.oauth2RequestHost || '').trim().replace(/\/$/, '');
    const pathRaw = String(d.oauth2RequestPath || '').trim();
    const path = pathRaw ? (pathRaw.startsWith('/') ? pathRaw : `/${pathRaw}`) : '';
    if (!host) return '';
    return `${host}${path}`;
  }

  function buildTokenRequestBase(d: ApiDraft) {
    return {
      method: toHttpMethod(String(d.oauth2RequestMethod || 'POST').toUpperCase()),
      url: resolveTokenRequestUrl(d),
      headers: parseJsonObjectField('Подзапрос токена: headers', d.oauth2RequestHeadersJson || '{}'),
      query: parseJsonObjectField('Подзапрос токена: query', d.oauth2RequestQueryJson || '{}'),
      body:
        (() => {
          const parsed = parseJsonAnyField('Подзапрос токена: body', d.oauth2RequestBodyJson || '{}');
          if (parsed && typeof parsed === 'object') return deepClone(parsed);
          return parsed;
        })()
    };
  }

  type OAuthTokenResolveResult = {
    token: string;
    tokenType: string;
    mappedAliases: Record<string, any>;
    requestPreview: any;
    responsePreview: any;
    cacheHit: boolean;
  };

  async function getOAuthToken(
    d: ApiDraft,
    contextValues: Record<string, any> = {}
  ): Promise<OAuthTokenResolveResult> {
    const reqBase = buildTokenRequestBase(d);
    if (!reqBase.url) {
      throw new Error('Token request: не указан URL/Host подзапроса токена');
    }

    const reqHeaders = deepClone(reqBase.headers || {});
    const reqQuery = deepClone(reqBase.query || {});
    const reqBody =
      reqBase.body && typeof reqBase.body === 'object'
        ? deepClone(reqBase.body)
        : reqBase.body;

    applyParametersToValue(reqHeaders, contextValues || {});
    applyParametersToValue(reqQuery, contextValues || {});
    applyParametersToValue(reqBody, contextValues || {});

    let reqUrl = replaceParameterTokens(reqBase.url, contextValues || {});
    const reqUrlObj = new URL(reqUrl);
    Object.entries(reqQuery || {}).forEach(([k, v]) => reqUrlObj.searchParams.set(k, String(v)));
    reqUrl = reqUrlObj.toString();

    const unresolved = new Set<string>();
    collectParameterTokens(reqUrl, unresolved);
    collectParameterTokens(reqHeaders, unresolved);
    collectParameterTokens(reqQuery, unresolved);
    collectParameterTokens(reqBody, unresolved);
    if (unresolved.size) {
      throw new Error(`Token request: не удалось подставить параметры: ${Array.from(unresolved).join(', ')}`);
    }

    const outgoingBody = buildHttpRequestBodyByContentType(reqBase.method, reqHeaders, reqBody);

    const cacheKey = `${refOf(d)}|${reqBase.method}|${reqUrl}|${JSON.stringify(reqHeaders)}|${JSON.stringify(outgoingBody)}`;
    const cached = oauthTokenCache[cacheKey];
    if (cached && Date.now() < cached.expiresAt - 60_000) {
      const mappedFromCache = {
        access_token: cached.token,
        token_type: cached.tokenType || 'Bearer',
        expires_in: Math.max(0, Math.floor((cached.expiresAt - Date.now()) / 1000))
      };
      return {
        token: cached.token,
        tokenType: cached.tokenType || 'Bearer',
        mappedAliases: mappedFromCache,
        requestPreview: {
          method: reqBase.method,
          url: reqUrl,
          headers: reqHeaders,
          body: outgoingBody,
          source: 'cache'
        },
        responsePreview: {
          status: 200,
          from_cache: true
        },
        cacheHit: true
      };
    }

    const proxyResp = await runHttpRequestViaServerWithRetry(
      reqUrl,
      {
        method: reqBase.method,
        headers: reqHeaders,
        body: outgoingBody
      },
      30_000
    );
    const httpPayload = proxyResp?.proxied || {};
    const resOk = Boolean(httpPayload?.ok);
    const resStatus = Number(httpPayload?.status || 0);
    const txt = String(httpPayload?.body_text || '');
    const json = httpPayload?.body_json && typeof httpPayload.body_json === 'object'
      ? httpPayload.body_json
      : (() => {
          try {
            return txt ? JSON.parse(txt) : {};
          } catch {
            return {};
          }
        })();
    if (!resOk) {
      throw new Error(`Token request: HTTP ${resStatus}. ${txt || JSON.stringify(json)}`);
    }

    const mappings = oauth2MappingsForDraft(d);
    const mappedAliases: Record<string, any> = {};
    const missingMappings: string[] = [];
    for (const map of mappings) {
      const val = getByPath(json, map.responsePath);
      if (val === undefined) {
        missingMappings.push(`${map.alias} <- ${map.responsePath}`);
        continue;
      }
      mappedAliases[map.alias] = val;
    }
    if (missingMappings.length) {
      throw new Error(`Token response mapping: не найдены поля: ${missingMappings.join(', ')}`);
    }

    const tokenAlias = Object.prototype.hasOwnProperty.call(mappedAliases, 'access_token')
      ? 'access_token'
      : String(d.oauth2TokenField || 'access_token');
    const token = String(mappedAliases[tokenAlias] ?? getByPath(json, tokenAlias) ?? '').trim();
    if (!token) {
      throw new Error(`Token response mapping: не найден токен (alias/поле ${tokenAlias})`);
    }
    const tokenType = String(
      mappedAliases.token_type ??
        getByPath(json, d.oauth2TokenTypeField || 'token_type') ??
        getByPath(json, 'token_type') ??
        'Bearer'
    ).trim() || 'Bearer';
    const expiresInRaw =
      mappedAliases.expires_in ??
      getByPath(json, d.oauth2ExpiresField || 'expires_in') ??
      getByPath(json, 'expires_in') ??
      0;
    const expiresIn = Number(expiresInRaw || 0);

    oauthTokenCache = {
      ...oauthTokenCache,
      [cacheKey]: {
        token,
        tokenType,
        expiresAt: Date.now() + Math.max(0, expiresIn) * 1000
      }
    };

    return {
      token,
      tokenType,
      mappedAliases: {
        ...mappedAliases,
        access_token: token,
        token_type: tokenType,
        expires_in: expiresIn
      },
      requestPreview: {
        method: reqBase.method,
        url: reqUrl,
        headers: reqHeaders,
        body: outgoingBody
      },
      responsePreview: {
        status: resStatus,
        headers: httpPayload?.headers || {},
        body: json && typeof json === 'object' && Object.keys(json).length ? json : txt
      },
      cacheHit: false
    };
  }

  function previewUrlFromInput(selectedDraft: ApiDraft | null): string {
    if (!selectedDraft) return '';
    const raw = String(requestInput || '').trim();
    if (!raw) {
      try {
        return fullUrl(selectedDraft);
      } catch {
        return `${selectedDraft.baseUrl}${selectedDraft.path}`;
      }
    }
    const curlMatch = raw.match(/curl\s+(?:-X\s+(GET|POST|PUT|PATCH|DELETE)\s+)?['\"]([^'\"]+)['\"]/i);
    const source = curlMatch ? curlMatch[2] : raw;
    try {
      return new URL(source).toString();
    } catch {
      try {
        return fullUrl(selectedDraft);
      } catch {
        return `${selectedDraft.baseUrl}${selectedDraft.path}`;
      }
    }
  }

  function applyMyPreviewToFields() {
    err = '';
    myPreviewApplyMessage = '';
    if (!selected) return;
    const src = String(myApiPreviewDraft || '').trim();
    if (!src) {
      err = 'Предпросмотр API пуст';
      return;
    }
    let parsed: any;
    try {
      parsed = JSON.parse(src);
    } catch {
      err = 'Предпросмотр API: некорректный JSON';
      return;
    }
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      err = 'Предпросмотр API: ожидается JSON-объект';
      return;
    }

    const method = parsed?.method ? toHttpMethod(String(parsed.method).toUpperCase()) : undefined;
    const urlRaw = String(parsed?.url || '').trim();
    const auth = parsed?.auth && typeof parsed.auth === 'object' && !Array.isArray(parsed.auth) ? parsed.auth : {};
    const headersObj = parsed?.headers && typeof parsed.headers === 'object' && !Array.isArray(parsed.headers) ? parsed.headers : {};
    const queryObj = parsed?.query && typeof parsed.query === 'object' && !Array.isArray(parsed.query) ? parsed.query : undefined;
    const bodyAny = parsed?.body ?? {};

    let baseUrl = selected.baseUrl;
    let path = selected.path;
    let queryFromUrl: Record<string, string> = {};
    if (urlRaw) {
      try {
        const u = new URL(urlRaw);
        baseUrl = u.origin;
        path = decodeUrlPathname(u.pathname);
        u.searchParams.forEach((v, k) => {
          queryFromUrl[k] = v;
        });
      } catch {
        err = 'Предпросмотр API: некорректный URL';
        return;
      }
    }
    const finalQuery = queryObj ?? queryFromUrl;

    mutateSelected((d) => {
      if (method) d.method = method;
      d.baseUrl = baseUrl;
      d.path = path;
      d.authJson = toPrettyJson(auth);
      d.headersJson = toPrettyJson(headersObj);
      d.queryJson = toPrettyJson(finalQuery);
      d.bodyJson = toPrettyJson(bodyAny);
    });

    const next = byRef(selectedRef);
    if (next) {
      requestInput = `${next.baseUrl.replace(/\/$/, '')}${next.path.startsWith('/') ? next.path : `/${next.path}`}`;
    }
    myPreviewDirty = false;
    myPreviewApplyMessage = 'Применено в поля';
  }

  function applyUrlInput(raw: string) {
    const s = String(raw || '').trim();
    if (!selected || !s) return;

    // поддержка: curl 'https://...' / curl "https://..."
    const curlMatch = s.match(/curl\s+(?:-X\s+(GET|POST|PUT|PATCH|DELETE)\s+)?['\"]([^'\"]+)['\"]/i);
    const source = curlMatch ? curlMatch[2] : s;
    const method = curlMatch?.[1] ? toHttpMethod(curlMatch[1].toUpperCase()) : selected.method;

    try {
      const u = new URL(source);
      const q: Record<string, string> = {};
      u.searchParams.forEach((v, k) => {
        q[k] = v;
      });
      mutateSelected((d) => {
        d.method = method;
        d.baseUrl = u.origin;
        d.path = decodeUrlPathname(u.pathname);
        d.queryJson = toPrettyJson(q);
      });
      err = '';
    } catch {
      err = 'Некорректная строка подключения';
    }
  }

  function unwrapCodeFence(raw: string): string {
    const s = String(raw || '').trim();
    const m = s.match(/```(?:json|bash|sh)?\s*([\s\S]*?)```/i);
    return (m?.[1] || s).trim();
  }

  function toObj(v: any): Record<string, any> {
    return v && typeof v === 'object' && !Array.isArray(v) ? v : {};
  }

  function fromHeaderLines(raw: string): Record<string, string> {
    const out: Record<string, string> = {};
    for (const line of String(raw || '').split('\n')) {
      const s = line.trim();
      if (!s || !s.includes(':')) continue;
      const idx = s.indexOf(':');
      const k = s.slice(0, idx).trim();
      const v = s.slice(idx + 1).trim();
      if (k) out[k] = v;
    }
    return out;
  }

  function splitAuthHeaders(headersObj: Record<string, any>) {
    const auth: Record<string, string> = {};
    const headersOut: Record<string, string> = {};
    for (const [k, v] of Object.entries(headersObj || {})) {
      const key = String(k || '').trim();
      if (!key) continue;
      const val = String(v ?? '');
      const lk = key.toLowerCase();
      const isAuth =
        lk === 'authorization' ||
        lk === 'x-api-key' ||
        lk === 'api-key' ||
        lk === 'apikey' ||
        lk.includes('token');
      if (isAuth) auth[key] = val;
      else headersOut[key] = val;
    }
    return { auth, headersOut };
  }

  function parseCurlTemplate(raw: string) {
    const src = String(raw || '');
    if (!/\bcurl\b/i.test(src)) return null;

    const methodMatch = src.match(/(?:^|\s)-X\s+(GET|POST|PUT|PATCH|DELETE)\b/i);
    const method = methodMatch ? toHttpMethod(methodMatch[1].toUpperCase()) : undefined;

    const urlQuoted = src.match(/curl(?:\s+-[A-Za-z]\s+\S+)*\s+['"]([^'"]+)['"]/i);
    const urlBare = src.match(/(https?:\/\/[^\s"'\\]+)/i);
    const url = (urlQuoted?.[1] || urlBare?.[1] || '').trim();

    const headerRe = /(?:^|\s)-H\s+['"]([^'"]+)['"]/gi;
    const headerLines: string[] = [];
    let hm: RegExpExecArray | null;
    while ((hm = headerRe.exec(src))) headerLines.push(hm[1]);

    const dataMatch = src.match(/(?:^|\s)(?:--data-raw|--data|-d)\s+('(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*"|[^\s]+)/i);
    const bodyRaw = dataMatch ? String(dataMatch[1] || '').replace(/^['"]|['"]$/g, '') : '';

    return { method, url, headerLines, bodyRaw };
  }

  function parseTemplateToPatch(raw: string) {
    const text = unwrapCodeFence(raw);
    if (!text) return { ok: false, message: '' as string, patch: null as any };

    const curl = parseCurlTemplate(text);
    if (curl?.url) {
      const u = new URL(curl.url);
      const query: Record<string, string> = {};
      u.searchParams.forEach((v, k) => (query[k] = v));
      const headersObj = fromHeaderLines(curl.headerLines.join('\n'));
      const { auth, headersOut } = splitAuthHeaders(headersObj);
      let bodyValue: any = {};
      if (curl.bodyRaw) {
        try {
          bodyValue = JSON.parse(curl.bodyRaw);
        } catch {
          bodyValue = { raw: curl.bodyRaw };
        }
      }
      return {
        ok: true,
        message: 'Шаблон разобран из curl',
        patch: {
          method: curl.method,
          baseUrl: u.origin,
          path: decodeUrlPathname(u.pathname),
          queryJson: toPrettyJson(query),
          headersJson: toPrettyJson(headersOut),
          authJson: toPrettyJson(auth),
          bodyJson: toPrettyJson(bodyValue)
        }
      };
    }

    try {
      const parsed = JSON.parse(text);
      const root = toObj(parsed?.request || parsed);
      const method = root?.method ? toHttpMethod(String(root.method).toUpperCase()) : undefined;
      const urlRaw = String(root?.url || root?.endpoint || '').trim();
      const baseUrl = String(root?.baseUrl || root?.base_url || '').trim();
      const path = String(root?.path || '').trim();
      const headersSrc = toObj(root?.headers);
      const authSrc = toObj(root?.auth || root?.authorization);
      const querySrc = toObj(root?.query || root?.params);
      const hasExplicitBody = root && (Object.prototype.hasOwnProperty.call(root, 'body') || Object.prototype.hasOwnProperty.call(root, 'data'));
      const bodySrc = hasExplicitBody ? root?.body ?? root?.data ?? {} : parsed;

      let finalBaseUrl = baseUrl;
      let finalPath = path || '/';
      let queryFromUrl: Record<string, string> = {};
      if (urlRaw) {
        const u = new URL(urlRaw);
        finalBaseUrl = u.origin;
        finalPath = decodeUrlPathname(u.pathname);
        u.searchParams.forEach((v, k) => (queryFromUrl[k] = v));
      }

      const { auth, headersOut } = splitAuthHeaders(headersSrc);
      const authFinal = Object.keys(authSrc).length ? authSrc : auth;
      const queryFinal = Object.keys(querySrc).length ? querySrc : queryFromUrl;
      const modeMessage = hasExplicitBody || urlRaw || method
        ? 'Шаблон разобран из JSON'
        : 'Шаблон разобран как Body JSON';
      return {
        ok: true,
        message: modeMessage,
        patch: {
          method,
          baseUrl: finalBaseUrl,
          path: finalPath,
          headersJson: toPrettyJson(headersOut),
          authJson: toPrettyJson(authFinal),
          queryJson: toPrettyJson(queryFinal),
          bodyJson: toPrettyJson(bodySrc)
        }
      };
    } catch {
      // ignore and try URL fallback
    }

    const urlMatch = text.match(/https?:\/\/[^\s"'\\]+/i);
    if (urlMatch?.[0]) {
      const u = new URL(urlMatch[0]);
      const query: Record<string, string> = {};
      u.searchParams.forEach((v, k) => (query[k] = v));
      return {
        ok: true,
        message: 'Шаблон разобран из URL',
        patch: {
          baseUrl: u.origin,
          path: decodeUrlPathname(u.pathname),
          queryJson: toPrettyJson(query)
        }
      };
    }

    return {
      ok: false,
      message: 'Не удалось распознать шаблон. Вставьте curl, URL или JSON.',
      patch: null
    };
  }

  function applyTemplatePatch(patch: any) {
    if (!patch) return;
    mutateSelected((d) => {
      if (patch.method) d.method = patch.method;
      if (typeof patch.baseUrl === 'string') d.baseUrl = patch.baseUrl;
      if (typeof patch.path === 'string') d.path = patch.path || '/';
      if (typeof patch.authJson === 'string') d.authJson = patch.authJson;
      if (typeof patch.headersJson === 'string') d.headersJson = patch.headersJson;
      if (typeof patch.queryJson === 'string') d.queryJson = patch.queryJson;
      if (typeof patch.bodyJson === 'string') d.bodyJson = patch.bodyJson;
    });
    const next = byRef(selectedRef);
    if (next) {
      requestInput = `${next.baseUrl.replace(/\/$/, '')}${next.path.startsWith('/') ? next.path : `/${next.path}`}`;
    }
  }

  function parseTemplateNow(force = false) {
    if (!selected) {
      templateParseMessage = 'Выбери API в правом блоке';
      return;
    }
    const srcRaw = exampleApiEl ? String(exampleApiEl.value || '') : String(selected.exampleRequest || '');
    const src = srcRaw.trim();
    if (!src) {
      templateParseMessage = '';
      return;
    }
    mutateSelected((d) => (d.exampleRequest = srcRaw));
    if (!force && src.length < 8) return;
    const parsed = parseTemplateToPatch(src);
    if (!parsed.ok) {
      if (force) templateParseMessage = parsed.message;
      return;
    }
    applyTemplatePatch(parsed.patch);
    templateParseMessage = parsed.message;
  }

  function clearTemplateField() {
    if (exampleApiEl) exampleApiEl.value = '';
    mutateSelected((d) => (d.exampleRequest = ''));
    templateParseMessage = '';
    syncLeftTextareasHeight();
  }

  function onTemplateParseClick() {
    parseTemplateNow(true);
  }

  function onTemplateClearClick() {
    clearTemplateField();
  }

  function mutateSelected(mutator: (d: ApiDraft) => void) {
    if (!selectedRef) return;
    drafts = drafts.map((d) => {
      if (refOf(d) !== selectedRef) return d;
      const next = { ...d };
      mutator(next);
      return next;
    });
  }

  function isGroupedDispatchEnabled(draft: ApiDraft | null) {
    if (!draft) return false;
    const sanitized = sanitizeAliasReferences(draft);
    return sanitized.groupByAliases.length > 0;
  }

  function normalizeDraggedToken(raw: string) {
    const src = String(raw || '').trim();
    if (!src) return '';
    if (/^\{\{\s*[^{}]+\s*\}\}$/.test(src)) return src;
    return `{{${src}}}`;
  }

  function insertTokenAt(raw: string, token: string, start?: number, end?: number) {
    const base = String(raw || '');
    if (typeof start === 'number' && typeof end === 'number' && Number.isFinite(start) && Number.isFinite(end)) {
      return `${base.slice(0, start)}${token}${base.slice(end)}`;
    }
    if (!base.trim()) return token;
    return `${base}${base.endsWith('\n') ? '' : '\n'}${token}`;
  }

  function aliasChipDragStart(alias: string, event: DragEvent) {
    if (!event.dataTransfer) return;
    event.dataTransfer.setData('text/plain', `{{${alias}}}`);
    event.dataTransfer.effectAllowed = 'copy';
  }

  function dropAliasTokenToField(
    event: DragEvent,
    field:
      | 'authJson'
      | 'headersJson'
      | 'queryJson'
      | 'bodyJson'
      | 'oauth2RequestHeadersJson'
      | 'oauth2RequestQueryJson'
      | 'oauth2RequestBodyJson',
    el?: HTMLTextAreaElement | null
  ) {
    event.preventDefault();
    const token = normalizeDraggedToken(String(event.dataTransfer?.getData('text/plain') || ''));
    if (!token) return;
    const targetEl = (event.currentTarget as HTMLTextAreaElement) || el || null;
    const selStart = targetEl?.selectionStart;
    const selEnd = targetEl?.selectionEnd;
    mutateSelected((d) => {
      (d as any)[field] = insertTokenAt((d as any)[field], token, selStart ?? undefined, selEnd ?? undefined);
    });
    requestAnimationFrame(() => {
      if (!targetEl) return;
      const pos = (selStart ?? String((targetEl as any).value || '').length) + token.length;
      targetEl.focus();
      try {
        targetEl.setSelectionRange(pos, pos);
      } catch {
        // ignore
      }
    });
  }

  function hasDataModelConfigured(draft: ApiDraft | null) {
    if (!draft) return false;
    const hasTableFields =
      Array.isArray(draft.dataFields) &&
      draft.dataFields.some((f) => String(f?.tableId || '').trim() && String(f?.field || '').trim() && String(f?.alias || '').trim());
    const hasDateParams =
      Array.isArray(draft.dataDateParams) &&
      draft.dataDateParams.some((p) => String(p?.alias || '').trim());
    return Boolean(hasTableFields || hasDateParams);
  }

  function pad2(v: number) {
    return String(v).padStart(2, '0');
  }

  function parseDateInput(raw: string): Date | null {
    const src = String(raw || '').trim();
    if (!src) return null;
    if (/^\d{4}-\d{2}-\d{2}$/.test(src)) {
      const [y, m, d] = src.split('-').map((x) => Number(x));
      return new Date(Date.UTC(y, (m || 1) - 1, d || 1, 0, 0, 0, 0));
    }
    if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$/.test(src)) {
      const [datePart, timePart] = src.split('T');
      const [y, m, d] = datePart.split('-').map((x) => Number(x));
      const [hh, mm, ss] = timePart.split(':').map((x) => Number(x));
      return new Date(Date.UTC(y, (m || 1) - 1, d || 1, hh || 0, mm || 0, ss || 0, 0));
    }
    const parsed = new Date(src);
    if (Number.isNaN(parsed.getTime())) return null;
    return parsed;
  }

  function shiftDateByMonthsUtc(base: Date, deltaMonths: number) {
    if (!deltaMonths) return new Date(base.getTime());
    const d = new Date(base.getTime());
    const day = d.getUTCDate();
    d.setUTCDate(1);
    d.setUTCMonth(d.getUTCMonth() + deltaMonths);
    const maxDay = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 0)).getUTCDate();
    d.setUTCDate(Math.min(day, maxDay));
    return d;
  }

  function applyDateAnchorPreset(base: Date, anchor: DateAnchorPreset) {
    const d = new Date(base.getTime());
    switch (anchor) {
      case 'start_of_day':
        d.setUTCHours(0, 0, 0, 0);
        return d;
      case 'end_of_day':
        d.setUTCHours(23, 59, 59, 0);
        return d;
      case 'start_of_week': {
        d.setUTCHours(0, 0, 0, 0);
        const day = d.getUTCDay();
        const diff = (day + 6) % 7;
        d.setUTCDate(d.getUTCDate() - diff);
        return d;
      }
      case 'end_of_week': {
        d.setUTCHours(23, 59, 59, 0);
        const day = d.getUTCDay();
        const diff = 6 - ((day + 6) % 7);
        d.setUTCDate(d.getUTCDate() + diff);
        return d;
      }
      case 'start_of_month':
        d.setUTCHours(0, 0, 0, 0);
        d.setUTCDate(1);
        return d;
      case 'end_of_month':
        d.setUTCHours(23, 59, 59, 0);
        d.setUTCMonth(d.getUTCMonth() + 1, 0);
        return d;
      case 'start_of_year':
        d.setUTCHours(0, 0, 0, 0);
        d.setUTCMonth(0, 1);
        return d;
      case 'end_of_year':
        d.setUTCHours(23, 59, 59, 0);
        d.setUTCMonth(11, 31);
        return d;
      case 'raw':
      default:
        return d;
    }
  }

  function formatDateByPreset(value: Date, preset: DateFormatPreset): string {
    const y = value.getUTCFullYear();
    const m = pad2(value.getUTCMonth() + 1);
    const d = pad2(value.getUTCDate());
    const hh = pad2(value.getUTCHours());
    const mm = pad2(value.getUTCMinutes());
    const ss = pad2(value.getUTCSeconds());
    if (preset === 'yyyy_mm_dd') return `${y}-${m}-${d}`;
    if (preset === 'datetime_utc_z') return `${y}-${m}-${d}T${hh}:${mm}:${ss}Z`;
    if (preset === 'datetime_msk' || preset === 'rfc3339_msk') {
      const msk = new Date(value.getTime() + 3 * 60 * 60 * 1000);
      const my = msk.getUTCFullYear();
      const mmn = pad2(msk.getUTCMonth() + 1);
      const md = pad2(msk.getUTCDate());
      const mhh = pad2(msk.getUTCHours());
      const mmi = pad2(msk.getUTCMinutes());
      const mss = pad2(msk.getUTCSeconds());
      if (preset === 'datetime_msk') return `${my}-${mmn}-${md}T${mhh}:${mmi}:${mss}`;
      return `${my}-${mmn}-${md}T${mhh}:${mmi}:${mss}+03:00`;
    }
    return value.toISOString();
  }

  function normalizeDataDateParameter(input: Partial<DataDateParameter> | null | undefined, idx = 1): DataDateParameter {
    const baseRaw = String(input?.basePreset || '').trim() as DateBasePreset;
    const anchorRaw = String(input?.anchorPreset || '').trim() as DateAnchorPreset;
    const formatRaw = String(input?.formatPreset || '').trim() as DateFormatPreset;
    const basePreset: DateBasePreset = baseRaw === 'custom' ? 'custom' : 'today';
    const anchorPreset: DateAnchorPreset = DATE_ANCHOR_PRESETS.some((x) => x.value === anchorRaw) ? anchorRaw : 'raw';
    const formatPreset: DateFormatPreset = DATE_FORMAT_PRESETS.some((x) => x.value === formatRaw) ? formatRaw : 'yyyy_mm_dd';
    return {
      id: String(input?.id || uid()),
      alias: String(input?.alias || `date_${idx}`).trim(),
      basePreset,
      customDate: String(input?.customDate || '').trim(),
      anchorPreset,
      addDays: Number.isFinite(Number(input?.addDays)) ? Number(input?.addDays) : 0,
      addMonths: Number.isFinite(Number(input?.addMonths)) ? Number(input?.addMonths) : 0,
      formatPreset,
      grouped: Boolean(input?.grouped)
    };
  }

  function normalizeDataApiParameter(input: Partial<DataApiParameter> | null | undefined, idx = 1): DataApiParameter {
    const sourceTypeRaw = String(input?.sourceType || '').trim().toLowerCase();
    const sourceType: DataApiParameterSourceType =
      sourceTypeRaw === 'input' || sourceTypeRaw === 'output' ? (sourceTypeRaw as DataApiParameterSourceType) : 'system';
    const storeId = Number.isFinite(Number(input?.sourceApiStoreId)) ? Number(input?.sourceApiStoreId) : 0;
    const sourceApiRefRaw = String(input?.sourceApiRef || '').trim();
    const sourceApiRef = sourceApiRefRaw || (storeId > 0 ? `db:${Math.trunc(storeId)}` : '');
    return {
      id: String(input?.id || uid()),
      sourceApiRef,
      sourceApiStoreId: storeId > 0 ? storeId : undefined,
      sourceType,
      sourceKey: String(input?.sourceKey || '').trim(),
      alias: String(input?.alias || `api_param_${idx}`).trim(),
      grouped: Boolean(input?.grouped)
    };
  }

  function dataApiParametersForDraft(draft: ApiDraft | null): DataApiParameter[] {
    void draft;
    return [];
  }

  function sourceDraftByRefOrStoreId(sourceApiRef: string, sourceApiStoreId?: number): ApiDraft | null {
    const refRaw = String(sourceApiRef || '').trim();
    if (refRaw) {
      const by = byRef(refRaw);
      if (by) return by;
    }
    const storeId = Number(sourceApiStoreId || 0);
    if (storeId > 0) {
      return drafts.find((d) => Number(d?.storeId || 0) === storeId) || null;
    }
    return null;
  }

  function apiSourceDraftOptions(current: ApiDraft | null) {
    return (Array.isArray(drafts) ? drafts : [])
      .filter((d) => d && (!current || refOf(d) !== refOf(current)))
      .map((d) => ({
        ref: refOf(d),
        storeId: Number(d?.storeId || 0) || null,
        label: String(d?.name || '').trim() || `API ${d?.storeId || d?.localId || ''}`
      }));
  }

  function apiSystemFieldOptions() {
    return [
      { key: 'api_name', label: 'Название API' },
      { key: 'method', label: 'HTTP метод' },
      { key: 'base_url', label: 'Base URL' },
      { key: 'path', label: 'Path' },
      { key: 'auth_mode', label: 'Режим авторизации' },
      { key: 'execution_mode', label: 'Режим выполнения' },
      { key: 'sync_planner', label: 'Планировщик sync' },
      { key: 'async_concurrency', label: 'Лимит async' }
    ];
  }

  function sourceDraftInputAliases(source: ApiDraft | null) {
    if (!source) return [];
    const defs = uniqueAliasList(
      (Array.isArray(source.parameterDefinitions) ? source.parameterDefinitions : [])
        .map((p) => String(p?.alias || '').trim())
        .filter(Boolean)
    );
    const data = uniqueAliasList(
      (Array.isArray(source.dataFields) ? source.dataFields : [])
        .map((f) => String(f?.alias || '').trim())
        .filter(Boolean)
    );
    const dates = uniqueAliasList(dataDateParametersForDraft(source).map((p) => String(p?.alias || '').trim()).filter(Boolean));
    return uniqueAliasList([...defs, ...data, ...dates]);
  }

  function sourceDraftOutputPaths(source: ApiDraft | null): string[] {
    if (!source) return [];
    const fromPicked = uniqueAliasList((Array.isArray(source.pickedPaths) ? source.pickedPaths : []).map((p) => String(p || '').trim()).filter(Boolean));
    const fromCache: string[] = [];
    const cached = apiResponseCache[refOf(source)];
    if (cached !== undefined) {
      collectResponsePaths(cached, '', fromCache);
    }
    return uniqueAliasList([...fromPicked, ...fromCache].map((x) => String(x || '').trim()).filter(Boolean));
  }

  function resolveApiSystemValue(source: ApiDraft, sourceKey: string) {
    const key = String(sourceKey || '').trim().toLowerCase();
    if (key === 'api_name') return source.name;
    if (key === 'method') return source.method;
    if (key === 'base_url') return source.baseUrl;
    if (key === 'path') return source.path;
    if (key === 'auth_mode') return source.authMode;
    if (key === 'execution_mode') return source.executionMode;
    if (key === 'sync_planner') return source.syncPlanner;
    if (key === 'async_concurrency') return source.asyncConcurrency;
    return '';
  }

  function toDateAnchorPreset(value: string): DateAnchorPreset {
    const raw = String(value || '').trim();
    return DATE_ANCHOR_PRESETS.some((x) => x.value === raw) ? (raw as DateAnchorPreset) : 'raw';
  }

  function toDateFormatPreset(value: string): DateFormatPreset {
    const raw = String(value || '').trim();
    return DATE_FORMAT_PRESETS.some((x) => x.value === raw) ? (raw as DateFormatPreset) : 'yyyy_mm_dd';
  }

  function dataDateParametersForDraft(draft: ApiDraft | null): DataDateParameter[] {
    if (!draft) return [];
    return (Array.isArray(draft.dataDateParams) ? draft.dataDateParams : [])
      .map((p, idx) => normalizeDataDateParameter(p, idx + 1))
      .filter((p) => String(p.alias || '').trim());
  }

  function computeDateParameterValue(param: DataDateParameter, now = new Date()): { value: string; error?: string } {
    const normalized = normalizeDataDateParameter(param, 1);
    const base =
      normalized.basePreset === 'custom'
        ? parseDateInput(normalized.customDate)
        : new Date(now.getTime());
    if (!base) return { value: '', error: 'некорректная свободная дата' };
    let value = applyDateAnchorPreset(base, normalized.anchorPreset);
    value = shiftDateByMonthsUtc(value, Number(normalized.addMonths || 0));
    value.setUTCDate(value.getUTCDate() + Number(normalized.addDays || 0));
    return { value: formatDateByPreset(value, normalized.formatPreset) };
  }

  function parseDateValueFromTable(value: any): Date | null {
    if (value instanceof Date) {
      return Number.isNaN(value.getTime()) ? null : new Date(value.getTime());
    }
    if (typeof value === 'number' && Number.isFinite(value)) {
      const ms = Math.abs(value) > 1e11 ? value : value * 1000;
      const dt = new Date(ms);
      return Number.isNaN(dt.getTime()) ? null : dt;
    }
    if (typeof value === 'string') {
      const raw = String(value || '').trim();
      if (!raw) return null;
      if (/^\d+(\.\d+)?$/.test(raw)) {
        const num = Number(raw);
        if (Number.isFinite(num)) {
          const ms = Math.abs(num) > 1e11 ? num : num * 1000;
          const dt = new Date(ms);
          if (!Number.isNaN(dt.getTime())) return dt;
        }
      }
      const parsed = parseDateInput(raw);
      if (parsed) return parsed;
      const dt = new Date(raw);
      return Number.isNaN(dt.getTime()) ? null : dt;
    }
    return null;
  }

  function transformDataFieldDateValue(value: any, field: DataModelField): any {
    if (!field || String(field.dateMode || 'raw') !== 'process') return value;
    const dt = parseDateValueFromTable(value);
    if (!dt) return value;
    let v = applyDateAnchorPreset(dt, toDateAnchorPreset(String(field.dateAnchorPreset || 'raw')));
    v = shiftDateByMonthsUtc(v, Number(field.dateAddMonths || 0));
    v.setUTCDate(v.getUTCDate() + Number(field.dateAddDays || 0));
    return formatDateByPreset(v, toDateFormatPreset(String(field.dateFormatPreset || 'yyyy_mm_dd')));
  }

  function applyDataFieldTransformsToRows(draft: ApiDraft, rows: Array<Record<string, any>>) {
    const sourceRows = Array.isArray(rows) ? rows : [];
    if (!sourceRows.length) return sourceRows;
    const fields = (Array.isArray(draft?.dataFields) ? draft.dataFields : []).filter((f) => String(f?.alias || '').trim());
    if (!fields.length) return sourceRows;
    return sourceRows.map((row) => {
      const out = { ...(row || {}) };
      fields.forEach((field) => {
        if (!isDataFieldDateType(draft, field)) return;
        if (String(field.dateMode || 'raw') !== 'process') return;
        const found = findAliasInMap(out, String(field.alias || '').trim());
        if (!found.found) return;
        out[found.key] = transformDataFieldDateValue(found.value, field);
      });
      return out;
    });
  }

  function resolveDateAliasValues(draft: ApiDraft | null, requestedAliases: string[]) {
    const map: Record<string, any> = {};
    const issues: Record<string, string> = {};
    const requested = uniqueAliasList((requestedAliases || []).map((a) => String(a || '').trim()).filter(Boolean));
    if (!draft || !requested.length) return { map, issues };
    const params = dataDateParametersForDraft(draft);
    if (!params.length) return { map, issues };
    const byLower = new Map<string, DataDateParameter>();
    params.forEach((p) => {
      const key = String(p.alias || '').trim().toLowerCase();
      if (key && !byLower.has(key)) byLower.set(key, p);
    });
    requested.forEach((alias) => {
      const param = byLower.get(alias.toLowerCase());
      if (!param) return;
      const computed = computeDateParameterValue(param);
      if (computed.error) {
        issues[param.alias] = computed.error;
        return;
      }
      map[param.alias] = computed.value;
      if (alias !== param.alias) map[alias] = computed.value;
    });
    return { map, issues };
  }

  function normalizePaginationParameter(input: Partial<PaginationParameter> | null | undefined, fallbackAlias = 'cursor'): PaginationParameter {
    const aliasRaw = String(input?.alias || '').trim();
    const requestPathRaw = String(input?.requestPath || '').trim();
    const alias = aliasRaw || requestPathRaw || fallbackAlias;
    return {
      id: String(input?.id || uid()),
      alias,
      responsePath: String(input?.responsePath || '').trim(),
      firstValue: String(input?.firstValue || '').trim(),
      requestTarget: toPaginationParameterTarget(String(input?.requestTarget || 'body').trim()),
      requestPath: requestPathRaw,
      applyForAllResponses: true
    };
  }

  function normalizePaginationStopRule(input: Partial<PaginationStopRule> | null | undefined): PaginationStopRule {
    return {
      id: String(input?.id || uid()),
      responsePath: String(input?.responsePath || '').trim(),
      operator: toPaginationStopOperator(String(input?.operator || 'equals').trim()),
      compareValue: String(input?.compareValue || '').trim()
    };
  }

  function paginationParametersForDraft(draft: ApiDraft | null): PaginationParameter[] {
    if (!draft) return [];
    const direct = (Array.isArray(draft.paginationParameters) ? draft.paginationParameters : [])
      .map((p, idx) => normalizePaginationParameter(p, `cursor_${idx + 1}`))
      .filter((p) => p.alias);
    if (direct.length) return direct;
    const legacyTarget: PaginationParameterTarget = draft.paginationTarget === 'query' ? 'query' : 'body';
    const legacy: PaginationParameter[] = [];
    if (draft.paginationCursorResPath1 || draft.paginationCursorReqPath1) {
      legacy.push(
        normalizePaginationParameter(
          {
            id: uid(),
            alias: 'cursor_1',
            responsePath: draft.paginationCursorResPath1,
            firstValue: '',
            requestTarget: legacyTarget,
            requestPath: draft.paginationCursorReqPath1,
            applyForAllResponses: true
          },
          'cursor_1'
        )
      );
    }
    if (draft.paginationCursorResPath2 || draft.paginationCursorReqPath2) {
      legacy.push(
        normalizePaginationParameter(
          {
            id: uid(),
            alias: 'cursor_2',
            responsePath: draft.paginationCursorResPath2,
            firstValue: '',
            requestTarget: legacyTarget,
            requestPath: draft.paginationCursorReqPath2,
            applyForAllResponses: true
          },
          'cursor_2'
        )
      );
    }
    return legacy;
  }

  function paginationStopRulesForDraft(draft: ApiDraft | null): PaginationStopRule[] {
    if (!draft) return [];
    return (Array.isArray(draft.paginationStopRules) ? draft.paginationStopRules : [])
      .map((rule) => normalizePaginationStopRule(rule));
  }

  function bindingAliasOptions(draft: ApiDraft | null) {
    if (!draft) return [];
    const fromDataModel = (Array.isArray(draft.dataFields) ? draft.dataFields : [])
      .map((f) => String(f?.alias || '').trim())
      .filter(Boolean);
    const fromDateParams = dataDateParametersForDraft(draft)
      .map((p) => String(p?.alias || '').trim())
      .filter(Boolean);
    const fromDefinitions = (Array.isArray(draft.parameterDefinitions) ? draft.parameterDefinitions : [])
      .map((p) => String(p?.alias || '').trim())
      .filter(Boolean);
    if (fromDataModel.length || fromDateParams.length) {
      return uniqueAliasList([...fromDataModel, ...fromDateParams]);
    }
    return uniqueAliasList(fromDefinitions);
  }

  function sanitizeAliasReferences(draft: ApiDraft) {
    const options = bindingAliasOptions(draft);
    const optionSet = new Set(options);
    const canonicalByLower = new Map<string, string>();
    options.forEach((alias) => {
      const key = alias.toLowerCase();
      if (!canonicalByLower.has(key)) canonicalByLower.set(key, alias);
    });
    const removed: string[] = [];
    const canonicalAlias = (raw: string) => {
      const trimmed = String(raw || '').trim();
      if (!trimmed) return '';
      if (optionSet.has(trimmed)) return trimmed;
      const canonical = canonicalByLower.get(trimmed.toLowerCase()) || '';
      if (!canonical) {
        removed.push(trimmed);
        return '';
      }
      if (canonical !== trimmed) removed.push(trimmed);
      return canonical;
    };

    const groupByFromFields = (Array.isArray(draft.dataFields) ? draft.dataFields : [])
      .filter((f) => Boolean(f?.grouped))
      .map((f) => canonicalAlias(String(f?.alias || '')))
      .filter(Boolean);
    const groupByFromDates = dataDateParametersForDraft(draft)
      .filter((p) => Boolean(p?.grouped))
      .map((p) => canonicalAlias(String(p?.alias || '')))
      .filter(Boolean);
    const groupByLegacy = (Array.isArray(draft.groupByAliases) ? draft.groupByAliases : [])
      .map(canonicalAlias)
      .filter(Boolean);
    const groupedAuto = uniqueAliasList([...groupByFromFields, ...groupByFromDates]);
    const groupByAliases = uniqueAliasList(groupedAuto.length ? groupedAuto : groupByLegacy);
    const seenRules = new Set<string>();
    const bindingRules = (Array.isArray(draft.bindingRules) ? draft.bindingRules : [])
      .map((rule) => ({
        id: String(rule?.id || uid()),
        alias: canonicalAlias(String(rule?.alias || '')),
        target: toBindingTarget(String(rule?.target || 'body_item')),
        path: String(rule?.path || '').trim()
      }))
      .filter((rule) => rule.alias && rule.path)
      .filter((rule) => {
        const key = `${rule.alias}|${rule.target}|${rule.path}`;
        if (seenRules.has(key)) return false;
        seenRules.add(key);
        return true;
      });

    return {
      groupByAliases,
      bindingRules,
      removedAliases: uniqueAliasList(removed)
    };
  }

  function ensureDataTableColumnsLoaded(tableId: string) {
    if (!selected) return;
    const t = selected.dataTables.find((x) => x.id === tableId);
    if (!t) return;
    ensureColumnsFor(t.schema, t.table);
  }

  function addDataTable() {
    const fallback = existingTables[0];
    const tableId = uid();
    mutateSelected((d) => {
      const idx = (d.dataTables?.length || 0) + 1;
      d.dataTables = [
        ...(Array.isArray(d.dataTables) ? d.dataTables : []),
        {
          id: tableId,
          schema: fallback?.schema_name || '',
          table: fallback?.table_name || '',
          alias: fallback?.table_name || `table_${idx}`
        }
      ];
    });
    activeDataTableId = tableId;
    if (fallback?.schema_name && fallback?.table_name) ensureColumnsFor(fallback.schema_name, fallback.table_name);
  }

  async function updateDataTableRef(tableId: string, value: string) {
    const [schema, table] = String(value || '').split('.');
    mutateSelected((d) => {
      d.dataTables = d.dataTables.map((t) => {
        if (t.id !== tableId) return t;
        return { ...t, schema: schema || '', table: table || '', alias: t.alias || table || '' };
      });
    });
    await ensureColumnsFor(schema || '', table || '');
  }

  function updateDataTableAlias(tableId: string, alias: string) {
    mutateSelected((d) => {
      d.dataTables = d.dataTables.map((t) => (t.id === tableId ? { ...t, alias } : t));
    });
  }

  function removeDataTable(tableId: string) {
    mutateSelected((d) => {
      d.dataTables = d.dataTables.filter((t) => t.id !== tableId);
      d.dataJoins = d.dataJoins.filter((j) => j.leftTableId !== tableId && j.rightTableId !== tableId);
      d.dataFields = d.dataFields.filter((f) => f.tableId !== tableId);
      d.dataFilters = d.dataFilters.filter((f) => f.tableId !== tableId);
    });
    if (activeDataTableId === tableId) activeDataTableId = '';
  }

  function addDataJoin() {
    if (!selected) return;
    const tables = selected.dataTables || [];
    if (!tables.length) {
      err = 'Сначала добавь таблицы для работы.';
      return;
    }
    err = '';
    const left = tables[0]?.id || '';
    const right = tables[1]?.id || tables[0]?.id || '';
    mutateSelected((d) => {
      const joinId = uid();
      d.dataJoins = [
        ...(Array.isArray(d.dataJoins) ? d.dataJoins : []),
        {
          id: joinId,
          leftTableId: left,
          leftField: '',
          rightTableId: right,
          rightField: '',
          joinType: 'inner'
        }
      ];
      activeDataJoinId = joinId;
    });
    if (left) ensureDataTableColumnsLoaded(left);
    if (right) ensureDataTableColumnsLoaded(right);
  }

  function updateDataJoin(joinId: string, patch: Partial<DataModelJoin>) {
    mutateSelected((d) => {
      d.dataJoins = d.dataJoins.map((j) => (j.id === joinId ? { ...j, ...patch } : j));
    });
  }

  function updateDataJoinTableRef(joinId: string, side: 'left' | 'right', tableIdRaw: string) {
    const tableId = String(tableIdRaw || '').trim();
    mutateSelected((d) => {
      d.dataJoins = d.dataJoins.map((j) => {
        if (j.id !== joinId) return j;
        if (side === 'left') return { ...j, leftTableId: tableId, leftField: '' };
        return { ...j, rightTableId: tableId, rightField: '' };
      });
    });
    if (tableId) ensureDataTableColumnsLoaded(tableId);
  }

  function removeDataJoin(joinId: string) {
    mutateSelected((d) => {
      d.dataJoins = d.dataJoins.filter((j) => j.id !== joinId);
    });
    if (activeDataJoinId === joinId) activeDataJoinId = '';
  }

  function addDataFilter() {
    if (!selected) return;
    const tableId = selected.dataTables[0]?.id || FILTER_SCOPE_ALIAS;
    err = '';
    mutateSelected((d) => {
      const filterId = uid();
      d.dataFilters = [
        ...(Array.isArray(d.dataFilters) ? d.dataFilters : []),
        {
          id: filterId,
          tableId,
          field: '',
          operator: 'equals',
          compareValue: ''
        }
      ];
      activeDataFilterId = filterId;
    });
    if (tableId !== FILTER_SCOPE_ALIAS) ensureDataTableColumnsLoaded(tableId);
  }

  function updateDataFilter(filterId: string, patch: Partial<DataModelFilter>) {
    mutateSelected((d) => {
      d.dataFilters = d.dataFilters.map((f) => (f.id === filterId ? { ...f, ...patch } : f));
    });
  }

  function removeDataFilter(filterId: string) {
    mutateSelected((d) => {
      d.dataFilters = d.dataFilters.filter((f) => f.id !== filterId);
    });
    if (activeDataFilterId === filterId) activeDataFilterId = '';
  }

  function updateDataFilterTableRef(filterId: string, value: string) {
    const tableId = String(value || '').trim();
    mutateSelected((d) => {
      d.dataFilters = d.dataFilters.map((f) =>
        f.id === filterId ? { ...f, tableId, field: '', operator: 'equals', compareValue: '' } : f
      );
    });
    if (tableId && tableId !== FILTER_SCOPE_ALIAS) ensureDataTableColumnsLoaded(tableId);
  }

  function addDataField() {
    if (!selected) return;
    const tableId = selected.dataTables[0]?.id || '';
    if (!tableId) {
      err = 'Сначала добавь таблицы для работы.';
      return;
    }
    err = '';
    mutateSelected((d) => {
      const idx = (d.dataFields?.length || 0) + 1;
      d.dataFields = [
        ...(Array.isArray(d.dataFields) ? d.dataFields : []),
        {
          id: uid(),
          tableId,
          field: '',
          alias: `field_${idx}`,
          grouped: false,
          dateMode: 'raw',
          dateAnchorPreset: 'raw',
          dateAddDays: 0,
          dateAddMonths: 0,
          dateFormatPreset: 'yyyy_mm_dd'
        }
      ];
    });
    ensureDataTableColumnsLoaded(tableId);
  }

  function updateDataField(fieldId: string, patch: Partial<DataModelField>) {
    mutateSelected((d) => {
      d.dataFields = d.dataFields.map((f) => (f.id === fieldId ? { ...f, ...patch } : f));
    });
  }

  function removeDataField(fieldId: string) {
    mutateSelected((d) => {
      d.dataFields = d.dataFields.filter((f) => f.id !== fieldId);
    });
  }

  function updateDataFieldTableRef(fieldId: string, tableIdRaw: string) {
    const tableId = String(tableIdRaw || '').trim();
    mutateSelected((d) => {
      d.dataFields = d.dataFields.map((f) => (f.id === fieldId ? { ...f, tableId, field: '' } : f));
    });
    if (tableId) ensureDataTableColumnsLoaded(tableId);
  }

  function moveDataField(fieldId: string, direction: -1 | 1) {
    mutateSelected((d) => {
      const idx = d.dataFields.findIndex((f) => f.id === fieldId);
      if (idx < 0) return;
      const nextIdx = idx + direction;
      if (nextIdx < 0 || nextIdx >= d.dataFields.length) return;
      const items = [...d.dataFields];
      const [item] = items.splice(idx, 1);
      items.splice(nextIdx, 0, item);
      d.dataFields = items;
    });
  }

  function addDataDateParam() {
    mutateSelected((d) => {
      const idx = (Array.isArray(d.dataDateParams) ? d.dataDateParams.length : 0) + 1;
      const next = normalizeDataDateParameter(
        {
          id: uid(),
          alias: `date_${idx}`,
          basePreset: 'today',
          customDate: '',
          anchorPreset: 'raw',
          addDays: 0,
          addMonths: 0,
          formatPreset: 'yyyy_mm_dd',
          grouped: false
        },
        idx
      );
      d.dataDateParams = [...(Array.isArray(d.dataDateParams) ? d.dataDateParams : []), next];
    });
  }

  function updateDataDateParam(paramId: string, patch: Partial<DataDateParameter>) {
    mutateSelected((d) => {
      d.dataDateParams = (Array.isArray(d.dataDateParams) ? d.dataDateParams : []).map((p, idx) =>
        p.id === paramId ? normalizeDataDateParameter({ ...p, ...patch }, idx + 1) : p
      );
    });
  }

  function removeDataDateParam(paramId: string) {
    mutateSelected((d) => {
      d.dataDateParams = (Array.isArray(d.dataDateParams) ? d.dataDateParams : []).filter((p) => p.id !== paramId);
    });
  }

  function dateParamPreviewValue(param: DataDateParameter) {
    const computed = computeDateParameterValue(param);
    return computed.error ? `Ошибка: ${computed.error}` : computed.value;
  }

  function addDataApiParam() {
    if (!selected) return;
    const options = apiSourceDraftOptions(selected);
    const first = options[0];
    const idx = (Array.isArray(selected.dataApiParams) ? selected.dataApiParams.length : 0) + 1;
    const initial = normalizeDataApiParameter(
      {
        id: uid(),
        sourceApiRef: String(first?.ref || ''),
        sourceApiStoreId: Number(first?.storeId || 0) || undefined,
        sourceType: 'system',
        sourceKey: 'api_name',
        alias: `api_${idx}`,
        grouped: false
      },
      idx
    );
    mutateSelected((d) => {
      d.dataApiParams = [...(Array.isArray(d.dataApiParams) ? d.dataApiParams : []), initial];
    });
  }

  function updateDataApiParam(paramId: string, patch: Partial<DataApiParameter>) {
    mutateSelected((d) => {
      d.dataApiParams = (Array.isArray(d.dataApiParams) ? d.dataApiParams : []).map((p, idx) =>
        p.id === paramId ? normalizeDataApiParameter({ ...p, ...patch }, idx + 1) : p
      );
    });
  }

  function removeDataApiParam(paramId: string) {
    mutateSelected((d) => {
      d.dataApiParams = (Array.isArray(d.dataApiParams) ? d.dataApiParams : []).filter((p) => p.id !== paramId);
    });
  }

  function apiParamSourceTypeOptions() {
    return [
      { value: 'system', label: 'Системные поля' },
      { value: 'input', label: 'Входные параметры API' },
      { value: 'output', label: 'Выходные параметры API' }
    ] as Array<{ value: DataApiParameterSourceType; label: string }>;
  }

  function apiParamSourceKeyOptions(param: DataApiParameter): Array<{ value: string; label: string }> {
    const source = sourceDraftByRefOrStoreId(param.sourceApiRef, param.sourceApiStoreId);
    if (!source) return [];
    if (param.sourceType === 'system') {
      return apiSystemFieldOptions().map((x) => ({ value: x.key, label: x.label }));
    }
    if (param.sourceType === 'output') {
      return sourceDraftOutputPaths(source).map((path) => ({ value: path, label: path }));
    }
    return sourceDraftInputAliases(source).map((alias) => ({ value: alias, label: alias }));
  }

  function tableColumnsById(draft: ApiDraft | null, tableId: string) {
    if (!draft) return [];
    const t = draft.dataTables.find((x) => x.id === tableId);
    if (!t) return [];
    return columnOptionsFor(t.schema, t.table);
  }

  function dataFieldColumnOptions(draft: ApiDraft | null, dataField: DataModelField) {
    if (!draft) return [];
    const tableId = String(dataField?.tableId || '').trim();
    const current = String(dataField?.field || '').trim();
    const t = draft.dataTables.find((x) => x.id === tableId);
    if (!t) return current ? [current] : [];
    return tableFieldOptionsFor(t.schema, t.table, current);
  }

  function isDataFieldDateType(draft: ApiDraft | null, field: DataModelField) {
    if (!draft) return false;
    const table = (draft.dataTables || []).find((x) => x.id === field.tableId);
    if (!table || !String(field?.field || '').trim()) return false;
    const type = normalizeTypeName(columnMeta(table.schema, table.table, field.field)?.type || '');
    return type.includes('date') || type.includes('time');
  }

  function tableAliasById(draft: ApiDraft | null, tableId: string) {
    if (!draft) return '';
    const t = draft.dataTables.find((x) => x.id === tableId);
    if (!t) return '';
    return String(t.alias || `${t.schema}.${t.table}`).trim();
  }

  function tableAddressById(draft: ApiDraft | null, tableId: string) {
    if (!draft) return '';
    const t = draft.dataTables.find((x) => x.id === tableId);
    if (!t) return '';
    return `${t.schema}.${t.table}`;
  }

  function joinCrumbLabel(draft: ApiDraft | null, join: DataModelJoin) {
    const leftTbl = tableAliasById(draft, join.leftTableId) || 'таблица';
    const rightTbl = tableAliasById(draft, join.rightTableId) || 'таблица';
    const leftField = String(join.leftField || '?');
    const rightField = String(join.rightField || '?');
    return `${leftTbl}.${leftField} = ${rightTbl}.${rightField}`;
  }

  function filterCrumbLabel(draft: ApiDraft | null, filter: DataModelFilter) {
    if (String(filter?.tableId || '').trim() === FILTER_SCOPE_ALIAS) {
      return `Итог.${String(filter.field || '?')}`;
    }
    const tbl = tableAliasById(draft, filter.tableId) || 'таблица';
    const fld = String(filter.field || '?');
    return `${tbl}.${fld}`;
  }

  function dataFilterFieldKind(filter: DataModelFilter): 'text' | 'number' | 'boolean' | 'date' {
    if (String(filter?.tableId || '').trim() === FILTER_SCOPE_ALIAS) return 'text';
    if (!selected) return 'text';
    const t = (selected.dataTables || []).find((x) => x.id === filter.tableId);
    if (!t || !filter.field) return 'text';
    const type = normalizeTypeName(columnMeta(t.schema, t.table, filter.field)?.type || '');
    if (type.includes('bool')) return 'boolean';
    if (type.includes('date') || type.includes('time')) return 'date';
    if (
      type.includes('int') ||
      type.includes('numeric') ||
      type.includes('double') ||
      type.includes('decimal') ||
      type.includes('real') ||
      type.includes('serial')
    ) {
      return 'number';
    }
    return 'text';
  }

  function dataFilterOperatorsFor(filter: DataModelFilter) {
    return DATA_FILTER_OPERATORS_BY_KIND[dataFilterFieldKind(filter)] || DATA_FILTER_OPERATORS_BY_KIND.text;
  }

  function isBooleanDataFilter(filter: DataModelFilter) {
    return dataFilterFieldKind(filter) === 'boolean';
  }

  function isAliasDataFilter(filter: DataModelFilter) {
    return String(filter?.tableId || '').trim() === FILTER_SCOPE_ALIAS;
  }

  function aliasOptionsForFilters(draft: ApiDraft | null) {
    return bindingAliasOptions(draft);
  }

  function compareFilterValues(operator: string, left: any, rightRaw: string) {
    const op = String(operator || '').trim();
    const leftText = left === null || left === undefined ? '' : String(left);
    const rightText = String(rightRaw || '');
    if (op === 'equals') return leftText === rightText;
    if (op === 'not_equals') return leftText !== rightText;
    if (op === 'contains') return leftText.toLowerCase().includes(rightText.toLowerCase());
    if (op === 'starts_with') return leftText.toLowerCase().startsWith(rightText.toLowerCase());
    if (op === 'ends_with') return leftText.toLowerCase().endsWith(rightText.toLowerCase());
    if (op === 'gt' || op === 'lt') {
      const leftNum = Number(left);
      const rightNum = Number(rightRaw);
      if (Number.isFinite(leftNum) && Number.isFinite(rightNum)) {
        return op === 'gt' ? leftNum > rightNum : leftNum < rightNum;
      }
      const leftDate = parseDateValueFromTable(left);
      const rightDate = parseDateValueFromTable(rightRaw);
      if (leftDate && rightDate) {
        return op === 'gt' ? leftDate.getTime() > rightDate.getTime() : leftDate.getTime() < rightDate.getTime();
      }
      return op === 'gt' ? leftText > rightText : leftText < rightText;
    }
    if (op === 'before' || op === 'after') {
      const leftDate = parseDateValueFromTable(left);
      const rightDate = parseDateValueFromTable(rightRaw);
      if (leftDate && rightDate) {
        return op === 'before' ? leftDate.getTime() < rightDate.getTime() : leftDate.getTime() > rightDate.getTime();
      }
      return op === 'before' ? leftText < rightText : leftText > rightText;
    }
    return true;
  }

  function applyAliasFiltersToRows(rows: Array<Record<string, any>>, filters: DataModelFilter[]) {
    const source = Array.isArray(rows) ? rows : [];
    const activeFilters = (Array.isArray(filters) ? filters : [])
      .filter((f) => isAliasDataFilter(f))
      .filter((f) => String(f.field || '').trim())
      .filter((f) => String(f.operator || '').trim());
    if (!activeFilters.length) return source;
    return source.filter((row) =>
      activeFilters.every((filter) => {
        const found = findAliasInMap(row || {}, filter.field);
        const value = found.found ? found.value : undefined;
        return compareFilterValues(filter.operator, value, String(filter.compareValue || ''));
      })
    );
  }

  async function fetchDataModelRows(
    draft: ApiDraft,
    aliases: string[],
    limit: number,
    offset: number
  ): Promise<{ rows: Array<Record<string, any>>; has_more?: boolean }> {
    const fieldSpecs = (draft.dataFields || []).map((f) => ({
      id: f.id,
      tableId: f.tableId,
      field: f.field,
      alias: f.alias
    }));
    return fetchDataModelRowsByFields(draft, fieldSpecs, aliases, limit, offset);
  }

  async function fetchDataModelRowsByFields(
    draft: ApiDraft,
    fieldSpecs: Array<{ id: string; tableId: string; field: string; alias: string }>,
    aliases: string[],
    limit: number,
    offset: number
  ): Promise<{ rows: Array<Record<string, any>>; has_more?: boolean }> {
    const aliasFilter = new Set(uniqueAliasList(aliases));
    const allTables = (draft.dataTables || []).map((t) => ({
      id: String(t.id || '').trim(),
      schema: String(t.schema || '').trim(),
      table: String(t.table || '').trim(),
      alias: String(t.alias || '').trim()
    }));
    const allJoins = (draft.dataJoins || []).map((j) => ({
      id: String(j.id || '').trim(),
      left_table_id: String(j.leftTableId || '').trim(),
      left_field: String(j.leftField || '').trim(),
      right_table_id: String(j.rightTableId || '').trim(),
      right_field: String(j.rightField || '').trim(),
      join_type: j.joinType
    }));
    const allFilters = (draft.dataFilters || []).map((f) => ({
      id: String(f.id || '').trim(),
      table_id: String(f.tableId || '').trim(),
      field: String(f.field || '').trim(),
      operator: String(f.operator || '').trim(),
      compare_value: String(f.compareValue || '')
    }));
    const localAliasFilters = (draft.dataFilters || []).filter((f) => isAliasDataFilter(f));
    const fields = fieldSpecs
      .map((f) => ({
        id: String(f.id || uid()),
        table_id: String(f.tableId || '').trim(),
        field: String(f.field || '').trim(),
        alias: String(f.alias || '').trim()
      }))
      .filter((f) => f.table_id && f.field && f.alias)
      .filter((f) => (aliasFilter.size ? aliasFilter.has(f.alias) : true));

    // If current selection effectively uses one table, send only that table.
    // This allows keeping extra tables in breadcrumbs without forcing joins.
    const usedTableIds = new Set<string>();
    fields.forEach((f) => {
      if (f.table_id) usedTableIds.add(f.table_id);
    });
    let tablesForRequest = allTables;
    let joinsForRequest = allJoins;
    let filtersForRequest = allFilters.filter((f) => String(f.table_id || '').trim() !== FILTER_SCOPE_ALIAS);
    if (usedTableIds.size <= 1 && usedTableIds.size > 0) {
      tablesForRequest = allTables.filter((t) => usedTableIds.has(t.id));
      joinsForRequest = [];
      filtersForRequest = allFilters.filter((f) => usedTableIds.has(f.table_id));
    }

    const resp = await apiJson<{ rows: Array<Record<string, any>>; has_more?: boolean }>(`${apiBase}/parameter-join-values`, {
      method: 'POST',
      headers: headers(),
      body: JSON.stringify({
        tables: tablesForRequest,
        joins: joinsForRequest,
        fields,
        filters: filtersForRequest,
        aliases,
        limit,
        offset
      })
    });
    let rows = applyDataFieldTransformsToRows(draft, Array.isArray(resp?.rows) ? resp.rows : []);
    rows = applyAliasFiltersToRows(rows, localAliasFilters);
    return {
      ...(resp || {}),
      rows
    };
  }

  function mergeRowsWithCap(
    leftRows: Array<Record<string, any>>,
    rightRows: Array<Record<string, any>>,
    cap = MERGE_ROW_CAP
  ): Array<Record<string, any>> {
    const left = Array.isArray(leftRows) && leftRows.length ? leftRows : [{}];
    const right = Array.isArray(rightRows) && rightRows.length ? rightRows : [{}];
    const out: Array<Record<string, any>> = [];
    for (const l of left) {
      for (const r of right) {
        out.push({ ...(l || {}), ...(r || {}) });
        if (out.length >= cap) {
          throw new Error(`limit_reached: превышен лимит объединения строк (${cap})`);
        }
      }
    }
    return out;
  }

  function joinRowsByKeysWithCap(
    baseRows: Array<Record<string, any>>,
    apiRows: Array<Record<string, any>>,
    keys: string[],
    cap = MERGE_ROW_CAP
  ): Array<Record<string, any>> {
    const out: Array<Record<string, any>> = [];
    const normalizedKeys = uniqueAliasList((keys || []).map((k) => String(k || '').trim()).filter(Boolean));
    if (!normalizedKeys.length) return mergeRowsWithCap(baseRows, apiRows, cap);
    const indexed = new Map<string, Array<Record<string, any>>>();
    for (const row of apiRows || []) {
      const key = JSON.stringify(
        normalizedKeys.map((k) => {
          const found = findAliasInMap(row || {}, k);
          return found.found ? found.value : null;
        })
      );
      const bucket = indexed.get(key) || [];
      bucket.push(row || {});
      indexed.set(key, bucket);
    }
    for (const row of baseRows || []) {
      const key = JSON.stringify(
        normalizedKeys.map((k) => {
          const found = findAliasInMap(row || {}, k);
          return found.found ? found.value : null;
        })
      );
      const matches = indexed.get(key) || [];
      if (!matches.length) continue;
      for (const match of matches) {
        out.push({ ...(row || {}), ...(match || {}) });
        if (out.length >= cap) {
          throw new Error(`limit_reached: превышен лимит объединения строк (${cap})`);
        }
      }
    }
    return out;
  }

  async function loadApiSourceRowsForAliases(
    draft: ApiDraft,
    aliases: string[],
    visited: Set<string> = new Set<string>()
  ): Promise<{ rows: Array<Record<string, any>>; issues: Record<string, string> }> {
    const requested = uniqueAliasList((aliases || []).map((a) => String(a || '').trim()).filter(Boolean));
    const params = dataApiParametersForDraft(draft).filter((p) => String(p.alias || '').trim());
    const issues: Record<string, string> = {};
    if (!requested.length || !params.length) return { rows: [], issues };
    const needed = params.filter((p) => requested.some((a) => a.toLowerCase() === String(p.alias || '').trim().toLowerCase()));
    if (!needed.length) return { rows: [], issues };

    const bySource = new Map<string, DataApiParameter[]>();
    needed.forEach((p) => {
      if (!String(p.sourceKey || '').trim()) {
        issues[p.alias] = 'не выбран параметр источника';
        return;
      }
      const source = sourceDraftByRefOrStoreId(p.sourceApiRef, p.sourceApiStoreId);
      if (!source) {
        issues[p.alias] = 'источник API не найден';
        return;
      }
      const key = refOf(source);
      const list = bySource.get(key) || [];
      list.push(p);
      bySource.set(key, list);
    });
    const sourceRowsList: Array<Array<Record<string, any>>> = [];
    for (const [sourceRef, sourceParams] of bySource.entries()) {
      const source = byRef(sourceRef) || null;
      if (!source) continue;
      if (visited.has(sourceRef)) {
        sourceParams.forEach((p) => {
          if (!issues[p.alias]) issues[p.alias] = 'обнаружен циклический источник API';
        });
        continue;
      }
      const systemParams = sourceParams.filter((p) => p.sourceType === 'system');
      const inputParams = sourceParams.filter((p) => p.sourceType === 'input');
      const outputParams = sourceParams.filter((p) => p.sourceType === 'output');

      const sourceRows: Array<Record<string, any>> = [];
      if (inputParams.length) {
        const needAliases = uniqueAliasList(inputParams.map((p) => p.sourceKey).filter(Boolean));
        const loaded = await loadDataModelRowsForAliases(source, needAliases, new Set<string>([...Array.from(visited), sourceRef]));
        if (Object.keys(loaded.issues || {}).length) {
          inputParams.forEach((p) => {
            const keyIssue = findAliasInMap(loaded.issues, p.sourceKey);
            if (keyIssue.found && !issues[p.alias]) issues[p.alias] = String(keyIssue.value || '');
          });
        }
        (loaded.rows || []).forEach((row) => {
          const out: Record<string, any> = {};
          inputParams.forEach((p) => {
            const found = findAliasInMap(row || {}, p.sourceKey);
            if (found.found) out[p.alias] = found.value;
          });
          sourceRows.push(out);
        });
      }
      if (!sourceRows.length) sourceRows.push({});

      systemParams.forEach((p) => {
        const value = resolveApiSystemValue(source, p.sourceKey);
        sourceRows.forEach((row) => {
          row[p.alias] = value;
        });
      });

      if (outputParams.length) {
        const cached = apiResponseCache[sourceRef];
        outputParams.forEach((p) => {
          const path = String(p.sourceKey || '').trim();
          const value = path ? getByPath(cached, path) : undefined;
          if (value === undefined && !issues[p.alias]) {
            issues[p.alias] = 'для выходного параметра нет значения (запусти источник API и выбери путь)';
          }
          sourceRows.forEach((row) => {
            row[p.alias] = value;
          });
        });
      }
      sourceRowsList.push(sourceRows);
    }

    let merged: Array<Record<string, any>> = [{}];
    sourceRowsList.forEach((rows) => {
      const common = uniqueAliasList(
        Object.keys(merged[0] || {}).filter((k) => Object.prototype.hasOwnProperty.call(rows[0] || {}, k))
      );
      merged = common.length ? joinRowsByKeysWithCap(merged, rows, common) : mergeRowsWithCap(merged, rows);
      if (!merged.length) merged = [{}];
    });

    return { rows: merged.filter((r) => Object.keys(r || {}).length > 0), issues };
  }

  async function loadDataModelRowsForAliases(
    draft: ApiDraft,
    aliases: string[],
    visited: Set<string> = new Set<string>()
  ): Promise<{ rows: Array<Record<string, any>>; issues: Record<string, string> }> {
    const issues: Record<string, string> = {};
    const aliasFilters = (draft.dataFilters || []).filter((f) => isAliasDataFilter(f));
    if (!hasDataModelConfigured(draft)) return { rows: [], issues };
    const aliasesRequested = uniqueAliasList(aliases);
    const dataAliases = uniqueAliasList((draft.dataFields || []).map((f) => String(f.alias || '').trim()).filter(Boolean));
    const dateAliases = uniqueAliasList(dataDateParametersForDraft(draft).map((p) => String(p.alias || '').trim()).filter(Boolean));
    const available = new Set([...dataAliases, ...dateAliases]);
    aliasesRequested.forEach((alias) => {
      if (!available.has(alias)) issues[alias] = 'alias не найден в конструкторе данных';
    });
    if (Object.keys(issues).length) return { rows: [], issues };

    const dateResolved = resolveDateAliasValues(draft, aliasesRequested);
    Object.entries(dateResolved.issues).forEach(([alias, reason]) => {
      if (!issues[alias]) issues[alias] = reason;
    });
    if (Object.keys(issues).length) return { rows: [], issues };
    const dateMap = dateResolved.map;
    const hasDataAlias = aliasesRequested.some((alias) => dataAliases.includes(alias));

    const allRows: Array<Record<string, any>> = [];
    if (hasDataAlias) {
      const limit = 1000;
      let offset = 0;
      while (offset < DATA_MODEL_ROW_HARD_LIMIT) {
        const onlyDataAliases = aliasesRequested.filter((alias) => dataAliases.includes(alias));
        const resp = await fetchDataModelRows(draft, onlyDataAliases, limit, offset);
        const rows = Array.isArray(resp?.rows) ? resp.rows : [];
        if (!rows.length) break;
        const merged = rows.map((row) => ({ ...row, ...dateMap }));
        allRows.push(...merged);
        offset += rows.length;
        if (!resp?.has_more || rows.length < limit) break;
        if (offset >= DATA_MODEL_ROW_HARD_LIMIT) {
          issues.limit_reached = `limit_reached: достигнут лимит строк конструктора данных (${DATA_MODEL_ROW_HARD_LIMIT})`;
          break;
        }
      }
    } else {
      allRows.push({ ...dateMap });
    }
    if (!allRows.length && Object.keys(dateMap).length) {
      allRows.push({ ...dateMap });
    }

    void visited;
    return { rows: applyAliasFiltersToRows(allRows, aliasFilters), issues };
  }

  async function previewDataModelNow() {
    const requestId = ++datasetPreviewRequestSeq;
    datasetPreviewError = '';
    datasetPreviewRows = [];
    datasetPreviewHasMore = false;
    if (!selected) {
      datasetPreviewError = 'Выбери API';
      return;
    }
    const dateParams = dataDateParametersForDraft(selected);
    if (!selected.dataTables?.length && !dateParams.length) {
      datasetPreviewColumns = [];
      datasetPreviewError = 'Добавь источник данных: таблицы или даты.';
      return;
    }
    const previewCols = (selected.dataFields || [])
      .filter((f) => String(f?.tableId || '').trim() && String(f?.field || '').trim() && String(f?.alias || '').trim())
      .map((f) => ({ id: f.id, tableId: f.tableId, field: f.field, alias: f.alias }));
    const dateAliases = uniqueAliasList(dateParams.map((p) => String(p.alias || '').trim()).filter(Boolean));
    if (!previewCols.length && !dateAliases.length) {
      datasetPreviewColumns = [];
      datasetPreviewError = 'Добавь хотя бы один показатель для работы.';
      return;
    }
    datasetPreviewLoading = true;
    try {
      const aliases = uniqueAliasList([
        ...previewCols.map((f) => String(f?.alias || '').trim()).filter(Boolean),
        ...dateAliases
      ]);
      if (requestId !== datasetPreviewRequestSeq) return;
      datasetPreviewColumns = aliases;
      const loaded = await loadDataModelRowsForAliases(selected, aliases);
      if (Object.keys(loaded.issues || {}).length) {
        datasetPreviewError = Object.entries(loaded.issues || {})
          .map(([k, v]) => `${k}: ${v}`)
          .join('; ');
      }
      const rows = Array.isArray(loaded?.rows) ? loaded.rows : [];
      datasetPreviewRows = rows.slice(0, 10);
      datasetPreviewHasMore = rows.length > 10;
    } catch (e: any) {
      if (requestId !== datasetPreviewRequestSeq) return;
      datasetPreviewError = e?.message ?? String(e);
    } finally {
      if (requestId === datasetPreviewRequestSeq) datasetPreviewLoading = false;
    }
  }

  function dataModelPreviewSignature(draft: ApiDraft | null) {
    if (!draft) return '';
    const tables = (draft.dataTables || []).map((t) => ({
      id: t.id,
      schema: t.schema,
      table: t.table,
      alias: t.alias
    }));
    const fields = (draft.dataFields || []).map((f) => ({
      id: f.id,
      tableId: f.tableId,
      field: f.field,
      alias: f.alias,
      grouped: Boolean(f.grouped),
      dateMode: f.dateMode || 'raw',
      dateAnchorPreset: f.dateAnchorPreset || 'raw',
      dateAddDays: Number(f.dateAddDays || 0),
      dateAddMonths: Number(f.dateAddMonths || 0),
      dateFormatPreset: f.dateFormatPreset || 'yyyy_mm_dd'
    }));
    const joins = (draft.dataJoins || []).map((j) => ({
      id: j.id,
      leftTableId: j.leftTableId,
      leftField: j.leftField,
      rightTableId: j.rightTableId,
      rightField: j.rightField,
      joinType: j.joinType
    }));
    const filters = (draft.dataFilters || []).map((f) => ({
      id: f.id,
      tableId: f.tableId,
      field: f.field,
      operator: f.operator,
      compareValue: f.compareValue
    }));
    const dateParams = dataDateParametersForDraft(draft).map((p) => ({
      id: p.id,
      alias: p.alias,
      basePreset: p.basePreset,
      customDate: p.customDate,
      anchorPreset: p.anchorPreset,
      addDays: p.addDays,
      addMonths: p.addMonths,
      formatPreset: p.formatPreset,
      grouped: Boolean(p.grouped)
    }));
    const apiParams = dataApiParametersForDraft(draft).map((p) => ({
      id: p.id,
      sourceApiRef: p.sourceApiRef,
      sourceApiStoreId: Number(p.sourceApiStoreId || 0) || 0,
      sourceType: p.sourceType,
      sourceKey: p.sourceKey,
      alias: p.alias,
      grouped: Boolean(p.grouped)
    }));
    return JSON.stringify({ tables, fields, joins, filters, dateParams, apiParams });
  }

  function serializeParameterDefinition(param: ParameterDefinition) {
    const sourceParts = [param.sourceSchema, param.sourceTable, param.sourceField].filter(Boolean);
    const conditions = param.conditions.map((cond) => {
      const columnParts = [cond.schema, cond.table, cond.field].filter(Boolean);
      return {
        column: columnParts.join('.'),
        operator: cond.operator,
        compare_mode: cond.compareMode,
        compare: cond.compareMode === 'column' ? cond.compareColumn : cond.compareValue
      };
    });
    return JSON.stringify(
      {
        alias: param.alias,
        source: sourceParts.join('.'),
        definition: param.definition,
        conditions
      },
      null,
      2
    );
  }

  function normalizeConditionEntry(entry: any) {
    const rawCompareMode = String(entry?.compare_mode || entry?.compareMode || '').toLowerCase();
    const compareMode: 'column' | 'value' = rawCompareMode === 'column' ? 'column' : 'value';
    const column = String(entry?.column || entry?.source_column || entry?.compare_column || entry?.compareColumn || '').trim();
    const [schema, table, field] = column.split('.');
    return {
      id: String(entry?.id || uid()),
      schema: schema || '',
      table: table || '',
      field: field || '',
      operator: String(entry?.operator || 'equals'),
      compareMode,
      compareValue: compareMode === 'value' ? String(entry?.compare || entry?.value || entry?.compare_value || '') : '',
      compareColumn: compareMode === 'column' ? String(entry?.compare || entry?.compare_column || entry?.compareColumn || '') : ''
    };
  }

  function applyDefinitionFromJson(payload: any, paramId: string) {
    if (!selected) return;
    const param = selected.parameterDefinitions.find((p) => p.id === paramId);
    if (!param) return;
    const alias = String(payload?.alias || param.alias);
    const definitionText = String(payload?.definition || param.definition);
    const sourceRaw = String(payload?.source || `${param.sourceSchema}.${param.sourceTable}.${param.sourceField}`);
    const [sourceSchema, sourceTable, sourceField] = sourceRaw.split('.');
    const conditionsRaw = Array.isArray(payload?.conditions) ? payload.conditions : [];
    const parsedConditions = conditionsRaw
      .map((entry) => normalizeConditionEntry(entry))
      .filter((cond) => cond.schema && cond.table && cond.field && cond.operator);
    parsedConditions.forEach((cond) => {
      ensureColumnsFor(cond.schema, cond.table);
      if (cond.compareMode === 'column' && cond.compareColumn) {
        const [cmpSchema, cmpTable] = cond.compareColumn.split('.');
        ensureColumnsFor(cmpSchema, cmpTable);
      }
    });
    ensureColumnsFor(sourceSchema, sourceTable);
    updateParameterDefinition(paramId, {
      alias,
      definition: definitionText,
      sourceSchema: sourceSchema || param.sourceSchema,
      sourceTable: sourceTable || param.sourceTable,
      sourceField: sourceField || param.sourceField,
      conditions: parsedConditions.length ? parsedConditions : param.conditions
    });
  }

function handleDefinitionInput(value: string) {
    definitionDraft = value;
    definitionError = '';
    definitionDirty = true;
    const trimmed = value.trim();
    definitionTree = null;
    definitionViewMode = 'text';
    if (!trimmed || !activeParameter) return;
    try {
      const parsed = JSON.parse(trimmed);
      applyDefinitionFromJson(parsed, activeParameter.id);
      definitionDirty = false;
      definitionTree = parsed;
    } catch {
      definitionError = 'Некорректный JSON. Проверь кавычки и скобки.';
    }
  }

  $: if (definitionTree) {
    definitionTreeDisplay = {
      ...definitionTree,
      alias: activeParameter?.alias || definitionTree.alias
    };
  } else {
    definitionTreeDisplay = null;
  }

  function clearParameterPreview() {
    parameterPreviewRows = [];
    parameterPreviewError = '';
    parameterPreviewLoading = false;
  }

  function ensureDefinitionTree(): boolean {
    if (definitionTree) return true;
    const src = String(definitionDraft || '').trim();
    if (!src) return false;
    try {
      definitionTree = JSON.parse(src);
      return true;
    } catch {
      definitionError = 'Некорректный JSON. Проверь кавычки и скобки.';
      return false;
    }
  }

  function toggleDefinitionViewMode() {
    if (definitionViewMode === 'tree') {
      definitionViewMode = 'text';
      tick().then(syncParameterEditorsHeight);
      return;
    }
    if (ensureDefinitionTree()) {
      definitionViewMode = 'tree';
    }
  }

  $: definitionTreeDisplay =
    definitionTree && activeParameter
      ? { ...definitionTree, alias: activeParameter.alias || definitionTree.alias }
      : definitionTree;

  function getParameterPreviewTarget(param: ParameterDefinition | null) {
    const schema = String(param?.sourceSchema || '').trim();
    const table = String(param?.sourceTable || '').trim();
    return schema && table ? { schema, table } : null;
  }

  function formatParameterRowValue(row: Record<string, any>, field?: string) {
    const value = field ? row[field] : row;
    if (value === undefined || value === null) return '—';
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
  }

  function findAliasInMap<T = any>(map: Record<string, T>, rawAlias: string): { found: boolean; key: string; value?: T } {
    const alias = String(rawAlias || '').trim();
    if (!alias || !map || typeof map !== 'object') return { found: false, key: '' };
    if (Object.prototype.hasOwnProperty.call(map, alias)) {
      return { found: true, key: alias, value: map[alias] };
    }
    const lower = alias.toLowerCase();
    for (const key of Object.keys(map)) {
      if (key.toLowerCase() === lower) {
        return { found: true, key, value: map[key] };
      }
    }
    return { found: false, key: '' };
  }

  function replaceParameterTokens(str: string, map: Record<string, any>) {
    if (!str || !map || !Object.keys(map).length) return str;
    return str.replace(PARAMETER_TOKEN_RE, (match, rawAlias) => {
      const alias = String(rawAlias || '').trim();
      const resolved = findAliasInMap(map, alias);
      if (!alias || !resolved.found) return match;
      const value = resolved.value;
      if (value === undefined || value === null) return match;
      if (typeof value === 'string') return value;
      if (typeof value === 'object') return JSON.stringify(value);
      return String(value);
    });
  }

  function applyParametersToValue(value: any, map: Record<string, any>) {
    if (!map || !Object.keys(map).length) return value;
    if (typeof value === 'string') {
      const exact = value.match(PARAMETER_TOKEN_EXACT_RE);
      if (exact?.[1]) {
        const alias = String(exact[1] || '').trim();
        const resolved = findAliasInMap(map, alias);
        if (alias && resolved.found) {
          const raw = resolved.value;
          if (raw !== undefined && raw !== null) return raw;
        }
      }
      return replaceParameterTokens(value, map);
    }
    if (Array.isArray(value)) return value.map((item) => applyParametersToValue(item, map));
    if (value && typeof value === 'object') {
      Object.entries(value).forEach(([key, val]) => {
        value[key] = applyParametersToValue(val, map);
      });
      return value;
    }
    return value;
  }

  function collectParameterTokens(value: any, out: Set<string>) {
    if (typeof value === 'string') {
      const matches = value.matchAll(new RegExp(PARAMETER_TOKEN_RE.source, 'g'));
      for (const m of matches) {
        const alias = String(m?.[1] || '').trim();
        if (alias) out.add(alias);
      }
      return;
    }
    if (Array.isArray(value)) {
      value.forEach((item) => collectParameterTokens(item, out));
      return;
    }
    if (value && typeof value === 'object') {
      Object.values(value).forEach((item) => collectParameterTokens(item, out));
    }
  }

  function isExactTemplateAlias(value: any, aliasesLower: Set<string>) {
    if (typeof value !== 'string' || !aliasesLower.size) return false;
    const exact = value.match(PARAMETER_TOKEN_EXACT_RE);
    if (!exact?.[1]) return false;
    return aliasesLower.has(String(exact[1] || '').trim().toLowerCase());
  }

  function stripUnresolvedPaginationTokens(value: any, aliasesLower: Set<string>) {
    if (!value || !aliasesLower.size) return;
    if (Array.isArray(value)) {
      for (let i = value.length - 1; i >= 0; i--) {
        const item = value[i];
        if (isExactTemplateAlias(item, aliasesLower)) {
          value.splice(i, 1);
          continue;
        }
        stripUnresolvedPaginationTokens(item, aliasesLower);
      }
      return;
    }
    if (typeof value !== 'object') return;
    Object.keys(value).forEach((key) => {
      const val = value[key];
      if (isExactTemplateAlias(val, aliasesLower)) {
        delete value[key];
        return;
      }
      stripUnresolvedPaginationTokens(val, aliasesLower);
    });
  }

  function parseParameterSourceRef(raw: string, param: ParameterDefinition): { schema: string; table: string; field: string } | null {
    const src = String(raw || '').trim().replace(/^['"]|['"]$/g, '');
    if (!src) return null;
    const parts = src.split('.').map((x) => String(x || '').trim()).filter(Boolean);
    if (parts.length >= 3) {
      const [schema, table, ...fieldParts] = parts;
      const field = fieldParts.join('.');
      return schema && table && field ? { schema, table, field } : null;
    }
    if (parts.length === 2) {
      const [table, field] = parts;
      const schema =
        String(param?.sourceSchema || '').trim() ||
        existingTables.find((t) => String(t.table_name || '').trim() === table)?.schema_name ||
        existingTables[0]?.schema_name ||
        '';
      return schema && table && field ? { schema, table, field } : null;
    }
    if (parts.length === 1) {
      const field = parts[0];
      const table = String(param?.sourceTable || '').trim();
      const schema =
        String(param?.sourceSchema || '').trim() ||
        (table ? existingTables.find((t) => String(t.table_name || '').trim() === table)?.schema_name || '' : '') ||
        existingTables[0]?.schema_name ||
        '';
      return schema && table && field ? { schema, table, field } : null;
    }
    return null;
  }

  function sourceFromDefinition(param: ParameterDefinition): { schema: string; table: string; field: string } | null {
    const definition = String(param?.definition || '').trim();
    if (!definition) return null;

    try {
      const parsed = JSON.parse(definition);
      if (parsed && typeof parsed === 'object') {
        const source = String((parsed as any).source || '').trim();
        if (source) {
          const fromSource = parseParameterSourceRef(source, param);
          if (fromSource) return fromSource;
        }
        const schema = String((parsed as any).source_schema || (parsed as any).schema || '').trim();
        const table = String((parsed as any).source_table || (parsed as any).table || '').trim();
        const field = String((parsed as any).source_field || (parsed as any).field || '').trim();
        if (schema && table && field) return { schema, table, field };
      }
    } catch {
      // ignore, then try text formats
    }

    const fieldFn = definition.match(/FIELD\(\s*['"]([^'"]+)['"]\s*\)/i);
    if (fieldFn?.[1]) {
      const fromField = parseParameterSourceRef(fieldFn[1], param);
      if (fromField) return fromField;
    }

    if (/^[A-Za-z0-9_.-]+$/.test(definition)) {
      const fromRaw = parseParameterSourceRef(definition, param);
      if (fromRaw) return fromRaw;
    }
    return null;
  }

  function resolveParameterSource(param: ParameterDefinition | null): { schema: string; table: string; field: string } | null {
    if (!param) return null;
    const target = getParameterPreviewTarget(param);
    const field = String(param?.sourceField || '').trim();
    if (target && field) {
      return { schema: target.schema, table: target.table, field };
    }
    return sourceFromDefinition(param);
  }

  async function fetchParameterValue(param: ParameterDefinition | null, source?: { schema: string; table: string; field: string }) {
    if (!param) return null;
    const src = source || resolveParameterSource(param);
    if (!src) return null;
    const query = new URLSearchParams();
    query.set('schema', src.schema);
    query.set('table', src.table);
    query.set('field', src.field);
    if (Array.isArray(param.conditions) && param.conditions.length) {
      query.set('conditions', JSON.stringify(param.conditions));
    }
    query.set('limit', '1');
    const response = await apiJson<{ value: any }>(`${apiBase}/parameter-value?${query.toString()}`, { headers: headers() });
    return response?.value ?? null;
  }

  async function resolveParameterValues(definitions: ParameterDefinition[]) {
    const map: Record<string, any> = {};
    const issues: Record<string, string> = {};
    if (!Array.isArray(definitions)) return { map, issues };
    for (const param of definitions) {
      const alias = String(param.alias || '').trim();
      if (!alias) continue;
      try {
        const source = resolveParameterSource(param);
        if (!source) {
          issues[alias] = 'не задан источник (schema.table.field) или FIELD(...) в описании';
          continue;
        }
        const value = await fetchParameterValue(param, source);
        if (value !== undefined && value !== null) {
          map[alias] = value;
        } else {
          issues[alias] = `источник ${source.schema}.${source.table}.${source.field} вернул пустое значение`;
        }
      } catch (error) {
        const msg = error instanceof Error ? error.message : String(error);
        issues[alias] = msg || 'ошибка вычисления';
      }
    }
    return { map, issues };
  }

  async function resolveRuntimeAliasValues(draft: ApiDraft, requestedAliases: string[]) {
    const aliases = uniqueAliasList(requestedAliases);
    if (!aliases.length) return { map: {}, issues: {} as Record<string, string> };

    const issues: Record<string, string> = {};
    const map: Record<string, any> = {};

    const dateResolved = resolveDateAliasValues(draft, aliases);
    Object.entries(dateResolved.map).forEach(([k, v]) => {
      map[k] = v;
    });
    Object.entries(dateResolved.issues).forEach(([k, v]) => {
      if (!issues[k]) issues[k] = v;
    });

    const dataAliases = uniqueAliasList([
      ...(draft.dataFields || []).map((f) => String(f?.alias || '').trim()).filter(Boolean)
    ]);
    const dataAliasByLower = new Map<string, string>();
    dataAliases.forEach((alias) => {
      const key = alias.toLowerCase();
      if (!dataAliasByLower.has(key)) dataAliasByLower.set(key, alias);
    });

    if (hasDataModelConfigured(draft)) {
      const needFromData = uniqueAliasList(
        aliases
          .map((alias) => dataAliasByLower.get(alias.toLowerCase()) || '')
          .filter(Boolean)
      );
      if (needFromData.length) {
        try {
          const loaded = await loadDataModelRowsForAliases(draft, needFromData);
          const rows = Array.isArray(loaded?.rows) ? loaded.rows : [];
          Object.entries(loaded?.issues || {}).forEach(([k, v]) => {
            if (!issues[k]) issues[k] = String(v || '');
          });
          if (rows.length) {
            needFromData.forEach((alias) => {
              const chosen = pickPreferredAliasValue(rows.map((row: Record<string, any>) => row?.[alias]));
              const value = chosen.value;
              if (value !== undefined && value !== null) {
                map[alias] = value;
                aliases
                  .filter((raw) => raw.toLowerCase() === alias.toLowerCase())
                  .forEach((raw) => {
                    map[raw] = value;
                  });
                if (chosen.exp && chosen.exp <= Math.floor(Date.now() / 1000) && !issues[alias]) {
                  issues[alias] = 'выбран JWT-токен с истёкшим сроком действия';
                }
              } else if (!issues[alias]) {
                issues[alias] = 'конструктор данных вернул пустое значение';
              }
            });
          } else {
            needFromData.forEach((alias) => {
              if (!issues[alias]) issues[alias] = 'конструктор данных вернул 0 строк';
            });
          }
        } catch (error) {
          const msg = error instanceof Error ? error.message : String(error);
          needFromData.forEach((alias) => {
            if (!issues[alias]) issues[alias] = `ошибка чтения из конструктора данных: ${msg}`;
          });
        }
      }
    }

    const missing = aliases.filter((alias) => !findAliasInMap(map, alias).found);
    if (!missing.length) return { map, issues };

    const definitions = Array.isArray(draft.parameterDefinitions) ? draft.parameterDefinitions : [];
    const missingLower = new Set(missing.map((a) => a.toLowerCase()));
    const matchedDefinitions = definitions.filter((param) => missingLower.has(String(param?.alias || '').trim().toLowerCase()));
    const { map: defMap, issues: defIssues } = await resolveParameterValues(matchedDefinitions);
    Object.entries(defIssues).forEach(([k, v]) => {
      if (!issues[k]) issues[k] = v;
    });
    missing.forEach((alias) => {
      const found = findAliasInMap(defMap, alias);
      if (found.found && found.value !== undefined && found.value !== null) {
        map[alias] = found.value;
      }
    });

    return { map, issues };
  }

  function extractJwtExp(value: any): number | null {
    const raw = String(value || '').trim();
    if (!raw) return null;
    const parts = raw.split('.');
    if (parts.length < 2) return null;
    try {
      const normalized = parts[1].replace(/-/g, '+').replace(/_/g, '/');
      const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, '=');
      const decoded = atob(padded);
      const payload = JSON.parse(decoded);
      const exp = Number(payload?.exp || 0);
      return Number.isFinite(exp) && exp > 0 ? exp : null;
    } catch {
      return null;
    }
  }

  function pickPreferredAliasValue(values: any[]) {
    const candidates = values.filter((v) => v !== undefined && v !== null);
    if (!candidates.length) return { value: undefined as any, exp: null as number | null };
    const jwtCandidates = candidates
      .map((v) => ({ value: v, exp: extractJwtExp(v) }))
      .filter((x) => x.exp !== null) as Array<{ value: any; exp: number }>;
    if (jwtCandidates.length) {
      jwtCandidates.sort((a, b) => b.exp - a.exp);
      const nowSec = Math.floor(Date.now() / 1000);
      const active = jwtCandidates.find((x) => x.exp > nowSec);
      if (active) return { value: active.value, exp: active.exp };
      return { value: jwtCandidates[0].value, exp: jwtCandidates[0].exp };
    }
    return { value: candidates[0], exp: null };
  }

  function toUiErrorMessage(error: any) {
    const msg = error?.message ?? String(error);
    if (/failed to fetch/i.test(String(msg))) {
      return 'Не удалось выполнить запрос из браузера (CORS/сеть). Предпросмотр корректный, но отправка из UI-браузера заблокирована.';
    }
    return msg;
  }

  function requestPreviewModeLabel(mode: string) {
    if (mode === 'preview_before_send') return 'До отправки (предпросмотр)';
    if (mode === 'sent_grouped_requests') return 'Отправлено с группировкой';
    if (mode === 'sent_row_requests') return 'Отправлено по строкам';
    if (!mode) return 'Обычный запрос';
    return mode;
  }

  function applyBindingRulesToRequest(
    rules: BindingRule[],
    values: Record<string, any>,
    authHdr: Record<string, any>,
    hdr: Record<string, any>,
    queryObj: Record<string, any>,
    bodyObj: any
  ) {
    for (const rule of Array.isArray(rules) ? rules : []) {
      const aliasValue = findAliasInMap(values, rule.alias);
      if (!aliasValue.found || aliasValue.value === undefined || aliasValue.value === null) continue;
      const value = aliasValue.value;
      if (rule.target === 'header') {
        if (String(rule.path || '').trim() === 'Authorization') {
          const source = authHdr.Authorization;
          if (typeof source === 'string' && source.includes('{{')) {
            authHdr.Authorization = applyParametersToValue(source, { [rule.alias]: value });
          } else {
            authHdr.Authorization = value;
          }
        } else {
          hdr[rule.path] = value;
        }
      } else if (rule.target === 'query') {
        setByPath(queryObj, rule.path, value);
      } else if (rule.target === 'body' || rule.target === 'body_item') {
        setByPath(bodyObj, rule.path, value);
      }
    }
  }

  function deepClone<T>(value: T): T {
    return JSON.parse(JSON.stringify(value));
  }

  function stripInternalPlanFields(plan: any) {
    if (!plan || typeof plan !== 'object') return plan;
    const copy = { ...plan };
    delete (copy as any).context_values;
    return copy;
  }

  function sameValue(a: any, b: any) {
    if (a === b) return true;
    return JSON.stringify(a) === JSON.stringify(b);
  }

  function uniqueAliasList(values: string[]) {
    return [...new Set(values.map((x) => String(x || '').trim()).filter(Boolean))];
  }

  function paginationAliasLowerSet(draft: ApiDraft | null) {
    return new Set(
      paginationParametersForDraft(draft)
        .map((p) => String(p?.alias || '').trim().toLowerCase())
        .filter(Boolean)
    );
  }

  function excludePaginationAliases(values: string[], paginationAliasesLower: Set<string>) {
    return uniqueAliasList(
      values.filter((alias) => !paginationAliasesLower.has(String(alias || '').trim().toLowerCase()))
    );
  }

  function oauth2RequestAliases(draft: ApiDraft | null) {
    if (!draft || draft.authMode !== AUTH_MODE_OAUTH2) return [];
    const tokens = new Set<string>();
    collectParameterTokens(String(draft.oauth2RequestUrl || ''), tokens);
    collectParameterTokens(String(draft.oauth2RequestHost || ''), tokens);
    collectParameterTokens(String(draft.oauth2RequestPath || ''), tokens);
    collectParameterTokens(String(draft.oauth2RequestHeadersJson || ''), tokens);
    collectParameterTokens(String(draft.oauth2RequestQueryJson || ''), tokens);
    collectParameterTokens(String(draft.oauth2RequestBodyJson || ''), tokens);
    return uniqueAliasList(Array.from(tokens));
  }

  async function loadParameterRowsForAliases(
    definitions: ParameterDefinition[],
    aliases: string[]
  ): Promise<{ rows: Array<Record<string, any>>; issues: Record<string, string> }> {
    const issues: Record<string, string> = {};
    const requiredAliases = uniqueAliasList(aliases);
    if (!requiredAliases.length) return { rows: [], issues };

    const aliasSpecs: Array<{
      alias: string;
      source: { schema: string; table: string; field: string };
      conditions: ParameterCondition[];
    }> = [];
    for (const alias of requiredAliases) {
      const param = definitions.find((p) => String(p.alias || '').trim() === alias);
      if (!param) {
        issues[alias] = 'параметр не найден в витрине параметров';
        continue;
      }
      const source = resolveParameterSource(param);
      if (!source) {
        issues[alias] = 'не задан источник для параметра';
        continue;
      }
      aliasSpecs.push({ alias, source, conditions: Array.isArray(param.conditions) ? param.conditions : [] });
    }
    if (!aliasSpecs.length) return { rows: [], issues };

    const sourceKey = `${aliasSpecs[0].source.schema}.${aliasSpecs[0].source.table}`;
    const sourceMismatch = aliasSpecs.find(
      (spec) => `${spec.source.schema}.${spec.source.table}` !== sourceKey
    );
    if (sourceMismatch) {
      issues[sourceMismatch.alias] = 'для групповой отправки все параметры должны быть из одной таблицы-источника';
      return { rows: [], issues };
    }

    const conditionSignatures = [...new Set(aliasSpecs.map((spec) => JSON.stringify(spec.conditions || [])))];
    if (conditionSignatures.length > 1) {
      aliasSpecs.forEach((spec) => {
        if (spec.conditions?.length) {
          issues[spec.alias] = 'условия параметров должны совпадать для групповой отправки';
        }
      });
      return { rows: [], issues };
    }
    const sharedConditions = conditionSignatures[0] && conditionSignatures[0] !== '[]'
      ? conditionSignatures[0]
      : '';

    const fields = uniqueAliasList(aliasSpecs.map((spec) => spec.source.field));
    const limit = 1000;
    let offset = 0;
    const allRows: Array<Record<string, any>> = [];
    while (offset < PARAMETER_ROW_HARD_LIMIT) {
      const query = new URLSearchParams();
      query.set('schema', aliasSpecs[0].source.schema);
      query.set('table', aliasSpecs[0].source.table);
      query.set('fields', fields.join(','));
      query.set('limit', String(limit));
      query.set('offset', String(offset));
      if (sharedConditions) query.set('conditions', sharedConditions);
      const resp = await apiJson<{ rows: Array<Record<string, any>>; has_more?: boolean }>(
        `${apiBase}/parameter-values?${query.toString()}`,
        { headers: headers() }
      );
      const rows = Array.isArray(resp?.rows) ? resp.rows : [];
      if (!rows.length) break;
      rows.forEach((row) => {
        const mapped: Record<string, any> = {};
        aliasSpecs.forEach((spec) => {
          mapped[spec.alias] = row?.[spec.source.field];
        });
        allRows.push(mapped);
      });
      offset += rows.length;
      if (!resp?.has_more || rows.length < limit) break;
      if (offset >= PARAMETER_ROW_HARD_LIMIT) {
        issues.limit_reached = `limit_reached: достигнут лимит строк витрины параметров (${PARAMETER_ROW_HARD_LIMIT})`;
        break;
      }
    }
    return { rows: allRows, issues };
  }

  async function buildGroupedRequestPlan(draft: ApiDraft) {
    const issues: Record<string, string> = {};
    const sanitizedAliases = sanitizeAliasReferences(draft);
    const paginationAliasesLower = paginationAliasLowerSet(draft);
    const authTemplate = parseJsonObjectField('Авторизация', draft.authJson);
    const headersTemplate = parseJsonObjectField('Headers JSON', draft.headersJson);
    const queryTemplate = parseJsonObjectField('Query JSON', draft.queryJson);
    const bodyTemplateRaw = parseJsonAnyField('Body JSON', draft.bodyJson);
    const bodyTemplate = bodyTemplateRaw && typeof bodyTemplateRaw === 'object' ? bodyTemplateRaw : {};
    const urlTemplate = `${draft.baseUrl.replace(/\/$/, '')}${draft.path.startsWith('/') ? draft.path : `/${draft.path}`}`;
    const templateTokenSet = new Set<string>();
    collectParameterTokens(urlTemplate, templateTokenSet);
    collectParameterTokens(authTemplate, templateTokenSet);
    collectParameterTokens(headersTemplate, templateTokenSet);
    collectParameterTokens(queryTemplate, templateTokenSet);
    collectParameterTokens(bodyTemplate, templateTokenSet);
    const templateAliases = excludePaginationAliases(Array.from(templateTokenSet), paginationAliasesLower);

    const bindingRules = (Array.isArray(sanitizedAliases.bindingRules) ? sanitizedAliases.bindingRules : [])
      .map((rule) => ({
        id: String(rule?.id || uid()),
        alias: String(rule?.alias || '').trim(),
        target: toBindingTarget(String(rule?.target || 'body_item')),
        path: String(rule?.path || '').trim()
      }))
      .filter((rule) => rule.alias && rule.path);
    const groupByAliases = uniqueAliasList(sanitizedAliases.groupByAliases || []);
    if (!groupByAliases.length) {
      throw new Error(
        'Не указан ключ группировки. Включи кнопку «Группировать» у нужного параметра (например, TKN/кабинет/клиент), чтобы избежать бизнес-ошибок.'
      );
    }
    if (sanitizedAliases.removedAliases.length) {
      issues.removed_aliases = `удалены неактуальные алиасы: ${sanitizedAliases.removedAliases.join(', ')}`;
    }

    const requiredAliases = uniqueAliasList([
      ...groupByAliases,
      ...bindingRules.map((rule) => rule.alias),
      ...templateAliases,
      ...oauth2RequestAliases(draft)
    ]);
    const oauth2ProvidedAliasesLower = new Set(
      (draft.authMode === AUTH_MODE_OAUTH2 ? oauth2MappingsForDraft(draft) : [])
        .map((m) => String(m.alias || '').trim().toLowerCase())
        .filter(Boolean)
    );
    const requiredAliasesFiltered = requiredAliases.filter(
      (alias) => !oauth2ProvidedAliasesLower.has(String(alias || '').trim().toLowerCase())
    );
    const dataset = hasDataModelConfigured(draft)
      ? await loadDataModelRowsForAliases(draft, requiredAliasesFiltered)
      : await loadParameterRowsForAliases(draft.parameterDefinitions, requiredAliasesFiltered);
    Object.assign(issues, dataset.issues);
    if (!dataset.rows.length) {
      const issueEntries = Object.entries(issues);
      if (issueEntries.length) {
        const details = issueEntries
          .slice(0, 5)
          .map(([k, v]) => `${k}: ${v}`)
          .join('; ');
        throw new Error(`Нет строк для групповой отправки. Причины: ${details}`);
      }
      if (hasDataModelConfigured(draft)) {
        throw new Error(
          `Конструктор данных вернул 0 строк. Проверь Параметры/Связи/Фильтры и "Предпросмотр данных". Нужные поля: ${requiredAliases.join(', ')}`
        );
      }
      throw new Error(
        `Витрина параметров вернула 0 строк. Для группировки параметры должны быть из одной таблицы и с совместимыми условиями. Нужные поля: ${requiredAliases.join(', ')}`
      );
    }

    const grouped = groupRowsByAliases(dataset.rows, groupByAliases) as Map<
      string,
      { key: Record<string, any>; rows: Array<Record<string, any>> }
    >;

    const plans: Array<any> = [];
    const groupedKnownAliasesLower = new Set([
      ...bindingAliasOptions(draft).map((x) => x.toLowerCase()),
      ...Array.from(oauth2ProvidedAliasesLower),
      ...Array.from(paginationAliasesLower)
    ]);
    for (const group of grouped.values()) {
      const authHdr = parseJsonObjectField('Авторизация', draft.authJson);
      const hdr = parseJsonObjectField('Headers JSON', draft.headersJson);
      const queryObj = parseJsonObjectField('Query JSON', draft.queryJson);
      const bodyBaseRaw = parseJsonAnyField('Body JSON', draft.bodyJson);
      const bodyObj = bodyBaseRaw && typeof bodyBaseRaw === 'object' ? deepClone(bodyBaseRaw) : {};
      const groupFirstMap = (group.rows[0] || {}) as Record<string, any>;

      let oauthResolvedAliases: Record<string, any> = {};
      let oauthRequestPreview: any = null;
      let oauthResponsePreview: any = null;
      if (draft.authMode === 'oauth2_client_credentials') {
        const tokenData = await getOAuthToken(draft, groupFirstMap);
        oauthResolvedAliases = tokenData.mappedAliases || {};
        oauthRequestPreview = tokenData.requestPreview;
        oauthResponsePreview = tokenData.responsePreview;
      }

      const itemRules = bindingRules.filter((rule) => rule.target === 'body_item');
      const scalarRules = bindingRules.filter((rule) => rule.target !== 'body_item');

      for (const rule of scalarRules) {
        const firstValue = group.rows[0]?.[rule.alias];
        const hasMismatch = group.rows.some((row) => !sameValue(row?.[rule.alias], firstValue));
        if (hasMismatch) {
          issues[`${rule.alias}:${rule.target}.${rule.path}`] =
            'в группе разные значения; использовано первое';
        }
        if (rule.target === 'header') {
          if (String(rule.path || '').trim() === 'Authorization') {
            const source = authHdr.Authorization;
            if (typeof source === 'string' && source.includes('{{')) {
              authHdr.Authorization = applyParametersToValue(source, { [rule.alias]: firstValue });
            } else {
              authHdr.Authorization = firstValue;
            }
          } else {
            hdr[rule.path] = firstValue;
          }
        } else if (rule.target === 'query') {
          setByPath(queryObj, rule.path, firstValue);
        } else if (rule.target === 'body') {
          setByPath(bodyObj, rule.path, firstValue);
        }
      }

      if (itemRules.length) {
        const items = group.rows.map((row) => {
          const item: Record<string, any> = {};
          itemRules.forEach((rule) => {
            setByPath(item, rule.path, row?.[rule.alias]);
          });
          return item;
        });
        setByPath(bodyObj, draft.bodyItemsPath || 'items', items);
      }

      const runtimeValues = { ...groupFirstMap, ...oauthResolvedAliases };
      applyParametersToValue(authHdr, runtimeValues);
      applyParametersToValue(hdr, runtimeValues);
      applyParametersToValue(queryObj, runtimeValues);
      applyParametersToValue(bodyObj, runtimeValues);

      let url = `${draft.baseUrl.replace(/\/$/, '')}${draft.path.startsWith('/') ? draft.path : `/${draft.path}`}`;
      url = replaceParameterTokens(url, runtimeValues);
      const u = new URL(url);
      Object.entries(queryObj || {}).forEach(([k, v]) => u.searchParams.set(k, String(v)));
      url = u.toString();

      const unresolvedTokens = new Set<string>();
      collectParameterTokens(url, unresolvedTokens);
      collectParameterTokens(authHdr, unresolvedTokens);
      collectParameterTokens(hdr, unresolvedTokens);
      collectParameterTokens(queryObj, unresolvedTokens);
      collectParameterTokens(bodyObj, unresolvedTokens);
      const unresolvedRelevant = Array.from(unresolvedTokens).filter(
        (alias) => !paginationAliasesLower.has(String(alias || '').trim().toLowerCase())
      );
      if (unresolvedRelevant.length) {
        const detail = unresolvedRelevant.map((alias) => {
          if (!groupedKnownAliasesLower.has(alias.toLowerCase())) return `${alias} (нет параметра с таким alias)`;
          return `${alias} (значение не вычислено для группы)`;
        });
        throw new Error(`Не удалось подставить параметры в групповой отправке: ${detail.join(', ')}`);
      }

      plans.push({
        group: group.key,
        rows_in_group: group.rows.length,
        context_values: deepClone(runtimeValues),
        method: draft.method,
        url,
        headers: { 'Content-Type': 'application/json', ...authHdr, ...hdr },
        body: draft.method === 'GET' || draft.method === 'DELETE' ? undefined : bodyObj,
        token_request_preview: oauthRequestPreview,
        token_response_preview: oauthResponsePreview,
        token_resolved_aliases: deepClone(oauthResolvedAliases)
      });
    }

    return {
      allRequests: plans,
      previewRequests: plans.slice(0, Math.max(1, Number(draft.previewRequestLimit || 5))).map(stripInternalPlanFields),
      issues
    };
  }

  async function buildUngroupedRowRequestPlan(draft: ApiDraft) {
    const issues: Record<string, string> = {};
    const sanitizedAliases = sanitizeAliasReferences(draft);
    const paginationAliasesLower = paginationAliasLowerSet(draft);
    const authTemplate = parseJsonObjectField('Авторизация', draft.authJson);
    const headersTemplate = parseJsonObjectField('Headers JSON', draft.headersJson);
    const queryTemplate = parseJsonObjectField('Query JSON', draft.queryJson);
    const bodyTemplateRaw = parseJsonAnyField('Body JSON', draft.bodyJson);
    const bodyTemplate = bodyTemplateRaw && typeof bodyTemplateRaw === 'object' ? bodyTemplateRaw : {};
    const urlTemplate = `${draft.baseUrl.replace(/\/$/, '')}${draft.path.startsWith('/') ? draft.path : `/${draft.path}`}`;
    const templateTokenSet = new Set<string>();
    collectParameterTokens(urlTemplate, templateTokenSet);
    collectParameterTokens(authTemplate, templateTokenSet);
    collectParameterTokens(headersTemplate, templateTokenSet);
    collectParameterTokens(queryTemplate, templateTokenSet);
    collectParameterTokens(bodyTemplate, templateTokenSet);
    const templateAliases = excludePaginationAliases(Array.from(templateTokenSet), paginationAliasesLower);
    const bindingRules = (Array.isArray(sanitizedAliases.bindingRules) ? sanitizedAliases.bindingRules : [])
      .map((rule) => ({
        id: String(rule?.id || uid()),
        alias: String(rule?.alias || '').trim(),
        target: toBindingTarget(String(rule?.target || 'body_item')),
        path: String(rule?.path || '').trim()
      }))
      .filter((rule) => rule.alias && rule.path);

    if (sanitizedAliases.removedAliases.length) {
      issues.removed_aliases = `удалены неактуальные алиасы: ${sanitizedAliases.removedAliases.join(', ')}`;
    }

    const requiredAliases = uniqueAliasList([
      ...bindingRules.map((rule) => rule.alias),
      ...templateAliases,
      ...oauth2RequestAliases(draft)
    ]);
    const oauth2ProvidedAliasesLower = new Set(
      (draft.authMode === AUTH_MODE_OAUTH2 ? oauth2MappingsForDraft(draft) : [])
        .map((m) => String(m.alias || '').trim().toLowerCase())
        .filter(Boolean)
    );
    const requiredAliasesFiltered = requiredAliases.filter(
      (alias) => !oauth2ProvidedAliasesLower.has(String(alias || '').trim().toLowerCase())
    );
    if (!requiredAliasesFiltered.length) {
      return {
        allRequests: [] as any[],
        previewRequests: [] as any[],
        issues
      };
    }

    const dataset = hasDataModelConfigured(draft)
      ? await loadDataModelRowsForAliases(draft, requiredAliasesFiltered)
      : await loadParameterRowsForAliases(draft.parameterDefinitions, requiredAliasesFiltered);
    Object.assign(issues, dataset.issues);
    if (!dataset.rows.length) {
      const issueEntries = Object.entries(issues);
      if (issueEntries.length) {
        const details = issueEntries
          .slice(0, 5)
          .map(([k, v]) => `${k}: ${v}`)
          .join('; ');
        throw new Error(`Нет строк для отправки по списку. Причины: ${details}`);
      }
      if (hasDataModelConfigured(draft)) {
        throw new Error(
          `Конструктор данных вернул 0 строк. Проверь Параметры/Связи/Фильтры и "Предпросмотр данных". Нужные поля: ${requiredAliases.join(', ')}`
        );
      }
      throw new Error(
        `Витрина параметров вернула 0 строк. Для отправки по списку параметры должны быть из одной таблицы и с совместимыми условиями. Нужные поля: ${requiredAliases.join(', ')}`
      );
    }

    const plans: Array<any> = [];
    const knownAliasesLower = new Set([
      ...bindingAliasOptions(draft).map((x) => x.toLowerCase()),
      ...Array.from(oauth2ProvidedAliasesLower),
      ...Array.from(paginationAliasesLower)
    ]);
    for (let rowIndex = 0; rowIndex < dataset.rows.length; rowIndex++) {
      const row = (dataset.rows[rowIndex] || {}) as Record<string, any>;
      const authHdr = parseJsonObjectField('Авторизация', draft.authJson);
      const hdr = parseJsonObjectField('Headers JSON', draft.headersJson);
      const queryObj = parseJsonObjectField('Query JSON', draft.queryJson);
      const bodyBaseRaw = parseJsonAnyField('Body JSON', draft.bodyJson);
      const bodyObj = bodyBaseRaw && typeof bodyBaseRaw === 'object' ? deepClone(bodyBaseRaw) : {};
      let oauthResolvedAliases: Record<string, any> = {};
      let oauthRequestPreview: any = null;
      let oauthResponsePreview: any = null;
      if (draft.authMode === 'oauth2_client_credentials') {
        const tokenData = await getOAuthToken(draft, row);
        oauthResolvedAliases = tokenData.mappedAliases || {};
        oauthRequestPreview = tokenData.requestPreview;
        oauthResponsePreview = tokenData.responsePreview;
      }

      applyBindingRulesToRequest(bindingRules, row, authHdr, hdr, queryObj, bodyObj);
      const runtimeValues = { ...row, ...oauthResolvedAliases };
      applyParametersToValue(authHdr, runtimeValues);
      applyParametersToValue(hdr, runtimeValues);
      applyParametersToValue(queryObj, runtimeValues);
      applyParametersToValue(bodyObj, runtimeValues);

      let url = `${draft.baseUrl.replace(/\/$/, '')}${draft.path.startsWith('/') ? draft.path : `/${draft.path}`}`;
      url = replaceParameterTokens(url, runtimeValues);
      const u = new URL(url);
      Object.entries(queryObj || {}).forEach(([k, v]) => u.searchParams.set(k, String(v)));
      url = u.toString();

      const unresolvedTokens = new Set<string>();
      collectParameterTokens(url, unresolvedTokens);
      collectParameterTokens(authHdr, unresolvedTokens);
      collectParameterTokens(hdr, unresolvedTokens);
      collectParameterTokens(queryObj, unresolvedTokens);
      collectParameterTokens(bodyObj, unresolvedTokens);
      const unresolvedRelevant = Array.from(unresolvedTokens).filter(
        (alias) => !paginationAliasesLower.has(String(alias || '').trim().toLowerCase())
      );
      if (unresolvedRelevant.length) {
        const detail = unresolvedRelevant.map((alias) => {
          if (!knownAliasesLower.has(alias.toLowerCase())) return `${alias} (нет параметра с таким alias)`;
          return `${alias} (значение не вычислено в строке ${rowIndex + 1})`;
        });
        throw new Error(`Не удалось подставить параметры в отправке по списку: ${detail.join(', ')}`);
      }

      plans.push({
        row_index: rowIndex + 1,
        context_values: deepClone(runtimeValues),
        method: draft.method,
        url,
        headers: { 'Content-Type': 'application/json', ...authHdr, ...hdr },
        body: draft.method === 'GET' || draft.method === 'DELETE' ? undefined : bodyObj,
        token_request_preview: oauthRequestPreview,
        token_response_preview: oauthResponsePreview,
        token_resolved_aliases: deepClone(oauthResolvedAliases)
      });
    }

    return {
      allRequests: plans,
      previewRequests: plans.slice(0, Math.max(1, Number(draft.previewRequestLimit || 5))).map(stripInternalPlanFields),
      issues
    };
  }

  async function previewRequestsNow() {
    err = '';
    ok = '';
    myPreviewApplyMessage = '';
    if (!selected) {
      err = 'Выбери API';
      return;
    }
    try {
      applyUrlInput(requestInput);
      const s = byRef(selectedRef) || selected;
      if (isGroupedDispatchEnabled(s)) {
        const groupedPlan = await buildGroupedRequestPlan(s);
        myApiPreview = JSON.stringify(
          {
            mode: 'preview_before_send',
            total_requests: groupedPlan.allRequests.length,
            shown_requests: groupedPlan.previewRequests.length,
            requests: groupedPlan.previewRequests,
            issues: Object.keys(groupedPlan.issues).length ? groupedPlan.issues : undefined
          },
          null,
          2
        );
        myPreviewDirty = false;
        myPreviewApplyMessage = `Показано ${groupedPlan.previewRequests.length} из ${groupedPlan.allRequests.length} запросов перед отправкой`;
        return;
      }
      const rowPlan = await buildUngroupedRowRequestPlan(s);
      if (rowPlan.allRequests.length) {
        myApiPreview = JSON.stringify(
          {
            mode: 'preview_before_send',
            total_requests: rowPlan.allRequests.length,
            shown_requests: rowPlan.previewRequests.length,
            requests: rowPlan.previewRequests,
            issues: Object.keys(rowPlan.issues).length ? rowPlan.issues : undefined
          },
          null,
          2
        );
        myPreviewDirty = false;
        myPreviewApplyMessage = `Показано ${rowPlan.previewRequests.length} из ${rowPlan.allRequests.length} запросов перед отправкой`;
        return;
      }

      const authHdr = parseJsonObjectField('Авторизация', s.authJson);
      const hdr = parseJsonObjectField('Headers JSON', s.headersJson);
      const queryObj = parseJsonObjectField('Query JSON', s.queryJson);
      const bodyRaw = parseJsonAnyField('Body JSON', s.bodyJson);
      const bodyObj = bodyRaw && typeof bodyRaw === 'object' ? deepClone(bodyRaw) : {};
      const sanitizedAliases = sanitizeAliasReferences(s);
      const paginationAliasesLower = paginationAliasLowerSet(s);
      const initialTokens = new Set<string>();
      const urlTemplate = `${s.baseUrl.replace(/\/$/, '')}${s.path.startsWith('/') ? s.path : `/${s.path}`}`;
      collectParameterTokens(urlTemplate, initialTokens);
      collectParameterTokens(authHdr, initialTokens);
      collectParameterTokens(hdr, initialTokens);
      collectParameterTokens(queryObj, initialTokens);
      collectParameterTokens(bodyObj, initialTokens);
      const requestedAliases = excludePaginationAliases(
        [...Array.from(initialTokens), ...sanitizedAliases.bindingRules.map((r) => r.alias)],
        paginationAliasesLower
      );
      const { map: parameterValues, issues: parameterIssues } = await resolveRuntimeAliasValues(s, requestedAliases);
      if (Object.keys(parameterValues).length) {
        applyParametersToValue(authHdr, parameterValues);
        applyParametersToValue(hdr, parameterValues);
        applyParametersToValue(queryObj, parameterValues);
        applyParametersToValue(bodyObj, parameterValues);
      }
      applyBindingRulesToRequest(sanitizedAliases.bindingRules, parameterValues, authHdr, hdr, queryObj, bodyObj);
      let url = `${s.baseUrl.replace(/\/$/, '')}${s.path.startsWith('/') ? s.path : `/${s.path}`}`;
      if (Object.keys(parameterValues).length) {
        url = replaceParameterTokens(url, parameterValues);
      }
      const u = new URL(url);
      Object.entries(queryObj || {}).forEach(([k, v]) => u.searchParams.set(k, String(v)));
      const unresolvedTokens = new Set<string>();
      collectParameterTokens(u.toString(), unresolvedTokens);
      collectParameterTokens(authHdr, unresolvedTokens);
      collectParameterTokens(hdr, unresolvedTokens);
      collectParameterTokens(queryObj, unresolvedTokens);
      collectParameterTokens(bodyObj, unresolvedTokens);
      const unresolvedRelevant = Array.from(unresolvedTokens).filter(
        (alias) => !paginationAliasesLower.has(String(alias || '').trim().toLowerCase())
      );
      if (unresolvedRelevant.length) {
        const knownAliasesLower = new Set(
          [
            ...uniqueAliasList((s.parameterDefinitions || []).map((p) => String(p?.alias || '').trim()).filter(Boolean)),
            ...uniqueAliasList((s.dataFields || []).map((f) => String(f?.alias || '').trim()).filter(Boolean)),
            ...uniqueAliasList(dataDateParametersForDraft(s).map((p) => String(p?.alias || '').trim()).filter(Boolean)),
            ...uniqueAliasList(dataApiParametersForDraft(s).map((p) => String(p?.alias || '').trim()).filter(Boolean)),
            ...Array.from(paginationAliasesLower)
          ].map((x) => x.toLowerCase())
        );
        const detail = unresolvedRelevant.map((alias) => {
          const issueByAlias = findAliasInMap(parameterIssues, alias);
          if (issueByAlias.found) return `${alias} (${issueByAlias.value})`;
          if (!knownAliasesLower.has(alias.toLowerCase())) return `${alias} (нет параметра с таким alias)`;
          return `${alias} (значение не вычислено)`;
        });
        throw new Error(`Не удалось подставить параметры: ${detail.join(', ')}`);
      }
      const request = {
        method: s.method,
        url: u.toString(),
        headers: { 'Content-Type': 'application/json', ...authHdr, ...hdr },
        body: s.method === 'GET' || s.method === 'DELETE' ? undefined : bodyObj
      };
      myApiPreview = JSON.stringify(
        {
          mode: 'preview_before_send',
          total_requests: 1,
          shown_requests: 1,
          requests: [request],
          resolved_parameters: parameterValues,
          issues: Object.keys(parameterIssues).length ? parameterIssues : undefined
        },
        null,
        2
      );
      myPreviewDirty = false;
      myPreviewApplyMessage = 'Показан запрос перед отправкой';
    } catch (e: any) {
      err = e?.message ?? String(e);
    }
  }

  async function loadParameterPreview() {
    parameterPreviewError = '';
    parameterPreviewRows = [];
    if (!activeParameter) {
      parameterPreviewError = 'Выбери параметр.';
      return;
    }
    const target = getParameterPreviewTarget(activeParameter);
    if (!target) {
      parameterPreviewError = 'Укажи источник (схема и таблица).';
      return;
    }
    parameterPreviewLoading = true;
    try {
      const url = `${apiBase}/preview?schema=${encodeURIComponent(target.schema)}&table=${encodeURIComponent(target.table)}&limit=${PARAMETER_PREVIEW_LIMIT}`;
      const j = await apiJson<{ rows: any[] }>(url);
      parameterPreviewRows = j.rows || [];
      if (!parameterPreviewRows.length) {
        parameterPreviewError = 'Пока нет строк. Попробуй другой источник или добавь данные.';
      }
    } catch (e: any) {
      parameterPreviewError = e?.message ?? String(e);
    } finally {
      parameterPreviewLoading = false;
    }
  }

  async function checkStorageTable(schema: string, table: string) {
    try {
      const j = await apiJson<{ columns: Array<{ name: string; type: string }> }>(
        `${apiBase}/columns?schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`,
        { headers: headers() }
      );
      const cols = Array.isArray(j?.columns) ? j.columns : [];
      if (!cols.length) {
        err = `Таблица ${schema}.${table} не найдена или пуста.`;
        return false;
      }
      const colTypeByName = new Map(
        cols.map((x) => [String(x?.name || '').toLowerCase(), normalizeTypeName(String(x?.type || ''))])
      );
      const issues: string[] = [];
      for (const need of API_STORAGE_REQUIRED_COLUMNS) {
        const actualType = colTypeByName.get(String(need.name || '').toLowerCase());
        if (!actualType) {
          issues.push(`${need.name}: нет колонки`);
          continue;
        }
        const match = need.types.some((expected) => actualType.includes(normalizeTypeName(expected)));
        if (!match) {
          issues.push(`${need.name}: тип ${actualType}`);
        }
      }
      if (issues.length) {
        const short = issues.slice(0, 12);
        const tail = issues.length > short.length ? `; ещё ${issues.length - short.length}` : '';
        err =
          `Структура ${schema}.${table} не подходит для "Хранилища API". ` +
          `Проблемные поля: ${short.join(', ')}${tail}.`;
        return false;
      }
      return true;
    } catch (e: any) {
      err = e?.message ?? String(e);
      return false;
    }
  }

  async function applyStorageChoice() {
    err = '';
    ok = '';
    warn = '';
    const [schema, table] = String(api_storage_pick_value || '').split('.');
    if (!schema || !table) return;
    const valid = await checkStorageTable(schema, table);
    if (!valid) return;
    await apiJson(`${apiBase}/settings/upsert`, {
      method: 'POST',
      headers: headers(),
      body: JSON.stringify({
        setting_key: 'api_configs_storage',
        setting_value: { schema, table },
        description: 'Хранилище преднастроенных API',
        scope: 'global',
        is_active: true
      })
    });
    api_storage_schema = schema;
    api_storage_table = table;
    api_storage_picker_open = false;
    ok = 'Хранилище API подключено';
    await loadAll();
  }

  function applyInitialApiStoreSelection() {
    const targetId = Math.trunc(Number(initialApiStoreId || 0));
    if (!Number.isFinite(targetId) || targetId <= 0) return false;
    const match = drafts.find((x) => Number(x.storeId || 0) === targetId);
    if (!match) return false;
    selectedRef = refOf(match);
    nameDraft = match.name;
    initialApiStoreIdApplied = targetId;
    return true;
  }

  async function loadAll() {
    loading = true;
    err = '';
    warn = '';
    try {
      try {
        const cfg = await apiJson<{ effective?: any }>(`${apiBase}/settings/effective`, { headers: headers() });
        const eff = cfg?.effective || {};
        const s = String(eff?.api_configs_schema || '').trim();
        const t = String(eff?.api_configs_table || '').trim();
        if (s) api_storage_schema = s;
        if (t) api_storage_table = t;
      } catch {
        // ignore settings failure
      }
      api_storage_pick_value = `${api_storage_schema}.${api_storage_table}`;
      loadTemplateUiPrefs();

      const j = await apiJson<{ api_configs: any[] }>(`${apiBase}/api-configs`, { headers: headers() });
      drafts = (Array.isArray(j?.api_configs) ? j.api_configs : []).map((r) => fromRow(r));
      const missingStoreId = drafts.filter((x) => !resolveDraftStoreId(x)).length;
      if (missingStoreId > 0) {
        warn = `Часть API-шаблонов загружена без ID (${missingStoreId}). Сохранение/удаление для них может работать ограниченно.`;
      }
      cleanupTemplateUiRefs();

      if (!drafts.length) {
        const d = baseDraft();
        drafts = [d];
        selectedRef = refOf(d);
        nameDraft = d.name;
      } else if (!byRef(selectedRef)) {
        selectedRef = refOf(drafts[0]);
      }
      applyInitialApiStoreSelection();
      const baselineDraft = byRef(selectedRef);
      selectedBaselineSignature = selectedDraftSignature(baselineDraft, String(baselineDraft?.name || ''));
    } catch (e: any) {
      err = e?.message ?? String(e);
    } finally {
      loading = false;
    }
  }

  async function saveSelected() {
    err = '';
    ok = '';
    warn = '';
    const current = selected;
    if (!current) {
      err = 'Выбери API для сохранения';
      return;
    }
    const name = String(nameDraft || current.name || '').trim();
    if (!name) {
      err = 'Укажи название API';
      return;
    }
    const duplicateOnSave = duplicateNameMatches(name, current);
    if (duplicateOnSave.length) {
      warn = `Предупреждение: шаблон с таким именем уже есть (${duplicateOnSave.length}).`;
    }

    applyUrlInput(requestInput);
    mutateSelected((d) => {
      d.name = name;
    });

    const next = byRef(selectedRef);
    if (!next) {
      err = 'Не удалось найти выбранный API';
      return;
    }

    let parsedFields: { headersJson: Record<string, any>; queryJson: Record<string, any>; bodyJson: any; authJson: Record<string, any> };
    try {
      parsedFields = {
        authJson: parseJsonObjectField('Авторизация', next.authJson),
        headersJson: parseJsonObjectField('Headers JSON', next.headersJson),
        queryJson: parseJsonObjectField('Query JSON', next.queryJson),
        bodyJson: parseJsonAnyField('Body JSON', next.bodyJson)
      };
    } catch (e: any) {
      err = e?.message ?? String(e);
      return;
    }

    saving = true;
    try {
      let resolvedStoreId = resolveDraftStoreId(next, selectedRef);
      if (!resolvedStoreId) {
        resolvedStoreId = await resolveServerStoreIdForDraft(next);
      }
      const payload = toPayload(next, parsedFields);
      if (resolvedStoreId > 0) payload.id = resolvedStoreId;
      const r = await apiJson<{ id?: number }>(`${apiBase}/api-configs/upsert`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify(payload)
      });
      const id = parsePositiveInt(r?.id) || resolvedStoreId;
      if (!id) {
        throw new Error('Сервер не вернул числовой ID. Проверь структуру таблицы хранилища API (id bigint с авто-генерацией).');
      }
      await loadAll();
      if (Number.isFinite(id) && id > 0) {
        const m = drafts.find((x) => Number(x.storeId || 0) === id);
        if (m) {
          selectedRef = refOf(m);
          nameDraft = m.name;
          selectedBaselineSignature = selectedDraftSignature(m, m.name);
        }
      }
      ok = 'API сохранен в БД';
    } catch (e: any) {
      err = e?.message ?? String(e);
    } finally {
      saving = false;
    }
  }

  async function addApi() {
    err = '';
    ok = '';
    warn = '';
    const name = String(nameDraft || '').trim();
    if (!name) {
      err = 'Укажи название API';
      return;
    }
    const duplicateOnAdd = duplicateNameMatches(name, null);
    if (duplicateOnAdd.length) {
      warn = `Предупреждение: шаблон с таким именем уже есть (${duplicateOnAdd.length}).`;
    }
    const d = baseDraft();
    d.name = name;
    drafts = [d, ...drafts];
    selectedRef = refOf(d);
    requestInput = '';
    await saveSelected();
  }

  async function deleteApi(d: ApiDraft) {
    err = '';
    ok = '';
    warn = '';
    try {
      const deletedRef = refOf(d);
      let storeId = resolveDraftStoreId(d, refOf(d));
      if (!storeId) {
        storeId = await resolveServerStoreIdForDraft(d);
      }
      if (!storeId) {
        drafts = drafts.filter((x) => x.localId !== d.localId);
        templateFavoriteRefs = templateFavoriteRefs.filter((x) => x !== deletedRef);
        templateRecentRefs = templateRecentRefs.filter((x) => x !== deletedRef);
        persistTemplateUiPrefs();
        if (!byRef(selectedRef) && drafts.length) selectedRef = refOf(drafts[0]);
        warn = 'Удален только локальный черновик (ID в БД не найден).';
        return;
      }
      await apiJson(`${apiBase}/api-configs/delete`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({ id: storeId })
      });
      await loadAll();
      ok = 'API удален';
    } catch (e: any) {
      err = e?.message ?? String(e);
    }
  }

  async function executePlannedRequestWithPagination(
    draft: ApiDraft,
    reqPlan: any,
    sentRequests: any[],
    onStep?: (entry: any) => Promise<void> | void
  ): Promise<{
    requestCount: number;
    success: number;
    failed: number;
    totalItems: number;
    totalSize: number;
    lastStatus: number;
    stopReason: string;
    responses_dropped: number;
  }> {
    const state = initWaveRuntimeState(draft, reqPlan);
    const executionDelayMs = Math.max(0, Number(draft.executionDelayMs || 0));
    let waveNo = 1;
    while (!state.done) {
      if (executionDelayMs > 0 && state.requestCount > 0) {
        await new Promise((resolve) => setTimeout(resolve, executionDelayMs));
      }
      const step = await executeWaveRuntimeStep(draft, state, sentRequests, waveNo);
      if (step?.entry) {
        if (onStep) await onStep(step.entry);
      }
      if (step?.done) break;
      waveNo += 1;
    }
    return {
      requestCount: state.requestCount,
      success: state.success,
      failed: state.failed,
      totalItems: state.totalItems,
      totalSize: state.totalSize,
      lastStatus: state.lastStatus,
      stopReason: state.stopReason || (!draft.paginationEnabled ? 'Пагинация отключена' : ''),
      responses_dropped: 0
    };
  }

  type WaveRuntimeState = {
    reqPlan: any;
    method: string;
    isBodyMethod: boolean;
    baseHeaders: Record<string, string>;
    paginationParameters: PaginationParameter[];
    paginationAliasesLower: Set<string>;
    paginationState: Record<string, any>;
    bodyCursorState: any;
    cursorQueryState: Record<string, any>;
    nextUrlOverride: string;
    currentPage: number;
    currentOffset: number;
    requestCount: number;
    success: number;
    failed: number;
    totalItems: number;
    totalSize: number;
    lastStatus: number;
    stopReason: string;
    done: boolean;
    nextIterationReason: string;
    lastResponseHash: string;
    sameResponseCount: number;
    seenPaginationStates: Set<string>;
  };

  function initWaveRuntimeState(draft: ApiDraft, reqPlan: any): WaveRuntimeState {
    const method = String(reqPlan?.method || draft.method || 'GET').toUpperCase();
    const isBodyMethod = method !== 'GET' && method !== 'DELETE';
    const paginationParameters = paginationParametersForDraft(draft).filter(
      (param) => String(param?.responsePath || '').trim() && String(param?.alias || '').trim()
    );
    return {
      reqPlan,
      method,
      isBodyMethod,
      baseHeaders: normalizeRequestHeaders(reqPlan?.headers || {}),
      paginationParameters,
      paginationAliasesLower: new Set(
        paginationParameters.map((param) => String(param?.alias || '').trim().toLowerCase()).filter(Boolean)
      ),
      paginationState: {},
      bodyCursorState: isBodyMethod && reqPlan?.body && typeof reqPlan.body === 'object' ? deepClone(reqPlan.body) : {},
      cursorQueryState: {},
      nextUrlOverride: '',
      currentPage: Number(draft.paginationStartPage || 1),
      currentOffset: Number(draft.paginationStartPage || 0),
      requestCount: 0,
      success: 0,
      failed: 0,
      totalItems: 0,
      totalSize: 0,
      lastStatus: 0,
      stopReason: '',
      done: false,
      nextIterationReason: 'initial_request',
      lastResponseHash: '',
      sameResponseCount: 0,
      seenPaginationStates: new Set<string>()
    };
  }

  async function executeWaveRuntimeStep(
    draft: ApiDraft,
    state: WaveRuntimeState,
    sentRequests: any[],
    waveNo: number
  ): Promise<{ entry: any | null; done: boolean; stopReason: string }> {
    if (state.done) return { entry: null, done: true, stopReason: state.stopReason || '' };
    const maxPagesEnabled = draft.paginationEnabled && Boolean(draft.paginationUseMaxPages);
    const pagesMax = maxPagesEnabled ? Math.max(1, Number(draft.paginationMaxPages || 1)) : Number.MAX_SAFE_INTEGER;
    if (maxPagesEnabled && state.requestCount >= pagesMax) {
      state.done = true;
      state.stopReason = `Достигнут лимит страниц (${pagesMax})`;
      return { entry: null, done: true, stopReason: state.stopReason };
    }

    const sourceUrl = String(state.nextUrlOverride || state.reqPlan?.url || '').trim();
    if (!sourceUrl) throw new Error('Пустой URL в плане отправки');
    const u = new URL(sourceUrl);
    const queryObj: Record<string, any> = {};
    u.searchParams.forEach((v, k) => {
      queryObj[k] = v;
    });
    const bodyObj = state.isBodyMethod ? deepClone(state.bodyCursorState || {}) : undefined;
    const headersObj: Record<string, any> = { ...state.baseHeaders };
    const iterationReason = String(state.nextIterationReason || '').trim() || (state.requestCount === 0 ? 'initial_request' : 'wave_iteration');
    let nextReason = 'pagination_values_updated';

    if (draft.paginationEnabled) {
      if (state.paginationParameters.length) {
        for (const param of state.paginationParameters) {
          let value = state.paginationState[param.id];
          if ((value === undefined || value === null) && state.requestCount === 0) {
            const first = String(param?.firstValue || '').trim();
            if (first) value = parseStopRuleCompareValue(first);
          }
          if (value === undefined || value === null) continue;
          applyPaginationValueToRequest(param, value, queryObj, bodyObj, headersObj);
        }
      } else if (draft.paginationStrategy === 'page_number') {
        const p = draft.paginationPageParam || 'page';
        if (draft.paginationTarget === 'query') queryObj[p] = state.currentPage;
        else if (bodyObj) setByPath(bodyObj, p, state.currentPage);
      } else if (draft.paginationStrategy === 'offset_limit') {
        const off = draft.paginationPageParam || 'offset';
        const lim = draft.paginationLimitParam || 'limit';
        const limVal = Number(draft.paginationLimitValue || 100);
        if (draft.paginationTarget === 'query') {
          queryObj[off] = state.currentOffset;
          queryObj[lim] = limVal;
        } else if (bodyObj) {
          setByPath(bodyObj, off, state.currentOffset);
          setByPath(bodyObj, lim, limVal);
        }
      } else if (draft.paginationStrategy === 'cursor_fields') {
        if (draft.paginationCursorReqPath1) {
          if (draft.paginationTarget === 'query') {
            const v = state.cursorQueryState[draft.paginationCursorReqPath1];
            if (v !== undefined && v !== null) queryObj[draft.paginationCursorReqPath1] = v;
          } else if (bodyObj) {
            const v = getByPath(state.bodyCursorState, draft.paginationCursorReqPath1);
            if (v !== undefined && v !== null) setByPath(bodyObj, draft.paginationCursorReqPath1, v);
          }
        }
        if (draft.paginationCursorReqPath2) {
          if (draft.paginationTarget === 'query') {
            const v = state.cursorQueryState[draft.paginationCursorReqPath2];
            if (v !== undefined && v !== null) queryObj[draft.paginationCursorReqPath2] = v;
          } else if (bodyObj) {
            const v = getByPath(state.bodyCursorState, draft.paginationCursorReqPath2);
            if (v !== undefined && v !== null) setByPath(bodyObj, draft.paginationCursorReqPath2, v);
          }
        }
      }
    }

    if (state.paginationParameters.length) {
      const tokenMap: Record<string, any> = {};
      state.paginationParameters.forEach((param) => {
        let value = state.paginationState[param.id];
        if ((value === undefined || value === null) && state.requestCount === 0) {
          const first = String(param?.firstValue || '').trim();
          if (first) value = parseStopRuleCompareValue(first);
        }
        if (value !== undefined && value !== null) tokenMap[param.alias] = value;
      });
      if (Object.keys(tokenMap).length) {
        applyParametersToValue(headersObj, tokenMap);
        applyParametersToValue(queryObj, tokenMap);
        if (bodyObj && typeof bodyObj === 'object') {
          applyParametersToValue(bodyObj, tokenMap);
        }
      }
      stripUnresolvedPaginationTokens(queryObj, state.paginationAliasesLower);
      stripUnresolvedPaginationTokens(headersObj, state.paginationAliasesLower);
      if (bodyObj && typeof bodyObj === 'object') {
        stripUnresolvedPaginationTokens(bodyObj, state.paginationAliasesLower);
      }
    }

    u.search = '';
    Object.entries(queryObj).forEach(([k, v]) => {
      if (v === undefined || v === null) return;
      u.searchParams.set(k, String(v));
    });
    const finalUrl = u.toString();
    const requestHeaders: Record<string, string> = {};
    Object.entries(headersObj).forEach(([k, v]) => {
      if (v === undefined || v === null) return;
      requestHeaders[String(k)] = String(v);
    });
    const init: RequestInit = { method: state.method, headers: requestHeaders };
    if (state.isBodyMethod) init.body = JSON.stringify(bodyObj || {});

    if (sentRequests.length < REQUEST_PREVIEW_MAX) {
      const sentReq: Record<string, any> = {
        method: state.method,
        url: finalUrl,
        headers: { ...requestHeaders },
        query: deepClone(queryObj || {}),
        body: state.isBodyMethod ? deepClone(bodyObj || {}) : undefined,
        page: state.requestCount + 1,
        wave: waveNo,
        iteration_reason: iterationReason
      };
      if (state.reqPlan?.group) sentReq.group = state.reqPlan.group;
      if (state.reqPlan?.row_index) sentReq.row_index = state.reqPlan.row_index;
      sentRequests.push(sentReq);
    }

    const startedAt = Date.now();
    let proxied: any = null;
    let requestAttempts = 0;
    try {
      const retried = await runHttpRequestViaServerWithRetry(finalUrl, init, 60_000, {
        attempts: RETRY_MAX_ATTEMPTS_DEFAULT,
        baseDelayMs: RETRY_BASE_DELAY_MS_DEFAULT,
        maxDelayMs: RETRY_MAX_DELAY_MS_DEFAULT
      });
      proxied = retried.proxied;
      requestAttempts = Number(retried.attempts || 1);
    } catch (requestError: any) {
      const durationMs = Date.now() - startedAt;
      state.failed += 1;
      state.requestCount += 1;
      state.done = true;
      state.stopReason = `Ошибка запроса: ${requestError?.message ?? String(requestError)}`;
      const failureEntry: Record<string, any> = {
        page: state.requestCount,
        status: 0,
        error: requestError?.message ?? String(requestError),
        wave: waveNo,
        iteration_reason: iterationReason,
        decision: 'fail',
        stop_reason: state.stopReason,
        request: {
          method: state.method,
          url: finalUrl,
          headers: deepClone(requestHeaders),
          query: deepClone(queryObj || {}),
          body: state.isBodyMethod ? deepClone(bodyObj || {}) : undefined
        },
        duration_ms: durationMs,
        response_size: String(requestError?.message ?? String(requestError)).length,
        retry_attempts: requestAttempts || 1
      };
      if (state.reqPlan?.group) failureEntry.group = state.reqPlan.group;
      if (state.reqPlan?.row_index) failureEntry.row_index = state.reqPlan.row_index;
      return { entry: failureEntry, done: true, stopReason: state.stopReason };
    }
    const durationMs = Date.now() - startedAt;
    state.lastStatus = Number(proxied?.status || 0);
    const txt = String(proxied?.body_text || '');
    state.totalSize += txt ? txt.length : 0;
    const parsed = proxied?.body_json !== undefined
      ? proxied.body_json
      : (() => {
          try {
            return txt ? JSON.parse(txt) : null;
          } catch {
            return null;
          }
        })();
    if (proxied?.ok) state.success += 1;
    else state.failed += 1;
    state.totalItems += countPayloadItems(parsed ?? txt);
    state.requestCount += 1;
    const currentHash = responseFingerprint(parsed ?? txt);
    if (currentHash && currentHash === state.lastResponseHash) state.sameResponseCount += 1;
    else state.sameResponseCount = 1;
    state.lastResponseHash = currentHash;

    const responseEntry: Record<string, any> = {
      page: state.requestCount,
      status: Number(proxied?.status || 0),
      response: parsed ?? txt,
      wave: waveNo,
      iteration_reason: iterationReason,
      decision: 'continue',
      request: {
        method: state.method,
        url: finalUrl,
        headers: deepClone(requestHeaders),
        query: deepClone(queryObj || {}),
        body: state.isBodyMethod ? deepClone(bodyObj || {}) : undefined
      },
      duration_ms: durationMs,
      response_size: txt ? txt.length : JSON.stringify(parsed ?? '').length,
      retry_attempts: requestAttempts || 1
    };
    if (state.reqPlan?.group) responseEntry.group = state.reqPlan.group;
    if (state.reqPlan?.row_index) responseEntry.row_index = state.reqPlan.row_index;

    if (!draft.paginationEnabled) {
      state.done = true;
      state.stopReason = 'Пагинация отключена';
      responseEntry.decision = 'stop';
      responseEntry.stop_reason = state.stopReason;
      return { entry: responseEntry, done: true, stopReason: state.stopReason };
    }
    if (draft.paginationStopOnHttpError && Number(state.lastStatus || 0) >= 400) {
      state.done = true;
      state.stopReason = `HTTP ошибка ${state.lastStatus}`;
      responseEntry.decision = 'fail';
      responseEntry.stop_reason = state.stopReason;
      return { entry: responseEntry, done: true, stopReason: state.stopReason };
    }

    const paginationStopRules = paginationStopRulesForDraft(draft).filter((rule) => String(rule?.responsePath || '').trim());
    if (paginationStopRules.length) {
      const runtimeTokenMap: Record<string, any> = {};
      state.paginationParameters.forEach((param) => {
        const value = state.paginationState[param.id];
        if (value !== undefined && value !== null) runtimeTokenMap[param.alias] = value;
      });
      const contextValues =
        state.reqPlan?.context_values && typeof state.reqPlan.context_values === 'object'
          ? { ...state.reqPlan.context_values }
          : {};
      const groupValues =
        state.reqPlan?.group && typeof state.reqPlan.group === 'object'
          ? { ...state.reqPlan.group }
          : {};
      const matched = findMatchedPaginationStopRule(parsed, paginationStopRules, {
        values: { ...contextValues, ...groupValues, ...runtimeTokenMap },
        queryObj: queryObj || {},
        bodyObj: bodyObj || {},
        headersObj: headersObj || {}
      });
      if (matched) {
        state.done = true;
        state.stopReason = formatPaginationStopRuleReason(matched.rule, matched.actualValue, matched.expectedValue);
        responseEntry.decision = 'stop';
        responseEntry.stop_reason = state.stopReason;
        return { entry: responseEntry, done: true, stopReason: state.stopReason };
      }
    }

    if (state.paginationParameters.length) {
      let foundValues = 0;
      let updatedValues = 0;
      const extracted: Record<string, any> = {};
      for (const param of state.paginationParameters) {
        const value = readPaginationValueFromResponse(parsed, param);
        if (value === undefined || value === null) continue;
        extracted[param.alias] = value;
        foundValues += 1;
        const prev = state.paginationState[param.id];
        if (prev === undefined || prev === null || !sameValue(prev, value)) {
          state.paginationState[param.id] = value;
          updatedValues += 1;
        }
      }
      responseEntry.pagination_values = extracted;
      if (!foundValues && draft.paginationStopOnMissingValue) {
        state.done = true;
        state.stopReason = 'Не найдено значение параметров пагинации';
        responseEntry.decision = 'stop';
        responseEntry.stop_reason = state.stopReason;
        return { entry: responseEntry, done: true, stopReason: state.stopReason };
      }
      if (!updatedValues && draft.paginationStopOnMissingValue) {
        state.done = true;
        state.stopReason = 'Не найдено новое значение параметров пагинации (значения не изменились)';
        responseEntry.decision = 'stop';
        responseEntry.stop_reason = state.stopReason;
        return { entry: responseEntry, done: true, stopReason: state.stopReason };
      }
      if (foundValues) {
        const fp = JSON.stringify(state.paginationParameters.map((param) => [param.id, state.paginationState[param.id]]));
        if (state.seenPaginationStates.has(fp)) {
          state.done = true;
          state.stopReason = 'Повторилось состояние пагинации (зацикливание)';
          responseEntry.decision = 'stop';
          responseEntry.stop_reason = state.stopReason;
          return { entry: responseEntry, done: true, stopReason: state.stopReason };
        }
        state.seenPaginationStates.add(fp);
        nextReason = updatedValues > 0 ? 'pagination_values_updated' : 'pagination_values_repeat';
      }
    } else if (draft.paginationStrategy === 'page_number') {
      state.currentPage += 1;
      nextReason = 'page_increment';
    } else if (draft.paginationStrategy === 'offset_limit') {
      state.currentOffset += Number(draft.paginationLimitValue || 100);
      nextReason = 'offset_increment';
    } else if (draft.paginationStrategy === 'cursor_fields') {
      const v1 = draft.paginationCursorResPath1 ? getByPath(parsed, draft.paginationCursorResPath1) : undefined;
      const v2 = draft.paginationCursorResPath2 ? getByPath(parsed, draft.paginationCursorResPath2) : undefined;
      if (v1 == null && v2 == null) {
        if (draft.paginationStopOnMissingValue) {
          state.done = true;
          state.stopReason = 'Не найдено новое значение курсора';
          responseEntry.decision = 'stop';
          responseEntry.stop_reason = state.stopReason;
          return { entry: responseEntry, done: true, stopReason: state.stopReason };
        }
      } else {
        if (draft.paginationCursorReqPath1) {
          if (draft.paginationTarget === 'query') {
            state.cursorQueryState[draft.paginationCursorReqPath1] = v1;
          } else {
            setByPath(state.bodyCursorState, draft.paginationCursorReqPath1, v1);
          }
        }
        if (draft.paginationCursorReqPath2) {
          if (draft.paginationTarget === 'query') {
            state.cursorQueryState[draft.paginationCursorReqPath2] = v2;
          } else {
            setByPath(state.bodyCursorState, draft.paginationCursorReqPath2, v2);
          }
        }
        nextReason = 'cursor_updated';
      }
    } else if (draft.paginationStrategy === 'next_url') {
      const n = draft.paginationNextUrlPath ? getByPath(parsed, draft.paginationNextUrlPath) : undefined;
      if (!n || typeof n !== 'string') {
        if (draft.paginationStopOnMissingValue) {
          state.done = true;
          state.stopReason = 'Не найден следующий URL для пагинации';
          responseEntry.decision = 'stop';
          responseEntry.stop_reason = state.stopReason;
          return { entry: responseEntry, done: true, stopReason: state.stopReason };
        }
      } else {
        state.nextUrlOverride = n;
        nextReason = 'next_url_received';
      }
    } else if (draft.paginationStrategy === 'custom') {
      state.done = true;
      state.stopReason = 'Кастомная стратегия не настроена';
      responseEntry.decision = 'stop';
      responseEntry.stop_reason = state.stopReason;
      return { entry: responseEntry, done: true, stopReason: state.stopReason };
    } else {
      state.done = true;
      state.stopReason = 'Стратегия пагинации не настроена';
      responseEntry.decision = 'stop';
      responseEntry.stop_reason = state.stopReason;
      return { entry: responseEntry, done: true, stopReason: state.stopReason };
    }

    const sameLimit = Math.max(2, Number(draft.paginationSameResponseLimit || 5));
    if (draft.paginationStopOnSameResponse && state.sameResponseCount >= sameLimit) {
      state.done = true;
      state.stopReason = `Получено ${state.sameResponseCount} одинаковых ответов подряд`;
      responseEntry.decision = 'stop';
      responseEntry.stop_reason = state.stopReason;
      return { entry: responseEntry, done: true, stopReason: state.stopReason };
    }

    if (maxPagesEnabled && state.requestCount >= pagesMax) {
      state.done = true;
      state.stopReason = `Достигнут лимит страниц (${pagesMax})`;
      responseEntry.decision = 'stop';
      responseEntry.stop_reason = state.stopReason;
      return { entry: responseEntry, done: true, stopReason: state.stopReason };
    }

    responseEntry.decision = 'continue';
    responseEntry.next_iteration_reason = nextReason;
    state.nextIterationReason = nextReason;
    if (Boolean(draft.paginationUseDelay) && Number(draft.paginationDelayMs || 0) > 0) {
      await new Promise((resolve) => setTimeout(resolve, Number(draft.paginationDelayMs || 0)));
    }
    return { entry: responseEntry, done: false, stopReason: '' };
  }

  type ExecutionRunSummary = {
    requestCount: number;
    success: number;
    failed: number;
    totalItems: number;
    totalSize: number;
    lastStatus: number;
    responses: any[];
    stopReasons: any[];
    sentRequests: any[];
    issues?: Record<string, string>;
    modeLabel: string;
    responsesDropped: number;
    auditLog: { written: number; skipped: number; error?: string };
  };

  function runEntityIdentity(reqPlan: any, index: number) {
    if (reqPlan?.group && typeof reqPlan.group === 'object') {
      const entries = Object.entries(reqPlan.group);
      const label = entries.length ? entries.map(([k, v]) => `${k}=${String(v)}`).join(', ') : `Сущность ${index + 1}`;
      return {
        entity_key: JSON.stringify(reqPlan.group),
        entity_label: label
      };
    }
    if (Number.isFinite(Number(reqPlan?.row_index)) && Number(reqPlan.row_index) > 0) {
      const rowIndex = Number(reqPlan.row_index);
      return {
        entity_key: `row_${rowIndex}`,
        entity_label: `Строка ${rowIndex}`
      };
    }
    return {
      entity_key: `entity_${index + 1}`,
      entity_label: `Сущность ${index + 1}`
    };
  }

  function buildExecutionErrorRun(reqPlan: any, message: string) {
    const msg = String(message || 'Неизвестная ошибка');
    const fallbackRequest =
      reqPlan && typeof reqPlan === 'object'
        ? {
            method: String(reqPlan?.method || 'GET').toUpperCase(),
            url: String(reqPlan?.url || ''),
            headers: normalizeRequestHeaders(reqPlan?.headers || {}),
            query: {},
            body: reqPlan?.body && typeof reqPlan.body === 'object' ? deepClone(reqPlan.body) : reqPlan?.body
          }
        : null;
    const entry: Record<string, any> = {
      page: 1,
      status: 0,
      error: msg,
      decision: 'fail',
      stop_reason: `Ошибка выполнения: ${msg}`,
      iteration_reason: 'initial_request'
    };
    if (fallbackRequest) entry.request = fallbackRequest;
    if (reqPlan?.group) entry.group = reqPlan.group;
    if (reqPlan?.row_index) entry.row_index = reqPlan.row_index;
    return {
      requestCount: 1,
      success: 0,
      failed: 1,
      totalItems: 0,
      totalSize: msg.length,
      lastStatus: 0,
      stopReason: `Ошибка выполнения: ${msg}`,
      responses: [entry]
    };
  }

  function runIdForExecution() {
    return `api_run_${Date.now()}_${Math.random().toString(16).slice(2, 10)}`;
  }

  function buildAuditRowsFromResponses(
    runId: string,
    draft: ApiDraft,
    reqPlan: any,
    responses: any[],
    entityIndex: number
  ) {
    const dispatchMode = isGroupedDispatchEnabled(draft) ? 'group_by' : 'single';
    const identity = runEntityIdentity(reqPlan, entityIndex);
    const logMode = (draft.responseLogMode || 'standard') as ResponseLogMode;
    const includeRequestPayload =
      logMode === 'debug' ? true : logMode === 'minimal' ? false : Boolean(draft.responseLogWriteRequestPayload);
    const includeResponsePayload =
      logMode === 'debug' ? true : logMode === 'minimal' ? false : Boolean(draft.responseLogWriteResponsePayload);
    const includePaginationValues =
      logMode === 'debug' ? true : logMode === 'minimal' ? false : Boolean(draft.responseLogWritePaginationValues);
    const onlyErrors = Boolean(draft.responseLogOnlyErrors);
    const responsePayloadLimit = Math.max(0, Number(draft.responseLogResponsePayloadLimit || 0));
    const rows: any[] = [];
    const list = Array.isArray(responses) ? responses : [];
    for (let i = 0; i < list.length; i += 1) {
      const entry = list[i] && typeof list[i] === 'object' ? list[i] : {};
      const statusCode = Number.isFinite(Number(entry?.status)) ? Number(entry?.status) : null;
      const hasError = Boolean(String(entry?.error || '').trim()) || (typeof statusCode === 'number' && statusCode >= 400);
      if (onlyErrors && !hasError) continue;
      const requestPayload =
        entry?.request && typeof entry.request === 'object'
          ? entry.request
          : reqPlan && typeof reqPlan === 'object'
          ? {
              method: String(reqPlan?.method || draft.method || 'GET').toUpperCase(),
              url: String(reqPlan?.url || ''),
              headers: normalizeRequestHeaders(reqPlan?.headers || {}),
              body: reqPlan?.body && typeof reqPlan.body === 'object' ? deepClone(reqPlan.body) : reqPlan?.body
            }
          : null;
      let responsePayload: any = entry?.response ?? null;
      if (includeResponsePayload && responsePayloadLimit > 0) {
        try {
          const raw = typeof responsePayload === 'string' ? responsePayload : JSON.stringify(responsePayload);
          if (typeof raw === 'string' && raw.length > responsePayloadLimit) {
            responsePayload =
              typeof responsePayload === 'string'
                ? `${raw.slice(0, responsePayloadLimit)}...`
                : {
                    __truncated: true,
                    preview: raw.slice(0, responsePayloadLimit),
                    total_chars: raw.length
                  };
          }
        } catch {
          // keep original response payload when serialization fails
        }
      }
      rows.push({
        run_id: runId,
        api_name: String(draft?.name || '').trim(),
        execution_mode: draft.executionMode || 'sync',
        sync_planner: draft.syncPlanner || 'entity_to_stop',
        execution_delay_ms: Math.max(0, Number(draft.executionDelayMs || 0)),
        dispatch_mode: dispatchMode,
        entity_key: identity.entity_key,
        entity_label: identity.entity_label,
        row_index: Number(reqPlan?.row_index || entry?.row_index || 0) || null,
        wave_no: Number(entry?.wave || 0) || null,
        page_no: Number(entry?.page || i + 1) || i + 1,
        iteration_reason: String(entry?.iteration_reason || '').trim() || (i === 0 ? 'initial_request' : 'pagination_values_updated'),
        decision: String(entry?.decision || '').trim() || (hasError ? 'fail' : 'continue'),
        stop_reason: String(entry?.stop_reason || '').trim() || null,
        error_message: String(entry?.error || '').trim() || null,
        status_code: statusCode,
        request_payload: includeRequestPayload ? requestPayload : null,
        response_payload: includeResponsePayload ? responsePayload : null,
        pagination_values:
          includePaginationValues && entry?.pagination_values && typeof entry.pagination_values === 'object'
            ? entry.pagination_values
            : {},
        duration_ms: Number.isFinite(Number(entry?.duration_ms)) ? Number(entry?.duration_ms) : null,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      });
    }
    return rows;
  }

  async function writeExecutionAuditRows(draft: ApiDraft, rows: any[]) {
    if (!draft.responseLogEnabled) return { written: 0, skipped: rows.length };
    const schema = String(draft.responseLogSchema || '').trim();
    const table = String(draft.responseLogTable || '').trim();
    if (!schema || !table) {
      throw new Error('Для лога ответов API укажи схему и таблицу');
    }
    if (!isConnectedTable(schema, table)) {
      throw new Error('Для лога ответов выбери таблицу из подключённых');
    }
    const compatibility = await checkResponseLogTableCompatibility(schema, table);
    if (!compatibility.ok) {
      throw new Error(`Таблица лога не соответствует системному шаблону: ${compatibility.reason}`);
    }
    const payloadRows = Array.isArray(rows) ? rows.filter((x) => x && typeof x === 'object') : [];
    if (!payloadRows.length) return { written: 0, skipped: 0 };
    const resp = await apiJson<{ ok?: boolean; inserted?: number; skipped?: number }>(`${apiBase}/api-execution-log/write`, {
      method: 'POST',
      headers: headers(),
      body: JSON.stringify({
        schema,
        table,
        rows: payloadRows
      })
    });
    return {
      written: Number(resp?.inserted || 0),
      skipped: Number(resp?.skipped || 0)
    };
  }

  async function executePlanRequests(
    draft: ApiDraft,
    allRequests: any[],
    modeLabel: string,
    issues?: Record<string, string>
  ): Promise<ExecutionRunSummary> {
    const sentRequests: any[] = [];
    const responses: any[] = [];
    const stopReasons: any[] = [];
    const runId = runIdForExecution();
    const runIssues: Record<string, string> = { ...(issues || {}) };
    let requestCount = 0;
    let success = 0;
    let failed = 0;
    let totalItems = 0;
    let totalSize = 0;
    let lastStatus = 0;
    let responsesDropped = 0;
    let auditWritten = 0;
    let auditSkipped = 0;
    let auditError = '';
    const executionDelayMs = Math.max(0, Number(draft.executionDelayMs || 0));
    let scheduledStepsCount = 0;

    const registerIssue = (key: string, value: string) => {
      const k = String(key || '').trim();
      const v = String(value || '').trim();
      if (!k || !v) return;
      if (!runIssues[k]) runIssues[k] = v;
    };

    const toPreviewEntry = (entry: any) => {
      if (!entry || typeof entry !== 'object') return entry;
      const preview = deepClone(entry);
      const limit = Math.max(10_000, Number(EXECUTION_RESPONSE_ENTRY_MAX_CHARS || 120000));
      if (typeof preview.error === 'string' && preview.error.length > limit) {
        preview.error = `${preview.error.slice(0, limit)}...`;
        preview.error_truncated = true;
      }
      if ('response' in preview) {
        try {
          const raw = preview.response;
          const text = typeof raw === 'string' ? raw : JSON.stringify(raw);
          if (text.length > limit) {
            preview.response = typeof raw === 'string'
              ? `${text.slice(0, limit)}...`
              : {
                  __truncated: true,
                  preview: text.slice(0, limit),
                  total_chars: text.length
                };
            preview.response_truncated = true;
            preview.response_total_chars = text.length;
          }
        } catch {
          // keep original payload in preview if serialization failed
        }
      }
      return preview;
    };

    const pushResponsePreview = (entry: any) => {
      if (responses.length < EXECUTION_RESPONSE_PREVIEW_MAX) {
        responses.push(toPreviewEntry(entry));
      } else {
        responsesDropped += 1;
      }
    };

    const writeAuditChunk = async (reqPlan: any, chunk: any[], idx: number) => {
      if (!draft.responseLogEnabled) return;
      const entries = Array.isArray(chunk) ? chunk.filter((x) => x && typeof x === 'object') : [];
      if (!entries.length) return;
      for (let offset = 0; offset < entries.length; offset += EXECUTION_AUDIT_FLUSH_BATCH) {
        const batchEntries = entries.slice(offset, offset + EXECUTION_AUDIT_FLUSH_BATCH);
        const rows = buildAuditRowsFromResponses(runId, draft, reqPlan, batchEntries, idx);
        if (!rows.length) continue;
        try {
          const wr = await writeExecutionAuditRows(draft, rows);
          auditWritten += Number(wr?.written || 0);
          auditSkipped += Number(wr?.skipped || 0);
        } catch (e: any) {
          auditSkipped += rows.length;
          const msg = String(e?.message || e || 'Не удалось записать лог выполнения');
          if (!auditError) auditError = msg;
          registerIssue('audit_log_error', msg);
        }
      }
    };

    const absorbEntry = async (reqPlan: any, entry: any, idx: number) => {
      if (!entry || typeof entry !== 'object') return;
      pushResponsePreview(entry);
      const status = Number(entry?.status || 0);
      requestCount += 1;
      if (status >= 200 && status < 400) success += 1;
      else failed += 1;
      if (Number.isFinite(status) && status > 0) lastStatus = status;
      totalItems += countPayloadItems(entry?.response ?? entry?.error ?? null);
      const entrySize = Number(entry?.response_size);
      if (Number.isFinite(entrySize) && entrySize >= 0) {
        totalSize += entrySize;
      } else {
        totalSize += JSON.stringify(entry?.response ?? entry?.error ?? '').length;
      }
      await writeAuditChunk(reqPlan, [entry], idx);
    };

    const registerStopReason = (reqPlan: any, run: any) => {
      stopReasons.push({
        ...(reqPlan?.group ? { group: reqPlan.group } : {}),
        ...(reqPlan?.row_index ? { row_index: reqPlan.row_index } : {}),
        stop_reason: run?.stopReason || '',
        requests: Number(run?.requestCount || 0),
        success: Number(run?.success || 0),
        failed: Number(run?.failed || 0),
        last_status: Number(run?.lastStatus || 0)
      });
    };

    const planner = draft.executionMode === 'sync' ? draft.syncPlanner || 'entity_to_stop' : 'entity_to_stop';
    if (draft.executionMode === 'sync' && planner === 'by_wave') {
      const states = allRequests.map((reqPlan) => initWaveRuntimeState(draft, reqPlan));
      const finishedStateIds = new Set<number>();
      let waveNo = 1;
      while (states.some((state) => !state.done)) {
        for (let idx = 0; idx < states.length; idx += 1) {
          const state = states[idx];
          if (!state || state.done) continue;
          if (executionDelayMs > 0 && scheduledStepsCount > 0) {
            await new Promise((resolve) => setTimeout(resolve, executionDelayMs));
          }
          scheduledStepsCount += 1;
          try {
            const step = await executeWaveRuntimeStep(draft, state, sentRequests, waveNo);
            if (step?.entry) {
              await absorbEntry(state.reqPlan, step.entry, idx);
            }
            if (step?.done && !finishedStateIds.has(idx)) {
              finishedStateIds.add(idx);
              registerStopReason(state.reqPlan, {
                stopReason: step.stopReason || state.stopReason || '',
                requestCount: state.requestCount,
                success: state.success,
                failed: state.failed,
                lastStatus: state.lastStatus
              });
            }
          } catch (e: any) {
            const fallback = buildExecutionErrorRun(state.reqPlan, e?.message ?? String(e));
            for (const entry of fallback.responses || []) {
              await absorbEntry(state.reqPlan, entry, idx);
            }
            if (!finishedStateIds.has(idx)) {
              finishedStateIds.add(idx);
              registerStopReason(state.reqPlan, fallback);
            }
            state.done = true;
            state.stopReason = fallback.stopReason || 'Ошибка выполнения';
          }
        }
        waveNo += 1;
      }
    } else if (draft.executionMode === 'async') {
      const limit = Math.max(1, Math.min(20, Number(draft.asyncConcurrency || 3)));
      let cursor = 0;
      const worker = async () => {
        let workerStepCount = 0;
        while (true) {
          const idx = cursor;
          cursor += 1;
          if (idx >= allRequests.length) return;
          if (executionDelayMs > 0 && workerStepCount > 0) {
            await new Promise((resolve) => setTimeout(resolve, executionDelayMs));
          }
          workerStepCount += 1;
          const reqPlan = allRequests[idx];
          let run: any;
          try {
            run = await executePlannedRequestWithPagination(draft, reqPlan, sentRequests, async (entry) => {
              await absorbEntry(reqPlan, entry, idx);
            });
          } catch (e: any) {
            run = buildExecutionErrorRun(reqPlan, e?.message ?? String(e));
            for (const entry of run.responses || []) {
              await absorbEntry(reqPlan, entry, idx);
            }
          }
          registerStopReason(reqPlan, run);
        }
      };
      const workers = Array.from({ length: Math.min(limit, Math.max(1, allRequests.length)) }, () => worker());
      await Promise.all(workers);
    } else {
      for (let idx = 0; idx < allRequests.length; idx += 1) {
        if (executionDelayMs > 0 && idx > 0) {
          await new Promise((resolve) => setTimeout(resolve, executionDelayMs));
        }
        const reqPlan = allRequests[idx];
        let run: any;
        try {
          run = await executePlannedRequestWithPagination(draft, reqPlan, sentRequests, async (entry) => {
            await absorbEntry(reqPlan, entry, idx);
          });
        } catch (e: any) {
          run = buildExecutionErrorRun(reqPlan, e?.message ?? String(e));
          for (const entry of run.responses || []) {
            await absorbEntry(reqPlan, entry, idx);
          }
        }
        registerStopReason(reqPlan, run);
      }
    }

    if (responsesDropped > 0) {
      registerIssue(
        'responses_truncated',
        `limit_reached: в UI сохранены первые ${EXECUTION_RESPONSE_PREVIEW_MAX} ответов, отброшено ${responsesDropped}`
      );
    }

    return {
      requestCount,
      success,
      failed,
      totalItems,
      totalSize,
      lastStatus,
      responses,
      stopReasons,
      sentRequests,
      issues: Object.keys(runIssues).length ? runIssues : undefined,
      modeLabel,
      responsesDropped,
      auditLog: {
        written: auditWritten,
        skipped: auditSkipped,
        ...(auditError ? { error: auditError } : {})
      }
    };
  }

  async function checkApiNow() {
    err = '';
    ok = '';
    myPreviewApplyMessage = '';
    responseStatus = 0;
    responseText = '';
    responseTimeMs = 0;
    responsePayloadCount = 0;
    responsePagesCount = 0;
    responsePayloadSize = 0;
    if (!selected) {
      err = 'Выбери API';
      return;
    }
    checking = true;
    try {
      applyUrlInput(requestInput);
      const s = byRef(selectedRef) || selected;
      if (isGroupedDispatchEnabled(s)) {
        const groupedPlan = await buildGroupedRequestPlan(s);
        if (!groupedPlan.allRequests.length) {
          throw new Error('Не удалось сформировать запросы для отправки');
        }

        const startedAt = Date.now();
        const execution = await executePlanRequests(s, groupedPlan.allRequests, 'sent_grouped_requests', groupedPlan.issues);
        const auditLog = execution.auditLog || { written: 0, skipped: 0 };

        myApiPreview = JSON.stringify(
          {
            mode: execution.modeLabel,
            execution_mode: s.executionMode || 'sync',
            sync_planner: s.executionMode === 'sync' ? s.syncPlanner || 'entity_to_stop' : undefined,
            async_concurrency: s.executionMode === 'async' ? Math.max(1, Number(s.asyncConcurrency || 3)) : undefined,
            execution_delay_ms: Math.max(0, Number(s.executionDelayMs || 0)),
            total_requests: execution.requestCount,
            request_groups: groupedPlan.allRequests.length,
            shown_requests: execution.sentRequests.length,
            requests: execution.sentRequests,
            issues:
              Object.keys({ ...(groupedPlan.issues || {}), ...(execution.issues || {}) }).length
                ? { ...(groupedPlan.issues || {}), ...(execution.issues || {}) }
                : undefined
          },
          null,
          2
        );
        myPreviewDirty = false;
        myPreviewApplyMessage =
          execution.requestCount > execution.sentRequests.length
            ? `Показаны первые ${execution.sentRequests.length} отправленных запросов из ${execution.requestCount}`
            : execution.requestCount > 1
            ? `Показаны отправленные запросы: ${execution.requestCount}`
            : 'Показан отправленный запрос';

        responseStatus = execution.lastStatus;
        responsePagesCount = execution.requestCount;
        responsePayloadCount = execution.totalItems;
        responsePayloadSize = execution.totalSize;
        responseTimeMs = Date.now() - startedAt;
        const groupedResponsePayload = {
            total_requests: execution.requestCount,
            request_groups: groupedPlan.allRequests.length,
            execution_mode: s.executionMode || 'sync',
            sync_planner: s.executionMode === 'sync' ? s.syncPlanner || 'entity_to_stop' : undefined,
            async_concurrency: s.executionMode === 'async' ? Math.max(1, Number(s.asyncConcurrency || 3)) : undefined,
            execution_delay_ms: Math.max(0, Number(s.executionDelayMs || 0)),
            success: execution.success,
            failed: execution.failed,
            stop_reasons: execution.stopReasons,
            responses: execution.responses,
            responses_dropped: execution.responsesDropped || 0,
            issues:
              Object.keys({ ...(groupedPlan.issues || {}), ...(execution.issues || {}) }).length
                ? { ...(groupedPlan.issues || {}), ...(execution.issues || {}) }
                : undefined,
            audit_log: s.responseLogEnabled
              ? {
                  schema: s.responseLogSchema,
                  table: s.responseLogTable,
                  written: auditLog.written,
                  skipped: auditLog.skipped,
                  error: auditLog.error || undefined
                }
              : undefined
          };
        responseText = JSON.stringify(groupedResponsePayload, null, 2);
        apiResponseCache = { ...apiResponseCache, [refOf(s)]: groupedResponsePayload };
        ok = execution.failed
          ? `Проверка завершена: успех ${execution.success}, ошибок ${execution.failed}`
          : `Проверка выполнена, запросов: ${execution.success}`;
        if (auditLog.error) {
          ok += ` | Лог не записан: ${auditLog.error}`;
        } else if (s.responseLogEnabled) {
          ok += ` | Лог записан: ${auditLog.written}`;
        }
        return;
      }
      const rowPlan = await buildUngroupedRowRequestPlan(s);
      if (rowPlan.allRequests.length) {
        const startedAt = Date.now();
        const execution = await executePlanRequests(s, rowPlan.allRequests, 'sent_row_requests', rowPlan.issues);
        const auditLog = execution.auditLog || { written: 0, skipped: 0 };

        myApiPreview = JSON.stringify(
          {
            mode: execution.modeLabel,
            execution_mode: s.executionMode || 'sync',
            sync_planner: s.executionMode === 'sync' ? s.syncPlanner || 'entity_to_stop' : undefined,
            async_concurrency: s.executionMode === 'async' ? Math.max(1, Number(s.asyncConcurrency || 3)) : undefined,
            execution_delay_ms: Math.max(0, Number(s.executionDelayMs || 0)),
            total_requests: execution.requestCount,
            request_rows: rowPlan.allRequests.length,
            shown_requests: execution.sentRequests.length,
            requests: execution.sentRequests,
            issues:
              Object.keys({ ...(rowPlan.issues || {}), ...(execution.issues || {}) }).length
                ? { ...(rowPlan.issues || {}), ...(execution.issues || {}) }
                : undefined
          },
          null,
          2
        );
        myPreviewDirty = false;
        myPreviewApplyMessage =
          execution.requestCount > execution.sentRequests.length
            ? `Показаны первые ${execution.sentRequests.length} отправленных запросов из ${execution.requestCount}`
            : execution.requestCount > 1
            ? `Показаны отправленные запросы: ${execution.requestCount}`
            : 'Показан отправленный запрос';

        responseStatus = execution.lastStatus;
        responsePagesCount = execution.requestCount;
        responsePayloadCount = execution.totalItems;
        responsePayloadSize = execution.totalSize;
        responseTimeMs = Date.now() - startedAt;
        const rowResponsePayload = {
            total_requests: execution.requestCount,
            request_rows: rowPlan.allRequests.length,
            execution_mode: s.executionMode || 'sync',
            sync_planner: s.executionMode === 'sync' ? s.syncPlanner || 'entity_to_stop' : undefined,
            async_concurrency: s.executionMode === 'async' ? Math.max(1, Number(s.asyncConcurrency || 3)) : undefined,
            execution_delay_ms: Math.max(0, Number(s.executionDelayMs || 0)),
            success: execution.success,
            failed: execution.failed,
            stop_reasons: execution.stopReasons,
            responses: execution.responses,
            responses_dropped: execution.responsesDropped || 0,
            issues:
              Object.keys({ ...(rowPlan.issues || {}), ...(execution.issues || {}) }).length
                ? { ...(rowPlan.issues || {}), ...(execution.issues || {}) }
                : undefined,
            audit_log: s.responseLogEnabled
              ? {
                  schema: s.responseLogSchema,
                  table: s.responseLogTable,
                  written: auditLog.written,
                  skipped: auditLog.skipped,
                  error: auditLog.error || undefined
                }
              : undefined
          };
        responseText = JSON.stringify(rowResponsePayload, null, 2);
        apiResponseCache = { ...apiResponseCache, [refOf(s)]: rowResponsePayload };
        ok = execution.failed
          ? `Проверка завершена: успех ${execution.success}, ошибок ${execution.failed}`
          : `Проверка выполнена, запросов: ${execution.success}`;
        if (auditLog.error) {
          ok += ` | Лог не записан: ${auditLog.error}`;
        } else if (s.responseLogEnabled) {
          ok += ` | Лог записан: ${auditLog.written}`;
        }
        return;
      }
      const authHdr = parseJsonObjectField('Авторизация', s.authJson);
      const hdr = parseJsonObjectField('Headers JSON', s.headersJson);
      const queryObjBase = parseJsonObjectField('Query JSON', s.queryJson);
      const bodyBaseRaw = parseJsonAnyField('Body JSON', s.bodyJson);
      const bodyObjBase = bodyBaseRaw && typeof bodyBaseRaw === 'object' ? JSON.parse(JSON.stringify(bodyBaseRaw)) : {};

      const sanitizedAliases = sanitizeAliasReferences(s);
      const definitionAliases = uniqueAliasList(
        (Array.isArray(s.parameterDefinitions) ? s.parameterDefinitions : [])
          .map((p) => String(p?.alias || '').trim())
          .filter(Boolean)
      );
      const dataModelAliases = uniqueAliasList(
        (Array.isArray(s.dataFields) ? s.dataFields : [])
          .map((f) => String(f?.alias || '').trim())
          .filter(Boolean)
      );
      const dateModelAliases = uniqueAliasList(
        dataDateParametersForDraft(s)
          .map((p) => String(p?.alias || '').trim())
          .filter(Boolean)
      );
      const apiModelAliases = uniqueAliasList(
        dataApiParametersForDraft(s)
          .map((p) => String(p?.alias || '').trim())
          .filter(Boolean)
      );
      const paginationAliasesLower = paginationAliasLowerSet(s);
      const oauth2ProvidedAliasesLower = new Set(
        (s.authMode === AUTH_MODE_OAUTH2 ? oauth2MappingsForDraft(s) : [])
          .map((m) => String(m.alias || '').trim().toLowerCase())
          .filter(Boolean)
      );
      const knownAliasesLower = new Set(
        [...definitionAliases, ...dataModelAliases, ...dateModelAliases, ...apiModelAliases, ...Array.from(paginationAliasesLower), ...Array.from(oauth2ProvidedAliasesLower)].map((x) =>
          x.toLowerCase()
        )
      );

      const requestBase = `${s.baseUrl.replace(/\/$/, '')}${s.path.startsWith('/') ? s.path : `/${s.path}`}`;
      const initialTokens = new Set<string>();
      collectParameterTokens(requestBase, initialTokens);
      collectParameterTokens(authHdr, initialTokens);
      collectParameterTokens(hdr, initialTokens);
      collectParameterTokens(queryObjBase, initialTokens);
      collectParameterTokens(bodyObjBase, initialTokens);
      const requestedAliases = excludePaginationAliases(
        [...Array.from(initialTokens), ...sanitizedAliases.bindingRules.map((r) => r.alias), ...oauth2RequestAliases(s)],
        paginationAliasesLower
      ).filter((alias) => !oauth2ProvidedAliasesLower.has(String(alias || '').trim().toLowerCase()));
      const { map: parameterValuesRaw, issues: parameterIssues } = await resolveRuntimeAliasValues(s, requestedAliases);
      let tokenRequestPreview: any = null;
      let tokenResponsePreview: any = null;
      let tokenResolvedAliases: Record<string, any> = {};
      if (s.authMode === 'oauth2_client_credentials') {
        const tokenData = await getOAuthToken(s, parameterValuesRaw || {});
        tokenRequestPreview = tokenData.requestPreview;
        tokenResponsePreview = tokenData.responsePreview;
        tokenResolvedAliases = tokenData.mappedAliases || {};
      }
      const parameterValues = { ...(parameterValuesRaw || {}), ...tokenResolvedAliases };
      const hasParameterValues = Object.keys(parameterValues).length > 0;
      if (hasParameterValues) {
        applyParametersToValue(authHdr, parameterValues);
        applyParametersToValue(hdr, parameterValues);
        applyParametersToValue(queryObjBase, parameterValues);
        applyParametersToValue(bodyObjBase, parameterValues);
      }
      applyBindingRulesToRequest(sanitizedAliases.bindingRules, parameterValues, authHdr, hdr, queryObjBase, bodyObjBase);

      const startTime = Date.now();
      let url = `${s.baseUrl.replace(/\/$/, '')}${s.path.startsWith('/') ? s.path : `/${s.path}`}`;
      if (hasParameterValues) {
        url = replaceParameterTokens(url, parameterValues);
      }
      const u = new URL(url);
      Object.entries(queryObjBase || {}).forEach(([k, v]) => u.searchParams.set(k, String(v)));
      url = u.toString();

      const unresolvedTokens = new Set<string>();
      collectParameterTokens(url, unresolvedTokens);
      collectParameterTokens(authHdr, unresolvedTokens);
      collectParameterTokens(hdr, unresolvedTokens);
      collectParameterTokens(queryObjBase, unresolvedTokens);
      collectParameterTokens(bodyObjBase, unresolvedTokens);
      const unresolvedRelevant = Array.from(unresolvedTokens).filter(
        (alias) => !paginationAliasesLower.has(String(alias || '').trim().toLowerCase())
      );
      if (unresolvedRelevant.length) {
        const detail = unresolvedRelevant.map((alias) => {
          const issueByAlias = findAliasInMap(parameterIssues, alias);
          if (issueByAlias.found) return `${alias} (${issueByAlias.value})`;
          if (!knownAliasesLower.has(alias.toLowerCase())) return `${alias} (нет параметра с таким alias)`;
          return `${alias} (значение не вычислено)`;
        });
        throw new Error(`Не удалось подставить параметры: ${detail.join(', ')}`);
      }

      const reqPlan = {
        method: s.method,
        url,
        headers: { 'Content-Type': 'application/json', ...authHdr, ...hdr },
        body: s.method === 'GET' || s.method === 'DELETE' ? undefined : bodyObjBase,
        context_values: deepClone(parameterValues || {})
      };
      const execution = await executePlanRequests(s, [reqPlan], 'sent_single_request');
      const auditLog = execution.auditLog || { written: 0, skipped: 0 };
      const pageCounter = execution.requestCount;
      const pagePayloads = execution.responses.map((entry) => entry?.response);
      const totalSize = execution.totalSize;
      const lastStatus = Number(execution.lastStatus || 0);

      const sentPreviewPayload =
        pageCounter <= 1
          ? {
              request: execution.sentRequests[0] || null,
              resolved_parameters: parameterValues,
              parameter_issues: Object.keys(parameterIssues).length ? parameterIssues : undefined,
              token_request: tokenRequestPreview || undefined,
              token_response: tokenResponsePreview || undefined,
              token_mapped_aliases: Object.keys(tokenResolvedAliases).length ? tokenResolvedAliases : undefined
            }
          : {
              requests: execution.sentRequests,
              request_count: pageCounter,
              shown_requests: execution.sentRequests.length,
              truncated: pageCounter > execution.sentRequests.length,
              resolved_parameters: parameterValues,
              parameter_issues: Object.keys(parameterIssues).length ? parameterIssues : undefined,
              execution_issues: execution.issues,
              token_request: tokenRequestPreview || undefined,
              token_response: tokenResponsePreview || undefined,
              token_mapped_aliases: Object.keys(tokenResolvedAliases).length ? tokenResolvedAliases : undefined
            };
      myApiPreview = JSON.stringify(sentPreviewPayload, null, 2);
      myPreviewDirty = false;
      myPreviewApplyMessage =
        pageCounter > REQUEST_PREVIEW_MAX
          ? `Показаны первые ${REQUEST_PREVIEW_MAX} отправленных запросов из ${pageCounter}`
          : pageCounter > 1
          ? `Показаны отправленные запросы по страницам: ${pageCounter}`
          : 'Показан последний отправленный запрос';

      responseStatus = lastStatus;
      const singleResponsePayload = {
          total_requests: execution.requestCount,
          execution_mode: s.executionMode || 'sync',
          sync_planner: s.executionMode === 'sync' ? s.syncPlanner || 'entity_to_stop' : undefined,
          async_concurrency: s.executionMode === 'async' ? Math.max(1, Number(s.asyncConcurrency || 3)) : undefined,
          execution_delay_ms: Math.max(0, Number(s.executionDelayMs || 0)),
          success: execution.success,
          failed: execution.failed,
          stop_reasons: execution.stopReasons,
          responses: execution.responses,
          responses_dropped: execution.responsesDropped || 0,
          issues: execution.issues,
          payloads_only: pagePayloads,
          audit_log: s.responseLogEnabled
            ? {
                schema: s.responseLogSchema,
                table: s.responseLogTable,
                written: auditLog.written,
                skipped: auditLog.skipped,
                error: auditLog.error || undefined
              }
            : undefined
        };
      responseText = JSON.stringify(singleResponsePayload, null, 2);
      apiResponseCache = { ...apiResponseCache, [refOf(s)]: singleResponsePayload };
      responsePagesCount = pagePayloads.length;
      responsePayloadCount = execution.totalItems;
      responsePayloadSize = totalSize;
      responseTimeMs = Date.now() - startTime;
      ok =
        execution.failed > 0
          ? `Проверка завершена: успех ${execution.success}, ошибок ${execution.failed}`
          : s.paginationEnabled
          ? `Проверка выполнена, страниц: ${pageCounter}`
          : 'Проверка выполнена';
      if (auditLog.error) {
        ok += ` | Лог не записан: ${auditLog.error}`;
      } else if (s.responseLogEnabled) {
        ok += ` | Лог записан: ${auditLog.written}`;
      }
    } catch (e: any) {
      err = toUiErrorMessage(e);
    } finally {
      checking = false;
    }
  }

  $: {
    drafts;
    selected = byRef(selectedRef);
  }
  $: {
    const items: TemplateListItem[] = drafts.map((draft) => {
      const ref = refOf(draft);
      const section = sectionForDraft(draft);
      const method = toHttpMethod(String(draft?.method || 'GET').toUpperCase());
      const path = shortPathForDraft(draft);
      const templateCode = templateCodeForDraft(draft);
      const searchText = [
        draft?.name,
        draft?.description,
        path,
        method,
        templateCode
      ]
        .map((x) => String(x || '').toLowerCase())
        .join(' ');
      return { ref, draft, section, method, path, templateCode, searchText };
    });

    templateListItems = items;
    templateScopeCounts = {
      all: items.length,
      recent: items.filter((item) => templateRecentRefs.includes(item.ref)).length,
      favorites: items.filter((item) => templateFavoriteRefs.includes(item.ref)).length
    };

    const sections = [...new Set(items.map((item) => item.section).filter(Boolean))];
    templateSectionOptions = sections.sort((a, b) => a.localeCompare(b, 'ru', { sensitivity: 'base' }));
    if (templateSectionFilter !== 'all' && !templateSectionOptions.includes(templateSectionFilter)) {
      templateSectionFilter = 'all';
    }
  }
  $: {
    let items = [...templateListItems];
    if (templateScopeFilter === 'recent') {
      items = orderTemplateItemsByRefList(
        items.filter((item) => templateRecentRefs.includes(item.ref)),
        templateRecentRefs
      );
    } else if (templateScopeFilter === 'favorites') {
      items = orderTemplateItemsByRefList(
        items.filter((item) => templateFavoriteRefs.includes(item.ref)),
        templateFavoriteRefs
      );
    }

    if (templateMethodFilter !== 'all') {
      items = items.filter((item) => item.method === templateMethodFilter);
    }

    if (templateSectionFilter !== 'all') {
      items = items.filter((item) => item.section === templateSectionFilter);
    }

    const q = String(templateSearch || '').trim().toLowerCase();
    if (q) {
      items = items.filter((item) => item.searchText.includes(q));
    }

    templateFilteredItems = items;
  }
  $: {
    const map = new Map<string, TemplateListItem[]>();
    for (const item of templateFilteredItems) {
      const key = item.section || 'Без раздела';
      const group = map.get(key);
      if (group) group.push(item);
      else map.set(key, [item]);
    }
    templateGroupedItems = [...map.entries()]
      .sort((a, b) => a[0].localeCompare(b[0], 'ru', { sensitivity: 'base' }))
      .map(([section, items]) => ({ section, items }));
  }
  $: {
    if (!templateListItems.length) {
      templateListEmptyHint = 'Список шаблонов пуст.';
    } else if (!templateFilteredItems.length) {
      templateListEmptyHint = 'По текущему поиску и фильтрам ничего не найдено.';
    } else {
      templateListEmptyHint = '';
    }
  }
  $: if (selectedRef !== lastSelectedRef) {
    lastSelectedRef = selectedRef;
    if (selected) {
      markTemplateRecent(selectedRef);
      nameDraft = selected.name;
      selectedBaselineSignature = selectedDraftSignature(selected, selected.name);
      requestInput = `${selected.baseUrl.replace(/\/$/, '')}${selected.path.startsWith('/') ? selected.path : `/${selected.path}`}`;
      myPreviewDirty = false;
      myPreviewApplyMessage = '';
      myApiPreview = '';
      myApiPreviewDraft = '';
      response_log_picker_open = false;
      response_log_pick_value = responseLogQualifiedTable(selected);
      activeResponseFieldRef = '';
      datasetPreviewRows = [];
      datasetPreviewColumns = [];
      datasetPreviewHasMore = false;
      datasetPreviewError = '';
    } else {
      selectedBaselineSignature = '';
    }
  }
  $: {
    const effectiveName = String(nameDraft || selected?.name || '').trim();
    const currentSignature = selectedDraftSignature(selected, effectiveName);
    canAddTemplate = Boolean(effectiveName) && !saving;
    canSaveSelected = Boolean(selected) && Boolean(currentSignature) && currentSignature !== selectedBaselineSignature && !saving;
  }
  $: if (!myPreviewDirty) {
    myApiPreviewDraft = myApiPreview;
  }
  $: if (selected?.responseLogEnabled) {
    const currentSchema = String(selected.responseLogSchema || '').trim();
    const currentTable = String(selected.responseLogTable || '').trim();
    if (existingTables.length && !isConnectedTable(currentSchema, currentTable)) {
      const fallback = firstCompatibleResponseLogTable() || existingTables[0];
      if (fallback) {
        mutateSelected((d) => {
          d.responseLogSchema = String(fallback.schema_name || '').trim();
          d.responseLogTable = String(fallback.table_name || '').trim();
        });
      }
    }
  }
  $: if (existingTables.length) {
    void refreshResponseLogTableCompatibility();
  }
  $: {
    const txt = String(responseText || '').trim();
    if (!txt) {
      responseIsJson = false;
      responseJson = null;
    } else {
      try {
        responseJson = JSON.parse(txt);
        responseIsJson = true;
      } catch {
        responseJson = null;
        responseIsJson = false;
      }
    }
  }
  $: {
    const out: string[] = [];
    if (responseIsJson) collectResponsePaths(responseJson, '', out);
    responsePathOptions = [...new Set(out)].filter(Boolean);
    if (!responsePathOptions.includes(responsePathPick)) {
      responsePathPick = responsePathOptions[0] || '';
    }
  }
  $: {
    const scalar = new Set<string>();
    if (responseIsJson) collectScalarResponsePaths(responseJson, '', scalar);
    const all = Array.from(scalar).filter(Boolean);
    const cursorFirst = all.filter((p) => p.toLowerCase().includes('cursor'));
    const rest = all.filter((p) => !p.toLowerCase().includes('cursor'));
    paginationCursorResponsePathOptions = [...new Set([...cursorFirst, ...rest])];
    const activePath = String(activePaginationParameter?.responsePath || '').trim();
    if (activePath && paginationCursorResponsePathOptions.includes(activePath)) {
      paginationCursorResponsePick = activePath;
    } else if (!paginationCursorResponsePathOptions.includes(paginationCursorResponsePick)) {
      paginationCursorResponsePick = paginationCursorResponsePathOptions[0] || '';
    }
  }
  $: if (
    activePaginationParameter &&
    !String(activePaginationParameter.responsePath || '').trim() &&
    String(paginationCursorResponsePick || '').trim()
  ) {
    updatePaginationParameter(activePaginationParameter.id, { responsePath: paginationCursorResponsePick });
  }
  $: {
    const txt = unwrapCodeFence(String(selected?.exampleRequest || '')).trim();
    if (!txt) {
      exampleIsJson = false;
      exampleJson = null;
    } else {
      try {
        exampleJson = JSON.parse(txt);
        exampleIsJson = true;
      } catch {
        exampleJson = null;
        exampleIsJson = false;
      }
    }
  }
  $: {
    const txt = String(myApiPreviewDraft || '').trim();
    if (!txt) {
      myPreviewIsJson = false;
      myPreviewJson = null;
    } else {
      try {
        myPreviewJson = JSON.parse(txt);
        myPreviewIsJson = true;
      } catch {
        myPreviewJson = null;
        myPreviewIsJson = false;
      }
    }
  }
  $: {
    const txt = String(selected?.authJson || '').trim();
    if (!txt) {
      authJsonValid = false;
      authJsonTree = null;
    } else {
      try {
        authJsonTree = parseJsonObjectField('Авторизация', txt);
        authJsonValid = true;
      } catch {
        authJsonTree = null;
        authJsonValid = false;
      }
    }
  }
  $: {
    const txt = String(selected?.headersJson || '').trim();
    if (!txt) {
      headersJsonValid = false;
      headersJsonTree = null;
    } else {
      try {
        headersJsonTree = parseJsonObjectField('Headers JSON', txt);
        headersJsonValid = true;
      } catch {
        headersJsonTree = null;
        headersJsonValid = false;
      }
    }
  }
  $: {
    const txt = String(selected?.oauth2RequestHeadersJson || '').trim();
    if (!txt) {
      tokenHeadersJsonValid = false;
      tokenHeadersJsonTree = null;
    } else {
      try {
        tokenHeadersJsonTree = parseJsonObjectField('Token Headers JSON', txt);
        tokenHeadersJsonValid = true;
      } catch {
        tokenHeadersJsonTree = null;
        tokenHeadersJsonValid = false;
      }
    }
  }
  $: {
    const txt = String(selected?.oauth2RequestQueryJson || '').trim();
    if (!txt) {
      tokenQueryJsonValid = false;
      tokenQueryJsonTree = null;
    } else {
      try {
        tokenQueryJsonTree = parseJsonObjectField('Token Query JSON', txt);
        tokenQueryJsonValid = true;
      } catch {
        tokenQueryJsonTree = null;
        tokenQueryJsonValid = false;
      }
    }
  }
  $: {
    const txt = String(selected?.oauth2RequestBodyJson || '').trim();
    if (!txt) {
      tokenBodyJsonValid = false;
      tokenBodyJsonTree = null;
    } else {
      try {
        tokenBodyJsonTree = parseJsonAnyField('Token Body JSON', txt);
        tokenBodyJsonValid = true;
      } catch {
        tokenBodyJsonTree = null;
        tokenBodyJsonValid = false;
      }
    }
  }
  $: {
    const txt = String(selected?.queryJson || '').trim();
    if (!txt) {
      queryJsonValid = false;
      queryJsonTree = null;
    } else {
      try {
        queryJsonTree = parseJsonObjectField('Query JSON', txt);
        queryJsonValid = true;
      } catch {
        queryJsonTree = null;
        queryJsonValid = false;
      }
    }
  }
  $: {
    const txt = String(selected?.bodyJson || '').trim();
    if (!txt) {
      bodyJsonValid = false;
      bodyJsonTree = null;
    } else {
      try {
        bodyJsonTree = parseJsonAnyField('Body JSON', txt);
        bodyJsonValid = true;
      } catch {
        bodyJsonTree = null;
        bodyJsonValid = false;
      }
    }
  }
  $: responseText, tick().then(syncLeftTextareasHeight);
  $: selected?.exampleRequest, tick().then(syncLeftTextareasHeight);
  $: myApiPreviewDraft, tick().then(syncLeftTextareasHeight);
  $: requestInput, tick().then(syncLeftTextareasHeight);
  $: responseViewMode, tick().then(syncLeftTextareasHeight);
  $: exampleViewMode, tick().then(syncLeftTextareasHeight);
  $: myPreviewViewMode, tick().then(syncLeftTextareasHeight);
  $: selected?.description, tick().then(syncMainTextareasHeight);
  $: selected?.authJson, tick().then(syncMainTextareasHeight);
  $: selected?.oauth2RequestHeadersJson, tick().then(syncMainTextareasHeight);
  $: selected?.oauth2RequestQueryJson, tick().then(syncMainTextareasHeight);
  $: selected?.oauth2RequestBodyJson, tick().then(syncMainTextareasHeight);
  $: selected?.headersJson, tick().then(syncMainTextareasHeight);
  $: selected?.queryJson, tick().then(syncMainTextareasHeight);
  $: selected?.bodyJson, tick().then(syncMainTextareasHeight);
  $: {
    const currentName = String(nameDraft || selected?.name || '').trim();
    if (!currentName) {
      nameDuplicateHint = '';
    } else {
      const duplicates = duplicateNameMatches(currentName, selected || null);
      nameDuplicateHint = duplicates.length
        ? `Внимание: такое имя уже используется (${duplicates.length}).`
        : '';
    }
  }
  $: definitionDraft, tick().then(syncParameterEditorsHeight);
  $: activeParameter, tick().then(syncParameterEditorsHeight);
  $: {
    const rows = mappingRowsOf(selected);
    for (const row of rows) {
      void ensureColumnsFor(row.schema, row.table);
    }
  }
  $: {
    const tables = selected?.dataTables || [];
    for (const t of tables) {
      void ensureColumnsFor(t.schema, t.table);
    }
  }
  $: {
    const sig = `${selectedRef || ''}|${dataModelPreviewSignature(selected)}`;
    if (sig !== datasetPreviewSignature) {
      datasetPreviewSignature = sig;
      if (datasetPreviewTimer) {
        clearTimeout(datasetPreviewTimer);
        datasetPreviewTimer = null;
      }
      if (!selected) {
        datasetPreviewRows = [];
        datasetPreviewColumns = [];
        datasetPreviewHasMore = false;
        datasetPreviewLoading = false;
        datasetPreviewError = '';
      } else if (!selected.dataTables?.length) {
        datasetPreviewRows = [];
        datasetPreviewColumns = [];
        datasetPreviewHasMore = false;
        datasetPreviewLoading = false;
        datasetPreviewError = '';
      } else {
        datasetPreviewLoading = true;
        datasetPreviewTimer = setTimeout(() => {
          void previewDataModelNow();
        }, 250);
      }
    }
  }
  $: {
    const payload = myPreviewJson && typeof myPreviewJson === 'object' ? myPreviewJson : null;
    const mode = payload ? String(payload.mode || '').trim() : '';
    myPreviewModeLabel = payload ? requestPreviewModeLabel(mode) : '-';
    myPreviewSourceLabel = payload
      ? mode.startsWith('sent_')
        ? 'Проверить (запросы уже отправлены)'
        : 'Предпросмотр (до отправки)'
      : '-';

    const totalCandidate = payload
      ? Number(
          payload.total_requests ??
            payload.request_count ??
            (Array.isArray(payload.requests) ? payload.requests.length : payload.request ? 1 : 0)
        )
      : NaN;
    const shownCandidate = payload
      ? Number(
          payload.shown_requests ??
            (Array.isArray(payload.requests) ? payload.requests.length : payload.request ? 1 : 0)
        )
      : NaN;
    myPreviewTotalRequests = Number.isFinite(totalCandidate) && totalCandidate > 0 ? totalCandidate : null;
    myPreviewShownRequests = Number.isFinite(shownCandidate) && shownCandidate > 0 ? shownCandidate : null;
  }
  $: authViewMode, tick().then(syncMainTextareasHeight);
  $: headersViewMode, tick().then(syncMainTextareasHeight);
  $: queryViewMode, tick().then(syncMainTextareasHeight);
  $: bodyViewMode, tick().then(syncMainTextareasHeight);
  $: tokenHeadersViewMode, tick().then(syncMainTextareasHeight);
  $: tokenQueryViewMode, tick().then(syncMainTextareasHeight);
  $: tokenBodyViewMode, tick().then(syncMainTextareasHeight);

function autosize(el: HTMLTextAreaElement | null, minPx = 78) {
  if (!el) return;
  el.style.height = 'auto';
  el.style.height = `${Math.max(el.scrollHeight, minPx)}px`;
}

function syncParameterEditorsHeight() {
  autosize(aliasParamEl, 34);
  autosize(definitionParamEl, 60);
}

  function syncLeftTextareasHeight() {
    autosize(responsePreviewEl);
    autosize(exampleApiEl);
    autosize(myPreviewEl);
  }

  function syncMainTextareasHeight() {
    autosize(descriptionEl);
    autosize(authEl);
    autosize(tokenHeadersEl);
    autosize(tokenQueryEl);
    autosize(tokenBodyEl);
    autosize(headersEl);
    autosize(queryEl);
    autosize(bodyEl);
  }

  $: {
    const targetId = Math.trunc(Number(initialApiStoreId || 0));
    if (targetId > 0 && targetId !== initialApiStoreIdApplied && drafts.length) {
      applyInitialApiStoreSelection();
    }
  }

  onDestroy(() => {
    if (datasetPreviewTimer) clearTimeout(datasetPreviewTimer);
  });

  function updateEmbeddedLayoutByWidth(width: number) {
    compactLayoutByContainer = Number.isFinite(width) && width > 0 ? width < EMBEDDED_LAYOUT_BREAKPOINT : false;
  }

  function observePanelWidth(node: HTMLElement) {
    if (typeof ResizeObserver === 'undefined') return {};
    const ro = new ResizeObserver((entries) => {
      const width = entries?.[0]?.contentRect?.width || node.clientWidth || 0;
      updateEmbeddedLayoutByWidth(width);
    });
    ro.observe(node);
    updateEmbeddedLayoutByWidth(node.clientWidth || 0);
    return {
      destroy() {
        ro.disconnect();
      }
    };
  }

  void loadAll();
</script>

<section class="panel" use:observePanelWidth class:panel-embedded={effectiveEmbeddedMode}>
  <div class="panel-head">
    <h2>API (конструктор)</h2>
  </div>

  {#if err}
    <div class="alert">
      <div class="alert-title">Ошибка</div>
      <pre>{err}</pre>
    </div>
  {/if}
  {#if ok}
    <div class="okbox">{ok}</div>
  {/if}
  {#if warn}
    <div class="warnbox">{warn}</div>
  {/if}

  <div class="layout">
    <aside class="aside compare-aside">
      <div class="aside-title">Предпросмотр API</div>
      <div class="subsec">
        <div class="subttl response-head">
          <span>Предпросмотр ответа</span>
          {#if responseIsJson}
            <button type="button" class="view-toggle" on:click={() => (responseViewMode = responseViewMode === 'tree' ? 'raw' : 'tree')}>
              {responseViewMode === 'tree' ? 'RAW' : 'Дерево'}
            </button>
          {/if}
        </div>
        <div class="metrics-row">
          <span>Страницы: {responsePagesCount || '-'}</span>
          <span>Записей: {responsePayloadCount || '-'}</span>
          <span>Размер: {formatBytes(responsePayloadSize)}</span>
          <span>Время: {responseTimeMs ? `${responseTimeMs} мс` : '-'}</span>
        </div>
      {#if responseIsJson && responseViewMode === 'tree'}
        <div class="response-tree-wrap">
          <JsonTreeView node={responseJson} name="response" level={0} pickEnabled={true} on:pickpath={(e) => applyPickedResponsePath(e.detail.path)} />
        </div>
        {:else}
          <textarea bind:this={responsePreviewEl} readonly value={responseText}></textarea>
        {/if}
      </div>
      <div class="subsec">
        <div class="subttl response-head">
          <span>Что уйдет/ушло на сервер</span>
          {#if myPreviewIsJson}
            <button type="button" class="view-toggle" on:click={() => (myPreviewViewMode = myPreviewViewMode === 'tree' ? 'raw' : 'tree')}>
              {myPreviewViewMode === 'tree' ? 'RAW' : 'Дерево'}
            </button>
          {/if}
        </div>
        <div class="statusline">Источник: {myPreviewSourceLabel}</div>
        <div class="metrics-row">
          <span>Режим: {myPreviewModeLabel}</span>
          <span>Всего запросов: {myPreviewTotalRequests ?? '-'}</span>
          <span>Показано: {myPreviewShownRequests ?? '-'}</span>
        </div>
        <p class="hint small-hint">Здесь показывается реальный JSON запроса, который будет отправлен или уже отправлен на сервер.</p>
        {#if myPreviewIsJson && myPreviewViewMode === 'tree'}
          <div class="response-tree-wrap">
            <JsonTreeView node={myPreviewJson} name="request" level={0} />
          </div>
        {:else}
          <textarea
            bind:this={myPreviewEl}
            readonly
            value={myApiPreviewDraft}
          ></textarea>
        {/if}
        {#if !myApiPreviewDraft}
          <p class="hint small-hint">Нажми «Проверить» в строке URL.</p>
        {/if}
        {#if myPreviewApplyMessage}
          <div class="template-parse-note">{myPreviewApplyMessage}</div>
      {/if}
    </div>

  </aside>

    <div class="main">
      <div class="card">
        <h3 style="margin:0;">Настройка API</h3>

        <div class="connect-row">
          <select
            value={selected?.method || 'GET'}
            on:change={(e) => mutateSelected((d) => (d.method = toHttpMethod(e.currentTarget.value)))}
          >
            <option value="GET">GET</option>
            <option value="POST">POST</option>
            <option value="PUT">PUT</option>
            <option value="PATCH">PATCH</option>
            <option value="DELETE">DELETE</option>
          </select>
          <input
            value={requestInput}
            on:input={(e) => (requestInput = e.currentTarget.value)}
            on:blur={() => applyUrlInput(requestInput)}
            placeholder="Строка подключения (URL или curl)"
          />
          <div class="connect-actions">
            <button class="primary" on:click={checkApiNow} disabled={checking}>{checking ? 'Проверка...' : 'Проверить'}</button>
          </div>
        </div>

        <textarea
          bind:this={descriptionEl}
          class="desc"
          placeholder="Описание API"
          value={selected?.description || ''}
          on:input={(e) => mutateSelected((d) => (d.description = e.currentTarget.value))}
        ></textarea>

        <div class="dispatch-box">
          <div class="response-head field-head">
            <span>Настройка выполнения API</span>
          </div>
          <div class="pagination-grid">
            <div class="pagination-field">
              <small>Режим выполнения</small>
              <select
                value={selected?.executionMode || 'sync'}
                on:change={(e) =>
                  mutateSelected((d) => {
                    d.executionMode = e.currentTarget.value === 'async' ? 'async' : 'sync';
                  })}
              >
                <option value="sync">Синхронный</option>
                <option value="async">Асинхронный</option>
              </select>
              <p class="hint small-hint">Синхронный: обрабатывает сущности последовательно. Проще контролировать и разбирать ошибки.</p>
              <p class="hint small-hint">Сущность: одна независимая цепочка пагинации. Примеры: 1 магазин (token), 1 кабинет, 1 клиент, 1 строка параметров.</p>
            </div>
            {#if (selected?.executionMode || 'sync') === 'sync'}
              <div class="pagination-field">
                <small>Планировщик синхронного режима</small>
                <select
                  value={selected?.syncPlanner || 'entity_to_stop'}
                  on:change={(e) =>
                    mutateSelected((d) => {
                      d.syncPlanner = e.currentTarget.value === 'by_wave' ? 'by_wave' : 'entity_to_stop';
                    })}
                >
                  <option value="entity_to_stop">До остановки сущности</option>
                  <option value="by_wave">По шагу (волнами)</option>
                </select>
                {#if (selected?.syncPlanner || 'entity_to_stop') === 'entity_to_stop'}
                  <p class="hint small-hint">До остановки сущности: сначала полностью одна сущность, потом следующая.</p>
                {:else}
                  <p class="hint small-hint">По шагу (волнами): шаг 1 по всем сущностям, потом шаг 2 и так далее.</p>
                  <p class="hint small-hint">Пример: сущности A/B/C. Волна 1: A1,B1,C1. Волна 2: A2,B2,C2. Волна 3: только те, кто не остановился.</p>
                {/if}
              </div>
            {:else}
              <div class="pagination-field">
                <small>Лимит параллелизма (асинхронно)</small>
                <input
                  type="number"
                  min="1"
                  max="20"
                  value={selected?.asyncConcurrency || 3}
                  on:input={(e) =>
                    mutateSelected((d) => {
                      d.asyncConcurrency = Math.max(1, Math.min(20, Number(e.currentTarget.value) || 3));
                    })}
                />
                <p class="hint small-hint">Сколько сущностей запускать одновременно. Внутри одной сущности страницы всегда идут по порядку.</p>
                <p class="hint small-hint">Пример: лимит 3 и сущности A/B/C/D/E. Сначала стартуют A/B/C, потом по завершению подключаются D и E.</p>
              </div>
            {/if}
            <div class="pagination-field">
              <small>Пауза между запросами (мс, общий режим)</small>
              <input
                type="number"
                min="0"
                value={selected?.executionDelayMs || 0}
                on:input={(e) =>
                  mutateSelected((d) => {
                    d.executionDelayMs = Math.max(0, Number(e.currentTarget.value) || 0);
                  })}
              />
              <p class="hint small-hint">Применяется между шагами, волнами и асинхронными цепочками.</p>
            </div>
          </div>
          {#if (selected?.executionMode || 'sync') === 'async'}
            <p class="hint small-hint">Асинхронный: несколько сущностей выполняются параллельно для ускорения.</p>
          {/if}
          <p class="hint small-hint">Шаг: один запрос внутри сущности. Например шаг 1 (первая страница), шаг 2 (следующая), шаг 3.</p>
          <p class="hint small-hint">Причины отправки итерации пишутся в лог: initial_request, pagination_values_updated, page_increment, offset_increment, cursor_updated, next_url_received.</p>

          <div class="response-head field-head parameter-subhead">
            <small>Настройки логирования API</small>
            <button
              type="button"
              class="group-toggle"
              class:active-group-toggle={Boolean(selected?.responseLogEnabled)}
              on:click={toggleResponseLogEnabled}
              aria-pressed={Boolean(selected?.responseLogEnabled)}
            >
              <span>{selected?.responseLogEnabled ? 'Лог включен (пишется)' : 'Лог выключен (не пишется)'}</span>
              {#if Boolean(selected?.responseLogEnabled)}
                <span class="group-toggle-indicator"></span>
              {/if}
            </button>
          </div>
          <p class="hint small-hint">Это журнал шагов API (что отправили, что получили, почему остановились). Это отдельный блок и не связан с «Выходными параметрами».</p>
          {#if selected?.responseLogEnabled}
            <div class="hint small-hint">Системный шаблон лога: <b>Bronze системный лог API</b>.</div>
            <div class="storage-meta">
              <span>Хранятся в таблице:</span>
              <button
                class="link-btn"
                type="button"
                on:click={toggleResponseLogPicker}
              >
                {responseLogQualifiedTable(selected) || 'выбери таблицу'}
              </button>
            </div>
            {#if response_log_picker_open}
              <div class="storage-picker">
                <select bind:value={response_log_pick_value}>
                  {#if response_log_pick_value && !responseLogCompatibleTables().some((t) => `${t.schema_name}.${t.table_name}` === response_log_pick_value)}
                    <option value={response_log_pick_value}>{response_log_pick_value}</option>
                  {/if}
                  {#each responseLogCompatibleTables() as et}
                    <option value={`${et.schema_name}.${et.table_name}`}>{et.schema_name}.{et.table_name}</option>
                  {/each}
                </select>
                <button type="button" on:click={applyResponseLogChoice} disabled={!response_log_pick_value}>Подключить</button>
              </div>
            {/if}
            {#if !responseLogCompatibleTables().length}
              <p class="hint small-hint">Нет подходящих таблиц для лога. Нужна таблица, которая соответствует системному шаблону Bronze логов API.</p>
            {/if}
            <div class="log-mode-grid">
              <label class="pagination-field">
                <small>Режим логирования</small>
                <select
                  value={selected?.responseLogMode || 'standard'}
                  on:change={(e) => setResponseLogMode(e.currentTarget.value)}
                >
                  <option value="minimal">Минимальный</option>
                  <option value="standard">Стандартный</option>
                  <option value="debug">Расширенный (отладка)</option>
                </select>
              </label>
              <label class="pagination-field">
                <small>Ограничение размера response (символов)</small>
                <input
                  type="number"
                  min="0"
                  value={selected?.responseLogResponsePayloadLimit || 0}
                  on:input={(e) =>
                    mutateSelected((d) => {
                      d.responseLogResponsePayloadLimit = Math.max(0, Number(e.currentTarget.value) || 0);
                    })}
                />
                <p class="hint small-hint">0 = без ограничения. Защищает от слишком тяжелых логов.</p>
              </label>
            </div>
            <div class="pagination-logic-list">
              <div class="pagination-logic-item">
                <span>Писать request_payload</span>
                <button
                  type="button"
                  class="group-toggle group-toggle-sm"
                  class:active-group-toggle={Boolean(selected?.responseLogWriteRequestPayload)}
                  on:click={toggleResponseLogWriteRequestPayload}
                >
                  <span>{selected?.responseLogWriteRequestPayload ? 'Вкл' : 'Выкл'}</span>
                  {#if Boolean(selected?.responseLogWriteRequestPayload)}
                    <span class="group-toggle-indicator"></span>
                  {/if}
                </button>
              </div>
              <div class="pagination-logic-item">
                <span>Писать response_payload</span>
                <button
                  type="button"
                  class="group-toggle group-toggle-sm"
                  class:active-group-toggle={Boolean(selected?.responseLogWriteResponsePayload)}
                  on:click={toggleResponseLogWriteResponsePayload}
                >
                  <span>{selected?.responseLogWriteResponsePayload ? 'Вкл' : 'Выкл'}</span>
                  {#if Boolean(selected?.responseLogWriteResponsePayload)}
                    <span class="group-toggle-indicator"></span>
                  {/if}
                </button>
              </div>
              <div class="pagination-logic-item">
                <span>Писать pagination_values</span>
                <button
                  type="button"
                  class="group-toggle group-toggle-sm"
                  class:active-group-toggle={Boolean(selected?.responseLogWritePaginationValues)}
                  on:click={toggleResponseLogWritePaginationValues}
                >
                  <span>{selected?.responseLogWritePaginationValues ? 'Вкл' : 'Выкл'}</span>
                  {#if Boolean(selected?.responseLogWritePaginationValues)}
                    <span class="group-toggle-indicator"></span>
                  {/if}
                </button>
              </div>
              <div class="pagination-logic-item">
                <span>Писать только ошибки</span>
                <button
                  type="button"
                  class="group-toggle group-toggle-sm"
                  class:active-group-toggle={Boolean(selected?.responseLogOnlyErrors)}
                  on:click={toggleResponseLogOnlyErrors}
                >
                  <span>{selected?.responseLogOnlyErrors ? 'Вкл' : 'Выкл'}</span>
                  {#if Boolean(selected?.responseLogOnlyErrors)}
                    <span class="group-toggle-indicator"></span>
                  {/if}
                </button>
              </div>
            </div>
            <p class="hint small-hint">Если список таблиц пустой, сначала создай/подключи таблицу по системному шаблону Bronze логов API.</p>
          {/if}
        </div>

        <label class="auth-section">
          <div class="response-head field-head">
            <span>Авторизация</span>
            {#if authJsonValid}
              <button type="button" class="view-toggle" on:click={() => (authViewMode = authViewMode === 'tree' ? 'raw' : 'tree')}>
                {authViewMode === 'tree' ? 'RAW' : 'Дерево'}
              </button>
            {/if}
          </div>
          {#if groupByAliasCandidates.length}
            <div class="field-param-showcase">
              {#each groupByAliasCandidates as alias}
                <button
                  type="button"
                  class="param-token-chip"
                  title="Перетащи в JSON авторизации"
                  draggable="true"
                  on:dragstart={(e) => aliasChipDragStart(alias, e)}
                >
                  {alias}
                </button>
              {/each}
            </div>
          {/if}
          {#if authJsonValid && authViewMode === 'tree'}
            <div class="response-tree-wrap"><JsonTreeView node={authJsonTree} name="auth" level={0} /></div>
          {:else}
            <textarea
              class="json-code-editor"
              spellcheck="false"
              bind:this={authEl}
              value={selected?.authJson || ''}
              on:input={(e) => mutateSelected((d) => (d.authJson = e.currentTarget.value))}
              on:dragover|preventDefault
              on:drop={(e) => dropAliasTokenToField(e, 'authJson', authEl)}
            ></textarea>
          {/if}
          <p class="hint small-hint">Основной API-запрос: настрой вручную, как в документации сервиса. Пример: <code>Authorization: Bearer {"{{access_token}}"}</code></p>
        </label>
        <div class="auth-subrequest-box">
          <div class="response-head field-head">
            <span>Подзапрос токена</span>
            <button
              type="button"
              class="group-toggle group-toggle-sm auth-mode-btn"
              class:active-group-toggle={selected?.authMode === AUTH_MODE_OAUTH2}
              on:click={() =>
                mutateSelected((d) =>
                  (d.authMode = d.authMode === AUTH_MODE_OAUTH2 ? 'manual' : AUTH_MODE_OAUTH2)
                )
              }
              aria-pressed={selected?.authMode === AUTH_MODE_OAUTH2}
            >
              <span>{selected?.authMode === AUTH_MODE_OAUTH2 ? 'Включен' : 'Выключен'}</span>
              {#if selected?.authMode === AUTH_MODE_OAUTH2}
                <span class="group-toggle-indicator"></span>
              {/if}
            </button>
          </div>
          <p class="hint small-hint">
            Метод, адрес, headers/query/body и разбор ответа в алиасы.
          </p>
          {#if selected?.authMode === 'oauth2_client_credentials'}
            <div class="auth-subrequest-content">
            <div class="oauth-subreq-grid">
              <select
                value={selected?.oauth2RequestMethod || 'POST'}
                on:change={(e) => mutateSelected((d) => (d.oauth2RequestMethod = toHttpMethod(e.currentTarget.value)))}
              >
                <option value="GET">GET</option>
                <option value="POST">POST</option>
                <option value="PUT">PUT</option>
                <option value="PATCH">PATCH</option>
                <option value="DELETE">DELETE</option>
              </select>
              <input
                placeholder="URL/адрес подзапроса токена"
                value={selected?.oauth2RequestUrl || ''}
                on:input={(e) =>
                  mutateSelected((d) => {
                    d.oauth2RequestUrl = e.currentTarget.value;
                    d.oauth2TokenUrl = e.currentTarget.value;
                  })}
              />
            </div>
            <div class="raw-grid">
              <label>
                <div class="response-head field-head">
                  <span>Token Headers JSON</span>
                  {#if tokenHeadersJsonValid}
                    <button type="button" class="view-toggle" on:click={() => (tokenHeadersViewMode = tokenHeadersViewMode === 'tree' ? 'raw' : 'tree')}>
                      {tokenHeadersViewMode === 'tree' ? 'RAW' : 'Дерево'}
                    </button>
                  {/if}
                </div>
                {#if groupByAliasCandidates.length}
                  <div class="field-param-showcase">
                    {#each groupByAliasCandidates as alias}
                      <button
                        type="button"
                        class="param-token-chip"
                        title="Перетащи в Token Headers JSON"
                        draggable="true"
                        on:dragstart={(e) => aliasChipDragStart(alias, e)}
                      >
                        {alias}
                      </button>
                    {/each}
                  </div>
                {/if}
                {#if tokenHeadersJsonValid && tokenHeadersViewMode === 'tree'}
                  <div class="response-tree-wrap"><JsonTreeView node={tokenHeadersJsonTree} name="token_headers" level={0} /></div>
                {:else}
                  <textarea
                    class="json-code-editor"
                    spellcheck="false"
                    bind:this={tokenHeadersEl}
                    value={selected?.oauth2RequestHeadersJson || '{}'}
                    on:input={(e) => mutateSelected((d) => (d.oauth2RequestHeadersJson = e.currentTarget.value))}
                    on:dragover|preventDefault
                    on:drop={(e) => dropAliasTokenToField(e, 'oauth2RequestHeadersJson', tokenHeadersEl)}
                  ></textarea>
                {/if}
              </label>
              <label>
                <div class="response-head field-head">
                  <span>Token Query JSON</span>
                  {#if tokenQueryJsonValid}
                    <button type="button" class="view-toggle" on:click={() => (tokenQueryViewMode = tokenQueryViewMode === 'tree' ? 'raw' : 'tree')}>
                      {tokenQueryViewMode === 'tree' ? 'RAW' : 'Дерево'}
                    </button>
                  {/if}
                </div>
                {#if groupByAliasCandidates.length}
                  <div class="field-param-showcase">
                    {#each groupByAliasCandidates as alias}
                      <button
                        type="button"
                        class="param-token-chip"
                        title="Перетащи в Token Query JSON"
                        draggable="true"
                        on:dragstart={(e) => aliasChipDragStart(alias, e)}
                      >
                        {alias}
                      </button>
                    {/each}
                  </div>
                {/if}
                {#if tokenQueryJsonValid && tokenQueryViewMode === 'tree'}
                  <div class="response-tree-wrap"><JsonTreeView node={tokenQueryJsonTree} name="token_query" level={0} /></div>
                {:else}
                  <textarea
                    class="json-code-editor"
                    spellcheck="false"
                    bind:this={tokenQueryEl}
                    value={selected?.oauth2RequestQueryJson || '{}'}
                    on:input={(e) => mutateSelected((d) => (d.oauth2RequestQueryJson = e.currentTarget.value))}
                    on:dragover|preventDefault
                    on:drop={(e) => dropAliasTokenToField(e, 'oauth2RequestQueryJson', tokenQueryEl)}
                  ></textarea>
                {/if}
              </label>
            </div>
            <label>
              <div class="response-head field-head">
                <span>Token Body JSON</span>
                {#if tokenBodyJsonValid}
                  <button type="button" class="view-toggle" on:click={() => (tokenBodyViewMode = tokenBodyViewMode === 'tree' ? 'raw' : 'tree')}>
                    {tokenBodyViewMode === 'tree' ? 'RAW' : 'Дерево'}
                  </button>
                {/if}
              </div>
              {#if groupByAliasCandidates.length}
                <div class="field-param-showcase">
                  {#each groupByAliasCandidates as alias}
                    <button
                      type="button"
                      class="param-token-chip"
                      title="Перетащи в Token Body JSON"
                      draggable="true"
                      on:dragstart={(e) => aliasChipDragStart(alias, e)}
                    >
                      {alias}
                    </button>
                  {/each}
                </div>
              {/if}
              {#if tokenBodyJsonValid && tokenBodyViewMode === 'tree'}
                <div class="response-tree-wrap"><JsonTreeView node={tokenBodyJsonTree} name="token_body" level={0} /></div>
              {:else}
                <textarea
                  class="json-code-editor"
                  spellcheck="false"
                  bind:this={tokenBodyEl}
                  value={selected?.oauth2RequestBodyJson || '{}'}
                  on:input={(e) => mutateSelected((d) => (d.oauth2RequestBodyJson = e.currentTarget.value))}
                  on:dragover|preventDefault
                  on:drop={(e) => dropAliasTokenToField(e, 'oauth2RequestBodyJson', tokenBodyEl)}
                ></textarea>
              {/if}
            </label>
            <div class="rule-card">
              <div class="rule-card-head">
                <small>Как разобрать token response в параметры</small>
                <button type="button" class="icon-btn plus-dark" title="Добавить mapping" on:click={addOAuth2ResponseMapping}>+</button>
              </div>
              {#if selected?.oauth2ResponseMappings?.length}
                <div class="pagination-stop-rules-list">
                  {#each selected.oauth2ResponseMappings as map (map.id)}
                    <div class="data-row oauth2-map-row">
                      <input
                        placeholder="Путь поля в ответе (например access_token)"
                        value={map.responsePath}
                        on:input={(e) => updateOAuth2ResponseMapping(map.id, { responsePath: e.currentTarget.value })}
                      />
                      <input
                        placeholder="Alias параметра (например access_token)"
                        value={map.alias}
                        on:input={(e) => updateOAuth2ResponseMapping(map.id, { alias: e.currentTarget.value })}
                      />
                      <button type="button" class="chip-remove" title="Удалить mapping" on:click={() => removeOAuth2ResponseMapping(map.id)}>x</button>
                    </div>
                  {/each}
                </div>
              {/if}
            </div>
            <p class="hint small-hint">
              После подзапроса алиасы доступны в основном запросе через <code>{"{{alias}}"}</code>.
            </p>
            </div>
          {/if}
        </div>

        <div class="raw-grid">
          <label>
            <div class="response-head field-head">
              <span>Headers JSON</span>
              {#if headersJsonValid}
                <button type="button" class="view-toggle" on:click={() => (headersViewMode = headersViewMode === 'tree' ? 'raw' : 'tree')}>
                  {headersViewMode === 'tree' ? 'RAW' : 'Дерево'}
                </button>
              {/if}
            </div>
            {#if groupByAliasCandidates.length}
              <div class="field-param-showcase">
                {#each groupByAliasCandidates as alias}
                  <button
                    type="button"
                    class="param-token-chip"
                    title="Перетащи в Headers JSON"
                    draggable="true"
                    on:dragstart={(e) => aliasChipDragStart(alias, e)}
                  >
                    {alias}
                  </button>
                {/each}
              </div>
            {/if}
            {#if headersJsonValid && headersViewMode === 'tree'}
              <div class="response-tree-wrap"><JsonTreeView node={headersJsonTree} name="headers" level={0} /></div>
            {:else}
              <textarea
                class="json-code-editor"
                spellcheck="false"
                bind:this={headersEl}
                value={selected?.headersJson || ''}
                on:input={(e) => mutateSelected((d) => (d.headersJson = e.currentTarget.value))}
                on:dragover|preventDefault
                on:drop={(e) => dropAliasTokenToField(e, 'headersJson', headersEl)}
              ></textarea>
            {/if}
          </label>
          <label>
            <div class="response-head field-head">
              <span>Query JSON</span>
              {#if queryJsonValid}
                <button type="button" class="view-toggle" on:click={() => (queryViewMode = queryViewMode === 'tree' ? 'raw' : 'tree')}>
                  {queryViewMode === 'tree' ? 'RAW' : 'Дерево'}
                </button>
              {/if}
            </div>
            {#if groupByAliasCandidates.length}
              <div class="field-param-showcase">
                {#each groupByAliasCandidates as alias}
                  <button
                    type="button"
                    class="param-token-chip"
                    title="Перетащи в Query JSON"
                    draggable="true"
                    on:dragstart={(e) => aliasChipDragStart(alias, e)}
                  >
                    {alias}
                  </button>
                {/each}
              </div>
            {/if}
            {#if queryJsonValid && queryViewMode === 'tree'}
              <div class="response-tree-wrap"><JsonTreeView node={queryJsonTree} name="query" level={0} /></div>
            {:else}
              <textarea
                class="json-code-editor"
                spellcheck="false"
                bind:this={queryEl}
                value={selected?.queryJson || ''}
                on:input={(e) => mutateSelected((d) => (d.queryJson = e.currentTarget.value))}
                on:dragover|preventDefault
                on:drop={(e) => dropAliasTokenToField(e, 'queryJson', queryEl)}
              ></textarea>
            {/if}
          </label>
        </div>

        <label>
          <div class="response-head field-head">
            <span>Body JSON</span>
            {#if bodyJsonValid}
              <button type="button" class="view-toggle" on:click={() => (bodyViewMode = bodyViewMode === 'tree' ? 'raw' : 'tree')}>
                {bodyViewMode === 'tree' ? 'RAW' : 'Дерево'}
              </button>
            {/if}
          </div>
          {#if groupByAliasCandidates.length}
            <div class="field-param-showcase">
              {#each groupByAliasCandidates as alias}
                <button
                  type="button"
                  class="param-token-chip"
                  title="Перетащи в Body JSON"
                  draggable="true"
                  on:dragstart={(e) => aliasChipDragStart(alias, e)}
                >
                  {alias}
                </button>
              {/each}
            </div>
          {/if}
          {#if bodyJsonValid && bodyViewMode === 'tree'}
            <div class="response-tree-wrap"><JsonTreeView node={bodyJsonTree} name="body" level={0} /></div>
          {:else}
            <textarea
              class="json-code-editor"
              spellcheck="false"
              bind:this={bodyEl}
              value={selected?.bodyJson || ''}
              on:input={(e) => mutateSelected((d) => (d.bodyJson = e.currentTarget.value))}
              on:dragover|preventDefault
              on:drop={(e) => dropAliasTokenToField(e, 'bodyJson', bodyEl)}
            ></textarea>
          {/if}
        </label>

        <div class="dispatch-box parameter-management-box">
          <div class="response-head field-head">
            <span>Управление параметрами</span>
          </div>

          <div class="data-builder-box">
            <div class="response-head field-head parameter-subhead">
              <span>Конструктор данных (таблицы, связи, фильтры, показатели)</span>
              <span class="inline-actions">
                <label class="preview-limit-inline">
                  <small>Лимит предпросмотра</small>
                  <input
                    type="number"
                    min="1"
                    max="50"
                    value={selected?.previewRequestLimit || 5}
                    on:input={(e) => mutateSelected((d) => (d.previewRequestLimit = Math.max(1, Math.min(50, Number(e.currentTarget.value) || 5))))}
                  />
                </label>
              </span>
            </div>

            <div class="data-section">
              <div class="response-head field-head parameter-subhead">
                <small>Таблицы</small>
                <span class="inline-actions">
                  <button type="button" class="view-toggle" on:click={addDataTable}>Таблица +</button>
                  <button type="button" class="view-toggle" on:click={addDataDateParam}>Дата +</button>
                </span>
              </div>
              {#if selected?.dataTables?.length}
                <div class="crumb-strip">
                  {#each selected.dataTables as tbl (tbl.id)}
                    <div class="entity-crumb-wrap" class:active-crumb-wrap={activeDataTableId === tbl.id}>
                      <button type="button" class="entity-crumb" on:click={() => (activeDataTableId = tbl.id)}>
                        {tbl.alias || `${tbl.schema}.${tbl.table}`}
                      </button>
                      <button
                        type="button"
                        class="entity-crumb-remove"
                        class:active-crumb-remove={activeDataTableId === tbl.id}
                        title="Удалить таблицу"
                        aria-label="Удалить таблицу"
                        on:click|stopPropagation={() => removeDataTable(tbl.id)}
                      >
                        ×
                      </button>
                    </div>
                  {/each}
                </div>
                {@const activeTable = selected.dataTables.find((tbl) => tbl.id === activeDataTableId)}
                {#if activeTable}
                  <div class="rule-card">
                    <div class="rule-card-head">
                      <small>Правила выбора таблицы</small>
                      <button type="button" class="chip-remove" on:click={() => removeDataTable(activeTable.id)}>x</button>
                    </div>
                    <div class="data-row table-rule-row">
                      <select value={`${activeTable.schema}.${activeTable.table}`} on:change={(e) => updateDataTableRef(activeTable.id, e.currentTarget.value)}>
                        <option value="">Таблица</option>
                        {#each existingTables as et}
                          <option value={`${et.schema_name}.${et.table_name}`}>{et.schema_name}.{et.table_name}</option>
                        {/each}
                      </select>
                      <input value={activeTable.alias} placeholder="Краткое наименование" on:input={(e) => updateDataTableAlias(activeTable.id, e.currentTarget.value)} />
                      <input value={tableAddressById(selected, activeTable.id)} readonly />
                    </div>
                  </div>
                {/if}
              {:else}
                <p class="hint">Сначала добавь таблицы, с которыми будешь работать.</p>
              {/if}
            </div>

            <div class="data-section">
              <div class="response-head field-head parameter-subhead">
                <small>Связи таблиц</small>
                <button type="button" class="view-toggle" on:click={addDataJoin}>Связь +</button>
              </div>
              {#if selected?.dataJoins?.length}
                <div class="crumb-strip">
                  {#each selected.dataJoins as j (j.id)}
                    <div class="entity-crumb-wrap" class:active-crumb-wrap={activeDataJoinId === j.id}>
                      <button type="button" class="entity-crumb" on:click={() => (activeDataJoinId = j.id)}>
                        {joinCrumbLabel(selected, j)}
                      </button>
                      <button
                        type="button"
                        class="entity-crumb-remove"
                        class:active-crumb-remove={activeDataJoinId === j.id}
                        title="Удалить связь"
                        aria-label="Удалить связь"
                        on:click|stopPropagation={() => removeDataJoin(j.id)}
                      >
                        ×
                      </button>
                    </div>
                  {/each}
                </div>
                {@const activeJoin = selected.dataJoins.find((j) => j.id === activeDataJoinId)}
                {#if activeJoin}
                  <div class="rule-card">
                    <div class="rule-card-head">
                      <small>Правила связи</small>
                      <button type="button" class="chip-remove" on:click={() => removeDataJoin(activeJoin.id)}>x</button>
                    </div>
                    <div class="data-row join-rule-row">
                      <select value={activeJoin.leftTableId} on:change={(e) => updateDataJoinTableRef(activeJoin.id, 'left', e.currentTarget.value)}>
                        <option value="">Таблица (краткое наименование)</option>
                        {#each selected.dataTables as t}
                          <option value={t.id}>{t.alias || `${t.schema}.${t.table}`}</option>
                        {/each}
                      </select>
                      <select value={activeJoin.leftField} on:change={(e) => updateDataJoin(activeJoin.id, { leftField: e.currentTarget.value })}>
                        <option value="">Поле сопоставления</option>
                        {#each tableColumnsById(selected, activeJoin.leftTableId) as col}
                          <option value={col}>{col}</option>
                        {/each}
                      </select>
                      <input value="=" readonly />
                      <select value={activeJoin.rightTableId} on:change={(e) => updateDataJoinTableRef(activeJoin.id, 'right', e.currentTarget.value)}>
                        <option value="">Другая таблица (краткое наименование)</option>
                        {#each selected.dataTables as t}
                          <option value={t.id}>{t.alias || `${t.schema}.${t.table}`}</option>
                        {/each}
                      </select>
                      <select value={activeJoin.rightField} on:change={(e) => updateDataJoin(activeJoin.id, { rightField: e.currentTarget.value })}>
                        <option value="">Поле параметра</option>
                        {#each tableColumnsById(selected, activeJoin.rightTableId) as col}
                          <option value={col}>{col}</option>
                        {/each}
                      </select>
                      <select value={activeJoin.joinType} on:change={(e) => updateDataJoin(activeJoin.id, { joinType: e.currentTarget.value === 'left' ? 'left' : 'inner' })}>
                        {#each DATA_JOIN_TYPES as jt}
                          <option value={jt.value}>{jt.label}</option>
                        {/each}
                      </select>
                    </div>
                  </div>
                {/if}
              {:else}
                <p class="hint">После выбора таблиц добавь связи по ключевым колонкам.</p>
              {/if}
            </div>

            <div class="data-section">
              <div class="response-head field-head parameter-subhead">
                <small>Фильтры</small>
                <button type="button" class="view-toggle" on:click={addDataFilter}>Фильтр +</button>
              </div>
              {#if selected?.dataFilters?.length}
                <div class="crumb-strip">
                  {#each selected.dataFilters as f (f.id)}
                    <div class="entity-crumb-wrap" class:active-crumb-wrap={activeDataFilterId === f.id}>
                      <button type="button" class="entity-crumb" on:click={() => (activeDataFilterId = f.id)}>
                        {filterCrumbLabel(selected, f)}
                      </button>
                      <button
                        type="button"
                        class="entity-crumb-remove"
                        class:active-crumb-remove={activeDataFilterId === f.id}
                        title="Удалить фильтр"
                        aria-label="Удалить фильтр"
                        on:click|stopPropagation={() => removeDataFilter(f.id)}
                      >
                        ×
                      </button>
                    </div>
                  {/each}
                </div>
                {@const activeFilter = selected.dataFilters.find((f) => f.id === activeDataFilterId)}
                {#if activeFilter}
                  <div class="rule-card">
                    <div class="rule-card-head">
                      <small>Правила фильтрации</small>
                      <button type="button" class="chip-remove" on:click={() => removeDataFilter(activeFilter.id)}>x</button>
                    </div>
                    <div class="data-row filter-rule-row">
                      <select value={activeFilter.tableId} on:change={(e) => updateDataFilterTableRef(activeFilter.id, e.currentTarget.value)}>
                        <option value="">Краткое наименование таблицы</option>
                        <option value={FILTER_SCOPE_ALIAS}>Итоговые параметры (таблицы/даты)</option>
                        {#each selected.dataTables as t}
                          <option value={t.id}>{t.alias || `${t.schema}.${t.table}`}</option>
                        {/each}
                      </select>
                      <select value={activeFilter.field} on:change={(e) => updateDataFilter(activeFilter.id, { field: e.currentTarget.value })}>
                        {#if isAliasDataFilter(activeFilter)}
                          <option value="">Alias параметра</option>
                          {#each aliasOptionsForFilters(selected) as aliasOpt}
                            <option value={aliasOpt}>{aliasOpt}</option>
                          {/each}
                        {:else}
                          <option value="">Поле</option>
                          {#each tableColumnsById(selected, activeFilter.tableId) as col}
                            <option value={col}>{col}</option>
                          {/each}
                        {/if}
                      </select>
                      <select value={activeFilter.operator} on:change={(e) => updateDataFilter(activeFilter.id, { operator: e.currentTarget.value })}>
                        {#each dataFilterOperatorsFor(activeFilter) as op}
                          <option value={op.value}>{op.label}</option>
                        {/each}
                      </select>
                      {#if isBooleanDataFilter(activeFilter)}
                        <select value={activeFilter.compareValue} on:change={(e) => updateDataFilter(activeFilter.id, { compareValue: e.currentTarget.value })}>
                          <option value="">Условие</option>
                          <option value="true">true</option>
                          <option value="false">false</option>
                        </select>
                      {:else}
                        <input value={activeFilter.compareValue} placeholder="Условие" on:input={(e) => updateDataFilter(activeFilter.id, { compareValue: e.currentTarget.value })} />
                      {/if}
                    </div>
                  </div>
                {/if}
              {:else}
                <p class="hint">Фильтры применяются после связей и перед выбором показателей.</p>
              {/if}
            </div>

            <div class="parameter-sources-grid">
              <div class="source-card">
                <div class="response-head field-head parameter-subhead param-source-head">
                  <small>Показатели для работы</small>
                  <span class="inline-actions">
                    <button type="button" class="view-toggle" on:click={addDataField}>Таблица +</button>
                    <button type="button" class="view-toggle" on:click={addDataDateParam}>Дата +</button>
                  </span>
                </div>
                {#if selected?.dataFields?.length || selected?.dataDateParams?.length}
                  <div class="data-list">
                    {#each selected.dataFields as f, idx (f.id)}
                      <div class="rule-card">
                        <div class="data-row param-row">
                          <select value={f.tableId} on:change={(e) => updateDataFieldTableRef(f.id, e.currentTarget.value)}>
                            <option value="">Таблица</option>
                            {#each selected.dataTables as t}
                              <option value={t.id}>{t.schema}.{t.table}</option>
                            {/each}
                          </select>
                          <select value={f.field} on:change={(e) => updateDataField(f.id, { field: e.currentTarget.value })}>
                            <option value="">Колонка</option>
                            {#each dataFieldColumnOptions(selected, f) as col}
                              <option value={col}>{col}</option>
                            {/each}
                          </select>
                          <input value={f.alias} placeholder="Название параметра (alias)" on:input={(e) => updateDataField(f.id, { alias: e.currentTarget.value })} />
                          <button
                            type="button"
                            class="group-toggle"
                            class:active-group-toggle={Boolean(f.grouped)}
                            on:click={() => updateDataField(f.id, { grouped: !Boolean(f.grouped) })}
                            aria-pressed={Boolean(f.grouped)}
                          >
                            <span>Группировать</span>
                            {#if Boolean(f.grouped)}
                              <span class="group-toggle-indicator" aria-hidden="true"></span>
                            {/if}
                          </button>
                          <div class="row-order-actions">
                            <button
                              type="button"
                              class="row-icon-btn"
                              title="Поднять выше"
                              aria-label="Поднять выше"
                              on:click={() => moveDataField(f.id, -1)}
                              disabled={idx === 0}
                            >
                              ▲
                            </button>
                            <button
                              type="button"
                              class="row-icon-btn"
                              title="Опустить ниже"
                              aria-label="Опустить ниже"
                              on:click={() => moveDataField(f.id, 1)}
                              disabled={idx === selected.dataFields.length - 1}
                            >
                              ▼
                            </button>
                          </div>
                          <button type="button" class="chip-remove" on:click={() => removeDataField(f.id)}>x</button>
                        </div>
                        {#if isDataFieldDateType(selected, f)}
                          <div class="data-row field-date-row">
                            <select
                              value={toDateAnchorPreset(String(f.dateAnchorPreset || 'raw'))}
                              on:change={(e) =>
                                updateDataField(f.id, {
                                  dateMode: 'process',
                                  dateAnchorPreset: toDateAnchorPreset(e.currentTarget.value)
                                })}
                            >
                              {#each DATE_ANCHOR_PRESETS as opt}
                                <option value={opt.value}>{opt.label}</option>
                              {/each}
                            </select>
                            <input
                              type="number"
                              value={f.dateAddDays || 0}
                              placeholder="+/- дни"
                              on:input={(e) =>
                                updateDataField(f.id, {
                                  dateMode: 'process',
                                  dateAddDays: Number(e.currentTarget.value) || 0
                                })}
                            />
                            <input
                              type="number"
                              value={f.dateAddMonths || 0}
                              placeholder="+/- месяцы"
                              on:input={(e) =>
                                updateDataField(f.id, {
                                  dateMode: 'process',
                                  dateAddMonths: Number(e.currentTarget.value) || 0
                                })}
                            />
                            <select
                              value={toDateFormatPreset(String(f.dateFormatPreset || 'yyyy_mm_dd'))}
                              on:change={(e) =>
                                updateDataField(f.id, {
                                  dateMode: 'process',
                                  dateFormatPreset: toDateFormatPreset(e.currentTarget.value)
                                })}
                            >
                              {#each DATE_FORMAT_PRESETS as fmt}
                                <option value={fmt.value}>{fmt.label}</option>
                              {/each}
                            </select>
                          </div>
                        {/if}
                      </div>
                    {/each}
                    {#each selected.dataDateParams as p (p.id)}
                      <div class="rule-card">
                        <div class="data-row param-row date-param-inline-row">
                          <select
                            value={p.basePreset}
                            on:change={(e) => updateDataDateParam(p.id, { basePreset: e.currentTarget.value === 'custom' ? 'custom' : 'today' })}
                          >
                            {#each DATE_BASE_PRESETS as opt}
                              <option value={opt.value}>{opt.label}</option>
                            {/each}
                          </select>
                          <input
                            type="date"
                            value={String(p.customDate || '').slice(0, 10)}
                            disabled={p.basePreset !== 'custom'}
                            on:input={(e) => updateDataDateParam(p.id, { customDate: e.currentTarget.value })}
                          />
                          <input
                            value={p.alias}
                            placeholder="Краткое наименование (alias)"
                            on:input={(e) => updateDataDateParam(p.id, { alias: e.currentTarget.value })}
                          />
                          <button
                            type="button"
                            class="group-toggle group-toggle-sm"
                            class:active-group-toggle={Boolean(p.grouped)}
                            on:click={() => updateDataDateParam(p.id, { grouped: !Boolean(p.grouped) })}
                            aria-pressed={Boolean(p.grouped)}
                          >
                            <span>Группировать</span>
                            {#if Boolean(p.grouped)}
                              <span class="group-toggle-indicator"></span>
                            {/if}
                          </button>
                          <button type="button" class="chip-remove" title="Удалить параметр даты" on:click={() => removeDataDateParam(p.id)}>x</button>
                        </div>
                        <div class="data-row field-date-row">
                          <select value={p.anchorPreset} on:change={(e) => updateDataDateParam(p.id, { anchorPreset: toDateAnchorPreset(e.currentTarget.value) })}>
                            {#each DATE_ANCHOR_PRESETS as opt}
                              <option value={opt.value}>{opt.label}</option>
                            {/each}
                          </select>
                          <input
                            type="number"
                            value={p.addDays || 0}
                            placeholder="+/- дни"
                            on:input={(e) => updateDataDateParam(p.id, { addDays: Number(e.currentTarget.value) || 0 })}
                          />
                          <input
                            type="number"
                            value={p.addMonths || 0}
                            placeholder="+/- месяцы"
                            on:input={(e) => updateDataDateParam(p.id, { addMonths: Number(e.currentTarget.value) || 0 })}
                          />
                          <select value={p.formatPreset} on:change={(e) => updateDataDateParam(p.id, { formatPreset: toDateFormatPreset(e.currentTarget.value) })}>
                            {#each DATE_FORMAT_PRESETS as fmt}
                              <option value={fmt.value}>{fmt.label}</option>
                            {/each}
                          </select>
                        </div>
                        <p class="hint small-hint">Превью значения: <code>{dateParamPreviewValue(p)}</code></p>
                      </div>
                    {/each}
                  </div>
                {:else}
                  <p class="hint">Добавь параметры через «Таблица +» или «Дата +».</p>
                {/if}
                <p class="hint small-hint data-helper-note">Группировка работает одинаково для параметров из таблиц и дат.</p>
              </div>
            </div>

            <div class="data-section">
              <div class="response-head field-head parameter-subhead">
                <small>Предпросмотр данных</small>
                <span class="hint small-hint">{datasetPreviewLoading ? 'Обновляется...' : 'Обновляется автоматически'}</span>
              </div>

              {#if datasetPreviewError}
                <p class="definition-error">{datasetPreviewError}</p>
              {/if}
              <div class="dataset-preview-table-wrap">
                {#if datasetPreviewColumns.length}
                  <table class="dataset-preview-table">
                    <thead>
                      <tr>
                        {#each datasetPreviewColumns as col}
                          <th>{col}</th>
                        {/each}
                      </tr>
                    </thead>
                    <tbody>
                      {#if datasetPreviewRows.length}
                        {#each datasetPreviewRows as row}
                          <tr>
                            {#each datasetPreviewColumns as col}
                              <td title={formatParameterRowValue(row, col)}>{formatParameterRowValue(row, col)}</td>
                            {/each}
                          </tr>
                        {/each}
                      {:else}
                        <tr>
                          <td colspan={datasetPreviewColumns.length}>Нет строк по текущим условиям.</td>
                        </tr>
                      {/if}
                    </tbody>
                  </table>
                {:else}
                  <div class="empty-preview-state">
                    <p class="hint">Добавь показатели или параметры из даты, чтобы увидеть предпросмотр.</p>
                  </div>
                {/if}
              </div>
              {#if datasetPreviewColumns.length}
                <p class="hint small-hint">
                  Показано {datasetPreviewRows.length} строк (макс. 10){datasetPreviewHasMore ? ', есть ещё данные.' : '.'}
                </p>
              {/if}
            </div>
          </div>
        </div>

        <div class="pagination-box">
          <div class="response-head field-head">
            <span>Параметры пагинации</span>
            <button
              type="button"
              class="group-toggle group-toggle-sm"
              class:active-group-toggle={Boolean(selected?.paginationEnabled)}
              on:click={togglePaginationEnabled}
              aria-pressed={Boolean(selected?.paginationEnabled)}
            >
              <span>{selected?.paginationEnabled ? 'Пагинация включена' : 'Пагинация выключена'}</span>
              {#if Boolean(selected?.paginationEnabled)}
                <span class="group-toggle-indicator"></span>
              {/if}
            </button>
          </div>
          {#if selected?.paginationEnabled}
            <div class="data-section">
              <div class="response-head field-head parameter-subhead">
                <small>Параметры пагинации</small>
                <button type="button" class="view-toggle" on:click={addPaginationParameter}>Параметр +</button>
              </div>
              {#if selected?.paginationParameters?.length}
                <div class="crumb-strip">
                  {#each selected.paginationParameters as param (param.id)}
                    <div class="entity-crumb-wrap" class:active-crumb-wrap={activePaginationParameterId === param.id}>
                      <button type="button" class="entity-crumb" on:click={() => setActivePaginationParameter(param.id)}>
                        {param.alias || 'параметр'}
                      </button>
                      <button
                        type="button"
                        class="entity-crumb-remove"
                        class:active-crumb-remove={activePaginationParameterId === param.id}
                        title="Удалить параметр"
                        aria-label="Удалить параметр"
                        on:click|stopPropagation={() => removePaginationParameter(param.id)}
                      >
                        ×
                      </button>
                    </div>
                  {/each}
                </div>
                {#if activePaginationParameter}
                  <div class="rule-card pagination-param-editor">
                    <div class="rule-card-head">
                      <small>Настройка параметра</small>
                    </div>
                    <div class="pagination-param-helper">
                      <p class="hint small-hint">
                        {#if !paginationCursorResponsePathOptions.length}
                          Если список путей пустой, обнови ответ API.
                        {:else}
                          Путь можно выбрать из ответа. При необходимости обнови ответ API.
                        {/if}
                      </p>
                      <button
                        type="button"
                        class="icon-btn refresh-btn"
                        title="Обновить ответ API"
                        aria-label="Обновить ответ API"
                        on:click={checkApiNow}
                        disabled={checking}
                      >
                        {checking ? '…' : '↻'}
                      </button>
                    </div>
                    <div class="pagination-grid pagination-param-grid pagination-param-inline-row">
                      <div class="pagination-field">
                        <small>Короткое название</small>
                        <input
                          value={activePaginationParameter.alias}
                          placeholder="cursor_updated_at"
                          on:input={(e) => updatePaginationParameter(activePaginationParameter.id, { alias: e.currentTarget.value })}
                        />
                      </div>
                      <div class="pagination-field">
                        <small>Выбор пути в ответе</small>
                        <select
                          value={paginationCursorResponsePick}
                          disabled={!paginationCursorResponsePathOptions.length}
                          on:change={(e) => setPaginationResponsePick(e.currentTarget.value)}
                        >
                          {#if !paginationCursorResponsePathOptions.length}
                            <option value="">Нет путей из тестового ответа</option>
                          {:else}
                            {#each paginationCursorResponsePathOptions as opt}
                              <option value={opt}>{opt}</option>
                            {/each}
                          {/if}
                        </select>
                      </div>
                      <div class="pagination-field">
                        <small>Путь в ответе</small>
                        <input
                          value={activePaginationParameter.responsePath}
                          placeholder="cursor.updatedAt"
                          on:input={(e) => updatePaginationParameter(activePaginationParameter.id, { responsePath: e.currentTarget.value })}
                        />
                      </div>
                      <div class="pagination-field">
                        <small>Первое значение (первая волна)</small>
                        <input
                          value={activePaginationParameter.firstValue || ''}
                          placeholder='например: "", 0, null, &#123;&#123;nmID&#125;&#125;'
                          on:input={(e) => updatePaginationParameter(activePaginationParameter.id, { firstValue: e.currentTarget.value })}
                        />
                      </div>
                    </div>
                    <p class="hint small-hint">Используй alias параметра как <code>&#123;&#123;alias&#125;&#125;</code> в Body/Headers/Query/Авторизация.</p>
                  </div>
                {/if}
              {:else}
                <p class="hint">Добавь параметр пагинации. Для каждого параметра укажи короткое имя и путь в ответе.</p>
              {/if}
            </div>

            <div class="data-section">
              <div class="response-head field-head parameter-subhead">
                <small>Лимиты и безопасность</small>
              </div>
              <div class="pagination-safety-layout">
                <div class="pagination-numeric-grid">
                  <div class="pagination-setting-item">
                    <div class="pagination-setting-head">
                      <small class="pagination-setting-title">Лимит страниц</small>
                      <button
                        type="button"
                        class="pagination-inline-toggle"
                        class:active-inline-toggle={Boolean(selected?.paginationUseMaxPages)}
                        on:click={togglePaginationUseMaxPages}
                        aria-pressed={Boolean(selected?.paginationUseMaxPages)}
                      >
                        <span>{selected?.paginationUseMaxPages ? 'Вкл' : 'Выкл'}</span>
                        {#if Boolean(selected?.paginationUseMaxPages)}
                          <span class="pagination-inline-indicator"></span>
                        {/if}
                      </button>
                    </div>
                    <input
                      type="number"
                      min="1"
                      class="pagination-value-input"
                      value={selected?.paginationMaxPages || 1}
                      on:input={(e) => mutateSelected((d) => (d.paginationMaxPages = Number(e.currentTarget.value) || 1))}
                      disabled={!selected?.paginationUseMaxPages}
                    />
                  </div>

                  <div class="pagination-setting-item">
                    <div class="pagination-setting-head">
                      <small class="pagination-setting-title">Пауза между страницами (мс)</small>
                      <button
                        type="button"
                        class="pagination-inline-toggle"
                        class:active-inline-toggle={Boolean(selected?.paginationUseDelay)}
                        on:click={togglePaginationUseDelay}
                        aria-pressed={Boolean(selected?.paginationUseDelay)}
                      >
                        <span>{selected?.paginationUseDelay ? 'Вкл' : 'Выкл'}</span>
                        {#if Boolean(selected?.paginationUseDelay)}
                          <span class="pagination-inline-indicator"></span>
                        {/if}
                      </button>
                    </div>
                    <input
                      type="number"
                      min="0"
                      class="pagination-value-input"
                      value={selected?.paginationDelayMs || 0}
                      on:input={(e) => mutateSelected((d) => (d.paginationDelayMs = Number(e.currentTarget.value) || 0))}
                      disabled={!selected?.paginationUseDelay}
                    />
                  </div>

                  <div class="pagination-setting-item">
                    <div class="pagination-setting-head">
                      <small class="pagination-setting-title">Одинаковые ответы подряд</small>
                      <button
                        type="button"
                        class="pagination-inline-toggle"
                        class:active-inline-toggle={Boolean(selected?.paginationStopOnSameResponse)}
                        on:click={togglePaginationStopOnSameResponse}
                        aria-pressed={Boolean(selected?.paginationStopOnSameResponse)}
                      >
                        <span>{selected?.paginationStopOnSameResponse ? 'Вкл' : 'Выкл'}</span>
                        {#if Boolean(selected?.paginationStopOnSameResponse)}
                          <span class="pagination-inline-indicator"></span>
                        {/if}
                      </button>
                    </div>
                    <input
                      type="number"
                      min="2"
                      max="50"
                      class="pagination-value-input"
                      value={selected?.paginationSameResponseLimit || 5}
                      on:input={(e) => mutateSelected((d) => (d.paginationSameResponseLimit = Math.max(2, Math.min(50, Number(e.currentTarget.value) || 5))))}
                      disabled={!selected?.paginationStopOnSameResponse}
                    />
                  </div>
                </div>

                <div class="pagination-boolean-list">
                    <div class="pagination-bool-row">
                    <small class="pagination-setting-title">Остановка при отсутствии нового значения пагинации</small>
                    <button
                      type="button"
                      class="pagination-inline-toggle"
                      class:active-inline-toggle={Boolean(selected?.paginationStopOnMissingValue)}
                      on:click={togglePaginationStopOnMissingValue}
                      aria-pressed={Boolean(selected?.paginationStopOnMissingValue)}
                    >
                      <span>{selected?.paginationStopOnMissingValue ? 'Вкл' : 'Выкл'}</span>
                      {#if Boolean(selected?.paginationStopOnMissingValue)}
                        <span class="pagination-inline-indicator"></span>
                      {/if}
                    </button>
                  </div>

                    <div class="pagination-bool-row">
                    <small class="pagination-setting-title">Остановка при HTTP ошибке</small>
                    <button
                      type="button"
                      class="pagination-inline-toggle"
                      class:active-inline-toggle={Boolean(selected?.paginationStopOnHttpError)}
                      on:click={togglePaginationStopOnHttpError}
                      aria-pressed={Boolean(selected?.paginationStopOnHttpError)}
                    >
                      <span>{selected?.paginationStopOnHttpError ? 'Вкл' : 'Выкл'}</span>
                      {#if Boolean(selected?.paginationStopOnHttpError)}
                        <span class="pagination-inline-indicator"></span>
                      {/if}
                    </button>
                    </div>
                </div>
              </div>
              <div class="pagination-stop-rules">
                <div class="response-head field-head parameter-subhead">
                  <small>Условия остановки по полю ответа</small>
                  <button type="button" class="view-toggle" on:click={addPaginationStopRule}>Условие +</button>
                </div>
                {#if selected?.paginationStopRules?.length}
                  <div class="pagination-stop-rules-list">
                    {#each selected.paginationStopRules as rule (rule.id)}
                      <div class="data-row pagination-stop-rule-row">
                        <select
                          value={rule.responsePath}
                          on:change={(e) => updatePaginationStopRule(rule.id, { responsePath: e.currentTarget.value })}
                        >
                          <option value="">Выбор пути в ответе</option>
                          {#each paginationStopRulePathOptionsFor(rule.responsePath) as opt}
                            <option value={opt}>{opt}</option>
                          {/each}
                        </select>
                        <input
                          value={rule.responsePath}
                          placeholder="Путь в ответе"
                          on:input={(e) => updatePaginationStopRule(rule.id, { responsePath: e.currentTarget.value })}
                        />
                        <select
                          value={rule.operator}
                          on:change={(e) =>
                            updatePaginationStopRule(rule.id, {
                              operator: toPaginationStopOperator(e.currentTarget.value),
                              compareValue: paginationStopOperatorNeedsValue(toPaginationStopOperator(e.currentTarget.value))
                                ? rule.compareValue
                                : ''
                            })}
                        >
                          {#each PAGINATION_STOP_OPERATORS as op}
                            <option value={op.value}>{op.label}</option>
                          {/each}
                        </select>
                        <input
                          value={rule.compareValue}
                          placeholder={"Значение или {{alias}} / {{body.path}}"}
                          on:input={(e) => updatePaginationStopRule(rule.id, { compareValue: e.currentTarget.value })}
                          disabled={!paginationStopOperatorNeedsValue(rule.operator)}
                        />
                        <button type="button" class="chip-remove" title="Удалить условие" on:click={() => removePaginationStopRule(rule.id)}>x</button>
                      </div>
                    {/each}
                  </div>
                {:else}
                  <p class="hint">Добавь условие, если нужно останавливать пагинацию по значению в ответе.</p>
                {/if}
                <p class="hint small-hint">Поддержка параметров: <code>&#123;&#123;alias&#125;&#125;</code>, <code>&#123;&#123;body.path&#125;&#125;</code>, <code>&#123;&#123;query.path&#125;&#125;</code>, <code>&#123;&#123;headers.Name&#125;&#125;</code>.</p>
              </div>
              <p class="hint small-hint">Причина остановки показывается в результате проверки для каждого запроса.</p>
            </div>
          {:else}
            <p class="hint">Пагинация отключена. Включи переключатель, затем добавь параметры через «Параметр +».</p>
          {/if}
        </div>

        <div class="targets-wrap">
          <div class="targets-head">
            <div class="targets-title">Выходные параметры</div>
          </div>
          <div class="crumbs-panel">
            <div class="crumbs-title-row">
              <div class="crumbs-title">Витрина</div>
              <button class="icon-btn plus-dark" type="button" title="Добавить путь из ответа" on:click={() => (responsePathPickerOpen = !responsePathPickerOpen)}>+</button>
            </div>
            {#if responsePathPickerOpen}
              <div class="crumbs-picker">
                <select bind:value={responsePathPick}>
                  {#each responsePathOptions as opt}
                    <option value={opt}>{opt}</option>
                  {/each}
                </select>
                <button class="icon-btn plus-green" type="button" title="Добавить путь" on:click={addPickedPathFromPicker} disabled={!responsePathPick}>+</button>
              </div>
            {/if}
            {#if !(selected?.pickedPaths?.length)}
              <p class="hint">Отметь узлы в дереве ответа, они появятся здесь.</p>
            {:else}
              <div class="crumbs-list">
                {#each selected?.pickedPaths || [] as pth (pth)}
                  <div class="crumb-chip" draggable="true" on:dragstart={(e) => onPathChipDragStart(e, pth)}>
                    <button type="button" class="chip-path" title="Подставить в активное поле" on:click={() => applyPickedResponsePath(pth)}>{pth}</button>
                    <button type="button" class="chip-remove" title="Убрать из витрины" on:click={() => removePickedPath(pth)}>x</button>
                  </div>
                {/each}
              </div>
            {/if}
          </div>

          <div class="mapping-panel">
            <div class="mapping-head">
              <span>Сопоставление</span>
              <span class="mapping-head-right">Ответ API | Таблица | Поле таблицы</span>
            </div>
            {#if !mappingRowsOf(selected).length}
              <p class="hint">Добавь сопоставление и укажи куда писать данные.</p>
            {:else}
              <div class="mapping-list">
                {#each mappingRowsOf(selected) as m (`${m.targetId}:${m.fieldId}`)}
                  <div class="map-row" class:active-map={isActiveResponseField(m.targetId, m.fieldId)}>
                    <select
                      value={m.responsePath}
                      on:focus={() => setActiveResponseField(m.targetId, m.fieldId)}
                      on:click={() => setActiveResponseField(m.targetId, m.fieldId)}
                      on:dragover|preventDefault
                      on:drop={(e) => dropPathToMapping(e, m.targetId, m.fieldId)}
                      on:change={(e) => setMappingRowResponsePath(m.targetId, m.fieldId, e.currentTarget.value)}
                    >
                      <option value="">Ответ API</option>
                      {#each responsePathOptionsFor(m.responsePath) as pathOpt}
                        <option value={pathOpt}>{pathOpt}</option>
                      {/each}
                    </select>
                    <select
                      value={formatQualifiedTable(m.schema, m.table)}
                      on:change={(e) => setMappingRowTable(m.targetId, e.currentTarget.value)}
                    >
                      <option value="">Таблица</option>
                      {#each existingTables as et}
                        <option value={`${et.schema_name}.${et.table_name}`}>{et.schema_name}.{et.table_name}</option>
                      {/each}
                    </select>
                    <select
                      value={m.targetField}
                      on:focus={() => setActiveResponseField(m.targetId, m.fieldId)}
                      on:click={() => setActiveResponseField(m.targetId, m.fieldId)}
                      on:change={(e) => setMappingRowColumn(m.targetId, m.fieldId, e.currentTarget.value)}
                    >
                      <option value="">Поле таблицы</option>
                      {#each tableFieldOptionsFor(m.schema, m.table, m.targetField) as col}
                        <option value={col}>{col}</option>
                      {/each}
                    </select>
                    <button class="icon-btn danger" type="button" on:click={() => removeMappingRow(m.targetId, m.fieldId)} title="Удалить сопоставление">x</button>
                  </div>
                {/each}
              </div>
            {/if}
            <div class="mapping-actions">
              <button class="icon-btn plus-dark" type="button" title="Добавить сопоставление" on:click={addMappingRow}>+</button>
            </div>
          </div>
        </div>
      </div>
    </div>

    <aside class="aside saved-aside">
      <div class="aside-title">Список API</div>
      <div class="storage-meta">
        <span>Хранятся в таблице:</span>
        <button
          class="link-btn"
          on:click={() => {
            api_storage_picker_open = !api_storage_picker_open;
            api_storage_pick_value = `${api_storage_schema}.${api_storage_table}`;
          }}
        >
          {api_storage_schema}.{api_storage_table}
        </button>
      </div>
      <div class="storage-hint">Выбранный API-шаблон загружается из этой таблицы и используется для сборки запроса.</div>

      {#if api_storage_picker_open}
        <div class="storage-picker">
          <select bind:value={api_storage_pick_value}>
            {#each existingTables as t}
              <option value={`${t.schema_name}.${t.table_name}`}>{t.schema_name}.{t.table_name}</option>
            {/each}
          </select>
          <button on:click={applyStorageChoice} disabled={!api_storage_pick_value}>Подключить</button>
        </div>
      {/if}

      <div class="current-template-box">
        <small class="current-template-label">Текущий шаблон</small>
        <div class="current-template-name">{selected?.name || 'Шаблон не выбран'}</div>
      </div>

      <div class="template-controls">
        <input
          class="template-name {nameDuplicateHint ? 'warn' : ''}"
          value={nameDraft}
          on:input={(e) => {
            nameDraft = e.currentTarget.value;
            if (selected) mutateSelected((d) => (d.name = e.currentTarget.value));
          }}
          placeholder="Название API"
        />
        {#if nameDuplicateHint}
          <div class="name-warn">{nameDuplicateHint}</div>
        {/if}
        <div class="saved-inline-actions">
          <button class="primary template-main-btn" on:click={addApi} disabled={!canAddTemplate}>Добавить</button>
          <button class="primary template-main-btn" on:click={saveSelected} disabled={!canSaveSelected}>{saving ? 'Сохранение...' : 'Сохранить'}</button>
        </div>
      </div>

      <div class="template-list-controls">
        <input
          class="template-search"
          value={templateSearch}
          placeholder="Поиск: название, описание, путь, метод, код"
          on:input={(e) => (templateSearch = e.currentTarget.value)}
        />
        <div class="template-filters-row">
          <select bind:value={templateScopeFilter}>
            <option value="all">Все ({templateScopeCounts.all})</option>
            <option value="recent">Недавние ({templateScopeCounts.recent})</option>
            <option value="favorites">Избранные ({templateScopeCounts.favorites})</option>
          </select>
          <select bind:value={templateSectionFilter}>
            <option value="all">Все разделы</option>
            {#each templateSectionOptions as sec}
              <option value={sec}>{sec}</option>
            {/each}
          </select>
          <select bind:value={templateMethodFilter}>
            <option value="all">Все методы</option>
            <option value="GET">GET</option>
            <option value="POST">POST</option>
            <option value="PUT">PUT</option>
            <option value="PATCH">PATCH</option>
            <option value="DELETE">DELETE</option>
          </select>
        </div>
      </div>

      <div class="list api-list">
        {#if templateGroupedItems.length}
          {#each templateGroupedItems as group (group.section)}
            <div class="template-group">
              <button
                type="button"
                class="template-group-head"
                on:click={() => toggleTemplateSection(group.section)}
                aria-expanded={!isTemplateSectionCollapsed(group.section)}
              >
                <span>{isTemplateSectionCollapsed(group.section) ? '▸' : '▾'} {group.section}</span>
                <span class="template-group-count">{group.items.length}</span>
              </button>
              {#if !isTemplateSectionCollapsed(group.section)}
                {#each group.items as item (item.ref)}
                  <div class="row-item" class:activeitem={item.ref === selectedRef}>
                    <button type="button" class="item-button" on:click={() => applyTemplateSelection(item.ref)}>
                      <div class="row-name">{item.draft.name}</div>
                      <div class="row-meta">
                        <span class="method-pill">{item.method}</span>
                        <span class="path-pill">{item.path}</span>
                        {#if item.templateCode}
                          <span class="code-pill">{item.templateCode}</span>
                        {/if}
                      </div>
                    </button>
                    <div class="row-actions">
                      <button
                        type="button"
                        class="icon-btn fav-icon-btn"
                        class:active-favorite={isTemplateFavorite(item.ref)}
                        on:click|stopPropagation={() => toggleTemplateFavorite(item.ref)}
                        title={isTemplateFavorite(item.ref) ? 'Убрать из избранного' : 'Добавить в избранное'}
                      >
                        {isTemplateFavorite(item.ref) ? '★' : '☆'}
                      </button>
                      <button type="button" class="danger icon-btn" on:click|stopPropagation={() => deleteApi(item.draft)} title="Удалить API">x</button>
                    </div>
                  </div>
                {/each}
              {/if}
            </div>
          {/each}
        {:else}
          <p class="hint">{templateListEmptyHint}</p>
        {/if}
      </div>

      <div class="subsec">
        <div class="subttl template-head">
          <span>Шаблон API</span>
          <span class="inline-actions">
            <button type="button" class="view-toggle template-action-btn view-toggle-primary" on:click={onTemplateParseClick}>Разобрать</button>
            <button type="button" class="view-toggle template-action-btn view-toggle-primary" on:click={onTemplateClearClick}>Очистить</button>
            {#if exampleIsJson}
              <button type="button" class="view-toggle template-action-btn" on:click={() => (exampleViewMode = exampleViewMode === 'tree' ? 'raw' : 'tree')}>
                {exampleViewMode === 'tree' ? 'RAW' : 'Дерево'}
              </button>
            {/if}
          </span>
        </div>
        {#if exampleIsJson && exampleViewMode === 'tree'}
          <div class="response-tree-wrap">
            <JsonTreeView node={exampleJson} name="template" level={0} />
          </div>
        {:else}
          <textarea
            bind:this={exampleApiEl}
            value={selected?.exampleRequest || ''}
            on:input={(e) => {
              mutateSelected((d) => (d.exampleRequest = e.currentTarget.value));
              syncLeftTextareasHeight();
            }}
            placeholder="Вставьте пример API"
          ></textarea>
        {/if}
        <div class="template-parse-actions">
          {#if templateParseMessage}
            <span class="template-parse-note">{templateParseMessage}</span>
          {/if}
        </div>
      </div>
    </aside>
  </div>
</section>

<style>
  .panel { background:#fff; border:1px solid #e6eaf2; border-radius:18px; padding:14px; box-shadow:0 6px 20px rgba(15,23,42,.05); margin-top:12px; min-width:0; }
  .panel-head { display:flex; align-items:center; justify-content:space-between; gap:12px; }
  .panel-head h2 { margin:0; font-size:18px; }

  .layout {
    display:grid;
    grid-template-columns: minmax(260px, 0.9fr) minmax(0, 1.8fr) minmax(280px, 1fr);
    gap:12px;
    margin-top:12px;
    align-items:start;
  }
  .layout > * { min-width:0; }
  .panel-embedded .layout { grid-template-columns: minmax(0, 1fr); gap:10px; }
  .panel-embedded .api-list {
    max-height:min(42vh, 420px);
  }
  .panel-embedded .raw-grid { grid-template-columns: 1fr; }
  .panel-embedded .connect-row,
  .panel-embedded .oauth-subreq-grid,
  .panel-embedded .oauth2-map-row,
  .panel-embedded .template-filters-row,
  .panel-embedded .data-row,
  .panel-embedded .table-rule-row,
  .panel-embedded .join-rule-row,
  .panel-embedded .filter-rule-row,
  .panel-embedded .param-row,
  .panel-embedded .date-param-inline-row,
  .panel-embedded .field-date-row,
  .panel-embedded .pagination-param-inline-row,
  .panel-embedded .pagination-stop-rule-row,
  .panel-embedded .saved-inline-actions {
    grid-template-columns: 1fr;
  }
  .panel-embedded .connect-actions { justify-content:flex-start; flex-wrap:wrap; }
  .panel-embedded .response-head,
  .panel-embedded .inline-actions,
  .panel-embedded .param-source-head .inline-actions {
    flex-wrap:wrap;
  }
  .panel-embedded .response-head > span:first-child,
  .panel-embedded .param-source-head small,
  .panel-embedded .group-toggle {
    white-space:normal;
  }
  .panel-embedded .storage-picker {
    flex-wrap:wrap;
  }
  .panel-embedded .row-item {
    grid-template-columns: 1fr;
  }
  .panel-embedded .row-actions {
    min-width:0;
    justify-content:flex-end;
  }
  .panel-embedded .pagination-numeric-grid,
  .panel-embedded .pagination-boolean-list {
    grid-template-columns: 1fr;
  }
  .panel-embedded .dataset-preview-table {
    min-width:520px;
  }
  @media (max-width: 1500px) { .layout { grid-template-columns: 1fr; } }

  .aside { border:1px solid #e6eaf2; border-radius:16px; padding:12px; background:#f8fafc; min-width:0; }
  .saved-aside {
    display:flex;
    flex-direction:column;
    min-height:0;
  }
  .aside-title { font-weight:700; font-size:14px; line-height:1.3; margin-bottom:8px; }

  .subsec { margin-top:10px; }
  .subttl { font-size:12px; color:#475569; margin-bottom:6px; }
  .response-head { display:flex; align-items:center; justify-content:space-between; gap:8px; flex-wrap:wrap; }
  .response-head > span:first-child { white-space:nowrap; }
  .field-head { justify-content:flex-start; }
  .inline-actions {
    display:inline-flex;
    align-items:center;
    gap:6px;
    flex-wrap:wrap;
    margin-left:auto;
  }
  .view-toggle { border-radius:10px; border:1px solid #e2e8f0; background:#fff; color:#0f172a; padding:4px 8px; font-size:11px; line-height:1.2; }
  .template-action-btn { min-height:28px; min-width:72px; padding:5px 10px; font-size:11px; font-weight:500; }
  .view-toggle.view-toggle-primary { background:#0f172a; border-color:#0f172a; color:#fff; }
  .view-toggle.view-toggle-primary:hover:not(:disabled) { background:#1e293b; border-color:#1e293b; }
  .response-tree-wrap { border:1px solid #e6eaf2; border-radius:12px; background:#fff; padding:8px; min-height:78px; overflow:visible; }
  .template-head { display:flex; align-items:center; justify-content:space-between; gap:8px; }
  .statusline { font-size:12px; color:#64748b; margin-bottom:6px; }
  .metrics-row { display:flex; gap:10px; font-size:12px; color:#475569; margin-bottom:8px; flex-wrap:wrap; }
  .metrics-row span { font-weight:500; }
  .template-parse-actions { margin-top:8px; display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
  .template-parse-note { font-size:12px; color:#64748b; }

  .main { min-width:0; }
  .card { border:1px solid #e6eaf2; border-radius:16px; padding:12px; background:#fff; }

  .connect-row { margin-top:10px; display:grid; grid-template-columns: minmax(180px, 260px) minmax(0, 1fr) auto; gap:8px; align-items:center; }
  .connect-row > * { min-width:0; }
  .connect-actions { display:flex; gap:8px; align-items:center; justify-content:flex-end; }
  .connect-actions .primary { min-width:130px; }
  .targets-wrap { margin-top:10px; border:1px solid #e6eaf2; border-radius:12px; padding:10px; background:transparent; }
  .targets-head { display:flex; align-items:center; justify-content:space-between; gap:8px; margin-bottom:8px; }
  .targets-title { font-size:13px; font-weight:700; color:#0f172a; }
  .crumbs-panel { border:1px dashed #dbe3ef; border-radius:12px; padding:8px; background:#fff; margin-bottom:10px; }
  .crumbs-title-row { display:flex; align-items:center; justify-content:space-between; gap:8px; }
  .crumbs-title { font-size:12px; font-weight:600; color:#334155; margin-bottom:0; }
  .crumbs-picker { margin:6px 0 8px; display:grid; grid-template-columns:1fr auto; gap:8px; align-items:center; }
  .crumbs-list { display:flex; flex-wrap:wrap; gap:6px; }
  .crumb-chip { display:inline-flex; align-items:center; gap:4px; border:1px solid #e2e8f0; border-radius:999px; background:#f8fafc; padding:3px 6px; max-width:100%; }
  .chip-path { border:0; background:transparent; color:#0f172a; padding:0; font-size:11px; max-width:420px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .chip-remove { border:0; background:transparent; color:#b91c1c; padding:0 2px; font-size:12px; line-height:1; }
  .mapping-panel { border:1px solid #e6eaf2; border-radius:12px; background:transparent; padding:8px; }
  .mapping-head { display:flex; align-items:center; justify-content:space-between; gap:8px; margin-bottom:8px; }
  .mapping-head-right { font-size:11px; color:#64748b; }
  .mapping-list { display:flex; flex-direction:column; gap:8px; }
  .map-row { display:grid; grid-template-columns: 1.2fr 1fr 1fr auto; gap:8px; align-items:center; border:1px solid #eef2f7; border-radius:10px; padding:6px; background:transparent; }
  .map-row.active-map { border-color:#cbd5e1; background:#f8fafc; }
  .mapping-actions { margin-top:8px; display:flex; justify-content:flex-end; }
  .desc { width:100%; box-sizing:border-box; margin-top:8px; min-height:56px; resize:vertical; }
  .raw-grid { display:grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap:10px; margin-top:8px; }
  .field-param-showcase { margin:8px 0; display:flex; flex-wrap:wrap; gap:6px; }
  .param-token-chip {
    width:auto;
    border:1px dashed #cbd5e1;
    border-radius:999px;
    background:#f8fafc;
    color:#334155;
    padding:4px 10px;
    font-size:11px;
    line-height:1.2;
    cursor:grab;
  }
  .param-token-chip:active { cursor:grabbing; }

  .storage-meta { margin:0 0 8px; display:flex; align-items:center; gap:6px; font-size:12px; color:#64748b; }
  .storage-hint { margin:-2px 0 8px; font-size:11px; color:#64748b; }
  .link-btn { border:0; background:transparent; color:#0f172a; padding:0; text-decoration:underline; font-size:12px; font-weight:500; }
  .storage-picker { display:flex; gap:8px; align-items:center; margin-bottom:8px; }
  .storage-picker select { flex:1; min-width:0; }
  .template-controls { margin-bottom:8px; display:grid; gap:8px; }
  .template-name { width:100%; box-sizing:border-box; }
  .template-name.warn { border-color:#f59e0b; background:#fffbeb; }
  .name-warn { font-size:12px; color:#92400e; margin-top:-2px; }
  .current-template-box {
    margin-bottom:8px;
    padding:8px 10px;
    border:1px solid #e2e8f0;
    border-radius:10px;
    background:#f8fafc;
    display:flex;
    align-items:baseline;
    gap:8px;
    flex-wrap:wrap;
  }
  .current-template-label {
    display:inline;
    margin:0;
    font-size:11px;
    color:#64748b;
  }
  .current-template-name {
    font-size:13.5px;
    line-height:1.2;
    font-weight:600;
    color:#0f172a;
    word-break:break-word;
  }
  .saved-inline-actions { display:grid; grid-template-columns:1fr 1fr; gap:8px; }
  .template-main-btn { font-weight:600; }
  .template-list-controls {
    display:flex;
    flex-direction:column;
    gap:8px;
    margin-bottom:8px;
  }
  .template-filters-row {
    display:grid;
    grid-template-columns: minmax(0, 1fr) minmax(0, 1.3fr) minmax(0, 1fr);
    gap:8px;
  }
  .template-group { display:flex; flex-direction:column; gap:6px; }
  .template-group-head {
    width:100%;
    border:1px solid #e2e8f0;
    border-radius:10px;
    background:#f8fafc;
    padding:6px 8px;
    display:flex;
    align-items:center;
    justify-content:space-between;
    font-size:12px;
    color:#334155;
    text-align:left;
  }
  .template-group-count {
    min-width:20px;
    height:20px;
    border-radius:999px;
    border:1px solid #cbd5e1;
    background:#fff;
    color:#334155;
    font-size:11px;
    line-height:18px;
    text-align:center;
    padding:0 6px;
    box-sizing:border-box;
  }

  .list { display:flex; flex-direction:column; gap:8px; min-width:0; }
  .api-list {
    flex:1 1 auto;
    min-height:clamp(220px, 30vh, 420px);
    max-height:min(58vh, 720px);
    overflow:auto;
    padding-right:4px;
  }
  .row-item { display:grid; grid-template-columns: 1fr auto; gap:8px; align-items:center; border:1px solid #e6eaf2; border-radius:14px; background:#0f172a; padding:8px 10px; }
  .row-actions { display:flex; align-items:center; justify-content:flex-end; gap:4px; min-width:84px; }
  .item-button { text-align:left; border:0; background:transparent; padding:0; color:inherit; width:100%; }
  .row-name { font-weight:400; font-size:13px; line-height:1.25; word-break:break-word; color:#fff; }
  .row-meta { font-size:12px; color:#cbd5e1; margin-top:4px; word-break: break-word; display:flex; flex-wrap:wrap; gap:6px; align-items:center; }
  .method-pill,
  .path-pill,
  .code-pill {
    border:1px solid #334155;
    border-radius:999px;
    background:rgba(15,23,42,.25);
    color:#cbd5e1;
    padding:1px 8px;
    font-size:11px;
    line-height:1.3;
  }
  .path-pill {
    max-width:100%;
    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
  }
  .api-list .activeitem { background:#fff; border-color:#e6eaf2; color:#0f172a; }
  .api-list .activeitem .row-name { font-size:15px; font-weight:600; letter-spacing:.01em; color:#0f172a; }
  .api-list .activeitem .row-meta { color:#64748b; }
  .api-list .activeitem .method-pill,
  .api-list .activeitem .path-pill,
  .api-list .activeitem .code-pill {
    border-color:#cbd5e1;
    background:#f8fafc;
    color:#475569;
  }

  .icon-btn { width:34px; min-width:34px; padding:6px 0; font-size:14px; text-transform:uppercase; border-color:transparent; background:transparent; color:#fff; }
  .refresh-btn { color:#16a34a; }
  .danger.icon-btn { color:#b91c1c; }
  .fav-icon-btn { font-size:15px; }
  .api-list .row-item:not(.activeitem) .row-actions .danger.icon-btn { color:#f8fafc; }
  .api-list .row-item:not(.activeitem) .row-actions .fav-icon-btn { color:#e2e8f0; }
  .api-list .row-item:not(.activeitem) .row-actions .fav-icon-btn.active-favorite { color:#fff; }
  .api-list .activeitem .row-actions .danger.icon-btn { color:#b91c1c; }
  .api-list .activeitem .row-actions .fav-icon-btn { color:#64748b; }
  .api-list .activeitem .row-actions .fav-icon-btn.active-favorite { color:#d97706; }
  .plus-dark.icon-btn { color:#0f172a; font-weight:700; }
  .plus-green.icon-btn { color:#16a34a; font-weight:700; }
  .map-row .icon-btn { color:#b91c1c; }

  input, select, textarea { border-radius:14px; border:1px solid #e6eaf2; padding:10px 12px; outline:none; background:#fff; box-sizing:border-box; width:100%; }
  textarea { min-height:78px; resize:none; overflow:hidden; }

  button { border-radius:14px; border:1px solid #e6eaf2; padding:10px 12px; background:#fff; cursor:pointer; }
  button:disabled { opacity:.6; cursor:not-allowed; }
  .primary { background:#0f172a; color:#fff; border-color:#0f172a; }

  .hint { margin:0; color:#64748b; font-size:13px; }
  .alert { margin: 12px 0; padding: 10px 12px; border-radius: 14px; border: 1px solid #f3c0c0; background: #fff5f5; }
  .alert-title { font-weight: 700; margin-bottom: 6px; }
  .okbox { margin: 12px 0; padding: 10px 12px; border-radius: 14px; border: 1px solid #bbf7d0; background: #f0fdf4; color:#166534; }
  .warnbox { margin: 12px 0; padding: 10px 12px; border-radius: 14px; border: 1px solid #fcd34d; background: #fffbeb; color:#92400e; }
  pre { margin:0; white-space: pre-wrap; word-break: break-word; }

  .auth-section { display:block; margin-top:14px; }
  .auth-mode-btn { width:auto; min-width:112px; }
  .auth-subrequest-box {
    margin:10px 0 12px;
    border:1px solid #e6eaf2;
    border-radius:12px;
    background:#f8fafc;
    padding:10px;
    display:flex;
    flex-direction:column;
    gap:8px;
  }
  .auth-subrequest-content {
    display:flex;
    flex-direction:column;
    gap:8px;
  }
  .oauth-subreq-grid {
    display:grid;
    grid-template-columns: minmax(120px, 0.6fr) minmax(320px, 1.8fr);
    gap:8px;
    align-items:center;
  }
  .oauth2-map-row {
    grid-template-columns: minmax(240px, 1.3fr) minmax(220px, 1fr) auto;
    align-items:center;
  }
  .pagination-box { margin-top:10px; border:1px solid #e6eaf2; border-radius:12px; padding:10px; background:#f8fafc; }
  .dispatch-box { margin-top:10px; margin-bottom:14px; border:1px solid #e6eaf2; border-radius:12px; padding:10px; background:#f8fafc; }
  .parameter-subhead { margin-top:12px; }
  .data-builder-box { border:1px solid #e2e8f0; border-radius:12px; padding:10px; background:#fff; margin-top:8px; display:flex; flex-direction:column; gap:10px; }
  .data-section { display:flex; flex-direction:column; gap:6px; }
  .data-helper-note {
    margin:14px 0 10px;
    padding:8px 10px;
    border:1px dashed #dbe4ef;
    border-radius:10px;
    background:#f8fafc;
  }
  .parameter-sources-grid {
    display:grid;
    grid-template-columns: minmax(0, 1fr);
    gap:10px;
    align-items:start;
  }
  .source-card {
    border:1px solid #e2e8f0;
    border-radius:10px;
    background:#fff;
    padding:8px;
    min-height:100%;
    min-width:0;
  }
  .data-list { display:flex; flex-direction:column; gap:8px; }
  .data-row { display:grid; gap:8px; align-items:center; }
  .crumb-strip { display:flex; flex-wrap:wrap; gap:6px; }
  .entity-crumb-wrap {
    display:inline-flex;
    align-items:center;
    gap:4px;
    border:1px solid #cbd5e1;
    border-radius:999px;
    background:#fff;
    color:#334155;
    padding:0 6px 0 0;
  }
  .entity-crumb-wrap.active-crumb-wrap { border-color:#0f172a; background:#0f172a; color:#fff; }
  .entity-crumb {
    width:auto;
    border:1px solid #cbd5e1;
    border-radius:999px;
    background:#fff;
    color:#334155;
    padding:6px 10px;
    font-size:12px;
    line-height:1.2;
  }
  .entity-crumb-wrap .entity-crumb {
    border:0;
    background:transparent;
    color:inherit;
    padding:6px 4px 6px 10px;
  }
  .entity-crumb-remove {
    width:20px;
    height:20px;
    border:0;
    border-radius:999px;
    background:#fff;
    color:#b91c1c;
    display:inline-flex;
    align-items:center;
    justify-content:center;
    font-size:13px;
    line-height:1;
    padding:0;
  }
  .entity-crumb-remove.active-crumb-remove {
    border:0;
    background:rgba(255,255,255,.12);
    color:#f8fafc;
  }
  .rule-card { border:1px solid #e2e8f0; border-radius:10px; background:#fff; padding:8px; display:flex; flex-direction:column; gap:8px; min-width:0; }
  .rule-card-head { display:flex; align-items:center; justify-content:space-between; gap:8px; }
  .table-rule-row { grid-template-columns: 1.2fr 1fr 1fr; }
  .join-rule-row { grid-template-columns: 1fr 1fr 80px 1fr 1fr 1fr; }
  .filter-rule-row { grid-template-columns: 1.2fr 1fr 1fr 1fr; }
  .param-source-head { justify-content:space-between; align-items:center; gap:8px; }
  .param-source-head small { white-space:nowrap; }
  .param-source-head .inline-actions { margin-left:0; flex-wrap:wrap; justify-content:flex-end; }
  .param-row {
    grid-template-columns:
      minmax(140px, 2fr)
      minmax(140px, 2fr)
      minmax(280px, 4.5fr)
      minmax(110px, 1fr)
      minmax(64px, 0.65fr)
      minmax(28px, 0.3fr);
  }
  .param-row > * { min-width:0; }
  .date-param-inline-row {
    grid-template-columns:
      minmax(150px, 1.7fr)
      minmax(150px, 1.5fr)
      minmax(260px, 4fr)
      minmax(110px, 1fr)
      minmax(28px, 0.3fr);
  }
  .field-date-row { grid-template-columns: 1fr 140px 140px 1fr; align-items:end; }
  .group-toggle {
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:8px;
    border:1px solid #e2e8f0;
    border-radius:10px;
    background:#f8fafc;
    padding:8px 10px;
    font-size:12px;
    color:#334155;
    min-height:34px;
    white-space:normal;
  }
  .group-toggle-sm {
    padding:6px 9px;
    min-height:30px;
    font-size:11px;
    border-radius:9px;
  }
  .group-toggle.active-group-toggle {
    border-color:#0f172a;
    background:#0f172a;
    color:#f8fafc;
  }
  .group-toggle-indicator {
    width:8px;
    height:8px;
    border-radius:999px;
    background:#22c55e;
    box-shadow:0 0 0 2px rgba(34,197,94,.25);
  }
  .row-order-actions {
    display:flex;
    align-items:center;
    justify-content:flex-end;
    gap:4px;
    min-width:0;
  }
  .row-icon-btn {
    border:0;
    background:transparent;
    color:#334155;
    width:24px;
    min-width:24px;
    height:24px;
    padding:0;
    border-radius:6px;
    font-size:12px;
    line-height:1;
  }
  .row-icon-btn:hover:not(:disabled) {
    background:#eef2f7;
    color:#0f172a;
  }
  .row-icon-btn:disabled {
    opacity:.35;
    cursor:not-allowed;
  }
  .preview-limit-inline {
    display:inline-flex;
    align-items:center;
    gap:8px;
  }
  .preview-limit-inline small {
    font-size:11px;
    color:#64748b;
    white-space:nowrap;
  }
  .preview-limit-inline input {
    width:74px;
    padding:6px 8px;
    border-radius:10px;
  }
  .dataset-preview-table-wrap { overflow:auto; border:1px solid #e2e8f0; border-radius:10px; background:#fff; }
  .dataset-preview-table { width:100%; border-collapse:collapse; min-width:640px; }
  .dataset-preview-table th,
  .dataset-preview-table td { border-bottom:1px solid #edf2f7; padding:8px; text-align:left; font-size:12px; vertical-align:top; }
  .dataset-preview-table th { background:#f8fafc; color:#334155; font-weight:600; position:sticky; top:0; z-index:1; }
  .dataset-preview-table td { max-width:320px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; color:#0f172a; }
  .empty-preview-state { min-height:96px; display:flex; flex-direction:column; align-items:flex-start; justify-content:center; gap:8px; padding:10px; }
  .pagination-grid { margin-top:8px; display:grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap:8px; }
  .pagination-field small { display:block; margin-bottom:4px; font-size:11px; color:#64748b; }
  .log-mode-grid {
    margin-top:8px;
    display:grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap:8px;
  }
  .pagination-logic-list {
    margin-top:8px;
    display:grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap:8px;
  }
  .pagination-logic-item {
    border:1px solid #e2e8f0;
    border-radius:10px;
    background:#fff;
    padding:8px;
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:8px;
    min-width:0;
  }
  .pagination-logic-item > span {
    font-size:12px;
    color:#334155;
    line-height:1.3;
  }
  .pagination-safety-layout {
    margin-top:6px;
    display:flex;
    flex-direction:column;
    gap:6px;
    padding:8px;
    border:1px solid #e2e8f0;
    border-radius:10px;
    background:#f8fafc;
  }
  .pagination-numeric-grid {
    display:grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap:8px;
    align-items:start;
  }
  .pagination-setting-item {
    min-width:0;
    border:1px solid #e2e8f0;
    border-radius:10px;
    background:#fff;
    padding:8px;
    display:flex;
    flex-direction:column;
    gap:6px;
  }
  .pagination-boolean-list {
    display:grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap:6px;
  }
  .pagination-bool-row {
    border:1px solid #e2e8f0;
    border-radius:10px;
    background:#fff;
    padding:8px;
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:8px;
  }
  .pagination-setting-head {
    display:flex;
    align-items:flex-start;
    justify-content:space-between;
    gap:8px;
  }
  .pagination-setting-title {
    margin:0;
    font-size:12px;
    font-weight:600;
    line-height:1.3;
    color:#334155;
    flex:1;
    min-width:0;
  }
  .pagination-inline-toggle {
    width:64px;
    min-width:64px;
    height:24px;
    padding:0 8px;
    border:1px solid #cbd5e1;
    border-radius:999px;
    background:#fff;
    color:#334155;
    font-size:11px;
    line-height:1.2;
    display:inline-flex;
    align-items:center;
    justify-content:center;
    gap:5px;
    white-space:nowrap;
    flex:0 0 64px;
  }
  .pagination-inline-toggle.active-inline-toggle {
    border-color:#0f172a;
    background:#0f172a;
    color:#f8fafc;
  }
  .pagination-inline-indicator {
    width:7px;
    height:7px;
    border-radius:999px;
    background:#22c55e;
    box-shadow:0 0 0 2px rgba(34,197,94,.2);
  }
  .pagination-value-input {
    width:100%;
    padding:7px 8px;
    border-radius:10px;
  }
  .pagination-value-input:disabled {
    background:#f8fafc;
    color:#94a3b8;
  }
  .pagination-param-editor { margin-top:8px; }
  .pagination-param-helper {
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:8px;
  }
  .pagination-param-helper .hint {
    flex:1;
    font-size:12px;
    line-height:1.3;
  }
  .pagination-param-helper .icon-btn {
    width:30px;
    min-width:30px;
    height:30px;
    padding:0;
    border-radius:8px;
    border:1px solid #e2e8f0;
    background:#fff;
  }
  .pagination-param-grid { margin-top:0; }
  .pagination-param-inline-row {
    grid-template-columns: minmax(160px, 220px) minmax(220px, 1fr) minmax(220px, 1fr) minmax(220px, 1fr);
    align-items:end;
  }
  .pagination-stop-rules {
    margin-top:8px;
    display:flex;
    flex-direction:column;
    gap:8px;
  }
  .pagination-stop-rules-list {
    display:flex;
    flex-direction:column;
    gap:8px;
  }
  .pagination-stop-rule-row {
    grid-template-columns: minmax(180px, 1fr) minmax(180px, 1fr) 180px 160px auto;
    align-items:end;
    padding:8px;
    border:1px solid #e2e8f0;
    border-radius:10px;
    background:#fff;
  }
  .pagination-stop-rule-row .chip-remove {
    align-self:center;
    justify-self:center;
    font-size:14px;
  }
  .json-code-editor {
    font-family: "Cascadia Mono", Consolas, "SFMono-Regular", Menlo, Monaco, monospace;
    font-size:12px;
    line-height:1.45;
    white-space:pre;
    tab-size:2;
    min-height:150px;
  }
  .definition-error { margin:0; font-size:11px; color:#b91c1c; }
  @media (max-width: 900px) {
    .connect-row { grid-template-columns: 1fr; }
    .connect-actions { justify-content:flex-start; flex-wrap:wrap; }
    .raw-grid { grid-template-columns: 1fr; }
    .saved-inline-actions { grid-template-columns: 1fr; }
    .api-list { min-height:220px; max-height:min(50vh, 460px); }
    .parameter-sources-grid { grid-template-columns: 1fr; }
    .oauth-subreq-grid,
    .oauth2-map-row {
      grid-template-columns: 1fr;
    }
    .data-row, .table-rule-row, .join-rule-row, .filter-rule-row, .param-row, .date-param-inline-row, .field-date-row { grid-template-columns: 1fr; }
    .pagination-numeric-grid {
      grid-template-columns: 1fr;
    }
    .pagination-boolean-list {
      grid-template-columns: 1fr;
    }
    .pagination-param-inline-row,
    .pagination-stop-rule-row {
      grid-template-columns: 1fr;
    }
  }
</style>









