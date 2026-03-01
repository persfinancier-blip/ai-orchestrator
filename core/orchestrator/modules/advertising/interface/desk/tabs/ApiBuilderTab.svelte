<script lang="ts">
  import { tick } from 'svelte';
  import JsonTreeView from '../components/JsonTreeView.svelte';
  export type ExistingTable = { schema_name: string; table_name: string };
  type HttpMethod = 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE';
  type DispatchMode = 'single' | 'group_by';
  type BindingTarget = 'header' | 'query' | 'body' | 'body_item';
  export let apiBase: string;
  export let apiJson: <T = any>(url: string, init?: RequestInit) => Promise<T>;
  export let headers: () => Record<string, string>;
  export let existingTables: ExistingTable[] = [];
  export let refreshTables: () => Promise<void>;

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
  };

  type DataModelFilter = {
    id: string;
    tableId: string;
    field: string;
    operator: string;
    compareValue: string;
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
    paginationMaxPages: number;
    paginationDelayMs: number;
    pickedPaths: string[];
    responseTargets: Array<{
      id: string;
      schema: string;
      table: string;
      fields: Array<{ id: string; responsePath: string; targetField: string }>;
    }>;
    description: string;
    exampleRequest: string;
    parameterDefinitions: ParameterDefinition[];
    dispatchMode: DispatchMode;
    groupByAliases: string[];
    bodyItemsPath: string;
    previewRequestLimit: number;
    bindingRules: BindingRule[];
    dataTables: DataModelTable[];
    dataJoins: DataModelJoin[];
    dataFields: DataModelField[];
    dataFilters: DataModelFilter[];
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
  const BINDING_TARGETS: Array<{ value: BindingTarget; label: string }> = [
    { value: 'header', label: 'Header' },
    { value: 'query', label: 'Query' },
    { value: 'body', label: 'Body' },
    { value: 'body_item', label: 'Body: элемент массива' }
  ];
  const DATA_JOIN_TYPES: Array<{ value: 'inner' | 'left'; label: string }> = [
    { value: 'inner', label: 'Только совпавшие' },
    { value: 'left', label: 'Оставить все из основной' }
  ];
  const DATA_FILTER_OPERATORS: Array<{ value: string; label: string }> = [
    { value: 'equals', label: '=' },
    { value: 'not_equals', label: '!=' },
    { value: 'contains', label: 'содержит' },
    { value: 'starts_with', label: 'начинается с' },
    { value: 'ends_with', label: 'заканчивается на' },
    { value: 'gt', label: '>' },
    { value: 'lt', label: '<' }
  ];

  const PAGINATION_STRATEGIES = [
    { value: 'none', label: 'Не использовать' },
    { value: 'page_number', label: 'Номер страницы' },
    { value: 'offset_limit', label: 'Смещение + лимит' },
    { value: 'cursor_fields', label: 'Курсоры (две метки)' },
    { value: 'next_url', label: 'Ссылка next' },
    { value: 'custom', label: 'Своя логика' }
  ];

  const PAGINATION_TARGETS = [
    { value: 'query', label: 'query (параметры URL)' },
    { value: 'body', label: 'body (тело запроса)' }
  ];

  const API_STORAGE_REQUIRED_COLUMNS: Array<{ name: string; types: string[] }> = [
    { name: 'api_name', types: ['text', 'character varying', 'varchar'] },
    { name: 'method', types: ['text', 'character varying', 'varchar'] },
    { name: 'base_url', types: ['text', 'character varying', 'varchar'] },
    { name: 'path', types: ['text', 'character varying', 'varchar'] },
    { name: 'headers_json', types: ['jsonb', 'json'] },
    { name: 'query_json', types: ['jsonb', 'json'] },
    { name: 'body_json', types: ['jsonb', 'json'] },
    { name: 'target_schema', types: ['text', 'character varying', 'varchar'] },
    { name: 'target_table', types: ['text', 'character varying', 'varchar'] },
    { name: 'mapping_json', types: ['jsonb', 'json'] },
    { name: 'description', types: ['text', 'character varying', 'varchar'] },
    { name: 'is_active', types: ['boolean'] }
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

  let loading = false;
  let saving = false;
  let checking = false;
  let err = '';
  let ok = '';

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
  let myApiPreviewDraft = '';
  let myPreviewDirty = false;
  let myPreviewApplyMessage = '';
  let authJsonTree: any = null;
  let authJsonValid = false;
  let authViewMode: 'tree' | 'raw' = 'raw';
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
  let groupByAliasCandidates: string[] = [];
  let selectedApiAlias = '';
  const PARAMETER_PREVIEW_LIMIT = 5;
  const REQUEST_PREVIEW_MAX = 20;
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

  function parseJsonObjectField(label: string, text: string): Record<string, any> {
    const src = String(text || '').trim();
    if (!src) return {};
    let parsed: any;
    try {
      parsed = JSON.parse(src);
    } catch {
      const loose = parseLooseObject(src);
      if (Object.keys(loose).length) return loose;
      throw new Error(`${label}: некорректный JSON`);
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
      throw new Error(`${label}: некорректный JSON`);
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
    return d.storeId ? `db:${d.storeId}` : `tmp:${d.localId}`;
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
      paginationMaxPages: 3,
      paginationDelayMs: 0,
      paginationCustomStrategy: '',
      pickedPaths: [],
      responseTargets: [],
      description: '',
      exampleRequest: '',
      parameterDefinitions: [],
      dispatchMode: 'single',
      groupByAliases: [],
      bodyItemsPath: 'items',
      previewRequestLimit: 5,
      bindingRules: [],
      dataTables: [],
      dataJoins: [],
      dataFields: [],
      dataFilters: []
    };
  }

  function fromRow(row: any): ApiDraft {
    const d = baseDraft();
    const storeIdRaw = row?.id ?? row?.api_config_id ?? row?.config_id ?? null;
    const storeIdNum = Number(String(storeIdRaw ?? '').trim());

    const mapping = tryObj(row?.mapping_json);
    const legacy = tryObj(mapping?.source ?? mapping?.config ?? mapping?.payload);
    const pagination = tryObj(row?.pagination_json || mapping?.pagination || legacy?.pagination);
    const oauth2 = tryObj(mapping?.oauth2 || legacy?.oauth2);
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
        alias: String(f?.alias || f?.field || `field_${idx + 1}`).trim()
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
      .filter((f: DataModelFilter) => f.tableId && f.field && dataTableIdSet.has(f.tableId));
    const groupByRaw = Array.isArray(row?.group_by_aliases)
      ? row.group_by_aliases
      : Array.isArray(executionCfg?.group_by_aliases)
      ? executionCfg.group_by_aliases
      : [];
    const normalizedGroupBy = [...new Set(groupByRaw.map((x: any) => String(x || '').trim()).filter(Boolean))];
    const dispatchMode = toDispatchMode(String(row?.dispatch_mode || executionCfg?.dispatch_mode || 'single'));
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
    const paginationMaxPagesValue = Number.isFinite(Number(row?.pagination_max_pages))
      ? Number(row?.pagination_max_pages)
      : Number(pagination?.max_pages || 3);
    const paginationDelayMsValue = Number.isFinite(Number(row?.pagination_delay_ms))
      ? Number(row?.pagination_delay_ms)
      : Number(pagination?.delay_ms || 0);
    const paginationCustomStrategyValue = String(
      row?.pagination_custom_strategy || pagination?.custom_strategy || ''
    );

    return {
      ...d,
      localId: uid(),
      storeId: Number.isFinite(storeIdNum) && storeIdNum > 0 ? Math.trunc(storeIdNum) : undefined,
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
      paginationMaxPages: paginationMaxPagesValue,
      paginationDelayMs: paginationDelayMsValue,
      paginationCustomStrategy: paginationCustomStrategyValue,
      pickedPaths: pickedPathsSource.map((x: any) => String(x || '').trim()).filter(Boolean),
      responseTargets: normalizedTargets,
      description: String(row?.description || legacy?.description || ''),
      exampleRequest: exampleRequestValue,
      parameterDefinitions: normalizedDefinitions,
      dispatchMode,
      groupByAliases: normalizedGroupBy,
      bodyItemsPath,
      previewRequestLimit: Math.max(1, Math.min(50, Number(previewRequestLimit || 5))),
      bindingRules: normalizedBindingRules,
      dataTables: normalizedDataTables,
      dataJoins: normalizedDataJoins,
      dataFields: normalizedDataFields,
      dataFilters: normalizedDataFilters
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
        strategy: d.paginationStrategy,
        target: d.paginationTarget,
        data_path: d.paginationDataPath,
        page_param: d.paginationPageParam,
        start_page: d.paginationStartPage,
        limit_param: d.paginationLimitParam,
        limit_value: d.paginationLimitValue,
        cursor_req_path_1: d.paginationCursorReqPath1,
        cursor_req_path_2: d.paginationCursorReqPath2,
        cursor_res_path_1: d.paginationCursorResPath1,
        cursor_res_path_2: d.paginationCursorResPath2,
        next_url_path: d.paginationNextUrlPath,
        max_pages: d.paginationMaxPages,
        delay_ms: d.paginationDelayMs,
        custom_strategy: d.paginationCustomStrategy
      },
      target_schema: firstTarget?.schema || '',
      target_table: firstTarget?.table || '',
      mapping_json: {
        exampleRequest: d.exampleRequest,
      response_targets: d.responseTargets,
      picked_paths: d.pickedPaths,
      oauth2:
        d.authMode === 'oauth2_client_credentials'
          ? {
                mode: d.authMode,
                token_url: d.oauth2TokenUrl,
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
      execution: {
        dispatch_mode: sanitizedAliases.groupByAliases.length ? 'group_by' : 'single',
        group_by_aliases: sanitizedAliases.groupByAliases,
        body_items_path: d.bodyItemsPath,
        preview_request_limit: d.previewRequestLimit,
        data_model: {
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
            alias: f.alias
          })),
          filters: d.dataFilters.map((f) => ({
            id: f.id,
            table_id: f.tableId,
            field: f.field,
            operator: f.operator,
            compare_value: f.compareValue
          }))
        },
        binding_rules: sanitizedAliases.bindingRules.map((rule) => ({
          id: rule.id,
          alias: rule.alias,
          target: rule.target,
          path: rule.path
        }))
      }
    },
    auth_mode: d.authMode,
    auth_json: parsed?.authJson ?? tryObj(d.authJson),
    oauth2_token_url: d.oauth2TokenUrl,
    oauth2_client_id: d.oauth2ClientId,
    oauth2_client_secret: d.oauth2ClientSecret,
    oauth2_grant_type: d.oauth2GrantType,
    oauth2_token_field: d.oauth2TokenField,
    oauth2_expires_field: d.oauth2ExpiresField,
    oauth2_token_type_field: d.oauth2TokenTypeField,
    parameter_definitions: parameterDefinitionsPayload,
    dispatch_mode: sanitizedAliases.groupByAliases.length ? 'group_by' : 'single',
    group_by_aliases: sanitizedAliases.groupByAliases,
    body_items_path: d.bodyItemsPath,
    preview_request_limit: d.previewRequestLimit,
    binding_rules: sanitizedAliases.bindingRules.map((rule) => ({
      id: rule.id,
      alias: rule.alias,
      target: rule.target,
      path: rule.path
    })),
    response_targets: d.responseTargets,
    picked_paths: d.pickedPaths,
    example_request: d.exampleRequest,
    pagination_enabled: d.paginationEnabled,
    pagination_strategy: d.paginationStrategy,
    pagination_target: d.paginationTarget,
    pagination_data_path: d.paginationDataPath,
    pagination_page_param: d.paginationPageParam,
    pagination_start_page: d.paginationStartPage,
    pagination_limit_param: d.paginationLimitParam,
    pagination_limit_value: d.paginationLimitValue,
    pagination_cursor_req_path_1: d.paginationCursorReqPath1,
    pagination_cursor_req_path_2: d.paginationCursorReqPath2,
    pagination_cursor_res_path_1: d.paginationCursorResPath1,
    pagination_cursor_res_path_2: d.paginationCursorResPath2,
    pagination_next_url_path: d.paginationNextUrlPath,
    pagination_max_pages: d.paginationMaxPages,
    pagination_delay_ms: d.paginationDelayMs,
    pagination_custom_strategy: d.paginationCustomStrategy,
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
  $: groupByAliasCandidates = bindingAliasOptions(selected);
  $: if (groupByAliasCandidates.length && !groupByAliasCandidates.includes(selectedApiAlias)) {
    selectedApiAlias = groupByAliasCandidates[0] || '';
  }
  $: if (!groupByAliasCandidates.length) {
    selectedApiAlias = '';
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

  async function getOAuthToken(d: ApiDraft): Promise<{ token: string; tokenType: string }> {
    const cacheKey = refOf(d);
    const cached = oauthTokenCache[cacheKey];
    if (cached && Date.now() < cached.expiresAt - 60_000) {
      return { token: cached.token, tokenType: cached.tokenType || 'Bearer' };
    }

    const tokenUrl = resolveAbsoluteUrl(d.oauth2TokenUrl, d);
    const payload = {
      client_id: d.oauth2ClientId,
      client_secret: d.oauth2ClientSecret,
      grant_type: d.oauth2GrantType || 'client_credentials'
    };
    const res = await fetch(tokenUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
      body: JSON.stringify(payload)
    });
    const txt = await res.text();
    let json: any = {};
    try {
      json = txt ? JSON.parse(txt) : {};
    } catch {
      throw new Error(`OAuth2: некорректный ответ токена (${txt.slice(0, 200)})`);
    }
    if (!res.ok) {
      throw new Error(`OAuth2: ${res.status} ${JSON.stringify(json)}`);
    }
    const tokenField = d.oauth2TokenField || 'access_token';
    const expiresField = d.oauth2ExpiresField || 'expires_in';
    const typeField = d.oauth2TokenTypeField || 'token_type';
    const token = String(json?.[tokenField] || '').trim();
    if (!token) throw new Error(`OAuth2: поле ${tokenField} не найдено`);
    const expiresIn = Number(json?.[expiresField] || 0);
    const tokenType = String(json?.[typeField] || 'Bearer').trim() || 'Bearer';
    oauthTokenCache = {
      ...oauthTokenCache,
      [cacheKey]: {
        token,
        tokenType,
        expiresAt: Date.now() + Math.max(0, expiresIn) * 1000
      }
    };
    return { token, tokenType };
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

  function parseAliasList(raw: string): string[] {
    return [...new Set(String(raw || '').split(/[,\n;]/).map((x) => x.trim()).filter(Boolean))];
  }

  function updateGroupByAliases(raw: string) {
    const next = parseAliasList(raw);
    mutateSelected((d) => {
      d.groupByAliases = next;
    });
  }

  function toggleGroupByAlias(alias: string) {
    const key = String(alias || '').trim();
    if (!key) return;
    mutateSelected((d) => {
      const set = new Set((Array.isArray(d.groupByAliases) ? d.groupByAliases : []).map((x) => String(x || '').trim()).filter(Boolean));
      if (set.has(key)) set.delete(key);
      else set.add(key);
      d.groupByAliases = [...set];
    });
  }

  function isGroupedDispatchEnabled(draft: ApiDraft | null) {
    if (!draft) return false;
    return sanitizeAliasReferences(draft).groupByAliases.length > 0;
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
    field: 'authJson' | 'headersJson' | 'queryJson' | 'bodyJson',
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

  function hasBindingRule(alias: string, pathMode: 'auth' | 'header' | 'query' | 'body') {
    const key = String(alias || '').trim();
    if (!key || !selected) return false;
    const rules = Array.isArray(selected.bindingRules) ? selected.bindingRules : [];
    if (pathMode === 'auth') {
      return rules.some((r) => r.alias === key && r.target === 'header' && String(r.path || '').trim() === 'Authorization');
    }
    if (pathMode === 'header') {
      return rules.some((r) => r.alias === key && r.target === 'header' && String(r.path || '').trim() !== 'Authorization');
    }
    if (pathMode === 'query') {
      return rules.some((r) => r.alias === key && r.target === 'query');
    }
    return rules.some((r) => r.alias === key && (r.target === 'body' || r.target === 'body_item'));
  }

  function setBindingUsage(alias: string, usage: 'auth' | 'header' | 'query' | 'body', enabled: boolean) {
    const key = String(alias || '').trim();
    if (!key) return;
    mutateSelected((d) => {
      let rules = Array.isArray(d.bindingRules) ? [...d.bindingRules] : [];
      if (!enabled) {
        if (usage === 'auth') {
          rules = rules.filter((r) => !(r.alias === key && r.target === 'header' && String(r.path || '').trim() === 'Authorization'));
        } else if (usage === 'header') {
          rules = rules.filter((r) => !(r.alias === key && r.target === 'header' && String(r.path || '').trim() !== 'Authorization'));
        } else if (usage === 'query') {
          rules = rules.filter((r) => !(r.alias === key && r.target === 'query'));
        } else {
          rules = rules.filter((r) => !(r.alias === key && (r.target === 'body' || r.target === 'body_item')));
        }
        d.bindingRules = rules;
        return;
      }

      if (usage === 'auth') {
        if (!rules.some((r) => r.alias === key && r.target === 'header' && String(r.path || '').trim() === 'Authorization')) {
          rules.push({ id: uid(), alias: key, target: 'header', path: 'Authorization' });
        }
      } else if (usage === 'header') {
        if (!rules.some((r) => r.alias === key && r.target === 'header' && String(r.path || '').trim() !== 'Authorization')) {
          rules.push({ id: uid(), alias: key, target: 'header', path: key });
        }
      } else if (usage === 'query') {
        if (!rules.some((r) => r.alias === key && r.target === 'query')) {
          rules.push({ id: uid(), alias: key, target: 'query', path: key });
        }
      } else {
        if (!rules.some((r) => r.alias === key && (r.target === 'body' || r.target === 'body_item'))) {
          rules.push({ id: uid(), alias: key, target: 'body_item', path: key });
        }
      }
      d.bindingRules = rules;
    });
  }

  function toggleBindingUsage(alias: string, usage: 'auth' | 'header' | 'query' | 'body') {
    const enabled = hasBindingRule(alias, usage);
    setBindingUsage(alias, usage, !enabled);
  }

  function isAliasGrouped(alias: string) {
    if (!selected) return false;
    return (selected.groupByAliases || []).includes(alias);
  }

  function hasDataModelConfigured(draft: ApiDraft | null) {
    if (!draft) return false;
    return Array.isArray(draft.dataTables) && draft.dataTables.length > 0 && Array.isArray(draft.dataFields) && draft.dataFields.length > 0;
  }

  function bindingAliasOptions(draft: ApiDraft | null) {
    if (!draft) return [];
    const fromDataModel = (Array.isArray(draft.dataFields) ? draft.dataFields : [])
      .map((f) => String(f?.alias || '').trim())
      .filter(Boolean);
    if (fromDataModel.length) return uniqueAliasList(fromDataModel);
    return uniqueAliasList((Array.isArray(draft.parameterDefinitions) ? draft.parameterDefinitions : []).map((p) => String(p?.alias || '').trim()));
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

    const groupByAliases = uniqueAliasList((Array.isArray(draft.groupByAliases) ? draft.groupByAliases : []).map(canonicalAlias).filter(Boolean));
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
  }

  function addDataJoin() {
    if (!selected) return;
    const tables = selected.dataTables || [];
    const left = tables[0]?.id || '';
    const right = tables[1]?.id || tables[0]?.id || '';
    mutateSelected((d) => {
      d.dataJoins = [
        ...(Array.isArray(d.dataJoins) ? d.dataJoins : []),
        {
          id: uid(),
          leftTableId: left,
          leftField: '',
          rightTableId: right,
          rightField: '',
          joinType: 'inner'
        }
      ];
    });
    if (left) ensureDataTableColumnsLoaded(left);
    if (right) ensureDataTableColumnsLoaded(right);
  }

  function updateDataJoin(joinId: string, patch: Partial<DataModelJoin>) {
    mutateSelected((d) => {
      d.dataJoins = d.dataJoins.map((j) => (j.id === joinId ? { ...j, ...patch } : j));
    });
  }

  function removeDataJoin(joinId: string) {
    mutateSelected((d) => {
      d.dataJoins = d.dataJoins.filter((j) => j.id !== joinId);
    });
  }

  function addDataField() {
    if (!selected) return;
    const tableId = selected.dataTables[0]?.id || '';
    mutateSelected((d) => {
      const idx = (d.dataFields?.length || 0) + 1;
      d.dataFields = [
        ...(Array.isArray(d.dataFields) ? d.dataFields : []),
        {
          id: uid(),
          tableId,
          field: '',
          alias: `field_${idx}`
        }
      ];
    });
    if (tableId) ensureDataTableColumnsLoaded(tableId);
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

  function addDataFilter() {
    if (!selected) return;
    const tableId = selected.dataTables[0]?.id || '';
    mutateSelected((d) => {
      d.dataFilters = [
        ...(Array.isArray(d.dataFilters) ? d.dataFilters : []),
        {
          id: uid(),
          tableId,
          field: '',
          operator: 'equals',
          compareValue: ''
        }
      ];
    });
    if (tableId) ensureDataTableColumnsLoaded(tableId);
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
  }

  function tableRefById(draft: ApiDraft | null, tableId: string) {
    if (!draft) return '';
    const t = draft.dataTables.find((x) => x.id === tableId);
    return t ? `${t.schema}.${t.table}` : '';
  }

  function tableLabelById(draft: ApiDraft | null, tableId: string) {
    if (!draft) return '';
    const t = draft.dataTables.find((x) => x.id === tableId);
    if (!t) return '';
    return `${t.alias || t.table} (${t.schema}.${t.table})`;
  }

  function tableColumnsById(draft: ApiDraft | null, tableId: string) {
    if (!draft) return [];
    const t = draft.dataTables.find((x) => x.id === tableId);
    if (!t) return [];
    return columnOptionsFor(t.schema, t.table);
  }

  function aliasSourceLabel(draft: ApiDraft | null, alias: string) {
    if (!draft || !alias) return 'не найден';
    const f = (draft.dataFields || []).find((x) => x.alias === alias);
    if (!f) return 'не найден';
    return `${tableLabelById(draft, f.tableId)}.${f.field}`;
  }

  async function fetchDataModelRows(
    draft: ApiDraft,
    aliases: string[],
    limit: number,
    offset: number
  ): Promise<{ rows: Array<Record<string, any>>; has_more?: boolean }> {
    return apiJson<{ rows: Array<Record<string, any>>; has_more?: boolean }>(`${apiBase}/parameter-join-values`, {
      method: 'POST',
      headers: headers(),
      body: JSON.stringify({
        tables: (draft.dataTables || []).map((t) => ({
          id: t.id,
          schema: t.schema,
          table: t.table,
          alias: t.alias
        })),
        joins: (draft.dataJoins || []).map((j) => ({
          id: j.id,
          left_table_id: j.leftTableId,
          left_field: j.leftField,
          right_table_id: j.rightTableId,
          right_field: j.rightField,
          join_type: j.joinType
        })),
        fields: (draft.dataFields || []).map((f) => ({
          id: f.id,
          table_id: f.tableId,
          field: f.field,
          alias: f.alias
        })),
        filters: (draft.dataFilters || []).map((f) => ({
          id: f.id,
          table_id: f.tableId,
          field: f.field,
          operator: f.operator,
          compare_value: f.compareValue
        })),
        aliases,
        limit,
        offset
      })
    });
  }

  async function loadDataModelRowsForAliases(
    draft: ApiDraft,
    aliases: string[]
  ): Promise<{ rows: Array<Record<string, any>>; issues: Record<string, string> }> {
    const issues: Record<string, string> = {};
    if (!hasDataModelConfigured(draft)) return { rows: [], issues };
    const aliasesRequested = uniqueAliasList(aliases);
    const available = new Set((draft.dataFields || []).map((f) => String(f.alias || '').trim()).filter(Boolean));
    aliasesRequested.forEach((alias) => {
      if (!available.has(alias)) issues[alias] = 'alias не найден в конструкторе данных';
    });
    if (Object.keys(issues).length) return { rows: [], issues };

    const allRows: Array<Record<string, any>> = [];
    const limit = 1000;
    let offset = 0;
    while (offset <= 100000) {
      const resp = await fetchDataModelRows(draft, aliasesRequested, limit, offset);
      const rows = Array.isArray(resp?.rows) ? resp.rows : [];
      if (!rows.length) break;
      allRows.push(...rows);
      offset += rows.length;
      if (!resp?.has_more || rows.length < limit) break;
    }
    return { rows: allRows, issues };
  }

  async function previewDataModelNow() {
    datasetPreviewError = '';
    datasetPreviewRows = [];
    datasetPreviewColumns = [];
    datasetPreviewHasMore = false;
    if (!selected) {
      datasetPreviewError = 'Выбери API';
      return;
    }
    if (!hasDataModelConfigured(selected)) {
      datasetPreviewError = 'Добавь таблицы и поля результата';
      return;
    }
    datasetPreviewLoading = true;
    try {
      const aliases = uniqueAliasList((selected.dataFields || []).map((f) => f.alias));
      datasetPreviewColumns = aliases;
      const resp = await fetchDataModelRows(selected, aliases, 10, 0);
      datasetPreviewRows = Array.isArray(resp?.rows) ? resp.rows : [];
      datasetPreviewHasMore = Boolean(resp?.has_more);
    } catch (e: any) {
      datasetPreviewError = e?.message ?? String(e);
    } finally {
      datasetPreviewLoading = false;
    }
  }

  function addBindingRule() {
    if (!selected) return;
    const firstAlias = bindingAliasOptions(selected)[0] || '';
    mutateSelected((d) => {
      d.bindingRules = [
        ...d.bindingRules,
        {
          id: uid(),
          alias: firstAlias,
          target: 'body_item',
          path: 'value'
        }
      ];
    });
  }

  function updateBindingRule(ruleId: string, patch: Partial<BindingRule>) {
    mutateSelected((d) => {
      d.bindingRules = d.bindingRules.map((rule) => (rule.id === ruleId ? { ...rule, ...patch } : rule));
    });
  }

  function removeBindingRule(ruleId: string) {
    mutateSelected((d) => {
      d.bindingRules = d.bindingRules.filter((rule) => rule.id !== ruleId);
    });
  }

  function handlePaginationStrategyChange(value: string) {
    const normalized = PAGINATION_STRATEGIES.some((s) => s.value === value) ? value : 'none';
    mutateSelected((d) => (d.paginationStrategy = normalized as ApiDraft['paginationStrategy']));
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

    const definitions = Array.isArray(draft.parameterDefinitions) ? draft.parameterDefinitions : [];
    const requestedLower = new Set(aliases.map((a) => a.toLowerCase()));
    const matchedDefinitions = definitions.filter((param) => requestedLower.has(String(param?.alias || '').trim().toLowerCase()));
    const { map: baseMap, issues } = await resolveParameterValues(matchedDefinitions);
    const map: Record<string, any> = {};
    aliases.forEach((alias) => {
      const found = findAliasInMap(baseMap, alias);
      if (found.found && found.value !== undefined && found.value !== null) {
        map[alias] = found.value;
      }
    });

    const missing = aliases.filter((alias) => !findAliasInMap(map, alias).found);
    if (!missing.length || !hasDataModelConfigured(draft)) return { map, issues };

    const dataAliases = uniqueAliasList((draft.dataFields || []).map((f) => String(f?.alias || '').trim()).filter(Boolean));
    const dataAliasByLower = new Map<string, string>();
    dataAliases.forEach((alias) => {
      const key = alias.toLowerCase();
      if (!dataAliasByLower.has(key)) dataAliasByLower.set(key, alias);
    });
    const needFromData = uniqueAliasList(
      missing
        .map((alias) => dataAliasByLower.get(alias.toLowerCase()) || '')
        .filter(Boolean)
    );
    if (!needFromData.length) return { map, issues };

    try {
      const resp = await fetchDataModelRows(draft, needFromData, 1, 0);
      const firstRow = Array.isArray(resp?.rows) && resp.rows.length ? resp.rows[0] : null;
      if (firstRow) {
        needFromData.forEach((alias) => {
          const value = firstRow[alias];
          if (value !== undefined && value !== null) {
            map[alias] = value;
            missing
              .filter((raw) => raw.toLowerCase() === alias.toLowerCase())
              .forEach((raw) => {
                map[raw] = value;
              });
          }
          else if (!issues[alias]) issues[alias] = 'конструктор данных вернул пустое значение';
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

    return { map, issues };
  }

  function toUiErrorMessage(error: any) {
    const msg = error?.message ?? String(error);
    if (/failed to fetch/i.test(String(msg))) {
      return 'Не удалось выполнить запрос из браузера (CORS/сеть). Предпросмотр корректный, но отправка из UI-браузера заблокирована.';
    }
    return msg;
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

  function sameValue(a: any, b: any) {
    if (a === b) return true;
    return JSON.stringify(a) === JSON.stringify(b);
  }

  function uniqueAliasList(values: string[]) {
    return [...new Set(values.map((x) => String(x || '').trim()).filter(Boolean))];
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
    while (offset <= 100000) {
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
    }
    return { rows: allRows, issues };
  }

  async function buildGroupedRequestPlan(draft: ApiDraft) {
    const issues: Record<string, string> = {};
    const sanitizedAliases = sanitizeAliasReferences(draft);
    const groupByAliases = sanitizedAliases.groupByAliases;
    if (!groupByAliases.length) {
      throw new Error('Укажи ключ группировки (например: client_id)');
    }
    const bindingRules = (Array.isArray(sanitizedAliases.bindingRules) ? sanitizedAliases.bindingRules : [])
      .map((rule) => ({
        id: String(rule?.id || uid()),
        alias: String(rule?.alias || '').trim(),
        target: toBindingTarget(String(rule?.target || 'body_item')),
        path: String(rule?.path || '').trim()
      }))
      .filter((rule) => rule.alias && rule.path);
    if (!bindingRules.length) {
      throw new Error('Добавь хотя бы одно правило подстановки');
    }
    if (sanitizedAliases.removedAliases.length) {
      issues.removed_aliases = `удалены неактуальные алиасы: ${sanitizedAliases.removedAliases.join(', ')}`;
    }

    const requiredAliases = uniqueAliasList([
      ...groupByAliases,
      ...bindingRules.map((rule) => rule.alias)
    ]);
    const dataset = hasDataModelConfigured(draft)
      ? await loadDataModelRowsForAliases(draft, requiredAliases)
      : await loadParameterRowsForAliases(draft.parameterDefinitions, requiredAliases);
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
          `Конструктор данных вернул 0 строк. Проверь Таблицы/Связи/Фильтры и "Предпросмотр данных". Нужные поля: ${requiredAliases.join(', ')}`
        );
      }
      throw new Error(
        `Витрина параметров вернула 0 строк. Для группировки параметры должны быть из одной таблицы и с совместимыми условиями. Нужные поля: ${requiredAliases.join(', ')}`
      );
    }

    const grouped = new Map<string, { key: Record<string, any>; rows: Array<Record<string, any>> }>();
    dataset.rows.forEach((row) => {
      const keyObj: Record<string, any> = {};
      groupByAliases.forEach((alias) => {
        keyObj[alias] = row[alias];
      });
      const key = JSON.stringify(keyObj);
      if (!grouped.has(key)) grouped.set(key, { key: keyObj, rows: [] });
      grouped.get(key)?.rows.push(row);
    });

    let oauthAuthValue = '';
    if (draft.authMode === 'oauth2_client_credentials') {
      if (!draft.oauth2TokenUrl || !draft.oauth2ClientId || !draft.oauth2ClientSecret) {
        throw new Error('OAuth2: заполни token URL, client_id и client_secret');
      }
      const tokenData = await getOAuthToken(draft);
      oauthAuthValue = `${tokenData.tokenType} ${tokenData.token}`;
    }

    const plans: Array<any> = [];
    const groupedKnownAliasesLower = new Set(bindingAliasOptions(draft).map((x) => x.toLowerCase()));
    for (const group of grouped.values()) {
      const authHdr = parseJsonObjectField('Авторизация', draft.authJson);
      if (oauthAuthValue) {
        authHdr.Authorization = oauthAuthValue;
      }
      const hdr = parseJsonObjectField('Headers JSON', draft.headersJson);
      const queryObj = parseJsonObjectField('Query JSON', draft.queryJson);
      const bodyBaseRaw = parseJsonAnyField('Body JSON', draft.bodyJson);
      const bodyObj = bodyBaseRaw && typeof bodyBaseRaw === 'object' ? deepClone(bodyBaseRaw) : {};

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

      const groupFirstMap = (group.rows[0] || {}) as Record<string, any>;
      applyParametersToValue(authHdr, groupFirstMap);
      applyParametersToValue(hdr, groupFirstMap);
      applyParametersToValue(queryObj, groupFirstMap);
      applyParametersToValue(bodyObj, groupFirstMap);

      let url = `${draft.baseUrl.replace(/\/$/, '')}${draft.path.startsWith('/') ? draft.path : `/${draft.path}`}`;
      url = replaceParameterTokens(url, groupFirstMap);
      const u = new URL(url);
      Object.entries(queryObj || {}).forEach(([k, v]) => u.searchParams.set(k, String(v)));
      url = u.toString();

      const unresolvedTokens = new Set<string>();
      collectParameterTokens(url, unresolvedTokens);
      collectParameterTokens(authHdr, unresolvedTokens);
      collectParameterTokens(hdr, unresolvedTokens);
      collectParameterTokens(queryObj, unresolvedTokens);
      collectParameterTokens(bodyObj, unresolvedTokens);
      if (unresolvedTokens.size) {
        const detail = Array.from(unresolvedTokens).map((alias) => {
          if (!groupedKnownAliasesLower.has(alias.toLowerCase())) return `${alias} (нет параметра с таким alias)`;
          return `${alias} (значение не вычислено для группы)`;
        });
        throw new Error(`Не удалось подставить параметры в групповой отправке: ${detail.join(', ')}`);
      }

      plans.push({
        group: group.key,
        rows_in_group: group.rows.length,
        method: draft.method,
        url,
        headers: { 'Content-Type': 'application/json', ...authHdr, ...hdr },
        body: draft.method === 'GET' || draft.method === 'DELETE' ? undefined : bodyObj
      });
    }

    return {
      allRequests: plans,
      previewRequests: plans.slice(0, Math.max(1, Number(draft.previewRequestLimit || 5))),
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

      const authHdr = parseJsonObjectField('Авторизация', s.authJson);
      const hdr = parseJsonObjectField('Headers JSON', s.headersJson);
      const queryObj = parseJsonObjectField('Query JSON', s.queryJson);
      const bodyRaw = parseJsonAnyField('Body JSON', s.bodyJson);
      const bodyObj = bodyRaw && typeof bodyRaw === 'object' ? deepClone(bodyRaw) : {};
      const sanitizedAliases = sanitizeAliasReferences(s);
      const initialTokens = new Set<string>();
      const urlTemplate = `${s.baseUrl.replace(/\/$/, '')}${s.path.startsWith('/') ? s.path : `/${s.path}`}`;
      collectParameterTokens(urlTemplate, initialTokens);
      collectParameterTokens(authHdr, initialTokens);
      collectParameterTokens(hdr, initialTokens);
      collectParameterTokens(queryObj, initialTokens);
      collectParameterTokens(bodyObj, initialTokens);
      const requestedAliases = uniqueAliasList([
        ...Array.from(initialTokens),
        ...sanitizedAliases.bindingRules.map((r) => r.alias)
      ]);
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
      if (unresolvedTokens.size) {
        const knownAliasesLower = new Set(
          [
            ...uniqueAliasList((s.parameterDefinitions || []).map((p) => String(p?.alias || '').trim()).filter(Boolean)),
            ...uniqueAliasList((s.dataFields || []).map((f) => String(f?.alias || '').trim()).filter(Boolean))
          ].map((x) => x.toLowerCase())
        );
        const detail = Array.from(unresolvedTokens).map((alias) => {
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

  function handlePaginationTargetChange(value: string) {
    mutateSelected((d) => (d.paginationTarget = String(value) === 'query' ? 'query' : 'body'));
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
      for (const need of API_STORAGE_REQUIRED_COLUMNS) {
        const c = cols.find((x) => String(x.name || '').toLowerCase() === need.name);
        if (!c || !need.types.includes(normalizeTypeName(c.type))) {
          err = `Структура ${schema}.${table} не подходит: колонка ${need.name} отсутствует или имеет неверный тип.`;
          return false;
        }
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

  async function loadAll() {
    loading = true;
    err = '';
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

      const j = await apiJson<{ api_configs: any[] }>(`${apiBase}/api-configs`, { headers: headers() });
      drafts = (Array.isArray(j?.api_configs) ? j.api_configs : []).map((r) => fromRow(r));

      if (!drafts.length) {
        const d = baseDraft();
        drafts = [d];
        selectedRef = refOf(d);
        nameDraft = d.name;
      } else if (!byRef(selectedRef)) {
        selectedRef = refOf(drafts[0]);
      }
    } catch (e: any) {
      err = e?.message ?? String(e);
    } finally {
      loading = false;
    }
  }

  async function saveSelected() {
    err = '';
    ok = '';
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
      const r = await apiJson<{ id?: number }>(`${apiBase}/api-configs/upsert`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify(toPayload(next, parsedFields))
      });
      const id = Number(r?.id || next.storeId || 0);
      await loadAll();
      if (Number.isFinite(id) && id > 0) {
        const m = drafts.find((x) => Number(x.storeId || 0) === id);
        if (m) {
          selectedRef = refOf(m);
          nameDraft = m.name;
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
    const name = String(nameDraft || '').trim();
    if (!name) {
      err = 'Укажи название API';
      return;
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
    if (!d.storeId) {
      drafts = drafts.filter((x) => x.localId !== d.localId);
      if (!byRef(selectedRef) && drafts.length) selectedRef = refOf(drafts[0]);
      return;
    }
    await apiJson(`${apiBase}/api-configs/delete`, {
      method: 'POST',
      headers: headers(),
      body: JSON.stringify({ id: d.storeId })
    });
    await loadAll();
    ok = 'API удален';
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
        myApiPreview = JSON.stringify(
          {
            mode: 'sent_grouped_requests',
            total_requests: groupedPlan.allRequests.length,
            shown_requests: groupedPlan.previewRequests.length,
            requests: groupedPlan.previewRequests,
            issues: Object.keys(groupedPlan.issues).length ? groupedPlan.issues : undefined
          },
          null,
          2
        );
        myPreviewDirty = false;
        myPreviewApplyMessage =
          groupedPlan.allRequests.length > groupedPlan.previewRequests.length
            ? `Перед отправкой показаны первые ${groupedPlan.previewRequests.length} запросов из ${groupedPlan.allRequests.length}`
            : `Перед отправкой показаны ${groupedPlan.previewRequests.length} запросов`;

        if (!groupedPlan.allRequests.length) {
          throw new Error('Не удалось сформировать запросы для отправки');
        }

        const startedAt = Date.now();
        let success = 0;
        let failed = 0;
        let totalItems = 0;
        let totalSize = 0;
        let lastStatus = 0;
        const samples: any[] = [];
        for (const reqPlan of groupedPlan.allRequests) {
          const init: RequestInit = {
            method: reqPlan.method,
            headers: reqPlan.headers
          };
          if (reqPlan.method !== 'GET' && reqPlan.method !== 'DELETE') {
            init.body = JSON.stringify(reqPlan.body || {});
          }
          const res = await fetch(reqPlan.url, init);
          lastStatus = res.status;
          const txt = await res.text();
          totalSize += txt ? txt.length : 0;
          let parsed: any = null;
          try {
            parsed = txt ? JSON.parse(txt) : null;
          } catch {
            parsed = null;
          }
          if (res.ok) success += 1;
          else failed += 1;
          totalItems += countPayloadItems(parsed ?? txt);
          if (samples.length < REQUEST_PREVIEW_MAX) {
            samples.push({
              group: reqPlan.group,
              status: res.status,
              response: parsed ?? txt
            });
          }
        }

        responseStatus = lastStatus;
        responsePagesCount = groupedPlan.allRequests.length;
        responsePayloadCount = totalItems;
        responsePayloadSize = totalSize;
        responseTimeMs = Date.now() - startedAt;
        responseText = JSON.stringify(
          {
            total_requests: groupedPlan.allRequests.length,
            success,
            failed,
            sample_responses: samples
          },
          null,
          2
        );
        ok = failed ? `Проверка завершена: успех ${success}, ошибок ${failed}` : `Проверка выполнена, запросов: ${success}`;
        return;
      }
      const authHdr = parseJsonObjectField('Авторизация', s.authJson);
      const hdr = parseJsonObjectField('Headers JSON', s.headersJson);
      const queryObjBase = parseJsonObjectField('Query JSON', s.queryJson);
      const bodyBaseRaw = parseJsonAnyField('Body JSON', s.bodyJson);
      const bodyObjBase = bodyBaseRaw && typeof bodyBaseRaw === 'object' ? JSON.parse(JSON.stringify(bodyBaseRaw)) : {};

      if (s.authMode === 'oauth2_client_credentials') {
        if (!s.oauth2TokenUrl || !s.oauth2ClientId || !s.oauth2ClientSecret) {
          throw new Error('OAuth2: заполни token URL, client_id и client_secret');
        }
        const t = await getOAuthToken(s);
        authHdr.Authorization = `${t.tokenType} ${t.token}`;
      }

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
      const knownAliasesLower = new Set([...definitionAliases, ...dataModelAliases].map((x) => x.toLowerCase()));

      const requestBase = `${s.baseUrl.replace(/\/$/, '')}${s.path.startsWith('/') ? s.path : `/${s.path}`}`;
      const initialTokens = new Set<string>();
      collectParameterTokens(requestBase, initialTokens);
      collectParameterTokens(authHdr, initialTokens);
      collectParameterTokens(hdr, initialTokens);
      collectParameterTokens(queryObjBase, initialTokens);
      collectParameterTokens(bodyObjBase, initialTokens);
      const requestedAliases = uniqueAliasList([
        ...Array.from(initialTokens),
        ...sanitizedAliases.bindingRules.map((r) => r.alias)
      ]);
      const { map: parameterValues, issues: parameterIssues } = await resolveRuntimeAliasValues(s, requestedAliases);

      const hasParameterValues = Object.keys(parameterValues).length > 0;
      if (hasParameterValues) {
        applyParametersToValue(authHdr, parameterValues);
        applyParametersToValue(hdr, parameterValues);
        applyParametersToValue(queryObjBase, parameterValues);
        applyParametersToValue(bodyObjBase, parameterValues);
      }
      applyBindingRulesToRequest(sanitizedAliases.bindingRules, parameterValues, authHdr, hdr, queryObjBase, bodyObjBase);

      let nextUrlOverride = '';
      const pagesMax = s.paginationEnabled ? Math.max(1, Number(s.paginationMaxPages || 1)) : 1;
      const startTime = Date.now();
      let totalSize = 0;
      const pagePayloads: any[] = [];
      let lastStatus = 0;
      let pageCounter = 0;
      let currentPage = Number(s.paginationStartPage || 1);
      let currentOffset = 0;
      const sentRequests: any[] = [];

      for (let pageIdx = 0; pageIdx < pagesMax; pageIdx++) {
        const queryObj = JSON.parse(JSON.stringify(queryObjBase || {}));
        const bodyObj = JSON.parse(JSON.stringify(bodyObjBase || {}));

        if (s.paginationEnabled) {
          if (s.paginationStrategy === 'page_number') {
            const p = s.paginationPageParam || 'page';
            if (s.paginationTarget === 'query') queryObj[p] = currentPage;
            else setByPath(bodyObj, p, currentPage);
          } else if (s.paginationStrategy === 'offset_limit') {
            const off = s.paginationPageParam || 'offset';
            const lim = s.paginationLimitParam || 'limit';
            const limVal = Number(s.paginationLimitValue || 100);
            if (s.paginationTarget === 'query') {
              queryObj[off] = currentOffset;
              queryObj[lim] = limVal;
            } else {
              setByPath(bodyObj, off, currentOffset);
              setByPath(bodyObj, lim, limVal);
            }
          }
        }

        let url = nextUrlOverride || `${s.baseUrl.replace(/\/$/, '')}${s.path.startsWith('/') ? s.path : `/${s.path}`}`;
        if (hasParameterValues) {
          url = replaceParameterTokens(url, parameterValues);
        }
        const u = new URL(url);
        for (const [k, v] of Object.entries(queryObj || {})) u.searchParams.set(k, String(v));
        url = u.toString();

        const unresolvedTokens = new Set<string>();
        collectParameterTokens(url, unresolvedTokens);
        collectParameterTokens(authHdr, unresolvedTokens);
        collectParameterTokens(hdr, unresolvedTokens);
        collectParameterTokens(queryObj, unresolvedTokens);
        collectParameterTokens(bodyObj, unresolvedTokens);
        if (unresolvedTokens.size) {
          const detail = Array.from(unresolvedTokens).map((alias) => {
            const issueByAlias = findAliasInMap(parameterIssues, alias);
            if (issueByAlias.found) return `${alias} (${issueByAlias.value})`;
            if (!knownAliasesLower.has(alias.toLowerCase())) return `${alias} (нет параметра с таким alias)`;
            return `${alias} (значение не вычислено)`;
          });
          throw new Error(`Не удалось подставить параметры: ${detail.join(', ')}`);
        }

        const init: RequestInit = {
          method: s.method,
          headers: { 'Content-Type': 'application/json', ...authHdr, ...hdr }
        };
        if (s.method !== 'GET' && s.method !== 'DELETE') {
          init.body = JSON.stringify(bodyObj || {});
        }
        if (sentRequests.length < REQUEST_PREVIEW_MAX) {
          sentRequests.push({
            page: pageIdx + 1,
            method: s.method,
            url,
            headers: { 'Content-Type': 'application/json', ...authHdr, ...hdr },
            query: JSON.parse(JSON.stringify(queryObj || {})),
            body: s.method === 'GET' || s.method === 'DELETE' ? undefined : JSON.parse(JSON.stringify(bodyObj || {}))
          });
        }

        const res = await fetch(url, init);
        lastStatus = res.status;
        const txt = await res.text();
        let parsed: any = null;
        try {
          parsed = txt ? JSON.parse(txt) : null;
        } catch {
          parsed = null;
        }
        totalSize += txt ? txt.length : 0;
        pagePayloads.push(parsed ?? txt);
        pageCounter += 1;
        responseStatus = lastStatus;
        responseText = txt;

        if (!s.paginationEnabled) break;

        const dataPath = String(s.paginationDataPath || '').trim();
        if (dataPath && parsed) {
          const items = getByPath(parsed, dataPath);
          if (Array.isArray(items) && items.length === 0) break;
        }

        if (s.paginationStrategy === 'page_number') {
          currentPage += 1;
        } else if (s.paginationStrategy === 'offset_limit') {
          currentOffset += Number(s.paginationLimitValue || 100);
        } else if (s.paginationStrategy === 'cursor_fields') {
          const v1 = s.paginationCursorResPath1 ? getByPath(parsed, s.paginationCursorResPath1) : undefined;
          const v2 = s.paginationCursorResPath2 ? getByPath(parsed, s.paginationCursorResPath2) : undefined;
          if (v1 == null && v2 == null) break;
          if (s.paginationCursorReqPath1) {
            if (s.paginationTarget === 'query') queryObjBase[s.paginationCursorReqPath1] = v1;
            else setByPath(bodyObjBase, s.paginationCursorReqPath1, v1);
          }
          if (s.paginationCursorReqPath2) {
            if (s.paginationTarget === 'query') queryObjBase[s.paginationCursorReqPath2] = v2;
            else setByPath(bodyObjBase, s.paginationCursorReqPath2, v2);
          }
        } else if (s.paginationStrategy === 'next_url') {
          const n = s.paginationNextUrlPath ? getByPath(parsed, s.paginationNextUrlPath) : undefined;
          if (!n || typeof n !== 'string') break;
          nextUrlOverride = n;
        } else if (s.paginationStrategy === 'custom') {
          break;
        } else {
          break;
        }

        if (Number(s.paginationDelayMs || 0) > 0) {
          await new Promise((resolve) => setTimeout(resolve, Number(s.paginationDelayMs || 0)));
        }
      }

      const sentPreviewPayload =
        pageCounter <= 1
          ? {
              request: sentRequests[0] || null,
              resolved_parameters: parameterValues,
              parameter_issues: Object.keys(parameterIssues).length ? parameterIssues : undefined
            }
          : {
              requests: sentRequests,
              request_count: pageCounter,
              shown_requests: sentRequests.length,
              truncated: pageCounter > sentRequests.length,
              resolved_parameters: parameterValues,
              parameter_issues: Object.keys(parameterIssues).length ? parameterIssues : undefined
            };
      myApiPreview = JSON.stringify(sentPreviewPayload, null, 2);
      myPreviewDirty = false;
      myPreviewApplyMessage =
        pageCounter > REQUEST_PREVIEW_MAX
          ? `Показаны первые ${REQUEST_PREVIEW_MAX} отправленных запросов из ${pageCounter}`
          : pageCounter > 1
          ? `Показаны отправленные запросы по страницам: ${pageCounter}`
          : 'Показан последний отправленный запрос';

      if (s.paginationEnabled && pagePayloads.length > 1) {
        responseText = JSON.stringify({ pages: pageCounter, last_status: lastStatus, samples: pagePayloads }, null, 2);
      }
      responsePagesCount = pagePayloads.length;
      responsePayloadCount = pagePayloads.reduce((sum, item) => sum + countPayloadItems(item), 0);
      responsePayloadSize = totalSize;
      responseTimeMs = Date.now() - startTime;
      ok = s.paginationEnabled ? `Проверка выполнена, страниц: ${pageCounter}` : 'Проверка выполнена';
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
  $: if (selectedRef !== lastSelectedRef) {
    lastSelectedRef = selectedRef;
    if (selected) {
      nameDraft = selected.name;
      requestInput = `${selected.baseUrl.replace(/\/$/, '')}${selected.path.startsWith('/') ? selected.path : `/${selected.path}`}`;
      myPreviewDirty = false;
      myPreviewApplyMessage = '';
      myApiPreview = '';
      myApiPreviewDraft = '';
      activeResponseFieldRef = '';
      datasetPreviewRows = [];
      datasetPreviewColumns = [];
      datasetPreviewHasMore = false;
      datasetPreviewError = '';
    }
  }
  $: if (!myPreviewDirty) {
    myApiPreviewDraft = myApiPreview;
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
  $: selected?.headersJson, tick().then(syncMainTextareasHeight);
  $: selected?.queryJson, tick().then(syncMainTextareasHeight);
  $: selected?.bodyJson, tick().then(syncMainTextareasHeight);
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
  $: authViewMode, tick().then(syncMainTextareasHeight);
  $: headersViewMode, tick().then(syncMainTextareasHeight);
  $: queryViewMode, tick().then(syncMainTextareasHeight);
  $: bodyViewMode, tick().then(syncMainTextareasHeight);

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
    autosize(headersEl);
    autosize(queryEl);
    autosize(bodyEl);
  }

  void loadAll();
</script>

<section class="panel">
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
        <div class="statusline">status: {responseStatus || '-'}</div>
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
          <span>Что ушло на сервер</span>
          <span class="inline-actions">
            <button type="button" class="view-toggle" on:click={previewRequestsNow} disabled={checking}>Предпросмотр 5</button>
            <button type="button" class="view-toggle" on:click={checkApiNow} disabled={checking}>
              {checking ? 'Проверка...' : 'Обновить'}
            </button>
            {#if myPreviewIsJson}
              <button type="button" class="view-toggle" on:click={() => (myPreviewViewMode = myPreviewViewMode === 'tree' ? 'raw' : 'tree')}>
                {myPreviewViewMode === 'tree' ? 'RAW' : 'Дерево'}
              </button>
            {/if}
          </span>
        </div>
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
          <p class="hint small-hint">Нажми «Предпросмотр 5», чтобы увидеть первые реальные запросы до отправки, или «Проверить/Обновить» для запуска.</p>
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
            <button type="button" class="view-toggle" on:click={previewRequestsNow} disabled={checking}>Предпросмотр 5</button>
          </div>
        </div>

        <textarea
          bind:this={descriptionEl}
          class="desc"
          placeholder="Описание API"
          value={selected?.description || ''}
          on:input={(e) => mutateSelected((d) => (d.description = e.currentTarget.value))}
        ></textarea>

        <label>
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
                  class:active-chip={hasBindingRule(alias, 'auth')}
                  title="Перетащи в JSON или нажми для включения/выключения в Авторизации"
                  draggable="true"
                  on:dragstart={(e) => aliasChipDragStart(alias, e)}
                  on:click={() => toggleBindingUsage(alias, 'auth')}
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
              bind:this={authEl}
              value={selected?.authJson || ''}
              on:input={(e) => mutateSelected((d) => (d.authJson = e.currentTarget.value))}
              on:dragover|preventDefault
              on:drop={(e) => dropAliasTokenToField(e, 'authJson', authEl)}
            ></textarea>
          {/if}
        </label>
        <div class="auth-mode-buttons">
          <button
            type="button"
            class="view-toggle auth-mode-btn"
            class:active={selected?.authMode === AUTH_MODE_OAUTH2}
            on:click={() =>
              mutateSelected((d) =>
                (d.authMode = d.authMode === AUTH_MODE_OAUTH2 ? 'manual' : AUTH_MODE_OAUTH2)
              )
            }
          >
            OAuth2 (client_credentials)
          </button>
        </div>
        {#if selected?.authMode === 'oauth2_client_credentials'}
          <div class="oauth-grid">
            <input
              placeholder="Token URL"
              value={selected?.oauth2TokenUrl || ''}
              on:input={(e) => mutateSelected((d) => (d.oauth2TokenUrl = e.currentTarget.value))}
            />
            <input
              placeholder="Client ID"
              value={selected?.oauth2ClientId || ''}
              on:input={(e) => mutateSelected((d) => (d.oauth2ClientId = e.currentTarget.value))}
            />
            <input
              placeholder="Client Secret"
              value={selected?.oauth2ClientSecret || ''}
              on:input={(e) => mutateSelected((d) => (d.oauth2ClientSecret = e.currentTarget.value))}
            />
            <input
              placeholder="Grant type"
              value={selected?.oauth2GrantType || ''}
              on:input={(e) => mutateSelected((d) => (d.oauth2GrantType = e.currentTarget.value))}
            />
            <input
              placeholder="Поле токена"
              value={selected?.oauth2TokenField || ''}
              on:input={(e) => mutateSelected((d) => (d.oauth2TokenField = e.currentTarget.value))}
            />
            <input
              placeholder="Поле expires_in"
              value={selected?.oauth2ExpiresField || ''}
              on:input={(e) => mutateSelected((d) => (d.oauth2ExpiresField = e.currentTarget.value))}
            />
            <input
              placeholder="Поле token_type"
              value={selected?.oauth2TokenTypeField || ''}
              on:input={(e) => mutateSelected((d) => (d.oauth2TokenTypeField = e.currentTarget.value))}
            />
          </div>
          <p class="hint">OAuth2: конструктор автоматически получает токен и подставляет заголовок Authorization.</p>
        {/if}

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
                    class:active-chip={hasBindingRule(alias, 'header')}
                    title="Перетащи в JSON или нажми для включения/выключения в Headers"
                    draggable="true"
                    on:dragstart={(e) => aliasChipDragStart(alias, e)}
                    on:click={() => toggleBindingUsage(alias, 'header')}
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
                    class:active-chip={hasBindingRule(alias, 'query')}
                    title="Перетащи в JSON или нажми для включения/выключения в Query"
                    draggable="true"
                    on:dragstart={(e) => aliasChipDragStart(alias, e)}
                    on:click={() => toggleBindingUsage(alias, 'query')}
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
                  class:active-chip={hasBindingRule(alias, 'body')}
                  title="Перетащи в JSON или нажми для включения/выключения в Body"
                  draggable="true"
                  on:dragstart={(e) => aliasChipDragStart(alias, e)}
                  on:click={() => toggleBindingUsage(alias, 'body')}
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
              <span>Конструктор данных (таблицы, связи, поля)</span>
              <span class="inline-actions">
                <button type="button" class="view-toggle" on:click={previewDataModelNow} disabled={datasetPreviewLoading}>
                  {datasetPreviewLoading ? 'Загрузка...' : 'Предпросмотр данных'}
                </button>
              </span>
            </div>

            <div class="data-section">
              <div class="response-head field-head parameter-subhead">
                <small>Таблицы</small>
                <button type="button" class="view-toggle" on:click={addDataTable}>Таблица +</button>
              </div>
              {#if selected?.dataTables?.length}
                <div class="data-list">
                  {#each selected.dataTables as tbl (tbl.id)}
                    <div class="data-row">
                      <select value={`${tbl.schema}.${tbl.table}`} on:change={(e) => updateDataTableRef(tbl.id, e.currentTarget.value)}>
                        <option value="">Таблица</option>
                        {#each existingTables as et}
                          <option value={`${et.schema_name}.${et.table_name}`}>{et.schema_name}.{et.table_name}</option>
                        {/each}
                      </select>
                      <input value={tbl.alias} placeholder="alias таблицы" on:input={(e) => updateDataTableAlias(tbl.id, e.currentTarget.value)} />
                      <button type="button" class="chip-remove" on:click={() => removeDataTable(tbl.id)}>x</button>
                    </div>
                  {/each}
                </div>
              {:else}
                <p class="hint">Добавь минимум одну таблицу.</p>
              {/if}
            </div>

            <div class="data-section">
              <div class="response-head field-head parameter-subhead">
                <small>Связи таблиц</small>
                <button type="button" class="view-toggle" on:click={addDataJoin}>Связь +</button>
              </div>
              {#if selected?.dataJoins?.length}
                <div class="data-list">
                  {#each selected.dataJoins as j (j.id)}
                    <div class="data-row join-row">
                      <select
                        value={j.leftTableId}
                        on:change={(e) => {
                          updateDataJoin(j.id, { leftTableId: e.currentTarget.value, leftField: '' });
                          ensureDataTableColumnsLoaded(e.currentTarget.value);
                        }}
                      >
                        <option value="">Основная таблица</option>
                        {#each selected.dataTables as t}
                          <option value={t.id}>{tableLabelById(selected, t.id)}</option>
                        {/each}
                      </select>
                      <select value={j.leftField} on:change={(e) => updateDataJoin(j.id, { leftField: e.currentTarget.value })}>
                        <option value="">Ключ из основной таблицы</option>
                        {#each tableColumnsById(selected, j.leftTableId) as col}
                          <option value={col}>{col}</option>
                        {/each}
                      </select>
                      <select value={j.joinType} on:change={(e) => updateDataJoin(j.id, { joinType: e.currentTarget.value === 'left' ? 'left' : 'inner' })}>
                        {#each DATA_JOIN_TYPES as jt}
                          <option value={jt.value}>{jt.label}</option>
                        {/each}
                      </select>
                      <select
                        value={j.rightTableId}
                        on:change={(e) => {
                          updateDataJoin(j.id, { rightTableId: e.currentTarget.value, rightField: '' });
                          ensureDataTableColumnsLoaded(e.currentTarget.value);
                        }}
                      >
                        <option value="">Подключаемая таблица</option>
                        {#each selected.dataTables as t}
                          <option value={t.id}>{tableLabelById(selected, t.id)}</option>
                        {/each}
                      </select>
                      <select value={j.rightField} on:change={(e) => updateDataJoin(j.id, { rightField: e.currentTarget.value })}>
                        <option value="">Ключ из подключаемой таблицы</option>
                        {#each tableColumnsById(selected, j.rightTableId) as col}
                          <option value={col}>{col}</option>
                        {/each}
                      </select>
                      <button type="button" class="chip-remove" on:click={() => removeDataJoin(j.id)}>x</button>
                    </div>
                  {/each}
                </div>
              {:else}
                <p class="hint">Для нескольких таблиц добавь связи по ключам (например store_id, campaign_id).</p>
              {/if}
            </div>

            <div class="data-section">
              <div class="response-head field-head parameter-subhead">
                <small>Поля результата</small>
                <button type="button" class="view-toggle" on:click={addDataField}>Поле +</button>
              </div>
              {#if selected?.dataFields?.length}
                <div class="data-list">
                  {#each selected.dataFields as f (f.id)}
                    <div class="data-row">
                      <select
                        value={f.tableId}
                        on:change={(e) => {
                          updateDataField(f.id, { tableId: e.currentTarget.value, field: '' });
                          ensureDataTableColumnsLoaded(e.currentTarget.value);
                        }}
                      >
                        <option value="">Таблица</option>
                        {#each selected.dataTables as t}
                          <option value={t.id}>{tableLabelById(selected, t.id)}</option>
                        {/each}
                      </select>
                      <select value={f.field} on:change={(e) => updateDataField(f.id, { field: e.currentTarget.value })}>
                        <option value="">Колонка</option>
                        {#each tableColumnsById(selected, f.tableId) as col}
                          <option value={col}>{col}</option>
                        {/each}
                      </select>
                      <input value={f.alias} placeholder="alias поля (например token, campaign_id, sku)" on:input={(e) => updateDataField(f.id, { alias: e.currentTarget.value })} />
                      <button type="button" class="chip-remove" on:click={() => removeDataField(f.id)}>x</button>
                    </div>
                  {/each}
                </div>
              {:else}
                <p class="hint">Выбери поля, которые пойдут в запрос (token, campaign_id, sku и т.д.).</p>
              {/if}
            </div>

            <div class="data-section">
              <div class="response-head field-head parameter-subhead">
                <small>Фильтры</small>
                <button type="button" class="view-toggle" on:click={addDataFilter}>Фильтр +</button>
              </div>
              {#if selected?.dataFilters?.length}
                <div class="data-list">
                  {#each selected.dataFilters as f (f.id)}
                    <div class="data-row">
                      <select
                        value={f.tableId}
                        on:change={(e) => {
                          updateDataFilter(f.id, { tableId: e.currentTarget.value, field: '' });
                          ensureDataTableColumnsLoaded(e.currentTarget.value);
                        }}
                      >
                        <option value="">Таблица</option>
                        {#each selected.dataTables as t}
                          <option value={t.id}>{tableLabelById(selected, t.id)}</option>
                        {/each}
                      </select>
                      <select value={f.field} on:change={(e) => updateDataFilter(f.id, { field: e.currentTarget.value })}>
                        <option value="">Колонка</option>
                        {#each tableColumnsById(selected, f.tableId) as col}
                          <option value={col}>{col}</option>
                        {/each}
                      </select>
                      <select value={f.operator} on:change={(e) => updateDataFilter(f.id, { operator: e.currentTarget.value })}>
                        {#each DATA_FILTER_OPERATORS as op}
                          <option value={op.value}>{op.label}</option>
                        {/each}
                      </select>
                      <input value={f.compareValue} placeholder="значение" on:input={(e) => updateDataFilter(f.id, { compareValue: e.currentTarget.value })} />
                      <button type="button" class="chip-remove" on:click={() => removeDataFilter(f.id)}>x</button>
                    </div>
                  {/each}
                </div>
              {/if}
            </div>

            {#if datasetPreviewError}
              <p class="definition-error">{datasetPreviewError}</p>
            {/if}
            {#if datasetPreviewColumns.length}
              <div class="dataset-preview-table-wrap">
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
              </div>
              <p class="hint small-hint">
                Показано {datasetPreviewRows.length} строк (макс. 10){datasetPreviewHasMore ? ', есть ещё данные.' : '.'}
              </p>
            {/if}
          </div>

          <div class="response-head field-head parameter-subhead">
            <span>Витрина API</span>
          </div>

          <div class="api-showcase-grid">
            <div class="api-showcase-col api-showcase-left">
              <div class="response-head field-head">
                <span>Параметры (поля результата)</span>
              </div>
              {#if groupByAliasCandidates.length}
                <div class="api-crumbs">
                  {#each groupByAliasCandidates as alias}
                    <button
                      type="button"
                      class="api-crumb"
                      class:active-crumb={selectedApiAlias === alias}
                      draggable="true"
                      on:dragstart={(e) => aliasChipDragStart(alias, e)}
                      on:click={() => (selectedApiAlias = alias)}
                    >
                      {alias}
                    </button>
                  {/each}
                </div>
                {#if selectedApiAlias}
                  <p class="hint small-hint">
                    Alias выбранного параметра: <strong>{selectedApiAlias}</strong>
                  </p>
                  <p class="hint small-hint">
                    Источник: {aliasSourceLabel(selected, selectedApiAlias)}
                  </p>
                {/if}
                <p class="hint small-hint">Перетащи крошку в нужный JSON-блок или выбери справа места использования.</p>
              {:else}
                <p class="hint">Сначала заполни “Поля результата” в конструкторе данных.</p>
              {/if}
            </div>

            <div class="api-showcase-col api-showcase-right">
              <div class="response-head field-head">
                <span>Настройка отправки</span>
              </div>
              <p class="hint small-hint">Режим отправки определяется автоматически: если у параметра включена галочка “Группировать”, отправка будет групповой.</p>

              <div class="response-head field-head parameter-subhead">
                <span>Витрина настроек параметра</span>
              </div>
              {#if selectedApiAlias}
                <div class="usage-toggle-list">
                  <label class="usage-toggle-item">
                    <input type="checkbox" checked={isAliasGrouped(selectedApiAlias)} on:change={() => toggleGroupByAlias(selectedApiAlias)} />
                    <span>Группировать</span>
                  </label>
                  <label class="usage-toggle-item">
                    <input type="checkbox" checked={hasBindingRule(selectedApiAlias, 'auth')} on:change={() => toggleBindingUsage(selectedApiAlias, 'auth')} />
                    <span>Авторизация</span>
                  </label>
                  <label class="usage-toggle-item">
                    <input type="checkbox" checked={hasBindingRule(selectedApiAlias, 'header')} on:change={() => toggleBindingUsage(selectedApiAlias, 'header')} />
                    <span>Headers</span>
                  </label>
                  <label class="usage-toggle-item">
                    <input type="checkbox" checked={hasBindingRule(selectedApiAlias, 'query')} on:change={() => toggleBindingUsage(selectedApiAlias, 'query')} />
                    <span>Query</span>
                  </label>
                  <label class="usage-toggle-item">
                    <input type="checkbox" checked={hasBindingRule(selectedApiAlias, 'body')} on:change={() => toggleBindingUsage(selectedApiAlias, 'body')} />
                    <span>Body</span>
                  </label>
                </div>
                <p class="hint small-hint">Выбран параметр: <strong>{selectedApiAlias}</strong></p>
              {:else}
                <p class="hint">Выбери крошку параметра слева.</p>
              {/if}
              <div class="pagination-field preview-limit-field">
                <small>Лимит предпросмотра</small>
                <input
                  type="number"
                  min="1"
                  max="50"
                  value={selected?.previewRequestLimit || 5}
                  on:input={(e) => mutateSelected((d) => (d.previewRequestLimit = Math.max(1, Math.min(50, Number(e.currentTarget.value) || 5))))}
                />
              </div>
            </div>
          </div>
        </div>

        <div class="pagination-box">
          <div class="response-head field-head">
            <span>Пагинация</span>
            <label class="pagination-toggle">
              <input
                type="checkbox"
                checked={selected?.paginationEnabled}
                on:change={(e) => mutateSelected((d) => (d.paginationEnabled = e.currentTarget.checked))}
              />
              <span>Включить</span>
            </label>
          </div>
          {#if selected?.paginationEnabled}
            <div class="pagination-grid">
              <div class="pagination-field">
                <small>Стратегия</small>
                <select
                  value={selected?.paginationStrategy || 'none'}
                  on:change={(e) => handlePaginationStrategyChange(e.currentTarget.value)}
                >
                  {#each PAGINATION_STRATEGIES as strat}
                    <option value={strat.value}>{strat.label}</option>
                  {/each}
                </select>
              </div>
              <div class="pagination-field">
                <small>Куда писать</small>
                <select
                  value={selected?.paginationTarget || 'body'}
                  on:change={(e) => handlePaginationTargetChange(e.currentTarget.value)}
                >
                  {#each PAGINATION_TARGETS as target}
                    <option value={target.value}>{target.label}</option>
                  {/each}
                </select>
              </div>
            </div>
            {#if selected?.paginationStrategy === 'custom'}
              <div class="pagination-grid">
                <div class="pagination-field">
                  <small>Своя инструкция</small>
                  <input
                    placeholder="Например: cursor_name + limit"
                    value={selected?.paginationCustomStrategy || ''}
                    on:input={(e) => mutateSelected((d) => (d.paginationCustomStrategy = e.currentTarget.value))}
                  />
                </div>
              </div>
            {/if}

            <div class="pagination-grid">
              <div class="pagination-field">
                <small>Путь к данным (массив)</small>
                <input
                  placeholder="Например: settings.cursor.items"
                  value={selected?.paginationDataPath || ''}
                  on:input={(e) => mutateSelected((d) => (d.paginationDataPath = e.currentTarget.value))}
                />
              </div>
              <div class="pagination-field">
                <small>Макс. страниц</small>
                <input
                  type="number"
                  min="1"
                  value={selected?.paginationMaxPages || 1}
                  on:input={(e) => mutateSelected((d) => (d.paginationMaxPages = Number(e.currentTarget.value) || 1))}
                />
              </div>
            </div>

            <div class="pagination-grid">
              <div class="pagination-field">
                <small>Параметр страницы</small>
                <input
                  placeholder="page"
                  value={selected?.paginationPageParam || ''}
                  on:input={(e) => mutateSelected((d) => (d.paginationPageParam = e.currentTarget.value))}
                />
              </div>
              <div class="pagination-field">
                <small>Стартовая страница / смещение</small>
                <input
                  type="number"
                  value={selected?.paginationStartPage || 1}
                  on:input={(e) => mutateSelected((d) => (d.paginationStartPage = Number(e.currentTarget.value) || 1))}
                />
              </div>
            </div>

            <div class="pagination-grid">
              <div class="pagination-field">
                <small>Limit параметр</small>
                <input
                  placeholder="limit"
                  value={selected?.paginationLimitParam || ''}
                  on:input={(e) => mutateSelected((d) => (d.paginationLimitParam = e.currentTarget.value))}
                />
              </div>
              <div class="pagination-field">
                <small>Limit значение</small>
                <input
                  type="number"
                  min="1"
                  value={selected?.paginationLimitValue || 1}
                  on:input={(e) => mutateSelected((d) => (d.paginationLimitValue = Number(e.currentTarget.value) || 1))}
                />
              </div>
            </div>

            <div class="pagination-grid">
              <div class="pagination-field">
                <small>Cursor request path 1</small>
                <input
                  placeholder="cursor.after"
                  value={selected?.paginationCursorReqPath1 || ''}
                  on:input={(e) => mutateSelected((d) => (d.paginationCursorReqPath1 = e.currentTarget.value))}
                />
              </div>
              <div class="pagination-field">
                <small>Cursor request path 2</small>
                <input
                  placeholder="cursor.before"
                  value={selected?.paginationCursorReqPath2 || ''}
                  on:input={(e) => mutateSelected((d) => (d.paginationCursorReqPath2 = e.currentTarget.value))}
                />
              </div>
            </div>

            <div class="pagination-grid">
              <div class="pagination-field">
                <small>Cursor response path 1</small>
                <input
                  value={selected?.paginationCursorResPath1 || ''}
                  on:input={(e) => mutateSelected((d) => (d.paginationCursorResPath1 = e.currentTarget.value))}
                />
              </div>
              <div class="pagination-field">
                <small>Cursor response path 2</small>
                <input
                  value={selected?.paginationCursorResPath2 || ''}
                  on:input={(e) => mutateSelected((d) => (d.paginationCursorResPath2 = e.currentTarget.value))}
                />
              </div>
            </div>

            <div class="pagination-grid">
              <div class="pagination-field">
                <small>Next URL path</small>
                <input
                  placeholder="links.next"
                  value={selected?.paginationNextUrlPath || ''}
                  on:input={(e) => mutateSelected((d) => (d.paginationNextUrlPath = e.currentTarget.value))}
                />
              </div>
              <div class="pagination-field">
                <small>Delay между запросами (мс)</small>
                <input
                  type="number"
                  min="0"
                  value={selected?.paginationDelayMs || 0}
                  on:input={(e) => mutateSelected((d) => (d.paginationDelayMs = Number(e.currentTarget.value) || 0))}
                />
              </div>
            </div>
            <p class="hint">Запросы автоматически повторяются по стратегии в пределах {selected?.paginationMaxPages || 1} страниц.</p>
          {/if}
        </div>

        <div class="targets-wrap">
          <div class="targets-head">
            <div class="targets-title">Куда записывать ответ</div>
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

      <div class="subsec">
        <div class="subttl template-head">
          <span>Шаблон API</span>
          <span class="inline-actions">
            <button type="button" class="view-toggle" on:click={onTemplateParseClick}>Сохранить</button>
            <button type="button" class="view-toggle" on:click={onTemplateClearClick}>Очистить</button>
            {#if exampleIsJson}
              <button type="button" class="view-toggle" on:click={() => (exampleViewMode = exampleViewMode === 'tree' ? 'raw' : 'tree')}>
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

      <div class="template-controls">
        <input
          class="template-name"
          value={nameDraft}
          on:input={(e) => {
            nameDraft = e.currentTarget.value;
            if (selected) mutateSelected((d) => (d.name = e.currentTarget.value));
          }}
          placeholder="Название API"
        />
        <div class="saved-inline-actions">
          <button on:click={addApi}>Добавить</button>
          <button on:click={saveSelected} disabled={saving || !selected}>{saving ? 'Сохранение...' : 'Сохранить'}</button>
        </div>
      </div>

      <div class="list api-list">
        {#each drafts as d (d.localId)}
          <div class="row-item" class:activeitem={refOf(d) === selectedRef}>
            <button class="item-button" on:click={() => (selectedRef = refOf(d))}>
              <div class="row-name">{d.storeId || 'new'} • {d.name}</div>
              <div class="row-meta">{d.method} {d.baseUrl}{d.path}</div>
            </button>
            <div class="row-actions">
              <button class="danger icon-btn" on:click|stopPropagation={() => deleteApi(d)} title="Удалить API">x</button>
            </div>
          </div>
        {/each}
      </div>
    </aside>
  </div>
</section>

<style>
  .panel { background:#fff; border:1px solid #e6eaf2; border-radius:18px; padding:14px; box-shadow:0 6px 20px rgba(15,23,42,.05); margin-top:12px; }
  .panel-head { display:flex; align-items:center; justify-content:space-between; gap:12px; }
  .panel-head h2 { margin:0; font-size:18px; }

  .layout { display:grid; grid-template-columns: 320px 1fr 340px; gap:12px; margin-top:12px; align-items:start; }
  @media (max-width: 1300px) { .layout { grid-template-columns: 1fr; } }

  .aside { border:1px solid #e6eaf2; border-radius:16px; padding:12px; background:#f8fafc; }
  .aside-title { font-weight:700; font-size:14px; line-height:1.3; margin-bottom:8px; }

  .subsec { margin-top:10px; }
  .subttl { font-size:12px; color:#475569; margin-bottom:6px; }
  .response-head { display:flex; align-items:center; justify-content:space-between; gap:8px; flex-wrap:nowrap; }
  .response-head > span:first-child { white-space:nowrap; }
  .field-head { justify-content:flex-start; }
  .inline-actions {
    display:inline-flex;
    align-items:center;
    gap:6px;
    flex-wrap:nowrap;
    margin-left:auto;
  }
  .view-toggle { border-radius:10px; border:1px solid #e2e8f0; background:#fff; color:#0f172a; padding:4px 8px; font-size:11px; line-height:1.2; }
  .response-tree-wrap { border:1px solid #e6eaf2; border-radius:12px; background:#fff; padding:8px; min-height:78px; overflow:visible; }
  .template-head { display:flex; align-items:center; justify-content:space-between; gap:8px; }
  .statusline { font-size:12px; color:#64748b; margin-bottom:6px; }
  .metrics-row { display:flex; gap:10px; font-size:12px; color:#475569; margin-bottom:8px; flex-wrap:wrap; }
  .metrics-row span { font-weight:500; }
  .template-parse-actions { margin-top:8px; display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
  .template-parse-note { font-size:12px; color:#64748b; }

  .main { min-width:0; }
  .card { border:1px solid #e6eaf2; border-radius:16px; padding:12px; background:#fff; }

  .connect-row { margin-top:10px; display:grid; grid-template-columns: 180px 1fr auto; gap:8px; align-items:center; }
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
  .raw-grid { display:grid; grid-template-columns: 1fr 1fr; gap:10px; margin-top:8px; }
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
  .param-token-chip.active-chip { border-color:#0f172a; background:#0f172a; color:#fff; }

  .storage-meta { margin:0 0 8px; display:flex; align-items:center; gap:6px; font-size:12px; color:#64748b; }
  .link-btn { border:0; background:transparent; color:#0f172a; padding:0; text-decoration:underline; font-size:12px; font-weight:500; }
  .storage-picker { display:flex; gap:8px; align-items:center; margin-bottom:8px; }
  .storage-picker select { flex:1; min-width:0; }
  .template-controls { margin-bottom:8px; display:grid; gap:8px; }
  .template-name { width:100%; box-sizing:border-box; }
  .saved-inline-actions { display:grid; grid-template-columns:1fr 1fr; gap:8px; }

  .list { display:flex; flex-direction:column; gap:8px; }
  .row-item { display:grid; grid-template-columns: 1fr auto; gap:8px; align-items:center; border:1px solid #e6eaf2; border-radius:14px; background:#0f172a; padding:8px 10px; }
  .row-actions { display:flex; align-items:center; justify-content:flex-end; min-width:54px; }
  .item-button { text-align:left; border:0; background:transparent; padding:0; color:inherit; width:100%; }
  .row-name { font-weight:400; font-size:13px; line-height:1.25; word-break:break-word; color:#fff; }
  .row-meta { font-size:12px; color:#cbd5e1; margin-top:4px; word-break: break-word; }
  .api-list .activeitem { background:#fff; border-color:#e6eaf2; color:#0f172a; }
  .api-list .activeitem .row-name { font-size:15px; font-weight:600; letter-spacing:.01em; color:#0f172a; }
  .api-list .activeitem .row-name::before { content:'●'; margin-right:8px; font-size:11px; color:#0f172a; vertical-align:middle; }
  .api-list .activeitem .row-meta { color:#64748b; }

  .icon-btn { width:34px; min-width:34px; padding:6px 0; font-size:14px; text-transform:uppercase; border-color:transparent; background:transparent; color:#fff; }
  .danger.icon-btn { color:#b91c1c; }
  .plus-dark.icon-btn { color:#0f172a; font-weight:700; }
  .plus-green.icon-btn { color:#16a34a; font-weight:700; }
  .map-row .icon-btn { color:#b91c1c; }
  .activeitem .icon-btn { color:#b91c1c; }

  input, select, textarea { border-radius:14px; border:1px solid #e6eaf2; padding:10px 12px; outline:none; background:#fff; box-sizing:border-box; width:100%; }
  textarea { min-height:78px; resize:none; overflow:hidden; }

  button { border-radius:14px; border:1px solid #e6eaf2; padding:10px 12px; background:#fff; cursor:pointer; }
  button:disabled { opacity:.6; cursor:not-allowed; }
  .primary { background:#0f172a; color:#fff; border-color:#0f172a; }

  .hint { margin:0; color:#64748b; font-size:13px; }
  .alert { margin: 12px 0; padding: 10px 12px; border-radius: 14px; border: 1px solid #f3c0c0; background: #fff5f5; }
  .alert-title { font-weight: 700; margin-bottom: 6px; }
  .okbox { margin: 12px 0; padding: 10px 12px; border-radius: 14px; border: 1px solid #bbf7d0; background: #f0fdf4; color:#166534; }
  pre { margin:0; white-space: pre-wrap; word-break: break-word; }

  .auth-mode-buttons { margin:12px 0 10px; display:flex; gap:8px; }
  .auth-mode-btn {
    border:0;
    background:#0f172a;
    color:#fff;
    padding:8px 16px;
    border-radius:999px;
    font-size:12px;
    font-weight:600;
    display:inline-flex;
    gap:6px;
    align-items:center;
    min-height:34px;
    cursor:pointer;
  }
  .auth-mode-btn.active {
    background:#fff;
    color:#0f172a;
    border:1px solid transparent;
    box-shadow:0 0 0 2px rgba(15,23,42,0.1);
  }
  .auth-mode-btn:focus-visible {
    outline:2px solid #93c5fd;
  }
  .oauth-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap:8px; }
  .oauth-grid input { margin:0; }
  .auth-mode-buttons + .oauth-grid + .hint { margin-top:0; }
  .pagination-box { margin-top:10px; border:1px solid #e6eaf2; border-radius:12px; padding:10px; background:#f8fafc; }
  .dispatch-box { margin-top:10px; border:1px solid #e6eaf2; border-radius:12px; padding:10px; background:#f8fafc; }
  .parameter-subhead { margin-top:12px; }
  .data-builder-box { border:1px solid #e2e8f0; border-radius:12px; padding:10px; background:#fff; margin-top:8px; display:flex; flex-direction:column; gap:10px; }
  .data-section { display:flex; flex-direction:column; gap:6px; }
  .data-list { display:flex; flex-direction:column; gap:8px; }
  .data-row { display:grid; grid-template-columns: 1.2fr 1fr 1fr auto; gap:8px; align-items:center; }
  .join-row { grid-template-columns: 1fr 1fr 120px 1fr 1fr auto; }
  .api-showcase-grid { margin-top:8px; display:grid; grid-template-columns: minmax(280px, 1fr) minmax(320px, 1.2fr); gap:10px; align-items:start; }
  .api-showcase-col { border:1px solid #e2e8f0; border-radius:12px; background:#fff; padding:10px; }
  .api-crumbs { margin-top:8px; display:flex; flex-wrap:wrap; gap:6px; }
  .api-crumb {
    width:auto;
    border:1px solid #cbd5e1;
    border-radius:999px;
    background:#fff;
    color:#334155;
    font-size:12px;
    font-weight:600;
    line-height:1.2;
    padding:6px 11px;
    cursor:pointer;
  }
  .api-crumb.active-crumb { border-color:#0f172a; background:#0f172a; color:#fff; }
  .usage-toggle-list { margin-top:8px; display:grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap:8px; }
  .usage-toggle-item {
    display:flex;
    align-items:center;
    gap:6px;
    border:1px solid #e2e8f0;
    border-radius:10px;
    background:#f8fafc;
    padding:8px 10px;
    font-size:12px;
    color:#334155;
  }
  .usage-toggle-item input { width:auto; margin:0; }
  .dataset-preview-table-wrap { overflow:auto; border:1px solid #e2e8f0; border-radius:10px; background:#fff; }
  .dataset-preview-table { width:100%; border-collapse:collapse; min-width:640px; }
  .dataset-preview-table th,
  .dataset-preview-table td { border-bottom:1px solid #edf2f7; padding:8px; text-align:left; font-size:12px; vertical-align:top; }
  .dataset-preview-table th { background:#f8fafc; color:#334155; font-weight:600; position:sticky; top:0; z-index:1; }
  .dataset-preview-table td { max-width:320px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; color:#0f172a; }
  .pagination-toggle { display:inline-flex; align-items:center; gap:6px; font-size:12px; color:#475569; cursor:pointer; }
  .pagination-toggle input { width:auto; }
  .pagination-grid { margin-top:8px; display:grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap:8px; }
  .pagination-field small { display:block; margin-bottom:4px; font-size:11px; color:#64748b; }
  .definition-error { margin:0; font-size:11px; color:#b91c1c; }
  @media (max-width: 900px) {
    .connect-row { grid-template-columns: 1fr; }
    .connect-actions { justify-content:flex-start; flex-wrap:wrap; }
    .raw-grid { grid-template-columns: 1fr; }
    .saved-inline-actions { grid-template-columns: 1fr; }
    .api-showcase-grid { grid-template-columns: 1fr; }
    .data-row, .join-row { grid-template-columns: 1fr; }
  }
</style>







