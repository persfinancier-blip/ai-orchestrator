<script lang="ts">
  import { tick } from 'svelte';
  import JsonTreeView from '../components/JsonTreeView.svelte';
  export type ExistingTable = { schema_name: string; table_name: string };
  type HttpMethod = 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE';
  type ParameterFilterType = 'text' | 'number' | 'date' | 'boolean' | 'custom';
  type ParameterFilter = {
    type: ParameterFilterType;
    operator: string;
    value: string;
    valueTo?: string;
  };
  type ParameterSource = {
    id: string;
    schema: string;
    table: string;
    field: string;
    alias: string;
    filter: ParameterFilter;
  };
  type ParameterConnection = { schema: string; table: string };

  export let apiBase: string;
  export let apiJson: <T = any>(url: string, init?: RequestInit) => Promise<T>;
  export let headers: () => Record<string, string>;
  export let existingTables: ExistingTable[] = [];
  export let refreshTables: () => Promise<void>;

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
    parameterConnections: ParameterConnection[];
    parameterSources: ParameterSource[];
  };

  const AUTH_MODE_OAUTH2 = 'oauth2_client_credentials';

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

  const FILTER_OPERATORS: Record<ParameterFilterType, Array<{ value: string; label: string; needsValueTo?: boolean }>> = {
    text: [
      { value: 'contains', label: 'содержит' },
      { value: 'equals', label: 'равно' },
      { value: 'starts_with', label: 'начинается с' },
      { value: 'ends_with', label: 'заканчивается на' }
    ],
    number: [
      { value: 'equals', label: '=' },
      { value: 'gt', label: '>' },
      { value: 'lt', label: '<' },
      { value: 'between', label: 'между', needsValueTo: true }
    ],
    date: [
      { value: 'equals', label: '=' },
      { value: 'before', label: 'раньше' },
      { value: 'after', label: 'позже' },
      { value: 'between', label: 'между', needsValueTo: true }
    ],
    boolean: [
      { value: 'is_true', label: 'истина' },
      { value: 'is_false', label: 'ложь' }
    ],
    custom: [{ value: 'custom', label: 'своя логика' }]
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
  let columnsCache: Record<string, string[]> = {};
  let responsePathOptions: string[] = [];
  let responsePathPickerOpen = false;
  let responsePathPick = '';
  let oauthTokenCache: Record<string, { token: string; tokenType: string; expiresAt: number }> = {};
  let tableConnectValue = '';
  let builderTableValue = '';
  let builderFieldValue = '';
  let builderFilterType: ParameterFilterType = 'text';
  let builderFilterOperator = FILTER_OPERATORS.text[0].value;
  let builderFilterValue = '';
  let builderFilterValueTo = '';
  let builderAlias = '';
  let parameterMode: 'table' | 'date' | 'formula' = 'table';

  function uid() {
    return `${Date.now()}_${Math.random().toString(16).slice(2)}`;
  }

  function normalizeTypeName(type: string) {
    return String(type || '').toLowerCase().trim();
  }

  function toHttpMethod(v: string): HttpMethod {
    return v === 'GET' || v === 'POST' || v === 'PUT' || v === 'PATCH' || v === 'DELETE' ? v : 'GET';
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
      parameterConnections: [],
      parameterSources: []
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
    const parameterConnections = Array.isArray(mapping?.parameter_connections)
      ? mapping.parameter_connections
      : Array.isArray(legacy?.parameter_connections)
      ? legacy.parameter_connections
      : [];
    const parameterSources = Array.isArray(mapping?.parameter_sources)
      ? mapping.parameter_sources
      : Array.isArray(legacy?.parameter_sources)
      ? legacy.parameter_sources
      : [];
    const parsedTargets = Array.isArray(mapping?.response_targets) ? mapping.response_targets : [];
    const normalizedTargets = parsedTargets
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

    return {
      ...d,
      localId: uid(),
      storeId: Number.isFinite(storeIdNum) && storeIdNum > 0 ? Math.trunc(storeIdNum) : undefined,
      name: String(row?.api_name || legacy?.name || 'API'),
      method: toHttpMethod(String(row?.method || legacy?.method || 'GET').toUpperCase()),
      baseUrl: String(row?.base_url || legacy?.base_url || legacy?.baseUrl || ''),
      path: String(row?.path || legacy?.path || '/'),
      headersJson: toPrettyJson(tryObj(row?.headers_json || legacy?.headers_json || legacy?.headersJson || legacy?.headers)),
      authJson: toPrettyJson(tryObj(mapping?.auth_json || legacy?.auth_json || legacy?.authJson)),
      authMode: String(oauth2?.mode || 'manual') === 'oauth2_client_credentials' ? 'oauth2_client_credentials' : 'manual',
      oauth2TokenUrl: String(oauth2?.token_url || ''),
      oauth2ClientId: String(oauth2?.client_id || ''),
      oauth2ClientSecret: String(oauth2?.client_secret || ''),
      oauth2GrantType: String(oauth2?.grant_type || 'client_credentials'),
      oauth2TokenField: String(oauth2?.token_field || 'access_token'),
      oauth2ExpiresField: String(oauth2?.expires_field || 'expires_in'),
      oauth2TokenTypeField: String(oauth2?.token_type_field || 'token_type'),
      queryJson: toPrettyJson(tryObj(row?.query_json || legacy?.query_json || legacy?.queryJson || legacy?.query || legacy?.params)),
      bodyJson: toPrettyJson(tryObj(row?.body_json || legacy?.body_json || legacy?.bodyJson || legacy?.body || legacy?.data)),
      paginationEnabled: Boolean(pagination?.enabled),
      paginationStrategy: (['cursor_fields', 'page_number', 'offset_limit', 'next_url'].includes(String(pagination?.strategy || ''))
        ? String(pagination.strategy)
        : 'none') as ApiDraft['paginationStrategy'],
      paginationTarget: String(pagination?.target || 'body') === 'query' ? 'query' : 'body',
      paginationDataPath: String(pagination?.data_path || ''),
      paginationPageParam: String(pagination?.page_param || 'page'),
      paginationStartPage: Number(pagination?.start_page || 1),
      paginationLimitParam: String(pagination?.limit_param || 'limit'),
      paginationLimitValue: Number(pagination?.limit_value || 100),
      paginationCursorReqPath1: String(pagination?.cursor_req_path_1 || ''),
      paginationCursorReqPath2: String(pagination?.cursor_req_path_2 || ''),
      paginationCursorResPath1: String(pagination?.cursor_res_path_1 || ''),
      paginationCursorResPath2: String(pagination?.cursor_res_path_2 || ''),
      paginationNextUrlPath: String(pagination?.next_url_path || 'next'),
      paginationMaxPages: Number(pagination?.max_pages || 3),
      paginationDelayMs: Number(pagination?.delay_ms || 0),
      paginationCustomStrategy: String(pagination?.custom_strategy || ''),
      pickedPaths: Array.isArray(mapping?.picked_paths) ? mapping.picked_paths.map((x: any) => String(x || '').trim()).filter(Boolean) : [],
      responseTargets: normalizedTargets,
      description: String(row?.description || legacy?.description || ''),
      exampleRequest: String(mapping?.exampleRequest || legacy?.exampleRequest || ''),
      parameterConnections: parameterConnections
        .map((c: any) => ({ schema: String(c?.schema || ''), table: String(c?.table || '') }))
        .filter((c) => c.schema && c.table),
      parameterSources: parameterSources
        .map((src: any) => ({
          id: String(src?.id || uid()),
          schema: String(src?.schema || ''),
          table: String(src?.table || ''),
          field: String(src?.field || ''),
          alias: String(src?.alias || '') || '',
          filter: {
            type: (['text', 'number', 'date', 'boolean', 'custom'].includes(String(src?.filter?.type || 'text'))
              ? (src?.filter?.type as ParameterFilterType)
              : 'text') as ParameterFilterType,
            operator: String(src?.filter?.operator || ''),
            value: String(src?.filter?.value || ''),
            valueTo: src?.filter?.valueTo ? String(src.filter.valueTo) : undefined
          }
        }))
        .filter((src) => src.schema && src.table && src.field),
    };
  }

  function toPayload(
    d: ApiDraft,
    parsed?: { headersJson: Record<string, any>; queryJson: Record<string, any>; bodyJson: any; authJson: Record<string, any> }
  ) {
    const firstTarget = d.responseTargets.find((t) => t.schema && t.table);
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
        parameter_connections: d.parameterConnections,
        parameter_sources: d.parameterSources
      },
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
      const cols = Array.isArray(j?.columns) ? j.columns.map((c) => String(c?.name || '').trim()).filter(Boolean) : [];
      columnsCache = { ...columnsCache, [key]: cols };
    } catch {
      columnsCache = { ...columnsCache, [key]: [] };
    }
  }

  function columnOptionsFor(schema: string, table: string) {
    return columnsCache[tableCacheKey(schema, table)] || [];
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

  function connectionKey(conn: ParameterConnection) {
    return `${conn.schema}.${conn.table}`;
  }

  function ensureConnectionColumns(schema: string, table: string) {
    ensureColumnsFor(schema, table);
  }

  function addParameterConnectionByValue(value: string) {
    const { schema, table } = parseQualifiedTable(value);
    if (!schema || !table) return;
    mutateSelected((d) => {
      const exists = d.parameterConnections.some((c) => c.schema === schema && c.table === table);
      if (!exists) {
        d.parameterConnections = [...d.parameterConnections, { schema, table }];
      }
    });
    ensureConnectionColumns(schema, table);
  }

  function removeParameterConnection(schema: string, table: string) {
    mutateSelected((d) => {
      d.parameterConnections = d.parameterConnections.filter((c) => !(c.schema === schema && c.table === table));
      d.parameterSources = d.parameterSources.filter((p) => !(p.schema === schema && p.table === table));
    });
  }

  function handleFilterTypeSelection(value: string) {
    const normalized = (['text', 'number', 'date', 'boolean', 'custom'].includes(value) ? (value as ParameterFilterType) : 'text') as ParameterFilterType;
    builderFilterType = normalized;
    const ops = FILTER_OPERATORS[normalized];
    builderFilterOperator = ops?.[0]?.value || '';
  }

  function operatorNeedsValueTo(type: ParameterFilterType, operator: string) {
    return FILTER_OPERATORS[type]?.some((o) => o.value === operator && o.needsValueTo) ?? false;
  }

  function operatorLabel(type: ParameterFilterType, operator: string) {
    return FILTER_OPERATORS[type]?.find((o) => o.value === operator)?.label || operator;
  }

  function describeFilter(filter: ParameterFilter) {
    const label = operatorLabel(filter.type, filter.operator);
    if (!filter.value) return label;
    if (filter.valueTo) return `${label}: ${filter.value} — ${filter.valueTo}`;
    return `${label}: ${filter.value}`;
  }

  function builderTableColumns() {
    const { schema, table } = parseQualifiedTable(builderTableValue);
    if (!schema || !table) return [];
    return columnOptionsFor(schema, table);
  }

  function addParameterSource() {
    const tableRef = builderTableValue || '';
    if (!tableRef || !builderFieldValue || !builderFilterOperator || !builderFilterValue) return;
    const { schema, table } = parseQualifiedTable(tableRef);
    if (!schema || !table) return;
    const needsSecond = operatorNeedsValueTo(builderFilterType, builderFilterOperator);
    mutateSelected((d) => {
      if (!d.parameterConnections.some((c) => c.schema === schema && c.table === table)) {
        d.parameterConnections = [...d.parameterConnections, { schema, table }];
      }
      d.parameterSources = [
        ...(d.parameterSources || []),
        {
          id: uid(),
          schema,
          table,
          field: builderFieldValue,
          alias: builderAlias || `${table}.${builderFieldValue}`,
          filter: {
            type: builderFilterType,
            operator: builderFilterOperator,
            value: builderFilterValue,
            valueTo: needsSecond ? builderFilterValueTo : undefined
          }
        }
      ];
    });
    ensureConnectionColumns(schema, table);
    builderFieldValue = '';
    builderFilterValue = '';
    builderFilterValueTo = '';
    builderAlias = '';
  }

  function removeParameterSource(id: string) {
    mutateSelected((d) => {
      d.parameterSources = d.parameterSources.filter((p) => p.id !== id);
    });
  }

  $: if (!tableConnectValue && existingTables.length) {
    tableConnectValue = `${existingTables[0].schema_name}.${existingTables[0].table_name}`;
  }

  $: if (!builderTableValue && selected?.parameterConnections?.length) {
    builderTableValue = `${selected.parameterConnections[0].schema}.${selected.parameterConnections[0].table}`;
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
        path = u.pathname || '/';
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
        d.path = u.pathname || '/';
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
          path: u.pathname || '/',
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
        finalPath = u.pathname || '/';
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
          path: u.pathname || '/',
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

  function handlePaginationStrategyChange(value: string) {
    const normalized = PAGINATION_STRATEGIES.some((s) => s.value === value) ? value : 'none';
    mutateSelected((d) => (d.paginationStrategy = normalized as ApiDraft['paginationStrategy']));
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
    responseStatus = 0;
    responseText = '';
    if (!selected) {
      err = 'Выбери API';
      return;
    }
    checking = true;
    try {
      applyUrlInput(requestInput);
      const s = byRef(selectedRef) || selected;
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

      let nextUrlOverride = '';
      const pagesMax = s.paginationEnabled ? Math.max(1, Number(s.paginationMaxPages || 1)) : 1;
      const pagePayloads: any[] = [];
      let lastStatus = 0;
      let pageCounter = 0;
      let currentPage = Number(s.paginationStartPage || 1);
      let currentOffset = 0;

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
        const u = new URL(url);
        for (const [k, v] of Object.entries(queryObj || {})) u.searchParams.set(k, String(v));
        url = u.toString();

        const init: RequestInit = {
          method: s.method,
          headers: { 'Content-Type': 'application/json', ...authHdr, ...hdr }
        };
        if (s.method !== 'GET' && s.method !== 'DELETE') {
          init.body = JSON.stringify(bodyObj || {});
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

      if (s.paginationEnabled && pagePayloads.length > 1) {
        responseText = JSON.stringify({ pages: pageCounter, last_status: lastStatus, samples: pagePayloads }, null, 2);
      }
      ok = s.paginationEnabled ? `Проверка выполнена, страниц: ${pageCounter}` : 'Проверка выполнена';
    } catch (e: any) {
      err = e?.message ?? String(e);
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
      activeResponseFieldRef = '';
    }
  }
  $: myApiPreview = selected
    ? JSON.stringify(
        {
          method: selected.method,
          url: previewUrlFromInput(selected),
          auth_mode: selected.authMode,
          auth: safePreviewObj(selected.authJson),
          oauth2:
            selected.authMode === 'oauth2_client_credentials'
              ? {
                  token_url: selected.oauth2TokenUrl,
                  client_id: selected.oauth2ClientId,
                  grant_type: selected.oauth2GrantType,
                  token_field: selected.oauth2TokenField
                }
              : undefined,
          headers: safePreviewObj(selected.headersJson),
          query: safePreviewObj(selected.queryJson),
          body: selected.method === 'GET' || selected.method === 'DELETE' ? undefined : safePreviewObj(selected.bodyJson),
          pagination:
            selected.paginationEnabled
              ? {
                  strategy: selected.paginationStrategy,
                  target: selected.paginationTarget,
                  data_path: selected.paginationDataPath,
                  max_pages: selected.paginationMaxPages
                }
              : undefined
        },
        null,
        2
      )
    : '';
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
  $: {
    const rows = mappingRowsOf(selected);
    for (const row of rows) {
      void ensureColumnsFor(row.schema, row.table);
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
        {#if responseIsJson && responseViewMode === 'tree'}
          <div class="response-tree-wrap">
            <JsonTreeView node={responseJson} name="response" level={0} pickEnabled={true} on:pickpath={(e) => applyPickedResponsePath(e.detail.path)} />
          </div>
        {:else}
          <textarea bind:this={responsePreviewEl} readonly value={responseText}></textarea>
        {/if}
      </div>
      <div class="subsec">
        <div class="subttl">Витрина параметров</div>
        <div class="parameter-vitrina-block">
          <div class="param-mode-row">
            <button
              type="button"
              class="view-toggle param-mode-btn"
              class:active={parameterMode === 'table'}
              on:click={() => (parameterMode = 'table')}
            >
              Таблицы
            </button>
            <button
              type="button"
              class="view-toggle param-mode-btn"
              class:active={parameterMode === 'date'}
              on:click={() => (parameterMode = 'date')}
            >
              Даты
            </button>
            <button
              type="button"
              class="view-toggle param-mode-btn"
              class:active={parameterMode === 'formula'}
              on:click={() => (parameterMode = 'formula')}
            >
              Формулы
            </button>
          </div>
          {#if parameterMode === 'table'}
            <div class="parameter-table-picker">
              <select bind:value={tableConnectValue}>
                {#each existingTables as tbl}
                  <option value={`${tbl.schema_name}.${tbl.table_name}`}>{tbl.schema_name}.{tbl.table_name}</option>
                {/each}
              </select>
              <button class="primary" type="button" on:click={() => addParameterConnectionByValue(tableConnectValue)}>Подключить</button>
            </div>
          {/if}
          {#if selected?.parameterSources?.length}
            <div class="parameter-list">
              {#each selected.parameterSources as src (src.id)}
                <div class="parameter-chip">
                  <div>
                    <div class="param-chip-title">{src.alias || `${src.table}.${src.field}`}</div>
                    <div class="param-chip-sub">{describeFilter(src.filter)}</div>
                  </div>
                  <button class="chip-remove" type="button" on:click={() => removeParameterSource(src.id)}>x</button>
                </div>
              {/each}
            </div>
          {:else}
            <p class="hint">Параметры пока не выбраны. Добавь таблицы и поля внизу.</p>
          {/if}
        </div>
      </div>
      <div class="subsec">
        <div class="subttl response-head">
          <span>Предпросмотр твоего API</span>
          <span class="inline-actions">
            <button type="button" class="view-toggle" on:click={applyMyPreviewToFields}>Сохранить</button>
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
            value={myApiPreviewDraft}
            on:input={(e) => {
              myApiPreviewDraft = e.currentTarget.value;
              myPreviewDirty = true;
            }}
          ></textarea>
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
          <button class="primary" on:click={checkApiNow} disabled={checking}>{checking ? 'Проверка...' : 'Проверить'}</button>
        </div>

        <textarea
          bind:this={descriptionEl}
          class="desc"
          placeholder="Описание API"
          value={selected?.description || ''}
          on:input={(e) => mutateSelected((d) => (d.description = e.currentTarget.value))}
        ></textarea>

        <div class="parameter-connect-block">
          <div class="param-connections-row">
            <span>Подключенные таблицы</span>
            <div class="param-connection-actions">
              <select
                value={tableConnectValue}
                on:change={(e) => (tableConnectValue = e.currentTarget.value)}
              >
                {#each existingTables as tbl}
                  <option value={`${tbl.schema_name}.${tbl.table_name}`}>{tbl.schema_name}.{tbl.table_name}</option>
                {/each}
              </select>
              <button
                class="icon-btn plus-dark"
                type="button"
                title="Подключить таблицу"
                on:click={() => addParameterConnectionByValue(tableConnectValue)}
              >
                +
              </button>
            </div>
          </div>
          <div class="param-connection-crumbs">
            {#if !(selected?.parameterConnections?.length)}
              <p class="hint">Подключи таблицу, чтобы использовать её поля в параметрах.</p>
            {:else}
              <div class="table-crumbs">
                {#each selected.parameterConnections as conn (connectionKey(conn))}
                  <div class="table-chip">
                    <span>{conn.schema}.{conn.table}</span>
                    <button type="button" class="chip-remove" on:click={() => removeParameterConnection(conn.schema, conn.table)}>x</button>
                  </div>
                {/each}
              </div>
            {/if}
          </div>

          <div class="parameter-builder">
            <div class="parameter-builder-row">
              <select
                value={builderTableValue}
                on:change={(e) => {
                  builderTableValue = e.currentTarget.value;
                  builderFieldValue = '';
                  const parsed = parseQualifiedTable(builderTableValue);
                  ensureConnectionColumns(parsed.schema, parsed.table);
                }}
              >
                <option value="">Таблица из подключенных</option>
                {#each selected?.parameterConnections || [] as conn}
                  <option value={`${conn.schema}.${conn.table}`}>{conn.schema}.{conn.table}</option>
                {/each}
              </select>
              <select
                value={builderFieldValue}
                on:change={(e) => {
                  builderFieldValue = e.currentTarget.value;
                }}
              >
                <option value="">Поле таблицы</option>
                {#if builderTableValue}
                  {#each builderTableColumns() as field}
                    <option value={field}>{field}</option>
                  {/each}
                {/if}
              </select>
              <select
                value={builderFilterType}
                on:change={(e) => handleFilterTypeSelection(e.currentTarget.value)}
              >
                {#each Object.keys(FILTER_OPERATORS) as type}
                  <option value={type}>{type}</option>
                {/each}
              </select>
              <select
                value={builderFilterOperator}
                on:change={(e) => (builderFilterOperator = e.currentTarget.value)}
              >
                {#each FILTER_OPERATORS[builderFilterType] as op}
                  <option value={op.value}>{op.label}</option>
                {/each}
              </select>
              <input
                placeholder="Значение"
                value={builderFilterValue}
                on:input={(e) => (builderFilterValue = e.currentTarget.value)}
              />
              {#if operatorNeedsValueTo(builderFilterType, builderFilterOperator)}
                <input
                  placeholder="до"
                  value={builderFilterValueTo}
                  on:input={(e) => (builderFilterValueTo = e.currentTarget.value)}
                />
              {/if}
              <button
                class="icon-btn plus-green"
                type="button"
                title="Добавить параметр"
                on:click={addParameterSource}
                disabled={!builderTableValue || !builderFieldValue || !builderFilterValue}
              >
                +
              </button>
            </div>
            <div class="parameter-builder-row">
              <input
                placeholder="Псевдоним параметра"
                value={builderAlias}
                on:input={(e) => (builderAlias = e.currentTarget.value)}
              />
              <p class="hint">Параметры можно использовать как токены/ID, фильтры — любые условия (текст, числа, даты).</p>
            </div>
          </div>

          <div class="parameter-vitrina">
            {#if selected?.parameterSources?.length}
              <div class="parameter-list">
                {#each selected.parameterSources as src (src.id)}
                  <div class="parameter-chip">
                    <div>
                      <div class="param-chip-title">{src.alias || `${src.table}.${src.field}`}</div>
                      <div class="param-chip-sub">{describeFilter(src.filter)}</div>
                    </div>
                    <button class="chip-remove" type="button" on:click={() => removeParameterSource(src.id)}>x</button>
                  </div>
                {/each}
              </div>
            {:else}
              <p class="hint">Добавь параметр, чтобы он появился в витрине.</p>
            {/if}
          </div>
        </div>

        <label>
          <div class="response-head field-head">
            <span>Авторизация</span>
            {#if authJsonValid}
              <button type="button" class="view-toggle" on:click={() => (authViewMode = authViewMode === 'tree' ? 'raw' : 'tree')}>
                {authViewMode === 'tree' ? 'RAW' : 'Дерево'}
              </button>
            {/if}
          </div>
          {#if authJsonValid && authViewMode === 'tree'}
            <div class="response-tree-wrap"><JsonTreeView node={authJsonTree} name="auth" level={0} /></div>
          {:else}
            <textarea
              bind:this={authEl}
              value={selected?.authJson || ''}
              on:input={(e) => mutateSelected((d) => (d.authJson = e.currentTarget.value))}
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
            {#if headersJsonValid && headersViewMode === 'tree'}
              <div class="response-tree-wrap"><JsonTreeView node={headersJsonTree} name="headers" level={0} /></div>
            {:else}
              <textarea
                bind:this={headersEl}
                value={selected?.headersJson || ''}
                on:input={(e) => mutateSelected((d) => (d.headersJson = e.currentTarget.value))}
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
            {#if queryJsonValid && queryViewMode === 'tree'}
              <div class="response-tree-wrap"><JsonTreeView node={queryJsonTree} name="query" level={0} /></div>
            {:else}
              <textarea
                bind:this={queryEl}
                value={selected?.queryJson || ''}
                on:input={(e) => mutateSelected((d) => (d.queryJson = e.currentTarget.value))}
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
          {#if bodyJsonValid && bodyViewMode === 'tree'}
            <div class="response-tree-wrap"><JsonTreeView node={bodyJsonTree} name="body" level={0} /></div>
          {:else}
            <textarea
              bind:this={bodyEl}
              value={selected?.bodyJson || ''}
              on:input={(e) => mutateSelected((d) => (d.bodyJson = e.currentTarget.value))}
            ></textarea>
          {/if}
        </label>

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
  .inline-actions { display:inline-flex; align-items:center; gap:6px; flex-wrap:nowrap; }
  .view-toggle { border-radius:10px; border:1px solid #e2e8f0; background:#fff; color:#0f172a; padding:4px 8px; font-size:11px; line-height:1.2; }
  .response-tree-wrap { border:1px solid #e6eaf2; border-radius:12px; background:#fff; padding:8px; min-height:78px; overflow:visible; }
  .template-head { display:flex; align-items:center; justify-content:space-between; gap:8px; }
  .statusline { font-size:12px; color:#64748b; margin-bottom:6px; }
  .template-parse-actions { margin-top:8px; display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
  .template-parse-note { font-size:12px; color:#64748b; }

  .main { min-width:0; }
  .card { border:1px solid #e6eaf2; border-radius:16px; padding:12px; background:#fff; }

  .connect-row { margin-top:10px; display:grid; grid-template-columns: 180px 1fr 150px; gap:8px; align-items:center; }
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
  .parameter-connect-block { margin-top:12px; border:1px solid #e6eaf2; border-radius:14px; padding:12px; background:#f1f5f9; }
  .param-connections-row { display:flex; align-items:center; justify-content:space-between; gap:10px; margin-bottom:8px; font-weight:600; color:#0f172a; }
  .param-connection-actions { display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
  .table-crumbs { display:flex; flex-wrap:wrap; gap:6px; }
  .table-chip { display:inline-flex; align-items:center; gap:6px; border-radius:10px; background:#0f172a; color:#fff; padding:4px 10px; font-size:12px; }
  .table-chip .chip-remove { background:transparent; border:0; color:#fee2e2; padding:0 4px; font-size:12px; cursor:pointer; }
  .parameter-builder { border:1px solid #e2e8f0; border-radius:12px; background:#fff; padding:10px; margin-bottom:10px; }
  .parameter-builder-row { display:grid; gap:8px; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); align-items:center; }
  .parameter-vitrina { border:1px solid #e2e8f0; border-radius:12px; background:#fff; padding:10px; }
  .parameter-list { display:flex; flex-direction:column; gap:6px; }
  .parameter-chip { display:flex; align-items:center; justify-content:space-between; gap:10px; border-radius:10px; border:1px solid #dbe3ef; padding:8px 10px; background:#f8fafc; }
  .param-chip-title { font-size:13px; font-weight:600; color:#0f172a; }
  .param-chip-sub { font-size:11px; color:#475569; }
  .parameter-vitrina-block {
    margin-top:10px;
    border:1px solid #e6eaf2;
    border-radius:14px;
    background:#fff;
    padding:10px;
  }
  .parameter-table-picker { margin-top:8px; display:flex; gap:8px; align-items:center; }
  .parameter-table-picker select { flex:1; }
  .targets-actions { display:flex; align-items:center; }
  .param-mode-row { display:flex; gap:8px; }
  .param-mode-btn {
    border:1px solid #0f172a;
    background:#0f172a;
    color:#f1f5f9;
    font-weight:600;
    display:inline-flex;
    align-items:center;
    gap:4px;
    cursor:pointer;
    border-radius:999px;
    padding:6px 14px;
    font-size:12px;
  }
  .param-mode-btn.active {
    background:#fff;
    color:#0f172a;
    border-color:#0f172a;
  }
  .param-mode-btn.active::before {
    content:'●';
    color:#0f172a;
    font-size:10px;
    margin-right:4px;
  }
  .parameter-table-picker { margin-top:8px; display:flex; gap:8px; align-items:center; }
  .parameter-table-picker select { flex:1; }
  .oauth-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap:8px; }
  .oauth-grid input { margin:0; }
  .auth-mode-buttons + .oauth-grid + .hint { margin-top:0; }
  .pagination-box { margin-top:10px; border:1px solid #e6eaf2; border-radius:12px; padding:10px; background:#f8fafc; }
  .pagination-toggle { display:inline-flex; align-items:center; gap:6px; font-size:12px; color:#475569; cursor:pointer; }
  .pagination-toggle input { width:auto; }
  .pagination-grid { margin-top:8px; display:grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap:8px; }
  .pagination-field small { display:block; margin-bottom:4px; font-size:11px; color:#64748b; }
</style>







