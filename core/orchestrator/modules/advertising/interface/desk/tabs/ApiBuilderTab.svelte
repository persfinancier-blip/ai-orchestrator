<script lang="ts">
  import { tick } from 'svelte';

  export type ExistingTable = { schema_name: string; table_name: string };
  type HttpMethod = 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE';
  type AuthMode = 'none' | 'bearer' | 'basic' | 'apiKey';
  type PaginationMode = 'none' | 'page' | 'offset';
  type BindingTarget = 'query' | 'header' | 'body';

  export let apiBase: string;
  export let apiJson: <T = any>(url: string, init?: RequestInit) => Promise<T>;
  export let headers: () => Record<string, string>;
  export let existingTables: ExistingTable[] = [];
  export let refreshTables: () => Promise<void>;

  type AuthConfig = {
    mode: AuthMode;
    bearerToken: string;
    basicUsername: string;
    basicPassword: string;
    apiKeyName: string;
    apiKeyValue: string;
    apiKeyIn: 'header' | 'query';
  };

  type PaginationConfig = {
    mode: PaginationMode;
    pageParam: string;
    pageSizeParam: string;
    pageStart: number;
    defaultPageSize: number;
    offsetParam: string;
    limitParam: string;
    offsetStart: number;
    defaultLimit: number;
  };

  type DbBinding = {
    id: string;
    target: BindingTarget;
    targetKey: string;
    sourceColumn: string;
  };

  type DbConfig = {
    schema: string;
    table: string;
    rowIndex: number;
    bindings: DbBinding[];
  };

  type AuthTemplateField = { key: string; value: string };
  type AuthTemplateModel = { name: string; type: string; fields: AuthTemplateField[] };

  type ApiSource = {
    id: string;
    storeId?: number;
    name: string;
    baseUrl: string;
    method: HttpMethod;
    path: string;
    headersJson: string;
    queryJson: string;
    bodyJson: string;
    exampleRequest: string;
    auth: AuthConfig;
    authTemplate: AuthTemplateModel;
    pagination: PaginationConfig;
    db: DbConfig;
    createdAt: number;
    updatedAt: number;
  };

  type HistoryItem = {
    ts: number;
    sourceId?: string;
    method: HttpMethod;
    url: string;
    status?: number;
    ms?: number;
    requestHeadersJson: string;
    requestBodyJson: string;
    responseText?: string;
  };

  const HISTORY_KEY = 'ao_api_builder_history_v1';
  const AUTH_TEMPLATES_KEY = 'ao_api_builder_auth_templates_v1';

  const PLACEHOLDER_HEADERS = '{"Authorization":"Bearer ..."}';
  const PLACEHOLDER_QUERY = '{"date_from":"2026-01-01"}';
  const PLACEHOLDER_BODY = '{"limit":100}';

  let sources: ApiSource[] = [];
  let selectedId: string | null = null;

  let history: HistoryItem[] = [];
  let pageSize = 10;
  let page = 1;

  let err = '';
  let sending = false;

  let respStatus = 0;
  let respHeaders: Record<string, string> = {};
  let respText = '';

  let dbPreviewRows: any[] = [];
  let dbPreviewColumns: string[] = [];
  let dbPreviewLoading = false;
  let dbPreviewError = '';
  let dbLoadedKey = '';
  let api_storage_schema = 'ao_system';
  let api_storage_table = 'api_configs_store';
  let api_storage_picker_open = false;
  let api_storage_pick_value = '';
  let generatedApiPreview = '';
  let previewDraft = '';
  let editingPreview = false;
  let previewSyncError = '';
  let previewApplyMessage = '';
  let exampleRequestEl: HTMLTextAreaElement | null = null;
  let generatedPreviewEl: HTMLTextAreaElement | null = null;
  let authFieldsEl: HTMLDivElement | null = null;
  let authRawEl: HTMLTextAreaElement | null = null;
  let urlInput = '';

  type KvRow = { id: string; key: string; value: string };
  type BodyRow = { id: string; path: string; value: string };
  let paramsRawDraft = '';
  let headersRawDraft = '';
  let bodyRawDraft = '';
  let authRawDraft = '';
  let authRawError = '';
  let authRawTouched = false;
  let authSyncLock: 'left' | 'right' | null = null;
  let authRawDebounce: any = null;
  let authTemplateNameDraft = '';
  let authTemplateRows: KvRow[] = [];
  let authTemplates: Array<{ id: string; name: string; type: string; fields: AuthTemplateField[] }> = [];
  let selectedAuthTemplateId = '';
  let headersRawError = '';
  let headersSyncLock: 'left' | 'right' | null = null;
  let headersRawDebounce: any = null;
  let settingsRawDraft = '';
  let scriptRawDraft = '';
  let paramRows: KvRow[] = [];
  let headerRows: KvRow[] = [];
  let bodyRows: BodyRow[] = [];
  let lastEditorSourceId = '';
  let apiNameDraft = '';
  let lastNameDraftSourceId = '';
  const API_STORAGE_REQUIRED_COLUMNS: Array<{ name: string; types: string[] }> = [
    { name: 'api_name', types: ['text', 'character varying', 'varchar'] },
    { name: 'method', types: ['text', 'character varying', 'varchar'] },
    { name: 'base_url', types: ['text', 'character varying', 'varchar'] },
    { name: 'path', types: ['text', 'character varying', 'varchar'] },
    { name: 'headers_json', types: ['jsonb', 'json'] },
    { name: 'query_json', types: ['jsonb', 'json'] },
    { name: 'body_json', types: ['jsonb', 'json'] },
    { name: 'pagination_json', types: ['jsonb', 'json'] },
    { name: 'target_schema', types: ['text', 'character varying', 'varchar'] },
    { name: 'target_table', types: ['text', 'character varying', 'varchar'] },
    { name: 'mapping_json', types: ['jsonb', 'json'] },
    { name: 'description', types: ['text', 'character varying', 'varchar'] },
    { name: 'is_active', types: ['boolean'] },
    { name: 'updated_at', types: ['timestamp with time zone', 'timestamptz', 'timestamp'] },
    { name: 'updated_by', types: ['text', 'character varying', 'varchar'] }
  ];

  function uid() {
    return `${Date.now()}_${Math.random().toString(16).slice(2)}`;
  }

  function safeJsonParse(text: string): any {
    return text.trim() ? JSON.parse(text) : {};
  }

  function normalizeTypeName(type: string) {
    return String(type || '').toLowerCase().trim();
  }

  function objectToKvRows(obj: any): KvRow[] {
    if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return [];
    return Object.entries(obj).map(([k, v]) => ({ id: uid(), key: k, value: String(v ?? '') }));
  }

  function kvRowsToObject(rows: KvRow[]): Record<string, any> {
    const o: Record<string, any> = {};
    for (const r of rows) {
      const k = (r.key || '').trim();
      if (!k) continue;
      o[k] = r.value;
    }
    return o;
  }

  function authFieldsToRawObject(fields: AuthTemplateField[]): Record<string, string> {
    const out: Record<string, string> = {};
    for (const f of fields || []) {
      const k = String(f?.key || '').trim();
      if (!k) continue;
      out[k] = String(f?.value ?? '');
    }
    return out;
  }

  function flattenBodyObject(obj: any, prefix = '', out: BodyRow[] = []): BodyRow[] {
    if (obj && typeof obj === 'object' && !Array.isArray(obj)) {
      for (const [k, v] of Object.entries(obj)) {
        const p = prefix ? `${prefix}.${k}` : k;
        if (v && typeof v === 'object' && !Array.isArray(v)) flattenBodyObject(v, p, out);
        else out.push({ id: uid(), path: p, value: typeof v === 'string' ? v : JSON.stringify(v) });
      }
      return out;
    }
    return out;
  }

  function bodyRowsToObject(rows: BodyRow[]): any {
    const root: any = {};
    for (const r of rows) {
      const path = (r.path || '').trim();
      if (!path) continue;
      const parts = path.split('.').filter(Boolean);
      if (!parts.length) continue;
      let cur = root;
      for (let i = 0; i < parts.length - 1; i += 1) {
        const p = parts[i];
        if (!cur[p] || typeof cur[p] !== 'object' || Array.isArray(cur[p])) cur[p] = {};
        cur = cur[p];
      }
      const leaf = parts[parts.length - 1];
      let val: any = r.value;
      try { val = JSON.parse(r.value); } catch {}
      cur[leaf] = val;
    }
    return root;
  }

  function syncEditorsFromSelected(force = false) {
    if (!selected) return;
    if (!force && lastEditorSourceId === selected.id) return;
    lastEditorSourceId = selected.id;
    urlInput = `${selected.baseUrl.replace(/\/$/, '')}${selected.path.startsWith('/') ? selected.path : `/${selected.path}`}`;
    paramsRawDraft = selected.queryJson || '';
    headersRawDraft = selected.headersJson || '';
    headersRawError = '';
    bodyRawDraft = selected.bodyJson || '';
    const t = selected.authTemplate || { name: '', type: 'custom', fields: [] };
    authTemplateNameDraft = String(t.name || '');
    authTemplateRows = (t.fields || []).map((f) => ({ id: uid(), key: f.key, value: f.value }));
    const matched = authTemplates.find((x) => x.name === t.name);
    selectedAuthTemplateId = matched?.id || '';
    authRawDraft = JSON.stringify(authFieldsToRawObject(t.fields || []), null, 2);
    authRawError = '';
    settingsRawDraft = JSON.stringify({ pagination: selected.pagination || {} }, null, 2);
    scriptRawDraft = scriptRawDraft || '';
    try { paramRows = objectToKvRows(safeJsonParse(paramsRawDraft)); } catch { paramRows = []; }
    try { headerRows = objectToKvRows(safeJsonParse(headersRawDraft)); } catch { headerRows = []; }
    try { bodyRows = flattenBodyObject(safeJsonParse(bodyRawDraft)); } catch { bodyRows = []; }
  }

  function applyUrlInputRaw(raw: string) {
    const t = (raw || '').trim();
    if (!t) return;
    if (tryApplyCurlPreview(t)) return;
    try {
      const u = new URL(t);
      const queryObj: Record<string, string> = {};
      u.searchParams.forEach((v, k) => (queryObj[k] = v));
      mutateSelected((s) => {
        s.baseUrl = u.origin;
        s.path = u.pathname || '/';
        s.queryJson = Object.keys(queryObj).length ? JSON.stringify(queryObj, null, 2) : '';
      });
      syncEditorsFromSelected(true);
    } catch {
      err = 'Некорректный URL';
    }
  }

  function syncParamsRowsToRaw() {
    const raw = JSON.stringify(kvRowsToObject(paramRows), null, 2);
    paramsRawDraft = raw;
    mutateSelected((s) => (s.queryJson = raw === '{}' ? '' : raw));
  }

  function syncHeadersRowsToRaw() {
    if (headersSyncLock === 'right') return;
    headersSyncLock = 'left';
    const raw = JSON.stringify(kvRowsToObject(headerRows), null, 2);
    headersRawDraft = raw;
    mutateSelected((s) => (s.headersJson = raw === '{}' ? '' : raw));
    headersRawError = '';
    headersSyncLock = null;
  }

  function syncBodyRowsToRaw() {
    const raw = JSON.stringify(bodyRowsToObject(bodyRows), null, 2);
    bodyRawDraft = raw;
    mutateSelected((s) => (s.bodyJson = raw === '{}' ? '' : raw));
  }

  function parseParamsRaw() {
    try {
      paramRows = objectToKvRows(safeJsonParse(paramsRawDraft));
      mutateSelected((s) => (s.queryJson = paramsRawDraft.trim()));
      err = '';
    } catch {
      err = 'Параметры RAW: некорректный JSON';
    }
  }

  function parseHeadersRaw() {
    try {
      const parsed = safeJsonParse(headersRawDraft);
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) throw new Error('bad_raw');
      headersSyncLock = 'right';
      headerRows = objectToKvRows(parsed);
      mutateSelected((s) => (s.headersJson = headersRawDraft.trim() || ''));
      headersRawError = '';
      err = '';
    } catch {
      headersRawError = 'Некорректный JSON. Проверь скобки и кавычки.';
    } finally {
      headersSyncLock = null;
    }
  }

  function scheduleParseHeadersRaw() {
    if (headersSyncLock === 'left') return;
    if (headersRawDebounce) clearTimeout(headersRawDebounce);
    headersRawDebounce = setTimeout(() => {
      parseHeadersRaw();
    }, 400);
  }

  function parseBodyRaw() {
    try {
      bodyRows = flattenBodyObject(safeJsonParse(bodyRawDraft));
      mutateSelected((s) => (s.bodyJson = bodyRawDraft.trim()));
      err = '';
    } catch {
      err = 'Body RAW: некорректный JSON';
    }
  }

  function parseAuthRaw() {
    authRawTouched = true;
    try {
      const a = safeJsonParse(authRawDraft);
      if (!a || typeof a !== 'object' || Array.isArray(a)) throw new Error('bad_raw');
      const fields = Object.entries(a).map(([key, value]) => ({
        key: String(key || ''),
        value: String(value ?? '')
      }));
      const next: AuthTemplateModel = canonicalAuthTemplateFromLeft();
      next.fields = fields;
      authSyncLock = 'right';
      authTemplateRows = next.fields.map((f) => ({ id: uid(), key: f.key, value: f.value }));
      mutateSelected((s) => {
        s.authTemplate = { ...next, fields: [...next.fields] };
      });
      authRawError = '';
      err = '';
    } catch {
      authRawError = 'Некорректный JSON. Проверь скобки и кавычки.';
    } finally {
      authSyncLock = null;
    }
  }

  function canonicalAuthTemplateFromLeft(): AuthTemplateModel {
    const cur = getSelected()?.authTemplate || selected?.authTemplate || { name: '', type: 'header', fields: [] };
    const fields = authTemplateRows
      .map((r) => ({ key: (r.key || '').trim(), value: r.value || '' }))
      .filter((r) => r.key.length > 0);
    return {
      name: String(cur.name || ''),
      type: String(cur.type || 'header'),
      fields
    };
  }

  function syncAuthLeftToRawAndSource() {
    if (!selected) return;
    if (authSyncLock === 'right') return;
    authSyncLock = 'left';
    const next = canonicalAuthTemplateFromLeft();
    authRawDraft = JSON.stringify(authFieldsToRawObject(next.fields), null, 2);
    authRawError = '';
    mutateSelected((s) => {
      s.authTemplate = { ...next, fields: [...next.fields] };
    });
    authSyncLock = null;
  }

  function scheduleParseAuthRaw() {
    if (authSyncLock === 'left') return;
    if (authRawDebounce) clearTimeout(authRawDebounce);
    authRawDebounce = setTimeout(() => {
      parseAuthRaw();
    }, 400);
  }

  function addAuthTemplateRow() {
    authTemplateRows = [...authTemplateRows, { id: uid(), key: '', value: '' }];
    syncAuthLeftToRawAndSource();
  }

  function removeAuthTemplateRow(id: string) {
    authTemplateRows = authTemplateRows.filter((r) => r.id !== id);
    syncAuthLeftToRawAndSource();
  }

  function setAuthTemplateName(name: string) {
    authTemplateNameDraft = name;
    mutateSelected((s) => {
      s.authTemplate.name = name;
    });
  }

  function setAuthTemplatePlacement(type: string) {
    mutateSelected((s) => {
      s.authTemplate.type = type === 'query' ? 'query' : 'header';
    });
    syncAuthLeftToRawAndSource();
  }

  function authTypeLabel(type: string) {
    return type === 'query' ? 'URL' : 'Заголовок';
  }

  function applyAuthPreset(kind: 'bearer' | 'api_header' | 'api_query') {
    if (!selected) return;
    if (kind === 'bearer') {
      authTemplateRows = [{ id: uid(), key: 'Authorization', value: 'Bearer YOUR_TOKEN' }];
      mutateSelected((s) => {
        if (!String(s.authTemplate.name || '').trim()) {
          s.authTemplate.name = 'Bearer токен';
          authTemplateNameDraft = s.authTemplate.name;
        }
        s.authTemplate.type = 'header';
      });
    }
    if (kind === 'api_header') {
      authTemplateRows = [{ id: uid(), key: 'X-API-Key', value: 'YOUR_API_KEY' }];
      mutateSelected((s) => {
        if (!String(s.authTemplate.name || '').trim()) {
          s.authTemplate.name = 'API ключ в заголовке';
          authTemplateNameDraft = s.authTemplate.name;
        }
        s.authTemplate.type = 'header';
      });
    }
    if (kind === 'api_query') {
      authTemplateRows = [{ id: uid(), key: 'api_key', value: 'YOUR_API_KEY' }];
      mutateSelected((s) => {
        if (!String(s.authTemplate.name || '').trim()) {
          s.authTemplate.name = 'API ключ в URL';
          authTemplateNameDraft = s.authTemplate.name;
        }
        s.authTemplate.type = 'query';
      });
    }
    syncAuthLeftToRawAndSource();
  }

  function loadAuthTemplates() {
    authTemplates = [];
  }

  function saveAuthTemplates() {
    // local storage disabled
  }

  function saveCurrentAuthTemplate() {
    if (!selected) return;
    if (authRawError) {
      err = 'Сначала исправьте RAW JSON';
      return;
    }
    try {
      const rawTemplate = safeJsonParse(authRawDraft);
      if (!rawTemplate || typeof rawTemplate !== 'object' || Array.isArray(rawTemplate)) {
        err = 'RAW должен быть JSON-объектом';
        return;
      }
    } catch {
      err = 'Сначала исправьте RAW JSON';
      return;
    }
    const left = canonicalAuthTemplateFromLeft();
    const t: AuthTemplateModel = {
      name: String(authTemplateNameDraft || selected.authTemplate?.name || '').trim(),
      type: String(selected.authTemplate?.type || 'header'),
      fields: left.fields
    };
    if (!t.name.trim()) {
      err = 'Введите название шаблона типа подключения';
      return;
    }
    if (t.fields.some((f) => !f.key.trim())) {
      err = 'У шаблона есть поле с пустым названием';
      return;
    }
    try {
      JSON.parse(JSON.stringify(t));
    } catch {
      err = 'Некорректный JSON. Проверь скобки и кавычки.';
      return;
    }
    const idxBySelected = selectedAuthTemplateId
      ? authTemplates.findIndex((x) => x.id === selectedAuthTemplateId)
      : -1;
    const idxByName = idxBySelected < 0 ? authTemplates.findIndex((x) => x.name === t.name) : -1;
    const idx = idxBySelected >= 0 ? idxBySelected : idxByName;
    const item = { id: idx >= 0 ? authTemplates[idx].id : uid(), ...t };
    if (idx >= 0) authTemplates[idx] = item;
    else authTemplates = [item, ...authTemplates];
    saveAuthTemplates();
    selectedAuthTemplateId = item.id;
    previewApplyMessage = 'Шаблон сохранён';
    err = '';
  }

  function deleteCurrentAuthTemplate() {
    if (!selectedAuthTemplateId) {
      err = 'Сначала выбери шаблон для удаления';
      return;
    }
    authTemplates = authTemplates.filter((x) => x.id !== selectedAuthTemplateId);
    saveAuthTemplates();
    selectedAuthTemplateId = '';
    previewApplyMessage = 'Шаблон удалён';
    err = '';
  }

  function startNewAuthTemplate() {
    if (!selected) return;
    selectedAuthTemplateId = '';
    authTemplateNameDraft = '';
    authTemplateRows = [];
    mutateSelected((s) => {
      s.authTemplate = { name: '', type: 'header', fields: [] };
    });
    authRawDraft = '{}';
    authRawError = '';
    err = '';
  }

  function applySelectedAuthTemplate(id: string) {
    selectedAuthTemplateId = id;
    const t = authTemplates.find((x) => x.id === id);
    if (!t || !selected) return;
    authSyncLock = 'right';
    authTemplateNameDraft = t.name;
    authTemplateRows = t.fields.map((f) => ({ id: uid(), key: f.key, value: f.value }));
    mutateSelected((s) => {
      s.authTemplate = { name: t.name, type: t.type, fields: t.fields.map((f) => ({ ...f })) };
    });
    authRawDraft = JSON.stringify(authFieldsToRawObject(t.fields), null, 2);
    authRawError = '';
    authSyncLock = null;
  }

  function parseSettingsRaw() {
    try {
      const st = safeJsonParse(settingsRawDraft);
      const p = st.pagination || st;
      mutateSelected((s) => {
        s.pagination.mode = toPaginationMode(String(p.mode ?? s.pagination.mode));
        s.pagination.pageParam = String(p.pageParam ?? s.pagination.pageParam);
        s.pagination.pageSizeParam = String(p.pageSizeParam ?? s.pagination.pageSizeParam);
        s.pagination.pageStart = Number(p.pageStart ?? s.pagination.pageStart);
        s.pagination.defaultPageSize = Number(p.defaultPageSize ?? s.pagination.defaultPageSize);
        s.pagination.offsetParam = String(p.offsetParam ?? s.pagination.offsetParam);
        s.pagination.limitParam = String(p.limitParam ?? s.pagination.limitParam);
        s.pagination.offsetStart = Number(p.offsetStart ?? s.pagination.offsetStart);
        s.pagination.defaultLimit = Number(p.defaultLimit ?? s.pagination.defaultLimit);
      });
      err = '';
    } catch {
      err = 'Настройки RAW: некорректный JSON';
    }
  }

  function defaultSource(): ApiSource {
    const now = Date.now();
    return {
      id: uid(),
      name: 'New API',
      baseUrl: 'https://example.com',
      method: 'GET',
      path: '/endpoint',
      headersJson: '',
      queryJson: '',
      bodyJson: '',
      exampleRequest: '',
      auth: {
        mode: 'none',
        bearerToken: '',
        basicUsername: '',
        basicPassword: '',
        apiKeyName: 'X-API-KEY',
        apiKeyValue: '',
        apiKeyIn: 'header'
      },
      authTemplate: {
        name: '',
        type: 'custom',
        fields: []
      },
      pagination: {
        mode: 'none',
        pageParam: 'page',
        pageSizeParam: 'page_size',
        pageStart: 1,
        defaultPageSize: 50,
        offsetParam: 'offset',
        limitParam: 'limit',
        offsetStart: 0,
        defaultLimit: 50
      },
      db: {
        schema: '',
        table: '',
        rowIndex: 0,
        bindings: []
      },
      createdAt: now,
      updatedAt: now
    };
  }

  function normalizeSource(s: any): ApiSource {
    const d = defaultSource();
    const src = { ...d, ...(s || {}) };
    src.auth = { ...d.auth, ...(s?.auth || {}) };
    src.authTemplate = {
      ...d.authTemplate,
      ...(s?.authTemplate || {}),
      fields: Array.isArray(s?.authTemplate?.fields)
        ? s.authTemplate.fields.map((f: any) => ({
            key: String(f?.key || ''),
            value: String(f?.value || '')
          }))
        : []
    };
    src.pagination = { ...d.pagination, ...(s?.pagination || {}) };
    src.db = { ...d.db, ...(s?.db || {}) };
    src.db.bindings = Array.isArray(s?.db?.bindings)
      ? s.db.bindings.map((b: any) => ({
          id: String(b?.id || uid()),
          target: (b?.target || 'query') as BindingTarget,
          targetKey: String(b?.targetKey || ''),
          sourceColumn: String(b?.sourceColumn || '')
        }))
      : [];
    return src;
  }

  function fromDbConfigRow(row: any): ApiSource {
    const base = defaultSource();
    const storeIdRaw = row?.id ?? row?.api_config_id ?? row?.config_id ?? null;
    const storeIdNum = Number(String(storeIdRaw ?? '').trim());
    const toObj = (v: any): any => {
      if (v && typeof v === 'object') return v;
      if (typeof v === 'string') {
        try {
          const p = JSON.parse(v);
          return p && typeof p === 'object' ? p : {};
        } catch {
          return {};
        }
      }
      return {};
    };
    const mapping = toObj(row?.mapping_json);
    const legacySource = toObj(mapping?.source ?? mapping?.source_json ?? mapping?.config ?? mapping?.payload);
    const legacyRequest = toObj(mapping?.request ?? legacySource?.request ?? legacySource?.http ?? legacySource?.requestConfig);
    const auth = toObj(mapping?.auth ?? legacySource?.auth);
    const db = toObj(mapping?.db ?? legacySource?.db);
    const authTemplate = toObj(mapping?.authTemplate ?? legacySource?.authTemplate);
    const headersObj = toObj(
      row?.headers_json ??
      legacySource?.headers_json ??
      legacySource?.headersJson ??
      legacySource?.headers ??
      legacyRequest?.headers
    );
    const queryObj = toObj(
      row?.query_json ??
      legacySource?.query_json ??
      legacySource?.queryJson ??
      legacySource?.query ??
      legacySource?.params ??
      legacyRequest?.query ??
      legacyRequest?.params
    );
    const bodyObj = toObj(
      row?.body_json ??
      legacySource?.body_json ??
      legacySource?.bodyJson ??
      legacySource?.body ??
      legacySource?.data ??
      legacyRequest?.body ??
      legacyRequest?.data
    );
    const paginationObj = toObj(row?.pagination_json ?? legacySource?.pagination_json ?? legacySource?.pagination);
    const rawUrl = String(
      row?.url ??
      row?.full_url ??
      mapping?.url ??
      mapping?.full_url ??
      legacySource?.url ??
      legacySource?.fullUrl ??
      legacyRequest?.url ??
      ''
    ).trim();

    let derivedBaseUrl = '';
    let derivedPath = '';
    const derivedQueryObj: Record<string, string> = {};
    if (rawUrl) {
      try {
        const u = new URL(rawUrl);
        derivedBaseUrl = u.origin;
        derivedPath = u.pathname || '/';
        u.searchParams.forEach((v, k) => {
          derivedQueryObj[k] = v;
        });
      } catch {}
    }

    const finalQueryObj =
      Object.keys(queryObj || {}).length > 0 ? queryObj : derivedQueryObj;

    const finalMethod = String(
      row?.method ??
      legacySource?.method ??
      legacyRequest?.method ??
      'GET'
    ).toUpperCase();

    const finalBaseUrl = String(
      row?.base_url ??
      legacySource?.base_url ??
      legacySource?.baseUrl ??
      legacyRequest?.baseURL ??
      legacyRequest?.baseUrl ??
      derivedBaseUrl ??
      ''
    );

    const finalPath = String(
      row?.path ??
      legacySource?.path ??
      legacyRequest?.path ??
      derivedPath ??
      ''
    );

    return normalizeSource({
      ...base,
      id: uid(),
      storeId: Number.isFinite(storeIdNum) && storeIdNum > 0 ? Math.trunc(storeIdNum) : undefined,
      name: String(row?.api_name || legacySource?.name || 'API'),
      method: toHttpMethod(finalMethod),
      baseUrl: finalBaseUrl,
      path: finalPath,
      headersJson: JSON.stringify(headersObj, null, 2),
      queryJson: JSON.stringify(finalQueryObj, null, 2),
      bodyJson: JSON.stringify(bodyObj, null, 2),
      pagination: { ...base.pagination, ...paginationObj },
      auth: { ...base.auth, ...auth, apiKeyIn: toApiKeyIn(String(auth?.apiKeyIn || base.auth.apiKeyIn)) },
      db: { ...base.db, ...db, bindings: Array.isArray(db?.bindings) ? db.bindings : [] },
      authTemplate: {
        ...base.authTemplate,
        ...authTemplate,
        fields: Array.isArray(authTemplate?.fields) ? authTemplate.fields : []
      },
      exampleRequest: String(mapping?.exampleRequest || legacySource?.exampleRequest || ''),
      createdAt: Date.parse(String(row?.updated_at || '')) || Date.now(),
      updatedAt: Date.parse(String(row?.updated_at || '')) || Date.now()
    });
  }

  function parseJsonSafeObject(text: string) {
    if (!String(text || '').trim()) return {};
    try {
      const parsed = JSON.parse(text);
      return parsed && typeof parsed === 'object' ? parsed : {};
    } catch {
      return {};
    }
  }

  function toDbConfigPayload(source: ApiSource) {
    return {
      id: source.storeId || undefined,
      api_name: source.name,
      method: source.method,
      base_url: source.baseUrl,
      path: source.path,
      headers_json: parseJsonSafeObject(source.headersJson),
      query_json: parseJsonSafeObject(source.queryJson),
      body_json: parseJsonSafeObject(source.bodyJson),
      pagination_json: source.pagination || {},
      target_schema: '',
      target_table: '',
      mapping_json: {
        auth: source.auth,
        db: source.db,
        authTemplate: source.authTemplate,
        exampleRequest: source.exampleRequest
      },
      description: '',
      is_active: true
    };
  }

  function toHttpMethod(v: string): HttpMethod {
    return v === 'GET' || v === 'POST' || v === 'PUT' || v === 'PATCH' || v === 'DELETE' ? v : 'GET';
  }

  function toAuthMode(v: string): AuthMode {
    return v === 'none' || v === 'bearer' || v === 'basic' || v === 'apiKey' ? v : 'none';
  }

  function toPaginationMode(v: string): PaginationMode {
    return v === 'none' || v === 'page' || v === 'offset' ? v : 'none';
  }

  function toBindingTarget(v: string): BindingTarget {
    return v === 'query' || v === 'header' || v === 'body' ? v : 'query';
  }

  function toApiKeyIn(v: string): 'header' | 'query' {
    return v === 'query' ? 'query' : 'header';
  }

  let saveSourceDebounce: any = null;

  async function loadAll() {
    err = '';
    try {
      try {
        const cfg = await apiJson<{ effective?: any }>(`${apiBase}/settings/effective`, { headers: headers() });
        const eff = cfg?.effective || {};
        const s = String(eff?.api_configs_schema || '').trim();
        const t = String(eff?.api_configs_table || '').trim();
        if (s) api_storage_schema = s;
        if (t) api_storage_table = t;
        api_storage_pick_value = `${api_storage_schema}.${api_storage_table}`;
      } catch {}
      const j = await apiJson<{ api_configs: any[] }>(`${apiBase}/api-configs`, { headers: headers() });
      const rows = Array.isArray(j?.api_configs) ? j.api_configs : [];
      sources = rows.map((r) => fromDbConfigRow(r));
      history = [];
      if (!sources.length) {
        const draft = defaultSource();
        sources = [draft];
        selectedId = draft.id;
        apiNameDraft = draft.name;
      } else {
        if (!selectedId && sources.length) selectedId = sources[0].id;
        if (selectedId && !sources.some((s) => s.id === selectedId)) selectedId = sources[0]?.id || null;
      }
      loadAuthTemplates();
    } catch (e: any) {
      err = e?.message ?? String(e);
      sources = [];
      selectedId = null;
    } finally {
      // no-op
    }
  }

  async function checkApiStorageTable(schema: string, table: string) {
    try {
      const j = await apiJson<{ columns: Array<{ name: string; type: string }> }>(
        `${apiBase}/columns?schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`
      );
      const cols = Array.isArray(j?.columns) ? j.columns : [];
      if (!cols.length) {
        err = `Таблица ${schema}.${table} не найдена или пуста.`;
        return false;
      }
      const map = new Map(cols.map((c) => [String(c.name || '').toLowerCase(), normalizeTypeName(c.type)]));
      for (const need of API_STORAGE_REQUIRED_COLUMNS) {
        const actual = map.get(need.name);
        if (!actual || !need.types.some((t) => actual.includes(t))) {
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

  async function applyApiStorageChoice() {
    if (!api_storage_pick_value) return;
    const [schema, table] = api_storage_pick_value.split('.');
    if (!schema || !table) return;
    const ok = await checkApiStorageTable(schema, table);
    if (!ok) return;
    try {
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
      await loadAll();
    } catch (e: any) {
      err = e?.message ?? String(e);
    }
  }

  async function persistSelectedNow() {
    const src = getSelected();
    if (!src) return;
    try {
      const payload = toDbConfigPayload(src);
      const r = await apiJson<{ id?: number }>(`${apiBase}/api-configs/upsert`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify(payload)
      });
      const nextId = Number(r?.id || 0);
      if (Number.isFinite(nextId) && nextId > 0) {
        sources = sources.map((s) => (s.id === src.id ? { ...s, storeId: nextId, updatedAt: Date.now() } : s));
      }
    } catch (e: any) {
      err = e?.message ?? String(e);
    }
  }

  async function applySelectedSource(id: string) {
    selectedId = id;
    const src = sources.find((s) => s.id === id);
    apiNameDraft = String(src?.name || '');
    previewSyncError = '';
    previewApplyMessage = '';
    await tick();
    syncEditorsFromSelected(true);
    void loadDbPreview();
  }

  async function onAddSourceClick() {
    err = '';
    const name = String(apiNameDraft || '').trim();
    if (!name) {
      err = 'Укажи название API';
      return;
    }
    const s = defaultSource();
    s.name = name;
    sources = [s, ...sources];
    selectedId = s.id;
    await persistSelectedNow();
  }

  async function onSaveSourceClick() {
    err = '';
    if (!selectedId) {
      err = 'Выбери API для сохранения';
      return;
    }
    const name = String(apiNameDraft || '').trim();
    if (!name) {
      err = 'Укажи название API';
      return;
    }
    mutateSelected((s) => (s.name = name));
    await persistSelectedNow();
  }

  function saveSources() {
    if (saveSourceDebounce) clearTimeout(saveSourceDebounce);
    saveSourceDebounce = setTimeout(() => {
      persistSelectedNow();
    }, 300);
  }

  function saveHistory() {
    // local storage disabled
  }

  function getSelected(): ApiSource | null {
    return sources.find((s) => s.id === selectedId) || null;
  }

  $: selected = getSelected();
  $: if (!selected && sources.length === 0) {
    const draft = defaultSource();
    sources = [draft];
    selectedId = draft.id;
    apiNameDraft = draft.name;
  }
  $: if (!selected && sources.length > 0) {
    selectedId = sources[0].id;
  }
  $: if (selected && selected.id !== lastNameDraftSourceId) {
    apiNameDraft = selected.name;
    lastNameDraftSourceId = selected.id;
  }

  function mutateSelected(mutator: (next: ApiSource) => void) {
    if (!selectedId) return;
    sources = sources.map((s) => {
      if (s.id !== selectedId) return s;
      const next: ApiSource = {
        ...s,
        auth: { ...s.auth },
        authTemplate: { ...s.authTemplate, fields: [...(s.authTemplate?.fields || [])] },
        pagination: { ...s.pagination },
        db: { ...s.db, bindings: [...s.db.bindings] },
        updatedAt: Date.now()
      };
      mutator(next);
      return next;
    });
    saveSources();
    generatedApiPreview = buildGeneratedPreview(getSelected());
    if (!editingPreview) previewDraft = generatedApiPreview;
  }

  function totalPages() {
    return Math.max(1, Math.ceil(history.length / pageSize));
  }

  function pagedHistory() {
    const p = Math.min(Math.max(page, 1), totalPages());
    const start = (p - 1) * pageSize;
    return history.slice(start, start + pageSize);
  }

  function parseJsonOrEmpty(s: string): any {
    if (!s.trim()) return {};
    return JSON.parse(s);
  }

  function normalizeUrl(baseUrl: string, path: string, queryObj: Record<string, any>) {
    const p = path.startsWith('/') ? path : `/${path}`;
    const url = new URL(`${baseUrl.replace(/\/$/, '')}${p}`);
    for (const [k, v] of Object.entries(queryObj || {})) url.searchParams.set(k, String(v));
    return url.toString();
  }

  async function loadDbPreview() {
    dbPreviewError = '';
    dbPreviewRows = [];
    dbPreviewColumns = [];

    if (!selected?.db?.schema || !selected?.db?.table) return;

    dbPreviewLoading = true;
    try {
      const key = `${selected.db.schema}.${selected.db.table}`;
      const j = await apiJson<{ rows: any[] }>(
        `${apiBase}/preview?schema=${encodeURIComponent(selected.db.schema)}&table=${encodeURIComponent(selected.db.table)}&limit=5`
      );
      dbPreviewRows = j.rows || [];
      const first = dbPreviewRows[0] || {};
      dbPreviewColumns = Object.keys(first).filter((k) => k !== '__ctid');
      dbLoadedKey = key;
      generatedApiPreview = buildGeneratedPreview(getSelected());
      if (!editingPreview) previewDraft = generatedApiPreview;
    } catch (e: any) {
      dbPreviewError = e?.message ?? String(e);
    } finally {
      dbPreviewLoading = false;
    }
  }

  function ensureTableSelection() {
    if (!selected) return;
    if (selected.db.schema && selected.db.table) return;
    if (!existingTables.length) return;
    const first = existingTables[0];
    mutateSelected((s) => {
      s.db.schema = first.schema_name;
      s.db.table = first.table_name;
      s.db.rowIndex = 0;
    });
  }

  function newSource() {
    const s = defaultSource();
    sources = [s, ...sources];
    selectedId = s.id;
    apiNameDraft = s.name;
    saveSources();
  }

  async function deleteSelected() {
    if (!selectedId) return;
    await deleteSourceById(selectedId);
  }

  async function deleteSourceById(id: string) {
    const target = sources.find((s) => s.id === id);
    if (!target) return;
    if (target.storeId) {
      try {
        await apiJson(`${apiBase}/api-configs/delete`, {
          method: 'POST',
          headers: headers(),
          body: JSON.stringify({ id: target.storeId })
        });
      } catch (e: any) {
        err = e?.message ?? String(e);
        return;
      }
    }
    const wasSelected = selectedId === id;
    sources = sources.filter((s) => s.id !== id);
    if (wasSelected) {
      selectedId = sources[0]?.id ?? null;
    } else if (selectedId && !sources.some((s) => s.id === selectedId)) {
      selectedId = sources[0]?.id ?? null;
    }
  }

  function addBinding() {
    mutateSelected((s) => {
      s.db.bindings = [
        ...s.db.bindings,
        { id: uid(), target: 'query', targetKey: '', sourceColumn: dbPreviewColumns[0] || '' }
      ];
    });
  }

  function removeBinding(id: string) {
    mutateSelected((s) => {
      s.db.bindings = s.db.bindings.filter((b) => b.id !== id);
    });
  }

  function basicAuthHeader(username: string, password: string): string {
    return `Basic ${btoa(`${username}:${password}`)}`;
  }

  function applyDbBindings(
    headersObj: Record<string, any>,
    queryObj: Record<string, any>,
    bodyObj: any,
    source: ApiSource,
    row: Record<string, any>
  ) {
    for (const b of source.db.bindings) {
      if (!b.targetKey || !b.sourceColumn) continue;
      const value = row[b.sourceColumn];
      if (b.target === 'header') headersObj[b.targetKey] = value;
      if (b.target === 'query') queryObj[b.targetKey] = value;
      if (b.target === 'body') {
        if (!bodyObj || typeof bodyObj !== 'object' || Array.isArray(bodyObj)) bodyObj = {};
        bodyObj[b.targetKey] = value;
      }
    }
    return bodyObj;
  }

  type PreparedRequest = {
    method: HttpMethod;
    url: string;
    headersObj: Record<string, any>;
    sendBody: string | undefined;
  };

  function prepareRequest(source: ApiSource, rowForBindings?: Record<string, any>): PreparedRequest {
    let headersObj: Record<string, any> = {};
    let queryObj: Record<string, any> = {};
    let bodyObj: any = undefined;

    try {
      headersObj = parseJsonOrEmpty(source.headersJson);
      queryObj = parseJsonOrEmpty(source.queryJson);
      bodyObj = source.bodyJson.trim() ? JSON.parse(source.bodyJson) : {};
    } catch {
      throw new Error('Некорректный JSON в headers/query/body');
    }

    if (source.auth.mode === 'bearer' && source.auth.bearerToken.trim()) {
      headersObj.Authorization = `Bearer ${source.auth.bearerToken.trim()}`;
    }
    if (source.auth.mode === 'basic') {
      headersObj.Authorization = basicAuthHeader(source.auth.basicUsername, source.auth.basicPassword);
    }
    if (source.auth.mode === 'apiKey' && source.auth.apiKeyName.trim()) {
      if (source.auth.apiKeyIn === 'header') headersObj[source.auth.apiKeyName.trim()] = source.auth.apiKeyValue;
      if (source.auth.apiKeyIn === 'query') queryObj[source.auth.apiKeyName.trim()] = source.auth.apiKeyValue;
    }

    for (const f of source.authTemplate?.fields || []) {
      const k = String(f?.key || '').trim();
      if (!k) continue;
      const v = String(f?.value ?? '');
      if (String(source.authTemplate?.type || 'custom') === 'query') queryObj[k] = v;
      else headersObj[k] = v;
    }

    if (source.pagination.mode === 'page') {
      queryObj[source.pagination.pageParam] = source.pagination.pageStart;
      queryObj[source.pagination.pageSizeParam] = source.pagination.defaultPageSize;
    }
    if (source.pagination.mode === 'offset') {
      queryObj[source.pagination.offsetParam] = source.pagination.offsetStart;
      queryObj[source.pagination.limitParam] = source.pagination.defaultLimit;
    }

    if (rowForBindings && source.db.bindings.length) {
      bodyObj = applyDbBindings(headersObj, queryObj, bodyObj, source, rowForBindings);
    }

    let url = '';
    try {
      url = normalizeUrl(source.baseUrl, source.path, queryObj);
    } catch {
      throw new Error('Некорректный URL или query-параметры');
    }

    const sendBody = source.method === 'GET' || source.method === 'DELETE'
      ? undefined
      : JSON.stringify(bodyObj ?? {});

    return {
      method: source.method,
      url,
      headersObj,
      sendBody
    };
  }

  function buildGeneratedPreview(source: ApiSource | null): string {
    if (!source) return '';

    let row: Record<string, any> | undefined = undefined;
    if (source.db.bindings.length > 0 && dbPreviewRows.length > 0) {
      const index = Math.max(0, Math.min(source.db.rowIndex, dbPreviewRows.length - 1));
      row = dbPreviewRows[index];
    }

    try {
      const req = prepareRequest(source, row);
      return [
        `${req.method} ${req.url}`,
        '',
        'Headers:',
        JSON.stringify(req.headersObj, null, 2),
        '',
        'Body:',
        req.sendBody ?? '(empty)'
      ].join('\n');
    } catch (e: any) {
      return `Ошибка предпросмотра: ${e?.message ?? String(e)}`;
    }
  }

  function unquoteToken(v: string): string {
    if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
      return v.slice(1, -1);
    }
    return v;
  }

  function tryApplyCurlPreview(text: string): boolean {
    const t = text.trim();
    if (!t.toLowerCase().startsWith('curl ')) return false;

    const tokens = t.match(/"[^"]*"|'[^']*'|\S+/g) || [];
    if (!tokens.length) return false;

    let method: HttpMethod = 'GET';
    let rawUrl = '';
    let body = '';
    const headersObj: Record<string, string> = {};

    for (let i = 1; i < tokens.length; i += 1) {
      const token = tokens[i];
      if (token === '-X' || token === '--request') {
        const next = unquoteToken(tokens[i + 1] || '');
        method = toHttpMethod(next.toUpperCase());
        i += 1;
        continue;
      }
      if (token === '--url') {
        rawUrl = unquoteToken(tokens[i + 1] || '');
        i += 1;
        continue;
      }
      if (token === '-H' || token === '--header') {
        const line = unquoteToken(tokens[i + 1] || '');
        const idx = line.indexOf(':');
        if (idx > 0) {
          const k = line.slice(0, idx).trim();
          const v = line.slice(idx + 1).trim();
          if (k) headersObj[k] = v;
        }
        i += 1;
        continue;
      }
      if (token === '-d' || token === '--data' || token === '--data-raw' || token === '--data-binary') {
        body = unquoteToken(tokens[i + 1] || '');
        i += 1;
        continue;
      }
      if (!rawUrl && /^https?:\/\//i.test(token)) {
        rawUrl = unquoteToken(token);
      }
    }

    if (!rawUrl) {
      previewSyncError = 'В curl не найден URL';
      return true;
    }

    let parsedUrl: URL;
    try {
      parsedUrl = new URL(rawUrl);
    } catch {
      previewSyncError = 'Некорректный URL в curl';
      return true;
    }

    const queryObj: Record<string, string> = {};
    parsedUrl.searchParams.forEach((v, k) => {
      queryObj[k] = v;
    });

    let bodyJsonText = '';
    if (body) {
      try {
        bodyJsonText = JSON.stringify(JSON.parse(body), null, 2);
      } catch {
        bodyJsonText = body;
      }
    }

    previewSyncError = '';
    mutateSelected((s) => {
      s.method = method;
      s.baseUrl = parsedUrl.origin;
      s.path = parsedUrl.pathname || '/';
      s.queryJson = Object.keys(queryObj).length ? JSON.stringify(queryObj, null, 2) : '';
      s.headersJson = Object.keys(headersObj).length ? JSON.stringify(headersObj, null, 2) : '';
      s.bodyJson = bodyJsonText;
    });
    return true;
  }

  function extractBodyCandidate(text: string): { found: boolean; body: string } {
    const t = text || '';
    const lines = t.split(/\r?\n/);
    const bodyIdx = lines.findIndex((x) => /^body\s*:\s*$/i.test(x.trim()));
    if (bodyIdx >= 0) {
      const raw = lines.slice(bodyIdx + 1).join('\n').trim();
      return { found: true, body: raw === '(empty)' ? '' : raw };
    }

    // Generic fallback: if there is a JSON-looking block anywhere, treat it as body.
    const jsonStartIdx = lines.findIndex((x) => {
      const s = x.trim();
      return s.startsWith('{') || s.startsWith('[');
    });
    if (jsonStartIdx >= 0) {
      const raw = lines.slice(jsonStartIdx).join('\n').trim();
      return { found: true, body: raw };
    }

    const trimmed = t.trim();
    if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
      return { found: true, body: trimmed };
    }
    return { found: false, body: '' };
  }

  function syncBodyFromPreviewText(text: string) {
    const c = extractBodyCandidate(text);
    if (!c.found) return;
    if (!selectedId) return;
    if ((selected?.bodyJson || '') === c.body) return;
    mutateSelected((s) => {
      s.bodyJson = c.body;
    });
  }

  function applyGeneratedPreviewEdit(input: string) {
    const text = input || '';
    const trimmed = text.trim();
    if (!trimmed) {
      previewSyncError = '';
      return;
    }

    // Always keep Body JSON in sync, even if full preview parse is incomplete.
    syncBodyFromPreviewText(text);

    if (tryApplyCurlPreview(text)) return;

    // Body-only mode for fast editing: user can paste/edit only JSON body.
    if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
      let bodyJsonText = '';
      try {
        bodyJsonText = JSON.stringify(JSON.parse(trimmed), null, 2);
      } catch {
        bodyJsonText = trimmed;
      }
      previewSyncError = '';
      mutateSelected((s) => {
        s.bodyJson = bodyJsonText;
      });
      return;
    }

    const lines = text.split(/\r?\n/);
    const first = (lines[0] || '').trim();
    const m = first.match(/^(GET|POST|PUT|PATCH|DELETE)\s+(\S+)$/i);
    if (!m) {
      previewSyncError = 'Первая строка должна быть в формате: METHOD URL';
      return;
    }

    let parsedUrl: URL;
    try {
      parsedUrl = new URL(m[2]);
    } catch {
      previewSyncError = 'Некорректный URL в первой строке';
      return;
    }

    const headersIdx = lines.findIndex((x) => x.trim() === 'Headers:');
    const bodyIdx = lines.findIndex((x) => x.trim() === 'Body:');

    let headersObj: Record<string, any> = {};
    let bodyText = '';
    let bodyJsonText = '';

    if (headersIdx >= 0 && bodyIdx > headersIdx) {
      const headersText = lines.slice(headersIdx + 1, bodyIdx).join('\n').trim();
      const bodyTextRaw = lines.slice(bodyIdx + 1).join('\n').trim();
      bodyText = bodyTextRaw === '(empty)' ? '' : bodyTextRaw;

      try {
        headersObj = headersText ? JSON.parse(headersText) : {};
      } catch {
        previewSyncError = 'Headers должен быть корректным JSON-объектом';
        return;
      }
      if (!headersObj || typeof headersObj !== 'object' || Array.isArray(headersObj)) {
        previewSyncError = 'Headers должен быть JSON-объектом';
        return;
      }
    } else {
      // Fallback: HTTP-style format after first line:
      // Header-Name: value
      //
      // { ...json body... }
      const rest = lines.slice(1);
      const headerLines: string[] = [];
      const bodyLines: string[] = [];
      let inBody = false;
      for (const line of rest) {
        const trimmed = line.trim();
        if (!inBody) {
          if (!trimmed && headerLines.length > 0) {
            inBody = true;
            continue;
          }
          const colon = line.indexOf(':');
          const looksLikeHeader = colon > 0 && !trimmed.startsWith('{') && !trimmed.startsWith('[');
          if (looksLikeHeader) {
            headerLines.push(line);
            continue;
          }
          if (trimmed) {
            inBody = true;
            bodyLines.push(line);
          }
          continue;
        }
        bodyLines.push(line);
      }

      for (const h of headerLines) {
        const idx = h.indexOf(':');
        if (idx <= 0) continue;
        const k = h.slice(0, idx).trim();
        const v = h.slice(idx + 1).trim();
        if (k) headersObj[k] = v;
      }
      bodyText = bodyLines.join('\n').trim();
    }

    if (bodyText) {
      try {
        bodyJsonText = JSON.stringify(JSON.parse(bodyText), null, 2);
      } catch {
        // Keep raw text so user can continue editing and still sync other fields.
        bodyJsonText = bodyText;
      }
    }

    const queryObj: Record<string, string> = {};
    parsedUrl.searchParams.forEach((v, k) => {
      queryObj[k] = v;
    });

    previewSyncError = '';
    mutateSelected((s) => {
      s.method = toHttpMethod(m[1].toUpperCase());
      s.baseUrl = parsedUrl.origin;
      s.path = parsedUrl.pathname || '/';
      s.queryJson = Object.keys(queryObj).length ? JSON.stringify(queryObj, null, 2) : '';
      s.headersJson = Object.keys(headersObj).length ? JSON.stringify(headersObj, null, 2) : '';
      s.bodyJson = bodyJsonText;
    });
  }

  function applyPreviewNow() {
    previewApplyMessage = '';
    applyGeneratedPreviewEdit(previewDraft);
    if (!previewSyncError) {
      previewApplyMessage = 'Применено в параметры.';
    }
  }

  async function sendTest() {
    err = '';
    respStatus = 0;
    respHeaders = {};
    respText = '';
    if (!selected) return;

    let rowForBindings: Record<string, any> | undefined = undefined;

    const needsDb = !!(selected.db.schema && selected.db.table && selected.db.bindings.length);
    if (needsDb) {
      const key = `${selected.db.schema}.${selected.db.table}`;
      if (dbLoadedKey !== key || dbPreviewRows.length === 0) {
        await loadDbPreview();
      }
      if (!dbPreviewRows.length) {
        err = dbPreviewError || 'Нет данных предпросмотра выбранной таблицы для параметров из БД';
        return;
      }
      const index = Math.max(0, Math.min(selected.db.rowIndex, dbPreviewRows.length - 1));
      rowForBindings = dbPreviewRows[index];
    }

    let prepared: PreparedRequest;
    try {
      prepared = prepareRequest(selected, rowForBindings);
    } catch (e: any) {
      err = e?.message ?? String(e);
      return;
    }

    const item: HistoryItem = {
      ts: Date.now(),
      sourceId: selected.id,
      method: prepared.method,
      url: prepared.url,
      requestHeadersJson: JSON.stringify(prepared.headersObj, null, 2),
      requestBodyJson: prepared.sendBody || ''
    };

    sending = true;
    const t0 = performance.now();
    try {
      const res = await fetch(prepared.url, {
        method: prepared.method,
        headers: { 'Content-Type': 'application/json', ...prepared.headersObj },
        body: prepared.sendBody
      });

      item.status = res.status;
      item.ms = Math.round(performance.now() - t0);

      const h: Record<string, string> = {};
      res.headers.forEach((v, k) => (h[k] = v));
      respHeaders = h;

      respStatus = res.status;
      const text = await res.text();
      respText = text;
      item.responseText = text;

      history = [item, ...history];
      saveHistory();
    } catch (e: any) {
      err = e?.message ?? String(e);
      item.ms = Math.round(performance.now() - t0);
      history = [item, ...history];
      saveHistory();
    } finally {
      sending = false;
    }
  }
  function clearHistory() {
    history = [];
    saveHistory();
    page = 1;
  }

  function autosizeTextarea(el: HTMLTextAreaElement | null) {
    if (!el) return;
    el.style.height = 'auto';
    el.style.height = `${el.scrollHeight}px`;
  }

  function autosizeCompareTextareas() {
    autosizeTextarea(exampleRequestEl);
    autosizeTextarea(generatedPreviewEl);
  }

  function syncAuthRawHeight() {
    if (!authFieldsEl || !authRawEl) return;
    const min = Math.max(authFieldsEl.offsetHeight, 120);
    authRawEl.style.minHeight = `${min}px`;
    autosizeTextarea(authRawEl);
    if (authRawEl.offsetHeight < min) authRawEl.style.height = `${min}px`;
  }

  void loadAll();
  $: syncEditorsFromSelected();
  $: if (selected) ensureTableSelection();
  $: if (selectedId) generatedApiPreview = buildGeneratedPreview(selected);
  $: if (selected) settingsRawDraft = JSON.stringify({ pagination: selected.pagination || {} }, null, 2);
  $: if (!editingPreview) previewDraft = generatedApiPreview;
  $: selectedId, tick().then(autosizeCompareTextareas);
  $: previewDraft, tick().then(autosizeCompareTextareas);
  $: selected?.auth?.mode, tick().then(syncAuthRawHeight);
  $: authRawDraft, tick().then(syncAuthRawHeight);
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

  <div class="layout">
    <aside class="aside saved-aside">
      <div class="aside-head">
        <div class="aside-title">Сохраненные API</div>
      </div>
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
          <button on:click={applyApiStorageChoice} disabled={!api_storage_pick_value}>Подключить</button>
        </div>
      {/if}
      <div class="template-controls">
        <input class="template-name" bind:value={apiNameDraft} placeholder="Название API" />
        <div class="saved-inline-actions">
          <button on:click={onAddSourceClick}>Добавить</button>
          <button on:click={onSaveSourceClick} disabled={!selectedId}>Сохранить</button>
        </div>
      </div>
      {#if sources.length === 0}
        <div class="hint">Пока нет ни одного.</div>
      {:else}
        <div class="list api-list">
          {#each sources as s (s.id)}
            <div class="row-item" class:activeitem={s.id === selectedId}>
              <button class="item-button" on:click={() => applySelectedSource(s.id)}>
                <div class="row-name">{s.name}</div>
                <div class="row-meta">{s.method} {s.baseUrl}{s.path}</div>
              </button>
              <div class="row-actions">
                <button class="danger icon-btn" on:click|stopPropagation={() => deleteSourceById(s.id)} title="Удалить API">x</button>
              </div>
            </div>
          {/each}
        </div>
      {/if}
    </aside>

    <div class="main">
      <div class="card">
        <h3 style="margin:0;">Конструктор API</h3>
        <div class="request-top" style="margin-top:10px;">
          <input
            placeholder="Название API"
            value={selected?.name || ''}
            readonly
          />
          <div class="method-url">
            <select
              value={selected?.method || 'GET'}
              on:change={(e) => mutateSelected((s) => (s.method = toHttpMethod(e.currentTarget.value)))}
            >
              <option value="GET">Метод: GET</option>
              <option value="POST">Метод: POST</option>
              <option value="PUT">Метод: PUT</option>
              <option value="PATCH">Метод: PATCH</option>
              <option value="DELETE">Метод: DELETE</option>
            </select>
            <input
              value={urlInput}
              on:input={(e) => (urlInput = e.currentTarget.value)}
              on:blur={() => applyUrlInputRaw(urlInput)}
              placeholder="Строка подключения (URL / curl ...)"
            />
            <button on:click={() => applyUrlInputRaw(urlInput)}>Разобрать cURL</button>
          </div>
        </div>
      </div>
    </div>

    <aside class="aside compare-aside">
      <div class="aside-title">Настройка API</div>
      {#if !selected}
        <div class="hint">Выберите API слева.</div>
      {:else}
        <div class="compare-fields">
          <label>
            Пример API (вставьте вручную)
            <textarea
              bind:this={exampleRequestEl}
              value={selected.exampleRequest}
              on:input={async (e) => {
                mutateSelected((s) => (s.exampleRequest = e.currentTarget.value));
                await tick();
                autosizeCompareTextareas();
              }}
              placeholder="Вставьте пример запроса от документации или коллег"
            ></textarea>
          </label>

          <label>
            Предпросмотр моего API (авто)
            <textarea
              bind:this={generatedPreviewEl}
              class="preview-readonly"
              bind:value={previewDraft}
              on:focus={() => (editingPreview = true)}
              on:blur={() => {
                editingPreview = false;
                generatedApiPreview = buildGeneratedPreview(getSelected());
                previewDraft = generatedApiPreview;
              }}
              on:input={async (e) => {
                previewDraft = e.currentTarget.value;
                applyGeneratedPreviewEdit(previewDraft);
                await tick();
                autosizeCompareTextareas();
              }}
              placeholder="Можно вставить METHOD URL + Headers/Body, body-only JSON или curl ..."
            ></textarea>
          </label>
          <div class="inline-actions">
            <button on:click={applyPreviewNow}>Разобрать в поля</button>
            {#if previewApplyMessage}
              <span class="muted">{previewApplyMessage}</span>
            {/if}
          </div>
          {#if previewSyncError}
            <p class="error">{previewSyncError}</p>
          {/if}
        </div>
      {/if}
    </aside>
  </div>
</section>

<style>
  .panel { background:#fff; border:1px solid #e6eaf2; border-radius:18px; padding:14px; box-shadow:0 6px 20px rgba(15,23,42,.05); margin-top:12px; }
  .panel-head { display:flex; align-items:center; justify-content:space-between; gap:12px; }
  .panel-head h2 { margin:0; font-size:18px; }

  .hint { margin:10px 0 0; color:#64748b; font-size:13px; }
  .error { margin:8px 0; color:#b91c1c; font-size:13px; }

  .layout { display:grid; grid-template-columns: 320px 1fr 360px; gap:12px; margin-top:12px; align-items:start; }
  @media (max-width: 1300px) { .layout { grid-template-columns: 320px 1fr; } }
  @media (max-width: 1100px) { .layout { grid-template-columns: 1fr; } }
  .compare-aside { order: 1; }
  .main { order: 2; min-width:0; }
  .saved-aside { order: 3; }

  .aside { border:1px solid #e6eaf2; border-radius:16px; padding:12px; background:#f8fafc; }
  .aside-head { display:flex; align-items:center; justify-content:space-between; gap:8px; margin-bottom:8px; }
  .aside-title { font-weight:700; font-size:14px; line-height:1.3; margin-bottom:0; }
  .compare-aside { position: sticky; top: 12px; }
  .storage-meta { margin-top:0; margin-bottom:8px; display:flex; align-items:center; gap:6px; font-size:12px; color:#64748b; }
  .link-btn { border:0; background:transparent; color:#0f172a; padding:0; text-decoration:underline; font-size:12px; font-weight:500; }
  .storage-picker { display:flex; gap:8px; align-items:center; margin-bottom:8px; }
  .storage-picker select { flex:1; min-width:0; }
  .template-controls { margin-bottom:8px; display:grid; grid-template-columns:1fr; gap:8px; }
  .template-name { width:100%; box-sizing:border-box; }
  .saved-inline-actions { display:grid; grid-template-columns:1fr 1fr; gap:8px; }
  .saved-inline-actions button { width:100%; }
  .compare-fields { display:flex; flex-direction:column; gap:10px; }
  .compare-fields textarea { overflow:hidden; resize:none; }
  .list { display:flex; flex-direction:column; gap:8px; }
  .row-item { display:grid; grid-template-columns: 1fr auto; gap:8px; align-items:center; border:1px solid #e6eaf2; border-radius:14px; background:#fff; padding:8px 10px; }
  .row-actions { display:flex; align-items:center; justify-content:flex-end; min-width:54px; }
  .item-button { text-align:left; border:0; background:transparent; padding:0; color:inherit; }
  .row-name { font-weight:400; font-size:13px; line-height:1.25; word-break:break-word; }
  .row-meta { font-size:12px; color:#64748b; margin-top:4px; word-break: break-word; }
  .api-list .row-item { background:#0f172a; border-color:#0f172a; color:#fff; }
  .api-list .row-meta { color:#cbd5e1; }
  .api-list .activeitem { background:#fff; border-color:#e6eaf2; color:#0f172a; }
  .api-list .activeitem .row-name { font-size:15px; font-weight:600; letter-spacing:.01em; }
  .api-list .activeitem .row-name::before { content:'●'; margin-right:8px; font-size:11px; color:#0f172a; vertical-align:middle; }
  .api-list .activeitem .row-meta { color:#64748b; }

  .card { border:1px solid #e6eaf2; border-radius:16px; padding:12px; background:#fff; margin-bottom:12px; }
  .subcard { margin-top:10px; border:1px dashed #e6eaf2; border-radius:14px; padding:10px; }
  .subcard h3 { margin:0 0 8px 0; font-size:14px; }

  .grid { display:grid; grid-template-columns: 1fr 1fr; gap:10px; }
  @media (max-width: 1100px) { .grid { grid-template-columns: 1fr; } }
  .request-top { display:flex; flex-direction:column; gap:10px; }
  .method-url { display:grid; grid-template-columns: 180px 1fr 180px; gap:10px; align-items:center; }
  .method-url select, .method-url input, .method-url button { height:42px; }
  @media (max-width: 1100px) { .method-url { grid-template-columns: 1fr; } }
  .auth-split { display:grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap:12px; align-items:start; }
  .auth-left, .auth-right { min-width: 0; }
  .auth-top { display:grid; grid-template-columns: 1fr minmax(170px, 26%) 44px; gap:10px; align-items:center; }
  .auth-top .quarter { width:100%; min-width:0; max-width:none; }
  .auth-top select { width:100%; }
  .auth-right-top { display:grid; grid-template-columns: 1fr minmax(170px, 26%) 44px; gap:10px; align-items:center; margin-bottom:10px; }
  .auth-right-controls { display:flex; flex-direction:column; gap:8px; margin-bottom:10px; }
  .icon-btn { width:44px; min-width:44px; padding:10px 0; text-transform:uppercase; border-color:transparent; background:transparent; color:#b91c1c; }
  .danger.icon-btn { border-color:transparent; background:transparent; color:#b91c1c; }
  .api-list .icon-btn { color:#fff; width:34px; min-width:34px; padding:6px 0; font-size:14px; }
  .api-list .activeitem .icon-btn { color:#b91c1c; }
  .auth-name-input { min-width:0; }
  .auth-fields { display:flex; flex-direction:column; gap:10px; margin-top:10px; }
  .auth-fields input, .auth-right textarea { width:100%; }
  .auth-rows { display:flex; flex-direction:column; gap:8px; }
  .auth-row { display:grid; grid-template-columns: 25% 1fr auto; gap:8px; align-items:center; }
  .auth-row .key-col { min-width:0; }
  .auth-row .val-col { min-width:0; }
  .auth-right textarea { resize:none; overflow:hidden; max-width:100%; }
  .auth-right textarea.invalid { border-color:#ef4444; background:#fff5f5; }
  @media (max-width: 1400px) { .auth-top, .auth-right-top { grid-template-columns: 1fr; } .icon-btn { width:100%; min-width:0; } }
  @media (max-width: 1100px) { .auth-split { grid-template-columns: 1fr; } }

  label { display:flex; flex-direction:column; gap:6px; font-size:13px; }
  input, select, textarea { border-radius:14px; border:1px solid #e6eaf2; padding:10px 12px; outline:none; background:#fff; box-sizing:border-box; }
  textarea { min-height: 90px; resize: vertical; }
  .preview-readonly { background:#f8fafc; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
  .wide { grid-column: 1 / -1; }

  .inline-actions { display:flex; gap:8px; align-items:end; flex-wrap:wrap; }
  .bindings-head { margin-top:10px; display:flex; align-items:center; justify-content:space-between; gap:10px; }
  .bindings { margin-top:8px; display:flex; flex-direction:column; gap:8px; }
  .binding-row { display:grid; grid-template-columns: 140px 1fr 1fr auto; gap:8px; }
  @media (max-width: 1100px) { .binding-row { grid-template-columns: 1fr; } }

  .preview-wrap { margin-top:10px; border:1px solid #e6eaf2; border-radius:12px; overflow:auto; }
  table { width:100%; border-collapse:collapse; min-width: 560px; }
  th, td { border-bottom:1px solid #eef2f7; padding:8px; text-align:left; vertical-align:top; }

  .actions { display:flex; align-items:center; justify-content:space-between; gap:10px; margin-top:10px; flex-wrap:wrap; }
  button { border-radius:14px; border:1px solid #e6eaf2; padding:10px 12px; background:#fff; cursor:pointer; }
  button:disabled { opacity:.6; cursor:not-allowed; }
  .primary { background:#0f172a; color:#fff; border-color:#0f172a; }
  .danger { border-color:#f3c0c0; color:#b91c1c; }
  .muted { color:#64748b; font-size:13px; }

  details { border:1px solid #e6eaf2; border-radius:14px; padding:10px 12px; margin-top:8px; }
  summary { cursor:pointer; font-weight:700; }
  pre { margin:10px 0 0 0; white-space:pre-wrap; word-break:break-word; }

  .hist-head { display:flex; align-items:center; justify-content:space-between; gap:12px; flex-wrap:wrap; }
  .pager { display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
  .hist-list { display:flex; flex-direction:column; gap:10px; margin-top:10px; }
  .hist-item { border:1px solid #e6eaf2; border-radius:14px; padding:10px 12px; background:#f8fafc; }
  .topline { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
  .url { margin-top:6px; font-size:13px; word-break: break-all; }

  .pill { border:1px solid #e6eaf2; border-radius:999px; padding:2px 10px; font-size:12px; background:#fff; }
  .pill2 { border:1px solid #e6eaf2; border-radius:999px; padding:2px 10px; font-size:12px; background:#fff; }

  .alert { margin: 12px 0; padding: 10px 12px; border-radius: 14px; border: 1px solid #f3c0c0; background: #fff5f5; }
  .alert-title { font-weight: 700; margin-bottom: 6px; }
</style>
