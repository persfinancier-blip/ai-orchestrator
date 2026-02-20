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

  type ApiSource = {
    id: string;
    name: string;
    baseUrl: string;
    method: HttpMethod;
    path: string;
    headersJson: string;
    queryJson: string;
    bodyJson: string;
    exampleRequest: string;
    auth: AuthConfig;
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

  const SOURCES_KEY = 'ao_api_builder_sources_v2';
  const HISTORY_KEY = 'ao_api_builder_history_v1';

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
  let generatedApiPreview = '';
  let previewDraft = '';
  let editingPreview = false;
  let previewSyncError = '';
  let previewApplyMessage = '';
  let exampleRequestEl: HTMLTextAreaElement | null = null;
  let generatedPreviewEl: HTMLTextAreaElement | null = null;

  function uid() {
    return `${Date.now()}_${Math.random().toString(16).slice(2)}`;
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

  function loadAll() {
    try {
      const raw = JSON.parse(localStorage.getItem(SOURCES_KEY) || '[]') || [];
      sources = raw.map(normalizeSource);
    } catch {
      sources = [];
    }

    try {
      history = JSON.parse(localStorage.getItem(HISTORY_KEY) || '[]') || [];
    } catch {
      history = [];
    }

    if (!selectedId && sources.length) selectedId = sources[0].id;
  }

  function saveSources() {
    localStorage.setItem(SOURCES_KEY, JSON.stringify(sources.slice(0, 200)));
  }

  function saveHistory() {
    localStorage.setItem(HISTORY_KEY, JSON.stringify(history.slice(0, 300)));
  }

  function getSelected(): ApiSource | null {
    return sources.find((s) => s.id === selectedId) || null;
  }

  $: selected = getSelected();

  function mutateSelected(mutator: (next: ApiSource) => void) {
    if (!selectedId) return;
    sources = sources.map((s) => {
      if (s.id !== selectedId) return s;
      const next: ApiSource = {
        ...s,
        auth: { ...s.auth },
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
    saveSources();
  }

  function deleteSelected() {
    if (!selectedId) return;
    sources = sources.filter((s) => s.id !== selectedId);
    selectedId = sources[0]?.id ?? null;
    saveSources();
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

  loadAll();
  $: if (selected) ensureTableSelection();
  $: if (selectedId) generatedApiPreview = buildGeneratedPreview(selected);
  $: if (!editingPreview) previewDraft = generatedApiPreview;
  $: selectedId, tick().then(autosizeCompareTextareas);
  $: previewDraft, tick().then(autosizeCompareTextareas);
</script>

<section class="panel">
  <div class="panel-head">
    <h2>API (конструктор)</h2>
    <div class="quick">
      <button on:click={newSource}>+ Новый</button>
      <button class="danger" on:click={deleteSelected} disabled={!selectedId}>Удалить</button>
      <button class="danger" on:click={clearHistory} disabled={history.length === 0}>Очистить историю</button>
    </div>
  </div>

  {#if err}
    <div class="alert">
      <div class="alert-title">Ошибка</div>
      <pre>{err}</pre>
    </div>
  {/if}

  <div class="layout">
    <aside class="aside">
      <div class="aside-title">Сохраненные API</div>
      {#if sources.length === 0}
        <div class="hint">Пока нет ни одного.</div>
      {:else}
        <div class="list">
          {#each sources as s (s.id)}
            <button class="item" class:activeitem={s.id === selectedId} on:click={() => (selectedId = s.id)}>
              <div class="name">{s.name}</div>
              <div class="meta">{s.method} {s.baseUrl}{s.path}</div>
            </button>
          {/each}
        </div>
      {/if}
    </aside>

    <div class="main">
      {#if !selected}
        <div class="hint">Создай или выбери API слева.</div>
      {:else}
        {#key selectedId}
          <div class="card">
            <div class="grid">
              <label>
                Название
                <input value={selected.name} on:input={(e) => mutateSelected((s) => (s.name = e.currentTarget.value))} />
              </label>

              <label>
                Base URL
                <input value={selected.baseUrl} on:input={(e) => mutateSelected((s) => (s.baseUrl = e.currentTarget.value))} />
              </label>

              <label>
                Method
                <select value={selected.method} on:change={(e) => mutateSelected((s) => (s.method = toHttpMethod(e.currentTarget.value)))}>
                  <option>GET</option><option>POST</option><option>PUT</option><option>PATCH</option><option>DELETE</option>
                </select>
              </label>

              <label>
                Path
                <input value={selected.path} on:input={(e) => mutateSelected((s) => (s.path = e.currentTarget.value))} />
              </label>
            </div>

            <div class="subcard">
              <h3>Авторизация</h3>
              <div class="grid">
                <label>
                  Тип
                  <select value={selected.auth.mode} on:change={(e) => mutateSelected((s) => (s.auth.mode = toAuthMode(e.currentTarget.value)))}>
                    <option value="none">none</option>
                    <option value="bearer">bearer</option>
                    <option value="basic">basic</option>
                    <option value="apiKey">apiKey</option>
                  </select>
                </label>

                {#if selected.auth.mode === 'bearer'}
                  <label>
                    Bearer token
                    <input value={selected.auth.bearerToken} on:input={(e) => mutateSelected((s) => (s.auth.bearerToken = e.currentTarget.value))} />
                  </label>
                {/if}

                {#if selected.auth.mode === 'basic'}
                  <label>
                    Username
                    <input value={selected.auth.basicUsername} on:input={(e) => mutateSelected((s) => (s.auth.basicUsername = e.currentTarget.value))} />
                  </label>
                  <label>
                    Password
                    <input value={selected.auth.basicPassword} on:input={(e) => mutateSelected((s) => (s.auth.basicPassword = e.currentTarget.value))} />
                  </label>
                {/if}

                {#if selected.auth.mode === 'apiKey'}
                  <label>
                    Key name
                    <input value={selected.auth.apiKeyName} on:input={(e) => mutateSelected((s) => (s.auth.apiKeyName = e.currentTarget.value))} />
                  </label>
                  <label>
                    Key value
                    <input value={selected.auth.apiKeyValue} on:input={(e) => mutateSelected((s) => (s.auth.apiKeyValue = e.currentTarget.value))} />
                  </label>
                  <label>
                    Передавать в
                    <select value={selected.auth.apiKeyIn} on:change={(e) => mutateSelected((s) => (s.auth.apiKeyIn = toApiKeyIn(e.currentTarget.value)))}>
                      <option value="header">header</option>
                      <option value="query">query</option>
                    </select>
                  </label>
                {/if}
              </div>
            </div>

            <div class="subcard">
              <h3>Пагинация</h3>
              <div class="grid">
                <label>
                  Тип
                  <select value={selected.pagination.mode} on:change={(e) => mutateSelected((s) => (s.pagination.mode = toPaginationMode(e.currentTarget.value)))}>
                    <option value="none">none</option>
                    <option value="page">page</option>
                    <option value="offset">offset</option>
                  </select>
                </label>

                {#if selected.pagination.mode === 'page'}
                  <label>
                    page param
                    <input value={selected.pagination.pageParam} on:input={(e) => mutateSelected((s) => (s.pagination.pageParam = e.currentTarget.value))} />
                  </label>
                  <label>
                    page size param
                    <input value={selected.pagination.pageSizeParam} on:input={(e) => mutateSelected((s) => (s.pagination.pageSizeParam = e.currentTarget.value))} />
                  </label>
                  <label>
                    start page
                    <input type="number" value={selected.pagination.pageStart} on:input={(e) => mutateSelected((s) => (s.pagination.pageStart = Number(e.currentTarget.value || 1)))} />
                  </label>
                  <label>
                    page size
                    <input type="number" value={selected.pagination.defaultPageSize} on:input={(e) => mutateSelected((s) => (s.pagination.defaultPageSize = Number(e.currentTarget.value || 50)))} />
                  </label>
                {/if}

                {#if selected.pagination.mode === 'offset'}
                  <label>
                    offset param
                    <input value={selected.pagination.offsetParam} on:input={(e) => mutateSelected((s) => (s.pagination.offsetParam = e.currentTarget.value))} />
                  </label>
                  <label>
                    limit param
                    <input value={selected.pagination.limitParam} on:input={(e) => mutateSelected((s) => (s.pagination.limitParam = e.currentTarget.value))} />
                  </label>
                  <label>
                    offset start
                    <input type="number" value={selected.pagination.offsetStart} on:input={(e) => mutateSelected((s) => (s.pagination.offsetStart = Number(e.currentTarget.value || 0)))} />
                  </label>
                  <label>
                    default limit
                    <input type="number" value={selected.pagination.defaultLimit} on:input={(e) => mutateSelected((s) => (s.pagination.defaultLimit = Number(e.currentTarget.value || 50)))} />
                  </label>
                {/if}
              </div>
            </div>

            <div class="grid">
              <label class="wide">
                Headers JSON
                <textarea value={selected.headersJson} on:input={(e) => mutateSelected((s) => (s.headersJson = e.currentTarget.value))} placeholder={PLACEHOLDER_HEADERS}></textarea>
              </label>

              <label class="wide">
                Query JSON
                <textarea value={selected.queryJson} on:input={(e) => mutateSelected((s) => (s.queryJson = e.currentTarget.value))} placeholder={PLACEHOLDER_QUERY}></textarea>
              </label>

              <label class="wide">
                Body JSON
                <textarea value={selected.bodyJson} on:input={(e) => mutateSelected((s) => (s.bodyJson = e.currentTarget.value))} placeholder={PLACEHOLDER_BODY}></textarea>
              </label>
            </div>

            <div class="subcard">
              <h3>Параметры из БД</h3>
              <div class="grid">
                <label>
                  Таблица
                  <select value={`${selected.db.schema}.${selected.db.table}`} on:change={(e) => {
                    const v = e.currentTarget.value;
                    const [schema, table] = v.split('.');
                    mutateSelected((s) => {
                      s.db.schema = schema || '';
                      s.db.table = table || '';
                      s.db.rowIndex = 0;
                    });
                    loadDbPreview();
                  }}>
                    {#each existingTables as t}
                      <option value={`${t.schema_name}.${t.table_name}`}>{t.schema_name}.{t.table_name}</option>
                    {/each}
                  </select>
                </label>

                <label>
                  Строка preview (индекс)
                  <input type="number" min="0" value={selected.db.rowIndex} on:input={(e) => mutateSelected((s) => (s.db.rowIndex = Number(e.currentTarget.value || 0)))} />
                </label>

                <div class="inline-actions">
                  <button on:click={refreshTables}>Обновить список таблиц</button>
                  <button on:click={loadDbPreview}>Загрузить preview</button>
                </div>
              </div>

              {#if dbPreviewError}
                <p class="error">{dbPreviewError}</p>
              {/if}

              <div class="bindings-head">
                <b>Привязки параметров</b>
                <button on:click={addBinding} disabled={dbPreviewColumns.length === 0}>+ Добавить привязку</button>
              </div>

              {#if selected.db.bindings.length === 0}
                <p class="hint">Нет привязок. Добавь хотя бы одну, чтобы подставлять значения из таблицы в query/header/body.</p>
              {:else}
                <div class="bindings">
                  {#each selected.db.bindings as b}
                    <div class="binding-row">
                      <select value={b.target} on:change={(e) => mutateSelected((s) => {
                        const x = s.db.bindings.find((z) => z.id === b.id);
                        if (x) x.target = toBindingTarget(e.currentTarget.value);
                      })}>
                        <option value="query">query</option>
                        <option value="header">header</option>
                        <option value="body">body</option>
                      </select>

                      <input placeholder="target key" value={b.targetKey} on:input={(e) => mutateSelected((s) => {
                        const x = s.db.bindings.find((z) => z.id === b.id);
                        if (x) x.targetKey = e.currentTarget.value;
                      })} />

                      <select value={b.sourceColumn} on:change={(e) => mutateSelected((s) => {
                        const x = s.db.bindings.find((z) => z.id === b.id);
                        if (x) x.sourceColumn = e.currentTarget.value;
                      })}>
                        <option value="">source column</option>
                        {#each dbPreviewColumns as c}
                          <option value={c}>{c}</option>
                        {/each}
                      </select>

                      <button class="danger" on:click={() => removeBinding(b.id)}>Удалить</button>
                    </div>
                  {/each}
                </div>
              {/if}

              {#if dbPreviewLoading}
                <p class="hint">Загрузка preview...</p>
              {:else if dbPreviewRows.length > 0}
                <div class="preview-wrap">
                  <table>
                    <thead>
                      <tr>{#each dbPreviewColumns as c}<th>{c}</th>{/each}</tr>
                    </thead>
                    <tbody>
                      {#each dbPreviewRows as r}
                        <tr>{#each dbPreviewColumns as c}<td>{String(r[c] ?? '')}</td>{/each}</tr>
                      {/each}
                    </tbody>
                  </table>
                </div>
              {/if}
            </div>

            <div class="actions">
              <button class="primary" on:click={sendTest} disabled={sending}>
                {sending ? 'Отправляю...' : 'Тест-запрос'}
              </button>
              <div class="muted">Ответ: status {respStatus || '-'}</div>
            </div>
          </div>

          <div class="card">
            <h3>Ответ</h3>
            <details open>
              <summary>Headers</summary>
              <pre>{JSON.stringify(respHeaders, null, 2)}</pre>
            </details>
            <details open>
              <summary>Body</summary>
              <pre>{respText || ''}</pre>
            </details>
          </div>

          <div class="card">
            <div class="hist-head">
              <h3>История запросов</h3>
              <div class="pager">
                <label>
                  page size
                  <select bind:value={pageSize}>
                    <option value="5">5</option>
                    <option value="10">10</option>
                    <option value="20">20</option>
                  </select>
                </label>

                <button on:click={() => (page = Math.max(1, page - 1))} disabled={page <= 1}>←</button>
                <span class="muted">{page} / {totalPages()}</span>
                <button on:click={() => (page = Math.min(totalPages(), page + 1))} disabled={page >= totalPages()}>→</button>
              </div>
            </div>

            {#if history.length === 0}
              <div class="hint">Пока пусто.</div>
            {:else}
              <div class="hist-list">
                {#each pagedHistory() as h (h.ts)}
                  <div class="hist-item">
                    <div class="topline">
                      <span class="pill">{h.method}</span>
                      <span class="muted">{new Date(h.ts).toLocaleString()}</span>
                      <span class="pill2">{h.status ?? '-'}</span>
                      <span class="pill2">{h.ms ?? '-'} ms</span>
                    </div>
                    <div class="url">{h.url}</div>
                  </div>
                {/each}
              </div>
            {/if}
          </div>
        {/key}
      {/if}
    </div>

    <aside class="aside compare-aside">
      <div class="aside-title">Сравнение API</div>
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
  .quick { display:flex; gap:8px; flex-wrap:wrap; align-items:center; }

  .hint { margin:10px 0 0; color:#64748b; font-size:13px; }
  .error { margin:8px 0; color:#b91c1c; font-size:13px; }

  .layout { display:grid; grid-template-columns: 320px 1fr 360px; gap:12px; margin-top:12px; align-items:start; }
  @media (max-width: 1300px) { .layout { grid-template-columns: 320px 1fr; } }
  @media (max-width: 1100px) { .layout { grid-template-columns: 1fr; } }

  .aside { border:1px solid #e6eaf2; border-radius:16px; padding:12px; background:#f8fafc; }
  .aside-title { font-weight:700; margin-bottom:8px; }
  .compare-aside { position: sticky; top: 12px; }
  .compare-fields { display:flex; flex-direction:column; gap:10px; }
  .compare-fields textarea { overflow:hidden; resize:none; }
  .list { display:flex; flex-direction:column; gap:8px; }
  .item { text-align:left; padding:10px 12px; border-radius:14px; border:1px solid #e6eaf2; background:#fff; cursor:pointer; }
  .activeitem { background:#0f172a; color:#fff; border-color:#0f172a; }
  .name { font-weight:700; }
  .meta { font-size:12px; color:#64748b; margin-top:4px; word-break: break-word; }
  .activeitem .meta { color:#cbd5e1; }

  .card { border:1px solid #e6eaf2; border-radius:16px; padding:12px; background:#fff; margin-bottom:12px; }
  .subcard { margin-top:10px; border:1px dashed #e6eaf2; border-radius:14px; padding:10px; }
  .subcard h3 { margin:0 0 8px 0; font-size:14px; }

  .grid { display:grid; grid-template-columns: 1fr 1fr; gap:10px; }
  @media (max-width: 1100px) { .grid { grid-template-columns: 1fr; } }

  label { display:flex; flex-direction:column; gap:6px; font-size:13px; }
  input, select, textarea { border-radius:14px; border:1px solid #e6eaf2; padding:10px 12px; outline:none; background:#fff; }
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
