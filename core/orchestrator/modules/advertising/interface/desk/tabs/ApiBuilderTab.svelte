<!-- File: core/orchestrator/modules/advertising/interface/desk/tabs/ApiBuilderTab.svelte -->
<script lang="ts">
  type HttpMethod = 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE';

  type ApiSource = {
    id: string;
    name: string;
    baseUrl: string;
    method: HttpMethod;
    path: string;
    headersJson: string;
    queryJson: string;
    bodyJson: string;
    pagination: {
      mode: 'none' | 'page' | 'offset';
      pageParam: string;
      pageSizeParam: string;
      pageStart: number;
      defaultPageSize: number;
      offsetParam: string;
      limitParam: string;
      offsetStart: number;
      defaultLimit: number;
    };
    target: {
      schema: string;
      table: string;
      mappingJson: string;
    };
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

  const SOURCES_KEY = 'ao_api_builder_sources_v1';
  const HISTORY_KEY = 'ao_api_builder_history_v1';

  const PLACEHOLDER_HEADERS = 'например: {"Authorization":"***"}';
  const PLACEHOLDER_QUERY = 'например: {"date_from":"2026-01-01","page":1}';
  const PLACEHOLDER_BODY = 'например: {"a":1}';

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

  function uid() {
    return `${Date.now()}_${Math.random().toString(16).slice(2)}`;
  }

  function loadAll() {
    try {
      sources = JSON.parse(localStorage.getItem(SOURCES_KEY) || '[]') || [];
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

  // ✅ вместо {#let ...} в шаблоне
  $: selected = getSelected();

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

  function normalizeUrl(baseUrl: string, path: string, queryJson: string) {
    const p = path.startsWith('/') ? path : `/${path}`;
    const url = new URL(`${baseUrl.replace(/\/$/, '')}${p}`);
    const q = parseJsonOrEmpty(queryJson);
    for (const [k, v] of Object.entries(q)) url.searchParams.set(k, String(v));
    return url.toString();
  }

  function newSource() {
    const now = Date.now();
    const s: ApiSource = {
      id: uid(),
      name: 'New API',
      baseUrl: 'https://example.com',
      method: 'GET',
      path: '/endpoint',
      headersJson: '',
      queryJson: '',
      bodyJson: '',
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
      target: { schema: '', table: '', mappingJson: '' },
      createdAt: now,
      updatedAt: now
    };

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

  function updateSelected(patch: Partial<ApiSource>) {
    if (!selectedId) return;
    sources = sources.map((s) => (s.id === selectedId ? { ...s, ...patch, updatedAt: Date.now() } : s));
    saveSources();
  }

  // ✅ handlers (чтобы не писать "as ..." в шаблоне)
  function onNameInput(e: Event) {
    updateSelected({ name: (e.currentTarget as HTMLInputElement).value });
  }
  function onBaseUrlInput(e: Event) {
    updateSelected({ baseUrl: (e.currentTarget as HTMLInputElement).value });
  }
  function onMethodChange(e: Event) {
    updateSelected({ method: (e.currentTarget as HTMLSelectElement).value as HttpMethod });
  }
  function onPathInput(e: Event) {
    updateSelected({ path: (e.currentTarget as HTMLInputElement).value });
  }
  function onHeadersInput(e: Event) {
    updateSelected({ headersJson: (e.currentTarget as HTMLTextAreaElement).value });
  }
  function onQueryInput(e: Event) {
    updateSelected({ queryJson: (e.currentTarget as HTMLTextAreaElement).value });
  }
  function onBodyInput(e: Event) {
    updateSelected({ bodyJson: (e.currentTarget as HTMLTextAreaElement).value });
  }

  async function sendTest() {
    err = '';
    respStatus = 0;
    respHeaders = {};
    respText = '';

    if (!selected) return;

    let headersObj: Record<string, string> = {};
    try {
      headersObj = parseJsonOrEmpty(selected.headersJson);
    } catch {
      err = 'Headers JSON: неверный JSON';
      return;
    }

    let url = '';
    try {
      url = normalizeUrl(selected.baseUrl, selected.path, selected.queryJson);
    } catch {
      err = 'Query JSON: неверный JSON';
      return;
    }

    let body: any = undefined;
    if (selected.method !== 'GET' && selected.method !== 'DELETE') {
      if (selected.bodyJson.trim()) {
        try {
          body = JSON.stringify(JSON.parse(selected.bodyJson));
        } catch {
          err = 'Body JSON: неверный JSON';
          return;
        }
      } else {
        body = '';
      }
    }

    const item: HistoryItem = {
      ts: Date.now(),
      sourceId: selected.id,
      method: selected.method,
      url,
      requestHeadersJson: selected.headersJson,
      requestBodyJson: selected.bodyJson
    };

    sending = true;
    const t0 = performance.now();
    try {
      const res = await fetch(url, {
        method: selected.method,
        headers: { 'Content-Type': 'application/json', ...headersObj },
        body
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

  loadAll();
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

  <p class="hint">
    Это будущий “Postman-конструктор”: настраиваешь внешние API-эндпоинты для наполнения таблиц.
    Он <b>не использует</b> текущие серверные эндпоинты модуля и не ломает “Создание/Таблицы и данные”.
  </p>

  {#if err}
    <div class="alert">
      <div class="alert-title">Ошибка</div>
      <pre>{err}</pre>
    </div>
  {/if}

  <div class="layout">
    <aside class="aside">
      <div class="aside-title">Сохранённые API</div>

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
        <div class="hint">Создай или выбери конфиг слева.</div>
      {:else}
        {#key selectedId}
          <div class="card">
            <div class="grid">
              <label>
                Название
                <input value={selected.name} on:input={onNameInput} />
              </label>

              <label>
                Base URL
                <input value={selected.baseUrl} on:input={onBaseUrlInput} />
              </label>

              <label>
                Method
                <select value={selected.method} on:change={onMethodChange}>
                  <option>GET</option><option>POST</option><option>PUT</option><option>PATCH</option><option>DELETE</option>
                </select>
              </label>

              <label>
                Path
                <input value={selected.path} on:input={onPathInput} />
              </label>

              <label class="wide">
                Headers JSON
                <textarea value={selected.headersJson} on:input={onHeadersInput} placeholder={PLACEHOLDER_HEADERS}></textarea>
              </label>

              <label class="wide">
                Query JSON
                <textarea value={selected.queryJson} on:input={onQueryInput} placeholder={PLACEHOLDER_QUERY}></textarea>
              </label>

              <label class="wide">
                Body JSON
                <textarea value={selected.bodyJson} on:input={onBodyInput} placeholder={PLACEHOLDER_BODY}></textarea>
              </label>
            </div>

            <div class="actions">
              <button class="primary" on:click={sendTest} disabled={sending}>
                {sending ? 'Отправляю…' : 'Тест-запрос'}
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
  </div>
</section>

<style>
  .panel { background:#fff; border:1px solid #e6eaf2; border-radius:18px; padding:14px; box-shadow:0 6px 20px rgba(15,23,42,.05); margin-top:12px; }
  .panel-head { display:flex; align-items:center; justify-content:space-between; gap:12px; }
  .panel-head h2 { margin:0; font-size:18px; }
  .quick { display:flex; gap:8px; flex-wrap:wrap; align-items:center; }

  .hint { margin:10px 0 0; color:#64748b; font-size:13px; }

  .layout { display:grid; grid-template-columns: 320px 1fr; gap:12px; margin-top:12px; }
  @media (max-width: 1100px) { .layout { grid-template-columns: 1fr; } }

  .aside { border:1px solid #e6eaf2; border-radius:16px; padding:12px; background:#f8fafc; }
  .aside-title { font-weight:700; margin-bottom:8px; }
  .list { display:flex; flex-direction:column; gap:8px; }
  .item { text-align:left; padding:10px 12px; border-radius:14px; border:1px solid #e6eaf2; background:#fff; cursor:pointer; }
  .activeitem { background:#0f172a; color:#fff; border-color:#0f172a; }
  .name { font-weight:700; }
  .meta { font-size:12px; color:#64748b; margin-top:4px; word-break: break-word; }
  .activeitem .meta { color:#cbd5e1; }

  .card { border:1px solid #e6eaf2; border-radius:16px; padding:12px; background:#fff; margin-bottom:12px; }
  .grid { display:grid; grid-template-columns: 1fr 1fr; gap:10px; }
  @media (max-width: 1100px) { .grid { grid-template-columns: 1fr; } }

  label { display:flex; flex-direction:column; gap:6px; font-size:13px; }
  input, select, textarea { border-radius:14px; border:1px solid #e6eaf2; padding:10px 12px; outline:none; background:#fff; }
  textarea { min-height: 90px; resize: vertical; }
  .wide { grid-column: 1 / -1; }

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
