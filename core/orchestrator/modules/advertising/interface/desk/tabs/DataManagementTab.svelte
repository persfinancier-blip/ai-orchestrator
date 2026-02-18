<!-- File: core/orchestrator/modules/advertising/interface/desk/tabs/DataManagementTab.svelte -->
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
    pagination: any;
    target: { schema: string; table: string; mappingJson: string };
    createdAt: number;
    updatedAt: number;
  };

  const SOURCES_KEY = 'ao_api_builder_sources_v1';

  let sources: ApiSource[] = [];
  let selectedId: string | null = null;

  let err = '';
  let running = false;

  let previewText = '';
  let previewStatus = 0;

  function loadSources() {
    try {
      sources = JSON.parse(localStorage.getItem(SOURCES_KEY) || '[]') || [];
    } catch {
      sources = [];
    }
    if (!selectedId && sources.length) selectedId = sources[0].id;
  }

  function getSelected(): ApiSource | null {
    return sources.find((s) => s.id === selectedId) || null;
  }

  // ✅ Svelte way вместо {#let ...}
  $: selected = getSelected();

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

  async function runPreview() {
    err = '';
    previewText = '';
    previewStatus = 0;

    if (!selected) return;

    let headersObj: any = {};
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

    running = true;
    try {
      const res = await fetch(url, {
        method: selected.method,
        headers: { 'Content-Type': 'application/json', ...headersObj },
        body
      });

      previewStatus = res.status;
      previewText = await res.text();
    } catch (e: any) {
      err = e?.message ?? String(e);
    } finally {
      running = false;
    }
  }

  loadSources();
</script>

<section class="panel">
  <div class="panel-head">
    <h2>Управление данными</h2>
    <div class="quick">
      <button on:click={loadSources}>Обновить</button>
    </div>
  </div>

  <p class="hint">
    Эта вкладка использует <b>сохранённые конфиги</b> из “API (конструктор)” для будущего workflow.
    Сейчас тут только “preview run” (не пишет в БД).
  </p>

  {#if err}
    <div class="alert">
      <div class="alert-title">Ошибка</div>
      <pre>{err}</pre>
    </div>
  {/if}

  <div class="layout">
    <aside class="aside">
      <div class="aside-title">Источники</div>

      {#if sources.length === 0}
        <div class="hint">Нет сохранённых API. Создай их во вкладке “API”.</div>
      {:else}
        <div class="list">
          {#each sources as s (s.id)}
            <button class="item" class:activeitem={s.id === selectedId} on:click={() => (selectedId = s.id)}>
              <div class="name">{s.name}</div>
              <div class="meta">{s.method} {s.baseUrl}{s.path}</div>
              {#if s.target?.schema || s.target?.table}
                <div class="meta">target: {s.target.schema}.{s.target.table}</div>
              {/if}
            </button>
          {/each}
        </div>
      {/if}
    </aside>

    <div class="main">
      {#if !selected}
        <div class="hint">Выбери источник слева.</div>
      {:else}
        <div class="card">
          <div class="row">
            <div>
              <div class="title">{selected.name}</div>
              <div class="muted">{selected.method} {selected.baseUrl}{selected.path}</div>
            </div>

            <button class="primary" on:click={runPreview} disabled={running}>
              {running ? 'Запуск…' : 'Preview run'}
            </button>
          </div>

          <div class="muted" style="margin-top:8px;">
            status: {previewStatus || '-'}
          </div>

          <div class="box">
            <pre>{previewText || ''}</pre>
          </div>

          <div class="hint" style="margin-top:10px;">
            Дальше здесь появится: маппинг полей → вставка в таблицы → расписания → логирование.
          </div>
        </div>
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

  .card { border:1px solid #e6eaf2; border-radius:16px; padding:12px; background:#fff; }
  .row { display:flex; align-items:center; justify-content:space-between; gap:10px; flex-wrap:wrap; }
  .title { font-weight:800; }
  .muted { color:#64748b; font-size:13px; }

  .box { margin-top:10px; border:1px solid #e6eaf2; border-radius:14px; padding:10px 12px; background:#f8fafc; }
  pre { margin:0; white-space: pre-wrap; word-break: break-word; }

  button { border-radius:14px; border:1px solid #e6eaf2; padding:10px 12px; background:#fff; cursor:pointer; }
  button:disabled { opacity:.6; cursor:not-allowed; }
  .primary { background:#0f172a; color:#fff; border-color:#0f172a; }

  .alert { margin: 12px 0; padding: 10px 12px; border-radius: 14px; border: 1px solid #f3c0c0; background: #fff5f5; }
  .alert-title { font-weight: 700; margin-bottom: 6px; }
</style>
