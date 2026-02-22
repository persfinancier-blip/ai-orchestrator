<script lang="ts">
  export type ExistingTable = { schema_name: string; table_name: string };
  type HttpMethod = 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE';

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
    queryJson: string;
    bodyJson: string;
    targetSchema: string;
    targetTable: string;
    description: string;
    isActive: boolean;
    exampleRequest: string;
  };

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
  let selected: ApiDraft | null = null;
  let myApiPreview = '';
  let lastSelectedRef = '';

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
      queryJson: '',
      bodyJson: '',
      targetSchema: '',
      targetTable: '',
      description: '',
      isActive: true,
      exampleRequest: ''
    };
  }

  function fromRow(row: any): ApiDraft {
    const d = baseDraft();
    const storeIdRaw = row?.id ?? row?.api_config_id ?? row?.config_id ?? null;
    const storeIdNum = Number(String(storeIdRaw ?? '').trim());

    const mapping = tryObj(row?.mapping_json);
    const legacy = tryObj(mapping?.source ?? mapping?.config ?? mapping?.payload);

    return {
      ...d,
      localId: uid(),
      storeId: Number.isFinite(storeIdNum) && storeIdNum > 0 ? Math.trunc(storeIdNum) : undefined,
      name: String(row?.api_name || legacy?.name || 'API'),
      method: toHttpMethod(String(row?.method || legacy?.method || 'GET').toUpperCase()),
      baseUrl: String(row?.base_url || legacy?.base_url || legacy?.baseUrl || ''),
      path: String(row?.path || legacy?.path || '/'),
      headersJson: toPrettyJson(tryObj(row?.headers_json || legacy?.headers_json || legacy?.headersJson || legacy?.headers)),
      queryJson: toPrettyJson(tryObj(row?.query_json || legacy?.query_json || legacy?.queryJson || legacy?.query || legacy?.params)),
      bodyJson: toPrettyJson(tryObj(row?.body_json || legacy?.body_json || legacy?.bodyJson || legacy?.body || legacy?.data)),
      targetSchema: String(row?.target_schema || legacy?.targetSchema || ''),
      targetTable: String(row?.target_table || legacy?.targetTable || ''),
      description: String(row?.description || legacy?.description || ''),
      isActive: row?.is_active === undefined ? true : Boolean(row?.is_active),
      exampleRequest: String(mapping?.exampleRequest || legacy?.exampleRequest || '')
    };
  }

  function toPayload(d: ApiDraft) {
    return {
      id: d.storeId || undefined,
      api_name: d.name,
      method: d.method,
      base_url: d.baseUrl,
      path: d.path,
      headers_json: tryObj(d.headersJson),
      query_json: tryObj(d.queryJson),
      body_json: tryObj(d.bodyJson),
      pagination_json: {},
      target_schema: d.targetSchema,
      target_table: d.targetTable,
      mapping_json: { exampleRequest: d.exampleRequest },
      description: d.description,
      is_active: d.isActive !== false
    };
  }

  function fullUrl(d: ApiDraft) {
    const base = String(d.baseUrl || '').trim();
    const path = String(d.path || '').trim() || '/';
    const p = path.startsWith('/') ? path : `/${path}`;
    const u = new URL(`${base.replace(/\/$/, '')}${p}`);
    const q = parseObj(d.queryJson);
    for (const [k, v] of Object.entries(q || {})) u.searchParams.set(k, String(v));
    return u.toString();
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

  function mutateSelected(mutator: (d: ApiDraft) => void) {
    if (!selectedRef) return;
    drafts = drafts.map((d) => {
      if (refOf(d) !== selectedRef) return d;
      const next = { ...d };
      mutator(next);
      return next;
    });
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

    saving = true;
    try {
      const r = await apiJson<{ id?: number }>(`${apiBase}/api-configs/upsert`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify(toPayload(next))
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
      const url = fullUrl(s);
      const hdr = parseObj(s.headersJson);
      const init: RequestInit = {
        method: s.method,
        headers: { 'Content-Type': 'application/json', ...hdr }
      };
      if (s.method !== 'GET' && s.method !== 'DELETE') {
        const b = String(s.bodyJson || '').trim();
        init.body = b ? JSON.stringify(parseObj(b)) : '';
      }
      const res = await fetch(url, init);
      responseStatus = res.status;
      responseText = await res.text();
      ok = 'Проверка выполнена';
    } catch (e: any) {
      err = e?.message ?? String(e);
    } finally {
      checking = false;
    }
  }

  $: selected = byRef(selectedRef);
  $: if (selectedRef !== lastSelectedRef) {
    lastSelectedRef = selectedRef;
    if (selected) {
      nameDraft = selected.name;
      requestInput = `${selected.baseUrl.replace(/\/$/, '')}${selected.path.startsWith('/') ? selected.path : `/${selected.path}`}`;
    }
  }
  $: myApiPreview = selected
    ? JSON.stringify(
        {
          method: selected.method,
          url: (() => {
            try {
              return fullUrl(selected);
            } catch {
              return `${selected.baseUrl}${selected.path}`;
            }
          })(),
          headers: tryObj(selected.headersJson),
          body: selected.method === 'GET' || selected.method === 'DELETE' ? undefined : tryObj(selected.bodyJson)
        },
        null,
        2
      )
    : '';

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
        <div class="subttl">Предпросмотр ответа</div>
        <div class="statusline">status: {responseStatus || '-'}</div>
        <textarea readonly value={responseText}></textarea>
      </div>
      <div class="subsec">
        <div class="subttl">Шаблон API</div>
        <textarea
          value={selected?.exampleRequest || ''}
          on:input={(e) => mutateSelected((d) => (d.exampleRequest = e.currentTarget.value))}
          placeholder="Вставьте пример API"
        ></textarea>
      </div>
      <div class="subsec">
        <div class="subttl">Предпросмотр твоего API</div>
        <textarea readonly value={myApiPreview}></textarea>
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

        <div class="api-meta-row">
          <input
            placeholder="Схема назначения"
            value={selected?.targetSchema || ''}
            on:input={(e) => mutateSelected((d) => (d.targetSchema = e.currentTarget.value))}
          />
          <input
            placeholder="Таблица назначения"
            value={selected?.targetTable || ''}
            on:input={(e) => mutateSelected((d) => (d.targetTable = e.currentTarget.value))}
          />
          <label class="api-active">
            <input
              type="checkbox"
              checked={selected?.isActive !== false}
              on:change={(e) => mutateSelected((d) => (d.isActive = Boolean(e.currentTarget.checked)))}
            />
            Активен
          </label>
        </div>

        <textarea
          class="desc"
          placeholder="Описание API"
          value={selected?.description || ''}
          on:input={(e) => mutateSelected((d) => (d.description = e.currentTarget.value))}
        ></textarea>

        <div class="raw-grid">
          <label>
            Query JSON
            <textarea
              value={selected?.queryJson || ''}
              on:input={(e) => mutateSelected((d) => (d.queryJson = e.currentTarget.value))}
            ></textarea>
          </label>
          <label>
            Headers JSON
            <textarea
              value={selected?.headersJson || ''}
              on:input={(e) => mutateSelected((d) => (d.headersJson = e.currentTarget.value))}
            ></textarea>
          </label>
        </div>

        <label>
          Body JSON
          <textarea
            value={selected?.bodyJson || ''}
            on:input={(e) => mutateSelected((d) => (d.bodyJson = e.currentTarget.value))}
          ></textarea>
        </label>
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
  .statusline { font-size:12px; color:#64748b; margin-bottom:6px; }

  .main { min-width:0; }
  .card { border:1px solid #e6eaf2; border-radius:16px; padding:12px; background:#fff; }

  .connect-row { margin-top:10px; display:grid; grid-template-columns: 180px 1fr 150px; gap:8px; align-items:center; }
  .api-meta-row { margin-top:8px; display:grid; grid-template-columns: 1fr 1fr auto; gap:8px; align-items:center; }
  .api-active { display:flex; align-items:center; gap:6px; white-space:nowrap; }
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
  .activeitem .icon-btn { color:#b91c1c; }

  input, select, textarea { border-radius:14px; border:1px solid #e6eaf2; padding:10px 12px; outline:none; background:#fff; box-sizing:border-box; width:100%; }
  textarea { min-height:90px; resize:vertical; }

  button { border-radius:14px; border:1px solid #e6eaf2; padding:10px 12px; background:#fff; cursor:pointer; }
  button:disabled { opacity:.6; cursor:not-allowed; }
  .primary { background:#0f172a; color:#fff; border-color:#0f172a; }

  .alert { margin: 12px 0; padding: 10px 12px; border-radius: 14px; border: 1px solid #f3c0c0; background: #fff5f5; }
  .alert-title { font-weight: 700; margin-bottom: 6px; }
  .okbox { margin: 12px 0; padding: 10px 12px; border-radius: 14px; border: 1px solid #bbf7d0; background: #f0fdf4; color:#166534; }
  pre { margin:0; white-space: pre-wrap; word-break: break-word; }
</style>
