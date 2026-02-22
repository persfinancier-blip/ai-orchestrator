<script lang="ts">
  import { tick } from 'svelte';
  import JsonTreeView from '../components/JsonTreeView.svelte';
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
    authJson: string;
    queryJson: string;
    bodyJson: string;
    pickedPaths: string[];
    responseTargets: Array<{
      id: string;
      schema: string;
      table: string;
      fields: Array<{ id: string; responsePath: string; targetField: string }>;
    }>;
    description: string;
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
  let templateParseTimer: ReturnType<typeof setTimeout> | null = null;
  let activeResponseFieldRef = '';
  let columnsCache: Record<string, string[]> = {};

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
      queryJson: '',
      bodyJson: '',
      pickedPaths: [],
      responseTargets: [],
      description: '',
      exampleRequest: ''
    };
  }

  function fromRow(row: any): ApiDraft {
    const d = baseDraft();
    const storeIdRaw = row?.id ?? row?.api_config_id ?? row?.config_id ?? null;
    const storeIdNum = Number(String(storeIdRaw ?? '').trim());

    const mapping = tryObj(row?.mapping_json);
    const legacy = tryObj(mapping?.source ?? mapping?.config ?? mapping?.payload);
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
      queryJson: toPrettyJson(tryObj(row?.query_json || legacy?.query_json || legacy?.queryJson || legacy?.query || legacy?.params)),
      bodyJson: toPrettyJson(tryObj(row?.body_json || legacy?.body_json || legacy?.bodyJson || legacy?.body || legacy?.data)),
      pickedPaths: Array.isArray(mapping?.picked_paths) ? mapping.picked_paths.map((x: any) => String(x || '').trim()).filter(Boolean) : [],
      responseTargets: normalizedTargets,
      description: String(row?.description || legacy?.description || ''),
      exampleRequest: String(mapping?.exampleRequest || legacy?.exampleRequest || '')
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
      pagination_json: {},
      target_schema: firstTarget?.schema || '',
      target_table: firstTarget?.table || '',
      mapping_json: {
        exampleRequest: d.exampleRequest,
        response_targets: d.responseTargets,
        picked_paths: d.pickedPaths,
        auth_json: parsed?.authJson ?? tryObj(d.authJson)
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

  function scheduleTemplateParse() {
    if (templateParseTimer) clearTimeout(templateParseTimer);
    templateParseTimer = setTimeout(() => parseTemplateNow(false), 450);
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
      const url = fullUrl(s);
      const authHdr = parseJsonObjectField('Авторизация', s.authJson);
      const hdr = parseJsonObjectField('Headers JSON', s.headersJson);
      const init: RequestInit = {
        method: s.method,
        headers: { 'Content-Type': 'application/json', ...authHdr, ...hdr }
      };
      if (s.method !== 'GET' && s.method !== 'DELETE') {
        const b = String(s.bodyJson || '').trim();
        init.body = b ? JSON.stringify(parseJsonAnyField('Body JSON', b)) : '';
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
          auth: safePreviewObj(selected.authJson),
          headers: safePreviewObj(selected.headersJson),
          query: safePreviewObj(selected.queryJson),
          body: selected.method === 'GET' || selected.method === 'DELETE' ? undefined : safePreviewObj(selected.bodyJson)
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
            <JsonTreeView node={responseJson} name="response" level={0} on:pickpath={(e) => applyPickedResponsePath(e.detail.path)} />
          </div>
        {:else}
          <textarea bind:this={responsePreviewEl} readonly value={responseText}></textarea>
        {/if}
      </div>
      <div class="subsec">
        <div class="subttl template-head">
          <span>Шаблон API</span>
          <span class="template-head-actions">
            {#if exampleIsJson}
              <button type="button" class="view-toggle" on:click={() => (exampleViewMode = exampleViewMode === 'tree' ? 'raw' : 'tree')}>
                {exampleViewMode === 'tree' ? 'RAW' : 'Дерево'}
              </button>
            {/if}
            <button class="icon-btn template-action" type="button" title="Разобрать в настройки" on:click|preventDefault|stopPropagation={onTemplateParseClick}>+</button>
            <button class="icon-btn danger template-action" type="button" title="Очистить поле" on:click|preventDefault|stopPropagation={onTemplateClearClick}>x</button>
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
              scheduleTemplateParse();
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

        <label>
          <div class="response-head">
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

        <div class="raw-grid">
          <label>
            <div class="response-head">
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
            <div class="response-head">
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
          <div class="response-head">
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

        <div class="targets-wrap">
          <div class="targets-head">
            <div class="targets-title">Куда записывать ответ</div>
            <button type="button" on:click={addMappingRow}>+ Добавить сопоставление</button>
          </div>
          <div class="crumbs-panel">
            <div class="crumbs-title">Витрина</div>
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
              <span class="mapping-head-right">Ответ API | Таблица | Колонка</span>
            </div>
            {#if !mappingRowsOf(selected).length}
              <p class="hint">Добавь сопоставление и укажи куда писать данные.</p>
            {:else}
              <div class="mapping-list">
                {#each mappingRowsOf(selected) as m (`${m.targetId}:${m.fieldId}`)}
                  <div class="map-row" class:active-map={isActiveResponseField(m.targetId, m.fieldId)}>
                    <input
                      placeholder="Ответ API (путь)"
                      value={m.responsePath}
                      on:focus={() => setActiveResponseField(m.targetId, m.fieldId)}
                      on:click={() => setActiveResponseField(m.targetId, m.fieldId)}
                      on:dragover|preventDefault
                      on:drop={(e) => dropPathToMapping(e, m.targetId, m.fieldId)}
                      on:input={(e) => setMappingRowResponsePath(m.targetId, m.fieldId, e.currentTarget.value)}
                    />
                    <select
                      value={formatQualifiedTable(m.schema, m.table)}
                      on:change={(e) => setMappingRowTable(m.targetId, e.currentTarget.value)}
                    >
                      <option value="">Таблица</option>
                      {#each existingTables as et}
                        <option value={`${et.schema_name}.${et.table_name}`}>{et.schema_name}.{et.table_name}</option>
                      {/each}
                    </select>
                    <input
                      list={`cols_${m.targetId}_${m.fieldId}`}
                      placeholder="Колонка"
                      value={m.targetField}
                      on:focus={() => setActiveResponseField(m.targetId, m.fieldId)}
                      on:click={() => setActiveResponseField(m.targetId, m.fieldId)}
                      on:input={(e) => setMappingRowColumn(m.targetId, m.fieldId, e.currentTarget.value)}
                    />
                    <datalist id={`cols_${m.targetId}_${m.fieldId}`}>
                      {#each columnOptionsFor(m.schema, m.table) as col}
                        <option value={col}></option>
                      {/each}
                    </datalist>
                    <button class="icon-btn danger" type="button" on:click={() => removeMappingRow(m.targetId, m.fieldId)} title="Удалить сопоставление">x</button>
                  </div>
                {/each}
              </div>
            {/if}
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
  .inline-actions { display:inline-flex; align-items:center; gap:6px; flex-wrap:nowrap; }
  .view-toggle { border-radius:10px; border:1px solid #e2e8f0; background:#fff; color:#0f172a; padding:4px 8px; font-size:11px; line-height:1.2; }
  .response-tree-wrap { border:1px solid #e6eaf2; border-radius:12px; background:#fff; padding:8px; min-height:78px; overflow:visible; }
  .template-head { display:flex; align-items:center; justify-content:space-between; gap:8px; }
  .template-head-actions { display:flex; align-items:center; gap:4px; }
  .template-action {
    width:28px;
    min-width:28px;
    padding:4px 0;
    font-size:16px;
    line-height:1;
    color:#0f172a;
    border:1px solid #e6eaf2;
    background:#fff;
  }
  .statusline { font-size:12px; color:#64748b; margin-bottom:6px; }
  .template-parse-actions { margin-top:8px; display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
  .template-parse-note { font-size:12px; color:#64748b; }

  .main { min-width:0; }
  .card { border:1px solid #e6eaf2; border-radius:16px; padding:12px; background:#fff; }

  .connect-row { margin-top:10px; display:grid; grid-template-columns: 180px 1fr 150px; gap:8px; align-items:center; }
  .targets-wrap { margin-top:10px; border:1px solid #e6eaf2; border-radius:12px; padding:10px; background:transparent; }
  .targets-head { display:flex; align-items:center; justify-content:space-between; gap:8px; margin-bottom:8px; flex-wrap:wrap; }
  .targets-title { font-size:13px; font-weight:700; color:#0f172a; }
  .crumbs-panel { border:1px dashed #dbe3ef; border-radius:12px; padding:8px; background:#fff; margin-bottom:10px; }
  .crumbs-title { font-size:12px; font-weight:600; color:#334155; margin-bottom:6px; }
  .crumbs-list { display:flex; flex-wrap:wrap; gap:6px; }
  .crumb-chip { display:inline-flex; align-items:center; gap:4px; border:1px solid #e2e8f0; border-radius:999px; background:#f8fafc; padding:3px 6px; max-width:100%; }
  .chip-path { border:0; background:transparent; color:#0f172a; padding:0; font-size:11px; max-width:420px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .chip-remove { border:0; background:transparent; color:#b91c1c; padding:0 2px; font-size:12px; line-height:1; }
  .mapping-panel { border:1px solid #e6eaf2; border-radius:12px; background:#fff; padding:8px; }
  .mapping-head { display:flex; align-items:center; justify-content:space-between; gap:8px; margin-bottom:8px; }
  .mapping-head-right { font-size:11px; color:#64748b; }
  .mapping-list { display:flex; flex-direction:column; gap:8px; }
  .map-row { display:grid; grid-template-columns: 1.2fr 1fr 1fr auto; gap:8px; align-items:center; border:1px solid #eef2f7; border-radius:10px; padding:6px; background:#fff; }
  .map-row.active-map { border-color:#cbd5e1; background:#f8fafc; }
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
  .map-row .icon-btn { color:#b91c1c; }
  .activeitem .icon-btn { color:#b91c1c; }
  .template-head .icon-btn.template-action { width:28px; min-width:28px; padding:4px 0; font-size:16px; border-color:transparent; background:transparent; color:#0f172a; }
  .template-head .icon-btn.template-action.danger { color:#b91c1c; }

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
</style>







