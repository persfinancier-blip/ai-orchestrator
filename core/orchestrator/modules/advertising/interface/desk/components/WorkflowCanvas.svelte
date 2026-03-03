<script lang="ts">
  import { onMount } from 'svelte';
  import {
    sourceGroups,
    sourceItemsByGroup,
    tools,
    toolPorts,
    type ApiRequestTemplate,
    type SourceGroup,
    type SourceItem,
    type ToolItem,
    type ToolType
  } from '../data/workflowEditor';

  type WorkflowNode = {
    id: string;
    type: 'data' | 'tool';
    x: number;
    y: number;
    config: any;
  };
  type WorkflowEdge = { id: string; from: string; to: string; fromPort: string; toPort: string };
  type NodeRuntime = { inRows: number; outRows: number; lossRows: number; lossPct: number; successPct: number; status: 'idle' | 'ok' | 'warn' | 'error' };
  type WorkflowIssue = { level: 'error' | 'warn' | 'info'; text: string };
  type ApiNodeRequest = {
    method: string;
    url: string;
    authMode: string;
    headersText: string;
    queryText: string;
    bodyText: string;
  };
  type ApiNodeExecution = {
    startedAt: string;
    durationMs: number;
    status: number;
    ok: boolean;
    request: {
      method: string;
      url: string;
      headers: Record<string, any>;
      query: Record<string, any>;
      body: any;
    };
    response: {
      headers: Record<string, string>;
      body: any;
    };
    error?: string;
  };

  let activeTab: SourceGroup = 'api_requests';
  let nodes: WorkflowNode[] = [];
  let edges: WorkflowEdge[] = [];
  let selectedNodeId = '';
  let linkFrom: { nodeId: string; port: string } | null = null;
  let banner = '';
  let nodeRuntime: Record<string, NodeRuntime> = {};
  let edgeRows: Record<string, number> = {};
  let issues: WorkflowIssue[] = [];
  let summary = { sourceRows: 0, finalRows: 0, lossRows: 0, lossPct: 0, successPct: 0 };
  let sourceCatalogLoading = false;
  let sourceCatalogError = '';
  let dynamicApiSources: SourceItem[] = [];
  let dynamicTableSources: SourceItem[] = [];
  let settingsNodeId = '';
  let settingsModalOpen = false;
  let nodeExecutions: Record<string, ApiNodeExecution> = {};
  let nodeExecutionLoading: Record<string, boolean> = {};

  let canvasEl: HTMLDivElement;
  let panX = 0;
  let panY = 0;
  let zoom = 1;
  let isPanning = false;
  let panStartX = 0;
  let panStartY = 0;
  let dragNode: { id: string; offsetX: number; offsetY: number } | null = null;

  const API_BASE = '/ai-orchestrator/api';
  const API_ROLE = 'data_admin';

  $: selectedNode = nodes.find((n) => n.id === selectedNodeId) ?? null;
  $: settingsNode = nodes.find((n) => n.id === settingsNodeId) ?? null;
  $: settingsApiExecution = settingsNodeId ? nodeExecutions[settingsNodeId] : undefined;
  $: settingsApiLoading = settingsNodeId ? Boolean(nodeExecutionLoading[settingsNodeId]) : false;
  $: sourceList =
    activeTab === 'api_requests'
      ? dynamicApiSources.length
        ? dynamicApiSources
        : sourceItemsByGroup.api_requests || []
      : activeTab === 'data_tables'
      ? dynamicTableSources.length
        ? dynamicTableSources
        : sourceItemsByGroup.data_tables || []
      : sourceItemsByGroup.math_calculations || [];

  const uid = (p: string) => `${p}-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
  const toolCfg = (n: WorkflowNode) => n.config as { name: string; toolType: ToolType; settings: Record<string, string> };
  const apiCfg = (n: WorkflowNode) => n.config as SourceItem & { apiRequest?: ApiNodeRequest };

  function prettyJson(value: any) {
    try {
      return JSON.stringify(value ?? {}, null, 2);
    } catch {
      return '{}';
    }
  }

  function normalizeApiRequest(template: ApiRequestTemplate | undefined): ApiNodeRequest {
    return {
      method: String(template?.method || 'GET').trim().toUpperCase() || 'GET',
      url: String(template?.url || '').trim(),
      authMode: String(template?.authMode || 'manual').trim() || 'manual',
      headersText: prettyJson(template?.headers || {}),
      queryText: prettyJson(template?.query || {}),
      bodyText: prettyJson(template?.body || {})
    };
  }

  function isApiNode(n: WorkflowNode | null | undefined) {
    return Boolean(n && n.type === 'data' && String(n.config?.group || '').trim() === 'api_requests');
  }

  function isApiToolNode(n: WorkflowNode | null | undefined) {
    return Boolean(n && n.type === 'tool' && toolCfg(n).toolType === 'api_request');
  }

  function getApiRequestForNode(n: WorkflowNode | null | undefined): ApiNodeRequest {
    if (!n) return normalizeApiRequest(undefined);
    if (isApiNode(n)) return apiCfg(n).apiRequest || normalizeApiRequest(undefined);
    if (isApiToolNode(n)) {
      const settings = toolCfg(n).settings || {};
      return {
        method: String(settings.apiMethod || 'GET').trim().toUpperCase() || 'GET',
        url: String(settings.apiUrl || '').trim(),
        authMode: String(settings.apiAuthMode || 'manual').trim() || 'manual',
        headersText: String(settings.apiHeaders || '{}'),
        queryText: String(settings.apiQuery || '{}'),
        bodyText: String(settings.apiBody || '{}')
      };
    }
    return normalizeApiRequest(undefined);
  }

  function updateApiToolRequest(nodeId: string, key: keyof ApiNodeRequest, value: string) {
    const map: Record<keyof ApiNodeRequest, string> = {
      method: 'apiMethod',
      url: 'apiUrl',
      authMode: 'apiAuthMode',
      headersText: 'apiHeaders',
      queryText: 'apiQuery',
      bodyText: 'apiBody'
    };
    updateSetting(nodeId, map[key], value);
  }

  function applyTemplateToNode(nodeId: string, templateId: string) {
    const src = dynamicApiSources.find((x) => x.id === templateId);
    if (!src) return;
    const tpl = normalizeApiRequest(src.requestTemplate);
    const node = nodes.find((n) => n.id === nodeId);
    if (!node) return;
    if (isApiNode(node)) {
      updateDataNodeConfig(nodeId, { requestTemplate: src.requestTemplate, apiRequest: tpl, templateId: src.id });
      return;
    }
    if (isApiToolNode(node)) {
      updateSetting(nodeId, 'templateId', src.id);
      updateApiToolRequest(nodeId, 'method', tpl.method);
      updateApiToolRequest(nodeId, 'url', tpl.url);
      updateApiToolRequest(nodeId, 'authMode', tpl.authMode);
      updateApiToolRequest(nodeId, 'headersText', tpl.headersText);
      updateApiToolRequest(nodeId, 'queryText', tpl.queryText);
      updateApiToolRequest(nodeId, 'bodyText', tpl.bodyText);
    }
  }

  function nodeTemplateId(n: WorkflowNode | null | undefined) {
    if (!n) return '';
    if (isApiNode(n)) return String(n.config?.templateId || n.config?.id || '').trim();
    if (isApiToolNode(n)) return String(toolCfg(n).settings.templateId || '').trim();
    return '';
  }

  function defaultSettings(toolType: ToolType) {
    if (toolType === 'start_process') return { triggerType: 'interval', intervalValue: '1', intervalUnit: 'minutes' };
    if (toolType === 'schedule_process') return { cron: '0 * * * *', mode: 'sync' };
    if (toolType === 'api_request') return { templateId: '', apiMethod: 'GET', apiUrl: '', apiAuthMode: 'manual', apiHeaders: '{}', apiQuery: '{}', apiBody: '{}' };
    if (toolType === 'table_parser') return { parserMultiplier: '1' };
    if (toolType === 'db_write') return { targetSchema: 'ao_data', targetTable: 'bronze_result', writeSuccessRate: '98' };
    return {};
  }

  function normalizeLayer(schemaName: string, tableName: string) {
    const s = String(schemaName || '').trim().toLowerCase();
    const t = String(tableName || '').trim().toLowerCase();
    if (s.includes('bronze') || t.startsWith('bronze_')) return 'bronze';
    if (s.includes('silver') || t.startsWith('silver_')) return 'silver';
    if (s.includes('gold') || t.startsWith('gold_')) return 'gold';
    return '';
  }

  function buildApiTemplatePreview(apiName: string, method: string, path: string) {
    return [
      {
        api_name: apiName || '',
        method: method || '',
        endpoint: path || ''
      }
    ];
  }

  async function loadDynamicSourceCatalog() {
    sourceCatalogLoading = true;
    sourceCatalogError = '';
    try {
      const [apiConfigsRaw, tablesRaw] = await Promise.all([
        fetch(`${API_BASE}/api-configs`, {
          method: 'GET',
          headers: { Accept: 'application/json', 'X-AO-ROLE': API_ROLE }
        }),
        fetch(`${API_BASE}/tables`, {
          method: 'GET',
          headers: { Accept: 'application/json', 'X-AO-ROLE': API_ROLE }
        })
      ]);

      let apiJson: any = {};
      let tableJson: any = {};
      try {
        apiJson = await apiConfigsRaw.json();
      } catch {
        apiJson = {};
      }
      try {
        tableJson = await tablesRaw.json();
      } catch {
        tableJson = {};
      }

      const apiRows = Array.isArray(apiJson?.api_configs) ? apiJson.api_configs : [];
      dynamicApiSources = apiRows.map((row: any, idx: number) => {
        const name = String(row?.api_name || row?.name || '').trim() || `API template ${idx + 1}`;
        const method = String(row?.method || 'GET').trim().toUpperCase();
        const baseUrl = String(row?.base_url || '').trim();
        const path = String(row?.path || '').trim();
        const finalPath = path ? (path.startsWith('/') ? path : `/${path}`) : '/';
        const templateUrl = `${baseUrl}${finalPath}`;
        return {
          id: `api_tpl_${String(row?.id || idx + 1)}`,
          name,
          group: 'api_requests' as SourceGroup,
          datasetId: `api_template:${String(row?.id || idx + 1)}`,
          schema: ['request', 'response', 'status'],
          size: 100,
          preview: buildApiTemplatePreview(name, method, templateUrl),
          description: `Преднастроенный API-шаблон (${method} ${templateUrl})`,
          requestTemplate: {
            method,
            url: templateUrl,
            authMode: String(row?.auth_mode || 'manual'),
            headers: row?.headers_json && typeof row.headers_json === 'object' ? row.headers_json : {},
            query: row?.query_json && typeof row.query_json === 'object' ? row.query_json : {},
            body: row?.body_json !== undefined ? row.body_json : {}
          }
        };
      });

      const existingTables = Array.isArray(tableJson?.existing_tables) ? tableJson.existing_tables : [];
      const layerTables = existingTables.filter((t: any) =>
        Boolean(normalizeLayer(String(t?.schema_name || ''), String(t?.table_name || '')))
      );
      dynamicTableSources = layerTables.map((t: any, idx: number) => {
        const schema = String(t?.schema_name || '').trim();
        const table = String(t?.table_name || '').trim();
        const layer = normalizeLayer(schema, table);
        return {
          id: `mart_tbl_${idx + 1}_${schema}_${table}`,
          name: `${schema}.${table}`,
          group: 'data_tables' as SourceGroup,
          datasetId: `db:${schema}.${table}`,
          schema: ['*'],
          size: 0,
          preview: [{ layer, schema, table }],
          description: `Таблица витрины данных уровня ${layer}`
        };
      });
    } catch (e: any) {
      sourceCatalogError = String(e?.message || e || 'Не удалось загрузить каталоги источников');
    } finally {
      sourceCatalogLoading = false;
    }
  }

  function dropPoint(clientX: number, clientY: number) {
    const r = canvasEl.getBoundingClientRect();
    return { x: (clientX - r.left - panX) / zoom, y: (clientY - r.top - panY) / zoom };
  }

  function onDrop(event: DragEvent) {
    event.preventDefault();
    const p = dropPoint(event.clientX, event.clientY);
    const sourceRaw = event.dataTransfer?.getData('application/x-workflow-source');
    if (sourceRaw) {
      const item = JSON.parse(sourceRaw) as SourceItem;
      const config: any = { ...item };
      if (item.group === 'api_requests') {
        config.apiRequest = normalizeApiRequest(item.requestTemplate);
      }
      nodes = [...nodes, { id: uid('node'), type: 'data', x: p.x, y: p.y, config }];
      return;
    }
    const toolRaw = event.dataTransfer?.getData('application/x-workflow-tool');
    if (toolRaw) {
      const item = JSON.parse(toolRaw) as ToolItem;
      const settings = defaultSettings(item.toolType);
      if (item.toolType === 'api_request') {
        const firstTemplate = dynamicApiSources[0];
        if (firstTemplate) {
          const tpl = normalizeApiRequest(firstTemplate.requestTemplate);
          settings.templateId = firstTemplate.id;
          settings.apiMethod = tpl.method;
          settings.apiUrl = tpl.url;
          settings.apiAuthMode = tpl.authMode;
          settings.apiHeaders = tpl.headersText;
          settings.apiQuery = tpl.queryText;
          settings.apiBody = tpl.bodyText;
        }
      }
      nodes = [...nodes, { id: uid('node'), type: 'tool', x: p.x, y: p.y, config: { name: item.name, toolType: item.toolType, settings } }];
    }
  }

  function onDragOver(event: DragEvent) {
    event.preventDefault();
  }

  function onWheel(event: WheelEvent) {
    event.preventDefault();
    zoom = Math.max(0.5, Math.min(1.8, zoom - Math.sign(event.deltaY) * 0.08));
  }

  function startPan(event: MouseEvent) {
    if ((event.target as HTMLElement).closest('.node')) return;
    isPanning = true;
    panStartX = event.clientX - panX;
    panStartY = event.clientY - panY;
  }
  function stopAll() {
    isPanning = false;
    dragNode = null;
  }
  function onMouseMove(event: MouseEvent) {
    if (dragNode) {
      const p = dropPoint(event.clientX, event.clientY);
      nodes = nodes.map((n) => (n.id === dragNode?.id ? { ...n, x: p.x - dragNode.offsetX, y: p.y - dragNode.offsetY } : n));
      return;
    }
    if (!isPanning) return;
    panX = event.clientX - panStartX;
    panY = event.clientY - panStartY;
  }

  function startNodeDrag(event: MouseEvent, n: WorkflowNode) {
    if (event.detail > 1) return;
    const p = dropPoint(event.clientX, event.clientY);
    dragNode = { id: n.id, offsetX: p.x - n.x, offsetY: p.y - n.y };
  }

  function inputs(n: WorkflowNode) {
    if (isApiNode(n)) return ['in'];
    return n.type === 'tool' ? toolPorts[toolCfg(n).toolType].inputs : [];
  }
  function outputs(n: WorkflowNode) {
    if (isApiNode(n)) return ['out'];
    return n.type === 'tool' ? toolPorts[toolCfg(n).toolType].outputs : ['out'];
  }
  function canConnect(fromId: string, fromPort: string, toId: string, toPort: string) {
    if (fromId === toId) return false;
    const from = nodes.find((n) => n.id === fromId);
    const to = nodes.find((n) => n.id === toId);
    if (!from || !to) return false;
    if (!outputs(from).includes(fromPort) || !inputs(to).includes(toPort)) return false;
    if (edges.some((e) => e.to === toId && e.toPort === toPort)) return false;
    return true;
  }
  function connectFrom(nodeId: string, port: string) {
    linkFrom = { nodeId, port };
    banner = '';
  }
  function connectTo(nodeId: string, port: string) {
    if (!linkFrom) return;
    if (!canConnect(linkFrom.nodeId, linkFrom.port, nodeId, port)) {
      banner = 'Нельзя соединить выбранные порты';
      linkFrom = null;
      return;
    }
    edges = [...edges, { id: uid('edge'), from: linkFrom.nodeId, to: nodeId, fromPort: linkFrom.port, toPort: port }];
    linkFrom = null;
  }

  function deleteNode(id: string) {
    nodes = nodes.filter((n) => n.id !== id);
    edges = edges.filter((e) => e.from !== id && e.to !== id);
    selectedNodeId = selectedNodeId === id ? '' : selectedNodeId;
  }
  function deleteEdge(id: string) {
    edges = edges.filter((e) => e.id !== id);
  }

  function openNodeSettings(nodeId: string) {
    const node = nodes.find((n) => n.id === nodeId);
    if (!node) return;
    const isStartTool = node.type === 'tool' && toolCfg(node).toolType === 'start_process';
    if (!isStartTool && !isApiNode(node) && !isApiToolNode(node)) {
      banner = 'Для этого узла пока нет отдельной настройки по двойному клику';
      return;
    }
    selectedNodeId = nodeId;
    settingsNodeId = nodeId;
    settingsModalOpen = true;
  }

  function closeSettingsModal() {
    settingsModalOpen = false;
    settingsNodeId = '';
  }

  function center(n: WorkflowNode) {
    return { x: n.x + 125, y: n.y + 55 };
  }

  function topo(): WorkflowNode[] | null {
    const indeg = new Map(nodes.map((n) => [n.id, 0]));
    const byId = new Map(nodes.map((n) => [n.id, n]));
    edges.forEach((e) => indeg.set(e.to, Number(indeg.get(e.to) || 0) + 1));
    const q = Array.from(indeg.entries()).filter(([, d]) => d === 0).map(([id]) => id);
    const out: WorkflowNode[] = [];
    while (q.length) {
      const id = String(q.shift());
      const n = byId.get(id);
      if (!n) continue;
      out.push(n);
      edges.filter((e) => e.from === id).forEach((e) => {
        const d = Number(indeg.get(e.to) || 0) - 1;
        indeg.set(e.to, d);
        if (d === 0) q.push(e.to);
      });
    }
    return out.length === nodes.length ? out : null;
  }

  function validate() {
    const out: WorkflowIssue[] = [];
    if (!nodes.some((n) => n.type === 'data')) out.push({ level: 'error', text: 'Нет источников данных' });
    if (!nodes.some((n) => n.type === 'tool' && toolCfg(n).toolType === 'start_process')) out.push({ level: 'error', text: 'Нет узла Старт процесса' });
    if (!nodes.some((n) => n.type === 'tool' && toolCfg(n).toolType === 'end_process')) out.push({ level: 'error', text: 'Нет узла Конец процесса' });
    nodes.filter((n) => n.type === 'tool' && toolCfg(n).toolType === 'db_write').forEach((n) => {
      if (!String(toolCfg(n).settings.targetTable || '').trim()) out.push({ level: 'error', text: `Узел ${toolCfg(n).name} без таблицы записи` });
    });
    if (!edges.length) out.push({ level: 'warn', text: 'Нет связей между узлами' });
    return out;
  }

  function runWorkflow() {
    nodeRuntime = {};
    edgeRows = {};
    issues = validate();
    if (issues.some((x) => x.level === 'error')) {
      banner = 'Исправь ошибки схемы перед запуском';
      return;
    }
    const order = topo();
    if (!order) {
      issues = [...issues, { level: 'error', text: 'Обнаружен цикл в графе' }];
      banner = 'Обнаружен цикл в графе';
      return;
    }
    for (const n of order) {
      const inEdges = edges.filter((e) => e.to === n.id);
      const inRows = inEdges.reduce((s, e) => s + Number(edgeRows[e.id] || 0), 0);
      let outRows = inRows;
      if (n.type === 'data') {
        if (isApiNode(n)) {
          const baseRows = Math.max(1, Number(n.config.size || 1));
          outRows = inEdges.length ? Math.max(baseRows, inRows || 1) : baseRows;
        } else {
          outRows = inEdges.length ? inRows : Number(n.config.size || 0);
        }
      }
      if (n.type === 'tool') {
        const cfg = toolCfg(n);
        if (cfg.toolType === 'start_process') outRows = inRows > 0 ? inRows : 1;
        if (cfg.toolType === 'schedule_process') outRows = inRows;
        if (cfg.toolType === 'api_request') outRows = inRows > 0 ? inRows : 1;
        if (cfg.toolType === 'table_parser') outRows = Math.max(0, Math.round(inRows * Math.max(0.1, Number(cfg.settings.parserMultiplier || 1))));
        if (cfg.toolType === 'db_write') outRows = Math.max(0, Math.round(inRows * Math.max(0, Math.min(100, Number(cfg.settings.writeSuccessRate || 98))) / 100));
        if (cfg.toolType === 'end_process') outRows = 0;
      }
      const lossRows = Math.max(0, inRows - outRows);
      const lossPct = inRows > 0 ? Number(((lossRows / inRows) * 100).toFixed(2)) : 0;
      const successPct = inRows > 0 ? Number((((inRows - lossRows) / inRows) * 100).toFixed(2)) : n.type === 'tool' && toolCfg(n).toolType !== 'start_process' ? 0 : 100;
      const status: NodeRuntime['status'] = successPct >= 95 ? 'ok' : successPct >= 80 ? 'warn' : 'error';
      nodeRuntime[n.id] = { inRows, outRows, lossRows, lossPct, successPct, status };
      const outs = edges.filter((e) => e.from === n.id);
      const base = outs.length ? Math.floor(outRows / outs.length) : 0;
      let rem = outs.length ? outRows - base * outs.length : 0;
      outs.forEach((e) => {
        edgeRows[e.id] = base + (rem > 0 ? 1 : 0);
        if (rem > 0) rem -= 1;
      });
    }
    const sourceRows = nodes.filter((n) => n.type === 'data' && edges.every((e) => e.to !== n.id)).reduce((s, n) => s + Number(nodeRuntime[n.id]?.outRows || 0), 0);
    const finalRows = nodes.filter((n) => n.type === 'tool' && toolCfg(n).toolType === 'end_process').reduce((s, n) => s + Number(nodeRuntime[n.id]?.inRows || 0), 0);
    const lossRows = Math.max(0, sourceRows - finalRows);
    const lossPct = sourceRows > 0 ? Number(((lossRows / sourceRows) * 100).toFixed(2)) : 0;
    const successPct = sourceRows > 0 ? Number((((sourceRows - lossRows) / sourceRows) * 100).toFixed(2)) : 100;
    summary = { sourceRows, finalRows, lossRows, lossPct, successPct };
    banner = `Симуляция завершена. Успешность ${successPct}%`;
  }

  function updateSetting(nodeId: string, key: string, value: string) {
    nodes = nodes.map((n) => (n.id === nodeId && n.type === 'tool' ? { ...n, config: { ...toolCfg(n), settings: { ...toolCfg(n).settings, [key]: value } } } : n));
  }

  function updateToolName(nodeId: string, value: string) {
    nodes = nodes.map((n) => (n.id === nodeId && n.type === 'tool' ? { ...n, config: { ...toolCfg(n), name: value } } : n));
  }

  function updateDataNodeConfig(nodeId: string, patch: Record<string, any>) {
    nodes = nodes.map((n) => (n.id === nodeId && n.type === 'data' ? { ...n, config: { ...n.config, ...patch } } : n));
  }

  function updateApiNodeRequest(nodeId: string, key: keyof ApiNodeRequest, value: string) {
    const targetNode = nodes.find((n) => n.id === nodeId);
    if (isApiToolNode(targetNode)) {
      updateApiToolRequest(nodeId, key, value);
      return;
    }
    nodes = nodes.map((n) => {
      if (n.id !== nodeId || n.type !== 'data') return n;
      const current = apiCfg(n).apiRequest || normalizeApiRequest(undefined);
      return { ...n, config: { ...n.config, apiRequest: { ...current, [key]: value } } };
    });
  }

  function onSettingInput(nodeId: string, key: string, event: Event) {
    const target = event.currentTarget as HTMLInputElement | null;
    updateSetting(nodeId, key, target?.value ?? '');
  }

  function inputValue(event: Event) {
    return (event.currentTarget as HTMLInputElement | null)?.value ?? '';
  }

  function selectValue(event: Event) {
    return (event.currentTarget as HTMLSelectElement | null)?.value ?? '';
  }

  function textareaValue(event: Event) {
    return (event.currentTarget as HTMLTextAreaElement | null)?.value ?? '';
  }

  function onNodeDoubleClick(event: MouseEvent, nodeId: string) {
    event.preventDefault();
    event.stopPropagation();
    openNodeSettings(nodeId);
  }

  function parseObjectJsonSafe(label: string, raw: string) {
    const txt = String(raw || '').trim() || '{}';
    let parsed: any = {};
    try {
      parsed = JSON.parse(txt);
    } catch {
      throw new Error(`${label}: некорректный JSON`);
    }
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new Error(`${label}: нужен JSON-объект`);
    }
    return parsed as Record<string, any>;
  }

  function parseAnyJsonSafe(label: string, raw: string) {
    const txt = String(raw || '').trim();
    if (!txt) return {};
    try {
      return JSON.parse(txt);
    } catch {
      throw new Error(`${label}: некорректный JSON`);
    }
  }

  async function executeApiNode(nodeId: string) {
    const node = nodes.find((n) => n.id === nodeId);
    if (!node || (!isApiNode(node) && !isApiToolNode(node))) {
      banner = 'Этот узел не поддерживает Execute Node';
      return;
    }

    const request = getApiRequestForNode(node);
    const method = String(request.method || 'GET').trim().toUpperCase() || 'GET';
    const urlRaw = String(request.url || '').trim();
    if (!urlRaw) {
      banner = 'Укажи URL в настройке API-узла';
      return;
    }

    try {
      nodeExecutionLoading = { ...nodeExecutionLoading, [nodeId]: true };
      const headersObj = parseObjectJsonSafe('Headers JSON', request.headersText);
      const queryObj = parseObjectJsonSafe('Query JSON', request.queryText);
      const bodyObj = parseAnyJsonSafe('Body JSON', request.bodyText);

      const url = new URL(urlRaw);
      Object.entries(queryObj).forEach(([k, v]) => {
        if (v !== undefined && v !== null) url.searchParams.set(k, String(v));
      });

      const startTs = Date.now();
      const resp = await fetch(url.toString(), {
        method,
        headers: headersObj,
        body: method === 'GET' || method === 'DELETE' ? undefined : JSON.stringify(bodyObj)
      });
      const durationMs = Date.now() - startTs;
      const responseHeaders: Record<string, string> = {};
      resp.headers.forEach((v, k) => (responseHeaders[k] = v));
      const text = await resp.text();
      let responseBody: any = text;
      try {
        responseBody = text ? JSON.parse(text) : {};
      } catch {
        responseBody = text;
      }

      nodeExecutions = {
        ...nodeExecutions,
        [nodeId]: {
          startedAt: new Date(startTs).toISOString(),
          durationMs,
          status: resp.status,
          ok: resp.ok,
          request: {
            method,
            url: url.toString(),
            headers: headersObj,
            query: queryObj,
            body: bodyObj
          },
          response: {
            headers: responseHeaders,
            body: responseBody
          }
        }
      };
      banner = resp.ok ? `Узел выполнен: ${resp.status}` : `Узел вернул ошибку: ${resp.status}`;
    } catch (e: any) {
      nodeExecutions = {
        ...nodeExecutions,
        [nodeId]: {
          startedAt: new Date().toISOString(),
          durationMs: 0,
          status: 0,
          ok: false,
          request: {
            method: String(request.method || 'GET').trim().toUpperCase() || 'GET',
            url: String(request.url || '').trim(),
            headers: {},
            query: {},
            body: {}
          },
          response: { headers: {}, body: null },
          error: String(e?.message || e || 'Ошибка выполнения узла')
        }
      };
      banner = String(e?.message || e || 'Ошибка выполнения узла');
    } finally {
      nodeExecutionLoading = { ...nodeExecutionLoading, [nodeId]: false };
    }
  }

  function openFullApiBuilder(nodeId: string) {
    const node = nodes.find((n) => n.id === nodeId);
    if (!node || (!isApiNode(node) && !isApiToolNode(node))) return;
    const req = getApiRequestForNode(node);
    const templateId = nodeTemplateId(node);
    const payload = {
      nodeId,
      templateId,
      nodeName: String(node.config?.name || '').trim(),
      request: {
        method: req.method,
        url: req.url,
        authMode: req.authMode,
        headers: req.headersText,
        query: req.queryText,
        body: req.bodyText
      },
      createdAt: new Date().toISOString()
    };
    try {
      localStorage.setItem('ao_workflow_api_handoff', JSON.stringify(payload));
    } catch {
      // ignore storage errors
    }
    window.location.hash = '#desk/workflow';
  }

  function resetCanvas() {
    nodes = [];
    edges = [];
    selectedNodeId = '';
    linkFrom = null;
    nodeRuntime = {};
    edgeRows = {};
    nodeExecutions = {};
    nodeExecutionLoading = {};
    issues = [];
    summary = { sourceRows: 0, finalRows: 0, lossRows: 0, lossPct: 0, successPct: 0 };
    banner = '';
  }

  onMount(() => {
    resetCanvas();
    loadDynamicSourceCatalog();
  });
</script>

<section class="panel workflow-v2">
  <h3>Workflow-конструктор данных</h3>

  <div class="topbar">
    <button on:click={() => (issues = validate())}>Проверить схему</button>
    <button class="primary" on:click={runWorkflow}>Смоделировать запуск</button>
    <button on:click={resetCanvas}>Очистить</button>
    <div class="summary">
      <span>Источник: {summary.sourceRows}</span>
      <span>Выход: {summary.finalRows}</span>
      <span>Потери: {summary.lossRows} ({summary.lossPct}%)</span>
      <span>Успех: {summary.successPct}%</span>
    </div>
  </div>

  <div class="workspace">
    <aside class="sources">
      <div class="source-head">
        <h4>Источники данных</h4>
        <button class="mini" on:click={loadDynamicSourceCatalog} disabled={sourceCatalogLoading}>
          {sourceCatalogLoading ? '...' : 'Обновить'}
        </button>
      </div>
      <div class="tabs">
        {#each sourceGroups as g (g.key)}
          <button class:active={activeTab === g.key} on:click={() => (activeTab = g.key)}>{g.label}</button>
        {/each}
      </div>
      {#if sourceCatalogError}
        <div class="source-error">{sourceCatalogError}</div>
      {/if}
      <div class="items">
        {#each sourceList as item (item.id)}
          <button draggable="true" class="item source" on:dragstart={(e) => e.dataTransfer?.setData('application/x-workflow-source', JSON.stringify(item))}>
            <strong>{item.name}</strong>
            <span>{item.datasetId}</span>
            <span>Строк: {item.size}</span>
          </button>
        {/each}
      </div>
    </aside>

    <div class="canvas-wrap" bind:this={canvasEl} on:drop={onDrop} on:dragover={onDragOver} on:wheel={onWheel} on:mousedown={startPan} on:mousemove={onMouseMove} on:mouseup={stopAll} on:mouseleave={stopAll}>
      <div class="canvas" style={`transform: translate(${panX}px, ${panY}px) scale(${zoom});`}>
        <svg class="edges" width="3000" height="2000">
          {#each edges as edge (edge.id)}
            {@const fromNode = nodes.find((n) => n.id === edge.from)}
            {@const toNode = nodes.find((n) => n.id === edge.to)}
            {#if fromNode && toNode}
              {@const a = center(fromNode)}
              {@const b = center(toNode)}
              <path d={`M ${a.x} ${a.y} C ${a.x + 90} ${a.y}, ${b.x - 90} ${b.y}, ${b.x} ${b.y}`} stroke="#64748b" fill="none" stroke-width="2" />
              <text x={(a.x + b.x) / 2} y={(a.y + b.y) / 2 - 6} text-anchor="middle" class="edge-label">{Number(edgeRows[edge.id] || 0)} строк</text>
            {/if}
          {/each}
        </svg>

        {#each nodes as node (node.id)}
          {@const rt = nodeRuntime[node.id]}
          <div
            class="node {node.type} {selectedNodeId === node.id ? 'selected' : ''}"
            style={`left:${node.x}px;top:${node.y}px;`}
            on:mousedown={(e) => startNodeDrag(e, node)}
            on:click={() => (selectedNodeId = node.id)}
            on:dblclick={(e) => onNodeDoubleClick(e, node.id)}
          >
            <div class="node-head">
              <strong>{node.config.name}</strong>
              <button on:click|stopPropagation={() => deleteNode(node.id)}>x</button>
            </div>
            <div class="node-meta">{node.type === 'data' ? node.config.datasetId : node.config.toolType}</div>
            {#if isApiNode(node) || isApiToolNode(node)}
              {@const req = getApiRequestForNode(node)}
              <div class="api-meta">{req.method || 'GET'} {req.url || ''}</div>
            {/if}
            {#if rt}
              <div class="runtime {rt.status}">in {rt.inRows} | out {rt.outRows} | loss {rt.lossRows} ({rt.lossPct}%)</div>
            {/if}
            <div class="ports">
              <div>{#each inputs(node) as p (p)}<button class="port in" on:click|stopPropagation={() => connectTo(node.id, p)}>{p}</button>{/each}</div>
              <div>{#each outputs(node) as p (p)}<button class="port out {linkFrom?.nodeId === node.id ? 'active' : ''}" on:click|stopPropagation={() => connectFrom(node.id, p)}>{p}</button>{/each}</div>
            </div>
          </div>
        {/each}
      </div>
      {#if banner}<div class="banner">{banner}</div>{/if}
    </div>

    <aside class="tools">
      <h4>Инструменты работы с данными</h4>
      <div class="items">
        {#each tools as t (t.id)}
          <button draggable="true" class="item tool" on:dragstart={(e) => e.dataTransfer?.setData('application/x-workflow-tool', JSON.stringify(t))}>
            <strong>{t.name}</strong>
            <span>{t.description}</span>
          </button>
        {/each}
      </div>

      <section class="props">
        <h5>Свойства</h5>
        {#if selectedNode && selectedNode.type === 'tool'}
          {#if toolCfg(selectedNode).toolType === 'schedule_process'}
            <label>Cron<input value={toolCfg(selectedNode).settings.cron || ''} on:input={(e) => onSettingInput(selectedNode.id, 'cron', e)} /></label>
          {/if}
          {#if toolCfg(selectedNode).toolType === 'table_parser'}
            <label>Множитель парсинга<input value={toolCfg(selectedNode).settings.parserMultiplier || ''} on:input={(e) => onSettingInput(selectedNode.id, 'parserMultiplier', e)} /></label>
          {/if}
          {#if toolCfg(selectedNode).toolType === 'db_write'}
            <label>Схема<input value={toolCfg(selectedNode).settings.targetSchema || ''} on:input={(e) => onSettingInput(selectedNode.id, 'targetSchema', e)} /></label>
            <label>Таблица<input value={toolCfg(selectedNode).settings.targetTable || ''} on:input={(e) => onSettingInput(selectedNode.id, 'targetTable', e)} /></label>
            <label>Успешная запись, %<input value={toolCfg(selectedNode).settings.writeSuccessRate || ''} on:input={(e) => onSettingInput(selectedNode.id, 'writeSuccessRate', e)} /></label>
          {/if}
        {:else}
          <div class="empty">Выбери узел-инструмент на canvas</div>
        {/if}
      </section>

      <section class="props">
        <h5>Связи</h5>
        {#if edges.length}
          {#each edges as e (e.id)}
            <div class="edge-row"><span>{e.fromPort} -> {e.toPort}</span><button on:click={() => deleteEdge(e.id)}>Удалить</button></div>
          {/each}
        {:else}
          <div class="empty">Связей нет</div>
        {/if}
      </section>

      <section class="props">
        <h5>Ошибки и предупреждения</h5>
        {#if issues.length}
          {#each issues as i, idx (idx)}
            <div class="issue {i.level}">{i.text}</div>
          {/each}
        {:else}
          <div class="empty">Нет</div>
        {/if}
      </section>
    </aside>
  </div>

  {#if settingsModalOpen && settingsNode}
    <div class="node-modal-backdrop" on:click={closeSettingsModal}></div>
    <div class="node-modal">
      <div class="node-modal-head">
        <h4>Настройка узла: {settingsNode.config.name}</h4>
        <button class="close-btn" on:click={closeSettingsModal}>x</button>
      </div>

      {#if settingsNode.type === 'tool' && toolCfg(settingsNode).toolType === 'start_process'}
        <div class="node-modal-body">
          <div class="help">Интервал запуска в стиле n8n: каждые N секунд/минут/часов.</div>
          <label>
            Тип запуска
            <select
              value={toolCfg(settingsNode).settings.triggerType || 'interval'}
              on:change={(e) => updateSetting(settingsNode.id, 'triggerType', selectValue(e))}
            >
              <option value="interval">Интервал</option>
              <option value="manual">Ручной</option>
            </select>
          </label>
          {#if (toolCfg(settingsNode).settings.triggerType || 'interval') === 'interval'}
            <div class="interval-grid">
              <label>
                Каждые
                <input
                  type="number"
                  min="1"
                  value={toolCfg(settingsNode).settings.intervalValue || '1'}
                  on:input={(e) => onSettingInput(settingsNode.id, 'intervalValue', e)}
                />
              </label>
              <label>
                Единица
                <select
                  value={toolCfg(settingsNode).settings.intervalUnit || 'minutes'}
                  on:change={(e) => updateSetting(settingsNode.id, 'intervalUnit', selectValue(e))}
                >
                  <option value="seconds">Секунды</option>
                  <option value="minutes">Минуты</option>
                  <option value="hours">Часы</option>
                </select>
              </label>
            </div>
          {/if}
        </div>
      {/if}

      {#if isApiNode(settingsNode) || isApiToolNode(settingsNode)}
        <div class="node-modal-body">
          <div class="help">Легкая поднастройка API-узла: шаблон, метод, URL и JSON-поля. Ниже можно выполнить один запрос (Execute Node) и посмотреть сырой request/response.</div>
          <label>
            Шаблон API
            <select value={nodeTemplateId(settingsNode)} on:change={(e) => applyTemplateToNode(settingsNode.id, selectValue(e))}>
              <option value="">Без шаблона</option>
              {#each dynamicApiSources as src (src.id)}
                <option value={src.id}>{src.name}</option>
              {/each}
            </select>
          </label>
          <label>
            Название узла
            <input
              value={settingsNode.config.name || ''}
              on:input={(e) => (settingsNode.type === 'data' ? updateDataNodeConfig(settingsNode.id, { name: inputValue(e) }) : updateToolName(settingsNode.id, inputValue(e)))}
            />
          </label>
          <div class="interval-grid">
            <label>
              Метод
              <select
                value={getApiRequestForNode(settingsNode).method || 'GET'}
                on:change={(e) => updateApiNodeRequest(settingsNode.id, 'method', selectValue(e))}
              >
                <option value="GET">GET</option>
                <option value="POST">POST</option>
                <option value="PUT">PUT</option>
                <option value="PATCH">PATCH</option>
                <option value="DELETE">DELETE</option>
              </select>
            </label>
            <label>
              Режим авторизации
              <select
                value={getApiRequestForNode(settingsNode).authMode || 'manual'}
                on:change={(e) => updateApiNodeRequest(settingsNode.id, 'authMode', selectValue(e))}
              >
                <option value="manual">Ручная</option>
                <option value="oauth2_client_credentials">OAuth2 (client_credentials)</option>
              </select>
            </label>
          </div>
          <label>
            URL
            <input
              value={getApiRequestForNode(settingsNode).url || ''}
              on:input={(e) => updateApiNodeRequest(settingsNode.id, 'url', inputValue(e))}
            />
          </label>
          <label>
            Headers JSON
            <textarea
              rows="4"
              value={getApiRequestForNode(settingsNode).headersText || '{}'}
              on:input={(e) => updateApiNodeRequest(settingsNode.id, 'headersText', textareaValue(e))}
            ></textarea>
          </label>
          <label>
            Query JSON
            <textarea
              rows="4"
              value={getApiRequestForNode(settingsNode).queryText || '{}'}
              on:input={(e) => updateApiNodeRequest(settingsNode.id, 'queryText', textareaValue(e))}
            ></textarea>
          </label>
          <label>
            Body JSON
            <textarea
              rows="6"
              value={getApiRequestForNode(settingsNode).bodyText || '{}'}
              on:input={(e) => updateApiNodeRequest(settingsNode.id, 'bodyText', textareaValue(e))}
            ></textarea>
          </label>
          <div class="actions-row">
            <button class="primary-action" on:click={() => executeApiNode(settingsNode.id)} disabled={settingsApiLoading}>
              {settingsApiLoading ? 'Выполнение...' : 'Execute Node'}
            </button>
            <button on:click={() => openFullApiBuilder(settingsNode.id)}>Открыть полный конструктор API</button>
          </div>
          {#if settingsApiExecution}
            <div class="exec-block">
              <div class="exec-head">
                <span>Статус: {settingsApiExecution.status || '-'}</span>
                <span>Время: {settingsApiExecution.durationMs} мс</span>
                <span>Запуск: {settingsApiExecution.startedAt}</span>
              </div>
              {#if settingsApiExecution.error}
                <div class="issue error">{settingsApiExecution.error}</div>
              {/if}
              <label>
                RAW request
                <textarea readonly rows="10" value={JSON.stringify(settingsApiExecution.request, null, 2)}></textarea>
              </label>
              <label>
                RAW response
                <textarea readonly rows="12" value={JSON.stringify(settingsApiExecution.response, null, 2)}></textarea>
              </label>
            </div>
          {/if}
        </div>
      {/if}
    </div>
  {/if}
</section>

<style>
  .workflow-v2 { display: flex; flex-direction: column; gap: 10px; }
  .topbar { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
  .topbar button { border: 1px solid #dbe4f0; background: #fff; border-radius: 9px; padding: 6px 10px; cursor: pointer; }
  .topbar .primary { background: #0f172a; color: #fff; border-color: #0f172a; }
  .summary { display: flex; gap: 8px; font-size: 12px; color: #334155; }
  .workspace { display: grid; grid-template-columns: 280px minmax(0, 1fr) 360px; gap: 10px; min-height: 720px; }
  .sources,.tools { border: 1px solid #e2e8f0; border-radius: 12px; padding: 10px; background: #f8fbff; display: flex; flex-direction: column; gap: 8px; overflow: auto; }
  .source-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
  .mini { border: 1px solid #dbe4f0; background: #fff; border-radius: 8px; padding: 4px 8px; font-size: 11px; cursor: pointer; }
  .source-error { border: 1px solid #fecaca; background: #fef2f2; color: #991b1b; border-radius: 8px; padding: 6px 8px; font-size: 11px; }
  .tabs { display: grid; gap: 6px; }
  .tabs button { border: 1px solid #dbe4f0; background: #fff; border-radius: 8px; padding: 6px; text-align: left; cursor: pointer; }
  .tabs button.active { background: #0f172a; color: #fff; }
  .items { display: flex; flex-direction: column; gap: 6px; }
  .item { text-align: left; border: 1px solid #dbe4f0; background: #fff; border-radius: 8px; padding: 7px; cursor: grab; display: flex; flex-direction: column; gap: 2px; }
  .item span { font-size: 11px; color: #64748b; }
  .item.tool { background: #fff7ed; border-color: #fed7aa; }
  .canvas-wrap { position: relative; border: 1px solid #dbe4f0; border-radius: 12px; overflow: hidden; background: #fff; }
  .canvas { position: relative; width: 3000px; height: 2000px; transform-origin: 0 0; background-image: linear-gradient(#eef2f7 1px, transparent 1px), linear-gradient(90deg, #eef2f7 1px, transparent 1px); background-size: 24px 24px; }
  .edges { position: absolute; inset: 0; pointer-events: none; }
  .edge-label { fill: #334155; font-size: 11px; }
  .node { position: absolute; width: 250px; border: 1px solid #cbd5e1; border-radius: 10px; background: #fff; padding: 8px; box-shadow: 0 4px 12px rgba(15,23,42,.07); }
  .node.tool { background: #fff7ed; }
  .node.data { background: #f8fafc; }
  .node.selected { border-color: #2563eb; }
  .node-head { display: flex; justify-content: space-between; gap: 8px; }
  .node-head button { border: none; background: transparent; cursor: pointer; color: #64748b; }
  .node-meta { color: #64748b; font-size: 11px; margin-top: 4px; }
  .api-meta { color: #0f172a; font-size: 10px; margin-top: 3px; line-height: 1.3; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .runtime { margin-top: 6px; font-size: 11px; border-radius: 7px; padding: 4px 6px; border: 1px solid #e2e8f0; }
  .runtime.ok { background: #f0fdf4; border-color: #86efac; }
  .runtime.warn { background: #fffbeb; border-color: #fcd34d; }
  .runtime.error { background: #fef2f2; border-color: #fca5a5; }
  .ports { margin-top: 7px; display: flex; justify-content: space-between; }
  .port { border: 1px solid #cbd5e1; border-radius: 999px; background: #fff; cursor: pointer; padding: 2px 7px; font-size: 10px; }
  .port.in { border-color: #f59e0b; } .port.out { border-color: #2563eb; } .port.active { background: #dbeafe; }
  .banner { position: absolute; left: 10px; bottom: 10px; background: #0f172a; color: #fff; border-radius: 7px; padding: 6px 9px; font-size: 12px; }
  .props { border-top: 1px solid #e2e8f0; margin-top: 6px; padding-top: 7px; display: flex; flex-direction: column; gap: 6px; }
  .props h5 { margin: 0; font-size: 13px; }
  .props label { display: flex; flex-direction: column; gap: 3px; font-size: 12px; }
  .props input { border: 1px solid #dbe4f0; border-radius: 7px; padding: 6px; }
  .edge-row { border: 1px solid #e2e8f0; border-radius: 7px; background: #fff; padding: 6px; display: flex; justify-content: space-between; gap: 8px; font-size: 11px; }
  .edge-row button { border: 1px solid #dbe4f0; border-radius: 6px; background: #fff; cursor: pointer; }
  .issue { border-radius: 7px; border: 1px solid #e2e8f0; background: #fff; padding: 6px; font-size: 12px; }
  .issue.error { border-color: #fecaca; background: #fef2f2; }
  .issue.warn { border-color: #fde68a; background: #fffbeb; }
  .issue.info { border-color: #bfdbfe; background: #eff6ff; }
  .empty { color: #64748b; font-size: 12px; }
  .node-modal-backdrop { position: fixed; inset: 0; background: rgba(15, 23, 42, 0.48); z-index: 40; }
  .node-modal { position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%); width: min(760px, calc(100vw - 32px)); max-height: calc(100vh - 48px); overflow: auto; background: #fff; border: 1px solid #dbe4f0; border-radius: 14px; box-shadow: 0 18px 48px rgba(15, 23, 42, 0.28); z-index: 41; }
  .node-modal-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; padding: 12px 14px; border-bottom: 1px solid #e2e8f0; position: sticky; top: 0; background: #fff; }
  .node-modal-head h4 { margin: 0; font-size: 16px; }
  .close-btn { border: 1px solid #dbe4f0; border-radius: 9px; background: #fff; cursor: pointer; padding: 4px 10px; }
  .node-modal-body { padding: 12px 14px; display: flex; flex-direction: column; gap: 10px; }
  .node-modal-body label { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: #334155; }
  .node-modal-body input,
  .node-modal-body select,
  .node-modal-body textarea { border: 1px solid #dbe4f0; border-radius: 8px; padding: 7px 8px; font-family: inherit; font-size: 13px; }
  .node-modal-body textarea { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
  .interval-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
  .help { font-size: 12px; color: #475569; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 8px; }
  .actions-row { display: flex; gap: 8px; flex-wrap: wrap; }
  .actions-row button { border: 1px solid #dbe4f0; background: #fff; border-radius: 8px; padding: 7px 10px; cursor: pointer; }
  .actions-row .primary-action { background: #0f172a; color: #fff; border-color: #0f172a; }
  .exec-block { border: 1px solid #e2e8f0; border-radius: 10px; padding: 8px; display: flex; flex-direction: column; gap: 8px; background: #f8fafc; }
  .exec-head { display: flex; gap: 10px; flex-wrap: wrap; font-size: 12px; color: #334155; }
</style>



