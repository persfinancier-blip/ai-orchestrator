<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import JsonTreeView from './JsonTreeView.svelte';
  import ApiBuilderTab from '../tabs/ApiBuilderTab.svelte';
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
  type NodeRuntime = { inRows: number; outRows: number; lossRows: number; lossPct: number; successPct: number; status: 'idle' | 'running' | 'paused' | 'ok' | 'warn' | 'error' | 'stopped' };
  type WorkflowIssue = { level: 'error' | 'warn' | 'info'; text: string };
  type ApiNodeRequest = {
    method: string;
    url: string;
    authMode: string;
    headersText: string;
    queryText: string;
    bodyText: string;
  };
  type ApiPaginationMode = 'none' | 'page_number' | 'offset_limit' | 'cursor';
  type ApiPaginationTarget = 'query' | 'body';
  type ApiNodePagination = {
    enabled: boolean;
    mode: ApiPaginationMode;
    target: ApiPaginationTarget;
    useMaxPages: boolean;
    maxPages: number;
    useDelay: boolean;
    pauseMs: number;
    dataPath: string;
    stopOnMissingValue: boolean;
    stopOnHttpError: boolean;
    stopOnSameResponse: boolean;
    sameResponseLimit: number;
    stopRules: Array<{ id: string; responsePath: string; operator: string; compareValue: string }>;
    pageParam: string;
    startPage: number;
    limitParam: string;
    limitValue: number;
    offsetParam: string;
    startOffset: number;
    cursorReqPath: string;
    cursorResPath: string;
  };
  type ApiNodeExecution = {
    startedAt: string;
    durationMs: number;
    status: number;
    ok: boolean;
    totalRequests: number;
    payloadCount: number;
    payloadSize: number;
    requestPreview: any;
    responsePreview: any;
    error?: string;
  };
  type TemplateParameterCondition = {
    id?: string;
    schema?: string;
    table?: string;
    field?: string;
    operator?: string;
    compareMode?: string;
    compareValue?: string;
    compareColumn?: string;
  };
  type TemplateParameterDefinition = {
    alias: string;
    definition: string;
    sourceSchema: string;
    sourceTable: string;
    sourceField: string;
    conditions: TemplateParameterCondition[];
  };
  type TemplatePaginationParameter = {
    alias: string;
    firstValue: any;
  };
  type DataModelTable = { id: string; schema: string; table: string; alias: string };
  type DataModelJoin = {
    id: string;
    left_table_id: string;
    left_field: string;
    right_table_id: string;
    right_field: string;
    join_type: 'inner' | 'left';
  };
  type DataModelField = { id: string; table_id: string; field: string; alias: string };
  type DataModelFilter = { id: string; table_id: string; field: string; operator: string; compare_value: string };
  type TemplateDataModel = {
    tables: DataModelTable[];
    joins: DataModelJoin[];
    fields: DataModelField[];
    filters: DataModelFilter[];
  };
  type DynamicApiSource = SourceItem & {
    storeId?: number;
    rawRow?: any;
    parameterDefinitions?: TemplateParameterDefinition[];
    paginationParameters?: TemplatePaginationParameter[];
    dataModel?: TemplateDataModel;
  };
  type WorkflowDeskConfig = {
    nodes: WorkflowNode[];
    edges: WorkflowEdge[];
    viewport?: { panX?: number; panY?: number; zoom?: number };
    selectedNodeId?: string;
  };
  type WorkflowDeskRow = {
    id?: number;
    desk_name?: string;
    desk_type?: string;
    config_json?: any;
    schema_version?: number;
    revision?: number;
    description?: string;
    is_active?: boolean;
    updated_at?: string;
    updated_by?: string;
  };
  type ExistingTable = { schema_name: string; table_name: string };
  type DeskProcessSummary = {
    start_node_id: string;
    process_code: string;
    name: string;
    is_enabled: boolean;
    trigger_type: string;
    schedule_value: string;
    timezone: string;
    run_policy: string;
    execution_scope_mode: string;
    scope_type: string;
    scope_ref: string;
    tenant_id: string | null;
    context_json: Record<string, any>;
    scope_source: Record<string, any>;
    input_scope?: string;
    output_scope?: string;
    last_run?: any;
  };
  type SchedulerStateView = {
    enabled: boolean;
    last_tick_at: string;
    last_error: string;
    worker_last_tick_at: string;
    worker_last_error: string;
    queue_depth: number;
    queue_running: number;
    queue_dead_letter: number;
    dependency_event_backlog: number;
    active_workers: number;
  };
  type ProcessRunRow = {
    run_uid: string;
    status: string;
    start_node_id: string;
    process_code: string;
    trigger_type: string;
    started_at: string;
    finished_at?: string;
    error_text?: string;
    scope_type?: string;
    scope_ref?: string;
  };
  type WorkflowJobRow = {
    job_id: number;
    status: string;
    parent_run_uid: string;
    start_node_id: string;
    job_type: string;
    attempt_no: number;
    max_attempts: number;
    error_text?: string;
    created_at?: string;
    updated_at?: string;
  };

  let activeTab: SourceGroup = 'data_tables';
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
  let dynamicApiSources: DynamicApiSource[] = [];
  let dynamicTableSources: SourceItem[] = [];
  let apiBuilderExistingTables: ExistingTable[] = [];
  let apiTemplateStorageRef = 'ao_system.api_configs_store';
  let workflowDeskStorageRef = 'ao_system.workflow_desks_store';
  let settingsNodeId = '';
  let settingsModalOpen = false;
  let nodeExecutions: Record<string, ApiNodeExecution> = {};
  let nodeExecutionLoading: Record<string, boolean> = {};
  let nodeTemplateSaving: Record<string, boolean> = {};
  let settingsRequestViewMode: 'tree' | 'raw' = 'tree';
  let settingsResponseViewMode: 'tree' | 'raw' = 'tree';
  let chainRunActive = false;
  let chainRunPaused = false;
  let chainRunStopRequested = false;
  let chainRunId = 0;
  let chainCurrentNodeId = '';
  let deskId = 0;
  let deskRevision = 0;
  let deskName = 'Рабочий стол данных';
  let deskDescription = '';
  let deskSchemaVersion = 1;
  let deskDirty = false;
  let deskLoading = false;
  let deskSaving = false;
  let deskLastSavedAt = '';
  let deskSaveError = '';
  let deskAutosaveEnabled = true;
  let deskAutosaveSeconds = 10;
  let deskInitialized = false;
  let deskSignatureMute = false;
  let deskLastSavedSignature = '';
  let deskCurrentSignature = '';
  let deskAutosaveTimer: any = null;
  let publishBusy = false;
  let triggerBusy = false;
  let monitorBusy = false;
  let processBusyByNode: Record<string, boolean> = {};
  let schedulerView: SchedulerStateView = {
    enabled: false,
    last_tick_at: '',
    last_error: '',
    worker_last_tick_at: '',
    worker_last_error: '',
    queue_depth: 0,
    queue_running: 0,
    queue_dead_letter: 0,
    dependency_event_backlog: 0,
    active_workers: 0
  };
  let publishedDeskVersionId = 0;
  let publishedDeskReady = false;
  let publishedProcesses: DeskProcessSummary[] = [];
  let processRuns: ProcessRunRow[] = [];
  let workflowJobs: WorkflowJobRow[] = [];

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
  const PARAMETER_TOKEN_RE = /\{\{\s*([^{}]+?)\s*\}\}/g;
  const PARAMETER_TOKEN_EXACT_RE = /^\{\{\s*([^{}]+?)\s*\}\}$/;

  $: selectedNode = nodes.find((n) => n.id === selectedNodeId) ?? null;
  $: settingsNode = nodes.find((n) => n.id === settingsNodeId) ?? null;
  $: settingsApiExecution = settingsNodeId ? nodeExecutions[settingsNodeId] : undefined;
  $: settingsApiLoading = settingsNodeId ? Boolean(nodeExecutionLoading[settingsNodeId]) : false;
  $: settingsPagination = settingsNode ? getPaginationForNode(settingsNode) : defaultApiPagination();
  $: settingsApiBuilderStoreId = (() => {
    if (!settingsNode || (!isApiNode(settingsNode) && !isApiToolNode(settingsNode))) return null;
    const raw = Number(nodeTemplateStoreId(settingsNode) || 0);
    return Number.isFinite(raw) && raw > 0 ? Math.trunc(raw) : null;
  })();
  $: visibleSourceGroups = sourceGroups.filter((g) => g.key !== 'api_requests');
  $: if (activeTab === 'api_requests') activeTab = 'data_tables';
  $: sourceList =
    activeTab === 'data_tables'
      ? dynamicTableSources.length
        ? dynamicTableSources
        : sourceItemsByGroup.data_tables || []
      : sourceItemsByGroup.math_calculations || [];
  $: deskCurrentSignature = deskSignatureFromState({
    nodes,
    edges,
    viewport: { panX, panY, zoom },
    selectedNodeId
  });
  $: if (deskInitialized && !deskSignatureMute) {
    deskDirty = deskCurrentSignature !== deskLastSavedSignature;
  }

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

  function formatBytesShort(value: number | null | undefined) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return '-';
    const bytes = Math.max(0, Number(value));
    if (bytes < 1024) return `${bytes} B`;
    const kb = bytes / 1024;
    if (kb < 1024) return `${Math.round(kb * 10) / 10} KB`;
    const mb = kb / 1024;
    return `${Math.round(mb * 10) / 10} MB`;
  }

  function formatDurationShort(value: number | null | undefined) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return '-';
    const ms = Math.max(0, Number(value));
    if (ms < 1000) return `${ms} мс`;
    return `${Math.round((ms / 1000) * 100) / 100} с`;
  }

  function hashQueryParams() {
    const rawHash = String(window.location.hash || '');
    const idx = rawHash.indexOf('?');
    if (idx < 0) return new URLSearchParams();
    return new URLSearchParams(rawHash.slice(idx + 1));
  }

  function hashParamInt(name: string) {
    const n = Number(hashQueryParams().get(name) || 0);
    return Number.isFinite(n) && n > 0 ? Math.trunc(n) : 0;
  }

  function safeIso(value: any) {
    const txt = String(value || '').trim();
    if (!txt) return '';
    const dt = new Date(txt);
    if (Number.isNaN(dt.getTime())) return '';
    return dt.toISOString();
  }

  function deskSignatureFromState(state: WorkflowDeskConfig) {
    try {
      const normalized = {
        nodes: Array.isArray(state?.nodes) ? state.nodes : [],
        edges: Array.isArray(state?.edges) ? state.edges : [],
        viewport: {
          panX: Number(state?.viewport?.panX || 0),
          panY: Number(state?.viewport?.panY || 0),
          zoom: Number(state?.viewport?.zoom || 1)
        },
        selectedNodeId: String(state?.selectedNodeId || '').trim()
      };
      return JSON.stringify(normalized);
    } catch {
      return '';
    }
  }

  function captureDeskState(): WorkflowDeskConfig {
    return {
      nodes: deepClone(nodes || []),
      edges: deepClone(edges || []),
      viewport: { panX, panY, zoom },
      selectedNodeId
    };
  }

  function normalizeWorkflowNode(raw: any): WorkflowNode | null {
    if (!raw || typeof raw !== 'object') return null;
    const type: 'data' | 'tool' = raw.type === 'tool' ? 'tool' : 'data';
    const id = String(raw.id || uid('node')).trim();
    if (!id) return null;
    const x = Number.isFinite(Number(raw.x)) ? Number(raw.x) : 120;
    const y = Number.isFinite(Number(raw.y)) ? Number(raw.y) : 120;
    if (type === 'tool') {
      const rawToolType = String(raw?.config?.toolType || '').trim();
      const knownTool = tools.find((t) => t.toolType === (rawToolType as ToolType));
      const toolType: ToolType = knownTool ? knownTool.toolType : 'table_parser';
      const base = defaultSettings(toolType);
      const settings =
        raw?.config?.settings && typeof raw.config.settings === 'object' && !Array.isArray(raw.config.settings)
          ? { ...base, ...raw.config.settings }
          : { ...base };
      return {
        id,
        type,
        x,
        y,
        config: {
          name: String(raw?.config?.name || knownTool?.name || 'Узел').trim() || 'Узел',
          toolType,
          settings
        }
      };
    }
    const cfg = raw?.config && typeof raw.config === 'object' ? { ...raw.config } : {};
    if (String(cfg.group || '').trim() === 'api_requests') {
      cfg.apiRequest = normalizeApiRequest(cfg.apiRequest || cfg.requestTemplate);
      cfg.templateId = String(cfg.templateId || '').trim();
      cfg.templateStoreId = String(cfg.templateStoreId || '').trim();
    }
    return { id, type, x, y, config: cfg };
  }

  function normalizeWorkflowEdge(raw: any): WorkflowEdge | null {
    if (!raw || typeof raw !== 'object') return null;
    const id = String(raw.id || uid('edge')).trim();
    const from = String(raw.from || '').trim();
    const to = String(raw.to || '').trim();
    const fromPort = String(raw.fromPort || 'out').trim() || 'out';
    const toPort = String(raw.toPort || 'in').trim() || 'in';
    if (!id || !from || !to) return null;
    return { id, from, to, fromPort, toPort };
  }

  function applyDeskConfig(rawConfig: any) {
    const cfg = rawConfig && typeof rawConfig === 'object' ? rawConfig : {};
    const nextNodes = (Array.isArray(cfg?.nodes) ? cfg.nodes : [])
      .map((n: any) => normalizeWorkflowNode(n))
      .filter((n: WorkflowNode | null): n is WorkflowNode => Boolean(n));
    const nodeIds = new Set(nextNodes.map((n) => n.id));
    const nextEdges = (Array.isArray(cfg?.edges) ? cfg.edges : [])
      .map((e: any) => normalizeWorkflowEdge(e))
      .filter((e: WorkflowEdge | null): e is WorkflowEdge => Boolean(e))
      .filter((e) => nodeIds.has(e.from) && nodeIds.has(e.to));
    deskSignatureMute = true;
    nodes = nextNodes;
    edges = nextEdges;
    selectedNodeId = nodeIds.has(String(cfg?.selectedNodeId || '').trim()) ? String(cfg?.selectedNodeId || '').trim() : '';
    const vp = cfg?.viewport && typeof cfg.viewport === 'object' ? cfg.viewport : {};
    panX = Number.isFinite(Number(vp?.panX)) ? Number(vp.panX) : 0;
    panY = Number.isFinite(Number(vp?.panY)) ? Number(vp.panY) : 0;
    zoom = Number.isFinite(Number(vp?.zoom)) ? Math.max(0.5, Math.min(1.8, Number(vp.zoom))) : 1;
    linkFrom = null;
    settingsNodeId = '';
    settingsModalOpen = false;
    nodeRuntime = {};
    edgeRows = {};
    nodeExecutions = {};
    nodeExecutionLoading = {};
    issues = [];
    summary = { sourceRows: 0, finalRows: 0, lossRows: 0, lossPct: 0, successPct: 0 };
    chainRunActive = false;
    chainRunPaused = false;
    chainRunStopRequested = false;
    chainCurrentNodeId = '';
    deskSignatureMute = false;
  }

  function applyDeskRow(row: WorkflowDeskRow) {
    deskId = Number(row?.id || 0) || 0;
    deskRevision = Math.max(1, Number(row?.revision || 1) || 1);
    deskSchemaVersion = Math.max(1, Number(row?.schema_version || 1) || 1);
    deskName = String(row?.desk_name || '').trim() || 'Рабочий стол данных';
    deskDescription = String(row?.description || '').trim();
    applyDeskConfig(row?.config_json || {});
    const signature = deskSignatureFromState(captureDeskState());
    deskLastSavedSignature = signature;
    deskDirty = false;
    deskLastSavedAt = safeIso(row?.updated_at);
    deskSaveError = '';
  }

  function clearDeskAutosaveTimer() {
    if (deskAutosaveTimer) {
      clearInterval(deskAutosaveTimer);
      deskAutosaveTimer = null;
    }
  }

  async function saveDesk(silent = false) {
    if (deskSaving || deskLoading) return false;
    const safeName = String(deskName || '').trim() || 'Рабочий стол данных';
    const safeDesc = String(deskDescription || '').trim();
    deskName = safeName;
    deskDescription = safeDesc;
    deskSaving = true;
    deskSaveError = '';
    try {
      const response = await fetch(`${API_BASE}/workflow-desks/upsert`, {
        method: 'POST',
        headers: {
          Accept: 'application/json',
          'Content-Type': 'application/json',
          'X-AO-ROLE': API_ROLE
        },
        body: JSON.stringify({
          id: deskId > 0 ? deskId : undefined,
          expected_revision: deskRevision > 0 ? deskRevision : undefined,
          desk_name: safeName,
          desk_type: 'data',
          description: safeDesc,
          schema_version: Math.max(1, Number(deskSchemaVersion || 1) || 1),
          is_active: true,
          updated_by: API_ROLE,
          config_json: captureDeskState()
        })
      });
      let payload: any = {};
      try {
        payload = await response.json();
      } catch {
        payload = {};
      }
      if (!response.ok) {
        const details = String(payload?.details || payload?.error || `${response.status} ${response.statusText}`);
        throw new Error(details);
      }
      deskId = Number(payload?.id || deskId || 0) || 0;
      deskRevision = Math.max(1, Number(payload?.revision || deskRevision + 1 || 1) || 1);
      deskLastSavedSignature = deskSignatureFromState(captureDeskState());
      deskDirty = false;
      deskLastSavedAt = new Date().toISOString();
      if (!silent) banner = 'Рабочий стол сохранен на сервере';
      return true;
    } catch (e: any) {
      const msg = String(e?.message || e || 'Ошибка сохранения рабочего стола');
      deskSaveError = msg;
      if (!silent) banner = msg;
      return false;
    } finally {
      deskSaving = false;
    }
  }

  async function loadWorkflowDeskFromServer() {
    deskLoading = true;
    deskSaveError = '';
    try {
      const desiredDeskId = hashParamInt('desk_id');
      const listResp = await fetch(`${API_BASE}/workflow-desks?desk_type=data&limit=100`, {
        method: 'GET',
        headers: { Accept: 'application/json', 'X-AO-ROLE': API_ROLE }
      });
      let listPayload: any = {};
      try {
        listPayload = await listResp.json();
      } catch {
        listPayload = {};
      }
      if (!listResp.ok) {
        const details = String(listPayload?.details || listPayload?.error || `${listResp.status} ${listResp.statusText}`);
        throw new Error(details);
      }
      const list: WorkflowDeskRow[] = Array.isArray(listPayload?.workflow_desks) ? listPayload.workflow_desks : [];
      let target: WorkflowDeskRow | undefined;
      if (desiredDeskId > 0) {
        target = list.find((r) => Number(r?.id || 0) === desiredDeskId);
        if (!target) {
          const oneResp = await fetch(`${API_BASE}/workflow-desks/${desiredDeskId}`, {
            method: 'GET',
            headers: { Accept: 'application/json', 'X-AO-ROLE': API_ROLE }
          });
          if (oneResp.ok) {
            let onePayload: any = {};
            try {
              onePayload = await oneResp.json();
            } catch {
              onePayload = {};
            }
            if (onePayload?.workflow_desk && typeof onePayload.workflow_desk === 'object') {
              target = onePayload.workflow_desk as WorkflowDeskRow;
            }
          }
        }
      }
      if (!target && list.length) target = list[0];
      if (!target) {
        const created = await fetch(`${API_BASE}/workflow-desks/upsert`, {
          method: 'POST',
          headers: {
            Accept: 'application/json',
            'Content-Type': 'application/json',
            'X-AO-ROLE': API_ROLE
          },
          body: JSON.stringify({
            desk_name: deskName || 'Рабочий стол данных',
            desk_type: 'data',
            description: '',
            schema_version: 1,
            is_active: true,
            updated_by: API_ROLE,
            config_json: captureDeskState()
          })
        });
        let createdPayload: any = {};
        try {
          createdPayload = await created.json();
        } catch {
          createdPayload = {};
        }
        if (!created.ok) {
          const details = String(
            createdPayload?.details || createdPayload?.error || `${created.status} ${created.statusText}`
          );
          throw new Error(details);
        }
        deskId = Number(createdPayload?.id || 0) || 0;
        deskRevision = Math.max(1, Number(createdPayload?.revision || 1) || 1);
        deskLastSavedAt = new Date().toISOString();
        deskLastSavedSignature = deskSignatureFromState(captureDeskState());
        deskDirty = false;
        banner = `Создан новый рабочий стол (ID ${deskId})`;
        return true;
      }
      applyDeskRow(target);
      banner = `Рабочий стол загружен (ID ${deskId})`;
      return true;
    } catch (e: any) {
      const msg = String(e?.message || e || 'Ошибка загрузки рабочего стола');
      deskSaveError = msg;
      banner = msg;
      return false;
    } finally {
      deskLoading = false;
    }
  }

  async function reloadDeskFromServer() {
    await loadWorkflowDeskFromServer();
    await refreshAutomationContour();
  }

  async function refreshSchedulerState() {
    const resp = await fetch(`${API_BASE}/workflow-scheduler/state`, {
      method: 'GET',
      headers: { Accept: 'application/json', 'X-AO-ROLE': API_ROLE }
    });
    let payload: any = {};
    try {
      payload = await resp.json();
    } catch {
      payload = {};
    }
    if (!resp.ok) {
      throw new Error(String(payload?.details || payload?.error || `${resp.status} ${resp.statusText}`));
    }
    const queue = payload?.queue && typeof payload.queue === 'object' ? payload.queue : {};
    schedulerView = {
      enabled: Boolean(payload?.enabled),
      last_tick_at: String(payload?.last_tick_at || ''),
      last_error: String(payload?.last_error || ''),
      worker_last_tick_at: String(payload?.worker_last_tick_at || ''),
      worker_last_error: String(payload?.worker_last_error || ''),
      queue_depth: Number(queue?.queue_depth || 0),
      queue_running: Number(queue?.queue_running || 0),
      queue_dead_letter: Number(queue?.queue_dead_letter || 0),
      dependency_event_backlog: Number(queue?.dependency_event_backlog || 0),
      active_workers: Number(queue?.active_workers || 0)
    };
  }

  async function refreshDeskProcesses() {
    if (!deskId) {
      publishedProcesses = [];
      publishedDeskVersionId = 0;
      publishedDeskReady = false;
      return;
    }
    const resp = await fetch(`${API_BASE}/desks/${deskId}/processes`, {
      method: 'GET',
      headers: { Accept: 'application/json', 'X-AO-ROLE': API_ROLE }
    });
    let payload: any = {};
    try {
      payload = await resp.json();
    } catch {
      payload = {};
    }
    if (!resp.ok) {
      throw new Error(String(payload?.details || payload?.error || `${resp.status} ${resp.statusText}`));
    }
    publishedDeskVersionId = Math.max(0, Number(payload?.desk_version_id || 0));
    publishedDeskReady = Boolean(payload?.published);
    publishedProcesses = Array.isArray(payload?.processes) ? payload.processes : [];
  }

  async function refreshRunsAndJobs() {
    if (!deskId) {
      processRuns = [];
      workflowJobs = [];
      return;
    }
    const [runsResp, jobsResp] = await Promise.all([
      fetch(`${API_BASE}/process-runs?desk_id=${deskId}&limit=30`, {
        method: 'GET',
        headers: { Accept: 'application/json', 'X-AO-ROLE': API_ROLE }
      }),
      fetch(`${API_BASE}/workflow-jobs?limit=120`, {
        method: 'GET',
        headers: { Accept: 'application/json', 'X-AO-ROLE': API_ROLE }
      })
    ]);
    let runsPayload: any = {};
    let jobsPayload: any = {};
    try {
      runsPayload = await runsResp.json();
    } catch {
      runsPayload = {};
    }
    try {
      jobsPayload = await jobsResp.json();
    } catch {
      jobsPayload = {};
    }
    if (!runsResp.ok) {
      throw new Error(String(runsPayload?.details || runsPayload?.error || `${runsResp.status} ${runsResp.statusText}`));
    }
    if (!jobsResp.ok) {
      throw new Error(String(jobsPayload?.details || jobsPayload?.error || `${jobsResp.status} ${jobsResp.statusText}`));
    }
    processRuns = Array.isArray(runsPayload?.runs) ? runsPayload.runs : [];
    const jobs = Array.isArray(jobsPayload?.jobs) ? jobsPayload.jobs : [];
    workflowJobs = jobs
      .filter((j: any) => Number(j?.desk_id || 0) === Number(deskId))
      .sort((a: any, b: any) => Number(b?.job_id || 0) - Number(a?.job_id || 0))
      .slice(0, 120);
  }

  async function refreshAutomationContour() {
    monitorBusy = true;
    try {
      await Promise.all([refreshSchedulerState(), refreshDeskProcesses(), refreshRunsAndJobs()]);
    } catch (e: any) {
      banner = String(e?.message || e || 'Ошибка обновления мониторинга');
    } finally {
      monitorBusy = false;
    }
  }

  async function publishDeskVersion() {
    if (!deskId || publishBusy) return;
    publishBusy = true;
    try {
      if (deskDirty) {
        const saved = await saveDesk(true);
        if (!saved) throw new Error('Не удалось сохранить рабочий стол перед публикацией');
      }
      const resp = await fetch(`${API_BASE}/desks/${deskId}/publish`, {
        method: 'POST',
        headers: {
          Accept: 'application/json',
          'Content-Type': 'application/json',
          'X-AO-ROLE': API_ROLE
        },
        body: JSON.stringify({ published_by: API_ROLE })
      });
      let payload: any = {};
      try {
        payload = await resp.json();
      } catch {
        payload = {};
      }
      if (!resp.ok) {
        throw new Error(String(payload?.details || payload?.error || `${resp.status} ${resp.statusText}`));
      }
      publishedDeskVersionId = Math.max(0, Number(payload?.desk_version_id || 0));
      publishedDeskReady = true;
      await refreshAutomationContour();
      banner = `Опубликована версия desk #${publishedDeskVersionId}`;
    } catch (e: any) {
      banner = String(e?.message || e || 'Ошибка публикации');
    } finally {
      publishBusy = false;
    }
  }

  async function togglePublishedProcess(startNodeId: string, enabled: boolean) {
    if (!deskId || !startNodeId) return;
    processBusyByNode = { ...processBusyByNode, [startNodeId]: true };
    try {
      const action = enabled ? 'enable' : 'disable';
      const resp = await fetch(`${API_BASE}/processes/${deskId}/${encodeURIComponent(startNodeId)}/${action}`, {
        method: 'POST',
        headers: {
          Accept: 'application/json',
          'Content-Type': 'application/json',
          'X-AO-ROLE': API_ROLE
        },
        body: JSON.stringify({ updated_by: API_ROLE })
      });
      let payload: any = {};
      try {
        payload = await resp.json();
      } catch {
        payload = {};
      }
      if (!resp.ok) {
        throw new Error(String(payload?.details || payload?.error || `${resp.status} ${resp.statusText}`));
      }
      publishedProcesses = publishedProcesses.map((p) =>
        String(p.start_node_id || '') === startNodeId ? { ...p, is_enabled: Boolean(enabled) } : p
      );
      banner = enabled ? `Start ${startNodeId} включен` : `Start ${startNodeId} выключен`;
      await refreshRunsAndJobs();
    } catch (e: any) {
      banner = String(e?.message || e || 'Ошибка переключения Start-процесса');
    } finally {
      processBusyByNode = { ...processBusyByNode, [startNodeId]: false };
    }
  }

  async function triggerPublishedProcess(startNodeId = '') {
    if (!deskId || triggerBusy) return;
    triggerBusy = true;
    try {
      const resp = await fetch(`${API_BASE}/process-runs/trigger`, {
        method: 'POST',
        headers: {
          Accept: 'application/json',
          'Content-Type': 'application/json',
          'X-AO-ROLE': API_ROLE
        },
        body: JSON.stringify({
          desk_id: deskId,
          start_node_id: startNodeId || undefined,
          wait: false,
          created_by: API_ROLE,
          note: 'workflow_ui_trigger'
        })
      });
      let payload: any = {};
      try {
        payload = await resp.json();
      } catch {
        payload = {};
      }
      if (!resp.ok) {
        throw new Error(String(payload?.details || payload?.error || `${resp.status} ${resp.statusText}`));
      }
      const runs = Array.isArray(payload?.runs) ? payload.runs.length : 0;
      banner = runs > 0 ? `Триггер запущен, queued runs: ${runs}` : 'Триггер отправлен';
      await refreshAutomationContour();
    } catch (e: any) {
      banner = String(e?.message || e || 'Ошибка ручного trigger');
    } finally {
      triggerBusy = false;
    }
  }

  function applyScopeTypeByMode(nodeId: string, mode: string) {
    const normalizedMode = String(mode || '').trim();
    if (normalizedMode === 'single_global') {
      updateSetting(nodeId, 'scopeType', 'global');
      updateSetting(nodeId, 'scopeRef', 'global');
      return;
    }
    if (normalizedMode === 'for_each_tenant') updateSetting(nodeId, 'scopeType', 'tenant');
    if (normalizedMode === 'for_each_source_account') updateSetting(nodeId, 'scopeType', 'source_account');
    if (normalizedMode === 'for_each_segment') updateSetting(nodeId, 'scopeType', 'segment');
    if (normalizedMode === 'for_each_partition') updateSetting(nodeId, 'scopeType', 'partition');
    const current = nodes.find((n) => n.id === nodeId);
    const scopeRef = String((current && current.type === 'tool' && toolCfg(current).settings.scopeRef) || '').trim();
    if (!scopeRef) updateSetting(nodeId, 'scopeRef', 'default');
  }

  function restartDeskAutosaveTimer() {
    clearDeskAutosaveTimer();
    if (!deskAutosaveEnabled || !deskInitialized) return;
    const sec = Math.max(3, Number(deskAutosaveSeconds || 10) || 10);
    deskAutosaveSeconds = sec;
    deskAutosaveTimer = setInterval(() => {
      if (!deskInitialized || !deskDirty || deskSaving || deskLoading) return;
      void saveDesk(true);
    }, sec * 1000);
  }

  function toggleDeskAutosave() {
    deskAutosaveEnabled = !deskAutosaveEnabled;
    restartDeskAutosaveTimer();
  }

  function onDeskAutosaveSecondsInput(event: Event) {
    const v = Number((event.currentTarget as HTMLInputElement | null)?.value || 10);
    deskAutosaveSeconds = Number.isFinite(v) && v > 0 ? Math.max(3, Math.trunc(v)) : 10;
    restartDeskAutosaveTimer();
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

  function tryObj(value: any): Record<string, any> {
    if (value && typeof value === 'object' && !Array.isArray(value)) return value;
    if (typeof value !== 'string') return {};
    const src = String(value || '').trim();
    if (!src) return {};
    try {
      const parsed = JSON.parse(src);
      return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
    } catch {
      return {};
    }
  }

  function uniqueAliasList(values: string[]) {
    return [...new Set((values || []).map((v) => String(v || '').trim()).filter(Boolean))];
  }

  function parseScalarValue(raw: any) {
    if (raw === null || raw === undefined) return raw;
    if (typeof raw !== 'string') return raw;
    const txt = String(raw || '').trim();
    if (!txt) return '';
    if (txt.toLowerCase() === 'null') return null;
    if (txt.toLowerCase() === 'true') return true;
    if (txt.toLowerCase() === 'false') return false;
    if (/^-?\d+(\.\d+)?$/.test(txt)) return Number(txt);
    if ((txt.startsWith('{') && txt.endsWith('}')) || (txt.startsWith('[') && txt.endsWith(']'))) {
      try {
        return JSON.parse(txt);
      } catch {
        return txt;
      }
    }
    return txt;
  }

  function normalizeTemplateParameterDefinitions(row: any): TemplateParameterDefinition[] {
    const mapping = tryObj(row?.mapping_json);
    const definitionsRaw = Array.isArray(row?.parameter_definitions)
      ? row.parameter_definitions
      : Array.isArray(mapping?.parameter_definitions)
      ? mapping.parameter_definitions
      : [];
    return definitionsRaw
      .map((pd: any) => ({
        alias: String(pd?.alias || '').trim(),
        definition: String(pd?.definition || '').trim(),
        sourceSchema: String(pd?.source_schema || pd?.sourceSchema || '').trim(),
        sourceTable: String(pd?.source_table || pd?.sourceTable || '').trim(),
        sourceField: String(pd?.source_field || pd?.sourceField || '').trim(),
        conditions: Array.isArray(pd?.conditions)
          ? pd.conditions.map((cond: any) => ({
              id: String(cond?.id || '').trim(),
              schema: String(cond?.schema || '').trim(),
              table: String(cond?.table || '').trim(),
              field: String(cond?.field || '').trim(),
              operator: String(cond?.operator || '').trim(),
              compareMode: String(cond?.compare_mode || cond?.compareMode || 'value').trim(),
              compareValue: String(cond?.compare_value || cond?.compareValue || '').trim(),
              compareColumn: String(cond?.compare_column || cond?.compareColumn || '').trim()
            }))
          : []
      }))
      .filter((pd: TemplateParameterDefinition) => Boolean(pd.alias));
  }

  function normalizeTemplatePaginationParameters(row: any): TemplatePaginationParameter[] {
    const pagination = tryObj(row?.pagination_json);
    const mapping = tryObj(row?.mapping_json);
    const raw =
      Array.isArray(pagination?.parameters)
        ? pagination.parameters
        : Array.isArray(mapping?.pagination_parameters)
        ? mapping.pagination_parameters
        : [];
    return raw
      .map((p: any) => ({
        alias: String(p?.alias || '').trim(),
        firstValue: parseScalarValue(p?.first_value ?? p?.firstValue ?? '')
      }))
      .filter((p: TemplatePaginationParameter) => Boolean(p.alias));
  }

  function normalizeTemplateDataModel(row: any): TemplateDataModel {
    const execution = tryObj(row?.execution_json);
    const mapping = tryObj(row?.mapping_json);
    const source = tryObj(execution?.data_model || mapping?.data_model || row?.data_model_json);

    const tables = (Array.isArray(source?.tables) ? source.tables : [])
      .map((t: any, idx: number) => ({
        id: String(t?.id || `t${idx}`).trim(),
        schema: String(t?.schema || '').trim(),
        table: String(t?.table || '').trim(),
        alias: String(t?.alias || t?.table || `table_${idx + 1}`).trim()
      }))
      .filter((t: DataModelTable) => Boolean(t.id && t.schema && t.table));
    const tableIds = new Set(tables.map((t) => t.id));

    const joins = (Array.isArray(source?.joins) ? source.joins : [])
      .map((j: any, idx: number) => ({
        id: String(j?.id || `j${idx}`).trim(),
        left_table_id: String(j?.left_table_id || j?.leftTableId || '').trim(),
        left_field: String(j?.left_field || j?.leftField || '').trim(),
        right_table_id: String(j?.right_table_id || j?.rightTableId || '').trim(),
        right_field: String(j?.right_field || j?.rightField || '').trim(),
        join_type: String(j?.join_type || j?.joinType || 'inner').toLowerCase() === 'left' ? 'left' : 'inner'
      }))
      .filter((j: DataModelJoin) => Boolean(j.left_table_id && j.right_table_id && j.left_field && j.right_field))
      .filter((j: DataModelJoin) => tableIds.has(j.left_table_id) && tableIds.has(j.right_table_id));

    const fields = (Array.isArray(source?.fields) ? source.fields : [])
      .map((f: any, idx: number) => ({
        id: String(f?.id || `f${idx}`).trim(),
        table_id: String(f?.table_id || f?.tableId || '').trim(),
        field: String(f?.field || '').trim(),
        alias: String(f?.alias || f?.field || `field_${idx + 1}`).trim()
      }))
      .filter((f: DataModelField) => Boolean(f.table_id && f.field && f.alias && tableIds.has(f.table_id)));

    const filters = (Array.isArray(source?.filters) ? source.filters : [])
      .map((f: any, idx: number) => ({
        id: String(f?.id || `flt_${idx}`).trim(),
        table_id: String(f?.table_id || f?.tableId || '').trim(),
        field: String(f?.field || '').trim(),
        operator: String(f?.operator || '').trim(),
        compare_value: String(f?.compare_value || f?.compareValue || '').trim()
      }))
      .filter((f: DataModelFilter) => Boolean(f.table_id && f.field && f.operator));

    return { tables, joins, fields, filters };
  }

  function findAliasInMap<T = any>(map: Record<string, T>, rawAlias: string): { found: boolean; key: string; value?: T } {
    const alias = String(rawAlias || '').trim();
    if (!alias || !map || typeof map !== 'object') return { found: false, key: '' };
    if (Object.prototype.hasOwnProperty.call(map, alias)) {
      return { found: true, key: alias, value: map[alias] };
    }
    const lower = alias.toLowerCase();
    for (const key of Object.keys(map)) {
      if (key.toLowerCase() === lower) return { found: true, key, value: map[key] };
    }
    return { found: false, key: '' };
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

  function isExactTemplateAlias(value: any, aliasesLower: Set<string>) {
    if (typeof value !== 'string' || !aliasesLower.size) return false;
    const exact = value.match(PARAMETER_TOKEN_EXACT_RE);
    if (!exact?.[1]) return false;
    return aliasesLower.has(String(exact[1] || '').trim().toLowerCase());
  }

  function stripUnresolvedTemplateTokens(value: any, aliasesLower: Set<string>) {
    if (!value || !aliasesLower.size) return;
    if (Array.isArray(value)) {
      for (let i = value.length - 1; i >= 0; i -= 1) {
        const item = value[i];
        if (isExactTemplateAlias(item, aliasesLower)) {
          value.splice(i, 1);
          continue;
        }
        stripUnresolvedTemplateTokens(item, aliasesLower);
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
      stripUnresolvedTemplateTokens(val, aliasesLower);
    });
  }

  function resolveStopRuleCompareValue(raw: string, resolvedParams: Record<string, any>, requestCtx?: { body?: any; query?: any; headers?: any }) {
    const source = String(raw || '').trim();
    if (!source) return '';
    const bodyRef = source.match(/^\{\{\s*body\.([^{}]+)\s*\}\}$/i);
    if (bodyRef?.[1]) return getByPath(requestCtx?.body || {}, String(bodyRef[1] || '').trim());
    const queryRef = source.match(/^\{\{\s*query\.([^{}]+)\s*\}\}$/i);
    if (queryRef?.[1]) return getByPath(requestCtx?.query || {}, String(queryRef[1] || '').trim());
    const headersRef = source.match(/^\{\{\s*headers\.([^{}]+)\s*\}\}$/i);
    if (headersRef?.[1]) return getByPath(requestCtx?.headers || {}, String(headersRef[1] || '').trim());
    return applyParametersToValue(source, resolvedParams);
  }

  function evaluateNodeStopRuleOperator(operatorRaw: string, actual: any, expected: any) {
    const op = normalizeStopRuleOperator(operatorRaw);
    if (op === 'equals') return evaluateStopRuleValue(actual, '=', expected);
    if (op === 'not_equals') return evaluateStopRuleValue(actual, '!=', expected);
    if (op === 'gt') return evaluateStopRuleValue(actual, '>', expected);
    if (op === 'gte') return evaluateStopRuleValue(actual, '>=', expected);
    if (op === 'lt') return evaluateStopRuleValue(actual, '<', expected);
    if (op === 'lte') return evaluateStopRuleValue(actual, '<=', expected);
    if (op === 'contains') return evaluateStopRuleValue(actual, 'contains', expected);
    if (op === 'not_contains') return evaluateStopRuleValue(actual, 'not_contains', expected);
    if (op === 'is_empty') return evaluateStopRuleValue(actual, 'empty', expected);
    if (op === 'not_empty') return evaluateStopRuleValue(actual, 'exists', expected);
    return false;
  }

  function findTriggeredStopRule(
    responseBody: any,
    rules: Array<{ id: string; responsePath: string; operator: string; compareValue: string }>,
    resolvedParams: Record<string, any>,
    requestCtx?: { body?: any; query?: any; headers?: any }
  ) {
    const list = Array.isArray(rules) ? rules : [];
    for (const rule of list) {
      const path = String(rule?.responsePath || '').trim();
      if (!path) continue;
      const actual = getByPath({ response: responseBody }, path);
      const expected = resolveStopRuleCompareValue(String(rule?.compareValue || ''), resolvedParams, requestCtx);
      if (evaluateNodeStopRuleOperator(String(rule?.operator || ''), actual, expected)) {
        return {
          id: String(rule?.id || ''),
          path,
          operator: normalizeStopRuleOperator(rule?.operator),
          actual,
          expected
        };
      }
    }
    return null;
  }

  function parseParameterSourceRef(raw: string, param: TemplateParameterDefinition): { schema: string; table: string; field: string } | null {
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
      const schema = String(param?.sourceSchema || '').trim();
      return schema && table && field ? { schema, table, field } : null;
    }
    if (parts.length === 1) {
      const field = parts[0];
      const schema = String(param?.sourceSchema || '').trim();
      const table = String(param?.sourceTable || '').trim();
      return schema && table && field ? { schema, table, field } : null;
    }
    return null;
  }

  function resolveParameterSource(param: TemplateParameterDefinition): { schema: string; table: string; field: string } | null {
    const directSchema = String(param?.sourceSchema || '').trim();
    const directTable = String(param?.sourceTable || '').trim();
    const directField = String(param?.sourceField || '').trim();
    if (directSchema && directTable && directField) {
      return { schema: directSchema, table: directTable, field: directField };
    }
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
      // continue with text format parsing
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

  async function fetchParameterValue(param: TemplateParameterDefinition): Promise<any> {
    const source = resolveParameterSource(param);
    if (!source) return null;
    const query = new URLSearchParams();
    query.set('schema', source.schema);
    query.set('table', source.table);
    query.set('field', source.field);
    if (Array.isArray(param.conditions) && param.conditions.length) {
      query.set('conditions', JSON.stringify(param.conditions));
    }
    const resp = await fetch(`${API_BASE}/parameter-value?${query.toString()}`, {
      method: 'GET',
      headers: { Accept: 'application/json', 'X-AO-ROLE': API_ROLE }
    });
    if (!resp.ok) {
      const txt = await resp.text();
      throw new Error(txt || `parameter-value HTTP ${resp.status}`);
    }
    let json: any = {};
    try {
      json = await resp.json();
    } catch {
      json = {};
    }
    return json?.value ?? null;
  }

  async function resolveAliasesFromParameterDefinitions(
    definitions: TemplateParameterDefinition[],
    aliases: string[]
  ): Promise<{ map: Record<string, any>; issues: Record<string, string> }> {
    const map: Record<string, any> = {};
    const issues: Record<string, string> = {};
    const requested = uniqueAliasList(aliases);
    if (!requested.length) return { map, issues };

    for (const alias of requested) {
      const param = definitions.find((d) => d.alias.toLowerCase() === alias.toLowerCase());
      if (!param) continue;
      try {
        const value = await fetchParameterValue(param);
        if (value !== undefined && value !== null && String(value) !== '') {
          map[alias] = value;
        } else {
          issues[alias] = 'источник параметра вернул пустое значение';
        }
      } catch (e: any) {
        issues[alias] = String(e?.message || e || 'ошибка получения значения');
      }
    }
    return { map, issues };
  }

  async function resolveAliasesFromDataModel(
    source: DynamicApiSource,
    aliases: string[]
  ): Promise<{ map: Record<string, any>; issues: Record<string, string> }> {
    const map: Record<string, any> = {};
    const issues: Record<string, string> = {};
    const requested = uniqueAliasList(aliases);
    if (!requested.length) return { map, issues };
    const model = source.dataModel;
    if (!model || !model.tables.length || !model.fields.length) return { map, issues };

    const aliasSet = new Set(requested.map((a) => a.toLowerCase()));
    const fields = model.fields.filter((f) => aliasSet.has(String(f.alias || '').trim().toLowerCase()));
    if (!fields.length) return { map, issues };

    const usedTableIds = new Set(fields.map((f) => f.table_id));
    const tables = model.tables.filter((t) => usedTableIds.has(t.id));
    const joins = usedTableIds.size <= 1 ? [] : model.joins;
    const filters = model.filters.filter((f) => usedTableIds.has(f.table_id));

    const resp = await fetch(`${API_BASE}/parameter-join-values`, {
      method: 'POST',
      headers: { Accept: 'application/json', 'Content-Type': 'application/json', 'X-AO-ROLE': API_ROLE },
      body: JSON.stringify({
        tables,
        joins,
        fields,
        filters,
        aliases: requested,
        limit: 1,
        offset: 0
      })
    });
    if (!resp.ok) {
      const txt = await resp.text();
      throw new Error(txt || `parameter-join-values HTTP ${resp.status}`);
    }
    let json: any = {};
    try {
      json = await resp.json();
    } catch {
      json = {};
    }
    const row = Array.isArray(json?.rows) && json.rows.length ? json.rows[0] : null;
    if (!row) {
      requested.forEach((alias) => {
        issues[alias] = 'конструктор данных вернул 0 строк';
      });
      return { map, issues };
    }
    requested.forEach((alias) => {
      const found = findAliasInMap(row, alias);
      if (found.found && found.value !== undefined && found.value !== null && String(found.value) !== '') {
        map[alias] = found.value;
      }
    });
    return { map, issues };
  }

  async function resolveAliasRowsFromDataModel(
    source: DynamicApiSource,
    aliases: string[]
  ): Promise<{ rows: Record<string, any>[]; issues: Record<string, string> }> {
    const issues: Record<string, string> = {};
    const requested = uniqueAliasList(aliases);
    if (!requested.length) return { rows: [], issues };
    const model = source.dataModel;
    if (!model || !model.tables.length || !model.fields.length) return { rows: [], issues };

    const aliasSet = new Set(requested.map((a) => a.toLowerCase()));
    const fields = model.fields.filter((f) => aliasSet.has(String(f.alias || '').trim().toLowerCase()));
    if (!fields.length) return { rows: [], issues };

    const usedTableIds = new Set(fields.map((f) => f.table_id));
    const tables = model.tables.filter((t) => usedTableIds.has(t.id));
    const joins = usedTableIds.size <= 1 ? [] : model.joins;
    const filters = model.filters.filter((f) => usedTableIds.has(f.table_id));

    const resp = await fetch(`${API_BASE}/parameter-join-values`, {
      method: 'POST',
      headers: { Accept: 'application/json', 'Content-Type': 'application/json', 'X-AO-ROLE': API_ROLE },
      body: JSON.stringify({
        tables,
        joins,
        fields,
        filters,
        aliases: requested,
        limit: 5000,
        offset: 0
      })
    });
    if (!resp.ok) {
      const txt = await resp.text();
      throw new Error(txt || `parameter-join-values HTTP ${resp.status}`);
    }
    let json: any = {};
    try {
      json = await resp.json();
    } catch {
      json = {};
    }
    const rowsRaw = Array.isArray(json?.rows) ? json.rows : [];
    const rows = rowsRaw.map((row: any) => {
      const out: Record<string, any> = {};
      requested.forEach((alias) => {
        const found = findAliasInMap(row, alias);
        if (found.found) out[alias] = found.value;
      });
      return out;
    });
    return { rows, issues };
  }

  function templateDispatchConfig(source: DynamicApiSource | null | undefined) {
    const raw = source?.rawRow && typeof source.rawRow === 'object' ? source.rawRow : {};
    const mapping = tryObj(raw?.mapping_json);
    const execution = tryObj(raw?.execution_json || mapping?.execution);
    const dispatchModeRaw = String(raw?.dispatch_mode || execution?.dispatch_mode || 'single').trim().toLowerCase();
    const dispatchMode: 'single' | 'group_by' = dispatchModeRaw === 'group_by' ? 'group_by' : 'single';
    const groupByRaw = Array.isArray(raw?.group_by_aliases)
      ? raw.group_by_aliases
      : Array.isArray(execution?.group_by_aliases)
      ? execution.group_by_aliases
      : [];
    const groupByAliases = uniqueAliasList(groupByRaw.map((x: any) => String(x || '').trim()));
    return { dispatchMode, groupByAliases };
  }

  function templateAliasesForNode(node: WorkflowNode | null | undefined): string[] {
    if (!node || (!isApiNode(node) && !isApiToolNode(node))) return [];
    const request = getApiRequestForNode(node);
    const source = resolveTemplateSourceForNode(node, request);
    if (!source) return [];
    const out: string[] = [];
    (source.parameterDefinitions || []).forEach((p) => {
      const alias = String(p?.alias || '').trim();
      if (alias) out.push(alias);
    });
    (source.paginationParameters || []).forEach((p) => {
      const alias = String(p?.alias || '').trim();
      if (alias) out.push(alias);
    });
    (source.dataModel?.fields || []).forEach((f) => {
      const alias = String(f?.alias || '').trim();
      if (alias) out.push(alias);
    });
    return uniqueAliasList(out);
  }

  function parseParameterOverridesRaw(value: any): Record<string, any> {
    if (value && typeof value === 'object' && !Array.isArray(value)) return value as Record<string, any>;
    const txt = String(value || '').trim();
    if (!txt) return {};
    try {
      const parsed = JSON.parse(txt);
      return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
    } catch {
      return {};
    }
  }

  function getNodeParameterOverrides(node: WorkflowNode | null | undefined): Record<string, any> {
    if (!node) return {};
    if (isApiNode(node)) return parseParameterOverridesRaw(node.config?.parameterOverrides || {});
    if (isApiToolNode(node)) return parseParameterOverridesRaw(toolCfg(node).settings?.parameterOverrides || '{}');
    return {};
  }

  function setNodeParameterOverride(nodeId: string, alias: string, value: string) {
    const node = nodes.find((n) => n.id === nodeId);
    if (!node || (!isApiNode(node) && !isApiToolNode(node))) return;
    const key = String(alias || '').trim();
    if (!key) return;
    const current = getNodeParameterOverrides(node);
    const next = { ...current };
    const txt = String(value ?? '').trim();
    if (!txt) delete next[key];
    else next[key] = txt;
    if (isApiNode(node)) {
      updateDataNodeConfig(nodeId, { parameterOverrides: next });
      return;
    }
    updateSetting(nodeId, 'parameterOverrides', JSON.stringify(next));
  }

  function resolveAliasesFromPaginationFirstValues(source: DynamicApiSource, aliases: string[]) {
    const map: Record<string, any> = {};
    const requested = new Set(uniqueAliasList(aliases).map((a) => a.toLowerCase()));
    (source.paginationParameters || []).forEach((p) => {
      const alias = String(p.alias || '').trim();
      if (!alias || !requested.has(alias.toLowerCase())) return;
      if (p.firstValue === undefined || p.firstValue === null || p.firstValue === '') return;
      map[alias] = p.firstValue;
    });
    return map;
  }

  async function resolveTemplateAliasValues(source: DynamicApiSource, aliases: string[]) {
    const map: Record<string, any> = {};
    const issues: Record<string, string> = {};
    const requested = uniqueAliasList(aliases);
    if (!requested.length) return { map, issues };

    const paginationValues = resolveAliasesFromPaginationFirstValues(source, requested);
    Object.entries(paginationValues).forEach(([alias, value]) => {
      map[alias] = value;
    });

    const unresolvedAfterPagination = requested.filter((alias) => !findAliasInMap(map, alias).found);
    if (unresolvedAfterPagination.length) {
      try {
        const fromDataModel = await resolveAliasesFromDataModel(source, unresolvedAfterPagination);
        Object.entries(fromDataModel.map).forEach(([alias, value]) => {
          if (!findAliasInMap(map, alias).found) map[alias] = value;
        });
        Object.entries(fromDataModel.issues).forEach(([alias, reason]) => {
          if (!issues[alias]) issues[alias] = reason;
        });
      } catch (e: any) {
        const msg = String(e?.message || e || 'ошибка чтения конструктора данных');
        unresolvedAfterPagination.forEach((alias) => {
          if (!issues[alias]) issues[alias] = msg;
        });
      }
    }

    const unresolvedAfterData = requested.filter((alias) => !findAliasInMap(map, alias).found);
    if (unresolvedAfterData.length) {
      const fromDefinitions = await resolveAliasesFromParameterDefinitions(source.parameterDefinitions || [], unresolvedAfterData);
      Object.entries(fromDefinitions.map).forEach(([alias, value]) => {
        if (!findAliasInMap(map, alias).found) map[alias] = value;
      });
      Object.entries(fromDefinitions.issues).forEach(([alias, reason]) => {
        if (!issues[alias]) issues[alias] = reason;
      });
    }

    return { map, issues };
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

  function paginationModeFromTemplateRaw(rawRow: any): ApiPaginationMode {
    const pagination = tryObj(rawRow?.pagination_json);
    const strategy = String(rawRow?.pagination_strategy || pagination?.strategy || 'none').trim().toLowerCase();
    if (strategy === 'page_number') return 'page_number';
    if (strategy === 'offset_limit') return 'offset_limit';
    if (strategy === 'cursor_fields' || strategy === 'cursor') return 'cursor';
    return 'none';
  }

  function paginationStrategyForUpsert(mode: ApiPaginationMode, enabled: boolean) {
    if (!enabled || mode === 'none') return 'none';
    if (mode === 'page_number') return 'page_number';
    if (mode === 'offset_limit') return 'offset_limit';
    if (mode === 'cursor') return 'cursor_fields';
    return 'none';
  }

  function normalizeStopRuleOperator(raw: any) {
    const op = String(raw || '').trim().toLowerCase();
    if (
      op === 'equals' ||
      op === 'not_equals' ||
      op === 'gt' ||
      op === 'gte' ||
      op === 'lt' ||
      op === 'lte' ||
      op === 'contains' ||
      op === 'not_contains' ||
      op === 'is_empty' ||
      op === 'not_empty'
    ) {
      return op;
    }
    if (op === '=' || op === '==') return 'equals';
    if (op === '!=' || op === '<>') return 'not_equals';
    if (op === '>') return 'gt';
    if (op === '>=') return 'gte';
    if (op === '<') return 'lt';
    if (op === '<=') return 'lte';
    if (op === 'empty') return 'is_empty';
    if (op === 'exists') return 'not_empty';
    return 'equals';
  }

  function parsePaginationStopRules(raw: any) {
    const src = Array.isArray(raw) ? raw : [];
    return src
      .map((rule: any, idx: number) => ({
        id: String(rule?.id || `stop_${idx + 1}`),
        responsePath: String(rule?.response_path || rule?.responsePath || rule?.path || '').trim(),
        operator: normalizeStopRuleOperator(rule?.operator),
        compareValue: String(rule?.compare_value || rule?.compareValue || rule?.value || '').trim()
      }))
      .filter((rule) => Boolean(rule.responsePath));
  }

  function parsePaginationStopRulesFromSetting(raw: any) {
    if (Array.isArray(raw)) return parsePaginationStopRules(raw);
    const txt = String(raw || '').trim();
    if (!txt) return [];
    try {
      const parsed = JSON.parse(txt);
      return parsePaginationStopRules(parsed);
    } catch {
      return [];
    }
  }

  function paginationFromTemplateRaw(rawRow: any): ApiNodePagination {
    const pagination = tryObj(rawRow?.pagination_json);
    const stopConditions = tryObj(pagination?.stop_conditions);
    const enabledRaw = rawRow?.pagination_enabled;
    const enabled =
      enabledRaw === undefined || enabledRaw === null
        ? Boolean(pagination?.enabled)
        : Boolean(enabledRaw);
    const mode = paginationModeFromTemplateRaw(rawRow);
    const targetRaw = String(rawRow?.pagination_target || pagination?.target || 'query').trim().toLowerCase();
    const target: ApiPaginationTarget = targetRaw === 'body' ? 'body' : 'query';
    const maxPages = Number.isFinite(Number(rawRow?.pagination_max_pages))
      ? Number(rawRow?.pagination_max_pages)
      : Number.isFinite(Number(pagination?.max_pages))
      ? Number(pagination?.max_pages)
      : 10;
    const pauseMs = Number.isFinite(Number(rawRow?.pagination_delay_ms))
      ? Number(rawRow?.pagination_delay_ms)
      : Number.isFinite(Number(pagination?.delay_ms))
      ? Number(pagination?.delay_ms)
      : 0;
    const useMaxPagesRaw = rawRow?.pagination_use_max_pages ?? pagination?.use_max_pages ?? pagination?.max_pages_enabled;
    const useDelayRaw = rawRow?.pagination_use_delay ?? pagination?.use_delay ?? pagination?.delay_enabled;
    const stopOnMissingRaw = rawRow?.pagination_stop_on_missing_value ?? stopConditions?.on_missing_pagination_value;
    const stopOnHttpErrorRaw = rawRow?.pagination_stop_on_http_error ?? stopConditions?.on_http_error;
    const stopOnSameRaw = rawRow?.pagination_stop_on_same_response ?? stopConditions?.on_same_response;
    const sameResponseLimitRaw = rawRow?.pagination_same_response_limit ?? stopConditions?.same_response_limit;
    const dataPath = String(rawRow?.pagination_data_path || pagination?.data_path || '').trim();
    const pageParam = String(rawRow?.pagination_page_param || pagination?.page_param || 'page').trim() || 'page';
    const startPage = Number.isFinite(Number(rawRow?.pagination_start_page))
      ? Number(rawRow?.pagination_start_page)
      : Number.isFinite(Number(pagination?.start_page))
      ? Number(pagination?.start_page)
      : 1;
    const limitParam = String(rawRow?.pagination_limit_param || pagination?.limit_param || 'limit').trim() || 'limit';
    const limitValue = Number.isFinite(Number(rawRow?.pagination_limit_value))
      ? Number(rawRow?.pagination_limit_value)
      : Number.isFinite(Number(pagination?.limit_value))
      ? Number(pagination?.limit_value)
      : 100;
    const offsetParam = String(rawRow?.pagination_offset_param || pagination?.offset_param || 'offset').trim() || 'offset';
    const startOffset = Number.isFinite(Number(rawRow?.pagination_start_offset))
      ? Number(rawRow?.pagination_start_offset)
      : Number.isFinite(Number(pagination?.start_offset))
      ? Number(pagination?.start_offset)
      : 0;
    const cursorReqPath = String(rawRow?.pagination_cursor_req_path_1 || pagination?.cursor_req_path_1 || 'cursor').trim() || 'cursor';
    const cursorResPath = String(rawRow?.pagination_cursor_res_path_1 || pagination?.cursor_res_path_1 || 'response.cursor').trim() || 'response.cursor';
    const stopRules = parsePaginationStopRules(stopConditions?.response_rules || stopConditions?.rules || []);
    return {
      enabled: Boolean(enabled),
      mode,
      target,
      useMaxPages: useMaxPagesRaw === undefined || useMaxPagesRaw === null ? true : Boolean(useMaxPagesRaw),
      maxPages: Math.max(1, Number(maxPages || 1)),
      useDelay: useDelayRaw === undefined || useDelayRaw === null ? Math.max(0, Number(pauseMs || 0)) > 0 : Boolean(useDelayRaw),
      pauseMs: Math.max(0, Number(pauseMs || 0)),
      dataPath,
      stopOnMissingValue: stopOnMissingRaw === undefined || stopOnMissingRaw === null ? true : Boolean(stopOnMissingRaw),
      stopOnHttpError: stopOnHttpErrorRaw === undefined || stopOnHttpErrorRaw === null ? true : Boolean(stopOnHttpErrorRaw),
      stopOnSameResponse: stopOnSameRaw === undefined || stopOnSameRaw === null ? true : Boolean(stopOnSameRaw),
      sameResponseLimit: Math.max(2, Math.min(50, Number(sameResponseLimitRaw || 5))),
      stopRules,
      pageParam,
      startPage: Math.max(1, Number(startPage || 1)),
      limitParam,
      limitValue: Math.max(1, Number(limitValue || 1)),
      offsetParam,
      startOffset: Math.max(0, Number(startOffset || 0)),
      cursorReqPath,
      cursorResPath
    };
  }

  function applyPaginationToNodeFromTemplate(nodeId: string, rawRow: any) {
    const pg = paginationFromTemplateRaw(rawRow);
    const node = nodes.find((n) => n.id === nodeId);
    if (!node) return;
    if (isApiNode(node)) {
      updateDataNodeConfig(nodeId, { apiPagination: pg });
      return;
    }
    if (!isApiToolNode(node)) return;
    updateSetting(nodeId, 'paginationEnabled', pg.enabled ? 'true' : 'false');
    updateSetting(nodeId, 'paginationMode', pg.mode);
    updateSetting(nodeId, 'paginationTarget', pg.target);
    updateSetting(nodeId, 'paginationUseMaxPages', pg.useMaxPages ? 'true' : 'false');
    updateSetting(nodeId, 'paginationMaxPages', String(pg.maxPages));
    updateSetting(nodeId, 'paginationUseDelay', pg.useDelay ? 'true' : 'false');
    updateSetting(nodeId, 'paginationPauseMs', String(pg.pauseMs));
    updateSetting(nodeId, 'paginationDataPath', pg.dataPath);
    updateSetting(nodeId, 'paginationStopOnMissingValue', pg.stopOnMissingValue ? 'true' : 'false');
    updateSetting(nodeId, 'paginationStopOnHttpError', pg.stopOnHttpError ? 'true' : 'false');
    updateSetting(nodeId, 'paginationStopOnSameResponse', pg.stopOnSameResponse ? 'true' : 'false');
    updateSetting(nodeId, 'paginationSameResponseLimit', String(pg.sameResponseLimit));
    updateSetting(nodeId, 'paginationStopRules', JSON.stringify(pg.stopRules || []));
    updateSetting(nodeId, 'paginationPageParam', pg.pageParam);
    updateSetting(nodeId, 'paginationStartPage', String(pg.startPage));
    updateSetting(nodeId, 'paginationLimitParam', pg.limitParam);
    updateSetting(nodeId, 'paginationLimitValue', String(pg.limitValue));
    updateSetting(nodeId, 'paginationOffsetParam', pg.offsetParam);
    updateSetting(nodeId, 'paginationStartOffset', String(pg.startOffset));
    updateSetting(nodeId, 'paginationCursorReqPath', pg.cursorReqPath);
    updateSetting(nodeId, 'paginationCursorResPath', pg.cursorResPath);
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
      updateDataNodeConfig(nodeId, {
        requestTemplate: src.requestTemplate,
        apiRequest: tpl,
        templateId: src.id,
        templateStoreId: src.storeId ? String(src.storeId) : ''
      });
      applyPaginationToNodeFromTemplate(nodeId, src.rawRow || {});
      return;
    }
    if (isApiToolNode(node)) {
      updateSetting(nodeId, 'templateId', src.id);
      updateSetting(nodeId, 'templateStoreId', src.storeId ? String(src.storeId) : '');
      updateApiToolRequest(nodeId, 'method', tpl.method);
      updateApiToolRequest(nodeId, 'url', tpl.url);
      updateApiToolRequest(nodeId, 'authMode', tpl.authMode);
      updateApiToolRequest(nodeId, 'headersText', tpl.headersText);
      updateApiToolRequest(nodeId, 'queryText', tpl.queryText);
      updateApiToolRequest(nodeId, 'bodyText', tpl.bodyText);
      applyPaginationToNodeFromTemplate(nodeId, src.rawRow || {});
    }
  }

  function parseUrlForTemplatePayload(urlRaw: string) {
    const raw = String(urlRaw || '').trim();
    const fallback = { baseUrl: '', path: '/', query: {} as Record<string, any> };
    if (!raw) return fallback;
    try {
      const u = new URL(raw);
      const query: Record<string, any> = {};
      u.searchParams.forEach((value, key) => {
        query[key] = value;
      });
      return {
        baseUrl: `${u.protocol}//${u.host}`,
        path: u.pathname || '/',
        query
      };
    } catch {
      return fallback;
    }
  }

  function withApiRoleHeaders(): HeadersInit {
    return {
      Accept: 'application/json',
      'Content-Type': 'application/json',
      'X-AO-ROLE': API_ROLE
    };
  }

  async function workflowApiJson<T = any>(url: string, init?: RequestInit): Promise<T> {
    const response = await fetch(url, init);
    let payload: any = null;
    try {
      payload = await response.json();
    } catch {
      payload = null;
    }
    if (!response.ok) {
      const details = payload?.details || payload?.error || `${response.status} ${response.statusText}`;
      throw new Error(String(details || 'Ошибка API'));
    }
    return (payload || {}) as T;
  }

  function workflowApiHeaders(): Record<string, string> {
    return {
      'Content-Type': 'application/json',
      'X-AO-ROLE': API_ROLE
    };
  }

  async function refreshApiBuilderTables(): Promise<void> {
    await loadDynamicSourceCatalog();
  }

  async function saveNodeEditsToApiTemplate(nodeId: string) {
    const node = nodes.find((n) => n.id === nodeId);
    if (!node || (!isApiNode(node) && !isApiToolNode(node))) {
      banner = 'Сохранение доступно только для API-нод';
      return;
    }
    const request = getApiRequestForNode(node);
    const templateSource = resolveTemplateSourceForNode(node, request);
    const storeId = Number(templateSource?.storeId || nodeTemplateStoreId(node) || 0);
    if (!Number.isFinite(storeId) || storeId <= 0) {
      banner = 'Не найден ID шаблона API для сохранения. Выбери шаблон API в узле.';
      return;
    }
    const raw = templateSource?.rawRow && typeof templateSource.rawRow === 'object' ? templateSource.rawRow : {};
    const urlParts = parseUrlForTemplatePayload(request.url);
    if (!urlParts.baseUrl || !urlParts.path) {
      banner = 'Некорректный URL. Укажи полный URL перед сохранением в шаблон.';
      return;
    }

    try {
      nodeTemplateSaving = { ...nodeTemplateSaving, [nodeId]: true };
      const method = String(request.method || 'GET').trim().toUpperCase() || 'GET';
      const headersObj = parseObjectJsonSafe('Headers JSON', request.headersText);
      const queryObj = parseObjectJsonSafe('Query JSON', request.queryText);
      const bodyObj = parseAnyJsonSafe('Body JSON', request.bodyText);
      const pagination = getPaginationForNode(node);
      const paginationStrategy = paginationStrategyForUpsert(pagination.mode, pagination.enabled);
      const mergedQuery = { ...urlParts.query, ...queryObj };
      const basePagination = tryObj(raw?.pagination_json);
      const baseStopConditions = tryObj(basePagination?.stop_conditions);
      const stopRulesPayload = parsePaginationStopRules(pagination.stopRules || []).map((rule) => ({
        id: rule.id,
        response_path: rule.responsePath,
        operator: normalizeStopRuleOperator(rule.operator),
        compare_value: String(rule.compareValue || '')
      }));
      const paginationJson = {
        ...basePagination,
        enabled: Boolean(pagination.enabled),
        strategy: paginationStrategy,
        target: pagination.target,
        data_path: String(pagination.dataPath || ''),
        page_param: String(pagination.pageParam || 'page'),
        start_page: Math.max(1, Number(pagination.startPage || 1)),
        limit_param: String(pagination.limitParam || 'limit'),
        limit_value: Math.max(1, Number(pagination.limitValue || 1)),
        offset_param: String(pagination.offsetParam || 'offset'),
        start_offset: Math.max(0, Number(pagination.startOffset || 0)),
        cursor_req_path_1: String(pagination.cursorReqPath || ''),
        cursor_res_path_1: String(pagination.cursorResPath || ''),
        use_max_pages: Boolean(pagination.useMaxPages),
        max_pages: Math.max(1, Number(pagination.maxPages || 1)),
        use_delay: Boolean(pagination.useDelay),
        delay_ms: Math.max(0, Number(pagination.pauseMs || 0)),
        stop_conditions: {
          ...baseStopConditions,
          on_missing_pagination_value: Boolean(pagination.stopOnMissingValue),
          on_same_response: Boolean(pagination.stopOnSameResponse),
          same_response_limit: Math.max(2, Math.min(50, Number(pagination.sameResponseLimit || 5))),
          on_http_error: Boolean(pagination.stopOnHttpError),
          response_rules: stopRulesPayload
        }
      };

      const payload: Record<string, any> = {
        ...raw,
        id: Math.trunc(storeId),
        expected_revision: Number.isFinite(Number(raw?.revision)) ? Math.max(1, Math.trunc(Number(raw.revision))) : 0,
        api_name: String(raw?.api_name || templateSource?.name || node.config?.name || 'API').trim() || 'API',
        method,
        base_url: urlParts.baseUrl,
        path: urlParts.path,
        auth_mode: String(request.authMode || raw?.auth_mode || 'manual').trim() || 'manual',
        headers_json: headersObj,
        query_json: mergedQuery,
        body_json: bodyObj,
        pagination_json: paginationJson,
        pagination_enabled: Boolean(pagination.enabled),
        pagination_strategy: paginationStrategy,
        pagination_target: pagination.target,
        pagination_data_path: String(pagination.dataPath || ''),
        pagination_page_param: String(pagination.pageParam || 'page'),
        pagination_start_page: Math.max(1, Number(pagination.startPage || 1)),
        pagination_limit_param: String(pagination.limitParam || 'limit'),
        pagination_limit_value: Math.max(1, Number(pagination.limitValue || 1)),
        pagination_offset_param: String(pagination.offsetParam || 'offset'),
        pagination_start_offset: Math.max(0, Number(pagination.startOffset || 0)),
        pagination_cursor_req_path_1: String(pagination.cursorReqPath || ''),
        pagination_cursor_res_path_1: String(pagination.cursorResPath || ''),
        pagination_use_max_pages: Boolean(pagination.useMaxPages),
        pagination_max_pages: Math.max(1, Number(pagination.maxPages || 1)),
        pagination_use_delay: Boolean(pagination.useDelay),
        pagination_delay_ms: Math.max(0, Number(pagination.pauseMs || 0)),
        pagination_stop_on_missing_value: Boolean(pagination.stopOnMissingValue),
        pagination_stop_on_same_response: Boolean(pagination.stopOnSameResponse),
        pagination_same_response_limit: Math.max(2, Math.min(50, Number(pagination.sameResponseLimit || 5))),
        pagination_stop_on_http_error: Boolean(pagination.stopOnHttpError),
        updated_by: 'workflow_node'
      };

      const response = await fetch(`${API_BASE}/api-configs/upsert`, {
        method: 'POST',
        headers: withApiRoleHeaders(),
        body: JSON.stringify(payload)
      });
      let body: any = {};
      try {
        body = await response.json();
      } catch {
        body = {};
      }
      if (!response.ok) {
        const details = String(body?.details || body?.error || `HTTP ${response.status}`);
        throw new Error(`Не удалось сохранить шаблон API: ${details}`);
      }

      await loadDynamicSourceCatalog();
      const currentTemplateId = nodeTemplateId(node);
      if (currentTemplateId) applyTemplateToNode(nodeId, currentTemplateId);
      banner = `Шаблон API сохранен в хранилище (ID ${Math.trunc(storeId)})`;
    } catch (e: any) {
      banner = String(e?.message || e || 'Ошибка сохранения шаблона API');
    } finally {
      nodeTemplateSaving = { ...nodeTemplateSaving, [nodeId]: false };
    }
  }

  function nodeTemplateId(n: WorkflowNode | null | undefined) {
    if (!n) return '';
    if (isApiNode(n)) {
      const direct = String(n.config?.templateId || '').trim();
      if (direct) return direct;
      const refs = uniqueAliasList([
        String(n.config?.templateStoreId || '').trim(),
        String(n.config?.id || '').trim(),
        String(n.config?.datasetId || '').trim()
      ]);
      for (const ref of refs) {
        const found = dynamicApiSources.find((src) => sourceMatchesTemplateRef(src, ref));
        if (found) return found.id;
      }
      return '';
    }
    if (isApiToolNode(n)) {
      const settings = toolCfg(n).settings || {};
      const direct = String(settings.templateId || '').trim();
      if (direct) return direct;
      const ref = String(settings.templateStoreId || '').trim();
      if (ref) {
        const found = dynamicApiSources.find((src) => sourceMatchesTemplateRef(src, ref));
        if (found) return found.id;
      }
      return '';
    }
    return '';
  }

  function sourceMatchesTemplateRef(src: DynamicApiSource, refRaw: string) {
    const ref = String(refRaw || '').trim();
    if (!ref) return false;
    const lower = ref.toLowerCase();
    const storeId = Number(src.storeId || 0);
    const storeIdStr = storeId > 0 ? String(storeId) : '';
    if (lower === String(src.id || '').toLowerCase()) return true;
    if (lower === String(src.datasetId || '').toLowerCase()) return true;
    if (storeIdStr && lower === storeIdStr.toLowerCase()) return true;
    if (storeIdStr && lower === `api_template:${storeIdStr}`.toLowerCase()) return true;
    if (storeIdStr && lower === `api_tpl_${storeIdStr}`.toLowerCase()) return true;
    return false;
  }

  function nodeTemplateRefs(node: WorkflowNode | null | undefined): string[] {
    if (!node) return [];
    const refs: string[] = [];
    if (isApiNode(node)) {
      refs.push(String(node.config?.templateId || '').trim());
      refs.push(String(node.config?.templateStoreId || '').trim());
      refs.push(String(node.config?.id || '').trim());
      refs.push(String(node.config?.datasetId || '').trim());
    } else if (isApiToolNode(node)) {
      const settings = toolCfg(node).settings || {};
      refs.push(String(settings.templateId || '').trim());
      refs.push(String(settings.templateStoreId || '').trim());
    }
    return uniqueAliasList(refs.filter(Boolean));
  }

  function nodeTemplateStoreId(node: WorkflowNode | null | undefined) {
    const src = resolveTemplateSourceForNode(node, node ? getApiRequestForNode(node) : normalizeApiRequest(undefined));
    if (src?.storeId) return String(src.storeId);
    const refs = nodeTemplateRefs(node);
    for (const ref of refs) {
      const txt = String(ref || '').trim();
      if (/^\d+$/.test(txt)) return txt;
      const m1 = txt.match(/^api_template:(\d+)$/i);
      if (m1?.[1]) return m1[1];
      const m2 = txt.match(/^api_tpl_(\d+)$/i);
      if (m2?.[1]) return m2[1];
    }
    return '';
  }

  function resolveTemplateSourceForNode(node: WorkflowNode, request: ApiNodeRequest): DynamicApiSource | null {
    void request;
    const refs = nodeTemplateRefs(node);
    for (const ref of refs) {
      const found = dynamicApiSources.find((src) => sourceMatchesTemplateRef(src, ref));
      if (found) return found;
    }
    return null;
  }

  function defaultApiPagination(): ApiNodePagination {
    return {
      enabled: false,
      mode: 'none',
      target: 'query',
      useMaxPages: true,
      maxPages: 10,
      useDelay: false,
      pauseMs: 0,
      dataPath: '',
      stopOnMissingValue: true,
      stopOnHttpError: true,
      stopOnSameResponse: true,
      sameResponseLimit: 5,
      stopRules: [],
      pageParam: 'page',
      startPage: 1,
      limitParam: 'limit',
      limitValue: 100,
      offsetParam: 'offset',
      startOffset: 0,
      cursorReqPath: 'cursor',
      cursorResPath: 'response.cursor'
    };
  }

  function parsePathParts(path: string): Array<string | number> {
    const raw = String(path || '').trim();
    if (!raw) return [];
    const normalized = raw.replace(/\[(\d+)\]/g, '.$1');
    return normalized
      .split('.')
      .map((p) => p.trim())
      .filter(Boolean)
      .map((p) => (/^\d+$/.test(p) ? Number(p) : p));
  }

  function getByPath(obj: any, path: string): any {
    if (!path) return obj;
    const parts = parsePathParts(path);
    let cur = obj;
    for (const part of parts) {
      if (cur == null) return undefined;
      cur = cur[part as any];
    }
    return cur;
  }

  function setByPath(obj: any, path: string, value: any) {
    const parts = parsePathParts(path);
    if (!parts.length) return;
    let cur = obj;
    for (let i = 0; i < parts.length - 1; i += 1) {
      const key = parts[i];
      const nextKey = parts[i + 1];
      if (cur[key as any] === undefined || cur[key as any] === null || typeof cur[key as any] !== 'object') {
        cur[key as any] = typeof nextKey === 'number' ? [] : {};
      }
      cur = cur[key as any];
    }
    cur[parts[parts.length - 1] as any] = value;
  }

  function getPaginationForNode(n: WorkflowNode | null | undefined): ApiNodePagination {
    if (!n) return defaultApiPagination();
    if (isApiNode(n)) return { ...defaultApiPagination(), ...(n.config?.apiPagination || {}) };
    if (isApiToolNode(n)) {
      const s = toolCfg(n).settings || {};
      return {
        enabled: String(s.paginationEnabled || 'false') === 'true',
        mode: (String(s.paginationMode || 'none') as ApiPaginationMode) || 'none',
        target: (String(s.paginationTarget || 'query') as ApiPaginationTarget) || 'query',
        useMaxPages: String(s.paginationUseMaxPages || 'true') !== 'false',
        maxPages: Math.max(1, Number(s.paginationMaxPages || 10)),
        useDelay: String(s.paginationUseDelay || 'false') === 'true',
        pauseMs: Math.max(0, Number(s.paginationPauseMs || 0)),
        dataPath: String(s.paginationDataPath || '').trim(),
        stopOnMissingValue: String(s.paginationStopOnMissingValue || 'true') !== 'false',
        stopOnHttpError: String(s.paginationStopOnHttpError || 'true') !== 'false',
        stopOnSameResponse: String(s.paginationStopOnSameResponse || 'true') !== 'false',
        sameResponseLimit: Math.max(2, Math.min(50, Number(s.paginationSameResponseLimit || 5))),
        stopRules: parsePaginationStopRulesFromSetting(s.paginationStopRules),
        pageParam: String(s.paginationPageParam || 'page').trim() || 'page',
        startPage: Math.max(1, Number(s.paginationStartPage || 1)),
        limitParam: String(s.paginationLimitParam || 'limit').trim() || 'limit',
        limitValue: Math.max(1, Number(s.paginationLimitValue || 100)),
        offsetParam: String(s.paginationOffsetParam || 'offset').trim() || 'offset',
        startOffset: Math.max(0, Number(s.paginationStartOffset || 0)),
        cursorReqPath: String(s.paginationCursorReqPath || 'cursor').trim() || 'cursor',
        cursorResPath: String(s.paginationCursorResPath || 'response.cursor').trim() || 'response.cursor'
      };
    }
    return defaultApiPagination();
  }

  function updatePaginationForNode(nodeId: string, key: keyof ApiNodePagination, value: any) {
    const node = nodes.find((n) => n.id === nodeId);
    if (!node) return;
    if (isApiToolNode(node)) {
      const map: Record<keyof ApiNodePagination, string> = {
        enabled: 'paginationEnabled',
        mode: 'paginationMode',
        target: 'paginationTarget',
        useMaxPages: 'paginationUseMaxPages',
        maxPages: 'paginationMaxPages',
        useDelay: 'paginationUseDelay',
        pauseMs: 'paginationPauseMs',
        dataPath: 'paginationDataPath',
        stopOnMissingValue: 'paginationStopOnMissingValue',
        stopOnHttpError: 'paginationStopOnHttpError',
        stopOnSameResponse: 'paginationStopOnSameResponse',
        sameResponseLimit: 'paginationSameResponseLimit',
        stopRules: 'paginationStopRules',
        pageParam: 'paginationPageParam',
        startPage: 'paginationStartPage',
        limitParam: 'paginationLimitParam',
        limitValue: 'paginationLimitValue',
        offsetParam: 'paginationOffsetParam',
        startOffset: 'paginationStartOffset',
        cursorReqPath: 'paginationCursorReqPath',
        cursorResPath: 'paginationCursorResPath'
      };
      if (key === 'stopRules') updateSetting(nodeId, map[key], JSON.stringify(Array.isArray(value) ? value : []));
      else updateSetting(nodeId, map[key], String(value));
      return;
    }
    if (isApiNode(node)) {
      const current = getPaginationForNode(node);
      updateDataNodeConfig(nodeId, { apiPagination: { ...current, [key]: value } });
    }
  }

  function deepClone<T>(v: T): T {
    return JSON.parse(JSON.stringify(v));
  }

  async function delayMs(ms: number) {
    if (ms <= 0) return;
    await new Promise((resolve) => setTimeout(resolve, ms));
  }

  function defaultSettings(toolType: ToolType) {
    if (toolType === 'start_process')
      return {
        isEnabled: 'true',
        processCode: '',
        triggerType: 'interval',
        intervalValue: '1',
        intervalUnit: 'minutes',
        cron: '0 * * * *',
        timezone: 'UTC',
        runPolicy: 'single_instance',
        executionScopeMode: 'single_global',
        scopeType: 'global',
        scopeRef: 'global',
        tenantId: '',
        contextJson: '{}',
        scopeSource: '{}',
        inputScope: '',
        outputScope: ''
      };
    if (toolType === 'schedule_process') return { cron: '0 * * * *', mode: 'sync' };
    if (toolType === 'api_request')
      return {
        templateId: '',
        templateStoreId: '',
        apiMethod: 'GET',
        apiUrl: '',
        apiAuthMode: 'manual',
        apiHeaders: '{}',
        apiQuery: '{}',
        apiBody: '{}',
        paginationEnabled: 'false',
        paginationMode: 'none',
        paginationTarget: 'query',
        paginationUseMaxPages: 'true',
        paginationMaxPages: '10',
        paginationUseDelay: 'false',
        paginationPauseMs: '0',
        paginationDataPath: '',
        paginationStopOnMissingValue: 'true',
        paginationStopOnHttpError: 'true',
        paginationStopOnSameResponse: 'true',
        paginationSameResponseLimit: '5',
        paginationStopRules: '[]',
        paginationPageParam: 'page',
        paginationStartPage: '1',
        paginationLimitParam: 'limit',
        paginationLimitValue: '100',
        paginationOffsetParam: 'offset',
        paginationStartOffset: '0',
        paginationCursorReqPath: 'cursor',
        paginationCursorResPath: 'response.cursor'
      };
    if (toolType === 'table_parser')
      return {
        sourceMode: 'input',
        sourceSchema: '',
        sourceTable: '',
        channel: '',
        limit: '1000',
        inputPath: '',
        recordPath: '',
        selectFields: '',
        renameMap: '{}',
        filterField: '',
        filterOperator: '',
        filterValue: '',
        parserMultiplier: '1'
      };
    if (toolType === 'db_write')
      return {
        targetSchema: 'ao_data',
        targetTable: 'bronze_result',
        writeMode: 'insert',
        keyColumns: '',
        batchSize: '500',
        channel: '',
        writeSuccessRate: '98'
      };
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
      const [apiConfigsRaw, tablesRaw, settingsRaw] = await Promise.all([
        fetch(`${API_BASE}/api-configs`, {
          method: 'GET',
          headers: { Accept: 'application/json', 'X-AO-ROLE': API_ROLE }
        }),
        fetch(`${API_BASE}/tables`, {
          method: 'GET',
          headers: { Accept: 'application/json', 'X-AO-ROLE': API_ROLE }
        }),
        fetch(`${API_BASE}/settings/effective`, {
          method: 'GET',
          headers: { Accept: 'application/json', 'X-AO-ROLE': API_ROLE }
        })
      ]);

      let apiJson: any = {};
      let tableJson: any = {};
      let settingsJson: any = {};
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
      try {
        settingsJson = await settingsRaw.json();
      } catch {
        settingsJson = {};
      }

      const effective = settingsJson?.effective && typeof settingsJson.effective === 'object' ? settingsJson.effective : {};
      const cfgSchema = String(effective?.api_configs_schema || '').trim();
      const cfgTable = String(effective?.api_configs_table || '').trim();
      if (cfgSchema && cfgTable) apiTemplateStorageRef = `${cfgSchema}.${cfgTable}`;
      else apiTemplateStorageRef = 'ao_system.api_configs_store';
      const deskSchema = String(effective?.workflow_desks_schema || '').trim();
      const deskTable = String(effective?.workflow_desks_table || '').trim();
      if (deskSchema && deskTable) workflowDeskStorageRef = `${deskSchema}.${deskTable}`;
      else workflowDeskStorageRef = 'ao_system.workflow_desks_store';

      const apiRows = Array.isArray(apiJson?.api_configs) ? apiJson.api_configs : [];
      dynamicApiSources = apiRows.map((row: any, idx: number) => {
        const storeId = Number(String(row?.id ?? '').trim());
        const name = String(row?.api_name || row?.name || '').trim() || `API template ${idx + 1}`;
        const method = String(row?.method || 'GET').trim().toUpperCase();
        const baseUrl = String(row?.base_url || '').trim();
        const path = String(row?.path || '').trim();
        const finalPath = path ? (path.startsWith('/') ? path : `/${path}`) : '/';
        const templateUrl = `${baseUrl}${finalPath}`;
        const datasetStoreId = Number.isFinite(storeId) && storeId > 0 ? String(storeId) : String(idx + 1);
        return {
          id: `api_tpl_${datasetStoreId}`,
          storeId: Number.isFinite(storeId) && storeId > 0 ? storeId : undefined,
          name,
          group: 'api_requests' as SourceGroup,
          datasetId: `api_template:${datasetStoreId}`,
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
          },
          rawRow: row,
          parameterDefinitions: normalizeTemplateParameterDefinitions(row),
          paginationParameters: normalizeTemplatePaginationParameters(row),
          dataModel: normalizeTemplateDataModel(row)
        };
      });

      const existingTables = Array.isArray(tableJson?.existing_tables) ? tableJson.existing_tables : [];
      apiBuilderExistingTables = [...existingTables];
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
      const item = JSON.parse(sourceRaw) as DynamicApiSource;
      const config: any = { ...item };
      if (item.group === 'api_requests') {
        config.apiRequest = normalizeApiRequest(item.requestTemplate);
        config.templateId = String(item.id || '').trim();
        config.templateStoreId = item.storeId ? String(item.storeId) : '';
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
          settings.templateStoreId = firstTemplate.storeId ? String(firstTemplate.storeId) : '';
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
    settingsRequestViewMode = 'tree';
    settingsResponseViewMode = 'tree';
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

  function reachableFrom(startId: string) {
    const seen = new Set<string>();
    const stack = [startId];
    while (stack.length) {
      const id = String(stack.pop());
      if (seen.has(id)) continue;
      seen.add(id);
      edges.filter((e) => e.from === id).forEach((e) => stack.push(e.to));
    }
    return seen;
  }

  function topoForSubset(nodeIds: Set<string>): WorkflowNode[] | null {
    const subsetNodes = nodes.filter((n) => nodeIds.has(n.id));
    const indeg = new Map(subsetNodes.map((n) => [n.id, 0]));
    const byId = new Map(subsetNodes.map((n) => [n.id, n]));
    const subsetEdges = edges.filter((e) => nodeIds.has(e.from) && nodeIds.has(e.to));
    subsetEdges.forEach((e) => indeg.set(e.to, Number(indeg.get(e.to) || 0) + 1));
    const q = Array.from(indeg.entries())
      .filter(([, d]) => d === 0)
      .map(([id]) => id);
    const out: WorkflowNode[] = [];
    while (q.length) {
      const id = String(q.shift());
      const n = byId.get(id);
      if (!n) continue;
      out.push(n);
      subsetEdges
        .filter((e) => e.from === id)
        .forEach((e) => {
          const d = Number(indeg.get(e.to) || 0) - 1;
          indeg.set(e.to, d);
          if (d === 0) q.push(e.to);
        });
    }
    return out.length === subsetNodes.length ? out : null;
  }

  function computeOutRows(n: WorkflowNode, inRows: number, apiExec?: ApiNodeExecution | null) {
    let outRows = inRows;
    if (n.type === 'data') {
      if (isApiNode(n)) {
        if (apiExec) {
          outRows = apiExec.payloadCount > 0 ? apiExec.payloadCount : Math.max(inRows, apiExec.totalRequests || 1);
        } else {
          const baseRows = Math.max(1, Number(n.config.size || 1));
          outRows = inRows > 0 ? Math.max(baseRows, inRows) : baseRows;
        }
      } else {
        outRows = inRows > 0 ? inRows : Number(n.config.size || 0);
      }
    }
    if (n.type === 'tool') {
      const cfg = toolCfg(n);
      if (cfg.toolType === 'start_process') outRows = inRows > 0 ? inRows : 1;
      if (cfg.toolType === 'schedule_process') outRows = inRows;
      if (cfg.toolType === 'api_request') {
        if (apiExec) {
          outRows = apiExec.payloadCount > 0 ? apiExec.payloadCount : Math.max(inRows, apiExec.totalRequests || 1);
        } else {
          outRows = inRows > 0 ? inRows : 1;
        }
      }
      if (cfg.toolType === 'table_parser') outRows = Math.max(0, Math.round(inRows * Math.max(0.1, Number(cfg.settings.parserMultiplier || 1))));
      if (cfg.toolType === 'db_write') outRows = Math.max(0, Math.round(inRows * Math.max(0, Math.min(100, Number(cfg.settings.writeSuccessRate || 98))) / 100));
      if (cfg.toolType === 'end_process') outRows = 0;
    }
    return outRows;
  }

  function setNodeRuntime(n: WorkflowNode, inRows: number, outRows: number, forceStatus?: NodeRuntime['status']) {
    const lossRows = Math.max(0, inRows - outRows);
    const lossPct = inRows > 0 ? Number(((lossRows / inRows) * 100).toFixed(2)) : 0;
    const successPct = inRows > 0 ? Number((((inRows - lossRows) / inRows) * 100).toFixed(2)) : n.type === 'tool' && toolCfg(n).toolType !== 'start_process' ? 0 : 100;
    const status: NodeRuntime['status'] =
      forceStatus || (successPct >= 95 ? 'ok' : successPct >= 80 ? 'warn' : 'error');
    nodeRuntime = { ...nodeRuntime, [n.id]: { inRows, outRows, lossRows, lossPct, successPct, status } };
  }

  function applyOutRowsToEdges(n: WorkflowNode, outRows: number, allowedNodeIds?: Set<string>) {
    const outs = edges.filter((e) => e.from === n.id && (!allowedNodeIds || (allowedNodeIds.has(e.from) && allowedNodeIds.has(e.to))));
    const base = outs.length ? Math.floor(outRows / outs.length) : 0;
    let rem = outs.length ? outRows - base * outs.length : 0;
    const next = { ...edgeRows };
    outs.forEach((e) => {
      next[e.id] = base + (rem > 0 ? 1 : 0);
      if (rem > 0) rem -= 1;
    });
    edgeRows = next;
  }

  async function startNodeChain(nodeId: string) {
    const startNode = nodes.find((n) => n.id === nodeId);
    if (!startNode) return;

    chainRunId += 1;
    const myRunId = chainRunId;
    chainRunActive = true;
    chainRunPaused = false;
    chainRunStopRequested = false;
    chainCurrentNodeId = '';
    banner = '';
    issues = validate();
    if (issues.some((x) => x.level === 'error')) {
      banner = 'Исправь ошибки схемы перед запуском';
      chainRunActive = false;
      return;
    }

    const reachableIds = reachableFrom(nodeId);
    const order = topoForSubset(reachableIds);
    if (!order) {
      banner = 'Обнаружен цикл в цепочке от выбранной ноды';
      chainRunActive = false;
      return;
    }

    nodeRuntime = {};
    edgeRows = {};

    for (const n of order) {
      if (!chainRunActive || myRunId !== chainRunId) return;
      if (chainRunStopRequested) break;
      while (chainRunPaused && !chainRunStopRequested && chainRunActive && myRunId === chainRunId) {
        const current = nodeRuntime[n.id];
        if (current) setNodeRuntime(n, current.inRows, current.outRows, 'paused');
        await delayMs(120);
      }
      if (chainRunStopRequested || !chainRunActive || myRunId !== chainRunId) break;

      chainCurrentNodeId = n.id;
      const inEdges = edges.filter((e) => e.to === n.id && reachableIds.has(e.from) && reachableIds.has(e.to));
      const inRows = inEdges.reduce((s, e) => s + Number(edgeRows[e.id] || 0), 0);
      setNodeRuntime(n, inRows, inRows, 'running');

      let apiExec: ApiNodeExecution | null = null;
      if (isApiNode(n) || isApiToolNode(n)) {
        apiExec = await executeApiNode(n.id);
      } else {
        await delayMs(180);
      }
      if (!chainRunActive || myRunId !== chainRunId) return;

      const outRows = computeOutRows(n, inRows, apiExec);
      const forcedStatus: NodeRuntime['status'] | undefined = chainRunStopRequested ? 'stopped' : undefined;
      setNodeRuntime(n, inRows, outRows, forcedStatus);
      applyOutRowsToEdges(n, outRows, reachableIds);
      await delayMs(80);
    }

    chainCurrentNodeId = '';
    if (chainRunStopRequested) {
      banner = 'Выполнение остановлено';
    } else {
      const executed = order.length;
      banner = `Цепочка выполнена: узлов ${executed}`;
    }
    chainRunActive = false;
    chainRunPaused = false;
    chainRunStopRequested = false;
  }

  function toggleChainPause() {
    if (!chainRunActive) return;
    chainRunPaused = !chainRunPaused;
    banner = chainRunPaused ? 'Выполнение на паузе' : 'Выполнение продолжено';
  }

  function stopChainRun() {
    if (!chainRunActive) return;
    chainRunStopRequested = true;
    chainRunPaused = false;
  }

  function validate() {
    const out: WorkflowIssue[] = [];
    if (!nodes.some((n) => n.type === 'data')) out.push({ level: 'warn', text: 'Нет источников данных: поток может начинаться от Старт процесса' });
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

  async function executeApiNode(nodeId: string): Promise<ApiNodeExecution | null> {
    const node = nodes.find((n) => n.id === nodeId);
    if (!node || (!isApiNode(node) && !isApiToolNode(node))) {
      banner = 'Этот узел не поддерживает Execute Node';
      return null;
    }

    const request = getApiRequestForNode(node);
    const method = String(request.method || 'GET').trim().toUpperCase() || 'GET';
    const urlRaw = String(request.url || '').trim();
    if (!urlRaw) {
      banner = 'Укажи URL в настройке API-узла';
      return null;
    }

    try {
      nodeExecutionLoading = { ...nodeExecutionLoading, [nodeId]: true };
      settingsRequestViewMode = 'tree';
      settingsResponseViewMode = 'tree';
      const pagination = getPaginationForNode(node);
      const headersObj = parseObjectJsonSafe('Headers JSON', request.headersText);
      const baseQueryObj = parseObjectJsonSafe('Query JSON', request.queryText);
      const baseBodyObj = parseAnyJsonSafe('Body JSON', request.bodyText);

      const requestedAliasesSet = new Set<string>();
      collectParameterTokens(urlRaw, requestedAliasesSet);
      collectParameterTokens(headersObj, requestedAliasesSet);
      collectParameterTokens(baseQueryObj, requestedAliasesSet);
      collectParameterTokens(baseBodyObj, requestedAliasesSet);
      const requestedAliases = Array.from(requestedAliasesSet);

      const templateRefs = nodeTemplateRefs(node);
      const templateSource = resolveTemplateSourceForNode(node, request);
      let resolveIssues: Record<string, string> = {};
      let entityParameterMaps: Array<Record<string, any>> = [{}];
      if (requestedAliases.length) {
        if (!templateSource) {
          throw new Error(
            `Не найден привязанный API-шаблон для Request-ноды. Проверь поле "Шаблон API". refs: ${templateRefs.join(', ') || 'пусто'}`
          );
        }
        const dispatchCfg = templateDispatchConfig(templateSource);
        const paginationSeed = resolveAliasesFromPaginationFirstValues(templateSource, requestedAliases);
        const unresolvedAfterPagination = requestedAliases.filter((alias) => !findAliasInMap(paginationSeed, alias).found);

        let dataRows: Array<Record<string, any>> = [];
        if (unresolvedAfterPagination.length) {
          try {
            const fromRows = await resolveAliasRowsFromDataModel(templateSource, unresolvedAfterPagination);
            dataRows = Array.isArray(fromRows.rows) ? fromRows.rows : [];
            Object.entries(fromRows.issues || {}).forEach(([alias, reason]) => {
              if (!resolveIssues[alias]) resolveIssues[alias] = String(reason || '');
            });
          } catch (e: any) {
            const msg = String(e?.message || e || 'ошибка чтения конструктора данных');
            unresolvedAfterPagination.forEach((alias) => {
              if (!resolveIssues[alias]) resolveIssues[alias] = msg;
            });
          }
        }

        const fromDefinitions = await resolveAliasesFromParameterDefinitions(
          templateSource.parameterDefinitions || [],
          unresolvedAfterPagination
        );
        Object.entries(fromDefinitions.issues || {}).forEach(([alias, reason]) => {
          if (!resolveIssues[alias]) resolveIssues[alias] = String(reason || '');
        });

        const rowsBase = dataRows.length ? dataRows : [{}];
        let mergedRows = rowsBase.map((row) => ({
          ...row,
          ...fromDefinitions.map,
          ...paginationSeed
        }));

        if (dispatchCfg.dispatchMode === 'group_by' && dispatchCfg.groupByAliases.length && mergedRows.length > 1) {
          const grouped = groupRowsByAliases(mergedRows, dispatchCfg.groupByAliases);
          mergedRows = Array.from(grouped.values()).map((g: any) => ({
            ...(g?.rows?.[0] || {}),
            ...(g?.key || {})
          }));
        }

        entityParameterMaps = mergedRows.length ? mergedRows : [{ ...fromDefinitions.map, ...paginationSeed }];
      }

      const startTs = Date.now();
      const requestsSent: any[] = [];
      const responses: Array<{ page: number; status: number; response: any; entity_index?: number }> = [];
      let success = 0;
      let failed = 0;
      let lastStatus = 0;
      const stopReasons: Array<{ entity_index: number; stop_reason: string }> = [];
      const paginationAliasesLower = new Set(
        (templateSource?.paginationParameters || [])
          .map((p) => String(p?.alias || '').trim().toLowerCase())
          .filter(Boolean)
      );
      const nodeOverrides = getNodeParameterOverrides(node);
      const finalEntityMaps = (entityParameterMaps.length ? entityParameterMaps : [{}]).map((m) => ({
        ...m,
        ...nodeOverrides
      }));

      for (let entityIdx = 0; entityIdx < finalEntityMaps.length; entityIdx += 1) {
        const resolvedParameters = finalEntityMaps[entityIdx] || {};
        const headersForEntity = deepClone(headersObj || {});
        const queryForEntity = deepClone(baseQueryObj || {});
        const bodyForEntity = deepClone(baseBodyObj || {});
        const resolvedUrlRaw = replaceParameterTokens(urlRaw, resolvedParameters);
        applyParametersToValue(headersForEntity, resolvedParameters);
        applyParametersToValue(queryForEntity, resolvedParameters);
        applyParametersToValue(bodyForEntity, resolvedParameters);

        const unresolvedBeforeStrip = new Set<string>();
        collectParameterTokens(resolvedUrlRaw, unresolvedBeforeStrip);
        collectParameterTokens(headersForEntity, unresolvedBeforeStrip);
        collectParameterTokens(queryForEntity, unresolvedBeforeStrip);
        collectParameterTokens(bodyForEntity, unresolvedBeforeStrip);
        const unresolvedPaginationAliasesLower = new Set(
          Array.from(unresolvedBeforeStrip)
            .map((a) => String(a || '').trim().toLowerCase())
            .filter((a) => paginationAliasesLower.has(a))
        );
        if (unresolvedPaginationAliasesLower.size) {
          stripUnresolvedTemplateTokens(queryForEntity, unresolvedPaginationAliasesLower);
          if (bodyForEntity && typeof bodyForEntity === 'object') {
            stripUnresolvedTemplateTokens(bodyForEntity, unresolvedPaginationAliasesLower);
          }
        }

        const unresolvedTokens = new Set<string>();
        collectParameterTokens(resolvedUrlRaw, unresolvedTokens);
        collectParameterTokens(headersForEntity, unresolvedTokens);
        collectParameterTokens(queryForEntity, unresolvedTokens);
        collectParameterTokens(bodyForEntity, unresolvedTokens);
        if (!Object.keys(resolveIssues).length && unresolvedTokens.size) {
          Array.from(unresolvedTokens).forEach((alias) => {
            resolveIssues[alias] = 'alias не найден в источниках шаблона (конструктор/параметры/первое значение пагинации)';
          });
        }
        if (unresolvedTokens.size) {
          const unresolvedList = Array.from(unresolvedTokens);
          const details = unresolvedList
            .map((alias) => {
              const issue = findAliasInMap(resolveIssues, alias);
              return issue.found && issue.value ? `${alias} (${issue.value})` : alias;
            })
            .join(', ');
          throw new Error(`Не удалось подставить параметры (сущность ${entityIdx + 1}): ${details}`);
        }

        let entityStopReason = '';
        let pageNo = 1;
        let pageNumberValue = Math.max(1, Number(pagination.startPage || 1));
        let offsetValue = Math.max(0, Number(pagination.startOffset || 0));
        let cursorValue: any = undefined;
        let lastResponseFingerprint = '';
        let sameResponseStreak = 0;
        const absoluteSafetyLimit = 1000;

        while (true) {
          if (pageNo > absoluteSafetyLimit) {
            entityStopReason = `Достигнут защитный лимит страниц (${absoluteSafetyLimit})`;
            break;
          }
          const queryObj = deepClone(queryForEntity || {});
          const bodyRaw = deepClone(bodyForEntity || {});
          const bodyObj = bodyRaw && typeof bodyRaw === 'object' ? bodyRaw : {};
          const targetObj = pagination.target === 'body' ? bodyObj : queryObj;
          if (pagination.enabled) {
            if (pagination.mode === 'page_number') {
              setByPath(targetObj, pagination.pageParam, pageNumberValue);
              if (pagination.limitParam) setByPath(targetObj, pagination.limitParam, Math.max(1, Number(pagination.limitValue || 1)));
            } else if (pagination.mode === 'offset_limit') {
              setByPath(targetObj, pagination.offsetParam, offsetValue);
              if (pagination.limitParam) setByPath(targetObj, pagination.limitParam, Math.max(1, Number(pagination.limitValue || 1)));
            } else if (pagination.mode === 'cursor') {
              if (pageNo > 1) {
                if (cursorValue === undefined || cursorValue === null || cursorValue === '') {
                  if (pagination.stopOnMissingValue) {
                    entityStopReason = 'Остановка: курсор не найден в предыдущем ответе';
                    break;
                  }
                }
                setByPath(targetObj, pagination.cursorReqPath, cursorValue);
              }
            }
          }

          const url = new URL(resolvedUrlRaw);
          Object.entries(queryObj || {}).forEach(([k, v]) => {
            if (v !== undefined && v !== null) url.searchParams.set(k, String(v));
          });

          const reqPayload = {
            entity_index: entityIdx + 1,
            page: pageNo,
            method,
            url: url.toString(),
            headers: headersForEntity,
            query: queryObj,
            body: method === 'GET' || method === 'DELETE' ? undefined : bodyObj
          };
          requestsSent.push(reqPayload);

          const resp = await fetch(url.toString(), {
            method,
            headers: headersForEntity,
            body: method === 'GET' || method === 'DELETE' ? undefined : JSON.stringify(bodyObj)
          });
          lastStatus = resp.status;
          if (resp.ok) success += 1;
          else failed += 1;
          const text = await resp.text();
          let responseBody: any = text;
          try {
            responseBody = text ? JSON.parse(text) : {};
          } catch {
            responseBody = text;
          }
          responses.push({ entity_index: entityIdx + 1, page: pageNo, status: resp.status, response: responseBody });
          if (!resp.ok) {
            if (pagination.stopOnHttpError) {
              entityStopReason = `HTTP ошибка ${resp.status}`;
              break;
            }
          }

          if (!pagination.enabled || pagination.mode === 'none') break;
          if (pagination.useMaxPages) {
            const maxPages = Math.max(1, Number(pagination.maxPages || 1));
            if (pageNo >= maxPages) {
              entityStopReason = `Достигнут лимит страниц (${maxPages})`;
              break;
            }
          }

          const stopRuleHit = findTriggeredStopRule(responseBody, pagination.stopRules || [], resolvedParameters, {
            body: bodyObj,
            query: queryObj,
            headers: headersForEntity
          });
          if (stopRuleHit) {
            entityStopReason = `Сработало условие остановки: ${stopRuleHit.path}`;
            break;
          }

          if (pagination.stopOnSameResponse) {
            const fingerprint =
              typeof responseBody === 'string'
                ? responseBody
                : (() => {
                    try {
                      return JSON.stringify(responseBody);
                    } catch {
                      return String(responseBody);
                    }
                  })();
            if (fingerprint === lastResponseFingerprint) sameResponseStreak += 1;
            else sameResponseStreak = 1;
            lastResponseFingerprint = fingerprint;
            if (sameResponseStreak >= Math.max(2, Math.min(50, Number(pagination.sameResponseLimit || 5)))) {
              entityStopReason = `Получено ${sameResponseStreak} одинаковых ответов подряд`;
              break;
            }
          }

          const dataArr = pagination.dataPath ? getByPath({ response: responseBody }, pagination.dataPath) : undefined;
          const isMissingData = pagination.dataPath
            ? dataArr === undefined || dataArr === null || (Array.isArray(dataArr) && dataArr.length === 0)
            : false;
          if (pagination.mode === 'page_number') {
            if (pagination.stopOnMissingValue && isMissingData) {
              entityStopReason = 'Остановка: массив данных пустой или отсутствует';
              break;
            }
            pageNumberValue += 1;
          } else if (pagination.mode === 'offset_limit') {
            if (pagination.stopOnMissingValue && isMissingData) {
              entityStopReason = 'Остановка: массив данных пустой или отсутствует';
              break;
            }
            offsetValue += Math.max(1, Number(pagination.limitValue || 1));
          } else if (pagination.mode === 'cursor') {
            cursorValue = getByPath({ response: responseBody }, pagination.cursorResPath);
            if (pagination.stopOnMissingValue && isMissingData) {
              entityStopReason = 'Остановка: массив данных пустой или отсутствует';
              break;
            }
            if (
              pagination.stopOnMissingValue &&
              (cursorValue === undefined || cursorValue === null || cursorValue === '')
            ) {
              entityStopReason = 'Остановка: курсор пустой';
              break;
            }
          }

          pageNo += 1;
          if (pagination.useDelay) {
            await delayMs(Math.max(0, Number(pagination.pauseMs || 0)));
          }
        }

        if (entityStopReason) {
          stopReasons.push({ entity_index: entityIdx + 1, stop_reason: entityStopReason });
        }
      }

      const durationMs = Date.now() - startTs;
      const payloadCount = responses.reduce((acc, item) => {
        const arr = pagination.dataPath ? getByPath({ response: item.response }, pagination.dataPath) : undefined;
        return acc + (Array.isArray(arr) ? arr.length : 0);
      }, 0);

      const requestPreview =
        requestsSent.length <= 1
          ? {
              request: requestsSent[0] || null,
              template_storage_ref: apiTemplateStorageRef,
              template_store_id: templateSource?.storeId || nodeTemplateStoreId(node) || undefined,
              template_ref: templateRefs.length ? templateRefs[0] : undefined,
              resolved_parameters:
                finalEntityMaps.length === 1
                  ? finalEntityMaps[0]
                  : finalEntityMaps.slice(0, 5).map((p, idx) => ({ entity_index: idx + 1, values: p })),
              resolve_issues: Object.keys(resolveIssues).length ? resolveIssues : undefined
            }
          : {
              total_requests: requestsSent.length,
              shown_requests: requestsSent.length,
              requests: requestsSent,
              template_storage_ref: apiTemplateStorageRef,
              template_store_id: templateSource?.storeId || nodeTemplateStoreId(node) || undefined,
              template_ref: templateRefs.length ? templateRefs[0] : undefined,
              entities: finalEntityMaps.length,
              resolved_parameters:
                finalEntityMaps.length === 1
                  ? finalEntityMaps[0]
                  : finalEntityMaps.slice(0, 5).map((p, idx) => ({ entity_index: idx + 1, values: p })),
              resolve_issues: Object.keys(resolveIssues).length ? resolveIssues : undefined
            };
      const responsePreview = {
        total_requests: requestsSent.length,
        request_entities: finalEntityMaps.length,
        success,
        failed,
        stop_reasons: stopReasons.length ? stopReasons : undefined,
        responses,
        template_storage_ref: apiTemplateStorageRef,
        template_store_id: templateSource?.storeId || nodeTemplateStoreId(node) || undefined,
        template_ref: templateRefs.length ? templateRefs[0] : undefined,
        resolved_parameters:
          finalEntityMaps.length === 1
            ? finalEntityMaps[0]
            : finalEntityMaps.slice(0, 5).map((p, idx) => ({ entity_index: idx + 1, values: p })),
        resolve_issues: Object.keys(resolveIssues).length ? resolveIssues : undefined
      };
      const payloadSize = new TextEncoder().encode(JSON.stringify(responsePreview)).length;

      const result: ApiNodeExecution = {
        startedAt: new Date(startTs).toISOString(),
        durationMs,
        status: lastStatus,
        ok: failed === 0,
        totalRequests: requestsSent.length,
        payloadCount,
        payloadSize,
        requestPreview,
        responsePreview
      };
      nodeExecutions = {
        ...nodeExecutions,
        [nodeId]: result
      };
      banner = failed > 0 ? `Узел выполнен с ошибками: ${failed}` : `Узел выполнен, запросов: ${requestsSent.length}`;
      return result;
    } catch (e: any) {
      const failedResult: ApiNodeExecution = {
        startedAt: new Date().toISOString(),
        durationMs: 0,
        status: 0,
        ok: false,
        totalRequests: 0,
        payloadCount: 0,
        payloadSize: 0,
        requestPreview: {},
        responsePreview: {},
        error: String(e?.message || e || 'Ошибка выполнения узла')
      };
      nodeExecutions = {
        ...nodeExecutions,
        [nodeId]: failedResult
      };
      banner = String(e?.message || e || 'Ошибка выполнения узла');
      return failedResult;
    } finally {
      nodeExecutionLoading = { ...nodeExecutionLoading, [nodeId]: false };
    }
  }

  function openFullApiBuilder(nodeId: string) {
    const node = nodes.find((n) => n.id === nodeId);
    if (!node || (!isApiNode(node) && !isApiToolNode(node))) return;
    const templateStoreId = nodeTemplateStoreId(node);
    const q = new URLSearchParams();
    if (templateStoreId) q.set('api_store_id', templateStoreId);
    if (deskId > 0) q.set('desk_id', String(deskId));
    const nodeName = String(node.config?.name || '').trim();
    if (nodeName) q.set('from_node', nodeName);
    window.location.hash = q.toString() ? `#desk/workflow?${q.toString()}` : '#desk/workflow';
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
    chainRunActive = false;
    chainRunPaused = false;
    chainRunStopRequested = false;
    chainCurrentNodeId = '';
    issues = [];
    summary = { sourceRows: 0, finalRows: 0, lossRows: 0, lossPct: 0, successPct: 0 };
    banner = '';
  }

  onMount(async () => {
    resetCanvas();
    await loadDynamicSourceCatalog();
    await loadWorkflowDeskFromServer();
    await refreshAutomationContour();
    deskInitialized = true;
    restartDeskAutosaveTimer();
  });

  onDestroy(() => {
    clearDeskAutosaveTimer();
  });
</script>

<section class="panel workflow-v2">
  <h3>Workflow-конструктор данных</h3>

  <div class="desk-bar">
    <div class="desk-storage">
      <span>Хранятся в таблице:</span>
      <strong>{workflowDeskStorageRef}</strong>
      <span>ID: {deskId || '-'}</span>
      <span>Ревизия: {deskRevision || '-'}</span>
      <span class={deskDirty ? 'dirty-flag' : 'clean-flag'}>{deskDirty ? 'Есть несохраненные изменения' : 'Синхронизировано'}</span>
    </div>
    <div class="desk-actions">
      <label>
        Название
        <input value={deskName} on:input={(e) => (deskName = inputValue(e))} />
      </label>
      <label>
        Описание
        <input value={deskDescription} on:input={(e) => (deskDescription = inputValue(e))} />
      </label>
      <button class="mini" on:click={() => saveDesk(false)} disabled={deskSaving || deskLoading}>
        {deskSaving ? 'Сохранение...' : 'Сохранить'}
      </button>
      <button class="mini" on:click={reloadDeskFromServer} disabled={deskSaving || deskLoading}>
        {deskLoading ? 'Загрузка...' : 'Перезагрузить'}
      </button>
      <button class="mini toggle-btn" class:active={deskAutosaveEnabled} on:click={toggleDeskAutosave}>
        {deskAutosaveEnabled ? 'Автосохранение: вкл' : 'Автосохранение: выкл'}
      </button>
      <label class="sec-label">
        Интервал (сек)
        <input type="number" min="3" value={String(deskAutosaveSeconds)} on:input={onDeskAutosaveSecondsInput} disabled={!deskAutosaveEnabled} />
      </label>
      <span class="saved-at">{deskLastSavedAt ? `Сохранено: ${deskLastSavedAt}` : 'Пока не сохранено'}</span>
    </div>
    {#if deskSaveError}
      <div class="desk-error">{deskSaveError}</div>
    {/if}
  </div>

  <div class="ops-bar">
    <div class="ops-actions">
      <button class="mini primary" on:click={publishDeskVersion} disabled={!deskId || publishBusy || deskLoading || deskSaving}>
        {publishBusy ? 'Публикация...' : 'Publish desk'}
      </button>
      <button class="mini" on:click={() => triggerPublishedProcess('')} disabled={!deskId || triggerBusy || !publishedDeskReady}>
        {triggerBusy ? 'Запуск...' : 'Trigger manual'}
      </button>
      <button class="mini" on:click={refreshAutomationContour} disabled={monitorBusy || !deskId}>
        {monitorBusy ? 'Обновление...' : 'Обновить мониторинг'}
      </button>
      <span class="ops-ref">
        Published version: <strong>{publishedDeskVersionId || '-'}</strong>
      </span>
      <span class={publishedDeskReady ? 'clean-flag' : 'dirty-flag'}>
        {publishedDeskReady ? 'Опубликовано' : 'Публикации нет'}
      </span>
    </div>
    <div class="ops-grid">
      <div class="ops-card">
        <h5>Scheduler</h5>
        <div class="kv"><span>enabled</span><strong>{schedulerView.enabled ? 'yes' : 'no'}</strong></div>
        <div class="kv"><span>last tick</span><strong>{schedulerView.last_tick_at || '-'}</strong></div>
        <div class="kv"><span>worker tick</span><strong>{schedulerView.worker_last_tick_at || '-'}</strong></div>
        <div class="kv"><span>queue</span><strong>{schedulerView.queue_depth} / running {schedulerView.queue_running}</strong></div>
        <div class="kv"><span>dead letter</span><strong>{schedulerView.queue_dead_letter}</strong></div>
        <div class="kv"><span>events backlog</span><strong>{schedulerView.dependency_event_backlog}</strong></div>
        {#if schedulerView.last_error}
          <div class="ops-error">scheduler error: {schedulerView.last_error}</div>
        {/if}
        {#if schedulerView.worker_last_error}
          <div class="ops-error">worker error: {schedulerView.worker_last_error}</div>
        {/if}
      </div>
      <div class="ops-card">
        <h5>Start-процессы ({publishedProcesses.length})</h5>
        {#if publishedProcesses.length}
          {#each publishedProcesses as p (p.start_node_id)}
            <div class="process-row">
              <div class="process-main">
                <strong>{p.name || p.start_node_id}</strong>
                <span>{p.process_code}</span>
                <span>{p.trigger_type} / {p.execution_scope_mode}</span>
                <span>{p.scope_type}:{p.scope_ref}</span>
              </div>
              <div class="process-actions">
                <button
                  class="mini toggle-btn"
                  class:active={Boolean(p.is_enabled)}
                  on:click={() => togglePublishedProcess(p.start_node_id, !p.is_enabled)}
                  disabled={Boolean(processBusyByNode[p.start_node_id])}
                >
                  {Boolean(p.is_enabled) ? 'Вкл' : 'Выкл'}
                </button>
                <button class="mini" on:click={() => triggerPublishedProcess(p.start_node_id)} disabled={triggerBusy || !Boolean(p.is_enabled)}>
                  Trigger
                </button>
              </div>
              {#if p.last_run}
                <div class="process-last">
                  last: {p.last_run.status} / {p.last_run.trigger_type} / {p.last_run.started_at || '-'}
                </div>
              {/if}
            </div>
          {/each}
        {:else}
          <div class="empty">Start-процессы появятся после публикации</div>
        {/if}
      </div>
      <div class="ops-card">
        <h5>Runs / Jobs</h5>
        <div class="ops-columns">
          <div>
            <div class="ops-subhead">Runs ({processRuns.length})</div>
            {#if processRuns.length}
              {#each processRuns.slice(0, 8) as run (run.run_uid)}
                <div class="compact-row">
                  <span>{run.status}</span>
                  <span>{run.start_node_id}</span>
                  <code>{run.run_uid}</code>
                </div>
              {/each}
            {:else}
              <div class="empty">Нет запусков</div>
            {/if}
          </div>
          <div>
            <div class="ops-subhead">Jobs ({workflowJobs.length})</div>
            {#if workflowJobs.length}
              {#each workflowJobs.slice(0, 8) as job (`${job.job_id}`)}
                <div class="compact-row">
                  <span>{job.status}</span>
                  <span>{job.job_type}</span>
                  <span>#{job.job_id}</span>
                </div>
              {/each}
            {:else}
              <div class="empty">Нет jobs</div>
            {/if}
          </div>
        </div>
      </div>
    </div>
  </div>

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
        {#each visibleSourceGroups as g (g.key)}
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
          {@const exec = nodeExecutions[node.id]}
          <div
            class="node {node.type} {selectedNodeId === node.id ? 'selected' : ''}"
            style={`left:${node.x}px;top:${node.y}px;`}
            on:mousedown={(e) => startNodeDrag(e, node)}
            on:click={() => (selectedNodeId = node.id)}
            on:dblclick={(e) => onNodeDoubleClick(e, node.id)}
          >
            <div class="node-head">
              <strong>{node.config.name}</strong>
              <div class="node-controls">
                <button
                  class="ctrl-btn"
                  title="Старт цепочки от этой ноды"
                  on:click|stopPropagation={() => startNodeChain(node.id)}
                >></button>
                <button
                  class="ctrl-btn"
                  title="Пауза/продолжить"
                  on:click|stopPropagation={toggleChainPause}
                  disabled={!chainRunActive}
                >||</button>
                <button
                  class="ctrl-btn"
                  title="Остановить выполнение"
                  on:click|stopPropagation={stopChainRun}
                  disabled={!chainRunActive}
                >[]</button>
                <button class="ctrl-btn delete-btn" title="Удалить ноду" on:click|stopPropagation={() => deleteNode(node.id)}>x</button>
              </div>
            </div>
            <div class="node-meta">{node.type === 'data' ? node.config.datasetId : node.config.toolType}</div>
            {#if chainRunActive && chainCurrentNodeId === node.id}
              <div class="runtime running">Выполняется...</div>
            {/if}
            {#if isApiNode(node) || isApiToolNode(node)}
              {@const req = getApiRequestForNode(node)}
              {@const templateStoreId = nodeTemplateStoreId(node)}
              <div class="api-meta">{req.method || 'GET'} {req.url || ''}</div>
              <div class="api-template-meta">
                <span>Шаблон ID: {templateStoreId || '-'}</span>
                <span>Хранилище: {apiTemplateStorageRef}</span>
              </div>
              <div class="api-stats">
                <span>Страницы: {exec ? exec.totalRequests : '-'}</span>
                <span>Записей: {exec ? exec.payloadCount : '-'}</span>
                <span>Размер: {exec ? formatBytesShort(exec.payloadSize) : '-'}</span>
                <span>Время: {exec ? formatDurationShort(exec.durationMs) : '-'}</span>
              </div>
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
            <label>
              Источник
              <select
                value={toolCfg(selectedNode).settings.sourceMode || 'input'}
                on:change={(e) => updateSetting(selectedNode.id, 'sourceMode', selectValue(e))}
              >
                <option value="input">Вход предыдущей ноды</option>
                <option value="table">Таблица</option>
                <option value="process_bus">Process bus</option>
              </select>
            </label>
            <label>Схема<input value={toolCfg(selectedNode).settings.sourceSchema || ''} on:input={(e) => onSettingInput(selectedNode.id, 'sourceSchema', e)} /></label>
            <label>Таблица<input value={toolCfg(selectedNode).settings.sourceTable || ''} on:input={(e) => onSettingInput(selectedNode.id, 'sourceTable', e)} /></label>
            <label>Канал bus<input value={toolCfg(selectedNode).settings.channel || ''} on:input={(e) => onSettingInput(selectedNode.id, 'channel', e)} /></label>
            <label>Лимит<input value={toolCfg(selectedNode).settings.limit || ''} on:input={(e) => onSettingInput(selectedNode.id, 'limit', e)} /></label>
            <label>Путь во входе<input value={toolCfg(selectedNode).settings.inputPath || ''} on:input={(e) => onSettingInput(selectedNode.id, 'inputPath', e)} /></label>
            <label>Путь записи<input value={toolCfg(selectedNode).settings.recordPath || ''} on:input={(e) => onSettingInput(selectedNode.id, 'recordPath', e)} /></label>
            <label>Поля (csv)<input value={toolCfg(selectedNode).settings.selectFields || ''} on:input={(e) => onSettingInput(selectedNode.id, 'selectFields', e)} /></label>
            <label>Переименовать JSON<input value={toolCfg(selectedNode).settings.renameMap || ''} on:input={(e) => onSettingInput(selectedNode.id, 'renameMap', e)} /></label>
            <label>Фильтр: поле<input value={toolCfg(selectedNode).settings.filterField || ''} on:input={(e) => onSettingInput(selectedNode.id, 'filterField', e)} /></label>
            <label>Фильтр: оператор<input value={toolCfg(selectedNode).settings.filterOperator || ''} on:input={(e) => onSettingInput(selectedNode.id, 'filterOperator', e)} /></label>
            <label>Фильтр: значение<input value={toolCfg(selectedNode).settings.filterValue || ''} on:input={(e) => onSettingInput(selectedNode.id, 'filterValue', e)} /></label>
            <label>Множитель парсинга<input value={toolCfg(selectedNode).settings.parserMultiplier || ''} on:input={(e) => onSettingInput(selectedNode.id, 'parserMultiplier', e)} /></label>
          {/if}
          {#if toolCfg(selectedNode).toolType === 'db_write'}
            <label>Схема<input value={toolCfg(selectedNode).settings.targetSchema || ''} on:input={(e) => onSettingInput(selectedNode.id, 'targetSchema', e)} /></label>
            <label>Таблица<input value={toolCfg(selectedNode).settings.targetTable || ''} on:input={(e) => onSettingInput(selectedNode.id, 'targetTable', e)} /></label>
            <label>
              Режим записи
              <select
                value={toolCfg(selectedNode).settings.writeMode || 'insert'}
                on:change={(e) => updateSetting(selectedNode.id, 'writeMode', selectValue(e))}
              >
                <option value="insert">insert</option>
                <option value="upsert">upsert</option>
                <option value="update_by_key">update_by_key</option>
              </select>
            </label>
            <label>Ключевые колонки (csv)<input value={toolCfg(selectedNode).settings.keyColumns || ''} on:input={(e) => onSettingInput(selectedNode.id, 'keyColumns', e)} /></label>
            <label>Batch size<input value={toolCfg(selectedNode).settings.batchSize || ''} on:input={(e) => onSettingInput(selectedNode.id, 'batchSize', e)} /></label>
            <label>Канал fallback<input value={toolCfg(selectedNode).settings.channel || ''} on:input={(e) => onSettingInput(selectedNode.id, 'channel', e)} /></label>
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
    <div class="node-modal" class:node-modal-wide={isApiNode(settingsNode) || isApiToolNode(settingsNode)}>
      <div class="node-modal-head">
        <h4>Настройка узла: {settingsNode.config.name}</h4>
        <button class="close-btn" on:click={closeSettingsModal}>x</button>
      </div>

      {#if settingsNode.type === 'tool' && toolCfg(settingsNode).toolType === 'start_process'}
        <div class="node-modal-body">
          <div class="help">Настройка серверного процесса: расписание, scope и политика запуска.</div>
          <label>
            Активен
            <select
              value={toolCfg(settingsNode).settings.isEnabled || 'true'}
              on:change={(e) => updateSetting(settingsNode.id, 'isEnabled', selectValue(e))}
            >
              <option value="true">Да</option>
              <option value="false">Нет</option>
            </select>
          </label>
          <label>
            Process code
            <input
              value={toolCfg(settingsNode).settings.processCode || ''}
              on:input={(e) => onSettingInput(settingsNode.id, 'processCode', e)}
              placeholder="например client_cards_sync"
            />
          </label>
          <label>
            Тип запуска
            <select
              value={toolCfg(settingsNode).settings.triggerType || 'interval'}
              on:change={(e) => updateSetting(settingsNode.id, 'triggerType', selectValue(e))}
            >
              <option value="interval">Интервал</option>
              <option value="cron">Cron</option>
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
          {:else if (toolCfg(settingsNode).settings.triggerType || 'interval') === 'cron'}
            <label>
              Cron выражение
              <input
                value={toolCfg(settingsNode).settings.cron || '0 * * * *'}
                on:input={(e) => onSettingInput(settingsNode.id, 'cron', e)}
                placeholder="0 * * * *"
              />
            </label>
          {/if}
          <div class="interval-grid">
            <label>
              Timezone
              <input value={toolCfg(settingsNode).settings.timezone || 'UTC'} on:input={(e) => onSettingInput(settingsNode.id, 'timezone', e)} />
            </label>
            <label>
              Run policy
              <select
                value={toolCfg(settingsNode).settings.runPolicy || 'single_instance'}
                on:change={(e) => updateSetting(settingsNode.id, 'runPolicy', selectValue(e))}
              >
                <option value="single_instance">single_instance</option>
                <option value="allow_parallel">allow_parallel</option>
              </select>
            </label>
          </div>
          <label>
            Execution scope mode
            <select
              value={toolCfg(settingsNode).settings.executionScopeMode || 'single_global'}
              on:change={(e) => {
                const mode = selectValue(e);
                updateSetting(settingsNode.id, 'executionScopeMode', mode);
                applyScopeTypeByMode(settingsNode.id, mode);
              }}
            >
              <option value="single_global">single_global</option>
              <option value="for_each_tenant">for_each_tenant</option>
              <option value="for_each_source_account">for_each_source_account</option>
              <option value="for_each_segment">for_each_segment</option>
              <option value="for_each_partition">for_each_partition</option>
            </select>
          </label>
          <div class="interval-grid">
            <label>
              Scope type
              <select
                value={toolCfg(settingsNode).settings.scopeType || 'global'}
                on:change={(e) => updateSetting(settingsNode.id, 'scopeType', selectValue(e))}
              >
                <option value="global">global</option>
                <option value="tenant">tenant</option>
                <option value="source_account">source_account</option>
                <option value="segment">segment</option>
                <option value="partition">partition</option>
                <option value="system">system</option>
              </select>
            </label>
            <label>
              Scope ref
              <input value={toolCfg(settingsNode).settings.scopeRef || ''} on:input={(e) => onSettingInput(settingsNode.id, 'scopeRef', e)} />
            </label>
          </div>
          <div class="interval-grid">
            <label>
              Tenant id (nullable)
              <input value={toolCfg(settingsNode).settings.tenantId || ''} on:input={(e) => onSettingInput(settingsNode.id, 'tenantId', e)} />
            </label>
            <label>
              Input scope
              <input value={toolCfg(settingsNode).settings.inputScope || ''} on:input={(e) => onSettingInput(settingsNode.id, 'inputScope', e)} />
            </label>
          </div>
          <label>
            Output scope
            <input value={toolCfg(settingsNode).settings.outputScope || ''} on:input={(e) => onSettingInput(settingsNode.id, 'outputScope', e)} />
          </label>
          <label>
            context_json
            <textarea
              rows="5"
              value={toolCfg(settingsNode).settings.contextJson || '{}'}
              on:input={(e) => onSettingInput(settingsNode.id, 'contextJson', e)}
            ></textarea>
          </label>
          <label>
            scope_source
            <textarea
              rows="5"
              value={toolCfg(settingsNode).settings.scopeSource || '{}'}
              on:input={(e) => onSettingInput(settingsNode.id, 'scopeSource', e)}
            ></textarea>
          </label>
        </div>
      {/if}

      {#if isApiNode(settingsNode) || isApiToolNode(settingsNode)}
        <div class="node-modal-body node-modal-body-api">
          {#if settingsNode}
            {#key `${settingsNode.id}:${settingsApiBuilderStoreId || 0}`}
              <ApiBuilderTab
                apiBase={API_BASE}
                apiJson={workflowApiJson}
                headers={workflowApiHeaders}
                existingTables={apiBuilderExistingTables}
                refreshTables={refreshApiBuilderTables}
                initialApiStoreId={settingsApiBuilderStoreId}
              />
            {/key}
          {/if}
        </div>
      {/if}
    </div>
  {/if}
</section>

<style>
  .workflow-v2 { display: flex; flex-direction: column; gap: 10px; }
  .desk-bar {
    border: 1px solid #dbe4f0;
    border-radius: 12px;
    padding: 10px;
    background: #f8fbff;
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  .desk-storage {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    font-size: 12px;
    color: #334155;
    align-items: center;
  }
  .dirty-flag { color: #b45309; font-weight: 600; }
  .clean-flag { color: #166534; font-weight: 600; }
  .desk-actions {
    display: grid;
    grid-template-columns: minmax(180px, 1fr) minmax(220px, 1fr) auto auto auto auto minmax(170px, 1fr);
    gap: 8px;
    align-items: end;
  }
  .desk-actions label {
    display: flex;
    flex-direction: column;
    gap: 4px;
    font-size: 11px;
    color: #475569;
  }
  .desk-actions input {
    border: 1px solid #dbe4f0;
    border-radius: 8px;
    background: #fff;
    padding: 6px 8px;
    font-size: 12px;
  }
  .desk-actions .sec-label input {
    min-width: 88px;
  }
  .toggle-btn.active {
    background: #0f172a;
    color: #fff;
    border-color: #0f172a;
  }
  .saved-at {
    font-size: 11px;
    color: #475569;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .desk-error {
    border: 1px solid #fecaca;
    border-radius: 8px;
    background: #fef2f2;
    color: #991b1b;
    padding: 6px 8px;
    font-size: 12px;
  }
  .ops-bar {
    border: 1px solid #dbe4f0;
    border-radius: 12px;
    padding: 10px;
    background: #fff;
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  .ops-actions {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 8px;
  }
  .ops-ref {
    font-size: 12px;
    color: #334155;
  }
  .ops-grid {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 10px;
  }
  .ops-card {
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    background: #f8fafc;
    padding: 8px;
    display: flex;
    flex-direction: column;
    gap: 6px;
    min-height: 180px;
  }
  .ops-card h5 {
    margin: 0;
    font-size: 13px;
    color: #0f172a;
  }
  .kv {
    display: flex;
    justify-content: space-between;
    gap: 8px;
    font-size: 12px;
    color: #334155;
  }
  .ops-error {
    border: 1px solid #fecaca;
    border-radius: 7px;
    background: #fef2f2;
    color: #991b1b;
    padding: 4px 6px;
    font-size: 11px;
  }
  .process-row {
    border: 1px solid #dbeafe;
    border-radius: 8px;
    background: #eff6ff;
    padding: 6px;
    display: flex;
    flex-direction: column;
    gap: 4px;
  }
  .process-main {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    font-size: 11px;
    color: #1e293b;
    align-items: center;
  }
  .process-actions {
    display: flex;
    gap: 6px;
    align-items: center;
  }
  .process-last {
    font-size: 11px;
    color: #334155;
  }
  .ops-columns {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 8px;
  }
  .ops-subhead {
    font-size: 11px;
    color: #475569;
    margin-bottom: 4px;
  }
  .compact-row {
    display: grid;
    grid-template-columns: 1fr 1fr 1.6fr;
    gap: 6px;
    align-items: center;
    font-size: 11px;
    border: 1px solid #e2e8f0;
    border-radius: 7px;
    background: #fff;
    padding: 4px 6px;
    margin-bottom: 4px;
  }
  .compact-row code {
    font-size: 10px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .topbar { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
  .topbar button { border: 1px solid #dbe4f0; background: #fff; border-radius: 9px; padding: 6px 10px; cursor: pointer; }
  .topbar .primary { background: #0f172a; color: #fff; border-color: #0f172a; }
  .summary { display: flex; gap: 8px; font-size: 12px; color: #334155; }
  .workspace { display: grid; grid-template-columns: 280px minmax(0, 1fr) 360px; gap: 10px; min-height: 720px; }
  .sources,.tools { border: 1px solid #e2e8f0; border-radius: 12px; padding: 10px; background: #f8fbff; display: flex; flex-direction: column; gap: 8px; overflow: auto; }
  .source-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
  .mini { border: 1px solid #dbe4f0; background: #fff; border-radius: 8px; padding: 4px 8px; font-size: 11px; cursor: pointer; }
  .mini.primary { background: #0f172a; color: #fff; border-color: #0f172a; }
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
  .node-controls { display: inline-flex; align-items: center; gap: 4px; }
  .ctrl-btn {
    border: 1px solid #dbe4f0;
    background: #fff;
    color: #475569;
    border-radius: 6px;
    cursor: pointer;
    font-size: 10px;
    line-height: 1;
    min-width: 22px;
    height: 20px;
    padding: 0 4px;
  }
  .ctrl-btn:disabled { opacity: 0.45; cursor: not-allowed; }
  .delete-btn { color: #b91c1c; border-color: #fecaca; }
  .node-meta { color: #64748b; font-size: 11px; margin-top: 4px; }
  .api-meta { color: #0f172a; font-size: 10px; margin-top: 3px; line-height: 1.3; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .api-template-meta { margin-top: 4px; display: grid; grid-template-columns: 1fr; gap: 1px; font-size: 9px; color: #475569; }
  .api-template-meta span { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .api-stats { margin-top: 5px; display: grid; grid-template-columns: 1fr; gap: 2px; font-size: 10px; color: #334155; }
  .api-stats span { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .runtime { margin-top: 6px; font-size: 11px; border-radius: 7px; padding: 4px 6px; border: 1px solid #e2e8f0; }
  .runtime.running { background: #eff6ff; border-color: #93c5fd; }
  .runtime.paused { background: #fff7ed; border-color: #fdba74; }
  .runtime.stopped { background: #f1f5f9; border-color: #cbd5e1; }
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
  .node-modal.node-modal-wide { width: min(1600px, calc(100vw - 24px)); max-height: calc(100vh - 24px); }
  .node-modal-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; padding: 12px 14px; border-bottom: 1px solid #e2e8f0; position: sticky; top: 0; background: #fff; }
  .node-modal-head h4 { margin: 0; font-size: 16px; }
  .close-btn { border: 1px solid #dbe4f0; border-radius: 9px; background: #fff; cursor: pointer; padding: 4px 10px; }
  .node-modal-body { padding: 12px 14px; display: flex; flex-direction: column; gap: 10px; }
  .node-modal-body label { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: #334155; }
  .node-modal-body input,
  .node-modal-body select,
  .node-modal-body textarea { border: 1px solid #dbe4f0; border-radius: 8px; padding: 7px 8px; font-family: inherit; font-size: 13px; }
  .node-modal-body textarea { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
  .node-modal-body-api { padding: 8px; background: #f8fafc; }
  .node-modal-body-api :global(.panel) { border: 0; border-radius: 0; padding: 0; background: transparent; }
  .interval-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
  .help { font-size: 12px; color: #475569; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 8px; }
  .actions-row { display: flex; gap: 8px; flex-wrap: wrap; }
  .actions-row button { border: 1px solid #dbe4f0; background: #fff; border-radius: 8px; padding: 7px 10px; cursor: pointer; }
  .actions-row button.active { background: #0f172a; color: #fff; border-color: #0f172a; }
  .actions-row .primary-action { background: #0f172a; color: #fff; border-color: #0f172a; }
  .exec-block { border: 1px solid #e2e8f0; border-radius: 10px; padding: 8px; display: flex; flex-direction: column; gap: 8px; background: #f8fafc; }
  .exec-head { display: flex; gap: 10px; flex-wrap: wrap; font-size: 12px; color: #334155; }
  .subbox { border: 1px solid #e2e8f0; border-radius: 10px; background: #fff; padding: 8px; display: flex; flex-direction: column; gap: 8px; }
  .subttl { font-size: 12px; font-weight: 600; color: #334155; }
  .params-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; }
  .exec-preview-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
  .view-toggle { border-radius: 10px; border: 1px solid #e2e8f0; background: #fff; color: #0f172a; padding: 4px 8px; font-size: 11px; line-height: 1.2; cursor: pointer; }
  .response-tree-wrap { border: 1px solid #e6eaf2; border-radius: 12px; background: #fff; padding: 8px; min-height: 78px; overflow: visible; }
  @media (max-width: 1440px) {
    .desk-actions {
      grid-template-columns: repeat(3, minmax(0, 1fr));
    }
    .ops-grid {
      grid-template-columns: 1fr;
    }
  }
  @media (max-width: 1100px) {
    .workspace {
      grid-template-columns: 1fr;
      min-height: 0;
    }
    .desk-actions {
      grid-template-columns: 1fr;
    }
    .params-grid {
      grid-template-columns: 1fr;
    }
    .ops-columns {
      grid-template-columns: 1fr;
    }
  }
</style>



