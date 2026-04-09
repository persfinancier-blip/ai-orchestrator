<script lang="ts">
  import { onDestroy, onMount, tick } from 'svelte';
  import ApiBuilderTab from '../tabs/ApiBuilderTab.svelte';
  import ParserBuilderTab from '../tabs/ParserBuilderTab.svelte';
  import ActionPrepBuilderTab from '../tabs/ActionPrepBuilderTab.svelte';
  import ApiMutationBuilderTab from '../tabs/ApiMutationBuilderTab.svelte';
  import TableNodeBuilderTab from '../tabs/TableNodeBuilderTab.svelte';
  import WriteBuilderTab from '../tabs/WriteBuilderTab.svelte';
  import { buildAliasFromPath, normalizeTemplatePath } from '../tabs/outputContractCore.js';
  import { buildParserPublishDescriptorFields } from '../../shared/parserPublishContractCore.js';
  import {
    buildDeskExecutionStateSummary,
    buildNodeProcessVisualState,
    buildProcessStatusModel,
    pickDominantProcessModel,
    pickProcessActiveJob,
    pickProcessFocusRun
  } from '../data/workflowProcessStatusCore.js';
  import {
    tools,
    toolPorts,
    type ApiRequestTemplate,
    type SourceGroup,
    type SourceItem,
    type ToolItem,
    type ToolType
  } from '../data/workflowEditor';
  import {
    emptyNodeDescriptor,
    descriptorFields,
    normalizeDescriptorField,
    uniqueDescriptorFields,
    type NodeDescriptor,
    type NodeDescriptorField,
    type NodeDescriptorFlow,
    type NodeDescriptorOutputKind
  } from '../data/nodeDescriptorFlow';
  import { canOpenWorkflowNodeSettings } from '../data/workflowNodeSettingsAccess.js';

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
    settings?: {
      workflow_log?: {
        enabled?: boolean;
        template_id?: string;
        source_key?: string;
      };
    };
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
  type WorkflowDeskOption = {
    id: number;
    name: string;
    description: string;
    updated_at: string;
  };
  type ExistingTable = { schema_name: string; table_name: string };
  type WorkflowLogSourceBinding = {
    key: string;
    label: string;
    schema: string;
    table: string;
    qname: string;
  };
  type WorkflowLogSource = {
    key: string;
    name: string;
    description: string;
    template_id: string;
    source_kind: string;
    primary?: WorkflowLogSourceBinding | null;
    details?: WorkflowLogSourceBinding[];
  };
  type NodeRegistryEntry = {
    id: number;
    node_type_code: string;
    node_name_ru: string;
    description_ru: string;
    section_code: string;
    section_name_ru: string;
    section_order: number;
    node_order: number;
    is_enabled: boolean;
    is_system: boolean;
    hidden_in_palette: boolean;
    node_label_ru: string;
    icon_key: string;
    visual_preset_key: string;
    editor_type_code: string;
    runtime_handler_code: string;
    updated_at?: string;
    updated_by?: string;
  };
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
  type DraftProcessSummary = {
    start_node_id: string;
    process_code: string;
    name: string;
    trigger_type: string;
    execution_scope_mode: string;
    scope_type: string;
    scope_ref: string;
  };
  type ProcessCodeConflict = {
    normalized_code: string;
    display_code: string;
    node_ids: string[];
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
    duration_ms?: number;
    summary_json?: Record<string, any>;
    trigger_meta?: Record<string, any>;
    source_run_uid?: string;
    error_text?: string;
    scope_type?: string;
    scope_ref?: string;
  };
  type ProcessRunStepRow = {
    id?: number;
    run_uid: string;
    step_order: number;
    node_id: string;
    node_name: string;
    node_type: string;
    status: string;
    started_at: string;
    finished_at?: string;
    duration_ms?: number;
    input_json?: any;
    output_json?: any;
    request_payload?: any;
    response_payload?: any;
    metrics_json?: any;
    error_text?: string;
  };
  type ProcessRunDetail = {
    run: ProcessRunRow & Record<string, any>;
    steps: ProcessRunStepRow[];
  };
  type RuntimeNodeSnapshot = {
    run_uid: string;
    run_status: string;
    run_started_at: string;
    run_finished_at?: string;
    step_order: number;
    node_id: string;
    node_name: string;
    node_type: string;
    status: string;
    input_json?: any;
    output_json?: any;
    request_payload?: any;
    response_payload?: any;
    metrics_json?: any;
    error_text?: string;
    previous_step: ProcessRunStepRow | null;
    next_step: ProcessRunStepRow | null;
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
    last_error?: string;
    created_at?: string;
    updated_at?: string;
  };
  type WorkflowJobDetail = WorkflowJobRow & Record<string, any>;
  type NodeDescriptorContext = {
    sourcePort?: string;
    upstreamDescriptors?: NodeDescriptor[];
  };

  type ApiTemplateSelectionChangePayload = {
    ref: string;
    name: string;
    storeId: number;
    templateId: string;
  };

  type ApiTemplateUsageNodeEntry = {
    node_id: string;
    node_name: string;
  };

  type ApiTemplateUsageProcessEntry = {
    start_node_id: string;
    process_code: string;
    process_name: string;
  };

  type ApiTemplateUsageSummary = {
    inCurrentNode: boolean;
    otherNodes: ApiTemplateUsageNodeEntry[];
    processes: ApiTemplateUsageProcessEntry[];
  };
  type CubeTableNodeSourcePayload = {
    source: 'cube';
    point?: {
      id?: string;
      label?: string;
      sourceField?: string;
      sourceFieldName?: string;
      metrics?: Record<string, number>;
      isCluster?: boolean;
      clusterCount?: number;
      position?: { x?: number; y?: number; z?: number };
    };
    context?: {
      axisX?: string;
      axisY?: string;
      axisZ?: string;
      axisXName?: string;
      axisYName?: string;
      axisZName?: string;
      groupingPrinciple?: string;
      detail?: number;
    };
    createdAt?: string;
  };
  type TableNodeFilter = {
    id: string;
    dataType: 'text' | 'number' | 'date' | 'boolean';
    operator:
      | 'equals'
      | 'not_equals'
      | 'gt'
      | 'gte'
      | 'lt'
      | 'lte'
      | 'contains'
      | 'not_contains'
      | 'is_empty'
      | 'not_empty';
    value: string;
  };

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
  const WORKFLOW_LOG_TEMPLATE_ID = 'builtin_bronze_system_workflow_log';
  let workflowLogEnabled = false;
  let workflowLogTemplateId = WORKFLOW_LOG_TEMPLATE_ID;
  let workflowLogSourceKey = '';
  let workflowLogSources: WorkflowLogSource[] = [];
  let workflowLogPickerOpen = false;
  let workflowLogPickValue = '';
  let workflowLogSourceError = '';
  let workflowLogCompatibleSources: WorkflowLogSource[] = [];
  let workflowLogCurrentSource: WorkflowLogSource | null = null;
  let workflowLogHasValidSource = true;
  let settingsNodeId = '';
  let settingsModalOpen = false;
  let nodeModalFullscreen = false;
  let nodeModalEl: HTMLDivElement | null = null;
  let nodeModalRect: { left: number; top: number; width: number; height: number } | null = null;
  let nodeModalResizeState:
    | {
        edge: 'n' | 's' | 'e' | 'w' | 'ne' | 'nw' | 'se' | 'sw';
        startX: number;
        startY: number;
        startRect: { left: number; top: number; width: number; height: number };
      }
    | null = null;
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
  let workflowDeskOptions: WorkflowDeskOption[] = [];
  let workflowDeskPickId = '';
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
  let draftProcesses: DraftProcessSummary[] = [];
  let outdatedPublishedProcesses: DeskProcessSummary[] = [];
  let processCodeConflicts: ProcessCodeConflict[] = [];
  let processRuns: ProcessRunRow[] = [];
  let processStatusModels: any[] = [];
  let runtimeInspectorRunUid = '';
  let runtimeInspectorRunDetail: ProcessRunDetail | null = null;
  let runtimeInspectorLoading = false;
  let runtimeInspectorError = '';
  let runtimeInspectorSelectedStepOrder = 0;
  let runtimeInspectorAutoSyncKey = '';
  let workflowJobs: WorkflowJobRow[] = [];
  let processStatusRunDetails: Record<string, ProcessRunDetail> = {};
  let workflowJobDetails: Record<string, WorkflowJobDetail> = {};
  let schedulerDetailsOpen = false;
  let workflowLogDetailsOpen = false;
  let runsJobsDetailsOpen = false;
  let runtimeInspectorDetailsOpen = false;
  let processStatusClock = Date.now();
  let processStatusClockTimer: any = null;
  let apiLibrarySelection: ApiTemplateSelectionChangePayload | null = null;
  let apiTemplateUsageExpanded = false;
  let apiTemplateUsageRefreshTick = 0;
  let apiTemplateUsageSummary: ApiTemplateUsageSummary = {
    inCurrentNode: false,
    otherNodes: [],
    processes: []
  };
  let settingsApiTemplateSource: DynamicApiSource | null = null;
  let settingsCurrentTemplateBinding = { storeId: 0, templateId: '' };
  let settingsSelectedLibraryBinding = { storeId: 0, templateId: '', ref: '' };
  let settingsSelectedLibraryName = '';
  let settingsHasLibraryIdentity = false;
  let settingsHasLibrarySelection = false;
  let settingsLibraryMatchesCurrentTemplate = false;
  let settingsLibraryReadyToSwitch = false;
  let settingsCanSwitchTemplate = false;
  let apiTemplateUsageNodeKey = '';

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
  const NODE_MODAL_MIN_WIDTH = 760;
  const NODE_MODAL_MIN_HEIGHT = 420;
  const NODE_MODAL_SCREEN_GAP = 8;

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
  $: settingsApiTemplateSource = (() => {
    if (!settingsNode || (!isApiNode(settingsNode) && !isApiToolNode(settingsNode))) return null;
    return resolveTemplateSourceForNode(settingsNode, getApiRequestForNode(settingsNode));
  })();
  $: settingsCurrentTemplateBinding = (() => {
    settingsNode;
    return currentSettingsTemplateBinding();
  })();
  $: settingsSelectedLibraryBinding = (() => {
    apiLibrarySelection;
    return selectedLibraryTemplateBinding();
  })();
  $: settingsSelectedLibraryName = String(apiLibrarySelection?.name || '').trim();
  $: settingsHasLibraryIdentity =
    settingsSelectedLibraryBinding.storeId > 0 || Boolean(settingsSelectedLibraryBinding.templateId);
  $: settingsHasLibrarySelection = Boolean(settingsSelectedLibraryName || settingsHasLibraryIdentity);
  $: settingsLibraryMatchesCurrentTemplate =
    settingsHasLibraryIdentity &&
    (settingsCurrentTemplateBinding.storeId > 0 || Boolean(settingsCurrentTemplateBinding.templateId)) &&
    templateBindingsMatch(settingsCurrentTemplateBinding, settingsSelectedLibraryBinding);
  $: settingsLibraryReadyToSwitch = settingsHasLibraryIdentity && !settingsLibraryMatchesCurrentTemplate;
  $: settingsCanSwitchTemplate =
    Boolean(settingsNode && (isApiNode(settingsNode) || isApiToolNode(settingsNode))) &&
    settingsHasLibraryIdentity &&
    (!(settingsCurrentTemplateBinding.storeId > 0 || Boolean(settingsCurrentTemplateBinding.templateId)) ||
      !settingsLibraryMatchesCurrentTemplate);
  $: apiTemplateUsageSummary = buildApiTemplateUsageSummary(
    settingsApiTemplateSource,
    settingsNode,
    apiTemplateUsageRefreshTick
  );
  $: workflowLogCompatibleSources = workflowLogSources.filter(
    (source) => String(source?.template_id || '').trim() === WORKFLOW_LOG_TEMPLATE_ID
  );
  $: workflowLogCurrentSource =
    workflowLogCompatibleSources.find((source) => String(source?.key || '').trim() === String(workflowLogSourceKey || '').trim()) || null;
  $: workflowLogHasValidSource = !workflowLogEnabled || Boolean(workflowLogCurrentSource);
  $: workflowLogSourceError =
    workflowLogEnabled && !workflowLogCurrentSource
      ? 'Подключённый источник workflow log недоступен или не соответствует системному шаблону.'
      : '';
  $: if (!processRuns.length && !runtimeInspectorLoading) {
    runtimeInspectorRunUid = '';
    runtimeInspectorRunDetail = null;
    runtimeInspectorSelectedStepOrder = 0;
    runtimeInspectorAutoSyncKey = '';
  }
  $: if (
    runtimeInspectorRunUid &&
    processRuns.length &&
    !processRuns.some((run) => String(run?.run_uid || '').trim() === String(runtimeInspectorRunUid || '').trim())
  ) {
    runtimeInspectorRunUid = '';
    runtimeInspectorRunDetail = null;
    runtimeInspectorSelectedStepOrder = 0;
  }
  $: if (processRuns.length && !runtimeInspectorRunUid && !runtimeInspectorLoading) {
    const initialRun = settingsNode ? latestRunForNode(settingsNode.id) : processRuns[0];
    if (initialRun?.run_uid) {
      runtimeInspectorAutoSyncKey = `initial:${initialRun.run_uid}`;
      void loadRuntimeInspectorRun(initialRun.run_uid, { focusNodeId: settingsNode?.id || '' });
    }
  }
  $: if (!settingsModalOpen || !settingsNodeId) {
    apiLibrarySelection = null;
    apiTemplateUsageExpanded = false;
  }
  $: if (settingsNodeId !== apiTemplateUsageNodeKey) {
    apiTemplateUsageNodeKey = settingsNodeId;
    apiLibrarySelection = null;
    apiTemplateUsageExpanded = false;
    apiTemplateUsageRefreshTick += 1;
  }
  $: if (settingsModalOpen && settingsNode && processRuns.length && !runtimeInspectorLoading) {
    const preferredRun = latestRunForNode(settingsNode.id);
    const hasSnapshotForNode = Boolean(runtimeSnapshotForNode(settingsNode));
    const syncKey = `${settingsNode.id}:${preferredRun?.run_uid || ''}`;
    if (preferredRun?.run_uid && !hasSnapshotForNode && runtimeInspectorAutoSyncKey !== syncKey) {
      runtimeInspectorAutoSyncKey = syncKey;
      void loadRuntimeInspectorRun(preferredRun.run_uid, { focusNodeId: settingsNode.id });
    }
  }
  $: deskCurrentSignature = deskSignatureFromState({
    nodes,
    edges,
    viewport: { panX, panY, zoom },
    selectedNodeId,
    settings: captureDeskSettings()
  });
  $: if (deskInitialized && !deskSignatureMute) {
    deskDirty = deskCurrentSignature !== deskLastSavedSignature;
  }

  const uid = (p: string) => `${p}-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
  const toolCfg = (n: WorkflowNode) => n.config as { name: string; toolType: ToolType; settings: Record<string, string> };
  const apiCfg = (n: WorkflowNode) => n.config as SourceItem & { apiRequest?: ApiNodeRequest };
  const supportedToolTypeSet = new Set<ToolType>([
    'start_process',
    'api_request',
    'api_mutation',
    'http_request',
    'table_parser',
    'table_node',
    'action_prep',
    'db_write',
    'split_data',
    'merge_data',
    'condition_if',
    'condition_switch',
    'code_node',
    'end_process'
  ]);
  const fallbackNodeRegistryEntries: NodeRegistryEntry[] = [
    {
      id: 1,
      node_type_code: 'start_process',
      node_name_ru: 'Старт процесса',
      description_ru: 'Точка начала процесса. Здесь настраивается запуск по расписанию или вручную.',
      section_code: 'start',
      section_name_ru: 'Старт',
      section_order: 10,
      node_order: 10,
      is_enabled: true,
      is_system: true,
      hidden_in_palette: false,
      node_label_ru: 'Старт',
      icon_key: 'start_process',
      visual_preset_key: 'start',
      editor_type_code: 'start_process',
      runtime_handler_code: 'start_process'
    },
    {
      id: 2,
      node_type_code: 'api_request',
      node_name_ru: 'API-запрос',
      description_ru: 'Отправляет запрос во внешний API и передает ответ дальше по цепочке.',
      section_code: 'requests',
      section_name_ru: 'Запросы',
      section_order: 20,
      node_order: 10,
      is_enabled: true,
      is_system: true,
      hidden_in_palette: false,
      node_label_ru: 'API',
      icon_key: 'api_request',
      visual_preset_key: 'request',
      editor_type_code: 'api_builder',
      runtime_handler_code: 'api_request'
    },
    {
      id: 3,
      node_type_code: 'http_request',
      node_name_ru: 'HTTP-запрос',
      description_ru: 'Выполняет HTTP-запрос по произвольному URL на серверном уровне.',
      section_code: 'requests',
      section_name_ru: 'Запросы',
      section_order: 20,
      node_order: 20,
      is_enabled: true,
      is_system: true,
      hidden_in_palette: false,
      node_label_ru: 'HTTP',
      icon_key: 'http_request',
      visual_preset_key: 'request',
      editor_type_code: 'http_request',
      runtime_handler_code: 'http_request'
    },
    {
      id: 13,
      node_type_code: 'api_mutation',
      node_name_ru: 'API-изменение',
      description_ru: 'Принимает входные строки, собирает mutation payload и выполняет API-изменения с dry-run и батчами.',
      section_code: 'requests',
      section_name_ru: 'Запросы',
      section_order: 20,
      node_order: 30,
      is_enabled: true,
      is_system: true,
      hidden_in_palette: false,
      node_label_ru: 'Мутация',
      icon_key: 'api_mutation',
      visual_preset_key: 'request',
      editor_type_code: 'api_mutation_builder',
      runtime_handler_code: 'api_mutation'
    },
    {
      id: 4,
      node_type_code: 'condition_if',
      node_name_ru: 'Если',
      description_ru: 'Проверяет условие и направляет поток данных в одну из двух веток.',
      section_code: 'logic',
      section_name_ru: 'Логика',
      section_order: 25,
      node_order: 10,
      is_enabled: true,
      is_system: true,
      hidden_in_palette: false,
      node_label_ru: 'Если',
      icon_key: 'condition_if',
      visual_preset_key: 'logic',
      editor_type_code: 'condition_if',
      runtime_handler_code: 'condition_if'
    },
    {
      id: 5,
      node_type_code: 'condition_switch',
      node_name_ru: 'Переключатель',
      description_ru: 'Разводит поток данных по нескольким веткам в зависимости от значения выбранного поля.',
      section_code: 'logic',
      section_name_ru: 'Логика',
      section_order: 25,
      node_order: 20,
      is_enabled: true,
      is_system: true,
      hidden_in_palette: false,
      node_label_ru: 'Выбор',
      icon_key: 'condition_switch',
      visual_preset_key: 'logic',
      editor_type_code: 'condition_switch',
      runtime_handler_code: 'condition_switch'
    },
    {
      id: 6,
      node_type_code: 'split_data',
      node_name_ru: 'Разделить данные',
      description_ru: 'Разделяет или размножает поток строк по правилам обработки данных.',
      section_code: 'data_processing',
      section_name_ru: 'Работа с данными',
      section_order: 30,
      node_order: 20,
      is_enabled: true,
      is_system: true,
      hidden_in_palette: false,
      node_label_ru: 'Разделить',
      icon_key: 'split_data',
      visual_preset_key: 'data',
      editor_type_code: 'split_data',
      runtime_handler_code: 'split_data'
    },
    {
      id: 7,
      node_type_code: 'merge_data',
      node_name_ru: 'Объединить данные',
      description_ru: 'Объединяет потоки строк и подготавливает единый набор данных для следующего шага.',
      section_code: 'data_processing',
      section_name_ru: 'Работа с данными',
      section_order: 30,
      node_order: 30,
      is_enabled: true,
      is_system: true,
      hidden_in_palette: false,
      node_label_ru: 'Объединить',
      icon_key: 'merge_data',
      visual_preset_key: 'data',
      editor_type_code: 'merge_data',
      runtime_handler_code: 'merge_data'
    },
    {
      id: 8,
      node_type_code: 'table_node',
      node_name_ru: 'Табличный набор',
      description_ru: 'Собирает рабочий набор строк из таблиц, входных параметров и вычислений для следующих шагов процесса.',
      section_code: 'data_processing',
      section_name_ru: 'Работа с данными',
      section_order: 30,
      node_order: 40,
      is_enabled: true,
      is_system: true,
      hidden_in_palette: false,
      node_label_ru: 'Таблицы',
      icon_key: 'table_node',
      visual_preset_key: 'data',
      editor_type_code: 'table_node',
      runtime_handler_code: 'table_node'
    },
    {
      id: 14,
      node_type_code: 'action_prep',
      node_name_ru: 'Подготовка действий',
      description_ru: 'Отбирает строки, добавляет action-колонки и формирует поток для массовых API-изменений.',
      section_code: 'data_processing',
      section_name_ru: 'Работа с данными',
      section_order: 30,
      node_order: 50,
      is_enabled: true,
      is_system: true,
      hidden_in_palette: false,
      node_label_ru: 'Действия',
      icon_key: 'action_prep',
      visual_preset_key: 'data',
      editor_type_code: 'action_prep_builder',
      runtime_handler_code: 'action_prep'
    },
    {
      id: 9,
      node_type_code: 'table_parser',
      node_name_ru: 'Парсер данных',
      description_ru: 'Разбирает входные данные, выделяет строки и подготавливает результат для следующей ноды.',
      section_code: 'data_processing',
      section_name_ru: 'Работа с данными',
      section_order: 30,
      node_order: 10,
      is_enabled: true,
      is_system: true,
      hidden_in_palette: false,
      node_label_ru: 'Парсер',
      icon_key: 'table_parser',
      visual_preset_key: 'data',
      editor_type_code: 'parser_builder',
      runtime_handler_code: 'table_parser'
    },
    {
      id: 10,
      node_type_code: 'code_node',
      node_name_ru: 'Код',
      description_ru: 'Выполняет пользовательский код на сервере и передает результат дальше по цепочке.',
      section_code: 'tools',
      section_name_ru: 'Инструменты',
      section_order: 35,
      node_order: 10,
      is_enabled: true,
      is_system: true,
      hidden_in_palette: false,
      node_label_ru: 'Код',
      icon_key: 'code_node',
      visual_preset_key: 'tools',
      editor_type_code: 'code_node',
      runtime_handler_code: 'code_node'
    },
    {
      id: 11,
      node_type_code: 'db_write',
      node_name_ru: 'Запись в БД',
      description_ru: 'Сохраняет результат в таблицу базы данных через insert, upsert или update.',
      section_code: 'write',
      section_name_ru: 'Запись',
      section_order: 40,
      node_order: 10,
      is_enabled: true,
      is_system: true,
      hidden_in_palette: false,
      node_label_ru: 'Запись',
      icon_key: 'db_write',
      visual_preset_key: 'write',
      editor_type_code: 'db_write',
      runtime_handler_code: 'db_write'
    },
    {
      id: 12,
      node_type_code: 'end_process',
      node_name_ru: 'Конец процесса',
      description_ru: 'Фиксирует завершение цепочки.',
      section_code: 'finish',
      section_name_ru: 'Завершение',
      section_order: 50,
      node_order: 10,
      is_enabled: true,
      is_system: true,
      hidden_in_palette: false,
      node_label_ru: 'Финиш',
      icon_key: 'end_process',
      visual_preset_key: 'finish',
      editor_type_code: 'end_process',
      runtime_handler_code: 'end_process'
    }
  ];
  const runPolicyLabelMap: Record<string, string> = {
    single_instance: 'Не запускать повторно, пока текущий запуск не завершен',
    allow_parallel: 'Разрешить параллельные запуски'
  };
  const executionScopeModeLabelMap: Record<string, string> = {
    single_global: 'Один общий запуск',
    for_each_tenant: 'Отдельно для каждого клиента',
    for_each_source_account: 'Отдельно для каждого источника',
    for_each_segment: 'Отдельно для каждой группы',
    for_each_partition: 'Отдельно по частям данных'
  };
  const executionScopeModeHelpMap: Record<string, string> = {
    single_global: 'Создается один общий запуск процесса.',
    for_each_tenant: 'Создается отдельный запуск для каждого клиента.',
    for_each_source_account: 'Создается отдельный запуск для каждого источника/кабинета.',
    for_each_segment: 'Создается отдельный запуск для каждой выбранной группы.',
    for_each_partition: 'Данные делятся на части, и для каждой части идет отдельный запуск.'
  };
  const triggerLabelMap: Record<string, string> = {
    interval: 'По интервалу',
    cron: 'По расписанию Cron',
    manual: 'Только вручную'
  };
  const triggerHintMap: Record<string, string> = {
    interval: 'Процесс будет запускаться каждые N секунд/минут/часов.',
    cron: 'Процесс будет запускаться по cron-выражению.',
    manual: 'Процесс запускается только кнопкой "Запустить".'
  };
  const scopeTypeLabelMap: Record<string, string> = {
    global: 'Общий',
    tenant: 'Клиент',
    source_account: 'Источник',
    segment: 'Группа',
    partition: 'Часть данных',
    system: 'Системный'
  };

  let nodeRegistryEntries: NodeRegistryEntry[] = [...fallbackNodeRegistryEntries];
  let nodeRegistryError = '';
  let nodeRegistryLoading = false;
  let supportedTools: ToolItem[] = [];
  let groupedTools: Array<{ title: string; items: ToolItem[] }> = [];
  $: supportedTools = tools.filter((tool) => supportedToolTypeSet.has(tool.toolType));
  $: groupedTools = Array.from(
    supportedTools
      .filter((tool) => {
        const meta = toolMetaByType(tool.toolType);
        return Boolean(meta?.is_enabled) && !Boolean(meta?.hidden_in_palette);
      })
      .reduce((acc, tool) => {
        const meta = toolMetaByType(tool.toolType);
        const section = meta?.section_name_ru || 'Прочее';
        const hit = acc.get(section) || [];
        hit.push(tool);
        acc.set(section, hit);
        return acc;
      }, new Map<string, ToolItem[]>())
      .entries()
  )
    .map(([title, items]) => ({
      title,
      order: toolSectionOrderByTitle(title),
      items: [...items].sort((a, b) => toolNodeOrderByType(a.toolType) - toolNodeOrderByType(b.toolType))
    }))
    .sort((a, b) => a.order - b.order)
    .map((group) => ({ title: group.title, items: group.items }))
    .filter((group) => group.items.length > 0);
  $: draftProcesses = buildDraftProcessList(nodes);
  $: outdatedPublishedProcesses = publishedProcesses.filter(
    (pp) => !draftProcesses.some((dp) => String(dp.start_node_id || '').trim() === String(pp.start_node_id || '').trim())
  );
  $: processCodeConflicts = findProcessCodeConflicts(nodes);
  $: ensureStartProcessCodes();
  $: enabledPublishedProcesses = publishedProcesses.filter((p) => Boolean(p?.is_enabled));
  $: enabledAutomaticProcesses = enabledPublishedProcesses.filter((p) => String(p?.trigger_type || '').trim() !== 'manual');
  $: processStatusModels = draftProcesses.map((draftProcess) => {
    const publishedProcess = publishedProcessByNodeId(draftProcess.start_node_id);
    const relevantRuns = processRuns.filter(
      (run) => String(run?.start_node_id || '').trim() === String(draftProcess.start_node_id || '').trim()
    );
    const focusRun = pickProcessFocusRun({ publishedProcess, processRuns: relevantRuns });
    const activeJob = focusRun?.run_uid ? pickProcessActiveJob(workflowJobs, focusRun.run_uid) : null;
    return buildProcessStatusModel({
      draftProcess,
      publishedProcess,
      publishedDeskReady,
      publishedDeskVersionId,
      deskDirty,
      schedulerEnabled: Boolean(schedulerView.enabled),
      processRuns: relevantRuns,
      workflowJobs,
      runDetail: focusRun?.run_uid ? processStatusRunDetails[String(focusRun.run_uid).trim()] || null : null,
      activeJobDetail: activeJob?.job_id ? workflowJobDetails[String(activeJob.job_id)] || null : null,
      nowMs: processStatusClock
    });
  });
  $: deskExecutionState = (() => {
    return buildDeskExecutionStateSummary({
      deskId,
      publishedDeskReady,
      processModels: processStatusModels
    });
  })();

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

  function currentNodeRegistryEntries() {
    return Array.isArray(nodeRegistryEntries) && nodeRegistryEntries.length ? nodeRegistryEntries : fallbackNodeRegistryEntries;
  }

  function toolMetaByType(toolType: string) {
    const code = String(toolType || '').trim();
    return currentNodeRegistryEntries().find((entry) => entry.node_type_code === code) || null;
  }

  function toolLabelByType(toolType: string) {
    return toolMetaByType(toolType)?.node_name_ru || String(toolType || '').trim() || 'Нода';
  }

  function toolDescriptionByType(toolType: string) {
    return toolMetaByType(toolType)?.description_ru || '';
  }

  function toolCategoryByType(toolType: string) {
    return toolMetaByType(toolType)?.section_name_ru || 'Прочее';
  }

  function toolSectionOrderByTitle(title: string) {
    const hit = currentNodeRegistryEntries().find((entry) => entry.section_name_ru === title);
    return Math.trunc(Number(hit?.section_order || 999)) || 999;
  }

  function toolNodeOrderByType(toolType: string) {
    return Math.trunc(Number(toolMetaByType(toolType)?.node_order || 999)) || 999;
  }

  function nodeDisplayName(node: WorkflowNode | null | undefined) {
    if (!node) return 'Нода';
    if (node.type === 'tool') {
      const cfg = toolCfg(node);
      return String(cfg?.name || '').trim() || toolLabelByType(cfg?.toolType || '');
    }
    return String(node?.config?.name || '').trim() || String(node?.id || '').trim() || 'Нода';
  }

  async function loadNodeRegistry() {
    nodeRegistryLoading = true;
    nodeRegistryError = '';
    try {
      const raw = await fetch(`${API_BASE}/node-registry`, {
        method: 'GET',
        headers: { Accept: 'application/json', 'X-AO-ROLE': API_ROLE }
      });
      let payload: any = {};
      try {
        payload = await raw.json();
      } catch {
        payload = {};
      }
      if (!raw.ok) {
        throw new Error(String(payload?.details || payload?.error || `HTTP ${raw.status}`));
      }
      const rows = Array.isArray(payload?.node_registry) ? payload.node_registry : [];
      const loadedRows = rows
        .map((row: any) => ({
          id: Number(row?.id || 0) || 0,
          node_type_code: String(row?.node_type_code || '').trim(),
          node_name_ru: String(row?.node_name_ru || '').trim(),
          description_ru: String(row?.description_ru || '').trim(),
          section_code: String(row?.section_code || '').trim(),
          section_name_ru: String(row?.section_name_ru || '').trim(),
          section_order: Math.trunc(Number(row?.section_order || 0)) || 0,
          node_order: Math.trunc(Number(row?.node_order || 0)) || 0,
          is_enabled: Boolean(row?.is_enabled),
          is_system: Boolean(row?.is_system),
          hidden_in_palette: Boolean(row?.hidden_in_palette),
          node_label_ru: String(row?.node_label_ru || '').trim(),
          icon_key: String(row?.icon_key || '').trim(),
          visual_preset_key: String(row?.visual_preset_key || '').trim(),
          editor_type_code: String(row?.editor_type_code || '').trim(),
          runtime_handler_code: String(row?.runtime_handler_code || '').trim(),
          updated_at: String(row?.updated_at || '').trim(),
          updated_by: String(row?.updated_by || '').trim()
        }))
        .filter((entry: NodeRegistryEntry) => Boolean(entry.node_type_code && entry.node_name_ru));
      const mergedRows = new Map<string, NodeRegistryEntry>();
      for (const entry of fallbackNodeRegistryEntries) mergedRows.set(String(entry.node_type_code || '').trim(), entry);
      for (const entry of loadedRows) mergedRows.set(String(entry.node_type_code || '').trim(), entry);
      nodeRegistryEntries = [...mergedRows.values()];
    } catch (e: any) {
      nodeRegistryEntries = [...fallbackNodeRegistryEntries];
      nodeRegistryError = String(e?.message || e || 'Не удалось загрузить реестр нод');
    } finally {
      nodeRegistryLoading = false;
    }
  }

  function triggerLabel(value: string) {
    return triggerLabelMap[String(value || '').trim()] || String(value || '').trim() || '-';
  }

  function triggerHint(value: string) {
    return triggerHintMap[String(value || '').trim()] || '';
  }

  function runPolicyLabel(value: string) {
    return runPolicyLabelMap[String(value || '').trim()] || String(value || '').trim() || '-';
  }

  function executionScopeModeLabel(value: string) {
    return executionScopeModeLabelMap[String(value || '').trim()] || String(value || '').trim() || '-';
  }

  function executionScopeModeHelp(value: string) {
    return executionScopeModeHelpMap[String(value || '').trim()] || '';
  }

  function scopeTypeLabel(value: string) {
    return scopeTypeLabelMap[String(value || '').trim()] || String(value || '').trim() || '-';
  }

  function normalizeProcessCodePart(input: string) {
    const translitMap: Record<string, string> = {
      а: 'a', б: 'b', в: 'v', г: 'g', д: 'd', е: 'e', ё: 'e', ж: 'zh', з: 'z',
      и: 'i', й: 'y', к: 'k', л: 'l', м: 'm', н: 'n', о: 'o', п: 'p', р: 'r',
      с: 's', т: 't', у: 'u', ф: 'f', х: 'h', ц: 'ts', ч: 'ch', ш: 'sh', щ: 'sch',
      ъ: '', ы: 'y', ь: '', э: 'e', ю: 'yu', я: 'ya'
    };
    const normalized = String(input || '')
      .trim()
      .toLowerCase()
      .split('')
      .map((char) => translitMap[char] ?? char)
      .join('')
      .replace(/[^a-z0-9]+/g, '_')
      .replace(/^_+|_+$/g, '')
      .replace(/__+/g, '_');
    if (!normalized) return '';
    return /^\d/.test(normalized) ? `p_${normalized}` : normalized;
  }

  function generateProcessCodeFromNodeName(nodeName: string, nodeId: string) {
    const byName = normalizeProcessCodePart(nodeName);
    if (byName) return byName;
    const fallback = normalizeProcessCodePart(nodeId).slice(-12);
    return fallback ? `process_${fallback}` : `process_${Date.now()}`;
  }

  function resolveUniqueProcessCodeCandidate(baseCode: string, nodeId = '') {
    const base = normalizeProcessCodePart(baseCode) || generateProcessCodeFromNodeName('', nodeId);
    const usedCodes = new Set(
      nodes
        .filter((node) => node.type === 'tool' && toolCfg(node).toolType === 'start_process' && node.id !== nodeId)
        .map((node) => normalizeProcessCodePart(String(toolCfg(node).settings.processCode || '').trim()))
        .filter(Boolean)
    );
    let candidate = base;
    let idx = 2;
    while (usedCodes.has(candidate)) {
      candidate = `${base}_${idx}`;
      idx += 1;
    }
    return candidate;
  }

  function assignUniqueStartProcessCode(nodeId: string) {
    const node = nodes.find((n) => n.id === nodeId);
    if (!node || node.type !== 'tool' || toolCfg(node).toolType !== 'start_process') return;
    const cfg = toolCfg(node);
    const base = String(cfg.settings.processCode || '').trim() || String(cfg.name || '').trim() || node.id;
    const uniqueCode = resolveUniqueProcessCodeCandidate(base, nodeId);
    updateSetting(nodeId, 'processCode', uniqueCode);
  }

  function findProcessCodeConflicts(nodesList: WorkflowNode[]): ProcessCodeConflict[] {
    const byCode = new Map<string, { display_code: string; node_ids: string[] }>();
    (Array.isArray(nodesList) ? nodesList : [])
      .filter((node) => node.type === 'tool' && toolCfg(node).toolType === 'start_process')
      .forEach((node) => {
        const raw = String(toolCfg(node).settings.processCode || '').trim();
        if (!raw) return;
        const normalized = normalizeProcessCodePart(raw);
        if (!normalized) return;
        const current = byCode.get(normalized) || { display_code: raw, node_ids: [] };
        current.node_ids.push(String(node.id || '').trim());
        byCode.set(normalized, current);
      });
    const out: ProcessCodeConflict[] = [];
    byCode.forEach((value, normalized_code) => {
      if (value.node_ids.length > 1) {
        out.push({
          normalized_code,
          display_code: value.display_code || normalized_code,
          node_ids: value.node_ids
        });
      }
    });
    return out;
  }

  function processCodeConflictByNodeId(nodeId: string) {
    const target = String(nodeId || '').trim();
    if (!target) return null;
    return processCodeConflicts.find((conflict) => conflict.node_ids.includes(target)) || null;
  }

  function hasProcessCodeConflicts() {
    return processCodeConflicts.length > 0;
  }

  function ensureStartProcessCodes() {
    const usedCodes = new Set(
      nodes
        .filter((node) => node.type === 'tool' && toolCfg(node).toolType === 'start_process')
        .map((node) => String(toolCfg(node).settings.processCode || '').trim())
        .filter(Boolean)
        .map((code) => code.toLowerCase())
    );
    let changed = false;
    const nextNodes = nodes.map((node) => {
      if (node.type !== 'tool' || toolCfg(node).toolType !== 'start_process') return node;
      const cfg = toolCfg(node);
      const currentCode = String(cfg.settings.processCode || '').trim();
      if (currentCode) return node;
      let candidate = generateProcessCodeFromNodeName(String(cfg.name || ''), node.id);
      let idx = 2;
      while (usedCodes.has(candidate.toLowerCase())) {
        candidate = `${candidate}_${idx}`;
        idx += 1;
      }
      usedCodes.add(candidate.toLowerCase());
      changed = true;
      return {
        ...node,
        config: {
          ...cfg,
          settings: {
            ...cfg.settings,
            processCode: candidate
          }
        }
      };
    });
    if (changed) nodes = nextNodes;
  }

  function shouldShowTenantId(settings: Record<string, string>) {
    const mode = String(settings?.executionScopeMode || '').trim();
    const scopeType = String(settings?.scopeType || '').trim();
    return mode === 'for_each_tenant' || scopeType === 'tenant';
  }

  function processScopeBadge(process: DeskProcessSummary) {
    const mode = executionScopeModeLabel(String(process.execution_scope_mode || ''));
    const type = scopeTypeLabel(String(process.scope_type || ''));
    const ref = String(process.scope_ref || '').trim() || 'default';
    return `${mode} • ${type}: ${ref}`;
  }

  function draftProcessScopeBadge(process: DraftProcessSummary) {
    const mode = executionScopeModeLabel(String(process.execution_scope_mode || ''));
    const type = scopeTypeLabel(String(process.scope_type || ''));
    const ref = String(process.scope_ref || '').trim() || 'default';
    return `${mode} • ${type}: ${ref}`;
  }

  function buildDraftProcessList(nodesList: WorkflowNode[]) {
    return (Array.isArray(nodesList) ? nodesList : [])
      .filter((node) => node.type === 'tool' && toolCfg(node).toolType === 'start_process')
      .map((node) => {
        const cfg = toolCfg(node);
        const settings = cfg.settings || {};
        return {
          start_node_id: String(node.id || '').trim(),
          process_code: String(settings.processCode || '').trim(),
          name: String(cfg.name || '').trim() || String(node.id || '').trim(),
          trigger_type: String(settings.triggerType || 'interval').trim(),
          execution_scope_mode: String(settings.executionScopeMode || 'single_global').trim(),
          scope_type: String(settings.scopeType || 'global').trim(),
          scope_ref: String(settings.scopeRef || 'global').trim() || 'global'
        } as DraftProcessSummary;
      });
  }

  function publishedProcessByNodeId(startNodeId: string) {
    return publishedProcesses.find((p) => String(p?.start_node_id || '').trim() === String(startNodeId || '').trim()) || null;
  }

  function runtimeRowsFromValue(value: any) {
    const src = value && typeof value === 'object' ? value : null;
    if (src && String(src.contract_version || '').trim() === 'node_io_v1' && Array.isArray(src.rows)) {
      return src.rows
        .map((row: any) => {
          if (row && typeof row === 'object' && !Array.isArray(row)) return row;
          return { value: row };
        })
        .slice(0, 25);
    }
    if (Array.isArray(value)) {
      return value
        .map((row: any) => {
          if (row && typeof row === 'object' && !Array.isArray(row)) return row;
          return { value: row };
        })
        .slice(0, 25);
    }
    if (value && typeof value === 'object' && !Array.isArray(value)) return [value];
    return [];
  }

  function runtimeColumnsFromRows(rows: Array<Record<string, any>>) {
    return Array.from(
      new Set(
        (Array.isArray(rows) ? rows : [])
          .flatMap((row) => (row && typeof row === 'object' ? Object.keys(row) : []))
          .map((item) => String(item || '').trim())
          .filter(Boolean)
      )
    );
  }

  function runtimeRowCountFromValue(value: any) {
    if (value && typeof value === 'object' && !Array.isArray(value) && String(value?.contract_version || '').trim() === 'node_io_v1') {
      return Math.max(0, Number(value?.row_count || (Array.isArray(value?.rows) ? value.rows.length : 0)) || 0);
    }
    if (Array.isArray(value)) return value.length;
    if (value === undefined || value === null || value === '') return 0;
    return 1;
  }

  function runtimeJsonText(value: any, fallback = '{}') {
    try {
      return JSON.stringify(value ?? {}, null, 2);
    } catch {
      return fallback;
    }
  }

  function runtimeValuesSemanticallyMatch(left: any, right: any) {
    try {
      return JSON.stringify(left ?? null) === JSON.stringify(right ?? null);
    } catch {
      return false;
    }
  }

  function startNodeIdsForNode(nodeId: string) {
    const wanted = String(nodeId || '').trim();
    if (!wanted) return [];
    const startIds = new Set<string>();
    const visit = (currentId: string, seen = new Set<string>()) => {
      const normalizedId = String(currentId || '').trim();
      if (!normalizedId || seen.has(normalizedId)) return;
      seen.add(normalizedId);
      const node = nodes.find((item) => item.id === normalizedId);
      if (!node) return;
      if (node.type === 'tool' && toolCfg(node).toolType === 'start_process') {
        startIds.add(normalizedId);
        return;
      }
      const incoming = edges.filter((edge) => edge.to === normalizedId);
      incoming.forEach((edge) => visit(String(edge.from || '').trim(), new Set(seen)));
    };
    visit(wanted);
    return [...startIds];
  }

  function latestRunForNode(nodeId: string) {
    const startIds = new Set(startNodeIdsForNode(nodeId));
    if (!startIds.size) return processRuns[0] || null;
    return processRuns.find((run) => startIds.has(String(run?.start_node_id || '').trim())) || null;
  }

  async function loadRuntimeInspectorRun(runUid: string, options: { focusNodeId?: string; preserveStep?: boolean } = {}) {
    const safeRunUid = String(runUid || '').trim();
    if (!safeRunUid) {
      runtimeInspectorRunUid = '';
      runtimeInspectorRunDetail = null;
      runtimeInspectorSelectedStepOrder = 0;
      runtimeInspectorError = '';
      return;
    }
    runtimeInspectorLoading = true;
    runtimeInspectorError = '';
    try {
      const payload = await workflowApiJson<ProcessRunDetail>(`${API_BASE}/process-runs/${encodeURIComponent(safeRunUid)}`, {
        method: 'GET',
        headers: { Accept: 'application/json', 'X-AO-ROLE': API_ROLE }
      });
      const detail: ProcessRunDetail = {
        run: payload?.run && typeof payload.run === 'object' ? payload.run : ({} as any),
        steps: Array.isArray(payload?.steps) ? payload.steps : []
      };
      runtimeInspectorRunUid = safeRunUid;
      runtimeInspectorRunDetail = detail;
      if (!options.preserveStep) {
        const focusNodeId = String(options.focusNodeId || '').trim();
        const focusStep = focusNodeId
          ? detail.steps.find((step) => String(step?.node_id || '').trim() === focusNodeId)
          : detail.steps[0];
        runtimeInspectorSelectedStepOrder = Math.max(0, Number(focusStep?.step_order || detail.steps[0]?.step_order || 0));
      }
    } catch (e: any) {
      runtimeInspectorError = String(e?.message || e || 'Не удалось загрузить runtime run');
      runtimeInspectorRunDetail = null;
      runtimeInspectorSelectedStepOrder = 0;
    } finally {
      runtimeInspectorLoading = false;
    }
  }

  function runtimeInspectorSteps() {
    return Array.isArray(runtimeInspectorRunDetail?.steps) ? runtimeInspectorRunDetail.steps : [];
  }

  function runtimeInspectorSelectedStep() {
    const steps = runtimeInspectorSteps();
    if (!steps.length) return null;
    return (
      steps.find((step) => Math.max(0, Number(step?.step_order || 0)) === Math.max(0, Number(runtimeInspectorSelectedStepOrder || 0))) ||
      steps[0]
    );
  }

  function runtimeSnapshotForNode(node: WorkflowNode | null | undefined): RuntimeNodeSnapshot | null {
    if (!node || !runtimeInspectorRunDetail) return null;
    const steps = runtimeInspectorSteps();
    const currentIndex = steps.findIndex((step) => String(step?.node_id || '').trim() === String(node.id || '').trim());
    if (currentIndex < 0) return null;
    const current = steps[currentIndex];
    const previous = currentIndex > 0 ? steps[currentIndex - 1] : null;
    const next = currentIndex + 1 < steps.length ? steps[currentIndex + 1] : null;
    return {
      run_uid: String(runtimeInspectorRunDetail.run?.run_uid || '').trim(),
      run_status: String(runtimeInspectorRunDetail.run?.status || '').trim(),
      run_started_at: String(runtimeInspectorRunDetail.run?.started_at || '').trim(),
      run_finished_at: String(runtimeInspectorRunDetail.run?.finished_at || '').trim(),
      step_order: Math.max(0, Number(current?.step_order || 0)),
      node_id: String(current?.node_id || '').trim(),
      node_name: String(current?.node_name || '').trim(),
      node_type: String(current?.node_type || '').trim(),
      status: String(current?.status || '').trim(),
      input_json: current?.input_json,
      output_json: current?.output_json,
      request_payload: current?.request_payload,
      response_payload: current?.response_payload,
      metrics_json: current?.metrics_json,
      error_text: String(current?.error_text || '').trim(),
      previous_step: previous || null,
      next_step: next || null
    };
  }

  async function loadProcessStatusRunDetail(runUid: string) {
    const safeRunUid = String(runUid || '').trim();
    if (!safeRunUid || processStatusRunDetails[safeRunUid]) return processStatusRunDetails[safeRunUid] || null;
    const payload = await workflowApiJson<ProcessRunDetail>(`${API_BASE}/process-runs/${encodeURIComponent(safeRunUid)}`, {
      method: 'GET',
      headers: { Accept: 'application/json', 'X-AO-ROLE': API_ROLE }
    });
    const detail: ProcessRunDetail = {
      run: payload?.run && typeof payload.run === 'object' ? payload.run : ({} as any),
      steps: Array.isArray(payload?.steps) ? payload.steps : []
    };
    processStatusRunDetails = { ...processStatusRunDetails, [safeRunUid]: detail };
    return detail;
  }

  async function loadWorkflowJobDetail(jobId: number) {
    const safeJobId = Math.max(0, Math.trunc(Number(jobId || 0)));
    const cacheKey = String(safeJobId);
    if (!safeJobId || workflowJobDetails[cacheKey]) return workflowJobDetails[cacheKey] || null;
    const payload = await workflowApiJson<{ job?: WorkflowJobDetail }>(`${API_BASE}/workflow-jobs/${safeJobId}`, {
      method: 'GET',
      headers: { Accept: 'application/json', 'X-AO-ROLE': API_ROLE }
    });
    const detail = payload?.job && typeof payload.job === 'object' ? payload.job : null;
    if (detail) {
      workflowJobDetails = { ...workflowJobDetails, [cacheKey]: detail };
    }
    return detail;
  }

  async function refreshProcessStatusDetails() {
    if (!deskId) {
      processStatusRunDetails = {};
      workflowJobDetails = {};
      return;
    }
    const nextRunUids = new Set<string>();
    const nextJobIds = new Set<number>();
    draftProcesses.forEach((draftProcess) => {
      const publishedProcess = publishedProcessByNodeId(draftProcess.start_node_id);
      const relevantRuns = processRuns.filter(
        (run) => String(run?.start_node_id || '').trim() === String(draftProcess.start_node_id || '').trim()
      );
      const focusRun = pickProcessFocusRun({ publishedProcess, processRuns: relevantRuns });
      const activeJob = focusRun?.run_uid ? pickProcessActiveJob(workflowJobs, focusRun.run_uid) : null;
      const runUid = String(focusRun?.run_uid || '').trim();
      const jobId = Math.max(0, Math.trunc(Number(activeJob?.job_id || 0)));
      if (runUid) nextRunUids.add(runUid);
      if (jobId > 0) nextJobIds.add(jobId);
    });

    const staleRunKeys = Object.keys(processStatusRunDetails).filter((runUid) => !nextRunUids.has(String(runUid || '').trim()));
    if (staleRunKeys.length) {
      const nextCache = { ...processStatusRunDetails };
      staleRunKeys.forEach((runUid) => delete nextCache[runUid]);
      processStatusRunDetails = nextCache;
    }
    const staleJobKeys = Object.keys(workflowJobDetails).filter((jobId) => !nextJobIds.has(Math.trunc(Number(jobId || 0))));
    if (staleJobKeys.length) {
      const nextCache = { ...workflowJobDetails };
      staleJobKeys.forEach((jobId) => delete nextCache[jobId]);
      workflowJobDetails = nextCache;
    }

    const missingRuns = [...nextRunUids].filter((runUid) => !processStatusRunDetails[runUid]);
    for (const runUid of missingRuns) {
      try {
        await loadProcessStatusRunDetail(runUid);
      } catch (e: any) {
        banner = String(e?.message || e || 'Не удалось загрузить детали server run');
      }
    }

    const missingJobs = [...nextJobIds].filter((jobId) => !workflowJobDetails[String(jobId)]);
    for (const jobId of missingJobs) {
      try {
        await loadWorkflowJobDetail(jobId);
      } catch (e: any) {
        banner = String(e?.message || e || 'Не удалось загрузить детали workflow job');
      }
    }
  }

  function processStatusByStartNodeId(startNodeId: string) {
    return processStatusModels.find((item) => String(item?.startNodeId || '').trim() === String(startNodeId || '').trim()) || null;
  }

  function processModelsForNode(nodeId: string) {
    const startIds = startNodeIdsForNode(nodeId);
    return startIds.map((startId) => processStatusByStartNodeId(startId)).filter(Boolean);
  }

  function processIndicatorForNode(node: WorkflowNode | null | undefined) {
    if (!node) return null;
    if (node.type === 'tool' && toolCfg(node).toolType === 'start_process') {
      return buildNodeProcessVisualState({
        nodeId: node.id,
        isStartNode: true,
        processModel: processStatusByStartNodeId(node.id)
      });
    }
    const dominant = pickDominantProcessModel(processModelsForNode(node.id));
    return buildNodeProcessVisualState({
      nodeId: node.id,
      isStartNode: false,
      processModel: dominant
    });
  }

  function durationMsFromBounds(startedAt: string, finishedAt = '') {
    const startTs = Date.parse(String(startedAt || '').trim());
    if (!Number.isFinite(startTs)) return 0;
    const finishTs = finishedAt ? Date.parse(String(finishedAt || '').trim()) : processStatusClock;
    const safeFinishTs = Number.isFinite(finishTs) ? finishTs : processStatusClock;
    return Math.max(0, safeFinishTs - startTs);
  }

  function processDurationLabel(model: any) {
    if (!model?.runStartedAt) return '-';
    return formatDurationShort(durationMsFromBounds(model.runStartedAt, model.runFinishedAt || ''));
  }

  function openRuntimeInspectorForRun(runUid: string, focusNodeId = '') {
    const safeRunUid = String(runUid || '').trim();
    if (!safeRunUid) return;
    runtimeInspectorDetailsOpen = true;
    void loadRuntimeInspectorRun(safeRunUid, { focusNodeId });
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

  function updateDeskHash(nextDeskId: number) {
    const rawHash = String(window.location.hash || '#desk/data').replace(/^#/, '');
    const [routeRaw, queryRaw = ''] = rawHash.split('?');
    const route = String(routeRaw || '').trim() || 'desk/data';
    const params = new URLSearchParams(queryRaw);
    if (nextDeskId > 0) params.set('desk_id', String(nextDeskId));
    else params.delete('desk_id');
    const target = `#${route}${params.toString() ? `?${params.toString()}` : ''}`;
    if (window.location.hash !== target) window.location.hash = target;
  }

  function confirmDeskNavigation(message: string) {
    if (!deskDirty) return true;
    if (typeof window === 'undefined' || typeof window.confirm !== 'function') return false;
    return window.confirm(message);
  }

  function emptyDeskConfig(): WorkflowDeskConfig {
    return {
      nodes: [],
      edges: [],
      viewport: { panX: 0, panY: 0, zoom: 1 },
      selectedNodeId: '',
      settings: {
        workflow_log: {
          enabled: false,
          template_id: WORKFLOW_LOG_TEMPLATE_ID,
          source_key: ''
        }
      }
    };
  }

  function defaultNewDeskName() {
    const iso = new Date().toISOString().slice(0, 16).replace('T', ' ');
    return `Новый рабочий стол ${iso}`;
  }

  function safeIso(value: any) {
    const txt = String(value || '').trim();
    if (!txt) return '';
    const dt = new Date(txt);
    if (Number.isNaN(dt.getTime())) return '';
    return dt.toISOString();
  }

  function normalizeWorkflowLogSourceBinding(raw: any, fallbackLabel = ''): WorkflowLogSourceBinding | null {
    if (!raw || typeof raw !== 'object') return null;
    const key = String(raw?.key || '').trim();
    const schema = String(raw?.schema || '').trim();
    const table = String(raw?.table || '').trim();
    const qname = String(raw?.qname || (schema && table ? `${schema}.${table}` : '')).trim();
    if (!key || !schema || !table || !qname) return null;
    return {
      key,
      label: String(raw?.label || fallbackLabel || qname).trim() || qname,
      schema,
      table,
      qname
    };
  }

  function normalizeWorkflowLogSource(raw: any): WorkflowLogSource | null {
    if (!raw || typeof raw !== 'object') return null;
    const key = String(raw?.key || '').trim();
    const name = String(raw?.name || '').trim();
    if (!key || !name) return null;
    const primary = normalizeWorkflowLogSourceBinding(raw?.primary, 'Основной источник');
    const details = Array.isArray(raw?.details)
      ? raw.details
          .map((item: any) => normalizeWorkflowLogSourceBinding(item))
          .filter((item: WorkflowLogSourceBinding | null): item is WorkflowLogSourceBinding => Boolean(item))
      : [];
    return {
      key,
      name,
      description: String(raw?.description || '').trim(),
      template_id: String(raw?.template_id || WORKFLOW_LOG_TEMPLATE_ID).trim() || WORKFLOW_LOG_TEMPLATE_ID,
      source_kind: String(raw?.source_kind || 'system_bundle').trim() || 'system_bundle',
      primary,
      details
    };
  }

  function normalizeWorkflowLogState(raw: any) {
    const state = raw && typeof raw === 'object' ? raw : {};
    return {
      enabled: Boolean(state?.enabled),
      template_id: String(state?.template_id || WORKFLOW_LOG_TEMPLATE_ID).trim() || WORKFLOW_LOG_TEMPLATE_ID,
      source_key: String(state?.source_key || '').trim()
    };
  }

  function captureDeskSettings() {
    return {
      workflow_log: {
        enabled: Boolean(workflowLogEnabled),
        template_id: String(workflowLogTemplateId || WORKFLOW_LOG_TEMPLATE_ID).trim() || WORKFLOW_LOG_TEMPLATE_ID,
        source_key: String(workflowLogSourceKey || '').trim()
      }
    };
  }

  function workflowLogSourceByKey(key: string) {
    const normalizedKey = String(key || '').trim();
    if (!normalizedKey) return null;
    return workflowLogCompatibleSources.find((source) => String(source?.key || '').trim() === normalizedKey) || null;
  }

  function firstCompatibleWorkflowLogSource() {
    return workflowLogCompatibleSources[0] || null;
  }

  function toggleWorkflowLogEnabled() {
    workflowLogEnabled = !workflowLogEnabled;
    workflowLogPickerOpen = false;
    workflowLogPickValue =
      String(workflowLogSourceKey || '').trim() || String(firstCompatibleWorkflowLogSource()?.key || '').trim();
  }

  function toggleWorkflowLogPicker() {
    workflowLogPickerOpen = !workflowLogPickerOpen;
    if (workflowLogPickerOpen) {
      workflowLogPickValue =
        String(workflowLogSourceKey || '').trim() || String(firstCompatibleWorkflowLogSource()?.key || '').trim();
    }
  }

  function applyWorkflowLogSourceChoice() {
    const source = workflowLogSourceByKey(workflowLogPickValue);
    if (!source) {
      workflowLogSourceError = 'Нельзя подключить этот источник: он не соответствует системному шаблону workflow log.';
      return;
    }
    workflowLogSourceKey = source.key;
    workflowLogTemplateId = String(source.template_id || WORKFLOW_LOG_TEMPLATE_ID).trim() || WORKFLOW_LOG_TEMPLATE_ID;
    workflowLogPickValue = source.key;
    workflowLogPickerOpen = false;
    workflowLogSourceError = '';
  }

  function deskSignatureFromState(state: WorkflowDeskConfig) {
    try {
      const workflowLog = normalizeWorkflowLogState(state?.settings?.workflow_log || {});
      const normalized = {
        nodes: Array.isArray(state?.nodes) ? state.nodes : [],
        edges: Array.isArray(state?.edges) ? state.edges : [],
        viewport: {
          panX: Number(state?.viewport?.panX || 0),
          panY: Number(state?.viewport?.panY || 0),
          zoom: Number(state?.viewport?.zoom || 1)
        },
        selectedNodeId: String(state?.selectedNodeId || '').trim(),
        settings: {
          workflow_log: {
            enabled: Boolean(workflowLog.enabled),
            template_id: String(workflowLog.template_id || WORKFLOW_LOG_TEMPLATE_ID).trim() || WORKFLOW_LOG_TEMPLATE_ID,
            source_key: String(workflowLog.source_key || '').trim()
          }
        }
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
      selectedNodeId,
      settings: captureDeskSettings()
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
    const workflowLog = normalizeWorkflowLogState(cfg?.settings?.workflow_log || {});
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
    processStatusRunDetails = {};
    workflowJobDetails = {};
    workflowLogEnabled = Boolean(workflowLog.enabled);
    workflowLogTemplateId = String(workflowLog.template_id || WORKFLOW_LOG_TEMPLATE_ID).trim() || WORKFLOW_LOG_TEMPLATE_ID;
    workflowLogSourceKey = String(workflowLog.source_key || '').trim();
    workflowLogPickerOpen = false;
    workflowLogPickValue = workflowLogSourceKey || String(firstCompatibleWorkflowLogSource()?.key || '').trim();
    workflowLogSourceError = '';
    schedulerDetailsOpen = false;
    workflowLogDetailsOpen = false;
    runsJobsDetailsOpen = false;
    runtimeInspectorDetailsOpen = false;
    deskSignatureMute = false;
  }

  function applyDeskRow(row: WorkflowDeskRow) {
    deskId = Number(row?.id || 0) || 0;
    workflowDeskPickId = deskId > 0 ? String(deskId) : '';
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
      if (workflowLogEnabled && !workflowLogCurrentSource) {
        throw new Error('Выбери совместимый системный источник журнала выполнения.');
      }
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
      workflowDeskPickId = deskId > 0 ? String(deskId) : '';
      deskRevision = Math.max(1, Number(payload?.revision || deskRevision + 1 || 1) || 1);
      deskLastSavedSignature = deskSignatureFromState(captureDeskState());
      deskDirty = false;
      deskLastSavedAt = new Date().toISOString();
      workflowDeskOptions = [
        {
          id: deskId,
          name: safeName,
          description: safeDesc,
          updated_at: deskLastSavedAt
        },
        ...workflowDeskOptions.filter((item) => Number(item.id || 0) !== deskId)
      ];
      updateDeskHash(deskId);
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

  async function loadWorkflowDeskFromServer(desiredDeskIdOverride = 0) {
    deskLoading = true;
    deskSaveError = '';
    try {
      const desiredDeskId = desiredDeskIdOverride > 0 ? Math.trunc(desiredDeskIdOverride) : hashParamInt('desk_id');
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
      workflowDeskOptions = list.map((row) => ({
        id: Number(row?.id || 0) || 0,
        name: String(row?.desk_name || '').trim() || `Рабочий стол ${Number(row?.id || 0) || ''}`.trim(),
        description: String(row?.description || '').trim(),
        updated_at: safeIso(row?.updated_at)
      }));
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
        workflowDeskPickId = deskId > 0 ? String(deskId) : '';
        deskRevision = Math.max(1, Number(createdPayload?.revision || 1) || 1);
        deskLastSavedAt = new Date().toISOString();
        deskLastSavedSignature = deskSignatureFromState(captureDeskState());
        deskDirty = false;
        workflowDeskOptions = [
          {
            id: deskId,
            name: deskName || 'Рабочий стол данных',
            description: '',
            updated_at: deskLastSavedAt
          },
          ...workflowDeskOptions.filter((item) => Number(item.id || 0) !== deskId)
        ];
        updateDeskHash(deskId);
        banner = `Создан новый рабочий стол (ID ${deskId})`;
        return true;
      }
      applyDeskRow(target);
      updateDeskHash(deskId);
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

  async function selectWorkflowDeskById(nextDeskId: number) {
    if (!Number.isFinite(nextDeskId) || nextDeskId <= 0) return;
    if (deskSaving || deskLoading) return;
    if (Math.trunc(nextDeskId) === deskId) {
      workflowDeskPickId = String(nextDeskId);
      return;
    }
    if (
      !confirmDeskNavigation(
        'Есть несохраненные изменения. Переключение рабочего стола загрузит другое состояние и несохраненные изменения будут потеряны. Продолжить?'
      )
    ) {
      workflowDeskPickId = deskId > 0 ? String(deskId) : '';
      return;
    }
    workflowDeskPickId = String(nextDeskId);
    await loadWorkflowDeskFromServer(Math.trunc(nextDeskId));
    await refreshAutomationContour();
  }

  async function createNewWorkflowDesk() {
    if (deskSaving || deskLoading) return;
    if (
      !confirmDeskNavigation(
        'Есть несохраненные изменения. Создание нового рабочего стола откроет пустой стол и несохраненные изменения текущего стола будут потеряны. Продолжить?'
      )
    ) {
      return;
    }
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
          desk_name: defaultNewDeskName(),
          desk_type: 'data',
          description: '',
          schema_version: 1,
          is_active: true,
          updated_by: API_ROLE,
          config_json: emptyDeskConfig()
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
      const createdDeskId = Number(payload?.id || 0) || 0;
      if (!(createdDeskId > 0)) throw new Error('Не удалось получить ID нового рабочего стола');
      await loadWorkflowDeskFromServer(createdDeskId);
      await refreshAutomationContour();
      banner = `Создан новый рабочий стол (ID ${createdDeskId})`;
    } catch (e: any) {
      const msg = String(e?.message || e || 'Ошибка создания рабочего стола');
      deskSaveError = msg;
      banner = msg;
    } finally {
      deskSaving = false;
    }
  }

  async function deleteCurrentWorkflowDesk() {
    if (!deskId || deskSaving || deskLoading) return;
    const deletingDeskId = deskId;
    const safeDeskName = String(deskName || '').trim() || `ID ${deskId}`;
    const confirmed =
      typeof window !== 'undefined' && typeof window.confirm === 'function'
        ? window.confirm(
            `Удалить рабочий стол "${safeDeskName}"? Опубликованные процессы этого рабочего стола перестанут планироваться, потому что стол станет неактивным. Уже запущенные jobs могут завершиться отдельно.`
          )
        : false;
    if (!confirmed) return;
    deskSaving = true;
    deskSaveError = '';
    try {
      const response = await fetch(`${API_BASE}/workflow-desks/delete`, {
        method: 'POST',
        headers: {
          Accept: 'application/json',
          'Content-Type': 'application/json',
          'X-AO-ROLE': API_ROLE
        },
        body: JSON.stringify({
          id: deletingDeskId,
          updated_by: API_ROLE
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
      await loadWorkflowDeskFromServer();
      await refreshAutomationContour();
      banner = `Рабочий стол удален (ID ${deletingDeskId})`;
    } catch (e: any) {
      const msg = String(e?.message || e || 'Ошибка удаления рабочего стола');
      deskSaveError = msg;
      banner = msg;
    } finally {
      deskSaving = false;
    }
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
      processStatusRunDetails = {};
      workflowJobDetails = {};
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
      await refreshProcessStatusDetails();
    } catch (e: any) {
      banner = String(e?.message || e || 'Ошибка обновления мониторинга');
    } finally {
      monitorBusy = false;
    }
  }

  async function publishDeskVersion() {
    if (!deskId || publishBusy) return;
    if (hasProcessCodeConflicts()) {
      const codes = processCodeConflicts.map((c) => c.display_code || c.normalized_code).join(', ');
      banner = `Нельзя опубликовать: внутренний код процесса должен быть уникальным. Дубли: ${codes}`;
      return;
    }
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
      banner = `Опубликована версия рабочего стола #${publishedDeskVersionId}`;
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
      banner = enabled ? `Процесс ${startNodeId} включен` : `Процесс ${startNodeId} выключен`;
      await refreshRunsAndJobs();
      await refreshProcessStatusDetails();
    } catch (e: any) {
      banner = String(e?.message || e || 'Ошибка переключения процесса');
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
      banner = runs > 0 ? `Ручной запуск поставлен в очередь: ${runs}` : 'Команда запуска отправлена';
      await refreshAutomationContour();
    } catch (e: any) {
      banner = String(e?.message || e || 'Ошибка ручного запуска');
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
    return Boolean(n && n.type === 'tool' && (toolCfg(n).toolType === 'api_request' || toolCfg(n).toolType === 'http_request'));
  }

  function isParserToolNode(n: WorkflowNode | null | undefined) {
    return Boolean(n && n.type === 'tool' && toolCfg(n).toolType === 'table_parser');
  }

  function isActionPrepToolNode(n: WorkflowNode | null | undefined) {
    return Boolean(n && n.type === 'tool' && toolCfg(n).toolType === 'action_prep');
  }

  function isApiMutationToolNode(n: WorkflowNode | null | undefined) {
    return Boolean(n && n.type === 'tool' && toolCfg(n).toolType === 'api_mutation');
  }

  function tryList(value: any): any[] {
    if (Array.isArray(value)) return value;
    if (typeof value !== 'string') return [];
    const src = String(value || '').trim();
    if (!src) return [];
    try {
      const parsed = JSON.parse(src);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }

  function descriptorContractPath(raw: any, fallbackPath = '') {
    const rootPath = normalizeTemplatePath(String(raw?.rootPath || raw?.root_path || ''));
    const directPath = normalizeTemplatePath(
      String(raw?.path || raw?.response_path || raw?.sourcePath || raw?.source_path || fallbackPath || '')
    );
    if (rootPath && directPath) {
      return directPath.startsWith(`${rootPath}.`) ? directPath : `${rootPath}.${directPath}`;
    }
    return directPath || rootPath;
  }

  function descriptorContractField(raw: any, index: number, fallbackPath = '', origin = ''): NodeDescriptorField | null {
    const path = descriptorContractPath(raw, fallbackPath);
    return normalizeDescriptorField(
      {
        ...raw,
        path,
        alias: String(raw?.alias || raw?.outputName || raw?.output_name || raw?.name || '').trim() || (path ? buildAliasFromPath(path) : ''),
        origin: origin || raw?.origin || raw?.source || ''
      },
      index,
      path
    );
  }

  function descriptorRegistryForNode(node: WorkflowNode | null | undefined) {
    if (!node) return null;
    if (node.type === 'tool') return toolMetaByType(String(toolCfg(node).toolType || '').trim());
    if (isApiNode(node)) return toolMetaByType('api_request');
    return null;
  }

  function descriptorOutputKindFromFields(fields: NodeDescriptorField[], fallback: NodeDescriptorOutputKind = 'unknown'): NodeDescriptorOutputKind {
    const safeFields = Array.isArray(fields) ? fields : [];
    if (!safeFields.length) return fallback;
    return safeFields.some((field) => String(field.path || '').trim()) ? 'row set' : 'tabular contract';
  }

  function descriptorFieldsFromApiNode(node: WorkflowNode): NodeDescriptorField[] {
    const source = resolveTemplateSourceForNode(node, getApiRequestForNode(node));
    const row = source?.rawRow && typeof source.rawRow === 'object' ? source.rawRow : {};
    const config = tryObj(row?.config_json);
    const mapping = tryObj(config?.mapping_json || row?.mapping_json);
    const outputParametersRaw = Array.isArray(row?.output_parameters)
      ? row.output_parameters
      : Array.isArray(mapping?.output_parameters)
      ? mapping.output_parameters
      : Array.isArray(config?.output_parameters)
      ? config.output_parameters
      : [];
    const directFields = outputParametersRaw
      .map((item: any, index: number) => descriptorContractField(item, index, '', 'api_output_contract'))
      .filter((item: NodeDescriptorField | null): item is NodeDescriptorField => Boolean(item));
    if (directFields.length) {
      const dedupedByPath = new Map<string, NodeDescriptorField>();
      directFields.forEach((field) => {
        const semanticPath = normalizeTemplatePath(String(field?.path || ''));
        const key = semanticPath || String(field?.alias || field?.name || '').trim().toLowerCase();
        if (!key || dedupedByPath.has(key)) return;
        dedupedByPath.set(key, field);
      });
      return uniqueDescriptorFields([...dedupedByPath.values()]);
    }

    const pickedPathsRaw = Array.isArray(row?.picked_paths)
      ? row.picked_paths
      : Array.isArray(mapping?.picked_paths)
      ? mapping.picked_paths
      : Array.isArray(config?.picked_paths)
      ? config.picked_paths
      : [];
    const pickedFields = pickedPathsRaw
      .map((item: any, index: number) => descriptorContractField({}, index, String(item || ''), 'api_picked_path'))
      .filter((entry: NodeDescriptorField | null): entry is NodeDescriptorField => Boolean(entry));
    return uniqueDescriptorFields(pickedFields);
  }

  function descriptorFieldsFromParserNode(node: WorkflowNode, upstreamDescriptors: NodeDescriptor[] = []): NodeDescriptorField[] {
    const settings = toolCfg(node).settings || {};
    return uniqueDescriptorFields(
      buildParserPublishDescriptorFields({ settings, incomingDescriptors: upstreamDescriptors, origin: 'parser_settings' })
        .map((field: any, index: number) =>
          descriptorContractField(
            {
              path: String(field?.path || '').trim(),
              alias: String(field?.alias || field?.name || '').trim(),
              type: String(field?.type || '').trim(),
              name: String(field?.name || field?.alias || '').trim(),
              origin: 'parser_settings'
            },
            index,
            String(field?.path || '').trim(),
            'parser_settings'
          )
        )
        .filter((item: NodeDescriptorField | null): item is NodeDescriptorField => Boolean(item))
    );
  }

  function descriptorFieldsFromActionPrepNode(node: WorkflowNode, upstreamDescriptors: NodeDescriptor[] = []): NodeDescriptorField[] {
    const settings = toolCfg(node).settings || {};
    const actionColumns = tryList(settings.actionColumnsJson || settings.actionColumns || settings.action_columns);
    const passthroughFields =
      String(settings.sourceMode || settings.source_mode || 'node').trim().toLowerCase() === 'node'
        ? upstreamDescriptors
            .flatMap((descriptor) => descriptorFields(descriptor))
            .map((field, index) =>
              normalizeDescriptorField(
                {
                  name: String(field?.alias || field?.name || field?.path || '').trim(),
                  alias: String(field?.alias || field?.name || field?.path || '').trim(),
                  path: String(field?.path || field?.alias || field?.name || '').trim(),
                  type: String(field?.type || '').trim(),
                  origin: 'action_prep_input'
                },
                index
              )
            )
            .filter((item: NodeDescriptorField | null): item is NodeDescriptorField => Boolean(item))
        : [];
    const actionFields = actionColumns
      .filter((item: any) => String(item?.name || item?.outputName || item?.output_name || '').trim())
      .map((item: any, index: number) =>
        normalizeDescriptorField(
          {
            name: String(item?.name || item?.outputName || item?.output_name || '').trim(),
            alias: String(item?.name || item?.outputName || item?.output_name || '').trim(),
            path: String(item?.name || item?.outputName || item?.output_name || '').trim(),
            type: String(item?.type || '').trim(),
            origin: 'action_prep_column'
          },
          passthroughFields.length + index
        )
      )
      .filter((item: NodeDescriptorField | null): item is NodeDescriptorField => Boolean(item));
    return uniqueDescriptorFields([...passthroughFields, ...actionFields]);
  }

  function descriptorFieldsFromTableNode(node: WorkflowNode): NodeDescriptorField[] {
    const settings = toolCfg(node).settings || {};
    const selectedFields = tryList(settings.selectedFieldsJson || settings.selectedFields || settings.selected_fields);
    const selectedContractFields = selectedFields
      .map((item: any, index: number) =>
        descriptorContractField(
          {
            name: String(item?.outputName || item?.output_name || item?.fieldName || item?.field_name || '').trim(),
            alias: String(item?.outputName || item?.output_name || '').trim(),
            type: '',
            path: String(item?.sourceAlias || item?.source_alias || '').trim() && String(item?.fieldName || item?.field_name || '').trim()
              ? `${String(item?.sourceAlias || item?.source_alias || '').trim()}.${String(item?.fieldName || item?.field_name || '').trim()}`
              : String(item?.fieldName || item?.field_name || '').trim(),
            origin: 'table_settings'
          },
          index,
          '',
          'table_settings'
        )
      )
      .filter((item: NodeDescriptorField | null): item is NodeDescriptorField => Boolean(item));
    const outputMode = String(settings.outputMode || settings.output_mode || 'rows').trim().toLowerCase();
    const outputMappings = tryList(settings.outputParamsMappingJson || settings.outputParamsMapping || settings.output_params_mapping);
    const mappedOutputFields = outputMappings
      .map((item: any, index: number) =>
        normalizeDescriptorField(
          {
            name: String(item?.outputParamName || item?.output_param_name || '').trim(),
            alias: String(item?.outputParamName || item?.output_param_name || '').trim(),
            path: String(item?.sourceField || item?.source_field || item?.expression || '').trim(),
            origin: 'table_output_mapping'
          },
          index
        )
      )
      .filter((item: NodeDescriptorField | null): item is NodeDescriptorField => Boolean(item));
    return uniqueDescriptorFields(outputMode === 'named_output_params' ? mappedOutputFields : selectedContractFields);
  }

  function descriptorFieldsFromWriteNode(node: WorkflowNode, upstreamDescriptors: NodeDescriptor[] = []): NodeDescriptorField[] {
    const settings = toolCfg(node).settings || {};
    const fieldMappings = tryList(settings.fieldMappingsJson || settings.fieldMappings || settings.field_mappings);
    const targetColumns = uniqueDescriptorFields(
      fieldMappings
        .filter((item: any) => String(item?.targetField || item?.target_field || '').trim())
        .map((item: any, index: number) =>
          normalizeDescriptorField(
            {
              name: String(item?.targetField || item?.target_field || '').trim(),
              alias: String(item?.targetField || item?.target_field || '').trim(),
              path: String(item?.targetField || item?.target_field || '').trim(),
              origin: 'write_mapping'
            },
            index
          )
        )
        .filter((item: NodeDescriptorField | null): item is NodeDescriptorField => Boolean(item))
    );
    if (targetColumns.length) return targetColumns;
    const sourceMode = String(settings.sourceMode || settings.source_mode || 'node').trim().toLowerCase();
    if (sourceMode === 'node') {
      return uniqueDescriptorFields(
        upstreamDescriptors
          .flatMap((descriptor) => descriptorFields(descriptor))
          .map((field, index) =>
            normalizeDescriptorField(
              {
                name: String(field?.alias || field?.name || field?.path || '').trim(),
                alias: String(field?.alias || field?.name || field?.path || '').trim(),
                path: String(field?.path || field?.alias || field?.name || '').trim(),
                type: String(field?.type || '').trim(),
                origin: 'write_passthrough'
              },
              index
            )
          )
          .filter((item: NodeDescriptorField | null): item is NodeDescriptorField => Boolean(item))
      );
    }
    return targetColumns;
  }

  function descriptorFieldsFromApiMutationNode(node: WorkflowNode, upstreamDescriptors: NodeDescriptor[] = []): NodeDescriptorField[] {
    const settings = toolCfg(node).settings || {};
    const passthroughFields = upstreamDescriptors
      .flatMap((descriptor) => descriptorFields(descriptor))
      .map((field, index) =>
        normalizeDescriptorField(
          {
            name: String(field?.alias || field?.name || field?.path || '').trim(),
            alias: String(field?.alias || field?.name || field?.path || '').trim(),
            path: String(field?.path || field?.alias || field?.name || '').trim(),
            type: String(field?.type || '').trim(),
            origin: 'api_mutation_input'
          },
          index
        )
      )
      .filter((item: NodeDescriptorField | null): item is NodeDescriptorField => Boolean(item));
    const responseMappings = tryList(settings.responseMappingsJson || settings.responseMappings || settings.response_mappings);
    const mutationSummaryFields = ['mutation_request_index', 'mutation_batch_index', 'mutation_source_rows', 'mutation_dry_run', 'mutation_executed', 'mutation_ok', 'mutation_status', 'mutation_error']
      .map((name, index) =>
        normalizeDescriptorField(
          {
            name,
            alias: name,
            path: name,
            origin: 'api_mutation_summary'
          },
          passthroughFields.length + index
        )
      )
      .filter((item: NodeDescriptorField | null): item is NodeDescriptorField => Boolean(item));
    const mappedResponseFields = responseMappings
      .filter((item: any) => String(item?.alias || item?.outputName || item?.output_name || '').trim())
      .map((item: any, index: number) =>
        normalizeDescriptorField(
          {
            name: String(item?.alias || item?.outputName || item?.output_name || '').trim(),
            alias: String(item?.alias || item?.outputName || item?.output_name || '').trim(),
            path: String(item?.alias || item?.outputName || item?.output_name || '').trim(),
            origin: 'api_mutation_response'
          },
          passthroughFields.length + mutationSummaryFields.length + index
        )
      )
      .filter((item: NodeDescriptorField | null): item is NodeDescriptorField => Boolean(item));
    return uniqueDescriptorFields([...passthroughFields, ...mutationSummaryFields, ...mappedResponseFields]);
  }

  function nodeDescriptorFieldsForNode(node: WorkflowNode, upstreamDescriptors: NodeDescriptor[] = []): NodeDescriptorField[] {
    if (isApiNode(node) || isApiToolNode(node)) return descriptorFieldsFromApiNode(node);
    if (isParserToolNode(node)) return descriptorFieldsFromParserNode(node, upstreamDescriptors);
    if (isActionPrepToolNode(node)) return descriptorFieldsFromActionPrepNode(node, upstreamDescriptors);
    if (isTableNodeTool(node)) return descriptorFieldsFromTableNode(node);
    if (isWriteToolNode(node)) return descriptorFieldsFromWriteNode(node, upstreamDescriptors);
    if (isApiMutationToolNode(node)) return descriptorFieldsFromApiMutationNode(node, upstreamDescriptors);
    return [];
  }

  function nodeDescriptorSampleForApiNode(node: WorkflowNode) {
    if (!(isApiNode(node) || isApiToolNode(node))) return undefined;
    const exec = nodeExecutions[String(node.id || '').trim()];
    const responsePreview = exec?.responsePreview && typeof exec.responsePreview === 'object' ? exec.responsePreview : null;
    const responses = Array.isArray(responsePreview?.responses) ? responsePreview.responses : [];
    const firstResponse = responses[0]?.response;
    if (firstResponse === undefined) return undefined;
    const pagination = getPaginationForNode(node);
    const payloadPath = String(pagination?.dataPath || '').trim();
    const samplePayload = payloadPath
      ? getByPath({ response: firstResponse, value: firstResponse, input: firstResponse }, payloadPath)
      : undefined;
    const sampleRows = Array.isArray(samplePayload)
      ? samplePayload.slice(0, 5)
      : samplePayload && typeof samplePayload === 'object' && !Array.isArray(samplePayload)
      ? [samplePayload]
      : [];
    const sampleColumns = Array.from(
      new Set(
        sampleRows
          .flatMap((row) => (row && typeof row === 'object' ? Object.keys(row) : []))
          .map((item) => String(item || '').trim())
          .filter(Boolean)
      )
    );
    return {
      sampleRaw: firstResponse,
      samplePayload: samplePayload === undefined ? undefined : samplePayload,
      sampleRows: sampleRows.length ? sampleRows : undefined,
      sampleColumns: sampleColumns.length ? sampleColumns : undefined,
      sampleMeta: {
        source: 'node_execution' as const,
        status: Number.isFinite(Number(exec?.status)) ? Number(exec?.status) : undefined,
        responsesCount: responses.length || undefined,
        payloadPath: payloadPath || undefined,
        startedAt: String(exec?.startedAt || '').trim() || undefined
      }
    };
  }

  function descriptorDetectionFromApiNode(node: WorkflowNode) {
    const pagination = getPaginationForNode(node);
    const exec = nodeExecutions[String(node.id || '').trim()];
    const responsePreview = exec?.responsePreview && typeof exec.responsePreview === 'object' ? exec.responsePreview : null;
    const warnings = Array.isArray(responsePreview?.warnings) ? responsePreview.warnings.map((item: any) => String(item || '').trim()).filter(Boolean) : [];
    return {
      detectedFormat: 'json',
      readMode: 'api_response',
      parseMode: 'api_result_contract',
      payloadPath: String(pagination?.dataPath || '').trim() || undefined,
      recordPath: String(pagination?.dataPath || '').trim() || undefined,
      warnings,
      appliedSteps: ['api_template_contract']
    };
  }

  function descriptorDetectionFromParserNode(node: WorkflowNode) {
    const settings = toolCfg(node).settings || {};
    const warnings: string[] = [];
    if (!String(settings.selectFields || '').trim()) warnings.push('parser_result_fields_not_fixed');
    return {
      detectedFormat: String(settings.sourceFormat || settings.source_format || 'auto').trim() || undefined,
      readMode: 'parser',
      parseMode: 'parser_pipeline',
      payloadPath: String(settings.inputPath || settings.input_path || '').trim() || undefined,
      recordPath: String(settings.recordPath || settings.record_path || '').trim() || undefined,
      warnings,
      appliedSteps: ['parser_settings']
    };
  }

  function descriptorDetectionFromTableNode(node: WorkflowNode) {
    const settings = toolCfg(node).settings || {};
    const warnings: string[] = [];
    if (!String(settings.selectedFieldsJson || settings.selectedFields || '').trim()) warnings.push('table_selected_fields_not_fixed');
    return {
      readMode: 'table_node',
      parseMode: String(settings.outputMode || settings.output_mode || 'rows').trim() || undefined,
      warnings,
      appliedSteps: ['table_settings']
    };
  }

  function descriptorDetectionFromWriteNode(node: WorkflowNode) {
    const settings = toolCfg(node).settings || {};
    return {
      readMode: 'db_write',
      parseMode: String(settings.writeMode || settings.write_mode || 'insert').trim() || undefined,
      warnings: [],
      appliedSteps: ['write_mapping']
    };
  }

  function descriptorDetectionFromActionPrepNode(node: WorkflowNode) {
    const settings = toolCfg(node).settings || {};
    return {
      readMode: 'action_prep',
      parseMode: String(settings.sourceMode || settings.source_mode || 'node').trim().toLowerCase() === 'table' ? 'table_source' : 'upstream_rows',
      warnings: [],
      appliedSteps: ['filters', 'action_columns']
    };
  }

  function descriptorDetectionFromApiMutationNode(node: WorkflowNode) {
    const settings = toolCfg(node).settings || {};
    return {
      readMode: 'api_mutation',
      parseMode: String(settings.requestMode || settings.request_mode || 'row_per_request').trim().toLowerCase() || 'row_per_request',
      warnings: [],
      appliedSteps: ['payload_mapping', 'mutation_execution']
    };
  }

  function nodeDescriptorDetectionForNode(node: WorkflowNode) {
    if (isApiNode(node) || isApiToolNode(node)) return descriptorDetectionFromApiNode(node);
    if (isParserToolNode(node)) return descriptorDetectionFromParserNode(node);
    if (isActionPrepToolNode(node)) return descriptorDetectionFromActionPrepNode(node);
    if (isTableNodeTool(node)) return descriptorDetectionFromTableNode(node);
    if (isWriteToolNode(node)) return descriptorDetectionFromWriteNode(node);
    if (isApiMutationToolNode(node)) return descriptorDetectionFromApiMutationNode(node);
    return undefined;
  }

  function nodeDescriptorOutputKindForNode(node: WorkflowNode, fields: NodeDescriptorField[], sample: any, upstreamDescriptors: NodeDescriptor[]): NodeDescriptorOutputKind {
    if (isParserToolNode(node)) return 'row set';
    if (isActionPrepToolNode(node)) return 'row set';
    if (isTableNodeTool(node)) {
      const mode = String(toolCfg(node).settings?.outputMode || toolCfg(node).settings?.output_mode || 'rows').trim().toLowerCase();
      if (mode === 'named_output_params' || mode === 'aggregated_values') return 'structured object';
      return 'row set';
    }
    if (isWriteToolNode(node)) return fields.length ? 'row set' : 'unknown';
    if (isApiMutationToolNode(node)) return 'row set';
    if (isApiNode(node) || isApiToolNode(node)) {
      if (Array.isArray(sample?.sampleRows) && sample.sampleRows.length) return 'row set';
      return descriptorOutputKindFromFields(fields, 'structured object');
    }
    if (fields.length) return descriptorOutputKindFromFields(fields, 'tabular contract');
    const inherited = Array.isArray(upstreamDescriptors) && upstreamDescriptors.length === 1 ? upstreamDescriptors[0]?.outputKind : 'unknown';
    return (inherited || 'unknown') as NodeDescriptorOutputKind;
  }

  function buildNodeOutputDescriptor(node: WorkflowNode | null | undefined, context: NodeDescriptorContext = {}): NodeDescriptor | null {
    if (!node) return null;
    const sourcePort = String(context.sourcePort || 'out').trim() || 'out';
    const upstreamDescriptors = Array.isArray(context.upstreamDescriptors) ? context.upstreamDescriptors : [];
    const fields = nodeDescriptorFieldsForNode(node, upstreamDescriptors);
    const sample = nodeDescriptorSampleForApiNode(node);
    const registry = descriptorRegistryForNode(node);
    if (!fields.length && upstreamDescriptors.length === 1 && !(isApiNode(node) || isApiToolNode(node) || isParserToolNode(node) || isActionPrepToolNode(node) || isTableNodeTool(node) || isWriteToolNode(node) || isApiMutationToolNode(node))) {
      const inherited = upstreamDescriptors[0] || emptyNodeDescriptor(String(node.id || '').trim(), sourcePort);
      return {
        ...inherited,
        sourceNodeId: String(node.id || '').trim(),
        sourceNodeName: String(node.config?.name || node.id || '').trim() || String(node.id || '').trim(),
        sourceNodeType:
          node.type === 'tool'
            ? String(toolCfg(node).toolType || node.type).trim()
            : String(node.config?.group || node.type || '').trim() || node.type,
        sourcePort,
        editorType: String(registry?.editor_type_code || inherited.editorType || '').trim() || undefined,
        runtimeHandler: String(registry?.runtime_handler_code || inherited.runtimeHandler || '').trim() || undefined
      };
    }
    return {
      descriptorVersion: 'node_descriptor_v1',
      sourceNodeId: String(node.id || '').trim(),
      sourceNodeName: String(node.config?.name || node.id || '').trim() || String(node.id || '').trim(),
      sourceNodeType:
        node.type === 'tool'
          ? String(toolCfg(node).toolType || node.type).trim()
          : String(node.config?.group || node.type || '').trim() || node.type,
      sourcePort,
      editorType: String(registry?.editor_type_code || '').trim() || undefined,
      runtimeHandler: String(registry?.runtime_handler_code || '').trim() || undefined,
      outputKind: nodeDescriptorOutputKindForNode(node, fields, sample, upstreamDescriptors),
      fields: uniqueDescriptorFields(fields),
      detection: nodeDescriptorDetectionForNode(node),
      sample
    };
  }

  function incomingDescriptorForNode(n: WorkflowNode | null | undefined): NodeDescriptorFlow | null {
    if (!n) return null;
    const upstreamDescriptors = edges
      .filter((edge) => edge.to === n.id)
      .reduce<NodeDescriptor[]>((acc, edge) => {
        const src = nodes.find((candidate) => candidate.id === edge.from);
        if (!src) return acc;
        const srcUpstream = edges
          .filter((candidate) => candidate.to === src.id)
          .map((candidate) => {
            const upstreamNode = nodes.find((item) => item.id === candidate.from);
            return buildNodeOutputDescriptor(upstreamNode, { sourcePort: String(candidate.fromPort || 'out').trim() || 'out' });
          })
          .filter((item: NodeDescriptor | null): item is NodeDescriptor => Boolean(item));
        const descriptor = buildNodeOutputDescriptor(src, {
          sourcePort: String(edge.fromPort || 'out').trim() || 'out',
          upstreamDescriptors: srcUpstream
        });
        if (descriptor) acc.push(descriptor);
        return acc;
      }, []);
    return { nodeId: n.id, upstreamDescriptors };
  }

  function isTableNodeTool(n: WorkflowNode | null | undefined) {
    return Boolean(n && n.type === 'tool' && toolCfg(n).toolType === 'table_node');
  }

  function isWriteToolNode(n: WorkflowNode | null | undefined) {
    return Boolean(n && n.type === 'tool' && toolCfg(n).toolType === 'db_write');
  }

  function isWideSettingsNode(n: WorkflowNode | null | undefined) {
    return Boolean(
          n &&
        (isApiNode(n) ||
          (n.type === 'tool' && toolCfg(n).toolType === 'api_request') ||
          isApiMutationToolNode(n) ||
          isTableNodeTool(n) ||
          isParserToolNode(n) ||
          isActionPrepToolNode(n) ||
          isWriteToolNode(n) ||
          toolCfg(n).toolType === 'condition_switch' ||
          toolCfg(n).toolType === 'code_node')
    );
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

  function pluralRu(count: number, one: string, two: string, five: string) {
    const n = Math.abs(Number(count || 0)) % 100;
    const n1 = n % 10;
    if (n > 10 && n < 20) return five;
    if (n1 > 1 && n1 < 5) return two;
    if (n1 === 1) return one;
    return five;
  }

  function buildApiTemplateUsageSummary(
    source: DynamicApiSource | null,
    currentNode: WorkflowNode | null,
    refreshTick: number
  ): ApiTemplateUsageSummary {
    void refreshTick;
    if (!source || !currentNode) {
      return { inCurrentNode: false, otherNodes: [], processes: [] };
    }

    const templateNodes = nodes.filter(
      (node) =>
        (isApiNode(node) || isApiToolNode(node)) &&
        nodeTemplateRefs(node).some((ref) => sourceMatchesTemplateRef(source, ref))
    );
    const currentNodeId = String(currentNode.id || '').trim();
    const nodeIds = new Set(templateNodes.map((node) => String(node.id || '').trim()));
    const inCurrentNode = nodeIds.has(currentNodeId);

    const otherNodes = templateNodes
      .filter((node) => String(node.id || '').trim() !== currentNodeId)
      .map((node) => ({
        node_id: String(node.id || '').trim(),
        node_name: nodeDisplayName(node)
      }))
      .sort((a, b) => a.node_name.localeCompare(b.node_name, 'ru', { sensitivity: 'base' }));

    const processes = nodes
      .filter((node) => node.type === 'tool' && toolCfg(node).toolType === 'start_process')
      .filter((startNode) => {
        const reachableIds = reachableFrom(startNode.id);
        for (const nodeId of reachableIds) {
          if (nodeIds.has(nodeId)) return true;
        }
        return false;
      })
      .map((startNode) => {
        const cfg = toolCfg(startNode);
        const settings = cfg.settings || {};
        const processCode = String(settings.processCode || '').trim() || String(startNode.id || '').trim();
        return {
          start_node_id: String(startNode.id || '').trim(),
          process_code: processCode,
          process_name: String(cfg.name || '').trim() || processCode
        } as ApiTemplateUsageProcessEntry;
      })
      .sort((a, b) => a.process_name.localeCompare(b.process_name, 'ru', { sensitivity: 'base' }));

    return { inCurrentNode, otherNodes, processes };
  }

  function onApiBuilderTemplateSelectionChange(event: CustomEvent<ApiTemplateSelectionChangePayload>) {
    const detail = event?.detail && typeof event.detail === 'object' ? event.detail : ({} as ApiTemplateSelectionChangePayload);
    const selectedStoreId = Number(String(detail?.storeId ?? '').trim());
    const storeId = Number.isFinite(selectedStoreId) && selectedStoreId > 0 ? Math.trunc(selectedStoreId) : 0;
    let templateId = String(detail?.templateId || '').trim();
    if (storeId > 0) {
      const found = dynamicApiSources.find((src) => Number(src?.storeId || 0) === storeId);
      if (found?.id) templateId = found.id;
      else if (!templateId) templateId = `api_tpl_${storeId}`;
    }
    apiLibrarySelection = {
      ref: String(detail?.ref || '').trim(),
      name: String(detail?.name || '').trim(),
      storeId,
      templateId
    };
  }

  async function onApiBuilderTemplateSaved(event: CustomEvent<{ ref: string; storeId: number; templateId: string }>) {
    onApiBuilderTemplateSelectionChange(event as unknown as CustomEvent<ApiTemplateSelectionChangePayload>);
    await loadDynamicSourceCatalog();
  }

  function parseTemplateStoreId(value: any) {
    const txt = String(value || '').trim();
    if (!txt) return 0;
    const direct = Number(txt);
    if (Number.isFinite(direct) && direct > 0) return Math.trunc(direct);
    const m1 = txt.match(/^api_template:(\d+)$/i);
    if (m1?.[1]) return Math.trunc(Number(m1[1]));
    const m2 = txt.match(/^api_tpl_(\d+)$/i);
    if (m2?.[1]) return Math.trunc(Number(m2[1]));
    return 0;
  }

  function normalizeTemplateId(value: any) {
    return String(value || '').trim().toLowerCase();
  }

  function currentSettingsTemplateBinding() {
    if (!settingsNode || (!isApiNode(settingsNode) && !isApiToolNode(settingsNode))) {
      return { storeId: 0, templateId: '' };
    }
    let rawStoreId = '';
    let rawTemplateId = '';
    if (isApiNode(settingsNode)) {
      rawStoreId = String(settingsNode.config?.templateStoreId || '').trim();
      rawTemplateId = String(settingsNode.config?.templateId || '').trim();
    } else if (isApiToolNode(settingsNode)) {
      const settings = toolCfg(settingsNode).settings || {};
      rawStoreId = String(settings.templateStoreId || '').trim();
      rawTemplateId = String(settings.templateId || '').trim();
    }
    let storeId = parseTemplateStoreId(rawStoreId);
    if (storeId <= 0) storeId = parseTemplateStoreId(rawTemplateId);
    if (storeId <= 0) {
      const refs = nodeTemplateRefs(settingsNode);
      for (const ref of refs) {
        const parsed = parseTemplateStoreId(ref);
        if (parsed > 0) {
          storeId = parsed;
          break;
        }
      }
    }
    return {
      storeId,
      templateId: normalizeTemplateId(rawTemplateId)
    };
  }

  function selectedLibraryTemplateBinding() {
    const rawStore = Number(apiLibrarySelection?.storeId || 0);
    const storeId = Number.isFinite(rawStore) && rawStore > 0 ? Math.trunc(rawStore) : 0;
    const templateId = normalizeTemplateId(apiLibrarySelection?.templateId || '');
    const ref = String(apiLibrarySelection?.ref || '').trim();
    return { storeId, templateId, ref };
  }

  function templateBindingsMatch(
    current: { storeId: number; templateId: string },
    selected: { storeId: number; templateId: string }
  ) {
    if (selected.storeId > 0 && current.storeId > 0) {
      return selected.storeId === current.storeId;
    }
    if (selected.templateId && current.templateId) {
      return selected.templateId === current.templateId;
    }
    if (selected.storeId > 0 && current.templateId) {
      const currentTemplateStoreId = parseTemplateStoreId(current.templateId);
      return currentTemplateStoreId > 0 && currentTemplateStoreId === selected.storeId;
    }
    if (current.storeId > 0 && selected.templateId) {
      const selectedTemplateStoreId = parseTemplateStoreId(selected.templateId);
      return selectedTemplateStoreId > 0 && selectedTemplateStoreId === current.storeId;
    }
    return false;
  }

  function hasSelectedLibraryTemplateIdentity() {
    const selected = selectedLibraryTemplateBinding();
    return selected.storeId > 0 || Boolean(selected.templateId);
  }

  async function switchSettingsNodeTemplate() {
    if (!settingsNode || (!isApiNode(settingsNode) && !isApiToolNode(settingsNode))) return;
    if (!settingsCanSwitchTemplate) return;
    const selected = settingsSelectedLibraryBinding;
    let source =
      dynamicApiSources.find((src) => selected.storeId > 0 && Number(src?.storeId || 0) === selected.storeId) ||
      dynamicApiSources.find((src) => Boolean(selected.templateId) && normalizeTemplateId(src?.id || '') === selected.templateId) ||
      dynamicApiSources.find((src) => Boolean(selected.ref) && sourceMatchesTemplateRef(src, selected.ref)) ||
      null;
    if (!source) {
      await loadDynamicSourceCatalog();
      source =
        dynamicApiSources.find((src) => selected.storeId > 0 && Number(src?.storeId || 0) === selected.storeId) ||
        dynamicApiSources.find((src) => Boolean(selected.templateId) && normalizeTemplateId(src?.id || '') === selected.templateId) ||
        dynamicApiSources.find((src) => Boolean(selected.ref) && sourceMatchesTemplateRef(src, selected.ref)) ||
        null;
    }
    if (!source) {
      banner = 'Не удалось найти выбранный шаблон в библиотеке. Обнови список API и попробуй снова.';
      return;
    }
    applyTemplateToNode(settingsNode.id, source.id);
    await tick();
    const applied = currentSettingsTemplateBinding();
    const appliedByStore = Number(source.storeId || 0) > 0 && applied.storeId > 0 && Math.trunc(Number(source.storeId || 0)) === applied.storeId;
    const appliedByTemplate = Boolean(applied.templateId) && applied.templateId === normalizeTemplateId(source.id);
    if (!appliedByStore && !appliedByTemplate) {
      banner = 'Не удалось перепривязать ноду к выбранному шаблону. Обнови рабочий стол и попробуй снова.';
      return;
    }
    const saved = await saveDesk(true);
    if (!saved) {
      banner = 'Шаблон в ноде изменён, но сохранить рабочий стол на сервер не удалось. Проверь сохранение.';
    }
    apiLibrarySelection = {
      ...(apiLibrarySelection || { ref: '', name: '', storeId: 0, templateId: '' }),
      storeId: Number(source.storeId || selected.storeId || 0),
      templateId: source.id,
      name: String(source.name || apiLibrarySelection?.name || '').trim()
    };
    apiTemplateUsageExpanded = false;
    apiTemplateUsageRefreshTick += 1;
    const switchedName = String(source.name || apiLibrarySelection?.name || settingsApiTemplateSource?.name || '').trim();
    if (saved) {
      banner = switchedName ? `Шаблон в ноде изменён и сохранён: ${switchedName}` : 'Шаблон в ноде изменён и сохранён.';
    }
  }

  async function refreshApiTemplateUsageSummary() {
    await loadDynamicSourceCatalog();
    apiTemplateUsageRefreshTick += 1;
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
    if (toolType === 'http_request') {
      return {
        templateId: '',
        templateStoreId: '',
        apiMethod: 'GET',
        apiUrl: '',
        apiAuthMode: 'manual',
        apiHeaders: '{}',
        apiQuery: '{}',
        apiBody: '{}'
      };
    }
    if (toolType === 'api_mutation')
      return {
        endpointUrl: '',
        httpMethod: 'POST',
        authMode: 'manual',
        authJson: '{}',
        headersJson: '{"Content-Type":"application/json"}',
        queryJson: '{}',
        bodyJson: '{}',
        bodyItemsPath: 'items',
        bindingRulesJson: '[]',
        responseMappingsJson: '[]',
        requestMode: 'row_per_request',
        batchSize: '50',
        dryRun: 'true',
        maxRowsPerRun: '500',
        retryCount: '1',
        timeoutMs: '15000',
        stopPolicy: 'stop_on_error',
        previewLimit: '20',
        channel: ''
      };
    if (toolType === 'table_node')
      return {
        baseSchema: '',
        baseTable: '',
        baseAlias: 'base',
        joinedSourcesJson: '[]',
        inputSourcesJson: '[]',
        selectedFieldsJson: '[]',
        computedFieldsJson: '[]',
        filterRulesJson: '[]',
        filterLogic: 'and',
        dedupeMode: '',
        dedupeFields: '',
        dedupeKeep: 'first',
        groupByFields: '',
        aggregateRulesJson: '[]',
        outputMode: 'rows',
        outputParamsMappingJson: '[]',
        previewInputParamsJson: '{}',
        batchSize: '200',
        previewLimit: '20',
        channel: ''
      };
    if (toolType === 'split_data')
      return {
        splitMode: 'duplicate',
        splitMultiplier: '2',
        splitKeyField: '',
        splitPrefix: ''
      };
    if (toolType === 'merge_data')
      return {
        mergeMode: 'dedupe',
        dedupeBy: '',
        mergeKeep: 'first',
        mergeEmptySource: '[]'
      };
    if (toolType === 'condition_if')
      return {
        conditionField: '',
        conditionOperator: 'equals',
        conditionValue: '',
        ifTruePort: 'true',
        ifFalsePort: 'false',
        conditionSimulateRoute: 'true'
      };
    if (toolType === 'condition_switch')
      return {
        switchField: '',
        switchDefaultPort: 'default',
        switchDefaultValue: '',
        switchCase1Value: '',
        switchCase2Value: '',
        switchCase3Value: '',
        switchCase4Value: '',
        switchSimulatePort: 'default'
      };
    if (toolType === 'code_node')
      return {
        scriptCode: 'return input;',
        codeTimeoutMs: '5000',
        codeContext: '{}'
      };
    if (toolType === 'table_parser')
      return {
        templateId: '',
        templateStoreId: '',
        sourceMode: 'node',
        sourceNodeTemplateRef: '',
        sourceNodeTemplateType: '',
        sourceNodeTemplateStoreId: '',
        sourceNodeTemplateName: '',
        sourceFormat: 'auto',
        sourceSchema: '',
        sourceTable: '',
        sourceColumn: '',
        inputPath: '',
        recordPath: '',
        fileUrl: '',
        fileUrlPath: '',
        archiveEntry: '',
        archiveFormat: 'auto',
        csvDelimiter: ',',
        textMode: 'lines',
        batchSize: '200',
        previewLimit: '20',
        maxJsonBytes: '20971520',
        selectFields: '',
        renameMap: '{}',
        defaultValues: '{}',
        typeMap: '{}',
        filterField: '',
        filterOperator: '',
        filterValue: '',
        sampleInput: '',
        lookupEnabled: 'false',
        lookupSchema: '',
        lookupTable: '',
        lookupSourceField: '',
        lookupTargetField: '',
        lookupFields: '',
        lookupPrefix: '',
        channel: '',
        limit: '1000',
        parserMultiplier: '1'
      };
    if (toolType === 'action_prep')
      return {
        sourceMode: 'node',
        sourceSchema: '',
        sourceTable: '',
        filterRulesJson: '[]',
        filterLogic: 'and',
        actionColumnsJson: '[]',
        batchSize: '500',
        previewLimit: '20',
        channel: ''
      };
    if (toolType === 'db_write')
      return {
        templateId: '',
        templateStoreId: '',
        sourceMode: 'node',
        sourceNodeTemplateRef: '',
        sourceNodeTemplateType: '',
        sourceNodeTemplateStoreId: '',
        sourceNodeTemplateName: '',
        sourceSchema: '',
        sourceTable: '',
        sourceColumn: '',
        targetSchema: '',
        targetTable: '',
        writeMode: 'insert',
        fieldMappingsJson: '[]',
        keyFields: '',
        batchSize: '500',
        previewLimit: '20',
        unmappedMode: 'matched_only',
        conflictMode: 'input_wins',
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
      const [apiConfigsRaw, tablesRaw, settingsRaw, workflowLogRaw] = await Promise.all([
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
        }),
        fetch(`${API_BASE}/workflow-log-sources`, {
          method: 'GET',
          headers: { Accept: 'application/json', 'X-AO-ROLE': API_ROLE }
        })
      ]);

      let apiJson: any = {};
      let tableJson: any = {};
      let settingsJson: any = {};
      let workflowLogJson: any = {};
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
      try {
        workflowLogJson = await workflowLogRaw.json();
      } catch {
        workflowLogJson = {};
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

      if (workflowLogRaw.ok) {
        workflowLogSources = (Array.isArray(workflowLogJson?.sources) ? workflowLogJson.sources : [])
          .map((source: any) => normalizeWorkflowLogSource(source))
          .filter((source: WorkflowLogSource | null): source is WorkflowLogSource => Boolean(source));
      } else {
        workflowLogSources = [];
        const details = String(
          workflowLogJson?.details || workflowLogJson?.error || `${workflowLogRaw.status} ${workflowLogRaw.statusText}`
        );
        sourceCatalogError = details ? `Не удалось загрузить системные источники workflow log: ${details}` : sourceCatalogError;
      }
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

  function createTableNodeFromCube(payload?: CubeTableNodeSourcePayload | null) {
    if (!canvasEl) return;
    const cfg = payload && typeof payload === 'object' ? payload : {};
    const point = cfg.point && typeof cfg.point === 'object' ? cfg.point : {};
    const rect = canvasEl.getBoundingClientRect();
    const existing = nodes.filter((n) => n.type === 'tool' && toolCfg(n).toolType === 'table_node').length;
    const p = dropPoint(rect.left + rect.width * 0.54 + existing * 18, rect.top + rect.height * 0.32 + existing * 14);
    const settings = defaultSettings('table_node');
    settings.sourcePayload = JSON.stringify(cfg || {});
    settings.sourceLabel = String(point.label || '').trim();
    settings.sourceField = String(point.sourceField || '').trim();
    settings.sourceFieldName = String(point.sourceFieldName || '').trim();
    settings.sourceClusterCount = String(Math.max(1, Number(point.clusterCount || 1)));
    const nodeName = settings.sourceLabel ? `Таблица: ${settings.sourceLabel}` : 'Таблица';
    const id = uid('node');
    nodes = [
      ...nodes,
      {
        id,
        type: 'tool',
        x: p.x,
        y: p.y,
        config: { name: nodeName, toolType: 'table_node' as ToolType, settings }
      }
    ];
    selectedNodeId = id;
    settingsNodeId = id;
    settingsModalOpen = true;
    nodeModalFullscreen = false;
    nodeModalRect = null;
    banner = 'Добавлена нода "Таблица" из выбранной точки';
  }

  function onCreateTableNodeEvent(event: Event) {
    const custom = event as CustomEvent<CubeTableNodeSourcePayload>;
    createTableNodeFromCube(custom?.detail || null);
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
      if (item.toolType === 'http_request') {
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
      const nodeTitle =
        toolLabelByType(item.toolType) || String(item.name || '').trim() || String(item.toolType || '').trim() || 'Нода';
      nodes = [...nodes, { id: uid('node'), type: 'tool', x: p.x, y: p.y, config: { name: nodeTitle, toolType: item.toolType, settings } }];
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
    const canOpenSettings = canOpenWorkflowNodeSettings(node);
    if (!canOpenSettings) {
      banner = 'Для этого узла пока нет отдельной настройки по двойному клику';
      return;
    }
    selectedNodeId = nodeId;
    settingsNodeId = nodeId;
    settingsModalOpen = true;
    nodeModalFullscreen = false;
    nodeModalRect = null;
    settingsRequestViewMode = 'tree';
    settingsResponseViewMode = 'tree';
  }

  function closeSettingsModal() {
    settingsModalOpen = false;
    settingsNodeId = '';
    nodeModalFullscreen = false;
    nodeModalRect = null;
    stopNodeModalResize();
  }

  function toggleNodeModalFullscreen() {
    stopNodeModalResize();
    if (!nodeModalFullscreen) ensureNodeModalRect();
    nodeModalFullscreen = !nodeModalFullscreen;
  }

  function clamp(value: number, min: number, max: number) {
    if (!Number.isFinite(value)) return min;
    return Math.max(min, Math.min(max, value));
  }

  function ensureNodeModalRect() {
    if (nodeModalRect || !nodeModalEl) return;
    const rect = nodeModalEl.getBoundingClientRect();
    nodeModalRect = {
      left: rect.left,
      top: rect.top,
      width: rect.width,
      height: rect.height
    };
  }

  function resizeCursorByEdge(edge: 'n' | 's' | 'e' | 'w' | 'ne' | 'nw' | 'se' | 'sw') {
    if (edge === 'n' || edge === 's') return 'ns-resize';
    if (edge === 'e' || edge === 'w') return 'ew-resize';
    if (edge === 'nw' || edge === 'se') return 'nwse-resize';
    return 'nesw-resize';
  }

  function onNodeModalResizeMove(e: MouseEvent) {
    if (!nodeModalResizeState) return;
    e.preventDefault();
    const { edge, startX, startY, startRect } = nodeModalResizeState;
    const dx = e.clientX - startX;
    const dy = e.clientY - startY;
    const viewW = window.innerWidth;
    const viewH = window.innerHeight;
    const minW = NODE_MODAL_MIN_WIDTH;
    const minH = NODE_MODAL_MIN_HEIGHT;
    const gap = NODE_MODAL_SCREEN_GAP;

    let left = startRect.left;
    let top = startRect.top;
    let width = startRect.width;
    let height = startRect.height;

    const rightFixed = startRect.left + startRect.width;
    const bottomFixed = startRect.top + startRect.height;

    if (edge.includes('e')) {
      const maxWFromLeft = Math.max(minW, viewW - gap - startRect.left);
      width = clamp(startRect.width + dx, minW, maxWFromLeft);
    }

    if (edge.includes('w')) {
      const maxWFromRight = Math.max(minW, rightFixed - gap);
      width = clamp(startRect.width - dx, minW, maxWFromRight);
      left = rightFixed - width;
      if (left < gap) {
        left = gap;
        width = rightFixed - left;
      }
    }

    if (edge.includes('s')) {
      const maxHFromTop = Math.max(minH, viewH - gap - startRect.top);
      height = clamp(startRect.height + dy, minH, maxHFromTop);
    }

    if (edge.includes('n')) {
      const maxHFromBottom = Math.max(minH, bottomFixed - gap);
      height = clamp(startRect.height - dy, minH, maxHFromBottom);
      top = bottomFixed - height;
      if (top < gap) {
        top = gap;
        height = bottomFixed - top;
      }
    }

    const maxLeft = Math.max(gap, viewW - gap - width);
    const maxTop = Math.max(gap, viewH - gap - height);
    left = clamp(left, gap, maxLeft);
    top = clamp(top, gap, maxTop);

    nodeModalRect = { left, top, width, height };
  }

  function stopNodeModalResize() {
    if (!nodeModalResizeState) return;
    window.removeEventListener('mousemove', onNodeModalResizeMove);
    window.removeEventListener('mouseup', stopNodeModalResize);
    nodeModalResizeState = null;
    document.body.style.removeProperty('cursor');
    document.body.style.removeProperty('user-select');
  }

  function startNodeModalResize(e: MouseEvent, edge: 'n' | 's' | 'e' | 'w' | 'ne' | 'nw' | 'se' | 'sw') {
    if (nodeModalFullscreen) return;
    if (e.button !== 0) return;
    ensureNodeModalRect();
    if (!nodeModalRect) return;
    e.preventDefault();
    e.stopPropagation();
    nodeModalResizeState = {
      edge,
      startX: e.clientX,
      startY: e.clientY,
      startRect: { ...nodeModalRect }
    };
    window.addEventListener('mousemove', onNodeModalResizeMove);
    window.addEventListener('mouseup', stopNodeModalResize);
    document.body.style.cursor = resizeCursorByEdge(edge);
    document.body.style.userSelect = 'none';
  }

  function detectNodeModalResizeEdge(e: MouseEvent): 'n' | 's' | 'e' | 'w' | 'ne' | 'nw' | 'se' | 'sw' | null {
    if (!nodeModalEl || nodeModalFullscreen || !settingsNode) return null;
    if (!isApiNode(settingsNode) && !isApiToolNode(settingsNode)) return null;
    const rect = nodeModalEl.getBoundingClientRect();
    const zone = 12;
    const nearLeft = e.clientX >= rect.left && e.clientX <= rect.left + zone;
    const nearRight = e.clientX <= rect.right && e.clientX >= rect.right - zone;
    const nearTop = e.clientY >= rect.top && e.clientY <= rect.top + zone;
    const nearBottom = e.clientY <= rect.bottom && e.clientY >= rect.bottom - zone;

    if (nearTop && nearLeft) return 'nw';
    if (nearTop && nearRight) return 'ne';
    if (nearBottom && nearLeft) return 'sw';
    if (nearBottom && nearRight) return 'se';
    if (nearTop) return 'n';
    if (nearBottom) return 's';
    if (nearRight) return 'e';
    if (nearLeft) return 'w';
    return null;
  }

  function onNodeModalMouseMoveForResize(e: MouseEvent) {
    if (nodeModalResizeState || !nodeModalEl) return;
    const edge = detectNodeModalResizeEdge(e);
    if (edge) {
      nodeModalEl.style.cursor = resizeCursorByEdge(edge);
    } else {
      nodeModalEl.style.removeProperty('cursor');
    }
  }

  function onNodeModalMouseDownForResize(e: MouseEvent) {
    const edge = detectNodeModalResizeEdge(e);
    if (!edge) return;
    startNodeModalResize(e, edge);
  }

  function onNodeModalMouseLeaveForResize() {
    if (!nodeModalResizeState && nodeModalEl) nodeModalEl.style.removeProperty('cursor');
  }

  function nodeModalInlineStyle() {
    if (!nodeModalRect || nodeModalFullscreen) return '';
    return `left:${nodeModalRect.left}px;top:${nodeModalRect.top}px;width:${nodeModalRect.width}px;height:${nodeModalRect.height}px;max-height:none;transform:none;`;
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
    if (cfg.toolType === 'action_prep') outRows = inRows;
    if (cfg.toolType === 'table_node') outRows = inRows;
    if (cfg.toolType === 'api_mutation') {
      const requestMode = String(cfg.settings.requestMode || cfg.settings.request_mode || 'row_per_request').trim().toLowerCase();
      const batchSize = Math.max(1, Number(cfg.settings.batchSize || cfg.settings.batch_size || 50));
      outRows = requestMode === 'batch_request' ? Math.ceil(inRows / batchSize) : inRows;
    }
    if (cfg.toolType === 'db_write') outRows = Math.max(0, Math.round(inRows * Math.max(0, Math.min(100, Number(cfg.settings.writeSuccessRate || 98))) / 100));
    if (cfg.toolType === 'end_process') outRows = 0;
    if (cfg.toolType === 'split_data') {
      const mode = String(cfg.settings.splitMode || 'duplicate').trim().toLowerCase();
      const factor = Number(cfg.settings.splitMultiplier || 1);
      if (mode === 'split') {
        const raw = Number.isFinite(factor) && factor > 0 ? Math.max(0, factor) : 1;
        outRows = Math.max(0, Math.round(inRows / raw));
      } else {
        const raw = Number.isFinite(factor) && factor > 1 ? Math.max(1, factor) : 2;
        outRows = Math.max(0, Math.round(inRows * raw));
      }
    }
    if (cfg.toolType === 'merge_data') outRows = inRows;
    if (cfg.toolType === 'condition_if') outRows = inRows;
    if (cfg.toolType === 'condition_switch') outRows = inRows;
    if (cfg.toolType === 'code_node') outRows = inRows;
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
    if (!nodes.some((n) => n.type === 'data')) {
      out.push({ level: 'info', text: 'Нет отдельных узлов-источников. Это нормально, если данные читает сама нода процесса.' });
    }
    if (!nodes.some((n) => n.type === 'tool' && toolCfg(n).toolType === 'start_process')) out.push({ level: 'error', text: 'Нет узла Старт процесса' });
    if (!nodes.some((n) => n.type === 'tool' && toolCfg(n).toolType === 'end_process')) out.push({ level: 'error', text: 'Нет узла Конец процесса' });
    if (processCodeConflicts.length) {
      const dup = processCodeConflicts.map((c) => c.display_code || c.normalized_code).join(', ');
      out.push({ level: 'error', text: `Дубли внутренних кодов процесса: ${dup}` });
    }
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
    if (cfg.toolType === 'action_prep') outRows = inRows;
    if (cfg.toolType === 'table_node') outRows = inRows;
    if (cfg.toolType === 'api_mutation') {
      const requestMode = String(cfg.settings.requestMode || cfg.settings.request_mode || 'row_per_request').trim().toLowerCase();
      const batchSize = Math.max(1, Number(cfg.settings.batchSize || cfg.settings.batch_size || 50));
      outRows = requestMode === 'batch_request' ? Math.ceil(inRows / batchSize) : inRows;
    }
    if (cfg.toolType === 'db_write') outRows = Math.max(0, Math.round(inRows * Math.max(0, Math.min(100, Number(cfg.settings.writeSuccessRate || 98))) / 100));
    if (cfg.toolType === 'end_process') outRows = 0;
    if (cfg.toolType === 'split_data') {
      const mode = String(cfg.settings.splitMode || 'duplicate').trim().toLowerCase();
      const factor = Number(cfg.settings.splitMultiplier || 1);
      if (mode === 'split') {
        const raw = Number.isFinite(factor) && factor > 0 ? Math.max(0, factor) : 1;
        outRows = Math.max(0, Math.round(inRows / raw));
      } else {
        const raw = Number.isFinite(factor) && factor > 1 ? Math.max(1, factor) : 2;
        outRows = Math.max(0, Math.round(inRows * raw));
      }
    }
    if (cfg.toolType === 'merge_data') outRows = inRows;
    if (cfg.toolType === 'condition_if') outRows = inRows;
    if (cfg.toolType === 'condition_switch') outRows = inRows;
    if (cfg.toolType === 'code_node') outRows = inRows;
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

  function normalizeTableNodeFilter(raw: any, idx: number): TableNodeFilter {
    const dataType = String(raw?.dataType || '').trim();
    const operator = String(raw?.operator || '').trim();
    const nextDataType: TableNodeFilter['dataType'] =
      dataType === 'text' || dataType === 'date' || dataType === 'boolean' ? dataType : 'number';
    const nextOperator: TableNodeFilter['operator'] =
      operator === 'not_equals' ||
      operator === 'gt' ||
      operator === 'gte' ||
      operator === 'lt' ||
      operator === 'lte' ||
      operator === 'contains' ||
      operator === 'not_contains' ||
      operator === 'is_empty' ||
      operator === 'not_empty'
        ? (operator as TableNodeFilter['operator'])
        : 'equals';
    const id = String(raw?.id || '').trim() || `flt_${idx + 1}`;
    return {
      id,
      dataType: nextDataType,
      operator: nextOperator,
      value: String(raw?.value ?? '').trim()
    };
  }

  function newTableNodeFilter(): TableNodeFilter {
    return { id: uid('flt'), dataType: 'number', operator: 'equals', value: '' };
  }

  function tableNodeFiltersFromSettings(settings: Record<string, string> | undefined): TableNodeFilter[] {
    const raw = settings || {};
    try {
      const parsed = JSON.parse(String(raw.tableFiltersJson || '[]'));
      if (Array.isArray(parsed) && parsed.length) {
        return parsed.map((item, idx) => normalizeTableNodeFilter(item, idx));
      }
    } catch {
      // ignore and fallback to legacy single-filter settings
    }
    const legacy = normalizeTableNodeFilter(
      {
        id: 'flt_1',
        dataType: String(raw.filterDataType || 'number'),
        operator: String(raw.filterOperator || 'equals'),
        value: String(raw.filterValue || '')
      },
      0
    );
    return [legacy];
  }

  function tableNodeFilters(node: WorkflowNode | null | undefined): TableNodeFilter[] {
    if (!node || node.type !== 'tool' || toolCfg(node).toolType !== 'table_node') return [];
    return tableNodeFiltersFromSettings(toolCfg(node).settings || {});
  }

  function replaceTableNodeFilters(nodeId: string, filtersInput: TableNodeFilter[]) {
    const safe = (Array.isArray(filtersInput) && filtersInput.length ? filtersInput : [newTableNodeFilter()]).map((f, idx) =>
      normalizeTableNodeFilter(f, idx)
    );
    const first = safe[0];
    nodes = nodes.map((n) => {
      if (n.id !== nodeId || n.type !== 'tool' || toolCfg(n).toolType !== 'table_node') return n;
      const cfg = toolCfg(n);
      return {
        ...n,
        config: {
          ...cfg,
          settings: {
            ...(cfg.settings || {}),
            tableFiltersJson: JSON.stringify(safe),
            filterDataType: first.dataType,
            filterOperator: first.operator,
            filterValue: first.value
          }
        }
      };
    });
  }

  function addTableNodeFilter(nodeId: string) {
    const node = nodes.find((n) => n.id === nodeId);
    const curr = tableNodeFilters(node);
    replaceTableNodeFilters(nodeId, [...curr, newTableNodeFilter()]);
  }

  function removeTableNodeFilter(nodeId: string, filterId: string) {
    const node = nodes.find((n) => n.id === nodeId);
    const curr = tableNodeFilters(node);
    replaceTableNodeFilters(
      nodeId,
      curr.filter((f) => f.id !== filterId)
    );
  }

  function updateTableNodeFilter(nodeId: string, filterId: string, patch: Partial<TableNodeFilter>) {
    const node = nodes.find((n) => n.id === nodeId);
    const curr = tableNodeFilters(node);
    const next = curr.map((f) => (f.id === filterId ? normalizeTableNodeFilter({ ...f, ...patch, id: f.id }, 0) : f));
    replaceTableNodeFilters(nodeId, next);
  }

  function confirmTableNodeFilters(nodeId: string) {
    const now = new Date().toISOString();
    nodes = nodes.map((n) => {
      if (n.id !== nodeId || n.type !== 'tool' || toolCfg(n).toolType !== 'table_node') return n;
      const cfg = toolCfg(n);
      return {
        ...n,
        config: {
          ...cfg,
          settings: {
            ...(cfg.settings || {}),
            filtersConfirmedAt: now
          }
        }
      };
    });
    banner = 'Фильтры ноды "Таблица" подтверждены';
  }

  function replaceToolSettings(nodeId: string, settingsPatch: Record<string, any>) {
    nodes = nodes.map((n) => {
      if (n.id !== nodeId || n.type !== 'tool') return n;
      const current = toolCfg(n);
      const nextSettings: Record<string, string> = {};
      const src = settingsPatch && typeof settingsPatch === 'object' ? settingsPatch : {};
      for (const [key, value] of Object.entries(src)) {
        if (value === undefined || value === null) continue;
        nextSettings[key] = typeof value === 'string' ? value : JSON.stringify(value);
      }
      return {
        ...n,
        config: {
          ...current,
          settings: {
            ...(current.settings || {}),
            ...nextSettings
          }
        }
      };
    });
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

  function onTableNodeFilterDataTypeChange(nodeId: string, filterId: string, event: Event) {
    const raw = selectValue(event);
    const dataType: TableNodeFilter['dataType'] =
      raw === 'text' || raw === 'date' || raw === 'boolean' ? raw : 'number';
    updateTableNodeFilter(nodeId, filterId, { dataType });
  }

  function onTableNodeFilterOperatorChange(nodeId: string, filterId: string, event: Event) {
    const raw = selectValue(event);
    const operator: TableNodeFilter['operator'] =
      raw === 'not_equals' ||
      raw === 'gt' ||
      raw === 'gte' ||
      raw === 'lt' ||
      raw === 'lte' ||
      raw === 'contains' ||
      raw === 'not_contains' ||
      raw === 'is_empty' ||
      raw === 'not_empty'
        ? raw
        : 'equals';
    updateTableNodeFilter(nodeId, filterId, { operator });
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
    q.set('pane', 'api');
    if (templateStoreId) q.set('api_store_id', templateStoreId);
    if (deskId > 0) q.set('desk_id', String(deskId));
    const nodeName = String(node.config?.name || '').trim();
    if (nodeName) q.set('from_node', nodeName);
    window.location.hash = q.toString() ? `#desk/data?${q.toString()}` : '#desk/data?pane=api';
  }

  function onApiBuilderExecutionPreviewChange(event: CustomEvent<any>) {
    const nodeId = String(settingsNodeId || '').trim();
    if (!nodeId) return;
    const execution = event?.detail?.execution;
    if (!execution || typeof execution !== 'object') return;
    nodeExecutions = {
      ...nodeExecutions,
      [nodeId]: {
        startedAt: String(execution.startedAt || new Date().toISOString()),
        durationMs: Math.max(0, Number(execution.durationMs || 0)),
        status: Math.max(0, Number(execution.status || 0)),
        ok: Boolean(execution.ok),
        totalRequests: Math.max(0, Number(execution.totalRequests || 0)),
        payloadCount: Math.max(0, Number(execution.payloadCount || 0)),
        payloadSize: Math.max(0, Number(execution.payloadSize || 0)),
        requestPreview: execution.requestPreview && typeof execution.requestPreview === 'object' ? structuredClone(execution.requestPreview) : execution.requestPreview,
        responsePreview: execution.responsePreview && typeof execution.responsePreview === 'object' ? structuredClone(execution.responsePreview) : execution.responsePreview,
        error: execution.error ? String(execution.error) : undefined
      }
    };
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
    window.addEventListener('ao:create-table-node', onCreateTableNodeEvent as EventListener);
    processStatusClock = Date.now();
    processStatusClockTimer = setInterval(() => {
      processStatusClock = Date.now();
    }, 1000);
    await loadNodeRegistry();
    await loadDynamicSourceCatalog();
    await loadWorkflowDeskFromServer();
    await refreshAutomationContour();
    deskInitialized = true;
    restartDeskAutosaveTimer();
  });

  onDestroy(() => {
    window.removeEventListener('ao:create-table-node', onCreateTableNodeEvent as EventListener);
    stopNodeModalResize();
    clearDeskAutosaveTimer();
    if (processStatusClockTimer) {
      clearInterval(processStatusClockTimer);
      processStatusClockTimer = null;
    }
  });
</script>

<section class="panel workflow-v2">
  <h3>Конструктор процессов данных</h3>

  <div class="desk-bar">
    <div class="desk-storage">
      <label class="desk-picker">
        <span>Рабочий стол</span>
        <select
          value={workflowDeskPickId}
          on:change={(e) => selectWorkflowDeskById(Number(selectValue(e) || 0))}
          disabled={deskSaving || deskLoading || !workflowDeskOptions.length}
          title="Выбери рабочий стол"
        >
          {#if !workflowDeskPickId}
            <option value="">Выбери рабочий стол</option>
          {/if}
          {#each workflowDeskOptions as deskOption (deskOption.id)}
            <option value={String(deskOption.id)}>{deskOption.name}</option>
          {/each}
        </select>
      </label>
      <button class="mini" type="button" on:click={createNewWorkflowDesk} disabled={deskSaving || deskLoading}>
        Новый рабочий стол
      </button>
      <button class="mini danger" type="button" on:click={deleteCurrentWorkflowDesk} disabled={!deskId || deskSaving || deskLoading}>
        Удалить рабочий стол
      </button>
      <span>Хранятся в таблице:</span>
      <strong>{workflowDeskStorageRef}</strong>
      <span>ID: {deskId || '-'}</span>
      <span>Ревизия: {deskRevision || '-'}</span>
      <span class={deskDirty ? 'dirty-flag' : 'clean-flag'}>{deskDirty ? 'Есть несохраненные изменения' : 'Синхронизировано'}</span>
      <span class={`desk-run-state desk-run-state-${deskExecutionState.mode}`} title={deskExecutionState.hint}>
        {deskExecutionState.label}
      </span>
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

  <div class="topbar">
    <button title="Удаляет ноды и связи на текущем рабочем поле." on:click={resetCanvas}>Очистить рабочее поле</button>
    <button title="Обновляет список API-шаблонов и таблиц для настроек нод." on:click={loadDynamicSourceCatalog} disabled={sourceCatalogLoading}>
      {sourceCatalogLoading ? 'Обновление каталога...' : 'Обновить каталог шаблонов'}
    </button>
    <div class="summary">
      <span>Двойной клик по ноде открывает её настройку</span>
      <span>Кнопки на ноде: запуск, пауза, остановка</span>
    </div>
  </div>

  <div class="workspace">
    <aside class="ops-pane">
      <div class="ops-bar ops-bar-side">
        <div class="ops-actions">
          <button
            class="mini primary"
            title="Фиксирует текущую схему как опубликованную версию, по которой работает сервер."
            on:click={publishDeskVersion}
            disabled={!deskId || publishBusy || deskLoading || deskSaving || hasProcessCodeConflicts()}
          >
            {publishBusy ? 'Публикация...' : 'Опубликовать рабочий стол'}
          </button>
          <button
            class="mini"
            title="Запускает все включенные старт-процессы вручную."
            on:click={() => triggerPublishedProcess('')}
            disabled={!deskId || triggerBusy || !publishedDeskReady}
          >
            {triggerBusy ? 'Запуск...' : 'Ручной запуск'}
          </button>
          <button
            class="mini"
            title="Обновляет статусы планировщика, процессов, запусков и очереди."
            on:click={refreshAutomationContour}
            disabled={monitorBusy || !deskId}
          >
            {monitorBusy ? 'Обновление...' : 'Обновить мониторинг'}
          </button>
          <span class="ops-ref">
            Опубликованная версия: <strong>{publishedDeskVersionId || '-'}</strong>
          </span>
          <span class={publishedDeskReady ? 'clean-flag' : 'dirty-flag'}>
            {publishedDeskReady ? 'Опубликовано' : 'Публикации нет'}
          </span>
        </div>
        {#if processCodeConflicts.length}
          <div class="ops-error">
            Внутренний код процесса должен быть уникальным. Найдены дубли:
            {processCodeConflicts.map((c) => c.display_code || c.normalized_code).join(', ')}
          </div>
        {/if}
        <div class="ops-grid ops-grid-side">
          <div class="ops-card process-status-board">
            <div class="process-board-head">
              <div>
                <h5>Состояние процессов</h5>
                <div class="ops-note">
                  Здесь собран один пользовательский статус поверх опубликованной версии, Start-настроек, server runs, steps и jobs.
                </div>
              </div>
              <div class="process-board-chips">
                <span class="desk-status-chip">Версия: {publishedDeskVersionId || '-'}</span>
                <span class={deskDirty ? 'desk-status-chip warn' : 'desk-status-chip ok'}>
                  {deskDirty ? 'Есть неопубликованные изменения' : 'Черновик синхронизирован'}
                </span>
                <span class={`desk-status-chip desk-status-chip-${deskExecutionState.mode}`} title={deskExecutionState.hint}>
                  {deskExecutionState.label}
                </span>
              </div>
            </div>
            {#if processStatusModels.length}
              <div class="process-status-list">
                {#each processStatusModels as processModel (processModel.startNodeId)}
                  {@const draftProcess = draftProcesses.find((item) => item.start_node_id === processModel.startNodeId)}
                  <article class={`process-status-card tone-${processModel.statusTone}`}>
                    <div class="process-status-card-head">
                      <div class="process-status-main">
                        <strong>{processModel.name || processModel.startNodeId}</strong>
                        <div class="process-status-meta">
                          <span>Код: {processModel.processCode || '-'}</span>
                          <span>Триггер: {triggerLabel(processModel.triggerType)}</span>
                          {#if draftProcess}
                            <span>{draftProcessScopeBadge(draftProcess)}</span>
                          {/if}
                        </div>
                      </div>
                      <div class="process-status-actions">
                        <button
                          class="mini toggle-btn"
                          class:active={Boolean(processModel.isEnabled)}
                          title={processModel.isPublished ? (processModel.isEnabled ? 'Отключить опубликованный процесс' : 'Включить опубликованный процесс') : 'Сначала опубликуй рабочий стол'}
                          on:click={() => togglePublishedProcess(processModel.startNodeId, !Boolean(processModel.isEnabled))}
                          disabled={processModel.enableActionDisabled || Boolean(processBusyByNode[processModel.startNodeId])}
                        >
                          {processModel.enableActionLabel}
                        </button>
                        <button
                          class="mini primary"
                          on:click={() => triggerPublishedProcess(processModel.startNodeId)}
                          disabled={processModel.actionDisabled || triggerBusy}
                        >
                          {processModel.actionLabel}
                        </button>
                        <button
                          class="mini"
                          on:click={() =>
                            openRuntimeInspectorForRun(
                              processModel.runUid,
                              processModel.currentStep.nodeId || processModel.failedStep.nodeId || processModel.startNodeId
                            )}
                          disabled={!processModel.runUid}
                        >
                          Открыть детали
                        </button>
                      </div>
                    </div>
                    <div class="process-status-strip">
                      <span class={`process-state-pill tone-${processModel.statusTone}`}>{processModel.statusLabel}</span>
                      <span class={processModel.isPublished ? 'clean-flag' : 'dirty-flag'}>
                        {processModel.isPublished ? 'Опубликован' : 'Не опубликован'}
                      </span>
                      <span class={processModel.isEnabled ? 'clean-flag' : 'dirty-flag'}>
                        {processModel.isEnabled ? 'Включён' : 'Выключен'}
                      </span>
                      {#if processModel.runUid}
                        <code>{processModel.runUid}</code>
                      {/if}
                    </div>
                    <div class="process-status-message">{processModel.primaryMessage}</div>
                    <div class="process-status-grid">
                      <div class="process-status-kv">
                        <span>Последний / активный run</span>
                        <strong>{processModel.runUid || '-'}</strong>
                      </div>
                      <div class="process-status-kv">
                        <span>Старт</span>
                        <strong>{processModel.runStartedAt || '-'}</strong>
                      </div>
                      <div class="process-status-kv">
                        <span>Длительность</span>
                        <strong>{processModel.runUid ? processDurationLabel(processModel) : '-'}</strong>
                      </div>
                      <div class="process-status-kv">
                        <span>Текущий шаг</span>
                        <strong>{processModel.currentStep.nodeName || '-'}</strong>
                      </div>
                      <div class="process-status-kv">
                        <span>Последний успешный шаг</span>
                        <strong>{processModel.lastSuccessfulStep.nodeName || '-'}</strong>
                      </div>
                      <div class="process-status-kv">
                        <span>Ошибка на шаге</span>
                        <strong>{processModel.failedStep.nodeName || '-'}</strong>
                      </div>
                    </div>
                  </article>
                {/each}
              </div>
            {:else}
              <div class="empty">На рабочем столе нет старт-процессов.</div>
            {/if}
            {#if outdatedPublishedProcesses.length}
              <div class="issue warn">
                В опубликованной версии ещё есть {outdatedPublishedProcesses.length} процесс(а), которых нет на рабочем столе.
                Нажми «Опубликовать рабочий стол», чтобы синхронизировать сервер.
              </div>
            {/if}
          </div>
          <details class="ops-card ops-card-details" bind:open={schedulerDetailsOpen}>
            <summary>Планировщик</summary>
            <div class="ops-card-body">
              <div class="kv"><span>Состояние</span><strong>{schedulerView.enabled ? 'Включен' : 'Выключен'}</strong></div>
              <div class="kv"><span>Последний тик</span><strong>{schedulerView.last_tick_at || '-'}</strong></div>
              <div class="kv"><span>Тик воркера</span><strong>{schedulerView.worker_last_tick_at || '-'}</strong></div>
              <div class="kv"><span>Очередь</span><strong>{schedulerView.queue_depth} / в работе {schedulerView.queue_running}</strong></div>
              <div class="kv"><span>Ошибочные задания</span><strong>{schedulerView.queue_dead_letter}</strong></div>
              <div class="kv"><span>Событий в ожидании</span><strong>{schedulerView.dependency_event_backlog}</strong></div>
              {#if schedulerView.last_error}
                <div class="ops-error">Ошибка планировщика: {schedulerView.last_error}</div>
              {/if}
              {#if schedulerView.worker_last_error}
                <div class="ops-error">Ошибка воркера: {schedulerView.worker_last_error}</div>
              {/if}
            </div>
          </details>
          <details class="ops-card ops-card-details" bind:open={workflowLogDetailsOpen}>
            <summary>Журнал выполнения</summary>
            <div class="ops-card-body">
              <div class="ops-note">
                Это системный журнал выполнения рабочего стола. Он подключается по системному шаблону и не создает вторую таблицу.
              </div>
              <div class="kv"><span>Состояние</span><strong>{workflowLogEnabled ? 'Подключен' : 'Выключен'}</strong></div>
              <div class="kv"><span>Системный шаблон</span><strong>Bronze системный лог workflow</strong></div>
              <div class="process-actions">
                <button
                  class="mini toggle-btn"
                  class:active={workflowLogEnabled}
                  on:click={toggleWorkflowLogEnabled}
                  type="button"
                >
                  {workflowLogEnabled ? 'Вести журнал: вкл' : 'Вести журнал: выкл'}
                </button>
              </div>
              <div class="ops-subhead">Источник журнала выполнения</div>
              <button
                type="button"
                class="source-pill"
                on:click={toggleWorkflowLogPicker}
                disabled={!workflowLogCompatibleSources.length}
                title={workflowLogCurrentSource?.description || 'Выбери системный источник журнала выполнения'}
              >
                {workflowLogCurrentSource?.name || 'Выбери системный источник'}
              </button>
              {#if workflowLogPickerOpen}
                <div class="storage-picker">
                  <select bind:value={workflowLogPickValue}>
                    {#if !workflowLogPickValue}
                      <option value="">Выбери источник</option>
                    {/if}
                    {#each workflowLogCompatibleSources as source (source.key)}
                      <option value={source.key}>{source.name}</option>
                    {/each}
                  </select>
                  <button type="button" class="mini" on:click={applyWorkflowLogSourceChoice} disabled={!workflowLogPickValue}>
                    Подключить
                  </button>
                </div>
              {/if}
              {#if workflowLogCurrentSource}
                <div class="ops-note">
                  {workflowLogCurrentSource.description || 'Подключенный источник использует системный run log и связанные шаги, очередь и агрегаты.'}
                </div>
                <div class="system-source-list">
                  {#if workflowLogCurrentSource.primary}
                    <div class="system-source-row">
                      <span>{workflowLogCurrentSource.primary.label}</span>
                      <code>{workflowLogCurrentSource.primary.qname}</code>
                    </div>
                  {/if}
                  {#each workflowLogCurrentSource.details || [] as detail (detail.key)}
                    <div class="system-source-row">
                      <span>{detail.label}</span>
                      <code>{detail.qname}</code>
                    </div>
                  {/each}
                </div>
              {:else if workflowLogEnabled}
                <div class="ops-error">Выбери совместимый системный источник журнала выполнения.</div>
              {:else}
                <div class="ops-note">Журнал выполнения не подключен к настройкам этого рабочего стола.</div>
              {/if}
              {#if !workflowLogCompatibleSources.length}
                <div class="ops-error">Нет доступных системных источников workflow log. Проверь server runtime и bootstrap workflow automation.</div>
              {/if}
              {#if workflowLogSourceError}
                <div class="ops-error">{workflowLogSourceError}</div>
              {/if}
            </div>
          </details>
          <details class="ops-card ops-card-details" bind:open={runsJobsDetailsOpen}>
            <summary>Запуски и очередь</summary>
            <div class="ops-card-body">
              <div class="ops-columns">
                <div>
                  <div class="ops-subhead">Запуски ({processRuns.length})</div>
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
                  <div class="ops-subhead">Задания очереди ({workflowJobs.length})</div>
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
          </details>
          <details class="ops-card ops-card-details" bind:open={runtimeInspectorDetailsOpen}>
            <summary>Runtime inspector</summary>
            <div class="ops-card-body">
              <div class="ops-note">
                Read-only просмотр server runtime truth. Здесь отдельно показаны canonical input/output шага и transport/debug payload, без подмены desk descriptor или preview.
              </div>
              {#if processRuns.length}
                <label class="sec-label runtime-run-picker">
                  <span>Run</span>
                  <select
                    value={runtimeInspectorRunUid}
                    on:change={(e) => loadRuntimeInspectorRun(selectValue(e), { focusNodeId: settingsNode?.id || '' })}
                    disabled={runtimeInspectorLoading}
                  >
                    {#each processRuns.slice(0, 30) as run (run.run_uid)}
                      <option value={run.run_uid}>
                        {run.start_node_id} · {run.status} · {run.started_at || run.run_uid}
                      </option>
                    {/each}
                  </select>
                </label>
                {#if runtimeInspectorLoading}
                  <div class="empty">Загрузка runtime run...</div>
                {:else if runtimeInspectorError}
                  <div class="ops-error">{runtimeInspectorError}</div>
                {:else if runtimeInspectorRunDetail}
                  <div class="preview-meta">
                    <span>Run UID: {runtimeInspectorRunDetail.run?.run_uid || '-'}</span>
                    <span>Статус: {runtimeInspectorRunDetail.run?.status || '-'}</span>
                    <span>Старт: {runtimeInspectorRunDetail.run?.started_at || '-'}</span>
                    <span>Шагов: {runtimeInspectorSteps().length}</span>
                  </div>
                  <div class="runtime-step-strip">
                    {#each runtimeInspectorSteps() as step}
                      <button
                        type="button"
                        class="mini runtime-step-pill"
                        class:active={Math.max(0, Number(step?.step_order || 0)) === Math.max(0, Number(runtimeInspectorSelectedStepOrder || 0))}
                        on:click={() => (runtimeInspectorSelectedStepOrder = Math.max(0, Number(step?.step_order || 0)))}
                      >
                        {step.step_order}. {step.node_name || step.node_id} · {step.status}
                      </button>
                    {/each}
                  </div>
                  {@const selectedRuntimeStep = runtimeInspectorSelectedStep()}
                  {#if selectedRuntimeStep}
                    {@const selectedRuntimeSteps = runtimeInspectorSteps()}
                    {@const selectedRuntimeIndex = selectedRuntimeSteps.findIndex((step) => Math.max(0, Number(step?.step_order || 0)) === Math.max(0, Number(selectedRuntimeStep?.step_order || 0)))}
                    {@const previousRuntimeStep = selectedRuntimeIndex > 0 ? selectedRuntimeSteps[selectedRuntimeIndex - 1] : null}
                    {@const currentInputRows = runtimeRowsFromValue(selectedRuntimeStep.input_json)}
                    {@const currentInputColumns = runtimeColumnsFromRows(currentInputRows)}
                    {@const currentOutputRows = runtimeRowsFromValue(selectedRuntimeStep.output_json)}
                    {@const currentOutputColumns = runtimeColumnsFromRows(currentOutputRows)}
                    <div class="runtime-step-card">
                      <div class="parser-shell-summary">
                        <span class="chip-chip readonly-chip">Шаг {selectedRuntimeStep.step_order}</span>
                        <span class="chip-chip readonly-chip">{selectedRuntimeStep.node_name || selectedRuntimeStep.node_id}</span>
                        <span class="chip-chip readonly-chip">Тип: {selectedRuntimeStep.node_type || '-'}</span>
                        <span class="chip-chip readonly-chip">Статус: {selectedRuntimeStep.status || '-'}</span>
                      </div>
                      <div class="preview-meta">
                        <span>Canonical input rows: {runtimeRowCountFromValue(selectedRuntimeStep.input_json)}</span>
                        <span>Canonical output rows: {runtimeRowCountFromValue(selectedRuntimeStep.output_json)}</span>
                        <span>Request/debug: {selectedRuntimeStep.request_payload !== undefined ? 'есть' : 'нет'}</span>
                        <span>Response/debug: {selectedRuntimeStep.response_payload !== undefined ? 'есть' : 'нет'}</span>
                      </div>
                      {#if previousRuntimeStep}
                        <div class="inline-hint">
                          Handoff от предыдущей ноды `{previousRuntimeStep.node_name || previousRuntimeStep.node_id}` к текущей:
                          {runtimeValuesSemanticallyMatch(previousRuntimeStep.output_json, selectedRuntimeStep.input_json)
                            ? ' canonical output и canonical input совпадают по сохранённому значению.'
                            : ' вход и выход различаются, смотри canonical JSON ниже.'}
                        </div>
                      {/if}
                      {#if settingsNode && String(settingsNode.id || '').trim() === String(selectedRuntimeStep.node_id || '').trim()}
                        <div class="inline-hint">Этот runtime step соответствует ноде, которая сейчас открыта в редакторе.</div>
                      {/if}
                      <div class="runtime-io-grid">
                        <details class="runtime-io-card" open>
                          <summary>Canonical input</summary>
                          {#if currentInputColumns.length}
                            <div class="preview-columns">
                              {#each currentInputColumns as column}
                                <span>{column}</span>
                              {/each}
                            </div>
                          {/if}
                          {#if currentInputRows.length}
                            <div class="preview-table-wrap compact-preview-wrap">
                              <table class="preview-table">
                                <thead>
                                  <tr>
                                    {#each currentInputColumns as column}
                                      <th>{column}</th>
                                    {/each}
                                  </tr>
                                </thead>
                                <tbody>
                                  {#each currentInputRows.slice(0, 5) as row}
                                    <tr>
                                      {#each currentInputColumns as column}
                                        <td>{typeof row?.[column] === 'object' ? JSON.stringify(row?.[column]) : String(row?.[column] ?? '')}</td>
                                      {/each}
                                    </tr>
                                  {/each}
                                </tbody>
                              </table>
                            </div>
                          {/if}
                          <pre>{runtimeJsonText(selectedRuntimeStep.input_json, '{}')}</pre>
                        </details>
                        <details class="runtime-io-card" open>
                          <summary>Canonical output</summary>
                          {#if currentOutputColumns.length}
                            <div class="preview-columns">
                              {#each currentOutputColumns as column}
                                <span>{column}</span>
                              {/each}
                            </div>
                          {/if}
                          {#if currentOutputRows.length}
                            <div class="preview-table-wrap compact-preview-wrap">
                              <table class="preview-table">
                                <thead>
                                  <tr>
                                    {#each currentOutputColumns as column}
                                      <th>{column}</th>
                                    {/each}
                                  </tr>
                                </thead>
                                <tbody>
                                  {#each currentOutputRows.slice(0, 5) as row}
                                    <tr>
                                      {#each currentOutputColumns as column}
                                        <td>{typeof row?.[column] === 'object' ? JSON.stringify(row?.[column]) : String(row?.[column] ?? '')}</td>
                                      {/each}
                                    </tr>
                                  {/each}
                                </tbody>
                              </table>
                            </div>
                          {/if}
                          <pre>{runtimeJsonText(selectedRuntimeStep.output_json, '{}')}</pre>
                        </details>
                        <details class="runtime-io-card">
                          <summary>Request / debug</summary>
                          <pre>{runtimeJsonText(selectedRuntimeStep.request_payload, '{}')}</pre>
                        </details>
                        <details class="runtime-io-card">
                          <summary>Response / debug</summary>
                          <pre>{runtimeJsonText(selectedRuntimeStep.response_payload, '{}')}</pre>
                        </details>
                      </div>
                    </div>
                  {/if}
                {:else}
                  <div class="empty">Выбери запуск, чтобы увидеть сохранённый server runtime path.</div>
                {/if}
              {:else}
                <div class="empty">Серверных запусков пока нет. Сначала опубликуй и запусти процесс.</div>
              {/if}
            </div>
          </details>
        </div>
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
          {@const apiNode = isApiNode(node) || isApiToolNode(node)}
          {@const processIndicator = processIndicatorForNode(node)}
          <div
            class="node {node.type} {selectedNodeId === node.id ? 'selected' : ''}"
            class:node-api={apiNode}
            style={`left:${node.x}px;top:${node.y}px;`}
            on:mousedown={(e) => startNodeDrag(e, node)}
            on:click={() => (selectedNodeId = node.id)}
            on:dblclick={(e) => onNodeDoubleClick(e, node.id)}
          >
            <div class="node-head">
              {#if apiNode}
                {@const req = getApiRequestForNode(node)}
                {@const templateSource = resolveTemplateSourceForNode(node, req)}
                <strong class="node-title node-title-api">{String(templateSource?.name || node.config.name || 'API-нода').trim()}</strong>
              {:else}
                <strong class="node-title">{node.config.name}</strong>
              {/if}
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
            <div class="node-meta">
              {node.type === 'data'
                ? node.config.datasetId
                : apiNode
                ? 'API-нода'
                : toolLabelByType(toolCfg(node).toolType)}
            </div>
            {#if processIndicator}
              <div class={`node-process-indicator tone-${processIndicator.tone}`} title={processIndicator.hint}>
                {processIndicator.label}
              </div>
            {/if}
            {#if chainRunActive && chainCurrentNodeId === node.id}
              <div class="runtime running">Выполняется...</div>
            {/if}
            {#if apiNode}
              {@const req = getApiRequestForNode(node)}
              <div class="api-meta">{req.method || 'GET'} {req.url || ''}</div>
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
      <h4>Ноды процесса</h4>
      <div class="help">Перетащи нужную ноду на рабочее поле. В списке показаны только поддерживаемые ноды.</div>
      {#if sourceCatalogError}
        <div class="source-error">{sourceCatalogError}</div>
      {/if}
      {#if nodeRegistryError}
        <div class="source-error">Реестр нод недоступен. Используется встроенный резервный список.</div>
      {/if}
      {#each groupedTools as group (group.title)}
        <section class="tool-group">
          <h5>{group.title}</h5>
          <div class="items">
            {#each group.items as t (t.id)}
              <button
                draggable="true"
                class="item tool"
                title={toolDescriptionByType(t.toolType)}
                on:dragstart={(e) =>
                  e.dataTransfer?.setData(
                    'application/x-workflow-tool',
                    JSON.stringify({
                      ...t,
                      name: toolLabelByType(t.toolType),
                      description: toolDescriptionByType(t.toolType)
                    })
                  )}
              >
                <strong>{toolLabelByType(t.toolType)}</strong>
                <span>{toolDescriptionByType(t.toolType)}</span>
              </button>
            {/each}
          </div>
        </section>
      {/each}

      <section class="props">
        <h5>Настройка выбранной ноды</h5>
        {#if selectedNode && selectedNode.type === 'tool'}
          {#if toolCfg(selectedNode).toolType === 'schedule_process'}
            <div class="issue warn">Эта нода относится к старой версии конструктора и больше не используется в новых схемах.</div>
          {/if}
          {#if toolCfg(selectedNode).toolType === 'table_parser'}
            <div class="empty">
              Полная настройка parser-ноды открывается по двойному клику по карточке или через модалку настройки узла.
            </div>
          {/if}
          {#if toolCfg(selectedNode).toolType === 'action_prep'}
            <div class="empty">
              Полная настройка подготовки действий открывается по двойному клику по карточке или через модалку настройки узла.
            </div>
          {/if}
          {#if toolCfg(selectedNode).toolType === 'table_node'}
            <div class="empty">
              Полная настройка табличного набора открывается по двойному клику по карточке или через модалку настройки узла.
            </div>
          {/if}
          {#if toolCfg(selectedNode).toolType === 'db_write'}
            <div class="empty">
              Полная настройка ноды записи данных открывается по двойному клику по карточке или через модалку настройки узла.
            </div>
          {/if}
          {#if toolCfg(selectedNode).toolType === 'api_mutation'}
            <div class="empty">
              Полная настройка API-изменения открывается по двойному клику по карточке или через модалку настройки узла.
            </div>
          {/if}
          {#if toolCfg(selectedNode).toolType === 'http_request'}
            <label>
              Метод
              <select value={toolCfg(selectedNode).settings.apiMethod || 'GET'} on:change={(e) => updateSetting(selectedNode.id, 'apiMethod', selectValue(e))}>
                <option value="GET">GET</option>
                <option value="POST">POST</option>
                <option value="PUT">PUT</option>
                <option value="PATCH">PATCH</option>
                <option value="DELETE">DELETE</option>
              </select>
            </label>
            <label>URL<input value={toolCfg(selectedNode).settings.apiUrl || ''} on:input={(e) => onSettingInput(selectedNode.id, 'apiUrl', e)} /></label>
          {/if}
          {#if toolCfg(selectedNode).toolType === 'split_data'}
            <label>
              Режим
              <select value={toolCfg(selectedNode).settings.splitMode || 'duplicate'} on:change={(e) => updateSetting(selectedNode.id, 'splitMode', selectValue(e))}>
                <option value="duplicate">Дублировать строки</option>
                <option value="split">Отбирать каждую N-ю строку</option>
              </select>
            </label>
            <label>Коэффициент<input value={toolCfg(selectedNode).settings.splitMultiplier || '2'} on:input={(e) => onSettingInput(selectedNode.id, 'splitMultiplier', e)} /></label>
          {/if}
          {#if toolCfg(selectedNode).toolType === 'merge_data'}
            <label>
              Режим
              <select value={toolCfg(selectedNode).settings.mergeMode || 'dedupe'} on:change={(e) => updateSetting(selectedNode.id, 'mergeMode', selectValue(e))}>
                <option value="dedupe">Дедупликация</option>
                <option value="passthrough">Без изменений</option>
              </select>
            </label>
            <label>Поля дедупликации<input value={toolCfg(selectedNode).settings.dedupeBy || ''} on:input={(e) => onSettingInput(selectedNode.id, 'dedupeBy', e)} /></label>
          {/if}
          {#if toolCfg(selectedNode).toolType === 'condition_if'}
            <label>Поле<input value={toolCfg(selectedNode).settings.conditionField || ''} on:input={(e) => onSettingInput(selectedNode.id, 'conditionField', e)} /></label>
            <label>
              Оператор
              <select value={toolCfg(selectedNode).settings.conditionOperator || 'equals'} on:change={(e) => updateSetting(selectedNode.id, 'conditionOperator', selectValue(e))}>
                <option value="equals">Равно</option>
                <option value="not_equals">Не равно</option>
                <option value="gt">Больше</option>
                <option value="gte">Больше или равно</option>
                <option value="lt">Меньше</option>
                <option value="lte">Меньше или равно</option>
                <option value="contains">Содержит</option>
                <option value="not_contains">Не содержит</option>
                <option value="is_empty">Пусто</option>
                <option value="not_empty">Не пусто</option>
              </select>
            </label>
            <label>Значение<input value={toolCfg(selectedNode).settings.conditionValue || ''} on:input={(e) => onSettingInput(selectedNode.id, 'conditionValue', e)} /></label>
          {/if}
          {#if toolCfg(selectedNode).toolType === 'condition_switch'}
            <label>Поле<input value={toolCfg(selectedNode).settings.switchField || ''} on:input={(e) => onSettingInput(selectedNode.id, 'switchField', e)} /></label>
            <label>Case 1<input value={toolCfg(selectedNode).settings.switchCase1Value || ''} on:input={(e) => onSettingInput(selectedNode.id, 'switchCase1Value', e)} /></label>
            <label>Case 2<input value={toolCfg(selectedNode).settings.switchCase2Value || ''} on:input={(e) => onSettingInput(selectedNode.id, 'switchCase2Value', e)} /></label>
            <label>Case 3<input value={toolCfg(selectedNode).settings.switchCase3Value || ''} on:input={(e) => onSettingInput(selectedNode.id, 'switchCase3Value', e)} /></label>
            <label>Case 4<input value={toolCfg(selectedNode).settings.switchCase4Value || ''} on:input={(e) => onSettingInput(selectedNode.id, 'switchCase4Value', e)} /></label>
          {/if}
          {#if toolCfg(selectedNode).toolType === 'code_node'}
            <label>Таймаут, мс<input value={toolCfg(selectedNode).settings.codeTimeoutMs || '5000'} on:input={(e) => onSettingInput(selectedNode.id, 'codeTimeoutMs', e)} /></label>
          {/if}
        {:else}
          <div class="empty">Выберите ноду на рабочем поле, чтобы увидеть настройки.</div>
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
      <div
        bind:this={nodeModalEl}
        class="node-modal"
        class:node-modal-wide={isWideSettingsNode(settingsNode)}
        class:node-modal-fullscreen={nodeModalFullscreen}
        class:node-modal-manual={Boolean(nodeModalRect) && !nodeModalFullscreen}
        class:node-modal-resizable={isWideSettingsNode(settingsNode) && !nodeModalFullscreen}
        style={nodeModalInlineStyle()}
        on:mousedown={onNodeModalMouseDownForResize}
        on:mousemove={onNodeModalMouseMoveForResize}
      on:mouseleave={onNodeModalMouseLeaveForResize}
    >
      <div class="node-modal-head">
        <h4>Настройка узла: {nodeDisplayName(settingsNode)}</h4>
        <div class="node-modal-actions">
          {#if isWideSettingsNode(settingsNode)}
            <button
              class="window-btn"
              title={nodeModalFullscreen ? 'Вернуть обычный размер' : 'Развернуть на весь экран'}
              aria-label={nodeModalFullscreen ? 'Вернуть обычный размер' : 'Развернуть на весь экран'}
              on:click={toggleNodeModalFullscreen}
            >
              {#if nodeModalFullscreen}
                <svg viewBox="0 0 16 16" aria-hidden="true">
                  <path d="M3 5V3h6v2H3zm4 8v-2h6V5h2v8H7zM1 7h2v6h6v2H1V7z" />
                </svg>
              {:else}
                <svg viewBox="0 0 16 16" aria-hidden="true">
                  <path d="M2 2h12v12H2V2zm2 2v8h8V4H4z" />
                </svg>
              {/if}
            </button>
          {/if}
          <button class="window-btn window-btn-close" title="Закрыть окно" aria-label="Закрыть окно" on:click={closeSettingsModal}>
            <svg viewBox="0 0 16 16" aria-hidden="true">
              <path d="M4.2 3L8 6.8 11.8 3 13 4.2 9.2 8 13 11.8 11.8 13 8 9.2 4.2 13 3 11.8 6.8 8 3 4.2 4.2 3z" />
            </svg>
          </button>
        </div>
      </div>

      {#if settingsNode.type === 'tool' && toolCfg(settingsNode).toolType === 'start_process'}
        <div class="node-modal-body">
          <div class="help">
            Настройка точки старта процесса. Здесь задается, когда запускать процесс и в каком режиме он будет работать на сервере.
          </div>
          <label>
            Название процесса
            <input
              value={toolCfg(settingsNode).name || ''}
              on:input={(e) => updateToolName(settingsNode.id, inputValue(e))}
              placeholder="Например: Синхронизация карточек"
            />
            <span class="hint">Это имя отображается в списке процессов и в логах запуска.</span>
          </label>
          <label>
            Активность процесса
            <select
              value={toolCfg(settingsNode).settings.isEnabled || 'true'}
              on:change={(e) => updateSetting(settingsNode.id, 'isEnabled', selectValue(e))}
              title="Если выключено, процесс не будет запускаться автоматически."
            >
              <option value="true">Включен</option>
              <option value="false">Выключен</option>
            </select>
            <span class="hint">Выключенный процесс игнорируется планировщиком.</span>
          </label>
          <label>
            Внутренний код процесса
            <input
              value={toolCfg(settingsNode).settings.processCode || ''}
              on:input={(e) => onSettingInput(settingsNode.id, 'processCode', e)}
              placeholder="Например: client_cards_sync"
            />
            <span class="hint">Это внутренний код процесса. Нужен серверу, чтобы отличать процессы друг от друга. Обычно менять не нужно.</span>
            {#if processCodeConflictByNodeId(settingsNode.id)}
              <span class="code-error">
                Код должен быть уникальным. Найден дубль: {processCodeConflictByNodeId(settingsNode.id)?.display_code || processCodeConflictByNodeId(settingsNode.id)?.normalized_code}
              </span>
              <button class="mini" type="button" on:click={() => assignUniqueStartProcessCode(settingsNode.id)}>
                Исправить автоматически
              </button>
            {:else}
              <span class="code-ok">Код уникален.</span>
            {/if}
          </label>
          <label>
            Тип запуска
            <select
              value={toolCfg(settingsNode).settings.triggerType || 'interval'}
              on:change={(e) => updateSetting(settingsNode.id, 'triggerType', selectValue(e))}
            >
              <option value="interval">По интервалу</option>
              <option value="cron">По расписанию Cron</option>
              <option value="manual">Только вручную</option>
            </select>
            <span class="hint">{triggerHint(toolCfg(settingsNode).settings.triggerType || 'interval')}</span>
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
              Cron-выражение
              <input
                value={toolCfg(settingsNode).settings.cron || '0 * * * *'}
                on:input={(e) => onSettingInput(settingsNode.id, 'cron', e)}
                placeholder="0 * * * *"
              />
              <span class="hint">Формат расписания Cron, например запуск каждый час: <code>0 * * * *</code>.</span>
            </label>
          {/if}
          <div class="interval-grid">
            <label>
              Часовой пояс
              <input value={toolCfg(settingsNode).settings.timezone || 'UTC'} on:input={(e) => onSettingInput(settingsNode.id, 'timezone', e)} />
              <span class="hint">Например: UTC или Europe/Moscow.</span>
            </label>
            <label>
              Правило повторного запуска
              <select
                value={toolCfg(settingsNode).settings.runPolicy || 'single_instance'}
                on:change={(e) => updateSetting(settingsNode.id, 'runPolicy', selectValue(e))}
              >
                <option value="single_instance">Не запускать второй раз, пока первый не завершен</option>
                <option value="allow_parallel">Разрешить параллельные запуски</option>
              </select>
              <span class="hint">{runPolicyLabel(toolCfg(settingsNode).settings.runPolicy || 'single_instance')}</span>
            </label>
          </div>
          <label>
            Для кого запускать процесс
            <select
              value={toolCfg(settingsNode).settings.executionScopeMode || 'single_global'}
              on:change={(e) => {
                const mode = selectValue(e);
                updateSetting(settingsNode.id, 'executionScopeMode', mode);
                applyScopeTypeByMode(settingsNode.id, mode);
              }}
            >
              <option value="single_global">Один общий запуск</option>
              <option value="for_each_tenant">Отдельно для каждого клиента</option>
              <option value="for_each_source_account">Отдельно для каждого источника</option>
              <option value="for_each_segment">Отдельно для каждой группы</option>
              <option value="for_each_partition">Отдельно по частям данных</option>
            </select>
            <span class="hint">{executionScopeModeHelp(toolCfg(settingsNode).settings.executionScopeMode || 'single_global')}</span>
          </label>
          {#if shouldShowTenantId(toolCfg(settingsNode).settings || {})}
            <label>
              Идентификатор клиента
              <input value={toolCfg(settingsNode).settings.tenantId || ''} on:input={(e) => onSettingInput(settingsNode.id, 'tenantId', e)} />
              <span class="hint">Заполняется только если процесс должен работать для конкретного клиента.</span>
            </label>
          {/if}

          <details class="advanced-box">
            <summary>Расширенные настройки (для технической настройки)</summary>
            <div class="advanced-grid">
              <label>
                Тип контекста запуска
                <select
                  value={toolCfg(settingsNode).settings.scopeType || 'global'}
                  on:change={(e) => updateSetting(settingsNode.id, 'scopeType', selectValue(e))}
                >
                  <option value="global">Общий</option>
                  <option value="tenant">Клиент</option>
                  <option value="source_account">Источник</option>
                  <option value="segment">Группа</option>
                  <option value="partition">Часть данных</option>
                  <option value="system">Системный</option>
                </select>
              </label>
              <label>
                Ключ контекста
                <input value={toolCfg(settingsNode).settings.scopeRef || ''} on:input={(e) => onSettingInput(settingsNode.id, 'scopeRef', e)} />
              </label>
            </div>
            <div class="advanced-grid">
              <label>
                Входной контекст
                <input value={toolCfg(settingsNode).settings.inputScope || ''} on:input={(e) => onSettingInput(settingsNode.id, 'inputScope', e)} />
              </label>
              <label>
                Выходной контекст
                <input value={toolCfg(settingsNode).settings.outputScope || ''} on:input={(e) => onSettingInput(settingsNode.id, 'outputScope', e)} />
              </label>
            </div>
            <label>
              Дополнительный контекст (JSON)
              <textarea
                rows="4"
                value={toolCfg(settingsNode).settings.contextJson || '{}'}
                on:input={(e) => onSettingInput(settingsNode.id, 'contextJson', e)}
              ></textarea>
            </label>
            <label>
              Источник контекста (JSON)
              <textarea
                rows="4"
                value={toolCfg(settingsNode).settings.scopeSource || '{}'}
                on:input={(e) => onSettingInput(settingsNode.id, 'scopeSource', e)}
              ></textarea>
            </label>
          </details>
        </div>
      {/if}

      {#if isApiNode(settingsNode) || (settingsNode.type === 'tool' && toolCfg(settingsNode).toolType === 'api_request')}
        <div class="node-modal-body node-modal-body-api">
          {#if settingsNode}
            {@const currentTemplateName = String(settingsApiTemplateSource?.name || '').trim() || 'Шаблон не привязан'}
            {@const otherNodesCount = apiTemplateUsageSummary.otherNodes.length}
            {@const processCount = apiTemplateUsageSummary.processes.length}
            {@const switchDisabled = !settingsCanSwitchTemplate}
            <section class="api-template-current-block">
              <div class="api-template-line api-template-line-main">
                <span class="api-template-key">Текущий шаблон:</span>
                <span class="api-template-value">{currentTemplateName}</span>
              </div>
              <div class="api-template-line api-template-line-selected">
                <span class="api-template-key">Выбран в библиотеке:</span>
                <span class="api-template-value api-template-value-muted">
                  {settingsHasLibrarySelection ? settingsSelectedLibraryName : 'В библиотеке ничего не выбрано'}
                </span>
              </div>
              <div class="api-template-line api-template-line-usage">
                <span class="api-template-usage-text">
                  Используется: {apiTemplateUsageSummary.inCurrentNode ? 'в этой ноде' : 'эта нода не подключена'}, ещё в {otherNodesCount} {pluralRu(
                    otherNodesCount,
                    'ноде',
                    'нодах',
                    'нодах'
                  )}, в {processCount} {pluralRu(processCount, 'процессе', 'процессах', 'процессах')}
                </span>
                <span class="api-template-usage-actions">
                  <button type="button" class="mini" on:click={() => (apiTemplateUsageExpanded = !apiTemplateUsageExpanded)}>
                    {apiTemplateUsageExpanded ? 'Свернуть' : 'Развернуть'}
                  </button>
                  <button type="button" class="mini" on:click={refreshApiTemplateUsageSummary} disabled={sourceCatalogLoading}>
                    Обновить
                  </button>
                </span>
              </div>
              {#if apiTemplateUsageExpanded}
                <div class="api-template-usage-expanded">
                  <div class="api-template-usage-col">
                    <div class="api-template-usage-title">Ноды</div>
                    {#if apiTemplateUsageSummary.otherNodes.length}
                      <ul>
                        {#each apiTemplateUsageSummary.otherNodes as usageNode (usageNode.node_id)}
                          <li>{usageNode.node_name}</li>
                        {/each}
                      </ul>
                    {:else}
                      <div class="api-template-empty">Только текущая нода.</div>
                    {/if}
                  </div>
                  <div class="api-template-usage-col">
                    <div class="api-template-usage-title">Процессы</div>
                    {#if apiTemplateUsageSummary.processes.length}
                      <ul>
                        {#each apiTemplateUsageSummary.processes as usageProcess (usageProcess.start_node_id)}
                          <li>{usageProcess.process_name} <span class="api-template-process-code">({usageProcess.process_code})</span></li>
                        {/each}
                      </ul>
                    {:else}
                      <div class="api-template-empty">Не используется в опубликованных цепочках.</div>
                    {/if}
                  </div>
                </div>
              {/if}
              <div class="api-template-line api-template-line-switch">
                <button
                  type="button"
                  class="mini primary"
                  on:click={switchSettingsNodeTemplate}
                  disabled={switchDisabled}
                  title={switchDisabled ? 'Выбери другой сохранённый шаблон в библиотеке ниже' : 'Перепривязать ноду к выбранному шаблону'}
                >
                  Сменить шаблон
                </button>
                {#if !settingsHasLibrarySelection}
                  <span class="api-template-switch-hint">В библиотеке ничего не выбрано.</span>
                {:else if !settingsHasLibraryIdentity}
                  <span class="api-template-switch-hint">Выбран черновик. Сначала сохрани шаблон в библиотеке.</span>
                {:else if settingsLibraryMatchesCurrentTemplate}
                  <span class="api-template-switch-hint">Этот шаблон уже подключён.</span>
                {:else if settingsLibraryReadyToSwitch}
                  <span class="api-template-switch-hint">Готов к перепривязке.</span>
                {:else}
                  <span class="api-template-switch-hint">Выбери шаблон в секции “Список API”, затем нажми “Сменить шаблон”.</span>
                {/if}
              </div>
            </section>
            {#key `${settingsNode.id}:${settingsApiBuilderStoreId || 0}`}
              <ApiBuilderTab
                apiBase={API_BASE}
                apiJson={workflowApiJson}
                headers={workflowApiHeaders}
                existingTables={apiBuilderExistingTables}
                refreshTables={refreshApiBuilderTables}
                initialApiStoreId={settingsApiBuilderStoreId}
                workflowDeskId={deskId}
                workflowNodeId={settingsNode.id}
                workflowGraph={captureDeskState()}
                embeddedMode={false}
                on:templateSelectionChange={onApiBuilderTemplateSelectionChange}
                on:templateSaved={onApiBuilderTemplateSaved}
                on:executionPreviewChange={onApiBuilderExecutionPreviewChange}
              />
            {/key}
          {/if}
        </div>
      {/if}
      {#if settingsNode.type === 'tool' && toolCfg(settingsNode).toolType === 'http_request'}
        <div class="node-modal-body">
          <div class="help">Прямой HTTP запрос без привязки к библиотечному шаблону.</div>
          <label>
            Метод
            <select value={toolCfg(settingsNode).settings.apiMethod || 'GET'} on:change={(e) => updateSetting(settingsNode.id, 'apiMethod', selectValue(e))}>
              <option value="GET">GET</option>
              <option value="POST">POST</option>
              <option value="PUT">PUT</option>
              <option value="PATCH">PATCH</option>
              <option value="DELETE">DELETE</option>
            </select>
          </label>
          <label>
            URL
            <input value={toolCfg(settingsNode).settings.apiUrl || ''} on:input={(e) => onSettingInput(settingsNode.id, 'apiUrl', e)} />
          </label>
          <label>
            Headers (JSON)
            <textarea rows="4" value={toolCfg(settingsNode).settings.apiHeaders || '{}'} on:input={(e) => onSettingInput(settingsNode.id, 'apiHeaders', e)}></textarea>
          </label>
          <label>
            Query (JSON)
            <textarea rows="4" value={toolCfg(settingsNode).settings.apiQuery || '{}'} on:input={(e) => onSettingInput(settingsNode.id, 'apiQuery', e)}></textarea>
          </label>
          <label>
            Body (JSON)
            <textarea rows="6" value={toolCfg(settingsNode).settings.apiBody || '{}'} on:input={(e) => onSettingInput(settingsNode.id, 'apiBody', e)}></textarea>
          </label>
        </div>
      {/if}
      {#if isParserToolNode(settingsNode)}
        <div class="node-modal-body node-modal-body-api">
          {#key `${settingsNode.id}:${toolCfg(settingsNode).settings.templateStoreId || 0}:${toolCfg(settingsNode).settings.templateId || ''}`}
            <ParserBuilderTab
              apiBase={API_BASE}
              apiJson={workflowApiJson}
              headers={workflowApiHeaders}
              existingTables={apiBuilderExistingTables}
              workflowDeskId={deskId}
              workflowNodeId={settingsNode.id}
              workflowGraph={captureDeskState()}
              initialSettings={toolCfg(settingsNode).settings || {}}
              incomingDescriptor={incomingDescriptorForNode(settingsNode)}
              outputDescriptor={buildNodeOutputDescriptor(settingsNode, {
                sourcePort: 'out',
                upstreamDescriptors: incomingDescriptorForNode(settingsNode)?.upstreamDescriptors || []
              })}
              lastRuntimeStep={runtimeSnapshotForNode(settingsNode)}
              embeddedMode={false}
              on:configChange={(event) => replaceToolSettings(settingsNode.id, event.detail?.settings || {})}
            />
          {/key}
        </div>
      {/if}
      {#if isActionPrepToolNode(settingsNode)}
        <div class="node-modal-body node-modal-body-api">
          {#key `${settingsNode.id}:${toolCfg(settingsNode).settings.sourceMode || 'node'}:${toolCfg(settingsNode).settings.sourceSchema || ''}:${toolCfg(settingsNode).settings.sourceTable || ''}`}
            <ActionPrepBuilderTab
              apiBase={API_BASE}
              apiJson={workflowApiJson}
              headers={workflowApiHeaders}
              existingTables={apiBuilderExistingTables}
              workflowDeskId={deskId}
              workflowNodeId={settingsNode.id}
              workflowGraph={captureDeskState()}
              initialSettings={toolCfg(settingsNode).settings || {}}
              incomingDescriptor={incomingDescriptorForNode(settingsNode)}
              outputDescriptor={buildNodeOutputDescriptor(settingsNode, {
                sourcePort: 'out',
                upstreamDescriptors: incomingDescriptorForNode(settingsNode)?.upstreamDescriptors || []
              })}
              lastRuntimeStep={runtimeSnapshotForNode(settingsNode)}
              embeddedMode={false}
              on:configChange={(event) => replaceToolSettings(settingsNode.id, event.detail?.settings || {})}
            />
          {/key}
        </div>
      {/if}
      {#if isTableNodeTool(settingsNode)}
        <div class="node-modal-body node-modal-body-api">
          {#key `${settingsNode.id}:${toolCfg(settingsNode).settings.baseSchema || ''}:${toolCfg(settingsNode).settings.baseTable || ''}`}
            <TableNodeBuilderTab
              apiBase={API_BASE}
              apiJson={workflowApiJson}
              headers={workflowApiHeaders}
              existingTables={apiBuilderExistingTables}
              initialSettings={toolCfg(settingsNode).settings || {}}
              incomingDescriptor={incomingDescriptorForNode(settingsNode)}
              outputDescriptor={buildNodeOutputDescriptor(settingsNode, {
                sourcePort: 'out',
                upstreamDescriptors: incomingDescriptorForNode(settingsNode)?.upstreamDescriptors || []
              })}
              embeddedMode={false}
              on:configChange={(event) => replaceToolSettings(settingsNode.id, event.detail?.settings || {})}
            />
          {/key}
        </div>
      {/if}
      {#if isApiMutationToolNode(settingsNode)}
        <div class="node-modal-body node-modal-body-api">
          {#key `${settingsNode.id}:${toolCfg(settingsNode).settings.endpointUrl || ''}:${toolCfg(settingsNode).settings.requestMode || 'row_per_request'}`}
            <ApiMutationBuilderTab
              apiBase={API_BASE}
              apiJson={workflowApiJson}
              headers={workflowApiHeaders}
              workflowDeskId={deskId}
              workflowNodeId={settingsNode.id}
              workflowGraph={captureDeskState()}
              initialSettings={toolCfg(settingsNode).settings || {}}
              incomingDescriptor={incomingDescriptorForNode(settingsNode)}
              outputDescriptor={buildNodeOutputDescriptor(settingsNode, {
                sourcePort: 'out',
                upstreamDescriptors: incomingDescriptorForNode(settingsNode)?.upstreamDescriptors || []
              })}
              lastRuntimeStep={runtimeSnapshotForNode(settingsNode)}
              embeddedMode={false}
              on:configChange={(event) => replaceToolSettings(settingsNode.id, event.detail?.settings || {})}
            />
          {/key}
        </div>
      {/if}
      {#if isWriteToolNode(settingsNode)}
        <div class="node-modal-body node-modal-body-api">
          {#key `${settingsNode.id}:${toolCfg(settingsNode).settings.templateStoreId || 0}:${toolCfg(settingsNode).settings.templateId || ''}`}
            <WriteBuilderTab
              apiBase={API_BASE}
              apiJson={workflowApiJson}
              headers={workflowApiHeaders}
              existingTables={apiBuilderExistingTables}
              workflowDeskId={deskId}
              workflowNodeId={settingsNode.id}
              workflowGraph={captureDeskState()}
              initialSettings={toolCfg(settingsNode).settings || {}}
              incomingDescriptor={incomingDescriptorForNode(settingsNode)}
              outputDescriptor={buildNodeOutputDescriptor(settingsNode, {
                sourcePort: 'out',
                upstreamDescriptors: incomingDescriptorForNode(settingsNode)?.upstreamDescriptors || []
              })}
              lastRuntimeStep={runtimeSnapshotForNode(settingsNode)}
              embeddedMode={false}
              on:configChange={(event) => replaceToolSettings(settingsNode.id, event.detail?.settings || {})}
            />
          {/key}
        </div>
      {/if}
      {#if settingsNode.type === 'tool' && toolCfg(settingsNode).toolType === 'split_data'}
        <div class="node-modal-body">
          <div class="help">Разделяет входной поток: либо дублирует строки, либо берет каждую N-ю.</div>
          <label>
            Режим
            <select value={toolCfg(settingsNode).settings.splitMode || 'duplicate'} on:change={(e) => updateSetting(settingsNode.id, 'splitMode', selectValue(e))}>
              <option value="duplicate">Дублировать строки</option>
              <option value="split">Отбирать каждую N-ю строку</option>
            </select>
          </label>
          <label>
            Коэффициент
            <input type="number" min="1" value={toolCfg(settingsNode).settings.splitMultiplier || '2'} on:input={(e) => onSettingInput(settingsNode.id, 'splitMultiplier', e)} />
          </label>
          <label>
            Имя технического поля (опционально)
            <input value={toolCfg(settingsNode).settings.splitKeyField || ''} on:input={(e) => onSettingInput(settingsNode.id, 'splitKeyField', e)} />
          </label>
          <label>
            Префикс значения поля
            <input value={toolCfg(settingsNode).settings.splitPrefix || ''} on:input={(e) => onSettingInput(settingsNode.id, 'splitPrefix', e)} />
          </label>
        </div>
      {/if}
      {#if settingsNode.type === 'tool' && toolCfg(settingsNode).toolType === 'merge_data'}
        <div class="node-modal-body">
          <div class="help">Объединяет поток и может удалять дубликаты по выбранным полям.</div>
          <label>
            Режим
            <select value={toolCfg(settingsNode).settings.mergeMode || 'dedupe'} on:change={(e) => updateSetting(settingsNode.id, 'mergeMode', selectValue(e))}>
              <option value="dedupe">Дедупликация</option>
              <option value="passthrough">Без изменений</option>
            </select>
          </label>
          <label>
            Поля для дедупликации (через запятую)
            <input value={toolCfg(settingsNode).settings.dedupeBy || ''} on:input={(e) => onSettingInput(settingsNode.id, 'dedupeBy', e)} />
          </label>
          <label>
            Какой дубль оставлять
            <select value={toolCfg(settingsNode).settings.mergeKeep || 'first'} on:change={(e) => updateSetting(settingsNode.id, 'mergeKeep', selectValue(e))}>
              <option value="first">Первый</option>
              <option value="last">Последний</option>
            </select>
          </label>
          <label>
            Данные по умолчанию, если поток пуст (JSON array)
            <textarea rows="4" value={toolCfg(settingsNode).settings.mergeEmptySource || '[]'} on:input={(e) => onSettingInput(settingsNode.id, 'mergeEmptySource', e)}></textarea>
          </label>
        </div>
      {/if}
      {#if settingsNode.type === 'tool' && toolCfg(settingsNode).toolType === 'condition_if'}
        <div class="node-modal-body">
          <div class="help">Условная развилка в порт `true` или `false` по первой строке входного потока.</div>
          <label>
            Поле (path)
            <input value={toolCfg(settingsNode).settings.conditionField || ''} on:input={(e) => onSettingInput(settingsNode.id, 'conditionField', e)} placeholder="Например: metrics.drr" />
          </label>
          <label>
            Оператор
            <select value={toolCfg(settingsNode).settings.conditionOperator || 'equals'} on:change={(e) => updateSetting(settingsNode.id, 'conditionOperator', selectValue(e))}>
              <option value="equals">Равно</option>
              <option value="not_equals">Не равно</option>
              <option value="gt">Больше</option>
              <option value="gte">Больше или равно</option>
              <option value="lt">Меньше</option>
              <option value="lte">Меньше или равно</option>
              <option value="contains">Содержит</option>
              <option value="not_contains">Не содержит</option>
              <option value="is_empty">Пусто</option>
              <option value="not_empty">Не пусто</option>
            </select>
          </label>
          <label>
            Значение для сравнения
            <input value={toolCfg(settingsNode).settings.conditionValue || ''} on:input={(e) => onSettingInput(settingsNode.id, 'conditionValue', e)} />
          </label>
        </div>
      {/if}
      {#if settingsNode.type === 'tool' && toolCfg(settingsNode).toolType === 'condition_switch'}
        <div class="node-modal-body">
          <div class="help">Ветвление по значению поля: `case_1..case_4` или `default`.</div>
          <label>
            Поле (path)
            <input value={toolCfg(settingsNode).settings.switchField || ''} on:input={(e) => onSettingInput(settingsNode.id, 'switchField', e)} placeholder="Например: status" />
          </label>
          <div class="advanced-grid">
            <label>Case 1<input value={toolCfg(settingsNode).settings.switchCase1Value || ''} on:input={(e) => onSettingInput(settingsNode.id, 'switchCase1Value', e)} /></label>
            <label>Case 2<input value={toolCfg(settingsNode).settings.switchCase2Value || ''} on:input={(e) => onSettingInput(settingsNode.id, 'switchCase2Value', e)} /></label>
            <label>Case 3<input value={toolCfg(settingsNode).settings.switchCase3Value || ''} on:input={(e) => onSettingInput(settingsNode.id, 'switchCase3Value', e)} /></label>
            <label>Case 4<input value={toolCfg(settingsNode).settings.switchCase4Value || ''} on:input={(e) => onSettingInput(settingsNode.id, 'switchCase4Value', e)} /></label>
          </div>
          <label>
            Порт по умолчанию
            <select value={toolCfg(settingsNode).settings.switchDefaultPort || 'default'} on:change={(e) => updateSetting(settingsNode.id, 'switchDefaultPort', selectValue(e))}>
              <option value="default">default</option>
            </select>
          </label>
        </div>
      {/if}
      {#if settingsNode.type === 'tool' && toolCfg(settingsNode).toolType === 'code_node'}
        <div class="node-modal-body">
          <div class="help">JS-код выполняется в sandbox. Доступны переменные: `input`, `rows`, `context`, `meta`.</div>
          <label>
            Таймаут, мс
            <input type="number" min="100" value={toolCfg(settingsNode).settings.codeTimeoutMs || '5000'} on:input={(e) => onSettingInput(settingsNode.id, 'codeTimeoutMs', e)} />
          </label>
          <label>
            Код
            <textarea rows="14" value={toolCfg(settingsNode).settings.scriptCode || 'return input;'} on:input={(e) => onSettingInput(settingsNode.id, 'scriptCode', e)}></textarea>
          </label>
        </div>
      {/if}
      {#if isWideSettingsNode(settingsNode) && !nodeModalFullscreen}
        <button type="button" class="modal-resize-handle modal-resize-n" aria-label="Изменить высоту сверху" on:mousedown={(e) => startNodeModalResize(e, 'n')}></button>
        <button type="button" class="modal-resize-handle modal-resize-s" aria-label="Изменить высоту снизу" on:mousedown={(e) => startNodeModalResize(e, 's')}></button>
        <button type="button" class="modal-resize-handle modal-resize-e" aria-label="Изменить ширину справа" on:mousedown={(e) => startNodeModalResize(e, 'e')}></button>
        <button type="button" class="modal-resize-handle modal-resize-w" aria-label="Изменить ширину слева" on:mousedown={(e) => startNodeModalResize(e, 'w')}></button>
        <button type="button" class="modal-resize-handle modal-resize-ne" aria-label="Изменить размер справа сверху" on:mousedown={(e) => startNodeModalResize(e, 'ne')}></button>
        <button type="button" class="modal-resize-handle modal-resize-nw" aria-label="Изменить размер слева сверху" on:mousedown={(e) => startNodeModalResize(e, 'nw')}></button>
        <button type="button" class="modal-resize-handle modal-resize-se" aria-label="Изменить размер справа снизу" on:mousedown={(e) => startNodeModalResize(e, 'se')}></button>
        <button type="button" class="modal-resize-handle modal-resize-sw" aria-label="Изменить размер слева снизу" on:mousedown={(e) => startNodeModalResize(e, 'sw')}></button>
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
  .desk-picker {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    font-size: 12px;
    color: #334155;
  }
  .desk-picker span {
    color: #475569;
  }
  .desk-picker select {
    min-width: 240px;
    max-width: 360px;
    border: 1px solid #dbe4f0;
    border-radius: 10px;
    background: #fff;
    padding: 6px 10px;
    color: #0f172a;
  }
  .mini.danger {
    border-color: #fecaca;
    color: #b91c1c;
    background: #fff;
  }
  .mini.danger:disabled {
    color: #94a3b8;
    border-color: #e2e8f0;
  }
  .dirty-flag { color: #b45309; font-weight: 600; }
  .clean-flag { color: #166534; font-weight: 600; }
  .desk-run-state {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 5px 10px;
    border-radius: 999px;
    border: 1px solid #dbe4f0;
    background: #f8fafc;
    font-weight: 600;
    white-space: nowrap;
  }
  .desk-run-state-active {
    color: #166534;
    border-color: #bbf7d0;
    background: #f0fdf4;
  }
  .desk-run-state-idle,
  .desk-run-state-blocked {
    color: #92400e;
    border-color: #fde68a;
    background: #fffbeb;
  }
  .desk-run-state-manual {
    color: #1d4ed8;
    border-color: #bfdbfe;
    background: #eff6ff;
  }
  .desk-run-state-queued {
    color: #7c3aed;
    border-color: #ddd6fe;
    background: #f5f3ff;
  }
  .desk-run-state-success {
    color: #166534;
    border-color: #bbf7d0;
    background: #f0fdf4;
  }
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
  .ops-pane {
    min-width: 0;
    display: flex;
  }
  .ops-bar-side {
    width: 100%;
    max-height: 100%;
    overflow: auto;
  }
  .ops-grid {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 10px;
  }
  .ops-grid-side {
    grid-template-columns: 1fr;
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
  .ops-card-details {
    min-height: 0;
  }
  .ops-card-details summary {
    cursor: pointer;
    list-style: none;
    font-size: 13px;
    font-weight: 700;
    color: #0f172a;
    display: flex;
    align-items: center;
    justify-content: space-between;
  }
  .ops-card-details summary::-webkit-details-marker {
    display: none;
  }
  .ops-card-details summary::after {
    content: '▾';
    color: #64748b;
    font-size: 12px;
    transform: rotate(-90deg);
    transition: transform 0.18s ease;
  }
  .ops-card-details[open] summary::after {
    transform: rotate(0deg);
  }
  .ops-card-body {
    display: flex;
    flex-direction: column;
    gap: 8px;
    padding-top: 6px;
  }
  .process-status-board {
    gap: 10px;
  }
  .process-board-head {
    display: flex;
    justify-content: space-between;
    gap: 10px;
    align-items: flex-start;
    flex-wrap: wrap;
  }
  .process-board-chips {
    display: flex;
    flex-wrap: wrap;
    justify-content: flex-end;
    gap: 6px;
  }
  .desk-status-chip {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 5px 10px;
    border-radius: 999px;
    border: 1px solid #dbe4f0;
    background: #fff;
    font-size: 11px;
    line-height: 1.2;
    color: #334155;
    white-space: nowrap;
  }
  .desk-status-chip.ok {
    border-color: #bbf7d0;
    background: #f0fdf4;
    color: #166534;
  }
  .desk-status-chip.warn {
    border-color: #fde68a;
    background: #fffbeb;
    color: #92400e;
  }
  .desk-status-chip-active,
  .process-state-pill.tone-running {
    border-color: #bfdbfe;
    background: #eff6ff;
    color: #1d4ed8;
  }
  .desk-status-chip-blocked,
  .process-state-pill.tone-failed,
  .process-state-pill.tone-attention {
    border-color: #fecaca;
    background: #fef2f2;
    color: #b91c1c;
  }
  .desk-status-chip-manual,
  .process-state-pill.tone-pending {
    border-color: #fde68a;
    background: #fffbeb;
    color: #92400e;
  }
  .desk-status-chip-queued,
  .process-state-pill.tone-queued {
    border-color: #ddd6fe;
    background: #f5f3ff;
    color: #7c3aed;
  }
  .desk-status-chip-success,
  .process-state-pill.tone-success {
    border-color: #bbf7d0;
    background: #f0fdf4;
    color: #166534;
  }
  .desk-status-chip-idle,
  .desk-status-chip-draft,
  .process-state-pill.tone-disabled,
  .process-state-pill.tone-draft,
  .process-state-pill.tone-neutral {
    border-color: #dbe4f0;
    background: #fff;
    color: #475569;
  }
  .process-status-list {
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  .process-status-card {
    border: 1px solid #dbe4f0;
    border-radius: 12px;
    background: #fff;
    padding: 10px;
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  .process-status-card.tone-running {
    border-color: #bfdbfe;
    box-shadow: inset 0 0 0 1px rgba(59, 130, 246, 0.12);
  }
  .process-status-card.tone-failed,
  .process-status-card.tone-attention {
    border-color: #fecaca;
  }
  .process-status-card.tone-success {
    border-color: #bbf7d0;
  }
  .process-status-card-head {
    display: flex;
    gap: 10px;
    justify-content: space-between;
    align-items: flex-start;
    flex-wrap: wrap;
  }
  .process-status-main {
    display: flex;
    flex-direction: column;
    gap: 4px;
    min-width: 0;
    flex: 1;
  }
  .process-status-meta {
    display: flex;
    flex-wrap: wrap;
    gap: 6px 10px;
    font-size: 11px;
    color: #475569;
  }
  .process-status-actions {
    display: flex;
    align-items: center;
    gap: 6px;
    flex-wrap: wrap;
    justify-content: flex-end;
  }
  .process-status-strip {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    align-items: center;
  }
  .process-state-pill {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 5px 10px;
    border-radius: 999px;
    border: 1px solid #dbe4f0;
    font-size: 11px;
    line-height: 1.2;
    font-weight: 700;
  }
  .process-status-message {
    font-size: 12px;
    color: #334155;
    line-height: 1.45;
  }
  .process-status-grid {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 8px;
  }
  .process-status-kv {
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    background: #f8fafc;
    padding: 8px;
    display: flex;
    flex-direction: column;
    gap: 4px;
    min-width: 0;
  }
  .process-status-kv span {
    font-size: 11px;
    color: #64748b;
  }
  .process-status-kv strong {
    font-size: 12px;
    color: #0f172a;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .ops-note {
    font-size: 11px;
    color: #475569;
    line-height: 1.4;
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
    flex-wrap: wrap;
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
  .ops-grid-side .ops-columns {
    grid-template-columns: 1fr;
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
  .runtime-run-picker {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }
  .runtime-run-picker select {
    width: 100%;
    min-width: 0;
  }
  .runtime-step-strip {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
  }
  .runtime-step-pill {
    border: 1px solid #cbd5e1;
    border-radius: 999px;
    background: #fff;
    color: #334155;
  }
  .runtime-step-pill.active {
    border-color: #2563eb;
    background: #dbeafe;
    color: #1d4ed8;
  }
  .runtime-step-card {
    border: 1px solid #dbeafe;
    border-radius: 10px;
    background: #eff6ff;
    padding: 8px;
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  .runtime-io-grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 8px;
  }
  .runtime-io-card {
    border: 1px solid #bfdbfe;
    border-radius: 8px;
    background: rgba(255, 255, 255, 0.9);
    padding: 6px;
  }
  .runtime-io-card summary {
    cursor: pointer;
    font-size: 12px;
    font-weight: 600;
    color: #0f172a;
  }
  .runtime-io-card pre {
    margin: 8px 0 0;
    max-height: 220px;
    overflow: auto;
    font-size: 11px;
    line-height: 1.4;
    color: #0f172a;
    background: #f8fafc;
    border-radius: 6px;
    padding: 8px;
  }
  .compact-preview-wrap {
    max-height: 180px;
  }
  .source-pill {
    border: 1px solid #cbd5e1;
    border-radius: 8px;
    background: #fff;
    color: #0f172a;
    padding: 6px 8px;
    font-size: 11px;
    text-align: left;
    cursor: pointer;
  }
  .source-pill:disabled {
    opacity: 0.55;
    cursor: not-allowed;
  }
  .storage-picker {
    display: flex;
    gap: 8px;
    align-items: center;
    flex-wrap: wrap;
  }
  .storage-picker select {
    flex: 1;
    min-width: 0;
  }
  .system-source-list {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  .system-source-row {
    display: flex;
    flex-direction: column;
    gap: 2px;
    border: 1px solid #e2e8f0;
    border-radius: 8px;
    background: #fff;
    padding: 6px 8px;
  }
  .system-source-row span {
    font-size: 11px;
    color: #475569;
  }
  .system-source-row code {
    font-size: 11px;
    color: #0f172a;
    word-break: break-all;
  }
  .topbar { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
  .topbar button { border: 1px solid #dbe4f0; background: #fff; border-radius: 9px; padding: 6px 10px; cursor: pointer; }
  .topbar .primary { background: #0f172a; color: #fff; border-color: #0f172a; }
  .summary { display: flex; flex-wrap: wrap; gap: 8px; font-size: 12px; color: #334155; }
  .workspace { display: grid; grid-template-columns: 380px minmax(0, 1fr) 360px; gap: 10px; min-height: 720px; }
  .tools { border: 1px solid #e2e8f0; border-radius: 12px; padding: 10px; background: #f8fbff; display: flex; flex-direction: column; gap: 8px; overflow: auto; }
  .mini { border: 1px solid #dbe4f0; background: #fff; border-radius: 8px; padding: 4px 8px; font-size: 11px; cursor: pointer; }
  .mini.primary { background: #0f172a; color: #fff; border-color: #0f172a; }
  .source-error { border: 1px solid #fecaca; background: #fef2f2; color: #991b1b; border-radius: 8px; padding: 6px 8px; font-size: 11px; }
  .tool-group { border: 1px solid #e2e8f0; border-radius: 10px; background: #fff; padding: 8px; display: flex; flex-direction: column; gap: 6px; }
  .tool-group h5 { margin: 0; font-size: 12px; color: #0f172a; }
  .items { display: flex; flex-direction: column; gap: 6px; }
  .item { text-align: left; border: 1px solid #dbe4f0; background: #fff; border-radius: 8px; padding: 7px; cursor: grab; display: flex; flex-direction: column; gap: 2px; }
  .item span { font-size: 11px; color: #64748b; }
  .item.tool { background: #fff7ed; border-color: #fed7aa; }
  .canvas-wrap { position: relative; border: 1px solid #dbe4f0; border-radius: 12px; overflow: hidden; background: #fff; }
  .canvas { position: relative; width: 3000px; height: 2000px; transform-origin: 0 0; background-image: linear-gradient(#eef2f7 1px, transparent 1px), linear-gradient(90deg, #eef2f7 1px, transparent 1px); background-size: 24px 24px; }
  .edges { position: absolute; inset: 0; pointer-events: none; }
  .edge-label { fill: #334155; font-size: 11px; }
  .node { position: absolute; width: 250px; border: 1px solid #cbd5e1; border-radius: 10px; background: #fff; padding: 8px; box-shadow: 0 4px 12px rgba(15,23,42,.07); }
  .node.node-api { width: 320px; }
  .node.tool { background: #fff7ed; }
  .node.data { background: #f8fafc; }
  .node.selected { border-color: #2563eb; }
  .node-head { display: flex; justify-content: space-between; gap: 8px; align-items: flex-start; }
  .node-process-indicator {
    margin-top: 6px;
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 4px 8px;
    border-radius: 999px;
    border: 1px solid #dbe4f0;
    background: #fff;
    font-size: 10px;
    line-height: 1.2;
    font-weight: 700;
    color: #475569;
    max-width: 100%;
  }
  .node-process-indicator.tone-running {
    border-color: #bfdbfe;
    background: #eff6ff;
    color: #1d4ed8;
  }
  .node-process-indicator.tone-queued {
    border-color: #ddd6fe;
    background: #f5f3ff;
    color: #7c3aed;
  }
  .node-process-indicator.tone-success {
    border-color: #bbf7d0;
    background: #f0fdf4;
    color: #166534;
  }
  .node-process-indicator.tone-failed {
    border-color: #fecaca;
    background: #fef2f2;
    color: #b91c1c;
  }
  .node-process-indicator.tone-pending {
    border-color: #fde68a;
    background: #fffbeb;
    color: #92400e;
  }
  .node-process-indicator.tone-disabled,
  .node-process-indicator.tone-draft {
    border-color: #dbe4f0;
    background: #f8fafc;
    color: #475569;
  }
  .node-title {
    display: block;
    flex: 1;
    min-width: 0;
    font-size: 13px;
    line-height: 1.3;
    color: #0f172a;
  }
  .node-title-api {
    font-size: 12px;
    font-weight: 700;
  }
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
  .api-meta {
    color: #0f172a;
    font-size: 10px;
    margin-top: 4px;
    line-height: 1.35;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .api-stats {
    margin-top: 6px;
    display: flex;
    flex-wrap: wrap;
    gap: 4px 10px;
    font-size: 10px;
    color: #334155;
    align-items: center;
  }
  .api-stats span {
    white-space: nowrap;
    line-height: 1.25;
  }
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
  .props select { border: 1px solid #dbe4f0; border-radius: 7px; padding: 6px; }
  .table-filter-tools { display: flex; gap: 6px; align-items: center; }
  .table-filter-row {
    display: grid;
    grid-template-columns: minmax(0, 1fr) minmax(0, 1fr) minmax(0, 1.2fr) auto;
    gap: 6px;
    align-items: center;
  }
  .table-filter-row input,
  .table-filter-row select {
    width: 100%;
    box-sizing: border-box;
  }
  .edge-row { border: 1px solid #e2e8f0; border-radius: 7px; background: #fff; padding: 6px; display: flex; justify-content: space-between; gap: 8px; font-size: 11px; }
  .edge-row button { border: 1px solid #dbe4f0; border-radius: 6px; background: #fff; cursor: pointer; }
  .issue { border-radius: 7px; border: 1px solid #e2e8f0; background: #fff; padding: 6px; font-size: 12px; }
  .issue.error { border-color: #fecaca; background: #fef2f2; }
  .issue.warn { border-color: #fde68a; background: #fffbeb; }
  .issue.info { border-color: #bfdbfe; background: #eff6ff; }
  .empty { color: #64748b; font-size: 12px; }
  .node-modal-backdrop { position: fixed; inset: 0; background: rgba(15, 23, 42, 0.48); z-index: 40; }
  .node-modal {
    position: fixed;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    width: min(760px, calc(100vw - 32px));
    max-height: calc(100vh - 48px);
    display: flex;
    flex-direction: column;
    overflow: hidden;
    background: #fff;
    border: 1px solid #dbe4f0;
    border-radius: 14px;
    box-shadow: 0 18px 48px rgba(15, 23, 42, 0.28);
    z-index: 41;
  }
  .node-modal.node-modal-wide { width: min(1600px, calc(100vw - 24px)); max-height: calc(100vh - 24px); }
  .node-modal.node-modal-fullscreen {
    top: 8px;
    left: 8px;
    transform: none;
    width: calc(100vw - 16px);
    height: calc(100vh - 16px);
    max-height: none;
    border-radius: 12px;
  }
  .node-modal.node-modal-resizable {
    min-width: 760px;
    min-height: 420px;
  }
  .node-modal.node-modal-manual {
    margin: 0;
  }
  .node-modal-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; padding: 12px 14px; border-bottom: 1px solid #e2e8f0; position: sticky; top: 0; background: #fff; }
  .node-modal-head h4 { margin: 0; font-size: 16px; }
  .node-modal-actions { display: flex; align-items: center; gap: 6px; justify-content: flex-end; }
  .window-btn {
    width: 30px;
    height: 30px;
    border: 1px solid #dbe4f0;
    border-radius: 8px;
    background: #fff;
    color: #334155;
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    padding: 0;
  }
  .window-btn svg {
    width: 14px;
    height: 14px;
    fill: currentColor;
  }
  .window-btn:hover { background: #f8fafc; }
  .window-btn-close:hover {
    background: #fff1f2;
    border-color: #fecdd3;
    color: #be123c;
  }
  .modal-resize-handle {
    position: absolute;
    border: 0;
    background: transparent;
    padding: 0;
    margin: 0;
    z-index: 20;
    touch-action: none;
    pointer-events: auto;
  }
  .modal-resize-n { top: 0; left: 10px; right: 10px; height: 12px; cursor: ns-resize; }
  .modal-resize-s { bottom: 0; left: 10px; right: 10px; height: 12px; cursor: ns-resize; }
  .modal-resize-e { right: 0; top: 10px; bottom: 10px; width: 12px; cursor: ew-resize; }
  .modal-resize-w { left: 0; top: 10px; bottom: 10px; width: 12px; cursor: ew-resize; }
  .modal-resize-ne { top: 0; right: 0; width: 16px; height: 16px; cursor: nesw-resize; }
  .modal-resize-nw { top: 0; left: 0; width: 16px; height: 16px; cursor: nwse-resize; }
  .modal-resize-se { bottom: 0; right: 0; width: 16px; height: 16px; cursor: nwse-resize; }
  .modal-resize-sw { bottom: 0; left: 0; width: 16px; height: 16px; cursor: nesw-resize; }
  .node-modal-body {
    padding: 12px 14px;
    display: flex;
    flex-direction: column;
    gap: 10px;
    min-height: 0;
    overflow: auto;
  }
  .node-modal-body label { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: #334155; }
  .node-modal-body input,
  .node-modal-body select,
  .node-modal-body textarea { border: 1px solid #dbe4f0; border-radius: 8px; padding: 7px 8px; font-family: inherit; font-size: 13px; }
  .node-modal-body textarea { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
  .node-modal-body .table-filter-row {
    grid-template-columns: minmax(0, 1fr) minmax(0, 1fr) minmax(0, 1.6fr) auto;
  }
  .node-modal-body-api { padding: 8px; background: #fff; min-height: 0; overflow: auto; }
  .api-template-current-block {
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    background: #f8fafc;
    padding: 10px;
    margin: 0 0 8px;
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  .api-template-line {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    flex-wrap: wrap;
  }
  .api-template-line-main {
    align-items: baseline;
  }
  .api-template-key {
    font-size: 12px;
    color: #475569;
  }
  .api-template-value {
    font-size: 14px;
    font-weight: 600;
    color: #0f172a;
  }
  .api-template-value-muted {
    font-size: 13px;
    font-weight: 500;
    color: #1e293b;
  }
  .api-template-usage-text {
    font-size: 12px;
    color: #334155;
  }
  .api-template-usage-actions {
    display: flex;
    align-items: center;
    gap: 6px;
  }
  .api-template-usage-expanded {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 8px;
  }
  .api-template-usage-col {
    border: 1px solid #e6eaf2;
    border-radius: 8px;
    background: #fff;
    padding: 8px;
    min-width: 0;
  }
  .api-template-usage-title {
    font-size: 12px;
    font-weight: 600;
    color: #1e293b;
    margin-bottom: 4px;
  }
  .api-template-usage-col ul {
    margin: 0;
    padding-left: 16px;
    display: flex;
    flex-direction: column;
    gap: 3px;
    font-size: 12px;
    color: #334155;
  }
  .api-template-empty {
    font-size: 12px;
    color: #64748b;
  }
  .api-template-process-code {
    color: #64748b;
    font-size: 11px;
  }
  .api-template-line-switch .primary {
    min-width: 132px;
  }
  .api-template-switch-hint {
    font-size: 12px;
    color: #475569;
    min-width: 0;
  }
  .interval-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
  .help { font-size: 12px; color: #475569; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 8px; }
  .hint { font-size: 11px; color: #64748b; line-height: 1.35; }
  .code-ok { font-size: 11px; color: #166534; }
  .code-error { font-size: 11px; color: #b91c1c; font-weight: 600; }
  .advanced-box {
    border: 1px solid #dbe4f0;
    border-radius: 10px;
    background: #f8fafc;
    padding: 8px;
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  .advanced-box summary {
    cursor: pointer;
    font-size: 12px;
    font-weight: 600;
    color: #1e293b;
    outline: none;
  }
  .advanced-grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 10px;
  }
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
  @media (max-width: 1100px) {
    .api-template-usage-expanded {
      grid-template-columns: 1fr;
    }
  }
  @media (max-width: 1440px) {
    .desk-actions {
      grid-template-columns: repeat(3, minmax(0, 1fr));
    }
    .workspace {
      grid-template-columns: 340px minmax(0, 1fr) 340px;
    }
    .ops-grid:not(.ops-grid-side) {
      grid-template-columns: 1fr;
    }
    .process-status-grid {
      grid-template-columns: repeat(2, minmax(0, 1fr));
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
    .runtime-io-grid {
      grid-template-columns: 1fr;
    }
    .process-board-head,
    .process-status-card-head {
      flex-direction: column;
    }
    .process-board-chips,
    .process-status-actions {
      justify-content: flex-start;
    }
    .process-status-grid {
      grid-template-columns: 1fr;
    }
  }
</style>




