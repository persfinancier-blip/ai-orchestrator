<script lang="ts">
  import { createEventDispatcher, onMount } from 'svelte';
  import ComputedExpressionBuilder from '../components/ComputedExpressionBuilder.svelte';
  import {
    COMPUTED_FUNCTION_CATEGORIES,
    COMPUTED_FUNCTIONS,
    buildComputedFieldPreview,
    createComputedFieldRule,
    duplicateComputedFieldRule,
    normalizeComputedFieldRule,
    serializeComputedFieldRule
  } from './computedFieldBuilderCore.js';
  import { buildSourceNodePreviewFromTemplate } from './parserSourcePreviewCore.js';

  type ExistingTable = { schema_name: string; table_name: string };
  type ColumnMeta = { name: string; type?: string };
  type ParserSettings = Record<string, string>;
  type ParserDraft = {
    id: number;
    name: string;
    parser_name: string;
    description: string;
    revision: number;
    updated_at?: string | null;
    updated_by?: string;
    config_json?: Record<string, any>;
  };
  type ParserPreview = {
    row_count: number;
    column_count: number;
    columns: string[];
    sample_rows: Array<Record<string, any>>;
    raw_row_count: number;
    raw_column_count: number;
    raw_columns: string[];
    raw_sample_rows: Array<Record<string, any>>;
    source_type: string;
    source_ref: string;
    source_format: string;
    batch: Record<string, any>;
    stats: Record<string, any>;
  };
  type SourceNodeTemplate = {
    ref: string;
    templateType: 'api_request' | 'table_parser';
    storeId: number;
    name: string;
    description: string;
    config_json?: Record<string, any>;
    output_parameters?: Array<Record<string, any>>;
    picked_paths?: string[];
  };
  type ParserIncomingContractField = {
    name: string;
    alias?: string;
    type?: string;
    path?: string;
  };
  type ParserIncomingSampleMeta = {
    source: 'node_execution';
    status?: number;
    responsesCount?: number;
    payloadPath?: string;
    startedAt?: string;
  };
  type ParserDerivedOutputField = {
    name: string;
    alias?: string;
    type?: string;
    path?: string;
    source: 'preview' | 'settings';
  };
  type ParserIncomingDescriptor = {
    nodeId?: string;
    upstreamNodes?: Array<{
      nodeId: string;
      nodeName: string;
      nodeType: string;
      fromPort: string;
      contractFields?: ParserIncomingContractField[];
      sampleRaw?: any;
      samplePayload?: any;
      sampleRows?: Array<Record<string, any>>;
      sampleMeta?: ParserIncomingSampleMeta;
    }>;
  };
  type ParserPipelineInputKind =
    | 'structured payload'
    | 'text/string payload'
    | 'tabular text'
    | 'archive/container'
    | 'link/reference/locator'
    | 'unknown';
  type ParserPipelineSourceBasis = 'contract-only' | 'sample-based' | 'preview-derived' | 'mixed' | 'insufficient-data';
  type ParserPipelineStepMode = 'auto' | 'manual' | 'partial' | 'unknown';
  type ParserPipelineStrategy =
    | 'direct read'
    | 'unwrap envelope'
    | 'parse JSON string'
    | 'parse CSV/TSV'
    | 'extract archive entry'
    | 'dereference link/reference'
    | 'path-based extraction'
    | 'multi-step extraction'
    | 'unknown';
  type ParserPipelineViewModel = {
    inputProfile: {
      upstreamSummary: string;
      upstreamCount: number;
      contractFieldsCount: number;
      hasSampleRaw: boolean;
      hasSamplePayload: boolean;
      hasSampleRows: boolean;
      inputKind: ParserPipelineInputKind;
      sourceBasis: ParserPipelineSourceBasis;
    };
    parseStrategy: {
      detectedFormat: string;
      strategy: ParserPipelineStrategy;
      manualOverride: boolean;
    };
    payloadCandidate: {
      candidateSource: string;
      candidatePath: string;
      mode: ParserPipelineStepMode;
      hasPayloadSample: boolean;
    };
    workingSetCandidate: {
      candidateRecordPath: string;
      rowsDetected: number | null;
      sampleRowsAvailable: number;
      mode: ParserPipelineStepMode;
      hasTabularSet: boolean;
    };
    resultSummary: {
      fieldsCount: number;
      source: 'preview' | 'settings' | 'mixed' | 'insufficient-data';
      hasDerivedOutputPreview: boolean;
    };
    warningsSummary: {
      ambiguity: boolean;
      missingSample: boolean;
      legacyFallbackInUse: boolean;
      insufficientData: boolean;
      parserWarnings: string[];
      total: number;
    };
    autoManualState: {
      input: ParserPipelineStepMode;
      strategy: ParserPipelineStepMode;
      payload: ParserPipelineStepMode;
      workingSet: ParserPipelineStepMode;
      result: ParserPipelineStepMode;
    };
  };
  type LookupJoinRule = {
    sourceField: string;
    targetField: string;
  };
  type ParserEditorStep = 'input' | 'parse' | 'payload' | 'workingSet' | 'fields' | 'processing' | 'result';

  export let apiBase: string;
  export let apiJson: <T = any>(url: string, init?: RequestInit) => Promise<T>;
  export let headers: () => Record<string, string>;
  export let existingTables: ExistingTable[] = [];
  export let initialSettings: Record<string, any> = {};
  export let embeddedMode = false;
  export let incomingDescriptor: ParserIncomingDescriptor | null = null;

  const dispatch = createEventDispatcher<{
    configChange: { settings: ParserSettings };
  }>();

  const DEFAULT_SETTINGS: ParserSettings = {
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
    filterRulesJson: '[]',
    computedFieldsJson: '[]',
    parserMultiplier: '1',
    sampleInput: '',
    lookupEnabled: 'false',
    lookupSchema: '',
    lookupTable: '',
    lookupSourceField: '',
    lookupTargetField: '',
    lookupFields: '',
    lookupPrefix: '',
    lookupJoinMode: 'left',
    lookupConflictMode: 'suffix',
    lookupSelectedFieldsJson: '[]',
    lookupJoinRulesJson: '[]',
    dedupeEnabled: 'false',
    dedupeMode: 'full_row',
    dedupeFields: '',
    dedupeKeep: 'first',
    groupEnabled: 'false',
    groupByFields: '',
    aggregateRulesJson: '[]'
  };
  const COMPUTED_EXPRESSION_PLACEHOLDER = 'если({price} > 0, {price} * 0.9, 0)';
  const INCOMING_NODE_TYPE_LABELS: Record<string, string> = {
    api_request: 'API-запрос',
    http_request: 'HTTP-запрос',
    table_node: 'Табличный набор',
    table_parser: 'Парсер данных',
    db_write: 'Запись в БД',
    split_data: 'Разделение данных',
    merge_data: 'Объединение данных',
    condition_if: 'Условие',
    condition_switch: 'Переключатель',
    code_node: 'Код',
    start_process: 'Старт процесса'
  };
  const PARSER_EDITOR_STEPS: Array<{ id: ParserEditorStep; label: string; order: string }> = [
    { id: 'input', label: 'Вход', order: '1' },
    { id: 'parse', label: 'Разбор', order: '2' },
    { id: 'payload', label: 'Payload', order: '3' },
    { id: 'workingSet', label: 'Набор', order: '4' },
    { id: 'fields', label: 'Поля', order: '5' },
    { id: 'processing', label: 'Обработка', order: '6' },
    { id: 'result', label: 'Результат', order: '7' }
  ];

  function normalizeSettings(raw: Record<string, any> | null | undefined): ParserSettings {
    const next: ParserSettings = { ...DEFAULT_SETTINGS };
    const src = raw && typeof raw === 'object' ? raw : {};
    for (const key of Object.keys(DEFAULT_SETTINGS)) {
      const value = src[key];
      if (value === undefined || value === null) continue;
      next[key] = typeof value === 'string' ? value : JSON.stringify(value);
    }
    const rawSourceMode = String(src.sourceMode || src.source_mode || next.sourceMode || 'node').trim().toLowerCase();
    next.sourceMode = rawSourceMode === 'table' || rawSourceMode === 'file_url' ? rawSourceMode : 'node';
    next.sourceNodeTemplateRef = String(
      src.sourceNodeTemplateRef || src.source_node_template_ref || next.sourceNodeTemplateRef || ''
    ).trim();
    next.sourceNodeTemplateType = String(
      src.sourceNodeTemplateType || src.source_node_template_type || next.sourceNodeTemplateType || ''
    ).trim();
    next.sourceNodeTemplateStoreId = String(
      src.sourceNodeTemplateStoreId || src.source_node_template_store_id || next.sourceNodeTemplateStoreId || ''
    ).trim();
    next.sourceNodeTemplateName = String(
      src.sourceNodeTemplateName || src.source_node_template_name || next.sourceNodeTemplateName || ''
    ).trim();
    next.sourceFormat = String(src.sourceFormat || src.source_format || next.sourceFormat || 'auto').trim() || 'auto';
    next.sourceSchema = String(src.sourceSchema || src.source_schema || next.sourceSchema || '').trim();
    next.sourceTable = String(src.sourceTable || src.source_table || next.sourceTable || '').trim();
    next.sourceColumn = String(src.sourceColumn || src.source_column || next.sourceColumn || '').trim();
    next.inputPath = String(src.inputPath || src.input_path || next.inputPath || '').trim();
    next.recordPath = String(src.recordPath || src.record_path || next.recordPath || '').trim();
    next.fileUrl = String(src.fileUrl || src.file_url || next.fileUrl || '').trim();
    next.fileUrlPath = String(src.fileUrlPath || src.file_url_path || next.fileUrlPath || '').trim();
    next.archiveEntry = String(src.archiveEntry || src.archive_entry || next.archiveEntry || '').trim();
    next.archiveFormat = String(src.archiveFormat || src.archive_format || next.archiveFormat || 'auto').trim() || 'auto';
    next.csvDelimiter = String(src.csvDelimiter || src.csv_delimiter || next.csvDelimiter || ',') || ',';
    next.textMode = String(src.textMode || src.text_mode || next.textMode || 'lines').trim() || 'lines';
    next.batchSize = String(src.batchSize || src.batch_size || next.batchSize || '200').trim() || '200';
    next.previewLimit = String(src.previewLimit || src.preview_limit || next.previewLimit || '20').trim() || '20';
    next.maxJsonBytes = String(src.maxJsonBytes || src.max_json_bytes || next.maxJsonBytes || '20971520').trim() || '20971520';
    next.selectFields = Array.isArray(src.selectFields || src.select_fields)
      ? (src.selectFields || src.select_fields).join(', ')
      : String(src.selectFields || src.select_fields || next.selectFields || '').trim();
    next.renameMap = stringifyJson(src.renameMap || src.rename_map, next.renameMap);
    next.defaultValues = stringifyJson(src.defaultValues || src.default_values, next.defaultValues);
    next.typeMap = stringifyJson(src.typeMap || src.type_map, next.typeMap);
    next.filterField = String(src.filterField || src.filter_field || next.filterField || '').trim();
    next.filterOperator = String(src.filterOperator || src.filter_operator || next.filterOperator || '').trim();
    next.filterValue = String(src.filterValue || src.filter_value || next.filterValue || '');
    next.filterRulesJson = stringifyJson(src.filterRules || src.filter_rules, next.filterRulesJson || '[]');
    next.computedFieldsJson = stringifyJson(src.computedFields || src.computed_fields, next.computedFieldsJson || '[]');
    next.parserMultiplier = String(src.parserMultiplier || src.parser_multiplier || next.parserMultiplier || '1').trim() || '1';
    next.sampleInput = String(src.sampleInput || src.sample_input || next.sampleInput || '');
    next.lookupEnabled = boolString(src.lookupEnabled ?? src.lookup_enabled ?? next.lookupEnabled);
    next.lookupSchema = String(src.lookupSchema || src.lookup_schema || next.lookupSchema || '').trim();
    next.lookupTable = String(src.lookupTable || src.lookup_table || next.lookupTable || '').trim();
    next.lookupSourceField = String(src.lookupSourceField || src.lookup_source_field || next.lookupSourceField || '').trim();
    next.lookupTargetField = String(src.lookupTargetField || src.lookup_target_field || next.lookupTargetField || '').trim();
    next.lookupFields = Array.isArray(src.lookupFields || src.lookup_fields)
      ? (src.lookupFields || src.lookup_fields).join(', ')
      : String(src.lookupFields || src.lookup_fields || next.lookupFields || '').trim();
    next.lookupPrefix = String(src.lookupPrefix || src.lookup_prefix || next.lookupPrefix || '').trim();
    next.lookupJoinMode = normalizeLookupJoinMode(src.lookupJoinMode || src.lookup_join_mode || next.lookupJoinMode || 'left');
    next.lookupConflictMode = String(src.lookupConflictMode || src.lookup_conflict_mode || next.lookupConflictMode || 'suffix').trim() || 'suffix';
    next.lookupSelectedFieldsJson = stringifyJson(src.lookupSelectedFields || src.lookup_selected_fields, next.lookupSelectedFieldsJson || '[]');
    next.lookupJoinRulesJson = stringifyJson(src.lookupJoinRules || src.lookup_join_rules, next.lookupJoinRulesJson || '[]');
    next.dedupeEnabled = boolString(src.dedupeEnabled ?? src.dedupe_enabled ?? next.dedupeEnabled);
    next.dedupeMode = String(src.dedupeMode || src.dedupe_mode || next.dedupeMode || 'full_row').trim() || 'full_row';
    next.dedupeFields = Array.isArray(src.dedupeFields || src.dedupe_fields)
      ? (src.dedupeFields || src.dedupe_fields).join(', ')
      : String(src.dedupeFields || src.dedupe_fields || next.dedupeFields || '').trim();
    next.dedupeKeep = String(src.dedupeKeep || src.dedupe_keep || next.dedupeKeep || 'first').trim() || 'first';
    next.groupEnabled = boolString(src.groupEnabled ?? src.group_enabled ?? next.groupEnabled);
    next.groupByFields = Array.isArray(src.groupByFields || src.group_by_fields)
      ? (src.groupByFields || src.group_by_fields).join(', ')
      : String(src.groupByFields || src.group_by_fields || next.groupByFields || '').trim();
    next.aggregateRulesJson = stringifyJson(src.aggregateRules || src.aggregate_rules, next.aggregateRulesJson || '[]');
    next.templateId = String(src.templateId || '').trim();
    next.templateStoreId = String(src.templateStoreId || '').trim();
    return next;
  }

  function stringifyJson(value: any, fallback = '{}') {
    if (typeof value === 'string') {
      const txt = value.trim();
      return txt || fallback;
    }
    if (value && typeof value === 'object') {
      try {
        return JSON.stringify(value, null, 2);
      } catch {
        return fallback;
      }
    }
    return fallback;
  }

  function boolString(value: any) {
    if (typeof value === 'boolean') return value ? 'true' : 'false';
    const raw = String(value ?? '').trim().toLowerCase();
    return ['1', 'true', 'yes', 'on', 'enabled'].includes(raw) ? 'true' : 'false';
  }

  function normalizeLookupJoinMode(value: any) {
    const raw = String(value ?? '').trim().toLowerCase();
    if (raw === 'left_join' || raw === 'all_matches') return 'left';
    if (raw === 'only_matches') return 'inner';
    if (raw === 'first_match') return 'first_match';
    if (['inner', 'left', 'right', 'full'].includes(raw)) return raw;
    return 'left';
  }

  function parseJsonSafe(raw: string, fallback: any) {
    const txt = String(raw || '').trim();
    if (!txt) return fallback;
    try {
      return JSON.parse(txt);
    } catch {
      return fallback;
    }
  }

  function parseCsvText(raw: string) {
    return String(raw || '')
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean);
  }

  function fallbackFieldName(path: string) {
    return (
      String(path || '')
        .split('.')
        .filter(Boolean)
        .slice(-1)[0] || String(path || '').trim()
    );
  }

  function inferFieldTypeFromRows(rows: Array<Record<string, any>>, fieldName: string) {
    for (const row of Array.isArray(rows) ? rows : []) {
      const value = row?.[fieldName];
      if (value === undefined || value === null || value === '') continue;
      if (typeof value === 'boolean') return 'boolean';
      if (typeof value === 'number') return Number.isInteger(value) ? 'integer' : 'numeric';
      if (typeof value === 'string') {
        const trimmed = String(value).trim();
        if (/^\d+$/.test(trimmed)) return 'integer';
        if (/^\d+\.\d+$/.test(trimmed)) return 'numeric';
        if (!Number.isNaN(Date.parse(trimmed)) && /[-T:]/.test(trimmed)) return 'timestamp';
        return 'text';
      }
      if (Array.isArray(value) || (value && typeof value === 'object')) return 'json';
    }
    return '';
  }

  function toPrettyJson(value: any, fallback = '[]') {
    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return fallback;
    }
  }

  function emptyParserPipelineModel(): ParserPipelineViewModel {
    return {
      inputProfile: {
        upstreamSummary: 'Источник не определён',
        upstreamCount: 0,
        contractFieldsCount: 0,
        hasSampleRaw: false,
        hasSamplePayload: false,
        hasSampleRows: false,
        inputKind: 'unknown',
        sourceBasis: 'insufficient-data'
      },
      parseStrategy: {
        detectedFormat: '-',
        strategy: 'unknown',
        manualOverride: false
      },
      payloadCandidate: {
        candidateSource: '-',
        candidatePath: '(вход целиком)',
        mode: 'unknown',
        hasPayloadSample: false
      },
      workingSetCandidate: {
        candidateRecordPath: '(корень)',
        rowsDetected: null,
        sampleRowsAvailable: 0,
        mode: 'unknown',
        hasTabularSet: false
      },
      resultSummary: {
        fieldsCount: 0,
        source: 'insufficient-data',
        hasDerivedOutputPreview: false
      },
      warningsSummary: {
        ambiguity: false,
        missingSample: true,
        legacyFallbackInUse: false,
        insufficientData: true,
        parserWarnings: [],
        total: 0
      },
      autoManualState: {
        input: 'unknown',
        strategy: 'unknown',
        payload: 'unknown',
        workingSet: 'unknown',
        result: 'unknown'
      }
    };
  }

  function cloneSettings(value: ParserSettings): ParserSettings {
    return { ...value };
  }

  function storeIdFromSettings(value: Record<string, any> | null | undefined) {
    const raw = Number(String(value?.templateStoreId || '').trim());
    return Number.isFinite(raw) && raw > 0 ? Math.trunc(raw) : 0;
  }

  function templateIdForDraft(draft: ParserDraft | null | undefined) {
    const id = Number(draft?.id || 0);
    return id > 0 ? `parser_tpl_${id}` : '';
  }

  function buildTemplatePayload(settingsValue: ParserSettings) {
    const cfg: Record<string, any> = {
      sourceMode: settingsValue.sourceMode,
      sourceNodeTemplateRef: settingsValue.sourceNodeTemplateRef,
      sourceNodeTemplateType: settingsValue.sourceNodeTemplateType,
      sourceNodeTemplateStoreId: settingsValue.sourceNodeTemplateStoreId,
      sourceNodeTemplateName: settingsValue.sourceNodeTemplateName,
      sourceFormat: settingsValue.sourceFormat,
      sourceSchema: settingsValue.sourceSchema,
      sourceTable: settingsValue.sourceTable,
      sourceColumn: settingsValue.sourceColumn,
      inputPath: settingsValue.inputPath,
      recordPath: settingsValue.recordPath,
      fileUrl: settingsValue.fileUrl,
      fileUrlPath: settingsValue.fileUrlPath,
      archiveEntry: settingsValue.archiveEntry,
      archiveFormat: settingsValue.archiveFormat,
      csvDelimiter: settingsValue.csvDelimiter,
      textMode: settingsValue.textMode,
      batchSize: settingsValue.batchSize,
      previewLimit: settingsValue.previewLimit,
      maxJsonBytes: settingsValue.maxJsonBytes,
      selectFields: settingsValue.selectFields,
      renameMap: parseJsonSafe(settingsValue.renameMap, {}),
      defaultValues: parseJsonSafe(settingsValue.defaultValues, {}),
      typeMap: parseJsonSafe(settingsValue.typeMap, {}),
      filterField: settingsValue.filterField,
      filterOperator: settingsValue.filterOperator,
      filterValue: settingsValue.filterValue,
      filterRules: parseJsonSafe(settingsValue.filterRulesJson, []),
      computedFields: (Array.isArray(parseJsonSafe(settingsValue.computedFieldsJson, [])) ? parseJsonSafe(settingsValue.computedFieldsJson, []) : [])
        .map((row: any) => serializeComputedFieldRule(row)),
      parserMultiplier: settingsValue.parserMultiplier,
      lookupEnabled: settingsValue.lookupEnabled === 'true',
      lookupSchema: settingsValue.lookupSchema,
      lookupTable: settingsValue.lookupTable,
      lookupSourceField: settingsValue.lookupSourceField,
      lookupTargetField: settingsValue.lookupTargetField,
      lookupFields: settingsValue.lookupFields,
      lookupPrefix: settingsValue.lookupPrefix,
      lookupJoinMode: normalizeLookupJoinMode(settingsValue.lookupJoinMode),
      lookupConflictMode: settingsValue.lookupConflictMode,
      lookupSelectedFields: parseJsonSafe(settingsValue.lookupSelectedFieldsJson, []),
      lookupJoinRules: parseJsonSafe(settingsValue.lookupJoinRulesJson, []),
      dedupeEnabled: settingsValue.dedupeEnabled === 'true',
      dedupeMode: settingsValue.dedupeMode,
      dedupeFields: settingsValue.dedupeFields,
      dedupeKeep: settingsValue.dedupeKeep,
      groupEnabled: settingsValue.groupEnabled === 'true',
      groupByFields: settingsValue.groupByFields,
      aggregateRules: parseJsonSafe(settingsValue.aggregateRulesJson, [])
    };
    return cfg;
  }

  let settings = normalizeSettings(initialSettings);
  let lastInitialSignature = JSON.stringify(settings);
  let templatesLoading = false;
  let templatesError = '';
  let drafts: ParserDraft[] = [];
  let sourceTemplatesLoading = false;
  let sourceTemplatesError = '';
  let sourceTemplates: SourceNodeTemplate[] = [];
  let columnsCache: Record<string, ColumnMeta[]> = {};
  let selectedDraftId = 0;
  let templateNameInput = '';
  let templateDescriptionInput = '';
  let templateSaving = false;
  let templateDeleting = false;
  let templateSearch = '';
  let previewLoading = false;
  let previewError = '';
  let previewData: ParserPreview | null = null;
  let previewRaw: any = null;
  let previewUpdatedAt = '';
  let sourceNodePreviewMessage = '';
  let parserPipelineModel: ParserPipelineViewModel = emptyParserPipelineModel();
  let activeStep: ParserEditorStep = 'fields';

  $: {
    const next = normalizeSettings(initialSettings);
    const signature = JSON.stringify(next);
    if (signature !== lastInitialSignature) {
      lastInitialSignature = signature;
      settings = next;
    }
  }

  $: selectedDraft = drafts.find((item) => Number(item.id || 0) === selectedDraftId) || null;
  $: filteredDrafts = drafts.filter((item) => {
    const needle = templateSearch.trim().toLowerCase();
    if (!needle) return true;
    const hay = `${item.name} ${item.description}`.toLowerCase();
    return hay.includes(needle);
  });
  $: currentTemplateStoreId = storeIdFromSettings(settings);
  $: currentTemplate = drafts.find((item) => Number(item.id || 0) === currentTemplateStoreId) || null;
  $: selectedIsCurrent = Boolean(selectedDraft && currentTemplate && Number(selectedDraft.id) === Number(currentTemplate.id));
  $: canSwitchTemplate = Boolean(selectedDraft && !selectedIsCurrent);
  $: currentSourceTemplate =
    sourceTemplates.find((item) => item.ref === String(settings.sourceNodeTemplateRef || '').trim()) ||
    sourceTemplates.find((item) => String(item.storeId || 0) === String(settings.sourceNodeTemplateStoreId || '').trim()) ||
    null;
  $: sourceNodePreview = settings.sourceMode === 'node' ? buildSourceNodePreviewFromTemplate(currentSourceTemplate) : { rows: [], columns: [], message: '' };
  $: sourceNodePreviewJson = sourceNodePreview.rows.length ? JSON.stringify(sourceNodePreview.rows, null, 2) : '';
  $: sourceNodePreviewMessage = sourceNodePreview.message || '';
  $: sourcePreviewColumns =
    settings.sourceMode === 'node'
      ? sourceNodePreview.columns
      : Array.isArray(previewData?.raw_columns)
      ? previewData.raw_columns
      : [];
  $: sourcePreviewRows =
    settings.sourceMode === 'node'
      ? sourceNodePreview.rows
      : Array.isArray(previewData?.raw_sample_rows)
      ? previewData.raw_sample_rows
      : [];
  $: sourcePreviewRowCount =
    settings.sourceMode === 'node'
      ? currentSourceTemplate
        ? sourceNodePreview.rows.length
        : '-'
      : (previewData?.raw_row_count ?? '-');
  $: sourcePreviewColumnCount =
    settings.sourceMode === 'node'
      ? currentSourceTemplate
        ? sourceNodePreview.columns.length
        : '-'
      : (previewData?.raw_column_count ?? '-');
  $: sourcePreviewSourceRef =
    settings.sourceMode === 'node'
      ? currentSourceTemplate
        ? sourceTemplateLabel(currentSourceTemplate)
        : '-'
      : previewData?.source_ref || '-';
  $: workingFieldCandidates = Array.from(
    new Set([
      ...sourcePreviewColumns,
      ...(Array.isArray(previewData?.columns) ? previewData.columns : []),
      ...parseCsvText(settings.selectFields),
      ...parseCsvText(settings.groupByFields),
      ...parseCsvText(settings.dedupeFields)
    ].filter(Boolean))
  );
  $: renameMapValue = parseJsonSafe(settings.renameMap, {});
  $: defaultValuesValue = parseJsonSafe(settings.defaultValues, {});
  $: typeMapValue = parseJsonSafe(settings.typeMap, {});
  $: selectedFieldPaths = parseCsvText(settings.selectFields);
  $: selectedFieldRows = selectedFieldPaths.map((path) => {
    const fallbackKey = fallbackFieldName(path);
    return {
      path,
      alias: String(renameMapValue?.[path] || renameMapValue?.[fallbackKey] || fallbackKey).trim(),
      type: String(typeMapValue?.[path] || typeMapValue?.[fallbackKey] || '').trim(),
      defaultValue:
        defaultValuesValue?.[path] !== undefined
          ? String(defaultValuesValue[path])
          : defaultValuesValue?.[fallbackKey] !== undefined
          ? String(defaultValuesValue[fallbackKey])
          : ''
    };
  });
  $: filterRules = Array.isArray(parseJsonSafe(settings.filterRulesJson, [])) ? parseJsonSafe(settings.filterRulesJson, []) : [];
  $: computedFields = (Array.isArray(parseJsonSafe(settings.computedFieldsJson, [])) ? parseJsonSafe(settings.computedFieldsJson, []) : [])
    .map((row: any) => normalizeComputedFieldRule(row));
  $: aggregateRules = Array.isArray(parseJsonSafe(settings.aggregateRulesJson, [])) ? parseJsonSafe(settings.aggregateRulesJson, []) : [];
  $: lookupSelectedFields = parseLookupSelectedFields(settings.lookupSelectedFieldsJson, settings.lookupFields);
  $: lookupJoinRules = parseLookupJoinRules(settings.lookupJoinRulesJson, settings.lookupSourceField, settings.lookupTargetField);
  $: parserWarnings = Array.isArray(previewData?.stats?.warnings) ? previewData.stats.warnings : [];
  $: parserAppliedSteps = Array.isArray(previewData?.stats?.applied_steps) ? previewData.stats.applied_steps : [];
  $: bayesInput =
    previewData?.stats && typeof previewData.stats.bayes_input === 'object' && previewData.stats.bayes_input
      ? previewData.stats.bayes_input
      : null;
  $: bayesJoin =
    previewData?.stats && typeof previewData.stats.bayes_join === 'object' && previewData.stats.bayes_join
      ? previewData.stats.bayes_join
      : null;
  $: bayesInputAlternativesLabel =
    Array.isArray(bayesInput?.alternatives) && bayesInput.alternatives.length
      ? bayesInput.alternatives.map((item: any) => `${item.path_label} (${formatProbability(item.probability)})`).join(' • ')
      : '';
  $: lookupSummary =
    previewData?.stats && typeof previewData.stats.lookup_summary === 'object' && previewData.stats.lookup_summary
      ? previewData.stats.lookup_summary
      : null;
  $: autoParseInfo = {
    detectedFormat: String(previewData?.stats?.detected_format || previewData?.source_format || '-'),
    payloadOrigin: String(previewData?.stats?.payload_origin || '-'),
    inputKind: String(previewData?.stats?.input_kind || '-'),
    workingSetPath: String(previewData?.stats?.working_set_path || settings.recordPath || '(корень)'),
    sourcePath: String(previewData?.stats?.source_path || settings.inputPath || '(вход целиком)')
  };
  $: sourceTableColumnsMeta = columnsCache[tableCacheKey(settings.sourceSchema, settings.sourceTable)] || [];
  $: previewResultRows = Array.isArray(previewData?.sample_rows) ? previewData.sample_rows : [];
  $: incomingNodes = Array.isArray(incomingDescriptor?.upstreamNodes) ? incomingDescriptor.upstreamNodes : [];
  $: incomingDescriptorNodeId = String(incomingDescriptor?.nodeId || '').trim();
  $: derivedOutputFields = buildDerivedOutputFields();
  $: derivedOutputSourceLabel =
    Array.isArray(previewData?.columns) && previewData.columns.length
      ? 'По результату preview'
      : derivedOutputFields.length
      ? 'По текущим settings parser'
      : '';
  $: parserPipelineModel = buildParserPipelineViewModel();
  $: computedFunctionLibrary = COMPUTED_FUNCTION_CATEGORIES.map((category) => ({
    ...category,
    items: COMPUTED_FUNCTIONS.filter((item) => item.category === category.id)
  }));
  $: if (!selectedDraft && currentTemplate) {
    templateNameInput = templateNameInput || currentTemplate.name;
    templateDescriptionInput = templateDescriptionInput || currentTemplate.description;
  }
  function dispatchSettings(next: ParserSettings) {
    settings = cloneSettings(next);
    dispatch('configChange', { settings: settings });
  }

  function patchSetting(key: string, value: string) {
    const next = cloneSettings(settings);
    next[key] = value;
    dispatchSettings(next);
  }

  function sourceTemplateRef(kind: 'api_request' | 'table_parser', storeId: number) {
    return `${kind}:${Math.trunc(Number(storeId || 0))}`;
  }

  function sourceTemplateLabel(item: SourceNodeTemplate) {
    return `${item.templateType === 'api_request' ? 'Запросы' : 'Legacy parser'} / ${item.name}`;
  }

  function applySourceNodeTemplateRef(ref: string) {
    const next = cloneSettings(settings);
    const selected = sourceTemplates.find((item) => item.ref === String(ref || '').trim()) || null;
    if (!selected) {
      next.sourceNodeTemplateRef = '';
      next.sourceNodeTemplateType = '';
      next.sourceNodeTemplateStoreId = '';
      next.sourceNodeTemplateName = '';
    } else {
      next.sourceNodeTemplateRef = selected.ref;
      next.sourceNodeTemplateType = selected.templateType;
      next.sourceNodeTemplateStoreId = String(selected.storeId);
      next.sourceNodeTemplateName = selected.name;
    }
    dispatchSettings(next);
  }

  function patchJsonSetting(key: string, value: any, fallback = '[]') {
    patchSetting(key, toPrettyJson(value, fallback));
  }

  function tableCacheKey(schema: string, table: string) {
    return `${String(schema || '').trim()}.${String(table || '').trim()}`;
  }

  async function ensureColumnsFor(schema: string, table: string) {
    const s = String(schema || '').trim();
    const t = String(table || '').trim();
    if (!s || !t) return;
    const key = tableCacheKey(s, t);
    if (Array.isArray(columnsCache[key])) return;
    try {
      const payload = await apiJson<{ columns: Array<{ name: string; type?: string }> }>(
        `${apiBase}/columns?schema=${encodeURIComponent(s)}&table=${encodeURIComponent(t)}`,
        { headers: headers() }
      );
      const cols = Array.isArray(payload?.columns)
        ? payload.columns
            .map((item) => ({
              name: String(item?.name || '').trim(),
              type: String(item?.type || '').trim().toLowerCase() || undefined
            }))
            .filter((item) => item.name)
        : [];
      columnsCache = { ...columnsCache, [key]: cols };
    } catch {
      columnsCache = { ...columnsCache, [key]: [] };
    }
  }

  function columnOptionsFor(schema: string, table: string) {
    return (columnsCache[tableCacheKey(schema, table)] || []).map((item) => item.name);
  }

  function parseLookupSelectedFields(raw: string, legacyRaw: string) {
    const parsed = parseJsonSafe(raw, []);
    if (Array.isArray(parsed)) return parsed.map((item) => String(item || '').trim()).filter(Boolean);
    return parseCsvText(legacyRaw);
  }

  function parseLookupJoinRules(raw: string, legacySourceField: string, legacyTargetField: string): LookupJoinRule[] {
    const parsed = parseJsonSafe(raw, []);
    if (Array.isArray(parsed) && parsed.length) {
      return parsed
        .map((item) => ({
          sourceField: String(item?.sourceField || item?.source_field || '').trim(),
          targetField: String(item?.targetField || item?.target_field || '').trim()
        }))
        .filter((item) => item.sourceField || item.targetField);
    }
    const sourceField = String(legacySourceField || '').trim();
    const targetField = String(legacyTargetField || '').trim();
    return sourceField || targetField ? [{ sourceField, targetField }] : [];
  }

  function syncLookupSettings(rows: LookupJoinRule[], selectedFields = lookupSelectedFields) {
    const next = cloneSettings(settings);
    const cleanRows = rows
      .map((row) => ({
        sourceField: String(row?.sourceField || '').trim(),
        targetField: String(row?.targetField || '').trim()
      }))
      .filter((row) => row.sourceField || row.targetField);
    const cleanFields = (Array.isArray(selectedFields) ? selectedFields : [])
      .map((item) => String(item || '').trim())
      .filter(Boolean);
    next.lookupJoinRulesJson = toPrettyJson(cleanRows, '[]');
    next.lookupSelectedFieldsJson = toPrettyJson(cleanFields, '[]');
    next.lookupSourceField = cleanRows[0]?.sourceField || '';
    next.lookupTargetField = cleanRows[0]?.targetField || '';
    next.lookupFields = cleanFields.join(', ');
    dispatchSettings(next);
  }

  function addLookupJoinRule() {
    const sourceField = workingFieldCandidates[0] || '';
    const targetField = lookupColumnOptions[0] || '';
    syncLookupSettings([...lookupJoinRules, { sourceField, targetField }]);
  }

  function updateLookupJoinRule(index: number, patch: Partial<LookupJoinRule>) {
    syncLookupSettings(lookupJoinRules.map((row, idx) => (idx === index ? { ...row, ...patch } : row)));
  }

  function removeLookupJoinRule(index: number) {
    syncLookupSettings(lookupJoinRules.filter((_row, idx) => idx !== index));
  }

  function addLookupField(fieldName: string) {
    const wanted = String(fieldName || '').trim();
    if (!wanted) return;
    if (lookupSelectedFields.includes(wanted)) return;
    syncLookupSettings(lookupJoinRules, [...lookupSelectedFields, wanted]);
  }

  function removeLookupField(fieldName: string) {
    syncLookupSettings(
      lookupJoinRules,
      lookupSelectedFields.filter((item) => item !== fieldName)
    );
  }

  function setLookupFields(fields: string[]) {
    syncLookupSettings(lookupJoinRules, fields);
  }

  function addAllLookupFields() {
    setLookupFields(lookupColumnOptions);
  }

  function clearLookupFields() {
    setLookupFields([]);
  }

  function formatProbability(value: any) {
    const num = Number(value || 0);
    if (!Number.isFinite(num)) return '0%';
    return `${Math.round(num * 100)}%`;
  }

  function applyBayesWorkingSet() {
    const recommendedPath = String(bayesInput?.recommended_candidate?.path || '').trim();
    patchSetting('recordPath', recommendedPath);
  }

  function applyBayesJoinRules() {
    const rules = Array.isArray(bayesJoin?.recommended_rules)
      ? bayesJoin.recommended_rules.map((item: any) => ({
          sourceField: String(item?.sourceField || '').trim(),
          targetField: String(item?.targetField || '').trim()
        }))
      : [];
    if (!rules.length) return;
    syncLookupSettings(rules);
  }

  function rebuildFieldSettings(rows: Array<{ path: string; alias?: string; type?: string; defaultValue?: string }>) {
    const next = cloneSettings(settings);
    const cleanRows = rows
      .map((row) => ({
        path: String(row?.path || '').trim(),
        alias: String(row?.alias || '').trim(),
        type: String(row?.type || '').trim(),
        defaultValue: row?.defaultValue ?? ''
      }))
      .filter((row) => row.path);
    next.selectFields = cleanRows.map((row) => row.path).join(', ');
    const renameMap: Record<string, any> = {};
    const defaultValues: Record<string, any> = {};
    const typeMap: Record<string, any> = {};
    for (const row of cleanRows) {
      const fallbackKey = String(row.path || '').split('.').filter(Boolean).slice(-1)[0] || row.path;
      if (row.alias && row.alias !== fallbackKey) renameMap[row.path] = row.alias;
      if (row.defaultValue !== '') defaultValues[row.path] = row.defaultValue;
      if (row.type) typeMap[row.path] = row.type;
    }
    next.renameMap = toPrettyJson(renameMap, '{}');
    next.defaultValues = toPrettyJson(defaultValues, '{}');
    next.typeMap = toPrettyJson(typeMap, '{}');
    dispatchSettings(next);
  }

  function addAllDetectedFields() {
    const rows = workingFieldCandidates.map((path) => ({ path }));
    rebuildFieldSettings(rows);
  }

  function addSelectedField(path: string) {
    const wanted = String(path || '').trim();
    if (!wanted) return;
    if (selectedFieldPaths.includes(wanted)) return;
    rebuildFieldSettings([...selectedFieldRows, { path: wanted }]);
  }

  function updateSelectedFieldRow(index: number, patch: Record<string, any>) {
    const rows = selectedFieldRows.map((row, idx) => (idx === index ? { ...row, ...patch } : row));
    rebuildFieldSettings(rows);
  }

  function removeSelectedField(index: number) {
    rebuildFieldSettings(selectedFieldRows.filter((_row, idx) => idx !== index));
  }

  function updateRulesSetting(key: string, rows: any[]) {
    patchJsonSetting(key, rows);
  }

  function addFilterRule() {
    updateRulesSetting('filterRulesJson', [...filterRules, { field: '', operator: '=', value: '', secondValue: '' }]);
  }

  function updateFilterRule(index: number, patch: Record<string, any>) {
    updateRulesSetting('filterRulesJson', filterRules.map((row: any, idx: number) => (idx === index ? { ...row, ...patch } : row)));
  }

  function removeFilterRule(index: number) {
    updateRulesSetting('filterRulesJson', filterRules.filter((_row: any, idx: number) => idx !== index));
  }

  function syncComputedFieldRules(rows: any[]) {
    updateRulesSetting(
      'computedFieldsJson',
      rows.map((row: any) => serializeComputedFieldRule(row))
    );
  }

  function addComputedField() {
    syncComputedFieldRules([...computedFields, createComputedFieldRule()]);
  }

  function updateComputedField(index: number, patch: Record<string, any>) {
    syncComputedFieldRules(
      computedFields.map((row: any, idx: number) =>
        idx === index ? normalizeComputedFieldRule({ ...row, ...patch }) : normalizeComputedFieldRule(row)
      )
    );
  }

  function duplicateComputedField(index: number) {
    const target = computedFields[index];
    if (!target) return;
    const duplicate = duplicateComputedFieldRule(target);
    syncComputedFieldRules([...computedFields.slice(0, index + 1), duplicate, ...computedFields.slice(index + 1)]);
  }

  function removeComputedField(index: number) {
    syncComputedFieldRules(computedFields.filter((_row: any, idx: number) => idx !== index));
  }

  function availableComputedFieldsFor(
    index: number,
    baseFields: Array<{ name: string; type?: string }> = currentBaseComputedFields(
      selectedFieldRows,
      workingFieldCandidates,
      sourceTableColumnsMeta,
      sourcePreviewRows,
      previewResultRows
    ),
    allComputedFields: any[] = computedFields
  ) {
    const priorComputed = (Array.isArray(allComputedFields) ? allComputedFields : [])
      .slice(0, index)
      .map((row: any) => ({
        name: String(row?.name || '').trim(),
        type: String(row?.type || '').trim()
      }))
      .filter((row) => row.name);
    const merged = [...(Array.isArray(baseFields) ? baseFields : []), ...priorComputed];
    const seen = new Set<string>();
    return merged.filter((item) => {
      const key = String(item?.name || '').trim();
      if (!key || seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }

  function computedFieldTypesFor(
    index: number,
    baseFields: Array<{ name: string; type?: string }> = currentBaseComputedFields(
      selectedFieldRows,
      workingFieldCandidates,
      sourceTableColumnsMeta,
      sourcePreviewRows,
      previewResultRows
    ),
    allComputedFields: any[] = computedFields
  ) {
    const entries = availableComputedFieldsFor(index, baseFields, allComputedFields).map((item) => [item.name, item.type || '']);
    return Object.fromEntries(entries);
  }

  function currentBaseComputedFields(
    selectedRows: Array<{ path: string; alias: string; type: string }> = [],
    workingCandidates: string[] = [],
    tableColumns: ColumnMeta[] = [],
    sourceRows: Array<Record<string, any>> = [],
    resultRows: Array<Record<string, any>> = []
  ) {
    const safeSelectedRows = Array.isArray(selectedRows) ? selectedRows : [];
    const safeWorkingCandidates = Array.isArray(workingCandidates) ? workingCandidates : [];
    const safeTableColumns = Array.isArray(tableColumns) ? tableColumns : [];
    const safeSourceRows = Array.isArray(sourceRows) ? sourceRows : [];
    const safeResultRows = Array.isArray(resultRows) ? resultRows : [];
    if (safeSelectedRows.length) {
      return safeSelectedRows.map((row) => ({
        name: row.alias || fallbackFieldName(row.path),
        type: row.type || inferFieldTypeFromRows(safeSourceRows, fallbackFieldName(row.path)) || ''
      }));
    }
    return safeWorkingCandidates.map((name) => ({
      name,
      type:
        safeTableColumns.find((item) => item.name === name)?.type ||
        inferFieldTypeFromRows(safeSourceRows, name) ||
        inferFieldTypeFromRows(safeResultRows, name) ||
        ''
    }));
  }

  function addAggregateRule() {
    updateRulesSetting('aggregateRulesJson', [...aggregateRules, { field: '', op: 'count', as: '' }]);
  }

  function updateAggregateRule(index: number, patch: Record<string, any>) {
    updateRulesSetting('aggregateRulesJson', aggregateRules.map((row: any, idx: number) => (idx === index ? { ...row, ...patch } : row)));
  }

  function removeAggregateRule(index: number) {
    updateRulesSetting('aggregateRulesJson', aggregateRules.filter((_row: any, idx: number) => idx !== index));
  }

  function patchFromDraft(draft: ParserDraft) {
    const next = normalizeSettings({
      ...(draft?.config_json && typeof draft.config_json === 'object' ? draft.config_json : {}),
      templateId: templateIdForDraft(draft),
      templateStoreId: String(draft.id)
    });
    dispatchSettings(next);
  }

  async function loadDrafts(selectStoreId = 0) {
    templatesLoading = true;
    templatesError = '';
    try {
      const payload = await apiJson<{ parser_configs?: ParserDraft[] }>(`${apiBase}/parser-configs`, {
        method: 'GET',
        headers: { Accept: 'application/json', ...headers() }
      });
      drafts = (Array.isArray(payload?.parser_configs) ? payload.parser_configs : []).map((item) => ({
        id: Number(item?.id || 0),
        name: String(item?.name || item?.parser_name || '').trim(),
        parser_name: String(item?.parser_name || item?.name || '').trim(),
        description: String(item?.description || '').trim(),
        revision: Number(item?.revision || 1) || 1,
        updated_at: item?.updated_at || null,
        updated_by: String(item?.updated_by || '').trim(),
        config_json: item?.config_json && typeof item.config_json === 'object' ? item.config_json : {}
      }));
      const wantedId = selectStoreId > 0 ? selectStoreId : selectedDraftId;
      if (wantedId > 0 && drafts.some((item) => Number(item.id) === wantedId)) {
        selectedDraftId = wantedId;
      } else if (currentTemplateStoreId > 0 && drafts.some((item) => Number(item.id) === currentTemplateStoreId)) {
        selectedDraftId = currentTemplateStoreId;
      } else if (drafts.length && !selectedDraftId) {
        selectedDraftId = Number(drafts[0].id || 0);
      }
    } catch (e: any) {
      templatesError = String(e?.message || e || 'Не удалось загрузить шаблоны parser');
    } finally {
      templatesLoading = false;
    }
  }

  async function loadSourceTemplates() {
    sourceTemplatesLoading = true;
    sourceTemplatesError = '';
    try {
      const [apiPayload, parserPayload] = await Promise.all([
        apiJson<{ api_configs?: any[] }>(`${apiBase}/api-configs`, {
          method: 'GET',
          headers: { Accept: 'application/json', ...headers() }
        }),
        apiJson<{ parser_configs?: ParserDraft[] }>(`${apiBase}/parser-configs`, {
          method: 'GET',
          headers: { Accept: 'application/json', ...headers() }
        })
      ]);
      const apiTemplates: SourceNodeTemplate[] = (Array.isArray(apiPayload?.api_configs) ? apiPayload.api_configs : [])
        .map((item) => ({
          ref: sourceTemplateRef('api_request', Number(item?.id || 0)),
          templateType: 'api_request' as const,
          storeId: Number(item?.id || 0),
          name: String(item?.api_name || item?.name || '').trim(),
          description: String(item?.description || '').trim(),
          config_json: item?.config_json && typeof item.config_json === 'object' ? item.config_json : {},
          output_parameters: Array.isArray(item?.output_parameters) ? item.output_parameters : [],
          picked_paths: Array.isArray(item?.picked_paths) ? item.picked_paths : []
        }))
        .filter((item) => item.storeId > 0 && item.name);
      const parserTemplates: SourceNodeTemplate[] = (Array.isArray(parserPayload?.parser_configs) ? parserPayload.parser_configs : [])
        .map((item) => ({
          ref: sourceTemplateRef('table_parser', Number(item?.id || 0)),
          templateType: 'table_parser' as const,
          storeId: Number(item?.id || 0),
          name: String(item?.parser_name || item?.name || '').trim(),
          description: String(item?.description || '').trim(),
          config_json: item?.config_json && typeof item.config_json === 'object' ? item.config_json : {}
        }))
        .filter((item) => item.storeId > 0 && item.name);
      sourceTemplates = [...apiTemplates, ...parserTemplates];
    } catch (e: any) {
      sourceTemplatesError = String(e?.message || e || 'Не удалось загрузить шаблоны нод');
      sourceTemplates = [];
    } finally {
      sourceTemplatesLoading = false;
    }
  }

  async function saveTemplate() {
    const parser_name = String(templateNameInput || '').trim();
    if (!parser_name) {
      templatesError = 'Укажи название шаблона парсинга.';
      return;
    }
    templateSaving = true;
    templatesError = '';
    try {
      const currentId = Number(selectedDraft?.id || 0);
      const payload = await apiJson<{ id: number; revision: number }>(`${apiBase}/parser-configs/upsert`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({
          id: currentId > 0 ? currentId : undefined,
          parser_name,
          description: String(templateDescriptionInput || '').trim(),
          config_json: buildTemplatePayload(settings)
        })
      });
      const savedId = Number(payload?.id || 0);
      await loadDrafts(savedId);
      await loadSourceTemplates();
      const savedDraft = drafts.find((item) => Number(item.id) === savedId);
      if (savedDraft) {
        patchFromDraft(savedDraft);
        templateNameInput = savedDraft.name;
        templateDescriptionInput = savedDraft.description;
      } else if (savedId > 0) {
        const next = cloneSettings(settings);
        next.templateStoreId = String(savedId);
        next.templateId = `parser_tpl_${savedId}`;
        dispatchSettings(next);
      }
    } catch (e: any) {
      templatesError = String(e?.message || e || 'Не удалось сохранить шаблон');
    } finally {
      templateSaving = false;
    }
  }

  async function deleteTemplate() {
    if (!selectedDraft) return;
    templateDeleting = true;
    templatesError = '';
    try {
      await apiJson(`${apiBase}/parser-configs/delete`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({ id: selectedDraft.id })
      });
      const deletedId = Number(selectedDraft.id || 0);
      selectedDraftId = 0;
      if (currentTemplateStoreId === deletedId) {
        const next = cloneSettings(settings);
        next.templateId = '';
        next.templateStoreId = '';
        dispatchSettings(next);
      }
      templateNameInput = '';
      templateDescriptionInput = '';
      await loadDrafts();
      await loadSourceTemplates();
    } catch (e: any) {
      templatesError = String(e?.message || e || 'Не удалось удалить шаблон');
    } finally {
      templateDeleting = false;
    }
  }

  function resetTemplateDraft() {
    selectedDraftId = 0;
    templateNameInput = '';
    templateDescriptionInput = '';
  }

  function selectDraft(draft: ParserDraft) {
    selectedDraftId = Number(draft.id || 0);
    templateNameInput = draft.name;
    templateDescriptionInput = draft.description;
  }

  function applySelectedTemplate() {
    if (!selectedDraft || selectedIsCurrent) return;
    patchFromDraft(selectedDraft);
  }

  async function previewNow() {
    previewLoading = true;
    previewError = '';
    try {
      let inputValue = settings.sampleInput;
      if (settings.sourceMode === 'node') {
        if (!currentSourceTemplate) {
          previewError = 'Сначала выбери шаблон ноды-источника.';
          previewData = null;
          previewRaw = null;
          return;
        }
        if (!sourceNodePreviewJson) {
          previewError = sourceNodePreviewMessage || 'У выбранного шаблона ноды ещё не настроены выходные параметры.';
          previewData = null;
          previewRaw = null;
          return;
        }
        inputValue = sourceNodePreviewJson;
      }
      const payload = await apiJson<{ preview?: ParserPreview; result?: any }>(`${apiBase}/parser-configs/preview`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({
          config_json: buildTemplatePayload(settings),
          input_value: inputValue,
          cursor: { offset: 0 }
        })
      });
      previewData = payload?.preview || null;
      previewRaw = payload?.result || null;
      previewUpdatedAt = new Date().toISOString();
    } catch (e: any) {
      previewError = String(e?.message || e || 'Не удалось получить preview');
      previewData = null;
      previewRaw = null;
    } finally {
      previewLoading = false;
    }
  }

  function sourceTableOptions() {
    return Array.isArray(existingTables) ? existingTables : [];
  }

  function currentTableColumnsHint() {
    const parts = [];
    if (settings.sourceSchema && settings.sourceTable) parts.push(`${settings.sourceSchema}.${settings.sourceTable}`);
    if (settings.sourceColumn) parts.push(`колонка ${settings.sourceColumn}`);
    else if (sourceColumnOptions.length) parts.push(`доступно колонок: ${sourceColumnOptions.length}`);
    return parts.join(' / ');
  }

  function inputValue(event: Event) {
    return (event.currentTarget as HTMLInputElement | null)?.value ?? '';
  }

  function incomingNodeTypeLabel(value: string) {
    const key = String(value || '').trim();
    return INCOMING_NODE_TYPE_LABELS[key] || key || 'Неизвестный источник';
  }

  function incomingNodeDescription(item: { nodeType?: string; fromPort?: string }) {
    const nodeType = incomingNodeTypeLabel(String(item?.nodeType || '').trim());
    const port = String(item?.fromPort || 'out').trim() || 'out';
    return `Источник приходит из desk graph от ноды типа «${nodeType}» через порт «${port}».`;
  }

  function incomingContractFields(item: { contractFields?: ParserIncomingContractField[] }) {
    return Array.isArray(item?.contractFields) ? item.contractFields.filter(Boolean) : [];
  }

  function incomingSampleMeta(item: { sampleMeta?: ParserIncomingSampleMeta | null }) {
    return item?.sampleMeta && typeof item.sampleMeta === 'object' ? item.sampleMeta : null;
  }

  function incomingSampleNode() {
    return incomingNodes.find((item) => item?.sampleRaw !== undefined || item?.samplePayload !== undefined || (Array.isArray(item?.sampleRows) && item.sampleRows.length)) || null;
  }

  function incomingFieldKey(field: ParserIncomingContractField | null | undefined) {
    return String(field?.path || field?.alias || field?.name || '').trim();
  }

  function incomingFieldSelected(field: ParserIncomingContractField | null | undefined) {
    const key = incomingFieldKey(field);
    return key ? selectedFieldPaths.includes(key) : false;
  }

  function useIncomingField(field: ParserIncomingContractField | null | undefined) {
    const key = incomingFieldKey(field);
    activeStep = 'fields';
    if (!key) return;
    addSelectedField(key);
  }

  function fieldOptionsForRow(row: { path?: string }) {
    const current = String(row?.path || '').trim();
    return Array.from(new Set([current, ...(Array.isArray(workingFieldCandidates) ? workingFieldCandidates : [])].filter(Boolean)));
  }

  function pipelineInputKindFromCandidate(candidate: any, formatHint: string, contractFieldsCount: number): ParserPipelineInputKind {
    const detectedFormat = String(formatHint || '').trim().toLowerCase();
    if (settings.sourceMode === 'file_url') return 'link/reference/locator';
    if (settings.archiveEntry || settings.archiveFormat === 'zip' || detectedFormat === 'zip') return 'archive/container';
    if (Array.isArray(candidate) || (candidate && typeof candidate === 'object')) return 'structured payload';
    if (typeof candidate === 'string') {
      const txt = String(candidate || '').trim();
      if (!txt) return contractFieldsCount > 0 ? 'structured payload' : 'unknown';
      if (/^(https?:\/\/|ftp:\/\/)/i.test(txt) || /^[a-z]:\\/i.test(txt) || txt.startsWith('/')) return 'link/reference/locator';
      if (detectedFormat === 'csv' || detectedFormat === 'tsv' || /^[^\n]+\t[^\n]+/.test(txt) || /^[^\n]+,[^\n]+/.test(txt)) return 'tabular text';
      return 'text/string payload';
    }
    if (detectedFormat === 'csv' || detectedFormat === 'tsv') return 'tabular text';
    if (detectedFormat === 'json' || detectedFormat === 'ndjson' || contractFieldsCount > 0) return 'structured payload';
    return 'unknown';
  }

  function pipelineStrategyFromSignals(inputKind: ParserPipelineInputKind, detectedFormat: string): ParserPipelineStrategy {
    const format = String(detectedFormat || '').trim().toLowerCase();
    const payloadOrigin = String(autoParseInfo.payloadOrigin || '').trim().toLowerCase();
    const hasInputPath = Boolean(String(settings.inputPath || '').trim());
    const hasRecordPath = Boolean(String(settings.recordPath || '').trim());
    if (settings.sourceMode === 'file_url') return 'dereference link/reference';
    if (settings.archiveEntry || settings.archiveFormat === 'zip' || format === 'zip') return 'extract archive entry';
    if (hasInputPath && hasRecordPath) return 'multi-step extraction';
    if (hasInputPath || hasRecordPath) return 'path-based extraction';
    if (payloadOrigin && !['-', 'input', 'input_value', 'value', 'row'].includes(payloadOrigin)) return 'unwrap envelope';
    if (inputKind === 'text/string payload' && format === 'json') return 'parse JSON string';
    if (format === 'csv' || format === 'tsv') return 'parse CSV/TSV';
    if (inputKind === 'structured payload') return 'direct read';
    return 'unknown';
  }

  function buildParserPipelineViewModel(): ParserPipelineViewModel {
    try {
      const safeIncomingNodes = Array.isArray(incomingNodes) ? incomingNodes : [];
      const safeSelectedFieldRows = Array.isArray(selectedFieldRows) ? selectedFieldRows : [];
      const safeComputedFields = Array.isArray(computedFields) ? computedFields : [];
      const safeAggregateRules = Array.isArray(aggregateRules) ? aggregateRules : [];
      const safeDerivedOutputFields = Array.isArray(derivedOutputFields) ? derivedOutputFields : [];
      const safeSourcePreviewRows = Array.isArray(sourcePreviewRows) ? sourcePreviewRows : [];
      const safeSourcePreviewColumns = Array.isArray(sourcePreviewColumns) ? sourcePreviewColumns : [];
      const safeParserWarnings = Array.isArray(parserWarnings) ? parserWarnings : [];
      const safePreviewData = previewData && typeof previewData === 'object' ? previewData : null;
      const safeAutoParseInfo = autoParseInfo && typeof autoParseInfo === 'object'
        ? autoParseInfo
        : { detectedFormat: '-', payloadOrigin: '-', inputKind: '-', workingSetPath: '(корень)', sourcePath: '(вход целиком)' };
      const safeRenameMap = renameMapValue && typeof renameMapValue === 'object' && !Array.isArray(renameMapValue) ? renameMapValue : {};
      const safeTypeMap = typeMapValue && typeof typeMapValue === 'object' && !Array.isArray(typeMapValue) ? typeMapValue : {};
      const safeDefaultValues = defaultValuesValue && typeof defaultValuesValue === 'object' && !Array.isArray(defaultValuesValue) ? defaultValuesValue : {};
      const safeBayesAlternatives = Array.isArray(bayesInput?.alternatives) ? bayesInput.alternatives : [];
      const sampleNode = safeIncomingNodes.find((item) => item?.sampleRaw !== undefined || item?.samplePayload !== undefined || (Array.isArray(item?.sampleRows) && item.sampleRows.length)) || null;
      const firstSampleRaw = sampleNode?.sampleRaw;
      const firstSamplePayload = sampleNode?.samplePayload;
      const firstSampleRows = Array.isArray(sampleNode?.sampleRows) ? sampleNode.sampleRows : [];
      const totalContractFields = safeIncomingNodes.reduce((acc, item) => acc + incomingContractFields(item).length, 0);
      const hasSampleRaw = firstSampleRaw !== undefined;
      const hasSamplePayload = firstSamplePayload !== undefined;
      const hasSampleRows = firstSampleRows.length > 0;
      const hasPreviewSignals =
        Boolean(safePreviewData?.stats) ||
        Boolean(Array.isArray(safePreviewData?.raw_columns) && safePreviewData.raw_columns.length) ||
        Boolean(Array.isArray(safePreviewData?.columns) && safePreviewData.columns.length);
      const sourcesCount = [totalContractFields > 0, hasSampleRaw || hasSamplePayload || hasSampleRows, hasPreviewSignals].filter(Boolean).length;
      const sourceBasis: ParserPipelineSourceBasis =
        sourcesCount > 1
          ? 'mixed'
          : hasSampleRaw || hasSamplePayload || hasSampleRows
          ? 'sample-based'
          : hasPreviewSignals
          ? 'preview-derived'
          : totalContractFields > 0
          ? 'contract-only'
          : 'insufficient-data';
      const sampleCandidate = hasSamplePayload ? firstSamplePayload : hasSampleRaw ? firstSampleRaw : null;
      const inputKind = pipelineInputKindFromCandidate(sampleCandidate, String(safeAutoParseInfo.detectedFormat || ''), totalContractFields);
      const strategy = pipelineStrategyFromSignals(inputKind, String(safeAutoParseInfo.detectedFormat || ''));
      const strategyManualOverride = Boolean(
        settings.sourceFormat !== 'auto' ||
          settings.archiveFormat !== 'auto' ||
          String(settings.archiveEntry || '').trim() ||
          settings.textMode !== 'lines' ||
          settings.csvDelimiter !== ','
      );
      const payloadMode: ParserPipelineStepMode = String(settings.inputPath || '').trim()
        ? 'manual'
        : hasSamplePayload || String(safeAutoParseInfo.payloadOrigin || '').trim() !== '-'
        ? 'auto'
        : 'unknown';
      const rawRowCount = Number(safePreviewData?.raw_row_count);
      const rowsDetected = Number.isFinite(rawRowCount) ? rawRowCount : null;
      const sampleRowsAvailable = hasSampleRows ? firstSampleRows.length : safeSourcePreviewRows.length;
      const hasTabularSet = Boolean(sampleRowsAvailable > 0 || safeSourcePreviewColumns.length);
      const workingSetMode: ParserPipelineStepMode = String(settings.recordPath || '').trim()
        ? 'manual'
        : String(safeAutoParseInfo.workingSetPath || '').trim() && String(safeAutoParseInfo.workingSetPath || '').trim() !== '-'
        ? 'auto'
        : hasTabularSet
        ? 'partial'
        : 'unknown';
      const hasResultSettings = Boolean(
        safeSelectedFieldRows.length ||
          safeComputedFields.length ||
          safeAggregateRules.length ||
          parseCsvText(settings.selectFields).length ||
          Object.keys(safeRenameMap).length ||
          Object.keys(safeTypeMap).length ||
          Object.keys(safeDefaultValues).length
      );
      const resultSource: ParserPipelineViewModel['resultSummary']['source'] =
        Array.isArray(safePreviewData?.columns) && safePreviewData.columns.length && hasResultSettings
          ? 'mixed'
          : Array.isArray(safePreviewData?.columns) && safePreviewData.columns.length
          ? 'preview'
          : safeDerivedOutputFields.length
          ? 'settings'
          : 'insufficient-data';
      const ambiguity = Boolean((safeBayesAlternatives.length > 1) || safeIncomingNodes.length > 1);
      const missingSample = !(hasSampleRaw || hasSamplePayload || hasSampleRows);
      const legacyFallbackInUse = settings.sourceMode === 'node' && Boolean(String(settings.sourceNodeTemplateRef || '').trim());
      const insufficientData = !totalContractFields && missingSample && !hasPreviewSignals;
      const parserWarningsList = safeParserWarnings.map((item) => String(item || '').trim()).filter(Boolean);
      const warningsTotal =
        parserWarningsList.length +
        (ambiguity ? 1 : 0) +
        (missingSample ? 1 : 0) +
        (legacyFallbackInUse ? 1 : 0) +
        (insufficientData ? 1 : 0);

      return {
        inputProfile: {
          upstreamSummary: safeIncomingNodes.length
            ? safeIncomingNodes.length === 1
              ? `${safeIncomingNodes[0].nodeName} / ${incomingNodeTypeLabel(safeIncomingNodes[0].nodeType)}`
              : `${safeIncomingNodes.length} upstream-источника`
            : 'Источник не определён',
          upstreamCount: safeIncomingNodes.length,
          contractFieldsCount: totalContractFields,
          hasSampleRaw,
          hasSamplePayload,
          hasSampleRows,
          inputKind,
          sourceBasis
        },
        parseStrategy: {
          detectedFormat: String(safeAutoParseInfo.detectedFormat || settings.sourceFormat || '-'),
          strategy,
          manualOverride: strategyManualOverride
        },
        payloadCandidate: {
          candidateSource: String(safeAutoParseInfo.payloadOrigin || incomingSampleMeta(sampleNode || {})?.source || settings.sourceMode || '-'),
          candidatePath: String(safeAutoParseInfo.sourcePath || settings.inputPath || '(вход целиком)'),
          mode: payloadMode,
          hasPayloadSample: hasSamplePayload
        },
        workingSetCandidate: {
          candidateRecordPath: String(safeAutoParseInfo.workingSetPath || settings.recordPath || '(корень)'),
          rowsDetected,
          sampleRowsAvailable,
          mode: workingSetMode,
          hasTabularSet
        },
        resultSummary: {
          fieldsCount: safeDerivedOutputFields.length,
          source: resultSource,
          hasDerivedOutputPreview: safeDerivedOutputFields.length > 0 || (Array.isArray(safePreviewData?.sample_rows) && safePreviewData.sample_rows.length > 0)
        },
        warningsSummary: {
          ambiguity,
          missingSample,
          legacyFallbackInUse,
          insufficientData,
          parserWarnings: parserWarningsList,
          total: warningsTotal
        },
        autoManualState: {
          input: settings.sourceMode === 'node' ? (safeIncomingNodes.length ? 'auto' : 'unknown') : 'manual',
          strategy: strategyManualOverride ? 'manual' : strategy !== 'unknown' ? 'auto' : 'unknown',
          payload: payloadMode,
          workingSet: workingSetMode,
          result:
            resultSource === 'mixed'
              ? 'partial'
              : hasResultSettings
              ? 'manual'
              : safeDerivedOutputFields.length || (Array.isArray(safePreviewData?.sample_rows) && safePreviewData.sample_rows.length > 0)
              ? 'auto'
              : 'unknown'
        }
      };
    } catch {
      return emptyParserPipelineModel();
    }
  }

  function outputFieldTypeFromSettings(fieldName: string) {
    const wanted = String(fieldName || '').trim();
    if (!wanted) return '';
    const selected = selectedFieldRows.find((row) => String(row?.alias || fallbackFieldName(row?.path || '')).trim() === wanted);
    if (selected?.type) return String(selected.type).trim();
    const computed = computedFields.find((row: any) => String(row?.name || '').trim() === wanted);
    if (computed?.type) return String(computed.type).trim();
    const aggregate = aggregateRules.find((row: any) => String(row?.as || `${row?.op || 'count'}_${row?.field || 'rows'}`).trim() === wanted);
    if (aggregate) return 'numeric';
    const fallback = String(typeMapValue?.[wanted] || '').trim();
    return fallback;
  }

  function buildDerivedOutputFields(): ParserDerivedOutputField[] {
    const previewColumns = Array.isArray(previewData?.columns) ? previewData.columns.map((item: any) => String(item || '').trim()).filter(Boolean) : [];
    const safeSelectedFieldRows = Array.isArray(selectedFieldRows) ? selectedFieldRows : [];
    const safeComputedFields = Array.isArray(computedFields) ? computedFields : [];
    const safeAggregateRules = Array.isArray(aggregateRules) ? aggregateRules : [];
    const safePreviewResultRows = Array.isArray(previewResultRows) ? previewResultRows : [];
    const out: ParserDerivedOutputField[] = [];
    const seen = new Set<string>();
    const pushField = (field: ParserDerivedOutputField | null | undefined) => {
      const name = String(field?.name || '').trim();
      if (!name) return;
      const key = name.toLowerCase();
      if (seen.has(key)) return;
      seen.add(key);
      out.push({
        name,
        alias: String(field?.alias || '').trim() || undefined,
        type: String(field?.type || '').trim() || undefined,
        path: String(field?.path || '').trim() || undefined,
        source: field?.source === 'settings' ? 'settings' : 'preview'
      });
    };

    if (previewColumns.length) {
      previewColumns.forEach((column) => {
        const selected = safeSelectedFieldRows.find((row) => String(row?.alias || fallbackFieldName(row?.path || '')).trim() === column);
        const computed = safeComputedFields.find((row: any) => String(row?.name || '').trim() === column);
        const aggregate = safeAggregateRules.find((row: any) => String(row?.as || `${row?.op || 'count'}_${row?.field || 'rows'}`).trim() === column);
        pushField({
          name: column,
          alias: selected?.alias || computed?.name || aggregate?.as || undefined,
          type:
            String(selected?.type || computed?.type || '').trim() ||
            outputFieldTypeFromSettings(column) ||
            inferFieldTypeFromRows(safePreviewResultRows, column),
          path: selected?.path || undefined,
          source: 'preview'
        });
      });
      return out;
    }

    safeSelectedFieldRows.forEach((row) => {
      pushField({
        name: String(row?.alias || fallbackFieldName(row?.path || '')).trim(),
        alias: String(row?.alias || '').trim(),
        type: String(row?.type || '').trim(),
        path: String(row?.path || '').trim(),
        source: 'settings'
      });
    });
    safeComputedFields.forEach((row: any) => {
      const name = String(row?.name || '').trim();
      if (!name) return;
      pushField({
        name,
        alias: name,
        type: String(row?.type || '').trim(),
        source: 'settings'
      });
    });
    safeAggregateRules.forEach((row: any) => {
      const name = String(row?.as || `${row?.op || 'count'}_${row?.field || 'rows'}`).trim();
      if (!name) return;
      pushField({
        name,
        alias: name,
        type: 'numeric',
        path: String(row?.field || '').trim() || undefined,
        source: 'settings'
      });
    });
    return out;
  }

  function selectValue(event: Event) {
    return (event.currentTarget as HTMLSelectElement | null)?.value ?? '';
  }

  function textareaValue(event: Event) {
    return (event.currentTarget as HTMLTextAreaElement | null)?.value ?? '';
  }

  function clearSelectValue(event: Event) {
    const target = event.currentTarget as HTMLSelectElement | null;
    if (target) target.value = '';
  }

  onMount(() => {
    void loadDrafts(storeIdFromSettings(initialSettings));
    void loadSourceTemplates();
    void previewNow();
  });

  $: sourceColumnOptions = columnOptionsFor(settings.sourceSchema, settings.sourceTable);
  $: lookupColumnOptions = columnOptionsFor(settings.lookupSchema, settings.lookupTable);
  $: if (settings.sourceSchema && settings.sourceTable) {
    void ensureColumnsFor(settings.sourceSchema, settings.sourceTable);
  }
  $: if (settings.lookupSchema && settings.lookupTable) {
    void ensureColumnsFor(settings.lookupSchema, settings.lookupTable);
  }
</script>

<section class="panel" class:panel-embedded={embeddedMode}>
  <div class="parser-layout">
    <section class="parser-column parser-column-input">
      <div class="parser-shell-card">
        <div class="parser-card-head">
          <div>
            <h3>Входящие параметры</h3>
            <p>Реальный upstream из текущего desk graph. Поля ниже можно быстро добавить в результат parser без отдельного legacy-flow.</p>
          </div>
        </div>
        {#if incomingNodes.length}
          <div class="parser-shell-summary">
            <span class="chip-chip readonly-chip">{incomingNodes.length} {incomingNodes.length === 1 ? 'источник' : 'источника'}</span>
            {#if incomingDescriptorNodeId}
              <span class="inline-hint">Для ноды: {incomingDescriptorNodeId}</span>
            {/if}
          </div>
          <div class="parser-shell-list">
            {#each incomingNodes as item}
              <div class="parser-shell-item">
                <strong>{item.nodeName}</strong>
                <div class="parser-shell-meta">
                  <span>{incomingNodeTypeLabel(item.nodeType)}</span>
                  <span>Порт: {item.fromPort || 'out'}</span>
                </div>
                <div class="parser-shell-description">{incomingNodeDescription(item)}</div>
                {#if incomingSampleMeta(item)}
                  <div class="inline-hint">Доступен optional sample snapshot из текущего UI-state{#if incomingSampleMeta(item)?.responsesCount} · ответов: {incomingSampleMeta(item)?.responsesCount}{/if}{#if incomingSampleMeta(item)?.payloadPath} · payload path: {incomingSampleMeta(item)?.payloadPath}{/if}</div>
                {/if}
                <div class="parser-contract-block">
                  <div class="parser-contract-head">
                    <span>Входной контракт upstream</span>
                    <span>{incomingContractFields(item).length ? `${incomingContractFields(item).length} полей` : 'контракт не определён'}</span>
                  </div>
                  {#if incomingContractFields(item).length}
                    <div class="parser-contract-list">
                      {#each incomingContractFields(item) as field}
                        <div class="parser-contract-item" class:is-selected={incomingFieldSelected(field)}>
                          <div class="parser-contract-name">{field.alias || field.name || field.path || 'Поле'}</div>
                          <div class="parser-contract-meta">
                            {#if field.name && field.alias && field.name !== field.alias}
                              <span>Имя: {field.name}</span>
                            {/if}
                            {#if field.alias}
                              <span>Alias: {field.alias}</span>
                            {/if}
                            {#if field.type}
                              <span>Тип: {field.type}</span>
                            {/if}
                            {#if field.path}
                              <span>Path: {field.path}</span>
                            {/if}
                          </div>
                          <div class="parser-contract-actions">
                            <button type="button" class="mini-btn parser-contract-add-btn" on:click={() => useIncomingField(field)}>
                              {incomingFieldSelected(field) ? 'Открыть в полях' : 'В результат'}
                            </button>
                          </div>
                        </div>
                      {/each}
                    </div>
                  {:else}
                    <div class="parser-contract-fallback">
                      Upstream источник определён, но его входной контракт сейчас не описан в текущем desk state.
                    </div>
                  {/if}
                </div>
              </div>
            {/each}
          </div>
        {:else}
          <div class="inline-hint">В текущем desk graph для parser пока нет входящего соединения или upstream descriptor ещё не определён. Секция остаётся пустой, потому что fake input здесь не используется.</div>
        {/if}
      </div>
    </section>

    <section class="parser-column parser-column-settings">
      <div class="parser-steps-bar" aria-label="Шаги parser pipeline">
        {#each PARSER_EDITOR_STEPS as step}
          <button
            type="button"
            class="parser-step-btn"
            class:is-active={activeStep === step.id}
            aria-pressed={activeStep === step.id}
            on:click={() => (activeStep = step.id)}
          >
            <span class="parser-step-index">{step.order}</span>
            <span>{step.label}</span>
          </button>
        {/each}
      </div>

      <div class="parser-card">
        <div class="parser-card-head">
          <div>
            <h3>Вход parser и preview</h3>
            <p>Parser получает вход из upstream node, который уже показан слева. Здесь задаётся только способ чтения payload и проверка preview.</p>
          </div>
          <button type="button" class="primary-btn" on:click={previewNow} disabled={previewLoading}>
            {previewLoading ? 'Обновление...' : 'Обновить preview'}
          </button>
        </div>

        <div class="parser-pipeline-summary-box">
          <div class="parser-shell-summary">
            <span class="chip-chip readonly-chip">Вход: {parserPipelineModel.inputProfile.inputKind}</span>
            <span class="chip-chip readonly-chip">Основа: {parserPipelineModel.inputProfile.sourceBasis}</span>
            <span class="chip-chip readonly-chip">Разбор: {parserPipelineModel.parseStrategy.strategy}</span>
          </div>
          <div class="preview-meta">
            <span>Payload: {parserPipelineModel.payloadCandidate.candidatePath}</span>
            <span>Набор: {parserPipelineModel.workingSetCandidate.candidateRecordPath}</span>
            <span>Результат: {parserPipelineModel.resultSummary.fieldsCount}</span>
            <span>Warnings: {parserPipelineModel.warningsSummary.total}</span>
          </div>
        </div>

        {#if activeStep === 'input'}
          <div class="form-grid form-grid-1">
            <label>
              Тип источника
              <select value={settings.sourceMode} on:change={(e) => patchSetting('sourceMode', selectValue(e))}>
                <option value="node">Нода</option>
                <option value="table">Таблица</option>
                <option value="file_url">Файл / ссылка</option>
              </select>
            </label>
          </div>

          {#if settings.sourceMode === 'node'}
            <div class="selected-source-box">
              <div><strong>Основной сценарий: вход из upstream node</strong></div>
              <div class="hint">Слева уже показан источник и его контракт. Legacy-шаблон источника ниже нужен только как fallback, если для preview требуется пример входа без живого upstream payload.</div>
            </div>

            <details class="parser-legacy-panel">
              <summary class="parser-legacy-summary">Legacy fallback: шаблон ноды-источника для preview</summary>
              <div class="parser-legacy-body">
                <label>
                  Legacy-шаблон ноды-источника
                  <select value={settings.sourceNodeTemplateRef} on:change={(e) => applySourceNodeTemplateRef(selectValue(e))}>
                    <option value="">Выбери шаблон ноды</option>
                    {#each sourceTemplates as item (item.ref)}
                      <option value={item.ref}>{sourceTemplateLabel(item)}</option>
                    {/each}
                  </select>
                  <span class="hint">Этот выбор не определяет основной parser flow. Он нужен только для fallback preview входа.</span>
                </label>
                {#if sourceTemplatesError}
                  <div class="inline-error">{sourceTemplatesError}</div>
                {:else if sourceTemplatesLoading}
                  <div class="inline-hint">Загрузка шаблонов нод...</div>
                {:else if currentSourceTemplate}
                  <div class="selected-source-box">
                    <div><strong>{sourceTemplateLabel(currentSourceTemplate)}</strong></div>
                    <div class="hint">{currentSourceTemplate.description || 'Описание шаблона не заполнено.'}</div>
                  </div>
                {:else}
                  <div class="inline-hint">Оставь блок пустым, если preview можно строить без legacy fallback-источника.</div>
                {/if}

                <label>
                  Legacy preview входа
                  {#if currentSourceTemplate && sourceNodePreviewJson}
                    <textarea rows="10" readonly value={sourceNodePreviewJson}></textarea>
                    <span class="hint">{sourceNodePreviewMessage}</span>
                  {:else if currentSourceTemplate}
                    <div class="inline-hint">{sourceNodePreviewMessage}</div>
                  {:else}
                    <div class="inline-hint">Выбери legacy-шаблон ноды-источника только если нужен fallback preview входа.</div>
                  {/if}
                </label>
              </div>
            </details>
          {/if}

          {#if settings.sourceMode === 'table'}
            <div class="form-grid form-grid-3">
              <label>
                Схема
                <select value={settings.sourceSchema} on:change={(e) => patchSetting('sourceSchema', selectValue(e))}>
                  <option value="">Выбери схему</option>
                  {#each Array.from(new Set(sourceTableOptions().map((item) => item.schema_name))) as schemaName}
                    <option value={schemaName}>{schemaName}</option>
                  {/each}
                </select>
              </label>
              <label>
                Таблица
                <select value={settings.sourceTable} on:change={(e) => patchSetting('sourceTable', selectValue(e))}>
                  <option value="">Выбери таблицу</option>
                  {#each sourceTableOptions().filter((item) => !settings.sourceSchema || item.schema_name === settings.sourceSchema) as item}
                    <option value={item.table_name}>{item.table_name}</option>
                  {/each}
                </select>
              </label>
              <label>
                Колонка с payload
                <select value={settings.sourceColumn} on:change={(e) => patchSetting('sourceColumn', selectValue(e))}>
                  <option value="">Вся строка таблицы</option>
                  {#each sourceColumnOptions as columnName}
                    <option value={columnName}>{columnName}</option>
                  {/each}
                </select>
              </label>
            </div>
            <div class="inline-hint">{currentTableColumnsHint() || 'Выбери таблицу и колонку, если парсить нужно поле с JSON/CSV внутри таблицы.'}</div>
          {/if}

          {#if settings.sourceMode === 'file_url'}
            <div class="form-grid form-grid-2">
              <label>
                URL / путь к файлу
                <input value={settings.fileUrl} on:input={(e) => patchSetting('fileUrl', inputValue(e))} placeholder="https://... или /path/to/file" />
              </label>
              <label>
                Путь до URL во входе
                <input value={settings.fileUrlPath} on:input={(e) => patchSetting('fileUrlPath', inputValue(e))} placeholder="response.download_url" />
              </label>
            </div>
          {/if}

          {#if previewError}
            <div class="inline-error">{previewError}</div>
          {/if}

          <div class="preview-metrics">
            <span>Сырых строк: {sourcePreviewRowCount}</span>
            <span>Сырых колонок: {sourcePreviewColumnCount}</span>
            <span>Формат: {previewData?.source_format || '-'}</span>
            <span>Источник: {settings.sourceMode === 'node' ? 'Нода' : settings.sourceMode === 'table' ? 'Таблица' : 'Файл / ссылка'}</span>
          </div>

          <div class="preview-meta">
            <span>Источник: {sourcePreviewSourceRef}</span>
            <span>Пакет: {previewData?.batch?.returned_rows ?? 0} / {previewData?.batch?.batch_size ?? '-'}</span>
            <span>Есть ещё данные: {previewData?.batch?.has_more ? 'да' : 'нет'}</span>
            <span>Обновлено: {previewUpdatedAt ? new Date(previewUpdatedAt).toLocaleString('ru-RU') : '-'}</span>
          </div>

          {#if sourcePreviewColumns.length}
            <div class="preview-columns">
              {#each sourcePreviewColumns as column}
                <span>{column}</span>
              {/each}
            </div>
          {/if}
          {#if sourcePreviewRows.length}
            <div class="preview-table-wrap">
              <table class="preview-table">
                <thead>
                  <tr>
                    {#each sourcePreviewColumns as column}
                      <th>{column}</th>
                    {/each}
                  </tr>
                </thead>
                <tbody>
                  {#each sourcePreviewRows as row}
                    <tr>
                      {#each sourcePreviewColumns as column}
                        <td>{typeof row?.[column] === 'object' ? JSON.stringify(row?.[column]) : String(row?.[column] ?? '')}</td>
                      {/each}
                    </tr>
                  {/each}
                </tbody>
              </table>
            </div>
          {:else}
            <div class="empty-box">{settings.sourceMode === 'node' && sourceNodePreviewMessage ? sourceNodePreviewMessage : 'Нет preview входа. Выбери источник и обнови preview.'}</div>
          {/if}
        {:else}
          <div class="parser-step-context-strip">
            <span class="chip-chip readonly-chip">Текущий шаг: {PARSER_EDITOR_STEPS.find((item) => item.id === activeStep)?.label || '—'}</span>
            <span>Источник: {settings.sourceMode === 'node' ? 'upstream node' : settings.sourceMode === 'table' ? 'таблица' : 'файл / ссылка'}</span>
            <span>Preview входа: {sourcePreviewRows.length ? `${sourcePreviewRows.length} строк` : 'без данных'}</span>
            <button type="button" class="mini-btn" on:click={() => (activeStep = 'input')}>Открыть шаг «Вход»</button>
          </div>
        {/if}
      </div>

      <div class="parser-card">
        <div class="parser-card-head">
          <div>
            <h3>Настройка обработки</h3>
            <p>Шаги идут последовательно: разбор входа, приведение полей, присоединение данных из таблицы, фильтры, вычисления, дедупликация, группировка и preview результата.</p>
          </div>
        </div>

        {#if activeStep === 'input'}
          <div class="subsection parser-step-panel">
            <div class="subsection-head">
              <h4>Вход</h4>
            </div>
            <div class="inline-hint inline-hint-box">Parser получает вход из upstream node слева. Здесь показано, насколько хватает текущих contract/sample/preview данных для автоматического старта.</div>
            <div class="preview-metrics">
              <span>Тип входа: {parserPipelineModel.inputProfile.inputKind}</span>
              <span>Основа: {parserPipelineModel.inputProfile.sourceBasis}</span>
              <span>Источников: {parserPipelineModel.inputProfile.upstreamCount}</span>
              <span>Полей контракта: {parserPipelineModel.inputProfile.contractFieldsCount}</span>
            </div>
            <div class="preview-meta">
              <span>Sample raw: {parserPipelineModel.inputProfile.hasSampleRaw ? 'есть' : 'нет'}</span>
              <span>Sample payload: {parserPipelineModel.inputProfile.hasSamplePayload ? 'есть' : 'нет'}</span>
              <span>Sample rows: {parserPipelineModel.inputProfile.hasSampleRows ? 'есть' : 'нет'}</span>
              <span>Вход: {parserPipelineModel.inputProfile.upstreamSummary}</span>
            </div>
            {#if parserPipelineModel.warningsSummary.total}
              <div class="warnings-box">
                {#each parserPipelineModel.warningsSummary.parserWarnings as warning}
                  <div class="warning-line">{warning}</div>
                {/each}
                {#if parserPipelineModel.warningsSummary.insufficientData}
                  <div class="warning-line">Для полной автодетекции пока не хватает sample/preview данных. Parser использует доступный contract и текущие settings.</div>
                {/if}
              </div>
            {:else}
              <div class="inline-hint">Если upstream описан хорошо, шаги ниже можно почти не трогать: parser сам распознает формат, payload и рабочий набор.</div>
            {/if}
          </div>
        {/if}

        {#if activeStep === 'parse'}
          <div class="subsection parser-step-panel">
            <div class="subsection-head">
              <h4>Разбор</h4>
            </div>
            <div class="inline-hint inline-hint-box">Шаг отвечает за то, как parser будет читать вход. Оставь автоопределение, если формат уже распознан корректно.</div>
            <div class="preview-metrics">
              <span>Распознанный формат: {autoParseInfo.detectedFormat}</span>
              <span>Стратегия: {parserPipelineModel.parseStrategy.strategy}</span>
              <span>Режим: {parserPipelineModel.autoManualState.strategy}</span>
            </div>
            {#if parserWarnings.length}
              <div class="warnings-box">
                {#each parserWarnings as warning}
                  <div class="warning-line">{warning}</div>
                {/each}
              </div>
            {:else}
              <div class="inline-hint">Авторазбор умеет распознавать JSON, JSON в строке, CSV, NDJSON, текст и runtime-обёртки. Явное переопределение нужно только когда результат не совпадает с ожиданием.</div>
            {/if}
            <div class="form-grid form-grid-2">
              <label>
                Формат
                <select value={settings.sourceFormat} on:change={(e) => patchSetting('sourceFormat', selectValue(e))}>
                  <option value="auto">Определять автоматически</option>
                  <option value="json">JSON</option>
                  <option value="csv">CSV</option>
                  <option value="ndjson">NDJSON</option>
                  <option value="text">Текст</option>
                  <option value="zip">ZIP</option>
                </select>
                <span class="hint">Явный формат включает manual override для чтения входа.</span>
              </label>
              <label>
                Делимитер CSV
                <input value={settings.csvDelimiter} on:input={(e) => patchSetting('csvDelimiter', inputValue(e))} placeholder="," />
                <span class="hint">Используется только для CSV/TSV-подобного входа.</span>
              </label>
            </div>
            {#if settings.sourceFormat === 'zip'}
              <div class="form-grid form-grid-2">
                <label>
                  Файл внутри ZIP
                  <input value={settings.archiveEntry} on:input={(e) => patchSetting('archiveEntry', inputValue(e))} placeholder="export/data.csv" />
                </label>
                <label>
                  Формат файла внутри ZIP
                  <select value={settings.archiveFormat} on:change={(e) => patchSetting('archiveFormat', selectValue(e))}>
                    <option value="auto">Определять автоматически</option>
                    <option value="csv">CSV</option>
                    <option value="json">JSON</option>
                    <option value="ndjson">NDJSON</option>
                    <option value="text">Текст</option>
                  </select>
                </label>
              </div>
            {/if}
          </div>
        {/if}

        {#if activeStep === 'payload'}
          <div class="subsection parser-step-panel">
            <div class="subsection-head">
              <h4>Payload</h4>
            </div>
            <div class="inline-hint inline-hint-box">На этом шаге parser определяет, где внутри входа лежат полезные данные. Если payload уже в корне, поле можно оставить пустым.</div>
            <div class="preview-metrics">
              <span>Источник payload: {autoParseInfo.payloadOrigin}</span>
              <span>Candidate path: {parserPipelineModel.payloadCandidate.candidatePath}</span>
              <span>Режим: {parserPipelineModel.autoManualState.payload}</span>
            </div>
            <div class="form-grid form-grid-1">
              <label>
                Путь до payload во входе
                <input value={settings.inputPath} on:input={(e) => patchSetting('inputPath', inputValue(e))} placeholder="response.data" />
                <span class="hint">Нужен, если полезный payload лежит глубже входного контракта слева. Пустое значение означает чтение входа целиком.</span>
              </label>
            </div>
          </div>
        {/if}

        {#if activeStep === 'workingSet'}
          <div class="subsection parser-step-panel">
            <div class="subsection-head">
              <h4>Набор</h4>
            </div>
            <div class="inline-hint inline-hint-box">Здесь parser определяет рабочий набор строк, над которым дальше выполняются поля, формулы, фильтры, join и агрегаты.</div>
            <div class="preview-metrics">
              <span>Working set path: {autoParseInfo.workingSetPath}</span>
              <span>Режим: {parserPipelineModel.autoManualState.workingSet}</span>
              <span>Preview строк: {parserPipelineModel.workingSetCandidate.sampleRowsAvailable}</span>
            </div>
            {#if bayesInput?.recommended_candidate}
              <div class="bayes-box">
                <div class="bayes-box-head">
                  <div>
                    <strong>Байесовская рекомендация рабочего набора</strong>
                    <div class="hint">Это quickest path, если parser нашёл лучший candidate и нужен один клик для фиксации.</div>
                  </div>
                  <button type="button" class="mini-btn" on:click={applyBayesWorkingSet}>
                    Применить рекомендацию
                  </button>
                </div>
                <div class="preview-metrics">
                  <span>Рекомендованный узел: {bayesInput.recommended_candidate.path_label || '(корень)'}</span>
                  <span>Вероятность: {formatProbability(bayesInput.recommended_candidate.probability)}</span>
                  <span>Гипотеза: {bayesInput.recommended_candidate.top_hypothesis?.name_ru || '-'}</span>
                </div>
                {#if Array.isArray(bayesInput.recommended_candidate.reasons) && bayesInput.recommended_candidate.reasons.length}
                  <div class="chip-list">
                    {#each bayesInput.recommended_candidate.reasons as reason}
                      <span class="chip-chip">{reason}</span>
                    {/each}
                  </div>
                {/if}
              </div>
            {/if}
            <div class="form-grid form-grid-3">
              <label>
                Путь до массива записей
                <input value={settings.recordPath} on:input={(e) => patchSetting('recordPath', inputValue(e))} placeholder="items.rows" />
                <span class="hint">Укажи путь только если внутри payload записи лежат глубже корня.</span>
              </label>
              <label>
                Размер пакета
                <input type="number" min="1" max="5000" value={settings.batchSize} on:input={(e) => patchSetting('batchSize', inputValue(e))} />
                <span class="hint">Влияет на объём данных в одном проходе preview/runtime.</span>
              </label>
              <label>
                Множитель парсинга
                <input type="number" min="1" value={settings.parserMultiplier} on:input={(e) => patchSetting('parserMultiplier', inputValue(e))} />
                <span class="hint">Нужен только если один входной элемент разворачивается в несколько строк результата.</span>
              </label>
            </div>
          </div>
        {/if}

        {#if activeStep === 'fields'}
          <div class="subsection parser-step-panel">
            <div class="subsection-head">
              <h4>Поля результата</h4>
              <div class="subsection-actions">
                <button type="button" class="mini-btn" on:click={addAllDetectedFields} disabled={!workingFieldCandidates.length}>Взять всё из рабочего набора</button>
                <button type="button" class="mini-btn" on:click={() => rebuildFieldSettings([])} disabled={!selectedFieldRows.length}>Очистить</button>
              </div>
            </div>
            <div class="inline-hint">Здесь фиксируется итоговый набор полей parser. Поле можно добавить прямо из левой колонки или выбрать вручную из текущего рабочего набора.</div>
            <div class="field-picker">
              <select on:change={(e) => { addSelectedField(selectValue(e)); clearSelectValue(e); }}>
                <option value="">Добавить поле в результат</option>
                {#each workingFieldCandidates.filter((item) => !selectedFieldPaths.includes(item)) as fieldName}
                  <option value={fieldName}>{fieldName}</option>
                {/each}
              </select>
            </div>
            <div class="inline-hint">Список берётся из preview результата, уже выбранных полей и зафиксированных путей parser. Клик по полю слева тоже приводит сюда.</div>
            {#if selectedFieldRows.length}
              <div class="rules-grid">
                <div class="rules-grid-head">Поле из рабочего набора</div>
                <div class="rules-grid-head">Имя в результате</div>
                <div class="rules-grid-head">Тип</div>
                <div class="rules-grid-head">Значение по умолчанию</div>
                <div class="rules-grid-head"></div>
                {#each selectedFieldRows as row, index}
                  <select value={row.path} on:change={(e) => updateSelectedFieldRow(index, { path: selectValue(e) })}>
                    {#each fieldOptionsForRow(row) as fieldName}
                      <option value={fieldName}>{fieldName}</option>
                    {/each}
                  </select>
                  <input value={row.alias} on:input={(e) => updateSelectedFieldRow(index, { alias: inputValue(e) })} placeholder="alias" />
                  <select value={row.type} on:change={(e) => updateSelectedFieldRow(index, { type: selectValue(e) })}>
                    <option value="">Без приведения</option>
                    <option value="text">Текст</option>
                    <option value="integer">Целое число</option>
                    <option value="numeric">Число</option>
                    <option value="boolean">Логический</option>
                    <option value="json">JSON</option>
                    <option value="timestamp">Дата и время</option>
                  </select>
                  <input value={row.defaultValue} on:input={(e) => updateSelectedFieldRow(index, { defaultValue: inputValue(e) })} placeholder="по умолчанию" />
                  <button type="button" class="icon-btn danger-icon-btn" on:click={() => removeSelectedField(index)}>x</button>
                {/each}
              </div>
            {:else}
              <div class="inline-hint">Если оставить блок пустым, parser попытается отдать весь рабочий набор. Нажми «Взять всё из рабочего набора» или добавь поле из левой колонки, чтобы явно зафиксировать результат.</div>
            {/if}
          </div>
        {/if}

        {#if activeStep === 'processing'}
        <div class="subsection parser-step-panel">
          <div class="subsection-head">
            <h4>Присоединить данные из таблицы</h4>
            <label class="switch-line">
              <span>Включить присоединение</span>
              <select value={settings.lookupEnabled} on:change={(e) => patchSetting('lookupEnabled', selectValue(e))}>
                <option value="false">Нет</option>
                <option value="true">Да</option>
              </select>
            </label>
          </div>
          {#if settings.lookupEnabled === 'true'}
            <div class="form-grid form-grid-4">
              <label>
                Схема таблицы
                <select value={settings.lookupSchema} on:change={(e) => patchSetting('lookupSchema', selectValue(e))}>
                  <option value="">Выбери схему</option>
                  {#each Array.from(new Set(sourceTableOptions().map((item) => item.schema_name))) as schemaName}
                    <option value={schemaName}>{schemaName}</option>
                  {/each}
                </select>
              </label>
              <label>
                Таблица
                <select value={settings.lookupTable} on:change={(e) => patchSetting('lookupTable', selectValue(e))}>
                  <option value="">Выбери таблицу</option>
                  {#each sourceTableOptions().filter((item) => !settings.lookupSchema || item.schema_name === settings.lookupSchema) as item}
                    <option value={item.table_name}>{item.table_name}</option>
                  {/each}
                </select>
              </label>
              <label>
                Как объединять наборы
                <select value={settings.lookupJoinMode} on:change={(e) => patchSetting('lookupJoinMode', selectValue(e))}>
                  <option value="inner">Только совпавшие строки</option>
                  <option value="left">Все строки текущих данных</option>
                  <option value="right">Все строки таблицы</option>
                  <option value="full">Все строки из обоих наборов</option>
                </select>
              </label>
              <label>
                Префикс для новых полей
                <input value={settings.lookupPrefix} on:input={(e) => patchSetting('lookupPrefix', inputValue(e))} placeholder="lookup_" />
              </label>
            </div>
            <div class="form-grid form-grid-2">
              <label>
                Что делать, если имя поля уже занято
                <select value={settings.lookupConflictMode} on:change={(e) => patchSetting('lookupConflictMode', selectValue(e))}>
                  <option value="suffix">Добавить суффикс</option>
                  <option value="overwrite">Перезаписать текущее поле</option>
                  <option value="skip">Пропустить совпавшее имя</option>
                </select>
              </label>
              <div class="inline-hint inline-hint-box">По умолчанию все условия связи соединяются через И. Сначала выбери таблицу и поля, затем проверь preview результата ниже.</div>
            </div>

            {#if bayesJoin?.suggestions?.length}
              <div class="bayes-box">
                <div class="bayes-box-head">
                  <div>
                    <strong>Байесовская рекомендация полей связи</strong>
                    <div class="hint">Модель оценивает похожесть имён, совместимость типов, заполненность, пересечение значений и риск дублей.</div>
                  </div>
                  <button type="button" class="mini-btn" on:click={applyBayesJoinRules} disabled={!bayesJoin.recommended_rules?.length}>
                    Применить предложенные связи
                  </button>
                </div>
                <div class="rules-grid rules-grid-join-suggestions">
                  <div class="rules-grid-head">Поле текущих данных</div>
                  <div class="rules-grid-head">Поле таблицы</div>
                  <div class="rules-grid-head">Вероятность</div>
                  <div class="rules-grid-head">Почему</div>
                  {#each bayesJoin.suggestions.slice(0, 5) as suggestion}
                    <div>{suggestion.sourceField}</div>
                    <div>{suggestion.targetField}</div>
                    <div>{formatProbability(suggestion.probability)}</div>
                    <div>{Array.isArray(suggestion.reasons) && suggestion.reasons.length ? suggestion.reasons.join(', ') : 'Совместимая пара по наблюдаемым признакам'}</div>
                  {/each}
                </div>
                {#if Array.isArray(bayesJoin.warnings) && bayesJoin.warnings.length}
                  <div class="warnings-box">
                    {#each bayesJoin.warnings as warning}
                      <div class="warning-line">{warning}</div>
                    {/each}
                  </div>
                {/if}
              </div>
            {/if}

            <div class="sub-block">
              <div class="subsection-head">
                <h5>Какие поля подтянуть из таблицы</h5>
                <div class="subsection-actions">
                  <button type="button" class="mini-btn" on:click={addAllLookupFields} disabled={!lookupColumnOptions.length}>Взять все поля</button>
                  <button type="button" class="mini-btn" on:click={clearLookupFields} disabled={!lookupSelectedFields.length}>Очистить</button>
                </div>
              </div>
              {#if lookupColumnOptions.length}
                <div class="field-picker">
                  <select on:change={(e) => { addLookupField(selectValue(e)); clearSelectValue(e); }}>
                    <option value="">Добавить поле из таблицы</option>
                    {#each lookupColumnOptions.filter((item) => !lookupSelectedFields.includes(item)) as fieldName}
                      <option value={fieldName}>{fieldName}</option>
                    {/each}
                  </select>
                </div>
                {#if lookupSelectedFields.length}
                  <div class="chip-list">
                    {#each lookupSelectedFields as fieldName}
                      <button type="button" class="chip-btn" on:click={() => removeLookupField(fieldName)} title="Убрать поле">
                        <span>{fieldName}</span>
                        <span aria-hidden="true">×</span>
                      </button>
                    {/each}
                  </div>
                {:else}
                  <div class="inline-hint">Сначала можно взять все поля таблицы, а потом убрать лишние.</div>
                {/if}
              {:else if settings.lookupSchema && settings.lookupTable}
                <div class="inline-hint">Колонки выбранной таблицы загружаются...</div>
              {:else}
                <div class="inline-hint">Сначала выбери таблицу, чтобы увидеть доступные поля.</div>
              {/if}
            </div>

            <div class="sub-block">
              <div class="subsection-head">
                <h5>Когда считать строки одинаковыми</h5>
                <button type="button" class="mini-btn" on:click={addLookupJoinRule}>Условие +</button>
              </div>
              {#if lookupJoinRules.length}
                <div class="rules-grid rules-grid-join">
                  <div class="rules-grid-head">Поле текущих данных</div>
                  <div class="rules-grid-head">Равно полю таблицы</div>
                  <div class="rules-grid-head"></div>
                  {#each lookupJoinRules as rule, index}
                    <select value={rule.sourceField} on:change={(e) => updateLookupJoinRule(index, { sourceField: selectValue(e) })}>
                      <option value="">Выбери поле</option>
                      {#each workingFieldCandidates as fieldName}
                        <option value={fieldName}>{fieldName}</option>
                      {/each}
                    </select>
                    <select value={rule.targetField} on:change={(e) => updateLookupJoinRule(index, { targetField: selectValue(e) })}>
                      <option value="">Выбери поле таблицы</option>
                      {#each lookupColumnOptions as fieldName}
                        <option value={fieldName}>{fieldName}</option>
                      {/each}
                    </select>
                    <button type="button" class="icon-btn danger-icon-btn" on:click={() => removeLookupJoinRule(index)}>x</button>
                  {/each}
                </div>
              {:else}
                <div class="inline-hint">Добавь хотя бы одно условие, чтобы parser понимал, по каким полям связывать текущие строки и выбранную таблицу.</div>
              {/if}
            </div>

            {#if lookupSummary}
              <div class="lookup-summary">
                <span>Совпало строк: {lookupSummary.matched_source_rows ?? 0}</span>
                <span>Без совпадения в текущих данных: {lookupSummary.unmatched_source_rows ?? 0}</span>
                <span>Без совпадения в таблице: {lookupSummary.unmatched_lookup_rows ?? 0}</span>
                <span>Новых полей: {lookupSummary.added_fields_count ?? 0}</span>
              </div>
            {/if}
          {:else}
            <div class="inline-hint">Этот блок нужен, когда к текущему набору строк нужно присоединить дополнительные данные из выбранной таблицы по понятным условиям связи.</div>
          {/if}
        </div>

        <div class="subsection parser-step-panel">
          <div class="subsection-head">
            <h4>Фильтры</h4>
            <button type="button" class="mini-btn" on:click={addFilterRule}>Фильтр +</button>
          </div>
          {#if filterRules.length}
            <div class="rules-grid rules-grid-filters">
              <div class="rules-grid-head">Поле</div>
              <div class="rules-grid-head">Оператор</div>
              <div class="rules-grid-head">Значение</div>
              <div class="rules-grid-head">Второе значение</div>
              <div class="rules-grid-head"></div>
              {#each filterRules as rule, index}
                <input value={rule.field || ''} on:input={(e) => updateFilterRule(index, { field: inputValue(e) })} placeholder="status" />
                <select value={rule.operator || '='} on:change={(e) => updateFilterRule(index, { operator: selectValue(e) })}>
                  <option value="=">Равно</option>
                  <option value="!=">Не равно</option>
                  <option value=">">Больше</option>
                  <option value=">=">Больше или равно</option>
                  <option value="<">Меньше</option>
                  <option value="<=">Меньше или равно</option>
                  <option value="contains">Содержит</option>
                  <option value="not_contains">Не содержит</option>
                  <option value="empty">Пусто</option>
                  <option value="not_empty">Не пусто</option>
                  <option value="between">Между</option>
                  <option value="in_list">В списке</option>
                </select>
                <input value={rule.value || ''} on:input={(e) => updateFilterRule(index, { value: inputValue(e) })} placeholder="active" />
                <input value={rule.secondValue || ''} on:input={(e) => updateFilterRule(index, { secondValue: inputValue(e) })} placeholder="для между" />
                <button type="button" class="icon-btn danger-icon-btn" on:click={() => removeFilterRule(index)}>x</button>
              {/each}
            </div>
          {:else}
            <div class="inline-hint">Фильтры применяются после вычисляемых полей и могут работать как по исходным, так и по новым колонкам.</div>
          {/if}
        </div>

        <div class="subsection parser-step-panel">
          <div class="subsection-head">
            <h4>Вычисляемые поля</h4>
            <button type="button" class="mini-btn" on:click={addComputedField}>Поле +</button>
          </div>
          <div class="inline-hint">По умолчанию используется конструктор выражений. Ручной ввод оставлен для сложных случаев, когда удобнее написать формулу текстом.</div>
          <details class="computed-help">
            <summary>Справочник функций</summary>
            <div class="computed-help-grid">
              {#each computedFunctionLibrary as category}
                <div class="computed-help-category">
                  <div class="computed-help-title">{category.label}</div>
                  {#each category.items as item}
                    <div class="computed-help-item">
                      <div class="computed-help-name">{item.label}</div>
                      <div class="hint">{item.description}</div>
                      <code>{item.example}</code>
                    </div>
                  {/each}
                </div>
              {/each}
            </div>
          </details>
          {#if computedFields.length}
            <div class="computed-fields-list">
              {#each computedFields as rule, index}
                <div class="computed-field-card">
                  <div class="computed-field-row computed-field-row-main">
                    <label>
                      Новое поле
                      <input value={rule.name || ''} on:input={(e) => updateComputedField(index, { name: inputValue(e) })} placeholder="discount_value" />
                    </label>
                    <label>
                      Режим ввода
                      <select value={rule.mode || 'builder'} on:change={(e) => updateComputedField(index, { mode: selectValue(e) })}>
                        <option value="builder">Конструктор</option>
                        <option value="manual">Ручной ввод</option>
                      </select>
                    </label>
                    <label>
                      Тип результата
                      <select value={rule.type || ''} on:change={(e) => updateComputedField(index, { type: selectValue(e) })}>
                        <option value="">Без приведения</option>
                        <option value="text">Текст</option>
                        <option value="integer">Целое число</option>
                        <option value="numeric">Число</option>
                        <option value="boolean">Логический</option>
                        <option value="timestamp">Дата и время</option>
                      </select>
                    </label>
                    <div class="computed-field-actions">
                      <button type="button" class="mini-btn" on:click={() => duplicateComputedField(index)}>Дублировать</button>
                      <button type="button" class="icon-btn danger-icon-btn" on:click={() => removeComputedField(index)}>x</button>
                    </div>
                  </div>

                  {#if rule.mode === 'builder'}
                    <ComputedExpressionBuilder
                      builder={rule.builder}
                      availableFields={availableComputedFieldsFor(index)}
                      fallbackFields={currentBaseComputedFields(selectedFieldRows, workingFieldCandidates, sourceTableColumnsMeta, sourcePreviewRows, previewResultRows)}
                      fieldTypes={computedFieldTypesFor(index)}
                      on:change={(e) => updateComputedField(index, { builder: e.detail.builder, expression: buildComputedFieldPreview({ ...rule, builder: e.detail.builder, mode: 'builder' }) })}
                    />
                  {:else}
                    <div class="computed-manual-box">
                      <label>
                        Формула
                        <textarea
                          rows="4"
                          value={rule.expression || ''}
                          on:input={(e) => updateComputedField(index, { expression: textareaValue(e) })}
                          placeholder={COMPUTED_EXPRESSION_PLACEHOLDER}
                        ></textarea>
                      </label>
                      <div class="hint">Ручной режим нужен для продвинутых выражений. Поля используй как {"{поле}"}, функции можно посмотреть в справочнике выше.</div>
                      <div class="computed-manual-preview">
                        <div class="computed-manual-preview-label">Предпросмотр выражения</div>
                        <code>{rule.expression || COMPUTED_EXPRESSION_PLACEHOLDER}</code>
                      </div>
                    </div>
                  {/if}
                </div>
              {/each}
            </div>
          {:else}
            <div class="inline-hint">Добавь вычисляемое поле, чтобы собрать выражение через конструктор или перейти в ручной режим.</div>
          {/if}
        </div>

        <div class="subsection parser-step-panel">
          <div class="subsection-head">
            <h4>Удаление дублей</h4>
          </div>
          <div class="form-grid form-grid-4">
            <label>
              Включить
              <select value={settings.dedupeEnabled} on:change={(e) => patchSetting('dedupeEnabled', selectValue(e))}>
                <option value="false">Нет</option>
                <option value="true">Да</option>
              </select>
            </label>
            <label>
              Режим
              <select value={settings.dedupeMode} on:change={(e) => patchSetting('dedupeMode', selectValue(e))}>
                <option value="full_row">Полные дубликаты</option>
                <option value="by_fields">По выбранным полям</option>
              </select>
            </label>
            <label>
              По каким полям
              <input value={settings.dedupeFields} on:input={(e) => patchSetting('dedupeFields', inputValue(e))} placeholder="sku, nm_id" />
            </label>
            <label>
              Какую запись оставить
              <select value={settings.dedupeKeep} on:change={(e) => patchSetting('dedupeKeep', selectValue(e))}>
                <option value="first">Первую</option>
                <option value="last">Последнюю</option>
              </select>
            </label>
          </div>
        </div>

        <div class="subsection parser-step-panel">
          <div class="subsection-head">
            <h4>Группировки и агрегаты</h4>
            <button type="button" class="mini-btn" on:click={addAggregateRule}>Агрегат +</button>
          </div>
          <div class="form-grid form-grid-2">
            <label>
              Включить группировку
              <select value={settings.groupEnabled} on:change={(e) => patchSetting('groupEnabled', selectValue(e))}>
                <option value="false">Нет</option>
                <option value="true">Да</option>
              </select>
            </label>
            <label>
              Поля группировки
              <input value={settings.groupByFields} on:input={(e) => patchSetting('groupByFields', inputValue(e))} placeholder="date, campaign_id" />
            </label>
          </div>
          {#if aggregateRules.length}
            <div class="rules-grid rules-grid-aggregates">
              <div class="rules-grid-head">Поле</div>
              <div class="rules-grid-head">Агрегат</div>
              <div class="rules-grid-head">Имя результата</div>
              <div class="rules-grid-head"></div>
              {#each aggregateRules as rule, index}
                <input value={rule.field || ''} on:input={(e) => updateAggregateRule(index, { field: inputValue(e) })} placeholder="amount" />
                <select value={rule.op || 'count'} on:change={(e) => updateAggregateRule(index, { op: selectValue(e) })}>
                  <option value="count">Количество</option>
                  <option value="sum">Сумма</option>
                  <option value="min">Минимум</option>
                  <option value="max">Максимум</option>
                  <option value="avg">Среднее</option>
                </select>
                <input value={rule.as || ''} on:input={(e) => updateAggregateRule(index, { as: inputValue(e) })} placeholder="total_amount" />
                <button type="button" class="icon-btn danger-icon-btn" on:click={() => removeAggregateRule(index)}>x</button>
              {/each}
            </div>
          {:else}
            <div class="inline-hint">Сначала укажи поля группировки, затем добавь нужные агрегаты: сумма, количество, минимум, максимум или среднее.</div>
          {/if}
        </div>

        <div class="subsection parser-step-panel">
          <div class="subsection-head">
            <h4>Безопасность и runtime</h4>
          </div>
          <div class="form-grid form-grid-3">
            <label>
              Лимит preview
              <input type="number" min="1" max="200" value={settings.previewLimit} on:input={(e) => patchSetting('previewLimit', inputValue(e))} />
            </label>
            <label>
              Лимит JSON в памяти (байт)
              <input type="number" min="65536" value={settings.maxJsonBytes} on:input={(e) => patchSetting('maxJsonBytes', inputValue(e))} />
            </label>
            <label>
              Размер пакета runtime
              <input type="number" min="1" max="5000" value={settings.batchSize} on:input={(e) => patchSetting('batchSize', inputValue(e))} />
            </label>
          </div>
          <div class="inline-hint">Parser обрабатывает данные пакетно внутри одной ноды. Preview ограничен, а основной runtime остаётся пакетным.</div>
        </div>
        {/if}

        {#if activeStep === 'result'}
        <div class="subsection parser-step-panel">
          <div class="subsection-head">
            <h4>Итог результата</h4>
          </div>
          <div class="preview-metrics">
            <span>Поля: {parserPipelineModel.resultSummary.fieldsCount}</span>
            <span>Источник summary: {parserPipelineModel.resultSummary.source}</span>
            <span>Derived preview: {parserPipelineModel.resultSummary.hasDerivedOutputPreview ? 'есть' : 'нет'}</span>
          </div>
          {#if derivedOutputFields.length}
            <div class="preview-columns">
              {#each derivedOutputFields as field}
                <span>{field.name}</span>
              {/each}
            </div>
          {:else}
            <div class="inline-hint">Итоговый output строится из текущих settings parser и preview результата. Если данных мало, справа и здесь остаётся только partial summary.</div>
          {/if}
        </div>

        <div class="subsection parser-step-panel">
          <div class="subsection-head">
            <h4>Результат обработки</h4>
            <button type="button" class="mini-btn" on:click={previewNow} disabled={previewLoading}>
              {previewLoading ? 'Обновление...' : 'Обновить preview'}
            </button>
          </div>
          <div class="preview-metrics">
            <span>Строк: {previewData?.row_count ?? '-'}</span>
            <span>Колонок: {previewData?.column_count ?? '-'}</span>
            <span>Формат: {previewData?.source_format || '-'}</span>
            <span>Источник: {settings.sourceMode === 'node' ? 'Нода' : settings.sourceMode === 'table' ? 'Таблица' : 'Файл / ссылка'}</span>
          </div>
          <div class="preview-meta">
            <span>Пакет: {previewData?.batch?.returned_rows ?? 0} / {previewData?.batch?.batch_size ?? '-'}</span>
            <span>Есть ещё данные: {previewData?.batch?.has_more ? 'да' : 'нет'}</span>
            <span>Обновлено: {previewUpdatedAt ? new Date(previewUpdatedAt).toLocaleString('ru-RU') : '-'}</span>
          </div>
          {#if parserAppliedSteps.length}
            <div class="applied-steps">
              {#each parserAppliedSteps as step}
                <span>{step}</span>
              {/each}
            </div>
          {/if}
          {#if previewData?.columns?.length}
            <div class="preview-columns">
              {#each previewData.columns as column}
                <span>{column}</span>
              {/each}
            </div>
          {/if}
          {#if previewData?.sample_rows?.length}
            <div class="preview-table-wrap">
              <table class="preview-table">
                <thead>
                  <tr>
                    {#each previewData.columns as column}
                      <th>{column}</th>
                    {/each}
                  </tr>
                </thead>
                <tbody>
                  {#each previewData.sample_rows as row}
                    <tr>
                      {#each previewData.columns as column}
                        <td>{typeof row?.[column] === 'object' ? JSON.stringify(row?.[column]) : String(row?.[column] ?? '')}</td>
                      {/each}
                    </tr>
                  {/each}
                </tbody>
              </table>
            </div>
          {:else}
            <div class="empty-box">Нет результата preview. Обнови preview после изменения настроек обработки.</div>
          {/if}
          {#if previewRaw}
            <details class="preview-raw">
              <summary>Показать raw результат parser runtime</summary>
              <pre>{JSON.stringify(previewRaw, null, 2)}</pre>
            </details>
          {/if}
        </div>
        {/if}

        <details class="parser-legacy-panel">
          <summary class="parser-legacy-summary">Legacy: библиотека шаблонов parser</summary>
          <div class="parser-legacy-body">
          <div class="parser-card-head">
            <div>
              <p>Совместимость со старыми desks сохранена, но основной сценарий теперь идёт через node editor текущего рабочего стола.</p>
            </div>
            <button type="button" class="mini-btn" on:click={() => loadDrafts()} disabled={templatesLoading}>
              Обновить
            </button>
          </div>

          <div class="current-template-box">
            <div class="current-template-line">
              <span class="current-template-key">Текущий шаблон:</span>
              <span class="current-template-value">{currentTemplate?.name || 'Шаблон не подключён'}</span>
            </div>
            <div class="current-template-line">
              <span class="current-template-key">Выбран в библиотеке:</span>
              <span class="current-template-value muted">{selectedDraft?.name || 'Ничего не выбрано'}</span>
            </div>
            <div class="current-template-actions">
              <button type="button" class="primary-btn" on:click={applySelectedTemplate} disabled={!canSwitchTemplate}>
                Сменить шаблон
              </button>
              <span class="hint">
                {#if !selectedDraft}
                  Выбери шаблон из библиотеки.
                {:else if selectedIsCurrent}
                  Этот шаблон уже подключён.
                {:else}
                  Готов к перепривязке.
                {/if}
              </span>
            </div>
          </div>

          {#if templatesError}
            <div class="inline-error">{templatesError}</div>
          {/if}

          <div class="template-editor">
            <label>
              Название шаблона
              <input value={templateNameInput} on:input={(e) => (templateNameInput = inputValue(e))} placeholder="Например: JSON список карточек" />
            </label>
            <label>
              Описание
              <textarea rows="3" value={templateDescriptionInput} on:input={(e) => (templateDescriptionInput = textareaValue(e))}></textarea>
            </label>
            <div class="template-editor-actions">
              <button type="button" class="primary-btn" on:click={saveTemplate} disabled={templateSaving}>
                {templateSaving ? 'Сохранение...' : 'Сохранить'}
              </button>
              <button type="button" class="secondary-btn" on:click={resetTemplateDraft}>Новый</button>
              <button type="button" class="secondary-btn danger" on:click={deleteTemplate} disabled={!selectedDraft || templateDeleting}>
                {templateDeleting ? 'Удаление...' : 'Удалить'}
              </button>
            </div>
          </div>

          <label>
            Поиск шаблона
            <input value={templateSearch} on:input={(e) => (templateSearch = inputValue(e))} placeholder="Название или описание" />
          </label>

          <div class="template-list-wrap">
            {#if templatesLoading}
              <div class="empty-box">Загрузка шаблонов...</div>
            {:else if filteredDrafts.length}
              <div class="template-list">
                {#each filteredDrafts as draft (draft.id)}
                  <button
                    type="button"
                    class="template-item"
                    class:is-selected={selectedDraftId === draft.id}
                    class:is-current={currentTemplateStoreId === draft.id}
                    on:click={() => selectDraft(draft)}
                  >
                    <span class="template-item-name">{draft.name}</span>
                    <span class="template-item-desc">{draft.description || 'Без описания'}</span>
                    <span class="template-item-meta">
                      rev {draft.revision}
                      {#if currentTemplateStoreId === draft.id}
                        <strong>подключён</strong>
                      {/if}
                    </span>
                  </button>
                {/each}
              </div>
            {:else}
              <div class="empty-box">Шаблоны parser пока не найдены.</div>
            {/if}
          </div>
          </div>
        </details>
      </div>
    </section>

    <section class="parser-column parser-column-output">
      <div class="parser-shell-card">
        <div class="parser-card-head">
          <div>
            <h3>Исходящие параметры</h3>
            <p>Read-only derived preview результата для следующей ноды. Правая колонка не вводит новую schema и не даёт редактирование.</p>
          </div>
        </div>
        {#if derivedOutputFields.length}
          <div class="parser-shell-summary">
            <span class="chip-chip readonly-chip">{derivedOutputFields.length} {derivedOutputFields.length === 1 ? 'поле' : derivedOutputFields.length < 5 ? 'поля' : 'полей'}</span>
            {#if derivedOutputSourceLabel}
              <span class="inline-hint">{derivedOutputSourceLabel}</span>
            {/if}
          </div>
          <div class="parser-contract-list">
            {#each derivedOutputFields as field}
              <div class="parser-contract-item">
                <div class="parser-contract-name">{field.name}</div>
                <div class="parser-contract-meta">
                  {#if field.alias}
                    <span>Alias: {field.alias}</span>
                  {/if}
                  {#if field.type}
                    <span>Тип: {field.type}</span>
                  {/if}
                  {#if field.path}
                    <span>Path: {field.path}</span>
                  {/if}
                  <span>Источник: {field.source === 'preview' ? 'preview результата' : 'settings parser'}</span>
                </div>
              </div>
            {/each}
          </div>
        {:else}
          <div class="inline-hint">Derived output preview пока недоступен: parser ещё не зафиксировал поля результата и preview не вернул итоговые колонки.</div>
        {/if}
      </div>
    </section>
  </div>
</section>

<style>
  .panel {
    min-width: 0;
    display: flex;
    flex-direction: column;
    gap: 12px;
  }
  .panel-embedded {
    min-height: 0;
  }
  .parser-layout {
    display: grid;
    grid-template-columns: minmax(280px, 0.9fr) minmax(520px, 1.5fr) minmax(300px, 0.95fr);
    gap: 14px;
    min-width: 0;
    align-items: start;
  }
  .parser-column {
    min-width: 0;
    display: flex;
    flex-direction: column;
    gap: 14px;
  }
  .parser-steps-bar {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    align-items: center;
  }
  .parser-step-btn {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 8px 10px;
    border-radius: 999px;
    border: 1px solid #dbe4f0;
    background: #fff;
    color: #334155;
    font-size: 12px;
    font-weight: 600;
    cursor: pointer;
    transition: border-color 0.15s ease, background 0.15s ease, color 0.15s ease;
  }
  .parser-step-btn.is-active {
    background: #0f172a;
    border-color: #0f172a;
    color: #fff;
  }
  .parser-step-index {
    display: inline-flex;
    width: 20px;
    height: 20px;
    align-items: center;
    justify-content: center;
    border-radius: 999px;
    background: #e2e8f0;
    color: #0f172a;
    font-size: 11px;
    font-weight: 700;
  }
  .parser-step-btn.is-active .parser-step-index {
    background: rgba(255, 255, 255, 0.18);
    color: #fff;
  }
  .parser-shell-card {
    border: 1px solid #dbe4f0;
    border-radius: 12px;
    background: #fff;
    padding: 12px;
    display: flex;
    flex-direction: column;
    gap: 10px;
    min-width: 0;
  }
  .parser-card {
    border: 1px solid #dbe4f0;
    border-radius: 12px;
    background: #fff;
    padding: 12px;
    display: flex;
    flex-direction: column;
    gap: 12px;
    min-width: 0;
  }
  .parser-card-head {
    display: flex;
    justify-content: space-between;
    gap: 12px;
    align-items: flex-start;
  }
  .parser-card-head h3 {
    margin: 0;
    font-size: 18px;
    color: #0f172a;
  }
  .parser-card-head p {
    margin: 4px 0 0;
    font-size: 12px;
    color: #64748b;
    line-height: 1.4;
  }
  .parser-shell-list {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  .parser-shell-summary {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    align-items: center;
  }
  .parser-pipeline-summary-box {
    margin-bottom: 12px;
    padding: 10px 12px;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    background: #f8fafc;
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  .parser-shell-item {
    display: flex;
    flex-direction: column;
    gap: 4px;
    padding: 8px 10px;
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    background: #f8fafc;
  }
  .parser-shell-item strong {
    font-size: 12px;
    color: #0f172a;
  }
  .parser-shell-item span {
    font-size: 11px;
    color: #64748b;
  }
  .parser-shell-meta {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
  }
  .parser-shell-description {
    font-size: 11px;
    line-height: 1.45;
    color: #475569;
  }
  .parser-contract-block {
    margin-top: 4px;
    padding-top: 8px;
    border-top: 1px dashed #dbe4f0;
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  .parser-contract-head {
    display: flex;
    justify-content: space-between;
    gap: 8px;
    flex-wrap: wrap;
    font-size: 11px;
    color: #475569;
  }
  .parser-contract-list {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  .parser-contract-item {
    border: 1px solid #e2e8f0;
    border-radius: 8px;
    background: #fff;
    padding: 8px;
    display: flex;
    flex-direction: column;
    gap: 4px;
  }
  .parser-contract-item.is-selected {
    border-color: #c7d2fe;
    background: #f8faff;
  }
  .parser-contract-name {
    font-size: 12px;
    font-weight: 600;
    color: #0f172a;
    word-break: break-word;
  }
  .parser-contract-meta {
    display: flex;
    flex-direction: column;
    gap: 2px;
    font-size: 11px;
    color: #64748b;
    word-break: break-word;
  }
  .parser-contract-actions {
    margin-top: 4px;
    display: flex;
    justify-content: flex-start;
  }
  .parser-contract-add-btn {
    padding: 6px 10px;
  }
  .parser-contract-fallback {
    font-size: 11px;
    line-height: 1.45;
    color: #64748b;
  }
  .parser-step-context-strip {
    display: flex;
    flex-wrap: wrap;
    gap: 8px 12px;
    align-items: center;
    padding: 10px 12px;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    background: #f8fafc;
    font-size: 12px;
    color: #334155;
  }
  .parser-legacy-panel {
    border: 1px dashed #dbe4f0;
    border-radius: 12px;
    background: #f8fafc;
    padding: 0;
  }
  .parser-legacy-summary {
    cursor: pointer;
    list-style: none;
    padding: 12px 14px;
    font-size: 12px;
    font-weight: 700;
    color: #334155;
  }
  .parser-legacy-summary::-webkit-details-marker {
    display: none;
  }
  .parser-legacy-body {
    padding: 0 14px 14px;
    display: flex;
    flex-direction: column;
    gap: 12px;
  }
  .primary-btn,
  .secondary-btn,
  .mini-btn {
    border-radius: 10px;
    border: 1px solid #dbe4f0;
    padding: 8px 12px;
    font-size: 12px;
    cursor: pointer;
    transition: background 0.15s ease, color 0.15s ease, border-color 0.15s ease;
    white-space: nowrap;
  }
  .primary-btn {
    background: #0f172a;
    color: #fff;
    border-color: #0f172a;
  }
  .secondary-btn,
  .mini-btn {
    background: #fff;
    color: #334155;
  }
  .secondary-btn.danger {
    color: #b91c1c;
    border-color: #fecaca;
  }
  .primary-btn:disabled,
  .secondary-btn:disabled,
  .mini-btn:disabled {
    opacity: 0.55;
    cursor: not-allowed;
  }
  .preview-metrics,
  .preview-meta {
    display: flex;
    flex-wrap: wrap;
    gap: 6px 12px;
    font-size: 12px;
    color: #334155;
  }
  .preview-columns {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
  }
  .preview-columns span {
    padding: 4px 8px;
    border-radius: 999px;
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    font-size: 11px;
    color: #334155;
  }
  .preview-table-wrap {
    overflow: auto;
    border: 1px solid #e2e8f0;
    border-radius: 10px;
  }
  .preview-table {
    width: 100%;
    border-collapse: collapse;
    min-width: 520px;
  }
  .preview-table th,
  .preview-table td {
    padding: 8px;
    border-bottom: 1px solid #edf2f7;
    text-align: left;
    font-size: 12px;
    vertical-align: top;
  }
  .preview-table th {
    background: #f8fafc;
    color: #334155;
  }
  .preview-table td {
    color: #0f172a;
    max-width: 280px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .preview-raw {
    border-top: 1px dashed #dbe4f0;
    padding-top: 10px;
  }
  .preview-raw summary {
    cursor: pointer;
    color: #334155;
    font-size: 12px;
  }
  .preview-raw pre {
    margin: 10px 0 0;
    padding: 10px;
    border-radius: 10px;
    background: #0f172a;
    color: #e2e8f0;
    overflow: auto;
    font-size: 11px;
  }
  .form-grid {
    display: grid;
    gap: 10px;
    min-width: 0;
  }
  .form-grid-1 {
    grid-template-columns: 1fr;
  }
  .form-grid-2 {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
  .form-grid-3 {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
  .form-grid-4 {
    grid-template-columns: repeat(4, minmax(0, 1fr));
  }
  label {
    display: flex;
    flex-direction: column;
    gap: 6px;
    min-width: 0;
    font-size: 12px;
    color: #334155;
  }
  input,
  textarea,
  select {
    width: 100%;
    min-width: 0;
    border: 1px solid #dbe4f0;
    border-radius: 10px;
    padding: 8px 10px;
    font-size: 13px;
    color: #0f172a;
    background: #fff;
    box-sizing: border-box;
  }
  textarea {
    resize: vertical;
  }
  .hint,
  .inline-hint {
    color: #64748b;
    font-size: 11px;
    line-height: 1.4;
  }
  .inline-hint-box {
    display: flex;
    align-items: center;
    border: 1px dashed #dbe4f0;
    border-radius: 10px;
    padding: 8px 10px;
    background: #f8fafc;
  }
  .inline-error {
    border: 1px solid #fecaca;
    background: #fff1f2;
    color: #b91c1c;
    border-radius: 10px;
    padding: 10px;
    font-size: 12px;
  }
  .subsection {
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 12px;
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  .parser-step-panel {
    background: #fcfdff;
  }
  .subsection-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
  }
  .subsection-actions {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
  }
  .subsection-head h4 {
    margin: 0;
    font-size: 14px;
    color: #0f172a;
  }
  .subsection-head h5 {
    margin: 0;
    font-size: 13px;
    color: #0f172a;
  }
  .sub-block {
    border-top: 1px dashed #e2e8f0;
    padding-top: 10px;
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  .warnings-box,
  .applied-steps {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
  }
  .warning-line,
  .applied-steps span {
    padding: 4px 8px;
    border-radius: 999px;
    font-size: 11px;
    line-height: 1.3;
  }
  .warning-line {
    background: #fff7ed;
    border: 1px solid #fed7aa;
    color: #9a3412;
  }
  .applied-steps span {
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    color: #334155;
  }
  .switch-line {
    flex-direction: row;
    align-items: center;
    gap: 8px;
    font-size: 12px;
  }
  .field-picker {
    display: flex;
    gap: 8px;
    align-items: center;
  }
  .rules-grid {
    display: grid;
    gap: 8px;
    align-items: center;
  }
  .rules-grid-filters {
    grid-template-columns: minmax(0, 1fr) minmax(170px, 0.9fr) minmax(0, 1fr) minmax(0, 1fr) auto;
  }
  .rules-grid-join {
    grid-template-columns: minmax(0, 1fr) minmax(0, 1fr) auto;
  }
  .rules-grid-computed {
    grid-template-columns: minmax(170px, 0.8fr) minmax(0, 1.5fr) minmax(150px, 0.6fr) auto;
  }
  .rules-grid-aggregates {
    grid-template-columns: minmax(0, 1fr) minmax(170px, 0.8fr) minmax(0, 1fr) auto;
  }
  .rules-grid:not(.rules-grid-filters):not(.rules-grid-computed):not(.rules-grid-aggregates) {
    grid-template-columns: minmax(0, 1.1fr) minmax(0, 1fr) minmax(170px, 0.6fr) minmax(0, 1fr) auto;
  }
  .rules-grid-head {
    font-size: 11px;
    color: #64748b;
    font-weight: 600;
  }
  .danger-icon-btn {
    color: #b91c1c;
    border-color: #fecaca;
    width: 34px;
    height: 34px;
    border-radius: 10px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
  }
  .current-template-box,
  .selected-source-box,
  .template-editor {
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 10px;
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  .selected-source-box {
    background: #f8fafc;
  }
  .current-template-line {
    display: flex;
    flex-direction: column;
    gap: 2px;
  }
  .current-template-key {
    font-size: 11px;
    color: #64748b;
  }
  .current-template-value {
    font-size: 13px;
    font-weight: 600;
    color: #0f172a;
  }
  .current-template-value.muted {
    font-weight: 500;
    color: #334155;
  }
  .current-template-actions,
  .template-editor-actions {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    align-items: center;
  }
  .template-list-wrap {
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    min-height: 220px;
    max-height: 520px;
    overflow: auto;
    background: #f8fafc;
  }
  .template-list {
    display: flex;
    flex-direction: column;
    gap: 8px;
    padding: 10px;
  }
  .template-item {
    text-align: left;
    border: 1px solid #dbe4f0;
    border-radius: 12px;
    padding: 10px;
    background: #fff;
    display: flex;
    flex-direction: column;
    gap: 4px;
    cursor: pointer;
  }
  .template-item.is-selected {
    border-color: #2563eb;
    box-shadow: 0 0 0 1px rgba(37, 99, 235, 0.18);
  }
  .template-item.is-current {
    background: #eff6ff;
  }
  .template-item-name {
    font-size: 13px;
    font-weight: 600;
    color: #0f172a;
  }
  .template-item-desc {
    font-size: 11px;
    color: #64748b;
  }
  .template-item-meta {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    font-size: 10px;
    color: #475569;
  }
  .empty-box {
    padding: 14px;
    font-size: 12px;
    color: #64748b;
  }
  .chip-list {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
  }
  .chip-chip {
    border: 1px solid #dbe4f0;
    background: #fff;
    border-radius: 999px;
    padding: 5px 10px;
    display: inline-flex;
    align-items: center;
    font-size: 11px;
    color: #334155;
  }
  .chip-btn {
    border: 1px solid #dbe4f0;
    background: #fff;
    border-radius: 999px;
    padding: 6px 10px;
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-size: 11px;
    color: #334155;
    cursor: pointer;
  }
  .lookup-summary {
    display: flex;
    flex-wrap: wrap;
    gap: 8px 12px;
    padding: 10px;
    border-radius: 10px;
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    font-size: 11px;
    color: #334155;
  }
  .computed-help {
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    background: #f8fafc;
    padding: 10px 12px;
  }
  .computed-help-grid {
    margin-top: 10px;
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 12px;
  }
  .computed-help-category,
  .computed-field-card,
  .computed-manual-box {
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  .computed-help-category {
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    background: #fff;
    padding: 10px;
  }
  .computed-help-title,
  .computed-manual-preview-label {
    font-size: 12px;
    font-weight: 700;
    color: #0f172a;
  }
  .computed-help-item {
    display: flex;
    flex-direction: column;
    gap: 4px;
    padding-top: 8px;
    border-top: 1px dashed #e2e8f0;
  }
  .computed-help-item:first-of-type {
    border-top: 0;
    padding-top: 0;
  }
  .computed-help-name {
    font-size: 12px;
    font-weight: 600;
    color: #0f172a;
  }
  .computed-help-item code,
  .computed-manual-preview code {
    white-space: pre-wrap;
    word-break: break-word;
    font-size: 12px;
    color: #334155;
  }
  .computed-fields-list {
    display: flex;
    flex-direction: column;
    gap: 12px;
  }
  .computed-field-card {
    border: 1px solid #e2e8f0;
    border-radius: 14px;
    background: #fff;
    padding: 12px;
  }
  .computed-field-row {
    display: grid;
    gap: 10px;
    align-items: start;
  }
  .computed-field-row-main {
    grid-template-columns: minmax(180px, 1fr) 180px 180px auto;
  }
  .computed-field-actions {
    display: flex;
    align-items: end;
    gap: 8px;
  }
  .computed-manual-preview {
    border: 1px solid #dbe4f0;
    border-radius: 10px;
    background: #f8fafc;
    padding: 10px;
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  .bayes-box {
    border: 1px solid #dbe4f0;
    border-radius: 12px;
    background: #f8fafc;
    padding: 12px;
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  .bayes-box-head {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 10px;
  }
  .rules-grid-join-suggestions {
    grid-template-columns: minmax(0, 1fr) minmax(0, 1fr) 120px minmax(0, 2fr);
  }
  @media (max-width: 1380px) {
    .parser-layout {
      grid-template-columns: minmax(260px, 0.9fr) minmax(420px, 1.25fr) minmax(280px, 0.95fr);
    }
    .form-grid-3,
    .form-grid-4 {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .rules-grid,
    .rules-grid-filters,
    .rules-grid-computed,
    .rules-grid-aggregates,
    .rules-grid-join-suggestions {
      grid-template-columns: 1fr;
    }
    .computed-help-grid,
    .computed-field-row-main {
      grid-template-columns: 1fr;
    }
  }
  @media (max-width: 1120px) {
    .parser-layout {
      grid-template-columns: 1fr;
    }
    .form-grid-2,
    .form-grid-3,
    .form-grid-4 {
      grid-template-columns: 1fr;
    }
    .rules-grid,
    .rules-grid-filters,
    .rules-grid-computed,
    .rules-grid-aggregates,
    .rules-grid-join-suggestions {
      grid-template-columns: 1fr;
    }
    .computed-help-grid,
    .computed-field-row-main {
      grid-template-columns: 1fr;
    }
  }
</style>

