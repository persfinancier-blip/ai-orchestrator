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
  import {
    descriptorFieldKey,
    descriptorFields,
    descriptorOutputKindLabel,
    descriptorSampleColumns,
    descriptorSampleRows,
    type NodeDescriptor,
    type NodeDescriptorField,
    type NodeDescriptorFlow
  } from '../data/nodeDescriptorFlow';

  type ExistingTable = { schema_name: string; table_name: string };
  type ColumnMeta = { name: string; type?: string };
  type ParserSettings = Record<string, string>;
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
  type ParserDerivedOutputField = {
    name: string;
    alias?: string;
    type?: string;
    path?: string;
    source: 'preview' | 'settings';
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
  type ParserDetectStatus = 'auto' | 'partial' | 'unknown';
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
  type ParserEditorStep = 'input' | 'fields' | 'result';
  type ParserChangeStep = 'incoming' | 'lookup' | 'filters' | 'computed' | 'dedupe' | 'grouping';

  export let apiBase: string;
  export let apiJson: <T = any>(url: string, init?: RequestInit) => Promise<T>;
  export let headers: () => Record<string, string>;
  export let existingTables: ExistingTable[] = [];
  export let initialSettings: Record<string, any> = {};
  export let embeddedMode = false;
  export let incomingDescriptor: NodeDescriptorFlow | null = null;
  export let outputDescriptor: NodeDescriptor | null = null;

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
    { id: 'input', label: 'Определение данных', order: '1' },
    { id: 'fields', label: 'Изменение данных', order: '2' },
    { id: 'result', label: 'Результат', order: '3' }
  ];
  const PARSER_CHANGE_STEPS: Array<{ id: ParserChangeStep; label: string }> = [
    { id: 'incoming', label: 'Входящие данные' },
    { id: 'lookup', label: 'Присоединить данные из таблицы' },
    { id: 'filters', label: 'Фильтры' },
    { id: 'computed', label: 'Вычисляемые поля' },
    { id: 'dedupe', label: 'Удаление дублей' },
    { id: 'grouping', label: 'Группировки и агрегаты' }
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
        candidatePath: '-',
        mode: 'unknown',
        hasPayloadSample: false
      },
      workingSetCandidate: {
        candidateRecordPath: '-',
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
  let columnsCache: Record<string, ColumnMeta[]> = {};
  let previewLoading = false;
  let previewError = '';
  let previewData: ParserPreview | null = null;
  let previewRaw: any = null;
  let previewUpdatedAt = '';
  let parserPipelineModel: ParserPipelineViewModel = emptyParserPipelineModel();
  let activeStep: ParserEditorStep = 'input';
  let activeChangeStep: ParserChangeStep = 'incoming';
  let detectOverrideOpen = false;
  let payloadManualInputOpen = false;
  let workingSetManualInputOpen = false;

  $: {
    const next = normalizeSettings(initialSettings);
    const signature = JSON.stringify(next);
    if (signature !== lastInitialSignature) {
      lastInitialSignature = signature;
      settings = next;
    }
  }

  $: incomingSamplePreview = buildIncomingSamplePreview(incomingDescriptor);
  $: sourcePreviewColumns =
    Array.isArray(previewData?.raw_columns) && previewData.raw_columns.length
      ? previewData.raw_columns
      : incomingSamplePreview.columns;
  $: sourcePreviewRows =
    Array.isArray(previewData?.raw_sample_rows) && previewData.raw_sample_rows.length
      ? previewData.raw_sample_rows
      : incomingSamplePreview.rows;
  $: sourcePreviewRowCount =
    Array.isArray(previewData?.raw_sample_rows) && previewData.raw_sample_rows.length
      ? previewData?.raw_row_count ?? previewData.raw_sample_rows.length
      : incomingSamplePreview.rowCount;
  $: sourcePreviewColumnCount =
    Array.isArray(previewData?.raw_columns) && previewData.raw_columns.length
      ? previewData?.raw_column_count ?? previewData.raw_columns.length
      : incomingSamplePreview.columnCount;
  $: sourcePreviewSourceRef =
    String(previewData?.source_ref || '').trim() || incomingSamplePreview.sourceRef || '-';
  $: sourcePreviewEmptyMessage = incomingSamplePreview.emptyMessage || 'Upstream источник уже определён, но sample snapshot для preview пока недоступен.';
  $: incomingContractFieldCandidates = Array.from(
    new Set(
      incomingNodes
        .flatMap((item) => incomingContractFields(item))
        .map((field) => descriptorFieldKey(field))
        .filter(Boolean)
    )
  );
  $: workingFieldCandidates = Array.from(
    new Set([
      ...(Array.isArray(previewData?.columns) ? previewData.columns : []),
      ...sourcePreviewColumns,
      ...incomingContractFieldCandidates,
      ...parseCsvText(settings.selectFields),
      ...parseCsvText(settings.groupByFields),
      ...parseCsvText(settings.dedupeFields)
    ].filter(Boolean))
  );
  $: hasIncomingUpstream = incomingNodes.length > 0;
  $: autoSourceDefined = hasIncomingUpstream;
  $: legacyStandaloneSourceMode = !hasIncomingUpstream && settings.sourceMode !== 'node';
  $: legacyStandaloneSourceLabel =
    settings.sourceMode === 'table' ? 'table source' : settings.sourceMode === 'file_url' ? 'file/link source' : 'node';
  $: hasDetectManualOverrides =
    parserPipelineModel.autoManualState.strategy === 'manual' ||
    parserPipelineModel.autoManualState.payload === 'manual' ||
    parserPipelineModel.autoManualState.workingSet === 'manual';
  $: showDetectOverride = detectOverrideOpen;
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
  $: primaryIncomingDescriptor = Array.isArray(incomingNodes) && incomingNodes.length ? incomingNodes[0] : null;
  $: incomingDetection = primaryIncomingDescriptor?.detection && typeof primaryIncomingDescriptor.detection === 'object' ? primaryIncomingDescriptor.detection : {};
  $: autoParseInfo = {
    detectedFormat: String(previewData?.stats?.detected_format || previewData?.source_format || incomingDetection?.detectedFormat || '-'),
    payloadOrigin: String(previewData?.stats?.payload_origin || incomingDetection?.readMode || '-'),
    inputKind: String(previewData?.stats?.input_kind || primaryIncomingDescriptor?.outputKind || '-'),
    workingSetPath: String(previewData?.stats?.working_set_path || incomingDetection?.recordPath || '-'),
    sourcePath: String(previewData?.stats?.source_path || incomingDetection?.payloadPath || '-')
  };
  $: sourceTableColumnsMeta = columnsCache[tableCacheKey(settings.sourceSchema, settings.sourceTable)] || [];
  $: previewResultRows = Array.isArray(previewData?.sample_rows) ? previewData.sample_rows : [];
  $: previewResultColumns = Array.isArray(previewData?.columns) ? previewData.columns : [];
  $: incomingNodes = Array.isArray(incomingDescriptor?.upstreamDescriptors) ? incomingDescriptor.upstreamDescriptors : [];
  $: incomingDescriptorNodeId = String(incomingDescriptor?.nodeId || '').trim();
  $: derivedOutputFields = buildDerivedOutputFields();
  $: publishedDescriptorFields = descriptorFields(outputDescriptor);
  $: outputDescriptorDetection = outputDescriptor?.detection && typeof outputDescriptor.detection === 'object' ? outputDescriptor.detection : {};
  $: outputDescriptorKind = String(outputDescriptor?.outputKind || 'unknown').trim() || 'unknown';
  $: outputDescriptorKindLabelValue = descriptorOutputKindLabel(outputDescriptor?.outputKind || 'unknown');
  $: derivedOutputSourceLabel =
    Array.isArray(previewData?.columns) && previewData.columns.length
      ? 'По результату preview'
      : derivedOutputFields.length
      ? 'По текущим settings parser'
      : '';
  $: parserPipelineModel = buildParserPipelineViewModel();
  $: detectedInputLabel = humanizeDetectedInput(parserPipelineModel.inputProfile.inputKind, autoParseInfo.detectedFormat, settings.sourceMode);
  $: detectedReadLabel = humanizeReadMode(parserPipelineModel.parseStrategy.strategy, autoParseInfo.detectedFormat, settings.sourceMode);
  $: payloadAutoPathOptions = uniqueStringList([
    String(autoParseInfo.sourcePath || ''),
    ...incomingNodes.map((item) => String(incomingSampleMeta(item)?.payloadPath || ''))
  ]).filter((item) => item && !['-', '(вход целиком)'].includes(item));
  $: workingSetAutoPathOptions = uniqueStringList([
    String(autoParseInfo.workingSetPath || ''),
    String(bayesInput?.recommended_candidate?.path || ''),
    ...(Array.isArray(bayesInput?.alternatives) ? bayesInput.alternatives.map((item: any) => String(item?.path || '')) : [])
  ]).filter((item) => item && !['-', '(корень)'].includes(item));
  $: archiveEntryOptions = uniqueStringList(
    Array.isArray(previewData?.stats?.archive_entries)
      ? previewData.stats.archive_entries
      : Array.isArray(previewData?.stats?.archiveEntries)
      ? previewData.stats.archiveEntries
      : []
  );
  $: detectedCandidateFieldsCount = workingFieldCandidates.length;
  $: detectedCandidateFieldsSource =
    Array.isArray(previewData?.columns) && previewData.columns.length && (sourcePreviewColumns.length || incomingContractFieldCandidates.length)
      ? 'mixed'
      : Array.isArray(previewData?.columns) && previewData.columns.length
      ? 'preview'
      : sourcePreviewColumns.length && incomingContractFieldCandidates.length
      ? 'mixed'
      : sourcePreviewColumns.length
      ? 'preview'
      : incomingContractFieldCandidates.length
      ? 'upstream contract'
      : selectedFieldPaths.length
      ? 'settings'
      : 'unknown';
  $: detectStatus = (() => {
    const states = [
      parserPipelineModel.autoManualState.strategy,
      parserPipelineModel.autoManualState.payload,
      parserPipelineModel.autoManualState.workingSet
    ];
    if (states.every((item) => item === 'auto')) return 'auto' as ParserDetectStatus;
    if (states.every((item) => item === 'unknown')) return 'unknown' as ParserDetectStatus;
    return 'partial' as ParserDetectStatus;
  })();
  $: detectStatusLabel =
    detectStatus === 'auto' ? 'Определено автоматически' : detectStatus === 'partial' ? 'Определено частично' : 'Не определено';
  $: readModeLabel =
    parserPipelineModel.parseStrategy.manualOverride && settings.sourceFormat !== 'auto'
      ? `Ручной override: ${String(settings.sourceFormat || '').toUpperCase()}`
      : parserPipelineModel.parseStrategy.strategy !== 'unknown'
      ? `Определено автоматически: ${detectedReadLabel}`
      : 'Не определено автоматически';
  $: payloadManualValue = String(settings.inputPath || '').trim();
  $: workingSetManualValue = String(settings.recordPath || '').trim();
  $: payloadUsesSavedOverride = Boolean(payloadManualValue) && !payloadAutoPathOptions.includes(payloadManualValue);
  $: workingSetUsesSavedOverride = Boolean(workingSetManualValue) && !workingSetAutoPathOptions.includes(workingSetManualValue);
  $: payloadHasAutoCandidate =
    hasResolvedAutoPath(parserPipelineModel.payloadCandidate.candidatePath, '(вход целиком)') ||
    payloadAutoPathOptions.length > 0;
  $: workingSetHasAutoCandidate =
    hasResolvedAutoPath(parserPipelineModel.workingSetCandidate.candidateRecordPath, '(корень)') ||
    workingSetAutoPathOptions.length > 0;
  $: payloadResolutionLabel =
    payloadUsesSavedOverride
      ? `Ручной override: ${payloadManualValue}`
      : payloadHasAutoCandidate
      ? `Найден candidate: ${normalizeAutoPathLabel(parserPipelineModel.payloadCandidate.candidatePath, '(вход целиком)', 'Корень входа')}`
      : 'Не определено автоматически';
  $: workingSetResolutionLabel =
    workingSetUsesSavedOverride
      ? `Ручной override: ${workingSetManualValue}`
      : workingSetHasAutoCandidate
      ? `Найден candidate: ${normalizeAutoPathLabel(parserPipelineModel.workingSetCandidate.candidateRecordPath, '(корень)', 'Корень данных')}`
      : 'Не определено автоматически';
  $: payloadCandidateLabel =
    payloadUsesSavedOverride
      ? `Ручной override: ${payloadManualValue}`
      : payloadHasAutoCandidate
      ? normalizeAutoPathLabel(parserPipelineModel.payloadCandidate.candidatePath, '(вход целиком)', 'Корень входа')
      : 'Не определено';
  $: workingSetCandidateLabel =
    workingSetUsesSavedOverride
      ? `Ручной override: ${workingSetManualValue}`
      : workingSetHasAutoCandidate
      ? normalizeAutoPathLabel(parserPipelineModel.workingSetCandidate.candidateRecordPath, '(корень)', 'Корень данных')
      : 'Не определено';
  $: payloadSelectedCandidateValue = payloadAutoPathOptions.includes(payloadManualValue) ? payloadManualValue : '';
  $: workingSetSelectedCandidateValue = workingSetAutoPathOptions.includes(workingSetManualValue) ? workingSetManualValue : '';
  $: showPayloadManualInput = showDetectOverride && (payloadManualInputOpen || payloadUsesSavedOverride || !payloadAutoPathOptions.length);
  $: showWorkingSetManualInput = showDetectOverride && (workingSetManualInputOpen || workingSetUsesSavedOverride || !workingSetAutoPathOptions.length);
  $: shouldShowCsvOverride = showDetectOverride && (settings.sourceFormat === 'csv' || String(autoParseInfo.detectedFormat || '').trim().toLowerCase() === 'csv');
  $: shouldShowZipOverride = showDetectOverride && (settings.sourceFormat === 'zip' || String(autoParseInfo.detectedFormat || '').trim().toLowerCase() === 'zip');
  $: computedFunctionLibrary = COMPUTED_FUNCTION_CATEGORIES.map((category) => ({
    ...category,
    items: COMPUTED_FUNCTIONS.filter((item) => item.category === category.id)
  }));
  function dispatchSettings(next: ParserSettings) {
    settings = cloneSettings(next);
    dispatch('configChange', { settings: settings });
  }

  function patchSetting(key: string, value: string) {
    const next = cloneSettings(settings);
    next[key] = value;
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

  async function previewNow(options: { silentMissingInput?: boolean } = {}) {
    previewLoading = true;
    previewError = '';
    try {
      let inputValue = settings.sampleInput;
      if (settings.sourceMode === 'node') {
        const sampleNode = incomingSampleNode();
        const sampleInput = inputValueFromIncomingSample(sampleNode);
        if (sampleInput) {
          inputValue = sampleInput;
        } else if (String(inputValue || '').trim()) {
          inputValue = String(inputValue || '').trim();
        } else {
          if (!options.silentMissingInput) {
            previewError = 'Upstream источник уже определён, но sample snapshot для preview пока недоступен. Для preview нужны sample-данные или явный ручной override.';
          }
          previewData = null;
          previewRaw = null;
          return;
        }
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

  function inputValue(event: Event) {
    return (event.currentTarget as HTMLInputElement | null)?.value ?? '';
  }

  function incomingNodeTypeLabel(value: string) {
    const key = String(value || '').trim();
    return INCOMING_NODE_TYPE_LABELS[key] || key || 'Неизвестный источник';
  }

  function incomingNodeDescription(item: { sourceNodeType?: string; sourcePort?: string }) {
    const nodeType = incomingNodeTypeLabel(String(item?.sourceNodeType || '').trim());
    const port = String(item?.sourcePort || 'out').trim() || 'out';
    return `Источник приходит из desk graph от ноды типа «${nodeType}» через порт «${port}».`;
  }

  function incomingContractFields(item: NodeDescriptor | null | undefined) {
    return descriptorFields(item);
  }

  function incomingSampleMeta(item: NodeDescriptor | null | undefined) {
    return item?.sample?.sampleMeta && typeof item.sample.sampleMeta === 'object' ? item.sample.sampleMeta : null;
  }

  function incomingSampleNode() {
    return (
      incomingNodes.find(
        (item) =>
          item?.sample?.sampleRaw !== undefined ||
          item?.sample?.samplePayload !== undefined ||
          (Array.isArray(item?.sample?.sampleRows) && item.sample.sampleRows.length)
      ) || null
    );
  }

  function previewRowsFromValue(value: any) {
    if (Array.isArray(value)) {
      const rows = value.filter((item) => item && typeof item === 'object' && !Array.isArray(item));
      return rows.slice(0, 10);
    }
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      return [value];
    }
    return [];
  }

  function previewColumnsFromRows(rows: Array<Record<string, any>>) {
    return Array.from(
      new Set(
        (Array.isArray(rows) ? rows : [])
          .flatMap((row) => (row && typeof row === 'object' ? Object.keys(row) : []))
          .map((item) => String(item || '').trim())
          .filter(Boolean)
      )
    );
  }

  function inputValueFromIncomingSample(item: NodeDescriptor | null | undefined) {
    const value =
      item?.sample?.samplePayload ?? item?.sample?.sampleRaw ?? (Array.isArray(item?.sample?.sampleRows) ? item.sample.sampleRows : null);
    if (value === undefined || value === null) return '';
    return typeof value === 'string' ? value : JSON.stringify(value, null, 2);
  }

  function buildIncomingSamplePreview(descriptor: NodeDescriptorFlow | null | undefined) {
    const node =
      Array.isArray(descriptor?.upstreamDescriptors)
        ? descriptor.upstreamDescriptors.find(
            (item) =>
              item?.sample?.sampleRaw !== undefined ||
              item?.sample?.samplePayload !== undefined ||
              (Array.isArray(item?.sample?.sampleRows) && item.sample.sampleRows.length)
          ) || null
        : null;
    if (!node) {
      return {
        rows: [] as Array<Record<string, any>>,
        columns: [] as string[],
        rowCount: '-' as number | string,
        columnCount: '-' as number | string,
        sourceRef: '',
        emptyMessage: 'Upstream источник определён, но sample snapshot для preview пока недоступен.'
      };
    }
    const sampleRows = descriptorSampleRows(node).filter((item) => item && typeof item === 'object');
    const rows =
      sampleRows.length
        ? sampleRows.slice(0, 10)
        : previewRowsFromValue(node?.sample?.samplePayload !== undefined ? node.sample.samplePayload : node?.sample?.sampleRaw);
    const columns = descriptorSampleColumns(node).length ? descriptorSampleColumns(node) : previewColumnsFromRows(rows);
    return {
      rows,
      columns,
      rowCount: rows.length,
      columnCount: columns.length,
      sourceRef: `Sample snapshot · ${node.sourceNodeName}`,
      emptyMessage: 'Sample snapshot у upstream есть, но он не содержит табличного фрагмента для preview.'
    };
  }

  function incomingFieldSelected(field: NodeDescriptorField | null | undefined) {
    const key = descriptorFieldKey(field);
    return key ? selectedFieldPaths.includes(key) : false;
  }

  function useIncomingField(field: NodeDescriptorField | null | undefined) {
    const key = descriptorFieldKey(field);
    activeStep = 'fields';
    activeChangeStep = 'incoming';
    if (!key) return;
    addSelectedField(key);
  }

  function fieldOptionsForRow(row: { path?: string }) {
    const current = String(row?.path || '').trim();
    return Array.from(new Set([current, ...(Array.isArray(workingFieldCandidates) ? workingFieldCandidates : [])].filter(Boolean)));
  }

  function uniqueStringList(values: any[]) {
    return Array.from(
      new Set(
        (Array.isArray(values) ? values : [])
          .map((item) => String(item || '').trim())
          .filter(Boolean)
      )
    );
  }

  function normalizeAutoPathLabel(value: string, autoRootToken: string, rootLabel: string) {
    const txt = String(value || '').trim();
    if (!txt || txt === '-') return 'Не определено';
    if (txt === autoRootToken) return rootLabel;
    return txt;
  }

  function hasResolvedAutoPath(value: string, autoRootToken: string) {
    const txt = String(value || '').trim();
    return Boolean(txt && txt !== '-' && (txt === autoRootToken || txt.length));
  }

  function humanizeDetectedInput(inputKind: ParserPipelineInputKind, detectedFormat: string, sourceMode: string) {
    const format = String(detectedFormat || '').trim().toLowerCase();
    if (sourceMode === 'file_url') return 'Ссылка / ресурс';
    if (format === 'json') return 'JSON';
    if (format === 'csv' || format === 'tsv') return 'CSV';
    if (format === 'zip') return 'ZIP';
    if (format === 'text') return 'Текст';
    if (format === 'ndjson') return 'NDJSON';
    if (inputKind === 'structured payload') return 'Структурированные данные';
    if (inputKind === 'tabular text') return 'Табличный текст';
    if (inputKind === 'link/reference/locator') return 'Ссылка / ресурс';
    if (inputKind === 'text/string payload') return 'Текст';
    if (inputKind === 'archive/container') return 'Архив';
    return 'Unknown';
  }

  function humanizeReadMode(strategy: ParserPipelineStrategy, detectedFormat: string, sourceMode: string) {
    const format = String(detectedFormat || '').trim().toLowerCase();
    if (sourceMode === 'file_url' || strategy === 'dereference link/reference') return 'Dereference';
    if (strategy === 'extract archive entry' || format === 'zip') return 'Unzip';
    if (strategy === 'parse CSV/TSV' || format === 'csv' || format === 'tsv') return 'CSV';
    if (strategy === 'parse JSON string' || format === 'json' || format === 'ndjson') return 'JSON';
    if (strategy === 'direct read') return 'Direct read';
    if (strategy === 'unwrap envelope') return 'Unwrap envelope';
    if (strategy === 'path-based extraction' || strategy === 'multi-step extraction') return 'Path-based';
    return 'Unknown';
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
        : { detectedFormat: '-', payloadOrigin: '-', inputKind: '-', workingSetPath: '-', sourcePath: '-' };
      const safeRenameMap = renameMapValue && typeof renameMapValue === 'object' && !Array.isArray(renameMapValue) ? renameMapValue : {};
      const safeTypeMap = typeMapValue && typeof typeMapValue === 'object' && !Array.isArray(typeMapValue) ? typeMapValue : {};
      const safeDefaultValues = defaultValuesValue && typeof defaultValuesValue === 'object' && !Array.isArray(defaultValuesValue) ? defaultValuesValue : {};
      const safeBayesAlternatives = Array.isArray(bayesInput?.alternatives) ? bayesInput.alternatives : [];
      const sampleNode =
        safeIncomingNodes.find(
          (item) =>
            item?.sample?.sampleRaw !== undefined ||
            item?.sample?.samplePayload !== undefined ||
            (Array.isArray(item?.sample?.sampleRows) && item.sample.sampleRows.length)
        ) || null;
      const firstSampleRaw = sampleNode?.sample?.sampleRaw;
      const firstSamplePayload = sampleNode?.sample?.samplePayload;
      const firstSampleRows = Array.isArray(sampleNode?.sample?.sampleRows) ? sampleNode.sample.sampleRows : [];
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
      const autoPayloadPath = String(safeAutoParseInfo.sourcePath || incomingSampleMeta(sampleNode || {})?.payloadPath || '').trim();
      const autoWorkingSetPath = String(safeAutoParseInfo.workingSetPath || '').trim();
      const payloadMode: ParserPipelineStepMode = String(settings.inputPath || '').trim()
        ? 'manual'
        : hasSamplePayload || (String(safeAutoParseInfo.payloadOrigin || '').trim() && String(safeAutoParseInfo.payloadOrigin || '').trim() !== '-') || autoPayloadPath
        ? 'auto'
        : 'unknown';
      const rawRowCount = Number(safePreviewData?.raw_row_count);
      const rowsDetected = Number.isFinite(rawRowCount) ? rawRowCount : null;
      const sampleRowsAvailable = hasSampleRows ? firstSampleRows.length : safeSourcePreviewRows.length;
      const hasTabularSet = Boolean(sampleRowsAvailable > 0 || safeSourcePreviewColumns.length);
      const workingSetMode: ParserPipelineStepMode = String(settings.recordPath || '').trim()
        ? 'manual'
        : autoWorkingSetPath && autoWorkingSetPath !== '-'
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
      const insufficientData = !totalContractFields && missingSample && !hasPreviewSignals;
      const parserWarningsList = safeParserWarnings.map((item) => String(item || '').trim()).filter(Boolean);
      const warningsTotal =
        parserWarningsList.length +
        (ambiguity ? 1 : 0) +
        (missingSample ? 1 : 0) +
        (insufficientData ? 1 : 0);

      return {
        inputProfile: {
          upstreamSummary: safeIncomingNodes.length
            ? safeIncomingNodes.length === 1
              ? `${safeIncomingNodes[0].sourceNodeName} / ${incomingNodeTypeLabel(safeIncomingNodes[0].sourceNodeType)}`
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
          candidatePath: autoPayloadPath || '-',
          mode: payloadMode,
          hasPayloadSample: hasSamplePayload
        },
        workingSetCandidate: {
          candidateRecordPath: autoWorkingSetPath || '-',
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
    const descriptorOutputFields = descriptorFields(outputDescriptor);
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

    if (descriptorOutputFields.length) {
      descriptorOutputFields.forEach((field) => {
        pushField({
          name: String(field.alias || field.name || field.path || '').trim(),
          alias: String(field.alias || '').trim() || undefined,
          type: String(field.type || '').trim() || undefined,
          path: String(field.path || '').trim() || undefined,
          source: 'settings'
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
    void previewNow({ silentMissingInput: true });
  });

  $: lookupColumnOptions = columnOptionsFor(settings.lookupSchema, settings.lookupTable);
  $: if (settings.sourceSchema && settings.sourceTable) {
    void ensureColumnsFor(settings.sourceSchema, settings.sourceTable);
  }
  $: if (settings.lookupSchema && settings.lookupTable) {
    void ensureColumnsFor(settings.lookupSchema, settings.lookupTable);
  }
</script>

<section class="panel" class:panel-embedded={embeddedMode}>
  <div class="parser-modal-split">
    <div class="parser-top-pane">
      <div class="parser-layout">
        <section class="parser-column parser-column-input">
      <div class="parser-shell-card">
        <div class="parser-card-head">
          <div>
            <h3>Входящие параметры</h3>
            <p>Consume-side единого descriptor flow. Здесь parser читает опубликованный upstream descriptor и может сразу брать его поля в результат.</p>
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
                <strong>{item.sourceNodeName}</strong>
                <div class="parser-shell-meta">
                  <span>{incomingNodeTypeLabel(item.sourceNodeType)}</span>
                  <span>Порт: {item.sourcePort || 'out'}</span>
                </div>
                <div class="parser-shell-description">{incomingNodeDescription(item)}</div>
                <div class="parser-shell-summary">
                  <span class="chip-chip readonly-chip">Kind: {descriptorOutputKindLabel(item.outputKind)}</span>
                  {#if item.detection?.detectedFormat}
                    <span class="chip-chip readonly-chip">Формат: {item.detection.detectedFormat}</span>
                  {/if}
                  {#if item.detection?.recordPath}
                    <span class="chip-chip readonly-chip">Строки: {item.detection.recordPath}</span>
                  {/if}
                </div>
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

      {#if activeStep === 'input'}
      <div class="parser-card">
        <div class="parser-card-head">
          <div>
            <h3>Настройка parser</h3>
            <p>Секция 2 отвечает только за логику parser: она читает consume descriptor слева, меняет данные текущей ноды и публикует итог вправо и вниз.</p>
          </div>
          <button type="button" class="primary-btn" on:click={previewNow} disabled={previewLoading}>
            {previewLoading ? 'Обновление...' : 'Обновить preview'}
          </button>
        </div>

        <div class="parser-pipeline-summary-box">
          <div class="parser-shell-summary">
            <span class="chip-chip readonly-chip">Вход: {detectedInputLabel}</span>
            <span class="chip-chip readonly-chip">Основа: {parserPipelineModel.inputProfile.sourceBasis}</span>
            <span class="chip-chip readonly-chip">Чтение: {detectedReadLabel}</span>
          </div>
          <div class="preview-meta">
            <span>Данные: {payloadCandidateLabel}</span>
            <span>Строки: {workingSetCandidateLabel}</span>
            <span>Результат: {parserPipelineModel.resultSummary.fieldsCount}</span>
            <span>Warnings: {parserPipelineModel.warningsSummary.total}</span>
          </div>
        </div>

        {#if autoSourceDefined}
          <div class="selected-source-box">
            <div><strong>Источник определён автоматически</strong></div>
            <div class="hint">{parserPipelineModel.inputProfile.upstreamSummary}. Входной контракт уже показан слева, а parser строится от этого upstream без обязательного выбора источника вручную.</div>
            <div class="preview-meta">
              <span>Режим: node-driven</span>
              <span>Источников: {incomingNodes.length}</span>
              <span>Основа: {parserPipelineModel.inputProfile.sourceBasis}</span>
              <span>Preview входа: {sourcePreviewRows.length ? `${sourcePreviewRows.length} строк` : 'без sample'}</span>
            </div>
          </div>
        {:else if legacyStandaloneSourceMode}
          <div class="warning-box">
            <strong>Открыта legacy-конфигурация parser</strong>
            <div>В этой ноде нет upstream descriptor, а сохранённый source mode = <code>{legacyStandaloneSourceLabel}</code>. Исполнение старого desk не ломается, но новый editor больше не строится вокруг самостоятельного выбора источника.</div>
          </div>
        {:else}
          <div class="inline-hint inline-hint-box">Upstream descriptor пока не найден. Parser откроется и сохранит текущие settings, но auto-first flow начнётся только после подключения входящей ноды.</div>
        {/if}

        {#if previewError}
          <div class="inline-error">{previewError}</div>
        {/if}

        <div class="preview-metrics">
          <span>Сырых строк: {sourcePreviewRowCount}</span>
          <span>Сырых колонок: {sourcePreviewColumnCount}</span>
          <span>Формат: {previewData?.source_format || '-'}</span>
          <span>Источник: {autoSourceDefined ? 'Upstream descriptor' : legacyStandaloneSourceMode ? legacyStandaloneSourceLabel : 'Не определён'}</span>
        </div>

        <div class="preview-meta">
          <span>Источник: {sourcePreviewSourceRef}</span>
          <span>Пакет: {previewData?.batch?.returned_rows ?? 0} / {previewData?.batch?.batch_size ?? '-'}</span>
          <span>Есть ещё данные: {previewData?.batch?.has_more ? 'да' : 'нет'}</span>
          <span>Обновлено: {previewUpdatedAt ? new Date(previewUpdatedAt).toLocaleString('ru-RU') : '-'}</span>
        </div>

        <details class="parser-preview-panel">
          <summary class="parser-preview-summary">Показать preview входа</summary>
          <div class="parser-preview-body">
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
              <div class="empty-box">{sourcePreviewEmptyMessage}</div>
            {/if}
          </div>
        </details>
      </div>
      {/if}

      <div class="parser-card">
        <div class="parser-card-head">
          <div>
            <h3>{PARSER_EDITOR_STEPS.find((item) => item.id === activeStep)?.label || 'Настройка parser'}</h3>
            <p>Центр теперь работает тремя шагами: parser сначала определяет данные, затем изменяет их и в конце показывает обзор результата.</p>
          </div>
        </div>

        {#if activeStep === 'input'}
          <div class="subsection parser-step-panel">
            <div class="subsection-head">
              <h4>Определение данных</h4>
              <button type="button" class="mini-btn" on:click={() => (detectOverrideOpen = !detectOverrideOpen)}>
                {showDetectOverride ? 'Скрыть ручную корректировку' : hasDetectManualOverrides ? 'Открыть ручную корректировку' : 'Скорректировать вручную'}
              </button>
            </div>
            <div class="inline-hint inline-hint-box">Parser сначала сам пытается понять вход: что пришло, как это читать, где лежат полезные данные, где лежат рабочие строки и сколько полей уже видно.</div>

            <div class="parser-shell-summary">
              <span class="chip-chip readonly-chip">{detectStatusLabel}</span>
              {#if hasDetectManualOverrides}
                <span class="chip-chip readonly-chip">Есть ручной override</span>
              {/if}
            </div>

            <div class="detect-summary-grid">
              <div class="detect-summary-card">
                <span class="detect-summary-label">Что пришло</span>
                <strong>{detectedInputLabel}</strong>
                <span class="hint">Основа: {parserPipelineModel.inputProfile.sourceBasis}</span>
              </div>
              <div class="detect-summary-card">
                <span class="detect-summary-label">Как прочитали</span>
                <strong>{detectedReadLabel}</strong>
                <span class="hint">Формат: {parserPipelineModel.parseStrategy.detectedFormat || 'unknown'}</span>
              </div>
              <div class="detect-summary-card">
                <span class="detect-summary-label">Где нашли данные</span>
                <strong>{payloadCandidateLabel}</strong>
                <span class="hint">
                  {payloadManualValue
                    ? 'Сохранённое ручное значение'
                    : payloadHasAutoCandidate
                    ? `Источник: ${parserPipelineModel.payloadCandidate.candidateSource}`
                    : 'Auto path пока не найден'}
                </span>
              </div>
              <div class="detect-summary-card">
                <span class="detect-summary-label">Где нашли строки</span>
                <strong>{workingSetCandidateLabel}</strong>
                <span class="hint">
                  {workingSetManualValue
                    ? 'Сохранённое ручное значение'
                    : workingSetHasAutoCandidate
                    ? `Preview строк: ${parserPipelineModel.workingSetCandidate.sampleRowsAvailable}`
                    : 'Auto path пока не найден'}
                </span>
              </div>
              <div class="detect-summary-card">
                <span class="detect-summary-label">Какие поля видим</span>
                <strong>{detectedCandidateFieldsCount || 0}</strong>
                <span class="hint">Источник: {detectedCandidateFieldsSource}</span>
              </div>
            </div>

            {#if parserPipelineModel.warningsSummary.total}
              <div class="warnings-box">
                {#each parserPipelineModel.warningsSummary.parserWarnings as warning}
                  <div class="warning-line">{warning}</div>
                {/each}
                {#if parserPipelineModel.warningsSummary.insufficientData}
                  <div class="warning-line">Для полной автодетекции пока не хватает sample/preview данных. Parser использует contract upstream и текущие settings.</div>
                {/if}
                {#if parserPipelineModel.warningsSummary.ambiguity}
                  <div class="warning-line">Есть неоднозначность в путях или в нескольких upstream-источниках. При необходимости скорректируй шаг вручную.</div>
                {/if}
              </div>
            {/if}

            {#if showDetectOverride}
              <div class="detect-override-box">
                <div class="sub-block">
                  <div class="subsection-head">
                    <h5>Как читать вход</h5>
                  </div>
                  <div class="inline-hint">{readModeLabel}</div>
                  <div class="form-grid form-grid-2">
                    <label>
                      Переопределить режим чтения
                      <select value={settings.sourceFormat} on:change={(e) => patchSetting('sourceFormat', selectValue(e))}>
                        <option value="auto">Auto</option>
                        <option value="json">JSON</option>
                        <option value="csv">CSV</option>
                        <option value="zip">ZIP</option>
                        <option value="text">Текст</option>
                        <option value="ndjson">Другое / NDJSON</option>
                      </select>
                      <span class="hint">Оставь auto, если parser уже правильно понял формат.</span>
                    </label>
                    {#if shouldShowCsvOverride}
                      <label>
                        Делимитер CSV
                        <input value={settings.csvDelimiter} on:input={(e) => patchSetting('csvDelimiter', inputValue(e))} placeholder="," />
                        <span class="hint">Нужен только для CSV/TSV-подобного входа.</span>
                      </label>
                    {:else}
                      <div class="inline-hint inline-hint-box">Сейчас parser не требует специальных параметров чтения. Override открывай только если auto-режим прочитал вход не так, как ожидалось.</div>
                    {/if}
                  </div>
                  {#if shouldShowZipOverride}
                    <div class="form-grid form-grid-2">
                      {#if archiveEntryOptions.length}
                        <label>
                          Файл внутри ZIP
                          <select value={settings.archiveEntry} on:change={(e) => patchSetting('archiveEntry', selectValue(e))}>
                            <option value="">Определить автоматически</option>
                            {#each archiveEntryOptions as entryName}
                              <option value={entryName}>{entryName}</option>
                            {/each}
                          </select>
                          <span class="hint">Если parser уже видит список файлов внутри архива, можно выбрать entry без ручного ввода.</span>
                        </label>
                      {:else}
                        <label>
                          Файл внутри ZIP
                          <input value={settings.archiveEntry} on:input={(e) => patchSetting('archiveEntry', inputValue(e))} placeholder="export/data.csv" />
                          <span class="hint">Если список файлов недоступен, укажи entry вручную.</span>
                        </label>
                      {/if}
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

                <div class="sub-block">
                  <div class="subsection-head">
                    <h5>Где лежат данные</h5>
                  </div>
                  <div class="inline-hint">{payloadResolutionLabel}</div>
                  {#if payloadAutoPathOptions.length}
                    <div class="form-grid form-grid-2">
                      <label>
                        Выбрать найденный путь до данных
                        <select value={payloadSelectedCandidateValue} on:change={(e) => patchSetting('inputPath', selectValue(e))}>
                          <option value="">Оставить auto: {payloadCandidateLabel}</option>
                          {#each payloadAutoPathOptions as path}
                            <option value={path}>{path}</option>
                          {/each}
                        </select>
                        <span class="hint">Это candidate paths, которые parser уже смог найти автоматически.</span>
                      </label>
                      <div class="inline-hint inline-hint-box">
                        {#if payloadUsesSavedOverride}
                          Сейчас сохранён ручной override: <code>{payloadManualValue}</code>
                        {:else}
                          Если ни один из найденных candidate paths не подходит, открой ручной ввод ниже.
                        {/if}
                        <div class="subsection-actions">
                          <button type="button" class="mini-btn" on:click={() => (payloadManualInputOpen = !payloadManualInputOpen)}>
                            {showPayloadManualInput ? 'Скрыть ручной ввод' : 'Ввести вручную'}
                          </button>
                        </div>
                      </div>
                    </div>
                  {:else}
                    <div class="inline-hint inline-hint-box">Parser пока не смог автоматически определить путь до полезных данных. Здесь нужен ручной override.</div>
                  {/if}
                  {#if showPayloadManualInput}
                    <div class="form-grid form-grid-1">
                      <label>
                        Ручной путь до данных
                        <input value={settings.inputPath} on:input={(e) => patchSetting('inputPath', inputValue(e))} placeholder="response.data" />
                        <span class="hint">Используй это поле только как fallback, если auto path не найден или найден неправильно.</span>
                      </label>
                    </div>
                  {/if}
                </div>

                <div class="sub-block">
                  <div class="subsection-head">
                    <h5>Где лежат строки</h5>
                  </div>
                  <div class="inline-hint">{workingSetResolutionLabel}</div>
                  {#if bayesInput?.recommended_candidate}
                    <div class="bayes-box">
                      <div class="bayes-box-head">
                        <div>
                          <strong>Байесовская рекомендация рабочего набора</strong>
                          <div class="hint">Parser уже нашёл наиболее вероятный путь до строк и может зафиксировать его одним кликом.</div>
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
                    </div>
                  {/if}
                  {#if workingSetAutoPathOptions.length}
                    <div class="form-grid form-grid-2">
                      <label>
                        Выбрать найденный путь до строк
                        <select value={workingSetSelectedCandidateValue} on:change={(e) => patchSetting('recordPath', selectValue(e))}>
                          <option value="">Оставить auto: {workingSetCandidateLabel}</option>
                          {#each workingSetAutoPathOptions as path}
                            <option value={path}>{path}</option>
                          {/each}
                        </select>
                        <span class="hint">Это candidate paths, которые parser уже считает вероятным рабочим набором.</span>
                      </label>
                      <div class="inline-hint inline-hint-box">
                        {#if workingSetUsesSavedOverride}
                          Сейчас сохранён ручной override: <code>{workingSetManualValue}</code>
                        {:else}
                          Если рабочие строки лежат глубже найденных данных, открой ручной ввод ниже.
                        {/if}
                        <div class="subsection-actions">
                          <button type="button" class="mini-btn" on:click={() => (workingSetManualInputOpen = !workingSetManualInputOpen)}>
                            {showWorkingSetManualInput ? 'Скрыть ручной ввод' : 'Ввести вручную'}
                          </button>
                        </div>
                      </div>
                    </div>
                  {:else}
                    <div class="inline-hint inline-hint-box">Parser пока не смог автоматически определить путь до рабочего набора строк. Здесь нужен ручной override.</div>
                  {/if}
                  {#if showWorkingSetManualInput}
                    <div class="form-grid form-grid-1">
                      <label>
                        Ручной путь до строк
                        <input value={settings.recordPath} on:input={(e) => patchSetting('recordPath', inputValue(e))} placeholder="items.rows" />
                        <span class="hint">Нужен только если рабочие строки лежат глубже найденных данных.</span>
                      </label>
                    </div>
                  {/if}
                  <div class="form-grid form-grid-1">
                    <label>
                      Множитель парсинга
                      <input type="number" min="1" value={settings.parserMultiplier} on:input={(e) => patchSetting('parserMultiplier', inputValue(e))} />
                      <span class="hint">Нужен только если один входной элемент разворачивается в несколько строк результата.</span>
                    </label>
                  </div>
                </div>
              </div>
            {:else}
              <div class="inline-hint">
                Сейчас parser уже дал auto summary. Если он выглядит адекватно, ручные overrides можно не открывать.
                {#if hasDetectManualOverrides}
                  В этой ноде уже сохранены ручные корректировки чтения/данных/строк, но они скрыты до явного открытия.
                {/if}
              </div>
            {/if}
          </div>
        {/if}

        {#if activeStep === 'fields'}
          <div class="subsection parser-step-panel">
            <div class="subsection-head">
              <h4>Изменение данных</h4>
            </div>
            <div class="inline-hint inline-hint-box">Это главный рабочий шаг parser. Сначала фиксируй поля результата, затем подключай присоединения, фильтры, вычисляемые поля, удаление дублей и агрегаты.</div>
            <div class="parser-substeps-bar" aria-label="Подшаги изменения данных">
              {#each PARSER_CHANGE_STEPS as step}
                <button
                  type="button"
                  class="parser-substep-btn"
                  class:is-active={activeChangeStep === step.id}
                  aria-pressed={activeChangeStep === step.id}
                  on:click={() => (activeChangeStep = step.id)}
                >
                  {step.label}
                </button>
              {/each}
            </div>
          </div>

          {#if activeChangeStep === 'incoming'}
          <div class="subsection parser-step-panel">
            <div class="subsection-head">
              <h4>Входящие данные</h4>
              <div class="subsection-actions">
                <button type="button" class="mini-btn" on:click={addAllDetectedFields} disabled={!workingFieldCandidates.length}>Взять всё из рабочего набора</button>
                <button type="button" class="mini-btn" on:click={() => rebuildFieldSettings([])} disabled={!selectedFieldRows.length}>Очистить</button>
              </div>
            </div>
            <div class="inline-hint">Здесь фиксируется итоговый набор полей parser: какие поля уйдут дальше, как они будут переименованы, типизированы и чем будут заполняться по умолчанию.</div>
            <div class="field-picker">
              <select on:change={(e) => { addSelectedField(selectValue(e)); clearSelectValue(e); }}>
                <option value="">Добавить поле в результат</option>
                {#each workingFieldCandidates.filter((item) => !selectedFieldPaths.includes(item)) as fieldName}
                  <option value={fieldName}>{fieldName}</option>
                {/each}
              </select>
            </div>
            <div class="inline-hint">Клик по полю слева сразу добавляет его в `selectFields` через существующие parser settings и переводит в этот подпункт.</div>
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

            <details class="parser-advanced-panel">
              <summary>Дополнительно: preview и runtime</summary>
              <div class="parser-advanced-body">
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
                <div class="inline-hint">Эти параметры не участвуют в определении данных. Они только ограничивают preview и пакетную обработку внутри runtime parser.</div>
              </div>
            </details>
          </div>
          {/if}

        {#if activeChangeStep === 'lookup'}
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
        {/if}

        {#if activeChangeStep === 'filters'}
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
        {/if}

        {#if activeChangeStep === 'computed'}
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
        {/if}

        {#if activeChangeStep === 'dedupe'}
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
        {/if}

        {#if activeChangeStep === 'grouping'}
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
        {/if}
        {/if}

        {#if activeStep === 'result'}
        <div class="subsection parser-step-panel">
          <div class="subsection-head">
            <h4>Результат</h4>
          </div>
          <div class="inline-hint inline-hint-box">Это обзор publish-side parser. Табличный preview итогового результата вынесен в отдельную нижнюю секцию, чтобы не смешивать summary и сами строки.</div>
          <div class="preview-metrics">
            <span>Поля: {parserPipelineModel.resultSummary.fieldsCount}</span>
            <span>Источник summary: {parserPipelineModel.resultSummary.source}</span>
            <span>Derived preview: {parserPipelineModel.resultSummary.hasDerivedOutputPreview ? 'есть' : 'нет'}</span>
            <span>Publish kind: {outputDescriptorKindLabelValue}</span>
          </div>
          <div class="preview-meta">
            <span>Публикуемый descriptor: {publishedDescriptorFields.length ? `${publishedDescriptorFields.length} полей` : 'пока partial'}</span>
            <span>Read mode: {outputDescriptorDetection?.readMode || '-'}</span>
            <span>Payload path: {outputDescriptorDetection?.payloadPath || '-'}</span>
            <span>Rows path: {outputDescriptorDetection?.recordPath || '-'}</span>
          </div>
          {#if parserAppliedSteps.length}
            <div class="applied-steps">
              {#each parserAppliedSteps as step}
                <span>{step}</span>
              {/each}
            </div>
          {/if}
          {#if publishedDescriptorFields.length}
            <div class="preview-columns">
              {#each publishedDescriptorFields as field}
                <span>{field.alias || field.name}</span>
              {/each}
            </div>
          {:else if derivedOutputFields.length}
            <div class="preview-columns">
              {#each derivedOutputFields as field}
                <span>{field.name}</span>
              {/each}
            </div>
          {:else}
            <div class="inline-hint">Итоговый output строится из текущих settings parser и preview результата. Если данных мало, справа и здесь остаётся только partial summary.</div>
          {/if}
          {#if parserPipelineModel.warningsSummary.total}
            <div class="warnings-box">
              {#each parserPipelineModel.warningsSummary.parserWarnings as warning}
                <div class="warning-line">{warning}</div>
              {/each}
              {#if parserPipelineModel.warningsSummary.insufficientData}
                <div class="warning-line">Для полного итогового результата пока не хватает preview-данных. Parser остаётся в settings-first режиме и публикует partial descriptor.</div>
              {/if}
            </div>
          {/if}
        </div>
        {/if}
      </div>

    </section>

        <section class="parser-column parser-column-output">
      <div class="parser-shell-card">
        <div class="parser-card-head">
          <div>
            <h3>Исходящие параметры</h3>
            <p>Publish-side того же descriptor flow. Правая колонка показывает, что parser публикует для следующей ноды, без отдельной editable schema.</p>
          </div>
        </div>
        <div class="parser-shell-summary">
          <span class="chip-chip readonly-chip">Kind: {outputDescriptorKindLabelValue}</span>
          <span class="chip-chip readonly-chip">Поля: {publishedDescriptorFields.length}</span>
          {#if outputDescriptorDetection?.detectedFormat}
            <span class="chip-chip readonly-chip">Формат: {outputDescriptorDetection.detectedFormat}</span>
          {/if}
        </div>
        <div class="preview-meta">
          <span>Read mode: {outputDescriptorDetection?.readMode || '-'}</span>
          <span>Payload path: {outputDescriptorDetection?.payloadPath || '-'}</span>
          <span>Rows path: {outputDescriptorDetection?.recordPath || '-'}</span>
        </div>
        {#if publishedDescriptorFields.length}
          <div class="parser-shell-summary">
            <span class="chip-chip readonly-chip">{publishedDescriptorFields.length} {publishedDescriptorFields.length === 1 ? 'поле' : publishedDescriptorFields.length < 5 ? 'поля' : 'полей'}</span>
            <span class="inline-hint">Publish descriptor текущей ноды</span>
          </div>
          <div class="parser-contract-list">
            {#each publishedDescriptorFields as field}
              <div class="parser-contract-item">
                <div class="parser-contract-name">{field.alias || field.name}</div>
                <div class="parser-contract-meta">
                  <span>Имя: {field.name}</span>
                  {#if field.alias}
                    <span>Alias: {field.alias}</span>
                  {/if}
                  {#if field.type}
                    <span>Тип: {field.type}</span>
                  {/if}
                  {#if field.path}
                    <span>Path: {field.path}</span>
                  {/if}
                  {#if field.origin}
                    <span>Источник: {field.origin}</span>
                  {/if}
                </div>
              </div>
            {/each}
          </div>
        {:else if derivedOutputFields.length}
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
          <div class="inline-hint">Publish descriptor пока partial: parser ещё не зафиксировал поля результата и preview не вернул итоговые колонки.</div>
        {/if}
      </div>
        </section>
      </div>
    </div>

    <div class="parser-bottom-pane">
      <section class="parser-result-preview-section parser-card">
        <div class="parser-card-head">
          <div>
            <h3>Предпросмотр результата</h3>
            <p>Это итоговый tabular preview результата parser, а не preview сырого входа. Здесь видно, что реально выйдет из ноды и уйдёт дальше.</p>
          </div>
          <button type="button" class="mini-btn" on:click={previewNow} disabled={previewLoading}>
            {previewLoading ? 'Обновление...' : 'Обновить preview'}
          </button>
        </div>
        <div class="preview-metrics">
          <span>Строк: {previewData?.row_count ?? '-'}</span>
          <span>Колонок: {previewData?.column_count ?? '-'}</span>
          <span>Формат: {previewData?.source_format || '-'}</span>
          <span>Источник: {autoSourceDefined ? 'Upstream descriptor' : legacyStandaloneSourceMode ? legacyStandaloneSourceLabel : 'Не определён'}</span>
        </div>
        <div class="preview-meta">
          <span>Пакет: {previewData?.batch?.returned_rows ?? 0} / {previewData?.batch?.batch_size ?? '-'}</span>
          <span>Есть ещё данные: {previewData?.batch?.has_more ? 'да' : 'нет'}</span>
          <span>Обновлено: {previewUpdatedAt ? new Date(previewUpdatedAt).toLocaleString('ru-RU') : '-'}</span>
        </div>
        {#if previewResultColumns.length}
          <div class="preview-columns">
            {#each previewResultColumns as column}
              <span>{column}</span>
            {/each}
          </div>
        {/if}
        {#if previewResultRows.length && previewResultColumns.length}
          <div class="preview-table-wrap">
            <table class="preview-table">
              <thead>
                <tr>
                  {#each previewResultColumns as column}
                    <th>{column}</th>
                  {/each}
                </tr>
              </thead>
              <tbody>
                {#each previewResultRows as row}
                  <tr>
                    {#each previewResultColumns as column}
                      <td>{typeof row?.[column] === 'object' ? JSON.stringify(row?.[column]) : String(row?.[column] ?? '')}</td>
                    {/each}
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
        {:else}
          <div class="empty-box">Нет итогового preview результата. Обнови preview после изменения настроек parser.</div>
        {/if}
      </section>
    </div>
  </div>
</section>

<style>
  .panel {
    min-width: 0;
    min-height: 0;
    height: 100%;
    display: flex;
    flex-direction: column;
    gap: 12px;
  }
  .panel-embedded {
    min-height: 0;
    flex: 1 1 auto;
  }
  .parser-modal-split {
    flex: 1 1 auto;
    min-height: 0;
    display: grid;
    grid-template-rows: minmax(0, 1fr) minmax(0, 1fr);
    gap: 12px;
  }
  .parser-top-pane,
  .parser-bottom-pane {
    min-height: 0;
    overflow: auto;
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
  .parser-substeps-bar {
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
  .parser-substep-btn {
    border-radius: 999px;
    border: 1px solid #dbe4f0;
    background: #fff;
    color: #334155;
    padding: 7px 11px;
    font-size: 12px;
    font-weight: 600;
    cursor: pointer;
    transition: border-color 0.15s ease, background 0.15s ease, color 0.15s ease;
  }
  .parser-substep-btn.is-active {
    background: #e8f0ff;
    border-color: #93c5fd;
    color: #1d4ed8;
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
  .parser-result-preview-section {
    min-height: 100%;
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
  .detect-summary-grid {
    display: grid;
    grid-template-columns: repeat(5, minmax(0, 1fr));
    gap: 10px;
  }
  .detect-summary-card {
    display: flex;
    flex-direction: column;
    gap: 6px;
    padding: 10px 12px;
    border: 1px solid #dbe4f0;
    border-radius: 12px;
    background: #f8fafc;
  }
  .detect-summary-card strong {
    font-size: 13px;
    color: #0f172a;
    word-break: break-word;
  }
  .detect-summary-label {
    font-size: 11px;
    font-weight: 700;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.02em;
  }
  .detect-override-box {
    display: flex;
    flex-direction: column;
    gap: 12px;
    border: 1px solid #dbe4f0;
    border-radius: 14px;
    background: #fcfdff;
    padding: 12px;
  }
  .parser-preview-panel {
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    background: #fff;
  }
  .parser-preview-summary {
    cursor: pointer;
    list-style: none;
    padding: 10px 12px;
    font-size: 12px;
    font-weight: 600;
    color: #334155;
  }
  .parser-preview-summary::-webkit-details-marker {
    display: none;
  }
  .parser-preview-body {
    display: flex;
    flex-direction: column;
    gap: 10px;
    padding: 0 12px 12px;
  }
  .parser-advanced-panel {
    border: 1px solid #dbe4f0;
    border-radius: 12px;
    background: #f8fafc;
    padding: 10px 12px;
  }
  .parser-advanced-panel summary {
    cursor: pointer;
    font-size: 12px;
    font-weight: 600;
    color: #334155;
  }
  .parser-advanced-body {
    margin-top: 10px;
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  .primary-btn,
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
  .mini-btn {
    background: #fff;
    color: #334155;
  }
  .primary-btn:disabled,
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
  .rules-grid-aggregates {
    grid-template-columns: minmax(0, 1fr) minmax(170px, 0.8fr) minmax(0, 1fr) auto;
  }
  .rules-grid:not(.rules-grid-filters):not(.rules-grid-aggregates) {
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
  .selected-source-box {
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
    .detect-summary-grid,
    .form-grid-3,
    .form-grid-4 {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .rules-grid,
    .rules-grid-filters,
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
    .detect-summary-grid,
    .form-grid-2,
    .form-grid-3,
    .form-grid-4 {
      grid-template-columns: 1fr;
    }
    .rules-grid,
    .rules-grid-filters,
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

