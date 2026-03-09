<script lang="ts">
  import { createEventDispatcher, onMount } from 'svelte';
  import { buildSourceNodePreviewFromTemplate } from './parserSourcePreviewCore.js';

  type ExistingTable = { schema_name: string; table_name: string };
  type ColumnMeta = { name: string; type?: string };
  type WriteSettings = Record<string, string>;
  type WriteDraft = {
    id: number;
    name: string;
    write_name: string;
    description: string;
    revision: number;
    updated_at?: string | null;
    updated_by?: string;
    config_json?: Record<string, any>;
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
  type WriteMappingRow = {
    sourceField: string;
    targetField: string;
    status: string;
    autoMatched?: boolean;
  };
  type WritePreview = {
    source_type: string;
    source_ref: string;
    source_row_count: number;
    source_column_count: number;
    source_columns: string[];
    source_sample_rows: Array<Record<string, any>>;
    target_schema: string;
    target_table: string;
    target_columns: Array<{ name: string; type?: string }>;
    mapping_rows: WriteMappingRow[];
    mapping_summary: {
      rows_ready: number;
      mapped_fields_count: number;
      unmatched_source_fields: string[];
      unmatched_target_fields: string[];
      key_fields: string[];
      rows_with_complete_key: number;
      matched_existing_rows: number;
      insert_candidates: number;
      update_candidates: number;
    };
    result_row_count: number;
    result_column_count: number;
    result_columns: string[];
    result_sample_rows: Array<Record<string, any>>;
    stats: Record<string, any>;
  };

  export let apiBase: string;
  export let apiJson: <T = any>(url: string, init?: RequestInit) => Promise<T>;
  export let headers: () => Record<string, string>;
  export let existingTables: ExistingTable[] = [];
  export let initialSettings: Record<string, any> = {};
  export let embeddedMode = false;

  const dispatch = createEventDispatcher<{ configChange: { settings: WriteSettings } }>();

  const DEFAULT_SETTINGS: WriteSettings = {
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
    unmappedMode: 'matched_only',
    conflictMode: 'input_wins',
    batchSize: '500',
    previewLimit: '20',
    channel: ''
  };

  function normalizeWriteMode(value: any) {
    const raw = String(value || '').trim().toLowerCase();
    if (raw === 'update' || raw === 'update_by_key') return 'update_by_key';
    if (raw === 'upsert') return 'upsert';
    return 'insert';
  }

  function normalizeUnmappedMode(value: any) {
    const raw = String(value || '').trim().toLowerCase();
    if (raw === 'skip') return 'skip';
    if (raw === 'error') return 'error';
    return 'matched_only';
  }

  function normalizeConflictMode(value: any) {
    const raw = String(value || '').trim().toLowerCase();
    if (raw === 'keep_table' || raw === 'keep_existing') return 'keep_table';
    if (raw === 'only_if_empty' || raw === 'fill_if_empty') return 'only_if_empty';
    return 'input_wins';
  }

  function stringifyJson(value: any, fallback = '[]') {
    if (typeof value === 'string') return value.trim() || fallback;
    if (value && typeof value === 'object') {
      try {
        return JSON.stringify(value, null, 2);
      } catch {
        return fallback;
      }
    }
    return fallback;
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

  function toPrettyJson(value: any, fallback = '[]') {
    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return fallback;
    }
  }

  function normalizeFieldName(value: string) {
    return String(value || '')
      .trim()
      .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
      .replace(/[\s\-./]+/g, '_')
      .replace(/__+/g, '_')
      .toLowerCase();
  }

  function cloneSettings(value: WriteSettings): WriteSettings {
    return { ...value };
  }

  function normalizeSettings(raw: Record<string, any> | null | undefined): WriteSettings {
    const next: WriteSettings = { ...DEFAULT_SETTINGS };
    const src = raw && typeof raw === 'object' ? raw : {};
    for (const key of Object.keys(DEFAULT_SETTINGS)) {
      const value = src[key];
      if (value === undefined || value === null) continue;
      next[key] = typeof value === 'string' ? value : JSON.stringify(value);
    }
    next.templateId = String(src.templateId || '').trim();
    next.templateStoreId = String(src.templateStoreId || '').trim();
    next.sourceMode = String(src.sourceMode || src.source_mode || next.sourceMode || 'node').trim().toLowerCase() === 'table' ? 'table' : 'node';
    next.sourceNodeTemplateRef = String(src.sourceNodeTemplateRef || src.source_node_template_ref || '').trim();
    next.sourceNodeTemplateType = String(src.sourceNodeTemplateType || src.source_node_template_type || '').trim();
    next.sourceNodeTemplateStoreId = String(src.sourceNodeTemplateStoreId || src.source_node_template_store_id || '').trim();
    next.sourceNodeTemplateName = String(src.sourceNodeTemplateName || src.source_node_template_name || '').trim();
    next.sourceSchema = String(src.sourceSchema || src.source_schema || '').trim();
    next.sourceTable = String(src.sourceTable || src.source_table || '').trim();
    next.sourceColumn = String(src.sourceColumn || src.source_column || '').trim();
    next.targetSchema = String(src.targetSchema || src.target_schema || '').trim();
    next.targetTable = String(src.targetTable || src.target_table || '').trim();
    next.writeMode = normalizeWriteMode(src.writeMode || src.write_mode || 'insert');
    next.fieldMappingsJson = stringifyJson(src.fieldMappings || src.field_mappings, '[]');
    next.keyFields = Array.isArray(src.keyFields || src.key_fields || src.keyColumns || src.key_columns)
      ? (src.keyFields || src.key_fields || src.keyColumns || src.key_columns).join(', ')
      : String(src.keyFields || src.key_fields || src.keyColumns || src.key_columns || '').trim();
    next.unmappedMode = normalizeUnmappedMode(src.unmappedMode || src.unmapped_mode || 'matched_only');
    next.conflictMode = normalizeConflictMode(src.conflictMode || src.conflict_mode || 'input_wins');
    next.batchSize = String(src.batchSize || src.batch_size || '500').trim() || '500';
    next.previewLimit = String(src.previewLimit || src.preview_limit || '20').trim() || '20';
    next.channel = String(src.channel || '').trim();
    return next;
  }

  function storeIdFromSettings(value: Record<string, any> | null | undefined) {
    const raw = Number(String(value?.templateStoreId || '').trim());
    return Number.isFinite(raw) && raw > 0 ? Math.trunc(raw) : 0;
  }

  function templateIdForDraft(draft: WriteDraft | null | undefined) {
    const id = Number(draft?.id || 0);
    return id > 0 ? `write_tpl_${id}` : '';
  }

  function buildTemplatePayload(settingsValue: WriteSettings) {
    return {
      sourceMode: settingsValue.sourceMode,
      sourceNodeTemplateRef: settingsValue.sourceNodeTemplateRef,
      sourceNodeTemplateType: settingsValue.sourceNodeTemplateType,
      sourceNodeTemplateStoreId: settingsValue.sourceNodeTemplateStoreId,
      sourceNodeTemplateName: settingsValue.sourceNodeTemplateName,
      sourceSchema: settingsValue.sourceSchema,
      sourceTable: settingsValue.sourceTable,
      sourceColumn: settingsValue.sourceColumn,
      targetSchema: settingsValue.targetSchema,
      targetTable: settingsValue.targetTable,
      writeMode: settingsValue.writeMode,
      fieldMappings: parseJsonSafe(settingsValue.fieldMappingsJson, []),
      keyFields: parseCsvText(settingsValue.keyFields),
      unmappedMode: settingsValue.unmappedMode,
      conflictMode: settingsValue.conflictMode,
      batchSize: settingsValue.batchSize,
      previewLimit: settingsValue.previewLimit,
      channel: settingsValue.channel
    };
  }

  function sourceTemplateRef(kind: 'api_request' | 'table_parser', storeId: number) {
    return `${kind}:${Math.trunc(Number(storeId || 0))}`;
  }

  function sourceTemplateLabel(item: SourceNodeTemplate) {
    return `${item.templateType === 'api_request' ? 'Запросы' : 'Работа с данными'} / ${item.name}`;
  }

  let settings = normalizeSettings(initialSettings);
  let lastInitialSignature = JSON.stringify(settings);
  let templatesLoading = false;
  let templatesError = '';
  let drafts: WriteDraft[] = [];
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
  let previewData: WritePreview | null = null;
  let previewUpdatedAt = '';

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
    return `${item.name} ${item.description}`.toLowerCase().includes(needle);
  });
  $: currentTemplateStoreId = storeIdFromSettings(settings);
  $: currentTemplate = drafts.find((item) => Number(item.id || 0) === Number(currentTemplateStoreId)) || null;
  $: selectedIsCurrent = Boolean(selectedDraft && currentTemplate && Number(selectedDraft.id) === Number(currentTemplate.id));
  $: canSwitchTemplate = Boolean(selectedDraft && !selectedIsCurrent);
  $: currentSourceTemplate =
    sourceTemplates.find((item) => item.ref === String(settings.sourceNodeTemplateRef || '').trim()) ||
    sourceTemplates.find((item) => String(item.storeId || 0) === String(settings.sourceNodeTemplateStoreId || '').trim()) ||
    null;
  $: sourceNodePreview = settings.sourceMode === 'node' ? buildSourceNodePreviewFromTemplate(currentSourceTemplate) : { rows: [], columns: [], message: '' };
  $: sourceNodePreviewJson = sourceNodePreview.rows.length ? JSON.stringify(sourceNodePreview.rows, null, 2) : '';
  $: sourcePreviewColumns =
    settings.sourceMode === 'node'
      ? sourceNodePreview.columns
      : Array.isArray(previewData?.source_columns)
      ? previewData.source_columns
      : [];
  $: sourcePreviewRows =
    settings.sourceMode === 'node'
      ? sourceNodePreview.rows
      : Array.isArray(previewData?.source_sample_rows)
      ? previewData.source_sample_rows
      : [];
  $: sourcePreviewMessage = settings.sourceMode === 'node' ? sourceNodePreview.message || '' : previewError || '';
  $: sourcePreviewRowCount =
    settings.sourceMode === 'node'
      ? currentSourceTemplate
        ? sourceNodePreview.rows.length
        : '-'
      : (previewData?.source_row_count ?? '-');
  $: sourcePreviewColumnCount =
    settings.sourceMode === 'node'
      ? currentSourceTemplate
        ? sourceNodePreview.columns.length
        : '-'
      : (previewData?.source_column_count ?? '-');
  $: sourcePreviewSourceRef =
    settings.sourceMode === 'node'
      ? currentSourceTemplate
        ? sourceTemplateLabel(currentSourceTemplate)
        : '-'
      : previewData?.source_ref || '-';
  $: targetColumnOptions = columnOptionsFor(settings.targetSchema, settings.targetTable);
  $: sourceTableColumnOptions = columnOptionsFor(settings.sourceSchema, settings.sourceTable);
  $: explicitMappings = parseFieldMappings(settings.fieldMappingsJson);
  $: mappingRows = buildMappingRows(sourcePreviewColumns, targetColumnOptions, explicitMappings);
  $: keyFieldList = parseCsvText(settings.keyFields);
  $: previewWarnings = Array.isArray(previewData?.stats?.warnings) ? previewData.stats.warnings : [];
  $: previewSteps = Array.isArray(previewData?.stats?.applied_steps) ? previewData.stats.applied_steps : [];
  $: mappedRowsSummary = previewData?.mapping_summary || null;
  $: writePreviewColumns = Array.isArray(previewData?.result_columns) ? previewData.result_columns : [];
  $: writePreviewRows = Array.isArray(previewData?.result_sample_rows) ? previewData.result_sample_rows : [];

  $: if (settings.sourceSchema && settings.sourceTable) {
    void ensureColumnsFor(settings.sourceSchema, settings.sourceTable);
  }

  $: if (settings.targetSchema && settings.targetTable) {
    void ensureColumnsFor(settings.targetSchema, settings.targetTable);
  }

  function dispatchSettings(next: WriteSettings) {
    settings = cloneSettings(next);
    dispatch('configChange', { settings });
  }

  function patchSetting(key: string, value: string) {
    const next = cloneSettings(settings);
    next[key] = value;
    dispatchSettings(next);
  }

  function patchJsonSetting(key: string, value: any, fallback = '[]') {
    patchSetting(key, toPrettyJson(value, fallback));
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

  function parseFieldMappings(raw: string) {
    const parsed = parseJsonSafe(raw, []);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .map((item) => ({
        sourceField: String(item?.sourceField || item?.source_field || '').trim(),
        targetField: String(item?.targetField || item?.target_field || '').trim()
      }))
      .filter((item) => item.sourceField || item.targetField);
  }

  function buildMappingRows(sourceFields: string[], targetFields: string[], explicitRows: Array<{ sourceField: string; targetField: string }>) {
    const explicitBySource = new Map(explicitRows.map((item) => [item.sourceField, item]));
    const normalizedTarget = new Map((Array.isArray(targetFields) ? targetFields : []).map((field) => [normalizeFieldName(field), field]));
    const targetSet = new Set((Array.isArray(targetFields) ? targetFields : []).map((field) => String(field || '').trim()).filter(Boolean));
    return (Array.isArray(sourceFields) ? sourceFields : [])
      .map((sourceField) => String(sourceField || '').trim())
      .filter(Boolean)
      .map((sourceField) => {
        const explicit = explicitBySource.get(sourceField);
        const autoTarget = normalizedTarget.get(normalizeFieldName(sourceField)) || '';
        const targetField = String(explicit?.targetField || autoTarget || '').trim();
        const status = !targetField ? 'unmapped' : targetSet.has(targetField) ? 'mapped' : 'missing_target';
        return {
          sourceField,
          targetField,
          status,
          autoMatched: !explicit && Boolean(autoTarget)
        };
      });
  }

  function rebuildFieldMappings(rows: Array<{ sourceField: string; targetField: string }>) {
    const normalized = rows
      .map((row) => ({
        sourceField: String(row?.sourceField || '').trim(),
        targetField: String(row?.targetField || '').trim()
      }))
      .filter((row) => row.sourceField || row.targetField);
    patchJsonSetting('fieldMappingsJson', normalized);
  }

  function autoMatchAll() {
    rebuildFieldMappings(
      sourcePreviewColumns.map((sourceField) => ({
        sourceField,
        targetField: targetColumnOptions.find((target) => normalizeFieldName(target) === normalizeFieldName(sourceField)) || ''
      }))
    );
  }

  function clearMappings() {
    rebuildFieldMappings(sourcePreviewColumns.map((sourceField) => ({ sourceField, targetField: '' })));
  }

  function updateMappingRow(sourceField: string, targetField: string) {
    rebuildFieldMappings(mappingRows.map((row) => (row.sourceField === sourceField ? { ...row, targetField } : row)));
  }

  function toggleKeyField(fieldName: string) {
    const wanted = String(fieldName || '').trim();
    if (!wanted) return;
    const next = new Set(keyFieldList);
    if (next.has(wanted)) next.delete(wanted);
    else next.add(wanted);
    patchSetting('keyFields', [...next].join(', '));
  }

  function mappingStatusLabel(status: string) {
    if (status === 'mapped') return 'Сопоставлено';
    if (status === 'missing_target') return 'Нет такого поля в таблице';
    return 'Не сопоставлено';
  }

  function sourceTableOptions() {
    return Array.isArray(existingTables) ? existingTables : [];
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

  function patchFromDraft(draft: WriteDraft) {
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
      const payload = await apiJson<{ write_configs?: WriteDraft[] }>(`${apiBase}/write-configs`, {
        method: 'GET',
        headers: { Accept: 'application/json', ...headers() }
      });
      drafts = (Array.isArray(payload?.write_configs) ? payload.write_configs : []).map((item) => ({
        id: Number(item?.id || 0),
        name: String(item?.name || item?.write_name || '').trim(),
        write_name: String(item?.write_name || item?.name || '').trim(),
        description: String(item?.description || '').trim(),
        revision: Number(item?.revision || 1) || 1,
        updated_at: item?.updated_at || null,
        updated_by: String(item?.updated_by || '').trim(),
        config_json: item?.config_json && typeof item.config_json === 'object' ? item.config_json : {}
      }));
      const wantedId = selectStoreId > 0 ? selectStoreId : selectedDraftId;
      if (wantedId > 0 && drafts.some((item) => Number(item.id) === wantedId)) selectedDraftId = wantedId;
      else if (currentTemplateStoreId > 0 && drafts.some((item) => Number(item.id) === currentTemplateStoreId)) selectedDraftId = currentTemplateStoreId;
      else if (drafts.length && !selectedDraftId) selectedDraftId = Number(drafts[0].id || 0);
    } catch (e: any) {
      templatesError = String(e?.message || e || 'Не удалось загрузить шаблоны записи');
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
        apiJson<{ parser_configs?: any[] }>(`${apiBase}/parser-configs`, {
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
    const write_name = String(templateNameInput || '').trim();
    if (!write_name) {
      templatesError = 'Укажи название шаблона записи.';
      return;
    }
    templateSaving = true;
    templatesError = '';
    try {
      const currentId = Number(selectedDraft?.id || 0);
      const payload = await apiJson<{ id: number; revision: number }>(`${apiBase}/write-configs/upsert`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({
          id: currentId > 0 ? currentId : undefined,
          write_name,
          description: String(templateDescriptionInput || '').trim(),
          config_json: buildTemplatePayload(settings)
        })
      });
      const savedId = Number(payload?.id || 0);
      await loadDrafts(savedId);
      const savedDraft = drafts.find((item) => Number(item.id) === savedId) || null;
      if (savedDraft) {
        patchFromDraft(savedDraft);
        templateNameInput = savedDraft.name;
        templateDescriptionInput = savedDraft.description;
      }
    } catch (e: any) {
      templatesError = String(e?.message || e || 'Не удалось сохранить шаблон записи');
    } finally {
      templateSaving = false;
    }
  }

  async function deleteTemplate() {
    if (!selectedDraft) return;
    templateDeleting = true;
    templatesError = '';
    try {
      await apiJson(`${apiBase}/write-configs/delete`, {
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
    } catch (e: any) {
      templatesError = String(e?.message || e || 'Не удалось удалить шаблон записи');
    } finally {
      templateDeleting = false;
    }
  }

  function selectDraft(draft: WriteDraft) {
    selectedDraftId = Number(draft.id || 0);
    templateNameInput = draft.name;
    templateDescriptionInput = draft.description;
  }

  function resetTemplateDraft() {
    selectedDraftId = 0;
    templateNameInput = '';
    templateDescriptionInput = '';
  }

  function applySelectedTemplate() {
    if (!selectedDraft || selectedIsCurrent) return;
    patchFromDraft(selectedDraft);
  }

  async function previewNow() {
    previewLoading = true;
    previewError = '';
    try {
      let inputValue: any = undefined;
      if (settings.sourceMode === 'node') {
        if (!currentSourceTemplate) {
          previewError = 'Сначала выбери шаблон ноды-источника.';
          previewData = null;
          return;
        }
        if (!sourceNodePreviewJson) {
          previewError = sourcePreviewMessage || 'У выбранного шаблона ноды ещё не настроен выходной контракт.';
          previewData = null;
          return;
        }
        inputValue = sourceNodePreviewJson;
      } else if (!settings.sourceSchema || !settings.sourceTable) {
        previewError = 'Сначала выбери таблицу-источник.';
        previewData = null;
        return;
      }
      const payload = await apiJson<{ preview?: WritePreview }>(`${apiBase}/write-configs/preview`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({
          config_json: buildTemplatePayload(settings),
          input_value: inputValue
        })
      });
      previewData = payload?.preview || null;
      previewUpdatedAt = new Date().toISOString();
    } catch (e: any) {
      previewError = String(e?.message || e || 'Не удалось получить preview записи');
      previewData = null;
    } finally {
      previewLoading = false;
    }
  }

  function currentTableColumnsHint() {
    const columns = sourceTableColumnOptions.length;
    if (!settings.sourceSchema || !settings.sourceTable) return '';
    if (columns) return `${settings.sourceSchema}.${settings.sourceTable} / доступно колонок: ${columns}`;
    return `${settings.sourceSchema}.${settings.sourceTable}`;
  }

  function currentTargetColumnsHint() {
    const columns = targetColumnOptions.length;
    if (!settings.targetSchema || !settings.targetTable) return '';
    if (columns) return `${settings.targetSchema}.${settings.targetTable} / колонок: ${columns}`;
    return `${settings.targetSchema}.${settings.targetTable}`;
  }

  onMount(() => {
    void loadDrafts(storeIdFromSettings(initialSettings));
    void loadSourceTemplates();
  });
</script>

<section class="panel" class:panel-embedded={embeddedMode}>
  <div class="write-layout">
    <section class="write-column write-column-preview">
      <div class="write-card">
        <div class="write-card-head">
          <div>
            <h3>Источник и preview входа</h3>
            <p>Показывает, какой набор строк придёт в запись: от шаблона ноды-источника или из выбранной таблицы.</p>
          </div>
          <button type="button" class="primary-btn" on:click={previewNow} disabled={previewLoading}>
            {previewLoading ? 'Обновление...' : 'Обновить preview'}
          </button>
        </div>

        <div class="form-grid form-grid-2">
          <label>
            Тип источника
            <select value={settings.sourceMode} on:change={(e) => patchSetting('sourceMode', selectValue(e))}>
              <option value="node">Предыдущая нода</option>
              <option value="table">Таблица</option>
            </select>
          </label>
        </div>

        {#if settings.sourceMode === 'node'}
          <label>
            Шаблон ноды-источника
            <select value={settings.sourceNodeTemplateRef} on:change={(e) => applySourceNodeTemplateRef(selectValue(e))}>
              <option value="">Выбери шаблон ноды</option>
              {#each sourceTemplates as item (item.ref)}
                <option value={item.ref}>{sourceTemplateLabel(item)}</option>
              {/each}
            </select>
            <span class="hint">Для preview используется выходной контракт шаблона ноды, а не конкретный экземпляр на canvas.</span>
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
            <div class="inline-hint">Выбери шаблон API-ноды или ноды “Работа с данными”, чтобы увидеть ожидаемый вход.</div>
          {/if}
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
              Колонка payload
              <select value={settings.sourceColumn} on:change={(e) => patchSetting('sourceColumn', selectValue(e))}>
                <option value="">Взять строки как есть</option>
                {#each sourceTableColumnOptions as column}
                  <option value={column}>{column}</option>
                {/each}
              </select>
            </label>
          </div>
          <div class="inline-hint">{currentTableColumnsHint() || 'Выбери таблицу-источник, чтобы увидеть данные перед записью.'}</div>
        {/if}

        {#if previewError}
          <div class="inline-error">{previewError}</div>
        {/if}

        <div class="preview-metrics">
          <span>Строк: {sourcePreviewRowCount}</span>
          <span>Колонок: {sourcePreviewColumnCount}</span>
          <span>Источник: {sourcePreviewSourceRef}</span>
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
          <div class="empty-box">{sourcePreviewMessage || 'Нет preview входных данных. Обнови preview после выбора источника.'}</div>
        {/if}
      </div>
    </section>

    <section class="write-column write-column-main">
      <div class="write-card">
        <div class="write-card-head">
          <div>
            <h3>Настройка записи</h3>
            <p>Выбери таблицу назначения, режим записи, сопоставление полей, ключи и предварительный результат.</p>
          </div>
        </div>

        <div class="subsection">
          <div class="subsection-head">
            <h4>Куда записывать</h4>
          </div>
          <div class="form-grid form-grid-2">
            <label>
              Схема назначения
              <select value={settings.targetSchema} on:change={(e) => patchSetting('targetSchema', selectValue(e))}>
                <option value="">Выбери схему</option>
                {#each Array.from(new Set(existingTables.map((item) => item.schema_name))) as schemaName}
                  <option value={schemaName}>{schemaName}</option>
                {/each}
              </select>
            </label>
            <label>
              Таблица назначения
              <select value={settings.targetTable} on:change={(e) => patchSetting('targetTable', selectValue(e))}>
                <option value="">Выбери таблицу</option>
                {#each existingTables.filter((item) => !settings.targetSchema || item.schema_name === settings.targetSchema) as item}
                  <option value={item.table_name}>{item.table_name}</option>
                {/each}
              </select>
            </label>
          </div>
          <div class="inline-hint">{currentTargetColumnsHint() || 'Выбери таблицу, чтобы загрузить поля назначения и автосопоставление.'}</div>
        </div>

        <div class="subsection">
          <div class="subsection-head">
            <h4>Режим записи</h4>
          </div>
          <div class="form-grid form-grid-3">
            <label>
              Что делать с данными
              <select value={settings.writeMode} on:change={(e) => patchSetting('writeMode', selectValue(e))}>
                <option value="insert">Только добавить новые строки</option>
                <option value="update_by_key">Только обновить существующие</option>
                <option value="upsert">Добавить новые и обновить существующие</option>
              </select>
            </label>
            <label>
              Что делать с несопоставленными полями
              <select value={settings.unmappedMode} on:change={(e) => patchSetting('unmappedMode', selectValue(e))}>
                <option value="matched_only">Записывать только сопоставленные</option>
                <option value="skip">Пропустить несопоставленные</option>
                <option value="error">Считать ошибкой</option>
              </select>
            </label>
            <label>
              Поведение при конфликте значений
              <select value={settings.conflictMode} on:change={(e) => patchSetting('conflictMode', selectValue(e))}>
                <option value="input_wins">Заменить значением из входа</option>
                <option value="keep_table">Оставить значение таблицы</option>
                <option value="only_if_empty">Записывать только если в таблице пусто</option>
              </select>
            </label>
          </div>
        </div>

        <div class="subsection">
          <div class="subsection-head">
            <h4>Сопоставление полей</h4>
            <div class="row-actions">
              <button type="button" class="mini-btn" on:click={autoMatchAll}>Автосопоставить все</button>
              <button type="button" class="mini-btn" on:click={clearMappings}>Очистить</button>
            </div>
          </div>
          <div class="inline-hint">Слева поле входных данных, справа поле таблицы. Если имена совпадают по смыслу, автосопоставление заполнит их само.</div>
          <div class="mapping-table-wrap">
            <table class="mapping-table">
              <thead>
                <tr>
                  <th>Поле входных данных</th>
                  <th>Поле таблицы</th>
                  <th>Ключ</th>
                  <th>Статус</th>
                </tr>
              </thead>
              <tbody>
                {#if mappingRows.length}
                  {#each mappingRows as row (row.sourceField)}
                    <tr>
                      <td>{row.sourceField}</td>
                      <td>
                        <select value={row.targetField} on:change={(e) => updateMappingRow(row.sourceField, selectValue(e))}>
                          <option value="">Не записывать</option>
                          {#each targetColumnOptions as field}
                            <option value={field}>{field}</option>
                          {/each}
                        </select>
                      </td>
                      <td class="key-cell">
                        {#if row.targetField}
                          <input type="checkbox" checked={keyFieldList.includes(row.targetField)} on:change={() => toggleKeyField(row.targetField)} />
                        {:else}
                          <span class="muted">—</span>
                        {/if}
                      </td>
                      <td>
                        <span class={`mapping-status mapping-status-${row.status}`}>{mappingStatusLabel(row.status)}</span>
                      </td>
                    </tr>
                  {/each}
                {:else}
                  <tr>
                    <td colspan="4" class="muted">Нет входных полей для сопоставления. Сначала выбери источник и обнови preview.</td>
                  </tr>
                {/if}
              </tbody>
            </table>
          </div>
        </div>

        <div class="subsection">
          <div class="subsection-head">
            <h4>Ключи для update / upsert</h4>
          </div>
          <div class="inline-hint">Отметь поля таблицы, по которым система будет понимать, что запись уже существует.</div>
          {#if keyFieldList.length}
            <div class="preview-columns">
              {#each keyFieldList as keyField}
                <span>{keyField}</span>
              {/each}
            </div>
          {:else}
            <div class="inline-hint">Ключевые поля пока не выбраны.</div>
          {/if}
        </div>

        <div class="subsection">
          <div class="subsection-head">
            <h4>Preview результата записи</h4>
            <button type="button" class="mini-btn" on:click={previewNow} disabled={previewLoading}>
              {previewLoading ? 'Обновление...' : 'Обновить preview'}
            </button>
          </div>
          {#if mappedRowsSummary}
            <div class="preview-metrics">
              <span>Готово строк: {mappedRowsSummary.rows_ready}</span>
              <span>Сопоставлено полей: {mappedRowsSummary.mapped_fields_count}</span>
              <span>Есть ключ: {mappedRowsSummary.rows_with_complete_key}</span>
              <span>Совпадений найдено: {mappedRowsSummary.matched_existing_rows}</span>
            </div>
            <div class="preview-meta">
              <span>Кандидаты на insert: {mappedRowsSummary.insert_candidates}</span>
              <span>Кандидаты на update: {mappedRowsSummary.update_candidates}</span>
              <span>Обновлено: {previewUpdatedAt ? new Date(previewUpdatedAt).toLocaleString('ru-RU') : '-'}</span>
            </div>
            {#if mappedRowsSummary.unmatched_source_fields?.length}
              <div class="inline-hint">Не сопоставлены входные поля: {mappedRowsSummary.unmatched_source_fields.join(', ')}</div>
            {/if}
            {#if mappedRowsSummary.unmatched_target_fields?.length}
              <div class="inline-hint">Не используются поля таблицы: {mappedRowsSummary.unmatched_target_fields.join(', ')}</div>
            {/if}
          {/if}
          {#if previewWarnings.length}
            <div class="warnings-box">
              {#each previewWarnings as warning}
                <div class="inline-hint">{warning}</div>
              {/each}
            </div>
          {/if}
          {#if previewSteps.length}
            <div class="preview-columns">
              {#each previewSteps as step}
                <span>{step}</span>
              {/each}
            </div>
          {/if}
          {#if writePreviewColumns.length}
            <div class="preview-columns">
              {#each writePreviewColumns as column}
                <span>{column}</span>
              {/each}
            </div>
          {/if}
          {#if writePreviewRows.length}
            <div class="preview-table-wrap">
              <table class="preview-table">
                <thead>
                  <tr>
                    {#each writePreviewColumns as column}
                      <th>{column}</th>
                    {/each}
                  </tr>
                </thead>
                <tbody>
                  {#each writePreviewRows as row}
                    <tr>
                      {#each writePreviewColumns as column}
                        <td>{typeof row?.[column] === 'object' ? JSON.stringify(row?.[column]) : String(row?.[column] ?? '')}</td>
                      {/each}
                    </tr>
                  {/each}
                </tbody>
              </table>
            </div>
          {:else}
            <div class="empty-box">Нет preview результата записи. Обнови preview после выбора таблицы и сопоставления полей.</div>
          {/if}
        </div>
      </div>
    </section>

    <section class="write-column write-column-library">
      <div class="write-card">
        <div class="write-card-head">
          <div>
            <h3>Шаблоны записи данных</h3>
            <p>Сохраняют источник, сопоставление полей и правила записи. Нода привязывается к шаблону через desk state.</p>
          </div>
          <button type="button" class="mini-btn" on:click={() => loadDrafts()} disabled={templatesLoading}>
            Обновить
          </button>
        </div>

        <div class="current-template-box">
          <div class="current-template-line">
            <span class="template-key">Текущий шаблон</span>
            <span class="template-value">{currentTemplate ? currentTemplate.name : 'Шаблон не привязан'}</span>
          </div>
          <div class="current-template-actions">
            <button type="button" class="mini-btn" on:click={applySelectedTemplate} disabled={!canSwitchTemplate}>
              Сменить шаблон
            </button>
            <span class="hint">
              {#if !selectedDraft}
                В библиотеке ничего не выбрано.
              {:else if selectedIsCurrent}
                Этот шаблон уже подключён.
              {:else}
                Выбранный шаблон готов к перепривязке.
              {/if}
            </span>
          </div>
        </div>

        <div class="template-editor">
          <label>
            Название шаблона
            <input value={templateNameInput} on:input={(e) => (templateNameInput = inputValue(e))} placeholder="Например: Запись карточек в bronze" />
          </label>
          <label>
            Описание
            <textarea rows="4" value={templateDescriptionInput} on:input={(e) => (templateDescriptionInput = textareaValue(e))}></textarea>
          </label>
          <div class="row-actions">
            <button type="button" class="primary-btn" on:click={saveTemplate} disabled={templateSaving}>
              {templateSaving ? 'Сохранение...' : 'Сохранить шаблон'}
            </button>
            <button type="button" class="mini-btn" on:click={resetTemplateDraft}>Новый</button>
            <button type="button" class="mini-btn danger-btn" on:click={deleteTemplate} disabled={!selectedDraft || templateDeleting}>
              {templateDeleting ? 'Удаление...' : 'Удалить'}
            </button>
          </div>
          {#if templatesError}
            <div class="inline-error">{templatesError}</div>
          {/if}
        </div>

        <label>
          Поиск шаблона
          <input value={templateSearch} on:input={(e) => (templateSearch = inputValue(e))} placeholder="Поиск по названию и описанию" />
        </label>

        <div class="draft-list">
          {#if templatesLoading}
            <div class="empty-box">Загрузка шаблонов...</div>
          {:else if filteredDrafts.length}
            {#each filteredDrafts as draft (draft.id)}
              <button
                type="button"
                class={`draft-item ${Number(draft.id) === Number(selectedDraftId) ? 'active' : ''}`}
                on:click={() => selectDraft(draft)}
              >
                <strong>{draft.name}</strong>
                <span>{draft.description || 'Описание не заполнено'}</span>
                <small>Ревизия {draft.revision}</small>
              </button>
            {/each}
          {:else}
            <div class="empty-box">Шаблоны записи пока не созданы.</div>
          {/if}
        </div>
      </div>
    </section>
  </div>
</section>

<style>
  .panel { min-width: 0; }
  .panel-embedded { padding: 0; }
  .write-layout {
    display: grid;
    grid-template-columns: minmax(260px, 0.95fr) minmax(420px, 1.45fr) minmax(280px, 0.9fr);
    gap: 16px;
    min-width: 0;
    align-items: start;
  }
  .write-column { min-width: 0; }
  .write-card {
    background: #f8fafc;
    border: 1px solid #dbe4f0;
    border-radius: 14px;
    padding: 14px;
    display: flex;
    flex-direction: column;
    gap: 12px;
    min-width: 0;
  }
  .write-card-head { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; }
  .write-card-head h3 { margin: 0 0 4px; font-size: 15px; line-height: 1.2; color: #0f172a; }
  .write-card-head p { margin: 0; font-size: 12px; line-height: 1.45; color: #64748b; }
  .form-grid { display: grid; gap: 10px; min-width: 0; }
  .form-grid-2 { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .form-grid-3 { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  label { display: flex; flex-direction: column; gap: 6px; min-width: 0; font-size: 12px; color: #334155; }
  input, select, textarea, button { font: inherit; }
  input, select, textarea {
    width: 100%;
    min-width: 0;
    border: 1px solid #cbd5e1;
    border-radius: 10px;
    background: #fff;
    color: #0f172a;
    padding: 8px 10px;
    box-sizing: border-box;
  }
  textarea { resize: vertical; }
  .hint, .inline-hint, .muted { color: #64748b; font-size: 11px; line-height: 1.45; }
  .inline-error { color: #b91c1c; font-size: 11px; line-height: 1.45; }
  .primary-btn, .mini-btn {
    border: 1px solid #cbd5e1;
    border-radius: 10px;
    background: #fff;
    color: #0f172a;
    padding: 7px 12px;
    cursor: pointer;
  }
  .primary-btn { background: #0f172a; border-color: #0f172a; color: #fff; }
  .danger-btn { color: #b91c1c; }
  .primary-btn:disabled, .mini-btn:disabled { opacity: 0.55; cursor: not-allowed; }
  .subsection {
    border: 1px dashed #dbe4f0;
    border-radius: 12px;
    padding: 12px;
    display: flex;
    flex-direction: column;
    gap: 10px;
    min-width: 0;
  }
  .subsection-head { display: flex; justify-content: space-between; align-items: center; gap: 10px; }
  .subsection-head h4 { margin: 0; font-size: 13px; line-height: 1.2; color: #0f172a; }
  .row-actions { display: inline-flex; align-items: center; gap: 8px; flex-wrap: wrap; }
  .preview-metrics, .preview-meta, .preview-columns { display: flex; flex-wrap: wrap; gap: 6px 10px; align-items: center; }
  .preview-metrics span, .preview-meta span, .preview-columns span {
    border: 1px solid #dbe4f0;
    border-radius: 999px;
    background: #fff;
    padding: 4px 8px;
    font-size: 11px;
    color: #334155;
  }
  .preview-table-wrap, .mapping-table-wrap, .draft-list { min-width: 0; overflow: auto; }
  .preview-table, .mapping-table { width: 100%; border-collapse: collapse; min-width: 0; }
  .preview-table th, .preview-table td, .mapping-table th, .mapping-table td {
    border-bottom: 1px solid #e2e8f0;
    padding: 8px 10px;
    text-align: left;
    vertical-align: top;
    font-size: 12px;
  }
  .preview-table th, .mapping-table th {
    font-size: 11px;
    color: #64748b;
    background: #fff;
    position: sticky;
    top: 0;
  }
  .mapping-status {
    display: inline-flex;
    align-items: center;
    border-radius: 999px;
    padding: 4px 8px;
    font-size: 11px;
    background: #fff;
    border: 1px solid #dbe4f0;
    color: #334155;
  }
  .mapping-status-mapped { color: #166534; border-color: #bbf7d0; background: #f0fdf4; }
  .mapping-status-unmapped, .mapping-status-missing_target { color: #92400e; border-color: #fde68a; background: #fffbeb; }
  .key-cell { width: 50px; text-align: center; }
  .selected-source-box, .current-template-box, .template-editor {
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    background: #fff;
    padding: 12px;
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  .current-template-line { display: flex; flex-direction: column; gap: 4px; }
  .template-key { font-size: 11px; color: #64748b; }
  .template-value { font-size: 14px; font-weight: 600; color: #0f172a; }
  .current-template-actions { display: flex; flex-direction: column; gap: 6px; }
  .draft-item {
    width: 100%;
    text-align: left;
    border: 1px solid #dbe4f0;
    border-radius: 12px;
    background: #fff;
    padding: 10px 12px;
    display: flex;
    flex-direction: column;
    gap: 4px;
    cursor: pointer;
    margin-bottom: 8px;
  }
  .draft-item strong { font-size: 13px; color: #0f172a; }
  .draft-item span, .draft-item small { color: #64748b; font-size: 11px; }
  .draft-item.active { border-color: #0f172a; box-shadow: inset 0 0 0 1px #0f172a; }
  .empty-box {
    border: 1px dashed #cbd5e1;
    border-radius: 12px;
    background: #fff;
    color: #64748b;
    padding: 14px;
    font-size: 12px;
  }
  .warnings-box { display: flex; flex-direction: column; gap: 6px; }
  @media (max-width: 1480px) {
    .write-layout { grid-template-columns: minmax(280px, 1fr) minmax(420px, 1.2fr); }
    .write-column-library { grid-column: 1 / -1; }
  }
  @media (max-width: 1040px) {
    .write-layout { grid-template-columns: 1fr; }
    .form-grid-2, .form-grid-3 { grid-template-columns: 1fr; }
  }
</style>
