<script lang="ts">
  import { createEventDispatcher, onMount } from 'svelte';
  import { buildSourceNodePreviewFromTemplate } from './parserSourcePreviewCore.js';

  type ExistingTable = { schema_name: string; table_name: string };
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

  export let apiBase: string;
  export let apiJson: <T = any>(url: string, init?: RequestInit) => Promise<T>;
  export let headers: () => Record<string, string>;
  export let existingTables: ExistingTable[] = [];
  export let initialSettings: Record<string, any> = {};
  export let embeddedMode = false;

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
    parserMultiplier: '1',
    sampleInput: '',
    lookupEnabled: 'false',
    lookupSchema: '',
    lookupTable: '',
    lookupSourceField: '',
    lookupTargetField: '',
    lookupFields: '',
    lookupPrefix: ''
  };

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

  function parseJsonSafe(raw: string, fallback: any) {
    const txt = String(raw || '').trim();
    if (!txt) return fallback;
    try {
      return JSON.parse(txt);
    } catch {
      return fallback;
    }
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
      parserMultiplier: settingsValue.parserMultiplier,
      lookupEnabled: settingsValue.lookupEnabled === 'true',
      lookupSchema: settingsValue.lookupSchema,
      lookupTable: settingsValue.lookupTable,
      lookupSourceField: settingsValue.lookupSourceField,
      lookupTargetField: settingsValue.lookupTargetField,
      lookupFields: settingsValue.lookupFields,
      lookupPrefix: settingsValue.lookupPrefix
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
    return `${item.templateType === 'api_request' ? 'Запросы' : 'Работа с данными'} / ${item.name}`;
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
    return parts.join(' / ');
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

  onMount(() => {
    void loadDrafts(storeIdFromSettings(initialSettings));
    void loadSourceTemplates();
    void previewNow();
  });
</script>

<section class="panel" class:panel-embedded={embeddedMode}>
  <div class="parser-layout">
    <section class="parser-column parser-column-preview">
      <div class="parser-card">
        <div class="parser-card-head">
          <div>
            <h3>Источник данных</h3>
            <p>Выбери, откуда брать вход: от шаблона ноды, из таблицы или из файла/ссылки. Здесь же видно preview входа.</p>
          </div>
          <button type="button" class="primary-btn" on:click={previewNow} disabled={previewLoading}>
            {previewLoading ? 'Обновление...' : 'Обновить preview'}
          </button>
        </div>

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
          <label>
            Шаблон ноды-источника
            <select value={settings.sourceNodeTemplateRef} on:change={(e) => applySourceNodeTemplateRef(selectValue(e))}>
              <option value="">Выбери шаблон ноды</option>
              {#each sourceTemplates as item (item.ref)}
                <option value={item.ref}>{sourceTemplateLabel(item)}</option>
              {/each}
            </select>
            <span class="hint">Для нод источник задаётся через шаблон ноды, а не через конкретный экземпляр на canvas.</span>
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
            <div class="inline-hint">Выбери шаблон API-ноды или другой ноды “Работа с данными”, чтобы зафиксировать ожидаемый тип входа.</div>
          {/if}

          <label>
            Пример входных данных для preview
            {#if currentSourceTemplate && sourceNodePreviewJson}
              <textarea rows="10" readonly value={sourceNodePreviewJson}></textarea>
              <span class="hint">{sourceNodePreviewMessage}</span>
            {:else if currentSourceTemplate}
              <div class="inline-hint">{sourceNodePreviewMessage}</div>
            {:else}
              <div class="inline-hint">Выбери шаблон ноды-источника, чтобы автоматически увидеть ожидаемый вход.</div>
            {/if}
          </label>
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
              <input value={settings.sourceColumn} on:input={(e) => patchSetting('sourceColumn', inputValue(e))} placeholder="payload_json или другая колонка" />
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
      </div>
    </section>

    <section class="parser-column parser-column-settings">
      <div class="parser-card">
        <div class="parser-card-head">
          <div>
            <h3>Настройка обработки</h3>
            <p>Здесь задаётся формат, путь до записей, mapping, lookup и ограничения для безопасной пакетной обработки.</p>
          </div>
        </div>

        <div class="form-grid form-grid-3">
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
          </label>
          <label>
            Размер пакета
            <input type="number" min="1" max="5000" value={settings.batchSize} on:input={(e) => patchSetting('batchSize', inputValue(e))} />
          </label>
        </div>

        <div class="form-grid form-grid-3">
          <label>
            Путь до входа
            <input value={settings.inputPath} on:input={(e) => patchSetting('inputPath', inputValue(e))} placeholder="response.data" />
          </label>
          <label>
            Путь до массива записей
            <input value={settings.recordPath} on:input={(e) => patchSetting('recordPath', inputValue(e))} placeholder="items.rows" />
          </label>
          <label>
            Делимитер CSV
            <input value={settings.csvDelimiter} on:input={(e) => patchSetting('csvDelimiter', inputValue(e))} placeholder="," />
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

        <div class="form-grid form-grid-3">
          <label>
            Поля
            <input value={settings.selectFields} on:input={(e) => patchSetting('selectFields', inputValue(e))} placeholder="id, name, meta.updatedAt" />
            <span class="hint">Какие поля оставить в результате. Пусто = взять все поля объекта.</span>
          </label>
          <label>
            Переименовать поля (JSON)
            <textarea rows="4" value={settings.renameMap} on:input={(e) => patchSetting('renameMap', textareaValue(e))}></textarea>
          </label>
          <label>
            Значения по умолчанию (JSON)
            <textarea rows="4" value={settings.defaultValues} on:input={(e) => patchSetting('defaultValues', textareaValue(e))}></textarea>
          </label>
        </div>

        <div class="form-grid form-grid-3">
          <label>
            Типы полей (JSON)
            <textarea rows="4" value={settings.typeMap} on:input={(e) => patchSetting('typeMap', textareaValue(e))}></textarea>
            <span class="hint">Например: {`{"id":"integer","updated_at":"timestamp"}`}</span>
          </label>
          <label>
            Фильтр: поле
            <input value={settings.filterField} on:input={(e) => patchSetting('filterField', inputValue(e))} placeholder="status" />
          </label>
          <label>
            Фильтр: оператор и значение
            <div class="field-inline">
              <select value={settings.filterOperator} on:change={(e) => patchSetting('filterOperator', selectValue(e))}>
                <option value="">Без фильтра</option>
                <option value="=">=</option>
                <option value="!=">!=</option>
                <option value=">">&gt;</option>
                <option value=">=">&gt;=</option>
                <option value="<">&lt;</option>
                <option value="<=">&lt;=</option>
                <option value="contains">содержит</option>
                <option value="not_contains">не содержит</option>
                <option value="empty">пусто</option>
                <option value="not_empty">не пусто</option>
              </select>
              <input value={settings.filterValue} on:input={(e) => patchSetting('filterValue', inputValue(e))} placeholder="active" />
            </div>
          </label>
        </div>
        <div class="subsection">
          <div class="subsection-head">
            <h4>Lookup из таблицы</h4>
            <label class="switch-line">
              <span>Включить lookup</span>
              <select value={settings.lookupEnabled} on:change={(e) => patchSetting('lookupEnabled', selectValue(e))}>
                <option value="false">Нет</option>
                <option value="true">Да</option>
              </select>
            </label>
          </div>
          {#if settings.lookupEnabled === 'true'}
            <div class="form-grid form-grid-3">
              <label>
                Схема lookup
                <select value={settings.lookupSchema} on:change={(e) => patchSetting('lookupSchema', selectValue(e))}>
                  <option value="">Выбери схему</option>
                  {#each Array.from(new Set(sourceTableOptions().map((item) => item.schema_name))) as schemaName}
                    <option value={schemaName}>{schemaName}</option>
                  {/each}
                </select>
              </label>
              <label>
                Таблица lookup
                <select value={settings.lookupTable} on:change={(e) => patchSetting('lookupTable', selectValue(e))}>
                  <option value="">Выбери таблицу</option>
                  {#each sourceTableOptions().filter((item) => !settings.lookupSchema || item.schema_name === settings.lookupSchema) as item}
                    <option value={item.table_name}>{item.table_name}</option>
                  {/each}
                </select>
              </label>
              <label>
                Поля lookup
                <input value={settings.lookupFields} on:input={(e) => patchSetting('lookupFields', inputValue(e))} placeholder="client_id, token" />
              </label>
            </div>
            <div class="form-grid form-grid-3">
              <label>
                Поле источника
                <input value={settings.lookupSourceField} on:input={(e) => patchSetting('lookupSourceField', inputValue(e))} placeholder="client_id" />
              </label>
              <label>
                Поле поиска в lookup
                <input value={settings.lookupTargetField} on:input={(e) => patchSetting('lookupTargetField', inputValue(e))} placeholder="client_id" />
              </label>
              <label>
                Префикс полей
                <input value={settings.lookupPrefix} on:input={(e) => patchSetting('lookupPrefix', inputValue(e))} placeholder="lookup_" />
              </label>
            </div>
          {:else}
            <div class="inline-hint">Lookup нужен, если parser должен обогатить строки из существующей таблицы перед передачей дальше по workflow.</div>
          {/if}
        </div>

        <div class="subsection">
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
              Множитель парсинга
              <input type="number" min="1" value={settings.parserMultiplier} on:input={(e) => patchSetting('parserMultiplier', inputValue(e))} />
            </label>
          </div>
          <div class="inline-hint">В runtime одна и та же нода обрабатывает пакеты по очереди. Пользователь не рисует отдельные ноды под чанки и не обязан знать их количество заранее.</div>
        </div>

        <div class="subsection">
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
      </div>
    </section>

    <section class="parser-column parser-column-library">
      <div class="parser-card">
        <div class="parser-card-head">
          <div>
            <h3>Шаблоны обработки данных</h3>
            <p>Сохранённые настройки источника и обработки. Нода привязывается к шаблону, а результат всё равно передаётся дальше как обычный output ноды.</p>
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
                Выбери шаблон справа.
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
  .subsection-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
  }
  .subsection-head h4 {
    margin: 0;
    font-size: 14px;
    color: #0f172a;
  }
  .switch-line {
    flex-direction: row;
    align-items: center;
    gap: 8px;
    font-size: 12px;
  }
  .field-inline {
    display: grid;
    grid-template-columns: minmax(120px, 0.7fr) minmax(0, 1fr);
    gap: 8px;
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
  @media (max-width: 1380px) {
    .parser-layout {
      grid-template-columns: minmax(260px, 0.9fr) minmax(420px, 1.25fr) minmax(280px, 0.95fr);
    }
    .form-grid-3 {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
  }
  @media (max-width: 1120px) {
    .parser-layout {
      grid-template-columns: 1fr;
    }
    .form-grid-2,
    .form-grid-3,
    .field-inline {
      grid-template-columns: 1fr;
    }
  }
</style>

