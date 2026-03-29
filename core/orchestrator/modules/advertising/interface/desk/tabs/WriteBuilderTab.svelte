<script>
  import { createEventDispatcher, onMount } from 'svelte';
  import {
    descriptorFields,
    descriptorSampleColumns,
    descriptorSampleRows,
    descriptorOutputKindLabel
  } from '../data/nodeDescriptorFlow';

  export let apiBase = '';
  export let apiJson;
  export let headers;
  export let existingTables = [];
  export let workflowDeskId = 0;
  export let workflowNodeId = '';
  export let workflowGraph = null;
  export let initialSettings = {};
  export let embeddedMode = false;
  export let incomingDescriptor = null;
  export let outputDescriptor = null;
  export let lastRuntimeStep = null;

  const dispatch = createEventDispatcher();

  const DEFAULT_SETTINGS = {
    templateId: '',
    templateStoreId: '',
    sourceMode: 'node',
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

  const str = (value) => String(value ?? '').trim();

  function normalizeWriteMode(value) {
    const raw = str(value).toLowerCase();
    if (raw === 'update' || raw === 'update_by_key') return 'update_by_key';
    if (raw === 'upsert') return 'upsert';
    return 'insert';
  }

  function normalizeUnmappedMode(value) {
    const raw = str(value).toLowerCase();
    if (raw === 'skip') return 'skip';
    if (raw === 'error') return 'error';
    return 'matched_only';
  }

  function normalizeConflictMode(value) {
    const raw = str(value).toLowerCase();
    if (raw === 'keep_table' || raw === 'keep_existing') return 'keep_table';
    if (raw === 'only_if_empty' || raw === 'fill_if_empty') return 'only_if_empty';
    return 'input_wins';
  }

  function stringifyJson(value, fallback = '[]') {
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

  function parseJsonSafe(raw, fallback) {
    if (raw && typeof raw === 'object') return raw;
    const txt = str(raw);
    if (!txt) return fallback;
    try {
      return JSON.parse(txt);
    } catch {
      return fallback;
    }
  }

  function parseCsvText(raw) {
    return String(raw || '')
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean);
  }

  function toPrettyJson(value, fallback = '[]') {
    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return fallback;
    }
  }

  function normalizeFieldName(value) {
    return String(value || '')
      .trim()
      .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
      .replace(/[\s\-./]+/g, '_')
      .replace(/__+/g, '_')
      .toLowerCase();
  }

  function cloneSettings(value) {
    return { ...(value && typeof value === 'object' ? value : {}) };
  }

  function normalizeSettings(raw) {
    const src = raw && typeof raw === 'object' ? raw : {};
    const next = { ...DEFAULT_SETTINGS };
    next.templateId = str(src.templateId || '');
    next.templateStoreId = str(src.templateStoreId || '');
    next.sourceMode = str(src.sourceMode || src.source_mode || 'node').toLowerCase() === 'table' ? 'table' : 'node';
    next.sourceSchema = str(src.sourceSchema || src.source_schema || '');
    next.sourceTable = str(src.sourceTable || src.source_table || '');
    next.sourceColumn = str(src.sourceColumn || src.source_column || '');
    next.targetSchema = str(src.targetSchema || src.target_schema || '');
    next.targetTable = str(src.targetTable || src.target_table || '');
    next.writeMode = normalizeWriteMode(src.writeMode || src.write_mode || 'insert');
    next.fieldMappingsJson = stringifyJson(src.fieldMappingsJson || src.fieldMappings || src.field_mappings, '[]');
    next.keyFields = Array.isArray(src.keyFields || src.key_fields || src.keyColumns || src.key_columns)
      ? (src.keyFields || src.key_fields || src.keyColumns || src.key_columns).join(', ')
      : str(src.keyFields || src.key_fields || src.keyColumns || src.key_columns || '');
    next.unmappedMode = normalizeUnmappedMode(src.unmappedMode || src.unmapped_mode || 'matched_only');
    next.conflictMode = normalizeConflictMode(src.conflictMode || src.conflict_mode || 'input_wins');
    next.batchSize = str(src.batchSize || src.batch_size || '500') || '500';
    next.previewLimit = str(src.previewLimit || src.preview_limit || '20') || '20';
    next.channel = str(src.channel || '');
    return next;
  }

  function storeIdFromSettings(value) {
    const raw = Number(str(value?.templateStoreId || ''));
    return Number.isFinite(raw) && raw > 0 ? Math.trunc(raw) : 0;
  }

  function templateIdForDraft(draft) {
    const id = Number(draft?.id || 0);
    return id > 0 ? `write_tpl_${id}` : '';
  }

  function buildTemplatePayload(settingsValue) {
    return {
      sourceMode: settingsValue.sourceMode,
      sourceSchema: settingsValue.sourceSchema,
      sourceTable: settingsValue.sourceTable,
      sourceColumn: settingsValue.sourceColumn,
      targetSchema: settingsValue.targetSchema,
      targetTable: settingsValue.targetTable,
      writeMode: settingsValue.writeMode,
      fieldMappings: parseFieldMappings(settingsValue.fieldMappingsJson),
      keyFields: parseCsvText(settingsValue.keyFields),
      unmappedMode: settingsValue.unmappedMode,
      conflictMode: settingsValue.conflictMode,
      batchSize: settingsValue.batchSize,
      previewLimit: settingsValue.previewLimit,
      channel: settingsValue.channel
    };
  }

  function uniqueStrings(items = []) {
    return [...new Set((Array.isArray(items) ? items : []).map((item) => str(item)).filter(Boolean))];
  }

  function rowsFromNodeIo(value) {
    if (value && typeof value === 'object' && String(value.contract_version || '').trim() === 'node_io_v1' && Array.isArray(value.rows)) {
      return value.rows.filter((row) => row && typeof row === 'object' && !Array.isArray(row));
    }
    if (Array.isArray(value)) {
      return value.filter((row) => row && typeof row === 'object' && !Array.isArray(row));
    }
    return [];
  }

  function columnsFromRows(rows = [], fallback = []) {
    const explicit = uniqueStrings(fallback);
    if (explicit.length) return explicit;
    return uniqueStrings(
      (Array.isArray(rows) ? rows : []).flatMap((row) =>
        row && typeof row === 'object' && !Array.isArray(row) ? Object.keys(row) : []
      )
    );
  }

  function fieldDisplayName(field) {
    return str(field?.alias || field?.name || field?.path || '');
  }

  function fieldMeta(field) {
    const parts = [];
    if (field?.name) parts.push(`Имя: ${field.name}`);
    if (field?.alias) parts.push(`Alias: ${field.alias}`);
    if (field?.path) parts.push(`Path: ${field.path}`);
    if (field?.type) parts.push(`Тип: ${field.type}`);
    return parts.join(' · ');
  }

  function fieldAuxLabel(field) {
    const parts = [];
    if (field?.type) parts.push(String(field.type));
    const path = str(field?.path || '');
    if (path) parts.push(path);
    return parts.join(' · ');
  }

  function formatCell(value) {
    if (value === null || value === undefined) return '';
    if (typeof value === 'string') return value;
    if (typeof value === 'number' || typeof value === 'boolean') return String(value);
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }

  function normalizePreviewStep(step, runUid = '') {
    const src = step && typeof step === 'object' ? step : {};
    return {
      run_uid: str(src.run_uid || runUid),
      node_id: str(src.node_id || ''),
      status: str(src.status || ''),
      input_json: src.input_json && typeof src.input_json === 'object' ? src.input_json : null,
      output_json: src.output_json && typeof src.output_json === 'object' ? src.output_json : null,
      metrics_json: src.metrics_json && typeof src.metrics_json === 'object' ? src.metrics_json : null
    };
  }

  function parseFieldMappings(raw) {
    const parsed = parseJsonSafe(raw, []);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .map((item) => ({
        sourceField: str(item?.sourceField || item?.source_field || ''),
        targetField: str(item?.targetField || item?.target_field || '')
      }))
      .filter((item) => item.sourceField || item.targetField);
  }

  function buildMappingRows(sourceFields, targetFields, explicitRows) {
    const explicitBySource = new Map((Array.isArray(explicitRows) ? explicitRows : []).map((item) => [item.sourceField, item]));
    const normalizedTarget = new Map(
      (Array.isArray(targetFields) ? targetFields : []).map((field) => [normalizeFieldName(field), field])
    );
    const targetSet = new Set((Array.isArray(targetFields) ? targetFields : []).map((field) => str(field)).filter(Boolean));
    return (Array.isArray(sourceFields) ? sourceFields : [])
      .map((sourceField) => str(sourceField))
      .filter(Boolean)
      .map((sourceField) => {
        const explicit = explicitBySource.get(sourceField);
        const autoTarget = normalizedTarget.get(normalizeFieldName(sourceField)) || '';
        const targetField = str(explicit?.targetField || autoTarget || '');
        const status = !targetField ? 'unmapped' : targetSet.has(targetField) ? 'mapped' : 'missing_target';
        return {
          sourceField,
          targetField,
          status,
          autoMatched: !explicit && Boolean(autoTarget)
        };
      });
  }

  function rebuildFieldMappings(rows) {
    const normalized = (Array.isArray(rows) ? rows : [])
      .map((row) => ({
        sourceField: str(row?.sourceField || ''),
        targetField: str(row?.targetField || '')
      }))
      .filter((row) => row.sourceField || row.targetField);
    patchSetting('fieldMappingsJson', toPrettyJson(normalized, '[]'));
  }

  function autoMatchAll() {
    rebuildFieldMappings(
      canonicalInputContractColumns.map((sourceField) => ({
        sourceField,
        targetField: targetColumnOptions.find((target) => normalizeFieldName(target) === normalizeFieldName(sourceField)) || ''
      }))
    );
  }

  function clearMappings() {
    rebuildFieldMappings(canonicalInputContractColumns.map((sourceField) => ({ sourceField, targetField: '' })));
  }

  function updateMappingRow(sourceField, targetField) {
    rebuildFieldMappings(mappingRows.map((row) => (row.sourceField === sourceField ? { ...row, targetField } : row)));
  }

  function toggleKeyField(fieldName) {
    const wanted = str(fieldName);
    if (!wanted) return;
    const next = new Set(keyFieldList);
    if (next.has(wanted)) next.delete(wanted);
    else next.add(wanted);
    patchSetting('keyFields', [...next].join(', '));
  }

  function mappingStatusLabel(status) {
    if (status === 'mapped') return 'Сопоставлено';
    if (status === 'missing_target') return 'Нет поля в таблице';
    return 'Не сопоставлено';
  }

  function dispatchSettings(next) {
    settings = cloneSettings(next);
    dispatch('configChange', { settings });
  }

  function patchSetting(key, value) {
    const next = cloneSettings(settings);
    next[key] = value;
    dispatchSettings(next);
  }

  function inputValue(event) {
    return event.currentTarget?.value ?? '';
  }

  function selectValue(event) {
    return event.currentTarget?.value ?? '';
  }

  function textareaValue(event) {
    return event.currentTarget?.value ?? '';
  }

  function tableCacheKey(schema, table) {
    return `${str(schema)}.${str(table)}`;
  }

  async function ensureColumnsFor(schema, table) {
    const s = str(schema);
    const t = str(table);
    if (!s || !t) return;
    const key = tableCacheKey(s, t);
    if (Array.isArray(columnsCache[key])) return;
    try {
      const payload = await apiJson(`${apiBase}/columns?schema=${encodeURIComponent(s)}&table=${encodeURIComponent(t)}`, {
        method: 'GET',
        headers: { Accept: 'application/json', ...(typeof headers === 'function' ? headers() : {}) }
      });
      const cols = Array.isArray(payload?.columns)
        ? payload.columns
            .map((item) => ({
              name: str(item?.name || ''),
              type: str(item?.type || '').toLowerCase() || undefined
            }))
            .filter((item) => item.name)
        : [];
      columnsCache = { ...columnsCache, [key]: cols };
    } catch {
      columnsCache = { ...columnsCache, [key]: [] };
    }
  }

  function columnOptionsFor(schema, table) {
    return (columnsCache[tableCacheKey(schema, table)] || []).map((item) => item.name);
  }

  async function loadDrafts(selectStoreId = 0) {
    templatesLoading = true;
    templatesError = '';
    try {
      const payload = await apiJson(`${apiBase}/write-configs`, {
        method: 'GET',
        headers: { Accept: 'application/json', ...(typeof headers === 'function' ? headers() : {}) }
      });
      drafts = (Array.isArray(payload?.write_configs) ? payload.write_configs : []).map((item) => ({
        id: Number(item?.id || 0),
        name: str(item?.name || item?.write_name || ''),
        description: str(item?.description || ''),
        revision: Number(item?.revision || 1) || 1,
        updated_at: item?.updated_at || null,
        updated_by: str(item?.updated_by || ''),
        config_json: item?.config_json && typeof item.config_json === 'object' ? item.config_json : {}
      }));
      const wantedId = selectStoreId > 0 ? selectStoreId : selectedDraftId;
      if (wantedId > 0 && drafts.some((item) => Number(item.id) === wantedId)) selectedDraftId = wantedId;
      else if (currentTemplateStoreId > 0 && drafts.some((item) => Number(item.id) === currentTemplateStoreId)) selectedDraftId = currentTemplateStoreId;
      else if (drafts.length && !selectedDraftId) selectedDraftId = Number(drafts[0].id || 0);
    } catch (e) {
      templatesError = String(e?.message || e || 'Не удалось загрузить шаблоны записи');
    } finally {
      templatesLoading = false;
    }
  }

  function patchFromDraft(draft) {
    const next = normalizeSettings({
      ...(draft?.config_json && typeof draft.config_json === 'object' ? draft.config_json : {}),
      templateId: templateIdForDraft(draft),
      templateStoreId: String(draft?.id || '')
    });
    dispatchSettings(next);
  }

  function selectDraft(draft) {
    selectedDraftId = Number(draft?.id || 0);
    templateNameInput = draft?.name || '';
    templateDescriptionInput = draft?.description || '';
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

  async function saveTemplate() {
    const write_name = str(templateNameInput);
    if (!write_name) {
      templatesError = 'Укажи название шаблона записи.';
      return;
    }
    templateSaving = true;
    templatesError = '';
    try {
      const currentId = Number(selectedDraft?.id || 0);
      const payload = await apiJson(`${apiBase}/write-configs/upsert`, {
        method: 'POST',
        headers: typeof headers === 'function' ? headers() : {},
        body: JSON.stringify({
          id: currentId > 0 ? currentId : undefined,
          write_name,
          description: str(templateDescriptionInput),
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
    } catch (e) {
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
        headers: typeof headers === 'function' ? headers() : {},
        body: JSON.stringify({ id: selectedDraft.id })
      });
      const deletedId = Number(selectedDraft?.id || 0);
      selectedDraftId = 0;
      if (currentTemplateStoreId === deletedId) {
        const next = cloneSettings(settings);
        next.templateId = '';
        next.templateStoreId = '';
        dispatchSettings(next);
      }
      resetTemplateDraft();
      await loadDrafts();
    } catch (e) {
      templatesError = String(e?.message || e || 'Не удалось удалить шаблон записи');
    } finally {
      templateDeleting = false;
    }
  }

  async function previewNow() {
    previewLoading = true;
    previewError = '';
    try {
      if (!Number(workflowDeskId) || !str(workflowNodeId) || !workflowGraph || typeof workflowGraph !== 'object') {
        throw new Error('preview_write_node_context_missing');
      }
      const payload = await apiJson(`${apiBase}/process-runs/preview-write-node`, {
        method: 'POST',
        headers: typeof headers === 'function' ? headers() : {},
        body: JSON.stringify({
          desk_id: Number(workflowDeskId),
          target_node_id: workflowNodeId,
          graph_json: workflowGraph,
          write_settings_override: cloneSettings(settings)
        })
      });
      draftPreviewStep = normalizePreviewStep(payload?.target_step, payload?.run_uid);
      previewUpdatedAt = new Date().toISOString();
    } catch (e) {
      previewError = String(e?.message || e || 'Не удалось получить preview записи');
      draftPreviewStep = null;
    } finally {
      previewLoading = false;
    }
  }

  function currentTableColumnsHint() {
    const columns = sourceTableColumnOptions.length;
    if (!settings.sourceSchema || !settings.sourceTable) return '';
    if (columns) return `${settings.sourceSchema}.${settings.sourceTable} · колонок: ${columns}`;
    return `${settings.sourceSchema}.${settings.sourceTable}`;
  }

  function currentTargetColumnsHint() {
    const columns = targetColumnOptions.length;
    if (!settings.targetSchema || !settings.targetTable) return '';
    if (columns) return `${settings.targetSchema}.${settings.targetTable} · колонок: ${columns}`;
    return `${settings.targetSchema}.${settings.targetTable}`;
  }

  let settings = normalizeSettings(initialSettings);
  let lastInitialSignature = JSON.stringify(settings);
  let columnsCache = {};
  let templatesLoading = false;
  let templatesError = '';
  let templateSaving = false;
  let templateDeleting = false;
  let drafts = [];
  let selectedDraftId = 0;
  let templateNameInput = '';
  let templateDescriptionInput = '';
  let templateSearch = '';
  let previewLoading = false;
  let previewError = '';
  let previewUpdatedAt = '';
  let draftPreviewStep = null;

  let incomingDescriptors = [];
  let primaryIncomingDescriptor = null;
  let incomingDescriptorFields = [];
  let incomingDescriptorRows = [];
  let incomingDescriptorColumns = [];
  let targetColumnOptions = [];
  let sourceTableColumnOptions = [];
  let explicitMappings = [];
  let mappingRows = [];
  let keyFieldList = [];
  let outputDescriptorFields = [];
  let section1FieldItems = [];
  let canonicalInputContractColumns = [];
  let canonicalInputPreviewRows = [];
  let canonicalInputPreviewColumns = [];
  let canonicalOutputPreviewRows = [];
  let canonicalOutputPreviewColumns = [];
  let currentWriteResultColumns = [];
  let currentWriteResultRows = [];
  let compactInputStatus = 'Preview входа пока не запускался';
  let compactOutputStatus = 'Preview выхода пока не запускался';
  let previewStatusText = 'Предпросмотр не запускался';
  let selectedDraft = null;
  let filteredDrafts = [];
  let currentTemplateStoreId = 0;
  let currentTemplate = null;
  let selectedIsCurrent = false;

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
  $: currentTemplate = drafts.find((item) => Number(item.id || 0) === currentTemplateStoreId) || null;
  $: selectedIsCurrent = Boolean(selectedDraft && currentTemplate && Number(selectedDraft.id) === Number(currentTemplate.id));
  $: incomingDescriptors = Array.isArray(incomingDescriptor?.upstreamDescriptors) ? incomingDescriptor.upstreamDescriptors : [];
  $: primaryIncomingDescriptor = incomingDescriptors.length ? incomingDescriptors[0] : null;
  $: incomingDescriptorFields = descriptorFields(primaryIncomingDescriptor);
  $: incomingDescriptorRows = descriptorSampleRows(primaryIncomingDescriptor);
  $: incomingDescriptorColumns = descriptorSampleColumns(primaryIncomingDescriptor);
  $: targetColumnOptions = columnOptionsFor(settings.targetSchema, settings.targetTable);
  $: sourceTableColumnOptions = columnOptionsFor(settings.sourceSchema, settings.sourceTable);
  $: explicitMappings = parseFieldMappings(settings.fieldMappingsJson);
  $: outputDescriptorFields = descriptorFields(outputDescriptor);
  $: section1FieldItems =
    settings.sourceMode === 'table'
      ? sourceTableColumnOptions.map((name) => ({ name, alias: name, type: '', path: name }))
      : incomingDescriptorFields;
  $: canonicalInputContractColumns =
    settings.sourceMode === 'table'
      ? uniqueStrings(sourceTableColumnOptions)
      : uniqueStrings([...incomingDescriptorFields.map((field) => fieldDisplayName(field)), ...incomingDescriptorColumns]);
  $: mappingRows = buildMappingRows(canonicalInputContractColumns, targetColumnOptions, explicitMappings);
  $: keyFieldList = parseCsvText(settings.keyFields);
  $: canonicalInputPreviewRows = rowsFromNodeIo(draftPreviewStep?.input_json);
  $: canonicalInputPreviewColumns = columnsFromRows(canonicalInputPreviewRows, canonicalInputContractColumns);
  $: canonicalOutputPreviewRows = rowsFromNodeIo(draftPreviewStep?.output_json);
  $: canonicalOutputPreviewColumns = columnsFromRows(
    canonicalOutputPreviewRows,
    outputDescriptorFields.map((field) => fieldDisplayName(field))
  );
  $: currentWriteResultColumns = canonicalOutputPreviewColumns.length
    ? canonicalOutputPreviewColumns
    : outputDescriptorFields.map((field) => fieldDisplayName(field)).filter(Boolean);
  $: currentWriteResultRows = canonicalOutputPreviewRows;
  $: compactInputStatus = previewLoading
    ? 'Preview входа собирается...'
    : previewError
    ? 'Preview входа недоступен'
    : canonicalInputPreviewRows.length
    ? `Preview входа: ${canonicalInputPreviewRows.length} строк`
    : draftPreviewStep
    ? 'Preview входа не вернул строк'
    : 'Preview входа пока не запускался';
  $: compactOutputStatus = previewLoading
    ? 'Preview выхода собирается...'
    : previewError
    ? 'Preview выхода недоступен'
    : canonicalOutputPreviewRows.length
    ? `Preview выхода: ${canonicalOutputPreviewRows.length} строк`
    : draftPreviewStep
    ? currentWriteResultColumns.length
      ? 'Preview вернул структуру без строк'
      : 'Preview выхода не вернул строк'
    : 'Preview выхода пока не запускался';
  $: previewStatusText = previewLoading
    ? 'Предпросмотр собирается'
    : previewError
    ? 'Ошибка предпросмотра'
    : draftPreviewStep
    ? 'Предпросмотр актуален'
    : 'Предпросмотр не запускался';

  $: if (settings.sourceSchema && settings.sourceTable) {
    void ensureColumnsFor(settings.sourceSchema, settings.sourceTable);
  }

  $: if (settings.targetSchema && settings.targetTable) {
    void ensureColumnsFor(settings.targetSchema, settings.targetTable);
  }

  onMount(() => {
    void loadDrafts(storeIdFromSettings(initialSettings));
  });
</script>

<section class="panel" class:panel-embedded={embeddedMode}>
  <div class="write-layout">
    <section class="write-column">
      <div class="write-card">
        <div class="write-card-head">
          <div>
            <h3>Входящие параметры</h3>
            <p>Consume-view publish-блока предыдущей ноды.</p>
          </div>
        </div>

        <div class="meta-row">
          <span>{section1FieldItems.length} полей</span>
          <span>Формат: {descriptorOutputKindLabel(primaryIncomingDescriptor?.outputKind || 'unknown')}</span>
          <span>Источник: {settings.sourceMode === 'table' ? 'таблица' : 'upstream publish block'}</span>
        </div>

        {#if section1FieldItems.length}
          <div class="field-chip-wrap">
            {#each section1FieldItems as field}
              <span class="field-chip" title={fieldMeta(field)}>
                <strong>{fieldDisplayName(field)}</strong>
                {#if fieldAuxLabel(field)}
                  <small>{fieldAuxLabel(field)}</small>
                {/if}
              </span>
            {/each}
          </div>
        {:else}
          <div class="empty-box">Входной контракт пока не определён.</div>
        {/if}

        <div class="compact-preview">
          <div class="compact-preview-head">
            <strong>Preview входа</strong>
            <span class="hint">{compactInputStatus}</span>
          </div>
          {#if canonicalInputPreviewRows.length && canonicalInputPreviewColumns.length}
            <div class="preview-table-wrap">
              <table class="preview-table compact-table">
                <thead>
                  <tr>
                    {#each canonicalInputPreviewColumns as column}
                      <th>{column}</th>
                    {/each}
                  </tr>
                </thead>
                <tbody>
                  {#each canonicalInputPreviewRows.slice(0, Number(settings.previewLimit || 20)) as row}
                    <tr>
                      {#each canonicalInputPreviewColumns as column}
                        <td>{formatCell(row?.[column])}</td>
                      {/each}
                    </tr>
                  {/each}
                </tbody>
              </table>
            </div>
          {:else}
            <div class="empty-box">
              {#if previewError}
                {previewError}
              {:else if settings.sourceMode === 'table'}
                {currentTableColumnsHint() || 'Выбери таблицу-источник и запусти preview.'}
              {:else}
                Запусти preview, чтобы увидеть canonical input текущей ноды.
              {/if}
            </div>
          {/if}
        </div>
      </div>
    </section>

    <section class="write-column write-column-main">
      <div class="write-card">
        <div class="write-card-head">
          <div>
            <h3>Настройка записи</h3>
            <p>Выбирает, куда и как писать входной канонический поток.</p>
          </div>
        </div>

        <div class="subsection">
          <div class="subsection-head">
            <h4>Источник</h4>
          </div>
          <div class="form-grid form-grid-3">
            <label>
              Режим источника
              <select value={settings.sourceMode} on:change={(e) => patchSetting('sourceMode', selectValue(e))}>
                <option value="node">Предыдущая нода</option>
                <option value="table">Таблица</option>
              </select>
            </label>
            {#if settings.sourceMode === 'table'}
              <label>
                Схема источника
                <select value={settings.sourceSchema} on:change={(e) => patchSetting('sourceSchema', selectValue(e))}>
                  <option value="">Выбери схему</option>
                  {#each uniqueStrings(existingTables.map((item) => item.schema_name)) as schemaName}
                    <option value={schemaName}>{schemaName}</option>
                  {/each}
                </select>
              </label>
              <label>
                Таблица-источник
                <select value={settings.sourceTable} on:change={(e) => patchSetting('sourceTable', selectValue(e))}>
                  <option value="">Выбери таблицу</option>
                  {#each existingTables.filter((item) => !settings.sourceSchema || item.schema_name === settings.sourceSchema) as item}
                    <option value={item.table_name}>{item.table_name}</option>
                  {/each}
                </select>
              </label>
            {/if}
          </div>
          {#if settings.sourceMode === 'table'}
            <div class="form-grid form-grid-2">
              <label>
                Payload-колонка
                <select value={settings.sourceColumn} on:change={(e) => patchSetting('sourceColumn', selectValue(e))}>
                  <option value="">Взять строки таблицы как есть</option>
                  {#each sourceTableColumnOptions as column}
                    <option value={column}>{column}</option>
                  {/each}
                </select>
              </label>
              <label>
                Канал process bus
                <input value={settings.channel} on:input={(e) => patchSetting('channel', inputValue(e))} placeholder="Необязательно" />
              </label>
            </div>
          {/if}
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
                {#each uniqueStrings(existingTables.map((item) => item.schema_name)) as schemaName}
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
          <div class="inline-hint">{currentTargetColumnsHint() || 'Выбери таблицу назначения.'}</div>
        </div>

        <div class="subsection">
          <div class="subsection-head">
            <h4>Режим записи</h4>
          </div>
          <div class="form-grid form-grid-3">
            <label>
              Что делать
              <select value={settings.writeMode} on:change={(e) => patchSetting('writeMode', selectValue(e))}>
                <option value="insert">Insert</option>
                <option value="update_by_key">Update по ключу</option>
                <option value="upsert">Upsert</option>
              </select>
            </label>
            <label>
              Несопоставленные поля
              <select value={settings.unmappedMode} on:change={(e) => patchSetting('unmappedMode', selectValue(e))}>
                <option value="matched_only">Только сопоставленные</option>
                <option value="skip">Пропускать</option>
                <option value="error">Считать ошибкой</option>
              </select>
            </label>
            <label>
              Поведение при конфликте
              <select value={settings.conflictMode} on:change={(e) => patchSetting('conflictMode', selectValue(e))}>
                <option value="input_wins">Вход побеждает</option>
                <option value="keep_table">Оставить таблицу</option>
                <option value="only_if_empty">Только если пусто</option>
              </select>
            </label>
          </div>
          <div class="form-grid form-grid-2">
            <label>
              Batch size
              <input type="number" min="1" value={settings.batchSize} on:input={(e) => patchSetting('batchSize', inputValue(e))} />
            </label>
            <label>
              Preview limit
              <input type="number" min="1" value={settings.previewLimit} on:input={(e) => patchSetting('previewLimit', inputValue(e))} />
            </label>
          </div>
        </div>

        <div class="subsection">
          <div class="subsection-head">
            <h4>Сопоставление полей</h4>
            <div class="row-actions">
              <button type="button" class="mini-btn" on:click={autoMatchAll}>Автосопоставить</button>
              <button type="button" class="mini-btn" on:click={clearMappings}>Очистить</button>
            </div>
          </div>
          <div class="mapping-table-wrap">
            <table class="mapping-table">
              <thead>
                <tr>
                  <th>Источник</th>
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
                      <td><span class={`mapping-status mapping-status-${row.status}`}>{mappingStatusLabel(row.status)}</span></td>
                    </tr>
                  {/each}
                {:else}
                  <tr>
                    <td colspan="4" class="muted">Нет полей для сопоставления. Запусти preview или выбери таблицу.</td>
                  </tr>
                {/if}
              </tbody>
            </table>
          </div>
          <div class="inline-hint">Ключи: {keyFieldList.length ? keyFieldList.join(', ') : 'не выбраны'}</div>
        </div>

        <div class="subsection">
          <div class="subsection-head">
            <h4>Шаблон записи</h4>
            <div class="row-actions">
              <button type="button" class="mini-btn" on:click={applySelectedTemplate} disabled={!selectedDraft || selectedIsCurrent}>Применить</button>
              <button type="button" class="mini-btn" on:click={resetTemplateDraft}>Сбросить выбор</button>
            </div>
          </div>
          <div class="form-grid form-grid-2">
            <label>
              Название шаблона
              <input value={templateNameInput} on:input={(e) => (templateNameInput = inputValue(e))} placeholder="Например: WB silver write" />
            </label>
            <label>
              Привязка к store
              <input value={settings.templateStoreId} on:input={(e) => patchSetting('templateStoreId', inputValue(e))} placeholder="store id" />
            </label>
          </div>
          <label>
            Описание
            <textarea rows="2" value={templateDescriptionInput} on:input={(e) => (templateDescriptionInput = textareaValue(e))} placeholder="Короткое описание шаблона записи"></textarea>
          </label>
          <div class="row-actions">
            <button type="button" class="primary-btn" on:click={saveTemplate} disabled={templateSaving}>{templateSaving ? 'Сохраняем...' : 'Сохранить шаблон'}</button>
            <button type="button" class="danger-btn" on:click={deleteTemplate} disabled={!selectedDraft || templateDeleting}>
              {templateDeleting ? 'Удаляем...' : 'Удалить'}
            </button>
          </div>
          {#if templatesError}
            <div class="inline-error">{templatesError}</div>
          {/if}
          <label>
            Найти шаблон
            <input value={templateSearch} on:input={(e) => (templateSearch = inputValue(e))} placeholder="Название или описание" />
          </label>
          <div class="draft-list">
            {#if templatesLoading}
              <div class="empty-box">Загрузка шаблонов записи...</div>
            {:else if filteredDrafts.length}
              {#each filteredDrafts as draft (draft.id)}
                <button type="button" class:selected={selectedDraftId === Number(draft.id)} class="draft-item" on:click={() => selectDraft(draft)}>
                  <strong>{draft.name}</strong>
                  <span>{draft.description || 'Без описания'}</span>
                </button>
              {/each}
            {:else}
              <div class="empty-box">Шаблоны записи пока не найдены.</div>
            {/if}
          </div>
        </div>
      </div>
    </section>

    <section class="write-column">
      <div class="write-card">
        <div class="write-card-head">
          <div>
            <h3>Выходные параметры</h3>
            <p>Publish-view текущей write-node.</p>
          </div>
        </div>

        <div class="meta-row">
          <span>{outputDescriptorFields.length} полей</span>
          <span>Формат: {descriptorOutputKindLabel(outputDescriptor?.outputKind || 'unknown')}</span>
          <span>{compactOutputStatus}</span>
        </div>

        {#if outputDescriptorFields.length}
          <div class="field-chip-wrap">
            {#each outputDescriptorFields as field}
              <span class="field-chip" title={fieldMeta(field)}>
                <strong>{fieldDisplayName(field)}</strong>
                {#if fieldAuxLabel(field)}
                  <small>{fieldAuxLabel(field)}</small>
                {/if}
              </span>
            {/each}
          </div>
        {:else}
          <div class="empty-box">Publish contract пока не определён.</div>
        {/if}

        <div class="compact-preview">
          <div class="compact-preview-head">
            <strong>Preview выхода</strong>
            <span class="hint">{compactOutputStatus}</span>
          </div>
          {#if canonicalOutputPreviewRows.length && canonicalOutputPreviewColumns.length}
            <div class="preview-table-wrap">
              <table class="preview-table compact-table">
                <thead>
                  <tr>
                    {#each canonicalOutputPreviewColumns as column}
                      <th>{column}</th>
                    {/each}
                  </tr>
                </thead>
                <tbody>
                  {#each canonicalOutputPreviewRows.slice(0, Number(settings.previewLimit || 20)) as row}
                    <tr>
                      {#each canonicalOutputPreviewColumns as column}
                        <td>{formatCell(row?.[column])}</td>
                      {/each}
                    </tr>
                  {/each}
                </tbody>
              </table>
            </div>
          {:else}
            <div class="empty-box">
              {#if previewError}
                {previewError}
              {:else if currentWriteResultColumns.length}
                Preview вернул структуру без строк.
              {:else}
                Запусти preview, чтобы увидеть canonical output текущей ноды.
              {/if}
            </div>
          {/if}
        </div>
      </div>
    </section>
  </div>

  <section class="write-card result-card">
    <div class="write-card-head">
      <div>
        <h3>Предпросмотр результата</h3>
        <p>Один server preview-run по текущему graph_json до write-node.</p>
      </div>
      <button type="button" class="primary-btn" on:click={previewNow} disabled={previewLoading}>
        {previewLoading ? 'Собираем...' : draftPreviewStep ? 'Обновить предпросмотр' : 'Запустить предпросмотр'}
      </button>
    </div>

    <div class="result-status-row">
      <span class="status-pill">{previewStatusText}</span>
      <span class="hint">Run: {draftPreviewStep?.run_uid || '—'}</span>
      <span class="hint">Обновлено: {previewUpdatedAt ? new Date(previewUpdatedAt).toLocaleString('ru-RU') : '—'}</span>
      {#if lastRuntimeStep?.run_uid}
        <span class="hint">Last runtime: {lastRuntimeStep.run_uid}</span>
      {/if}
    </div>

    {#if previewError}
      <div class="inline-error-block">{previewError}</div>
    {:else if currentWriteResultRows.length && currentWriteResultColumns.length}
      <div class="result-grid-wrap">
        <table class="preview-table result-table">
          <thead>
            <tr>
              {#each currentWriteResultColumns as column}
                <th>{column}</th>
              {/each}
            </tr>
          </thead>
          <tbody>
            {#each currentWriteResultRows as row}
              <tr>
                {#each currentWriteResultColumns as column}
                  <td>{formatCell(row?.[column])}</td>
                {/each}
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {:else if currentWriteResultColumns.length}
      <div class="empty-box">Preview-run завершился без строк. Ниже показана структура publish-result.</div>
      <div class="field-chip-wrap">
        {#each currentWriteResultColumns as column}
          <span class="field-chip"><strong>{column}</strong></span>
        {/each}
      </div>
    {:else}
      <div class="empty-box">Запусти preview, чтобы увидеть табличный результат write-node.</div>
    {/if}
  </section>
</section>

<style>
  .panel {
    display: flex;
    flex-direction: column;
    gap: 16px;
    min-width: 0;
  }

  .panel.panel-embedded {
    gap: 12px;
  }

  .write-layout {
    display: grid;
    grid-template-columns: minmax(260px, 0.95fr) minmax(420px, 1.45fr) minmax(260px, 0.95fr);
    gap: 16px;
    min-width: 0;
    align-items: start;
  }

  .write-column {
    min-width: 0;
  }

  .write-card {
    display: flex;
    flex-direction: column;
    gap: 14px;
    padding: 16px;
    border: 1px solid #dbe4f0;
    border-radius: 16px;
    background: #f8fbff;
    min-width: 0;
  }

  .write-card-head,
  .subsection-head {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 12px;
  }

  .write-card-head h3,
  .subsection-head h4 {
    margin: 0;
    color: #0f172a;
  }

  .write-card-head p {
    margin: 4px 0 0;
    color: #64748b;
    font-size: 12px;
    line-height: 1.45;
  }

  .meta-row,
  .result-status-row,
  .row-actions {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    align-items: center;
  }

  .meta-row span,
  .status-pill {
    padding: 4px 8px;
    border: 1px solid #dbe4f0;
    border-radius: 999px;
    background: #fff;
    color: #475569;
    font-size: 11px;
  }

  .field-chip-wrap {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
  }

  .field-chip {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    max-width: 100%;
    padding: 6px 10px;
    border: 1px solid #dbe4f0;
    border-radius: 999px;
    background: #fff;
  }

  .field-chip strong {
    color: #0f172a;
    font-size: 12px;
    font-weight: 600;
  }

  .field-chip small {
    color: #64748b;
    font-size: 11px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 240px;
  }

  .compact-preview {
    display: flex;
    flex-direction: column;
    gap: 8px;
    min-width: 0;
  }

  .compact-preview-head {
    display: flex;
    justify-content: space-between;
    gap: 10px;
    align-items: center;
  }

  .subsection {
    display: flex;
    flex-direction: column;
    gap: 10px;
    padding-top: 4px;
    border-top: 1px dashed #dbe4f0;
  }

  .subsection:first-of-type {
    border-top: 0;
    padding-top: 0;
  }

  .form-grid {
    display: grid;
    gap: 12px;
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
    color: #334155;
    font-size: 12px;
    font-weight: 600;
  }

  input,
  select,
  textarea,
  button {
    font: inherit;
  }

  input,
  select,
  textarea {
    width: 100%;
    min-width: 0;
    padding: 10px 12px;
    border: 1px solid #cbd5e1;
    border-radius: 12px;
    background: #fff;
    color: #0f172a;
  }

  textarea {
    resize: vertical;
    min-height: 72px;
  }

  .primary-btn,
  .mini-btn,
  .danger-btn {
    border: 0;
    border-radius: 12px;
    padding: 10px 14px;
    cursor: pointer;
    transition: opacity 0.15s ease;
  }

  .primary-btn {
    background: #0f172a;
    color: #fff;
  }

  .mini-btn {
    background: #e2e8f0;
    color: #0f172a;
  }

  .danger-btn {
    background: #fee2e2;
    color: #b91c1c;
  }

  .primary-btn:disabled,
  .mini-btn:disabled,
  .danger-btn:disabled {
    cursor: default;
    opacity: 0.6;
  }

  .mapping-table-wrap,
  .preview-table-wrap,
  .result-grid-wrap,
  .draft-list {
    min-width: 0;
    overflow: auto;
    border: 1px solid #dbe4f0;
    border-radius: 12px;
    background: #fff;
  }

  .preview-table,
  .mapping-table {
    width: 100%;
    border-collapse: collapse;
    min-width: max-content;
  }

  .preview-table th,
  .preview-table td,
  .mapping-table th,
  .mapping-table td {
    padding: 8px 10px;
    border-bottom: 1px solid #eef2f7;
    text-align: left;
    font-size: 12px;
    color: #0f172a;
    vertical-align: top;
  }

  .preview-table th,
  .mapping-table th {
    position: sticky;
    top: 0;
    background: #f8fafc;
    color: #475569;
    z-index: 1;
  }

  .compact-table th,
  .compact-table td {
    padding: 6px 8px;
    font-size: 11px;
  }

  .result-grid-wrap {
    max-height: 420px;
  }

  .result-table th,
  .result-table td {
    white-space: nowrap;
  }

  .mapping-status {
    display: inline-flex;
    align-items: center;
    padding: 4px 8px;
    border-radius: 999px;
    font-size: 11px;
    border: 1px solid transparent;
  }

  .mapping-status-mapped {
    color: #166534;
    background: #dcfce7;
    border-color: #bbf7d0;
  }

  .mapping-status-unmapped {
    color: #92400e;
    background: #fef3c7;
    border-color: #fde68a;
  }

  .mapping-status-missing_target {
    color: #991b1b;
    background: #fee2e2;
    border-color: #fecaca;
  }

  .key-cell {
    text-align: center;
    width: 64px;
  }

  .empty-box,
  .inline-error-block {
    padding: 12px;
    border-radius: 12px;
    font-size: 12px;
    line-height: 1.45;
  }

  .empty-box {
    border: 1px dashed #cbd5e1;
    background: #fff;
    color: #64748b;
  }

  .inline-error,
  .inline-error-block {
    border: 1px solid #fecaca;
    background: #fef2f2;
    color: #b91c1c;
    border-radius: 12px;
    padding: 10px 12px;
  }

  .inline-hint,
  .hint,
  .muted {
    color: #64748b;
    font-size: 12px;
  }

  .draft-item {
    display: flex;
    flex-direction: column;
    gap: 4px;
    width: 100%;
    padding: 10px 12px;
    border: 0;
    border-bottom: 1px solid #eef2f7;
    background: #fff;
    text-align: left;
    cursor: pointer;
  }

  .draft-item.selected {
    background: #eff6ff;
  }

  @media (max-width: 1180px) {
    .write-layout {
      grid-template-columns: 1fr;
    }
  }

  @media (max-width: 900px) {
    .form-grid-2,
    .form-grid-3 {
      grid-template-columns: 1fr;
    }
  }
</style>
