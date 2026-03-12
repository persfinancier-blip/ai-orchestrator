
<script>
  import { createEventDispatcher, onMount } from 'svelte';
  import ComputedExpressionBuilder from '../components/ComputedExpressionBuilder.svelte';
  import {
    buildComputedFieldPreview,
    createComputedFieldRule,
    duplicateComputedFieldRule,
    normalizeComputedFieldRule,
    serializeComputedFieldRule
  } from './computedFieldBuilderCore.js';

  export let apiBase;
  export let apiJson;
  export let headers;
  export let existingTables = [];
  export let initialSettings = {};
  export let embeddedMode = false;

  const dispatch = createEventDispatcher();
  const DEFAULT_SETTINGS = {
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
  const JOIN_MODE_OPTIONS = [
    { value: 'inner', label: 'Только совпавшие строки' },
    { value: 'left', label: 'Все строки текущих данных' },
    { value: 'right', label: 'Все строки таблицы' },
    { value: 'full', label: 'Все строки из обоих наборов' }
  ];
  const CONFLICT_MODE_OPTIONS = [
    { value: 'suffix', label: 'Добавить суффикс' },
    { value: 'replace', label: 'Перезаписать текущее поле' },
    { value: 'skip', label: 'Пропустить совпавшее имя' }
  ];
  const FILTER_OPERATOR_OPTIONS = [
    { value: '=', label: 'Равно' },
    { value: '!=', label: 'Не равно' },
    { value: '>', label: 'Больше' },
    { value: '>=', label: 'Больше или равно' },
    { value: '<', label: 'Меньше' },
    { value: '<=', label: 'Меньше или равно' },
    { value: 'contains', label: 'Содержит' },
    { value: 'not_contains', label: 'Не содержит' },
    { value: 'empty', label: 'Пусто' },
    { value: 'not_empty', label: 'Не пусто' },
    { value: 'between', label: 'Между' },
    { value: 'in_list', label: 'В списке' }
  ];
  const OUTPUT_MODE_OPTIONS = [
    { value: 'rows', label: 'Набор строк' },
    { value: 'aggregated_values', label: 'Агрегированные значения' },
    { value: 'named_output_params', label: 'Именованные выходные параметры' }
  ];
  const AGGREGATE_OPTIONS = [
    { value: 'count', label: 'Количество' },
    { value: 'sum', label: 'Сумма' },
    { value: 'min', label: 'Минимум' },
    { value: 'max', label: 'Максимум' },
    { value: 'avg', label: 'Среднее' }
  ];
  const PREVIEW_PARAMS_PLACEHOLDER = '{"sku":"WB123","ids":[1,2,3]}';
  const OUTPUT_EXPRESSION_PLACEHOLDER = '{price} * 0.9';

  const str = (v) => String(v ?? '').trim();
  const csv = (v) => String(v || '').split(',').map((x) => x.trim()).filter(Boolean);
  const parseJson = (raw, fallback) => {
    if (raw && typeof raw === 'object') return raw;
    const txt = str(raw);
    if (!txt) return fallback;
    try { return JSON.parse(txt); } catch { return fallback; }
  };
  const pretty = (v, fallback = '[]') => { try { return JSON.stringify(v, null, 2); } catch { return fallback; } };
  const fieldLabel = (item) => item?.type ? `${item.name} (${item.type})` : item.name;
  const describeParamValue = (value) => {
    if (Array.isArray(value)) return { type: 'array', summary: `${value.length} эл.` };
    if (value === null) return { type: 'null', summary: 'null' };
    if (typeof value === 'object') return { type: 'json', summary: `${Object.keys(value || {}).length} полей` };
    if (typeof value === 'number') return { type: Number.isInteger(value) ? 'integer' : 'numeric', summary: String(value) };
    if (typeof value === 'boolean') return { type: 'boolean', summary: value ? 'true' : 'false' };
    const text = String(value ?? '');
    return { type: 'text', summary: text.length > 32 ? `${text.slice(0, 32)}…` : text || 'пусто' };
  };
  const inferFieldTypeFromRows = (rows, fieldName) => {
    for (const row of Array.isArray(rows) ? rows : []) {
      const value = row?.[fieldName];
      if (value === undefined || value === null || value === '') continue;
      if (typeof value === 'number') return Number.isInteger(value) ? 'integer' : 'numeric';
      if (typeof value === 'boolean') return 'boolean';
      if (typeof value === 'object') return 'json';
      if (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}/.test(value)) return 'timestamp';
      return 'text';
    }
    return '';
  };

  function normalizeSettings(raw) {
    const src = raw && typeof raw === 'object' ? raw : {};
    const next = { ...DEFAULT_SETTINGS };
    for (const key of Object.keys(DEFAULT_SETTINGS)) {
      const value = src[key];
      if (value === undefined || value === null) continue;
      next[key] = typeof value === 'string' ? value : JSON.stringify(value);
    }
    next.baseSchema = str(src.baseSchema || src.base_schema);
    next.baseTable = str(src.baseTable || src.base_table);
    next.baseAlias = str(src.baseAlias || src.base_alias || 'base') || 'base';
    next.joinedSourcesJson = pretty(src.joinedSources || src.joined_sources || parseJson(next.joinedSourcesJson, []), '[]');
    next.inputSourcesJson = pretty(src.inputSources || src.input_sources || parseJson(next.inputSourcesJson, []), '[]');
    next.selectedFieldsJson = pretty(src.selectedFields || src.selected_fields || parseJson(next.selectedFieldsJson, []), '[]');
    next.computedFieldsJson = pretty(src.computedFields || src.computed_fields || parseJson(next.computedFieldsJson, []), '[]');
    next.filterRulesJson = pretty(src.filterRules || src.filter_rules || parseJson(next.filterRulesJson, []), '[]');
    next.filterLogic = str(src.filterLogic || src.filter_logic || 'and').toLowerCase() === 'or' ? 'or' : 'and';
    next.dedupeMode = str(src.dedupeMode || src.dedupe_mode);
    next.dedupeFields = Array.isArray(src.dedupeFields || src.dedupe_fields) ? (src.dedupeFields || src.dedupe_fields).join(', ') : str(src.dedupeFields || src.dedupe_fields);
    next.dedupeKeep = str(src.dedupeKeep || src.dedupe_keep || 'first') || 'first';
    next.groupByFields = Array.isArray(src.groupByFields || src.group_by_fields) ? (src.groupByFields || src.group_by_fields).join(', ') : str(src.groupByFields || src.group_by_fields);
    next.aggregateRulesJson = pretty(src.aggregateRules || src.aggregate_rules || parseJson(next.aggregateRulesJson, []), '[]');
    next.outputMode = str(src.outputMode || src.output_mode || 'rows') || 'rows';
    next.outputParamsMappingJson = pretty(src.outputParamsMapping || src.output_params_mapping || parseJson(next.outputParamsMappingJson, []), '[]');
    next.previewInputParamsJson = pretty(src.previewInputParams || src.preview_input_params || parseJson(next.previewInputParamsJson, {}), '{}');
    next.batchSize = str(src.batchSize || src.batch_size || '200') || '200';
    next.previewLimit = str(src.previewLimit || src.preview_limit || '20') || '20';
    next.channel = str(src.channel);
    return next;
  }

  const normalizeJoinedSource = (item, index) => ({
    id: str(item?.id || `join_${index + 1}`),
    sourceType: str(item?.sourceType || item?.source_type).toLowerCase() === 'input_param' ? 'input_param' : 'table',
    sourceSchema: str(item?.sourceSchema || item?.source_schema),
    sourceTable: str(item?.sourceTable || item?.source_table),
    parameterName: str(item?.parameterName || item?.parameter_name),
    alias: str(item?.alias || `src_${index + 1}`),
    joinMode: str(item?.joinMode || item?.join_mode || 'inner') || 'inner',
    joinKeys: parseJson(item?.joinKeys || item?.join_keys, []).map((rule) => ({ leftField: str(rule?.leftField || rule?.left_field), rightField: str(rule?.rightField || rule?.right_field) })).filter((rule) => rule.leftField || rule.rightField),
    selectedFields: parseJson(item?.selectedFields || item?.selected_fields, []).map((field) => str(field)).filter(Boolean),
    prefix: str(item?.prefix),
    conflictMode: str(item?.conflictMode || item?.conflict_mode || 'suffix') || 'suffix'
  });
  const normalizeInputSource = (item, index) => ({
    id: str(item?.id || `param_${index + 1}`),
    parameterName: str(item?.parameterName || item?.parameter_name),
    bindMode: str(item?.bindMode || item?.bind_mode).toLowerCase() === 'rows' ? 'rows' : 'broadcast_fields',
    fieldMapping: parseJson(item?.fieldMapping || item?.field_mapping, []).map((rule) => ({ sourceField: str(rule?.sourceField || rule?.source_field), targetField: str(rule?.targetField || rule?.target_field) })).filter((rule) => rule.targetField)
  });
  const normalizeSelectedField = (item, index, baseAlias) => ({ sourceAlias: str(item?.sourceAlias || item?.source_alias || baseAlias), fieldName: str(item?.fieldName || item?.field_name), outputName: str(item?.outputName || item?.output_name || item?.fieldName || `field_${index + 1}`) });
  const normalizeFilterRule = (item) => ({ field: str(item?.field), expression: str(item?.expression), operator: str(item?.operator || '='), value: String(item?.value ?? ''), secondValue: String(item?.secondValue ?? item?.second_value ?? ''), caseSensitive: Boolean(item?.caseSensitive ?? item?.case_sensitive) });
  const normalizeOutputMapping = (item, index) => ({ outputParamName: str(item?.outputParamName || item?.output_param_name || `output_${index + 1}`), sourceField: str(item?.sourceField || item?.source_field), expression: str(item?.expression), mode: str(item?.mode).toLowerCase() === 'array' ? 'array' : 'scalar' });
  const normalizeAggregateRule = (item) => ({ field: str(item?.field), op: str(item?.op || item?.operator || 'count').toLowerCase() || 'count', as: str(item?.as || item?.alias) });

  let settings = normalizeSettings(initialSettings);
  let lastInitialSignature = JSON.stringify(settings);
  let columnsCache = {};
  let previewLoading = false;
  let previewError = '';
  let previewData = null;
  let previewUpdatedAt = '';
  let joinedSources = [];
  let inputSources = [];
  let selectedFields = [];
  let computedFields = [];
  let filterRules = [];
  let outputMappings = [];
  let aggregateRules = [];
  let previewInputParamsValue = {};
  let schemaOptions = [];
  let baseTableOptions = [];
  let baseColumnsMeta = [];
  let inputPreviewColumns = [];
  let inputPreviewRows = [];
  let resultPreviewColumns = [];
  let resultPreviewRows = [];
  let previewWarnings = [];
  let previewSteps = [];
  let joinSummaries = [];
  let outputParamsPreview = {};
  let inputParamNames = [];
  let incomingPreviewParams = [];

  $: {
    const next = normalizeSettings(initialSettings);
    const signature = JSON.stringify(next);
    if (signature !== lastInitialSignature) {
      lastInitialSignature = signature;
      settings = next;
    }
  }
  $: joinedSources = parseJson(settings.joinedSourcesJson, []).map(normalizeJoinedSource);
  $: inputSources = parseJson(settings.inputSourcesJson, []).map(normalizeInputSource);
  $: selectedFields = parseJson(settings.selectedFieldsJson, []).map((item, index) => normalizeSelectedField(item, index, settings.baseAlias || 'base'));
  $: computedFields = parseJson(settings.computedFieldsJson, []).map((row) => normalizeComputedFieldRule(row));
  $: filterRules = parseJson(settings.filterRulesJson, []).map(normalizeFilterRule);
  $: outputMappings = parseJson(settings.outputParamsMappingJson, []).map(normalizeOutputMapping);
  $: aggregateRules = parseJson(settings.aggregateRulesJson, []).map(normalizeAggregateRule);
  $: previewInputParamsValue = parseJson(settings.previewInputParamsJson, {});
  $: schemaOptions = Array.from(new Set(existingTables.map((item) => item.schema_name))).filter(Boolean);
  $: baseTableOptions = existingTables.filter((item) => !settings.baseSchema || item.schema_name === settings.baseSchema);
  $: baseColumnsMeta = columnsCache[`${settings.baseSchema}.${settings.baseTable}`] || [];
  $: inputPreviewColumns = Array.isArray(previewData?.input_columns) ? previewData.input_columns : [];
  $: inputPreviewRows = Array.isArray(previewData?.input_sample_rows) ? previewData.input_sample_rows : [];
  $: resultPreviewColumns = Array.isArray(previewData?.columns) ? previewData.columns : [];
  $: resultPreviewRows = Array.isArray(previewData?.sample_rows) ? previewData.sample_rows : [];
  $: previewWarnings = Array.isArray(previewData?.warnings) ? previewData.warnings : [];
  $: previewSteps = Array.isArray(previewData?.stats?.applied_steps) ? previewData.stats.applied_steps : [];
  $: joinSummaries = Array.isArray(previewData?.join_summaries) ? previewData.join_summaries : [];
  $: outputParamsPreview = previewData?.output_params && typeof previewData.output_params === 'object' ? previewData.output_params : {};
  $: inputParamNames = Array.from(new Set([...Object.keys(previewInputParamsValue || {}), ...joinedSources.filter((item) => item.sourceType === 'input_param').map((item) => item.parameterName), ...inputSources.map((item) => item.parameterName)].filter(Boolean)));
  $: incomingPreviewParams =
    previewInputParamsValue && typeof previewInputParamsValue === 'object' && !Array.isArray(previewInputParamsValue)
      ? Object.entries(previewInputParamsValue).map(([name, value]) => ({ name, ...describeParamValue(value) }))
      : [];
  $: if (settings.baseSchema && settings.baseTable) void ensureColumnsFor(settings.baseSchema, settings.baseTable);
  $: for (const source of joinedSources) if (source.sourceType === 'table' && source.sourceSchema && source.sourceTable) void ensureColumnsFor(source.sourceSchema, source.sourceTable);

  function dispatchSettings(next) { settings = { ...next }; dispatch('configChange', { settings }); }
  function patchSetting(key, value) { dispatchSettings({ ...settings, [key]: value }); }
  function patchJsonSetting(key, value, fallback = '[]') { patchSetting(key, pretty(value, fallback)); }
  const selectValue = (event) => event.currentTarget?.value ?? '';
  const inputValue = (event) => event.currentTarget?.value ?? '';
  const textareaValue = (event) => event.currentTarget?.value ?? '';
  const checkboxChecked = (event) => Boolean(event.currentTarget?.checked);
  function clearSelectValue(event) { if (event.currentTarget) event.currentTarget.value = ''; }

  async function ensureColumnsFor(schema, table) {
    const s = str(schema);
    const t = str(table);
    if (!s || !t) return;
    const key = `${s}.${t}`;
    if (Array.isArray(columnsCache[key])) return;
    try {
      const payload = await apiJson(`${apiBase}/columns?schema=${encodeURIComponent(s)}&table=${encodeURIComponent(t)}`, { headers: headers() });
      const cols = Array.isArray(payload?.columns) ? payload.columns.map((item) => ({ name: str(item?.name), type: str(item?.type).toLowerCase() || undefined })).filter((item) => item.name) : [];
      columnsCache = { ...columnsCache, [key]: cols };
    } catch {
      columnsCache = { ...columnsCache, [key]: [] };
    }
  }

  const columnMetaFor = (schema, table) => columnsCache[`${str(schema)}.${str(table)}`] || [];
  const columnOptionsFor = (schema, table) => columnMetaFor(schema, table).map((item) => item.name);
  const tableOptionsForSchema = (schema) => existingTables.filter((item) => !schema || item.schema_name === schema);
  function parameterFieldOptions(parameterName) {
    const value = previewInputParamsValue?.[parameterName];
    const rows =
      Array.isArray(value)
        ? value
        : value && typeof value === 'object'
        ? [value]
        : value === undefined || value === null
        ? []
        : [{ value }];
    return Array.from(new Set(rows.flatMap((row) => (row && typeof row === 'object' && !Array.isArray(row) ? Object.keys(row) : [])))).filter(Boolean);
  }
  function inputMappingOptions() {
    const out = [];
    for (const source of inputSources) for (const mapping of source.fieldMapping) out.push({ sourceAlias: source.parameterName || source.id, fieldName: mapping.targetField, type: '', outputName: mapping.targetField });
    return out;
  }
  function joinedProjectionOptions() {
    const out = [];
    for (const source of joinedSources) {
      const meta = source.sourceType === 'table' ? columnMetaFor(source.sourceSchema, source.sourceTable) : [];
      for (const fieldName of source.selectedFields) out.push({ sourceAlias: source.alias, fieldName, type: meta.find((item) => item.name === fieldName)?.type, outputName: source.prefix ? `${source.prefix}${fieldName}` : `${source.alias}_${fieldName}` });
    }
    return out;
  }
  function availableProjectionFields() {
    const baseAlias = settings.baseAlias || 'base';
    return [...baseColumnsMeta.map((col) => ({ sourceAlias: baseAlias, fieldName: col.name, type: col.type, outputName: col.name })), ...inputMappingOptions(), ...joinedProjectionOptions()].filter((item) => item.fieldName);
  }
  function workingFieldOptionsBeforeJoin(sourceIndex) {
    const out = [...baseColumnsMeta.map((col) => ({ fieldName: col.name, type: col.type }))];
    for (const source of inputSources) for (const mapping of source.fieldMapping) out.push({ fieldName: mapping.targetField, type: '' });
    for (const source of joinedSources.slice(0, sourceIndex)) {
      const meta = source.sourceType === 'table' ? columnMetaFor(source.sourceSchema, source.sourceTable) : [];
      for (const fieldName of source.selectedFields) out.push({ fieldName: source.prefix ? `${source.prefix}${fieldName}` : `${source.alias}_${fieldName}`, type: meta.find((item) => item.name === fieldName)?.type || '' });
    }
    return Array.from(new Map(out.map((item) => [item.fieldName, item])).values());
  }
  function joinedSourceFieldOptions(source) { return source.sourceType === 'input_param' ? parameterFieldOptions(source.parameterName) : columnOptionsFor(source.sourceSchema, source.sourceTable); }
  function projectionFieldType(sourceAlias, fieldName) {
    const baseAlias = settings.baseAlias || 'base';
    if (sourceAlias === baseAlias) return baseColumnsMeta.find((item) => item.name === fieldName)?.type || inferFieldTypeFromRows(inputPreviewRows, fieldName);
    const joined = joinedSources.find((item) => item.alias === sourceAlias);
    if (joined?.sourceType === 'table') return columnMetaFor(joined.sourceSchema, joined.sourceTable).find((item) => item.name === fieldName)?.type || '';
    return inferFieldTypeFromRows(inputPreviewRows, fieldName);
  }
  function outputFieldOptions() {
    const out = (selectedFields.length ? selectedFields.map((item) => ({ name: item.outputName, type: projectionFieldType(item.sourceAlias, item.fieldName) })) : availableProjectionFields().map((item) => ({ name: item.outputName, type: item.type || '' })));
    for (const item of computedFields) if (str(item?.name)) out.push({ name: str(item.name), type: str(item?.type) });
    for (const field of csv(settings.groupByFields)) out.push({ name: field, type: '' });
    for (const rule of aggregateRules) { const name = str(rule.as || `${rule.op}_${rule.field || 'rows'}`); if (name) out.push({ name, type: '' }); }
    return Array.from(new Map(out.map((item) => [item.name, item])).values()).filter((item) => item.name);
  }
  function fieldOptionsForComputed(index) {
    const base = selectedFields.length ? selectedFields.map((item) => ({ name: item.outputName, type: projectionFieldType(item.sourceAlias, item.fieldName) })) : availableProjectionFields().map((item) => ({ name: item.outputName, type: item.type || '' }));
    const previous = computedFields.slice(0, index).map((item) => ({ name: str(item?.name), type: str(item?.type) })).filter((item) => item.name);
    return Array.from(new Map([...base, ...previous].map((item) => [item.name, item])).values()).filter((item) => item.name);
  }

  function addJoinedSource() { patchJsonSetting('joinedSourcesJson', [...joinedSources, { id: `join_${joinedSources.length + 1}`, sourceType: 'table', sourceSchema: settings.baseSchema || '', sourceTable: '', parameterName: '', alias: `src_${joinedSources.length + 1}`, joinMode: 'inner', joinKeys: [], selectedFields: [], prefix: '', conflictMode: 'suffix' }]); }
  function updateJoinedSource(index, patch) { patchJsonSetting('joinedSourcesJson', joinedSources.map((row, idx) => idx === index ? { ...row, ...patch } : row)); }
  function removeJoinedSource(index) { patchJsonSetting('joinedSourcesJson', joinedSources.filter((_row, idx) => idx !== index)); }
  function addJoinRule(sourceIndex) { patchJsonSetting('joinedSourcesJson', joinedSources.map((row, idx) => idx === sourceIndex ? { ...row, joinKeys: [...row.joinKeys, { leftField: workingFieldOptionsBeforeJoin(sourceIndex)[0]?.fieldName || '', rightField: joinedSourceFieldOptions(joinedSources[sourceIndex])[0] || '' }] } : row)); }
  function updateJoinRule(sourceIndex, ruleIndex, patch) { patchJsonSetting('joinedSourcesJson', joinedSources.map((row, idx) => idx === sourceIndex ? { ...row, joinKeys: row.joinKeys.map((rule, ridx) => ridx === ruleIndex ? { ...rule, ...patch } : rule) } : row)); }
  function removeJoinRule(sourceIndex, ruleIndex) { patchJsonSetting('joinedSourcesJson', joinedSources.map((row, idx) => idx === sourceIndex ? { ...row, joinKeys: row.joinKeys.filter((_rule, ridx) => ridx !== ruleIndex) } : row)); }
  function addJoinedField(sourceIndex, fieldName) { const value = str(fieldName); if (!value) return; patchJsonSetting('joinedSourcesJson', joinedSources.map((row, idx) => idx === sourceIndex ? { ...row, selectedFields: Array.from(new Set([...row.selectedFields, value])) } : row)); }
  function removeJoinedField(sourceIndex, fieldName) { patchJsonSetting('joinedSourcesJson', joinedSources.map((row, idx) => idx === sourceIndex ? { ...row, selectedFields: row.selectedFields.filter((item) => item !== fieldName) } : row)); }
  function addAllJoinedFields(sourceIndex) { const source = joinedSources[sourceIndex]; if (source) updateJoinedSource(sourceIndex, { selectedFields: joinedSourceFieldOptions(source) }); }
  function addInputSource() { patchJsonSetting('inputSourcesJson', [...inputSources, { id: `param_${inputSources.length + 1}`, parameterName: inputParamNames[0] || '', bindMode: 'broadcast_fields', fieldMapping: [] }]); }
  function hasInputSourceBinding(parameterName) { return inputSources.some((item) => str(item?.parameterName) === str(parameterName)); }
  function bindInputSourceFromPreview(parameterName) {
    const name = str(parameterName);
    if (!name || hasInputSourceBinding(name)) return;
    patchJsonSetting('inputSourcesJson', [...inputSources, { id: `param_${inputSources.length + 1}`, parameterName: name, bindMode: 'broadcast_fields', fieldMapping: [] }]);
  }
  function updateInputSource(index, patch) { patchJsonSetting('inputSourcesJson', inputSources.map((row, idx) => idx === index ? { ...row, ...patch } : row)); }
  function removeInputSource(index) { patchJsonSetting('inputSourcesJson', inputSources.filter((_row, idx) => idx !== index)); }
  function addInputFieldMapping(sourceIndex) { const source = inputSources[sourceIndex]; if (!source) return; const sourceField = parameterFieldOptions(source.parameterName)[0] || ''; patchJsonSetting('inputSourcesJson', inputSources.map((row, idx) => idx === sourceIndex ? { ...row, fieldMapping: [...row.fieldMapping, { sourceField, targetField: sourceField || `field_${row.fieldMapping.length + 1}` }] } : row)); }
  function updateInputFieldMapping(sourceIndex, mappingIndex, patch) { patchJsonSetting('inputSourcesJson', inputSources.map((row, idx) => idx === sourceIndex ? { ...row, fieldMapping: row.fieldMapping.map((item, midx) => midx === mappingIndex ? { ...item, ...patch } : item) } : row)); }
  function removeInputFieldMapping(sourceIndex, mappingIndex) { patchJsonSetting('inputSourcesJson', inputSources.map((row, idx) => idx === sourceIndex ? { ...row, fieldMapping: row.fieldMapping.filter((_item, midx) => midx !== mappingIndex) } : row)); }
  function addSelectedField(item) { if (!item?.fieldName) return; patchJsonSetting('selectedFieldsJson', [...selectedFields, { sourceAlias: item.sourceAlias, fieldName: item.fieldName, outputName: item.outputName }]); }
  function addAllSelectedFields() { patchJsonSetting('selectedFieldsJson', availableProjectionFields().map((item) => ({ sourceAlias: item.sourceAlias, fieldName: item.fieldName, outputName: item.outputName }))); }
  function clearSelectedFields() { patchJsonSetting('selectedFieldsJson', []); }
  function updateSelectedField(index, patch) { patchJsonSetting('selectedFieldsJson', selectedFields.map((row, idx) => idx === index ? { ...row, ...patch } : row)); }
  function removeSelectedField(index) { patchJsonSetting('selectedFieldsJson', selectedFields.filter((_row, idx) => idx !== index)); }
  function addComputedField() { patchJsonSetting('computedFieldsJson', [...computedFields, createComputedFieldRule()]); }
  function updateComputedField(index, patch) { patchJsonSetting('computedFieldsJson', computedFields.map((row, idx) => idx === index ? serializeComputedFieldRule(normalizeComputedFieldRule({ ...row, ...patch })) : serializeComputedFieldRule(row))); }
  function duplicateComputedField(index) { const item = computedFields[index]; if (item) patchJsonSetting('computedFieldsJson', [...computedFields, duplicateComputedFieldRule(item)].map((row) => serializeComputedFieldRule(row))); }
  function removeComputedField(index) { patchJsonSetting('computedFieldsJson', computedFields.filter((_row, idx) => idx !== index).map((row) => serializeComputedFieldRule(row))); }
  function addFilterRule() { patchJsonSetting('filterRulesJson', [...filterRules, { field: '', expression: '', operator: '=', value: '', secondValue: '', caseSensitive: false }]); }
  function updateFilterRule(index, patch) { patchJsonSetting('filterRulesJson', filterRules.map((row, idx) => idx === index ? { ...row, ...patch } : row)); }
  function removeFilterRule(index) { patchJsonSetting('filterRulesJson', filterRules.filter((_row, idx) => idx !== index)); }
  function addAggregateRule() { patchJsonSetting('aggregateRulesJson', [...aggregateRules, { field: '', op: 'count', as: '' }]); }
  function updateAggregateRule(index, patch) { patchJsonSetting('aggregateRulesJson', aggregateRules.map((row, idx) => idx === index ? { ...row, ...patch } : row)); }
  function removeAggregateRule(index) { patchJsonSetting('aggregateRulesJson', aggregateRules.filter((_row, idx) => idx !== index)); }
  function addOutputMapping() { patchJsonSetting('outputParamsMappingJson', [...outputMappings, { outputParamName: '', sourceField: '', expression: '', mode: 'scalar' }]); }
  function updateOutputMapping(index, patch) { patchJsonSetting('outputParamsMappingJson', outputMappings.map((row, idx) => idx === index ? { ...row, ...patch } : row)); }
  function removeOutputMapping(index) { patchJsonSetting('outputParamsMappingJson', outputMappings.filter((_row, idx) => idx !== index)); }
  async function previewNow() {
    previewLoading = true;
    previewError = '';
    try {
      const payload = await apiJson(`${apiBase}/table-node-configs/preview`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({ config_json: { baseSchema: settings.baseSchema, baseTable: settings.baseTable, baseAlias: settings.baseAlias, joinedSources, inputSources, selectedFields, computedFields: computedFields.map((row) => serializeComputedFieldRule(row)), filterRules, filterLogic: settings.filterLogic, dedupeMode: settings.dedupeMode, dedupeFields: csv(settings.dedupeFields), dedupeKeep: settings.dedupeKeep, groupByFields: csv(settings.groupByFields), aggregateRules, outputMode: settings.outputMode, outputParamsMapping: outputMappings, previewInputParams: previewInputParamsValue, batchSize: settings.batchSize, previewLimit: settings.previewLimit, channel: settings.channel }, input_value: null, input_envelope: { contract_version: 'node_io_v1', rows: [], row_count: 0, meta: { output_params: previewInputParamsValue } }, cursor: { offset: 0 } })
      });
      previewData = payload?.preview || null;
      previewUpdatedAt = new Date().toISOString();
    } catch (e) {
      previewError = String(e?.message || e || 'Не удалось получить preview табличного набора');
      previewData = null;
    } finally {
      previewLoading = false;
    }
  }
  const hasBetweenOperator = (operator) => str(operator).toLowerCase() === 'between';
  onMount(() => { if (settings.baseSchema && settings.baseTable) void previewNow(); });
</script>

<div class:table-node-builder={true} class:table-node-builder-embedded={embeddedMode}>
  <div class="table-node-shell">
    <section class="shell-strip">
      <div class="shell-strip-copy">
        <h3>Входящие параметры</h3>
        <p>{incomingPreviewParams.length ? 'Показаны параметры из блока preview JSON. Реальный upstream envelope в этом UI пока не подгружается.' : 'В текущем UI доступны только тестовые параметры preview. Реальный upstream envelope здесь пока не читается.'}</p>
      </div>
      {#if incomingPreviewParams.length}
        <div class="shell-param-list">
          {#each incomingPreviewParams as item}
            <div class="shell-param-item">
              <div class="shell-param-main">
                <strong>{item.name}</strong>
                <div class="shell-param-toolbar">
                  <span class="shell-strip-chip">{item.type}</span>
                  <button type="button" class="mini-btn shell-bind-btn" on:click={() => bindInputSourceFromPreview(item.name)} disabled={hasInputSourceBinding(item.name)}>
                    {hasInputSourceBinding(item.name) ? 'Уже в настройке' : 'Использовать'}
                  </button>
                </div>
              </div>
              <span class="shell-param-meta">{item.summary}</span>
            </div>
          {/each}
        </div>
      {:else}
        <span class="shell-strip-note">Источник пока ограничен `previewInputParamsJson`.</span>
      {/if}
    </section>

    <section class="shell-section">
      <div class="shell-head">
        <div>
          <h3>Настройка ноды</h3>
          <p>Основной рабочий UI для настройки TABLE NODE.</p>
        </div>
      </div>
      <div class="shell-body">
  <section class="builder-card">
    <div class="builder-card-head"><div><h3>Основная таблица и вход</h3><p>Выбери базовую таблицу и при необходимости добавь тестовые входные параметры для server-side preview.</p></div><button type="button" class="mini-btn" on:click={previewNow} disabled={previewLoading || !settings.baseSchema || !settings.baseTable}>{previewLoading ? 'Обновление...' : 'Обновить preview'}</button></div>
    <div class="form-grid form-grid-3">
      <label>Схема<select value={settings.baseSchema} on:change={(e) => patchSetting('baseSchema', selectValue(e))}><option value="">Выбери схему</option>{#each schemaOptions as schemaName}<option value={schemaName}>{schemaName}</option>{/each}</select></label>
      <label>Основная таблица<select value={settings.baseTable} on:change={(e) => patchSetting('baseTable', selectValue(e))}><option value="">Выбери таблицу</option>{#each baseTableOptions as table}<option value={table.table_name}>{table.table_name}</option>{/each}</select></label>
      <label>Алиас основной таблицы<input value={settings.baseAlias} on:input={(e) => patchSetting('baseAlias', inputValue(e))} placeholder="base" /></label>
    </div>
    <label>Тестовые входные параметры для preview (JSON)<textarea rows="6" value={settings.previewInputParamsJson} on:input={(e) => patchSetting('previewInputParamsJson', textareaValue(e))} placeholder={PREVIEW_PARAMS_PLACEHOLDER}></textarea><span class="hint">Используется только в preview. В runtime TABLE NODE получает параметры из upstream envelope.</span></label>
  </section>

  <section class="builder-card">
    <div class="builder-card-head"><div><h3>Преобразование табличного набора</h3><p>Собери рабочий набор данных из таблиц и входных параметров, затем подготовь поля результата и output params.</p></div></div>
    <div class="subsection">
      <div class="subsection-head"><h4>Дополнительные источники</h4><button type="button" class="mini-btn" on:click={addJoinedSource}>Добавить источник</button></div>
      {#if joinedSources.length}
        <div class="stack-list">{#each joinedSources as source, sourceIndex (source.id)}<div class="item-card"><div class="item-card-head"><strong>{source.alias}</strong><button type="button" class="danger-btn" on:click={() => removeJoinedSource(sourceIndex)}>Удалить</button></div>
          <div class="form-grid form-grid-4">
            <label>Тип источника<select value={source.sourceType} on:change={(e) => updateJoinedSource(sourceIndex, { sourceType: selectValue(e) })}><option value="table">Таблица</option><option value="input_param">Входной параметр</option></select></label>
            {#if source.sourceType === 'table'}
              <label>Схема<select value={source.sourceSchema} on:change={(e) => updateJoinedSource(sourceIndex, { sourceSchema: selectValue(e), sourceTable: '' })}><option value="">Выбери схему</option>{#each schemaOptions as schemaName}<option value={schemaName}>{schemaName}</option>{/each}</select></label>
              <label>Таблица<select value={source.sourceTable} on:change={(e) => updateJoinedSource(sourceIndex, { sourceTable: selectValue(e) })}><option value="">Выбери таблицу</option>{#each tableOptionsForSchema(source.sourceSchema) as table}<option value={table.table_name}>{table.table_name}</option>{/each}</select></label>
            {:else}
              <label class="span-2">Параметр workflow<input value={source.parameterName} on:input={(e) => updateJoinedSource(sourceIndex, { parameterName: inputValue(e) })} list={`join_param_names_${sourceIndex}`} placeholder="orders_payload" /><datalist id={`join_param_names_${sourceIndex}`}>{#each inputParamNames as item}<option value={item}></option>{/each}</datalist></label>
            {/if}
            <label>Алиас<input value={source.alias} on:input={(e) => updateJoinedSource(sourceIndex, { alias: inputValue(e) })} /></label>
          </div>
          <div class="form-grid form-grid-3"><label>Тип объединения<select value={source.joinMode} on:change={(e) => updateJoinedSource(sourceIndex, { joinMode: selectValue(e) })}>{#each JOIN_MODE_OPTIONS as option}<option value={option.value}>{option.label}</option>{/each}</select></label><label>Префикс новых полей<input value={source.prefix} on:input={(e) => updateJoinedSource(sourceIndex, { prefix: inputValue(e) })} placeholder="src_" /></label><label>Конфликт имён полей<select value={source.conflictMode} on:change={(e) => updateJoinedSource(sourceIndex, { conflictMode: selectValue(e) })}>{#each CONFLICT_MODE_OPTIONS as option}<option value={option.value}>{option.label}</option>{/each}</select></label></div>
          <div class="subsection-head compact-head"><h5>Когда считать строки одинаковыми</h5><button type="button" class="mini-btn" on:click={() => addJoinRule(sourceIndex)}>Добавить условие</button></div>
          {#if source.joinKeys.length}<div class="stack-list compact-list">{#each source.joinKeys as rule, ruleIndex}<div class="form-grid form-grid-3 inline-card"><label>Поле текущих данных<select value={rule.leftField} on:change={(e) => updateJoinRule(sourceIndex, ruleIndex, { leftField: selectValue(e) })}><option value="">Выбери поле</option>{#each workingFieldOptionsBeforeJoin(sourceIndex) as option}<option value={option.fieldName}>{fieldLabel({ name: option.fieldName, type: option.type })}</option>{/each}</select></label><label>Поле источника<select value={rule.rightField} on:change={(e) => updateJoinRule(sourceIndex, ruleIndex, { rightField: selectValue(e) })}><option value="">Выбери поле</option>{#each joinedSourceFieldOptions(source) as option}<option value={option}>{option}</option>{/each}</select></label><div class="button-cell"><button type="button" class="danger-btn" on:click={() => removeJoinRule(sourceIndex, ruleIndex)}>Удалить</button></div></div>{/each}</div>{:else}<div class="hint">Добавь хотя бы одно условие связи.</div>{/if}
          <div class="subsection-head compact-head"><h5>Какие поля подтянуть</h5><div class="toolbar-inline"><select on:change={(e) => addJoinedField(sourceIndex, selectValue(e))} on:click={clearSelectValue}><option value="">Добавить поле</option>{#each joinedSourceFieldOptions(source) as option}<option value={option}>{option}</option>{/each}</select><button type="button" class="mini-btn" on:click={() => addAllJoinedFields(sourceIndex)}>Взять все поля</button></div></div>
          {#if source.selectedFields.length}<div class="chip-list">{#each source.selectedFields as fieldName}<button type="button" class="chip" on:click={() => removeJoinedField(sourceIndex, fieldName)}>{fieldName} ×</button>{/each}</div>{:else}<div class="hint">Выбери поля, которые нужно подтянуть в итоговый набор.</div>{/if}
        </div>{/each}</div>
      {:else}<div class="empty-box">Дополнительные источники не добавлены.</div>{/if}
    </div>
    <div class="subsection"><div class="subsection-head"><h4>Входные параметры workflow</h4><button type="button" class="mini-btn" on:click={addInputSource}>Добавить параметр</button></div>
      {#if inputSources.length}<div class="stack-list">{#each inputSources as source, sourceIndex (source.id)}<div class="item-card"><div class="item-card-head"><strong>{source.parameterName || source.id}</strong><button type="button" class="danger-btn" on:click={() => removeInputSource(sourceIndex)}>Удалить</button></div><div class="form-grid form-grid-3"><label>Имя параметра<input value={source.parameterName} on:input={(e) => updateInputSource(sourceIndex, { parameterName: inputValue(e) })} list={`input_param_names_${sourceIndex}`} placeholder="orders_payload" /><datalist id={`input_param_names_${sourceIndex}`}>{#each inputParamNames as item}<option value={item}></option>{/each}</datalist></label><label>Режим привязки<select value={source.bindMode} on:change={(e) => updateInputSource(sourceIndex, { bindMode: selectValue(e) })}><option value="broadcast_fields">Подставить поля во все строки</option><option value="rows">Источник-строки</option></select></label></div><div class="subsection-head compact-head"><h5>Сопоставление полей параметра</h5><button type="button" class="mini-btn" on:click={() => addInputFieldMapping(sourceIndex)}>Добавить поле</button></div>{#if source.fieldMapping.length}<div class="stack-list compact-list">{#each source.fieldMapping as mapping, mappingIndex}<div class="form-grid form-grid-3 inline-card"><label>Поле параметра<select value={mapping.sourceField} on:change={(e) => updateInputFieldMapping(sourceIndex, mappingIndex, { sourceField: selectValue(e) })}><option value="">Выбери поле</option>{#each parameterFieldOptions(source.parameterName) as option}<option value={option}>{option}</option>{/each}</select></label><label>Имя в рабочем наборе<input value={mapping.targetField} on:input={(e) => updateInputFieldMapping(sourceIndex, mappingIndex, { targetField: inputValue(e) })} /></label><div class="button-cell"><button type="button" class="danger-btn" on:click={() => removeInputFieldMapping(sourceIndex, mappingIndex)}>Удалить</button></div></div>{/each}</div>{:else}<div class="hint">Параметр добавлен, но поля ещё не сопоставлены.</div>{/if}</div>{/each}</div>{:else}<div class="empty-box">Источники из параметров workflow не добавлены.</div>{/if}
    </div>
    <div class="subsection"><div class="subsection-head"><h4>Поля результата</h4><div class="toolbar-inline"><select on:change={(e) => { const key = selectValue(e); const item = availableProjectionFields().find((option) => `${option.sourceAlias}::${option.fieldName}` === key); if (item) addSelectedField(item); }} on:click={clearSelectValue}><option value="">Добавить поле</option>{#each availableProjectionFields() as option}<option value={`${option.sourceAlias}::${option.fieldName}`}>{option.outputName} ← {option.sourceAlias}.{option.fieldName}</option>{/each}</select><button type="button" class="mini-btn" on:click={addAllSelectedFields}>Взять все поля</button><button type="button" class="mini-btn" on:click={clearSelectedFields}>Очистить</button></div></div>{#if selectedFields.length}<div class="stack-list compact-list">{#each selectedFields as field, fieldIndex}<div class="form-grid form-grid-4 inline-card"><label>Источник<input value={`${field.sourceAlias}.${field.fieldName}`} readonly /></label><label>Выходное имя<input value={field.outputName} on:input={(e) => updateSelectedField(fieldIndex, { outputName: inputValue(e) })} /></label><label>Тип<input value={projectionFieldType(field.sourceAlias, field.fieldName) || '-'} readonly /></label><div class="button-cell"><button type="button" class="danger-btn" on:click={() => removeSelectedField(fieldIndex)}>Удалить</button></div></div>{/each}</div>{:else}<div class="hint">Если поля результата не указаны, TABLE NODE вернёт все доступные поля.</div>{/if}</div>
    <div class="subsection"><div class="subsection-head"><h4>Вычисления</h4><button type="button" class="mini-btn" on:click={addComputedField}>Добавить вычисляемое поле</button></div>{#if computedFields.length}<div class="stack-list">{#each computedFields as field, fieldIndex}<div class="item-card"><div class="item-card-head"><strong>{field.name || `Вычисление ${fieldIndex + 1}`}</strong><div class="toolbar-inline"><button type="button" class="mini-btn" on:click={() => duplicateComputedField(fieldIndex)}>Дублировать</button><button type="button" class="danger-btn" on:click={() => removeComputedField(fieldIndex)}>Удалить</button></div></div><div class="form-grid form-grid-3"><label>Новое поле<input value={field.name || ''} on:input={(e) => updateComputedField(fieldIndex, { name: inputValue(e) })} /></label><label>Режим<select value={field.mode || 'builder'} on:change={(e) => updateComputedField(fieldIndex, { mode: selectValue(e) })}><option value="builder">Конструктор</option><option value="manual">Ручной ввод</option></select></label><label>Тип результата<select value={field.type || ''} on:change={(e) => updateComputedField(fieldIndex, { type: selectValue(e) })}><option value="">Без приведения</option><option value="text">Текст</option><option value="integer">Целое число</option><option value="numeric">Число</option><option value="boolean">Логическое</option><option value="timestamp">Дата/время</option><option value="json">JSON</option></select></label></div>{#if field.mode === 'manual'}<label>Формула<textarea rows="4" value={field.expression || ''} on:input={(e) => updateComputedField(fieldIndex, { expression: textareaValue(e) })}></textarea></label>{:else}<ComputedExpressionBuilder builder={field.builder} availableFields={fieldOptionsForComputed(fieldIndex)} fallbackFields={outputFieldOptions()} fieldTypes={Object.fromEntries(fieldOptionsForComputed(fieldIndex).map((item) => [item.name, item.type || '']))} on:change={(e) => updateComputedField(fieldIndex, { builder: e.detail.builder, expression: buildComputedFieldPreview({ ...field, builder: e.detail.builder }) })} />{/if}<div class="inline-preview">Предпросмотр: <code>{buildComputedFieldPreview(field)}</code></div></div>{/each}</div>{:else}<div class="empty-box">Вычисляемые поля не добавлены.</div>{/if}</div>
    <div class="subsection"><div class="subsection-head"><h4>Фильтры</h4><div class="toolbar-inline"><label class="inline-select">Логика<select value={settings.filterLogic} on:change={(e) => patchSetting('filterLogic', selectValue(e))}><option value="and">И</option><option value="or">ИЛИ</option></select></label><button type="button" class="mini-btn" on:click={addFilterRule}>Добавить фильтр</button></div></div>{#if filterRules.length}<div class="stack-list compact-list">{#each filterRules as rule, ruleIndex}<div class="item-card compact-card"><div class="form-grid form-grid-4"><label>Поле<select value={rule.field} on:change={(e) => updateFilterRule(ruleIndex, { field: selectValue(e) })}><option value="">Поле не выбрано</option>{#each outputFieldOptions() as option}<option value={option.name}>{fieldLabel(option)}</option>{/each}</select></label><label>Оператор<select value={rule.operator} on:change={(e) => updateFilterRule(ruleIndex, { operator: selectValue(e) })}>{#each FILTER_OPERATOR_OPTIONS as option}<option value={option.value}>{option.label}</option>{/each}</select></label><label>Значение<input value={rule.value} on:input={(e) => updateFilterRule(ruleIndex, { value: inputValue(e) })} /></label><div class="button-cell"><button type="button" class="danger-btn" on:click={() => removeFilterRule(ruleIndex)}>Удалить</button></div></div>{#if hasBetweenOperator(rule.operator)}<label>Второе значение диапазона<input value={rule.secondValue} on:input={(e) => updateFilterRule(ruleIndex, { secondValue: inputValue(e) })} /></label>{/if}<label>Выражение вместо поля (необязательно)<input value={rule.expression} on:input={(e) => updateFilterRule(ruleIndex, { expression: inputValue(e) })} placeholder="если нужно вычислять значение перед сравнением" /></label><label class="inline-check"><input type="checkbox" checked={Boolean(rule.caseSensitive)} on:change={(e) => updateFilterRule(ruleIndex, { caseSensitive: checkboxChecked(e) })} />Учитывать регистр</label></div>{/each}</div>{:else}<div class="empty-box">Фильтры не добавлены.</div>{/if}</div>
    <div class="subsection"><div class="subsection-head"><h4>Удаление дублей</h4></div><div class="form-grid form-grid-3"><label>Режим<select value={settings.dedupeMode} on:change={(e) => patchSetting('dedupeMode', selectValue(e))}><option value="">Не применять</option><option value="full_row">Полные дубликаты строк</option><option value="by_fields">По выбранным полям</option></select></label><label>Поля для дедупликации<input value={settings.dedupeFields} on:input={(e) => patchSetting('dedupeFields', inputValue(e))} placeholder="sku, offer_id" /></label><label>Какую запись оставить<select value={settings.dedupeKeep} on:change={(e) => patchSetting('dedupeKeep', selectValue(e))}><option value="first">Первую</option><option value="last">Последнюю</option></select></label></div></div>
    <div class="subsection"><div class="subsection-head"><h4>Группировки и агрегаты</h4><button type="button" class="mini-btn" on:click={addAggregateRule}>Добавить агрегат</button></div><div class="form-grid form-grid-2"><label>Поля группировки<input value={settings.groupByFields} on:input={(e) => patchSetting('groupByFields', inputValue(e))} placeholder="brand, category" /></label></div>{#if aggregateRules.length}<div class="stack-list compact-list">{#each aggregateRules as rule, ruleIndex}<div class="form-grid form-grid-4 inline-card"><label>Поле<select value={rule.field} on:change={(e) => updateAggregateRule(ruleIndex, { field: selectValue(e) })}><option value="">Выбери поле</option>{#each outputFieldOptions() as option}<option value={option.name}>{fieldLabel(option)}</option>{/each}</select></label><label>Агрегат<select value={rule.op} on:change={(e) => updateAggregateRule(ruleIndex, { op: selectValue(e) })}>{#each AGGREGATE_OPTIONS as option}<option value={option.value}>{option.label}</option>{/each}</select></label><label>Имя результата<input value={rule.as} on:input={(e) => updateAggregateRule(ruleIndex, { as: inputValue(e) })} placeholder="total_rows" /></label><div class="button-cell"><button type="button" class="danger-btn" on:click={() => removeAggregateRule(ruleIndex)}>Удалить</button></div></div>{/each}</div>{:else}<div class="hint">Если агрегаты не заданы, группировка не применяется.</div>{/if}</div>
    <div class="subsection"><div class="subsection-head"><h4>Исходящие параметры</h4><div class="toolbar-inline"><label class="inline-select">Режим<select value={settings.outputMode} on:change={(e) => patchSetting('outputMode', selectValue(e))}>{#each OUTPUT_MODE_OPTIONS as option}<option value={option.value}>{option.label}</option>{/each}</select></label><button type="button" class="mini-btn" on:click={addOutputMapping}>Добавить параметр</button></div></div>{#if outputMappings.length}<div class="stack-list compact-list">{#each outputMappings as mapping, mappingIndex}<div class="item-card compact-card"><div class="form-grid form-grid-4"><label>Имя выходного параметра<input value={mapping.outputParamName} on:input={(e) => updateOutputMapping(mappingIndex, { outputParamName: inputValue(e) })} /></label><label>Поле<select value={mapping.sourceField} on:change={(e) => updateOutputMapping(mappingIndex, { sourceField: selectValue(e) })}><option value="">Выбери поле</option>{#each outputFieldOptions() as option}<option value={option.name}>{fieldLabel(option)}</option>{/each}</select></label><label>Режим значения<select value={mapping.mode} on:change={(e) => updateOutputMapping(mappingIndex, { mode: selectValue(e) })}><option value="scalar">Скаляр</option><option value="array">Массив</option></select></label><div class="button-cell"><button type="button" class="danger-btn" on:click={() => removeOutputMapping(mappingIndex)}>Удалить</button></div></div><label>Выражение вместо поля (необязательно)<input value={mapping.expression} on:input={(e) => updateOutputMapping(mappingIndex, { expression: inputValue(e) })} placeholder={OUTPUT_EXPRESSION_PLACEHOLDER} /></label></div>{/each}</div>{:else}<div class="hint">При режиме «Набор строк» downstream-узлы получат весь result set через node_io_v1.rows. Именованные параметры добавляются здесь по необходимости.</div>{/if}</div>
  </section>
  <section class="builder-card"><div class="builder-card-head"><div><h3>Server-side preview</h3><p>Preview строится на сервере по реальной config TABLE NODE. Здесь видны warnings, join summary и output params.</p></div><button type="button" class="primary-btn" on:click={previewNow} disabled={previewLoading || !settings.baseSchema || !settings.baseTable}>{previewLoading ? 'Обновление...' : 'Обновить preview'}</button></div>{#if previewError}<div class="inline-error">{previewError}</div>{/if}<div class="subsection"><div class="subsection-head"><h4>Preview входа</h4></div><div class="preview-metrics"><span>Строк на входе: {previewData?.input_row_count ?? '-'}</span><span>Колонок на входе: {previewData?.input_column_count ?? '-'}</span><span>Источник: {previewData?.source_ref || '-'}</span><span>Обновлено: {previewUpdatedAt ? new Date(previewUpdatedAt).toLocaleString('ru-RU') : '-'}</span></div>{#if inputPreviewColumns.length}<div class="preview-columns">{#each inputPreviewColumns as column}<span>{column}</span>{/each}</div>{/if}{#if inputPreviewRows.length}<div class="preview-table-wrap"><table class="preview-table"><thead><tr>{#each inputPreviewColumns as column}<th>{column}</th>{/each}</tr></thead><tbody>{#each inputPreviewRows as row}<tr>{#each inputPreviewColumns as column}<td>{typeof row?.[column] === 'object' ? JSON.stringify(row?.[column]) : String(row?.[column] ?? '')}</td>{/each}</tr>{/each}</tbody></table></div>{:else}<div class="empty-box">Preview входа появится после выбора основной таблицы и запуска server-side preview.</div>{/if}</div><div class="subsection"><div class="subsection-head"><h4>Preview результата</h4></div><div class="preview-metrics"><span>Строк результата: {previewData?.row_count ?? '-'}</span><span>Колонок результата: {previewData?.column_count ?? '-'}</span><span>Лимит preview: {settings.previewLimit}</span><span>Пакет: {previewData?.batch?.returned_rows ?? 0} / {previewData?.batch?.batch_size ?? '-'}</span></div>{#if previewWarnings.length}<div class="warning-box"><strong>Предупреждения</strong><ul>{#each previewWarnings as warning}<li>{warning}</li>{/each}</ul></div>{/if}{#if previewSteps.length}<div class="summary-box"><strong>Применённые шаги</strong><div class="chip-list readonly-chip-list">{#each previewSteps as step}<span class="chip readonly">{step}</span>{/each}</div></div>{/if}{#if joinSummaries.length}<div class="summary-box"><strong>Сводка по объединениям</strong><div class="stack-list compact-list">{#each joinSummaries as item}<div class="inline-card summary-row"><span><strong>{item.alias || '-'}</strong></span><span>Совпало: {item.matched_rows ?? 0}</span><span>Не совпало: {item.unmatched_rows ?? 0}</span><span>Новых полей: {item.added_fields_count ?? 0}</span></div>{/each}</div></div>{/if}{#if resultPreviewColumns.length}<div class="preview-columns">{#each resultPreviewColumns as column}<span>{column}</span>{/each}</div>{/if}{#if resultPreviewRows.length}<div class="preview-table-wrap"><table class="preview-table"><thead><tr>{#each resultPreviewColumns as column}<th>{column}</th>{/each}</tr></thead><tbody>{#each resultPreviewRows as row}<tr>{#each resultPreviewColumns as column}<td>{typeof row?.[column] === 'object' ? JSON.stringify(row?.[column]) : String(row?.[column] ?? '')}</td>{/each}</tr>{/each}</tbody></table></div>{:else if previewData && !previewError}<div class="empty-box">Preview не вернул строк. Конфигурация валидна, но результат пустой.</div>{:else}<div class="empty-box">Запусти preview, чтобы увидеть итоговый набор строк.</div>{/if}<div class="summary-grid"><div class="summary-box"><strong>Output params preview</strong><pre>{pretty(outputParamsPreview, '{}')}</pre></div><div class="summary-box"><strong>Техническая статистика</strong><pre>{pretty(previewData?.stats || {}, '{}')}</pre></div></div></div></section>
      </div>
    </section>

    <section class="shell-strip">
      <div class="shell-strip-copy">
        <h3>Исходящие параметры</h3>
        <p>Зона для будущего preview выходных параметров. Логика будет добавлена отдельно.</p>
      </div>
      <span class="shell-strip-chip">Шаг 1: shell only</span>
    </section>
  </div>
</div>

<style>
  .table-node-builder{display:flex;flex-direction:column;gap:16px}.table-node-builder-embedded{gap:12px}.table-node-shell{display:grid;grid-template-columns:minmax(180px,.8fr) minmax(0,2.4fr) minmax(180px,.8fr);gap:16px;align-items:start}.shell-strip{display:flex;flex-direction:column;align-items:flex-start;gap:8px;padding:12px;border:1px solid #e2e8f0;border-radius:12px;background:#fff;min-width:0}.shell-strip-copy{display:flex;flex-direction:column;gap:3px;min-width:0}.shell-strip-copy h3{margin:0;font-size:13px;color:#0f172a}.shell-strip-copy p{margin:0;font-size:11px;line-height:1.45;color:#64748b}.shell-strip-chip{display:inline-flex;align-items:center;justify-content:center;flex-shrink:0;border-radius:999px;padding:4px 10px;font-size:11px;line-height:1.2;color:#475569;background:#f8fafc;border:1px solid #dbe4f0}.shell-strip-note{font-size:11px;line-height:1.4;color:#64748b}.shell-param-list{display:flex;flex-direction:column;gap:8px;width:100%}.shell-param-item{display:flex;flex-direction:column;gap:6px;padding:8px 10px;border:1px solid #e2e8f0;border-radius:10px;background:#f8fafc;width:100%;box-sizing:border-box}.shell-param-main{display:flex;align-items:flex-start;justify-content:space-between;gap:8px}.shell-param-main strong{font-size:12px;color:#0f172a;min-width:0;word-break:break-word}.shell-param-toolbar{display:flex;align-items:center;justify-content:flex-end;gap:6px;flex-wrap:wrap}.shell-param-meta{font-size:11px;color:#64748b;word-break:break-word}.shell-bind-btn{padding:5px 8px;font-size:11px}.shell-section{display:flex;flex-direction:column;gap:10px;border:1px solid #e2e8f0;border-radius:16px;background:#f8fafc;padding:16px;min-width:0}.shell-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px}.shell-head h3{margin:0;font-size:15px;color:#0f172a}.shell-head p{margin:4px 0 0;color:#64748b;font-size:12px;line-height:1.5}.subsection-head.compact-head,.compact-head{margin-top:4px}.subsection-head h4,.subsection-head h5{margin:0;font-size:13px;color:#0f172a}.shell-body{display:flex;flex-direction:column;gap:16px}.builder-card{border:1px solid #dbe4f0;border-radius:16px;background:#fff;padding:16px;display:flex;flex-direction:column;gap:14px}.builder-card-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px}.builder-card-head h3{margin:0;font-size:16px;color:#0f172a}.builder-card-head p{margin:4px 0 0;color:#475569;font-size:12px;line-height:1.5}.subsection{border-top:1px solid #eef2f7;padding-top:12px;display:flex;flex-direction:column;gap:10px}.subsection-head{display:flex;align-items:center;justify-content:space-between;gap:12px}.form-grid{display:grid;gap:10px}.form-grid-2{grid-template-columns:repeat(2,minmax(0,1fr))}.form-grid-3{grid-template-columns:repeat(3,minmax(0,1fr))}.form-grid-4{grid-template-columns:repeat(4,minmax(0,1fr))}.span-2{grid-column:span 2}label{display:flex;flex-direction:column;gap:6px;font-size:12px;color:#334155}input,select,textarea{width:100%;border:1px solid #cbd5e1;border-radius:10px;padding:8px 10px;font-size:12px;color:#0f172a;background:#fff;box-sizing:border-box}textarea{resize:vertical;min-height:84px;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}.hint{color:#64748b;font-size:11px;line-height:1.45}.stack-list{display:flex;flex-direction:column;gap:10px}.compact-list{gap:8px}.item-card,.inline-card,.summary-box{border:1px solid #e2e8f0;border-radius:12px;background:#f8fafc;padding:12px}.item-card{display:flex;flex-direction:column;gap:10px}.item-card-head{display:flex;align-items:center;justify-content:space-between;gap:12px}.toolbar-inline{display:inline-flex;align-items:center;gap:8px;flex-wrap:wrap}.button-cell{display:flex;align-items:end}.mini-btn,.primary-btn,.danger-btn{border-radius:10px;padding:7px 10px;font-size:12px;cursor:pointer;border:1px solid #cbd5e1;background:#fff;color:#0f172a}.primary-btn{background:#0f172a;color:#fff;border-color:#0f172a}.danger-btn{color:#b91c1c;border-color:#fecaca;background:#fff}.mini-btn:disabled,.primary-btn:disabled,.danger-btn:disabled{opacity:.5;cursor:not-allowed}.chip-list{display:flex;flex-wrap:wrap;gap:8px}.chip{border:1px solid #cbd5e1;border-radius:999px;background:#fff;padding:4px 10px;font-size:11px;color:#334155;cursor:pointer}.chip.readonly,.readonly-chip-list .chip{cursor:default}.preview-metrics{display:flex;flex-wrap:wrap;gap:8px 14px;font-size:11px;color:#475569}.preview-columns{display:flex;flex-wrap:wrap;gap:8px}.preview-columns span{font-size:11px;border-radius:999px;background:#eff6ff;color:#1d4ed8;padding:4px 10px}.preview-table-wrap{overflow:auto;border:1px solid #e2e8f0;border-radius:12px;background:#fff}.preview-table{width:100%;border-collapse:collapse;font-size:11px}.preview-table th,.preview-table td{border-bottom:1px solid #eef2f7;padding:8px 10px;text-align:left;vertical-align:top;white-space:nowrap}.preview-table th{background:#f8fafc;color:#334155;position:sticky;top:0}.empty-box,.inline-error,.warning-box{border-radius:12px;padding:12px;font-size:12px}.empty-box{border:1px dashed #cbd5e1;color:#64748b;background:#fff}.inline-error{border:1px solid #fecaca;background:#fef2f2;color:#b91c1c}.warning-box{border:1px solid #fde68a;background:#fffbeb;color:#92400e}.warning-box ul{margin:8px 0 0 18px;padding:0}.summary-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}.summary-box strong{display:block;margin-bottom:8px;font-size:12px;color:#0f172a}.summary-box pre{margin:0;white-space:pre-wrap;word-break:break-word;font-size:11px;color:#334155;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}.inline-preview{font-size:11px;color:#475569}.inline-preview code{color:#0f172a}.inline-select{display:inline-flex;align-items:center;gap:8px}.inline-select select{width:auto;min-width:180px}.inline-check{display:inline-flex;align-items:center;gap:8px}.inline-check input{width:auto}.summary-row{display:flex;flex-wrap:wrap;gap:8px 16px}@media (max-width:1100px){.table-node-shell{grid-template-columns:1fr}.shell-strip{align-items:flex-start}.form-grid-4,.form-grid-3,.form-grid-2,.summary-grid{grid-template-columns:1fr}.span-2{grid-column:span 1}.shell-param-main{flex-direction:column;align-items:flex-start}.shell-param-toolbar{justify-content:flex-start}}
</style>
