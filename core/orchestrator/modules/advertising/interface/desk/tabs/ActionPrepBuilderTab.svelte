<script>
  import { createEventDispatcher } from 'svelte';
  import { descriptorFields, descriptorOutputKindLabel, descriptorSampleColumns, descriptorSampleRows } from '../data/nodeDescriptorFlow';

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
  const DEFAULT_SETTINGS = { sourceMode: 'node', sourceSchema: '', sourceTable: '', filterRulesJson: '[]', filterLogic: 'and', actionColumnsJson: '[]', batchSize: '500', previewLimit: '20', channel: '' };
  const FILTERS = [['=','Равно'],['!=','Не равно'],['>','Больше'],['>=','Больше или равно'],['<','Меньше'],['<=','Меньше или равно'],['contains','Содержит'],['not_contains','Не содержит'],['empty','Пусто'],['not_empty','Не пусто'],['between','Между'],['in_list','В списке']];
  const MODES = [['source_field','Из поля'],['constant','Константа'],['formula','Формула'],['string_template','Шаблон'],['if_else','Если / иначе'],['percent_change','Изменить на %'],['absolute_change','Изменить на число'],['replace_text','Заменить текст'],['override_value','Переопределить']];
  const TYPES = [['','Без приведения'],['text','Текст'],['integer','Целое'],['numeric','Число'],['boolean','Булево'],['json','JSON']];
  const str = (v) => String(v ?? '').trim();
  const h = () => (typeof headers === 'function' ? headers() : headers || {});
  const parseJson = (raw, fallback) => { if (raw && typeof raw === 'object') return raw; try { const txt = str(raw); return txt ? JSON.parse(txt) : fallback; } catch { return fallback; } };
  const pretty = (value, fallback='[]') => { try { return JSON.stringify(value, null, 2); } catch { return fallback; } };
  const rowsFromNodeIo = (value) => value && typeof value === 'object' && String(value.contract_version || '').trim() === 'node_io_v1' && Array.isArray(value.rows) ? value.rows.filter((row) => row && typeof row === 'object' && !Array.isArray(row)) : Array.isArray(value) ? value.filter((row) => row && typeof row === 'object' && !Array.isArray(row)) : [];
  const uniq = (items=[]) => [...new Set((Array.isArray(items) ? items : []).map((item) => str(item)).filter(Boolean))];
  const colsFromRows = (rows=[], fallback=[]) => { const found = uniq((Array.isArray(rows) ? rows : []).flatMap((row) => row && typeof row === 'object' && !Array.isArray(row) ? Object.keys(row) : [])); return found.length ? found : uniq(fallback); };
  const fmt = (v) => v === null || v === undefined || v === '' ? '—' : typeof v === 'object' ? JSON.stringify(v) : String(v);
  const fieldName = (field) => str(field?.alias || field?.name || field?.path || '');
  const fieldMeta = (field) => [field?.type, field?.path].filter(Boolean).join(' · ');
  const kindRu = (kind) => ({ 'row set': 'Набор строк', 'tabular contract': 'Табличный контракт', 'structured object': 'Структурированный объект', unknown: 'Не определён' }[str(kind).toLowerCase()] || kind || 'Не определён');
  const normalizeFilter = (item={}, index=0) => ({ id: str(item.id || `filter_${index + 1}`), field: str(item.field), operator: str(item.operator || '=') || '=', value: String(item.value ?? ''), secondValue: String(item.secondValue ?? item.second_value ?? ''), caseSensitive: Boolean(item.caseSensitive ?? item.case_sensitive) });
  const normalizeAction = (item={}, index=0) => ({ id: str(item.id || `action_${index + 1}`), name: str(item.name || item.outputName || item.output_name || `action_${index + 1}`), mode: str(item.mode || item.sourceMode || item.actionType || 'source_field').toLowerCase() || 'source_field', sourceField: str(item.sourceField || item.source_field), baseField: str(item.baseField || item.base_field), constantValue: item.constantValue ?? item.constant_value ?? '', expression: str(item.expression), template: String(item.template ?? ''), conditionExpression: str(item.conditionExpression || item.condition_expression), trueValue: item.trueValue ?? item.true_value ?? '', falseValue: item.falseValue ?? item.false_value ?? '', percentValue: item.percentValue ?? item.percent_value ?? '', deltaValue: item.deltaValue ?? item.delta_value ?? '', replaceFrom: String(item.replaceFrom ?? item.replace_from ?? ''), replaceTo: String(item.replaceTo ?? item.replace_to ?? ''), overrideValue: item.overrideValue ?? item.override_value ?? '', type: str(item.type) });
  function normalizeSettings(raw) { const src = raw && typeof raw === 'object' ? raw : {}; return { sourceMode: str(src.sourceMode || src.source_mode || 'node').toLowerCase() === 'table' ? 'table' : 'node', sourceSchema: str(src.sourceSchema || src.source_schema), sourceTable: str(src.sourceTable || src.source_table), filterRulesJson: pretty(src.filterRules ?? src.filterRulesJson ?? src.filter_rules ?? [], '[]'), filterLogic: str(src.filterLogic || src.filter_logic || 'and').toLowerCase() === 'or' ? 'or' : 'and', actionColumnsJson: pretty(src.actionColumns ?? src.actionColumnsJson ?? src.action_columns ?? [], '[]'), batchSize: str(src.batchSize || src.batch_size || '500') || '500', previewLimit: str(src.previewLimit || src.preview_limit || '20') || '20', channel: str(src.channel || '') }; }
  let settings = normalizeSettings(initialSettings);
  let initialSignature = JSON.stringify(settings);
  let previewLoading = false;
  let previewError = '';
  let previewRunUid = '';
  let draftPreviewStep = null;
  let columnsCache = {};
  let incomingDescriptors = [];
  let primaryIncomingDescriptor = null;
  let incomingFields = [];
  let incomingRows = [];
  let incomingCols = [];
  let outputFields = [];
  let filterRules = [];
  let actionColumns = [];
  let tableCols = [];
  let sourceFields = [];
  let sourceCols = [];
  let inputRows = [];
  let inputCols = [];
  let outputRows = [];
  let outputCols = [];
  let fieldOptions = [];
  $: { const next = normalizeSettings(initialSettings); const signature = JSON.stringify(next); if (signature !== initialSignature) { initialSignature = signature; settings = next; } }
  $: incomingDescriptors = Array.isArray(incomingDescriptor?.upstreamDescriptors) ? incomingDescriptor.upstreamDescriptors : [];
  $: primaryIncomingDescriptor = incomingDescriptors.length ? incomingDescriptors[0] : null;
  $: incomingFields = descriptorFields(primaryIncomingDescriptor);
  $: incomingRows = descriptorSampleRows(primaryIncomingDescriptor);
  $: incomingCols = descriptorSampleColumns(primaryIncomingDescriptor);
  $: outputFields = descriptorFields(outputDescriptor);
  $: filterRules = parseJson(settings.filterRulesJson, []).map((item, index) => normalizeFilter(item, index));
  $: actionColumns = parseJson(settings.actionColumnsJson, []).map((item, index) => normalizeAction(item, index));
  $: tableCols = columnsCache[`${settings.sourceSchema}.${settings.sourceTable}`] || [];
  $: sourceFields = settings.sourceMode === 'table' ? tableCols.map((item) => ({ name: item.name, alias: item.name, path: item.name, type: item.type || '' })) : incomingFields;
  $: sourceCols = settings.sourceMode === 'table' ? tableCols.map((item) => item.name) : uniq([...incomingFields.map((field) => fieldName(field)), ...incomingCols]);
  $: inputRows = rowsFromNodeIo(draftPreviewStep?.input_json);
  $: inputCols = colsFromRows(inputRows, sourceCols);
  $: outputRows = rowsFromNodeIo(draftPreviewStep?.output_json);
  $: outputCols = colsFromRows(outputRows, outputFields.map((field) => fieldName(field)));
  $: fieldOptions = uniq([...sourceCols, ...inputCols, ...actionColumns.map((item) => item.name)]);
  $: inputStatus = previewLoading ? 'Предпросмотр входа собирается...' : previewError ? 'Предпросмотр входа недоступен' : inputRows.length ? `Предпросмотр входа: ${inputRows.length} строк` : draftPreviewStep ? 'Предпросмотр входа не вернул строк' : 'Предпросмотр входа пока не запускался';
  $: outputStatus = previewLoading ? 'Предпросмотр результата собирается...' : previewError ? 'Предпросмотр результата недоступен' : outputRows.length ? `Предпросмотр результата: ${outputRows.length} строк` : draftPreviewStep ? outputCols.length ? 'Предпросмотр вернул структуру без строк' : 'Предпросмотр результата не вернул строк' : 'Предпросмотр результата пока не запускался';
  $: if (settings.sourceMode === 'table' && settings.sourceSchema && settings.sourceTable) void ensureColumns(settings.sourceSchema, settings.sourceTable);

  function patch(key, value) { settings = { ...settings, [key]: value }; dispatch('configChange', { settings }); }
  function patchJson(key, value, fallback='[]') { patch(key, pretty(value, fallback)); }
  const selectValue = (e) => e.currentTarget?.value ?? '';
  const inputValue = (e) => e.currentTarget?.value ?? '';
  const checked = (e) => Boolean(e.currentTarget?.checked);
  async function ensureColumns(schema, table) { const s = str(schema); const t = str(table); if (!s || !t || Array.isArray(columnsCache[`${s}.${t}`])) return; try { const payload = await apiJson(`${apiBase}/columns?schema=${encodeURIComponent(s)}&table=${encodeURIComponent(t)}`, { headers: h() }); columnsCache = { ...columnsCache, [`${s}.${t}`]: Array.isArray(payload?.columns) ? payload.columns.map((item) => ({ name: str(item?.name), type: str(item?.type).toLowerCase() || '' })).filter((item) => item.name) : [] }; } catch { columnsCache = { ...columnsCache, [`${s}.${t}`]: [] }; } }
  function addFilter() { patchJson('filterRulesJson', [...filterRules, normalizeFilter({}, filterRules.length)]); }
  function updateFilter(index, patchValue) { patchJson('filterRulesJson', filterRules.map((row, rowIndex) => rowIndex === index ? { ...row, ...patchValue } : row)); }
  function removeFilter(index) { patchJson('filterRulesJson', filterRules.filter((_row, rowIndex) => rowIndex !== index)); }
  function addAction() { patchJson('actionColumnsJson', [...actionColumns, normalizeAction({}, actionColumns.length)]); }
  function updateAction(index, patchValue) { patchJson('actionColumnsJson', actionColumns.map((row, rowIndex) => rowIndex === index ? { ...row, ...patchValue } : row)); }
  function removeAction(index) { patchJson('actionColumnsJson', actionColumns.filter((_row, rowIndex) => rowIndex !== index)); }
  async function previewNow() {
    previewLoading = true; previewError = '';
    try {
      const payload = await apiJson(`${apiBase}/process-runs/preview-action-prep-node`, { method: 'POST', headers: h(), body: JSON.stringify({ desk_id: workflowDeskId, target_node_id: workflowNodeId, graph_json: workflowGraph, action_prep_settings_override: { ...settings, filterRules, actionColumns } }) });
      previewRunUid = str(payload?.run_uid);
      draftPreviewStep = payload?.target_step || null;
    } catch (error) {
      previewError = String(error?.message || error || 'Не удалось получить preview подготовки действий');
      previewRunUid = '';
      draftPreviewStep = null;
    } finally { previewLoading = false; }
  }
</script>

<section class="panel" class:embedded={embeddedMode}>
  <div class="layout">
    <section class="card">
      <h3>Источник и входящие данные</h3>
      <div class="grid cols-3">
        <label><span>Источник</span><select value={settings.sourceMode} on:change={(e) => patch('sourceMode', selectValue(e))}><option value="node">Предыдущая нода</option><option value="table">Таблица</option></select></label>
        {#if settings.sourceMode === 'table'}
          <label><span>Схема</span><select value={settings.sourceSchema} on:change={(e) => patch('sourceSchema', selectValue(e))}><option value="">Выбери схему</option>{#each uniq(existingTables.map((item) => item.schema_name)) as schemaName}<option value={schemaName}>{schemaName}</option>{/each}</select></label>
          <label><span>Таблица</span><select value={settings.sourceTable} on:change={(e) => patch('sourceTable', selectValue(e))}><option value="">Выбери таблицу</option>{#each existingTables.filter((item) => !settings.sourceSchema || item.schema_name === settings.sourceSchema) as item}<option value={item.table_name}>{item.table_name}</option>{/each}</select></label>
        {/if}
      </div>
      <div class="meta"><span>{sourceFields.length} полей</span><span>Формат: {kindRu(descriptorOutputKindLabel(primaryIncomingDescriptor?.outputKind || 'unknown'))}</span>{#if lastRuntimeStep?.run_uid}<span>Последний run: {lastRuntimeStep.run_uid}</span>{/if}</div>
      {#if sourceFields.length}<div class="chips">{#each sourceFields as field}<span class="chip" title={fieldMeta(field)}><strong>{fieldName(field)}</strong>{#if fieldMeta(field)}<small>{fieldMeta(field)}</small>{/if}</span>{/each}</div>{:else}<div class="empty">Входной контракт пока не определён.</div>{/if}
      <div class="preview-head"><strong>Предпросмотр входа</strong><span class="hint">{inputStatus}</span></div>
      {#if inputRows.length && inputCols.length}<div class="table-wrap"><table class="preview-table"><thead><tr>{#each inputCols as col}<th>{col}</th>{/each}</tr></thead><tbody>{#each inputRows.slice(0, Number(settings.previewLimit || 20)) as row}<tr>{#each inputCols as col}<td>{fmt(row?.[col])}</td>{/each}</tr>{/each}</tbody></table></div>{:else}<div class="empty">{previewError || (settings.sourceMode === 'table' ? 'Выбери таблицу и запусти предпросмотр.' : 'Запусти предпросмотр, чтобы увидеть входной поток.')}</div>{/if}
    </section>

    <section class="card main">
      <h3>Фильтрация и подготовка действий</h3>
      <div class="section-head"><h4>Фильтрация</h4><button type="button" class="mini" on:click={addFilter}>Добавить фильтр</button></div>
      <div class="grid cols-3">
        <label><span>Логика правил</span><select value={settings.filterLogic} on:change={(e) => patch('filterLogic', selectValue(e))}><option value="and">AND</option><option value="or">OR</option></select></label>
        <label><span>Batch size</span><input value={settings.batchSize} on:input={(e) => patch('batchSize', inputValue(e))} /></label>
        <label><span>Лимит preview</span><input value={settings.previewLimit} on:input={(e) => patch('previewLimit', inputValue(e))} /></label>
      </div>
      {#if filterRules.length}
        {#each filterRules as rule, index}
          <div class="rule">
            <select value={rule.field} on:change={(e) => updateFilter(index, { field: selectValue(e) })}><option value="">Поле</option>{#each fieldOptions as field}<option value={field}>{field}</option>{/each}</select>
            <select value={rule.operator} on:change={(e) => updateFilter(index, { operator: selectValue(e) })}>{#each FILTERS as [value,label]}<option value={value}>{label}</option>{/each}</select>
            <input value={rule.value} on:input={(e) => updateFilter(index, { value: inputValue(e) })} placeholder="Значение" />
            {#if rule.operator === 'between'}<input value={rule.secondValue} on:input={(e) => updateFilter(index, { secondValue: inputValue(e) })} placeholder="И второе значение" />{/if}
            <label class="check"><input type="checkbox" checked={rule.caseSensitive} on:change={(e) => updateFilter(index, { caseSensitive: checked(e) })} /> Регистр</label>
            <button type="button" class="danger icon" on:click={() => removeFilter(index)}>x</button>
          </div>
        {/each}
      {:else}<div class="empty">Без фильтров будут использованы все строки.</div>{/if}

      <div class="section-head"><h4>Action columns</h4><button type="button" class="mini" on:click={addAction}>Добавить action-колонку</button></div>
      {#if actionColumns.length}
        {#each actionColumns as column, index}
          <div class="action">
            <div class="section-head"><strong>{column.name || `Action ${index + 1}`}</strong><button type="button" class="danger icon" on:click={() => removeAction(index)}>x</button></div>
            <div class="grid cols-3">
              <label><span>Колонка результата</span><input value={column.name} on:input={(e) => updateAction(index, { name: inputValue(e) })} placeholder="new_bid / action_type" /></label>
              <label><span>Режим</span><select value={column.mode} on:change={(e) => updateAction(index, { mode: selectValue(e) })}>{#each MODES as [value,label]}<option value={value}>{label}</option>{/each}</select></label>
              <label><span>Тип</span><select value={column.type} on:change={(e) => updateAction(index, { type: selectValue(e) })}>{#each TYPES as [value,label]}<option value={value}>{label}</option>{/each}</select></label>
            </div>
            {#if column.mode === 'source_field'}
              <div class="grid cols-2"><label><span>Исходное поле</span><select value={column.sourceField} on:change={(e) => updateAction(index, { sourceField: selectValue(e) })}><option value="">Выбери поле</option>{#each fieldOptions as field}<option value={field}>{field}</option>{/each}</select></label></div>
            {:else if column.mode === 'constant'}
              <label><span>Константа</span><input value={column.constantValue} on:input={(e) => updateAction(index, { constantValue: inputValue(e) })} placeholder="update_bid" /></label>
            {:else if column.mode === 'formula'}
              <label><span>Формула</span><input value={column.expression} on:input={(e) => updateAction(index, { expression: inputValue(e) })} placeholder="current_bid * 1.05" /></label>
            {:else if column.mode === 'string_template'}
              <label><span>Шаблон строки</span><input value={column.template} on:input={(e) => updateAction(index, { template: inputValue(e) })} placeholder={'low_roas_{campaign_id}'} /></label>
            {:else if column.mode === 'if_else'}
              <div class="grid cols-3"><label><span>Условие</span><input value={column.conditionExpression} on:input={(e) => updateAction(index, { conditionExpression: inputValue(e) })} placeholder="roas < 1.2" /></label><label><span>Если true</span><input value={column.trueValue} on:input={(e) => updateAction(index, { trueValue: inputValue(e) })} /></label><label><span>Если false</span><input value={column.falseValue} on:input={(e) => updateAction(index, { falseValue: inputValue(e) })} /></label></div>
            {:else if column.mode === 'percent_change'}
              <div class="grid cols-2"><label><span>Базовое поле</span><select value={column.baseField} on:change={(e) => updateAction(index, { baseField: selectValue(e) })}><option value="">Выбери поле</option>{#each fieldOptions as field}<option value={field}>{field}</option>{/each}</select></label><label><span>Изменение, %</span><input value={column.percentValue} on:input={(e) => updateAction(index, { percentValue: inputValue(e) })} placeholder="5 / -10" /></label></div>
            {:else if column.mode === 'absolute_change'}
              <div class="grid cols-2"><label><span>Базовое поле</span><select value={column.baseField} on:change={(e) => updateAction(index, { baseField: selectValue(e) })}><option value="">Выбери поле</option>{#each fieldOptions as field}<option value={field}>{field}</option>{/each}</select></label><label><span>Дельта</span><input value={column.deltaValue} on:input={(e) => updateAction(index, { deltaValue: inputValue(e) })} placeholder="+5 / -3" /></label></div>
            {:else if column.mode === 'replace_text'}
              <div class="grid cols-3"><label><span>Базовое поле</span><select value={column.baseField} on:change={(e) => updateAction(index, { baseField: selectValue(e) })}><option value="">Выбери поле</option>{#each fieldOptions as field}<option value={field}>{field}</option>{/each}</select></label><label><span>Что заменить</span><input value={column.replaceFrom} on:input={(e) => updateAction(index, { replaceFrom: inputValue(e) })} /></label><label><span>На что заменить</span><input value={column.replaceTo} on:input={(e) => updateAction(index, { replaceTo: inputValue(e) })} /></label></div>
            {:else if column.mode === 'override_value'}
              <label><span>Значение override</span><input value={column.overrideValue} on:input={(e) => updateAction(index, { overrideValue: inputValue(e) })} /></label>
            {/if}
          </div>
        {/each}
      {:else}<div class="empty">Добавь action-колонки, чтобы сформировать action-ready поток.</div>{/if}
    </section>

    <section class="card">
      <h3>Выходные параметры</h3>
      <div class="meta"><span>{outputFields.length} полей</span><span>Формат: {kindRu(descriptorOutputKindLabel(outputDescriptor?.outputKind || 'unknown'))}</span></div>
      {#if outputFields.length}<div class="chips">{#each outputFields as field}<span class="chip" title={fieldMeta(field)}><strong>{fieldName(field)}</strong>{#if fieldMeta(field)}<small>{fieldMeta(field)}</small>{/if}</span>{/each}</div>{:else}<div class="empty">Выходной контракт появится после настройки action-колонок.</div>{/if}
      <div class="preview-head"><strong>Предпросмотр результата</strong><span class="hint">{outputStatus}</span></div>
      {#if outputRows.length && outputCols.length}<div class="table-wrap"><table class="preview-table"><thead><tr>{#each outputCols as col}<th>{col}</th>{/each}</tr></thead><tbody>{#each outputRows.slice(0, Number(settings.previewLimit || 20)) as row}<tr>{#each outputCols as col}<td>{fmt(row?.[col])}</td>{/each}</tr>{/each}</tbody></table></div>{:else}<div class="empty">{previewError || 'Запусти предпросмотр, чтобы увидеть action-ready таблицу.'}</div>{/if}
    </section>
  </div>

  <section class="card">
    <div class="section-head"><div><h3>Предпросмотр результата</h3><div class="hint">Один server preview-run до текущей ноды подготовки действий.</div></div><div class="actions">{#if previewRunUid}<span class="hint">Run: {previewRunUid}</span>{/if}<button type="button" class="primary" on:click={previewNow} disabled={previewLoading}>{previewLoading ? 'Обновляем...' : 'Обновить предпросмотр'}</button></div></div>
    <div class="meta"><span>{outputStatus}</span>{#if outputRows.length}<span>Строк: {outputRows.length}</span>{/if}{#if outputCols.length}<span>Колонок: {outputCols.length}</span>{/if}</div>
    {#if previewError}<div class="error">{previewError}</div>{/if}
    {#if outputRows.length && outputCols.length}<div class="table-wrap large"><table class="preview-table"><thead><tr>{#each outputCols as col}<th>{col}</th>{/each}</tr></thead><tbody>{#each outputRows as row}<tr>{#each outputCols as col}<td>{fmt(row?.[col])}</td>{/each}</tr>{/each}</tbody></table></div>{:else if draftPreviewStep}<div class="empty">Предпросмотр завершился без строк.</div>{:else}<div class="empty">Запусти preview, чтобы получить action-ready результат.</div>{/if}
  </section>
</section>

<style>
  .panel,.card,.main,.layout,.grid,.meta,.chips,.preview-head,.section-head,.actions,.rule,.check{display:grid;gap:12px}
  .panel{gap:16px}.layout{grid-template-columns:minmax(260px,.95fr) minmax(420px,1.35fr) minmax(260px,.95fr);align-items:start}.card{padding:16px;border:1px solid #dbe5f3;border-radius:18px;background:#fff;box-sizing:border-box}.main{min-width:0}.grid.cols-2{grid-template-columns:repeat(2,minmax(0,1fr))}.grid.cols-3{grid-template-columns:repeat(3,minmax(0,1fr))}.meta,.chips,.preview-head,.section-head,.actions,.rule{display:flex;flex-wrap:wrap;align-items:center}.meta,.hint{font-size:12px;color:#6a7692}.chips{gap:8px}.chip{display:inline-flex;flex-direction:column;gap:2px;max-width:100%;padding:8px 10px;border:1px solid #dbe5f3;border-radius:999px;background:#f8fbff;font-size:12px;color:#31425f}.chip strong,.chip small{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.section-head,.preview-head{justify-content:space-between}.actions{gap:8px}.empty,.error{padding:12px;border-radius:14px;font-size:12px}.empty{border:1px dashed #dbe5f3;color:#6a7692}.error{border:1px solid #f2b5bf;background:#fff5f6;color:#8a2335}label{display:grid;gap:6px;font-size:12px;color:#50607c;min-width:0}input,select,button{font:inherit;box-sizing:border-box}input,select{width:100%;min-width:0;padding:10px 12px;border:1px solid #c9d7ea;border-radius:12px;background:#fff}.table-wrap{overflow:auto;border:1px solid #dbe5f3;border-radius:14px}.table-wrap.large{max-height:420px}.preview-table{width:max-content;min-width:100%;border-collapse:collapse;font-size:12px}.preview-table th,.preview-table td{padding:8px 10px;border-bottom:1px solid #edf2fa;text-align:left;vertical-align:top;white-space:nowrap}.preview-table th{position:sticky;top:0;background:#f7faff}.mini,.primary,.danger{border:0;border-radius:12px;padding:10px 14px;cursor:pointer}.mini{background:#edf3ff;color:#264a8a}.primary{background:#111c36;color:#fff}.danger{background:#ffe8ea;color:#a92438}.icon{width:40px;min-width:40px;padding:0;display:inline-flex;align-items:center;justify-content:center}.action{display:grid;gap:12px;padding:12px;border:1px solid #dbe5f3;border-radius:16px;background:#fbfdff}.check{grid-auto-flow:column;justify-content:start}.embedded{padding:0}@media (max-width:1320px){.layout{grid-template-columns:1fr}}@media (max-width:860px){.grid.cols-2,.grid.cols-3{grid-template-columns:1fr}}
</style>
