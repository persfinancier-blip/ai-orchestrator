<script>
  import { createEventDispatcher } from 'svelte';
  import { descriptorFields, descriptorOutputKindLabel, descriptorSampleColumns, descriptorSampleRows } from '../data/nodeDescriptorFlow';
  import { normalizeJsonTextSetting, safeJsonArray } from '../data/workflowBuilderJsonState.js';

  export let apiBase = '';
  export let apiJson;
  export let headers;
  export let workflowDeskId = 0;
  export let workflowNodeId = '';
  export let workflowGraph = null;
  export let initialSettings = {};
  export let embeddedMode = false;
  export let incomingDescriptor = null;
  export let outputDescriptor = null;
  export let lastRuntimeStep = null;

  const dispatch = createEventDispatcher();
  const DEFAULT_SETTINGS = { endpointUrl: '', httpMethod: 'POST', authMode: 'manual', authJson: '{}', headersJson: '{"Content-Type":"application/json"}', queryJson: '{}', bodyJson: '{}', bodyItemsPath: 'items', bindingRulesJson: '[]', responseMappingsJson: '[]', requestMode: 'row_per_request', batchSize: '50', dryRun: 'true', maxRowsPerRun: '500', retryCount: '1', timeoutMs: '15000', stopPolicy: 'stop_on_error', previewLimit: '20', channel: '' };
  const METHODS = ['POST', 'PUT', 'PATCH', 'DELETE'];
  const REQUEST_MODES = [['row_per_request', '1 строка = 1 запрос'], ['batch_request', 'N строк = 1 batch']];
  const TARGETS = [['header', 'Header'], ['query', 'Query'], ['body', 'Body'], ['body_item', 'Body item']];
  const STOP_POLICIES = [['stop_on_error', 'Остановиться на первой ошибке'], ['continue', 'Продолжать при ошибках']];
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
  const normalizeBinding = (item={}, index=0) => ({ id: str(item.id || `binding_${index + 1}`), sourceField: str(item.sourceField || item.alias || item.source_field), target: str(item.target || 'body_item').toLowerCase() || 'body_item', path: str(item.path || item.targetPath || item.target_path) });
  const normalizeResponse = (item={}, index=0) => ({ id: str(item.id || `mapping_${index + 1}`), responsePath: str(item.responsePath || item.response_path || item.path), alias: str(item.alias || item.outputName || item.output_name || `response_${index + 1}`) });
  function normalizeSettings(raw) { const src = raw && typeof raw === 'object' ? raw : {}; return { endpointUrl: str(src.endpointUrl || src.endpoint_url || src.apiUrl || src.api_url), httpMethod: str(src.httpMethod || src.http_method || src.apiMethod || src.api_method || 'POST').toUpperCase() || 'POST', authMode: str(src.authMode || src.auth_mode || 'manual').toLowerCase() === 'bearer_token' ? 'bearer_token' : 'manual', authJson: normalizeJsonTextSetting(src.authJson ?? src.auth_json, '{}'), headersJson: normalizeJsonTextSetting(src.headersJson ?? src.headers_json ?? src.apiHeaders ?? src.api_headers ?? { 'Content-Type': 'application/json' }, '{}'), queryJson: normalizeJsonTextSetting(src.queryJson ?? src.query_json ?? src.apiQuery ?? src.api_query, '{}'), bodyJson: normalizeJsonTextSetting(src.bodyJson ?? src.body_json ?? src.apiBody ?? src.api_body, '{}'), bodyItemsPath: str(src.bodyItemsPath || src.body_items_path || 'items') || 'items', bindingRulesJson: normalizeJsonTextSetting(src.bindingRules ?? src.bindingRulesJson ?? src.binding_rules, '[]'), responseMappingsJson: normalizeJsonTextSetting(src.responseMappings ?? src.responseMappingsJson ?? src.response_mappings, '[]'), requestMode: str(src.requestMode || src.request_mode || 'row_per_request').toLowerCase() === 'batch_request' ? 'batch_request' : 'row_per_request', batchSize: str(src.batchSize || src.batch_size || '50') || '50', dryRun: String(src.dryRun ?? src.dry_run ?? 'true').trim().toLowerCase() === 'false' ? 'false' : 'true', maxRowsPerRun: str(src.maxRowsPerRun || src.max_rows_per_run || '500') || '500', retryCount: str(src.retryCount || src.retry_count || '1') || '1', timeoutMs: str(src.timeoutMs || src.timeout_ms || '15000') || '15000', stopPolicy: str(src.stopPolicy || src.stop_policy || 'stop_on_error').toLowerCase() === 'continue' ? 'continue' : 'stop_on_error', previewLimit: str(src.previewLimit || src.preview_limit || '20') || '20', channel: str(src.channel || '') }; }
  let settings = normalizeSettings(initialSettings);
  let initialSignature = JSON.stringify(settings);
  let previewLoading = false;
  let previewError = '';
  let previewRunUid = '';
  let previewStep = null;
  let requestPreview = [];
  let incomingDescriptors = [];
  let primaryIncomingDescriptor = null;
  let incomingFields = [];
  let incomingRows = [];
  let incomingCols = [];
  let outputFields = [];
  let bindingRules = [];
  let responseMappings = [];
  let inputContractCols = [];
  let previewInputRows = [];
  let previewInputCols = [];
  let previewOutputRows = [];
  let previewOutputCols = [];
  $: { const next = normalizeSettings(initialSettings); const signature = JSON.stringify(next); if (signature !== initialSignature) { initialSignature = signature; settings = next; } }
  $: incomingDescriptors = Array.isArray(incomingDescriptor?.upstreamDescriptors) ? incomingDescriptor.upstreamDescriptors : [];
  $: primaryIncomingDescriptor = incomingDescriptors.length ? incomingDescriptors[0] : null;
  $: incomingFields = descriptorFields(primaryIncomingDescriptor);
  $: incomingRows = descriptorSampleRows(primaryIncomingDescriptor);
  $: incomingCols = descriptorSampleColumns(primaryIncomingDescriptor);
  $: outputFields = descriptorFields(outputDescriptor);
  $: bindingRules = safeJsonArray(parseJson(settings.bindingRulesJson, [])).map((item, index) => normalizeBinding(item, index));
  $: responseMappings = safeJsonArray(parseJson(settings.responseMappingsJson, [])).map((item, index) => normalizeResponse(item, index));
  $: inputContractCols = uniq([...incomingFields.map((field) => fieldName(field)), ...incomingCols]);
  $: previewInputRows = rowsFromNodeIo(previewStep?.input_json);
  $: previewInputCols = colsFromRows(previewInputRows, inputContractCols);
  $: previewOutputRows = rowsFromNodeIo(previewStep?.output_json);
  $: previewOutputCols = colsFromRows(previewOutputRows, outputFields.map((field) => fieldName(field)));
  $: inputStatus = previewLoading ? 'Предпросмотр входа собирается...' : previewError ? 'Предпросмотр входа недоступен' : previewInputRows.length ? `Предпросмотр входа: ${previewInputRows.length} строк` : previewStep ? 'Предпросмотр входа не вернул строк' : 'Предпросмотр входа пока не запускался';
  $: outputStatus = previewLoading ? 'Dry-run preview собирается...' : previewError ? 'Предпросмотр результата недоступен' : previewOutputRows.length ? `Предпросмотр результата: ${previewOutputRows.length} строк` : previewStep ? previewOutputCols.length ? 'Предпросмотр вернул структуру без строк' : 'Предпросмотр результата не вернул строк' : 'Предпросмотр результата пока не запускался';

  function patch(key, value) { settings = { ...settings, [key]: value }; dispatch('configChange', { settings }); }
  function patchJson(key, value, fallback='[]') { patch(key, pretty(value, fallback)); }
  const selectValue = (e) => e.currentTarget?.value ?? '';
  const inputValue = (e) => e.currentTarget?.value ?? '';
  const checked = (e) => Boolean(e.currentTarget?.checked);
  function addBinding() { patchJson('bindingRulesJson', [...bindingRules, normalizeBinding({}, bindingRules.length)]); }
  function updateBinding(index, patchValue) { patchJson('bindingRulesJson', bindingRules.map((row, rowIndex) => rowIndex === index ? { ...row, ...patchValue } : row)); }
  function removeBinding(index) { patchJson('bindingRulesJson', bindingRules.filter((_row, rowIndex) => rowIndex !== index)); }
  function addResponseMapping() { patchJson('responseMappingsJson', [...responseMappings, normalizeResponse({}, responseMappings.length)]); }
  function updateResponseMapping(index, patchValue) { patchJson('responseMappingsJson', responseMappings.map((row, rowIndex) => rowIndex === index ? { ...row, ...patchValue } : row)); }
  function removeResponseMapping(index) { patchJson('responseMappingsJson', responseMappings.filter((_row, rowIndex) => rowIndex !== index)); }
  function addCommonBindings() { patchJson('bindingRulesJson', inputContractCols.map((name, index) => ({ id: `binding_${index + 1}`, sourceField: name, target: settings.requestMode === 'batch_request' ? 'body_item' : 'body', path: name }))); }
  async function previewNow() { previewLoading = true; previewError = ''; try { const payload = await apiJson(`${apiBase}/process-runs/preview-api-mutation-node`, { method: 'POST', headers: h(), body: JSON.stringify({ desk_id: workflowDeskId, target_node_id: workflowNodeId, graph_json: workflowGraph, api_mutation_settings_override: { ...settings, dryRun: 'true', bindingRules, responseMappings } }) }); previewRunUid = str(payload?.run_uid); previewStep = payload?.target_step || null; requestPreview = Array.isArray(payload?.target_step?.request_payload?.request_preview) ? payload.target_step.request_payload.request_preview : []; } catch (error) { previewError = String(error?.message || error || 'Не удалось получить dry-run preview API-изменения'); previewRunUid = ''; previewStep = null; requestPreview = []; } finally { previewLoading = false; } }
</script>

<section class="panel" class:embedded={embeddedMode}>
  <div class="layout">
    <section class="card">
      <h3>Входящие параметры</h3>
      <div class="meta"><span>{incomingFields.length} полей</span><span>Формат: {kindRu(descriptorOutputKindLabel(primaryIncomingDescriptor?.outputKind || 'unknown'))}</span>{#if lastRuntimeStep?.run_uid}<span>Последний run: {lastRuntimeStep.run_uid}</span>{/if}</div>
      {#if incomingFields.length}<div class="chips">{#each incomingFields as field}<span class="chip" title={fieldMeta(field)}><strong>{fieldName(field)}</strong>{#if fieldMeta(field)}<small>{fieldMeta(field)}</small>{/if}</span>{/each}</div>{:else}<div class="empty">Входной контракт пока не определён.</div>{/if}
      <div class="preview-head"><strong>Предпросмотр входа</strong><span class="hint">{inputStatus}</span></div>
      {#if previewInputRows.length && previewInputCols.length}<div class="table-wrap"><table class="preview-table"><thead><tr>{#each previewInputCols as col}<th>{col}</th>{/each}</tr></thead><tbody>{#each previewInputRows.slice(0, Number(settings.previewLimit || 20)) as row}<tr>{#each previewInputCols as col}<td>{fmt(row?.[col])}</td>{/each}</tr>{/each}</tbody></table></div>{:else}<div class="empty">{previewError || 'Запусти dry-run preview, чтобы увидеть входные строки.'}</div>{/if}
    </section>

    <section class="card main">
      <h3>Настройка API и выполнение</h3>
      <div class="grid cols-3">
        <label><span>Endpoint</span><input value={settings.endpointUrl} on:input={(e) => patch('endpointUrl', inputValue(e))} placeholder="https://api.example.com/mutate" /></label>
        <label><span>Метод</span><select value={settings.httpMethod} on:change={(e) => patch('httpMethod', selectValue(e))}>{#each METHODS as method}<option value={method}>{method}</option>{/each}</select></label>
        <label><span>Auth mode</span><select value={settings.authMode} on:change={(e) => patch('authMode', selectValue(e))}><option value="manual">Manual</option><option value="bearer_token">Bearer token</option></select></label>
      </div>
      <div class="grid cols-3">
        <label><span>Request mode</span><select value={settings.requestMode} on:change={(e) => patch('requestMode', selectValue(e))}>{#each REQUEST_MODES as [value,label]}<option value={value}>{label}</option>{/each}</select></label>
        <label><span>Batch size</span><input value={settings.batchSize} on:input={(e) => patch('batchSize', inputValue(e))} /></label>
        <label><span>Лимит preview</span><input value={settings.previewLimit} on:input={(e) => patch('previewLimit', inputValue(e))} /></label>
      </div>
      <div class="grid cols-3">
        <label><span>Retry count</span><input value={settings.retryCount} on:input={(e) => patch('retryCount', inputValue(e))} /></label>
        <label><span>Timeout, ms</span><input value={settings.timeoutMs} on:input={(e) => patch('timeoutMs', inputValue(e))} /></label>
        <label><span>Max rows per run</span><input value={settings.maxRowsPerRun} on:input={(e) => patch('maxRowsPerRun', inputValue(e))} /></label>
      </div>
      <div class="grid cols-2">
        <label><span>Stop policy</span><select value={settings.stopPolicy} on:change={(e) => patch('stopPolicy', selectValue(e))}>{#each STOP_POLICIES as [value,label]}<option value={value}>{label}</option>{/each}</select></label>
        <label class="check"><input type="checkbox" checked={settings.dryRun !== 'false'} on:change={(e) => patch('dryRun', checked(e) ? 'true' : 'false')} /> Dry-run по умолчанию</label>
      </div>
      <label><span>Auth JSON</span><textarea rows="3" value={settings.authJson} on:input={(e) => patch('authJson', inputValue(e))}></textarea></label>
      <div class="grid cols-2">
        <label><span>Headers JSON</span><textarea rows="4" value={settings.headersJson} on:input={(e) => patch('headersJson', inputValue(e))}></textarea></label>
        <label><span>Query JSON</span><textarea rows="4" value={settings.queryJson} on:input={(e) => patch('queryJson', inputValue(e))}></textarea></label>
      </div>
      <label><span>Body JSON</span><textarea rows="5" value={settings.bodyJson} on:input={(e) => patch('bodyJson', inputValue(e))}></textarea></label>
      <div class="grid cols-2">
        <label><span>Путь массива для batch body</span><input value={settings.bodyItemsPath} on:input={(e) => patch('bodyItemsPath', inputValue(e))} /></label>
        <label><span>Канал</span><input value={settings.channel} on:input={(e) => patch('channel', inputValue(e))} placeholder="Необязательно" /></label>
      </div>

      <div class="section-head"><h4>Payload mapping</h4><div class="actions"><button type="button" class="mini" on:click={addCommonBindings}>Автозаполнить</button><button type="button" class="mini" on:click={addBinding}>Добавить правило</button></div></div>
      {#if bindingRules.length}
        {#each bindingRules as rule, index}
          <div class="rule">
            <select value={rule.sourceField} on:change={(e) => updateBinding(index, { sourceField: selectValue(e) })}><option value="">Входное поле</option>{#each inputContractCols as field}<option value={field}>{field}</option>{/each}</select>
            <select value={rule.target} on:change={(e) => updateBinding(index, { target: selectValue(e) })}>{#each TARGETS as [value,label]}<option value={value}>{label}</option>{/each}</select>
            <input value={rule.path} on:input={(e) => updateBinding(index, { path: inputValue(e) })} placeholder="payload.path / header name" />
            <button type="button" class="danger icon" on:click={() => removeBinding(index)}>x</button>
          </div>
        {/each}
      {:else}<div class="empty">Добавь правила, чтобы собрать mutation payload из входных полей.</div>{/if}

      <div class="section-head"><h4>Response mapping</h4><button type="button" class="mini" on:click={addResponseMapping}>Добавить поле ответа</button></div>
      {#if responseMappings.length}
        {#each responseMappings as mapping, index}
          <div class="rule">
            <input value={mapping.responsePath} on:input={(e) => updateResponseMapping(index, { responsePath: inputValue(e) })} placeholder="response.data.id" />
            <input value={mapping.alias} on:input={(e) => updateResponseMapping(index, { alias: inputValue(e) })} placeholder="mutation_id" />
            <button type="button" class="danger icon" on:click={() => removeResponseMapping(index)}>x</button>
          </div>
        {/each}
      {:else}<div class="empty">При необходимости добавь поля ответа, которые нужно отдать downstream.</div>{/if}
    </section>

    <section class="card">
      <h3>Выходные параметры</h3>
      <div class="meta"><span>{outputFields.length} полей</span><span>Формат: {kindRu(descriptorOutputKindLabel(outputDescriptor?.outputKind || 'unknown'))}</span></div>
      {#if outputFields.length}<div class="chips">{#each outputFields as field}<span class="chip" title={fieldMeta(field)}><strong>{fieldName(field)}</strong>{#if fieldMeta(field)}<small>{fieldMeta(field)}</small>{/if}</span>{/each}</div>{:else}<div class="empty">Выходной контракт появится после настройки mapping и preview.</div>{/if}
      <div class="preview-head"><strong>Предпросмотр результата</strong><span class="hint">{outputStatus}</span></div>
      {#if previewOutputRows.length && previewOutputCols.length}<div class="table-wrap"><table class="preview-table"><thead><tr>{#each previewOutputCols as col}<th>{col}</th>{/each}</tr></thead><tbody>{#each previewOutputRows.slice(0, Number(settings.previewLimit || 20)) as row}<tr>{#each previewOutputCols as col}<td>{fmt(row?.[col])}</td>{/each}</tr>{/each}</tbody></table></div>{:else}<div class="empty">{previewError || 'Запусти dry-run preview, чтобы увидеть результат API-изменения.'}</div>{/if}
    </section>
  </div>

  <section class="card">
    <div class="section-head"><div><h3>Предпросмотр результата</h3><div class="hint">Dry-run preview выполняется через server path текущего workflow.</div></div><div class="actions">{#if previewRunUid}<span class="hint">Run: {previewRunUid}</span>{/if}<button type="button" class="primary" on:click={previewNow} disabled={previewLoading}>{previewLoading ? 'Обновляем...' : 'Обновить предпросмотр'}</button></div></div>
    <div class="meta"><span>{outputStatus}</span>{#if previewOutputRows.length}<span>Строк: {previewOutputRows.length}</span>{/if}{#if previewOutputCols.length}<span>Колонок: {previewOutputCols.length}</span>{/if}</div>
    {#if previewError}<div class="error">{previewError}</div>{/if}
    {#if requestPreview.length}
      <details class="details"><summary>Preview запросов</summary><div class="table-wrap"><table class="preview-table"><thead><tr><th>#</th><th>URL</th><th>Method</th><th>Rows</th><th>Body</th></tr></thead><tbody>{#each requestPreview as request}<tr><td>{fmt(request.request_index)}</td><td>{fmt(request.url)}</td><td>{fmt(request.method)}</td><td>{fmt(request.source_rows)}</td><td>{fmt(request.body)}</td></tr>{/each}</tbody></table></div></details>
    {/if}
    {#if previewOutputRows.length && previewOutputCols.length}<div class="table-wrap large"><table class="preview-table"><thead><tr>{#each previewOutputCols as col}<th>{col}</th>{/each}</tr></thead><tbody>{#each previewOutputRows as row}<tr>{#each previewOutputCols as col}<td>{fmt(row?.[col])}</td>{/each}</tr>{/each}</tbody></table></div>{:else if previewStep}<div class="empty">Предпросмотр завершился без строк.</div>{:else}<div class="empty">Запусти dry-run preview, чтобы увидеть результат и summary по success/fail.</div>{/if}
  </section>
</section>

<style>
  .panel,.card,.main,.layout,.grid,.meta,.chips,.preview-head,.section-head,.actions,.rule,.check,.details{display:grid;gap:12px}
  .panel{gap:16px}.layout{grid-template-columns:minmax(260px,.95fr) minmax(420px,1.35fr) minmax(260px,.95fr);align-items:start}.card{padding:16px;border:1px solid #dbe5f3;border-radius:18px;background:#fff;box-sizing:border-box}.main{min-width:0}.grid.cols-2{grid-template-columns:repeat(2,minmax(0,1fr))}.grid.cols-3{grid-template-columns:repeat(3,minmax(0,1fr))}.meta,.chips,.preview-head,.section-head,.actions,.rule{display:flex;flex-wrap:wrap;align-items:center}.meta,.hint{font-size:12px;color:#6a7692}.chips{gap:8px}.chip{display:inline-flex;flex-direction:column;gap:2px;max-width:100%;padding:8px 10px;border:1px solid #dbe5f3;border-radius:999px;background:#f8fbff;font-size:12px;color:#31425f}.chip strong,.chip small{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.section-head,.preview-head{justify-content:space-between}.actions{gap:8px}.empty,.error{padding:12px;border-radius:14px;font-size:12px}.empty{border:1px dashed #dbe5f3;color:#6a7692}.error{border:1px solid #f2b5bf;background:#fff5f6;color:#8a2335}label{display:grid;gap:6px;font-size:12px;color:#50607c;min-width:0}input,select,textarea,button{font:inherit;box-sizing:border-box}input,select,textarea{width:100%;min-width:0;padding:10px 12px;border:1px solid #c9d7ea;border-radius:12px;background:#fff}textarea{resize:vertical}.table-wrap{overflow:auto;border:1px solid #dbe5f3;border-radius:14px}.table-wrap.large{max-height:420px}.preview-table{width:max-content;min-width:100%;border-collapse:collapse;font-size:12px}.preview-table th,.preview-table td{padding:8px 10px;border-bottom:1px solid #edf2fa;text-align:left;vertical-align:top;white-space:nowrap}.preview-table th{position:sticky;top:0;background:#f7faff}.mini,.primary,.danger{border:0;border-radius:12px;padding:10px 14px;cursor:pointer}.mini{background:#edf3ff;color:#264a8a}.primary{background:#111c36;color:#fff}.danger{background:#ffe8ea;color:#a92438}.icon{width:40px;min-width:40px;padding:0;display:inline-flex;align-items:center;justify-content:center}.embedded{padding:0}summary{cursor:pointer;color:#2a3c5c}@media (max-width:1320px){.layout{grid-template-columns:1fr}}@media (max-width:860px){.grid.cols-2,.grid.cols-3{grid-template-columns:1fr}}
</style>
