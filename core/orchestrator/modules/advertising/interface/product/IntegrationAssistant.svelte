<script lang="ts">
  import NodeAssistantPanel from '../desk/components/NodeAssistantPanel.svelte';
  import { productGet, productPost } from './productApi.js';
  import { integrationDraftFromAssistant } from './integrationDraft.js';
  import { buildSimpleApiWorkflow } from './simpleWorkflowCore.js';

  const nodeContext = { node_type_code: 'api_request', editor_type_code: '', node_kind: 'tool' };
  let saving = false, building = false, savedId = 0, savedName = '', scenarioId = 0, message = '';
  let warnings: string[] = [];
  let dataSchema = 'showcase', dataTable = 'advertising', dataName = '', dataPlan: any = null, dataMessage = '', dataError = '', dataBusy = false;

  async function saveDraft(event: CustomEvent) {
    const draft = integrationDraftFromAssistant(event.detail || {});
    warnings = Array.isArray(draft.warnings) ? draft.warnings : [];
    scenarioId = 0;
    if (!draft.ok || !draft.payload) {
      savedId = 0;
      message = draft.reason === 'missing_endpoint' ? 'Ассистенту не хватило подтверждённого endpoint. Уточните задачу или приложите документацию.' : draft.reason === 'embedded_credentials' ? 'Credentials нельзя передавать внутри URL. Укажите чистый endpoint и настройте авторизацию отдельно.' : 'Полученный endpoint нельзя сохранить без ручной проверки.';
      return;
    }
    saving = true; message = '';
    try { const response = await productPost('/api-configs/upsert', draft.payload); savedId = Number(response?.id || 0); savedName = String(draft.payload.api_name || 'Сценарий'); message = savedId ? 'Источник подготовлен. Теперь можно собрать сценарий одной кнопкой.' : 'Черновик сохранён.'; }
    catch (error) { message = String(error?.message || error || 'Не удалось сохранить черновик'); }
    finally { saving = false; }
  }

  async function buildScenario() {
    if (!savedId) return;
    building = true; message = '';
    try {
      const context = await productGet('/context/container').catch(() => ({}));
      const graph = buildSimpleApiWorkflow({ name:savedName || `Сценарий ${savedId}`, apiStoreId:savedId, containerId:context?.container_id || null });
      const result = await productPost('/workflow-desks/upsert', { desk_name:savedName || `Сценарий ${savedId}`, desk_type:'data', config_json:graph, description:'Черновик простого сценария, созданный продуктовым ассистентом.', is_active:true, updated_by:'product_assistant' });
      scenarioId = Number(result?.id || 0);
      if (scenarioId && context?.container_id) await productPost('/product/container-workflows/link', { container_id:context.container_id, desk_id:scenarioId, role:'collection' });
      message = 'Сценарий создан как черновик. Старт выключен, публикации и запуска не было.';
    } catch (error) { message = String(error?.message || error || 'Не удалось создать сценарий'); }
    finally { building = false; }
  }

  async function inspectData() {
    dataBusy = true; dataError = ''; dataMessage = '';
    try { const result = await productPost('/product/normalization/inspect', { schema:dataSchema, table:dataTable }); dataPlan = result?.plan || null; if (dataPlan && !dataName) dataName = `${dataTable} · clean`; }
    catch (error) { dataError = String(error?.message || error); dataPlan = null; }
    finally { dataBusy = false; }
  }

  async function saveDataProduct() {
    if (!dataPlan) return;
    dataBusy = true; dataError = ''; dataMessage = '';
    try {
      const context = await productGet('/context/container');
      if (!context?.container_id) throw new Error('Сначала выберите контейнер на Обзоре.');
      await productPost('/product/data-products/upsert', { container_id:context.container_id, name:dataName || `${dataTable} · clean`, source_schema:dataSchema, source_table:dataTable, stage:'clean', semantic_schema_json:{ fields:dataPlan.fields, roles:dataPlan.roles }, normalization_json:{ mode:'guided_auto', fields:dataPlan.fields, warnings:dataPlan.warnings }, status:dataPlan.ready_for_analysis ? 'ready' : 'draft' });
      dataMessage = 'Data Product сохранён. Эти роли теперь можно использовать в аналитике и прогнозах.';
    } catch (error) { dataError = String(error?.message || error); }
    finally { dataBusy = false; }
  }
</script>

<section class="page">
  <header><small>Простой сценарий</small><h1>Опишите, что должно происходить</h1><p>Подключение источника и подготовка уже собранных данных — два простых пути. Технический граф и SQL остаются под капотом.</p></header>

  <section class="block">
    <div class="block-head"><b>Нужно получать данные</b><span>Опишите API или приложите документацию.</span></div>
    <NodeAssistantPanel apiBase="/ai-orchestrator/api" headers={{}} {nodeContext} currentValues={{}} compact={true} on:apply={saveDraft} />
    {#if saving}<div class="message">Подготавливаем источник…</div>{/if}
    {#if warnings.length}<div class="warning"><strong>Секреты не сохранены автоматически.</strong>{#each warnings as item}<span>{item}</span>{/each}</div>{/if}
    {#if message}<div class="message">{message}</div>{/if}
    {#if savedId && !scenarioId}<button class="primary" on:click={buildScenario} disabled={building}>{building ? 'Собираем…' : 'Создать сценарий'}</button>{/if}
    <div class="actions">{#if scenarioId}<a class="open" href={`#desk/data?desk_id=${scenarioId}`}>Проверить граф →</a>{/if}{#if savedId}<a href={`#desk/data?pane=api&api_store_id=${savedId}`}>API вручную</a>{/if}</div>
  </section>

  <section class="block data">
    <div class="block-head"><b>Данные уже есть</b><span>Система сама определит семантику таблицы.</span></div>
    <form on:submit|preventDefault={inspectData}><input bind:value={dataSchema} aria-label="Схема"/><input bind:value={dataTable} aria-label="Таблица"/><button class="primary" disabled={dataBusy}>{dataBusy ? 'Анализ…' : 'Определить структуру'}</button></form>
    {#if dataPlan}<div class="roles">{#each Object.entries(dataPlan.roles || {}) as [key,value]}<span><b>{value}</b>{key}</span>{/each}</div>{#if dataPlan.warnings?.length}<div class="warning">{#each dataPlan.warnings as warning}<span>{warning}</span>{/each}</div>{/if}<div class="save"><input bind:value={dataName} placeholder="Название Data Product"/><button class="primary" on:click={saveDataProduct} disabled={dataBusy}>Сохранить Data Product</button></div>{/if}
    {#if dataMessage}<div class="message">{dataMessage}</div>{/if}{#if dataError}<div class="error">{dataError}</div>{/if}
  </section>
</section>

<style>
.page{max-width:900px;margin:0 auto;padding:34px 28px;color:#172033}.page>header{max-width:720px;margin-bottom:18px}header small{color:#94a3b8;font-size:9px;font-weight:800;letter-spacing:.12em;text-transform:uppercase}h1{margin:6px 0;font-size:28px;letter-spacing:-.035em}p{margin:0;color:#64748b;font-size:11px;line-height:1.5}.block{margin-top:12px;padding:15px;background:#fff;border:1px solid #e4e9f1;border-radius:14px}.block-head{display:flex;justify-content:space-between;gap:12px;margin-bottom:10px}.block-head b{font-size:12px}.block-head span{color:#64748b;font-size:9px}.message,.warning,.error{margin-top:10px;padding:9px 11px;border-radius:9px;background:#f1f5f9;color:#475569;font-size:9px}.warning{display:flex;flex-direction:column;gap:3px;background:#fffbeb;color:#854d0e}.error{background:#fff1f2;color:#b42318}.primary,.open{display:inline-flex;margin-top:10px;padding:9px 12px;border:0;border-radius:9px;background:#172033;color:#fff;text-decoration:none;font-size:10px;font-weight:800;cursor:pointer}.actions{display:flex;gap:10px;align-items:center}.actions>a:not(.open){margin-top:10px;color:#64748b;font-size:10px;text-decoration:none}.data form,.save{display:flex;gap:6px}.data input{flex:1;min-width:0;border:1px solid #dfe5ed;border-radius:8px;padding:8px 9px;font-size:10px}.data form .primary,.save .primary{margin-top:0}.roles{display:flex;gap:6px;flex-wrap:wrap;margin-top:9px}.roles span{padding:7px 9px;border:1px solid #edf0f4;border-radius:8px;font-size:8px}.roles b{display:block;font-size:14px}@media(max-width:600px){.block-head,.data form,.save{flex-direction:column}.data form .primary,.save .primary{margin-top:0}}
</style>
