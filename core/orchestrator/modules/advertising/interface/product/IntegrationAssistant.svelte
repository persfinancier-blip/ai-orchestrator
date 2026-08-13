<script lang="ts">
  import NodeAssistantPanel from '../desk/components/NodeAssistantPanel.svelte';
  import { productGet, productPost } from './productApi.js';
  import { integrationDraftFromAssistant } from './integrationDraft.js';
  import { buildSimpleApiWorkflow } from './simpleWorkflowCore.js';

  const nodeContext = { node_type_code: 'api_request', editor_type_code: '', node_kind: 'tool' };
  let saving = false;
  let building = false;
  let savedId = 0;
  let savedName = '';
  let scenarioId = 0;
  let message = '';
  let warnings: string[] = [];

  async function saveDraft(event: CustomEvent) {
    const draft = integrationDraftFromAssistant(event.detail || {});
    warnings = Array.isArray(draft.warnings) ? draft.warnings : [];
    scenarioId = 0;
    if (!draft.ok || !draft.payload) {
      savedId = 0;
      message = draft.reason === 'missing_endpoint'
        ? 'Ассистенту не хватило подтверждённого endpoint. Уточните задачу или приложите документацию.'
        : draft.reason === 'embedded_credentials'
          ? 'Credentials нельзя передавать внутри URL. Укажите чистый endpoint и настройте авторизацию отдельно.'
          : 'Полученный endpoint нельзя сохранить без ручной проверки.';
      return;
    }
    saving = true; message = '';
    try {
      const response = await productPost('/api-configs/upsert', draft.payload);
      savedId = Number(response?.id || 0);
      savedName = String(draft.payload.api_name || 'Сценарий');
      message = savedId ? 'Источник подготовлен. Теперь можно собрать безопасный сценарий одной кнопкой.' : 'Черновик сохранён.';
    } catch (error) { message = String(error?.message || error || 'Не удалось сохранить черновик'); }
    finally { saving = false; }
  }

  async function buildScenario() {
    if (!savedId) return;
    building = true; message = '';
    try {
      const context = await productGet('/context/container').catch(() => ({}));
      const graph = buildSimpleApiWorkflow({ name:savedName || `Сценарий ${savedId}`, apiStoreId:savedId, containerId:context?.container_id || null });
      const result = await productPost('/workflow-desks/upsert', {
        desk_name: savedName || `Сценарий ${savedId}`,
        desk_type: 'data',
        config_json: graph,
        description: 'Черновик простого сценария, созданный продуктовым ассистентом.',
        is_active: true,
        updated_by: 'product_assistant'
      });
      scenarioId = Number(result?.id || 0);
      if (scenarioId && context?.container_id) await productPost('/product/container-workflows/link', { container_id:context.container_id, desk_id:scenarioId, role:'collection' });
      message = 'Сценарий создан как черновик. Старт выключен, публикации и запуска не было.';
    } catch (error) { message = String(error?.message || error || 'Не удалось создать сценарий'); }
    finally { building = false; }
  }
</script>

<section class="page">
  <header><small>Простой сценарий</small><h1>Опишите, что должно происходить</h1><p>Сначала формируется проверяемый источник. Затем система сама собирает технический граф. Публикация и запуск остаются отдельным действием.</p></header>
  <NodeAssistantPanel apiBase="/ai-orchestrator/api" headers={{}} {nodeContext} currentValues={{}} compact={true} on:apply={saveDraft} />
  {#if saving}<div class="message">Подготавливаем источник…</div>{/if}
  {#if warnings.length}<div class="warning"><strong>Секреты не сохранены автоматически.</strong>{#each warnings as item}<span>{item}</span>{/each}<span>Авторизация подставляется отдельно из подключений клиента.</span></div>{/if}
  {#if message}<div class="message">{message}</div>{/if}
  {#if savedId && !scenarioId}<button class="build" on:click={buildScenario} disabled={building}>{building ? 'Собираем…' : 'Создать сценарий из черновика'}</button>{/if}
  <div class="actions">
    {#if scenarioId}<a class="open" href={`#desk/data?desk_id=${scenarioId}`}>Проверить граф →</a>{/if}
    {#if savedId}<a href={`#desk/data?pane=api&api_store_id=${savedId}`}>API вручную</a>{/if}
  </div>
</section>

<style>
  .page{max-width:900px;margin:0 auto;padding:34px 28px;color:#172033}header{max-width:720px;margin-bottom:18px}header small{color:#94a3b8;font-size:9px;font-weight:800;letter-spacing:.12em;text-transform:uppercase}h1{margin:6px 0;font-size:28px;letter-spacing:-.035em}p{margin:0;color:#64748b;font-size:11px;line-height:1.5}.message,.warning{margin-top:10px;padding:10px 12px;border-radius:9px;background:#f1f5f9;color:#475569;font-size:10px}.warning{display:flex;flex-direction:column;gap:4px;background:#fffbeb;color:#854d0e}.build,.open{display:inline-flex;margin-top:10px;padding:9px 12px;border:0;border-radius:9px;background:#172033;color:#fff;text-decoration:none;font-size:10px;font-weight:800;cursor:pointer}.actions{display:flex;gap:10px;align-items:center}.actions>a:not(.open){margin-top:10px;color:#64748b;font-size:10px;text-decoration:none}
</style>
