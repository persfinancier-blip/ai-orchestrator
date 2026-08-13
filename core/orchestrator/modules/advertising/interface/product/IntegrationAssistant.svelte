<script lang="ts">
  import NodeAssistantPanel from '../desk/components/NodeAssistantPanel.svelte';
  import { productPost } from './productApi.js';
  import { integrationDraftFromAssistant } from './integrationDraft.js';

  const nodeContext = { node_type_code: 'api_request', editor_type_code: '', node_kind: 'tool' };
  let saving = false;
  let savedId = 0;
  let message = '';

  async function saveDraft(event: CustomEvent) {
    const draft = integrationDraftFromAssistant(event.detail || {});
    if (!draft.ok || !draft.payload) {
      message = draft.reason === 'missing_endpoint'
        ? 'Ассистенту не хватило подтверждённого endpoint. Уточните задачу или приложите документацию.'
        : 'Полученный endpoint нельзя сохранить без ручной проверки.';
      return;
    }

    saving = true;
    message = '';
    try {
      const response = await productPost('/api-configs/upsert', draft.payload);
      savedId = Number(response?.id || 0);
      message = savedId ? 'Черновик сохранён. Проверьте его в конструкторе перед использованием.' : 'Черновик сохранён.';
    } catch (error) {
      message = String(error?.message || error || 'Не удалось сохранить черновик');
    } finally {
      saving = false;
    }
  }
</script>

<section class="page">
  <header><small>Integration Assistant</small><h1>Опишите интеграцию обычным языком</h1><p>Ассистент читает задачу или документацию и предлагает только уверенные поля. Сохранённый результат остаётся неактивным черновиком.</p></header>
  <NodeAssistantPanel apiBase="/ai-orchestrator/api" headers={{}} {nodeContext} currentValues={{}} compact={true} on:apply={saveDraft} />
  {#if saving}<div class="message">Сохраняем черновик…</div>{/if}
  {#if message}<div class="message">{message}</div>{/if}
  {#if savedId}<a class="open" href={`#desk/data?pane=api&api_store_id=${savedId}`}>Открыть API-черновик →</a>{/if}
</section>

<style>
  .page { max-width: 900px; margin: 0 auto; padding: 34px 28px; color: #172033; } header { max-width: 720px; margin-bottom: 18px; } header small { color: #94a3b8; font-size: 9px; font-weight: 800; letter-spacing: .12em; text-transform: uppercase; } h1 { margin: 6px 0; font-size: 28px; letter-spacing: -.035em; } p { margin: 0; color: #64748b; font-size: 11px; line-height: 1.5; }.message { margin-top: 10px; padding: 10px 12px; border-radius: 9px; background: #f1f5f9; color: #475569; font-size: 10px; }.open { display: inline-flex; margin-top: 10px; padding: 9px 12px; border-radius: 9px; background: #172033; color: #fff; text-decoration: none; font-size: 10px; font-weight: 800; }
</style>
