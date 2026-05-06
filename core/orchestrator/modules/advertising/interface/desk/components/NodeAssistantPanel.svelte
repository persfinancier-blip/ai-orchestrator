<script lang="ts">
  import { createEventDispatcher } from 'svelte';

  export let apiBase = '';
  export let headers: Record<string, string> = {};
  export let nodeContext: any = {};
  export let currentValues: Record<string, any> = {};
  export let compact = false;

  const dispatch = createEventDispatcher<{ apply: any }>();

  let userText = '';
  let loading = false;
  let error = '';
  let result: any = null;
  let applyMessage = '';
  let clarificationAnswers: Record<string, string> = {};

  $: schema = result?.node_context?.schema || nodeContext?.editor_schema_json || nodeContext?.editor_schema || null;
  $: groups = Array.isArray(schema?.groups) ? schema.groups : [];
  $: recommendations = Array.isArray(result?.recommendations) ? result.recommendations : [];
  $: questions = Array.isArray(result?.clarification_questions) ? result.clarification_questions : [];
  $: warnings = Array.isArray(result?.warnings) ? result.warnings : [];
  $: unresolved = Array.isArray(result?.unresolved_items) ? result.unresolved_items : [];
  $: canApply = Boolean(result?.apply_patch?.fields?.length) && !loading;
  $: supportsAi = nodeContext?.editor_schema_json?.supports_ai !== false || nodeContext?.editor_schema?.supports_ai !== false;

  function valueText(value: any) {
    if (value === undefined || value === null || value === '') return '-';
    if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') return String(value);
    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return String(value);
    }
  }

  function groupLabel(groupKey: string) {
    return groups.find((group: any) => String(group?.key || '') === String(groupKey || ''))?.label || groupKey || 'Поля';
  }

  function groupedRecommendations() {
    const map = new Map<string, any[]>();
    for (const rec of recommendations) {
      const key = String(rec?.group || 'main');
      map.set(key, [...(map.get(key) || []), rec]);
    }
    return [...map.entries()].map(([key, rows]) => ({ key, label: groupLabel(key), rows }));
  }

  async function runRecommend(extraAnswers: Record<string, string> = {}) {
    const text = String(userText || '').trim();
    const answers = { ...clarificationAnswers, ...extraAnswers };
    if (!text && !Object.keys(answers).length) {
      error = 'Опишите задачу ноды или вставьте ссылку на документацию.';
      return;
    }
    loading = true;
    error = '';
    applyMessage = '';
    try {
      const response = await fetch(`${apiBase}/node-assistant/recommend`, {
        method: 'POST',
        headers: { Accept: 'application/json', 'Content-Type': 'application/json', ...headers },
        body: JSON.stringify({
          node_type_code: nodeContext?.node_type_code || '',
          editor_type_code: nodeContext?.editor_type_code || '',
          node_kind: nodeContext?.node_kind || '',
          user_text: text,
          current_values: currentValues || {},
          clarification_answers: answers
        })
      });
      let payload: any = {};
      try {
        payload = await response.json();
      } catch {
        payload = {};
      }
      if (!response.ok) throw new Error(String(payload?.details || payload?.error || `HTTP ${response.status}`));
      result = payload?.node_assistant || payload?.recommendation || payload || null;
      clarificationAnswers = answers;
    } catch (e: any) {
      error = String(e?.message || e || 'Не удалось получить рекомендации');
    } finally {
      loading = false;
    }
  }

  function clearInput() {
    userText = '';
    error = '';
  }

  function applyResult() {
    if (!canApply) return;
    dispatch('apply', result);
    applyMessage = 'Рекомендации подставлены в форму ноды. Проверьте результат и сохраните рабочий стол вручную.';
  }

  function answerValue(key: string) {
    return clarificationAnswers[key] || '';
  }

  function setAnswer(key: string, value: string) {
    clarificationAnswers = { ...clarificationAnswers, [key]: value };
  }

  function inputString(event: Event) {
    return (event.currentTarget as HTMLInputElement | null)?.value || '';
  }

  async function submitClarifications() {
    await runRecommend(clarificationAnswers);
  }
</script>

{#if supportsAi}
  <section class:node-assistant-panel={true} class:node-assistant-panel-compact={compact}>
    <div class="node-assistant-head">
      <div>
        <div class="node-assistant-title">AI-помощник настройки ноды</div>
        <div class="node-assistant-subtitle">Читает задачу, пример запроса или manual и предлагает draft-настройки по schema этой ноды.</div>
      </div>
    </div>

    <textarea
      rows="4"
      bind:value={userText}
      placeholder="Опишите задачу ноды, вставьте пример запроса или ссылку на документацию"
    ></textarea>

    <div class="node-assistant-actions">
      <button type="button" class="mini primary" on:click={() => runRecommend()} disabled={loading}>
        {loading ? 'Разбираю...' : 'Разобрать'}
      </button>
      <button type="button" class="mini" on:click={clearInput} disabled={loading}>Очистить</button>
    </div>

    {#if error}
      <div class="node-assistant-error">{error}</div>
    {/if}

    {#if result}
      <div class="node-assistant-summary">
        <strong>{result.summary}</strong>
        <span>Применение не сохраняет Desk и не пишет в source of truth. Оно только подставляет значения в текущую форму.</span>
      </div>

      {#if result?.intent}
        <div class="node-assistant-section">
          <div class="node-assistant-section-title">Что распознано</div>
          <div class="node-assistant-chips">
            <span>intent: {result.intent.action || '-'}</span>
            <span>source: {result.intent.source || '-'}</span>
            <span>operation: {result.intent.operation || '-'}</span>
          </div>
        </div>
      {/if}

      {#if groupedRecommendations().length}
        <div class="node-assistant-section">
          <div class="node-assistant-section-title">Рекомендации по полям</div>
          {#each groupedRecommendations() as group (group.key)}
            <div class="node-assistant-group">
              <div class="node-assistant-group-title">{group.label}</div>
              {#each group.rows as rec (`${rec.field_key}:${rec.source_type}`)}
                <div class:node-assistant-rec={true} class:node-assistant-rec-blocked={!rec.apply_allowed}>
                  <span>{rec.label || rec.field_key}</span>
                  <code>{valueText(rec.suggested_value)}</code>
                  <small>{rec.confidence} · {rec.apply_allowed ? 'можно применить' : rec.warning || 'ручная проверка'}</small>
                </div>
              {/each}
            </div>
          {/each}
        </div>
      {/if}

      {#if questions.length}
        <div class="node-assistant-section">
          <div class="node-assistant-section-title">Нужно уточнить</div>
          {#each questions as question (question.id)}
            <label class="node-assistant-question">
              <span>{question.prompt}</span>
              <input
                value={answerValue(question.answer_key)}
                placeholder={(question.examples || [])[0] || ''}
                on:input={(event) => setAnswer(question.answer_key, inputString(event))}
              />
            </label>
          {/each}
          <button type="button" class="mini" on:click={submitClarifications} disabled={loading}>Пересчитать рекомендации</button>
        </div>
      {/if}

      {#if warnings.length}
        <div class="node-assistant-section">
          <div class="node-assistant-section-title">Warnings</div>
          <ul>
            {#each warnings as item}
              <li>{item}</li>
            {/each}
          </ul>
        </div>
      {/if}

      {#if unresolved.length}
        <div class="node-assistant-section">
          <div class="node-assistant-section-title">Что не распознано</div>
          <ul>
            {#each unresolved as item}
              <li>{item}</li>
            {/each}
          </ul>
        </div>
      {/if}

      <div class="node-assistant-actions">
        <button type="button" class="mini primary" on:click={applyResult} disabled={!canApply}>
          Применить в форму
        </button>
      </div>
    {/if}

    {#if applyMessage}
      <div class="node-assistant-applied">{applyMessage}</div>
    {/if}
  </section>
{/if}

<style>
  .node-assistant-panel {
    display: grid;
    gap: 10px;
    padding: 10px;
    border-bottom: 1px solid #e2e8f0;
    background: #f8fafc;
  }

  .node-assistant-panel-compact {
    border: 1px solid #dbe4f0;
    border-radius: 8px;
    margin-bottom: 10px;
  }

  .node-assistant-head {
    display: flex;
    justify-content: space-between;
    gap: 10px;
    align-items: start;
  }

  .node-assistant-title {
    font-size: 13px;
    font-weight: 800;
    color: #0f172a;
  }

  .node-assistant-subtitle,
  .node-assistant-summary,
  .node-assistant-applied {
    font-size: 12px;
    line-height: 1.4;
    color: #475569;
  }

  .node-assistant-panel textarea,
  .node-assistant-panel input {
    border: 1px solid #dbe4f0;
    border-radius: 8px;
    padding: 7px 8px;
    font-family: inherit;
    font-size: 13px;
  }

  .node-assistant-actions,
  .node-assistant-chips {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    align-items: center;
  }

  .node-assistant-chips span {
    border: 1px solid #cbd5e1;
    background: #fff;
    border-radius: 999px;
    padding: 4px 8px;
    font-size: 11px;
    color: #334155;
  }

  .node-assistant-error {
    font-size: 12px;
    color: #b91c1c;
    background: #fff1f2;
    border: 1px solid #fecdd3;
    border-radius: 8px;
    padding: 8px;
  }

  .node-assistant-summary {
    display: grid;
    gap: 4px;
    border: 1px solid #bfdbfe;
    background: #eef6ff;
    border-radius: 8px;
    padding: 8px;
  }

  .node-assistant-section {
    display: grid;
    gap: 6px;
  }

  .node-assistant-section-title,
  .node-assistant-group-title {
    font-size: 12px;
    font-weight: 800;
    color: #334155;
  }

  .node-assistant-group {
    display: grid;
    gap: 5px;
  }

  .node-assistant-rec {
    display: grid;
    grid-template-columns: minmax(120px, 0.7fr) minmax(0, 1.4fr) minmax(110px, 0.8fr);
    gap: 8px;
    align-items: start;
    font-size: 12px;
  }

  .node-assistant-rec code {
    white-space: pre-wrap;
    overflow-wrap: anywhere;
    background: #fff;
    border: 1px solid #e2e8f0;
    border-radius: 6px;
    padding: 4px 6px;
    color: #0f172a;
  }

  .node-assistant-rec small {
    color: #64748b;
  }

  .node-assistant-rec-blocked {
    opacity: 0.75;
  }

  .node-assistant-question {
    display: grid;
    gap: 5px;
    font-size: 12px;
    color: #334155;
  }

  .node-assistant-section ul {
    margin: 0;
    padding-left: 18px;
    font-size: 12px;
    line-height: 1.45;
  }

  .node-assistant-applied {
    color: #166534;
    background: #ecfdf5;
    border: 1px solid #bbf7d0;
    border-radius: 8px;
    padding: 8px;
  }

  @media (max-width: 720px) {
    .node-assistant-rec {
      grid-template-columns: 1fr;
    }
  }
</style>
