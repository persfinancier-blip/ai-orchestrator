<script lang="ts">
  import { createEventDispatcher } from 'svelte';

  export let result: any = null;
  export let loading = false;
  export let error = '';
  export let applyMessage = '';

  const dispatch = createEventDispatcher<{ apply: void }>();

  $: detected = Array.isArray(result?.detected_fields) ? result.detected_fields : [];
  $: changes = Array.isArray(result?.recommended_changes) ? result.recommended_changes : [];
  $: unresolved = Array.isArray(result?.unresolved_items) ? result.unresolved_items : [];
  $: warnings = Array.isArray(result?.warnings) ? result.warnings : [];
  $: hasResult = Boolean(result && (detected.length || changes.length || unresolved.length || warnings.length || result.summary));
  $: canApply = Boolean(result?.apply_patch && Object.keys(result.apply_patch || {}).length && !loading);

  function formatValue(value: any) {
    if (value === undefined || value === null || value === '') return '-';
    if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') return String(value);
    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return String(value);
    }
  }

  function confidenceLabel(value: any) {
    const raw = String(value || '').trim().toLowerCase();
    if (raw === 'high') return 'высокая';
    if (raw === 'low') return 'низкая';
    return 'средняя';
  }
</script>

{#if loading || error || hasResult || applyMessage}
  <div class="api-example-result">
    {#if loading}
      <div class="api-example-status">Разбор примера API...</div>
    {/if}

    {#if error}
      <div class="api-example-error">{error}</div>
    {/if}

    {#if hasResult}
      <div class="api-example-section">
        <div class="api-example-title">Инструкция по настройке</div>
        <p>{result.summary || 'Проверь распознанные поля и примени рекомендации, если они подходят.'}</p>
      </div>

      {#if detected.length}
        <div class="api-example-section">
          <div class="api-example-title">Что распознано</div>
          <div class="api-example-list">
            {#each detected as item, idx (`${item?.key || 'detected'}:${idx}`)}
              <div class="api-example-row">
                <span>{item?.label || item?.key || 'Поле'}</span>
                <code>{formatValue(item?.value)}</code>
                <small>{confidenceLabel(item?.confidence)}</small>
              </div>
            {/each}
          </div>
        </div>
      {/if}

      {#if changes.length}
        <div class="api-example-section">
          <div class="api-example-title">Что будет изменено</div>
          <div class="api-example-list">
            {#each changes as item, idx (`${item?.field || 'change'}:${idx}`)}
              <div class="api-example-row">
                <span>{item?.label || item?.field || 'Поле'}</span>
                <code>{formatValue(item?.value)}</code>
                <small>{item?.action === 'merge' ? 'merge' : 'safe overwrite'}</small>
              </div>
            {/each}
          </div>
        </div>
      {/if}

      {#if warnings.length}
        <div class="api-example-section">
          <div class="api-example-title">Warnings</div>
          <ul>
            {#each warnings as item}
              <li>{item}</li>
            {/each}
          </ul>
        </div>
      {/if}

      {#if unresolved.length}
        <div class="api-example-section">
          <div class="api-example-title">Требует ручной проверки</div>
          <ul>
            {#each unresolved as item}
              <li>{item}</li>
            {/each}
          </ul>
        </div>
      {/if}

      <div class="api-example-actions">
        <button type="button" class="view-toggle template-action-btn view-toggle-primary" on:click={() => dispatch('apply')} disabled={!canApply}>
          Применить рекомендованные настройки
        </button>
      </div>
    {/if}

    {#if applyMessage}
      <div class="api-example-apply-message">{applyMessage}</div>
    {/if}
  </div>
{/if}

<style>
  .api-example-result {
    margin-top: 10px;
    border: 1px solid #dbe3ef;
    border-radius: 10px;
    padding: 10px;
    background: #f8fafc;
    color: #1f2937;
    display: grid;
    gap: 10px;
  }

  .api-example-status,
  .api-example-apply-message {
    font-size: 12px;
    color: #475569;
  }

  .api-example-error {
    font-size: 12px;
    color: #b91c1c;
    background: #fff1f2;
    border: 1px solid #fecdd3;
    border-radius: 8px;
    padding: 8px;
  }

  .api-example-section {
    display: grid;
    gap: 6px;
  }

  .api-example-title {
    font-size: 12px;
    font-weight: 700;
    color: #334155;
  }

  .api-example-section p,
  .api-example-section ul {
    margin: 0;
    font-size: 12px;
    line-height: 1.45;
  }

  .api-example-section ul {
    padding-left: 18px;
  }

  .api-example-list {
    display: grid;
    gap: 6px;
  }

  .api-example-row {
    display: grid;
    grid-template-columns: minmax(90px, 0.8fr) minmax(0, 1.6fr) minmax(70px, auto);
    gap: 8px;
    align-items: start;
    font-size: 12px;
  }

  .api-example-row code {
    white-space: pre-wrap;
    overflow-wrap: anywhere;
    background: #fff;
    border: 1px solid #e2e8f0;
    border-radius: 6px;
    padding: 4px 6px;
    color: #0f172a;
  }

  .api-example-row small {
    color: #64748b;
  }

  .api-example-actions {
    display: flex;
    justify-content: flex-start;
    flex-wrap: wrap;
    gap: 8px;
  }

  .api-example-actions button {
    border: 1px solid #2563eb;
    background: #2563eb;
    color: #fff;
    border-radius: 8px;
    padding: 7px 10px;
    font-size: 12px;
    cursor: pointer;
  }

  .api-example-actions button:disabled {
    opacity: 0.55;
    cursor: not-allowed;
  }

  @media (max-width: 720px) {
    .api-example-row {
      grid-template-columns: 1fr;
    }
  }
</style>
