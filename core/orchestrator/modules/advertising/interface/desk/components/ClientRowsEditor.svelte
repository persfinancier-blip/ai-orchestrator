<script lang="ts">
  import { createEventDispatcher } from 'svelte';

  export type OptionItem = { value: string; label: string };
  export type ClientFieldConfig = {
    key: string;
    label: string;
    type?: 'text' | 'textarea' | 'number' | 'date' | 'datetime' | 'checkbox' | 'select';
    placeholder?: string;
    options?: OptionItem[];
    optionsKey?: string;
    min?: string | number;
    step?: string | number;
    rows?: number;
  };

  export let title = '';
  export let rows: Array<Record<string, any>> = [];
  export let fields: ClientFieldConfig[] = [];
  export let optionSets: Record<string, OptionItem[]> = {};
  export let addLabel = 'Добавить запись';
  export let emptyText = 'Записей пока нет.';
  export let busyRowKey = '';

  const dispatch = createEventDispatcher<{
    add: undefined;
    change: { index: number; field: string; value: any };
    save: { index: number };
    remove: { index: number };
    duplicate: { index: number };
  }>();

  function keyForRow(row: Record<string, any>, index: number) {
    return String(row?.id || row?.__localId || `row_${index}`);
  }

  function optionsForField(field: ClientFieldConfig) {
    if (Array.isArray(field.options) && field.options.length) return field.options;
    if (field.optionsKey && Array.isArray(optionSets[field.optionsKey])) return optionSets[field.optionsKey];
    return [];
  }

  function onTextChange(index: number, field: string, value: string) {
    dispatch('change', { index, field, value });
  }

  function onBoolChange(index: number, field: string, checked: boolean) {
    dispatch('change', { index, field, value: checked });
  }

  function textValue(event: Event) {
    const target = event.currentTarget as HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement | null;
    return target ? target.value : '';
  }

  function boolValue(event: Event) {
    const target = event.currentTarget as HTMLInputElement | null;
    return Boolean(target?.checked);
  }
</script>

<section class="rows-editor">
  <div class="rows-head">
    <div>
      <h3>{title}</h3>
      <p>{emptyText}</p>
    </div>
    <button class="primary-btn" type="button" on:click={() => dispatch('add')}>{addLabel}</button>
  </div>

  {#if rows.length === 0}
    <div class="empty-box">{emptyText}</div>
  {:else}
    <div class="rows-list">
      {#each rows as row, index (keyForRow(row, index))}
        {@const rowKey = keyForRow(row, index)}
        <article class="row-card">
          <div class="row-card-head">
            <div class="row-card-title">
              <strong>{row.id ? `Запись #${row.id}` : `Новая запись ${index + 1}`}</strong>
            </div>
            <div class="row-card-actions">
              <button class="mini-btn" type="button" on:click={() => dispatch('duplicate', { index })}>Дублировать</button>
              <button class="mini-btn" type="button" on:click={() => dispatch('save', { index })} disabled={busyRowKey === rowKey}>
                {busyRowKey === rowKey ? 'Сохраняем...' : 'Сохранить'}
              </button>
              <button class="danger-btn" type="button" on:click={() => dispatch('remove', { index })} disabled={busyRowKey === rowKey}>Удалить</button>
            </div>
          </div>

          <div class="form-grid">
            {#each fields as field}
              <label class:checkbox-field={field.type === 'checkbox'}>
                <span>{field.label}</span>
                {#if field.type === 'textarea'}
                  <textarea
                    rows={field.rows || 3}
                    value={String(row?.[field.key] ?? '')}
                    placeholder={field.placeholder || ''}
                    on:input={(e) => onTextChange(index, field.key, textValue(e))}
                  ></textarea>
                {:else if field.type === 'select'}
                  <select
                    value={String(row?.[field.key] ?? '')}
                    on:change={(e) => onTextChange(index, field.key, textValue(e))}
                  >
                    <option value="">Выбери значение</option>
                    {#each optionsForField(field) as option}
                      <option value={option.value}>{option.label}</option>
                    {/each}
                  </select>
                {:else if field.type === 'checkbox'}
                  <input
                    type="checkbox"
                    checked={Boolean(row?.[field.key])}
                    on:change={(e) => onBoolChange(index, field.key, boolValue(e))}
                  />
                {:else}
                  <input
                    type={field.type === 'number' ? 'number' : field.type === 'date' ? 'date' : field.type === 'datetime' ? 'datetime-local' : 'text'}
                    value={String(row?.[field.key] ?? '')}
                    placeholder={field.placeholder || ''}
                    min={field.min ?? undefined}
                    step={field.step ?? undefined}
                    on:input={(e) => onTextChange(index, field.key, textValue(e))}
                  />
                {/if}
              </label>
            {/each}
          </div>
        </article>
      {/each}
    </div>
  {/if}
</section>

<style>
  .rows-editor { display: flex; flex-direction: column; gap: 12px; min-width: 0; }
  .rows-head { display: flex; justify-content: space-between; gap: 12px; align-items: flex-start; }
  .rows-head h3 { margin: 0; font-size: 16px; }
  .rows-head p { margin: 4px 0 0; font-size: 12px; color: #64748b; }
  .rows-list { display: flex; flex-direction: column; gap: 12px; }
  .row-card { border: 1px solid #e2e8f0; border-radius: 14px; padding: 14px; background: #fff; display: flex; flex-direction: column; gap: 12px; }
  .row-card-head { display: flex; justify-content: space-between; gap: 12px; align-items: center; flex-wrap: wrap; }
  .row-card-actions { display: inline-flex; gap: 8px; flex-wrap: wrap; }
  .form-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; min-width: 0; }
  label { display: flex; flex-direction: column; gap: 6px; min-width: 0; font-size: 12px; color: #334155; }
  label span { font-weight: 600; }
  input, textarea, select {
    width: 100%;
    min-width: 0;
    box-sizing: border-box;
    border: 1px solid #cbd5e1;
    border-radius: 10px;
    padding: 9px 10px;
    font: inherit;
    color: #0f172a;
    background: #fff;
  }
  textarea { resize: vertical; }
  .checkbox-field { justify-content: center; }
  .checkbox-field input { width: auto; align-self: flex-start; min-width: auto; }
  .primary-btn, .mini-btn, .danger-btn {
    border-radius: 10px;
    border: 1px solid #dbe4f0;
    background: #fff;
    padding: 8px 10px;
    cursor: pointer;
    font-size: 12px;
  }
  .primary-btn { background: #0f172a; border-color: #0f172a; color: #fff; }
  .danger-btn { border-color: #fecaca; color: #b91c1c; }
  .empty-box {
    border: 1px dashed #cbd5e1;
    border-radius: 12px;
    padding: 16px;
    color: #64748b;
    background: #f8fafc;
    font-size: 13px;
  }
  @media (max-width: 1280px) {
    .form-grid { grid-template-columns: 1fr; }
  }
</style>


