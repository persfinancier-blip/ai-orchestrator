<script lang="ts">
  import { tick } from 'svelte';

  export let title: string;
  export let placeholder: string;
  export let value: string;

  export let onCancel: () => void;
  export let onSave: (value: string) => void;

  let inputEl: HTMLInputElement | null = null;

  function stop(e: MouseEvent): void {
    e.stopPropagation();
  }

  (async () => {
    await tick();
    inputEl?.focus();
  })();
</script>

<div class="modal-backdrop" on:click={onCancel}>
  <div class="modal save-modal" on:click={stop}>
    <div class="modal-title">{title}</div>

    <div class="field">
      <input
        bind:this={inputEl}
        class="input save-input"
        {placeholder}
        bind:value
      />
    </div>

    <div class="row two">
      <button class="btn" type="button" on:click={onCancel}>Отмена</button>
      <button class="btn btn-primary" type="button" on:click={() => onSave(value)}>Сохранить</button>
    </div>
  </div>
</div>

<style>
  .save-modal {
    padding: 16px;        /* ✅ “отступ как в начале” */
    border-radius: 18px;
    overflow: hidden;     /* ✅ режем всё по скруглению */
    box-sizing: border-box;
  }

  .field { margin-top: 10px; margin-bottom: 12px; }

  .save-input {
    width: 100%;
    max-width: 100%;
    box-sizing: border-box;

    /* ✅ длинные строки не вылезают */
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  /* ✅ убираем зелёную подсветку/рамку ТОЛЬКО в модалке */
  .save-modal :global(.input:focus),
  .save-modal :global(.select:focus) {
    box-shadow: none !important;
    outline: none !important;
  }

  /* ✅ если где-то осталась зелёная полоса у primary (на всякий) */
  .save-modal :global(.btn.btn-primary::after) {
    content: none !important;
    display: none !important;
  }
</style>
