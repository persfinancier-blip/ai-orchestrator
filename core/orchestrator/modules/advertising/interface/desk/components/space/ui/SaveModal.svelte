<!-- core/orchestrator/modules/advertising/interface/desk/components/space/ui/SaveModal.svelte -->
<script lang="ts">
  export let title: string;
  export let placeholder: string;
  export let value: string;

  export let onCancel: () => void;
  export let onSave: (v: string) => void;

  function stop(e: Event): void {
    e.stopPropagation();
  }

  function onBackdropKey(e: KeyboardEvent): void {
    if (e.key === 'Escape' || e.key === 'Enter' || e.key === ' ') onCancel();
  }

  function onModalKey(e: KeyboardEvent): void {
    if (e.key === 'Escape') onCancel();
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') onSave(value);
  }
</script>

<div
  class="modal-backdrop"
  role="button"
  tabindex="0"
  on:click={onCancel}
  on:keydown={onBackdropKey}
>
  <div class="modal save-modal" role="dialog" aria-modal="true" tabindex="0" on:click={stop} on:keydown={onModalKey}>
    <div class="modal-title">{title}</div>

    <input class="input" placeholder={placeholder} bind:value />

    <div class="row two">
      <button class="btn" type="button" on:click={onCancel}>Отмена</button>
      <button class="btn btn-primary" type="button" on:click={() => onSave(value)}>Сохранить</button>
    </div>
  </div>
</div>

<style>
  .modal-backdrop {
    position: fixed;
    inset: 0;
    z-index: 50;
    display: grid;
    place-items: center;
    background: rgba(15, 23, 42, 0.35);
    padding: 12px;
    box-sizing: border-box;
  }

  .modal {
    width: min(520px, calc(100vw - 24px));
    background: rgba(255, 255, 255, 0.92);
    border-radius: 18px;
    padding: 14px;
    box-shadow: var(--shadow-modal, 0 30px 80px rgba(15, 23, 42, 0.24));
    backdrop-filter: blur(14px);
    border: 1px solid var(--stroke-soft, rgba(15, 23, 42, 0.08));
    box-sizing: border-box;
    overflow: hidden;
  }

  .modal-title {
    font-weight: 800;
    font-size: 14px;
    color: rgba(15, 23, 42, 0.92);
    margin-bottom: 10px;
  }

  .row {
    display: flex;
    gap: 10px;
    align-items: center;
    margin-top: 10px;
  }

  .row.two {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 10px;
  }

  .input {
    width: 100%;
    border: 1px solid var(--stroke-soft, rgba(15, 23, 42, 0.08));
    background: rgba(248, 251, 255, 0.9);
    border-radius: 12px;
    padding: 10px 12px;
    font-size: 12px;
    outline: none;
    color: rgba(15, 23, 42, 0.9);
    box-sizing: border-box;
  }

  .input:focus {
    box-shadow: var(--focus-ring, 0 0 0 4px rgba(15, 23, 42, 0.1));
    border-color: var(--stroke-mid, rgba(15, 23, 42, 0.12));
  }

  /* btn стили у тебя глобальные (GraphPanel.svelte), тут не дублирую */
</style>
