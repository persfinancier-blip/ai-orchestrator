<script lang="ts">
  export let title: string;
  export let placeholder: string;
  export let value: string;

  export let onCancel: () => void;
  export let onSave: (value: string) => void;

  function stop(e: MouseEvent): void {
    e.stopPropagation();
  }
</script>

<div class="modal-backdrop" on:click={onCancel}>
  <div class="modal save-modal" on:click={stop}>
    <div class="modal-title">{title}</div>

    <input class="input" {placeholder} bind:value />

    <div class="row two">
      <button class="btn" type="button" on:click={onCancel}>Отмена</button>
      <button class="btn btn-primary" type="button" on:click={() => onSave(value)}>Сохранить</button>
    </div>
  </div>
</div>

<style>
  .save-modal {
    overflow: hidden; /* ✅ режем всё по скруглению */
  }

</style>
