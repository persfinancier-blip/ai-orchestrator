<script lang="ts">
  export let title: string;
  export let placeholder: string;
  export let value = '';
  export let onCancel: () => void;
  export let onSave: (value: string) => void;
</script>

<div class="modal-backdrop" on:click={onCancel}>
  <div class="modal" on:click|stopPropagation>
    <div class="modal-title">{title}</div>
    <input class="input" placeholder={placeholder} bind:value />
    <div class="row two">
      <button class="btn" on:click={onCancel}>Отмена</button>
      <button class="btn btn-primary" on:click={() => onSave(value)}>Сохранить</button>
    </div>
  </div>
</div>
