<script>
  import { createEventDispatcher } from 'svelte';
  
  export let selectedCount = 0;
  
  const dispatch = createEventDispatcher();
  
  function handleAction(actionType) {
    dispatch('action', { type: actionType });
  }
</script>

<div class="bulk-action-bar">
  <div class="selected-count">
    Выбрано: {selectedCount} элементов
  </div>
  <div class="actions">
    <button class="action-button" on:click={() => handleAction('activate')}>
      Активировать
    </button>
    <button class="action-button" on:click={() => handleAction('pause')}>
      Приостановить
    </button>
    <button class="action-button" on:click={() => handleAction('archive')}>
      В архив
    </button>
    <button class="action-button destructive" on:click={() => handleAction('delete')}>
      Удалить
    </button>
  </div>
</div>

<style>
  .bulk-action-bar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 12px 16px;
    background-color: #f5f5f7;
    border: 1px solid #d2d2d7;
    border-radius: 12px;
    margin-bottom: 20px;
  }
  
  .selected-count {
    font-size: 14px;
    font-weight: 500;
    color: #86868b;
  }
  
  .actions {
    display: flex;
    gap: 8px;
  }
  
  .action-button {
    padding: 8px 16px;
    border: 1px solid #d2d2d7;
    border-radius: 8px;
    background-color: white;
    font-size: 14px;
    font-weight: 500;
    color: #1d1d1f;
    cursor: pointer;
    transition: all 0.2s ease;
  }
  
  .action-button:hover {
    background-color: #f5f5f7;
    border-color: #86868b;
  }
  
  .action-button.destructive {
    color: #ff3b30;
    border-color: #ff3b30;
  }
  
  .action-button.destructive:hover {
    background-color: #ff3b30;
    color: white;
  }
</style>