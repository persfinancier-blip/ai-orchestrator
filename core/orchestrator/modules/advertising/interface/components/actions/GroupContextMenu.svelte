<script>
  import { createEventDispatcher } from 'svelte';
  
  export let groupName = '';
  
  const dispatch = createEventDispatcher();
  
  function handleAction(action) {
    dispatch('action', action);
  }
  
  function handleClose() {
    dispatch('close');
  }
</script>

<div class="group-context-menu" on:click|stopPropagation>
  <div class="menu-header">
    <h4>Действия для группы "{groupName}"</h4>
  </div>
  
  <div class="menu-items">
    <button class="menu-item" on:click={() => handleAction('enableCpm')}>
      Включить CPM для группы
    </button>
    <button class="menu-item" on:click={() => handleAction('disableCpm')}>
      Выключить CPM для группы
    </button>
    <button class="menu-item" on:click={() => handleAction('enableCpc')}>
      Включить CPC для группы
    </button>
    <button class="menu-item" on:click={() => handleAction('disableCpc')}>
      Выключить CPC для группы
    </button>
    <button class="menu-item" on:click={() => handleAction('assignStrategy')}>
      Назначить автостратегию
    </button>
    <button class="menu-item" on:click={() => handleAction('viewMetrics')}>
      Показать агрегатные метрики
    </button>
  </div>
  
  <div class="menu-footer">
    <button class="close-button" on:click={handleClose}>
      Закрыть
    </button>
  </div>
</div>

<style>
  .group-context-menu {
    position: absolute;
    top: 40px;
    right: 0;
    width: 250px;
    background-color: white;
    border: 1px solid #d2d2d7;
    border-radius: 12px;
    box-shadow: 0 4px 20px rgba(0, 0, 0, 0.15);
    z-index: 1000;
    padding: 16px;
  }
  
  .menu-header h4 {
    margin: 0 0 12px 0;
    font-size: 16px;
    font-weight: 600;
    color: #000000;
  }
  
  .menu-items {
    display: flex;
    flex-direction: column;
    gap: 8px;
    margin-bottom: 16px;
  }
  
  .menu-item {
    padding: 8px 12px;
    text-align: left;
    background-color: white;
    color: #000000;
    border: 1px solid #f5f5f7;
    border-radius: 6px;
    font-size: 14px;
    cursor: pointer;
    transition: all 0.2s ease;
  }
  
  .menu-item:hover {
    background-color: #f5f5f7;
    border-color: #d2d2d7;
  }
  
  .menu-footer {
    display: flex;
    justify-content: flex-end;
  }
  
  .close-button {
    padding: 6px 12px;
    background-color: white;
    color: #86868b;
    border: 1px solid #d2d2d7;
    border-radius: 6px;
    font-size: 14px;
    cursor: pointer;
    transition: all 0.2s ease;
  }
  
  .close-button:hover {
    background-color: #f5f5f7;
    border-color: #c0c0c4;
  }
</style>