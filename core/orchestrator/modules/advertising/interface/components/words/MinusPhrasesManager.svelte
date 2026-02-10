<script>
  import { createEventDispatcher } from 'svelte';
  
  export let articleId = null;
  export let selectedArticle = null;
  
  const dispatch = createEventDispatcher();
  
  // Пример данных для минус-фраз
  let minusPhrases = [
    { id: 1, name: 'футболка мужская', status: 'active' },
    { id: 2, name: 'детская футболка', status: 'active' },
    { id: 3, name: 'хлопок', status: 'minus' },
    { id: 4, name: 'принт', status: 'minus' },
    { id: 5, name: 'зимняя футболка', status: 'active' }
  ];
  
  let selectedPhrases = new Set();
  
  function handlePhraseToggle(phraseId) {
    if (selectedPhrases.has(phraseId)) {
      selectedPhrases.delete(phraseId);
    } else {
      selectedPhrases.add(phraseId);
    }
    selectedPhrases = new Set(selectedPhrases); // Триггер обновления
  }
  
  function handleAddToMinus() {
    // Логика добавления в минус-фразы
    console.log('Add to minus phrases:', Array.from(selectedPhrases));
  }
  
  function handleRemoveFromMinus() {
    // Логика удаления из минус-фраз
    console.log('Remove from minus phrases:', Array.from(selectedPhrases));
  }
  
  function handleApply() {
    // Логика применения изменений
    console.log('Apply minus phrase changes');
  }
  
  function handleRevert() {
    // Логика отката изменений
    console.log('Revert minus phrase changes');
  }
</script>

<div class="minus-phrases-manager">
  <div class="phrases-list">
    <div class="phrases-header">
      <div class="header-item">Кластер</div>
      <div class="header-item">Статус</div>
      <div class="header-item">Действие</div>
    </div>
    
    {#each minusPhrases as phrase}
      <div class="phrase-item" class:selected={selectedPhrases.has(phrase.id)}>
        <div class="phrase-name">{phrase.name}</div>
        <div class="phrase-status">
          <span class="status-badge" class:active={phrase.status === 'active'} class:minus={phrase.status === 'minus'}>
            {phrase.status === 'minus' ? 'В минусе' : 'Активен'}
          </span>
        </div>
        <div class="phrase-action">
          <label class="checkbox-label">
            <input 
              type="checkbox"
              checked={selectedPhrases.has(phrase.id)}
              on:change={() => handlePhraseToggle(phrase.id)}
            />
            {#if phrase.status === 'minus'}
              Вернуть из минус
            {:else}
              Добавить в минус
            {/if}
          </label>
        </div>
      </div>
    {/each}
  </div>
  
  <div class="manager-actions">
    <button class="secondary-button" on:click={handleRevert}>
      Откатить
    </button>
    <button class="primary-button" on:click={handleApply}>
      Применить
    </button>
  </div>
</div>

<style>
  .minus-phrases-manager {
    display: flex;
    flex-direction: column;
    height: 100%;
  }
  
  .phrases-list {
    flex: 1;
    display: flex;
    flex-direction: column;
    gap: 8px;
    margin-bottom: 16px;
  }
  
  .phrases-header {
    display: flex;
    padding: 12px 8px;
    background-color: #f5f5f7;
    border-radius: 6px;
    font-weight: 500;
    color: #86868b;
  }
  
  .header-item {
    flex: 1;
  }
  
  .phrase-item {
    display: flex;
    align-items: center;
    padding: 12px 8px;
    border: 1px solid #d2d2d7;
    border-radius: 6px;
    transition: all 0.2s ease;
  }
  
  .phrase-item:hover {
    background-color: #f5f5f7;
  }
  
  .phrase-item.selected {
    background-color: #e8f1ff;
    border-color: #0071e3;
  }
  
  .phrase-name {
    flex: 1;
    font-size: 14px;
    color: #000000;
  }
  
  .phrase-status {
    flex: 1;
  }
  
  .phrase-action {
    flex: 2;
  }
  
  .status-badge {
    padding: 2px 8px;
    border-radius: 12px;
    font-size: 12px;
    font-weight: 500;
  }
  
  .status-badge.active {
    background-color: #e8f1ff;
    color: #0071e3;
  }
  
  .status-badge.minus {
    background-color: #ffe8e8;
    color: #ff453a;
  }
  
  .checkbox-label {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 14px;
    color: #000000;
    cursor: pointer;
  }
  
  .manager-actions {
    display: flex;
    justify-content: flex-end;
    gap: 12px;
    padding-top: 16px;
    border-top: 1px solid #d2d2d7;
  }
  
  .primary-button {
    padding: 8px 16px;
    background-color: #0071e3;
    color: white;
    border: none;
    border-radius: 8px;
    font-size: 14px;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.2s ease-out;
  }
  
  .primary-button:hover {
    background-color: #0077ed;
    box-shadow: 0 4px 12px rgba(0, 113, 227, 0.2);
  }
  
  .secondary-button {
    padding: 8px 16px;
    background-color: white;
    color: #0071e3;
    border: 1px solid #d2d2d7;
    border-radius: 8px;
    font-size: 14px;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.2s ease-out;
  }
  
  .secondary-button:hover {
    background-color: #f5f5f7;
    border-color: #c0c0c4;
  }
</style>