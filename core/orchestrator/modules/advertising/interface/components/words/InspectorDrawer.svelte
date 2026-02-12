<script>
  import ClusterBidsTable from './ClusterBidsTable.svelte';
  import MinusPhrasesManager from './MinusPhrasesManager.svelte';
  import { createEventDispatcher } from 'svelte';
  
  export let articleId = null;
  export let selectedArticle = null;
  
  const dispatch = createEventDispatcher();
  let activeTab = 'bids'; // 'bids' or 'minus'
  
  function handleClose() {
    dispatch('close');
  }
  
  function handleSave() {
    dispatch('save', { tab: activeTab });
  }
</script>

<div class="inspector-drawer">
  <div class="drawer-header">
    <div class="header-content">
      <h3>Кластеры и ставки</h3>
      <button class="close-button" on:click={handleClose}>×</button>
    </div>
    
    <div class="tab-switcher">
      <button 
        class="tab-button" 
        class:active={activeTab === 'bids'}
        on:click={() => activeTab = 'bids'}
      >
        Ставки по кластерам
      </button>
      <button 
        class="tab-button" 
        class:active={activeTab === 'minus'}
        on:click={() => activeTab = 'minus'}
      >
        Минус-фразы
      </button>
    </div>
  </div>
  
  <div class="drawer-content">
    {#if activeTab === 'bids'}
      <ClusterBidsTable 
        articleId={articleId}
        selectedArticle={selectedArticle}
      />
    {:else if activeTab === 'minus'}
      <MinusPhrasesManager 
        articleId={articleId}
        selectedArticle={selectedArticle}
      />
    {/if}
  </div>
  
  <div class="drawer-footer">
    <button class="secondary-button" on:click={handleClose}>
      Отмена
    </button>
    <button class="primary-button" on:click={handleSave}>
      Применить
    </button>
  </div>
</div>

<style>
  .inspector-drawer {
    width: 400px;
    height: 100%;
    display: flex;
    flex-direction: column;
    background-color: white;
    border-left: 1px solid #d2d2d7;
    box-shadow: -2px 0 10px rgba(0, 0, 0, 0.05);
  }
  
  .drawer-header {
    padding: 16px;
    border-bottom: 1px solid #d2d2d7;
  }
  
  .header-content {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 16px;
  }
  
  .header-content h3 {
    margin: 0;
    font-size: 18px;
    font-weight: 600;
    color: #000000;
  }
  
  .close-button {
    width: 28px;
    height: 28px;
    border-radius: 6px;
    border: 1px solid #d2d2d7;
    background-color: white;
    color: #86868b;
    font-size: 16px;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
  }
  
  .close-button:hover {
    background-color: #f5f5f7;
  }
  
  .tab-switcher {
    display: flex;
    gap: 4px;
  }
  
  .tab-button {
    padding: 6px 12px;
    border: 1px solid #d2d2d7;
    background-color: white;
    color: #86868b;
    font-size: 14px;
    cursor: pointer;
    border-radius: 6px;
  }
  
  .tab-button.active {
    background-color: #0071e3;
    color: white;
    border-color: #0071e3;
  }
  
  .drawer-content {
    flex: 1;
    overflow-y: auto;
    padding: 16px;
  }
  
  .drawer-footer {
    padding: 16px;
    border-top: 1px solid #d2d2d7;
    display: flex;
    justify-content: flex-end;
    gap: 12px;
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