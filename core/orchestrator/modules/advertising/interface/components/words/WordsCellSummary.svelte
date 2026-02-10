<script>
  import { createEventDispatcher } from 'svelte';
  
  export let wordData = {};
  
  const dispatch = createEventDispatcher();
  
  function handleClick() {
    dispatch('click');
  }
</script>

<div class="words-cell-summary" on:click={handleClick}>
  <div class="summary-content">
    <div class="summary-item">
      <span class="label">Кластеры:</span>
      <span class="value">{wordData.clusters || 0}</span>
    </div>
    <div class="summary-item">
      <span class="label">В минус:</span>
      <span class="value">{wordData.minus || 0}</span>
    </div>
    {#if wordData.hasChanges}
      <div class="changes-badge">Есть изменения</div>
    {/if}
  </div>
</div>

<style>
  .words-cell-summary {
    display: flex;
    align-items: center;
    height: 100%;
    cursor: pointer;
    padding: 8px;
    border-radius: 6px;
    transition: background-color 0.2s ease;
  }
  
  .words-cell-summary:hover {
    background-color: #f5f5f7;
  }
  
  .summary-content {
    display: flex;
    flex-direction: column;
    gap: 4px;
    font-size: 12px;
  }
  
  .summary-item {
    display: flex;
    justify-content: space-between;
    gap: 8px;
  }
  
  .label {
    color: #86868b;
  }
  
  .value {
    color: #000000;
    font-weight: 500;
  }
  
  .changes-badge {
    display: inline-block;
    font-size: 10px;
    background-color: #ff9f0a;
    color: white;
    padding: 2px 6px;
    border-radius: 4px;
    margin-top: 4px;
  }
</style>