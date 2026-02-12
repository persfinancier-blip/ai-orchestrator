<script>
  import { createEventDispatcher } from 'svelte';
  
  export let placements = {};
  
  const dispatch = createEventDispatcher();
  
  function handleToggle(placement, value) {
    dispatch('toggle', { placement, value });
  }
</script>

<div class="placement-toggle-row">
  <div class="placement-item">
    <label class="placement-toggle">
      <input 
        type="checkbox"
        checked={placements.search}
        on:change={(e) => handleToggle('search', e.target.checked)}
      />
      <span class="placement-text">Поиск</span>
    </label>
  </div>
  
  <div class="placement-item">
    <label class="placement-toggle">
      <input 
        type="checkbox"
        checked={placements.recommendations}
        on:change={(e) => handleToggle('recommendations', e.target.checked)}
      />
      <span class="placement-text">Рекомендации</span>
    </label>
  </div>
</div>

<style>
  .placement-toggle-row {
    display: flex;
    flex-direction: column;
    gap: 4px;
    margin-left: 36px;
  }
  
  .placement-item {
    display: flex;
    align-items: center;
  }
  
  .placement-toggle {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 14px;
    color: #000000;
    cursor: pointer;
  }
  
  .placement-toggle input[type="checkbox"] {
    width: 16px;
    height: 16px;
    accent-color: #0071e3;
  }
  
  .placement-text {
    user-select: none;
  }
</style>