<script>
  import { createEventDispatcher } from 'svelte';
  
  const dispatch = createEventDispatcher();
  
  let searchTerm = '';
  let filters = {};
  
  function handleSearch() {
    dispatch('search', { term: searchTerm });
  }
  
  function handleFilter() {
    dispatch('filter', { filters });
  }
  
  function clearFilters() {
    filters = {};
    dispatch('filter', { filters });
  }
</script>

<div class="control-bar">
  <div class="search-section">
    <input 
      type="text" 
      bind:value={searchTerm} 
      placeholder="Поиск по артикулам, названиям..." 
      class="search-input"
    />
    <button class="search-button" on:click={handleSearch}>Найти</button>
  </div>
  
  <div class="filter-section">
    <button class="filter-button" on:click={handleFilter}>Применить фильтры</button>
    <button class="clear-button" on:click={clearFilters}>Сбросить</button>
  </div>
</div>

<style>
  .control-bar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 16px 20px;
    background: #f8f9fa;
    border-radius: 8px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    margin-bottom: 20px;
  }
  
  .search-section {
    display: flex;
    gap: 12px;
    align-items: center;
  }
  
  .search-input {
    padding: 8px 12px;
    border: 1px solid #ddd;
    border-radius: 4px;
    font-size: 14px;
    min-width: 300px;
  }
  
  .search-button, .filter-button, .clear-button {
    padding: 8px 16px;
    border: none;
    border-radius: 4px;
    font-size: 14px;
    cursor: pointer;
    transition: background-color 0.2s;
  }
  
  .search-button {
    background-color: #007bff;
    color: white;
  }
  
  .search-button:hover {
    background-color: #0056b3;
  }
  
  .filter-button {
    background-color: #28a745;
    color: white;
  }
  
  .filter-button:hover {
    background-color: #218838;
  }
  
  .clear-button {
    background-color: #6c757d;
    color: white;
  }
  
  .clear-button:hover {
    background-color: #5a6268;
  }
</style>