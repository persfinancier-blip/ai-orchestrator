<script>
  import ArticleRow from './ArticleRow.svelte';
  import GroupRowHeader from './GroupRowHeader.svelte';
  import { createEventDispatcher } from 'svelte';
  
  export let articles = [];
  export let selectedArticles = [];
  export let groupSettings = {};
  
  const dispatch = createEventDispatcher();
  
  // Группировка артикулов по настройкам
  $: groupedArticles = groupArticles(articles, groupSettings);
  
  function groupArticles(articles, settings) {
    if (!settings.rows || settings.rows.length === 0) {
      return [{ type: 'ungrouped', items: articles }];
    }
    
    // Пока что простая группировка по предмету
    const groups = {};
    articles.forEach(article => {
      const key = article.subject || 'Без категории';
      if (!groups[key]) {
        groups[key] = [];
      }
      groups[key].push(article);
    });
    
    return Object.entries(groups).map(([name, items]) => ({
      type: 'group',
      name,
      items
    }));
  }
  
  function handleSelect(articleId, selected) {
    dispatch('select', { articleId, selected });
  }
  
  function handleToggle(articleId, toggleType, value) {
    dispatch('toggle', { articleId, toggleType, value });
  }
</script>

<div class="articles-table">
  <div class="table-header">
    <div class="header-cell checkbox-cell">
      <input 
        type="checkbox" 
        title="Выбрать все"
      />
    </div>
    <div class="header-cell identification">Артикул / Название</div>
    <div class="header-cell metrics">Метрики</div>
    <div class="header-cell campaign">РК</div>
    <div class="header-cell words">Слова / Кластеры</div>
  </div>
  
  <div class="table-body">
    {#each groupedArticles as group}
      {#if group.type === 'group'}
        <GroupRowHeader 
          name={group.name}
          count={group.items.length}
          on:action={(e) => console.log('Group action:', e.detail)}
        />
      {/if}
      
      {#each group.items as article}
        <ArticleRow 
          {article}
          selected={selectedArticles.includes(article.id)}
          on:select={(e) => handleSelect(article.id, e.detail)}
          on:toggle={(e) => handleToggle(article.id, e.detail.type, e.detail.value)}
          on:openWords={(e) => dispatch('openWords', { articleId: article.id })}
        />
      {/each}
    {/each}
  </div>
</div>

<style>
  .articles-table {
    flex: 1;
    display: flex;
    flex-direction: column;
    background-color: white;
    border: 1px solid #d2d2d7;
    border-radius: 12px;
    overflow: hidden;
    box-shadow: 0 2px 10px rgba(0, 0, 0, 0.05);
  }
  
  .table-header {
    display: flex;
    background-color: #f5f5f7;
    border-bottom: 1px solid #d2d2d7;
    padding: 12px 16px;
    font-weight: 500;
    font-size: 14px;
    color: #86868b;
  }
  
  .table-body {
    flex: 1;
    overflow-y: auto;
  }
  
  .header-cell {
    padding: 0 12px;
  }
  
  .checkbox-cell {
    width: 40px;
  }
  
  .identification {
    flex: 2;
    min-width: 200px;
  }
  
  .metrics {
    flex: 3;
    min-width: 300px;
  }
  
  .campaign {
    flex: 2;
    min-width: 200px;
  }
  
  .words {
    width: 150px;
  }
</style>