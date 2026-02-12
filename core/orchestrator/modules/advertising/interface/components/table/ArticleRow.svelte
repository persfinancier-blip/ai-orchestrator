<script>
  import CampaignToggleCell from '@components/campaign/CampaignToggleCell.svelte';
  import WordsCellSummary from '@components/words/WordsCellSummary.svelte';
  import { createEventDispatcher } from 'svelte';
  
  export let article = {};
  export let selected = false;
  
  const dispatch = createEventDispatcher();
  
  function handleSelect() {
    dispatch('select', !selected);
  }
  
  function handleToggle(toggleType, value) {
    dispatch('toggle', { type: toggleType, value });
  }
  
  function handleOpenWords() {
    dispatch('openWords');
  }
</script>

<div class="article-row" class:selected>
  <div class="cell checkbox-cell">
    <input 
      type="checkbox" 
      bind:checked={selected}
      on:change={handleSelect}
    />
  </div>
  
  <div class="cell identification-cell">
    <div class="article-info">
      <div class="article-id">{article.id}</div>
      <div class="article-name">{article.name || 'Без названия'}</div>
      <div class="article-subject">{article.subject || 'Без категории'}</div>
      <div class="user-fields">
        {#if article.userFields}
          {#each article.userFields.slice(0, 3) as field}
            <span class="user-field-tag">{field}</span>
          {/each}
          {#if article.userFields.length > 3}
            <span class="more-fields">+{article.userFields.length - 3}</span>
          {/if}
        {/if}
      </div>
    </div>
  </div>
  
  <div class="cell metrics-cell">
    <div class="metrics-grid">
      <div class="metric">
        <div class="metric-label">ДРР</div>
        <div class="metric-value {article.drr > 20 ? 'high' : article.drr > 10 ? 'medium' : 'low'}">
          {article.drr || 0}%
        </div>
      </div>
      <div class="metric">
        <div class="metric-label">Расход</div>
        <div class="metric-value">{article.expense || 0} ₽</div>
      </div>
      <div class="metric">
        <div class="metric-label">Выручка</div>
        <div class="metric-value">{article.revenue || 0} ₽</div>
      </div>
      <div class="metric">
        <div class="metric-label">Заказы</div>
        <div class="metric-value">{article.orders || 0}</div>
      </div>
      <div class="metric">
        <div class="metric-label">CTR</div>
        <div class="metric-value">{article.ctr || 0}%</div>
      </div>
      <div class="metric">
        <div class="metric-label">CPC/CPM</div>
        <div class="metric-value">{article.cpc || article.cpm || 0}</div>
      </div>
    </div>
  </div>
  
  <div class="cell campaign-cell">
    <CampaignToggleCell 
      campaignData={article.campaigns}
      on:toggle={(e) => handleToggle(e.detail.type, e.detail.value)}
    />
  </div>
  
  <div class="cell words-cell">
    <WordsCellSummary 
      wordData={article.words}
      on:click={handleOpenWords}
    />
  </div>
</div>

<style>
  .article-row {
    display: flex;
    align-items: center;
    padding: 12px 16px;
    border-bottom: 1px solid #f5f5f7;
    transition: background-color 0.2s ease;
  }
  
  .article-row:hover {
    background-color: #f5f5f7;
  }
  
  .article-row.selected {
    background-color: #e8f1ff;
  }
  
  .cell {
    padding: 0 12px;
    display: flex;
    align-items: center;
  }
  
  .checkbox-cell {
    width: 40px;
  }
  
  .identification-cell {
    flex: 2;
    min-width: 200px;
  }
  
  .metrics-cell {
    flex: 3;
    min-width: 300px;
  }
  
  .campaign-cell {
    flex: 2;
    min-width: 200px;
  }
  
  .words-cell {
    width: 150px;
  }
  
  .article-info {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }
  
  .article-id {
    font-weight: 600;
    font-size: 14px;
    color: #000000;
  }
  
  .article-name {
    font-size: 14px;
    color: #000000;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 180px;
  }
  
  .article-subject {
    font-size: 12px;
    color: #86868b;
  }
  
  .user-fields {
    display: flex;
    gap: 4px;
    flex-wrap: wrap;
  }
  
  .user-field-tag {
    font-size: 10px;
    background-color: #e8f1ff;
    color: #0071e3;
    padding: 2px 6px;
    border-radius: 4px;
  }
  
  .more-fields {
    font-size: 10px;
    color: #86868b;
  }
  
  .metrics-grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 12px;
    width: 100%;
  }
  
  .metric {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }
  
  .metric-label {
    font-size: 12px;
    color: #86868b;
  }
  
  .metric-value {
    font-size: 14px;
    font-weight: 500;
    color: #000000;
  }
  
  .metric-value.high {
    color: #ff453a;
  }
  
  .metric-value.medium {
    color: #ff9f0a;
  }
  
  .metric-value.low {
    color: #32d74b;
  }
</style>