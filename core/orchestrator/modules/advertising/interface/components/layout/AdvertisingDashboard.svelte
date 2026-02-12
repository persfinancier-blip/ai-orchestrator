<script>
  import ControlBar from './ControlBar.svelte';
  import PivotGroupPanel from './PivotGroupPanel.svelte';
  import ArticlesTable from '../table/ArticlesTable.svelte';
  import BulkActionBar from '../actions/BulkActionBar.svelte';
  import InspectorDrawer from '../words/InspectorDrawer.svelte';
  import DraftChangesBanner from '../status/DraftChangesBanner.svelte';
  
  import { articlesStore } from '../../stores/articlesStore';
  import { uiStateStore } from '../../stores/uiStateStore';
  
  let articles = [];
  let selectedArticles = [];
  let showBulkActions = false;
  let showInspector = false;
  let draftChanges = [];
  let groupSettings = {};
  
  // Подписка на хранилища
  articlesStore.subscribe(value => {
    articles = value;
  });
  
  uiStateStore.subscribe(value => {
    selectedArticles = value.selectedArticles;
    showBulkActions = selectedArticles.length > 0;
    showInspector = value.showInspector;
    draftChanges = value.draftChanges;
  });
</script>

<div class="advertising-dashboard">
  {#if draftChanges.length > 0}
    <DraftChangesBanner {draftChanges} />
  {/if}
  
  <ControlBar 
    on:search={(e) => console.log('Search:', e.detail)}
    on:filter={(e) => console.log('Filter:', e.detail)}
  />
  
  <PivotGroupPanel 
    on:groupChange={(e) => console.log('Group changed:', e.detail)}
  />
  
  {#if showBulkActions}
    <BulkActionBar 
      selectedCount={selectedArticles.length}
      on:action={(e) => console.log('Bulk action:', e.detail)}
    />
  {/if}
  
  <div class="main-content">
    <ArticlesTable 
      {articles}
      {selectedArticles}
      {groupSettings}
      on:select={(e) => console.log('Article selected:', e.detail)}
      on:toggle={(e) => console.log('Toggle changed:', e.detail)}
    />
    
    {#if showInspector}
      <InspectorDrawer 
        on:close={() => uiStateStore.update(state => ({...state, showInspector: false}))}
        on:save={(e) => console.log('Inspector save:', e.detail)}
      />
    {/if}
  </div>
</div>

<style>
  .advertising-dashboard {
    display: flex;
    flex-direction: column;
    height: 100%;
    padding: 20px;
    background-color: #ffffff;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  }
  
  .main-content {
    display: flex;
    flex: 1;
    gap: 20px;
    margin-top: 20px;
  }
</style>