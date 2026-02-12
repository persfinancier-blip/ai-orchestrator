<script>
  import GroupContextMenu from '@components/actions/GroupContextMenu.svelte';
  import { createEventDispatcher } from 'svelte';
  
  export let name = '';
  export let count = 0;
  
  const dispatch = createEventDispatcher();
  let showContextMenu = false;
  
  function handleContextMenu(e) {
    e.preventDefault();
    showContextMenu = true;
  }
  
  function handleAction(action) {
    dispatch('action', { action, group: name });
    showContextMenu = false;
  }
</script>

<div class="group-row-header" on:contextmenu={handleContextMenu}>
  <div class="group-info">
    <div class="group-name">{name}</div>
    <div class="group-count">{count} артикулов</div>
  </div>
  
  <div class="group-actions">
    <button class="context-button" on:click={() => showContextMenu = true}>
      ⋮
    </button>
  </div>
  
  {#if showContextMenu}
    <GroupContextMenu 
      groupName={name}
      on:action={handleAction}
      on:close={() => showContextMenu = false}
    />
  {/if}
</div>

<style>
  .group-row-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 12px 16px;
    background-color: #f5f5f7;
    border-bottom: 1px solid #d2d2d7;
    font-weight: 600;
    position: relative;
  }
  
  .group-info {
    display: flex;
    align-items: center;
    gap: 12px;
  }
  
  .group-name {
    font-size: 16px;
    color: #000000;
  }
  
  .group-count {
    font-size: 14px;
    color: #86868b;
    background-color: white;
    padding: 2px 8px;
    border-radius: 12px;
    border: 1px solid #d2d2d7;
  }
  
  .group-actions {
    display: flex;
    gap: 8px;
  }
  
  .context-button {
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
  
  .context-button:hover {
    background-color: #e8f1ff;
    border-color: #0071e3;
    color: #0071e3;
  }
</style>