<script>
  import RadioToggleGroup from './RadioToggleGroup.svelte';
  import PlacementToggleRow from './PlacementToggleRow.svelte';
  import { createEventDispatcher } from 'svelte';
  
  export let items = [];
  export let path = [];
  
  const dispatch = createEventDispatcher();
  
  function handleToggle(item, value) {
    const currentPath = [...path, item.id];
    dispatch('toggle', { path: currentPath, value, item });
  }
  
  function handleRadioChange(item, value) {
    const currentPath = [...path, item.id];
    dispatch('toggle', { path: currentPath, value, item });
    
    // Отправляем события для отключения других опций в группе
    if (item.options) {
      item.options.forEach(option => {
        if (option.id !== value) {
          dispatch('toggle', { 
            path: [...currentPath, option.id], 
            value: false, 
            item: option 
          });
        }
      });
    }
  }
</script>

<div class="toggle-tree">
  {#each items as item}
    <div class="toggle-item">
      {#if item.type === 'toggle'}
        <div class="toggle-row">
          <label class="toggle-label">
            <input 
              type="checkbox"
              checked={item.value}
              on:change={(e) => handleToggle(item, e.target.checked)}
            />
            <span class="toggle-text">{item.label}</span>
          </label>
        </div>
        
        {#if item.value && item.children && item.children.length > 0}
          <div class="toggle-children">
            <svelte:self 
              items={item.children}
              path={[...path, item.id]}
              on:toggle
            />
          </div>
        {/if}
      {:else if item.type === 'radio-group'}
        <div class="radio-group-section">
          <div class="radio-group-label">{item.label}</div>
          <RadioToggleGroup 
            options={item.options}
            value={item.value}
            on:change={(e) => handleRadioChange(item, e.detail.value)}
          />
          
          {#if (item.value === 'manual' || (item.value !== 'single' && item.value)) && item.children}
            <div class="radio-children">
              <svelte:self 
                items={item.children}
                path={[...path, item.id]}
                on:toggle
              />
            </div>
          {/if}
        </div>
      {:else if item.type === 'group'}
        <div class="group-section">
          <div class="group-label">{item.label}</div>
          <div class="group-children">
            <svelte:self 
              items={item.children}
              path={[...path, item.id]}
              on:toggle
            />
          </div>
        </div>
      {/if}
    </div>
  {/each}
</div>

<style>
  .toggle-tree {
    display: flex;
    flex-direction: column;
    gap: 8px;
    width: 100%;
  }
  
  .toggle-item {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }
  
  .toggle-row {
    display: flex;
    align-items: center;
  }
  
  .toggle-label {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 14px;
    color: #000000;
    cursor: pointer;
  }
  
  .toggle-text {
    user-select: none;
  }
  
  .toggle-children {
    margin-left: 20px;
    padding-left: 12px;
    border-left: 1px solid #d2d2d7;
  }
  
  .radio-group-section {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }
  
  .radio-group-label {
    font-size: 12px;
    color: #86868b;
    margin-left: 24px;
  }
  
  .radio-children {
    margin-left: 36px;
    padding-left: 12px;
    border-left: 1px solid #d2d2d7;
  }
  
  .group-section {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }
  
  .group-label {
    font-size: 12px;
    color: #86868b;
    margin-left: 24px;
  }
  
  .group-children {
    margin-left: 24px;
  }

</style>
