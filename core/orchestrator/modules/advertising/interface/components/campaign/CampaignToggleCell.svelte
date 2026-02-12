<script>
  import ToggleTree from './ToggleTree.svelte';
  import { createEventDispatcher } from 'svelte';
  
  export let campaignData = {};
  
  const dispatch = createEventDispatcher();
  
  function handleToggle(togglePath, value) {
    dispatch('toggle', { type: togglePath.join('.'), value });
  }
</script>

<div class="campaign-toggle-cell">
  <ToggleTree 
    items={[
      {
        id: 'cpm',
        label: 'CPM',
        type: 'toggle',
        value: campaignData?.cpm?.enabled || false,
        children: campaignData?.cpm?.enabled ? [
          {
            id: 'bid-mode',
            label: 'Режим ставки',
            type: 'radio-group',
            value: campaignData?.cpm?.bidMode || 'single',
            options: [
              { id: 'single', label: 'Единая ставка' },
              { id: 'manual', label: 'Ручная ставка' }
            ],
            children: (campaignData?.cpm?.bidMode === 'manual' || 
                     (campaignData?.cpm?.bidMode !== 'single' && campaignData?.cpm?.enabled)) ? [
              {
                id: 'placements',
                label: 'Плейсменты',
                type: 'group',
                children: [
                  {
                    id: 'search',
                    label: 'Поиск',
                    type: 'toggle',
                    value: campaignData?.cpm?.placements?.search || false
                  },
                  {
                    id: 'recommendations',
                    label: 'Рекомендации',
                    type: 'toggle',
                    value: campaignData?.cpm?.placements?.recommendations || false
                  }
                ]
              }
            ] : []
          }
        ] : []
      },
      {
        id: 'cpc',
        label: 'CPC',
        type: 'toggle',
        value: campaignData?.cpc?.enabled || false
      }
    ]}
    on:toggle={(e) => handleToggle(e.detail.path, e.detail.value)}
  />
</div>

<style>
  .campaign-toggle-cell {
    display: flex;
    align-items: center;
    height: 100%;
  }
</style>