<script>
  import { createEventDispatcher } from 'svelte';
  
  const dispatch = createEventDispatcher();
  
  let groupSettings = {
    rows: ['subject', 'userField', 'article'],
    columns: [],
    values: ['drr', 'expense', 'revenue', 'orders', 'ctr', 'cpc']
  };
  
  let rowGroupOptions = [
    { value: 'subject', label: 'Предмет' },
    { value: 'userField', label: 'Пользовательское поле' },
    { value: 'article', label: 'Артикул' }
  ];
  
  let valueOptions = [
    { value: 'drr', label: 'ДРР' },
    { value: 'expense', label: 'Расход' },
    { value: 'revenue', label: 'Выручка' },
    { value: 'orders', label: 'Заказы' },
    { value: 'ctr', label: 'CTR' },
    { value: 'cpc', label: 'CPC/CPM' }
  ];
  
  let userFields = [
    { id: 'good-drr', name: 'Хороший ДРР' },
    { id: 'bad-drr', name: 'Плохой ДРР' },
    { id: 'new-items', name: 'Новинки' }
  ];
  
  function handleGroupChange() {
    dispatch('groupChange', { ...groupSettings });
  }
  
  function addUserField() {
    // Логика добавления пользовательского поля
    console.log('Add user field');
  }
  
  function saveView() {
    // Логика сохранения представления
    console.log('Save view');
  }
</script>

<div class="pivot-group-panel">
  <div class="panel-header">
    <h3>Группировка</h3>
  </div>
  
  <div class="group-settings">
    <div class="setting-group">
      <label for="rows-control" class="setting-label">Rows (строки)</label>
      <div class="row-groups" id="rows-control">
        {#each groupSettings.rows as group, i}
          <select 
            class="group-select"
            on:change={(e) => {
              groupSettings.rows[i] = e.target.value;
              handleGroupChange();
            }}
          >
            {#each rowGroupOptions as option}
              <option value={option.value} selected={option.value === group}>
                {option.label}
              </option>
            {/each}
          </select>
        {/each}
      </div>
    </div>
    
    <div class="setting-group">
      <label for="values-control" class="setting-label">Values (метрики)</label>
      <div class="value-filters" id="values-control">
        {#each valueOptions as option}
          <label class="checkbox-label">
            <input 
              type="checkbox" 
              checked={groupSettings.values.includes(option.value)}
              on:change={(e) => {
                if (e.target.checked) {
                  groupSettings.values = [...groupSettings.values, option.value];
                } else {
                  groupSettings.values = groupSettings.values.filter(v => v !== option.value);
                }
                handleGroupChange();
              }}
            />
            {option.label}
          </label>
        {/each}
      </div>
    </div>
  </div>
  
  <div class="user-fields-manager">
    <div class="manager-header">
      <h4>User Fields Manager</h4>
    </div>
    
    <div class="user-fields-list">
      {#each userFields as field}
        <div class="user-field-item">
          <span>{field.name}</span>
        </div>
      {/each}
    </div>
    
    <div class="manager-actions">
      <button class="secondary-button" on:click={addUserField}>
        Создать поле
      </button>
      <button class="secondary-button" on:click={saveView}>
        Сохранить представление
      </button>
    </div>
  </div>
</div>

<style>
  .pivot-group-panel {
    padding: 20px;
    background-color: #f5f5f7;
    border-radius: 12px;
    box-shadow: 0 2px 10px rgba(0, 0, 0, 0.05);
    margin-bottom: 20px;
  }
  
  .panel-header h3 {
    margin: 0 0 16px 0;
    font-size: 18px;
    font-weight: 600;
    color: #000000;
  }
  
  .group-settings {
    display: flex;
    gap: 32px;
    margin-bottom: 20px;
  }
  
  .setting-group {
    flex: 1;
  }
  
  .setting-label {
    display: block;
    margin-bottom: 8px;
    font-size: 14px;
    font-weight: 500;
    color: #86868b;
  }
  
  .row-groups {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  
  .group-select {
    padding: 8px 12px;
    border: 1px solid #d2d2d7;
    border-radius: 6px;
    background-color: white;
    font-size: 14px;
    color: #000000;
    width: 200px;
  }
  
  .value-filters {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 8px;
  }
  
  .checkbox-label {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 14px;
    color: #000000;
  }
  
  .user-fields-manager {
    border-top: 1px solid #d2d2d7;
    padding-top: 20px;
  }
  
  .manager-header h4 {
    margin: 0 0 12px 0;
    font-size: 16px;
    font-weight: 600;
    color: #000000;
  }
  
  .user-fields-list {
    margin-bottom: 16px;
  }
  
  .user-field-item {
    padding: 8px 12px;
    background-color: white;
    border: 1px solid #d2d2d7;
    border-radius: 6px;
    margin-bottom: 8px;
    font-size: 14px;
  }
  
  .manager-actions {
    display: flex;
    gap: 12px;
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