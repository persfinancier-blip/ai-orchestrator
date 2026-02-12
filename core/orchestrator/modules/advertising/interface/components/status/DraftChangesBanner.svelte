<script>
  import { createEventDispatcher } from 'svelte';
  
  export let draftChanges = [];
  
  const dispatch = createEventDispatcher();
  
  function handleApply() {
    dispatch('apply');
  }
  
  function handleCancel() {
    dispatch('cancel');
  }
</script>

<div class="draft-changes-banner">
  <div class="banner-content">
    <div class="banner-icon">✏️</div>
    <div class="banner-text">
      <div class="title">Есть несохраненные изменения</div>
      <div class="description">
        {draftChanges.length} {draftChanges.length === 1 ? 'изменение' : draftChanges.length < 5 ? 'изменения' : 'изменений'} в черновике
      </div>
    </div>
    <div class="banner-actions">
      <button class="secondary-button" on:click={handleCancel}>
        Отменить
      </button>
      <button class="primary-button" on:click={handleApply}>
        Применить изменения
      </button>
    </div>
  </div>
</div>

<style>
  .draft-changes-banner {
    padding: 16px;
    background-color: #fff9e6;
    border: 1px solid #ffd60a;
    border-radius: 12px;
    margin-bottom: 20px;
    box-shadow: 0 2px 10px rgba(255, 214, 10, 0.1);
  }
  
  .banner-content {
    display: flex;
    align-items: center;
    gap: 16px;
  }
  
  .banner-icon {
    font-size: 24px;
  }
  
  .banner-text {
    flex: 1;
  }
  
  .title {
    font-size: 16px;
    font-weight: 600;
    color: #000000;
    margin-bottom: 4px;
  }
  
  .description {
    font-size: 14px;
    color: #86868b;
  }
  
  .banner-actions {
    display: flex;
    gap: 12px;
  }
  
  .primary-button {
    padding: 8px 16px;
    background-color: #0071e3;
    color: white;
    border: none;
    border-radius: 8px;
    font-size: 14px;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.2s ease-out;
  }
  
  .primary-button:hover {
    background-color: #0077ed;
    box-shadow: 0 4px 12px rgba(0, 113, 227, 0.2);
  }
  
  .secondary-button {
    padding: 8px 16px;
    background-color: white;
    color: #0071e3;
    border: 1px solid #0071e3;
    border-radius: 8px;
    font-size: 14px;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.2s ease-out;
  }
  
  .secondary-button:hover {
    background-color: #e8f1ff;
  }
</style>