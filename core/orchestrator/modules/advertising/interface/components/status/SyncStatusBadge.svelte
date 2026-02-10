<script>
  export let status = 'off'; // 'on', 'off', 'syncing', 'error'
  export let size = 'medium'; // 'small', 'medium', 'large'
</script>

<div class="sync-status-badge" class:{status} class:{size}>
  {#if status === 'on'}
    <div class="status-icon">●</div>
  {:else if status === 'off'}
    <div class="status-icon">○</div>
  {:else if status === 'syncing'}
    <div class="status-icon syncing">↻</div>
  {:else if status === 'error'}
    <div class="status-icon">⚠</div>
  {/if}
</div>

<style>
  .sync-status-badge {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    border-radius: 50%;
    background-color: #f5f5f7;
  }
  
  .sync-status-badge.small {
    width: 16px;
    height: 16px;
    font-size: 10px;
  }
  
  .sync-status-badge.medium {
    width: 20px;
    height: 20px;
    font-size: 12px;
  }
  
  .sync-status-badge.large {
    width: 24px;
    height: 24px;
    font-size: 14px;
  }
  
  .status-icon {
    display: flex;
    align-items: center;
    justify-content: center;
  }
  
  .sync-status-badge.on {
    background-color: #e8f1ff;
    color: #0071e3;
  }
  
  .sync-status-badge.off {
    background-color: #f5f5f7;
    color: #86868b;
  }
  
  .sync-status-badge.syncing {
    background-color: #e8f1ff;
    color: #0071e3;
    animation: spin 1s linear infinite;
  }
  
  .sync-status-badge.error {
    background-color: #ffe8e8;
    color: #ff453a;
  }
  
  @keyframes spin {
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
  }
</style>