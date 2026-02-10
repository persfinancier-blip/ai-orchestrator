<script>
  export let message = '';
  export let show = false;
  export let position = 'top'; // 'top', 'bottom', 'left', 'right'
</script>

{#if show}
  <div class="error-tooltip" class:{position}>
    <div class="tooltip-content">
      <div class="error-icon">⚠</div>
      <div class="error-message">{message}</div>
    </div>
    <div class="tooltip-arrow"></div>
  </div>
{/if}

<style>
  .error-tooltip {
    position: relative;
    display: inline-block;
  }
  
  .tooltip-content {
    position: absolute;
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 8px 12px;
    background-color: #ff453a;
    color: white;
    border-radius: 8px;
    font-size: 12px;
    font-weight: 500;
    box-shadow: 0 4px 12px rgba(255, 69, 58, 0.3);
    z-index: 1000;
    min-width: 200px;
    max-width: 300px;
  }
  
  .error-tooltip.top .tooltip-content {
    bottom: 100%;
    left: 50%;
    transform: translateX(-50%);
    margin-bottom: 8px;
  }
  
  .error-tooltip.bottom .tooltip-content {
    top: 100%;
    left: 50%;
    transform: translateX(-50%);
    margin-top: 8px;
  }
  
  .error-tooltip.left .tooltip-content {
    right: 100%;
    top: 50%;
    transform: translateY(-50%);
    margin-right: 8px;
  }
  
  .error-tooltip.right .tooltip-content {
    left: 100%;
    top: 50%;
    transform: translateY(-50%);
    margin-left: 8px;
  }
  
  .error-icon {
    font-size: 16px;
  }
  
  .tooltip-arrow {
    position: absolute;
    width: 0;
    height: 0;
    border: 6px solid transparent;
  }
  
  .error-tooltip.top .tooltip-arrow {
    top: 100%;
    left: 50%;
    transform: translateX(-50%);
    border-top-color: #ff453a;
  }
  
  .error-tooltip.bottom .tooltip-arrow {
    bottom: 100%;
    left: 50%;
    transform: translateX(-50%);
    border-bottom-color: #ff453a;
  }
  
  .error-tooltip.left .tooltip-arrow {
    left: 100%;
    top: 50%;
    transform: translateY(-50%);
    border-left-color: #ff453a;
  }
  
  .error-tooltip.right .tooltip-arrow {
    right: 100%;
    top: 50%;
    transform: translateY(-50%);
    border-right-color: #ff453a;
  }
</style>