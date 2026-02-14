<script lang="ts">
  import type { TooltipState } from '../types';
  export let tooltip: TooltipState;
</script>

{#if tooltip.visible}
  <div class="tooltip" style={`left:${tooltip.x}px;top:${tooltip.y}px`}>
    {#each tooltip.lines as line}<div>{line}</div>{/each}
  </div>
{/if}
