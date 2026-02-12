<script lang="ts">
  import type { GraphNode } from '../data/mockGraph';
  export let selectedNode: GraphNode | null = null;
  export let onAddToDataCloud: (node: GraphNode) => void = () => {};
</script>

<aside class="panel inspector">
  {#if selectedNode}
    <h3>{selectedNode.label}</h3>
    <p class="muted">type: {selectedNode.type} · id: {selectedNode.id}</p>
    <div class="grid">
      <div>CTR <strong>{selectedNode.metrics.ctr}%</strong></div>
      <div>CR2 <strong>{selectedNode.metrics.cr2}%</strong></div>
      <div>Rating <strong>{selectedNode.metrics.rating}</strong></div>
      <div>Returns <strong>{selectedNode.metrics.returnsPct}%</strong></div>
      <div>Spend <strong>{selectedNode.metrics.spend}</strong></div>
      <div>Clicks <strong>{selectedNode.metrics.clicks}</strong></div>
      <div>Orders <strong>{selectedNode.metrics.orders}</strong></div>
      <div>DRR <strong>{selectedNode.metrics.drr}%</strong></div>
      <div>ROI <strong>{selectedNode.metrics.roi}</strong></div>
    </div>
    <button on:click={() => onAddToDataCloud(selectedNode)}>Add to Data Cloud</button>
  {:else}
    <h3>Inspector</h3>
    <p class="muted">Выберите узел на графе для параметров и метрик.</p>
    <ul>
      <li>Green — SKU</li><li>Light blue — client typology</li><li>Dark blue — ad target</li>
      <li>Gray — organic entry</li><li>Dark gray — paid entry</li><li>Yellow — KPI</li>
    </ul>
  {/if}
</aside>

<style>
  .inspector { min-height: 380px; }
  .muted { color:#64748b; font-size:12px; }
  .grid { display:grid; grid-template-columns:1fr 1fr; gap:8px; font-size:12px; margin:12px 0; }
  button { border:1px solid #cbd5e1; background:#fff; border-radius:10px; padding:8px 10px; }
  ul { color:#475569; font-size:12px; line-height:1.6; padding-left:16px; }
</style>
