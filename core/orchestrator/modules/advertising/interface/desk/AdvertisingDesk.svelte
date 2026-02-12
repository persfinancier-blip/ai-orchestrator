<script lang="ts">
  import GraphPanel from './components/GraphPanel.svelte';
  import WorkflowCanvas from './components/WorkflowCanvas.svelte';
  import InspectorPanel from './components/InspectorPanel.svelte';
  import type { GraphNode, LodMode } from './data/mockGraph';
  import { generateMockGraph } from './data/mockGraph';
  import { initialWorkflow, generatePivotRows, newLog, type CommandLogItem } from './data/mockWorkflow';

  const graph = generateMockGraph(2000);
  let selectedNode: GraphNode | null = null;
  let mode: LodMode = 'SKU';
  let workflow = structuredClone(initialWorkflow);
  let commandLog: CommandLogItem[] = [];
  let previewRows = generatePivotRows(20);
  let toast = '';

  function onSelectNode(e: CustomEvent<GraphNode>): void {
    selectedNode = e.detail;
  }

  function addToCloud(node: GraphNode): void {
    workflow.filters = [...workflow.filters, { id: String(Date.now()), field: 'skuId', operator: 'IN', value: node.skuId ?? node.id }];
    toast = `${node.label} added to Data Cloud`;
    setTimeout(() => (toast = ''), 1400);
  }

  function preview(): void {
    previewRows = generatePivotRows(20);
  }

  function runFlow(): void {
    const affected = 120 + Math.round(Math.random() * 900);
    commandLog = [newLog(workflow.workflowName, workflow.action, affected), ...commandLog].slice(0, 10);
    toast = 'Simulated run created';
    setTimeout(() => (toast = ''), 1600);
  }
</script>

<main class="desk-root">
  <header>
    <h1>Advertising Control Desk</h1>
    <div class="mode">
      <label>Level of detail</label>
      <select bind:value={mode}>
        <option>SKU</option>
        <option>Предмет</option>
        <option>Campaign ID</option>
        <option>Entry points</option>
      </select>
    </div>
  </header>

  <section class="layout">
    <div class="left">
      <GraphPanel allNodes={graph.nodes} allEdges={graph.edges} {mode} on:selectNode={onSelectNode} />
      <WorkflowCanvas state={workflow} {commandLog} {previewRows} onPreview={preview} onRun={runFlow} />
    </div>
    <InspectorPanel {selectedNode} onAddToDataCloud={addToCloud} />
  </section>

  {#if toast}<div class="toast">{toast}</div>{/if}
</main>

<style>
  :global(body){margin:0;font-family:Inter,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f4f6fa;color:#0f172a;}
  .desk-root{padding:18px;}
  header{display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;}
  h1{margin:0;font-size:24px;font-weight:650;letter-spacing:.01em;}
  .mode{display:flex;gap:8px;align-items:center;color:#64748b;font-size:12px;}
  .mode select{border:1px solid #dbe4f0;border-radius:10px;padding:6px 10px;background:#fff;}
  .layout{display:grid;grid-template-columns:1fr 320px;gap:14px;}
  .left{display:flex;flex-direction:column;gap:14px;}
  :global(.panel){background:#fff;border:1px solid #e8edf5;border-radius:18px;padding:12px;box-shadow:0 10px 30px rgba(15,23,42,.05);transition:all .2s ease;}
  .toast{position:fixed;right:20px;bottom:18px;background:#0f172a;color:#fff;padding:10px 14px;border-radius:12px;box-shadow:0 10px 25px rgba(0,0,0,.15);animation:fade .25s ease;}
  @keyframes fade{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
</style>
