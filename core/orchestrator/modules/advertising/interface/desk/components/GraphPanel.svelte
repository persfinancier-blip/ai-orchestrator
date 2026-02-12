<script lang="ts">
  import { onDestroy, onMount, createEventDispatcher } from 'svelte';
  import { DataSet, Network } from 'vis-network/standalone';
  import type { LodMode, GraphNode, GraphEdge } from '../data/mockGraph';
  import { applyLod, nodeColors } from '../data/mockGraph';

  export let allNodes: GraphNode[] = [];
  export let allEdges: GraphEdge[] = [];
  export let mode: LodMode = 'SKU';

  const dispatch = createEventDispatcher();
  let container: HTMLDivElement;
  let network: Network;
  let query = '';
  let view = { nodes: [] as GraphNode[], edges: [] as GraphEdge[] };

  $: view = applyLod(allNodes, allEdges, mode);
  $: renderGraph();

  function renderGraph(): void {
    if (!container) return;
    const nodes = new DataSet(
      view.nodes.map((n) => ({
        id: n.id,
        label: n.label,
        color: nodeColors[n.type],
        shape: n.type === 'kpi' ? 'diamond' : 'dot',
        size: n.type === 'sku' ? 10 : 14,
        font: { color: n.type === 'ad_target' || n.type === 'entry_paid' ? '#f8fafc' : '#1e293b', size: 12 },
      }))
    );
    const edges = new DataSet(
      view.edges.map((e) => ({ id: e.id, from: e.from, to: e.to, arrows: 'to', width: Math.min(6, 0.8 + e.weight), color: { color: '#94a3b8' } }))
    );

    if (!network) {
      network = new Network(container, { nodes, edges }, {
        physics: { stabilization: false, barnesHut: { springLength: 90 } },
        interaction: { hover: true, multiselect: false, zoomView: true, dragView: true },
      });
      network.on('click', (params) => {
        if (!params.nodes?.length) return;
        const node = view.nodes.find((n) => n.id === params.nodes[0]);
        if (node) dispatch('selectNode', node);
      });
      network.on('hoverNode', (p) => {
        const connected = network.getConnectedNodes(p.node);
        network.selectNodes([p.node, ...connected]);
      });
      network.on('blurNode', () => network.unselectAll());
    } else {
      network.setData({ nodes, edges });
    }
  }

  function focusByQuery(): void {
    const found = view.nodes.find((n) => `${n.id} ${n.label} ${n.campaignId ?? ''}`.toLowerCase().includes(query.toLowerCase()));
    if (!found || !network) return;
    network.selectNodes([found.id]);
    network.focus(found.id, { scale: 1.2, animation: true });
    dispatch('selectNode', found);
  }

  function onKeydown(e: KeyboardEvent): void {
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
      e.preventDefault();
      const input = document.getElementById('desk-node-search') as HTMLInputElement | null;
      input?.focus();
    }
    if (e.code === 'Space' && network) network.setOptions({ interaction: { dragView: true } });
  }
  function onKeyup(e: KeyboardEvent): void {
    if (e.code === 'Space' && network) network.setOptions({ interaction: { dragView: true } });
  }

  onMount(() => {
    window.addEventListener('keydown', onKeydown);
    window.addEventListener('keyup', onKeyup);
  });
  onDestroy(() => {
    window.removeEventListener('keydown', onKeydown);
    window.removeEventListener('keyup', onKeyup);
    network?.destroy();
  });
</script>

<section class="panel graph-panel">
  <div class="bar">
    <div>Nodes: {view.nodes.length} | Edges: {view.edges.length} | Mode: {mode}</div>
    <div class="controls">
      <input id="desk-node-search" bind:value={query} on:change={focusByQuery} placeholder="Search SKU/keyword/campaign (Ctrl+K)" />
      <button on:click={focusByQuery}>Focus</button>
    </div>
  </div>
  <div bind:this={container} class="graph-canvas" />
  <div class="legend">
    <span><i style="background:#6ecf6a"></i>SKU</span>
    <span><i style="background:#b7dcff"></i>Типология</span>
    <span><i style="background:#1f4f8b"></i>Таргет</span>
    <span><i style="background:#aeb4bd"></i>Орг вход</span>
    <span><i style="background:#4d5663"></i>Рекл вход</span>
    <span><i style="background:#ffd666"></i>KPI</span>
  </div>
</section>

<style>
  .graph-panel { display:flex; flex-direction:column; gap:10px; }
  .bar { display:flex; justify-content:space-between; font-size:12px; color:#475569; }
  .controls { display:flex; gap:8px; }
  input { border:1px solid #e2e8f0; border-radius:10px; padding:6px 10px; width:260px; }
  button { border:1px solid #dbe4f0; background:#fff; border-radius:10px; padding:6px 10px; }
  .graph-canvas { height: 380px; border:1px solid #e2e8f0; border-radius:16px; background:#fbfdff; }
  .legend { display:flex; gap:14px; flex-wrap:wrap; font-size:12px; color:#475569; }
  .legend i { width:10px; height:10px; border-radius:50%; display:inline-block; margin-right:6px; }
</style>
