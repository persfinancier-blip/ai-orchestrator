<script lang="ts">
  import { onMount } from 'svelte';
  import {
    cohortSources,
    datamartSources,
    graphSources,
    toolPorts,
    tools,
    type SourceItem,
    type ToolItem,
    type ToolType,
    type SourceType,
  } from '../data/workflowEditor';

  type Tab = 'graph' | 'datamart' | 'cohort';

  type DataNodeConfig = {
    name: string;
    sourceType: SourceType;
    datasetId: string;
    schema: string[];
    size: number;
    preview: Array<Record<string, string | number>>;
    dynamic?: boolean;
    definition?: unknown;
    generated?: boolean;
  };

  type ToolNodeConfig = {
    name: string;
    toolType: ToolType;
    settings: Record<string, string>;
  };

  type WorkflowNode = {
    id: string;
    type: 'data' | 'tool';
    x: number;
    y: number;
    config: DataNodeConfig | ToolNodeConfig;
  };

  type WorkflowEdge = {
    id: string;
    from: string;
    to: string;
    fromPort: string;
    toPort: string;
  };

  let activeTab: Tab = 'graph';
  let nodes: WorkflowNode[] = [];
  let edges: WorkflowEdge[] = [];
  let selectedNodeId = '';
  let linkFrom: { nodeId: string; port: string } | null = null;
  let banner = '';

  let canvasEl: HTMLDivElement;

  let panX = 0;
  let panY = 0;
  let zoom = 1;

  let isPanning = false;
  let panStartX = 0;
  let panStartY = 0;

  let dragNode: { id: string; offsetX: number; offsetY: number } | null = null;

  $: selectedNode = nodes.find((n) => n.id === selectedNodeId) ?? null;
  $: persist();

  function persist(): void {
    // local storage disabled
  }

  function restore(): void {
    nodes = [];
    edges = [];
    panX = 0;
    panY = 0;
    zoom = 1;
  }

  function sourceList(): SourceItem[] {
    if (activeTab === 'graph') return graphSources;
    if (activeTab === 'datamart') return datamartSources;
    return cohortSources;
  }

  function newId(prefix: string): string {
    return `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
  }

  function makeNodeFromSource(item: SourceItem, x: number, y: number): WorkflowNode {
    return {
      id: newId('node'),
      type: 'data',
      x,
      y,
      config: {
        name: item.name,
        sourceType: item.sourceType,
        datasetId: item.datasetId,
        schema: item.schema,
        size: item.size,
        preview: item.preview,
        dynamic: item.dynamic,
        definition: item.definition,
        generated: false,
      },
    };
  }

  function makeNodeFromTool(item: ToolItem, x: number, y: number): WorkflowNode {
    return {
      id: newId('node'),
      type: 'tool',
      x,
      y,
      config: {
        name: item.name,
        toolType: item.toolType,
        settings: {
          condition: 'revenue > 10000',
          key: 'sku',
          groupBy: 'subject',
        },
      },
    };
  }

  function nodeName(node: WorkflowNode): string {
    return node.config.name;
  }

  function dataCfg(node: WorkflowNode): DataNodeConfig {
    return node.config as DataNodeConfig;
  }

  function toolCfg(node: WorkflowNode): ToolNodeConfig {
    return node.config as ToolNodeConfig;
  }

  function onDragStartSource(event: DragEvent, item: SourceItem): void {
    event.dataTransfer?.setData('application/x-workflow-source', JSON.stringify(item));
  }

  function onDragStartTool(event: DragEvent, item: ToolItem): void {
    event.dataTransfer?.setData('application/x-workflow-tool', JSON.stringify(item));
  }

  function toCanvasPoint(clientX: number, clientY: number): { x: number; y: number } {
    const rect = canvasEl.getBoundingClientRect();
    return {
      x: (clientX - rect.left - panX) / zoom,
      y: (clientY - rect.top - panY) / zoom,
    };
  }

  function onDrop(event: DragEvent): void {
    event.preventDefault();
    banner = '';
    const p = toCanvasPoint(event.clientX, event.clientY);

    const sourceRaw = event.dataTransfer?.getData('application/x-workflow-source');
    if (sourceRaw) {
      const item = JSON.parse(sourceRaw) as SourceItem;
      nodes = [...nodes, makeNodeFromSource(item, p.x, p.y)];
      return;
    }

    const toolRaw = event.dataTransfer?.getData('application/x-workflow-tool');
    if (toolRaw) {
      const item = JSON.parse(toolRaw) as ToolItem;
      nodes = [...nodes, makeNodeFromTool(item, p.x, p.y)];
    }
  }

  function onDragOver(event: DragEvent): void {
    event.preventDefault();
  }

  function onWheel(event: WheelEvent): void {
    event.preventDefault();
    const next = zoom - Math.sign(event.deltaY) * 0.08;
    zoom = Math.max(0.5, Math.min(1.8, next));
  }

  function startPan(event: MouseEvent): void {
    if ((event.target as HTMLElement).closest('.node')) return;
    isPanning = true;
    panStartX = event.clientX - panX;
    panStartY = event.clientY - panY;
  }

  function stopAll(): void {
    isPanning = false;
    dragNode = null;
  }

  function onMouseMove(event: MouseEvent): void {
    if (dragNode) {
      const p = toCanvasPoint(event.clientX, event.clientY);
      nodes = nodes.map((n) =>
        n.id === dragNode?.id ? { ...n, x: p.x - dragNode.offsetX, y: p.y - dragNode.offsetY } : n
      );
      return;
    }
    if (!isPanning) return;
    panX = event.clientX - panStartX;
    panY = event.clientY - panStartY;
  }

  function selectNode(id: string): void {
    selectedNodeId = id;
    linkFrom = null;
  }

  function deleteNode(id: string): void {
    nodes = nodes.filter((n) => n.id !== id);
    edges = edges.filter((e) => e.from !== id && e.to !== id);
    if (selectedNodeId === id) selectedNodeId = '';
    if (linkFrom?.nodeId === id) linkFrom = null;
  }

  function deleteEdge(id: string): void {
    edges = edges.filter((e) => e.id !== id);
  }

  function startNodeDrag(event: MouseEvent, node: WorkflowNode): void {
    const p = toCanvasPoint(event.clientX, event.clientY);
    dragNode = { id: node.id, offsetX: p.x - node.x, offsetY: p.y - node.y };
  }

  function inputs(node: WorkflowNode): string[] {
    if (node.type === 'tool') return toolPorts[toolCfg(node).toolType].inputs;
    return dataCfg(node).generated ? ['in'] : [];
  }

  function outputs(node: WorkflowNode): string[] {
    if (node.type === 'tool') return toolPorts[toolCfg(node).toolType].outputs;
    return ['out'];
  }

  function canConnect(fromId: string, fromPort: string, toId: string, toPort: string): boolean {
    if (fromId === toId) return false;
    const fromNode = nodes.find((n) => n.id === fromId);
    const toNode = nodes.find((n) => n.id === toId);
    if (!fromNode || !toNode) return false;

    if (!outputs(fromNode).includes(fromPort)) return false;
    if (!inputs(toNode).includes(toPort)) return false;

    if (toNode.type === 'tool') {
      if (edges.some((e) => e.to === toId && e.toPort === toPort)) return false;
    }
    if (toNode.type === 'data') {
      if (edges.some((e) => e.to === toId && e.toPort === 'in')) return false;
    }

    return true;
  }

  function connectFrom(nodeId: string, port: string): void {
    linkFrom = { nodeId, port };
    banner = '';
  }

  function connectTo(nodeId: string, port: string): void {
    if (!linkFrom) return;
    if (!canConnect(linkFrom.nodeId, linkFrom.port, nodeId, port)) {
      banner = 'Нельзя соединить';
      linkFrom = null;
      return;
    }
    edges = [
      ...edges,
      {
        id: newId('edge'),
        from: linkFrom.nodeId,
        to: nodeId,
        fromPort: linkFrom.port,
        toPort: port,
      },
    ];
    linkFrom = null;
  }

  function center(node: WorkflowNode): { x: number; y: number } {
    return { x: node.x + 120, y: node.y + 50 };
  }

  function updateToolSetting(nodeId: string, key: string, value: string): void {
    nodes = nodes.map((n) => {
      if (n.id !== nodeId || n.type !== 'tool') return n;
      const cfg = toolCfg(n);
      return { ...n, config: { ...cfg, settings: { ...cfg.settings, [key]: value } } };
    });
  }

  function runTool(node: WorkflowNode): void {
    if (node.type !== 'tool') return;

    const incoming = edges.filter((e) => e.to === node.id);
    if (incoming.length === 0) {
      banner = 'Нельзя выполнить инструмент без входных данных';
      return;
    }

    const outPorts = outputs(node);
    if (outPorts.length === 0) {
      banner = 'Инструмент без выходов (Export)';
      return;
    }

    const datasetId = `result:${Date.now().toString(36)}`;
    const resultNode: WorkflowNode = {
      id: newId('node'),
      type: 'data',
      x: node.x + 300,
      y: node.y + 40,
      config: {
        name: `Результат: ${nodeName(node)}`,
        sourceType: 'datamart',
        datasetId,
        schema: ['entity', 'score', 'revenue', 'spend'],
        size: 50 + Math.round(Math.random() * 400),
        preview: Array.from({ length: 10 }).map((_, idx) => ({
          entity: `row-${idx + 1}`,
          score: Number((Math.random() * 100).toFixed(2)),
          revenue: Math.round(2000 + Math.random() * 18000),
          spend: Math.round(700 + Math.random() * 9000),
        })),
        generated: true,
      },
    };

    const edge: WorkflowEdge = {
      id: newId('edge'),
      from: node.id,
      to: resultNode.id,
      fromPort: outPorts[0],
      toPort: 'in',
    };

    nodes = [...nodes, resultNode];
    edges = [...edges, edge];
    banner = `Создан datasetId: ${datasetId}`;
  }

  onMount(restore);
</script>

<section class="panel workflow-v2">
  <h3>Workflow-редактор</h3>

  <div class="workspace">
    <aside class="sources">
      <h4>Источники данных</h4>

      <div class="tabs">
        <button class:active={activeTab === 'graph'} on:click={() => (activeTab = 'graph')}>Из графа</button>
        <button class:active={activeTab === 'datamart'} on:click={() => (activeTab = 'datamart')}>Из витрины</button>
        <button class:active={activeTab === 'cohort'} on:click={() => (activeTab = 'cohort')}>Когорты</button>
      </div>

      <div class="items">
        {#each sourceList() as item}
          <button
            draggable="true"
            on:dragstart={(e) => onDragStartSource(e, item)}
            class="item source {item.sourceType === 'cohort' ? 'cohort' : ''}"
            title="Перетащите на canvas"
          >
            <strong>{item.name}</strong>
            <span>{item.sourceType} · {item.size}</span>
          </button>
        {/each}
      </div>
    </aside>

    <div
      class="canvas-wrap"
      bind:this={canvasEl}
      on:drop={onDrop}
      on:dragover={onDragOver}
      on:wheel={onWheel}
      on:mousedown={startPan}
      on:mousemove={onMouseMove}
      on:mouseup={stopAll}
      on:mouseleave={stopAll}
    >
      <div class="canvas" style={`transform: translate(${panX}px, ${panY}px) scale(${zoom});`}>
        <svg class="edges" width="3000" height="2000">
          {#each edges as edge (edge.id)}
            {@const fromNode = nodes.find((n) => n.id === edge.from)}
            {@const toNode = nodes.find((n) => n.id === edge.to)}
            {#if fromNode && toNode}
              {@const a = center(fromNode)}
              {@const b = center(toNode)}
              <path
                d={`M ${a.x} ${a.y} C ${a.x + 90} ${a.y}, ${b.x - 90} ${b.y}, ${b.x} ${b.y}`}
                stroke="#64748b"
                fill="none"
                stroke-width="2"
              />
              <g>
                <rect
                  x={(a.x + b.x) / 2 - 10}
                  y={(a.y + b.y) / 2 - 20}
                  width="20"
                  height="20"
                  rx="6"
                  fill="#fff"
                  stroke="#e2e8f0"
                />
                <text
                  x={(a.x + b.x) / 2}
                  y={(a.y + b.y) / 2 - 6}
                  text-anchor="middle"
                  class="edge-x"
                  on:click={() => deleteEdge(edge.id)}
                >
                  ×
                </text>
              </g>
            {/if}
          {/each}
        </svg>

        {#each nodes as node (node.id)}
          <div
            class="node {node.type} {selectedNodeId === node.id ? 'selected' : ''}"
            style={`left:${node.x}px;top:${node.y}px;`}
            on:mousedown={(e) => startNodeDrag(e, node)}
            on:click={() => selectNode(node.id)}
          >
            <div class="node-head">
              <strong>{nodeName(node)}</strong>
              <button on:click|stopPropagation={() => deleteNode(node.id)}>×</button>
            </div>

            {#if node.type === 'data'}
              <div class="node-meta">{dataCfg(node).sourceType} · {dataCfg(node).datasetId}</div>
            {:else}
              <div class="node-meta">Инструмент · {toolCfg(node).toolType}</div>
            {/if}

            <div class="ports">
              <div class="ins">
                {#each inputs(node) as port (port)}
                  <button class="port in" on:click|stopPropagation={() => connectTo(node.id, port)}>{port}</button>
                {/each}
              </div>
              <div class="outs">
                {#each outputs(node) as port (port)}
                  <button
                    class="port out {linkFrom?.nodeId === node.id ? 'active' : ''}"
                    on:click|stopPropagation={() => connectFrom(node.id, port)}
                  >
                    {port}
                  </button>
                {/each}
              </div>
            </div>

            {#if node.type === 'data' && dataCfg(node).sourceType === 'cohort'}
              <div class="cohort-badge">КОГОРТА</div>
            {/if}
          </div>
        {/each}
      </div>

      {#if banner}
        <div class="banner">{banner}</div>
      {/if}
    </div>

    <aside class="tools">
      <h4>Инструменты</h4>

      <div class="items">
        {#each tools as tool}
          <button
            draggable="true"
            on:dragstart={(e) => onDragStartTool(e, tool)}
            class="item tool"
            title="Перетащите на canvas"
          >
            <strong>{tool.name}</strong>
            <span>{tool.toolType}</span>
          </button>
        {/each}
      </div>

      <section class="props">
        <h5>Свойства ноды</h5>

        {#if selectedNode}
          {#if selectedNode.type === 'data'}
            <div><b>name:</b> {dataCfg(selectedNode).name}</div>
            <div><b>sourceType:</b> {dataCfg(selectedNode).sourceType}</div>
            <div><b>schema:</b> {dataCfg(selectedNode).schema.join(', ')}</div>
            <div><b>size:</b> {dataCfg(selectedNode).size}</div>

            {#if dataCfg(selectedNode).sourceType === 'cohort'}
              <div><b>dynamic:</b> {String(dataCfg(selectedNode).dynamic ?? false)}</div>
              <div class="definition">{JSON.stringify(dataCfg(selectedNode).definition ?? {}, null, 2)}</div>
            {/if}

            {#if (dataCfg(selectedNode).preview?.length ?? 0) > 0}
              <table>
                <thead>
                  <tr>
                    {#each Object.keys(dataCfg(selectedNode).preview[0]) as k (k)}
                      <th>{k}</th>
                    {/each}
                  </tr>
                </thead>
                <tbody>
                  {#each dataCfg(selectedNode).preview as row, i (i)}
                    <tr>
                      {#each Object.values(row) as v, j (j)}
                        <td>{v}</td>
                      {/each}
                    </tr>
                  {/each}
                </tbody>
              </table>
            {/if}
          {:else}
            <div><b>name:</b> {toolCfg(selectedNode).name}</div>
            <div><b>toolType:</b> {toolCfg(selectedNode).toolType}</div>

            <label>
              Условие
              <input
                value={toolCfg(selectedNode).settings.condition ?? ''}
                on:input={(e) => updateToolSetting(selectedNode.id, 'condition', e.currentTarget.value)}
              />
            </label>

            <label>
              Ключ
              <input
                value={toolCfg(selectedNode).settings.key ?? ''}
                on:input={(e) => updateToolSetting(selectedNode.id, 'key', e.currentTarget.value)}
              />
            </label>

            <label>
              Group by
              <input
                value={toolCfg(selectedNode).settings.groupBy ?? ''}
                on:input={(e) => updateToolSetting(selectedNode.id, 'groupBy', e.currentTarget.value)}
              />
            </label>

            <button class="run" on:click={() => runTool(selectedNode)}>Run</button>
          {/if}
        {:else}
          <div class="empty">Выберите ноду на canvas.</div>
        {/if}
      </section>
    </aside>
  </div>
</section>

<style>
  .workflow-v2 {
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  .workspace {
    display: grid;
    grid-template-columns: 260px minmax(0, 1fr) 320px;
    gap: 10px;
    min-height: 720px;
  }

  .sources,
  .tools {
    border: 1px solid #e2e8f0;
    border-radius: 14px;
    padding: 10px;
    background: #f8fbff;
    display: flex;
    flex-direction: column;
    gap: 8px;
  }

  .tabs {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 6px;
  }
  .tabs button {
    border: 1px solid #dbe4f0;
    background: #fff;
    border-radius: 9px;
    font-size: 11px;
    padding: 6px;
    cursor: pointer;
  }
  .tabs button.active {
    background: #0f172a;
    color: #fff;
  }

  .items {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  .item {
    text-align: left;
    border: 1px solid #dbe4f0;
    background: #fff;
    border-radius: 10px;
    padding: 8px;
    cursor: grab;
    display: flex;
    flex-direction: column;
    gap: 3px;
  }
  .item span {
    color: #64748b;
    font-size: 11px;
  }
  .item.cohort {
    border-color: #c4b5fd;
    background: #f5f3ff;
  }

  .canvas-wrap {
    position: relative;
    border: 1px solid #dbe4f0;
    border-radius: 14px;
    overflow: hidden;
    background: #fff;
  }
  .canvas {
    position: relative;
    width: 3000px;
    height: 2000px;
    transform-origin: 0 0;
    background-image: linear-gradient(#f1f5f9 1px, transparent 1px),
      linear-gradient(90deg, #f1f5f9 1px, transparent 1px);
    background-size: 24px 24px;
  }

  .edges {
    position: absolute;
    inset: 0;
    pointer-events: none;
  }
  .edge-x {
    pointer-events: all;
    cursor: pointer;
    fill: #334155;
    font-size: 14px;
    font-weight: 700;
  }

  .node {
    position: absolute;
    width: 240px;
    border: 1px solid #cbd5e1;
    border-radius: 12px;
    background: #fff;
    box-shadow: 0 4px 14px rgba(15, 23, 42, 0.08);
    padding: 8px;
    user-select: none;
  }
  .node.selected {
    border-color: #2563eb;
  }
  .node.data {
    background: #f8fafc;
  }
  .node.tool {
    background: #fff7ed;
  }
  .node-head {
    display: flex;
    justify-content: space-between;
    gap: 8px;
  }
  .node-head button {
    border: none;
    background: transparent;
    cursor: pointer;
    color: #64748b;
  }
  .node-meta {
    font-size: 11px;
    color: #64748b;
    margin-top: 4px;
  }

  .ports {
    display: flex;
    justify-content: space-between;
    margin-top: 8px;
  }
  .ins,
  .outs {
    display: flex;
    gap: 6px;
  }
  .port {
    border: 1px solid #cbd5e1;
    border-radius: 999px;
    font-size: 10px;
    padding: 2px 7px;
    background: #fff;
    cursor: pointer;
  }
  .port.in {
    border-color: #f59e0b;
  }
  .port.out {
    border-color: #2563eb;
  }
  .port.active {
    background: #dbeafe;
  }

  .cohort-badge {
    margin-top: 8px;
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-size: 10px;
    font-weight: 700;
    color: #6d28d9;
    background: #f5f3ff;
    border: 1px solid #c4b5fd;
    border-radius: 999px;
    padding: 2px 8px;
    width: max-content;
  }

  .banner {
    position: absolute;
    left: 12px;
    bottom: 12px;
    padding: 6px 10px;
    border-radius: 8px;
    background: #0f172a;
    color: #fff;
    font-size: 12px;
  }

  .props {
    margin-top: 8px;
    border-top: 1px solid #e2e8f0;
    padding-top: 8px;
    display: flex;
    flex-direction: column;
    gap: 6px;
    min-height: 300px;
  }
  .props h5 {
    margin: 0;
    font-size: 13px;
  }
  .props table {
    width: 100%;
    border-collapse: collapse;
    font-size: 10px;
  }
  .props th,
  .props td {
    border: 1px solid #e2e8f0;
    padding: 3px;
  }
  .props label {
    display: flex;
    flex-direction: column;
    gap: 3px;
    font-size: 12px;
  }
  .props input {
    border: 1px solid #dbe4f0;
    border-radius: 8px;
    padding: 6px;
  }

  .run {
    border: 1px solid #0f172a;
    background: #0f172a;
    color: #fff;
    border-radius: 8px;
    padding: 6px;
    cursor: pointer;
  }
  .definition {
    white-space: pre-wrap;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 10px;
    border: 1px dashed #dbe4f0;
    padding: 6px;
    border-radius: 8px;
  }
  .empty {
    color: #64748b;
    font-size: 12px;
  }
</style>
