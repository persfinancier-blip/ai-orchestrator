<script lang="ts">
  import { onMount } from 'svelte';
  import {
    sourceGroups,
    sourceItemsByGroup,
    tools,
    toolPorts,
    type SourceGroup,
    type SourceItem,
    type ToolItem,
    type ToolType
  } from '../data/workflowEditor';

  type WorkflowNode = {
    id: string;
    type: 'data' | 'tool';
    x: number;
    y: number;
    config: any;
  };
  type WorkflowEdge = { id: string; from: string; to: string; fromPort: string; toPort: string };
  type NodeRuntime = { inRows: number; outRows: number; lossRows: number; lossPct: number; successPct: number; status: 'idle' | 'ok' | 'warn' | 'error' };
  type WorkflowIssue = { level: 'error' | 'warn' | 'info'; text: string };

  let activeTab: SourceGroup = 'api_requests';
  let nodes: WorkflowNode[] = [];
  let edges: WorkflowEdge[] = [];
  let selectedNodeId = '';
  let linkFrom: { nodeId: string; port: string } | null = null;
  let banner = '';
  let nodeRuntime: Record<string, NodeRuntime> = {};
  let edgeRows: Record<string, number> = {};
  let issues: WorkflowIssue[] = [];
  let summary = { sourceRows: 0, finalRows: 0, lossRows: 0, lossPct: 0, successPct: 0 };
  let sourceCatalogLoading = false;
  let sourceCatalogError = '';
  let dynamicApiSources: SourceItem[] = [];
  let dynamicTableSources: SourceItem[] = [];

  let canvasEl: HTMLDivElement;
  let panX = 0;
  let panY = 0;
  let zoom = 1;
  let isPanning = false;
  let panStartX = 0;
  let panStartY = 0;
  let dragNode: { id: string; offsetX: number; offsetY: number } | null = null;

  const API_BASE = '/ai-orchestrator/api';
  const API_ROLE = 'data_admin';

  $: selectedNode = nodes.find((n) => n.id === selectedNodeId) ?? null;
  $: sourceList =
    activeTab === 'api_requests'
      ? dynamicApiSources.length
        ? dynamicApiSources
        : sourceItemsByGroup.api_requests || []
      : activeTab === 'data_tables'
      ? dynamicTableSources.length
        ? dynamicTableSources
        : sourceItemsByGroup.data_tables || []
      : sourceItemsByGroup.math_calculations || [];

  const uid = (p: string) => `${p}-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
  const toolCfg = (n: WorkflowNode) => n.config as { name: string; toolType: ToolType; settings: Record<string, string> };

  function defaultSettings(toolType: ToolType) {
    if (toolType === 'schedule_process') return { cron: '0 * * * *', mode: 'sync' };
    if (toolType === 'table_parser') return { parserMultiplier: '1' };
    if (toolType === 'db_write') return { targetSchema: 'ao_data', targetTable: 'bronze_result', writeSuccessRate: '98' };
    return {};
  }

  function normalizeLayer(schemaName: string, tableName: string) {
    const s = String(schemaName || '').trim().toLowerCase();
    const t = String(tableName || '').trim().toLowerCase();
    if (s.includes('bronze') || t.startsWith('bronze_')) return 'bronze';
    if (s.includes('silver') || t.startsWith('silver_')) return 'silver';
    if (s.includes('gold') || t.startsWith('gold_')) return 'gold';
    return '';
  }

  function buildApiTemplatePreview(apiName: string, method: string, path: string) {
    return [
      {
        api_name: apiName || '',
        method: method || '',
        endpoint: path || ''
      }
    ];
  }

  async function loadDynamicSourceCatalog() {
    sourceCatalogLoading = true;
    sourceCatalogError = '';
    try {
      const [apiConfigsRaw, tablesRaw] = await Promise.all([
        fetch(`${API_BASE}/api-configs`, {
          method: 'GET',
          headers: { Accept: 'application/json', 'X-AO-ROLE': API_ROLE }
        }),
        fetch(`${API_BASE}/tables`, {
          method: 'GET',
          headers: { Accept: 'application/json', 'X-AO-ROLE': API_ROLE }
        })
      ]);

      let apiJson: any = {};
      let tableJson: any = {};
      try {
        apiJson = await apiConfigsRaw.json();
      } catch {
        apiJson = {};
      }
      try {
        tableJson = await tablesRaw.json();
      } catch {
        tableJson = {};
      }

      const apiRows = Array.isArray(apiJson?.api_configs) ? apiJson.api_configs : [];
      dynamicApiSources = apiRows.map((row: any, idx: number) => {
        const name = String(row?.api_name || row?.name || '').trim() || `API template ${idx + 1}`;
        const method = String(row?.method || 'GET').trim().toUpperCase();
        const baseUrl = String(row?.base_url || '').trim();
        const path = String(row?.path || '').trim();
        return {
          id: `api_tpl_${String(row?.id || idx + 1)}`,
          name,
          group: 'api_requests' as SourceGroup,
          datasetId: `api_template:${String(row?.id || idx + 1)}`,
          schema: ['request', 'response', 'status'],
          size: 0,
          preview: buildApiTemplatePreview(name, method, `${baseUrl}${path}`),
          description: `Преднастроенный API-шаблон (${method} ${baseUrl}${path})`
        };
      });

      const existingTables = Array.isArray(tableJson?.existing_tables) ? tableJson.existing_tables : [];
      const layerTables = existingTables.filter((t: any) =>
        Boolean(normalizeLayer(String(t?.schema_name || ''), String(t?.table_name || '')))
      );
      dynamicTableSources = layerTables.map((t: any, idx: number) => {
        const schema = String(t?.schema_name || '').trim();
        const table = String(t?.table_name || '').trim();
        const layer = normalizeLayer(schema, table);
        return {
          id: `mart_tbl_${idx + 1}_${schema}_${table}`,
          name: `${schema}.${table}`,
          group: 'data_tables' as SourceGroup,
          datasetId: `db:${schema}.${table}`,
          schema: ['*'],
          size: 0,
          preview: [{ layer, schema, table }],
          description: `Таблица витрины данных уровня ${layer}`
        };
      });
    } catch (e: any) {
      sourceCatalogError = String(e?.message || e || 'Не удалось загрузить каталоги источников');
    } finally {
      sourceCatalogLoading = false;
    }
  }

  function dropPoint(clientX: number, clientY: number) {
    const r = canvasEl.getBoundingClientRect();
    return { x: (clientX - r.left - panX) / zoom, y: (clientY - r.top - panY) / zoom };
  }

  function onDrop(event: DragEvent) {
    event.preventDefault();
    const p = dropPoint(event.clientX, event.clientY);
    const sourceRaw = event.dataTransfer?.getData('application/x-workflow-source');
    if (sourceRaw) {
      const item = JSON.parse(sourceRaw) as SourceItem;
      nodes = [...nodes, { id: uid('node'), type: 'data', x: p.x, y: p.y, config: { ...item } }];
      return;
    }
    const toolRaw = event.dataTransfer?.getData('application/x-workflow-tool');
    if (toolRaw) {
      const item = JSON.parse(toolRaw) as ToolItem;
      nodes = [...nodes, { id: uid('node'), type: 'tool', x: p.x, y: p.y, config: { name: item.name, toolType: item.toolType, settings: defaultSettings(item.toolType) } }];
    }
  }

  function onDragOver(event: DragEvent) {
    event.preventDefault();
  }

  function onWheel(event: WheelEvent) {
    event.preventDefault();
    zoom = Math.max(0.5, Math.min(1.8, zoom - Math.sign(event.deltaY) * 0.08));
  }

  function startPan(event: MouseEvent) {
    if ((event.target as HTMLElement).closest('.node')) return;
    isPanning = true;
    panStartX = event.clientX - panX;
    panStartY = event.clientY - panY;
  }
  function stopAll() {
    isPanning = false;
    dragNode = null;
  }
  function onMouseMove(event: MouseEvent) {
    if (dragNode) {
      const p = dropPoint(event.clientX, event.clientY);
      nodes = nodes.map((n) => (n.id === dragNode?.id ? { ...n, x: p.x - dragNode.offsetX, y: p.y - dragNode.offsetY } : n));
      return;
    }
    if (!isPanning) return;
    panX = event.clientX - panStartX;
    panY = event.clientY - panStartY;
  }

  function startNodeDrag(event: MouseEvent, n: WorkflowNode) {
    const p = dropPoint(event.clientX, event.clientY);
    dragNode = { id: n.id, offsetX: p.x - n.x, offsetY: p.y - n.y };
  }

  function inputs(n: WorkflowNode) {
    return n.type === 'tool' ? toolPorts[toolCfg(n).toolType].inputs : [];
  }
  function outputs(n: WorkflowNode) {
    return n.type === 'tool' ? toolPorts[toolCfg(n).toolType].outputs : ['out'];
  }
  function canConnect(fromId: string, fromPort: string, toId: string, toPort: string) {
    if (fromId === toId) return false;
    const from = nodes.find((n) => n.id === fromId);
    const to = nodes.find((n) => n.id === toId);
    if (!from || !to) return false;
    if (!outputs(from).includes(fromPort) || !inputs(to).includes(toPort)) return false;
    if (edges.some((e) => e.to === toId && e.toPort === toPort)) return false;
    return true;
  }
  function connectFrom(nodeId: string, port: string) {
    linkFrom = { nodeId, port };
    banner = '';
  }
  function connectTo(nodeId: string, port: string) {
    if (!linkFrom) return;
    if (!canConnect(linkFrom.nodeId, linkFrom.port, nodeId, port)) {
      banner = 'Нельзя соединить выбранные порты';
      linkFrom = null;
      return;
    }
    edges = [...edges, { id: uid('edge'), from: linkFrom.nodeId, to: nodeId, fromPort: linkFrom.port, toPort: port }];
    linkFrom = null;
  }

  function deleteNode(id: string) {
    nodes = nodes.filter((n) => n.id !== id);
    edges = edges.filter((e) => e.from !== id && e.to !== id);
    selectedNodeId = selectedNodeId === id ? '' : selectedNodeId;
  }
  function deleteEdge(id: string) {
    edges = edges.filter((e) => e.id !== id);
  }

  function center(n: WorkflowNode) {
    return { x: n.x + 125, y: n.y + 55 };
  }

  function topo(): WorkflowNode[] | null {
    const indeg = new Map(nodes.map((n) => [n.id, 0]));
    const byId = new Map(nodes.map((n) => [n.id, n]));
    edges.forEach((e) => indeg.set(e.to, Number(indeg.get(e.to) || 0) + 1));
    const q = Array.from(indeg.entries()).filter(([, d]) => d === 0).map(([id]) => id);
    const out: WorkflowNode[] = [];
    while (q.length) {
      const id = String(q.shift());
      const n = byId.get(id);
      if (!n) continue;
      out.push(n);
      edges.filter((e) => e.from === id).forEach((e) => {
        const d = Number(indeg.get(e.to) || 0) - 1;
        indeg.set(e.to, d);
        if (d === 0) q.push(e.to);
      });
    }
    return out.length === nodes.length ? out : null;
  }

  function validate() {
    const out: WorkflowIssue[] = [];
    if (!nodes.some((n) => n.type === 'data')) out.push({ level: 'error', text: 'Нет источников данных' });
    if (!nodes.some((n) => n.type === 'tool' && toolCfg(n).toolType === 'start_process')) out.push({ level: 'error', text: 'Нет узла Старт процесса' });
    if (!nodes.some((n) => n.type === 'tool' && toolCfg(n).toolType === 'end_process')) out.push({ level: 'error', text: 'Нет узла Конец процесса' });
    nodes.filter((n) => n.type === 'tool' && toolCfg(n).toolType === 'db_write').forEach((n) => {
      if (!String(toolCfg(n).settings.targetTable || '').trim()) out.push({ level: 'error', text: `Узел ${toolCfg(n).name} без таблицы записи` });
    });
    if (!edges.length) out.push({ level: 'warn', text: 'Нет связей между узлами' });
    return out;
  }

  function runWorkflow() {
    nodeRuntime = {};
    edgeRows = {};
    issues = validate();
    if (issues.some((x) => x.level === 'error')) {
      banner = 'Исправь ошибки схемы перед запуском';
      return;
    }
    const order = topo();
    if (!order) {
      issues = [...issues, { level: 'error', text: 'Обнаружен цикл в графе' }];
      banner = 'Обнаружен цикл в графе';
      return;
    }
    for (const n of order) {
      const inEdges = edges.filter((e) => e.to === n.id);
      const inRows = inEdges.reduce((s, e) => s + Number(edgeRows[e.id] || 0), 0);
      let outRows = inRows;
      if (n.type === 'data') outRows = inEdges.length ? inRows : Number(n.config.size || 0);
      if (n.type === 'tool') {
        const cfg = toolCfg(n);
        if (cfg.toolType === 'start_process') outRows = inRows > 0 ? inRows : 1;
        if (cfg.toolType === 'schedule_process') outRows = inRows;
        if (cfg.toolType === 'table_parser') outRows = Math.max(0, Math.round(inRows * Math.max(0.1, Number(cfg.settings.parserMultiplier || 1))));
        if (cfg.toolType === 'db_write') outRows = Math.max(0, Math.round(inRows * Math.max(0, Math.min(100, Number(cfg.settings.writeSuccessRate || 98))) / 100));
        if (cfg.toolType === 'end_process') outRows = 0;
      }
      const lossRows = Math.max(0, inRows - outRows);
      const lossPct = inRows > 0 ? Number(((lossRows / inRows) * 100).toFixed(2)) : 0;
      const successPct = inRows > 0 ? Number((((inRows - lossRows) / inRows) * 100).toFixed(2)) : n.type === 'tool' && toolCfg(n).toolType !== 'start_process' ? 0 : 100;
      const status: NodeRuntime['status'] = successPct >= 95 ? 'ok' : successPct >= 80 ? 'warn' : 'error';
      nodeRuntime[n.id] = { inRows, outRows, lossRows, lossPct, successPct, status };
      const outs = edges.filter((e) => e.from === n.id);
      const base = outs.length ? Math.floor(outRows / outs.length) : 0;
      let rem = outs.length ? outRows - base * outs.length : 0;
      outs.forEach((e) => {
        edgeRows[e.id] = base + (rem > 0 ? 1 : 0);
        if (rem > 0) rem -= 1;
      });
    }
    const sourceRows = nodes.filter((n) => n.type === 'data' && edges.every((e) => e.to !== n.id)).reduce((s, n) => s + Number(nodeRuntime[n.id]?.outRows || 0), 0);
    const finalRows = nodes.filter((n) => n.type === 'tool' && toolCfg(n).toolType === 'end_process').reduce((s, n) => s + Number(nodeRuntime[n.id]?.inRows || 0), 0);
    const lossRows = Math.max(0, sourceRows - finalRows);
    const lossPct = sourceRows > 0 ? Number(((lossRows / sourceRows) * 100).toFixed(2)) : 0;
    const successPct = sourceRows > 0 ? Number((((sourceRows - lossRows) / sourceRows) * 100).toFixed(2)) : 100;
    summary = { sourceRows, finalRows, lossRows, lossPct, successPct };
    banner = `Симуляция завершена. Успешность ${successPct}%`;
  }

  function updateSetting(nodeId: string, key: string, value: string) {
    nodes = nodes.map((n) => (n.id === nodeId && n.type === 'tool' ? { ...n, config: { ...toolCfg(n), settings: { ...toolCfg(n).settings, [key]: value } } } : n));
  }

  function onSettingInput(nodeId: string, key: string, event: Event) {
    const target = event.currentTarget as HTMLInputElement | null;
    updateSetting(nodeId, key, target?.value ?? '');
  }

  function resetCanvas() {
    nodes = [];
    edges = [];
    selectedNodeId = '';
    linkFrom = null;
    nodeRuntime = {};
    edgeRows = {};
    issues = [];
    summary = { sourceRows: 0, finalRows: 0, lossRows: 0, lossPct: 0, successPct: 0 };
    banner = '';
  }

  onMount(() => {
    resetCanvas();
    loadDynamicSourceCatalog();
  });
</script>

<section class="panel workflow-v2">
  <h3>Workflow-конструктор данных</h3>

  <div class="topbar">
    <button on:click={() => (issues = validate())}>Проверить схему</button>
    <button class="primary" on:click={runWorkflow}>Смоделировать запуск</button>
    <button on:click={resetCanvas}>Очистить</button>
    <div class="summary">
      <span>Источник: {summary.sourceRows}</span>
      <span>Выход: {summary.finalRows}</span>
      <span>Потери: {summary.lossRows} ({summary.lossPct}%)</span>
      <span>Успех: {summary.successPct}%</span>
    </div>
  </div>

  <div class="workspace">
    <aside class="sources">
      <div class="source-head">
        <h4>Источники данных</h4>
        <button class="mini" on:click={loadDynamicSourceCatalog} disabled={sourceCatalogLoading}>
          {sourceCatalogLoading ? '...' : 'Обновить'}
        </button>
      </div>
      <div class="tabs">
        {#each sourceGroups as g (g.key)}
          <button class:active={activeTab === g.key} on:click={() => (activeTab = g.key)}>{g.label}</button>
        {/each}
      </div>
      {#if sourceCatalogError}
        <div class="source-error">{sourceCatalogError}</div>
      {/if}
      <div class="items">
        {#each sourceList as item (item.id)}
          <button draggable="true" class="item source" on:dragstart={(e) => e.dataTransfer?.setData('application/x-workflow-source', JSON.stringify(item))}>
            <strong>{item.name}</strong>
            <span>{item.datasetId}</span>
            <span>Строк: {item.size}</span>
          </button>
        {/each}
      </div>
    </aside>

    <div class="canvas-wrap" bind:this={canvasEl} on:drop={onDrop} on:dragover={onDragOver} on:wheel={onWheel} on:mousedown={startPan} on:mousemove={onMouseMove} on:mouseup={stopAll} on:mouseleave={stopAll}>
      <div class="canvas" style={`transform: translate(${panX}px, ${panY}px) scale(${zoom});`}>
        <svg class="edges" width="3000" height="2000">
          {#each edges as edge (edge.id)}
            {@const fromNode = nodes.find((n) => n.id === edge.from)}
            {@const toNode = nodes.find((n) => n.id === edge.to)}
            {#if fromNode && toNode}
              {@const a = center(fromNode)}
              {@const b = center(toNode)}
              <path d={`M ${a.x} ${a.y} C ${a.x + 90} ${a.y}, ${b.x - 90} ${b.y}, ${b.x} ${b.y}`} stroke="#64748b" fill="none" stroke-width="2" />
              <text x={(a.x + b.x) / 2} y={(a.y + b.y) / 2 - 6} text-anchor="middle" class="edge-label">{Number(edgeRows[edge.id] || 0)} строк</text>
            {/if}
          {/each}
        </svg>

        {#each nodes as node (node.id)}
          {@const rt = nodeRuntime[node.id]}
          <div class="node {node.type} {selectedNodeId === node.id ? 'selected' : ''}" style={`left:${node.x}px;top:${node.y}px;`} on:mousedown={(e) => startNodeDrag(e, node)} on:click={() => (selectedNodeId = node.id)}>
            <div class="node-head">
              <strong>{node.config.name}</strong>
              <button on:click|stopPropagation={() => deleteNode(node.id)}>x</button>
            </div>
            <div class="node-meta">{node.type === 'data' ? node.config.datasetId : node.config.toolType}</div>
            {#if rt}
              <div class="runtime {rt.status}">in {rt.inRows} | out {rt.outRows} | loss {rt.lossRows} ({rt.lossPct}%)</div>
            {/if}
            <div class="ports">
              <div>{#each inputs(node) as p (p)}<button class="port in" on:click|stopPropagation={() => connectTo(node.id, p)}>{p}</button>{/each}</div>
              <div>{#each outputs(node) as p (p)}<button class="port out {linkFrom?.nodeId === node.id ? 'active' : ''}" on:click|stopPropagation={() => connectFrom(node.id, p)}>{p}</button>{/each}</div>
            </div>
          </div>
        {/each}
      </div>
      {#if banner}<div class="banner">{banner}</div>{/if}
    </div>

    <aside class="tools">
      <h4>Инструменты работы с данными</h4>
      <div class="items">
        {#each tools as t (t.id)}
          <button draggable="true" class="item tool" on:dragstart={(e) => e.dataTransfer?.setData('application/x-workflow-tool', JSON.stringify(t))}>
            <strong>{t.name}</strong>
            <span>{t.description}</span>
          </button>
        {/each}
      </div>

      <section class="props">
        <h5>Свойства</h5>
        {#if selectedNode && selectedNode.type === 'tool'}
          {#if toolCfg(selectedNode).toolType === 'schedule_process'}
            <label>Cron<input value={toolCfg(selectedNode).settings.cron || ''} on:input={(e) => onSettingInput(selectedNode.id, 'cron', e)} /></label>
          {/if}
          {#if toolCfg(selectedNode).toolType === 'table_parser'}
            <label>Множитель парсинга<input value={toolCfg(selectedNode).settings.parserMultiplier || ''} on:input={(e) => onSettingInput(selectedNode.id, 'parserMultiplier', e)} /></label>
          {/if}
          {#if toolCfg(selectedNode).toolType === 'db_write'}
            <label>Схема<input value={toolCfg(selectedNode).settings.targetSchema || ''} on:input={(e) => onSettingInput(selectedNode.id, 'targetSchema', e)} /></label>
            <label>Таблица<input value={toolCfg(selectedNode).settings.targetTable || ''} on:input={(e) => onSettingInput(selectedNode.id, 'targetTable', e)} /></label>
            <label>Успешная запись, %<input value={toolCfg(selectedNode).settings.writeSuccessRate || ''} on:input={(e) => onSettingInput(selectedNode.id, 'writeSuccessRate', e)} /></label>
          {/if}
        {:else}
          <div class="empty">Выбери узел-инструмент на canvas</div>
        {/if}
      </section>

      <section class="props">
        <h5>Связи</h5>
        {#if edges.length}
          {#each edges as e (e.id)}
            <div class="edge-row"><span>{e.fromPort} -> {e.toPort}</span><button on:click={() => deleteEdge(e.id)}>Удалить</button></div>
          {/each}
        {:else}
          <div class="empty">Связей нет</div>
        {/if}
      </section>

      <section class="props">
        <h5>Ошибки и предупреждения</h5>
        {#if issues.length}
          {#each issues as i, idx (idx)}
            <div class="issue {i.level}">{i.text}</div>
          {/each}
        {:else}
          <div class="empty">Нет</div>
        {/if}
      </section>
    </aside>
  </div>
</section>

<style>
  .workflow-v2 { display: flex; flex-direction: column; gap: 10px; }
  .topbar { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
  .topbar button { border: 1px solid #dbe4f0; background: #fff; border-radius: 9px; padding: 6px 10px; cursor: pointer; }
  .topbar .primary { background: #0f172a; color: #fff; border-color: #0f172a; }
  .summary { display: flex; gap: 8px; font-size: 12px; color: #334155; }
  .workspace { display: grid; grid-template-columns: 280px minmax(0, 1fr) 360px; gap: 10px; min-height: 720px; }
  .sources,.tools { border: 1px solid #e2e8f0; border-radius: 12px; padding: 10px; background: #f8fbff; display: flex; flex-direction: column; gap: 8px; overflow: auto; }
  .source-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
  .mini { border: 1px solid #dbe4f0; background: #fff; border-radius: 8px; padding: 4px 8px; font-size: 11px; cursor: pointer; }
  .source-error { border: 1px solid #fecaca; background: #fef2f2; color: #991b1b; border-radius: 8px; padding: 6px 8px; font-size: 11px; }
  .tabs { display: grid; gap: 6px; }
  .tabs button { border: 1px solid #dbe4f0; background: #fff; border-radius: 8px; padding: 6px; text-align: left; cursor: pointer; }
  .tabs button.active { background: #0f172a; color: #fff; }
  .items { display: flex; flex-direction: column; gap: 6px; }
  .item { text-align: left; border: 1px solid #dbe4f0; background: #fff; border-radius: 8px; padding: 7px; cursor: grab; display: flex; flex-direction: column; gap: 2px; }
  .item span { font-size: 11px; color: #64748b; }
  .item.tool { background: #fff7ed; border-color: #fed7aa; }
  .canvas-wrap { position: relative; border: 1px solid #dbe4f0; border-radius: 12px; overflow: hidden; background: #fff; }
  .canvas { position: relative; width: 3000px; height: 2000px; transform-origin: 0 0; background-image: linear-gradient(#eef2f7 1px, transparent 1px), linear-gradient(90deg, #eef2f7 1px, transparent 1px); background-size: 24px 24px; }
  .edges { position: absolute; inset: 0; pointer-events: none; }
  .edge-label { fill: #334155; font-size: 11px; }
  .node { position: absolute; width: 250px; border: 1px solid #cbd5e1; border-radius: 10px; background: #fff; padding: 8px; box-shadow: 0 4px 12px rgba(15,23,42,.07); }
  .node.tool { background: #fff7ed; }
  .node.data { background: #f8fafc; }
  .node.selected { border-color: #2563eb; }
  .node-head { display: flex; justify-content: space-between; gap: 8px; }
  .node-head button { border: none; background: transparent; cursor: pointer; color: #64748b; }
  .node-meta { color: #64748b; font-size: 11px; margin-top: 4px; }
  .runtime { margin-top: 6px; font-size: 11px; border-radius: 7px; padding: 4px 6px; border: 1px solid #e2e8f0; }
  .runtime.ok { background: #f0fdf4; border-color: #86efac; }
  .runtime.warn { background: #fffbeb; border-color: #fcd34d; }
  .runtime.error { background: #fef2f2; border-color: #fca5a5; }
  .ports { margin-top: 7px; display: flex; justify-content: space-between; }
  .port { border: 1px solid #cbd5e1; border-radius: 999px; background: #fff; cursor: pointer; padding: 2px 7px; font-size: 10px; }
  .port.in { border-color: #f59e0b; } .port.out { border-color: #2563eb; } .port.active { background: #dbeafe; }
  .banner { position: absolute; left: 10px; bottom: 10px; background: #0f172a; color: #fff; border-radius: 7px; padding: 6px 9px; font-size: 12px; }
  .props { border-top: 1px solid #e2e8f0; margin-top: 6px; padding-top: 7px; display: flex; flex-direction: column; gap: 6px; }
  .props h5 { margin: 0; font-size: 13px; }
  .props label { display: flex; flex-direction: column; gap: 3px; font-size: 12px; }
  .props input { border: 1px solid #dbe4f0; border-radius: 7px; padding: 6px; }
  .edge-row { border: 1px solid #e2e8f0; border-radius: 7px; background: #fff; padding: 6px; display: flex; justify-content: space-between; gap: 8px; font-size: 11px; }
  .edge-row button { border: 1px solid #dbe4f0; border-radius: 6px; background: #fff; cursor: pointer; }
  .issue { border-radius: 7px; border: 1px solid #e2e8f0; background: #fff; padding: 6px; font-size: 12px; }
  .issue.error { border-color: #fecaca; background: #fef2f2; }
  .issue.warn { border-color: #fde68a; background: #fffbeb; }
  .issue.info { border-color: #bfdbfe; background: #eff6ff; }
  .empty { color: #64748b; font-size: 12px; }
</style>



