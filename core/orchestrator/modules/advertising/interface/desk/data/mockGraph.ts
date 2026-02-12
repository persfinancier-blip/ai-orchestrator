export type NodeType = 'sku' | 'segment' | 'ad_target' | 'entry_org' | 'entry_paid' | 'kpi';
export type LodMode = 'SKU' | 'Предмет' | 'Campaign ID' | 'Entry points';

export type GraphNode = {
  id: string;
  label: string;
  type: NodeType;
  skuId?: string;
  subject?: string;
  campaignId?: string;
  entryPoint?: string;
  metrics: {
    ctr: number;
    cr2: number;
    rating: number;
    returnsPct: number;
    spend: number;
    clicks: number;
    orders: number;
    drr: number;
    roi: number;
  };
};

export type GraphEdge = {
  id: string;
  from: string;
  to: string;
  weight: number;
};

const subjects = ['Кроссовки', 'Футболки', 'Аксессуары', 'Джинсы', 'Куртки'];
const segments = ['price-sensitive', 'premium', 'спорт', 'молодежь', 'семья'];
const entryPoints = ['Search', 'Category', 'Shelf', 'Recommendations'];
const adTargets = ['keyword:running', 'audience:lookalike', 'retarget:cart', 'keyword:street'];
const kpis = ['Revenue', 'DRR', 'ROI', 'Margin', 'Stock'];

const random = (min: number, max: number): number => Math.round((Math.random() * (max - min) + min) * 100) / 100;

function metrics(seed = 1): GraphNode['metrics'] {
  return {
    ctr: random(0.8, 5.5) + seed * 0.001,
    cr2: random(0.2, 2.9),
    rating: random(3.7, 4.9),
    returnsPct: random(1.2, 9.8),
    spend: random(1000, 70000),
    clicks: Math.round(random(40, 4000)),
    orders: Math.round(random(4, 420)),
    drr: random(6, 29),
    roi: random(0.8, 3.1),
  };
}

export function generateMockGraph(totalSku = 2000): { nodes: GraphNode[]; edges: GraphEdge[] } {
  const nodes: GraphNode[] = [];
  const edges: GraphEdge[] = [];

  for (let i = 0; i < totalSku; i += 1) {
    const skuId = `SKU-${100000 + i}`;
    const subject = subjects[i % subjects.length];
    const campaignId = `CMP-${(i % 40) + 1}`;
    const segment = segments[i % segments.length];
    const entryPoint = entryPoints[i % entryPoints.length];
    const adTarget = adTargets[i % adTargets.length];

    const skuNode: GraphNode = {
      id: skuId,
      label: skuId,
      type: 'sku',
      skuId,
      subject,
      campaignId,
      entryPoint,
      metrics: metrics(i),
    };

    const segmentNodeId = `SEG-${segment}`;
    const entryOrgId = `ENTRY-ORG-${entryPoint}`;
    const entryPaidId = `ENTRY-PAID-${entryPoint}`;
    const adTargetId = `TARGET-${adTarget}`;
    const kpiId = `KPI-${kpis[i % kpis.length]}`;

    nodes.push(skuNode);

    const reusable: GraphNode[] = [
      { id: segmentNodeId, label: segment, type: 'segment', metrics: metrics(i + 9) },
      { id: entryOrgId, label: `${entryPoint} (org)`, type: 'entry_org', entryPoint, metrics: metrics(i + 11) },
      { id: entryPaidId, label: `${entryPoint} (ad)`, type: 'entry_paid', entryPoint, metrics: metrics(i + 15) },
      { id: adTargetId, label: adTarget, type: 'ad_target', campaignId, metrics: metrics(i + 19) },
      { id: kpiId, label: kpis[i % kpis.length], type: 'kpi', metrics: metrics(i + 27) },
    ];

    reusable.forEach((n) => {
      if (!nodes.find((x) => x.id === n.id)) nodes.push(n);
    });

    const pushEdge = (from: string, to: string, w: number): void => {
      const id = `${from}->${to}`;
      if (!edges.find((e) => e.id === id)) edges.push({ id, from, to, weight: w });
    };

    pushEdge(skuId, segmentNodeId, random(0.5, 2.5));
    pushEdge(segmentNodeId, entryOrgId, random(0.3, 1.4));
    pushEdge(entryOrgId, adTargetId, random(0.6, 2.8));
    pushEdge(adTargetId, entryPaidId, random(0.6, 3.3));
    pushEdge(entryPaidId, kpiId, random(0.7, 3.8));
  }

  return { nodes, edges };
}

function aggregateKey(node: GraphNode, mode: LodMode): string {
  if (mode === 'Предмет') return `SUBJECT:${node.subject ?? 'N/A'}`;
  if (mode === 'Campaign ID') return `CMP:${node.campaignId ?? 'N/A'}`;
  if (mode === 'Entry points') return `ENTRY:${node.entryPoint ?? 'N/A'}`;
  return node.id;
}

export function applyLod(nodes: GraphNode[], edges: GraphEdge[], mode: LodMode): { nodes: GraphNode[]; edges: GraphEdge[] } {
  if (mode === 'SKU') {
    return { nodes: nodes.slice(0, 260), edges: edges.filter((e) => nodes.slice(0, 260).some((n) => n.id === e.from) && nodes.slice(0, 260).some((n) => n.id === e.to)) };
  }

  const map = new Map<string, GraphNode[]>();
  nodes.forEach((n) => {
    const key = aggregateKey(n, mode);
    if (!map.has(key)) map.set(key, []);
    map.get(key)?.push(n);
  });

  const groupedNodes: GraphNode[] = Array.from(map.entries()).map(([key, arr]) => {
    const sample = arr[0];
    return {
      id: key,
      label: `${key} (${arr.length})`,
      type: sample.type,
      subject: sample.subject,
      campaignId: sample.campaignId,
      entryPoint: sample.entryPoint,
      metrics: sample.metrics,
    };
  });

  const groupedEdgeMap = new Map<string, GraphEdge>();
  edges.forEach((edge) => {
    const fromNode = nodes.find((n) => n.id === edge.from);
    const toNode = nodes.find((n) => n.id === edge.to);
    if (!fromNode || !toNode) return;
    const from = aggregateKey(fromNode, mode);
    const to = aggregateKey(toNode, mode);
    if (from === to) return;
    const id = `${from}->${to}`;
    const prev = groupedEdgeMap.get(id);
    if (!prev) groupedEdgeMap.set(id, { id, from, to, weight: edge.weight });
    else prev.weight += edge.weight * 0.2;
  });

  return { nodes: groupedNodes.slice(0, 260), edges: Array.from(groupedEdgeMap.values()).slice(0, 900) };
}

export const nodeColors: Record<NodeType, string> = {
  sku: '#6ecf6a',
  segment: '#b7dcff',
  ad_target: '#1f4f8b',
  entry_org: '#aeb4bd',
  entry_paid: '#4d5663',
  kpi: '#ffd666',
};
