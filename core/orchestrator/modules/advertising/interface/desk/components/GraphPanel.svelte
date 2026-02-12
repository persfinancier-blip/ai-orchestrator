<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount } from 'svelte';
  import * as THREE from 'three';
  import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
  import type {
    BuyerCluster,
    BuyerPoint,
    ColorMetric,
    LevelMode,
    Mode3D,
    PeriodMode,
    SimilarityEntity,
    SizeMetric,
  } from '../data/mockGraph';
  import { aggregateItemsByLevel, formatMetricLabel, generateBuyerCloud, metricValue } from '../data/mockGraph';

  export let entities: SimilarityEntity[] = [];

  const dispatch = createEventDispatcher();

  let mode: Mode3D = '3D';
  let level: LevelMode = 'SKU';
  let period: PeriodMode = '30 дней';
  let colorBy: ColorMetric = 'DRR';
  let sizeBy: SizeMetric = 'Sales';

  let showBuyers = true;
  let showCampaigns = true;
  let showVectors = true;
  let nearestLimit = 50;
  let search = '';

  let panel: HTMLDivElement;
  let canvas2d: HTMLCanvasElement;
  let searchInput: HTMLInputElement;

  let width = 980;
  let height = 540;
  let ctx: CanvasRenderingContext2D;
  let scale2d = 1;
  let panX = width / 2;
  let panY = height / 2;
  let dragging = false;
  let dragStart = { x: 0, y: 0 };

  let selectedId = '';
  let hovered: SimilarityEntity | null = null;
  let tooltip = { visible: false, x: 0, y: 0, entity: null as SimilarityEntity | null };

  const cloud = generateBuyerCloud(220);
  const buyerClusters = cloud.clusters;
  const buyerPoints = cloud.points;

  $: base = aggregateItemsByLevel(entities, level);
  $: filtered = filterEntities(base);
  $: visible = applyCampaignLimit(filtered);
  $: draw2d();
  $: if (mode === '3D') rebuild3d();

  function filterEntities(list: SimilarityEntity[]): SimilarityEntity[] {
    const q = search.trim().toLowerCase();
    return list.filter((entity) => {
      if (entity.type === 'campaign' && !showCampaigns) return false;
      if (q && !entity.name.toLowerCase().includes(q) && !entity.id.toLowerCase().includes(q)) return false;
      return true;
    });
  }

  function applyCampaignLimit(list: SimilarityEntity[]): SimilarityEntity[] {
    const items = list.filter((e) => e.type === 'item');
    const campaigns = list
      .filter((e) => e.type === 'campaign')
      .sort((a, b) => (b.matchScore ?? 0) - (a.matchScore ?? 0))
      .slice(0, nearestLimit);
    return [...items, ...campaigns];
  }

  function worldToScreen(x: number, y: number): { x: number; y: number } {
    return { x: x * scale2d + panX, y: y * scale2d + panY };
  }

  function colorFor(v: number): string {
    const n = Math.max(0, Math.min(1, v));
    const h = 220 - n * 180;
    return `hsl(${h}, 64%, 56%)`;
  }

  function ensure2dCtx(): void {
    if (canvas2d && !ctx) ctx = canvas2d.getContext('2d') as CanvasRenderingContext2D;
  }

  function draw2d(): void {
    ensure2dCtx();
    if (mode !== '2D' || !ctx) return;
    ctx.clearRect(0, 0, width, height);
    ctx.fillStyle = '#f8fbff';
    ctx.fillRect(0, 0, width, height);

    const values = visible.map((e) => metricValue(e, colorBy));
    const min = Math.min(...values, 0);
    const max = Math.max(...values, 1);

    visible.forEach((point) => {
      const pos = worldToScreen(point.x, point.y);
      const m = metricValue(point, colorBy);
      const normalized = (m - min) / Math.max(max - min, 0.001);
      const sizeV = metricValue(point, sizeBy);
      const r = 4 + Math.min(18, Math.sqrt(sizeV) / 20);
      const selected = selectedId === point.id;

      ctx.beginPath();
      ctx.fillStyle = colorFor(normalized);
      if (point.type === 'campaign') {
        ctx.moveTo(pos.x, pos.y - r);
        ctx.lineTo(pos.x + r, pos.y);
        ctx.lineTo(pos.x, pos.y + r);
        ctx.lineTo(pos.x - r, pos.y);
        ctx.closePath();
      } else {
        ctx.arc(pos.x, pos.y, r, 0, Math.PI * 2);
      }
      ctx.fill();

      if (selected) {
        ctx.strokeStyle = '#0f172a';
        ctx.lineWidth = 2;
        ctx.stroke();
      }
    });
  }

  function pick2d(mx: number, my: number): SimilarityEntity | null {
    let best: SimilarityEntity | null = null;
    let bestD = Infinity;
    visible.forEach((entity) => {
      const p = worldToScreen(entity.x, entity.y);
      const d = Math.hypot(mx - p.x, my - p.y);
      if (d < bestD && d < 20) {
        best = entity;
        bestD = d;
      }
    });
    return best;
  }

  function selectEntity(entity: SimilarityEntity): void {
    selectedId = entity.id;
    dispatch('selectEntity', entity);
    highlightCluster(entity.clusterName);
    updateMeshes();
  }

  function on2dWheel(e: WheelEvent): void {
    if (mode !== '2D') return;
    e.preventDefault();
    const delta = e.deltaY > 0 ? 0.9 : 1.1;
    scale2d = Math.max(0.25, Math.min(3.3, scale2d * delta));
  }

  function on2dMouseDown(e: MouseEvent): void {
    if (mode !== '2D') return;
    dragging = true;
    dragStart = { x: e.clientX - panX, y: e.clientY - panY };
  }

  function on2dMouseMove(e: MouseEvent): void {
    if (mode !== '2D') return;
    const rect = canvas2d.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const my = e.clientY - rect.top;
    const picked = pick2d(mx, my);

    hovered = picked;
    if (picked) {
      tooltip = { visible: true, x: mx + 12, y: my + 12, entity: picked };
    } else {
      tooltip.visible = false;
    }

    if (dragging) {
      panX = e.clientX - dragStart.x;
      panY = e.clientY - dragStart.y;
    }
  }

  function on2dMouseUp(e: MouseEvent): void {
    if (mode !== '2D') return;
    dragging = false;
    const rect = canvas2d.getBoundingClientRect();
    const picked = pick2d(e.clientX - rect.left, e.clientY - rect.top);
    if (picked) selectEntity(picked);
  }

  let renderer: THREE.WebGLRenderer;
  let scene: THREE.Scene;
  let camera: THREE.PerspectiveCamera;
  let controls: OrbitControls;
  let animationId = 0;
  let raycaster = new THREE.Raycaster();
  let mouseNdc = new THREE.Vector2();
  let container3d: HTMLDivElement;

  let buyerMesh: THREE.InstancedMesh;
  let itemMesh: THREE.InstancedMesh;
  let campaignMesh: THREE.InstancedMesh;
  let vectorGroup = new THREE.Group();
  let clusterHighlight = new THREE.Mesh(
    new THREE.SphereGeometry(8, 18, 18),
    new THREE.MeshBasicMaterial({ color: '#22c55e', wireframe: true, transparent: true, opacity: 0.55 })
  );

  let buyerVisible: BuyerPoint[] = [];
  let itemsVisible: SimilarityEntity[] = [];
  let campaignsVisible: SimilarityEntity[] = [];

  function init3d(): void {
    scene = new THREE.Scene();
    scene.background = new THREE.Color('#f8fbff');

    camera = new THREE.PerspectiveCamera(50, width / height, 0.1, 1400);
    camera.position.set(0, 60, 180);

    renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    renderer.setSize(width, height);
    container3d.innerHTML = '';
    container3d.appendChild(renderer.domElement);

    controls = new OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.08;

    scene.add(new THREE.AmbientLight('#ffffff', 0.95));
    const directional = new THREE.DirectionalLight('#ffffff', 0.45);
    directional.position.set(50, 90, 60);
    scene.add(directional);

    const grid = new THREE.GridHelper(260, 16, '#dbe4f0', '#eaf0f7');
    scene.add(grid);

    clusterHighlight.visible = false;
    scene.add(clusterHighlight);

    scene.add(vectorGroup);

    renderer.domElement.addEventListener('mousemove', on3dMouseMove);
    renderer.domElement.addEventListener('click', on3dClick);

    animate();
  }

  function clearMesh(mesh?: THREE.InstancedMesh): void {
    if (!mesh) return;
    scene.remove(mesh);
    mesh.geometry.dispose();
    (mesh.material as THREE.Material).dispose();
  }

  function colorMetric(entity: SimilarityEntity, min: number, max: number): THREE.Color {
    const value = metricValue(entity, colorBy);
    const t = (value - min) / Math.max(max - min, 0.001);
    return new THREE.Color(colorFor(t));
  }

  function sizeMetric(entity: SimilarityEntity): number {
    const value = metricValue(entity, sizeBy);
    return 0.65 + Math.min(1.6, Math.sqrt(value) / 120);
  }

  function rebuild3d(): void {
    if (mode !== '3D' || !scene) return;

    clearMesh(buyerMesh);
    clearMesh(itemMesh);
    clearMesh(campaignMesh);
    vectorGroup.clear();

    const values = visible.map((e) => metricValue(e, colorBy));
    const min = Math.min(...values, 0);
    const max = Math.max(...values, 1);

    buyerVisible = showBuyers ? buyerPoints : [];
    itemsVisible = visible.filter((e) => e.type === 'item');
    campaignsVisible = visible.filter((e) => e.type === 'campaign');

    if (showBuyers) {
      buyerMesh = new THREE.InstancedMesh(
        new THREE.SphereGeometry(0.85, 6, 6),
        new THREE.MeshBasicMaterial({ color: '#86efac', transparent: true, opacity: 0.23 }),
        buyerVisible.length
      );
      const dummy = new THREE.Object3D();
      buyerVisible.forEach((point, i) => {
        dummy.position.set(point.x, point.y, point.z);
        dummy.scale.setScalar(1);
        dummy.updateMatrix();
        buyerMesh.setMatrixAt(i, dummy.matrix);
      });
      buyerMesh.instanceMatrix.needsUpdate = true;
      scene.add(buyerMesh);
    }

    if (itemsVisible.length > 0) {
      itemMesh = new THREE.InstancedMesh(
        new THREE.SphereGeometry(1, 8, 8),
        new THREE.MeshBasicMaterial({ vertexColors: true }),
        itemsVisible.length
      );
      itemMesh.instanceColor = new THREE.InstancedBufferAttribute(new Float32Array(itemsVisible.length * 3), 3);
      const dummy = new THREE.Object3D();
      itemsVisible.forEach((item, i) => {
        dummy.position.set(item.x, item.y, item.z);
        const s = sizeMetric(item);
        dummy.scale.set(s, s, s);
        dummy.updateMatrix();
        itemMesh.setMatrixAt(i, dummy.matrix);
        itemMesh.setColorAt(i, colorMetric(item, min, max));
      });
      itemMesh.instanceMatrix.needsUpdate = true;
      scene.add(itemMesh);
    }

    if (campaignsVisible.length > 0) {
      campaignMesh = new THREE.InstancedMesh(
        new THREE.OctahedronGeometry(1.1, 0),
        new THREE.MeshBasicMaterial({ vertexColors: true }),
        campaignsVisible.length
      );
      campaignMesh.instanceColor = new THREE.InstancedBufferAttribute(new Float32Array(campaignsVisible.length * 3), 3);
      const dummy = new THREE.Object3D();
      campaignsVisible.forEach((campaign, i) => {
        dummy.position.set(campaign.x, campaign.y, campaign.z);
        const s = sizeMetric(campaign);
        dummy.scale.set(s, s, s);
        dummy.rotation.set(0.2, 0.4, 0.1);
        dummy.updateMatrix();
        campaignMesh.setMatrixAt(i, dummy.matrix);
        campaignMesh.setColorAt(i, colorMetric(campaign, min, max));
      });
      campaignMesh.instanceMatrix.needsUpdate = true;
      scene.add(campaignMesh);
    }

    if (showVectors) {
      campaignsVisible.forEach((campaign) => {
        const cluster = buyerClusters.find((x) => x.name === campaign.targetCluster);
        if (!cluster) return;
        const start = new THREE.Vector3(campaign.x, campaign.y, campaign.z);
        const end = new THREE.Vector3(cluster.center.x, cluster.center.y, cluster.center.z);
        const dist = start.distanceTo(end);
        const t = Math.min(1, dist / 120);
        const color = new THREE.Color(`hsl(${(1 - t) * 120}, 70%, 45%)`);

        const line = new THREE.Line(
          new THREE.BufferGeometry().setFromPoints([start, end]),
          new THREE.LineBasicMaterial({ color, transparent: true, opacity: 0.72 })
        );
        vectorGroup.add(line);

        const dir = end.clone().sub(start).normalize();
        const arrow = new THREE.ArrowHelper(dir, end.clone().add(dir.clone().multiplyScalar(-6)), 5, color.getHex(), 2.3, 1.4);
        vectorGroup.add(arrow);
      });
    }

    updateMeshes();
  }

  function updateMeshes(): void {
    if (mode !== '3D') return;
    if (itemMesh) itemMesh.material.opacity = showCampaigns ? 0.95 : 0.85;
    if (campaignMesh) campaignMesh.visible = showCampaigns;
    if (buyerMesh) buyerMesh.visible = showBuyers;
    vectorGroup.visible = showVectors;

    if (!selectedId) return;
    const selected = visible.find((e) => e.id === selectedId);
    if (!selected) return;
    highlightCluster(selected.clusterName);
  }

  function highlightCluster(name: string): void {
    const cluster = buyerClusters.find((entry) => entry.name === name);
    if (!cluster) {
      clusterHighlight.visible = false;
      return;
    }
    clusterHighlight.position.set(cluster.center.x, cluster.center.y, cluster.center.z);
    clusterHighlight.visible = true;
  }

  function resolveEntityFromHit(intersections: THREE.Intersection[]): SimilarityEntity | null {
    const hit = intersections.find((entry) => entry.object === itemMesh || entry.object === campaignMesh);
    if (!hit || hit.instanceId === undefined) return null;
    if (hit.object === itemMesh) return itemsVisible[hit.instanceId] ?? null;
    if (hit.object === campaignMesh) return campaignsVisible[hit.instanceId] ?? null;
    return null;
  }

  function on3dMouseMove(event: MouseEvent): void {
    if (mode !== '3D') return;
    const rect = renderer.domElement.getBoundingClientRect();
    mouseNdc.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
    mouseNdc.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;

    raycaster.setFromCamera(mouseNdc, camera);
    const intersects = raycaster.intersectObjects([itemMesh, campaignMesh].filter(Boolean), true);
    const picked = resolveEntityFromHit(intersects);
    if (picked) {
      hovered = picked;
      tooltip = { visible: true, x: event.clientX - rect.left + 12, y: event.clientY - rect.top + 12, entity: picked };
    } else {
      hovered = null;
      tooltip.visible = false;
    }
  }

  function on3dClick(): void {
    if (!hovered) return;
    selectEntity(hovered);
  }

  function animate(): void {
    controls?.update();
    if (buyerMesh) {
      const dist = camera.position.length();
      if (dist > 290) buyerMesh.count = Math.floor(buyerVisible.length * 0.25);
      else if (dist > 220) buyerMesh.count = Math.floor(buyerVisible.length * 0.55);
      else buyerMesh.count = buyerVisible.length;
    }
    renderer?.render(scene, camera);
    animationId = requestAnimationFrame(animate);
  }

  function resetView(): void {
    if (mode === '2D') {
      scale2d = 1;
      panX = width / 2;
      panY = height / 2;
      return;
    }
    camera.position.set(0, 60, 180);
    controls.target.set(0, 0, 0);
    controls.update();
  }

  function onKeydown(event: KeyboardEvent): void {
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'k') {
      event.preventDefault();
      searchInput?.focus();
      searchInput?.select();
    }
  }

  function onResize(): void {
    const rect = panel.getBoundingClientRect();
    width = Math.max(800, Math.floor(rect.width - 24));
    height = 540;
    if (mode === '3D' && renderer && camera) {
      renderer.setSize(width, height);
      camera.aspect = width / height;
      camera.updateProjectionMatrix();
    }
  }

  onMount(() => {
    onResize();
    init3d();
    window.addEventListener('resize', onResize);
    window.addEventListener('keydown', onKeydown);
  });

  onDestroy(() => {
    window.removeEventListener('resize', onResize);
    window.removeEventListener('keydown', onKeydown);
    renderer?.domElement.removeEventListener('mousemove', on3dMouseMove);
    renderer?.domElement.removeEventListener('click', on3dClick);
    cancelAnimationFrame(animationId);
    renderer?.dispose();
  });
</script>

<section class="panel similarity-map" bind:this={panel}>
  <div class="topbar">
    <label>Уровень
      <select bind:value={level}><option>SKU</option><option>Предмет</option></select>
    </label>
    <label>Период
      <select bind:value={period}><option>7 дней</option><option>14 дней</option><option>30 дней</option></select>
    </label>
    <label>Режим
      <select bind:value={mode}><option>2D</option><option>3D</option></select>
    </label>
    <label>Цвет по
      <select bind:value={colorBy}><option value="DRR">ДРР</option><option value="ROI">ROI</option><option value="CR2">CR2</option><option value="Spend">Расход</option><option value="EntryPoint(SearchShare)">Доля поиска</option><option value="SegmentShare">Доля сегментов</option></select>
    </label>
    <label>Размер по
      <select bind:value={sizeBy}><option value="Sales">Выручка</option><option value="Spend">Расход</option><option value="Orders">Заказы</option></select>
    </label>
    <label class="toggle"><input type="checkbox" bind:checked={showBuyers} /> Покупатели</label>
    <label class="toggle"><input type="checkbox" bind:checked={showCampaigns} /> РК</label>
    <label class="toggle"><input type="checkbox" bind:checked={showVectors} /> Векторы</label>
    <label>Ближайшие РК
      <select bind:value={nearestLimit}><option value={10}>10</option><option value={25}>25</option><option value={50}>50</option><option value={100}>100</option></select>
    </label>
    <input bind:this={searchInput} bind:value={search} placeholder="Поиск SKU / РК" />
    <button class="ghost" on:click={resetView}>Сбросить вид</button>
  </div>

  <div class="status">
    Покупатели: {showBuyers ? buyerPoints.length : 0} · Сущности: {visible.length} · Режим: {mode} · Цвет: {formatMetricLabel(colorBy)} · Размер: {formatMetricLabel(sizeBy)}
  </div>

  <div class="stage">
    {#if mode === '2D'}
      <canvas
        bind:this={canvas2d}
        width={width}
        height={height}
        on:wheel|preventDefault={on2dWheel}
        on:mousedown={on2dMouseDown}
        on:mousemove={on2dMouseMove}
        on:mouseup={on2dMouseUp}
        on:mouseleave={() => { dragging = false; tooltip.visible = false; }}
      />
    {:else}
      <div bind:this={container3d} class="scene3d" />
    {/if}

    {#if tooltip.visible && tooltip.entity}
      <div class="tooltip" style={`left:${tooltip.x}px;top:${tooltip.y}px`}>
        <div class="name">{tooltip.entity.name}</div>
        <div>{tooltip.entity.type === 'item' ? 'Товар' : 'Рекламная кампания'}</div>
        <div>ДРР {tooltip.entity.metrics.DRR}% · ROI {tooltip.entity.metrics.ROI} · CR2 {tooltip.entity.metrics.CR2}%</div>
        <div>Выручка {tooltip.entity.metrics.sales.toLocaleString('ru-RU')} ₽ · Расход {tooltip.entity.metrics.spend.toLocaleString('ru-RU')} ₽</div>
      </div>
    {/if}
  </div>
</section>

<style>
  .similarity-map { display:flex; flex-direction:column; gap:8px; }
  .topbar { display:flex; flex-wrap:wrap; gap:10px; font-size:12px; color:#475569; align-items:center; }
  .topbar label { display:flex; align-items:center; gap:6px; }
  .toggle { background:#f8fbff; border:1px solid #e2e8f0; border-radius:10px; padding:4px 8px; }
  select,input { border:1px solid #dbe4f0; border-radius:10px; padding:5px 8px; background:#fff; }
  input { min-width:150px; }
  .ghost { border:1px solid #dbe4f0; background:#fff; border-radius:10px; padding:6px 10px; color:#334155; }
  .status { font-size:12px; color:#64748b; }
  .stage { position:relative; width:100%; }
  canvas,.scene3d { width:100%; height:540px; border:1px solid #e2e8f0; border-radius:16px; background:#f9fbff; }
  .tooltip {
    position:absolute;
    pointer-events:none;
    background:rgba(15,23,42,.92);
    color:#f8fafc;
    border-radius:12px;
    padding:10px 12px;
    font-size:12px;
    max-width:320px;
    box-shadow:0 10px 24px rgba(15,23,42,.2);
  }
  .tooltip .name { font-weight:600; margin-bottom:3px; }
</style>
