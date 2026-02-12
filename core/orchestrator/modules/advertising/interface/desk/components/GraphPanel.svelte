<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import * as THREE from 'three';
  import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
  import type { BuyerPoint, ColorField, EntityMode, MeasureField, Mode3D, PeriodMode, SimilarityEntity } from '../data/mockGraph';
  import { DATA_FIELDS, MEASURE_FIELDS, formatFieldTitle, generateBuyerCloud, getMeasure } from '../data/mockGraph';

  export let entities: SimilarityEntity[] = [];

  type Preset = { name: string; x: MeasureField; y: MeasureField; z: MeasureField; colorBy: ColorField; sizeBy: MeasureField };

  const presets: Preset[] = [
    { name: 'Попадание по входам', x: 'revenue', y: 'spend', z: 'search_share', colorBy: 'entry_gap', sizeBy: 'orders' },
    { name: 'Эффективность', x: 'revenue', y: 'spend', z: 'roi', colorBy: 'drr', sizeBy: 'orders' },
    { name: 'Конверсия', x: 'clicks', y: 'orders', z: 'cr2', colorBy: 'roi', sizeBy: 'revenue' },
  ];

  let selectedPreset = presets[1].name;
  let entityMode: EntityMode = 'Артикулы (SKU)';
  let showBuyers = true;
  let showAds = true;

  let axisX: MeasureField = presets[1].x;
  let axisY: MeasureField = presets[1].y;
  let axisZ: MeasureField = presets[1].z;
  let colorBy: ColorField = presets[1].colorBy;
  let sizeBy: MeasureField = presets[1].sizeBy;

  let period: PeriodMode = '30 дней';
  let fromDate = '2026-02-01';
  let toDate = '2026-02-28';

  let filterCategory = 'Все';
  let filterSubject = 'Все';
  let filterBrand = 'Все';
  let filterRkType = 'Все';
  let minRevenue = 0;
  let maxDrr = 100;

  let demoMode = true;
  let mode: Mode3D = '3D';
  let search = '';

  let container3d: HTMLDivElement;
  let canvas2d: HTMLCanvasElement;
  let panel: HTMLDivElement;
  let searchInput: HTMLInputElement;
  let ctx: CanvasRenderingContext2D;

  let width = 960;
  let height = 560;
  let scale2d = 1;
  let panX = width / 2;
  let panY = height / 2;
  let dragging = false;
  let dragStart = { x: 0, y: 0 };

  const buyersFull = generateBuyerCloud(220);
  const buyersDemo = generateBuyerCloud(40);

  let sourceEntities: SimilarityEntity[] = [];
  let transformed: SimilarityEntity[] = [];
  let filtered: SimilarityEntity[] = [];
  let projected: Array<SimilarityEntity & { px: number; py: number; pz: number }> = [];
  let categoryOptions: string[] = ['Все'];
  let subjectOptions: string[] = ['Все'];
  let brandOptions: string[] = ['Все'];
  let rkTypeOptions: string[] = ['Все'];

  $: sourceEntities = demoMode
    ? [...entities.filter((entity) => entity.type === 'item').slice(0, 10), ...entities.filter((entity) => entity.type === 'campaign').slice(0, 10)]
    : entities;

  $: categoryOptions = uniqueValues('category');
  $: subjectOptions = uniqueValues('subject');
  $: brandOptions = uniqueValues('brand');
  $: rkTypeOptions = uniqueValues('rkType');

  $: transformed = transformEntities(sourceEntities);
  $: filtered = applyFilters(transformed);
  $: projected = projectEntities(filtered);
  $: if (mode === '2D') draw2d();
  $: if (mode === '3D') rebuild3d();

  function uniqueValues(field: 'category' | 'subject' | 'brand' | 'rkType'): string[] {
    if (!sourceEntities || sourceEntities.length === 0) return ['Все'];
    return ['Все', ...Array.from(new Set(sourceEntities.map((entity) => String(entity[field]))))];
  }

  function aggregateBy(field: 'subject' | 'category' | 'brand', label: string, list: SimilarityEntity[]): SimilarityEntity[] {
    const campaigns = list.filter((e) => e.type === 'campaign');
    const items = list.filter((e) => e.type === 'item');
    const groups = new Map<string, SimilarityEntity[]>();
    items.forEach((item) => {
      const key = item[field];
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key)?.push(item);
    });

    const aggregated = Array.from(groups.entries()).map(([key, group], idx) => {
      const avg = (measure: MeasureField): number => group.reduce((sum, entity) => sum + entity.measures[measure], 0) / group.length;
      return {
        ...group[0],
        id: `${label}-${idx + 1}`,
        name: `${key} (${group.length})`,
        sku: '',
        campaign_id: '',
        subject: field === 'subject' ? key : group[0].subject,
        category: field === 'category' ? key : group[0].category,
        brand: field === 'brand' ? key : group[0].brand,
        measures: {
          revenue: Math.round(avg('revenue')),
          orders: Math.round(avg('orders')),
          spend: Math.round(avg('spend')),
          drr: Number(avg('drr').toFixed(2)),
          roi: Number(avg('roi').toFixed(2)),
          cr0: Number(avg('cr0').toFixed(2)),
          cr1: Number(avg('cr1').toFixed(2)),
          cr2: Number(avg('cr2').toFixed(2)),
          clicks: Math.round(avg('clicks')),
          search_share: Number(avg('search_share').toFixed(2)),
          shelf_share: Number(avg('shelf_share').toFixed(2)),
          stock_days: Math.round(avg('stock_days')),
          entry_gap: Number(avg('entry_gap').toFixed(2)),
        },
      };
    });

    return [...aggregated, ...campaigns];
  }

  function transformEntities(list: SimilarityEntity[]): SimilarityEntity[] {
    if (entityMode === 'Рекламные кампании (РК)') return list.filter((e) => e.type === 'campaign');
    if (entityMode === 'Предметы') return aggregateBy('subject', 'SUB', list);
    if (entityMode === 'Категории') return aggregateBy('category', 'CAT', list);
    if (entityMode === 'Бренды') return aggregateBy('brand', 'BR', list);
    return list;
  }

  function inPeriod(date: string): boolean {
    if (period === 'Выбрать даты') return date >= fromDate && date <= toDate;
    const day = Number(date.slice(-2));
    if (period === '7 дней') return day >= 22;
    if (period === '14 дней') return day >= 15;
    return true;
  }

  function applyFilters(list: SimilarityEntity[]): SimilarityEntity[] {
    const query = search.trim().toLowerCase();
    return list.filter((entity) => {
      if (!inPeriod(entity.date)) return false;
      if (filterCategory !== 'Все' && entity.category !== filterCategory) return false;
      if (filterSubject !== 'Все' && entity.subject !== filterSubject) return false;
      if (filterBrand !== 'Все' && entity.brand !== filterBrand) return false;
      if (filterRkType !== 'Все' && entity.rkType !== filterRkType) return false;
      if (entity.measures.revenue < minRevenue) return false;
      if (entity.measures.drr > maxDrr) return false;
      if (query && !entity.name.toLowerCase().includes(query) && !entity.id.toLowerCase().includes(query)) return false;
      if (!showAds && entity.type !== 'campaign') return false;
      if (!showAds && entity.type === 'campaign') return false;
      return true;
    });
  }

  function projectEntities(list: SimilarityEntity[]): Array<SimilarityEntity & { px: number; py: number; pz: number }> {
    const xVals = list.map((e) => getMeasure(e, axisX));
    const yVals = list.map((e) => getMeasure(e, axisY));
    const zVals = list.map((e) => getMeasure(e, axisZ));

    const minX = Math.min(...xVals, 0);
    const maxX = Math.max(...xVals, 1);
    const minY = Math.min(...yVals, 0);
    const maxY = Math.max(...yVals, 1);
    const minZ = Math.min(...zVals, 0);
    const maxZ = Math.max(...zVals, 1);

    return list.map((e) => ({
      ...e,
      px: normalize(getMeasure(e, axisX), minX, maxX),
      py: normalize(getMeasure(e, axisY), minY, maxY),
      pz: normalize(getMeasure(e, axisZ), minZ, maxZ),
    }));
  }

  function normalize(value: number, min: number, max: number): number {
    return ((value - min) / Math.max(max - min, 0.0001) - 0.5) * 180;
  }

  function applyPreset(name: string): void {
    const preset = presets.find((p) => p.name === name);
    if (!preset) return;
    axisX = preset.x;
    axisY = preset.y;
    axisZ = preset.z;
    colorBy = preset.colorBy;
    sizeBy = preset.sizeBy;
  }

  function formatTooltip(entity: SimilarityEntity): string[] {
    return [
      entity.name,
      `ДРР ${entity.measures.drr}% · ROI ${entity.measures.roi} · CR2 ${entity.measures.cr2}%`,
      `Выручка ${entity.measures.revenue.toLocaleString('ru-RU')} ₽ · Расход ${entity.measures.spend.toLocaleString('ru-RU')} ₽`,
    ];
  }

  function getColor(entity: SimilarityEntity & { px: number; py: number; pz: number }, min: number, max: number): THREE.Color {
    if (colorBy === 'entry_point') {
      if (entity.entry_point === 'Поиск') return new THREE.Color('#2563eb');
      if (entity.entry_point === 'Полка') return new THREE.Color('#16a34a');
      return new THREE.Color('#f97316');
    }
    const value = getMeasure(entity, colorBy);
    const t = (value - min) / Math.max(max - min, 0.0001);
    return new THREE.Color(`hsl(${(1 - t) * 140}, 65%, 48%)`);
  }

  function getSize(entity: SimilarityEntity & { px: number; py: number; pz: number }): number {
    return 0.65 + Math.min(1.8, Math.sqrt(getMeasure(entity, sizeBy)) / 130);
  }

  let tooltip = { visible: false, x: 0, y: 0, lines: [] as string[] };

  function ensureCtx(): void {
    if (canvas2d && !ctx) ctx = canvas2d.getContext('2d') as CanvasRenderingContext2D;
  }

  function draw2d(): void {
    ensureCtx();
    if (!ctx || mode !== '2D') return;
    ctx.clearRect(0, 0, width, height);
    ctx.fillStyle = '#f8fbff';
    ctx.fillRect(0, 0, width, height);

    const cVals = projected.map((e) => (colorBy === 'entry_point' ? 0 : getMeasure(e, colorBy)));
    const min = Math.min(...cVals, 0);
    const max = Math.max(...cVals, 1);

    projected.forEach((entity) => {
      const x = entity.px * scale2d + panX;
      const y = entity.py * scale2d + panY;
      const r = 3 + getSize(entity) * 3;
      ctx.beginPath();
      ctx.fillStyle = getColor(entity, min, max).getStyle();
      if (entity.type === 'campaign') {
        ctx.moveTo(x, y - r); ctx.lineTo(x + r, y); ctx.lineTo(x, y + r); ctx.lineTo(x - r, y); ctx.closePath();
      } else {
        ctx.arc(x, y, r, 0, Math.PI * 2);
      }
      ctx.fill();
    });
  }

  function pick2d(mx: number, my: number): (SimilarityEntity & { px: number; py: number; pz: number }) | null {
    let best: (SimilarityEntity & { px: number; py: number; pz: number }) | null = null;
    let bestD = Infinity;
    projected.forEach((entity) => {
      const x = entity.px * scale2d + panX;
      const y = entity.py * scale2d + panY;
      const d = Math.hypot(mx - x, my - y);
      if (d < bestD && d < 14) { best = entity; bestD = d; }
    });
    return best;
  }

  function onMove2d(event: MouseEvent): void {
    if (mode !== '2D') return;
    const rect = canvas2d.getBoundingClientRect();
    const mx = event.clientX - rect.left;
    const my = event.clientY - rect.top;
    const picked = pick2d(mx, my);
    if (picked) tooltip = { visible: true, x: mx + 12, y: my + 12, lines: formatTooltip(picked) };
    else tooltip.visible = false;
    if (dragging) {
      panX = event.clientX - dragStart.x;
      panY = event.clientY - dragStart.y;
    }
  }

  let renderer: THREE.WebGLRenderer;
  let scene: THREE.Scene;
  let camera: THREE.PerspectiveCamera;
  let controls: OrbitControls;
  let buyersMesh: THREE.InstancedMesh;
  let entitiesMesh: THREE.InstancedMesh;
  let vectors = new THREE.Group();
  let anim = 0;
  const raycaster = new THREE.Raycaster();
  const ndc = new THREE.Vector2();
  let renderPoints: BuyerPoint[] = [];
  let renderEntities: Array<SimilarityEntity & { px: number; py: number; pz: number }> = [];

  function init3d(): void {
    scene = new THREE.Scene();
    scene.background = new THREE.Color('#f8fbff');
    camera = new THREE.PerspectiveCamera(52, width / height, 0.1, 1800);
    camera.position.set(0, 80, 210);
    renderer = new THREE.WebGLRenderer({ antialias: true });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    renderer.setSize(width, height);
    container3d.innerHTML = '';
    container3d.appendChild(renderer.domElement);

    controls = new OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.08;

    scene.add(new THREE.AmbientLight('#ffffff', 0.95));
    const light = new THREE.DirectionalLight('#ffffff', 0.35);
    light.position.set(80, 120, 75);
    scene.add(light);
    scene.add(new THREE.GridHelper(260, 16, '#d6deea', '#eaf0f7'));
    scene.add(vectors);

    renderer.domElement.addEventListener('mousemove', onMove3d);
    animate();
  }

  function clearMesh(mesh?: THREE.InstancedMesh): void {
    if (!mesh) return;
    scene.remove(mesh);
    mesh.geometry.dispose();
    (mesh.material as THREE.Material).dispose();
  }

  function rebuild3d(): void {
    if (mode !== '3D' || !scene) return;
    clearMesh(buyersMesh);
    clearMesh(entitiesMesh);
    vectors.clear();

    renderPoints = showBuyers ? (demoMode ? buyersDemo.points : buyersFull.points) : [];
    renderEntities = showAds ? projected : [];

    if (renderPoints.length > 0) {
      buyersMesh = new THREE.InstancedMesh(
        new THREE.SphereGeometry(0.7, 6, 6),
        new THREE.MeshBasicMaterial({ color: '#60a5fa', transparent: true, opacity: 0.2 }),
        renderPoints.length
      );
      const d = new THREE.Object3D();
      renderPoints.forEach((p, i) => { d.position.set(p.x, p.y, p.z); d.updateMatrix(); buyersMesh.setMatrixAt(i, d.matrix); });
      scene.add(buyersMesh);
    }

    if (renderEntities.length > 0) {
      const cVals = renderEntities.map((e) => (colorBy === 'entry_point' ? 0 : getMeasure(e, colorBy)));
      const min = Math.min(...cVals, 0);
      const max = Math.max(...cVals, 1);
      entitiesMesh = new THREE.InstancedMesh(
        new THREE.OctahedronGeometry(1.05, 0),
        new THREE.MeshBasicMaterial({ vertexColors: true }),
        renderEntities.length
      );
      entitiesMesh.instanceColor = new THREE.InstancedBufferAttribute(new Float32Array(renderEntities.length * 3), 3);
      const d = new THREE.Object3D();
      renderEntities.forEach((e, i) => {
        d.position.set(e.px, e.py, e.pz);
        const s = getSize(e);
        d.scale.set(s, s, s);
        d.rotation.set(0.2, 0.5, 0.1);
        d.updateMatrix();
        entitiesMesh.setMatrixAt(i, d.matrix);
        entitiesMesh.setColorAt(i, getColor(e, min, max));

        const cluster = (demoMode ? buyersDemo.clusters : buyersFull.clusters).find((c) => c.name === e.targetCluster);
        if (cluster) {
          const start = new THREE.Vector3(e.px, e.py, e.pz);
          const end = new THREE.Vector3(cluster.center.x, cluster.center.y, cluster.center.z);
          const dist = start.distanceTo(end);
          const t = Math.min(1, dist / 130);
          const color = new THREE.Color(`hsl(${(1 - t) * 120}, 70%, 45%)`);
          vectors.add(new THREE.Line(new THREE.BufferGeometry().setFromPoints([start, end]), new THREE.LineBasicMaterial({ color, transparent: true, opacity: 0.65 })));
        }
      });
      scene.add(entitiesMesh);
    }
  }

  function onMove3d(event: MouseEvent): void {
    if (mode !== '3D' || !entitiesMesh) return;
    const rect = renderer.domElement.getBoundingClientRect();
    ndc.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
    ndc.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;
    raycaster.setFromCamera(ndc, camera);
    const hit = raycaster.intersectObject(entitiesMesh, true)[0];
    if (hit && hit.instanceId !== undefined) {
      tooltip = { visible: true, x: event.clientX - rect.left + 12, y: event.clientY - rect.top + 12, lines: formatTooltip(renderEntities[hit.instanceId]) };
    } else tooltip.visible = false;
  }

  function animate(): void {
    controls?.update();
    if (buyersMesh) {
      const dist = camera.position.length();
      if (dist > 300) buyersMesh.count = Math.floor(renderPoints.length * 0.3);
      else if (dist > 230) buyersMesh.count = Math.floor(renderPoints.length * 0.6);
      else buyersMesh.count = renderPoints.length;
    }
    renderer?.render(scene, camera);
    anim = requestAnimationFrame(animate);
  }

  function resetView(): void {
    if (mode === '2D') {
      scale2d = 1; panX = width / 2; panY = height / 2; return;
    }
    camera.position.set(0, 80, 210);
    controls.target.set(0, 0, 0);
    controls.update();
  }

  function onResize(): void {
    const rect = panel.getBoundingClientRect();
    width = Math.max(760, Math.floor(rect.width - 380));
    height = 560;
    if (renderer && camera) {
      renderer.setSize(width, height);
      camera.aspect = width / height;
      camera.updateProjectionMatrix();
    }
  }

  function onKey(event: KeyboardEvent): void {
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'k') {
      event.preventDefault(); searchInput.focus(); searchInput.select();
    }
  }

  onMount(() => {
    onResize();
    init3d();
    window.addEventListener('resize', onResize);
    window.addEventListener('keydown', onKey);
  });

  onDestroy(() => {
    window.removeEventListener('resize', onResize);
    window.removeEventListener('keydown', onKey);
    cancelAnimationFrame(anim);
    renderer?.dispose();
  });
</script>

<section class="panel graph-root" bind:this={panel}>
  <div class="header">
    <h2>Пространство</h2>
    <p>3D-граф сущностей по выбранным метрикам</p>
  </div>

  <div class="content">
    <div class="graph-wrap">
      <div class="status">
        Поля: {DATA_FIELDS.length} · Показано точек: {projected.length} · X: {formatFieldTitle(axisX)} · Y: {formatFieldTitle(axisY)} · Z: {formatFieldTitle(axisZ)}
      </div>
      <div class="stage">
        {#if mode === '2D'}
          <canvas
            bind:this={canvas2d}
            width={width}
            height={height}
            on:mousedown={(e) => { dragging = true; dragStart = { x: e.clientX - panX, y: e.clientY - panY }; }}
            on:mousemove={onMove2d}
            on:mouseup={() => { dragging = false; }}
            on:wheel|preventDefault={(e) => { scale2d = Math.max(0.2, Math.min(4, scale2d * (e.deltaY > 0 ? 0.9 : 1.1))); }}
          />
        {:else}
          <div bind:this={container3d} class="scene" />
        {/if}
        {#if tooltip.visible}
          <div class="tooltip" style={`left:${tooltip.x}px;top:${tooltip.y}px`}>
            {#each tooltip.lines as line}<div>{line}</div>{/each}
          </div>
        {/if}
      </div>
    </div>

    <aside class="control-panel">
      <section class="section">
        <h4>Точки</h4>
        <label>Точки на графе
          <select bind:value={entityMode}>
            <option>Артикулы (SKU)</option>
            <option>Предметы</option>
            <option>Категории</option>
            <option>Бренды</option>
            <option>Рекламные кампании (РК)</option>
          </select>
        </label>
        <label><input type="checkbox" bind:checked={demoMode} /> Демо-режим (10 SKU + 10 РК)</label>
      </section>

      <section class="section">
        <h4>Слои</h4>
        <label><input type="checkbox" bind:checked={showBuyers} /> Покупатели (факт)</label>
        <label><input type="checkbox" bind:checked={showAds} /> Реклама (управление)</label>
      </section>

      <section class="section">
        <h4>Координаты</h4>
        <label>Ось X
          <select bind:value={axisX}>{#each MEASURE_FIELDS as f}<option value={f.key}>{f.title}</option>{/each}</select>
        </label>
        <label>Ось Y
          <select bind:value={axisY}>{#each MEASURE_FIELDS as f}<option value={f.key}>{f.title}</option>{/each}</select>
        </label>
        <label>Ось Z
          <select bind:value={axisZ}>{#each MEASURE_FIELDS as f}<option value={f.key}>{f.title}</option>{/each}</select>
        </label>
        <small>Оси принимают только числовые метрики</small>
        <label>Окраска по
          <select bind:value={colorBy}>
            {#each MEASURE_FIELDS as f}<option value={f.key}>{f.title}</option>{/each}
            <option value="entry_point">Точка входа</option>
          </select>
        </label>
        <label>Размер по
          <select bind:value={sizeBy}>{#each MEASURE_FIELDS as f}<option value={f.key}>{f.title}</option>{/each}</select>
        </label>
        <label>Режим
          <select bind:value={mode}><option>2D</option><option>3D</option></select>
        </label>
        <label>Пресет
          <select bind:value={selectedPreset} on:change={() => applyPreset(selectedPreset)}>{#each presets as p}<option>{p.name}</option>{/each}</select>
        </label>
      </section>

      <section class="section">
        <h4>Период и фильтры</h4>
        <label>Период
          <select bind:value={period}>
            <option>7 дней</option><option>14 дней</option><option>30 дней</option><option>Выбрать даты</option>
          </select>
        </label>
        {#if period === 'Выбрать даты'}
          <div class="dates"><input type="date" bind:value={fromDate} /><input type="date" bind:value={toDate} /></div>
        {/if}
        <label>Категория <select bind:value={filterCategory}>{#each categoryOptions as o}<option>{o}</option>{/each}</select></label>
        <label>Предмет <select bind:value={filterSubject}>{#each subjectOptions as o}<option>{o}</option>{/each}</select></label>
        <label>Бренд <select bind:value={filterBrand}>{#each brandOptions as o}<option>{o}</option>{/each}</select></label>
        <label>Тип РК <select bind:value={filterRkType}>{#each rkTypeOptions as o}<option>{o}</option>{/each}</select></label>
        <label>Выручка &gt; <input type="number" min="0" bind:value={minRevenue} /></label>
        <label>ДРР &lt; <input type="number" min="0" max="100" bind:value={maxDrr} /></label>
        <input bind:this={searchInput} bind:value={search} placeholder="Поиск SKU / РК" />
        <button on:click={resetView}>Сбросить вид</button>
      </section>
    </aside>
  </div>
</section>

<style>
  .graph-root { display:flex; flex-direction:column; gap:10px; }
  .header h2 { margin:0; font-size:24px; }
  .header p { margin:0; color:#64748b; font-size:12px; }
  .content { display:grid; grid-template-columns: minmax(0, 1fr) 340px; gap:12px; }
  .graph-wrap { min-width:0; }
  .status { font-size:12px; color:#64748b; margin-bottom:6px; }
  .stage { position:relative; }
  canvas, .scene { width:100%; height:560px; border:1px solid #e2e8f0; border-radius:16px; background:#f8fbff; }
  .control-panel { border:1px solid #e2e8f0; border-radius:14px; padding:10px; background:#fbfdff; display:flex; flex-direction:column; gap:10px; }
  .section { border:1px solid #e7edf6; border-radius:10px; padding:8px; display:flex; flex-direction:column; gap:6px; }
  .section h4 { margin:0; font-size:12px; color:#334155; }
  .section label { display:flex; justify-content:space-between; align-items:center; gap:6px; font-size:12px; color:#334155; }
  .section select,.section input,.section button { border:1px solid #dbe4f0; border-radius:10px; padding:6px 8px; background:#fff; font-size:12px; }
  .section button { cursor:pointer; }
  .dates { display:flex; gap:6px; }
  .tooltip { position:absolute; pointer-events:none; background:rgba(15,23,42,.94); color:#f8fafc; font-size:12px; padding:10px; border-radius:10px; max-width:360px; }
  small { color:#64748b; font-size:11px; }
</style>
