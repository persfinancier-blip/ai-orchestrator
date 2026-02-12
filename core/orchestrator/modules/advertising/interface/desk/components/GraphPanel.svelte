<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import * as THREE from 'three';
  import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
  import type { BuyerPoint, ColorField, EntityMode, MeasureField, Mode3D, PeriodMode, SimilarityEntity } from '../data/mockGraph';
  import { DATA_FIELDS, MEASURE_FIELDS, aggregateBySubject, formatFieldTitle, generateBuyerCloud, getMeasure } from '../data/mockGraph';

  export let entities: SimilarityEntity[] = [];

  type Preset = {
    name: string;
    x: MeasureField;
    y: MeasureField;
    z: MeasureField;
    colorBy: ColorField;
    sizeBy: MeasureField;
  };

  const presets: Preset[] = [
    { name: 'Попадание по входам', x: 'revenue', y: 'spend', z: 'search_share', colorBy: 'entry_gap', sizeBy: 'orders' },
    { name: 'Эффективность', x: 'revenue', y: 'spend', z: 'roi', colorBy: 'drr', sizeBy: 'orders' },
    { name: 'Конверсия', x: 'clicks', y: 'orders', z: 'cr2', colorBy: 'roi', sizeBy: 'revenue' },
  ];

  let selectedPreset = presets[1].name;
  let entityMode: EntityMode = 'SKU';
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
  let filterEntryPoint = 'Все';
  let minRevenue = 0;
  let maxDrr = 100;

  let mode: Mode3D = '3D';
  let search = '';

  let container3d: HTMLDivElement;
  let canvas2d: HTMLCanvasElement;
  let panel: HTMLDivElement;
  let searchInput: HTMLInputElement;
  let ctx: CanvasRenderingContext2D;

  let width = 980;
  let height = 560;
  let scale2d = 1;
  let panX = width / 2;
  let panY = height / 2;
  let dragging = false;
  let dragStart = { x: 0, y: 0 };

  const buyers = generateBuyerCloud(220);

  $: categoryOptions = uniqueValues('category');
  $: subjectOptions = uniqueValues('subject');
  $: brandOptions = uniqueValues('brand');
  $: rkTypeOptions = uniqueValues('rkType');
  $: entryPointOptions = uniqueValues('entry_point');

  $: transformed = transformEntities();
  $: filtered = applyFilters(transformed);
  $: projected = projectEntities(filtered);
  $: if (mode === '2D') draw2d();
  $: if (mode === '3D') rebuild3d();

  function uniqueValues(field: 'category' | 'subject' | 'brand' | 'rkType' | 'entry_point'): string[] {
    return ['Все', ...Array.from(new Set(entities.map((entity) => String(entity[field]))))];
  }

  function transformEntities(): SimilarityEntity[] {
    if (entityMode === 'Рекламная кампания (РК)') return entities.filter((entity) => entity.type === 'campaign');
    if (entityMode === 'Предмет') return aggregateBySubject(entities).filter((entity) => entity.type === 'item' || entity.type === 'campaign');
    return entities;
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
      if (!showAds && entity.type !== 'campaign' && entity.type !== 'item') return false;
      if (query && !entity.name.toLowerCase().includes(query) && !entity.id.toLowerCase().includes(query)) return false;
      if (!inPeriod(entity.date)) return false;
      if (filterCategory !== 'Все' && entity.category !== filterCategory) return false;
      if (filterSubject !== 'Все' && entity.subject !== filterSubject) return false;
      if (filterBrand !== 'Все' && entity.brand !== filterBrand) return false;
      if (filterRkType !== 'Все' && entity.rkType !== filterRkType) return false;
      if (filterEntryPoint !== 'Все' && entity.entry_point !== filterEntryPoint) return false;
      if (entity.measures.revenue < minRevenue) return false;
      if (entity.measures.drr > maxDrr) return false;
      if (!showAds) return false;
      return true;
    });
  }

  function projectEntities(list: SimilarityEntity[]): Array<SimilarityEntity & { px: number; py: number; pz: number }> {
    const xVals = list.map((entity) => getMeasure(entity, axisX));
    const yVals = list.map((entity) => getMeasure(entity, axisY));
    const zVals = list.map((entity) => getMeasure(entity, axisZ));

    const minX = Math.min(...xVals, 0);
    const maxX = Math.max(...xVals, 1);
    const minY = Math.min(...yVals, 0);
    const maxY = Math.max(...yVals, 1);
    const minZ = Math.min(...zVals, 0);
    const maxZ = Math.max(...zVals, 1);

    return list.map((entity) => ({
      ...entity,
      px: normalize(getMeasure(entity, axisX), minX, maxX),
      py: normalize(getMeasure(entity, axisY), minY, maxY),
      pz: normalize(getMeasure(entity, axisZ), minZ, maxZ),
    }));
  }

  function normalize(value: number, min: number, max: number): number {
    return ((value - min) / Math.max(max - min, 0.0001) - 0.5) * 180;
  }

  function applyPreset(name: string): void {
    const preset = presets.find((item) => item.name === name);
    if (!preset) return;
    axisX = preset.x;
    axisY = preset.y;
    axisZ = preset.z;
    colorBy = preset.colorBy;
    sizeBy = preset.sizeBy;
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
    return 0.6 + Math.min(1.8, Math.sqrt(getMeasure(entity, sizeBy)) / 130);
  }

  function formatTooltip(entity: SimilarityEntity): string[] {
    return [
      entity.name,
      entity.type === 'campaign' ? 'Рекламная кампания' : entityMode,
      `ДРР ${entity.measures.drr}% · ROI ${entity.measures.roi} · CR2 ${entity.measures.cr2}%`,
      `Выручка ${entity.measures.revenue.toLocaleString('ru-RU')} ₽ · Расход ${entity.measures.spend.toLocaleString('ru-RU')} ₽`,
    ];
  }

  let tooltip = { visible: false, x: 0, y: 0, lines: [] as string[] };
  let hovered: (SimilarityEntity & { px: number; py: number; pz: number }) | null = null;

  // 2D
  function ensureCtx(): void {
    if (canvas2d && !ctx) ctx = canvas2d.getContext('2d') as CanvasRenderingContext2D;
  }

  function draw2d(): void {
    ensureCtx();
    if (!ctx || mode !== '2D') return;
    ctx.clearRect(0, 0, width, height);
    ctx.fillStyle = '#f8fbff';
    ctx.fillRect(0, 0, width, height);

    const colorVals = projected.map((entity) => (colorBy === 'entry_point' ? 0 : getMeasure(entity, colorBy as MeasureField)));
    const min = Math.min(...colorVals, 0);
    const max = Math.max(...colorVals, 1);

    projected.forEach((entity) => {
      const sx = entity.px * scale2d + panX;
      const sy = entity.py * scale2d + panY;
      const size = 3 + getSize(entity) * 3;
      const color = getColor(entity, min, max).getStyle();

      ctx.beginPath();
      ctx.fillStyle = color;
      if (entity.type === 'campaign') {
        ctx.moveTo(sx, sy - size);
        ctx.lineTo(sx + size, sy);
        ctx.lineTo(sx, sy + size);
        ctx.lineTo(sx - size, sy);
        ctx.closePath();
      } else {
        ctx.arc(sx, sy, size, 0, Math.PI * 2);
      }
      ctx.fill();
    });
  }

  function pick2d(mx: number, my: number): (SimilarityEntity & { px: number; py: number; pz: number }) | null {
    let best: (SimilarityEntity & { px: number; py: number; pz: number }) | null = null;
    let bestDist = Infinity;
    projected.forEach((entity) => {
      const sx = entity.px * scale2d + panX;
      const sy = entity.py * scale2d + panY;
      const dist = Math.hypot(mx - sx, my - sy);
      if (dist < bestDist && dist < 16) {
        best = entity;
        bestDist = dist;
      }
    });
    return best;
  }

  function onCanvasMove(event: MouseEvent): void {
    if (mode !== '2D') return;
    const rect = canvas2d.getBoundingClientRect();
    const mx = event.clientX - rect.left;
    const my = event.clientY - rect.top;
    const picked = pick2d(mx, my);
    if (picked) {
      hovered = picked;
      tooltip = { visible: true, x: mx + 12, y: my + 12, lines: formatTooltip(picked) };
    } else tooltip.visible = false;
    if (dragging) {
      panX = event.clientX - dragStart.x;
      panY = event.clientY - dragStart.y;
    }
  }

  // 3D
  let renderer: THREE.WebGLRenderer;
  let scene: THREE.Scene;
  let camera: THREE.PerspectiveCamera;
  let controls: OrbitControls;
  let buyerMesh: THREE.InstancedMesh;
  let entityMesh: THREE.InstancedMesh;
  let vectors = new THREE.Group();
  let anim = 0;
  const raycaster = new THREE.Raycaster();
  const ndc = new THREE.Vector2();

  let buyerVisible: BuyerPoint[] = [];
  let entityVisible: Array<SimilarityEntity & { px: number; py: number; pz: number }> = [];

  function init3d(): void {
    scene = new THREE.Scene();
    scene.background = new THREE.Color('#f8fbff');
    camera = new THREE.PerspectiveCamera(52, width / height, 0.1, 1800);
    camera.position.set(0, 80, 210);
    renderer = new THREE.WebGLRenderer({ antialias: true });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    renderer.setSize(width, height);
    container3d.appendChild(renderer.domElement);

    controls = new OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.08;

    scene.add(new THREE.AmbientLight('#ffffff', 0.95));
    const light = new THREE.DirectionalLight('#ffffff', 0.38);
    light.position.set(80, 120, 75);
    scene.add(light);
    scene.add(new THREE.GridHelper(260, 16, '#d6deea', '#eaf0f7'));
    scene.add(vectors);

    renderer.domElement.addEventListener('mousemove', onMove3d);
    renderer.domElement.addEventListener('wheel', () => {}, { passive: true });
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
    clearMesh(buyerMesh);
    clearMesh(entityMesh);
    vectors.clear();

    buyerVisible = showBuyers ? buyers.points : [];
    entityVisible = showAds ? projected : [];

    if (buyerVisible.length > 0) {
      buyerMesh = new THREE.InstancedMesh(
        new THREE.SphereGeometry(0.7, 6, 6),
        new THREE.MeshBasicMaterial({ color: '#60a5fa', transparent: true, opacity: 0.2 }),
        buyerVisible.length
      );
      const dummy = new THREE.Object3D();
      buyerVisible.forEach((point, i) => {
        dummy.position.set(point.x, point.y, point.z);
        dummy.updateMatrix();
        buyerMesh.setMatrixAt(i, dummy.matrix);
      });
      scene.add(buyerMesh);
    }

    if (entityVisible.length > 0) {
      const colorVals = entityVisible.map((entity) => (colorBy === 'entry_point' ? 0 : getMeasure(entity, colorBy as MeasureField)));
      const min = Math.min(...colorVals, 0);
      const max = Math.max(...colorVals, 1);
      entityMesh = new THREE.InstancedMesh(
        new THREE.OctahedronGeometry(1.05, 0),
        new THREE.MeshBasicMaterial({ vertexColors: true }),
        entityVisible.length
      );
      entityMesh.instanceColor = new THREE.InstancedBufferAttribute(new Float32Array(entityVisible.length * 3), 3);
      const dummy = new THREE.Object3D();
      entityVisible.forEach((entity, i) => {
        dummy.position.set(entity.px, entity.py, entity.pz);
        const scale = getSize(entity);
        dummy.scale.set(scale, scale, scale);
        dummy.rotation.set(0.2, 0.5, 0.1);
        dummy.updateMatrix();
        entityMesh.setMatrixAt(i, dummy.matrix);
        entityMesh.setColorAt(i, getColor(entity, min, max));

        const cluster = buyers.clusters.find((c) => c.name === entity.targetCluster);
        if (cluster) {
          const start = new THREE.Vector3(entity.px, entity.py, entity.pz);
          const end = new THREE.Vector3(cluster.center.x, cluster.center.y, cluster.center.z);
          const dist = start.distanceTo(end);
          const t = Math.min(1, dist / 130);
          const color = new THREE.Color(`hsl(${(1 - t) * 120}, 70%, 45%)`);
          const line = new THREE.Line(new THREE.BufferGeometry().setFromPoints([start, end]), new THREE.LineBasicMaterial({ color, transparent: true, opacity: 0.65 }));
          vectors.add(line);
        }
      });
      scene.add(entityMesh);
    }
  }

  function onMove3d(event: MouseEvent): void {
    if (mode !== '3D' || !entityMesh) return;
    const rect = renderer.domElement.getBoundingClientRect();
    ndc.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
    ndc.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;
    raycaster.setFromCamera(ndc, camera);
    const hits = raycaster.intersectObject(entityMesh, true);
    const hit = hits[0];
    if (hit && hit.instanceId !== undefined) {
      const entity = entityVisible[hit.instanceId];
      hovered = entity;
      tooltip = { visible: true, x: event.clientX - rect.left + 12, y: event.clientY - rect.top + 12, lines: formatTooltip(entity) };
    } else {
      tooltip.visible = false;
    }
  }

  function animate(): void {
    controls?.update();
    if (buyerMesh) {
      const dist = camera.position.length();
      if (dist > 300) buyerMesh.count = Math.floor(buyerVisible.length * 0.28);
      else if (dist > 230) buyerMesh.count = Math.floor(buyerVisible.length * 0.58);
      else buyerMesh.count = buyerVisible.length;
    }
    renderer?.render(scene, camera);
    anim = requestAnimationFrame(animate);
  }

  function resetView(): void {
    if (mode === '2D') {
      scale2d = 1;
      panX = width / 2;
      panY = height / 2;
      return;
    }
    camera.position.set(0, 80, 210);
    controls.target.set(0, 0, 0);
    controls.update();
  }

  function onResize(): void {
    const rect = panel.getBoundingClientRect();
    width = Math.max(900, Math.floor(rect.width - 24));
    height = 560;
    if (renderer && camera) {
      renderer.setSize(width, height);
      camera.aspect = width / height;
      camera.updateProjectionMatrix();
    }
  }

  function onKey(event: KeyboardEvent): void {
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'k') {
      event.preventDefault();
      searchInput.focus();
      searchInput.select();
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
  <div class="controls">
    <div class="group">
      <h4>Пресет</h4>
      <select bind:value={selectedPreset} on:change={() => applyPreset(selectedPreset)}>
        {#each presets as preset}<option>{preset.name}</option>{/each}
      </select>
    </div>

    <div class="group">
      <h4>Точки графа</h4>
      <label>Сущность
        <select bind:value={entityMode}>
          <option>SKU</option>
          <option>Предмет</option>
          <option>Рекламная кампания (РК)</option>
        </select>
      </label>
      <div class="checks">
        <label><input type="checkbox" bind:checked={showBuyers} /> Покупатели (факт)</label>
        <label><input type="checkbox" bind:checked={showAds} /> Реклама (управление)</label>
      </div>
    </div>

    <div class="group">
      <h4>Оси</h4>
      <label>Ось X
        <select bind:value={axisX}>{#each MEASURE_FIELDS as field}<option value={field.key}>{field.title}</option>{/each}</select>
      </label>
      <small>Только числовые метрики</small>
      <label>Ось Y
        <select bind:value={axisY}>{#each MEASURE_FIELDS as field}<option value={field.key}>{field.title}</option>{/each}</select>
      </label>
      <small>Только числовые метрики</small>
      <label>Ось Z
        <select bind:value={axisZ}>{#each MEASURE_FIELDS as field}<option value={field.key}>{field.title}</option>{/each}</select>
      </label>
      <small>Только числовые метрики</small>
    </div>

    <div class="group">
      <h4>Вид</h4>
      <label>Окраска по
        <select bind:value={colorBy}>
          {#each MEASURE_FIELDS as field}<option value={field.key}>{field.title}</option>{/each}
          <option value="entry_point">Точка входа</option>
        </select>
      </label>
      <label>Размер по
        <select bind:value={sizeBy}>{#each MEASURE_FIELDS as field}<option value={field.key}>{field.title}</option>{/each}</select>
      </label>
      <label>Режим
        <select bind:value={mode}><option>2D</option><option>3D</option></select>
      </label>
    </div>

    <div class="group">
      <h4>Период</h4>
      <select bind:value={period}>
        <option>7 дней</option>
        <option>14 дней</option>
        <option>30 дней</option>
        <option>Выбрать даты</option>
      </select>
      {#if period === 'Выбрать даты'}
        <div class="dates">
          <input type="date" bind:value={fromDate} />
          <input type="date" bind:value={toDate} />
        </div>
      {/if}
    </div>

    <div class="group">
      <h4>Фильтры</h4>
      <div class="chips">
        <label>Категория <select bind:value={filterCategory}>{#each categoryOptions as opt}<option>{opt}</option>{/each}</select></label>
        <label>Предмет <select bind:value={filterSubject}>{#each subjectOptions as opt}<option>{opt}</option>{/each}</select></label>
        <label>Бренд <select bind:value={filterBrand}>{#each brandOptions as opt}<option>{opt}</option>{/each}</select></label>
        <label>Тип РК <select bind:value={filterRkType}>{#each rkTypeOptions as opt}<option>{opt}</option>{/each}</select></label>
        <label>Точка входа <select bind:value={filterEntryPoint}>{#each entryPointOptions as opt}<option>{opt}</option>{/each}</select></label>
      </div>
      <div class="numbers">
        <label>Выручка &gt; <input type="number" min="0" bind:value={minRevenue} /></label>
        <label>ДРР &lt; <input type="number" min="0" max="100" bind:value={maxDrr} /></label>
      </div>
    </div>

    <div class="group search">
      <input bind:this={searchInput} bind:value={search} placeholder="Поиск SKU / РК" />
      <button on:click={resetView}>Сбросить вид</button>
    </div>
  </div>

  <div class="status">
    Поля: {DATA_FIELDS.length} · Показано точек: {projected.length} · X: {formatFieldTitle(axisX)} · Y: {formatFieldTitle(axisY)} · Z: {formatFieldTitle(axisZ)}
  </div>

  <div class="stage">
    {#if mode === '2D'}
      <canvas
        bind:this={canvas2d}
        width={width}
        height={height}
        on:mousedown={(event) => { dragging = true; dragStart = { x: event.clientX - panX, y: event.clientY - panY }; }}
        on:mousemove={onCanvasMove}
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
</section>

<style>
  .graph-root { display:flex; flex-direction:column; gap:10px; }
  .controls { display:grid; grid-template-columns:repeat(3, minmax(240px, 1fr)); gap:10px; }
  .group { border:1px solid #e2e8f0; border-radius:12px; padding:8px; background:#fbfdff; display:flex; flex-direction:column; gap:6px; }
  .group h4 { margin:0; font-size:12px; color:#475569; }
  .group label { display:flex; justify-content:space-between; align-items:center; gap:6px; font-size:12px; color:#334155; }
  .group select, .group input { border:1px solid #dbe4f0; border-radius:10px; padding:5px 8px; font-size:12px; background:#fff; }
  .checks { display:flex; gap:10px; }
  .checks label { font-size:12px; }
  small { color:#64748b; font-size:11px; }
  .chips { display:grid; grid-template-columns:1fr 1fr; gap:6px; }
  .numbers { display:flex; gap:8px; }
  .search { grid-column: span 3; display:flex; flex-direction:row; align-items:center; }
  .search input { flex:1; }
  .search button { border:1px solid #cbd5e1; border-radius:10px; background:#fff; padding:7px 12px; }
  .dates { display:flex; gap:6px; }
  .status { font-size:12px; color:#64748b; }
  .stage { position:relative; }
  canvas, .scene { width:100%; height:560px; border:1px solid #e2e8f0; border-radius:16px; background:#f8fbff; }
  .tooltip { position:absolute; pointer-events:none; background:rgba(15,23,42,.94); color:#f8fafc; font-size:12px; padding:10px; border-radius:10px; max-width:360px; }
</style>
