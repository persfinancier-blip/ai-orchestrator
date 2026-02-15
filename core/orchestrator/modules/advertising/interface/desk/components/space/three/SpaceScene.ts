import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
import { CSS2DRenderer, CSS2DObject } from 'three/examples/jsm/renderers/CSS2DRenderer.js';

import type { ShowcaseField } from '../../data/showcaseStore';
import type { SpacePoint } from '../types';
import { buildBBox, normalizeBBox, calcMax, sanitizePoints, formatValueByMetric } from '../pipeline';

export type SpaceSceneTheme = {
  bg: string;
  edge: string;
  pointColor?: string;
};

export type SpaceSceneDeps = {
  fieldName: (code: string) => string;
  getFields: () => ShowcaseField[];
};

export type SpaceSceneCallbacks = {
  onTooltip: (payload: { visible: boolean; x: number; y: number; lines: string[] }) => void;
  onAxisRemove?: (axis: 'x' | 'y' | 'z') => void;
};

export class SpaceScene {
  private deps: SpaceSceneDeps;
  private cb: SpaceSceneCallbacks;

  private container: HTMLDivElement | null = null;

  private renderer: THREE.WebGLRenderer | null = null;
  private labelRenderer: CSS2DRenderer | null = null;

  private scene: THREE.Scene | null = null;
  private camera: THREE.PerspectiveCamera | null = null;
  private controls: OrbitControls | null = null;

  private planeGroup = new THREE.Group();
  private edgeLabelGroup = new THREE.Group();
  private cornerFrame: THREE.LineSegments<THREE.BufferGeometry, THREE.LineBasicMaterial> | null = null;
  private axisLabelObjects: CSS2DObject[] = [];

  private pointsMesh?: THREE.InstancedMesh;
  private clusterCloudMesh?: THREE.InstancedMesh; // ✅ “облако” кластера

  private renderPoints: SpacePoint[] = [];

  private anim = 0;

  private theme: Required<SpaceSceneTheme> = {
    bg: '#ffffff',
    edge: '#334155',
    pointColor: '#22c55e'
  };

  private raycaster = new THREE.Raycaster();
  private ndc = new THREE.Vector2();

  private axisCodes = { x: '', y: '', z: '' };

  public constructor(deps: SpaceSceneDeps, cb: SpaceSceneCallbacks) {
    this.deps = deps;
    this.cb = cb;
  }

  public init(container: HTMLDivElement, opts?: { height?: number }): void {
    this.container = container;

    const width = container.clientWidth;
    const height = opts?.height ?? 560;

    this.scene = new THREE.Scene();
    this.scene.background = new THREE.Color(this.theme.bg);

    this.camera = new THREE.PerspectiveCamera(52, width / height, 0.1, 1500);
    this.camera.position.set(0, 80, 220);

    this.renderer = new THREE.WebGLRenderer({ antialias: true });
    this.renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    this.renderer.setSize(width, height);

    container.innerHTML = '';
    container.appendChild(this.renderer.domElement);

    this.controls = new OrbitControls(this.camera, this.renderer.domElement);
    this.controls.enableDamping = true;
    this.controls.dampingFactor = 0.08;

    this.scene.add(new THREE.AmbientLight('#ffffff', 0.95));
    const light = new THREE.DirectionalLight('#ffffff', 0.35);
    light.position.set(80, 120, 75);
    this.scene.add(light);

    this.scene.add(this.planeGroup);
    this.scene.add(this.edgeLabelGroup);

    this.labelRenderer = new CSS2DRenderer();
    this.labelRenderer.setSize(width, height);
    this.labelRenderer.domElement.style.position = 'absolute';
    this.labelRenderer.domElement.style.left = '0';
    this.labelRenderer.domElement.style.top = '0';
    this.labelRenderer.domElement.style.pointerEvents = 'none';
    this.labelRenderer.domElement.className = 'label-layer';
    container.appendChild(this.labelRenderer.domElement);

    this.renderer.domElement.addEventListener('mousemove', this.onMove3d);
    this.animate();
  }

  public setTheme(theme: SpaceSceneTheme): void {
    this.theme = {
      bg: theme.bg,
      edge: theme.edge,
      pointColor: theme.pointColor ?? this.theme.pointColor
    };

    if (this.scene) this.scene.background = new THREE.Color(this.theme.bg);

    this.rebuildPlanesForCurrentPoints();
  }

  public setAxisCodes(axis: { x: string; y: string; z: string }): void {
    this.axisCodes = axis;
    this.rebuildPlanesForCurrentPoints();
  }

  public resize(height = 560): void {
    if (!this.renderer || !this.camera || !this.container) return;

    const width = this.container.clientWidth;
    this.renderer.setSize(width, height);
    this.labelRenderer?.setSize(width, height);
    this.camera.aspect = width / height;
    this.camera.updateProjectionMatrix();
  }

  public resetView(): void {
    if (!this.camera || !this.controls) return;
    this.camera.position.set(0, 80, 220);
    this.controls.target.set(0, 0, 0);
    this.controls.update();
  }

  // -----------------------
  // ✅ helpers for clusters
  // -----------------------
  private isClusterPoint(p: SpacePoint): boolean {
    return Boolean((p as any)?.isCluster);
  }

  private clusterCount(p: SpacePoint): number {
    const n = Number((p as any)?.clusterCount);
    return Number.isFinite(n) && n > 0 ? n : 1;
  }

  private clusterSpan(p: SpacePoint): { x: number; y: number; z: number } | null {
    const s = (p as any)?.span;
    const sx = Number(s?.x);
    const sy = Number(s?.y);
    const sz = Number(s?.z);
    if (!Number.isFinite(sx) || !Number.isFinite(sy) || !Number.isFinite(sz)) return null;
    if (sx <= 0 && sy <= 0 && sz <= 0) return null;
    return { x: Math.max(0.001, sx), y: Math.max(0.001, sy), z: Math.max(0.001, sz) };
  }

  // sqrt(n)/log(n+1) с клампом
  private pointScale(p: SpacePoint): number {
    if (!this.isClusterPoint(p)) return 1;

    const n = this.clusterCount(p);
    const denom = Math.log(n + 1);
    const raw = denom > 0 ? Math.sqrt(n) / denom : 1;

    const s = 0.55 * raw;
    return Math.min(6, Math.max(1.35, s));
  }

  public setPoints(points: SpacePoint[]): { renderedCount: number; bboxLabel: string } {
    if (!this.scene || !this.camera || !this.controls || !this.renderer) {
      return { renderedCount: 0, bboxLabel: '—' };
    }

    this.scene.background = new THREE.Color(this.theme.bg);

    const colorById = new Map<string, string | undefined>();
    for (const p of points ?? []) colorById.set(p.id, p.color);

    this.clearMesh(this.pointsMesh);
    this.pointsMesh = undefined;

    this.clearMesh(this.clusterCloudMesh);
    this.clusterCloudMesh = undefined;

    const sanitized = sanitizePoints(points);

    const renderable = sanitized.map((p) => ({
      ...p,
      color: colorById.get(p.id) ?? p.color
    }));

    this.renderPoints = renderable;

    // -----------------------
    // 1) MAIN POINTS (spheres)
    // -----------------------
    if (this.renderPoints.length) {
      this.pointsMesh = new THREE.InstancedMesh(
        new THREE.SphereGeometry(1.55, 10, 10),
        new THREE.MeshBasicMaterial({ vertexColors: true }),
        this.renderPoints.length
      );

      const o = new THREE.Object3D();
      this.renderPoints.forEach((point, idx) => {
        const s = this.pointScale(point);
        o.position.set(point.x, point.y, point.z);
        o.scale.set(s, s, s);
        o.updateMatrix();
        this.pointsMesh!.setMatrixAt(idx, o.matrix);
      });

      const c = new THREE.Color();
      for (let i = 0; i < this.renderPoints.length; i += 1) {
        const hex = this.renderPoints[i].color ?? this.theme.pointColor;
        c.set(hex);
        this.pointsMesh!.setColorAt(i, c);
      }
      this.pointsMesh!.instanceColor!.needsUpdate = true;

      this.scene.add(this.pointsMesh);
    }

    // ---------------------------------------
    // 2) CLUSTER CLOUDS (ellipsoid by bbox span)
    // ---------------------------------------
    const clusterIdx: number[] = [];
    for (let i = 0; i < this.renderPoints.length; i += 1) {
      if (this.isClusterPoint(this.renderPoints[i]) && this.clusterSpan(this.renderPoints[i])) clusterIdx.push(i);
    }

    if (clusterIdx.length) {
      // ✅ сфера -> масштабируем в эллипсоид (по span)
      const geom = new THREE.SphereGeometry(1, 14, 14);
      const mat = new THREE.MeshBasicMaterial({
        transparent: true,
        opacity: 0.10,
        depthWrite: false,
        vertexColors: true,
        side: THREE.DoubleSide
      });

      this.clusterCloudMesh = new THREE.InstancedMesh(geom, mat, clusterIdx.length);

      const o = new THREE.Object3D();
      const col = new THREE.Color();

      for (let ii = 0; ii < clusterIdx.length; ii += 1) {
        const p = this.renderPoints[clusterIdx[ii]];
        const span = this.clusterSpan(p);
        if (!span) continue;

        o.position.set(p.x, p.y, p.z);

        // geom radius=1 => diameter=2, поэтому scale = span/2
        const pad = 1.12;
        o.scale.set((span.x / 2) * pad, (span.y / 2) * pad, (span.z / 2) * pad);

        o.updateMatrix();
        this.clusterCloudMesh.setMatrixAt(ii, o.matrix);

        const hex = p.color ?? this.theme.pointColor;
        col.set(hex);
        this.clusterCloudMesh.setColorAt(ii, col);
      }

      this.clusterCloudMesh.instanceMatrix.needsUpdate = true;
      this.clusterCloudMesh.instanceColor!.needsUpdate = true;

      this.scene.add(this.clusterCloudMesh);
    }

    this.rebuildPlanes(renderable);

    const bbox = renderable.length ? buildBBox(renderable) : null;
    const bboxLabel = bbox
      ? `x ${bbox.minX.toFixed(2)}..${bbox.maxX.toFixed(2)} | y ${bbox.minY.toFixed(2)}..${bbox.maxY.toFixed(2)} | z ${bbox.minZ.toFixed(2)}..${bbox.maxZ.toFixed(2)}`
      : '—';

    this.fitCamera(renderable);
    return { renderedCount: renderable.length, bboxLabel };
  }

  public dispose(): void {
    cancelAnimationFrame(this.anim);

    this.disposeAxisLabels();
    this.disposeCornerFrame();

    this.renderer?.domElement?.removeEventListener('mousemove', this.onMove3d);

    this.clearMesh(this.pointsMesh);
    this.clearMesh(this.clusterCloudMesh);

    this.renderer?.dispose();
    this.controls?.dispose();
    this.labelRenderer?.domElement?.remove();

    this.renderer = null;
    this.controls = null;
    this.labelRenderer = null;
    this.scene = null;
    this.camera = null;
    this.container = null;
  }

  private animate = (): void => {
    this.controls?.update();
    this.renderer?.render(this.scene!, this.camera!);
    this.labelRenderer?.render(this.scene!, this.camera!);
    this.anim = requestAnimationFrame(this.animate);
  };

  private clearMesh(mesh?: THREE.InstancedMesh): void {
    if (!mesh || !this.scene) return;
    this.scene.remove(mesh);
    mesh.geometry.dispose();
    (mesh.material as THREE.Material).dispose();
  }

  private fitCamera(list: SpacePoint[]): void {
    if (!this.camera || !this.controls) return;

    if (!list.length) {
      this.camera.position.set(0, 0, 40);
      this.controls.target.set(0, 0, 0);
      this.controls.update();
      return;
    }

    const bbox = buildBBox(list);
    const center = new THREE.Vector3(
      (bbox.minX + bbox.maxX) / 2,
      (bbox.minY + bbox.maxY) / 2,
      (bbox.minZ + bbox.maxZ) / 2
    );

    const spanX = Math.max(0.001, bbox.maxX - bbox.minX);
    const spanY = Math.max(0.001, bbox.maxY - bbox.minY);
    const spanZ = Math.max(0.001, bbox.maxZ - bbox.minZ);
    const maxSpan = Math.max(spanX, spanY, spanZ);

    const distance = Math.max(10, maxSpan * 1.55);
    this.camera.position.set(center.x + distance * 0.55, center.y + distance * 0.45, center.z + distance);
    this.controls.target.copy(center);
    this.controls.update();
  }

  private disposeCornerFrame(): void {
    if (!this.cornerFrame) return;
    this.planeGroup.remove(this.cornerFrame);
    this.cornerFrame.geometry.dispose();
    this.cornerFrame.material.dispose();
    this.cornerFrame = null;
  }

  private disposeAxisLabels(): void {
    for (const l of this.axisLabelObjects) this.edgeLabelGroup.remove(l);
    this.axisLabelObjects = [];
  }

  private makeChipEl(
    text: string,
    opts: { tone?: 'neutral' | 'accent'; removable?: boolean; onRemove?: () => void } = {}
  ): HTMLDivElement {
    const el = document.createElement('div');
    el.className = 'chip3d';
    el.style.pointerEvents = opts.removable ? 'auto' : 'none';
    el.style.userSelect = 'none';
    if (opts.tone === 'accent') el.classList.add('accent');

    const span = document.createElement('span');
    span.className = 'txt';
    span.textContent = text;
    el.appendChild(span);

    if (opts.removable) {
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'x';
      btn.textContent = '×';
      btn.addEventListener('click', (e) => {
        e.preventDefault();
        e.stopPropagation();
        opts.onRemove?.();
      });
      el.appendChild(btn);
    }

    return el;
  }

  private createChipLabel(
    text: string,
    opts: { tone?: 'neutral' | 'accent'; removable?: boolean; onRemove?: () => void } = {}
  ): CSS2DObject {
    return new CSS2DObject(this.makeChipEl(text, opts));
  }

  private rebuildPlanesForCurrentPoints(): void {
    this.rebuildPlanes(this.renderPoints);
  }

  private rebuildPlanes(list: SpacePoint[]): void {
    if (!this.scene) return;

    this.planeGroup.clear();
    this.disposeCornerFrame();
    this.disposeAxisLabels();

    const bbox = normalizeBBox(list);

    const A = new THREE.Vector3(bbox.minX, bbox.minY, bbox.minZ);
    const Bx = new THREE.Vector3(bbox.maxX, bbox.minY, bbox.minZ);
    const By = new THREE.Vector3(bbox.minX, bbox.maxY, bbox.minZ);
    const Bz = new THREE.Vector3(bbox.minX, bbox.minY, bbox.maxZ);

    const Cxy = new THREE.Vector3(bbox.maxX, bbox.maxY, bbox.minZ);
    const Cxz = new THREE.Vector3(bbox.maxX, bbox.minY, bbox.maxZ);
    const Cyz = new THREE.Vector3(bbox.minX, bbox.maxY, bbox.maxZ);

    const segments: Array<[THREE.Vector3, THREE.Vector3]> = [
      [A, Bx], [Bx, Cxy], [Cxy, By], [By, A],
      [A, Bx], [Bx, Cxz], [Cxz, Bz], [Bz, A],
      [A, By], [By, Cyz], [Cyz, Bz], [Bz, A]
    ];

    const vertices: number[] = [];
    for (const [p1, p2] of segments) vertices.push(p1.x, p1.y, p1.z, p2.x, p2.y, p2.z);

    const geom = new THREE.BufferGeometry();
    geom.setAttribute('position', new THREE.Float32BufferAttribute(vertices, 3));

    const mat = new THREE.LineBasicMaterial({ color: new THREE.Color(this.theme.edge) });
    this.cornerFrame = new THREE.LineSegments(geom, mat);
    this.planeGroup.add(this.cornerFrame);

    const midX = A.clone().lerp(Bx, 0.5);
    const midY = A.clone().lerp(By, 0.5);
    const midZ = A.clone().lerp(Bz, 0.5);

    const axisX = this.axisCodes.x;
    const axisY = this.axisCodes.y;
    const axisZ = this.axisCodes.z;

    const xChip = axisX
      ? this.createChipLabel(this.deps.fieldName(axisX), { removable: true, onRemove: () => this.cb.onAxisRemove?.('x') })
      : this.createChipLabel('X: выберите', { tone: 'accent' });
    xChip.position.copy(midX);

    const yChip = axisY
      ? this.createChipLabel(this.deps.fieldName(axisY), { removable: true, onRemove: () => this.cb.onAxisRemove?.('y') })
      : this.createChipLabel('Y: выберите', { tone: 'accent' });
    yChip.position.copy(midY);

    const zChip = axisZ
      ? this.createChipLabel(this.deps.fieldName(axisZ), { removable: true, onRemove: () => this.cb.onAxisRemove?.('z') })
      : this.createChipLabel('Z: выберите', { tone: 'accent' });
    zChip.position.copy(midZ);

    const fields = this.deps.getFields();

    const mx = this.createChipLabel(formatValueByMetric(axisX, axisX ? calcMax(list, axisX) : Number.NaN, fields), { tone: 'accent' });
    mx.position.copy(Bx);

    const my = this.createChipLabel(formatValueByMetric(axisY, axisY ? calcMax(list, axisY) : Number.NaN, fields), { tone: 'accent' });
    my.position.copy(By);

    const mz = this.createChipLabel(formatValueByMetric(axisZ, axisZ ? calcMax(list, axisZ) : Number.NaN, fields), { tone: 'accent' });
    mz.position.copy(Bz);

    this.axisLabelObjects = [xChip, yChip, zChip, mx, my, mz];
    this.edgeLabelGroup.add(...this.axisLabelObjects);
  }

  private onMove3d = (event: MouseEvent): void => {
    if (!this.renderer || !this.camera) return;

    const rect = this.renderer.domElement.getBoundingClientRect();
    this.ndc.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
    this.ndc.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;
    this.raycaster.setFromCamera(this.ndc, this.camera);

    const hit = this.pointsMesh ? this.raycaster.intersectObject(this.pointsMesh, true)[0] : undefined;

    if (!hit || hit.instanceId === undefined) {
      this.cb.onTooltip({ visible: false, x: 0, y: 0, lines: [] });
      return;
    }

    const point = this.renderPoints[hit.instanceId];
    if (!point) return;

    if (this.isClusterPoint(point)) {
      const n = this.clusterCount(point);

      const revenue = Math.round(point.metrics.revenue ?? 0);
      const spend = Math.round(point.metrics.spend ?? 0);
      const orders = Math.round(point.metrics.orders ?? 0);
      const drr = Number(point.metrics.drr ?? 0);
      const roi = Number(point.metrics.roi ?? 0);

      this.cb.onTooltip({
        visible: true,
        x: event.clientX - rect.left + 12,
        y: event.clientY - rect.top + 12,
        lines: [
          `Кластер (${n})`,
          `Поле: ${this.deps.fieldName(point.sourceField)}`,
          `Выручка: ${revenue.toLocaleString('ru-RU')} ₽`,
          `Расход: ${spend.toLocaleString('ru-RU')} ₽`,
          `Заказы: ${orders.toLocaleString('ru-RU')}`,
          `ДРР: ${drr.toFixed(2)}% · ROI: ${roi.toFixed(2)}`
        ]
      });
      return;
    }

    this.cb.onTooltip({
      visible: true,
      x: event.clientX - rect.left + 12,
      y: event.clientY - rect.top + 12,
      lines: [
        `${point.label}`,
        `Поле: ${this.deps.fieldName(point.sourceField)}`,
        `Выручка: ${Math.round(point.metrics.revenue ?? 0).toLocaleString('ru-RU')} ₽`,
        `Расход: ${Math.round(point.metrics.spend ?? 0).toLocaleString('ru-RU')} ₽`,
        `ДРР: ${(point.metrics.drr ?? 0).toFixed(2)}% · ROI: ${(point.metrics.roi ?? 0).toFixed(2)}`
      ]
    });
  };
}
