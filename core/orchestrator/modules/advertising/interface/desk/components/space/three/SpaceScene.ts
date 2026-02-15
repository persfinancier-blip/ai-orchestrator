import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
import { CSS2DRenderer } from 'three/examples/jsm/renderers/CSS2DRenderer.js';
import type { CSS2DObject } from 'three/examples/jsm/renderers/CSS2DRenderer.js';
import { ConvexGeometry } from 'three/examples/jsm/geometries/ConvexGeometry.js';

import type { SpacePoint } from '../types';
import { buildBBox, normalizeBBox, sanitizePoints } from '../pipeline';

export type SpaceSceneTheme = {
  bg?: string;
  edge?: string;
  pointColor?: string;
};

export type SpaceSceneDeps = {
  container: HTMLElement;
};

export type SpaceSceneCallbacks = {
  onHover?: (p: SpacePoint | null) => void;
};

type Span = { x: number; y: number; z: number };

export class SpaceScene {
  private deps: SpaceSceneDeps;
  private cb: SpaceSceneCallbacks;

  private scene = new THREE.Scene();
  private camera = new THREE.PerspectiveCamera(50, 1, 0.1, 10_000);
  private renderer = new THREE.WebGLRenderer({ antialias: true });
  private labelRenderer = new CSS2DRenderer();

  private controls?: OrbitControls;

  private labelObjects: CSS2DObject[] = [];

  private pointsMesh?: THREE.InstancedMesh;

  // ✅ либо hull-группа, либо instanced "облака-эллипсоиды" (fallback)
  private clusterCloudGroup?: THREE.Group;
  private clusterCloudEllipsoids?: THREE.InstancedMesh;

  private renderPoints: SpacePoint[] = [];

  private anim = 0;

  private theme: Required<SpaceSceneTheme> = {
    bg: '#ffffff',
    edge: '#334155',
    pointColor: '#22c55e'
  };

  private raycaster = new THREE.Raycaster();
  private ndc = new THREE.Vector2();

  public constructor(deps: SpaceSceneDeps, cb: SpaceSceneCallbacks) {
    this.deps = deps;
    this.cb = cb;

    this.renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    this.renderer.setSize(deps.container.clientWidth, deps.container.clientHeight);
    deps.container.appendChild(this.renderer.domElement);

    this.labelRenderer.setSize(deps.container.clientWidth, deps.container.clientHeight);
    this.labelRenderer.domElement.style.position = 'absolute';
    this.labelRenderer.domElement.style.top = '0';
    this.labelRenderer.domElement.style.left = '0';
    deps.container.appendChild(this.labelRenderer.domElement);

    this.camera.position.set(0, 0, 160);
    this.scene.background = new THREE.Color(this.theme.bg);

    // ✅ controls на renderer, а не на label-layer
    this.controls = new OrbitControls(this.camera, this.renderer.domElement);
    this.controls.enableDamping = true;

    this.deps.container.addEventListener('pointermove', this.onPointerMove);
    window.addEventListener('resize', this.onResize);

    this.onResize();
    this.loop();
  }

  public dispose(): void {
    cancelAnimationFrame(this.anim);
    this.deps.container.removeEventListener('pointermove', this.onPointerMove);
    window.removeEventListener('resize', this.onResize);

    this.disposeMeshes();
    this.disposeLabels();

    this.renderer.dispose();
    this.deps.container.innerHTML = '';
  }

  public setTheme(theme: SpaceSceneTheme): void {
    this.theme = { ...this.theme, ...theme };
    this.scene.background = new THREE.Color(this.theme.bg);
    if (this.pointsMesh?.instanceColor) this.pointsMesh.instanceColor.needsUpdate = true;
  }

  public setPoints(points: SpacePoint[]): void {
    this.renderPoints = sanitizePoints(points);
    this.rebuild();
  }

  private isClusterPoint(p: SpacePoint): boolean {
    return Boolean((p as any).isCluster && (p as any).clusterCount);
  }

  private clusterSpan(p: SpacePoint): Span | null {
    const span = (p as any).span as Span | undefined;
    if (!span) return null;
    return {
      x: Math.max(0.001, Number(span.x)),
      y: Math.max(0.001, Number(span.y)),
      z: Math.max(0.001, Number(span.z))
    };
  }

  private disposeMeshes(): void {
    if (this.pointsMesh) {
      this.scene.remove(this.pointsMesh);
      this.pointsMesh.geometry.dispose();
      (this.pointsMesh.material as THREE.Material).dispose();
      this.pointsMesh = undefined;
    }

    if (this.clusterCloudEllipsoids) {
      this.scene.remove(this.clusterCloudEllipsoids);
      this.clusterCloudEllipsoids.geometry.dispose();
      (this.clusterCloudEllipsoids.material as THREE.Material).dispose();
      this.clusterCloudEllipsoids = undefined;
    }

    if (this.clusterCloudGroup) {
      this.scene.remove(this.clusterCloudGroup);
      this.clusterCloudGroup.traverse((o) => {
        const m = o as THREE.Mesh;
        if (m.geometry) m.geometry.dispose();
        const mat = m.material as THREE.Material | THREE.Material[] | undefined;
        if (Array.isArray(mat)) mat.forEach((x) => x.dispose());
        else mat?.dispose();
      });
      this.clusterCloudGroup = undefined;
    }
  }

  private disposeLabels(): void {
    for (const l of this.labelObjects) this.scene.remove(l);
    this.labelObjects = [];
  }

  private rebuild(): void {
    this.disposeMeshes();
    this.disposeLabels();

    const renderable = this.renderPoints;

    // 1) POINTS
    if (renderable.length) {
      const geom = new THREE.SphereGeometry(0.55, 10, 8);
      const mat = new THREE.MeshBasicMaterial({ vertexColors: true });

      this.pointsMesh = new THREE.InstancedMesh(geom, mat, renderable.length);

      const o = new THREE.Object3D();
      const c = new THREE.Color();

      for (let i = 0; i < renderable.length; i += 1) {
        const p = renderable[i];
        o.position.set(p.x, p.y, p.z);
        o.updateMatrix();
        this.pointsMesh.setMatrixAt(i, o.matrix);

        const hex = p.color ?? this.theme.pointColor;
        c.set(hex);
        this.pointsMesh.setColorAt(i, c);
      }

      this.pointsMesh.instanceMatrix.needsUpdate = true;
      this.pointsMesh.instanceColor!.needsUpdate = true;

      this.scene.add(this.pointsMesh);
    }

    // 2) CLUSTER CLOUDS: ConvexHull first, else ellipsoid fallback. NO BOXES.
    const clusterIdx: number[] = [];
    for (let i = 0; i < renderable.length; i += 1) {
      if (this.isClusterPoint(renderable[i])) clusterIdx.push(i);
    }

    const HULL_CLUSTER_LIMIT = 250;
    const c = new THREE.Color();

    if (clusterIdx.length && clusterIdx.length <= HULL_CLUSTER_LIMIT) {
      const group = new THREE.Group();
      const baseMat = new THREE.MeshBasicMaterial({
        transparent: true,
        opacity: 0.22,
        depthWrite: false,
        wireframe: true
      });

      for (const idx of clusterIdx) {
        const p = renderable[idx];

        const hull = (p as any).hull as [number, number, number][] | undefined;
        if (!hull || hull.length < 4) continue;

        const pts = hull.map(([x, y, z]) => new THREE.Vector3(x - p.x, y - p.y, z - p.z));

        try {
          const geom = new ConvexGeometry(pts);
          const mat = baseMat.clone();

          const hex = p.color ?? this.theme.pointColor;
          c.set(hex);
          (mat as THREE.MeshBasicMaterial).color.copy(c);

          const mesh = new THREE.Mesh(geom, mat);
          mesh.position.set(p.x, p.y, p.z);

          group.add(mesh);
        } catch {
          // если hull вдруг кривой — оставим на fallback-эллипсоид
        }
      }

      if (group.children.length) {
        this.clusterCloudGroup = group;
        this.scene.add(group);
      }
    }

    // Fallback: instanced ellipsoid-clouds (sphere scaled by span)
    if (clusterIdx.length && !this.clusterCloudGroup) {
      const geom = new THREE.SphereGeometry(0.5, 12, 10);
      const mat = new THREE.MeshBasicMaterial({
        transparent: true,
        opacity: 0.18,
        depthWrite: false,
        vertexColors: true,
        wireframe: true
      });

      this.clusterCloudEllipsoids = new THREE.InstancedMesh(geom, mat, clusterIdx.length);

      const o = new THREE.Object3D();
      for (let ii = 0; ii < clusterIdx.length; ii += 1) {
        const p = renderable[clusterIdx[ii]];
        const span = this.clusterSpan(p) ?? { x: 1, y: 1, z: 1 };

        o.position.set(p.x, p.y, p.z);
        const pad = 1.18;
        o.scale.set(span.x * pad, span.y * pad, span.z * pad);
        o.updateMatrix();

        this.clusterCloudEllipsoids.setMatrixAt(ii, o.matrix);

        const hex = p.color ?? this.theme.pointColor;
        c.set(hex);
        this.clusterCloudEllipsoids.setColorAt(ii, c);
      }

      this.clusterCloudEllipsoids.instanceMatrix.needsUpdate = true;
      this.clusterCloudEllipsoids.instanceColor!.needsUpdate = true;
      this.scene.add(this.clusterCloudEllipsoids);
    }

    // ---- bbox label etc (как у тебя ниже) ----
    const bbox = renderable.length ? buildBBox(renderable) : null;
    const nb = bbox ? normalizeBBox(bbox) : null;
    if (nb) {
      // оставь свой существующий код подписей/плоскостей
    }
  }

  private onPointerMove = (ev: PointerEvent): void => {
    const rect = this.renderer.domElement.getBoundingClientRect();
    this.ndc.x = ((ev.clientX - rect.left) / rect.width) * 2 - 1;
    this.ndc.y = -(((ev.clientY - rect.top) / rect.height) * 2 - 1);

    this.raycaster.setFromCamera(this.ndc, this.camera);

    const hit = this.pointsMesh ? this.raycaster.intersectObject(this.pointsMesh) : [];
    if (!hit.length) {
      this.cb.onHover?.(null);
      return;
    }

    const instId = hit[0].instanceId;
    if (typeof instId !== 'number') {
      this.cb.onHover?.(null);
      return;
    }

    this.cb.onHover?.(this.renderPoints[instId] ?? null);
  };

  private onResize = (): void => {
    const w = this.deps.container.clientWidth;
    const h = this.deps.container.clientHeight;

    this.camera.aspect = w / h;
    this.camera.updateProjectionMatrix();

    this.renderer.setSize(w, h);
    this.labelRenderer.setSize(w, h);
  };

  private loop = (): void => {
    this.anim = requestAnimationFrame(this.loop);

    this.controls?.update();
    this.renderer.render(this.scene, this.camera);
    this.labelRenderer.render(this.scene, this.camera);
  };
}
