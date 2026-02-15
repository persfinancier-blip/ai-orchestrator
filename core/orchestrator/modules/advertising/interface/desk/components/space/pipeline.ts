*** core/orchestrator/modules/advertising/interface/desk/space/pipeline.ts
@@
 // ==============================
 // ✅ VOXEL / GRID LOD AGGREGATION
 // ==============================
 
 function clamp01(v: number): number {
   return Math.min(1, Math.max(0, v));
 }
 
-// ✅ Адаптивный размер вокселя: зависит от bbox и detail
-// detail: 0..1, чем больше — тем грубее (крупнее воксель)
-function voxelSizeFromDetailAndBBox(detail01: number, bbox: BBox): number {
-  const d = clamp01(detail01);
-
-  const spanX = Math.max(0.001, bbox.maxX - bbox.minX);
-  const spanY = Math.max(0.001, bbox.maxY - bbox.minY);
-  const spanZ = Math.max(0.001, bbox.maxZ - bbox.minZ);
-  const maxSpan = Math.max(spanX, spanY, spanZ);
-
-  // хотим в среднем 30 ячеек на максимальном span при detail=0 (мелко),
-  // и ~8 ячеек при detail=1 (крупно)
-  const bins = 30 - 22 * d; // 30..8
-  const size = maxSpan / Math.max(4, bins);
-
-  // clamp чтобы не было слишком мелко/
-  // слишком крупно
-  return Math.max(2.5, Math.min(45, size));
-}
-
-function voxelKey(x: number, y: number, z: number, size: number): string {
-  const ix = Math.floor(x / size);
-  const iy = Math.floor(y / size);
-  const iz = Math.floor(z / size);
-  return `${ix}|${iy}|${iz}`;
-}
+/**
+ * ✅ Гарантированный LOD:
+ * - detail=0  -> воксель очень мелкий, но minCount будет N+1 => без схлопывания
+ * - detail=1  -> воксель >= 2*maxSpan => ВСЁ в одной ячейке => один кластер
+ *
+ * Важно: ключ строим относительно bbox.min*, иначе отрицательные координаты ломают "одна ячейка".
+ */
+function voxelSizeFromDetailAndBBox(detail01: number, bbox: BBox): number {
+  const d = clamp01(detail01);
+
+  const spanX = Math.max(0.001, bbox.maxX - bbox.minX);
+  const spanY = Math.max(0.001, bbox.maxY - bbox.minY);
+  const spanZ = Math.max(0.001, bbox.maxZ - bbox.minZ);
+  const maxSpan = Math.max(spanX, spanY, spanZ);
+
+  const minSize = Math.max(1e-9, maxSpan / 10_000); // практически "по точкам"
+  const maxSize = maxSpan * 2;                      // гарантированно "один воксель"
+  return minSize + (maxSize - minSize) * d;
+}
+
+function voxelKey(p: SpacePoint, bbox: BBox, size: number): string {
+  const ix = Math.floor((p.x - bbox.minX) / size);
+  const iy = Math.floor((p.y - bbox.minY) / size);
+  const iz = Math.floor((p.z - bbox.minZ) / size);
+  return `${ix}|${iy}|${iz}`;
+}
+
+function effectiveMinCount(detail01: number, total: number, baseMinCount: number): number {
+  const d = clamp01(detail01);
+  if (total <= 1) return 2;
+
+  // d=0  => total+1 (никогда не схлопнёт)
+  // d=1  => 1       (схлопнёт даже 1 точку, но при maxSize всё равно будет одна ячейка)
+  const noAgg = total + 1;
+
+  // середина управляется baseMinCount, но в конце принудительно уходим к 1
+  const mid = Math.max(2, Math.round(noAgg * (1 - d) + baseMinCount * d));
+  if (d < 0.85) return Math.min(noAgg, Math.max(2, mid));
+
+  const u = (d - 0.85) / 0.15; // 0..1
+  const tail = Math.round(mid * (1 - u) + 1 * u);
+  return Math.max(1, Math.min(noAgg, tail));
+}
 
 function voxelAggregateHomogeneous(points: SpacePoint[], opts: { minCount: number; detail: number }): SpacePoint[] {
   const { minCount, detail } = opts;
   if (!points.length) return [];
@@
   for (const [field, list] of byField.entries()) {
     const bbox = buildBBox(list);
     const size = voxelSizeFromDetailAndBBox(detail, bbox);
+    const minCountEff = effectiveMinCount(detail, list.length, minCount);
 
     const grid = new Map<string, SpacePoint[]>();
 
     for (const p of list) {
-      const k = voxelKey(p.x, p.y, p.z, size);
+      const k = voxelKey(p, bbox, size);
       const arr = grid.get(k);
       if (arr) arr.push(p);
       else grid.set(k, [p]);
     }
 
     for (const [cell, cellPoints] of grid.entries()) {
-      if (cellPoints.length < minCount) {
+      if (cellPoints.length < minCountEff) {
         out.push(...cellPoints);
         continue;
       }
@@
 export function buildPoints(args: {
@@
   lodEnabled?: boolean;
   lodDetail?: number;       // 0..1
   lodMinCount?: number;     // например 5
 }): SpacePoint[] {
@@
   const {
@@
     lodEnabled = true,
     lodDetail = 0.5,
     lodMinCount = 5
   } = args;
@@
   entityFields.forE
*** END

*** core/orchestrator/modules/advertising/interface/desk/space/SpaceScene.ts
@@
-    if (clusterIdx.length) {
-      const geom = new THREE.BoxGeometry(1, 1, 1);
-
-      const mat = new THREE.MeshBasicMaterial({
-        transparent: true,
-        opacity: 0.35,
-        depthWrite: false,
-        vertexColors: true,
-        wireframe: true // ✅ не “кубик-кирпич”, а границы
-      });
+    if (clusterIdx.length) {
+      // ✅ “облако” по крайним точкам (span) — эллипсоид, а не кубик
+      // sphere radius=0.5 => diameter=1 => scale напрямую в размеры bbox-span
+      const geom = new THREE.SphereGeometry(0.5, 12, 10);
+
+      const mat = new THREE.MeshBasicMaterial({
+        transparent: true,
+        opacity: 0.18,
+        depthWrite: false,
+        vertexColors: true,
+        wireframe: true
+      });
 
       this.clusterCloudMesh = new THREE.InstancedMesh(geom, mat, clusterIdx.length);
@@
       for (let ii = 0; ii < clusterIdx.length; ii += 1) {
         const p = this.renderPoints[clusterIdx[ii]];
         const span = this.clusterSpan(p);
         if (!span) continue;
 
         o.position.set(p.x, p.y, p.z);
 
-        const pad = 1.10;
-        o.scale.set(span.x * pad, span.y * pad, span.z * pad);
+        const pad = 1.18;
+        // span может быть 0 по оси, но clusterSpan уже clamp'ит до 0.001
+        o.scale.set(span.x * pad, span.y * pad, span.z * pad);
 
         o.updateMatrix();
         this.clusterCloudMesh.setMatrixAt(ii, o.matrix);
@@
       this.scene.add(this.clusterCloudMesh);
     }
*** END
