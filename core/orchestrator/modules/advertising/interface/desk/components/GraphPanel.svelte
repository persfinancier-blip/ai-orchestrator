function updateEdgeLabels(params: {
  bbox: BBox;
  pointsList: SpacePoint[];
  xMetric: string;
  yMetric: string;
  zMetric: string;
  xName: string;
  yName: string;
  zName: string;
}): void {
  if (!scene || !camera) return;

  disposeEdgeLabels();

  const { bbox, pointsList, xMetric, yMetric, zMetric, xName, yName, zName } = params;

  // midpoints ребер (нижний ближний угол как базовая точка)
  const A = new THREE.Vector3(bbox.minX, bbox.minY, bbox.minZ);
  const Bx = new THREE.Vector3(bbox.maxX, bbox.minY, bbox.minZ);
  const By = new THREE.Vector3(bbox.minX, bbox.maxY, bbox.minZ);
  const Bz = new THREE.Vector3(bbox.minX, bbox.minY, bbox.maxZ);

  const midX = A.clone().lerp(Bx, 0.5);
  const midY = A.clone().lerp(By, 0.5);
  const midZ = A.clone().lerp(Bz, 0.5);

  const dirX = Bx.clone().sub(A).normalize(); // вдоль ребра X
  const dirY = By.clone().sub(A).normalize(); // вдоль ребра Y
  const dirZ = Bz.clone().sub(A).normalize(); // вдоль ребра Z

  // делает плоскость: X ось вдоль ребра, Y ось “в сторону камеры”
  function orientParallelToEdge(mesh: THREE.Mesh, pos: THREE.Vector3, edgeDir: THREE.Vector3) {
    const toCam = camera.position.clone().sub(pos).normalize();

    // берём компоненту к камере, перпендикулярную ребру
    const yAxis = toCam.clone().sub(edgeDir.clone().multiplyScalar(toCam.dot(edgeDir))).normalize();
    if (!Number.isFinite(yAxis.x) || yAxis.length() < 1e-6) {
      // fallback если камера почти точно по ребру
      yAxis.set(0, 1, 0);
    }

    const zAxis = new THREE.Vector3().crossVectors(edgeDir, yAxis).normalize();
    const xAxis = edgeDir;

    const m = new THREE.Matrix4().makeBasis(xAxis, yAxis, zAxis);
    mesh.quaternion.setFromRotationMatrix(m);
    mesh.position.copy(pos);

    // небольшой отрыв от ребра, чтобы не прилипало
    mesh.position.add(yAxis.clone().multiplyScalar(4));
  }

  // X
  if (xMetric) {
    const xMax = calcMax(pointsList, xMetric);
    const text = `${xName} · 0 — ${formatValueByMetric(xMetric, xMax)}`;
    const mesh = makeTextPlane(text, { fontSize: 70, worldScale: 0.22 }); // <-- крупнее/мельче тут
    orientParallelToEdge(mesh, midX, dirX);
    edgeLabelGroup.add(mesh);
  }

  // Y
  if (yMetric) {
    const yMax = calcMax(pointsList, yMetric);
    const text = `${yName} · 0 — ${formatValueByMetric(yMetric, yMax)}`;
    const mesh = makeTextPlane(text, { fontSize: 70, worldScale: 0.22 });
    orientParallelToEdge(mesh, midY, dirY);
    edgeLabelGroup.add(mesh);
  }

  // Z
  if (zMetric) {
    const zMax = calcMax(pointsList, zMetric);
    const text = `${zName} · 0 — ${formatValueByMetric(zMetric, zMax)}`;
    const mesh = makeTextPlane(text, { fontSize: 70, worldScale: 0.22 });
    orientParallelToEdge(mesh, midZ, dirZ);
    edgeLabelGroup.add(mesh);
  }
}
