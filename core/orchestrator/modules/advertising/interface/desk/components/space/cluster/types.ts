export type GroupingPrinciple = 'efficiency' | 'behavior' | 'proximity';

export type RecomputeMode = 'auto' | 'fixed' | 'manual';

export type GroupingConfig = {
  enabled: boolean;

  principle: GroupingPrinciple;

  // до 3 выбранных полей (number/text) в зависимости от principle
  featureFields: string[];

  // 0..1 — главный ползунок “детализация”
  detail: number;

  // веса для X/Y/Z (используется для proximity, и только если customWeights=true)
  customWeights: boolean;
  wX: number;
  wY: number;
  wZ: number;

  // минимальный размер кластера
  minClusterSize: number;

  // авто/фикс/ручной
  recompute: RecomputeMode;
};

export type DbscanParams = {
  eps: number;
  minPts: number;
};
