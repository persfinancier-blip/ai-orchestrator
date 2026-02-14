import type { ShowcaseField, ShowcaseRow, DatasetId } from '../data/showcaseStore';

export type PeriodMode = '7 дней' | '14 дней' | '30 дней' | 'Даты';

export type SpacePoint = {
  id: string;
  label: string;
  sourceField: string;
  metrics: Record<string, number>;
  x: number;
  y: number;
  z: number;
  color?: string;
};

export type BBox = {
  minX: number; maxX: number;
  minY: number; maxY: number;
  minZ: number; maxZ: number;
};

export type VisualScheme = { id: string; name: string; bg: string; edge: string };

export type DatasetPresetValue = {
  selectedEntityFields: string[];
  axisX: string;
  axisY: string;
  axisZ: string;
  period: PeriodMode;
  fromDate: string;
  toDate: string;
  search: string;
};

export type DatasetPreset = { id: string; name: string; value: DatasetPresetValue };

export type TooltipState = { visible: boolean; x: number; y: number; lines: string[] };

export type ShowcaseSafe = {
  fields: ShowcaseField[];
  rows: ShowcaseRow[];
  datasets: any[];
};

export type SpaceSelectionState = {
  activeLayers: DatasetId[];
  selectedEntityFields: string[];
  axisX: string;
  axisY: string;
  axisZ: string;
  period: PeriodMode;
  fromDate: string;
  toDate: string;
  search: string;
};
