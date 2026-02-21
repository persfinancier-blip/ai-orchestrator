import type { DatasetPreset, VisualScheme } from './types';

function safeParseJson<T>(raw: string | null, fallback: T): T {
  if (!raw) return fallback;
  try {
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

export function loadVisualSchemes(storageKey: string): VisualScheme[] {
  return [];
}

export function saveVisualSchemes(storageKey: string, schemes: VisualScheme[]): void {
  // local storage disabled
}

export function loadDatasetPresets(storageKey: string): DatasetPreset[] {
  return [];
}

export function saveDatasetPresets(storageKey: string, presets: DatasetPreset[]): void {
  // local storage disabled
}
