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
  return safeParseJson<VisualScheme[]>(localStorage.getItem(storageKey), []);
}

export function saveVisualSchemes(storageKey: string, schemes: VisualScheme[]): void {
  localStorage.setItem(storageKey, JSON.stringify(schemes));
}

export function loadDatasetPresets(storageKey: string): DatasetPreset[] {
  return safeParseJson<DatasetPreset[]>(localStorage.getItem(storageKey), []);
}

export function saveDatasetPresets(storageKey: string, presets: DatasetPreset[]): void {
  localStorage.setItem(storageKey, JSON.stringify(presets));
}
