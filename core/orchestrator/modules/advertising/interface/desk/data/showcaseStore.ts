import { writable } from 'svelte/store';

export type DatasetId = 'sales_fact' | 'ads';
export type FieldKind = 'text' | 'number' | 'date';
export type FieldRole = 'entity' | 'axis' | 'filter';

export interface ShowcaseField {
  code: string;
  name: string;
  kind: FieldKind;
  roles: FieldRole[];
  datasetIds: DatasetId[];
}

export interface ShowcaseRow {
  id: string;
  [key: string]: string | number | boolean | null | undefined;
}

export interface ShowcaseState {
  datasets: Array<{ id: DatasetId; name: string }>;
  fields: ShowcaseField[];
  rows: ShowcaseRow[];
}

const datasets: ShowcaseState['datasets'] = [
  { id: 'sales_fact', name: 'Данные' },
  { id: 'ads', name: 'Дополнительный слой' }
];

let currentFields: ShowcaseField[] = [];

function normalizeKind(value: unknown): FieldKind {
  const raw = String(value || '').toLowerCase();
  if (raw === 'number' || /(int|numeric|decimal|double|real|money)/.test(raw)) return 'number';
  if (raw === 'date' || /(date|time)/.test(raw)) return 'date';
  return 'text';
}

function normalizeRoles(raw: unknown, kind: FieldKind): FieldRole[] {
  const list = Array.isArray(raw) ? raw.map((v) => String(v)) : [];
  const roles: FieldRole[] = [];
  if (list.includes('entity') || list.includes('dimension') || list.includes('scope')) roles.push('entity');
  if (list.includes('metric') || list.includes('axis') || kind === 'number') roles.push('axis');
  roles.push('filter');
  return [...new Set(roles)];
}

export function makeShowcaseFields(rawFields: any[] = []): ShowcaseField[] {
  return (Array.isArray(rawFields) ? rawFields : [])
    .map((raw) => {
      const code = String(raw?.code || raw?.source || raw?.name || '').trim();
      if (!code) return null;
      const kind = normalizeKind(raw?.kind || raw?.data_type || raw?.type);
      return {
        code,
        name: String(raw?.label || raw?.name || raw?.source || code).trim() || code,
        kind,
        roles: normalizeRoles(raw?.roles || [raw?.role], kind),
        datasetIds: ['sales_fact', 'ads'] as DatasetId[]
      };
    })
    .filter(Boolean) as ShowcaseField[];
}

export function setShowcaseData(rows: any[] = [], rawFields: any[] = []): void {
  currentFields = makeShowcaseFields(rawFields);
  const normalizedRows: ShowcaseRow[] = (Array.isArray(rows) ? rows : []).map((row, index) => ({
    ...(row && typeof row === 'object' ? row : {}),
    id: String(row?.id ?? row?.sku ?? row?.campaign_id ?? `row-${index + 1}`)
  }));
  showcaseStore.set({ datasets, fields: currentFields, rows: normalizedRows });
}

export function clearShowcase(): void {
  currentFields = [];
  showcaseStore.set({ datasets, fields: [], rows: [] });
}

export function generateShowcaseRows(): ShowcaseRow[] {
  return [];
}

export function regenerateShowcase(): void {
  clearShowcase();
}

export const showcaseStore = writable<ShowcaseState>({ datasets, fields: [], rows: [] });

export function fieldName(code: string): string {
  return currentFields.find((field) => field.code === code)?.name ?? code;
}
