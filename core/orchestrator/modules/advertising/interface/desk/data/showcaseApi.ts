import { get } from 'svelte/store';
import { showcaseStore, type ShowcaseRow } from './showcaseStore';

function text(value: unknown): string {
  return value === undefined || value === null ? '' : String(value);
}

function num(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

export function normalizeSpaceRows(input: unknown): ShowcaseRow[] {
  if (!Array.isArray(input)) return [];
  return input.map((raw: any, index: number) => ({
    id: text(raw?.id || raw?.row_id || `${raw?.sku || 'row'}:${raw?.campaign_id || ''}:${raw?.date || index}`),
    sku: text(raw?.sku || raw?.nm_id || raw?.nmId),
    campaign_id: text(raw?.campaign_id || raw?.advert_id || raw?.advertId),
    subject: text(raw?.subject),
    brand: text(raw?.brand),
    category: text(raw?.category),
    entry_point: text(raw?.entry_point || raw?.placement),
    keyword_cluster: text(raw?.keyword_cluster),
    search_query: text(raw?.search_query || raw?.query),
    revenue: num(raw?.revenue),
    orders: num(raw?.orders),
    spend: num(raw?.spend),
    drr: num(raw?.drr),
    roi: num(raw?.roi),
    cr2: num(raw?.cr2),
    position: num(raw?.position),
    search_share: num(raw?.search_share),
    shelf_share: num(raw?.shelf_share),
    date: text(raw?.date).slice(0, 10)
  }));
}

export function clearGeneratedShowcase(): void {
  const state = get(showcaseStore);
  showcaseStore.set({ ...state, rows: [] });
}

export async function loadRealShowcase(limit = 5000): Promise<number> {
  clearGeneratedShowcase();
  const safeLimit = Math.max(1, Math.min(5000, Math.trunc(Number(limit || 5000))));
  const response = await fetch(`/ai-orchestrator/api/space?limit=${safeLimit}&offset=0`, {
    cache: 'no-store',
    headers: { Accept: 'application/json' }
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(String(payload?.details || payload?.error || `HTTP ${response.status}`));
  const rows = normalizeSpaceRows(payload?.points);
  const state = get(showcaseStore);
  showcaseStore.set({ ...state, rows });
  return rows.length;
}
