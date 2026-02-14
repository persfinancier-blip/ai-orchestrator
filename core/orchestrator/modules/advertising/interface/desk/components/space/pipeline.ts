import type { ShowcaseField, ShowcaseRow } from '../data/showcaseStore';
import type { BBox, PeriodMode, SpacePoint } from './types';

export function hashToJitter(input: string): number {
  let hash = 0;
  for (let i = 0; i < input.length; i += 1) hash = (hash * 31 + input.charCodeAt(i)) >>> 0;
  return ((hash % 1000) / 1000 - 0.5) * 10;
}

export function parseDateToNumber(v: unknown): number {
  const s = String(v ?? '').trim();
  if (!s) return Number.NaN;
  const t = Date.parse(s);
  return Number.isFinite(t) ? t : Number.NaN;
}

export function inPeriod(date: string, period: PeriodMode, fromDate: string, toDate: string): boolean {
  if (!date) return true;

  if (period === 'Даты') {
    if (fromDate && date < fromDate) return false;
    if (toDate && date > toDate) return false;
    return true;
  }

  const days = period === '7 дней' ? 7 : period === '14 дней' ? 14 : 30;
  const border = new Date();
  border.setDate(border.getDate() - (days - 1));
  return new Date(date) >= border;
}

export function applyRowsFilter(args: {
  rows: ShowcaseRow[];
  period: PeriodMode;
  fromDate: string;
  toDate: string;
  search: string;
  selectedEntityFields: string[];
}): ShowcaseRow[] {
  const { rows, period, fromDate, toDate, search, selectedEntityFields } = args;

  return rows.filter((row) => {
    const anyRow = row as any;
    if (!inPeriod(String(anyRow?.date ?? ''), period, fromDate, toDate)) return false;

    if (!search.trim()) return true;
    const q = search.trim().toLowerCase();

    return selectedEntityFields.some((field) =>
      String((row as Record<string, unknown>)[field] ?? '').toLowerCase().includes(q)
    );
  });
}

export function normalize(value: number, min: number, max: number): number {
  return ((value - min) / Math.max(max - min, 0.0001) - 0.5) * 180;
}

export function projectPoints(list: SpacePoint[], xCode: string, yCode: string, zCode: string): SpacePoint[] {
  if (!list.length) return [];

  const useX = !!xCode;
  const useY = !!yCode;
  const useZ = !!zCode;

  const xValues = useX ? list.map((p) => p.metrics[xCode]).filter((n) => Number.isFinite(n)) : [0, 1];
  const yValues = useY ? list.map((p) => p.metrics[yCode]).filter((n) => Number.isFinite(n)) : [0, 1];
  const zValues = useZ ? list.map((p) => p.metrics[zCode]).filter((n) => Number.isFinite(n)) : [0, 1];

  const minX = Math.min(...xValues);
  const maxX = Math.max(...xValues);
  const minY = Math.min(...yValues);
  const maxY = Math.max(...yValues);
  const minZ = Math.min(...zValues);
  const maxZ = Math.max(...zValues);

  return list.map((point) => {
    const x = useX && Number.isFinite(point.metrics[xCode])
      ? normalize(point.metrics[xCode], minX, maxX)
      : hashToJitter(`${point.id}:x`);

    const y = useY && Number.isFinite(point.metrics[yCode])
      ? normalize(point.metrics[yCode], minY, maxY)
      : hashToJitter(`${point.id}:y`);

    const z = useZ && Number.isFinite(point.metrics[zCode])
      ? normalize(point.metrics[zCode], minZ, maxZ)
      : hashToJitter(`${point.id}:z`);

    return { ...point, x, y, z };
  });
}

function sanitizeCoord(value: number, id: string, axis: 'x' | 'y' | 'z'): number {
  if (!Number.isFinite(value)) return hashToJitter(`${id}:${axis}`) * 0.1;
  return value;
}

export function sanitizePoints(input: SpacePoint[]): SpacePoint[] {
  return input.map((point) => ({
    ...point,
    x: sanitizeCoord(point.x, point.id, 'x') + hashToJitter(`${point.id}:x`) * 0.05,
    y: sanitizeCoord(point.y, point.id, 'y') + hashToJitter(`${point.id}:y`) * 0.05,
    z: sanitizeCoord(point.z, point.id, 'z') + hashToJitter(`${point.id}:z`) * 0.05
  }));
}

export function buildBBox(list: SpacePoint[]): BBox {
  const xs = list.map((p) => p.x);
  const ys = list.map((p) => p.y);
  const zs = list.map((p) => p.z);
  return {
    minX: Math.min(...xs), maxX: Math.max(...xs),
    minY: Math.min(...ys), maxY: Math.max(...ys),
    minZ: Math.min(...zs), maxZ: Math.max(...zs)
  };
}

export function normalizeBBox(list: SpacePoint[]): BBox {
  if (!list.length) return { minX: -1, maxX: 1, minY: -1, maxY: 1, minZ: -1, maxZ: 1 };

  const bbox = buildBBox(list);
  const spanX = bbox.maxX - bbox.minX;
  const spanY = bbox.maxY - bbox.minY;
  const spanZ = bbox.maxZ - bbox.minZ;

  if (spanX < 0.001 && spanY < 0.001 && spanZ < 0.001) {
    const cx = (bbox.minX + bbox.maxX) / 2;
    const cy = (bbox.minY + bbox.maxY) / 2;
    const cz = (bbox.minZ + bbox.maxZ) / 2;
    return { minX: cx - 1, maxX: cx + 1, minY: cy - 1, maxY: cy + 1, minZ: cz - 1, maxZ: cz + 1 };
  }

  return bbox;
}

export function calcMax(pointsList: SpacePoint[], metricKey: string): number {
  if (!metricKey) return Number.NaN;
  let max = -Infinity;

  for (const p of pointsList) {
    const v = p?.metrics?.[metricKey];
    if (Number.isFinite(v)) max = Math.max(max, v);
  }

  return max === -Infinity ? Number.NaN : max;
}

export function formatNumberHuman(n: number): string {
  if (!Number.isFinite(n)) return 'нет данных';
  const abs = Math.abs(n);

  if (abs >= 1_000_000_000) return (n / 1_000_000_000).toFixed(1).replace('.', ',') + ' млрд';
  if (abs >= 1_000_000) return (n / 1_000_000).toFixed(1).replace('.', ',') + ' млн';
  if (abs >= 1_000) return (n / 1_000).toFixed(1).replace('.', ',') + ' тыс';

  return Math.round(n).toString();
}

export function formatValueByMetric(metricCode: string, maxValue: number, fields: ShowcaseField[]): string {
  if (!metricCode) return '—';
  if (!Number.isFinite(maxValue)) return 'нет данных';

  const f = fields.find((x) => x.code === metricCode);

  if (f?.kind === 'date') {
    const d = new Date(maxValue);
    if (Number.isNaN(d.getTime())) return 'нет данных';
    const dd = String(d.getDate()).padStart(2, '0');
    const mm = String(d.getMonth() + 1).padStart(2, '0');
    const yy = d.getFullYear();
    return `${dd}.${mm}.${yy}`;
  }

  const money = new Set(['revenue', 'spend', 'margin', 'profit']);
  if (money.has(metricCode)) return `${formatNumberHuman(maxValue)} ₽`;

  const percent = new Set(['drr', 'cr0', 'cr1', 'cr2', 'roi_pct']);
  if (percent.has(metricCode)) {
    const v = maxValue <= 1 ? maxValue * 100 : maxValue;
    return `${v.toFixed(1).replace('.', ',')}%`;
  }

  if (metricCode === 'roi') return maxValue.toFixed(2).replace('.', ',');

  return formatNumberHuman(maxValue);
}

export function buildPoints(args: {
  rows: ShowcaseRow[];
  entityFields: string[];
  axisX: string;
  axisY: string;
  axisZ: string;
  numberFields: ShowcaseField[];
  dateFields: ShowcaseField[];
}): SpacePoint[] {
  const { rows, entityFields, axisX, axisY, axisZ, numberFields, dateFields } = args;

  if (!entityFields.length) return [];

  const numberCodes = numberFields.map((f) => f.code);
  const dateCodes = dateFields.map((f) => f.code);

  const result: SpacePoint[] = [];

  entityFields.forEach((entityField) => {
    const groups = new Map<string, ShowcaseRow[]>();

    rows.forEach((row) => {
      const key = String((row as Record<string, unknown>)[entityField] ?? '').trim();
      if (!key) return;
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key)!.push(row);
    });

    groups.forEach((groupRows, key) => {
      const metrics: Record<string, number> = {};

      for (const code of numberCodes) {
        let sum = 0;
        for (const r of groupRows) sum += Number((r as any)[code] ?? 0);
        metrics[code] = sum / Math.max(groupRows.length, 1);
      }

      for (const code of dateCodes) {
        let sum = 0;
        let cnt = 0;
        for (const r of groupRows) {
          const n = parseDateToNumber((r as any)[code]);
          if (Number.isFinite(n)) {
            sum += n;
            cnt += 1;
          }
        }
        metrics[code] = cnt ? sum / cnt : Number.NaN;
      }

      metrics.revenue = groupRows.reduce((sum, r) => sum + Number((r as any).revenue ?? 0), 0);
      metrics.spend = groupRows.reduce((sum, r) => sum + Number((r as any).spend ?? 0), 0);
      metrics.orders = groupRows.reduce((sum, r) => sum + Number((r as any).orders ?? 0), 0);
      metrics.drr = metrics.revenue > 0 ? (metrics.spend / metrics.revenue) * 100 : 0;
      metrics.roi = metrics.spend > 0 ? metrics.revenue / metrics.spend : 0;

      result.push({
        id: `${entityField}:${key}`,
        label: key,
        sourceField: entityField,
        metrics,
        x: 0,
        y: 0,
        z: 0
      });
    });
  });

  return projectPoints(result, axisX, axisY, axisZ);
}
