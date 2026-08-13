function finite(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

export function normalizeContainerKind(value) {
  const kind = String(value || '').trim().toLowerCase();
  return ['client', 'process_group', 'project', 'experiment'].includes(kind) ? kind : 'project';
}

export function inferFieldRole(name, dataType = '', samples = []) {
  const key = String(name || '').trim().toLowerCase();
  const type = String(dataType || '').trim().toLowerCase();
  const values = (Array.isArray(samples) ? samples : []).filter((v) => v !== null && v !== undefined && String(v).trim() !== '');
  const numericType = /(int|numeric|decimal|double|real|money)/.test(type);
  const dateType = /(date|time)/.test(type);
  if (dateType || /(^|_)(date|day|dt|created_at|updated_at)$/.test(key)) return { role: 'time', kind: 'date', confidence: 0.98 };
  if (/client(_id)?$|tenant(_id)?$/.test(key)) return { role: 'scope', kind: numericType ? 'number' : 'text', confidence: 0.96 };
  if (/(sku|nm_id|offer_id|product_id|campaign_id|cabinet_id|external_id|^id$)/.test(key)) return { role: 'entity', kind: numericType ? 'number' : 'text', confidence: 0.93 };
  if (/(revenue|sales|gmv|amount|spend|cost|price|orders|clicks|views|impressions|profit|margin|drr|roas|roi|ctr|cr|position|share|budget)/.test(key)) {
    return { role: 'metric', kind: 'number', confidence: numericType ? 0.98 : 0.82 };
  }
  if (numericType) return { role: 'metric', kind: 'number', confidence: 0.72 };
  if (values.length >= 3) {
    const numericShare = values.filter((v) => finite(v) !== null).length / values.length;
    if (numericShare >= 0.9) return { role: 'metric', kind: 'number', confidence: 0.68 };
  }
  if (/(name|brand|category|subject|status|type|cluster|query|keyword)/.test(key)) return { role: 'dimension', kind: 'text', confidence: 0.82 };
  return { role: 'dimension', kind: 'text', confidence: 0.55 };
}

export function buildNormalizationPlan(columns = [], rows = []) {
  const list = Array.isArray(columns) ? columns : [];
  const sampleRows = Array.isArray(rows) ? rows : [];
  const fields = list.map((column) => {
    const name = String(column?.name || column?.column_name || '').trim();
    const dataType = String(column?.type || column?.data_type || '').trim();
    const samples = sampleRows.slice(0, 50).map((row) => row?.[name]);
    const inferred = inferFieldRole(name, dataType, samples);
    return {
      source: name,
      name,
      data_type: dataType,
      role: inferred.role,
      kind: inferred.kind,
      confidence: inferred.confidence,
      nullable: Boolean(column?.is_nullable ?? true)
    };
  });
  const roles = Object.fromEntries(['time', 'entity', 'scope', 'metric', 'dimension'].map((role) => [role, fields.filter((f) => f.role === role).length]));
  const warnings = [];
  if (!roles.time) warnings.push('Не найдено временное поле — прогнозы по времени будут недоступны до выбора даты.');
  if (!roles.metric) warnings.push('Не найдено числовых метрик для аналитики.');
  if (!roles.entity) warnings.push('Не определена сущность (SKU, кампания и т.п.) — группировка будет ограничена.');
  return { fields, roles, warnings, ready_for_analysis: roles.metric > 0 };
}

export function pearson(xs = [], ys = []) {
  const pairs = [];
  const len = Math.min(xs.length, ys.length);
  for (let i = 0; i < len; i += 1) {
    const x = finite(xs[i]);
    const y = finite(ys[i]);
    if (x !== null && y !== null) pairs.push([x, y]);
  }
  if (pairs.length < 3) return null;
  const mx = pairs.reduce((s, p) => s + p[0], 0) / pairs.length;
  const my = pairs.reduce((s, p) => s + p[1], 0) / pairs.length;
  let num = 0;
  let dx = 0;
  let dy = 0;
  for (const [x, y] of pairs) {
    const ax = x - mx;
    const ay = y - my;
    num += ax * ay;
    dx += ax * ax;
    dy += ay * ay;
  }
  if (!(dx > 0) || !(dy > 0)) return null;
  return num / Math.sqrt(dx * dy);
}

function averageRanks(values = []) {
  const indexed = values.map((value, index) => ({ value, index })).sort((a, b) => a.value - b.value);
  const ranks = new Array(values.length);
  for (let i = 0; i < indexed.length;) {
    let j = i + 1;
    while (j < indexed.length && indexed[j].value === indexed[i].value) j += 1;
    const rank = (i + 1 + j) / 2;
    for (let k = i; k < j; k += 1) ranks[indexed[k].index] = rank;
    i = j;
  }
  return ranks;
}

export function spearman(xs = [], ys = []) {
  const pairs = [];
  const len = Math.min(xs.length, ys.length);
  for (let i = 0; i < len; i += 1) {
    const x = finite(xs[i]);
    const y = finite(ys[i]);
    if (x !== null && y !== null) pairs.push([x, y]);
  }
  if (pairs.length < 3) return null;
  const rx = averageRanks(pairs.map((pair) => pair[0]));
  const ry = averageRanks(pairs.map((pair) => pair[1]));
  return pearson(rx, ry);
}

export function rankCorrelations(rows = [], metricFields = [], limit = 12) {
  const data = Array.isArray(rows) ? rows : [];
  const fields = [...new Set((metricFields || []).map((x) => String(x || '').trim()).filter(Boolean))];
  const out = [];
  for (let i = 0; i < fields.length; i += 1) {
    for (let j = i + 1; j < fields.length; j += 1) {
      const x = fields[i];
      const y = fields[j];
      const xs = data.map((row) => row?.[x]);
      const ys = data.map((row) => row?.[y]);
      const p = pearson(xs, ys);
      const s = spearman(xs, ys);
      if (p === null && s === null) continue;
      const pearsonAbs = Math.abs(p || 0);
      const spearmanAbs = Math.abs(s || 0);
      const useSpearman = spearmanAbs > pearsonAbs + 0.08;
      const chosen = useSpearman ? s : p ?? s;
      out.push({
        x,
        y,
        correlation: Number(Number(chosen || 0).toFixed(4)),
        pearson: p === null ? null : Number(p.toFixed(4)),
        spearman: s === null ? null : Number(s.toFixed(4)),
        strength: Math.max(pearsonAbs, spearmanAbs),
        relation_type: useSpearman ? 'monotonic' : 'linear'
      });
    }
  }
  return out.sort((a, b) => b.strength - a.strength).slice(0, Math.max(1, Number(limit || 12)));
}

function medianFinite(values = []) {
  const sorted = (Array.isArray(values) ? values : []).map(finite).filter((v) => v !== null).sort((a, b) => a - b);
  if (!sorted.length) return null;
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
}

export function rankAnomalies(rows = [], metricFields = [], options = {}) {
  const data = Array.isArray(rows) ? rows : [];
  const fields = [...new Set((metricFields || []).map((x) => String(x || '').trim()).filter(Boolean))];
  const threshold = Math.max(1.5, Number(options.threshold || 3.5));
  const limit = Math.max(1, Math.min(500, Math.trunc(Number(options.limit || 50))));
  const stats = new Map();
  for (const field of fields) {
    const values = data.map((row) => row?.[field]);
    const median = medianFinite(values);
    if (median === null) continue;
    const mad = medianFinite(values.map((value) => {
      const n = finite(value);
      return n === null ? null : Math.abs(n - median);
    }));
    if (mad === null || mad <= 0) continue;
    stats.set(field, { median, mad });
  }
  const out = [];
  data.forEach((row, rowIndex) => {
    const findings = [];
    for (const [field, stat] of stats.entries()) {
      const value = finite(row?.[field]);
      if (value === null) continue;
      const score = 0.6745 * (value - stat.median) / stat.mad;
      if (Math.abs(score) >= threshold) {
        findings.push({ field, value, score: Number(score.toFixed(3)), direction: score > 0 ? 'high' : 'low' });
      }
    }
    if (!findings.length) return;
    findings.sort((a, b) => Math.abs(b.score) - Math.abs(a.score));
    const top = findings[0];
    const entity = row?.sku ?? row?.nm_id ?? row?.product_id ?? row?.campaign_id ?? row?.offer_id ?? row?.id ?? null;
    out.push({ row_index: rowIndex, entity, score: Math.abs(top.score), primary_field: top.field, findings });
  });
  return out.sort((a, b) => b.score - a.score).slice(0, limit);
}

function mulberry32(seed) {
  let a = seed >>> 0;
  return function random() {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function gaussian(random) {
  let u = 0;
  let v = 0;
  while (u === 0) u = random();
  while (v === 0) v = random();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
}

function quantile(sorted, p) {
  if (!sorted.length) return null;
  const pos = (sorted.length - 1) * p;
  const base = Math.floor(pos);
  const rest = pos - base;
  return sorted[base + 1] === undefined ? sorted[base] : sorted[base] + rest * (sorted[base + 1] - sorted[base]);
}

export function monteCarloForecast(series = [], options = {}) {
  const values = (Array.isArray(series) ? series : []).map(finite).filter((v) => v !== null);
  if (values.length < 3) throw new Error('forecast_series_too_short');
  const horizon = Math.max(1, Math.min(365, Math.trunc(Number(options.horizon || 30))));
  const simulations = Math.max(100, Math.min(20000, Math.trunc(Number(options.simulations || 2000))));
  const seed = Math.trunc(Number(options.seed || 20260814));
  const random = mulberry32(seed);
  const n = values.length;
  const meanX = (n - 1) / 2;
  const meanY = values.reduce((s, v) => s + v, 0) / n;
  let cov = 0;
  let varX = 0;
  for (let i = 0; i < n; i += 1) {
    cov += (i - meanX) * (values[i] - meanY);
    varX += (i - meanX) ** 2;
  }
  const slope = varX > 0 ? cov / varX : 0;
  const intercept = meanY - slope * meanX;
  const residuals = values.map((v, i) => v - (intercept + slope * i));
  const variance = residuals.reduce((s, v) => s + v * v, 0) / Math.max(1, residuals.length - 2);
  const sigma = Math.sqrt(Math.max(0, variance));
  const paths = [];
  const endValues = [];
  const target = finite(options.target);
  let targetHits = 0;
  for (let sim = 0; sim < simulations; sim += 1) {
    const path = [];
    for (let step = 1; step <= horizon; step += 1) {
      const baseline = intercept + slope * (n - 1 + step);
      const value = baseline + gaussian(random) * sigma;
      path.push(Number(value.toFixed(4)));
    }
    const end = path[path.length - 1];
    if (target !== null && end >= target) targetHits += 1;
    endValues.push(end);
    if (sim < 40) paths.push(path);
  }
  endValues.sort((a, b) => a - b);
  const forecast = [];
  for (let step = 0; step < horizon; step += 1) {
    const stepValues = paths.map((path) => path[step]).sort((a, b) => a - b);
    forecast.push({
      step: step + 1,
      median: quantile(stepValues, 0.5),
      low: quantile(stepValues, 0.1),
      high: quantile(stepValues, 0.9)
    });
  }
  return {
    model: 'monte_carlo_trend',
    observations: n,
    horizon,
    simulations,
    trend_per_step: Number(slope.toFixed(6)),
    noise_sigma: Number(sigma.toFixed(6)),
    final: {
      p10: quantile(endValues, 0.1),
      median: quantile(endValues, 0.5),
      p90: quantile(endValues, 0.9),
      target_probability: target === null ? null : targetHits / simulations
    },
    forecast
  };
}

export const productAnalyticsTestkit = Object.freeze({ finite, quantile, mulberry32, averageRanks, medianFinite });
