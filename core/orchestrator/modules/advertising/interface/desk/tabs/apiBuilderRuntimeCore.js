export function shouldRetryStatus(status) {
  const code = Number(status || 0);
  return code === 429 || code >= 500;
}

export function retryDelayMs(attempt, baseDelayMs = 250, maxDelayMs = 5000, randomFn = Math.random) {
  const n = Math.max(1, Number(attempt || 1));
  const base = Math.max(0, Number(baseDelayMs || 0));
  const maxDelay = Math.max(base, Number(maxDelayMs || 0));
  const exp = Math.min(maxDelay, base * Math.pow(2, n - 1));
  const jitter = Math.floor((typeof randomFn === 'function' ? randomFn() : Math.random()) * 200);
  return Math.min(maxDelay, exp + jitter);
}

export function groupRowsByAliases(rows, aliases) {
  const out = new Map();
  const source = Array.isArray(rows) ? rows : [];
  const keys = Array.isArray(aliases)
    ? [...new Set(aliases.map((x) => String(x || '').trim()).filter(Boolean))]
    : [];
  for (const row of source) {
    const keyObj = {};
    for (const alias of keys) keyObj[alias] = row?.[alias];
    const key = JSON.stringify(keyObj);
    if (!out.has(key)) out.set(key, { key: keyObj, rows: [] });
    out.get(key).rows.push(row || {});
  }
  return out;
}

export function buildExecutionOrder(mode, syncPlanner, entityStepCounts) {
  const entities = Array.isArray(entityStepCounts)
    ? entityStepCounts.map((x, idx) => ({ entity: idx, steps: Math.max(0, Number(x || 0)) }))
    : [];
  const out = [];
  if (mode === 'sync' && syncPlanner !== 'by_wave') {
    for (const e of entities) {
      for (let step = 1; step <= e.steps; step += 1) out.push({ entity: e.entity, step });
    }
    return out;
  }
  const maxSteps = entities.reduce((m, e) => Math.max(m, e.steps), 0);
  for (let step = 1; step <= maxSteps; step += 1) {
    for (const e of entities) {
      if (e.steps >= step) out.push({ entity: e.entity, step });
    }
  }
  return out;
}

export function evaluateStopRuleValue(actual, operator, expected) {
  if (operator === 'exists') return actual !== undefined && actual !== null && actual !== '';
  if (operator === 'empty') return actual === undefined || actual === null || actual === '';
  if (operator === 'contains') return String(actual ?? '').includes(String(expected ?? ''));
  if (operator === 'not_contains') return !String(actual ?? '').includes(String(expected ?? ''));
  const left = Number(actual);
  const right = Number(expected);
  if (Number.isFinite(left) && Number.isFinite(right)) {
    if (operator === '=') return left === right;
    if (operator === '!=') return left !== right;
    if (operator === '>') return left > right;
    if (operator === '>=') return left >= right;
    if (operator === '<') return left < right;
    if (operator === '<=') return left <= right;
  }
  const l = String(actual ?? '');
  const r = String(expected ?? '');
  if (operator === '=') return l === r;
  if (operator === '!=') return l !== r;
  if (operator === '>') return l > r;
  if (operator === '>=') return l >= r;
  if (operator === '<') return l < r;
  if (operator === '<=') return l <= r;
  return false;
}
