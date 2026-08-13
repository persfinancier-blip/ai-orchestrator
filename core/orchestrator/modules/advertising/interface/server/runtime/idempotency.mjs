import crypto from 'node:crypto';

export function stableValue(value) {
  if (Array.isArray(value)) return value.map(stableValue);
  if (!value || typeof value !== 'object') return value;
  const out = {};
  for (const key of Object.keys(value).sort()) out[key] = stableValue(value[key]);
  return out;
}

export function stableStringify(value) {
  return JSON.stringify(stableValue(value ?? null));
}

export function sha256Hex(value) {
  return crypto.createHash('sha256').update(String(value ?? '')).digest('hex');
}

export function buildIdempotencyKey(namespace, payload) {
  const ns = String(namespace || 'runtime').trim().toLowerCase() || 'runtime';
  return `${ns}:${sha256Hex(stableStringify(payload))}`;
}

export function normalizeIdempotencyKey(headerValue, namespace, payload) {
  const supplied = String(headerValue || '').trim();
  if (supplied) return supplied.slice(0, 240);
  return buildIdempotencyKey(namespace, payload);
}
