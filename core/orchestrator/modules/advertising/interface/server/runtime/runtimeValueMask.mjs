const MASK = '[redacted]';

function list(values) {
  return [...new Set((Array.isArray(values) ? values : []).map((value) => String(value || '')).filter(Boolean))]
    .sort((a, b) => b.length - a.length);
}

function maskText(value, values) {
  let output = String(value ?? '');
  for (const item of values) output = output.split(item).join(MASK);
  return output;
}

export function maskRuntimeValues(value, values = []) {
  const items = list(values);
  if (typeof value === 'string') return maskText(value, items);
  if (Array.isArray(value)) return value.map((item) => maskRuntimeValues(item, items));
  if (!value || typeof value !== 'object') return value;
  const output = {};
  for (const [key, item] of Object.entries(value)) output[key] = maskRuntimeValues(item, items);
  return output;
}

export const runtimeValueMaskTestkit = Object.freeze({ MASK, list, maskText });
