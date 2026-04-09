export function normalizeJsonTextSetting(raw, fallback = '[]') {
  if (typeof raw === 'string') {
    const txt = raw.trim();
    if (!txt) return fallback;
    try {
      return JSON.stringify(JSON.parse(txt), null, 2);
    } catch {
      return txt;
    }
  }
  if (raw === undefined || raw === null || raw === '') return fallback;
  try {
    return JSON.stringify(raw, null, 2);
  } catch {
    return fallback;
  }
}

export function safeJsonArray(value) {
  return Array.isArray(value) ? value : [];
}
