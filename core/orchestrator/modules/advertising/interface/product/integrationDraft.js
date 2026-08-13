function objectValue(value) {
  if (value && typeof value === 'object') return value;
  if (typeof value !== 'string' || !value.trim()) return {};
  try { return JSON.parse(value); } catch { return {}; }
}

const SECRET_KEY = /(authorization|proxy-authorization|cookie|api[-_]?key|access[-_]?token|refresh[-_]?token|client[-_]?secret|password|secret)/i;

function scrubSecrets(value, path = '', warnings = []) {
  if (Array.isArray(value)) return value.map((item, index) => scrubSecrets(item, `${path}[${index}]`, warnings));
  if (!value || typeof value !== 'object') return value;
  const out = {};
  for (const [key, item] of Object.entries(value)) {
    const fieldPath = path ? `${path}.${key}` : key;
    if (SECRET_KEY.test(String(key))) {
      warnings.push(`Секретное поле не сохранено автоматически: ${fieldPath}`);
      continue;
    }
    out[key] = scrubSecrets(item, fieldPath, warnings);
  }
  return out;
}

export function integrationDraftFromAssistant(result = {}) {
  const values = result?.apply_patch?.by_field || {};
  const fullUrl = String(values.url || '').trim();
  if (!fullUrl) return { ok: false, reason: 'missing_endpoint', payload: null, warnings: [] };

  let url;
  try { url = new URL(fullUrl); } catch { return { ok: false, reason: 'invalid_endpoint', payload: null, warnings: [] }; }
  if (!['http:', 'https:'].includes(url.protocol)) return { ok: false, reason: 'invalid_protocol', payload: null, warnings: [] };
  if (url.username || url.password) return { ok: false, reason: 'embedded_credentials', payload: null, warnings: [] };
  if (url.hash) return { ok: false, reason: 'url_fragment_not_allowed', payload: null, warnings: [] };

  const method = String(values.method || 'GET').toUpperCase();
  if (!['GET', 'POST', 'PUT', 'PATCH', 'DELETE'].includes(method)) return { ok: false, reason: 'invalid_method', payload: null, warnings: [] };

  const warnings = [];
  const headers = scrubSecrets(objectValue(values.headersText), 'headers', warnings);
  const query = scrubSecrets(objectValue(values.queryText), 'query', warnings);
  const body = scrubSecrets(objectValue(values.bodyText), 'body', warnings);

  return {
    ok: true,
    reason: '',
    warnings,
    payload: {
      api_name: String(result?.intent?.operation || result?.summary || 'API draft').slice(0, 120),
      method,
      base_url: url.origin,
      path: `${url.pathname}${url.search || ''}`,
      headers_json: headers,
      query_json: query,
      body_json: body,
      pagination_json: values.paginationDataPath ? { enabled: true, dataPath: values.paginationDataPath } : {},
      description: `Draft generated from Node Assistant. ${String(result?.summary || '')}`.trim(),
      is_active: false,
      updated_by: 'product_assistant'
    }
  };
}

export const integrationDraftTestkit = Object.freeze({ scrubSecrets });
