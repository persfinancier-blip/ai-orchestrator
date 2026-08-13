function objectValue(value) {
  if (value && typeof value === 'object') return value;
  if (typeof value !== 'string' || !value.trim()) return {};
  try { return JSON.parse(value); } catch { return {}; }
}

export function integrationDraftFromAssistant(result = {}) {
  const values = result?.apply_patch?.by_field || {};
  const fullUrl = String(values.url || '').trim();
  if (!fullUrl) return { ok: false, reason: 'missing_endpoint', payload: null };

  let url;
  try { url = new URL(fullUrl); } catch { return { ok: false, reason: 'invalid_endpoint', payload: null }; }
  if (!['http:', 'https:'].includes(url.protocol)) return { ok: false, reason: 'invalid_protocol', payload: null };

  const method = String(values.method || 'GET').toUpperCase();
  if (!['GET', 'POST', 'PUT', 'PATCH', 'DELETE'].includes(method)) return { ok: false, reason: 'invalid_method', payload: null };

  return {
    ok: true,
    reason: '',
    payload: {
      api_name: String(result?.intent?.operation || result?.summary || 'API draft').slice(0, 120),
      method,
      base_url: url.origin,
      path: `${url.pathname}${url.search || ''}`,
      headers_json: objectValue(values.headersText),
      query_json: objectValue(values.queryText),
      body_json: objectValue(values.bodyText),
      pagination_json: values.paginationDataPath ? { enabled: true, dataPath: values.paginationDataPath } : {},
      description: `Draft generated from Node Assistant. ${String(result?.summary || '')}`.trim(),
      is_active: false,
      updated_by: 'product_assistant'
    }
  };
}
