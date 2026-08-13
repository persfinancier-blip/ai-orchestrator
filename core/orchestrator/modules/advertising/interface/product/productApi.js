export const API_BASE = '/ai-orchestrator/api';

export async function productApi(path, init = {}) {
  const response = await fetch(`${API_BASE}${path}`, {
    cache: init.cache ?? 'no-store',
    ...init,
    headers: {
      Accept: 'application/json',
      ...(init.body ? { 'Content-Type': 'application/json' } : {}),
      ...(init.headers || {})
    }
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message = payload?.details || payload?.error || `${response.status} ${response.statusText}`;
    const error = new Error(String(message));
    error.status = response.status;
    throw error;
  }
  return payload;
}

export function productGet(path) {
  return productApi(path);
}

export function productPost(path, body = {}) {
  return productApi(path, { method: 'POST', body: JSON.stringify(body) });
}
