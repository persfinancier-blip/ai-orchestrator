const DEFAULT_RETRYABLE_STATUS = new Set([408, 425, 429, 500, 502, 503, 504]);

export function computeRetryDelayMs(options = {}) {
  const attempt = Math.max(1, Math.trunc(Number(options.attempt || 1)));
  const baseMs = Math.max(1, Math.trunc(Number(options.baseMs || 250)));
  const maxMs = Math.max(baseMs, Math.trunc(Number(options.maxMs || 60_000)));
  const jitter = Math.max(0, Math.min(1, Number(options.jitter ?? 0.25)));
  const random = typeof options.random === 'function' ? options.random : Math.random;
  const exponential = Math.min(maxMs, baseMs * 2 ** Math.max(0, attempt - 1));
  const spread = exponential * jitter;
  const sample = Math.max(0, Math.min(1, Number(random())));
  return Math.round(Math.max(0, exponential - spread + sample * spread * 2));
}

export function isRetryableFailure(errorLike = {}) {
  const status = Number(errorLike.status ?? errorLike.statusCode ?? errorLike.http_status ?? 0);
  if (DEFAULT_RETRYABLE_STATUS.has(status)) return true;
  if (status >= 500 && status <= 599) return true;
  const code = String(errorLike.code || '').trim().toUpperCase();
  return [
    'ECONNRESET',
    'ECONNREFUSED',
    'ETIMEDOUT',
    'EAI_AGAIN',
    'UND_ERR_CONNECT_TIMEOUT',
    'UND_ERR_HEADERS_TIMEOUT',
    'UND_ERR_SOCKET'
  ].includes(code);
}

export function nextRetryAt(options = {}) {
  const nowMs = Number(options.nowMs ?? Date.now());
  const delayMs = computeRetryDelayMs(options);
  return { delayMs, availableAt: new Date(nowMs + delayMs).toISOString() };
}
