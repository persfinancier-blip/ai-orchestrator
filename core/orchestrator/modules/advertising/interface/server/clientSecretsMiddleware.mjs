const MASK = '••••••••';
const SECRET_FIELDS = Object.freeze(['token_value', 'password_value', 'api_key']);

function maskRecord(record) {
  if (!record || typeof record !== 'object' || Array.isArray(record)) return record;
  const next = { ...record };
  for (const field of SECRET_FIELDS) {
    if (next[field] !== undefined && next[field] !== null && String(next[field]) !== '') next[field] = MASK;
  }
  return next;
}

export function maskClientSecrets(payload) {
  if (!payload || typeof payload !== 'object') return payload;
  if (Array.isArray(payload)) return payload.map(maskClientSecrets);
  const next = { ...payload };
  if (Array.isArray(next.accesses)) next.accesses = next.accesses.map(maskRecord);
  if (next.detail && typeof next.detail === 'object') next.detail = maskClientSecrets(next.detail);
  if (next.saved && typeof next.saved === 'object') next.saved = maskRecord(next.saved);
  return next;
}

export function clientSecretsMiddleware(req, res, next) {
  if (!String(req.path || '').startsWith('/clients/module/')) return next();

  if (req.method === 'POST' && req.path === '/clients/module/section/upsert' && req.body?.section === 'accesses' && req.body?.record) {
    const record = { ...req.body.record };
    for (const field of SECRET_FIELDS) {
      if (record[field] === MASK) delete record[field];
    }
    req.body = { ...req.body, record };
  }

  const sendJson = res.json.bind(res);
  res.json = (payload) => sendJson(maskClientSecrets(payload));
  next();
}

export const clientSecretsTestkit = Object.freeze({ MASK, SECRET_FIELDS, maskRecord });
