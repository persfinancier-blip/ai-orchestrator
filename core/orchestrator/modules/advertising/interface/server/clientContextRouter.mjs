import express from 'express';

const COOKIE = 'ao_client_id';
const COOKIE_ATTRS = 'Path=/; HttpOnly; SameSite=Strict; Max-Age=2592000';

export function readClientId(req) {
  const raw = String(req?.headers?.cookie || '');
  for (const part of raw.split(';')) {
    const [name, value] = part.trim().split('=');
    if (name === COOKIE) {
      const id = Math.trunc(Number(decodeURIComponent(value || '0')));
      return id > 0 ? id : 0;
    }
  }
  return 0;
}

function setClientCookie(res, id) {
  res.setHeader('Set-Cookie', `${COOKIE}=${id}; ${COOKIE_ATTRS}`);
}

export const clientContextRouter = express.Router();

clientContextRouter.get('/context/client', (req, res) => {
  res.json({ client_id: readClientId(req) || null });
});

clientContextRouter.post('/context/client', (req, res) => {
  const id = Math.trunc(Number(req.body?.client_id || 0));
  if (!(id > 0)) return res.status(400).json({ error: 'invalid_client_id' });
  setClientCookie(res, id);
  return res.json({ ok: true, client_id: id });
});

clientContextRouter.delete('/context/client', (_req, res) => {
  res.setHeader('Set-Cookie', `${COOKIE}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0`);
  res.json({ ok: true });
});

export function injectClientContext(req, res, next) {
  const detailMatch = String(req.path || '').match(/^\/clients\/module\/client\/(\d+)$/);
  const routeId = detailMatch ? Math.trunc(Number(detailMatch[1] || 0)) : 0;
  const id = routeId > 0 ? routeId : readClientId(req);

  if (routeId > 0 && routeId !== readClientId(req)) setClientCookie(res, routeId);
  if (id > 0) {
    req.headers['x-ao-client-id'] = String(id);
    if (req.path === '/process-runs/trigger' && req.body && typeof req.body === 'object') {
      req.body.client_id = id;
      req.body.context_json = { ...(req.body.context_json || {}), client_id: id };
    }
  }
  next();
}
