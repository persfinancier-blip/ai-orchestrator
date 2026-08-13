import express from 'express';

const COOKIE = 'ao_client_id';

function readClientId(req) {
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

export const clientContextRouter = express.Router();

clientContextRouter.get('/context/client', (req, res) => {
  res.json({ client_id: readClientId(req) || null });
});

clientContextRouter.post('/context/client', (req, res) => {
  const id = Math.trunc(Number(req.body?.client_id || 0));
  if (!(id > 0)) return res.status(400).json({ error: 'invalid_client_id' });
  res.setHeader('Set-Cookie', `${COOKIE}=${id}; Path=/; HttpOnly; SameSite=Strict; Max-Age=2592000`);
  return res.json({ ok: true, client_id: id });
});

clientContextRouter.delete('/context/client', (_req, res) => {
  res.setHeader('Set-Cookie', `${COOKIE}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0`);
  res.json({ ok: true });
});

export function injectClientContext(req, _res, next) {
  const id = readClientId(req);
  if (id > 0) req.headers['x-ao-client-id'] = String(id);
  next();
}
