import express from 'express';
import { clearSessionCookie, createSession, readCookie, sessionCookie, sessionTestkit, verifySession } from './session.mjs';

function envFlag(value, fallback = false) {
  if (value === undefined || value === null || value === '') return fallback;
  return ['1', 'true', 'yes', 'on'].includes(String(value).trim().toLowerCase());
}

export function createAppAuth(options = {}) {
  const secret = String(options.secret ?? process.env.AO_CONTROL_TOKEN ?? '').trim();
  const production = String(options.nodeEnv ?? process.env.NODE_ENV ?? '').toLowerCase() === 'production';
  const allowInsecure = envFlag(options.allowInsecure ?? process.env.AO_CONTROL_ALLOW_INSECURE, !production);
  const cookieSecure = envFlag(options.cookieSecure ?? process.env.AO_COOKIE_SECURE, production);
  const router = express.Router();

  function currentSession(req) {
    if (!secret && allowInsecure) return { role: 'data_admin', dev: true };
    return secret ? verifySession(readCookie(req), secret) : null;
  }

  router.get('/auth/session', (req, res) => {
    const session = currentSession(req);
    res.json({ authenticated: Boolean(session), role: session?.role || null, dev: Boolean(session?.dev) });
  });

  router.post('/auth/login', (req, res) => {
    if (!secret) {
      if (allowInsecure) return res.json({ ok: true, authenticated: true, dev: true });
      return res.status(503).json({ error: 'auth_not_configured' });
    }
    const supplied = String(req.body?.token || '').trim();
    if (!sessionTestkit.equal(secret, supplied)) return res.status(403).json({ error: 'invalid_credentials' });
    res.setHeader('Set-Cookie', sessionCookie(createSession(secret), { secure: cookieSecure }));
    return res.json({ ok: true, authenticated: true, role: 'data_admin' });
  });

  router.post('/auth/logout', (_req, res) => {
    res.setHeader('Set-Cookie', clearSessionCookie({ secure: cookieSecure }));
    res.json({ ok: true });
  });

  function gate(req, res, next) {
    delete req.headers['x-ao-role'];
    const session = currentSession(req);
    if (!session) return res.status(secret ? 401 : 503).json({ error: secret ? 'authentication_required' : 'auth_not_configured' });
    req.headers['x-ao-role'] = 'data_admin';
    return next();
  }

  return { router, gate, configured: Boolean(secret), allowInsecure };
}

export const appAuthTestkit = Object.freeze({ envFlag });
