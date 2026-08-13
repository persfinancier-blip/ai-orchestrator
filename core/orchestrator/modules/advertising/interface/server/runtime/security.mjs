import crypto from 'node:crypto';

function boolEnv(value, fallback = false) {
  if (value === undefined || value === null || value === '') return fallback;
  return ['1', 'true', 'yes', 'on'].includes(String(value).trim().toLowerCase());
}

function safeEqual(left, right) {
  const a = Buffer.from(String(left || ''), 'utf8');
  const b = Buffer.from(String(right || ''), 'utf8');
  if (a.length !== b.length || a.length === 0) return false;
  return crypto.timingSafeEqual(a, b);
}

export function readBearerToken(req) {
  const header = String(req?.header?.('authorization') || '').trim();
  const match = /^Bearer\s+(.+)$/i.exec(header);
  return match ? match[1].trim() : '';
}

export function buildControlAuth(options = {}) {
  const configuredToken = String(options.token ?? process.env.AO_CONTROL_TOKEN ?? '').trim();
  const allowInsecure = boolEnv(
    options.allowInsecure ?? process.env.AO_CONTROL_ALLOW_INSECURE,
    process.env.NODE_ENV !== 'production'
  );
  const trustedRole = String(options.trustedRole || 'data_admin').trim() || 'data_admin';

  return function controlAuth(req, res, next) {
    if (!configuredToken) {
      if (allowInsecure) return next();
      return res.status(503).json({
        error: 'control_plane_not_configured',
        details: 'AO_CONTROL_TOKEN is required'
      });
    }

    const supplied = readBearerToken(req);
    if (!supplied) {
      return res.status(401).json({ error: 'unauthorized', details: 'Bearer token required' });
    }
    if (!safeEqual(configuredToken, supplied)) {
      return res.status(403).json({ error: 'forbidden' });
    }

    req.headers['x-ao-role'] = trustedRole;
    return next();
  };
}

export const runtimeSecurityTestkit = Object.freeze({ boolEnv, safeEqual });
