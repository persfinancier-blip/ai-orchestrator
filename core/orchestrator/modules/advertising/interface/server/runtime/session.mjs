import crypto from 'node:crypto';

const COOKIE_NAME = 'ao_session';

function encode(value) {
  return Buffer.from(value, 'utf8').toString('base64url');
}

function decode(value) {
  return Buffer.from(value, 'base64url').toString('utf8');
}

function signature(payload, secret) {
  return crypto.createHmac('sha256', secret).update(payload).digest('base64url');
}

function equal(left, right) {
  const a = Buffer.from(String(left || ''));
  const b = Buffer.from(String(right || ''));
  return a.length > 0 && a.length === b.length && crypto.timingSafeEqual(a, b);
}

export function createSession(secret, ttlSeconds = 8 * 60 * 60) {
  const now = Math.floor(Date.now() / 1000);
  const payload = encode(JSON.stringify({ role: 'data_admin', iat: now, exp: now + ttlSeconds, sid: crypto.randomUUID() }));
  return `${payload}.${signature(payload, secret)}`;
}

export function verifySession(token, secret, nowSeconds = Math.floor(Date.now() / 1000)) {
  const [payload, supplied] = String(token || '').split('.');
  if (!payload || !supplied || !secret || !equal(supplied, signature(payload, secret))) return null;
  try {
    const parsed = JSON.parse(decode(payload));
    if (parsed?.role !== 'data_admin' || Number(parsed?.exp || 0) <= nowSeconds) return null;
    return parsed;
  } catch {
    return null;
  }
}

export function readCookie(req, name = COOKIE_NAME) {
  const raw = String(req?.headers?.cookie || '');
  for (const part of raw.split(';')) {
    const index = part.indexOf('=');
    if (index < 0) continue;
    if (part.slice(0, index).trim() === name) return decodeURIComponent(part.slice(index + 1).trim());
  }
  return '';
}

export function sessionCookie(token, options = {}) {
  const secure = Boolean(options.secure);
  const maxAge = Math.max(60, Number(options.maxAge || 8 * 60 * 60));
  return `${COOKIE_NAME}=${encodeURIComponent(token)}; Path=/; HttpOnly; SameSite=Strict; Max-Age=${maxAge}${secure ? '; Secure' : ''}`;
}

export function clearSessionCookie(options = {}) {
  return `${COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0${options.secure ? '; Secure' : ''}`;
}

export const sessionTestkit = Object.freeze({ encode, decode, signature, equal, COOKIE_NAME });
