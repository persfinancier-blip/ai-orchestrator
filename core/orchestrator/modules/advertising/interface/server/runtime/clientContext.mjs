import crypto from 'node:crypto';

const COOKIE_NAME = 'ao_client';

function sign(value, secret) {
  return crypto.createHmac('sha256', secret).update(String(value)).digest('base64url');
}

function safeEqual(left, right) {
  const a = Buffer.from(String(left || ''));
  const b = Buffer.from(String(right || ''));
  return a.length > 0 && a.length === b.length && crypto.timingSafeEqual(a, b);
}

function readCookie(req, name = COOKIE_NAME) {
  const raw = String(req?.headers?.cookie || '');
  for (const part of raw.split(';')) {
    const index = part.indexOf('=');
    if (index > 0 && part.slice(0, index).trim() === name) return decodeURIComponent(part.slice(index + 1).trim());
  }
  return '';
}

export function encodeClientContext(clientId, secret) {
  const id = Math.trunc(Number(clientId || 0));
  if (!(id > 0) || !secret) return '';
  return `${id}.${sign(id, secret)}`;
}

export function decodeClientContext(token, secret) {
  const [rawId, supplied] = String(token || '').split('.');
  const id = Math.trunc(Number(rawId || 0));
  if (!(id > 0) || !supplied || !secret || !safeEqual(supplied, sign(id, secret))) return 0;
  return id;
}

export function readClientContext(req, secret) {
  return decodeClientContext(readCookie(req), secret);
}

export function clientContextCookie(clientId, secret, options = {}) {
  const token = encodeClientContext(clientId, secret);
  const secure = Boolean(options.secure);
  return `${COOKIE_NAME}=${encodeURIComponent(token)}; Path=/; HttpOnly; SameSite=Strict; Max-Age=2592000${secure ? '; Secure' : ''}`;
}

export function clearClientContextCookie(options = {}) {
  return `${COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0${options.secure ? '; Secure' : ''}`;
}

export const clientContextTestkit = Object.freeze({ sign, safeEqual, readCookie, COOKIE_NAME });
