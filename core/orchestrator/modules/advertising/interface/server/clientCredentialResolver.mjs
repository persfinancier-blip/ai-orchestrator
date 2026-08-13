import { decryptClientSecret } from './runtime/clientSecretCrypto.mjs';

function norm(value) {
  return String(value || '').trim().toLowerCase();
}

export function normalizeProviderCode(value) {
  const raw = norm(value).replace(/[^a-zа-яё0-9]+/gi, '_').replace(/^_+|_+$/g, '');
  if (!raw) return '';
  if (['wb', 'wildberries', 'вайлдберриз'].includes(raw)) return 'wildberries';
  if (['ozon', 'озон'].includes(raw) || raw.includes('ozon')) return 'ozon';
  if (raw === 'yandex' || raw === 'яндекс' || raw.includes('yandex_market') || raw.includes('яндекс_маркет')) return 'yandex_market';
  return raw;
}

function secret(value) {
  return value === undefined || value === null || String(value) === '' ? '' : decryptClientSecret(value);
}

function buildAliases(row, providerCode) {
  const token = secret(row?.token_value);
  const apiKey = secret(row?.api_key);
  const password = secret(row?.password_value);
  const login = String(row?.login_value || '').trim();
  const cabinetId = String(row?.cabinet_id || '').trim();
  const aliases = {
    client_access_token: token,
    client_token: token,
    client_api_key: apiKey,
    client_password: password,
    client_login: login,
    client_cabinet_id: cabinetId
  };

  if (providerCode === 'ozon') {
    aliases.ozon_performance_token = token;
    aliases.ozon_token = token;
    aliases.ozon_api_key = apiKey;
    aliases.ozon_client_id = cabinetId || String(row?.external_id || '').trim();
  } else if (providerCode === 'wildberries') {
    aliases.wb_token = token;
    aliases.wildberries_token = token;
    aliases.wb_api_key = apiKey;
  } else if (providerCode === 'yandex_market') {
    aliases.yandex_market_token = token;
    aliases.yandex_token = token;
    aliases.yandex_api_key = apiKey;
  }

  return Object.fromEntries(Object.entries(aliases).filter(([, value]) => String(value || '') !== ''));
}

export async function resolveClientCredentialAliases(client, options = {}) {
  const clientId = Math.trunc(Number(options.clientId || 0));
  const providerCode = normalizeProviderCode(options.providerCode);
  const cabinetId = String(options.cabinetId || '').trim();
  if (!(clientId > 0) || !providerCode) {
    return { bound: false, aliases: {}, secret_values: [], access: null };
  }

  const params = [clientId, providerCode];
  let cabinetSql = '';
  if (cabinetId) {
    params.push(cabinetId);
    cabinetSql = ` AND COALESCE(cabinet_id, '') = $${params.length}`;
  }

  const result = await client.query(
    `SELECT id, client_id, platform_code, platform_name, system_name, external_id,
            cabinet_name, cabinet_id, auth_type, token_value, login_value,
            password_value, api_key, access_scope, access_mode, check_status,
            expires_at
       FROM ao_clients.client_accesses
      WHERE client_id = $1
        AND lower(trim(COALESCE(platform_code, ''))) = $2
        AND COALESCE(is_active, true) = true
        ${cabinetSql}
      ORDER BY id ASC
      LIMIT 3`,
    params
  );

  const rows = Array.isArray(result?.rows) ? result.rows : [];
  if (!rows.length) {
    const error = new Error(`client_credential_not_found:${providerCode}`);
    error.code = 'client_credential_not_found';
    throw error;
  }
  if (rows.length > 1) {
    const error = new Error(`client_credential_ambiguous:${providerCode}`);
    error.code = 'client_credential_ambiguous';
    throw error;
  }

  const row = rows[0];
  if (row?.expires_at && new Date(row.expires_at).getTime() <= Date.now()) {
    const error = new Error(`client_credential_expired:${providerCode}`);
    error.code = 'client_credential_expired';
    throw error;
  }

  const aliases = buildAliases(row, providerCode);
  const secretValues = [aliases.client_access_token, aliases.client_api_key, aliases.client_password]
    .map((value) => String(value || ''))
    .filter(Boolean);

  return {
    bound: true,
    aliases,
    secret_values: [...new Set(secretValues)],
    access: {
      id: Number(row.id || 0),
      client_id: Number(row.client_id || 0),
      provider_code: providerCode,
      cabinet_id: String(row.cabinet_id || ''),
      cabinet_name: String(row.cabinet_name || ''),
      auth_type: String(row.auth_type || ''),
      access_mode: String(row.access_mode || ''),
      check_status: String(row.check_status || '')
    }
  };
}

export const clientCredentialResolverTestkit = Object.freeze({ norm, buildAliases });
