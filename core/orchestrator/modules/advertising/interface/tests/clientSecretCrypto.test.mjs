import test from 'node:test';
import assert from 'node:assert/strict';
import {
  clientSecretKeyConfigured,
  decryptClientSecret,
  encryptClientSecret,
  isEncryptedClientSecret
} from '../server/runtime/clientSecretCrypto.mjs';
import { encryptClientSecretPatch } from '../server/clientSecretsMiddleware.mjs';
import { clientSecretMigrationTestkit } from '../server/clientSecretMigration.mjs';

const KEY = 'test-client-secret-key';

test('client credential encryption round-trips with authenticated encryption', () => {
  const encrypted = encryptClientSecret('top-secret-token', KEY);
  assert.equal(isEncryptedClientSecret(encrypted), true);
  assert.notEqual(encrypted, 'top-secret-token');
  assert.equal(decryptClientSecret(encrypted, KEY), 'top-secret-token');
});

test('wrong key and tampering are rejected', () => {
  const encrypted = encryptClientSecret('top-secret-token', KEY);
  assert.throws(() => decryptClientSecret(encrypted, 'wrong-key'));
  const tampered = `${encrypted.slice(0, -1)}${encrypted.endsWith('A') ? 'B' : 'A'}`;
  assert.throws(() => decryptClientSecret(tampered, KEY));
});

test('new access secret patch encrypts secret fields but not metadata', () => {
  const patch = encryptClientSecretPatch({
    token_value: 'token',
    password_value: 'password',
    api_key: 'key',
    login_value: 'operator',
    cabinet_id: 'cab-1'
  }, KEY);
  assert.equal(decryptClientSecret(patch.token_value, KEY), 'token');
  assert.equal(decryptClientSecret(patch.password_value, KEY), 'password');
  assert.equal(decryptClientSecret(patch.api_key, KEY), 'key');
  assert.equal(patch.login_value, 'operator');
  assert.equal(patch.cabinet_id, 'cab-1');
});

test('migration helper only selects plaintext non-empty secrets', () => {
  assert.equal(clientSecretMigrationTestkit.needsEncryption('plain'), true);
  assert.equal(clientSecretMigrationTestkit.needsEncryption(encryptClientSecret('plain', KEY)), false);
  assert.equal(clientSecretMigrationTestkit.needsEncryption(''), false);
});

test('client encryption key must be explicitly configured', () => {
  assert.equal(clientSecretKeyConfigured(KEY), true);
  assert.equal(clientSecretKeyConfigured(''), false);
});
