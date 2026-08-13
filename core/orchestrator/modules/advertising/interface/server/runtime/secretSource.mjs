import fs from 'node:fs';

const DEFAULT_CONTROL_SECRET_FILE = '/etc/ai-orchestrator/control-token';

function secureFileMode(stat) {
  if (!stat || process.platform === 'win32') return true;
  return (Number(stat.mode || 0) & 0o077) === 0;
}

export function resolveControlSecret(options = {}) {
  const explicit = options.secret;
  if (explicit !== undefined && explicit !== null && String(explicit).trim()) return String(explicit).trim();

  const envSecret = String(process.env.AO_CONTROL_TOKEN || '').trim();
  if (envSecret) return envSecret;

  const filePath = String(options.filePath || process.env.AO_CONTROL_TOKEN_FILE || DEFAULT_CONTROL_SECRET_FILE).trim();
  if (!filePath) return '';
  try {
    const stat = fs.statSync(filePath);
    if (!stat.isFile() || !secureFileMode(stat)) return '';
    return String(fs.readFileSync(filePath, 'utf8') || '').trim();
  } catch {
    return '';
  }
}

export const secretSourceTestkit = Object.freeze({ DEFAULT_CONTROL_SECRET_FILE, secureFileMode });
