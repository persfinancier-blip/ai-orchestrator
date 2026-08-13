const token = String(process.env.AO_CONTROL_TOKEN || '').trim();

if (!token) {
  console.error('AI Orchestrator startup refused: AO_CONTROL_TOKEN is required.');
  process.exit(78);
}

process.env.NODE_ENV = 'production';
process.env.AO_CONTROL_ALLOW_INSECURE = 'false';

await import('./spaceServer.mjs');
