import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const root = resolve(__dirname, '..');

function readProjectFile(relativePath) {
  return readFileSync(resolve(root, relativePath), 'utf8');
}

function functionBody(source, name) {
  const needle = `function ${name}`;
  const start = source.indexOf(needle);
  assert.notEqual(start, -1, `function ${name} not found`);
  const signatureTail = source.slice(start).match(/\)\s*\{/);
  const openBrace = signatureTail ? start + Number(signatureTail.index) + signatureTail[0].lastIndexOf('{') : source.indexOf('{', start);
  assert.notEqual(openBrace, -1, `function ${name} opening brace not found`);
  let depth = 0;
  for (let idx = openBrace; idx < source.length; idx += 1) {
    const char = source[idx];
    if (char === '{') depth += 1;
    if (char === '}') {
      depth -= 1;
      if (depth === 0) return source.slice(openBrace, idx + 1);
    }
  }
  assert.fail(`function ${name} closing brace not found`);
}

test('node assistant endpoint returns structured result', () => {
  const source = readProjectFile('server/tableBuilder.mjs');

  assert.match(source, /tableBuilderRouter\.post\('\/node-assistant\/recommend'/);
  assert.match(source, /runNodeAssistantFlow/);
  assert.match(source, /node_assistant:\s*result/);
  assert.match(source, /documentation_fetches/);
  assert.match(source, /web_search/);
});

test('workflow node modal uses shared node assistant panel and draft apply', () => {
  const source = readProjectFile('desk/components/WorkflowCanvas.svelte');
  const applyBody = functionBody(source, 'applyNodeAssistantRecommendations');

  assert.match(source, /NodeAssistantPanel/);
  assert.match(source, /nodeAssistantContextForNode/);
  assert.match(applyBody, /rec\?\.apply_allowed/);
  assert.match(applyBody, /setByPath/);
  assert.doesNotMatch(applyBody, /saveDesk\s*\(/);
  assert.doesNotMatch(applyBody, /workflow-desks\/upsert/);
});
