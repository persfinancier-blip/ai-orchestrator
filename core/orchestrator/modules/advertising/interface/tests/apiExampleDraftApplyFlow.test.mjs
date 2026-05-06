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
  const openBrace = signatureTail
    ? start + Number(signatureTail.index) + signatureTail[0].lastIndexOf('{')
    : source.indexOf('{', start);
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

test('api example apply in node modal hydrates draft without saving desk', () => {
  const source = readProjectFile('desk/components/WorkflowCanvas.svelte');
  const body = functionBody(source, 'onApiBuilderTemplatePatchApplied');

  assert.match(body, /applyApiBuilderRequestPatchToNode/);
  assert.match(body, /applyApiBuilderPaginationPatchToNode/);
  assert.match(body, /deskAutosavePausedForApiDraft\s*=\s*true/);
  assert.doesNotMatch(body, /saveDesk\s*\(/);
  assert.doesNotMatch(body, /workflow-desks\/upsert/);
  assert.match(body, /Desk не сохранен/);
});

test('api template save from node modal updates node draft without saving desk', () => {
  const source = readProjectFile('desk/components/WorkflowCanvas.svelte');
  const body = functionBody(source, 'onApiBuilderTemplateSaved');

  assert.match(body, /applyTemplateToNode/);
  assert.match(body, /deskAutosavePausedForApiDraft\s*=\s*true/);
  assert.doesNotMatch(body, /saveDesk\s*\(/);
  assert.doesNotMatch(body, /workflow-desks\/upsert/);
  assert.match(body, /ручного «Сохранить»/);
});

test('desk autosave skips api recommendation draft until manual save', () => {
  const source = readProjectFile('desk/components/WorkflowCanvas.svelte');
  const autosaveBody = functionBody(source, 'restartDeskAutosaveTimer');
  const saveBody = functionBody(source, 'saveDesk');

  assert.match(autosaveBody, /deskAutosaveAiDraftSignature/);
  assert.match(autosaveBody, /deskCurrentSignature\s*===\s*deskAutosaveAiDraftSignature/);
  assert.match(saveBody, /deskAutosavePausedForApiDraft\s*=\s*false/);
  assert.match(saveBody, /deskAutosaveAiDraftSignature\s*=\s*''/);
  assert.match(source, /Автосохранение ждёт ручного сохранения/);
});

test('api constructor apply hydrates form draft without template upsert', () => {
  const source = readProjectFile('desk/tabs/ApiBuilderTab.svelte');
  const body = functionBody(source, 'applyTemplateParseRecommendations');

  assert.match(body, /applyApiExamplePatchToDraft/);
  assert.match(body, /mutateSelected/);
  assert.match(body, /dispatchTemplatePatchApplied/);
  assert.doesNotMatch(body, /api-configs\/upsert/);
  assert.doesNotMatch(body, /saveSelected\s*\(/);
  assert.match(body, /Подставлено в текущую форму/);
  assert.match(body, /Нажми «Проверить»/);
});

test('api example preview explains draft-only apply', () => {
  const source = readProjectFile('desk/components/ApiExampleParseResult.svelte');

  assert.match(source, /Что будет подставлено в форму/);
  assert.match(source, /не сохраняют API template и Desk автоматически/);
  assert.match(source, /Применить рекомендованные настройки в форму/);
});
