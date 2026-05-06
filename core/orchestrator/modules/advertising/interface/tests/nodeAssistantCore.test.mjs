import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { DEFAULT_NODE_EDITOR_SCHEMAS, runNodeAssistantFlow } from '../shared/nodeAssistantCore.mjs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const root = resolve(__dirname, '..');

function readProjectFile(relativePath) {
  return readFileSync(resolve(root, relativePath), 'utf8');
}

test('docs url is not applied as api url without resolved documentation facts', async () => {
  const result = await runNodeAssistantFlow({
    node_type_code: 'http_request',
    node_kind: 'tool',
    registry_row: {
      node_type_code: 'http_request',
      editor_type_code: 'http_request',
      editor_schema_json: DEFAULT_NODE_EDITOR_SCHEMAS.http_request
    },
    user_text: 'https://docs.example.com/reference/ListCampaigns',
    current_values: {}
  });

  assert.equal(result.source_analysis.has_documentation, true);
  assert.equal(result.apply_patch.by_field.apiUrl, undefined);
  assert.match(result.warnings.join('\n'), /URL документации.*не будет подставлена/);
  assert.equal(result.status, 'clarification_required');
});

test('intent extraction is generic and does not depend on marketplace builtin values', async () => {
  const result = await runNodeAssistantFlow({
    node_type_code: 'http_request',
    node_kind: 'tool',
    registry_row: {
      node_type_code: 'http_request',
      editor_type_code: 'http_request',
      editor_schema_json: DEFAULT_NODE_EDITOR_SCHEMAS.http_request
    },
    user_text: 'хочу получить кампании ozon',
    current_values: {}
  });

  assert.equal(result.intent.identified, true);
  assert.equal(result.intent.action, 'get');
  assert.equal(result.intent.source, 'ozon');
  assert.equal(result.apply_patch.by_field.apiUrl, undefined);
  assert.ok(result.clarification_questions.length > 0);

  const source = readProjectFile('shared/nodeAssistantCore.mjs');
  assert.doesNotMatch(source, /builtinKnowledge/);
  assert.doesNotMatch(source, /api-performance\.ozon\.ru/);
});

test('documentation text can resolve endpoint only when facts are explicit', async () => {
  const result = await runNodeAssistantFlow({
    node_type_code: 'http_request',
    node_kind: 'tool',
    registry_row: {
      node_type_code: 'http_request',
      editor_type_code: 'http_request',
      editor_schema_json: DEFAULT_NODE_EDITOR_SCHEMAS.http_request
    },
    user_text: 'настрой запрос по этой документации https://docs.example.com/campaigns',
    documentation_texts: [
      {
        url: 'https://docs.example.com/campaigns',
        text: 'Base URL: https://api.example.com\nGET /v1/campaigns\nAuthorization: Bearer token'
      }
    ],
    current_values: {}
  });

  assert.equal(result.apply_patch.by_field.apiMethod, 'GET');
  assert.equal(result.apply_patch.by_field.apiUrl, 'https://api.example.com/v1/campaigns');
  assert.match(result.warnings.join('\n'), /URL документации/);
});

test('clarification mode asks questions instead of generating unsafe values', async () => {
  const result = await runNodeAssistantFlow({
    node_type_code: 'http_request',
    node_kind: 'tool',
    registry_row: {
      node_type_code: 'http_request',
      editor_type_code: 'http_request',
      editor_schema_json: DEFAULT_NODE_EDITOR_SCHEMAS.http_request
    },
    user_text: 'настрой интеграцию',
    current_values: {}
  });

  assert.equal(result.status, 'clarification_required');
  assert.ok(result.clarification_questions.some((q) => q.id === 'intent_goal'));
  assert.deepEqual(result.apply_patch.fields, []);
});

test('schema traversal works for non-api node', async () => {
  const result = await runNodeAssistantFlow({
    node_type_code: 'start_process',
    node_kind: 'tool',
    registry_row: {
      node_type_code: 'start_process',
      editor_type_code: 'start_process',
      editor_schema_json: DEFAULT_NODE_EDITOR_SCHEMAS.start_process
    },
    user_text: 'запускать процесс каждые 5 минут',
    current_values: {}
  });

  assert.equal(result.apply_patch.by_field.triggerType, 'interval');
  assert.equal(result.apply_patch.by_field.intervalValue, '5');
  assert.equal(result.apply_patch.by_field.intervalUnit, 'minutes');
  assert.ok(result.node_context.schema.fields.length >= DEFAULT_NODE_EDITOR_SCHEMAS.start_process.fields.length);
});

test('manual fields are preserved by merge strategy', async () => {
  const schema = {
    schema_version: 1,
    supports_ai: true,
    fields: [
      {
        field_key: 'manualMethod',
        label: 'Manual method',
        type: 'text',
        group: 'request',
        semantic_key: 'http_method',
        supports_ai: true,
        merge_strategy: 'preserve_manual',
        draft_path: 'settings.manualMethod'
      }
    ]
  };
  const result = await runNodeAssistantFlow({
    node_type_code: 'custom_request',
    node_kind: 'tool',
    registry_row: {
      node_type_code: 'custom_request',
      editor_type_code: 'custom_request',
      editor_schema_json: schema
    },
    user_text: 'GET https://api.example.com/v1/campaigns',
    current_values: { manualMethod: 'POST' }
  });

  assert.equal(result.apply_patch.by_field.manualMethod, undefined);
  assert.match(result.warnings.join('\n'), /уже есть ручное значение/);
});
