import test from 'node:test';
import assert from 'node:assert/strict';

import {
  buildAliasFromPath,
  buildOutputFieldsFromNode,
  fullOutputContractPath,
  normalizeTemplatePath,
  resolveOutputContractValue
} from '../desk/tabs/outputContractCore.js';

test('output contract: normalizes test response path into template path', () => {
  assert.equal(normalizeTemplatePath('responses[0].response.cards[0].nmID'), 'cards[].nmID');
  assert.equal(normalizeTemplatePath('response.items[12].vendorCode'), 'items[].vendorCode');
});

test('output contract: builds stable snake_case aliases', () => {
  assert.equal(buildAliasFromPath('nmID'), 'nm_id');
  assert.equal(buildAliasFromPath('subjectName'), 'subject_name');
  assert.equal(buildAliasFromPath('dimensions.width'), 'dimensions_width');
});

test('output contract: builds fields from selected sample node massively', () => {
  const rows = buildOutputFieldsFromNode(
    {
      nmID: 1,
      vendorCode: 'A-1',
      dimensions: { width: 10, height: 20 }
    },
    'cards[]'
  );
  const byAlias = new Map(rows.map((item) => [item.alias, item]));
  assert.equal(rows.length, 4);
  assert.equal(byAlias.get('nm_id')?.rootPath, 'cards[]');
  assert.equal(byAlias.get('nm_id')?.path, 'nmID');
  assert.equal(byAlias.get('dimensions_width')?.path, 'dimensions.width');
  assert.equal(byAlias.get('dimensions_height')?.valueType, 'Число');
});

test('output contract: resolves value by alias entry against wrapped API test payload', () => {
  const payload = {
    responses: [
      {
        response: {
          cards: [
            {
              nmID: 123,
              vendorCode: 'ABC-1'
            }
          ]
        }
      }
    ]
  };
  const entry = {
    rootPath: 'cards[]',
    path: 'nmID',
    alias: 'nm_id',
    valueType: 'Число'
  };
  assert.equal(fullOutputContractPath(entry), 'cards[].nmID');
  assert.equal(resolveOutputContractValue(payload, entry), 123);
});
