import test from 'node:test';
import assert from 'node:assert/strict';

import { buildSourceNodePreviewFromTemplate } from '../desk/tabs/parserSourcePreviewCore.js';

test('parser source preview: api template uses output contract aliases automatically', () => {
  const preview = buildSourceNodePreviewFromTemplate({
    templateType: 'api_request',
    output_parameters: [
      { root_path: 'responses.products[]', path: 'id', alias: 'products_id', value_type: 'Число' },
      { root_path: 'responses.products[]', path: 'title', alias: 'products_title', value_type: 'Текст' },
      { root_path: 'responses.products[]', path: 'isActive', alias: 'products_is_active', value_type: 'Да / нет' }
    ]
  });

  assert.equal(preview.rows.length, 1);
  assert.deepEqual(preview.columns, ['products_id', 'products_title', 'products_is_active']);
  assert.deepEqual(preview.rows[0], {
    products_id: 0,
    products_title: 'products_title',
    products_is_active: false
  });
});

test('parser source preview: parser template derives preview from mapping fields', () => {
  const preview = buildSourceNodePreviewFromTemplate({
    templateType: 'table_parser',
    config_json: {
      selectFields: 'id, vendorCode',
      renameMap: { vendorCode: 'vendor_code' },
      typeMap: { id: 'integer', vendor_code: 'text' }
    }
  });

  assert.equal(preview.rows.length, 1);
  assert.deepEqual(preview.columns, ['id', 'vendor_code']);
  assert.deepEqual(preview.rows[0], {
    id: 0,
    vendor_code: 'vendor_code'
  });
});

test('parser source preview: parser template respects path-keyed rename and default values', () => {
  const preview = buildSourceNodePreviewFromTemplate({
    templateType: 'table_parser',
    config_json: {
      selectFields: 'meta.id',
      renameMap: { 'meta.id': 'product_id' },
      defaultValues: { 'meta.id': '100' },
      typeMap: { 'meta.id': 'integer' }
    }
  });

  assert.equal(preview.rows.length, 1);
  assert.deepEqual(preview.columns, ['product_id']);
  assert.deepEqual(preview.rows[0], { product_id: '100' });
});

test('parser source preview: missing contract returns russian explanation instead of fake json error', () => {
  const preview = buildSourceNodePreviewFromTemplate({
    templateType: 'api_request',
    output_parameters: []
  });

  assert.equal(preview.rows.length, 0);
  assert.match(preview.message, /выходные параметры/i);
});
