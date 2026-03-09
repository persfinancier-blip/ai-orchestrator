import test from 'node:test';
import assert from 'node:assert/strict';

import {
  buildComputedFieldPreview,
  buildExpressionFromBuilder,
  createComputedFieldRule,
  duplicateComputedFieldRule,
  normalizeComputedFieldRule,
  serializeComputedFieldRule
} from '../desk/tabs/computedFieldBuilderCore.js';

test('computed field builder: default rule starts in builder mode', () => {
  const rule = createComputedFieldRule();
  assert.equal(rule.mode, 'builder');
  assert.equal(typeof rule.expression, 'string');
  assert.ok(rule.expression.length > 0);
});

test('computed field builder: if builder composes russian expression preview', () => {
  const preview = buildExpressionFromBuilder({
    category: 'condition',
    functionKey: 'if',
    condition: {
      left: { sourceType: 'field', field: 'price' },
      operator: 'gt',
      right: { sourceType: 'number', numberValue: '0' }
    },
    whenTrue: {
      sourceType: 'expression',
      builder: {
        category: 'math',
        functionKey: 'multiply',
        args: [
          { sourceType: 'field', field: 'price' },
          { sourceType: 'number', numberValue: '0.9' }
        ]
      }
    },
    whenFalse: { sourceType: 'number', numberValue: '0' }
  });

  assert.equal(preview, 'если({price} > 0, {price} * 0.9, 0)');
});

test('computed field builder: serialize builder rule stores compiled expression', () => {
  const raw = normalizeComputedFieldRule({
    name: 'discount_value',
    mode: 'builder',
    type: 'numeric',
    builder: {
      category: 'math',
      functionKey: 'round',
      args: [
        { sourceType: 'field', field: 'price' },
        { sourceType: 'number', numberValue: '2' }
      ]
    }
  });

  const serialized = serializeComputedFieldRule(raw);
  assert.equal(serialized.expression, 'округлить({price}, 2)');
  assert.equal(buildComputedFieldPreview(serialized), 'округлить({price}, 2)');
});

test('computed field builder: duplicate keeps builder payload and renames field', () => {
  const duplicated = duplicateComputedFieldRule({
    name: 'margin_value',
    mode: 'builder',
    type: 'numeric',
    builder: {
      category: 'math',
      functionKey: 'subtract',
      args: [
        { sourceType: 'field', field: 'price' },
        { sourceType: 'field', field: 'cost' }
      ]
    }
  });

  assert.equal(duplicated.name, 'margin_value_copy');
  assert.equal(duplicated.expression, '{price} - {cost}');
});
