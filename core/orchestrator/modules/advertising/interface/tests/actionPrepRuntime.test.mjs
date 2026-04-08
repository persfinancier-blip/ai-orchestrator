import test from 'node:test';
import assert from 'node:assert/strict';

const { previewActionPrepConfig, executeActionPrepConfig } = await import('../server/actionPrepRuntime.mjs');

function createClient(rows = []) {
  return {
    async query() {
      return { rows };
    }
  };
}

test('action prep runtime: reads upstream rows, filters them and appends action columns', async () => {
  const result = await executeActionPrepConfig(
    createClient(),
    {
      sourceMode: 'node',
      filterRulesJson: JSON.stringify([{ field: 'roas', operator: '<', value: '4' }]),
      actionColumnsJson: JSON.stringify([
        { name: 'action_type', mode: 'constant', constantValue: 'update_bid' },
        { name: 'new_bid', mode: 'percent_change', baseField: 'current_bid', percentValue: '5', type: 'numeric' },
        { name: 'reason', mode: 'string_template', template: '{sku}:low_roas' }
      ])
    },
    {
      inputValue: {
        contract_version: 'node_io_v1',
        rows: [
          { campaign_id: 1, sku: 'A', current_bid: 100, roas: 3.2 },
          { campaign_id: 2, sku: 'B', current_bid: 90, roas: 4.8 }
        ]
      }
    }
  );

  assert.equal(result.rows.length, 1);
  assert.equal(result.rows[0].action_type, 'update_bid');
  assert.equal(result.rows[0].new_bid, 105);
  assert.equal(result.rows[0].reason, 'A:low_roas');
  assert.deepEqual(result.columns.sort(), ['action_type', 'campaign_id', 'current_bid', 'new_bid', 'reason', 'roas', 'sku']);
});

test('action prep runtime: supports table source preview and formula / if-else columns', async () => {
  const preview = await previewActionPrepConfig(
    createClient([
      { id: 10, current_bid: 80, spend: 1000, roas: 2.5 },
      { id: 11, current_bid: 110, spend: 900, roas: 5.1 }
    ]),
    {
      sourceMode: 'table',
      sourceSchema: 'ao_data',
      sourceTable: 'silver_ads',
      actionColumnsJson: JSON.stringify([
        { name: 'new_bid', mode: 'formula', expression: '{current_bid} * 1.1', type: 'numeric' },
        {
          name: 'decision',
          mode: 'if_else',
          conditionExpression: '{roas} < 3',
          trueValue: 'decrease',
          falseValue: 'keep'
        }
      ])
    }
  );

  assert.equal(preview.input_row_count, 2);
  assert.equal(preview.row_count, 2);
  assert.equal(preview.sample_rows[0].new_bid, 88);
  assert.equal(preview.sample_rows[0].decision, 'decrease');
  assert.equal(preview.sample_rows[1].decision, 'keep');
});
