import test from 'node:test';
import assert from 'node:assert/strict';

import { executeTableNodeConfig, previewTableNodeConfig } from '../server/tableNodeRuntime.mjs';

function createTableClient(fixtures = {}) {
  return {
    async query(sql, params = []) {
      const text = String(sql || '').replace(/\s+/g, ' ').trim();
      if (/information_schema\.columns/i.test(text)) {
        const [schema, table] = params;
        const meta = fixtures[`${schema}.${table}`];
        return {
          rows: (meta?.columns || []).map((column) => ({
            column_name: column.name,
            data_type: column.type
          }))
        };
      }

      const match = text.match(/FROM\s+"([^"]+)"\."([^"]+)"/i);
      if (!match) throw new Error(`unexpected_sql:${text}`);
      const [, schema, table] = match;
      const fixture = fixtures[`${schema}.${table}`];
      if (!fixture) throw new Error(`missing_fixture:${schema}.${table}`);

      const limitMatch = text.match(/LIMIT\s+(\d+)/i);
      const offsetMatch = text.match(/OFFSET\s+(\d+)/i);
      const limit = limitMatch ? Number(limitMatch[1]) : fixture.rows.length;
      const offset = offsetMatch ? Number(offsetMatch[1]) : 0;
      const selectPart = text.match(/^SELECT\s+(.+?)\s+FROM\s+/i)?.[1] || '*';
      const selectedColumns =
        selectPart === '*'
          ? []
          : selectPart
              .split(',')
              .map((item) => item.trim().replace(/^"|"$/g, ''))
              .filter(Boolean);
      const rows = fixture.rows.slice(offset, offset + limit).map((row) => {
        if (!selectedColumns.length) return { ...row };
        return Object.fromEntries(selectedColumns.map((column) => [column, row[column]]));
      });
      return { rows };
    }
  };
}

const FIXTURES = {
  'ao_data.products': {
    columns: [
      { name: 'id', type: 'integer' },
      { name: 'sku', type: 'text' },
      { name: 'brand_id', type: 'integer' },
      { name: 'qty', type: 'integer' }
    ],
    rows: [
      { id: 1, sku: 'wb-1', brand_id: 10, qty: 2 },
      { id: 2, sku: 'wb-2', brand_id: 20, qty: 1 }
    ]
  },
  'ao_ref.brands': {
    columns: [
      { name: 'brand_id', type: 'integer' },
      { name: 'brand_name', type: 'text' }
    ],
    rows: [
      { brand_id: 10, brand_name: 'Acme' },
      { brand_id: 20, brand_name: 'Beta' }
    ]
  }
};

function workingConfig(overrides = {}) {
  return {
    baseSchema: 'ao_data',
    baseTable: 'products',
    baseAlias: 'base',
    joinedSources: [
      {
        id: 'join_1',
        sourceType: 'table',
        sourceSchema: 'ao_ref',
        sourceTable: 'brands',
        alias: 'brand_src',
        joinMode: 'left',
        joinKeys: [{ leftField: 'brand_id', rightField: 'brand_id' }],
        selectedFields: ['brand_name'],
        prefix: '',
        conflictMode: 'suffix'
      }
    ],
    inputSources: [
      {
        id: 'param_1',
        parameterName: 'status_info',
        bindMode: 'broadcast_fields',
        fieldMapping: [{ sourceField: 'status', targetField: 'status_from_param' }]
      }
    ],
    selectedFields: [
      { sourceAlias: 'base', fieldName: 'sku', outputName: 'sku' },
      { sourceAlias: 'base', fieldName: 'qty', outputName: 'qty' },
      { sourceAlias: 'brand_src', fieldName: 'brand_name', outputName: 'brand_name' },
      { sourceAlias: 'status_info', fieldName: 'status_from_param', outputName: 'status_from_param' }
    ],
    computedFields: [
      { name: 'score', expression: 'если({qty} > 1, {qty} * 10, 0)', type: 'integer' }
    ],
    filterRules: [{ field: 'qty', operator: '>', value: '0' }],
    filterLogic: 'and',
    aggregateRules: [],
    outputMode: 'named_output_params',
    outputParamsMapping: [
      { outputParamName: 'first_sku', sourceField: 'sku', mode: 'scalar' },
      { outputParamName: 'all_brand_names', sourceField: 'brand_name', mode: 'array' }
    ],
    previewInputParams: { status_info: { status: 'ok' } },
    previewLimit: 10,
    batchSize: 100,
    ...overrides
  };
}

test('table node runtime: preview builds result set from base table, join and input params', async () => {
  const client = createTableClient(FIXTURES);
  const preview = await previewTableNodeConfig(client, workingConfig());

  assert.equal(preview.row_count, 2);
  assert.deepEqual(preview.columns, ['sku', 'qty', 'brand_name', 'status_from_param', 'score']);
  assert.equal(preview.sample_rows[0].brand_name, 'Acme');
  assert.equal(preview.sample_rows[0].status_from_param, 'ok');
  assert.equal(preview.sample_rows[0].score, 20);
  assert.equal(preview.output_params.first_sku, 'wb-1');
  assert.deepEqual(preview.output_params.all_brand_names, ['Acme', 'Beta']);
  assert.equal(preview.join_summaries[0].matched_rows, 2);
});

test('table node runtime: preview rejects invalid join field with user-facing error', async () => {
  const client = createTableClient(FIXTURES);
  await assert.rejects(
    () =>
      previewTableNodeConfig(
        client,
        workingConfig({
          joinedSources: [
            {
              id: 'join_1',
              sourceType: 'table',
              sourceSchema: 'ao_ref',
              sourceTable: 'brands',
              alias: 'brand_src',
              joinMode: 'left',
              joinKeys: [{ leftField: 'brand_id', rightField: 'missing_field' }],
              selectedFields: ['brand_name'],
              prefix: '',
              conflictMode: 'suffix'
            }
          ]
        })
      ),
    /поле "missing_field" не найдено в подключаемом источнике/i
  );
});

test('table node runtime: preview rejects invalid computed expression with user-facing error', async () => {
  const client = createTableClient(FIXTURES);
  await assert.rejects(
    () =>
      previewTableNodeConfig(
        client,
        workingConfig({
          computedFields: [{ name: 'broken', expression: 'если({qty} > 0,', type: 'integer' }]
        })
      ),
    /Вычисляемое поле "broken"/i
  );
});

test('table node runtime: execute returns rows and output params', async () => {
  const client = createTableClient(FIXTURES);
  const exec = await executeTableNodeConfig(client, workingConfig());

  assert.equal(exec.rows.length, 2);
  assert.equal(exec.outputParams.first_sku, 'wb-1');
  assert.deepEqual(exec.outputParams.all_brand_names, ['Acme', 'Beta']);
  assert.equal(exec.meta.source_type, 'table_node');
  assert.equal(exec.meta.output_params.first_sku, 'wb-1');
});
