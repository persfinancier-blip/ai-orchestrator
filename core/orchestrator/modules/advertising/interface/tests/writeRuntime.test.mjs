import test from 'node:test';
import assert from 'node:assert/strict';

const { previewWriteConfig, executeWriteConfig } = await import('../server/writeRuntime.mjs');

function createWriteClient({ existingIds = new Set(), columns = ['id', 'title', 'status'] } = {}) {
  const calls = [];
  const normalizedExisting = new Set([...existingIds].map((value) => String(value)));
  const client = {
    calls,
    async query(sql, params = []) {
      const text = String(sql || '');
      calls.push({ sql: text, params });
      if (text.includes('information_schema.tables')) return { rows: [{ '?column?': 1 }], rowCount: 1 };
      if (text.includes('information_schema.columns')) {
        return {
          rows: columns.map((name) => ({ column_name: name, data_type: name === 'id' ? 'integer' : 'text' })),
          rowCount: columns.length
        };
      }
      if (text.startsWith('SELECT 1 FROM "ao_data"."silver_products" WHERE')) {
        const key = String(params?.[0] ?? '');
        return normalizedExisting.has(key) ? { rows: [{ '?column?': 1 }], rowCount: 1 } : { rows: [], rowCount: 0 };
      }
      if (text.startsWith('INSERT INTO "ao_data"."silver_products"')) {
        const valuesCount = Array.isArray(params) ? params.length : 0;
        const colsCountMatch = text.match(/\(([^)]+)\)\s+VALUES/i);
        const colsCount = colsCountMatch ? colsCountMatch[1].split(',').length : 1;
        const rowCount = colsCount > 0 ? Math.trunc(valuesCount / colsCount) : 0;
        return { rows: [], rowCount };
      }
      if (text.startsWith('UPDATE "ao_data"."silver_products" SET')) {
        return { rows: [], rowCount: 1 };
      }
      return { rows: [], rowCount: 0 };
    }
  };
  return client;
}

test('write runtime: preview auto-maps fields and reports candidates', async () => {
  const client = createWriteClient({ existingIds: new Set(['2']) });
  const preview = await previewWriteConfig(
    client,
    {
      sourceMode: 'node',
      sourceNodeTemplateName: 'Парсер данных / Карточки',
      targetSchema: 'ao_data',
      targetTable: 'silver_products',
      writeMode: 'upsert',
      keyFields: 'id'
    },
    {
      inputValue: {
        contract_version: 'node_io_v1',
        rows: [
          { id: 1, title: 'A', status: 'new', ignored_field: 'x' },
          { id: 2, title: 'B', status: 'active', ignored_field: 'y' }
        ]
      }
    }
  );

  assert.equal(preview.source_row_count, 2);
  assert.equal(preview.mapping_summary.rows_ready, 2);
  assert.equal(preview.mapping_summary.mapped_fields_count, 3);
  assert.deepEqual(preview.mapping_summary.key_fields, ['id']);
  assert.equal(preview.mapping_summary.matched_existing_rows, 1);
  assert.equal(preview.mapping_summary.insert_candidates, 1);
  assert.equal(preview.mapping_summary.update_candidates, 1);
  assert.ok(preview.mapping_rows.some((row) => row.sourceField === 'id' && row.targetField === 'id' && row.status === 'mapped'));
});

test('write runtime: insert writes mapped rows into target table', async () => {
  const client = createWriteClient();
  const result = await executeWriteConfig(
    client,
    {
      sourceMode: 'node',
      targetSchema: 'ao_data',
      targetTable: 'silver_products',
      writeMode: 'insert'
    },
    {
      inputValue: {
        contract_version: 'node_io_v1',
        rows: [
          { id: 1, title: 'A', status: 'new' },
          { id: 2, title: 'B', status: 'active' }
        ]
      }
    }
  );

  assert.equal(result.stats.target_type, 'table');
  assert.equal(result.stats.wrote, 2);
  assert.equal(result.stats.inserted, 2);
  assert.equal(result.stats.updated, 0);
  assert.equal(result.meta.target, 'ao_data.silver_products');
  assert.equal(result.rows.length, 2);
});

test('write runtime: update_by_key updates only rows with complete keys', async () => {
  const client = createWriteClient({ existingIds: new Set(['1', '2']) });
  const result = await executeWriteConfig(
    client,
    {
      sourceMode: 'node',
      targetSchema: 'ao_data',
      targetTable: 'silver_products',
      writeMode: 'update_by_key',
      keyFields: 'id'
    },
    {
      inputValue: {
        contract_version: 'node_io_v1',
        rows: [
          { id: 1, title: 'A1', status: 'new' },
          { id: 2, title: 'B1', status: 'active' },
          { title: 'No key', status: 'draft' }
        ]
      }
    }
  );

  assert.equal(result.stats.target_type, 'table');
  assert.equal(result.stats.updated, 2);
  assert.equal(result.stats.wrote, 2);
  assert.equal(result.stats.inserted, 0);
});

test('write runtime: upsert splits inserted and updated rows by existing key matches', async () => {
  const client = createWriteClient({ existingIds: new Set(['2']) });
  const result = await executeWriteConfig(
    client,
    {
      sourceMode: 'node',
      targetSchema: 'ao_data',
      targetTable: 'silver_products',
      writeMode: 'upsert',
      keyFields: 'id'
    },
    {
      inputValue: {
        contract_version: 'node_io_v1',
        rows: [
          { id: 1, title: 'A', status: 'new' },
          { id: 2, title: 'B', status: 'active' }
        ]
      }
    }
  );

  assert.equal(result.stats.target_type, 'table');
  assert.equal(result.stats.wrote, 2);
  assert.equal(result.stats.inserted, 1);
  assert.equal(result.stats.updated, 1);
  assert.deepEqual(result.meta.key_fields, ['id']);
});
