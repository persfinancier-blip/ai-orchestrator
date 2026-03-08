import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import fs from 'node:fs/promises';
import { execFileSync } from 'node:child_process';

const { executeParserRows, parserPreviewSummary } = await import('../server/parserRuntime.mjs');

async function createZipArchive(zipPath, entryName, contents, tempCsvPath) {
  const pythonScript = [
    'import sys, zipfile',
    'z = zipfile.ZipFile(sys.argv[1], "w", compression=zipfile.ZIP_DEFLATED)',
    'z.writestr(sys.argv[2], sys.argv[3])',
    'z.close()'
  ].join('; ');
  const attempts = [
    ['python3', ['-c', pythonScript, zipPath, entryName, contents]],
    ['python', ['-c', pythonScript, zipPath, entryName, contents]]
  ];
  for (const [cmd, args] of attempts) {
    try {
      execFileSync(cmd, args, { stdio: 'ignore' });
      return;
    } catch {
      // try next tool
    }
  }
  await fs.writeFile(tempCsvPath, contents, 'utf8');
  execFileSync(
    'powershell',
    [
      '-NoProfile',
      '-Command',
      `Compress-Archive -Path '${tempCsvPath.replace(/'/g, "''")}' -DestinationPath '${zipPath.replace(/'/g, "''")}' -Force`
    ],
    { stdio: 'ignore' }
  );
}

test('parser runtime: JSON input -> mapped rows -> preview summary', async () => {
  const result = await executeParserRows(
    { query: async () => ({ rows: [] }) },
    {
      sourceMode: 'node',
      sourceFormat: 'json',
      recordPath: 'items',
      selectFields: 'id,name',
      renameMap: '{"name":"title"}',
      defaultValues: '{"country":"RU"}',
      typeMap: '{"id":"integer"}',
      filterField: 'id',
      filterOperator: '>=',
      filterValue: '2',
      batchSize: '50'
    },
    {
      inputValue: {
        items: [
          { id: '1', name: 'a' },
          { id: '2', name: 'b' },
          { id: '3', name: 'c' }
        ]
      }
    }
  );

  assert.equal(result.rows.length, 2);
  assert.deepEqual(result.rows[0], { id: 2, title: 'b', country: 'RU' });
  assert.equal(result.batch.has_more, false);

  const preview = parserPreviewSummary(result);
  assert.equal(preview.row_count, 2);
  assert.deepEqual(preview.columns.sort(), ['country', 'id', 'title']);
  assert.equal(preview.raw_row_count, 3);
  assert.deepEqual(preview.raw_columns.sort(), ['id', 'name']);
});

test('parser runtime: CSV input supports batch cursor without loading fake chunks in UI', async () => {
  const csv = 'id,name\n1,a\n2,b\n3,c\n4,d\n';
  const settings = {
    sourceMode: 'input',
    sourceFormat: 'csv',
    batchSize: '2'
  };

  const first = await executeParserRows({ query: async () => ({ rows: [] }) }, settings, { inputValue: csv });
  assert.equal(first.rows.length, 2);
  assert.equal(first.batch.has_more, true);
  assert.deepEqual(first.batch.next_cursor, { offset: 2 });

  const second = await executeParserRows({ query: async () => ({ rows: [] }) }, settings, {
    inputValue: csv,
    cursor: first.batch.next_cursor
  });
  assert.equal(second.rows.length, 2);
  assert.equal(second.batch.has_more, false);
  assert.deepEqual(second.rows[0], { id: '3', name: 'c' });
});

test('parser runtime: table source parses payload column iteratively', async () => {
  const sourceRows = [
    { payload_json: JSON.stringify({ items: [{ id: 1, title: 'a' }, { id: 2, title: 'b' }] }) },
    { payload_json: JSON.stringify({ items: [{ id: 3, title: 'c' }] }) }
  ];
  const client = {
    query: async (_sql, params) => {
      const limit = Number(params?.[0] || 0);
      const offset = Number(params?.[1] || 0);
      return { rows: sourceRows.slice(offset, offset + limit) };
    }
  };

  const result = await executeParserRows(
    client,
    {
      sourceMode: 'table',
      sourceSchema: 'ao_data',
      sourceTable: 'bronze_raw',
      sourceColumn: 'payload_json',
      sourceFormat: 'json',
      recordPath: 'items',
      batchSize: '2'
    },
    {}
  );

  assert.equal(result.source_type, 'table');
  assert.equal(result.rows.length, 2);
  assert.equal(result.batch.has_more, true);
  assert.deepEqual(result.rows[0], { id: 1, title: 'a' });
});

test('parser runtime: lookup enriches rows from existing table', async () => {
  const lookupRows = [
    { _lookup_key: 'cab_1', token: 't1', shop_name: 'Shop 1' },
    { _lookup_key: 'cab_2', token: 't2', shop_name: 'Shop 2' }
  ];
  const client = {
    query: async (sql, params) => {
      if (String(sql).includes('FROM "ao_data"."client_tokens"')) {
        const wanted = new Set(params?.[0] || []);
        return { rows: lookupRows.filter((row) => wanted.has(row._lookup_key)) };
      }
      return { rows: [] };
    }
  };

  const result = await executeParserRows(
    client,
    {
      sourceMode: 'input',
      sourceFormat: 'json',
      recordPath: 'items',
      lookupEnabled: true,
      lookupSchema: 'ao_data',
      lookupTable: 'client_tokens',
      lookupSourceField: 'client_id',
      lookupTargetField: 'client_id',
      lookupFields: 'token,shop_name',
      lookupPrefix: 'lk_'
    },
    {
      inputValue: {
        items: [
          { client_id: 'cab_1', metric: 10 },
          { client_id: 'cab_2', metric: 20 }
        ]
      }
    }
  );

  assert.deepEqual(result.rows[0], {
    client_id: 'cab_1',
    metric: 10,
    lk_token: 't1',
    lk_shop_name: 'Shop 1'
  });
});

test('parser runtime: file_url reads CSV over HTTP without breaking contract', async () => {
  const server = http.createServer((_req, res) => {
    res.writeHead(200, { 'content-type': 'text/csv' });
    res.end('id,name\n10,alpha\n11,beta\n');
  });
  await new Promise((resolve) => server.listen(0, resolve));
  const address = server.address();
  const url = `http://127.0.0.1:${address.port}/sample.csv`;

  try {
    const result = await executeParserRows(
      { query: async () => ({ rows: [] }) },
      {
        sourceMode: 'file_url',
        fileUrl: url,
        sourceFormat: 'csv',
        batchSize: '10'
      },
      {}
    );
    assert.equal(result.rows.length, 2);
    assert.equal(result.source_type, 'file_url');
    assert.deepEqual(result.rows[1], { id: '11', name: 'beta' });
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test('parser runtime: ZIP -> CSV parses selected entry without loading whole archive into UI logic', async () => {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'parser-zip-'));
  const csvPath = path.join(tempDir, 'rows.csv');
  const zipPath = path.join(tempDir, 'rows.zip');
  await createZipArchive(zipPath, 'rows.csv', 'id,name\n21,gamma\n22,delta\n', csvPath);

  const zipBuffer = await fs.readFile(zipPath);
  const server = http.createServer((_req, res) => {
    res.writeHead(200, { 'content-type': 'application/zip' });
    res.end(zipBuffer);
  });
  await new Promise((resolve) => server.listen(0, resolve));
  const address = server.address();
  const url = `http://127.0.0.1:${address.port}/rows.zip`;

  try {
    const result = await executeParserRows(
      { query: async () => ({ rows: [] }) },
      {
        sourceMode: 'file_url',
        fileUrl: url,
        sourceFormat: 'zip',
        archiveEntry: 'rows.csv',
        archiveFormat: 'csv',
        batchSize: '10'
      },
      {}
    );
    assert.equal(result.rows.length, 2);
    assert.deepEqual(result.rows[0], { id: '21', name: 'gamma' });
  } finally {
    await new Promise((resolve) => server.close(resolve));
    await fs.rm(tempDir, { recursive: true, force: true });
  }
});
