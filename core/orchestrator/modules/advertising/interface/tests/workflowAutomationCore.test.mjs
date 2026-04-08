import test from 'node:test';
import assert from 'node:assert/strict';
process.env.PGUSER ||= 'test_user';
process.env.PGHOST ||= '127.0.0.1';
process.env.PGPASSWORD ||= 'test_password';
process.env.PGDATABASE ||= 'test_db';
process.env.PGPORT ||= '5432';
const { workflowAutomationTestkit } = await import('../server/workflowAutomation.mjs');

const {
  parseIntervalToMs,
  cronMatchesNow,
  normalizeExecutionScopeMode,
  buildExecutionScope,
  dedupeScopes,
  buildScopeSqlFilter,
  processConfigFromStartNode,
  discoverProcessesFromGraph,
  resolveExecutionScopesFromValues,
  dependencyShouldDispatch,
  buildDependencyDispatchDedupeKey,
  buildRunAggregationSnapshot,
  buildRunSummaryFromAggregation,
  buildProcessStepObservabilityRow,
  normalizeNodeIoEnvelope,
  composeNodeOutputEnvelope,
  executeProcessPreviewUntilNode,
  applyToolSettingsOverrideToGraph,
  executeTableParserNode,
  executeActionPrepNode,
  executeTableNode,
  executeApiMutationNode,
  executeDbWriteNode,
  _testEnsureWorkflowAutomationTables,
  _testWriteChunkLog,
  _testReserveRunSlotForProcess,
  _testReleaseRunSlotForProcess,
  _testClearActiveRunSlots
} = workflowAutomationTestkit;

test('scheduler contract: interval and cron parsing', () => {
  assert.equal(parseIntervalToMs(2, 'minutes'), 120000);
  assert.equal(parseIntervalToMs(3, 'hours'), 10800000);
  const now = new Date(2026, 2, 6, 10, 15, 0, 0);
  assert.equal(cronMatchesNow('15 10 * * *', now), true);
  assert.equal(cronMatchesNow('0 10 * * *', now), false);
});

test('scope contract: normalize + sql filter parameter continuity', () => {
  const scope = buildExecutionScope({ scope_type: 'tenant', scope_ref: 't-1', tenant_id: 'tenant-1' }, {});
  assert.equal(scope.scope_type, 'tenant');
  assert.equal(scope.scope_ref, 't-1');
  assert.equal(scope.tenant_id, 'tenant-1');

  const f1 = buildScopeSqlFilter({ scope_type: 'global', scope_ref: 'global' }, 3);
  assert.match(f1.sql, /\$3/);
  assert.match(f1.sql, /\$4/);
  assert.equal(f1.params.length, 2);

  const f2 = buildScopeSqlFilter({ scope_type: 'tenant', scope_ref: 'x', tenant_id: 't1' }, 3);
  assert.match(f2.sql, /\$3/);
  assert.match(f2.sql, /\$4/);
  assert.match(f2.sql, /\$5/);
  assert.equal(f2.params.length, 3);
});

test('process discovery: one desk, many start nodes, independent subgraphs', () => {
  const graph = {
    nodes: [
      { id: 'start_1', type: 'tool', config: { name: 'Start 1', toolType: 'start_process', settings: { executionScopeMode: 'single_global' } } },
      { id: 'start_2', type: 'tool', config: { name: 'Start 2', toolType: 'start_process', settings: { executionScopeMode: 'for_each_tenant' } } },
      { id: 'parser_1', type: 'tool', config: { name: 'Parser 1', toolType: 'table_parser', settings: {} } },
      { id: 'writer_2', type: 'tool', config: { name: 'Writer 2', toolType: 'db_write', settings: {} } },
      { id: 'end_1', type: 'tool', config: { name: 'End', toolType: 'end_process', settings: {} } }
    ],
    edges: [
      { from: 'start_1', to: 'parser_1' },
      { from: 'parser_1', to: 'end_1' },
      { from: 'start_2', to: 'writer_2' },
      { from: 'writer_2', to: 'end_1' }
    ]
  };

  const processes = discoverProcessesFromGraph({
    deskId: 15,
    deskVersionId: 5,
    versionNo: 7,
    graphJson: graph,
    overrideRows: []
  });

  assert.equal(processes.length, 2);
  const p1 = processes.find((p) => p.start_node_id === 'start_1');
  const p2 = processes.find((p) => p.start_node_id === 'start_2');
  assert.ok(p1);
  assert.ok(p2);
  assert.equal(p1.execution_scope_mode, 'single_global');
  assert.equal(p2.execution_scope_mode, 'for_each_tenant');
  assert.equal(Array.isArray(p1.subgraph.order), true);
  assert.equal(Array.isArray(p2.subgraph.order), true);
});

test('scope source resolution: values mode supports for_each_source_account and dedupe', () => {
  const process = {
    execution_scope_mode: 'for_each_source_account',
    scope_type: 'source_account',
    scope_ref: 'default'
  };
  const scopes = resolveExecutionScopesFromValues(
    {
      values: [
        { scope_ref: 'cab_1', tenant_id: 'tenant_a', context_json: { region: 'RU' } },
        { scope_ref: 'cab_1', tenant_id: 'tenant_a', context_json: { region: 'RU' } },
        { scope_ref: 'cab_2', tenant_id: 'tenant_b', context_json: { region: 'EU' } }
      ]
    },
    process
  );
  const deduped = dedupeScopes(scopes);
  assert.equal(scopes.length, 3);
  assert.equal(deduped.length, 2);
  assert.equal(deduped[0].scope_type, 'source_account');
});

test('dependency dispatch modes and dedupe keys', () => {
  const ruleChunks = { id: 1, dispatch_mode: 'for_each_chunk', dedupe_policy: 'deduplicate_by_payload', target_start_node_id: 'start_b' };
  const evChunk = {
    event_type: 'chunk_completed',
    run_uid: 'run_1',
    source_run_uid: 'run_1',
    chunk_key: 'chunk_1',
    scope_type: 'tenant',
    scope_ref: 'cab_1',
    tenant_id: 'tenant_1',
    payload: { id: 10 }
  };
  assert.equal(dependencyShouldDispatch(ruleChunks, evChunk), true);
  const key1 = buildDependencyDispatchDedupeKey(ruleChunks, evChunk);
  const key2 = buildDependencyDispatchDedupeKey(ruleChunks, { ...evChunk, payload: { id: 11 } });
  assert.notEqual(key1, key2);

  const ruleAfterAll = { dispatch_mode: 'once_after_all_chunks', trigger_status: 'completed' };
  assert.equal(
    dependencyShouldDispatch(ruleAfterAll, {
      event_type: 'process_completed',
      status: 'completed',
      all_chunks_done: true
    }),
    true
  );
  assert.equal(
    dependencyShouldDispatch(ruleAfterAll, {
      event_type: 'process_completed',
      status: 'completed',
      all_chunks_done: false
    }),
    false
  );
});

test('single-instance run slot contract blocks parallel run for same process key', () => {
  _testClearActiveRunSlots();
  const ok1 = _testReserveRunSlotForProcess('desk:1:start:A', 'run_1', { run_policy: 'single_instance', desk_id: 1, start_node_id: 'A' });
  const ok2 = _testReserveRunSlotForProcess('desk:1:start:A', 'run_2', { run_policy: 'single_instance', desk_id: 1, start_node_id: 'A' });
  assert.equal(ok1, true);
  assert.equal(ok2, false);

  _testReleaseRunSlotForProcess('run_1');
  const ok3 = _testReserveRunSlotForProcess('desk:1:start:A', 'run_3', { run_policy: 'single_instance', desk_id: 1, start_node_id: 'A' });
  assert.equal(ok3, true);

  const okParallel = _testReserveRunSlotForProcess('desk:1:start:A', 'run_4', { run_policy: 'allow_parallel', desk_id: 1, start_node_id: 'A' });
  assert.equal(okParallel, true);
  _testClearActiveRunSlots();
});

test('node io contract for first nodes: normalize input + envelope output', () => {
  const normalized = normalizeNodeIoEnvelope({
    responses: [{ response: { id: 1 } }, { response: [{ id: 2 }, { id: 3 }] }]
  });
  assert.equal(normalized.contract_version, 'node_io_v1');
  assert.equal(normalized.row_count, 3);

  const output = composeNodeOutputEnvelope(
    { run_uid: 'r_1', process_code: 'p_1' },
    { id: 'node_parser', type: 'tool', config: { name: 'Parser', toolType: 'table_parser' } },
    normalized.rows,
    { source_type: 'input' },
    { parsed_rows: 3 }
  );
  assert.equal(output.contract_version, 'node_io_v1');
  assert.equal(output.row_count, 3);
  assert.equal(output.meta.node_type, 'table_parser');
  assert.equal(output.meta.source_type, 'input');
});

test('step observability contract: canonical handoff is separate from request/response debug', () => {
  const apiOutput = composeNodeOutputEnvelope(
    { run_uid: 'run_obs', process_code: 'proc_obs' },
    { id: 'api_1', type: 'tool', config: { name: 'API', toolType: 'api_request' } },
    [{ id: 10, name: 'Alpha' }],
    { source_type: 'api_request', request_count: 1 },
    { response_preview: { responses: [{ response: [{ id: 10, name: 'Alpha' }] }] } }
  );
  const parserOutput = composeNodeOutputEnvelope(
    { run_uid: 'run_obs', process_code: 'proc_obs' },
    { id: 'parser_1', type: 'tool', config: { name: 'Parser', toolType: 'table_parser' } },
    [{ product_id: 10, title: 'Alpha' }],
    { source_type: 'table_parser' },
    { parsed_rows: 1 }
  );
  const parserRequestPayload = {
    mode: 'table_parser',
    input_contract: normalizeNodeIoEnvelope(apiOutput, { run_uid: 'run_obs', process_code: 'proc_obs' })
  };
  const stepRow = buildProcessStepObservabilityRow({
    run_uid: 'run_obs',
    step_order: 2,
    node: { id: 'parser_1', type: 'tool', config: { name: 'Parser', toolType: 'table_parser' } },
    status: 'ok',
    input_value: apiOutput,
    output_value: parserOutput,
    request_payload: parserRequestPayload,
    response_payload: parserOutput,
    metrics_json: { payloadCount: 1 }
  });

  assert.deepEqual(stepRow.input_json, apiOutput);
  assert.deepEqual(stepRow.output_json, parserOutput);
  assert.deepEqual(stepRow.request_payload, parserRequestPayload);
  assert.deepEqual(stepRow.response_payload, parserOutput);
  assert.deepEqual(stepRow.request_payload.input_contract, apiOutput);
  assert.notDeepEqual(stepRow.input_json, stepRow.request_payload);
});

test('preview parser run: api published output becomes canonical parser input instead of debug wrapper', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => ({
    ok: true,
    status: 200,
    async text() {
      return JSON.stringify({
        list: [
          {
            id: '24105106',
            title: '982379323 25.03.2026',
            state: 'CAMPAIGN_STATE_RUNNING',
            advObjectType: 'SKU',
            fromDate: '2026-03-25'
          }
        ]
      });
    }
  });

  const client = {
    query: async (sql) => {
      const text = String(sql || '').replace(/\s+/g, ' ').trim();
      if (text.includes('"ao_system"."api_configs_store"')) {
        return {
          rows: [
            {
              id: 1,
              is_active: true,
              api_name: 'API Preview',
              method: 'GET',
              base_url: 'https://example.test',
              path: '/campaigns',
              headers_json: {},
              query_json: {},
              body_json: {},
              auth_mode: 'manual',
              pagination_json: {
                enabled: false,
                data_path: 'list'
              },
              output_parameters: [
                { root_path: 'list', path: 'id', alias: 'id' },
                { root_path: 'list', path: 'title', alias: 'title' },
                { root_path: 'list', path: 'state', alias: 'state' },
                { root_path: 'list', path: 'advObjectType', alias: 'adv_object_type' },
                { root_path: 'list', path: 'fromDate', alias: 'from_date' }
              ]
            }
          ]
        };
      }
      if (text.includes('"ao_system"."workflow_run_steps_store"')) {
        return { rows: [], rowCount: 1 };
      }
      return { rows: [], rowCount: 0 };
    }
  };

  const process = {
    start_node_id: 'start_1',
    process_code: 'preview_api_parser',
    execution_scope_mode: 'single_global',
    subgraph: {
      order: [
        {
          id: 'start_1',
          type: 'tool',
          config: { name: 'Start', toolType: 'start_process', settings: {} }
        },
        {
          id: 'api_1',
          type: 'tool',
          config: {
            name: 'API',
            toolType: 'api_request',
            settings: {
              templateStoreId: '1'
            }
          }
        },
        {
          id: 'parser_1',
          type: 'tool',
          config: {
            name: 'Parser',
            toolType: 'table_parser',
            settings: {
              sourceMode: 'input',
              sourceFormat: 'json',
              selectFields: 'list.id, list.title, list.state, list.advObjectType, list.fromDate',
              renameMap:
                '{"list.id":"adv_id","list.title":"adv_title","list.state":"state","list.advObjectType":"adv_object_type","list.fromDate":"from_date"}'
            }
          }
        }
      ]
    }
  };

  try {
    const preview = await executeProcessPreviewUntilNode(
      client,
      {
        api_configs_schema: 'ao_system',
        api_configs_table: 'api_configs_store',
        workflow_runs_schema: 'ao_system',
        workflow_run_steps_table: 'workflow_run_steps_store'
      },
      {
        desk_id: 24,
        desk_version_id: 1
      },
      process,
      {
        run_uid: 'wf_preview_test_api_parser'
      },
      'parser_1'
    );

    const apiStep = preview.steps.find((step) => step.node_id === 'api_1');
    const parserStep = preview.steps.find((step) => step.node_id === 'parser_1');

    assert.ok(apiStep);
    assert.ok(parserStep);
    assert.deepEqual(apiStep.output_json, parserStep.input_json);
    assert.equal(apiStep.output_json.row_count, 1);
    assert.deepEqual(apiStep.output_json.rows[0], {
      id: '24105106',
      title: '982379323 25.03.2026',
      state: 'CAMPAIGN_STATE_RUNNING',
      adv_object_type: 'SKU',
      from_date: '2026-03-25'
    });
    assert.deepEqual(parserStep.input_json.rows[0], apiStep.output_json.rows[0]);
    assert.deepEqual(parserStep.output_json.rows[0], {
      adv_id: '24105106',
      adv_title: '982379323 25.03.2026',
      state: 'CAMPAIGN_STATE_RUNNING',
      adv_object_type: 'SKU',
      from_date: '2026-03-25'
    });
    assert.equal(Object.prototype.hasOwnProperty.call(parserStep.input_json.rows[0], 'response'), false);
    assert.equal(Object.prototype.hasOwnProperty.call(parserStep.input_json.rows[0], 'entity_index'), false);
    assert.equal(Array.isArray(apiStep.response_payload.responses), true);
    assert.equal(apiStep.response_payload.responses[0].status, 200);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('preview write run: api published output becomes canonical write input and preview stays non-mutating', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => ({
    ok: true,
    status: 200,
    async text() {
      return JSON.stringify({
        list: [
          {
            id: '24105106',
            title: '982379323 25.03.2026',
            state: 'CAMPAIGN_STATE_RUNNING',
            advObjectType: 'SKU',
            fromDate: '2026-03-25'
          }
        ]
      });
    }
  });

  const calls = [];
  const client = {
    query: async (sql, params = []) => {
      const text = String(sql || '').replace(/\s+/g, ' ').trim();
      calls.push(text);
      if (text.includes('"ao_system"."api_configs_store"')) {
        return {
          rows: [
            {
              id: 1,
              is_active: true,
              api_name: 'API Preview',
              method: 'GET',
              base_url: 'https://example.test',
              path: '/campaigns',
              headers_json: {},
              query_json: {},
              body_json: {},
              auth_mode: 'manual',
              pagination_json: {
                enabled: false,
                data_path: 'list'
              },
              output_parameters: [
                { root_path: 'list', path: 'id', alias: 'id' },
                { root_path: 'list', path: 'title', alias: 'title' },
                { root_path: 'list', path: 'state', alias: 'state' },
                { root_path: 'list', path: 'advObjectType', alias: 'adv_object_type' },
                { root_path: 'list', path: 'fromDate', alias: 'from_date' }
              ]
            }
          ]
        };
      }
      if (text.includes('"ao_system"."workflow_run_steps_store"')) {
        return { rows: [], rowCount: 1 };
      }
      if (/information_schema\.columns/i.test(text)) {
        return {
          rows: [
            { column_name: 'adv_id', data_type: 'text' },
            { column_name: 'adv_title', data_type: 'text' },
            { column_name: 'state', data_type: 'text' },
            { column_name: 'adv_object_type', data_type: 'text' },
            { column_name: 'from_date', data_type: 'text' }
          ],
          rowCount: 5
        };
      }
      if (text.startsWith('SELECT 1 FROM "ao_data"."silver_ads" WHERE')) {
        return { rows: [], rowCount: 0 };
      }
      if (text.startsWith('INSERT INTO "ao_data"."silver_ads"')) {
        throw new Error('preview_write_run_must_not_insert');
      }
      return { rows: [], rowCount: 0 };
    }
  };

  const process = {
    start_node_id: 'start_1',
    process_code: 'preview_api_write',
    execution_scope_mode: 'single_global',
    subgraph: {
      order: [
        {
          id: 'start_1',
          type: 'tool',
          config: { name: 'Start', toolType: 'start_process', settings: {} }
        },
        {
          id: 'api_1',
          type: 'tool',
          config: {
            name: 'API',
            toolType: 'api_request',
            settings: {
              templateStoreId: '1'
            }
          }
        },
        {
          id: 'writer_1',
          type: 'tool',
          config: {
            name: 'Writer',
            toolType: 'db_write',
            settings: {
              sourceMode: 'node',
              targetSchema: 'ao_data',
              targetTable: 'silver_ads',
              writeMode: 'upsert',
              keyFields: 'adv_id',
              fieldMappingsJson: JSON.stringify([
                { sourceField: 'id', targetField: 'adv_id' },
                { sourceField: 'title', targetField: 'adv_title' },
                { sourceField: 'state', targetField: 'state' },
                { sourceField: 'adv_object_type', targetField: 'adv_object_type' },
                { sourceField: 'from_date', targetField: 'from_date' }
              ])
            }
          }
        }
      ]
    }
  };

  try {
    const preview = await executeProcessPreviewUntilNode(
      client,
      {
        api_configs_schema: 'ao_system',
        api_configs_table: 'api_configs_store',
        workflow_runs_schema: 'ao_system',
        workflow_run_steps_table: 'workflow_run_steps_store'
      },
      {
        desk_id: 24,
        desk_version_id: 1
      },
      process,
      {
        run_uid: 'wf_preview_test_api_write'
      },
      'writer_1'
    );

    const apiStep = preview.steps.find((step) => step.node_id === 'api_1');
    const writeStep = preview.steps.find((step) => step.node_id === 'writer_1');

    assert.ok(apiStep);
    assert.ok(writeStep);
    assert.deepEqual(apiStep.output_json, writeStep.input_json);
    assert.deepEqual(apiStep.output_json.rows[0], {
      id: '24105106',
      title: '982379323 25.03.2026',
      state: 'CAMPAIGN_STATE_RUNNING',
      adv_object_type: 'SKU',
      from_date: '2026-03-25'
    });
    assert.deepEqual(writeStep.input_json.rows[0], apiStep.output_json.rows[0]);
    assert.deepEqual(writeStep.output_json.rows[0], {
      adv_id: '24105106',
      adv_title: '982379323 25.03.2026',
      state: 'CAMPAIGN_STATE_RUNNING',
      adv_object_type: 'SKU',
      from_date: '2026-03-25'
    });
    assert.equal(Object.prototype.hasOwnProperty.call(writeStep.input_json.rows[0], 'response'), false);
    assert.equal(Object.prototype.hasOwnProperty.call(writeStep.input_json.rows[0], 'entity_index'), false);
    assert.equal(calls.some((item) => item.startsWith('INSERT INTO "ao_data"."silver_ads"')), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('preview graph override: parser settings patch only changes target parser node in graph snapshot', () => {
  const graph = {
    nodes: [
      {
        id: 'parser_1',
        type: 'tool',
        config: {
          name: 'Parser',
          toolType: 'table_parser',
          settings: { selectFields: 'id,title', previewLimit: '20' }
        }
      },
      {
        id: 'writer_1',
        type: 'tool',
        config: {
          name: 'Writer',
          toolType: 'db_write',
          settings: { targetTable: 'ao_data.target' }
        }
      }
    ],
    edges: [{ from: 'parser_1', to: 'writer_1' }]
  };

  const patched = applyToolSettingsOverrideToGraph(graph, 'parser_1', { selectFields: 'id,state', defaultValues: '{"state":"draft"}' }, 'table_parser');

  assert.equal(patched.nodes[0].config.settings.selectFields, 'id,state');
  assert.equal(patched.nodes[0].config.settings.defaultValues, '{"state":"draft"}');
  assert.equal(patched.nodes[1].config.settings.targetTable, 'ao_data.target');
  assert.equal(graph.nodes[0].config.settings.selectFields, 'id,title');
});

test('first nodes runtime contract: table_parser -> db_write (process_bus fallback)', async () => {
  const parserNode = {
    id: 'parser_1',
    type: 'tool',
    config: {
      name: 'Parser',
      toolType: 'table_parser',
      settings: {
        sourceMode: 'input',
        selectFields: 'id,name',
        renameMap: '{"name":"title"}',
        filterField: 'id',
        filterOperator: '>=',
        filterValue: '2'
      }
    }
  };
  const parserOut = await executeTableParserNode(
    { query: async () => ({ rows: [] }) },
    { workflow_runs_schema: 'ao_system', workflow_process_bus_table: 'workflow_process_bus_store' },
    { desk_id: 10, run_uid: 'run_parser', process_code: 'proc_parser' },
    parserNode,
    [{ id: 1, name: 'a' }, { id: 2, name: 'b' }, { id: 3, name: 'c' }]
  );
  assert.equal(parserOut.output.contract_version, 'node_io_v1');
  assert.equal(parserOut.output.row_count, 2);
  assert.deepEqual(parserOut.output.rows[0], { id: 2, title: 'b' });

  const calls = [];
  const dbClient = {
    query: async (sql, params) => {
      calls.push({ sql: String(sql || ''), params });
      if (String(sql || '').includes('information_schema.tables')) return { rows: [] };
      if (String(sql || '').includes('INSERT INTO') && String(sql || '').includes('workflow_process_bus_store')) return { rows: [{ id: 77 }], rowCount: 1 };
      return { rows: [], rowCount: 0 };
    }
  };
  const writerNode = {
    id: 'writer_1',
    type: 'tool',
    config: {
      name: 'Writer',
      toolType: 'db_write',
      settings: {
        targetSchema: 'ao_data',
        targetTable: 'bronze_target',
        writeMode: 'insert',
        channel: 'writer_channel'
      }
    }
  };
  const writeOut = await executeDbWriteNode(
    dbClient,
    { workflow_runs_schema: 'ao_system', workflow_process_bus_table: 'workflow_process_bus_store' },
    { desk_id: 10, run_uid: 'run_parser', process_code: 'proc_parser' },
    writerNode,
    parserOut.output
  );
  assert.equal(writeOut.metrics.target_type, 'process_bus');
  assert.equal(writeOut.output.wrote, 2);
  assert.equal(writeOut.output.channel, 'writer_channel');
  assert.equal(calls.length > 0, true);
});

test('first nodes runtime contract: table_node output params propagate to downstream table_node', async () => {
  const fixtures = {
    'ao_data.products': {
      columns: [
        { name: 'id', type: 'integer' },
        { name: 'sku', type: 'text' }
      ],
      rows: [
        { id: 1, sku: 'wb-1' },
        { id: 2, sku: 'wb-2' }
      ]
    }
  };
  const client = {
    query: async (sql, params = []) => {
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
      const meta = fixtures[`${schema}.${table}`];
      return { rows: Array.isArray(meta?.rows) ? meta.rows.map((row) => ({ ...row })) : [] };
    }
  };

  const processCtx = { desk_id: 11, run_uid: 'run_table', process_code: 'proc_table' };
  const firstNode = {
    id: 'table_1',
    type: 'tool',
    config: {
      name: 'Table 1',
      toolType: 'table_node',
      settings: {
        baseSchema: 'ao_data',
        baseTable: 'products',
        baseAlias: 'base',
        selectedFieldsJson: JSON.stringify([{ sourceAlias: 'base', fieldName: 'sku', outputName: 'sku' }]),
        outputMode: 'named_output_params',
        outputParamsMappingJson: JSON.stringify([{ outputParamName: 'first_sku', sourceField: 'sku', mode: 'scalar' }])
      }
    }
  };
  const firstOut = await executeTableNode(client, {}, processCtx, firstNode, []);
  assert.equal(firstOut.output.contract_version, 'node_io_v1');
  assert.equal(firstOut.output.meta.output_params.first_sku, 'wb-1');

  const secondNode = {
    id: 'table_2',
    type: 'tool',
    config: {
      name: 'Table 2',
      toolType: 'table_node',
      settings: {
        baseSchema: 'ao_data',
        baseTable: 'products',
        baseAlias: 'base',
        inputSourcesJson: JSON.stringify([
          {
            id: 'param_1',
            parameterName: 'first_sku',
            bindMode: 'broadcast_fields',
            fieldMapping: [{ sourceField: 'value', targetField: 'picked_sku' }]
          }
        ]),
        selectedFieldsJson: JSON.stringify([
          { sourceAlias: 'base', fieldName: 'sku', outputName: 'sku' },
          { sourceAlias: 'first_sku', fieldName: 'picked_sku', outputName: 'picked_sku' }
        ])
      }
    }
  };
  const secondOut = await executeTableNode(client, {}, processCtx, secondNode, firstOut.output);
  assert.equal(secondOut.output.contract_version, 'node_io_v1');
  assert.equal(secondOut.output.rows[0].picked_sku, 'wb-1');
  assert.equal(secondOut.output.trace.input_rows, 2);
});

test('action prep handoff: canonical output feeds api mutation dry-run preview', async () => {
  const originalFetch = globalThis.fetch;
  const requests = [];
  globalThis.fetch = async (url, init = {}) => {
    requests.push({ url: String(url || ''), init });
    throw new Error('dry_run_should_not_call_network');
  };
  try {
    const client = {
      query: async () => ({
        rows: [
          { campaign_id: 'cmp_1', current_bid: 100, roas: 0.8 },
          { campaign_id: 'cmp_2', current_bid: 120, roas: 1.4 }
        ]
      })
    };
    const processCtx = { desk_id: 91, run_uid: 'run_actions', process_code: 'proc_actions' };
    const prepNode = {
      id: 'action_prep_1',
      type: 'tool',
      config: {
        name: 'Подготовка действий',
        toolType: 'action_prep',
        settings: {
          sourceMode: 'table',
          sourceSchema: 'ao_data',
          sourceTable: 'campaigns',
          filterRulesJson: JSON.stringify([{ field: 'roas', operator: '<', value: 1 }]),
          actionColumnsJson: JSON.stringify([
            { name: 'action_type', mode: 'constant', constantValue: 'update_bid' },
            { name: 'new_bid', mode: 'percent_change', baseField: 'current_bid', percentValue: 5 },
            { name: 'reason', mode: 'string_template', template: 'rebalance_{campaign_id}' }
          ])
        }
      }
    };
    const prepResult = await executeActionPrepNode(client, {}, processCtx, prepNode, null, {});
    assert.equal(prepResult.output.contract_version, 'node_io_v1');
    assert.equal(prepResult.output.row_count, 1);
    assert.equal(prepResult.output.rows[0].action_type, 'update_bid');
    assert.equal(prepResult.output.rows[0].new_bid, 105);

    const mutationNode = {
      id: 'api_mutation_1',
      type: 'tool',
      config: {
        name: 'API-изменение',
        toolType: 'api_mutation',
        settings: {
          endpointUrl: 'https://mutation.example.test/bids',
          httpMethod: 'POST',
          headersJson: '{"Content-Type":"application/json"}',
          bodyJson: '{}',
          bindingRulesJson: JSON.stringify([
            { sourceField: 'campaign_id', target: 'body', path: 'campaign_id' },
            { sourceField: 'new_bid', target: 'body', path: 'bid' },
            { sourceField: 'reason', target: 'body', path: 'reason' }
          ]),
          responseMappingsJson: JSON.stringify([{ responsePath: 'dry_run', alias: 'mutation_preview_dry_run' }]),
          requestMode: 'row_per_request',
          dryRun: 'true'
        }
      }
    };
    const mutationResult = await executeApiMutationNode(client, {}, processCtx, mutationNode, prepResult.output, { preview: true, dry_run_override: true });
    assert.equal(mutationResult.output.contract_version, 'node_io_v1');
    assert.equal(mutationResult.output.row_count, 1);
    assert.equal(mutationResult.output.rows[0].campaign_id, 'cmp_1');
    assert.equal(mutationResult.output.rows[0].mutation_dry_run, true);
    assert.equal(mutationResult.request_payload.request_preview[0].body.bid, 105);
    assert.equal(requests.length, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('start settings sync contract: execution scope mode values stay from UI options', () => {
  const startNode = {
    id: 'start_scope',
    config: {
      name: 'Start scope',
      settings: {
        executionScopeMode: 'for_each_partition',
        scopeType: 'partition',
        scopeRef: 'part_01',
        tenantId: '',
        contextJson: '{"source":"manual"}',
        scopeSource: '{"kind":"values","values":["part_01","part_02"]}',
        runPolicy: 'single_instance',
        triggerType: 'interval',
        intervalValue: '5',
        intervalUnit: 'minutes'
      }
    }
  };
  const processCfg = processConfigFromStartNode(startNode, 101, null);
  assert.equal(normalizeExecutionScopeMode(processCfg.execution_scope_mode), 'for_each_partition');
  assert.equal(processCfg.scope_type, 'partition');
  assert.equal(processCfg.scope_ref, 'part_01');
  assert.equal(processCfg.run_policy, 'single_instance');
});

test('aggregation summary: completed run stays consistent with progress and final status', () => {
  const agg = buildRunAggregationSnapshot({
    total_jobs: 4,
    queued_jobs: 0,
    running_jobs: 0,
    completed_jobs: 4,
    failed_jobs: 0,
    dead_letter_jobs: 0,
    skipped_jobs: 0,
    total_steps: 4,
    ok_steps: 4,
    warn_steps: 0,
    error_steps: 0
  });
  const summary = buildRunSummaryFromAggregation(agg, { trigger_type: 'manual' });
  assert.equal(agg.final_status, 'completed');
  assert.equal(agg.progress_percent, 100);
  assert.equal(summary.final_status, 'completed');
  assert.equal(summary.progress_percent, 100);
  assert.equal(summary.completed_jobs, 4);
  assert.equal(summary.total_steps, 4);
});

test('aggregation summary: failed run keeps failed final_status even when all jobs are done', () => {
  const agg = buildRunAggregationSnapshot({
    total_jobs: 3,
    queued_jobs: 0,
    running_jobs: 0,
    completed_jobs: 3,
    failed_jobs: 0,
    dead_letter_jobs: 0,
    skipped_jobs: 0,
    total_steps: 2,
    ok_steps: 1,
    warn_steps: 0,
    error_steps: 1
  }, 'failed');
  const summary = buildRunSummaryFromAggregation(agg, {});
  assert.equal(agg.final_status, 'failed');
  assert.equal(agg.progress_percent, 100);
  assert.equal(summary.final_status, 'failed');
  assert.equal(summary.error_steps, 1);
});

test('aggregation race safety: summary changes from running(80) to completed(100) after last job completion', () => {
  const before = buildRunAggregationSnapshot({
    total_jobs: 5,
    queued_jobs: 0,
    running_jobs: 1,
    completed_jobs: 4,
    failed_jobs: 0,
    dead_letter_jobs: 0,
    skipped_jobs: 0
  }, 'running');
  const after = buildRunAggregationSnapshot({
    total_jobs: 5,
    queued_jobs: 0,
    running_jobs: 0,
    completed_jobs: 5,
    failed_jobs: 0,
    dead_letter_jobs: 0,
    skipped_jobs: 0
  }, 'running');
  assert.equal(before.final_status, 'running');
  assert.equal(before.progress_percent, 80);
  assert.equal(after.final_status, 'completed');
  assert.equal(after.progress_percent, 100);
});

test('aggregation consistency: manual trigger and scheduler trigger produce identical aggregate fields', () => {
  const counts = {
    total_jobs: 6,
    queued_jobs: 0,
    running_jobs: 0,
    completed_jobs: 6,
    failed_jobs: 0,
    dead_letter_jobs: 0,
    skipped_jobs: 0,
    total_steps: 6,
    ok_steps: 6,
    warn_steps: 0,
    error_steps: 0
  };
  const manualSummary = buildRunSummaryFromAggregation(buildRunAggregationSnapshot(counts, 'running'), { trigger_type: 'manual' });
  const schedulerSummary = buildRunSummaryFromAggregation(buildRunAggregationSnapshot(counts, 'running'), { trigger_type: 'interval' });
  assert.equal(manualSummary.final_status, schedulerSummary.final_status);
  assert.equal(manualSummary.progress_percent, schedulerSummary.progress_percent);
  assert.equal(manualSummary.total_jobs, schedulerSummary.total_jobs);
  assert.equal(manualSummary.completed_jobs, schedulerSummary.completed_jobs);
});

test('schema migration: workflow chunk logs store adds request and response payload columns for older installs', async () => {
  const statements = [];
  const fakeClient = {
    async query(sql) {
      statements.push(String(sql || '').replace(/\s+/g, ' ').trim());
      return { rows: [] };
    }
  };
  const config = {
    workflow_runs_schema: 'ao_system',
    workflow_desks_schema: 'ao_system',
    workflow_desks_table: 'workflow_desks_store',
    workflow_desk_versions_table: 'workflow_desk_versions_store',
    workflow_runs_table: 'workflow_runs_store',
    workflow_run_steps_table: 'workflow_run_steps_store',
    workflow_process_overrides_table: 'workflow_process_overrides_store',
    workflow_process_locks_table: 'workflow_process_locks_store',
    workflow_process_bus_table: 'workflow_process_bus_store',
    workflow_job_queue_table: 'workflow_job_queue_store',
    workflow_incremental_state_table: 'workflow_incremental_state_store',
    workflow_dependencies_table: 'workflow_process_dependencies_store',
    workflow_rate_limit_policies_table: 'workflow_rate_limit_policies_store',
    workflow_tenant_policies_table: 'workflow_tenant_policies_store',
    workflow_run_aggregation_table: 'workflow_run_aggregation_store',
    workflow_dead_jobs_table: 'workflow_dead_jobs_store',
    workflow_scheduler_leases_table: 'workflow_scheduler_leases_store',
    workflow_worker_leases_table: 'workflow_worker_leases_store',
    workflow_provider_registry_table: 'workflow_provider_registry_store',
    workflow_chunk_logs_table: 'workflow_chunk_logs_store'
  };
  await _testEnsureWorkflowAutomationTables(fakeClient, config);
  const chunkAlter = statements.find(
    (sql) =>
      sql.includes('ALTER TABLE "ao_system"."workflow_chunk_logs_store"') &&
      sql.includes('request_payload') &&
      sql.includes('response_payload')
  );
  assert.ok(chunkAlter, 'expected ALTER TABLE for workflow_chunk_logs_store to backfill payload columns');
  assert.match(chunkAlter, /ADD COLUMN IF NOT EXISTS request_payload jsonb NOT NULL DEFAULT '\{\}'::jsonb/i);
  assert.match(chunkAlter, /ADD COLUMN IF NOT EXISTS response_payload jsonb NOT NULL DEFAULT '\{\}'::jsonb/i);
});

test('chunk log write: insert keeps request/response payload expressions aligned with target columns', async () => {
  const calls = [];
  const fakeClient = {
    async query(sql, params = []) {
      calls.push({
        sql: String(sql || '').replace(/\s+/g, ' ').trim(),
        params: Array.isArray(params) ? [...params] : []
      });
      return { rows: [] };
    }
  };
  await _testWriteChunkLog(
    fakeClient,
    {
      workflow_runs_schema: 'ao_system',
      workflow_chunk_logs_table: 'workflow_chunk_logs_store'
    },
    {
      job_id: 11,
      run_uid: 'wf_run_test',
      tenant_id: 'tenant_a',
      desk_id: 24,
      desk_version_id: 16,
      start_node_id: 'start_1',
      process_code: 'desk_24_start',
      provider_code: 'demo_provider',
      endpoint_code: 'demo_endpoint',
      chunk_key: 'chunk_1',
      chunk_no: 1,
      request_payload: { method: 'GET' },
      response_payload: { rows: 2 },
      metrics_json: { ok: 2 }
    }
  );
  const insert = calls.find((call) => call.sql.includes('INSERT INTO "ao_system"."workflow_chunk_logs_store"'));
  assert.ok(insert, 'expected insert into workflow_chunk_logs_store');
  assert.match(insert.sql, /\$21::jsonb, \$22::jsonb, \$23::jsonb, \$24\)/);
  assert.equal(insert.params.length, 24);
  assert.deepEqual(JSON.parse(insert.params[20]), { method: 'GET' });
  assert.deepEqual(JSON.parse(insert.params[21]), { rows: 2 });
  assert.deepEqual(JSON.parse(insert.params[22]), { ok: 2 });
});
