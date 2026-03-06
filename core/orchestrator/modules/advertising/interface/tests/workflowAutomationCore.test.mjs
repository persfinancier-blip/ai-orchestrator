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
  normalizeNodeIoEnvelope,
  composeNodeOutputEnvelope,
  executeTableParserNode,
  executeDbWriteNode,
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
