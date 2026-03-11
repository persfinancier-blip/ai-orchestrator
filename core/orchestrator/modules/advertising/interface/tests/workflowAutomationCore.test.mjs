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
  normalizeNodeIoEnvelope,
  composeNodeOutputEnvelope,
  resolveAliasesFromParameterDefinitions,
  writeApiStepLog,
  executeTableParserNode,
  executeTableNode,
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

test('api runtime contract: parameter definitions resolve client-scoped token alias', async () => {
  const calls = [];
  const client = {
    async query(sql, params = []) {
      calls.push({ sql: String(sql || ''), params });
      return { rows: [{ value: 'wb_api_key_for_client_18' }], rowCount: 1 };
    }
  };

  const resolved = await resolveAliasesFromParameterDefinitions(
    client,
    [
      {
        alias: 'token',
        sourceSchema: 'ao_clients',
        sourceTable: 'client_accesses',
        sourceField: 'api_key',
        conditions: [
          { field: 'client_id', operator: 'equals', compareValue: '{{client_id}}' },
          { field: 'platform_code', operator: 'equals', compareValue: 'wildberries' },
          { field: 'is_active', operator: 'equals', compareValue: 'true' }
        ]
      }
    ],
    ['token'],
    { client_id: '18' }
  );

  assert.equal(resolved.map.token, 'wb_api_key_for_client_18');
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].params, ['18', 'wildberries', true]);
});

test('api runtime contract: api step log writes redacted request/response rows', async () => {
  const calls = [];
  const client = {
    async query(sql, params = []) {
      const text = String(sql || '');
      calls.push({ sql: text, params });
      if (/information_schema\.columns/i.test(text)) {
        return {
          rows: [
            'run_id',
            'api_name',
            'execution_mode',
            'dispatch_mode',
            'entity_key',
            'entity_label',
            'row_index',
            'wave_no',
            'page_no',
            'iteration_reason',
            'decision',
            'stop_reason',
            'error_message',
            'status_code',
            'request_payload',
            'response_payload',
            'pagination_values',
            'duration_ms',
            'created_at',
            'updated_at'
          ].map((column_name) => ({ column_name }))
        };
      }
      if (text.startsWith('INSERT INTO "bronze"."api_step_log"')) {
        return { rows: [], rowCount: 1 };
      }
      return { rows: [], rowCount: 0 };
    }
  };

  const out = await writeApiStepLog(
    client,
    {
      api_name: 'WB cards',
      execution_mode: 'sync',
      dispatch_mode: 'single',
      response_log_enabled: true,
      response_log_schema: 'bronze',
      response_log_table: 'api_step_log',
      response_log_write_request_payload: true,
      response_log_write_response_payload: true,
      response_log_write_pagination_values: true,
      response_log_only_errors: false,
      response_log_response_payload_limit: 1000
    },
    { run_uid: 'wf_run_1' },
    {
      requestPreview: {
        requests: [
          {
            entity_index: 1,
            page: 1,
            headers: { Authorization: 'secret-key' },
            body: { settings: { cursor: { limit: 5 } } }
          }
        ]
      },
      responsePreview: {
        responses: [
          {
            entity_index: 1,
            page: 1,
            status: 200,
            response: { cards: [{ nmID: 1, title: 'A' }] }
          }
        ]
      },
      metrics: {}
    }
  );

  assert.equal(out.written, 1);
  const insertCall = calls.find((call) => call.sql.startsWith('INSERT INTO "bronze"."api_step_log"'));
  assert.ok(insertCall);
  const payloadText = JSON.stringify(insertCall.params || []);
  assert.doesNotMatch(payloadText, /secret-key/);
  assert.match(payloadText, /\*\*\*/);
  assert.match(payloadText, /"cards"/);
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
