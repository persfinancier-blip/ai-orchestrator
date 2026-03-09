import test from 'node:test';
import assert from 'node:assert/strict';

import {
  buildClientCode,
  buildClientListItem,
  buildClientModuleSources,
  buildClientSummaryView,
  sanitizeRecordForTemplate
} from '../server/clientModuleCore.mjs';
import { CLIENT_MODULE_SECTION_DEFINITIONS, getClientModuleTemplateByKey } from '../shared/clientModuleTemplates.mjs';

test('client module: section definitions are exported for shared UI/server usage', () => {
  assert.equal(CLIENT_MODULE_SECTION_DEFINITIONS.main_data.title, 'Основные данные');
  assert.equal(CLIENT_MODULE_SECTION_DEFINITIONS.accesses.title, 'Платформы и доступы');
  assert.equal(CLIENT_MODULE_SECTION_DEFINITIONS.kpis.mode, 'multi');
});

test('client module: buildClientCode generates stable unique code', () => {
  const code = buildClientCode('Свежий Рынок', ['client', 'svexii_rynok']);
  assert.match(code, /^[a-z0-9_]+$/);
  assert.notEqual(code, 'svexii_rynok');
});

test('client module: sanitizeRecordForTemplate normalizes typed values', () => {
  const row = sanitizeRecordForTemplate('client_accesses', {
    id: '7',
    client_id: '55',
    platform_code: 'ozon',
    is_active: 'true',
    expires_at: '2026-03-14T12:00:00.000Z',
    comment: '  test  ',
    unknown_field: 'skip me'
  }, { preserveId: true, preserveClientId: true });

  assert.deepEqual(row, {
    id: 7,
    client_id: 55,
    platform_code: 'ozon',
    is_active: true,
    expires_at: '2026-03-14T12:00:00.000Z',
    comment: 'test'
  });
});

test('client module: summary view builds warnings from tables', () => {
  const summary = buildClientSummaryView({
    goals: [{ id: 1, is_active: true, goal_name: 'Рост продаж' }],
    kpis: [],
    accesses: [
      {
        id: 1,
        is_active: true,
        platform_name: 'Wildberries',
        expires_at: new Date(Date.now() + 3 * 86400000).toISOString(),
        data_table_ref: ''
      }
    ],
    constraints: [{ id: 1, is_active: true, constraint_type: 'budget' }],
    summaryMetrics: { budget_plan: 1000 },
    actionItems: [{ id: 1, is_active: true, status: 'open', title: 'Проверить KPI' }]
  });

  assert.equal(summary.counts.goals, 1);
  assert.equal(summary.counts.kpis, 0);
  assert.ok(summary.actionItems.some((item) => item.type === 'missing_kpi'));
  assert.ok(summary.actionItems.some((item) => item.type === 'token_expiring'));
  assert.ok(summary.actionItems.some((item) => item.type === 'missing_data_table'));
});

test('client module: list item summary uses accesses and warnings', () => {
  const item = buildClientListItem(
    { id: 5, client_code: 'fresh_market', client_display_name: 'Свежий Рынок', status: 'active', comment: '' },
    {
      goals: [{ id: 1, is_active: true }],
      kpis: [{ id: 1, is_active: true }],
      accesses: [
        { id: 1, is_active: true, platform_name: 'Ozon' },
        { id: 2, is_active: true, platform_name: 'Wildberries' }
      ],
      actionItems: [{ id: 1, is_active: true, status: 'open' }]
    }
  );

  assert.equal(item.id, 5);
  assert.match(item.platform_summary, /Ozon/);
  assert.equal(item.active_goal_count, 1);
  assert.equal(item.active_kpi_count, 1);
  assert.equal(item.active_access_count, 2);
});

test('client module: sources map points to real built-in templates', () => {
  const sources = buildClientModuleSources();
  const accesses = getClientModuleTemplateByKey('client_accesses');
  assert.equal(sources.list.table_name, 'clients');
  assert.equal(sources.accesses.table_name, accesses.table_name);
  assert.equal(Array.isArray(sources.summary), true);
  assert.equal(sources.summary.length, 3);
});
