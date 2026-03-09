import {
  CLIENT_MODULE_SECTION_DEFINITIONS as CLIENT_SECTION_DEFINITIONS,
  CLIENT_MODULE_TEMPLATES,
  clientModuleSourceMeta,
  getClientModuleTemplateByKey
} from '../shared/clientModuleTemplates.mjs';

export { CLIENT_SECTION_DEFINITIONS };

export function buildClientModuleSources() {
  const raw = clientModuleSourceMeta();
  return {
    list: raw.clients,
    main_data: raw.client_main_data,
    legal_entities: raw.client_legal_entities,
    contracts: raw.client_contracts,
    payment_terms: raw.client_payment_terms,
    payment_schedule: raw.client_payment_schedule,
    goals: raw.client_goals,
    kpis: raw.client_kpis,
    accesses: raw.client_accesses,
    summary: [raw.client_summary_metrics, raw.client_constraints, raw.client_action_items]
  };
}

function normalizeCodePart(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-zA-Z0-9а-яА-ЯёЁ]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 24);
}

export function buildClientCode(displayName, usedCodes = []) {
  const baseRaw = normalizeCodePart(displayName) || 'client';
  const base = baseRaw
    .replace(/[а-яё]/gi, 'x')
    .replace(/_+/g, '_')
    .slice(0, 20) || 'client';
  const used = new Set((Array.isArray(usedCodes) ? usedCodes : []).map((item) => String(item || '').trim().toLowerCase()));
  if (!used.has(base)) return base;
  let idx = 2;
  while (idx < 100000) {
    const candidate = `${base}_${idx}`;
    if (!used.has(candidate)) return candidate;
    idx += 1;
  }
  return `${base}_${Date.now()}`;
}

export function normalizeTypedValue(fieldType, value) {
  const type = String(fieldType || '').trim().toLowerCase();
  if (value === undefined) return undefined;
  if (value === null) return null;
  if (type === 'boolean') {
    if (value === '' || value === null) return false;
    if (typeof value === 'boolean') return value;
    const raw = String(value).trim().toLowerCase();
    return raw === 'true' || raw === '1' || raw === 'yes' || raw === 'да' || raw === 'on';
  }
  if (type === 'int' || type === 'bigint') {
    const raw = String(value).trim();
    if (!raw) return null;
    const parsed = Number(raw);
    return Number.isFinite(parsed) ? Math.trunc(parsed) : null;
  }
  if (type === 'numeric') {
    const raw = String(value).trim();
    if (!raw) return null;
    const parsed = Number(raw);
    return Number.isFinite(parsed) ? parsed : null;
  }
  if (type === 'jsonb' || type === 'json_payload') {
    if (typeof value === 'object') return value;
    const raw = String(value).trim();
    if (!raw) return null;
    try {
      return JSON.parse(raw);
    } catch {
      return null;
    }
  }
  if (type === 'date' || type === 'timestamptz') {
    const raw = String(value).trim();
    return raw || null;
  }
  const raw = String(value ?? '').trim();
  return raw || null;
}

export function sanitizeRecordForTemplate(templateKey, rawRecord, options = {}) {
  const template = getClientModuleTemplateByKey(templateKey);
  const raw = rawRecord && typeof rawRecord === 'object' && !Array.isArray(rawRecord) ? rawRecord : {};
  if (!template) return {};
  const preserveId = Boolean(options.preserveId);
  const preserveClientId = Boolean(options.preserveClientId);
  const out = {};
  for (const column of template.columns) {
    const field = String(column.field_name || '').trim();
    if (!field) continue;
    if (field === 'id' && !preserveId) continue;
    if (field === 'client_id' && !preserveClientId) continue;
    if (field.startsWith('ao_')) continue;
    const next = normalizeTypedValue(column.field_type, raw[field]);
    if (next === undefined) continue;
    out[field] = next;
  }
  return out;
}

function daysUntil(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  const target = new Date(raw);
  if (Number.isNaN(target.getTime())) return null;
  return Math.ceil((target.getTime() - Date.now()) / 86400000);
}

export function buildClientSummaryView(payload = {}) {
  const goals = Array.isArray(payload.goals) ? payload.goals : [];
  const kpis = Array.isArray(payload.kpis) ? payload.kpis : [];
  const accesses = Array.isArray(payload.accesses) ? payload.accesses : [];
  const constraints = Array.isArray(payload.constraints) ? payload.constraints : [];
  const actionItems = Array.isArray(payload.actionItems) ? payload.actionItems : [];
  const metrics = payload.summaryMetrics && typeof payload.summaryMetrics === 'object' ? payload.summaryMetrics : {};
  const activeGoals = goals.filter((item) => item && item.is_active !== false);
  const activeKpis = kpis.filter((item) => item && item.is_active !== false);
  const activeAccesses = accesses.filter((item) => item && item.is_active !== false);
  const activeConstraints = constraints.filter((item) => item && item.is_active !== false);
  const openActionItems = actionItems.filter(
    (item) => item && item.is_active !== false && !['done', 'closed', 'resolved'].includes(String(item.status || '').trim().toLowerCase())
  );

  const computedWarnings = [];
  if (!activeKpis.length) {
    computedWarnings.push({
      type: 'missing_kpi',
      title: 'Не заполнены KPI',
      message: 'У клиента нет активных KPI.',
      severity: 'warning'
    });
  }
  if (!activeAccesses.length) {
    computedWarnings.push({
      type: 'missing_access',
      title: 'Нет активных доступов',
      message: 'У клиента нет активных платформ или сервисных доступов.',
      severity: 'warning'
    });
  }
  for (const access of activeAccesses) {
    const days = daysUntil(access.expires_at);
    if (days !== null && days <= 7) {
      computedWarnings.push({
        type: 'token_expiring',
        title: `Истекает доступ ${access.platform_name || access.platform_code || ''}`.trim(),
        message: days >= 0 ? `Срок действия заканчивается через ${days} дн.` : 'Срок действия уже истёк.',
        severity: days < 0 ? 'error' : 'warning'
      });
    }
    if (!String(access.data_table_ref || '').trim()) {
      computedWarnings.push({
        type: 'missing_data_table',
        title: `Не привязана таблица данных: ${access.platform_name || access.platform_code || 'доступ'}`,
        message: 'У доступа не заполнена ссылка на таблицу данных.',
        severity: 'warning'
      });
    }
  }

  return {
    goals: activeGoals,
    kpis: activeKpis,
    constraints: activeConstraints,
    metrics,
    actionItems: [...openActionItems, ...computedWarnings],
    counts: {
      goals: activeGoals.length,
      kpis: activeKpis.length,
      accesses: activeAccesses.length,
      warnings: openActionItems.length + computedWarnings.length
    }
  };
}

export function buildClientListItem(clientRow, payload = {}) {
  const client = clientRow && typeof clientRow === 'object' ? clientRow : {};
  const summary = buildClientSummaryView(payload);
  const platformSummary = (Array.isArray(payload.accesses) ? payload.accesses : [])
    .filter((item) => item && item.is_active !== false)
    .map((item) => String(item.platform_name || item.platform_code || '').trim())
    .filter(Boolean)
    .slice(0, 4)
    .join(' ');
  return {
    id: Number(client.id || 0) || 0,
    client_code: String(client.client_code || '').trim(),
    client_display_name: String(client.client_display_name || '').trim(),
    status: String(client.status || '').trim(),
    comment: String(client.comment || '').trim(),
    platform_summary: platformSummary,
    warning_count: summary.counts.warnings,
    active_goal_count: summary.counts.goals,
    active_kpi_count: summary.counts.kpis,
    active_access_count: summary.counts.accesses
  };
}

export function getClientTemplateKeys() {
  return CLIENT_MODULE_TEMPLATES.map((item) => item.key);
}
