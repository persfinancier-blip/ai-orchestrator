function asText(value) {
  return String(value || '').trim();
}

export const WORKFLOW_DESK_VISIBLE_PANES = Object.freeze(['workspace', 'api', 'clients']);

export function resolveWorkflowDeskPane({ route = '', paneParam = '', apiStoreId = null } = {}) {
  const normalizedRoute = asText(route) || 'desk/data';
  const normalizedPane = asText(paneParam).toLowerCase();
  const hasApiStoreId = Number.isFinite(Number(apiStoreId)) && Number(apiStoreId) > 0;

  if (normalizedRoute === 'desk/workflow' || normalizedPane === 'api' || hasApiStoreId) return 'api';
  if (normalizedPane === 'clients') return 'clients';
  return 'workspace';
}

export function sanitizeWorkflowDeskParams({ route = '', paneParam = '', params = new URLSearchParams() } = {}) {
  const nextParams = new URLSearchParams(params.toString());
  const normalizedRoute = asText(route) || 'desk/data';
  const normalizedPane = asText(paneParam).toLowerCase();

  if (normalizedRoute === 'desk/workflow') {
    nextParams.set('pane', 'api');
    return { route: 'desk/data', params: nextParams, changed: true };
  }

  if (normalizedPane === 'parser') {
    nextParams.delete('pane');
    nextParams.delete('api_store_id');
    nextParams.delete('from_node');
    return { route: 'desk/data', params: nextParams, changed: true };
  }

  return { route: normalizedRoute, params: nextParams, changed: false };
}

export function shouldRefreshWorkflowDeskTables(pane) {
  return pane === 'api' || pane === 'clients';
}
