const OPENABLE_TOOL_TYPES = new Set([
  'start_process',
  'api_request',
  'http_request',
  'table_node',
  'table_parser',
  'action_prep',
  'api_mutation',
  'db_write',
  'split_data',
  'merge_data',
  'condition_if',
  'condition_switch',
  'code_node'
]);

export function canOpenWorkflowNodeSettings(node) {
  if (!node || typeof node !== 'object') return false;
  const nodeType = String(node.type || '').trim().toLowerCase();
  if (nodeType === 'data' && String(node?.config?.group || '').trim().toLowerCase() === 'api_requests') return true;
  if (nodeType !== 'tool') return false;
  const toolType = String(node?.config?.toolType || '').trim().toLowerCase();
  return OPENABLE_TOOL_TYPES.has(toolType);
}

export function openableWorkflowToolTypes() {
  return [...OPENABLE_TOOL_TYPES];
}
