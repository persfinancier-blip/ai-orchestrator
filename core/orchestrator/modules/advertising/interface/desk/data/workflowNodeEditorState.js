function isPlainObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

export function safeWorkflowToolSettings(node) {
  if (!node || node.type !== 'tool' || !isPlainObject(node.config)) return {};
  const settings = node.config.settings;
  return isPlainObject(settings) ? settings : {};
}

export function mergeWorkflowToolSettings(defaultSettings, node) {
  const base = isPlainObject(defaultSettings) ? defaultSettings : {};
  return { ...base, ...safeWorkflowToolSettings(node) };
}

export function actionPrepEditorMountKey(node) {
  const settings = safeWorkflowToolSettings(node);
  return [
    String(node?.id || '').trim(),
    String(settings.sourceMode || 'node').trim() || 'node',
    String(settings.sourceSchema || '').trim(),
    String(settings.sourceTable || '').trim()
  ].join(':');
}

export function apiMutationEditorMountKey(node) {
  const settings = safeWorkflowToolSettings(node);
  return [
    String(node?.id || '').trim(),
    String(settings.endpointUrl || '').trim(),
    String(settings.requestMode || 'row_per_request').trim() || 'row_per_request'
  ].join(':');
}
