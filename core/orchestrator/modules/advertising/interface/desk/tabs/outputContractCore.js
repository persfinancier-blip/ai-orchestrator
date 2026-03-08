const ARRAY_TOKEN = '[]';

function isObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function toSnakeCase(value) {
  return String(value || '')
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replace(/[^a-zA-Z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .replace(/__+/g, '_')
    .toLowerCase();
}

export function stripResponseEnvelopePrefix(path) {
  const raw = String(path || '').trim();
  if (!raw) return '';
  return raw
    .replace(/^responses\[\d+\]\.response\.?/, '')
    .replace(/^responses\[\]\.response\.?/, '')
    .replace(/^responses\[\d+\]\.?/, '')
    .replace(/^responses\[\]\.?/, '')
    .replace(/^response\./, '')
    .replace(/^response$/, '')
    .trim();
}

export function normalizeTemplatePath(path) {
  const stripped = stripResponseEnvelopePrefix(path)
    .replace(/\[(\d+)\]/g, ARRAY_TOKEN)
    .replace(/\.\./g, '.')
    .replace(/^\.+|\.+$/g, '')
    .trim();
  return stripped === ARRAY_TOKEN ? '' : stripped;
}

export function parseTemplatePathParts(path) {
  const raw = normalizeTemplatePath(path);
  if (!raw) return [];
  const out = [];
  for (const part of raw.split('.')) {
    if (!part) continue;
    const re = /([^[\]]+)|\[\]/g;
    let match;
    while ((match = re.exec(part))) {
      if (match[1]) out.push(match[1]);
      else out.push(ARRAY_TOKEN);
    }
  }
  return out;
}

function normalizeLeafPath(path) {
  return String(path || '')
    .replace(/^\.+|\.+$/g, '')
    .replace(/\.\./g, '.')
    .trim();
}

export function joinTemplatePath(rootPath, relativePath = '') {
  const root = normalizeTemplatePath(rootPath);
  const relative = normalizeLeafPath(relativePath).replace(/\[(\d+)\]/g, ARRAY_TOKEN);
  if (!root) return relative;
  if (!relative) return root;
  return `${root}.${relative}`;
}

export function valueTypeLabel(value) {
  if (Array.isArray(value)) return 'Массив';
  if (value === null) return 'Пустое значение';
  if (isObject(value)) return 'Объект';
  if (typeof value === 'number') return 'Число';
  if (typeof value === 'boolean') return 'Да / нет';
  if (typeof value === 'string') return 'Текст';
  return 'Значение';
}

function lastMeaningfulSegment(path) {
  const parts = parseTemplatePathParts(path).filter((part) => part !== ARRAY_TOKEN);
  return String(parts[parts.length - 1] || '').trim();
}

export function buildAliasFromPath(path) {
  const raw = normalizeLeafPath(String(path || '').replace(/\[\]/g, ''));
  const bySegments = raw
    .split('.')
    .map((part) => toSnakeCase(part))
    .filter(Boolean)
    .join('_');
  if (bySegments) return bySegments;
  const fallback = toSnakeCase(lastMeaningfulSegment(path));
  return fallback || 'field';
}

function uniqueAlias(baseAlias, used) {
  const normalized = toSnakeCase(baseAlias) || 'field';
  let candidate = normalized;
  let idx = 2;
  while (used.has(candidate)) {
    candidate = `${normalized}_${idx}`;
    idx += 1;
  }
  used.add(candidate);
  return candidate;
}

function collectLeaves(node, basePath, out) {
  if (Array.isArray(node)) {
    const sampleItems = node.slice(0, 3);
    if (!sampleItems.length) return;
    sampleItems.forEach((item) => {
      collectLeaves(item, joinTemplatePath(basePath, ARRAY_TOKEN), out);
    });
    return;
  }
  if (isObject(node)) {
    Object.entries(node).forEach(([key, value]) => {
      const next = joinTemplatePath(basePath, key);
      if (Array.isArray(value) || isObject(value)) {
        collectLeaves(value, next, out);
        return;
      }
      out.push({
        path: normalizeLeafPath(next),
        valueType: valueTypeLabel(value)
      });
    });
    return;
  }
  out.push({
    path: normalizeLeafPath(basePath),
    valueType: valueTypeLabel(node)
  });
}

export function buildOutputFieldsFromNode(sampleNode, rootPath = '') {
  const fields = [];
  collectLeaves(sampleNode, '', fields);
  const used = new Set();
  return fields
    .filter((entry) => String(entry.path || '').trim())
    .map((entry, idx) => ({
      id: `out_${Date.now()}_${idx}_${Math.random().toString(16).slice(2, 8)}`,
      rootPath: normalizeTemplatePath(rootPath),
      path: normalizeLeafPath(entry.path),
      alias: uniqueAlias(buildAliasFromPath(entry.path), used),
      valueType: entry.valueType
    }));
}

export function extractTemplateValues(source, path) {
  const normalizedPath = normalizeTemplatePath(path);
  const parts = parseTemplatePathParts(normalizedPath);
  const roots = [];
  if (Array.isArray(source?.responses)) {
    source.responses.forEach((entry) => {
      if (entry && typeof entry === 'object' && 'response' in entry) roots.push(entry.response);
      else roots.push(entry);
    });
  }
  roots.push(source);
  const out = [];

  function walk(node, idx) {
    if (idx >= parts.length) {
      out.push(node);
      return;
    }
    const part = parts[idx];
    if (part === ARRAY_TOKEN) {
      if (!Array.isArray(node)) return;
      node.forEach((item) => walk(item, idx + 1));
      return;
    }
    if (node == null) return;
    walk(node[part], idx + 1);
  }

  roots.forEach((root) => walk(root, 0));
  return out.filter((value) => value !== undefined);
}

export function resolveOutputContractValue(source, entry) {
  if (!entry) return undefined;
  const fullPath = joinTemplatePath(entry.rootPath, entry.path);
  const values = extractTemplateValues(source, fullPath);
  if (!values.length) return undefined;
  return values.length === 1 ? values[0] : values;
}

export function fullOutputContractPath(entry) {
  return joinTemplatePath(entry?.rootPath, entry?.path);
}
