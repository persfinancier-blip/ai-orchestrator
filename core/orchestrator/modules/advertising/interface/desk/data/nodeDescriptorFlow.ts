export type NodeDescriptorField = {
  name: string;
  alias?: string;
  type?: string;
  path?: string;
  origin?: string;
  source?: string;
  nullable?: boolean;
  optional?: boolean;
};

export type NodeDescriptorOutputKind =
  | 'structured object'
  | 'row set'
  | 'tabular contract'
  | 'text payload'
  | 'archive/container'
  | 'link/reference'
  | 'mixed'
  | 'unknown';

export type NodeDescriptorDetectionMeta = {
  detectedFormat?: string;
  readMode?: string;
  parseMode?: string;
  payloadPath?: string;
  recordPath?: string;
  confidence?: string | number;
  warnings?: string[];
  appliedSteps?: string[];
};

export type NodeDescriptorSample = {
  sampleRaw?: any;
  samplePayload?: any;
  sampleRows?: Array<Record<string, any>>;
  sampleColumns?: string[];
  sampleMeta?: Record<string, any>;
};

export type NodeDescriptor = {
  descriptorVersion: 'node_descriptor_v1';
  sourceNodeId: string;
  sourceNodeName: string;
  sourceNodeType: string;
  sourcePort: string;
  editorType?: string;
  runtimeHandler?: string;
  outputKind: NodeDescriptorOutputKind;
  fields: NodeDescriptorField[];
  detection?: NodeDescriptorDetectionMeta;
  sample?: NodeDescriptorSample;
};

export type NodeDescriptorFlow = {
  nodeId?: string;
  upstreamDescriptors?: NodeDescriptor[];
};

function trim(value: any) {
  return String(value ?? '').trim();
}

function normalizePath(value: any) {
  return trim(value).replace(/\[(\w+)\]/g, '.$1').replace(/^\.+/, '').replace(/\.+/g, '.');
}

function aliasFromPath(value: string) {
  const path = normalizePath(value);
  if (!path) return '';
  const leaf = path
    .split('.')
    .map((part) => trim(part))
    .filter(Boolean)
    .slice(-1)[0];
  return trim(leaf || path);
}

export function normalizeDescriptorField(raw: any, index = 0, fallbackPath = ''): NodeDescriptorField | null {
  const path = normalizePath(raw?.path || raw?.response_path || raw?.sourcePath || raw?.source_path || fallbackPath || '');
  const alias =
    trim(raw?.alias || raw?.outputName || raw?.output_name || raw?.field_alias || raw?.fieldAlias || raw?.name || '') ||
    aliasFromPath(path);
  const name =
    trim(raw?.name || raw?.fieldName || raw?.field_name || raw?.outputName || raw?.output_name || '') ||
    alias ||
    aliasFromPath(path) ||
    `field_${index + 1}`;
  const type = trim(raw?.type || raw?.valueType || raw?.value_type || '');
  const origin = trim(raw?.origin || raw?.source || '');
  if (!(name || alias || path)) return null;
  const field: NodeDescriptorField = { name };
  if (alias) field.alias = alias;
  if (type) field.type = type;
  if (path) field.path = path;
  if (origin) {
    field.origin = origin;
    field.source = origin;
  }
  if (typeof raw?.nullable === 'boolean') field.nullable = raw.nullable;
  if (typeof raw?.optional === 'boolean') field.optional = raw.optional;
  return field;
}

export function uniqueDescriptorFields(fields: NodeDescriptorField[]) {
  const seen = new Set<string>();
  return (Array.isArray(fields) ? fields : []).filter((field) => {
    if (!field || typeof field !== 'object') return false;
    const key = [trim(field.path || ''), trim(field.alias || ''), trim(field.name || '')]
      .map((item) => item.toLowerCase())
      .join('::');
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

export function descriptorFieldKey(field: NodeDescriptorField | null | undefined) {
  return trim(field?.path || field?.alias || field?.name || '');
}

export function descriptorFieldLabel(field: NodeDescriptorField | null | undefined) {
  const name = trim(field?.alias || field?.name || field?.path || '');
  const type = trim(field?.type || '');
  return type ? `${name} (${type})` : name;
}

export function descriptorFields(descriptor: NodeDescriptor | null | undefined) {
  return uniqueDescriptorFields(Array.isArray(descriptor?.fields) ? descriptor!.fields : []);
}

export function descriptorSampleRows(descriptor: NodeDescriptor | null | undefined) {
  const rows = Array.isArray(descriptor?.sample?.sampleRows) ? descriptor!.sample!.sampleRows : [];
  return rows.filter((item) => item && typeof item === 'object' && !Array.isArray(item));
}

export function descriptorSampleColumns(descriptor: NodeDescriptor | null | undefined) {
  const explicit = Array.isArray(descriptor?.sample?.sampleColumns) ? descriptor!.sample!.sampleColumns : [];
  if (explicit.length) return explicit.map((item) => trim(item)).filter(Boolean);
  const rows = descriptorSampleRows(descriptor);
  return Array.from(
    new Set(
      rows
        .flatMap((row) => (row && typeof row === 'object' ? Object.keys(row) : []))
        .map((item) => trim(item))
        .filter(Boolean)
    )
  );
}

export function descriptorOutputKindLabel(kind: NodeDescriptorOutputKind) {
  if (kind === 'row set') return 'row set';
  if (kind === 'tabular contract') return 'tabular contract';
  if (kind === 'structured object') return 'structured object';
  if (kind === 'text payload') return 'text payload';
  if (kind === 'archive/container') return 'archive/container';
  if (kind === 'link/reference') return 'link/reference';
  if (kind === 'mixed') return 'mixed';
  return 'unknown';
}

export function emptyNodeDescriptor(sourceNodeId = '', sourcePort = 'out'): NodeDescriptor {
  return {
    descriptorVersion: 'node_descriptor_v1',
    sourceNodeId: trim(sourceNodeId),
    sourceNodeName: '',
    sourceNodeType: '',
    sourcePort: trim(sourcePort) || 'out',
    outputKind: 'unknown',
    fields: []
  };
}
