function trim(value) {
  return String(value ?? '').trim();
}

function normalizePath(value) {
  return trim(value).replace(/\[(\w+)\]/g, '.$1').replace(/^\.+/, '').replace(/\.+/g, '.');
}

function hasOwn(source, key) {
  return Boolean(source) && Object.prototype.hasOwnProperty.call(source, key);
}

export function parseParserSelectFields(raw) {
  if (Array.isArray(raw)) {
    return raw.map((item) => normalizePath(item)).filter(Boolean);
  }
  return String(raw || '')
    .split(',')
    .map((item) => normalizePath(item))
    .filter(Boolean);
}

export function parseParserPublishMap(raw, fallback = {}) {
  if (raw && typeof raw === 'object' && !Array.isArray(raw)) return raw;
  const text = trim(raw);
  if (!text) return fallback;
  try {
    const parsed = JSON.parse(text);
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : fallback;
  } catch {
    return fallback;
  }
}

export function parserPublishLeaf(path) {
  const normalized = normalizePath(path);
  return normalized
    .split('.')
    .map((part) => trim(part))
    .filter(Boolean)
    .slice(-1)[0] || normalized;
}

export function parserPublishDefaultName(fieldOrPath) {
  if (fieldOrPath && typeof fieldOrPath === 'object') {
    return (
      trim(fieldOrPath.alias) ||
      trim(fieldOrPath.name) ||
      parserPublishLeaf(fieldOrPath.path) ||
      'field'
    );
  }
  return parserPublishLeaf(fieldOrPath) || 'field';
}

export function isParserPublishComplexType(typeName) {
  const raw = trim(typeName).toLowerCase();
  if (!raw) return false;
  return [
    'object',
    'array',
    'json',
    'jsonb',
    'blob',
    'container',
    'archive',
    'struct',
    'map',
    'объект',
    'массив',
    'контейнер',
    'архив'
  ].some((token) => raw.includes(token));
}

function normalizeIncomingField(raw, index = 0) {
  const path = normalizePath(raw?.path || raw?.response_path || raw?.sourcePath || raw?.source_path || raw?.name || raw?.alias || '');
  const alias = trim(raw?.alias || raw?.outputName || raw?.output_name || raw?.name || parserPublishLeaf(path));
  const name = trim(raw?.name || raw?.fieldName || raw?.field_name || alias || parserPublishLeaf(path) || `field_${index + 1}`);
  const type = trim(raw?.type || raw?.valueType || raw?.value_type || '');
  if (!(path || alias || name)) return null;
  return {
    path: path || normalizePath(alias || name),
    alias,
    name,
    type,
    defaultOutputName: parserPublishDefaultName({ path, alias, name }),
    isDirectPublishSupported: !isParserPublishComplexType(type)
  };
}

function uniqueAlternativeIndex(fields) {
  const counts = new Map();
  const store = new Map();

  (Array.isArray(fields) ? fields : []).forEach((field) => {
    const alternatives = new Set(
      [field?.path, field?.alias, field?.name, parserPublishLeaf(field?.path)]
        .map((item) => trim(item).toLowerCase())
        .filter(Boolean)
    );
    alternatives.forEach((key) => {
      counts.set(key, (counts.get(key) || 0) + 1);
      if (!store.has(key)) store.set(key, field);
    });
  });

  const out = new Map();
  counts.forEach((count, key) => {
    if (count === 1 && store.has(key)) out.set(key, store.get(key));
  });
  return out;
}

export function normalizeIncomingParserFields(descriptors = []) {
  const seen = new Set();
  const out = [];
  (Array.isArray(descriptors) ? descriptors : []).forEach((descriptor) => {
    const fields = Array.isArray(descriptor?.fields) ? descriptor.fields : [];
    fields.forEach((field, index) => {
      const normalized = normalizeIncomingField(field, index);
      if (!normalized) return;
      const key = trim(normalized.path).toLowerCase();
      if (!key || seen.has(key)) return;
      seen.add(key);
      out.push(normalized);
    });
  });
  return out;
}

export function resolveIncomingParserField(reference, incomingFields = []) {
  const fields = Array.isArray(incomingFields) ? incomingFields : [];
  const wanted = normalizePath(reference);
  if (!wanted) return null;
  const direct = fields.find((field) => trim(field?.path).toLowerCase() === wanted.toLowerCase());
  if (direct) return direct;
  const alternatives = uniqueAlternativeIndex(fields);
  return alternatives.get(wanted.toLowerCase()) || null;
}

function lookupParserValue(map, candidates) {
  const safeMap = map && typeof map === 'object' && !Array.isArray(map) ? map : {};
  for (const candidate of Array.isArray(candidates) ? candidates : []) {
    const key = trim(candidate);
    if (!key) continue;
    if (hasOwn(safeMap, key)) return safeMap[key];
  }
  return undefined;
}

function statusMeta(status) {
  if (status === 'manual') {
    return {
      label: 'manual',
      hint: 'Сложное или container-поле было взято вручную: `Добавить все` такие поля не подбирает автоматически.'
    };
  }
  if (status === 'missing upstream') {
    return {
      label: 'missing upstream',
      hint: 'Поле сохранено в settings, но во входном контракте текущего upstream его больше нет.'
    };
  }
  if (status === 'changed') {
    return {
      label: 'changed',
      hint: 'Для поля сохранены publish-изменения: имя результата, тип и/или default отличаются от upstream по умолчанию.'
    };
  }
  if (status === 'duplicate blocked') {
    return {
      label: 'duplicate blocked',
      hint: 'Имя результата конфликтует с другой строкой publish-модели. Пока конфликт не снят, поле не входит в publish contract.'
    };
  }
  return {
    label: 'auto',
    hint: 'Поле синхронизировано с upstream без дополнительных publish-override.'
  };
}

export function buildParserPublishRows({ settings = {}, incomingDescriptors = [] } = {}) {
  const incomingFields = normalizeIncomingParserFields(incomingDescriptors);
  const selectFields = parseParserSelectFields(settings?.selectFields ?? settings?.select_fields);
  const renameMap = parseParserPublishMap(settings?.renameMap ?? settings?.rename_map, {});
  const typeMap = parseParserPublishMap(settings?.typeMap ?? settings?.type_map, {});
  const defaultValues = parseParserPublishMap(settings?.defaultValues ?? settings?.default_values, {});
  const rows = [];
  const duplicateSourcePaths = [];
  const seenPaths = new Set();

  selectFields.forEach((rawReference) => {
    const resolved = resolveIncomingParserField(rawReference, incomingFields);
    const path = normalizePath(resolved?.path || rawReference);
    if (!path) return;
    const pathKey = path.toLowerCase();
    if (seenPaths.has(pathKey)) {
      duplicateSourcePaths.push(path);
      return;
    }
    seenPaths.add(pathKey);
    const leaf = parserPublishLeaf(path);
    const defaultOutputName = parserPublishDefaultName(resolved || path);
    const explicitRename = trim(lookupParserValue(renameMap, [path, rawReference, leaf]));
    const explicitType = trim(lookupParserValue(typeMap, [path, rawReference, leaf, defaultOutputName]));
    const explicitDefault = lookupParserValue(defaultValues, [path, rawReference, leaf, defaultOutputName]);
    rows.push({
      path,
      alias: explicitRename || defaultOutputName,
      type: explicitType || trim(resolved?.type || ''),
      defaultValue: explicitDefault === undefined || explicitDefault === null ? '' : String(explicitDefault),
      sourcePath: path,
      sourceAlias: trim(resolved?.alias || ''),
      sourceName: trim(resolved?.name || leaf),
      sourceType: trim(resolved?.type || ''),
      sourceExists: Boolean(resolved),
      sourceSupported: resolved ? Boolean(resolved.isDirectPublishSupported) : true,
      defaultAlias: defaultOutputName,
      hasExplicitRename: Boolean(explicitRename),
      hasExplicitType: explicitType !== '',
      hasExplicitDefault: explicitDefault !== undefined && explicitDefault !== null && String(explicitDefault) !== ''
    });
  });

  const aliasCounts = new Map();
  rows.forEach((row) => {
    const key = trim(row.alias).toLowerCase();
    if (!key) return;
    aliasCounts.set(key, (aliasCounts.get(key) || 0) + 1);
  });

  const normalizedRows = rows.map((row) => {
    const aliasKey = trim(row.alias).toLowerCase();
    const isDuplicateAlias = Boolean(aliasKey) && (aliasCounts.get(aliasKey) || 0) > 1;
    const hasOverrides = row.hasExplicitRename || row.hasExplicitType || row.hasExplicitDefault;
    let status = 'auto';
    if (!row.sourceExists) status = 'missing upstream';
    else if (isDuplicateAlias) status = 'duplicate blocked';
    else if (!row.sourceSupported) status = 'manual';
    else if (hasOverrides) status = 'changed';
    const meta = statusMeta(status);
    return {
      ...row,
      status,
      statusLabel: meta.label,
      statusHint: meta.hint,
      isDuplicateAlias,
      isMissingUpstream: !row.sourceExists,
      isValidForPublish: row.sourceExists && !isDuplicateAlias
    };
  });

  return {
    incomingFields,
    rows: normalizedRows,
    duplicateSourcePaths
  };
}

export function serializeParserPublishRows(rows = [], incomingDescriptors = []) {
  const incomingFields = normalizeIncomingParserFields(incomingDescriptors);
  const incomingByPath = new Map(
    incomingFields.map((field) => [trim(field.path).toLowerCase(), field])
  );
  const selectFields = [];
  const renameMap = {};
  const typeMap = {};
  const defaultValues = {};
  const seen = new Set();

  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const path = normalizePath(row?.path || row?.sourcePath);
    if (!path) return;
    const key = path.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    selectFields.push(path);
    const source = incomingByPath.get(key) || null;
    const defaultAlias = parserPublishDefaultName(source || path);
    const sourceType = trim(source?.type || '');
    const alias = trim(row?.alias || defaultAlias);
    const type = trim(row?.type || '');
    const defaultValue = row?.defaultValue ?? '';
    if (alias && alias !== defaultAlias) renameMap[path] = alias;
    if (type && type !== sourceType) typeMap[path] = type;
    if (defaultValue !== '') defaultValues[path] = defaultValue;
  });

  return { selectFields, renameMap, typeMap, defaultValues };
}

export function buildParserPublishDescriptorFields({ settings = {}, incomingDescriptors = [], origin = 'parser_settings' } = {}) {
  const { rows } = buildParserPublishRows({ settings, incomingDescriptors });
  const requireExistingSource = Array.isArray(incomingDescriptors) && incomingDescriptors.length > 0;
  return rows
    .filter((row) => (requireExistingSource ? row.isValidForPublish : !row.isDuplicateAlias))
    .map((row) => ({
      name: trim(row.alias || row.defaultAlias || parserPublishLeaf(row.path)),
      alias: trim(row.alias || row.defaultAlias || parserPublishLeaf(row.path)) || undefined,
      type: trim(row.type || row.sourceType || '') || undefined,
      path: trim(row.path) || undefined,
      origin
    }));
}
