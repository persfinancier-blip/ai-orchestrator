import { buildAliasFromPath } from './outputContractCore.js';

function parseJsonObjectSafe(raw, fallback = {}) {
  if (raw && typeof raw === 'object' && !Array.isArray(raw)) return raw;
  const txt = String(raw || '').trim();
  if (!txt) return fallback;
  try {
    const parsed = JSON.parse(txt);
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : fallback;
  } catch {
    return fallback;
  }
}

function parseCsvList(raw) {
  if (Array.isArray(raw)) return raw.map((x) => String(x || '').trim()).filter(Boolean);
  return String(raw || '')
    .split(',')
    .map((x) => x.trim())
    .filter(Boolean);
}

function lastSegment(path) {
  return String(path || '')
    .replace(/\[(\d+)\]/g, '')
    .replace(/\[\]/g, '')
    .split('.')
    .map((part) => part.trim())
    .filter(Boolean)
    .slice(-1)[0] || '';
}

function sampleValueFromType(typeName, alias) {
  const raw = String(typeName || '').trim().toLowerCase();
  if (!raw) return String(alias || 'value');
  if (raw.includes('числ') || raw.includes('number') || raw.includes('numeric') || raw.includes('integer') || raw.includes('int')) {
    return 0;
  }
  if (raw.includes('да / нет') || raw.includes('boolean') || raw.includes('bool')) {
    return false;
  }
  if (raw.includes('массив') || raw.includes('array')) {
    return [];
  }
  if (raw.includes('объект') || raw.includes('object')) {
    return {};
  }
  return String(alias || 'value');
}

function normalizeOutputParameters(rawList) {
  const list = Array.isArray(rawList) ? rawList : [];
  return list
    .map((item, idx) => {
      const path = String(item?.path ?? item?.response_path ?? '').trim();
      const rootPath = String(item?.rootPath ?? item?.root_path ?? '').trim();
      const alias = String(item?.alias || '').trim() || buildAliasFromPath(path || rootPath || `field_${idx + 1}`);
      const valueType = String(item?.valueType ?? item?.value_type ?? '').trim();
      if (!alias || !(path || rootPath)) return null;
      return { alias, valueType, path, rootPath };
    })
    .filter(Boolean);
}

function outputParametersFromTemplate(template) {
  const src = template && typeof template === 'object' ? template : {};
  const cfg = src.config_json && typeof src.config_json === 'object' ? src.config_json : {};
  const mapping = cfg.mapping_json && typeof cfg.mapping_json === 'object' ? cfg.mapping_json : {};
  const direct =
    Array.isArray(src.output_parameters) ? src.output_parameters : Array.isArray(src.outputParameters) ? src.outputParameters : [];
  const cfgList = Array.isArray(cfg.output_parameters) ? cfg.output_parameters : [];
  const mappingList = Array.isArray(mapping.output_parameters) ? mapping.output_parameters : [];
  const picked =
    Array.isArray(src.picked_paths) ? src.picked_paths : Array.isArray(src.pickedPaths) ? src.pickedPaths : [];
  return {
    outputParameters: normalizeOutputParameters(direct.length ? direct : cfgList.length ? cfgList : mappingList),
    pickedPaths: picked.map((x) => String(x || '').trim()).filter(Boolean)
  };
}

function buildPreviewFromApiTemplate(template) {
  const contract = outputParametersFromTemplate(template);
  if (!contract.outputParameters.length) {
    return {
      rows: [],
      columns: [],
      message: 'У выбранного шаблона ноды ещё не настроены выходные параметры. Сначала настрой выходной контракт шаблона источника.'
    };
  }
  const row = {};
  contract.outputParameters.forEach((item) => {
    row[item.alias] = sampleValueFromType(item.valueType, item.alias);
  });
  return {
    rows: [row],
    columns: Object.keys(row),
    message: `Preview входа построен из выходного контракта шаблона ноды. Полей: ${contract.outputParameters.length}.`
  };
}

function buildPreviewFromParserTemplate(template) {
  const src = template && typeof template === 'object' ? template : {};
  const cfg = src.config_json && typeof src.config_json === 'object' ? src.config_json : {};

  const contract = outputParametersFromTemplate(template);
  if (contract.outputParameters.length) {
    const row = {};
    contract.outputParameters.forEach((item) => {
      row[item.alias] = sampleValueFromType(item.valueType, item.alias);
    });
    return {
      rows: [row],
      columns: Object.keys(row),
      message: `Preview входа построен из выходного контракта шаблона ноды. Полей: ${contract.outputParameters.length}.`
    };
  }

  const selectFields = parseCsvList(cfg.selectFields ?? cfg.select_fields);
  const renameMap = parseJsonObjectSafe(cfg.renameMap ?? cfg.rename_map, {});
  const defaultValues = parseJsonObjectSafe(cfg.defaultValues ?? cfg.default_values, {});
  const typeMap = parseJsonObjectSafe(cfg.typeMap ?? cfg.type_map, {});

  const keys = [];
  selectFields.forEach((sourcePath) => {
    const leaf = lastSegment(sourcePath);
    const targetKey = String(renameMap[sourcePath] || renameMap[leaf] || leaf || '').trim();
    if (targetKey && !keys.includes(targetKey)) keys.push(targetKey);
  });
  Object.keys(defaultValues).forEach((key) => {
    const normalized = String(key || '').trim();
    if (normalized && !keys.includes(normalized)) keys.push(normalized);
  });
  if (!keys.length) {
    Object.values(renameMap).forEach((value) => {
      const normalized = String(value || '').trim();
      if (normalized && !keys.includes(normalized)) keys.push(normalized);
    });
  }
  if (!keys.length) {
    return {
      rows: [],
      columns: [],
      message: 'У выбранного шаблона ноды ещё не настроен выходной контракт или явный набор полей. Сначала сохрани, какие поля эта нода отдаёт дальше.'
    };
  }

  const row = {};
  keys.forEach((key) => {
    const byDefault = defaultValues[key];
    const sourcePath = selectFields.find((field) => {
      const leaf = lastSegment(field);
      return String(renameMap[field] || renameMap[leaf] || leaf || '').trim() === key;
    });
    const sourceLeaf = lastSegment(sourcePath || '');
    const valueType = typeMap[key] || typeMap[sourcePath || ''] || typeMap[sourceLeaf] || '';
    row[key] = byDefault !== undefined ? byDefault : sampleValueFromType(valueType, key);
  });
  return {
    rows: [row],
    columns: Object.keys(row),
    message: 'Preview входа построен из настроек результата выбранного шаблона обработки данных.'
  };
}

export function buildSourceNodePreviewFromTemplate(template) {
  const src = template && typeof template === 'object' ? template : {};
  const templateType = String(src.templateType || src.template_type || '').trim();
  if (!src || !templateType) {
    return {
      rows: [],
      columns: [],
      message: 'Выбери шаблон ноды-источника, чтобы увидеть ожидаемый вход.'
    };
  }
  if (templateType === 'api_request') return buildPreviewFromApiTemplate(src);
  if (templateType === 'table_parser') return buildPreviewFromParserTemplate(src);
  return {
    rows: [],
    columns: [],
    message: 'Для выбранного типа ноды preview входа пока не поддержан.'
  };
}
