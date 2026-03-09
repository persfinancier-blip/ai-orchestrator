const EPSILON = 1e-6;

const INPUT_HYPOTHESES = [
  {
    code: 'working_set',
    name_ru: 'Основной рабочий набор записей',
    prior: 0.42,
    likelihoods: {
      candidate_kind: { array_objects: 0.86, array_scalars: 0.08, object: 0.26, string: 0.05, scalar: 0.03 },
      homogeneous_schema: { yes: 0.78, no: 0.22 },
      business_field_ratio: { high: 0.72, medium: 0.22, low: 0.06 },
      wrapper_field_ratio: { high: 0.08, medium: 0.18, low: 0.74 },
      container_hint: { strong: 0.58, weak: 0.28, none: 0.14 },
      sample_size: { zero: 0.02, one: 0.08, small: 0.18, medium: 0.3, large: 0.42 },
      fill_ratio: { high: 0.58, medium: 0.3, low: 0.12 },
      string_json: { yes: 0.07, no: 0.93 },
      string_html: { yes: 0.01, no: 0.99 }
    }
  },
  {
    code: 'wrapper',
    name_ru: 'Служебная обёртка, а не рабочий набор',
    prior: 0.2,
    likelihoods: {
      candidate_kind: { array_objects: 0.16, array_scalars: 0.04, object: 0.68, string: 0.06, scalar: 0.06 },
      homogeneous_schema: { yes: 0.32, no: 0.68 },
      business_field_ratio: { high: 0.12, medium: 0.2, low: 0.68 },
      wrapper_field_ratio: { high: 0.82, medium: 0.12, low: 0.06 },
      container_hint: { strong: 0.12, weak: 0.25, none: 0.63 },
      sample_size: { zero: 0.05, one: 0.28, small: 0.4, medium: 0.18, large: 0.09 },
      fill_ratio: { high: 0.28, medium: 0.44, low: 0.28 },
      string_json: { yes: 0.04, no: 0.96 },
      string_html: { yes: 0.01, no: 0.99 }
    }
  },
  {
    code: 'json_string',
    name_ru: 'JSON-строка с полезным объектом внутри',
    prior: 0.18,
    likelihoods: {
      candidate_kind: { array_objects: 0.04, array_scalars: 0.03, object: 0.05, string: 0.84, scalar: 0.04 },
      homogeneous_schema: { yes: 0.4, no: 0.6 },
      business_field_ratio: { high: 0.22, medium: 0.3, low: 0.48 },
      wrapper_field_ratio: { high: 0.18, medium: 0.22, low: 0.6 },
      container_hint: { strong: 0.14, weak: 0.2, none: 0.66 },
      sample_size: { zero: 0.18, one: 0.34, small: 0.22, medium: 0.16, large: 0.1 },
      fill_ratio: { high: 0.2, medium: 0.36, low: 0.44 },
      string_json: { yes: 0.91, no: 0.09 },
      string_html: { yes: 0.03, no: 0.97 }
    }
  },
  {
    code: 'html_error',
    name_ru: 'HTML или текстовая ошибка вместо данных',
    prior: 0.08,
    likelihoods: {
      candidate_kind: { array_objects: 0.02, array_scalars: 0.02, object: 0.03, string: 0.88, scalar: 0.05 },
      homogeneous_schema: { yes: 0.5, no: 0.5 },
      business_field_ratio: { high: 0.02, medium: 0.08, low: 0.9 },
      wrapper_field_ratio: { high: 0.14, medium: 0.2, low: 0.66 },
      container_hint: { strong: 0.04, weak: 0.08, none: 0.88 },
      sample_size: { zero: 0.24, one: 0.42, small: 0.18, medium: 0.1, large: 0.06 },
      fill_ratio: { high: 0.18, medium: 0.24, low: 0.58 },
      string_json: { yes: 0.03, no: 0.97 },
      string_html: { yes: 0.94, no: 0.06 }
    }
  },
  {
    code: 'auxiliary_value',
    name_ru: 'Вспомогательное значение, а не основной набор',
    prior: 0.12,
    likelihoods: {
      candidate_kind: { array_objects: 0.14, array_scalars: 0.28, object: 0.28, string: 0.18, scalar: 0.12 },
      homogeneous_schema: { yes: 0.46, no: 0.54 },
      business_field_ratio: { high: 0.22, medium: 0.3, low: 0.48 },
      wrapper_field_ratio: { high: 0.18, medium: 0.28, low: 0.54 },
      container_hint: { strong: 0.14, weak: 0.22, none: 0.64 },
      sample_size: { zero: 0.14, one: 0.32, small: 0.28, medium: 0.16, large: 0.1 },
      fill_ratio: { high: 0.28, medium: 0.4, low: 0.32 },
      string_json: { yes: 0.08, no: 0.92 },
      string_html: { yes: 0.04, no: 0.96 }
    }
  }
];

const JOIN_HYPOTHESES = [
  {
    code: 'good_join',
    name_ru: 'Пара полей подходит для связи',
    prior: 0.08,
    likelihoods: {
      name_similarity: { exact: 0.62, high: 0.24, medium: 0.1, low: 0.04 },
      type_compatibility: { exact: 0.56, compatible: 0.32, mismatch: 0.12 },
      source_fill: { high: 0.58, medium: 0.28, low: 0.14 },
      lookup_fill: { high: 0.6, medium: 0.27, low: 0.13 },
      source_uniqueness: { high: 0.46, medium: 0.34, low: 0.2 },
      lookup_uniqueness: { high: 0.62, medium: 0.26, low: 0.12 },
      overlap_ratio: { high: 0.64, medium: 0.24, low: 0.08, none: 0.04 },
      pattern_match: { yes: 0.72, no: 0.28 },
      duplicate_risk: { low: 0.78, high: 0.22 }
    }
  },
  {
    code: 'bad_join',
    name_ru: 'Пара полей плохо подходит для связи',
    prior: 0.92,
    likelihoods: {
      name_similarity: { exact: 0.06, high: 0.12, medium: 0.24, low: 0.58 },
      type_compatibility: { exact: 0.16, compatible: 0.28, mismatch: 0.56 },
      source_fill: { high: 0.26, medium: 0.32, low: 0.42 },
      lookup_fill: { high: 0.24, medium: 0.34, low: 0.42 },
      source_uniqueness: { high: 0.2, medium: 0.28, low: 0.52 },
      lookup_uniqueness: { high: 0.14, medium: 0.22, low: 0.64 },
      overlap_ratio: { high: 0.04, medium: 0.14, low: 0.28, none: 0.54 },
      pattern_match: { yes: 0.24, no: 0.76 },
      duplicate_risk: { low: 0.18, high: 0.82 }
    }
  }
];

const CONTAINER_HINTS = new Set([
  'list',
  'items',
  'rows',
  'results',
  'data',
  'products',
  'records',
  'entries',
  'offers',
  'campaigns'
]);

const WRAPPER_FIELDS = new Set([
  'success',
  'failed',
  'total_requests',
  'responses',
  'payloads_only',
  'preview',
  '__truncated',
  'status',
  'page',
  'execution_mode'
]);

const BUSINESS_FIELD_HINTS = [
  'id',
  'name',
  'title',
  'price',
  'status',
  'date',
  'created',
  'updated',
  'amount',
  'qty',
  'token',
  'sku',
  'campaign',
  'state',
  'brand',
  'subject'
];

function isObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function tryParseJsonString(value) {
  if (typeof value !== 'string') return { ok: false, value };
  const text = String(value || '').trim();
  if (!text) return { ok: false, value };
  try {
    return { ok: true, value: JSON.parse(text) };
  } catch {
    return { ok: false, value };
  }
}

function looksLikeHtml(value) {
  const text = String(value || '').trim().toLowerCase();
  if (!text) return false;
  return (
    text.startsWith('<!doctype html') ||
    text.startsWith('<html') ||
    text.startsWith('<head') ||
    text.startsWith('<body') ||
    text.startsWith('<div') ||
    text.startsWith('<script')
  );
}

function tokeniseName(value) {
  return String(value || '')
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .toLowerCase()
    .split(/[^a-z0-9а-яё]+/i)
    .map((item) => item.trim())
    .filter(Boolean);
}

function jaccardSimilarity(left, right) {
  const a = new Set(tokeniseName(left));
  const b = new Set(tokeniseName(right));
  if (!a.size && !b.size) return 0;
  let intersection = 0;
  for (const item of a) if (b.has(item)) intersection += 1;
  const union = new Set([...a, ...b]).size || 1;
  return intersection / union;
}

function toBucketByRatio(value, high = 0.7, medium = 0.35) {
  if (value >= high) return 'high';
  if (value >= medium) return 'medium';
  return 'low';
}

function countFilled(values) {
  return values.filter((value) => !(value === undefined || value === null || value === '')).length;
}

function classifyValuePattern(value) {
  const text = String(value ?? '').trim();
  if (!text) return 'empty';
  if (/^[0-9]+$/.test(text)) return 'integer';
  if (/^[0-9]+(?:\.[0-9]+)?$/.test(text)) return 'numeric';
  if (/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(text)) return 'uuid';
  if (/^\d{4}-\d{2}-\d{2}(?:[t ]\d{2}:\d{2}(?::\d{2})?)?/i.test(text)) return 'date';
  if (/^https?:\/\//i.test(text)) return 'url';
  if (/^[A-Za-z0-9._-]{12,}$/.test(text)) return 'token_like';
  return 'text';
}

function dominantPattern(values) {
  const counts = new Map();
  for (const value of values) {
    const pattern = classifyValuePattern(value);
    counts.set(pattern, (counts.get(pattern) || 0) + 1);
  }
  const sorted = [...counts.entries()].sort((a, b) => b[1] - a[1]);
  return sorted[0]?.[0] || 'empty';
}

function inferFieldStats(rows, field) {
  const values = (Array.isArray(rows) ? rows : []).map((row) => row?.[field]);
  const nonEmpty = values.filter((value) => !(value === undefined || value === null || value === ''));
  const textValues = nonEmpty.map((value) => String(value).trim()).filter(Boolean);
  const uniqueCount = new Set(textValues).size;
  const fillRatio = values.length ? nonEmpty.length / values.length : 0;
  const uniqueness = nonEmpty.length ? uniqueCount / nonEmpty.length : 0;
  return {
    field,
    fillRatio,
    uniqueness,
    pattern: dominantPattern(textValues),
    values: textValues
  };
}

function normalizePosteriorSet(hypotheses, observations) {
  const logScores = [];
  for (const hypothesis of hypotheses) {
    let score = Math.log(Math.max(hypothesis.prior, EPSILON));
    for (const [featureName, featureValue] of Object.entries(observations)) {
      const likelihoodSet = hypothesis.likelihoods?.[featureName] || {};
      const likelihood = likelihoodSet?.[featureValue] ?? EPSILON;
      score += Math.log(Math.max(likelihood, EPSILON));
    }
    logScores.push([hypothesis.code, score]);
  }
  const maxLog = Math.max(...logScores.map((item) => item[1]));
  const weighted = logScores.map(([code, score]) => [code, Math.exp(score - maxLog)]);
  const sum = weighted.reduce((acc, [, value]) => acc + value, 0) || 1;
  const result = {};
  for (const [code, value] of weighted) result[code] = value / sum;
  return result;
}

function sortPosterior(result) {
  return Object.entries(result)
    .map(([code, probability]) => ({ code, probability }))
    .sort((a, b) => b.probability - a.probability);
}

function buildInputCandidateObservations(candidate) {
  const value = candidate.value;
  const isArray = Array.isArray(value);
  const arrayObjects = isArray && value.every((item) => isObject(item));
  const sampleRows = arrayObjects ? value.slice(0, 20) : [];
  const keys = arrayObjects
    ? Array.from(new Set(sampleRows.flatMap((item) => Object.keys(item || {}))))
    : isObject(value)
    ? Object.keys(value)
    : [];
  const businessMatches = keys.filter((key) => BUSINESS_FIELD_HINTS.some((hint) => key.toLowerCase().includes(hint))).length;
  const wrapperMatches = keys.filter((key) => WRAPPER_FIELDS.has(String(key || '').trim())).length;
  const lastPathPart = String(candidate.path || '').split('.').filter(Boolean).slice(-1)[0] || '';
  const homogenous =
    arrayObjects && sampleRows.length > 1
      ? sampleRows.every((row) => JSON.stringify(Object.keys(row || {}).sort()) === JSON.stringify(Object.keys(sampleRows[0] || {}).sort()))
      : false;
  const fillRatio = arrayObjects
    ? (() => {
        if (!sampleRows.length || !keys.length) return 0;
        const totalCells = sampleRows.length * keys.length;
        const filled = sampleRows.reduce(
          (acc, row) => acc + keys.reduce((inner, key) => inner + (!(row?.[key] === undefined || row?.[key] === null || row?.[key] === '') ? 1 : 0), 0),
          0
        );
        return totalCells ? filled / totalCells : 0;
      })()
    : isObject(value)
    ? countFilled(Object.values(value || {})) / Math.max(Object.keys(value || {}).length, 1)
    : typeof value === 'string'
    ? (String(value || '').trim() ? 1 : 0)
    : 0;
  const size = isArray ? value.length : isObject(value) ? Object.keys(value).length : String(value ?? '').trim() ? 1 : 0;
  const containerHint = CONTAINER_HINTS.has(lastPathPart.toLowerCase()) ? 'strong' : lastPathPart ? 'weak' : 'none';
  return {
    candidate_kind: arrayObjects ? 'array_objects' : isArray ? 'array_scalars' : isObject(value) ? 'object' : typeof value === 'string' ? 'string' : 'scalar',
    homogeneous_schema: homogenous ? 'yes' : 'no',
    business_field_ratio: toBucketByRatio(keys.length ? businessMatches / keys.length : 0, 0.45, 0.15),
    wrapper_field_ratio: toBucketByRatio(keys.length ? wrapperMatches / keys.length : 0, 0.35, 0.15),
    container_hint: containerHint,
    sample_size: size === 0 ? 'zero' : size === 1 ? 'one' : size <= 5 ? 'small' : size <= 20 ? 'medium' : 'large',
    fill_ratio: toBucketByRatio(fillRatio, 0.75, 0.45),
    string_json: typeof value === 'string' && tryParseJsonString(value).ok ? 'yes' : 'no',
    string_html: typeof value === 'string' && looksLikeHtml(value) ? 'yes' : 'no'
  };
}

function extractCandidateNodes(value, path = '', depth = 0, out = [], maxDepth = 2) {
  out.push({ path, value });
  if (depth >= maxDepth) return out;
  if (Array.isArray(value)) {
    const firstObject = value.find((item) => isObject(item) || Array.isArray(item));
    if (firstObject !== undefined) extractCandidateNodes(firstObject, path ? `${path}[]` : '[]', depth + 1, out, maxDepth);
    return out;
  }
  if (!isObject(value)) return out;
  for (const [key, nested] of Object.entries(value)) {
    if (nested === undefined) continue;
    const nextPath = path ? `${path}.${key}` : key;
    if (Array.isArray(nested) || isObject(nested) || typeof nested === 'string') {
      extractCandidateNodes(nested, nextPath, depth + 1, out, maxDepth);
    }
  }
  return out;
}

function buildInputReasons(observations) {
  const reasons = [];
  if (observations.candidate_kind === 'array_objects') reasons.push('это массив объектов');
  if (observations.homogeneous_schema === 'yes') reasons.push('элементы массива однотипны');
  if (observations.business_field_ratio === 'high') reasons.push('в полях преобладают бизнес-признаки');
  if (observations.wrapper_field_ratio === 'high') reasons.push('в полях доминируют служебные признаки');
  if (observations.container_hint === 'strong') reasons.push('название узла похоже на контейнер данных');
  if (observations.string_json === 'yes') reasons.push('значение похоже на JSON-строку');
  if (observations.string_html === 'yes') reasons.push('значение похоже на HTML-ответ');
  return reasons;
}

function buildInputRecommendation(candidates) {
  const sorted = [...candidates].sort((a, b) => (b.posterior?.working_set || 0) - (a.posterior?.working_set || 0));
  const recommended = sorted[0] || null;
  const alternatives = sorted.slice(1, 4);
  return { recommended, alternatives };
}

export function analyzeInputCandidatesByBayes(value) {
  const candidates = extractCandidateNodes(value).map((candidate) => {
    const observations = buildInputCandidateObservations(candidate);
    const posterior = normalizePosteriorSet(INPUT_HYPOTHESES, observations);
    return {
      path: candidate.path,
      path_label: candidate.path || '(корень)',
      observations,
      posterior,
      top_hypothesis: sortPosterior(posterior)[0] || null,
      reasons: buildInputReasons(observations),
      sample_kind: observations.candidate_kind
    };
  });
  const recommendation = buildInputRecommendation(candidates);
  return {
    model_code: 'naive_bayes',
    model_name_ru: 'Наивная байесовская модель разбора входа',
    assumptions: ['Признаки считаются условно независимыми при фиксированной гипотезе.'],
    hypotheses: INPUT_HYPOTHESES.map(({ code, name_ru, prior }) => ({ code, name_ru, prior })),
    recommended_candidate: recommendation.recommended
      ? {
          path: recommendation.recommended.path,
          path_label: recommendation.recommended.path_label,
          probability: recommendation.recommended.posterior?.working_set || 0,
          top_hypothesis: recommendation.recommended.top_hypothesis,
          reasons: recommendation.recommended.reasons
        }
      : null,
    alternatives: recommendation.alternatives.map((item) => ({
      path: item.path,
      path_label: item.path_label,
      probability: item.posterior?.working_set || 0,
      top_hypothesis: item.top_hypothesis,
      reasons: item.reasons
    })),
    candidates
  };
}

function fieldNameSimilarity(left, right) {
  const normLeft = tokeniseName(left).join('_');
  const normRight = tokeniseName(right).join('_');
  if (normLeft && normLeft === normRight) return 'exact';
  const score = jaccardSimilarity(left, right);
  if (score >= 0.75) return 'high';
  if (score >= 0.4) return 'medium';
  return 'low';
}

function fieldTypeCompatibility(leftPattern, rightPattern) {
  if (leftPattern === rightPattern) return 'exact';
  const numeric = new Set(['integer', 'numeric']);
  if (numeric.has(leftPattern) && numeric.has(rightPattern)) return 'compatible';
  const idLike = new Set(['integer', 'uuid', 'token_like', 'text']);
  if (idLike.has(leftPattern) && idLike.has(rightPattern)) return 'compatible';
  if (leftPattern === 'empty' || rightPattern === 'empty') return 'compatible';
  return 'mismatch';
}

function overlapBucket(sourceValues, lookupValues) {
  if (!sourceValues.length || !lookupValues.length) return 'none';
  const left = new Set(sourceValues.map((value) => String(value)));
  const right = new Set(lookupValues.map((value) => String(value)));
  let intersection = 0;
  for (const value of left) if (right.has(value)) intersection += 1;
  const ratio = intersection / Math.max(Math.min(left.size, right.size), 1);
  if (ratio >= 0.6) return 'high';
  if (ratio >= 0.3) return 'medium';
  if (ratio > 0) return 'low';
  return 'none';
}

function duplicateRiskBucket(sourceStats, lookupStats) {
  const sourceUnique = sourceStats.uniqueness >= 0.9;
  const lookupUnique = lookupStats.uniqueness >= 0.9;
  return sourceUnique && lookupUnique ? 'low' : 'high';
}

function buildJoinReasons(observations) {
  const reasons = [];
  if (observations.name_similarity === 'exact') reasons.push('названия полей совпадают');
  else if (observations.name_similarity === 'high') reasons.push('названия полей очень похожи');
  if (observations.type_compatibility === 'exact') reasons.push('типы значений совпадают');
  else if (observations.type_compatibility === 'compatible') reasons.push('типы значений совместимы');
  if (observations.overlap_ratio === 'high') reasons.push('на sample высокая доля совпадающих значений');
  else if (observations.overlap_ratio === 'medium') reasons.push('на sample есть заметное пересечение значений');
  if (observations.lookup_uniqueness === 'high') reasons.push('поле таблицы похоже на устойчивый ключ');
  if (observations.duplicate_risk === 'high') reasons.push('есть риск дублей при объединении');
  return reasons;
}

export function analyzeJoinPairsByBayes(sourceRows, lookupRows) {
  const sourceColumns = Array.from(new Set((Array.isArray(sourceRows) ? sourceRows : []).flatMap((row) => Object.keys(row || {}))));
  const lookupColumns = Array.from(new Set((Array.isArray(lookupRows) ? lookupRows : []).flatMap((row) => Object.keys(row || {}))));
  const sourceStats = Object.fromEntries(sourceColumns.map((field) => [field, inferFieldStats(sourceRows, field)]));
  const lookupStats = Object.fromEntries(lookupColumns.map((field) => [field, inferFieldStats(lookupRows, field)]));
  const suggestions = [];
  for (const sourceField of sourceColumns) {
    for (const targetField of lookupColumns) {
      const left = sourceStats[sourceField];
      const right = lookupStats[targetField];
      const observations = {
        name_similarity: fieldNameSimilarity(sourceField, targetField),
        type_compatibility: fieldTypeCompatibility(left.pattern, right.pattern),
        source_fill: toBucketByRatio(left.fillRatio, 0.8, 0.4),
        lookup_fill: toBucketByRatio(right.fillRatio, 0.8, 0.4),
        source_uniqueness: toBucketByRatio(left.uniqueness, 0.9, 0.5),
        lookup_uniqueness: toBucketByRatio(right.uniqueness, 0.9, 0.5),
        overlap_ratio: overlapBucket(left.values, right.values),
        pattern_match: left.pattern === right.pattern ? 'yes' : 'no',
        duplicate_risk: duplicateRiskBucket(left, right)
      };
      const posterior = normalizePosteriorSet(JOIN_HYPOTHESES, observations);
      suggestions.push({
        sourceField,
        targetField,
        posterior,
        probability: posterior.good_join || 0,
        observations,
        reasons: buildJoinReasons(observations)
      });
    }
  }
  suggestions.sort((a, b) => b.probability - a.probability);
  const usedSource = new Set();
  const usedTarget = new Set();
  const recommendedRules = [];
  for (const suggestion of suggestions) {
    if (suggestion.probability < 0.55) continue;
    if (usedSource.has(suggestion.sourceField) || usedTarget.has(suggestion.targetField)) continue;
    recommendedRules.push({
      sourceField: suggestion.sourceField,
      targetField: suggestion.targetField,
      probability: suggestion.probability
    });
    usedSource.add(suggestion.sourceField);
    usedTarget.add(suggestion.targetField);
    if (recommendedRules.length >= 3) break;
  }
  return {
    model_code: 'naive_bayes',
    model_name_ru: 'Наивная байесовская модель подбора полей связи',
    assumptions: ['Признаки считаются условно независимыми при фиксированной гипотезе.'],
    hypotheses: JOIN_HYPOTHESES.map(({ code, name_ru, prior }) => ({ code, name_ru, prior })),
    suggestions: suggestions.slice(0, 12),
    recommended_rules: recommendedRules,
    warnings:
      suggestions.length && suggestions[0].probability < 0.55
        ? ['Уверенность модели в качестве связи низкая. Проверь поля вручную.']
        : [],
    stats: {
      source_fields: sourceColumns.length,
      lookup_fields: lookupColumns.length,
      compared_pairs: sourceColumns.length * lookupColumns.length
    }
  };
}
