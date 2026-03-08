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

function isRuntimeWrapper(value) {
  return (
    isObject(value) &&
    ('payloads_only' in value ||
      'responses' in value ||
      'total_requests' in value ||
      'success' in value ||
      'failed' in value ||
      'responses_dropped' in value ||
      'stop_reasons' in value)
  );
}

function isStructuredJson(value) {
  return Array.isArray(value) || isObject(value);
}

function extractFromIterable(list) {
  const items = Array.isArray(list) ? list : [];
  for (const item of items) {
    const value =
      isObject(item) && Object.prototype.hasOwnProperty.call(item, 'response')
        ? extractApiBusinessPayload(item.response)
        : extractApiBusinessPayload(item);
    if (value !== undefined) return value;
  }
  return undefined;
}

export function extractApiBusinessPayload(value) {
  if (value === undefined) return undefined;
  if (typeof value === 'string') {
    const parsed = tryParseJsonString(value);
    if (parsed.ok) return extractApiBusinessPayload(parsed.value);
    return value;
  }
  if (Array.isArray(value)) return value;
  if (!isObject(value)) return value;

  if (value.__truncated && typeof value.preview === 'string') {
    const parsedPreview = tryParseJsonString(value.preview);
    if (parsedPreview.ok) return extractApiBusinessPayload(parsedPreview.value);
    return undefined;
  }

  if (Array.isArray(value.payloads_only)) {
    const payload = extractFromIterable(value.payloads_only);
    if (payload !== undefined) return payload;
  }

  if (Array.isArray(value.responses)) {
    const payload = extractFromIterable(value.responses);
    if (payload !== undefined) return payload;
  }

  if (Object.prototype.hasOwnProperty.call(value, 'response')) {
    const payload = extractApiBusinessPayload(value.response);
    if (payload !== undefined) return payload;
  }

  if (isRuntimeWrapper(value)) return undefined;
  return value;
}

function stringifyPreviewValue(value) {
  if (value === undefined || value === null) return '';
  if (typeof value === 'string') {
    const parsed = tryParseJsonString(value);
    if (parsed.ok) return JSON.stringify(parsed.value, null, 2);
    return value;
  }
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

export function buildApiResponsePreviewState(primaryValue, fallbackValue = undefined) {
  const payload = extractApiBusinessPayload(primaryValue);
  const previewSource = payload !== undefined ? payload : fallbackValue;
  return {
    payload,
    treePayload: isStructuredJson(payload) ? payload : null,
    isJson: isStructuredJson(payload),
    text: stringifyPreviewValue(previewSource)
  };
}
