function trim(value) {
  return String(value ?? '').trim();
}

function uniqueStrings(values) {
  return Array.from(
    new Set(
      (Array.isArray(values) ? values : [])
        .map((item) => trim(item))
        .filter(Boolean)
    )
  );
}

function normalizePath(value) {
  return trim(value).replace(/\[(\w+)\]/g, '.$1').replace(/^\.+/, '').replace(/\.+/g, '.');
}

function parsePathParts(path) {
  const normalized = normalizePath(path);
  if (!normalized) return [];
  return normalized
    .split('.')
    .map((part) => trim(part))
    .filter(Boolean)
    .map((part) => (/^\d+$/.test(part) ? Number(part) : part));
}

function hasOwn(source, key) {
  return Boolean(source) && Object.prototype.hasOwnProperty.call(source, key);
}

function getByPath(source, path) {
  const parts = parsePathParts(path);
  if (!parts.length) return { found: false, value: undefined };
  let current = source;
  for (const part of parts) {
    if (current == null) return { found: false, value: undefined };
    if (typeof part === 'number') {
      if (!Array.isArray(current) || part >= current.length) return { found: false, value: undefined };
      current = current[part];
      continue;
    }
    if (!hasOwn(current, part)) return { found: false, value: undefined };
    current = current[part];
  }
  return { found: true, value: current };
}

function parserLeaf(path) {
  const normalized = normalizePath(path);
  return normalized
    .split('.')
    .map((part) => trim(part))
    .filter(Boolean)
    .slice(-1)[0] || normalized;
}

function normalizePreviewRows(rows) {
  return (Array.isArray(rows) ? rows : []).filter(
    (row) => row && typeof row === 'object' && !Array.isArray(row)
  );
}

function normalizePreviewSource(source) {
  const safe = source && typeof source === 'object' ? source : {};
  const key = trim(safe.key) || 'missing';
  const label = trim(safe.label) || 'Источник входа для preview не найден';
  const description =
    trim(safe.description) ||
    'Для запуска предпросмотра нужен last runtime input, upstream sample или manual sample.';
  return {
    key,
    label,
    description,
    available: Boolean(safe.available) && key !== 'missing',
    value: safe.value
  };
}

function normalizeStructureFields(fields) {
  return (Array.isArray(fields) ? fields : [])
    .map((field) => {
      const name = trim(field?.alias || field?.name || field?.path);
      if (!name) return null;
      return {
        name,
        alias: trim(field?.alias || field?.name || field?.path),
        type: trim(field?.type),
        path: normalizePath(field?.path),
        origin: trim(field?.origin)
      };
    })
    .filter(Boolean);
}

export function parserResultPreviewColumnsFromRows(rows) {
  return uniqueStrings(
    normalizePreviewRows(rows).flatMap((row) => Object.keys(row || {}))
  );
}

export function parserRuntimeRowsFromValue(value) {
  const src = value && typeof value === 'object' ? value : null;
  if (src && trim(src.contract_version) === 'node_io_v1' && Array.isArray(src.rows)) {
    return src.rows
      .map((row) => {
        if (row && typeof row === 'object' && !Array.isArray(row)) return row;
        return { value: row };
      })
      .filter(Boolean);
  }
  if (Array.isArray(value)) {
    return value
      .map((row) => {
        if (row && typeof row === 'object' && !Array.isArray(row)) return row;
        return { value: row };
      })
      .filter(Boolean);
  }
  if (src && !Array.isArray(src)) {
    const keys = Object.keys(src);
    if ((src.error !== undefined && keys.length <= 2) || src.end === true) return [];
    return [src];
  }
  return [];
}

export function buildParserPreviewDataFromRuntimeValue(runtimeValue = null, options = {}) {
  const runtimeObject = runtimeValue && typeof runtimeValue === 'object' ? runtimeValue : null;
  if (!runtimeObject) return null;

  const rows = parserRuntimeRowsFromValue(runtimeObject);
  const requestedLimit = Math.trunc(Number(options?.previewLimit || options?.limit || 20));
  const previewLimit = Number.isFinite(requestedLimit) && requestedLimit > 0 ? requestedLimit : 20;
  const sampleRows = rows.slice(0, previewLimit);
  const columns = uniqueStrings([
    ...parserResultPreviewColumnsFromRows(rows),
    ...parserResultPreviewColumnsFromRows(sampleRows)
  ]);
  const parserBatch =
    runtimeObject?.meta && typeof runtimeObject.meta === 'object' && typeof runtimeObject.meta.parser_batch === 'object'
      ? runtimeObject.meta.parser_batch
      : null;
  const returnedRows = sampleRows.length;
  const rowCount = Math.max(0, Number(runtimeObject?.row_count || rows.length) || 0);
  const batch = parserBatch
    ? {
        ...parserBatch,
        returned_rows: Math.max(0, Number(parserBatch.returned_rows || returnedRows) || returnedRows),
        batch_size: Math.max(1, Number(parserBatch.batch_size || previewLimit) || previewLimit),
        has_more:
          typeof parserBatch.has_more === 'boolean'
            ? parserBatch.has_more
            : rowCount > Math.max(0, Number(parserBatch.returned_rows || returnedRows) || returnedRows)
      }
    : {
        offset: 0,
        batch_size: previewLimit,
        returned_rows: returnedRows,
        has_more: rowCount > returnedRows,
        next_cursor: null
      };
  const metrics = options?.metrics && typeof options.metrics === 'object' ? options.metrics : {};
  const meta = runtimeObject?.meta && typeof runtimeObject.meta === 'object' ? runtimeObject.meta : {};

  return {
    row_count: rowCount,
    column_count: columns.length,
    columns,
    sample_rows: sampleRows,
    raw_row_count: rowCount,
    raw_column_count: columns.length,
    raw_columns: columns,
    raw_sample_rows: sampleRows,
    source_type: trim(options?.sourceType || meta?.source_type),
    source_ref: trim(options?.sourceRef || meta?.source_ref),
    source_format: trim(options?.sourceFormat || meta?.source_format),
    batch,
    stats: metrics
  };
}

export function buildParserPreviewDataFromRuntimeStep(runtimeStep = null, options = {}) {
  const outputValue = runtimeStep && typeof runtimeStep === 'object' ? runtimeStep.output_json : null;
  const metrics = runtimeStep?.metrics_json && typeof runtimeStep.metrics_json === 'object' ? runtimeStep.metrics_json : {};
  return buildParserPreviewDataFromRuntimeValue(outputValue, {
    ...options,
    metrics
  });
}

function readValueFromRow(row, candidates = []) {
  const source = row && typeof row === 'object' && !Array.isArray(row) ? row : null;
  if (!source) return { found: false, value: undefined, candidate: '' };
  for (const rawCandidate of Array.isArray(candidates) ? candidates : []) {
    const candidate = trim(rawCandidate);
    if (!candidate) continue;
    if (hasOwn(source, candidate)) {
      return { found: true, value: source[candidate], candidate };
    }
    const normalizedCandidate = normalizePath(candidate);
    if (normalizedCandidate && normalizedCandidate !== candidate && hasOwn(source, normalizedCandidate)) {
      return { found: true, value: source[normalizedCandidate], candidate: normalizedCandidate };
    }
    if (!normalizedCandidate) continue;
    const nested = getByPath(source, normalizedCandidate);
    if (nested.found) return { found: true, value: nested.value, candidate: normalizedCandidate };
  }
  return { found: false, value: undefined, candidate: '' };
}

function buildPreviewGridFromColumns(rows = [], columns = []) {
  const safeRows = normalizePreviewRows(rows);
  const safeColumns = uniqueStrings(columns);
  const mappedCounts = new Map(safeColumns.map((column) => [column, 0]));
  const preparedRows = safeRows.map((row) => {
    const prepared = {};
    safeColumns.forEach((column) => {
      const resolved = readValueFromRow(row, [column]);
      prepared[column] = resolved.found ? resolved.value : '';
      if (resolved.found) mappedCounts.set(column, (mappedCounts.get(column) || 0) + 1);
    });
    return prepared;
  });
  return {
    rows: preparedRows,
    columns: safeColumns,
    columnsWithoutMappedValues: safeColumns.filter((column) => (mappedCounts.get(column) || 0) === 0),
    firstPreparedRowKeys: Object.keys(preparedRows[0] || {})
  };
}

function buildPreviewGridFromStructure(rows = [], structureFields = []) {
  const safeRows = normalizePreviewRows(rows);
  const safeFields = Array.isArray(structureFields) ? structureFields : [];
  const columns = uniqueStrings(safeFields.map((field) => field.name));
  const mappedCounts = new Map(columns.map((column) => [column, 0]));
  const preparedRows = safeRows.map((row) => {
    const prepared = {};
    safeFields.forEach((field) => {
      const candidates = uniqueStrings([
        field?.name,
        field?.alias,
        field?.path,
        parserLeaf(field?.path)
      ]);
      const resolved = readValueFromRow(row, candidates);
      prepared[field.name] = resolved.found ? resolved.value : '';
      if (resolved.found) mappedCounts.set(field.name, (mappedCounts.get(field.name) || 0) + 1);
    });
    return prepared;
  });
  return {
    rows: preparedRows,
    columns,
    columnsWithoutMappedValues: columns.filter((column) => (mappedCounts.get(column) || 0) === 0),
    firstPreparedRowKeys: Object.keys(preparedRows[0] || {})
  };
}

function isPreviewGridRenderable(grid = null) {
  return Boolean(
    grid &&
      Array.isArray(grid.rows) &&
      grid.rows.length > 0 &&
      Array.isArray(grid.columns) &&
      grid.columns.length > 0 &&
      Array.isArray(grid.columnsWithoutMappedValues) &&
      grid.columnsWithoutMappedValues.length < grid.columns.length
  );
}

function chooseEffectiveInputSource({
  currentInputSource = null,
  lastResolvedInputSource = null,
  preferLastResolved = false
} = {}) {
  const currentSource = normalizePreviewSource(currentInputSource);
  const lastSource = normalizePreviewSource(lastResolvedInputSource);
  if (preferLastResolved && (lastSource.available || lastSource.key !== 'missing')) return lastSource;
  return currentSource.key ? currentSource : lastSource;
}

function createPreviewDebugState({
  effectiveInputSource,
  structureColumns = [],
  previewRowCount = 0,
  preparedRowsCount = 0,
  firstPreparedRowKeys = [],
  columnsWithoutMappedValues = [],
  stalePreview = false,
  previewError = '',
  previewSuccess = false,
  gridColumns = [],
  rawPreviewColumns = []
} = {}) {
  return {
    resolvedInputSourceKey: trim(effectiveInputSource?.key),
    resolvedInputSourceLabel: trim(effectiveInputSource?.label),
    effectivePublishColumns: uniqueStrings(structureColumns),
    previewResponseRowCount: Math.max(0, Number(previewRowCount) || 0),
    preparedGridRowsCount: Math.max(0, Number(preparedRowsCount) || 0),
    firstPreparedRowKeys: uniqueStrings(firstPreparedRowKeys),
    columnsWithoutMappedValues: uniqueStrings(columnsWithoutMappedValues),
    stalePreview: Boolean(stalePreview),
    previewError: trim(previewError),
    previewSuccess: Boolean(previewSuccess),
    sourceResolved: Boolean(effectiveInputSource?.available),
    gridColumns: uniqueStrings(gridColumns),
    rawPreviewColumns: uniqueStrings(rawPreviewColumns)
  };
}

function createPreviewState(base = {}) {
  return {
    mode: base.mode || 'no_preview_yet',
    modeLabel: base.modeLabel || '',
    statusTone: base.statusTone || 'info',
    statusTitle: base.statusTitle || '',
    statusDescription: base.statusDescription || '',
    rows: Array.isArray(base.rows) ? base.rows : [],
    columns: uniqueStrings(base.columns),
    structureFields: Array.isArray(base.structureFields) ? base.structureFields : [],
    showStructure: Boolean(base.showStructure),
    structureColumns: uniqueStrings(base.structureColumns),
    liveRowsCount: Math.max(0, Number(base.liveRowsCount) || 0),
    liveColumnsCount: Math.max(0, Number(base.liveColumnsCount) || 0),
    responseRowCount: Math.max(0, Number(base.responseRowCount) || 0),
    preparedRowsCount: Math.max(0, Number(base.preparedRowsCount) || 0),
    sourceFormat: trim(base.sourceFormat),
    isStalePreview: Boolean(base.isStalePreview),
    effectiveInputSource: normalizePreviewSource(base.effectiveInputSource),
    debug: createPreviewDebugState(base.debug || {})
  };
}

export function buildParserRuntimeResultState({ runtimeStep = null, publishedDescriptorFields = [] } = {}) {
  const structureFields = normalizeStructureFields(publishedDescriptorFields);
  const structureColumns = uniqueStrings(structureFields.map((field) => field.name));
  const runtimeOutput = runtimeStep && typeof runtimeStep === 'object' ? runtimeStep.output_json : null;
  const rows = parserRuntimeRowsFromValue(runtimeOutput);
  const rowColumns = uniqueStrings([
    ...structureColumns,
    ...parserResultPreviewColumnsFromRows(rows)
  ]);
  const previousOutput =
    runtimeStep?.previous_step && typeof runtimeStep.previous_step === 'object' ? runtimeStep.previous_step.output_json : undefined;
  const handoffMatches =
    previousOutput === undefined
      ? null
      : (() => {
          try {
            return JSON.stringify(previousOutput ?? null) === JSON.stringify(runtimeStep?.input_json ?? null);
          } catch {
            return false;
          }
        })();
  const previousNodeLabel = trim(runtimeStep?.previous_step?.node_name || runtimeStep?.previous_step?.node_id);

  if (!runtimeStep || typeof runtimeStep !== 'object') {
    return {
      mode: 'no_data',
      modeLabel: 'Last runtime не найден',
      statusTone: 'info',
      statusTitle: 'Для этой ноды ещё нет сохранённого runtime результата',
      statusDescription:
        'После server run здесь можно будет отдельно увидеть последний canonical output этой ноды и сравнить его с draft preview ниже.',
      rows: [],
      columns: [],
      structureFields,
      showStructure: false,
      rowCount: 0,
      handoffMatchesUpstream: null,
      previousNodeLabel: '',
      runUid: '',
      runStatus: ''
    };
  }

  if (rows.length) {
    return {
      mode: 'rows',
      modeLabel: 'Last runtime rows',
      statusTone: 'ok',
      statusTitle: 'Показан последний сохранённый runtime результат ноды',
      statusDescription:
        handoffMatches === true
          ? `Canonical handoff с предыдущей нодой${previousNodeLabel ? ` (${previousNodeLabel})` : ''} совпадает.`
          : handoffMatches === false
          ? `Canonical input этой ноды отличается от сохранённого output предыдущего шага${previousNodeLabel ? ` (${previousNodeLabel})` : ''}.`
          : 'Показаны строки последнего server run для этой ноды.',
      rows,
      columns: rowColumns,
      structureFields,
      showStructure: false,
      rowCount: Math.max(0, Number(runtimeOutput?.row_count || rows.length) || 0),
      handoffMatchesUpstream: handoffMatches,
      previousNodeLabel,
      runUid: trim(runtimeStep?.run_uid),
      runStatus: trim(runtimeStep?.run_status)
    };
  }

  if (structureFields.length) {
    const runtimeFailed = trim(runtimeStep?.status).toLowerCase() === 'error';
    return {
      mode: 'shape_only',
      modeLabel: 'Last runtime без строк',
      statusTone: runtimeFailed ? 'error' : 'warn',
      statusTitle: runtimeFailed
        ? 'Последний runtime этой ноды завершился ошибкой'
        : 'Последний runtime не сохранил строк результата',
      statusDescription: runtimeFailed
        ? 'Строки runtime результата недоступны, поэтому ниже остаётся только текущая publish-структура.'
        : 'Ниже показана publish-структура текущей ноды. Это не live rows, а shape последнего известного результата.',
      rows: [],
      columns: [],
      structureFields,
      showStructure: true,
      structureColumns,
      rowCount: 0,
      handoffMatchesUpstream: handoffMatches,
      previousNodeLabel,
      runUid: trim(runtimeStep?.run_uid),
      runStatus: trim(runtimeStep?.run_status)
    };
  }

  return {
    mode: 'no_data',
    modeLabel: 'Last runtime без данных',
    statusTone: trim(runtimeStep?.status).toLowerCase() === 'error' ? 'error' : 'info',
    statusTitle:
      trim(runtimeStep?.status).toLowerCase() === 'error'
        ? 'Последний runtime этой ноды завершился ошибкой'
        : 'Последний runtime не дал данных для отображения',
    statusDescription:
      'Для этой ноды нет ни сохранённых строк результата, ни publish-структуры для shape-only режима.',
    rows: [],
    columns: [],
    structureFields,
    showStructure: false,
    rowCount: 0,
    handoffMatchesUpstream: handoffMatches,
    previousNodeLabel,
    runUid: trim(runtimeStep?.run_uid),
    runStatus: trim(runtimeStep?.run_status)
  };
}

export function buildParserResultPreviewState({
  previewData = null,
  previewError = '',
  publishedDescriptorFields = [],
  currentConfigSignature = '',
  previewLastAttemptSignature = '',
  previewLastSuccessSignature = '',
  currentInputSource = null,
  lastResolvedInputSource = null
} = {}) {
  const structureFields = normalizeStructureFields(publishedDescriptorFields);
  const structureColumns = uniqueStrings(structureFields.map((field) => field.name));
  const previewRows = normalizePreviewRows(previewData?.sample_rows);
  const previewColumns = uniqueStrings([
    ...(Array.isArray(previewData?.columns) ? previewData.columns : []),
    ...parserResultPreviewColumnsFromRows(previewRows)
  ]);

  const currentSignature = trim(currentConfigSignature);
  const lastAttemptSignature = trim(previewLastAttemptSignature);
  const lastSuccessSignature = trim(previewLastSuccessSignature);
  const hasAnyAttempt = Boolean(lastAttemptSignature);
  const freshAttempt = Boolean(currentSignature) && lastAttemptSignature === currentSignature;
  const freshSuccess = Boolean(currentSignature) && lastSuccessSignature === currentSignature;
  const freshError = Boolean(trim(previewError)) && freshAttempt && !freshSuccess;
  const stalePreview = Boolean(lastSuccessSignature) && Boolean(currentSignature) && lastSuccessSignature !== currentSignature;
  const previewResponseRowCount = Math.max(0, Number(previewData?.row_count || previewRows.length) || 0);
  const effectiveInputSource = chooseEffectiveInputSource({
    currentInputSource,
    lastResolvedInputSource,
    preferLastResolved: freshAttempt || freshSuccess || stalePreview
  });

  const structuredGrid = buildPreviewGridFromStructure(previewRows, structureFields);
  const rawGrid = buildPreviewGridFromColumns(previewRows, previewColumns);
  const canRenderStructuredRows =
    structuredGrid.rows.length > 0 &&
    structuredGrid.columns.length > 0 &&
    structuredGrid.columnsWithoutMappedValues.length < structuredGrid.columns.length;
  const canRenderRawRows = rawGrid.rows.length > 0 && rawGrid.columns.length > 0;

  const previewRowsRequireMapping = freshSuccess && structureColumns.length > 0;
  const freshRowsRenderable = previewRowsRequireMapping ? canRenderStructuredRows : canRenderRawRows;
  const freshGrid = previewRowsRequireMapping && canRenderStructuredRows ? structuredGrid : rawGrid;
  const staleGrid = canRenderStructuredRows ? structuredGrid : rawGrid;
  const displayGrid = stalePreview ? staleGrid : freshGrid;

  if (freshSuccess && previewRows.length && effectiveInputSource.available && freshRowsRenderable) {
    return createPreviewState({
      mode: 'rows',
      modeLabel: 'Реальные строки результата',
      statusTone: 'ok',
      statusTitle: 'Показаны реальные строки результата parser',
      statusDescription:
        'Таблица ниже собрана из текущего runtime preview. Это живые строки результата, уже приведённые к publish-модели секции 3.',
      rows: displayGrid.rows,
      columns: displayGrid.columns,
      structureFields,
      showStructure: false,
      liveRowsCount: displayGrid.rows.length,
      liveColumnsCount: displayGrid.columns.length,
      responseRowCount: previewResponseRowCount,
      preparedRowsCount: displayGrid.rows.length,
      sourceFormat: trim(previewData?.source_format),
      isStalePreview: false,
      effectiveInputSource,
      debug: {
        effectiveInputSource,
        structureColumns,
        previewRowCount: previewResponseRowCount,
        preparedRowsCount: displayGrid.rows.length,
        firstPreparedRowKeys: displayGrid.firstPreparedRowKeys,
        columnsWithoutMappedValues: displayGrid.columnsWithoutMappedValues,
        stalePreview: false,
        previewError: '',
        previewSuccess: true,
        gridColumns: displayGrid.columns,
        rawPreviewColumns: previewColumns
      }
    });
  }

  if (freshError) {
    return createPreviewState({
      mode: 'preview_failed',
      modeLabel: 'Preview с ошибкой',
      statusTone: 'error',
      statusTitle: 'Preview результата завершился с ошибкой',
      statusDescription: trim(previewError),
      rows: [],
      columns: [],
      structureFields,
      showStructure: structureFields.length > 0,
      structureColumns,
      liveRowsCount: 0,
      liveColumnsCount: 0,
      responseRowCount: 0,
      preparedRowsCount: 0,
      sourceFormat: trim(previewData?.source_format),
      isStalePreview: false,
      effectiveInputSource,
      debug: {
        effectiveInputSource,
        structureColumns,
        previewRowCount: 0,
        preparedRowsCount: 0,
        firstPreparedRowKeys: [],
        columnsWithoutMappedValues: structureColumns,
        stalePreview: false,
        previewError,
        previewSuccess: false,
        gridColumns: [],
        rawPreviewColumns: previewColumns
      }
    });
  }

  if (freshSuccess && previewRows.length && !effectiveInputSource.available) {
    return createPreviewState({
      mode: 'preview_failed',
      modeLabel: 'Preview без источника',
      statusTone: 'error',
      statusTitle: 'Не удалось подтвердить источник входа для preview',
      statusDescription:
        'Preview вернул строки результата, но в UI не сохранилась информация о том, какой вход был реально использован. Таблица скрыта, чтобы не показывать противоречивое состояние.',
      rows: [],
      columns: [],
      structureFields,
      showStructure: structureFields.length > 0,
      structureColumns,
      liveRowsCount: 0,
      liveColumnsCount: 0,
      responseRowCount: previewResponseRowCount,
      preparedRowsCount: 0,
      sourceFormat: trim(previewData?.source_format),
      isStalePreview: false,
      effectiveInputSource,
      debug: {
        effectiveInputSource,
        structureColumns,
        previewRowCount: previewResponseRowCount,
        preparedRowsCount: 0,
        firstPreparedRowKeys: [],
        columnsWithoutMappedValues: structureColumns,
        stalePreview: false,
        previewError: 'preview_source_resolution_missing',
        previewSuccess: false,
        gridColumns: [],
        rawPreviewColumns: previewColumns
      }
    });
  }

  if (freshSuccess && previewRows.length && !freshRowsRenderable) {
    const mappingError = previewRowsRequireMapping
      ? 'Preview rows получены, но их ключи не удалось сопоставить с publish columns секции 3.'
      : 'Preview rows получены, но их не удалось подготовить к рендеру таблицы.';
    return createPreviewState({
      mode: 'preview_failed',
      modeLabel: 'Preview без готовой таблицы',
      statusTone: 'error',
      statusTitle: 'Не удалось подготовить preview rows для таблицы',
      statusDescription: mappingError,
      rows: [],
      columns: [],
      structureFields,
      showStructure: structureFields.length > 0,
      structureColumns,
      liveRowsCount: 0,
      liveColumnsCount: 0,
      responseRowCount: previewResponseRowCount,
      preparedRowsCount: 0,
      sourceFormat: trim(previewData?.source_format),
      isStalePreview: false,
      effectiveInputSource,
      debug: {
        effectiveInputSource,
        structureColumns,
        previewRowCount: previewResponseRowCount,
        preparedRowsCount: 0,
        firstPreparedRowKeys: [],
        columnsWithoutMappedValues: previewRowsRequireMapping ? structuredGrid.columnsWithoutMappedValues : rawGrid.columnsWithoutMappedValues,
        stalePreview: false,
        previewError: 'preview_grid_mapping_failed',
        previewSuccess: false,
        gridColumns: previewRowsRequireMapping ? structuredGrid.columns : rawGrid.columns,
        rawPreviewColumns: previewColumns
      }
    });
  }

  if (stalePreview && displayGrid.rows.length) {
    const usesCurrentPublishColumns = canRenderStructuredRows;
    return createPreviewState({
      mode: 'rows',
      modeLabel: 'Последний preview устарел',
      statusTone: 'warn',
      statusTitle: 'Настройки изменились, предпросмотр устарел',
      statusDescription: usesCurrentPublishColumns
        ? 'Таблица ниже показывает последний draft preview по старым settings, но её строки уже приведены к текущей publish-модели настолько, насколько это возможно.'
        : 'Таблица ниже показывает последний draft preview по старым settings с его исходными колонками. Чтобы увидеть актуальные строки результата, обнови предпросмотр заново.',
      rows: displayGrid.rows,
      columns: displayGrid.columns,
      structureFields,
      showStructure: false,
      liveRowsCount: displayGrid.rows.length,
      liveColumnsCount: displayGrid.columns.length,
      responseRowCount: previewResponseRowCount,
      preparedRowsCount: displayGrid.rows.length,
      sourceFormat: trim(previewData?.source_format),
      isStalePreview: true,
      effectiveInputSource,
      debug: {
        effectiveInputSource,
        structureColumns,
        previewRowCount: previewResponseRowCount,
        preparedRowsCount: displayGrid.rows.length,
        firstPreparedRowKeys: displayGrid.firstPreparedRowKeys,
        columnsWithoutMappedValues: displayGrid.columnsWithoutMappedValues,
        stalePreview: true,
        previewError: '',
        previewSuccess: false,
        gridColumns: displayGrid.columns,
        rawPreviewColumns: previewColumns
      }
    });
  }

  if (!hasAnyAttempt) {
    return createPreviewState({
      mode: 'no_preview_yet',
      modeLabel: 'Preview ещё не запускался',
      statusTone: 'info',
      statusTitle: 'Preview результата ещё не запускался',
      statusDescription: structureFields.length
        ? 'Живые строки результата ещё не запрашивались. Ниже можно посмотреть текущую структуру publish result, но это не реальные data rows.'
        : 'Пока нет ни живых строк preview, ни структуры publish result. Настрой секции 2-3 и нажми «Запустить предпросмотр результата».',
      rows: [],
      columns: [],
      structureFields,
      showStructure: structureFields.length > 0,
      structureColumns,
      liveRowsCount: 0,
      liveColumnsCount: 0,
      responseRowCount: 0,
      preparedRowsCount: 0,
      sourceFormat: trim(previewData?.source_format),
      isStalePreview: false,
      effectiveInputSource,
      debug: {
        effectiveInputSource,
        structureColumns,
        previewRowCount: 0,
        preparedRowsCount: 0,
        firstPreparedRowKeys: [],
        columnsWithoutMappedValues: structureColumns,
        stalePreview: false,
        previewError: '',
        previewSuccess: false,
        gridColumns: [],
        rawPreviewColumns: previewColumns
      }
    });
  }

  if (structureFields.length) {
    let statusDescription =
      'Живые preview rows для текущих settings недоступны. Ниже показана только структура publish result без строк.';
    if (stalePreview) {
      statusDescription =
        'После последнего preview настройки parser изменились. Живые строки ниже больше не считаются актуальными, поэтому показана только текущая структура результата.';
    } else if (freshSuccess && previewResponseRowCount === 0) {
      statusDescription =
        'Preview отработал на текущих settings, но не вернул строк результата. Ниже показана только структура publish result.';
    } else if (!effectiveInputSource.available) {
      statusDescription = effectiveInputSource.description;
    }

    return createPreviewState({
      mode: 'shape_only',
      modeLabel: 'Только структура результата',
      statusTone: stalePreview ? 'warn' : freshSuccess ? 'info' : 'warn',
      statusTitle: freshSuccess && previewResponseRowCount === 0 ? 'Preview не вернул строки результата' : 'Пока доступна только структура результата',
      statusDescription,
      rows: [],
      columns: [],
      structureFields,
      showStructure: true,
      structureColumns,
      liveRowsCount: 0,
      liveColumnsCount: structureColumns.length,
      responseRowCount: previewResponseRowCount,
      preparedRowsCount: 0,
      sourceFormat: trim(previewData?.source_format),
      isStalePreview: stalePreview,
      effectiveInputSource,
      debug: {
        effectiveInputSource,
        structureColumns,
        previewRowCount: previewResponseRowCount,
        preparedRowsCount: 0,
        firstPreparedRowKeys: [],
        columnsWithoutMappedValues: structureColumns,
        stalePreview,
        previewError,
        previewSuccess: false,
        gridColumns: [],
        rawPreviewColumns: previewColumns
      }
    });
  }

  return createPreviewState({
    mode: 'no_preview_yet',
    modeLabel: 'Preview ещё не запускался',
    statusTone: stalePreview ? 'warn' : 'info',
    statusTitle: stalePreview ? 'Старый preview больше не актуален' : 'Preview результата ещё не запускался',
    statusDescription: stalePreview
      ? 'После изменения настроек актуальный preview ещё не собран. Сначала обнови preview.'
      : 'Пока нет ни живых строк preview, ни структуры publish result. Настрой секции 2-3 и нажми «Запустить предпросмотр результата».',
    rows: [],
    columns: [],
    structureFields: [],
    showStructure: false,
    structureColumns: [],
    liveRowsCount: 0,
    liveColumnsCount: 0,
    responseRowCount: previewResponseRowCount,
    preparedRowsCount: 0,
    sourceFormat: trim(previewData?.source_format),
    isStalePreview: stalePreview,
    effectiveInputSource,
    debug: {
      effectiveInputSource,
      structureColumns: [],
      previewRowCount: previewResponseRowCount,
      preparedRowsCount: 0,
      firstPreparedRowKeys: [],
      columnsWithoutMappedValues: [],
      stalePreview,
      previewError,
      previewSuccess: false,
      gridColumns: [],
      rawPreviewColumns: previewColumns
    }
  });
}

export function buildParserFlowPreviewState({
  viewKind = 'result',
  previewData = null,
  previewError = '',
  publishedDescriptorFields = [],
  currentConfigSignature = '',
  previewLastAttemptSignature = '',
  previewLastSuccessSignature = '',
  currentInputSource = null,
  lastResolvedInputSource = null
} = {}) {
  const kind = trim(viewKind).toLowerCase();
  if (kind === 'result') {
    return buildParserResultPreviewState({
      previewData,
      previewError,
      publishedDescriptorFields,
      currentConfigSignature,
      previewLastAttemptSignature,
      previewLastSuccessSignature,
      currentInputSource,
      lastResolvedInputSource
    });
  }

  const structureFields = normalizeStructureFields(publishedDescriptorFields);
  const structureColumns = uniqueStrings(structureFields.map((field) => field.name));
  const previewRows = normalizePreviewRows(previewData?.sample_rows);
  const previewColumns = uniqueStrings([
    ...(Array.isArray(previewData?.columns) ? previewData.columns : []),
    ...parserResultPreviewColumnsFromRows(previewRows)
  ]);

  const currentSignature = trim(currentConfigSignature);
  const lastAttemptSignature = trim(previewLastAttemptSignature);
  const lastSuccessSignature = trim(previewLastSuccessSignature);
  const freshAttempt = Boolean(currentSignature) && lastAttemptSignature === currentSignature;
  const freshSuccess = Boolean(currentSignature) && lastSuccessSignature === currentSignature;
  const freshError = Boolean(trim(previewError)) && freshAttempt && !freshSuccess;
  const stalePreview = Boolean(lastSuccessSignature) && Boolean(currentSignature) && lastSuccessSignature !== currentSignature;
  const previewResponseRowCount = Math.max(0, Number(previewData?.row_count || previewRows.length) || 0);
  const effectiveInputSource = chooseEffectiveInputSource({
    currentInputSource,
    lastResolvedInputSource,
    preferLastResolved: freshAttempt || freshSuccess || stalePreview
  });

  const structureGrid = buildPreviewGridFromStructure(previewRows, structureFields);
  const directContractGrid = buildPreviewGridFromColumns(previewRows, structureColumns);
  const rawGrid = buildPreviewGridFromColumns(previewRows, previewColumns);
  const inputView = kind === 'input';
  const preferredFreshGrid = (() => {
    if (!structureColumns.length) return rawGrid;
    if (inputView) return structureGrid;
    if (isPreviewGridRenderable(directContractGrid)) return directContractGrid;
    if (isPreviewGridRenderable(structureGrid)) return structureGrid;
    return directContractGrid;
  })();
  const preferredStaleGrid = (() => {
    if (inputView) {
      if (isPreviewGridRenderable(structureGrid)) return structureGrid;
      return rawGrid;
    }
    if (isPreviewGridRenderable(directContractGrid)) return directContractGrid;
    if (isPreviewGridRenderable(structureGrid)) return structureGrid;
    return rawGrid;
  })();
  const freshRowsRenderable = isPreviewGridRenderable(preferredFreshGrid);
  const staleRowsRenderable = isPreviewGridRenderable(preferredStaleGrid);
  const mappingError = inputView
    ? 'Preview rows получены, но их ключи не удалось сопоставить с consume columns секции 1.'
    : 'Preview rows получены, но их ключи не удалось сопоставить с publish columns секции 3.';
  const staleDescription = inputView
    ? 'Ниже остаётся canonical input текущей parser node из последнего preview-run. Настройки parser изменились, поэтому для синхронной проверки входа и выхода preview нужно обновить.'
    : 'Ниже остаётся canonical output текущей parser node из последнего preview-run. Настройки parser изменились, поэтому для синхронной проверки входа и выхода preview нужно обновить.';

  if (freshSuccess && previewRows.length && effectiveInputSource.available && freshRowsRenderable) {
    return createPreviewState({
      mode: 'rows',
      modeLabel: inputView ? 'Preview входных строк' : 'Preview выходных строк',
      statusTone: 'ok',
      statusTitle: inputView ? 'Показаны реальные входные строки parser' : 'Показаны реальные выходные строки parser',
      statusDescription: inputView
        ? 'Таблица показывает canonical input текущей parser node из того же server preview-run, который используется для проверки выхода.'
        : 'Таблица показывает canonical output текущей parser node из того же server preview-run, который подтверждает publish-блок section 3.',
      rows: preferredFreshGrid.rows,
      columns: preferredFreshGrid.columns,
      structureFields,
      showStructure: false,
      structureColumns,
      liveRowsCount: preferredFreshGrid.rows.length,
      liveColumnsCount: preferredFreshGrid.columns.length,
      responseRowCount: previewResponseRowCount,
      preparedRowsCount: preferredFreshGrid.rows.length,
      sourceFormat: trim(previewData?.source_format),
      isStalePreview: false,
      effectiveInputSource,
      debug: {
        effectiveInputSource,
        structureColumns,
        previewRowCount: previewResponseRowCount,
        preparedRowsCount: preferredFreshGrid.rows.length,
        firstPreparedRowKeys: preferredFreshGrid.firstPreparedRowKeys,
        columnsWithoutMappedValues: preferredFreshGrid.columnsWithoutMappedValues,
        stalePreview: false,
        previewError: '',
        previewSuccess: true,
        gridColumns: preferredFreshGrid.columns,
        rawPreviewColumns: previewColumns
      }
    });
  }

  if (freshSuccess && previewRows.length && !freshRowsRenderable) {
    const fallbackGrid = inputView ? structureGrid : preferredFreshGrid;
    return createPreviewState({
      mode: 'preview_failed',
      modeLabel: inputView ? 'Preview входа с ошибкой' : 'Preview выхода с ошибкой',
      statusTone: 'error',
      statusTitle: inputView
        ? 'Не удалось собрать preview входных строк parser'
        : 'Не удалось собрать preview выходных строк parser',
      statusDescription: mappingError,
      rows: [],
      columns: [],
      structureFields,
      showStructure: structureFields.length > 0,
      structureColumns,
      liveRowsCount: 0,
      liveColumnsCount: 0,
      responseRowCount: previewResponseRowCount,
      preparedRowsCount: 0,
      sourceFormat: trim(previewData?.source_format),
      isStalePreview: false,
      effectiveInputSource,
      debug: {
        effectiveInputSource,
        structureColumns,
        previewRowCount: previewResponseRowCount,
        preparedRowsCount: 0,
        firstPreparedRowKeys: fallbackGrid.firstPreparedRowKeys,
        columnsWithoutMappedValues:
          fallbackGrid.columnsWithoutMappedValues.length > 0
            ? fallbackGrid.columnsWithoutMappedValues
            : structureColumns,
        stalePreview: false,
        previewError: mappingError,
        previewSuccess: false,
        gridColumns: fallbackGrid.columns,
        rawPreviewColumns: previewColumns
      }
    });
  }

  if (freshError) {
    return createPreviewState({
      mode: 'preview_failed',
      modeLabel: inputView ? 'Preview входа с ошибкой' : 'Preview выхода с ошибкой',
      statusTone: 'error',
      statusTitle: inputView
        ? 'Не удалось собрать preview входных строк parser'
        : 'Не удалось собрать preview выходных строк parser',
      statusDescription: trim(previewError) || 'Не удалось собрать preview.',
      rows: [],
      columns: [],
      structureFields,
      showStructure: structureFields.length > 0,
      structureColumns,
      liveRowsCount: 0,
      liveColumnsCount: 0,
      responseRowCount: previewResponseRowCount,
      preparedRowsCount: 0,
      sourceFormat: trim(previewData?.source_format),
      isStalePreview: false,
      effectiveInputSource,
      debug: {
        effectiveInputSource,
        structureColumns,
        previewRowCount: previewResponseRowCount,
        preparedRowsCount: 0,
        firstPreparedRowKeys: [],
        columnsWithoutMappedValues: structureColumns,
        stalePreview: false,
        previewError,
        previewSuccess: false,
        gridColumns: [],
        rawPreviewColumns: previewColumns
      }
    });
  }

  if (stalePreview && staleRowsRenderable) {
    return createPreviewState({
      mode: 'rows',
      modeLabel: inputView ? 'Preview входных строк' : 'Preview выходных строк',
      statusTone: 'warn',
      statusTitle: inputView
        ? 'Показаны входные строки последнего preview-run'
        : 'Показаны выходные строки последнего preview-run',
      statusDescription: staleDescription,
      rows: preferredStaleGrid.rows,
      columns: preferredStaleGrid.columns,
      structureFields,
      showStructure: false,
      structureColumns,
      liveRowsCount: preferredStaleGrid.rows.length,
      liveColumnsCount: preferredStaleGrid.columns.length,
      responseRowCount: previewResponseRowCount,
      preparedRowsCount: preferredStaleGrid.rows.length,
      sourceFormat: trim(previewData?.source_format),
      isStalePreview: true,
      effectiveInputSource,
      debug: {
        effectiveInputSource,
        structureColumns,
        previewRowCount: previewResponseRowCount,
        preparedRowsCount: preferredStaleGrid.rows.length,
        firstPreparedRowKeys: preferredStaleGrid.firstPreparedRowKeys,
        columnsWithoutMappedValues: preferredStaleGrid.columnsWithoutMappedValues,
        stalePreview: true,
        previewError: '',
        previewSuccess: false,
        gridColumns: preferredStaleGrid.columns,
        rawPreviewColumns: previewColumns
      }
    });
  }

  if (structureFields.length) {
    return createPreviewState({
      mode: 'shape_only',
      modeLabel: inputView ? 'Preview входа без строк' : 'Preview выхода без строк',
      statusTone: stalePreview ? 'warn' : freshSuccess ? 'info' : 'warn',
      statusTitle: inputView ? 'Preview входа не вернул живые строки' : 'Preview выхода не вернул живые строки',
      statusDescription: freshSuccess && previewResponseRowCount === 0
        ? inputView
          ? 'Server preview-run отработал, но для текущего parser input не вернул строк. Ниже остаётся только consume-структура входа.'
          : 'Server preview-run отработал, но для текущего parser output не вернул строк. Ниже остаётся только publish-структура выхода.'
        : stalePreview
        ? staleDescription
        : !effectiveInputSource.available
        ? effectiveInputSource.description
        : inputView
        ? 'После запуска server preview-run здесь появятся canonical input rows текущей parser node.'
        : 'После запуска server preview-run здесь появятся canonical output rows текущей parser node.',
      rows: [],
      columns: [],
      structureFields,
      showStructure: true,
      structureColumns,
      liveRowsCount: 0,
      liveColumnsCount: structureColumns.length,
      responseRowCount: previewResponseRowCount,
      preparedRowsCount: 0,
      sourceFormat: trim(previewData?.source_format),
      isStalePreview: stalePreview,
      effectiveInputSource,
      debug: {
        effectiveInputSource,
        structureColumns,
        previewRowCount: previewResponseRowCount,
        preparedRowsCount: 0,
        firstPreparedRowKeys: [],
        columnsWithoutMappedValues: structureColumns,
        stalePreview,
        previewError: '',
        previewSuccess: false,
        gridColumns: [],
        rawPreviewColumns: previewColumns
      }
    });
  }

  return createPreviewState({
    mode: 'no_preview_yet',
    modeLabel: inputView ? 'Preview входа не запускался' : 'Preview выхода не запускался',
    statusTone: 'info',
    statusTitle: inputView ? 'Preview входных строк ещё не запускался' : 'Preview выходных строк ещё не запускался',
    statusDescription: inputView
      ? 'После запуска server preview-run здесь появятся canonical input rows текущей parser node.'
      : 'После запуска server preview-run здесь появятся canonical output rows текущей parser node.',
    rows: [],
    columns: [],
    structureFields: [],
    showStructure: false,
    structureColumns: [],
    liveRowsCount: 0,
    liveColumnsCount: 0,
    responseRowCount: previewResponseRowCount,
    preparedRowsCount: 0,
    sourceFormat: trim(previewData?.source_format),
    isStalePreview: stalePreview,
    effectiveInputSource,
    debug: {
      effectiveInputSource,
      structureColumns: [],
      previewRowCount: previewResponseRowCount,
      preparedRowsCount: 0,
      firstPreparedRowKeys: [],
      columnsWithoutMappedValues: [],
      stalePreview,
      previewError: '',
      previewSuccess: false,
      gridColumns: [],
      rawPreviewColumns: previewColumns
    }
  });
}

export function buildParserDraftPreviewUxState({
  previewState = null,
  previewLoading = false,
  inputSource = null,
  runError = '',
  hasFreshPreviewRun = false,
  isStalePreviewRun = false
} = {}) {
  const state = previewState && typeof previewState === 'object' ? previewState : {};
  const source = normalizePreviewSource(state.effectiveInputSource || inputSource);
  const sourceLabel = trim(source.label) || 'Источник входа для preview не найден';
  const sourceDescription =
    trim(source.description) ||
    'Для запуска предпросмотра нужен доступный вход: last runtime input, upstream sample или manual sample.';

  if (previewLoading) {
    return {
      state: 'preview_running',
      stateLabel: 'Предпросмотр собирается',
      statusTone: 'info',
      statusDescription: 'Собираем предпросмотр результата по текущим настройкам parser.',
      actionLabel: 'Собираем предпросмотр...',
      actionDisabled: true,
      sourceLabel,
      sourceDescription
    };
  }

  if (trim(runError)) {
    return {
      state: 'preview_failed',
      stateLabel: 'Ошибка предпросмотра',
      statusTone: 'error',
      statusDescription: trim(runError) || 'Не удалось собрать предпросмотр результата.',
      actionLabel: 'Повторить предпросмотр',
      actionDisabled: false,
      sourceLabel,
      sourceDescription
    };
  }

  if (isStalePreviewRun) {
    return {
      state: 'stale_preview',
      stateLabel: 'Предпросмотр устарел',
      statusTone: 'warn',
      statusDescription: 'Настройки изменились, поэтому таблицы preview ниже больше не считаются актуальными.',
      actionLabel: 'Обновить предпросмотр',
      actionDisabled: false,
      sourceLabel,
      sourceDescription
    };
  }

  if (hasFreshPreviewRun) {
    return {
      state: 'preview_ready',
      stateLabel: 'Предпросмотр актуален',
      statusTone: 'ok',
      statusDescription: 'Server preview-run успешно завершился. Ниже показаны input/output детали этого же запуска.',
      actionLabel: 'Обновить предпросмотр',
      actionDisabled: false,
      sourceLabel,
      sourceDescription
    };
  }

  if (state.mode === 'preview_failed') {
    return {
      state: 'preview_failed',
      stateLabel: 'Ошибка предпросмотра',
      statusTone: 'error',
      statusDescription: trim(state.statusDescription) || 'Не удалось собрать предпросмотр результата.',
      actionLabel: 'Повторить предпросмотр',
      actionDisabled: false,
      sourceLabel,
      sourceDescription
    };
  }

  if (state.isStalePreview) {
    return {
      state: 'stale_preview',
      stateLabel: 'Предпросмотр устарел',
      statusTone: 'warn',
      statusDescription: 'Настройки изменились, поэтому таблица ниже больше не считается актуальной.',
      actionLabel: 'Обновить предпросмотр',
      actionDisabled: false,
      sourceLabel,
      sourceDescription
    };
  }

  if (state.mode === 'rows' || state.mode === 'shape_only') {
    return {
      state: 'preview_ready',
      stateLabel: 'Предпросмотр актуален',
      statusTone: state.mode === 'rows' ? 'ok' : 'info',
      statusDescription:
        state.mode === 'rows'
          ? 'Показаны актуальные draft preview rows по текущим настройкам parser.'
          : 'Предпросмотр выполнен, но для текущих настроек доступна только структура результата без строк.',
      actionLabel: 'Обновить предпросмотр',
      actionDisabled: false,
      sourceLabel,
      sourceDescription
    };
  }

  return {
    state: 'no_preview_yet',
    stateLabel: 'Предпросмотр не запускался',
    statusTone: 'info',
    statusDescription: 'Предпросмотр результата ещё не запускался.',
    actionLabel: 'Запустить предпросмотр результата',
    actionDisabled: false,
    sourceLabel,
    sourceDescription
  };
}
