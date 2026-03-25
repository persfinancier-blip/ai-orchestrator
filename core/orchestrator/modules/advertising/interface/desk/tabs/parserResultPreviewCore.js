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

export function parserResultPreviewColumnsFromRows(rows) {
  return uniqueStrings(
    (Array.isArray(rows) ? rows : []).flatMap((row) =>
      row && typeof row === 'object' && !Array.isArray(row) ? Object.keys(row) : []
    )
  );
}

function normalizeStructureFields(fields) {
  return (Array.isArray(fields) ? fields : [])
    .map((field) => {
      const name = trim(field?.alias || field?.name || field?.path);
      if (!name) return null;
      return {
        name,
        alias: trim(field?.alias),
        type: trim(field?.type),
        path: trim(field?.path),
        origin: trim(field?.origin)
      };
    })
    .filter(Boolean);
}

export function buildParserResultPreviewState({
  previewData = null,
  previewError = '',
  publishedDescriptorFields = [],
  currentConfigSignature = '',
  previewLastAttemptSignature = '',
  previewLastSuccessSignature = ''
} = {}) {
  const structureFields = normalizeStructureFields(publishedDescriptorFields);
  const structureColumns = uniqueStrings(structureFields.map((field) => field.name));
  const previewRows = (Array.isArray(previewData?.sample_rows) ? previewData.sample_rows : []).filter(
    (row) => row && typeof row === 'object' && !Array.isArray(row)
  );
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
  const liveRows = freshSuccess ? previewRows : [];
  const liveColumns = freshSuccess ? previewColumns : [];
  const liveRowsCount = freshSuccess ? Math.max(0, Number(previewData?.row_count || liveRows.length) || 0) : 0;
  const liveDisplayColumns = structureColumns.length ? structureColumns : liveColumns;
  const structureDisplayColumns = structureColumns;

  if (freshSuccess && liveRows.length) {
    return {
      mode: 'rows',
      modeLabel: 'Реальные строки результата',
      statusTone: 'ok',
      statusTitle: 'Показаны реальные строки результата parser',
      statusDescription: 'Таблица ниже собрана из текущего runtime preview. Это живые строки результата, а не fallback-структура.',
      rows: liveRows,
      columns: liveDisplayColumns,
      structureFields,
      showStructure: false,
      liveRowsCount,
      liveColumnsCount: liveDisplayColumns.length,
      sourceFormat: trim(previewData?.source_format),
      isStalePreview: false
    };
  }

  if (freshError) {
    return {
      mode: 'preview_failed',
      modeLabel: 'Preview с ошибкой',
      statusTone: 'error',
      statusTitle: 'Preview результата завершился с ошибкой',
      statusDescription: trim(previewError),
      rows: [],
      columns: [],
      structureFields,
      showStructure: structureFields.length > 0,
      structureColumns: structureDisplayColumns,
      liveRowsCount: 0,
      liveColumnsCount: 0,
      sourceFormat: trim(previewData?.source_format),
      isStalePreview: false
    };
  }

  if (!hasAnyAttempt) {
    return {
      mode: 'no_preview_yet',
      modeLabel: 'Preview ещё не запускался',
      statusTone: 'info',
      statusTitle: 'Preview результата ещё не запускался',
      statusDescription: structureFields.length
        ? 'Живые строки результата ещё не запрашивались. Ниже можно посмотреть текущую структуру publish result, но это не реальные data rows.'
        : 'Пока нет ни живых строк preview, ни структуры publish result. Настрой секции 2-3 и нажми «Обновить preview».',
      rows: [],
      columns: [],
      structureFields,
      showStructure: structureFields.length > 0,
      structureColumns: structureDisplayColumns,
      liveRowsCount: 0,
      liveColumnsCount: 0,
      sourceFormat: trim(previewData?.source_format),
      isStalePreview: false
    };
  }

  if (structureFields.length) {
    let statusDescription =
      'Живые preview rows для текущих settings недоступны. Ниже показана только структура publish result без строк.';
    if (stalePreview) {
      statusDescription =
        'После последнего preview настройки parser изменились. Живые строки ниже больше не считаются актуальными, поэтому показана только текущая структура результата.';
    } else if (freshSuccess && liveRowsCount === 0) {
      statusDescription =
        'Preview отработал на текущих settings, но не вернул строк результата. Ниже показана только структура publish result.';
    } else if (freshAttempt && !freshSuccess) {
      statusDescription =
        'Для текущих settings живые preview rows пока недоступны. Ниже показана только структура publish result без строк.';
    }

    return {
      mode: 'shape_only',
      modeLabel: 'Только структура результата',
      statusTone: 'warn',
      statusTitle: 'Пока доступна только структура результата',
      statusDescription,
      rows: [],
      columns: [],
      structureFields,
      showStructure: true,
      structureColumns: structureDisplayColumns,
      liveRowsCount,
      liveColumnsCount: structureDisplayColumns.length,
      sourceFormat: trim(previewData?.source_format),
      isStalePreview: stalePreview
    };
  }

  return {
    mode: 'no_preview_yet',
    modeLabel: 'Preview ещё не запускался',
    statusTone: stalePreview ? 'warn' : 'info',
    statusTitle: stalePreview ? 'Старый preview больше не актуален' : 'Preview результата ещё не запускался',
    statusDescription: stalePreview
      ? 'После изменения настроек актуальный preview ещё не собран. Сначала обнови preview.'
      : 'Пока нет ни живых строк preview, ни структуры publish result. Настрой секции 2-3 и нажми «Обновить preview».',
    rows: [],
    columns: [],
    structureFields: [],
    showStructure: false,
    structureColumns: [],
    liveRowsCount,
    liveColumnsCount: 0,
    sourceFormat: trim(previewData?.source_format),
    isStalePreview: stalePreview
  };
}
