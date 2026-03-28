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
      statusDescription: 'После server run здесь можно будет отдельно увидеть последний canonical output этой ноды и compare с draft preview ниже.',
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
    return {
      mode: 'shape_only',
      modeLabel: 'Last runtime без строк',
      statusTone: trim(runtimeStep?.status).toLowerCase() === 'error' ? 'error' : 'warn',
      statusTitle:
        trim(runtimeStep?.status).toLowerCase() === 'error'
          ? 'Последний runtime этой ноды завершился ошибкой'
          : 'Последний runtime не сохранил строк результата',
      statusDescription:
        trim(runtimeStep?.status).toLowerCase() === 'error'
          ? 'Строки runtime результата недоступны, поэтому ниже остаётся только текущая publish-структура.'
          : 'Ниже показана publish-структура текущей ноды. Это не live rows, а shape последнего известного результата.',
      rows: [],
      columns: [],
      structureFields,
      showStructure: true,
      structureColumns: structureColumns,
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
    statusDescription: 'Для этой ноды нет ни сохранённых строк результата, ни publish-структуры для shape-only режима.',
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

  if (stalePreview && previewRows.length) {
    const staleDisplayColumns = structureColumns.length ? structureColumns : previewColumns;
    return {
      mode: 'rows',
      modeLabel: 'Последний preview устарел',
      statusTone: 'warn',
      statusTitle: 'Настройки изменились, предпросмотр устарел',
      statusDescription:
        'Таблица ниже показывает последний draft preview по старым settings. Чтобы увидеть актуальные строки результата, обнови предпросмотр заново.',
      rows: previewRows,
      columns: staleDisplayColumns,
      structureFields,
      showStructure: false,
      liveRowsCount: Math.max(0, Number(previewData?.row_count || previewRows.length) || 0),
      liveColumnsCount: staleDisplayColumns.length,
      sourceFormat: trim(previewData?.source_format),
      isStalePreview: true
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

export function buildParserDraftPreviewUxState({ previewState = null, previewLoading = false, inputSource = null } = {}) {
  const state = previewState && typeof previewState === 'object' ? previewState : {};
  const source = inputSource && typeof inputSource === 'object' ? inputSource : {};
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
      statusTone: 'ok',
      statusDescription:
        state.mode === 'rows'
          ? 'Показаны актуальные draft preview rows по текущим настройкам parser.'
          : 'Предпросмотр выполнен, но для текущих настроек доступны только структура результата без строк.',
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
