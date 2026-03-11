function isIdent(value) {
  return typeof value === 'string' && /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(value);
}

function qi(ident) {
  if (!isIdent(ident)) throw new Error(`invalid_identifier:${ident}`);
  return `"${ident}"`;
}

function qname(schema, table) {
  return `${qi(schema)}.${qi(table)}`;
}

function normalizeFieldName(value) {
  return String(value || '')
    .trim()
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replace(/[\s\-./]+/g, '_')
    .replace(/__+/g, '_')
    .toLowerCase();
}

function parseCsvList(raw) {
  if (Array.isArray(raw)) return raw.map((x) => String(x || '').trim()).filter(Boolean);
  return String(raw || '')
    .split(',')
    .map((x) => x.trim())
    .filter(Boolean);
}

function parseJsonSafe(raw, fallback) {
  if (raw !== null && typeof raw === 'object') return raw;
  const txt = String(raw || '').trim();
  if (!txt) return fallback;
  try {
    return JSON.parse(txt);
  } catch {
    return fallback;
  }
}

function deepClone(value) {
  return JSON.parse(JSON.stringify(value));
}

function columnsFromRows(rows = []) {
  return [...new Set((Array.isArray(rows) ? rows : []).flatMap((row) => (row && typeof row === 'object' && !Array.isArray(row) ? Object.keys(row) : [])))];
}

function toArrayRows(value) {
  if (Array.isArray(value)) return value;
  if (value === undefined || value === null) return [];
  return [value];
}

function isHtmlLike(value) {
  const txt = String(value || '').trim().toLowerCase();
  return txt.startsWith('<!doctype html') || txt.startsWith('<html') || txt.includes('<body');
}

function parseDelimitedText(text, delimiter) {
  const rows = String(text || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  if (rows.length < 2) return [];
  const headers = rows[0].split(delimiter).map((x) => x.trim()).filter(Boolean);
  if (!headers.length) return [];
  return rows.slice(1).map((line) => {
    const parts = line.split(delimiter);
    const row = {};
    headers.forEach((header, idx) => {
      row[header] = String(parts[idx] ?? '').trim();
    });
    return row;
  });
}

function parseBusinessPayload(value) {
  if (value === undefined) return undefined;
  if (typeof value === 'string') {
    const txt = String(value || '').trim();
    if (!txt) return '';
    if (isHtmlLike(txt)) return { __html_text: txt };
    try {
      return parseBusinessPayload(JSON.parse(txt));
    } catch {
      const csvRows = parseDelimitedText(txt, txt.includes('\t') ? '\t' : ',');
      if (csvRows.length) return csvRows;
      return txt;
    }
  }
  if (Array.isArray(value)) return value;
  if (!value || typeof value !== 'object') return value;
  if (String(value.contract_version || '').trim() === 'node_io_v1' && Array.isArray(value.rows)) return value.rows;
  if (Array.isArray(value.rows)) return value.rows;
  if (Array.isArray(value.payloads_only) && value.payloads_only.length) return parseBusinessPayload(value.payloads_only[0]);
  if (Array.isArray(value.responses) && value.responses.length) {
    const payloads = value.responses
      .map((item) => parseBusinessPayload(item?.response ?? item))
      .filter((item) => item !== undefined);
    if (payloads.length === 1) return payloads[0];
    if (payloads.length > 1 && payloads.every((item) => Array.isArray(item))) return payloads.flat();
    if (payloads.length > 1) return payloads;
  }
  if (Object.prototype.hasOwnProperty.call(value, 'response')) return parseBusinessPayload(value.response);
  if (value.__truncated && typeof value.preview === 'string') {
    try {
      return parseBusinessPayload(JSON.parse(value.preview));
    } catch {
      return undefined;
    }
  }
  return value;
}

function normalizeSourceRows(value) {
  const payload = parseBusinessPayload(value);
  const warnings = [];
  let detectedFormat = 'json';
  let payloadOrigin = 'payload';
  let rows = [];
  if (Array.isArray(payload)) {
    rows = payload;
  } else if (payload && typeof payload === 'object' && payload.__html_text) {
    detectedFormat = 'html';
    payloadOrigin = 'html_text';
    warnings.push('Вход распознан как HTML/текст ошибки, а не как набор данных.');
    rows = [];
  } else if (payload && typeof payload === 'object') {
    rows = [payload];
  } else if (typeof payload === 'string') {
    detectedFormat = 'text';
    payloadOrigin = 'text';
    rows = payload ? [{ value: payload }] : [];
  } else {
    rows = toArrayRows(payload);
  }
  const normalizedRows = rows.map((row) => (row && typeof row === 'object' && !Array.isArray(row) ? row : { value: row }));
  return {
    rows: normalizedRows,
    detectedFormat,
    payloadOrigin,
    warnings
  };
}

function boolFromAny(value, fallback = false) {
  if (typeof value === 'boolean') return value;
  const raw = String(value ?? '').trim().toLowerCase();
  if (!raw) return fallback;
  return ['1', 'true', 'yes', 'on', 'enabled'].includes(raw);
}

export function normalizeWriteMode(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'update' || raw === 'update_by_key') return 'update_by_key';
  if (raw === 'upsert') return 'upsert';
  return 'insert';
}

export function normalizeJoinMode(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'all_source' || raw === 'left') return 'left';
  if (raw === 'all_lookup' || raw === 'right') return 'right';
  if (raw === 'all_both' || raw === 'full') return 'full';
  return 'inner';
}

export function normalizeConflictMode(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'keep_table' || raw === 'keep_existing') return 'keep_table';
  if (raw === 'only_if_empty' || raw === 'fill_if_empty') return 'only_if_empty';
  return 'input_wins';
}

export function normalizeUnmappedMode(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'error') return 'error';
  if (raw === 'skip') return 'skip';
  return 'matched_only';
}

export function normalizeWriteSettings(raw = {}) {
  const src = raw && typeof raw === 'object' ? raw : {};
  return {
    templateId: String(src.templateId || '').trim(),
    templateStoreId: String(src.templateStoreId || '').trim(),
    sourceMode: String(src.sourceMode || '').trim().toLowerCase() === 'table' ? 'table' : 'node',
    sourceNodeTemplateRef: String(src.sourceNodeTemplateRef || '').trim(),
    sourceNodeTemplateType: String(src.sourceNodeTemplateType || '').trim(),
    sourceNodeTemplateStoreId: String(src.sourceNodeTemplateStoreId || '').trim(),
    sourceNodeTemplateName: String(src.sourceNodeTemplateName || '').trim(),
    sourceSchema: String(src.sourceSchema || '').trim(),
    sourceTable: String(src.sourceTable || '').trim(),
    sourceColumn: String(src.sourceColumn || '').trim(),
    targetSchema: String(src.targetSchema || src.target_schema || '').trim(),
    targetTable: String(src.targetTable || src.target_table || '').trim(),
    writeMode: normalizeWriteMode(src.writeMode || src.write_mode || 'insert'),
    fieldMappings: Array.isArray(src.fieldMappings || src.field_mappings)
      ? src.fieldMappings || src.field_mappings
      : parseJsonSafe(src.fieldMappingsJson || src.field_mappings_json || '[]', []),
    keyFields: parseCsvList(src.keyFields || src.key_fields || src.keyColumns || src.key_columns || ''),
    unmappedMode: normalizeUnmappedMode(src.unmappedMode || src.unmapped_mode || 'matched_only'),
    conflictMode: normalizeConflictMode(src.conflictMode || src.conflict_mode || 'input_wins'),
    batchSize: Math.max(1, Math.min(5000, Math.trunc(Number(src.batchSize || src.batch_size || 500)))),
    previewLimit: Math.max(1, Math.min(200, Math.trunc(Number(src.previewLimit || src.preview_limit || 20)))),
    channel: String(src.channel || '').trim()
  };
}

export async function tableExists(client, schema, table) {
  const r = await client.query(
    `
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = $1
      AND table_name = $2
    LIMIT 1
    `,
    [String(schema || '').trim(), String(table || '').trim()]
  );
  return Boolean(r.rows?.length);
}

export async function tableColumnsDetailed(client, schema, table) {
  const r = await client.query(
    `
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = $1
      AND table_name = $2
    ORDER BY ordinal_position ASC
    `,
    [String(schema || '').trim(), String(table || '').trim()]
  );
  return (Array.isArray(r.rows) ? r.rows : [])
    .map((row) => ({
      name: String(row?.column_name || '').trim(),
      type: String(row?.data_type || '').trim().toLowerCase()
    }))
    .filter((row) => row.name);
}

async function readTableRows(client, schema, table, limit) {
  if (!isIdent(schema) || !isIdent(table)) return [];
  const qn = qname(schema, table);
  const safeLimit = Math.max(1, Math.min(5000, Math.trunc(Number(limit || 100))));
  const r = await client.query(`SELECT * FROM ${qn} LIMIT ${safeLimit}`);
  return (Array.isArray(r.rows) ? r.rows : []).map((row) => (row && typeof row === 'object' ? row : { value: row }));
}

function coerceTableSourceRows(rows, sourceColumn) {
  const column = String(sourceColumn || '').trim();
  if (!column) return rows;
  return rows
    .map((row) => row?.[column])
    .map((value) => {
      const parsed = parseBusinessPayload(value);
      if (Array.isArray(parsed)) return parsed;
      if (parsed && typeof parsed === 'object') return parsed;
      if (parsed === undefined || parsed === null || parsed === '') return null;
      return { value: parsed };
    })
    .flat()
    .filter(Boolean)
    .map((row) => (row && typeof row === 'object' && !Array.isArray(row) ? row : { value: row }));
}

async function resolveSourceRows(client, settings, { inputValue, inputEnvelope, limit = 20 } = {}) {
  const cfg = normalizeWriteSettings(settings);
  if (cfg.sourceMode === 'table') {
    if (!cfg.sourceSchema || !cfg.sourceTable) {
      return { rows: [], sourceType: 'table', sourceRef: '-', warnings: ['Сначала выбери таблицу-источник.'] };
    }
    const tableRows = await readTableRows(client, cfg.sourceSchema, cfg.sourceTable, limit);
    return {
      rows: coerceTableSourceRows(tableRows, cfg.sourceColumn),
      sourceType: 'table',
      sourceRef: `${cfg.sourceSchema}.${cfg.sourceTable}`,
      warnings: []
    };
  }

  const envelopeRows =
    inputEnvelope && typeof inputEnvelope === 'object' && Array.isArray(inputEnvelope.rows)
      ? inputEnvelope.rows
      : undefined;
  const normalized = normalizeSourceRows(envelopeRows !== undefined ? envelopeRows : inputValue);
  return {
    rows: normalized.rows.slice(0, Math.max(1, limit)),
    sourceType: 'node',
    sourceRef: String(cfg.sourceNodeTemplateName || cfg.sourceNodeTemplateRef || 'upstream').trim(),
    warnings: normalized.warnings,
    diagnostics: {
      detected_format: normalized.detectedFormat,
      payload_origin: normalized.payloadOrigin
    }
  };
}

function fieldMappingsArray(raw) {
  const list = Array.isArray(raw) ? raw : [];
  return list
    .map((item) => ({
      sourceField: String(item?.sourceField || item?.source_field || '').trim(),
      targetField: String(item?.targetField || item?.target_field || '').trim(),
      mode: String(item?.mode || '').trim()
    }))
    .filter((item) => item.sourceField || item.targetField);
}

function buildAutoMapping(sourceColumns, targetColumns) {
  const byNormalizedTarget = new Map(targetColumns.map((item) => [normalizeFieldName(item.name), item.name]));
  return sourceColumns
    .map((sourceField) => {
      const targetField = byNormalizedTarget.get(normalizeFieldName(sourceField)) || '';
      return { sourceField, targetField, auto: Boolean(targetField) };
    })
    .filter((item) => item.sourceField);
}

export function resolveWriteMappings(sourceColumns, targetColumns, rawMappings) {
  const targetSet = new Set(targetColumns.map((item) => item.name));
  const explicit = fieldMappingsArray(rawMappings);
  const explicitBySource = new Map(explicit.map((item) => [item.sourceField, item]));
  const autoRows = buildAutoMapping(sourceColumns, targetColumns);
  const combined = sourceColumns.map((sourceField) => {
    const explicitItem = explicitBySource.get(sourceField);
    const autoItem = autoRows.find((item) => item.sourceField === sourceField);
    const targetField = String(explicitItem?.targetField || autoItem?.targetField || '').trim();
    const status = !targetField ? 'unmapped' : targetSet.has(targetField) ? 'mapped' : 'missing_target';
    return {
      sourceField,
      targetField,
      status,
      autoMatched: !explicitItem && Boolean(autoItem?.targetField)
    };
  });
  return {
    rows: combined,
    matchedRows: combined.filter((item) => item.status === 'mapped'),
    unmatchedSourceFields: combined.filter((item) => item.status !== 'mapped').map((item) => item.sourceField),
    unmatchedTargetFields: targetColumns
      .map((item) => item.name)
      .filter((field) => !combined.some((mapping) => mapping.targetField === field && mapping.status === 'mapped'))
  };
}

function applyMappingsToRow(row, mappings, unmappedMode) {
  const src = row && typeof row === 'object' && !Array.isArray(row) ? row : { value: row };
  const out = {};
  const usedTargets = [];
  for (const mapping of mappings) {
    if (mapping.status !== 'mapped' || !mapping.targetField) continue;
    out[mapping.targetField] = src[mapping.sourceField];
    usedTargets.push(mapping.targetField);
  }
  if (unmappedMode === 'error') {
    const missing = mappings.filter((item) => item.status !== 'mapped').map((item) => item.sourceField);
    if (missing.length) {
      const error = new Error(`write_unmapped_fields:${missing.join(',')}`);
      error.code = 'write_unmapped_fields';
      throw error;
    }
  }
  return { row: out, usedTargets };
}

function rowsWithCompleteKeys(rows, keyFields) {
  const keys = Array.isArray(keyFields) ? keyFields : [];
  return rows.filter((row) => keys.every((key) => row?.[key] !== undefined && row?.[key] !== null && String(row[key]) !== ''));
}

async function countExistingMatches(client, schema, table, rows, keyFields) {
  const keys = parseCsvList(keyFields);
  if (!rows.length || !keys.length || !isIdent(schema) || !isIdent(table)) return { matched: 0 };
  const qn = qname(schema, table);
  let matched = 0;
  for (const row of rows) {
    const values = [];
    const whereSql = keys
      .map((key) => {
        values.push(row[key] ?? null);
        return `${qi(key)} = $${values.length}`;
      })
      .join(' AND ');
    const r = await client.query(`SELECT 1 FROM ${qn} WHERE ${whereSql} LIMIT 1`, values);
    if (r.rows?.length) matched += 1;
  }
  return { matched };
}

export async function previewWriteConfig(client, rawSettings, options = {}) {
  const cfg = normalizeWriteSettings(rawSettings);
  const source = await resolveSourceRows(client, cfg, {
    inputValue: options.inputValue,
    inputEnvelope: options.inputEnvelope,
    limit: cfg.previewLimit
  });
  const sourceRows = source.rows;
  const sourceColumns = columnsFromRows(sourceRows);
  const targetColumns = cfg.targetSchema && cfg.targetTable ? await tableColumnsDetailed(client, cfg.targetSchema, cfg.targetTable) : [];
  const mapping = resolveWriteMappings(sourceColumns, targetColumns, cfg.fieldMappings);
  const hasPayloadJson = targetColumns.some((column) => column.name === 'payload_json');
  const hasPayload = targetColumns.some((column) => column.name === 'payload');
  const readyRows = [];
  for (const sourceRow of sourceRows) {
    const mapped = applyMappingsToRow(sourceRow, mapping.rows, cfg.unmappedMode).row;
    if (Object.keys(mapped).length) readyRows.push(mapped);
    else if (hasPayloadJson) readyRows.push({ payload_json: sourceRow });
    else if (hasPayload) readyRows.push({ payload: sourceRow });
  }
  const keys = cfg.keyFields.filter((field) => targetColumns.some((column) => column.name === field));
  const rowsWithKeys = rowsWithCompleteKeys(readyRows, keys);
  const matchStats = cfg.targetSchema && cfg.targetTable ? await countExistingMatches(client, cfg.targetSchema, cfg.targetTable, rowsWithKeys.slice(0, cfg.previewLimit), keys) : { matched: 0 };
  const writeSummary = {
    rows_ready: readyRows.length,
    mapped_fields_count: mapping.matchedRows.length,
    unmatched_source_fields: mapping.unmatchedSourceFields,
    unmatched_target_fields: mapping.unmatchedTargetFields,
    key_fields: keys,
    rows_with_complete_key: rowsWithKeys.length,
    matched_existing_rows: matchStats.matched,
    insert_candidates: cfg.writeMode === 'update_by_key' ? 0 : Math.max(0, rowsWithKeys.length - matchStats.matched) + Math.max(0, readyRows.length - rowsWithKeys.length),
    update_candidates: cfg.writeMode === 'insert' ? 0 : matchStats.matched
  };
  return {
    source_type: source.sourceType,
    source_ref: source.sourceRef,
    source_row_count: sourceRows.length,
    source_column_count: sourceColumns.length,
    source_columns: sourceColumns,
    source_sample_rows: sourceRows.slice(0, cfg.previewLimit),
    target_schema: cfg.targetSchema,
    target_table: cfg.targetTable,
    target_columns: targetColumns,
    mapping_rows: mapping.rows,
    mapping_summary: writeSummary,
    result_row_count: readyRows.length,
    result_column_count: columnsFromRows(readyRows).length,
    result_columns: columnsFromRows(readyRows),
    result_sample_rows: readyRows.slice(0, cfg.previewLimit),
    stats: {
      warnings: source.warnings,
      applied_steps: ['source', 'mapping', 'write_preview'],
      detected_format: source.diagnostics?.detected_format || '-',
      payload_origin: source.diagnostics?.payload_origin || '-'
    }
  };
}

export async function executeWriteConfig(client, rawSettings, options = {}) {
  const cfg = normalizeWriteSettings(rawSettings);
  const source = await resolveSourceRows(client, cfg, {
    inputValue: options.inputValue,
    inputEnvelope: options.inputEnvelope,
    limit: Number.isFinite(Number(options.limit)) ? Number(options.limit) : Number.MAX_SAFE_INTEGER
  });
  const sourceRows = source.rows;
  if (!sourceRows.length) {
    return {
      rows: [],
      stats: { wrote: 0, inserted: 0, updated: 0, skipped: 0, warnings: source.warnings, target_type: 'none' },
      meta: { target: cfg.targetSchema && cfg.targetTable ? `${cfg.targetSchema}.${cfg.targetTable}` : 'process_bus' }
    };
  }
  const targetExists = cfg.targetSchema && cfg.targetTable && (await tableExists(client, cfg.targetSchema, cfg.targetTable));
  if (!targetExists) {
    return {
      rows: sourceRows,
      stats: { wrote: sourceRows.length, inserted: sourceRows.length, updated: 0, skipped: 0, warnings: source.warnings, target_type: 'process_bus' },
      meta: { target: 'process_bus', channel: cfg.channel }
    };
  }
  const targetColumns = await tableColumnsDetailed(client, cfg.targetSchema, cfg.targetTable);
  const hasPayloadJson = targetColumns.some((column) => column.name === 'payload_json');
  const hasPayload = targetColumns.some((column) => column.name === 'payload');
  const mapping = resolveWriteMappings(columnsFromRows(sourceRows), targetColumns, cfg.fieldMappings);
  const readyRows = [];
  for (const sourceRow of sourceRows) {
    const mapped = applyMappingsToRow(sourceRow, mapping.rows, cfg.unmappedMode).row;
    if (Object.keys(mapped).length) readyRows.push(mapped);
    else if (hasPayloadJson) readyRows.push({ payload_json: sourceRow });
    else if (hasPayload) readyRows.push({ payload: sourceRow });
  }
  if (!readyRows.length) throw new Error(`db_write_no_matching_columns:${cfg.targetSchema}.${cfg.targetTable}`);

  const keyFields = cfg.keyFields.filter((field) => targetColumns.some((column) => column.name === field));
  const writeColumns = [...new Set(readyRows.flatMap((row) => Object.keys(row).filter((key) => targetColumns.some((col) => col.name === key))))];
  const qn = qname(cfg.targetSchema, cfg.targetTable);
  const batches = [];
  for (let i = 0; i < readyRows.length; i += cfg.batchSize) batches.push(readyRows.slice(i, i + cfg.batchSize));
  let wrote = 0;
  let inserted = 0;
  let updated = 0;
  let skipped = 0;

  for (const batch of batches) {
    if (!batch.length) continue;
    const completeKeyRows = rowsWithCompleteKeys(batch, keyFields);
    const matchStats = cfg.writeMode === 'insert' ? { matched: 0 } : await countExistingMatches(client, cfg.targetSchema, cfg.targetTable, completeKeyRows, keyFields);
    if (cfg.writeMode === 'update_by_key') {
      const nonKeys = writeColumns.filter((column) => !keyFields.includes(column));
      if (!keyFields.length || !nonKeys.length) throw new Error('db_write_update_requires_key_and_nonkey_columns');
      for (const row of completeKeyRows) {
        if (cfg.conflictMode === 'keep_table') {
          skipped += 1;
          continue;
        }
        const values = [];
        const setSql = nonKeys
          .map((column) => {
            values.push(row[column] ?? null);
            const paramNo = values.length;
            if (cfg.conflictMode === 'only_if_empty') return `${qi(column)} = COALESCE(${qi(column)}, $${paramNo})`;
            return `${qi(column)} = $${paramNo}`;
          })
          .join(', ');
        const whereSql = keyFields
          .map((column) => {
            values.push(row[column] ?? null);
            return `${qi(column)} = $${values.length}`;
          })
          .join(' AND ');
        const result = await client.query(`UPDATE ${qn} SET ${setSql} WHERE ${whereSql}`, values);
        const affected = Math.max(0, Number(result.rowCount || 0));
        wrote += affected;
        updated += affected;
      }
      continue;
    }

    const values = [];
    const tupleSql = batch
      .map((row) => {
        const tuple = writeColumns.map((column) => {
          const value =
            column === 'payload_json' || column === 'payload'
              ? JSON.stringify(row[column] ?? row)
              : row[column] ?? null;
          values.push(value);
          return column === 'payload_json' || column === 'payload' ? `$${values.length}::jsonb` : `$${values.length}`;
        });
        return `(${tuple.join(', ')})`;
      })
      .join(', ');
    let sql = `INSERT INTO ${qn} (${writeColumns.map((column) => qi(column)).join(', ')}) VALUES ${tupleSql}`;
    if (cfg.writeMode === 'upsert') {
      if (!keyFields.length) throw new Error('db_write_upsert_requires_key_columns');
      const nonKeys = writeColumns.filter((column) => !keyFields.includes(column));
      if (!nonKeys.length || cfg.conflictMode === 'keep_table') {
        sql += ` ON CONFLICT (${keyFields.map((column) => qi(column)).join(', ')}) DO NOTHING`;
      } else {
        const setSql = nonKeys
          .map((column) => {
            if (cfg.conflictMode === 'only_if_empty') return `${qi(column)} = COALESCE(${qi(column)}, EXCLUDED.${qi(column)})`;
            return `${qi(column)} = EXCLUDED.${qi(column)}`;
          })
          .join(', ');
        sql += ` ON CONFLICT (${keyFields.map((column) => qi(column)).join(', ')}) DO UPDATE SET ${setSql}`;
      }
    }
    const result = await client.query(sql, values);
    const affected = Math.max(0, Number(result.rowCount || 0));
    wrote += affected;
    if (cfg.writeMode === 'insert') {
      inserted += affected;
    } else if (cfg.writeMode === 'upsert') {
      updated += Math.min(matchStats.matched, affected);
      inserted += Math.max(0, affected - matchStats.matched);
      skipped += cfg.conflictMode === 'keep_table' ? matchStats.matched : 0;
    }
  }

  return {
    rows: readyRows,
    stats: {
      wrote,
      inserted,
      updated,
      skipped,
      warnings: source.warnings,
      target_type: 'table',
      batches: batches.length
    },
    meta: {
      target: `${cfg.targetSchema}.${cfg.targetTable}`,
      key_fields: keyFields,
      write_mode: cfg.writeMode
    }
  };
}
