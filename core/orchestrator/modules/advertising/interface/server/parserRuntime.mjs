import { Readable } from 'node:stream';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const { parse: csvParse } = require('csv-parse');
const { parse: csvParseSync } = require('csv-parse/sync');
const split2 = require('split2');
const unzipper = require('unzipper');
const { chain } = require('stream-chain');
const { parser: jsonParser } = require('stream-json');
const { pick } = require('stream-json/filters/Pick');
const { streamArray } = require('stream-json/streamers/StreamArray');

const DEFAULT_BATCH_SIZE = 200;
const DEFAULT_SCAN_WINDOW = 500;
const DEFAULT_MAX_JSON_BYTES = 20 * 1024 * 1024;
const DEFAULT_PREVIEW_LIMIT = 20;

function isIdent(s) {
  return typeof s === 'string' && /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(s);
}

function qi(ident) {
  if (!isIdent(ident)) throw new Error(`invalid_identifier:${ident}`);
  return `"${ident}"`;
}

function toArrayRows(value) {
  if (Array.isArray(value)) return value;
  if (value === undefined || value === null) return [];
  return [value];
}

function parseCsvList(raw) {
  if (Array.isArray(raw)) return raw.map((x) => String(x || '').trim()).filter(Boolean);
  return String(raw || '')
    .split(',')
    .map((x) => x.trim())
    .filter(Boolean);
}

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

function parseJsonArraySafe(raw, fallback = []) {
  if (Array.isArray(raw)) return raw;
  const txt = String(raw || '').trim();
  if (!txt) return fallback;
  try {
    const parsed = JSON.parse(txt);
    return Array.isArray(parsed) ? parsed : fallback;
  } catch {
    return fallback;
  }
}

function parsePathParts(path) {
  const raw = String(path || '').trim();
  if (!raw) return [];
  const normalized = raw.replace(/\[(\d+)\]/g, '.$1');
  return normalized
    .split('.')
    .map((p) => p.trim())
    .filter(Boolean)
    .map((p) => (/^\d+$/.test(p) ? Number(p) : p));
}

function getByPath(obj, path) {
  if (!path) return obj;
  const parts = parsePathParts(path);
  let cur = obj;
  for (const part of parts) {
    if (cur == null) return undefined;
    cur = cur[part];
  }
  return cur;
}

function boolFromAny(value, fallback = false) {
  if (value === undefined || value === null || value === '') return Boolean(fallback);
  if (typeof value === 'boolean') return value;
  const raw = String(value).trim().toLowerCase();
  if (!raw) return Boolean(fallback);
  if (['1', 'true', 'yes', 'y', 'on', 'enabled'].includes(raw)) return true;
  if (['0', 'false', 'no', 'n', 'off', 'disabled'].includes(raw)) return false;
  return Boolean(fallback);
}

function normalizeParserFormat(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return 'auto';
  if (['auto', 'json', 'csv', 'ndjson', 'text', 'zip'].includes(raw)) return raw;
  if (raw === 'json_array' || raw === 'nested_json') return 'json';
  return 'auto';
}

function normalizeSourceMode(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'table' || raw === 'file_url' || raw === 'input') return raw;
  if (raw === 'node') return 'input';
  return 'input';
}

function detectFormatFromUrl(url) {
  const raw = String(url || '').trim().toLowerCase();
  if (!raw) return 'auto';
  if (raw.endsWith('.csv')) return 'csv';
  if (raw.endsWith('.ndjson') || raw.endsWith('.jsonl')) return 'ndjson';
  if (raw.endsWith('.json')) return 'json';
  if (raw.endsWith('.txt') || raw.endsWith('.log')) return 'text';
  if (raw.endsWith('.zip')) return 'zip';
  return 'auto';
}

function detectFormatFromContentType(contentType) {
  const raw = String(contentType || '').trim().toLowerCase();
  if (!raw) return 'auto';
  if (raw.includes('csv')) return 'csv';
  if (raw.includes('x-ndjson') || raw.includes('jsonl')) return 'ndjson';
  if (raw.includes('json')) return 'json';
  if (raw.includes('zip')) return 'zip';
  if (raw.startsWith('text/')) return 'text';
  return 'auto';
}

function inferFormat(value, explicitFormat = 'auto', hint = '') {
  const format = normalizeParserFormat(explicitFormat);
  if (format !== 'auto') return format;
  const hintFormat = detectFormatFromUrl(hint);
  if (hintFormat !== 'auto') return hintFormat;
  if (typeof value === 'string') {
    const txt = value.trim();
    if (txt.startsWith('{') || txt.startsWith('[')) return 'json';
    if (txt.includes('\n') && txt.split('\n').every((line) => !line.trim() || line.trim().startsWith('{'))) return 'ndjson';
    if (txt.includes(',') && txt.includes('\n')) return 'csv';
    return 'text';
  }
  if (Array.isArray(value) || (value && typeof value === 'object')) return 'json';
  return 'text';
}

function normalizeParserSettings(raw = {}, overrides = {}) {
  const settings = raw && typeof raw === 'object' ? raw : {};
  return {
    sourceMode: normalizeSourceMode(overrides.sourceMode ?? settings.sourceMode ?? settings.source_mode),
    sourceFormat: normalizeParserFormat(overrides.sourceFormat ?? settings.sourceFormat ?? settings.source_format),
    sourceSchema: String((overrides.sourceSchema ?? settings.sourceSchema ?? settings.source_schema) || '').trim(),
    sourceTable: String((overrides.sourceTable ?? settings.sourceTable ?? settings.source_table) || '').trim(),
    sourceColumn: String((overrides.sourceColumn ?? settings.sourceColumn ?? settings.source_column) || '').trim(),
    inputPath: String((overrides.inputPath ?? settings.inputPath ?? settings.input_path) || '').trim(),
    recordPath: String((overrides.recordPath ?? settings.recordPath ?? settings.record_path) || '').trim(),
    fileUrl: String((overrides.fileUrl ?? settings.fileUrl ?? settings.file_url) || '').trim(),
    fileUrlPath: String((overrides.fileUrlPath ?? settings.fileUrlPath ?? settings.file_url_path) || '').trim(),
    archiveEntry: String((overrides.archiveEntry ?? settings.archiveEntry ?? settings.archive_entry) || '').trim(),
    archiveFormat: normalizeParserFormat(overrides.archiveFormat ?? settings.archiveFormat ?? settings.archive_format),
    csvDelimiter: String((overrides.csvDelimiter ?? settings.csvDelimiter ?? settings.csv_delimiter) || ',').slice(0, 4) || ',',
    textMode: String((overrides.textMode ?? settings.textMode ?? settings.text_mode) || 'lines').trim().toLowerCase() || 'lines',
    batchSize: Math.max(1, Math.min(5000, Math.trunc(Number(overrides.batchSize ?? settings.batchSize ?? settings.batch_size ?? DEFAULT_BATCH_SIZE)))),
    previewLimit: Math.max(1, Math.min(200, Math.trunc(Number(overrides.previewLimit ?? settings.previewLimit ?? settings.preview_limit ?? DEFAULT_PREVIEW_LIMIT)))),
    maxJsonBytes: Math.max(64 * 1024, Math.trunc(Number(overrides.maxJsonBytes ?? settings.maxJsonBytes ?? settings.max_json_bytes ?? DEFAULT_MAX_JSON_BYTES))),
    selectFields: parseCsvList(overrides.selectFields ?? settings.selectFields ?? settings.select_fields),
    renameMap: parseJsonObjectSafe(overrides.renameMap ?? settings.renameMap ?? settings.rename_map, {}),
    defaultValues: parseJsonObjectSafe(overrides.defaultValues ?? settings.defaultValues ?? settings.default_values, {}),
    typeMap: parseJsonObjectSafe(overrides.typeMap ?? settings.typeMap ?? settings.type_map, {}),
    filterField: String((overrides.filterField ?? settings.filterField ?? settings.filter_field) || '').trim(),
    filterOperator: String((overrides.filterOperator ?? settings.filterOperator ?? settings.filter_operator) || '').trim(),
    filterValue: overrides.filterValue ?? settings.filterValue ?? settings.filter_value ?? '',
    parserMultiplier: Math.max(1, Math.trunc(Number(overrides.parserMultiplier ?? settings.parserMultiplier ?? settings.parser_multiplier ?? 1))),
    sampleInput: overrides.sampleInput ?? settings.sampleInput ?? settings.sample_input ?? '',
    lookupEnabled: boolFromAny(overrides.lookupEnabled ?? settings.lookupEnabled ?? settings.lookup_enabled, false),
    lookupSchema: String((overrides.lookupSchema ?? settings.lookupSchema ?? settings.lookup_schema) || '').trim(),
    lookupTable: String((overrides.lookupTable ?? settings.lookupTable ?? settings.lookup_table) || '').trim(),
    lookupSourceField: String((overrides.lookupSourceField ?? settings.lookupSourceField ?? settings.lookup_source_field) || '').trim(),
    lookupTargetField: String((overrides.lookupTargetField ?? settings.lookupTargetField ?? settings.lookup_target_field) || '').trim(),
    lookupFields: parseCsvList(overrides.lookupFields ?? settings.lookupFields ?? settings.lookup_fields),
    lookupPrefix: String((overrides.lookupPrefix ?? settings.lookupPrefix ?? settings.lookup_prefix) || '').trim()
  };
}

function normalizeObjectRow(value) {
  if (value && typeof value === 'object' && !Array.isArray(value)) return value;
  return { value };
}

function candidateByInputPath(inputValue, inputEnvelope, inputPath) {
  if (!inputPath) return Array.isArray(inputEnvelope?.rows) ? inputEnvelope.rows : inputValue;
  return getByPath({ input: inputValue, envelope: inputEnvelope || {}, response: inputValue, value: inputValue }, inputPath);
}

function extractRecordsFromJsonValue(value, recordPath = '') {
  const source = value;
  let extracted = source;
  if (recordPath) {
    extracted =
      getByPath(source, recordPath) ??
      getByPath({ row: source, response: source, value: source, input: source }, recordPath);
  }
  if (Array.isArray(extracted)) return extracted;
  if (extracted === undefined) return [];
  return [extracted];
}

function convertFieldType(value, typeName) {
  const type = String(typeName || '').trim().toLowerCase();
  if (!type) return value;
  if (value === undefined || value === null || value === '') return value;
  if (type === 'int' || type === 'integer' || type === 'bigint') {
    const n = Number(value);
    return Number.isFinite(n) ? Math.trunc(n) : value;
  }
  if (type === 'numeric' || type === 'float' || type === 'number') {
    const n = Number(value);
    return Number.isFinite(n) ? n : value;
  }
  if (type === 'boolean' || type === 'bool') return boolFromAny(value, false);
  if (type === 'json' || type === 'jsonb') {
    if (typeof value === 'string') {
      try {
        return JSON.parse(value);
      } catch {
        return value;
      }
    }
    return value;
  }
  if (type === 'text' || type === 'string') return String(value);
  if (type === 'date' || type === 'timestamptz' || type === 'timestamp') {
    const dt = new Date(value);
    return Number.isFinite(dt.getTime()) ? dt.toISOString() : value;
  }
  return value;
}

function applyFieldMapping(record, cfg) {
  const src = normalizeObjectRow(record);
  let out = {};
  if (cfg.selectFields.length) {
    for (const path of cfg.selectFields) {
      const value = getByPath(src, path);
      const fallbackKey = String(path || '').split('.').filter(Boolean).slice(-1)[0] || String(path || '').trim();
      const targetKey = String(cfg.renameMap?.[path] || cfg.renameMap?.[fallbackKey] || fallbackKey).trim() || fallbackKey;
      out[targetKey] = value;
    }
  } else {
    for (const [key, value] of Object.entries(src)) {
      const targetKey = String(cfg.renameMap?.[key] || key).trim() || key;
      out[targetKey] = value;
    }
  }
  for (const [key, value] of Object.entries(cfg.defaultValues || {})) {
    if (out[key] === undefined || out[key] === null || out[key] === '') out[key] = value;
  }
  for (const [key, typeName] of Object.entries(cfg.typeMap || {})) {
    if (Object.prototype.hasOwnProperty.call(out, key)) out[key] = convertFieldType(out[key], typeName);
  }
  return out;
}

function compareByOperator(left, operator, right) {
  const op = String(operator || '').trim().toLowerCase();
  if (!op || op === '=' || op === 'eq') return String(left ?? '') === String(right ?? '');
  if (op === '!=' || op === '<>' || op === 'neq') return String(left ?? '') !== String(right ?? '');
  if (op === '>') return Number(left) > Number(right);
  if (op === '>=') return Number(left) >= Number(right);
  if (op === '<') return Number(left) < Number(right);
  if (op === '<=') return Number(left) <= Number(right);
  if (op === 'contains') return String(left ?? '').includes(String(right ?? ''));
  if (op === 'not_contains') return !String(left ?? '').includes(String(right ?? ''));
  if (op === 'empty') return left === undefined || left === null || String(left) === '';
  if (op === 'not_empty' || op === 'exists') return !(left === undefined || left === null || String(left) === '');
  return String(left ?? '') === String(right ?? '');
}

function applyParserTransform(record, cfg) {
  const mapped = applyFieldMapping(record, cfg);
  if (cfg.filterField || cfg.filterOperator) {
    if (!compareByOperator(mapped?.[cfg.filterField], cfg.filterOperator, cfg.filterValue)) return null;
  }
  if (cfg.parserMultiplier <= 1) return [mapped];
  return Array.from({ length: cfg.parserMultiplier }).map(() => JSON.parse(JSON.stringify(mapped)));
}

function columnsFromRows(rows) {
  const cols = new Set();
  for (const row of Array.isArray(rows) ? rows : []) {
    if (!row || typeof row !== 'object' || Array.isArray(row)) continue;
    for (const key of Object.keys(row)) cols.add(String(key));
  }
  return Array.from(cols);
}

async function applyLookupToRows(client, cfg, rows) {
  if (!cfg.lookupEnabled) return rows;
  if (!isIdent(cfg.lookupSchema) || !isIdent(cfg.lookupTable) || !isIdent(cfg.lookupTargetField) || !cfg.lookupSourceField) {
    return rows;
  }
  const sourceValues = [...new Set(rows.map((row) => String(row?.[cfg.lookupSourceField] ?? '')).filter(Boolean))];
  if (!sourceValues.length) return rows;
  const lookupCols = cfg.lookupFields.filter((x) => isIdent(x));
  if (!lookupCols.length) return rows;
  const qn = `${qi(cfg.lookupSchema)}.${qi(cfg.lookupTable)}`;
  const prefix = cfg.lookupPrefix ? String(cfg.lookupPrefix).trim() : '';
  const selectCols = [
    `${qi(cfg.lookupTargetField)}::text AS _lookup_key`,
    ...lookupCols.map((col) => `${qi(col)}`)
  ];
  const lookupRes = await client.query(
    `SELECT ${selectCols.join(', ')} FROM ${qn} WHERE ${qi(cfg.lookupTargetField)}::text = ANY($1::text[])`,
    [sourceValues]
  );
  const map = new Map();
  for (const row of Array.isArray(lookupRes.rows) ? lookupRes.rows : []) {
    const key = String(row?._lookup_key || '').trim();
    if (key && !map.has(key)) map.set(key, row);
  }
  return rows.map((row) => {
    const hit = map.get(String(row?.[cfg.lookupSourceField] ?? '').trim());
    if (!hit) return row;
    const next = { ...row };
    for (const col of lookupCols) {
      const target = prefix ? `${prefix}${col}` : col;
      next[target] = hit[col];
    }
    return next;
  });
}

function toNodeReadable(stream) {
  if (!stream) return null;
  if (typeof stream.pipe === 'function') return stream;
  if (typeof Readable.fromWeb === 'function') return Readable.fromWeb(stream);
  return null;
}

async function collectFromCsvStream(stream, cfg, offset, take) {
  const parser = csvParse({
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true,
    trim: true,
    delimiter: cfg.csvDelimiter || ','
  });
  stream.pipe(parser);
  let skipped = 0;
  let scanned = 0;
  const rows = [];
  let hasMore = false;
  for await (const record of parser) {
    scanned += 1;
    if (skipped < offset) {
      skipped += 1;
      continue;
    }
    if (rows.length < take) {
      rows.push(record);
      continue;
    }
    hasMore = true;
    break;
  }
  parser.destroy();
  return { rows, scanned, hasMore };
}

async function collectFromNdjsonStream(stream, offset, take) {
  const splitter = split2();
  stream.pipe(splitter);
  let skipped = 0;
  let scanned = 0;
  const rows = [];
  let hasMore = false;
  for await (const line of splitter) {
    const txt = String(line || '').trim();
    if (!txt) continue;
    scanned += 1;
    if (skipped < offset) {
      skipped += 1;
      continue;
    }
    if (rows.length < take) {
      try {
        rows.push(JSON.parse(txt));
      } catch {
        rows.push({ raw_text: txt, parse_error: 'invalid_json_line' });
      }
      continue;
    }
    hasMore = true;
    break;
  }
  splitter.destroy();
  return { rows, scanned, hasMore };
}

async function collectFromTextStream(stream, offset, take, textMode = 'lines') {
  if (textMode === 'whole') {
    let text = '';
    for await (const chunk of stream) text += chunk.toString('utf8');
    const row = { text };
    if (offset > 0) return { rows: [], scanned: 1, hasMore: false };
    return { rows: take > 0 ? [row] : [], scanned: 1, hasMore: false };
  }
  const splitter = split2();
  stream.pipe(splitter);
  let skipped = 0;
  let scanned = 0;
  const rows = [];
  let hasMore = false;
  for await (const line of splitter) {
    scanned += 1;
    if (skipped < offset) {
      skipped += 1;
      continue;
    }
    if (rows.length < take) {
      rows.push({ text: String(line || ''), line_no: scanned });
      continue;
    }
    hasMore = true;
    break;
  }
  splitter.destroy();
  return { rows, scanned, hasMore };
}

async function collectFromJsonStream(stream, cfg, offset, take) {
  const recordPath = String(cfg.recordPath || '').trim();
  if (!recordPath) {
    let text = '';
    let size = 0;
    for await (const chunk of stream) {
      const piece = chunk.toString('utf8');
      size += Buffer.byteLength(piece);
      if (size > cfg.maxJsonBytes) throw new Error('parser_json_requires_record_path_for_large_stream');
      text += piece;
    }
    const parsed = JSON.parse(text || 'null');
    const records = extractRecordsFromJsonValue(parsed, recordPath);
    const slice = records.slice(offset, offset + take + 1);
    return {
      rows: slice.slice(0, take),
      scanned: records.length,
      hasMore: slice.length > take
    };
  }

  const pipeline = chain([stream, jsonParser(), pick({ filter: recordPath }), streamArray()]);
  let skipped = 0;
  let scanned = 0;
  const rows = [];
  let hasMore = false;
  for await (const item of pipeline) {
    scanned += 1;
    if (skipped < offset) {
      skipped += 1;
      continue;
    }
    if (rows.length < take) {
      rows.push(item?.value);
      continue;
    }
    hasMore = true;
    break;
  }
  pipeline.destroy();
  return { rows, scanned, hasMore };
}

async function chooseZipEntry(zipStream, cfg) {
  const wanted = String(cfg.archiveEntry || '').trim().toLowerCase();
  const directory = zipStream.pipe(unzipper.Parse({ forceStream: true }));
  let selected = null;
  for await (const entry of directory) {
    const path = String(entry?.path || '').trim();
    const lower = path.toLowerCase();
    const type = String(entry?.type || '').trim().toLowerCase();
    if (type === 'directory') {
      entry.autodrain();
      continue;
    }
    const inferred = inferFormat(null, cfg.archiveFormat, path);
    const expectedFormat = normalizeParserFormat(cfg.archiveFormat);
    const formatMatches = expectedFormat === 'auto' ? ['csv', 'json', 'ndjson', 'text'].includes(inferred) : inferred === expectedFormat;
    if (wanted) {
      if (lower === wanted || lower.endsWith(`/${wanted}`) || lower.includes(wanted)) {
        selected = { entry, path, format: inferred };
        break;
      }
      entry.autodrain();
      continue;
    }
    if (!selected && formatMatches) {
      selected = { entry, path, format: inferred };
      break;
    }
    entry.autodrain();
  }
  return selected;
}

async function collectFromZipStream(stream, cfg, offset, take) {
  const selected = await chooseZipEntry(stream, cfg);
  if (!selected?.entry) throw new Error('parser_zip_entry_not_found');
  const innerFormat = normalizeParserFormat(cfg.archiveFormat) === 'auto' ? selected.format : cfg.archiveFormat;
  return collectFromStreamByFormat(selected.entry, { ...cfg, sourceFormat: innerFormat }, offset, take);
}

async function collectFromStreamByFormat(stream, cfg, offset, take) {
  const format = normalizeParserFormat(cfg.sourceFormat);
  if (format === 'csv') return collectFromCsvStream(stream, cfg, offset, take);
  if (format === 'ndjson') return collectFromNdjsonStream(stream, offset, take);
  if (format === 'text') return collectFromTextStream(stream, offset, take, cfg.textMode);
  if (format === 'json') return collectFromJsonStream(stream, cfg, offset, take);
  if (format === 'zip') return collectFromZipStream(stream, cfg, offset, take);
  throw new Error(`parser_unsupported_stream_format:${format}`);
}

async function collectInputRecords(candidate, cfg, offset, take) {
  const format = inferFormat(candidate, cfg.sourceFormat);
  if (typeof candidate === 'string') {
    if (format === 'csv') {
      const rows = csvParseSync(candidate, {
        columns: true,
        skip_empty_lines: true,
        relax_column_count: true,
        trim: true,
        delimiter: cfg.csvDelimiter || ','
      });
      const slice = rows.slice(offset, offset + take + 1);
      return { rows: slice.slice(0, take), scanned: rows.length, hasMore: slice.length > take };
    }
    if (format === 'ndjson') {
      const lines = String(candidate || '')
        .split(/\r?\n/)
        .map((x) => x.trim())
        .filter(Boolean)
        .map((line) => {
          try {
            return JSON.parse(line);
          } catch {
            return { raw_text: line, parse_error: 'invalid_json_line' };
          }
        });
      const slice = lines.slice(offset, offset + take + 1);
      return { rows: slice.slice(0, take), scanned: lines.length, hasMore: slice.length > take };
    }
    if (format === 'text') {
      if (cfg.textMode === 'whole') {
        if (offset > 0) return { rows: [], scanned: 1, hasMore: false };
        return { rows: take > 0 ? [{ text: candidate }] : [], scanned: 1, hasMore: false };
      }
      const lines = String(candidate || '').split(/\r?\n/).map((line, idx) => ({ text: line, line_no: idx + 1 }));
      const slice = lines.slice(offset, offset + take + 1);
      return { rows: slice.slice(0, take), scanned: lines.length, hasMore: slice.length > take };
    }
    if (format === 'json') {
      const parsed = JSON.parse(candidate);
      const records = extractRecordsFromJsonValue(parsed, cfg.recordPath);
      const slice = records.slice(offset, offset + take + 1);
      return { rows: slice.slice(0, take), scanned: records.length, hasMore: slice.length > take };
    }
  }
  const records = extractRecordsFromJsonValue(candidate, cfg.recordPath);
  const slice = records.slice(offset, offset + take + 1);
  return { rows: slice.slice(0, take), scanned: records.length, hasMore: slice.length > take };
}

async function collectTableRecords(client, cfg, offset, take) {
  if (!isIdent(cfg.sourceSchema) || !isIdent(cfg.sourceTable)) throw new Error('parser_table_source_invalid');
  const qn = `${qi(cfg.sourceSchema)}.${qi(cfg.sourceTable)}`;
  const scanWindow = Math.max(DEFAULT_SCAN_WINDOW, Math.min(5000, cfg.batchSize * 2));
  let dbOffset = 0;
  let skipped = 0;
  let scanned = 0;
  const rows = [];
  let hasMore = false;
  while (rows.length <= take) {
    const res = await client.query(`SELECT * FROM ${qn} LIMIT $1 OFFSET $2`, [scanWindow, dbOffset]);
    const batchRows = Array.isArray(res.rows) ? res.rows : [];
    if (!batchRows.length) break;
    dbOffset += batchRows.length;
    for (const sourceRow of batchRows) {
      scanned += 1;
      const value = cfg.sourceColumn ? sourceRow?.[cfg.sourceColumn] : sourceRow;
      const collected = await collectInputRecords(value, { ...cfg, recordPath: cfg.sourceColumn ? cfg.recordPath : cfg.recordPath }, 0, Number.MAX_SAFE_INTEGER);
      for (const record of collected.rows) {
        if (skipped < offset) {
          skipped += 1;
          continue;
        }
        if (rows.length < take) {
          rows.push(record);
          continue;
        }
        hasMore = true;
        break;
      }
      if (hasMore) break;
    }
    if (hasMore) break;
  }
  return { rows, scanned, hasMore };
}

async function collectFileRecords(cfg, sourceUrl, offset, take) {
  const res = await fetch(sourceUrl);
  if (!res.ok) throw new Error(`parser_file_fetch_failed:${res.status}`);
  const nodeStream = toNodeReadable(res.body);
  if (!nodeStream) throw new Error('parser_file_stream_unavailable');
  const contentType = String(res.headers.get('content-type') || '').trim();
  const resolvedFormat = normalizeParserFormat(cfg.sourceFormat) === 'auto'
    ? detectFormatFromContentType(contentType) !== 'auto'
      ? detectFormatFromContentType(contentType)
      : detectFormatFromUrl(sourceUrl)
    : cfg.sourceFormat;
  return collectFromStreamByFormat(nodeStream, { ...cfg, sourceFormat: resolvedFormat }, offset, take);
}

async function collectRawRecords(client, cfg, options = {}) {
  const offset = Math.max(0, Math.trunc(Number(options?.cursor?.offset || 0)));
  const take = Math.max(1, Math.min(5000, Math.trunc(Number(options?.limit || cfg.batchSize || DEFAULT_BATCH_SIZE))));
  const inputEnvelope = options?.inputEnvelope || {};
  const inputValue = options?.inputValue;
  if (cfg.sourceMode === 'table') {
    const tableRows = await collectTableRecords(client, cfg, offset, take);
    return {
      source_type: 'table',
      source_ref: `${cfg.sourceSchema}.${cfg.sourceTable}`,
      rows: tableRows.rows,
      scanned: tableRows.scanned,
      hasMore: tableRows.hasMore,
      format: cfg.sourceColumn ? inferFormat(null, cfg.sourceFormat, cfg.sourceColumn) : 'row'
    };
  }
  if (cfg.sourceMode === 'file_url') {
    const sourceUrl = String(cfg.fileUrl || '').trim() || String(getByPath({ input: inputValue, envelope: inputEnvelope }, cfg.fileUrlPath) || '').trim();
    if (!sourceUrl) throw new Error('parser_file_url_missing');
    const fileRows = await collectFileRecords(cfg, sourceUrl, offset, take);
    return {
      source_type: 'file_url',
      source_ref: sourceUrl,
      rows: fileRows.rows,
      scanned: fileRows.scanned,
      hasMore: fileRows.hasMore,
      format: inferFormat(null, cfg.sourceFormat, sourceUrl)
    };
  }
  const candidate = candidateByInputPath(inputValue, inputEnvelope, cfg.inputPath);
  const inputRows = await collectInputRecords(candidate, cfg, offset, take);
  return {
    source_type: 'input',
    source_ref: 'upstream',
    rows: inputRows.rows,
    scanned: inputRows.scanned,
    hasMore: inputRows.hasMore,
    format: inferFormat(candidate, cfg.sourceFormat)
  };
}

export async function executeParserRows(client, rawSettings = {}, options = {}) {
  const cfg = normalizeParserSettings(rawSettings, options?.overrides || {});
  const limit = options?.preview ? cfg.previewLimit : cfg.batchSize;
  const raw = await collectRawRecords(client, cfg, {
    inputValue: options?.inputValue ?? cfg.sampleInput,
    inputEnvelope: options?.inputEnvelope || {},
    cursor: options?.cursor || {},
    limit
  });

  let rows = [];
  for (const record of raw.rows) {
    const transformed = applyParserTransform(record, cfg);
    if (!transformed) continue;
    rows.push(...transformed);
  }
  if (rows.length > limit + 1) rows = rows.slice(0, limit + 1);
  const transformedHasMore = rows.length > limit;
  if (transformedHasMore) rows = rows.slice(0, limit);
  rows = await applyLookupToRows(client, cfg, rows);

  const offset = Math.max(0, Math.trunc(Number(options?.cursor?.offset || 0)));
  const hasMore = Boolean(raw.hasMore || transformedHasMore);
  const nextOffset = offset + rows.length;

  return {
    rows,
    columns: columnsFromRows(rows),
    raw_rows: Array.isArray(raw.rows) ? raw.rows.slice(0, cfg.previewLimit) : [],
    raw_columns: columnsFromRows(raw.rows),
    source_type: raw.source_type,
    source_ref: raw.source_ref,
    source_format: raw.format,
    batch: {
      offset,
      batch_size: limit,
      returned_rows: rows.length,
      has_more: hasMore,
      next_cursor: hasMore ? { offset: nextOffset } : null
    },
    stats: {
      scanned_records: raw.scanned,
      parsed_rows: rows.length
    }
  };
}

export function parserPreviewSummary(result) {
  const rows = Array.isArray(result?.rows) ? result.rows : [];
  const rawRows = Array.isArray(result?.raw_rows) ? result.raw_rows : [];
  return {
    row_count: rows.length,
    column_count: columnsFromRows(rows).length,
    columns: columnsFromRows(rows),
    sample_rows: rows.slice(0, 10),
    raw_row_count: rawRows.length,
    raw_column_count: columnsFromRows(rawRows).length,
    raw_columns: columnsFromRows(rawRows),
    raw_sample_rows: rawRows.slice(0, 10),
    source_type: String(result?.source_type || '').trim(),
    source_ref: String(result?.source_ref || '').trim(),
    source_format: String(result?.source_format || '').trim(),
    batch: result?.batch || {},
    stats: result?.stats || {}
  };
}

export const parserRuntimeTestkit = {
  parseCsvList,
  parseJsonObjectSafe,
  parseJsonArraySafe,
  parsePathParts,
  getByPath,
  normalizeParserSettings,
  inferFormat,
  applyParserTransform,
  compareByOperator
};
