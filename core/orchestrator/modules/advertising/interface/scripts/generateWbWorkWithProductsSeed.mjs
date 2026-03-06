#!/usr/bin/env node

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const SOURCE_URL = 'https://dev.wildberries.ru/en/docs/openapi/work-with-products';
const SOURCE_SLUG = 'wb_openapi_work_with_products';
const TARGET_SCHEMA = 'system';
const TARGET_TABLE = 'api_configs_store';
const UPDATED_BY = 'wb_seed_work_with_products';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const outputDir = path.resolve(__dirname, '..', 'seeds');
const outputJsonPath = path.join(outputDir, 'wb_work_with_products_api_configs.json');
const outputSqlPath = path.join(outputDir, 'wb_work_with_products_api_configs.sql');

const METHOD_ORDER = ['get', 'post', 'put', 'patch', 'delete'];

function sqlLiteral(value) {
  return `'${String(value ?? '').replace(/'/g, "''")}'`;
}

function cleanText(value) {
  if (value == null) return '';
  return String(value)
    .replace(/<[^>]*>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function clampText(value, max = 600) {
  const text = cleanText(value);
  if (text.length <= max) return text;
  return `${text.slice(0, max - 1)}…`;
}

function toArray(value) {
  return Array.isArray(value) ? value : [];
}

function decodeNextChunks(html) {
  const chunks = [];
  const re = /self\.__next_f\.push\(\[(\d+),("(?:\\.|[^"\\])*")\]\)/g;
  let m;
  while ((m = re.exec(html)) !== null) {
    try {
      chunks.push({ idx: Number(m[1] || 0), text: JSON.parse(m[2]) });
    } catch {
      // skip broken chunks
    }
  }
  return chunks.map((c) => c.text).join('');
}

function extractJsonObject(source, marker) {
  const markerIndex = source.indexOf(marker);
  if (markerIndex < 0) return null;
  const start = source.indexOf('{', markerIndex + marker.length);
  if (start < 0) return null;
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let i = start; i < source.length; i += 1) {
    const ch = source[i];
    if (inString) {
      if (escaped) {
        escaped = false;
        continue;
      }
      if (ch === '\\') {
        escaped = true;
        continue;
      }
      if (ch === '"') {
        inString = false;
      }
      continue;
    }
    if (ch === '"') {
      inString = true;
      continue;
    }
    if (ch === '{') {
      depth += 1;
      continue;
    }
    if (ch === '}') {
      depth -= 1;
      if (depth === 0) {
        return source.slice(start, i + 1);
      }
    }
  }
  return null;
}

function dedupeParameters(parameters) {
  const out = [];
  const seen = new Set();
  for (const p of toArray(parameters)) {
    const name = String(p?.name || '').trim();
    const where = String(p?.in || '').trim().toLowerCase();
    if (!name || !where) continue;
    const key = `${where}:${name}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(p);
  }
  return out;
}

function getExampleFromExamples(examples) {
  const entries = Object.entries(examples || {});
  for (const [, item] of entries) {
    if (item && Object.prototype.hasOwnProperty.call(item, 'value')) return item.value;
  }
  return undefined;
}

function schemaTemplate(schema, depth = 0) {
  if (!schema || typeof schema !== 'object') return {};
  if (depth > 8) return {};
  if (Object.prototype.hasOwnProperty.call(schema, 'example')) return schema.example;
  if (Object.prototype.hasOwnProperty.call(schema, 'default')) return schema.default;
  if (Array.isArray(schema.enum) && schema.enum.length) return schema.enum[0];

  const variant = toArray(schema.oneOf)[0] || toArray(schema.anyOf)[0] || toArray(schema.allOf)[0];
  if (variant) return schemaTemplate(variant, depth + 1);

  const type = String(schema.type || '').toLowerCase();
  if (type === 'object' || schema.properties) {
    const obj = {};
    for (const [key, value] of Object.entries(schema.properties || {})) {
      obj[key] = schemaTemplate(value, depth + 1);
    }
    return obj;
  }
  if (type === 'array') {
    if (schema.items) return [schemaTemplate(schema.items, depth + 1)];
    return [];
  }
  if (type === 'integer' || type === 'number') return 0;
  if (type === 'boolean') return false;
  if (type === 'string') return '';
  return {};
}

function pickRequestBody(operation) {
  const content = operation?.requestBody?.content || {};
  const preferred = ['application/json', 'multipart/form-data', 'application/x-www-form-urlencoded', 'application/octet-stream'];
  const mediaType = preferred.find((x) => content[x]) || Object.keys(content)[0] || '';
  if (!mediaType) return { mediaType: '', template: {} };
  const media = content[mediaType] || {};
  if (Object.prototype.hasOwnProperty.call(media, 'example')) {
    return { mediaType, template: media.example };
  }
  if (media.examples && typeof media.examples === 'object') {
    const fromExamples = getExampleFromExamples(media.examples);
    if (fromExamples !== undefined) return { mediaType, template: fromExamples };
  }
  if (media.schema && typeof media.schema === 'object') {
    return { mediaType, template: schemaTemplate(media.schema) };
  }
  return { mediaType, template: {} };
}

function pickResponseExample(operation) {
  const responses = operation?.responses || {};
  const statusOrder = ['200', '201', '202', '204', '206', 'default'];
  const statuses = statusOrder.concat(Object.keys(responses).filter((s) => !statusOrder.includes(s)));
  for (const status of statuses) {
    const response = responses[status];
    if (!response || typeof response !== 'object') continue;
    const content = response.content || {};
    const mediaType = ['application/json', 'application/problem+json', ...Object.keys(content)].find((x) => content[x]);
    if (!mediaType || !content[mediaType]) {
      if (status === '204') return { status, mediaType: '', example: null };
      continue;
    }
    const media = content[mediaType];
    if (Object.prototype.hasOwnProperty.call(media, 'example')) {
      return { status, mediaType, example: media.example };
    }
    if (media.examples && typeof media.examples === 'object') {
      const fromExamples = getExampleFromExamples(media.examples);
      if (fromExamples !== undefined) return { status, mediaType, example: fromExamples };
    }
    if (media.schema && typeof media.schema === 'object') {
      return { status, mediaType, example: schemaTemplate(media.schema) };
    }
  }
  return { status: '', mediaType: '', example: {} };
}

function paramPlaceholder(param) {
  if (!param || typeof param !== 'object') return '';
  if (Object.prototype.hasOwnProperty.call(param, 'example')) return param.example;
  if (param.examples && typeof param.examples === 'object') {
    const fromExamples = getExampleFromExamples(param.examples);
    if (fromExamples !== undefined) return fromExamples;
  }
  if (Object.prototype.hasOwnProperty.call(param?.schema || {}, 'default')) return param.schema.default;
  if (Array.isArray(param?.schema?.enum) && param.schema.enum.length === 1) return param.schema.enum[0];
  return `{{${String(param.name || '').trim()}}}`;
}

function guessServerByPath(apiPath) {
  if (apiPath.startsWith('/content/') || apiPath.startsWith('/api/content/')) return 'https://content-api.wildberries.ru';
  if (apiPath.startsWith('/api/v2/')) return 'https://discounts-prices-api.wildberries.ru';
  if (apiPath.startsWith('/api/v3/')) return 'https://marketplace-api.wildberries.ru';
  return '';
}

function getServerUrl(spec, pathNode, operation, apiPath) {
  const fromOperation = toArray(operation?.servers)[0]?.url || '';
  const fromPath = toArray(pathNode?.servers)[0]?.url || '';
  const fromSpec = toArray(spec?.servers)[0]?.url || '';
  return String(fromOperation || fromPath || fromSpec || guessServerByPath(apiPath)).trim();
}

function getSecurity(spec, pathNode, operation) {
  const sec = operation?.security ?? pathNode?.security ?? spec?.security ?? [];
  return toArray(sec);
}

function hasHeaderApiKey(security) {
  for (const sec of security) {
    if (!sec || typeof sec !== 'object') continue;
    for (const key of Object.keys(sec)) {
      if (/headerapikey/i.test(key)) return true;
    }
  }
  return false;
}

function sanitizeApiName(name) {
  return cleanText(name).slice(0, 240);
}

function normalizeSummary(text, fallback) {
  const source = String(text || '').replace(/\{\{[\s\S]*?\}\}/g, ' ');
  const cleaned = cleanText(source);
  if (cleaned) return cleaned;
  return cleanText(fallback || '');
}

function normalizeDescription(text) {
  return cleanText(text).slice(0, 600);
}

function pathTemplate(apiPath) {
  return String(apiPath || '').replace(/\{([a-zA-Z0-9_]+)\}/g, '{{$1}}');
}

function pruneValue(value, depth = 0) {
  if (depth > 4) return {};
  if (value == null) return value;
  if (typeof value === 'string') {
    return value.length > 280 ? `${value.slice(0, 279)}…` : value;
  }
  if (typeof value === 'number' || typeof value === 'boolean') return value;
  if (Array.isArray(value)) {
    return value.slice(0, 5).map((x) => pruneValue(x, depth + 1));
  }
  if (typeof value === 'object') {
    const out = {};
    const entries = Object.entries(value).slice(0, 20);
    for (const [k, v] of entries) {
      out[k] = pruneValue(v, depth + 1);
    }
    return out;
  }
  return {};
}

function buildOperationRecords(spec) {
  const records = [];
  const paths = spec?.paths || {};
  for (const [apiPath, pathNodeRaw] of Object.entries(paths)) {
    const pathNode = pathNodeRaw && typeof pathNodeRaw === 'object' ? pathNodeRaw : {};
    const sharedParams = toArray(pathNode.parameters);
    for (const method of METHOD_ORDER) {
      const operation = pathNode[method];
      if (!operation || typeof operation !== 'object') continue;
      const methodUpper = method.toUpperCase();
      const summary = normalizeSummary(operation.summary, `${methodUpper} ${apiPath}`);
      const tag = String(toArray(operation.tags)[0] || 'Product Management').trim();
      const server = getServerUrl(spec, pathNode, operation, apiPath);
      const params = dedupeParameters([...sharedParams, ...toArray(operation.parameters)]);
      const queryParams = params.filter((p) => String(p?.in || '').toLowerCase() === 'query');
      const pathParams = params.filter((p) => String(p?.in || '').toLowerCase() === 'path');
      const queryJson = {};
      for (const p of queryParams) {
        const name = String(p?.name || '').trim();
        if (!name) continue;
        queryJson[name] = paramPlaceholder(p);
      }
      const bodyPicked = pickRequestBody(operation);
      const responsePicked = pickResponseExample(operation);
      const security = getSecurity(spec, pathNode, operation);
      const securityNames = [];
      for (const sec of security) {
        if (!sec || typeof sec !== 'object') continue;
        for (const key of Object.keys(sec)) {
          if (!securityNames.includes(key)) securityNames.push(key);
        }
      }
      const needsHeaderToken = hasHeaderApiKey(security);
      const headersJson = needsHeaderToken ? { Authorization: '{{token}}' } : {};

      const paginationJson = {
        enabled: false,
        strategy: 'none',
        target: 'body',
        data_path: '',
        page_param: 'page',
        start_page: 1,
        limit_param: 'limit',
        limit_value: 100,
        cursor_req_path_1: '',
        cursor_req_path_2: '',
        cursor_res_path_1: '',
        cursor_res_path_2: '',
        next_url_path: 'next',
        use_max_pages: true,
        max_pages: 3,
        use_delay: false,
        delay_ms: 0,
        stop_conditions: {
          on_missing_pagination_value: true,
          on_same_response: true,
          same_response_limit: 5,
          on_http_error: true,
          response_rules: []
        },
        custom_strategy: '',
        parameters: []
      };

      const dataModelJson = {
        tables: [],
        joins: [],
        fields: [],
        filters: [],
        date_parameters: [],
        api_parameters: []
      };

      const executionJson = {
        dispatch_mode: 'single',
        execution_mode: 'sync',
        sync_planner: 'entity_to_stop',
        async_concurrency: 3,
        execution_delay_ms: 0,
        response_log: { enabled: false, schema: '', table: '' },
        group_by_aliases: [],
        body_items_path: 'items',
        preview_request_limit: 5,
        data_model: dataModelJson,
        binding_rules: []
      };

      const mappingJson = {
        source: {
          provider: 'wildberries',
          catalog: 'openapi',
          section: 'work-with-products',
          source_url: SOURCE_URL,
          source_slug: SOURCE_SLUG
        },
        tag,
        auth: {
          required: securityNames.length > 0,
          schemes: securityNames
        },
        parameters: {
          path: pathParams.map((p) => ({
            name: String(p?.name || ''),
            required: Boolean(p?.required),
            schema_type: String(p?.schema?.type || '')
          })),
          query: queryParams.map((p) => ({
            name: String(p?.name || ''),
            required: Boolean(p?.required),
            schema_type: String(p?.schema?.type || '')
          }))
        },
        request_body_media_type: bodyPicked.mediaType,
        response_example_status: responsePicked.status,
        response_example_media_type: responsePicked.mediaType
      };

      const pTemplate = pathTemplate(apiPath);
      const name = sanitizeApiName(`WB Work with Products :: ${summary} (${methodUpper} ${apiPath})`);
      const fullDescription = normalizeDescription(`${tag}. ${summary}. Source: ${SOURCE_URL}`);

      const configJson = {
        api_name: name,
        method: methodUpper,
        base_url: server,
        path: pTemplate,
        headers_json: headersJson,
        query_json: queryJson,
        body_json:
          bodyPicked.template && typeof bodyPicked.template === 'object'
            ? pruneValue(bodyPicked.template)
            : {},
        pagination_json: paginationJson,
        execution_json: executionJson,
        mapping_json: mappingJson,
        auth_mode: 'manual',
        auth_json: {},
        parameter_definitions: [],
        response_targets: [],
        picked_paths: [],
        description: fullDescription,
        is_active: true
      };

      records.push({
        source_url: SOURCE_URL,
        tag,
        method: methodUpper,
        path: apiPath,
        path_template: pTemplate,
        base_url: server,
        api_name: name,
        description: fullDescription,
        auth_required: securityNames.length > 0,
        auth_schemes: securityNames,
        query_parameters: queryParams.map((p) => ({
          name: String(p?.name || ''),
          required: Boolean(p?.required),
          description: clampText(p?.description || '', 220),
          schema_type: String(p?.schema?.type || '')
        })),
        path_parameters: pathParams.map((p) => ({
          name: String(p?.name || ''),
          required: Boolean(p?.required),
          description: clampText(p?.description || '', 220),
          schema_type: String(p?.schema?.type || '')
        })),
        request_body_media_type: bodyPicked.mediaType,
        request_body_template: configJson.body_json,
        response_example_status: responsePicked.status,
        response_example_media_type: responsePicked.mediaType,
        response_example: pruneValue(responsePicked.example),
        row_payload: {
          api_name: name,
          description: fullDescription,
          schema_version: 1,
          is_active: true,
          updated_by: UPDATED_BY,
          config_json: configJson
        }
      });
    }
  }

  records.sort((a, b) => {
    const ak = `${a.base_url}|${a.path}|${a.method}`;
    const bk = `${b.base_url}|${b.path}|${b.method}`;
    return ak.localeCompare(bk);
  });
  return records;
}

function buildSql(records) {
  const qn = `"${TARGET_SCHEMA}"."${TARGET_TABLE}"`;
  const seedRows = records.map((rec) => ({
    api_name: rec.row_payload.api_name,
    description: rec.row_payload.description,
    config_json: rec.row_payload.config_json
  }));
  const payload = JSON.stringify(seedRows);
  const parts = [];
  parts.push('-- Auto-generated seed for Wildberries "work-with-products"');
  parts.push(`-- Source: ${SOURCE_URL}`);
  parts.push(`-- Generated at: ${new Date().toISOString()}`);
  parts.push(`-- Target: ${TARGET_SCHEMA}.${TARGET_TABLE}`);
  parts.push(`-- Endpoints: ${seedRows.length}`);
  parts.push('');
  parts.push(`DO $$`);
  parts.push(`DECLARE`);
  parts.push(`  rec jsonb;`);
  parts.push(`  matched_id bigint;`);
  parts.push(`BEGIN`);
  parts.push(`  FOR rec IN SELECT value FROM jsonb_array_elements(${sqlLiteral(payload)}::jsonb)`);
  parts.push(`  LOOP`);
  parts.push(`    SELECT t.id INTO matched_id`);
  parts.push(`    FROM ${qn} t`);
  parts.push(`    WHERE UPPER(COALESCE(t.config_json->>'method', '')) = UPPER(COALESCE(rec->'config_json'->>'method', ''))`);
  parts.push(`      AND COALESCE(t.config_json->>'base_url', '') = COALESCE(rec->'config_json'->>'base_url', '')`);
  parts.push(`      AND COALESCE(t.config_json->>'path', '') = COALESCE(rec->'config_json'->>'path', '')`);
  parts.push(`    ORDER BY t.updated_at DESC NULLS LAST, t.id DESC`);
  parts.push(`    LIMIT 1;`);
  parts.push(``);
  parts.push(`    IF matched_id IS NULL THEN`);
  parts.push(`      INSERT INTO ${qn}`);
  parts.push(`        (api_name, config_json, schema_version, revision, description, is_active, updated_at, updated_by)`);
  parts.push(`      VALUES`);
  parts.push(`        (`);
  parts.push(`          rec->>'api_name',`);
  parts.push(`          rec->'config_json',`);
  parts.push(`          1,`);
  parts.push(`          1,`);
  parts.push(`          rec->>'description',`);
  parts.push(`          true,`);
  parts.push(`          now(),`);
  parts.push(`          ${sqlLiteral(UPDATED_BY)}`);
  parts.push(`        );`);
  parts.push(`    ELSE`);
  parts.push(`      UPDATE ${qn}`);
  parts.push(`      SET`);
  parts.push(`        api_name = rec->>'api_name',`);
  parts.push(`        config_json = rec->'config_json',`);
  parts.push(`        schema_version = 1,`);
  parts.push(`        revision = CASE WHEN COALESCE(revision, 0) < 1 THEN 1 ELSE revision + 1 END,`);
  parts.push(`        description = rec->>'description',`);
  parts.push(`        is_active = true,`);
  parts.push(`        updated_at = now(),`);
  parts.push(`        updated_by = ${sqlLiteral(UPDATED_BY)}`);
  parts.push(`      WHERE id = matched_id;`);
  parts.push(`    END IF;`);
  parts.push(`  END LOOP;`);
  parts.push(`END $$;`);
  parts.push('');
  return parts.join('\n');
}

async function main() {
  const res = await fetch(SOURCE_URL);
  if (!res.ok) {
    throw new Error(`Failed to fetch source docs: ${res.status} ${res.statusText}`);
  }
  const html = await res.text();
  const joined = decodeNextChunks(html);
  const redocObjectText = extractJsonObject(joined, 'const __redoc_state = ');
  if (!redocObjectText) {
    throw new Error('Cannot extract __redoc_state from docs page');
  }
  const redoc = JSON.parse(redocObjectText);
  const spec = redoc?.spec?.data || {};
  const records = buildOperationRecords(spec);
  const payload = {
    generated_at: new Date().toISOString(),
    source_url: SOURCE_URL,
    source_slug: SOURCE_SLUG,
    target_table: `${TARGET_SCHEMA}.${TARGET_TABLE}`,
    total_endpoints: records.length,
    endpoints: records
  };
  const sql = buildSql(records);
  await fs.mkdir(outputDir, { recursive: true });
  await fs.writeFile(outputJsonPath, JSON.stringify(payload, null, 2), 'utf8');
  await fs.writeFile(outputSqlPath, sql, 'utf8');
  console.log(
    JSON.stringify(
      {
        ok: true,
        total_endpoints: records.length,
        output_json: outputJsonPath,
        output_sql: outputSqlPath
      },
      null,
      2
    )
  );
}

main().catch((error) => {
  console.error(String(error?.stack || error));
  process.exitCode = 1;
});
