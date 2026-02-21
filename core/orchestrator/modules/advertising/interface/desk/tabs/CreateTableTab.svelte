<!-- File: core/orchestrator/modules/advertising/interface/desk/tabs/CreateTableTab.svelte -->
<script lang="ts">
  import { onMount } from 'svelte';
  export type Role = 'viewer' | 'operator' | 'data_admin';

  export let apiBase: string;
  export let role: Role;
  export let loading: boolean;
  export let dbStatus: 'checking' | 'ok' | 'error' = 'checking';
  export let dbStatusMessage = '';

  export let refreshTables: () => Promise<void>;
  export let onCreated: (schema: string, table: string) => void;

  export let headers: () => Record<string, string>;
  export let apiJson: <T = any>(url: string, init?: RequestInit) => Promise<T>;

  let error = '';

  let schema_name = '';
  let table_name = '';
  let description = '';
  let table_class = 'custom';

  const typeOptions = ['text', 'int', 'bigint', 'numeric', 'boolean', 'date', 'timestamptz', 'jsonb', 'uuid'];
  const CREATE_TIMEOUT_MS = 30000;
  const CREATE_BUTTON_LABEL = '\u0421\u043e\u0437\u0434\u0430\u0442\u044c \u0442\u0430\u0431\u043b\u0438\u0446\u0443';
  const CREATE_WAIT_LABEL = '\u0417\u0430\u043f\u0440\u043e\u0441 \u0441\u043e\u0437\u0434\u0430\u043d\u0438\u044f \u043e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d. \u041e\u0436\u0438\u0434\u0430\u0435\u043c \u043e\u0442\u0432\u0435\u0442 \u0441\u0435\u0440\u0432\u0435\u0440\u0430...';
  const DEFAULT_DB_STATUS_LABEL = '\u0421\u0442\u0430\u0442\u0443\u0441 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u044f \u043a \u0431\u0430\u0437\u0435: \u043d\u0435\u0442 \u0434\u0430\u043d\u043d\u044b\u0445.';

  type ColumnDef = { field_name: string; field_type: string; description?: string };
  let columns: ColumnDef[] = [{ field_name: '', field_type: 'text', description: '' }];

  let partition_enabled = false;
  let partition_column = 'event_date';
  let partition_interval: 'day' | 'month' = 'day';

  let result_modal_open = false;
  let result_modal_title = '';
  let result_modal_text = '';
  let result_created_schema = '';
  let result_created_table = '';
  let result_is_success = false;
  let creating = false;
  const TABLE_TEMPLATES_KEY = 'ao_create_table_templates_v1';
  type TableTemplate = {
    id: string;
    name: string;
    schema_name: string;
    table_name: string;
    table_class: string;
    description: string;
    columns: ColumnDef[];
    partition_enabled: boolean;
    partition_column: string;
    partition_interval: 'day' | 'month';
  };
  let tableTemplates: TableTemplate[] = [];
  let selectedTemplateId = '';
  let templateNameDraft = '';

  function canWrite(): boolean {
    return role === 'data_admin';
  }

  function pickTemplateBronze() {
    schema_name = 'bronze';
    table_name = 'wb_ads_raw1';
    table_class = 'bronze_raw';
    description = 'РЎС‹СЂС‹Рµ РѕС‚РІРµС‚С‹ API (append-only JSON)';
    columns = [
      { field_name: 'dataset', field_type: 'text', description: 'dataset name' },
      { field_name: 'endpoint', field_type: 'text', description: 'endpoint name' },
      { field_name: 'request_id', field_type: 'text', description: 'request id' },
      { field_name: 'ingested_at', field_type: 'timestamptz', description: 'ingest timestamp' },
      { field_name: 'payload', field_type: 'jsonb', description: 'raw payload' }
    ];
    partition_enabled = true;
    partition_column = 'ingested_at';
    partition_interval = 'day';
    selectedTemplateId = 'builtin_bronze';
    templateNameDraft = 'Bronze';
  }


  function pickTemplateSilver() {
    schema_name = 'silver_adv';
    table_name = 'wb_ads_daily';
    table_class = 'silver_table';
    description = 'Р”РЅРµРІРЅР°СЏ Р°РіСЂРµРіРёСЂРѕРІР°РЅРЅР°СЏ С‚Р°Р±Р»РёС†Р° СЂРµРєР»Р°РјС‹';
    columns = [
      { field_name: 'event_date', field_type: 'date', description: 'РґР°С‚Р° РјРµС‚СЂРёРєРё' },
      { field_name: 'campaign_id', field_type: 'text', description: 'РёРґРµРЅС‚РёС„РёРєР°С‚РѕСЂ РєР°РјРїР°РЅРёРё' },
      { field_name: 'impressions', field_type: 'bigint', description: 'РїРѕРєР°Р·С‹' },
      { field_name: 'clicks', field_type: 'bigint', description: 'РєР»РёРєРё' },
      { field_name: 'spend', field_type: 'numeric', description: 'СЂР°СЃС…РѕРґ' }
    ];
    partition_enabled = true;
    partition_column = 'event_date';
    partition_interval = 'day';
    selectedTemplateId = 'builtin_silver';
    templateNameDraft = 'Silver';
  }
  function uid() {
    return `${Date.now()}_${Math.random().toString(16).slice(2)}`;
  }

  function currentTableTemplate(name: string, id?: string): TableTemplate {
    return {
      id: id || uid(),
      name,
      schema_name: schema_name.trim(),
      table_name: table_name.trim(),
      table_class: table_class.trim() || 'custom',
      description: description.trim(),
      columns: normalizeColumns(columns),
      partition_enabled,
      partition_column: partition_column.trim(),
      partition_interval
    };
  }

  function getBuiltinTemplates(): TableTemplate[] {
    return [
      {
        id: 'builtin_bronze',
        name: 'Bronze',
        schema_name: 'bronze',
        table_name: 'wb_ads_raw1',
        table_class: 'bronze_raw',
        description: 'Raw API payload',
        columns: [
          { field_name: 'dataset', field_type: 'text', description: 'dataset name' },
          { field_name: 'endpoint', field_type: 'text', description: 'endpoint name' },
          { field_name: 'request_id', field_type: 'text', description: 'request id' },
          { field_name: 'ingested_at', field_type: 'timestamptz', description: 'ingest timestamp' },
          { field_name: 'payload', field_type: 'jsonb', description: 'raw payload' }
        ],
        partition_enabled: true,
        partition_column: 'ingested_at',
        partition_interval: 'day'
      },
      {
        id: 'builtin_silver',
        name: 'Silver',
        schema_name: 'silver_adv',
        table_name: 'wb_ads_daily',
        table_class: 'silver_table',
        description: 'Daily aggregated ads table',
        columns: [
          { field_name: 'event_date', field_type: 'date', description: 'date' },
          { field_name: 'campaign_id', field_type: 'text', description: 'campaign id' },
          { field_name: 'impressions', field_type: 'bigint', description: 'impressions' },
          { field_name: 'clicks', field_type: 'bigint', description: 'clicks' },
          { field_name: 'spend', field_type: 'numeric', description: 'spend' }
        ],
        partition_enabled: true,
        partition_column: 'event_date',
        partition_interval: 'day'
      }
    ];
  }

  function applyTemplate(t: TableTemplate) {
    schema_name = t.schema_name;
    table_name = t.table_name;
    table_class = t.table_class;
    description = t.description;
    columns = (t.columns || []).map((c) => ({ ...c }));
    partition_enabled = !!t.partition_enabled;
    partition_column = t.partition_column || 'event_date';
    partition_interval = t.partition_interval || 'day';
  }

  function loadTableTemplates() {
    try {
      const raw = JSON.parse(localStorage.getItem(TABLE_TEMPLATES_KEY) || '[]');
      const custom = Array.isArray(raw)
        ? raw.map((x: any) => ({
            id: String(x?.id || uid()),
            name: String(x?.name || ''),
            schema_name: String(x?.schema_name || ''),
            table_name: String(x?.table_name || ''),
            table_class: String(x?.table_class || 'custom'),
            description: String(x?.description || ''),
            columns: Array.isArray(x?.columns)
              ? x.columns.map((c: any) => ({
                  field_name: String(c?.field_name || ''),
                  field_type: String(c?.field_type || 'text'),
                  description: String(c?.description || '')
                }))
              : [],
            partition_enabled: Boolean(x?.partition_enabled),
            partition_column: String(x?.partition_column || 'event_date'),
            partition_interval: x?.partition_interval === 'month' ? 'month' : 'day'
          }))
        : [];
      tableTemplates = [...getBuiltinTemplates(), ...custom];
    } catch {
      tableTemplates = getBuiltinTemplates();
    }
  }

  function saveTableTemplates() {
    const custom = tableTemplates.filter((t) => !t.id.startsWith('builtin_'));
    localStorage.setItem(TABLE_TEMPLATES_KEY, JSON.stringify(custom.slice(0, 300)));
  }

  function applySelectedTemplate(id: string) {
    selectedTemplateId = id;
    const t = tableTemplates.find((x) => x.id === id);
    if (!t) return;
    templateNameDraft = t.name;
    applyTemplate(t);
  }

  function startNewTemplate() {
    selectedTemplateId = '';
    templateNameDraft = '';
    error = '';
  }

  function saveCurrentTemplate() {
    const name = String(templateNameDraft || '').trim();
    const cols = normalizeColumns(columns);
    if (!name) throw new Error('РЈРєР°Р¶Рё РЅР°Р·РІР°РЅРёРµ С€Р°Р±Р»РѕРЅР°');
    if (!cols.length) throw new Error('Р”РѕР±Р°РІСЊ С…РѕС‚СЏ Р±С‹ РѕРґРЅРѕ РїРѕР»Рµ');
    const idxById = selectedTemplateId ? tableTemplates.findIndex((x) => x.id === selectedTemplateId) : -1;
    const idxByName = idxById < 0 ? tableTemplates.findIndex((x) => x.name === name) : -1;
    const idx = idxById >= 0 ? idxById : idxByName;
    const next = currentTableTemplate(name, idx >= 0 ? tableTemplates[idx].id : undefined);
    if (idx >= 0) tableTemplates[idx] = next;
    else tableTemplates = [next, ...tableTemplates];
    selectedTemplateId = next.id;
    templateNameDraft = next.name;
    saveTableTemplates();
    error = '';
  }

  function deleteCurrentTemplate() {
    if (!selectedTemplateId) throw new Error('РЎРЅР°С‡Р°Р»Р° РІС‹Р±РµСЂРё С€Р°Р±Р»РѕРЅ');
    if (selectedTemplateId.startsWith('builtin_')) throw new Error('Р’СЃС‚СЂРѕРµРЅРЅС‹Р№ С€Р°Р±Р»РѕРЅ СѓРґР°Р»РёС‚СЊ РЅРµР»СЊР·СЏ');
    tableTemplates = tableTemplates.filter((x) => x.id !== selectedTemplateId);
    selectedTemplateId = '';
    templateNameDraft = '';
    saveTableTemplates();
    error = '';
  }

  function onSaveTemplateClick() {
    try {
      saveCurrentTemplate();
    } catch (e: any) {
      error = e?.message || String(e);
    }
  }

  function onDeleteTemplateClick() {
    try {
      deleteCurrentTemplate();
    } catch (e: any) {
      error = e?.message || String(e);
    }
  }
  function addField() {
    columns = [...columns, { field_name: '', field_type: 'text', description: '' }];
  }

  function removeField(ix: number) {
    columns = columns.filter((_, i) => i !== ix);
    if (columns.length === 0) columns = [{ field_name: '', field_type: 'text', description: '' }];
  }

  function normalizeColumns(cols: ColumnDef[]) {
    return cols
      .map((c) => ({
        field_name: (c.field_name || '').trim(),
        field_type: (c.field_type || '').trim(),
        description: (c.description || '').trim()
      }))
      .filter((c) => c.field_name.length > 0);
  }

  function validate() {
    if (!schema_name.trim()) throw new Error('РЈРєР°Р¶Рё СЃС…РµРјСѓ');
    if (!table_name.trim()) throw new Error('РЈРєР°Р¶Рё РёРјСЏ С‚Р°Р±Р»РёС†С‹');
    if (!table_class.trim()) throw new Error('РЈРєР°Р¶Рё РєР»Р°СЃСЃ');
    const cols = normalizeColumns(columns);
    if (cols.length === 0) throw new Error('Р”РѕР±Р°РІСЊ С…РѕС‚СЏ Р±С‹ РѕРґРЅРѕ РїРѕР»Рµ');
    for (const c of cols) if (!c.field_type) throw new Error("РЈРєР°Р¶Рё С‚РёРї РґР»СЏ РєР°Р¶РґРѕРіРѕ РїРѕР»СЏ");

    if (partition_enabled) {
      if (!partition_column.trim()) throw new Error('РџР°СЂС‚РёС†РёРѕРЅРёСЂРѕРІР°РЅРёРµ РІРєР»СЋС‡РµРЅРѕ: СѓРєР°Р¶Рё РєРѕР»РѕРЅРєСѓ');
      if (!partition_interval) throw new Error('РџР°СЂС‚РёС†РёРѕРЅРёСЂРѕРІР°РЅРёРµ РІРєР»СЋС‡РµРЅРѕ: СѓРєР°Р¶Рё РёРЅС‚РµСЂРІР°Р»');
    }
  }

  async function createTableNow() {
    error = '';
    creating = true;
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), CREATE_TIMEOUT_MS);
    try {
      if (!canWrite()) throw new Error('РќРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РїСЂР°РІ (РЅСѓР¶РЅР° СЂРѕР»СЊ data_admin)');

      validate();
      const cols = normalizeColumns(columns);

      const response = await apiJson(`${apiBase}/tables/create`, {
        method: 'POST',
        headers: headers(),
        signal: controller.signal,
        body: JSON.stringify({
          schema_name: schema_name.trim(),
          table_name: table_name.trim(),
          table_class,
          description: description.trim(),
          columns: cols,
          partitioning: partition_enabled
            ? { enabled: true, column: partition_column.trim(), interval: partition_interval }
            : { enabled: false }
        })
      });

      result_created_schema = schema_name.trim();
      result_created_table = table_name.trim();
      result_is_success = true;
      result_modal_title = 'РўР°Р±Р»РёС†Р° СЃРѕР·РґР°РЅР°';
      result_modal_text = response ? JSON.stringify(response, null, 2) : 'РћРїРµСЂР°С†РёСЏ РІС‹РїРѕР»РЅРµРЅР° СѓСЃРїРµС€РЅРѕ.';
      result_modal_open = true;

      refreshTables().catch(() => {
        // РћС€РёР±РєР° РѕР±РЅРѕРІР»РµРЅРёСЏ СЃРїРёСЃРєР° РЅРµ РґРѕР»Р¶РЅР° Р±Р»РѕРєРёСЂРѕРІР°С‚СЊ СЂРµР·СѓР»СЊС‚Р°С‚ СЃРѕР·РґР°РЅРёСЏ
      });
    } catch (e: any) {
      error = e?.name === 'AbortError'
        ? 'РЎРµСЂРІРµСЂ РЅРµ РѕС‚РІРµС‚РёР» РІРѕРІСЂРµРјСЏ. РџСЂРѕРІРµСЂСЊС‚Рµ СЃС‚Р°С‚СѓСЃ Р±Р°Р·С‹ Рё РїРѕРІС‚РѕСЂРёС‚Рµ РїРѕРїС‹С‚РєСѓ.'
        : (e?.message ?? String(e));
      result_is_success = false;
      result_modal_title = 'РћС€РёР±РєР° СЃРѕР·РґР°РЅРёСЏ';
      result_modal_text = error;
      result_modal_open = true;
    } finally {
      clearTimeout(timeoutId);
      creating = false;
    }
  }

  function closeResultModal() {
    result_modal_open = false;
    if (result_is_success && result_created_schema && result_created_table) {
      onCreated?.(result_created_schema, result_created_table);
    }
  }

  onMount(() => {
    loadTableTemplates();
  });
</script>

{#if error}
  <div class="alert">
    <div class="alert-title">РћС€РёР±РєР° СЃРѕР·РґР°РЅРёСЏ</div>
    <pre>{error}</pre>
  </div>
{/if}

<section class="grid single">
  <div class="panel">
    <div class="panel-head">
      <h2>РЎРѕР·РґР°С‚СЊ С‚Р°Р±Р»РёС†Сѓ</h2>
      <div class="quick">
        <select value={selectedTemplateId} on:change={(e) => applySelectedTemplate(e.currentTarget.value)}>
          <option value="">Шаблон таблицы</option>
          {#each tableTemplates as t}
            <option value={t.id}>{t.name}</option>
          {/each}
        </select>
        <input class="template-name" bind:value={templateNameDraft} placeholder="Название шаблона" />
        <button on:click={startNewTemplate}>Добавить шаблон</button>
        <button on:click={onSaveTemplateClick}>Сохранить шаблон</button>
        <button class="danger icon-btn" on:click={onDeleteTemplateClick}>x</button>
        <button on:click={pickTemplateBronze}>Bronze</button>
        <button on:click={pickTemplateSilver}>Silver</button>
        <button on:click={refreshTables} disabled={loading}>{loading ? 'Р—Р°РіСЂСѓР·РєР°вЂ¦' : 'РћР±РЅРѕРІРёС‚СЊ СЃРїРёСЃРѕРє'}</button>
      </div>
    </div>

    <div class="form">
      <label>
        РЎС…РµРјР°
        <input bind:value={schema_name} placeholder="РЅР°РїСЂРёРјРµСЂ: showcase / bronze / silver_adv" />
      </label>

      <label>
        РўР°Р±Р»РёС†Р°
        <input bind:value={table_name} placeholder="РЅР°РїСЂРёРјРµСЂ: advertising" />
      </label>

      <label>
        РљР»Р°СЃСЃ (РґР»СЏ СЃРµР±СЏ)
        <select bind:value={table_class}>
          <option value="custom">custom</option>
          <option value="bronze_raw">bronze_raw</option>
          <option value="silver_table">silver_table</option>
          <option value="showcase_table">showcase_table</option>
        </select>
      </label>

      <label>
        РћРїРёСЃР°РЅРёРµ
        <input bind:value={description} placeholder="С‡С‚Рѕ СЌС‚Рѕ Р·Р° С‚Р°Р±Р»РёС†Р°" />
      </label>
    </div>

    <div class="fields">
      <div class="fields-head">
        <h3>РџРѕР»СЏ</h3>
      </div>

      {#each columns as c, ix}
        <div class="field-row">
          <input placeholder="РёРјСЏ РїРѕР»СЏ" bind:value={c.field_name} />
          <select bind:value={c.field_type}>
            {#each typeOptions as t}
              <option value={t}>{t}</option>
            {/each}
          </select>
          <input placeholder="РѕРїРёСЃР°РЅРёРµ" bind:value={c.description} />
          <button class="danger icon-btn" on:click={() => removeField(ix)} title="РЈРґР°Р»РёС‚СЊ РїРѕР»Рµ">x</button>
        </div>
      {/each}

      <div class="fields-footer">
        <button on:click={addField}>+ Р”РѕР±Р°РІРёС‚СЊ РїРѕР»Рµ</button>
      </div>
    </div>

    <div class="panel2">
      <h3>РџР°СЂС‚РёС†РёРѕРЅРёСЂРѕРІР°РЅРёРµ</h3>
      <label class="row">
        <input type="checkbox" bind:checked={partition_enabled} />
        <span>Р’РєР»СЋС‡РёС‚СЊ РїР°СЂС‚РёС†РёРё</span>
      </label>

      {#if partition_enabled}
        <div class="form">
          <label>
            РљРѕР»РѕРЅРєР° РґР»СЏ РїР°СЂС‚РёС†РёР№
            <input bind:value={partition_column} placeholder="event_date / ingested_at / ..." />
          </label>
          <label>
            РРЅС‚РµСЂРІР°Р»
            <select bind:value={partition_interval}>
              <option value="day">day</option>
              <option value="month">month</option>
            </select>
          </label>
        </div>
      {/if}
    </div>

    <div class="actions">
      <button class="primary" on:click={createTableNow} disabled={loading || creating || !canWrite()}>
        {CREATE_BUTTON_LABEL}
      </button>
      {#if creating}
        <span class="hint">{CREATE_WAIT_LABEL}</span>
      {/if}
    </div>

    <div class="statusline" class:ok={dbStatus === 'ok'} class:error={dbStatus === 'error'}>
      {dbStatusMessage || DEFAULT_DB_STATUS_LABEL}
    </div>

    {#if !canWrite()}
      <p class="hint">РљРЅРѕРїРєР° Р°РєС‚РёРІРЅР° С‚РѕР»СЊРєРѕ РїСЂРё СЂРѕР»Рё <b>data_admin</b>.</p>
    {/if}
  </div>
</section>

{#if result_modal_open}
  <div
    class="modal-bg"
    role="button"
    tabindex="0"
    aria-label="Р—Р°РєСЂС‹С‚СЊ РѕРєРЅРѕ СЂРµР·СѓР»СЊС‚Р°С‚Р°"
    on:click={closeResultModal}
    on:keydown={(e) => (e.key === 'Escape' || e.key === 'Enter' || e.key === ' ') && closeResultModal()}
  >
    <div class="modal" on:click|stopPropagation on:keydown|stopPropagation>
      <h3>{result_modal_title}</h3>
      <pre>{result_modal_text}</pre>
      <div class="modal-actions">
        <button class="primary" on:click={closeResultModal}>OK</button>
      </div>
    </div>
  </div>
{/if}

<style>
  .grid { display:grid; gap: 12px; margin-top: 12px; }
  .grid.single { grid-template-columns: 1fr; }

  .panel { background:#fff; border:1px solid #e6eaf2; border-radius:18px; padding:14px; box-shadow:0 6px 20px rgba(15,23,42,.05); }
  .panel-head { display:flex; align-items:center; justify-content:space-between; gap:12px; }
  .panel-head h2 { margin:0; font-size:18px; }
  .quick { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
  .quick select, .quick input { border-radius:14px; border:1px solid #e6eaf2; padding:10px 12px; background:#fff; }
  .quick .template-name { min-width: 220px; }

  .form { display:grid; grid-template-columns: 1fr 1fr; gap:10px; margin-top:12px; }
  @media (max-width: 1100px) { .form { grid-template-columns: 1fr; } }
  .form label { display:flex; flex-direction:column; gap:6px; font-size:13px; }
  .form input, .form select { border-radius:14px; border:1px solid #e6eaf2; padding:10px 12px; outline:none; background:#fff; }

  .fields { margin-top:14px; border-top:1px dashed #e6eaf2; padding-top:14px; }
  .fields-head h3 { margin:0; font-size:16px; }
  .field-row { display:grid; grid-template-columns: 1.2fr .8fr 1.6fr auto; gap:8px; margin-top:10px; }
  @media (max-width: 1100px) { .field-row { grid-template-columns: 1fr; } }
  .field-row input, .field-row select { border-radius:14px; border:1px solid #e6eaf2; padding:10px 12px; }
  .fields-footer { margin-top:12px; }

  .panel2 { margin-top:14px; border-top:1px dashed #e6eaf2; padding-top:14px; }
  .panel2 h3 { margin:0 0 10px 0; font-size:16px; }
  .row { display:flex; align-items:center; gap:10px; }

  .actions { margin-top:14px; display:flex; gap:10px; align-items:center; }

  button { border-radius:14px; border:1px solid #e6eaf2; padding:10px 12px; background:#fff; cursor:pointer; }
  button:disabled { opacity:.6; cursor:not-allowed; }
  .primary { background:#0f172a; color:#fff; border-color:#0f172a; }
  .danger { border-color:#f3c0c0; color:#b91c1c; }
  .icon-btn { width:44px; min-width:44px; padding:10px 0; text-transform:uppercase; }

  .hint { margin:10px 0 0; color:#64748b; font-size:13px; }
  .statusline { margin-top:12px; border:1px solid #e6eaf2; border-radius:12px; padding:10px 12px; background:#f8fafc; color:#334155; font-size:13px; }
  .statusline.ok { border-color:#bbf7d0; background:#f0fdf4; color:#166534; }
  .statusline.error { border-color:#fecaca; background:#fef2f2; color:#991b1b; }

  .alert { margin: 12px 0; padding: 10px 12px; border-radius: 14px; border: 1px solid #f3c0c0; background: #fff5f5; }
  .alert-title { font-weight: 700; margin-bottom: 6px; }
  pre { margin:0; white-space: pre-wrap; word-break: break-word; }
  .modal-bg { position: fixed; inset: 0; background: rgba(15,23,42,.35); display:flex; align-items:center; justify-content:center; padding: 18px; z-index: 1000; }
  .modal { width: min(560px, 100%); background:#fff; border-radius:18px; border:1px solid #e6eaf2; padding:14px; box-shadow:0 20px 60px rgba(15,23,42,.25); }
  .modal h3 { margin: 0 0 10px 0; }
  .modal-actions { display:flex; justify-content:flex-end; margin-top:12px; }
</style>

