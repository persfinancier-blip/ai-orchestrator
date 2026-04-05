<script lang="ts">
  import { onMount } from 'svelte';
  import { normalizeGoldDefinition } from '../../shared/goldDefinitionCore.mjs';

  export type Role = 'viewer' | 'operator' | 'data_admin';

  export let apiBase: string;
  export let role: Role;
  export let loading: boolean;
  export let headers: () => Record<string, string>;
  export let apiJson: <T = any>(url: string, init?: RequestInit) => Promise<T>;

  type GoldField = { name?: string; field_name?: string; type?: string; field_type?: string; description?: string; path?: string };
  type GoldSource = any;

  const FILTER_OPERATORS = [
    { value: 'eq', label: '=' },
    { value: 'ne', label: '!=' },
    { value: 'gt', label: '>' },
    { value: 'gte', label: '>=' },
    { value: 'lt', label: '<' },
    { value: 'lte', label: '<=' },
    { value: 'contains', label: 'содержит' },
    { value: 'not_contains', label: 'не содержит' },
    { value: 'is_empty', label: 'пусто' },
    { value: 'not_empty', label: 'не пусто' },
    { value: 'in', label: 'в списке' }
  ];

  const AGGREGATORS = [
    { value: 'sum', label: 'Сумма' },
    { value: 'count', label: 'Количество' },
    { value: 'count_distinct', label: 'Уникальные' },
    { value: 'avg', label: 'Среднее' },
    { value: 'min', label: 'Минимум' },
    { value: 'max', label: 'Максимум' }
  ];

  function emptyDefinition() {
    return normalizeGoldDefinition({
      metadata: { code: '', name: '', description: '', owner: '', tags: [], status: 'draft', version: 1, published: false },
      sources: [],
      transformations: { joins: [], filters: [], derived_fields: [], metrics: [], dimensions: [], windows: [], groupings: [], aggregations: [] },
      model_enrichment: { mathematical_blocks: [], forecasting_blocks: [], inference_blocks: [] },
      materialization: { mode: 'snapshot_table', target_schema: 'gold_showcase', target_name: '', table_class: 'gold_showcase', data_level: 'gold' },
      refresh_policy: { mode: 'manual', schedule_value: '', timezone: 'UTC', dependency_sources: [] },
      quality: { required_output_fields: [], freshness_expectation_minutes: 0, completeness_threshold_pct: 0 },
      consumers: []
    });
  }

  function cloneJson<T>(value: T): T {
    return JSON.parse(JSON.stringify(value));
  }

  function slugify(value: string): string {
    return String(value || '')
      .toLowerCase()
      .replace(/[^a-z0-9_а-яё]+/gi, '_')
      .replace(/^_+|_+$/g, '')
      .replace(/_+/g, '_');
  }

  function uniqueStrings(values: string[] = []): string[] {
    return [...new Set(values.map((item) => String(item || '').trim()).filter(Boolean))];
  }

  function asArray(value: any): any[] {
    return Array.isArray(value) ? value : [];
  }

  function parseCsv(value: string): string[] {
    return uniqueStrings(String(value || '').split(','));
  }

  function formatTs(value: string): string {
    const raw = String(value || '').trim();
    if (!raw) return '—';
    const date = new Date(raw);
    if (Number.isNaN(date.getTime())) return raw;
    return date.toLocaleString('ru-RU');
  }

  function machineStatusLabel(status: string): string {
    const key = String(status || '').trim().toLowerCase();
    if (key === 'published') return 'Опубликована';
    if (key === 'healthy') return 'Здорова';
    if (key === 'stale') return 'Устарела';
    if (key === 'incompatible') return 'Несовместима';
    if (key === 'partial') return 'Частично собрана';
    if (key === 'broken') return 'Сломана';
    if (key === 'disabled') return 'Выключена';
    return 'Черновик';
  }

  let definitions: any[] = [];
  let selectedId = 0;
  let definition = emptyDefinition();
  let sourceCatalog: any = { process_sources: [], external_sources: [], reference_sources: [], source_map: {} };
  let preview: any = null;
  let health: any = null;
  let lineage: any = null;
  let impact: any = null;
  let consumersView: any = null;
  let activeDefinition: any = null;
  let latestRefresh: any = null;
  let statusMessage = '';
  let error = '';
  let loadingDefinitions = false;
  let busy = false;
  let registryDraft: any = {
    source_key: '',
    source_kind: 'external',
    source_name: '',
    description: '',
    schema_name: '',
    table_name: '',
    expected_freshness_minutes: 0
  };

  function canWrite() {
    return role === 'data_admin';
  }

  function selectedDefinitionSummary() {
    return definitions.find((item) => Number(item.id || 0) === Number(selectedId || 0)) || null;
  }

  function mutateDefinition(mutator: (draft: any) => void) {
    const next = cloneJson(definition);
    mutator(next);
    definition = normalizeGoldDefinition(next);
  }

  function fieldsForSource(sourceKey: string): GoldField[] {
    const catalog = sourceCatalog?.source_map?.[sourceKey];
    if (Array.isArray(catalog?.fields) && catalog.fields.length) return catalog.fields;
    const source = asArray(definition.sources).find((item) => item.source_key === sourceKey);
    return asArray(source?.selected_fields);
  }

  function isFieldSelected(source: GoldSource, field: GoldField): boolean {
    const key = String(field?.name || field?.field_name || '').trim();
    return asArray(source?.selected_fields).some((item) => String(item?.field_name || item?.alias || '').trim() === key);
  }

  function ensureSourceSelectedFields(source: any, catalog: any) {
    if (Array.isArray(source?.selected_fields) && source.selected_fields.length) return source.selected_fields;
    return asArray(catalog?.fields)
      .map((field: any) => ({
        field_name: String(field?.name || field?.field_name || '').trim(),
        alias: String(field?.name || field?.field_name || '').trim(),
        field_type: String(field?.type || field?.field_type || 'text').trim() || 'text',
        description: String(field?.description || '').trim(),
        path: String(field?.path || '').trim()
      }))
      .filter((field: any) => field.field_name);
  }

  function addSourceFromCatalog(source: any) {
    mutateDefinition((draft) => {
      if (draft.sources.some((item: any) => item.source_key === source.source_key)) return;
      draft.sources.push({
        source_key: source.source_key,
        source_kind: source.source_kind,
        source_name: source.source_name,
        process_code: source.process_code || '',
        desk_id: Number(source.desk_id || 0),
        start_node_id: source.start_node_id || '',
        schema_name: source.schema_name || '',
        table_name: source.table_name || '',
        contract_version: Number(source.contract_version || 0),
        required_fields: asArray(source.required_fields),
        optional_fields: asArray(source.optional_fields),
        selected_fields: ensureSourceSelectedFields(null, source),
        freshness_expectation_minutes: Number(source.freshness_expectation_minutes || 0)
      });
    });
  }

  function removeSource(sourceKey: string) {
    mutateDefinition((draft) => {
      draft.sources = draft.sources.filter((item: any) => item.source_key !== sourceKey);
      draft.refresh_policy.dependency_sources = asArray(draft.refresh_policy.dependency_sources).filter((item: string) => item !== sourceKey);
    });
  }

  function toggleSourceField(sourceKey: string, field: GoldField) {
    const fieldName = String(field?.name || field?.field_name || '').trim();
    if (!fieldName) return;
    mutateDefinition((draft) => {
      const source = draft.sources.find((item: any) => item.source_key === sourceKey);
      if (!source) return;
      const idx = asArray(source.selected_fields).findIndex((item: any) => String(item?.field_name || item?.alias || '').trim() === fieldName);
      if (idx >= 0) source.selected_fields.splice(idx, 1);
      else {
        source.selected_fields.push({
          field_name: fieldName,
          alias: fieldName,
          field_type: String(field?.type || field?.field_type || 'text').trim() || 'text',
          description: String(field?.description || '').trim(),
          path: String(field?.path || '').trim()
        });
      }
    });
  }

  function updateMetadataCodeFromName() {
    mutateDefinition((draft) => {
      draft.metadata.code = slugify(draft.metadata.name || draft.metadata.code || `gold_${Date.now()}`);
      if (!draft.materialization.target_name) draft.materialization.target_name = draft.metadata.code;
    });
  }

  async function loadCatalog() {
    const payload = await apiJson<any>(`${apiBase}/gold-sources/catalog`, { headers: headers() });
    sourceCatalog = payload || { process_sources: [], external_sources: [], reference_sources: [], source_map: {} };
  }

  async function loadDefinitions(preferredId = 0) {
    loadingDefinitions = true;
    try {
      const payload = await apiJson<any>(`${apiBase}/gold-definitions`, { headers: headers() });
      definitions = asArray(payload?.definitions);
      const nextId = Number(preferredId || selectedId || definitions[0]?.id || 0);
      if (nextId) await loadDefinitionDetail(nextId);
      else {
        selectedId = 0;
        definition = emptyDefinition();
      }
    } finally {
      loadingDefinitions = false;
    }
  }

  async function loadDefinitionDetail(id: number) {
    const payload = await apiJson<any>(`${apiBase}/gold-definitions/${id}`, { headers: headers() });
    selectedId = Number(payload?.definition?.id || id);
    definition = normalizeGoldDefinition(payload?.definition?.definition || emptyDefinition());
    activeDefinition = payload?.definition?.active_definition || null;
    latestRefresh = payload?.definition?.latest_refresh || null;
    if (payload?.source_catalog) sourceCatalog = payload.source_catalog;
    await loadDetailPanels(selectedId);
  }

  async function loadDetailPanels(id: number) {
    const [healthPayload, lineagePayload, impactPayload, consumersPayload] = await Promise.all([
      apiJson<any>(`${apiBase}/gold-definitions/${id}/health`, { headers: headers() }).catch(() => null),
      apiJson<any>(`${apiBase}/gold-definitions/${id}/lineage`, { headers: headers() }).catch(() => null),
      apiJson<any>(`${apiBase}/gold-definitions/${id}/impact`, { headers: headers() }).catch(() => null),
      apiJson<any>(`${apiBase}/gold-definitions/${id}/consumers`, { headers: headers() }).catch(() => null)
    ]);
    health = healthPayload;
    lineage = lineagePayload?.lineage || null;
    impact = impactPayload?.impact || null;
    consumersView = consumersPayload?.consumers || null;
  }

  async function ensureSavedDraft() {
    if (!canWrite()) throw new Error('Для изменения витрины нужна роль data_admin.');
    if (!definition.metadata.code) updateMetadataCodeFromName();
    if (!selectedId) {
      const created = await apiJson<any>(`${apiBase}/gold-definitions/create`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({ definition })
      });
      selectedId = Number(created?.definition?.id || 0);
      await loadDefinitions(selectedId);
      return selectedId;
    }
    await apiJson<any>(`${apiBase}/gold-definitions/${selectedId}/update-draft`, {
      method: 'POST',
      headers: headers(),
      body: JSON.stringify({ definition })
    });
    await loadDefinitions(selectedId);
    return selectedId;
  }

  async function saveDraft() {
    busy = true;
    error = '';
    try {
      const id = await ensureSavedDraft();
      statusMessage = `Черновик витрины сохранён (ID ${id}).`;
    } catch (e: any) {
      error = e?.message ?? String(e);
    } finally {
      busy = false;
    }
  }

  async function validateDraft() {
    busy = true;
    error = '';
    try {
      const id = await ensureSavedDraft();
      const payload = await apiJson<any>(`${apiBase}/gold-definitions/${id}/validate`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({ definition })
      });
      health = { ...health, validation: payload.validation, compatibility: payload.compatibility, status: payload.status };
      statusMessage = payload?.validation?.ok ? 'Валидация пройдена.' : 'Валидация нашла ошибки или предупреждения.';
    } catch (e: any) {
      error = e?.message ?? String(e);
    } finally {
      busy = false;
    }
  }

  async function previewDraft() {
    busy = true;
    error = '';
    try {
      const id = await ensureSavedDraft();
      const payload = await apiJson<any>(`${apiBase}/gold-definitions/${id}/preview`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({ definition, limit: 50 })
      });
      preview = payload?.preview || null;
      statusMessage = preview?.dataset?.rows?.length ? `Предпросмотр собран: ${preview.dataset.rows.length} строк.` : 'Предпросмотр собран, строк нет.';
      await loadDetailPanels(id);
    } catch (e: any) {
      error = e?.message ?? String(e);
    } finally {
      busy = false;
    }
  }

  async function publishDraft() {
    busy = true;
    error = '';
    try {
      const id = await ensureSavedDraft();
      const payload = await apiJson<any>(`${apiBase}/gold-definitions/${id}/publish`, {
        method: 'POST',
        headers: headers()
      });
      statusMessage = `Опубликована версия ${payload?.version || '—'}.`;
      await loadDefinitions(id);
    } catch (e: any) {
      error = e?.message ?? String(e);
    } finally {
      busy = false;
    }
  }

  async function refreshGold() {
    busy = true;
    error = '';
    try {
      const id = selectedId || (await ensureSavedDraft());
      const payload = await apiJson<any>(`${apiBase}/gold-definitions/${id}/refresh`, {
        method: 'POST',
        headers: headers()
      });
      preview = payload?.preview || preview;
      latestRefresh = payload?.refresh_run || null;
      statusMessage = `Материализация завершена. Строк: ${payload?.materialization?.row_count ?? 0}.`;
      await loadDefinitions(id);
    } catch (e: any) {
      error = e?.message ?? String(e);
    } finally {
      busy = false;
    }
  }

  function startNewDefinition() {
    selectedId = 0;
    definition = emptyDefinition();
    preview = null;
    health = null;
    lineage = null;
    impact = null;
    consumersView = null;
    activeDefinition = null;
    latestRefresh = null;
    statusMessage = 'Новая витрина создана в черновике.';
    error = '';
  }

  function addDerivedField() {
    mutateDefinition((draft) => {
      draft.transformations.derived_fields.push({ field_name: `derived_${draft.transformations.derived_fields.length + 1}`, field_type: 'numeric', formula: '', description: '' });
    });
  }

  function addFilter() {
    mutateDefinition((draft) => {
      draft.transformations.filters.push({ filter_key: `filter_${draft.transformations.filters.length + 1}`, field_name: '', operator: 'eq', value: '', formula: '' });
    });
  }

  function addGrouping() {
    mutateDefinition((draft) => {
      draft.transformations.groupings.push({ grouping_key: `group_${draft.transformations.groupings.length + 1}`, field_name: '', alias: '' });
    });
  }

  function addAggregation() {
    mutateDefinition((draft) => {
      draft.transformations.aggregations.push({
        metric_key: `metric_${draft.transformations.aggregations.length + 1}`,
        alias: `metric_${draft.transformations.aggregations.length + 1}`,
        field_name: '',
        aggregator: 'sum',
        field_type: 'numeric',
        description: ''
      });
    });
  }

  function removeItem(sectionPath: string, index: number) {
    mutateDefinition((draft) => {
      const [section, key] = sectionPath.split('.');
      draft[section][key].splice(index, 1);
    });
  }

  function addModelBlock(section: 'mathematical_blocks' | 'forecasting_blocks' | 'inference_blocks') {
    mutateDefinition((draft) => {
      draft.model_enrichment[section].push({
        block_key: `${section}_${draft.model_enrichment[section].length + 1}`,
        block_name: 'Новый блок',
        model_key: '',
        model_version: 'draft',
        required_input_features: [],
        output_fields: [{ field_name: 'score', field_type: 'numeric', formula: '', description: '' }],
        retraining_policy: '',
        block_type: 'custom'
      });
    });
  }

  function addConsumer() {
    mutateDefinition((draft) => {
      draft.consumers.push({ consumer_key: `consumer_${draft.consumers.length + 1}`, consumer_kind: 'report', name: '', description: '' });
    });
  }

  async function saveRegistrySource() {
    busy = true;
    error = '';
    try {
      if (!registryDraft.source_key) registryDraft.source_key = slugify(registryDraft.source_name || `source_${Date.now()}`);
      await apiJson<any>(`${apiBase}/gold-sources/upsert`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify(registryDraft)
      });
      await loadCatalog();
      statusMessage = 'Источник сохранён в реестре.';
      registryDraft = { source_key: '', source_kind: 'external', source_name: '', description: '', schema_name: '', table_name: '', expected_freshness_minutes: 0 };
    } catch (e: any) {
      error = e?.message ?? String(e);
    } finally {
      busy = false;
    }
  }

  async function deleteRegistrySource(sourceKey: string) {
    busy = true;
    error = '';
    try {
      await apiJson<any>(`${apiBase}/gold-sources/delete`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({ source_key: sourceKey })
      });
      await loadCatalog();
      statusMessage = `Источник ${sourceKey} удалён из реестра.`;
    } catch (e: any) {
      error = e?.message ?? String(e);
    } finally {
      busy = false;
    }
  }

  function previewColumns() {
    const outputFields = asArray(preview?.dataset?.output_fields).map((field: any) => String(field?.name || '').trim()).filter(Boolean);
    if (outputFields.length) return outputFields;
    return asArray(preview?.dataset?.columns).map((field: any) => String(field || '').trim()).filter(Boolean);
  }

  function inputValue(event: Event): string {
    return String((event.currentTarget as HTMLInputElement | null)?.value || '');
  }

  function updateTags(event: Event) {
    const value = inputValue(event);
    mutateDefinition((draft) => {
      draft.metadata.tags = parseCsv(value);
    });
  }

  function updateBlockRequiredFeatures(sectionKey: string, index: number, event: Event) {
    const value = inputValue(event);
    mutateDefinition((draft) => {
      draft.model_enrichment[sectionKey][index].required_input_features = parseCsv(value);
    });
  }

  function toggleRefreshDependencySource(sourceKey: string) {
    mutateDefinition((draft) => {
      const list = asArray(draft.refresh_policy.dependency_sources);
      const idx = list.indexOf(sourceKey);
      draft.refresh_policy.dependency_sources = idx >= 0 ? list.filter((item) => item !== sourceKey) : [...list, sourceKey];
    });
  }

  function updateRequiredOutputFields(event: Event) {
    const value = inputValue(event);
    mutateDefinition((draft) => {
      draft.quality.required_output_fields = parseCsv(value);
    });
  }

  onMount(async () => {
    try {
      await loadCatalog();
      await loadDefinitions();
    } catch (e: any) {
      error = e?.message ?? String(e);
    }
  });
</script>

<section class="panel">
  <div class="panel-head">
    <div>
      <h2>����������� Gold-������</h2>
      <p class="sub">��������� builder ������ ������: ��������� ���������, ������� � ���������� ������, �������, ������, materialization, refresh � quality.</p>
    </div>
    <div class="toolbar">
      <button on:click={startNewDefinition}>����� �������</button>
      <button on:click={loadCatalog} disabled={busy}>�������� ���������</button>
      <button on:click={() => loadDefinitions(selectedId)} disabled={busy || loadingDefinitions}>�������� ������</button>
    </div>
  </div>

  {#if error}<div class="alert error">{error}</div>{/if}
  {#if statusMessage}<div class="alert ok">{statusMessage}</div>{/if}

  <div class="layout">
    <aside class="sidebar">
      <div class="section-head">
        <h3>�������</h3>
        <span class="badge">{definitions.length}</span>
      </div>
      <div class="definition-list">
        {#if loadingDefinitions}
          <div class="hint">�������� ������...</div>
        {:else if definitions.length === 0}
          <div class="hint">���� ��� ������. ������ ������ Gold definition.</div>
        {:else}
          {#each definitions as item}
            <button class="definition-item" class:active={Number(item.id) === Number(selectedId)} on:click={() => loadDefinitionDetail(Number(item.id))}>
              <div class="item-top">
                <strong>{item.name || item.code}</strong>
                <span class={`status status-${item.status || 'draft'}`}>{machineStatusLabel(item.status)}</span>
              </div>
              <div class="item-code">{item.code}</div>
              <div class="item-meta">
                <span>{item.published ? `v${item.active_version || 0}` : '��������'}</span>
                {#if item.has_draft_changes}<span>���� ���������������� ���������</span>{/if}
                {#if item.latest_refresh?.started_at}<span>refresh: {formatTs(item.latest_refresh.started_at)}</span>{/if}
              </div>
            </button>
          {/each}
        {/if}
      </div>
    </aside>

    <div class="editor">
      <section class="card">
        <div class="section-head">
          <h3>���������� �������</h3>
          <div class="inline-actions">
            <button class="primary" on:click={saveDraft} disabled={busy || !canWrite()}>��������� ��������</button>
            <button on:click={validateDraft} disabled={busy || !canWrite()}>���������</button>
            <button on:click={publishDraft} disabled={busy || !canWrite()}>������������</button>
            <button on:click={refreshGold} disabled={busy || !canWrite()}>�������� �������</button>
          </div>
        </div>
        <div class="grid cols-3">
          <label>
            ���
            <div class="inline-field">
              <input bind:value={definition.metadata.code} placeholder="gold_revenue_daily" />
              <button type="button" on:click={updateMetadataCodeFromName}>�������������</button>
            </div>
          </label>
          <label>
            ��������
            <input bind:value={definition.metadata.name} placeholder="������� ������� �������" />
          </label>
          <label>
            ��������
            <input bind:value={definition.metadata.owner} placeholder="analytics_team" />
          </label>
          <label class="span-3">
            ��������
            <textarea bind:value={definition.metadata.description} rows="2" placeholder="��� ������� ������� � ��� ���� �����"></textarea>
          </label>
          <label class="span-3">
            ����
            <input value={definition.metadata.tags.join(', ')} on:input={updateTags} placeholder="revenue, gold, dashboard" />
          </label>
        </div>
      </section>

      <section class="card">
        <div class="section-head">
          <h3>���������</h3>
          <span class="hint">Published process outputs, named external sources � �����������.</span>
        </div>
        <div class="source-layout">
          <div class="catalog-column">
            <div class="catalog-block">
              <h4>��������� �� ���������</h4>
              {#if asArray(sourceCatalog.process_sources).length === 0}
                <div class="hint">Published process outputs ���� �� �������.</div>
              {:else}
                {#each sourceCatalog.process_sources as source}
                  <div class="catalog-item">
                    <div class="catalog-title">{source.source_name}</div>
                    <div class="catalog-meta">{source.process_code} � {source.schema_name}.{source.table_name}</div>
                    <div class="catalog-meta">��������: v{source.contract_version || 0} � �����: {asArray(source.fields).length}</div>
                    <button on:click={() => addSourceFromCatalog(source)}>��������</button>
                  </div>
                {/each}
              {/if}
            </div>

            <div class="catalog-block">
              <h4>������� ������ � �����������</h4>
              {#each [...asArray(sourceCatalog.external_sources), ...asArray(sourceCatalog.reference_sources)] as source}
                <div class="catalog-item">
                  <div class="catalog-title">{source.source_name}</div>
                  <div class="catalog-meta">{source.source_kind} � {source.schema_name}.{source.table_name}</div>
                  <div class="catalog-meta">����: {asArray(source.fields).length}</div>
                  <div class="inline-actions">
                    <button on:click={() => addSourceFromCatalog(source)}>��������</button>
                    <button class="ghost" on:click={() => deleteRegistrySource(source.source_key)}>�������</button>
                  </div>
                </div>
              {/each}
              {#if asArray(sourceCatalog.external_sources).length + asArray(sourceCatalog.reference_sources).length === 0}
                <div class="hint">������ named sources ���� ����.</div>
              {/if}
            </div>

            <div class="catalog-block">
              <h4>������ ������� ����������</h4>
              <div class="grid cols-2">
                <label>
                  ���
                  <select bind:value={registryDraft.source_kind}>
                    <option value="external">������� ��������</option>
                    <option value="reference">����������</option>
                  </select>
                </label>
                <label>
                  ����
                  <input bind:value={registryDraft.source_key} placeholder="crm_clients_v1" />
                </label>
                <label class="span-2">
                  ��������
                  <input bind:value={registryDraft.source_name} placeholder="CRM �������" />
                </label>
                <label>
                  �����
                  <input bind:value={registryDraft.schema_name} placeholder="gold_ref" />
                </label>
                <label>
                  �������
                  <input bind:value={registryDraft.table_name} placeholder="crm_clients" />
                </label>
                <label>
                  ��������, �����
                  <input bind:value={registryDraft.expected_freshness_minutes} type="number" min="0" />
                </label>
                <label class="span-2">
                  ��������
                  <input bind:value={registryDraft.description} placeholder="���������� �������� � ���������" />
                </label>
              </div>
              <button on:click={saveRegistrySource} disabled={busy || !canWrite()}>��������� �������� � ������</button>
            </div>
          </div>

          <div class="selected-sources">
            <h4>��������� ���������</h4>
            {#if definition.sources.length === 0}
              <div class="hint">������ ���� �� ���� ��������, ����� �������� contract preview � �������.</div>
            {/if}
            {#each definition.sources as source}
              <div class="selected-source">
                <div class="section-head compact">
                  <div>
                    <div class="catalog-title">{source.source_name || source.source_key}</div>
                    <div class="catalog-meta">{source.source_kind} � {source.schema_name}.{source.table_name}</div>
                  </div>
                  <button class="ghost" on:click={() => removeSource(source.source_key)}>������</button>
                </div>
                <div class="chip-wrap">
                  {#each fieldsForSource(source.source_key) as field}
                    <button type="button" class="chip" class:active={isFieldSelected(source, field)} title={field.path || field.description || ''} on:click={() => toggleSourceField(source.source_key, field)}>
                      {field.name || field.field_name}
                    </button>
                  {/each}
                </div>
              </div>
            {/each}
          </div>
        </div>
      </section>

      <section class="card">
        <div class="section-head">
          <h3>������� �������</h3>
          <div class="inline-actions">
            <button on:click={addDerivedField}>�������� ����������� ����</button>
            <button on:click={addFilter}>�������� ������</button>
            <button on:click={addGrouping}>�������� �����������</button>
            <button on:click={addAggregation}>�������� �������</button>
          </div>
        </div>

        <div class="subsection">
          <h4>Derived fields</h4>
          {#if definition.transformations.derived_fields.length === 0}<div class="hint">���� ��� ����������� �����.</div>{/if}
          {#each definition.transformations.derived_fields as field, index}
            <div class="row-grid derived">
              <input bind:value={field.field_name} placeholder="gross_profit" />
              <input bind:value={field.field_type} placeholder="numeric" />
              <input bind:value={field.formula} placeholder="Number(row.revenue || 0) - Number(row.cost || 0)" />
              <button class="ghost" on:click={() => removeItem('transformations.derived_fields', index)}>�������</button>
            </div>
          {/each}
        </div>

        <div class="subsection">
          <h4>�������</h4>
          {#if definition.transformations.filters.length === 0}<div class="hint">���� ��� ��������.</div>{/if}
          {#each definition.transformations.filters as filter, index}
            <div class="row-grid filter">
              <input bind:value={filter.field_name} placeholder="state" />
              <select bind:value={filter.operator}>
                {#each FILTER_OPERATORS as operator}
                  <option value={operator.value}>{operator.label}</option>
                {/each}
              </select>
              <input bind:value={filter.value} placeholder="active" />
              <input bind:value={filter.formula} placeholder="��� ������� ��� �������� �������" />
              <button class="ghost" on:click={() => removeItem('transformations.filters', index)}>�������</button>
            </div>
          {/each}
        </div>

        <div class="subsection">
          <h4>����������� � ��������</h4>
          {#each definition.transformations.groupings as field, index}
            <div class="row-grid grouping">
              <input bind:value={field.field_name} placeholder="campaign_id" />
              <input bind:value={field.alias} placeholder="campaign_id" />
              <button class="ghost" on:click={() => removeItem('transformations.groupings', index)}>������� �����������</button>
            </div>
          {/each}
          {#each definition.transformations.aggregations as metric, index}
            <div class="row-grid metric">
              <input bind:value={metric.alias} placeholder="revenue_sum" />
              <input bind:value={metric.field_name} placeholder="revenue" />
              <select bind:value={metric.aggregator}>
                {#each AGGREGATORS as aggregator}
                  <option value={aggregator.value}>{aggregator.label}</option>
                {/each}
              </select>
              <input bind:value={metric.field_type} placeholder="numeric" />
              <button class="ghost" on:click={() => removeItem('transformations.aggregations', index)}>������� �������</button>
            </div>
          {/each}
        </div>
      </section>

      <section class="card">
        <div class="section-head">
          <h3>�������������� ���������� � inference</h3>
          <div class="inline-actions">
            <button on:click={() => addModelBlock('mathematical_blocks')}>Math block</button>
            <button on:click={() => addModelBlock('forecasting_blocks')}>Forecast block</button>
            <button on:click={() => addModelBlock('inference_blocks')}>Inference block</button>
          </div>
        </div>
        {#each [
          ['mathematical_blocks', '�������������� �����'],
          ['forecasting_blocks', '���������������'],
          ['inference_blocks', 'Inference']
        ] as [sectionKey, title]}
          <div class="subsection">
            <h4>{title}</h4>
            {#if definition.model_enrichment[sectionKey].length === 0}<div class="hint">���� ��� ������.</div>{/if}
            {#each definition.model_enrichment[sectionKey] as block, index}
              <div class="model-block">
                <div class="row-grid model-top">
                  <input bind:value={block.block_name} placeholder="�������� �����" />
                  <input bind:value={block.model_key} placeholder="model_key" />
                  <input bind:value={block.model_version} placeholder="v1" />
                  <input value={asArray(block.required_input_features).join(', ')} on:input={(event) => updateBlockRequiredFeatures(sectionKey, index, event)} placeholder="required features через запятую" />
                  <button class="ghost" on:click={() => mutateDefinition((draft) => draft.model_enrichment[sectionKey].splice(index, 1))}>�������</button>
                </div>
                {#each block.output_fields as outputField, outputIndex}
                  <div class="row-grid model-output">
                    <input bind:value={outputField.field_name} placeholder="score" />
                    <input bind:value={outputField.field_type} placeholder="numeric" />
                    <input bind:value={outputField.formula} placeholder="Number(row.revenue || 0) / Number(row.clicks || 1)" />
                    <button class="ghost" on:click={() => mutateDefinition((draft) => draft.model_enrichment[sectionKey][index].output_fields.splice(outputIndex, 1))}>������� �����</button>
                  </div>
                {/each}
                <button on:click={() => mutateDefinition((draft) => draft.model_enrichment[sectionKey][index].output_fields.push({ field_name: `output_${draft.model_enrichment[sectionKey][index].output_fields.length + 1}`, field_type: 'numeric', formula: '', description: '' }))}>�������� ����� ������</button>
              </div>
            {/each}
          </div>
        {/each}
      </section>

      <section class="card">
        <div class="section-head">
          <h3>Materialization, refresh � quality</h3>
        </div>
        <div class="grid cols-3">
          <label>
            Materialization
            <select bind:value={definition.materialization.mode}>
              <option value="snapshot_table">Snapshot table</option>
              <option value="materialized_view">Materialized view</option>
              <option value="live_view">Live view</option>
            </select>
          </label>
          <label>
            ����� ����������
            <input bind:value={definition.materialization.target_schema} placeholder="gold_showcase" />
          </label>
          <label>
            ��� ����������
            <input bind:value={definition.materialization.target_name} placeholder="gold_revenue_daily" />
          </label>
          <label>
            Table class
            <input bind:value={definition.materialization.table_class} placeholder="gold_showcase" />
          </label>
          <label>
            Data level
            <input bind:value={definition.materialization.data_level} placeholder="gold" />
          </label>
          <label>
            Refresh mode
            <select bind:value={definition.refresh_policy.mode}>
              <option value="manual">manual</option>
              <option value="interval">interval</option>
              <option value="cron">cron</option>
              <option value="dependency">dependency</option>
            </select>
          </label>
          <label>
            Schedule / cron
            <input bind:value={definition.refresh_policy.schedule_value} placeholder="15m / 0 * * * *" />
          </label>
          <label>
            Freshness, �����
            <input bind:value={definition.quality.freshness_expectation_minutes} type="number" min="0" />
          </label>
          <label>
            Completeness threshold, %
            <input bind:value={definition.quality.completeness_threshold_pct} type="number" min="0" max="100" />
          </label>
          <label class="span-3">
            Dependency refresh sources
            <div class="chip-wrap">
              {#each definition.sources as source}
                <button type="button" class="chip" class:active={definition.refresh_policy.dependency_sources.includes(source.source_key)} on:click={() => toggleRefreshDependencySource(source.source_key)}>
                  {source.source_name || source.source_key}
                </button>
              {/each}
            </div>
          </label>
          <label class="span-3">
            Required output fields
            <input value={definition.quality.required_output_fields.join(', ')} on:input={updateRequiredOutputFields} placeholder="revenue, margin, score" />
          </label>
        </div>
      </section>

      <section class="card">
        <div class="section-head">
          <h3>Downstream consumers</h3>
          <button on:click={addConsumer}>�������� consumer</button>
        </div>
        {#if definition.consumers.length === 0}<div class="hint">���� ��� dashboards / reports / action rules.</div>{/if}
        {#each definition.consumers as consumer, index}
          <div class="row-grid consumer">
            <select bind:value={consumer.consumer_kind}>
              <option value="report">report</option>
              <option value="dashboard">dashboard</option>
              <option value="cube">cube</option>
              <option value="action_rule">action_rule</option>
              <option value="action_api">action_api</option>
            </select>
            <input bind:value={consumer.consumer_key} placeholder="dashboard:revenue_main" />
            <input bind:value={consumer.name} placeholder="������� �������" />
            <button class="ghost" on:click={() => mutateDefinition((draft) => draft.consumers.splice(index, 1))}>�������</button>
          </div>
        {/each}
      </section>

      <section class="card">
        <div class="section-head">
          <h3>������, lineage � impact</h3>
        </div>
        <div class="status-grid">
          <div class="status-card">
            <div class="status-title">������� ���������</div>
            <div class={`status-pill status-${health?.status?.machine_status || selectedDefinitionSummary()?.status || 'draft'}`}>{machineStatusLabel(health?.status?.machine_status || selectedDefinitionSummary()?.status || 'draft')}</div>
            <div class="meta-list">
              <div>Published: {selectedDefinitionSummary()?.published ? `��, v${selectedDefinitionSummary()?.active_version || 0}` : '���'}</div>
              <div>Draft changes: {selectedDefinitionSummary()?.has_draft_changes ? '����' : '���'}</div>
              <div>��������� refresh: {formatTs(latestRefresh?.finished_at || latestRefresh?.started_at || '')}</div>
              <div>�������� ������: {activeDefinition ? `v${selectedDefinitionSummary()?.active_version || 0}` : '���'}</div>
            </div>
          </div>
          <div class="status-card">
            <div class="status-title">Lineage</div>
            {#if lineage?.edges?.length}
              <ul class="flat-list">
                {#each lineage.edges as edge}
                  <li>{edge.from} > {edge.to} <span class="muted">({edge.edge_type})</span></li>
                {/each}
              </ul>
            {:else}
              <div class="hint">Lineage �������� ����� ������ ���������� � consumers.</div>
            {/if}
          </div>
          <div class="status-card">
            <div class="status-title">Impact</div>
            {#if impact}
              <ul class="flat-list">
                <li>���������� ���������: {asArray(impact.affected_sources).length}</li>
                <li>���������� ����: {asArray(impact.affected_output_fields).length}</li>
                <li>���������� consumers: {asArray(impact.affected_consumers).length}</li>
              </ul>
            {:else}
              <div class="hint">Impact analysis ���������� ��� ���������� �������.</div>
            {/if}
          </div>
        </div>
      </section>

      <section class="card preview-card">
        <div class="section-head">
          <h3>Preview / sample</h3>
          <div class="inline-actions">
            <button class="primary" on:click={previewDraft} disabled={busy || !canWrite()}>������� preview</button>
            <button on:click={refreshGold} disabled={busy || !canWrite()}>���������������</button>
          </div>
        </div>
        {#if preview}
          <div class="meta-list">
            <div>Rows: {asArray(preview?.dataset?.rows).length}</div>
            <div>Columns: {previewColumns().length}</div>
            <div>Warnings: {asArray(preview?.dataset?.warnings).length}</div>
          </div>
          {#if asArray(preview?.dataset?.rows).length}
            <div class="preview-grid-wrap">
              <table class="preview-grid">
                <thead><tr>{#each previewColumns() as column}<th>{column}</th>{/each}</tr></thead>
                <tbody>
                  {#each preview.dataset.rows.slice(0, 20) as row}
                    <tr>{#each previewColumns() as column}<td>{row?.[column] ?? ''}</td>{/each}</tr>
                  {/each}
                </tbody>
              </table>
            </div>
          {:else if previewColumns().length}
            <div class="hint">Preview ���������, �� ����� ���. ���� ����� shape ����������.</div>
            <div class="chip-wrap">{#each previewColumns() as column}<span class="chip static">{column}</span>{/each}</div>
          {:else}
            <div class="hint">Preview ���� �� ����������� �� shape, �� ������.</div>
          {/if}
          {#if asArray(preview?.dataset?.sample_aggregates).length}
            <div class="subsection">
              <h4>Sample aggregates</h4>
              <ul class="flat-list">
                {#each preview.dataset.sample_aggregates as aggregate}
                  <li>{aggregate.field_name}: ����� {aggregate.sum}, �������� �������� {aggregate.non_null_count}</li>
                {/each}
              </ul>
            </div>
          {/if}
        {:else}
          <div class="hint">������ preview, ����� ������� sample rows, �������� � contract preview.</div>
        {/if}
      </section>
    </div>
  </div>
</section>

<style>
  .panel { background:#fff; border:1px solid #e6eaf2; border-radius:18px; padding:14px; box-shadow:0 6px 20px rgba(15,23,42,.05); margin-top:12px; }
  .panel-head { display:flex; justify-content:space-between; align-items:flex-start; gap:12px; }
  .panel-head h2 { margin:0; font-size:20px; }
  .sub { margin:6px 0 0; color:#64748b; font-size:13px; max-width:900px; }
  .toolbar, .inline-actions { display:flex; gap:8px; flex-wrap:wrap; }
  .alert { margin-top:12px; padding:10px 12px; border-radius:14px; font-size:13px; }
  .alert.error { background:#fff1f2; border:1px solid #fecdd3; color:#9f1239; }
  .alert.ok { background:#ecfdf5; border:1px solid #bbf7d0; color:#166534; }
  .layout { display:grid; grid-template-columns: 320px 1fr; gap:14px; margin-top:14px; align-items:start; }
  .sidebar, .card { border:1px solid #e6eaf2; border-radius:16px; background:#f8fafc; padding:12px; }
  .editor { display:flex; flex-direction:column; gap:12px; }
  .section-head { display:flex; justify-content:space-between; align-items:flex-start; gap:12px; margin-bottom:10px; }
  .section-head.compact { margin-bottom:8px; }
  .section-head h3, .section-head h4 { margin:0; }
  .definition-list { display:flex; flex-direction:column; gap:8px; }
  .definition-item { text-align:left; border:1px solid #d8e1f0; background:#fff; border-radius:14px; padding:10px; }
  .definition-item.active { border-color:#0f172a; background:#0f172a; color:#fff; }
  .item-top { display:flex; justify-content:space-between; gap:8px; align-items:flex-start; }
  .item-code, .item-meta, .catalog-meta, .meta-list, .hint { font-size:12px; color:#64748b; }
  .definition-item.active .item-code, .definition-item.active .item-meta { color:#dbeafe; }
  .badge, .status-pill { display:inline-flex; align-items:center; justify-content:center; border-radius:999px; padding:4px 10px; font-size:11px; font-weight:700; border:1px solid #cbd5e1; background:#fff; }
  .status, .status-pill { color:#334155; }
  .status-draft { background:#f8fafc; }
  .status-published, .status-healthy { background:#ecfdf5; color:#166534; border-color:#bbf7d0; }
  .status-stale, .status-partial { background:#fffbeb; color:#92400e; border-color:#fde68a; }
  .status-incompatible, .status-broken { background:#fff1f2; color:#9f1239; border-color:#fecdd3; }
  .grid { display:grid; gap:10px; }
  .cols-2 { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .cols-3 { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .span-2 { grid-column: span 2; }
  .span-3 { grid-column: span 3; }
  label { display:flex; flex-direction:column; gap:6px; font-size:12px; color:#475569; }
  input, select, textarea, button { font:inherit; }
  input, select, textarea { width:100%; box-sizing:border-box; border:1px solid #cbd5e1; border-radius:12px; padding:10px 12px; background:#fff; }
  textarea { resize:vertical; min-height:72px; }
  button { border:1px solid #cbd5e1; border-radius:12px; padding:9px 12px; background:#fff; cursor:pointer; }
  button.primary { background:#0f172a; color:#fff; border-color:#0f172a; }
  button.ghost { background:#f8fafc; }
  button:disabled { opacity:.6; cursor:not-allowed; }
  .inline-field { display:grid; grid-template-columns: 1fr auto; gap:8px; }
  .source-layout { display:grid; grid-template-columns: minmax(320px, 420px) 1fr; gap:12px; }
  .catalog-column { display:flex; flex-direction:column; gap:12px; }
  .catalog-block, .selected-source, .status-card, .subsection { border:1px solid #dbe5f2; background:#fff; border-radius:14px; padding:10px; }
  .catalog-item { border:1px solid #edf2f7; border-radius:12px; padding:10px; display:flex; flex-direction:column; gap:6px; margin-top:8px; }
  .catalog-title { font-weight:700; color:#0f172a; }
  .selected-sources { display:flex; flex-direction:column; gap:10px; }
  .chip-wrap { display:flex; flex-wrap:wrap; gap:8px; }
  .chip { border-radius:999px; padding:6px 10px; font-size:12px; border:1px solid #cbd5e1; background:#fff; }
  .chip.active { background:#0f172a; color:#fff; border-color:#0f172a; }
  .chip.static { cursor:default; }
  .row-grid { display:grid; gap:8px; align-items:start; margin-top:8px; }
  .row-grid.derived { grid-template-columns: 180px 120px 1fr auto; }
  .row-grid.filter { grid-template-columns: 140px 130px 160px 1fr auto; }
  .row-grid.grouping { grid-template-columns: 1fr 1fr auto; }
  .row-grid.metric { grid-template-columns: 1fr 1fr 160px 120px auto; }
  .row-grid.model-top { grid-template-columns: 1fr 1fr 140px 1fr auto; }
  .row-grid.model-output { grid-template-columns: 180px 120px 1fr auto; }
  .row-grid.consumer { grid-template-columns: 180px 1fr 1fr auto; }
  .model-block { display:flex; flex-direction:column; gap:8px; margin-top:8px; }
  .status-grid { display:grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap:12px; }
  .status-title { font-weight:700; margin-bottom:8px; color:#0f172a; }
  .flat-list { margin:0; padding-left:18px; display:flex; flex-direction:column; gap:6px; }
  .muted { color:#64748b; }
  .preview-card { min-height:320px; }
  .preview-grid-wrap { overflow:auto; border:1px solid #d8e1f0; border-radius:14px; background:#fff; }
  .preview-grid { width:max-content; min-width:100%; border-collapse:collapse; }
  .preview-grid th, .preview-grid td { border-bottom:1px solid #edf2f7; padding:8px 10px; text-align:left; font-size:12px; vertical-align:top; white-space:nowrap; }
  .preview-grid th { position:sticky; top:0; background:#f8fafc; z-index:1; }
  @media (max-width: 1300px) { .layout, .source-layout, .status-grid { grid-template-columns: 1fr; } }
  @media (max-width: 960px) {
    .cols-2, .cols-3, .row-grid.derived, .row-grid.filter, .row-grid.grouping, .row-grid.metric, .row-grid.model-top, .row-grid.model-output, .row-grid.consumer, .inline-field { grid-template-columns: 1fr; }
    .span-2, .span-3 { grid-column: span 1; }
  }
</style>
