<script lang="ts">
  import { onMount } from 'svelte';
  import {
    addMartToGoldDefinition,
    addScenarioToMart,
    copyMartInGoldDefinition,
    copyScenarioInMart,
    createEmptyGoldDefinition,
    getActiveDataMart,
    getActiveScenario,
    markActiveScenarioPreviewStale,
    normalizeGoldDefinition,
    removeMartFromGoldDefinition,
    removeScenarioFromMart,
    setActiveDataMart,
    setActiveScenario
  } from '../../shared/goldDefinitionCore.mjs';

  export type Role = 'viewer' | 'operator' | 'data_admin';
  export let apiBase: string;
  export let role: Role;
  export let loading: boolean;
  export let headers: () => Record<string, string>;
  export let apiJson: <T = any>(url: string, init?: RequestInit) => Promise<T>;

  type AnyRecord = Record<string, any>;
  type GoldField = { name?: string; field_name?: string; type?: string; field_type?: string; description?: string; path?: string };

  const SOURCE_ROLE_OPTIONS = [
    { value: 'primary', label: 'Primary' },
    { value: 'fact', label: 'Fact' },
    { value: 'lookup', label: 'Lookup' },
    { value: 'reference', label: 'Reference' },
    { value: 'append', label: 'Append' }
  ];
  const RELATION_TYPE_OPTIONS = [
    { value: 'left_join', label: 'LEFT JOIN' },
    { value: 'inner_join', label: 'INNER JOIN' },
    { value: 'full_join', label: 'FULL JOIN' },
    { value: 'right_join', label: 'RIGHT JOIN' },
    { value: 'lookup_enrich', label: 'Lookup enrich' },
    { value: 'union', label: 'UNION' },
    { value: 'append', label: 'APPEND' }
  ];
  const CARDINALITY_OPTIONS = ['1:1', '1:N', 'N:1', 'N:N'];
  const MISMATCH_OPTIONS = [
    { value: 'keep_primary', label: 'Оставить primary' },
    { value: 'drop_row', label: 'Отбросить строку' },
    { value: 'null', label: 'Заполнить null' },
    { value: 'warning', label: 'Warning' },
    { value: 'error', label: 'Error' }
  ];
  const CONFLICT_OPTIONS = [
    { value: 'keep_left', label: 'Keep left' },
    { value: 'keep_right', label: 'Keep right' },
    { value: 'rename_with_prefix', label: 'Prefix' },
    { value: 'rename_with_alias', label: 'Alias' },
    { value: 'explicit', label: 'Explicit' }
  ];
  const TYPE_POLICY_OPTIONS = [
    { value: 'strict', label: 'Strict' },
    { value: 'cast', label: 'Cast' },
    { value: 'normalize_text', label: 'Normalize text' }
  ];
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

  function asArray(value: any): any[] {
    return Array.isArray(value) ? value : [];
  }

  function cloneJson<T>(value: T): T {
    return JSON.parse(JSON.stringify(value));
  }

  function slugify(value: string): string {
    return String(value || '')
      .toLowerCase()
      .replace(/[^0-9a-zа-яё_]+/gi, '_')
      .replace(/^_+|_+$/g, '')
      .replace(/_+/g, '_');
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
  let definition = createEmptyGoldDefinition();
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
  let busy = false;
  let activeMart: any = null;
  let activeScenario: any = null;
  let sourceOptions: any[] = [];

  $: activeMart = getActiveDataMart(definition);
  $: activeScenario = getActiveScenario(definition);
  $: sourceOptions = [...asArray(sourceCatalog.process_sources), ...asArray(sourceCatalog.external_sources), ...asArray(sourceCatalog.reference_sources)];

  function canWrite() {
    return role === 'data_admin';
  }

  function mutateDefinition(mutator: (draft: any) => void, markStale = true) {
    const next = cloneJson(definition);
    mutator(next);
    definition = normalizeGoldDefinition(next);
    if (markStale) definition = markActiveScenarioPreviewStale(definition);
  }

  function sourceFields(sourceKey: string): GoldField[] {
    const catalogSource = sourceCatalog?.source_map?.[sourceKey];
    if (Array.isArray(catalogSource?.fields) && catalogSource.fields.length) return catalogSource.fields;
    const source = asArray(activeScenario?.sources).find((item) => item.source_key === sourceKey);
    return asArray(source?.selected_fields);
  }

  function previewColumns() {
    const fields = asArray(preview?.dataset?.output_fields).map((field: any) => String(field?.name || '').trim()).filter(Boolean);
    if (fields.length) return fields;
    return asArray(preview?.dataset?.columns).map((item: any) => String(item || '').trim()).filter(Boolean);
  }

  function currentSummary() {
    return definitions.find((item) => Number(item.id || 0) === Number(selectedId || 0)) || null;
  }

  async function loadCatalog() {
    const payload = await apiJson<any>(`${apiBase}/gold-sources/catalog`, { headers: headers() });
    sourceCatalog = payload || { process_sources: [], external_sources: [], reference_sources: [], source_map: {} };
  }

  async function loadDefinitions(preferredId = 0) {
    const payload = await apiJson<any>(`${apiBase}/gold-definitions`, { headers: headers() });
    definitions = asArray(payload?.definitions);
    const nextId = Number(preferredId || selectedId || definitions[0]?.id || 0);
    if (nextId) await loadDefinitionDetail(nextId);
  }

  async function loadDefinitionDetail(id: number) {
    const payload = await apiJson<any>(`${apiBase}/gold-definitions/${id}`, { headers: headers() });
    selectedId = Number(payload?.definition?.id || id);
    definition = normalizeGoldDefinition(payload?.definition?.definition || createEmptyGoldDefinition());
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

  function ensureDeskCode() {
    mutateDefinition((draft) => {
      draft.metadata.code = slugify(draft.metadata.code || draft.metadata.name || `gold_${Date.now()}`);
      const mart = draft.marts.find((item: any) => item.id === draft.active_mart_id) || draft.marts[0];
      const scenario = mart?.scenarios?.find((item: any) => item.id === draft.active_scenario_id) || mart?.scenarios?.[0];
      if (mart && !mart.code) mart.code = slugify(mart.name || mart.id);
      if (scenario && !scenario.materialization?.target_name) {
        scenario.materialization.target_name = slugify(`${mart?.code || 'mart'}_${scenario?.name || 'scenario'}`);
      }
    }, false);
  }

  async function ensureSavedDraft() {
    if (!canWrite()) throw new Error('Для изменения витрины нужна роль data_admin.');
    ensureDeskCode();
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
    busy = true; error = '';
    try {
      const id = await ensureSavedDraft();
      statusMessage = `Черновик рабочего стола сохранён (ID ${id}).`;
    } catch (e: any) {
      error = e?.message ?? String(e);
    } finally {
      busy = false;
    }
  }

  async function validateDraft() {
    busy = true; error = '';
    try {
      const id = await ensureSavedDraft();
      const payload = await apiJson<any>(`${apiBase}/gold-definitions/${id}/validate`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({ definition })
      });
      health = { ...health, validation: payload.validation, compatibility: payload.compatibility, status: payload.status };
      statusMessage = payload?.validation?.ok ? 'Проверка пройдена.' : 'Проверка нашла ошибки или предупреждения.';
    } catch (e: any) {
      error = e?.message ?? String(e);
    } finally {
      busy = false;
    }
  }

  async function previewDraft() {
    busy = true; error = '';
    try {
      const id = await ensureSavedDraft();
      const payload = await apiJson<any>(`${apiBase}/gold-definitions/${id}/preview`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({ definition, limit: 50 })
      });
      preview = payload?.preview || null;
      statusMessage = asArray(preview?.dataset?.rows).length ? `Preview собран: ${preview.dataset.rows.length} строк.` : 'Preview собран, строк нет.';
      await loadDetailPanels(id);
    } catch (e: any) {
      error = e?.message ?? String(e);
    } finally {
      busy = false;
    }
  }

  async function publishDraft() {
    busy = true; error = '';
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
    busy = true; error = '';
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

  function startNewDesk() {
    selectedId = 0;
    definition = createEmptyGoldDefinition();
    preview = null;
    health = null;
    lineage = null;
    impact = null;
    consumersView = null;
    activeDefinition = null;
    latestRefresh = null;
    statusMessage = 'Новый рабочий стол витрин создан.';
    error = '';
  }

  function selectMart(martId: string) {
    definition = setActiveDataMart(definition, martId);
  }

  function selectScenario(martId: string, scenarioId: string) {
    definition = setActiveScenario(definition, martId, scenarioId);
  }

  function addMart() {
    definition = addMartToGoldDefinition(definition, { name: `Витрина ${definition.marts.length + 1}` });
  }

  function copyMart() {
    definition = copyMartInGoldDefinition(definition, activeMart?.id);
  }

  function removeMart() {
    definition = removeMartFromGoldDefinition(definition, activeMart?.id);
  }

  function addScenario() {
    definition = addScenarioToMart(definition, activeMart?.id, { name: `Сценарий ${asArray(activeMart?.scenarios).length + 1}` });
  }

  function copyScenario() {
    definition = copyScenarioInMart(definition, activeMart?.id, activeScenario?.id);
  }

  function removeScenario() {
    definition = removeScenarioFromMart(definition, activeMart?.id, activeScenario?.id);
  }

  function addSourceFromCatalog(source: AnyRecord) {
    mutateDefinition((draft) => {
      const mart = draft.marts.find((item: any) => item.id === draft.active_mart_id);
      const scenario = mart?.scenarios?.find((item: any) => item.id === draft.active_scenario_id);
      if (!scenario) return;
      if (scenario.sources.some((item: any) => item.source_key === source.source_key)) return;
      const role = scenario.sources.length === 0 ? 'primary' : 'lookup';
      scenario.sources.push({
        source_key: source.source_key,
        source_kind: source.source_kind,
        source_role: role,
        source_name: source.source_name,
        process_code: source.process_code || '',
        desk_id: Number(source.desk_id || 0),
        start_node_id: source.start_node_id || '',
        schema_name: source.schema_name || '',
        table_name: source.table_name || '',
        contract_version: Number(source.contract_version || 0),
        required_fields: asArray(source.required_fields),
        optional_fields: asArray(source.optional_fields),
        selected_fields: asArray(source.fields).map((field: any) => ({
          field_name: String(field?.name || field?.field_name || '').trim(),
          alias: String(field?.name || field?.field_name || '').trim(),
          field_type: String(field?.type || field?.field_type || 'text').trim() || 'text',
          description: String(field?.description || '').trim(),
          path: String(field?.path || '').trim()
        })).filter((field: any) => field.field_name),
        freshness_expectation_minutes: Number(source.freshness_expectation_minutes || 0)
      });
    });
  }

  function removeSource(sourceKey: string) {
    mutateDefinition((draft) => {
      const mart = draft.marts.find((item: any) => item.id === draft.active_mart_id);
      const scenario = mart?.scenarios?.find((item: any) => item.id === draft.active_scenario_id);
      if (!scenario) return;
      scenario.sources = scenario.sources.filter((item: any) => item.source_key !== sourceKey);
      scenario.relations = asArray(scenario.relations).filter((item: any) => item.left_source_key !== sourceKey && item.right_source_key !== sourceKey);
      scenario.refresh_policy.dependency_sources = asArray(scenario.refresh_policy.dependency_sources).filter((item: string) => item !== sourceKey);
    });
  }

  function toggleSourceField(sourceKey: string, field: GoldField) {
    const fieldName = String(field?.name || field?.field_name || '').trim();
    if (!fieldName) return;
    mutateDefinition((draft) => {
      const mart = draft.marts.find((item: any) => item.id === draft.active_mart_id);
      const scenario = mart?.scenarios?.find((item: any) => item.id === draft.active_scenario_id);
      const source = scenario?.sources?.find((item: any) => item.source_key === sourceKey);
      if (!source) return;
      const index = asArray(source.selected_fields).findIndex((item: any) => String(item?.field_name || item?.alias || '').trim() === fieldName);
      if (index >= 0) source.selected_fields.splice(index, 1);
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

  function addRelation() {
    mutateDefinition((draft) => {
      const mart = draft.marts.find((item: any) => item.id === draft.active_mart_id);
      const scenario = mart?.scenarios?.find((item: any) => item.id === draft.active_scenario_id);
      if (!scenario || scenario.sources.length < 2) return;
      const left = scenario.sources.find((item: any) => item.source_role === 'primary' || item.source_role === 'fact') || scenario.sources[0];
      const right = scenario.sources.find((item: any) => item.source_key !== left.source_key) || scenario.sources[1];
      scenario.relations.push({
        relation_key: `relation_${scenario.relations.length + 1}`,
        relation_name: `Связь ${scenario.relations.length + 1}`,
        relation_type: 'left_join',
        left_source_key: left.source_key,
        right_source_key: right.source_key,
        join_keys: [{ key_id: 'key_1', left_field: '', right_field: '', operator: 'eq' }],
        cardinality: 'N:1',
        mismatch_policy: 'keep_primary',
        conflict_policy: 'keep_left',
        type_policy: 'strict',
        rename_prefix: ''
      });
    });
  }

  function addRelationKey(index: number) {
    mutateDefinition((draft) => {
      const relation = asArray(draft.marts.find((item: any) => item.id === draft.active_mart_id)?.scenarios?.find((item: any) => item.id === draft.active_scenario_id)?.relations)[index];
      if (!relation) return;
      relation.join_keys.push({ key_id: `key_${relation.join_keys.length + 1}`, left_field: '', right_field: '', operator: 'eq' });
    });
  }

  function removeRelation(index: number) {
    mutateDefinition((draft) => {
      const scenario = draft.marts.find((item: any) => item.id === draft.active_mart_id)?.scenarios?.find((item: any) => item.id === draft.active_scenario_id);
      if (!scenario) return;
      scenario.relations.splice(index, 1);
    });
  }

  function addDerivedField() {
    mutateDefinition((draft) => {
      const scenario = draft.marts.find((item: any) => item.id === draft.active_mart_id)?.scenarios?.find((item: any) => item.id === draft.active_scenario_id);
      if (!scenario) return;
      scenario.transformations.derived_fields.push({ field_name: `derived_${scenario.transformations.derived_fields.length + 1}`, field_type: 'numeric', formula: '', description: '' });
    });
  }

  function addFilter() {
    mutateDefinition((draft) => {
      const scenario = draft.marts.find((item: any) => item.id === draft.active_mart_id)?.scenarios?.find((item: any) => item.id === draft.active_scenario_id);
      if (!scenario) return;
      scenario.transformations.filters.push({ filter_key: `filter_${scenario.transformations.filters.length + 1}`, field_name: '', operator: 'eq', value: '', formula: '' });
    });
  }

  function addGrouping() {
    mutateDefinition((draft) => {
      const scenario = draft.marts.find((item: any) => item.id === draft.active_mart_id)?.scenarios?.find((item: any) => item.id === draft.active_scenario_id);
      if (!scenario) return;
      scenario.transformations.groupings.push({ grouping_key: `group_${scenario.transformations.groupings.length + 1}`, field_name: '', alias: '' });
    });
  }

  function addAggregation() {
    mutateDefinition((draft) => {
      const scenario = draft.marts.find((item: any) => item.id === draft.active_mart_id)?.scenarios?.find((item: any) => item.id === draft.active_scenario_id);
      if (!scenario) return;
      scenario.transformations.aggregations.push({ metric_key: `metric_${scenario.transformations.aggregations.length + 1}`, alias: `metric_${scenario.transformations.aggregations.length + 1}`, field_name: '', aggregator: 'sum', field_type: 'numeric', description: '' });
    });
  }

  function removeItem(sectionPath: string, index: number) {
    mutateDefinition((draft) => {
      const scenario = draft.marts.find((item: any) => item.id === draft.active_mart_id)?.scenarios?.find((item: any) => item.id === draft.active_scenario_id);
      if (!scenario) return;
      const [section, key] = sectionPath.split('.');
      scenario[section][key].splice(index, 1);
    });
  }

  function toggleRefreshDependencySource(sourceKey: string) {
    mutateDefinition((draft) => {
      const scenario = draft.marts.find((item: any) => item.id === draft.active_mart_id)?.scenarios?.find((item: any) => item.id === draft.active_scenario_id);
      if (!scenario) return;
      const list = asArray(scenario.refresh_policy.dependency_sources);
      const idx = list.indexOf(sourceKey);
      scenario.refresh_policy.dependency_sources = idx >= 0 ? list.filter((item: string) => item !== sourceKey) : [...list, sourceKey];
    });
  }

  function updateActiveMartField(field: 'name' | 'code', value: string) {
    mutateDefinition((draft) => {
      const mart = draft.marts.find((item: any) => item.id === draft.active_mart_id);
      if (!mart) return;
      mart[field] = value;
    });
  }

  function updateActiveScenarioField(field: 'name' | 'scenario_type' | 'description', value: string) {
    mutateDefinition((draft) => {
      const scenario = draft.marts
        .find((item: any) => item.id === draft.active_mart_id)
        ?.scenarios?.find((item: any) => item.id === draft.active_scenario_id);
      if (!scenario) return;
      scenario[field] = value;
    });
  }

  function updateRequiredOutputs(value: string) {
    mutateDefinition((draft) => {
      const scenario = draft.marts
        .find((item: any) => item.id === draft.active_mart_id)
        ?.scenarios?.find((item: any) => item.id === draft.active_scenario_id);
      if (!scenario) return;
      scenario.quality.required_output_fields = String(value || '')
        .split(',')
        .map((item) => item.trim())
        .filter(Boolean);
    });
  }

  onMount(async () => {
    await loadCatalog();
    await loadDefinitions();
  });
</script>

<div class="gold-layout">
  <aside class="workspace-list card">
    <div class="section-head">
      <div>
        <h2>Рабочие столы витрин</h2>
        <p class="sub">Отдельные контейнеры Gold: внутри витрины и сценарии.</p>
      </div>
      <button class="primary" on:click={startNewDesk} disabled={!canWrite() || busy}>Новый стол</button>
    </div>
    <div class="workspace-items">
      {#if definitions.length === 0}
        <div class="hint">Пока рабочих столов нет.</div>
      {/if}
      {#each definitions as item}
        <button class:selected={selectedId === item.id} class="workspace-item" on:click={() => loadDefinitionDetail(Number(item.id || 0))}>
          <strong>{item.name}</strong>
          <span>{item.code}</span>
          <span>{item.group_name || 'Без группы'} · витрин: {item.mart_count || 1}</span>
          <span>{item.published ? `v${item.active_version || 0}` : 'черновик'}</span>
        </button>
      {/each}
    </div>
  </aside>

  <div class="builder-column">
    <section class="card">
      <div class="section-head">
        <div>
          <h2>Структура рабочего стола</h2>
          <p class="sub">Container-level метаданные: desk, активная витрина и активный сценарий.</p>
        </div>
        <div class="actions">
          <button on:click={saveDraft} disabled={busy || !canWrite()}>Сохранить черновик</button>
          <button on:click={validateDraft} disabled={busy || !canWrite()}>Проверить</button>
          <button on:click={publishDraft} disabled={busy || !canWrite()}>Опубликовать</button>
          <button on:click={previewDraft} disabled={busy || !canWrite()}>Обновить preview</button>
          <button on:click={refreshGold} disabled={busy || !canWrite()}>Материализовать</button>
        </div>
      </div>
      <div class="form-grid top-grid">
        <label><span>Рабочий стол</span><input bind:value={definition.metadata.name} on:change={ensureDeskCode} /></label>
        <label><span>Группа</span><input bind:value={definition.metadata.group_name} /></label>
        <label><span>Код</span><input bind:value={definition.metadata.code} /></label>
        <label class="wide"><span>Описание рабочего стола</span><textarea rows="2" bind:value={definition.metadata.description}></textarea></label>
        <label><span>Активная витрина</span><input value={activeMart?.name || ''} on:input={(e) => updateActiveMartField('name', e.currentTarget.value)} /></label>
        <label><span>Код витрины</span><input value={activeMart?.code || ''} on:input={(e) => updateActiveMartField('code', e.currentTarget.value)} /></label>
        <label><span>Сценарий</span><input value={activeScenario?.name || ''} on:input={(e) => updateActiveScenarioField('name', e.currentTarget.value)} /></label>
        <label><span>Тип сценария</span><input value={activeScenario?.scenario_type || ''} on:input={(e) => updateActiveScenarioField('scenario_type', e.currentTarget.value)} /></label>
        <label class="wide"><span>Описание сценария</span><textarea rows="2" value={activeScenario?.description || ''} on:input={(e) => updateActiveScenarioField('description', e.currentTarget.value)}></textarea></label>
      </div>
      {#if statusMessage}<div class="status ok">{statusMessage}</div>{/if}
      {#if error}<div class="status error">{error}</div>{/if}
    </section>

    <section class="card">
      <div class="section-head">
        <div><h3>Витрины внутри рабочего стола</h3></div>
        <div class="actions">
          <button on:click={addMart} disabled={!canWrite()}>Создать витрину</button>
          <button on:click={copyMart} disabled={!canWrite()}>Копировать</button>
          <button class="danger" on:click={removeMart} disabled={!canWrite() || definition.marts.length <= 1}>Удалить</button>
        </div>
      </div>
      <div class="chip-wrap">
        {#each definition.marts as mart}
          <button type="button" class:active={mart.id === activeMart.id} class="chip" on:click={() => selectMart(mart.id)}>
            {mart.name} <small>{mart.status}</small>
          </button>
        {/each}
      </div>
    </section>

    <section class="card">
      <div class="section-head">
        <div><h3>Сценарии активной витрины</h3></div>
        <div class="actions">
          <button on:click={addScenario} disabled={!canWrite()}>Создать сценарий</button>
          <button on:click={copyScenario} disabled={!canWrite()}>Копировать</button>
          <button class="danger" on:click={removeScenario} disabled={!canWrite() || asArray(activeMart?.scenarios).length <= 1}>Удалить</button>
        </div>
      </div>
      <div class="chip-wrap">
        {#each asArray(activeMart?.scenarios) as scenario}
          <button type="button" class:active={scenario.id === activeScenario.id} class="chip" on:click={() => selectScenario(activeMart.id, scenario.id)}>
            {scenario.name} <small>{scenario.enabled ? 'on' : 'off'}</small>
          </button>
        {/each}
      </div>
    </section>

    <section class="card">
      <div class="section-head">
        <div>
          <h3>Источники и явная модель объединения</h3>
          <p class="sub">Primary / lookup / append роли, relation type, keys, cardinality и conflict policies.</p>
        </div>
        <button on:click={addRelation} disabled={!canWrite() || asArray(activeScenario?.sources).length < 2}>Добавить связь</button>
      </div>
      <div class="source-catalog-grid">
        <div>
          <div class="minor-head">Каталог источников</div>
          <div class="catalog-list">
            {#each sourceOptions as source}
              <button class="catalog-item" on:click={() => addSourceFromCatalog(source)} disabled={!canWrite()}>
                <strong>{source.source_name}</strong>
                <span>{source.source_kind} · {source.schema_name}.{source.table_name}</span>
              </button>
            {/each}
          </div>
        </div>
        <div>
          <div class="minor-head">Источники сценария</div>
          {#if asArray(activeScenario?.sources).length === 0}
            <div class="hint">Добавь источники в сценарий, чтобы настроить relation-модель.</div>
          {/if}
          {#each asArray(activeScenario?.sources) as source}
            <div class="source-card">
              <div class="source-row">
                <strong>{source.source_name}</strong>
                <button class="ghost" on:click={() => removeSource(source.source_key)}>Убрать</button>
              </div>
              <div class="source-form">
                <label><span>Роль</span><select bind:value={source.source_role} on:change={() => mutateDefinition(() => {})}>{#each SOURCE_ROLE_OPTIONS as option}<option value={option.value}>{option.label}</option>{/each}</select></label>
                <label><span>Контракт</span><input value={`${source.schema_name}.${source.table_name}`} readonly /></label>
              </div>
              <div class="chip-wrap field-wrap">
                {#each sourceFields(source.source_key) as field}
                  <button type="button" class:active={asArray(source.selected_fields).some((item) => item.field_name === (field.name || field.field_name))} class="chip" on:click={() => toggleSourceField(source.source_key, field)}>
                    {field.name || field.field_name}
                  </button>
                {/each}
              </div>
            </div>
          {/each}

          <div class="minor-head">Связи между источниками</div>
          {#if asArray(activeScenario?.relations).length === 0}
            <div class="hint">Если источников несколько, задай relation rules явно.</div>
          {/if}
          {#each asArray(activeScenario?.relations) as relation, relationIndex}
            <div class="relation-card">
              <div class="source-row">
                <strong>{relation.relation_name}</strong>
                <div class="actions">
                  <button class="ghost" on:click={() => addRelationKey(relationIndex)}>+ ключ</button>
                  <button class="ghost danger" on:click={() => removeRelation(relationIndex)}>Удалить</button>
                </div>
              </div>
              <div class="relation-grid">
                <label><span>Тип связи</span><select bind:value={relation.relation_type} on:change={() => mutateDefinition(() => {})}>{#each RELATION_TYPE_OPTIONS as option}<option value={option.value}>{option.label}</option>{/each}</select></label>
                <label><span>Left source</span><select bind:value={relation.left_source_key} on:change={() => mutateDefinition(() => {})}>{#each asArray(activeScenario?.sources) as source}<option value={source.source_key}>{source.source_name}</option>{/each}</select></label>
                <label><span>Right source</span><select bind:value={relation.right_source_key} on:change={() => mutateDefinition(() => {})}>{#each asArray(activeScenario?.sources) as source}<option value={source.source_key}>{source.source_name}</option>{/each}</select></label>
                <label><span>Кардинальность</span><select bind:value={relation.cardinality} on:change={() => mutateDefinition(() => {})}>{#each CARDINALITY_OPTIONS as option}<option value={option}>{option}</option>{/each}</select></label>
                <label><span>Несовпадения</span><select bind:value={relation.mismatch_policy} on:change={() => mutateDefinition(() => {})}>{#each MISMATCH_OPTIONS as option}<option value={option.value}>{option.label}</option>{/each}</select></label>
                <label><span>Конфликты колонок</span><select bind:value={relation.conflict_policy} on:change={() => mutateDefinition(() => {})}>{#each CONFLICT_OPTIONS as option}<option value={option.value}>{option.label}</option>{/each}</select></label>
                <label><span>Политика типов</span><select bind:value={relation.type_policy} on:change={() => mutateDefinition(() => {})}>{#each TYPE_POLICY_OPTIONS as option}<option value={option.value}>{option.label}</option>{/each}</select></label>
                <label><span>Префикс rename</span><input bind:value={relation.rename_prefix} on:change={() => mutateDefinition(() => {})} placeholder="cost" /></label>
              </div>
              {#each asArray(relation.join_keys) as joinKey}
                <div class="relation-grid compact">
                  <label><span>Левое поле</span><input bind:value={joinKey.left_field} on:change={() => mutateDefinition(() => {})} /></label>
                  <label><span>Правое поле</span><input bind:value={joinKey.right_field} on:change={() => mutateDefinition(() => {})} /></label>
                  <label><span>Оператор</span><input bind:value={joinKey.operator} on:change={() => mutateDefinition(() => {})} readonly /></label>
                </div>
              {/each}
            </div>
          {/each}
        </div>
      </div>
    </section>

    <section class="card">
      <div class="section-head"><h3>Расчёты и materialization</h3></div>
      <div class="builder-grid">
        <div>
          <div class="minor-head">Derived fields</div>
          <button class="ghost" on:click={addDerivedField}>Добавить поле</button>
          {#each asArray(activeScenario?.transformations?.derived_fields) as field, index}
            <div class="row-editor"><input bind:value={field.field_name} /><input bind:value={field.formula} placeholder="formula" /><button class="ghost danger" on:click={() => removeItem('transformations.derived_fields', index)}>x</button></div>
          {/each}
          <div class="minor-head">Фильтры</div>
          <button class="ghost" on:click={addFilter}>Добавить фильтр</button>
          {#each asArray(activeScenario?.transformations?.filters) as filter, index}
            <div class="row-editor"><input bind:value={filter.field_name} /><select bind:value={filter.operator}>{#each FILTER_OPERATORS as op}<option value={op.value}>{op.label}</option>{/each}</select><input bind:value={filter.value} /><button class="ghost danger" on:click={() => removeItem('transformations.filters', index)}>x</button></div>
          {/each}
        </div>
        <div>
          <div class="minor-head">Grouping & metrics</div>
          <button class="ghost" on:click={addGrouping}>Добавить группировку</button>
          {#each asArray(activeScenario?.transformations?.groupings) as field, index}
            <div class="row-editor"><input bind:value={field.field_name} placeholder="field" /><input bind:value={field.alias} placeholder="alias" /><button class="ghost danger" on:click={() => removeItem('transformations.groupings', index)}>x</button></div>
          {/each}
          <button class="ghost" on:click={addAggregation}>Добавить метрику</button>
          {#each asArray(activeScenario?.transformations?.aggregations) as metric, index}
            <div class="row-editor"><input bind:value={metric.alias} placeholder="alias" /><input bind:value={metric.field_name} placeholder="field" /><select bind:value={metric.aggregator}>{#each AGGREGATORS as agg}<option value={agg.value}>{agg.label}</option>{/each}</select><button class="ghost danger" on:click={() => removeItem('transformations.aggregations', index)}>x</button></div>
          {/each}

          <div class="minor-head">Materialization / refresh / quality</div>
          <div class="relation-grid">
            <label><span>Materialization</span><select bind:value={activeScenario.materialization.mode}><option value="snapshot_table">Snapshot table</option><option value="materialized_view">Materialized view</option><option value="live_view">Live view</option></select></label>
            <label><span>Target schema</span><input bind:value={activeScenario.materialization.target_schema} /></label>
            <label><span>Target name</span><input bind:value={activeScenario.materialization.target_name} /></label>
            <label><span>Refresh mode</span><select bind:value={activeScenario.refresh_policy.mode}><option value="manual">Manual</option><option value="interval">Interval</option><option value="cron">Cron</option><option value="dependency">Dependency</option></select></label>
            <label><span>Schedule</span><input bind:value={activeScenario.refresh_policy.schedule_value} placeholder="15m / 0 * * * *" /></label>
            <label><span>Required outputs</span><input value={asArray(activeScenario?.quality?.required_output_fields).join(', ')} on:change={(e) => updateRequiredOutputs(e.currentTarget.value)} /></label>
          </div>
          <div class="chip-wrap">
            {#each asArray(activeScenario?.sources) as source}
              <button type="button" class:active={asArray(activeScenario?.refresh_policy?.dependency_sources).includes(source.source_key)} class="chip" on:click={() => toggleRefreshDependencySource(source.source_key)}>
                refresh: {source.source_name}
              </button>
            {/each}
          </div>
        </div>
      </div>
    </section>

    <section class="card preview-card">
      <div class="section-head">
        <div>
          <h3>Preview / Publish / Materialization</h3>
          <p class="sub">Основная таблица остаётся простой, explainability уходит в secondary block.</p>
        </div>
        <div class="actions">
          <button class="primary" on:click={previewDraft} disabled={busy || !canWrite()}>Собрать preview</button>
          <button on:click={refreshGold} disabled={busy || !canWrite()}>Материализовать</button>
        </div>
      </div>
      {#if preview}
        <div class="preview-summary">
          <span>Desk: {preview?.dataset?.assembly?.desk_name || definition.metadata.name}</span>
          <span>Mart: {preview?.dataset?.assembly?.mart_name || activeMart?.name}</span>
          <span>Scenario: {preview?.dataset?.assembly?.scenario_name || activeScenario?.name}</span>
          <span>Preview ID: {preview.preview_uid || '—'}</span>
          <span>Rows before merge: {preview?.dataset?.assembly?.row_count_before_merge ?? 0}</span>
          <span>Rows after merge: {preview?.dataset?.assembly?.row_count_after_merge ?? 0}</span>
        </div>
        {#if asArray(preview?.dataset?.rows).length}
          <div class="preview-grid-wrap">
            <table class="preview-grid">
              <thead><tr>{#each previewColumns() as column}<th>{column}</th>{/each}</tr></thead>
              <tbody>{#each preview.dataset.rows.slice(0, 20) as row}<tr>{#each previewColumns() as column}<td>{row?.[column] ?? ''}</td>{/each}</tr>{/each}</tbody>
            </table>
          </div>
        {:else if previewColumns().length}
          <div class="hint">Строк нет, но contract preview доступен.</div>
          <div class="chip-wrap">{#each previewColumns() as column}<span class="chip static">{column}</span>{/each}</div>
        {:else}
          <div class="hint">Серверный preview вернул пустой результат.</div>
        {/if}
        <details class="secondary-block">
          <summary>Объяснение сборки и вторичные детали</summary>
          <div class="secondary-grid">
            <div><strong>Primary source:</strong> {preview?.dataset?.assembly?.primary_source_key || '—'}</div>
            <div><strong>Warnings:</strong> {asArray(preview?.dataset?.warnings).length}</div>
            <div><strong>Последний refresh:</strong> {formatTs(latestRefresh?.finished_at || latestRefresh?.started_at || '')}</div>
          </div>
          <ul class="secondary-list">
            {#each asArray(preview?.dataset?.assembly?.relations) as relation}
              <li>{relation.relation_name || relation.relation_key}: {relation.relation_type_label || relation.relation_type} · {relation.join_keys?.join(', ') || 'без ключей'} · {relation.before_count} → {relation.after_count}</li>
            {/each}
          </ul>
          <div class="secondary-grid">
            <div><strong>Health:</strong> {machineStatusLabel(health?.status?.health_code || currentSummary()?.status || 'draft')}</div>
            <div><strong>Published:</strong> {currentSummary()?.published ? `да, v${currentSummary()?.active_version || 0}` : 'нет'}</div>
          </div>
          <details><summary>Lineage / impact / consumers</summary>
            <div class="secondary-grid">
              <div>Lineage edges: {asArray(lineage?.edges).length}</div>
              <div>Impact sources: {asArray(impact?.affected_sources).length}</div>
              <div>Consumers: {asArray(consumersView?.consumers || consumersView).length}</div>
            </div>
          </details>
        </details>
      {:else}
        <div class="hint">Собери preview, чтобы увидеть результат сценария и explainability merge.</div>
      {/if}
    </section>
  </div>
</div>

<style>
  .gold-layout { display:grid; grid-template-columns: 300px minmax(0,1fr); gap:16px; min-height:0; }
  .builder-column { display:flex; flex-direction:column; gap:16px; min-width:0; }
  .card { background:#fff; border:1px solid #d8e1f0; border-radius:18px; padding:16px; box-shadow:0 10px 24px rgba(15,23,42,.04); }
  .section-head { display:flex; align-items:flex-start; justify-content:space-between; gap:12px; margin-bottom:12px; flex-wrap:wrap; }
  .section-head h2, .section-head h3 { margin:0; font-size:18px; }
  .sub { margin:4px 0 0; color:#64748b; font-size:12px; }
  .actions { display:flex; gap:8px; flex-wrap:wrap; }
  button { border:1px solid #cbd5e1; border-radius:12px; padding:10px 14px; background:#fff; cursor:pointer; }
  button.primary { background:#172033; color:#fff; border-color:#172033; }
  button.danger { color:#b91c1c; }
  button.ghost { background:#f8fafc; }
  button:disabled { opacity:.55; cursor:not-allowed; }
  .workspace-list { display:flex; flex-direction:column; gap:12px; min-height:0; }
  .workspace-items { display:flex; flex-direction:column; gap:8px; overflow:auto; }
  .workspace-item { text-align:left; display:flex; flex-direction:column; gap:4px; }
  .workspace-item.selected { border-color:#172033; box-shadow:inset 0 0 0 1px #172033; }
  .form-grid { display:grid; gap:12px; }
  .top-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .wide { grid-column: 1 / -1; }
  label { display:flex; flex-direction:column; gap:6px; min-width:0; }
  label span { font-size:12px; color:#475569; }
  input, textarea, select { width:100%; box-sizing:border-box; min-height:40px; border:1px solid #cbd5e1; border-radius:12px; padding:10px 12px; background:#fff; }
  textarea { min-height:76px; resize:vertical; }
  .status { margin-top:12px; padding:10px 12px; border-radius:12px; font-size:13px; }
  .status.ok { background:#ecfdf5; color:#166534; border:1px solid #bbf7d0; }
  .status.error { background:#fef2f2; color:#b91c1c; border:1px solid #fecaca; }
  .chip-wrap { display:flex; flex-wrap:wrap; gap:8px; }
  .chip { border-radius:999px; padding:8px 12px; background:#f8fafc; border:1px solid #d8e1f0; }
  .chip.active { background:#172033; color:#fff; border-color:#172033; }
  .chip.static { cursor:default; }
  .source-catalog-grid, .builder-grid { display:grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap:16px; }
  .catalog-list, .selected-list { display:flex; flex-direction:column; gap:8px; }
  .catalog-item, .source-card, .relation-card { border:1px solid #d8e1f0; border-radius:14px; padding:12px; background:#f8fafc; }
  .catalog-item { text-align:left; display:flex; flex-direction:column; gap:4px; }
  .source-row { display:flex; align-items:center; justify-content:space-between; gap:10px; margin-bottom:8px; flex-wrap:wrap; }
  .source-form, .relation-grid { display:grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap:10px; margin-bottom:10px; }
  .relation-grid.compact { grid-template-columns: repeat(3, minmax(0,1fr)); }
  .minor-head { font-size:13px; font-weight:700; margin:0 0 8px; }
  .field-wrap { max-height:120px; overflow:auto; }
  .row-editor { display:grid; grid-template-columns: minmax(0,1fr) minmax(0,1fr) auto; gap:8px; margin-bottom:8px; }
  .preview-card { min-height:320px; }
  .preview-summary { display:flex; gap:10px; flex-wrap:wrap; color:#475569; font-size:12px; margin-bottom:12px; }
  .preview-grid-wrap { overflow:auto; border:1px solid #d8e1f0; border-radius:14px; background:#fff; }
  .preview-grid { width:max-content; min-width:100%; border-collapse:collapse; }
  .preview-grid th, .preview-grid td { border-bottom:1px solid #edf2f7; padding:8px 10px; text-align:left; white-space:nowrap; font-size:12px; vertical-align:top; }
  .preview-grid th { position:sticky; top:0; background:#f8fafc; z-index:1; }
  .secondary-block { margin-top:12px; }
  .secondary-grid { display:grid; grid-template-columns: repeat(3, minmax(0,1fr)); gap:12px; margin:10px 0; font-size:13px; }
  .secondary-list { margin:0; padding-left:18px; display:flex; flex-direction:column; gap:6px; }
  .hint { color:#64748b; font-size:12px; padding:10px 0; }
  @media (max-width: 1200px) {
    .gold-layout { grid-template-columns: 1fr; }
    .top-grid, .source-catalog-grid, .builder-grid, .source-form, .relation-grid, .secondary-grid { grid-template-columns: 1fr; }
    .relation-grid.compact, .row-editor { grid-template-columns: 1fr; }
  }
</style>
