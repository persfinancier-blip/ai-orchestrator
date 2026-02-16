<script lang="ts">
  import { onMount } from 'svelte';

  type FieldDef = { field_name: string; field_type: string; description: string };

  type TableDef = {
    id: string;
    schema_name: string;
    table_name: string;
    table_class: string;
    status: string;
    options: Record<string, any>;
    created_by: string;
    created_at: string;
    applied_at?: string | null;
  };

  let items: TableDef[] = [];
  let loading = false;
  let error = '';
  let selectedId: string | null = null;

  let generatedUpSql = '';
  let generatedDownSql = '';

  // MVP role toggle (потом подключим реальный RBAC)
  let role: 'viewer' | 'operator' | 'data_admin' = 'data_admin';

  // Create form
  let schema_name = 'platform';
  let table_class = 'bronze_raw';
  let table_name = 'bronze_raw';

  let options: any = {
    partitions_ahead_days: 30,
    partition_interval_days: 1,
  };

  let fields: FieldDef[] = [];

  const typeOptions = [
    'uuid',
    'text',
    'int4',
    'int8',
    'bool',
    'date',
    'timestamptz',
    'jsonb',
    'numeric(18,2)',
    'numeric(18,4)',
    'numeric(12,2)',
  ];

  async function refreshList() {
    loading = true;
    error = '';
    try {
      const r = await fetch('/ai-orchestrator/api/tables');
      const j = await r.json();
      if (!r.ok) throw new Error(j.error || 'failed_to_list');
      items = j.items || [];
    } catch (e: any) {
      error =
        'Не могу получить список таблиц. Проверь, что API поднят и доступен по /ai-orchestrator/api.\n\n' +
        String(e?.message || e);
    } finally {
      loading = false;
    }
  }

  function addField() {
    fields = [...fields, { field_name: 'new_field', field_type: 'text', description: 'Описание' }];
  }

  function removeField(ix: number) {
    fields = fields.filter((_, i) => i !== ix);
  }

  async function createDraft() {
    error = '';
    generatedUpSql = '';
    generatedDownSql = '';
    selectedId = null;

    try {
      const r = await fetch('/ai-orchestrator/api/tables', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-AO-ROLE': role,
          'X-AO-USER': 'ui',
        },
        body: JSON.stringify({
          schema_name,
          table_name,
          table_class,
          options,
          fields,
        }),
      });
      const j = await r.json();
      if (!r.ok) throw new Error(j.error || 'create_failed');

      selectedId = j.id;
      await refreshList();
    } catch (e: any) {
      error = String(e?.message || e);
    }
  }

  async function generateSql(id: string) {
    error = '';
    generatedUpSql = '';
    generatedDownSql = '';
    selectedId = id;

    try {
      const r = await fetch(`/ai-orchestrator/api/tables/${id}/generate-sql`, { method: 'POST' });
      const j = await r.json();
      if (!r.ok) throw new Error(j.error || 'generate_failed');

      generatedUpSql = j.upSql || '';
      generatedDownSql = j.downSql || '';
    } catch (e: any) {
      error = String(e?.message || e);
    }
  }

  async function apply(id: string) {
    error = '';
    try {
      const r = await fetch(`/ai-orchestrator/api/tables/${id}/apply`, {
        method: 'POST',
        headers: {
          'X-AO-ROLE': role,
          'X-AO-USER': 'ui_admin',
        },
      });
      const j = await r.json();
      if (!r.ok) throw new Error(j.details || j.error || 'apply_failed');

      await refreshList();
    } catch (e: any) {
      error = String(e?.message || e);
    }
  }

  onMount(refreshList);
</script>

<main class="root">
  <header class="header">
    <div>
      <h1>Данные</h1>
      <p class="sub">Создание таблиц (MVP). Сейчас поддерживается: <b>bronze_raw</b>.</p>
    </div>

    <div class="role">
      <span>Role:</span>
      <select bind:value={role}>
        <option value="viewer">viewer</option>
        <option value="operator">operator</option>
        <option value="data_admin">data_admin</option>
      </select>
    </div>
  </header>

  {#if error}
    <div class="alert">
      <div class="alert-title">Ошибка</div>
      <pre>{error}</pre>
    </div>
  {/if}

  <section class="grid">
    <div class="panel">
      <h2>Создать таблицу</h2>

      <div class="form">
        <label>
          Schema
          <select bind:value={schema_name}>
            <option value="platform">platform</option>
            <option value="adv">adv</option>
            <option value="content">content</option>
            <option value="strategy">strategy</option>
          </select>
        </label>

        <label>
          Class
          <select bind:value={table_class}>
            <option value="bronze_raw">bronze_raw</option>
            <option value="silver_table" disabled>silver_table (скоро)</option>
            <option value="showcase_table" disabled>showcase_table (скоро)</option>
          </select>
        </label>

        <label>
          Table name
          <input bind:value={table_name} />
        </label>

        <div class="row">
          <label>
            Partitions ahead (days)
            <input type="number" bind:value={options.partitions_ahead_days} min="0" max="365" />
          </label>

          <label>
            Partition interval (days)
            <input type="number" bind:value={options.partition_interval_days} min="1" max="30" />
          </label>
        </div>
      </div>

      <div class="fields">
        <div class="fields-head">
          <h3>Поля (опционально)</h3>
          <button on:click={addField}>+ Добавить поле</button>
        </div>

        {#if fields.length === 0}
          <p class="hint">
            Для bronze обычно достаточно системных полей (payload, run_id, dataset_name, contract_version, ingested_at…).
          </p>
        {/if}

        {#each fields as f, ix}
          <div class="field-row">
            <input placeholder="field_name" bind:value={f.field_name} />
            <select bind:value={f.field_type}>
              {#each typeOptions as t}
                <option value={t}>{t}</option>
              {/each}
            </select>
            <input placeholder="Описание" bind:value={f.description} />
            <button class="danger" on:click={() => removeField(ix)}>Удалить</button>
          </div>
        {/each}
      </div>

      <div class="actions">
        <button class="primary" on:click={createDraft}>Create draft</button>
        <button on:click={refreshList}>Reload</button>
      </div>
    </div>

    <div class="panel">
      <h2>Таблицы</h2>

      {#if loading}
        <p>Загрузка…</p>
      {:else}
        <div class="list">
          {#each items as t}
            <div class="item" class:active={selectedId === t.id}>
              <div class="meta">
                <div class="title">{t.schema_name}.{t.table_name}</div>
                <div class="sub">{t.table_class} · {t.status}</div>
                <div class="sub">created: {new Date(t.created_at).toLocaleString()}</div>
                {#if t.applied_at}
                  <div class="sub">applied: {new Date(t.applied_at).toLocaleString()}</div>
                {/if}
              </div>

              <div class="btns">
                <button on:click={() => generateSql(t.id)}>Generate SQL</button>
                <button on:click={() => apply(t.id)} disabled={role !== 'data_admin'}>
                  Apply
                </button>
              </div>
            </div>
          {/each}
        </div>
      {/if}
    </div>
  </section>

  <section class="panel">
    <h2>SQL</h2>
    <div class="sql">
      <h3>up.sql</h3>
      <pre>{generatedUpSql || '—'}</pre>

      <h3>down.sql</h3>
      <pre>{generatedDownSql || '—'}</pre>
    </div>
  </section>
</main>

<style>
  .root { padding: 18px; }
  .header { display:flex; justify-content:space-between; gap: 12px; align-items:flex-start; }
  h1 { margin:0; font-size: 24px; font-weight: 700; }
  .sub { margin: 6px 0 0; font-size: 12px; color:#64748b; }

  .role { display:flex; align-items:center; gap: 8px; font-size: 12px; color:#475569; }
  select, input { border: 1px solid #e2e8f0; border-radius: 12px; padding: 8px 10px; }

  .alert { margin: 12px 0; border: 1px solid #fecaca; background:#fff1f2; border-radius: 16px; padding: 12px; }
  .alert-title { font-weight: 700; margin-bottom: 6px; }
  .alert pre { margin: 0; white-space: pre-wrap; font-size: 12px; color:#7f1d1d; }

  .grid { display:grid; grid-template-columns: 1.2fr 1fr; gap: 12px; margin-top: 12px; }
  .panel { background:#fff; border:1px solid #e8edf5; border-radius: 18px; padding: 12px; box-shadow: 0 10px 30px rgba(15,23,42,0.06); }

  .form { display:grid; gap: 10px; }
  label { display:grid; gap: 6px; font-size: 12px; color:#334155; }
  .row { display:grid; grid-template-columns: 1fr 1fr; gap: 10px; }

  .fields { margin-top: 12px; }
  .fields-head { display:flex; justify-content:space-between; align-items:center; }
  .hint { font-size: 12px; color:#64748b; margin: 8px 0 0; }

  .field-row { display:grid; grid-template-columns: 1.2fr 1fr 2fr auto; gap: 8px; margin-top: 8px; }
  button { border: 1px solid #e2e8f0; background:#fff; padding: 8px 10px; border-radius: 12px; cursor:pointer; }
  button.primary { background:#0f172a; color:#fff; border-color:#0f172a; }
  button.danger { border-color:#fecaca; color:#b91c1c; }
  button:disabled { opacity: 0.55; cursor:not-allowed; }

  .actions { display:flex; gap: 8px; margin-top: 12px; }

  .list { display:grid; gap: 8px; margin-top: 8px; }
  .item { border: 1px solid #eef2f7; border-radius: 14px; padding: 10px; display:flex; justify-content:space-between; gap: 10px; }
  .item.active { border-color:#0f172a; }
  .title { font-weight: 700; font-size: 13px; }
  .sub { font-size: 12px; color:#64748b; }
  .btns { display:flex; gap: 8px; align-items:center; }

  .sql pre { background:#0b1220; color:#e2e8f0; padding: 12px; border-radius: 14px; overflow:auto; }
  @media (max-width: 980px) { .grid { grid-template-columns: 1fr; } }
</style>
