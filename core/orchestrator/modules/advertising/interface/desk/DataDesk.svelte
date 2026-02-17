<script lang="ts">
  import { onMount } from 'svelte';

  type ColumnDef = { field_name: string; field_type: string; description?: string };
  type ExistingTable = { schema_name: string; table_name: string };

  let role: 'viewer' | 'operator' | 'data_admin' = 'data_admin';

  let loading = false;
  let error = '';

  let existingTables: ExistingTable[] = [];

  // --- DB admin credentials (stored in localStorage) ---
  let db_admin_user = '';
  let db_admin_password = '';
  let showPassword = false;

  const LS_USER = 'ao_db_admin_user';
  const LS_PASS = 'ao_db_admin_password';

  function loadCreds() {
    try {
      db_admin_user = localStorage.getItem(LS_USER) || '';
      db_admin_password = localStorage.getItem(LS_PASS) || '';
    } catch (_e) {}
  }
  function saveCreds() {
    try {
      localStorage.setItem(LS_USER, db_admin_user || '');
      localStorage.setItem(LS_PASS, db_admin_password || '');
    } catch (_e) {}
  }
  function clearCreds() {
    db_admin_user = '';
    db_admin_password = '';
    saveCreds();
  }

  // Конструктор: пользователь вводит сам
  let schema_name = '';
  let table_name = '';
  let table_class = 'custom';
  let description = '';

  const typeOptions = ['text', 'int', 'bigint', 'numeric', 'boolean', 'date', 'timestamptz', 'jsonb', 'uuid'];
  let columns: ColumnDef[] = [{ field_name: '', field_type: 'text', description: '' }];

  // Партиционирование
  let partition_enabled = false;
  let partition_column = 'event_date';
  let partition_interval: 'day' | 'month' = 'day';

  // Тестовая запись (JSON)
  let test_row_text = '';

  // Предпросмотр
  let preview_schema = '';
  let preview_table = '';
  let preview_rows: any[] = [];
  let preview_error = '';
  let preview_loading = false;

  function setErr(e: any) {
    error = String(e?.details || e?.message || e);
  }

  function apiHeaders(extra: Record<string, string> = {}) {
    // ВАЖНО: передаем креды только если заполнены
    const h: Record<string, string> = {
      'Content-Type': 'application/json',
      'X-AO-ROLE': role,
      ...extra
    };

    if (db_admin_user.trim()) h['X-PG-USER'] = db_admin_user.trim();
    if (db_admin_password) h['X-PG-PASSWORD'] = db_admin_password;

    return h;
  }

  async function refresh() {
    loading = true;
    error = '';
    try {
      const r = await fetch('/ai-orchestrator/api/tables', {
        headers: apiHeaders({ 'Content-Type': 'application/json' })
      });
      const j = await r.json();
      if (!r.ok) throw new Error(j.error || 'failed_to_list');

      existingTables = j.existing_tables || [];

      if (!preview_schema && existingTables.length) {
        preview_schema = existingTables[0].schema_name;
        preview_table = existingTables[0].table_name;
      }
    } catch (e: any) {
      setErr(e);
    } finally {
      loading = false;
    }
  }

  function addColumn() {
    columns = [...columns, { field_name: '', field_type: 'text', description: '' }];
  }

  function removeColumn(ix: number) {
    columns = columns.filter((_, i) => i !== ix);
    if (!columns.length) columns = [{ field_name: '', field_type: 'text', description: '' }];
  }

  function parseTestRow(): any | null {
    const t = test_row_text.trim();
    if (!t) return null;
    const parsed = JSON.parse(t);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new Error('Тестовая запись должна быть JSON объектом (например {"a":1})');
    }
    return parsed;
  }

  function validateCreateForm(): string | null {
    if (role !== 'data_admin') return 'Недостаточно прав (роль должна быть data_admin)';
    if (!schema_name.trim()) return 'Заполни "Схема"';
    if (!table_name.trim()) return 'Заполни "Таблица"';

    const goodCols = columns.filter((c) => c.field_name.trim());
    if (goodCols.length === 0) return 'Добавь хотя бы одно поле (field_name)';

    // если есть "тестовая запись" — она должна быть валидным JSON
    if (test_row_text.trim()) {
      try {
        parseTestRow();
      } catch (e: any) {
        return e?.message || 'Некорректный JSON в тестовой записи';
      }
    }

    // Если включили партиции — проверим колонку
    if (partition_enabled && !partition_column.trim()) return 'Заполни колонку для партиций';

    return null;
  }

  async function createTableNow() {
    error = '';
    const v = validateCreateForm();
    if (v) {
      error = v;
      return;
    }

    try {
      const test_row = parseTestRow();

      // прямое создание (без черновиков)
      const r = await fetch('/ai-orchestrator/api/tables/create', {
        method: 'POST',
        headers: apiHeaders(),
        body: JSON.stringify({
          schema_name: schema_name.trim(),
          table_name: table_name.trim(),
          table_class,
          description,
          columns: columns
            .filter((c) => c.field_name.trim())
            .map((c) => ({
              field_name: c.field_name.trim(),
              field_type: c.field_type,
              description: (c.description || '').trim()
            })),
          partitioning: partition_enabled
            ? { enabled: true, column: partition_column.trim(), interval: partition_interval }
            : { enabled: false },
          test_row
        })
      });

      const j = await r.json();
      if (!r.ok) throw new Error(j.details || j.error || 'create_failed');

      await refresh();
      alert(`Таблица создана: ${schema_name.trim()}.${table_name.trim()}`);
    } catch (e: any) {
      setErr(e);
    }
  }

  async function loadPreview() {
    preview_loading = true;
    preview_error = '';
    preview_rows = [];
    try {
      const url =
        `/ai-orchestrator/api/preview?schema=${encodeURIComponent(preview_schema)}` +
        `&table=${encodeURIComponent(preview_table)}&limit=5`;

      const r = await fetch(url, { headers: apiHeaders({ 'Content-Type': 'application/json' }) });
      const j = await r.json();
      if (!r.ok) throw new Error(j.details || j.error || 'preview_failed');
      preview_rows = j.rows || [];
    } catch (e: any) {
      preview_error = String(e?.message || e);
    } finally {
      preview_loading = false;
    }
  }

  // Шаблон (служебно) — оставляем, но кнопки в отдельном блоке
  function fillTemplateBronze() {
    schema_name = 'bronze';
    table_name = 'wb_ads_raw';
    table_class = 'bronze_raw';
    description = 'Сырые ответы API (append-only JSON)';
    columns = [
      { field_name: 'dataset', field_type: 'text', description: 'dataset name' },
      { field_name: 'endpoint', field_type: 'text', description: 'endpoint name' },
      { field_name: 'ingested_at', field_type: 'timestamptz', description: 'время ingest' },
      { field_name: 'event_date', field_type: 'date', description: 'бизнес-дата' },
      { field_name: 'idempotency_key', field_type: 'text', description: 'ключ дедупликации' },
      { field_name: 'payload', field_type: 'jsonb', description: 'сырой JSON' }
    ];
    partition_enabled = true;
    partition_column = 'event_date';
    partition_interval = 'day';
    test_row_text = `{"dataset":"ads","event_date":"2026-02-17","payload":{"a":1}}`;
  }

  onMount(() => {
    loadCreds();
    refresh();
  });

  $: saveCreds();
</script>

<main class="root">
  <header class="header">
    <div>
      <h1>Конструктор таблиц</h1>
      <p class="sub">
        Создаёт схему/таблицу/поля по твоему вводу — опции: партиции, тестовая запись. Показывает текущие таблицы и предпросмотр 5 строк.
      </p>
    </div>

    <div class="role">
      <span>Роль:</span>
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
    <!-- LEFT: CREATE -->
    <div class="panel">
      <div class="panel-head">
        <h2>Создать таблицу</h2>
        <div class="quick">
          <button on:click={refresh}>Обновить список</button>
        </div>
      </div>

      <!-- DB ADMIN CREDS -->
      <div class="panel2">
        <h3>Доступ к базе (вводишь руками)</h3>
        <p class="hint">
          Эти поля сохраняются в браузере (localStorage) и не пропадут при обновлении/переходах.
          <br />
          ⚠️ Не используй супер-админ пароль, если эта страница доступна не только тебе.
        </p>

        <div class="form">
          <label>
            Логин администратора БД
            <input bind:value={db_admin_user} placeholder="например: ao_ai_orchestrator / postgres / ..." />
          </label>

          <label>
            Пароль администратора БД
            <input bind:value={db_admin_password} type={showPassword ? 'text' : 'password'} placeholder="пароль" />
          </label>

          <label class="row">
            <input type="checkbox" bind:checked={showPassword} />
            <span>Показать пароль</span>
          </label>

          <div class="actions">
            <button class="danger" on:click={clearCreds}>Очистить</button>
          </div>
        </div>
      </div>

      <!-- SERVICE MENU -->
      <details class="panel2">
        <summary><strong>Меню: шаблоны (служебное)</strong></summary>
        <div style="margin-top:10px;">
          <p class="hint">
            Шаблон — это просто пример заполнения полей. Можно игнорировать и заполнять всё вручную.
          </p>
          <button on:click={fillTemplateBronze}>Заполнить: шаблон Bronze</button>
        </div>
      </details>

      <div class="form" style="margin-top:12px;">
        <label>
          Схема
          <input bind:value={schema_name} placeholder="например: bronze / silver_adv / showcase" />
        </label>

        <label>
          Таблица
          <input bind:value={table_name} placeholder="например: wb_ads_raw" />
        </label>

        <label>
          Класс (для себя)
          <select bind:value={table_class}>
            <option value="custom">custom</option>
            <option value="bronze_raw">bronze_raw</option>
            <option value="silver_table">silver_table</option>
            <option value="showcase_table">showcase_table</option>
          </select>
        </label>

        <label>
          Описание
          <input bind:value={description} placeholder="что это за таблица" />
        </label>
      </div>

      <div class="fields">
        <div class="fields-head">
          <h3>Поля</h3>
        </div>

        {#each columns as c, ix}
          <div class="field-row">
            <input placeholder="показатель (field_name)" bind:value={c.field_name} />
            <select bind:value={c.field_type}>
              {#each typeOptions as t}
                <option value={t}>{t}</option>
              {/each}
            </select>
            <input placeholder="описание" bind:value={c.description} />
            <button class="danger" on:click={() => removeColumn(ix)}>Удалить</button>
          </div>
        {/each}

        <!-- move add button to bottom -->
        <div class="actions" style="justify-content:flex-end;">
          <button on:click={addColumn}>+ Добавить поле</button>
        </div>
      </div>

      <div class="panel2">
        <h3>Партиционирование</h3>
        <label class="row">
          <input type="checkbox" bind:checked={partition_enabled} />
          <span>Включить партиции</span>
        </label>

        {#if partition_enabled}
          <div class="form">
            <label>
              Колонка для партиций
              <input bind:value={partition_column} placeholder="event_date / ingested_at / date ..." />
            </label>
            <label>
              Интервал
              <select bind:value={partition_interval}>
                <option value="day">day</option>
                <option value="month">month</option>
              </select>
            </label>
          </div>
        {/if}
      </div>

      <div class="panel2">
        <h3>Тестовая запись (опционально)</h3>
        <p class="hint">Если заполнить JSON — одна строка будет вставлена сразу после создания таблицы.</p>
        <textarea
          bind:value={test_row_text}
          placeholder={`{"dataset":"ads","event_date":"2026-02-17","payload":{"a":1}}`}
        />
      </div>

      <div class="actions">
        <button class="primary" on:click={createTableNow} disabled={role !== 'data_admin'}>
          Создать таблицу
        </button>
      </div>
    </div>

    <!-- RIGHT: EXISTING + PREVIEW -->
    <div class="panel">
      <div class="panel-head">
        <h2>Текущие таблицы</h2>
      </div>

      {#if loading}
        <p>Загрузка…</p>
      {:else if existingTables.length === 0}
        <p class="hint">Таблиц пока нет (или нет доступа).</p>
      {:else}
        <div class="chips">
          {#each existingTables as t}
            <span class="chip">{t.schema_name}.{t.table_name}</span>
          {/each}
        </div>
      {/if}

      <h2 style="margin-top:16px;">Предпросмотр (5 строк)</h2>

      {#if preview_error}
        <div class="alert">
          <div class="alert-title">Ошибка предпросмотра</div>
          <pre>{preview_error}</pre>
        </div>
      {/if}

      <div class="form">
        <label>
          Схема
          <select bind:value={preview_schema} on:change={() => (preview_rows = [])}>
            {#each Array.from(new Set(existingTables.map((t) => t.schema_name))) as s}
              <option value={s}>{s}</option>
            {/each}
          </select>
        </label>

        <label>
          Таблица
          <select bind:value={preview_table} on:change={() => (preview_rows = [])}>
            {#each existingTables.filter((t) => t.schema_name === preview_schema) as t}
              <option value={t.table_name}>{t.schema_name}.{t.table_name}</option>
            {/each}
          </select>
        </label>
      </div>

      <div class="actions">
        <button on:click={loadPreview} disabled={preview_loading}>
          {preview_loading ? 'Загрузка…' : 'Показать 5 строк'}
        </button>
      </div>

      {#if preview_rows.length}
        <div class="preview">
          <table>
            <thead>
              <tr>
                {#each Object.keys(preview_rows[0]) as k}
                  <th>{k}</th>
                {/each}
              </tr>
            </thead>
            <tbody>
              {#each preview_rows as r}
                <tr>
                  {#each Object.keys(preview_rows[0]) as k}
                    <td>{typeof r[k] === 'object' ? JSON.stringify(r[k]) : String(r[k])}</td>
                  {/each}
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      {:else}
        <p class="hint">Нет данных для предпросмотра (таблица может быть пустой).</p>
      {/if}
    </div>
  </section>
</main>

<style>
  .root { padding: 18px; background:#f8fafc; min-height: 100vh; }
  .header { display:flex; justify-content:space-between; gap:12px; align-items:flex-start; }
  h1 { margin:0; font-size: 20px; font-weight: 800; }
  .sub { margin: 6px 0 0; font-size: 12px; color:#64748b; max-width: 900px; }
  .role { display:flex; gap:8px; align-items:center; font-size: 12px; color:#334155; }

  .grid { display:grid; grid-template-columns: 1.15fr 0.85fr; gap: 12px; margin-top: 12px; }
  @media (max-width: 1100px) { .grid { grid-template-columns: 1fr; } }

  .panel { background:#fff; border:1px solid #eef2f7; border-radius: 16px; padding: 14px; }
  .panel2 { margin-top: 12px; border:1px dashed #e2e8f0; border-radius: 14px; padding: 12px; background:#ffffff; }
  .panel-head { display:flex; justify-content:space-between; align-items:center; gap:8px; }
  .quick { display:flex; gap: 8px; }

  .form { display:grid; gap: 10px; margin-top: 12px; }
  label { display:grid; gap: 6px; font-size: 12px; color:#334155; }
  input, select, textarea { padding: 10px 12px; border-radius: 12px; border:1px solid #e2e8f0; outline: none; }
  textarea { min-height: 90px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }

  .fields { margin-top: 14px; }
  .fields-head { display:flex; align-items:center; justify-content:space-between; gap: 8px; }
  .field-row { display:grid; grid-template-columns: 1.1fr 0.7fr 1.6fr auto; gap: 8px; margin-top: 8px; }

  .row { display:flex; gap:8px; align-items:center; font-size: 12px; color:#334155; }

  .actions { display:flex; gap: 8px; margin-top: 12px; }
  button { padding: 10px 12px; border-radius: 12px; border:1px solid #e2e8f0; background:#fff; cursor:pointer; }
  button.primary { background:#0f172a; color:#fff; border-color:#0f172a; }
  button:disabled { opacity:0.5; cursor:not-allowed; }
  .danger { border:1px solid #fecaca; color:#b91c1c; }

  .hint { margin-top: 10px; font-size: 12px; color:#64748b; }

  .alert { background:#fff1f2; border:1px solid #fecdd3; border-radius: 14px; padding: 12px; margin: 12px 0; }
  .alert-title { font-weight: 800; margin-bottom: 6px; }
  pre { white-space: pre-wrap; margin:0; font-size: 12px; }

  .preview { margin-top: 10px; overflow:auto; border:1px solid #eef2f7; border-radius: 14px; }
  table { border-collapse: collapse; width: 100%; font-size: 12px; }
  th, td { border-bottom:1px solid #eef2f7; padding: 8px 10px; text-align:left; vertical-align:top; }
  th { position: sticky; top: 0; background:#fff; }

  .chips { display:flex; flex-wrap:wrap; gap:6px; margin-top:10px; }
  .chip { font-size: 12px; padding: 6px 10px; border:1px solid #e2e8f0; border-radius: 999px; background:#fff; color:#334155; }
</style>
