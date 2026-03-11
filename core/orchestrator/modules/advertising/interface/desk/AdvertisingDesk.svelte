<script lang="ts">
  import { onDestroy } from 'svelte';
  import { get } from 'svelte/store';
  import GraphPanel from './components/GraphPanel.svelte';
  import WorkflowCanvas from './components/WorkflowCanvas.svelte';
  import { initialWorkflow, generatePivotRows, newLog, type CommandLogItem } from './data/mockWorkflow';
  import { showcaseStore, regenerateShowcase, type ShowcaseField } from './data/showcaseStore';

  let showcase = get(showcaseStore);
  const unsubscribe = showcaseStore.subscribe((value) => (showcase = value));

  let workflow = structuredClone(initialWorkflow);
  let commandLog: CommandLogItem[] = [];
  let previewRows = generatePivotRows(20);
  let toast = '';
  let affectedEstimate = 0;

  let showVisualization = true;
  let showWorkspace = true;
  let showShowcase = true;

  const sampleColumns = ['sku', 'campaign_id', 'revenue', 'spend', 'date', 'entry_point'];

  $: sampleRows = showcase.rows.slice(0, 15);

  function preview(): void {
    previewRows = generatePivotRows(20);
    affectedEstimate = 90 + Math.round(Math.random() * 1100);
  }

  function runFlow(): void {
    const affected = affectedEstimate || (120 + Math.round(Math.random() * 900));
    commandLog = [newLog(workflow.workflowName, workflow.action, affected), ...commandLog].slice(0, 10);
    toast = 'Сформирован тестовый запуск';
    setTimeout(() => (toast = ''), 1400);
  }

  function formatCell(field: ShowcaseField | undefined, value: unknown): string {
    if (!field) return String(value ?? '');
    if (field.kind === 'date') {
      const raw = String(value ?? '');
      const [y, m, d] = raw.split('-');
      return y && m && d ? `${d}.${m}.${y}` : raw;
    }
    if (field.kind === 'number') {
      if (field.code === 'revenue' || field.code === 'spend') return `${Number(value ?? 0).toLocaleString('ru-RU')} ₽`;
      return Number(value ?? 0).toLocaleString('ru-RU');
    }
    return String(value ?? '');
  }

  function getField(code: string): ShowcaseField | undefined {
    return showcase.fields.find((field) => field.code === code);
  }

  function rowCell(row: Record<string, unknown>, column: string): unknown {
    return row[column];
  }

  onDestroy(() => unsubscribe());
</script>

<main class="desk-root">
  <header>
    <h1>AI Orchestrator</h1>
  </header>

  <div class="section-toggles" role="group" aria-label="Переключение разделов">
    <label class="section-toggle">
      <input type="checkbox" bind:checked={showVisualization} />
      <span class="dot" aria-hidden="true"></span>
      <span>Визуализация</span>
    </label>
    <label class="section-toggle">
      <input type="checkbox" bind:checked={showWorkspace} />
      <span class="dot" aria-hidden="true"></span>
      <span>Рабочий стол</span>
    </label>
    <label class="section-toggle">
      <input type="checkbox" bind:checked={showShowcase} />
      <span class="dot" aria-hidden="true"></span>
      <span>Витрина</span>
    </label>
  </div>

  <section class="layout">
    {#if showVisualization}
      <GraphPanel />
    {/if}

    {#if showShowcase}
      <section class="panel showcase">
        <div class="showcase-head">
          <h3>Витрина данных</h3>
          <button on:click={regenerateShowcase}>Сгенерировать заново</button>
        </div>

        <div class="table-wrap">
          <table>
            <thead><tr>{#each sampleColumns as column}<th>{getField(column)?.name ?? column}</th>{/each}</tr></thead>
            <tbody>
              {#each sampleRows as row}
                <tr>
                  {#each sampleColumns as column}
                    <td>{formatCell(getField(column), rowCell(row, column))}</td>
                  {/each}
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      </section>
    {/if}

    {#if showWorkspace}
      <WorkflowCanvas state={workflow} {commandLog} {previewRows} {affectedEstimate} onPreview={preview} onRun={runFlow} />
    {/if}

    {#if !showVisualization && !showWorkspace && !showShowcase}
      <section class="panel section-empty">Выберите хотя бы один раздел.</section>
    {/if}
  </section>

  {#if toast}<div class="toast">{toast}</div>{/if}
</main>

<style>
  :global(body){margin:0;font-family:Inter,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f4f6fa;color:#0f172a;}
  .desk-root{padding:18px;}
  header{display:flex;gap:3px;margin-bottom:12px;}
  h1{margin:0;font-size:24px;font-weight:650;letter-spacing:.01em;}
  .section-toggles{display:flex;flex-wrap:wrap;gap:10px;margin:0 0 12px 0;}
  .section-toggle{display:inline-flex;align-items:center;gap:8px;border:1px solid #dbe4f0;border-radius:999px;padding:7px 12px;background:#fff;font-size:13px;color:#0f172a;cursor:pointer;user-select:none;}
  .section-toggle input{position:absolute;opacity:0;width:1px;height:1px;pointer-events:none;}
  .section-toggle .dot{width:12px;height:12px;border-radius:50%;border:2px solid #94a3b8;background:#fff;box-sizing:border-box;transition:all .15s ease;}
  .section-toggle input:checked + .dot{border-color:#0f172a;background:#0f172a;}
  .layout{display:flex;flex-direction:column;gap:14px;}
  :global(.panel){background:#fff;border:1px solid #e8edf5;border-radius:18px;padding:12px;box-shadow:0 10px 30px rgba(15,23,42,.05);transition:all .2s ease;}
  .showcase{display:flex;flex-direction:column;gap:10px;}
  .showcase-head{display:flex;justify-content:space-between;align-items:center;gap:8px;}
  .showcase-head h3{margin:0;font-size:20px;}
  .showcase-head button{border:1px solid #dbe4f0;border-radius:10px;padding:8px 10px;background:#fff;cursor:pointer;}
  .table-wrap{overflow:auto;border:1px solid #e2e8f0;border-radius:12px;}
  table{width:100%;border-collapse:collapse;font-size:12px;min-width:780px;}
  th,td{padding:8px;border-bottom:1px solid #edf2f7;text-align:left;white-space:nowrap;}
  th{background:#f8fbff;color:#334155;font-weight:600;}
  .section-empty{font-size:14px;color:#475569;}
  .toast{position:fixed;right:20px;bottom:18px;background:#0f172a;color:#fff;padding:10px 14px;border-radius:12px;box-shadow:0 10px 25px rgba(0,0,0,.15);animation:fade .25s ease;}
  @keyframes fade{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
</style>
