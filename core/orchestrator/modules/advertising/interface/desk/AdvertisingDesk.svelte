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

  const sampleColumns = ['sku', 'campaign_id', 'revenue', 'spend', 'date', 'entry_point'];

  $: textFields = showcase.fields.filter((field) => field.kind === 'text');
  $: numberFields = showcase.fields.filter((field) => field.kind === 'number');
  $: dateFields = showcase.fields.filter((field) => field.kind === 'date');
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

  <section class="layout">
    <GraphPanel />

    <section class="panel showcase">
      <div class="showcase-head">
        <h3>Витрина данных</h3>
        <button on:click={regenerateShowcase}>Сгенерировать заново</button>
      </div>

      <div class="field-groups">
        <div>
          <h4>Текстовые поля</h4>
          <ul>{#each textFields as field}<li>{field.name} <span>Текст</span></li>{/each}</ul>
        </div>
        <div>
          <h4>Числовые поля</h4>
          <ul>{#each numberFields as field}<li>{field.name} <span>Число</span></li>{/each}</ul>
        </div>
        <div>
          <h4>Даты</h4>
          <ul>{#each dateFields as field}<li>{field.name} <span>Дата</span></li>{/each}</ul>
        </div>
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

    <WorkflowCanvas state={workflow} {commandLog} {previewRows} {affectedEstimate} onPreview={preview} onRun={runFlow} />
  </section>

  {#if toast}<div class="toast">{toast}</div>{/if}
</main>

<style>
  :global(body){margin:0;font-family:Inter,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f4f6fa;color:#0f172a;}
  .desk-root{padding:18px;}
  header{display:flex;gap:3px;margin-bottom:12px;}
  h1{margin:0;font-size:24px;font-weight:650;letter-spacing:.01em;}
  .layout{display:flex;flex-direction:column;gap:14px;}
  :global(.panel){background:#fff;border:1px solid #e8edf5;border-radius:18px;padding:12px;box-shadow:0 10px 30px rgba(15,23,42,.05);transition:all .2s ease;}
  .showcase{display:flex;flex-direction:column;gap:10px;}
  .showcase-head{display:flex;justify-content:space-between;align-items:center;gap:8px;}
  .showcase-head h3{margin:0;font-size:20px;}
  .showcase-head button{border:1px solid #dbe4f0;border-radius:10px;padding:8px 10px;background:#fff;cursor:pointer;}
  .field-groups{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;}
  .field-groups h4{margin:0 0 6px 0;font-size:14px;}
  .field-groups ul{margin:0;padding:0;list-style:none;display:flex;flex-direction:column;gap:4px;}
  .field-groups li{display:flex;justify-content:space-between;border:1px solid #e2e8f0;border-radius:8px;padding:6px 8px;font-size:12px;}
  .field-groups span{color:#64748b;}
  .table-wrap{overflow:auto;border:1px solid #e2e8f0;border-radius:12px;}
  table{width:100%;border-collapse:collapse;font-size:12px;min-width:780px;}
  th,td{padding:8px;border-bottom:1px solid #edf2f7;text-align:left;white-space:nowrap;}
  th{background:#f8fbff;color:#334155;font-weight:600;}
  .toast{position:fixed;right:20px;bottom:18px;background:#0f172a;color:#fff;padding:10px 14px;border-radius:12px;box-shadow:0 10px 25px rgba(0,0,0,.15);animation:fade .25s ease;}
  @keyframes fade{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
</style>
