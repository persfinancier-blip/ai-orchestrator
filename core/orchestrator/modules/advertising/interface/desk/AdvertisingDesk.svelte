<script lang="ts">
  import { onDestroy } from 'svelte';
  import { get } from 'svelte/store';
  import GraphPanel from './components/GraphPanel.svelte';
  import WorkflowCanvas from './components/WorkflowCanvas.svelte';
  import { initialWorkflow, generatePivotRows, newLog, type CommandLogItem } from './data/mockWorkflow';
  import { showcaseStore } from './data/showcaseStore';

  let showcase = get(showcaseStore);
  const unsubscribe = showcaseStore.subscribe((value) => (showcase = value));

  let workflow = structuredClone(initialWorkflow);
  let commandLog: CommandLogItem[] = [];
  let previewRows = generatePivotRows(20);
  let toast = '';
  let affectedEstimate = 0;

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

  onDestroy(() => unsubscribe());
</script>

<main class="desk-root">
  <header>
    <h1>AI Orchestrator</h1>
  </header>

  <section class="layout">
    <GraphPanel />

    <WorkflowCanvas
      state={workflow}
      {commandLog}
      {previewRows}
      {affectedEstimate}
      onPreview={preview}
      onRun={runFlow}
    />
  </section>

  {#if toast}
    <div class="toast">{toast}</div>
  {/if}
</main>

<style>
  :global(body){
    margin:0;
    font-family:Inter,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
    background:#f4f6fa;
    color:#0f172a;
  }

  .desk-root{
    padding:18px;
  }

  header{
    display:flex;
    gap:3px;
    margin-bottom:12px;
  }

  h1{
    margin:0;
    font-size:24px;
    font-weight:650;
    letter-spacing:.01em;
  }

  .layout{
    display:flex;
    flex-direction:column;
    gap:14px;
  }

  :global(.panel){
    background:#fff;
    border:1px solid #e8edf5;
    border-radius:18px;
    padding:12px;
    box-shadow:0 10px 30px rgba(15,23,42,.05);
    transition:all .2s ease;
  }

  .toast{
    position:fixed;
    right:20px;
    bottom:18px;
    background:#0f172a;
    color:#fff;
    padding:10px 14px;
    border-radius:12px;
    box-shadow:0 10px 25px rgba(0,0,0,.15);
    animation:fade .25s ease;
  }

  @keyframes fade{
    from{opacity:0;transform:translateY(6px)}
    to{opacity:1;transform:translateY(0)}
  }
</style>
