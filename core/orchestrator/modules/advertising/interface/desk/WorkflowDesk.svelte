<script lang="ts">
  import WorkflowCanvas from './components/WorkflowCanvas.svelte';
</script>

<main class="workflow-desk-root">
  <header>
    <h1>AI Orchestrator · Workflow</h1>
  </header>

  <WorkflowCanvas />
</main>

<style>
  :global(body) {
    margin: 0;
    font-family: Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: #f4f6fa;
    color: #0f172a;
  }

  .workflow-desk-root {
    padding: 18px;
  }

  header {
    display: flex;
    gap: 3px;
    margin-bottom: 12px;
  }

  h1 {
    margin: 0;
    font-size: 24px;
    font-weight: 650;
    letter-spacing: 0.01em;
  }

  :global(.panel) {
    background: #fff;
    border: 1px solid #e8edf5;
    border-radius: 18px;
    padding: 12px;
    box-shadow: 0 10px 30px rgba(15, 23, 42, 0.05);
    transition: all 0.2s ease;
  }
</style>
