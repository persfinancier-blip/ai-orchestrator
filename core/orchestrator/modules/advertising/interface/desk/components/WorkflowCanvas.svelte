<script lang="ts">
  import PivotPreview from './PivotPreview.svelte';
  import { dataCloudOptions, type WorkflowState, type CommandLogItem } from '../data/mockWorkflow';

  export let state: WorkflowState;
  export let commandLog: CommandLogItem[] = [];
  export let previewRows: Array<Record<string, string | number>> = [];
  export let onPreview: () => void = () => {};
  export let onRun: () => void = () => {};
  export let affectedEstimate = 0;

  function addFilter(): void {
    state.filters = [...state.filters, { id: String(Date.now()), field: 'skuId', operator: 'IN', value: 'SKU-100001' }];
  }
  function removeFilter(id: string): void {
    state.filters = state.filters.filter((f) => f.id !== id);
  }
</script>

<section class="panel workflow">
  <h3>Конструктор workflow</h3>
  <div class="flow-grid">
    <div class="block"><h4>1) Облако данных</h4><select bind:value={state.dataCloud}>{#each dataCloudOptions as o}<option>{o}</option>{/each}</select></div>
    <div class="block"><h4>2) Фильтры</h4>
      {#each state.filters as f (f.id)}
        <div class="row"><input bind:value={f.field} /><select bind:value={f.operator}><option>IN</option><option>=</option><option>&gt;</option><option>&lt;</option></select><input bind:value={f.value} /><button on:click={() => removeFilter(f.id)}>×</button></div>
      {/each}
      <button on:click={addFilter}>+ фильтр</button>
    </div>
    <div class="block"><h4>3) KPI / Условия</h4><label>ДРР &gt; <input type="number" bind:value={state.conditionDrr} /></label><label>ROI &lt; <input type="number" step="0.1" bind:value={state.conditionRoi} /></label><label>Остатки (дни) &gt; <input type="number" bind:value={state.conditionStockDays} /></label></div>
    <div class="block"><h4>4) Действие</h4><select bind:value={state.action}><option>Создать SPS/SPM</option><option>Изменить ставки</option><option>Изменить бюджет</option><option>Остановить РК</option></select></div>
    <div class="block"><h4>5) Расписание</h4><select bind:value={state.schedule}><option value="15m">15 мин</option><option value="1h">1 час</option><option value="6h">6 часов</option><option value="24h">24 часа</option></select></div>
    <div class="block run"><h4>6) Запуск / Превью</h4><button on:click={onPreview}>Превью</button><button class="primary" on:click={onRun}>Запуск</button></div>
  </div>

  <div class="estimate">Оценка затронутых SKU: <strong>{affectedEstimate}</strong></div>
  <PivotPreview rows={previewRows} />

  <div class="log">
    <h4>Журнал команд</h4>
    {#each commandLog.slice(0,10) as item}
      <div class="log-row">{item.time} · {item.workflowName} · {item.action} · затронуто {item.affectedCount} · <strong>{item.status}</strong></div>
    {/each}
  </div>
</section>

<style>
  .flow-grid { display:grid; grid-template-columns:repeat(3, 1fr); gap:10px; margin-bottom:12px; }
  .block { border:1px solid #e2e8f0; border-radius:14px; padding:10px; background:#fcfdff; box-shadow:0 4px 14px rgba(15,23,42,.04); }
  h4 { margin:0 0 8px; font-size:12px; color:#64748b; }
  select, input { width:100%; border:1px solid #dbe4f0; border-radius:10px; padding:6px 8px; margin-bottom:6px; }
  .row { display:grid; grid-template-columns:1fr 100px 1fr 28px; gap:6px; }
  .run button { margin-right:8px; }
  .primary { background:#0f172a; color:#fff; }
  .estimate{font-size:12px;color:#475569;margin:8px 0;}
  .log { margin-top:10px; border-top:1px solid #eef2f7; padding-top:10px; }
  .log-row { font-size:12px; color:#475569; margin-bottom:6px; }
</style>
