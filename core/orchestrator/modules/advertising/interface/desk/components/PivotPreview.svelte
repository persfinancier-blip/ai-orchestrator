<script lang="ts">
  export let rows: Array<Record<string, string | number>> = [];
  let q = '';
  let drrMax = 35;
  $: filtered = rows.filter((r) => String(r.sku).toLowerCase().includes(q.toLowerCase()) && Number(r.drr) <= drrMax);
</script>

<section class="pivot">
  <div class="filters">
    <input bind:value={q} placeholder="Search SKU" />
    <label>DRR ≤ {drrMax}% <input type="range" min="5" max="35" bind:value={drrMax} /></label>
  </div>
  <div class="table-wrap">
    <table>
      <thead><tr><th>SKU</th><th>Spend</th><th>Orders</th><th>DRR</th><th>ROI</th><th>StockDays</th><th>EntryPoint</th><th>TargetSegment</th></tr></thead>
      <tbody>
        {#each filtered as r}
          <tr><td>{r.sku}</td><td>{r.spend}</td><td>{r.orders}</td><td>{r.drr}</td><td>{r.roi}</td><td>{r.stockDays}</td><td>{r.entryPoint}</td><td>{r.targetSegment}</td></tr>
        {/each}
      </tbody>
    </table>
  </div>
</section>

<style>
  .filters { display:flex; justify-content:space-between; gap:10px; margin-bottom:8px; font-size:12px; color:#64748b; }
  input { border:1px solid #e2e8f0; border-radius:10px; padding:6px 10px; }
  .table-wrap { max-height:220px; overflow:auto; border:1px solid #e2e8f0; border-radius:12px; }
  table { width:100%; border-collapse:collapse; font-size:12px; }
  th, td { padding:8px; border-bottom:1px solid #f1f5f9; white-space:nowrap; }
  th { position:sticky; top:0; background:#f8fafc; text-align:left; }
</style>
