<script lang="ts">
  export let rows: Array<Record<string, string | number>> = [];

  let q = '';
  let drrMax = 35;

  type ColType = 'текст' | 'число' | 'процент' | 'день' | 'деньги';
  type Column = { key: string; title: string; type: ColType };

  const columns: Column[] = [
    { key: 'sku', title: 'SKU', type: 'текст' },
    { key: 'spend', title: 'Расход', type: 'деньги' },
    { key: 'orders', title: 'Заказы', type: 'число' },
    { key: 'drr', title: 'ДРР', type: 'процент' },
    { key: 'roi', title: 'ROI', type: 'число' },
    { key: 'stockDays', title: 'Остатки (дни)', type: 'день' },
    { key: 'entryPoint', title: 'Точка входа', type: 'текст' },
    { key: 'targetSegment', title: 'Сегмент', type: 'текст' }
  ];

  $: filtered = rows.filter((r) => {
    const sku = String(r.sku ?? '').toLowerCase();
    const okSku = sku.includes(q.toLowerCase());
    const okDrr = Number(r.drr ?? 0) <= drrMax;
    return okSku && okDrr;
  });

  function formatCell(col: Column, value: string | number | undefined): string {
    if (value === undefined || value === null) return '—';

    if (col.type === 'деньги') return `${Number(value || 0).toLocaleString('ru-RU')} ₽`;
    if (col.type === 'процент') return `${Number(value || 0).toLocaleString('ru-RU')}%`;
    if (col.type === 'день') return `${Number(value || 0).toLocaleString('ru-RU')}`;
    if (col.type === 'число') return Number(value || 0).toLocaleString('ru-RU');
    return String(value);
  }
</script>

<section class="pivot">
  <div class="filters">
    <input bind:value={q} placeholder="Поиск SKU" />
    <label>ДРР ≤ {drrMax}% <input type="range" min="5" max="35" bind:value={drrMax} /></label>
  </div>

  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          {#each columns as c}
            <th>
              <div class="th-title">{c.title}</div>
              <div class="th-type">{c.type}</div>
            </th>
          {/each}
        </tr>
      </thead>

      <tbody>
        {#each filtered as r}
          <tr>
            {#each columns as c}
              <td>{formatCell(c, r[c.key] as any)}</td>
            {/each}
          </tr>
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
  th { position:sticky; top:0; background:#f8fafc; text-align:left; vertical-align:bottom; }

  .th-title { font-weight:600; color:#0f172a; line-height:1.1; }
  .th-type { margin-top:4px; font-size:11px; color:#64748b; line-height:1.1; }
</style>
