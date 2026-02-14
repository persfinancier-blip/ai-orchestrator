```svelte
<!-- core/orchestrator/modules/advertising/interface/desk/components/PivotPreview.svelte -->
<script lang="ts">
  export type PivotColumn = {
    key: string;
    title: string;
    kind?: 'text' | 'number' | 'date';
  };

  export let columns: PivotColumn[] = [];
  export let rows: Array<Record<string, unknown>> = [];

  function formatDate(value: unknown): string {
    const v = String(value ?? '');
    // ожидаем YYYY-MM-DD
    const parts = v.split('-');
    if (parts.length === 3 && parts[0] && parts[1] && parts[2]) {
      const [y, m, d] = parts;
      return `${d}.${m}.${y}`;
    }
    return v;
  }

  function formatNumber(value: unknown): string {
    const n = typeof value === 'number' ? value : Number(value);
    if (!Number.isFinite(n)) return '—';
    return n.toLocaleString('ru-RU');
  }

  function formatCell(col: PivotColumn, value: unknown): string {
    if (value === null || value === undefined || value === '') return '—';
    if (col.kind === 'date') return formatDate(value);
    if (col.kind === 'number') return formatNumber(value);
    return String(value);
  }
</script>

<div class="pivot-preview">
  <table>
    <thead>
      <tr>
        {#each columns as c}
          <th title={c.key}>{c.title}</th>
        {/each}
      </tr>
    </thead>

    <tbody>
      {#if rows.length === 0}
        <tr>
          <td class="empty" colspan={Math.max(1, columns.length)}>Нет данных</td>
        </tr>
      {:else}
        {#each rows as r}
          <tr>
            {#each columns as c}
              <td>{formatCell(c, r[c.key])}</td>
            {/each}
          </tr>
        {/each}
      {/if}
    </tbody>
  </table>
</div>

<style>
  .pivot-preview {
    width: 100%;
    overflow: auto;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    background: #fff;
  }

  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 12px;
  }

  thead th {
    position: sticky;
    top: 0;
    background: #f8fafc;
    color: #334155;
    text-align: left;
    padding: 8px 10px;
    border-bottom: 1px solid #e2e8f0;
    white-space: nowrap;
  }

  tbody td {
    padding: 8px 10px;
    border-bottom: 1px solid #f1f5f9;
    color: #0f172a;
    white-space: nowrap;
  }

  tbody tr:hover td {
    background: #f8fbff;
  }

  .empty {
    color: #64748b;
    text-align: center;
    padding: 14px 10px;
  }
</style>
```
