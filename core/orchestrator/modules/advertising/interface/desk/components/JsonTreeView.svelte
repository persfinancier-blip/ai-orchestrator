<script lang="ts">
  export let node: any;
  export let name = '';
  export let level = 0;

  function isObject(v: any) {
    return v && typeof v === 'object' && !Array.isArray(v);
  }

  function valueClass(v: any) {
    if (v === null) return 'null';
    if (typeof v === 'number') return 'number';
    if (typeof v === 'boolean') return 'boolean';
    return 'string';
  }

  function valueText(v: any) {
    if (typeof v === 'string') return JSON.stringify(v);
    if (v === null) return 'null';
    return String(v);
  }
</script>

{#if isObject(node)}
  <details class="json-node" open={level < 1}>
    <summary>
      <span class="key">{name || 'root'}</span>
      <span class="meta">object ({Object.keys(node).length})</span>
    </summary>
    <div class="children">
      {#each Object.entries(node) as [k, v]}
        <svelte:self node={v} name={k} level={level + 1} />
      {/each}
    </div>
  </details>
{:else if Array.isArray(node)}
  <details class="json-node" open={level < 1}>
    <summary>
      <span class="key">{name || 'root'}</span>
      <span class="meta">array [{node.length}]</span>
    </summary>
    <div class="children">
      {#each node as v, i}
        <svelte:self node={v} name={`[${i}]`} level={level + 1} />
      {/each}
    </div>
  </details>
{:else}
  <div class="json-leaf">
    <span class="key">{name}</span>
    <span class="sep">:</span>
    <span class={`val ${valueClass(node)}`}>{valueText(node)}</span>
  </div>
{/if}

<style>
  .json-node {
    margin: 2px 0;
  }

  .json-node > summary {
    cursor: pointer;
    list-style: none;
    display: flex;
    align-items: center;
    gap: 8px;
    font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    font-size: 12px;
    line-height: 1.5;
    user-select: none;
  }

  .json-node > summary::-webkit-details-marker {
    display: none;
  }

  .json-node > summary::before {
    content: '+';
    width: 14px;
    display: inline-flex;
    justify-content: center;
    color: #0f172a;
    font-weight: 700;
  }

  .json-node[open] > summary::before {
    content: '-';
  }

  .children {
    margin-left: 18px;
    border-left: 1px dashed #cbd5e1;
    padding-left: 8px;
  }

  .json-leaf {
    margin-left: 22px;
    font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    font-size: 12px;
    line-height: 1.5;
    display: flex;
    align-items: baseline;
    gap: 6px;
    flex-wrap: wrap;
  }

  .key {
    color: #0f172a;
    font-weight: 600;
    word-break: break-word;
  }

  .sep {
    color: #64748b;
  }

  .meta {
    color: #64748b;
    font-size: 11px;
  }

  .val.string {
    color: #0f766e;
    word-break: break-word;
  }

  .val.number {
    color: #1d4ed8;
  }

  .val.boolean {
    color: #7c3aed;
  }

  .val.null {
    color: #b91c1c;
  }
</style>
