<script lang="ts">
  import { createEventDispatcher } from 'svelte';

  export let node: any;
  export let name = '';
  export let level = 0;
  export let path = '';
  export let pickEnabled = false;

  const dispatch = createEventDispatcher<{ pickpath: { path: string } }>();

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

  function objectChildPath(key: string) {
    return path ? `${path}.${key}` : key;
  }

  function arrayChildPath(index: number) {
    return path ? `${path}[${index}]` : `[${index}]`;
  }

  function pickCurrentPath() {
    if (!path) return;
    dispatch('pickpath', { path });
  }

  function forwardPickPath(e: CustomEvent<{ path: string }>) {
    dispatch('pickpath', e.detail);
  }
</script>

{#if isObject(node)}
  <details class="json-node" open={level < 1}>
    <summary>
      <span class="key">{name || 'root'}</span>
      <span class="meta">object ({Object.keys(node).length})</span>
      {#if pickEnabled && path}
        <button class="pick-btn" type="button" title="Добавить в поле ответа" on:click|preventDefault|stopPropagation={pickCurrentPath}>+</button>
      {/if}
    </summary>
    <div class="children">
      {#each Object.entries(node) as [k, v]}
        <svelte:self node={v} name={k} level={level + 1} path={objectChildPath(k)} pickEnabled={pickEnabled} on:pickpath={forwardPickPath} />
      {/each}
    </div>
  </details>
{:else if Array.isArray(node)}
  <details class="json-node" open={level < 1}>
    <summary>
      <span class="key">{name || 'root'}</span>
      <span class="meta">array [{node.length}]</span>
      {#if pickEnabled && path}
        <button class="pick-btn" type="button" title="Добавить в поле ответа" on:click|preventDefault|stopPropagation={pickCurrentPath}>+</button>
      {/if}
    </summary>
    <div class="children">
      {#each node as v, i}
        <svelte:self node={v} name={`[${i}]`} level={level + 1} path={arrayChildPath(i)} pickEnabled={pickEnabled} on:pickpath={forwardPickPath} />
      {/each}
    </div>
  </details>
{:else}
  <div class="json-leaf">
    <span class="key">{name}</span>
    <span class="sep">:</span>
    <span class={`val ${valueClass(node)}`}>{valueText(node)}</span>
    {#if pickEnabled && path}
      <button class="pick-btn" type="button" title="Добавить в поле ответа" on:click|stopPropagation={pickCurrentPath}>+</button>
    {/if}
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
    flex-wrap: wrap;
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

  .pick-btn {
    border: 0;
    background: transparent;
    color: #16a34a;
    border-radius: 999px;
    font-size: 14px;
    font-weight: 700;
    line-height: 1;
    padding: 0 4px;
    cursor: pointer;
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
