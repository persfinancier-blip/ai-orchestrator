<script lang="ts">
  import { createEventDispatcher } from 'svelte';

  export let node: any;
  export let name = '';
  export let level = 0;
  export let path = '';
  export let pickEnabled = false;

  const dispatch = createEventDispatcher<{
    pickpath: { path: string };
    pickgroup: { path: string };
    pickfields: { path: string };
    pickfield: { path: string };
  }>();

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

  function pickCurrentGroup() {
    if (!path) return;
    dispatch('pickgroup', { path });
  }

  function pickCurrentFields() {
    if (!path) return;
    dispatch('pickfields', { path });
  }

  function pickCurrentField() {
    if (!path) return;
    dispatch('pickfield', { path });
  }

  function forwardPickPath(e: CustomEvent<{ path: string }>) {
    dispatch('pickpath', e.detail);
  }

  function forwardPickGroup(e: CustomEvent<{ path: string }>) {
    dispatch('pickgroup', e.detail);
  }

  function forwardPickFields(e: CustomEvent<{ path: string }>) {
    dispatch('pickfields', e.detail);
  }

  function forwardPickField(e: CustomEvent<{ path: string }>) {
    dispatch('pickfield', e.detail);
  }
</script>

{#if isObject(node)}
  <details class="json-node" open={level < 1}>
    <summary>
      <span class="key">{name || 'root'}</span>
      <span class="meta">объект ({Object.keys(node).length})</span>
      {#if pickEnabled && path}
        <div class="pick-actions">
          <button class="pick-btn pick-group-btn" type="button" aria-label="Добавить как группу" on:click|preventDefault|stopPropagation={pickCurrentGroup}>Группа +</button>
          <button class="pick-btn pick-field-btn" type="button" aria-label="Добавить как поля" on:click|preventDefault|stopPropagation={pickCurrentFields}>Поля +</button>
        </div>
      {/if}
    </summary>
    <div class="children">
      {#each Object.entries(node) as [k, v]}
        <svelte:self
          node={v}
          name={k}
          level={level + 1}
          path={objectChildPath(k)}
          pickEnabled={pickEnabled}
          on:pickpath={forwardPickPath}
          on:pickgroup={forwardPickGroup}
          on:pickfields={forwardPickFields}
          on:pickfield={forwardPickField}
        />
      {/each}
    </div>
  </details>
{:else if Array.isArray(node)}
  <details class="json-node" open={level < 1}>
    <summary>
      <span class="key">{name || 'root'}</span>
      <span class="meta">массив [{node.length}]</span>
      {#if pickEnabled && path}
        <div class="pick-actions">
          <button class="pick-btn pick-group-btn" type="button" aria-label="Добавить как группу" on:click|preventDefault|stopPropagation={pickCurrentGroup}>Группа +</button>
          <button class="pick-btn pick-field-btn" type="button" aria-label="Добавить как поля" on:click|preventDefault|stopPropagation={pickCurrentFields}>Поля +</button>
        </div>
      {/if}
    </summary>
    <div class="children">
      {#each node as v, i}
        <svelte:self
          node={v}
          name={`[${i}]`}
          level={level + 1}
          path={arrayChildPath(i)}
          pickEnabled={pickEnabled}
          on:pickpath={forwardPickPath}
          on:pickgroup={forwardPickGroup}
          on:pickfields={forwardPickFields}
          on:pickfield={forwardPickField}
        />
      {/each}
    </div>
  </details>
{:else}
  <div class="json-leaf">
    <span class="key">{name}</span>
    <span class="sep">:</span>
    <span class={`val ${valueClass(node)}`}>{valueText(node)}</span>
    {#if pickEnabled && path}
      <button class="pick-btn pick-field-btn" type="button" aria-label="Добавить поле" on:click|stopPropagation={pickCurrentField}>Поле +</button>
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
    border: 1px solid #d7deea;
    background: #fff;
    color: #0f172a;
    border-radius: 999px;
    font-size: 11px;
    font-weight: 600;
    line-height: 1.2;
    padding: 3px 8px;
    cursor: pointer;
  }

  .pick-actions {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    flex-wrap: wrap;
  }

  .pick-group-btn {
    color: #0f172a;
  }

  .pick-field-btn {
    color: #166534;
    border-color: #bbf7d0;
    background: #f0fdf4;
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

  .val.null {
    color: #94a3b8;
  }

  .val.number {
    color: #2563eb;
  }

  .val.boolean {
    color: #7c3aed;
  }

  .val.string {
    color: #166534;
    word-break: break-word;
  }
</style>
