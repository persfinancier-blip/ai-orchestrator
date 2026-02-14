<script lang="ts">
  export type Crumb = { code: string; name: string };
  export let crumbs: Crumb[] = [];
  export let onRemove: (code: string) => void;
</script>

<div class="crumbs">
  {#each crumbs as c (c.code)}
    <button class="crumb" type="button" on:click={() => onRemove(c.code)}>
      <span class="t">{c.name}</span>
      <span class="x">×</span>
    </button>
  {/each}
</div>
