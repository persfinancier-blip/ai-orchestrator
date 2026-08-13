<script>
  import { createEventDispatcher } from 'svelte';
  import ProductHeader from './ProductHeader.svelte';
  import ProductNavigation from './ProductNavigation.svelte';
  export let section = 'home';
  export let title = 'Обзор';
  const dispatch = createEventDispatcher();
</script>

<div class="shell">
  <ProductNavigation {section} />
  <div class="workspace">
    <ProductHeader {title} on:logout={() => dispatch('logout')} />
    <main><slot /></main>
  </div>
</div>

<style>
  .shell{min-height:100vh;display:grid;grid-template-columns:218px minmax(0,1fr);color:#172033}.workspace{min-width:0;min-height:100vh}main{min-width:0;min-height:calc(100vh - 56px);overflow:auto}@media(max-width:760px){.shell{grid-template-columns:1fr}}
</style>
