<script>
  import { createEventDispatcher } from 'svelte';
  import ClientContextBar from './ClientContextBar.svelte';
  export let title = 'Обзор';
  const dispatch = createEventDispatcher();
</script>

<header>
  <span><small>Рабочее пространство</small><strong>{title}</strong></span>
  <div><ClientContextBar /><button on:click={() => dispatch('logout')}>Выйти</button></div>
</header>

<style>
  header { position: sticky; top: 0; z-index: 20; height: 56px; padding: 9px 17px; box-sizing: border-box; border-bottom: 1px solid #e4e9f1; background: rgba(255,255,255,.94); display: flex; align-items: center; justify-content: space-between; gap: 12px; } header > span { display: flex; flex-direction: column; } small { color: #94a3b8; font-size: 9px; } strong { font-size: 13px; } header > div { display: flex; align-items: center; gap: 9px; } button { border: 1px solid #dfe5ed; border-radius: 8px; padding: 6px 9px; background: #fff; color: #64748b; font-size: 10px; cursor: pointer; }
  @media (max-width: 760px) { header { top: 43px; } }
</style>
