<script lang="ts">
  import { onMount } from 'svelte';

  let clients: Array<Record<string, any>> = [];
  let clientId = 0;
  let loading = true;
  let error = '';

  async function read(url: string) {
    const res = await fetch(url, { cache: 'no-store', headers: { Accept: 'application/json' } });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
  }

  function notify() {
    window.dispatchEvent(new CustomEvent('ao:client-context-changed', { detail: { client_id: clientId } }));
  }

  async function load() {
    loading = true;
    error = '';
    try {
      const [list, context] = await Promise.all([
        read('/ai-orchestrator/api/clients/module/list'),
        read('/ai-orchestrator/api/context/client')
      ]);
      clients = Array.isArray(list?.clients) ? list.clients : [];
      const current = Number(context?.client_id || 0);
      clientId = clients.some((item) => Number(item.id) === current) ? current : Number(clients[0]?.id || 0);
      if (!current && clientId) await save();
    } catch (e) {
      error = String(e?.message || e || 'Контекст клиента недоступен');
    } finally {
      loading = false;
    }
  }

  async function save() {
    if (!(clientId > 0)) return;
    const res = await fetch('/ai-orchestrator/api/context/client', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ client_id: clientId })
    });
    if (!res.ok) error = `Не удалось выбрать клиента: HTTP ${res.status}`;
    else {
      error = '';
      notify();
    }
  }

  onMount(load);
</script>

<label title={error || 'Рабочий контекст клиента'} class:error={Boolean(error)}>
  <small>Клиент</small>
  <select bind:value={clientId} on:change={save} disabled={loading || !clients.length}>
    {#if loading}<option value={0}>Загрузка…</option>
    {:else if !clients.length}<option value={0}>Нет клиентов</option>
    {:else}{#each clients as client}<option value={client.id}>{client.client_display_name || client.client_code}</option>{/each}{/if}
  </select>
</label>

<style>
  label { display: flex; align-items: center; gap: 6px; }
  small { color: #94a3b8; font-size: 9px; font-weight: 700; text-transform: uppercase; letter-spacing: .08em; }
  select { min-width: 150px; max-width: 240px; border: 1px solid #dfe5ed; border-radius: 8px; padding: 6px 8px; background: #fff; color: #334155; font-size: 10px; }
  label.error select { border-color: #fda4af; }
  @media (max-width: 650px) { small { display: none; } select { min-width: 110px; max-width: 140px; } }
</style>
