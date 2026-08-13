<script>
  import { onMount } from 'svelte';
  import ProductStats from './ProductStats.svelte';
  import { productGet, productPost } from './productApi.js';

  let containers = [];
  let currentId = null;
  let name = '';
  let creating = false;
  let error = '';

  const flow = [
    ['1','Собрать','Описать задачу, проверить источник и автоматически собрать сценарий.','#assistant'],
    ['2','Привести','Проверить таблицы и подготовить понятную модель данных.','#desk/tables'],
    ['3','Исследовать','Найти сильные связи, кластеры и аномалии в 3D.','#legacy'],
    ['4','Спрогнозировать','Получить диапазон будущих результатов и вероятность достижения цели.','#legacy'],
    ['5','Действовать','Превратить решение в автоматическое действие workflow.','#desk/data']
  ];

  async function loadContainers() {
    try {
      const [list, context] = await Promise.all([productGet('/product/containers'), productGet('/context/container').catch(() => ({}))]);
      containers = Array.isArray(list?.containers) ? list.containers : [];
      currentId = context?.container_id || null;
    } catch (e) { error = String(e?.message || e); }
  }

  async function createContainer() {
    if (!name.trim()) return;
    creating = true; error = '';
    try {
      const result = await productPost('/product/containers/upsert', { name: name.trim(), kind: 'project', description: '', status: 'active' });
      const id = result?.container?.id;
      if (id) {
        await productPost('/context/container', { container_id: id });
        currentId = id;
        name = '';
        window.dispatchEvent(new CustomEvent('ao:container-context-refresh'));
        window.dispatchEvent(new CustomEvent('ao:container-context-changed', { detail: { container_id: id } }));
      }
      await loadContainers();
    } catch (e) { error = String(e?.message || e); }
    finally { creating = false; }
  }

  async function choose(id) {
    await productPost('/context/container', { container_id: id });
    currentId = id;
    window.dispatchEvent(new CustomEvent('ao:container-context-refresh'));
    window.dispatchEvent(new CustomEvent('ao:container-context-changed', { detail: { container_id: id } }));
  }

  onMount(loadContainers);
</script>

<section class="home">
  <header><span>AI Orchestrator</span><h1>От данных к решению</h1><p>Работа начинается с контейнера. Workflow, таблицы и модели остаются внутри него и не мешают простому пользовательскому сценарию.</p></header>

  <section class="containers">
    <div class="head"><div><small>Рабочий контекст</small><strong>Контейнер</strong></div><form on:submit|preventDefault={createContainer}><input bind:value={name} placeholder="Новый контейнер"/><button disabled={creating || !name.trim()}>Создать</button></form></div>
    <div class="chips">
      {#each containers as item (item.id)}<button class:active={Number(currentId)===Number(item.id)} on:click={() => choose(item.id)}><b>{item.name}</b><span>{item.kind}</span></button>{/each}
      {#if !containers.length}<span class="empty">Создай контейнер клиента, проекта или группы процессов.</span>{/if}
    </div>
    {#if error}<p class="error">{error}</p>{/if}
  </section>

  <ProductStats />

  <section class="flow">
    {#each flow as step}
      <a href={step[3]}><i>{step[0]}</i><div><strong>{step[1]}</strong><span>{step[2]}</span></div><b>→</b></a>
    {/each}
  </section>

  <div class="expert"><span><b>Нужен полный контроль?</b> Открой экспертный workflow и ручной API Builder.</span><a href="#desk/data">Экспертный режим →</a></div>
</section>

<style>
  .home{max-width:1180px;margin:0 auto;padding:38px 32px;color:#172033;box-sizing:border-box}.home>header{max-width:820px;margin-bottom:22px}.home>header>span,small{color:#94a3b8;font-size:9px;font-weight:800;letter-spacing:.12em;text-transform:uppercase}h1{margin:8px 0;font-size:clamp(32px,4vw,50px);line-height:1.02;letter-spacing:-.04em}header p{margin:0;color:#64748b;font-size:13px;line-height:1.6}.containers{margin:0 0 14px;padding:15px;background:#fff;border:1px solid #e4e9f1;border-radius:16px}.head{display:flex;align-items:end;justify-content:space-between;gap:12px}.head>div{display:flex;flex-direction:column;gap:3px}.head strong{font-size:17px}.head form{display:flex;gap:6px}.head input{border:1px solid #dfe5ed;border-radius:8px;padding:8px 10px;font-size:10px}.head button,.chips button{border:1px solid #dfe5ed;border-radius:8px;background:#fff;padding:8px 10px;cursor:pointer}.head button{background:#172033;color:#fff}.chips{display:flex;flex-wrap:wrap;gap:6px;margin-top:12px}.chips button{display:flex;flex-direction:column;align-items:flex-start;min-width:135px}.chips button.active{border-color:#172033;background:#f5f7fa}.chips b{font-size:10px}.chips span,.empty{color:#94a3b8;font-size:8px;text-transform:uppercase}.flow{display:grid;grid-template-columns:repeat(5,1fr);gap:8px;margin-top:14px}.flow a{min-height:150px;padding:14px;background:#fff;border:1px solid #e4e9f1;border-radius:14px;color:inherit;text-decoration:none;display:flex;flex-direction:column}.flow i{width:25px;height:25px;display:grid;place-items:center;border-radius:50%;background:#172033;color:#fff;font-style:normal;font-size:9px}.flow div{margin-top:auto;display:flex;flex-direction:column;gap:5px}.flow strong{font-size:13px}.flow span{color:#64748b;font-size:9px;line-height:1.45}.flow>a>b{align-self:flex-end;margin-top:8px}.expert{margin-top:14px;padding:13px 15px;border:1px dashed #cbd5e1;border-radius:12px;display:flex;justify-content:space-between;color:#64748b;font-size:10px}.expert a{color:#172033;font-weight:800;text-decoration:none}.error{color:#b42318;font-size:9px}@media(max-width:900px){.flow{grid-template-columns:repeat(2,1fr)}}@media(max-width:560px){.home{padding:24px 18px}.head{align-items:stretch;flex-direction:column}.head form{width:100%}.head input{flex:1}.flow{grid-template-columns:1fr}.expert{gap:8px;flex-direction:column}}
</style>
