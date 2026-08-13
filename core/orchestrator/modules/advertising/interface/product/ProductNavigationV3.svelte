<script>
  export let section = 'home';
  const items = [
    { id: 'home', href: '#home', icon: '⌂', label: 'Обзор' },
    { id: 'containers', href: '#containers', icon: '▣', label: 'Контейнеры' },
    { id: 'scenario', href: '#scenario', icon: '▶', label: 'Сценарий' },
    { id: 'data-products', href: '#data-products', icon: '▤', label: 'Данные' },
    { id: 'insights', href: '#insights', icon: '◈', label: 'Инсайты 3D' },
    { id: 'forecasts', href: '#forecasts', icon: '⌁', label: 'Прогнозы' },
    { id: 'clients', href: '#desk/data?pane=clients', icon: '●', label: 'Клиенты' },
    { id: 'automation', href: '#desk/data', icon: '⌘', label: 'Экспертный workflow' },
    { id: 'integrations', href: '#desk/data?pane=api', icon: '↔', label: 'API вручную' }
  ];
</script>

<nav aria-label="Навигация продукта">
  <a class="brand" href="#home"><b>AO</b><span>AI Orchestrator</span></a>
  <div class="items">
    {#each items as item}
      <a href={item.href} class:active={section === item.id} title={item.label}><i>{item.icon}</i><span>{item.label}</span></a>
    {/each}
  </div>
  <footer><span>Контейнер → данные → решение</span></footer>
</nav>

<style>
  nav { position: sticky; top: 0; height: 100vh; display:flex; flex-direction:column; border-right:1px solid #e4e9f1; background:#fff; padding:14px 10px; box-sizing:border-box; }
  .brand { display:flex; align-items:center; gap:9px; padding:4px 6px 16px; color:#172033; text-decoration:none; }
  .brand b { width:28px; height:28px; display:grid; place-items:center; border-radius:8px; background:#172033; color:#fff; font-size:10px; }
  .brand span { font-size:11px; font-weight:800; }
  .items { display:flex; flex-direction:column; gap:3px; }
  .items a { display:flex; align-items:center; gap:9px; min-height:36px; padding:0 9px; border-radius:9px; color:#667085; text-decoration:none; font-size:10px; font-weight:650; }
  .items a:hover { background:#f6f8fb; color:#172033; }
  .items a.active { background:#eef2f7; color:#172033; }
  i { width:17px; text-align:center; font-style:normal; color:#98a2b3; }
  footer { margin-top:auto; padding:10px 7px; color:#a2aabc; font-size:8px; }
  @media (max-width:760px) { nav { position:relative; height:auto; flex-direction:row; overflow:auto; border-right:0; border-bottom:1px solid #e4e9f1; padding:7px; } .brand, footer { display:none; } .items { flex-direction:row; } .items a { white-space:nowrap; } }
</style>
