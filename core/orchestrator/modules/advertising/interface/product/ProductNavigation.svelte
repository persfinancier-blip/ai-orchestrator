<script>
  export let section = 'home';
  const groups = [
    ['Работа', [['home','#home','Обзор'],['clients','#desk/data?pane=clients','Клиенты']]],
    ['Путь решения', [['assistant','#assistant','Собрать сценарий'],['data','#desk/tables','Подготовить данные'],['advertising','#legacy','Аналитика + прогноз']]],
    ['Экспертный режим', [['automation','#desk/data','Workflow'],['integrations','#desk/data?pane=api','API вручную']]]
  ];
</script>

<aside>
  <a class="brand" href="#home"><b>AO</b><span><strong>AI Orchestrator</strong><small>Container → Data → Decision</small></span></a>
  <nav>{#each groups as group}<section><small>{group[0]}</small>{#each group[1] as item}<a href={item[1]} class:active={section===item[0]}>{item[2]}</a>{/each}</section>{/each}</nav>
  <div class="status"><i></i> Self-hosted runtime</div>
</aside>

<style>
  aside{position:sticky;top:0;height:100vh;padding:18px 13px;box-sizing:border-box;background:#fff;border-right:1px solid #e4e9f1;display:flex;flex-direction:column}.brand{display:flex;align-items:center;gap:9px;padding:4px 7px 20px;color:#172033;text-decoration:none}.brand>b{width:33px;height:33px;display:grid;place-items:center;border-radius:10px;background:#172033;color:#fff;font-size:11px}.brand span{display:flex;flex-direction:column}.brand strong{font-size:12px}.brand small{color:#94a3b8;font-size:9px}nav{display:flex;flex-direction:column;gap:16px}nav section{display:flex;flex-direction:column;gap:3px}nav section>small{padding:0 8px 4px;color:#a0aabc;font-size:8px;font-weight:800;text-transform:uppercase;letter-spacing:.12em}nav a{padding:9px;border-radius:9px;color:#64748b;text-decoration:none;font-size:12px;font-weight:650}nav a:hover{background:#f4f6f9;color:#172033}nav a.active{background:#172033;color:#fff}.status{margin-top:auto;padding:9px;color:#94a3b8;font-size:9px;display:flex;gap:7px;align-items:center}.status i{width:7px;height:7px;border-radius:50%;background:#22c55e}@media(max-width:760px){aside{height:auto;z-index:30;padding:8px;border-right:0;border-bottom:1px solid #e4e9f1}.brand,nav section>small,.status{display:none}nav{flex-direction:row;gap:4px;overflow-x:auto}nav section{flex-direction:row;gap:4px}nav a{white-space:nowrap;padding:7px 9px}}
</style>
