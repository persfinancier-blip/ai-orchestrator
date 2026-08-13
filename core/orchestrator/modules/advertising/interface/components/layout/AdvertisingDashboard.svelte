<section class="page">
  <header>
    <small>Реклама</small>
    <h1>Управление рекламой клиента</h1>
    <p>Рекламный контур начинается с клиентских доступов и KPI, выполняется через проверяемые сценарии и завершается аналитикой результата.</p>
  </header>

  <div class="flow">
    <a href="#desk/data?pane=clients"><b>01</b><strong>Клиент</strong><span>Проверить кабинеты, доступы, цели и KPI.</span></a>
    <a href="#desk/data?pane=api"><b>02</b><strong>Интеграции</strong><span>Настроить API и проверить получение данных.</span></a>
    <a href="#desk/data"><b>03</b><strong>Сценарии</strong><span>Собрать и запустить управляемый процесс.</span></a>
    <a href="#desk"><b>04</b><strong>Результат</strong><span>Анализировать товары, рекламу и эффективность.</span></a>
  </div>

  <aside>
    <strong>Почему здесь больше нет старой таблицы</strong>
    <span>Прежний экран был прототипом: его действия выводились в console.log и не управляли реальными кампаниями. Он заменён маршрутом к рабочим механизмам проекта.</span>
  </aside>
</section>

<style>
  .page { max-width: 1050px; margin: 0 auto; padding: 40px 30px; box-sizing: border-box; color: #172033; } header { max-width: 760px; margin-bottom: 24px; } header small { color: #94a3b8; font-size: 9px; font-weight: 800; letter-spacing: .12em; text-transform: uppercase; } h1 { margin: 6px 0; font-size: 32px; letter-spacing: -.04em; } p { margin: 0; color: #64748b; font-size: 12px; line-height: 1.55; }.flow { display: grid; grid-template-columns: repeat(4,1fr); gap: 10px; }.flow a { min-height: 155px; padding: 16px; border: 1px solid #e4e9f1; border-radius: 14px; background: #fff; color: inherit; text-decoration: none; display: flex; flex-direction: column; box-sizing: border-box; }.flow b { color: #94a3b8; font-size: 9px; }.flow strong { margin-top: auto; font-size: 15px; }.flow span { margin-top: 5px; color: #64748b; font-size: 10px; line-height: 1.45; } aside { margin-top: 14px; padding: 14px 16px; border-radius: 12px; background: #eef2f7; display: flex; gap: 10px; align-items: baseline; } aside strong { font-size: 10px; white-space: nowrap; } aside span { color: #64748b; font-size: 10px; line-height: 1.45; }
  @media (max-width: 800px) { .flow { grid-template-columns: repeat(2,1fr); } }
  @media (max-width: 520px) { .page { padding: 24px 16px; }.flow { grid-template-columns: 1fr; } aside { flex-direction: column; } }
</style>
