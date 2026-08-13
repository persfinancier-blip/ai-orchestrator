<script>
  import CurrentClientCard from '../../product/CurrentClientCard.svelte';
</script>

<section class="page">
  <header>
    <small>Реклама</small>
    <h1>Управление рекламой клиента</h1>
    <p>Рекламный контур начинается с клиентских доступов и KPI, выполняется через проверяемые сценарии и завершается контролем фактических данных.</p>
  </header>

  <CurrentClientCard />

  <div class="flow">
    <a href="#desk/data?pane=clients"><b>01</b><strong>Клиент</strong><span>Проверить кабинеты, доступы, цели и KPI.</span></a>
    <a href="#assistant"><b>02</b><strong>Ассистент</strong><span>Подготовить безопасный API-черновик из задачи или документации.</span></a>
    <a href="#desk/data"><b>03</b><strong>Сценарии</strong><span>Проверить, опубликовать и запустить управляемый процесс.</span></a>
    <a href="#desk/tables"><b>04</b><strong>Данные</strong><span>Проверить таблицы, Gold-витрины и фактический результат.</span></a>
  </div>

  <aside>
    <strong>Рабочий контур</strong>
    <span>Старые демонстрационные кнопки удалены: здесь остались только переходы к механизмам, которые читают или изменяют реальное состояние системы.</span>
  </aside>
</section>

<style>
  .page { max-width: 1050px; margin: 0 auto; padding: 40px 30px; box-sizing: border-box; color: #172033; } header { max-width: 760px; margin-bottom: 20px; } header small { color: #94a3b8; font-size: 9px; font-weight: 800; letter-spacing: .12em; text-transform: uppercase; } h1 { margin: 6px 0; font-size: 32px; letter-spacing: -.04em; } p { margin: 0; color: #64748b; font-size: 12px; line-height: 1.55; }.flow { display: grid; grid-template-columns: repeat(4,1fr); gap: 10px; }.flow a { min-height: 155px; padding: 16px; border: 1px solid #e4e9f1; border-radius: 14px; background: #fff; color: inherit; text-decoration: none; display: flex; flex-direction: column; box-sizing: border-box; }.flow b { color: #94a3b8; font-size: 9px; }.flow strong { margin-top: auto; font-size: 15px; }.flow span { margin-top: 5px; color: #64748b; font-size: 10px; line-height: 1.45; } aside { margin-top: 14px; padding: 14px 16px; border-radius: 12px; background: #eef2f7; display: flex; gap: 10px; align-items: baseline; } aside strong { font-size: 10px; white-space: nowrap; } aside span { color: #64748b; font-size: 10px; line-height: 1.45; }
  @media (max-width: 800px) { .flow { grid-template-columns: repeat(2,1fr); } }
  @media (max-width: 520px) { .page { padding: 24px 16px; }.flow { grid-template-columns: 1fr; } aside { flex-direction: column; } }
</style>
