<script lang="ts">
  import ProductStats from './ProductStats.svelte';

  const sections = [
    { href: '#desk/data?pane=clients', title: 'Клиенты', text: 'Цели, KPI, доступы, договоры и контроль состояния.' },
    { href: '#legacy', title: 'Реклама', text: 'Рабочий маршрут от клиента к интеграциям, сценариям и аналитике.' },
    { href: '#desk/data', title: 'Сценарии', text: 'Процессы, расписания, публикация и история выполнения.' },
    { href: '#desk/data?pane=api', title: 'API вручную', text: 'Ручная проверка и доводка API-шаблонов.' },
    { href: '#desk/tables', title: 'Данные', text: 'Таблицы, источники и витрины.' },
    { href: '#desk', title: 'Пространство', text: 'Многомерный анализ товаров, рекламы и результатов.' }
  ];
</script>

<section class="home">
  <header>
    <span>AI Orchestrator</span>
    <h1>Управление рекламой и автоматизацией маркетплейсов</h1>
    <p>Один рабочий контур для клиента, интеграций, процессов и результата.</p>
  </header>

  <ProductStats />

  <div class="primary">
    <a href="#assistant">
      <small>Основной путь</small>
      <strong>Описать задачу ассистенту</strong>
      <span>Получите безопасный API-черновик из задачи или документации и проверьте его перед использованием.</span>
    </a>
    <a href="#desk/data?pane=clients">
      <small>Контекст</small>
      <strong>Открыть клиента</strong>
      <span>Проверьте цели, KPI и доступы к кабинетам до запуска автоматизации.</span>
    </a>
  </div>

  <div class="grid">
    {#each sections as section}
      <a href={section.href}>
        <strong>{section.title}</strong>
        <span>{section.text}</span>
        <b>Открыть →</b>
      </a>
    {/each}
  </div>
</section>

<style>
  .home { max-width: 1180px; margin: 0 auto; padding: 40px 32px; color: #172033; box-sizing: border-box; }
  header { max-width: 850px; margin-bottom: 28px; }
  header > span { color: #64748b; font-size: 11px; font-weight: 800; letter-spacing: .14em; text-transform: uppercase; }
  h1 { margin: 10px 0; font-size: clamp(30px, 4.5vw, 52px); line-height: 1.03; letter-spacing: -.04em; }
  p { margin: 0; color: #64748b; font-size: 15px; }
  .primary { display: grid; grid-template-columns: repeat(2, 1fr); gap: 14px; margin-bottom: 14px; }
  .primary a { min-height: 160px; padding: 22px; border-radius: 18px; background: #172033; color: white; text-decoration: none; display: flex; flex-direction: column; }
  .primary small { color: #9eabc0; font-weight: 700; }
  .primary strong { margin-top: auto; font-size: 22px; }
  .primary span { margin-top: 6px; color: #b7c1d1; font-size: 12px; }
  .grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 14px; }
  .grid a { min-height: 130px; padding: 18px; border: 1px solid #e4e9f1; border-radius: 16px; background: #fff; color: inherit; text-decoration: none; display: flex; flex-direction: column; }
  .grid strong { font-size: 15px; }
  .grid span { margin-top: 6px; color: #64748b; font-size: 12px; line-height: 1.45; }
  .grid b { margin-top: auto; color: #475569; font-size: 11px; }
  @media (max-width: 800px) { .grid { grid-template-columns: repeat(2, 1fr); } }
  @media (max-width: 560px) { .home { padding: 24px 18px; } .primary, .grid { grid-template-columns: 1fr; } }
</style>
