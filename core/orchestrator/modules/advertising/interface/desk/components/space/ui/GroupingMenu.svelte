<script lang="ts">
  import type { ShowcaseField } from '../../data/showcaseStore';
  import type { GroupingConfig, GroupingPrinciple, RecomputeMode } from '../cluster/types';

  export let cfg: GroupingConfig;

  export let numberFields: ShowcaseField[] = [];
  export let textFields: ShowcaseField[] = [];

  export let onRecompute: () => void;

  const principles: Array<{ id: GroupingPrinciple; name: string }> = [
    { id: 'efficiency', name: 'По эффективности' },
    { id: 'behavior', name: 'По поведению' },
    { id: 'proximity', name: 'По близости' }
  ];

  const recomputeModes: Array<{ id: RecomputeMode; name: string }> = [
    { id: 'auto', name: 'Авто' },
    { id: 'fixed', name: 'Фиксировать группы' },
    { id: 'manual', name: 'Ручной' }
  ];

  // ---- smart labels
  const CODE_ALIASES: Record<string, string> = {
    sku: 'Артикул',
    campaign_id: 'Кампания',
    keyword: 'Ключевое слово',
    query: 'Поисковый запрос',
    phrase: 'Поисковая фраза',
    product_id: 'Товар',
    brand: 'Бренд',
    category: 'Категория',

    revenue: 'Выручка',
    spend: 'Расход',
    cost: 'Стоимость',
    budget: 'Бюджет',
    profit: 'Прибыль',
    margin: 'Маржа',
    orders: 'Заказы',
    clicks: 'Клики',
    views: 'Показы',
    impressions: 'Показы',
    ctr: 'CTR',
    cpc: 'CPC',
    cpm: 'CPM',
    roi: 'ROI',
    drr: 'ДРР',
    cr: 'Конверсия'
  };

  function titleFromCode(code: string): string {
    const c = String(code ?? '').trim();
    if (!c) return '—';

    if (CODE_ALIASES[c]) return CODE_ALIASES[c];

    const norm = c.replace(/[_-]+/g, ' ').trim();
    const pretty = norm.charAt(0).toUpperCase() + norm.slice(1);
    return pretty;
  }

  function fieldTitle(f: ShowcaseField): string {
    const name = String((f as any)?.name ?? '').trim();
    if (name && name !== f.code) return name;
    return titleFromCode(f.code);
  }

  function groupForNumber(code: string): string {
    const c = code.toLowerCase();
    if (/(revenue|spend|cost|budget|profit|margin|price|gmv|turnover)/.test(c)) return 'Деньги';
    if (/(roi|drr|ctr|cr|conversion|rate|share|percent|pct)/.test(c)) return 'Эффективность';
    if (/(orders|order|qty|quantity|units)/.test(c)) return 'Заказы';
    if (/(click|view|impression|reach|traffic)/.test(c)) return 'Трафик';
    return 'Другое';
  }

  function groupForText(code: string): string {
    const c = code.toLowerCase();
    if (/(query|keyword|phrase|search)/.test(c)) return 'Поиск';
    if (/(campaign|adgroup|ad_group)/.test(c)) return 'Реклама';
    if (/(sku|product|item|offer)/.test(c)) return 'Товар';
    if (/(brand|category|catalog)/.test(c)) return 'Каталог';
    return 'Другое';
  }

  function sortByTitle(a: ShowcaseField, b: ShowcaseField): number {
    return fieldTitle(a).localeCompare(fieldTitle(b), 'ru');
  }

  function toggleField(code: string): void {
    const cur = new Set(cfg.featureFields);
    if (cur.has(code)) cur.delete(code);
    else {
      if (cur.size >= 3) return;
      cur.add(code);
    }
    cfg = { ...cfg, featureFields: [...cur] };
  }

  function setPrinciple(p: GroupingPrinciple): void {
    cfg = { ...cfg, principle: p, featureFields: [] };
  }

  $: canPickFields = cfg.principle !== 'proximity';

  $: availableFields =
    cfg.principle === 'efficiency'
      ? [...numberFields].sort(sortByTitle)
      : cfg.principle === 'behavior'
        ? [...textFields].sort(sortByTitle)
        : [];

  $: grouped = (() => {
    const m = new Map<string, ShowcaseField[]>();
    for (const f of availableFields) {
      const group = cfg.principle === 'efficiency' ? groupForNumber(f.code) : groupForText(f.code);
      if (!m.has(group)) m.set(group, []);
      m.get(group)!.push(f);
    }
    const order =
      cfg.principle === 'efficiency'
        ? ['Деньги', 'Эффективность', 'Заказы', 'Трафик', 'Другое']
        : ['Поиск', 'Реклама', 'Товар', 'Каталог', 'Другое'];

    const res: Array<{ title: string; items: ShowcaseField[] }> = [];
    for (const key of order) {
      const items = m.get(key);
      if (items?.length) res.push({ title: key, items: items.sort(sortByTitle) });
    }
    // на всякий — если появились неизвестные группы
    for (const [k, items] of m.entries()) {
      if (order.includes(k)) continue;
      if (items.length) res.push({ title: k, items: items.sort(sortByTitle) });
    }
    return res;
  })();
</script>

<div class="menu-pop pick" style="top: 56px; right: 368px; width: 360px;">
  <div class="menu-title">Формирование групп</div>

  <div class="row">
    <label style="display:flex; gap:10px; align-items:center; pointer-events:auto;">
      <input type="checkbox" bind:checked={cfg.enabled} />
      <span style="font-size:12px; font-weight:650;">Включить группировку</span>
    </label>
  </div>

  <div class="sep" />

  <div class="sub">Принцип объединения</div>
  <div class="row" style="flex-wrap:wrap;">
    {#each principles as p}
      <button
        class="btn"
        style={`padding:8px 12px; opacity:${cfg.principle === p.id ? 1 : 0.65};`}
        on:click={() => setPrinciple(p.id)}
      >
        {p.name}
      </button>
    {/each}
  </div>

  <div class="sub">Детализация групп</div>
  <div class="row">
    <input class="input" type="range" min="0" max="1" step="0.01" bind:value={cfg.detail} />
  </div>

  {#if canPickFields}
    <div class="sub">
      Признаки для похожести (до 3)
      <span class="hint">({cfg.featureFields.length}/3)</span>
    </div>

    <div class="list" style="max-height: 260px;">
      {#each grouped as g (g.title)}
        <div style="margin-top:10px; font-size:11px; font-weight:800; color: rgba(15,23,42,.75);">
          {g.title}
        </div>

        {#each g.items as f (f.code)}
          <button class="item" on:click={() => toggleField(f.code)} style="opacity:1;">
            <span class="name">{fieldTitle(f)}</span>
            <span class="tag">{cfg.featureFields.includes(f.code) ? 'выбрано' : ' '}</span>
          </button>
        {/each}
      {/each}
    </div>
  {/if}

  {#if cfg.principle === 'proximity'}
    <div class="sub">Приоритет измерений</div>
    <div class="row">
      <label style="display:flex; gap:10px; align-items:center; pointer-events:auto;">
        <input type="checkbox" bind:checked={cfg.customWeights} />
        <span style="font-size:12px;">Свой режим</span>
      </label>
    </div>

    {#if cfg.customWeights}
      <div class="row two">
        <input class="input" type="number" min="0.2" max="5" step="0.1" bind:value={cfg.wX} />
        <input class="input" type="number" min="0.2" max="5" step="0.1" bind:value={cfg.wY} />
      </div>
      <div class="row">
        <input class="input" type="number" min="0.2" max="5" step="0.1" bind:value={cfg.wZ} />
      </div>
    {/if}
  {/if}

  <div class="sub">Минимальный размер группы</div>
  <div class="row">
    <input class="input" type="number" min="1" step="1" bind:value={cfg.minClusterSize} />
  </div>

  <div class="sub">Режим пересчёта</div>
  <div class="row">
    <select class="select" bind:value={cfg.recompute}>
      {#each recomputeModes as m}
        <option value={m.id}>{m.name}</option>
      {/each}
    </select>
  </div>

  {#if cfg.recompute !== 'auto'}
    <div class="row">
      <button class="btn btn-primary wide" on:click={onRecompute}>Пересчитать группы</button>
    </div>
  {/if}
</div>
