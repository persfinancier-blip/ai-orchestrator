<script lang="ts">
  import { onMount } from 'svelte';
  import type { ShowcaseField } from '../../data/showcaseStore';
  import type { GroupingConfig, GroupingPrinciple, RecomputeMode } from '../cluster/types';

  export let cfg: GroupingConfig;
  export let numberFields: ShowcaseField[] = [];
  export let textFields: ShowcaseField[] = [];
  export let onRecompute: () => void;

  let menuEl: HTMLDivElement | null = null;
  let positioned = false;
  let menuX = 0;
  let menuY = 56;
  let dragging = false;
  let dragOffsetX = 0;
  let dragOffsetY = 0;

  const t = {
    title: '\u0424\u043e\u0440\u043c\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u0435 \u0433\u0440\u0443\u043f\u043f',
    enable: '\u0412\u043a\u043b\u044e\u0447\u0438\u0442\u044c \u0433\u0440\u0443\u043f\u043f\u0438\u0440\u043e\u0432\u043a\u0443',
    principle: '\u041f\u0440\u0438\u043d\u0446\u0438\u043f \u043e\u0431\u044a\u0435\u0434\u0438\u043d\u0435\u043d\u0438\u044f',
    detail: '\u0414\u0435\u0442\u0430\u043b\u0438\u0437\u0430\u0446\u0438\u044f \u0433\u0440\u0443\u043f\u043f',
    features: '\u041f\u0440\u0438\u0437\u043d\u0430\u043a\u0438 \u0434\u043b\u044f \u043f\u043e\u0445\u043e\u0436\u0435\u0441\u0442\u0438 (\u0434\u043e 3)',
    selected: '\u0432\u044b\u0431\u0440\u0430\u043d\u043e',
    axisPriority: '\u041f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442 \u0438\u0437\u043c\u0435\u0440\u0435\u043d\u0438\u0439',
    customMode: '\u0421\u0432\u043e\u0439 \u0440\u0435\u0436\u0438\u043c',
    minGroup: '\u041c\u0438\u043d\u0438\u043c\u0430\u043b\u044c\u043d\u044b\u0439 \u0440\u0430\u0437\u043c\u0435\u0440 \u0433\u0440\u0443\u043f\u043f\u044b',
    recomputeMode: '\u0420\u0435\u0436\u0438\u043c \u043f\u0435\u0440\u0435\u0441\u0447\u0435\u0442\u0430',
    recomputeBtn: '\u041f\u0435\u0440\u0435\u0441\u0447\u0438\u0442\u0430\u0442\u044c \u0433\u0440\u0443\u043f\u043f\u044b'
  };

  const principles: Array<{ id: GroupingPrinciple; name: string }> = [
    { id: 'efficiency', name: '\u041f\u043e \u044d\u0444\u0444\u0435\u043a\u0442\u0438\u0432\u043d\u043e\u0441\u0442\u0438' },
    { id: 'behavior', name: '\u041f\u043e \u043f\u043e\u0432\u0435\u0434\u0435\u043d\u0438\u044e' },
    { id: 'proximity', name: '\u041f\u043e \u0431\u043b\u0438\u0437\u043e\u0441\u0442\u0438' }
  ];

  const recomputeModes: Array<{ id: RecomputeMode; name: string }> = [
    { id: 'auto', name: '\u0410\u0432\u0442\u043e' },
    { id: 'fixed', name: '\u0424\u0438\u043a\u0441\u0438\u0440\u043e\u0432\u0430\u0442\u044c \u0433\u0440\u0443\u043f\u043f\u044b' },
    { id: 'manual', name: '\u0420\u0443\u0447\u043d\u043e\u0439' }
  ];

  const CODE_ALIASES: Record<string, string> = {
    sku: '\u0410\u0440\u0442\u0438\u043a\u0443\u043b',
    campaign_id: '\u041a\u0430\u043c\u043f\u0430\u043d\u0438\u044f',
    keyword: '\u041a\u043b\u044e\u0447\u0435\u0432\u043e\u0435 \u0441\u043b\u043e\u0432\u043e',
    query: '\u041f\u043e\u0438\u0441\u043a\u043e\u0432\u044b\u0439 \u0437\u0430\u043f\u0440\u043e\u0441',
    phrase: '\u041f\u043e\u0438\u0441\u043a\u043e\u0432\u0430\u044f \u0444\u0440\u0430\u0437\u0430',
    product_id: '\u0422\u043e\u0432\u0430\u0440',
    brand: '\u0411\u0440\u0435\u043d\u0434',
    category: '\u041a\u0430\u0442\u0435\u0433\u043e\u0440\u0438\u044f',
    revenue: '\u0412\u044b\u0440\u0443\u0447\u043a\u0430',
    spend: '\u0420\u0430\u0441\u0445\u043e\u0434',
    cost: '\u0421\u0442\u043e\u0438\u043c\u043e\u0441\u0442\u044c',
    budget: '\u0411\u044e\u0434\u0436\u0435\u0442',
    profit: '\u041f\u0440\u0438\u0431\u044b\u043b\u044c',
    margin: '\u041c\u0430\u0440\u0436\u0430',
    orders: '\u0417\u0430\u043a\u0430\u0437\u044b',
    clicks: '\u041a\u043b\u0438\u043a\u0438',
    views: '\u041f\u043e\u043a\u0430\u0437\u044b',
    impressions: '\u041f\u043e\u043a\u0430\u0437\u044b',
    ctr: 'CTR',
    cpc: 'CPC',
    cpm: 'CPM',
    roi: 'ROI',
    drr: '\u0414\u0420\u0420',
    cr: '\u041a\u043e\u043d\u0432\u0435\u0440\u0441\u0438\u044f'
  };

  function titleFromCode(code: string): string {
    const c = String(code ?? '').trim();
    if (!c) return '\u2014';
    if (CODE_ALIASES[c]) return CODE_ALIASES[c];
    const norm = c.replace(/[_-]+/g, ' ').trim();
    return norm.charAt(0).toUpperCase() + norm.slice(1);
  }

  function fieldTitle(f: ShowcaseField): string {
    const name = String((f as any)?.name ?? '').trim();
    if (name && name !== f.code) return name;
    return titleFromCode(f.code);
  }

  function groupForNumber(code: string): string {
    const c = code.toLowerCase();
    if (/(revenue|spend|cost|budget|profit|margin|price|gmv|turnover)/.test(c)) return '\u0414\u0435\u043d\u044c\u0433\u0438';
    if (/(roi|drr|ctr|cr|conversion|rate|share|percent|pct)/.test(c)) return '\u042d\u0444\u0444\u0435\u043a\u0442\u0438\u0432\u043d\u043e\u0441\u0442\u044c';
    if (/(orders|order|qty|quantity|units)/.test(c)) return '\u0417\u0430\u043a\u0430\u0437\u044b';
    if (/(click|view|impression|reach|traffic)/.test(c)) return '\u0422\u0440\u0430\u0444\u0438\u043a';
    return '\u0414\u0440\u0443\u0433\u043e\u0435';
  }

  function groupForText(code: string): string {
    const c = code.toLowerCase();
    if (/(query|keyword|phrase|search)/.test(c)) return '\u041f\u043e\u0438\u0441\u043a';
    if (/(campaign|adgroup|ad_group)/.test(c)) return '\u0420\u0435\u043a\u043b\u0430\u043c\u0430';
    if (/(sku|product|item|offer)/.test(c)) return '\u0422\u043e\u0432\u0430\u0440';
    if (/(brand|category|catalog)/.test(c)) return '\u041a\u0430\u0442\u0430\u043b\u043e\u0433';
    return '\u0414\u0440\u0443\u0433\u043e\u0435';
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

  function initPositionFromCurrentLayout(): void {
    if (positioned || !menuEl) return;
    const parent = menuEl.offsetParent as HTMLElement | null;
    const menuRect = menuEl.getBoundingClientRect();
    const parentRect = parent?.getBoundingClientRect();
    menuX = menuRect.left - (parentRect?.left ?? 0);
    menuY = menuRect.top - (parentRect?.top ?? 0);
    positioned = true;
  }

  function onDragStart(e: MouseEvent): void {
    if (!menuEl) return;
    initPositionFromCurrentLayout();
    dragging = true;
    dragOffsetX = e.clientX - menuX;
    dragOffsetY = e.clientY - menuY;
    window.addEventListener('mousemove', onDragMove);
    window.addEventListener('mouseup', onDragEnd);
  }

  function onDragMove(e: MouseEvent): void {
    if (!dragging || !menuEl) return;

    const parent = menuEl.offsetParent as HTMLElement | null;
    if (!parent) return;

    const maxX = Math.max(0, parent.clientWidth - menuEl.offsetWidth);
    const maxY = Math.max(0, parent.clientHeight - menuEl.offsetHeight);

    menuX = Math.max(0, Math.min(maxX, e.clientX - dragOffsetX));
    menuY = Math.max(0, Math.min(maxY, e.clientY - dragOffsetY));
  }

  function onDragEnd(): void {
    dragging = false;
    window.removeEventListener('mousemove', onDragMove);
    window.removeEventListener('mouseup', onDragEnd);
  }

  onMount(() => {
    requestAnimationFrame(initPositionFromCurrentLayout);
    return () => onDragEnd();
  });

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
        ? ['\u0414\u0435\u043d\u044c\u0433\u0438', '\u042d\u0444\u0444\u0435\u043a\u0442\u0438\u0432\u043d\u043e\u0441\u0442\u044c', '\u0417\u0430\u043a\u0430\u0437\u044b', '\u0422\u0440\u0430\u0444\u0438\u043a', '\u0414\u0440\u0443\u0433\u043e\u0435']
        : ['\u041f\u043e\u0438\u0441\u043a', '\u0420\u0435\u043a\u043b\u0430\u043c\u0430', '\u0422\u043e\u0432\u0430\u0440', '\u041a\u0430\u0442\u0430\u043b\u043e\u0433', '\u0414\u0440\u0443\u0433\u043e\u0435'];

    const res: Array<{ title: string; items: ShowcaseField[] }> = [];
    for (const key of order) {
      const items = m.get(key);
      if (items?.length) res.push({ title: key, items: items.sort(sortByTitle) });
    }
    for (const [k, items] of m.entries()) {
      if (order.includes(k)) continue;
      if (items.length) res.push({ title: k, items: items.sort(sortByTitle) });
    }
    return res;
  })();
</script>

<div
  bind:this={menuEl}
  class="menu-pop pick"
  style={positioned ? `top:${menuY}px; left:${menuX}px; width:360px;` : 'top:56px; right:368px; width:360px;'}
>
  <div class="menu-title" style="cursor:grab; user-select:none;" on:mousedown={onDragStart}>{t.title}</div>

  <div class="row">
    <label style="display:flex; gap:10px; align-items:center; pointer-events:auto;">
      <input type="checkbox" bind:checked={cfg.enabled} />
      <span style="font-size:12px; font-weight:650;">{t.enable}</span>
    </label>
  </div>

  <div class="sep" />

  <div class="sub">{t.principle}</div>
  <div class="row" style="flex-wrap:wrap;">
    {#each principles as p}
      <button class="btn" style={`padding:8px 12px; opacity:${cfg.principle === p.id ? 1 : 0.65};`} on:click={() => setPrinciple(p.id)}>
        {p.name}
      </button>
    {/each}
  </div>

  <div class="sub">{t.detail}</div>
  <div class="row">
    <input class="input" type="range" min="0" max="1" step="0.01" bind:value={cfg.detail} />
  </div>

  {#if canPickFields}
    <div class="sub">
      {t.features}
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
            <span class="tag">{cfg.featureFields.includes(f.code) ? t.selected : ' '}</span>
          </button>
        {/each}
      {/each}
    </div>
  {/if}

  {#if cfg.principle === 'proximity'}
    <div class="sub">{t.axisPriority}</div>
    <div class="row">
      <label style="display:flex; gap:10px; align-items:center; pointer-events:auto;">
        <input type="checkbox" bind:checked={cfg.customWeights} />
        <span style="font-size:12px;">{t.customMode}</span>
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

  <div class="sub">{t.minGroup}</div>
  <div class="row">
    <input class="input" type="number" min="1" step="1" bind:value={cfg.minClusterSize} />
  </div>

  <div class="sub">{t.recomputeMode}</div>
  <div class="row">
    <select class="select" bind:value={cfg.recompute}>
      {#each recomputeModes as m}
        <option value={m.id}>{m.name}</option>
      {/each}
    </select>
  </div>

  {#if cfg.recompute !== 'auto'}
    <div class="row">
      <button class="btn btn-primary wide" on:click={onRecompute}>{t.recomputeBtn}</button>
    </div>
  {/if}
</div>
