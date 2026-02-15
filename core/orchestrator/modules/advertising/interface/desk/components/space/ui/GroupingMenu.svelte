<!-- core/orchestrator/modules/advertising/interface/desk/components/space/ui/GroupingMenu.svelte -->
<script lang="ts">
  import type { ShowcaseField } from '../../../data/showcaseStore';
  import type { GroupingConfig, GroupingPrinciple } from '../cluster/types';

  export let cfg: GroupingConfig;
  export let numberFields: ShowcaseField[] = [];
  export let textFields: ShowcaseField[] = [];
  export let onRecompute: () => void;

  function setPrinciple(p: GroupingPrinciple): void {
    cfg = { ...cfg, principle: p };
  }

  function toggleEnabled(): void {
    cfg = { ...cfg, enabled: !cfg.enabled };
  }

  function toggleCustomWeights(): void {
    cfg = { ...cfg, customWeights: !cfg.customWeights };
  }

  function setRecompute(mode: 'auto' | 'fixed' | 'manual'): void {
    cfg = { ...cfg, recompute: mode };
  }

  function setFeatureField(code: string): void {
    if (!code) return;
    if (cfg.featureFields.includes(code)) return;
    cfg = { ...cfg, featureFields: [...cfg.featureFields, code] };
  }

  function removeFeatureField(code: string): void {
    cfg = { ...cfg, featureFields: cfg.featureFields.filter((x) => x !== code) };
  }
</script>

<div class="menu">
  <div class="title">Группировка</div>

  <label class="row">
    <input type="checkbox" checked={cfg.enabled} on:change={toggleEnabled} />
    <span>Включить группировку</span>
  </label>

  <div class="section">
    <div class="label">Принцип объединения</div>

    <div class="chips">
      <button type="button" class:active={cfg.principle === 'efficiency'} on:click={() => setPrinciple('efficiency')}>
        По эффективности
      </button>
      <button type="button" class:active={cfg.principle === 'behavior'} on:click={() => setPrinciple('behavior')}>
        По поведению
      </button>
      <button type="button" class:active={cfg.principle === 'proximity'} on:click={() => setPrinciple('proximity')}>
        По близости
      </button>
    </div>
  </div>

  <div class="section">
    <div class="label">Детализация групп (0 = без группировки)</div>
    <input
      type="range"
      min="0"
      max="1"
      step="0.01"
      bind:value={cfg.detail}
    />
  </div>

  <div class="section">
    <div class="label">Размер кластера (min)</div>
    <input
      type="number"
      min="2"
      step="1"
      bind:value={cfg.minClusterSize}
    />
  </div>

  <div class="section">
    <div class="label">Режим пересчёта</div>
    <div class="chips">
      <button type="button" class:active={cfg.recompute === 'auto'} on:click={() => setRecompute('auto')}>Авто</button>
      <button type="button" class:active={cfg.recompute === 'fixed'} on:click={() => setRecompute('fixed')}>Фикс</button>
      <button type="button" class:active={cfg.recompute === 'manual'} on:click={() => setRecompute('manual')}>Вручную</button>
    </div>

    <button class="recompute" type="button" on:click={onRecompute}>Пересчитать</button>
  </div>
</div>

<style>
  .menu { position: absolute; right: 12px; top: 12px; width: 320px; background: #fff; border: 1px solid #e2e8f0; border-radius: 12px; padding: 12px; }
  .title { font-weight: 700; margin-bottom: 8px; }
  .row { display: flex; align-items: center; gap: 8px; margin-bottom: 10px; }
  .section { margin-top: 12px; }
  .label { font-size: 12px; color: #475569; margin-bottom: 6px; }
  .chips { display: flex; flex-wrap: wrap; gap: 8px; }
  .chips button { padding: 6px 10px; border-radius: 999px; border: 1px solid #e2e8f0; background: #fff; cursor: pointer; }
  .chips button.active { border-color: #64748b; }
  .recompute { margin-top: 10px; width: 100%; padding: 8px 10px; border-radius: 10px; border: 1px solid #e2e8f0; background: #fff; cursor: pointer; }
</style>
