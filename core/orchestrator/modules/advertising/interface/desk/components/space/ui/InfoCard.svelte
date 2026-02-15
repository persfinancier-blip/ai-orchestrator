<!-- core/orchestrator/modules/advertising/interface/desk/components/space/ui/InfoCard.svelte -->
<script lang="ts">
  export let pointsCount: number;
  export let axesLabel: string;
  export let bboxLabel: string;
</script>

<div class="hud bottom-left">
  <div class="info-card">
    <div class="info-title">Точек: {pointsCount}</div>
    <div class="info-sub">{axesLabel}</div>
    <div class="info-sub">{bboxLabel}</div>
  </div>
</div>
