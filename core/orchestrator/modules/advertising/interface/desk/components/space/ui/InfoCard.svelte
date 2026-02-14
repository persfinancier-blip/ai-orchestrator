<script lang="ts">
  export let pointsCount = 0;
  export let axesLabel = '';
  export let bboxLabel = '—';
</script>

<div class="hud bottom-left">
  <div class="info-card">
    <div class="info-title">Точек: {pointsCount}</div>
    <div class="info-sub">{axesLabel}</div>
    <div class="info-sub">{bboxLabel}</div>
  </div>
</div>
