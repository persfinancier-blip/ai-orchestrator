// Экспорт компонентов интерфейса модуля advertising
export { default as AdvertisingDashboard } from './components/layout/AdvertisingDashboard.svelte';
export { default as ControlBar } from './components/layout/ControlBar.svelte';
export { default as PivotGroupPanel } from './components/layout/PivotGroupPanel.svelte';

// Компоненты таблицы
export { default as ArticlesTable } from './components/table/ArticlesTable.svelte';
export { default as ArticleRow } from './components/table/ArticleRow.svelte';
export { default as GroupRowHeader } from './components/table/GroupRowHeader.svelte';

// Компоненты управления РК
export { default as CampaignToggleCell } from './components/campaign/CampaignToggleCell.svelte';
export { default as ToggleTree } from './components/campaign/ToggleTree.svelte';
export { default as RadioToggleGroup } from './components/campaign/RadioToggleGroup.svelte';
export { default as PlacementToggleRow } from './components/campaign/PlacementToggleRow.svelte';

// Компоненты слов/кластеров
export { default as WordsCellSummary } from './components/words/WordsCellSummary.svelte';
export { default as InspectorDrawer } from './components/words/InspectorDrawer.svelte';
export { default as ClusterBidsTable } from './components/words/ClusterBidsTable.svelte';
export { default as MinusPhrasesManager } from './components/words/MinusPhrasesManager.svelte';

// Компоненты массовых действий
export { default as BulkActionBar } from './components/actions/BulkActionBar.svelte';
export { default as GroupContextMenu } from './components/actions/GroupContextMenu.svelte';

// Компоненты состояний
export { default as SyncStatusBadge } from './components/status/SyncStatusBadge.svelte';
export { default as ErrorTooltip } from './components/status/ErrorTooltip.svelte';
export { default as DraftChangesBanner } from './components/status/DraftChangesBanner.svelte';
// Advertising desk
export { default as AdvertisingDesk } from './desk/AdvertisingDesk.svelte';
