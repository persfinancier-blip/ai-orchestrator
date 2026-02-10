import { writable } from 'svelte/store';

// Начальное состояние UI
const initialUiState = {
  selectedArticles: [],
  showInspector: false,
  draftChanges: [],
  groupSettings: {
    rows: ['subject', 'userField', 'article'],
    columns: [],
    values: ['drr', 'expense', 'revenue', 'orders', 'ctr', 'cpc']
  },
  filters: {
    subject: '',
    campaignStatus: '',
    campaignType: '',
    strategy: '',
    drrRange: [0, 100]
  },
  searchQuery: '',
  selectedPeriod: '7d'
};

// Создаем хранилище состояния UI
export const uiStateStore = writable(initialUiState);

// Функции для обновления состояния UI

// Управление выбором артикулов
export const selectArticle = (articleId) => {
  uiStateStore.update(state => {
    const selectedArticles = [...state.selectedArticles];
    if (!selectedArticles.includes(articleId)) {
      selectedArticles.push(articleId);
    }
    return { ...state, selectedArticles };
  });
};

export const deselectArticle = (articleId) => {
  uiStateStore.update(state => {
    const selectedArticles = state.selectedArticles.filter(id => id !== articleId);
    return { ...state, selectedArticles };
  });
};

export const toggleArticleSelection = (articleId) => {
  uiStateStore.update(state => {
    const selectedArticles = [...state.selectedArticles];
    const index = selectedArticles.indexOf(articleId);
    
    if (index === -1) {
      selectedArticles.push(articleId);
    } else {
      selectedArticles.splice(index, 1);
    }
    
    return { ...state, selectedArticles };
  });
};

export const selectAllArticles = (articleIds) => {
  uiStateStore.update(state => {
    return { ...state, selectedArticles: [...articleIds] };
  });
};

export const clearSelection = () => {
  uiStateStore.update(state => {
    return { ...state, selectedArticles: [] };
  });
};

// Управление инспектором
export const openInspector = () => {
  uiStateStore.update(state => {
    return { ...state, showInspector: true };
  });
};

export const closeInspector = () => {
  uiStateStore.update(state => {
    return { ...state, showInspector: false };
  });
};

// Управление черновиками изменений
export const addDraftChange = (change) => {
  uiStateStore.update(state => {
    const draftChanges = [...state.draftChanges, change];
    return { ...state, draftChanges };
  });
};

export const removeDraftChange = (changeId) => {
  uiStateStore.update(state => {
    const draftChanges = state.draftChanges.filter(change => change.id !== changeId);
    return { ...state, draftChanges };
  });
};

export const clearDraftChanges = () => {
  uiStateStore.update(state => {
    return { ...state, draftChanges: [] };
  });
};

// Управление настройками группировки
export const updateGroupSettings = (settings) => {
  uiStateStore.update(state => {
    return { ...state, groupSettings: { ...state.groupSettings, ...settings } };
  });
};

// Управление фильтрами
export const updateFilters = (filters) => {
  uiStateStore.update(state => {
    return { ...state, filters: { ...state.filters, ...filters } };
  });
};

// Управление поисковым запросом
export const updateSearchQuery = (query) => {
  uiStateStore.update(state => {
    return { ...state, searchQuery: query };
  });
};

// Управление выбранным периодом
export const updateSelectedPeriod = (period) => {
  uiStateStore.update(state => {
    return { ...state, selectedPeriod: period };
  });
};