import { writable } from 'svelte/store';

// Пример данных для артикулов
const initialArticles = [
  {
    id: 12345678,
    name: 'Футболка женская хлопок',
    subject: 'Футболки',
    userFields: ['Хороший ДРР', 'Новинка'],
    drr: 15,
    expense: 2500,
    revenue: 15000,
    orders: 42,
    ctr: 2.3,
    cpc: 59.5,
    cpm: 120,
    campaigns: {
      cpm: {
        enabled: true,
        bidMode: 'manual',
        placements: {
          search: true,
          recommendations: true
        }
      },
      cpc: {
        enabled: false
      }
    },
    words: {
      clusters: 24,
      minus: 6,
      hasChanges: true
    }
  },
  {
    id: 23456789,
    name: 'Футболка мужская премиум',
    subject: 'Футболки',
    userFields: ['Плохой ДРР'],
    drr: 32,
    expense: 4200,
    revenue: 12000,
    orders: 28,
    ctr: 1.8,
    cpc: 150,
    cpm: 280,
    campaigns: {
      cpm: {
        enabled: true,
        bidMode: 'single',
        placements: {
          search: true,
          recommendations: false
        }
      },
      cpc: {
        enabled: false
      }
    },
    words: {
      clusters: 18,
      minus: 3,
      hasChanges: false
    }
  },
  {
    id: 34567890,
    name: 'Джинсы женские узкие',
    subject: 'Джинсы',
    userFields: ['Хороший ДРР', 'Хит продаж'],
    drr: 8,
    expense: 5800,
    revenue: 42000,
    orders: 67,
    ctr: 3.1,
    cpc: 86.5,
    cpm: 180,
    campaigns: {
      cpm: {
        enabled: false
      },
      cpc: {
        enabled: true
      }
    },
    words: {
      clusters: 32,
      minus: 8,
      hasChanges: false
    }
  }
];

// Создаем хранилище с начальными данными
export const articlesStore = writable(initialArticles);

// Функции для обновления состояния артикулов
export const updateArticle = (articleId, updates) => {
  articlesStore.update(articles => 
    articles.map(article => 
      article.id === articleId ? { ...article, ...updates } : article
    )
  );
};

export const updateArticleCampaign = (articleId, campaignType, updates) => {
  articlesStore.update(articles => 
    articles.map(article => {
      if (article.id === articleId) {
        return {
          ...article,
          campaigns: {
            ...article.campaigns,
            [campaignType]: {
              ...article.campaigns[campaignType],
              ...updates
            }
          }
        };
      }
      return article;
    })
  );
};

export const toggleArticleSelection = (articleId) => {
  // Эта функция будет реализована в uiStateStore
  console.log('Toggle selection for article:', articleId);
};