// Utility functions for formatting numbers and data

export const formatNumber = (num) => {
  if (num >= 10000000) {
    return (num / 10000000).toFixed(1) + 'Cr';
  }
  if (num >= 100000) {
    return (num / 100000).toFixed(1) + 'L';
  }
  if (num >= 1000) {
    return (num / 1000).toFixed(1) + 'K';
  }
  return num.toString();
};

export const formatPercentage = (num) => {
  return num.toFixed(1) + '%';
};

export const formatCompactNumber = (num) => {
  if (num >= 1000000) {
    return (num / 1000000).toFixed(1) + 'M';
  }
  if (num >= 1000) {
    return (num / 1000).toFixed(1) + 'K';
  }
  return num.toString();
};

export const getGrowthIndicator = (current, previous) => {
  const growth = ((current - previous) / previous) * 100;
  return {
    value: Math.abs(growth).toFixed(1),
    isPositive: growth >= 0,
    icon: growth >= 0 ? '↑' : '↓'
  };
};

export const getSentimentColor = (sentiment) => {
  switch (sentiment) {
    case 'positive':
      return 'text-sentiment-positive';
    case 'negative':
      return 'text-sentiment-negative';
    default:
      return 'text-sentiment-neutral';
  }
};

export const getSentimentBgColor = (sentiment) => {
  switch (sentiment) {
    case 'positive':
      return 'bg-sentiment-positive/20';
    case 'negative':
      return 'bg-sentiment-negative/20';
    default:
      return 'bg-sentiment-neutral/20';
  }
};

export const getAlertColor = (type) => {
  switch (type) {
    case 'warning':
      return { bg: 'bg-amber-500/20', text: 'text-amber-500', border: 'border-amber-500/30' };
    case 'success':
      return { bg: 'bg-sentiment-positive/20', text: 'text-sentiment-positive', border: 'border-sentiment-positive/30' };
    case 'error':
      return { bg: 'bg-sentiment-negative/20', text: 'text-sentiment-negative', border: 'border-sentiment-negative/30' };
    default:
      return { bg: 'bg-ysrcp-primary/20', text: 'text-ysrcp-primary', border: 'border-ysrcp-primary/30' };
  }
};

export const getPlatformColor = (platform) => {
  const colors = {
    twitter: '#1DA1F2',
    facebook: '#4267B2',
    instagram: '#E4405F',
    youtube: '#FF0000',
    news: '#6B7280'
  };
  return colors[platform] || '#6B7280';
};
