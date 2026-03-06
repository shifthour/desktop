import { Hash, TrendingUp, Flame, Loader2 } from 'lucide-react';
import { useDashboard } from '../context/DashboardContext';
import { formatNumber } from '../utils/formatters';

const TrendingHashtags = () => {
  const { trendingHashtags, loading } = useDashboard();

  // Show loading state if no data
  if (loading || !trendingHashtags || trendingHashtags.length === 0) {
    return (
      <div className="card p-6 h-full">
        <div className="flex items-center gap-2 mb-4">
          <Hash size={20} className="text-ysrcp-light" />
          <h2 className="text-lg font-semibold text-dashboard-text">Trending Hashtags</h2>
        </div>
        <div className="flex items-center justify-center h-[300px]">
          <Loader2 className="w-8 h-8 animate-spin text-dashboard-textMuted" />
        </div>
      </div>
    );
  }

  const getSentimentStyle = (sentiment) => {
    switch (sentiment) {
      case 'positive':
        return 'bg-sentiment-positive/20 text-sentiment-positive';
      case 'negative':
        return 'bg-sentiment-negative/20 text-sentiment-negative';
      default:
        return 'bg-sentiment-neutral/20 text-sentiment-neutral';
    }
  };

  const getPartyStyle = (party) => {
    switch (party) {
      case 'ysrcp':
        return 'border-l-ysrcp-primary';
      case 'tdp':
        return 'border-l-tdp-primary';
      default:
        return 'border-l-dashboard-border';
    }
  };

  return (
    <div className="card p-6 h-full">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <Hash size={20} className="text-ysrcp-light" />
          <h2 className="text-lg font-semibold text-dashboard-text">Trending Hashtags</h2>
        </div>
        <span className="text-xs text-dashboard-textMuted">Last 24 hours</span>
      </div>

      <div className="space-y-3 max-h-[400px] overflow-y-auto pr-2">
        {trendingHashtags.map((hashtag, index) => (
          <div
            key={hashtag.tag}
            className={`p-3 rounded-lg bg-dashboard-bg border-l-4 ${getPartyStyle(hashtag.party)} hover:bg-dashboard-cardHover transition-colors cursor-pointer group`}
          >
            <div className="flex items-start justify-between">
              <div className="flex-1">
                <div className="flex items-center gap-2">
                  {hashtag.trending && (
                    <Flame size={14} className="text-orange-500 animate-pulse" />
                  )}
                  <span className="font-semibold text-dashboard-text group-hover:text-ysrcp-light transition-colors">
                    {hashtag.tag}
                  </span>
                </div>
                <div className="flex items-center gap-3 mt-1">
                  <span className="text-sm text-dashboard-textMuted">
                    {formatNumber(hashtag.count)} mentions
                  </span>
                  <span className={`text-xs px-2 py-0.5 rounded-full ${getSentimentStyle(hashtag.sentiment)}`}>
                    {hashtag.sentiment}
                  </span>
                </div>
              </div>
              <div className="text-right">
                <span className="text-lg font-bold text-dashboard-textMuted">#{index + 1}</span>
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* Legend */}
      <div className="mt-4 pt-4 border-t border-dashboard-border">
        <div className="flex items-center justify-center gap-6 text-xs text-dashboard-textMuted">
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded-full bg-ysrcp-primary" />
            <span>YSRCP Related</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded-full bg-tdp-primary" />
            <span>TDP Related</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded-full bg-dashboard-border" />
            <span>General</span>
          </div>
        </div>
      </div>
    </div>
  );
};

export default TrendingHashtags;
