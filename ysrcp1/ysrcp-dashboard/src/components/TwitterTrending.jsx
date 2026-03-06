import { useState } from 'react';
import { Twitter, Heart, Repeat2, MessageCircle, Eye, ExternalLink, Play, BadgeCheck } from 'lucide-react';
import { useDashboard } from '../context/DashboardContext';

const TweetCard = ({ tweet }) => {
  const partyColors = {
    ysrcp: 'border-l-ysrcp-primary',
    tdp: 'border-l-tdp-primary',
    general: 'border-l-gray-500'
  };

  const formatNumber = (num) => {
    if (num >= 10000000) return `${(num / 10000000).toFixed(1)}Cr`;
    if (num >= 100000) return `${(num / 100000).toFixed(1)}L`;
    if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
    return num.toString();
  };

  return (
    <a
      href={tweet.url}
      target="_blank"
      rel="noopener noreferrer"
      className={`block p-4 rounded-lg bg-dashboard-bg border-l-4 ${partyColors[tweet.party] || partyColors.general} hover:bg-dashboard-cardHover transition-all group`}
    >
      {/* User Info */}
      <div className="flex items-start gap-3">
        <div className="flex-shrink-0 w-10 h-10 rounded-full overflow-hidden bg-dashboard-border">
          {tweet.user?.avatar ? (
            <img
              src={tweet.user.avatar}
              alt={tweet.user.name}
              className="w-full h-full object-cover"
              onError={(e) => {
                e.target.onerror = null;
                e.target.src = '';
                e.target.parentElement.innerHTML = `<div class="w-full h-full flex items-center justify-center bg-gradient-to-br from-[#1DA1F2] to-[#0d8bd9] text-white text-sm font-bold">${tweet.user?.name?.charAt(0) || 'T'}</div>`;
              }}
            />
          ) : (
            <div className="w-full h-full flex items-center justify-center bg-gradient-to-br from-[#1DA1F2] to-[#0d8bd9] text-white text-sm font-bold">
              {tweet.user?.name?.charAt(0) || 'T'}
            </div>
          )}
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-1 flex-wrap">
            <span className="font-semibold text-sm text-dashboard-text truncate max-w-[150px]">
              {tweet.user?.name || 'Unknown'}
            </span>
            {tweet.user?.verified && (
              <BadgeCheck size={14} className="text-[#1DA1F2] flex-shrink-0" />
            )}
            <span className="text-xs text-dashboard-textMuted truncate">
              @{tweet.user?.handle || 'unknown'}
            </span>
            <span className="text-xs text-dashboard-textMuted">·</span>
            <span className="text-xs text-dashboard-textMuted">{tweet.timeAgo}</span>
          </div>

          {/* Tweet Text */}
          <p className="text-sm text-dashboard-text mt-2 whitespace-pre-wrap break-words line-clamp-4">
            {tweet.text}
          </p>

          {/* Media Preview */}
          {tweet.hasMedia && tweet.media && tweet.media.length > 0 && (
            <div className="mt-3 rounded-lg overflow-hidden bg-dashboard-border">
              {tweet.hasVideo ? (
                <div className="relative aspect-video bg-black">
                  {tweet.media[0]?.thumbnail && (
                    <img
                      src={tweet.media[0].thumbnail}
                      alt="Video thumbnail"
                      className="w-full h-full object-cover"
                    />
                  )}
                  <div className="absolute inset-0 flex items-center justify-center bg-black/40">
                    <div className="w-12 h-12 rounded-full bg-[#1DA1F2] flex items-center justify-center">
                      <Play size={20} className="text-white fill-white ml-1" />
                    </div>
                  </div>
                </div>
              ) : (
                <img
                  src={tweet.media[0]?.url}
                  alt="Tweet media"
                  className="w-full max-h-48 object-cover"
                  onError={(e) => {
                    e.target.style.display = 'none';
                  }}
                />
              )}
            </div>
          )}

          {/* Engagement Stats */}
          <div className="flex items-center gap-4 mt-3 text-xs text-dashboard-textMuted">
            <span className="flex items-center gap-1 hover:text-[#F91880] transition-colors">
              <Heart size={14} />
              {formatNumber(tweet.engagement?.likes || 0)}
            </span>
            <span className="flex items-center gap-1 hover:text-[#00BA7C] transition-colors">
              <Repeat2 size={14} />
              {formatNumber(tweet.engagement?.retweets || 0)}
            </span>
            <span className="flex items-center gap-1 hover:text-[#1DA1F2] transition-colors">
              <MessageCircle size={14} />
              {formatNumber(tweet.engagement?.replies || 0)}
            </span>
            {tweet.engagement?.views > 0 && (
              <span className="flex items-center gap-1">
                <Eye size={14} />
                {formatNumber(tweet.engagement.views)}
              </span>
            )}
          </div>
        </div>
      </div>
    </a>
  );
};

const TwitterTrending = () => {
  const { twitter, isLive } = useDashboard();
  const [activeTab, setActiveTab] = useState('all');

  // Get tweets based on active tab
  const getTweets = () => {
    if (!twitter) return [];

    if (activeTab === 'all') {
      return twitter.combined || [];
    }

    return twitter[activeTab]?.tweets || [];
  };

  const tweets = getTweets();

  // Calculate total engagement
  const getTotalEngagement = () => {
    if (!twitter) return '0';
    if (activeTab === 'all') {
      const total = (twitter.ysrcp?.totalEngagement || 0) + (twitter.tdp?.totalEngagement || 0);
      return formatNumber(total);
    }
    return formatNumber(twitter[activeTab]?.totalEngagement || 0);
  };

  const formatNumber = (num) => {
    if (num >= 10000000) return `${(num / 10000000).toFixed(1)}Cr`;
    if (num >= 100000) return `${(num / 100000).toFixed(1)}L`;
    if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
    return num.toString();
  };

  return (
    <div className="card p-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-lg bg-[#1DA1F2]/10">
            <Twitter size={20} className="text-[#1DA1F2]" />
          </div>
          <div>
            <h2 className="text-lg font-semibold text-dashboard-text">Twitter/X Trending</h2>
            <p className="text-xs text-dashboard-textMuted">Latest political tweets</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          {isLive && twitter?.isLive && (
            <span className="flex items-center gap-1 px-2 py-1 rounded-full bg-sentiment-positive/20 text-sentiment-positive text-xs font-medium">
              <span className="w-2 h-2 bg-sentiment-positive rounded-full animate-pulse" />
              LIVE
            </span>
          )}
          <div className="text-right">
            <p className="text-xs text-dashboard-textMuted">Total Engagement</p>
            <p className="text-lg font-bold text-dashboard-text">{getTotalEngagement()}</p>
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex gap-2 mb-4">
        {[
          { id: 'all', label: 'All', color: 'bg-gray-500' },
          { id: 'ysrcp', label: 'YSRCP', color: 'bg-ysrcp-primary' },
          { id: 'tdp', label: 'TDP', color: 'bg-tdp-primary' }
        ].map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
              activeTab === tab.id
                ? `${tab.color} text-white`
                : 'bg-dashboard-card text-dashboard-textMuted hover:bg-dashboard-cardHover border border-dashboard-border'
            }`}
          >
            <span>{tab.label}</span>
            {twitter && (
              <span className={`px-1.5 py-0.5 rounded text-[10px] ${
                activeTab === tab.id ? 'bg-white/20' : 'bg-dashboard-border'
              }`}>
                {tab.id === 'all'
                  ? (twitter.ysrcp?.tweets?.length || 0) + (twitter.tdp?.tweets?.length || 0)
                  : twitter[tab.id]?.tweets?.length || 0}
              </span>
            )}
          </button>
        ))}
      </div>

      {/* Tweet List */}
      <div className="space-y-3 max-h-[600px] overflow-y-auto pr-2">
        {tweets.length > 0 ? (
          tweets.map((tweet, index) => (
            <TweetCard key={tweet.id || index} tweet={tweet} />
          ))
        ) : (
          <div className="text-center py-8 text-dashboard-textMuted">
            <Twitter size={40} className="mx-auto mb-2 opacity-50" />
            <p>No tweets found</p>
            {twitter?.error && (
              <p className="text-xs mt-2 text-red-400">{twitter.error}</p>
            )}
          </div>
        )}
      </div>

      {/* Stats Summary */}
      {twitter && (
        <div className="grid grid-cols-2 gap-4 mt-4 pt-4 border-t border-dashboard-border">
          <div className="p-3 rounded-lg bg-ysrcp-primary/10">
            <div className="flex items-center justify-between">
              <span className="text-xs text-dashboard-textMuted">YSRCP Tweets</span>
              <span className="text-sm font-bold text-ysrcp-light">
                {twitter.ysrcp?.tweets?.length || 0}
              </span>
            </div>
            <p className="text-lg font-bold text-ysrcp-light mt-1">
              {formatNumber(twitter.ysrcp?.totalEngagement || 0)} engagement
            </p>
          </div>
          <div className="p-3 rounded-lg bg-tdp-primary/10">
            <div className="flex items-center justify-between">
              <span className="text-xs text-dashboard-textMuted">TDP Tweets</span>
              <span className="text-sm font-bold text-tdp-primary">
                {twitter.tdp?.tweets?.length || 0}
              </span>
            </div>
            <p className="text-lg font-bold text-tdp-primary mt-1">
              {formatNumber(twitter.tdp?.totalEngagement || 0)} engagement
            </p>
          </div>
        </div>
      )}

      {/* Footer */}
      <div className="mt-4 pt-4 border-t border-dashboard-border text-center">
        <a
          href="https://twitter.com/search?q=YSRCP%20OR%20TDP%20OR%20%22YS%20Jagan%22%20OR%20Chandrababu&src=typed_query&f=live"
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1 text-xs text-dashboard-textMuted hover:text-[#1DA1F2] transition-colors"
        >
          <span>View more on Twitter</span>
          <ExternalLink size={12} />
        </a>
      </div>
    </div>
  );
};

export default TwitterTrending;
