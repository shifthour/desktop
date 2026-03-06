import { useState } from 'react';
import { Facebook, ThumbsUp, MessageCircle, Share2, ExternalLink, Image, Play } from 'lucide-react';
import { useDashboard } from '../context/DashboardContext';

const PostCard = ({ post }) => {
  const partyColors = {
    ysrcp: 'border-l-ysrcp-primary',
    tdp: 'border-l-tdp-primary',
    general: 'border-l-gray-500'
  };

  const formatNumber = (num) => {
    if (num >= 10000000) return `${(num / 10000000).toFixed(1)}Cr`;
    if (num >= 100000) return `${(num / 100000).toFixed(1)}L`;
    if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
    return num?.toString() || '0';
  };

  return (
    <a
      href={post.url || '#'}
      target="_blank"
      rel="noopener noreferrer"
      className={`block p-4 rounded-lg bg-dashboard-bg border-l-4 ${partyColors[post.party] || partyColors.general} hover:bg-dashboard-cardHover transition-all group`}
    >
      {/* Author Info */}
      <div className="flex items-start gap-3">
        <div className="flex-shrink-0 w-10 h-10 rounded-full overflow-hidden bg-dashboard-border">
          {post.authorImage ? (
            <img
              src={post.authorImage}
              alt={post.author}
              className="w-full h-full object-cover"
              onError={(e) => {
                e.target.onerror = null;
                e.target.style.display = 'none';
                e.target.parentElement.innerHTML = `<div class="w-full h-full flex items-center justify-center bg-gradient-to-br from-[#1877F2] to-[#0d65d9] text-white text-sm font-bold">${post.author?.charAt(0) || 'F'}</div>`;
              }}
            />
          ) : (
            <div className="w-full h-full flex items-center justify-center bg-gradient-to-br from-[#1877F2] to-[#0d65d9] text-white text-sm font-bold">
              {post.author?.charAt(0) || 'F'}
            </div>
          )}
        </div>

        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="font-semibold text-sm text-dashboard-text truncate max-w-[200px]">
              {post.author || 'Unknown Page'}
            </span>
            {post.timeAgo && (
              <>
                <span className="text-xs text-dashboard-textMuted">·</span>
                <span className="text-xs text-dashboard-textMuted">{post.timeAgo}</span>
              </>
            )}
          </div>

          {/* Post Message */}
          <p className="text-sm text-dashboard-text mt-2 whitespace-pre-wrap break-words line-clamp-3 group-hover:text-[#1877F2] transition-colors">
            {post.message || 'No content'}
          </p>

          {/* Media Preview */}
          {post.hasMedia && post.thumbnail && (
            <div className="mt-3 rounded-lg overflow-hidden bg-dashboard-border relative">
              <img
                src={post.thumbnail}
                alt="Post media"
                className="w-full max-h-48 object-cover"
                onError={(e) => {
                  e.target.style.display = 'none';
                }}
              />
              {post.hasVideo && (
                <div className="absolute inset-0 flex items-center justify-center bg-black/40">
                  <div className="w-12 h-12 rounded-full bg-[#1877F2] flex items-center justify-center">
                    <Play size={20} className="text-white fill-white ml-1" />
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Engagement Stats */}
          <div className="flex items-center gap-4 mt-3 text-xs text-dashboard-textMuted">
            <span className="flex items-center gap-1 hover:text-[#1877F2] transition-colors">
              <ThumbsUp size={14} />
              {formatNumber(post.reactions || post.likes)}
            </span>
            <span className="flex items-center gap-1 hover:text-[#1877F2] transition-colors">
              <MessageCircle size={14} />
              {formatNumber(post.comments)}
            </span>
            <span className="flex items-center gap-1 hover:text-[#1877F2] transition-colors">
              <Share2 size={14} />
              {formatNumber(post.shares)}
            </span>
          </div>
        </div>
      </div>
    </a>
  );
};

const FacebookTrending = () => {
  const { facebook, isLive } = useDashboard();
  const [activeTab, setActiveTab] = useState('all');

  // Get posts based on active tab
  const getPosts = () => {
    if (!facebook) return [];

    if (activeTab === 'all') {
      return facebook.combined || [];
    }

    return facebook[activeTab]?.posts || [];
  };

  const posts = getPosts();

  // Calculate total engagement
  const getTotalEngagement = () => {
    if (!facebook) return '0';
    if (activeTab === 'all') {
      const total = (facebook.ysrcp?.totalEngagement || 0) + (facebook.tdp?.totalEngagement || 0);
      return formatNumber(total);
    }
    return formatNumber(facebook[activeTab]?.totalEngagement || 0);
  };

  const formatNumber = (num) => {
    if (num >= 10000000) return `${(num / 10000000).toFixed(1)}Cr`;
    if (num >= 100000) return `${(num / 100000).toFixed(1)}L`;
    if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
    return num?.toString() || '0';
  };

  return (
    <div className="card p-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-lg bg-[#1877F2]/10">
            <Facebook size={20} className="text-[#1877F2]" />
          </div>
          <div>
            <h2 className="text-lg font-semibold text-dashboard-text">Facebook Trending</h2>
            <p className="text-xs text-dashboard-textMuted">Latest political posts</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          {isLive && facebook?.isLive && (
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
            {facebook && (
              <span className={`px-1.5 py-0.5 rounded text-[10px] ${
                activeTab === tab.id ? 'bg-white/20' : 'bg-dashboard-border'
              }`}>
                {tab.id === 'all'
                  ? (facebook.ysrcp?.posts?.length || 0) + (facebook.tdp?.posts?.length || 0)
                  : facebook[tab.id]?.posts?.length || 0}
              </span>
            )}
          </button>
        ))}
      </div>

      {/* Post List */}
      <div className="space-y-3 max-h-[500px] overflow-y-auto pr-2">
        {posts.length > 0 ? (
          posts.map((post, index) => (
            <PostCard key={post.id || index} post={post} />
          ))
        ) : (
          <div className="text-center py-8 text-dashboard-textMuted">
            <Facebook size={40} className="mx-auto mb-2 opacity-50" />
            <p>No posts found</p>
            {facebook?.error && (
              <p className="text-xs mt-2 text-red-400">{facebook.error}</p>
            )}
            {!facebook?.isLive && (
              <p className="text-xs mt-2 text-amber-500">Using sample data</p>
            )}
          </div>
        )}
      </div>

      {/* Stats Summary */}
      {facebook && (
        <div className="grid grid-cols-2 gap-4 mt-4 pt-4 border-t border-dashboard-border">
          <div className="p-3 rounded-lg bg-ysrcp-primary/10">
            <div className="flex items-center justify-between">
              <span className="text-xs text-dashboard-textMuted">YSRCP Posts</span>
              <span className="text-sm font-bold text-ysrcp-light">
                {facebook.ysrcp?.posts?.length || 0}
              </span>
            </div>
            <p className="text-lg font-bold text-ysrcp-light mt-1">
              {formatNumber(facebook.ysrcp?.totalEngagement || 0)} engagement
            </p>
          </div>
          <div className="p-3 rounded-lg bg-tdp-primary/10">
            <div className="flex items-center justify-between">
              <span className="text-xs text-dashboard-textMuted">TDP Posts</span>
              <span className="text-sm font-bold text-tdp-primary">
                {facebook.tdp?.posts?.length || 0}
              </span>
            </div>
            <p className="text-lg font-bold text-tdp-primary mt-1">
              {formatNumber(facebook.tdp?.totalEngagement || 0)} engagement
            </p>
          </div>
        </div>
      )}

      {/* Footer */}
      <div className="mt-4 pt-4 border-t border-dashboard-border text-center">
        <a
          href="https://www.facebook.com/search/posts?q=YSRCP%20TDP%20Andhra%20Pradesh"
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1 text-xs text-dashboard-textMuted hover:text-[#1877F2] transition-colors"
        >
          <span>View more on Facebook</span>
          <ExternalLink size={12} />
        </a>
      </div>
    </div>
  );
};

export default FacebookTrending;
