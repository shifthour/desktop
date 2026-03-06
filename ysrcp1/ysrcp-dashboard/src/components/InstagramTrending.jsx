import { useState } from 'react';
import { Instagram, Heart, MessageCircle, ExternalLink, Image, Play } from 'lucide-react';
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

  const formatTimeAgo = (dateString) => {
    if (!dateString) return '';
    const date = new Date(dateString);
    const now = new Date();
    const diffMs = now - date;
    const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
    const diffHours = Math.floor(diffMs / (1000 * 60 * 60));

    if (diffDays > 0) return `${diffDays}d ago`;
    if (diffHours > 0) return `${diffHours}h ago`;
    return 'Just now';
  };

  return (
    <a
      href={post.url || '#'}
      target="_blank"
      rel="noopener noreferrer"
      className={`block p-3 rounded-lg bg-dashboard-bg border-l-4 ${partyColors[post.party] || partyColors.general} hover:bg-dashboard-cardHover transition-all group`}
    >
      <div className="flex gap-3">
        {/* Thumbnail */}
        <div className="relative flex-shrink-0 w-24 h-24 rounded-lg overflow-hidden bg-dashboard-border">
          {post.thumbnail ? (
            <img
              src={post.thumbnail}
              alt={post.caption?.substring(0, 50) || 'Instagram post'}
              className="w-full h-full object-cover"
              onError={(e) => {
                e.target.onerror = null;
                e.target.style.display = 'none';
                e.target.parentElement.innerHTML = `<div class="w-full h-full flex items-center justify-center bg-gradient-to-br from-[#E1306C] to-[#F77737]"><svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2"><rect x="2" y="2" width="20" height="20" rx="5" ry="5"></rect><circle cx="12" cy="12" r="4"></circle><line x1="17.5" y1="6.5" x2="17.51" y2="6.5"></line></svg></div>`;
              }}
            />
          ) : (
            <div className="w-full h-full flex items-center justify-center bg-gradient-to-br from-[#E1306C] to-[#F77737]">
              <Image size={24} className="text-white" />
            </div>
          )}
          {post.isVideo && (
            <div className="absolute inset-0 flex items-center justify-center bg-black/40">
              <Play size={20} className="text-white fill-white" />
            </div>
          )}
        </div>

        {/* Content */}
        <div className="flex-1 min-w-0">
          {/* Username */}
          <div className="flex items-center gap-2 mb-1">
            <span className="text-sm font-semibold text-dashboard-text">
              @{post.username || 'unknown'}
            </span>
            {post.timeAgo && (
              <span className="text-xs text-dashboard-textMuted">
                · {post.timeAgo}
              </span>
            )}
          </div>

          {/* Caption */}
          <p className="text-sm text-dashboard-text line-clamp-2 group-hover:text-pink-400 transition-colors">
            {post.caption || 'No caption'}
          </p>

          {/* Engagement Stats */}
          <div className="flex items-center gap-4 mt-2 text-xs text-dashboard-textMuted">
            <span className="flex items-center gap-1 hover:text-[#E1306C] transition-colors">
              <Heart size={14} />
              {formatNumber(post.likes)}
            </span>
            <span className="flex items-center gap-1 hover:text-[#E1306C] transition-colors">
              <MessageCircle size={14} />
              {formatNumber(post.comments)}
            </span>
          </div>
        </div>
      </div>
    </a>
  );
};

const InstagramTrending = () => {
  const { instagram, isLive } = useDashboard();
  const [activeTab, setActiveTab] = useState('all');

  // Get posts based on active tab
  const getPosts = () => {
    if (!instagram) return [];

    if (activeTab === 'all') {
      return instagram.combined || [];
    }

    return instagram[activeTab]?.posts || [];
  };

  const posts = getPosts();

  // Calculate total engagement
  const getTotalEngagement = () => {
    if (!instagram) return '0';
    if (activeTab === 'all') {
      const total = (instagram.ysrcp?.totalEngagement || 0) + (instagram.tdp?.totalEngagement || 0);
      return formatNumber(total);
    }
    return formatNumber(instagram[activeTab]?.totalEngagement || 0);
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
          <div className="p-2 rounded-lg bg-gradient-to-br from-[#E1306C]/20 to-[#F77737]/20">
            <Instagram size={20} className="text-[#E1306C]" />
          </div>
          <div>
            <h2 className="text-lg font-semibold text-dashboard-text">Instagram Trending</h2>
            <p className="text-xs text-dashboard-textMuted">Latest political posts</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          {isLive && instagram?.isLive && (
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
            {instagram && (
              <span className={`px-1.5 py-0.5 rounded text-[10px] ${
                activeTab === tab.id ? 'bg-white/20' : 'bg-dashboard-border'
              }`}>
                {tab.id === 'all'
                  ? (instagram.ysrcp?.posts?.length || 0) + (instagram.tdp?.posts?.length || 0)
                  : instagram[tab.id]?.posts?.length || 0}
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
            <Instagram size={40} className="mx-auto mb-2 opacity-50" />
            <p>No posts found</p>
            {instagram?.error && (
              <p className="text-xs mt-2 text-red-400">{instagram.error}</p>
            )}
          </div>
        )}
      </div>

      {/* Stats Summary */}
      {instagram && (
        <div className="grid grid-cols-2 gap-4 mt-4 pt-4 border-t border-dashboard-border">
          <div className="p-3 rounded-lg bg-ysrcp-primary/10">
            <div className="flex items-center justify-between">
              <span className="text-xs text-dashboard-textMuted">YSRCP Posts</span>
              <span className="text-sm font-bold text-ysrcp-light">
                {instagram.ysrcp?.posts?.length || 0}
              </span>
            </div>
            <p className="text-lg font-bold text-ysrcp-light mt-1">
              {formatNumber(instagram.ysrcp?.totalEngagement || 0)} engagement
            </p>
          </div>
          <div className="p-3 rounded-lg bg-tdp-primary/10">
            <div className="flex items-center justify-between">
              <span className="text-xs text-dashboard-textMuted">TDP Posts</span>
              <span className="text-sm font-bold text-tdp-primary">
                {instagram.tdp?.posts?.length || 0}
              </span>
            </div>
            <p className="text-lg font-bold text-tdp-primary mt-1">
              {formatNumber(instagram.tdp?.totalEngagement || 0)} engagement
            </p>
          </div>
        </div>
      )}

      {/* Footer */}
      <div className="mt-4 pt-4 border-t border-dashboard-border text-center">
        <a
          href="https://www.instagram.com/explore/tags/ysrcp/"
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1 text-xs text-dashboard-textMuted hover:text-[#E1306C] transition-colors"
        >
          <span>View more on Instagram</span>
          <ExternalLink size={12} />
        </a>
      </div>
    </div>
  );
};

export default InstagramTrending;
