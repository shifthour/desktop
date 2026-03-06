import { useState, useEffect } from 'react';
import { Youtube, Play, Eye, ThumbsUp, MessageCircle, ExternalLink, Clock } from 'lucide-react';
import { useDashboard } from '../context/DashboardContext';

const VideoCard = ({ video, index }) => {
  const partyColors = {
    ysrcp: 'border-l-ysrcp-primary',
    tdp: 'border-l-tdp-primary',
    general: 'border-l-gray-500'
  };

  const formatTimeAgo = (dateString) => {
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
      href={video.url}
      target="_blank"
      rel="noopener noreferrer"
      className={`block p-3 rounded-lg bg-dashboard-bg border-l-4 ${partyColors[video.party]} hover:bg-dashboard-cardHover transition-all group`}
    >
      <div className="flex gap-3">
        {/* Thumbnail */}
        <div className="relative flex-shrink-0 w-32 h-20 rounded-lg overflow-hidden bg-dashboard-border">
          {video.thumbnail && !video.thumbnail.includes('sample') ? (
            <img
              src={video.thumbnail}
              alt={video.title}
              className="w-full h-full object-cover"
            />
          ) : (
            <div className="w-full h-full flex items-center justify-center bg-gradient-to-br from-red-600 to-red-800">
              <Youtube size={24} className="text-white" />
            </div>
          )}
          {/* Duration badge */}
          <div className="absolute bottom-1 right-1 px-1.5 py-0.5 bg-black/80 rounded text-[10px] text-white font-medium">
            {video.duration}
          </div>
          {/* Play overlay */}
          <div className="absolute inset-0 flex items-center justify-center bg-black/40 opacity-0 group-hover:opacity-100 transition-opacity">
            <Play size={24} className="text-white fill-white" />
          </div>
        </div>

        {/* Content */}
        <div className="flex-1 min-w-0">
          <h4 className="text-sm font-medium text-dashboard-text line-clamp-2 group-hover:text-ysrcp-light transition-colors">
            {video.title}
          </h4>
          <p className="text-xs text-dashboard-textMuted mt-1 truncate">
            {video.channel}
          </p>
          <div className="flex items-center gap-3 mt-2 text-xs text-dashboard-textMuted">
            <span className="flex items-center gap-1">
              <Eye size={12} />
              {video.viewsFormatted}
            </span>
            <span className="flex items-center gap-1">
              <ThumbsUp size={12} />
              {video.likesFormatted}
            </span>
            <span className="flex items-center gap-1">
              <Clock size={12} />
              {formatTimeAgo(video.publishedAt)}
            </span>
          </div>
        </div>
      </div>
    </a>
  );
};

const YouTubeTrending = () => {
  const { youtube, isLive } = useDashboard();
  const [activeTab, setActiveTab] = useState('all');

  // Get videos based on active tab
  const getVideos = () => {
    if (!youtube) return [];

    if (activeTab === 'all') {
      const allVideos = [
        ...(youtube.ysrcp?.videos || []),
        ...(youtube.tdp?.videos || []),
        ...(youtube.general?.videos || [])
      ];
      // Sort by views and take top 6
      return allVideos.sort((a, b) => b.views - a.views).slice(0, 6);
    }

    return youtube[activeTab]?.videos || [];
  };

  const videos = getVideos();

  // Calculate total views
  const getTotalViews = () => {
    if (!youtube) return '0';
    if (activeTab === 'all') {
      const total = (youtube.ysrcp?.totalViews || 0) +
                    (youtube.tdp?.totalViews || 0) +
                    (youtube.general?.totalViews || 0);
      return formatNumber(total);
    }
    return youtube[activeTab]?.totalViewsFormatted || '0';
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
          <div className="p-2 rounded-lg bg-[#FF0000]/10">
            <Youtube size={20} className="text-[#FF0000]" />
          </div>
          <div>
            <h2 className="text-lg font-semibold text-dashboard-text">YouTube Trending</h2>
            <p className="text-xs text-dashboard-textMuted">Political videos from last 7 days</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          {isLive && youtube && !youtube.message && (
            <span className="flex items-center gap-1 px-2 py-1 rounded-full bg-sentiment-positive/20 text-sentiment-positive text-xs font-medium">
              <span className="w-2 h-2 bg-sentiment-positive rounded-full animate-pulse" />
              LIVE
            </span>
          )}
          <div className="text-right">
            <p className="text-xs text-dashboard-textMuted">Total Views</p>
            <p className="text-lg font-bold text-dashboard-text">{getTotalViews()}</p>
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
            {youtube && youtube[tab.id]?.videos && (
              <span className={`px-1.5 py-0.5 rounded text-[10px] ${
                activeTab === tab.id ? 'bg-white/20' : 'bg-dashboard-border'
              }`}>
                {tab.id === 'all'
                  ? (youtube.ysrcp?.videos?.length || 0) + (youtube.tdp?.videos?.length || 0)
                  : youtube[tab.id]?.videos?.length || 0}
              </span>
            )}
          </button>
        ))}
      </div>

      {/* Video List */}
      <div className="space-y-3 max-h-[500px] overflow-y-auto pr-2">
        {videos.length > 0 ? (
          videos.map((video, index) => (
            <VideoCard key={video.id} video={video} index={index} />
          ))
        ) : (
          <div className="text-center py-8 text-dashboard-textMuted">
            <Youtube size={40} className="mx-auto mb-2 opacity-50" />
            <p>No videos found</p>
            {youtube?.message && (
              <p className="text-xs mt-2">{youtube.message}</p>
            )}
          </div>
        )}
      </div>

      {/* Stats Summary */}
      {youtube && (
        <div className="grid grid-cols-2 gap-4 mt-4 pt-4 border-t border-dashboard-border">
          <div className="p-3 rounded-lg bg-ysrcp-primary/10">
            <div className="flex items-center justify-between">
              <span className="text-xs text-dashboard-textMuted">YSRCP Videos</span>
              <span className="text-sm font-bold text-ysrcp-light">
                {youtube.ysrcp?.videos?.length || 0}
              </span>
            </div>
            <p className="text-lg font-bold text-ysrcp-light mt-1">
              {youtube.ysrcp?.totalViewsFormatted || '0'} views
            </p>
          </div>
          <div className="p-3 rounded-lg bg-tdp-primary/10">
            <div className="flex items-center justify-between">
              <span className="text-xs text-dashboard-textMuted">TDP Videos</span>
              <span className="text-sm font-bold text-tdp-primary">
                {youtube.tdp?.videos?.length || 0}
              </span>
            </div>
            <p className="text-lg font-bold text-tdp-primary mt-1">
              {youtube.tdp?.totalViewsFormatted || '0'} views
            </p>
          </div>
        </div>
      )}

      {/* API Key Notice */}
      {youtube?.message && (
        <div className="mt-4 p-3 rounded-lg bg-amber-500/10 border border-amber-500/20">
          <p className="text-xs text-amber-500">
            <strong>Note:</strong> {youtube.message}
          </p>
        </div>
      )}

      {/* Footer */}
      <div className="mt-4 pt-4 border-t border-dashboard-border text-center">
        <a
          href="https://www.youtube.com/results?search_query=andhra+pradesh+politics"
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1 text-xs text-dashboard-textMuted hover:text-[#FF0000] transition-colors"
        >
          <span>View more on YouTube</span>
          <ExternalLink size={12} />
        </a>
      </div>
    </div>
  );
};

export default YouTubeTrending;
