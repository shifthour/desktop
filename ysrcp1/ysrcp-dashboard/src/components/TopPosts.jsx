import { useState } from 'react';
import { Twitter, Facebook, Instagram, Youtube, Heart, MessageCircle, Share2, Eye, Clock, Image, Wifi, Loader2 } from 'lucide-react';
import { useDashboard } from '../context/DashboardContext';
import { formatNumber } from '../utils/formatters';

const platformIcons = {
  twitter: Twitter,
  facebook: Facebook,
  instagram: Instagram,
  youtube: Youtube
};

const platformColors = {
  twitter: 'text-[#1DA1F2]',
  facebook: 'text-[#4267B2]',
  instagram: 'text-[#E4405F]',
  youtube: 'text-[#FF0000]'
};

const PostCard = ({ post, party }) => {
  const PlatformIcon = platformIcons[post.platform];
  const isYsrcp = party === 'ysrcp';

  return (
    <div className={`p-4 rounded-xl border ${isYsrcp ? 'bg-ysrcp-primary/5 border-ysrcp-primary/20' : 'bg-tdp-primary/5 border-tdp-primary/20'} hover:bg-dashboard-cardHover transition-colors`}>
      {/* Header */}
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <PlatformIcon size={18} className={platformColors[post.platform]} />
          <span className="text-sm font-medium capitalize text-dashboard-textMuted">{post.platform}</span>
        </div>
        <div className="flex items-center gap-1 text-xs text-dashboard-textMuted">
          <Clock size={12} />
          <span>{post.time}</span>
        </div>
      </div>

      {/* Content */}
      <p className="text-sm text-dashboard-text mb-3 line-clamp-2">
        {post.content}
      </p>

      {/* Image Indicator */}
      {post.image && (
        <div className="flex items-center gap-1 text-xs text-dashboard-textMuted mb-3">
          <Image size={12} />
          <span>Contains media</span>
        </div>
      )}

      {/* Stats */}
      <div className="grid grid-cols-4 gap-2 pt-3 border-t border-dashboard-border">
        <div className="text-center">
          <div className="flex items-center justify-center gap-1 text-sentiment-negative mb-1">
            <Heart size={14} />
          </div>
          <p className="text-xs font-medium text-dashboard-text">{formatNumber(post.likes)}</p>
        </div>
        <div className="text-center">
          <div className="flex items-center justify-center gap-1 text-ysrcp-light mb-1">
            <MessageCircle size={14} />
          </div>
          <p className="text-xs font-medium text-dashboard-text">{formatNumber(post.comments)}</p>
        </div>
        <div className="text-center">
          <div className="flex items-center justify-center gap-1 text-sentiment-positive mb-1">
            <Share2 size={14} />
          </div>
          <p className="text-xs font-medium text-dashboard-text">{formatNumber(post.shares)}</p>
        </div>
        <div className="text-center">
          <div className="flex items-center justify-center gap-1 text-dashboard-textMuted mb-1">
            <Eye size={14} />
          </div>
          <p className="text-xs font-medium text-dashboard-text">{formatNumber(post.reach)}</p>
        </div>
      </div>
    </div>
  );
};

const TopPosts = () => {
  const [activeTab, setActiveTab] = useState('ysrcp');
  const { twitter, instagram, facebook, youtube, isLive } = useDashboard();

  // Aggregate top posts from all platforms
  const getTopPosts = (party) => {
    const allPosts = [];

    // Add Twitter posts
    const twitterPosts = twitter?.[party]?.tweets || [];
    twitterPosts.forEach(tweet => {
      allPosts.push({
        id: `twitter_${tweet.id}`,
        platform: 'twitter',
        content: tweet.text || '',
        likes: tweet.engagement?.likes || 0,
        comments: tweet.engagement?.replies || 0,
        shares: tweet.engagement?.retweets || 0,
        reach: tweet.engagement?.views || 0,
        time: tweet.timeAgo || '',
        image: tweet.media?.length > 0,
        url: tweet.url
      });
    });

    // Add Instagram posts
    const instagramPosts = instagram?.[party]?.posts || [];
    instagramPosts.forEach(post => {
      allPosts.push({
        id: `instagram_${post.id}`,
        platform: 'instagram',
        content: post.caption || '',
        likes: post.likes || 0,
        comments: post.comments || 0,
        shares: 0,
        reach: (post.likes || 0) + (post.comments || 0),
        time: post.timeAgo || '',
        image: !!post.thumbnail,
        url: post.url
      });
    });

    // Add Facebook posts
    const facebookPosts = facebook?.[party]?.posts || [];
    facebookPosts.forEach(post => {
      allPosts.push({
        id: `facebook_${post.id}`,
        platform: 'facebook',
        content: post.message || '',
        likes: post.reactions || post.likes || 0,
        comments: post.comments || 0,
        shares: post.shares || 0,
        reach: (post.reactions || 0) + (post.comments || 0) + (post.shares || 0),
        time: post.timeAgo || '',
        image: post.hasMedia || !!post.thumbnail,
        url: post.url
      });
    });

    // Add YouTube videos
    const youtubeVideos = youtube?.[party]?.videos || [];
    youtubeVideos.forEach(video => {
      allPosts.push({
        id: `youtube_${video.id}`,
        platform: 'youtube',
        content: video.title || '',
        likes: video.likes || 0,
        comments: video.comments || 0,
        shares: 0,
        reach: video.views || 0,
        time: video.timeAgo || video.publishedAt || '',
        image: !!video.thumbnail,
        url: video.url
      });
    });

    // Sort by total engagement and return top 3
    allPosts.sort((a, b) => {
      const engagementA = a.likes + a.comments + a.shares;
      const engagementB = b.likes + b.comments + b.shares;
      return engagementB - engagementA;
    });

    return allPosts.slice(0, 3);
  };

  const topPosts = getTopPosts(activeTab);
  const hasRealData = topPosts.length > 0;

  // Show loading if no data
  if (!hasRealData) {
    return (
      <div className="card p-6">
        <div className="flex items-center gap-2 mb-6">
          <h2 className="text-lg font-semibold text-dashboard-text">Top Performing Posts</h2>
        </div>
        <div className="flex items-center justify-center h-[200px]">
          <Loader2 className="w-8 h-8 animate-spin text-dashboard-textMuted" />
        </div>
      </div>
    );
  }

  const displayPosts = topPosts;

  return (
    <div className="card p-6">
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-2">
          <h2 className="text-lg font-semibold text-dashboard-text">Top Performing Posts</h2>
          {hasRealData && isLive && (
            <span className="flex items-center gap-1 text-xs text-green-500 bg-green-500/10 px-2 py-0.5 rounded-full">
              <Wifi size={10} />
              LIVE
            </span>
          )}
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setActiveTab('ysrcp')}
            className={`px-4 py-1.5 rounded-lg text-sm font-medium transition-colors ${
              activeTab === 'ysrcp'
                ? 'bg-ysrcp-primary text-white'
                : 'bg-dashboard-card text-dashboard-textMuted hover:bg-dashboard-cardHover border border-dashboard-border'
            }`}
          >
            YSRCP
          </button>
          <button
            onClick={() => setActiveTab('tdp')}
            className={`px-4 py-1.5 rounded-lg text-sm font-medium transition-colors ${
              activeTab === 'tdp'
                ? 'bg-tdp-primary text-gray-900'
                : 'bg-dashboard-card text-dashboard-textMuted hover:bg-dashboard-cardHover border border-dashboard-border'
            }`}
          >
            TDP
          </button>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 animate-fade-in">
        {displayPosts.map((post) => (
          <PostCard key={post.id} post={post} party={activeTab} />
        ))}
      </div>

      {/* Summary */}
      <div className="mt-6 pt-4 border-t border-dashboard-border">
        <div className="flex items-center justify-between text-sm">
          <span className="text-dashboard-textMuted">
            Total Engagement (Top 3 Posts):
          </span>
          <span className={`font-bold ${activeTab === 'ysrcp' ? 'text-ysrcp-light' : 'text-tdp-primary'}`}>
            {formatNumber(
              displayPosts.reduce(
                (acc, post) => acc + (post.likes || 0) + (post.comments || 0) + (post.shares || 0),
                0
              )
            )}
          </span>
        </div>
      </div>
    </div>
  );
};

export default TopPosts;
