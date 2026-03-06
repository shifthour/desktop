import { useState } from 'react';
import { Twitter, Facebook, Instagram, Youtube, Newspaper, Users, Eye, MessageCircle, TrendingUp, Heart, Repeat2, Play, ExternalLink, Clock, ThumbsUp, BadgeCheck, Loader2 } from 'lucide-react';
import { useDashboard } from '../context/DashboardContext';
import { formatNumber, formatPercentage } from '../utils/formatters';

const platformIcons = {
  twitter: Twitter,
  facebook: Facebook,
  instagram: Instagram,
  youtube: Youtube,
  news: Newspaper
};

const platformColors = {
  twitter: 'hover:bg-[#1DA1F2]/10 hover:border-[#1DA1F2]/30',
  facebook: 'hover:bg-[#4267B2]/10 hover:border-[#4267B2]/30',
  instagram: 'hover:bg-[#E4405F]/10 hover:border-[#E4405F]/30',
  youtube: 'hover:bg-[#FF0000]/10 hover:border-[#FF0000]/30',
  news: 'hover:bg-gray-500/10 hover:border-gray-500/30'
};

const activeColors = {
  twitter: 'bg-[#1DA1F2] text-white',
  facebook: 'bg-[#4267B2] text-white',
  instagram: 'bg-[#E4405F] text-white',
  youtube: 'bg-[#FF0000] text-white',
  news: 'bg-gray-500 text-white'
};

// Tweet Card Component
const TweetCard = ({ tweet }) => {
  const partyColors = {
    ysrcp: 'border-l-ysrcp-primary',
    tdp: 'border-l-tdp-primary',
    general: 'border-l-gray-500'
  };

  // Check if tweet has media (video or image)
  const hasMedia = tweet.media?.length > 0 || tweet.mediaUrl || tweet.thumbnail;
  const mediaUrl = tweet.media?.[0]?.url || tweet.media?.[0]?.preview_image_url || tweet.mediaUrl || tweet.thumbnail;
  const isVideo = tweet.media?.[0]?.type === 'video' || tweet.hasVideo;

  return (
    <a
      href={tweet.url}
      target="_blank"
      rel="noopener noreferrer"
      className={`block p-3 rounded-lg bg-dashboard-bg border-l-4 ${partyColors[tweet.party] || partyColors.general} hover:bg-dashboard-cardHover transition-all group`}
    >
      <div className="flex items-start gap-3">
        <div className="flex-shrink-0 w-8 h-8 rounded-full overflow-hidden bg-dashboard-border">
          {tweet.user?.avatar ? (
            <img src={tweet.user.avatar} alt="" className="w-full h-full object-cover" onError={(e) => e.target.style.display = 'none'} />
          ) : (
            <div className="w-full h-full flex items-center justify-center bg-[#1DA1F2] text-white text-xs font-bold">
              {tweet.user?.name?.charAt(0) || 'T'}
            </div>
          )}
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-1 flex-wrap">
            <span className="font-medium text-sm text-dashboard-text truncate max-w-[120px]">{tweet.user?.name}</span>
            {tweet.user?.verified && <BadgeCheck size={12} className="text-[#1DA1F2]" />}
            <span className="text-xs text-dashboard-textMuted">· {tweet.timeAgo}</span>
          </div>
          <p className="text-sm text-dashboard-text mt-1 line-clamp-2">{tweet.text}</p>

          {/* Media thumbnail - responsive sizing */}
          {hasMedia && mediaUrl && (
            <div className="relative mt-2 w-full sm:w-auto sm:max-w-[180px] md:max-w-[200px] h-[80px] sm:h-[90px] md:h-[100px] rounded-lg overflow-hidden bg-dashboard-border flex-shrink-0">
              <img
                src={mediaUrl}
                alt=""
                className="w-full h-full object-cover"
                onError={(e) => e.target.parentElement.style.display = 'none'}
              />
              {isVideo && (
                <div className="absolute inset-0 flex items-center justify-center bg-black/40">
                  <Play size={16} className="text-white fill-white sm:w-5 sm:h-5" />
                </div>
              )}
            </div>
          )}

          <div className="flex items-center gap-3 mt-2 text-xs text-dashboard-textMuted">
            <span className="flex items-center gap-1"><Heart size={12} />{formatNumber(tweet.engagement?.likes || 0)}</span>
            <span className="flex items-center gap-1"><Repeat2 size={12} />{formatNumber(tweet.engagement?.retweets || 0)}</span>
            <span className="flex items-center gap-1"><MessageCircle size={12} />{formatNumber(tweet.engagement?.replies || 0)}</span>
          </div>
        </div>
      </div>
    </a>
  );
};

// Video Card Component
const VideoCard = ({ video }) => {
  const partyColors = {
    ysrcp: 'border-l-ysrcp-primary',
    tdp: 'border-l-tdp-primary',
    general: 'border-l-gray-500'
  };

  return (
    <a
      href={video.url}
      target="_blank"
      rel="noopener noreferrer"
      className={`block p-3 rounded-lg bg-dashboard-bg border-l-4 ${partyColors[video.party]} hover:bg-dashboard-cardHover transition-all group`}
    >
      <div className="flex gap-2 sm:gap-3">
        <div className="relative flex-shrink-0 w-24 h-14 sm:w-28 sm:h-16 md:w-32 md:h-18 rounded-lg overflow-hidden bg-dashboard-border">
          {video.thumbnail ? (
            <img src={video.thumbnail} alt="" className="w-full h-full object-cover" />
          ) : (
            <div className="w-full h-full flex items-center justify-center bg-red-600">
              <Youtube size={18} className="text-white sm:w-5 sm:h-5" />
            </div>
          )}
          <div className="absolute bottom-1 right-1 px-1 py-0.5 bg-black/80 rounded text-[8px] sm:text-[9px] text-white">{video.duration}</div>
          <div className="absolute inset-0 flex items-center justify-center bg-black/40 opacity-0 group-hover:opacity-100 transition-opacity">
            <Play size={14} className="text-white fill-white sm:w-4 sm:h-4" />
          </div>
        </div>
        <div className="flex-1 min-w-0">
          <h4 className="text-xs sm:text-sm font-medium text-dashboard-text line-clamp-2 group-hover:text-ysrcp-light">{video.title}</h4>
          <p className="text-[10px] sm:text-xs text-dashboard-textMuted mt-1 truncate">{video.channel}</p>
          <div className="flex items-center gap-2 mt-1 text-[10px] sm:text-xs text-dashboard-textMuted">
            <span className="flex items-center gap-1"><Eye size={10} />{video.viewsFormatted}</span>
            <span className="flex items-center gap-1"><ThumbsUp size={10} />{video.likesFormatted}</span>
          </div>
        </div>
      </div>
    </a>
  );
};

// News Card Component
const NewsCard = ({ article, party }) => {
  const partyColor = party === 'ysrcp' ? 'border-l-ysrcp-primary' : 'border-l-tdp-primary';

  return (
    <a
      href={article.link}
      target="_blank"
      rel="noopener noreferrer"
      className={`block p-3 rounded-lg bg-dashboard-bg border-l-4 ${partyColor} hover:bg-dashboard-cardHover transition-all group`}
    >
      <h4 className="text-sm font-medium text-dashboard-text line-clamp-2 group-hover:text-ysrcp-light">{article.title}</h4>
      <div className="flex items-center gap-2 mt-2 text-xs text-dashboard-textMuted">
        <span className="truncate max-w-[150px]">{article.source}</span>
        <span>·</span>
        <span>{article.publishedAt}</span>
      </div>
    </a>
  );
};

// Instagram Post Card Component
const InstagramCard = ({ post }) => {
  const partyColors = {
    ysrcp: 'border-l-ysrcp-primary',
    tdp: 'border-l-tdp-primary',
    unknown: 'border-l-[#E4405F]'
  };

  return (
    <a
      href={post.url}
      target="_blank"
      rel="noopener noreferrer"
      className={`block p-3 rounded-lg bg-dashboard-bg border-l-4 ${partyColors[post.party] || partyColors.unknown} hover:bg-dashboard-cardHover transition-all group`}
    >
      <div className="flex gap-2 sm:gap-3">
        {post.thumbnail && (
          <div className="relative flex-shrink-0 w-14 h-14 sm:w-16 sm:h-16 rounded-lg overflow-hidden bg-dashboard-border">
            <img src={post.thumbnail} alt="" className="w-full h-full object-cover" onError={(e) => e.target.style.display = 'none'} />
            {post.mediaType === 'video' && (
              <div className="absolute inset-0 flex items-center justify-center bg-black/40">
                <Play size={14} className="text-white fill-white sm:w-4 sm:h-4" />
              </div>
            )}
          </div>
        )}
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-1 mb-1 flex-wrap">
            <span className="text-[11px] sm:text-xs font-medium text-dashboard-text truncate max-w-[100px] sm:max-w-[120px]">{post.owner?.username || 'Unknown'}</span>
            <span className="text-[10px] sm:text-xs text-dashboard-textMuted">· {post.timeAgo}</span>
          </div>
          <p className="text-xs sm:text-sm text-dashboard-text line-clamp-2">{post.caption}</p>
          <div className="flex items-center gap-2 sm:gap-3 mt-2 text-[10px] sm:text-xs text-dashboard-textMuted">
            <span className="flex items-center gap-1"><Heart size={11} />{post.likesFormatted}</span>
            <span className="flex items-center gap-1"><MessageCircle size={11} />{post.commentsFormatted}</span>
          </div>
        </div>
      </div>
    </a>
  );
};

// Facebook Post Card Component
const FacebookCard = ({ post }) => {
  const partyColors = {
    ysrcp: 'border-l-ysrcp-primary',
    tdp: 'border-l-tdp-primary',
    unknown: 'border-l-[#4267B2]'
  };

  // Handle both old format (post.author?.name) and new format (post.author as string)
  const authorName = typeof post.author === 'string' ? post.author : (post.author?.name || 'Unknown');
  const authorImage = post.authorImage || post.author?.profilePic;

  // Check for media - post may have imageUrl, videoUrl, or thumbnail
  const hasMedia = post.imageUrl || post.videoUrl || post.thumbnail || post.hasMedia;
  const mediaUrl = post.imageUrl || post.thumbnail || post.videoUrl;
  const isVideo = post.hasVideo || !!post.videoUrl;

  return (
    <a
      href={post.url}
      target="_blank"
      rel="noopener noreferrer"
      className={`block p-3 rounded-lg bg-dashboard-bg border-l-4 ${partyColors[post.party] || partyColors.unknown} hover:bg-dashboard-cardHover transition-all group`}
    >
      <div className="flex items-start gap-3">
        <div className="flex-shrink-0 w-8 h-8 rounded-full overflow-hidden bg-dashboard-border">
          {authorImage ? (
            <img src={authorImage} alt="" className="w-full h-full object-cover" onError={(e) => e.target.style.display = 'none'} />
          ) : (
            <div className="w-full h-full flex items-center justify-center bg-[#4267B2] text-white text-xs font-bold">
              {authorName?.charAt(0) || 'F'}
            </div>
          )}
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-1 flex-wrap">
            <span className="font-medium text-sm text-dashboard-text truncate max-w-[120px] sm:max-w-[150px]">{authorName}</span>
            <span className="text-xs text-dashboard-textMuted">· {post.timeAgo}</span>
          </div>
          <p className="text-sm text-dashboard-text mt-1 line-clamp-2">{post.message}</p>

          {/* Media thumbnail - responsive sizing */}
          {hasMedia && mediaUrl && (
            <div className="relative mt-2 w-full sm:w-auto sm:max-w-[180px] md:max-w-[200px] h-[80px] sm:h-[90px] md:h-[100px] rounded-lg overflow-hidden bg-dashboard-border flex-shrink-0">
              <img
                src={mediaUrl}
                alt=""
                className="w-full h-full object-cover"
                onError={(e) => e.target.parentElement.style.display = 'none'}
              />
              {isVideo && (
                <div className="absolute inset-0 flex items-center justify-center bg-black/40">
                  <Play size={16} className="text-white fill-white sm:w-5 sm:h-5" />
                </div>
              )}
            </div>
          )}

          <div className="flex items-center gap-2 sm:gap-3 mt-2 text-xs text-dashboard-textMuted flex-wrap">
            <span className="flex items-center gap-1"><ThumbsUp size={12} />{post.reactionsFormatted || formatNumber(post.reactions || 0)}</span>
            <span className="flex items-center gap-1"><MessageCircle size={12} />{post.commentsFormatted || formatNumber(post.comments || 0)}</span>
            <span className="flex items-center gap-1"><Repeat2 size={12} />{post.sharesFormatted || formatNumber(post.shares || 0)}</span>
          </div>
        </div>
      </div>
    </a>
  );
};

const PlatformTabs = () => {
  const [activePlatform, setActivePlatform] = useState('twitter');
  const [trendingFilter, setTrendingFilter] = useState('all');
  const { platformStats, twitter, youtube, news, twitterStats, instagram, facebook, loading } = useDashboard();

  // Show loading state if no data
  if (loading || !platformStats) {
    return (
      <div className="card p-6">
        <h2 className="text-lg font-semibold text-dashboard-text mb-4">Platform Analytics</h2>
        <div className="flex items-center justify-center h-[300px]">
          <Loader2 className="w-8 h-8 animate-spin text-dashboard-textMuted" />
        </div>
      </div>
    );
  }

  const platforms = Object.keys(platformStats);

  // Merge real Twitter stats with platform stats
  const currentPlatform = activePlatform === 'twitter' && twitterStats?.isLive
    ? {
        ...platformStats[activePlatform],
        ysrcp: {
          ...platformStats[activePlatform].ysrcp,
          followers: twitterStats.ysrcp.followers,
          engagement: twitterStats.ysrcp.engagement,
        },
        tdp: {
          ...platformStats[activePlatform].tdp,
          followers: twitterStats.tdp.followers,
          engagement: twitterStats.tdp.engagement,
        }
      }
    : platformStats[activePlatform];
  const Icon = platformIcons[activePlatform];

  // Get trending content based on platform and filter
  const getTrendingContent = () => {
    if (activePlatform === 'twitter' && twitter) {
      if (trendingFilter === 'all') return twitter.combined?.slice(0, 6) || [];
      return twitter[trendingFilter]?.tweets?.slice(0, 6) || [];
    }
    if (activePlatform === 'youtube' && youtube) {
      if (trendingFilter === 'all') {
        const all = [...(youtube.ysrcp?.videos || []), ...(youtube.tdp?.videos || [])];
        return all.sort((a, b) => b.views - a.views).slice(0, 6);
      }
      return youtube[trendingFilter]?.videos?.slice(0, 6) || [];
    }
    if (activePlatform === 'instagram' && instagram) {
      if (trendingFilter === 'all') return instagram.combined?.slice(0, 6) || [];
      return instagram[trendingFilter]?.posts?.slice(0, 6) || [];
    }
    if (activePlatform === 'facebook' && facebook) {
      if (trendingFilter === 'all') return facebook.combined?.slice(0, 6) || [];
      return facebook[trendingFilter]?.posts?.slice(0, 6) || [];
    }
    if (activePlatform === 'news' && news) {
      if (trendingFilter === 'all') {
        return [...(news.ysrcp?.articles || []).slice(0, 3), ...(news.tdp?.articles || []).slice(0, 3)];
      }
      return news[trendingFilter]?.articles?.slice(0, 6) || [];
    }
    return [];
  };

  const trendingContent = getTrendingContent();
  const hasTrending = activePlatform === 'twitter' || activePlatform === 'youtube' || activePlatform === 'instagram' || activePlatform === 'facebook' || activePlatform === 'news';

  const renderSentimentBar = (sentiment) => (
    <div className="flex items-center gap-2">
      <div className="flex-1 h-2 bg-dashboard-border rounded-full overflow-hidden flex">
        <div className="h-full bg-sentiment-positive" style={{ width: `${sentiment.positive}%` }} />
        <div className="h-full bg-sentiment-neutral" style={{ width: `${sentiment.neutral}%` }} />
        <div className="h-full bg-sentiment-negative" style={{ width: `${sentiment.negative}%` }} />
      </div>
      <span className="text-xs text-dashboard-textMuted w-10">{sentiment.positive}%</span>
    </div>
  );

  return (
    <div className="card p-4 sm:p-6">
      <h2 className="text-base sm:text-lg font-semibold text-dashboard-text mb-3 sm:mb-4">Platform Analytics</h2>

      {/* Platform Tabs - Mobile optimized with horizontal scroll */}
      <div className="flex flex-wrap gap-1.5 sm:gap-2 mb-4 sm:mb-6 overflow-x-auto pb-1 -mx-1 px-1">
        {platforms.map((platform) => {
          const PlatformIcon = platformIcons[platform];
          const isActive = activePlatform === platform;
          return (
            <button
              key={platform}
              onClick={() => { setActivePlatform(platform); setTrendingFilter('all'); }}
              className={`flex items-center gap-1.5 sm:gap-2 px-2.5 sm:px-4 py-1.5 sm:py-2 rounded-lg border transition-all duration-200 flex-shrink-0 ${
                isActive
                  ? activeColors[platform]
                  : `bg-dashboard-card border-dashboard-border text-dashboard-textMuted ${platformColors[platform]}`
              }`}
            >
              <PlatformIcon size={16} className="sm:w-[18px] sm:h-[18px]" />
              <span className="font-medium text-sm sm:text-base">{platformStats[platform].name.split(' ')[0]}</span>
            </button>
          );
        })}
      </div>

      {/* Platform Content */}
      <div className="animate-fade-in">
        <div className="flex items-center gap-2 mb-3 sm:mb-4">
          <Icon size={20} className="text-dashboard-text sm:w-6 sm:h-6" />
          <h3 className="text-lg sm:text-xl font-bold text-dashboard-text">{currentPlatform.name}</h3>
        </div>

        {activePlatform !== 'news' ? (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 sm:gap-6">
            {/* YSRCP Stats */}
            <div className="p-3 sm:p-4 rounded-xl bg-ysrcp-primary/5 border border-ysrcp-primary/20">
              <div className="flex items-center gap-2 mb-3 sm:mb-4">
                <div className="w-2.5 h-2.5 sm:w-3 sm:h-3 rounded-full bg-ysrcp-primary" />
                <h4 className="font-semibold text-sm sm:text-base text-ysrcp-light">YSRCP</h4>
              </div>

              <div className="grid grid-cols-2 gap-3 sm:gap-4 mb-3 sm:mb-4">
                <div>
                  <div className="flex items-center gap-1.5 sm:gap-2 text-dashboard-textMuted text-xs sm:text-sm mb-1">
                    <Users size={12} className="sm:w-[14px] sm:h-[14px]" />
                    <span>Followers</span>
                  </div>
                  <p className="text-lg sm:text-xl font-bold text-dashboard-text">
                    {formatNumber(currentPlatform.ysrcp.followers)}
                  </p>
                  <p className="text-[10px] sm:text-xs text-sentiment-positive">
                    +{formatNumber(currentPlatform.ysrcp.followersGrowth)} this week
                  </p>
                </div>
                <div>
                  <div className="flex items-center gap-1.5 sm:gap-2 text-dashboard-textMuted text-xs sm:text-sm mb-1">
                    <Eye size={12} className="sm:w-[14px] sm:h-[14px]" />
                    <span>Reach</span>
                  </div>
                  <p className="text-lg sm:text-xl font-bold text-dashboard-text">
                    {formatNumber(currentPlatform.ysrcp.reach)}
                  </p>
                </div>
                <div>
                  <div className="flex items-center gap-1.5 sm:gap-2 text-dashboard-textMuted text-xs sm:text-sm mb-1">
                    <MessageCircle size={12} className="sm:w-[14px] sm:h-[14px]" />
                    <span>Engagement</span>
                  </div>
                  <p className="text-lg sm:text-xl font-bold text-dashboard-text">
                    {formatNumber(currentPlatform.ysrcp.engagement)}
                  </p>
                </div>
                <div>
                  <div className="flex items-center gap-1.5 sm:gap-2 text-dashboard-textMuted text-xs sm:text-sm mb-1">
                    <TrendingUp size={12} className="sm:w-[14px] sm:h-[14px]" />
                    <span>Eng. Rate</span>
                  </div>
                  <p className="text-lg sm:text-xl font-bold text-dashboard-text">
                    {currentPlatform.ysrcp.engagementRate}%
                  </p>
                </div>
              </div>

              <div>
                <p className="text-xs sm:text-sm text-dashboard-textMuted mb-2">Sentiment</p>
                {renderSentimentBar(currentPlatform.ysrcp.sentiment)}
              </div>
            </div>

            {/* TDP Stats */}
            <div className="p-3 sm:p-4 rounded-xl bg-tdp-primary/5 border border-tdp-primary/20">
              <div className="flex items-center gap-2 mb-3 sm:mb-4">
                <div className="w-2.5 h-2.5 sm:w-3 sm:h-3 rounded-full bg-tdp-primary" />
                <h4 className="font-semibold text-sm sm:text-base text-tdp-primary">TDP</h4>
              </div>

              <div className="grid grid-cols-2 gap-3 sm:gap-4 mb-3 sm:mb-4">
                <div>
                  <div className="flex items-center gap-1.5 sm:gap-2 text-dashboard-textMuted text-xs sm:text-sm mb-1">
                    <Users size={12} className="sm:w-[14px] sm:h-[14px]" />
                    <span>Followers</span>
                  </div>
                  <p className="text-lg sm:text-xl font-bold text-dashboard-text">
                    {formatNumber(currentPlatform.tdp.followers)}
                  </p>
                  <p className="text-[10px] sm:text-xs text-sentiment-positive">
                    +{formatNumber(currentPlatform.tdp.followersGrowth)} this week
                  </p>
                </div>
                <div>
                  <div className="flex items-center gap-1.5 sm:gap-2 text-dashboard-textMuted text-xs sm:text-sm mb-1">
                    <Eye size={12} className="sm:w-[14px] sm:h-[14px]" />
                    <span>Reach</span>
                  </div>
                  <p className="text-lg sm:text-xl font-bold text-dashboard-text">
                    {formatNumber(currentPlatform.tdp.reach)}
                  </p>
                </div>
                <div>
                  <div className="flex items-center gap-1.5 sm:gap-2 text-dashboard-textMuted text-xs sm:text-sm mb-1">
                    <MessageCircle size={12} className="sm:w-[14px] sm:h-[14px]" />
                    <span>Engagement</span>
                  </div>
                  <p className="text-lg sm:text-xl font-bold text-dashboard-text">
                    {formatNumber(currentPlatform.tdp.engagement)}
                  </p>
                </div>
                <div>
                  <div className="flex items-center gap-1.5 sm:gap-2 text-dashboard-textMuted text-xs sm:text-sm mb-1">
                    <TrendingUp size={12} className="sm:w-[14px] sm:h-[14px]" />
                    <span>Eng. Rate</span>
                  </div>
                  <p className="text-lg sm:text-xl font-bold text-dashboard-text">
                    {currentPlatform.tdp.engagementRate}%
                  </p>
                </div>
              </div>

              <div>
                <p className="text-xs sm:text-sm text-dashboard-textMuted mb-2">Sentiment</p>
                {renderSentimentBar(currentPlatform.tdp.sentiment)}
              </div>
            </div>
          </div>
        ) : (
          /* News Stats */
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="p-4 rounded-xl bg-ysrcp-primary/5 border border-ysrcp-primary/20">
              <div className="flex items-center gap-2 mb-4">
                <div className="w-3 h-3 rounded-full bg-ysrcp-primary" />
                <h4 className="font-semibold text-ysrcp-light">YSRCP</h4>
              </div>
              <div className="grid grid-cols-2 gap-4 mb-4">
                <div>
                  <p className="text-sm text-dashboard-textMuted mb-1">News Mentions</p>
                  <p className="text-2xl font-bold text-dashboard-text">{currentPlatform.ysrcp.mentions}</p>
                </div>
                <div>
                  <p className="text-sm text-dashboard-textMuted mb-1">Media Reach</p>
                  <p className="text-2xl font-bold text-dashboard-text">{formatNumber(currentPlatform.ysrcp.reach)}</p>
                </div>
              </div>
              <div>
                <p className="text-sm text-dashboard-textMuted mb-2">Sentiment</p>
                {renderSentimentBar(currentPlatform.ysrcp.sentiment)}
              </div>
            </div>

            <div className="p-4 rounded-xl bg-tdp-primary/5 border border-tdp-primary/20">
              <div className="flex items-center gap-2 mb-4">
                <div className="w-3 h-3 rounded-full bg-tdp-primary" />
                <h4 className="font-semibold text-tdp-primary">TDP</h4>
              </div>
              <div className="grid grid-cols-2 gap-4 mb-4">
                <div>
                  <p className="text-sm text-dashboard-textMuted mb-1">News Mentions</p>
                  <p className="text-2xl font-bold text-dashboard-text">{currentPlatform.tdp.mentions}</p>
                </div>
                <div>
                  <p className="text-sm text-dashboard-textMuted mb-1">Media Reach</p>
                  <p className="text-2xl font-bold text-dashboard-text">{formatNumber(currentPlatform.tdp.reach)}</p>
                </div>
              </div>
              <div>
                <p className="text-sm text-dashboard-textMuted mb-2">Sentiment</p>
                {renderSentimentBar(currentPlatform.tdp.sentiment)}
              </div>
            </div>
          </div>
        )}

        {/* Trending Content Section */}
        {hasTrending && (
          <div className="mt-4 sm:mt-6 pt-4 sm:pt-6 border-t border-dashboard-border">
            <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2 sm:gap-0 mb-3 sm:mb-4">
              <h4 className="text-sm sm:text-md font-semibold text-dashboard-text flex items-center gap-1.5 sm:gap-2">
                <TrendingUp size={16} className="sm:w-[18px] sm:h-[18px]" />
                Trending {activePlatform === 'twitter' ? 'Tweets' : activePlatform === 'youtube' ? 'Videos' : activePlatform === 'instagram' ? 'Posts' : activePlatform === 'facebook' ? 'Posts' : 'News'}
              </h4>

              {/* Filter Tabs */}
              <div className="flex gap-1">
                {['all', 'ysrcp', 'tdp'].map((filter) => (
                  <button
                    key={filter}
                    onClick={() => setTrendingFilter(filter)}
                    className={`px-2 sm:px-3 py-1 rounded-md text-[10px] sm:text-xs font-medium transition-colors ${
                      trendingFilter === filter
                        ? filter === 'ysrcp' ? 'bg-ysrcp-primary text-white'
                          : filter === 'tdp' ? 'bg-tdp-primary text-white'
                          : 'bg-gray-500 text-white'
                        : 'bg-dashboard-card text-dashboard-textMuted hover:bg-dashboard-cardHover border border-dashboard-border'
                    }`}
                  >
                    {filter === 'all' ? 'All' : filter.toUpperCase()}
                  </button>
                ))}
              </div>
            </div>

            {/* Trending Content Grid */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-2 sm:gap-3 max-h-[350px] sm:max-h-[400px] overflow-y-auto pr-1 sm:pr-2">
              {trendingContent.length > 0 ? (
                activePlatform === 'twitter' ? (
                  trendingContent.map((tweet, idx) => <TweetCard key={tweet.id || idx} tweet={tweet} />)
                ) : activePlatform === 'youtube' ? (
                  trendingContent.map((video, idx) => <VideoCard key={video.id || idx} video={video} />)
                ) : activePlatform === 'instagram' ? (
                  trendingContent.map((post, idx) => <InstagramCard key={post.id || idx} post={post} />)
                ) : activePlatform === 'facebook' ? (
                  trendingContent.map((post, idx) => <FacebookCard key={post.id || idx} post={post} />)
                ) : (
                  trendingContent.map((article, idx) => (
                    <NewsCard
                      key={article.link || idx}
                      article={article}
                      party={news?.ysrcp?.articles?.includes(article) ? 'ysrcp' : 'tdp'}
                    />
                  ))
                )
              ) : (
                <div className="col-span-2 text-center py-8 text-dashboard-textMuted">
                  <Icon size={32} className="mx-auto mb-2 opacity-50" />
                  <p className="text-sm">No trending content available</p>
                  {activePlatform === 'facebook' && !facebook?.isLive && (
                    <p className="text-xs mt-1">Facebook page IDs not configured</p>
                  )}
                </div>
              )}
            </div>

            {/* View More Link */}
            {trendingContent.length > 0 && (
              <div className="mt-4 text-center">
                <a
                  href={
                    activePlatform === 'twitter'
                      ? 'https://twitter.com/search?q=YSRCP%20OR%20TDP&src=typed_query&f=live'
                      : activePlatform === 'youtube'
                      ? 'https://www.youtube.com/results?search_query=andhra+pradesh+politics'
                      : activePlatform === 'instagram'
                      ? 'https://www.instagram.com/explore/tags/ysrcp/'
                      : activePlatform === 'facebook'
                      ? 'https://www.facebook.com/search/posts?q=YSRCP'
                      : 'https://news.google.com/search?q=YSRCP%20OR%20TDP'
                  }
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-1 text-xs text-dashboard-textMuted hover:text-ysrcp-light transition-colors"
                >
                  <span>View more on {activePlatform === 'twitter' ? 'Twitter' : activePlatform === 'youtube' ? 'YouTube' : activePlatform === 'instagram' ? 'Instagram' : activePlatform === 'facebook' ? 'Facebook' : 'Google News'}</span>
                  <ExternalLink size={12} />
                </a>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default PlatformTabs;
