import React, { useState } from 'react';
import './TrendingTopics.css';

const TrendingTopics = () => {
  const [trendingData] = useState({
    ysrcp: {
      hotTopics: [
        { topic: "Navaratnalu Schemes", platform: "all", mentions: 45230, sentiment: "positive", trend: "up", change: "+23%" },
        { topic: "Amaravati Development", platform: "youtube", mentions: 32100, sentiment: "mixed", trend: "up", change: "+15%" },
        { topic: "Farmer Welfare Programs", platform: "twitter", mentions: 28900, sentiment: "positive", trend: "up", change: "+18%" },
        { topic: "YS Jagan Rally Vizag", platform: "facebook", mentions: 25400, sentiment: "positive", trend: "up", change: "+32%" },
        { topic: "Free Bus Scheme Women", platform: "instagram", mentions: 21800, sentiment: "positive", trend: "up", change: "+12%" }
      ],
      platforms: {
        youtube: { trending: "CM Jagan Speech on Development", views: "2.3M", engagement: "high" },
        twitter: { trending: "#NavarataluSuccess", tweets: "45.2K", engagement: "very high" },
        facebook: { trending: "Welfare Scheme Beneficiaries", shares: "32.1K", engagement: "high" },
        instagram: { trending: "AP Development Photos", likes: "156K", engagement: "medium" },
        google: { trending: "YS Jagan achievements", searches: "rising", interest: "high" }
      }
    },
    tdp: {
      hotTopics: [
        { topic: "Chandrababu Naidu Campaign", platform: "all", mentions: 38900, sentiment: "mixed", trend: "up", change: "+19%" },
        { topic: "Smart City Vision", platform: "youtube", mentions: 29300, sentiment: "positive", trend: "stable", change: "+8%" },
        { topic: "TDP Manifesto Launch", platform: "twitter", mentions: 26700, sentiment: "positive", trend: "up", change: "+25%" },
        { topic: "Nara Lokesh Padayatra", platform: "facebook", mentions: 24100, sentiment: "positive", trend: "up", change: "+21%" },
        { topic: "TDP IT Initiatives", platform: "instagram", mentions: 19500, sentiment: "positive", trend: "up", change: "+14%" }
      ],
      platforms: {
        youtube: { trending: "CBN on Tech Development", views: "1.8M", engagement: "medium" },
        twitter: { trending: "#TDPForChange", tweets: "38.9K", engagement: "high" },
        facebook: { trending: "Lokesh Padayatra Updates", shares: "28.3K", engagement: "high" },
        instagram: { trending: "TDP Youth Connect", likes: "124K", engagement: "medium" },
        google: { trending: "Chandrababu Naidu plans", searches: "rising", interest: "medium" }
      }
    }
  });

  const getPlatformIcon = (platform) => {
    const icons = {
      youtube: "🎥",
      twitter: "🐦",
      facebook: "📘",
      instagram: "📷",
      google: "🔍",
      all: "🔥"
    };
    return icons[platform] || "📊";
  };

  const getSentimentColor = (sentiment) => {
    const colors = {
      positive: "#4caf50",
      negative: "#f44336",
      mixed: "#ff9800"
    };
    return colors[sentiment] || "#9e9e9e";
  };

  return (
    <div className="trending-topics">
      {/* Hero Section - Trending Now */}
      <div className="hero-trending">
        <h1 className="main-title">
          <span className="fire-icon">🔥</span>
          TRENDING NOW IN ANDHRA PRADESH POLITICS
          <span className="fire-icon">🔥</span>
        </h1>
        <div className="live-indicator">
          <span className="pulse"></span>
          <span>LIVE UPDATES</span>
        </div>
      </div>

      {/* Top Trending Topics - Side by Side */}
      <div className="top-trending-comparison">
        <div className="party-trending ysrcp-section">
          <div className="party-header">
            <h2>YSRCP TRENDING</h2>
            <div className="party-badge ysrcp-badge">YSR Congress Party</div>
          </div>

          <div className="top-topic-card">
            <div className="topic-rank">#1</div>
            <div className="topic-content">
              <div className="topic-icon">{getPlatformIcon(trendingData.ysrcp.hotTopics[0].platform)}</div>
              <div className="topic-details">
                <h3>{trendingData.ysrcp.hotTopics[0].topic}</h3>
                <div className="topic-stats">
                  <span className="mentions">{trendingData.ysrcp.hotTopics[0].mentions.toLocaleString()} mentions</span>
                  <span className="change positive">{trendingData.ysrcp.hotTopics[0].change}</span>
                </div>
              </div>
            </div>
            <div className="topic-meter">
              <div className="meter-fill" style={{ width: "92%", background: getSentimentColor(trendingData.ysrcp.hotTopics[0].sentiment) }}></div>
            </div>
          </div>

          <div className="trending-list">
            {trendingData.ysrcp.hotTopics.slice(1, 5).map((topic, index) => (
              <div key={index} className="trending-item">
                <span className="item-rank">#{index + 2}</span>
                <span className="item-icon">{getPlatformIcon(topic.platform)}</span>
                <span className="item-topic">{topic.topic}</span>
                <span className="item-mentions">{(topic.mentions / 1000).toFixed(1)}K</span>
                <span className={`item-change ${topic.trend}`}>{topic.change}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="party-trending tdp-section">
          <div className="party-header">
            <h2>TDP TRENDING</h2>
            <div className="party-badge tdp-badge">Telugu Desam Party</div>
          </div>

          <div className="top-topic-card">
            <div className="topic-rank">#1</div>
            <div className="topic-content">
              <div className="topic-icon">{getPlatformIcon(trendingData.tdp.hotTopics[0].platform)}</div>
              <div className="topic-details">
                <h3>{trendingData.tdp.hotTopics[0].topic}</h3>
                <div className="topic-stats">
                  <span className="mentions">{trendingData.tdp.hotTopics[0].mentions.toLocaleString()} mentions</span>
                  <span className="change positive">{trendingData.tdp.hotTopics[0].change}</span>
                </div>
              </div>
            </div>
            <div className="topic-meter">
              <div className="meter-fill" style={{ width: "86%", background: getSentimentColor(trendingData.tdp.hotTopics[0].sentiment) }}></div>
            </div>
          </div>

          <div className="trending-list">
            {trendingData.tdp.hotTopics.slice(1, 5).map((topic, index) => (
              <div key={index} className="trending-item">
                <span className="item-rank">#{index + 2}</span>
                <span className="item-icon">{getPlatformIcon(topic.platform)}</span>
                <span className="item-topic">{topic.topic}</span>
                <span className="item-mentions">{(topic.mentions / 1000).toFixed(1)}K</span>
                <span className={`item-change ${topic.trend}`}>{topic.change}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Platform-wise Trending */}
      <div className="platform-trending">
        <h2 className="section-title">PLATFORM-WISE TRENDING TOPICS</h2>

        <div className="platforms-grid">
          {/* YouTube */}
          <div className="platform-card youtube">
            <div className="platform-header">
              <span className="platform-icon">🎥</span>
              <h3>YouTube</h3>
            </div>
            <div className="platform-comparison">
              <div className="party-platform ysrcp">
                <span className="party-label">YSRCP</span>
                <div className="platform-topic">{trendingData.ysrcp.platforms.youtube.trending}</div>
                <div className="platform-stat">{trendingData.ysrcp.platforms.youtube.views} views</div>
              </div>
              <div className="vs-divider">VS</div>
              <div className="party-platform tdp">
                <span className="party-label">TDP</span>
                <div className="platform-topic">{trendingData.tdp.platforms.youtube.trending}</div>
                <div className="platform-stat">{trendingData.tdp.platforms.youtube.views} views</div>
              </div>
            </div>
          </div>

          {/* Twitter/X */}
          <div className="platform-card twitter">
            <div className="platform-header">
              <span className="platform-icon">𝕏</span>
              <h3>Twitter / X</h3>
            </div>
            <div className="platform-comparison">
              <div className="party-platform ysrcp">
                <span className="party-label">YSRCP</span>
                <div className="platform-topic">{trendingData.ysrcp.platforms.twitter.trending}</div>
                <div className="platform-stat">{trendingData.ysrcp.platforms.twitter.tweets} tweets</div>
              </div>
              <div className="vs-divider">VS</div>
              <div className="party-platform tdp">
                <span className="party-label">TDP</span>
                <div className="platform-topic">{trendingData.tdp.platforms.twitter.trending}</div>
                <div className="platform-stat">{trendingData.tdp.platforms.twitter.tweets} tweets</div>
              </div>
            </div>
          </div>

          {/* Facebook */}
          <div className="platform-card facebook">
            <div className="platform-header">
              <span className="platform-icon">📘</span>
              <h3>Facebook</h3>
            </div>
            <div className="platform-comparison">
              <div className="party-platform ysrcp">
                <span className="party-label">YSRCP</span>
                <div className="platform-topic">{trendingData.ysrcp.platforms.facebook.trending}</div>
                <div className="platform-stat">{trendingData.ysrcp.platforms.facebook.shares} shares</div>
              </div>
              <div className="vs-divider">VS</div>
              <div className="party-platform tdp">
                <span className="party-label">TDP</span>
                <div className="platform-topic">{trendingData.tdp.platforms.facebook.trending}</div>
                <div className="platform-stat">{trendingData.tdp.platforms.facebook.shares} shares</div>
              </div>
            </div>
          </div>

          {/* Instagram */}
          <div className="platform-card instagram">
            <div className="platform-header">
              <span className="platform-icon">📷</span>
              <h3>Instagram</h3>
            </div>
            <div className="platform-comparison">
              <div className="party-platform ysrcp">
                <span className="party-label">YSRCP</span>
                <div className="platform-topic">{trendingData.ysrcp.platforms.instagram.trending}</div>
                <div className="platform-stat">{trendingData.ysrcp.platforms.instagram.likes} likes</div>
              </div>
              <div className="vs-divider">VS</div>
              <div className="party-platform tdp">
                <span className="party-label">TDP</span>
                <div className="platform-topic">{trendingData.tdp.platforms.instagram.trending}</div>
                <div className="platform-stat">{trendingData.tdp.platforms.instagram.likes} likes</div>
              </div>
            </div>
          </div>

          {/* Google Trends */}
          <div className="platform-card google">
            <div className="platform-header">
              <span className="platform-icon">🔍</span>
              <h3>Google Trends</h3>
            </div>
            <div className="platform-comparison">
              <div className="party-platform ysrcp">
                <span className="party-label">YSRCP</span>
                <div className="platform-topic">{trendingData.ysrcp.platforms.google.trending}</div>
                <div className="platform-stat">Interest: {trendingData.ysrcp.platforms.google.interest}</div>
              </div>
              <div className="vs-divider">VS</div>
              <div className="party-platform tdp">
                <span className="party-label">TDP</span>
                <div className="platform-topic">{trendingData.tdp.platforms.google.trending}</div>
                <div className="platform-stat">Interest: {trendingData.tdp.platforms.google.interest}</div>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Mock Data Notice */}
      <div className="mock-data-notice">
        <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor">
          <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-2h2v2zm0-4h-2V7h2v6z"/>
        </svg>
        <span>
          Displaying demonstration data. Connect your API keys to see live trending topics across all platforms.
        </span>
      </div>
    </div>
  );
};

export default TrendingTopics;
