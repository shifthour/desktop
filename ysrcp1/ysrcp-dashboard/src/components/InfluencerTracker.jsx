import { Users, Twitter, Facebook, Instagram, CheckCircle, MessageCircle, TrendingUp, Wifi, Loader2 } from 'lucide-react';
import { useDashboard } from '../context/DashboardContext';
import { formatNumber } from '../utils/formatters';

const platformIcons = {
  twitter: Twitter,
  facebook: Facebook,
  instagram: Instagram
};

const InfluencerTracker = () => {
  const { influencers: influencerData, loading } = useDashboard();

  // Show loading state if no data
  if (loading || !influencerData?.influencers || influencerData.influencers.length === 0) {
    return (
      <div className="card p-6">
        <div className="flex items-center gap-2 mb-6">
          <Users size={20} className="text-ysrcp-light" />
          <h2 className="text-lg font-semibold text-dashboard-text">Influencer Tracker</h2>
        </div>
        <div className="flex items-center justify-center h-[200px]">
          <Loader2 className="w-8 h-8 animate-spin text-dashboard-textMuted" />
        </div>
      </div>
    );
  }

  const influencers = influencerData.influencers;
  const isRealData = influencerData?.isLive;

  const getSentimentStyle = (sentiment) => {
    switch (sentiment) {
      case 'pro-ysrcp':
        return { bg: 'bg-ysrcp-primary/20', text: 'text-ysrcp-light', label: 'Pro-YSRCP' };
      case 'pro-tdp':
        return { bg: 'bg-tdp-primary/20', text: 'text-tdp-primary', label: 'Pro-TDP' };
      default:
        return { bg: 'bg-sentiment-neutral/20', text: 'text-sentiment-neutral', label: 'Neutral' };
    }
  };

  const proYsrcp = influencers.filter(i => i.sentiment === 'pro-ysrcp');
  const proTdp = influencers.filter(i => i.sentiment === 'pro-tdp');
  const neutral = influencers.filter(i => i.sentiment === 'neutral');

  return (
    <div className="card p-6">
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-2">
          <Users size={20} className="text-ysrcp-light" />
          <h2 className="text-lg font-semibold text-dashboard-text">Influencer Tracker</h2>
          {isRealData && (
            <span className="flex items-center gap-1 text-xs text-green-500 bg-green-500/10 px-2 py-0.5 rounded-full">
              <Wifi size={10} />
              LIVE
            </span>
          )}
        </div>
        <div className="flex items-center gap-4 text-xs text-dashboard-textMuted">
          <div className="flex items-center gap-1">
            <div className="w-2 h-2 rounded-full bg-ysrcp-primary" />
            <span>Pro-YSRCP ({proYsrcp.length})</span>
          </div>
          <div className="flex items-center gap-1">
            <div className="w-2 h-2 rounded-full bg-tdp-primary" />
            <span>Pro-TDP ({proTdp.length})</span>
          </div>
          <div className="flex items-center gap-1">
            <div className="w-2 h-2 rounded-full bg-sentiment-neutral" />
            <span>Neutral ({neutral.length})</span>
          </div>
        </div>
      </div>

      {/* Influencer List */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {influencers.map((influencer) => {
          const PlatformIcon = platformIcons[influencer.platform];
          const sentimentStyle = getSentimentStyle(influencer.sentiment);

          return (
            <div
              key={influencer.id}
              className="p-4 rounded-xl bg-dashboard-bg border border-dashboard-border hover:border-ysrcp-primary/30 transition-all cursor-pointer group"
            >
              {/* Header */}
              <div className="flex items-start justify-between mb-3">
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-full bg-dashboard-card flex items-center justify-center overflow-hidden">
                    {influencer.avatar ? (
                      <img
                        src={influencer.avatar}
                        alt={influencer.name}
                        className="w-full h-full object-cover"
                        onError={(e) => {
                          e.target.style.display = 'none';
                          e.target.parentElement.innerHTML = `<span class="text-lg font-bold text-dashboard-text">${influencer.name?.charAt(0) || 'U'}</span>`;
                        }}
                      />
                    ) : (
                      <span className="text-lg font-bold text-dashboard-text">
                        {influencer.name?.charAt(0) || 'U'}
                      </span>
                    )}
                  </div>
                  <div>
                    <div className="flex items-center gap-1">
                      <h4 className="font-semibold text-dashboard-text text-sm group-hover:text-ysrcp-light transition-colors">
                        {influencer.name}
                      </h4>
                      {influencer.verified && (
                        <CheckCircle size={14} className="text-ysrcp-light fill-ysrcp-light" />
                      )}
                    </div>
                    <p className="text-xs text-dashboard-textMuted">{influencer.handle}</p>
                  </div>
                </div>
                {PlatformIcon && <PlatformIcon size={16} className="text-dashboard-textMuted" />}
              </div>

              {/* Stats */}
              <div className="grid grid-cols-3 gap-2 mb-3">
                <div className="text-center p-2 rounded-lg bg-dashboard-card">
                  <Users size={14} className="mx-auto text-dashboard-textMuted mb-1" />
                  <p className="text-xs font-semibold text-dashboard-text">
                    {formatNumber(influencer.followers)}
                  </p>
                  <p className="text-[10px] text-dashboard-textMuted">Followers</p>
                </div>
                <div className="text-center p-2 rounded-lg bg-dashboard-card">
                  <MessageCircle size={14} className="mx-auto text-dashboard-textMuted mb-1" />
                  <p className="text-xs font-semibold text-dashboard-text">
                    {influencer.recentMentions}
                  </p>
                  <p className="text-[10px] text-dashboard-textMuted">Mentions</p>
                </div>
                <div className="text-center p-2 rounded-lg bg-dashboard-card">
                  <TrendingUp size={14} className="mx-auto text-dashboard-textMuted mb-1" />
                  <p className="text-xs font-semibold text-dashboard-text">
                    {formatNumber(influencer.engagement)}
                  </p>
                  <p className="text-[10px] text-dashboard-textMuted">Engagement</p>
                </div>
              </div>

              {/* Sentiment Badge */}
              <div className="flex justify-center">
                <span className={`px-3 py-1 rounded-full text-xs font-medium ${sentimentStyle.bg} ${sentimentStyle.text}`}>
                  {sentimentStyle.label}
                </span>
              </div>
            </div>
          );
        })}
      </div>

      {/* Summary */}
      <div className="mt-6 pt-4 border-t border-dashboard-border">
        <div className="grid grid-cols-3 gap-4 text-center">
          <div>
            <p className="text-2xl font-bold text-dashboard-text">
              {formatNumber(influencers.reduce((acc, i) => acc + i.followers, 0))}
            </p>
            <p className="text-xs text-dashboard-textMuted">Total Reach</p>
          </div>
          <div>
            <p className="text-2xl font-bold text-dashboard-text">
              {influencers.reduce((acc, i) => acc + i.recentMentions, 0)}
            </p>
            <p className="text-xs text-dashboard-textMuted">Total Mentions (7d)</p>
          </div>
          <div>
            <p className="text-2xl font-bold text-sentiment-positive">
              {Math.round((proYsrcp.length / influencers.length) * 100)}%
            </p>
            <p className="text-xs text-dashboard-textMuted">Pro-YSRCP Share</p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default InfluencerTracker;
