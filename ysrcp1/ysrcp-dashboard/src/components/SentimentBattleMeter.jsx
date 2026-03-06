import { TrendingUp, TrendingDown, Minus, Wifi, WifiOff, Loader2 } from 'lucide-react';
import { useDashboard } from '../context/DashboardContext';

// Party data (static - leaders don't change)
const partyData = {
  ysrcp: { leader: 'YS Jagan Mohan Reddy' },
  tdp: { leader: 'N. Chandrababu Naidu' }
};

const SentimentBattleMeter = () => {
  const { overallStats, sentimentScore, sentimentBattle, loading } = useDashboard();

  // Show loading state if no data
  if (loading || !overallStats) {
    return (
      <div className="card p-6 lg:p-8">
        <div className="text-center mb-6">
          <h2 className="text-lg font-semibold text-dashboard-textMuted">SENTIMENT BATTLE</h2>
        </div>
        <div className="flex items-center justify-center h-[200px]">
          <Loader2 className="w-8 h-8 animate-spin text-dashboard-textMuted" />
        </div>
      </div>
    );
  }

  // Use real sentiment scores from API
  const ysrcpScore = sentimentScore?.ysrcp || 62;
  const tdpScore = sentimentScore?.tdp || 38;
  const difference = ysrcpScore - tdpScore;
  const isYsrcpLeading = difference > 0;
  const isLive = sentimentBattle?.isLive || false;

  return (
    <div className="card p-6 lg:p-8">
      <div className="text-center mb-6">
        <div className="flex items-center justify-center gap-2 mb-1">
          <h2 className="text-lg font-semibold text-dashboard-textMuted">
            SENTIMENT BATTLE
          </h2>
          {isLive && (
            <span className="flex items-center gap-1 text-xs text-green-500 bg-green-500/10 px-2 py-0.5 rounded-full">
              <Wifi size={10} />
              LIVE
            </span>
          )}
        </div>
        <p className="text-sm text-dashboard-textMuted">
          {isLive ? 'Based on real social media engagement' : 'Public sentiment comparison'}
        </p>
      </div>

      {/* Main Battle Display */}
      <div className="flex items-center justify-between gap-4 lg:gap-8 mb-8">
        {/* YSRCP Side */}
        <div className="flex-1 text-center">
          <div className="inline-flex flex-col items-center">
            <div className={`w-20 h-20 lg:w-28 lg:h-28 rounded-full gradient-ysrcp flex items-center justify-center glow-ysrcp transition-transform hover:scale-105`}>
              <span className="text-3xl lg:text-5xl font-bold text-white">{ysrcpScore}</span>
            </div>
            <h3 className="mt-3 text-xl lg:text-2xl font-bold text-ysrcp-light">YSRCP</h3>
            <p className="text-sm text-dashboard-textMuted">{partyData.ysrcp.leader}</p>
          </div>
        </div>

        {/* VS Indicator */}
        <div className="flex flex-col items-center">
          <div className="text-2xl lg:text-3xl font-bold text-dashboard-textMuted mb-2">VS</div>
          <div className={`px-4 py-2 rounded-full ${isYsrcpLeading ? 'bg-ysrcp-primary/20 text-ysrcp-light' : 'bg-tdp-primary/20 text-tdp-primary'} font-semibold flex items-center gap-2`}>
            {isYsrcpLeading ? <TrendingUp size={18} /> : <TrendingDown size={18} />}
            <span>{Math.abs(difference)}%</span>
          </div>
        </div>

        {/* TDP Side */}
        <div className="flex-1 text-center">
          <div className="inline-flex flex-col items-center">
            <div className={`w-20 h-20 lg:w-28 lg:h-28 rounded-full gradient-tdp flex items-center justify-center glow-tdp transition-transform hover:scale-105`}>
              <span className="text-3xl lg:text-5xl font-bold text-gray-900">{tdpScore}</span>
            </div>
            <h3 className="mt-3 text-xl lg:text-2xl font-bold text-tdp-primary">TDP</h3>
            <p className="text-sm text-dashboard-textMuted">{partyData.tdp.leader}</p>
          </div>
        </div>
      </div>

      {/* Battle Bar */}
      <div className="relative">
        <div className="h-4 lg:h-6 bg-dashboard-border rounded-full overflow-hidden flex">
          <div
            className="h-full gradient-ysrcp transition-all duration-1000 ease-out"
            style={{ width: `${ysrcpScore}%` }}
          />
          <div
            className="h-full gradient-tdp transition-all duration-1000 ease-out"
            style={{ width: `${tdpScore}%` }}
          />
        </div>

        {/* Center marker */}
        <div className="absolute top-1/2 left-1/2 transform -translate-x-1/2 -translate-y-1/2">
          <div className="w-1 h-8 lg:h-10 bg-white/30 rounded-full" />
        </div>
      </div>

      {/* Status Message */}
      <div className="mt-6 text-center">
        {isYsrcpLeading ? (
          <p className="text-ysrcp-light font-medium text-lg">
            YSRCP is leading with +{difference}% positive sentiment
          </p>
        ) : difference < 0 ? (
          <p className="text-tdp-primary font-medium text-lg">
            TDP is leading with +{Math.abs(difference)}% positive sentiment
          </p>
        ) : (
          <p className="text-dashboard-textMuted font-medium text-lg flex items-center justify-center gap-2">
            <Minus size={18} /> Both parties have equal sentiment
          </p>
        )}
      </div>

      {/* Quick Stats */}
      <div className="grid grid-cols-2 gap-4 mt-6 pt-6 border-t border-dashboard-border">
        <div className="text-center">
          <p className="text-sm text-dashboard-textMuted mb-1">Share of Voice</p>
          <div className="flex items-center justify-center gap-4">
            <span className="text-ysrcp-light font-bold">
              {sentimentBattle?.ysrcp?.shareOfVoice || overallStats.ysrcp.shareOfVoice}%
            </span>
            <span className="text-dashboard-textMuted">vs</span>
            <span className="text-tdp-primary font-bold">
              {sentimentBattle?.tdp?.shareOfVoice || overallStats.tdp.shareOfVoice}%
            </span>
          </div>
        </div>
        <div className="text-center">
          <p className="text-sm text-dashboard-textMuted mb-1">Engagement Rate</p>
          <div className="flex items-center justify-center gap-4">
            <span className="text-ysrcp-light font-bold">
              {sentimentBattle?.ysrcp?.avgEngagementRate || overallStats.ysrcp.avgEngagementRate}%
            </span>
            <span className="text-dashboard-textMuted">vs</span>
            <span className="text-tdp-primary font-bold">
              {sentimentBattle?.tdp?.avgEngagementRate || overallStats.tdp.avgEngagementRate}%
            </span>
          </div>
        </div>
      </div>
    </div>
  );
};

export default SentimentBattleMeter;
