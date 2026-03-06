import { Users, MessageCircle, Eye, FileText, TrendingUp, TrendingDown, Loader2 } from 'lucide-react';
import { useDashboard } from '../context/DashboardContext';
import { formatNumber } from '../utils/formatters';

const StatCard = ({ title, ysrcpValue, tdpValue, icon: Icon, formatFn = formatNumber }) => {
  const ysrcpFormatted = formatFn(ysrcpValue);
  const tdpFormatted = formatFn(tdpValue);
  const isYsrcpHigher = ysrcpValue > tdpValue;
  const difference = ((ysrcpValue - tdpValue) / tdpValue * 100).toFixed(1);

  return (
    <div className="stat-card group">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <div className="p-2 rounded-lg bg-ysrcp-primary/10">
            <Icon size={18} className="text-ysrcp-light" />
          </div>
          <span className="text-sm font-medium text-dashboard-textMuted">{title}</span>
        </div>
        <div className={`flex items-center gap-1 text-xs font-medium ${isYsrcpHigher ? 'text-sentiment-positive' : 'text-sentiment-negative'}`}>
          {isYsrcpHigher ? <TrendingUp size={14} /> : <TrendingDown size={14} />}
          <span>{Math.abs(difference)}%</span>
        </div>
      </div>

      <div className="flex items-end justify-between">
        {/* YSRCP */}
        <div className="flex-1">
          <p className="text-xs text-ysrcp-light mb-1">YSRCP</p>
          <p className="text-2xl font-bold text-dashboard-text">{ysrcpFormatted}</p>
        </div>

        {/* Divider */}
        <div className="px-3 text-dashboard-textMuted text-sm">vs</div>

        {/* TDP */}
        <div className="flex-1 text-right">
          <p className="text-xs text-tdp-primary mb-1">TDP</p>
          <p className="text-2xl font-bold text-dashboard-textMuted">{tdpFormatted}</p>
        </div>
      </div>

      {/* Comparison Bar */}
      <div className="mt-3 h-2 bg-dashboard-border rounded-full overflow-hidden flex">
        <div
          className="h-full bg-ysrcp-primary transition-all duration-700"
          style={{ width: `${(ysrcpValue / (ysrcpValue + tdpValue)) * 100}%` }}
        />
        <div
          className="h-full bg-tdp-primary transition-all duration-700"
          style={{ width: `${(tdpValue / (ysrcpValue + tdpValue)) * 100}%` }}
        />
      </div>
    </div>
  );
};

const StatsCards = () => {
  const { overallStats, loading } = useDashboard();

  // Show loading state if no data
  if (loading || !overallStats) {
    return (
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {[1, 2, 3, 4].map((i) => (
          <div key={i} className="stat-card animate-pulse">
            <div className="flex items-center justify-center h-24">
              <Loader2 className="w-6 h-6 animate-spin text-dashboard-textMuted" />
            </div>
          </div>
        ))}
      </div>
    );
  }

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
      <StatCard
        title="Total Reach"
        ysrcpValue={overallStats.ysrcp?.totalReach || 0}
        tdpValue={overallStats.tdp?.totalReach || 0}
        icon={Eye}
      />
      <StatCard
        title="Total Engagement"
        ysrcpValue={overallStats.ysrcp?.totalEngagement || 0}
        tdpValue={overallStats.tdp?.totalEngagement || 0}
        icon={MessageCircle}
      />
      <StatCard
        title="Total Followers"
        ysrcpValue={overallStats.ysrcp?.totalFollowers || 0}
        tdpValue={overallStats.tdp?.totalFollowers || 0}
        icon={Users}
      />
      <StatCard
        title="Posts Today"
        ysrcpValue={overallStats.ysrcp?.postsToday || 0}
        tdpValue={overallStats.tdp?.postsToday || 0}
        icon={FileText}
      />
    </div>
  );
};

export default StatsCards;
