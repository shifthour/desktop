import { useState } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, AreaChart, Area } from 'recharts';
import { TrendingUp, BarChart3, Wifi, Loader2 } from 'lucide-react';
import { useDashboard } from '../context/DashboardContext';
import { formatCompactNumber } from '../utils/formatters';

const CustomTooltip = ({ active, payload, label, type }) => {
  if (active && payload && payload.length) {
    return (
      <div className="bg-dashboard-card border border-dashboard-border rounded-lg p-3 shadow-xl">
        <p className="text-sm text-dashboard-textMuted mb-2">{label}</p>
        {payload.map((entry, index) => (
          <div key={index} className="flex items-center gap-2 text-sm">
            <div
              className="w-3 h-3 rounded-full"
              style={{ backgroundColor: entry.color }}
            />
            <span className="text-dashboard-text font-medium">
              {entry.name}: {type === 'sentiment' ? `${entry.value}%` : formatCompactNumber(entry.value)}
            </span>
          </div>
        ))}
      </div>
    );
  }
  return null;
};

const EngagementChart = () => {
  const [chartType, setChartType] = useState('sentiment');
  const { googleTrends, loading } = useDashboard();

  // Use real Google Trends data for sentiment (search interest as sentiment proxy)
  const realSentimentData = googleTrends?.interest?.searchTimeline?.slice(-7) || [];
  const isLive = realSentimentData.length > 0;

  // Show loading state if no data
  if (loading || !isLive) {
    return (
      <div className="card p-6">
        <div className="flex items-center gap-2 mb-6">
          <TrendingUp size={20} className="text-ysrcp-light" />
          <h2 className="text-lg font-semibold text-dashboard-text">7-Day Trend Analysis</h2>
        </div>
        <div className="flex items-center justify-center h-[300px]">
          <Loader2 className="w-8 h-8 animate-spin text-dashboard-textMuted" />
        </div>
      </div>
    );
  }

  // Format real data for the chart
  const sentimentTrend = realSentimentData.map(item => ({
    date: item.date,
    ysrcp: item.ysrcp,
    tdp: item.tdp
  }));

  // For engagement, we can estimate from sentiment * multiplier
  const engagementTrend = realSentimentData.map(item => ({
    date: item.date,
    ysrcp: item.ysrcp * 10000, // Scale up for engagement numbers
    tdp: item.tdp * 10000
  }));

  const data = chartType === 'sentiment' ? sentimentTrend : engagementTrend;

  return (
    <div className="card p-6">
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-2">
          <TrendingUp size={20} className="text-ysrcp-light" />
          <h2 className="text-lg font-semibold text-dashboard-text">7-Day Trend Analysis</h2>
          {isLive && (
            <span className="flex items-center gap-1 text-xs text-green-500 bg-green-500/10 px-2 py-0.5 rounded-full">
              <Wifi size={10} />
              LIVE
            </span>
          )}
        </div>

        <div className="flex items-center gap-2">
          <button
            onClick={() => setChartType('sentiment')}
            className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${
              chartType === 'sentiment'
                ? 'bg-ysrcp-primary text-white'
                : 'bg-dashboard-card text-dashboard-textMuted hover:bg-dashboard-cardHover'
            }`}
          >
            Sentiment
          </button>
          <button
            onClick={() => setChartType('engagement')}
            className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${
              chartType === 'engagement'
                ? 'bg-ysrcp-primary text-white'
                : 'bg-dashboard-card text-dashboard-textMuted hover:bg-dashboard-cardHover'
            }`}
          >
            Engagement
          </button>
        </div>
      </div>

      <div className="h-[300px]">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={data} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
            <defs>
              <linearGradient id="ysrcpGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#0066CC" stopOpacity={0.3} />
                <stop offset="95%" stopColor="#0066CC" stopOpacity={0} />
              </linearGradient>
              <linearGradient id="tdpGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#FFD700" stopOpacity={0.3} />
                <stop offset="95%" stopColor="#FFD700" stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#2a2a4a" />
            <XAxis
              dataKey="date"
              axisLine={false}
              tickLine={false}
              tick={{ fill: '#9ca3af', fontSize: 12 }}
            />
            <YAxis
              axisLine={false}
              tickLine={false}
              tick={{ fill: '#9ca3af', fontSize: 12 }}
              tickFormatter={chartType === 'engagement' ? formatCompactNumber : (v) => `${v}%`}
            />
            <Tooltip content={<CustomTooltip type={chartType} />} />
            <Legend
              wrapperStyle={{ paddingTop: '20px' }}
              formatter={(value) => <span className="text-dashboard-text text-sm">{value}</span>}
            />
            <Area
              type="monotone"
              dataKey="ysrcp"
              name="YSRCP"
              stroke="#0066CC"
              strokeWidth={3}
              fill="url(#ysrcpGradient)"
              dot={{ fill: '#0066CC', strokeWidth: 2 }}
              activeDot={{ r: 6, fill: '#0066CC', stroke: '#fff', strokeWidth: 2 }}
            />
            <Area
              type="monotone"
              dataKey="tdp"
              name="TDP"
              stroke="#FFD700"
              strokeWidth={3}
              fill="url(#tdpGradient)"
              dot={{ fill: '#FFD700', strokeWidth: 2 }}
              activeDot={{ r: 6, fill: '#FFD700', stroke: '#fff', strokeWidth: 2 }}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>

      {/* Summary */}
      <div className="grid grid-cols-2 gap-4 mt-6 pt-4 border-t border-dashboard-border">
        <div className="text-center">
          <p className="text-sm text-dashboard-textMuted mb-1">YSRCP Weekly Avg</p>
          <p className="text-xl font-bold text-ysrcp-light">
            {chartType === 'sentiment'
              ? `${(data.reduce((acc, d) => acc + d.ysrcp, 0) / data.length).toFixed(1)}%`
              : formatCompactNumber(data.reduce((acc, d) => acc + d.ysrcp, 0) / data.length)}
          </p>
        </div>
        <div className="text-center">
          <p className="text-sm text-dashboard-textMuted mb-1">TDP Weekly Avg</p>
          <p className="text-xl font-bold text-tdp-primary">
            {chartType === 'sentiment'
              ? `${(data.reduce((acc, d) => acc + d.tdp, 0) / data.length).toFixed(1)}%`
              : formatCompactNumber(data.reduce((acc, d) => acc + d.tdp, 0) / data.length)}
          </p>
        </div>
      </div>
    </div>
  );
};

export default EngagementChart;
