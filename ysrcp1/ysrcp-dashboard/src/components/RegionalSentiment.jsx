import { MapPin, TrendingUp, TrendingDown, Loader2 } from 'lucide-react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell } from 'recharts';
import { useDashboard } from '../context/DashboardContext';
import { formatNumber } from '../utils/formatters';

const CustomTooltip = ({ active, payload, label }) => {
  if (active && payload && payload.length) {
    const data = payload[0].payload;
    return (
      <div className="bg-dashboard-card border border-dashboard-border rounded-lg p-3 shadow-xl">
        <p className="text-sm font-semibold text-dashboard-text mb-2">{data.district}</p>
        <div className="space-y-1 text-sm">
          <div className="flex items-center justify-between gap-4">
            <span className="text-ysrcp-light">YSRCP:</span>
            <span className="font-medium text-dashboard-text">{data.ysrcp}%</span>
          </div>
          <div className="flex items-center justify-between gap-4">
            <span className="text-tdp-primary">TDP:</span>
            <span className="font-medium text-dashboard-text">{data.tdp}%</span>
          </div>
          <div className="flex items-center justify-between gap-4 pt-1 border-t border-dashboard-border">
            <span className="text-dashboard-textMuted">Mentions:</span>
            <span className="font-medium text-dashboard-text">{formatNumber(data.mentions)}</span>
          </div>
        </div>
      </div>
    );
  }
  return null;
};

const RegionalSentiment = () => {
  const { googleTrends, loading } = useDashboard();

  // Use regional data from Google Trends
  const regionalData = googleTrends?.regional || [];

  // Show loading state if no data
  if (loading || regionalData.length === 0) {
    return (
      <div className="card p-6">
        <div className="flex items-center gap-2 mb-6">
          <MapPin size={20} className="text-ysrcp-light" />
          <h2 className="text-lg font-semibold text-dashboard-text">Regional Sentiment Analysis</h2>
        </div>
        <div className="flex items-center justify-center h-[300px]">
          <Loader2 className="w-8 h-8 animate-spin text-dashboard-textMuted" />
        </div>
      </div>
    );
  }

  const sortedData = [...regionalData].sort((a, b) => b.ysrcp - a.ysrcp);
  const topDistricts = sortedData.slice(0, 5);
  const weakDistricts = sortedData.slice(-3).reverse();

  return (
    <div className="card p-6">
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-2">
          <MapPin size={20} className="text-ysrcp-light" />
          <h2 className="text-lg font-semibold text-dashboard-text">Regional Sentiment Analysis</h2>
        </div>
        <span className="text-xs text-dashboard-textMuted">Andhra Pradesh Districts</span>
      </div>

      {/* Chart */}
      <div className="h-[300px] mb-6">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={regionalData}
            layout="vertical"
            margin={{ top: 5, right: 30, left: 80, bottom: 5 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#2a2a4a" horizontal={true} vertical={false} />
            <XAxis
              type="number"
              domain={[0, 100]}
              axisLine={false}
              tickLine={false}
              tick={{ fill: '#9ca3af', fontSize: 12 }}
              tickFormatter={(v) => `${v}%`}
            />
            <YAxis
              type="category"
              dataKey="district"
              axisLine={false}
              tickLine={false}
              tick={{ fill: '#9ca3af', fontSize: 11 }}
              width={75}
            />
            <Tooltip content={<CustomTooltip />} cursor={{ fill: 'rgba(255,255,255,0.05)' }} />
            <Bar dataKey="ysrcp" name="YSRCP" radius={[0, 4, 4, 0]} barSize={16}>
              {regionalData.map((entry, index) => (
                <Cell
                  key={`cell-${index}`}
                  fill={entry.ysrcp >= 60 ? '#0066CC' : entry.ysrcp >= 50 ? '#3399FF' : '#6B7280'}
                />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Quick Insights */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 pt-4 border-t border-dashboard-border">
        {/* Strong Districts */}
        <div className="p-4 rounded-lg bg-sentiment-positive/5 border border-sentiment-positive/20">
          <div className="flex items-center gap-2 mb-3">
            <TrendingUp size={16} className="text-sentiment-positive" />
            <h4 className="font-semibold text-sentiment-positive text-sm">Strongest Districts</h4>
          </div>
          <div className="space-y-2">
            {topDistricts.map((district, index) => (
              <div key={district.district} className="flex items-center justify-between text-sm">
                <span className="text-dashboard-text">{district.district}</span>
                <span className="font-semibold text-ysrcp-light">{district.ysrcp}%</span>
              </div>
            ))}
          </div>
        </div>

        {/* Weak Districts */}
        <div className="p-4 rounded-lg bg-amber-500/5 border border-amber-500/20">
          <div className="flex items-center gap-2 mb-3">
            <TrendingDown size={16} className="text-amber-500" />
            <h4 className="font-semibold text-amber-500 text-sm">Focus Areas</h4>
          </div>
          <div className="space-y-2">
            {weakDistricts.map((district, index) => (
              <div key={district.district} className="flex items-center justify-between text-sm">
                <span className="text-dashboard-text">{district.district}</span>
                <span className="font-semibold text-tdp-primary">{district.tdp}%</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
};

export default RegionalSentiment;
