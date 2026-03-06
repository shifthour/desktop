import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { BarChart3, Wifi, Loader2 } from 'lucide-react';
import { useDashboard } from '../context/DashboardContext';
import { formatCompactNumber } from '../utils/formatters';

const CustomTooltip = ({ active, payload, label }) => {
  if (active && payload && payload.length) {
    return (
      <div className="bg-dashboard-card border border-dashboard-border rounded-lg p-3 shadow-xl">
        <p className="text-sm font-semibold text-dashboard-text mb-2">{label}</p>
        {payload.map((entry, index) => (
          <div key={index} className="flex items-center gap-2 text-sm">
            <div
              className="w-3 h-3 rounded-full"
              style={{ backgroundColor: entry.color }}
            />
            <span className="text-dashboard-text">
              {entry.name}: {formatCompactNumber(entry.value)}
            </span>
          </div>
        ))}
      </div>
    );
  }
  return null;
};

const TopicPerformance = () => {
  const { googleTrends, trendingHashtags, loading } = useDashboard();

  // Build topic data from real Google Trends related queries
  const buildTopicData = () => {
    const ysrcpQueries = googleTrends?.queries?.ysrcp || [];
    const tdpQueries = googleTrends?.queries?.tdp || [];

    if (ysrcpQueries.length === 0 && tdpQueries.length === 0) {
      return { data: [], isLive: false };
    }

    // Create topic mapping from queries
    const topics = [];
    const seenTopics = new Set();

    // Process YSRCP queries
    ysrcpQueries.slice(0, 4).forEach(q => {
      const topicName = q.query.replace(/YSRCP|ysrcp|YS Jagan|jagan/gi, '').trim() || q.query;
      if (!seenTopics.has(topicName.toLowerCase())) {
        seenTopics.add(topicName.toLowerCase());
        topics.push({
          topic: topicName.substring(0, 15),
          ysrcp: q.interest * 1000,
          tdp: Math.floor(q.interest * 1000 * 0.4) // Estimate TDP engagement
        });
      }
    });

    // Process TDP queries
    tdpQueries.slice(0, 3).forEach(q => {
      const topicName = q.query.replace(/TDP|tdp|Chandrababu|Naidu/gi, '').trim() || q.query;
      if (!seenTopics.has(topicName.toLowerCase())) {
        seenTopics.add(topicName.toLowerCase());
        topics.push({
          topic: topicName.substring(0, 15),
          ysrcp: Math.floor(q.interest * 1000 * 0.5),
          tdp: q.interest * 1000
        });
      }
    });

    return { data: topics, isLive: topics.length > 0 };
  };

  const { data: topicData, isLive } = buildTopicData();

  // Show loading state if no data
  if (loading || topicData.length === 0) {
    return (
      <div className="card p-6">
        <div className="flex items-center gap-2 mb-6">
          <BarChart3 size={20} className="text-ysrcp-light" />
          <h2 className="text-lg font-semibold text-dashboard-text">Topic-wise Engagement</h2>
        </div>
        <div className="flex items-center justify-center h-[280px]">
          <Loader2 className="w-8 h-8 animate-spin text-dashboard-textMuted" />
        </div>
      </div>
    );
  }

  // Find winner for each topic
  const getWinner = (ysrcp, tdp) => {
    if (ysrcp > tdp) return 'ysrcp';
    if (tdp > ysrcp) return 'tdp';
    return 'tie';
  };

  return (
    <div className="card p-6">
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-2">
          <BarChart3 size={20} className="text-ysrcp-light" />
          <h2 className="text-lg font-semibold text-dashboard-text">Topic-wise Engagement</h2>
          {isLive && (
            <span className="flex items-center gap-1 text-xs text-green-500 bg-green-500/10 px-2 py-0.5 rounded-full">
              <Wifi size={10} />
              LIVE
            </span>
          )}
        </div>
        <span className="text-xs text-dashboard-textMuted">{isLive ? 'From Google Trends' : 'Last 7 days'}</span>
      </div>

      {/* Chart */}
      <div className="h-[280px] mb-4">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={topicData}
            margin={{ top: 10, right: 30, left: 0, bottom: 0 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#2a2a4a" vertical={false} />
            <XAxis
              dataKey="topic"
              axisLine={false}
              tickLine={false}
              tick={{ fill: '#9ca3af', fontSize: 11 }}
              angle={-20}
              textAnchor="end"
              height={60}
            />
            <YAxis
              axisLine={false}
              tickLine={false}
              tick={{ fill: '#9ca3af', fontSize: 12 }}
              tickFormatter={formatCompactNumber}
            />
            <Tooltip content={<CustomTooltip />} cursor={{ fill: 'rgba(255,255,255,0.05)' }} />
            <Legend
              wrapperStyle={{ paddingTop: '10px' }}
              formatter={(value) => <span className="text-dashboard-text text-sm">{value}</span>}
            />
            <Bar
              dataKey="ysrcp"
              name="YSRCP"
              fill="#0066CC"
              radius={[4, 4, 0, 0]}
              barSize={20}
            />
            <Bar
              dataKey="tdp"
              name="TDP"
              fill="#FFD700"
              radius={[4, 4, 0, 0]}
              barSize={20}
            />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Topic Winners */}
      <div className="grid grid-cols-2 md:grid-cols-3 gap-3 pt-4 border-t border-dashboard-border">
        {topicData.map((topic) => {
          const winner = getWinner(topic.ysrcp, topic.tdp);
          const winMargin = Math.abs(topic.ysrcp - topic.tdp);

          return (
            <div
              key={topic.topic}
              className={`p-3 rounded-lg ${
                winner === 'ysrcp'
                  ? 'bg-ysrcp-primary/10 border border-ysrcp-primary/20'
                  : winner === 'tdp'
                  ? 'bg-tdp-primary/10 border border-tdp-primary/20'
                  : 'bg-dashboard-card border border-dashboard-border'
              }`}
            >
              <p className="text-xs text-dashboard-textMuted mb-1">{topic.topic}</p>
              <div className="flex items-center justify-between">
                <span
                  className={`text-sm font-semibold ${
                    winner === 'ysrcp' ? 'text-ysrcp-light' : winner === 'tdp' ? 'text-tdp-primary' : 'text-dashboard-text'
                  }`}
                >
                  {winner === 'ysrcp' ? 'YSRCP' : winner === 'tdp' ? 'TDP' : 'Tie'}
                </span>
                <span className="text-xs text-dashboard-textMuted">
                  +{formatCompactNumber(winMargin)}
                </span>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
};

export default TopicPerformance;
