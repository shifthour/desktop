import { useState } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar, Cell } from 'recharts';
import { TrendingUp, Search, MapPin, Flame, ExternalLink, ArrowUpRight, ArrowDownRight, Loader2 } from 'lucide-react';
import { useDashboard } from '../context/DashboardContext';

const CustomTooltip = ({ active, payload, label }) => {
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
              {entry.name}: {entry.value}
            </span>
          </div>
        ))}
      </div>
    );
  }
  return null;
};

const GoogleTrends = () => {
  const [activeTab, setActiveTab] = useState('overview');
  const { googleTrends, loading, isLive } = useDashboard();

  // Show loading state if no data
  if (loading || !googleTrends) {
    return (
      <div className="card p-6">
        <div className="flex items-center gap-3 mb-6">
          <div className="p-2 rounded-lg bg-[#4285F4]/10">
            <svg className="w-6 h-6" viewBox="0 0 24 24" fill="none">
              <path d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z" fill="#4285F4"/>
              <path d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z" fill="#34A853"/>
              <path d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z" fill="#FBBC05"/>
              <path d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z" fill="#EA4335"/>
            </svg>
          </div>
          <div>
            <h2 className="text-lg font-semibold text-dashboard-text">Google Trends</h2>
            <p className="text-xs text-dashboard-textMuted">Search interest in Andhra Pradesh</p>
          </div>
        </div>
        <div className="flex items-center justify-center h-[300px]">
          <Loader2 className="w-8 h-8 animate-spin text-dashboard-textMuted" />
        </div>
      </div>
    );
  }

  // Extract data from API response
  const searchInterest = googleTrends?.interest?.searchInterest || { ysrcp: 0, tdp: 0, trend: '0%' };
  const searchTimeline = googleTrends?.interest?.searchTimeline || [];
  const regionalInterest = googleTrends?.regional || [];
  const relatedQueries = googleTrends?.queries || { ysrcp: [], tdp: [] };
  const breakoutTopics = googleTrends?.breakout || [];
  const comparisonSearches = googleTrends?.comparison || [];

  return (
    <div className="card p-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-lg bg-[#4285F4]/10">
            <svg className="w-6 h-6" viewBox="0 0 24 24" fill="none">
              <path d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z" fill="#4285F4"/>
              <path d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z" fill="#34A853"/>
              <path d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z" fill="#FBBC05"/>
              <path d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z" fill="#EA4335"/>
            </svg>
          </div>
          <div>
            <h2 className="text-lg font-semibold text-dashboard-text">Google Trends</h2>
            <p className="text-xs text-dashboard-textMuted">Search interest in Andhra Pradesh</p>
          </div>
        </div>
        <div className="flex items-center gap-3">
          {isLive && (
            <span className="flex items-center gap-1 px-2 py-1 rounded-full bg-sentiment-positive/20 text-sentiment-positive text-xs font-medium">
              <span className="w-2 h-2 bg-sentiment-positive rounded-full animate-pulse" />
              LIVE
            </span>
          )}
          <a
            href="https://trends.google.com"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-1 text-xs text-dashboard-textMuted hover:text-ysrcp-light transition-colors"
          >
            <span>View on Google</span>
            <ExternalLink size={12} />
          </a>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex flex-wrap gap-2 mb-6">
        {[
          { id: 'overview', label: 'Overview' },
          { id: 'regional', label: 'Regional' },
          { id: 'queries', label: 'Related Queries' },
          { id: 'breakout', label: 'Breakout' }
        ].map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
              activeTab === tab.id
                ? 'bg-[#4285F4] text-white'
                : 'bg-dashboard-card text-dashboard-textMuted hover:bg-dashboard-cardHover border border-dashboard-border'
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Overview Tab */}
      {activeTab === 'overview' && (
        <div className="space-y-6 animate-fade-in">
          {/* Search Interest Comparison */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {/* YSRCP Interest */}
            <div className="p-4 rounded-xl bg-ysrcp-primary/10 border border-ysrcp-primary/20">
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm text-dashboard-textMuted">YSRCP Interest</span>
                <span className="flex items-center gap-1 text-xs text-sentiment-positive">
                  <ArrowUpRight size={12} />
                  {searchInterest.trend}
                </span>
              </div>
              <div className="flex items-end gap-2">
                <span className="text-4xl font-bold text-ysrcp-light">{searchInterest.ysrcp}</span>
                <span className="text-sm text-dashboard-textMuted mb-1">/100</span>
              </div>
            </div>

            {/* TDP Interest */}
            <div className="p-4 rounded-xl bg-tdp-primary/10 border border-tdp-primary/20">
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm text-dashboard-textMuted">TDP Interest</span>
                <span className="flex items-center gap-1 text-xs text-sentiment-negative">
                  <ArrowDownRight size={12} />
                  -5%
                </span>
              </div>
              <div className="flex items-end gap-2">
                <span className="text-4xl font-bold text-tdp-primary">{searchInterest.tdp}</span>
                <span className="text-sm text-dashboard-textMuted mb-1">/100</span>
              </div>
            </div>

            {/* Lead */}
            <div className="p-4 rounded-xl bg-sentiment-positive/10 border border-sentiment-positive/20">
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm text-dashboard-textMuted">YSRCP Lead</span>
                <TrendingUp size={16} className="text-sentiment-positive" />
              </div>
              <div className="flex items-end gap-2">
                <span className="text-4xl font-bold text-sentiment-positive">+{searchInterest.ysrcp - searchInterest.tdp}</span>
                <span className="text-sm text-dashboard-textMuted mb-1">points</span>
              </div>
            </div>
          </div>

          {/* Timeline Chart */}
          <div>
            <h3 className="text-sm font-medium text-dashboard-textMuted mb-4">30-Day Search Interest Trend</h3>
            <div className="h-[250px]">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={searchTimeline} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#2a2a4a" />
                  <XAxis
                    dataKey="date"
                    axisLine={false}
                    tickLine={false}
                    tick={{ fill: '#9ca3af', fontSize: 11 }}
                  />
                  <YAxis
                    domain={[0, 100]}
                    axisLine={false}
                    tickLine={false}
                    tick={{ fill: '#9ca3af', fontSize: 12 }}
                  />
                  <Tooltip content={<CustomTooltip />} />
                  <Line
                    type="monotone"
                    dataKey="ysrcp"
                    name="YSRCP"
                    stroke="#0066CC"
                    strokeWidth={3}
                    dot={{ fill: '#0066CC', strokeWidth: 2, r: 4 }}
                    activeDot={{ r: 6, fill: '#0066CC', stroke: '#fff', strokeWidth: 2 }}
                  />
                  <Line
                    type="monotone"
                    dataKey="tdp"
                    name="TDP"
                    stroke="#FFD700"
                    strokeWidth={3}
                    dot={{ fill: '#FFD700', strokeWidth: 2, r: 4 }}
                    activeDot={{ r: 6, fill: '#FFD700', stroke: '#fff', strokeWidth: 2 }}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* Topic Comparison */}
          <div>
            <h3 className="text-sm font-medium text-dashboard-textMuted mb-4">Search Interest by Topic</h3>
            <div className="space-y-3">
              {comparisonSearches.map((item) => (
                <div key={item.topic} className="flex items-center gap-4">
                  <span className="w-32 text-sm text-dashboard-textMuted truncate">{item.topic}</span>
                  <div className="flex-1 flex items-center gap-2">
                    <div className="flex-1 h-3 bg-dashboard-border rounded-full overflow-hidden flex">
                      <div
                        className="h-full bg-ysrcp-primary transition-all duration-500"
                        style={{ width: `${item.ysrcp}%` }}
                      />
                    </div>
                    <span className="w-8 text-xs text-ysrcp-light font-medium">{item.ysrcp}</span>
                  </div>
                  <div className="flex-1 flex items-center gap-2">
                    <div className="flex-1 h-3 bg-dashboard-border rounded-full overflow-hidden flex">
                      <div
                        className="h-full bg-tdp-primary transition-all duration-500"
                        style={{ width: `${item.tdp}%` }}
                      />
                    </div>
                    <span className="w-8 text-xs text-tdp-primary font-medium">{item.tdp}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Regional Tab */}
      {activeTab === 'regional' && (
        <div className="animate-fade-in">
          <div className="flex items-center gap-2 mb-4">
            <MapPin size={16} className="text-ysrcp-light" />
            <h3 className="text-sm font-medium text-dashboard-textMuted">Search Interest by District</h3>
          </div>

          <div className="h-[350px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={regionalInterest}
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
                <Bar dataKey="ysrcp" name="YSRCP" fill="#0066CC" radius={[0, 4, 4, 0]} barSize={12} />
                <Bar dataKey="tdp" name="TDP" fill="#FFD700" radius={[0, 4, 4, 0]} barSize={12} />
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* District Summary */}
          <div className="grid grid-cols-2 gap-4 mt-4 pt-4 border-t border-dashboard-border">
            <div className="p-3 rounded-lg bg-ysrcp-primary/10">
              <p className="text-xs text-dashboard-textMuted mb-1">YSRCP Dominates</p>
              <p className="text-sm font-semibold text-ysrcp-light">
                {regionalInterest.filter(r => r.ysrcp > r.tdp).length} of {regionalInterest.length} districts
              </p>
            </div>
            <div className="p-3 rounded-lg bg-tdp-primary/10">
              <p className="text-xs text-dashboard-textMuted mb-1">TDP Leads</p>
              <p className="text-sm font-semibold text-tdp-primary">
                {regionalInterest.filter(r => r.tdp > r.ysrcp).length} of {regionalInterest.length} districts
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Related Queries Tab */}
      {activeTab === 'queries' && (
        <div className="animate-fade-in">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {/* YSRCP Queries */}
            <div className="p-4 rounded-xl bg-ysrcp-primary/5 border border-ysrcp-primary/20">
              <div className="flex items-center gap-2 mb-4">
                <Search size={16} className="text-ysrcp-light" />
                <h4 className="font-semibold text-ysrcp-light">YSRCP Related Searches</h4>
              </div>
              <div className="space-y-3">
                {relatedQueries.ysrcp.map((query, index) => (
                  <div key={query.query} className="flex items-center justify-between p-2 rounded-lg bg-dashboard-bg hover:bg-dashboard-card transition-colors">
                    <div className="flex items-center gap-2">
                      {query.isBreakout && (
                        <Flame size={14} className="text-orange-500" />
                      )}
                      <span className="text-sm text-dashboard-text">{query.query}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <div className="w-16 h-2 bg-dashboard-border rounded-full overflow-hidden">
                        <div
                          className="h-full bg-ysrcp-primary"
                          style={{ width: `${query.interest}%` }}
                        />
                      </div>
                      <span className={`text-xs font-medium ${query.isBreakout ? 'text-orange-500' : 'text-sentiment-positive'}`}>
                        {query.change}
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* TDP Queries */}
            <div className="p-4 rounded-xl bg-tdp-primary/5 border border-tdp-primary/20">
              <div className="flex items-center gap-2 mb-4">
                <Search size={16} className="text-tdp-primary" />
                <h4 className="font-semibold text-tdp-primary">TDP Related Searches</h4>
              </div>
              <div className="space-y-3">
                {relatedQueries.tdp.map((query, index) => (
                  <div key={query.query} className="flex items-center justify-between p-2 rounded-lg bg-dashboard-bg hover:bg-dashboard-card transition-colors">
                    <div className="flex items-center gap-2">
                      {query.isBreakout && (
                        <Flame size={14} className="text-orange-500" />
                      )}
                      <span className="text-sm text-dashboard-text">{query.query}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <div className="w-16 h-2 bg-dashboard-border rounded-full overflow-hidden">
                        <div
                          className="h-full bg-tdp-primary"
                          style={{ width: `${query.interest}%` }}
                        />
                      </div>
                      <span className={`text-xs font-medium ${query.isBreakout ? 'text-orange-500' : 'text-sentiment-positive'}`}>
                        {query.change}
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Breakout Tab */}
      {activeTab === 'breakout' && (
        <div className="animate-fade-in">
          <div className="flex items-center gap-2 mb-4">
            <Flame size={16} className="text-orange-500" />
            <h3 className="text-sm font-medium text-dashboard-textMuted">Breakout Topics (Sudden Spikes)</h3>
          </div>

          <div className="space-y-3">
            {breakoutTopics.map((topic, index) => {
              const partyColor = topic.party === 'ysrcp'
                ? 'border-l-ysrcp-primary bg-ysrcp-primary/5'
                : topic.party === 'tdp'
                ? 'border-l-tdp-primary bg-tdp-primary/5'
                : 'border-l-dashboard-border bg-dashboard-card';

              return (
                <div
                  key={topic.topic}
                  className={`p-4 rounded-lg border-l-4 ${partyColor} hover:bg-dashboard-cardHover transition-colors`}
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                      <span className="text-2xl font-bold text-dashboard-textMuted">#{index + 1}</span>
                      <div>
                        <p className="font-semibold text-dashboard-text">{topic.topic}</p>
                        <p className="text-xs text-dashboard-textMuted capitalize">
                          {topic.party === 'neutral' ? 'General Politics' : `${topic.party.toUpperCase()} Related`}
                        </p>
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      <Flame size={18} className="text-orange-500 animate-pulse" />
                      <span className="text-xl font-bold text-orange-500">{topic.growth}</span>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>

          {/* Breakout Summary */}
          <div className="mt-6 p-4 rounded-xl bg-orange-500/10 border border-orange-500/20">
            <div className="flex items-center gap-2 mb-2">
              <Flame size={16} className="text-orange-500" />
              <span className="font-semibold text-orange-500">Breakout Insight</span>
            </div>
            <p className="text-sm text-dashboard-textMuted">
              YSRCP-related topics are dominating breakout searches with <span className="text-ysrcp-light font-medium">3 out of 5</span> top breakout topics.
              Welfare schemes and education initiatives are driving the most search interest spikes.
            </p>
          </div>
        </div>
      )}

      {/* Footer */}
      <div className="mt-6 pt-4 border-t border-dashboard-border text-center">
        <p className="text-xs text-dashboard-textMuted">
          Data represents relative search interest (0-100 scale) | Updated every 24 hours
        </p>
      </div>
    </div>
  );
};

export default GoogleTrends;
