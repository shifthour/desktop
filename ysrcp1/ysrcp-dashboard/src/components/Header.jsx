import { RefreshCw, Calendar, Wifi, WifiOff } from 'lucide-react';
import { useDashboard } from '../context/DashboardContext';

const Header = () => {
  const { isLive, apiStatus, lastUpdated, refresh, loading } = useDashboard();

  const currentDate = new Date().toLocaleDateString('en-IN', {
    weekday: 'long',
    year: 'numeric',
    month: 'long',
    day: 'numeric'
  });

  const currentTime = new Date().toLocaleTimeString('en-IN', {
    hour: '2-digit',
    minute: '2-digit'
  });

  const lastUpdateTime = lastUpdated
    ? lastUpdated.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' })
    : currentTime;

  return (
    <header className="bg-dashboard-card border-b border-dashboard-border sticky top-0 z-50">
      <div className="max-w-[1920px] mx-auto px-4 lg:px-6 py-4">
        <div className="flex items-center justify-between">
          {/* Logo & Title */}
          <div className="flex items-center gap-4">
            <div className="w-12 h-12 rounded-xl overflow-hidden border-2 border-ysrcp-primary glow-ysrcp">
              <img
                src="/ys-jagan.png"
                alt="YS Jagan Mohan Reddy"
                className="w-full h-full object-contain bg-white"
                onError={(e) => {
                  e.target.style.display = 'none';
                  e.target.parentElement.innerHTML = '<span class="text-white font-bold text-lg flex items-center justify-center w-full h-full gradient-ysrcp">YS</span>';
                }}
              />
            </div>
            <div>
              <h1 className="text-xl lg:text-2xl font-bold text-dashboard-text">
                YSRCP <span className="text-ysrcp-light">Social Intelligence</span>
              </h1>
              <p className="text-sm text-dashboard-textMuted hidden sm:block">
                Real-time Political Dashboard
              </p>
            </div>
          </div>

          {/* Live Status & Date */}
          <div className="hidden md:flex items-center gap-6">
            {/* API Status Indicator */}
            {isLive ? (
              <div className="flex items-center gap-2 text-sentiment-positive text-sm font-medium">
                <Wifi size={16} />
                <span className="relative flex items-center gap-2">
                  <span className="absolute -left-1 w-2 h-2 bg-sentiment-positive rounded-full animate-pulse" />
                  <span className="ml-2">Live</span>
                </span>
              </div>
            ) : (
              <div className="flex items-center gap-2 text-amber-500 text-sm font-medium">
                <WifiOff size={16} />
                <span>Offline Mode</span>
              </div>
            )}

            <div className="flex items-center gap-2 text-dashboard-textMuted">
              <Calendar size={16} />
              <span className="text-sm">{currentDate}</span>
              <span className="text-sm font-medium text-dashboard-text ml-2">{lastUpdateTime}</span>
            </div>
          </div>

          {/* Actions */}
          <div className="flex items-center gap-2">
            {/* Refresh Button */}
            <button
              onClick={refresh}
              disabled={loading}
              className="flex items-center gap-2 px-3 py-2 rounded-lg bg-dashboard-card hover:bg-dashboard-cardHover border border-dashboard-border transition-all disabled:opacity-50 hover:border-ysrcp-primary/50"
              title="Refresh data"
            >
              <RefreshCw
                size={18}
                className={`text-dashboard-textMuted ${loading ? 'animate-spin text-ysrcp-light' : ''}`}
              />
              <span className="hidden sm:inline text-sm text-dashboard-textMuted">
                {loading ? 'Refreshing...' : 'Refresh'}
              </span>
            </button>

            {/* Mobile Live Indicator */}
            <div className="md:hidden">
              {isLive ? (
                <div className="w-3 h-3 bg-sentiment-positive rounded-full animate-pulse" title="Live" />
              ) : (
                <div className="w-3 h-3 bg-amber-500 rounded-full" title="Offline" />
              )}
            </div>
          </div>
        </div>
      </div>
    </header>
  );
};

export default Header;
