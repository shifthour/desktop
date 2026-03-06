import { AlertTriangle, Info, CheckCircle, Clock, Twitter, Globe, Loader2 } from 'lucide-react';
import { useDashboard } from '../context/DashboardContext';

const AlertsPanel = () => {
  const { alerts, loading } = useDashboard();

  // Show loading state if no data
  if (loading || !alerts || alerts.length === 0) {
    return (
      <div className="card p-6 h-full">
        <div className="flex items-center gap-2 mb-4">
          <AlertTriangle size={20} className="text-amber-500" />
          <h2 className="text-lg font-semibold text-dashboard-text">Alerts & Notifications</h2>
        </div>
        <div className="flex items-center justify-center h-[200px]">
          <Loader2 className="w-8 h-8 animate-spin text-dashboard-textMuted" />
        </div>
      </div>
    );
  }
  const getAlertIcon = (type) => {
    switch (type) {
      case 'warning':
        return <AlertTriangle size={18} className="text-amber-500" />;
      case 'success':
        return <CheckCircle size={18} className="text-sentiment-positive" />;
      case 'error':
        return <AlertTriangle size={18} className="text-sentiment-negative" />;
      default:
        return <Info size={18} className="text-ysrcp-light" />;
    }
  };

  const getAlertStyle = (type) => {
    switch (type) {
      case 'warning':
        return 'border-l-amber-500 bg-amber-500/5';
      case 'success':
        return 'border-l-sentiment-positive bg-sentiment-positive/5';
      case 'error':
        return 'border-l-sentiment-negative bg-sentiment-negative/5';
      default:
        return 'border-l-ysrcp-primary bg-ysrcp-primary/5';
    }
  };

  const getPriorityBadge = (priority) => {
    switch (priority) {
      case 'high':
        return 'bg-sentiment-negative/20 text-sentiment-negative';
      case 'medium':
        return 'bg-amber-500/20 text-amber-500';
      default:
        return 'bg-sentiment-neutral/20 text-sentiment-neutral';
    }
  };

  return (
    <div className="card p-6 h-full">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <AlertTriangle size={20} className="text-amber-500" />
          <h2 className="text-lg font-semibold text-dashboard-text">Alerts & Notifications</h2>
        </div>
        <span className="px-2 py-1 rounded-full bg-sentiment-negative/20 text-sentiment-negative text-xs font-semibold">
          {alerts.filter(a => a.priority === 'high').length} High Priority
        </span>
      </div>

      <div className="space-y-3 max-h-[400px] overflow-y-auto pr-2">
        {alerts.map((alert) => (
          <div
            key={alert.id}
            className={`p-4 rounded-lg border-l-4 ${getAlertStyle(alert.type)} hover:bg-dashboard-cardHover transition-colors cursor-pointer`}
          >
            <div className="flex items-start gap-3">
              <div className="mt-0.5">
                {getAlertIcon(alert.type)}
              </div>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 mb-1">
                  <h4 className="font-semibold text-dashboard-text truncate">
                    {alert.title}
                  </h4>
                  <span className={`px-2 py-0.5 rounded text-xs font-medium ${getPriorityBadge(alert.priority)}`}>
                    {alert.priority}
                  </span>
                </div>
                <p className="text-sm text-dashboard-textMuted mb-2">
                  {alert.message}
                </p>
                <div className="flex items-center gap-3 text-xs text-dashboard-textMuted">
                  <div className="flex items-center gap-1">
                    <Clock size={12} />
                    <span>{alert.time}</span>
                  </div>
                  <div className="flex items-center gap-1">
                    {alert.platform === 'twitter' ? (
                      <Twitter size={12} />
                    ) : (
                      <Globe size={12} />
                    )}
                    <span className="capitalize">{alert.platform}</span>
                  </div>
                </div>
              </div>
            </div>
          </div>
        ))}
      </div>

      <div className="mt-4 pt-4 border-t border-dashboard-border">
        <button className="w-full py-2 text-center text-sm text-ysrcp-light hover:text-ysrcp-primary transition-colors">
          View All Alerts →
        </button>
      </div>
    </div>
  );
};

export default AlertsPanel;
