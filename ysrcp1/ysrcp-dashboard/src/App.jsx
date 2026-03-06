import { DashboardProvider } from './context/DashboardContext';
import Header from './components/Header';
import SentimentBattleMeter from './components/SentimentBattleMeter';
import PlatformTabs from './components/PlatformTabs';
import TrendingHashtags from './components/TrendingHashtags';
import AlertsPanel from './components/AlertsPanel';
import EngagementChart from './components/EngagementChart';
import RegionalSentiment from './components/RegionalSentiment';
import TopPosts from './components/TopPosts';
import InfluencerTracker from './components/InfluencerTracker';
import TopicPerformance from './components/TopicPerformance';
import WordCloud from './components/WordCloud';
import YouTubeTrending from './components/YouTubeTrending';
import TwitterTrending from './components/TwitterTrending';
import InstagramTrending from './components/InstagramTrending';
import FacebookTrending from './components/FacebookTrending';

function Dashboard() {
  return (
    <div className="min-h-screen bg-dashboard-bg">
      {/* Header */}
      <Header />

      {/* Main Content */}
      <main className="max-w-[1920px] mx-auto px-4 lg:px-6 py-6">
        {/* Hero Section - Sentiment Battle */}
        <section className="mb-6 animate-fade-in">
          <SentimentBattleMeter />
        </section>

        {/* Main Grid - Platform Analytics & Trending/Alerts */}
        <section className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-6">
          {/* Platform Analytics - Takes 2 columns */}
          <div className="lg:col-span-2 animate-fade-in" style={{ animationDelay: '0.2s' }}>
            <PlatformTabs />
          </div>

          {/* Sidebar - Trending & Alerts */}
          <div className="space-y-6">
            <div className="animate-fade-in" style={{ animationDelay: '0.3s' }}>
              <TrendingHashtags />
            </div>
          </div>
        </section>

        {/* Charts Section */}
        <section className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
          <div className="animate-fade-in" style={{ animationDelay: '0.4s' }}>
            <EngagementChart />
          </div>
          <div className="animate-fade-in" style={{ animationDelay: '0.5s' }}>
            <TopicPerformance />
          </div>
        </section>

        {/* YouTube Trending Section */}
        <section className="mb-6 animate-fade-in" style={{ animationDelay: '0.57s' }}>
          <YouTubeTrending />
        </section>

        {/* Twitter Trending Section */}
        <section className="mb-6 animate-fade-in" style={{ animationDelay: '0.58s' }}>
          <TwitterTrending />
        </section>

        {/* Instagram Trending Section */}
        <section className="mb-6 animate-fade-in" style={{ animationDelay: '0.59s' }}>
          <InstagramTrending />
        </section>

        {/* Facebook Trending Section */}
        <section className="mb-6 animate-fade-in" style={{ animationDelay: '0.6s' }}>
          <FacebookTrending />
        </section>

        {/* Regional Analysis & Alerts */}
        <section className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-6">
          <div className="lg:col-span-2 animate-fade-in" style={{ animationDelay: '0.6s' }}>
            <RegionalSentiment />
          </div>
          <div className="animate-fade-in" style={{ animationDelay: '0.7s' }}>
            <AlertsPanel />
          </div>
        </section>

        {/* Top Posts */}
        <section className="mb-6 animate-fade-in" style={{ animationDelay: '0.8s' }}>
          <TopPosts />
        </section>

        {/* Influencer Tracker & Word Cloud */}
        <section className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-6">
          <div className="lg:col-span-2 animate-fade-in" style={{ animationDelay: '0.9s' }}>
            <InfluencerTracker />
          </div>
          <div className="animate-fade-in" style={{ animationDelay: '1s' }}>
            <WordCloud />
          </div>
        </section>

        {/* Footer */}
        <footer className="text-center py-6 border-t border-dashboard-border">
          <p className="text-sm text-dashboard-textMuted">
            YSRCP Social Intelligence Dashboard | Real-time political analytics
          </p>
          <p className="text-xs text-dashboard-textMuted mt-1">
            Data refreshes every 5 minutes | Last updated: {new Date().toLocaleTimeString('en-IN')}
          </p>
        </footer>
      </main>
    </div>
  );
}

function App() {
  return (
    <DashboardProvider>
      <Dashboard />
    </DashboardProvider>
  );
}

export default App;
