/**
 * Dashboard Context
 * Provides real-time data from API to all components
 * NO MOCK DATA - All data comes from live APIs
 */

import { createContext, useContext, useState, useEffect, useCallback } from 'react';
import apiService from '../services/api';

const DashboardContext = createContext(null);

export function DashboardProvider({ children }) {
  const [data, setData] = useState({
    overallStats: null,
    platformStats: null,
    trendingHashtags: null,
    googleTrends: null,
    youtube: null,
    twitter: null,
    twitterStats: null,
    instagram: null,
    facebook: null,
    influencers: null,
    sentiment: null,
    sentimentBattle: null,
    news: null,
    alerts: null,
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [isLive, setIsLive] = useState(false);
  const [apiStatus, setApiStatus] = useState('checking');

  // Check API health
  const checkApiHealth = useCallback(async () => {
    try {
      await apiService.healthCheck();
      setApiStatus('online');
      return true;
    } catch (err) {
      setApiStatus('offline');
      return false;
    }
  }, []);

  // Fetch data from API
  const fetchData = useCallback(async () => {
    try {
      const isApiOnline = await checkApiHealth();

      if (!isApiOnline) {
        console.log('API offline - no data available');
        setIsLive(false);
        setLoading(false);
        setError('API is offline. Please check your backend server.');
        return;
      }

      setLoading(true);

      // Fetch dashboard data and Twitter stats in parallel
      const [dashboardData, twitterStats] = await Promise.all([
        apiService.getDashboardData(),
        apiService.getTwitterStats().catch(() => null)
      ]);

      // Update state with real data only
      setData({
        overallStats: dashboardData.overallStats || null,
        platformStats: dashboardData.platformStats || null,
        trendingHashtags: dashboardData.trendingHashtags || null,
        googleTrends: dashboardData.googleTrends || null,
        youtube: dashboardData.youtube || null,
        twitter: dashboardData.twitter || null,
        twitterStats: twitterStats,
        instagram: dashboardData.instagram || null,
        facebook: dashboardData.facebook || null,
        influencers: dashboardData.influencers || null,
        sentiment: dashboardData.sentiment || null,
        sentimentBattle: dashboardData.sentimentBattle || null,
        news: dashboardData.news || null,
        alerts: dashboardData.alerts || null,
      });

      setLastUpdated(new Date());
      setIsLive(true);
      setError(null);

    } catch (err) {
      console.error('Failed to fetch data:', err);
      setError(err.message);
      setIsLive(false);
    } finally {
      setLoading(false);
    }
  }, [checkApiHealth]);

  // Initial fetch only - NO auto-refresh
  useEffect(() => {
    fetchData();
  }, [fetchData]);

  // Manual refresh - clears caches and fetches fresh data from all 4 sources
  const refresh = useCallback(async () => {
    try {
      setLoading(true);

      // First, call the backend to clear caches and fetch fresh data from all sources
      console.log('Refreshing all data sources (Twitter, Instagram, Facebook, YouTube)...');
      const refreshResult = await apiService.refreshAllSources();
      console.log('Refresh result:', refreshResult);

      // Then fetch the updated dashboard data
      await fetchData();
    } catch (error) {
      console.error('Error refreshing data sources:', error);
      setError('Failed to refresh data: ' + error.message);
      setLoading(false);
    }
  }, [fetchData]);

  // Calculate sentiment scores from real data
  const sentimentScore = data.sentimentBattle
    ? {
        ysrcp: data.sentimentBattle.ysrcp?.score || 0,
        tdp: data.sentimentBattle.tdp?.score || 0,
      }
    : data.sentiment
    ? {
        ysrcp: data.sentiment.ysrcp?.score || 0,
        tdp: data.sentiment.tdp?.score || 0,
      }
    : { ysrcp: 0, tdp: 0 };

  const value = {
    // Data
    overallStats: data.overallStats,
    platformStats: data.platformStats,
    trendingHashtags: data.trendingHashtags,
    googleTrends: data.googleTrends,
    youtube: data.youtube,
    twitter: data.twitter,
    twitterStats: data.twitterStats,
    instagram: data.instagram,
    facebook: data.facebook,
    influencers: data.influencers,
    sentiment: data.sentiment,
    sentimentBattle: data.sentimentBattle,
    sentimentScore: sentimentScore,
    news: data.news,
    alerts: data.alerts,

    // Status
    loading,
    error,
    lastUpdated,
    isLive,
    apiStatus,

    // Actions
    refresh,
  };

  return (
    <DashboardContext.Provider value={value}>
      {children}
    </DashboardContext.Provider>
  );
}

export function useDashboard() {
  const context = useContext(DashboardContext);
  if (!context) {
    throw new Error('useDashboard must be used within a DashboardProvider');
  }
  return context;
}

export default DashboardContext;
