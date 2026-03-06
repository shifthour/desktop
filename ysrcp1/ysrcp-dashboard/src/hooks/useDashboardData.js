/**
 * Custom hook for fetching dashboard data
 * Provides loading, error states and manual refresh only
 * NO AUTO-REFRESH - Data only refreshes when refresh button is clicked
 */

import { useState, useEffect, useCallback } from 'react';
import apiService from '../services/api';

export function useDashboardData() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [isLive, setIsLive] = useState(false);

  const fetchData = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);

      const dashboardData = await apiService.getDashboardData();
      setData(dashboardData);
      setLastUpdated(new Date());
      setIsLive(true);

    } catch (err) {
      console.error('Failed to fetch dashboard data:', err);
      setError(err.message);
      setIsLive(false);
    } finally {
      setLoading(false);
    }
  }, []);

  // Initial fetch only - NO auto-refresh
  useEffect(() => {
    fetchData();
  }, [fetchData]);

  // Manual refresh function - only way to refresh data
  const refresh = useCallback(() => {
    fetchData();
  }, [fetchData]);

  return {
    data,
    loading,
    error,
    lastUpdated,
    isLive,
    refresh,
  };
}

/**
 * Hook for fetching Google Trends data
 */
export function useGoogleTrends() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchTrends = async () => {
      try {
        setLoading(true);
        const trendsData = await apiService.getGoogleTrends();
        setData(trendsData);
      } catch (err) {
        console.error('Failed to fetch Google Trends:', err);
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchTrends();
  }, []);

  return { data, loading, error };
}

/**
 * Hook for fetching news data
 */
export function useNews() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchNews = async () => {
      try {
        setLoading(true);
        const newsData = await apiService.getNews();
        setData(newsData);
      } catch (err) {
        console.error('Failed to fetch news:', err);
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchNews();
  }, []);

  return { data, loading, error };
}

/**
 * Hook for fetching platform stats
 */
export function usePlatformStats() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchStats = async () => {
      try {
        setLoading(true);
        const statsData = await apiService.getPlatformStats();
        setData(statsData);
      } catch (err) {
        console.error('Failed to fetch platform stats:', err);
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchStats();
  }, []);

  return { data, loading, error };
}

export default useDashboardData;
