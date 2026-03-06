/**
 * API Service for YSRCP Dashboard
 * Connects to the Python FastAPI backend
 */

// Use environment variable for API URL, or Render backend URL in production
// Local development uses localhost:8000, production calls Render directly (CORS enabled)
const API_BASE_URL = import.meta.env.VITE_API_URL ||
  (import.meta.env.PROD ? 'https://ysrcp-dashboard.onrender.com/api' : 'http://127.0.0.1:8000/api');

// Helper function for API calls
async function fetchAPI(endpoint, options = {}) {
  try {
    const response = await fetch(`${API_BASE_URL}${endpoint}`, {
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
      ...options,
    });

    if (!response.ok) {
      throw new Error(`API Error: ${response.status} ${response.statusText}`);
    }

    return await response.json();
  } catch (error) {
    console.error(`API call failed for ${endpoint}:`, error);
    throw error;
  }
}

// API Service object
export const apiService = {
  /**
   * Get complete dashboard data in one call
   */
  async getDashboardData() {
    return fetchAPI('/dashboard');
  },

  /**
   * Get overall statistics for both parties
   */
  async getOverallStats() {
    return fetchAPI('/stats/overall');
  },

  /**
   * Get platform-wise statistics
   */
  async getPlatformStats() {
    return fetchAPI('/stats/platforms');
  },

  /**
   * Get Google Trends data
   */
  async getGoogleTrends() {
    return fetchAPI('/trends/google');
  },

  /**
   * Get regional search interest
   */
  async getRegionalTrends() {
    return fetchAPI('/trends/regional');
  },

  /**
   * Get related search queries
   */
  async getRelatedQueries() {
    return fetchAPI('/trends/queries');
  },

  /**
   * Get trending hashtags
   */
  async getTrendingHashtags() {
    return fetchAPI('/hashtags');
  },

  /**
   * Get news articles
   */
  async getNews() {
    return fetchAPI('/news');
  },

  /**
   * Get news statistics
   */
  async getNewsStats() {
    return fetchAPI('/news/stats');
  },

  /**
   * Get sentiment analysis
   */
  async getSentiment() {
    return fetchAPI('/sentiment');
  },

  /**
   * Analyze custom text for sentiment
   */
  async analyzeText(text) {
    return fetchAPI('/sentiment/analyze', {
      method: 'POST',
      body: JSON.stringify({ text }),
    });
  },

  /**
   * Get alerts
   */
  async getAlerts() {
    return fetchAPI('/alerts');
  },

  /**
   * Health check
   */
  async healthCheck() {
    return fetchAPI('/health');
  },

  /**
   * Refresh all data sources - clears caches and fetches fresh data
   * from Twitter, Instagram, Facebook, and YouTube
   */
  async refreshAllSources() {
    return fetchAPI('/refresh', {
      method: 'POST',
    });
  },

  /**
   * Get real-time Twitter stats (follower counts, etc.)
   */
  async getTwitterStats() {
    return fetchAPI('/twitter/stats');
  },

  /**
   * Get Twitter user profile
   */
  async getTwitterUser(username) {
    return fetchAPI(`/twitter/user/${username}`);
  },

  /**
   * Get Instagram trending posts
   */
  async getInstagramTrending(party = 'all') {
    return fetchAPI(`/instagram/trending?party=${party}`);
  },

  /**
   * Get Instagram stats for both parties
   */
  async getInstagramStats() {
    return fetchAPI('/instagram/stats');
  },

  /**
   * Get Instagram posts for a user
   */
  async getInstagramUser(username) {
    return fetchAPI(`/instagram/user/${username}`);
  },

  /**
   * Get Facebook trending posts
   */
  async getFacebookTrending(party = 'all') {
    return fetchAPI(`/facebook/trending?party=${party}`);
  },

  /**
   * Get Facebook stats for both parties
   */
  async getFacebookStats() {
    return fetchAPI('/facebook/stats');
  },

  /**
   * Get Facebook page posts
   */
  async getFacebookPage(profileId) {
    return fetchAPI(`/facebook/page/${profileId}`);
  },
};

export default apiService;
