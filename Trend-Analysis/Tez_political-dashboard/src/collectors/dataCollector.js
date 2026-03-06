import axios from 'axios';
import * as cheerio from 'cheerio';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import YouTubeCollector from './youtubeCollector.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

class DataCollector {
  constructor() {
    this.youtubeCollector = new YouTubeCollector();
    this.dataCache = {
      ysrcp: {
        news: [],
        pros: [],
        cons: [],
        metrics: {}
      },
      tdp: {
        news: [],
        pros: [],
        cons: [],
        metrics: {}
      },
      lastUpdated: null
    };
    
    this.sources = {
      news: [
        'https://www.thehindu.com/news/national/andhra-pradesh/',
        'https://www.deccanchronicle.com/nation/politics',
        'https://www.newindianexpress.com/states/andhra-pradesh',
        'https://timesofindia.indiatimes.com/city/vijayawada'
      ]
    };
    
    this.partyKeywords = {
      ysrcp: ['YSRCP', 'YSR Congress', 'Jagan', 'Jagan Mohan Reddy', 'YS Jagan'],
      tdp: ['TDP', 'Telugu Desam', 'Chandrababu', 'Naidu', 'Chandrababu Naidu', 'CBN']
    };

    this.districts = [
      'Visakhapatnam', 'Vijayawada', 'Guntur', 'Tirupati', 'Kurnool',
      'Anantapur', 'Nellore', 'Kadapa', 'Chittoor', 'Kakinada',
      'Rajahmundry', 'Eluru', 'Ongole'
    ];

    // Simple sentiment keywords
    this.sentimentKeywords = {
      positive: ['success', 'achieve', 'win', 'progress', 'growth', 'improve', 'launch', 'inaugurate', 'development', 'welfare', 'benefit', 'praise', 'honor', 'award'],
      negative: ['fail', 'loss', 'crisis', 'scandal', 'corruption', 'arrest', 'protest', 'clash', 'controversy', 'allegation', 'accuse', 'criticize', 'problem', 'issue']
    };
    
    this.loadCachedData();
  }
  
  async loadCachedData() {
    try {
      const cachePath = path.join(__dirname, '../../data/cache.json');
      const data = await fs.readFile(cachePath, 'utf8');
      this.dataCache = JSON.parse(data);
    } catch (error) {
      console.log('No cached data found, starting fresh');
      this.initializeMockData();
    }
  }
  
  initializeMockData() {
    this.dataCache = {
      ysrcp: {
        news: [
          {
            title: "YSRCP launches new welfare scheme",
            date: new Date().toISOString(),
            sentiment: 'positive',
            source: 'mock'
          },
          {
            title: "Opposition questions YSRCP's budget allocation",
            date: new Date().toISOString(),
            sentiment: 'negative',
            source: 'mock'
          }
        ],
        pros: [
          "Strong welfare programs implementation",
          "Focus on social justice initiatives",
          "Direct benefit transfer schemes",
          "Agricultural support programs",
          "Youth employment initiatives"
        ],
        cons: [
          "High fiscal deficit concerns",
          "Infrastructure development pace",
          "Industrial growth challenges",
          "Opposition coordination issues",
          "Urban development gaps"
        ],
        metrics: {
          popularity: 42,
          sentiment: 0.3,
          mediaPresence: 65,
          socialMedia: 58
        }
      },
      tdp: {
        news: [
          {
            title: "TDP proposes technology-driven governance",
            date: new Date().toISOString(),
            sentiment: 'positive',
            source: 'mock'
          },
          {
            title: "TDP faces internal disagreements on policy",
            date: new Date().toISOString(),
            sentiment: 'negative',
            source: 'mock'
          }
        ],
        pros: [
          "Experience in governance and administration",
          "Strong IT and technology focus",
          "Industrial development track record",
          "International investment attraction",
          "Infrastructure project expertise"
        ],
        cons: [
          "Alliance management challenges",
          "Rural connect concerns",
          "Youth engagement gaps",
          "Welfare scheme implementation",
          "Regional balance issues"
        ],
        metrics: {
          popularity: 45,
          sentiment: 0.4,
          mediaPresence: 70,
          socialMedia: 62
        }
      },
      lastUpdated: new Date().toISOString()
    };
  }
  
  analyzeSentiment(text) {
    const lowerText = text.toLowerCase();
    let positiveCount = 0;
    let negativeCount = 0;

    // Count positive keywords
    for (const keyword of this.sentimentKeywords.positive) {
      if (lowerText.includes(keyword)) {
        positiveCount++;
      }
    }

    // Count negative keywords
    for (const keyword of this.sentimentKeywords.negative) {
      if (lowerText.includes(keyword)) {
        negativeCount++;
      }
    }

    // Determine sentiment
    if (positiveCount > negativeCount) {
      return 'positive';
    } else if (negativeCount > positiveCount) {
      return 'negative';
    } else {
      return 'neutral';
    }
  }

  detectDistrict(text) {
    const lowerText = text.toLowerCase();
    for (const district of this.districts) {
      if (lowerText.includes(district.toLowerCase())) {
        return district;
      }
    }
    return null;
  }

  async scrapeNews() {
    const newsData = { ysrcp: [], tdp: [] };

    // Use Google News RSS feeds for reliable news scraping
    const googleNewsFeeds = [
      'https://news.google.com/rss/search?q=YSRCP+OR+Jagan+OR+YSR+Congress+Andhra+Pradesh&hl=en-IN&gl=IN&ceid=IN:en',
      'https://news.google.com/rss/search?q=TDP+OR+Chandrababu+Naidu+OR+Telugu+Desam+Andhra+Pradesh&hl=en-IN&gl=IN&ceid=IN:en'
    ];

    try {
      // YSRCP news
      const ysrcpResponse = await axios.get(googleNewsFeeds[0], {
        timeout: 8000,
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
      });
      const $ysrcp = cheerio.load(ysrcpResponse.data, { xmlMode: true });
      $ysrcp('item').slice(0, 15).each((i, elem) => {
        const title = $ysrcp(elem).find('title').text();
        const link = $ysrcp(elem).find('link').text();
        const pubDate = $ysrcp(elem).find('pubDate').text();

        if (title && !title.includes('Google News')) {
          const sentiment = this.analyzeSentiment(title);
          const district = this.detectDistrict(title);

          newsData.ysrcp.push({
            title: title.trim(),
            link: link.trim(),
            date: pubDate ? new Date(pubDate).toISOString() : new Date().toISOString(),
            source: 'Google News',
            sentiment: sentiment,
            district: district
          });
        }
      });
      console.log(`Scraped ${newsData.ysrcp.length} YSRCP news items with sentiment analysis`);
    } catch (error) {
      console.log('Failed to scrape YSRCP news:', error.message);
    }

    try {
      // TDP news
      const tdpResponse = await axios.get(googleNewsFeeds[1], {
        timeout: 8000,
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
      });
      const $tdp = cheerio.load(tdpResponse.data, { xmlMode: true });
      $tdp('item').slice(0, 15).each((i, elem) => {
        const title = $tdp(elem).find('title').text();
        const link = $tdp(elem).find('link').text();
        const pubDate = $tdp(elem).find('pubDate').text();

        if (title && !title.includes('Google News')) {
          const sentiment = this.analyzeSentiment(title);
          const district = this.detectDistrict(title);

          newsData.tdp.push({
            title: title.trim(),
            link: link.trim(),
            date: pubDate ? new Date(pubDate).toISOString() : new Date().toISOString(),
            source: 'Google News',
            sentiment: sentiment,
            district: district
          });
        }
      });
      console.log(`Scraped ${newsData.tdp.length} TDP news items with sentiment analysis`);
    } catch (error) {
      console.log('Failed to scrape TDP news:', error.message);
    }

    return newsData;
  }
  
  async refreshData() {
    try {
      // Get real YouTube data
      console.log('Fetching real YouTube data for metrics...');
      const youtubeData = await this.youtubeCollector.getAllPartyVideos();

      // Calculate real metrics from YouTube data
      const ysrcpMetrics = this.calculateRealMetrics(youtubeData.ysrcp);
      const tdpMetrics = this.calculateRealMetrics(youtubeData.tdp);

      // Update cache with real data
      this.dataCache.ysrcp.metrics = ysrcpMetrics;
      this.dataCache.tdp.metrics = tdpMetrics;

      // Scrape real news
      const newsData = await this.scrapeNews();

      if (newsData.ysrcp.length > 0) {
        this.dataCache.ysrcp.news = [...newsData.ysrcp, ...this.dataCache.ysrcp.news].slice(0, 10);
      }
      if (newsData.tdp.length > 0) {
        this.dataCache.tdp.news = [...newsData.tdp, ...this.dataCache.tdp.news].slice(0, 10);
      }

      this.dataCache.lastUpdated = new Date().toISOString();

      console.log('Real data updated - YSRCP:', ysrcpMetrics.popularity, '%, TDP:', tdpMetrics.popularity, '%');

      await this.saveCache();
    } catch (error) {
      console.error('Error refreshing data:', error);
    }
  }

  calculateRealMetrics(partyData) {
    // Combine all videos
    const allVideos = [...partyData.trending, ...partyData.official];

    if (allVideos.length === 0) {
      return {
        popularity: 0,
        sentiment: 0,
        mediaPresence: 0,
        socialMedia: 0
      };
    }

    // Calculate total engagement
    const totalViews = allVideos.reduce((sum, v) => sum + v.views, 0);
    const totalLikes = allVideos.reduce((sum, v) => sum + v.likes, 0);
    const totalComments = allVideos.reduce((sum, v) => sum + v.comments, 0);
    const totalVideos = allVideos.length;

    // Calculate average engagement rate
    const avgEngagementRate = totalViews > 0
      ? ((totalLikes + totalComments) / totalViews) * 100
      : 0;

    // Calculate popularity (0-100 scale based on views)
    // Normalize views to a 0-100 scale (assuming 10M views = 100%)
    const popularity = Math.min(100, Math.round((totalViews / 10000000) * 100));

    // Calculate sentiment based on like ratio
    // High likes vs low comments = positive sentiment
    const likeRatio = totalViews > 0 ? (totalLikes / totalViews) : 0;
    const sentiment = Math.min(1, likeRatio * 100); // Normalize to 0-1

    // Media presence based on video count and views
    const mediaPresence = Math.min(100, Math.round((totalVideos * 2) + (totalViews / 100000)));

    // Social media score based on engagement
    const socialMedia = Math.min(100, Math.round(avgEngagementRate * 1000));

    return {
      popularity: Math.max(1, popularity), // Ensure at least 1%
      sentiment: sentiment,
      mediaPresence: Math.max(1, mediaPresence),
      socialMedia: Math.max(1, socialMedia),
      totalViews: totalViews,
      totalVideos: totalVideos,
      totalEngagement: totalLikes + totalComments
    };
  }
  
  async saveCache() {
    try {
      const dataDir = path.join(__dirname, '../../data');
      await fs.mkdir(dataDir, { recursive: true });
      const cachePath = path.join(dataDir, 'cache.json');
      await fs.writeFile(cachePath, JSON.stringify(this.dataCache, null, 2));
    } catch (error) {
      console.error('Error saving cache:', error);
    }
  }
  
  async getLatestData() {
    if (!this.dataCache.lastUpdated || 
        Date.now() - new Date(this.dataCache.lastUpdated).getTime() > 3600000) {
      await this.refreshData();
    }
    return this.dataCache;
  }
}

export default DataCollector;