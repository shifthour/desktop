import axios from 'axios';
import * as cheerio from 'cheerio';
import YouTubeCollector from './youtubeCollector.js';

class SocialMediaCollector {
  constructor() {
    this.youtubeCollector = new YouTubeCollector();
    this.platforms = {
      twitter: {
        name: 'X (Twitter)',
        icon: '𝕏',
        color: '#000000'
      },
      youtube: {
        name: 'YouTube',
        icon: '▶️',
        color: '#FF0000'
      },
      facebook: {
        name: 'Facebook',
        icon: '👤',
        color: '#1877F2'
      },
      instagram: {
        name: 'Instagram',
        icon: '📷',
        color: '#E4405F'
      },
      googleTrends: {
        name: 'Google Trends',
        icon: '📊',
        color: '#4285F4'
      },
      reddit: {
        name: 'Reddit',
        icon: '🔺',
        color: '#FF4500'
      },
      linkedin: {
        name: 'LinkedIn',
        icon: '💼',
        color: '#0077B5'
      }
    };

    this.partyHandles = {
      ysrcp: {
        twitter: ['ysjagan', 'YSRCParty'],
        youtube: ['YSRCongress'],
        facebook: ['YSRCParty'],
        instagram: ['ysrcpofficial'],
        keywords: ['YSRCP', 'YS Jagan', 'Jagan Mohan Reddy', 'వైఎస్ జగన్']
      },
      tdp: {
        twitter: ['ncbn', 'JaiTDP'],
        youtube: ['TeluguDesamParty'],
        facebook: ['TeluguDesamParty'],
        instagram: ['telugudesamparty'],
        keywords: ['TDP', 'Chandrababu Naidu', 'CBN', 'చంద్రబాబు నాయుడు']
      }
    };

    this.mockData = this.generateMockData();
  }

  generateMockData() {
    const now = new Date();
    const data = {
      ysrcp: {},
      tdp: {},
      timestamp: now.toISOString()
    };

    const generateTimeSeriesData = (baseValue, volatility = 0.1) => {
      const hours = [];
      for (let i = 23; i >= 0; i--) {
        const time = new Date(now - i * 3600000);
        const variation = (Math.random() - 0.5) * 2 * volatility;
        hours.push({
          time: time.toISOString(),
          value: Math.max(0, baseValue * (1 + variation))
        });
      }
      return hours;
    };

    const generateDailyData = (baseValue, days = 7) => {
      const dailyData = [];
      for (let i = days - 1; i >= 0; i--) {
        const date = new Date(now - i * 86400000);
        const variation = (Math.random() - 0.5) * 0.3;
        dailyData.push({
          date: date.toISOString().split('T')[0],
          value: Math.max(0, Math.floor(baseValue * (1 + variation)))
        });
      }
      return dailyData;
    };

    ['ysrcp', 'tdp'].forEach(party => {
      const baseMultiplier = party === 'ysrcp' ? 0.9 : 1.1;
      
      data[party] = {
        twitter: {
          followers: Math.floor(850000 * baseMultiplier),
          followersChange: Math.floor((Math.random() - 0.5) * 5000),
          tweets24h: Math.floor(25 + Math.random() * 20),
          engagement: {
            likes: Math.floor(45000 * baseMultiplier),
            retweets: Math.floor(12000 * baseMultiplier),
            replies: Math.floor(8500 * baseMultiplier),
            impressions: Math.floor(2500000 * baseMultiplier)
          },
          trending: Math.random() > 0.7,
          hashtagPerformance: [
            { tag: party === 'ysrcp' ? '#YSJaganForAP' : '#CBNForDevelopment', count: Math.floor(15000 * baseMultiplier) },
            { tag: party === 'ysrcp' ? '#JaganAnnaNaveenaNidhi' : '#TDPForProgress', count: Math.floor(12000 * baseMultiplier) },
            { tag: party === 'ysrcp' ? '#WhyJaganWhy' : '#VisionaryNaidu', count: Math.floor(9000 * baseMultiplier) }
          ],
          hourlyActivity: generateTimeSeriesData(1000, 0.3),
          sentimentScore: 0.3 + Math.random() * 0.4
        },
        
        youtube: {
          subscribers: Math.floor(425000 * baseMultiplier),
          subscriberChange: Math.floor((Math.random() - 0.3) * 2000),
          videos24h: Math.floor(2 + Math.random() * 3),
          totalViews: Math.floor(85000000 * baseMultiplier),
          avgViewDuration: Math.floor(240 + Math.random() * 120),
          engagement: {
            likes: Math.floor(25000 * baseMultiplier),
            comments: Math.floor(8500 * baseMultiplier),
            shares: Math.floor(5500 * baseMultiplier)
          },
          topVideos: [
            {
              title: party === 'ysrcp' ? 'CM Jagan Speech on Development' : 'CBN Technology Vision 2030',
              views: Math.floor(250000 * baseMultiplier),
              likes: Math.floor(15000 * baseMultiplier)
            },
            {
              title: party === 'ysrcp' ? 'YSRCP Welfare Schemes Explained' : 'TDP Manifesto Launch',
              views: Math.floor(180000 * baseMultiplier),
              likes: Math.floor(12000 * baseMultiplier)
            }
          ],
          dailyViews: generateDailyData(50000)
        },
        
        facebook: {
          pageFollowers: Math.floor(1250000 * baseMultiplier),
          followersChange: Math.floor((Math.random() - 0.4) * 8000),
          posts24h: Math.floor(8 + Math.random() * 7),
          engagement: {
            likes: Math.floor(125000 * baseMultiplier),
            comments: Math.floor(35000 * baseMultiplier),
            shares: Math.floor(45000 * baseMultiplier),
            reactions: {
              like: Math.floor(80000 * baseMultiplier),
              love: Math.floor(25000 * baseMultiplier),
              wow: Math.floor(10000 * baseMultiplier),
              angry: Math.floor(5000 * baseMultiplier),
              sad: Math.floor(5000 * baseMultiplier)
            }
          },
          reach: Math.floor(3500000 * baseMultiplier),
          videoViews: Math.floor(850000 * baseMultiplier),
          dailyEngagement: generateDailyData(150000)
        },
        
        instagram: {
          followers: Math.floor(385000 * baseMultiplier),
          followersChange: Math.floor((Math.random() - 0.3) * 3000),
          posts24h: Math.floor(3 + Math.random() * 4),
          stories24h: Math.floor(5 + Math.random() * 5),
          engagement: {
            likes: Math.floor(85000 * baseMultiplier),
            comments: Math.floor(12000 * baseMultiplier),
            saves: Math.floor(8500 * baseMultiplier),
            shares: Math.floor(15000 * baseMultiplier)
          },
          reelsViews: Math.floor(1250000 * baseMultiplier),
          avgEngagementRate: (3.5 + Math.random() * 2).toFixed(2) + '%',
          topHashtags: [
            '#' + party.toUpperCase(),
            party === 'ysrcp' ? '#JaganAnna' : '#CBN',
            '#AndhraPradesh',
            '#APPolitics'
          ],
          storyViews: Math.floor(125000 * baseMultiplier)
        },
        
        googleTrends: {
          searchInterest: Math.floor(65 + Math.random() * 20),
          searchVolume: Math.floor(450000 * baseMultiplier),
          relatedQueries: [
            party === 'ysrcp' ? 'YSRCP schemes 2024' : 'TDP manifesto 2024',
            party === 'ysrcp' ? 'Jagan latest news' : 'Chandrababu Naidu news',
            party === 'ysrcp' ? 'YCP government achievements' : 'TDP IT policy'
          ],
          regionalInterest: {
            'Andhra Pradesh': Math.floor(85 + Math.random() * 10),
            'Telangana': Math.floor(45 + Math.random() * 15),
            'Karnataka': Math.floor(25 + Math.random() * 10),
            'Tamil Nadu': Math.floor(20 + Math.random() * 10)
          },
          trendingScore: Math.floor(70 + Math.random() * 20),
          weeklyTrend: generateDailyData(75)
        },
        
        reddit: {
          mentions: Math.floor(250 * baseMultiplier),
          upvotes: Math.floor(8500 * baseMultiplier),
          comments: Math.floor(3200 * baseMultiplier),
          sentiment: (Math.random() - 0.3) * 2,
          activeThreads: Math.floor(15 + Math.random() * 10),
          topSubreddits: [
            { name: 'r/AndhraPradesh', mentions: Math.floor(120 * baseMultiplier) },
            { name: 'r/IndiaSpeaks', mentions: Math.floor(80 * baseMultiplier) },
            { name: 'r/india', mentions: Math.floor(50 * baseMultiplier) }
          ]
        },
        
        linkedin: {
          followers: Math.floor(125000 * baseMultiplier),
          posts24h: Math.floor(2 + Math.random() * 3),
          engagement: {
            likes: Math.floor(8500 * baseMultiplier),
            comments: Math.floor(2500 * baseMultiplier),
            shares: Math.floor(3500 * baseMultiplier)
          },
          articleViews: Math.floor(45000 * baseMultiplier),
          professionalMentions: Math.floor(850 * baseMultiplier)
        },
        
        aggregateMetrics: {
          totalFollowers: 0,
          totalEngagement: 0,
          avgSentiment: 0,
          viralityScore: 0,
          crossPlatformReach: 0
        }
      };
    });

    // Calculate aggregate metrics
    ['ysrcp', 'tdp'].forEach(party => {
      const metrics = data[party];
      
      metrics.aggregateMetrics.totalFollowers = 
        metrics.twitter.followers +
        metrics.youtube.subscribers +
        metrics.facebook.pageFollowers +
        metrics.instagram.followers +
        metrics.linkedin.followers;
      
      metrics.aggregateMetrics.totalEngagement = 
        metrics.twitter.engagement.likes + metrics.twitter.engagement.retweets +
        metrics.youtube.engagement.likes + metrics.youtube.engagement.comments +
        metrics.facebook.engagement.likes + metrics.facebook.engagement.shares +
        metrics.instagram.engagement.likes + metrics.instagram.engagement.comments;
      
      metrics.aggregateMetrics.avgSentiment = 
        (metrics.twitter.sentimentScore + metrics.reddit.sentiment / 2 + 0.5) / 2;
      
      metrics.aggregateMetrics.viralityScore = 
        Math.min(100, (metrics.aggregateMetrics.totalEngagement / metrics.aggregateMetrics.totalFollowers) * 100);
      
      metrics.aggregateMetrics.crossPlatformReach = 
        metrics.twitter.engagement.impressions +
        metrics.facebook.reach +
        metrics.instagram.storyViews +
        metrics.youtube.totalViews / 30;
    });

    return data;
  }

  async collectLiveData() {
    try {
      console.log('Collecting real YouTube data for social media overview...');

      // Get real YouTube data
      const youtubeData = await this.youtubeCollector.getAllPartyVideos();

      // Start with mock data for other platforms
      const data = this.generateMockData();

      // Replace YouTube section with REAL data
      ['ysrcp', 'tdp'].forEach(party => {
        const partyYouTubeData = youtubeData[party];
        const allVideos = [...partyYouTubeData.trending, ...partyYouTubeData.official];

        if (allVideos.length > 0) {
          const totalViews = allVideos.reduce((sum, v) => sum + v.views, 0);
          const totalLikes = allVideos.reduce((sum, v) => sum + v.likes, 0);
          const totalComments = allVideos.reduce((sum, v) => sum + v.comments, 0);

          // Update with REAL YouTube data
          data[party].youtube = {
            subscribers: partyYouTubeData.totalVideos * 5000, // Estimate based on videos
            subscriberChange: Math.floor((Math.random() - 0.3) * 2000),
            videos24h: partyYouTubeData.totalVideos,
            totalViews: totalViews,
            avgViewDuration: 240,
            engagement: {
              likes: totalLikes,
              comments: totalComments,
              shares: Math.floor(totalLikes * 0.3)
            },
            topVideos: allVideos.slice(0, 3).map(v => ({
              title: v.title,
              views: v.views,
              likes: v.likes
            })),
            dailyViews: []
          };

          // Recalculate aggregate metrics with real YouTube data
          data[party].aggregateMetrics.totalFollowers =
            data[party].twitter.followers +
            data[party].youtube.subscribers +
            data[party].facebook.pageFollowers +
            data[party].instagram.followers +
            data[party].linkedin.followers;

          data[party].aggregateMetrics.totalEngagement =
            data[party].twitter.engagement.likes + data[party].twitter.engagement.retweets +
            data[party].youtube.engagement.likes + data[party].youtube.engagement.comments +
            data[party].facebook.engagement.likes + data[party].facebook.engagement.shares +
            data[party].instagram.engagement.likes + data[party].instagram.engagement.comments;
        }
      });

      console.log('Real YouTube data integrated into social media overview');
      return data;

    } catch (error) {
      console.error('Error collecting real YouTube data:', error.message);
      // Fallback to mock data if YouTube API fails
      return this.generateMockData();
    }
  }

  async getTrendingTopics() {
    return {
      ysrcp: [
        { topic: 'Welfare Schemes', momentum: 85, sentiment: 'positive' },
        { topic: 'Rural Development', momentum: 72, sentiment: 'positive' },
        { topic: 'Youth Employment', momentum: 68, sentiment: 'mixed' },
        { topic: 'Agriculture Support', momentum: 78, sentiment: 'positive' }
      ],
      tdp: [
        { topic: 'IT Development', momentum: 88, sentiment: 'positive' },
        { topic: 'Infrastructure', momentum: 82, sentiment: 'positive' },
        { topic: 'Investment Plans', momentum: 75, sentiment: 'mixed' },
        { topic: 'Smart Cities', momentum: 70, sentiment: 'positive' }
      ]
    };
  }

  async getInfluencerMentions() {
    return {
      ysrcp: [
        { name: 'Political Analyst 1', followers: 250000, mentions: 12, sentiment: 'positive' },
        { name: 'News Anchor 2', followers: 180000, mentions: 8, sentiment: 'neutral' },
        { name: 'Social Activist 3', followers: 95000, mentions: 5, sentiment: 'positive' }
      ],
      tdp: [
        { name: 'Tech Influencer 1', followers: 320000, mentions: 15, sentiment: 'positive' },
        { name: 'Business Leader 2', followers: 210000, mentions: 10, sentiment: 'positive' },
        { name: 'Media Personality 3', followers: 150000, mentions: 7, sentiment: 'neutral' }
      ]
    };
  }

  calculateEngagementRate(engagement, followers) {
    if (!followers || followers === 0) return 0;
    return ((engagement / followers) * 100).toFixed(2);
  }

  analyzeSentiment(text) {
    // Simple sentiment analysis
    const positiveWords = ['good', 'great', 'excellent', 'amazing', 'positive', 'success', 'development'];
    const negativeWords = ['bad', 'poor', 'failure', 'corrupt', 'negative', 'problem', 'issue'];
    
    const words = text.toLowerCase().split(/\s+/);
    let score = 0;
    
    words.forEach(word => {
      if (positiveWords.includes(word)) score++;
      if (negativeWords.includes(word)) score--;
    });
    
    return score / Math.max(words.length, 1);
  }
}

export default SocialMediaCollector;