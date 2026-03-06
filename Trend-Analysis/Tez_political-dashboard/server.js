import express from 'express';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import DataCollector from './src/collectors/dataCollector.js';
import SocialMediaCollector from './src/collectors/socialMediaCollector.js';
import AdvancedDataCollector from './src/collectors/advancedDataCollector.js';
import YouTubeCollector from './src/collectors/youtubeCollector.js';
import SentimentAnalyzer from './src/analysis/sentimentAnalyzer.js';
import TrendAnalyzer from './src/analysis/trendAnalyzer.js';
import StrategicAnalyzer from './src/analysis/strategicAnalyzer.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const app = express();
const PORT = 3000;

app.use(cors());
app.use(express.json());
app.use(express.static('public'));

const dataCollector = new DataCollector();
const socialMediaCollector = new SocialMediaCollector();
const advancedDataCollector = new AdvancedDataCollector();
const youtubeCollector = new YouTubeCollector();
const sentimentAnalyzer = new SentimentAnalyzer();
const trendAnalyzer = new TrendAnalyzer();
const strategicAnalyzer = new StrategicAnalyzer();

app.get('/api/dashboard-data', async (req, res) => {
  try {
    const data = await dataCollector.getLatestData();
    const sentiment = await sentimentAnalyzer.analyzeSentiment(data);
    const trends = await trendAnalyzer.analyzeTrends(data);
    
    res.json({
      ysrcp: {
        sentiment: sentiment.ysrcp,
        trends: trends.ysrcp,
        pros: data.ysrcp.pros,
        cons: data.ysrcp.cons,
        recentNews: data.ysrcp.news
      },
      tdp: {
        sentiment: sentiment.tdp,
        trends: trends.tdp,
        pros: data.tdp.pros,
        cons: data.tdp.cons,
        recentNews: data.tdp.news
      },
      comparison: {
        popularityTrend: trends.comparison,
        sentimentComparison: sentiment.comparison
      }
    });
  } catch (error) {
    console.error('Error fetching dashboard data:', error);
    res.status(500).json({ error: 'Failed to fetch data' });
  }
});

app.get('/api/refresh-data', async (req, res) => {
  try {
    await dataCollector.refreshData();
    res.json({ message: 'Data refreshed successfully' });
  } catch (error) {
    console.error('Error refreshing data:', error);
    res.status(500).json({ error: 'Failed to refresh data' });
  }
});

app.get('/api/social-media-data', async (req, res) => {
  try {
    const socialData = await socialMediaCollector.collectLiveData();
    const trending = await socialMediaCollector.getTrendingTopics();
    const influencers = await socialMediaCollector.getInfluencerMentions();
    
    res.json({
      platforms: socialData,
      trending: trending,
      influencers: influencers,
      lastUpdated: new Date().toISOString()
    });
  } catch (error) {
    console.error('Error fetching social media data:', error);
    res.status(500).json({ error: 'Failed to fetch social media data' });
  }
});

app.get('/api/strategic-analysis', async (req, res) => {
  try {
    const recommendations = strategicAnalyzer.generateStrategicRecommendations();
    const winProbability = strategicAnalyzer.calculateWinProbability();
    const demographics = strategicAnalyzer.keyMetrics.demographics;
    const issues = strategicAnalyzer.keyMetrics.issues;
    const regions = strategicAnalyzer.keyMetrics.regions;
    const segments = strategicAnalyzer.keyMetrics.voterSegments;
    const campaigns = strategicAnalyzer.keyMetrics.campaigns;
    const media = strategicAnalyzer.keyMetrics.mediaPerception;
    
    res.json({
      recommendations,
      winProbability,
      demographics,
      issues,
      regions,
      segments,
      campaigns,
      mediaPerception: media
    });
  } catch (error) {
    console.error('Error in strategic analysis:', error);
    res.status(500).json({ error: 'Failed to generate strategic analysis' });
  }
});

app.get('/api/advanced-data', async (req, res) => {
  try {
    const realTimeData = await advancedDataCollector.collectRealTimeData();
    const electionData = advancedDataCollector.electionData;
    const economicData = advancedDataCollector.economicIndicators;
    const publicOpinion = advancedDataCollector.publicOpinion;
    const performance = advancedDataCollector.governmentPerformance;
    
    res.json({
      realTime: realTimeData,
      elections: electionData,
      economy: economicData,
      publicOpinion,
      performance
    });
  } catch (error) {
    console.error('Error fetching advanced data:', error);
    res.status(500).json({ error: 'Failed to fetch advanced data' });
  }
});

// YouTube Trending Videos API
app.get('/api/youtube/trending', async (req, res) => {
  try {
    const data = await youtubeCollector.getAllPartyVideos();
    res.json({
      success: true,
      data: data,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('Error fetching YouTube trending videos:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch YouTube data',
      message: error.message
    });
  }
});

// Get top videos comparison
app.get('/api/youtube/top-videos', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 10;
    const videos = await youtubeCollector.getTopVideosComparison(limit);
    res.json({
      success: true,
      videos: videos,
      count: videos.length
    });
  } catch (error) {
    console.error('Error fetching top videos:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch top videos'
    });
  }
});

// Get videos for specific party
app.get('/api/youtube/:party', async (req, res) => {
  try {
    const party = req.params.party.toLowerCase();
    if (!['ysrcp', 'tdp'].includes(party)) {
      return res.status(400).json({ error: 'Invalid party. Use ysrcp or tdp' });
    }

    const trending = await youtubeCollector.getTrendingVideos(party, 20);
    const official = await youtubeCollector.getOfficialChannelVideos(party, 10);

    res.json({
      success: true,
      party: party,
      trending: trending,
      official: official,
      totalVideos: trending.length + official.length
    });
  } catch (error) {
    console.error(`Error fetching ${req.params.party} videos:`, error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch party videos'
    });
  }
});

app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
  console.log('Political Dashboard is ready!');
  console.log('YouTube API integration enabled');
});