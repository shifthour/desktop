"""
Pydantic schemas for API responses
"""

from pydantic import BaseModel
from typing import Dict, List, Any, Optional
from datetime import datetime


class SentimentDistribution(BaseModel):
    positive: int
    negative: int
    neutral: int


class PartyStats(BaseModel):
    totalReach: int
    totalEngagement: int
    totalFollowers: int
    postsToday: int
    avgEngagementRate: float
    shareOfVoice: int


class OverallStatsResponse(BaseModel):
    ysrcp: PartyStats
    tdp: PartyStats


class PlatformPartyStats(BaseModel):
    followers: int
    followersGrowth: int
    posts: int
    reach: int
    engagement: int
    engagementRate: float
    sentiment: SentimentDistribution


class PlatformStats(BaseModel):
    name: str
    ysrcp: PlatformPartyStats
    tdp: PlatformPartyStats


class TrendingHashtag(BaseModel):
    tag: str
    count: int
    sentiment: str
    party: str
    trending: bool


class SearchInterest(BaseModel):
    ysrcp: int
    tdp: int
    trend: str


class TimelineDataPoint(BaseModel):
    date: str
    ysrcp: int
    tdp: int


class GoogleTrendsResponse(BaseModel):
    searchInterest: SearchInterest
    searchTimeline: List[TimelineDataPoint]
    averages: Dict[str, float]


class RegionalInterest(BaseModel):
    district: str
    ysrcp: int
    tdp: int


class RelatedQuery(BaseModel):
    query: str
    interest: int
    change: str
    isBreakout: bool


class NewsArticle(BaseModel):
    title: str
    link: str
    source: str
    publishedAt: str
    description: str
    party: str


class NewsPartyData(BaseModel):
    articles: List[NewsArticle]
    totalMentions: int
    sources: List[str]


class TrendingTopic(BaseModel):
    topic: str
    count: int


class NewsResponse(BaseModel):
    ysrcp: NewsPartyData
    tdp: NewsPartyData
    trending: List[TrendingTopic]
    lastUpdated: str


class SentimentScore(BaseModel):
    score: int
    sentiment: SentimentDistribution
    overall: str


class SentimentResponse(BaseModel):
    ysrcp: SentimentScore
    tdp: SentimentScore
    comparison: Dict[str, Any]


class DashboardResponse(BaseModel):
    """Complete dashboard data response"""
    overallStats: OverallStatsResponse
    platformStats: Dict[str, Any]
    trendingHashtags: List[TrendingHashtag]
    googleTrends: GoogleTrendsResponse
    sentiment: SentimentResponse
    lastUpdated: str


class HealthCheckResponse(BaseModel):
    status: str
    timestamp: str
    services: Dict[str, str]
