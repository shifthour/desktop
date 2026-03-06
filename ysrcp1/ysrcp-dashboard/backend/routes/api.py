"""
API Routes for YSRCP Political Dashboard
Uses lazy imports for faster startup
"""

from fastapi import APIRouter, HTTPException
from typing import Dict, Any, List
from datetime import datetime

# Lazy import helpers - services are loaded on first use, not at startup
_services = {}

def get_service(name):
    """Lazy load services to speed up server startup"""
    if name not in _services:
        if name == 'google_trends':
            from services.google_trends import google_trends_service
            _services[name] = google_trends_service
        elif name == 'news':
            from services.news_service import news_service
            _services[name] = news_service
        elif name == 'sentiment':
            from services.sentiment_service import sentiment_service
            _services[name] = sentiment_service
        elif name == 'social':
            from services.social_service import social_service
            _services[name] = social_service
        elif name == 'youtube':
            from services.youtube_service import youtube_service
            _services[name] = youtube_service
        elif name == 'twitter':
            from services.twitter_service import twitter_service
            _services[name] = twitter_service
        elif name == 'instagram':
            from services.instagram_service import instagram_service
            _services[name] = instagram_service
        elif name == 'facebook':
            from services.facebook_service import facebook_service
            _services[name] = facebook_service
        elif name == 'stats':
            from services.stats_aggregator import stats_aggregator
            _services[name] = stats_aggregator
    return _services[name]

router = APIRouter(prefix="/api", tags=["Dashboard API"])


@router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {
            "google_trends": "active",
            "news": "active",
            "sentiment": "active",
            "social": "active",
            "youtube": "active",
            "twitter": "active",
            "instagram": "active",
            "facebook": "active"
        }
    }


@router.post("/refresh")
async def refresh_all_data():
    """
    Clear all caches and fetch fresh data from all sources
    Called when user clicks the refresh button
    """
    try:
        # Clear all service caches
        get_service('twitter').clear_cache()
        get_service('instagram').clear_cache()
        get_service('facebook').clear_cache()
        get_service('youtube').clear_cache()

        # Fetch fresh data from all sources
        twitter_data = get_service('twitter').get_party_stats()
        instagram_data = get_service('instagram').get_trending_posts()
        facebook_data = get_service('facebook').get_trending_posts()
        youtube_data = get_service('youtube').get_trending_videos()

        return {
            "success": True,
            "message": "All data refreshed from sources",
            "timestamp": datetime.now().isoformat(),
            "sources": {
                "twitter": {
                    "status": "live" if twitter_data.get('isLive', False) else "cached",
                    "ysrcp_followers": twitter_data.get('ysrcp', {}).get('followers', 0),
                    "tdp_followers": twitter_data.get('tdp', {}).get('followers', 0)
                },
                "instagram": {
                    "status": "live" if instagram_data.get('isLive', False) else "offline",
                    "ysrcp_posts": len(instagram_data.get('ysrcp', {}).get('posts', [])),
                    "tdp_posts": len(instagram_data.get('tdp', {}).get('posts', []))
                },
                "facebook": {
                    "status": "live" if facebook_data.get('isLive', False) else "offline",
                    "ysrcp_posts": len(facebook_data.get('ysrcp', {}).get('posts', [])),
                    "tdp_posts": len(facebook_data.get('tdp', {}).get('posts', []))
                },
                "youtube": {
                    "status": "live" if youtube_data.get('isLive', False) else "offline",
                    "ysrcp_videos": len(youtube_data.get('ysrcp', {}).get('videos', [])),
                    "tdp_videos": len(youtube_data.get('tdp', {}).get('videos', []))
                }
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error refreshing data: {str(e)}")


@router.get("/dashboard")
async def get_dashboard_data():
    """
    Get all dashboard data in a single call
    This is the main endpoint for the frontend
    """
    try:
        # Get real stats from aggregator (uses cached, consistent values)
        real_overall_stats = get_service('stats').get_real_overall_stats()
        sentiment_battle = get_service('stats').get_real_sentiment_battle()

        # Fetch all data concurrently
        overall_stats = await get_service('social').get_overall_stats()
        platform_stats = await get_service('social').get_platform_stats()
        trending_hashtags = await get_service('social').get_trending_hashtags()

        # Merge real stats with overall stats
        overall_stats['ysrcp']['sentimentScore'] = real_overall_stats['ysrcp']['sentimentScore']
        overall_stats['ysrcp']['shareOfVoice'] = real_overall_stats['ysrcp']['shareOfVoice']
        overall_stats['ysrcp']['avgEngagementRate'] = real_overall_stats['ysrcp']['avgEngagementRate']
        overall_stats['tdp']['sentimentScore'] = real_overall_stats['tdp']['sentimentScore']
        overall_stats['tdp']['shareOfVoice'] = real_overall_stats['tdp']['shareOfVoice']
        overall_stats['tdp']['avgEngagementRate'] = real_overall_stats['tdp']['avgEngagementRate']

        # Get full Google Trends data
        google_trends_interest = get_service('google_trends').get_interest_over_time()
        google_trends_regional = get_service('google_trends').get_regional_interest()
        google_trends_queries = get_service('google_trends').get_related_queries()
        google_trends_breakout = get_service('google_trends').get_breakout_topics()

        google_trends = {
            "interest": google_trends_interest,
            "regional": google_trends_regional,
            "queries": google_trends_queries,
            "breakout": google_trends_breakout
        }

        # Get YouTube trending videos
        youtube_data = get_service('youtube').get_trending_videos()

        # Get Twitter trending tweets
        twitter_data = get_service('twitter').get_trending_tweets()

        # Get Instagram trending posts
        instagram_data = get_service('instagram').get_trending_posts()

        # Get Facebook trending posts
        facebook_data = get_service('facebook').get_trending_posts()

        # Get influencer data
        influencer_data = get_service('twitter').get_influencers()

        news_data = await get_service('news').get_all_news()

        # Get sentiment from news articles
        ysrcp_texts = [a['title'] + ' ' + a.get('description', '') for a in news_data['ysrcp']['articles']]
        tdp_texts = [a['title'] + ' ' + a.get('description', '') for a in news_data['tdp']['articles']]
        sentiment_data = get_service('sentiment').get_sentiment_score(ysrcp_texts, tdp_texts)

        return {
            "overallStats": overall_stats,
            "platformStats": platform_stats,
            "trendingHashtags": trending_hashtags,
            "googleTrends": google_trends,
            "youtube": youtube_data,
            "twitter": twitter_data,
            "instagram": instagram_data,
            "facebook": facebook_data,
            "influencers": influencer_data,
            "sentiment": sentiment_data,
            "sentimentBattle": sentiment_battle,
            "news": {
                "ysrcp": {
                    "mentions": news_data['ysrcp']['totalMentions'],
                    "articles": news_data['ysrcp']['articles'][:5]
                },
                "tdp": {
                    "mentions": news_data['tdp']['totalMentions'],
                    "articles": news_data['tdp']['articles'][:5]
                },
                "trending": news_data['trending']
            },
            "alerts": [
                {
                    "id": 1,
                    "type": "warning",
                    "title": "Negative Trend Detected",
                    "message": "Spike in negative mentions detected in social media",
                    "time": "15 mins ago",
                    "platform": "twitter",
                    "priority": "high"
                },
                {
                    "id": 2,
                    "type": "info",
                    "title": "Competitor Activity",
                    "message": "TDP launched new campaign hashtag",
                    "time": "1 hour ago",
                    "platform": "all",
                    "priority": "medium"
                },
                {
                    "id": 3,
                    "type": "success",
                    "title": "Engagement Milestone",
                    "message": "YSRCP Twitter reached 1M impressions today",
                    "time": "2 hours ago",
                    "platform": "twitter",
                    "priority": "low"
                },
                {
                    "id": 4,
                    "type": "info",
                    "title": "Trending Topic",
                    "message": "#JaganannaConnects trending in Andhra Pradesh",
                    "time": "3 hours ago",
                    "platform": "twitter",
                    "priority": "medium"
                }
            ],
            "lastUpdated": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching dashboard data: {str(e)}")


@router.get("/stats/overall")
async def get_overall_stats():
    """Get overall statistics for both parties"""
    try:
        return await get_service('social').get_overall_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/platforms")
async def get_platform_stats():
    """Get platform-wise statistics"""
    try:
        return await get_service('social').get_platform_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trends/google")
async def get_google_trends():
    """Get Google Trends data"""
    try:
        interest = get_service('google_trends').get_interest_over_time()
        regional = get_service('google_trends').get_regional_interest()
        related = get_service('google_trends').get_related_queries()
        breakout = get_service('google_trends').get_breakout_topics()

        return {
            "interest": interest,
            "regional": regional,
            "relatedQueries": related,
            "breakoutTopics": breakout
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trends/regional")
async def get_regional_trends():
    """Get regional search interest"""
    try:
        return get_service('google_trends').get_regional_interest()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trends/queries")
async def get_related_queries():
    """Get related search queries"""
    try:
        return get_service('google_trends').get_related_queries()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/hashtags")
async def get_trending_hashtags():
    """Get trending hashtags"""
    try:
        return await get_service('social').get_trending_hashtags()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/news")
async def get_news():
    """Get news articles for both parties"""
    try:
        return await get_service('news').get_all_news()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/news/stats")
async def get_news_stats():
    """Get news statistics"""
    try:
        return await get_service('news').get_news_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sentiment")
async def get_sentiment():
    """Get sentiment analysis"""
    try:
        # Get news data for sentiment analysis
        news_data = await get_service('news').get_all_news()

        ysrcp_texts = [a['title'] + ' ' + a.get('description', '') for a in news_data['ysrcp']['articles']]
        tdp_texts = [a['title'] + ' ' + a.get('description', '') for a in news_data['tdp']['articles']]

        return get_service('sentiment').get_sentiment_score(ysrcp_texts, tdp_texts)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sentiment/analyze")
async def analyze_text(data: Dict[str, Any]):
    """Analyze sentiment of provided text"""
    try:
        text = data.get("text", "")
        if not text:
            raise HTTPException(status_code=400, detail="Text is required")

        result = get_service('sentiment').analyze_text(text)
        party_context = get_service('sentiment').classify_party_sentiment(text)

        return {
            "analysis": result,
            "partyContext": party_context
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alerts")
async def get_alerts():
    """Get alerts and notifications"""
    # In production, this would track real alerts
    return [
        {
            "id": 1,
            "type": "warning",
            "title": "Negative Trend Detected",
            "message": "Spike in negative mentions detected in social media",
            "time": "15 mins ago",
            "platform": "twitter",
            "priority": "high"
        },
        {
            "id": 2,
            "type": "info",
            "title": "Competitor Activity",
            "message": "TDP launched new campaign hashtag",
            "time": "1 hour ago",
            "platform": "all",
            "priority": "medium"
        }
    ]


@router.get("/youtube/trending")
async def get_youtube_trending(party: str = "all"):
    """
    Get trending YouTube videos for YSRCP, TDP, or both
    party: 'ysrcp', 'tdp', or 'all' (default)
    """
    try:
        return get_service('youtube').get_trending_videos(party)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/youtube/ysrcp")
async def get_ysrcp_videos():
    """Get trending videos for YSRCP"""
    try:
        result = get_service('youtube').get_trending_videos('ysrcp')
        return result.get('ysrcp', {'videos': [], 'totalViews': 0})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/youtube/tdp")
async def get_tdp_videos():
    """Get trending videos for TDP"""
    try:
        result = get_service('youtube').get_trending_videos('tdp')
        return result.get('tdp', {'videos': [], 'totalViews': 0})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== TWITTER ENDPOINTS ====================

@router.get("/twitter/trending")
async def get_twitter_trending(party: str = "all"):
    """
    Get trending tweets for YSRCP, TDP, or both
    party: 'ysrcp', 'tdp', or 'all' (default)
    """
    try:
        return get_service('twitter').get_trending_tweets(party)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/twitter/search")
async def search_twitter(query: str, count: int = 20):
    """Search tweets by query"""
    try:
        tweets = get_service('twitter').search_tweets(query, count)
        return {"tweets": tweets, "count": len(tweets)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/twitter/topics")
async def get_twitter_topics():
    """Get trending hashtags from political tweets"""
    try:
        return get_service('twitter').get_trending_topics()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/twitter/stats")
async def get_twitter_stats():
    """Get real-time Twitter stats for both parties (follower counts, etc.)"""
    try:
        return get_service('twitter').get_party_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/twitter/user/{username}")
async def get_twitter_user(username: str):
    """Get Twitter user profile data"""
    try:
        profile = get_service('twitter').get_user_profile(username)
        if not profile:
            raise HTTPException(status_code=404, detail="User not found")
        return profile
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== INSTAGRAM ENDPOINTS ====================

@router.get("/instagram/trending")
async def get_instagram_trending(party: str = "all"):
    """
    Get trending Instagram posts for YSRCP, TDP, or both
    party: 'ysrcp', 'tdp', or 'all' (default)
    """
    try:
        return get_service('instagram').get_trending_posts(party)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/instagram/stats")
async def get_instagram_stats():
    """Get Instagram stats for both parties"""
    try:
        return get_service('instagram').get_party_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/instagram/user/{username}")
async def get_instagram_user_posts(username: str):
    """Get Instagram posts for a specific user"""
    try:
        posts = get_service('instagram').get_user_posts(username)
        return {"posts": posts, "count": len(posts)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== FACEBOOK ENDPOINTS ====================

@router.get("/facebook/trending")
async def get_facebook_trending(party: str = "all"):
    """
    Get trending Facebook posts for YSRCP, TDP, or both
    party: 'ysrcp', 'tdp', or 'all' (default)
    """
    try:
        return get_service('facebook').get_trending_posts(party)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/facebook/stats")
async def get_facebook_stats():
    """Get Facebook stats for both parties"""
    try:
        return get_service('facebook').get_party_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/facebook/page/{profile_id}")
async def get_facebook_page_posts(profile_id: str):
    """Get Facebook posts for a specific page"""
    try:
        posts = get_service('facebook').search_posts(profile_id)
        return {"posts": posts, "count": len(posts)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== INFLUENCER ENDPOINTS ====================

@router.get("/influencers")
async def get_influencers():
    """
    Get top influencers based on Twitter activity
    Returns users who actively tweet about YSRCP/TDP with their engagement stats
    """
    try:
        return get_service('twitter').get_influencers()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
