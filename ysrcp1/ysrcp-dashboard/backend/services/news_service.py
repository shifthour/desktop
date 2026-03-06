"""
News Service - Fetches news from multiple free sources
- Google News RSS (FREE, unlimited)
- NewsAPI.org (FREE tier: 100 requests/day)
"""

import feedparser
import httpx
from bs4 import BeautifulSoup
from cachetools import TTLCache
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import asyncio
import re
from config import NEWS_API_KEY, GOOGLE_NEWS_RSS, YSRCP_KEYWORDS, TDP_KEYWORDS

# Cache for news (30 minutes TTL)
news_cache = TTLCache(maxsize=100, ttl=1800)


class NewsService:
    def __init__(self):
        self.news_api_key = NEWS_API_KEY
        self.rss_feeds = GOOGLE_NEWS_RSS

    async def get_all_news(self) -> Dict[str, Any]:
        """Get combined news from all sources"""
        cache_key = "all_news"
        if cache_key in news_cache:
            return news_cache[cache_key]

        # Fetch from Google News RSS (always free)
        ysrcp_news = await self._fetch_rss_news('ysrcp')
        tdp_news = await self._fetch_rss_news('tdp')
        general_news = await self._fetch_rss_news('ap_politics')

        # Try NewsAPI if key is available
        if self.news_api_key:
            try:
                api_news = await self._fetch_newsapi()
                ysrcp_news.extend(api_news.get('ysrcp', []))
                tdp_news.extend(api_news.get('tdp', []))
            except Exception as e:
                print(f"NewsAPI error: {e}")

        # Remove duplicates and sort by date
        ysrcp_news = self._deduplicate_news(ysrcp_news)
        tdp_news = self._deduplicate_news(tdp_news)

        # Calculate mentions and sentiment distribution
        result = {
            "ysrcp": {
                "articles": ysrcp_news[:10],
                "totalMentions": len(ysrcp_news),
                "sources": list(set([n.get('source', 'Unknown') for n in ysrcp_news]))[:5]
            },
            "tdp": {
                "articles": tdp_news[:10],
                "totalMentions": len(tdp_news),
                "sources": list(set([n.get('source', 'Unknown') for n in tdp_news]))[:5]
            },
            "trending": self._get_trending_topics(ysrcp_news + tdp_news + general_news),
            "lastUpdated": datetime.now().isoformat()
        }

        news_cache[cache_key] = result
        return result

    async def _fetch_rss_news(self, feed_type: str) -> List[Dict[str, Any]]:
        """Fetch news from Google News RSS"""
        try:
            url = self.rss_feeds.get(feed_type)
            if not url:
                return []

            feed = feedparser.parse(url)
            articles = []

            for entry in feed.entries[:20]:
                # Parse the entry
                article = {
                    "title": entry.get('title', ''),
                    "link": entry.get('link', ''),
                    "source": self._extract_source(entry),
                    "publishedAt": self._parse_date(entry.get('published', '')),
                    "description": self._clean_html(entry.get('summary', '')),
                    "party": self._classify_party(entry.get('title', '') + ' ' + entry.get('summary', ''))
                }
                articles.append(article)

            return articles

        except Exception as e:
            print(f"RSS fetch error for {feed_type}: {e}")
            return []

    async def _fetch_newsapi(self) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch from NewsAPI.org (if key available)"""
        if not self.news_api_key:
            return {"ysrcp": [], "tdp": []}

        result = {"ysrcp": [], "tdp": []}

        async with httpx.AsyncClient() as client:
            for party, keywords in [("ysrcp", YSRCP_KEYWORDS[:3]), ("tdp", TDP_KEYWORDS[:3])]:
                try:
                    query = " OR ".join(keywords)
                    url = f"https://newsapi.org/v2/everything"
                    params = {
                        "q": query,
                        "language": "en",
                        "sortBy": "publishedAt",
                        "pageSize": 10,
                        "apiKey": self.news_api_key
                    }

                    response = await client.get(url, params=params)
                    if response.status_code == 200:
                        data = response.json()
                        for article in data.get('articles', []):
                            result[party].append({
                                "title": article.get('title', ''),
                                "link": article.get('url', ''),
                                "source": article.get('source', {}).get('name', 'Unknown'),
                                "publishedAt": article.get('publishedAt', ''),
                                "description": article.get('description', ''),
                                "party": party
                            })
                except Exception as e:
                    print(f"NewsAPI error for {party}: {e}")

        return result

    def _extract_source(self, entry) -> str:
        """Extract source name from RSS entry"""
        # Google News format: "Title - Source Name"
        title = entry.get('title', '')
        if ' - ' in title:
            return title.split(' - ')[-1].strip()

        # Try source tag
        if hasattr(entry, 'source'):
            return entry.source.get('title', 'Unknown')

        return 'Google News'

    def _parse_date(self, date_str: str) -> str:
        """Parse date string to ISO format"""
        try:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(date_str)
            return dt.isoformat()
        except:
            return datetime.now().isoformat()

    def _clean_html(self, html_text: str) -> str:
        """Remove HTML tags from text"""
        if not html_text:
            return ""
        soup = BeautifulSoup(html_text, 'html.parser')
        return soup.get_text()[:200]

    def _classify_party(self, text: str) -> str:
        """Classify which party the article is about"""
        text_lower = text.lower()

        ysrcp_count = sum(1 for kw in YSRCP_KEYWORDS if kw.lower() in text_lower)
        tdp_count = sum(1 for kw in TDP_KEYWORDS if kw.lower() in text_lower)

        if ysrcp_count > tdp_count:
            return "ysrcp"
        elif tdp_count > ysrcp_count:
            return "tdp"
        return "neutral"

    def _deduplicate_news(self, articles: List[Dict]) -> List[Dict]:
        """Remove duplicate articles based on title similarity"""
        seen_titles = set()
        unique = []

        for article in articles:
            # Normalize title for comparison
            title_key = re.sub(r'[^\w\s]', '', article.get('title', '').lower())[:50]
            if title_key not in seen_titles:
                seen_titles.add(title_key)
                unique.append(article)

        # Sort by date (newest first)
        unique.sort(key=lambda x: x.get('publishedAt', ''), reverse=True)
        return unique

    def _get_trending_topics(self, articles: List[Dict]) -> List[Dict[str, Any]]:
        """Extract trending topics from news articles"""
        # Keywords frequency
        keywords = {}
        topic_keywords = [
            "welfare", "development", "election", "rally", "scheme",
            "Amaravati", "capital", "agriculture", "farmers", "education",
            "healthcare", "jobs", "employment", "corruption", "alliance"
        ]

        for article in articles:
            text = (article.get('title', '') + ' ' + article.get('description', '')).lower()
            for keyword in topic_keywords:
                if keyword.lower() in text:
                    keywords[keyword] = keywords.get(keyword, 0) + 1

        # Sort by frequency
        sorted_topics = sorted(keywords.items(), key=lambda x: x[1], reverse=True)

        return [{"topic": topic, "count": count} for topic, count in sorted_topics[:10]]

    async def get_news_stats(self) -> Dict[str, Any]:
        """Get news statistics for dashboard"""
        all_news = await self.get_all_news()

        return {
            "ysrcp": {
                "mentions": all_news['ysrcp']['totalMentions'],
                "reach": all_news['ysrcp']['totalMentions'] * 50000,  # Estimated reach
                "sentiment": {"positive": 52, "negative": 28, "neutral": 20}  # Would need sentiment analysis
            },
            "tdp": {
                "mentions": all_news['tdp']['totalMentions'],
                "reach": all_news['tdp']['totalMentions'] * 50000,
                "sentiment": {"positive": 48, "negative": 32, "neutral": 20}
            }
        }


# Singleton instance
news_service = NewsService()
