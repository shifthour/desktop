"""
Twitter Service for YSRCP Political Dashboard
Uses RapidAPI Twitter241 to fetch tweets about YSRCP and TDP
"""

import os
import urllib.request
import urllib.parse
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# RapidAPI Configuration
RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY', '922556e08bmsh465b2b5025c11a5p176967jsn3ca78cdb094c')
RAPIDAPI_HOST = 'twitter241.p.rapidapi.com'

# Search keywords
YSRCP_KEYWORDS = ['YSRCP', 'YS Jagan', 'Jagan Mohan Reddy']
TDP_KEYWORDS = ['TDP', 'Chandrababu Naidu', 'Chandrababu']

# Official Twitter handles for stats
TWITTER_HANDLES = {
    'ysrcp': {
        'party': 'YSRCParty',  # Official YSRCP party handle
        'leader': None
    },
    'tdp': {
        'party': 'JaiTDP',  # Official TDP party handle
        'leader': None
    }
}


class TwitterService:
    def __init__(self):
        self.api_key = RAPIDAPI_KEY
        self.host = RAPIDAPI_HOST
        self.cache = {}
        self.cache_duration = timedelta(minutes=15)

    def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Make a request to Twitter API via RapidAPI"""
        try:
            base_url = f"https://{self.host}/{endpoint}"
            if params:
                query_string = urllib.parse.urlencode(params)
                url = f"{base_url}?{query_string}"
            else:
                url = base_url

            req = urllib.request.Request(url)
            req.add_header('x-rapidapi-host', self.host)
            req.add_header('x-rapidapi-key', self.api_key)

            with urllib.request.urlopen(req, timeout=15) as response:
                return json.loads(response.read().decode())
        except Exception as e:
            print(f"Twitter API error: {e}")
            return None

    def _parse_tweet(self, tweet_data: Dict, party: str = 'unknown') -> Optional[Dict]:
        """Parse a tweet from the API response"""
        try:
            # Handle different tweet structures
            result = tweet_data.get('tweet_results', {}).get('result', {})

            # Skip promoted tweets
            if result.get('__typename') == 'TweetWithVisibilityResults':
                result = result.get('tweet', {})

            if not result or result.get('__typename') not in ['Tweet', None]:
                return None

            # Get tweet ID
            tweet_id = result.get('rest_id', '')
            if not tweet_id:
                return None

            # Get user info
            user_result = result.get('core', {}).get('user_results', {}).get('result', {})
            user_legacy = user_result.get('legacy', {})
            user_core = user_result.get('core', {})

            user = {
                'id': user_result.get('rest_id', ''),
                'name': user_core.get('name', ''),
                'handle': user_core.get('screen_name', ''),
                'avatar': user_result.get('avatar', {}).get('image_url', '').replace('_normal', '_400x400'),
                'verified': user_result.get('is_blue_verified', False),
                'followers': user_legacy.get('followers_count', 0)
            }

            # Get tweet content
            legacy = result.get('legacy', {})
            text = legacy.get('full_text', '')

            # Get engagement metrics
            engagement = {
                'likes': legacy.get('favorite_count', 0),
                'retweets': legacy.get('retweet_count', 0),
                'replies': legacy.get('reply_count', 0),
                'quotes': legacy.get('quote_count', 0),
                'views': int(result.get('views', {}).get('count', 0) or 0)
            }

            # Get media
            media = []
            extended_media = legacy.get('extended_entities', {}).get('media', [])
            for m in extended_media:
                media_item = {
                    'type': m.get('type', 'photo'),
                    'url': m.get('media_url_https', ''),
                    'thumbnail': m.get('media_url_https', '')
                }

                # Get video URL if available
                if m.get('type') == 'video':
                    variants = m.get('video_info', {}).get('variants', [])
                    # Get highest quality MP4
                    mp4_variants = [v for v in variants if v.get('content_type') == 'video/mp4']
                    if mp4_variants:
                        mp4_variants.sort(key=lambda x: x.get('bitrate', 0), reverse=True)
                        media_item['video_url'] = mp4_variants[0].get('url', '')

                media.append(media_item)

            # Parse timestamp
            created_at = legacy.get('created_at', '')
            timestamp = None
            if created_at:
                try:
                    timestamp = datetime.strptime(created_at, '%a %b %d %H:%M:%S %z %Y')
                except:
                    timestamp = datetime.now()

            # Determine party affiliation from content
            text_lower = text.lower()
            if any(kw.lower() in text_lower for kw in YSRCP_KEYWORDS):
                party = 'ysrcp'
            elif any(kw.lower() in text_lower for kw in TDP_KEYWORDS):
                party = 'tdp'
            else:
                party = 'general'

            return {
                'id': tweet_id,
                'text': text,
                'user': user,
                'engagement': engagement,
                'media': media,
                'hasMedia': len(media) > 0,
                'hasVideo': any(m['type'] == 'video' for m in media),
                'timestamp': timestamp.isoformat() if timestamp else None,
                'timeAgo': self._get_time_ago(timestamp) if timestamp else '',
                'url': f"https://twitter.com/{user['handle']}/status/{tweet_id}",
                'party': party,
                'lang': legacy.get('lang', 'en')
            }
        except Exception as e:
            print(f"Error parsing tweet: {e}")
            return None

    def _get_time_ago(self, timestamp: datetime) -> str:
        """Get human-readable time ago string"""
        now = datetime.now(timestamp.tzinfo) if timestamp.tzinfo else datetime.now()
        diff = now - timestamp

        if diff.days > 0:
            return f"{diff.days}d ago"
        hours = diff.seconds // 3600
        if hours > 0:
            return f"{hours}h ago"
        minutes = diff.seconds // 60
        if minutes > 0:
            return f"{minutes}m ago"
        return "Just now"

    def search_tweets(self, query: str, count: int = 20, search_type: str = 'Latest') -> List[Dict]:
        """Search for tweets by query"""
        cache_key = f"search_{query}_{count}_{search_type}"

        # Check cache
        if cache_key in self.cache:
            cached_data, cached_time = self.cache[cache_key]
            if datetime.now() - cached_time < self.cache_duration:
                return cached_data

        params = {
            'query': query,
            'type': search_type,
            'count': count
        }

        response = self._make_request('search', params)
        if not response:
            return []

        tweets = []
        try:
            instructions = response.get('result', {}).get('timeline', {}).get('instructions', [])
            for instruction in instructions:
                if instruction.get('type') == 'TimelineAddEntries':
                    entries = instruction.get('entries', [])
                    for entry in entries:
                        content = entry.get('content', {})
                        if content.get('entryType') == 'TimelineTimelineItem':
                            item_content = content.get('itemContent', {})
                            if item_content.get('itemType') == 'TimelineTweet':
                                tweet = self._parse_tweet(item_content, query)
                                if tweet:
                                    tweets.append(tweet)
        except Exception as e:
            print(f"Error parsing search results: {e}")

        # Cache results
        self.cache[cache_key] = (tweets, datetime.now())
        return tweets

    def get_trending_tweets(self, party: str = 'all') -> Dict[str, Any]:
        """Get trending tweets for YSRCP, TDP, or both"""
        result = {
            'ysrcp': {'tweets': [], 'totalEngagement': 0},
            'tdp': {'tweets': [], 'totalEngagement': 0},
            'combined': [],
            'lastUpdated': datetime.now().isoformat(),
            'isLive': True
        }

        try:
            # Fetch YSRCP tweets
            if party in ['all', 'ysrcp']:
                ysrcp_tweets = self.search_tweets('YSRCP', count=15)
                result['ysrcp']['tweets'] = ysrcp_tweets
                result['ysrcp']['totalEngagement'] = sum(
                    t['engagement']['likes'] + t['engagement']['retweets']
                    for t in ysrcp_tweets
                )

            # Fetch TDP tweets
            if party in ['all', 'tdp']:
                tdp_tweets = self.search_tweets('TDP Chandrababu', count=15)
                result['tdp']['tweets'] = tdp_tweets
                result['tdp']['totalEngagement'] = sum(
                    t['engagement']['likes'] + t['engagement']['retweets']
                    for t in tdp_tweets
                )

            # Combine and sort by engagement
            all_tweets = result['ysrcp']['tweets'] + result['tdp']['tweets']
            all_tweets.sort(key=lambda x: x['engagement']['likes'] + x['engagement']['retweets'], reverse=True)
            result['combined'] = all_tweets[:20]

        except Exception as e:
            print(f"Error fetching trending tweets: {e}")
            result['isLive'] = False
            result['error'] = str(e)

        return result

    def get_trending_topics(self) -> List[Dict]:
        """Get trending topics (hashtags) from recent tweets"""
        cache_key = "trending_topics"

        # Check cache
        if cache_key in self.cache:
            cached_data, cached_time = self.cache[cache_key]
            if datetime.now() - cached_time < self.cache_duration:
                return cached_data

        # Fetch recent tweets for both parties
        ysrcp_tweets = self.search_tweets('YSRCP', count=50)
        tdp_tweets = self.search_tweets('TDP Chandrababu', count=50)

        all_tweets = ysrcp_tweets + tdp_tweets

        # Extract hashtags with sentiment tracking
        hashtag_counts = {}
        for tweet in all_tweets:
            text = tweet.get('text', '')
            words = text.split()
            party = tweet.get('party', 'general')
            engagement = tweet.get('engagement', {})
            likes = engagement.get('likes', 0)
            retweets = engagement.get('retweets', 0)

            for word in words:
                if word.startswith('#') and len(word) > 1:
                    # Clean the hashtag (remove trailing punctuation)
                    clean_tag = word.rstrip('.,!?:;')
                    tag_lower = clean_tag.lower()

                    if tag_lower not in hashtag_counts:
                        # Determine party from hashtag content itself
                        ysrcp_tags = ['ysrcp', 'ysjagan', 'jagan', 'jagananna', 'jaganmohan', 'ysrcongress']
                        tdp_tags = ['tdp', 'chandrababu', 'naidu', 'lokesh', 'naralokesh', 'telugudesam']

                        tag_party = 'general'
                        tag_clean_lower = tag_lower.replace('#', '')
                        if any(yt in tag_clean_lower for yt in ysrcp_tags):
                            tag_party = 'ysrcp'
                        elif any(tt in tag_clean_lower for tt in tdp_tags):
                            tag_party = 'tdp'

                        hashtag_counts[tag_lower] = {
                            'tag': clean_tag,
                            'count': 0,
                            'engagement': 0,
                            'party': tag_party,
                            'positive_engagement': 0,
                            'negative_keywords': 0
                        }

                    hashtag_counts[tag_lower]['count'] += 1
                    hashtag_counts[tag_lower]['engagement'] += likes + retweets

                    # Track positive engagement (high likes = positive sentiment)
                    if likes > 100:
                        hashtag_counts[tag_lower]['positive_engagement'] += 1

                    # Check for negative keywords in tweet text
                    negative_words = ['fail', 'scam', 'corrupt', 'arrest', 'against', 'protest', 'fraud']
                    text_lower = text.lower()
                    if any(neg in text_lower for neg in negative_words):
                        hashtag_counts[tag_lower]['negative_keywords'] += 1

        # Process and add sentiment
        trending = []
        for tag_data in hashtag_counts.values():
            # Determine sentiment based on engagement and negative keywords
            if tag_data['negative_keywords'] > tag_data['count'] * 0.3:
                sentiment = 'negative'
            elif tag_data['positive_engagement'] > tag_data['count'] * 0.3:
                sentiment = 'positive'
            else:
                sentiment = 'neutral'

            # Mark top hashtags as trending
            is_trending = tag_data['count'] >= 3 or tag_data['engagement'] > 500

            trending.append({
                'tag': tag_data['tag'],
                'count': tag_data['count'],
                'engagement': tag_data['engagement'],
                'party': tag_data['party'],
                'sentiment': sentiment,
                'trending': is_trending
            })

        # Sort by count
        trending = sorted(trending, key=lambda x: x['count'], reverse=True)[:15]

        # Cache results
        self.cache[cache_key] = (trending, datetime.now())

        return trending

    def get_user_profile(self, username: str, skip_cache: bool = False) -> Optional[Dict]:
        """Get user profile data including follower count"""
        cache_key = f"user_{username}"

        # Check cache unless skip_cache is True
        if not skip_cache and cache_key in self.cache:
            cached_data, cached_time = self.cache[cache_key]
            if datetime.now() - cached_time < self.cache_duration:
                return cached_data

        response = self._make_request('user', {'username': username})
        if not response:
            return None

        try:
            user_data = response.get('result', {}).get('data', {}).get('user', {}).get('result', {})
            if not user_data:
                return None

            legacy = user_data.get('legacy', {})
            core = user_data.get('core', {})
            avatar = user_data.get('avatar', {})

            profile = {
                'id': user_data.get('rest_id', ''),
                'username': core.get('screen_name', username),
                'name': core.get('name', ''),
                'description': legacy.get('description', ''),
                'followers': legacy.get('followers_count', 0),
                'following': legacy.get('friends_count', 0),
                'tweets': legacy.get('statuses_count', 0),
                'likes': legacy.get('favourites_count', 0),
                'verified': user_data.get('is_blue_verified', False),
                'avatar': avatar.get('image_url', '').replace('_normal', '_400x400') if avatar else '',
                'banner': legacy.get('profile_banner_url', ''),
                'location': user_data.get('location', {}).get('location', '') if user_data.get('location') else '',
                'created_at': core.get('created_at', ''),
                'url': f"https://twitter.com/{core.get('screen_name', username)}"
            }

            # Cache results
            self.cache[cache_key] = (profile, datetime.now())
            return profile

        except Exception as e:
            print(f"Error parsing user profile: {e}")
            return None

    def clear_cache(self):
        """Clear all cached data for fresh fetch"""
        self.cache = {}

    def get_party_stats(self) -> Dict[str, Any]:
        """Get real-time Twitter stats for both parties"""
        result = {
            'ysrcp': {
                'followers': 0,
                'tweets': 0,
                'engagement': 0,
                'accounts': []
            },
            'tdp': {
                'followers': 0,
                'tweets': 0,
                'engagement': 0,
                'accounts': []
            },
            'lastUpdated': datetime.now().isoformat(),
            'isLive': True
        }

        try:
            # Fetch YSRCP official party profile
            ysrcp_party = self.get_user_profile(TWITTER_HANDLES['ysrcp']['party'])
            if ysrcp_party:
                result['ysrcp']['followers'] = ysrcp_party['followers']
                result['ysrcp']['tweets'] = ysrcp_party['tweets']
                result['ysrcp']['accounts'].append({
                    'handle': ysrcp_party['username'],
                    'name': ysrcp_party['name'],
                    'followers': ysrcp_party['followers'],
                    'verified': ysrcp_party['verified'],
                    'avatar': ysrcp_party['avatar'],
                    'type': 'party'
                })

            # Fetch TDP official party profile
            tdp_party = self.get_user_profile(TWITTER_HANDLES['tdp']['party'])
            if tdp_party:
                result['tdp']['followers'] = tdp_party['followers']
                result['tdp']['tweets'] = tdp_party['tweets']
                result['tdp']['accounts'].append({
                    'handle': tdp_party['username'],
                    'name': tdp_party['name'],
                    'followers': tdp_party['followers'],
                    'verified': tdp_party['verified'],
                    'avatar': tdp_party['avatar'],
                    'type': 'party'
                })

            # Calculate engagement from recent tweets
            ysrcp_tweets = self.search_tweets('YSRCP', count=20)
            tdp_tweets = self.search_tweets('TDP Chandrababu', count=20)

            result['ysrcp']['engagement'] = sum(
                t['engagement']['likes'] + t['engagement']['retweets']
                for t in ysrcp_tweets
            )
            result['tdp']['engagement'] = sum(
                t['engagement']['likes'] + t['engagement']['retweets']
                for t in tdp_tweets
            )

        except Exception as e:
            print(f"Error fetching party stats: {e}")
            result['isLive'] = False
            result['error'] = str(e)

        return result

    def get_influencers(self) -> Dict[str, Any]:
        """Get top influencers from Twitter based on recent tweet activity"""
        cache_key = "influencers"

        # Check cache
        if cache_key in self.cache:
            cached_data, cached_time = self.cache[cache_key]
            if datetime.now() - cached_time < self.cache_duration:
                return cached_data

        result = {
            'influencers': [],
            'stats': {
                'totalReach': 0,
                'totalMentions': 0,
                'proYsrcp': 0,
                'proTdp': 0,
                'neutral': 0
            },
            'lastUpdated': datetime.now().isoformat(),
            'isLive': True
        }

        try:
            # Fetch tweets from both parties
            ysrcp_tweets = self.search_tweets('YSRCP', count=50)
            tdp_tweets = self.search_tweets('TDP Chandrababu', count=50)

            all_tweets = ysrcp_tweets + tdp_tweets

            # Extract unique users with their stats
            users = {}
            for tweet in all_tweets:
                user = tweet.get('user', {})
                user_id = user.get('id', '')
                if not user_id:
                    continue

                if user_id not in users:
                    users[user_id] = {
                        'id': user_id,
                        'name': user.get('name', ''),
                        'handle': f"@{user.get('handle', '')}",
                        'followers': user.get('followers', 0),
                        'avatar': user.get('avatar', ''),
                        'verified': user.get('verified', False),
                        'platform': 'twitter',
                        'recentMentions': 0,
                        'engagement': 0,
                        'ysrcpMentions': 0,
                        'tdpMentions': 0
                    }

                users[user_id]['recentMentions'] += 1
                users[user_id]['engagement'] += tweet['engagement']['likes'] + tweet['engagement']['retweets']

                # Track party mentions
                party = tweet.get('party', 'general')
                if party == 'ysrcp':
                    users[user_id]['ysrcpMentions'] += 1
                elif party == 'tdp':
                    users[user_id]['tdpMentions'] += 1

            # Determine sentiment for each user
            for user_id, user in users.items():
                if user['ysrcpMentions'] > user['tdpMentions'] * 1.5:
                    user['sentiment'] = 'pro-ysrcp'
                    result['stats']['proYsrcp'] += 1
                elif user['tdpMentions'] > user['ysrcpMentions'] * 1.5:
                    user['sentiment'] = 'pro-tdp'
                    result['stats']['proTdp'] += 1
                else:
                    user['sentiment'] = 'neutral'
                    result['stats']['neutral'] += 1

            # Sort by followers and get top influencers
            sorted_users = sorted(users.values(), key=lambda x: x['followers'], reverse=True)
            top_influencers = sorted_users[:12]  # Top 12 influencers

            result['influencers'] = top_influencers
            result['stats']['totalReach'] = sum(u['followers'] for u in top_influencers)
            result['stats']['totalMentions'] = sum(u['recentMentions'] for u in top_influencers)

        except Exception as e:
            print(f"Error fetching influencers: {e}")
            result['isLive'] = False
            result['error'] = str(e)

        # Cache results
        self.cache[cache_key] = (result, datetime.now())
        return result


# Singleton instance
twitter_service = TwitterService()
