"""
Facebook Service for YSRCP Political Dashboard
Uses RapidAPI Facebook Scraper 3 to fetch page details and posts about YSRCP and TDP
"""

import os
import urllib.request
import urllib.parse
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# RapidAPI Configuration - Facebook Scraper 3
RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY', '922556e08bmsh465b2b5025c11a5p176967jsn3ca78cdb094c')
RAPIDAPI_HOST = 'facebook-scraper3.p.rapidapi.com'

# Official Facebook page URLs (for page/details endpoint)
FACEBOOK_PAGE_URLS = {
    'ysrcp': 'https://www.facebook.com/ysrcpofficial',
    'tdp': 'https://www.facebook.com/TDP.Official'
}


class FacebookService:
    def __init__(self):
        self.api_key = RAPIDAPI_KEY
        self.host = RAPIDAPI_HOST
        self.cache = {}
        self.cache_duration = timedelta(minutes=30)
        # Cache for page_ids to avoid repeated lookups
        self.page_id_cache = {}

    def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Make a GET request to Facebook Scraper 3 API via RapidAPI"""
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

            print(f"[Facebook API] Requesting: {endpoint} with params: {params}")
            with urllib.request.urlopen(req, timeout=30) as response:
                data = json.loads(response.read().decode())
                return data
        except urllib.error.HTTPError as e:
            print(f"Facebook API HTTP error: {e.code} - {e.reason}")
            try:
                error_body = e.read().decode()
                print(f"Error body: {error_body}")
            except:
                pass
            return None
        except Exception as e:
            print(f"Facebook API error: {e}")
            return None

    def get_page_details(self, page_url: str) -> Optional[Dict]:
        """Get page details including follower count and page_id using Facebook Scraper 3"""
        cache_key = f"fb_page_details_{page_url}"

        # Check cache
        if cache_key in self.cache:
            cached_data, cached_time = self.cache[cache_key]
            if datetime.now() - cached_time < self.cache_duration:
                return cached_data

        response = self._make_request('page/details', {'url': page_url})

        if not response:
            return None

        results = response.get('results', {})
        if not results:
            return None

        page_details = {
            'name': results.get('name', ''),
            'page_id': results.get('page_id', ''),
            'followers': results.get('followers', 0),
            'following': results.get('following', 0),
            'likes': results.get('likes', 0),
            'image': results.get('image', ''),
            'cover_image': results.get('cover_image', ''),
            'intro': results.get('intro', ''),
            'verified': results.get('verified', False),
            'categories': results.get('categories', []),
            'website': results.get('website', ''),
            'url': results.get('url', page_url)
        }

        # Cache the page_id for quick lookup
        if page_details['page_id']:
            self.page_id_cache[page_url] = page_details['page_id']

        # Cache the result
        self.cache[cache_key] = (page_details, datetime.now())
        return page_details

    def get_page_id(self, page_url: str) -> Optional[str]:
        """Get page_id from URL - uses cache or fetches page details"""
        # Check page_id cache first
        if page_url in self.page_id_cache:
            return self.page_id_cache[page_url]

        # Fetch page details to get page_id
        details = self.get_page_details(page_url)
        if details and details.get('page_id'):
            return details['page_id']

        return None

    def _format_count(self, count: int) -> str:
        """Format large numbers"""
        if count >= 10000000:
            return f"{count / 10000000:.1f}Cr"
        elif count >= 100000:
            return f"{count / 100000:.1f}L"
        elif count >= 1000:
            return f"{count / 1000:.1f}K"
        return str(count)

    def _parse_time_ago(self, timestamp: str) -> str:
        """Convert timestamp to human-readable time ago"""
        if not timestamp:
            return ''
        try:
            # Try parsing various formats
            for fmt in ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d']:
                try:
                    dt = datetime.strptime(timestamp.split('.')[0].split('+')[0], fmt)
                    diff = datetime.now() - dt

                    if diff.days > 30:
                        return f"{diff.days // 30}mo ago"
                    elif diff.days > 0:
                        return f"{diff.days}d ago"
                    elif diff.seconds > 3600:
                        return f"{diff.seconds // 3600}h ago"
                    elif diff.seconds > 60:
                        return f"{diff.seconds // 60}m ago"
                    else:
                        return "Just now"
                except ValueError:
                    continue
            return timestamp
        except:
            return timestamp

    def _parse_post(self, post_data: Dict, party: str = 'unknown', page_name: str = '') -> Optional[Dict]:
        """Parse a post from the page/posts API response"""
        try:
            post_id = post_data.get('post_id', '')
            if not post_id:
                return None

            # Get message/content
            message = post_data.get('message', '') or post_data.get('message_rich', '') or ''

            # Get media info
            media_type = post_data.get('type', 'post')
            image_data = post_data.get('image')
            video_data = post_data.get('video')
            has_media = bool(image_data) or bool(video_data)

            # Get engagement metrics - API returns *_count fields as integers
            reactions = post_data.get('reactions_count', 0) or 0
            comments = post_data.get('comments_count', 0) or 0
            shares = post_data.get('reshare_count', 0) or 0
            views = 0  # Not provided in this API

            # Get timestamp - API returns Unix timestamp as integer
            timestamp_raw = post_data.get('timestamp', 0)
            if isinstance(timestamp_raw, int) and timestamp_raw > 0:
                timestamp = datetime.fromtimestamp(timestamp_raw).isoformat()
                time_ago = self._parse_time_ago(timestamp)
            else:
                timestamp = ''
                time_ago = ''

            # Get media URLs - image/video are dicts with 'uri' key
            image_url = ''
            if isinstance(image_data, dict):
                image_url = image_data.get('uri', '')
            video_url = ''
            if isinstance(video_data, dict):
                video_url = video_data.get('uri', '') or video_data.get('url', '')

            # Get author info - author is a dict with 'name', 'id', 'url'
            author_data = post_data.get('author', {})
            author_name = author_data.get('name', '') if isinstance(author_data, dict) else ''
            author_image = author_data.get('profile_picture', '') if isinstance(author_data, dict) else ''

            if not author_name:
                author_name = page_name or party.upper()

            return {
                'id': post_id,
                'message': message[:200] + '...' if len(message) > 200 else message,
                'fullMessage': message,
                'reactions': reactions,
                'likes': reactions,
                'reactionsFormatted': self._format_count(reactions),
                'comments': comments,
                'commentsFormatted': self._format_count(comments),
                'shares': shares,
                'sharesFormatted': self._format_count(shares),
                'views': views,
                'viewsFormatted': self._format_count(views),
                'mediaType': media_type,
                'imageUrl': image_url,
                'videoUrl': video_url,
                'thumbnail': image_url,
                'hasVideo': bool(video_data),
                'hasMedia': has_media,
                'author': author_name,
                'authorImage': author_image,
                'isVerified': True,  # Official pages are verified
                'timestamp': timestamp,
                'timeAgo': time_ago,
                'url': post_data.get('url', ''),
                'party': party
            }
        except Exception as e:
            print(f"Error parsing Facebook post: {e}")
            import traceback
            traceback.print_exc()
            return None

    def get_page_posts(self, page_url: str, party: str = 'unknown') -> List[Dict]:
        """Get posts from a Facebook page using page_id"""
        cache_key = f"fb_page_posts_{page_url}"

        # Check cache
        if cache_key in self.cache:
            cached_data, cached_time = self.cache[cache_key]
            if datetime.now() - cached_time < self.cache_duration:
                return cached_data

        # First get the page_id
        page_id = self.get_page_id(page_url)
        if not page_id:
            print(f"[Facebook] Could not get page_id for {page_url}")
            return []

        # Get page name for author field
        details = self.get_page_details(page_url)
        page_name = details.get('name', '') if details else ''

        # Now fetch posts using page_id
        print(f"[Facebook] Fetching posts for page_id: {page_id}")
        response = self._make_request('page/posts', {'page_id': page_id})

        if not response:
            print(f"[Facebook] No response for page posts")
            return []

        posts = []
        # Get posts from response - could be in 'results' or 'data'
        post_list = response.get('results', []) or response.get('data', [])

        if isinstance(post_list, dict):
            # Sometimes API returns dict with posts inside
            post_list = post_list.get('posts', []) or post_list.get('data', [])

        print(f"[Facebook] Found {len(post_list)} posts for {party}")

        for post_data in post_list:
            post = self._parse_post(post_data, party, page_name)
            if post:
                posts.append(post)

        # Cache results
        if posts:
            self.cache[cache_key] = (posts, datetime.now())

        return posts

    def get_trending_posts(self, party: str = 'all') -> Dict[str, Any]:
        """Get trending Facebook posts for YSRCP, TDP, or both from official pages"""
        result = {
            'ysrcp': {'posts': [], 'totalEngagement': 0},
            'tdp': {'posts': [], 'totalEngagement': 0},
            'combined': [],
            'lastUpdated': datetime.now().isoformat(),
            'isLive': False
        }

        try:
            # Fetch YSRCP posts from official page
            if party in ['all', 'ysrcp']:
                ysrcp_posts = self.get_page_posts(FACEBOOK_PAGE_URLS['ysrcp'], 'ysrcp')

                if ysrcp_posts:
                    # Sort by engagement and take top 20
                    ysrcp_posts.sort(key=lambda x: x['reactions'] + x['comments'] + x['shares'], reverse=True)
                    result['ysrcp']['posts'] = ysrcp_posts[:20]
                    result['ysrcp']['totalEngagement'] = sum(
                        p['reactions'] + p['comments'] + p['shares'] for p in ysrcp_posts[:20]
                    )
                    result['isLive'] = True
                    print(f"[Facebook] YSRCP: {len(ysrcp_posts)} posts, engagement: {result['ysrcp']['totalEngagement']}")

            # Fetch TDP posts from official page
            if party in ['all', 'tdp']:
                tdp_posts = self.get_page_posts(FACEBOOK_PAGE_URLS['tdp'], 'tdp')

                if tdp_posts:
                    # Sort by engagement and take top 20
                    tdp_posts.sort(key=lambda x: x['reactions'] + x['comments'] + x['shares'], reverse=True)
                    result['tdp']['posts'] = tdp_posts[:20]
                    result['tdp']['totalEngagement'] = sum(
                        p['reactions'] + p['comments'] + p['shares'] for p in tdp_posts[:20]
                    )
                    result['isLive'] = True
                    print(f"[Facebook] TDP: {len(tdp_posts)} posts, engagement: {result['tdp']['totalEngagement']}")

            # Combine and sort by engagement
            all_posts = result['ysrcp']['posts'] + result['tdp']['posts']
            all_posts.sort(key=lambda x: x['reactions'] + x['comments'] + x['shares'], reverse=True)
            result['combined'] = all_posts[:40]

        except Exception as e:
            print(f"Error fetching Facebook trending posts: {e}")

        return result

    def get_party_stats(self) -> Dict[str, Any]:
        """Get Facebook stats for both parties including real follower counts"""
        trending = self.get_trending_posts()

        # Get real follower counts from Facebook Scraper 3
        ysrcp_followers = 0
        tdp_followers = 0
        ysrcp_details = None
        tdp_details = None
        is_live = False

        try:
            # Fetch YSRCP page details
            ysrcp_details = self.get_page_details(FACEBOOK_PAGE_URLS['ysrcp'])
            if ysrcp_details:
                ysrcp_followers = ysrcp_details.get('followers', 0)
                is_live = True
                print(f"[Facebook] YSRCP followers: {ysrcp_followers}")

            # Fetch TDP page details
            tdp_details = self.get_page_details(FACEBOOK_PAGE_URLS['tdp'])
            if tdp_details:
                tdp_followers = tdp_details.get('followers', 0)
                is_live = True
                print(f"[Facebook] TDP followers: {tdp_followers}")

        except Exception as e:
            print(f"Error fetching Facebook page details: {e}")

        return {
            'ysrcp': {
                'followers': ysrcp_followers,
                'followersFormatted': self._format_count(ysrcp_followers),
                'posts': len(trending['ysrcp']['posts']),
                'engagement': trending['ysrcp']['totalEngagement'],
                'engagementFormatted': self._format_count(trending['ysrcp']['totalEngagement']),
                'pageDetails': ysrcp_details
            },
            'tdp': {
                'followers': tdp_followers,
                'followersFormatted': self._format_count(tdp_followers),
                'posts': len(trending['tdp']['posts']),
                'engagement': trending['tdp']['totalEngagement'],
                'engagementFormatted': self._format_count(trending['tdp']['totalEngagement']),
                'pageDetails': tdp_details
            },
            'lastUpdated': datetime.now().isoformat(),
            'isLive': is_live or trending['isLive']
        }


    def clear_cache(self):
        """Clear all cached data for fresh fetch"""
        self.cache = {}
        self.page_id_cache = {}


# Singleton instance
facebook_service = FacebookService()
