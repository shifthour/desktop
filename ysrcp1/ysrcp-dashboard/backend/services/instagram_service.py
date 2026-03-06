"""
Instagram Service for YSRCP Political Dashboard
Uses RapidAPI Instagram120 to fetch posts about YSRCP and TDP
"""

import os
import urllib.request
import urllib.parse
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# RapidAPI Configuration
RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY', '922556e08bmsh465b2b5025c11a5p176967jsn3ca78cdb094c')
RAPIDAPI_HOST = 'instagram120.p.rapidapi.com'

# Official Instagram handles for political accounts
INSTAGRAM_HANDLES = {
    'ysrcp': {
        'party': 'ysrcongress',  # Official YSRCP Instagram handle
        'leader': None
    },
    'tdp': {
        'party': 'jai_tdp',  # Official TDP Instagram handle
        'leader': None
    }
}


class InstagramService:
    def __init__(self):
        self.api_key = RAPIDAPI_KEY
        self.host = RAPIDAPI_HOST
        self.cache = {}
        self.cache_duration = timedelta(minutes=30)

    def _make_request(self, endpoint: str, data: Dict) -> Optional[Dict]:
        """Make a POST request to Instagram API via RapidAPI"""
        try:
            url = f"https://{self.host}/api/instagram/{endpoint}"

            json_data = json.dumps(data).encode('utf-8')
            req = urllib.request.Request(url, data=json_data, method='POST')
            req.add_header('Content-Type', 'application/json')
            req.add_header('x-rapidapi-host', self.host)
            req.add_header('x-rapidapi-key', self.api_key)

            with urllib.request.urlopen(req, timeout=15) as response:
                return json.loads(response.read().decode())
        except Exception as e:
            print(f"Instagram API error: {e}")
            return None

    def get_user_profile(self, username: str) -> Optional[Dict]:
        """Get user profile including follower count"""
        cache_key = f"profile_{username}"

        # Check cache
        if cache_key in self.cache:
            cached_data, cached_time = self.cache[cache_key]
            if datetime.now() - cached_time < self.cache_duration:
                return cached_data

        response = self._make_request('profile', {'username': username})

        if not response:
            return None

        result = response.get('result', {})
        if not result:
            return None

        profile = {
            'id': result.get('id', ''),
            'username': result.get('username', username),
            'full_name': result.get('full_name', ''),
            'biography': result.get('biography', ''),
            'followers': result.get('edge_followed_by', {}).get('count', 0),
            'following': result.get('edge_follow', {}).get('count', 0),
            'posts_count': result.get('edge_owner_to_timeline_media', {}).get('count', 0),
            'profile_pic': result.get('profile_pic_url_hd', ''),
            'is_private': result.get('is_private', False),
            'is_verified': result.get('is_verified', False)
        }

        # Cache the result
        self.cache[cache_key] = (profile, datetime.now())
        return profile

    def _parse_post(self, post_data: Dict, party: str = 'unknown') -> Optional[Dict]:
        """Parse a post from the API response"""
        try:
            node = post_data.get('node', {})

            # Get post ID and code
            post_id = node.get('pk', '')
            code = node.get('code', '')

            if not post_id:
                return None

            # Get caption
            caption_data = node.get('caption', {})
            caption = caption_data.get('text', '') if caption_data else ''

            # Get engagement metrics
            likes = node.get('like_count', 0)
            comments = node.get('comment_count', 0)

            # Get media
            media_type = 'image'
            thumbnail = ''
            video_url = None

            if node.get('is_video'):
                media_type = 'video'
                video_url = node.get('video_url', '')

            # Get thumbnail
            image_versions = node.get('image_versions2', {})
            candidates = image_versions.get('candidates', [])
            if candidates:
                thumbnail = candidates[0].get('url', '')

            # Get user info
            user = node.get('user', {})
            owner = {
                'username': user.get('username', ''),
                'full_name': user.get('full_name', ''),
                'profile_pic': user.get('profile_pic_url', ''),
                'is_verified': user.get('is_verified', False)
            }

            # Parse timestamp
            taken_at = node.get('taken_at', 0)
            timestamp = datetime.fromtimestamp(taken_at) if taken_at else datetime.now()

            return {
                'id': post_id,
                'code': code,
                'caption': caption[:200] + '...' if len(caption) > 200 else caption,
                'fullCaption': caption,
                'likes': likes,
                'likesFormatted': self._format_count(likes),
                'comments': comments,
                'commentsFormatted': self._format_count(comments),
                'mediaType': media_type,
                'thumbnail': thumbnail,
                'videoUrl': video_url,
                'owner': owner,
                'timestamp': timestamp.isoformat(),
                'timeAgo': self._get_time_ago(timestamp),
                'url': f"https://www.instagram.com/p/{code}/",
                'party': party
            }
        except Exception as e:
            print(f"Error parsing Instagram post: {e}")
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

    def _get_time_ago(self, timestamp: datetime) -> str:
        """Get human-readable time ago string"""
        now = datetime.now()
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

    def get_user_posts(self, username: str, max_id: str = "") -> List[Dict]:
        """Get posts from a user"""
        cache_key = f"posts_{username}"

        # Check cache
        if cache_key in self.cache:
            cached_data, cached_time = self.cache[cache_key]
            if datetime.now() - cached_time < self.cache_duration:
                return cached_data

        response = self._make_request('posts', {
            'username': username,
            'maxId': max_id
        })

        if not response or not response.get('success', True) == True:
            if response and response.get('response_type') == 'private page':
                return []
            return []

        posts = []
        edges = response.get('result', {}).get('edges', [])

        for edge in edges:
            post = self._parse_post(edge, 'unknown')
            if post:
                posts.append(post)

        # Cache results
        self.cache[cache_key] = (posts, datetime.now())
        return posts

    def get_trending_posts(self, party: str = 'all') -> Dict[str, Any]:
        """Get trending posts for YSRCP, TDP, or both"""
        result = {
            'ysrcp': {'posts': [], 'totalEngagement': 0},
            'tdp': {'posts': [], 'totalEngagement': 0},
            'combined': [],
            'lastUpdated': datetime.now().isoformat(),
            'isLive': True
        }

        try:
            # Fetch YSRCP posts
            if party in ['all', 'ysrcp']:
                ysrcp_handle = INSTAGRAM_HANDLES['ysrcp']['party']
                if ysrcp_handle:
                    ysrcp_posts = self.get_user_posts(ysrcp_handle)
                    for post in ysrcp_posts:
                        post['party'] = 'ysrcp'
                    result['ysrcp']['posts'] = ysrcp_posts
                    result['ysrcp']['totalEngagement'] = sum(
                        p['likes'] + p['comments'] for p in ysrcp_posts
                    )

            # Fetch TDP posts
            if party in ['all', 'tdp']:
                tdp_handle = INSTAGRAM_HANDLES['tdp']['party']
                if tdp_handle:
                    tdp_posts = self.get_user_posts(tdp_handle)
                    for post in tdp_posts:
                        post['party'] = 'tdp'
                    result['tdp']['posts'] = tdp_posts
                    result['tdp']['totalEngagement'] = sum(
                        p['likes'] + p['comments'] for p in tdp_posts
                    )

            # Combine and sort by engagement
            all_posts = result['ysrcp']['posts'] + result['tdp']['posts']
            all_posts.sort(key=lambda x: x['likes'] + x['comments'], reverse=True)
            result['combined'] = all_posts[:40]

        except Exception as e:
            print(f"Error fetching Instagram trending posts: {e}")
            result['isLive'] = False
            result['error'] = str(e)

        return result

    def get_party_stats(self) -> Dict[str, Any]:
        """Get Instagram stats for both parties including real follower counts"""
        result = {
            'ysrcp': {
                'followers': 0,
                'followersFormatted': '0',
                'posts': 0,
                'engagement': 0,
                'accounts': []
            },
            'tdp': {
                'followers': 0,
                'followersFormatted': '0',
                'posts': 0,
                'engagement': 0,
                'accounts': []
            },
            'lastUpdated': datetime.now().isoformat(),
            'isLive': True
        }

        try:
            # YSRCP stats - get profile with follower count
            ysrcp_handle = INSTAGRAM_HANDLES['ysrcp']['party']
            if ysrcp_handle:
                ysrcp_profile = self.get_user_profile(ysrcp_handle)
                if ysrcp_profile:
                    result['ysrcp']['followers'] = ysrcp_profile.get('followers', 0)
                    result['ysrcp']['followersFormatted'] = self._format_count(ysrcp_profile.get('followers', 0))
                    result['ysrcp']['posts'] = ysrcp_profile.get('posts_count', 0)
                    result['ysrcp']['accounts'].append({
                        'handle': ysrcp_handle,
                        'name': ysrcp_profile.get('full_name', ysrcp_handle),
                        'followers': ysrcp_profile.get('followers', 0),
                        'profile_pic': ysrcp_profile.get('profile_pic', ''),
                        'type': 'party'
                    })

                # Get engagement from posts
                ysrcp_posts = self.get_user_posts(ysrcp_handle)
                result['ysrcp']['engagement'] = sum(p['likes'] + p['comments'] for p in ysrcp_posts)

            # TDP stats - get profile with follower count
            tdp_handle = INSTAGRAM_HANDLES['tdp']['party']
            if tdp_handle:
                tdp_profile = self.get_user_profile(tdp_handle)
                if tdp_profile:
                    result['tdp']['followers'] = tdp_profile.get('followers', 0)
                    result['tdp']['followersFormatted'] = self._format_count(tdp_profile.get('followers', 0))
                    result['tdp']['posts'] = tdp_profile.get('posts_count', 0)
                    result['tdp']['accounts'].append({
                        'handle': tdp_handle,
                        'name': tdp_profile.get('full_name', tdp_handle),
                        'followers': tdp_profile.get('followers', 0),
                        'profile_pic': tdp_profile.get('profile_pic', ''),
                        'type': 'party'
                    })

                # Get engagement from posts
                tdp_posts = self.get_user_posts(tdp_handle)
                result['tdp']['engagement'] = sum(p['likes'] + p['comments'] for p in tdp_posts)

        except Exception as e:
            print(f"Error fetching Instagram stats: {e}")
            result['isLive'] = False
            result['error'] = str(e)

        return result


    def clear_cache(self):
        """Clear all cached data for fresh fetch"""
        self.cache = {}


# Singleton instance
instagram_service = InstagramService()
