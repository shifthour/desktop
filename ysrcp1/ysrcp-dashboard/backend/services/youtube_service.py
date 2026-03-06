"""
YouTube Service for YSRCP Political Dashboard
Fetches trending videos related to YSRCP and TDP from YouTube using RapidAPI
"""

import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import urllib.request
import urllib.parse
import json

# RapidAPI Configuration
RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY', '922556e08bmsh465b2b5025c11a5p176967jsn3ca78cdb094c')
RAPIDAPI_HOST = 'youtube138.p.rapidapi.com'

# Official YouTube channel handles
YOUTUBE_CHANNELS = {
    'ysrcp': 'ysrcpofficial',  # Official YSRCP YouTube channel
    'tdp': 'TeluguDesamPartyOfficial'  # Official TDP YouTube channel
}

# Official YouTube channel IDs (for fetching subscriber counts)
YOUTUBE_CHANNEL_IDS = {
    'ysrcp': 'UC2asVumemMOELpHjmC9V7Dw',  # YSR Congress Party
    'tdp': 'UCvMZV13-yh2sUQY2s0Y5hlg'     # Telugu Desam Party Official
}

# Search keywords (used for supplementary searches)
YSRCP_KEYWORDS = ['YSRCP', 'YS Jagan', 'Jagan Mohan Reddy', 'ysrcpofficial']
TDP_KEYWORDS = ['TDP Chandrababu', 'Chandrababu Naidu', 'Telugu Desam Party', 'TeluguDesamPartyOfficial']
AP_KEYWORDS = ['Andhra Pradesh politics', 'AP politics']


class YouTubeService:
    def __init__(self):
        self.api_key = RAPIDAPI_KEY
        self.host = RAPIDAPI_HOST
        self.cache = {}
        self.cache_duration = timedelta(minutes=30)
        self.last_fetch = None

    def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Make a request to YouTube RapidAPI"""
        try:
            base_url = f"https://{self.host}/{endpoint}"
            if params:
                query_string = urllib.parse.urlencode(params)
                url = f"{base_url}/?{query_string}"
            else:
                url = base_url

            req = urllib.request.Request(url)
            req.add_header('x-rapidapi-host', self.host)
            req.add_header('x-rapidapi-key', self.api_key)

            with urllib.request.urlopen(req, timeout=15) as response:
                return json.loads(response.read().decode())
        except Exception as e:
            print(f"YouTube RapidAPI error: {e}")
            return None

    def _search_videos(self, query: str, max_results: int = 10) -> List[Dict]:
        """Search for videos by query using RapidAPI"""
        params = {
            'q': query,
            'hl': 'en',
            'gl': 'IN'
        }

        result = self._make_request('search', params)
        if not result:
            return []

        # Extract videos from the response (skip channels and other types)
        videos = []
        contents = result.get('contents', [])

        for item in contents:
            # Check if it's a video item
            if 'video' in item:
                video_data = item.get('video', {})
                if video_data and video_data.get('videoId'):
                    videos.append(video_data)
                    if len(videos) >= max_results:
                        break

        return videos

    def _format_count(self, count: int) -> str:
        """Format large numbers"""
        if count >= 10000000:
            return f"{count / 10000000:.1f}Cr"
        elif count >= 100000:
            return f"{count / 100000:.1f}L"
        elif count >= 1000:
            return f"{count / 1000:.1f}K"
        return str(count)

    def _parse_view_count(self, view_text: str) -> int:
        """Parse view count text like '1.2M views' to integer"""
        if not view_text:
            return 0
        try:
            # Remove 'views' and clean up
            text = view_text.lower().replace('views', '').replace(',', '').strip()

            multiplier = 1
            if 'k' in text:
                multiplier = 1000
                text = text.replace('k', '')
            elif 'm' in text:
                multiplier = 1000000
                text = text.replace('m', '')
            elif 'b' in text:
                multiplier = 1000000000
                text = text.replace('b', '')
            elif 'cr' in text:
                multiplier = 10000000
                text = text.replace('cr', '')
            elif 'l' in text or 'lakh' in text:
                multiplier = 100000
                text = text.replace('lakh', '').replace('l', '')

            return int(float(text.strip()) * multiplier)
        except:
            return 0

    def get_trending_videos(self, party: str = 'all') -> Dict[str, Any]:
        """Get trending videos for YSRCP, TDP, or both"""

        # Check cache
        cache_key = f"trending_{party}"
        if cache_key in self.cache:
            cached_data, cached_time = self.cache[cache_key]
            if datetime.now() - cached_time < self.cache_duration:
                return cached_data

        result = {
            'ysrcp': {'videos': [], 'totalViews': 0},
            'tdp': {'videos': [], 'totalViews': 0},
            'general': {'videos': [], 'totalViews': 0},
            'lastUpdated': datetime.now().isoformat(),
            'isLive': False
        }

        try:
            # Search for YSRCP videos - prioritize official channel
            if party in ['all', 'ysrcp']:
                ysrcp_videos = []
                # First search for official channel content
                ysrcp_videos.extend(self._search_videos(YOUTUBE_CHANNELS['ysrcp'], 15))
                # Then add keyword searches
                for keyword in YSRCP_KEYWORDS[:2]:
                    ysrcp_videos.extend(self._search_videos(keyword, 8))
                result['ysrcp'] = self._process_videos(ysrcp_videos, 'ysrcp')

            # Search for TDP videos - prioritize official channel
            if party in ['all', 'tdp']:
                tdp_videos = []
                # First search for official channel content
                tdp_videos.extend(self._search_videos(YOUTUBE_CHANNELS['tdp'], 15))
                # Then add keyword searches
                for keyword in TDP_KEYWORDS[:2]:
                    tdp_videos.extend(self._search_videos(keyword, 8))
                result['tdp'] = self._process_videos(tdp_videos, 'tdp')

            # Search for general AP politics
            if party == 'all':
                general_videos = self._search_videos('Andhra Pradesh politics news', 5)
                result['general'] = self._process_videos(general_videos, 'general')

            # Check if we got any videos - if not, return fallback
            has_videos = (
                len(result['ysrcp'].get('videos', [])) > 0 or
                len(result['tdp'].get('videos', [])) > 0
            )

            if not has_videos:
                print("YouTube API returned no videos, using fallback data")
                return self._get_fallback_data()

            result['isLive'] = True
            # Cache the result
            self.cache[cache_key] = (result, datetime.now())

        except Exception as e:
            print(f"Error fetching YouTube data: {e}")
            return self._get_fallback_data()

        return result

    def _process_videos(self, videos: List[Dict], party: str) -> Dict:
        """Process video search results from RapidAPI"""
        if not videos:
            return {'videos': [], 'totalViews': 0}

        # Remove duplicates
        seen_ids = set()
        unique_videos = []
        for v in videos:
            vid_id = v.get('videoId', '')
            if vid_id and vid_id not in seen_ids:
                seen_ids.add(vid_id)
                unique_videos.append(v)

        processed = []
        total_views = 0

        for video in unique_videos[:20]:
            vid_id = video.get('videoId', '')
            title = video.get('title', '')

            # Get channel info
            author = video.get('author', {})
            channel = author.get('title', '') if isinstance(author, dict) else str(author)
            channel_id = author.get('channelId', '') if isinstance(author, dict) else ''

            # Get thumbnail - thumbnails is an array of objects with url, width, height
            thumbnails = video.get('thumbnails', [])
            thumbnail = ''
            if thumbnails and isinstance(thumbnails, list):
                # Get the last thumbnail (usually highest quality)
                thumbnail = thumbnails[-1].get('url', '') if thumbnails[-1] else ''

            # Parse stats - can have 'views' for regular videos or 'viewers' for live
            stats = video.get('stats', {})
            if isinstance(stats, dict):
                # Try 'views' first, then 'viewers' for live videos
                view_text = stats.get('views') or stats.get('viewers') or '0'
            else:
                view_text = '0'
            views = self._parse_view_count(str(view_text))
            total_views += views

            # Check if live
            is_live = video.get('isLiveNow', False)

            # Get duration
            length_text = video.get('lengthSeconds', 0)
            if length_text and not is_live:
                try:
                    length_secs = int(length_text)
                    mins, secs = divmod(length_secs, 60)
                    hours, mins = divmod(mins, 60)
                    if hours > 0:
                        duration = f"{hours}:{mins:02d}:{secs:02d}"
                    else:
                        duration = f"{mins}:{secs:02d}"
                except:
                    duration = "0:00"
            elif is_live:
                duration = "LIVE"
            else:
                duration = video.get('lengthText', '0:00')

            # Get published time
            published = video.get('publishedTimeText', '')

            processed.append({
                'id': vid_id,
                'title': title,
                'channel': channel,
                'channelId': channel_id,
                'thumbnail': thumbnail,
                'publishedAt': published,
                'timeAgo': published,
                'views': views,
                'viewsFormatted': self._format_count(views) if views > 0 else ('LIVE' if is_live else '0'),
                'likes': 0,  # Not available in search results
                'likesFormatted': '0',
                'comments': 0,
                'commentsFormatted': '0',
                'duration': duration,
                'url': f"https://www.youtube.com/watch?v={vid_id}",
                'party': party,
                'isLive': is_live
            })

        # Sort by views (live videos will have lower view count but still show)
        processed.sort(key=lambda x: x['views'], reverse=True)

        return {
            'videos': processed,
            'totalViews': total_views,
            'totalViewsFormatted': self._format_count(total_views)
        }

    def _get_fallback_data(self) -> Dict[str, Any]:
        """Return fallback data when API is not available"""
        return {
            'ysrcp': {
                'videos': [
                    {
                        'id': 'sample1',
                        'title': 'YS Jagan Mohan Reddy Latest Speech at Pulivendula',
                        'channel': 'YSRCP Official',
                        'thumbnail': 'https://i.ytimg.com/vi/sample/mqdefault.jpg',
                        'publishedAt': (datetime.now() - timedelta(days=1)).isoformat(),
                        'views': 245000,
                        'viewsFormatted': '2.4L',
                        'likes': 12000,
                        'likesFormatted': '12K',
                        'comments': 890,
                        'commentsFormatted': '890',
                        'duration': '15:32',
                        'url': 'https://www.youtube.com/watch?v=sample1',
                        'party': 'ysrcp'
                    },
                    {
                        'id': 'sample2',
                        'title': 'YSRCP Leaders Press Meet on Cyclone Relief',
                        'channel': 'AP Political News',
                        'thumbnail': 'https://i.ytimg.com/vi/sample/mqdefault.jpg',
                        'publishedAt': (datetime.now() - timedelta(days=2)).isoformat(),
                        'views': 89000,
                        'viewsFormatted': '89K',
                        'likes': 5600,
                        'likesFormatted': '5.6K',
                        'comments': 432,
                        'commentsFormatted': '432',
                        'duration': '8:45',
                        'url': 'https://www.youtube.com/watch?v=sample2',
                        'party': 'ysrcp'
                    }
                ],
                'totalViews': 334000,
                'totalViewsFormatted': '3.3L'
            },
            'tdp': {
                'videos': [
                    {
                        'id': 'sample3',
                        'title': 'CM Chandrababu Naidu Review Meeting on Development',
                        'channel': 'TDP Official',
                        'thumbnail': 'https://i.ytimg.com/vi/sample/mqdefault.jpg',
                        'publishedAt': (datetime.now() - timedelta(days=1)).isoformat(),
                        'views': 198000,
                        'viewsFormatted': '1.9L',
                        'likes': 9800,
                        'likesFormatted': '9.8K',
                        'comments': 654,
                        'commentsFormatted': '654',
                        'duration': '12:18',
                        'url': 'https://www.youtube.com/watch?v=sample3',
                        'party': 'tdp'
                    },
                    {
                        'id': 'sample4',
                        'title': 'TDP MLA Speaks on Assembly Session',
                        'channel': 'Telugu News Channel',
                        'thumbnail': 'https://i.ytimg.com/vi/sample/mqdefault.jpg',
                        'publishedAt': (datetime.now() - timedelta(days=2)).isoformat(),
                        'views': 67000,
                        'viewsFormatted': '67K',
                        'likes': 3200,
                        'likesFormatted': '3.2K',
                        'comments': 287,
                        'commentsFormatted': '287',
                        'duration': '6:22',
                        'url': 'https://www.youtube.com/watch?v=sample4',
                        'party': 'tdp'
                    }
                ],
                'totalViews': 265000,
                'totalViewsFormatted': '2.6L'
            },
            'general': {
                'videos': [
                    {
                        'id': 'sample5',
                        'title': 'Andhra Pradesh Political News Update - Today',
                        'channel': 'AP News Live',
                        'thumbnail': 'https://i.ytimg.com/vi/sample/mqdefault.jpg',
                        'publishedAt': datetime.now().isoformat(),
                        'views': 156000,
                        'viewsFormatted': '1.5L',
                        'likes': 7800,
                        'likesFormatted': '7.8K',
                        'comments': 543,
                        'commentsFormatted': '543',
                        'duration': '10:15',
                        'url': 'https://www.youtube.com/watch?v=sample5',
                        'party': 'general'
                    }
                ],
                'totalViews': 156000,
                'totalViewsFormatted': '1.5L'
            },
            'lastUpdated': datetime.now().isoformat(),
            'isLive': False,
            'message': 'YouTube API key not configured. Showing sample data. Add YOUTUBE_API_KEY to enable real data.'
        }

    def get_channel_stats(self, channel_ids: List[str]) -> Dict[str, Dict]:
        """Get channel statistics"""
        if not self.api_key or not channel_ids:
            return {}

        params = {
            'part': 'statistics,snippet',
            'id': ','.join(channel_ids)
        }

        result = self._make_request('channels', params)
        if not result:
            return {}

        channels = {}
        for item in result.get('items', []):
            channels[item['id']] = {
                'title': item['snippet']['title'],
                'subscribers': int(item['statistics'].get('subscriberCount', 0)),
                'totalViews': int(item['statistics'].get('viewCount', 0)),
                'videoCount': int(item['statistics'].get('videoCount', 0)),
                'thumbnail': item['snippet']['thumbnails']['default']['url']
            }
        return channels


    def get_channel_details(self, channel_id: str) -> Optional[Dict]:
        """Get channel details including subscriber count"""
        cache_key = f"channel_{channel_id}"

        # Check cache
        if cache_key in self.cache:
            cached_data, cached_time = self.cache[cache_key]
            if datetime.now() - cached_time < self.cache_duration:
                return cached_data

        params = {
            'id': channel_id,
            'hl': 'en',
            'gl': 'IN'
        }

        result = self._make_request('channel/details', params)
        if not result:
            return None

        try:
            stats = result.get('stats', {})
            channel_data = {
                'channelId': result.get('channelId', channel_id),
                'title': result.get('title', ''),
                'description': result.get('description', ''),
                'subscribers': stats.get('subscribers', 0),
                'subscribersText': stats.get('subscribersText', '0'),
                'videos': stats.get('videos', 0),
                'views': stats.get('views', 0),
                'isVerified': result.get('isVerified', False),
                'avatar': result.get('avatar', [{}])[0].get('url', '') if result.get('avatar') else '',
                'country': result.get('country', ''),
                'joinedDate': result.get('joinedDate', '')
            }

            # Cache the result
            self.cache[cache_key] = (channel_data, datetime.now())
            return channel_data

        except Exception as e:
            print(f"Error parsing channel details: {e}")
            return None

    def get_party_stats(self) -> Dict[str, Any]:
        """Get real-time YouTube stats for both parties"""
        result = {
            'ysrcp': {
                'subscribers': 0,
                'videos': 0,
                'views': 0,
                'channel': None
            },
            'tdp': {
                'subscribers': 0,
                'videos': 0,
                'views': 0,
                'channel': None
            },
            'lastUpdated': datetime.now().isoformat(),
            'isLive': True
        }

        try:
            # Fetch YSRCP channel details
            ysrcp_channel = self.get_channel_details(YOUTUBE_CHANNEL_IDS['ysrcp'])
            if ysrcp_channel:
                result['ysrcp']['subscribers'] = ysrcp_channel.get('subscribers', 0)
                result['ysrcp']['videos'] = ysrcp_channel.get('videos', 0)
                result['ysrcp']['views'] = ysrcp_channel.get('views', 0)
                result['ysrcp']['channel'] = {
                    'id': ysrcp_channel.get('channelId', ''),
                    'title': ysrcp_channel.get('title', ''),
                    'avatar': ysrcp_channel.get('avatar', ''),
                    'isVerified': ysrcp_channel.get('isVerified', False)
                }

            # Fetch TDP channel details
            tdp_channel = self.get_channel_details(YOUTUBE_CHANNEL_IDS['tdp'])
            if tdp_channel:
                result['tdp']['subscribers'] = tdp_channel.get('subscribers', 0)
                result['tdp']['videos'] = tdp_channel.get('videos', 0)
                result['tdp']['views'] = tdp_channel.get('views', 0)
                result['tdp']['channel'] = {
                    'id': tdp_channel.get('channelId', ''),
                    'title': tdp_channel.get('title', ''),
                    'avatar': tdp_channel.get('avatar', ''),
                    'isVerified': tdp_channel.get('isVerified', False)
                }

        except Exception as e:
            print(f"Error fetching YouTube party stats: {e}")
            result['isLive'] = False
            result['error'] = str(e)

        return result

    def clear_cache(self):
        """Clear all cached data for fresh fetch"""
        self.cache = {}


# Singleton instance
youtube_service = YouTubeService()
