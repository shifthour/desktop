import { google } from 'googleapis';
import dotenv from 'dotenv';

dotenv.config();

class YouTubeCollector {
  constructor() {
    this.apiKey = process.env.YOUTUBE_API_KEY;
    this.youtube = google.youtube({
      version: 'v3',
      auth: this.apiKey
    });

    this.partyKeywords = {
      ysrcp: [
        'YSRCP',
        'YS Jagan',
        'Jagan Mohan Reddy',
        'YSR Congress',
        'CM Jagan',
        'వైఎస్సార్సీపీ'
      ],
      tdp: [
        'TDP',
        'Chandrababu Naidu',
        'CBN',
        'Telugu Desam Party',
        'Nara Chandrababu',
        'టీడీపీ'
      ]
    };

    // Official YouTube channels
    this.officialChannels = {
      ysrcp: [
        'UCt5NqQZiNKHWJXHqKaQQxfA', // YSRCP Official
        'UCBjNGOH1kGQZ5hGxIcXcUeg'  // YS Jagan Official
      ],
      tdp: [
        'UCvSl9QezYu7rQpEp2YaZmUg', // TDP Official
        'UCE_Q3pZqYJ5qLJZXU8KqPkg'  // Chandrababu Naidu
      ]
    };
  }

  /**
   * Get trending videos for a specific party
   */
  async getTrendingVideos(party, maxResults = 25) {
    if (!this.apiKey) {
      console.warn('YouTube API key not configured. Using empty results.');
      return [];
    }

    try {
      const videos = [];
      const keywords = this.partyKeywords[party];

      // Search for videos using party keywords
      for (const keyword of keywords.slice(0, 4)) { // Use 4 keywords for better coverage
        try {
          const searchResponse = await this.youtube.search.list({
            part: 'snippet',
            q: `${keyword} Andhra Pradesh`,
            type: 'video',
            maxResults: Math.min(maxResults, 10),
            order: 'viewCount',
            publishedAfter: this.getDateDaysAgo(30), // Last 30 days
            regionCode: 'IN',
            relevanceLanguage: 'te' // Telugu
          });

          if (searchResponse.data.items) {
            const videoIds = searchResponse.data.items.map(item => item.id.videoId);

            // Get detailed statistics for these videos
            const statsResponse = await this.youtube.videos.list({
              part: 'statistics,contentDetails,snippet',
              id: videoIds.join(',')
            });

            if (statsResponse.data.items) {
              const processedVideos = statsResponse.data.items.map(video => ({
                videoId: video.id,
                title: video.snippet.title,
                description: video.snippet.description,
                thumbnail: video.snippet.thumbnails.high?.url || video.snippet.thumbnails.default?.url,
                channelTitle: video.snippet.channelTitle,
                channelId: video.snippet.channelId,
                publishedAt: video.snippet.publishedAt,
                views: parseInt(video.statistics.viewCount) || 0,
                likes: parseInt(video.statistics.likeCount) || 0,
                comments: parseInt(video.statistics.commentCount) || 0,
                duration: this.parseDuration(video.contentDetails.duration),
                url: `https://www.youtube.com/watch?v=${video.id}`,
                party: party,
                keyword: keyword,
                platform: 'youtube'
              }));

              videos.push(...processedVideos);
            }
          }
        } catch (error) {
          console.error(`Error searching for keyword "${keyword}":`, error.message);
        }
      }

      // Remove duplicates and sort by views
      const uniqueVideos = this.removeDuplicates(videos, 'videoId');
      return uniqueVideos.sort((a, b) => b.views - a.views).slice(0, maxResults);

    } catch (error) {
      console.error(`Error getting trending videos for ${party}:`, error.message);
      return [];
    }
  }

  /**
   * Get videos from official party channels
   */
  async getOfficialChannelVideos(party, maxResults = 10) {
    if (!this.apiKey) {
      return [];
    }

    try {
      const videos = [];
      const channelIds = this.officialChannels[party] || [];

      for (const channelId of channelIds) {
        try {
          // Get uploads playlist ID
          const channelResponse = await this.youtube.channels.list({
            part: 'contentDetails',
            id: channelId
          });

          if (channelResponse.data.items && channelResponse.data.items.length > 0) {
            const uploadsPlaylistId = channelResponse.data.items[0].contentDetails.relatedPlaylists.uploads;

            // Get videos from uploads playlist
            const playlistResponse = await this.youtube.playlistItems.list({
              part: 'snippet',
              playlistId: uploadsPlaylistId,
              maxResults: maxResults
            });

            if (playlistResponse.data.items) {
              const videoIds = playlistResponse.data.items.map(item => item.snippet.resourceId.videoId);

              // Get statistics
              const statsResponse = await this.youtube.videos.list({
                part: 'statistics,contentDetails,snippet',
                id: videoIds.join(',')
              });

              if (statsResponse.data.items) {
                const processedVideos = statsResponse.data.items.map(video => ({
                  videoId: video.id,
                  title: video.snippet.title,
                  description: video.snippet.description,
                  thumbnail: video.snippet.thumbnails.high?.url || video.snippet.thumbnails.default?.url,
                  channelTitle: video.snippet.channelTitle,
                  channelId: video.snippet.channelId,
                  publishedAt: video.snippet.publishedAt,
                  views: parseInt(video.statistics.viewCount) || 0,
                  likes: parseInt(video.statistics.likeCount) || 0,
                  comments: parseInt(video.statistics.commentCount) || 0,
                  duration: this.parseDuration(video.contentDetails.duration),
                  url: `https://www.youtube.com/watch?v=${video.id}`,
                  party: party,
                  isOfficial: true,
                  platform: 'youtube'
                }));

                videos.push(...processedVideos);
              }
            }
          }
        } catch (error) {
          console.error(`Error getting videos for channel ${channelId}:`, error.message);
        }
      }

      return videos.sort((a, b) => b.views - a.views);

    } catch (error) {
      console.error(`Error getting official channel videos for ${party}:`, error.message);
      return [];
    }
  }

  /**
   * Get comprehensive data for both parties
   */
  async getAllPartyVideos() {
    console.log('Fetching YouTube trending videos...');

    const [ysrcpTrending, tdpTrending, ysrcpOfficial, tdpOfficial] = await Promise.all([
      this.getTrendingVideos('ysrcp', 20),
      this.getTrendingVideos('tdp', 20),
      this.getOfficialChannelVideos('ysrcp', 10),
      this.getOfficialChannelVideos('tdp', 10)
    ]);

    const result = {
      ysrcp: {
        trending: ysrcpTrending,
        official: ysrcpOfficial,
        totalVideos: ysrcpTrending.length + ysrcpOfficial.length,
        totalViews: [...ysrcpTrending, ...ysrcpOfficial].reduce((sum, v) => sum + v.views, 0),
        totalEngagement: [...ysrcpTrending, ...ysrcpOfficial].reduce((sum, v) => sum + v.likes + v.comments, 0)
      },
      tdp: {
        trending: tdpTrending,
        official: tdpOfficial,
        totalVideos: tdpTrending.length + tdpOfficial.length,
        totalViews: [...tdpTrending, ...tdpOfficial].reduce((sum, v) => sum + v.views, 0),
        totalEngagement: [...tdpTrending, ...tdpOfficial].reduce((sum, v) => sum + v.likes + v.comments, 0)
      },
      timestamp: new Date().toISOString()
    };

    console.log(`YouTube data fetched: YSRCP ${result.ysrcp.totalVideos} videos, TDP ${result.tdp.totalVideos} videos`);

    return result;
  }

  /**
   * Get top videos across both parties
   */
  async getTopVideosComparison(limit = 10) {
    const allData = await this.getAllPartyVideos();

    const allVideos = [
      ...allData.ysrcp.trending,
      ...allData.ysrcp.official,
      ...allData.tdp.trending,
      ...allData.tdp.official
    ];

    return allVideos
      .sort((a, b) => b.views - a.views)
      .slice(0, limit);
  }

  /**
   * Helper: Parse ISO 8601 duration to seconds
   */
  parseDuration(duration) {
    const match = duration.match(/PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?/);
    if (!match) return 0;

    const hours = parseInt(match[1]) || 0;
    const minutes = parseInt(match[2]) || 0;
    const seconds = parseInt(match[3]) || 0;

    return hours * 3600 + minutes * 60 + seconds;
  }

  /**
   * Helper: Get ISO date string for N days ago
   */
  getDateDaysAgo(days) {
    const date = new Date();
    date.setDate(date.getDate() - days);
    return date.toISOString();
  }

  /**
   * Helper: Remove duplicate items from array
   */
  removeDuplicates(array, key) {
    const seen = new Set();
    return array.filter(item => {
      const value = item[key];
      if (seen.has(value)) {
        return false;
      }
      seen.add(value);
      return true;
    });
  }

  /**
   * Format duration in seconds to readable format
   */
  formatDuration(seconds) {
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = seconds % 60;

    if (hours > 0) {
      return `${hours}:${minutes.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
    }
    return `${minutes}:${secs.toString().padStart(2, '0')}`;
  }
}

export default YouTubeCollector;
