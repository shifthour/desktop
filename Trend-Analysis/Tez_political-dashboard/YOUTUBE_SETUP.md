# YouTube API Setup Guide

## ✅ YouTube API Integration Complete!

Your dashboard now has **real-time YouTube trending videos** tracking for YSRCP vs TDP!

---

## 🚀 Quick Start

### Step 1: Get YouTube API Key (5 minutes - FREE!)

1. **Go to Google Cloud Console**
   - Visit: https://console.cloud.google.com/

2. **Create a Project**
   - Click "Select a Project" → "New Project"
   - Name it: "Political Analytics" or any name you prefer
   - Click "Create"

3. **Enable YouTube Data API v3**
   - In the search bar, type: "YouTube Data API v3"
   - Click on "YouTube Data API v3"
   - Click "Enable"

4. **Create API Key**
   - Go to "Credentials" (left sidebar)
   - Click "Create Credentials"
   - Select "API Key"
   - Copy the API key that appears

5. **Add API Key to .env**
   - Open `/Tez_political-dashboard/.env`
   - Replace `your_youtube_api_key_here` with your actual API key:
   ```
   YOUTUBE_API_KEY=AIzaSyBXXXXXXXXXXXXXXXXXXXXX
   ```

### Step 2: Access the YouTube Trending Page

1. Open your browser and go to: **http://localhost:3000**

2. Click the red **"📺 YouTube Trending"** button

3. You'll see real trending videos from both YSRCP and TDP!

---

## 📺 What You'll See

### Features:
- ✅ **Trending Videos** for both YSRCP and TDP
- ✅ **Official Channel Videos** from party YouTube channels
- ✅ **Real View Counts** from YouTube API
- ✅ **Engagement Metrics** (likes, comments, views)
- ✅ **Video Thumbnails** with clickable links
- ✅ **Filter by Party** (YSRCP / TDP / All / Official)
- ✅ **Summary Statistics** (total views, video counts)

### Video Information Displayed:
- Video title
- Channel name
- View count
- Likes count
- Comments count
- Party affiliation badge
- Official channel badge (if from official source)
- Direct link to watch on YouTube

---

## 🔍 How It Works

### Search Keywords:
The system searches for videos using these keywords:

**YSRCP:**
- YSRCP
- YS Jagan
- Jagan Mohan Reddy
- YSR Congress
- CM Jagan
- వైఎస్సార్సీపీ (Telugu)

**TDP:**
- TDP
- Chandrababu Naidu
- CBN
- Telugu Desam Party
- Nara Chandrababu
- టీడీపీ (Telugu)

### Data Sources:
1. **Trending Videos**: Searches YouTube for recent videos (last 30 days) using party keywords
2. **Official Channels**: Fetches latest videos from official party YouTube channels

---

## 📊 API Endpoints Available

You can also access the data programmatically:

### 1. Get All Trending Videos
```
GET http://localhost:3000/api/youtube/trending
```

Returns trending videos for both parties with complete statistics.

### 2. Get Top Videos
```
GET http://localhost:3000/api/youtube/top-videos?limit=10
```

Returns top 10 videos across both parties sorted by views.

### 3. Get Party-Specific Videos
```
GET http://localhost:3000/api/youtube/ysrcp
GET http://localhost:3000/api/youtube/tdp
```

Returns videos for a specific party.

---

## 💰 API Costs & Limits

### YouTube API Quota:
- **FREE**: 10,000 units per day
- **Cost per search**: ~100 units
- **Cost per video details**: ~1 unit per video

### Estimated Usage:
- Each page load: ~300-500 units
- You can refresh **20-30 times per day** within free quota
- Quota resets daily at midnight Pacific Time

### If You Exceed Quota:
- Wait until next day for quota reset
- Or upgrade to paid tier (not necessary for most use cases)
- Reduce `maxResults` in the code

---

## 🔧 Customization

### Change Search Keywords:
Edit `src/collectors/youtubeCollector.js`:
```javascript
this.partyKeywords = {
  ysrcp: ['Your', 'Custom', 'Keywords'],
  tdp: ['Your', 'Custom', 'Keywords']
};
```

### Change Official Channels:
Edit `src/collectors/youtubeCollector.js`:
```javascript
this.officialChannels = {
  ysrcp: ['CHANNEL_ID_HERE'],
  tdp: ['CHANNEL_ID_HERE']
};
```

To find a channel ID:
1. Go to the YouTube channel
2. View page source (Ctrl+U)
3. Search for "channelId"

### Change Time Range:
In `getTrendingVideos()` method, change:
```javascript
publishedAfter: this.getDateDaysAgo(30) // Change 30 to any number of days
```

---

## 🐛 Troubleshooting

### "Failed to fetch YouTube data"
**Solution**: Add your YouTube API key to `.env` file

### "Quota exceeded"
**Solution**: Wait 24 hours or reduce refresh frequency

### "No videos found"
**Possible causes**:
- Invalid API key
- No recent videos matching keywords
- API quota exceeded
- Keywords too specific

### Videos not loading
1. Check browser console for errors (F12)
2. Verify API key is correct in `.env`
3. Make sure YouTube Data API v3 is enabled in Google Cloud
4. Check server console for error messages

---

## 📈 Next Steps

### Add More Platforms:
- Instagram (use instagrapi - no API key needed)
- Twitter/X ($100/month required)
- Facebook (requires app approval)

### Enhance Analytics:
- Track video performance over time
- Compare engagement rates
- Identify viral content
- Sentiment analysis on comments

---

## 🎯 Pro Tips

1. **Refresh strategically**: Don't refresh too often to save API quota
2. **Monitor trending**: Check 2-3 times per day for best results
3. **Official channels**: Filter by "Official Channels" for verified content
4. **Save quota**: Use filters instead of refreshing entire page

---

## ✅ Success Checklist

- [ ] YouTube API key added to `.env`
- [ ] Server restarted
- [ ] Can access http://localhost:3000/youtube.html
- [ ] Videos are loading with real data
- [ ] Filters are working (YSRCP/TDP/All/Official)
- [ ] Can click videos to open on YouTube

---

**Your YouTube integration is complete and ready to use!** 🎉

For questions or issues, check the browser console (F12) and server logs.
