# 🎥 Video Demo Modal - Setup Guide

## ✅ Watch Demo Button is Now Working!

The "Watch Demo" button now opens a beautiful modal with a video player.

---

## 🎬 Current Setup

### Default Video
Currently using a placeholder YouTube video. The modal will:
- Open with smooth animation when clicking "Watch Demo"
- Display a full-screen video player
- Auto-play the video
- Close when clicking:
  - The X button (top right)
  - Outside the modal (on the dark overlay)
  - Pressing ESC key

---

## 🔧 How to Add Your Own Demo Video

### Option 1: YouTube Video

1. **Upload your demo video to YouTube**

2. **Get the video ID from the URL:**
   - YouTube URL: `https://www.youtube.com/watch?v=YOUR_VIDEO_ID`
   - Video ID is: `YOUR_VIDEO_ID`

3. **Update the JavaScript file:**

Open `/web-app/js/app.js` and find line 100:

```javascript
// Current (line 100):
const demoVideoUrl = 'https://www.youtube.com/embed/dQw4w9WgXcQ?autoplay=1&rel=0';

// Replace with your video:
const demoVideoUrl = 'https://www.youtube.com/embed/YOUR_VIDEO_ID?autoplay=1&rel=0';
```

**Example:**
If your YouTube URL is: `https://www.youtube.com/watch?v=abc123xyz`

Use:
```javascript
const demoVideoUrl = 'https://www.youtube.com/embed/abc123xyz?autoplay=1&rel=0';
```

### Option 2: Vimeo Video

1. **Upload to Vimeo**

2. **Get the video ID**

3. **Update the JavaScript:**

```javascript
const demoVideoUrl = 'https://player.vimeo.com/video/YOUR_VIMEO_ID?autoplay=1';
```

### Option 3: Self-Hosted Video (MP4)

1. **Place your video file in the assets folder:**
   - `/web-app/assets/demo-video.mp4`

2. **Update the HTML:**

Open `/web-app/index.html` and replace the iframe (lines 706-715) with:

```html
<div class="video-container">
    <video id="demoVideo" controls autoplay width="100%" height="100%">
        <source src="assets/demo-video.mp4" type="video/mp4">
        Your browser does not support the video tag.
    </video>
</div>
```

3. **Update the JavaScript:**

Replace the video modal functions in `js/app.js`:

```javascript
function openVideoModal() {
    if (videoModal && demoVideo) {
        videoModal.classList.add('active');
        document.body.style.overflow = 'hidden';

        // Play video
        demoVideo.play();

        trackEvent('Demo', 'Watch Video', 'Hero Section');
    }
}

function closeVideoModalFunc() {
    if (videoModal && demoVideo) {
        videoModal.classList.remove('active');
        document.body.style.overflow = '';

        // Pause and reset video
        demoVideo.pause();
        demoVideo.currentTime = 0;
    }
}
```

---

## 🎨 Customization Options

### Change Modal Size

Edit `/web-app/css/styles.css` (line 1304):

```css
.video-modal-content {
    width: 90%;  /* Change this (e.g., 80%, 95%, etc.) */
    max-width: 1200px;  /* Change max width */
    /* ... */
}
```

### Change Close Button Color

Edit `/web-app/css/styles.css` (line 1321):

```css
.video-modal-close {
    background: var(--danger-color);  /* Change to any color */
    /* ... */
}
```

### Change Modal Title and Description

Edit `/web-app/index.html` (lines 717-719):

```html
<div class="video-info">
    <h3>Your Custom Title</h3>
    <p>Your custom description</p>
</div>
```

### Disable Auto-play

Remove `?autoplay=1` from the video URL:

```javascript
// With autoplay (current):
const demoVideoUrl = 'https://www.youtube.com/embed/abc123?autoplay=1&rel=0';

// Without autoplay:
const demoVideoUrl = 'https://www.youtube.com/embed/abc123?rel=0';
```

### Change Video Aspect Ratio

Edit `/web-app/css/styles.css` (line 1350):

```css
.video-container {
    padding-bottom: 56.25%; /* 16:9 ratio */
    /* For 4:3 use: 75% */
    /* For 21:9 use: 42.86% */
}
```

---

## 🎯 Video URL Parameters

### YouTube Parameters

Add these to your YouTube URL:

```
?autoplay=1          → Auto-play video
&rel=0              → Don't show related videos
&controls=0         → Hide video controls
&modestbranding=1   → Hide YouTube logo
&loop=1             → Loop video
&mute=1             → Start muted
```

**Example:**
```javascript
const demoVideoUrl = 'https://www.youtube.com/embed/abc123?autoplay=1&rel=0&modestbranding=1';
```

### Vimeo Parameters

```
?autoplay=1         → Auto-play
&loop=1            → Loop video
&muted=1           → Start muted
&title=0           → Hide title
&byline=0          → Hide author
&portrait=0        → Hide author portrait
```

---

## 🧪 Testing the Video Modal

### Steps to Test:

1. **Open the website:**
   ```
   http://localhost:8000
   ```

2. **Click "Watch Demo" button** in the hero section

3. **Verify:**
   - Modal opens with smooth animation
   - Video plays (if autoplay is enabled)
   - Background is blurred/darkened
   - Close button (X) appears top-right
   - Can close by:
     - Clicking X button
     - Clicking outside modal
     - Pressing ESC key
   - Video stops when modal closes

### Browser Console

Check for errors:
1. Press F12 (or Cmd+Option+I on Mac)
2. Go to Console tab
3. Should see: `Track Event: {category: "Demo", action: "Watch Video", label: "Hero Section"}`

---

## 📱 Mobile Considerations

### Already Included:
- ✅ Responsive modal (95% width on mobile)
- ✅ Smaller close button on mobile
- ✅ Touch-friendly overlay close
- ✅ Proper video scaling

### Mobile-Specific Settings:

For better mobile experience, consider:

```javascript
// Disable autoplay on mobile
const isMobile = /iPhone|iPad|iPod|Android/i.test(navigator.userAgent);
const autoplayParam = isMobile ? '' : '?autoplay=1';
const demoVideoUrl = `https://www.youtube.com/embed/abc123${autoplayParam}&rel=0`;
```

---

## 🔍 Troubleshooting

### Video Not Playing?

**Issue:** Video doesn't load
**Solution:**
- Check if video URL is correct
- Ensure video is public (not private)
- Check browser console for errors

**Issue:** Autoplay blocked by browser
**Solution:**
- Some browsers block autoplay with sound
- Add `&mute=1` to URL for muted autoplay
- Or remove `?autoplay=1` to require manual play

### Modal Not Opening?

**Issue:** Button click does nothing
**Solution:**
1. Hard refresh: Cmd+Shift+R (Mac) or Ctrl+Shift+R
2. Check browser console for JavaScript errors
3. Verify all files are saved

### Modal Styling Issues?

**Issue:** Modal looks broken
**Solution:**
1. Clear browser cache
2. Hard refresh the page
3. Verify CSS file loaded (check Network tab in DevTools)

### ESC Key Not Working?

**Issue:** ESC doesn't close modal
**Solution:**
- Make sure modal is in focus
- Check if another script is capturing ESC
- Refresh page and try again

---

## 📊 Video Analytics

The modal already tracks when users watch the demo:

```javascript
trackEvent('Demo', 'Watch Video', 'Hero Section');
```

To see these events:
1. Check browser console
2. When ready, integrate with:
   - Google Analytics
   - Mixpanel
   - Custom analytics service

---

## 🎬 Creating a Demo Video

### Recommended Content (30-90 seconds):

1. **Intro (5s):** "Welcome to IHW"
2. **Problem (10s):** Show pain points
3. **Solution (30s):**
   - Activity tracking
   - Health records
   - AI insights
4. **Benefits (15s):** Key advantages
5. **CTA (5s):** "Get started today"

### Tools to Create Demo:

- **Screen Recording:**
  - Mac: QuickTime, ScreenFlow
  - Windows: OBS Studio, Camtasia
  - Online: Loom, Screencastify

- **Video Editing:**
  - Basic: iMovie, Windows Video Editor
  - Advanced: Final Cut Pro, Adobe Premiere
  - Online: Canva Video, Kapwing

- **Stock Footage:**
  - Pexels Videos (free)
  - Unsplash Videos (free)
  - Envato Elements (paid)

### Video Specs:

- **Resolution:** 1920x1080 (1080p) minimum
- **Format:** MP4 (H.264 codec)
- **Length:** 30-90 seconds recommended
- **File Size:** < 50MB for self-hosted
- **Aspect Ratio:** 16:9

---

## ✅ Quick Setup Checklist

- [ ] Upload demo video to YouTube/Vimeo
- [ ] Get video ID from URL
- [ ] Update video URL in `js/app.js` (line 100)
- [ ] Test by clicking "Watch Demo" button
- [ ] Verify video plays correctly
- [ ] Test close functionality (X, overlay, ESC)
- [ ] Test on mobile devices
- [ ] Update modal title/description if needed

---

## 🚀 Next Steps

1. **Create your demo video**
2. **Upload to YouTube or Vimeo**
3. **Update the video URL**
4. **Test thoroughly**
5. **Deploy to production**

---

## 📞 Need Help?

Check these resources:
- `README.md` - Full documentation
- `QUICKSTART.md` - Quick start guide
- Browser DevTools (F12) - Debug issues

---

**The Watch Demo button is now fully functional! 🎉**

Just add your actual demo video URL and you're ready to go!

---

*Last Updated: October 17, 2025*
