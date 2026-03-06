# 📁 Project Folders Explained

## 🆕 NEW Node.js Portfolio (LATEST - USE THIS!)

**Location:**
```
/Users/safestorage/Desktop/Istika-Branding/ishtika-portfolio-nodejs/
```

**Access:** http://localhost:3001

**Status:** ✅ **RUNNING NOW**

### ✨ Features Included (All from PHP version + More!)

✅ **Video Hero Section** - Auto-playing video with sound toggle button
✅ **Company Statistics** - 12+ years, 12+ projects, 5+ ongoing, 1K+ residents
✅ **About Section** - Company description with images
✅ **Ongoing Projects Slider** - Krishna, Vashishta, Naadam, Vyasa, Anahata
✅ **Completed Projects Gallery** - 9 projects with lightbox
✅ **Testimonials Slider** - 3 customer reviews
✅ **Contact Form** - With email sending capability
✅ **Google Maps** - Embedded location map
✅ **Mobile Navigation** - Responsive menu
✅ **Privacy Policy Page**
✅ **404 & Error Pages**

### 🎥 Video Feature Details
- **File:** `views/index.ejs` (lines 120-218)
- **Video Source:** `/assets/video.mp4` (from assets1)
- **Features:**
  - Auto-play on page load
  - Muted by default
  - Sound toggle button (top-left of video)
  - Pauses when scrolled out of view
  - Responsive design

### 📂 Key Files in This Project

```
ishtika-portfolio-nodejs/
├── server.js              ← Main server (Node.js/Express)
├── routes/index.js        ← ALL DATA HERE (edit this!)
├── views/
│   ├── index.ejs          ← Homepage with VIDEO
│   ├── about.ejs          ← About page
│   ├── projects.ejs       ← Projects page
│   ├── gallery.ejs        ← Gallery page
│   ├── contact.ejs        ← Contact form
│   └── partials/
│       ├── header.ejs     ← Navigation
│       └── footer.ejs     ← Footer
└── public/
    └── assets/            ← Symlink to assets1/assets
```

---

## 🗂️ OLD PHP Version (Reference Only)

**Location:**
```
/Users/safestorage/Desktop/Istika-Branding/assets1/
```

**Status:** 🔴 **NOT RUNNING** (Old PHP files for reference)

**Contents:**
- Old PHP files (index.php, about.php, etc.)
- All the assets (CSS, JS, images, video.mp4)
- PHPMailer library

**Purpose:**
- This is the ORIGINAL code I migrated FROM
- Keep for reference
- Assets are linked to new Node.js project

---

## 🎯 WHICH ONE TO USE?

### ✅ Use This: `ishtika-portfolio-nodejs/`
- **Technology:** Modern Node.js + Express
- **Status:** ✅ Running at http://localhost:3001
- **Features:** Everything from PHP + Better structure
- **Edit Content:** `routes/index.js`
- **Edit Design:** Files in `views/` folder

### 📚 Reference Only: `assets1/`
- Old PHP code (don't use)
- Assets are shared via symlink
- Keep for historical reference

---

## 🎥 Where is the Video?

The video feature is **FULLY IMPLEMENTED** in the Node.js version!

### Video Location in Code:
**File:** `/Users/safestorage/Desktop/Istika-Branding/ishtika-portfolio-nodejs/views/index.ejs`

**Lines:** 120-218 (Hero section)

### Video File Location:
**Actual File:** `/Users/safestorage/Desktop/Istika-Branding/assets1/assets/video.mp4`

**Accessed Via:** http://localhost:3001/assets/video.mp4

### How It Works:
1. Video auto-plays when page loads
2. Muted by default (browsers require this)
3. Click the sound button (🔊) on video to unmute
4. Video pauses when you scroll away
5. Responsive - works on mobile too!

---

## 🎨 What Else is on the Homepage?

When you visit http://localhost:3001, you'll see:

1. **Video Hero** - Full-width auto-playing video with stats
2. **About Section** - Company description with images
3. **Ongoing Projects** - 5 projects in a slider
4. **Completed Gallery** - 9 completed projects
5. **Testimonials** - Customer reviews slider
6. **Coming Soon** - Future projects preview
7. **Contact Form** - Property valuation request
8. **CTA Section** - Call-to-action buttons

---

## 📝 How to Edit the Video Section

### Change Video Source:
1. Open: `views/index.ejs`
2. Find line: `<source src="/assets/video.mp4" type="video/mp4">`
3. Replace `video.mp4` with your new video filename

### Modify Video Styles:
1. Open: `views/index.ejs`
2. Find the `<style>` section at the top (lines 3-110)
3. Edit CSS for `.toggleSwitch`, `.speaker`, etc.

### Change Stats Numbers:
1. Open: `routes/index.js`
2. Find `companyInfo.stats` object
3. Update the numbers

---

## 🚀 Quick Access URLs

**Homepage (with video):** http://localhost:3001
**About Page:** http://localhost:3001/about
**Projects:** http://localhost:3001/projects
**Gallery:** http://localhost:3001/gallery
**Contact:** http://localhost:3001/contact

---

## 💡 Summary

✅ **Your NEW Node.js portfolio is at:**
`/Users/safestorage/Desktop/Istika-Branding/ishtika-portfolio-nodejs/`

✅ **It includes EVERYTHING from the PHP version including:**
- ✅ Video hero with sound toggle
- ✅ All projects
- ✅ All pages
- ✅ All features

✅ **Currently running at:** http://localhost:3001

📁 **The `assets1` folder** is just the OLD PHP code for reference

---

**You're looking at the LATEST and BEST version! 🎉**
