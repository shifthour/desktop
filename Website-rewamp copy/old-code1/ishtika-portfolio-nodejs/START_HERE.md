# ✅ Server is Running!

## 🌐 Access Your Website

Your Ishtika Homes portfolio is now live at:

**http://localhost:3001**

The browser should have opened automatically. If not, click the link above or manually navigate to it.

---

## 📁 Project Location

```
/Users/safestorage/Desktop/Istika-Branding/ishtika-portfolio-nodejs/
```

---

## 🚀 Quick Commands

### Start the Server (Method 1 - Easiest)
```bash
cd /Users/safestorage/Desktop/Istika-Branding/ishtika-portfolio-nodejs
./start.sh
```

### Start the Server (Method 2)
```bash
cd /Users/safestorage/Desktop/Istika-Branding/ishtika-portfolio-nodejs
npm run dev
```

### Start the Server (Method 3)
```bash
cd /Users/safestorage/Desktop/Istika-Branding/ishtika-portfolio-nodejs
PORT=3001 node server.js
```

### Stop the Server
Press `Ctrl + C` in the terminal

Or kill the process:
```bash
lsof -ti:3001 | xargs kill -9
```

---

## 📄 Available Pages

| Page | URL |
|------|-----|
| Homepage | http://localhost:3001/ |
| About Us | http://localhost:3001/about |
| Projects | http://localhost:3001/projects |
| Gallery | http://localhost:3001/gallery |
| Contact | http://localhost:3001/contact |
| Privacy Policy | http://localhost:3001/policy |

---

## ⚙️ Configuration

### Email Setup for Contact Form

1. Open `.env` file in the project directory
2. Update these lines:
```env
SMTP_USER=your-email@gmail.com
SMTP_PASS=your-app-password
```

**For Gmail:**
- Enable 2-Factor Authentication on your Gmail account
- Generate App Password: https://myaccount.google.com/apppasswords
- Copy the 16-character password to `SMTP_PASS`

### Change Port

Edit `.env` and change:
```env
PORT=3001
```

---

## 📝 Modify Content

All content is centralized in one file for easy updates:

**File:** `routes/index.js`

### Company Information
Search for `companyInfo` object and update:
- Company name, tagline, description
- Phone numbers
- Email address
- Physical address
- Social media links
- Statistics

### Projects
Search for `projects` object and update:
- Ongoing projects
- Completed projects

### Testimonials
Search for `testimonials` array and add/edit customer reviews

### Core Benefits
Search for `coreBenefits` array and modify the 4 benefit cards

---

## 🎨 Modify Design/Layout

Edit EJS template files in the `views/` directory:
- `views/index.ejs` - Homepage
- `views/about.ejs` - About page
- `views/projects.ejs` - Projects page
- `views/gallery.ejs` - Gallery page
- `views/contact.ejs` - Contact page
- `views/partials/header.ejs` - Header/Navigation
- `views/partials/footer.ejs` - Footer

---

## 📦 Project Structure

```
ishtika-portfolio-nodejs/
├── server.js              # Main server file
├── package.json           # Dependencies
├── .env                   # Configuration (email, port, etc.)
├── start.sh              # Easy start script
├── routes/
│   └── index.js          # All routes & data (MODIFY THIS!)
├── views/                # Page templates
│   ├── partials/
│   │   ├── header.ejs
│   │   └── footer.ejs
│   ├── index.ejs
│   ├── about.ejs
│   ├── projects.ejs
│   ├── gallery.ejs
│   ├── contact.ejs
│   └── policy.ejs
├── public/
│   └── assets/           # CSS, JS, images (symlinked from assets1)
├── README.md             # Full documentation
└── QUICK_START.md        # Quick reference guide
```

---

## ✨ Features Implemented

✅ Homepage with video hero section
✅ Company statistics counter
✅ About page with vision/mission
✅ Projects showcase (ongoing & completed)
✅ Gallery with lightbox
✅ Contact form with email sending
✅ Google Maps integration
✅ Privacy policy page
✅ Fully responsive design
✅ Mobile navigation
✅ Testimonials slider
✅ Core benefits section

---

## 🔍 Troubleshooting

### Page not loading?
1. Check if server is running: Look for "Server running on http://localhost:3001"
2. Try accessing: http://localhost:3001
3. Check if port 3001 is free: `lsof -ti:3001`

### Assets (images/CSS) not loading?
Check if the symlink exists:
```bash
ls -la public/
# Should show: assets -> ../../assets1/assets
```

If missing, create it:
```bash
ln -s ../../assets1/assets public/assets
```

### Contact form not working?
1. Configure `.env` with email credentials
2. Check server console for errors
3. Verify SMTP settings are correct

---

## 🚢 Ready for Production?

See `README.md` for deployment instructions including:
- PM2 process manager setup
- Nginx/Apache configuration
- HTTPS/SSL setup
- Environment variables for production

---

## 📞 Need Help?

Contact: sales@ishtikahomes.com
Phone (Bengaluru): 96863 09767
Phone (Bellary): 96866 58656

---

**Built with ❤️ for Ishtika Homes**
