# Quick Start Guide

## Get Started in 3 Steps

### Step 1: Configure Email (Optional but Recommended)

Edit `.env` file and add your email credentials for the contact form:

```env
SMTP_USER=your-email@gmail.com
SMTP_PASS=your-app-password
```

**For Gmail:**
1. Enable 2-Factor Authentication
2. Generate App Password at: https://myaccount.google.com/apppasswords
3. Use the generated 16-character password

### Step 2: Start the Server

```bash
cd ishtika-portfolio-nodejs
npm run dev
```

Or for production:
```bash
npm start
```

### Step 3: Open in Browser

Navigate to: **http://localhost:3000**

## Available Pages

- `/` - Homepage
- `/about` - About Us
- `/projects` - Our Projects
- `/gallery` - Gallery
- `/contact` - Contact Form
- `/policy` - Privacy Policy

## Troubleshooting

### Assets not loading?

Make sure the symbolic link is created:
```bash
ls -la public/
# Should show: assets -> ../../assets1/assets
```

If not, create it:
```bash
mkdir -p public
ln -s ../../assets1/assets public/assets
```

### Contact form not working?

1. Check `.env` configuration
2. Verify SMTP credentials
3. Check console for errors

### Port already in use?

Change the port in `.env`:
```env
PORT=3001
```

## Project Structure

```
ishtika-portfolio-nodejs/
├── server.js          # Main server file
├── package.json       # Dependencies
├── .env              # Configuration
├── routes/
│   └── index.js      # All routes & data
├── views/            # EJS templates
│   ├── partials/
│   ├── index.ejs
│   ├── about.ejs
│   ├── projects.ejs
│   ├── gallery.ejs
│   ├── contact.ejs
│   └── policy.ejs
└── public/
    └── assets/       # Static files (symlinked)
```

## Modifying Content

### Company Information
Edit `routes/index.js` → `companyInfo` object

### Projects
Edit `routes/index.js` → `projects` object

### Testimonials
Edit `routes/index.js` → `testimonials` array

### Core Benefits
Edit `routes/index.js` → `coreBenefits` array

## Need Help?

Check the detailed [README.md](README.md) for complete documentation.
