# Ishtika Homes Portfolio - Node.js Application

A modern portfolio website for Ishtika Homes, built with Node.js, Express, and EJS templating. This project is a complete migration from the original PHP-based website.

## Features

- **Modern Architecture**: Built with Node.js and Express
- **Responsive Design**: Mobile-first, fully responsive layout
- **Dynamic Content**: EJS templating for dynamic page rendering
- **Contact Form**: Integrated email functionality using Nodemailer
- **Portfolio Showcase**: Display of ongoing and completed projects
- **Testimonials**: Customer reviews and feedback
- **Gallery**: Image gallery for completed projects
- **SEO Friendly**: Clean URLs and meta tag support

## Project Structure

```
ishtika-portfolio-nodejs/
├── server.js              # Main application server
├── package.json           # Dependencies and scripts
├── .env                   # Environment variables (configure before use)
├── routes/
│   └── index.js          # Application routes
├── views/
│   ├── partials/
│   │   ├── header.ejs    # Header component
│   │   └── footer.ejs    # Footer component
│   ├── index.ejs         # Homepage
│   ├── about.ejs         # About page
│   ├── projects.ejs      # Projects page
│   ├── gallery.ejs       # Gallery page
│   ├── contact.ejs       # Contact page
│   ├── policy.ejs        # Privacy policy page
│   ├── 404.ejs           # 404 error page
│   └── error.ejs         # Error page
└── public/               # Static assets (link from assets1 folder)
```

## Installation

### Prerequisites

- Node.js (v14 or higher)
- npm (comes with Node.js)

### Steps

1. **Navigate to the project directory**:
   ```bash
   cd ishtika-portfolio-nodejs
   ```

2. **Install dependencies**:
   ```bash
   npm install
   ```

3. **Setup Static Assets**:
   Create a symbolic link or copy the assets from the assets1 folder:
   ```bash
   # Create public directory
   mkdir public

   # Option 1: Create symbolic link (recommended)
   ln -s ../assets1/assets public/assets

   # Option 2: Copy assets (alternative)
   cp -r ../assets1/assets public/
   ```

4. **Configure Environment Variables**:
   Edit the `.env` file and update the email configuration:
   ```env
   PORT=3000
   NODE_ENV=development

   # Email Configuration
   SMTP_HOST=smtp.gmail.com
   SMTP_PORT=587
   SMTP_USER=your-email@gmail.com
   SMTP_PASS=your-app-password
   SMTP_FROM=sales@ishtikahomes.com
   SMTP_TO=sales@ishtikahomes.com
   ```

   **Gmail Setup**:
   - Enable 2-Factor Authentication on your Gmail account
   - Generate an App Password: https://myaccount.google.com/apppasswords
   - Use the generated password in `SMTP_PASS`

5. **Start the Development Server**:
   ```bash
   npm run dev
   ```

   Or for production:
   ```bash
   npm start
   ```

6. **Access the Application**:
   Open your browser and navigate to:
   ```
   http://localhost:3000
   ```

## Available Pages

- **Homepage** (`/`) - Main landing page with hero video, projects showcase, and testimonials
- **About** (`/about`) - Company information, vision, mission, and core benefits
- **Projects** (`/projects`) - List of all ongoing projects
- **Gallery** (`/gallery`) - Showcase of completed projects
- **Contact** (`/contact`) - Contact form with Google Maps integration
- **Privacy Policy** (`/policy`) - Privacy policy information

## Company Information

All company information (contact details, social media links, statistics) is centralized in `routes/index.js`. Update this file to modify company-wide information.

## Key Data Structures

### Company Info
Located in `routes/index.js`:
- Name, tagline, description
- Phone numbers (Bengaluru and Bellary)
- Email address
- Physical address
- Social media links
- Company statistics

### Projects
Two categories:
1. **Ongoing Projects**: Krishna, Vashishta, Naadam, Vyasa, Anahata
2. **Completed Projects**: Agastya, Sunrise, Advaitha, White Pearl, Pride, Flora, Arcadia, Chanasya, Prakriti

### Testimonials
Customer reviews from satisfied homeowners

## Email Configuration

The contact form uses Nodemailer to send emails. Configure the SMTP settings in the `.env` file:

- For Gmail, use App Passwords (not regular password)
- For other email providers, update `SMTP_HOST` and `SMTP_PORT` accordingly

## Development

### Adding New Pages

1. Create a new EJS file in `views/`
2. Add route in `routes/index.js`
3. Update navigation in `views/partials/header.ejs`

### Modifying Content

- **Page Content**: Edit corresponding `.ejs` files in `views/`
- **Company Info**: Update `companyInfo` object in `routes/index.js`
- **Projects**: Update `projects` object in `routes/index.js`
- **Testimonials**: Update `testimonials` array in `routes/index.js`

## Deployment

### Production Setup

1. Set `NODE_ENV=production` in `.env`
2. Use a process manager like PM2:
   ```bash
   npm install -g pm2
   pm2 start server.js --name "ishtika-portfolio"
   pm2 save
   pm2 startup
   ```

3. Configure reverse proxy (nginx/apache)
4. Enable HTTPS with Let's Encrypt

### Environment Variables for Production
- Update all SMTP credentials
- Set proper domain names
- Configure production database if needed

## Migration from PHP

This application is a complete migration from the original PHP codebase located in `assets1/`. Key improvements:

1. **Modern Stack**: PHP → Node.js/Express
2. **Template Engine**: PHP includes → EJS templates
3. **Better Structure**: Separated routes, views, and logic
4. **Enhanced Security**: Environment variables for sensitive data
5. **Improved Maintainability**: Centralized data management

## Technologies Used

- **Backend**: Node.js, Express.js
- **Templating**: EJS (Embedded JavaScript)
- **Email**: Nodemailer
- **Frontend**: Bootstrap, jQuery (from original design)
- **CSS/JS**: All original assets maintained for design consistency

## License

Copyright © 2024 Ishtika Homes. All rights reserved.

## Support

For technical support or questions:
- Email: sales@ishtikahomes.com
- Phone (Bengaluru): 96863 09767
- Phone (Bellary): 96866 58656

---

**Built with ❤️ for Ishtika Homes**
