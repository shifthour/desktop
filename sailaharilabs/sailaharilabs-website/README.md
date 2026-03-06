# Sailahari Labs Website

A modern, professional website for Sailahari Labs - Advanced Technology Lab Services. Built with Next.js 14, TypeScript, and Tailwind CSS.

## 🚀 Features

- **Modern Tech Stack**: Next.js 14 with App Router, TypeScript, Tailwind CSS
- **Responsive Design**: Fully responsive across all devices
- **SEO Optimized**: Complete SEO setup with meta tags, structured data, and sitemap
- **Performance Focused**: Optimized images, code splitting, and fast loading
- **Accessibility**: WCAG compliant with proper semantic HTML and ARIA labels
- **Professional UI**: Clean, modern design with consistent component system

## 📁 Project Structure

```
src/
├── app/                    # Next.js App Router pages
│   ├── about/             # About page
│   ├── contact/           # Contact page with form
│   ├── services/          # Services page
│   ├── case-studies/      # Case studies page
│   ├── layout.tsx         # Root layout with SEO
│   └── page.tsx           # Homepage
├── components/
│   ├── ui/                # Reusable UI components
│   ├── layout/            # Header, Footer components
│   ├── sections/          # Page sections (Hero, CTA, etc.)
│   └── StructuredData.tsx # Schema.org structured data
├── data/                  # Static data (services, etc.)
├── lib/                   # Utility functions
└── types/                 # TypeScript type definitions
```

## 🛠️ Installation & Setup

1. **Development Server**
   ```bash
   npm run dev
   ```
   Open [http://localhost:3000](http://localhost:3000) in your browser.

2. **Build for Production**
   ```bash
   npm run build
   npm start
   ```

## 🌐 Deployment Options

### Option 1: Vercel (Recommended)
1. Push code to GitHub
2. Connect repository to Vercel
3. Deploy automatically with zero configuration

### Option 2: Netlify
1. Build the project: `npm run build`
2. Deploy the generated files to Netlify

### Option 3: Traditional Hosting
1. Build the project: `npm run build`
2. Upload the generated files to your hosting provider

## 📊 SEO Features

- **Meta Tags**: Comprehensive meta tags for all pages
- **Open Graph**: Social media optimization
- **Structured Data**: Schema.org JSON-LD for better search results
- **Sitemap**: Automatically generated XML sitemap
- **Robots.txt**: Search engine crawling instructions
- **Performance**: Optimized for Core Web Vitals

## 🎨 Design System

### Colors
- Primary: `#2563EB` (Blue)
- Secondary: `#059669` (Green)
- Accent: `#DC2626` (Red)
- Neutral: `#64748B` (Slate)

### Typography
- Primary Font: Inter (system font)
- Headings: Bold weights (700)
- Body: Regular weight (400)

## 📱 Pages

1. **Homepage**: Hero section, services overview, about preview, CTA
2. **Services**: Detailed service descriptions with features
3. **About**: Company information, team, certifications
4. **Contact**: Contact form with company information
5. **Case Studies**: Success stories and project results

## 🔧 Customization

### Adding New Services
Edit `src/data/services.ts` to add or modify services.

### Updating Contact Information
Update contact details in:
- `src/components/layout/Footer.tsx`
- `src/app/contact/page.tsx`
- `src/components/StructuredData.tsx`

## 📈 Performance

- **Build Size**: Optimized bundle with tree shaking
- **Images**: WebP/AVIF format support with responsive sizing
- **Code Splitting**: Automatic route-based code splitting
- **Caching**: Optimal caching headers configured

## 🚀 Production Checklist

- [ ] Update domain in `next-sitemap.config.js`
- [ ] Configure contact form backend
- [ ] Set up analytics (Google Analytics, etc.)
- [ ] Test all forms and functionality
- [ ] Run Lighthouse audit
- [ ] Verify SEO optimization

## 📧 Contact Form Integration

The contact form is currently set up with client-side validation. For production, integrate with:
- **Formspree**: Simple form handling service
- **Netlify Forms**: If deploying to Netlify
- **Custom API**: Create API endpoint in `src/app/api/contact`

## 🔐 Security

- Headers configured for security (X-Frame-Options, etc.)
- Input validation on forms
- No exposed sensitive information
- HTTPS enforced

---

Built with ❤️ for Sailahari Labs
