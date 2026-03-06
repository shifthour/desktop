# IHW Platform - Web Application

A comprehensive health and fitness tracking web application with AI-powered insights, health records management, and seamless integration with national health systems.

## Features

### Fitness Tracking
- **Activity Tracking**: Steps, distance, calories monitoring with real-time updates
- **Heart Health Monitoring**: ECG recordings, HRV tracking, AFIB detection, 24/7 heart rate monitoring
- **Sleep Analysis**: Sleep stage tracking (Light, Deep, REM), respiratory rate monitoring, sleep score calculation
- **Stress & Recovery**: Body stress metrics, strain tracking, comprehensive recovery scoring
- **20+ Workout Modes**: Running, walking, cycling, swimming, yoga, HIIT, and more
- **Advanced Vital Signs**: VO2 max, SpO2, body temperature, blood pressure, diabetes management

### Health Records Management
- **Smart Cloud Storage**: Auto-parsing with OCR technology for automatic data extraction
- **On-Demand Sharing**: QR code and secure link generation for sharing with healthcare providers
- **Document Support**: Lab reports, prescriptions, vaccination certificates, imaging reports
- **AI-Powered Analytics**: Predictive analytics, risk assessment, personalized recommendations
- **ABHA & NDHM Integration**: Full integration with India's National Digital Health Mission

### Security & Compliance
- End-to-end encryption (AES-256)
- Two-factor authentication (2FA)
- HIPAA compliant
- GDPR ready
- Privacy controls and audit logs

### Integrations
- Apple HealthKit (iOS)
- Google Fit (Android)
- ABHA (Ayushman Bharat Health Account)
- NDHM (National Digital Health Mission)
- Cross-platform support (iOS, Android, Web)

## Technology Stack

### Frontend
- HTML5
- CSS3 (Custom CSS with CSS Variables)
- Vanilla JavaScript (ES6+)
- Font Awesome Icons
- Google Fonts (Inter)

### Design Features
- Responsive design (mobile-first approach)
- Modern gradient design system
- Smooth animations and transitions
- Intersection Observer API for scroll animations
- Accessible (WCAG 2.1 compliant)

## File Structure

```
web-app/
├── index.html              # Main HTML file
├── css/
│   └── styles.css         # Complete styling
├── js/
│   └── app.js            # JavaScript functionality
├── images/               # Image assets (placeholder)
├── assets/              # Additional assets (placeholder)
└── README.md           # This file
```

## Installation & Usage

### Local Development

1. **Clone or Download** the web-app folder

2. **Open the application**:
   ```bash
   # Option 1: Direct open
   open index.html

   # Option 2: Using Python HTTP server
   cd web-app
   python3 -m http.server 8000
   # Visit http://localhost:8000

   # Option 3: Using Node.js http-server
   npx http-server -p 8000
   # Visit http://localhost:8000
   ```

3. **View in browser**: Open your browser and navigate to the appropriate URL

### Deployment

#### Deploy to GitHub Pages
```bash
# Push to GitHub
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin <your-repo-url>
git push -u origin main

# Enable GitHub Pages in repository settings
```

#### Deploy to Netlify
1. Drag and drop the `web-app` folder to [Netlify Drop](https://app.netlify.com/drop)
2. Or connect your GitHub repository for continuous deployment

#### Deploy to Vercel
```bash
# Install Vercel CLI
npm i -g vercel

# Deploy
cd web-app
vercel
```

## Browser Support

- Chrome (latest 2 versions)
- Firefox (latest 2 versions)
- Safari (latest 2 versions)
- Edge (latest 2 versions)
- Mobile browsers (iOS Safari, Chrome Mobile)

## Features Breakdown

### Home Page Sections

1. **Hero Section**
   - Eye-catching headline with gradient text
   - Key statistics (10K+ users, 99.9% uptime, 24/7 monitoring)
   - Animated feature cards
   - CTA buttons

2. **Features Overview**
   - 6 main feature cards:
     - Activity Tracking
     - Heart Health Monitoring (Featured)
     - Sleep Analysis
     - Stress & Recovery
     - 20+ Workout Modes
     - Advanced Vital Signs
   - Detailed feature lists
   - Hover animations

3. **Health Records Section**
   - Visual representation of document types
   - 4 key features:
     - Smart Cloud Storage
     - On-Demand Sharing
     - AI-Powered Analytics
     - ABHA & NDHM Integration

4. **AI Analytics Section**
   - Interactive risk assessment dashboard
   - Animated progress bars
   - Real-time health insights
   - Predictive analytics showcase

5. **Security & Compliance**
   - 6 security features displayed
   - Icon-based presentation
   - Compliance badges

6. **Integrations**
   - Platform and service integrations
   - Logo grid layout

7. **Pricing Plans**
   - 3 tiers: Basic (₹199/mo), Premium (₹299/mo), Family (₹499/mo)
   - Feature comparison
   - Most popular badge

8. **Call-to-Action**
   - Download app CTA
   - Contact sales option
   - Trust indicators

9. **Footer**
   - Company information
   - Quick links
   - Social media links
   - Contact information
   - Compliance badges

## JavaScript Features

### Interactions
- Smooth scroll navigation
- Mobile responsive menu
- Scroll-to-top button
- Active link highlighting
- Button ripple effects

### Animations
- Intersection Observer for scroll animations
- Counter animations for statistics
- Progress bar animations
- Parallax effects
- Card floating animations

### Performance
- Debounced scroll events
- Lazy loading ready
- Performance monitoring
- Error handling

### Accessibility
- Keyboard navigation
- ARIA labels (can be enhanced)
- Focus management
- Semantic HTML

## Customization

### Colors
Edit CSS variables in `styles.css`:
```css
:root {
    --primary-color: #6366f1;
    --secondary-color: #ec4899;
    --accent-color: #14b8a6;
    /* ... more colors */
}
```

### Content
Edit text content directly in `index.html`

### Animations
Modify animation timing and effects in `app.js` and `styles.css`

## Performance Optimization

### Current Optimizations
- Minimal external dependencies
- Optimized animations (GPU-accelerated)
- Debounced scroll events
- Intersection Observer for lazy animations

### Recommended Enhancements
- Image optimization (WebP format)
- Implement lazy loading for images
- Add service worker for offline support
- Minify CSS and JavaScript for production
- Enable Gzip compression
- Use CDN for assets

## Accessibility

### Current Features
- Semantic HTML5 elements
- Proper heading hierarchy
- Alt text placeholders for images
- Keyboard navigation support
- Focus visible states

### Recommended Enhancements
- Add ARIA labels to all interactive elements
- Implement skip navigation links
- Ensure color contrast ratios meet WCAG AA standards
- Add screen reader announcements for dynamic content
- Test with screen readers (NVDA, JAWS, VoiceOver)

## Analytics Integration

The application is ready for analytics integration. Uncomment and configure in `app.js`:

```javascript
// Google Analytics
gtag('event', action, {
    'event_category': category,
    'event_label': label
});
```

## Future Enhancements

### Phase 2 Features
- User authentication and login
- Dashboard with real data
- Interactive charts and graphs
- Real-time data synchronization
- Push notifications
- Progressive Web App (PWA) features
- Multi-language support
- Dark mode toggle
- Advanced filtering and search
- Export reports functionality

### Integration Requirements
- Backend API connection
- Database integration
- User session management
- Real device data synchronization
- Payment gateway integration
- Email/SMS notification service

## Testing

### Manual Testing Checklist
- [ ] Navigation works on all screen sizes
- [ ] All links are functional
- [ ] Mobile menu opens and closes
- [ ] Scroll animations trigger correctly
- [ ] Buttons have hover effects
- [ ] Page loads in under 3 seconds
- [ ] Works across all major browsers
- [ ] Accessible via keyboard navigation

### Testing Tools
- Lighthouse (Performance, Accessibility, SEO)
- Chrome DevTools
- Cross-browser testing (BrowserStack)
- Mobile responsiveness testing

## License

Copyright © 2025 IHW. All rights reserved.

## Support

For support, email support@ihw.com or call +91 1800-123-4567

## Contributing

This is a proprietary project. For contribution inquiries, please contact the development team.

---

**Built with ❤️ for better health**
