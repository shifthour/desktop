# Ishtika Homes - Premium Real Estate Developer Website

A world-class, modern real estate developer website built with **Next.js 14**, **TypeScript**, and **Tailwind CSS**.

## 🎯 Features

### ✨ Modern UI/UX
- **Luxury Design**: Premium fonts (Playfair Display, Inter, Montserrat), elegant color palette
- **Smooth Animations**: Framer Motion for fade, parallax, and reveal-on-scroll effects
- **High-Quality Showcase**: Project galleries with lightbox, floor plans, amenities
- **Professional Layout**: Contemporary design with ample white space
- **Responsive Design**: Perfect on Desktop, Tablet, and Mobile devices

### 🏗️ Pages Included
1. **Home Page**
   - Hero section with video/image banner
   - Company highlights and stats
   - Featured ongoing & completed projects
   - Testimonials carousel
   - CTA buttons (Book Site Visit, Download Brochure)

2. **Projects Page**
   - Filters: Ongoing/Upcoming/Completed
   - Modern grid-based layout
   - Search and sort functionality

3. **Project Details Page**
   - Hero banner with image gallery
   - Project overview and highlights
   - Amenities with icons
   - Location advantages with map
   - Floor plans with interactive display
   - Specifications
   - Enquiry form
   - WhatsApp CTA button

4. **About Us Page**
   - Vision, Mission, Values
   - Company journey timeline
   - Leadership team
   - Awards and recognition

5. **Contact Page**
   - Contact form with validation
   - Google Maps integration
   - Office locations
   - Social media links

6. **Blog/Updates** (Optional)
   - Latest news and announcements
   - Construction updates
   - Category filtering

### 🛠️ Technology Stack

- **Framework**: Next.js 14 (App Router)
- **Language**: TypeScript
- **Styling**: Tailwind CSS
- **Animations**: Framer Motion
- **Icons**: Lucide React
- **Image Slider**: Swiper
- **Forms**: React Hook Form + Zod validation
- **SEO**: Next.js Metadata API

## 📁 Project Structure

```
ishtika-homes/
├── public/
│   ├── images/
│   │   ├── projects/
│   │   ├── team/
│   │   └── logos/
│   └── videos/
├── src/
│   ├── app/
│   │   ├── (pages)/
│   │   │   ├── about/
│   │   │   ├── projects/
│   │   │   ├── contact/
│   │   │   └── blog/
│   │   ├── layout.tsx
│   │   ├── page.tsx
│   │   └── globals.css
│   ├── components/
│   │   ├── layout/
│   │   │   ├── Header.tsx
│   │   │   ├── Footer.tsx
│   │   │   └── MobileMenu.tsx
│   │   ├── home/
│   │   │   ├── HeroSection.tsx
│   │   │   ├── FeaturedProjects.tsx
│   │   │   ├── StatsSection.tsx
│   │   │   └── TestimonialsSection.tsx
│   │   ├── projects/
│   │   │   ├── ProjectCard.tsx
│   │   │   ├── ProjectFilters.tsx
│   │   │   └── ProjectGallery.tsx
│   │   ├── common/
│   │   │   ├── Button.tsx
│   │   │   ├── Input.tsx
│   │   │   ├── Card.tsx
│   │   │   └── Modal.tsx
│   │   └── animations/
│   │       ├── FadeIn.tsx
│   │       ├── SlideIn.tsx
│   │       └── ScrollReveal.tsx
│   ├── lib/
│   │   ├── data.ts
│   │   ├── utils.ts
│   │   └── constants.ts
│   └── types/
│       └── index.ts
├── tailwind.config.ts
├── tsconfig.json
├── next.config.ts
└── package.json
```

## 🚀 Getting Started

### Prerequisites
- Node.js 18+
- npm or yarn

### Installation

1. **Install Dependencies**
   ```bash
   npm install
   ```

2. **Run Development Server**
   ```bash
   npm run dev
   ```

3. **Open Browser**
   Navigate to [http://localhost:3000](http://localhost:3000)

### Build for Production

```bash
npm run build
npm start
```

## 🎨 Design System

### Color Palette

**Primary Colors**
- Primary Blue: `#0ea5e9` to `#0c4a6e`
- Gold Accent: `#eab308` to `#713f12`

**Neutral Colors**
- Luxury Dark: `#1a1a1a`
- Luxury Darker: `#0a0a0a`
- Light: `#f8f9fa`
- Cream: `#faf8f5`

### Typography

- **Headings**: Playfair Display (Serif)
- **Body**: Inter (Sans-serif)
- **Accent**: Montserrat (Sans-serif)

### Components

#### Buttons
- `btn-primary`: Primary CTA button
- `btn-secondary`: Secondary action button
- `btn-gold`: Premium gold button
- `btn-dark`: Dark elegant button

#### Cards
- `card`: Standard card with shadow
- `card-luxury`: Premium card with hover effects

#### Inputs
- `input-field`: Text input with focus states
- `textarea-field`: Textarea with validation
- `select-field`: Dropdown select

## 📱 Responsive Breakpoints

- **Mobile**: < 640px
- **Tablet**: 640px - 1024px
- **Desktop**: 1024px - 1280px
- **Large Desktop**: > 1280px

## ✅ SEO Optimization

- **Metadata**: Dynamic meta tags for each page
- **Open Graph**: Social media sharing optimization
- **Schema Markup**: Real estate structured data
- **Sitemap**: Auto-generated sitemap
- **Robots.txt**: Search engine crawling rules
- **Image Optimization**: Next.js Image component
- **Performance**: Code splitting, lazy loading

## 🔧 Configuration

### Environment Variables

Create a `.env.local` file:

```env
NEXT_PUBLIC_SITE_URL=https://yourwebsite.com
NEXT_PUBLIC_GOOGLE_MAPS_API_KEY=your_api_key
NEXT_PUBLIC_WHATSAPP_NUMBER=+91XXXXXXXXXX
NEXT_PUBLIC_CONTACT_EMAIL=info@ishtikahomes.com
```

### Google Maps Integration

1. Get API key from [Google Cloud Console](https://console.cloud.google.com/)
2. Enable Maps JavaScript API
3. Add to `.env.local`

### WhatsApp Integration

Update the WhatsApp number in:
- `src/lib/constants.ts`
- `.env.local`

## 📊 Data Management

### Adding New Projects

Edit `src/lib/data.ts`:

```typescript
export const projects: Project[] = [
  {
    id: 'unique-id',
    title: 'Project Name',
    slug: 'project-slug',
    status: 'ongoing' | 'upcoming' | 'completed',
    // ... other fields
  }
];
```

### Updating Company Info

Edit company details in `src/lib/data.ts`:

```typescript
export const companyInfo: CompanyInfo = {
  name: 'Your Company',
  tagline: 'Your Tagline',
  // ... other fields
};
```

## 🎯 Key Features Implementation

### 1. Image Gallery
- Swiper for smooth carousel
- Lightbox for full-screen view
- Lazy loading for performance

### 2. Floor Plans
- Interactive floor plan viewer
- Download functionality
- Configuration filtering

### 3. Amenities
- Icon-based display
- Category grouping
- Hover effects

### 4. Contact Forms
- Form validation
- Success/Error states
- Email integration ready

### 5. Animations
- Scroll-triggered animations
- Page transitions
- Hover effects
- Loading states

## 🔒 Best Practices

- ✅ TypeScript for type safety
- ✅ Component-based architecture
- ✅ Mobile-first responsive design
- ✅ Accessibility (WCAG 2.1)
- ✅ Performance optimization
- ✅ SEO best practices
- ✅ Clean code structure
- ✅ Reusable components

## 📈 Performance Optimization

- **Image Optimization**: Next.js Image with AVIF/WebP
- **Code Splitting**: Automatic route-based splitting
- **Lazy Loading**: Dynamic imports for heavy components
- **Caching**: Static generation where possible
- **Minification**: Production build optimization

## 🚀 Deployment

### Vercel (Recommended)

```bash
npm install -g vercel
vercel
```

### Alternative Platforms
- **Netlify**: Connect GitHub repo
- **AWS Amplify**: Deploy from Git
- **DigitalOcean App Platform**: Docker deployment

## 📞 Support & Customization

For customization or support:
1. Review component documentation in code
2. Check TypeScript types in `src/types/`
3. Modify styles in `tailwind.config.ts`
4. Update content in `src/lib/data.ts`

## 🎓 Learning Resources

- [Next.js Documentation](https://nextjs.org/docs)
- [Tailwind CSS](https://tailwindcss.com/docs)
- [Framer Motion](https://www.framer.com/motion/)
- [TypeScript](https://www.typescriptlang.org/docs/)

## 📝 License

Private - © 2024 Ishtika Homes

---

**Built with ❤️ using Next.js 14 + TypeScript + Tailwind CSS**
