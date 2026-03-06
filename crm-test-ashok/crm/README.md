# Real Estate CRM

A production-ready, zero-cost real estate CRM system built for Bengaluru channel partners. Track leads from initial contact through site visits to bookings with automated workflows and comprehensive reporting.

## ✨ Features

- **🚀 Zero-Cost Architecture**: Runs entirely on free tiers (Vercel, Neon, Cloudflare, Resend)
- **📞 Telecaller-First Design**: Keyboard shortcuts, fast navigation, minimal clicks
- **🔒 PII Protection**: Automatic masking of sensitive data in lists and logs
- **📊 Advanced Lead Scoring**: Rule-based scoring with customizable weights
- **🤖 Smart Deduplication**: Automatic duplicate detection by phone/email
- **📱 WhatsApp Integration**: One-click messaging with pre-filled templates
- **📈 Comprehensive Reports**: CPL, CPSV, CPB metrics with funnel analysis
- **⚡ Real-time Notifications**: Email alerts for assignments and reminders
- **🌙 Dark Mode**: System-preference based theme switching

## 🏗️ Tech Stack

- **Frontend**: Next.js 14 (App Router), React 19, TypeScript
- **UI**: shadcn/ui, Tailwind CSS, Lucide Icons
- **Database**: Neon PostgreSQL (Free tier)
- **ORM**: Prisma
- **Authentication**: Custom credentials-based auth with Argon2
- **Email**: Resend (Free tier)
- **Deployment**: Vercel Hobby (Free)
- **Cron Jobs**: Cloudflare Workers
- **Testing**: Vitest, Testing Library

## 🚀 Quick Start

### 1. Clone and Install

```bash
cd crm
npm install
```

### 2. Set up Environment Variables

Copy the environment template:

```bash
cp .env.example .env
```

Update `.env` with your configuration:

```env
# Database - Get from Neon.tech
DATABASE_URL="postgresql://username:password@host:5432/database_name"

# Email - Get from Resend.com  
RESEND_API_KEY="re_xxxxxxxxxxxxx"

# Authentication - Generate random strings
AUTH_PASSWORD_SALT="your-random-32-byte-string"
NEXTAUTH_SECRET="your-random-32-byte-secret"
NEXTAUTH_URL="http://localhost:3001"
```

### 3. Database Setup

Generate Prisma client and run migrations:

```bash
npm run prisma:generate
npm run prisma:migrate
```

Seed the database with sample data:

```bash
npm run prisma:seed
```

### 4. Start Development Server

```bash
npm run dev
```

Visit http://localhost:3001 and login with:
- **Admin**: admin@crm.com / admin123
- **Manager**: manager@crm.com / manager123  
- **Agent**: agent@crm.com / agent123

## 🌐 Deployment

### Neon PostgreSQL Setup

1. Sign up at [Neon.tech](https://neon.tech)
2. Create a new database
3. Copy the connection string to your `.env` file
4. Run migrations: `npm run prisma:migrate`

### Vercel Deployment

1. Push your code to GitHub
2. Connect your repo to [Vercel](https://vercel.com)
3. Add environment variables in Vercel dashboard:
   - `DATABASE_URL`
   - `RESEND_API_KEY`  
   - `AUTH_PASSWORD_SALT`
   - `NEXTAUTH_SECRET`
   - `NEXTAUTH_URL` (your production URL)
4. Deploy!

### Resend Email Setup

1. Sign up at [Resend.com](https://resend.com)
2. Create an API key
3. Optional: Add your domain for custom sender emails
4. Update `RESEND_API_KEY` in your environment

### Cloudflare Workers Cron

1. Install Wrangler CLI: `npm install -g wrangler`
2. Login: `wrangler login`
3. Navigate to the worker directory: `cd cloudflare-worker`
4. Set your app URL as a secret:
   ```bash
   wrangler secret put BASE_URL
   # Enter: https://your-app.vercel.app
   ```
5. Deploy the worker:
   ```bash
   wrangler deploy
   ```

## 📱 Usage Guide

### Keyboard Shortcuts

The telecaller interface supports these shortcuts:

- **C** - Make a call (opens tel: link)
- **W** - Send WhatsApp message  
- **N** - Add note / View lead details
- **S** - Change stage (on lead detail page)
- **?** - Show help

### Lead Management Workflow

1. **New Leads**: Auto-assigned to agents via round-robin
2. **Scoring**: Automatic scoring based on budget, location, source
3. **Follow-up**: 15-minute SLA for first contact
4. **Site Visits**: Schedule with automatic reminders
5. **Reporting**: Track CPL, CPSV, CPB across sources

## 🧪 Testing

Run the test suite:

```bash
npm run test          # Run all tests
npm run test:watch    # Watch mode
npm run typecheck     # TypeScript checking
npm run lint          # ESLint
```

## 📊 Available Scripts

```bash
npm run dev           # Start development server
npm run build         # Build for production  
npm run start         # Start production server
npm run lint          # Run ESLint
npm run format        # Format with Prettier
npm run typecheck     # TypeScript type checking
npm run test          # Run tests
npm run prisma:generate  # Generate Prisma client
npm run prisma:migrate   # Run database migrations
npm run prisma:seed      # Seed database with sample data
```
