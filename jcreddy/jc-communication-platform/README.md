# JC Communication Platform

AI-Powered Bus Operations & Passenger Communication System for Mythri Travels.

## Overview

This platform integrates with **Bitla ticketSimply** to:
- Receive real-time booking, cancellation, and operational events via webhooks
- Store and manage passenger and trip data
- Send automated notifications (WhatsApp, SMS) to passengers
- Provide a beautiful dashboard for monitoring operations

## Quick Start

### 1. Install Dependencies

```bash
cd jc-communication-platform
npm install
```

### 2. Set Up Environment Variables

Copy the example env file and fill in your credentials:

```bash
cp .env.local.example .env.local
```

Required variables:
- `NEXT_PUBLIC_SUPABASE_URL` - Your Supabase project URL
- `NEXT_PUBLIC_SUPABASE_ANON_KEY` - Supabase anonymous key
- `SUPABASE_SERVICE_ROLE_KEY` - Supabase service role key
- `BITLA_API_BASE_URL` - Bitla API URL (default: http://myth.mythribus.com/api/)
- `BITLA_USERNAME` - Bitla API username
- `BITLA_PASSWORD` - Bitla API password

### 3. Set Up Database

Run the SQL schema in your Supabase SQL editor:

```bash
# Open database/001_schema.sql and execute in Supabase
```

### 4. Run Development Server

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) in your browser.

## Deployment to Vercel

### Option 1: Vercel CLI

```bash
npm i -g vercel
vercel
```

### Option 2: GitHub Integration

1. Push to GitHub
2. Import project in Vercel
3. Add environment variables
4. Deploy

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         BITLA SYSTEM                             │
│                    (ticketSimply API)                           │
└─────────────────────────────┬───────────────────────────────────┘
                              │
         ┌────────────────────┼────────────────────┐
         │                    │                    │
         ▼                    ▼                    ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│   WEBHOOKS      │  │    POLLING      │  │   SYNC API      │
│   /booking      │  │   (15 min)      │  │   /api/sync     │
│   /cancel       │  │                 │  │                 │
│   /status       │  │                 │  │                 │
└────────┬────────┘  └────────┬────────┘  └────────┬────────┘
         │                    │                    │
         └────────────────────┼────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     SUPABASE (PostgreSQL)                        │
│                                                                 │
│  Tables: jc_operators, jc_routes, jc_trips, jc_bookings,       │
│          jc_passengers, jc_notifications, jc_webhook_events     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    NEXT.JS FRONTEND                              │
│                                                                 │
│  Dashboard | Bookings | Trips | Notifications | Webhooks        │
└─────────────────────────────────────────────────────────────────┘
```

## Webhook Endpoints

Configure these URLs in Bitla for real-time callbacks:

| Event Type | Endpoint |
|------------|----------|
| Booking | `POST /api/webhooks/bitla/booking` |
| Cancellation | `POST /api/webhooks/bitla/cancellation` |
| Boarding Status | `POST /api/webhooks/bitla/boarding-status` |
| Vehicle Assignment | `POST /api/webhooks/bitla/vehicle-assign` |
| Service Update | `POST /api/webhooks/bitla/service-update` |

### Headers Required

```
X-Operator-ID: mythri
X-Webhook-Secret: your_secret
X-Timestamp: ISO8601 timestamp
```

## Pages

- **/** - Dashboard with overview stats, charts, and recent activity
- **/bookings** - List and search all bookings with filters
- **/trips** - View active and scheduled trips
- **/notifications** - Monitor notification delivery status
- **/webhooks** - View real-time webhook event stream
- **/sync** - Trigger manual data sync from Bitla

## Database Tables (jc_ prefix)

### Master Tables
- `jc_operators` - Bus operators/companies
- `jc_routes` - Route definitions
- `jc_coach_types` - Bus types
- `jc_coaches` - Vehicles
- `jc_stages` - Boarding/drop points
- `jc_agents` - Booking agents/OTAs

### Transactional Tables
- `jc_trips` - Trip instances
- `jc_bookings` - Booking records
- `jc_passengers` - Passenger details
- `jc_cancellations` - Cancellation records
- `jc_boarding_status` - Boarding status updates

### Communication Tables
- `jc_notifications` - Notification records
- `jc_notification_logs` - Delivery logs
- `jc_message_templates` - Message templates
- `jc_communication_queue` - Scheduled messages

### Operational Tables
- `jc_webhook_events` - Raw webhook events
- `jc_sync_jobs` - Sync job history
- `jc_daily_stats` - Daily statistics

## Tech Stack

- **Frontend**: Next.js 14, React, TailwindCSS, Recharts
- **Backend**: Next.js API Routes
- **Database**: Supabase (PostgreSQL)
- **State Management**: TanStack Query
- **Styling**: Tailwind CSS + custom components
- **Icons**: Lucide React

## Project Structure

```
jc-communication-platform/
├── database/
│   └── 001_schema.sql        # Supabase schema
├── src/
│   ├── app/
│   │   ├── api/
│   │   │   ├── webhooks/bitla/   # Webhook endpoints
│   │   │   ├── bookings/         # Bookings API
│   │   │   ├── notifications/    # Notifications API
│   │   │   ├── trips/            # Trips API
│   │   │   ├── sync/             # Data sync API
│   │   │   └── dashboard/        # Dashboard stats API
│   │   ├── bookings/
│   │   ├── notifications/
│   │   ├── trips/
│   │   ├── webhooks/
│   │   ├── sync/
│   │   ├── layout.tsx
│   │   └── page.tsx              # Dashboard
│   ├── components/
│   │   ├── dashboard/
│   │   └── ui/
│   ├── lib/
│   │   ├── supabase.ts
│   │   ├── bitla-client.ts
│   │   ├── event-processor.ts
│   │   └── utils.ts
│   └── types/
│       └── database.ts
├── .env.local.example
├── package.json
├── tailwind.config.ts
└── README.md
```

## Future Enhancements

- [ ] WhatsApp Business API integration
- [ ] SMS gateway integration (MSG91)
- [ ] Real-time push notifications
- [ ] Advanced analytics dashboard
- [ ] Multi-operator support
- [ ] AI-powered customer support bot

## License

Proprietary - JC Reddy Travels
