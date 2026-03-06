# 🕐 Housing.com Automated Lead Fetching - Setup Guide

## ⚠️ Important Limitation

**Vercel Hobby Plan**: Only supports **DAILY** cron jobs (not every 10 minutes as originally requested)

Your current plan does not support frequent cron jobs (every 10 minutes). Vercel Pro plan is required for that feature.

---

## ✅ What's Been Implemented

### 1. Database Sync Tracking
- Created `flatrix_integration_sync_log` table to track last sync time
- Ensures only NEW leads are fetched (no duplicates)
- Includes 2-minute buffer to prevent missing leads due to clock skew

### 2. Updated Fetch Endpoint
- **Route**: `/api/integrations/housing/fetch-leads`
- Now uses sync log to fetch only leads since last sync
- Falls back to 24 hours on first run
- Updates sync log after each fetch

### 3. Vercel Cron Job Endpoint
- **Route**: `/api/cron/fetch-housing-leads`
- Secured with `CRON_SECRET` environment variable
- Calls fetch-leads endpoint automatically
- **Current Schedule**: Daily at 9 AM UTC (Vercel Hobby limitation)

### 4. Environment Variables Added
- `CRON_SECRET`: Authentication for cron endpoint
- `NEXT_PUBLIC_APP_URL`: Base URL for internal API calls

---

## 🔧 Setup Steps Required

### Step 1: Create Sync Log Table in Supabase

You need to run the SQL file in your Supabase database:

**Option A: Using Supabase Dashboard**
1. Go to https://eflvzsfgoelonfclzrjy.supabase.co/project/eflvzsfgoelonfclzrjy
2. Click on "SQL Editor" in left sidebar
3. Open the file: `/supabase/create_integration_sync_log.sql`
4. Copy all contents and paste into SQL Editor
5. Click "Run"

**Option B: Using Direct URL**
1. Visit: https://supabase.com/dashboard/project/eflvzsfgoelonfclzrjy/sql
2. Copy and paste SQL from `/supabase/create_integration_sync_log.sql`
3. Run it

**What it creates**:
- `flatrix_integration_sync_log` table
- Initializes Housing.com sync record set to 24 hours ago
- Creates index for faster lookups

---

## 📊 Current Schedule

**⏰ Daily at 9:00 AM UTC** (Vercel Hobby Plan Limitation)

This means leads will be fetched once per day automatically.

---

## 🚀 Alternative Solutions for More Frequent Fetching

Since Vercel Hobby plan only allows daily cron jobs, here are your options:

### Option 1: Manual Fetching (Free)
Run this command whenever you want to fetch new leads:

```bash
curl -X POST https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/housing/fetch-leads \
  -H "Content-Type: application/json" \
  -H "x-api-key: a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6" \
  -d '{}'
```

Or use the test script:
```bash
./test-housing-integration.sh
```

### Option 2: Upgrade to Vercel Pro ($20/month)
- Allows cron jobs as frequent as every minute
- Change schedule in `vercel.json` to `"schedule": "*/10 * * * *"` (every 10 minutes)
- Redeploy with `npx vercel --prod`

### Option 3: External Cron Service (Free)
Use a free external service to trigger your endpoint:

**A. cron-job.org** (Free)
1. Sign up at https://cron-job.org
2. Create new cron job
3. URL: `https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/housing/fetch-leads`
4. Method: POST
5. Headers:
   - `Content-Type: application/json`
   - `x-api-key: a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6`
6. Body: `{}`
7. Schedule: Every 10 minutes

**B. EasyCron** (Free tier)
1. Sign up at https://www.easycron.com
2. Similar setup as above
3. Free tier: Up to 1000 executions/month (enough for every 10 min)

**C. UptimeRobot** (Free, but limited)
1. Sign up at https://uptimerobot.com
2. Can check every 5 minutes on free tier
3. Not ideal but works for basic monitoring and triggering

### Option 4: Run Your Own Cron (Free, requires server)
If you have a server or VPS, add this to crontab:

```bash
# Edit crontab
crontab -e

# Add this line (runs every 10 minutes)
*/10 * * * * curl -X POST https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/housing/fetch-leads -H "Content-Type: application/json" -H "x-api-key: a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6" -d '{}'
```

---

## 📝 How Sync Tracking Works

1. **First Fetch**: No sync record exists → Fetches last 24 hours
2. **Subsequent Fetches**:
   - Reads last sync time from database
   - Subtracts 2 minutes (clock skew buffer)
   - Fetches leads from (last_sync - 2min) to NOW
   - Updates sync log with new timestamp
3. **Result**: Each fetch gets only NEW leads since last fetch

### Example Timeline:
- **10:00 AM**: First fetch → Gets leads from 10:00 AM yesterday to now
- **10:10 AM**: Second fetch → Gets leads from 10:08 AM to now (2-min buffer)
- **10:20 AM**: Third fetch → Gets leads from 10:18 AM to now
- **Result**: Zero duplicates, zero missed leads

---

## ✅ Testing

### Test Manual Fetch:
```bash
curl -X POST https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/housing/fetch-leads \
  -H "Content-Type: application/json" \
  -H "x-api-key: a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6" \
  -d '{}'
```

### Test Cron Endpoint (after deployment completes):
```bash
curl -X GET https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/cron/fetch-housing-leads \
  -H "Authorization: Bearer 971e502ee20f75cdada290e78d557476230ab4c55eb130fb423e7e50827663d8"
```

---

## 📈 Monitoring

### Check Sync Log in Supabase:
```sql
SELECT * FROM flatrix_integration_sync_log WHERE integration_name = 'Housing.com';
```

This shows:
- Last sync time
- Leads fetched in last sync
- Leads created vs updated
- Status (success/error)
- Error message if any

### Check Vercel Cron Logs:
1. Go to https://vercel.com/shifthourjobs-gmailcoms-projects/flatrix-crm
2. Click "Cron Jobs" in sidebar
3. View execution history and logs

---

## 🎯 Recommendation

**For Your Use Case** (fetching every 10 minutes):

I recommend **Option 3A: cron-job.org** (Free external service)
- ✅ Free forever
- ✅ Reliable
- ✅ Easy setup (5 minutes)
- ✅ Email notifications on failures
- ✅ Execution history
- ✅ No server maintenance

**Steps**:
1. Sign up at cron-job.org
2. Create cron job with URL and headers (as shown above)
3. Set schedule to "Every 10 minutes"
4. Save and activate
5. Done! Leads will be fetched every 10 minutes automatically

---

## 🔐 Security Notes

- Cron endpoint is secured with `CRON_SECRET`
- Fetch endpoint is secured with `HOUSING_FETCH_API_KEY`
- Both secrets are stored in Vercel environment variables
- External services only need the fetch endpoint API key (not cron secret)

---

## 📚 Files Modified

1. `/src/app/api/integrations/housing/fetch-leads/route.ts` - Sync tracking logic
2. `/src/app/api/cron/fetch-housing-leads/route.ts` - Vercel Cron endpoint (NEW)
3. `/supabase/create_integration_sync_log.sql` - Database table (NEW)
4. `/vercel.json` - Cron configuration
5. `.env.local` & `.env.example` - New environment variables
6. `/src/components/LeadsComponent.tsx` - Added "Direct Call" and "Accompany client" options

---

## ❓ Need Help?

1. **Check sync log**: Query the `flatrix_integration_sync_log` table in Supabase
2. **Check Vercel logs**: View function logs in Vercel dashboard
3. **Test manually**: Use the curl commands above to test endpoints
4. **View leads**: Login to CRM and filter by source "Housing.com"

---

**Last Updated**: October 23, 2025
