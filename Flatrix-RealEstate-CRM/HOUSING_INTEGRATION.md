# Housing.com Integration - Complete Guide

## 🎉 Integration Status: **LIVE & TESTED**

Your Housing.com integration is fully deployed and working in production!

---

## 📋 Overview

**Integration Type:** Pull-based (Your CRM fetches leads from Housing.com)
**Status:** Production Ready ✓
**Last Tested:** October 23, 2025
**Test Results:** Successfully fetched and processed leads ✓

### Your Housing.com Credentials:
- **Profile ID:** `51846085`
- **Encryption Key:** `75edc303b71bbf4351f0eec8362f729a`

---

## 🔄 How It Works

Unlike MagicBricks (which pushes leads to you), Housing.com uses a **pull-based integration**:

1. **Your CRM calls Housing.com API** periodically to fetch new leads
2. **Authentication** using HMAC SHA256 hash generated from encryption key
3. **Leads are fetched** for a specified time range
4. **Automatic duplicate detection** - updates existing leads or creates new ones
5. **All leads tagged** with source "Housing.com"

---

## 🚀 API Endpoint

**Your Integration Endpoint:**
```
https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/housing/fetch-leads
```

**Method:** POST
**Authentication:** API Key in header

**Your API Key (for calling this endpoint):**
```
a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6
```

---

## 📝 How to Fetch Leads

### Option 1: Fetch Last 24 Hours (Default)

```bash
curl -X POST https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/housing/fetch-leads \
  -H "Content-Type: application/json" \
  -H "x-api-key: a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6" \
  -d '{}'
```

### Option 2: Fetch Specific Time Range

```bash
curl -X POST https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/housing/fetch-leads \
  -H "Content-Type: application/json" \
  -H "x-api-key: a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6" \
  -d '{
    "start_date": "1729584000",
    "end_date": "1729670400",
    "per_page": 100
  }'
```

### Option 3: Filter by Project

```bash
curl -X POST https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/housing/fetch-leads \
  -H "Content-Type: application/json" \
  -H "x-api-key: a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6" \
  -d '{
    "project_ids": "1234,5678",
    "apartment_names": "2 BHK, 3 BHK"
  }'
```

---

## 📊 Request Parameters

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `start_date` | String (Epoch) | No | Start date for fetching leads (default: 24h ago) | `1729584000` |
| `end_date` | String (Epoch) | No | End date for fetching leads (default: now) | `1729670400` |
| `per_page` | Integer | No | Number of records to fetch (default: 100, max: 1000) | `100` |
| `project_ids` | String | No | Comma-separated project IDs | `"1234,5678"` |
| `apartment_names` | String | No | Filter by apartment types | `"1 BHK, 2 BHK"` |

**Epoch Time Converter:** Use https://www.epochconverter.com/ or:
```bash
# Get current epoch time
date +%s

# Get epoch for specific date (example: Oct 22, 2025 00:00:00)
date -j -f "%Y-%m-%d %H:%M:%S" "2025-10-22 00:00:00" +%s
```

---

## ✅ Response Format

### Success Response:
```json
{
  "success": true,
  "message": "Leads fetched and processed successfully",
  "leadsProcessed": 2,
  "leadsCreated": 1,
  "leadsUpdated": 1,
  "timeRange": {
    "start": "2025-10-22T10:29:38.000Z",
    "end": "2025-10-23T10:29:38.000Z"
  }
}
```

### No Leads Response:
```json
{
  "success": true,
  "message": "No new leads found",
  "leadsProcessed": 0,
  "leadsCreated": 0,
  "leadsUpdated": 0
}
```

### Error Response:
```json
{
  "success": false,
  "error": "Error type",
  "message": "Error description"
}
```

---

## 🗓️ Recommended Fetching Schedule

### Manual Fetching:
- Run the API call whenever you want to fetch new leads
- Recommended: Every 1-2 hours during business hours

### Automated Fetching (Future Enhancement):
You can set up a cron job or scheduled task to automatically fetch leads:

**Example Cron (Every 2 hours):**
```bash
0 */2 * * * curl -X POST https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/housing/fetch-leads -H "Content-Type: application/json" -H "x-api-key: a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6" -d '{}'
```

**Using Vercel Cron Jobs:**
You can also set up Vercel Cron Jobs for automated fetching (requires configuration in `vercel.json`).

---

## 🔍 Lead Data Mapping

| Housing.com Field | CRM Field | Notes |
|-------------------|-----------|-------|
| `lead_name` | `name` | Customer name |
| `lead_phone` | `phone` | Required field |
| `lead_email` | `email` | Optional |
| `project_name` | `project_name` | Project details |
| `locality` | `preferred_location` | Location |
| `apartment_names` | `interested_in` | Mapped to property types |
| `service_type` | `notes` | rent/resale/new-projects |
| `lead_date` | `notes` | When lead was generated |
| - | `source` | Always "Housing.com" |
| - | `status` | Always "NEW" for new leads |

---

## 👀 Verify Leads in CRM

1. **Login to CRM:**
   - URL: https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app
   - Email: `admin@flatrix.com`
   - Password: `admin123`

2. **Navigate to Leads:**
   - Click on "Leads" in the sidebar
   - Filter by Source: "Housing.com"

3. **Check Lead Details:**
   - All Housing.com leads tagged with source "Housing.com"
   - Status: "NEW" for new leads
   - Notes contain all Housing.com metadata

4. **View Activity Log:**
   - Go to Activities section
   - See logs for all lead creations/updates

---

## 🔐 Security & Configuration

### Environment Variables (Already Configured):

**Production (Vercel):**
- ✅ `HOUSING_PROFILE_ID` - Your Housing.com Profile ID
- ✅ `HOUSING_ENCRYPTION_KEY` - Encryption key for HMAC hash
- ✅ `HOUSING_FETCH_API_KEY` - API key to call your fetch endpoint
- ✅ `SYSTEM_USER_ID` - System user for lead creation

**Local (.env.local):**
- ✅ All variables configured for local testing

### Security Features:
- ✅ HMAC SHA256 authentication with Housing.com
- ✅ API Key authentication for your fetch endpoint
- ✅ HTTPS only (encrypted communication)
- ✅ Duplicate detection (prevents duplicate leads)
- ✅ Activity logging (full audit trail)
- ✅ Time-bound requests (15-minute window)

---

## 🧪 Testing

### Test the Endpoint:

```bash
curl -X POST https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/housing/fetch-leads \
  -H "Content-Type: application/json" \
  -H "x-api-key: a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6" \
  -d '{}'
```

### Check Status:

```bash
curl -X GET https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/housing/fetch-leads \
  -H "x-api-key: a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6"
```

---

## 🔧 Technical Details

### Housing.com API:
- **URL:** `https://pahal.housing.com/api/v0/get-builder-leads`
- **Authentication:** HMAC SHA256 hash
- **Profile ID:** `51846085`
- **Encryption Key:** `75edc303b71bbf4351f0eec8362f729a`

### Hash Generation:
```javascript
const crypto = require('crypto');
const currentTime = Math.floor(Date.now() / 1000).toString();
const hash = crypto.createHmac('sha256', ENCRYPTION_KEY)
  .update(currentTime)
  .digest('hex');
```

### API Call Format:
```
GET https://pahal.housing.com/api/v0/get-builder-leads?
  start_date={epoch}&
  end_date={epoch}&
  current_time={epoch}&
  hash={generated_hash}&
  id={profile_id}&
  per_page=100
```

---

## ⚡ Common Use Cases

### 1. Fetch Today's Leads
```bash
curl -X POST https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/housing/fetch-leads \
  -H "Content-Type: application/json" \
  -H "x-api-key: a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6" \
  -d '{
    "start_date": "'$(date -v-1d +%s)'",
    "end_date": "'$(date +%s)'"
  }'
```

### 2. Fetch Last Week's Leads
```bash
curl -X POST https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/housing/fetch-leads \
  -H "Content-Type: application/json" \
  -H "x-api-key: a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6" \
  -d '{
    "start_date": "'$(date -v-7d +%s)'",
    "end_date": "'$(date +%s)'",
    "per_page": 1000
  }'
```

### 3. Setup Automated Hourly Fetch
Create a script `fetch-housing-leads.sh`:
```bash
#!/bin/bash
curl -X POST https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/housing/fetch-leads \
  -H "Content-Type: application/json" \
  -H "x-api-key: a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6" \
  -d '{}' \
  >> /var/log/housing-fetch.log 2>&1
```

Then add to crontab:
```bash
# Fetch every hour at :05
5 * * * * /path/to/fetch-housing-leads.sh
```

---

## 📞 Troubleshooting

### Issue: "Unauthorized - Invalid API key"
**Solution:** Verify the API key in header matches `HOUSING_FETCH_API_KEY`

### Issue: "Housing.com API error"
**Solution:**
- Check Profile ID and Encryption Key are correct
- Verify time range is valid
- Check Housing.com API is accessible

### Issue: No leads returned
**Solution:**
- Verify time range has leads in Housing.com
- Check if leads were already fetched (check CRM)
- Try expanding time range

### Issue: Leads not appearing in CRM
**Solution:**
- Check API response for errors
- Verify SYSTEM_USER_ID is configured
- Check CRM database connectivity

---

## 📋 Integration Checklist

- [x] API endpoint created and deployed
- [x] Profile ID and Encryption Key configured
- [x] Fetch API key generated and secured
- [x] Environment variables set in production
- [x] HMAC SHA256 hash generation working
- [x] Duplicate detection working
- [x] Activity logging working
- [x] Tested lead fetching ✓
- [x] Documentation complete
- [ ] Set up automated fetching schedule ← Next Step!

---

## 🎯 Next Steps

1. ✅ **Test the integration** - Already done! ✓
2. ⏳ **Set up regular fetching** - Schedule to fetch leads every 1-2 hours
3. ⏳ **Monitor leads** - Check CRM regularly for new Housing.com leads
4. ⏳ **Optimize time range** - Adjust based on lead volume

---

## 📁 Files Reference

| File | Purpose |
|------|---------|
| `HOUSING_INTEGRATION.md` | This file - complete guide |
| `src/app/api/integrations/housing/fetch-leads/route.ts` | API endpoint code |
| `test-housing-integration.sh` | Test script (to be created) |

---

## 🎉 Success Metrics

✅ **Integration Deployed:** October 23, 2025
✅ **Test Status:** Passed ✓
✅ **First Test:** Fetched 2 leads successfully
✅ **Production URL:** Live and accessible
✅ **Authentication:** Working
✅ **Database Integration:** Working

**🚀 INTEGRATION IS PRODUCTION READY!**

---

**Last Updated:** October 23, 2025
**Version:** 1.0
**Status:** ✅ Production Ready

---

## 💡 Pro Tips

1. **Fetch regularly** - Don't miss leads by fetching every 1-2 hours
2. **Monitor response** - Check `leadsCreated` vs `leadsUpdated` to track new vs existing
3. **Use filters** - Filter by `project_ids` if you have specific projects
4. **Time range** - Start with last 24 hours, expand if needed
5. **Activity logs** - Check CRM activities for integration health
