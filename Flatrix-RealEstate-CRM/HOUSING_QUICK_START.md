# 🏠 Housing.com Integration - Quick Start Guide

## ✅ Status: LIVE & WORKING!

Your Housing.com integration is successfully deployed and tested! ✓

---

## 🎯 What You Need to Know

**Integration Type:** Pull-based (You fetch leads from Housing.com)
**Your Credentials:**
- Profile ID: `51846085`
- Encryption Key: `75edc303b71bbf4351f0eec8362f729a`

**Endpoint:** `https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/housing/fetch-leads`

---

## ⚡ Quick Commands

### Fetch Leads Now (Last 24 Hours)
```bash
curl -X POST https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/housing/fetch-leads \
  -H "Content-Type: application/json" \
  -H "x-api-key: a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6" \
  -d '{}'
```

### Run Test Script
```bash
./test-housing-integration.sh
```

### Check Endpoint Status
```bash
curl -X GET https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/housing/fetch-leads \
  -H "x-api-key: a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6"
```

---

## 🔄 How It Works

1. **You call the API** → Your endpoint calls Housing.com API
2. **Authenticates** → Uses HMAC SHA256 hash with your encryption key
3. **Fetches leads** → Gets leads for specified time range
4. **Processes leads** → Creates new or updates existing leads in CRM
5. **Returns result** → Shows how many leads were created/updated

---

## 📊 What Gets Created in CRM

Each lead from Housing.com includes:
- ✅ Customer Name
- ✅ Phone Number (required)
- ✅ Email (if provided)
- ✅ Project Name
- ✅ Locality/Location
- ✅ Property Type (derived from apartment_names)
- ✅ Service Type (rent/resale/new-projects)
- ✅ Source: "Housing.com" (auto-tagged)
- ✅ Status: "NEW" (for new leads)
- ✅ Full metadata in Notes

---

## ⏰ Recommended Schedule

**Manual:** Run the fetch command whenever you want
**Automated:** Set up a cron job to fetch every 1-2 hours

### Example Cron Job (Every 2 hours):
```bash
0 */2 * * * curl -X POST https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/housing/fetch-leads -H "Content-Type: application/json" -H "x-api-key: a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6" -d '{}'
```

---

## 👀 View Leads in CRM

1. **Login:** https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app
2. **Email:** `admin@flatrix.com`
3. **Password:** `admin123`
4. **Go to Leads** → Filter by Source: "Housing.com"

---

## 📝 Advanced Usage

### Fetch Specific Date Range:
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

### Filter by Project IDs:
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

## 🎉 Test Results

**First Test:** ✓ Success!
- Fetched: 2 leads
- Created: 1 new lead
- Updated: 1 existing lead

---

## 📚 Documentation

- **Full Guide:** `HOUSING_INTEGRATION.md`
- **Test Script:** `test-housing-integration.sh`
- **Housing Docs:** `Housing/` folder

---

## ✅ Configuration Summary

| Item | Status |
|------|--------|
| **API Endpoint** | ✅ Deployed |
| **Profile ID** | ✅ Configured (51846085) |
| **Encryption Key** | ✅ Configured |
| **Fetch API Key** | ✅ Generated |
| **Database** | ✅ Connected |
| **Testing** | ✅ Passed |
| **Production** | ✅ Live |

---

## 🚀 You're All Set!

**The integration is ready to use.** Start fetching leads by running the curl command or test script!

**Questions?** Check `HOUSING_INTEGRATION.md` for detailed documentation.

---

**Last Updated:** October 23, 2025
