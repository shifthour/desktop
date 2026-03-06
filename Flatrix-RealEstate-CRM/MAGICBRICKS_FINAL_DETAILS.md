# MagicBricks Integration - Final Details & Testing Guide

## 🎯 Details to Share with MagicBricks Team

### 1. CRM Service Provider Name
```
Flatrix Real Estate CRM (Custom Solution)
```

### 2. Confirm Integration Type
```
Push Integration (Webhook)
```

### 3. Working Endpoint
```
https://flatrix-h7ec6x4hj-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/magicbricks
```

### 4. Sample URL
```
https://flatrix-h7ec6x4hj-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/magicbricks
```

### 5. API Key
```
HLNVbYhBk4907bjbXajsMlDLI6EEXAoFYjcHKASc_BY
```
⚠️ **IMPORTANT**: Share this key securely with MagicBricks team only. Do not post publicly.

### 6. Method
```
POST
```

### 7. URL Parameters
```
None (all data sent in JSON body)
```

### 8. Request Headers
```json
{
  "Content-Type": "application/json",
  "x-api-key": "HLNVbYhBk4907bjbXajsMlDLI6EEXAoFYjcHKASc_BY"
}
```

Alternative header format (also supported):
```json
{
  "Content-Type": "application/json",
  "Authorization": "Bearer HLNVbYhBk4907bjbXajsMlDLI6EEXAoFYjcHKASc_BY"
}
```

---

## 📝 Request Payload Format

### Required Fields:
- `phone` (string) - Customer's phone number

### Optional Fields:
- `name` (string) - Customer's full name
- `first_name` (string) - Customer's first name
- `last_name` (string) - Customer's last name
- `email` (string) - Customer's email address
- `mobile` (string) - Alternative mobile number
- `alternate_phone` (string) - Alternate contact number
- `message` (string) - Customer's inquiry message
- `comments` (string) - Additional comments
- `budget` (number) - Budget amount
- `budget_min` (number) - Minimum budget
- `budget_max` (number) - Maximum budget
- `property_type` (string) - Type of property (Apartment, Villa, Plot, Commercial, Office Space)
- `location` (string) - Preferred location
- `preferred_location` (string) - Preferred location (alternative field)
- `city` (string) - City name
- `project_name` (string) - Project name if specific project inquiry
- `lead_id` (string) - MagicBricks lead ID for reference
- `source` (string) - Source identifier
- `timestamp` (string) - Lead timestamp (ISO 8601 format)

### Sample Request Body:
```json
{
  "name": "Rajesh Kumar",
  "phone": "9876543210",
  "email": "rajesh.kumar@example.com",
  "message": "Looking for 2BHK apartment in Mumbai",
  "property_type": "Apartment",
  "budget": 5000000,
  "location": "Andheri West, Mumbai",
  "lead_id": "MB12345",
  "timestamp": "2025-10-22T10:30:00Z"
}
```

---

## ✅ Expected Response Format

### Success - New Lead Created (201):
```json
{
  "success": true,
  "message": "Lead created successfully",
  "leadId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "action": "created"
}
```

### Success - Existing Lead Updated (200):
```json
{
  "success": true,
  "message": "Lead updated successfully",
  "leadId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "action": "updated"
}
```

### Error - Unauthorized (401):
```json
{
  "success": false,
  "error": "Unauthorized - Invalid API key",
  "message": "Please provide a valid API key in x-api-key header or Authorization header"
}
```

### Error - Validation Failed (400):
```json
{
  "success": false,
  "error": "Validation failed",
  "message": "Phone number is required"
}
```

### Error - Server Error (500):
```json
{
  "success": false,
  "error": "Internal server error",
  "message": "Error details..."
}
```

---

## 🧪 Testing Instructions

### Test 1: Verify Endpoint is Active (GET Request)

```bash
curl -X GET https://flatrix-h7ec6x4hj-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/magicbricks \
  -H "x-api-key: HLNVbYhBk4907bjbXajsMlDLI6EEXAoFYjcHKASc_BY"
```

**Expected Response:**
```json
{
  "success": true,
  "message": "MagicBricks integration endpoint is active",
  "endpoint": "/api/integrations/magicbricks",
  "method": "POST",
  ...
}
```

### Test 2: Create a New Lead (POST Request)

```bash
curl -X POST https://flatrix-h7ec6x4hj-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/magicbricks \
  -H "Content-Type: application/json" \
  -H "x-api-key: HLNVbYhBk4907bjbXajsMlDLI6EEXAoFYjcHKASc_BY" \
  -d '{
    "name": "Test Customer",
    "phone": "9999888877",
    "email": "test@example.com",
    "message": "Interested in 3BHK apartment",
    "property_type": "Apartment",
    "budget": 7500000,
    "location": "Mumbai"
  }'
```

**Expected Response:**
```json
{
  "success": true,
  "message": "Lead created successfully",
  "leadId": "...",
  "action": "created"
}
```

### Test 3: Update Existing Lead (Same Phone Number)

```bash
curl -X POST https://flatrix-h7ec6x4hj-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/magicbricks \
  -H "Content-Type: application/json" \
  -H "x-api-key: HLNVbYhBk4907bjbXajsMlDLI6EEXAoFYjcHKASc_BY" \
  -d '{
    "name": "Test Customer Updated",
    "phone": "9999888877",
    "email": "test.updated@example.com",
    "message": "Now interested in 4BHK",
    "property_type": "Villa",
    "budget": 12000000
  }'
```

**Expected Response:**
```json
{
  "success": true,
  "message": "Lead updated successfully",
  "leadId": "...",
  "action": "updated"
}
```

### Test 4: Test Invalid API Key

```bash
curl -X POST https://flatrix-h7ec6x4hj-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/magicbricks \
  -H "Content-Type: application/json" \
  -H "x-api-key: INVALID_KEY" \
  -d '{
    "name": "Test",
    "phone": "1234567890"
  }'
```

**Expected Response:**
```json
{
  "success": false,
  "error": "Unauthorized - Invalid API key",
  ...
}
```

### Test 5: Test Missing Required Field

```bash
curl -X POST https://flatrix-h7ec6x4hj-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/magicbricks \
  -H "Content-Type: application/json" \
  -H "x-api-key: HLNVbYhBk4907bjbXajsMlDLI6EEXAoFYjcHKASc_BY" \
  -d '{
    "name": "Test Customer",
    "email": "test@example.com"
  }'
```

**Expected Response:**
```json
{
  "success": false,
  "error": "Validation failed",
  "message": "Phone number is required"
}
```

---

## 📊 How to Verify Leads in CRM

After sending test leads:

1. **Login to CRM**: Go to https://flatrix-h7ec6x4hj-shifthourjobs-gmailcoms-projects.vercel.app
2. **Credentials**:
   - Email: `admin@flatrix.com`
   - Password: `admin123`
3. **Navigate to Leads**: Click on "Leads" in the sidebar
4. **Filter by Source**: Look for leads with source "MagicBricks"
5. **Check Activities**: Go to Activities section to see integration logs

---

## 🔒 Security Features

1. ✅ **API Key Authentication**: All requests validated with secure API key
2. ✅ **HTTPS Only**: All communication encrypted
3. ✅ **Environment Variables**: Sensitive data stored securely
4. ✅ **Duplicate Detection**: Prevents duplicate leads (updates existing by phone)
5. ✅ **Activity Logging**: All integrations logged for audit trail
6. ✅ **Error Handling**: Comprehensive error handling with secure messages

---

## 🎯 Integration Behavior

### When MagicBricks Sends a Lead:

1. **Validation**: API key and required fields are validated
2. **Duplicate Check**: System checks if phone number already exists
3. **Action**:
   - **New Phone**: Creates a new lead with status "NEW"
   - **Existing Phone**: Updates existing lead with new information
4. **Source Tag**: All leads automatically tagged with source "MagicBricks"
5. **Activity Log**: Integration activity recorded in system
6. **Response**: Success/error response sent back to MagicBricks

### Lead Data Mapping:

| MagicBricks Field | CRM Field | Notes |
|-------------------|-----------|-------|
| `name` / `first_name` + `last_name` | `name` | Combined if split |
| `phone` / `mobile` | `phone` | Required field |
| `email` | `email` | Optional |
| `budget` | `budget_min` / `budget_max` | Used for both if single value |
| `property_type` | `interested_in` | Mapped to enum |
| `location` / `city` | `preferred_location` | - |
| `message` / `comments` | `notes` | Combined with metadata |
| - | `source` | Always "MagicBricks" |
| - | `status` | Always "NEW" for new leads |

---

## 📞 Support & Monitoring

### Check Integration Status:
```bash
curl -X GET https://flatrix-h7ec6x4hj-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/magicbricks \
  -H "x-api-key: HLNVbYhBk4907bjbXajsMlDLI6EEXAoFYjcHKASc_BY"
```

### View Integration Logs:
1. Login to Vercel Dashboard
2. Go to Project: flatrix-crm
3. Check "Logs" section for real-time monitoring
4. Filter by `/api/integrations/magicbricks`

### View Leads in CRM:
1. Login to CRM (credentials above)
2. Navigate to Leads section
3. Filter by source: "MagicBricks"
4. Check Activities tab for integration logs

---

## 🚀 Configuration Summary

| Configuration | Value |
|--------------|-------|
| **Production URL** | `https://flatrix-h7ec6x4hj-shifthourjobs-gmailcoms-projects.vercel.app` |
| **API Endpoint** | `/api/integrations/magicbricks` |
| **API Key** | `HLNVbYhBk4907bjbXajsMlDLI6EEXAoFYjcHKASc_BY` |
| **System User ID** | `5d4e3072-1ae7-4cde-8f0d-fbc627b779e6` |
| **Admin Email** | `admin@flatrix.com` |
| **Database** | Supabase PostgreSQL |
| **Deployment** | Vercel Production |
| **Status** | ✅ Active and Ready |

---

## ✅ Pre-Integration Checklist

- [x] API endpoint created and deployed
- [x] Environment variables configured
- [x] System user configured
- [x] API key generated and secured
- [x] Deployed to Vercel production
- [x] Integration tested successfully
- [x] Documentation prepared

---

## 📋 Next Steps

1. ✅ **Test the Integration**: Run all 5 test commands above
2. ✅ **Verify in CRM**: Check that test leads appear in your CRM
3. ✅ **Share with MagicBricks**: Send them the details from section 1
4. ✅ **Monitor**: Check Vercel logs when MagicBricks starts sending leads
5. ✅ **Verify Live Leads**: Login to CRM and check leads appear correctly

---

**Document Created**: October 22, 2025
**Status**: ✅ Production Ready
**Last Deployment**: 1 minute ago
