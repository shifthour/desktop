# 📋 MagicBricks Integration - Complete Parameters Documentation

## 🎯 Integration Details (Updated)

### 1. CRM Service Provider Name
```
Flatrix Real Estate CRM (Custom Solution)
```

### 2. Integration Type
```
Push Integration (Webhook)
```

### 3. Working Endpoint URL
```
https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/magicbricks
```

### 4. Sample URL
```
https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/magicbricks
```

### 5. API Key
```
9bb6bc225cec3f4bd79f9aae6b6657a16047886b79ceb7772ba81086081bf2ab
```

### 6. HTTP Method
```
POST
```

### 7. URL Parameters
```
None - All data sent in JSON request body
```

---

## 📤 Request Headers Required

MagicBricks should send these headers with every request:

```http
Content-Type: application/json
x-api-key: 9bb6bc225cec3f4bd79f9aae6b6657a16047886b79ceb7772ba81086081bf2ab
```

**Alternative Header Format** (also supported):
```http
Content-Type: application/json
Authorization: Bearer 9bb6bc225cec3f4bd79f9aae6b6657a16047886b79ceb7772ba81086081bf2ab
```

---

## 📝 JSON Body Parameters

### ✅ Required Parameters

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `phone` or `mobile` | string | Customer's primary phone number (at least one required) | `"9876543210"` |

### 📋 Optional Parameters (Recommended)

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `name` | string | Customer's full name | `"Rajesh Kumar"` |
| `first_name` | string | Customer's first name (alternative to name) | `"Rajesh"` |
| `last_name` | string | Customer's last name (used with first_name) | `"Kumar"` |
| `email` | string | Customer's email address | `"rajesh@example.com"` |
| `mobile` | string | Alternative mobile number field | `"9876543210"` |
| `alternate_phone` | string | Secondary contact number | `"9123456789"` |
| `message` | string | Customer's inquiry message | `"Looking for 2BHK in Andheri"` |
| `comments` | string | Additional comments or notes | `"Urgent requirement"` |
| `budget` | number | Budget amount in INR | `5000000` |
| `budget_min` | number | Minimum budget amount | `4500000` |
| `budget_max` | number | Maximum budget amount | `5500000` |
| `property_type` | string | Type of property interested in | `"Apartment"` / `"Villa"` / `"Plot"` |
| `location` | string | Preferred location/area | `"Andheri West, Mumbai"` |
| `preferred_location` | string | Alternative field for location | `"Mumbai"` |
| `city` | string | City name | `"Mumbai"` |
| `project_name` | string | Specific project name if applicable | `"Lodha Meridian"` |
| `lead_id` | string | MagicBricks internal lead ID | `"MB12345678"` |
| `source` | string | Source/campaign identifier | `"Website"` / `"Mobile App"` |
| `timestamp` | string | Lead creation timestamp (ISO 8601) | `"2025-10-23T10:30:00Z"` |

### 🏷️ Property Type Values Accepted

Our system maps these property types automatically:

- `"Apartment"` or `"Flat"` → Mapped to APARTMENT
- `"Villa"` or `"Independent House"` → Mapped to VILLA
- `"Plot"` or `"Land"` → Mapped to PLOT
- `"Commercial"` → Mapped to COMMERCIAL
- `"Office"` or `"Office Space"` → Mapped to OFFICE_SPACE

---

## 📨 Complete Sample Request

### Example 1: Basic Lead (Minimal Required)

```bash
curl -X POST https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/magicbricks \
  -H "Content-Type: application/json" \
  -H "x-api-key: 9bb6bc225cec3f4bd79f9aae6b6657a16047886b79ceb7772ba81086081bf2ab" \
  -d '{
    "phone": "9876543210"
  }'
```

### Example 2: Detailed Lead (Recommended Format)

```bash
curl -X POST https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/magicbricks \
  -H "Content-Type: application/json" \
  -H "x-api-key: 9bb6bc225cec3f4bd79f9aae6b6657a16047886b79ceb7772ba81086081bf2ab" \
  -d '{
    "name": "Rajesh Kumar",
    "phone": "9876543210",
    "email": "rajesh.kumar@example.com",
    "message": "Looking for 2BHK apartment in Andheri West",
    "property_type": "Apartment",
    "budget": 5000000,
    "budget_min": 4500000,
    "budget_max": 5500000,
    "location": "Andheri West, Mumbai",
    "city": "Mumbai",
    "project_name": "Lodha Meridian",
    "lead_id": "MB12345678",
    "source": "Website",
    "timestamp": "2025-10-23T10:30:00Z"
  }'
```

### Example 3: Lead with Split Name

```bash
curl -X POST https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/magicbricks \
  -H "Content-Type: application/json" \
  -H "x-api-key: 9bb6bc225cec3f4bd79f9aae6b6657a16047886b79ceb7772ba81086081bf2ab" \
  -d '{
    "first_name": "Priya",
    "last_name": "Sharma",
    "mobile": "9123456789",
    "email": "priya.sharma@example.com",
    "comments": "Interested in 3BHK with parking",
    "property_type": "Villa",
    "budget": 12000000,
    "preferred_location": "Bandra, Mumbai"
  }'
```

### Example 4: Commercial Property Lead

```bash
curl -X POST https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/magicbricks \
  -H "Content-Type: application/json" \
  -H "x-api-key: 9bb6bc225cec3f4bd79f9aae6b6657a16047886b79ceb7772ba81086081bf2ab" \
  -d '{
    "name": "Amit Patel",
    "phone": "9988776655",
    "email": "amit@company.com",
    "message": "Looking for office space for startup",
    "property_type": "Office Space",
    "budget": 8000000,
    "location": "BKC, Mumbai",
    "comments": "Prefer ground floor with parking"
  }'
```

---

## ✅ Response Formats

### Success - New Lead Created (HTTP 201)

```json
{
  "success": true,
  "message": "Lead created successfully",
  "leadId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "action": "created"
}
```

### Success - Existing Lead Updated (HTTP 200)

When a lead with the same phone number already exists:

```json
{
  "success": true,
  "message": "Lead updated successfully",
  "leadId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "action": "updated"
}
```

### Error - Unauthorized (HTTP 401)

```json
{
  "success": false,
  "error": "Unauthorized - Invalid API key",
  "message": "Please provide a valid API key in x-api-key header or Authorization header"
}
```

### Error - Missing Required Field (HTTP 400)

```json
{
  "success": false,
  "error": "Validation failed",
  "message": "Phone number is required"
}
```

### Error - Server Error (HTTP 500)

```json
{
  "success": false,
  "error": "Internal server error",
  "message": "Failed to process lead"
}
```

---

## 🔄 How Our System Handles Leads

### Duplicate Prevention
- System checks if phone number already exists in database
- **New Phone Number**: Creates a new lead with status "NEW"
- **Existing Phone Number**: Updates the existing lead with new information and appends to notes

### Automatic Field Mapping
1. **Name**: Uses `name` field, or combines `first_name` + `last_name`
2. **Phone**: Uses `phone` field, falls back to `mobile` if not provided
3. **Budget**: If only `budget` provided, uses it for both min and max
4. **Location**: Uses `preferred_location`, falls back to `location`, then `city`
5. **Property Type**: Automatically maps to our internal enums

### Source Tagging
- All leads automatically tagged with source: **"MagicBricks"**
- This allows easy filtering in CRM

### Activity Logging
- Every lead creation/update is logged in activities table
- Includes full payload metadata for auditing

---

## 🧪 Testing Instructions for MagicBricks Team

### Test 1: Verify Endpoint Connectivity

```bash
curl -X GET https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/magicbricks \
  -H "x-api-key: 9bb6bc225cec3f4bd79f9aae6b6657a16047886b79ceb7772ba81086081bf2ab"
```

**Expected**: HTTP 200 with endpoint information

### Test 2: Send Basic Lead

```bash
curl -X POST https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/magicbricks \
  -H "Content-Type: application/json" \
  -H "x-api-key: 9bb6bc225cec3f4bd79f9aae6b6657a16047886b79ceb7772ba81086081bf2ab" \
  -d '{
    "name": "Test Lead MagicBricks",
    "phone": "9999000001",
    "email": "test@magicbricks.com",
    "message": "Test integration",
    "property_type": "Apartment",
    "budget": 5000000,
    "location": "Mumbai"
  }'
```

**Expected**: HTTP 201 with `"action": "created"`

### Test 3: Update Same Lead

```bash
curl -X POST https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/magicbricks \
  -H "Content-Type: application/json" \
  -H "x-api-key: 9bb6bc225cec3f4bd79f9aae6b6657a16047886b79ceb7772ba81086081bf2ab" \
  -d '{
    "name": "Test Lead MagicBricks Updated",
    "phone": "9999000001",
    "email": "test@magicbricks.com",
    "message": "Updated inquiry - now interested in 3BHK",
    "property_type": "Villa",
    "budget": 8000000
  }'
```

**Expected**: HTTP 200 with `"action": "updated"`

### Test 4: Test Authentication Failure

```bash
curl -X POST https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/magicbricks \
  -H "Content-Type: application/json" \
  -H "x-api-key: WRONG_KEY" \
  -d '{
    "name": "Test",
    "phone": "9999000002"
  }'
```

**Expected**: HTTP 401 with unauthorized error

---

## 📊 Data Flow Summary

```
MagicBricks System
       ↓
  [HTTPS POST Request]
       ↓
  API Key Validation
       ↓
  Phone Number Check
       ↓
    ┌─────┴─────┐
    ↓           ↓
New Phone   Existing Phone
    ↓           ↓
Create Lead  Update Lead
    ↓           ↓
Log Activity  Log Activity
    ↓           ↓
  Return Success Response
       ↓
  Lead Visible in CRM
```

---

## 🔐 Security Features

- ✅ **API Key Authentication**: Every request validated
- ✅ **HTTPS Only**: All communication encrypted (TLS 1.2+)
- ✅ **Environment Variables**: Credentials stored securely
- ✅ **Duplicate Prevention**: Automatic phone number matching
- ✅ **Activity Audit Trail**: All actions logged with metadata
- ✅ **Error Masking**: No sensitive data in error messages

---

## 📞 Support & Monitoring

### For MagicBricks Team
- **Test Endpoint**: Use GET method to verify connectivity
- **Monitor Responses**: Check HTTP status codes (201/200 = success)
- **Error Handling**: Log and retry on 5xx errors
- **Timeout**: Set request timeout to 30 seconds

### For Flatrix Team
- **View Leads**: Login to CRM → Leads → Filter by Source: "MagicBricks"
- **Check Logs**: Vercel Dashboard → Logs → Filter by endpoint path
- **Monitor API Key**: Vercel Dashboard → Environment Variables

---

## 🎯 Important Notes for MagicBricks

1. **Required Field**: At minimum, send `phone` or `mobile` field
2. **Recommended Fields**: Include `name`, `email`, `message`, `property_type`, `budget`, `location` for best CRM experience
3. **Duplicate Handling**: System automatically handles duplicates by phone number
4. **Response Codes**:
   - 201 = New lead created
   - 200 = Existing lead updated
   - 401 = Authentication failed
   - 400 = Validation error
   - 500 = Server error (retry recommended)
5. **Retry Logic**: Implement exponential backoff for 5xx errors
6. **Webhook Security**: Always include API key in headers

---

## ✅ Integration Checklist for MagicBricks

- [ ] Configure webhook URL in MagicBricks system
- [ ] Add API key to request headers
- [ ] Map MagicBricks lead fields to JSON parameters
- [ ] Set Content-Type header to `application/json`
- [ ] Implement retry logic for failures
- [ ] Test with sample leads
- [ ] Monitor success/error responses
- [ ] Confirm leads appear in Flatrix CRM

---

## 📋 Quick Reference

**Endpoint**: `https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app/api/integrations/magicbricks`

**Method**: `POST`

**Headers**:
```
Content-Type: application/json
x-api-key: 9bb6bc225cec3f4bd79f9aae6b6657a16047886b79ceb7772ba81086081bf2ab
```

**Minimum Body**:
```json
{ "phone": "9876543210" }
```

**Recommended Body**:
```json
{
  "name": "Customer Name",
  "phone": "9876543210",
  "email": "email@example.com",
  "message": "Customer inquiry",
  "property_type": "Apartment",
  "budget": 5000000,
  "location": "City/Area",
  "lead_id": "MB123456"
}
```

---

**Document Updated**: October 23, 2025
**Endpoint Status**: ✅ Live and Ready
**Current Production URL**: https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app
