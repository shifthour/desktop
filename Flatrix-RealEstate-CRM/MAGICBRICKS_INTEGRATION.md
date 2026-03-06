# MagicBricks Integration Guide

## Overview

This document provides instructions for integrating MagicBricks leads into your Flatrix Real Estate CRM using a secure push integration (webhook).

## Setup Instructions

### Step 1: Generate API Key and Configure Environment

Run the setup script to generate a secure API key:

```bash
node scripts/setup-magicbricks-integration.js
```

This script will:
1. Generate a secure API key
2. Help you configure the system user ID
3. Update your `.env.local` file
4. Provide all the information needed for MagicBricks team

### Step 2: Create System User (if not exists)

Before running the integration, you need a system user in your CRM:

1. Log into your CRM as an admin
2. Create a new user with the following details:
   - Name: "MagicBricks System"
   - Email: "system-magicbricks@your-company.com"
   - Role: ADMIN or AGENT
   - Mark as active

3. Copy the user ID and update it in `.env.local`:
   ```
   SYSTEM_USER_ID=<your-user-id>
   ```

### Step 3: Deploy Your Application

Make sure your application is deployed and accessible:

```bash
# If using Vercel
vercel --prod

# Or if using other hosting
npm run build
npm start
```

### Step 4: Configure Environment Variables in Production

Add these environment variables to your production deployment:

```
MAGICBRICKS_API_KEY=<your-generated-api-key>
SYSTEM_USER_ID=<your-system-user-id>
```

**For Vercel:**
1. Go to your project settings
2. Navigate to Environment Variables
3. Add both variables
4. Redeploy your application

### Step 5: Test the Integration

Test the endpoint before sharing with MagicBricks:

```bash
# Test with curl
curl -X POST https://your-domain.vercel.app/api/integrations/magicbricks \
  -H "Content-Type: application/json" \
  -H "x-api-key: YOUR_API_KEY" \
  -d '{
    "name": "Test User",
    "phone": "9876543210",
    "email": "test@example.com",
    "message": "Interested in 2BHK apartment",
    "property_type": "Apartment",
    "budget": "5000000",
    "location": "Mumbai"
  }'
```

Expected response:
```json
{
  "success": true,
  "message": "Lead created successfully",
  "leadId": "clxxxxx...",
  "action": "created"
}
```

## Information to Share with MagicBricks Team

Provide the following details to the MagicBricks integration team:

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
https://your-domain.vercel.app/api/integrations/magicbricks
```
Replace `your-domain.vercel.app` with your actual domain.

### 4. Sample URL
```
https://your-domain.vercel.app/api/integrations/magicbricks
```

### 5. API Key
```
<Your generated API key from Step 1>
```
**Note:** Keep this secure! Only share it through secure channels.

### 6. Method
```
POST
```

### 7. URL Parameters
```
None (all data is sent in the request body as JSON)
```

### 8. Request Headers
```
Content-Type: application/json
x-api-key: <YOUR_API_KEY>
```

Alternatively, the API key can be sent as:
```
Authorization: Bearer <YOUR_API_KEY>
```

### 9. Request Body Format

The endpoint accepts the following JSON payload:

**Required Fields:**
- `phone` or `mobile` (string) - Contact phone number

**Optional Fields:**
- `name` (string) - Full name
- `first_name` (string) - First name
- `last_name` (string) - Last name
- `email` (string) - Email address
- `mobile` (string) - Mobile number
- `alternate_phone` (string) - Alternate contact number
- `message` (string) - Lead message/inquiry
- `comments` (string) - Additional comments
- `budget` (number or string) - Budget amount
- `property_type` (string) - Type of property (Apartment, Villa, Plot, Commercial, Office Space)
- `location` (string) - Preferred location
- `preferred_location` (string) - Preferred location
- `city` (string) - City name
- `lead_id` (string) - MagicBricks lead ID for reference
- `source` (string) - Lead source identifier
- `timestamp` (string) - Lead timestamp (ISO format)

**Sample Request:**
```json
{
  "name": "Rajesh Kumar",
  "phone": "9876543210",
  "email": "rajesh.kumar@example.com",
  "message": "Looking for 2BHK apartment in Mumbai",
  "property_type": "Apartment",
  "budget": "5000000",
  "location": "Andheri West, Mumbai",
  "lead_id": "MB12345",
  "timestamp": "2025-10-22T10:30:00Z"
}
```

### 10. Response Format

**Success Response (New Lead):**
```json
{
  "success": true,
  "message": "Lead created successfully",
  "leadId": "clxxxxx...",
  "action": "created"
}
```
Status Code: `201 Created`

**Success Response (Updated Lead):**
```json
{
  "success": true,
  "message": "Lead updated successfully",
  "leadId": "clxxxxx...",
  "action": "updated"
}
```
Status Code: `200 OK`

**Error Response (Missing API Key):**
```json
{
  "success": false,
  "error": "Unauthorized - Invalid API key",
  "message": "Please provide a valid API key in x-api-key header or Authorization header"
}
```
Status Code: `401 Unauthorized`

**Error Response (Missing Required Fields):**
```json
{
  "success": false,
  "error": "Validation failed",
  "message": "Phone number is required"
}
```
Status Code: `400 Bad Request`

**Error Response (Server Error):**
```json
{
  "success": false,
  "error": "Internal server error",
  "message": "Error details..."
}
```
Status Code: `500 Internal Server Error`

## How the Integration Works

1. **Lead Reception**: MagicBricks sends lead data via POST request to your endpoint
2. **Authentication**: The API validates the API key from the request headers
3. **Data Normalization**: The endpoint maps MagicBricks fields to your CRM schema
4. **Duplicate Check**: Checks if a lead with the same phone number exists
   - If exists: Updates the existing lead with new information
   - If new: Creates a new lead in your CRM
5. **Activity Logging**: Records the integration activity in your CRM
6. **Response**: Returns success/error response to MagicBricks

## Lead Mapping

| MagicBricks Field | CRM Field | Notes |
|-------------------|-----------|-------|
| `name` / `first_name` | `firstName` | Split name if only `name` provided |
| `last_name` | `lastName` | - |
| `email` | `email` | Optional |
| `phone` / `mobile` | `phone` | Required |
| `alternate_phone` | `alternatePhone` | Optional |
| `budget` | `budget` | Parsed as float |
| `property_type` | `interestedIn` | Mapped to enum values |
| `location` / `city` | `preferredLocation` | - |
| `message` / `comments` | `notes` | Combined with metadata |
| - | `source` | Always set to "MagicBricks" |
| - | `status` | Always set to "NEW" |

## Property Type Mapping

| MagicBricks Value | CRM Enum Value |
|-------------------|----------------|
| Apartment, Flat | APARTMENT |
| Villa, Independent House | VILLA |
| Plot, Land | PLOT |
| Commercial | COMMERCIAL |
| Office, Office Space | OFFICE_SPACE |

## Security Features

1. **API Key Authentication**: All requests require a valid API key
2. **Environment Variables**: Sensitive data stored in environment variables
3. **Request Validation**: Validates all incoming data before processing
4. **Error Handling**: Comprehensive error handling with secure error messages
5. **Audit Trail**: All lead creations/updates logged in activity table

## Monitoring and Maintenance

### Check Integration Status

You can verify the endpoint is working:

```bash
curl -X GET https://your-domain.vercel.app/api/integrations/magicbricks \
  -H "x-api-key: YOUR_API_KEY"
```

### View Incoming Leads

1. Log into your CRM
2. Navigate to Leads section
3. Filter by source: "MagicBricks"
4. Check the Activities section for integration logs

### Common Issues

**Issue: "Unauthorized - Invalid API key"**
- Solution: Verify API key matches in both .env.local and production environment

**Issue: "SYSTEM_USER_ID not configured"**
- Solution: Create a system user and add the ID to environment variables

**Issue: Leads not appearing in CRM**
- Solution: Check Activity logs for the system user to see if leads are being received
- Check application logs for any errors

## Support

For issues or questions:
1. Check application logs in your hosting platform
2. Review the Activity table for integration events
3. Test the endpoint manually using curl/Postman
4. Verify all environment variables are set correctly

## Security Best Practices

1. ✓ Never commit `.env.local` to version control
2. ✓ Rotate API keys periodically
3. ✓ Monitor for unusual activity patterns
4. ✓ Use HTTPS only (enforced by default)
5. ✓ Keep your dependencies updated
6. ✓ Review integration logs regularly

---

**Last Updated:** October 2025
**Version:** 1.0.0
