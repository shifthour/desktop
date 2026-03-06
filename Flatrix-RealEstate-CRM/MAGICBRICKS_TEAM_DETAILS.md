# Integration Details for MagicBricks Team

## Quick Reference for MagicBricks Integration

Please use the following information to set up the push integration:

---

### 1. CRM Service Provider Name
```
Flatrix Real Estate CRM (Custom Solution)
```

---

### 2. Confirm Integration Type
```
Push Integration (Webhook)
```
We support **only push integration**. We will receive leads via POST requests to our webhook endpoint.

---

### 3. Working Endpoint
```
https://YOUR-DOMAIN/api/integrations/magicbricks
```
> **Note:** Replace `YOUR-DOMAIN` with your actual deployment URL (e.g., `your-company.vercel.app`)

---

### 4. Sample URL
```
https://YOUR-DOMAIN/api/integrations/magicbricks
```

---

### 5. API Key
```
[TO BE GENERATED - Run setup script first]
```
> **Security:** The API key will be generated using the setup script and shared securely.

---

### 6. Method
```
POST
```

---

### 7. URL Parameters
```
None
```
All data should be sent in the **request body as JSON**.

---

### 8. Additional Information

#### Request Headers Required:
```
Content-Type: application/json
x-api-key: <YOUR_API_KEY>
```

#### Sample Request Payload:
```json
{
  "name": "Customer Name",
  "phone": "9876543210",
  "email": "customer@example.com",
  "message": "Interested in property",
  "property_type": "Apartment",
  "budget": "5000000",
  "location": "Mumbai",
  "lead_id": "MB12345"
}
```

#### Required Fields:
- `phone` (mandatory)

#### Optional Fields:
- `name`, `first_name`, `last_name`
- `email`, `mobile`, `alternate_phone`
- `message`, `comments`
- `budget`, `property_type`, `location`, `city`
- `lead_id`, `source`, `timestamp`

#### Expected Response Format:

**Success (201 Created):**
```json
{
  "success": true,
  "message": "Lead created successfully",
  "leadId": "clxxxxx...",
  "action": "created"
}
```

**Error (401 Unauthorized):**
```json
{
  "success": false,
  "error": "Unauthorized - Invalid API key",
  "message": "Please provide a valid API key"
}
```

**Error (400 Bad Request):**
```json
{
  "success": false,
  "error": "Validation failed",
  "message": "Phone number is required"
}
```

---

### Security & Authentication

- **Authentication Method:** API Key authentication via header
- **Header Name:** `x-api-key` or `Authorization: Bearer <key>`
- **Protocol:** HTTPS only (required)
- **Data Format:** JSON
- **Character Encoding:** UTF-8

---

### Duplicate Handling

Our system automatically handles duplicate leads:
- If a lead with the same phone number exists, it will be **updated** with new information
- If it's a new phone number, a new lead will be **created**
- Both scenarios return a success response with appropriate action ("created" or "updated")

---

### Lead Source Tracking

All leads received through this integration will be automatically tagged with:
- **Source:** "MagicBricks"
- **Status:** "NEW"
- All leads will be tracked in our activity log for audit purposes

---

### Contact Information

For any questions or support regarding this integration:
- Check endpoint status: `GET https://YOUR-DOMAIN/api/integrations/magicbricks` (with API key)
- Review our full integration documentation: `MAGICBRICKS_INTEGRATION.md`

---

## Setup Checklist (Internal Use)

Before sharing with MagicBricks team:

- [ ] Run `node scripts/setup-magicbricks-integration.js` to generate API key
- [ ] Create system user in CRM and update `SYSTEM_USER_ID`
- [ ] Deploy application to production
- [ ] Set environment variables in production (`MAGICBRICKS_API_KEY`, `SYSTEM_USER_ID`)
- [ ] Test the endpoint with sample data
- [ ] Replace `YOUR-DOMAIN` with actual domain in this document
- [ ] Share API key securely with MagicBricks team
- [ ] Monitor first few leads to ensure integration is working correctly

---

**Document Version:** 1.0
**Last Updated:** October 2025
