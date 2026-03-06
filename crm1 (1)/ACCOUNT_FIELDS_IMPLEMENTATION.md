# Account Fields Configuration System - Implementation Guide

## Overview
This system allows administrators to configure which fields appear in the account creation form. Out of 40+ fields, 9 are mandatory and always visible, while the rest can be toggled on/off by admins.

---

## Installation Steps

### 1. Execute Supabase SQL Scripts

Run these scripts **in order** in your Supabase SQL Editor:

#### Step 1: Create Tables
```bash
# File: supabase/create_account_field_configurations.sql
```
This creates:
- `account_field_configurations` table
- `industry_subindustry_mapping` table
- Indexes and RLS policies

#### Step 2: Update Accounts Table
```bash
# File: supabase/update_accounts_table_schema.sql
```
This adds missing columns to the `accounts` table for all new fields.

#### Step 3: Seed Default Configuration
```bash
# File: supabase/seed_default_field_configurations.sql
```
This:
- Creates the seed function
- Sets up auto-seeding trigger for new companies
- Seeds configurations for all existing companies

---

### 2. Verify Database Setup

After running the scripts, verify in Supabase:

#### Check Tables Created
```sql
SELECT table_name
FROM information_schema.tables
WHERE table_name IN ('account_field_configurations', 'industry_subindustry_mapping');
```

#### Check Field Configurations Seeded
```sql
SELECT company_id, COUNT(*) as field_count
FROM account_field_configurations
GROUP BY company_id;
```
You should see ~50 fields per company.

#### Check Industry Mappings
```sql
SELECT industry, COUNT(*) as subindustry_count
FROM industry_subindustry_mapping
GROUP BY industry;
```
You should see 9 industries with their subindustries.

---

### 3. Test the System

#### A. Access Admin Field Configuration Page
1. Login as **Super Admin** or **Company Admin**
2. Navigate to: `/admin/account-fields`
3. You should see:
   - Mandatory Fields tab (9 fields - read-only)
   - Optional Fields tab (~40 fields - toggleable)
   - Live preview panel on the right

#### B. Configure Fields
1. Go to **Optional Fields** tab
2. Toggle switches to enable/disable fields
3. Use up/down arrows to reorder fields within sections
4. Click **Save Changes**

#### C. Test Account Creation Form
1. Navigate to: `/accounts/add`
2. The form should now show only the enabled fields
3. Fields should appear in the order you configured
4. Test the cascading dropdown:
   - Select an **Industry** (e.g., "Research")
   - The **Sub-Industry** dropdown should populate with relevant options

---

## Mandatory Fields (9 - Always Visible)

These fields cannot be disabled:

1. **account_id** - Auto-generated unique ID
2. **account_name** - Company name
3. **headquarters_address** - Main office address
4. **main_phone** - Primary phone number
5. **customer_segment** - Dropdown: Government, Academic, Healthcare_Public, Healthcare_Private, Pharma, Biotech, Manufacturing, Research_Private
6. **account_type** - Dropdown: Govt Industry, Govt Institution, Private Industry, Private Institution
7. **acct_industry** - Dropdown: Research, Pharma Biopharma, Chemicals Petrochemicals, Healthcare Diagnostics, etc.
8. **acct_sub_industry** - Dependent dropdown (based on industry selection)
9. **account_status** - Dropdown: Active, Inactive, Prospect, Customer, Suspended

---

## Optional Fields (40+ - Configurable)

### Basic Information
- preferred_language
- created_by_id, created_date
- last_modified_by_id, last_modified_date

### Addresses
- Shipping: address, street, street2, city, state, country, postal_code
- Billing: address, street, street2, city, state, country, postal_code

### Contact Information
- primary_email
- preferred_contact_method
- email_opt_in
- alternate_phone
- website

### Business Details
- Territory
- communication_frequency
- communication_channel
- organization_size
- vendor_qualification

### Financial Information
- tax_id_number
- gstin, pan, cin
- udyam_registration
- gem_seller_id
- tax_classification
- preferred_currency, revenue_currency
- annual_revenue
- acc_budget_capacity
- payment_terms
- credit_limit, credit_status

### Advanced Fields
- academic_affiliation
- grant_funding_source
- regulatory_authority
- procurement_cycle

---

## Industry-SubIndustry Mapping

### Research
- Research Institutions
- BioTech R&D labs
- Pharma R&D labs
- Incubation Center

### Pharma Biopharma
- Pharma
- Bio-Pharma
- API
- Neutraceuticals

### Chemicals Petrochemicals
- Chemical manufacturers
- Petrochemicals
- Paints
- Pigments

### Healthcare Diagnostics
- Hospitals
- Wellness Center
- Blood banks
- Diagnostic labs
- Clinical testing

### Instrument Manufacturing
- Laboratory equipment manufacturers
- Analytical instrument companies

### Environmental Testing
- Environmental testing laboratories
- Pollution monitoring agencies

### Education
- Universities
- College
- Vocational training institutes
- Incubation Center
- Others

### Forensics
- Forensic laboratories
- Law enforcement agencies

### Testing Labs
- Regulatory
- Preclinical
- Biological
- Food
- Agricultural
- Calibration labs

---

## API Endpoints

### Account Field Configuration
```
GET    /api/admin/account-fields?companyId={id}     # Get all field configs
POST   /api/admin/account-fields                    # Create/update single field
PUT    /api/admin/account-fields                    # Bulk update fields
DELETE /api/admin/account-fields?companyId={id}&fieldName={name}  # Disable field
```

### Industry Mappings
```
GET /api/industries                         # Get all industries
GET /api/industries?industry={name}         # Get subindustries for an industry
POST /api/industries                        # Add new industry mapping (admin only)
```

### Accounts
```
POST /api/accounts  # Create new account with dynamic fields
```

---

## File Structure

```
app/
├── accounts/add/page.tsx                    # Updated to use dynamic form
├── admin/account-fields/page.tsx            # New: Field configuration page
└── api/
    ├── accounts/route.ts                    # Updated: Handles dynamic fields
    ├── admin/account-fields/route.ts        # New: Field config CRUD
    └── industries/route.ts                  # New: Industry-subindustry API

components/
├── account-fields-manager.tsx               # New: Admin UI for field config
├── dynamic-add-account-content.tsx          # New: Dynamic account form
├── dynamic-account-field.tsx                # New: Dynamic field renderer
└── add-account-content.tsx                  # Old: Keep for reference

supabase/
├── create_account_field_configurations.sql  # Creates tables
├── update_accounts_table_schema.sql         # Updates accounts table
└── seed_default_field_configurations.sql    # Seeds default configs
```

---

## Testing Checklist

### Admin Configuration
- [ ] Can access `/admin/account-fields` page
- [ ] See mandatory fields (9) in read-only state
- [ ] See optional fields (~40) with toggle switches
- [ ] Can enable/disable optional fields
- [ ] Can reorder fields using up/down buttons
- [ ] See live preview update when toggling fields
- [ ] Can save changes successfully
- [ ] Reset button discards unsaved changes

### Account Creation Form
- [ ] Form shows only enabled fields
- [ ] Mandatory fields are marked with red asterisk (*)
- [ ] Fields appear in configured order
- [ ] Industry dropdown shows all 9 industries
- [ ] Sub-industry dropdown is disabled until industry is selected
- [ ] After selecting industry, sub-industry shows correct options
- [ ] Can submit form with all mandatory fields filled
- [ ] Validation prevents submission with missing mandatory fields
- [ ] Optional fields are not required for submission

### Multi-Company Support
- [ ] Each company has its own field configuration
- [ ] Changing config in Company A doesn't affect Company B
- [ ] New companies auto-seed with default configuration

---

## Troubleshooting

### Issue: Field configurations not loading
**Solution:** Check that the seed script ran successfully:
```sql
SELECT COUNT(*) FROM account_field_configurations WHERE company_id = 'your-company-id';
```
Should return ~50.

### Issue: Sub-industry dropdown not populating
**Solution:** Verify industry mapping data:
```sql
SELECT * FROM industry_subindustry_mapping WHERE industry = 'Research';
```

### Issue: Form not showing any fields
**Solution:** Check that fields are enabled:
```sql
SELECT field_name, is_enabled
FROM account_field_configurations
WHERE company_id = 'your-company-id'
ORDER BY field_section, display_order;
```

### Issue: Cannot save form configuration
**Solution:** Check browser console for errors. Verify user has admin permissions.

---

## Adding New Fields (Future)

To add a new field to the system:

1. **Add column to accounts table:**
```sql
ALTER TABLE accounts ADD COLUMN new_field_name TEXT;
```

2. **Add to field configuration:**
```sql
INSERT INTO account_field_configurations (
    company_id, field_name, field_label, field_type,
    is_mandatory, is_enabled, field_section, display_order
) VALUES (
    'company-id', 'new_field_name', 'New Field Label', 'text',
    false, true, 'basic_info', 100
);
```

3. **No code changes needed!** The dynamic form will automatically pick it up.

---

## Migration for Existing Data

If you have existing accounts with old field names, run this migration:

```sql
-- Example: Migrate old 'name' field to 'account_name'
UPDATE accounts SET account_name = name WHERE account_name IS NULL AND name IS NOT NULL;

-- Verify migration
SELECT COUNT(*) FROM accounts WHERE account_name IS NULL;
```

---

## Security Considerations

1. **RLS Policies:** Only admins can modify field configurations
2. **Mandatory Fields:** Cannot be disabled via UI or API
3. **Validation:** Server-side validation ensures mandatory fields are present
4. **Cascading Deletes:** Deleting a company removes all its field configs

---

## Performance Optimization

1. **Caching:** Field configurations are loaded once per session
2. **Indexes:** Added on commonly queried columns
3. **Pagination:** Not needed for field configs (small dataset)

---

## Support

For issues or questions:
1. Check this documentation
2. Review Supabase logs for SQL errors
3. Check browser console for client-side errors
4. Verify user permissions (admin required)

---

## Next Steps

After successful implementation:
1. ✅ Configure fields for each company
2. ✅ Train admins on field configuration UI
3. ✅ Test account creation with various configurations
4. ✅ Monitor for any data inconsistencies
5. ✅ Consider adding field validation rules (regex, min/max)
6. ✅ Add field-level permissions (future enhancement)

---

**Implementation Complete! 🎉**

The system is now fully functional and ready for use.
