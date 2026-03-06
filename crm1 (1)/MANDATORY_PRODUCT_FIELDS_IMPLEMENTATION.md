# Mandatory Product Fields Implementation

## Overview
This document describes the implementation of three mandatory fields for the Products module:
1. **Principals** - Already exists in database, now marked as mandatory
2. **HSN Code** - New field for tax classification
3. **Unit of Measurement (UOM)** - New field for quantity calculations

## Changes Made

### 1. Database Schema Changes
**File**: `supabase/ADD_MANDATORY_PRODUCT_FIELDS.sql`

The SQL script:
- Adds `hsn_code` (VARCHAR(20)) column to products table
- Adds `unit_of_measurement` (VARCHAR(50)) column to products table
- Updates `principal` field to be mandatory in field configurations
- Adds `hsn_code` as mandatory field in field configurations
- Adds `unit_of_measurement` as mandatory field with predefined options in field configurations
- Creates indexes for better query performance

### 2. Field Configurations
All three fields are added to `product_field_configurations` table for all companies with:
- `is_mandatory: true`
- `is_enabled: true`
- Appropriate display order (11, 12, 13)
- Field-specific metadata (placeholder, help_text, field_options)

**Unit of Measurement Options**:
- Piece
- Kilogram (kg)
- Gram (g)
- Liter (L)
- Milliliter (mL)
- Meter (m)
- Centimeter (cm)
- Square Meter (sqm)
- Cubic Meter (cbm)
- Box
- Carton
- Dozen
- Pack
- Set
- Unit
- Hour
- Day
- Month
- Year

## How It Works

### 1. Admin Panel (Product Fields Manager)
**Component**: `components/product-fields-manager.tsx`

The admin panel automatically displays these fields because it:
- Fetches field configurations from `/api/admin/product-fields`
- Shows all fields including mandatory ones
- Allows admins to:
  - View field properties (mandatory status, type, section)
  - Enable/disable optional fields
  - Reorder fields
  - Edit field labels and placeholders

**Usage**: Navigate to Admin > Product Fields to manage these fields.

### 2. Product Creation Form
**Component**: `components/dynamic-add-product-content.tsx`

The add product form automatically includes these fields because it:
- Fetches field configurations from `/api/admin/product-fields?companyId={id}`
- Dynamically renders fields based on configuration
- Validates mandatory fields before submission
- Groups fields by section (Basic Info, Inventory, etc.)

**Field Rendering**:
- **Principals**: Text input field
- **HSN Code**: Text input field
- **Unit of Measurement**: Dropdown select with predefined options

**Validation**: All three fields are required - form won't submit without them.

### 3. Import Template
**Component**: `components/dynamic-import-modal.tsx`

The import template automatically includes these fields because it:
- Fetches field configurations from `/api/admin/product-fields?companyId={id}`
- Generates Excel template with columns for all enabled fields
- Uses `field_label` as column headers
- Provides sample data based on field type
- Maps imported data using `field_name` for database insertion

**Template Generation**:
1. Downloads template with three new columns:
   - "Principals" (text)
   - "HSN Code" (text)
   - "Unit of Measurement (UOM)" (dropdown values shown as sample)

2. Import Process:
   - Reads Excel file
   - Maps column headers to field names
   - Validates mandatory fields
   - Inserts products via `/api/products` POST endpoint

## API Endpoints

### 1. GET /api/admin/product-fields
- Returns field configurations for a company
- Ordered by section and display_order
- Includes all field metadata

### 2. PUT /api/admin/product-fields
- Updates field configurations (enable/disable, reorder)
- Bulk update support

### 3. POST /api/products
- Creates new product
- Validates enabled fields based on company configuration
- Stores principal, hsn_code, unit_of_measurement in database

### 4. PUT /api/products
- Updates existing product
- Same field validation as POST

## Database Structure

### products table columns (relevant)
```sql
- principal VARCHAR(255)          -- Already existed
- hsn_code VARCHAR(20)             -- NEW
- unit_of_measurement VARCHAR(50)  -- NEW
```

### product_field_configurations table
```sql
- field_name: 'principal', 'hsn_code', 'unit_of_measurement'
- field_label: Display names
- field_type: 'text' or 'select'
- is_mandatory: true
- is_enabled: true
- field_section: 'basic_info' or 'inventory'
- field_options: Array for UOM dropdown
```

## Steps to Apply Changes

### 1. Run the SQL Script in Supabase
```sql
-- Execute this in Supabase SQL Editor:
-- File: supabase/ADD_MANDATORY_PRODUCT_FIELDS.sql
```

This will:
- Add missing columns to products table
- Update product_field_configurations for all companies
- Create indexes for performance

### 2. Verify in Admin Panel
1. Login as admin
2. Navigate to Admin > Product Fields
3. Confirm you see:
   - Principals (mandatory, enabled, Basic Info section)
   - HSN Code (mandatory, enabled, Basic Info section)
   - Unit of Measurement (mandatory, enabled, Inventory section)

### 3. Test Product Creation
1. Go to Products > Add Product
2. Confirm all three fields are visible and marked as required
3. Try to save without filling them - should show validation errors
4. Fill all fields and save - should succeed

### 4. Test Import Template
1. Go to Products page
2. Click Import button
3. Download template
4. Confirm template has three columns:
   - Principals
   - HSN Code
   - Unit of Measurement (UOM)
5. Fill template and import
6. Verify products are created with all three fields

## No Code Changes Required!

The beauty of this implementation is that **NO code changes are needed** after running the SQL script. The entire application is built on a dynamic field configuration system:

- Admin panel reads from `product_field_configurations`
- Add/Edit forms read from `product_field_configurations`
- Import templates read from `product_field_configurations`
- API endpoints respect field configurations

Simply running the SQL script makes the fields appear everywhere automatically!

## Verification Checklist

After running the SQL script, verify:

- [ ] Admin panel shows all three fields as mandatory
- [ ] Add Product form displays all three fields
- [ ] Fields are marked as required with asterisk (*)
- [ ] Form validation prevents submission without these fields
- [ ] Import template includes all three columns
- [ ] Imported products include the three fields
- [ ] Exported products include the three fields
- [ ] Edit Product form shows existing values for these fields

## Production Deployment

Since there are no code changes, deployment is simple:

1. **Run SQL Script in Production Supabase**:
   - Open Supabase dashboard for production
   - Go to SQL Editor
   - Paste content from `supabase/ADD_MANDATORY_PRODUCT_FIELDS.sql`
   - Execute the script
   - Verify success messages

2. **No App Deployment Needed**:
   - The application code doesn't change
   - Changes take effect immediately after SQL execution
   - All users will see the new fields on next page load

3. **Verification**:
   - Test in production environment
   - Verify all three locations (admin, add form, import)
   - Create test product with all fields
   - Import test data

## Support & Troubleshooting

### Field not appearing?
- Check `product_field_configurations` table in Supabase
- Ensure `is_enabled = true` for the field
- Verify `company_id` matches your user's company

### Import failing?
- Check Excel column headers match `field_label` exactly
- For UOM, use values from the predefined list
- Ensure all mandatory fields have values

### Validation errors?
- Mandatory fields cannot be empty
- HSN Code: Alphanumeric, max 20 characters
- UOM: Must select from dropdown options
- Principals: Text field, max 255 characters

## Future Enhancements

If needed, you can easily:
- Add more UOM options by updating the SQL script
- Change field order by updating `display_order`
- Make fields optional by setting `is_mandatory = false`
- Add validation rules via `validation_rules` JSONB column
- Add help text via `help_text` column

All changes via SQL - no code deployment required!
