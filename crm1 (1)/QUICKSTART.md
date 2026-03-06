# Account Field Configuration - Quick Start Guide

## 🚀 Setup in 3 Steps (5 minutes)

### Step 1: Run SQL Script (2 minutes)
1. Open **Supabase Dashboard**
2. Go to **SQL Editor**
3. Create a new query
4. Copy and paste contents from: `supabase/EXECUTE_THIS_COMPLETE_SETUP.sql`
5. Click **Run** (or press F5)
6. Wait for success message

**⚠️ Important:** If you prefer to run scripts separately:
- Run `create_account_field_configurations.sql` first
- Then `update_accounts_table_schema.sql`
- Finally `seed_default_field_configurations.sql`

### Step 2: Verify Installation (1 minute)
Run this verification query in Supabase SQL Editor:

```sql
-- Should return ~50 for each company
SELECT company_id, COUNT(*) as field_count
FROM account_field_configurations
GROUP BY company_id;

-- Should return 37 rows (industry-subindustry mappings)
SELECT COUNT(*) FROM industry_subindustry_mapping;
```

✅ If you see data, installation was successful!

### Step 3: Configure Fields (2 minutes)
1. **Login** to your CRM as Admin
2. Navigate to: **Admin → Account Field Configuration**
   - URL: `http://localhost:3000/admin/account-fields`
3. Go to **Optional Fields** tab
4. Toggle fields **ON/OFF** as needed
5. Click **Save Changes**

---

## ✨ Test It Out

### Create an Account with Dynamic Fields
1. Navigate to: **Accounts → Add Account**
   - URL: `http://localhost:3000/accounts/add`
2. You'll see only the fields you enabled
3. Fill in the required fields (marked with *)
4. Test the cascading dropdown:
   - Select **Industry**: "Research"
   - **Sub-Industry** dropdown will show: Research Institutions, BioTech R&D labs, etc.
5. Click **Save Account**

---

## 📋 What You Get

### 9 Mandatory Fields (Always Visible)
✅ Account Name
✅ Headquarters Address
✅ Main Phone
✅ Customer Segment (dropdown)
✅ Account Type (dropdown)
✅ Industry (dropdown)
✅ Sub-Industry (cascading dropdown)
✅ Account Status (dropdown)
✅ Account ID (auto-generated)

### 40+ Optional Fields (Configurable)
Configure which optional fields to show:
- ⚙️ **Addresses**: Shipping, Billing (7 fields each)
- 📞 **Contact**: Email, Phone, Website, etc.
- 💼 **Business**: Territory, Communication, Organization Size
- 💰 **Financial**: GST, PAN, CIN, Credit Limit, Payment Terms
- 🎓 **Advanced**: Academic Affiliation, Grants, Regulatory

---

## 🎯 Key Features

### For Admins
- ✨ **Toggle fields** on/off with a switch
- 🔄 **Reorder fields** with up/down arrows
- 👁️ **Live preview** of form layout
- 💾 **Save configurations** per company
- 🔒 **Mandatory fields** cannot be disabled

### For Users
- 📝 **Simpler forms** with only relevant fields
- ✅ **Clear validation** for required fields
- 🔗 **Smart dropdowns** (sub-industry depends on industry)
- 🚀 **Faster data entry**

---

## 🐛 Troubleshooting

### Issue: "Page not found" for /admin/account-fields
**Solution:** Restart your Next.js dev server:
```bash
npm run dev
```

### Issue: No fields showing in configuration page
**Solution:** Check that the seed script ran successfully:
```sql
SELECT * FROM account_field_configurations LIMIT 5;
```

### Issue: Sub-industry dropdown not working
**Solution:** Verify industry mappings exist:
```sql
SELECT * FROM industry_subindustry_mapping WHERE industry = 'Research';
```

---

## 📖 Full Documentation

For detailed information, see:
- **ACCOUNT_FIELDS_IMPLEMENTATION.md** - Complete technical guide
- **supabase/** folder - All SQL scripts

---

## 🎉 You're Done!

The system is now ready. Admins can customize form fields, and users will see a cleaner, more relevant account creation form.

**Questions?** Check the troubleshooting section or review the full implementation guide.
