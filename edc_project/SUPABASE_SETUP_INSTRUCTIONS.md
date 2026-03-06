# EDC Clinical Trials System - Supabase Database Setup

## 🚀 Quick Setup Instructions

### Step 1: Run the SQL Script in Supabase

1. **Go to your Supabase Dashboard**: https://app.supabase.com/project/eflvzsfgoelonfclzrjy

2. **Navigate to SQL Editor**:
   - Click on "SQL Editor" in the left sidebar
   - Click "New query"

3. **Copy and Paste the SQL Script**:
   - Open the file: `supabase_schema.sql`
   - Copy the entire contents
   - Paste into the SQL Editor

4. **Execute the Script**:
   - Click "Run" button (or press Ctrl/Cmd + Enter)
   - Wait for execution to complete (should take 10-30 seconds)

### Step 2: Verify Tables Created

After running the script, you should see these tables created (all prefixed with `EDC_`):

**Core Tables:**
- `EDC_users` - User accounts and authentication
- `EDC_studies` - Clinical studies/protocols
- `EDC_sites` - Study sites and locations
- `EDC_subjects` - Study participants
- `EDC_form_templates` - Form designs
- `EDC_form_sections` - Form sections
- `EDC_form_fields` - Individual form fields
- `EDC_study_forms` - Forms assigned to studies
- `EDC_form_data` - Subject response data
- `EDC_subject_visits` - Visit schedules and tracking

**Management Tables:**
- `EDC_queries` - Data clarification queries
- `EDC_audit_trail` - Complete audit logging
- `EDC_study_metrics` - Dashboard metrics
- `EDC_data_verification` - Quality checks
- `EDC_closure_activities` - Study closure tasks

### Step 3: Verify Sample Data

The script includes sample data for all tables:

**Users (3 phase-based accounts):**
- `startup_admin` / `Startup@2024` - START_UP phase
- `conduct_crc` / `Conduct@2024` - CONDUCT phase  
- `closeout_manager` / `Closeout@2024` - CLOSE_OUT phase

**Studies:**
- Phase III Hypertension Study (PROTO-2024-001)
- Phase II Diabetes Study (PROTO-2024-002)

**Sites:**
- 5 clinical sites across different studies
- Various enrollment numbers and completion rates

**Forms:**
- Demographics Form
- Adverse Events Form
- Vital Signs Form
- Laboratory Values
- Medical History

### Step 4: Test the Connection

1. **Backend Server**: Your backend is now connected to Supabase
2. **Test Login**: Use any of the 3 user accounts
3. **Verify Data**: All dashboard data comes from database tables

## 📊 Database Schema Overview

### Authentication & Users
```sql
EDC_users - User accounts with phase-based roles
├── phase: START_UP, CONDUCT, CLOSE_OUT  
├── permissions: JSON object with role permissions
└── status: ACTIVE, INACTIVE, SUSPENDED
```

### Study Management
```sql
EDC_studies - Clinical trial protocols
├── protocol_number: Unique study identifier
├── current_enrollment / projected_enrollment
├── status: DRAFT, ACTIVE, COMPLETED, CANCELLED
└── Related: EDC_sites, EDC_subjects
```

### Form System
```sql
EDC_form_templates - Reusable form designs
├── EDC_form_sections - Form sections  
│   └── EDC_form_fields - Individual fields
├── EDC_study_forms - Forms assigned to studies
└── EDC_form_data - Subject responses (JSON)
```

### Data Quality & Compliance
```sql
EDC_queries - Data clarification system
EDC_audit_trail - 21 CFR Part 11 compliance
EDC_data_verification - Quality assurance checks
EDC_closure_activities - Study closure workflow
```

## 🔐 Security Features

**Row Level Security (RLS):**
- Enabled on sensitive tables
- User-based data access control

**Audit Compliance:**
- Complete audit trail for all changes
- User, timestamp, IP tracking
- Old/new value logging

**Data Integrity:**
- Foreign key constraints
- Check constraints for enums
- Automatic timestamps with triggers

## 📈 Dashboard Data Sources

### START_UP Phase Dashboard
- **Forms**: From `EDC_form_templates`
- **Sites**: From `EDC_sites` 
- **Metrics**: From `EDC_study_metrics`
- **Verification**: From `EDC_data_verification`

### CONDUCT Phase Dashboard  
- **Subjects**: From `EDC_subjects`
- **Queries**: From `EDC_queries`
- **Enrollment**: Live calculation from `EDC_subjects`
- **Sites**: From `EDC_sites` with completion rates

### CLOSE_OUT Phase Dashboard
- **Activities**: From `EDC_closure_activities`
- **Data Quality**: From `EDC_data_verification`
- **Final Metrics**: Calculated from all tables
- **Export Status**: From `EDC_closure_activities`

## ⚙️ Configuration Details

**Supabase Project:**
- URL: https://eflvzsfgoelonfclzrjy.supabase.co
- All tables prefixed with `EDC_`
- Real-time subscriptions available
- Automatic backups enabled

**Backend Connection:**
- Uses `@supabase/supabase-js` client
- JWT authentication with 24h expiry
- Error handling and fallback data
- Performance optimized queries

## 🔄 Data Flow

1. **Login** → Query `EDC_users` → Generate JWT
2. **Dashboard** → Query phase-specific tables → Format response  
3. **Form Builder** → CRUD operations on `EDC_form_*` tables
4. **All Actions** → Logged to `EDC_audit_trail`

## ✅ Verification Checklist

After setup, verify:

- [ ] All 14 EDC_ tables created successfully
- [ ] Sample data inserted (3 users, 2 studies, 5 sites, etc.)
- [ ] Login works with all 3 user accounts
- [ ] Dashboard shows real data from database
- [ ] Form Builder connects to database
- [ ] No console errors in backend/frontend

## 🎯 Ready for Production

The database is now fully configured with:
- ✅ All EDC_ prefixed tables
- ✅ Complete clinical trial data model
- ✅ Sample data for immediate testing
- ✅ 21 CFR Part 11 compliance features
- ✅ Phase-based user authentication
- ✅ Real-time dashboard integration

Your EDC Clinical Trials System is ready for client demonstration! 🚀