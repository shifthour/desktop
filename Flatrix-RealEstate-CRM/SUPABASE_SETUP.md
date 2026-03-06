# Supabase Database Setup for Flatrix Real Estate CRM

## Overview
This document contains all the SQL scripts needed to set up the Flatrix Real Estate CRM database in Supabase.

## Important Notes
- All tables are prefixed with `flatrix_`
- The `flatrix_leads` table already exists and should not be recreated
- Run these scripts in order

## Database Connection Details
- **URL**: `https://eflvzsfgoelonfclzrjy.supabase.co`
- **Anon Key**: `eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVmbHZ6c2Znb2Vsb25mY2x6cmp5Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDc2NTYxOTAsImV4cCI6MjA2MzIzMjE5MH0.LCxuFDJuBO8ggiqdWpv5uoOkplKWbcF8z5E_kdpOEWU`

## Setup Instructions

### Step 1: Run the SQL Scripts in Order

1. Navigate to your Supabase Dashboard SQL Editor
2. Run each migration file in order:
   - `001_create_tables.sql` - Creates all main tables
   - `002_add_foreign_keys_to_existing_leads.sql` - Adds foreign keys to existing leads table
   - `003_enable_rls_policies.sql` - Sets up Row Level Security
   - `004_seed_initial_data.sql` - Adds initial demo data

### Step 2: Verify Tables Created

After running the scripts, verify the following tables exist:
- `flatrix_users`
- `flatrix_channel_partners`
- `flatrix_projects`
- `flatrix_project_partners`
- `flatrix_properties`
- `flatrix_leads` (already exists)
- `flatrix_deals`
- `flatrix_commissions`
- `flatrix_activities`
- `flatrix_tasks`

### Step 3: Test User Credentials

The seed data creates the following test users:
- **Admin**: admin@flatrix.com / admin123
- **Manager**: manager@flatrix.com / manager123
- **Agent 1**: agent1@flatrix.com / agent123
- **Agent 2**: agent2@flatrix.com / agent123

## Table Descriptions

### flatrix_users
Stores user accounts with roles (ADMIN, CHANNEL_PARTNER, SALES_MANAGER, AGENT)

### flatrix_channel_partners
Channel partner companies with commission rates and earnings tracking

### flatrix_projects
Real estate projects with unit availability and pricing

### flatrix_properties
Individual properties/units within projects

### flatrix_leads
Customer leads with status tracking (NOTE: This table already exists)

### flatrix_deals
Sales deals linking leads to properties

### flatrix_commissions
Commission tracking for channel partners

### flatrix_activities
Activity log for lead interactions

### flatrix_tasks
Task management for follow-ups

## Row Level Security (RLS)

The system implements RLS policies for data security:
- **Admins** can access all data
- **Sales Managers** have broad access
- **Agents** can only see their assigned leads and deals
- **Channel Partners** can only see their own data

## Troubleshooting

### If tables already exist
If you get errors about tables already existing, you can drop them first:
```sql
DROP TABLE IF EXISTS flatrix_tasks CASCADE;
DROP TABLE IF EXISTS flatrix_activities CASCADE;
DROP TABLE IF EXISTS flatrix_commissions CASCADE;
DROP TABLE IF EXISTS flatrix_deals CASCADE;
DROP TABLE IF EXISTS flatrix_properties CASCADE;
DROP TABLE IF EXISTS flatrix_project_partners CASCADE;
DROP TABLE IF EXISTS flatrix_projects CASCADE;
DROP TABLE IF EXISTS flatrix_channel_partners CASCADE;
DROP TABLE IF EXISTS flatrix_users CASCADE;
DROP TYPE IF EXISTS user_role CASCADE;
DROP TYPE IF EXISTS lead_status CASCADE;
DROP TYPE IF EXISTS property_type CASCADE;
DROP TYPE IF EXISTS property_status CASCADE;
DROP TYPE IF EXISTS commission_status CASCADE;
DROP TYPE IF EXISTS task_priority CASCADE;
DROP TYPE IF EXISTS task_status CASCADE;
DROP TYPE IF EXISTS deal_status CASCADE;
```

### If flatrix_leads structure needs updating
Since `flatrix_leads` already exists, we only add columns if they don't exist. Check the current structure and adjust the migration script accordingly.

## Next Steps

1. Run the SQL scripts in order
2. Test the authentication with provided credentials
3. Start the application with `npm run dev`
4. Access the application at http://localhost:30001

## Support

For any issues or questions, please check:
- Supabase Dashboard logs
- Browser console for client-side errors
- Network tab for API responses