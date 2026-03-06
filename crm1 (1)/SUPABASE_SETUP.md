# 🚀 Multi-Tenant Instrumental CRM - Supabase Setup Guide

## 📋 Overview

This CRM system is designed for selling to instrumental companies with a complete multi-tenant architecture:

- **Product Owners (You)**: Create companies and admin users
- **Company Admins**: Manage their company's users (min 5 users: 1 admin + 4 users)
- **Role-Based Access**: 11 different roles tailored for instrumental businesses

## 🏗️ Database Architecture

### Companies
- Each client company gets their own space
- Configurable user limits (minimum 5)
- Subscription management

### User Roles for Instrumental CRM
1. **Super Admin** - Product owners (full system access)
2. **Company Admin** - Client company administrators
3. **Sales Manager** - Full sales operations
4. **Sales Representative** - Assigned sales activities
5. **Marketing Manager** - Campaigns and lead generation
6. **Field Service Manager** - Installation and maintenance oversight
7. **Field Service Engineer** - Assigned service tasks
8. **Customer Support** - Support tickets and communication
9. **Finance Manager** - Financial operations
10. **Inventory Manager** - Product and stock management
11. **Viewer** - Read-only access

## 🔧 Setup Instructions

### 1. Database Setup

1. Go to your Supabase dashboard: https://supabase.com/dashboard/project/eflvzsfgoelonfclzrjy

2. Navigate to **SQL Editor**

3. Copy the entire contents of `supabase/schema.sql` and paste it into the SQL editor

4. Click **Run** to execute the schema

This will create:
- ✅ Companies table
- ✅ User roles table (with predefined roles)
- ✅ Users table
- ✅ Row Level Security policies
- ✅ Automatic triggers for user counting

### 2. Environment Setup

Your `.env.local` file is already configured with:
```env
NEXT_PUBLIC_SUPABASE_URL=https://eflvzsfgoelonfclzrjy.supabase.co
NEXT_PUBLIC_SUPABASE_ANON_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
SUPABASE_SERVICE_ROLE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

### 3. Initialize Sample Data (Optional)

Run the initialization script:
```bash
node scripts/init-db.js --with-sample
```

This creates:
- Sample company "Acme Instruments"
- Sample admin user: `admin@acme-instruments.com`

## 🎯 How to Use the System

### For Product Owners (You)

1. Access the admin panel: `http://localhost:3000/admin`

2. **Create New Companies**:
   - Click "Add Company"
   - Enter company name, domain, and max users (minimum 5)
   - System creates the company space

3. **Manage Users**:
   - Select a company from the dropdown
   - Add users with appropriate roles
   - First user should be a "Company Admin"

### For Company Admins

Company admins can:
- View their company's user list
- Add new users (within their user limit)
- Assign roles to users
- Activate/deactivate users
- Cannot exceed their user limit

## 👥 User Management Workflow

### Creating a New Client Company

1. **Product Owner** creates company:
   ```
   Company: "XYZ Instruments Ltd"
   Domain: "xyz-instruments"
   Max Users: 10 (1 admin + 9 users)
   ```

2. **Product Owner** creates admin user:
   ```
   Email: admin@xyz-instruments.com
   Name: "John Smith"
   Role: "Company Admin"
   Is Admin: ✅
   ```

3. **Company Admin** logs in and creates team:
   ```
   - sales@xyz-instruments.com (Sales Manager)
   - rep1@xyz-instruments.com (Sales Rep)
   - engineer@xyz-instruments.com (Field Service Engineer)
   - support@xyz-instruments.com (Customer Support)
   - finance@xyz-instruments.com (Finance Manager)
   ```

## 🔐 Role Permissions

Each role has specific permissions for CRM modules:

### Sales Manager
- ✅ Full access to leads, accounts, opportunities
- ✅ Create/edit quotations and invoices
- ✅ View all sales reports and analytics

### Sales Representative  
- ✅ View assigned leads and opportunities
- ✅ Update assigned records
- ✅ Create quotations for assigned deals

### Field Service Engineer
- ✅ View assigned service tickets
- ✅ Update installation status
- ✅ Access equipment information

## 🚀 API Endpoints

### Companies (Super Admin only)
- `GET /api/admin/companies` - List all companies
- `POST /api/admin/companies` - Create new company

### Users (Company Admin or Super Admin)
- `GET /api/admin/users?companyId=xyz` - List company users
- `POST /api/admin/users` - Create new user
- `PUT /api/admin/users` - Update user
- `DELETE /api/admin/users?userId=xyz` - Delete user

### Roles
- `GET /api/admin/roles` - List available roles

## 🔒 Security Features

### Row Level Security (RLS)
- Users can only access their company's data
- Super admins can access all data
- Company admins can manage their company's users

### User Limits
- Companies cannot exceed their user limits
- Automatic user counting with database triggers
- User creation fails if limit reached

## 📊 Admin Dashboard Features

### Company Management
- View all client companies
- Monitor user counts vs limits
- Track subscription status
- Create new companies

### User Management
- Company-specific user lists
- Role assignment interface
- User activation/deactivation
- Bulk user operations

### Role Overview
- View all available roles
- See permission details
- Role descriptions for instrumental CRM

## 🎉 Next Steps

1. **Run the schema** in Supabase dashboard
2. **Create your first company** via admin panel
3. **Set up authentication** (optional - currently using mock data)
4. **Configure role-based routing** in the main CRM
5. **Test user permissions** across different modules

## 📞 Support

The system is now ready for:
- ✅ Multi-tenant company management
- ✅ User role management  
- ✅ Permission-based access control
- ✅ Scalable architecture for instrumental CRM

Your CRM can now be sold to multiple instrumental companies with complete data isolation and user management!