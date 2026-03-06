# Technical Specification Document
# DataTrial EDC/IWRS System

---

**Document Version:** 1.0
**Date:** January 27, 2026
**Backend Database:** Supabase (PostgreSQL)
**Classification:** Technical

---

## Table of Contents

1. [System Architecture](#1-system-architecture)
2. [Technology Stack](#2-technology-stack)
3. [Database Design](#3-database-design)
4. [API Specifications](#4-api-specifications)
5. [Authentication & Authorization](#5-authentication--authorization)
6. [Security Implementation](#6-security-implementation)
7. [Audit Trail Implementation](#7-audit-trail-implementation)
8. [File Storage](#8-file-storage)
9. [Real-time Features](#9-real-time-features)
10. [Integration Points](#10-integration-points)
11. [Deployment Architecture](#11-deployment-architecture)
12. [Performance Specifications](#12-performance-specifications)

---

## 1. System Architecture

### 1.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLIENT LAYER                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │   Web App   │  │  Mobile App │  │   Tablet    │              │
│  │  (React/Vue)│  │  (PWA/RN)   │  │    App      │              │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘              │
└─────────┼────────────────┼────────────────┼─────────────────────┘
          │                │                │
          ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────┐
│                      API GATEWAY / CDN                           │
│              (Cloudflare / AWS CloudFront)                       │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     APPLICATION LAYER                            │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              Supabase Edge Functions                      │   │
│  │         (Business Logic / API Endpoints)                  │   │
│  └──────────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              Supabase Realtime                            │   │
│  │         (WebSocket Subscriptions)                         │   │
│  └──────────────────────────────────────────────────────────┘   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                       DATA LAYER                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌────────────────┐   │
│  │   Supabase DB   │  │ Supabase Storage│  │  Supabase Auth │   │
│  │  (PostgreSQL)   │  │ (File Storage)  │  │  (JWT/OAuth)   │   │
│  └─────────────────┘  └─────────────────┘  └────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### 1.2 Component Overview

| Component | Technology | Purpose |
|-----------|------------|---------|
| Frontend | React/Next.js or Vue/Nuxt | User interface |
| API Layer | Supabase Edge Functions | Business logic |
| Database | Supabase (PostgreSQL) | Data persistence |
| Auth | Supabase Auth | Authentication/Authorization |
| Storage | Supabase Storage | Document/file storage |
| Realtime | Supabase Realtime | Live updates |
| Email | Supabase + Resend/SendGrid | Notifications |

---

## 2. Technology Stack

### 2.1 Frontend Stack

```yaml
Framework: Next.js 14+ / Nuxt 3+
Language: TypeScript
UI Library: Shadcn/ui or Vuetify
State Management: Zustand / Pinia
Form Handling: React Hook Form / VeeValidate
Data Fetching: TanStack Query
PDF Generation: React-PDF / pdfmake
Excel Export: SheetJS (xlsx)
Charts: Recharts / Chart.js
```

### 2.2 Backend Stack (Supabase)

```yaml
Database: PostgreSQL 15+
API: PostgREST (auto-generated REST API)
Auth: Supabase Auth (GoTrue)
Realtime: Supabase Realtime (Phoenix Channels)
Storage: Supabase Storage (S3-compatible)
Edge Functions: Deno runtime
```

### 2.3 Infrastructure

```yaml
Hosting: Supabase Cloud / Self-hosted
CDN: Cloudflare / Vercel Edge
CI/CD: GitHub Actions
Monitoring: Supabase Dashboard + Custom Logging
Backup: Supabase PITR (Point-in-Time Recovery)
```

---

## 3. Database Design

### 3.1 Schema Overview

```sql
-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Create schemas for organization
CREATE SCHEMA IF NOT EXISTS edc;      -- EDC module tables
CREATE SCHEMA IF NOT EXISTS iwrs;     -- IWRS module tables
CREATE SCHEMA IF NOT EXISTS audit;    -- Audit trail tables
CREATE SCHEMA IF NOT EXISTS config;   -- Configuration tables
```

### 3.2 Core Tables

#### 3.2.1 Organization & Study Structure

```sql
-- Studies Table
CREATE TABLE edc.studies (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    study_code VARCHAR(50) UNIQUE NOT NULL,
    study_name VARCHAR(255) NOT NULL,
    protocol_number VARCHAR(100),
    sponsor_name VARCHAR(255),
    phase VARCHAR(20), -- Phase I, II, III, IV
    status VARCHAR(20) DEFAULT 'draft', -- draft, active, completed, closed
    therapeutic_area VARCHAR(100),
    indication VARCHAR(255),
    start_date DATE,
    end_date DATE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_by UUID REFERENCES auth.users(id),
    updated_by UUID REFERENCES auth.users(id)
);

-- Sites Table
CREATE TABLE edc.sites (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    study_id UUID REFERENCES edc.studies(id) ON DELETE CASCADE,
    site_code VARCHAR(50) NOT NULL,
    site_name VARCHAR(255) NOT NULL,
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    pi_name VARCHAR(255), -- Principal Investigator
    pi_email VARCHAR(255),
    status VARCHAR(20) DEFAULT 'inactive', -- inactive, active, closed
    screening_cap INTEGER DEFAULT 0,
    randomization_cap INTEGER DEFAULT 0,
    screening_allocation INTEGER DEFAULT 0,
    randomization_allocation INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(study_id, site_code)
);

-- Study Arms (Treatment Groups)
CREATE TABLE iwrs.study_arms (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    study_id UUID REFERENCES edc.studies(id) ON DELETE CASCADE,
    arm_code VARCHAR(50) NOT NULL,
    arm_name VARCHAR(255) NOT NULL,
    arm_type VARCHAR(50), -- test, control, placebo
    ratio INTEGER DEFAULT 1, -- for randomization ratio
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(study_id, arm_code)
);
```

#### 3.2.2 User & Role Management

```sql
-- Custom user profiles (extends Supabase auth.users)
CREATE TABLE edc.user_profiles (
    id UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    phone VARCHAR(20),
    organization VARCHAR(255),
    is_active BOOLEAN DEFAULT true,
    last_login TIMESTAMPTZ,
    failed_login_attempts INTEGER DEFAULT 0,
    locked_until TIMESTAMPTZ,
    password_changed_at TIMESTAMPTZ DEFAULT NOW(),
    must_change_password BOOLEAN DEFAULT false,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Roles Definition
CREATE TABLE edc.roles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    role_code VARCHAR(50) UNIQUE NOT NULL,
    role_name VARCHAR(100) NOT NULL,
    description TEXT,
    is_blinded BOOLEAN DEFAULT true,
    is_system_role BOOLEAN DEFAULT false,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Insert default roles
INSERT INTO edc.roles (role_code, role_name, is_blinded, is_system_role) VALUES
('ADMIN', 'System Administrator', true, true),
('PM', 'Project Manager', true, false),
('CRA', 'Clinical Research Associate', true, false),
('DM', 'Data Manager', true, false),
('CRC', 'Clinical Research Coordinator', true, false),
('PI', 'Principal Investigator', true, false),
('SPONSOR', 'Sponsor', true, false),
('DEPOT', 'Depot Manager', false, false),
('MM', 'Medical Monitor', true, false),
('LAB', 'Lab User', true, false),
('QA', 'Quality Assurance', true, false);

-- User Study Assignments
CREATE TABLE edc.user_study_assignments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES auth.users(id) ON DELETE CASCADE,
    study_id UUID REFERENCES edc.studies(id) ON DELETE CASCADE,
    role_id UUID REFERENCES edc.roles(id),
    site_id UUID REFERENCES edc.sites(id), -- NULL for all sites access
    is_active BOOLEAN DEFAULT true,
    assigned_at TIMESTAMPTZ DEFAULT NOW(),
    assigned_by UUID REFERENCES auth.users(id),
    UNIQUE(user_id, study_id, role_id, site_id)
);

-- Permissions
CREATE TABLE edc.permissions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    permission_code VARCHAR(100) UNIQUE NOT NULL,
    permission_name VARCHAR(255) NOT NULL,
    module VARCHAR(50), -- edc, iwrs, reports, admin
    description TEXT
);

-- Role Permissions Mapping
CREATE TABLE edc.role_permissions (
    role_id UUID REFERENCES edc.roles(id) ON DELETE CASCADE,
    permission_id UUID REFERENCES edc.permissions(id) ON DELETE CASCADE,
    PRIMARY KEY (role_id, permission_id)
);
```

#### 3.2.3 CRF Builder Tables

```sql
-- Visits Definition
CREATE TABLE edc.visits (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    study_id UUID REFERENCES edc.studies(id) ON DELETE CASCADE,
    visit_code VARCHAR(50) NOT NULL,
    visit_name VARCHAR(255) NOT NULL,
    visit_type VARCHAR(50), -- screening, randomization, treatment, followup, unscheduled, common
    sequence_number INTEGER NOT NULL,
    duration_days INTEGER DEFAULT 0,
    window_before INTEGER DEFAULT 0,
    window_after INTEGER DEFAULT 0,
    is_schedulable BOOLEAN DEFAULT true,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(study_id, visit_code)
);

-- CRF Forms (Panels)
CREATE TABLE edc.crf_forms (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    study_id UUID REFERENCES edc.studies(id) ON DELETE CASCADE,
    form_code VARCHAR(50) NOT NULL,
    form_name VARCHAR(255) NOT NULL,
    form_type VARCHAR(50), -- demographics, vitals, lab, ae, cm, etc.
    is_repeating BOOLEAN DEFAULT false,
    is_log_form BOOLEAN DEFAULT false,
    sequence_number INTEGER,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(study_id, form_code)
);

-- Visit-Form Mapping
CREATE TABLE edc.visit_forms (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    visit_id UUID REFERENCES edc.visits(id) ON DELETE CASCADE,
    form_id UUID REFERENCES edc.crf_forms(id) ON DELETE CASCADE,
    is_required BOOLEAN DEFAULT false,
    sequence_number INTEGER,
    UNIQUE(visit_id, form_id)
);

-- CRF Fields/Items
CREATE TABLE edc.crf_fields (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    form_id UUID REFERENCES edc.crf_forms(id) ON DELETE CASCADE,
    field_code VARCHAR(100) NOT NULL,
    field_label VARCHAR(500) NOT NULL,
    field_type VARCHAR(50) NOT NULL, -- text, number, date, datetime, select, radio, checkbox, textarea
    data_type VARCHAR(50), -- string, integer, decimal, date, datetime, boolean
    is_required BOOLEAN DEFAULT false,
    is_readonly BOOLEAN DEFAULT false,
    is_hidden BOOLEAN DEFAULT false,
    default_value TEXT,
    placeholder TEXT,
    help_text TEXT,
    min_value DECIMAL,
    max_value DECIMAL,
    min_length INTEGER,
    max_length INTEGER,
    regex_pattern VARCHAR(500),
    decimal_places INTEGER,
    unit VARCHAR(50),
    sequence_number INTEGER NOT NULL,
    row_number INTEGER DEFAULT 1,
    column_number INTEGER DEFAULT 1,
    colspan INTEGER DEFAULT 1,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(form_id, field_code)
);

-- List of Values (LOV)
CREATE TABLE edc.lov_lists (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    study_id UUID REFERENCES edc.studies(id) ON DELETE CASCADE,
    list_code VARCHAR(100) NOT NULL,
    list_name VARCHAR(255) NOT NULL,
    is_active BOOLEAN DEFAULT true,
    UNIQUE(study_id, list_code)
);

CREATE TABLE edc.lov_items (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    list_id UUID REFERENCES edc.lov_lists(id) ON DELETE CASCADE,
    item_code VARCHAR(100) NOT NULL,
    item_value VARCHAR(500) NOT NULL,
    sequence_number INTEGER,
    is_active BOOLEAN DEFAULT true,
    UNIQUE(list_id, item_code)
);

-- Field LOV Mapping
CREATE TABLE edc.field_lov (
    field_id UUID REFERENCES edc.crf_fields(id) ON DELETE CASCADE,
    list_id UUID REFERENCES edc.lov_lists(id) ON DELETE CASCADE,
    PRIMARY KEY (field_id, list_id)
);

-- Edit Checks / Validations
CREATE TABLE edc.edit_checks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    study_id UUID REFERENCES edc.studies(id) ON DELETE CASCADE,
    check_code VARCHAR(100) NOT NULL,
    check_name VARCHAR(255) NOT NULL,
    check_type VARCHAR(50), -- range, cross_field, cross_form, cross_visit, custom
    source_field_id UUID REFERENCES edc.crf_fields(id),
    target_field_id UUID REFERENCES edc.crf_fields(id),
    condition_expression TEXT, -- SQL or custom expression
    error_message TEXT NOT NULL,
    severity VARCHAR(20) DEFAULT 'error', -- error, warning
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(study_id, check_code)
);
```

#### 3.2.4 Subject & Data Tables

```sql
-- Subjects
CREATE TABLE edc.subjects (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    study_id UUID REFERENCES edc.studies(id) ON DELETE CASCADE,
    site_id UUID REFERENCES edc.sites(id) ON DELETE CASCADE,
    screening_number VARCHAR(50) NOT NULL,
    randomization_number VARCHAR(50),
    subject_initials VARCHAR(10),
    status VARCHAR(50) DEFAULT 'screened', -- screened, screen_failed, randomized, completed, discontinued, withdrawn
    screening_date DATE,
    randomization_date DATE,
    discontinuation_date DATE,
    discontinuation_reason TEXT,
    arm_id UUID REFERENCES iwrs.study_arms(id), -- assigned after randomization
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_by UUID REFERENCES auth.users(id),
    UNIQUE(study_id, screening_number)
);

-- Subject Visits (Instances)
CREATE TABLE edc.subject_visits (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    subject_id UUID REFERENCES edc.subjects(id) ON DELETE CASCADE,
    visit_id UUID REFERENCES edc.visits(id),
    visit_date DATE,
    scheduled_date DATE,
    status VARCHAR(50) DEFAULT 'not_started', -- not_started, in_progress, complete, missed
    is_unscheduled BOOLEAN DEFAULT false,
    unscheduled_number INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- CRF Data (Main data storage)
CREATE TABLE edc.crf_data (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    subject_visit_id UUID REFERENCES edc.subject_visits(id) ON DELETE CASCADE,
    form_id UUID REFERENCES edc.crf_forms(id),
    repeat_number INTEGER DEFAULT 1, -- for repeating forms
    status VARCHAR(50) DEFAULT 'incomplete', -- incomplete, complete, submitted, sdv, dm_reviewed, qa_reviewed, pi_reviewed, locked
    submitted_at TIMESTAMPTZ,
    submitted_by UUID REFERENCES auth.users(id),
    sdv_at TIMESTAMPTZ,
    sdv_by UUID REFERENCES auth.users(id),
    dm_reviewed_at TIMESTAMPTZ,
    dm_reviewed_by UUID REFERENCES auth.users(id),
    qa_reviewed_at TIMESTAMPTZ,
    qa_reviewed_by UUID REFERENCES auth.users(id),
    pi_reviewed_at TIMESTAMPTZ,
    pi_reviewed_by UUID REFERENCES auth.users(id),
    locked_at TIMESTAMPTZ,
    locked_by UUID REFERENCES auth.users(id),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- CRF Field Values (Actual data)
CREATE TABLE edc.crf_field_values (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    crf_data_id UUID REFERENCES edc.crf_data(id) ON DELETE CASCADE,
    field_id UUID REFERENCES edc.crf_fields(id),
    field_value TEXT,
    previous_value TEXT,
    change_reason TEXT,
    is_system_populated BOOLEAN DEFAULT false,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    updated_by UUID REFERENCES auth.users(id),
    UNIQUE(crf_data_id, field_id)
);
```

#### 3.2.5 Query Management Tables

```sql
-- Queries
CREATE TABLE edc.queries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    query_number VARCHAR(50) UNIQUE NOT NULL,
    subject_id UUID REFERENCES edc.subjects(id),
    crf_data_id UUID REFERENCES edc.crf_data(id),
    field_id UUID REFERENCES edc.crf_fields(id),
    query_type VARCHAR(50), -- auto, manual, system
    query_category VARCHAR(50), -- same_panel_single, same_panel_multiple, cross_panel, cross_visit
    status VARCHAR(50) DEFAULT 'open', -- open, responded, closed, reopened
    query_text TEXT NOT NULL,
    response_text TEXT,
    resolution_text TEXT,
    raised_by UUID REFERENCES auth.users(id),
    raised_by_role VARCHAR(50),
    raised_at TIMESTAMPTZ DEFAULT NOW(),
    responded_by UUID REFERENCES auth.users(id),
    responded_at TIMESTAMPTZ,
    closed_by UUID REFERENCES auth.users(id),
    closed_at TIMESTAMPTZ,
    reopened_by UUID REFERENCES auth.users(id),
    reopened_at TIMESTAMPTZ,
    reopen_count INTEGER DEFAULT 0
);

-- Query History
CREATE TABLE edc.query_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    query_id UUID REFERENCES edc.queries(id) ON DELETE CASCADE,
    action VARCHAR(50), -- created, responded, closed, reopened
    action_by UUID REFERENCES auth.users(id),
    action_at TIMESTAMPTZ DEFAULT NOW(),
    old_status VARCHAR(50),
    new_status VARCHAR(50),
    comment TEXT
);
```

#### 3.2.6 IWRS / IP Management Tables

```sql
-- Kit Definitions
CREATE TABLE iwrs.kit_definitions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    study_id UUID REFERENCES edc.studies(id) ON DELETE CASCADE,
    kit_type_code VARCHAR(50) NOT NULL,
    kit_type_name VARCHAR(255) NOT NULL,
    arm_id UUID REFERENCES iwrs.study_arms(id),
    tablets_per_kit INTEGER,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(study_id, kit_type_code)
);

-- IP Inventory (Drug Stock)
CREATE TABLE iwrs.ip_inventory (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    study_id UUID REFERENCES edc.studies(id) ON DELETE CASCADE,
    kit_definition_id UUID REFERENCES iwrs.kit_definitions(id),
    kit_number VARCHAR(100) UNIQUE NOT NULL,
    batch_number VARCHAR(100),
    manufacture_date DATE,
    expiry_date DATE,
    location_type VARCHAR(50), -- depot, site, subject
    depot_id UUID,
    site_id UUID REFERENCES edc.sites(id),
    subject_id UUID REFERENCES edc.subjects(id),
    status VARCHAR(50) DEFAULT 'available', -- available, reserved, dispensed, damaged, lost, quarantined, expired, destroyed, returned
    received_at TIMESTAMPTZ,
    dispensed_at TIMESTAMPTZ,
    dispensed_by UUID REFERENCES auth.users(id),
    returned_at TIMESTAMPTZ,
    tablets_returned INTEGER,
    tablets_dispensed INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Shipments
CREATE TABLE iwrs.shipments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    shipment_number VARCHAR(100) UNIQUE NOT NULL,
    study_id UUID REFERENCES edc.studies(id),
    shipment_type VARCHAR(50), -- depot_to_site, site_to_depot
    from_depot_id UUID,
    from_site_id UUID REFERENCES edc.sites(id),
    to_depot_id UUID,
    to_site_id UUID REFERENCES edc.sites(id),
    status VARCHAR(50) DEFAULT 'created', -- created, dispatched, in_transit, delivered, received
    shipped_date DATE,
    expected_delivery_date DATE,
    actual_delivery_date DATE,
    tracking_number VARCHAR(255),
    carrier VARCHAR(255),
    temperature_excursion BOOLEAN DEFAULT false,
    comments TEXT,
    created_by UUID REFERENCES auth.users(id),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    dispatched_by UUID REFERENCES auth.users(id),
    dispatched_at TIMESTAMPTZ,
    received_by UUID REFERENCES auth.users(id),
    received_at TIMESTAMPTZ
);

-- Shipment Items
CREATE TABLE iwrs.shipment_items (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    shipment_id UUID REFERENCES iwrs.shipments(id) ON DELETE CASCADE,
    kit_id UUID REFERENCES iwrs.ip_inventory(id),
    status VARCHAR(50), -- included, received, damaged, quarantined
    received_condition VARCHAR(50),
    comments TEXT
);

-- Kit Requests
CREATE TABLE iwrs.kit_requests (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    request_number VARCHAR(100) UNIQUE NOT NULL,
    study_id UUID REFERENCES edc.studies(id),
    site_id UUID REFERENCES edc.sites(id),
    requested_by UUID REFERENCES auth.users(id),
    requested_at TIMESTAMPTZ DEFAULT NOW(),
    quantity_requested INTEGER NOT NULL,
    status VARCHAR(50) DEFAULT 'pending', -- pending, approved, declined, fulfilled
    approved_by UUID REFERENCES auth.users(id),
    approved_at TIMESTAMPTZ,
    decline_reason TEXT,
    comments TEXT
);

-- Randomization List
CREATE TABLE iwrs.randomization_list (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    study_id UUID REFERENCES edc.studies(id) ON DELETE CASCADE,
    randomization_number VARCHAR(50) NOT NULL,
    arm_id UUID REFERENCES iwrs.study_arms(id),
    site_id UUID REFERENCES edc.sites(id), -- NULL for central randomization
    is_used BOOLEAN DEFAULT false,
    used_at TIMESTAMPTZ,
    subject_id UUID REFERENCES edc.subjects(id),
    sequence_number INTEGER,
    UNIQUE(study_id, randomization_number)
);
```

#### 3.2.7 Audit Trail Table

```sql
-- Comprehensive Audit Trail
CREATE TABLE audit.audit_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name VARCHAR(100) NOT NULL,
    record_id UUID NOT NULL,
    action VARCHAR(20) NOT NULL, -- INSERT, UPDATE, DELETE
    old_values JSONB,
    new_values JSONB,
    changed_fields TEXT[],
    change_reason TEXT,
    user_id UUID REFERENCES auth.users(id),
    user_email VARCHAR(255),
    user_role VARCHAR(50),
    ip_address INET,
    user_agent TEXT,
    session_id VARCHAR(255),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create index for faster queries
CREATE INDEX idx_audit_log_table_record ON audit.audit_log(table_name, record_id);
CREATE INDEX idx_audit_log_user ON audit.audit_log(user_id);
CREATE INDEX idx_audit_log_created_at ON audit.audit_log(created_at);
```

### 3.3 Database Functions & Triggers

#### 3.3.1 Audit Trail Trigger

```sql
-- Function to capture audit trail
CREATE OR REPLACE FUNCTION audit.log_changes()
RETURNS TRIGGER AS $$
DECLARE
    old_data JSONB;
    new_data JSONB;
    changed_cols TEXT[];
BEGIN
    IF TG_OP = 'INSERT' THEN
        new_data := to_jsonb(NEW);
        INSERT INTO audit.audit_log (table_name, record_id, action, new_values, user_id, created_at)
        VALUES (TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, NEW.id, 'INSERT', new_data, auth.uid(), NOW());
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        old_data := to_jsonb(OLD);
        new_data := to_jsonb(NEW);
        -- Get changed columns
        SELECT array_agg(key) INTO changed_cols
        FROM jsonb_each(old_data) o
        FULL OUTER JOIN jsonb_each(new_data) n USING (key)
        WHERE o.value IS DISTINCT FROM n.value;

        INSERT INTO audit.audit_log (table_name, record_id, action, old_values, new_values, changed_fields, user_id, created_at)
        VALUES (TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, NEW.id, 'UPDATE', old_data, new_data, changed_cols, auth.uid(), NOW());
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        old_data := to_jsonb(OLD);
        INSERT INTO audit.audit_log (table_name, record_id, action, old_values, user_id, created_at)
        VALUES (TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, OLD.id, 'DELETE', old_data, auth.uid(), NOW());
        RETURN OLD;
    END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Apply trigger to all relevant tables
CREATE TRIGGER audit_subjects AFTER INSERT OR UPDATE OR DELETE ON edc.subjects
FOR EACH ROW EXECUTE FUNCTION audit.log_changes();

CREATE TRIGGER audit_crf_field_values AFTER INSERT OR UPDATE OR DELETE ON edc.crf_field_values
FOR EACH ROW EXECUTE FUNCTION audit.log_changes();

CREATE TRIGGER audit_queries AFTER INSERT OR UPDATE OR DELETE ON edc.queries
FOR EACH ROW EXECUTE FUNCTION audit.log_changes();

CREATE TRIGGER audit_ip_inventory AFTER INSERT OR UPDATE OR DELETE ON iwrs.ip_inventory
FOR EACH ROW EXECUTE FUNCTION audit.log_changes();
```

#### 3.3.2 Auto-Generate Numbers

```sql
-- Generate screening number
CREATE OR REPLACE FUNCTION edc.generate_screening_number(p_study_id UUID, p_site_id UUID)
RETURNS VARCHAR AS $$
DECLARE
    v_study_code VARCHAR;
    v_site_code VARCHAR;
    v_sequence INTEGER;
    v_screening_number VARCHAR;
BEGIN
    SELECT study_code INTO v_study_code FROM edc.studies WHERE id = p_study_id;
    SELECT site_code INTO v_site_code FROM edc.sites WHERE id = p_site_id;

    SELECT COALESCE(MAX(CAST(SUBSTRING(screening_number FROM '[0-9]+$') AS INTEGER)), 0) + 1
    INTO v_sequence
    FROM edc.subjects
    WHERE study_id = p_study_id AND site_id = p_site_id;

    v_screening_number := v_study_code || '-' || v_site_code || '-' || LPAD(v_sequence::TEXT, 4, '0');
    RETURN v_screening_number;
END;
$$ LANGUAGE plpgsql;

-- Generate query number
CREATE OR REPLACE FUNCTION edc.generate_query_number(p_study_id UUID)
RETURNS VARCHAR AS $$
DECLARE
    v_study_code VARCHAR;
    v_sequence INTEGER;
BEGIN
    SELECT study_code INTO v_study_code FROM edc.studies WHERE id = p_study_id;

    SELECT COALESCE(MAX(CAST(SUBSTRING(query_number FROM '[0-9]+$') AS INTEGER)), 0) + 1
    INTO v_sequence
    FROM edc.queries q
    JOIN edc.subjects s ON q.subject_id = s.id
    WHERE s.study_id = p_study_id;

    RETURN 'QRY-' || v_study_code || '-' || LPAD(v_sequence::TEXT, 6, '0');
END;
$$ LANGUAGE plpgsql;
```

### 3.4 Row Level Security (RLS)

```sql
-- Enable RLS on all tables
ALTER TABLE edc.subjects ENABLE ROW LEVEL SECURITY;
ALTER TABLE edc.crf_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE edc.crf_field_values ENABLE ROW LEVEL SECURITY;

-- Policy: Users can only see subjects from their assigned studies/sites
CREATE POLICY subjects_access ON edc.subjects
FOR ALL USING (
    EXISTS (
        SELECT 1 FROM edc.user_study_assignments usa
        WHERE usa.user_id = auth.uid()
        AND usa.study_id = edc.subjects.study_id
        AND usa.is_active = true
        AND (usa.site_id IS NULL OR usa.site_id = edc.subjects.site_id)
    )
);

-- Policy: Blinded users cannot see arm_id
CREATE POLICY hide_arm_for_blinded ON edc.subjects
FOR SELECT USING (
    EXISTS (
        SELECT 1 FROM edc.user_study_assignments usa
        JOIN edc.roles r ON usa.role_id = r.id
        WHERE usa.user_id = auth.uid()
        AND usa.study_id = edc.subjects.study_id
        AND (r.is_blinded = false OR edc.subjects.arm_id IS NULL)
    )
);
```

---

## 4. API Specifications

### 4.1 RESTful Endpoints (Auto-generated by Supabase)

Supabase automatically generates REST APIs for all tables. Access via:
```
https://<project-ref>.supabase.co/rest/v1/<table_name>
```

### 4.2 Custom Edge Functions

#### 4.2.1 Subject Enrollment

```typescript
// supabase/functions/enroll-subject/index.ts
import { serve } from "https://deno.land/std@0.168.0/http/server.ts"
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'

serve(async (req) => {
  const supabase = createClient(
    Deno.env.get('SUPABASE_URL') ?? '',
    Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? ''
  )

  const { study_id, site_id, subject_initials, consent_date } = await req.json()

  // Check site caps
  const { data: site } = await supabase
    .from('sites')
    .select('screening_cap, screening_allocation')
    .eq('id', site_id)
    .single()

  const { count } = await supabase
    .from('subjects')
    .select('*', { count: 'exact', head: true })
    .eq('site_id', site_id)

  if (count >= site.screening_allocation) {
    return new Response(
      JSON.stringify({ error: 'Site screening cap reached' }),
      { status: 400 }
    )
  }

  // Generate screening number
  const { data: screeningNum } = await supabase
    .rpc('generate_screening_number', { p_study_id: study_id, p_site_id: site_id })

  // Create subject
  const { data: subject, error } = await supabase
    .from('subjects')
    .insert({
      study_id,
      site_id,
      screening_number: screeningNum,
      subject_initials,
      screening_date: consent_date,
      status: 'screened'
    })
    .select()
    .single()

  if (error) {
    return new Response(JSON.stringify({ error: error.message }), { status: 500 })
  }

  // Create subject visits based on study schedule
  const { data: visits } = await supabase
    .from('visits')
    .select('id')
    .eq('study_id', study_id)
    .eq('is_active', true)

  const subjectVisits = visits.map(v => ({
    subject_id: subject.id,
    visit_id: v.id,
    status: 'not_started'
  }))

  await supabase.from('subject_visits').insert(subjectVisits)

  return new Response(JSON.stringify(subject), { status: 201 })
})
```

#### 4.2.2 Randomization

```typescript
// supabase/functions/randomize-subject/index.ts
import { serve } from "https://deno.land/std@0.168.0/http/server.ts"
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'

serve(async (req) => {
  const supabase = createClient(
    Deno.env.get('SUPABASE_URL') ?? '',
    Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? ''
  )

  const { subject_id } = await req.json()

  // Get subject details
  const { data: subject } = await supabase
    .from('subjects')
    .select('*, sites(*)')
    .eq('id', subject_id)
    .single()

  if (subject.status !== 'screened') {
    return new Response(
      JSON.stringify({ error: 'Subject not eligible for randomization' }),
      { status: 400 }
    )
  }

  // Get next available randomization number
  const { data: randEntry } = await supabase
    .from('randomization_list')
    .select('*')
    .eq('study_id', subject.study_id)
    .eq('is_used', false)
    .or(`site_id.is.null,site_id.eq.${subject.site_id}`)
    .order('sequence_number')
    .limit(1)
    .single()

  if (!randEntry) {
    return new Response(
      JSON.stringify({ error: 'No randomization slots available' }),
      { status: 400 }
    )
  }

  // Assign kit
  const { data: kit } = await supabase
    .from('ip_inventory')
    .select('*')
    .eq('site_id', subject.site_id)
    .eq('status', 'available')
    .eq('arm_id', randEntry.arm_id)
    .order('expiry_date')
    .limit(1)
    .single()

  // Update subject
  const { data: updatedSubject, error } = await supabase
    .from('subjects')
    .update({
      randomization_number: randEntry.randomization_number,
      randomization_date: new Date().toISOString().split('T')[0],
      arm_id: randEntry.arm_id,
      status: 'randomized'
    })
    .eq('id', subject_id)
    .select()
    .single()

  // Mark randomization as used
  await supabase
    .from('randomization_list')
    .update({ is_used: true, used_at: new Date().toISOString(), subject_id })
    .eq('id', randEntry.id)

  // Dispense kit
  if (kit) {
    await supabase
      .from('ip_inventory')
      .update({
        status: 'dispensed',
        subject_id,
        dispensed_at: new Date().toISOString()
      })
      .eq('id', kit.id)
  }

  return new Response(
    JSON.stringify({
      subject: updatedSubject,
      kit_number: kit?.kit_number,
      arm_id: randEntry.arm_id // Only return if user is unblinded
    }),
    { status: 200 }
  )
})
```

---

## 5. Authentication & Authorization

### 5.1 Supabase Auth Configuration

```typescript
// lib/supabase.ts
import { createClient } from '@supabase/supabase-js'

export const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!,
  {
    auth: {
      autoRefreshToken: true,
      persistSession: true,
      detectSessionInUrl: true
    }
  }
)
```

### 5.2 Password Policy Implementation

```sql
-- Password policy check function
CREATE OR REPLACE FUNCTION auth.check_password_policy(password TEXT)
RETURNS BOOLEAN AS $$
BEGIN
    -- Minimum 8 characters
    IF LENGTH(password) < 8 THEN
        RAISE EXCEPTION 'Password must be at least 8 characters';
    END IF;

    -- At least one uppercase
    IF password !~ '[A-Z]' THEN
        RAISE EXCEPTION 'Password must contain at least one uppercase letter';
    END IF;

    -- At least one lowercase
    IF password !~ '[a-z]' THEN
        RAISE EXCEPTION 'Password must contain at least one lowercase letter';
    END IF;

    -- At least one number
    IF password !~ '[0-9]' THEN
        RAISE EXCEPTION 'Password must contain at least one number';
    END IF;

    -- At least one special character
    IF password !~ '[!@#$%^&*(),.?":{}|<>]' THEN
        RAISE EXCEPTION 'Password must contain at least one special character';
    END IF;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;
```

### 5.3 Account Lockout

```sql
-- Function to handle failed login
CREATE OR REPLACE FUNCTION edc.handle_failed_login(p_user_id UUID)
RETURNS VOID AS $$
DECLARE
    v_attempts INTEGER;
BEGIN
    UPDATE edc.user_profiles
    SET failed_login_attempts = failed_login_attempts + 1
    WHERE id = p_user_id
    RETURNING failed_login_attempts INTO v_attempts;

    IF v_attempts >= 3 THEN
        UPDATE edc.user_profiles
        SET locked_until = NOW() + INTERVAL '30 minutes'
        WHERE id = p_user_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to check if account is locked
CREATE OR REPLACE FUNCTION edc.is_account_locked(p_user_id UUID)
RETURNS BOOLEAN AS $$
DECLARE
    v_locked_until TIMESTAMPTZ;
BEGIN
    SELECT locked_until INTO v_locked_until
    FROM edc.user_profiles
    WHERE id = p_user_id;

    RETURN v_locked_until IS NOT NULL AND v_locked_until > NOW();
END;
$$ LANGUAGE plpgsql;
```

---

## 6. Security Implementation

### 6.1 Data Encryption

```sql
-- Encrypt sensitive fields using pgcrypto
CREATE OR REPLACE FUNCTION edc.encrypt_pii(data TEXT)
RETURNS TEXT AS $$
BEGIN
    RETURN encode(
        encrypt(
            data::bytea,
            current_setting('app.encryption_key')::bytea,
            'aes'
        ),
        'base64'
    );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION edc.decrypt_pii(encrypted_data TEXT)
RETURNS TEXT AS $$
BEGIN
    RETURN convert_from(
        decrypt(
            decode(encrypted_data, 'base64'),
            current_setting('app.encryption_key')::bytea,
            'aes'
        ),
        'UTF8'
    );
END;
$$ LANGUAGE plpgsql;
```

### 6.2 Session Management

```typescript
// middleware/session.ts
import { createMiddlewareClient } from '@supabase/auth-helpers-nextjs'
import { NextResponse } from 'next/server'
import type { NextRequest } from 'next/server'

export async function middleware(req: NextRequest) {
  const res = NextResponse.next()
  const supabase = createMiddlewareClient({ req, res })

  const { data: { session } } = await supabase.auth.getSession()

  // Check session timeout (30 minutes of inactivity)
  if (session) {
    const lastActivity = new Date(session.user.last_sign_in_at || 0)
    const now = new Date()
    const diffMinutes = (now.getTime() - lastActivity.getTime()) / 60000

    if (diffMinutes > 30) {
      await supabase.auth.signOut()
      return NextResponse.redirect(new URL('/login?timeout=true', req.url))
    }
  }

  return res
}
```

---

## 7. Audit Trail Implementation

### 7.1 Audit Trail Query

```sql
-- Get audit trail for a specific record
CREATE OR REPLACE FUNCTION audit.get_record_history(
    p_table_name VARCHAR,
    p_record_id UUID
)
RETURNS TABLE (
    action VARCHAR,
    changed_fields TEXT[],
    old_values JSONB,
    new_values JSONB,
    change_reason TEXT,
    user_email VARCHAR,
    user_role VARCHAR,
    changed_at TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        al.action,
        al.changed_fields,
        al.old_values,
        al.new_values,
        al.change_reason,
        al.user_email,
        al.user_role,
        al.created_at
    FROM audit.audit_log al
    WHERE al.table_name = p_table_name
    AND al.record_id = p_record_id
    ORDER BY al.created_at DESC;
END;
$$ LANGUAGE plpgsql;
```

### 7.2 Electronic Signature

```sql
-- Electronic Signature Table
CREATE TABLE edc.e_signatures (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    record_type VARCHAR(100) NOT NULL, -- crf_data, subject, etc.
    record_id UUID NOT NULL,
    signature_meaning VARCHAR(255) NOT NULL, -- "I confirm this data is accurate"
    signed_by UUID REFERENCES auth.users(id),
    signed_at TIMESTAMPTZ DEFAULT NOW(),
    user_email VARCHAR(255),
    user_full_name VARCHAR(255),
    ip_address INET,
    signature_hash TEXT -- SHA-256 hash of record + timestamp + user
);

-- Function to create e-signature
CREATE OR REPLACE FUNCTION edc.create_e_signature(
    p_record_type VARCHAR,
    p_record_id UUID,
    p_meaning VARCHAR,
    p_password VARCHAR
)
RETURNS UUID AS $$
DECLARE
    v_user_id UUID;
    v_email VARCHAR;
    v_name VARCHAR;
    v_sig_id UUID;
    v_hash TEXT;
BEGIN
    -- Verify password (re-authenticate)
    -- This would integrate with Supabase Auth
    v_user_id := auth.uid();

    SELECT email INTO v_email FROM auth.users WHERE id = v_user_id;
    SELECT first_name || ' ' || last_name INTO v_name
    FROM edc.user_profiles WHERE id = v_user_id;

    -- Create hash
    v_hash := encode(
        sha256(
            (p_record_type || p_record_id || v_user_id || NOW()::TEXT)::bytea
        ),
        'hex'
    );

    INSERT INTO edc.e_signatures (
        record_type, record_id, signature_meaning,
        signed_by, user_email, user_full_name, signature_hash
    ) VALUES (
        p_record_type, p_record_id, p_meaning,
        v_user_id, v_email, v_name, v_hash
    )
    RETURNING id INTO v_sig_id;

    RETURN v_sig_id;
END;
$$ LANGUAGE plpgsql;
```

---

## 8. File Storage

### 8.1 Supabase Storage Buckets

```sql
-- Create storage buckets (via Supabase Dashboard or API)
-- Buckets: study-documents, lab-reports, source-documents, exports
```

```typescript
// lib/storage.ts
import { supabase } from './supabase'

export async function uploadDocument(
  bucket: string,
  path: string,
  file: File,
  metadata?: Record<string, string>
) {
  const { data, error } = await supabase.storage
    .from(bucket)
    .upload(path, file, {
      cacheControl: '3600',
      upsert: false,
      contentType: file.type,
      metadata
    })

  if (error) throw error
  return data
}

export async function getSignedUrl(bucket: string, path: string, expiresIn = 3600) {
  const { data, error } = await supabase.storage
    .from(bucket)
    .createSignedUrl(path, expiresIn)

  if (error) throw error
  return data.signedUrl
}
```

---

## 9. Real-time Features

### 9.1 Supabase Realtime Subscriptions

```typescript
// hooks/useRealtimeQueries.ts
import { useEffect, useState } from 'react'
import { supabase } from '@/lib/supabase'

export function useRealtimeQueries(studyId: string) {
  const [queries, setQueries] = useState([])

  useEffect(() => {
    // Initial fetch
    fetchQueries()

    // Subscribe to changes
    const subscription = supabase
      .channel('queries-changes')
      .on(
        'postgres_changes',
        {
          event: '*',
          schema: 'edc',
          table: 'queries',
          filter: `study_id=eq.${studyId}`
        },
        (payload) => {
          if (payload.eventType === 'INSERT') {
            setQueries(prev => [payload.new, ...prev])
          } else if (payload.eventType === 'UPDATE') {
            setQueries(prev =>
              prev.map(q => q.id === payload.new.id ? payload.new : q)
            )
          }
        }
      )
      .subscribe()

    return () => {
      subscription.unsubscribe()
    }
  }, [studyId])

  async function fetchQueries() {
    const { data } = await supabase
      .from('queries')
      .select('*')
      .eq('study_id', studyId)
      .order('raised_at', { ascending: false })

    setQueries(data || [])
  }

  return queries
}
```

---

## 10. Integration Points

### 10.1 Email Notifications (via Supabase Edge Functions + Resend)

```typescript
// supabase/functions/send-notification/index.ts
import { serve } from "https://deno.land/std@0.168.0/http/server.ts"
import { Resend } from 'https://esm.sh/resend'

const resend = new Resend(Deno.env.get('RESEND_API_KEY'))

serve(async (req) => {
  const { to, subject, template, data } = await req.json()

  const templates = {
    query_raised: (d) => `
      <h2>New Query Raised</h2>
      <p>A new query has been raised for subject ${d.screening_number}.</p>
      <p><strong>Query:</strong> ${d.query_text}</p>
      <p>Please log in to respond.</p>
    `,
    kit_request: (d) => `
      <h2>Kit Request Pending Approval</h2>
      <p>Site ${d.site_name} has requested ${d.quantity} kits.</p>
      <p>Please log in to approve or decline.</p>
    `,
    randomization_approved: (d) => `
      <h2>Randomization Approved</h2>
      <p>Subject ${d.screening_number} has been approved for randomization.</p>
    `
  }

  const html = templates[template](data)

  const { error } = await resend.emails.send({
    from: 'DataTrial <noreply@datatrial.com>',
    to,
    subject,
    html
  })

  if (error) {
    return new Response(JSON.stringify({ error }), { status: 500 })
  }

  return new Response(JSON.stringify({ success: true }), { status: 200 })
})
```

### 10.2 Lab Data Import

```typescript
// supabase/functions/import-lab-data/index.ts
import { serve } from "https://deno.land/std@0.168.0/http/server.ts"
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'
import * as XLSX from 'https://esm.sh/xlsx'

serve(async (req) => {
  const supabase = createClient(
    Deno.env.get('SUPABASE_URL')!,
    Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!
  )

  const formData = await req.formData()
  const file = formData.get('file') as File
  const studyId = formData.get('study_id') as string

  const arrayBuffer = await file.arrayBuffer()
  const workbook = XLSX.read(arrayBuffer, { type: 'array' })
  const sheet = workbook.Sheets[workbook.SheetNames[0]]
  const data = XLSX.utils.sheet_to_json(sheet)

  const results = []

  for (const row of data) {
    // Find subject
    const { data: subject } = await supabase
      .from('subjects')
      .select('id')
      .eq('screening_number', row.screening_number)
      .eq('study_id', studyId)
      .single()

    if (!subject) {
      results.push({ row, status: 'error', message: 'Subject not found' })
      continue
    }

    // Find lab form CRF data
    const { data: crfData } = await supabase
      .from('crf_data')
      .select('id')
      .eq('subject_id', subject.id)
      .eq('form_code', row.lab_panel) // LB1, LB2, etc.
      .single()

    // Update field values
    for (const [key, value] of Object.entries(row)) {
      if (key.startsWith('lab_')) {
        await supabase
          .from('crf_field_values')
          .upsert({
            crf_data_id: crfData.id,
            field_code: key,
            field_value: value,
            is_system_populated: true
          })
      }
    }

    results.push({ row, status: 'success' })
  }

  return new Response(JSON.stringify({ results }), { status: 200 })
})
```

---

## 11. Deployment Architecture

### 11.1 Environment Configuration

```yaml
# .env.production
NEXT_PUBLIC_SUPABASE_URL=https://xxxx.supabase.co
NEXT_PUBLIC_SUPABASE_ANON_KEY=eyJxxxx
SUPABASE_SERVICE_ROLE_KEY=eyJxxxx
RESEND_API_KEY=re_xxxx
ENCRYPTION_KEY=xxxx
```

### 11.2 Supabase Project Setup

```bash
# Install Supabase CLI
npm install -g supabase

# Initialize project
supabase init

# Link to remote project
supabase link --project-ref <project-id>

# Push database migrations
supabase db push

# Deploy edge functions
supabase functions deploy
```

### 11.3 Multi-Environment Setup

| Environment | Purpose | Database |
|-------------|---------|----------|
| Development | Local development | Local Supabase |
| Staging/UAT | Testing & validation | Supabase Project (Staging) |
| Production | Live system | Supabase Project (Prod) |

---

## 12. Performance Specifications

### 12.1 Database Optimization

```sql
-- Indexes for common queries
CREATE INDEX idx_subjects_study_site ON edc.subjects(study_id, site_id);
CREATE INDEX idx_subjects_status ON edc.subjects(status);
CREATE INDEX idx_crf_data_subject_visit ON edc.crf_data(subject_visit_id);
CREATE INDEX idx_crf_data_status ON edc.crf_data(status);
CREATE INDEX idx_queries_status ON edc.queries(status);
CREATE INDEX idx_ip_inventory_site_status ON iwrs.ip_inventory(site_id, status);

-- Materialized view for dashboard stats
CREATE MATERIALIZED VIEW edc.study_stats AS
SELECT
    s.id AS study_id,
    COUNT(DISTINCT sub.id) AS total_subjects,
    COUNT(DISTINCT sub.id) FILTER (WHERE sub.status = 'screened') AS screened,
    COUNT(DISTINCT sub.id) FILTER (WHERE sub.status = 'randomized') AS randomized,
    COUNT(DISTINCT q.id) FILTER (WHERE q.status = 'open') AS open_queries
FROM edc.studies s
LEFT JOIN edc.subjects sub ON s.id = sub.study_id
LEFT JOIN edc.queries q ON sub.id = q.subject_id
GROUP BY s.id;

-- Refresh periodically
CREATE OR REPLACE FUNCTION edc.refresh_study_stats()
RETURNS TRIGGER AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY edc.study_stats;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
```

### 12.2 Performance Targets

| Metric | Target |
|--------|--------|
| Page Load Time | < 2 seconds |
| API Response Time | < 500ms |
| Concurrent Users | 500+ |
| Data Export (1000 subjects) | < 30 seconds |
| Database Backup | Daily PITR |
| Uptime SLA | 99.9% |

---

## 13. Appendix

### 13.1 Database Diagram (ERD)

See separate ERD document or generate using:
```bash
# Generate ERD from Supabase
supabase db dump --schema=edc,iwrs,audit > schema.sql
# Use dbdiagram.io or similar tool to visualize
```

### 13.2 API Documentation

Auto-generated via Supabase at:
```
https://<project-ref>.supabase.co/rest/v1/
```

OpenAPI spec available at:
```
https://<project-ref>.supabase.co/rest/v1/?apikey=<anon-key>
```

---

*This document provides the technical blueprint for implementing the DataTrial EDC/IWRS system using Supabase. For business requirements, refer to the Functional Specification Document (FSD).*
