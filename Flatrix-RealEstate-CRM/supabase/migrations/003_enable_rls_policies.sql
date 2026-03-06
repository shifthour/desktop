-- Enable Row Level Security on all tables
ALTER TABLE flatrix_users ENABLE ROW LEVEL SECURITY;
ALTER TABLE flatrix_channel_partners ENABLE ROW LEVEL SECURITY;
ALTER TABLE flatrix_projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE flatrix_project_partners ENABLE ROW LEVEL SECURITY;
ALTER TABLE flatrix_properties ENABLE ROW LEVEL SECURITY;
ALTER TABLE flatrix_leads ENABLE ROW LEVEL SECURITY;
ALTER TABLE flatrix_deals ENABLE ROW LEVEL SECURITY;
ALTER TABLE flatrix_commissions ENABLE ROW LEVEL SECURITY;
ALTER TABLE flatrix_activities ENABLE ROW LEVEL SECURITY;
ALTER TABLE flatrix_tasks ENABLE ROW LEVEL SECURITY;

-- Create policies for authenticated users

-- Users table policies
CREATE POLICY "Users can view all users" ON flatrix_users
    FOR SELECT USING (true);

CREATE POLICY "Users can update their own profile" ON flatrix_users
    FOR UPDATE USING (auth.uid()::text = id::text);

-- Channel Partners policies
CREATE POLICY "View all active channel partners" ON flatrix_channel_partners
    FOR SELECT USING (is_active = true);

CREATE POLICY "Admins can manage channel partners" ON flatrix_channel_partners
    FOR ALL USING (
        EXISTS (
            SELECT 1 FROM flatrix_users 
            WHERE id::text = auth.uid()::text 
            AND role IN ('ADMIN', 'SALES_MANAGER')
        )
    );

-- Projects policies
CREATE POLICY "View all active projects" ON flatrix_projects
    FOR SELECT USING (is_active = true);

CREATE POLICY "Admins can manage projects" ON flatrix_projects
    FOR ALL USING (
        EXISTS (
            SELECT 1 FROM flatrix_users 
            WHERE id::text = auth.uid()::text 
            AND role IN ('ADMIN', 'SALES_MANAGER')
        )
    );

-- Properties policies
CREATE POLICY "View all properties" ON flatrix_properties
    FOR SELECT USING (true);

CREATE POLICY "Admins can manage properties" ON flatrix_properties
    FOR ALL USING (
        EXISTS (
            SELECT 1 FROM flatrix_users 
            WHERE id::text = auth.uid()::text 
            AND role IN ('ADMIN', 'SALES_MANAGER')
        )
    );

-- Leads policies
CREATE POLICY "View leads based on role" ON flatrix_leads
    FOR SELECT USING (
        -- Admins and Sales Managers can see all leads
        EXISTS (
            SELECT 1 FROM flatrix_users 
            WHERE id::text = auth.uid()::text 
            AND role IN ('ADMIN', 'SALES_MANAGER')
        )
        OR
        -- Agents can see their assigned leads
        assigned_to_id::text = auth.uid()::text
        OR
        -- Channel partners can see their leads
        EXISTS (
            SELECT 1 FROM flatrix_users u
            JOIN flatrix_channel_partners cp ON u.channel_partner_id = cp.id
            WHERE u.id::text = auth.uid()::text 
            AND cp.id = flatrix_leads.channel_partner_id
        )
    );

CREATE POLICY "Create and update leads based on role" ON flatrix_leads
    FOR ALL USING (
        EXISTS (
            SELECT 1 FROM flatrix_users 
            WHERE id::text = auth.uid()::text 
            AND role IN ('ADMIN', 'SALES_MANAGER', 'AGENT', 'CHANNEL_PARTNER')
        )
    );

-- Deals policies
CREATE POLICY "View deals based on role" ON flatrix_deals
    FOR SELECT USING (
        -- Admins and Sales Managers can see all deals
        EXISTS (
            SELECT 1 FROM flatrix_users 
            WHERE id::text = auth.uid()::text 
            AND role IN ('ADMIN', 'SALES_MANAGER')
        )
        OR
        -- Users can see their own deals
        user_id::text = auth.uid()::text
        OR
        -- Channel partners can see their deals
        EXISTS (
            SELECT 1 FROM flatrix_users u
            JOIN flatrix_channel_partners cp ON u.channel_partner_id = cp.id
            WHERE u.id::text = auth.uid()::text 
            AND cp.id = flatrix_deals.channel_partner_id
        )
    );

CREATE POLICY "Manage deals based on role" ON flatrix_deals
    FOR ALL USING (
        EXISTS (
            SELECT 1 FROM flatrix_users 
            WHERE id::text = auth.uid()::text 
            AND role IN ('ADMIN', 'SALES_MANAGER', 'AGENT')
        )
    );

-- Commissions policies
CREATE POLICY "View commissions based on role" ON flatrix_commissions
    FOR SELECT USING (
        -- Admins and Sales Managers can see all commissions
        EXISTS (
            SELECT 1 FROM flatrix_users 
            WHERE id::text = auth.uid()::text 
            AND role IN ('ADMIN', 'SALES_MANAGER')
        )
        OR
        -- Channel partners can see their own commissions
        EXISTS (
            SELECT 1 FROM flatrix_users u
            JOIN flatrix_channel_partners cp ON u.channel_partner_id = cp.id
            WHERE u.id::text = auth.uid()::text 
            AND cp.id = flatrix_commissions.channel_partner_id
        )
    );

CREATE POLICY "Only admins can manage commissions" ON flatrix_commissions
    FOR ALL USING (
        EXISTS (
            SELECT 1 FROM flatrix_users 
            WHERE id::text = auth.uid()::text 
            AND role IN ('ADMIN', 'SALES_MANAGER')
        )
    );

-- Activities policies
CREATE POLICY "View activities based on role" ON flatrix_activities
    FOR SELECT USING (
        -- Admins can see all activities
        EXISTS (
            SELECT 1 FROM flatrix_users 
            WHERE id::text = auth.uid()::text 
            AND role IN ('ADMIN', 'SALES_MANAGER')
        )
        OR
        -- Users can see their own activities
        user_id::text = auth.uid()::text
        OR
        -- Users can see activities related to their leads
        EXISTS (
            SELECT 1 FROM flatrix_leads 
            WHERE id = flatrix_activities.lead_id 
            AND assigned_to_id::text = auth.uid()::text
        )
    );

CREATE POLICY "Users can create activities" ON flatrix_activities
    FOR INSERT WITH CHECK (user_id::text = auth.uid()::text);

-- Tasks policies
CREATE POLICY "View tasks based on assignment" ON flatrix_tasks
    FOR SELECT USING (
        -- Admins can see all tasks
        EXISTS (
            SELECT 1 FROM flatrix_users 
            WHERE id::text = auth.uid()::text 
            AND role IN ('ADMIN', 'SALES_MANAGER')
        )
        OR
        -- Users can see tasks assigned to them
        assigned_to_id::text = auth.uid()::text
    );

CREATE POLICY "Manage tasks based on role" ON flatrix_tasks
    FOR ALL USING (
        EXISTS (
            SELECT 1 FROM flatrix_users 
            WHERE id::text = auth.uid()::text 
            AND role IN ('ADMIN', 'SALES_MANAGER')
        )
        OR
        assigned_to_id::text = auth.uid()::text
    );