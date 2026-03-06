-- Activities table for comprehensive activity management
-- Execute this script in Supabase SQL Editor

CREATE TABLE IF NOT EXISTS activities (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    
    -- Activity Details
    activity_type VARCHAR(50) NOT NULL, -- 'call', 'email', 'meeting', 'task', 'demo', 'follow-up', 'quote'
    title VARCHAR(255) NOT NULL,
    description TEXT,
    
    -- Entity Linking (what this activity is related to)
    entity_type VARCHAR(50), -- 'lead', 'deal', 'account', 'contact', 'product'
    entity_id UUID,
    entity_name VARCHAR(255), -- Denormalized for quick display
    
    -- Contact Information (for direct actions)
    contact_name VARCHAR(255),
    contact_phone VARCHAR(50),
    contact_email VARCHAR(255),
    contact_whatsapp VARCHAR(50),
    
    -- Scheduling and Timeline
    scheduled_date TIMESTAMP WITH TIME ZONE,
    due_date TIMESTAMP WITH TIME ZONE,
    completed_date TIMESTAMP WITH TIME ZONE,
    
    -- Status and Management
    status VARCHAR(20) DEFAULT 'pending', -- 'pending', 'in-progress', 'completed', 'cancelled', 'overdue'
    priority VARCHAR(20) DEFAULT 'medium', -- 'low', 'medium', 'high', 'urgent'
    assigned_to VARCHAR(255),
    
    -- Results and Follow-up
    outcome VARCHAR(100), -- 'successful', 'no-answer', 'callback-requested', 'not-interested', 'meeting-scheduled'
    outcome_notes TEXT,
    next_action VARCHAR(255),
    follow_up_required BOOLEAN DEFAULT false,
    follow_up_date DATE,
    
    -- Additional Context
    product_interest VARCHAR(255),
    estimated_value DECIMAL(15,2),
    duration_minutes INTEGER,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc'::text, NOW()) NOT NULL
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_activities_company_id ON activities(company_id);
CREATE INDEX IF NOT EXISTS idx_activities_user_id ON activities(user_id);
CREATE INDEX IF NOT EXISTS idx_activities_entity ON activities(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_activities_status ON activities(status);
CREATE INDEX IF NOT EXISTS idx_activities_type ON activities(activity_type);
CREATE INDEX IF NOT EXISTS idx_activities_assigned_to ON activities(assigned_to);
CREATE INDEX IF NOT EXISTS idx_activities_scheduled_date ON activities(scheduled_date);
CREATE INDEX IF NOT EXISTS idx_activities_due_date ON activities(due_date);
CREATE INDEX IF NOT EXISTS idx_activities_follow_up_date ON activities(follow_up_date);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_activities_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = TIMEZONE('utc'::text, NOW());
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to update timestamp
DROP TRIGGER IF EXISTS update_activities_updated_at ON activities;
CREATE TRIGGER update_activities_updated_at 
    BEFORE UPDATE ON activities
    FOR EACH ROW
    EXECUTE FUNCTION update_activities_updated_at();

-- Grant permissions
GRANT ALL ON activities TO authenticated;
GRANT SELECT ON activities TO anon;

-- Row Level Security
ALTER TABLE activities ENABLE ROW LEVEL SECURITY;

-- Create policy for company-based access
CREATE POLICY "Users can access activities from their company" ON activities
    FOR ALL
    USING (company_id IN (
        SELECT company_id FROM users WHERE id = auth.uid()
    ));

-- Create view for activity dashboard (quick stats)
CREATE OR REPLACE VIEW activity_dashboard AS
SELECT 
    company_id,
    COUNT(*) as total_activities,
    COUNT(CASE WHEN DATE(scheduled_date) = CURRENT_DATE THEN 1 END) as today_scheduled,
    COUNT(CASE WHEN status = 'completed' AND DATE(completed_date) = CURRENT_DATE THEN 1 END) as completed_today,
    COUNT(CASE WHEN status = 'pending' AND due_date < NOW() THEN 1 END) as overdue,
    COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending,
    COUNT(CASE WHEN (activity_type LIKE '%follow%' OR title ILIKE '%follow%') AND status != 'completed' THEN 1 END) as follow_ups_due
FROM activities
GROUP BY company_id;

GRANT SELECT ON activity_dashboard TO authenticated;