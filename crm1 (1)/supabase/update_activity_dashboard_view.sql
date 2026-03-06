-- Update activity dashboard view with clearer counting logic
CREATE OR REPLACE VIEW activity_dashboard AS
SELECT 
    company_id,
    COUNT(*) as total_activities,
    COUNT(CASE WHEN DATE(scheduled_date) = CURRENT_DATE OR DATE(due_date) = CURRENT_DATE THEN 1 END) as today_scheduled,
    COUNT(CASE WHEN status = 'completed' AND DATE(completed_date) = CURRENT_DATE THEN 1 END) as completed_today,
    COUNT(CASE WHEN status != 'completed' AND due_date < NOW() THEN 1 END) as overdue,
    COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending,
    COUNT(CASE WHEN (activity_type LIKE '%follow%' OR title ILIKE '%follow%') AND status != 'completed' AND (DATE(scheduled_date) = CURRENT_DATE OR DATE(due_date) = CURRENT_DATE) THEN 1 END) as follow_ups_due
FROM activities
GROUP BY company_id;

GRANT SELECT ON activity_dashboard TO authenticated;