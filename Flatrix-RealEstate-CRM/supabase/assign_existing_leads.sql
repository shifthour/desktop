-- Script to assign existing unassigned leads to agents using round-robin

-- First, make sure the get_next_assignee function exists by running update_lead_rotation_agent_only.sql

-- Then run this to assign all unassigned leads
DO $$
DECLARE
    lead_record RECORD;
    next_agent_id UUID;
BEGIN
    -- Loop through all leads that don't have an assigned_to_id
    FOR lead_record IN
        SELECT id
        FROM flatrix_leads
        WHERE assigned_to_id IS NULL
        ORDER BY created_at ASC
    LOOP
        -- Get next agent in rotation
        SELECT get_next_assignee() INTO next_agent_id;

        -- Update the lead with the assigned agent
        IF next_agent_id IS NOT NULL THEN
            UPDATE flatrix_leads
            SET assigned_to_id = next_agent_id,
                updated_at = NOW()
            WHERE id = lead_record.id;

            RAISE NOTICE 'Assigned lead % to agent %', lead_record.id, next_agent_id;
        ELSE
            RAISE NOTICE 'No agents available for assignment';
            EXIT; -- Stop if no agents available
        END IF;
    END LOOP;
END $$;

-- Verify the assignments
SELECT
    l.id,
    l.name,
    l.phone,
    u.name as assigned_to_name,
    l.created_at
FROM flatrix_leads l
LEFT JOIN flatrix_users u ON l.assigned_to_id = u.id
ORDER BY l.created_at DESC
LIMIT 20;
