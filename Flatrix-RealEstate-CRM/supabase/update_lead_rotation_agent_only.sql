-- Update function to assign leads only to AGENT role users (not ADMIN or SALES_MANAGER)
CREATE OR REPLACE FUNCTION get_next_assignee()
RETURNS UUID AS $$
DECLARE
    next_user_id UUID;
    last_user_id UUID;
BEGIN
    -- Get the last assigned user
    SELECT last_assigned_user_id INTO last_user_id
    FROM flatrix_lead_rotation
    WHERE id = '00000000-0000-0000-0000-000000000001';

    -- Get the next active user in rotation (AGENT role only)
    -- Order by created_at to maintain consistent order
    SELECT id INTO next_user_id
    FROM flatrix_users
    WHERE is_active = true
      AND role = 'AGENT'
      AND (
        -- If we have a last_user_id, get the next one after it
        (last_user_id IS NOT NULL AND created_at > (SELECT created_at FROM flatrix_users WHERE id = last_user_id))
        -- Or if last_user_id is null, get the first one
        OR last_user_id IS NULL
      )
    ORDER BY created_at ASC
    LIMIT 1;

    -- If no user found (we've reached the end), loop back to the first user
    IF next_user_id IS NULL THEN
        SELECT id INTO next_user_id
        FROM flatrix_users
        WHERE is_active = true
          AND role = 'AGENT'
        ORDER BY created_at ASC
        LIMIT 1;
    END IF;

    -- Update the rotation tracker
    IF next_user_id IS NOT NULL THEN
        UPDATE flatrix_lead_rotation
        SET last_assigned_user_id = next_user_id,
            last_assignment_at = NOW(),
            total_assignments = total_assignments + 1,
            updated_at = NOW()
        WHERE id = '00000000-0000-0000-0000-000000000001';
    END IF;

    RETURN next_user_id;
END;
$$ LANGUAGE plpgsql;

-- Update comment
COMMENT ON FUNCTION get_next_assignee() IS 'Returns the next AGENT user ID in round-robin rotation for lead assignment';
