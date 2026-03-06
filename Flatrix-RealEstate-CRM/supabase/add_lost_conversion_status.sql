-- Add LOST conversion status documentation
-- This script documents the new LOST conversion status added to the system
-- No database changes needed as conversion_status uses VARCHAR(50)

-- Updated conversion_status values:
-- 'PENDING' - Site visit done, awaiting booking decision
-- 'BOOKED' - Customer booked a flat after site visit  
-- 'NOT_BOOKED' - Customer decided not to book after site visit
-- 'CANCELLED' - Site visit was cancelled, no conversion possible
-- 'LOST' - Lead lost after site visit (no show, changed mind, etc.) - no longer needed but details maintained

-- No schema changes required - VARCHAR(50) column can accept LOST value