-- ═══════════════════════════════════════════════════════════════════════════════
-- UPDATE COMPETITOR ROUTE CONFIG
-- Remove non-overlapping routes, add all confirmed overlapping routes
-- ═══════════════════════════════════════════════════════════════════════════════

-- Disable routes where Vikram doesn't compete
UPDATE jc_competitor_route_config SET is_active = false
WHERE route_from = 'Hyderabad' AND route_to = 'Tirupathi';
UPDATE jc_competitor_route_config SET is_active = false
WHERE route_from = 'Tirupathi' AND route_to = 'Hyderabad';
UPDATE jc_competitor_route_config SET is_active = false
WHERE route_from = 'Hyderabad' AND route_to = 'Pune';
UPDATE jc_competitor_route_config SET is_active = false
WHERE route_from = 'Pune' AND route_to = 'Hyderabad';

-- Add new confirmed overlapping routes
-- Hyderabad ↔ Kakinada
INSERT INTO jc_competitor_route_config (route_name, route_from, route_to, mythri_from_id, mythri_to_id, vikram_from_code, vikram_to_code)
VALUES ('Hyderabad → Kakinada', 'Hyderabad', 'Kakinada', '1', '2', 'ST16891257D', 'ST5A86295N')
ON CONFLICT (route_from, route_to) DO NOTHING;

INSERT INTO jc_competitor_route_config (route_name, route_from, route_to, mythri_from_id, mythri_to_id, vikram_from_code, vikram_to_code)
VALUES ('Kakinada → Hyderabad', 'Kakinada', 'Hyderabad', '2', '1', 'ST5A86295N', 'ST16891257D')
ON CONFLICT (route_from, route_to) DO NOTHING;

-- Hyderabad ↔ Yanam
INSERT INTO jc_competitor_route_config (route_name, route_from, route_to, mythri_from_id, mythri_to_id, vikram_from_code, vikram_to_code)
VALUES ('Hyderabad → Yanam', 'Hyderabad', 'Yanam', '1', '57', 'ST16891257D', 'ST1A8D50D')
ON CONFLICT (route_from, route_to) DO NOTHING;

INSERT INTO jc_competitor_route_config (route_name, route_from, route_to, mythri_from_id, mythri_to_id, vikram_from_code, vikram_to_code)
VALUES ('Yanam → Hyderabad', 'Yanam', 'Hyderabad', '57', '1', 'ST1A8D50D', 'ST16891257D')
ON CONFLICT (route_from, route_to) DO NOTHING;

-- Hyderabad ↔ Warangal
INSERT INTO jc_competitor_route_config (route_name, route_from, route_to, mythri_from_id, mythri_to_id, vikram_from_code, vikram_to_code)
VALUES ('Hyderabad → Warangal', 'Hyderabad', 'Warangal', '1', '283', 'ST16891257D', 'ST5B861339E')
ON CONFLICT (route_from, route_to) DO NOTHING;

INSERT INTO jc_competitor_route_config (route_name, route_from, route_to, mythri_from_id, mythri_to_id, vikram_from_code, vikram_to_code)
VALUES ('Warangal → Hyderabad', 'Warangal', 'Hyderabad', '283', '1', 'ST5B861339E', 'ST16891257D')
ON CONFLICT (route_from, route_to) DO NOTHING;

-- Bangalore ↔ Warangal
INSERT INTO jc_competitor_route_config (route_name, route_from, route_to, mythri_from_id, mythri_to_id, vikram_from_code, vikram_to_code)
VALUES ('Bangalore → Warangal', 'Bangalore', 'Warangal', '89', '283', 'STF3OEX206', 'ST5B861339E')
ON CONFLICT (route_from, route_to) DO NOTHING;

INSERT INTO jc_competitor_route_config (route_name, route_from, route_to, mythri_from_id, mythri_to_id, vikram_from_code, vikram_to_code)
VALUES ('Warangal → Bangalore', 'Warangal', 'Bangalore', '283', '89', 'ST5B861339E', 'STF3OEX206')
ON CONFLICT (route_from, route_to) DO NOTHING;

-- Hyderabad ↔ Rajahmundry
INSERT INTO jc_competitor_route_config (route_name, route_from, route_to, mythri_from_id, mythri_to_id, vikram_from_code, vikram_to_code)
VALUES ('Hyderabad → Rajahmundry', 'Hyderabad', 'Rajahmundry', '1', '34', 'ST16891257D', 'ST47891418G')
ON CONFLICT (route_from, route_to) DO NOTHING;

INSERT INTO jc_competitor_route_config (route_name, route_from, route_to, mythri_from_id, mythri_to_id, vikram_from_code, vikram_to_code)
VALUES ('Rajahmundry → Hyderabad', 'Rajahmundry', 'Hyderabad', '34', '1', 'ST47891418G', 'ST16891257D')
ON CONFLICT (route_from, route_to) DO NOTHING;

-- Hyderabad ↔ Srikalahasti
INSERT INTO jc_competitor_route_config (route_name, route_from, route_to, mythri_from_id, mythri_to_id, vikram_from_code, vikram_to_code)
VALUES ('Hyderabad → Srikalahasti', 'Hyderabad', 'Srikalahasti', '1', '259', 'ST16891257D', 'ST578D323I')
ON CONFLICT (route_from, route_to) DO NOTHING;

INSERT INTO jc_competitor_route_config (route_name, route_from, route_to, mythri_from_id, mythri_to_id, vikram_from_code, vikram_to_code)
VALUES ('Srikalahasti → Hyderabad', 'Srikalahasti', 'Hyderabad', '259', '1', 'ST578D323I', 'ST16891257D')
ON CONFLICT (route_from, route_to) DO NOTHING;
