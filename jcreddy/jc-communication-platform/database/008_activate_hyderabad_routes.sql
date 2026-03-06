-- 008: Activate Bangalore ↔ Hyderabad routes with all 3 operators
-- VKaveri IDs: Bangalore=18, Hyderabad=1

UPDATE jc_competitor_route_config
SET
  is_active = true,
  vkaveri_from_id = '18',
  vkaveri_to_id = '1'
WHERE route_from = 'Bangalore' AND route_to = 'Hyderabad';

UPDATE jc_competitor_route_config
SET
  is_active = true,
  vkaveri_from_id = '1',
  vkaveri_to_id = '18'
WHERE route_from = 'Hyderabad' AND route_to = 'Bangalore';
