-- Clean up malformed products from failed CSV imports
-- These are clearly parsing errors where CSV line breaks were treated as separate records

DELETE FROM products 
WHERE company_id = 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
AND (
    -- Delete products with CSV data in the product name (clear parsing errors)
    product_name LIKE '%,AGILE Technologies,Pipettes,Pipettes,%'
    OR product_name LIKE '%Volume Range%,%'
    OR product_name = 'Raw Material & Installation'
    OR product_name = 'Raw Materila & Installation'  
    OR product_name = ',AGILE Technologies,Pipettes,Pipettes,1222102.40,Media Preparators,Active,Hari,'
    OR product_name = ',AGILE Technologies,Pipettes,Pipettes,65200.00,Lab Setup,Active,Hari,'
    OR product_name = 'Volume Range 2-20_L ,AGILE Technologies,Pipettes,Pipettes,22200.00,Equipment,Active,Hari,'
    OR product_name = 'Volume Range - 20 -200�L,AGILE Technologies,Pipettes,Pipettes,65200.00,SPR Systems,Active,Hari,'
    OR product_name = ''
    OR product_name IS NULL
    -- Also remove products that have empty principals/categories which indicate parsing errors
    OR (principal = '' AND category = '' AND sub_category = '' AND price = 0)
);

-- Show remaining products
SELECT 
    product_name, 
    product_reference_no, 
    principal, 
    category, 
    price 
FROM products 
WHERE company_id = 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
ORDER BY created_at;