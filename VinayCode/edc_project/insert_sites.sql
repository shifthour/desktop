-- Insert 4 sites to match the form creation dropdown
-- These will replace the hardcoded sites that were causing UUID errors

-- First, get the study IDs (assuming we want to add sites to existing studies)
-- We'll add these sites to the first study in the database

INSERT INTO edc_sites (study_id, name, site_number, city, state, country, principal_investigator, status, subjects_enrolled, data_complete_percentage)
VALUES 
    -- Site 1: Boston Medical Center
    (
        (SELECT id FROM edc_studies LIMIT 1),
        'Boston Medical Center',
        'SITE-001',
        'Boston',
        'Massachusetts',
        'USA',
        'Dr. Sarah Johnson',
        'ACTIVE',
        0,
        0.0
    ),
    -- Site 2: New York Hospital  
    (
        (SELECT id FROM edc_studies LIMIT 1),
        'New York Hospital',
        'SITE-002', 
        'New York',
        'New York',
        'USA',
        'Dr. Michael Chen',
        'ACTIVE',
        0,
        0.0
    ),
    -- Site 3: Los Angeles Clinic
    (
        (SELECT id FROM edc_studies LIMIT 1),
        'Los Angeles Clinic',
        'SITE-003',
        'Los Angeles', 
        'California',
        'USA',
        'Dr. Emily Rodriguez',
        'ACTIVE',
        0,
        0.0
    ),
    -- Site 4: Chicago Research Center
    (
        (SELECT id FROM edc_studies LIMIT 1),
        'Chicago Research Center',
        'SITE-004',
        'Chicago',
        'Illinois', 
        'USA',
        'Dr. James Wilson',
        'ACTIVE',
        0,
        0.0
    )
ON CONFLICT (study_id, site_number) DO NOTHING;

-- Verify the insert
SELECT id, name, site_number FROM edc_sites ORDER BY site_number;