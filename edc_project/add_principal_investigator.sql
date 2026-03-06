-- Add principal_investigator column to edc_studies table
ALTER TABLE edc_studies 
ADD COLUMN principal_investigator TEXT;