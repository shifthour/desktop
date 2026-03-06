-- Add new columns to flatrix_projects table

ALTER TABLE flatrix_projects 
ADD COLUMN IF NOT EXISTS project_stage VARCHAR(50) DEFAULT 'Planning' CHECK (project_stage IN ('Planning', 'Under Construction', 'Completed')),
ADD COLUMN IF NOT EXISTS start_date DATE,
ADD COLUMN IF NOT EXISTS number_of_floors INTEGER,
ADD COLUMN IF NOT EXISTS number_of_towers INTEGER,
ADD COLUMN IF NOT EXISTS rera_number VARCHAR(100),
ADD COLUMN IF NOT EXISTS primary_contact_name VARCHAR(100),
ADD COLUMN IF NOT EXISTS primary_contact_phone VARCHAR(20),
ADD COLUMN IF NOT EXISTS alternate_contact_name VARCHAR(100),
ADD COLUMN IF NOT EXISTS alternate_contact_phone VARCHAR(20),
ADD COLUMN IF NOT EXISTS project_size_acres DECIMAL(10,2),
ADD COLUMN IF NOT EXISTS cover_image_url TEXT;

-- Add indexes for better performance
CREATE INDEX IF NOT EXISTS idx_flatrix_projects_stage ON flatrix_projects(project_stage);
CREATE INDEX IF NOT EXISTS idx_flatrix_projects_rera ON flatrix_projects(rera_number);

-- Update any existing records to have default project_stage
UPDATE flatrix_projects 
SET project_stage = 'Planning' 
WHERE project_stage IS NULL;