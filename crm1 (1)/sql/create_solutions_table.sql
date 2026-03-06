-- Solutions table for tracking what was done when cases occurred
-- Solutions represent the actions taken to resolve issues and serve as a knowledge base

CREATE TABLE IF NOT EXISTS solutions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    solution_number VARCHAR(50) UNIQUE NOT NULL,
    solution_date DATE NOT NULL,
    case_id UUID REFERENCES cases(id),
    case_number VARCHAR(50),
    title VARCHAR(500) NOT NULL,
    description TEXT NOT NULL,
    solution_type VARCHAR(100) NOT NULL,
    solution_category VARCHAR(100) NOT NULL,
    problem_statement TEXT NOT NULL,
    root_cause TEXT,
    solution_steps TEXT NOT NULL,
    implementation_details TEXT,
    tools_required JSONB,
    parts_required JSONB,
    software_versions TEXT,
    configuration_changes TEXT,
    testing_performed TEXT,
    validation_steps TEXT,
    customer_name VARCHAR(255) NOT NULL,
    contact_person VARCHAR(255),
    product_name VARCHAR(500),
    product_version VARCHAR(100),
    environment VARCHAR(255),
    implementation_time_hours DECIMAL(5,2),
    difficulty_level VARCHAR(20) DEFAULT 'Medium',
    success_rate VARCHAR(20) DEFAULT 'High',
    status VARCHAR(50) DEFAULT 'Implemented',
    effectiveness VARCHAR(20) DEFAULT 'Effective',
    customer_satisfaction INTEGER CHECK (customer_satisfaction >= 1 AND customer_satisfaction <= 5),
    customer_feedback TEXT,
    implemented_by VARCHAR(255),
    verified_by VARCHAR(255),
    approved_by VARCHAR(255),
    implementation_date DATE,
    verification_date DATE,
    approval_date DATE,
    follow_up_required BOOLEAN DEFAULT FALSE,
    follow_up_date DATE,
    follow_up_notes TEXT,
    knowledge_base_entry BOOLEAN DEFAULT TRUE,
    public_solution BOOLEAN DEFAULT FALSE,
    tags JSONB,
    related_solutions JSONB,
    attachments JSONB,
    screenshots JSONB,
    documentation_links JSONB,
    training_material_links JSONB,
    cost_involved DECIMAL(10,2) DEFAULT 0,
    time_saved_hours DECIMAL(8,2),
    prevention_measures TEXT,
    lessons_learned TEXT,
    recommendations TEXT,
    applicable_products JSONB,
    applicable_versions JSONB,
    prerequisites TEXT,
    warnings TEXT,
    notes TEXT,
    internal_notes TEXT,
    version VARCHAR(10) DEFAULT '1.0',
    superseded_by UUID REFERENCES solutions(id),
    supersedes UUID REFERENCES solutions(id),
    created_from_case BOOLEAN DEFAULT TRUE,
    reusable BOOLEAN DEFAULT TRUE,
    region VARCHAR(100),
    company_id UUID NOT NULL,
    created_by UUID,
    updated_by UUID,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add indexes for better performance
CREATE INDEX IF NOT EXISTS idx_solutions_company_id ON solutions(company_id);
CREATE INDEX IF NOT EXISTS idx_solutions_case_id ON solutions(case_id);
CREATE INDEX IF NOT EXISTS idx_solutions_status ON solutions(status);
CREATE INDEX IF NOT EXISTS idx_solutions_solution_type ON solutions(solution_type);
CREATE INDEX IF NOT EXISTS idx_solutions_solution_category ON solutions(solution_category);
CREATE INDEX IF NOT EXISTS idx_solutions_customer_name ON solutions(customer_name);
CREATE INDEX IF NOT EXISTS idx_solutions_product_name ON solutions(product_name);
CREATE INDEX IF NOT EXISTS idx_solutions_implemented_by ON solutions(implemented_by);
CREATE INDEX IF NOT EXISTS idx_solutions_solution_date ON solutions(solution_date);
CREATE INDEX IF NOT EXISTS idx_solutions_difficulty_level ON solutions(difficulty_level);
CREATE INDEX IF NOT EXISTS idx_solutions_effectiveness ON solutions(effectiveness);
CREATE INDEX IF NOT EXISTS idx_solutions_knowledge_base ON solutions(knowledge_base_entry);
CREATE INDEX IF NOT EXISTS idx_solutions_public ON solutions(public_solution);
CREATE INDEX IF NOT EXISTS idx_solutions_reusable ON solutions(reusable);

-- Add check constraints
ALTER TABLE solutions ADD CONSTRAINT solutions_status_check 
    CHECK (status IN ('Draft', 'Under Review', 'Approved', 'Implemented', 'Tested', 'Verified', 'Published', 'Superseded', 'Archived'));

ALTER TABLE solutions ADD CONSTRAINT solutions_difficulty_level_check 
    CHECK (difficulty_level IN ('Easy', 'Medium', 'Hard', 'Expert'));

ALTER TABLE solutions ADD CONSTRAINT solutions_success_rate_check 
    CHECK (success_rate IN ('Low', 'Medium', 'High', 'Very High'));

ALTER TABLE solutions ADD CONSTRAINT solutions_effectiveness_check 
    CHECK (effectiveness IN ('Not Effective', 'Partially Effective', 'Effective', 'Highly Effective'));

ALTER TABLE solutions ADD CONSTRAINT solutions_solution_type_check 
    CHECK (solution_type IN ('Bug Fix', 'Configuration Change', 'Hardware Replacement', 'Software Update', 'Process Improvement', 'Training', 'Documentation Update', 'Preventive Measure', 'Workaround', 'Permanent Fix', 'Emergency Response', 'Optimization', 'Integration Fix', 'Security Fix', 'Performance Tuning'));

-- Sample data for testing
INSERT INTO solutions (
    solution_number, solution_date, case_number, title, description, 
    solution_type, solution_category, problem_statement, root_cause,
    solution_steps, customer_name, contact_person, product_name,
    implemented_by, status, difficulty_level, effectiveness, company_id
) VALUES 
(
    'SOL-2025-001', '2025-01-28', 'CASE-2025-001', 'Fibrinometer Calibration Drift Fix',
    'Complete solution for resolving calibration drift issues in Fibrinometer FG-2000 series after software updates.',
    'Software Update', 'Technical Support', 
    'Fibrinometer showing inconsistent calibration readings after software update causing unreliable test results.',
    'Software update modified calibration algorithm without preserving user-specific calibration parameters.',
    'STEP 1: Backup current calibration data\nSTEP 2: Download firmware patch v2.1.3\nSTEP 3: Install patch in maintenance mode\nSTEP 4: Restore calibration parameters\nSTEP 5: Perform full calibration verification\nSTEP 6: Test with known samples\nSTEP 7: Document new calibration values',
    'Kerala Agricultural University', 'Dr. Priya Sharma', 'Fibrinometer Model FG-2000',
    'Rajesh Kumar', 'Implemented', 'Medium', 'Highly Effective', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    'SOL-2025-002', '2025-01-26', 'CASE-2025-002', 'Missing Centrifuge Accessories Resolution',
    'Standard procedure for handling missing accessories in equipment shipments and ensuring complete delivery.',
    'Process Improvement', 'Delivery Issue',
    'Customer received centrifuge equipment but critical accessories (rotor, safety lid) were missing from shipment.',
    'Packaging error at warehouse level - accessories packed separately but shipping manifest not updated.',
    'STEP 1: Verify original order details and packaging list\nSTEP 2: Contact warehouse to locate missing items\nSTEP 3: Arrange express shipping of missing accessories\nSTEP 4: Coordinate with installation team for revised schedule\nSTEP 5: Update packaging procedures to prevent recurrence\nSTEP 6: Provide customer with tracking information\nSTEP 7: Follow up to ensure successful installation',
    'Eurofins Advinus', 'Mr. Mahesh Patel', 'High-Speed Centrifuge HSC-3000',
    'Anjali Menon', 'Implemented', 'Easy', 'Effective', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    'SOL-2025-003', '2025-01-21', 'CASE-2025-003', 'Premium License Activation Fix',
    'Solution for resolving software license authentication failures when upgrading to premium features.',
    'Configuration Change', 'Software Issue',
    'Customer unable to activate premium software features after license upgrade due to authentication server errors.',
    'License server cache not updated with new premium license credentials, causing authentication failures.',
    'STEP 1: Verify license upgrade in customer account\nSTEP 2: Clear license server cache\nSTEP 3: Regenerate license activation keys\nSTEP 4: Update customer license file\nSTEP 5: Test premium feature activation\nSTEP 6: Verify all premium modules are accessible\nSTEP 7: Provide backup activation procedure',
    'JNCASR', 'Dr. Anu Rang', 'SpectroPro Analysis Software',
    'Technical Team', 'Implemented', 'Medium', 'Highly Effective', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    'SOL-2025-004', '2025-01-19', 'CASE-2025-004', 'Microscope Objective Lens Replacement',
    'Standard warranty replacement procedure for defective microscope objective lenses with quality control measures.',
    'Hardware Replacement', 'Warranty',
    'Microscope objective lens showing optical distortion and poor image quality due to manufacturing defect.',
    'Manufacturing quality control issue - lens coating applied unevenly causing optical distortions.',
    'STEP 1: Document defect with high-resolution images\nSTEP 2: Verify warranty coverage and manufacturing date\nSTEP 3: Order replacement lens from quality-controlled batch\nSTEP 4: Coordinate pickup of defective lens\nSTEP 5: Install and test replacement lens\nSTEP 6: Perform optical calibration and image quality verification\nSTEP 7: Provide extended warranty for replacement',
    'Bio-Rad Laboratories', 'Mr. Sanjay Patel', 'High-Resolution Microscope Objective 100x',
    'Warranty Team', 'Implemented', 'Medium', 'Effective', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    'SOL-2025-005', '2025-01-16', 'CASE-2025-005', 'Comprehensive Staff Training Program',
    'Complete training solution for new laboratory staff covering equipment operation, safety procedures, and best practices.',
    'Training', 'Training',
    'Customer requires comprehensive training for 5 new lab technicians on equipment operation and safety procedures.',
    'New staff lack proper training on specialized laboratory equipment and safety protocols.',
    'STEP 1: Assess training needs and skill levels\nSTEP 2: Develop customized training curriculum\nSTEP 3: Schedule on-site training sessions\nSTEP 4: Conduct equipment operation training\nSTEP 5: Provide safety procedure certification\nSTEP 6: Deliver hands-on practical sessions\nSTEP 7: Conduct competency assessment and certification',
    'TSAR Labcare', 'Ms. Pauline D''Souza', 'Complete Laboratory Setup',
    'Sanjay Verma', 'Implemented', 'Easy', 'Highly Effective', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
);

-- Update trigger for updated_at
CREATE OR REPLACE FUNCTION update_solutions_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_solutions_updated_at_trigger
    BEFORE UPDATE ON solutions
    FOR EACH ROW
    EXECUTE FUNCTION update_solutions_updated_at();

COMMENT ON TABLE solutions IS 'Solutions and resolutions for cases - knowledge base for support';
COMMENT ON COLUMN solutions.solution_number IS 'Unique solution tracking number';
COMMENT ON COLUMN solutions.case_id IS 'Reference to the original case this solution addresses';
COMMENT ON COLUMN solutions.problem_statement IS 'Clear statement of the problem being solved';
COMMENT ON COLUMN solutions.root_cause IS 'Identified root cause of the problem';
COMMENT ON COLUMN solutions.solution_steps IS 'Detailed steps to implement the solution';
COMMENT ON COLUMN solutions.knowledge_base_entry IS 'Whether this solution should be in knowledge base';
COMMENT ON COLUMN solutions.reusable IS 'Whether this solution can be reused for similar cases';
COMMENT ON COLUMN solutions.effectiveness IS 'How effective this solution has proven to be';
COMMENT ON COLUMN solutions.superseded_by IS 'Reference to newer solution that replaces this one';
COMMENT ON COLUMN solutions.supersedes IS 'Reference to older solution this one replaces';