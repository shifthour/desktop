-- Document Library Tables for organizing and managing documents
-- Tables for folders, documents, and file storage management

-- Folders table for organizing documents
CREATE TABLE IF NOT EXISTS document_folders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    folder_name VARCHAR(255) NOT NULL,
    folder_description TEXT,
    parent_folder_id UUID REFERENCES document_folders(id),
    folder_path TEXT, -- Full path like "/Product Manuals/Spectrophotometers"
    folder_level INTEGER DEFAULT 0,
    folder_color VARCHAR(20) DEFAULT '#3B82F6',
    is_system_folder BOOLEAN DEFAULT FALSE,
    permissions JSONB, -- Read/write permissions
    tags JSONB,
    company_id UUID NOT NULL,
    created_by UUID,
    updated_by UUID,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Documents table for file management
CREATE TABLE IF NOT EXISTS documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_name VARCHAR(500) NOT NULL,
    original_filename VARCHAR(500) NOT NULL,
    file_extension VARCHAR(10) NOT NULL,
    file_size BIGINT NOT NULL, -- Size in bytes
    file_type VARCHAR(100) NOT NULL, -- MIME type
    file_url TEXT, -- URL to stored file (Supabase Storage, S3, etc.)
    file_path TEXT, -- Storage path
    folder_id UUID REFERENCES document_folders(id),
    folder_path TEXT, -- Denormalized folder path for quick access
    description TEXT,
    tags JSONB,
    metadata JSONB, -- Additional file metadata
    version VARCHAR(20) DEFAULT '1.0',
    status VARCHAR(50) DEFAULT 'Active',
    access_level VARCHAR(50) DEFAULT 'Company', -- Public, Company, Department, Private
    download_count INTEGER DEFAULT 0,
    last_accessed_at TIMESTAMP WITH TIME ZONE,
    last_accessed_by UUID,
    checksum VARCHAR(255), -- File integrity check
    uploaded_by UUID NOT NULL,
    uploaded_by_name VARCHAR(255),
    approved_by UUID,
    approved_at TIMESTAMP WITH TIME ZONE,
    expiry_date DATE,
    is_locked BOOLEAN DEFAULT FALSE,
    lock_reason TEXT,
    company_id UUID NOT NULL,
    created_by UUID,
    updated_by UUID,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Document versions table for version control
CREATE TABLE IF NOT EXISTS document_versions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    version_number VARCHAR(20) NOT NULL,
    file_url TEXT NOT NULL,
    file_size BIGINT NOT NULL,
    changes_description TEXT,
    uploaded_by UUID NOT NULL,
    uploaded_by_name VARCHAR(255),
    company_id UUID NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Document access logs for tracking usage
CREATE TABLE IF NOT EXISTS document_access_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    user_id UUID,
    user_name VARCHAR(255),
    action VARCHAR(50) NOT NULL, -- view, download, edit, delete
    ip_address INET,
    user_agent TEXT,
    company_id UUID NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add indexes for better performance
CREATE INDEX IF NOT EXISTS idx_document_folders_company_id ON document_folders(company_id);
CREATE INDEX IF NOT EXISTS idx_document_folders_parent_id ON document_folders(parent_folder_id);
CREATE INDEX IF NOT EXISTS idx_document_folders_path ON document_folders(folder_path);

CREATE INDEX IF NOT EXISTS idx_documents_company_id ON documents(company_id);
CREATE INDEX IF NOT EXISTS idx_documents_folder_id ON documents(folder_id);
CREATE INDEX IF NOT EXISTS idx_documents_uploaded_by ON documents(uploaded_by);
CREATE INDEX IF NOT EXISTS idx_documents_status ON documents(status);
CREATE INDEX IF NOT EXISTS idx_documents_file_type ON documents(file_type);
CREATE INDEX IF NOT EXISTS idx_documents_created_at ON documents(created_at);
CREATE INDEX IF NOT EXISTS idx_documents_name ON documents(document_name);

CREATE INDEX IF NOT EXISTS idx_document_versions_document_id ON document_versions(document_id);
CREATE INDEX IF NOT EXISTS idx_document_versions_company_id ON document_versions(company_id);

CREATE INDEX IF NOT EXISTS idx_document_access_logs_document_id ON document_access_logs(document_id);
CREATE INDEX IF NOT EXISTS idx_document_access_logs_user_id ON document_access_logs(user_id);
CREATE INDEX IF NOT EXISTS idx_document_access_logs_company_id ON document_access_logs(company_id);

-- Add check constraints
ALTER TABLE document_folders ADD CONSTRAINT document_folders_level_check 
    CHECK (folder_level >= 0 AND folder_level <= 10);

ALTER TABLE documents ADD CONSTRAINT documents_status_check 
    CHECK (status IN ('Active', 'Archived', 'Deleted', 'Under Review', 'Approved', 'Rejected'));

ALTER TABLE documents ADD CONSTRAINT documents_access_level_check 
    CHECK (access_level IN ('Public', 'Company', 'Department', 'Private'));

ALTER TABLE documents ADD CONSTRAINT documents_file_size_check 
    CHECK (file_size > 0);

ALTER TABLE document_access_logs ADD CONSTRAINT document_access_logs_action_check 
    CHECK (action IN ('view', 'download', 'edit', 'delete', 'upload', 'share'));

-- Sample default folders
INSERT INTO document_folders (
    folder_name, folder_description, folder_path, folder_level, 
    folder_color, is_system_folder, company_id
) VALUES 
(
    'Product Manuals', 'User manuals and documentation for all products', 
    '/Product Manuals', 0, '#3B82F6', TRUE, 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    'Installation Guides', 'Step-by-step installation and setup guides', 
    '/Installation Guides', 0, '#10B981', TRUE, 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    'Service Documents', 'Service agreements, maintenance schedules, and AMC documents', 
    '/Service Documents', 0, '#F59E0B', TRUE, 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    'Quotation Templates', 'Pre-formatted quotation templates and pricing documents', 
    '/Quotation Templates', 0, '#8B5CF6', TRUE, 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    'Compliance Certificates', 'Regulatory compliance, quality certificates, and approvals', 
    '/Compliance Certificates', 0, '#EF4444', TRUE, 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    'Training Materials', 'Training manuals, videos, and educational content', 
    '/Training Materials', 0, '#06B6D4', TRUE, 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
);

-- Sample documents
INSERT INTO documents (
    document_name, original_filename, file_extension, file_size, file_type,
    file_url, folder_id, folder_path, description, tags,
    uploaded_by, uploaded_by_name, company_id
) VALUES 
(
    'ND-1000 Spectrophotometer Manual', 'ND-1000_Spectrophotometer_Manual.pdf', 
    'pdf', 2516582, 'application/pdf', 
    '/storage/documents/ND-1000_Spectrophotometer_Manual.pdf',
    (SELECT id FROM document_folders WHERE folder_name = 'Product Manuals' LIMIT 1),
    '/Product Manuals', 
    'Complete user manual for ND-1000 Spectrophotometer including operation, maintenance, and troubleshooting',
    '["Manual", "Spectrophotometer", "ND-1000", "Operation", "Maintenance"]',
    'de19ccb7-e90d-4507-861d-a3aecf5e3f29', 'Hari Kumar K', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    'Installation Checklist Template', 'Installation_Checklist_Template.docx', 
    'docx', 159744, 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    '/storage/documents/Installation_Checklist_Template.docx',
    (SELECT id FROM document_folders WHERE folder_name = 'Installation Guides' LIMIT 1),
    '/Installation Guides',
    'Standard checklist template for equipment installation and commissioning',
    '["Template", "Installation", "Checklist", "Commissioning"]',
    'de19ccb7-e90d-4507-861d-a3aecf5e3f29', 'Pauline D''Souza', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    'AMC Service Agreement Template', 'AMC_Service_Agreement_Template.pdf', 
    'pdf', 912384, 'application/pdf',
    '/storage/documents/AMC_Service_Agreement_Template.pdf',
    (SELECT id FROM document_folders WHERE folder_name = 'Service Documents' LIMIT 1),
    '/Service Documents',
    'Annual Maintenance Contract template with standard terms and conditions',
    '["AMC", "Service", "Agreement", "Maintenance", "Template"]',
    'de19ccb7-e90d-4507-861d-a3aecf5e3f29', 'Arvind K', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    'Laboratory Equipment Quotation Template', 'Lab_Equipment_Quotation_Template.xlsx', 
    'xlsx', 251392, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    '/storage/documents/Lab_Equipment_Quotation_Template.xlsx',
    (SELECT id FROM document_folders WHERE folder_name = 'Quotation Templates' LIMIT 1),
    '/Quotation Templates',
    'Comprehensive quotation template for laboratory equipment with pricing calculations',
    '["Quotation", "Template", "Equipment", "Laboratory", "Pricing"]',
    'de19ccb7-e90d-4507-861d-a3aecf5e3f29', 'Hari Kumar K', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    'ISO 9001 Quality Certificate', 'ISO_9001_Quality_Certificate.pdf', 
    'pdf', 1245760, 'application/pdf',
    '/storage/documents/ISO_9001_Quality_Certificate.pdf',
    (SELECT id FROM document_folders WHERE folder_name = 'Compliance Certificates' LIMIT 1),
    '/Compliance Certificates',
    'ISO 9001:2015 Quality Management System certification',
    '["ISO", "Quality", "Certificate", "Compliance", "9001"]',
    'de19ccb7-e90d-4507-861d-a3aecf5e3f29', 'Certification Team', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
),
(
    'Equipment Operation Training Manual', 'Equipment_Operation_Training_Manual.pdf', 
    'pdf', 3145728, 'application/pdf',
    '/storage/documents/Equipment_Operation_Training_Manual.pdf',
    (SELECT id FROM document_folders WHERE folder_name = 'Training Materials' LIMIT 1),
    '/Training Materials',
    'Comprehensive training manual covering safe operation of laboratory equipment',
    '["Training", "Manual", "Equipment", "Operation", "Safety"]',
    'de19ccb7-e90d-4507-861d-a3aecf5e3f29', 'Sanjay Verma', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
);

-- Update trigger for folders updated_at
CREATE OR REPLACE FUNCTION update_document_folders_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_document_folders_updated_at_trigger
    BEFORE UPDATE ON document_folders
    FOR EACH ROW
    EXECUTE FUNCTION update_document_folders_updated_at();

-- Update trigger for documents updated_at
CREATE OR REPLACE FUNCTION update_documents_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_documents_updated_at_trigger
    BEFORE UPDATE ON documents
    FOR EACH ROW
    EXECUTE FUNCTION update_documents_updated_at();

-- Function to update folder document counts (called after document operations)
CREATE OR REPLACE FUNCTION update_folder_stats()
RETURNS TRIGGER AS $$
BEGIN
    -- Update folder statistics could be added here if needed
    RETURN COALESCE(NEW, OLD);
END;
$$ language 'plpgsql';

-- Table comments
COMMENT ON TABLE document_folders IS 'Folder structure for organizing documents';
COMMENT ON TABLE documents IS 'Main documents table with file metadata and storage information';
COMMENT ON TABLE document_versions IS 'Version history for documents';
COMMENT ON TABLE document_access_logs IS 'Audit trail for document access and operations';

COMMENT ON COLUMN document_folders.folder_path IS 'Full hierarchical path of the folder';
COMMENT ON COLUMN documents.file_url IS 'URL or path to the actual file in storage system';
COMMENT ON COLUMN documents.checksum IS 'File hash for integrity verification';
COMMENT ON COLUMN documents.access_level IS 'Document visibility and access control level';