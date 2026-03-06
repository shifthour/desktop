const express = require('express');
const supabase = require('../config/supabase');
const { createAuditLog } = require('../../api/enhanced-audit-trail');
const router = express.Router();

// Helper function to get user ID from request (simple version)
function getUserId(req) {
  // For now, return the startup_admin user ID as default
  // In production, this would come from JWT token or session
  return req.user?.id || 'b73c7f9f-3802-48b9-9b39-50ab242db83b';
}

// Get all subjects with filtering
router.get('/', async (req, res) => {
  try {
    const { 
      study_id, 
      site_id, 
      status, 
      page = 1, 
      limit = 50,
      search 
    } = req.query;

    let query = supabase
      .from('edc_subjects')
      .select(`
        *,
        edc_studies!inner(protocol_number, title),
        edc_sites!inner(name, site_number)
      `);

    // Apply filters
    if (study_id) query = query.eq('study_id', study_id);
    if (site_id) query = query.eq('site_id', site_id);
    if (status) query = query.eq('status', status);
    if (search) {
      query = query.or(`subject_number.ilike.%${search}%, initials.ilike.%${search}%`);
    }

    // Apply pagination
    const offset = (page - 1) * limit;
    query = query.range(offset, offset + limit - 1);

    const { data: subjects, error, count } = await query
      .order('created_at', { ascending: false });

    if (error) throw error;

    // Get total count for pagination
    const { count: totalCount } = await supabase
      .from('edc_subjects')
      .select('*', { count: 'exact', head: true });

    res.json({
      status: 'success',
      data: subjects || [],
      pagination: {
        page: parseInt(page),
        limit: parseInt(limit),
        total: totalCount,
        pages: Math.ceil(totalCount / limit)
      }
    });
  } catch (error) {
    console.error('Subjects fetch error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to fetch subjects',
      error: error.message
    });
  }
});

// Get subject by ID with full details
router.get('/:id', async (req, res) => {
  try {
    const { id } = req.params;

    const { data: subject, error: subjectError } = await supabase
      .from('edc_subjects')
      .select(`
        *,
        edc_studies!inner(protocol_number, title, description),
        edc_sites!inner(name, site_number, principal_investigator)
      `)
      .eq('id', id)
      .single();

    if (subjectError) throw subjectError;
    if (!subject) {
      return res.status(404).json({
        status: 'error',
        message: 'Subject not found'
      });
    }

    // Get subject visits
    const { data: visits } = await supabase
      .from('edc_visits')
      .select('*')
      .eq('subject_id', id)
      .order('visit_number');

    // Get subject queries
    const { data: queries } = await supabase
      .from('edc_queries')
      .select('*')
      .eq('subject_id', id)
      .order('raised_date', { ascending: false });

    res.json({
      status: 'success',
      data: {
        ...subject,
        visits: visits || [],
        queries: queries || []
      }
    });
  } catch (error) {
    console.error('Subject fetch error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to fetch subject',
      error: error.message
    });
  }
});

// Create new subject (enrollment)
router.post('/', async (req, res) => {
  try {
    const {
      study_id,
      subject_number,
      screening_number,
      initials,
      date_of_birth,
      gender,
      enrollment_date,
      treatment_arm
    } = req.body;
    
    let site_id = req.body.site_id;

    if (!study_id || !site_id || !subject_number) {
      return res.status(400).json({
        status: 'error',
        message: 'Study ID, Site ID, and Subject Number are required'
      });
    }

    // Check if subject number already exists in the study
    const { data: existingSubject } = await supabase
      .from('edc_subjects')
      .select('id')
      .eq('study_id', study_id)
      .eq('subject_number', subject_number)
      .single();

    if (existingSubject) {
      return res.status(409).json({
        status: 'error',
        message: 'Subject number already exists in this study'
      });
    }

    // Hardcoded site definitions - ensure they exist before creating subject
    const hardcodedSites = {
      'ba4f8e72-6285-4ae2-a46c-27ccfeac6af7': { site_number: 'SITE-001', name: 'Boston Medical Center' },
      'd9c2194f-1991-4bf8-ac3b-259bb79ed819': { site_number: 'SITE-002', name: 'University Medical Center' },
      '8d89af7e-c430-4fae-9934-753a31c2516f': { site_number: 'SITE-003', name: 'Los Angeles Clinic' },
      '024aeef6-19a6-4031-a5ac-2eb23c461e28': { site_number: 'SITE-004', name: 'Chicago Research Center' }
    };

    // Handle hardcoded sites - map to existing site IDs instead of creating new ones
    if (hardcodedSites[site_id]) {
      const siteData = hardcodedSites[site_id];
      
      console.log(`🔍 Processing hardcoded site: ${site_id} -> ${siteData.site_number}`);
      
      // First check if a site with this exact ID exists
      const { data: existingSiteById, error: byIdError } = await supabase
        .from('edc_sites')
        .select('id')
        .eq('id', site_id)
        .maybeSingle();

      console.log(`🔍 Exact ID check result:`, existingSiteById, byIdError);

      if (existingSiteById) {
        console.log(`✅ Hardcoded site ${site_id} already exists`);
      } else {
        // Check if a site with same study_id and site_number already exists
        const { data: existingSiteByNumber, error: siteQueryError } = await supabase
          .from('edc_sites')
          .select('id, site_number')
          .eq('study_id', study_id)
          .eq('site_number', siteData.site_number)
          .maybeSingle();
        
        console.log(`🔍 Looking for existing site: study_id=${study_id}, site_number=${siteData.site_number}`);
        console.log('🔍 Site query result:', existingSiteByNumber);
        console.log('🔍 Site query error:', siteQueryError);

        if (existingSiteByNumber && !siteQueryError) {
          // Site exists with different ID - use the existing site's ID for subject creation
          console.log(`🔄 Found existing site with ${siteData.site_number}, using existing ID: ${existingSiteByNumber.id}`);
          site_id = existingSiteByNumber.id;
        } else {
          // No site exists - create the hardcoded site for this study
          console.log(`🏥 Creating hardcoded site for study: ${siteData.site_number} - ${siteData.name}`);
          
          const { error: siteError } = await supabase
            .from('edc_sites')
            .insert([{
              id: site_id, // Use the hardcoded UUID
              study_id: null, // Make sites study-agnostic so they persist across studies
              name: siteData.name,
              site_number: siteData.site_number,
              principal_investigator: 'Dr. Principal Investigator',
              address: 'Medical Center Address',
              city: 'City',
              state: 'State',
              country: 'USA',
              coordinator_name: 'Site Coordinator',
              coordinator_email: 'coordinator@site.com',
              coordinator_phone: '(555) 123-4567',
              status: 'ACTIVE'
            }]);

          if (siteError) {
            console.error('Failed to create hardcoded site:', siteError);
            return res.status(500).json({
              status: 'error',
              message: `Failed to create required site ${siteData.site_number}: ${siteError.message}`,
              error: siteError.message
            });
          }
          
          console.log(`✅ Successfully created hardcoded site: ${siteData.site_number}`);
        }
      }
    }

    const { data: subject, error } = await supabase
      .from('edc_subjects')
      .insert([{
        study_id,
        site_id,
        subject_number,
        screening_number,
        initials,
        date_of_birth,
        gender,
        enrollment_date: enrollment_date || new Date().toISOString().split('T')[0],
        treatment_arm,
        status: 'ENROLLED'
      }])
      .select(`
        *,
        edc_studies!inner(protocol_number, title),
        edc_sites!inner(name, site_number)
      `)
      .single();

    if (error) throw error;

    // Create audit log entry for subject enrollment
    try {
      await createAuditLog({
        studyId: study_id,
        siteId: site_id,
        subjectId: subject.id,
        userId: getUserId(req),
        action: 'CREATE',
        tableName: 'edc_subjects',
        recordId: subject.id,
        fieldName: 'subject_enrollment',
        oldValue: null,
        newValue: 'ENROLLED',
        reasonForChange: `Subject ${subject_number} enrolled in study ${subject.edc_studies?.protocol_number}`,
        ipAddress: req.ip || req.connection.remoteAddress,
        userAgent: req.get('User-Agent'),
        riskLevel: 'MEDIUM',
        dataCategory: 'subject_data'
      });
    } catch (auditError) {
      console.error('Audit logging failed for subject enrollment:', auditError);
      // Don't fail the main operation if audit logging fails
    }

    res.status(201).json({
      status: 'success',
      message: 'Subject enrolled successfully',
      data: subject
    });
  } catch (error) {
    console.error('Subject creation error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to enroll subject',
      error: error.message
    });
  }
});

// Update subject
router.put('/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const updates = req.body;

    // Get current subject data before update for audit comparison
    const { data: currentSubject } = await supabase
      .from('edc_subjects')
      .select('*, edc_studies!inner(protocol_number)')
      .eq('id', id)
      .single();

    if (!currentSubject) {
      return res.status(404).json({
        status: 'error',
        message: 'Subject not found'
      });
    }

    // Remove fields that shouldn't be updated directly
    delete updates.id;
    delete updates.created_at;
    delete updates.study_id;
    delete updates.subject_number; // Subject number shouldn't change after creation

    updates.updated_at = new Date().toISOString();

    const { data: subject, error } = await supabase
      .from('edc_subjects')
      .update(updates)
      .eq('id', id)
      .select(`
        *,
        edc_studies!inner(protocol_number, title),
        edc_sites!inner(name, site_number)
      `)
      .single();

    if (error) throw error;
    if (!subject) {
      return res.status(404).json({
        status: 'error',
        message: 'Subject not found'
      });
    }

    // Create audit log entries for each changed field
    try {
      for (const [fieldName, newValue] of Object.entries(updates)) {
        if (fieldName === 'updated_at') continue; // Skip timestamp field
        
        const oldValue = currentSubject[fieldName];
        if (oldValue !== newValue) {
          await createAuditLog({
            studyId: subject.study_id,
            siteId: subject.site_id,
            subjectId: subject.id,
            userId: getUserId(req),
            action: 'UPDATE',
            tableName: 'edc_subjects',
            recordId: subject.id,
            fieldName: fieldName,
            oldValue: String(oldValue || ''),
            newValue: String(newValue || ''),
            reasonForChange: `Subject ${subject.subject_number} field ${fieldName} updated in study ${subject.edc_studies?.protocol_number}`,
            ipAddress: req.ip || req.connection.remoteAddress,
            userAgent: req.get('User-Agent'),
            riskLevel: fieldName === 'status' ? 'HIGH' : 'MEDIUM',
            dataCategory: 'subject_data'
          });
        }
      }
    } catch (auditError) {
      console.error('Audit logging failed for subject update:', auditError);
      // Don't fail the main operation if audit logging fails
    }

    res.json({
      status: 'success',
      message: 'Subject updated successfully',
      data: subject
    });
  } catch (error) {
    console.error('Subject update error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to update subject',
      error: error.message
    });
  }
});

// Get subject visits
router.get('/:id/visits', async (req, res) => {
  try {
    const { id } = req.params;

    const { data: visits, error } = await supabase
      .from('edc_visits')
      .select('*')
      .eq('subject_id', id)
      .order('visit_number');

    if (error) throw error;

    res.json({
      status: 'success',
      data: visits || []
    });
  } catch (error) {
    console.error('Subject visits error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to fetch subject visits',
      error: error.message
    });
  }
});

// Get subjects by study
router.get('/study/:study_id', async (req, res) => {
  try {
    const { study_id } = req.params;
    const { status, site_id } = req.query;

    let query = supabase
      .from('edc_subjects')
      .select(`
        *,
        edc_sites!inner(name, site_number)
      `)
      .eq('study_id', study_id);

    if (status) query = query.eq('status', status);
    if (site_id) query = query.eq('site_id', site_id);

    const { data: subjects, error } = await query
      .order('subject_number');

    if (error) throw error;

    res.json({
      status: 'success',
      data: subjects || []
    });
  } catch (error) {
    console.error('Study subjects error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to fetch study subjects',
      error: error.message
    });
  }
});

module.exports = router;
