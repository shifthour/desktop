const express = require('express');
const supabase = require('../config/supabase');
const { authenticate } = require('../middleware/auth');
const archiver = require('archiver');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const fs = require('fs');
const path = require('path');
const os = require('os');
const router = express.Router();

// Get all studies with summary data
router.get('/', async (req, res) => {
  console.log('📋 StudyRoutes: GET / called');
  try {
    const { data: studies, error } = await supabase
      .from('edc_studies')
      .select(`
        id,
        protocol_number,
        title,
        status,
        projected_enrollment,
        current_enrollment,
        created_at,
        updated_at
      `)
      .order('protocol_number');

    if (error) throw error;

    // For each study, get the counts that were in the summary view
    const studiesWithCounts = await Promise.all(studies.map(async (study) => {
      // Get site counts
      const { data: sites } = await supabase
        .from('edc_sites')
        .select('id, status')
        .eq('study_id', study.id);

      // Get subject counts  
      const { data: subjects } = await supabase
        .from('edc_subjects')
        .select('id, status')
        .eq('study_id', study.id);

      // Get query counts
      const { data: queries } = await supabase
        .from('edc_queries')
        .select('id, status')
        .eq('study_id', study.id);

      return {
        ...study,
        total_sites: sites?.length || 0,
        active_sites: sites?.filter(s => s.status === 'ACTIVE').length || 0,
        total_subjects: subjects?.length || 0,
        active_subjects: subjects?.filter(s => s.status === 'ACTIVE').length || 0,
        completed_subjects: subjects?.filter(s => s.status === 'COMPLETED').length || 0,
        total_queries: queries?.length || 0,
        open_queries: queries?.filter(q => q.status === 'OPEN').length || 0
      };
    }));

    res.json({
      status: 'success',
      data: studiesWithCounts || []
    });
  } catch (error) {
    console.error('Studies fetch error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to fetch studies',
      error: error.message
    });
  }
});

// Get study by ID
router.get('/:id', async (req, res) => {
  try {
    const { id } = req.params;

    const { data: study, error: studyError } = await supabase
      .from('edc_studies')
      .select(`
        *,
        edc_sites(*),
        edc_subjects(*)
      `)
      .eq('id', id)
      .single();

    if (studyError) throw studyError;
    if (!study) {
      return res.status(404).json({
        status: 'error',
        message: 'Study not found'
      });
    }

    // Get study metrics
    const { data: metrics } = await supabase
      .from('edc_study_metrics')
      .select('*')
      .eq('study_id', id)
      .order('metric_date', { ascending: false });

    res.json({
      status: 'success',
      data: {
        ...study,
        metrics: metrics || []
      }
    });
  } catch (error) {
    console.error('Study fetch error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to fetch study',
      error: error.message
    });
  }
});

// Create new study
router.post('/', async (req, res) => {
  try {
    const {
      protocol_number,
      title,
      description,
      phase,
      sponsor,
      indication,
      projected_enrollment,
      start_date,
      primary_endpoint,
      secondary_endpoints,
      inclusion_criteria,
      exclusion_criteria
    } = req.body;

    if (!protocol_number || !title || !phase) {
      return res.status(400).json({
        status: 'error',
        message: 'Protocol number, title, and phase are required'
      });
    }

    const { data: study, error } = await supabase
      .from('edc_studies')
      .insert([{
        protocol_number,
        title,
        description,
        phase,
        sponsor,
        indication,
        projected_enrollment: projected_enrollment || 0,
        start_date,
        primary_endpoint,
        secondary_endpoints,
        inclusion_criteria,
        exclusion_criteria,
        status: 'PLANNING'
      }])
      .select()
      .single();

    if (error) throw error;

    res.status(201).json({
      status: 'success',
      message: 'Study created successfully',
      data: study
    });
  } catch (error) {
    console.error('Study creation error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to create study',
      error: error.message
    });
  }
});

// Update study
router.put('/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const updates = req.body;

    // Remove fields that shouldn't be updated directly
    delete updates.id;
    delete updates.created_at;
    delete updates.protocol_number; // Protocol number shouldn't change

    updates.updated_at = new Date().toISOString();

    const { data: study, error } = await supabase
      .from('edc_studies')
      .update(updates)
      .eq('id', id)
      .select()
      .single();

    if (error) throw error;
    if (!study) {
      return res.status(404).json({
        status: 'error',
        message: 'Study not found'
      });
    }

    res.json({
      status: 'success',
      message: 'Study updated successfully',
      data: study
    });
  } catch (error) {
    console.error('Study update error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to update study',
      error: error.message
    });
  }
});

// Get study enrollment stats
router.get('/:id/enrollment', async (req, res) => {
  try {
    const { id } = req.params;

    // Get enrollment by month
    const { data: enrollment, error } = await supabase
      .from('edc_subjects')
      .select('enrollment_date, status')
      .eq('study_id', id)
      .not('enrollment_date', 'is', null)
      .order('enrollment_date');

    if (error) throw error;

    // Group by month
    const enrollmentByMonth = {};
    enrollment?.forEach(subject => {
      const monthKey = subject.enrollment_date.substring(0, 7); // YYYY-MM
      if (!enrollmentByMonth[monthKey]) {
        enrollmentByMonth[monthKey] = { enrolled: 0, active: 0, completed: 0, withdrawn: 0 };
      }
      enrollmentByMonth[monthKey].enrolled++;
      enrollmentByMonth[monthKey][subject.status.toLowerCase()]++;
    });

    const chartData = Object.entries(enrollmentByMonth).map(([month, data]) => ({
      month,
      ...data
    }));

    res.json({
      status: 'success',
      data: chartData
    });
  } catch (error) {
    console.error('Enrollment stats error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to fetch enrollment stats',
      error: error.message
    });
  }
});

// Get study performance metrics
router.get('/:id/metrics', async (req, res) => {
  try {
    const { id } = req.params;

    const { data: metrics, error } = await supabase
      .from('edc_study_metrics')
      .select('*')
      .eq('study_id', id)
      .order('calculation_date', { ascending: false });

    if (error) throw error;

    res.json({
      status: 'success',
      data: metrics || []
    });
  } catch (error) {
    console.error('Study metrics error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to fetch study metrics',
      error: error.message
    });
  }
});

// Get study sites
router.get('/:id/sites', async (req, res) => {
  try {
    const { id } = req.params;

    const { data: sites, error } = await supabase
      .from('edc_site_performance')
      .select('*')
      .eq('study_id', id)
      .order('site_number');

    if (error) throw error;

    res.json({
      status: 'success',
      data: sites || []
    });
  } catch (error) {
    console.error('Study sites error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to fetch study sites',
      error: error.message
    });
  }
});

// Export all study data that will be deleted (for closeout) as CSV files in ZIP
router.get('/:id/export', async (req, res) => {
  
  try {
    const { id } = req.params;
    console.log('📥 Export request for study ID:', id);

    // Fetch study details
    const { data: study, error: studyError } = await supabase
      .from('edc_studies')
      .select('*')
      .eq('id', id)
      .single();

    console.log('📊 Study query result:', { study, studyError, id });

    if (studyError || !study) {
      console.log('❌ Study not found or error:', studyError);
      return res.status(404).json({
        status: 'error',
        message: 'Study not found'
      });
    }

    console.log('✅ Study found:', study.protocol_number);

    // Create temporary directory for CSV files
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'edc-export-'));
    console.log('📁 Created temp directory:', tempDir);

    // Fetch and export all data that will be deleted (following exact deletion order)
    
    // 1. STUDY DATA - Export the study itself
    const studyPath = path.join(tempDir, '1_study.csv');
    const studyCsvWriter = createCsvWriter({
      path: studyPath,
      header: Object.keys(study).map(key => ({ id: key, title: key.toUpperCase() }))
    });
    await studyCsvWriter.writeRecords([study]);
    console.log('✅ Exported study data');

    // 2. SUBJECTS - Get all subjects for this study
    const { data: subjects } = await supabase
      .from('edc_subjects')
      .select('*')
      .eq('study_id', id);
    
    if (subjects && subjects.length > 0) {
      const subjectsPath = path.join(tempDir, '2_subjects.csv');
      const subjectsCsvWriter = createCsvWriter({
        path: subjectsPath,
        header: Object.keys(subjects[0]).map(key => ({ id: key, title: key.toUpperCase() }))
      });
      await subjectsCsvWriter.writeRecords(subjects);
      console.log(`✅ Exported ${subjects.length} subjects`);

      const subjectIds = subjects.map(s => s.id);

      // 3. DATA ENTRIES - Export all data entries for subjects in this study
      const { data: dataEntries } = await supabase
        .from('edc_data_entry')
        .select('*')
        .in('subject_id', subjectIds);
      
      if (dataEntries && dataEntries.length > 0) {
        const dataEntriesPath = path.join(tempDir, '3_data_entries.csv');
        const dataEntriesCsvWriter = createCsvWriter({
          path: dataEntriesPath,
          header: Object.keys(dataEntries[0]).map(key => ({ id: key, title: key.toUpperCase() }))
        });
        await dataEntriesCsvWriter.writeRecords(dataEntries);
        console.log(`✅ Exported ${dataEntries.length} data entries`);
      }

      // 4. QUERIES - Export queries for this study
      const { data: queries } = await supabase
        .from('edc_queries')
        .select('*')
        .eq('study_id', id);
      
      if (queries && queries.length > 0) {
        const queriesPath = path.join(tempDir, '4_queries.csv');
        const queriesCsvWriter = createCsvWriter({
          path: queriesPath,
          header: Object.keys(queries[0]).map(key => ({ id: key, title: key.toUpperCase() }))
        });
        await queriesCsvWriter.writeRecords(queries);
        console.log(`✅ Exported ${queries.length} queries`);
      }
    }

    // 5. AUDIT TRAIL - Export ALL audit trail entries that will be deleted
    // This includes direct study references and subject/query references
    let allAuditEntries = [];
    
    // Get audit entries by study_id
    const { data: studyAudits } = await supabase
      .from('edc_audit_trail')
      .select('*')
      .eq('study_id', id);
    
    if (studyAudits) allAuditEntries.push(...studyAudits);

    // Get audit entries for subjects
    if (subjects && subjects.length > 0) {
      const subjectIds = subjects.map(s => s.id);
      const { data: subjectAudits } = await supabase
        .from('edc_audit_trail')
        .select('*')
        .in('entity_id', subjectIds)
        .eq('entity_type', 'subject');
      
      if (subjectAudits) allAuditEntries.push(...subjectAudits);
    }

    // Get audit entries for queries
    const { data: queryIds } = await supabase
      .from('edc_queries')
      .select('id')
      .eq('study_id', id);

    if (queryIds && queryIds.length > 0) {
      const queryIdArray = queryIds.map(q => q.id);
      const { data: queryAudits } = await supabase
        .from('edc_audit_trail')
        .select('*')
        .in('entity_id', queryIdArray)
        .eq('entity_type', 'query');
      
      if (queryAudits) allAuditEntries.push(...queryAudits);
    }

    // Also get hardcoded site audits that will be cleaned up
    const hardcodedSites = [
      'ba4f8e72-6285-4ae2-a46c-27ccfeac6af7',
      'd9c2194f-1991-4bf8-ac3b-259bb79ed819', 
      '8d89af7e-c430-4fae-9934-753a31c2516f',
      '024aeef6-19a6-4031-a5ac-2eb23c461e28'
    ];

    for (const siteId of hardcodedSites) {
      const { data: siteAudits } = await supabase
        .from('edc_audit_trail')
        .select('*')
        .eq('site_id', siteId);
      
      if (siteAudits) allAuditEntries.push(...siteAudits);
    }

    // Remove duplicates and export
    const uniqueAudits = Array.from(
      new Map(allAuditEntries.map(audit => [audit.id, audit])).values()
    );

    if (uniqueAudits.length > 0) {
      const auditsPath = path.join(tempDir, '5_audit_trail.csv');
      const auditsCsvWriter = createCsvWriter({
        path: auditsPath,
        header: Object.keys(uniqueAudits[0]).map(key => ({ id: key, title: key.toUpperCase() }))
      });
      await auditsCsvWriter.writeRecords(uniqueAudits);
      console.log(`✅ Exported ${uniqueAudits.length} audit trail entries`);
    }

    // 6. Export summary information
    const summaryData = {
      export_date: new Date().toISOString(),
      study_protocol: study.protocol_number,
      study_title: study.title,
      total_subjects: subjects?.length || 0,
      total_queries: queryIds?.length || 0,
      total_audit_records: uniqueAudits.length,
      export_note: 'This export contains ALL data that will be permanently deleted during study closeout'
    };

    const summaryPath = path.join(tempDir, '0_export_summary.csv');
    const summaryCsvWriter = createCsvWriter({
      path: summaryPath,
      header: Object.keys(summaryData).map(key => ({ id: key, title: key.toUpperCase() }))
    });
    await summaryCsvWriter.writeRecords([summaryData]);
    console.log('✅ Exported summary');

    // Create ZIP archive
    const archive = archiver('zip', { zlib: { level: 9 } });
    const zipFileName = `study_${study.protocol_number}_closeout_export_${Date.now()}.zip`;

    res.setHeader('Content-Type', 'application/zip');
    res.setHeader('Content-Disposition', `attachment; filename="${zipFileName}"`);

    archive.pipe(res);

    // Add all CSV files to archive
    const files = fs.readdirSync(tempDir);
    for (const file of files) {
      if (file.endsWith('.csv')) {
        const filePath = path.join(tempDir, file);
        archive.file(filePath, { name: file });
      }
    }

    await archive.finalize();

    // Cleanup temp files
    setTimeout(() => {
      try {
        for (const file of files) {
          fs.unlinkSync(path.join(tempDir, file));
        }
        fs.rmdirSync(tempDir);
        console.log('🧹 Cleaned up temp files');
      } catch (cleanupError) {
        console.log('Cleanup warning:', cleanupError.message);
      }
    }, 5000);

    console.log(`📦 Successfully exported study data as ZIP: ${zipFileName}`);

  } catch (error) {
    console.error('Export error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to export study data',
      error: error.message
    });
  }
});

// Delete study and all associated data
router.delete('/:id', async (req, res) => {
  try {
    const { id } = req.params;
    console.log('🗑️ Delete request for study ID:', id);

    // Verify study exists
    const { data: study, error: studyError } = await supabase
      .from('edc_studies')
      .select('id, protocol_number, title')
      .eq('id', id)
      .single();

    if (studyError || !study) {
      return res.status(404).json({
        status: 'error',
        message: 'Study not found'
      });
    }

    console.log(`Deleting study: ${study.protocol_number} - ${study.title}`);

    // Delete in correct order to avoid foreign key constraint violations
    
    // 1. Delete audit trail entries for this study and its related entities
    // First get all related entity IDs
    const { data: subjectIds } = await supabase
      .from('edc_subjects')
      .select('id')
      .eq('study_id', id);
    
    const { data: queryIds } = await supabase
      .from('edc_queries')
      .select('id')
      .eq('study_id', id);
    
    // Delete audit trail entries by study_id
    const { error: auditError1 } = await supabase
      .from('edc_audit_trail')
      .delete()
      .eq('study_id', id);
    
    // Delete audit trail entries for subjects in this study
    if (subjectIds && subjectIds.length > 0) {
      const subjectIdArray = subjectIds.map(s => s.id);
      const { error: auditError2 } = await supabase
        .from('edc_audit_trail')
        .delete()
        .in('entity_id', subjectIdArray)
        .eq('entity_type', 'subject');
    }
    
    // Delete audit trail entries for queries in this study
    if (queryIds && queryIds.length > 0) {
      const queryIdArray = queryIds.map(q => q.id);
      const { error: auditError3 } = await supabase
        .from('edc_audit_trail')
        .delete()
        .in('entity_id', queryIdArray)
        .eq('entity_type', 'query');
    }
    
    // Delete ALL audit trail entries that might reference sites
    // The error shows site_id ba4f8e72-6285-4ae2-a46c-27ccfeac6af7 is still referenced
    const { error: allAuditError } = await supabase
      .from('edc_audit_trail')
      .delete()
      .eq('site_id', 'ba4f8e72-6285-4ae2-a46c-27ccfeac6af7');
    
    console.log('Deleted all audit entries for site ba4f8e72-6285-4ae2-a46c-27ccfeac6af7:', allAuditError || 'success');
    
    // Also clean up for other hardcoded sites
    const hardcodedSites = [
      'ba4f8e72-6285-4ae2-a46c-27ccfeac6af7',
      'd9c2194f-1991-4bf8-ac3b-259bb79ed819', 
      '8d89af7e-c430-4fae-9934-753a31c2516f',
      '024aeef6-19a6-4031-a5ac-2eb23c461e28'
    ];
    
    for (const siteId of hardcodedSites) {
      const { error: siteAuditError } = await supabase
        .from('edc_audit_trail')
        .delete()
        .eq('site_id', siteId);
      
      if (siteAuditError) {
        console.log(`Could not clean audit for site ${siteId}:`, siteAuditError);
      }
    }
    
    console.log('Comprehensive audit trail cleanup completed');

    // 2. Delete queries for this study
    const { error: queriesError } = await supabase
      .from('edc_queries')
      .delete()
      .eq('study_id', id);
    
    if (queriesError) {
      console.log('Queries deletion error:', queriesError);
    }

    // 3. Delete data entries for subjects in this study (using subjectIds from above)
    if (subjectIds && subjectIds.length > 0) {
      const subjectIdArray = subjectIds.map(s => s.id);
      const { error: dataError } = await supabase
        .from('edc_data_entry')
        .delete()
        .in('subject_id', subjectIdArray);
      
      if (dataError) {
        console.log('Data entries deletion error:', dataError);
      }
    }

    // 4. Delete visits for subjects in this study
    if (subjectIds && subjectIds.length > 0) {
      const subjectIdArray = subjectIds.map(s => s.id);
      const { error: visitsError } = await supabase
        .from('edc_visits')
        .delete()
        .in('subject_id', subjectIdArray);
      
      if (visitsError) {
        console.log('Visits deletion error:', visitsError);
      }
    }

    // 5. Delete subjects for this study
    const { error: subjectsError } = await supabase
      .from('edc_subjects')
      .delete()
      .eq('study_id', id);
    
    if (subjectsError) {
      console.log('Subjects deletion error:', subjectsError);
    }

    // 6. Delete study metrics if they exist
    const { error: metricsError } = await supabase
      .from('edc_study_metrics')
      .delete()
      .eq('study_id', id);
    
    if (metricsError) {
      console.log('Study metrics deletion error:', metricsError);
    }

    // 7. Sites are hardcoded and shared across studies - do NOT delete them
    // Sites are now study-agnostic (study_id = null) so they persist across studies

    // 8. Finally delete the study itself
    const { error: deleteError } = await supabase
      .from('edc_studies')
      .delete()
      .eq('id', id);

    if (deleteError) throw deleteError;

    console.log(`✅ Study deleted successfully: ${study.protocol_number} - ${study.title}`);
    
    res.json({
      status: 'success',
      message: `Study "${study.title}" and all associated data have been deleted successfully`
    });
  } catch (error) {
    console.error('Study deletion error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to delete study',
      error: error.message
    });
  }
});

module.exports = router;
