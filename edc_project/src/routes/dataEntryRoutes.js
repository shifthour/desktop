const express = require('express');
const supabase = require('../config/supabase');
const router = express.Router();

// Save form data as draft
router.post('/save-draft', async (req, res) => {
  try {
    const { subjectId, visitId, formId, formData } = req.body;

    if (!subjectId || !formId || !formData) {
      return res.status(400).json({
        status: 'error',
        message: 'Missing required fields: subjectId, formId, formData'
      });
    }

    // Get study_id and site_id from subject
    const { data: subjectInfo } = await supabase
      .from('edc_subjects')
      .select('study_id, site_id')
      .eq('id', subjectId)
      .single();

    if (!subjectInfo) {
      return res.status(400).json({
        status: 'error',
        message: 'Subject not found'
      });
    }

    // Check if form data already exists
    let existingData = null;
    let query = supabase
      .from('edc_form_data')
      .select('id')
      .eq('subject_id', subjectId)
      .eq('form_id', formId);

    if (visitId) {
      query = query.eq('visit_id', visitId);
    } else {
      query = query.is('visit_id', null);
    }

    const { data: existing } = await query.single();

    let data, error;
    
    if (existing) {
      // Update existing record
      ({ data, error } = await supabase
        .from('edc_form_data')
        .update({
          form_data: formData,
          status: 'DRAFT'
        })
        .eq('id', existing.id)
        .select());
    } else {
      // Insert new record
      ({ data, error } = await supabase
        .from('edc_form_data')
        .insert({
          study_id: subjectInfo.study_id,
          site_id: subjectInfo.site_id,
          subject_id: subjectId,
          visit_id: visitId || null,
          form_id: formId,
          form_data: formData,
          status: 'DRAFT',
          data_entry_start: new Date().toISOString()
        })
        .select());
    }

    if (error) {
      console.error('Error saving draft:', error);
      return res.status(500).json({
        status: 'error',
        message: 'Failed to save draft',
        error: error.message
      });
    }

    res.json({
      status: 'success',
      message: 'Draft saved successfully',
      data: data
    });

  } catch (error) {
    console.error('Error in save-draft:', error);
    res.status(500).json({
      status: 'error',
      message: 'Internal server error'
    });
  }
});

// Submit form for review
router.post('/submit-for-review', async (req, res) => {
  try {
    const { subjectId, visitId, formId, formData } = req.body;

    if (!subjectId || !formId || !formData) {
      return res.status(400).json({
        status: 'error',
        message: 'Missing required fields: subjectId, formId, formData'
      });
    }

    // Get study_id and site_id from subject
    const { data: subjectInfo } = await supabase
      .from('edc_subjects')
      .select('study_id, site_id')
      .eq('id', subjectId)
      .single();

    if (!subjectInfo) {
      return res.status(400).json({
        status: 'error',
        message: 'Subject not found'
      });
    }

    // Check if form is already submitted for review (COMPLETED status)
    let query = supabase
      .from('edc_form_data')
      .select('id, status')
      .eq('subject_id', subjectId)
      .eq('form_id', formId);

    if (visitId) {
      query = query.eq('visit_id', visitId);
    } else {
      query = query.is('visit_id', null);
    }

    const { data: existingSubmission } = await query.single();

    // Prevent duplicate submissions - only allow if no previous submission or if it was rejected
    if (existingSubmission && (existingSubmission.status === 'COMPLETED' || existingSubmission.status === 'SIGNED')) {
      return res.status(400).json({
        status: 'error',
        message: 'Form has already been submitted for review. Cannot submit again unless rejected by reviewer.'
      });
    }

    let data, error;

    if (existingSubmission && existingSubmission.status === 'REJECTED') {
      // Update rejected form to resubmit
      ({ data, error } = await supabase
        .from('edc_form_data')
        .update({
          form_data: formData,
          status: 'COMPLETED',
          data_entry_complete: new Date().toISOString()
        })
        .eq('id', existingSubmission.id)
        .select());
    } else {
      // Insert new submission
      ({ data, error } = await supabase
        .from('edc_form_data')
        .insert({
          study_id: subjectInfo.study_id,
          site_id: subjectInfo.site_id,
          subject_id: subjectId,
          visit_id: visitId || null,
          form_id: formId,
          form_data: formData,
          status: 'COMPLETED',
          data_entry_complete: new Date().toISOString()
        })
        .select());
    }

    if (error) {
      console.error('Error submitting for review:', error);
      return res.status(500).json({
        status: 'error',
        message: 'Failed to submit for review',
        error: error.message
      });
    }

    res.json({
      status: 'success',
      message: 'Form submitted for review successfully',
      data: data
    });

  } catch (error) {
    console.error('Error in submit-for-review:', error);
    res.status(500).json({
      status: 'error',
      message: 'Internal server error'
    });
  }
});

// Sign and lock form
router.post('/sign-and-lock', async (req, res) => {
  try {
    const { subjectId, visitId, formId, formData, signature } = req.body;

    if (!subjectId || !formId || !formData) {
      return res.status(400).json({
        status: 'error',
        message: 'Missing required fields: subjectId, formId, formData'
      });
    }

    // Get study_id and site_id from subject
    const { data: subjectInfo } = await supabase
      .from('edc_subjects')
      .select('study_id, site_id')
      .eq('id', subjectId)
      .single();

    if (!subjectInfo) {
      return res.status(400).json({
        status: 'error',
        message: 'Subject not found'
      });
    }

    // Insert or update form data with SIGNED status (equivalent to locked)
    const { data, error } = await supabase
      .from('edc_form_data')
      .insert({
        study_id: subjectInfo.study_id,
        site_id: subjectInfo.site_id,
        subject_id: subjectId,
        visit_id: visitId || null,
        form_id: formId,
        form_data: formData,
        status: 'SIGNED',
        data_entry_complete: new Date().toISOString()
      })
      .select();

    if (error) {
      console.error('Error signing and locking:', error);
      return res.status(500).json({
        status: 'error',
        message: 'Failed to sign and lock form',
        error: error.message
      });
    }

    res.json({
      status: 'success',
      message: 'Form signed and locked successfully',
      data: data
    });

  } catch (error) {
    console.error('Error in sign-and-lock:', error);
    res.status(500).json({
      status: 'error',
      message: 'Internal server error'
    });
  }
});

// Get form data for a specific subject/visit/form
router.get('/form-data', async (req, res) => {
  try {
    const { subjectId, visitId, formId } = req.query;

    if (!subjectId || !formId) {
      return res.status(400).json({
        status: 'error',
        message: 'Missing required parameters: subjectId, formId'
      });
    }

    let query = supabase
      .from('edc_form_data')
      .select('*')
      .eq('subject_id', subjectId)
      .eq('form_id', formId);

    if (visitId) {
      query = query.eq('visit_id', visitId);
    } else {
      query = query.is('visit_id', null);
    }

    // Get the most recent form data (could be DRAFT, COMPLETED, or SIGNED)
    const { data, error } = await query
      .order('updated_at', { ascending: false })
      .limit(1)
      .single();

    if (error && error.code !== 'PGRST116') { // PGRST116 is "not found"
      console.error('Error fetching form data:', error);
      return res.status(500).json({
        status: 'error',
        message: 'Failed to fetch form data',
        error: error.message
      });
    }

    res.json({
      status: 'success',
      data: data || null
    });

  } catch (error) {
    console.error('Error in get form-data:', error);
    res.status(500).json({
      status: 'error',
      message: 'Internal server error'
    });
  }
});

// Get forms pending review for a subject  
router.get('/pending-review/:subjectId', async (req, res) => {
  try {
    const { subjectId } = req.params;

    if (!subjectId) {
      return res.status(400).json({
        status: 'error',
        message: 'Subject ID is required'
      });
    }

    // Get all forms with COMPLETED status (pending review)
    const { data: pendingForms, error } = await supabase
      .from('edc_form_data')
      .select('*')
      .eq('subject_id', subjectId)
      .eq('status', 'COMPLETED')
      .order('updated_at', { ascending: false });

    if (error) {
      console.error('Error fetching pending review forms:', error);
      return res.status(500).json({
        status: 'error',
        message: 'Failed to fetch forms pending review',
        error: error.message
      });
    }

    // Enrich forms with form information
    if (pendingForms && pendingForms.length > 0) {
      for (let form of pendingForms) {
        // Get form details from edc_form_templates
        const { data: formData } = await supabase
          .from('edc_form_templates')
          .select('name, version')
          .eq('id', form.form_id)
          .single();
        
        if (formData) {
          form.edc_forms = formData;
        }
      }
    }

    res.json({
      status: 'success',
      data: pendingForms || []
    });

  } catch (error) {
    console.error('Error in get pending review forms:', error);
    res.status(500).json({
      status: 'error',
      message: 'Internal server error'
    });
  }
});

// Get draft forms for a subject
router.get('/drafts/:subjectId', async (req, res) => {
  try {
    const { subjectId } = req.params;

    if (!subjectId) {
      return res.status(400).json({
        status: 'error',
        message: 'Subject ID is required'
      });
    }

    // Get all draft form data for this subject (without joins first to test)
    const { data: drafts, error } = await supabase
      .from('edc_form_data')
      .select('*')
      .eq('subject_id', subjectId)
      .eq('status', 'DRAFT')
      .order('updated_at', { ascending: false });

    if (error) {
      console.error('Error fetching drafts:', error);
      return res.status(500).json({
        status: 'error',
        message: 'Failed to fetch draft forms',
        error: error.message
      });
    }

    // Enrich drafts with form information
    if (drafts && drafts.length > 0) {
      for (let draft of drafts) {
        // Get form details from edc_form_templates
        const { data: formData } = await supabase
          .from('edc_form_templates')
          .select('name, version')
          .eq('id', draft.form_id)
          .single();
        
        if (formData) {
          draft.edc_forms = formData;
        }
      }
    }

    res.json({
      status: 'success',
      data: drafts || []
    });

  } catch (error) {
    console.error('Error in get drafts:', error);
    res.status(500).json({
      status: 'error',
      message: 'Internal server error'
    });
  }
});

// Reject a form (send back for revision)
router.post('/reject-form', async (req, res) => {
  try {
    const { formDataId, rejectionReason } = req.body;

    if (!formDataId) {
      return res.status(400).json({
        status: 'error',
        message: 'Form data ID is required'
      });
    }

    // Update the form status to REJECTED
    const { data, error } = await supabase
      .from('edc_form_data')
      .update({
        status: 'REJECTED',
        review_date: new Date().toISOString(),
        rejection_reason: rejectionReason || 'Form rejected by reviewer'
      })
      .eq('id', formDataId)
      .select();

    if (error) {
      console.error('Error rejecting form:', error);
      return res.status(500).json({
        status: 'error',
        message: 'Failed to reject form',
        error: error.message
      });
    }

    if (!data || data.length === 0) {
      return res.status(404).json({
        status: 'error',
        message: 'Form not found'
      });
    }

    res.json({
      status: 'success',
      message: 'Form rejected successfully. User can now resubmit after making changes.',
      data: data[0]
    });

  } catch (error) {
    console.error('Error in reject-form:', error);
    res.status(500).json({
      status: 'error',
      message: 'Internal server error'
    });
  }
});

// Approve a form (change status from COMPLETED to APPROVED)
router.post('/approve-form', async (req, res) => {
  try {
    const { formDataId, reviewerId } = req.body;

    if (!formDataId) {
      return res.status(400).json({
        status: 'error',
        message: 'Form data ID is required'
      });
    }

    // Update the form status to SIGNED (approved forms are ready for final signature)
    const { data, error } = await supabase
      .from('edc_form_data')
      .update({
        status: 'SIGNED',
        review_date: new Date().toISOString(),
        data_entry_complete: new Date().toISOString()
        // Note: reviewed_by field expects UUID, skipping for now until user management is implemented
      })
      .eq('id', formDataId)
      .select();

    if (error) {
      console.error('Error approving form:', error);
      return res.status(500).json({
        status: 'error',
        message: 'Failed to approve form',
        error: error.message
      });
    }

    if (!data || data.length === 0) {
      return res.status(404).json({
        status: 'error',
        message: 'Form not found'
      });
    }

    res.json({
      status: 'success',
      message: 'Form approved successfully',
      data: data[0]
    });

  } catch (error) {
    console.error('Error in approve-form:', error);
    res.status(500).json({
      status: 'error',
      message: 'Internal server error'
    });
  }
});

// Get approved forms for a subject
router.get('/approved/:subjectId', async (req, res) => {
  try {
    const { subjectId } = req.params;

    if (!subjectId) {
      return res.status(400).json({
        status: 'error',
        message: 'Subject ID is required'
      });
    }

    // Get all forms with SIGNED status (these are approved and locked forms)
    const { data: approvedForms, error } = await supabase
      .from('edc_form_data')
      .select('*')
      .eq('subject_id', subjectId)
      .eq('status', 'SIGNED')
      .order('review_date', { ascending: false });

    if (error) {
      console.error('Error fetching approved forms:', error);
      return res.status(500).json({
        status: 'error',
        message: 'Failed to fetch approved forms',
        error: error.message
      });
    }

    // Enrich forms with form information
    if (approvedForms && approvedForms.length > 0) {
      for (let form of approvedForms) {
        // Get form details from edc_form_templates
        const { data: formData } = await supabase
          .from('edc_form_templates')
          .select('name, version')
          .eq('id', form.form_id)
          .single();
        
        if (formData) {
          form.edc_forms = formData;
        }
      }
    }

    res.json({
      status: 'success',
      data: approvedForms || []
    });

  } catch (error) {
    console.error('Error in get approved forms:', error);
    res.status(500).json({
      status: 'error',
      message: 'Internal server error'
    });
  }
});

// Get form completion stats for a study
router.get('/stats/:studyId', async (req, res) => {
  try {
    const { studyId } = req.params;

    if (!studyId) {
      return res.status(400).json({
        status: 'error',
        message: 'Study ID is required'
      });
    }

    // Get total forms assigned to this study (all statuses)
    const { data: assignedForms } = await supabase
      .from('edc_form_templates')
      .select('id, form_structure'); // Get all forms regardless of status

    // Filter forms assigned to this study
    const studyAssignedForms = assignedForms?.filter(form => {
      const structure = form.form_structure || {};
      const assignedStudies = structure.assignedStudies || [];
      return assignedStudies.includes(studyId);
    }) || [];

    const totalForms = studyAssignedForms.length;

    // Get completed forms count (SIGNED status) for this study
    const { data: completedFormsData } = await supabase
      .from('edc_form_data')
      .select('id')
      .eq('study_id', studyId)
      .eq('status', 'SIGNED');

    const formsCompleted = completedFormsData?.length || 0;

    // Get total subjects in study for additional stats
    const { data: subjectsData } = await supabase
      .from('edc_subjects')
      .select('id')
      .eq('study_id', studyId);

    const totalSubjects = subjectsData?.length || 0;

    // Get pending review forms count
    const { data: pendingFormsData } = await supabase
      .from('edc_form_data')
      .select('id')
      .eq('study_id', studyId)
      .eq('status', 'COMPLETED');

    const formsPendingReview = pendingFormsData?.length || 0;

    // Calculate visit completion rate
    // This is based on the percentage of subjects that have completed their assigned forms
    let visitCompletion = 0;
    if (totalSubjects > 0 && totalForms > 0) {
      // Expected total form instances = totalSubjects * totalForms (assuming each subject should fill each form)
      const expectedFormInstances = totalSubjects * totalForms;
      visitCompletion = expectedFormInstances > 0 ? Math.round((formsCompleted / expectedFormInstances) * 100) : 0;
    }

    res.json({
      status: 'success',
      data: {
        totalForms,
        formsCompleted,
        formsPendingReview,
        totalSubjects,
        completionRate: totalForms > 0 && totalSubjects > 0 ? Math.min(100, Math.round((formsCompleted / (totalForms * totalSubjects)) * 100)) : 0,
        visitCompletion: visitCompletion
      }
    });

  } catch (error) {
    console.error('Error getting form stats:', error);
    res.status(500).json({
      status: 'error',
      message: 'Internal server error'
    });
  }
});

module.exports = router;
