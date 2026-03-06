const express = require('express');
const router = express.Router();
const supabase = require('../config/supabase');

// Get all forms with optional study filter
router.get('/', async (req, res) => {
  try {
    const { study_id } = req.query;
    
    let query = supabase
      .from('edc_form_templates')
      .select('*')
      .order('created_at', { ascending: false });
    
    // Add study filter if provided
    if (study_id) {
      query = query.eq('study_id', study_id);
    }
    
    const { data: forms, error } = await query;

    if (error) {
      console.error('Forms fetch error:', error);
      return res.status(500).json({
        status: 'error',
        message: 'Failed to fetch forms',
        error: error.message
      });
    }

    res.json({
      status: 'success',
      data: forms || []
    });

  } catch (error) {
    console.error('Forms fetch error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to fetch forms',
      error: error.message
    });
  }
});

// Save a form template - complete version with form structure
router.post('/save', async (req, res) => {
  try {
    const { name, version, status, sections } = req.body;

    if (!name || !sections) {
      return res.status(400).json({
        status: 'error',
        message: 'Form name and sections are required'
      });
    }

    // Prepare complete form structure for storage
    const formStructure = {
      sections,
      fieldCount: sections.reduce((acc, s) => acc + s.fields.length, 0),
      requiredFields: sections.reduce((acc, s) => acc + s.fields.filter(f => f.required).length, 0),
      cdiscCompliant: sections.some(s => s.fields.some(f => f.cdiscMapping)),
      createdDate: new Date().toISOString(),
      modifiedDate: new Date().toISOString()
    };

    // Save to edc_form_templates table
    const { data: formTemplate, error: formError } = await supabase
      .from('edc_form_templates')
      .insert([{
        name,
        description: `Clinical form with ${sections.length} sections and ${formStructure.fieldCount} fields`,
        category: 'Clinical',
        version: version || '1.0.0',
        status: status || 'DRAFT',
        form_structure: formStructure
      }])
      .select()
      .single();

    if (formError) {
      console.error('Form save error:', formError);
      // Try updating if it exists (find by name and version)
      const { data: existingForm, error: findError } = await supabase
        .from('edc_form_templates')
        .select('*')
        .eq('name', name)
        .eq('version', version || '1.0.0')
        .single();
        
      if (existingForm && !findError) {
        // Update existing form
        const { data: updatedForm, error: updateError } = await supabase
          .from('edc_form_templates')
          .update({
            description: `Clinical form with ${sections.length} sections and ${formStructure.fieldCount} fields`,
            status: status || 'DRAFT',
            form_structure: formStructure,
            updated_at: new Date().toISOString()
          })
          .eq('id', existingForm.id)
          .select()
          .single();
          
        if (!updateError) {
          return res.json({
            status: 'success',
            message: 'Form updated successfully',
            data: {
              formId: existingForm.id,
              formName: name,
              version: version || '1.0.0',
              updatedAt: updatedForm.updated_at
            }
          });
        } else {
          console.error('Form update error:', updateError);
          return res.status(500).json({
            status: 'error',
            message: 'Failed to update form template',
            error: updateError.message
          });
        }
      }
      
      return res.status(500).json({
        status: 'error',
        message: 'Failed to save form template',
        error: formError.message
      });
    }

    console.log('Form template saved successfully:', formTemplate.id);

    res.json({
      status: 'success',
      message: 'Form saved successfully',
      data: {
        formId: formTemplate.id,
        formName: formTemplate.name,
        version: formTemplate.version,
        createdAt: formTemplate.created_at
      }
    });

  } catch (error) {
    console.error('Form save error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to save form',
      error: error.message
    });
  }
});

// Get all saved forms
router.get('/list', async (req, res) => {
  try {
    const { data: forms, error } = await supabase
      .from('edc_form_templates')
      .select('*')
      .order('created_at', { ascending: false });

    if (error) {
      console.error('Form list error:', error);
      return res.status(500).json({
        status: 'error',
        message: 'Failed to retrieve forms',
        error: error.message
      });
    }

    // Map database statuses back to frontend statuses
    const statusMapping = {
      'DRAFT': 'DRAFT',
      'ACTIVE': 'UAT_TESTING',
      'RETIRED': 'PRODUCTION'
    };

    const mappedForms = (forms || []).map(form => ({
      ...form,
      status: statusMapping[form.status] || form.status
    }));

    res.json({
      status: 'success',
      data: mappedForms
    });

  } catch (error) {
    console.error('Form list error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to retrieve forms',
      error: error.message
    });
  }
});

// Promote form status
router.put('/:formId/promote', async (req, res) => {
  try {
    const { formId } = req.params;
    const { newStatus } = req.body;

    if (!newStatus) {
      return res.status(400).json({
        status: 'error',
        message: 'New status is required'
      });
    }

    // Map frontend statuses to database-compatible statuses
    const statusMapping = {
      'DRAFT': 'DRAFT',
      'UAT_TESTING': 'ACTIVE',
      'PRODUCTION': 'RETIRED'
    };

    // Validate status transition
    const validStatuses = ['DRAFT', 'UAT_TESTING', 'PRODUCTION'];
    if (!validStatuses.includes(newStatus)) {
      return res.status(400).json({
        status: 'error',
        message: 'Invalid status'
      });
    }

    const dbStatus = statusMapping[newStatus];

    const { data: updatedForm, error } = await supabase
      .from('edc_form_templates')
      .update({
        status: dbStatus,
        updated_at: new Date().toISOString()
      })
      .eq('id', formId)
      .select()
      .single();

    if (error) {
      console.error('Form status promotion error:', error);
      return res.status(500).json({
        status: 'error',
        message: 'Failed to promote form status',
        error: error.message
      });
    }

    res.json({
      status: 'success',
      message: `Form promoted to ${newStatus} successfully`,
      data: updatedForm
    });

  } catch (error) {
    console.error('Form status promotion error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to promote form status',
      error: error.message
    });
  }
});

// Delete a form
router.delete('/:formId', async (req, res) => {
  try {
    const { formId } = req.params;

    const { error } = await supabase
      .from('edc_form_templates')
      .delete()
      .eq('id', formId);

    if (error) {
      console.error('Form delete error:', error);
      return res.status(500).json({
        status: 'error',
        message: 'Failed to delete form',
        error: error.message
      });
    }

    res.json({
      status: 'success',
      message: 'Form deleted successfully'
    });

  } catch (error) {
    console.error('Form delete error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to delete form',
      error: error.message
    });
  }
});

// Get forms assigned to a study (using a simple JSON storage in form_structure)
router.get('/study/:studyId/assignments', async (req, res) => {
  try {
    const { studyId } = req.params;
    
    // Get all forms (not just PRODUCTION status ones)
    const { data: forms, error } = await supabase
      .from('edc_form_templates')
      .select('*');

    if (error) throw error;

    // Filter forms that have this study in their assignedStudies array
    const assignedForms = forms?.filter(form => {
      const structure = form.form_structure || {};
      const assignedStudies = structure.assignedStudies || [];
      return assignedStudies.includes(studyId);
    }) || [];

    res.json({
      status: 'success',
      data: assignedForms
    });
  } catch (error) {
    console.error('Form assignments fetch error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to fetch form assignments',
      error: error.message
    });
  }
});

// Assign a form to a study
router.post('/study/:studyId/assign', async (req, res) => {
  try {
    const { studyId } = req.params;
    const { formId } = req.body;

    if (!formId) {
      return res.status(400).json({
        status: 'error',
        message: 'Form ID is required'
      });
    }

    // Get the current form
    const { data: form, error: fetchError } = await supabase
      .from('edc_form_templates')
      .select('*')
      .eq('id', formId)
      .single();

    if (fetchError || !form) {
      return res.status(404).json({
        status: 'error',
        message: 'Form not found'
      });
    }

    // Update form_structure to include assigned studies
    const currentStructure = form.form_structure || {};
    const assignedStudies = currentStructure.assignedStudies || [];
    
    if (!assignedStudies.includes(studyId)) {
      assignedStudies.push(studyId);
      currentStructure.assignedStudies = assignedStudies;
      currentStructure.assignmentDates = currentStructure.assignmentDates || {};
      currentStructure.assignmentDates[studyId] = new Date().toISOString();

      const { data: updatedForm, error: updateError } = await supabase
        .from('edc_form_templates')
        .update({ 
          form_structure: currentStructure,
          updated_at: new Date().toISOString()
        })
        .eq('id', formId)
        .select()
        .single();

      if (updateError) throw updateError;

      res.json({
        status: 'success',
        message: 'Form assigned to study successfully',
        data: updatedForm
      });
    } else {
      res.json({
        status: 'success',
        message: 'Form is already assigned to this study',
        data: form
      });
    }
  } catch (error) {
    console.error('Form assignment error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to assign form to study',
      error: error.message
    });
  }
});

// Unassign a form from a study
router.delete('/study/:studyId/assign/:formId', async (req, res) => {
  try {
    const { studyId, formId } = req.params;

    // Get the current form
    const { data: form, error: fetchError } = await supabase
      .from('edc_form_templates')
      .select('*')
      .eq('id', formId)
      .single();

    if (fetchError || !form) {
      return res.status(404).json({
        status: 'error',
        message: 'Form not found'
      });
    }

    // Update form_structure to remove assigned study
    const currentStructure = form.form_structure || {};
    const assignedStudies = currentStructure.assignedStudies || [];
    
    const updatedStudies = assignedStudies.filter(id => id !== studyId);
    currentStructure.assignedStudies = updatedStudies;

    const { data: updatedForm, error: updateError } = await supabase
      .from('edc_form_templates')
      .update({ 
        form_structure: currentStructure,
        updated_at: new Date().toISOString()
      })
      .eq('id', formId)
      .select()
      .single();

    if (updateError) throw updateError;

    res.json({
      status: 'success',
      message: 'Form unassigned from study successfully',
      data: updatedForm
    });
  } catch (error) {
    console.error('Form unassignment error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to unassign form from study',
      error: error.message
    });
  }
});

module.exports = router;