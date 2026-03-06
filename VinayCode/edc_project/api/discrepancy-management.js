const express = require('express');
const supabase = require('../src/config/supabase');
const { authenticateToken } = require('../src/middleware/auth');
const router = express.Router();

// Discrepancy severity levels
const SEVERITY_LEVELS = {
  CRITICAL: 'critical',
  MAJOR: 'major',
  MINOR: 'minor',
  NOTE: 'note'
};

// Discrepancy types
const DISCREPANCY_TYPES = {
  LEVEL1: 'pre_submission',  // Before first submission
  LEVEL2: 'post_submission'  // After submission
};

// Discrepancy statuses
const DISCREPANCY_STATUS = {
  OPEN: 'open',
  ASSIGNED: 'assigned',
  ANSWERED: 'answered',
  REVIEWED: 'reviewed',
  RESOLVED: 'resolved',
  CLOSED: 'closed',
  ACCEPTED: 'accepted_as_valid'
};

/**
 * Level 1: Pre-submission validation (inline checks)
 * These are real-time validations that occur during data entry
 */
router.post('/validate/inline', authenticateToken, async (req, res) => {
  try {
    const { formId, fieldId, value, formData } = req.body;
    const discrepancies = [];

    // Get field validation rules
    const { data: field, error } = await supabase
      .from('EDC_form_fields')
      .select('*')
      .eq('id', fieldId)
      .single();

    if (error || !field) {
      return res.status(404).json({ error: 'Field not found' });
    }

    // Datatype validation
    if (field.datatype === 'integer' && value && isNaN(parseInt(value))) {
      discrepancies.push({
        type: DISCREPANCY_TYPES.LEVEL1,
        severity: SEVERITY_LEVELS.CRITICAL,
        field: field.field_name,
        message: `Field "${field.field_label}" must be a valid integer`,
        value_entered: value,
        expected_type: 'integer'
      });
    }

    if (field.datatype === 'decimal' && value && isNaN(parseFloat(value))) {
      discrepancies.push({
        type: DISCREPANCY_TYPES.LEVEL1,
        severity: SEVERITY_LEVELS.CRITICAL,
        field: field.field_name,
        message: `Field "${field.field_label}" must be a valid decimal number`,
        value_entered: value,
        expected_type: 'decimal'
      });
    }

    // Range validation
    if (field.min_value !== null && parseFloat(value) < field.min_value) {
      discrepancies.push({
        type: DISCREPANCY_TYPES.LEVEL1,
        severity: SEVERITY_LEVELS.MAJOR,
        field: field.field_name,
        message: `Value must be at least ${field.min_value}`,
        value_entered: value,
        expected_range: `${field.min_value} - ${field.max_value || '∞'}`
      });
    }

    if (field.max_value !== null && parseFloat(value) > field.max_value) {
      discrepancies.push({
        type: DISCREPANCY_TYPES.LEVEL1,
        severity: SEVERITY_LEVELS.MAJOR,
        field: field.field_name,
        message: `Value must not exceed ${field.max_value}`,
        value_entered: value,
        expected_range: `${field.min_value || '0'} - ${field.max_value}`
      });
    }

    // Required field validation
    if (field.is_required && (!value || value.toString().trim() === '')) {
      discrepancies.push({
        type: DISCREPANCY_TYPES.LEVEL1,
        severity: SEVERITY_LEVELS.CRITICAL,
        field: field.field_name,
        message: `Field "${field.field_label}" is required`,
        value_entered: value,
        expected: 'non-empty value'
      });
    }

    // Pattern validation (regex)
    if (field.pattern && value && !new RegExp(field.pattern).test(value)) {
      discrepancies.push({
        type: DISCREPANCY_TYPES.LEVEL1,
        severity: SEVERITY_LEVELS.MAJOR,
        field: field.field_name,
        message: field.pattern_message || `Value does not match required pattern`,
        value_entered: value,
        expected_pattern: field.pattern
      });
    }

    // Cross-field validation (example: systolic > diastolic)
    if (field.field_name === 'bp_systolic' && formData.bp_diastolic) {
      const systolic = parseFloat(value);
      const diastolic = parseFloat(formData.bp_diastolic);
      if (systolic <= diastolic) {
        discrepancies.push({
          type: DISCREPANCY_TYPES.LEVEL1,
          severity: SEVERITY_LEVELS.CRITICAL,
          field: field.field_name,
          message: 'Systolic blood pressure must be greater than diastolic',
          value_entered: value,
          related_field: 'bp_diastolic',
          related_value: formData.bp_diastolic
        });
      }
    }

    // Temperature validation example (34-43°C for human body temperature)
    if (field.field_name === 'body_temperature') {
      const temp = parseFloat(value);
      if (temp < 34 || temp > 43) {
        discrepancies.push({
          type: DISCREPANCY_TYPES.LEVEL1,
          severity: SEVERITY_LEVELS.CRITICAL,
          field: field.field_name,
          message: 'Body temperature must be between 34°C and 43°C',
          value_entered: value,
          expected_range: '34-43°C'
        });
      }
    }

    res.json({
      valid: discrepancies.length === 0,
      discrepancies,
      canSave: !discrepancies.some(d => d.severity === SEVERITY_LEVELS.CRITICAL)
    });

  } catch (error) {
    console.error('Inline validation error:', error);
    res.status(500).json({ error: 'Validation failed' });
  }
});

/**
 * Level 2: Post-submission validation and query management
 * These are queries raised after data submission by Data Managers
 */
router.post('/validate/submission', authenticateToken, async (req, res) => {
  try {
    const { formDataId, subjectId, visitId, formId } = req.body;
    const discrepancies = [];

    // Get all form data for the submission
    const { data: formData, error } = await supabase
      .from('EDC_form_data')
      .select(`
        *,
        EDC_form_fields (*)
      `)
      .eq('id', formDataId)
      .single();

    if (error || !formData) {
      return res.status(404).json({ error: 'Form data not found' });
    }

    // Cross-visit validation (e.g., weight change > 30% from baseline)
    if (formData.EDC_form_fields.field_name === 'weight') {
      const { data: baselineWeight } = await supabase
        .from('EDC_form_data')
        .select('value')
        .eq('subject_id', subjectId)
        .eq('field_id', formData.field_id)
        .eq('visit_sequence', 1) // Baseline visit
        .single();

      if (baselineWeight) {
        const currentWeight = parseFloat(formData.value);
        const baseline = parseFloat(baselineWeight.value);
        const changePercent = Math.abs((currentWeight - baseline) / baseline * 100);

        if (changePercent > 30) {
          discrepancies.push({
            type: DISCREPANCY_TYPES.LEVEL2,
            severity: SEVERITY_LEVELS.MAJOR,
            field: 'weight',
            message: `Weight change of ${changePercent.toFixed(1)}% from baseline exceeds 30% threshold`,
            value_entered: currentWeight,
            baseline_value: baseline,
            change_percent: changePercent
          });
        }
      }
    }

    // Lab value consistency checks
    if (formData.EDC_form_fields.category === 'laboratory') {
      // Check for abnormal lab values
      const normalRanges = {
        'hemoglobin': { min: 12, max: 18, unit: 'g/dL' },
        'platelet_count': { min: 150000, max: 450000, unit: 'per μL' },
        'creatinine': { min: 0.6, max: 1.2, unit: 'mg/dL' }
      };

      const fieldName = formData.EDC_form_fields.field_name;
      if (normalRanges[fieldName]) {
        const value = parseFloat(formData.value);
        const range = normalRanges[fieldName];
        
        if (value < range.min || value > range.max) {
          discrepancies.push({
            type: DISCREPANCY_TYPES.LEVEL2,
            severity: SEVERITY_LEVELS.MAJOR,
            field: fieldName,
            message: `Lab value outside normal range (${range.min}-${range.max} ${range.unit})`,
            value_entered: value,
            normal_range: `${range.min}-${range.max} ${range.unit}`,
            requires_medical_review: true
          });
        }
      }
    }

    // Save discrepancies to database
    if (discrepancies.length > 0) {
      for (const disc of discrepancies) {
        await supabase
          .from('EDC_queries')
          .insert({
            study_id: formData.study_id,
            site_id: formData.site_id,
            subject_id: subjectId,
            visit_id: visitId,
            form_id: formId,
            field_id: formData.field_id,
            query_type: disc.type,
            severity: disc.severity,
            status: DISCREPANCY_STATUS.OPEN,
            query_text: disc.message,
            value_questioned: disc.value_entered,
            raised_by: req.user.userId,
            raised_date: new Date().toISOString(),
            metadata: JSON.stringify(disc)
          });
      }
    }

    res.json({
      discrepanciesFound: discrepancies.length,
      discrepancies,
      formLocked: discrepancies.some(d => d.severity === SEVERITY_LEVELS.CRITICAL)
    });

  } catch (error) {
    console.error('Submission validation error:', error);
    res.status(500).json({ error: 'Validation failed' });
  }
});

/**
 * Query Management Workflow
 */
router.get('/queries/:studyId', authenticateToken, async (req, res) => {
  try {
    const { studyId } = req.params;
    const { status, severity, assignedTo } = req.query;

    let query = supabase
      .from('EDC_queries')
      .select(`
        *,
        EDC_subjects (subject_code, initials),
        EDC_visits (visit_name),
        EDC_form_templates (form_name),
        raised_user:EDC_users!raised_by (first_name, last_name),
        assigned_user:EDC_users!assigned_to (first_name, last_name)
      `)
      .eq('study_id', studyId)
      .order('raised_date', { ascending: false });

    if (status) query = query.eq('status', status);
    if (severity) query = query.eq('severity', severity);
    if (assignedTo) query = query.eq('assigned_to', assignedTo);

    const { data: queries, error } = await query;

    if (error) throw error;

    res.json({ queries });

  } catch (error) {
    console.error('Get queries error:', error);
    res.status(500).json({ error: 'Failed to fetch queries' });
  }
});

/**
 * Update query status (workflow progression)
 */
router.put('/queries/:queryId/status', authenticateToken, async (req, res) => {
  try {
    const { queryId } = req.params;
    const { status, response, justification } = req.body;

    // Validate status transition
    const validTransitions = {
      [DISCREPANCY_STATUS.OPEN]: [DISCREPANCY_STATUS.ASSIGNED],
      [DISCREPANCY_STATUS.ASSIGNED]: [DISCREPANCY_STATUS.ANSWERED],
      [DISCREPANCY_STATUS.ANSWERED]: [DISCREPANCY_STATUS.REVIEWED, DISCREPANCY_STATUS.ASSIGNED],
      [DISCREPANCY_STATUS.REVIEWED]: [DISCREPANCY_STATUS.RESOLVED, DISCREPANCY_STATUS.ACCEPTED],
      [DISCREPANCY_STATUS.RESOLVED]: [DISCREPANCY_STATUS.CLOSED],
      [DISCREPANCY_STATUS.ACCEPTED]: [DISCREPANCY_STATUS.CLOSED]
    };

    // Get current query
    const { data: currentQuery } = await supabase
      .from('EDC_queries')
      .select('status')
      .eq('id', queryId)
      .single();

    if (!validTransitions[currentQuery.status]?.includes(status)) {
      return res.status(400).json({ 
        error: `Invalid status transition from ${currentQuery.status} to ${status}` 
      });
    }

    // Update query
    const updateData = {
      status,
      updated_by: req.user.userId,
      updated_date: new Date().toISOString()
    };

    if (response) updateData.response_text = response;
    if (justification) updateData.justification = justification;
    if (status === DISCREPANCY_STATUS.ANSWERED) {
      updateData.responded_by = req.user.userId;
      updateData.responded_date = new Date().toISOString();
    }
    if (status === DISCREPANCY_STATUS.CLOSED) {
      updateData.closed_by = req.user.userId;
      updateData.closed_date = new Date().toISOString();
    }

    const { data, error } = await supabase
      .from('EDC_queries')
      .update(updateData)
      .eq('id', queryId)
      .select()
      .single();

    if (error) throw error;

    // Create audit log
    await supabase
      .from('EDC_audit_trail')
      .insert({
        table_name: 'EDC_queries',
        record_id: queryId,
        action: 'UPDATE',
        field_name: 'status',
        old_value: currentQuery.status,
        new_value: status,
        user_id: req.user.userId,
        timestamp: new Date().toISOString(),
        reason_for_change: `Query status updated: ${response || justification || 'No reason provided'}`
      });

    res.json({ 
      success: true, 
      query: data,
      message: `Query status updated to ${status}`
    });

  } catch (error) {
    console.error('Update query status error:', error);
    res.status(500).json({ error: 'Failed to update query status' });
  }
});

/**
 * Get discrepancy statistics
 */
router.get('/stats/:studyId', authenticateToken, async (req, res) => {
  try {
    const { studyId } = req.params;

    // Get query statistics
    const { data: stats } = await supabase
      .from('EDC_queries')
      .select('status, severity, query_type')
      .eq('study_id', studyId);

    const summary = {
      total: stats.length,
      byStatus: {},
      bySeverity: {},
      byType: {},
      openQueries: 0,
      closedQueries: 0,
      averageResolutionTime: 0
    };

    stats.forEach(query => {
      // By status
      summary.byStatus[query.status] = (summary.byStatus[query.status] || 0) + 1;
      
      // By severity
      summary.bySeverity[query.severity] = (summary.bySeverity[query.severity] || 0) + 1;
      
      // By type
      summary.byType[query.query_type] = (summary.byType[query.query_type] || 0) + 1;
      
      // Open vs Closed
      if (query.status === DISCREPANCY_STATUS.CLOSED) {
        summary.closedQueries++;
      } else {
        summary.openQueries++;
      }
    });

    res.json({ statistics: summary });

  } catch (error) {
    console.error('Get statistics error:', error);
    res.status(500).json({ error: 'Failed to fetch statistics' });
  }
});

/**
 * SLA Monitoring - Get queries approaching or breaching SLA
 */
router.get('/sla-monitor/:studyId', authenticateToken, async (req, res) => {
  try {
    const { studyId } = req.params;
    
    // SLA thresholds (in days)
    const SLA_THRESHOLDS = {
      [SEVERITY_LEVELS.CRITICAL]: 2,
      [SEVERITY_LEVELS.MAJOR]: 5,
      [SEVERITY_LEVELS.MINOR]: 10,
      [SEVERITY_LEVELS.NOTE]: 15
    };

    const { data: queries } = await supabase
      .from('EDC_queries')
      .select('*')
      .eq('study_id', studyId)
      .neq('status', DISCREPANCY_STATUS.CLOSED);

    const now = new Date();
    const slaReport = {
      breached: [],
      approaching: [],
      onTrack: []
    };

    queries.forEach(query => {
      const raisedDate = new Date(query.raised_date);
      const daysOpen = Math.floor((now - raisedDate) / (1000 * 60 * 60 * 24));
      const slaLimit = SLA_THRESHOLDS[query.severity] || 10;
      
      const queryInfo = {
        ...query,
        daysOpen,
        slaLimit,
        daysRemaining: slaLimit - daysOpen
      };

      if (daysOpen > slaLimit) {
        slaReport.breached.push(queryInfo);
      } else if (daysOpen >= slaLimit * 0.8) {
        slaReport.approaching.push(queryInfo);
      } else {
        slaReport.onTrack.push(queryInfo);
      }
    });

    res.json({ slaReport });

  } catch (error) {
    console.error('SLA monitoring error:', error);
    res.status(500).json({ error: 'Failed to generate SLA report' });
  }
});

module.exports = router;