const express = require('express');
const supabase = require('../src/config/supabase');
const { authenticateToken } = require('../src/middleware/auth');
const router = express.Router();

/**
 * UAT Workspace Module
 * Provides a test environment for validating forms and edit checks
 * before promoting to production
 */

// UAT environment statuses
const UAT_STATUS = {
  DRAFT: 'draft',
  IN_UAT: 'in_uat',
  UAT_PASSED: 'uat_passed',
  UAT_FAILED: 'uat_failed',
  PRODUCTION: 'production'
};

// UAT defect statuses
const DEFECT_STATUS = {
  OPEN: 'open',
  IN_PROGRESS: 'in_progress',
  FIXED: 'fixed',
  RE_TESTED: 're_tested',
  CLOSED: 'closed',
  REJECTED: 'rejected'
};

// UAT test case statuses
const TEST_CASE_STATUS = {
  NOT_STARTED: 'not_started',
  IN_PROGRESS: 'in_progress',
  PASSED: 'passed',
  FAILED: 'failed',
  BLOCKED: 'blocked'
};

/**
 * Create UAT workspace for a form
 */
router.post('/workspace/create', authenticateToken, async (req, res) => {
  try {
    const { formId, studyId, testPlan } = req.body;

    // Check if form exists
    const { data: form } = await supabase
      .from('EDC_form_templates')
      .select('*')
      .eq('id', formId)
      .single();

    if (!form) {
      return res.status(404).json({ error: 'Form not found' });
    }

    // Create UAT workspace
    const workspaceData = {
      form_id: formId,
      study_id: studyId,
      form_version: form.version,
      status: UAT_STATUS.IN_UAT,
      test_plan: testPlan,
      created_by: req.user.userId,
      created_date: new Date().toISOString(),
      metadata: {
        original_form: form,
        test_environment: 'UAT',
        validation_rules: []
      }
    };

    const { data: workspace, error } = await supabase
      .from('EDC_uat_workspaces')
      .insert(workspaceData)
      .select()
      .single();

    if (error) throw error;

    // Create default test cases
    const testCases = await createDefaultTestCases(workspace.id, formId);

    res.json({
      success: true,
      workspace,
      testCases: testCases.length,
      message: 'UAT workspace created successfully'
    });

  } catch (error) {
    console.error('Create UAT workspace error:', error);
    res.status(500).json({ error: 'Failed to create UAT workspace' });
  }
});

/**
 * Create default test cases for a form
 */
async function createDefaultTestCases(workspaceId, formId) {
  // Get form fields
  const { data: fields } = await supabase
    .from('EDC_form_fields')
    .select('*')
    .eq('form_template_id', formId)
    .order('display_order');

  const testCases = [];

  for (const field of fields) {
    // Test case for required fields
    if (field.is_required) {
      testCases.push({
        workspace_id: workspaceId,
        field_id: field.id,
        test_type: 'required_field',
        test_name: `Verify ${field.field_label} is required`,
        test_description: `Ensure that ${field.field_label} field shows error when left empty`,
        expected_result: 'Error message displayed when field is empty',
        test_data: { field: field.field_name, value: '' },
        status: TEST_CASE_STATUS.NOT_STARTED
      });
    }

    // Test case for data type validation
    if (field.datatype === 'integer') {
      testCases.push({
        workspace_id: workspaceId,
        field_id: field.id,
        test_type: 'datatype',
        test_name: `Verify ${field.field_label} accepts only integers`,
        test_description: `Test that ${field.field_label} rejects non-integer values`,
        expected_result: 'Error when entering alphabets or decimals',
        test_data: { 
          field: field.field_name, 
          invalid_values: ['ABC', '12.5', 'test123'],
          valid_values: ['123', '0', '-45']
        },
        status: TEST_CASE_STATUS.NOT_STARTED
      });
    }

    // Test case for range validation
    if (field.min_value !== null || field.max_value !== null) {
      testCases.push({
        workspace_id: workspaceId,
        field_id: field.id,
        test_type: 'range',
        test_name: `Verify ${field.field_label} range validation`,
        test_description: `Test min (${field.min_value}) and max (${field.max_value}) values`,
        expected_result: 'Error when value is outside specified range',
        test_data: { 
          field: field.field_name,
          min: field.min_value,
          max: field.max_value,
          test_values: [
            field.min_value - 1,
            field.min_value,
            field.max_value,
            field.max_value + 1
          ]
        },
        status: TEST_CASE_STATUS.NOT_STARTED
      });
    }

    // Test case for pattern validation
    if (field.pattern) {
      testCases.push({
        workspace_id: workspaceId,
        field_id: field.id,
        test_type: 'pattern',
        test_name: `Verify ${field.field_label} pattern validation`,
        test_description: `Test that field matches pattern: ${field.pattern}`,
        expected_result: field.pattern_message || 'Error when value doesn\'t match pattern',
        test_data: { 
          field: field.field_name,
          pattern: field.pattern,
          valid_examples: field.valid_examples,
          invalid_examples: field.invalid_examples
        },
        status: TEST_CASE_STATUS.NOT_STARTED
      });
    }
  }

  // Insert test cases
  if (testCases.length > 0) {
    const { data, error } = await supabase
      .from('EDC_uat_test_cases')
      .insert(testCases)
      .select();
    
    return data || [];
  }

  return [];
}

/**
 * Get UAT workspace details
 */
router.get('/workspace/:workspaceId', authenticateToken, async (req, res) => {
  try {
    const { workspaceId } = req.params;

    // Get workspace
    const { data: workspace, error } = await supabase
      .from('EDC_uat_workspaces')
      .select(`
        *,
        EDC_form_templates (form_name, version),
        created_user:EDC_users!created_by (first_name, last_name)
      `)
      .eq('id', workspaceId)
      .single();

    if (error || !workspace) {
      return res.status(404).json({ error: 'Workspace not found' });
    }

    // Get test cases
    const { data: testCases } = await supabase
      .from('EDC_uat_test_cases')
      .select('*')
      .eq('workspace_id', workspaceId)
      .order('created_date');

    // Get defects
    const { data: defects } = await supabase
      .from('EDC_uat_defects')
      .select('*')
      .eq('workspace_id', workspaceId)
      .order('created_date', { ascending: false });

    // Calculate statistics
    const stats = {
      totalTestCases: testCases.length,
      passed: testCases.filter(tc => tc.status === TEST_CASE_STATUS.PASSED).length,
      failed: testCases.filter(tc => tc.status === TEST_CASE_STATUS.FAILED).length,
      inProgress: testCases.filter(tc => tc.status === TEST_CASE_STATUS.IN_PROGRESS).length,
      notStarted: testCases.filter(tc => tc.status === TEST_CASE_STATUS.NOT_STARTED).length,
      blocked: testCases.filter(tc => tc.status === TEST_CASE_STATUS.BLOCKED).length,
      totalDefects: defects.length,
      openDefects: defects.filter(d => d.status === DEFECT_STATUS.OPEN).length,
      closedDefects: defects.filter(d => d.status === DEFECT_STATUS.CLOSED).length
    };

    res.json({
      workspace,
      testCases,
      defects,
      statistics: stats
    });

  } catch (error) {
    console.error('Get workspace error:', error);
    res.status(500).json({ error: 'Failed to fetch workspace' });
  }
});

/**
 * Execute test case
 */
router.post('/test-case/:testCaseId/execute', authenticateToken, async (req, res) => {
  try {
    const { testCaseId } = req.params;
    const { actualResult, status, notes, testData } = req.body;

    // Update test case
    const updateData = {
      status,
      actual_result: actualResult,
      test_notes: notes,
      executed_by: req.user.userId,
      executed_date: new Date().toISOString(),
      execution_data: testData
    };

    const { data: testCase, error } = await supabase
      .from('EDC_uat_test_cases')
      .update(updateData)
      .eq('id', testCaseId)
      .select()
      .single();

    if (error) throw error;

    // If test failed, optionally create a defect
    if (status === TEST_CASE_STATUS.FAILED && req.body.createDefect) {
      const defect = await createDefect({
        workspaceId: testCase.workspace_id,
        testCaseId,
        title: `Test Case Failed: ${testCase.test_name}`,
        description: `Expected: ${testCase.expected_result}\nActual: ${actualResult}`,
        severity: 'major',
        reportedBy: req.user.userId
      });
    }

    res.json({
      success: true,
      testCase,
      message: `Test case ${status}`
    });

  } catch (error) {
    console.error('Execute test case error:', error);
    res.status(500).json({ error: 'Failed to execute test case' });
  }
});

/**
 * Create UAT defect
 */
async function createDefect(defectData) {
  const { data, error } = await supabase
    .from('EDC_uat_defects')
    .insert({
      workspace_id: defectData.workspaceId,
      test_case_id: defectData.testCaseId,
      defect_title: defectData.title,
      defect_description: defectData.description,
      severity: defectData.severity,
      status: DEFECT_STATUS.OPEN,
      reported_by: defectData.reportedBy,
      reported_date: new Date().toISOString()
    })
    .select()
    .single();

  return data;
}

/**
 * Report UAT defect
 */
router.post('/defect/create', authenticateToken, async (req, res) => {
  try {
    const { 
      workspaceId, 
      testCaseId, 
      title, 
      description, 
      severity,
      steps_to_reproduce,
      attachments 
    } = req.body;

    const defectData = {
      workspace_id: workspaceId,
      test_case_id: testCaseId,
      defect_title: title,
      defect_description: description,
      severity,
      status: DEFECT_STATUS.OPEN,
      steps_to_reproduce,
      attachments,
      reported_by: req.user.userId,
      reported_date: new Date().toISOString()
    };

    const { data: defect, error } = await supabase
      .from('EDC_uat_defects')
      .insert(defectData)
      .select()
      .single();

    if (error) throw error;

    res.json({
      success: true,
      defect,
      message: 'Defect reported successfully'
    });

  } catch (error) {
    console.error('Create defect error:', error);
    res.status(500).json({ error: 'Failed to create defect' });
  }
});

/**
 * Update defect status
 */
router.put('/defect/:defectId/status', authenticateToken, async (req, res) => {
  try {
    const { defectId } = req.params;
    const { status, resolution, fixNotes } = req.body;

    const updateData = {
      status,
      updated_by: req.user.userId,
      updated_date: new Date().toISOString()
    };

    if (resolution) updateData.resolution = resolution;
    if (fixNotes) updateData.fix_notes = fixNotes;
    
    if (status === DEFECT_STATUS.FIXED) {
      updateData.fixed_by = req.user.userId;
      updateData.fixed_date = new Date().toISOString();
    }
    
    if (status === DEFECT_STATUS.CLOSED) {
      updateData.closed_by = req.user.userId;
      updateData.closed_date = new Date().toISOString();
    }

    const { data: defect, error } = await supabase
      .from('EDC_uat_defects')
      .update(updateData)
      .eq('id', defectId)
      .select()
      .single();

    if (error) throw error;

    res.json({
      success: true,
      defect,
      message: `Defect status updated to ${status}`
    });

  } catch (error) {
    console.error('Update defect error:', error);
    res.status(500).json({ error: 'Failed to update defect' });
  }
});

/**
 * Promote form from UAT to Production
 */
router.post('/promote/:workspaceId', authenticateToken, async (req, res) => {
  try {
    const { workspaceId } = req.params;
    const { eSignature, password, promotionNotes } = req.body;

    // Verify user password for e-signature
    // In production, properly validate password
    if (!eSignature || !password) {
      return res.status(400).json({ 
        error: 'Electronic signature and password required for promotion' 
      });
    }

    // Get workspace
    const { data: workspace } = await supabase
      .from('EDC_uat_workspaces')
      .select('*')
      .eq('id', workspaceId)
      .single();

    if (!workspace) {
      return res.status(404).json({ error: 'Workspace not found' });
    }

    // Check if all test cases passed
    const { data: testCases } = await supabase
      .from('EDC_uat_test_cases')
      .select('status')
      .eq('workspace_id', workspaceId);

    const failedTests = testCases.filter(tc => 
      tc.status === TEST_CASE_STATUS.FAILED || 
      tc.status === TEST_CASE_STATUS.NOT_STARTED
    );

    if (failedTests.length > 0) {
      return res.status(400).json({ 
        error: `Cannot promote: ${failedTests.length} test cases not passed` 
      });
    }

    // Check for open defects
    const { data: openDefects } = await supabase
      .from('EDC_uat_defects')
      .select('id')
      .eq('workspace_id', workspaceId)
      .neq('status', DEFECT_STATUS.CLOSED);

    if (openDefects.length > 0) {
      return res.status(400).json({ 
        error: `Cannot promote: ${openDefects.length} open defects` 
      });
    }

    // Update form status to production
    const { error: formError } = await supabase
      .from('EDC_form_templates')
      .update({ 
        status: 'production',
        promoted_date: new Date().toISOString(),
        promoted_by: req.user.userId
      })
      .eq('id', workspace.form_id);

    if (formError) throw formError;

    // Update workspace status
    const { error: workspaceError } = await supabase
      .from('EDC_uat_workspaces')
      .update({ 
        status: UAT_STATUS.PRODUCTION,
        promoted_date: new Date().toISOString(),
        promoted_by: req.user.userId,
        promotion_notes: promotionNotes,
        e_signature: {
          signed_by: req.user.userId,
          signature: eSignature,
          timestamp: new Date().toISOString(),
          ip_address: req.ip
        }
      })
      .eq('id', workspaceId);

    if (workspaceError) throw workspaceError;

    // Create audit log
    await supabase
      .from('EDC_audit_trail')
      .insert({
        table_name: 'EDC_form_templates',
        record_id: workspace.form_id,
        action: 'PROMOTE',
        field_name: 'status',
        old_value: 'uat',
        new_value: 'production',
        user_id: req.user.userId,
        timestamp: new Date().toISOString(),
        reason_for_change: `Form promoted from UAT to Production: ${promotionNotes}`,
        e_signature: eSignature
      });

    res.json({
      success: true,
      message: 'Form successfully promoted to production',
      promotionDetails: {
        formId: workspace.form_id,
        promotedBy: req.user.userId,
        promotedDate: new Date().toISOString()
      }
    });

  } catch (error) {
    console.error('Promote form error:', error);
    res.status(500).json({ error: 'Failed to promote form' });
  }
});

/**
 * Generate UAT evidence report
 */
router.get('/workspace/:workspaceId/evidence', authenticateToken, async (req, res) => {
  try {
    const { workspaceId } = req.params;

    // Get all UAT data
    const { data: workspace } = await supabase
      .from('EDC_uat_workspaces')
      .select(`
        *,
        EDC_form_templates (form_name, version)
      `)
      .eq('id', workspaceId)
      .single();

    const { data: testCases } = await supabase
      .from('EDC_uat_test_cases')
      .select('*')
      .eq('workspace_id', workspaceId);

    const { data: defects } = await supabase
      .from('EDC_uat_defects')
      .select('*')
      .eq('workspace_id', workspaceId);

    // Generate evidence report
    const evidenceReport = {
      workspace,
      summary: {
        formName: workspace.EDC_form_templates.form_name,
        version: workspace.EDC_form_templates.version,
        uatStartDate: workspace.created_date,
        uatEndDate: workspace.promoted_date || 'In Progress',
        totalTestCases: testCases.length,
        passedTestCases: testCases.filter(tc => tc.status === TEST_CASE_STATUS.PASSED).length,
        totalDefects: defects.length,
        resolvedDefects: defects.filter(d => d.status === DEFECT_STATUS.CLOSED).length
      },
      testCases: testCases.map(tc => ({
        name: tc.test_name,
        type: tc.test_type,
        status: tc.status,
        executedBy: tc.executed_by,
        executedDate: tc.executed_date,
        result: tc.actual_result
      })),
      defects: defects.map(d => ({
        id: d.id,
        title: d.defect_title,
        severity: d.severity,
        status: d.status,
        reportedDate: d.reported_date,
        fixedDate: d.fixed_date,
        resolution: d.resolution
      })),
      traceability: {
        requirements: workspace.test_plan?.requirements || [],
        testCoverage: calculateTestCoverage(workspace, testCases),
        defectMetrics: calculateDefectMetrics(defects)
      },
      approvals: workspace.e_signature ? {
        approvedBy: workspace.promoted_by,
        approvalDate: workspace.promoted_date,
        eSignature: 'Signed'
      } : null
    };

    res.json({ evidenceReport });

  } catch (error) {
    console.error('Generate evidence error:', error);
    res.status(500).json({ error: 'Failed to generate UAT evidence' });
  }
});

/**
 * Helper function to calculate test coverage
 */
function calculateTestCoverage(workspace, testCases) {
  const totalFields = workspace.metadata?.original_form?.field_count || 0;
  const testedFields = new Set(testCases.map(tc => tc.field_id)).size;
  return {
    totalFields,
    testedFields,
    coverage: totalFields > 0 ? ((testedFields / totalFields) * 100).toFixed(1) + '%' : '0%'
  };
}

/**
 * Helper function to calculate defect metrics
 */
function calculateDefectMetrics(defects) {
  const severityCount = {};
  defects.forEach(d => {
    severityCount[d.severity] = (severityCount[d.severity] || 0) + 1;
  });

  const avgResolutionTime = defects
    .filter(d => d.fixed_date)
    .reduce((sum, d) => {
      const raised = new Date(d.reported_date);
      const fixed = new Date(d.fixed_date);
      return sum + (fixed - raised) / (1000 * 60 * 60 * 24); // Days
    }, 0) / defects.filter(d => d.fixed_date).length || 0;

  return {
    bySeverity: severityCount,
    averageResolutionDays: avgResolutionTime.toFixed(1),
    defectDensity: defects.length
  };
}

module.exports = router;