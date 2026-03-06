const express = require('express');
const supabase = require('../config/supabase');
const crypto = require('crypto');
const router = express.Router();

// Get audit trail entries with filtering and pagination
router.get('/', async (req, res) => {
  try {
    const {
      study_id,
      site_id,
      subject_id,
      user_id,
      action_type,
      entity_type,
      severity,
      date_from,
      date_to,
      page = 1,
      limit = 100,
      search
    } = req.query;

    let query = supabase
      .from('edc_audit_trail')
      .select(`
        *,
        edc_studies(protocol_number, title),
        edc_sites(name, site_number),
        edc_subjects(subject_number, initials),
        edc_users!user_id(first_name, last_name, username)
      `);

    // Apply filters
    if (study_id) query = query.eq('study_id', study_id);
    if (site_id) query = query.eq('site_id', site_id);
    if (subject_id) query = query.eq('subject_id', subject_id);
    if (user_id) query = query.eq('user_id', user_id);
    if (action_type) query = query.eq('action_type', action_type);
    if (entity_type) query = query.eq('entity_type', entity_type);
    if (severity) query = query.eq('severity', severity);
    
    if (date_from) query = query.gte('created_at', date_from);
    if (date_to) query = query.lte('created_at', date_to);
    
    if (search) {
      query = query.or(`field_name.ilike.%${search}%, reason_for_change.ilike.%${search}%`);
    }

    // Apply pagination
    const offset = (page - 1) * limit;
    query = query.range(offset, offset + limit - 1);

    const { data: auditEntries, error } = await query
      .order('created_at', { ascending: false });

    if (error) throw error;

    // Get total count for pagination
    const { count: totalCount } = await supabase
      .from('edc_audit_trail')
      .select('*', { count: 'exact', head: true });

    res.json({
      status: 'success',
      data: auditEntries || [],
      pagination: {
        page: parseInt(page),
        limit: parseInt(limit),
        total: totalCount,
        pages: Math.ceil(totalCount / limit)
      }
    });
  } catch (error) {
    console.error('Audit trail fetch error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to fetch audit trail',
      error: error.message
    });
  }
});

// Get audit entry by ID
router.get('/:id', async (req, res) => {
  try {
    const { id } = req.params;

    const { data: auditEntry, error } = await supabase
      .from('edc_audit_trail')
      .select(`
        *,
        edc_studies(protocol_number, title),
        edc_sites(name, site_number),
        edc_subjects(subject_number, initials),
        edc_visits(visit_name, visit_number),
        edc_form_templates(name, version),
        edc_users!user_id(first_name, last_name, username, email)
      `)
      .eq('id', id)
      .single();

    if (error) throw error;
    if (!auditEntry) {
      return res.status(404).json({
        status: 'error',
        message: 'Audit entry not found'
      });
    }

    res.json({
      status: 'success',
      data: auditEntry
    });
  } catch (error) {
    console.error('Audit entry fetch error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to fetch audit entry',
      error: error.message
    });
  }
});

// Create audit trail entry
router.post('/', async (req, res) => {
  try {
    const {
      study_id,
      site_id,
      subject_id,
      visit_id,
      form_id,
      form_data_id,
      user_id,
      action_type,
      entity_type,
      entity_id,
      field_name,
      old_value,
      new_value,
      reason_for_change,
      ip_address,
      user_agent,
      session_id,
      signature = false,
      signature_data,
      compliance_flags = [],
      severity = 'LOW'
    } = req.body;

    if (!user_id || !action_type || !entity_type) {
      return res.status(400).json({
        status: 'error',
        message: 'User ID, action type, and entity type are required'
      });
    }

    // Generate data integrity hash
    const dataToHash = JSON.stringify({
      user_id,
      action_type,
      entity_type,
      entity_id,
      field_name,
      old_value,
      new_value,
      timestamp: new Date().toISOString()
    });
    const data_integrity_hash = crypto.createHash('sha256').update(dataToHash).digest('hex');

    const { data: auditEntry, error } = await supabase
      .from('edc_audit_trail')
      .insert([{
        study_id,
        site_id,
        subject_id,
        visit_id,
        form_id,
        form_data_id,
        user_id,
        action_type,
        entity_type,
        entity_id,
        field_name,
        old_value,
        new_value,
        reason_for_change,
        ip_address,
        user_agent,
        session_id,
        signature,
        signature_data,
        compliance_flags,
        severity,
        data_integrity_hash
      }])
      .select(`
        *,
        edc_users!user_id(first_name, last_name, username)
      `)
      .single();

    if (error) throw error;

    res.status(201).json({
      status: 'success',
      message: 'Audit entry created successfully',
      data: auditEntry
    });
  } catch (error) {
    console.error('Audit entry creation error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to create audit entry',
      error: error.message
    });
  }
});

// Get audit statistics
router.get('/stats/overview', async (req, res) => {
  try {
    const { study_id, site_id, date_from, date_to } = req.query;

    let baseQuery = supabase.from('edc_audit_trail');
    
    if (study_id) baseQuery = baseQuery.eq('study_id', study_id);
    if (site_id) baseQuery = baseQuery.eq('site_id', site_id);
    if (date_from) baseQuery = baseQuery.gte('created_at', date_from);
    if (date_to) baseQuery = baseQuery.lte('created_at', date_to);

    // Get total audit entries
    const { count: totalEntries } = await baseQuery
      .select('*', { count: 'exact', head: true });

    // Get counts by action type
    const { data: actionStats } = await baseQuery
      .select('action_type')
      .then(({ data }) => ({
        data: data?.reduce((acc, entry) => {
          acc[entry.action_type] = (acc[entry.action_type] || 0) + 1;
          return acc;
        }, {}) || {}
      }));

    // Get counts by severity
    const { data: severityStats } = await baseQuery
      .select('severity')
      .then(({ data }) => ({
        data: data?.reduce((acc, entry) => {
          acc[entry.severity] = (acc[entry.severity] || 0) + 1;
          return acc;
        }, {}) || {}
      }));

    // Get counts by entity type
    const { data: entityStats } = await baseQuery
      .select('entity_type')
      .then(({ data }) => ({
        data: data?.reduce((acc, entry) => {
          acc[entry.entity_type] = (acc[entry.entity_type] || 0) + 1;
          return acc;
        }, {}) || {}
      }));

    // Get unique users count
    const { data: uniqueUsers } = await baseQuery
      .select('user_id')
      .then(({ data }) => ({
        data: new Set(data?.map(entry => entry.user_id) || []).size
      }));

    // Get signed entries count
    const { count: signedEntries } = await baseQuery
      .select('*', { count: 'exact', head: true })
      .eq('signature', true);

    // Get critical entries count
    const { count: criticalEntries } = await baseQuery
      .select('*', { count: 'exact', head: true })
      .eq('severity', 'CRITICAL');

    res.json({
      status: 'success',
      data: {
        total_entries: totalEntries,
        action_counts: actionStats.data || {},
        severity_counts: severityStats.data || {},
        entity_counts: entityStats.data || {},
        unique_users: uniqueUsers.data,
        signed_entries: signedEntries,
        critical_entries: criticalEntries
      }
    });
  } catch (error) {
    console.error('Audit stats error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to fetch audit statistics',
      error: error.message
    });
  }
});

// Get audit trail by study
router.get('/study/:study_id', async (req, res) => {
  try {
    const { study_id } = req.params;
    const { action_type, severity, entity_type, limit = 50 } = req.query;

    let query = supabase
      .from('edc_audit_trail')
      .select(`
        *,
        edc_sites(name, site_number),
        edc_subjects(subject_number, initials),
        edc_users!user_id(first_name, last_name)
      `)
      .eq('study_id', study_id);

    if (action_type) query = query.eq('action_type', action_type);
    if (severity) query = query.eq('severity', severity);
    if (entity_type) query = query.eq('entity_type', entity_type);

    const { data: auditEntries, error } = await query
      .order('created_at', { ascending: false })
      .limit(parseInt(limit));

    if (error) throw error;

    res.json({
      status: 'success',
      data: auditEntries || []
    });
  } catch (error) {
    console.error('Study audit trail error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to fetch study audit trail',
      error: error.message
    });
  }
});

// Get audit trail by subject
router.get('/subject/:subject_id', async (req, res) => {
  try {
    const { subject_id } = req.params;
    const { action_type, limit = 25 } = req.query;

    let query = supabase
      .from('edc_audit_trail')
      .select(`
        *,
        edc_visits(visit_name, visit_number),
        edc_form_templates(name),
        edc_users!user_id(first_name, last_name)
      `)
      .eq('subject_id', subject_id);

    if (action_type) query = query.eq('action_type', action_type);

    const { data: auditEntries, error } = await query
      .order('created_at', { ascending: false })
      .limit(parseInt(limit));

    if (error) throw error;

    res.json({
      status: 'success',
      data: auditEntries || []
    });
  } catch (error) {
    console.error('Subject audit trail error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to fetch subject audit trail',
      error: error.message
    });
  }
});

// Verify audit trail integrity
router.post('/verify', async (req, res) => {
  try {
    const { entry_ids } = req.body;

    if (!entry_ids || !Array.isArray(entry_ids)) {
      return res.status(400).json({
        status: 'error',
        message: 'Array of entry IDs is required'
      });
    }

    const { data: entries, error } = await supabase
      .from('edc_audit_trail')
      .select('*')
      .in('id', entry_ids);

    if (error) throw error;

    const verificationResults = entries.map(entry => {
      // Recreate hash to verify integrity
      const dataToHash = JSON.stringify({
        user_id: entry.user_id,
        action_type: entry.action_type,
        entity_type: entry.entity_type,
        entity_id: entry.entity_id,
        field_name: entry.field_name,
        old_value: entry.old_value,
        new_value: entry.new_value,
        timestamp: entry.created_at
      });
      const expectedHash = crypto.createHash('sha256').update(dataToHash).digest('hex');
      const isValid = expectedHash === entry.data_integrity_hash;

      return {
        id: entry.id,
        is_valid: isValid,
        stored_hash: entry.data_integrity_hash,
        calculated_hash: expectedHash,
        created_at: entry.created_at
      };
    });

    const totalChecked = verificationResults.length;
    const validEntries = verificationResults.filter(r => r.is_valid).length;
    const invalidEntries = totalChecked - validEntries;

    res.json({
      status: 'success',
      data: {
        summary: {
          total_checked: totalChecked,
          valid_entries: validEntries,
          invalid_entries: invalidEntries,
          integrity_percentage: totalChecked > 0 ? ((validEntries / totalChecked) * 100).toFixed(2) : '0.00'
        },
        results: verificationResults
      }
    });
  } catch (error) {
    console.error('Audit verification error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to verify audit trail integrity',
      error: error.message
    });
  }
});

// Export audit trail (compliance report)
router.get('/export/compliance', async (req, res) => {
  try {
    const { study_id, date_from, date_to, format = 'json' } = req.query;

    let query = supabase
      .from('edc_audit_trail')
      .select(`
        *,
        edc_studies(protocol_number, title),
        edc_sites(name, site_number),
        edc_subjects(subject_number, initials),
        edc_users!user_id(first_name, last_name, username)
      `);

    if (study_id) query = query.eq('study_id', study_id);
    if (date_from) query = query.gte('created_at', date_from);
    if (date_to) query = query.lte('created_at', date_to);

    const { data: auditEntries, error } = await query
      .order('created_at', { ascending: false });

    if (error) throw error;

    // Add compliance information
    const complianceReport = {
      generated_at: new Date().toISOString(),
      compliance_standard: '21 CFR Part 11',
      report_parameters: {
        study_id,
        date_from,
        date_to
      },
      summary: {
        total_entries: auditEntries.length,
        unique_users: new Set(auditEntries.map(e => e.user_id)).size,
        date_range: {
          earliest: auditEntries[auditEntries.length - 1]?.created_at,
          latest: auditEntries[0]?.created_at
        }
      },
      audit_entries: auditEntries
    };

    if (format === 'csv') {
      // For CSV format, we would need to implement CSV conversion
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', 'attachment; filename="audit_trail_compliance_report.csv"');
      // CSV implementation would go here
      res.json({ message: 'CSV export not implemented yet' });
    } else {
      res.setHeader('Content-Type', 'application/json');
      res.setHeader('Content-Disposition', 'attachment; filename="audit_trail_compliance_report.json"');
      res.json({
        status: 'success',
        data: complianceReport
      });
    }
  } catch (error) {
    console.error('Audit export error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to export audit trail',
      error: error.message
    });
  }
});

module.exports = router;
