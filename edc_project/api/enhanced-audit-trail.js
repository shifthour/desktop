const express = require('express');
const supabase = require('../src/config/supabase');
const { authenticateToken } = require('../src/middleware/auth');
const router = express.Router();

/**
 * Enhanced Audit Trail Module
 * 21 CFR Part 11 compliant audit logging system
 * Tracks all data changes with immutable records
 */

// Audit action types
const AUDIT_ACTIONS = {
  CREATE: 'CREATE',
  READ: 'READ',
  UPDATE: 'UPDATE',
  DELETE: 'DELETE',
  LOGIN: 'LOGIN',
  LOGOUT: 'LOGOUT',
  EXPORT: 'EXPORT',
  IMPORT: 'IMPORT',
  PROMOTE: 'PROMOTE',
  LOCK: 'LOCK',
  UNLOCK: 'UNLOCK',
  SIGN: 'E_SIGN'
};

// Data categories for audit classification
const DATA_CATEGORIES = {
  SUBJECT_DATA: 'subject_data',
  FORM_DATA: 'form_data',
  SYSTEM_CONFIG: 'system_config',
  USER_MANAGEMENT: 'user_management',
  STUDY_SETUP: 'study_setup',
  SECURITY: 'security'
};

// Risk levels for audit events
const RISK_LEVELS = {
  LOW: 'low',
  MEDIUM: 'medium',
  HIGH: 'high',
  CRITICAL: 'critical'
};

/**
 * Create audit log entry
 * This should be called automatically by other modules
 */
async function createAuditLog(auditData) {
  try {
    const logEntry = {
      study_id: auditData.studyId,
      site_id: auditData.siteId,
      subject_id: auditData.subjectId,
      user_id: auditData.userId,
      action_type: auditData.action,
      entity_type: auditData.tableName,
      entity_id: auditData.recordId,
      field_name: auditData.fieldName,
      old_value: auditData.oldValue,
      new_value: auditData.newValue,
      reason_for_change: auditData.reasonForChange,
      ip_address: auditData.ipAddress,
      user_agent: auditData.userAgent,
      session_id: auditData.sessionId,
      signature: auditData.eSignature || false,
      severity: auditData.riskLevel || 'LOW',
      data_integrity_hash: generateChecksum(auditData)
    };

    const { data, error } = await supabase
      .from('edc_audit_trail')
      .insert(logEntry)
      .select()
      .single();

    if (error) {
      console.error('Audit log creation failed:', error);
      throw error;
    }

    return data;
  } catch (error) {
    console.error('Audit logging error:', error);
    throw error;
  }
}

/**
 * Generate checksum for audit log integrity
 */
function generateChecksum(auditData) {
  const crypto = require('crypto');
  const dataString = JSON.stringify({
    table: auditData.tableName,
    record: auditData.recordId,
    action: auditData.action,
    old: auditData.oldValue,
    new: auditData.newValue,
    user: auditData.userId,
    timestamp: new Date().toISOString()
  });
  return crypto.createHash('sha256').update(dataString).digest('hex');
}

/**
 * Middleware to automatically capture audit information
 */
const auditMiddleware = (options = {}) => {
  return (req, res, next) => {
    // Store original res.json to intercept responses
    const originalJson = res.json;
    
    res.json = function(data) {
      // Capture audit info if this was a data modification
      if (['POST', 'PUT', 'PATCH', 'DELETE'].includes(req.method)) {
        const auditInfo = {
          userId: req.user?.userId,
          ipAddress: req.ip,
          userAgent: req.get('User-Agent'),
          sessionId: req.sessionID,
          action: getActionFromMethod(req.method),
          tableName: options.tableName || extractTableFromPath(req.path),
          timestamp: new Date().toISOString()
        };
        
        // Store audit info for use in route handlers
        req.auditInfo = auditInfo;
      }
      
      return originalJson.call(this, data);
    };
    
    next();
  };
};

/**
 * Get audit action from HTTP method
 */
function getActionFromMethod(method) {
  const methodMap = {
    'POST': AUDIT_ACTIONS.CREATE,
    'PUT': AUDIT_ACTIONS.UPDATE,
    'PATCH': AUDIT_ACTIONS.UPDATE,
    'DELETE': AUDIT_ACTIONS.DELETE,
    'GET': AUDIT_ACTIONS.READ
  };
  return methodMap[method] || AUDIT_ACTIONS.READ;
}

/**
 * Extract table name from API path
 */
function extractTableFromPath(path) {
  const pathParts = path.split('/');
  const resourceIndex = pathParts.indexOf('api') + 1;
  return pathParts[resourceIndex] || 'unknown';
}

/**
 * Get all audit trail entries (root route)
 */
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

/**
 * Create audit trail entry
 */
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
    const crypto = require('crypto');
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
      .select('*')
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

/**
 * Get audit trail for specific record
 */
router.get('/record/:tableName/:recordId', authenticateToken, async (req, res) => {
  try {
    const { tableName, recordId } = req.params;
    const { limit = 50, offset = 0 } = req.query;

    const { data: auditLogs, error } = await supabase
      .from('edc_audit_trail')
      .select(`
        *,
        edc_users (first_name, last_name, username)
      `)
      .eq('table_name', tableName)
      .eq('record_id', recordId)
      .order('timestamp', { ascending: false })
      .range(offset, offset + limit - 1);

    if (error) throw error;

    // Group by action type
    const groupedLogs = auditLogs.reduce((acc, log) => {
      if (!acc[log.action]) acc[log.action] = [];
      acc[log.action].push(log);
      return acc;
    }, {});

    res.json({
      auditLogs,
      groupedByAction: groupedLogs,
      totalRecords: auditLogs.length,
      recordInfo: {
        tableName,
        recordId,
        firstChange: auditLogs[auditLogs.length - 1]?.timestamp,
        lastChange: auditLogs[0]?.timestamp
      }
    });

  } catch (error) {
    console.error('Get record audit error:', error);
    res.status(500).json({ error: 'Failed to fetch audit trail' });
  }
});

/**
 * Get audit trail for user
 */
router.get('/user/:userId', authenticateToken, async (req, res) => {
  try {
    const { userId } = req.params;
    const { startDate, endDate, action, limit = 100 } = req.query;

    let query = supabase
      .from('edc_audit_trail')
      .select('*')
      .eq('user_id', userId)
      .order('timestamp', { ascending: false })
      .limit(limit);

    if (startDate) query = query.gte('timestamp', startDate);
    if (endDate) query = query.lte('timestamp', endDate);
    if (action) query = query.eq('action', action);

    const { data: auditLogs, error } = await query;

    if (error) throw error;

    // Calculate user activity summary
    const summary = {
      totalActions: auditLogs.length,
      actionBreakdown: {},
      mostActiveDay: null,
      riskLevelBreakdown: {}
    };

    auditLogs.forEach(log => {
      // Action breakdown
      summary.actionBreakdown[log.action] = (summary.actionBreakdown[log.action] || 0) + 1;
      
      // Risk level breakdown
      summary.riskLevelBreakdown[log.risk_level] = (summary.riskLevelBreakdown[log.risk_level] || 0) + 1;
    });

    res.json({
      auditLogs,
      summary,
      userId,
      dateRange: { startDate, endDate }
    });

  } catch (error) {
    console.error('Get user audit error:', error);
    res.status(500).json({ error: 'Failed to fetch user audit trail' });
  }
});

/**
 * Get audit trail for study
 */
router.get('/study/:studyId', authenticateToken, async (req, res) => {
  try {
    const { studyId } = req.params;
    const { 
      action, 
      riskLevel, 
      startDate, 
      endDate, 
      userId,
      limit = 200,
      offset = 0
    } = req.query;

    let query = supabase
      .from('edc_audit_trail')
      .select(`
        *,
        edc_users (first_name, last_name, username, role)
      `)
      .eq('study_id', studyId)
      .order('timestamp', { ascending: false })
      .range(offset, offset + limit - 1);

    if (action) query = query.eq('action', action);
    if (riskLevel) query = query.eq('risk_level', riskLevel);
    if (startDate) query = query.gte('timestamp', startDate);
    if (endDate) query = query.lte('timestamp', endDate);
    if (userId) query = query.eq('user_id', userId);

    const { data: auditLogs, error } = await query;

    if (error) throw error;

    // Study-specific analytics
    const analytics = {
      totalEvents: auditLogs.length,
      uniqueUsers: new Set(auditLogs.map(log => log.user_id)).size,
      dataIntegrityEvents: auditLogs.filter(log => 
        log.risk_level === RISK_LEVELS.HIGH || 
        log.risk_level === RISK_LEVELS.CRITICAL
      ).length,
      recentActivity: auditLogs.slice(0, 10),
      timeRange: {
        start: auditLogs[auditLogs.length - 1]?.timestamp,
        end: auditLogs[0]?.timestamp
      }
    };

    res.json({
      auditLogs,
      analytics,
      studyId,
      filters: { action, riskLevel, startDate, endDate, userId }
    });

  } catch (error) {
    console.error('Get study audit error:', error);
    res.status(500).json({ error: 'Failed to fetch study audit trail' });
  }
});

/**
 * Generate compliance report
 */
router.get('/compliance/:studyId', authenticateToken, async (req, res) => {
  try {
    const { studyId } = req.params;
    const { startDate, endDate } = req.query;

    // Get all audit logs for the study
    let query = supabase
      .from('edc_audit_trail')
      .select(`
        *,
        edc_users (first_name, last_name, username, role)
      `)
      .eq('study_id', studyId);

    if (startDate) query = query.gte('timestamp', startDate);
    if (endDate) query = query.lte('timestamp', endDate);

    const { data: auditLogs, error } = await query;

    if (error) throw error;

    // 21 CFR Part 11 compliance metrics
    const complianceMetrics = {
      // Data integrity
      dataModifications: auditLogs.filter(log => 
        log.action === AUDIT_ACTIONS.UPDATE && 
        log.data_category === DATA_CATEGORIES.SUBJECT_DATA
      ).length,
      
      // All modifications have reasons
      modificationsWithReasons: auditLogs.filter(log => 
        log.action === AUDIT_ACTIONS.UPDATE && 
        log.reason_for_change
      ).length,
      
      // E-signatures captured
      eSignedActions: auditLogs.filter(log => log.e_signature).length,
      
      // User access patterns
      uniqueUsers: new Set(auditLogs.map(log => log.user_id)).size,
      
      // System access
      loginEvents: auditLogs.filter(log => log.action === AUDIT_ACTIONS.LOGIN).length,
      
      // Data exports
      dataExports: auditLogs.filter(log => log.action === AUDIT_ACTIONS.EXPORT).length,
      
      // High-risk events
      highRiskEvents: auditLogs.filter(log => 
        log.risk_level === RISK_LEVELS.HIGH || 
        log.risk_level === RISK_LEVELS.CRITICAL
      ).length,
      
      // Audit log completeness
      recordsWithChecksum: auditLogs.filter(log => log.checksum).length,
      totalRecords: auditLogs.length
    };

    // Calculate compliance score
    const complianceScore = calculateComplianceScore(complianceMetrics);

    // Identify compliance gaps
    const complianceGaps = identifyComplianceGaps(complianceMetrics, auditLogs);

    const report = {
      studyId,
      reportDate: new Date().toISOString(),
      period: { startDate, endDate },
      metrics: complianceMetrics,
      complianceScore,
      gaps: complianceGaps,
      recommendations: generateComplianceRecommendations(complianceGaps),
      summary: {
        totalAuditRecords: auditLogs.length,
        complianceLevel: complianceScore >= 90 ? 'Excellent' : 
                        complianceScore >= 75 ? 'Good' : 
                        complianceScore >= 60 ? 'Needs Improvement' : 'Poor',
        riskLevel: complianceMetrics.highRiskEvents > 10 ? 'High' : 
                  complianceMetrics.highRiskEvents > 5 ? 'Medium' : 'Low'
      }
    };

    res.json({ complianceReport: report });

  } catch (error) {
    console.error('Compliance report error:', error);
    res.status(500).json({ error: 'Failed to generate compliance report' });
  }
});

/**
 * Calculate compliance score based on metrics
 */
function calculateComplianceScore(metrics) {
  let score = 100;
  
  // Deduct points for missing reasons
  const reasonsRatio = metrics.totalRecords > 0 ? 
    metrics.modificationsWithReasons / metrics.dataModifications : 1;
  score -= (1 - reasonsRatio) * 30;
  
  // Deduct points for missing checksums
  const checksumRatio = metrics.totalRecords > 0 ? 
    metrics.recordsWithChecksum / metrics.totalRecords : 1;
  score -= (1 - checksumRatio) * 20;
  
  // Deduct points for high-risk events
  if (metrics.highRiskEvents > 20) score -= 20;
  else if (metrics.highRiskEvents > 10) score -= 10;
  else if (metrics.highRiskEvents > 5) score -= 5;
  
  return Math.max(0, Math.round(score));
}

/**
 * Identify compliance gaps
 */
function identifyComplianceGaps(metrics, auditLogs) {
  const gaps = [];
  
  if (metrics.modificationsWithReasons < metrics.dataModifications) {
    gaps.push({
      type: 'missing_reasons',
      severity: 'high',
      description: 'Some data modifications lack reason for change',
      count: metrics.dataModifications - metrics.modificationsWithReasons
    });
  }
  
  if (metrics.recordsWithChecksum < metrics.totalRecords) {
    gaps.push({
      type: 'missing_checksums',
      severity: 'medium',
      description: 'Some audit records lack integrity checksums',
      count: metrics.totalRecords - metrics.recordsWithChecksum
    });
  }
  
  if (metrics.highRiskEvents > 10) {
    gaps.push({
      type: 'high_risk_events',
      severity: 'high',
      description: 'High number of high-risk audit events',
      count: metrics.highRiskEvents
    });
  }
  
  // Check for users without recent activity
  const recentLogs = auditLogs.filter(log => {
    const logDate = new Date(log.timestamp);
    const thirtyDaysAgo = new Date();
    thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
    return logDate > thirtyDaysAgo;
  });
  
  if (recentLogs.length === 0) {
    gaps.push({
      type: 'no_recent_activity',
      severity: 'medium',
      description: 'No audit activity in the last 30 days'
    });
  }
  
  return gaps;
}

/**
 * Generate compliance recommendations
 */
function generateComplianceRecommendations(gaps) {
  const recommendations = [];
  
  gaps.forEach(gap => {
    switch (gap.type) {
      case 'missing_reasons':
        recommendations.push({
          priority: 'high',
          action: 'Require reason for change on all data modifications',
          implementation: 'Update form validation to mandate reason field'
        });
        break;
      case 'missing_checksums':
        recommendations.push({
          priority: 'medium',
          action: 'Enable audit log integrity checking',
          implementation: 'Update audit trail system to generate checksums'
        });
        break;
      case 'high_risk_events':
        recommendations.push({
          priority: 'high',
          action: 'Review high-risk activities and implement additional controls',
          implementation: 'Conduct security review and add approval workflows'
        });
        break;
      case 'no_recent_activity':
        recommendations.push({
          priority: 'low',
          action: 'Verify system usage and user access',
          implementation: 'Check if study is active and users have proper access'
        });
        break;
    }
  });
  
  return recommendations;
}

/**
 * Verify audit log integrity
 */
router.post('/verify-integrity', authenticateToken, async (req, res) => {
  try {
    const { studyId, startDate, endDate } = req.body;

    let query = supabase
      .from('edc_audit_trail')
      .select('*')
      .eq('study_id', studyId);

    if (startDate) query = query.gte('timestamp', startDate);
    if (endDate) query = query.lte('timestamp', endDate);

    const { data: auditLogs, error } = await query;

    if (error) throw error;

    const verificationResults = {
      totalRecords: auditLogs.length,
      verifiedRecords: 0,
      corruptedRecords: [],
      missingChecksums: 0
    };

    // Verify each record's checksum
    for (const log of auditLogs) {
      if (!log.checksum) {
        verificationResults.missingChecksums++;
        continue;
      }

      const expectedChecksum = generateChecksum({
        tableName: log.table_name,
        recordId: log.record_id,
        action: log.action,
        oldValue: log.old_value,
        newValue: log.new_value,
        userId: log.user_id,
        timestamp: log.timestamp
      });

      if (log.checksum === expectedChecksum) {
        verificationResults.verifiedRecords++;
      } else {
        verificationResults.corruptedRecords.push({
          id: log.id,
          timestamp: log.timestamp,
          expectedChecksum,
          actualChecksum: log.checksum
        });
      }
    }

    const integrityScore = verificationResults.totalRecords > 0 ? 
      (verificationResults.verifiedRecords / verificationResults.totalRecords * 100).toFixed(2) : 100;

    res.json({
      verificationResults,
      integrityScore: `${integrityScore}%`,
      status: verificationResults.corruptedRecords.length === 0 ? 'INTACT' : 'COMPROMISED',
      verificationDate: new Date().toISOString()
    });

  } catch (error) {
    console.error('Integrity verification error:', error);
    res.status(500).json({ error: 'Failed to verify audit log integrity' });
  }
});

/**
 * Export audit trail
 */
router.get('/export/:studyId', authenticateToken, async (req, res) => {
  try {
    const { studyId } = req.params;
    const { format = 'json', startDate, endDate } = req.query;

    let query = supabase
      .from('edc_audit_trail')
      .select(`
        *,
        edc_users (first_name, last_name, username, role),
        edc_studies (study_name, study_code)
      `)
      .eq('study_id', studyId)
      .order('timestamp', { ascending: true });

    if (startDate) query = query.gte('timestamp', startDate);
    if (endDate) query = query.lte('timestamp', endDate);

    const { data: auditLogs, error } = await query;

    if (error) throw error;

    // Log the export action
    await createAuditLog({
      tableName: 'edc_audit_trail',
      recordId: studyId,
      action: AUDIT_ACTIONS.EXPORT,
      userId: req.user.userId,
      ipAddress: req.ip,
      reasonForChange: 'Audit trail export requested',
      riskLevel: RISK_LEVELS.HIGH,
      studyId,
      metadata: { exportFormat: format, recordCount: auditLogs.length }
    });

    if (format === 'csv') {
      // Convert to CSV format
      const csvData = convertToCSV(auditLogs);
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', `attachment; filename="audit_trail_${studyId}.csv"`);
      res.send(csvData);
    } else {
      // Return JSON format
      res.json({
        exportInfo: {
          studyId,
          exportDate: new Date().toISOString(),
          exportedBy: req.user.userId,
          recordCount: auditLogs.length,
          format
        },
        auditTrail: auditLogs
      });
    }

  } catch (error) {
    console.error('Export audit trail error:', error);
    res.status(500).json({ error: 'Failed to export audit trail' });
  }
});

/**
 * Convert audit logs to CSV format
 */
function convertToCSV(auditLogs) {
  const headers = [
    'Timestamp', 'User', 'Action', 'Table', 'Record ID', 
    'Field', 'Old Value', 'New Value', 'Reason', 'IP Address',
    'Risk Level', 'E-Signature'
  ];

  const rows = auditLogs.map(log => [
    log.timestamp,
    log.edc_users?.username || log.user_id,
    log.action,
    log.table_name,
    log.record_id,
    log.field_name || '',
    log.old_value || '',
    log.new_value || '',
    log.reason_for_change || '',
    log.ip_address || '',
    log.risk_level,
    log.e_signature ? 'Signed' : 'Not Signed'
  ]);

  return [headers, ...rows]
    .map(row => row.map(field => `"${field}"`).join(','))
    .join('\n');
}

// Export middleware and helper functions
module.exports = {
  router,
  createAuditLog,
  auditMiddleware,
  AUDIT_ACTIONS,
  DATA_CATEGORIES,
  RISK_LEVELS
};