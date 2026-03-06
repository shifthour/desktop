const { AuditTrail } = require('../models');
const crypto = require('crypto');

class AuditService {
  static async createAuditLog(data) {
    try {
      const auditEntry = await AuditTrail.create({
        studyId: data.studyId,
        entityType: data.entityType,
        entityId: data.entityId,
        action: data.action,
        userId: data.userId,
        userName: data.userName,
        userRole: data.userRole,
        timestamp: new Date(),
        ipAddress: data.ipAddress,
        userAgent: data.userAgent,
        sessionId: data.sessionId,
        fieldName: data.fieldName,
        oldValue: data.oldValue,
        newValue: data.newValue,
        reasonForChange: data.reasonForChange,
        changeDetails: data.changeDetails || {},
        digitalSignature: data.digitalSignature,
        signatureMeaning: data.signatureMeaning,
        signatureTimestamp: data.signatureTimestamp,
        relatedEntities: data.relatedEntities || {},
        systemMetadata: {
          serverVersion: process.env.npm_package_version || '1.0.0',
          clientVersion: data.clientVersion,
          apiVersion: '1.0.0'
        },
        complianceFlags: {
          cfr21Part11: true,
          gdpr: data.gdprCompliant || false,
          hipaa: data.hipaaCompliant || false
        },
        isSystemAction: data.isSystemAction || false,
        parentAuditId: data.parentAuditId,
        metadata: data.metadata || {}
      });

      return auditEntry;
    } catch (error) {
      console.error('Failed to create audit log:', error);
      throw error;
    }
  }

  static async logDataEntry(req, entityType, entityId, oldData, newData) {
    const changes = this.detectChanges(oldData, newData);
    
    for (const change of changes) {
      await this.createAuditLog({
        studyId: req.body.studyId || req.params.studyId,
        entityType,
        entityId,
        action: oldData ? 'UPDATE' : 'CREATE',
        userId: req.user.id,
        userName: `${req.user.firstName} ${req.user.lastName}`,
        userRole: req.user.role,
        ipAddress: req.ip,
        userAgent: req.get('user-agent'),
        sessionId: req.sessionID || req.token,
        fieldName: change.field,
        oldValue: change.oldValue,
        newValue: change.newValue,
        reasonForChange: req.body.reasonForChange || 'Data entry',
        changeDetails: {
          formId: req.body.formId,
          visitId: req.body.visitId,
          subjectId: req.body.subjectId
        }
      });
    }
  }

  static async logQuery(req, query, action) {
    await this.createAuditLog({
      studyId: query.studyId,
      entityType: 'Query',
      entityId: query.id,
      action,
      userId: req.user.id,
      userName: `${req.user.firstName} ${req.user.lastName}`,
      userRole: req.user.role,
      ipAddress: req.ip,
      userAgent: req.get('user-agent'),
      sessionId: req.sessionID || req.token,
      changeDetails: {
        queryNumber: query.queryNumber,
        status: query.status,
        severity: query.severity,
        subjectId: query.subjectId,
        formId: query.formId
      }
    });
  }

  static async logFormPromotion(req, form, fromStatus, toStatus) {
    await this.createAuditLog({
      studyId: form.studyId,
      entityType: 'Form',
      entityId: form.id,
      action: 'APPROVE',
      userId: req.user.id,
      userName: `${req.user.firstName} ${req.user.lastName}`,
      userRole: req.user.role,
      ipAddress: req.ip,
      userAgent: req.get('user-agent'),
      sessionId: req.sessionID || req.token,
      fieldName: 'status',
      oldValue: fromStatus,
      newValue: toStatus,
      reasonForChange: `Form promoted from ${fromStatus} to ${toStatus}`,
      digitalSignature: req.eSignature?.signature,
      signatureMeaning: req.eSignature?.meaning,
      signatureTimestamp: req.eSignature?.timestamp
    });
  }

  static async logDataLock(req, entityType, entityId, lockStatus) {
    await this.createAuditLog({
      studyId: req.body.studyId || req.params.studyId,
      entityType,
      entityId,
      action: lockStatus ? 'LOCK' : 'UNLOCK',
      userId: req.user.id,
      userName: `${req.user.firstName} ${req.user.lastName}`,
      userRole: req.user.role,
      ipAddress: req.ip,
      userAgent: req.get('user-agent'),
      sessionId: req.sessionID || req.token,
      reasonForChange: req.body.reason || `Data ${lockStatus ? 'locked' : 'unlocked'}`,
      digitalSignature: req.eSignature?.signature,
      signatureMeaning: req.eSignature?.meaning,
      signatureTimestamp: req.eSignature?.timestamp
    });
  }

  static async logExport(req, exportDetails) {
    await this.createAuditLog({
      studyId: exportDetails.studyId,
      entityType: 'Export',
      entityId: exportDetails.exportId,
      action: 'EXPORT',
      userId: req.user.id,
      userName: `${req.user.firstName} ${req.user.lastName}`,
      userRole: req.user.role,
      ipAddress: req.ip,
      userAgent: req.get('user-agent'),
      sessionId: req.sessionID || req.token,
      changeDetails: {
        format: exportDetails.format,
        includeAuditTrail: exportDetails.includeAuditTrail,
        filters: exportDetails.filters,
        recordCount: exportDetails.recordCount,
        fileName: exportDetails.fileName
      }
    });
  }

  static async logUnauthorizedAccess(req, attemptedPermission = null) {
    await this.createAuditLog({
      studyId: req.body.studyId || req.params.studyId || 'SYSTEM',
      entityType: 'SecurityEvent',
      entityId: crypto.randomUUID(),
      action: 'REJECT',
      userId: req.user?.id || 'UNKNOWN',
      userName: req.user ? `${req.user.firstName} ${req.user.lastName}` : 'Unknown User',
      userRole: req.user?.role || 'UNKNOWN',
      ipAddress: req.ip,
      userAgent: req.get('user-agent'),
      sessionId: req.sessionID || req.token || 'NO_SESSION',
      changeDetails: {
        attemptedAction: req.method + ' ' + req.path,
        attemptedPermission,
        reason: 'Unauthorized access attempt'
      },
      isSystemAction: true
    });
  }

  static async logFailedSignature(req) {
    await this.createAuditLog({
      studyId: req.body.studyId || req.params.studyId || 'SYSTEM',
      entityType: 'SecurityEvent',
      entityId: crypto.randomUUID(),
      action: 'REJECT',
      userId: req.user.id,
      userName: `${req.user.firstName} ${req.user.lastName}`,
      userRole: req.user.role,
      ipAddress: req.ip,
      userAgent: req.get('user-agent'),
      sessionId: req.sessionID || req.token,
      changeDetails: {
        attemptedAction: 'Electronic Signature',
        reason: 'Invalid signature credentials'
      },
      isSystemAction: true
    });
  }

  static async logLogin(user, req, success = true) {
    await this.createAuditLog({
      studyId: 'SYSTEM',
      entityType: 'Authentication',
      entityId: user?.id || crypto.randomUUID(),
      action: success ? 'CREATE' : 'REJECT',
      userId: user?.id || 'UNKNOWN',
      userName: user ? `${user.firstName} ${user.lastName}` : req.body.username || 'Unknown',
      userRole: user?.role || 'UNKNOWN',
      ipAddress: req.ip,
      userAgent: req.get('user-agent'),
      sessionId: req.sessionID || 'NO_SESSION',
      changeDetails: {
        event: success ? 'Login successful' : 'Login failed',
        username: req.body.username || req.body.email
      },
      isSystemAction: true
    });
  }

  static async logLogout(user, req) {
    await this.createAuditLog({
      studyId: 'SYSTEM',
      entityType: 'Authentication',
      entityId: user.id,
      action: 'DELETE',
      userId: user.id,
      userName: `${user.firstName} ${user.lastName}`,
      userRole: user.role,
      ipAddress: req.ip,
      userAgent: req.get('user-agent'),
      sessionId: req.sessionID || req.token,
      changeDetails: {
        event: 'Logout'
      },
      isSystemAction: true
    });
  }

  static detectChanges(oldData, newData) {
    const changes = [];
    
    if (!oldData) {
      Object.keys(newData).forEach(key => {
        if (newData[key] !== null && newData[key] !== undefined) {
          changes.push({
            field: key,
            oldValue: null,
            newValue: String(newData[key])
          });
        }
      });
    } else {
      Object.keys(newData).forEach(key => {
        if (oldData[key] !== newData[key]) {
          changes.push({
            field: key,
            oldValue: oldData[key] ? String(oldData[key]) : null,
            newValue: newData[key] ? String(newData[key]) : null
          });
        }
      });
    }
    
    return changes;
  }

  static async getAuditTrail(filters = {}) {
    const where = {};
    
    if (filters.studyId) where.studyId = filters.studyId;
    if (filters.entityType) where.entityType = filters.entityType;
    if (filters.entityId) where.entityId = filters.entityId;
    if (filters.userId) where.userId = filters.userId;
    if (filters.action) where.action = filters.action;
    
    if (filters.startDate || filters.endDate) {
      where.timestamp = {};
      if (filters.startDate) where.timestamp.$gte = filters.startDate;
      if (filters.endDate) where.timestamp.$lte = filters.endDate;
    }
    
    const auditTrail = await AuditTrail.findAll({
      where,
      order: [['timestamp', 'DESC']],
      limit: filters.limit || 1000
    });
    
    return auditTrail;
  }

  static async verifyAuditIntegrity(auditId) {
    const audit = await AuditTrail.findByPk(auditId);
    
    if (!audit) {
      throw new Error('Audit record not found');
    }
    
    const dataString = JSON.stringify({
      entityType: audit.entityType,
      entityId: audit.entityId,
      action: audit.action,
      userId: audit.userId,
      oldValue: audit.oldValue,
      newValue: audit.newValue,
      timestamp: audit.timestamp
    });
    
    const hash = crypto.createHash('sha256');
    hash.update(dataString);
    const calculatedHash = hash.digest('hex');
    
    return {
      isValid: calculatedHash === audit.dataIntegrity,
      storedHash: audit.dataIntegrity,
      calculatedHash
    };
  }
}

module.exports = AuditService;