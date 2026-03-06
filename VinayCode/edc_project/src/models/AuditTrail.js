const { DataTypes } = require('sequelize');
const { auditSequelize } = require('../config/database');

const AuditTrail = auditSequelize.define('AuditTrail', {
  id: {
    type: DataTypes.UUID,
    defaultValue: DataTypes.UUIDV4,
    primaryKey: true
  },
  studyId: {
    type: DataTypes.UUID,
    allowNull: false
  },
  entityType: {
    type: DataTypes.STRING,
    allowNull: false
  },
  entityId: {
    type: DataTypes.UUID,
    allowNull: false
  },
  action: {
    type: DataTypes.ENUM('CREATE', 'UPDATE', 'DELETE', 'VIEW', 'EXPORT', 'LOCK', 'UNLOCK', 'SIGN', 'APPROVE', 'REJECT', 'QUERY_RAISED', 'QUERY_ANSWERED', 'QUERY_CLOSED'),
    allowNull: false
  },
  userId: {
    type: DataTypes.UUID,
    allowNull: false
  },
  userName: {
    type: DataTypes.STRING,
    allowNull: false
  },
  userRole: {
    type: DataTypes.STRING,
    allowNull: false
  },
  timestamp: {
    type: DataTypes.DATE,
    allowNull: false,
    defaultValue: DataTypes.NOW
  },
  ipAddress: {
    type: DataTypes.STRING,
    allowNull: false
  },
  userAgent: {
    type: DataTypes.STRING
  },
  sessionId: {
    type: DataTypes.STRING
  },
  fieldName: {
    type: DataTypes.STRING
  },
  oldValue: {
    type: DataTypes.TEXT
  },
  newValue: {
    type: DataTypes.TEXT
  },
  reasonForChange: {
    type: DataTypes.TEXT
  },
  changeDetails: {
    type: DataTypes.JSONB,
    defaultValue: {}
  },
  dataIntegrity: {
    type: DataTypes.STRING
  },
  digitalSignature: {
    type: DataTypes.TEXT
  },
  signatureMeaning: {
    type: DataTypes.TEXT
  },
  signatureTimestamp: {
    type: DataTypes.DATE
  },
  relatedEntities: {
    type: DataTypes.JSONB,
    defaultValue: {}
  },
  systemMetadata: {
    type: DataTypes.JSONB,
    defaultValue: {
      serverVersion: null,
      clientVersion: null,
      apiVersion: null
    }
  },
  complianceFlags: {
    type: DataTypes.JSONB,
    defaultValue: {
      cfr21Part11: true,
      gdpr: false,
      hipaa: false
    }
  },
  isSystemAction: {
    type: DataTypes.BOOLEAN,
    defaultValue: false
  },
  parentAuditId: {
    type: DataTypes.UUID
  },
  metadata: {
    type: DataTypes.JSONB,
    defaultValue: {}
  }
}, {
  tableName: 'audit_trails',
  timestamps: false,
  indexes: [
    {
      fields: ['study_id', 'entity_type', 'entity_id']
    },
    {
      fields: ['user_id', 'timestamp']
    },
    {
      fields: ['action', 'timestamp']
    },
    {
      fields: ['timestamp']
    }
  ]
});

AuditTrail.addHook('beforeCreate', async (audit) => {
  const crypto = require('crypto');
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
  audit.dataIntegrity = hash.digest('hex');
});

module.exports = AuditTrail;