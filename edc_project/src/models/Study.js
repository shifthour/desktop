const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/database');

const Study = sequelize.define('Study', {
  id: {
    type: DataTypes.UUID,
    defaultValue: DataTypes.UUIDV4,
    primaryKey: true
  },
  protocolNumber: {
    type: DataTypes.STRING,
    allowNull: false,
    unique: true
  },
  title: {
    type: DataTypes.STRING,
    allowNull: false
  },
  shortTitle: {
    type: DataTypes.STRING
  },
  description: {
    type: DataTypes.TEXT
  },
  sponsor: {
    type: DataTypes.STRING,
    allowNull: false
  },
  therapeuticArea: {
    type: DataTypes.STRING
  },
  indication: {
    type: DataTypes.STRING
  },
  phase: {
    type: DataTypes.ENUM('STARTUP', 'CONDUCT', 'CLOSEOUT'),
    defaultValue: 'STARTUP'
  },
  studyType: {
    type: DataTypes.ENUM('INTERVENTIONAL', 'OBSERVATIONAL', 'EXPANDED_ACCESS'),
    allowNull: false
  },
  startDate: {
    type: DataTypes.DATE
  },
  endDate: {
    type: DataTypes.DATE
  },
  enrollmentTarget: {
    type: DataTypes.INTEGER
  },
  currentEnrollment: {
    type: DataTypes.INTEGER,
    defaultValue: 0
  },
  status: {
    type: DataTypes.ENUM('PLANNING', 'ACTIVE', 'SUSPENDED', 'TERMINATED', 'COMPLETED', 'LOCKED'),
    defaultValue: 'PLANNING'
  },
  regulatoryApprovalDate: {
    type: DataTypes.DATE
  },
  regulatoryApprovalNumber: {
    type: DataTypes.STRING
  },
  irbApprovalDate: {
    type: DataTypes.DATE
  },
  irbApprovalNumber: {
    type: DataTypes.STRING
  },
  primaryInvestigatorId: {
    type: DataTypes.UUID,
    references: {
      model: 'users',
      key: 'id'
    }
  },
  studyManagerId: {
    type: DataTypes.UUID,
    references: {
      model: 'users',
      key: 'id'
    }
  },
  dataManagerId: {
    type: DataTypes.UUID,
    references: {
      model: 'users',
      key: 'id'
    }
  },
  settings: {
    type: DataTypes.JSONB,
    defaultValue: {
      enableMfa: false,
      requireSourceDocuments: true,
      autoQueryGeneration: true,
      dataEntryRestrictions: {},
      exportSettings: {},
      notificationSettings: {}
    }
  },
  visitSchedule: {
    type: DataTypes.JSONB,
    defaultValue: []
  },
  inclusionCriteria: {
    type: DataTypes.JSONB,
    defaultValue: []
  },
  exclusionCriteria: {
    type: DataTypes.JSONB,
    defaultValue: []
  },
  metadata: {
    type: DataTypes.JSONB,
    defaultValue: {}
  },
  isLocked: {
    type: DataTypes.BOOLEAN,
    defaultValue: false
  },
  lockedAt: {
    type: DataTypes.DATE
  },
  lockedBy: {
    type: DataTypes.UUID,
    references: {
      model: 'users',
      key: 'id'
    }
  }
});

module.exports = Study;