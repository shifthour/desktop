const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/database');

const Visit = sequelize.define('Visit', {
  id: {
    type: DataTypes.UUID,
    defaultValue: DataTypes.UUIDV4,
    primaryKey: true
  },
  studyId: {
    type: DataTypes.UUID,
    allowNull: false,
    references: {
      model: 'studies',
      key: 'id'
    }
  },
  subjectId: {
    type: DataTypes.UUID,
    allowNull: false,
    references: {
      model: 'subjects',
      key: 'id'
    }
  },
  siteId: {
    type: DataTypes.UUID,
    allowNull: false,
    references: {
      model: 'sites',
      key: 'id'
    }
  },
  visitName: {
    type: DataTypes.STRING,
    allowNull: false
  },
  visitLabel: {
    type: DataTypes.STRING,
    allowNull: false
  },
  visitType: {
    type: DataTypes.ENUM('SCREENING', 'BASELINE', 'TREATMENT', 'FOLLOW_UP', 'UNSCHEDULED', 'EARLY_TERMINATION'),
    allowNull: false
  },
  visitNumber: {
    type: DataTypes.INTEGER
  },
  status: {
    type: DataTypes.ENUM('PLANNED', 'SCHEDULED', 'IN_PROGRESS', 'COMPLETED', 'MISSED', 'UNSCHEDULED'),
    defaultValue: 'PLANNED'
  },
  plannedDate: {
    type: DataTypes.DATE
  },
  scheduledDate: {
    type: DataTypes.DATE
  },
  actualDate: {
    type: DataTypes.DATE
  },
  windowStart: {
    type: DataTypes.DATE
  },
  windowEnd: {
    type: DataTypes.DATE
  },
  isOutOfWindow: {
    type: DataTypes.BOOLEAN,
    defaultValue: false
  },
  reasonForUnscheduled: {
    type: DataTypes.TEXT
  },
  reasonForMissed: {
    type: DataTypes.TEXT
  },
  forms: {
    type: DataTypes.JSONB,
    defaultValue: []
  },
  requiredForms: {
    type: DataTypes.JSONB,
    defaultValue: []
  },
  completedForms: {
    type: DataTypes.JSONB,
    defaultValue: []
  },
  completionPercentage: {
    type: DataTypes.DECIMAL(5, 2),
    defaultValue: 0
  },
  procedures: {
    type: DataTypes.JSONB,
    defaultValue: []
  },
  assessments: {
    type: DataTypes.JSONB,
    defaultValue: []
  },
  notes: {
    type: DataTypes.TEXT
  },
  createdBy: {
    type: DataTypes.UUID,
    references: {
      model: 'users',
      key: 'id'
    }
  },
  lastModifiedBy: {
    type: DataTypes.UUID,
    references: {
      model: 'users',
      key: 'id'
    }
  },
  metadata: {
    type: DataTypes.JSONB,
    defaultValue: {}
  }
}, {
  indexes: [
    {
      fields: ['study_id', 'subject_id', 'visit_number']
    },
    {
      fields: ['site_id', 'status']
    }
  ]
});

module.exports = Visit;