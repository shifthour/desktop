const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/database');

const Query = sequelize.define('Query', {
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
  siteId: {
    type: DataTypes.UUID,
    allowNull: false,
    references: {
      model: 'sites',
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
  visitId: {
    type: DataTypes.UUID,
    references: {
      model: 'visits',
      key: 'id'
    }
  },
  formId: {
    type: DataTypes.UUID,
    references: {
      model: 'forms',
      key: 'id'
    }
  },
  formDataId: {
    type: DataTypes.UUID,
    references: {
      model: 'form_data',
      key: 'id'
    }
  },
  queryNumber: {
    type: DataTypes.STRING,
    allowNull: false,
    unique: true
  },
  fieldName: {
    type: DataTypes.STRING
  },
  fieldValue: {
    type: DataTypes.TEXT
  },
  expectedValue: {
    type: DataTypes.TEXT
  },
  queryType: {
    type: DataTypes.ENUM('AUTO', 'MANUAL', 'SYSTEM'),
    allowNull: false
  },
  category: {
    type: DataTypes.STRING
  },
  severity: {
    type: DataTypes.ENUM('HARD_ERROR', 'WARNING', 'NOTE'),
    defaultValue: 'WARNING'
  },
  status: {
    type: DataTypes.ENUM('OPEN', 'ASSIGNED', 'SITE_ANSWERED', 'DM_REVIEWED', 'RESOLVED', 'CLOSED', 'ACCEPTED_AS_VALID'),
    defaultValue: 'OPEN'
  },
  message: {
    type: DataTypes.TEXT,
    allowNull: false
  },
  ruleId: {
    type: DataTypes.STRING
  },
  ruleName: {
    type: DataTypes.STRING
  },
  raisedBy: {
    type: DataTypes.UUID,
    allowNull: false,
    references: {
      model: 'users',
      key: 'id'
    }
  },
  raisedAt: {
    type: DataTypes.DATE,
    allowNull: false
  },
  assignedTo: {
    type: DataTypes.UUID,
    references: {
      model: 'users',
      key: 'id'
    }
  },
  assignedAt: {
    type: DataTypes.DATE
  },
  response: {
    type: DataTypes.TEXT
  },
  respondedBy: {
    type: DataTypes.UUID,
    references: {
      model: 'users',
      key: 'id'
    }
  },
  respondedAt: {
    type: DataTypes.DATE
  },
  reviewedBy: {
    type: DataTypes.UUID,
    references: {
      model: 'users',
      key: 'id'
    }
  },
  reviewedAt: {
    type: DataTypes.DATE
  },
  reviewNotes: {
    type: DataTypes.TEXT
  },
  closedBy: {
    type: DataTypes.UUID,
    references: {
      model: 'users',
      key: 'id'
    }
  },
  closedAt: {
    type: DataTypes.DATE
  },
  closureReason: {
    type: DataTypes.TEXT
  },
  acceptanceJustification: {
    type: DataTypes.TEXT
  },
  daysOpen: {
    type: DataTypes.INTEGER,
    defaultValue: 0
  },
  slaBreached: {
    type: DataTypes.BOOLEAN,
    defaultValue: false
  },
  slaDueDate: {
    type: DataTypes.DATE
  },
  priority: {
    type: DataTypes.ENUM('HIGH', 'MEDIUM', 'LOW'),
    defaultValue: 'MEDIUM'
  },
  history: {
    type: DataTypes.JSONB,
    defaultValue: []
  },
  attachments: {
    type: DataTypes.JSONB,
    defaultValue: []
  },
  metadata: {
    type: DataTypes.JSONB,
    defaultValue: {}
  }
}, {
  indexes: [
    {
      fields: ['study_id', 'site_id', 'status']
    },
    {
      fields: ['subject_id', 'status']
    },
    {
      fields: ['form_data_id']
    }
  ]
});

module.exports = Query;