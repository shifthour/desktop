const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/database');

const FormData = sequelize.define('FormData', {
  id: {
    type: DataTypes.UUID,
    defaultValue: DataTypes.UUIDV4,
    primaryKey: true
  },
  formId: {
    type: DataTypes.UUID,
    allowNull: false,
    references: {
      model: 'forms',
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
    allowNull: false,
    references: {
      model: 'visits',
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
  studyId: {
    type: DataTypes.UUID,
    allowNull: false,
    references: {
      model: 'studies',
      key: 'id'
    }
  },
  repetitionNumber: {
    type: DataTypes.INTEGER,
    defaultValue: 1
  },
  data: {
    type: DataTypes.JSONB,
    defaultValue: {}
  },
  status: {
    type: DataTypes.ENUM('IN_PROGRESS', 'SUBMITTED', 'VERIFIED', 'LOCKED'),
    defaultValue: 'IN_PROGRESS'
  },
  completionPercentage: {
    type: DataTypes.DECIMAL(5, 2),
    defaultValue: 0
  },
  validationErrors: {
    type: DataTypes.JSONB,
    defaultValue: []
  },
  hasDiscrepancies: {
    type: DataTypes.BOOLEAN,
    defaultValue: false
  },
  openQueriesCount: {
    type: DataTypes.INTEGER,
    defaultValue: 0
  },
  enteredBy: {
    type: DataTypes.UUID,
    references: {
      model: 'users',
      key: 'id'
    }
  },
  enteredAt: {
    type: DataTypes.DATE
  },
  lastModifiedBy: {
    type: DataTypes.UUID,
    references: {
      model: 'users',
      key: 'id'
    }
  },
  lastModifiedAt: {
    type: DataTypes.DATE
  },
  submittedBy: {
    type: DataTypes.UUID,
    references: {
      model: 'users',
      key: 'id'
    }
  },
  submittedAt: {
    type: DataTypes.DATE
  },
  verifiedBy: {
    type: DataTypes.UUID,
    references: {
      model: 'users',
      key: 'id'
    }
  },
  verifiedAt: {
    type: DataTypes.DATE
  },
  lockedBy: {
    type: DataTypes.UUID,
    references: {
      model: 'users',
      key: 'id'
    }
  },
  lockedAt: {
    type: DataTypes.DATE
  },
  sourceDocuments: {
    type: DataTypes.JSONB,
    defaultValue: []
  },
  changeHistory: {
    type: DataTypes.JSONB,
    defaultValue: []
  },
  signatures: {
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
      unique: true,
      fields: ['form_id', 'subject_id', 'visit_id', 'repetition_number']
    },
    {
      fields: ['study_id', 'site_id', 'status']
    }
  ]
});

module.exports = FormData;