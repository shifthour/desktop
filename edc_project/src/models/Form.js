const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/database');

const Form = sequelize.define('Form', {
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
  name: {
    type: DataTypes.STRING,
    allowNull: false
  },
  displayName: {
    type: DataTypes.STRING,
    allowNull: false
  },
  description: {
    type: DataTypes.TEXT
  },
  category: {
    type: DataTypes.STRING
  },
  version: {
    type: DataTypes.STRING,
    allowNull: false,
    defaultValue: '1.0.0'
  },
  status: {
    type: DataTypes.ENUM('DRAFT', 'UAT', 'PRODUCTION', 'DEPRECATED'),
    defaultValue: 'DRAFT'
  },
  visitType: {
    type: DataTypes.STRING
  },
  sequence: {
    type: DataTypes.INTEGER,
    defaultValue: 0
  },
  isRepeating: {
    type: DataTypes.BOOLEAN,
    defaultValue: false
  },
  maxRepetitions: {
    type: DataTypes.INTEGER
  },
  fields: {
    type: DataTypes.JSONB,
    defaultValue: []
  },
  layout: {
    type: DataTypes.JSONB,
    defaultValue: {
      sections: [],
      columns: 1
    }
  },
  validationRules: {
    type: DataTypes.JSONB,
    defaultValue: []
  },
  editChecks: {
    type: DataTypes.JSONB,
    defaultValue: []
  },
  conditionalLogic: {
    type: DataTypes.JSONB,
    defaultValue: []
  },
  calculatedFields: {
    type: DataTypes.JSONB,
    defaultValue: []
  },
  instructions: {
    type: DataTypes.TEXT
  },
  attachmentSettings: {
    type: DataTypes.JSONB,
    defaultValue: {
      allowAttachments: false,
      requiredAttachments: [],
      maxFileSize: 10485760,
      allowedFileTypes: []
    }
  },
  completionRequirements: {
    type: DataTypes.JSONB,
    defaultValue: {
      requiredFields: [],
      requiredSections: []
    }
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
  promotedBy: {
    type: DataTypes.UUID,
    references: {
      model: 'users',
      key: 'id'
    }
  },
  promotedAt: {
    type: DataTypes.DATE
  },
  uatTestedBy: {
    type: DataTypes.UUID,
    references: {
      model: 'users',
      key: 'id'
    }
  },
  uatTestedAt: {
    type: DataTypes.DATE
  },
  uatApprovedBy: {
    type: DataTypes.UUID,
    references: {
      model: 'users',
      key: 'id'
    }
  },
  uatApprovedAt: {
    type: DataTypes.DATE
  },
  metadata: {
    type: DataTypes.JSONB,
    defaultValue: {}
  }
}, {
  indexes: [
    {
      fields: ['study_id', 'name', 'version']
    }
  ]
});

module.exports = Form;