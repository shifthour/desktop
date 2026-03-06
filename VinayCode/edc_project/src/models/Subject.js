const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/database');

const Subject = sequelize.define('Subject', {
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
  subjectNumber: {
    type: DataTypes.STRING,
    allowNull: false
  },
  screeningNumber: {
    type: DataTypes.STRING,
    allowNull: false
  },
  randomizationNumber: {
    type: DataTypes.STRING
  },
  status: {
    type: DataTypes.ENUM('SCREENED', 'ENROLLED', 'ACTIVE', 'COMPLETED', 'WITHDRAWN', 'DISCONTINUED'),
    defaultValue: 'SCREENED'
  },
  screeningDate: {
    type: DataTypes.DATE,
    allowNull: false
  },
  enrollmentDate: {
    type: DataTypes.DATE
  },
  completionDate: {
    type: DataTypes.DATE
  },
  withdrawalDate: {
    type: DataTypes.DATE
  },
  withdrawalReason: {
    type: DataTypes.TEXT
  },
  demographics: {
    type: DataTypes.JSONB,
    defaultValue: {
      dateOfBirth: null,
      age: null,
      gender: null,
      race: null,
      ethnicity: null,
      initials: null
    }
  },
  treatmentArm: {
    type: DataTypes.STRING
  },
  stratificationFactors: {
    type: DataTypes.JSONB,
    defaultValue: {}
  },
  medicalHistory: {
    type: DataTypes.JSONB,
    defaultValue: []
  },
  concomitantMedications: {
    type: DataTypes.JSONB,
    defaultValue: []
  },
  informedConsentDate: {
    type: DataTypes.DATE
  },
  informedConsentVersion: {
    type: DataTypes.STRING
  },
  eligibilityStatus: {
    type: DataTypes.ENUM('PENDING', 'ELIGIBLE', 'NOT_ELIGIBLE', 'SCREEN_FAILURE'),
    defaultValue: 'PENDING'
  },
  eligibilityCriteria: {
    type: DataTypes.JSONB,
    defaultValue: {
      inclusion: [],
      exclusion: []
    }
  },
  protocolDeviations: {
    type: DataTypes.JSONB,
    defaultValue: []
  },
  adverseEvents: {
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
  }
}, {
  indexes: [
    {
      unique: true,
      fields: ['study_id', 'subject_number']
    },
    {
      unique: true,
      fields: ['study_id', 'screening_number']
    }
  ]
});

module.exports = Subject;