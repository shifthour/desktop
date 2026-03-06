const { DataTypes } = require('sequelize');
const { sequelize } = require('../config/database');

const Site = sequelize.define('Site', {
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
  siteNumber: {
    type: DataTypes.STRING,
    allowNull: false
  },
  name: {
    type: DataTypes.STRING,
    allowNull: false
  },
  institution: {
    type: DataTypes.STRING,
    allowNull: false
  },
  address: {
    type: DataTypes.JSONB,
    defaultValue: {
      street: '',
      city: '',
      state: '',
      country: '',
      postalCode: ''
    }
  },
  principalInvestigatorId: {
    type: DataTypes.UUID,
    references: {
      model: 'users',
      key: 'id'
    }
  },
  coordinatorId: {
    type: DataTypes.UUID,
    references: {
      model: 'users',
      key: 'id'
    }
  },
  status: {
    type: DataTypes.ENUM('PENDING', 'ACTIVE', 'INACTIVE', 'CLOSED'),
    defaultValue: 'PENDING'
  },
  activationDate: {
    type: DataTypes.DATE
  },
  closeoutDate: {
    type: DataTypes.DATE
  },
  enrollmentTarget: {
    type: DataTypes.INTEGER
  },
  currentEnrollment: {
    type: DataTypes.INTEGER,
    defaultValue: 0
  },
  regulatoryDocuments: {
    type: DataTypes.JSONB,
    defaultValue: []
  },
  irbApprovalDate: {
    type: DataTypes.DATE
  },
  irbExpirationDate: {
    type: DataTypes.DATE
  },
  contactInfo: {
    type: DataTypes.JSONB,
    defaultValue: {
      phone: '',
      email: '',
      emergencyContact: ''
    }
  },
  timeZone: {
    type: DataTypes.STRING,
    defaultValue: 'UTC'
  },
  settings: {
    type: DataTypes.JSONB,
    defaultValue: {}
  },
  metadata: {
    type: DataTypes.JSONB,
    defaultValue: {}
  }
}, {
  indexes: [
    {
      unique: true,
      fields: ['study_id', 'site_number']
    }
  ]
});

module.exports = Site;