const { DataTypes } = require('sequelize');
const bcrypt = require('bcryptjs');
const { sequelize } = require('../config/database');

const User = sequelize.define('User', {
  id: {
    type: DataTypes.UUID,
    defaultValue: DataTypes.UUIDV4,
    primaryKey: true
  },
  username: {
    type: DataTypes.STRING,
    allowNull: false,
    unique: true,
    validate: {
      len: [3, 50]
    }
  },
  email: {
    type: DataTypes.STRING,
    allowNull: false,
    unique: true,
    validate: {
      isEmail: true
    }
  },
  password: {
    type: DataTypes.STRING,
    allowNull: false
  },
  firstName: {
    type: DataTypes.STRING,
    allowNull: false
  },
  lastName: {
    type: DataTypes.STRING,
    allowNull: false
  },
  role: {
    type: DataTypes.ENUM('STUDY_ADMIN', 'STUDY_DESIGNER', 'CRC', 'PI', 'DATA_MANAGER', 'CRA_MONITOR', 'READ_ONLY', 'AUDITOR'),
    allowNull: false
  },
  title: {
    type: DataTypes.STRING
  },
  institution: {
    type: DataTypes.STRING
  },
  phone: {
    type: DataTypes.STRING
  },
  isActive: {
    type: DataTypes.BOOLEAN,
    defaultValue: true
  },
  isMfaEnabled: {
    type: DataTypes.BOOLEAN,
    defaultValue: false
  },
  mfaSecret: {
    type: DataTypes.STRING
  },
  lastLogin: {
    type: DataTypes.DATE
  },
  lastPasswordChange: {
    type: DataTypes.DATE
  },
  passwordExpiryDate: {
    type: DataTypes.DATE
  },
  failedLoginAttempts: {
    type: DataTypes.INTEGER,
    defaultValue: 0
  },
  accountLockedUntil: {
    type: DataTypes.DATE
  },
  resetPasswordToken: {
    type: DataTypes.STRING
  },
  resetPasswordExpire: {
    type: DataTypes.DATE
  },
  emailVerified: {
    type: DataTypes.BOOLEAN,
    defaultValue: false
  },
  emailVerificationToken: {
    type: DataTypes.STRING
  },
  preferences: {
    type: DataTypes.JSONB,
    defaultValue: {}
  }
}, {
  hooks: {
    beforeCreate: async (user) => {
      if (user.password) {
        const salt = await bcrypt.genSalt(10);
        user.password = await bcrypt.hash(user.password, salt);
      }
      user.lastPasswordChange = new Date();
      const expiryDays = 90;
      user.passwordExpiryDate = new Date(Date.now() + expiryDays * 24 * 60 * 60 * 1000);
    },
    beforeUpdate: async (user) => {
      if (user.changed('password')) {
        const salt = await bcrypt.genSalt(10);
        user.password = await bcrypt.hash(user.password, salt);
        user.lastPasswordChange = new Date();
        const expiryDays = 90;
        user.passwordExpiryDate = new Date(Date.now() + expiryDays * 24 * 60 * 60 * 1000);
      }
    }
  }
});

User.prototype.validatePassword = async function(password) {
  return await bcrypt.compare(password, this.password);
};

User.prototype.isPasswordExpired = function() {
  return this.passwordExpiryDate && new Date() > this.passwordExpiryDate;
};

User.prototype.isAccountLocked = function() {
  return this.accountLockedUntil && new Date() < this.accountLockedUntil;
};

module.exports = User;