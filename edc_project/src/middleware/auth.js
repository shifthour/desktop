const jwt = require('jsonwebtoken');
const { User } = require('../models');
const { AppError } = require('./errorHandler');
const AuditService = require('../services/auditService');
const { ROLES, PERMISSIONS } = require('../config/constants');

const authenticate = async (req, res, next) => {
  try {
    let token;

    if (req.headers.authorization && req.headers.authorization.startsWith('Bearer')) {
      token = req.headers.authorization.split(' ')[1];
    } else if (req.cookies?.jwt) {
      token = req.cookies.jwt;
    }

    if (!token) {
      return next(new AppError('Please log in to access this resource', 401));
    }

    const decoded = jwt.verify(token, process.env.JWT_SECRET);

    const user = await User.findByPk(decoded.id, {
      attributes: { exclude: ['password', 'mfaSecret'] }
    });

    if (!user) {
      return next(new AppError('User no longer exists', 401));
    }

    if (!user.isActive) {
      return next(new AppError('Your account has been deactivated', 401));
    }

    if (user.isAccountLocked()) {
      return next(new AppError('Your account is locked. Please contact administrator', 401));
    }

    if (user.isPasswordExpired()) {
      return next(new AppError('Your password has expired. Please reset your password', 401));
    }

    req.user = user;
    req.token = token;
    
    next();
  } catch (error) {
    if (error.name === 'JsonWebTokenError') {
      return next(new AppError('Invalid token. Please log in again', 401));
    }
    if (error.name === 'TokenExpiredError') {
      return next(new AppError('Your token has expired. Please log in again', 401));
    }
    return next(error);
  }
};

const authorize = (...allowedRoles) => {
  return (req, res, next) => {
    if (!req.user) {
      return next(new AppError('You must be logged in to access this resource', 401));
    }

    if (!allowedRoles.includes(req.user.role)) {
      AuditService.logUnauthorizedAccess(req);
      return next(new AppError('You do not have permission to perform this action', 403));
    }

    next();
  };
};

const checkPermission = (permission) => {
  return async (req, res, next) => {
    if (!req.user) {
      return next(new AppError('You must be logged in to access this resource', 401));
    }

    const rolePermissions = getRolePermissions(req.user.role);

    if (!rolePermissions.includes(permission)) {
      await AuditService.logUnauthorizedAccess(req, permission);
      return next(new AppError('You do not have the required permission', 403));
    }

    next();
  };
};

const getRolePermissions = (role) => {
  const permissionMap = {
    [ROLES.STUDY_ADMIN]: [
      PERMISSIONS.CREATE_STUDY, PERMISSIONS.VIEW_STUDY, PERMISSIONS.UPDATE_STUDY, PERMISSIONS.DELETE_STUDY, PERMISSIONS.LOCK_STUDY,
      PERMISSIONS.CREATE_FORM, PERMISSIONS.VIEW_FORM, PERMISSIONS.UPDATE_FORM, PERMISSIONS.DELETE_FORM, PERMISSIONS.PROMOTE_FORM,
      PERMISSIONS.VIEW_DATA, PERMISSIONS.EXPORT_DATA, PERMISSIONS.EXPORT_AUDIT,
      PERMISSIONS.CREATE_USER, PERMISSIONS.VIEW_USER, PERMISSIONS.UPDATE_USER, PERMISSIONS.DELETE_USER,
      PERMISSIONS.SYSTEM_CONFIG, PERMISSIONS.VIEW_AUDIT,
      PERMISSIONS.RAISE_QUERY, PERMISSIONS.VIEW_QUERY, PERMISSIONS.CLOSE_QUERY
    ],
    [ROLES.STUDY_DESIGNER]: [
      PERMISSIONS.CREATE_STUDY, PERMISSIONS.VIEW_STUDY, PERMISSIONS.UPDATE_STUDY,
      PERMISSIONS.CREATE_FORM, PERMISSIONS.VIEW_FORM, PERMISSIONS.UPDATE_FORM, PERMISSIONS.DELETE_FORM, PERMISSIONS.PROMOTE_FORM,
      PERMISSIONS.VIEW_DATA, PERMISSIONS.VIEW_QUERY
    ],
    [ROLES.CRC]: [
      PERMISSIONS.VIEW_STUDY, PERMISSIONS.VIEW_FORM,
      PERMISSIONS.ENTER_DATA, PERMISSIONS.VIEW_DATA, PERMISSIONS.UPDATE_DATA,
      PERMISSIONS.VIEW_QUERY, PERMISSIONS.ANSWER_QUERY
    ],
    [ROLES.PI]: [
      PERMISSIONS.VIEW_STUDY, PERMISSIONS.VIEW_FORM,
      PERMISSIONS.ENTER_DATA, PERMISSIONS.VIEW_DATA, PERMISSIONS.UPDATE_DATA, PERMISSIONS.LOCK_DATA,
      PERMISSIONS.VIEW_QUERY, PERMISSIONS.ANSWER_QUERY
    ],
    [ROLES.DATA_MANAGER]: [
      PERMISSIONS.VIEW_STUDY, PERMISSIONS.VIEW_FORM,
      PERMISSIONS.VIEW_DATA,
      PERMISSIONS.RAISE_QUERY, PERMISSIONS.VIEW_QUERY, PERMISSIONS.CLOSE_QUERY,
      PERMISSIONS.EXPORT_DATA
    ],
    [ROLES.CRA_MONITOR]: [
      PERMISSIONS.VIEW_STUDY, PERMISSIONS.VIEW_FORM,
      PERMISSIONS.VIEW_DATA,
      PERMISSIONS.RAISE_QUERY, PERMISSIONS.VIEW_QUERY
    ],
    [ROLES.READ_ONLY]: [
      PERMISSIONS.VIEW_STUDY, PERMISSIONS.VIEW_FORM, PERMISSIONS.VIEW_DATA, PERMISSIONS.VIEW_QUERY
    ],
    [ROLES.AUDITOR]: [
      PERMISSIONS.VIEW_STUDY, PERMISSIONS.VIEW_FORM, PERMISSIONS.VIEW_DATA, PERMISSIONS.VIEW_QUERY,
      PERMISSIONS.VIEW_AUDIT, PERMISSIONS.EXPORT_AUDIT
    ]
  };

  return permissionMap[role] || [];
};

const checkStudyPhaseAccess = (requiredPhase) => {
  return async (req, res, next) => {
    try {
      const { Study } = require('../models');
      let studyId = req.params.studyId || req.body.studyId || req.query.studyId;
      
      if (!studyId) {
        return next(new AppError('Study ID is required', 400));
      }

      const study = await Study.findByPk(studyId);
      
      if (!study) {
        return next(new AppError('Study not found', 404));
      }

      const phaseRestrictions = {
        STARTUP: {
          [ROLES.CRC]: ['VIEW_DATA', 'ENTER_DATA', 'UPDATE_DATA', 'VIEW_QUERY', 'ANSWER_QUERY'],
          [ROLES.PI]: ['VIEW_DATA', 'ENTER_DATA', 'UPDATE_DATA', 'VIEW_QUERY', 'ANSWER_QUERY'],
          [ROLES.DATA_MANAGER]: ['VIEW_DATA', 'VIEW_QUERY'],
          [ROLES.CRA_MONITOR]: ['VIEW_DATA', 'VIEW_QUERY']
        },
        CONDUCT: {
          [ROLES.CRC]: ['VIEW_DATA', 'ENTER_DATA', 'UPDATE_DATA', 'VIEW_QUERY', 'ANSWER_QUERY'],
          [ROLES.PI]: ['VIEW_DATA', 'ENTER_DATA', 'UPDATE_DATA', 'LOCK_DATA', 'VIEW_QUERY', 'ANSWER_QUERY'],
          [ROLES.DATA_MANAGER]: ['VIEW_DATA', 'RAISE_QUERY', 'VIEW_QUERY', 'CLOSE_QUERY'],
          [ROLES.CRA_MONITOR]: ['VIEW_DATA', 'RAISE_QUERY', 'VIEW_QUERY']
        },
        CLOSEOUT: {
          [ROLES.CRC]: ['VIEW_DATA', 'LOCK_DATA', 'VIEW_QUERY', 'ANSWER_QUERY'],
          [ROLES.PI]: ['VIEW_DATA', 'LOCK_DATA', 'VIEW_QUERY', 'ANSWER_QUERY'],
          [ROLES.DATA_MANAGER]: ['VIEW_DATA', 'VIEW_QUERY', 'CLOSE_QUERY', 'EXPORT_DATA'],
          [ROLES.CRA_MONITOR]: ['VIEW_DATA', 'VIEW_QUERY']
        }
      };

      const userRole = req.user.role;
      const studyPhase = study.phase;
      const allowedPermissions = phaseRestrictions[studyPhase]?.[userRole] || [];

      req.studyPhase = studyPhase;
      req.phasePermissions = allowedPermissions;
      req.study = study;

      next();
    } catch (error) {
      next(error);
    }
  };
};

const requireMFA = async (req, res, next) => {
  if (!req.user.isMfaEnabled) {
    return next(new AppError('MFA is required for this action', 403));
  }
  
  const mfaToken = req.headers['x-mfa-token'] || req.body.mfaToken;
  
  if (!mfaToken) {
    return next(new AppError('MFA token is required', 403));
  }

  // Verify MFA token logic would go here
  // For now, we'll assume it's valid
  
  next();
};

const requireESignature = async (req, res, next) => {
  const signature = req.body.eSignature;
  
  if (!signature || !signature.meaning || !signature.password) {
    return next(new AppError('Electronic signature is required for this action', 400));
  }

  const user = await User.findByPk(req.user.id);
  const isValidPassword = await user.validatePassword(signature.password);
  
  if (!isValidPassword) {
    await AuditService.logFailedSignature(req);
    return next(new AppError('Invalid signature credentials', 401));
  }

  req.eSignature = {
    userId: req.user.id,
    userName: `${req.user.firstName} ${req.user.lastName}`,
    meaning: signature.meaning,
    timestamp: new Date(),
    ipAddress: req.ip
  };

  next();
};

// Simple token authentication for enhanced modules
const authenticateToken = (req, res, next) => {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1]; // Bearer TOKEN

  if (!token) {
    return res.status(401).json({ 
      status: 'error',
      message: 'Access token required' 
    });
  }

  jwt.verify(token, process.env.JWT_SECRET || 'edc-secret-key-2024', (err, user) => {
    if (err) {
      return res.status(403).json({ 
        status: 'error',
        message: 'Invalid or expired token' 
      });
    }
    
    req.user = user;
    next();
  });
};

module.exports = {
  authenticate,
  authorize,
  checkPermission,
  checkStudyPhaseAccess,
  requireMFA,
  requireESignature,
  getRolePermissions,
  authenticateToken
};