const express = require('express');
const router = express.Router();
const { authenticate, authorize, checkPermission, requireESignature } = require('../middleware/auth');
const { ROLES, PERMISSIONS, FORM_STATUS } = require('../config/constants');
const { body, validationResult } = require('express-validator');
const { Form, Study, User, AuditTrail } = require('../models');
const AuditService = require('../services/auditService');
const { AppError } = require('../middleware/errorHandler');

// Get all forms for a study
router.get('/', 
  authenticate, 
  checkPermission(PERMISSIONS.VIEW_FORM),
  async (req, res, next) => {
    try {
      const { studyId, status, version } = req.query;
      
      const where = {};
      if (studyId) where.studyId = studyId;
      if (status) where.status = status;
      if (version) where.version = version;

      const forms = await Form.findAll({
        where,
        include: [
          { model: User, as: 'creator', attributes: ['id', 'firstName', 'lastName'] },
          { model: User, as: 'modifier', attributes: ['id', 'firstName', 'lastName'] }
        ],
        order: [['createdAt', 'DESC']]
      });

      res.json(forms);
    } catch (error) {
      next(error);
    }
  }
);

// Get single form by ID
router.get('/:id',
  authenticate,
  checkPermission(PERMISSIONS.VIEW_FORM),
  async (req, res, next) => {
    try {
      const form = await Form.findByPk(req.params.id, {
        include: [
          { model: User, as: 'creator', attributes: ['id', 'firstName', 'lastName'] },
          { model: User, as: 'modifier', attributes: ['id', 'firstName', 'lastName'] },
          { model: User, as: 'promoter', attributes: ['id', 'firstName', 'lastName'] }
        ]
      });

      if (!form) {
        return next(new AppError('Form not found', 404));
      }

      // Log form access
      await AuditService.createAuditLog({
        studyId: form.studyId,
        entityType: 'Form',
        entityId: form.id,
        action: 'VIEW',
        userId: req.user.id,
        userName: `${req.user.firstName} ${req.user.lastName}`,
        userRole: req.user.role,
        ipAddress: req.ip,
        userAgent: req.get('user-agent')
      });

      res.json(form);
    } catch (error) {
      next(error);
    }
  }
);

// Create new form
router.post('/',
  authenticate,
  checkPermission(PERMISSIONS.CREATE_FORM),
  [
    body('studyId').isUUID().withMessage('Valid study ID required'),
    body('name').trim().notEmpty().withMessage('Form name is required'),
    body('displayName').trim().notEmpty().withMessage('Display name is required'),
    body('version').matches(/^\d+\.\d+\.\d+$/).withMessage('Version must be in format X.Y.Z'),
    body('sections').isArray().withMessage('Sections must be an array'),
    body('fields').isObject().withMessage('Fields must be an object')
  ],
  async (req, res, next) => {
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({ errors: errors.array() });
      }

      const {
        studyId,
        name,
        displayName,
        description,
        category,
        version,
        visitType,
        sections,
        fields,
        layout,
        validationRules,
        editChecks,
        conditionalLogic,
        calculatedFields,
        instructions
      } = req.body;

      // Check if form name already exists for this study
      const existingForm = await Form.findOne({
        where: {
          studyId,
          name,
          version
        }
      });

      if (existingForm) {
        return next(new AppError('Form with this name and version already exists', 400));
      }

      // Create the form
      const form = await Form.create({
        studyId,
        name,
        displayName,
        description,
        category,
        version,
        status: FORM_STATUS.DRAFT,
        visitType,
        fields: sections, // Store sections as fields in JSONB
        layout,
        validationRules,
        editChecks,
        conditionalLogic,
        calculatedFields,
        instructions,
        createdBy: req.user.id,
        lastModifiedBy: req.user.id
      });

      // Log form creation
      await AuditService.createAuditLog({
        studyId,
        entityType: 'Form',
        entityId: form.id,
        action: 'CREATE',
        userId: req.user.id,
        userName: `${req.user.firstName} ${req.user.lastName}`,
        userRole: req.user.role,
        ipAddress: req.ip,
        userAgent: req.get('user-agent'),
        changeDetails: {
          formName: name,
          version,
          status: FORM_STATUS.DRAFT
        }
      });

      res.status(201).json(form);
    } catch (error) {
      next(error);
    }
  }
);

// Update form
router.put('/:id',
  authenticate,
  checkPermission(PERMISSIONS.UPDATE_FORM),
  async (req, res, next) => {
    try {
      const form = await Form.findByPk(req.params.id);

      if (!form) {
        return next(new AppError('Form not found', 404));
      }

      // Can't update production forms directly
      if (form.status === FORM_STATUS.PRODUCTION) {
        return next(new AppError('Cannot modify production forms. Create a new version instead.', 403));
      }

      const oldData = form.toJSON();

      // Update the form
      await form.update({
        ...req.body,
        lastModifiedBy: req.user.id
      });

      // Log the update
      await AuditService.logDataEntry(req, 'Form', form.id, oldData, form.toJSON());

      res.json(form);
    } catch (error) {
      next(error);
    }
  }
);

// Delete form (only drafts)
router.delete('/:id',
  authenticate,
  checkPermission(PERMISSIONS.DELETE_FORM),
  async (req, res, next) => {
    try {
      const form = await Form.findByPk(req.params.id);

      if (!form) {
        return next(new AppError('Form not found', 404));
      }

      if (form.status !== FORM_STATUS.DRAFT) {
        return next(new AppError('Only draft forms can be deleted', 403));
      }

      // Log deletion
      await AuditService.createAuditLog({
        studyId: form.studyId,
        entityType: 'Form',
        entityId: form.id,
        action: 'DELETE',
        userId: req.user.id,
        userName: `${req.user.firstName} ${req.user.lastName}`,
        userRole: req.user.role,
        ipAddress: req.ip,
        userAgent: req.get('user-agent'),
        changeDetails: {
          formName: form.name,
          version: form.version
        }
      });

      await form.destroy();

      res.json({ message: 'Form deleted successfully' });
    } catch (error) {
      next(error);
    }
  }
);

// Promote form to UAT or Production
router.post('/:id/promote',
  authenticate,
  checkPermission(PERMISSIONS.PROMOTE_FORM),
  requireESignature,
  [
    body('status').isIn([FORM_STATUS.UAT, FORM_STATUS.PRODUCTION])
      .withMessage('Invalid promotion status')
  ],
  async (req, res, next) => {
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({ errors: errors.array() });
      }

      const form = await Form.findByPk(req.params.id);

      if (!form) {
        return next(new AppError('Form not found', 404));
      }

      const { status } = req.body;
      const oldStatus = form.status;

      // Validation for promotion path
      if (oldStatus === FORM_STATUS.DRAFT && status === FORM_STATUS.PRODUCTION) {
        return next(new AppError('Forms must go through UAT before production', 400));
      }

      if (oldStatus === FORM_STATUS.PRODUCTION) {
        return next(new AppError('Production forms cannot be promoted further', 400));
      }

      // Update form status
      await form.update({
        status,
        promotedBy: req.user.id,
        promotedAt: new Date(),
        ...(status === FORM_STATUS.UAT && {
          uatTestedBy: req.user.id,
          uatTestedAt: new Date()
        }),
        ...(status === FORM_STATUS.PRODUCTION && {
          uatApprovedBy: req.user.id,
          uatApprovedAt: new Date()
        })
      });

      // Log promotion with e-signature
      await AuditService.logFormPromotion(req, form, oldStatus, status);

      res.json({
        message: `Form promoted to ${status} successfully`,
        form
      });
    } catch (error) {
      next(error);
    }
  }
);

// Clone form to new version
router.post('/:id/clone',
  authenticate,
  checkPermission(PERMISSIONS.CREATE_FORM),
  [
    body('version').matches(/^\d+\.\d+\.\d+$/).withMessage('Version must be in format X.Y.Z')
  ],
  async (req, res, next) => {
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({ errors: errors.array() });
      }

      const originalForm = await Form.findByPk(req.params.id);

      if (!originalForm) {
        return next(new AppError('Form not found', 404));
      }

      const { version } = req.body;

      // Check if version already exists
      const existingVersion = await Form.findOne({
        where: {
          studyId: originalForm.studyId,
          name: originalForm.name,
          version
        }
      });

      if (existingVersion) {
        return next(new AppError('Form with this version already exists', 400));
      }

      // Create cloned form
      const clonedForm = await Form.create({
        ...originalForm.toJSON(),
        id: undefined,
        version,
        status: FORM_STATUS.DRAFT,
        createdBy: req.user.id,
        lastModifiedBy: req.user.id,
        promotedBy: null,
        promotedAt: null,
        uatTestedBy: null,
        uatTestedAt: null,
        uatApprovedBy: null,
        uatApprovedAt: null,
        createdAt: undefined,
        updatedAt: undefined
      });

      // Log cloning
      await AuditService.createAuditLog({
        studyId: clonedForm.studyId,
        entityType: 'Form',
        entityId: clonedForm.id,
        action: 'CREATE',
        userId: req.user.id,
        userName: `${req.user.firstName} ${req.user.lastName}`,
        userRole: req.user.role,
        ipAddress: req.ip,
        userAgent: req.get('user-agent'),
        changeDetails: {
          action: 'Cloned from version ' + originalForm.version,
          originalFormId: originalForm.id,
          newVersion: version
        }
      });

      res.status(201).json(clonedForm);
    } catch (error) {
      next(error);
    }
  }
);

// Submit form for UAT testing
router.post('/:id/uat',
  authenticate,
  checkPermission(PERMISSIONS.PROMOTE_FORM),
  async (req, res, next) => {
    try {
      const form = await Form.findByPk(req.params.id);

      if (!form) {
        return next(new AppError('Form not found', 404));
      }

      if (form.status !== FORM_STATUS.DRAFT) {
        return next(new AppError('Only draft forms can be submitted for UAT', 400));
      }

      // Generate UAT test environment
      const testId = `UAT-${form.id}-${Date.now()}`;
      const testUrl = `${process.env.APP_URL}/uat/${testId}`;

      // Update form metadata
      await form.update({
        metadata: {
          ...form.metadata,
          uatTestId: testId,
          uatSubmittedAt: new Date(),
          uatSubmittedBy: req.user.id
        }
      });

      res.json({
        testId,
        testUrl,
        status: 'UAT environment created',
        instructions: 'Share this URL with UAT testers to begin testing'
      });
    } catch (error) {
      next(error);
    }
  }
);

// Export form definition
router.get('/:id/export',
  authenticate,
  checkPermission(PERMISSIONS.VIEW_FORM),
  async (req, res, next) => {
    try {
      const form = await Form.findByPk(req.params.id);

      if (!form) {
        return next(new AppError('Form not found', 404));
      }

      // Log export
      await AuditService.logExport(req, {
        studyId: form.studyId,
        exportId: `FORM-${form.id}-${Date.now()}`,
        format: 'JSON',
        recordCount: 1,
        fileName: `form_${form.name}_v${form.version}.json`
      });

      res.setHeader('Content-Type', 'application/json');
      res.setHeader('Content-Disposition', 
        `attachment; filename="form_${form.name}_v${form.version}.json"`);
      
      res.json(form);
    } catch (error) {
      next(error);
    }
  }
);

// Import form definition
router.post('/import',
  authenticate,
  checkPermission(PERMISSIONS.CREATE_FORM),
  async (req, res, next) => {
    try {
      // In production, use multer for file upload
      const formData = req.body;

      // Validate imported form structure
      if (!formData.name || !formData.version || !formData.fields) {
        return next(new AppError('Invalid form structure', 400));
      }

      // Create imported form
      const form = await Form.create({
        ...formData,
        id: undefined,
        status: FORM_STATUS.DRAFT,
        createdBy: req.user.id,
        lastModifiedBy: req.user.id,
        metadata: {
          ...formData.metadata,
          imported: true,
          importedAt: new Date(),
          importedBy: req.user.id
        }
      });

      res.status(201).json(form);
    } catch (error) {
      next(error);
    }
  }
);

// Test form with sample data
router.post('/:id/test',
  authenticate,
  checkPermission(PERMISSIONS.VIEW_FORM),
  async (req, res, next) => {
    try {
      const form = await Form.findByPk(req.params.id);

      if (!form) {
        return next(new AppError('Form not found', 404));
      }

      const { sampleData } = req.body;

      // Run validation rules
      const validationResults = [];
      
      if (form.validationRules) {
        for (const rule of form.validationRules) {
          // Implement validation logic here
          validationResults.push({
            rule: rule.name,
            passed: true, // Implement actual validation
            message: rule.message
          });
        }
      }

      // Run edit checks
      const editCheckResults = [];
      
      if (form.editChecks) {
        for (const check of form.editChecks) {
          // Implement edit check logic here
          editCheckResults.push({
            check: check.name,
            triggered: false, // Implement actual check
            query: check.queryTemplate
          });
        }
      }

      res.json({
        validationResults,
        editCheckResults,
        overallStatus: 'Test completed',
        timestamp: new Date()
      });
    } catch (error) {
      next(error);
    }
  }
);

module.exports = router;