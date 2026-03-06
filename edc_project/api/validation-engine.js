const express = require('express');
const supabase = require('../src/config/supabase');
const { authenticateToken } = require('../src/middleware/auth');
const router = express.Router();

/**
 * Validation & Edit Check Engine
 * Implements comprehensive validation rules for EDC forms
 */

// Validation rule types
const RULE_TYPES = {
  DATATYPE: 'datatype',
  RANGE: 'range',
  REQUIRED: 'required',
  PATTERN: 'pattern',
  CROSS_FIELD: 'cross_field',
  CROSS_VISIT: 'cross_visit',
  CALCULATED: 'calculated',
  CONDITIONAL: 'conditional',
  UNIT_CONVERSION: 'unit_conversion'
};

// Severity levels for validation failures
const SEVERITY = {
  ERROR: 'error',      // Hard stop - must fix
  WARNING: 'warning',  // Allow save but flagged
  NOTE: 'note'        // Informational only
};

/**
 * Main validation engine class
 */
class ValidationEngine {
  constructor() {
    this.rules = new Map();
    this.customValidators = new Map();
    this.initializeBuiltInRules();
  }

  /**
   * Initialize built-in validation rules
   */
  initializeBuiltInRules() {
    // Datatype validators
    this.addValidator('integer', (value) => {
      if (value === null || value === '') return { valid: true };
      const num = Number(value);
      return {
        valid: Number.isInteger(num),
        message: 'Value must be a whole number'
      };
    });

    this.addValidator('decimal', (value, params) => {
      if (value === null || value === '') return { valid: true };
      const num = Number(value);
      if (isNaN(num)) {
        return { valid: false, message: 'Value must be a number' };
      }
      if (params?.decimalPlaces) {
        const decimals = (value.toString().split('.')[1] || '').length;
        if (decimals > params.decimalPlaces) {
          return { 
            valid: false, 
            message: `Value must have at most ${params.decimalPlaces} decimal places` 
          };
        }
      }
      return { valid: true };
    });

    this.addValidator('date', (value, params) => {
      if (value === null || value === '') return { valid: true };
      const date = new Date(value);
      if (isNaN(date.getTime())) {
        return { valid: false, message: 'Invalid date format' };
      }
      
      // Check date ranges
      if (params?.minDate && date < new Date(params.minDate)) {
        return { 
          valid: false, 
          message: `Date must be on or after ${params.minDate}` 
        };
      }
      if (params?.maxDate && date > new Date(params.maxDate)) {
        return { 
          valid: false, 
          message: `Date must be on or before ${params.maxDate}` 
        };
      }
      
      // Future date check
      if (params?.allowFuture === false && date > new Date()) {
        return { valid: false, message: 'Future dates are not allowed' };
      }
      
      return { valid: true };
    });

    this.addValidator('email', (value) => {
      if (value === null || value === '') return { valid: true };
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      return {
        valid: emailRegex.test(value),
        message: 'Invalid email format'
      };
    });

    this.addValidator('phone', (value) => {
      if (value === null || value === '') return { valid: true };
      const phoneRegex = /^[\d\s\-\+\(\)]+$/;
      return {
        valid: phoneRegex.test(value) && value.replace(/\D/g, '').length >= 10,
        message: 'Invalid phone number format'
      };
    });

    // Clinical-specific validators
    this.addValidator('blood_pressure', (systolic, diastolic) => {
      const sys = Number(systolic);
      const dia = Number(diastolic);
      
      if (sys <= dia) {
        return { 
          valid: false, 
          message: 'Systolic pressure must be greater than diastolic' 
        };
      }
      
      if (sys < 70 || sys > 250) {
        return { 
          valid: false, 
          message: 'Systolic pressure out of valid range (70-250 mmHg)' 
        };
      }
      
      if (dia < 40 || dia > 150) {
        return { 
          valid: false, 
          message: 'Diastolic pressure out of valid range (40-150 mmHg)' 
        };
      }
      
      return { valid: true };
    });

    this.addValidator('bmi', (height, weight) => {
      const h = Number(height) / 100; // Convert cm to m
      const w = Number(weight);
      const bmi = w / (h * h);
      
      if (bmi < 10 || bmi > 60) {
        return { 
          valid: false, 
          message: `BMI ${bmi.toFixed(1)} is outside valid range (10-60)`,
          severity: SEVERITY.WARNING
        };
      }
      
      return { valid: true, calculatedValue: bmi.toFixed(1) };
    });

    this.addValidator('lab_value', (value, normalRange) => {
      const val = Number(value);
      if (val < normalRange.min || val > normalRange.max) {
        const severity = 
          val < normalRange.criticalMin || val > normalRange.criticalMax 
            ? SEVERITY.ERROR 
            : SEVERITY.WARNING;
        
        return {
          valid: false,
          message: `Value ${val} outside normal range (${normalRange.min}-${normalRange.max} ${normalRange.unit})`,
          severity,
          flagForReview: true
        };
      }
      return { valid: true };
    });
  }

  /**
   * Add custom validator
   */
  addValidator(name, validatorFn) {
    this.customValidators.set(name, validatorFn);
  }

  /**
   * Execute validation
   */
  async validate(fieldConfig, value, context = {}) {
    const results = [];

    // Required field check
    if (fieldConfig.required && (value === null || value === '' || value === undefined)) {
      results.push({
        type: RULE_TYPES.REQUIRED,
        severity: SEVERITY.ERROR,
        message: `${fieldConfig.label} is required`,
        field: fieldConfig.name
      });
    }

    // Datatype validation
    if (value !== null && value !== '' && fieldConfig.datatype) {
      const validator = this.customValidators.get(fieldConfig.datatype);
      if (validator) {
        const result = await validator(value, fieldConfig.validation_params);
        if (!result.valid) {
          results.push({
            type: RULE_TYPES.DATATYPE,
            severity: result.severity || SEVERITY.ERROR,
            message: result.message,
            field: fieldConfig.name
          });
        }
      }
    }

    // Range validation
    if (fieldConfig.min !== undefined || fieldConfig.max !== undefined) {
      const numValue = Number(value);
      if (!isNaN(numValue)) {
        if (fieldConfig.min !== undefined && numValue < fieldConfig.min) {
          results.push({
            type: RULE_TYPES.RANGE,
            severity: SEVERITY.ERROR,
            message: `Value must be at least ${fieldConfig.min}`,
            field: fieldConfig.name
          });
        }
        if (fieldConfig.max !== undefined && numValue > fieldConfig.max) {
          results.push({
            type: RULE_TYPES.RANGE,
            severity: SEVERITY.ERROR,
            message: `Value must not exceed ${fieldConfig.max}`,
            field: fieldConfig.name
          });
        }
      }
    }

    // Pattern validation
    if (fieldConfig.pattern && value) {
      const regex = new RegExp(fieldConfig.pattern);
      if (!regex.test(value)) {
        results.push({
          type: RULE_TYPES.PATTERN,
          severity: SEVERITY.ERROR,
          message: fieldConfig.patternMessage || 'Value does not match required format',
          field: fieldConfig.name
        });
      }
    }

    // Cross-field validation
    if (fieldConfig.crossFieldRules && context.formData) {
      for (const rule of fieldConfig.crossFieldRules) {
        const relatedValue = context.formData[rule.relatedField];
        const validator = this.customValidators.get(rule.validator);
        if (validator) {
          const result = await validator(value, relatedValue, rule.params);
          if (!result.valid) {
            results.push({
              type: RULE_TYPES.CROSS_FIELD,
              severity: result.severity || SEVERITY.ERROR,
              message: result.message,
              field: fieldConfig.name,
              relatedField: rule.relatedField
            });
          }
        }
      }
    }

    return results;
  }
}

// Create singleton instance
const validationEngine = new ValidationEngine();

/**
 * API Routes
 */

/**
 * Validate single field
 */
router.post('/validate/field', authenticateToken, async (req, res) => {
  try {
    const { fieldId, value, formData } = req.body;

    // Get field configuration
    const { data: field, error } = await supabase
      .from('EDC_form_fields')
      .select('*')
      .eq('id', fieldId)
      .single();

    if (error || !field) {
      return res.status(404).json({ error: 'Field not found' });
    }

    // Prepare field config
    const fieldConfig = {
      name: field.field_name,
      label: field.field_label,
      datatype: field.datatype,
      required: field.is_required,
      min: field.min_value,
      max: field.max_value,
      pattern: field.pattern,
      patternMessage: field.pattern_message,
      validation_params: field.validation_params
    };

    // Execute validation
    const validationResults = await validationEngine.validate(
      fieldConfig, 
      value, 
      { formData }
    );

    res.json({
      valid: validationResults.length === 0,
      errors: validationResults.filter(r => r.severity === SEVERITY.ERROR),
      warnings: validationResults.filter(r => r.severity === SEVERITY.WARNING),
      notes: validationResults.filter(r => r.severity === SEVERITY.NOTE)
    });

  } catch (error) {
    console.error('Field validation error:', error);
    res.status(500).json({ error: 'Validation failed' });
  }
});

/**
 * Validate entire form
 */
router.post('/validate/form', authenticateToken, async (req, res) => {
  try {
    const { formId, formData } = req.body;

    // Get all fields for the form
    const { data: fields, error } = await supabase
      .from('EDC_form_fields')
      .select('*')
      .eq('form_template_id', formId)
      .order('display_order');

    if (error) throw error;

    const allResults = [];

    // Validate each field
    for (const field of fields) {
      const fieldConfig = {
        name: field.field_name,
        label: field.field_label,
        datatype: field.datatype,
        required: field.is_required,
        min: field.min_value,
        max: field.max_value,
        pattern: field.pattern,
        patternMessage: field.pattern_message,
        validation_params: field.validation_params
      };

      const value = formData[field.field_name];
      const results = await validationEngine.validate(
        fieldConfig,
        value,
        { formData }
      );

      allResults.push(...results);
    }

    // Check for critical errors
    const hasErrors = allResults.some(r => r.severity === SEVERITY.ERROR);
    const hasWarnings = allResults.some(r => r.severity === SEVERITY.WARNING);

    res.json({
      valid: !hasErrors,
      canSubmit: !hasErrors,
      requiresReview: hasWarnings,
      validationResults: allResults,
      summary: {
        totalErrors: allResults.filter(r => r.severity === SEVERITY.ERROR).length,
        totalWarnings: allResults.filter(r => r.severity === SEVERITY.WARNING).length,
        totalNotes: allResults.filter(r => r.severity === SEVERITY.NOTE).length
      }
    });

  } catch (error) {
    console.error('Form validation error:', error);
    res.status(500).json({ error: 'Form validation failed' });
  }
});

/**
 * Create/update validation rule
 */
router.post('/rules', authenticateToken, async (req, res) => {
  try {
    const { 
      fieldId, 
      ruleType, 
      ruleName,
      condition, 
      parameters, 
      severity,
      message,
      active 
    } = req.body;

    const ruleData = {
      field_id: fieldId,
      rule_type: ruleType,
      rule_name: ruleName,
      condition,
      parameters,
      severity: severity || SEVERITY.ERROR,
      message,
      active: active !== false,
      created_by: req.user.userId,
      created_date: new Date().toISOString()
    };

    const { data, error } = await supabase
      .from('EDC_validation_rules')
      .insert(ruleData)
      .select()
      .single();

    if (error) throw error;

    res.json({ 
      success: true, 
      rule: data,
      message: 'Validation rule created successfully'
    });

  } catch (error) {
    console.error('Create rule error:', error);
    res.status(500).json({ error: 'Failed to create validation rule' });
  }
});

/**
 * Get validation rules for a form
 */
router.get('/rules/form/:formId', authenticateToken, async (req, res) => {
  try {
    const { formId } = req.params;

    const { data: rules, error } = await supabase
      .from('EDC_validation_rules')
      .select(`
        *,
        EDC_form_fields (field_name, field_label)
      `)
      .eq('form_id', formId)
      .eq('active', true)
      .order('created_date', { ascending: false });

    if (error) throw error;

    res.json({ rules });

  } catch (error) {
    console.error('Get rules error:', error);
    res.status(500).json({ error: 'Failed to fetch validation rules' });
  }
});

/**
 * Test validation rules
 */
router.post('/rules/test', authenticateToken, async (req, res) => {
  try {
    const { ruleId, testData } = req.body;

    // Get rule
    const { data: rule, error } = await supabase
      .from('EDC_validation_rules')
      .select('*')
      .eq('id', ruleId)
      .single();

    if (error || !rule) {
      return res.status(404).json({ error: 'Rule not found' });
    }

    // Execute rule against test data
    const validator = validationEngine.customValidators.get(rule.rule_type);
    if (!validator) {
      return res.status(400).json({ error: 'Unknown rule type' });
    }

    const result = await validator(testData.value, testData.params);

    res.json({
      ruleId,
      testData,
      result,
      passed: result.valid
    });

  } catch (error) {
    console.error('Test rule error:', error);
    res.status(500).json({ error: 'Failed to test validation rule' });
  }
});

module.exports = router;