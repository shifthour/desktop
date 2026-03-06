import axios from 'axios';

export interface FormField {
  id: string;
  type: 'text' | 'number' | 'date' | 'select' | 'radio' | 'checkbox' | 'textarea' | 'file' | 'calculated';
  label: string;
  name: string;
  required: boolean;
  placeholder?: string;
  helperText?: string;
  defaultValue?: any;
  options?: { label: string; value: string }[];
  validation?: {
    min?: number;
    max?: number;
    minLength?: number;
    maxLength?: number;
    pattern?: string;
    customRule?: string;
    errorMessage?: string;
  };
  conditionalLogic?: {
    showIf?: string;
    hideIf?: string;
    requiredIf?: string;
    dependsOn?: string[];
  };
  units?: string;
  normalRange?: string;
  calculation?: string;
  metadata?: Record<string, any>;
}

export interface FormSection {
  id: string;
  title: string;
  description?: string;
  fields: FormField[];
  sequence: number;
  isRepeating?: boolean;
  maxRepetitions?: number;
  conditionalDisplay?: string;
}

export interface ValidationRule {
  id: string;
  name: string;
  type: 'range' | 'pattern' | 'cross-field' | 'custom';
  fieldIds: string[];
  condition: string;
  severity: 'error' | 'warning' | 'info';
  message: string;
  active: boolean;
}

export interface EditCheck {
  id: string;
  name: string;
  description: string;
  query: string;
  triggerOn: 'save' | 'submit' | 'realtime';
  severity: 'hard' | 'soft' | 'query';
  autoQuery: boolean;
  queryTemplate?: string;
}

export interface FormConfig {
  id?: string;
  name: string;
  version: string;
  status: 'DRAFT' | 'UAT' | 'PRODUCTION' | 'DEPRECATED';
  studyId: string;
  sections: FormSection[];
  validationRules: ValidationRule[];
  editChecks: EditCheck[];
  metadata: {
    createdBy?: string;
    createdAt?: Date;
    lastModified?: Date;
    approvedBy?: string;
    approvalDate?: Date;
    changeLog?: Array<{
      version: string;
      changes: string;
      date: Date;
      user: string;
    }>;
  };
}

class FormBuilderService {
  private baseURL = process.env.REACT_APP_API_URL || 'http://localhost:4000/api';

  // Form Management
  async createForm(form: FormConfig): Promise<FormConfig> {
    try {
      const response = await axios.post(`${this.baseURL}/forms`, form);
      return response.data;
    } catch (error) {
      console.error('Error creating form:', error);
      throw error;
    }
  }

  async updateForm(formId: string, form: Partial<FormConfig>): Promise<FormConfig> {
    try {
      const response = await axios.put(`${this.baseURL}/forms/${formId}`, form);
      return response.data;
    } catch (error) {
      console.error('Error updating form:', error);
      throw error;
    }
  }

  async getForm(formId: string): Promise<FormConfig> {
    try {
      const response = await axios.get(`${this.baseURL}/forms/${formId}`);
      return response.data;
    } catch (error) {
      console.error('Error fetching form:', error);
      throw error;
    }
  }

  async getForms(studyId?: string): Promise<FormConfig[]> {
    try {
      const params = studyId ? { studyId } : {};
      const response = await axios.get(`${this.baseURL}/forms`, { params });
      return response.data;
    } catch (error) {
      console.error('Error fetching forms:', error);
      throw error;
    }
  }

  async deleteForm(formId: string): Promise<void> {
    try {
      await axios.delete(`${this.baseURL}/forms/${formId}`);
    } catch (error) {
      console.error('Error deleting form:', error);
      throw error;
    }
  }

  // Form Version Management
  async promoteForm(formId: string, toStatus: 'UAT' | 'PRODUCTION'): Promise<FormConfig> {
    try {
      const response = await axios.post(`${this.baseURL}/forms/${formId}/promote`, {
        status: toStatus
      });
      return response.data;
    } catch (error) {
      console.error('Error promoting form:', error);
      throw error;
    }
  }

  async cloneForm(formId: string, newVersion: string): Promise<FormConfig> {
    try {
      const response = await axios.post(`${this.baseURL}/forms/${formId}/clone`, {
        version: newVersion
      });
      return response.data;
    } catch (error) {
      console.error('Error cloning form:', error);
      throw error;
    }
  }

  // Field Validation
  validateField(field: FormField, value: any): { valid: boolean; errors: string[] } {
    const errors: string[] = [];

    // Required validation
    if (field.required && !value) {
      errors.push(`${field.label} is required`);
    }

    // Type-specific validation
    if (value && field.validation) {
      switch (field.type) {
        case 'text':
        case 'textarea':
          if (field.validation.minLength && value.length < field.validation.minLength) {
            errors.push(`${field.label} must be at least ${field.validation.minLength} characters`);
          }
          if (field.validation.maxLength && value.length > field.validation.maxLength) {
            errors.push(`${field.label} must be no more than ${field.validation.maxLength} characters`);
          }
          if (field.validation.pattern) {
            const regex = new RegExp(field.validation.pattern);
            if (!regex.test(value)) {
              errors.push(field.validation.errorMessage || `${field.label} format is invalid`);
            }
          }
          break;

        case 'number':
          const numValue = parseFloat(value);
          if (field.validation.min !== undefined && numValue < field.validation.min) {
            errors.push(`${field.label} must be at least ${field.validation.min}`);
          }
          if (field.validation.max !== undefined && numValue > field.validation.max) {
            errors.push(`${field.label} must be no more than ${field.validation.max}`);
          }
          break;

        case 'date':
          // Date validation logic
          break;
      }
    }

    return {
      valid: errors.length === 0,
      errors
    };
  }

  // Cross-field Validation
  validateCrossField(rules: ValidationRule[], formData: Record<string, any>): { 
    valid: boolean; 
    violations: Array<{ rule: ValidationRule; message: string }> 
  } {
    const violations: Array<{ rule: ValidationRule; message: string }> = [];

    for (const rule of rules) {
      if (!rule.active) continue;

      try {
        // Evaluate the condition
        const conditionMet = this.evaluateCondition(rule.condition, formData);
        
        if (!conditionMet) {
          violations.push({
            rule,
            message: rule.message
          });
        }
      } catch (error) {
        console.error(`Error evaluating rule ${rule.name}:`, error);
      }
    }

    return {
      valid: violations.length === 0,
      violations
    };
  }

  private evaluateCondition(condition: string, data: Record<string, any>): boolean {
    // Simple condition evaluator
    // In production, use a proper expression parser
    try {
      // Replace field references with actual values
      let evaluableCondition = condition;
      Object.keys(data).forEach(key => {
        const value = data[key];
        evaluableCondition = evaluableCondition.replace(
          new RegExp(`\\$\{${key}\\}`, 'g'),
          JSON.stringify(value)
        );
      });

      // WARNING: eval is dangerous - use a proper expression parser in production
      // This is just for demonstration
      return eval(evaluableCondition);
    } catch (error) {
      console.error('Error evaluating condition:', error);
      return true; // Default to passing if evaluation fails
    }
  }

  // Calculate Field Values
  calculateFieldValue(
    field: FormField,
    formData: Record<string, any>
  ): any {
    if (field.type !== 'calculated' || !field.calculation) {
      return null;
    }

    try {
      let calculation = field.calculation;
      
      // Replace field references with values
      Object.keys(formData).forEach(key => {
        const value = formData[key];
        calculation = calculation.replace(
          new RegExp(`\\$\{${key}\\}`, 'g'),
          value || 0
        );
      });

      // Evaluate the calculation
      // WARNING: Use a proper math expression parser in production
      return eval(calculation);
    } catch (error) {
      console.error('Calculation error:', error);
      return null;
    }
  }

  // Export/Import
  async exportForm(formId: string): Promise<Blob> {
    try {
      const response = await axios.get(`${this.baseURL}/forms/${formId}/export`, {
        responseType: 'blob'
      });
      return response.data;
    } catch (error) {
      console.error('Error exporting form:', error);
      throw error;
    }
  }

  async importForm(file: File): Promise<FormConfig> {
    try {
      const formData = new FormData();
      formData.append('file', file);

      const response = await axios.post(`${this.baseURL}/forms/import`, formData, {
        headers: {
          'Content-Type': 'multipart/form-data'
        }
      });
      return response.data;
    } catch (error) {
      console.error('Error importing form:', error);
      throw error;
    }
  }

  // UAT Testing
  async submitForUAT(formId: string): Promise<{
    testId: string;
    testUrl: string;
    status: string;
  }> {
    try {
      const response = await axios.post(`${this.baseURL}/forms/${formId}/uat`);
      return response.data;
    } catch (error) {
      console.error('Error submitting for UAT:', error);
      throw error;
    }
  }

  async getUATResults(formId: string, testId: string): Promise<{
    passed: number;
    failed: number;
    pending: number;
    issues: Array<{
      severity: 'critical' | 'major' | 'minor';
      description: string;
      field?: string;
    }>;
  }> {
    try {
      const response = await axios.get(`${this.baseURL}/forms/${formId}/uat/${testId}`);
      return response.data;
    } catch (error) {
      console.error('Error fetching UAT results:', error);
      throw error;
    }
  }

  // Generate Sample Data for Testing
  generateSampleData(form: FormConfig): Record<string, any> {
    const data: Record<string, any> = {};

    form.sections.forEach(section => {
      section.fields.forEach(field => {
        switch (field.type) {
          case 'text':
            data[field.name] = `Sample ${field.label}`;
            break;
          case 'number':
            const min = field.validation?.min || 0;
            const max = field.validation?.max || 100;
            data[field.name] = Math.floor(Math.random() * (max - min + 1)) + min;
            break;
          case 'date':
            data[field.name] = new Date().toISOString().split('T')[0];
            break;
          case 'select':
          case 'radio':
            if (field.options && field.options.length > 0) {
              data[field.name] = field.options[0].value;
            }
            break;
          case 'checkbox':
            data[field.name] = Math.random() > 0.5;
            break;
          default:
            data[field.name] = null;
        }
      });
    });

    return data;
  }
}

export default new FormBuilderService();