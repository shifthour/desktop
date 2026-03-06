import React, { useState, useEffect } from 'react';
import { useSearchParams, useNavigate } from 'react-router-dom';
import {
  Box,
  Paper,
  Typography,
  TextField,
  Button,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Stepper,
  Step,
  StepLabel,
  Card,
  CardContent,
  Chip,
  Alert,
  FormControlLabel,
  Checkbox,
  Radio,
  RadioGroup,
  FormLabel,
  Divider,
  IconButton,
  Tooltip,
  Grid,
  CircularProgress
} from '@mui/material';
// DatePicker removed - using TextField type="date" instead
import {
  Save as SaveIcon,
  Check as CheckIcon,
  Info as InfoIcon,
  AttachFile as AttachIcon,
  Lock as LockIcon,
} from '@mui/icons-material';

// Helper function to calculate age from date of birth
const calculateAge = (dateOfBirth: string | Date): number => {
  if (!dateOfBirth) return 0;
  const birth = new Date(dateOfBirth);
  const today = new Date();
  let age = today.getFullYear() - birth.getFullYear();
  const monthDiff = today.getMonth() - birth.getMonth();
  if (monthDiff < 0 || (monthDiff === 0 && today.getDate() < birth.getDate())) {
    age--;
  }
  return age;
};

// Helper function to evaluate calculation formulas
const evaluateFormula = (formula: string, formData: any, allFields: any[]): number | string => {
  try {
    console.log(`🧮 Evaluating formula: "${formula}"`);
    console.log(`🧮 Form data available:`, Object.keys(formData));
    
    // Replace field names with actual values
    let evaluatedFormula = formula;
    
    // Handle special functions
    if (formula.includes('calculateAge(')) {
      const match = formula.match(/calculateAge\(([^)]+)\)/);
      if (match) {
        const fieldReference = match[1].trim();
        // Try to find the field by id or name
        const field = allFields.find((f: any) => 
          f.fieldId === fieldReference || f.fieldName === fieldReference
        );
        const fieldId = field ? field.fieldId : fieldReference;
        const dateValue = formData[fieldId];
        console.log(`🧮 Looking for field "${fieldReference}" (id: "${fieldId}") with value:`, dateValue);
        if (dateValue) {
          const age = calculateAge(dateValue);
          console.log(`🧮 Calculated age: ${age} years`);
          evaluatedFormula = evaluatedFormula.replace(match[0], age.toString());
        } else {
          console.log(`🧮 No value found for field "${fieldId}"`);
          return '';
        }
      }
    }
    
    // Replace field references with values
    allFields.forEach(field => {
      const fieldId = field.fieldId;
      const fieldValue = formData[fieldId];
      if (fieldValue !== undefined && fieldValue !== '') {
        // Replace field id with value in formula (match both fieldId and fieldName)
        const regex = new RegExp(`\\b${fieldId}\\b`, 'g');
        evaluatedFormula = evaluatedFormula.replace(regex, fieldValue.toString());
        console.log(`🧮 Replaced ${fieldId} with ${fieldValue}`);
        
        // Also try replacing by field name if different
        if (field.fieldName && field.fieldName !== fieldId) {
          const nameRegex = new RegExp(`\\b${field.fieldName}\\b`, 'g');
          evaluatedFormula = evaluatedFormula.replace(nameRegex, fieldValue.toString());
        }
      }
    });
    
    console.log(`🧮 Final evaluated formula: "${evaluatedFormula}"`);
    
    // Check if formula still has unresolved field names
    if (evaluatedFormula.match(/[a-zA-Z_][a-zA-Z0-9_]*/)) {
      console.log(`🧮 Formula still has unresolved field names, returning empty`);
      return ''; // Return empty if not all fields are populated
    }
    
    // Evaluate mathematical expression
    // Using Function constructor for simple mathematical expressions
    // This is safe for controlled formulas but should be validated in production
    const result = Function(`"use strict"; return (${evaluatedFormula})`)();
    console.log(`🧮 Final result:`, result);
    return typeof result === 'number' ? result : result.toString();
    
  } catch (error) {
    console.error('🧮 Formula evaluation error:', error);
    return 'Error';
  }
};

const DataEntry: React.FC = () => {
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const subjectId = searchParams.get('subject');
  const rawVisitId = searchParams.get('visit');
  const visitId = rawVisitId === 'general' || rawVisitId === 'null' ? null : rawVisitId;
  const formId = searchParams.get('form');
  const studyId = searchParams.get('study');

  const [activeStep, setActiveStep] = useState(0);
  const [formStructure, setFormStructure] = useState<any>(null);
  const [formData, setFormData] = useState<any>({});
  const [calculatedValues, setCalculatedValues] = useState<any>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [validationErrors, setValidationErrors] = useState<any>({});
  const [saving, setSaving] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [locking, setLocking] = useState(false);
  const [formStatus, setFormStatus] = useState<string>('');
  const [isReadOnly, setIsReadOnly] = useState(false);

  // Fetch the form structure
  useEffect(() => {
    const fetchFormStructure = async () => {
      if (!formId) {
        setError('No form ID provided');
        setLoading(false);
        return;
      }

      try {
        const response = await fetch(`/api/forms/list`);
        if (!response.ok) throw new Error('Failed to fetch forms');
        
        const data = await response.json();
        const form = data.data?.find((f: any) => f.id === formId);
        
        if (!form) {
          setError('Form not found');
          setLoading(false);
          return;
        }

        setFormStructure(form);
        
        // Fetch subject information to auto-populate fields
        let subjectInfo: any = null;
        if (subjectId) {
          try {
            const subjectResponse = await fetch(`/api/subjects/${subjectId}`);
            if (subjectResponse.ok) {
              const subjectData = await subjectResponse.json();
              subjectInfo = subjectData.data;
            }
          } catch (error) {
            console.warn('Could not fetch subject information:', error);
          }
        }
        
        // Initialize form data based on form structure
        const initialData: any = {
          subjectId: subjectId || '',
          visitId: visitId || '',
          formId: formId
        };

        // Initialize fields from form structure
        if (form.form_structure?.sections) {
          form.form_structure.sections.forEach((section: any) => {
            section.fields?.forEach((field: any) => {
              let initialValue = field.defaultValue || '';
              
              // Auto-populate common fields from subject data
              if (subjectInfo) {
                switch (field.fieldId) {
                  case 'subject_id':
                    initialValue = subjectInfo.subject_number || '';
                    break;
                  case 'site_id':
                    initialValue = subjectInfo.edc_sites?.site_number || '';
                    break;
                  case 'date_of_birth':
                    initialValue = subjectInfo.date_of_birth || '';
                    break;
                  case 'sex':
                    initialValue = subjectInfo.gender === 'MALE' ? 'M' : 
                                  subjectInfo.gender === 'FEMALE' ? 'F' : 'U';
                    break;
                  case 'enrollment_date':
                    initialValue = subjectInfo.enrollment_date || '';
                    break;
                }
              }
              
              initialData[field.fieldId] = initialValue;
            });
          });
        }

        // Try to load existing draft data
        try {
          const existingDataResponse = await fetch(`/api/data/form-data?subjectId=${subjectId}&formId=${formId}&visitId=${visitId || ''}`);
          if (existingDataResponse.ok) {
            const existingData = await existingDataResponse.json();
            if (existingData.status === 'success' && existingData.data) {
              // Merge existing form data with initial data (existing data takes precedence)
              const mergedData = { ...initialData, ...existingData.data.form_data };
              setFormData(mergedData);
              
              // Set form status and read-only state
              const status = existingData.data.status || '';
              setFormStatus(status);
              setIsReadOnly(status === 'SIGNED');
            } else {
              setFormData(initialData);
              setFormStatus('');
              setIsReadOnly(false);
            }
          } else {
            setFormData(initialData);
          }
        } catch (error) {
          console.warn('Could not load existing form data:', error);
          setFormData(initialData);
        }
        
        setLoading(false);
      } catch (err: any) {
        setError(err.message);
        setLoading(false);
      }
    };

    fetchFormStructure();
  }, [formId, subjectId, visitId]);

  const handleValidation = () => {
    const errors: any = {};
    
    if (formData.temperature && (parseFloat(formData.temperature) < 35 || parseFloat(formData.temperature) > 42)) {
      errors.temperature = 'Temperature out of normal range (35-42°C)';
    }
    
    if (formData.bloodPressureSystolic && parseFloat(formData.bloodPressureSystolic) > 180) {
      errors.bloodPressureSystolic = 'High blood pressure detected';
    }

    setValidationErrors(errors);
  };

  const handleInputChange = (field: string, value: any) => {
    setFormData((prev: any) => ({ ...prev, [field]: value }));
    handleValidation();
  };

  // Recalculate all calculated fields when formData changes
  useEffect(() => {
    if (!formStructure) return;

    // Get all fields from all sections
    const allSections = formStructure.form_structure?.sections || formStructure.sections || [];
    const allFields = allSections.flatMap((section: any) => section.fields || []);
    const calculatedFields = allFields.filter((field: any) => field.fieldType === 'calculated' || field.type === 'calculated');
    
    console.log('🔍 Debug: Form structure:', formStructure);
    console.log('🔍 Debug: All sections:', allSections);
    console.log('🔍 Debug: All fields:', allFields.map((f: any) => ({ 
      id: f.fieldId || f.id, 
      name: f.fieldName || f.name, 
      type: f.fieldType || f.type 
    })));
    console.log('🔍 Debug: Calculated fields:', calculatedFields.map((f: any) => ({ 
      id: f.fieldId || f.id, 
      name: f.fieldName || f.name, 
      formula: f.formula 
    })));
    console.log('🔍 Debug: Current form data keys:', Object.keys(formData));
    console.log('🔍 Debug: Current form data values:', formData);
    
    const newCalculatedValues: any = {};
    
    calculatedFields.forEach((field: any) => {
      if (field.formula) {
        const fieldId = field.fieldId || field.id;
        const fieldName = field.fieldName || field.name;
        console.log(`🔍 Debug: Calculating field "${fieldName}" (id: ${fieldId}) with formula "${field.formula}"`);
        const calculatedValue = evaluateFormula(field.formula, formData, allFields);
        console.log(`🔍 Debug: Result for "${fieldName}":`, calculatedValue);
        newCalculatedValues[fieldId] = calculatedValue;
      }
    });
    
    console.log('🔍 Debug: New calculated values:', newCalculatedValues);
    setCalculatedValues(newCalculatedValues);
  }, [formData, formStructure]);

  // Save form as draft
  const handleSaveDraft = async () => {
    setSaving(true);
    try {
      const response = await fetch('/api/data/save-draft', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          subjectId,
          visitId,
          formId,
          formData
        })
      });

      const result = await response.json();
      
      if (result.status === 'success') {
        alert('Draft saved successfully!');
      } else {
        alert('Failed to save draft: ' + result.message);
      }
    } catch (error) {
      console.error('Error saving draft:', error);
      alert('Error saving draft. Please try again.');
    } finally {
      setSaving(false);
    }
  };

  // Submit form for review
  const handleSubmitForReview = async () => {
    setSubmitting(true);
    try {
      const response = await fetch('/api/data/submit-for-review', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          subjectId,
          visitId,
          formId,
          formData
        })
      });

      const result = await response.json();
      
      if (result.status === 'success') {
        alert('Form submitted for review successfully!');
        // Navigate back to the subjects page under the study
        if (studyId) {
          navigate(`/studies?studyId=${studyId}&tab=subjects`);
        } else {
          navigate('/studies');
        }
      } else {
        alert('Failed to submit for review: ' + result.message);
      }
    } catch (error) {
      console.error('Error submitting for review:', error);
      alert('Error submitting form. Please try again.');
    } finally {
      setSubmitting(false);
    }
  };

  // Sign and lock form
  const handleSignAndLock = async () => {
    if (!window.confirm('Are you sure you want to sign and lock this form? This action cannot be undone.')) {
      return;
    }

    setLocking(true);
    try {
      const response = await fetch('/api/data/sign-and-lock', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          subjectId,
          visitId,
          formId,
          formData,
          signature: 'Electronic Signature'
        })
      });

      const result = await response.json();
      
      if (result.status === 'success') {
        alert('Form signed and locked successfully!');
        // Update form status to locked
        setFormStatus('SIGNED');
        setIsReadOnly(true);
      } else {
        alert('Failed to sign and lock: ' + result.message);
      }
    } catch (error) {
      console.error('Error signing and locking:', error);
      alert('Error signing and locking form. Please try again.');
    } finally {
      setLocking(false);
    }
  };

  // Render a form field based on its type
  const renderFormField = (field: any) => {
    const value = formData[field.fieldId] || '';
    
    const commonProps = {
      key: field.fieldId,
      fullWidth: true,
      label: field.fieldName,
      value: value,
      onChange: isReadOnly ? undefined : (e: any) => handleInputChange(field.fieldId, e.target.value),
      required: field.required || false,
      helperText: field.helpText || '',
      disabled: isReadOnly,
      InputProps: isReadOnly ? { readOnly: true } : undefined,
    };

    switch (field.fieldType) {
      case 'text':
      case 'email':
        return (
          <Grid item xs={12} md={6}>
            <TextField {...commonProps} type={field.fieldType} />
          </Grid>
        );
      
      case 'textarea':
        return (
          <Grid item xs={12}>
            <TextField {...commonProps} multiline rows={4} />
          </Grid>
        );
      
      case 'number':
      case 'decimal':
        return (
          <Grid item xs={12} md={6}>
            <TextField {...commonProps} type="number" />
          </Grid>
        );
      
      case 'date':
        return (
          <Grid item xs={12} md={6}>
            <TextField 
              {...commonProps} 
              type="date" 
              InputLabelProps={{ shrink: true }}
            />
          </Grid>
        );
      
      case 'dropdown':
      case 'select':
        return (
          <Grid item xs={12} md={6}>
            <FormControl fullWidth required={field.required}>
              <InputLabel>{field.fieldName}</InputLabel>
              <Select
                value={value}
                label={field.fieldName}
                onChange={(e) => handleInputChange(field.fieldId, e.target.value)}
              >
                {field.options?.map((option: any) => (
                  <MenuItem key={option.value} value={option.value}>
                    {option.label}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>
        );

      case 'multiselect':
        return (
          <Grid item xs={12} md={6}>
            <FormControl fullWidth required={field.required}>
              <InputLabel>{field.fieldName}</InputLabel>
              <Select
                multiple
                value={value ? (Array.isArray(value) ? value : [value]) : []}
                label={field.fieldName}
                onChange={(e) => handleInputChange(field.fieldId, e.target.value)}
              >
                {field.options?.map((option: any) => (
                  <MenuItem key={option.value} value={option.value}>
                    {option.label}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>
        );
      
      case 'radio':
        return (
          <Grid item xs={12}>
            <FormControl component="fieldset" required={field.required}>
              <FormLabel component="legend">{field.fieldName}</FormLabel>
              <RadioGroup
                value={value}
                onChange={(e) => handleInputChange(field.fieldId, e.target.value)}
              >
                {field.options?.map((option: any) => (
                  <FormControlLabel 
                    key={option.value} 
                    value={option.value} 
                    control={<Radio />} 
                    label={option.label} 
                  />
                ))}
              </RadioGroup>
            </FormControl>
          </Grid>
        );
      
      case 'checkbox':
        return (
          <Grid item xs={12}>
            <FormControlLabel
              control={
                <Checkbox
                  checked={!!value}
                  onChange={(e) => handleInputChange(field.fieldId, e.target.checked)}
                />
              }
              label={field.fieldName}
            />
          </Grid>
        );

      case 'calculated':
        const calculatedValue = calculatedValues[field.fieldId];
        const displayValue = calculatedValue !== undefined && calculatedValue !== '' 
          ? calculatedValue 
          : '';
        
        return (
          <Grid item xs={12} md={6}>
            <TextField 
              {...commonProps}
              value={displayValue}
              type="text"
              placeholder="Auto-calculated"
              InputProps={{ readOnly: true }}
              disabled
              helperText={field.formula ? `Formula: ${field.formula}` : field.helpText || ''}
            />
          </Grid>
        );

      case 'file':
        return (
          <Grid item xs={12} md={6}>
            <TextField 
              {...commonProps} 
              type="file"
              InputLabelProps={{ shrink: true }}
            />
          </Grid>
        );
      
      default:
        return (
          <Grid item xs={12} md={6}>
            <TextField {...commonProps} />
          </Grid>
        );
    }
  };

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
        <CircularProgress />
        <Typography sx={{ ml: 2 }}>Loading form...</Typography>
      </Box>
    );
  }

  if (error) {
    return (
      <Box sx={{ p: 3 }}>
        <Alert severity="error">
          <Typography variant="h6">Error loading form</Typography>
          <Typography>{error}</Typography>
        </Alert>
      </Box>
    );
  }

  if (!formStructure) {
    return (
      <Box sx={{ p: 3 }}>
        <Alert severity="warning">
          <Typography>Form structure not available</Typography>
        </Alert>
      </Box>
    );
  }

  const sections = formStructure.form_structure?.sections || [];

  return (
    <Box>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <Box>
          <Typography variant="h4">Data Entry: {formStructure.name}</Typography>
          <Typography variant="body2" color="textSecondary">
            Subject: {subjectId} | Visit: {visitId} | Form: {formStructure.name}
          </Typography>
        </Box>
        <Box display="flex" gap={2}>
          <Chip 
            label={formStatus || 'New'} 
            color={
              formStatus === 'SIGNED' ? 'success' :
              formStatus === 'COMPLETED' ? 'primary' :
              formStatus === 'DRAFT' ? 'warning' : 'default'
            } 
          />
          <Chip label={`Version ${formStructure.version}`} color="info" />
          {isReadOnly && <Chip label="🔒 LOCKED" color="error" />}
        </Box>
      </Box>

      <Alert severity="info" sx={{ mb: 3 }}>
        <strong>Form:</strong> {formStructure.name} v{formStructure.version} | 
        <strong> Status:</strong> {formStructure.status} | 
        <strong> Sections:</strong> {sections.length}
      </Alert>

      {/* Workflow Guide */}
      <Alert severity={formStatus === 'REJECTED' ? 'warning' : 'success'} sx={{ mb: 3 }}>
        <Typography variant="body2">
          <strong>Workflow:</strong> Fill Form → Save Draft (optional) → Submit for Review → (Timeline) Review & Approve/Reject → Final Signature
          {formStatus === 'COMPLETED' && ' | ✅ Form submitted - awaiting review in timeline'}
          {formStatus === 'REJECTED' && ' | ⚠️ Form rejected by reviewer - please make changes and resubmit'}
          {formStatus === 'SIGNED' && ' | 🔒 Form is approved and locked'}
        </Typography>
      </Alert>

      <Grid container spacing={3}>
        <Grid item xs={12} md={3}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Form Sections
            </Typography>
            <Stepper activeStep={activeStep} orientation="vertical">
              {sections.map((section: any, index: number) => (
                <Step key={index}>
                  <StepLabel
                    onClick={() => setActiveStep(index)}
                    sx={{ cursor: 'pointer' }}
                  >
                    {section.name}
                  </StepLabel>
                </Step>
              ))}
            </Stepper>
          </Paper>
        </Grid>

        <Grid item xs={12} md={9}>
          <Paper sx={{ p: 3 }}>
            <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
              <Typography variant="h5">
                {sections[activeStep]?.name || 'Form Section'}
              </Typography>
              <Box>
                <Tooltip title="Attach source document">
                  <IconButton>
                    <AttachIcon />
                  </IconButton>
                </Tooltip>
                <Tooltip title="View form help">
                  <IconButton>
                    <InfoIcon />
                  </IconButton>
                </Tooltip>
              </Box>
            </Box>

            <Divider sx={{ mb: 3 }} />

            {/* Dynamic Form Content */}
            {sections[activeStep] && (
              <Grid container spacing={3}>
                {sections[activeStep].fields?.map((field: any) => renderFormField(field))}
                
                {sections[activeStep].fields?.length === 0 && (
                  <Grid item xs={12}>
                    <Alert severity="info">
                      No fields defined for this section yet. Please add fields using the Form Builder.
                    </Alert>
                  </Grid>
                )}
              </Grid>
            )}

            <Divider sx={{ my: 3 }} />

            {/* Action Buttons */}
            <Box display="flex" justifyContent="space-between" alignItems="center">
              <Box display="flex" gap={2}>
                <Button
                  variant="outlined"
                  disabled={activeStep === 0}
                  onClick={() => setActiveStep(prev => prev - 1)}
                >
                  Previous
                </Button>
                <Button
                  variant="outlined"
                  disabled={activeStep === sections.length - 1}
                  onClick={() => setActiveStep(prev => prev + 1)}
                >
                  Next
                </Button>
              </Box>
              
              <Box display="flex" gap={2}>
                <Button 
                  variant="outlined" 
                  startIcon={<SaveIcon />}
                  onClick={handleSaveDraft}
                  disabled={saving || loading || formStatus === 'SIGNED'}
                >
                  {saving ? 'Saving...' : 'Save Draft'}
                </Button>
                <Button
                  variant="contained"
                  startIcon={<CheckIcon />}
                  color={formStatus === 'REJECTED' ? 'warning' : 'primary'}
                  onClick={handleSubmitForReview}
                  disabled={submitting || loading || formStatus === 'SIGNED' || formStatus === 'COMPLETED'}
                >
                  {submitting ? 'Submitting...' : 
                   formStatus === 'COMPLETED' ? 'Already Submitted' : 
                   formStatus === 'REJECTED' ? 'Resubmit for Review' : 'Submit for Review'}
                </Button>
                {/* Sign & Lock button removed - only available in timeline after approval */}
              </Box>
            </Box>
          </Paper>

          {/* Validation Summary */}
          {Object.keys(validationErrors).length > 0 && (
            <Card sx={{ mt: 3, bgcolor: '#fff3e0' }}>
              <CardContent>
                <Typography variant="h6" color="warning.main" gutterBottom>
                  Validation Warnings
                </Typography>
                {Object.entries(validationErrors).map(([field, error]) => (
                  <Alert severity="warning" key={field} sx={{ mt: 1 }}>
                    {error as string}
                  </Alert>
                ))}
              </CardContent>
            </Card>
          )}
        </Grid>
      </Grid>
    </Box>
  );
};

export default DataEntry;