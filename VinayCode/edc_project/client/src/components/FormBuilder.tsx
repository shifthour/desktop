import React, { useState, useEffect } from 'react';
import {
  DndContext,
  closestCenter,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
  DragEndEvent,
  DragOverlay,
  DragStartEvent,
  useDraggable,
  useDroppable,
} from '@dnd-kit/core';
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  verticalListSortingStrategy,
} from '@dnd-kit/sortable';
import {
  useSortable,
} from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import {
  Box,
  Paper,
  Typography,
  Grid,
  Card,
  TextField,
  Button,
  IconButton,
  Chip,
  Divider,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  Switch,
  FormControlLabel,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Alert,
  Tabs,
  Tab,
  Tooltip,
  Badge,
  Fab,
  Snackbar,
  Stack,
  Checkbox,
  Radio,
  RadioGroup,
  FormLabel,
  FormGroup,
  InputAdornment,
  ToggleButton,
  ToggleButtonGroup,
  Slider,
  LinearProgress,
  Stepper,
  Step,
  StepLabel,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  ListItemSecondaryAction,
  CardHeader,
  CardActions,
  Container,
  useTheme,
  alpha,
} from '@mui/material';
import {
  Add,
  Delete,
  Edit,
  Save,
  ExpandMore,
  DragIndicator,
  Build,
  TextFields,
  CheckBox as CheckBoxIcon,
  RadioButtonChecked,
  CalendarToday,
  ArrowDropDown,
  AttachFile,
  Functions,
  Settings,
  Visibility,
  VisibilityOff,
  ContentCopy,
  Code,
  Preview,
  Download,
  Upload,
  CheckCircle,
  Warning,
  Info,
  Close,
  Person,
  Height,
  FitnessCenter,
  Wc,
  Public,
  Groups,
  Today,
  AssignmentTurnedIn,
  Rule,
  CompareArrows,
  Description,
  ToggleOn,
  FormatListNumbered,
  ShortText,
  Notes,
  CloudUpload,
  Calculate,
  EventNote,
  Check,
  Clear,
  Undo,
  Redo,
  FormatBold,
  FormatItalic,
  FormatUnderlined,
  Link,
  Image,
  TableChart,
  Psychology,
  Biotech,
  MedicalServices,
  Vaccines,
  LocalHospital,
  AccessibilityNew,
  EmojiPeople,
  Badge as BadgeIcon,
  LocationOn,
  Phone,
  Email,
  Home,
  Business,
  Flag,
  Language,
  Translate,
  DateRange,
  Schedule,
  MoreVert,
  FilterList,
  Search,
  SaveAlt,
  PlayArrow,
  Stop,
  Refresh,
  Help,
  InfoOutlined,
  ErrorOutline,
  WarningAmber,
  CheckCircleOutline,
  HighlightOff,
  Lock as LockIcon,
} from '@mui/icons-material';
import { useNavigate, useSearchParams } from 'react-router-dom';

// Field types available in the form builder
const FIELD_TYPES = {
  TEXT: 'text',
  NUMBER: 'number',
  DECIMAL: 'decimal',
  DATE: 'date',
  DROPDOWN: 'dropdown',
  RADIO: 'radio',
  CHECKBOX: 'checkbox',
  MULTISELECT: 'multiselect',
  FILE: 'file',
  CALCULATED: 'calculated',
  TEXTAREA: 'textarea',
};

// CDISC DM Standard Variables
const CDISC_MAPPINGS = {
  SUBJID: 'Subject Identifier',
  SITEID: 'Study Site Identifier',
  USUBJID: 'Unique Subject Identifier',
  BRTHDTC: 'Date/Time of Birth',
  AGE: 'Age',
  AGEU: 'Age Units',
  SEX: 'Sex',
  RACE: 'Race',
  ETHNIC: 'Ethnicity',
  ARMCD: 'Planned Arm Code',
  ARM: 'Description of Planned Arm',
  ACTARMCD: 'Actual Arm Code',
  ACTARM: 'Description of Actual Arm',
  COUNTRY: 'Country',
  DMDTC: 'Date/Time of Collection',
  DMDY: 'Study Day of Collection',
  RFICDTC: 'Date/Time of Informed Consent',
  RFSTDTC: 'Subject Reference Start Date/Time',
  RFENDTC: 'Subject Reference End Date/Time',
  RFXSTDTC: 'Date/Time of First Study Treatment',
  RFXENDTC: 'Date/Time of Last Study Treatment',
  RFICDTC_CONSENT: 'Date/Time of Informed Consent',
  DTHDTC: 'Date/Time of Death',
  DTHFL: 'Subject Death Flag',
  SITEID_SITE: 'Study Site Identifier',
  INVID: 'Investigator Identifier',
  INVNAM: 'Investigator Name',
  BRTHDTC_DOB: 'Date of Birth',
  HEIGHT: 'Height',
  WEIGHT: 'Weight',
  BMI: 'Body Mass Index',
  BSA: 'Body Surface Area',
};

// Predefined Demographics Fields with CDISC mapping
const DEMOGRAPHICS_TEMPLATE = [
  {
    id: 'subject_id',
    name: 'Subject ID',
    type: FIELD_TYPES.TEXT,
    required: true,
    cdiscMapping: 'SUBJID',
    validation: {
      pattern: '^[A-Z0-9]{3,10}$',
      message: 'Subject ID must be 3-10 alphanumeric characters',
    },
    placeholder: 'Enter Subject ID',
    icon: <BadgeIcon />,
    category: 'Identifiers',
  },
  {
    id: 'site_id',
    name: 'Site ID',
    type: FIELD_TYPES.DROPDOWN,
    required: true,
    cdiscMapping: 'SITEID',
    options: [
      { value: 'ba4f8e72-6285-4ae2-a46c-27ccfeac6af7', label: 'SITE-001 - Boston Medical Center' },
      { value: 'd9c2194f-1991-4bf8-ac3b-259bb79ed819', label: 'SITE-002 - University Medical Center' },
      { value: '8d89af7e-c430-4fae-9934-753a31c2516f', label: 'SITE-003 - Los Angeles Clinic' },
      { value: '024aeef6-19a6-4031-a5ac-2eb23c461e28', label: 'SITE-004 - Chicago Research Center' }
    ],
    icon: <Business />,
    category: 'Identifiers',
  },
  {
    id: 'consent_date',
    name: 'Informed Consent Date',
    type: FIELD_TYPES.DATE,
    required: true,
    cdiscMapping: 'RFICDTC',
    validation: {
      maxDate: 'today',
      message: 'Consent date cannot be in the future',
    },
    icon: <AssignmentTurnedIn />,
    category: 'Consent',
  },
  {
    id: 'date_of_birth',
    name: 'Date of Birth',
    type: FIELD_TYPES.DATE,
    required: true,
    cdiscMapping: 'BRTHDTC',
    validation: {
      maxDate: 'today',
      minAge: 18,
      maxAge: 90,
      message: 'Subject must be between 18 and 90 years old',
    },
    icon: <EventNote />,
    category: 'Demographics',
  },
  {
    id: 'age',
    name: 'Age',
    type: FIELD_TYPES.CALCULATED,
    cdiscMapping: 'AGE',
    formula: 'calculateAge(date_of_birth)',
    readonly: true,
    icon: <Person />,
    category: 'Demographics',
  },
  {
    id: 'sex',
    name: 'Biological Sex',
    type: FIELD_TYPES.RADIO,
    required: true,
    cdiscMapping: 'SEX',
    options: [
      { value: 'M', label: 'Male' },
      { value: 'F', label: 'Female' },
      { value: 'U', label: 'Unknown' },
    ],
    icon: <Wc />,
    category: 'Demographics',
  },
  {
    id: 'pregnancy_test',
    name: 'Pregnancy Test',
    type: FIELD_TYPES.RADIO,
    cdiscMapping: 'PREGTEST',
    options: [
      { value: 'POS', label: 'Positive' },
      { value: 'NEG', label: 'Negative' },
      { value: 'NA', label: 'Not Applicable' },
    ],
    conditionalLogic: {
      field: 'sex',
      operator: 'equals',
      value: 'F',
      action: 'show',
    },
    icon: <LocalHospital />,
    category: 'Demographics',
  },
  {
    id: 'race',
    name: 'Race',
    type: FIELD_TYPES.DROPDOWN,
    required: true,
    cdiscMapping: 'RACE',
    options: [
      { value: 'WHITE', label: 'White' },
      { value: 'BLACK', label: 'Black or African American' },
      { value: 'ASIAN', label: 'Asian' },
      { value: 'NATIVE', label: 'American Indian or Alaska Native' },
      { value: 'PACIFIC', label: 'Native Hawaiian or Pacific Islander' },
      { value: 'OTHER', label: 'Other' },
      { value: 'UNKNOWN', label: 'Unknown' },
    ],
    icon: <Public />,
    category: 'Demographics',
  },
  {
    id: 'ethnicity',
    name: 'Ethnicity',
    type: FIELD_TYPES.DROPDOWN,
    required: true,
    cdiscMapping: 'ETHNIC',
    options: [
      { value: 'HISPANIC', label: 'Hispanic or Latino' },
      { value: 'NOT_HISPANIC', label: 'Not Hispanic or Latino' },
      { value: 'UNKNOWN', label: 'Unknown' },
    ],
    icon: <Groups />,
    category: 'Demographics',
  },
  {
    id: 'height',
    name: 'Height (cm)',
    type: FIELD_TYPES.DECIMAL,
    cdiscMapping: 'HEIGHT',
    validation: {
      min: 100,
      max: 250,
      decimals: 1,
      message: 'Height must be between 100 and 250 cm',
    },
    unit: 'cm',
    icon: <Height />,
    category: 'Vitals',
  },
  {
    id: 'weight',
    name: 'Weight (kg)',
    type: FIELD_TYPES.DECIMAL,
    cdiscMapping: 'WEIGHT',
    validation: {
      min: 30,
      max: 300,
      decimals: 1,
      message: 'Weight must be between 30 and 300 kg',
    },
    unit: 'kg',
    icon: <FitnessCenter />,
    category: 'Vitals',
  },
  {
    id: 'bmi',
    name: 'BMI',
    type: FIELD_TYPES.CALCULATED,
    cdiscMapping: 'BMI',
    formula: 'weight / ((height/100) * (height/100))',
    readonly: true,
    decimals: 1,
    unit: 'kg/m²',
    icon: <Calculate />,
    category: 'Vitals',
  },
  {
    id: 'country',
    name: 'Country',
    type: FIELD_TYPES.DROPDOWN,
    required: true,
    cdiscMapping: 'COUNTRY',
    options: [
      { value: 'USA', label: 'United States' },
      { value: 'CAN', label: 'Canada' },
      { value: 'MEX', label: 'Mexico' },
      { value: 'GBR', label: 'United Kingdom' },
      { value: 'DEU', label: 'Germany' },
      { value: 'FRA', label: 'France' },
      { value: 'JPN', label: 'Japan' },
      { value: 'AUS', label: 'Australia' },
    ],
    icon: <Flag />,
    category: 'Location',
  },
  {
    id: 'primary_language',
    name: 'Primary Language',
    type: FIELD_TYPES.DROPDOWN,
    cdiscMapping: 'LANG',
    options: [
      { value: 'EN', label: 'English' },
      { value: 'ES', label: 'Spanish' },
      { value: 'FR', label: 'French' },
      { value: 'DE', label: 'German' },
      { value: 'ZH', label: 'Chinese' },
      { value: 'JA', label: 'Japanese' },
      { value: 'OTHER', label: 'Other' },
    ],
    icon: <Language />,
    category: 'Demographics',
  },
  {
    id: 'medical_history',
    name: 'Medical History Summary',
    type: FIELD_TYPES.TEXTAREA,
    cdiscMapping: 'MHTERM',
    maxLength: 500,
    rows: 4,
    icon: <Description />,
    category: 'Medical',
  },
  {
    id: 'consent_form',
    name: 'Signed Consent Form',
    type: FIELD_TYPES.FILE,
    required: true,
    cdiscMapping: 'CONSENT_DOC',
    accept: '.pdf,.jpg,.png',
    maxSize: 5242880, // 5MB
    icon: <AttachFile />,
    category: 'Consent',
  },
];

// Available field components for the toolbox
const FIELD_COMPONENTS = [
  { type: FIELD_TYPES.TEXT, label: 'Text Field', icon: <TextFields />, color: '#2196f3' },
  { type: FIELD_TYPES.NUMBER, label: 'Number', icon: <FormatListNumbered />, color: '#4caf50' },
  { type: FIELD_TYPES.DECIMAL, label: 'Decimal', icon: <Functions />, color: '#00bcd4' },
  { type: FIELD_TYPES.DATE, label: 'Date Picker', icon: <CalendarToday />, color: '#ff9800' },
  { type: FIELD_TYPES.DROPDOWN, label: 'Dropdown', icon: <ArrowDropDown />, color: '#9c27b0' },
  { type: FIELD_TYPES.RADIO, label: 'Radio Button', icon: <RadioButtonChecked />, color: '#f44336' },
  { type: FIELD_TYPES.CHECKBOX, label: 'Checkbox', icon: <CheckBoxIcon />, color: '#ff5722' },
  { type: FIELD_TYPES.MULTISELECT, label: 'Multi-Select', icon: <FilterList />, color: '#795548' },
  { type: FIELD_TYPES.FILE, label: 'File Upload', icon: <CloudUpload />, color: '#607d8b' },
  { type: FIELD_TYPES.CALCULATED, label: 'Calculated Field', icon: <Calculate />, color: '#3f51b5' },
  { type: FIELD_TYPES.TEXTAREA, label: 'Text Area', icon: <Notes />, color: '#009688' },
];

// File Upload Field Component
function FileUploadField({ field }: { field: any }) {
  const [selectedFile, setSelectedFile] = React.useState<File | null>(null);
  const [error, setError] = React.useState<string>('');

  const handleFileSelect = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    setError('');
    
    if (!file) {
      setSelectedFile(null);
      return;
    }

    // Validate file size
    const maxSize = field.maxSize || 10485760; // 10MB default
    if (file.size > maxSize) {
      setError(`File size (${(file.size / 1048576).toFixed(1)}MB) exceeds maximum allowed size (${(maxSize / 1048576).toFixed(1)}MB)`);
      return;
    }

    // Validate file type
    if (field.accept) {
      const allowedTypes = field.accept.split(',').map((type: string) => type.trim());
      const fileExtension = '.' + (file.name.split('.').pop() || '').toLowerCase();
      if (!allowedTypes.includes(fileExtension)) {
        setError(`File type not allowed. Allowed types: ${field.accept}`);
        return;
      }
    }

    setSelectedFile(file);
  };

  const handleRemoveFile = () => {
    setSelectedFile(null);
    setError('');
  };

  const formatFileSize = (bytes: number) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  return (
    <Box>
      <Typography variant="body2" gutterBottom>
        {field.name} {field.required && <span style={{ color: 'red' }}>*</span>}
      </Typography>
      
      {!selectedFile ? (
        <Button
          variant="outlined"
          component="label"
          startIcon={<CloudUpload />}
          sx={{
            borderStyle: 'dashed',
            borderWidth: 2,
            py: 2,
            px: 3,
            '&:hover': {
              borderStyle: 'dashed',
              borderWidth: 2,
            }
          }}
        >
          Choose File or Drag & Drop
          <input 
            type="file" 
            hidden 
            accept={field.accept}
            onChange={handleFileSelect}
          />
        </Button>
      ) : (
        <Paper 
          sx={{ 
            p: 2, 
            border: '1px solid', 
            borderColor: 'success.light',
            backgroundColor: 'success.50'
          }}
        >
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <AttachFile color="success" />
              <Box>
                <Typography variant="body2" fontWeight="medium">
                  {selectedFile.name}
                </Typography>
                <Typography variant="caption" color="text.secondary">
                  {formatFileSize(selectedFile.size)} • {selectedFile.type || 'Unknown type'}
                </Typography>
              </Box>
            </Box>
            <Box sx={{ display: 'flex', gap: 1 }}>
              <Button
                variant="outlined"
                component="label"
                size="small"
                startIcon={<CloudUpload />}
              >
                Replace
                <input 
                  type="file" 
                  hidden 
                  accept={field.accept}
                  onChange={handleFileSelect}
                />
              </Button>
              <IconButton 
                size="small" 
                color="error" 
                onClick={handleRemoveFile}
              >
                <Delete />
              </IconButton>
            </Box>
          </Box>
        </Paper>
      )}

      {error && (
        <Typography variant="caption" color="error" sx={{ mt: 1, display: 'block' }}>
          {error}
        </Typography>
      )}

      {field.helpText && (
        <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
          {field.helpText}
        </Typography>
      )}
      
      {field.accept && (
        <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, display: 'block' }}>
          Accepted formats: {field.accept} • Max size: {field.maxSize ? (field.maxSize / 1048576).toFixed(1) + 'MB' : '10MB'}
        </Typography>
      )}
    </Box>
  );
}

// Interactive form preview with calculations
const FormPreviewWithCalculations: React.FC<{ formFields: any[], formName: string, formDescription: string }> = ({ formFields, formName, formDescription }) => {
  const [formData, setFormData] = useState<any>({});
  const [calculatedValues, setCalculatedValues] = useState<any>({});

  // Calculate age function
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

  // Evaluate formula
  const evaluateFormula = (formula: string, data: any, fields: any[]): number | string => {
    try {
      console.log(`🔍 Preview: Evaluating formula: "${formula}"`);
      console.log(`🔍 Preview: Available data:`, data);
      let evaluatedFormula = formula;
      
      // Handle calculateAge function
      if (formula.includes('calculateAge(')) {
        const match = formula.match(/calculateAge\(([^)]+)\)/);
        if (match) {
          const fieldId = match[1].trim();
          const dateValue = data[fieldId];
          console.log(`🔍 Preview: Looking for field "${fieldId}" with value:`, dateValue);
          if (dateValue) {
            const age = calculateAge(dateValue);
            console.log(`🔍 Preview: Calculated age: ${age} years`);
            return age.toString();
          }
          return '';
        }
      }
      
      // Replace field references with values
      fields.forEach(field => {
        const value = data[field.id];
        if (value !== undefined && value !== '') {
          const regex = new RegExp(`\\b${field.id}\\b`, 'g');
          evaluatedFormula = evaluatedFormula.replace(regex, value.toString());
          console.log(`🔍 Preview: Replaced ${field.id} with ${value}`);
        }
      });
      
      console.log(`🔍 Preview: After field replacement: "${evaluatedFormula}"`);
      
      // Check if formula still has unresolved field names
      if (evaluatedFormula.match(/[a-zA-Z_][a-zA-Z0-9_]*/)) {
        console.log(`🔍 Preview: Formula still has unresolved field names: "${evaluatedFormula}"`);
        return '';
      }
      
      // Evaluate mathematical expression
      const result = Function(`"use strict"; return (${evaluatedFormula})`)();
      console.log(`🔍 Preview: Final result:`, result);
      return typeof result === 'number' ? parseFloat(result.toFixed(2)) : result.toString();
    } catch (error) {
      console.error('🔍 Preview formula error:', error);
      return '';
    }
  };

  // Recalculate when form data changes
  useEffect(() => {
    const newCalculatedValues: any = {};
    
    formFields.forEach(field => {
      if (field.type === FIELD_TYPES.CALCULATED && field.formula) {
        const value = evaluateFormula(field.formula, formData, formFields);
        newCalculatedValues[field.id] = value;
      }
    });
    
    console.log('🔍 Preview: Form data:', formData);
    console.log('🔍 Preview: Calculated values:', newCalculatedValues);
    setCalculatedValues(newCalculatedValues);
  }, [formData, formFields]);

  const handleFieldChange = (fieldId: string, value: any) => {
    setFormData((prev: any) => ({ ...prev, [fieldId]: value }));
  };

  console.log('🔍 Preview Debug: Form fields:', formFields.map(f => ({ id: f.id, name: f.name, type: f.type })));
  console.log('🔍 Preview Debug: Field types available:', Object.values(FIELD_TYPES));

  return (
    <Paper sx={{ p: 3 }}>
      <Typography variant="h5" gutterBottom>{formName}</Typography>
      <Typography variant="body2" color="text.secondary" gutterBottom>
        {formDescription}
      </Typography>
      <Divider sx={{ my: 2 }} />
      
      {formFields.length === 0 && (
        <Alert severity="info">
          No fields in the form. Click "Load Demographics Template" to populate the form with fields.
        </Alert>
      )}
      
      {formFields.map((field, index) => (
        <Box key={field.id} sx={{ mb: 3 }}>
          {field.type === FIELD_TYPES.TEXT && (
            <TextField
              fullWidth
              label={field.name}
              required={field.required}
              placeholder={field.placeholder}
              helperText={field.helpText}
              value={formData[field.id] || ''}
              onChange={(e) => handleFieldChange(field.id, e.target.value)}
              InputProps={{
                endAdornment: field.unit && (
                  <InputAdornment position="end">{field.unit}</InputAdornment>
                ),
              }}
            />
          )}
          {field.type === FIELD_TYPES.NUMBER && (
            <TextField
              fullWidth
              type="number"
              label={field.name}
              required={field.required}
              placeholder={field.placeholder}
              helperText={field.helpText}
              value={formData[field.id] || ''}
              onChange={(e) => handleFieldChange(field.id, e.target.value)}
              InputProps={{
                endAdornment: field.unit && (
                  <InputAdornment position="end">{field.unit}</InputAdornment>
                ),
              }}
            />
          )}
          {field.type === FIELD_TYPES.DECIMAL && (
            <TextField
              fullWidth
              type="number"
              label={field.name}
              required={field.required}
              placeholder={field.placeholder}
              helperText={field.helpText}
              value={formData[field.id] || ''}
              onChange={(e) => handleFieldChange(field.id, e.target.value)}
              inputProps={{
                step: 0.1
              }}
              InputProps={{
                endAdornment: field.unit && (
                  <InputAdornment position="end">{field.unit}</InputAdornment>
                ),
              }}
            />
          )}
          {field.type === FIELD_TYPES.DATE && (
            <TextField
              fullWidth
              type="date"
              label={field.name}
              required={field.required}
              InputLabelProps={{ shrink: true }}
              helperText={field.helpText}
              value={formData[field.id] || ''}
              onChange={(e) => handleFieldChange(field.id, e.target.value)}
            />
          )}
          {field.type === FIELD_TYPES.DROPDOWN && (
            <FormControl fullWidth required={field.required}>
              <InputLabel>{field.name}</InputLabel>
              <Select 
                label={field.name}
                value={formData[field.id] || ''}
                onChange={(e) => handleFieldChange(field.id, e.target.value)}
              >
                {field.options?.map((opt: any) => (
                  <MenuItem key={opt.value} value={opt.value}>
                    {opt.label}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          )}
          {field.type === FIELD_TYPES.RADIO && (
            <FormControl component="fieldset" required={field.required}>
              <FormLabel component="legend">{field.name}</FormLabel>
              <RadioGroup 
                row
                value={formData[field.id] || ''}
                onChange={(e) => handleFieldChange(field.id, e.target.value)}
              >
                {field.options?.map((opt: any) => (
                  <FormControlLabel
                    key={opt.value}
                    value={opt.value}
                    control={<Radio />}
                    label={opt.label}
                  />
                ))}
              </RadioGroup>
            </FormControl>
          )}
          {field.type === FIELD_TYPES.CHECKBOX && (
            <FormControlLabel
              control={
                <Checkbox 
                  checked={!!formData[field.id]}
                  onChange={(e) => handleFieldChange(field.id, e.target.checked)}
                />
              }
              label={field.name}
            />
          )}
          {field.type === FIELD_TYPES.MULTISELECT && (
            <FormControl fullWidth required={field.required}>
              <InputLabel>{field.name}</InputLabel>
              <Select
                multiple
                label={field.name}
                value={formData[field.id] || []}
                onChange={(e) => handleFieldChange(field.id, e.target.value)}
                renderValue={(selected) => {
                  if (!selected || selected.length === 0) return '';
                  const selectedLabels = selected.map((value: string) => {
                    const option = field.options?.find((opt: any) => opt.value === value);
                    return option ? option.label : value;
                  });
                  return selectedLabels.join(', ');
                }}
                MenuProps={{
                  PaperProps: {
                    style: {
                      maxHeight: 224,
                      width: 250,
                    },
                  },
                  // Close dropdown when clicking outside
                  disableAutoFocusItem: true,
                  variant: "menu"
                }}
              >
                {field.options?.map((opt: any) => (
                  <MenuItem key={opt.value} value={opt.value}>
                    <Checkbox 
                      checked={formData[field.id]?.indexOf(opt.value) > -1}
                      size="small"
                    />
                    {opt.label}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          )}
          {field.type === FIELD_TYPES.TEXTAREA && (
            <TextField
              fullWidth
              multiline
              rows={field.rows || 4}
              label={field.name}
              required={field.required}
              placeholder={field.placeholder}
              helperText={field.helpText}
              value={formData[field.id] || ''}
              onChange={(e) => handleFieldChange(field.id, e.target.value)}
            />
          )}
          {field.type === FIELD_TYPES.FILE && (
            <FileUploadField field={field} />
          )}
          {field.type === FIELD_TYPES.CALCULATED && (
            <TextField
              fullWidth
              label={field.name}
              value={calculatedValues[field.id] || ''}
              disabled
              placeholder="Auto-calculated"
              helperText={field.formula ? `Formula: ${field.formula}` : ''}
              InputProps={{
                readOnly: true,
                endAdornment: field.unit && (
                  <InputAdornment position="end">{field.unit}</InputAdornment>
                ),
              }}
              sx={{
                '& .MuiInputBase-input': {
                  color: calculatedValues[field.id] ? 'success.main' : 'text.disabled',
                  fontWeight: calculatedValues[field.id] ? 'bold' : 'normal'
                }
              }}
            />
          )}
          {/* Fallback for any other field types */}
          {!Object.values(FIELD_TYPES).includes(field.type) && (
            <Alert severity="warning" sx={{ mb: 2 }}>
              <Typography variant="body2">
                <strong>{field.name}</strong>: Unsupported field type "{field.type}"
              </Typography>
            </Alert>
          )}
        </Box>
      ))}
    </Paper>
  );
};

// Simple Click-only Toolbox Component
function ClickableToolboxItem({ component, onAddField }: { component: any; onAddField: (type: string) => void }) {
  const handleClick = (e: React.MouseEvent) => {
    console.log('🖱️ Toolbox item clicked:', component.type);
    onAddField(component.type);
  };

  return (
    <Paper
      onClick={handleClick}
      sx={{
        p: 1.5,
        textAlign: 'center',
        cursor: 'pointer',
        border: '1px solid',
        borderColor: 'divider',
        backgroundColor: 'background.paper',
        borderRadius: 2,
        transition: 'all 0.3s ease',
        '&:hover': {
          borderColor: component.color,
          backgroundColor: alpha(component.color, 0.05),
          transform: 'translateY(-2px)',
          boxShadow: 2,
        },
        '&:active': {
          transform: 'translateY(0px)',
          boxShadow: 1,
        }
      }}
    >
      <Box 
        sx={{ 
          color: component.color, 
          mb: 0.5,
        }}
      >
        {component.icon}
      </Box>
      
      <Typography variant="caption" display="block">
        {component.label}
      </Typography>
    </Paper>
  );
}

// Sortable Field Component
function SortableField({ field, onEdit, onDelete, onDuplicate }: any) {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id: field.id });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
  };

  const theme = useTheme();

  return (
    <Card
      ref={setNodeRef}
      style={style}
      sx={{
        mb: 2,
        background: isDragging 
          ? alpha(theme.palette.primary.main, 0.1)
          : 'background.paper',
        border: '1px solid',
        borderColor: isDragging 
          ? 'primary.main' 
          : 'divider',
        transition: 'all 0.3s ease',
        '&:hover': {
          boxShadow: 4,
          borderColor: 'primary.light',
        },
      }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', p: 2 }}>
        <IconButton
          {...attributes}
          {...listeners}
          size="small"
          sx={{ 
            cursor: 'grab',
            '&:active': { cursor: 'grabbing' },
            mr: 1,
          }}
        >
          <DragIndicator />
        </IconButton>
        
        <Box sx={{ mr: 2 }}>
          {field.icon || <TextFields />}
        </Box>

        <Box sx={{ flexGrow: 1 }}>
          <Typography variant="subtitle1" fontWeight="medium">
            {field.name}
            {field.required && (
              <Chip 
                label="Required" 
                size="small" 
                color="error" 
                sx={{ ml: 1, height: 20 }} 
              />
            )}
          </Typography>
          <Stack direction="row" spacing={1} alignItems="center" sx={{ mt: 0.5 }}>
            <Chip 
              label={field.type} 
              size="small" 
              sx={{ 
                height: 20,
                backgroundColor: alpha(theme.palette.info.main, 0.1),
                color: 'info.main',
              }} 
            />
            {field.cdiscMapping && (
              <Chip 
                label={`CDISC: ${field.cdiscMapping}`} 
                size="small" 
                variant="outlined"
                sx={{ height: 20 }} 
              />
            )}
            {field.conditionalLogic && (
              <Chip 
                icon={<Visibility fontSize="small" />}
                label="Conditional" 
                size="small" 
                sx={{ 
                  height: 20,
                  backgroundColor: alpha(theme.palette.warning.main, 0.1),
                  color: 'warning.main',
                }} 
              />
            )}
          </Stack>
        </Box>

        <Stack direction="row" spacing={1}>
          <Tooltip title="Duplicate">
            <IconButton size="small" onClick={() => onDuplicate(field.id)}>
              <ContentCopy fontSize="small" />
            </IconButton>
          </Tooltip>
          <Tooltip title="Edit">
            <IconButton size="small" color="primary" onClick={() => onEdit(field)}>
              <Edit fontSize="small" />
            </IconButton>
          </Tooltip>
          <Tooltip title="Delete">
            <IconButton size="small" color="error" onClick={() => onDelete(field.id)}>
              <Delete fontSize="small" />
            </IconButton>
          </Tooltip>
        </Stack>
      </Box>
    </Card>
  );
}

// Droppable Form Area Component
function DroppableFormArea({ children, isEmpty, onLoadTemplate }: { children: React.ReactNode; isEmpty: boolean; onLoadTemplate?: () => void }) {
  const { isOver, setNodeRef } = useDroppable({
    id: 'form-area',
    data: {
      type: 'form-area',
    },
  });

  return (
    <Paper
      ref={setNodeRef}
      sx={{
        p: 3,
        minHeight: isEmpty ? 400 : 'auto',
        background: isOver 
          ? 'linear-gradient(135deg, rgba(33, 150, 243, 0.1) 0%, rgba(33, 203, 243, 0.1) 100%)'
          : 'background.paper',
        border: isOver ? '2px dashed #2196F3' : '1px solid',
        borderColor: isOver ? 'primary.main' : 'divider',
        borderRadius: 2,
        transition: 'all 0.3s ease',
        position: 'relative',
        ...(isEmpty && {
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          flexDirection: 'column',
        }),
      }}
    >
      {isEmpty && (
        <Box
          sx={{
            textAlign: 'center',
            color: isOver ? 'primary.main' : 'text.secondary',
            transition: 'color 0.3s ease',
          }}
        >
          <DragIndicator sx={{ fontSize: 48, mb: 2, opacity: 0.5 }} />
          <Typography variant="h6" gutterBottom>
            {isOver ? 'Drop field here' : 'Drag fields here to build your form'}
          </Typography>
          <Typography variant="body2" sx={{ mb: 2 }}>
            Or click "Load Template" to start with pre-defined demographics fields
          </Typography>
          <Button
            variant="contained"
            startIcon={<Upload />}
            onClick={onLoadTemplate}
            size="large"
          >
            Load Demographics Template
          </Button>
        </Box>
      )}
      {!isEmpty && children}
    </Paper>
  );
}

// Main Form Builder Component
const FormBuilder: React.FC = () => {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const [formFields, setFormFields] = useState<any[]>([]);
  const [editingField, setEditingField] = useState<any>(null);
  const [showFieldEditor, setShowFieldEditor] = useState(false);
  const [showJsonPreview, setShowJsonPreview] = useState(false);
  const [showFormPreview, setShowFormPreview] = useState(false);
  const [formName, setFormName] = useState('Demographics eCRF Form');
  const [formVersion, setFormVersion] = useState('1.0');
  const [formDescription, setFormDescription] = useState('Electronic Case Report Form for collecting subject demographic information');
  const [formStatus, setFormStatus] = useState('DRAFT');
  const [snackbar, setSnackbar] = useState({ open: false, message: '', severity: 'success' as any });
  const [activeFieldId, setActiveFieldId] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [categoryFilter, setCategoryFilter] = useState('all');
  const [isEditingExistingForm, setIsEditingExistingForm] = useState(false);
  const [currentFormId, setCurrentFormId] = useState<string | null>(null);
  const [isViewMode, setIsViewMode] = useState(false);

  // Check if form is editable based on status
  const isFormEditable = () => {
    if (isViewMode) return false; // View mode is always read-only
    return formStatus === 'DRAFT' || formStatus === 'UAT_TESTING' || formStatus === 'UAT TESTING';
  };

  // Get next available status for promotion
  const getNextStatus = () => {
    switch (formStatus) {
      case 'DRAFT':
        return 'UAT_TESTING';
      case 'UAT_TESTING':
      case 'UAT TESTING':
        return 'PRODUCTION';
      default:
        return null;
    }
  };

  // Get display-friendly status name
  const getStatusDisplayName = (status: string) => {
    switch (status) {
      case 'UAT_TESTING':
        return 'UAT Testing';
      case 'PRODUCTION':
        return 'Production';
      case 'DRAFT':
        return 'Draft';
      default:
        return status;
    }
  };

  // Handle form status promotion
  const handlePromoteStatus = async () => {
    const nextStatus = getNextStatus();
    if (!nextStatus || !currentFormId) return;
    
    try {
      // Make API call to promote form status
      const response = await fetch(`/api/forms/${currentFormId}/promote`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ newStatus: nextStatus }),
      });

      const result = await response.json();

      if (!response.ok) {
        throw new Error(result.message || 'Failed to promote form status');
      }

      // Update current form status in the UI
      setFormStatus(nextStatus);

      // Also update localStorage for consistency
      const savedForms = JSON.parse(localStorage.getItem('savedForms') || '[]');
      const updatedForms = savedForms.map((form: any) =>
        form.id === currentFormId
          ? { ...form, status: nextStatus, updated_at: new Date().toISOString() }
          : form
      );
      localStorage.setItem('savedForms', JSON.stringify(updatedForms));
      
      setSnackbar({
        open: true,
        message: `Form promoted to ${getStatusDisplayName(nextStatus)} successfully!`,
        severity: 'success',
      });

      // Navigate back to Saved Forms after a short delay
      setTimeout(() => {
        navigate('/saved-forms');
      }, 1500);
    } catch (error) {
      console.error('Error promoting form:', error);
      setSnackbar({
        open: true,
        message: `Failed to promote form status: ${error instanceof Error ? error.message : 'Unknown error'}`,
        severity: 'error',
      });
    }
  };

  // Load form data if editing existing form
  React.useEffect(() => {
    const formId = searchParams.get('id');
    const viewMode = searchParams.get('view') === 'true';
    
    if (formId) {
      setCurrentFormId(formId);
      setIsEditingExistingForm(true);
      setIsViewMode(viewMode);
      
      // Fetch latest form data from API
      const loadFormData = async () => {
        try {
          const response = await fetch('/api/forms/list');
          const result = await response.json();
          
          if (result.status === 'success') {
            const formToEdit = result.data.find((form: any) => form.id === formId);
            
            if (formToEdit) {
              setFormName(formToEdit.name);
              setFormVersion(formToEdit.version);
              setFormDescription(formToEdit.description);
              setFormStatus(formToEdit.status); // This will have the latest status from database
              
              // Load detailed form data from form_structure if available
              if (formToEdit.form_structure && formToEdit.form_structure.sections) {
                const fields = formToEdit.form_structure.sections[0]?.fields || [];
                setFormFields(fields.map((field: any) => ({
                  id: field.fieldId,
                  name: field.fieldName,
                  type: field.fieldType,
                  required: field.required,
                  cdiscMapping: field.cdiscMapping,
                  validation: field.validation,
                  options: field.options,
                  conditionalLogic: field.conditionalLogic,
                  category: field.category,
                  placeholder: field.placeholder,
                  helpText: field.helpText,
                  defaultValue: field.defaultValue,
                  unit: field.unit,
                  formula: field.formula,
                  readonly: field.readonly
                })));
              } else {
                // Fallback to localStorage if no form_structure in database
                const detailedFormData = localStorage.getItem(`formData_${formId}`);
                if (detailedFormData) {
                  const formData = JSON.parse(detailedFormData);
                  if (formData.fields) {
                    setFormFields(formData.fields.map((field: any) => ({
                      id: field.fieldId,
                      name: field.fieldName,
                      type: field.fieldType,
                      required: field.required,
                      cdiscMapping: field.cdiscMapping,
                      validation: field.validation,
                      options: field.options,
                      conditionalLogic: field.conditionalLogic,
                      category: field.category,
                      placeholder: field.placeholder,
                      helpText: field.helpText,
                      defaultValue: field.defaultValue,
                      unit: field.unit,
                      formula: field.formula,
                      readonly: field.readonly
                    })));
                  }
                }
              }
            }
          } else {
            throw new Error(result.message || 'Failed to load form data');
          }
        } catch (error) {
          console.error('Error loading form:', error);
          setSnackbar({
            open: true,
            message: 'Error loading form data',
            severity: 'error',
          });
        }
      };
      
      loadFormData();
    } else {
      // Reset for new form
      setIsEditingExistingForm(false);
      setCurrentFormId(null);
      setFormStatus('DRAFT');
    }
  }, [searchParams]);

  const sensors = useSensors(
    useSensor(PointerSensor),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    })
  );

  // Load demographics template
  const loadTemplate = () => {
    setFormFields(DEMOGRAPHICS_TEMPLATE);
    setSnackbar({
      open: true,
      message: 'Demographics template loaded successfully!',
      severity: 'success',
    });
  };

  // Handle drag end
  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event;
    
    console.log('🔄 Drag End Event:', { 
      active: active.id, 
      over: over?.id, 
      activeData: active.data.current,
      overData: over?.data.current,
      hasOver: !!over,
      activeType: active.data.current?.type,
      fieldType: active.data.current?.fieldType
    });
    
    if (!over) {
      console.log('🔄 No drop target found - check if form area exists');
      return;
    }

    // Prevent drag and drop if form is not editable
    if (!isFormEditable()) {
      console.log('🔄 Form not editable:', { formStatus, isViewMode });
      setSnackbar({
        open: true,
        message: 'Form is locked in Production and cannot be edited',
        severity: 'warning',
      });
      setActiveFieldId(null);
      return;
    }

    // Handle drag from toolbox to form area
    if (active.data.current?.type === 'toolbox-item' && over.id === 'form-area') {
      const fieldType = active.data.current.fieldType;
      console.log('🔄 DRAG SUCCESS: Adding field from toolbox to form area');
      console.log('🔄 Field type:', fieldType);
      console.log('🔄 Active data:', active.data.current);
      console.log('🔄 Over target:', over.id);
      addField(fieldType);
      setActiveFieldId(null);
      return;
    }

    // Handle reordering existing fields
    if (active.id !== over.id && active.data.current?.type !== 'toolbox-item') {
      console.log('🔄 Reordering fields');
      setFormFields((items) => {
        const oldIndex = items.findIndex((item) => item.id === active.id);
        const newIndex = items.findIndex((item) => item.id === over.id);
        return arrayMove(items, oldIndex, newIndex);
      });
    }
    
    setActiveFieldId(null);
  };

  // Handle drag start
  const handleDragStart = (event: DragStartEvent) => {
    setActiveFieldId(event.active.id as string);
  };

  // Add new field
  const addField = (type: string) => {
    console.log('➕ Adding field:', { type, formStatus, isViewMode, editable: isFormEditable() });
    
    if (!isFormEditable()) {
      console.log('➕ Field addition blocked - form not editable');
      setSnackbar({
        open: true,
        message: 'Form is locked in Production and cannot be edited',
        severity: 'warning',
      });
      return;
    }

    const newField = {
      id: `field_${Date.now()}`,
      name: `New ${type} Field`,
      type: type,
      required: false,
      cdiscMapping: '',
      validation: {},
      options: type === FIELD_TYPES.DROPDOWN || type === FIELD_TYPES.RADIO || type === FIELD_TYPES.MULTISELECT
        ? [{ value: 'option1', label: 'Option 1' }]
        : undefined,
    };
    
    console.log('➕ Creating new field:', newField);
    setFormFields([...formFields, newField]);
    setEditingField(newField);
    setShowFieldEditor(true);
    console.log('➕ Field added successfully');
  };

  // Edit field
  const editField = (field: any) => {
    if (!isFormEditable()) {
      setSnackbar({
        open: true,
        message: 'Form is locked in Production and cannot be edited',
        severity: 'warning',
      });
      return;
    }
    setEditingField(field);
    setShowFieldEditor(true);
  };

  // Update field
  const updateField = (updatedField: any) => {
    setFormFields(formFields.map(f => f.id === updatedField.id ? updatedField : f));
    setShowFieldEditor(false);
    setEditingField(null);
    setSnackbar({
      open: true,
      message: 'Field updated successfully!',
      severity: 'success',
    });
  };

  // Delete field
  const deleteField = (fieldId: string) => {
    if (!isFormEditable()) {
      setSnackbar({
        open: true,
        message: 'Form is locked in Production and cannot be edited',
        severity: 'warning',
      });
      return;
    }
    setFormFields(formFields.filter(f => f.id !== fieldId));
    setSnackbar({
      open: true,
      message: 'Field deleted successfully!',
      severity: 'info',
    });
  };

  // Duplicate field
  const duplicateField = (fieldId: string) => {
    if (!isFormEditable()) {
      setSnackbar({
        open: true,
        message: 'Form is locked in Production and cannot be edited',
        severity: 'warning',
      });
      return;
    }
    const fieldToDuplicate = formFields.find(f => f.id === fieldId);
    if (fieldToDuplicate) {
      const newField = {
        ...fieldToDuplicate,
        id: `field_${Date.now()}`,
        name: `${fieldToDuplicate.name} (Copy)`,
      };
      setFormFields([...formFields, newField]);
      setSnackbar({
        open: true,
        message: 'Field duplicated successfully!',
        severity: 'success',
      });
    }
  };

  // Generate JSON
  const generateJSON = () => {
    const formDefinition = {
      formId: `FORM_${Date.now()}`,
      formName,
      formVersion,
      formDescription,
      createdDate: new Date().toISOString(),
      modifiedDate: new Date().toISOString(),
      status: formStatus,
      cdiscCompliant: true,
      fields: formFields.map((field, index) => ({
        fieldId: field.id,
        fieldName: field.name,
        fieldType: field.type,
        displayOrder: index + 1,
        required: field.required || false,
        cdiscMapping: field.cdiscMapping || null,
        validation: field.validation || {},
        options: field.options || null,
        conditionalLogic: field.conditionalLogic || null,
        category: field.category || 'General',
        placeholder: field.placeholder || '',
        helpText: field.helpText || '',
        defaultValue: field.defaultValue || null,
        unit: field.unit || null,
        formula: field.formula || null,
        readonly: field.readonly || false,
      })),
      auditTrail: {
        createdBy: 'Study Designer',
        createdDate: new Date().toISOString(),
        modifiedBy: 'Study Designer',
        modifiedDate: new Date().toISOString(),
        version: formVersion,
        changeLog: [],
      },
      compliance: {
        cfr21Part11: true,
        gdpr: true,
        hipaa: true,
      },
    };
    return formDefinition;
  };

  // Export form
  const exportForm = () => {
    const json = generateJSON();
    const blob = new Blob([JSON.stringify(json, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `${formName.replace(/\s+/g, '_')}_v${formVersion}.json`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
    setSnackbar({
      open: true,
      message: 'Form exported successfully!',
      severity: 'success',
    });
  };

  // Save form to database
  const saveForm = async () => {
    const formData = generateJSON();
    try {
      // Prepare data for API call - the API expects 'sections' format
      const apiData = {
        name: formData.formName,
        version: formData.formVersion,
        status: formData.status,
        sections: [{
          name: 'Demographics',
          fields: formData.fields
        }]
      };

      // Make API call to save form
      const response = await fetch('/api/forms/save', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(apiData),
      });

      const result = await response.json();

      if (!response.ok) {
        throw new Error(result.message || 'Failed to save form');
      }

      // Also save to localStorage for consistency (legacy support)
      const savedForm = {
        id: result.data.formId,
        name: formData.formName,
        description: formData.formDescription,
        category: 'Demographics',
        version: formData.formVersion,
        status: formData.status,
        created_at: formData.createdDate,
        updated_at: formData.modifiedDate,
        cdiscCompliant: formData.cdiscCompliant,
        fieldCount: formData.fields.length,
        requiredFields: formData.fields.filter((field: any) => field.required).length,
        fieldTypes: Array.from(new Set(formData.fields.map((field: any) => field.fieldType))),
        lastModifiedBy: formData.auditTrail.modifiedBy,
        phase: formStatus === 'PRODUCTION' ? 'CONDUCT' : 'SETUP'
      };

      const existingSavedForms = JSON.parse(localStorage.getItem('savedForms') || '[]');
      let updatedSavedForms;
      
      if (isEditingExistingForm) {
        updatedSavedForms = existingSavedForms.map((form: any) =>
          form.id === currentFormId ? savedForm : form
        );
      } else {
        updatedSavedForms = [...existingSavedForms, savedForm];
      }
      
      localStorage.setItem('savedForms', JSON.stringify(updatedSavedForms));
      localStorage.setItem(`formData_${savedForm.id}`, JSON.stringify(formData));

      console.log('Form saved successfully:', result);
      setSnackbar({
        open: true,
        message: `Form ${isEditingExistingForm ? 'updated' : 'saved'} successfully to database!`,
        severity: 'success',
      });

      // Navigate to Saved Forms after a short delay
      setTimeout(() => {
        navigate('/saved-forms');
      }, 1500);
    } catch (error) {
      console.error('Error saving form:', error);
      setSnackbar({
        open: true,
        message: `Error saving form: ${error instanceof Error ? error.message : 'Unknown error'}`,
        severity: 'error',
      });
    }
  };

  // Get unique categories
  const categories = ['all', ...Array.from(new Set(formFields.map(f => f.category).filter(Boolean)))];

  // Filter fields
  const filteredFields = formFields.filter(field => {
    const matchesSearch = (field.name || '').toLowerCase().includes(searchTerm.toLowerCase()) ||
                          (field.cdiscMapping || '').toLowerCase().includes(searchTerm.toLowerCase());
    const matchesCategory = categoryFilter === 'all' || field.category === categoryFilter;
    return matchesSearch && matchesCategory;
  });

  return (
    <Container maxWidth="xl" sx={{ py: 3 }}>
      <Paper sx={{ p: 3, mb: 3 }}>
        <Grid container spacing={3} alignItems="center">
          <Grid item xs={12} md={6}>
            <Typography variant="h4" fontWeight="bold" gutterBottom>
              {isViewMode ? (
                <>
                  <Visibility sx={{ mr: 1, verticalAlign: 'middle' }} />
                  Demographics eCRF Form Viewer (Read-Only)
                </>
              ) : (
                <>
                  <Build sx={{ mr: 1, verticalAlign: 'middle' }} />
                  Demographics eCRF Form Builder
                </>
              )}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              {isViewMode 
                ? 'View complete form structure and configuration (Production form - read-only)'
                : 'Design and configure demographic data collection forms with CDISC compliance'
              }
            </Typography>
          </Grid>
          <Grid item xs={12} md={6}>
            <Stack direction="row" spacing={2} justifyContent="flex-end">
              {!isViewMode && (
                <Button
                  variant="contained"
                  startIcon={<Upload />}
                  onClick={loadTemplate}
                  color="info"
                  sx={{
                    background: 'linear-gradient(45deg, #2196F3 30%, #21CBF3 90%)',
                    boxShadow: '0 3px 5px 2px rgba(33, 203, 243, .3)',
                    '&:hover': {
                      background: 'linear-gradient(45deg, #1976D2 30%, #0288D1 90%)',
                    }
                  }}
                >
                  Load Template
                </Button>
              )}
              <Button
                variant="contained"
                startIcon={<Preview />}
                onClick={() => setShowFormPreview(true)}
                color="secondary"
                sx={{
                  background: 'linear-gradient(45deg, #9C27B0 30%, #673AB7 90%)',
                  boxShadow: '0 3px 5px 2px rgba(156, 39, 176, .3)',
                  '&:hover': {
                    background: 'linear-gradient(45deg, #7B1FA2 30%, #512DA8 90%)',
                  }
                }}
              >
                {isViewMode ? 'Preview Form' : 'Preview Form'}
              </Button>
              {!isViewMode && (
                <Button
                  variant="contained"
                  startIcon={<Save />}
                  onClick={saveForm}
                  color="primary"
                  sx={{
                    background: 'linear-gradient(45deg, #FE6B8B 30%, #FF8E53 90%)',
                    boxShadow: '0 3px 5px 2px rgba(255, 105, 135, .3)',
                    '&:hover': {
                      background: 'linear-gradient(45deg, #E91E63 30%, #FF5722 90%)',
                    }
                  }}
                >
                  Save Form
                </Button>
              )}
              {isViewMode && (
                <Button
                  variant="outlined"
                  startIcon={<Close />}
                  onClick={() => navigate('/saved-forms')}
                  color="primary"
                >
                  Close View
                </Button>
              )}
            </Stack>
          </Grid>
        </Grid>

        <Divider sx={{ my: 3 }} />

        {/* Form Metadata */}
        <Grid container spacing={2}>
          <Grid item xs={12} md={4}>
            <TextField
              fullWidth
              label="Form Name"
              value={formName}
              onChange={(e) => setFormName(e.target.value)}
              variant="outlined"
              size="small"
              disabled={!isFormEditable()}
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <Description />
                  </InputAdornment>
                ),
              }}
            />
          </Grid>
          <Grid item xs={12} md={2}>
            <TextField
              fullWidth
              label="Version"
              value={formVersion}
              onChange={(e) => setFormVersion(e.target.value)}
              variant="outlined"
              size="small"
              disabled={!isFormEditable()}
            />
          </Grid>
          <Grid item xs={12} md={6}>
            <TextField
              fullWidth
              label="Description"
              value={formDescription}
              onChange={(e) => setFormDescription(e.target.value)}
              variant="outlined"
              size="small"
              multiline
              disabled={!isFormEditable()}
            />
          </Grid>

          {/* Status Management for Existing Forms */}
          {isEditingExistingForm && (
            <>
              <Grid item xs={12}>
                <Divider sx={{ my: 2 }}>
                  <Typography variant="body2" color="text.secondary">
                    Form Status Management
                  </Typography>
                </Divider>
              </Grid>
              <Grid item xs={12} md={4}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <Typography variant="body2" color="text.secondary">
                    Current Status:
                  </Typography>
                  <Chip
                    label={formStatus}
                    color={
                      formStatus === 'PRODUCTION' ? 'success' :
                      (formStatus === 'UAT_TESTING' || formStatus === 'UAT TESTING') ? 'warning' : 'info'
                    }
                    icon={
                      formStatus === 'PRODUCTION' ? <CheckCircle fontSize="small" /> :
                      (formStatus === 'UAT_TESTING' || formStatus === 'UAT TESTING') ? <Warning fontSize="small" /> : <Build fontSize="small" />
                    }
                  />
                  {formStatus === 'PRODUCTION' && (
                    <Chip
                      icon={<LockIcon fontSize="small" />}
                      label="Read Only"
                      size="small"
                      color="error"
                      variant="outlined"
                      sx={{ fontSize: '0.7rem', height: '20px', ml: 1 }}
                    />
                  )}
                </Box>
              </Grid>
              <Grid item xs={12} md={8}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                  {getNextStatus() && (
                    <Button
                      variant="contained"
                      size="small"
                      onClick={handlePromoteStatus}
                      startIcon={<ArrowDropDown sx={{ transform: 'rotate(-90deg)' }} />}
                      color="success"
                      sx={{
                        background: 'linear-gradient(45deg, #4CAF50 30%, #8BC34A 90%)',
                        boxShadow: '0 2px 4px 1px rgba(76, 175, 80, .2)',
                      }}
                    >
                      Promote to {getStatusDisplayName(getNextStatus() || '')}
                    </Button>
                  )}
                  <Typography variant="caption" color="text.secondary">
                    {formStatus === 'DRAFT' && 'Draft forms can be edited and promoted to UAT Testing'}
                    {(formStatus === 'UAT_TESTING' || formStatus === 'UAT TESTING') && 'UAT Testing forms can be edited and promoted to Production'}
                    {formStatus === 'PRODUCTION' && 'Production forms are locked and cannot be edited'}
                  </Typography>
                </Box>
              </Grid>
            </>
          )}
        </Grid>
      </Paper>

      <DndContext
        sensors={sensors}
        collisionDetection={closestCenter}
        onDragEnd={handleDragEnd}
        onDragStart={handleDragStart}
      >
        <Grid container spacing={3}>
        {/* Field Components Toolbox - Hidden in View Mode */}
        {!isViewMode && (
          <Grid item xs={12} md={3}>
          <Paper sx={{ p: 2, position: 'sticky', top: 20 }}>
            <Typography variant="h6" gutterBottom>
              <Add sx={{ mr: 1, verticalAlign: 'middle' }} />
              Field Components
            </Typography>
            <Divider sx={{ mb: 2 }} />
            <Typography variant="caption" color="text.secondary" display="block" sx={{ mb: 2 }}>
              Click to add fields
            </Typography>
            <Grid container spacing={1}>
              {FIELD_COMPONENTS.map((component) => (
                <Grid item xs={6} key={component.type}>
                  <ClickableToolboxItem component={component} onAddField={addField} />
                </Grid>
              ))}
            </Grid>

            <Divider sx={{ my: 3 }} />

            {/* Field Statistics */}
            <Typography variant="subtitle2" gutterBottom>
              Form Statistics
            </Typography>
            <List dense>
              <ListItem>
                <ListItemIcon>
                  <Badge badgeContent={formFields.length} color="primary">
                    <TextFields />
                  </Badge>
                </ListItemIcon>
                <ListItemText primary="Total Fields" />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <Badge badgeContent={formFields.filter(f => f.required).length} color="error">
                    <Warning />
                  </Badge>
                </ListItemIcon>
                <ListItemText primary="Required Fields" />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <Badge badgeContent={formFields.filter(f => f.cdiscMapping).length} color="success">
                    <CheckCircle />
                  </Badge>
                </ListItemIcon>
                <ListItemText primary="CDISC Mapped" />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <Badge badgeContent={formFields.filter(f => f.conditionalLogic).length} color="warning">
                    <Visibility />
                  </Badge>
                </ListItemIcon>
                <ListItemText primary="Conditional Fields" />
              </ListItem>
            </List>
          </Paper>
        </Grid>
        )}

        <Grid item xs={12} md={isViewMode ? 12 : 9}>
          <Paper sx={{ p: 3, minHeight: '600px' }}>
            {/* Search and Filter */}
            <Box sx={{ mb: 3 }}>
              <Grid container spacing={2} alignItems="center">
                <Grid item xs={12} md={6}>
                  <TextField
                    fullWidth
                    size="small"
                    placeholder="Search fields..."
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                    InputProps={{
                      startAdornment: (
                        <InputAdornment position="start">
                          <Search />
                        </InputAdornment>
                      ),
                    }}
                  />
                </Grid>
                <Grid item xs={12} md={4}>
                  <FormControl fullWidth size="small">
                    <InputLabel>Category</InputLabel>
                    <Select
                      value={categoryFilter}
                      label="Category"
                      onChange={(e) => setCategoryFilter(e.target.value)}
                    >
                      {categories.map(cat => (
                        <MenuItem key={cat} value={cat}>
                          {cat === 'all' ? 'All Categories' : cat}
                        </MenuItem>
                      ))}
                    </Select>
                  </FormControl>
                </Grid>
                <Grid item xs={12} md={2}>
                  <Typography variant="body2" color="text.secondary">
                    {filteredFields.length} fields
                  </Typography>
                </Grid>
              </Grid>
            </Box>

            <DroppableFormArea isEmpty={formFields.length === 0} onLoadTemplate={loadTemplate}>
                {formFields.length > 0 && (
                  <SortableContext
                    items={filteredFields.map(f => f.id)}
                    strategy={verticalListSortingStrategy}
                  >
                    {filteredFields.map((field) => (
                      <SortableField
                        key={field.id}
                        field={field}
                        onEdit={editField}
                        onDelete={deleteField}
                        onDuplicate={duplicateField}
                      />
                    ))}
                  </SortableContext>
                )}
              </DroppableFormArea>
          </Paper>
        </Grid>
        </Grid>
        
        <DragOverlay>
          {activeFieldId ? (
            activeFieldId.startsWith('toolbox-') ? (
              <Paper sx={{ p: 1.5, textAlign: 'center', opacity: 0.9, minWidth: 120 }}>
                <Typography variant="caption">
                  {FIELD_COMPONENTS.find(c => `toolbox-${c.type}` === activeFieldId)?.label}
                </Typography>
              </Paper>
            ) : (
              <Card sx={{ p: 2, opacity: 0.9 }}>
                <Typography>
                  {formFields.find(f => f.id === activeFieldId)?.name}
                </Typography>
              </Card>
            )
          ) : null}
        </DragOverlay>
      </DndContext>

      {/* Field Editor Dialog */}
      <Dialog 
        open={showFieldEditor} 
        onClose={() => setShowFieldEditor(false)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>
          <Typography variant="h6">
            {editingField?.id.startsWith('field_') ? 'Configure New Field' : 'Edit Field'}
          </Typography>
        </DialogTitle>
        <DialogContent dividers>
          {editingField && (
            <Grid container spacing={2} sx={{ mt: 1 }}>
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="Field Name"
                  value={editingField.name}
                  onChange={(e) => setEditingField({ ...editingField, name: e.target.value })}
                />
              </Grid>
              <Grid item xs={12} md={6}>
                <FormControl fullWidth>
                  <InputLabel>Field Type</InputLabel>
                  <Select
                    value={editingField.type}
                    label="Field Type"
                    onChange={(e) => setEditingField({ ...editingField, type: e.target.value })}
                  >
                    {Object.entries(FIELD_TYPES).map(([key, value]) => (
                      <MenuItem key={key} value={value}>{key}</MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Grid>
              <Grid item xs={12} md={6}>
                <FormControl fullWidth>
                  <InputLabel>CDISC Mapping</InputLabel>
                  <Select
                    value={editingField.cdiscMapping || ''}
                    label="CDISC Mapping"
                    onChange={(e) => setEditingField({ ...editingField, cdiscMapping: e.target.value })}
                  >
                    <MenuItem value="">None</MenuItem>
                    {Object.entries(CDISC_MAPPINGS).map(([key, value]) => (
                      <MenuItem key={key} value={key}>{key} - {value}</MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Grid>
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  label="Category"
                  value={editingField.category || ''}
                  onChange={(e) => setEditingField({ ...editingField, category: e.target.value })}
                />
              </Grid>
              <Grid item xs={12}>
                <FormControlLabel
                  control={
                    <Switch
                      checked={editingField.required || false}
                      onChange={(e) => setEditingField({ ...editingField, required: e.target.checked })}
                    />
                  }
                  label="Required Field"
                />
              </Grid>
              
              {/* Validation Rules */}
              {(editingField.type === FIELD_TYPES.TEXT || 
                editingField.type === FIELD_TYPES.NUMBER || 
                editingField.type === FIELD_TYPES.DECIMAL) && (
                <>
                  <Grid item xs={12}>
                    <Divider>Validation Rules</Divider>
                  </Grid>
                  {editingField.type === FIELD_TYPES.TEXT && (
                    <>
                      <Grid item xs={12} md={6}>
                        <TextField
                          fullWidth
                          label="Pattern (RegEx)"
                          value={editingField.validation?.pattern || ''}
                          onChange={(e) => setEditingField({
                            ...editingField,
                            validation: { ...editingField.validation, pattern: e.target.value }
                          })}
                        />
                      </Grid>
                      <Grid item xs={12} md={6}>
                        <TextField
                          fullWidth
                          label="Max Length"
                          type="number"
                          value={editingField.validation?.maxLength || ''}
                          onChange={(e) => setEditingField({
                            ...editingField,
                            validation: { ...editingField.validation, maxLength: e.target.value }
                          })}
                        />
                      </Grid>
                    </>
                  )}
                  {(editingField.type === FIELD_TYPES.NUMBER || editingField.type === FIELD_TYPES.DECIMAL) && (
                    <>
                      <Grid item xs={12} md={4}>
                        <TextField
                          fullWidth
                          label="Min Value"
                          type="number"
                          value={editingField.validation?.min || ''}
                          onChange={(e) => setEditingField({
                            ...editingField,
                            validation: { ...editingField.validation, min: e.target.value }
                          })}
                        />
                      </Grid>
                      <Grid item xs={12} md={4}>
                        <TextField
                          fullWidth
                          label="Max Value"
                          type="number"
                          value={editingField.validation?.max || ''}
                          onChange={(e) => setEditingField({
                            ...editingField,
                            validation: { ...editingField.validation, max: e.target.value }
                          })}
                        />
                      </Grid>
                      <Grid item xs={12} md={4}>
                        <TextField
                          fullWidth
                          label="Unit"
                          value={editingField.unit || ''}
                          onChange={(e) => setEditingField({ ...editingField, unit: e.target.value })}
                        />
                      </Grid>
                    </>
                  )}
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      label="Validation Message"
                      value={editingField.validation?.message || ''}
                      onChange={(e) => setEditingField({
                        ...editingField,
                        validation: { ...editingField.validation, message: e.target.value }
                      })}
                    />
                  </Grid>
                </>
              )}

              {/* Formula configuration for calculated fields */}
              {editingField.type === FIELD_TYPES.CALCULATED && (
                <>
                  <Grid item xs={12}>
                    <Divider>Calculation Formula</Divider>
                  </Grid>
                  <Grid item xs={12}>
                    <TextField
                      fullWidth
                      label="Formula"
                      multiline
                      rows={3}
                      value={editingField.formula || ''}
                      onChange={(e) => setEditingField({ ...editingField, formula: e.target.value })}
                      placeholder="e.g., weight / ((height/100) * (height/100)) or calculateAge(date_of_birth)"
                      helperText="Available functions: calculateAge(), sum(), average(), min(), max(). Reference other fields by their name."
                    />
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <TextField
                      fullWidth
                      label="Unit"
                      value={editingField.unit || ''}
                      onChange={(e) => setEditingField({ ...editingField, unit: e.target.value })}
                      placeholder="e.g., kg/m², years, mg/dL"
                    />
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <TextField
                      fullWidth
                      label="Decimal Places"
                      type="number"
                      value={editingField.decimals || ''}
                      onChange={(e) => setEditingField({ ...editingField, decimals: parseInt(e.target.value) || 0 })}
                      placeholder="Number of decimal places to display"
                    />
                  </Grid>
                  <Grid item xs={12}>
                    <FormControlLabel
                      control={
                        <Switch
                          checked={editingField.readonly || false}
                          onChange={(e) => setEditingField({ ...editingField, readonly: e.target.checked })}
                        />
                      }
                      label="Read-only (calculated fields are typically read-only)"
                    />
                  </Grid>
                </>
              )}

              {/* Options for dropdown, radio, multiselect */}
              {(editingField.type === FIELD_TYPES.DROPDOWN || 
                editingField.type === FIELD_TYPES.RADIO || 
                editingField.type === FIELD_TYPES.MULTISELECT) && (
                <>
                  <Grid item xs={12}>
                    <Divider>Options</Divider>
                  </Grid>
                  <Grid item xs={12}>
                    {editingField.options?.map((option: any, index: number) => (
                      <Box key={index} sx={{ display: 'flex', gap: 1, mb: 1 }}>
                        <TextField
                          size="small"
                          label="Value"
                          value={option.value}
                          onChange={(e) => {
                            const newOptions = [...editingField.options];
                            newOptions[index].value = e.target.value;
                            setEditingField({ ...editingField, options: newOptions });
                          }}
                        />
                        <TextField
                          size="small"
                          label="Label"
                          value={option.label}
                          onChange={(e) => {
                            const newOptions = [...editingField.options];
                            newOptions[index].label = e.target.value;
                            setEditingField({ ...editingField, options: newOptions });
                          }}
                          sx={{ flexGrow: 1 }}
                        />
                        <IconButton
                          size="small"
                          color="error"
                          onClick={() => {
                            const newOptions = editingField.options.filter((_: any, i: number) => i !== index);
                            setEditingField({ ...editingField, options: newOptions });
                          }}
                        >
                          <Delete />
                        </IconButton>
                      </Box>
                    ))}
                    <Button
                      startIcon={<Add />}
                      onClick={() => {
                        const newOptions = [...(editingField.options || []), { value: '', label: '' }];
                        setEditingField({ ...editingField, options: newOptions });
                      }}
                    >
                      Add Option
                    </Button>
                  </Grid>
                </>
              )}

              {/* Conditional Logic */}
              <Grid item xs={12}>
                <Divider>Conditional Logic (Optional)</Divider>
              </Grid>
              <Grid item xs={12}>
                <FormControlLabel
                  control={
                    <Switch
                      checked={!!editingField.conditionalLogic}
                      onChange={(e) => {
                        if (e.target.checked) {
                          setEditingField({
                            ...editingField,
                            conditionalLogic: {
                              field: '',
                              operator: 'equals',
                              value: '',
                              action: 'show',
                            }
                          });
                        } else {
                          const { conditionalLogic, ...rest } = editingField;
                          setEditingField(rest);
                        }
                      }}
                    />
                  }
                  label="Enable Conditional Logic"
                />
              </Grid>
              {editingField.conditionalLogic && (
                <>
                  <Grid item xs={12} md={3}>
                    <FormControl fullWidth size="small">
                      <InputLabel>If Field</InputLabel>
                      <Select
                        value={editingField.conditionalLogic.field || ''}
                        label="If Field"
                        onChange={(e) => setEditingField({
                          ...editingField,
                          conditionalLogic: {
                            ...editingField.conditionalLogic,
                            field: e.target.value
                          }
                        })}
                      >
                        {formFields.filter(f => f.id !== editingField.id).map(f => (
                          <MenuItem key={f.id} value={f.id}>{f.name}</MenuItem>
                        ))}
                      </Select>
                    </FormControl>
                  </Grid>
                  <Grid item xs={12} md={3}>
                    <FormControl fullWidth size="small">
                      <InputLabel>Operator</InputLabel>
                      <Select
                        value={editingField.conditionalLogic.operator || 'equals'}
                        label="Operator"
                        onChange={(e) => setEditingField({
                          ...editingField,
                          conditionalLogic: {
                            ...editingField.conditionalLogic,
                            operator: e.target.value
                          }
                        })}
                      >
                        <MenuItem value="equals">Equals</MenuItem>
                        <MenuItem value="not_equals">Not Equals</MenuItem>
                        <MenuItem value="contains">Contains</MenuItem>
                        <MenuItem value="greater_than">Greater Than</MenuItem>
                        <MenuItem value="less_than">Less Than</MenuItem>
                      </Select>
                    </FormControl>
                  </Grid>
                  <Grid item xs={12} md={3}>
                    <TextField
                      fullWidth
                      size="small"
                      label="Value"
                      value={editingField.conditionalLogic.value || ''}
                      onChange={(e) => setEditingField({
                        ...editingField,
                        conditionalLogic: {
                          ...editingField.conditionalLogic,
                          value: e.target.value
                        }
                      })}
                    />
                  </Grid>
                  <Grid item xs={12} md={3}>
                    <FormControl fullWidth size="small">
                      <InputLabel>Action</InputLabel>
                      <Select
                        value={editingField.conditionalLogic.action || 'show'}
                        label="Action"
                        onChange={(e) => setEditingField({
                          ...editingField,
                          conditionalLogic: {
                            ...editingField.conditionalLogic,
                            action: e.target.value
                          }
                        })}
                      >
                        <MenuItem value="show">Show Field</MenuItem>
                        <MenuItem value="hide">Hide Field</MenuItem>
                        <MenuItem value="enable">Enable Field</MenuItem>
                        <MenuItem value="disable">Disable Field</MenuItem>
                      </Select>
                    </FormControl>
                  </Grid>
                </>
              )}
            </Grid>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setShowFieldEditor(false)}>Cancel</Button>
          <Button 
            onClick={() => updateField(editingField)} 
            variant="contained"
            startIcon={<Save />}
          >
            Save Field
          </Button>
        </DialogActions>
      </Dialog>

      {/* JSON Preview Dialog */}
      <Dialog
        open={showJsonPreview}
        onClose={() => setShowJsonPreview(false)}
        maxWidth="lg"
        fullWidth
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Typography variant="h6">Form Definition JSON</Typography>
            <IconButton onClick={() => setShowJsonPreview(false)}>
              <Close />
            </IconButton>
          </Box>
        </DialogTitle>
        <DialogContent dividers>
          <Paper sx={{ p: 2, backgroundColor: 'grey.900', overflow: 'auto' }}>
            <pre style={{ 
              color: '#fff', 
              margin: 0,
              fontSize: '12px',
              fontFamily: 'Monaco, monospace'
            }}>
              {JSON.stringify(generateJSON(), null, 2)}
            </pre>
          </Paper>
        </DialogContent>
        <DialogActions>
          <Button
            startIcon={<ContentCopy />}
            onClick={() => {
              navigator.clipboard.writeText(JSON.stringify(generateJSON(), null, 2));
              setSnackbar({
                open: true,
                message: 'JSON copied to clipboard!',
                severity: 'success',
              });
            }}
          >
            Copy to Clipboard
          </Button>
          <Button
            startIcon={<Download />}
            onClick={exportForm}
            variant="contained"
          >
            Export JSON
          </Button>
        </DialogActions>
      </Dialog>

      {/* Form Preview Dialog */}
      <Dialog
        open={showFormPreview}
        onClose={() => setShowFormPreview(false)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Typography variant="h6">Interactive Form Preview</Typography>
            <IconButton onClick={() => setShowFormPreview(false)}>
              <Close />
            </IconButton>
          </Box>
        </DialogTitle>
        <DialogContent dividers>
          <Alert severity="success" sx={{ mb: 2 }}>
            <strong>Live Preview:</strong> This is an interactive preview. Calculated fields will auto-update when you enter values!
          </Alert>
          <FormPreviewWithCalculations formFields={formFields} formName={formName} formDescription={formDescription} />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setShowFormPreview(false)}>Close</Button>
        </DialogActions>
      </Dialog>

      {/* Snackbar for notifications */}
      <Snackbar
        open={snackbar.open}
        autoHideDuration={4000}
        onClose={() => setSnackbar({ ...snackbar, open: false })}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
      >
        <Alert
          onClose={() => setSnackbar({ ...snackbar, open: false })}
          severity={snackbar.severity}
          sx={{ width: '100%' }}
        >
          {snackbar.message}
        </Alert>
      </Snackbar>
    </Container>
  );
};

export default FormBuilder;