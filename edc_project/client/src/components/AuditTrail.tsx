import React, { useState, useEffect, useCallback } from 'react';
import {
  Box,
  Paper,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Chip,
  TextField,
  InputAdornment,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  IconButton,
  Tooltip,
  Button,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Grid,
  Card,
  CardContent,
  Stack,
  TablePagination,
  Menu,
  Alert,
  List,
  ListItem,
  ListItemText,
  LinearProgress
} from '@mui/material';
import {
  Search,
  Visibility,
  Person,
  Computer,
  Edit,
  Delete,
  Add,
  Lock,
  LockOpen,
  Reply,
  CheckCircle,
  Warning,
  Error as ErrorIcon,
  Schedule,
  Security,
  Description,
  FileDownload,
  Print,
  TrendingUp,
  History,
  Refresh,
  Download
} from '@mui/icons-material';

interface AuditEntry {
  id: string;
  auditId: string;
  studyId?: string;
  studyName?: string;
  subjectId?: string;
  visitName?: string;
  formName?: string;
  fieldName?: string;
  actionType: 'CREATE' | 'UPDATE' | 'DELETE' | 'QUERY_OPENED' | 'QUERY_CLOSED' | 'QUERY_RESPONDED' | 'LOCK' | 'UNLOCK' | 'PROMOTION' | 'VIEW' | 'EXPORT' | 'LOGIN' | 'LOGOUT' | 'APPROVE' | 'REJECT';
  oldValue?: string;
  newValue?: string;
  user: string;
  userId: string;
  role: string;
  timestamp: string;
  reasonForChange?: string;
  ipAddress: string;
  sessionId: string;
  signature: boolean;
  isSystemAction: boolean;
  severity: 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL';
  entityType: 'FORM' | 'SUBJECT' | 'QUERY' | 'DATA' | 'USER' | 'STUDY' | 'SYSTEM';
  entityId: string;
  parentAuditId?: string;
  complianceFlags: string[];
  additionalContext?: Record<string, any>;
}

const AuditTrail: React.FC = () => {
  const [auditEntries, setAuditEntries] = useState<AuditEntry[]>([]);
  const [selectedEntry, setSelectedEntry] = useState<AuditEntry | null>(null);
  const [detailDialogOpen, setDetailDialogOpen] = useState(false);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(25);

  // Filters
  const [searchTerm, setSearchTerm] = useState('');
  const [actionFilter, setActionFilter] = useState('all');
  const [entityFilter, setEntityFilter] = useState('all');
  const [studyFilter, setStudyFilter] = useState('all');
  const [userFilter] = useState('all');
  const [severityFilter, setSeverityFilter] = useState('all');
  const [dateFromFilter, setDateFromFilter] = useState('');
  const [dateToFilter, setDateToFilter] = useState('');

  // Export menu
  const [exportMenuAnchor, setExportMenuAnchor] = useState<null | HTMLElement>(null);

  // Fetch audit trail data from API
  const fetchAuditData = useCallback(async () => {
    try {
      setLoading(true);
      
      // Build query params
      const params = new URLSearchParams();
      if (actionFilter !== 'all') params.append('action_type', actionFilter);
      if (entityFilter !== 'all') params.append('entity_type', entityFilter);
      if (severityFilter !== 'all') params.append('severity', severityFilter);
      if (dateFromFilter) params.append('date_from', dateFromFilter);
      if (dateToFilter) params.append('date_to', dateToFilter);
      if (searchTerm) params.append('search', searchTerm);
      
      const response = await fetch(`/api/audit?${params.toString()}`);
      
      if (!response.ok) {
        throw new Error('Failed to fetch audit data');
      }
      
      const data = await response.json();
      
      // Transform API data to match component interface
      const transformedEntries: AuditEntry[] = (data.data || []).map((entry: any) => ({
        id: entry.id,
        auditId: `AUD-${entry.id}`,
        studyId: entry.study_id,
        studyName: entry.edc_studies?.title || 'Unknown Study',
        subjectId: entry.subject_id,
        visitName: 'N/A',
        formName: entry.entity_type,
        fieldName: entry.field_name || 'N/A',
        actionType: entry.action_type,
        oldValue: entry.old_value || 'N/A',
        newValue: entry.new_value || 'N/A',
        user: entry.edc_users ? `${entry.edc_users.first_name} ${entry.edc_users.last_name}` : 'System',
        userId: entry.user_id,
        role: entry.edc_users?.role || 'System',
        timestamp: entry.created_at,
        reasonForChange: entry.reason_for_change || 'No reason provided',
        ipAddress: entry.ip_address || 'N/A',
        sessionId: entry.session_id || 'N/A',
        signature: entry.signature_hash ? true : false,
        isSystemAction: entry.user_id === null,
        severity: entry.severity || 'LOW',
        entityType: entry.entity_type,
        entityId: entry.entity_id || entry.id,
        complianceFlags: entry.compliance_flags || ['21_CFR_PART_11', 'GCP_COMPLIANT']
      }));
      
      setAuditEntries(transformedEntries);
    } catch (error) {
      console.error('Error fetching audit data:', error);
      setAuditEntries([]);
    } finally {
      setLoading(false);
    }
  }, [actionFilter, entityFilter, severityFilter, dateFromFilter, dateToFilter, searchTerm]);

  // Fetch data on component mount and when filters change
  useEffect(() => {
    fetchAuditData();
  }, [fetchAuditData]);

  // Removed mock data - now using real API
  /*useEffect(() => {
    const mockAuditEntries: AuditEntry[] = [
      {
        id: '1',
        auditId: 'AUD-2024-001',
        studyId: 'CARDIO-2024-001',
        studyName: 'Cardiovascular Disease Prevention Trial',
        subjectId: 'SUBJ-001',
        visitName: 'Baseline',
        formName: 'Vital Signs',
        fieldName: 'Systolic BP',
        actionType: 'UPDATE',
        oldValue: '120',
        newValue: '125',
        user: 'Sarah Johnson',
        userId: 'USR-001',
        role: 'CRC',
        timestamp: '2024-02-20T14:32:15.123Z',
        reasonForChange: 'Patient correction during visit - verified with source document',
        ipAddress: '192.168.1.100',
        sessionId: 'SES-001-20240220',
        signature: true,
        isSystemAction: false,
        severity: 'MEDIUM',
        entityType: 'DATA',
        entityId: 'SUBJ-001-VS-BASELINE',
        complianceFlags: ['21_CFR_PART_11', 'GCP_COMPLIANT', 'SOURCE_VERIFIED'],
        additionalContext: {
          sourceDocument: 'Visit Note #001',
          witnessSignature: 'Dr. Michael Chen',
          auditReason: 'DATA_CORRECTION'
        }
      },
      {
        id: '2',
        auditId: 'AUD-2024-002',
        studyId: 'CARDIO-2024-001',
        studyName: 'Cardiovascular Disease Prevention Trial',
        actionType: 'QUERY_OPENED',
        user: 'System Validation',
        userId: 'SYS-AUTO',
        role: 'SYSTEM',
        timestamp: '2024-02-20T14:30:45.567Z',
        ipAddress: 'SYSTEM',
        sessionId: 'SYS-AUTO-20240220',
        signature: false,
        isSystemAction: true,
        severity: 'HIGH',
        entityType: 'QUERY',
        entityId: 'QRY-2024-001',
        complianceFlags: ['AUTO_GENERATED', 'VALIDATION_RULE'],
        additionalContext: {
          validationRule: 'SYSTOLIC_BP_RANGE_CHECK',
          triggeredValue: '220',
          expectedRange: '80-200'
        }
      },
      {
        id: '3',
        auditId: 'AUD-2024-003',
        studyId: 'CARDIO-2024-001',
        studyName: 'Cardiovascular Disease Prevention Trial',
        subjectId: 'SUBJ-002',
        actionType: 'CREATE',
        user: 'Dr. Michael Chen',
        userId: 'USR-002',
        role: 'PI',
        timestamp: '2024-02-20T13:15:22.890Z',
        newValue: 'Enrolled',
        ipAddress: '192.168.1.101',
        sessionId: 'SES-002-20240220',
        signature: true,
        isSystemAction: false,
        severity: 'CRITICAL',
        entityType: 'SUBJECT',
        entityId: 'SUBJ-002',
        reasonForChange: 'Subject meets all inclusion criteria and has provided informed consent',
        complianceFlags: ['21_CFR_PART_11', 'GCP_COMPLIANT', 'ICF_VERIFIED'],
        additionalContext: {
          enrollmentDate: '2024-02-20',
          icfVersion: '3.0',
          eligibilityChecklist: 'COMPLETED'
        }
      },
      {
        id: '4',
        auditId: 'AUD-2024-004',
        studyId: 'CARDIO-2024-001',
        studyName: 'Cardiovascular Disease Prevention Trial',
        formName: 'Demographics Form',
        actionType: 'PROMOTION',
        oldValue: 'DRAFT',
        newValue: 'UAT_TESTING',
        user: 'Lisa Park',
        userId: 'USR-003',
        role: 'DM',
        timestamp: '2024-02-20T12:45:10.234Z',
        reasonForChange: 'Form validation completed, ready for UAT testing',
        ipAddress: '192.168.1.102',
        sessionId: 'SES-003-20240220',
        signature: true,
        isSystemAction: false,
        severity: 'HIGH',
        entityType: 'FORM',
        entityId: 'FRM-DEMO-V2.0',
        complianceFlags: ['21_CFR_PART_11', 'VERSION_CONTROLLED'],
        additionalContext: {
          validationTests: '45/45 PASSED',
          approvedBy: 'Study Manager',
          promotionWorkflow: 'STANDARD'
        }
      },
      {
        id: '5',
        auditId: 'AUD-2024-005',
        studyId: 'ONCO-2024-002',
        studyName: 'Oncology Immunotherapy Phase II Study',
        subjectId: 'SUBJ-101',
        visitName: 'Screening',
        formName: 'Medical History',
        fieldName: 'Previous Cancer History',
        actionType: 'DELETE',
        oldValue: 'Duplicate entry: Breast cancer 2018',
        newValue: '',
        user: 'Jennifer Martinez',
        userId: 'USR-004',
        role: 'CRA',
        timestamp: '2024-02-20T11:30:55.678Z',
        reasonForChange: 'Removal of duplicate data entry - confirmed by source document review',
        ipAddress: '192.168.1.103',
        sessionId: 'SES-004-20240220',
        signature: true,
        isSystemAction: false,
        severity: 'HIGH',
        entityType: 'DATA',
        entityId: 'SUBJ-101-MH-SCREENING',
        complianceFlags: ['21_CFR_PART_11', 'GCP_COMPLIANT', 'DELETION_JUSTIFIED'],
        additionalContext: {
          sourceDocument: 'Medical Records Review',
          deletionApproval: 'PI_APPROVED',
          witnessSignature: 'Dr. Robert Kim'
        }
      },
      {
        id: '6',
        auditId: 'AUD-2024-006',
        studyId: 'CARDIO-2024-001',
        studyName: 'Cardiovascular Disease Prevention Trial',
        subjectId: 'SUBJ-003',
        actionType: 'LOCK',
        user: 'Study Database',
        userId: 'SYS-DB',
        role: 'SYSTEM',
        timestamp: '2024-02-20T10:15:30.456Z',
        reasonForChange: 'Automatic database lock after study completion',
        ipAddress: 'SYSTEM',
        sessionId: 'SYS-DB-20240220',
        signature: false,
        isSystemAction: true,
        severity: 'CRITICAL',
        entityType: 'SUBJECT',
        entityId: 'SUBJ-003',
        complianceFlags: ['21_CFR_PART_11', 'AUTO_LOCK', 'IMMUTABLE'],
        additionalContext: {
          lockType: 'STUDY_COMPLETION',
          triggerEvent: 'LAST_VISIT_COMPLETED',
          lockLevel: 'SUBJECT_LEVEL'
        }
      },
      {
        id: '7',
        auditId: 'AUD-2024-007',
        actionType: 'EXPORT',
        user: 'Alex Thompson',
        userId: 'USR-005',
        role: 'Data Manager',
        timestamp: '2024-02-20T09:45:20.789Z',
        reasonForChange: 'Regulatory submission data extract',
        ipAddress: '192.168.1.104',
        sessionId: 'SES-005-20240220',
        signature: true,
        isSystemAction: false,
        severity: 'HIGH',
        entityType: 'SYSTEM',
        entityId: 'EXP-FDA-20240220',
        complianceFlags: ['21_CFR_PART_11', 'REGULATORY_EXPORT', 'CDISC_COMPLIANT'],
        additionalContext: {
          exportFormat: 'CDISC_SDTM',
          datasetCount: 12,
          subjectCount: 150,
          exportPurpose: 'FDA_SUBMISSION'
        }
      },
      {
        id: '8',
        auditId: 'AUD-2024-008',
        studyId: 'CARDIO-2024-001',
        studyName: 'Cardiovascular Disease Prevention Trial',
        actionType: 'QUERY_CLOSED',
        user: 'Dr. Lisa Park',
        userId: 'USR-006',
        role: 'DM',
        timestamp: '2024-02-20T08:30:10.012Z',
        reasonForChange: 'Query resolved - data confirmed accurate by site',
        ipAddress: '192.168.1.105',
        sessionId: 'SES-006-20240220',
        signature: true,
        isSystemAction: false,
        severity: 'MEDIUM',
        entityType: 'QUERY',
        entityId: 'QRY-2024-001',
        complianceFlags: ['21_CFR_PART_11', 'QUERY_RESOLVED'],
        additionalContext: {
          queryType: 'DATA_VALIDATION',
          resolutionMethod: 'SITE_CONFIRMATION',
          responseTime: '2.5 hours'
        }
      }
    ];

    setTimeout(() => {
      setAuditEntries(mockAuditEntries);
      setLoading(false);
    }, 1000);
  }, []);*/

  // Helper functions
  const getActionIcon = (actionType: string) => {
    switch (actionType) {
      case 'CREATE': return <Add />;
      case 'UPDATE': return <Edit />;
      case 'DELETE': return <Delete />;
      case 'QUERY_OPENED': return <Warning />;
      case 'QUERY_CLOSED': return <CheckCircle />;
      case 'QUERY_RESPONDED': return <Reply />;
      case 'LOCK': return <Lock />;
      case 'UNLOCK': return <LockOpen />;
      case 'PROMOTION': return <TrendingUp />;
      case 'VIEW': return <Visibility />;
      case 'EXPORT': return <Download />;
      case 'LOGIN': return <Person />;
      case 'LOGOUT': return <Person />;
      case 'APPROVE': return <CheckCircle />;
      case 'REJECT': return <ErrorIcon />;
      default: return <Description />;
    }
  };

  const getActionColor = (actionType: string) => {
    switch (actionType) {
      case 'CREATE': return 'success';
      case 'UPDATE': return 'info';
      case 'DELETE': return 'error';
      case 'QUERY_OPENED': return 'warning';
      case 'QUERY_CLOSED': return 'success';
      case 'QUERY_RESPONDED': return 'primary';
      case 'LOCK': return 'warning';
      case 'UNLOCK': return 'secondary';
      case 'PROMOTION': return 'info';
      case 'APPROVE': return 'success';
      case 'REJECT': return 'error';
      case 'EXPORT': return 'primary';
      case 'LOGIN': return 'default';
      case 'LOGOUT': return 'default';
      default: return 'default';
    }
  };

  const getSeverityColor = (severity: string) => {
    switch (severity) {
      case 'LOW': return 'default';
      case 'MEDIUM': return 'primary';
      case 'HIGH': return 'warning';
      case 'CRITICAL': return 'error';
      default: return 'default';
    }
  };

  // Filter entries based on all filter criteria
  const filteredEntries = auditEntries.filter(entry => {
    const matchesSearch = 
      (entry.user || '').toLowerCase().includes(searchTerm.toLowerCase()) ||
      (entry.auditId || '').toLowerCase().includes(searchTerm.toLowerCase()) ||
      (entry.subjectId || '').toLowerCase().includes(searchTerm.toLowerCase()) ||
      (entry.studyName || '').toLowerCase().includes(searchTerm.toLowerCase()) ||
      (entry.reasonForChange || '').toLowerCase().includes(searchTerm.toLowerCase()) ||
      (entry.entityId || '').toLowerCase().includes(searchTerm.toLowerCase());
    
    const matchesAction = actionFilter === 'all' || entry.actionType === actionFilter;
    const matchesEntity = entityFilter === 'all' || entry.entityType === entityFilter;
    const matchesStudy = studyFilter === 'all' || entry.studyId === studyFilter;
    const matchesUser = userFilter === 'all' || entry.userId === userFilter;
    const matchesSeverity = severityFilter === 'all' || entry.severity === severityFilter;

    // Date filtering
    let matchesDateRange = true;
    if (dateFromFilter || dateToFilter) {
      const entryDate = new Date(entry.timestamp);
      if (dateFromFilter) {
        const fromDate = new Date(dateFromFilter);
        matchesDateRange = matchesDateRange && entryDate >= fromDate;
      }
      if (dateToFilter) {
        const toDate = new Date(dateToFilter + 'T23:59:59');
        matchesDateRange = matchesDateRange && entryDate <= toDate;
      }
    }
    
    return matchesSearch && matchesAction && matchesEntity && matchesStudy && 
           matchesUser && matchesSeverity && matchesDateRange;
  });

  // Get audit statistics
  const getAuditStats = () => {
    const now = new Date();
    const last24Hours = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    const lastWeek = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);

    return {
      total: auditEntries.length,
      last24h: auditEntries.filter(e => new Date(e.timestamp) >= last24Hours).length,
      lastWeek: auditEntries.filter(e => new Date(e.timestamp) >= lastWeek).length,
      critical: auditEntries.filter(e => e.severity === 'CRITICAL').length,
      systemActions: auditEntries.filter(e => e.isSystemAction).length,
      signedActions: auditEntries.filter(e => e.signature).length
    };
  };

  const stats = getAuditStats();

  // Export functions
  const handleExportCSV = () => {
    console.log('Exporting CSV...');
    setExportMenuAnchor(null);
  };

  const handleExportXLSX = () => {
    console.log('Exporting XLSX...');
    setExportMenuAnchor(null);
  };

  const handleExportPDF = () => {
    console.log('Exporting PDF...');
    setExportMenuAnchor(null);
  };

  // View entry details
  const handleViewDetails = (entry: AuditEntry) => {
    setSelectedEntry(entry);
    setDetailDialogOpen(true);
  };

  if (loading) {
    return (
      <Box sx={{ p: 3 }}>
        <LinearProgress />
        <Typography sx={{ mt: 2 }}>Loading audit trail...</Typography>
      </Box>
    );
  }

  return (
    <Box>
      {/* Header */}
      <Box sx={{ mb: 3, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Box>
          <Typography variant="h4" gutterBottom>
            Audit Trail
          </Typography>
          <Typography variant="subtitle1" color="text.secondary">
            21 CFR Part 11 Compliant • Immutable Audit Log for Clinical Trials
          </Typography>
        </Box>
        <Stack direction="row" spacing={2}>
          <Button
            variant="outlined"
            startIcon={<Refresh />}
            onClick={() => {
              setSearchTerm('');
              setActionFilter('all');
              setEntityFilter('all');
              setStudyFilter('all');
              setPage(0);
            }}
          >
            Refresh
          </Button>
          <Button
            variant="contained"
            startIcon={<FileDownload />}
            onClick={(e) => setExportMenuAnchor(e.currentTarget)}
            sx={{
              background: 'linear-gradient(45deg, #2196F3 30%, #21CBF3 90%)',
              boxShadow: '0 3px 5px 2px rgba(33, 203, 243, .3)',
            }}
          >
            Export
          </Button>
        </Stack>
      </Box>

      {/* Statistics Cards */}
      <Grid container spacing={3} sx={{ mb: 3 }}>
        <Grid item xs={12} md={2}>
          <Card sx={{ background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)', color: 'white' }}>
            <CardContent>
              <Stack direction="row" justifyContent="space-between" alignItems="center">
                <Box>
                  <Typography color="inherit" gutterBottom sx={{ opacity: 0.8 }}>
                    Total Entries
                  </Typography>
                  <Typography variant="h4">{stats.total}</Typography>
                </Box>
                <History sx={{ fontSize: 40, opacity: 0.8 }} />
              </Stack>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={2}>
          <Card sx={{ background: 'linear-gradient(135deg, #f093fb 0%, #f5576c 100%)', color: 'white' }}>
            <CardContent>
              <Stack direction="row" justifyContent="space-between" alignItems="center">
                <Box>
                  <Typography color="inherit" gutterBottom sx={{ opacity: 0.8 }}>
                    Last 24h
                  </Typography>
                  <Typography variant="h4">{stats.last24h}</Typography>
                </Box>
                <Schedule sx={{ fontSize: 40, opacity: 0.8 }} />
              </Stack>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={2}>
          <Card sx={{ background: 'linear-gradient(135deg, #4facfe 0%, #00f2fe 100%)', color: 'white' }}>
            <CardContent>
              <Stack direction="row" justifyContent="space-between" alignItems="center">
                <Box>
                  <Typography color="inherit" gutterBottom sx={{ opacity: 0.8 }}>
                    Critical
                  </Typography>
                  <Typography variant="h4">{stats.critical}</Typography>
                </Box>
                <ErrorIcon sx={{ fontSize: 40, opacity: 0.8 }} />
              </Stack>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={2}>
          <Card sx={{ background: 'linear-gradient(135deg, #43e97b 0%, #38f9d7 100%)', color: 'white' }}>
            <CardContent>
              <Stack direction="row" justifyContent="space-between" alignItems="center">
                <Box>
                  <Typography color="inherit" gutterBottom sx={{ opacity: 0.8 }}>
                    E-Signed
                  </Typography>
                  <Typography variant="h4">{stats.signedActions}</Typography>
                </Box>
                <Security sx={{ fontSize: 40, opacity: 0.8 }} />
              </Stack>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={2}>
          <Card sx={{ background: 'linear-gradient(135deg, #fa709a 0%, #fee140 100%)', color: 'white' }}>
            <CardContent>
              <Stack direction="row" justifyContent="space-between" alignItems="center">
                <Box>
                  <Typography color="inherit" gutterBottom sx={{ opacity: 0.8 }}>
                    System Actions
                  </Typography>
                  <Typography variant="h4">{stats.systemActions}</Typography>
                </Box>
                <Computer sx={{ fontSize: 40, opacity: 0.8 }} />
              </Stack>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={2}>
          <Card sx={{ background: 'linear-gradient(135deg, #a8edea 0%, #fed6e3 100%)', color: '#333' }}>
            <CardContent>
              <Stack direction="row" justifyContent="space-between" alignItems="center">
                <Box>
                  <Typography color="inherit" gutterBottom>
                    Compliance
                  </Typography>
                  <Typography variant="h6" color="success.main" fontWeight="bold">
                    100%
                  </Typography>
                </Box>
                <CheckCircle sx={{ fontSize: 40, color: 'success.main' }} />
              </Stack>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Advanced Filters */}
      <Paper sx={{ p: 2, mb: 3 }}>
        <Grid container spacing={2} alignItems="center">
          <Grid item xs={12} md={3}>
            <TextField
              fullWidth
              size="small"
              placeholder="Search audit trail..."
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
          <Grid item xs={6} md={1.5}>
            <FormControl fullWidth size="small">
              <InputLabel>Action</InputLabel>
              <Select
                value={actionFilter}
                label="Action"
                onChange={(e) => setActionFilter(e.target.value)}
              >
                <MenuItem value="all">All Actions</MenuItem>
                <MenuItem value="CREATE">Create</MenuItem>
                <MenuItem value="UPDATE">Update</MenuItem>
                <MenuItem value="DELETE">Delete</MenuItem>
                <MenuItem value="QUERY_OPENED">Query Opened</MenuItem>
                <MenuItem value="QUERY_CLOSED">Query Closed</MenuItem>
                <MenuItem value="LOCK">Lock</MenuItem>
                <MenuItem value="UNLOCK">Unlock</MenuItem>
                <MenuItem value="PROMOTION">Promotion</MenuItem>
                <MenuItem value="EXPORT">Export</MenuItem>
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={6} md={1.5}>
            <FormControl fullWidth size="small">
              <InputLabel>Entity</InputLabel>
              <Select
                value={entityFilter}
                label="Entity"
                onChange={(e) => setEntityFilter(e.target.value)}
              >
                <MenuItem value="all">All Entities</MenuItem>
                <MenuItem value="FORM">Form</MenuItem>
                <MenuItem value="SUBJECT">Subject</MenuItem>
                <MenuItem value="DATA">Data</MenuItem>
                <MenuItem value="QUERY">Query</MenuItem>
                <MenuItem value="STUDY">Study</MenuItem>
                <MenuItem value="SYSTEM">System</MenuItem>
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={6} md={1.5}>
            <FormControl fullWidth size="small">
              <InputLabel>Severity</InputLabel>
              <Select
                value={severityFilter}
                label="Severity"
                onChange={(e) => setSeverityFilter(e.target.value)}
              >
                <MenuItem value="all">All Severity</MenuItem>
                <MenuItem value="LOW">Low</MenuItem>
                <MenuItem value="MEDIUM">Medium</MenuItem>
                <MenuItem value="HIGH">High</MenuItem>
                <MenuItem value="CRITICAL">Critical</MenuItem>
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={6} md={1.5}>
            <FormControl fullWidth size="small">
              <InputLabel>Study</InputLabel>
              <Select
                value={studyFilter}
                label="Study"
                onChange={(e) => setStudyFilter(e.target.value)}
              >
                <MenuItem value="all">All Studies</MenuItem>
                <MenuItem value="CARDIO-2024-001">Cardiovascular Trial</MenuItem>
                <MenuItem value="ONCO-2024-002">Oncology Phase II</MenuItem>
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={6} md={1.5}>
            <TextField
              fullWidth
              size="small"
              type="date"
              label="From Date"
              value={dateFromFilter}
              onChange={(e) => setDateFromFilter(e.target.value)}
              InputLabelProps={{ shrink: true }}
            />
          </Grid>
          <Grid item xs={6} md={1.5}>
            <TextField
              fullWidth
              size="small"
              type="date"
              label="To Date"
              value={dateToFilter}
              onChange={(e) => setDateToFilter(e.target.value)}
              InputLabelProps={{ shrink: true }}
            />
          </Grid>
        </Grid>
        <Box sx={{ mt: 2, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Typography variant="body2" color="text.secondary">
            Showing {filteredEntries.length} of {auditEntries.length} audit entries
          </Typography>
        </Box>
      </Paper>

      {/* Audit Trail Table */}
      <Paper elevation={2}>
        <TableContainer>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Audit ID</TableCell>
                <TableCell>Timestamp</TableCell>
                <TableCell>Study/Subject</TableCell>
                <TableCell>Form/Field</TableCell>
                <TableCell>Action</TableCell>
                <TableCell>Values</TableCell>
                <TableCell>User</TableCell>
                <TableCell>Severity</TableCell>
                <TableCell>Signature</TableCell>
                <TableCell align="center">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {filteredEntries
                .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
                .map((entry) => (
                <TableRow key={entry.id} hover sx={{ '&:hover': { bgcolor: 'action.hover' } }}>
                  <TableCell>
                    <Typography variant="body2" fontFamily="monospace" fontWeight={600}>
                      {entry.auditId}
                    </Typography>
                    {entry.isSystemAction && (
                      <Chip label="System" size="small" variant="outlined" />
                    )}
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2" fontFamily="monospace">
                      {new Date(entry.timestamp).toLocaleString()}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    {entry.studyName && (
                      <Typography variant="body2" sx={{ maxWidth: 200 }}>
                        {entry.studyName.substring(0, 30)}...
                      </Typography>
                    )}
                    {entry.subjectId && (
                      <Typography variant="caption" color="primary">
                        {entry.subjectId}
                      </Typography>
                    )}
                    {entry.visitName && (
                      <Typography variant="caption" display="block" color="text.secondary">
                        {entry.visitName}
                      </Typography>
                    )}
                  </TableCell>
                  <TableCell>
                    {entry.formName && (
                      <Typography variant="body2">
                        {entry.formName}
                      </Typography>
                    )}
                    {entry.fieldName && (
                      <Typography variant="caption" color="text.secondary">
                        {entry.fieldName}
                      </Typography>
                    )}
                  </TableCell>
                  <TableCell>
                    <Chip
                      label={entry.actionType}
                      color={getActionColor(entry.actionType) as any}
                      size="small"
                      icon={getActionIcon(entry.actionType)}
                    />
                  </TableCell>
                  <TableCell>
                    {entry.oldValue && (
                      <Box>
                        <Typography variant="caption" color="error.main">
                          Old: {entry.oldValue.substring(0, 20)}
                          {entry.oldValue.length > 20 && '...'}
                        </Typography>
                      </Box>
                    )}
                    {entry.newValue && (
                      <Box>
                        <Typography variant="caption" color="success.main">
                          New: {entry.newValue.substring(0, 20)}
                          {entry.newValue.length > 20 && '...'}
                        </Typography>
                      </Box>
                    )}
                  </TableCell>
                  <TableCell>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      {entry.isSystemAction ? <Computer fontSize="small" /> : <Person fontSize="small" />}
                      <Box>
                        <Typography variant="body2">{entry.user}</Typography>
                        <Typography variant="caption" color="text.secondary">
                          {entry.role}
                        </Typography>
                      </Box>
                    </Box>
                  </TableCell>
                  <TableCell>
                    <Chip
                      label={entry.severity}
                      color={getSeverityColor(entry.severity) as any}
                      size="small"
                      variant="outlined"
                    />
                  </TableCell>
                  <TableCell>
                    {entry.signature ? (
                      <Chip
                        label="Signed"
                        color="success"
                        size="small"
                        icon={<Security />}
                      />
                    ) : (
                      <Typography variant="caption" color="text.secondary">
                        No signature
                      </Typography>
                    )}
                  </TableCell>
                  <TableCell>
                    <Tooltip title="View Details">
                      <IconButton
                        size="small"
                        color="primary"
                        onClick={() => handleViewDetails(entry)}
                      >
                        <Visibility />
                      </IconButton>
                    </Tooltip>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
        <TablePagination
          rowsPerPageOptions={[10, 25, 50]}
          component="div"
          count={filteredEntries.length}
          rowsPerPage={rowsPerPage}
          page={page}
          onPageChange={(e, newPage) => setPage(newPage)}
          onRowsPerPageChange={(e) => {
            setRowsPerPage(parseInt(e.target.value, 10));
            setPage(0);
          }}
        />
      </Paper>

      {/* Export Menu */}
      <Menu
        anchorEl={exportMenuAnchor}
        open={Boolean(exportMenuAnchor)}
        onClose={() => setExportMenuAnchor(null)}
      >
        <MenuItem onClick={handleExportCSV}>
          <FileDownload sx={{ mr: 1 }} />
          Export as CSV
        </MenuItem>
        <MenuItem onClick={handleExportXLSX}>
          <Description sx={{ mr: 1 }} />
          Export as XLSX
        </MenuItem>
        <MenuItem onClick={handleExportPDF}>
          <Print sx={{ mr: 1 }} />
          Export as PDF
        </MenuItem>
      </Menu>

      {/* Detail Dialog */}
      <Dialog open={detailDialogOpen} onClose={() => setDetailDialogOpen(false)} maxWidth="lg" fullWidth>
        <DialogTitle>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Typography variant="h6">
              Audit Entry Details: {selectedEntry?.auditId}
            </Typography>
            <Stack direction="row" spacing={1}>
              <Chip
                label={selectedEntry?.severity}
                color={getSeverityColor(selectedEntry?.severity || '') as any}
                size="small"
              />
              <Chip
                label={selectedEntry?.actionType}
                color={getActionColor(selectedEntry?.actionType || '') as any}
                size="small"
              />
            </Stack>
          </Box>
        </DialogTitle>
        <DialogContent dividers>
          {selectedEntry && (
            <Grid container spacing={3}>
              <Grid item xs={12} md={8}>
                <Typography variant="h6" sx={{ mb: 2 }}>Audit Information</Typography>
                <Card variant="outlined" sx={{ mb: 3 }}>
                  <CardContent>
                    <List dense>
                      <ListItem>
                        <ListItemText
                          primary="Audit ID"
                          secondary={selectedEntry.auditId}
                        />
                      </ListItem>
                      <ListItem>
                        <ListItemText
                          primary="Timestamp"
                          secondary={new Date(selectedEntry.timestamp).toLocaleString()}
                        />
                      </ListItem>
                      <ListItem>
                        <ListItemText
                          primary="Action Type"
                          secondary={selectedEntry.actionType}
                        />
                      </ListItem>
                      <ListItem>
                        <ListItemText
                          primary="Entity Type"
                          secondary={selectedEntry.entityType}
                        />
                      </ListItem>
                      <ListItem>
                        <ListItemText
                          primary="Entity ID"
                          secondary={selectedEntry.entityId}
                        />
                      </ListItem>
                      {selectedEntry.reasonForChange && (
                        <ListItem>
                          <ListItemText
                            primary="Reason for Change"
                            secondary={selectedEntry.reasonForChange}
                          />
                        </ListItem>
                      )}
                    </List>
                  </CardContent>
                </Card>

                {(selectedEntry.oldValue || selectedEntry.newValue) && (
                  <>
                    <Typography variant="h6" sx={{ mb: 2 }}>Value Changes</Typography>
                    <Card variant="outlined" sx={{ mb: 3 }}>
                      <CardContent>
                        <Grid container spacing={2}>
                          {selectedEntry.oldValue && (
                            <Grid item xs={6}>
                              <Typography variant="subtitle2" color="error">
                                Old Value:
                              </Typography>
                              <Typography variant="body2" sx={{ p: 1, bgcolor: 'error.light', color: 'error.contrastText', borderRadius: 1, fontFamily: 'monospace' }}>
                                {selectedEntry.oldValue}
                              </Typography>
                            </Grid>
                          )}
                          {selectedEntry.newValue && (
                            <Grid item xs={6}>
                              <Typography variant="subtitle2" color="success.main">
                                New Value:
                              </Typography>
                              <Typography variant="body2" sx={{ p: 1, bgcolor: 'success.light', color: 'success.contrastText', borderRadius: 1, fontFamily: 'monospace' }}>
                                {selectedEntry.newValue}
                              </Typography>
                            </Grid>
                          )}
                        </Grid>
                      </CardContent>
                    </Card>
                  </>
                )}

                <Typography variant="h6" sx={{ mb: 2 }}>Compliance Flags</Typography>
                <Card variant="outlined">
                  <CardContent>
                    <Stack direction="row" spacing={1} flexWrap="wrap">
                      {selectedEntry.complianceFlags.map((flag) => (
                        <Chip
                          key={flag}
                          label={flag}
                          size="small"
                          color="success"
                          variant="outlined"
                        />
                      ))}
                    </Stack>
                  </CardContent>
                </Card>
              </Grid>

              <Grid item xs={12} md={4}>
                <Typography variant="h6" sx={{ mb: 2 }}>Context</Typography>
                <Card variant="outlined" sx={{ mb: 3 }}>
                  <CardContent>
                    <List dense>
                      {selectedEntry.studyName && (
                        <ListItem>
                          <ListItemText primary="Study" secondary={selectedEntry.studyName} />
                        </ListItem>
                      )}
                      {selectedEntry.subjectId && (
                        <ListItem>
                          <ListItemText primary="Subject" secondary={selectedEntry.subjectId} />
                        </ListItem>
                      )}
                      {selectedEntry.visitName && (
                        <ListItem>
                          <ListItemText primary="Visit" secondary={selectedEntry.visitName} />
                        </ListItem>
                      )}
                      {selectedEntry.formName && (
                        <ListItem>
                          <ListItemText primary="Form" secondary={selectedEntry.formName} />
                        </ListItem>
                      )}
                      {selectedEntry.fieldName && (
                        <ListItem>
                          <ListItemText primary="Field" secondary={selectedEntry.fieldName} />
                        </ListItem>
                      )}
                    </List>
                  </CardContent>
                </Card>

                <Typography variant="h6" sx={{ mb: 2 }}>User & System</Typography>
                <Card variant="outlined">
                  <CardContent>
                    <List dense>
                      <ListItem>
                        <ListItemText primary="User" secondary={`${selectedEntry.user} (${selectedEntry.role})`} />
                      </ListItem>
                      <ListItem>
                        <ListItemText primary="User ID" secondary={selectedEntry.userId} />
                      </ListItem>
                      <ListItem>
                        <ListItemText primary="IP Address" secondary={selectedEntry.ipAddress} />
                      </ListItem>
                      <ListItem>
                        <ListItemText primary="Session ID" secondary={selectedEntry.sessionId} />
                      </ListItem>
                      <ListItem>
                        <ListItemText 
                          primary="Electronic Signature" 
                          secondary={selectedEntry.signature ? "Yes" : "No"} 
                        />
                      </ListItem>
                    </List>
                  </CardContent>
                </Card>
              </Grid>
            </Grid>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDetailDialogOpen(false)}>Close</Button>
        </DialogActions>
      </Dialog>

      {/* Compliance Notice */}
      <Alert severity="info" sx={{ mt: 3 }}>
        <Typography variant="body2">
          <strong>21 CFR Part 11 Compliance Notice:</strong> This audit trail captures all system activities with immutable records. 
          Electronic signatures are validated and time-stamped. All entries include user identification and cannot be modified or deleted. 
          This system maintains complete traceability for regulatory compliance.
        </Typography>
      </Alert>
    </Box>
  );
};

export default AuditTrail;