import React, { useState, useEffect } from 'react';
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
  Button,
  Chip,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Tooltip,
  Card,
  CardContent,
  Grid,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Stack,
  Alert,
  LinearProgress,
  Avatar,
  List,
  ListItem,
  ListItemText,
  ListItemIcon,
  ListItemButton,
  Divider,
  Badge,
  InputAdornment,
  Stepper,
  Step,
  StepLabel,
  StepContent,
  Collapse,
  ToggleButton,
  ToggleButtonGroup,
} from '@mui/material';
import {
  Add,
  Edit,
  Visibility,
  Person,
  PersonAdd,
  CalendarToday,
  Assignment,
  CheckCircle,
  Warning,
  Schedule,
  Lock,
  LockOpen,
  Timeline as TimelineIcon,
  EventNote,
  Group,
  Assessment,
  Description,
  LocalHospital,
  Science,
  TrendingUp,
  Search,
  FilterList,
  Save,
  Send,
  Print,
  Download,
  ExpandMore,
  ExpandLess,
  AccessTime,
  EventAvailable,
  EventBusy,
  PlayCircleOutline,
  CheckCircleOutline,
  RadioButtonUnchecked,
  Cancel,
  Info,
  VerifiedUser,
  AssignmentTurnedIn,
} from '@mui/icons-material';
import { useNavigate, useParams } from 'react-router-dom';

interface Subject {
  id: string;
  subjectId: string;
  studyId: string;
  studyName: string;
  initials: string;
  screeningDate: string;
  enrollmentDate?: string;
  status: 'SCREENED' | 'ENROLLED' | 'COMPLETED' | 'WITHDRAWN' | 'DISCONTINUED';
  lastVisitDate: string;
  nextVisitDate?: string;
  siteId: string;
  siteName: string;
  age: number;
  gender: 'M' | 'F' | 'Other';
  visitProgress: number;
  completedVisits: number;
  totalVisits: number;
  openQueries: number;
  protocolDeviations: number;
  adverseEvents: number;
  withdrawalReason?: string;
  withdrawalDate?: string;
}

interface Visit {
  id: string;
  visitName: string;
  visitType: 'SCREENING' | 'BASELINE' | 'FOLLOW_UP' | 'CLOSE_OUT' | 'UNSCHEDULED';
  visitDate?: string;
  scheduledDate: string;
  windowStart: string;
  windowEnd: string;
  status: 'NOT_STARTED' | 'IN_PROGRESS' | 'COMPLETED' | 'LOCKED' | 'MISSED';
  forms: VisitForm[];
  completedForms: number;
  totalForms: number;
}

interface VisitForm {
  id: string;
  formName: string;
  formVersion: string;
  status: 'NOT_STARTED' | 'DRAFT' | 'COMPLETED' | 'LOCKED' | 'QUERY';
  lastUpdated?: string;
  updatedBy?: string;
  queries: number;
}

const SubjectList: React.FC = () => {
  const [subjects, setSubjects] = useState<Subject[]>([]);
  const [selectedSubject, setSelectedSubject] = useState<Subject | null>(null);
  const [addDialogOpen, setAddDialogOpen] = useState(false);
  const [detailDialogOpen, setDetailDialogOpen] = useState(false);
  const [viewMode, setViewMode] = useState<'list' | 'timeline'>('list');
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [statusFilter, setStatusFilter] = useState('all');
  const [selectedVisit, setSelectedVisit] = useState<Visit | null>(null);
  const [expandedVisit, setExpandedVisit] = useState<string | null>(null);
  const navigate = useNavigate();
  const { studyId } = useParams();

  // New subject form state
  const [newSubject, setNewSubject] = useState({
    initials: '',
    screeningDate: new Date().toISOString().split('T')[0],
    siteId: '',
    age: '',
    gender: '',
  });

  // Mock data
  useEffect(() => {
    const mockSubjects: Subject[] = [
      {
        id: '1',
        subjectId: 'SUBJ-001',
        studyId: 'CARDIO-2024-001',
        studyName: 'Cardiovascular Disease Prevention Trial',
        initials: 'JD',
        screeningDate: '2024-02-01',
        enrollmentDate: '2024-02-05',
        status: 'ENROLLED',
        lastVisitDate: '2024-02-20',
        nextVisitDate: '2024-03-05',
        siteId: 'SITE-01',
        siteName: 'City Medical Center',
        age: 45,
        gender: 'M',
        visitProgress: 60,
        completedVisits: 3,
        totalVisits: 5,
        openQueries: 2,
        protocolDeviations: 0,
        adverseEvents: 1,
      },
      {
        id: '2',
        subjectId: 'SUBJ-002',
        studyId: 'CARDIO-2024-001',
        studyName: 'Cardiovascular Disease Prevention Trial',
        initials: 'SM',
        screeningDate: '2024-02-03',
        enrollmentDate: '2024-02-08',
        status: 'ENROLLED',
        lastVisitDate: '2024-02-22',
        nextVisitDate: '2024-03-08',
        siteId: 'SITE-01',
        siteName: 'City Medical Center',
        age: 52,
        gender: 'F',
        visitProgress: 40,
        completedVisits: 2,
        totalVisits: 5,
        openQueries: 0,
        protocolDeviations: 1,
        adverseEvents: 0,
      },
      {
        id: '3',
        subjectId: 'SUBJ-003',
        studyId: 'CARDIO-2024-001',
        studyName: 'Cardiovascular Disease Prevention Trial',
        initials: 'RK',
        screeningDate: '2024-02-05',
        status: 'SCREENED',
        lastVisitDate: '2024-02-05',
        siteId: 'SITE-02',
        siteName: 'General Hospital',
        age: 38,
        gender: 'M',
        visitProgress: 20,
        completedVisits: 1,
        totalVisits: 5,
        openQueries: 1,
        protocolDeviations: 0,
        adverseEvents: 0,
      },
      {
        id: '4',
        subjectId: 'SUBJ-004',
        studyId: 'CARDIO-2024-001',
        studyName: 'Cardiovascular Disease Prevention Trial',
        initials: 'LP',
        screeningDate: '2024-01-15',
        enrollmentDate: '2024-01-20',
        status: 'COMPLETED',
        lastVisitDate: '2024-02-15',
        siteId: 'SITE-01',
        siteName: 'City Medical Center',
        age: 61,
        gender: 'F',
        visitProgress: 100,
        completedVisits: 5,
        totalVisits: 5,
        openQueries: 0,
        protocolDeviations: 0,
        adverseEvents: 2,
      },
      {
        id: '5',
        subjectId: 'SUBJ-005',
        studyId: 'CARDIO-2024-001',
        studyName: 'Cardiovascular Disease Prevention Trial',
        initials: 'TW',
        screeningDate: '2024-01-25',
        enrollmentDate: '2024-01-30',
        status: 'WITHDRAWN',
        lastVisitDate: '2024-02-10',
        withdrawalDate: '2024-02-10',
        withdrawalReason: 'Adverse Event',
        siteId: 'SITE-02',
        siteName: 'General Hospital',
        age: 55,
        gender: 'M',
        visitProgress: 40,
        completedVisits: 2,
        totalVisits: 5,
        openQueries: 3,
        protocolDeviations: 1,
        adverseEvents: 3,
      },
    ];

    const fetchSubjects = async () => {
      try {
        setLoading(true);
        const url = studyId ? `/api/subjects?study_id=${studyId}` : '/api/subjects';
        const response = await fetch(url);
        
        if (!response.ok) {
          throw new Error('Failed to fetch subjects');
        }
        
        const data = await response.json();
        
        // Transform API data to match component interface
        const transformedSubjects: Subject[] = data.data?.map((subject: any) => {
          // Calculate age from date of birth
          let age = 0;
          if (subject.date_of_birth) {
            const birthDate = new Date(subject.date_of_birth);
            const today = new Date();
            if (!isNaN(birthDate.getTime())) {
              age = today.getFullYear() - birthDate.getFullYear();
              const monthDiff = today.getMonth() - birthDate.getMonth();
              if (monthDiff < 0 || (monthDiff === 0 && today.getDate() < birthDate.getDate())) {
                age--;
              }
            }
          }

          return {
            id: subject.id,
            subjectId: subject.subject_number || subject.subject_id,
            studyId: subject.study_id,
            studyName: subject.edc_studies?.title || 'Unknown Study',
            initials: subject.initials || 'N/A',
            screeningDate: subject.enrollment_date || subject.created_at,
            enrollmentDate: subject.enrollment_date,
            status: subject.status as any,
            lastVisitDate: subject.last_visit_date || subject.enrollment_date,
            nextVisitDate: subject.next_visit_date,
            siteId: subject.site_id,
            siteName: subject.edc_sites?.name || 'Unknown Site',
            age: age,
            gender: subject.gender as any,
            visitProgress: 0, // Would be calculated from actual visit data
            completedVisits: subject.visits_completed || 0,
            totalVisits: 5, // Would come from study design
            openQueries: 0, // Would be calculated from actual query data
            protocolDeviations: 0, // Would be calculated from actual PD data
            adverseEvents: 0, // Would be calculated from actual AE data
          };
        }) || [];
        
        setSubjects(transformedSubjects);
      } catch (error) {
        console.error('Error fetching subjects:', error);
        setSubjects([]);
      } finally {
        setLoading(false);
      }
    };

    fetchSubjects();
  }, [studyId]);

  // Mock visits data
  const getMockVisits = (subjectId: string): Visit[] => {
    return [
      {
        id: 'V1',
        visitName: 'Screening',
        visitType: 'SCREENING',
        scheduledDate: '2024-02-01',
        visitDate: '2024-02-01',
        windowStart: '2024-01-29',
        windowEnd: '2024-02-04',
        status: 'COMPLETED',
        forms: [
          { id: 'F1', formName: 'Demographics', formVersion: '1.2', status: 'COMPLETED', queries: 0 },
          { id: 'F2', formName: 'Medical History', formVersion: '1.0', status: 'COMPLETED', queries: 1 },
          { id: 'F3', formName: 'Eligibility Criteria', formVersion: '2.1', status: 'COMPLETED', queries: 0 },
        ],
        completedForms: 3,
        totalForms: 3,
      },
      {
        id: 'V2',
        visitName: 'Baseline',
        visitType: 'BASELINE',
        scheduledDate: '2024-02-05',
        visitDate: '2024-02-05',
        windowStart: '2024-02-03',
        windowEnd: '2024-02-07',
        status: 'COMPLETED',
        forms: [
          { id: 'F4', formName: 'Vital Signs', formVersion: '1.3', status: 'COMPLETED', queries: 0 },
          { id: 'F5', formName: 'Laboratory Tests', formVersion: '2.0', status: 'COMPLETED', queries: 2 },
          { id: 'F6', formName: 'Concomitant Medications', formVersion: '1.1', status: 'QUERY', queries: 1 },
        ],
        completedForms: 2,
        totalForms: 3,
      },
      {
        id: 'V3',
        visitName: 'Week 2 Follow-up',
        visitType: 'FOLLOW_UP',
        scheduledDate: '2024-02-19',
        visitDate: '2024-02-20',
        windowStart: '2024-02-17',
        windowEnd: '2024-02-21',
        status: 'IN_PROGRESS',
        forms: [
          { id: 'F7', formName: 'Vital Signs', formVersion: '1.3', status: 'COMPLETED', queries: 0 },
          { id: 'F8', formName: 'Adverse Events', formVersion: '2.2', status: 'DRAFT', queries: 0 },
          { id: 'F9', formName: 'Study Drug Compliance', formVersion: '1.0', status: 'NOT_STARTED', queries: 0 },
        ],
        completedForms: 1,
        totalForms: 3,
      },
      {
        id: 'V4',
        visitName: 'Week 4 Follow-up',
        visitType: 'FOLLOW_UP',
        scheduledDate: '2024-03-05',
        windowStart: '2024-03-03',
        windowEnd: '2024-03-07',
        status: 'NOT_STARTED',
        forms: [
          { id: 'F10', formName: 'Vital Signs', formVersion: '1.3', status: 'NOT_STARTED', queries: 0 },
          { id: 'F11', formName: 'Laboratory Tests', formVersion: '2.0', status: 'NOT_STARTED', queries: 0 },
          { id: 'F12', formName: 'Quality of Life', formVersion: '1.2', status: 'NOT_STARTED', queries: 0 },
        ],
        completedForms: 0,
        totalForms: 3,
      },
      {
        id: 'V5',
        visitName: 'Study Completion',
        visitType: 'CLOSE_OUT',
        scheduledDate: '2024-03-19',
        windowStart: '2024-03-17',
        windowEnd: '2024-03-21',
        status: 'NOT_STARTED',
        forms: [
          { id: 'F13', formName: 'End of Study', formVersion: '1.0', status: 'NOT_STARTED', queries: 0 },
          { id: 'F14', formName: 'Study Drug Return', formVersion: '1.1', status: 'NOT_STARTED', queries: 0 },
        ],
        completedForms: 0,
        totalForms: 2,
      },
    ];
  };

  // Filter subjects
  const filteredSubjects = subjects.filter(subject => {
    const matchesSearch = 
      (subject.subjectId || '').toLowerCase().includes(searchTerm.toLowerCase()) ||
      (subject.initials || '').toLowerCase().includes(searchTerm.toLowerCase()) ||
      (subject.siteName || '').toLowerCase().includes(searchTerm.toLowerCase());
    const matchesStatus = statusFilter === 'all' || subject.status === statusFilter;
    return matchesSearch && matchesStatus;
  });

  // Generate next subject ID
  const generateSubjectId = () => {
    const maxId = subjects.reduce((max, subject) => {
      if (subject.subjectId && typeof subject.subjectId === 'string' && subject.subjectId.includes('-')) {
        const idNum = parseInt(subject.subjectId.split('-')[1]) || 0;
        return idNum > max ? idNum : max;
      }
      return max;
    }, 0);
    return `SUBJ-${String(maxId + 1).padStart(3, '0')}`;
  };

  // Handle add subject
  const handleAddSubject = () => {
    const newSubjectData: Subject = {
      id: String(subjects.length + 1),
      subjectId: generateSubjectId(),
      studyId: studyId || 'CARDIO-2024-001',
      studyName: 'Cardiovascular Disease Prevention Trial',
      initials: newSubject.initials,
      screeningDate: newSubject.screeningDate,
      status: 'SCREENED',
      lastVisitDate: newSubject.screeningDate,
      siteId: newSubject.siteId,
      siteName: newSubject.siteId === 'SITE-01' ? 'City Medical Center' : 'General Hospital',
      age: parseInt(newSubject.age),
      gender: newSubject.gender as 'M' | 'F' | 'Other',
      visitProgress: 0,
      completedVisits: 0,
      totalVisits: 5,
      openQueries: 0,
      protocolDeviations: 0,
      adverseEvents: 0,
    };

    setSubjects([...subjects, newSubjectData]);
    setAddDialogOpen(false);
    setNewSubject({
      initials: '',
      screeningDate: new Date().toISOString().split('T')[0],
      siteId: '',
      age: '',
      gender: '',
    });
  };

  // Handle view subject timeline
  const handleViewTimeline = (subject: Subject) => {
    setSelectedSubject(subject);
    setDetailDialogOpen(true);
  };

  // Handle form data entry
  const handleFormEntry = (subjectId: string, visitId: string, formId: string) => {
    // Navigate to data entry page with subject, visit, and form context
    navigate(`/data-entry?subject=${subjectId}&visit=${visitId}&form=${formId}`);
  };

  // Get status color
  const getStatusColor = (status: string) => {
    switch (status) {
      case 'ENROLLED': return 'success';
      case 'SCREENED': return 'info';
      case 'COMPLETED': return 'primary';
      case 'WITHDRAWN': return 'error';
      case 'DISCONTINUED': return 'warning';
      default: return 'default';
    }
  };

  // Get visit status color
  const getVisitStatusColor = (status: string) => {
    switch (status) {
      case 'COMPLETED': return 'success';
      case 'IN_PROGRESS': return 'warning';
      case 'NOT_STARTED': return 'default';
      case 'LOCKED': return 'primary';
      case 'MISSED': return 'error';
      default: return 'default';
    }
  };

  // Get form status icon
  const getFormStatusIcon = (status: string) => {
    switch (status) {
      case 'COMPLETED': return <CheckCircleOutline color="success" />;
      case 'DRAFT': return <Edit color="warning" />;
      case 'NOT_STARTED': return <RadioButtonUnchecked />;
      case 'LOCKED': return <Lock color="primary" />;
      case 'QUERY': return <Warning color="error" />;
      default: return <RadioButtonUnchecked />;
    }
  };

  if (loading) {
    return (
      <Box sx={{ p: 3 }}>
        <LinearProgress />
        <Typography sx={{ mt: 2 }}>Loading subjects...</Typography>
      </Box>
    );
  }

  return (
    <Box>
      {/* Header */}
      <Box sx={{ mb: 3, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Box>
          <Typography variant="h4" gutterBottom>
            Subject Management
          </Typography>
          <Typography variant="subtitle1" color="text.secondary">
            Manage study participants and track their progress
          </Typography>
        </Box>
        <Stack direction="row" spacing={2}>
          <ToggleButtonGroup
            value={viewMode}
            exclusive
            onChange={(e, newMode) => newMode && setViewMode(newMode)}
            size="small"
          >
            <ToggleButton value="list">
              <Assessment sx={{ mr: 1 }} />
              List View
            </ToggleButton>
            <ToggleButton value="timeline">
              <TimelineIcon sx={{ mr: 1 }} />
              Timeline View
            </ToggleButton>
          </ToggleButtonGroup>
          <Button
            variant="contained"
            startIcon={<PersonAdd />}
            onClick={() => setAddDialogOpen(true)}
            sx={{
              background: 'linear-gradient(45deg, #FE6B8B 30%, #FF8E53 90%)',
              boxShadow: '0 3px 5px 2px rgba(255, 105, 135, .3)',
            }}
          >
            Add New Subject
          </Button>
        </Stack>
      </Box>

      {/* Statistics Cards */}
      <Grid container spacing={3} sx={{ mb: 3 }}>
        <Grid item xs={12} md={3}>
          <Card sx={{ background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)', color: 'white' }}>
            <CardContent>
              <Stack direction="row" justifyContent="space-between" alignItems="center">
                <Box>
                  <Typography color="inherit" gutterBottom sx={{ opacity: 0.8 }}>
                    Total Subjects
                  </Typography>
                  <Typography variant="h4">
                    {subjects.length}
                  </Typography>
                </Box>
                <Avatar sx={{ bgcolor: 'rgba(255,255,255,0.2)' }}>
                  <Group />
                </Avatar>
              </Stack>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card sx={{ background: 'linear-gradient(135deg, #f093fb 0%, #f5576c 100%)', color: 'white' }}>
            <CardContent>
              <Stack direction="row" justifyContent="space-between" alignItems="center">
                <Box>
                  <Typography color="inherit" gutterBottom sx={{ opacity: 0.8 }}>
                    Enrolled
                  </Typography>
                  <Typography variant="h4">
                    {subjects.filter(s => s.status === 'ENROLLED').length}
                  </Typography>
                </Box>
                <Avatar sx={{ bgcolor: 'rgba(255,255,255,0.2)' }}>
                  <CheckCircle />
                </Avatar>
              </Stack>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card sx={{ background: 'linear-gradient(135deg, #4facfe 0%, #00f2fe 100%)', color: 'white' }}>
            <CardContent>
              <Stack direction="row" justifyContent="space-between" alignItems="center">
                <Box>
                  <Typography color="inherit" gutterBottom sx={{ opacity: 0.8 }}>
                    Open Queries
                  </Typography>
                  <Typography variant="h4">
                    {subjects.reduce((sum, s) => sum + s.openQueries, 0)}
                  </Typography>
                </Box>
                <Avatar sx={{ bgcolor: 'rgba(255,255,255,0.2)' }}>
                  <Warning />
                </Avatar>
              </Stack>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card sx={{ background: 'linear-gradient(135deg, #fa709a 0%, #fee140 100%)', color: 'white' }}>
            <CardContent>
              <Stack direction="row" justifyContent="space-between" alignItems="center">
                <Box>
                  <Typography color="inherit" gutterBottom sx={{ opacity: 0.8 }}>
                    Completion Rate
                  </Typography>
                  <Typography variant="h4">
                    {Math.round(subjects.reduce((sum, s) => sum + s.visitProgress, 0) / subjects.length)}%
                  </Typography>
                </Box>
                <Avatar sx={{ bgcolor: 'rgba(255,255,255,0.2)' }}>
                  <TrendingUp />
                </Avatar>
              </Stack>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Search and Filters */}
      <Paper sx={{ p: 2, mb: 3 }}>
        <Grid container spacing={2} alignItems="center">
          <Grid item xs={12} md={6}>
            <TextField
              fullWidth
              size="small"
              placeholder="Search by Subject ID, Initials, or Site..."
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
              <InputLabel>Status Filter</InputLabel>
              <Select
                value={statusFilter}
                label="Status Filter"
                onChange={(e) => setStatusFilter(e.target.value)}
              >
                <MenuItem value="all">All Status</MenuItem>
                <MenuItem value="SCREENED">Screened</MenuItem>
                <MenuItem value="ENROLLED">Enrolled</MenuItem>
                <MenuItem value="COMPLETED">Completed</MenuItem>
                <MenuItem value="WITHDRAWN">Withdrawn</MenuItem>
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={12} md={2}>
            <Typography variant="body2" color="text.secondary" textAlign="center">
              {filteredSubjects.length} subjects
            </Typography>
          </Grid>
        </Grid>
      </Paper>

      {/* Subjects Table */}
      {viewMode === 'list' ? (
        <Paper elevation={2}>
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Subject Details</TableCell>
                  <TableCell>Site</TableCell>
                  <TableCell>Status</TableCell>
                  <TableCell>Visit Progress</TableCell>
                  <TableCell>Data Quality</TableCell>
                  <TableCell>Last/Next Visit</TableCell>
                  <TableCell align="center">Actions</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {filteredSubjects.map((subject) => (
                  <TableRow key={subject.id} hover>
                    <TableCell>
                      <Box>
                        <Typography variant="subtitle2" fontWeight={600}>
                          {subject.subjectId}
                        </Typography>
                        <Typography variant="body2">
                          Initials: {subject.initials} | Age: {subject.age} | Gender: {subject.gender}
                        </Typography>
                        <Typography variant="caption" color="text.secondary">
                          Screening: {new Date(subject.screeningDate).toLocaleDateString()}
                        </Typography>
                      </Box>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2">{subject.siteName}</Typography>
                    </TableCell>
                    <TableCell>
                      <Chip
                        label={subject.status}
                        color={getStatusColor(subject.status) as any}
                        size="small"
                      />
                      {subject.withdrawalReason && (
                        <Typography variant="caption" display="block" color="error">
                          {subject.withdrawalReason}
                        </Typography>
                      )}
                    </TableCell>
                    <TableCell>
                      <Box>
                        <Typography variant="body2">
                          {subject.completedVisits}/{subject.totalVisits} visits
                        </Typography>
                        <LinearProgress
                          variant="determinate"
                          value={subject.visitProgress}
                          sx={{ mt: 0.5, height: 6 }}
                        />
                        <Typography variant="caption" color="text.secondary">
                          {subject.visitProgress}% complete
                        </Typography>
                      </Box>
                    </TableCell>
                    <TableCell>
                      <Stack spacing={0.5}>
                        {subject.openQueries > 0 && (
                          <Chip
                            icon={<Warning />}
                            label={`${subject.openQueries} queries`}
                            size="small"
                            color="warning"
                          />
                        )}
                        {subject.protocolDeviations > 0 && (
                          <Chip
                            label={`${subject.protocolDeviations} PD`}
                            size="small"
                            color="error"
                            variant="outlined"
                          />
                        )}
                        {subject.adverseEvents > 0 && (
                          <Chip
                            label={`${subject.adverseEvents} AE`}
                            size="small"
                            color="error"
                          />
                        )}
                      </Stack>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2">
                        Last: {new Date(subject.lastVisitDate).toLocaleDateString()}
                      </Typography>
                      {subject.nextVisitDate && (
                        <Typography variant="caption" color="primary">
                          Next: {new Date(subject.nextVisitDate).toLocaleDateString()}
                        </Typography>
                      )}
                    </TableCell>
                    <TableCell>
                      <Stack direction="row" spacing={1} justifyContent="center">
                        <Tooltip title="View Timeline">
                          <IconButton
                            size="small"
                            color="primary"
                            onClick={() => handleViewTimeline(subject)}
                          >
                            <TimelineIcon />
                          </IconButton>
                        </Tooltip>
                        <Tooltip title="Edit Subject">
                          <IconButton size="small" color="secondary">
                            <Edit />
                          </IconButton>
                        </Tooltip>
                        <Tooltip title="View Details">
                          <IconButton size="small">
                            <Visibility />
                          </IconButton>
                        </Tooltip>
                      </Stack>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </Paper>
      ) : (
        // Timeline View
        <Grid container spacing={3}>
          {filteredSubjects.map((subject) => (
            <Grid item xs={12} md={6} key={subject.id}>
              <Paper sx={{ p: 2 }}>
                <Box sx={{ mb: 2, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <Box>
                    <Typography variant="h6">{subject.subjectId}</Typography>
                    <Typography variant="body2" color="text.secondary">
                      {subject.initials} | {subject.siteName}
                    </Typography>
                  </Box>
                  <Chip
                    label={subject.status}
                    color={getStatusColor(subject.status) as any}
                    size="small"
                  />
                </Box>
                <List dense>
                  {getMockVisits(subject.subjectId).slice(0, 3).map((visit) => (
                    <ListItem key={visit.id} divider>
                      <ListItemIcon>
                        {visit.status === 'COMPLETED' ? (
                          <CheckCircle color="success" />
                        ) : visit.status === 'IN_PROGRESS' ? (
                          <Schedule color="warning" />
                        ) : (
                          <RadioButtonUnchecked />
                        )}
                      </ListItemIcon>
                      <ListItemText
                        primary={visit.visitName}
                        secondary={
                          <Box>
                            <Typography variant="caption" color="text.secondary">
                              {visit.visitDate || visit.scheduledDate}
                            </Typography>
                            <Typography variant="caption" display="block">
                              {visit.completedForms}/{visit.totalForms} forms
                            </Typography>
                          </Box>
                        }
                      />
                      <Chip
                        label={visit.status.replace('_', ' ')}
                        size="small"
                        color={getVisitStatusColor(visit.status) as any}
                      />
                    </ListItem>
                  ))}
                </List>
                <Button
                  size="small"
                  startIcon={<Visibility />}
                  onClick={() => handleViewTimeline(subject)}
                  fullWidth
                >
                  View Full Timeline
                </Button>
              </Paper>
            </Grid>
          ))}
        </Grid>
      )}

      {/* Add Subject Dialog */}
      <Dialog open={addDialogOpen} onClose={() => setAddDialogOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Add New Subject</DialogTitle>
        <DialogContent>
          <Alert severity="info" sx={{ mb: 2 }}>
            Subject ID will be auto-generated: <strong>{generateSubjectId()}</strong>
          </Alert>
          <Grid container spacing={2} sx={{ mt: 1 }}>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Subject Initials"
                value={newSubject.initials}
                onChange={(e) => setNewSubject({ ...newSubject, initials: e.target.value.toUpperCase() })}
                inputProps={{ maxLength: 3 }}
                helperText="2-3 letters"
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Screening Date"
                type="date"
                value={newSubject.screeningDate}
                onChange={(e) => setNewSubject({ ...newSubject, screeningDate: e.target.value })}
                InputLabelProps={{ shrink: true }}
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth>
                <InputLabel>Site</InputLabel>
                <Select
                  value={newSubject.siteId}
                  label="Site"
                  onChange={(e) => setNewSubject({ ...newSubject, siteId: e.target.value })}
                >
                  <MenuItem value="SITE-01">City Medical Center</MenuItem>
                  <MenuItem value="SITE-02">General Hospital</MenuItem>
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Age"
                type="number"
                value={newSubject.age}
                onChange={(e) => setNewSubject({ ...newSubject, age: e.target.value })}
                inputProps={{ min: 18, max: 120 }}
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth>
                <InputLabel>Gender</InputLabel>
                <Select
                  value={newSubject.gender}
                  label="Gender"
                  onChange={(e) => setNewSubject({ ...newSubject, gender: e.target.value })}
                >
                  <MenuItem value="M">Male</MenuItem>
                  <MenuItem value="F">Female</MenuItem>
                  <MenuItem value="Other">Other</MenuItem>
                </Select>
              </FormControl>
            </Grid>
          </Grid>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setAddDialogOpen(false)}>Cancel</Button>
          <Button
            variant="contained"
            onClick={handleAddSubject}
            disabled={!newSubject.initials || !newSubject.siteId || !newSubject.age || !newSubject.gender}
          >
            Add Subject
          </Button>
        </DialogActions>
      </Dialog>

      {/* Subject Timeline Dialog */}
      <Dialog 
        open={detailDialogOpen} 
        onClose={() => setDetailDialogOpen(false)} 
        maxWidth="lg" 
        fullWidth
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Box>
              <Typography variant="h6">
                Subject Timeline: {selectedSubject?.subjectId}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                {selectedSubject?.initials} | {selectedSubject?.siteName} | Status: {selectedSubject?.status}
              </Typography>
            </Box>
            <Stack direction="row" spacing={1}>
              <Button startIcon={<Print />} size="small">Print</Button>
              <Button startIcon={<Download />} size="small">Export</Button>
            </Stack>
          </Box>
        </DialogTitle>
        <DialogContent dividers>
          {selectedSubject && (
            <Box>
              {/* Subject Info Cards */}
              <Grid container spacing={2} sx={{ mb: 3 }}>
                <Grid item xs={12} md={3}>
                  <Card variant="outlined">
                    <CardContent>
                      <Typography variant="caption" color="text.secondary">Demographics</Typography>
                      <Typography variant="body1">
                        Age: {selectedSubject.age} | Gender: {selectedSubject.gender}
                      </Typography>
                    </CardContent>
                  </Card>
                </Grid>
                <Grid item xs={12} md={3}>
                  <Card variant="outlined">
                    <CardContent>
                      <Typography variant="caption" color="text.secondary">Progress</Typography>
                      <Typography variant="body1">
                        {selectedSubject.completedVisits}/{selectedSubject.totalVisits} visits ({selectedSubject.visitProgress}%)
                      </Typography>
                    </CardContent>
                  </Card>
                </Grid>
                <Grid item xs={12} md={3}>
                  <Card variant="outlined">
                    <CardContent>
                      <Typography variant="caption" color="text.secondary">Data Quality</Typography>
                      <Typography variant="body1">
                        {selectedSubject.openQueries} queries, {selectedSubject.protocolDeviations} PDs
                      </Typography>
                    </CardContent>
                  </Card>
                </Grid>
                <Grid item xs={12} md={3}>
                  <Card variant="outlined">
                    <CardContent>
                      <Typography variant="caption" color="text.secondary">Safety</Typography>
                      <Typography variant="body1">
                        {selectedSubject.adverseEvents} Adverse Events
                      </Typography>
                    </CardContent>
                  </Card>
                </Grid>
              </Grid>

              {/* Visit Timeline */}
              <Typography variant="h6" sx={{ mb: 2 }}>Visit Schedule</Typography>
              <List>
                {getMockVisits(selectedSubject.subjectId).map((visit) => (
                  <Box key={visit.id}>
                    <ListItem
                      sx={{
                        bgcolor: expandedVisit === visit.id ? 'action.selected' : 'transparent',
                        borderRadius: 1,
                        mb: 1,
                      }}
                    >
                      <ListItemIcon>
                        {visit.status === 'COMPLETED' ? (
                          <CheckCircleOutline color="success" />
                        ) : visit.status === 'IN_PROGRESS' ? (
                          <PlayCircleOutline color="warning" />
                        ) : visit.status === 'NOT_STARTED' ? (
                          <RadioButtonUnchecked />
                        ) : visit.status === 'LOCKED' ? (
                          <Lock color="primary" />
                        ) : (
                          <Cancel color="error" />
                        )}
                      </ListItemIcon>
                      <ListItemText
                        primary={
                          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                            <Typography variant="subtitle1">{visit.visitName}</Typography>
                            <Chip
                              label={visit.status.replace('_', ' ')}
                              size="small"
                              color={getVisitStatusColor(visit.status) as any}
                            />
                            <Chip
                              label={visit.visitType}
                              size="small"
                              variant="outlined"
                            />
                          </Box>
                        }
                        secondary={
                          <Box>
                            <Typography variant="body2">
                              Scheduled: {new Date(visit.scheduledDate).toLocaleDateString()}
                              {visit.visitDate && ` | Actual: ${new Date(visit.visitDate).toLocaleDateString()}`}
                            </Typography>
                            <Typography variant="caption" color="text.secondary">
                              Window: {new Date(visit.windowStart).toLocaleDateString()} - {new Date(visit.windowEnd).toLocaleDateString()}
                            </Typography>
                          </Box>
                        }
                      />
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Typography variant="body2">
                          {visit.completedForms}/{visit.totalForms} forms
                        </Typography>
                        <IconButton
                          size="small"
                          onClick={() => setExpandedVisit(expandedVisit === visit.id ? null : visit.id)}
                        >
                          {expandedVisit === visit.id ? <ExpandLess /> : <ExpandMore />}
                        </IconButton>
                      </Box>
                    </ListItem>
                    
                    <Collapse in={expandedVisit === visit.id}>
                      <Box sx={{ pl: 7, pr: 2, pb: 2 }}>
                        <Typography variant="subtitle2" sx={{ mb: 1 }}>Forms</Typography>
                        <Grid container spacing={1}>
                          {visit.forms.map((form) => (
                            <Grid item xs={12} md={4} key={form.id}>
                              <Card variant="outlined">
                                <CardContent sx={{ p: 1.5 }}>
                                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                                    {getFormStatusIcon(form.status)}
                                    <Typography variant="body2" fontWeight={500}>
                                      {form.formName}
                                    </Typography>
                                  </Box>
                                  <Typography variant="caption" color="text.secondary">
                                    Version: {form.formVersion}
                                  </Typography>
                                  {form.queries > 0 && (
                                    <Chip
                                      label={`${form.queries} queries`}
                                      size="small"
                                      color="warning"
                                      sx={{ ml: 1 }}
                                    />
                                  )}
                                  <Box sx={{ mt: 1 }}>
                                    <Button
                                      size="small"
                                      variant={form.status === 'NOT_STARTED' ? 'contained' : 'outlined'}
                                      startIcon={form.status === 'NOT_STARTED' ? <Add /> : <Edit />}
                                      onClick={() => handleFormEntry(selectedSubject.subjectId, visit.id, form.id)}
                                      disabled={visit.status === 'LOCKED' || form.status === 'LOCKED'}
                                      fullWidth
                                    >
                                      {form.status === 'NOT_STARTED' ? 'Start' : 
                                       form.status === 'DRAFT' ? 'Continue' : 
                                       form.status === 'COMPLETED' ? 'Review' : 'View'}
                                    </Button>
                                  </Box>
                                </CardContent>
                              </Card>
                            </Grid>
                          ))}
                        </Grid>
                      </Box>
                    </Collapse>
                  </Box>
                ))}
              </List>
            </Box>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDetailDialogOpen(false)}>Close</Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default SubjectList;