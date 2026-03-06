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
  Tabs,
  Tab,
  Badge,
  LinearProgress,
  Alert,
  Divider,
  Avatar,
  List,
  ListItem,
  ListItemText,
  ListItemIcon,
  Fab,
  CircularProgress,
  Collapse,
  Stepper,
  Step,
  StepLabel,
} from '@mui/material';
import {
  Add,
  Edit,
  Visibility,
  Science,
  Group,
  Assignment,
  HelpOutline,
  Timeline,
  CheckCircle,
  Warning,
  Person,
  CalendarToday,
  Description,
  TrendingUp,
  Dashboard as DashboardIcon,
  Build,
  LocalHospital,
  Assessment,
  Security,
  ExpandMore,
  ExpandLess,
  CheckCircleOutline,
  PlayCircleOutline,
  RadioButtonUnchecked,
  Lock,
  Cancel,
  PlayArrow,
  Delete,
  Archive,
  Download,
} from '@mui/icons-material';
import { useNavigate } from 'react-router-dom';

interface Study {
  id: string;
  studyId: string;
  studyName: string;
  phase: 'START_UP' | 'CONDUCT' | 'CLOSE_OUT';
  status: 'ACTIVE' | 'INACTIVE' | 'COMPLETED';
  createdBy: string;
  lastModified: string;
  protocol: string;
  description: string;
  targetEnrollment: number;
  currentEnrollment: number;
  formsCompleted: number;
  totalForms: number;
  openQueries: number;
  visitCompletion: number;
  startDate: string;
  estimatedEndDate: string;
  principalInvestigator: string;
  sites: number;
  // Store original API data for editing
  originalData?: any;
}

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;
  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`study-tabpanel-${index}`}
      aria-labelledby={`study-tab-${index}`}
      {...other}
    >
      {value === index && <Box sx={{ p: 3 }}>{children}</Box>}
    </div>
  );
}

const Studies: React.FC = () => {
  const [studies, setStudies] = useState<Study[]>([]);
  const [selectedStudy, setSelectedStudy] = useState<Study | null>(null);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [studyDialogOpen, setStudyDialogOpen] = useState(false);
  const [viewMode, setViewMode] = useState<'list' | 'dashboard'>('list');
  const [tabValue, setTabValue] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  
  
  // Form state for creating/editing studies
  const [formData, setFormData] = useState({
    protocol_number: '',
    title: '',
    description: '',
    phase: 'PHASE_I',
    projected_enrollment: ''
  });
  
  const navigate = useNavigate();

  // Fetch studies function
  const fetchStudies = async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await fetch('/api/studies');
      
      if (!response.ok) {
        setError(`Failed to fetch studies: ${response.status}`);
        return;
      }
      
      const data = await response.json();
      
      // Transform API data to match component interface and fetch real stats
      const transformedStudies: Study[] = await Promise.all(data.data?.map(async (study: any) => {
          // Fetch real form completion stats for this study
          let realStats = {
            formsCompleted: 0,
            totalForms: 0,
            visitCompletion: 0
          };

          try {
            const statsResponse = await fetch(`/api/data/stats/${study.id}`);
            if (statsResponse.ok) {
              const statsData = await statsResponse.json();
              if (statsData.status === 'success') {
                realStats = {
                  formsCompleted: statsData.data.formsCompleted,
                  totalForms: statsData.data.totalForms,
                  visitCompletion: statsData.data.visitCompletion
                };
              }
            }
          } catch (error) {
            console.warn(`Could not fetch stats for study ${study.id}:`, error);
          }

          return {
            id: study.id,
            studyId: study.protocol_number,
            studyName: study.title,
            phase: calculateStudyPhase((() => {
              try {
                const user = JSON.parse(localStorage.getItem('user') || '{}');
                return user.phase || 'setup';
              } catch {
                return 'setup';
              }
            })()), // Dynamic based on logged-in user
            status: calculateStudyStatus({
              currentEnrollment: study.current_enrollment || 0,
              targetEnrollment: study.projected_enrollment || 0,
              totalForms: realStats?.totalForms || 0,
              formsCompleted: realStats?.formsCompleted || 0
            }), // Dynamic based on actual data
            createdBy: 'Study Administrator',
            lastModified: study.updated_at || study.created_at,
            protocol: study.protocol_number,
            description: study.description || 'No description available',
            targetEnrollment: study.projected_enrollment || 0,
            currentEnrollment: study.total_subjects || 0,
            formsCompleted: realStats.formsCompleted,
            totalForms: realStats.totalForms,
            openQueries: study.open_queries || 0,
            visitCompletion: realStats.visitCompletion,
            startDate: study.start_date,
            estimatedEndDate: study.estimated_completion_date,
            principalInvestigator: study.sponsor || 'Not assigned',
            sites: study.total_sites || 0,
            // Store original API data for editing
            originalData: study
          };
        }) || []);
        
        setStudies(transformedStudies);
      } catch (error) {
        console.error('Error fetching studies:', error);
        setError('Failed to load studies');
        setStudies([]);
      } finally {
        setLoading(false);
      }
    };

  // Fetch real data from API
  useEffect(() => {
    fetchStudies();
  }, []);


  const getPhaseColor = (phase: string) => {
    switch (phase) {
      case 'START_UP': return 'primary';
      case 'CONDUCT': return 'success';
      case 'CLOSE_OUT': return 'warning';
      default: return 'default';
    }
  };

  // Dynamic status calculation based on data flow
  const calculateStudyStatus = (study: any) => {
    // Check if study has subjects
    const currentEnrollment = study.currentEnrollment || 0;
    const targetEnrollment = study.targetEnrollment || 0;
    const totalForms = study.totalForms || 0;
    const completedForms = study.formsCompleted || 0;
    
    if (currentEnrollment === 0) {
      return 'INACTIVE'; // No subjects enrolled yet
    } else if (currentEnrollment >= targetEnrollment && totalForms > 0 && completedForms >= (currentEnrollment * totalForms)) {
      return 'COMPLETED'; // Target enrollment reached AND all enrolled subjects completed all forms
    } else {
      return 'ACTIVE'; // Has enrolled subjects but either not target reached or not all forms completed
    }
  };

  // Dynamic phase calculation based on user role
  const calculateStudyPhase = (userPhase: string) => {
    // If it's already a phase value, return it directly
    if (['START_UP', 'CONDUCT', 'CLOSE_OUT'].includes(userPhase?.toUpperCase())) {
      return userPhase.toUpperCase();
    }
    
    // Fallback: try to determine from role/username (legacy support)
    const phase = userPhase?.toLowerCase();
    if (phase?.includes('setup') || phase?.includes('administrator')) {
      return 'START_UP';
    } else if (phase?.includes('conduct') || phase?.includes('crc') || phase?.includes('doctor')) {
      return 'CONDUCT';
    } else if (phase?.includes('closure') || phase?.includes('closeout') || phase?.includes('data manager')) {
      return 'CLOSE_OUT';
    } else {
      return 'START_UP'; // Default
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'ACTIVE': return 'success';
      case 'INACTIVE': return 'warning';
      case 'COMPLETED': return 'info';
      default: return 'default';
    }
  };

  const getPhaseIcon = (phase: string) => {
    switch (phase) {
      case 'START_UP': return <Build />;
      case 'CONDUCT': return <LocalHospital />;
      case 'CLOSE_OUT': return <Assessment />;
      default: return <Science />;
    }
  };

  const handleViewStudy = (study: Study) => {
    setSelectedStudy(study);
    setViewMode('dashboard');
    setTabValue(0);
  };

  const handleEditStudy = (study: Study) => {
    if (!canEditStudy(study)) {
      alert('Studies can only be edited when they are INACTIVE or ACTIVE. COMPLETED studies cannot be modified.');
      return;
    }
    setSelectedStudy(study);
    
    // Use original API data for populating form fields
    const originalData = study.originalData;
    console.log('🔧 Editing study with original data:', originalData);
    
    setFormData({
      protocol_number: originalData?.protocol_number || study.studyId || '',
      title: originalData?.title || study.studyName || '',
      description: originalData?.description || study.description || '',
      phase: originalData?.phase || 'PHASE_I',
      projected_enrollment: originalData?.projected_enrollment?.toString() || study.targetEnrollment?.toString() || ''
    });
    setStudyDialogOpen(true);
  };

  // Check if current user is a closeout user
  const isCloseoutUser = () => {
    try {
      const userStr = localStorage.getItem('user');
      if (!userStr) {
        console.log('🔍 No user data in localStorage');
        return false;
      }
      
      const user = JSON.parse(userStr);
      console.log('🔍 Current user data:', { 
        username: user.username, 
        role: user.role, 
        phase: user.phase
      });
      
      const isCloseout = user.phase === 'CLOSE_OUT' || user.role === 'closeout_manager';
      console.log('🔍 Is closeout user:', isCloseout);
      
      return isCloseout;
    } catch (error) {
      console.error('🔍 Error checking closeout user:', error);
      return false;
    }
  };

  const handleCloseoutStudy = async (study: Study) => {
    if (!window.confirm(`Are you sure you want to closeout the study "${study.studyName}"?\n\nThis will:\n1. Export ALL study data as CSV files (subjects, queries, data entries, audit trail)\n2. Delete all subjects, forms, queries, and audit trails\n3. Delete the study itself\n\nThis action cannot be undone.`)) {
      return;
    }

    try {
      const token = localStorage.getItem('token');
      
      // Step 1: Export all study data as CSV ZIP file
      const exportResponse = await fetch(`/api/studies/${study.id}/export`, {
        method: 'GET',
        headers: {
          'Authorization': `Bearer ${token}`,
        }
      });

      if (!exportResponse.ok) {
        throw new Error('Failed to export study data');
      }

      // Get the exported data as blob (ZIP file)
      const blob = await exportResponse.blob();
      
      // Extract filename from response header
      const contentDisposition = exportResponse.headers.get('content-disposition');
      let fileName = `study_${study.studyId}_closeout_${new Date().toISOString().split('T')[0]}.zip`;
      if (contentDisposition) {
        const filenameMatch = contentDisposition.match(/filename="?([^"]+)"?/);
        if (filenameMatch) {
          fileName = filenameMatch[1];
        }
      }
      
      // Create download link
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = fileName;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
      
      alert(`Study data exported successfully as ${fileName}!\n\nThe ZIP file contains CSV files with ALL data that will be deleted:\n• Study details\n• All subjects\n• All data entries\n• All queries\n• Complete audit trail\n\nStarting deletion process...`);
      
      // Step 2: Delete the study and all associated data
      const deleteResponse = await fetch(`/api/studies/${study.id}`, {
        method: 'DELETE',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        }
      });

      const deleteResult = await deleteResponse.json();
      
      if (deleteResult.status === 'success') {
        alert('Study closeout completed successfully!\n\n✅ All data exported to CSV files\n✅ All study data permanently deleted\n\nThe exported files contain a complete record of all deleted data.');
        fetchStudies();
      } else {
        throw new Error(deleteResult.message || 'Failed to delete study');
      }
    } catch (error) {
      console.error('Closeout error:', error);
      alert('Error during closeout process: ' + (error instanceof Error ? error.message : 'Unknown error'));
    }
  };

  const handleDeleteStudy = async (study: Study) => {
    if (!window.confirm(`Are you sure you want to delete the study "${study.studyName}"?\n\nThis will also delete all subjects, forms, queries, and audit trails associated with this study. This action cannot be undone.`)) {
      return;
    }

    try {
      const token = localStorage.getItem('token');
      const response = await fetch(`/api/studies/${study.id}`, {
        method: 'DELETE',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        }
      });

      const result = await response.json();
      
      if (result.status === 'success') {
        alert('Study and all associated data deleted successfully!');
        // Refresh studies list
        fetchStudies();
      } else {
        alert('Failed to delete study: ' + result.message);
      }
    } catch (error) {
      console.error('Error deleting study:', error);
      alert('Error deleting study. Please try again.');
    }
  };

  const handleCreateNewStudy = () => {
    setSelectedStudy(null);
    // Reset form data
    setFormData({
      protocol_number: '',
      title: '',
      description: '',
      phase: 'PHASE_I',
      projected_enrollment: ''
    });
    setStudyDialogOpen(true);
  };

  // Handle form input changes
  const handleFormChange = (field: string, value: string) => {
    setFormData(prev => ({
      ...prev,
      [field]: value
    }));
  };

  // Handle study creation/update
  const handleCreateStudy = async () => {
    try {
      if (!formData.protocol_number || !formData.title) {
        setError('Protocol number and title are required');
        return;
      }

      const payload = {
        protocol_number: formData.protocol_number,
        title: formData.title,
        description: formData.description,
        phase: formData.phase,
        projected_enrollment: parseInt(formData.projected_enrollment) || 0,
        primary_endpoint: 'Primary endpoint to be defined',
        sponsor: 'Study Sponsor',
        indication: 'Clinical indication'
      };

      // Determine if we're creating or updating
      const isUpdating = selectedStudy !== null;
      const url = isUpdating ? `/api/studies/${selectedStudy.id}` : '/api/studies';
      const method = isUpdating ? 'PUT' : 'POST';

      const response = await fetch(url, {
        method: method,
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      });

      const result = await response.json();

      if (!response.ok) {
        const errorMessage = result.message || `Failed to ${isUpdating ? 'update' : 'create'} study`;
        throw errorMessage;
      }

      // Close dialog and refresh studies list
      setStudyDialogOpen(false);
      setSelectedStudy(null);
      setError(null);
      await fetchStudies();
      
    } catch (error) {
      console.error(`Error ${selectedStudy ? 'updating' : 'creating'} study:`, error);
      setError(error instanceof Error ? (error as any).message : `Failed to ${selectedStudy ? 'update' : 'create'} study`);
    }
  };

  const handleBackToList = () => {
    setViewMode('list');
    setSelectedStudy(null);
  };

  const canEditStudy = (study: Study) => {
    // Allow editing for studies that are not completed or locked
    // In a real system, this would also check user permissions
    return study.status === 'INACTIVE' || study.status === 'ACTIVE';
  };

  if (loading) {
    return (
      <Box sx={{ p: 3 }}>
        <LinearProgress />
        <Typography sx={{ mt: 2 }}>Loading studies...</Typography>
      </Box>
    );
  }

  if (viewMode === 'dashboard' && selectedStudy) {
    return (
      <Box>
        {/* Study Dashboard Header */}
        <Box sx={{ mb: 3, display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <Box>
            <Button 
              variant="outlined" 
              onClick={handleBackToList}
              sx={{ mb: 2 }}
              startIcon={<Timeline />}
            >
              Back to Studies List
            </Button>
            <Typography variant="h4" gutterBottom>
              {selectedStudy.studyName}
            </Typography>
            <Stack direction="row" spacing={2} alignItems="center">
              <Chip 
                icon={getPhaseIcon(selectedStudy.phase)}
                label={selectedStudy.phase.replace('_', '-')} 
                color={getPhaseColor(selectedStudy.phase) as any} 
              />
              <Chip 
                label={selectedStudy.status} 
                color={getStatusColor(selectedStudy.status) as any} 
                variant="outlined"
              />
              <Typography variant="body2" color="text.secondary">
                Protocol: {selectedStudy.protocol}
              </Typography>
            </Stack>
          </Box>
        </Box>

        {/* Metrics Cards */}
        <Grid container spacing={3} sx={{ mb: 3 }}>
          <Grid item xs={12} sm={6} md={3}>
            <Card>
              <CardContent>
                <Stack direction="row" justifyContent="space-between" alignItems="center">
                  <Box>
                    <Typography color="text.secondary" gutterBottom variant="body2">
                      Subjects Enrolled
                    </Typography>
                    <Typography variant="h4" color="primary">
                      {selectedStudy.currentEnrollment}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      of {selectedStudy.targetEnrollment} target
                    </Typography>
                  </Box>
                  <Avatar sx={{ bgcolor: 'primary.light' }}>
                    <Group />
                  </Avatar>
                </Stack>
                <LinearProgress 
                  variant="determinate" 
                  value={(selectedStudy.currentEnrollment / selectedStudy.targetEnrollment) * 100}
                  sx={{ mt: 1 }}
                />
              </CardContent>
            </Card>
          </Grid>
          <Grid item xs={12} sm={6} md={3}>
            <Card>
              <CardContent>
                <Stack direction="row" justifyContent="space-between" alignItems="center">
                  <Box>
                    <Typography color="text.secondary" gutterBottom variant="body2">
                      Forms Completed
                    </Typography>
                    <Typography variant="h4" color="success.main">
                      {selectedStudy.formsCompleted}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      of {selectedStudy.totalForms} total
                    </Typography>
                  </Box>
                  <Avatar sx={{ bgcolor: 'success.light' }}>
                    <Assignment />
                  </Avatar>
                </Stack>
                <LinearProgress 
                  variant="determinate" 
                  value={(selectedStudy.formsCompleted / selectedStudy.totalForms) * 100}
                  color="success"
                  sx={{ mt: 1 }}
                />
              </CardContent>
            </Card>
          </Grid>
          <Grid item xs={12} sm={6} md={3}>
            <Card>
              <CardContent>
                <Stack direction="row" justifyContent="space-between" alignItems="center">
                  <Box>
                    <Typography color="text.secondary" gutterBottom variant="body2">
                      Open Queries
                    </Typography>
                    <Typography variant="h4" color="warning.main">
                      {selectedStudy.openQueries}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      requiring attention
                    </Typography>
                  </Box>
                  <Avatar sx={{ bgcolor: 'warning.light' }}>
                    <HelpOutline />
                  </Avatar>
                </Stack>
              </CardContent>
            </Card>
          </Grid>
          <Grid item xs={12} sm={6} md={3}>
            <Card>
              <CardContent>
                <Stack direction="row" justifyContent="space-between" alignItems="center">
                  <Box>
                    <Typography color="text.secondary" gutterBottom variant="body2">
                      Visit Completion
                    </Typography>
                    <Typography variant="h4" color="info.main">
                      {selectedStudy.visitCompletion}%
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      overall progress
                    </Typography>
                  </Box>
                  <Avatar sx={{ bgcolor: 'info.light' }}>
                    <TrendingUp />
                  </Avatar>
                </Stack>
                <LinearProgress 
                  variant="determinate" 
                  value={selectedStudy.visitCompletion}
                  color="info"
                  sx={{ mt: 1 }}
                />
              </CardContent>
            </Card>
          </Grid>
        </Grid>

        {/* Study Tabs */}
        <Paper sx={{ width: '100%' }}>
          <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
            <Tabs value={tabValue} onChange={(e, newValue) => setTabValue(newValue)}>
              <Tab 
                label={
                  <Badge badgeContent={selectedStudy.currentEnrollment} color="primary">
                    Subjects
                  </Badge>
                } 
                icon={<Group />} 
              />
              <Tab 
                label={
                  <Badge badgeContent={`${selectedStudy.formsCompleted}/${selectedStudy.totalForms}`} color="success">
                    Forms
                  </Badge>
                } 
                icon={<Assignment />} 
              />
              <Tab 
                label={
                  <Badge badgeContent={selectedStudy.openQueries} color="warning">
                    Queries
                  </Badge>
                } 
                icon={<HelpOutline />} 
              />
              <Tab label="Audit Log" icon={<Security />} />
            </Tabs>
          </Box>
          
          <TabPanel value={tabValue} index={0}>
            <SubjectsTab studyId={selectedStudy.id} />
          </TabPanel>
          <TabPanel value={tabValue} index={1}>
            <FormsTab studyId={selectedStudy.id} />
          </TabPanel>
          <TabPanel value={tabValue} index={2}>
            <QueriesTab studyId={selectedStudy.id} />
          </TabPanel>
          <TabPanel value={tabValue} index={3}>
            <AuditLogTab studyId={selectedStudy.id} />
          </TabPanel>
        </Paper>
      </Box>
    );
  }

  return (
    <Box>
      {/* Header */}
      <Box sx={{ mb: 3, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Box>
          <Typography variant="h4" gutterBottom>
            Clinical Studies Management
          </Typography>
          <Typography variant="subtitle1" color="text.secondary">
            Manage clinical trials, subjects, and data collection workflows
          </Typography>
        </Box>
        <Button
          variant="contained"
          startIcon={<Add />}
          onClick={handleCreateNewStudy}
          sx={{
            background: 'linear-gradient(45deg, #FE6B8B 30%, #FF8E53 90%)',
            boxShadow: '0 3px 5px 2px rgba(255, 105, 135, .3)',
          }}
        >
          Create New Study
        </Button>
      </Box>

      {/* Statistics Cards */}
      <Grid container spacing={3} sx={{ mb: 3 }}>
        <Grid item xs={12} md={3}>
          <Card sx={{ background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)', color: 'white' }}>
            <CardContent>
              <Stack direction="row" justifyContent="space-between" alignItems="center">
                <Box>
                  <Typography color="inherit" gutterBottom sx={{ opacity: 0.8 }}>
                    Total Studies
                  </Typography>
                  <Typography variant="h4">
                    {studies.length}
                  </Typography>
                </Box>
                <Avatar sx={{ bgcolor: 'rgba(255,255,255,0.2)' }}>
                  <Science />
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
                    Active Studies
                  </Typography>
                  <Typography variant="h4">
                    {studies.filter(s => calculateStudyStatus(s) === 'ACTIVE').length}
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
                    Total Subjects
                  </Typography>
                  <Typography variant="h4">
                    {studies.reduce((sum, s) => sum + s.currentEnrollment, 0)}
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
          <Card sx={{ background: 'linear-gradient(135deg, #fa709a 0%, #fee140 100%)', color: 'white' }}>
            <CardContent>
              <Stack direction="row" justifyContent="space-between" alignItems="center">
                <Box>
                  <Typography color="inherit" gutterBottom sx={{ opacity: 0.8 }}>
                    Open Queries
                  </Typography>
                  <Typography variant="h4">
                    {studies.reduce((sum, s) => sum + s.openQueries, 0)}
                  </Typography>
                </Box>
                <Avatar sx={{ bgcolor: 'rgba(255,255,255,0.2)' }}>
                  <HelpOutline />
                </Avatar>
              </Stack>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Studies Table */}
      <Paper elevation={2}>
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Study Details</TableCell>
                <TableCell>Phase</TableCell>
                <TableCell>Status</TableCell>
                <TableCell>Enrollment</TableCell>
                <TableCell>Progress</TableCell>
                <TableCell>Last Modified</TableCell>
                <TableCell align="center">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {studies.map((study) => (
                <TableRow key={study.id} hover>
                  <TableCell>
                    <Box>
                      <Typography variant="subtitle2" fontWeight={600} sx={{ mb: 0.5 }}>
                        {study.studyId}
                      </Typography>
                      <Typography variant="body1" sx={{ mb: 0.5 }}>
                        {study.studyName}
                      </Typography>
                      <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                        {study.description.substring(0, 100)}...
                      </Typography>
                      <Stack direction="row" spacing={1}>
                        <Chip label={study.protocol} size="small" variant="outlined" />
                        <Chip label={`PI: ${study.principalInvestigator}`} size="small" />
                      </Stack>
                    </Box>
                  </TableCell>
                  <TableCell>
                    <Chip
                      icon={getPhaseIcon(study.phase)}
                      label={study.phase.replace('_', '-')}
                      color={getPhaseColor(study.phase) as any}
                      size="small"
                    />
                  </TableCell>
                  <TableCell>
                    <Chip
                      label={calculateStudyStatus(study)}
                      color={getStatusColor(calculateStudyStatus(study)) as any}
                      size="small"
                    />
                  </TableCell>
                  <TableCell>
                    <Box>
                      <Typography variant="h6" color="primary">
                        {study.currentEnrollment}
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        of {study.targetEnrollment}
                      </Typography>
                      <LinearProgress 
                        variant="determinate" 
                        value={(study.currentEnrollment / study.targetEnrollment) * 100}
                        sx={{ mt: 0.5, height: 4 }}
                      />
                    </Box>
                  </TableCell>
                  <TableCell>
                    <Box>
                      <Typography variant="body2">
                        Forms: {study.formsCompleted}/{study.totalForms}
                      </Typography>
                      <Typography variant="body2">
                        Queries: {study.openQueries}
                      </Typography>
                      <Typography variant="body2">
                        Visits: {study.visitCompletion}%
                      </Typography>
                    </Box>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2">
                      {study.lastModified && study.lastModified !== 'null' 
                        ? new Date(study.lastModified).toLocaleDateString()
                        : 'No date available'
                      }
                    </Typography>
                    <Typography variant="caption" color="text.secondary">
                      by {study.createdBy}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Stack direction="column" spacing={1} alignItems="center">
                      {/* Closeout button - only visible for closeout users */}
                      {isCloseoutUser() && (
                        <Button
                          variant="contained"
                          size="small"
                          color="warning"
                          startIcon={<Archive />}
                          onClick={() => handleCloseoutStudy(study)}
                          sx={{ 
                            mb: 1,
                            minWidth: '120px',
                            fontSize: '0.75rem',
                            fontWeight: 'bold'
                          }}
                        >
                          CLOSEOUT
                        </Button>
                      )}
                      <Stack direction="row" spacing={1}>
                        <Tooltip title="View Study Dashboard">
                          <IconButton
                            size="small"
                            color="primary"
                            onClick={() => handleViewStudy(study)}
                          >
                            <DashboardIcon />
                          </IconButton>
                        </Tooltip>
                        {canEditStudy(study) && (
                          <Tooltip title="Edit Study">
                            <IconButton
                              size="small"
                              color="secondary"
                              onClick={() => handleEditStudy(study)}
                            >
                              <Edit />
                            </IconButton>
                          </Tooltip>
                        )}
                        <Tooltip title="Delete Study">
                          <IconButton
                            size="small"
                            color="error"
                            onClick={() => handleDeleteStudy(study)}
                          >
                            <Delete />
                          </IconButton>
                        </Tooltip>
                      </Stack>
                    </Stack>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </Paper>

      {/* Create/Edit Study Dialog */}
      <Dialog open={studyDialogOpen} onClose={() => { setStudyDialogOpen(false); setSelectedStudy(null); }} maxWidth="md" fullWidth>
        <DialogTitle>
          {selectedStudy ? 'Edit Study' : 'Create New Study'}
        </DialogTitle>
        <DialogContent>
          <Alert severity="info" sx={{ mb: 2 }}>
            {selectedStudy ? 'Editing study details. COMPLETED studies cannot be modified for data integrity.' : 'Create a new clinical study with all required details.'}
          </Alert>
          {error && (
            <Alert severity="error" sx={{ mb: 2 }}>
              {error}
            </Alert>
          )}
          <Grid container spacing={2} sx={{ mt: 1 }}>
            <Grid item xs={12} md={6}>
              <TextField 
                fullWidth 
                label="Protocol Number" 
                value={formData.protocol_number}
                onChange={(e) => handleFormChange('protocol_number', e.target.value)}
                required
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth>
                <InputLabel>Phase</InputLabel>
                <Select
                  value={formData.phase}
                  onChange={(e) => handleFormChange('phase', e.target.value)}
                  label="Phase"
                >
                  <MenuItem value="PHASE_I">Phase I</MenuItem>
                  <MenuItem value="PHASE_II">Phase II</MenuItem>
                  <MenuItem value="PHASE_III">Phase III</MenuItem>
                  <MenuItem value="PHASE_IV">Phase IV</MenuItem>
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12}>
              <TextField 
                fullWidth 
                label="Study Title" 
                value={formData.title}
                onChange={(e) => handleFormChange('title', e.target.value)}
                required
              />
            </Grid>
            <Grid item xs={12}>
              <TextField 
                fullWidth 
                label="Description" 
                multiline 
                rows={3}
                value={formData.description}
                onChange={(e) => handleFormChange('description', e.target.value)}
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField 
                fullWidth 
                label="Target Enrollment" 
                type="number"
                value={formData.projected_enrollment}
                onChange={(e) => handleFormChange('projected_enrollment', e.target.value)}
              />
            </Grid>
          </Grid>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setStudyDialogOpen(false)}>Cancel</Button>
          <Button 
            variant="contained" 
            onClick={handleCreateStudy}
            disabled={!formData.protocol_number || !formData.title}
          >
            {selectedStudy ? 'Update' : 'Create'} Study
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

// Tab Components
const SubjectsTab: React.FC<{ studyId: string }> = ({ studyId }) => {
  const [subjects, setSubjects] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [addSubjectDialog, setAddSubjectDialog] = useState(false);
  const [viewSubjectDialog, setViewSubjectDialog] = useState(false);
  const [editSubjectDialog, setEditSubjectDialog] = useState(false);
  const [selectedSubject, setSelectedSubject] = useState<any>(null);
  const [sites, setSites] = useState<any[]>([]);
  const [expandedVisit, setExpandedVisit] = useState<string | null>(null);
  const [subjectVisits, setSubjectVisits] = useState<any[]>([]);
  const [visitForms, setVisitForms] = useState<any[]>([]);
  const [drafts, setDrafts] = useState<any[]>([]);
  const [pendingReview, setPendingReview] = useState<any[]>([]);
  const [approvedForms, setApprovedForms] = useState<any[]>([]);
  const navigate = useNavigate();
  const [subjectForm, setSubjectForm] = useState({
    subject_number: '',
    screening_number: '',
    initials: '',
    date_of_birth: '',
    gender: 'MALE',
    site_id: '',
    enrollment_date: new Date().toISOString().split('T')[0],
    treatment_arm: ''
  });


  const handleViewSubject = (subject: any) => {
    setSelectedSubject(subject);
    setViewSubjectDialog(true);
  };


  const handleViewTimeline = async (subject: any) => {
    setSelectedSubject(subject);
    await fetchSubjectVisits(subject.id);
    await fetchAssignedForms(studyId);
    await fetchSubjectDrafts(subject.id);
    await fetchPendingReviewForms(subject.id);
    await fetchApprovedForms(subject.id);
    setViewSubjectDialog(true);
  };

  const fetchSubjectVisits = async (subjectId: string) => {
    try {
      const response = await fetch(`/api/subjects/${subjectId}/visits`);
      if (response.ok) {
        const data = await response.json();
        setSubjectVisits(data.data || []);
      } else {
        // If visits API doesn't exist yet, show message
        console.warn('Visits API not yet implemented');
        setSubjectVisits([]);
      }
    } catch (error) {
      console.error('Error fetching visits:', error);
      setSubjectVisits([]);
    }
  };

  const fetchAssignedForms = async (studyId: string) => {
    try {
      const response = await fetch(`/api/forms/study/${studyId}/assignments`);
      if (response.ok) {
        const data = await response.json();
        setVisitForms(data.data || []);
      }
    } catch (error) {
      console.error('Error fetching assigned forms:', error);
      setVisitForms([]);
    }
  };

  // Handle form data entry navigation
  const handleFormEntry = (subjectId: string, visitId: string, formId: string) => {
    navigate(`/data-entry?subject=${subjectId}&visit=${visitId}&form=${formId}`);
  };

  // Fetch drafts for selected subject
  const fetchSubjectDrafts = async (subjectId: string) => {
    try {
      const response = await fetch(`/api/data/drafts/${subjectId}`);
      if (response.ok) {
        const data = await response.json();
        setDrafts(data.data || []);
      } else {
        setDrafts([]);
      }
    } catch (error) {
      console.error('Error fetching drafts:', error);
      setDrafts([]);
    }
  };

  const fetchPendingReviewForms = async (subjectId: string) => {
    try {
      const response = await fetch(`/api/data/pending-review/${subjectId}`);
      if (response.ok) {
        const data = await response.json();
        setPendingReview(data.data || []);
      } else {
        setPendingReview([]);
      }
    } catch (error) {
      console.error('Error fetching pending review forms:', error);
      setPendingReview([]);
    }
  };

  const fetchApprovedForms = async (subjectId: string) => {
    try {
      const response = await fetch(`/api/data/approved/${subjectId}`);
      if (response.ok) {
        const data = await response.json();
        setApprovedForms(data.data || []);
      } else {
        setApprovedForms([]);
      }
    } catch (error) {
      console.error('Error fetching approved forms:', error);
      setApprovedForms([]);
    }
  };

  const handleApproveForm = async (formDataId: string, subjectId: string) => {
    if (!window.confirm('Are you sure you want to approve and sign this form? This will lock the form permanently.')) {
      return;
    }

    try {
      const response = await fetch('/api/data/approve-form', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          formDataId: formDataId,
          reviewerId: 'Current User' // In a real app, this would be the logged-in user
        })
      });

      const result = await response.json();
      
      if (result.status === 'success') {
        alert('Form approved and signed successfully! Form is now locked.');
        // Refresh the lists
        await fetchPendingReviewForms(subjectId);
        await fetchApprovedForms(subjectId);
      } else {
        alert('Failed to approve and sign form: ' + result.message);
      }
    } catch (error) {
      console.error('Error approving form:', error);
      alert('Error approving form. Please try again.');
    }
  };

  const handleRejectForm = async (formDataId: string, subjectId: string) => {
    const rejectionReason = window.prompt('Please enter rejection reason (optional):');
    if (rejectionReason === null) return; // User cancelled

    try {
      const response = await fetch('/api/data/reject-form', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          formDataId: formDataId,
          rejectionReason: rejectionReason || 'Form rejected by reviewer'
        })
      });

      const result = await response.json();
      
      if (result.status === 'success') {
        alert('Form rejected successfully! User can now make changes and resubmit.');
        // Refresh the lists
        await fetchPendingReviewForms(subjectId);
        await fetchApprovedForms(subjectId);
      } else {
        alert('Failed to reject form: ' + result.message);
      }
    } catch (error) {
      console.error('Error rejecting form:', error);
      alert('Error rejecting form. Please try again.');
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

  const handleEditSubject = (subject: any) => {
    setSelectedSubject(subject);
    setSubjectForm({
      subject_number: subject.subject_number || '',
      screening_number: subject.screening_number || '',
      initials: subject.initials || '',
      date_of_birth: subject.date_of_birth || '',
      gender: subject.gender || 'MALE',
      site_id: subject.site_id || '',
      enrollment_date: subject.enrollment_date || new Date().toISOString().split('T')[0],
      treatment_arm: subject.treatment_arm || ''
    });
    setEditSubjectDialog(true);
  };

  const handleUpdateSubject = async () => {
    try {
      setError(null);
      
      if (!selectedSubject || !subjectForm.subject_number) {
        setError('Subject number is required');
        return;
      }

      const response = await fetch(`/api/subjects/${selectedSubject.id}`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          ...subjectForm,
          study_id: studyId
        }),
      });

      const result = await response.json();

      if (!response.ok) {
        const errorMessage = result.message || 'Failed to update subject';
        throw errorMessage;
      }

      // Reset form and close dialog
      setSubjectForm({
        subject_number: '',
        screening_number: '',
        initials: '',
        date_of_birth: '',
        gender: 'MALE',
        site_id: '',
        enrollment_date: new Date().toISOString().split('T')[0],
        treatment_arm: ''
      });
      setEditSubjectDialog(false);
      setSelectedSubject(null);
      
      // Refresh subjects list
      await fetchSubjects();
      
    } catch (error: any) {
      console.error('Error updating subject:', error);
      setError(error instanceof Error ? error.message : 'Failed to update subject');
    }
  };

  const handleCreateSubject = async () => {
    try {
      setError(null);
      
      if (!subjectForm.subject_number || !subjectForm.site_id) {
        setError('Subject number and site are required');
        return;
      }

      const response = await fetch('/api/subjects', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          ...subjectForm,
          study_id: studyId
        }),
      });

      const result = await response.json();

      if (!response.ok) {
        const errorMessage = result.message || 'Failed to create subject';
        throw errorMessage;
      }

      // Reset form and close dialog
      setSubjectForm({
        subject_number: '',
        screening_number: '',
        initials: '',
        date_of_birth: '',
        gender: 'MALE',
        site_id: '',
        enrollment_date: new Date().toISOString().split('T')[0],
        treatment_arm: ''
      });
      setAddSubjectDialog(false);
      
      // Refresh subjects list
      await fetchSubjects();
      
    } catch (error: any) {
      console.error('Error creating subject:', error);
      setError(error instanceof Error ? error.message : 'Failed to create subject');
    }
  };

  const fetchSubjects = async () => {
      try {
        setLoading(true);
        setError(null);
        
        const response = await fetch(`/api/subjects?study_id=${studyId}`);
        
        if (!response.ok) {
          throw new globalThis.Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        console.log('Raw subjects API response:', data);
        
        // Extremely defensive data processing
        const rawSubjects = Array.isArray(data.data) ? data.data : [];
        
        const safeSubjects = rawSubjects.map((subject: any, index: number) => {
          try {
            console.log('Processing subject from API:', subject);

            // Calculate age from date of birth (only if valid date exists)
            let age = null;
            if (subject?.date_of_birth && subject.date_of_birth !== 'Invalid Date' && subject.date_of_birth !== '') {
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
              id: subject?.id || `temp-${index}`,
              subject_number: subject?.subject_number || '',
              screening_number: subject?.screening_number || '',
              initials: subject?.initials || '',
              date_of_birth: subject?.date_of_birth || null,
              age: age,
              gender: subject?.gender || '',
              enrollment_date: subject?.enrollment_date || null,
              treatment_arm: subject?.treatment_arm || '',
              status: subject?.status || '',
              visits_completed: parseInt(String(subject?.visits_completed || '0')) || 0,
              visits_pending: parseInt(String(subject?.visits_pending || '0')) || 0,
              site_id: subject?.site_id || '',
              edc_sites: {
                name: subject?.edc_sites?.name || '',
                site_number: subject?.edc_sites?.site_number || ''
              }
            };
          } catch (err) {
            console.error('Error processing subject at index', index, err);
            return {
              id: `error-${index}`,
              subject_number: 'ERROR',
              screening_number: '',
              initials: 'ERROR',
              date_of_birth: null,
              age: null,
              gender: 'UNKNOWN',
              enrollment_date: null,
              treatment_arm: '',
              status: 'ERROR',
              visits_completed: 0,
              visits_pending: 0,
              site_id: '',
              edc_sites: { name: 'Error', site_number: '' }
            };
          }
        });
        
        console.log('Safe subjects data:', safeSubjects);
        setSubjects(safeSubjects);
        
      } catch (error: any) {
        console.error('Fetch subjects error:', error);
        setError(error.message || 'Failed to load subjects');
        setSubjects([]);
      } finally {
        setLoading(false);
      }
  };

  useEffect(() => {
    fetchSubjects();
    // Set sites directly with real database UUIDs
    setSites([
      { id: 'ba4f8e72-6285-4ae2-a46c-27ccfeac6af7', site_number: 'SITE-001', name: 'Boston Medical Center' },
      { id: 'd9c2194f-1991-4bf8-ac3b-259bb79ed819', site_number: 'SITE-002', name: 'University Medical Center' },
      { id: '8d89af7e-c430-4fae-9934-753a31c2516f', site_number: 'SITE-003', name: 'Los Angeles Clinic' },
      { id: '024aeef6-19a6-4031-a5ac-2eb23c461e28', site_number: 'SITE-004', name: 'Chicago Research Center' }
    ]);
  }, [studyId]);

  const fetchSites = async () => {
    if (!studyId) return;
    try {
      const token = localStorage.getItem('token');
      const response = await fetch(`http://localhost:3000/api/sites?study_id=${studyId}`, {
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        }
      });
      if (!response.ok) {
        const errorMessage = `HTTP error! status: ${response.status}`;
        throw errorMessage;
      }
      const result = await response.json();
      console.log('🏥 Fetched sites from API:', result);
      setSites(result.data || []);
    } catch (error) {
      console.error('Failed to fetch sites:', error);
      setSites([]);
    }
  };

  if (loading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', p: 3 }}>
        <CircularProgress />
        <Typography sx={{ ml: 2 }}>Loading subjects...</Typography>
      </Box>
    );
  }

  if (error) {
    return (
      <Box sx={{ p: 3 }}>
        <Typography color="error">Error: {error}</Typography>
        <Button onClick={fetchSubjects} sx={{ mt: 2 }}>
          Reload Page
        </Button>
      </Box>
    );
  }

  return (
    <Box>
      <Box sx={{ mb: 2, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Typography variant="h6">Subject Management</Typography>
        <Button 
          variant="contained" 
          startIcon={<Add />} 
          size="small"
          onClick={() => setAddSubjectDialog(true)}
        >
          Add New Subject
        </Button>
      </Box>
      <TableContainer component={Paper} variant="outlined">
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Subject ID</TableCell>
              <TableCell>Initials</TableCell>
              <TableCell>Age</TableCell>
              <TableCell>Gender</TableCell>
              <TableCell>Enrollment Date</TableCell>
              <TableCell>Status</TableCell>
              <TableCell>Site</TableCell>
              <TableCell>Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {subjects && subjects.length > 0 ? subjects.map((subject) => (
              <TableRow key={subject.id}>
                <TableCell>{subject.subject_number}</TableCell>
                <TableCell>{subject.initials || 'N/A'}</TableCell>
                <TableCell>{subject.age !== null ? `${subject.age} years` : 'N/A'}</TableCell>
                <TableCell>{subject.gender || 'N/A'}</TableCell>
                <TableCell>{subject.enrollment_date ? new Date(subject.enrollment_date).toLocaleDateString() : 'Not set'}</TableCell>
                <TableCell>
                  <Chip 
                    label={String(subject?.status || 'UNKNOWN')} 
                    size="small" 
                    color={subject.status === 'ENROLLED' ? 'success' : 'default'}
                  />
                </TableCell>
                <TableCell>{subject.edc_sites?.name || 'N/A'}</TableCell>
                <TableCell>
                  <Tooltip title="View Subject Details">
                    <IconButton size="small" onClick={() => handleViewSubject(subject)}>
                      <Visibility />
                    </IconButton>
                  </Tooltip>
                  <Tooltip title="View Timeline & Fill Forms">
                    <IconButton size="small" color="primary" onClick={() => handleViewTimeline(subject)}>
                      <Timeline />
                    </IconButton>
                  </Tooltip>
                  <Tooltip title="Edit Subject">
                    <IconButton size="small" onClick={() => handleEditSubject(subject)}>
                      <Edit />
                    </IconButton>
                  </Tooltip>
                </TableCell>
              </TableRow>
            )) : (
              <TableRow>
                <TableCell colSpan={8} align="center">
                  <Typography>No subjects found</Typography>
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </TableContainer>

      {/* Add Subject Dialog */}
      <Dialog open={addSubjectDialog} onClose={() => setAddSubjectDialog(false)} maxWidth="md" fullWidth>
        <DialogTitle>Enroll New Subject</DialogTitle>
        <DialogContent>
          <Alert severity="info" sx={{ mb: 2 }}>
            Please enter the subject details carefully. Subject number must be unique within this study.
          </Alert>
          {error && (
            <Alert severity="error" sx={{ mb: 2 }}>
              {error}
            </Alert>
          )}
          <Grid container spacing={2} sx={{ mt: 1 }}>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Subject Number"
                value={subjectForm.subject_number}
                onChange={(e) => setSubjectForm(prev => ({ ...prev, subject_number: e.target.value }))}
                required
                placeholder="e.g., 001, 002, 003"
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Screening Number"
                value={subjectForm.screening_number}
                onChange={(e) => setSubjectForm(prev => ({ ...prev, screening_number: e.target.value }))}
                placeholder="e.g., SCR001"
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Initials"
                value={subjectForm.initials}
                onChange={(e) => setSubjectForm(prev => ({ ...prev, initials: e.target.value.toUpperCase() }))}
                placeholder="e.g., JD"
                inputProps={{ maxLength: 4 }}
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth required>
                <InputLabel>Gender</InputLabel>
                <Select
                  value={subjectForm.gender}
                  onChange={(e) => setSubjectForm(prev => ({ ...prev, gender: e.target.value }))}
                  label="Gender"
                >
                  <MenuItem value="MALE">Male</MenuItem>
                  <MenuItem value="FEMALE">Female</MenuItem>
                  <MenuItem value="OTHER">Other</MenuItem>
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Date of Birth"
                type="date"
                value={subjectForm.date_of_birth}
                onChange={(e) => setSubjectForm(prev => ({ ...prev, date_of_birth: e.target.value }))}
                InputLabelProps={{ shrink: true }}
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Enrollment Date"
                type="date"
                value={subjectForm.enrollment_date}
                onChange={(e) => setSubjectForm(prev => ({ ...prev, enrollment_date: e.target.value }))}
                InputLabelProps={{ shrink: true }}
                required
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth required>
                <InputLabel>Site</InputLabel>
                <Select
                  value={subjectForm.site_id}
                  onChange={(e) => setSubjectForm(prev => ({ ...prev, site_id: e.target.value }))}
                  label="Site"
                >
                  {sites.length === 0 ? (
                    <MenuItem disabled>No sites available for this study</MenuItem>
                  ) : (
                    sites.map((site) => {
                      console.log('🏥 Rendering site:', site);
                      return (
                        <MenuItem key={site.id} value={site.id}>
                          {site.site_number} - {site.name}
                        </MenuItem>
                      );
                    })
                  )}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Treatment Arm"
                value={subjectForm.treatment_arm}
                onChange={(e) => setSubjectForm(prev => ({ ...prev, treatment_arm: e.target.value }))}
                placeholder="e.g., Placebo, Active"
              />
            </Grid>
          </Grid>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setAddSubjectDialog(false)}>Cancel</Button>
          <Button 
            variant="contained" 
            onClick={handleCreateSubject}
            disabled={!subjectForm.subject_number || !subjectForm.site_id}
          >
            Enroll Subject
          </Button>
        </DialogActions>
      </Dialog>

      {/* Subject Timeline & Forms Dialog */}
      <Dialog open={viewSubjectDialog} onClose={() => setViewSubjectDialog(false)} maxWidth="lg" fullWidth>
        <DialogTitle>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Box>
              <Typography variant="h6">
                Subject Timeline: {selectedSubject?.subject_number}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                {selectedSubject?.initials} | {selectedSubject?.edc_sites?.name} | Status: {selectedSubject?.status}
              </Typography>
            </Box>
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
                        Age: {selectedSubject.age !== null ? `${selectedSubject.age}` : 'N/A'} | Gender: {selectedSubject.gender || 'N/A'}
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        Initials: {selectedSubject.initials || 'N/A'}
                      </Typography>
                    </CardContent>
                  </Card>
                </Grid>
                <Grid item xs={12} md={3}>
                  <Card variant="outlined">
                    <CardContent>
                      <Typography variant="caption" color="text.secondary">Enrollment</Typography>
                      <Typography variant="body1">
                        {selectedSubject.enrollment_date ? new Date(selectedSubject.enrollment_date).toLocaleDateString() : 'Not enrolled'}
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        Site: {selectedSubject.edc_sites?.name || 'N/A'}
                      </Typography>
                    </CardContent>
                  </Card>
                </Grid>
                <Grid item xs={12} md={3}>
                  <Card variant="outlined">
                    <CardContent>
                      <Typography variant="caption" color="text.secondary">Progress</Typography>
                      <Typography variant="body1">
                        {selectedSubject.visits_completed || 0} visits completed
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        {selectedSubject.visits_pending || 0} pending
                      </Typography>
                    </CardContent>
                  </Card>
                </Grid>
                <Grid item xs={12} md={3}>
                  <Card variant="outlined">
                    <CardContent>
                      <Typography variant="caption" color="text.secondary">Treatment</Typography>
                      <Typography variant="body1">
                        {selectedSubject.treatment_arm || 'Not assigned'}
                      </Typography>
                    </CardContent>
                  </Card>
                </Grid>
              </Grid>

              <Divider sx={{ my: 3 }} />

              {/* Available Forms for Data Entry */}
              <Typography variant="h6" sx={{ mb: 2 }}>Available Forms</Typography>
              {visitForms && visitForms.length > 0 ? (
                <Grid container spacing={2}>
                  {visitForms.map((form) => (
                    <Grid item xs={12} md={6} lg={4} key={form.id}>
                      <Card variant="outlined" sx={{ height: '100%' }}>
                        <CardContent>
                          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                            <Assignment color="primary" />
                            <Typography variant="h6" component="div">
                              {form.name}
                            </Typography>
                          </Box>
                          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                            Version: {form.version} | Status: {form.status}
                          </Typography>
                          <Typography variant="caption" color="text.secondary" sx={{ mb: 2, display: 'block' }}>
                            Assigned: {form.form_structure?.assignmentDates?.[studyId] ? 
                              new Date(form.form_structure.assignmentDates[studyId]).toLocaleDateString() : 
                              'Recently assigned'}
                          </Typography>
                          <Box sx={{ mt: 2 }}>
                            <Button
                              variant="contained"
                              startIcon={<Edit />}
                              onClick={() => handleFormEntry(selectedSubject.id, 'null', form.id)}
                              fullWidth
                              color="primary"
                            >
                              Fill Form
                            </Button>
                          </Box>
                        </CardContent>
                      </Card>
                    </Grid>
                  ))}
                </Grid>
              ) : (
                <Alert severity="info" sx={{ mt: 2 }}>
                  <Typography variant="h6">No forms assigned to this study yet</Typography>
                  <Typography variant="body2">
                    Please go to the Forms tab and assign forms to this study first. Once forms are assigned, 
                    subjects can fill them through this timeline view.
                  </Typography>
                </Alert>
              )}

              {/* Draft Forms Section */}
              <Divider sx={{ my: 3 }} />
              <Typography variant="h6" sx={{ mb: 2 }}>Saved Drafts</Typography>
              {drafts && drafts.length > 0 ? (
                <Grid container spacing={2}>
                  {drafts.map((draft) => (
                    <Grid item xs={12} md={6} key={draft.id}>
                      <Card sx={{ bgcolor: '#fff3e0' }}>
                        <CardContent>
                          <Typography variant="subtitle1" color="warning.main" gutterBottom>
                            📝 {draft.edc_forms?.name || 'Unknown Form'}
                          </Typography>
                          <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                            Version: {draft.edc_forms?.version || 'N/A'}
                          </Typography>
                          <Typography variant="caption" color="text.secondary" display="block" sx={{ mb: 2 }}>
                            Last saved: {new Date(draft.updated_at).toLocaleString()}
                          </Typography>
                          <Chip 
                            label="DRAFT" 
                            color="warning" 
                            size="small" 
                            sx={{ mb: 2 }}
                          />
                          <Box sx={{ mt: 2 }}>
                            <Button
                              variant="outlined"
                              startIcon={<Edit />}
                              onClick={() => handleFormEntry(selectedSubject.id, draft.visit_id || 'null', draft.form_id)}
                              fullWidth
                              color="warning"
                            >
                              Continue Draft
                            </Button>
                          </Box>
                        </CardContent>
                      </Card>
                    </Grid>
                  ))}
                </Grid>
              ) : (
                <Alert severity="info" sx={{ mb: 2 }}>
                  <Typography variant="body2">
                    No saved drafts found. Start filling a form and save as draft to see it here.
                  </Typography>
                </Alert>
              )}

              {/* Pending Review Section */}
              <Divider sx={{ my: 3 }} />
              <Typography variant="h6" sx={{ mb: 2 }}>Forms Pending Review</Typography>
              {pendingReview && pendingReview.length > 0 ? (
                <Grid container spacing={2}>
                  {pendingReview.map((form) => (
                    <Grid item xs={12} md={6} key={form.id}>
                      <Card sx={{ bgcolor: '#e3f2fd' }}>
                        <CardContent>
                          <Typography variant="subtitle1" color="primary.main" gutterBottom>
                            📋 {form.edc_forms?.name || 'Unknown Form'}
                          </Typography>
                          <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                            Version: {form.edc_forms?.version || 'N/A'}
                          </Typography>
                          <Typography variant="caption" color="text.secondary" display="block" sx={{ mb: 2 }}>
                            Submitted: {new Date(form.updated_at).toLocaleString()}
                          </Typography>
                          <Chip 
                            label="PENDING REVIEW" 
                            color="primary" 
                            size="small" 
                            sx={{ mb: 2 }}
                          />
                          <Box sx={{ mt: 2 }}>
                            <Button
                              variant="outlined"
                              startIcon={<Visibility />}
                              onClick={() => handleFormEntry(selectedSubject.id, form.visit_id || 'null', form.form_id)}
                              size="small"
                              color="primary"
                              fullWidth
                              sx={{ mb: 1 }}
                            >
                              Review Form
                            </Button>
                            <Box sx={{ display: 'flex', gap: 1 }}>
                              <Button
                                variant="contained"
                                startIcon={<Lock />}
                                size="small"
                                color="success"
                                onClick={() => handleApproveForm(form.id, selectedSubject.id)}
                                sx={{ flex: 1 }}
                              >
                                Approve & Sign
                              </Button>
                              <Button
                                variant="outlined"
                                startIcon={<Cancel />}
                                size="small"
                                color="error"
                                onClick={() => handleRejectForm(form.id, selectedSubject.id)}
                                sx={{ flex: 1 }}
                              >
                                Reject
                              </Button>
                            </Box>
                          </Box>
                        </CardContent>
                      </Card>
                    </Grid>
                  ))}
                </Grid>
              ) : (
                <Alert severity="info" sx={{ mb: 2 }}>
                  <Typography variant="body2">
                    No forms pending review. Forms appear here after "Submit for Review" is clicked.
                  </Typography>
                </Alert>
              )}

              {/* Signed/Approved Forms Section */}
              <Divider sx={{ my: 3 }} />
              <Typography variant="h6" sx={{ mb: 2 }}>Signed & Approved Forms</Typography>
              {approvedForms && approvedForms.length > 0 ? (
                <Grid container spacing={2}>
                  {approvedForms.map((form) => (
                    <Grid item xs={12} md={6} key={form.id}>
                      <Card sx={{ bgcolor: '#e8f5e8' }}>
                        <CardContent>
                          <Typography variant="subtitle1" color="success.main" gutterBottom>
                            ✅ {form.edc_forms?.name || 'Unknown Form'}
                          </Typography>
                          <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                            Version: {form.edc_forms?.version || 'N/A'}
                          </Typography>
                          <Typography variant="caption" color="text.secondary" display="block" sx={{ mb: 2 }}>
                            Approved: {new Date(form.review_date).toLocaleString()}
                          </Typography>
                          <Chip 
                            label="SIGNED & APPROVED" 
                            color="success" 
                            size="small" 
                            sx={{ mb: 2 }}
                          />
                          <Box sx={{ mt: 2 }}>
                            <Button
                              variant="outlined"
                              startIcon={<Visibility />}
                              onClick={() => handleFormEntry(selectedSubject.id, form.visit_id || 'null', form.form_id)}
                              size="small"
                              color="success"
                              fullWidth
                            >
                              View (Read-Only)
                            </Button>
                          </Box>
                        </CardContent>
                      </Card>
                    </Grid>
                  ))}
                </Grid>
              ) : (
                <Alert severity="info" sx={{ mb: 2 }}>
                  <Typography variant="body2">
                    No signed forms yet. Forms appear here after being approved and signed from the review process.
                  </Typography>
                </Alert>
              )}

              {/* Visits Section - Show message if visits API not implemented yet */}
              {subjectVisits && subjectVisits.length > 0 ? (
                <>
                  <Divider sx={{ my: 3 }} />
                  <Typography variant="h6" sx={{ mb: 2 }}>Visit Schedule</Typography>
                  <List>
                    {subjectVisits.map((visit) => (
                      <ListItem key={visit.id}>
                        <ListItemIcon>
                          <CalendarToday />
                        </ListItemIcon>
                        <ListItemText
                          primary={visit.visit_name}
                          secondary={`Scheduled: ${visit.scheduled_date || 'TBD'} | Status: ${visit.status || 'Pending'}`}
                        />
                      </ListItem>
                    ))}
                  </List>
                </>
              ) : (
                <>
                  <Divider sx={{ my: 3 }} />
                  <Alert severity="info">
                    <Typography variant="body1">
                      <strong>Visit scheduling feature coming soon!</strong>
                    </Typography>
                    <Typography variant="body2">
                      In the meantime, you can fill out forms directly using the "Fill Form" buttons above. 
                      The visit scheduling and timeline will be available once the visits API is implemented.
                    </Typography>
                  </Alert>
                </>
              )}
            </Box>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setViewSubjectDialog(false)}>Close</Button>
          <Button 
            variant="outlined"
            onClick={() => {
              setViewSubjectDialog(false);
              handleEditSubject(selectedSubject);
            }}
          >
            Edit Subject
          </Button>
        </DialogActions>
      </Dialog>

      {/* Edit Subject Dialog */}
      <Dialog open={editSubjectDialog} onClose={() => setEditSubjectDialog(false)} maxWidth="md" fullWidth>
        <DialogTitle>Edit Subject</DialogTitle>
        <DialogContent>
          <Alert severity="warning" sx={{ mb: 2 }}>
            Editing subject information will be logged in the audit trail.
          </Alert>
          {error && (
            <Alert severity="error" sx={{ mb: 2 }}>
              {error}
            </Alert>
          )}
          <Grid container spacing={2} sx={{ mt: 1 }}>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Subject Number"
                value={subjectForm.subject_number}
                onChange={(e) => setSubjectForm(prev => ({ ...prev, subject_number: e.target.value }))}
                required
                disabled // Subject number should not be changed
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Screening Number"
                value={subjectForm.screening_number}
                onChange={(e) => setSubjectForm(prev => ({ ...prev, screening_number: e.target.value }))}
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Initials"
                value={subjectForm.initials}
                onChange={(e) => setSubjectForm(prev => ({ ...prev, initials: e.target.value.toUpperCase() }))}
                inputProps={{ maxLength: 4 }}
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth required>
                <InputLabel>Gender</InputLabel>
                <Select
                  value={subjectForm.gender}
                  onChange={(e) => setSubjectForm(prev => ({ ...prev, gender: e.target.value }))}
                  label="Gender"
                >
                  <MenuItem value="MALE">Male</MenuItem>
                  <MenuItem value="FEMALE">Female</MenuItem>
                  <MenuItem value="OTHER">Other</MenuItem>
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Date of Birth"
                type="date"
                value={subjectForm.date_of_birth}
                onChange={(e) => setSubjectForm(prev => ({ ...prev, date_of_birth: e.target.value }))}
                InputLabelProps={{ shrink: true }}
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Enrollment Date"
                type="date"
                value={subjectForm.enrollment_date}
                onChange={(e) => setSubjectForm(prev => ({ ...prev, enrollment_date: e.target.value }))}
                InputLabelProps={{ shrink: true }}
                required
              />
            </Grid>
            <Grid item xs={12} md={6}>
              <FormControl fullWidth required disabled>
                <InputLabel>Site</InputLabel>
                <Select
                  value={subjectForm.site_id}
                  label="Site"
                  disabled // Site should not be changed after enrollment
                >
                  {sites.map((site) => (
                    <MenuItem key={site.id} value={site.id}>
                      {site.site_number} - {site.name}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Treatment Arm"
                value={subjectForm.treatment_arm}
                onChange={(e) => setSubjectForm(prev => ({ ...prev, treatment_arm: e.target.value }))}
              />
            </Grid>
          </Grid>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setEditSubjectDialog(false)}>Cancel</Button>
          <Button 
            variant="contained" 
            onClick={handleUpdateSubject}
            disabled={!subjectForm.subject_number}
          >
            Update Subject
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

const FormsTab: React.FC<{ studyId: string }> = ({ studyId }) => {
  const [allForms, setAllForms] = useState<any[]>([]);
  const [assignedForms, setAssignedForms] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [assignDialog, setAssignDialog] = useState(false);
  const [selectedForm, setSelectedForm] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchForms = async () => {
      try {
        setLoading(true);
        setError(null);
        
        // Fetch all available forms (PRODUCTION status)
        const formsResponse = await fetch('/api/forms/list');
        const formsData = await formsResponse.json();
        
        if (formsResponse.ok) {
          const productionForms = formsData.data?.filter((form: any) => form.status === 'PRODUCTION') || [];
          setAllForms(productionForms);
        }

        // Fetch forms already assigned to this study from database
        const assignedResponse = await fetch(`/api/forms/study/${studyId}/assignments`);
        const assignedData = await assignedResponse.json();
        
        if (assignedResponse.ok) {
          setAssignedForms(assignedData.data || []);
        } else {
          setAssignedForms([]);
        }
        
      } catch (error) {
        console.error('Error fetching forms:', error);
        setError('Failed to load forms');
        setAllForms([]);
        setAssignedForms([]);
      } finally {
        setLoading(false);
      }
    };

    fetchForms();
  }, [studyId]);

  const handleAssignForm = (form: any) => {
    setSelectedForm(form);
    setAssignDialog(true);
  };

  const handleConfirmAssignment = async () => {
    if (selectedForm) {
      try {
        setError(null);
        
        const response = await fetch(`/api/forms/study/${studyId}/assign`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            formId: selectedForm.id
          }),
        });

        const result = await response.json();

        if (response.ok) {
          // Refresh the forms list to show the updated assignment
          const assignedResponse = await fetch(`/api/forms/study/${studyId}/assignments`);
          const assignedData = await assignedResponse.json();
          
          if (assignedResponse.ok) {
            setAssignedForms(assignedData.data || []);
          }
        } else {
          setError(result.message || 'Failed to assign form');
        }
      } catch (error) {
        console.error('Error assigning form:', error);
        setError('Failed to assign form to study');
      }
    }
    
    setAssignDialog(false);
    setSelectedForm(null);
  };

  const handleUnassignForm = async (formId: string) => {
    try {
      setError(null);
      
      const response = await fetch(`/api/forms/study/${studyId}/assign/${formId}`, {
        method: 'DELETE',
      });

      const result = await response.json();

      if (response.ok) {
        // Refresh the forms list to show the updated assignment
        const assignedResponse = await fetch(`/api/forms/study/${studyId}/assignments`);
        const assignedData = await assignedResponse.json();
        
        if (assignedResponse.ok) {
          setAssignedForms(assignedData.data || []);
        }
      } else {
        setError(result.message || 'Failed to unassign form');
      }
    } catch (error) {
      console.error('Error unassigning form:', error);
      setError('Failed to unassign form from study');
    }
  };

  if (loading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', p: 3 }}>
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Box>
      <Typography variant="h6" sx={{ mb: 2 }}>Form Schedule & Management</Typography>
      
      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      <Box sx={{ mb: 3 }}>
        <Typography variant="h6" gutterBottom>Assigned Forms ({assignedForms.length})</Typography>
        {assignedForms.length === 0 ? (
          <Alert severity="info">No forms assigned to this study yet. Assign forms from the available forms below.</Alert>
        ) : (
          <TableContainer component={Paper} variant="outlined" sx={{ mb: 3 }}>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Form Name</TableCell>
                  <TableCell>Version</TableCell>
                  <TableCell>Status</TableCell>
                  <TableCell>Assigned Visits</TableCell>
                  <TableCell>Assigned Date</TableCell>
                  <TableCell>Actions</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {assignedForms.map((form) => (
                  <TableRow key={form.id}>
                    <TableCell>
                      <Typography variant="body2" fontWeight={600}>
                        {form.name}
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        {form.description}
                      </Typography>
                    </TableCell>
                    <TableCell>{form.version}</TableCell>
                    <TableCell>
                      <Chip 
                        label={form.status} 
                        size="small" 
                        color="success"
                      />
                    </TableCell>
                    <TableCell>
                      <Chip label="Screening" size="small" variant="outlined" sx={{ mr: 0.5 }} />
                      <Chip label="Baseline" size="small" variant="outlined" sx={{ mr: 0.5 }} />
                    </TableCell>
                    <TableCell>
                      <Typography variant="caption">
                        {form.form_structure?.assignmentDates?.[studyId] 
                          ? new Date(form.form_structure.assignmentDates[studyId]).toLocaleDateString()
                          : 'Recently assigned'}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Tooltip title="View Form">
                        <IconButton size="small">
                          <Visibility />
                        </IconButton>
                      </Tooltip>
                      <Tooltip title="Unassign from Study">
                        <IconButton 
                          size="small" 
                          color="error"
                          onClick={() => handleUnassignForm(form.id)}
                        >
                          <Edit />
                        </IconButton>
                      </Tooltip>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        )}
      </Box>

      <Box>
        <Typography variant="h6" gutterBottom>Available Forms to Assign</Typography>
        <TableContainer component={Paper} variant="outlined">
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Form Name</TableCell>
                <TableCell>Version</TableCell>
                <TableCell>Status</TableCell>
                <TableCell>Created</TableCell>
                <TableCell>Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {allForms.filter(form => !assignedForms.find(af => af.id === form.id)).map((form) => (
                <TableRow key={form.id}>
                  <TableCell>
                    <Typography variant="body2" fontWeight={600}>
                      {form.name}
                    </Typography>
                    <Typography variant="caption" color="text.secondary">
                      {form.description}
                    </Typography>
                  </TableCell>
                  <TableCell>{form.version}</TableCell>
                  <TableCell>
                    <Chip 
                      label={form.status} 
                      size="small" 
                      color="success"
                    />
                  </TableCell>
                  <TableCell>
                    <Typography variant="caption">
                      {new Date(form.created_at).toLocaleDateString()}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Tooltip title="Preview Form">
                      <IconButton size="small">
                        <Visibility />
                      </IconButton>
                    </Tooltip>
                    <Tooltip title="Assign to Study">
                      <Button 
                        size="small" 
                        variant="contained" 
                        color="primary"
                        onClick={() => handleAssignForm(form)}
                        sx={{ ml: 1 }}
                      >
                        Assign
                      </Button>
                    </Tooltip>
                  </TableCell>
                </TableRow>
              ))}
              {allForms.filter(form => !assignedForms.find(af => af.id === form.id)).length === 0 && (
                <TableRow>
                  <TableCell colSpan={5} align="center">
                    <Typography>All available forms are already assigned to this study</Typography>
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </TableContainer>
      </Box>

      {/* Form Assignment Dialog */}
      <Dialog open={assignDialog} onClose={() => setAssignDialog(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Assign Form to Study</DialogTitle>
        <DialogContent>
          <Alert severity="info" sx={{ mb: 2 }}>
            This form will be available for data entry by subjects enrolled in this study.
          </Alert>
          {selectedForm && (
            <Box>
              <Typography variant="h6" gutterBottom>
                {selectedForm.name}
              </Typography>
              <Typography variant="body2" color="text.secondary" paragraph>
                {selectedForm.description}
              </Typography>
              <Typography variant="body2">
                <strong>Version:</strong> {selectedForm.version} | <strong>Status:</strong> {selectedForm.status}
              </Typography>
              <Typography variant="body2" sx={{ mt: 1 }}>
                <strong>Default Assignment:</strong> Screening and Baseline visits
              </Typography>
            </Box>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setAssignDialog(false)}>Cancel</Button>
          <Button 
            variant="contained" 
            onClick={handleConfirmAssignment}
          >
            Assign to Study
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

const QueriesTab: React.FC<{ studyId: string }> = ({ studyId }) => {
  const [queries, setQueries] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchQueries = async () => {
      try {
        setLoading(true);
        setError(null);
        
        const response = await fetch(`/api/queries?study_id=${studyId}`);
        
        if (!response.ok) {
          throw new globalThis.Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        console.log('Raw queries API response:', data);
        
        // Extremely defensive data processing
        const rawQueries = Array.isArray(data.data) ? data.data : [];
        
        const safeQueries = rawQueries.map((query: any, index: number) => {
          try {
            return {
              id: query?.id || `temp-${index}`,
              query_id: String(query?.query_id || 'N/A'),
              priority: String(query?.priority || 'MEDIUM'),
              status: String(query?.status || 'OPEN'),
              field_name: String(query?.field_name || 'N/A'),
              created_at: query?.created_at || null,
              edc_subjects: {
                subject_number: String(query?.edc_subjects?.subject_number || 'Unknown')
              },
              edc_forms: {
                form_name: String(query?.edc_forms?.form_name || 'Unknown')
              }
            };
          } catch (err) {
            console.error('Error processing query at index', index, err);
            return {
              id: `error-${index}`,
              query_id: 'ERROR',
              priority: 'MEDIUM',
              status: 'ERROR',
              field_name: 'Error',
              created_at: null,
              edc_subjects: { subject_number: 'Error' },
              edc_forms: { form_name: 'Error' }
            };
          }
        });
        
        console.log('Safe queries data:', safeQueries);
        setQueries(safeQueries);
        
      } catch (error: any) {
        console.error('Fetch queries error:', error);
        setError(error.message || 'Failed to load queries');
        setQueries([]);
      } finally {
        setLoading(false);
      }
    };

    fetchQueries();
  }, [studyId]);

  if (loading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', p: 3 }}>
        <CircularProgress />
      </Box>
    );
  }

  const getPriorityColor = (priority: string | undefined | null) => {
    if (!priority) return 'default';
    switch (priority.toString().toUpperCase()) {
      case 'HIGH': return 'error';
      case 'MEDIUM': return 'warning';
      case 'LOW': return 'success';
      default: return 'default';
    }
  };

  const getSubjectStatusColor = (status: string | undefined | null) => {
    if (!status) return 'default';
    switch (status.toString().toUpperCase()) {
      case 'ENROLLED': return 'success';
      case 'SCREENING': return 'warning';
      case 'ACTIVE': return 'success';
      case 'COMPLETED': return 'info';
      case 'WITHDRAWN': return 'error';
      default: return 'default';
    }
  };

  return (
    <Box>
      <Typography variant="h6" sx={{ mb: 2 }}>Query Management</Typography>
      <TableContainer component={Paper} variant="outlined">
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Query ID</TableCell>
              <TableCell>Subject</TableCell>
              <TableCell>Form/Field</TableCell>
              <TableCell>Priority</TableCell>
              <TableCell>Status</TableCell>
              <TableCell>Created</TableCell>
              <TableCell>Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {queries && queries.length > 0 ? queries.map((query) => (
              <TableRow key={query.id}>
                <TableCell>{query.query_id}</TableCell>
                <TableCell>{query.edc_subjects?.subject_number}</TableCell>
                <TableCell>
                  <Typography variant="body2">{query.edc_forms?.form_name || 'N/A'}</Typography>
                  <Typography variant="caption" color="text.secondary">{query.field_name}</Typography>
                </TableCell>
                <TableCell>
                  <Chip 
                    label={String(query?.priority || 'MEDIUM')} 
                    size="small" 
                    color="default"
                  />
                </TableCell>
                <TableCell>
                  <Chip 
                    label={String(query?.status || 'OPEN')} 
                    size="small" 
                    variant="outlined"
                  />
                </TableCell>
                <TableCell>{query.created_at ? new Date(query.created_at).toLocaleDateString() : 'N/A'}</TableCell>
                <TableCell>
                  <IconButton size="small"><Visibility /></IconButton>
                  <IconButton size="small"><Edit /></IconButton>
                </TableCell>
              </TableRow>
            )) : (
              <TableRow>
                <TableCell colSpan={7} align="center">
                  <Typography>No queries found</Typography>
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  );
};

const AuditLogTab: React.FC<{ studyId: string }> = ({ studyId }) => {
  const [auditLog, setAuditLog] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchAuditLog = async () => {
      try {
        // Try enhanced audit endpoint first
        const response = await fetch(`/api/audit/study/${studyId}?limit=20`);
        const data = await response.json();
        
        if (response.ok) {
          setAuditLog(data.data || []);
        } else {
          // If authentication required, show placeholder data
          console.warn('Audit trail requires authentication. Showing placeholder data.');
          setAuditLog([
            {
              id: '1',
              timestamp: new Date().toISOString(),
              action: 'FORM_SUBMITTED',
              entity_type: 'form_data',
              description: 'Form submitted for review',
              user_id: 'system',
              details: { form_name: 'Demographics Form', subject: 'Subject 001' }
            },
            {
              id: '2', 
              timestamp: new Date(Date.now() - 3600000).toISOString(),
              action: 'SUBJECT_ENROLLED',
              entity_type: 'subject_data',
              description: 'New subject enrolled in study',
              user_id: 'coordinator',
              details: { subject_number: 'Subject 001', site: 'Site 001' }
            }
          ]);
        }
      } catch (error) {
        console.error('Error fetching audit log:', error);
        setAuditLog([]);
      } finally {
        setLoading(false);
      }
    };

    fetchAuditLog();
  }, [studyId]);

  if (loading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', p: 3 }}>
        <CircularProgress />
      </Box>
    );
  }

  const getTypeIcon = (type: string) => {
    switch (type) {
      case 'CREATE': return <Assignment />;
      case 'UPDATE': return <Edit />;
      case 'DELETE': return <Warning />;
      case 'VIEW': return <Visibility />;
      default: return <Timeline />;
    }
  };

  return (
    <Box>
      <Typography variant="h6" sx={{ mb: 2 }}>Audit Trail</Typography>
      <Alert severity="info" sx={{ mb: 2 }}>
        <Typography variant="body2">
          <strong>Note:</strong> Full audit trail functionality requires user authentication. 
          Currently showing placeholder data. Once authentication is implemented, this will show real audit logs.
        </Typography>
      </Alert>
      <List>
        {auditLog && auditLog.length > 0 ? auditLog.map((log) => (
          <ListItem key={log.id} divider>
            <ListItemIcon>
              {getTypeIcon(log.action_type)}
            </ListItemIcon>
            <ListItemText
              primary={`${log.action_type} - ${log.entity_type}`}
              secondary={
                <Box>
                  <Typography variant="body2">{log.field_name ? `Field: ${log.field_name}` : 'Entity action'}</Typography>
                  <Typography variant="caption" color="text.secondary">
                    {log.created_at ? new Date(log.created_at).toLocaleString() : 'N/A'} by {log.edc_users?.first_name || ''} {log.edc_users?.last_name || 'Unknown User'}
                  </Typography>
                </Box>
              }
            />
          </ListItem>
        )) : (
          <ListItem>
            <ListItemText primary="No audit log entries found" />
          </ListItem>
        )}
      </List>
    </Box>
  );
};

export default Studies;