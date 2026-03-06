import React, { useEffect, useState } from 'react';
import {
  Box,
  Grid,
  Card,
  CardContent,
  Typography,
  LinearProgress,
  Chip,
  List,
  ListItem,
  ListItemText,
  ListItemIcon,
  Alert,
  Button,
  Paper,
  Divider,
  Stack,
  IconButton,
} from '@mui/material';
import {
  CheckCircle,
  Warning,
  Info,
  Error,
  Science,
  Assignment,
  Lock,
  TrendingUp,
  People,
  Description,
  VerifiedUser,
  Refresh,
} from '@mui/icons-material';
import axios from 'axios';

const PhaseDashboard: React.FC = () => {
  const [userData, setUserData] = useState<any>(null);
  const [studyData, setStudyData] = useState<any>(null);
  const [verificationData, setVerificationData] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    loadUserData();
    verifyData();
  }, []);

  const loadUserData = () => {
    try {
      const user = localStorage.getItem('user');
      const study = localStorage.getItem('studyData');
      
      if (user && study) {
        const parsedUser = JSON.parse(user);
        const parsedStudy = JSON.parse(study);
        setUserData(parsedUser);
        setStudyData(parsedStudy);
      } else {
        // If data is missing, redirect to login
        window.location.href = '/login';
      }
    } catch (error) {
      console.error('Error loading user data:', error);
      // Clear corrupted data and redirect to login
      localStorage.clear();
      window.location.href = '/login';
    }
    setLoading(false);
  };

  const verifyData = async () => {
    try {
      const token = localStorage.getItem('token');
      const response = await axios.get('/api/auth/verify-data', {
        headers: {
          Authorization: `Bearer ${token}`
        }
      });
      setVerificationData(response.data.data);
    } catch (error) {
      console.error('Failed to verify data:', error);
    }
  };

  const getPhaseIcon = (phase: string) => {
    switch (phase) {
      case 'START_UP': return <Science fontSize="large" />;
      case 'CONDUCT': return <Assignment fontSize="large" />;
      case 'CLOSE_OUT': return <Lock fontSize="large" />;
      default: return <Info fontSize="large" />;
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'PASSED': return 'success';
      case 'IN_PROGRESS': return 'info';
      case 'WARNING': return 'warning';
      case 'PENDING': return 'default';
      default: return 'default';
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'PASSED': return <CheckCircle color="success" />;
      case 'IN_PROGRESS': return <Info color="info" />;
      case 'WARNING': return <Warning color="warning" />;
      case 'PENDING': return <Error color="disabled" />;
      default: return <Info />;
    }
  };

  if (loading) {
    return <Box sx={{ p: 3 }}><LinearProgress /></Box>;
  }

  if (!userData || !studyData) {
    return <Alert severity="error">Failed to load user data</Alert>;
  }

  return (
    <Box sx={{ p: 3 }}>
      <Grid container spacing={3}>
        <Grid item xs={12}>
          <Paper sx={{ p: 3, mb: 3 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
              {getPhaseIcon(userData.phase)}
              <Box sx={{ ml: 2, flexGrow: 1 }}>
                <Typography variant="h4" gutterBottom>
                  {userData.phase.replace('_', '-')} Phase Dashboard
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  {userData.firstName} {userData.lastName} - {userData.role}
                </Typography>
              </Box>
              <IconButton onClick={() => { loadUserData(); verifyData(); }}>
                <Refresh />
              </IconButton>
            </Box>
            
            <Stack direction="row" spacing={2}>
              {Object.entries(userData.permissions).filter(([_, value]) => value === true).slice(0, 5).map(([key]) => (
                <Chip 
                  key={key} 
                  label={key.replace(/([A-Z])/g, ' $1').trim()} 
                  size="small" 
                  color="primary" 
                  variant="outlined"
                />
              ))}
            </Stack>
          </Paper>
        </Grid>

        <Grid item xs={12} md={8}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Study Information
              </Typography>
              <List>
                <ListItem>
                  <ListItemText
                    primary="Protocol Number"
                    secondary={studyData.studyInfo.protocolNumber}
                  />
                </ListItem>
                <ListItem>
                  <ListItemText
                    primary="Study Title"
                    secondary={studyData.studyInfo.title}
                  />
                </ListItem>
                <ListItem>
                  <ListItemText
                    primary="Status"
                    secondary={
                      <Chip 
                        label={studyData.studyInfo.status} 
                        size="small" 
                        color={studyData.studyInfo.status === 'ACTIVE' ? 'success' : 'warning'}
                      />
                    }
                  />
                </ListItem>
                {studyData.studyInfo.currentEnrollment && (
                  <ListItem>
                    <ListItemText
                      primary="Enrollment"
                      secondary={`${studyData.studyInfo.currentEnrollment} / ${studyData.studyInfo.projectedEnrollment} subjects`}
                    />
                  </ListItem>
                )}
              </List>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} md={4}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Key Metrics
              </Typography>
              <List dense>
                {Object.entries(studyData.metrics).map(([key, value]) => (
                  <ListItem key={key}>
                    <ListItemText
                      primary={key.replace(/([A-Z])/g, ' $1').trim()}
                      secondary={String(value)}
                    />
                  </ListItem>
                ))}
              </List>
            </CardContent>
          </Card>
        </Grid>

        {userData?.phase === 'START_UP' && studyData?.studyInfo?.forms && (
          <Grid item xs={12}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Form Design Status
                </Typography>
                <Grid container spacing={2}>
                  {studyData.studyInfo.forms.map((form: any) => (
                    <Grid item xs={12} md={4} key={form.id}>
                      <Paper sx={{ p: 2 }}>
                        <Typography variant="subtitle1">{form.name}</Typography>
                        <Typography variant="body2" color="text.secondary">
                          Version: {form.version} | Fields: {form.fields}
                        </Typography>
                        <Chip 
                          label={form.status} 
                          size="small" 
                          color={form.status === 'DRAFT' ? 'default' : 'info'}
                          sx={{ mt: 1 }}
                        />
                      </Paper>
                    </Grid>
                  ))}
                </Grid>
              </CardContent>
            </Card>
          </Grid>
        )}

        {userData?.phase === 'CONDUCT' && studyData?.studyInfo?.subjects && (
          <Grid item xs={12}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Recent Subjects
                </Typography>
                <List>
                  {studyData.studyInfo.subjects.slice(0, 5).map((subject: any) => (
                    <ListItem key={subject.id}>
                      <ListItemIcon>
                        <People />
                      </ListItemIcon>
                      <ListItemText
                        primary={`Subject ${subject.id}`}
                        secondary={`Site: ${subject.site} | Enrolled: ${subject.enrollmentDate} | Visits Pending: ${subject.visitsPending}`}
                      />
                      <Chip 
                        label={subject.status} 
                        size="small" 
                        color={subject.status === 'ACTIVE' ? 'success' : 'default'}
                      />
                    </ListItem>
                  ))}
                </List>
              </CardContent>
            </Card>
          </Grid>
        )}

        {userData?.phase === 'CLOSE_OUT' && studyData?.studyInfo?.closureActivities && (
          <Grid item xs={12}>
            <Card>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Closure Activities
                </Typography>
                <List>
                  {studyData.studyInfo.closureActivities.map((activity: any, index: number) => (
                    <ListItem key={index}>
                      <ListItemIcon>
                        {activity.status === 'COMPLETED' ? <CheckCircle color="success" /> : 
                         activity.status === 'IN_PROGRESS' ? <Info color="info" /> : 
                         <Error color="disabled" />}
                      </ListItemIcon>
                      <ListItemText
                        primary={activity.task}
                        secondary={
                          <LinearProgress 
                            variant="determinate" 
                            value={parseInt(activity.completion)} 
                            sx={{ mt: 1 }}
                          />
                        }
                      />
                      <Typography variant="body2">{activity.completion}</Typography>
                    </ListItem>
                  ))}
                </List>
              </CardContent>
            </Card>
          </Grid>
        )}

        {verificationData && (
          <Grid item xs={12}>
            <Card>
              <CardContent>
                <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                  <VerifiedUser sx={{ mr: 1 }} />
                  <Typography variant="h6">
                    Data Verification - {verificationData.verification.phase} Phase
                  </Typography>
                </Box>
                <Chip 
                  label={`Status: ${verificationData.verification.verificationStatus}`}
                  color={verificationData.verification.verificationStatus === 'PASSED' ? 'success' : 'info'}
                  sx={{ mb: 2 }}
                />
                <Divider sx={{ my: 2 }} />
                <List>
                  {verificationData.verification.checks.map((check: any, index: number) => (
                    <ListItem key={index}>
                      <ListItemIcon>
                        {getStatusIcon(check.status)}
                      </ListItemIcon>
                      <ListItemText
                        primary={check.item}
                        secondary={check.details}
                      />
                      <Chip 
                        label={check.status} 
                        size="small" 
                        color={getStatusColor(check.status) as any}
                      />
                    </ListItem>
                  ))}
                </List>
                <Typography variant="caption" color="text.secondary" sx={{ mt: 2, display: 'block' }}>
                  Verified by: {verificationData.verifiedBy} | {new Date(verificationData.timestamp).toLocaleString()}
                </Typography>
              </CardContent>
            </Card>
          </Grid>
        )}

        <Grid item xs={12}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Sites Overview
              </Typography>
              <Grid container spacing={2}>
                {studyData?.studyInfo?.sites?.map((site: any) => (
                  <Grid item xs={12} md={4} key={site.id}>
                    <Paper sx={{ p: 2 }}>
                      <Typography variant="subtitle1">{site.name}</Typography>
                      <Box sx={{ display: 'flex', justifyContent: 'space-between', mt: 1 }}>
                        <Chip 
                          label={site.status} 
                          size="small" 
                          color={site.status === 'ACTIVE' ? 'success' : 'default'}
                        />
                        {site.subjects !== undefined && (
                          <Typography variant="body2" color="text.secondary">
                            {site.subjects} subjects
                          </Typography>
                        )}
                        {site.dataComplete && (
                          <Typography variant="body2" color="text.secondary">
                            {site.dataComplete} complete
                          </Typography>
                        )}
                      </Box>
                    </Paper>
                  </Grid>
                ))}
              </Grid>
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
};

export default PhaseDashboard;