import React from 'react';
import {
  Box,
  Paper,
  Typography,
  Card,
  CardContent,
  Chip,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Button,
  Tabs,
  Tab,
  LinearProgress,
  Grid
} from '@mui/material';
import { 
  People, 
  Assignment, 
  Schedule, 
  CheckCircle,
  Warning,
  LocationOn 
} from '@mui/icons-material';

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

const StudyDetails: React.FC = () => {
  const [tabValue, setTabValue] = React.useState(0);

  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
  };

  // Sample study data
  const study = {
    id: 'STUDY-2024-001',
    name: 'Phase III Clinical Trial for Treatment X',
    protocol: 'PROT-2024-001',
    phase: 'III',
    sponsor: 'Global Pharma Inc.',
    status: 'ACTIVE',
    startDate: '2024-01-15',
    estimatedEndDate: '2025-12-31',
    enrollment: {
      target: 500,
      current: 234,
      percentage: 47
    }
  };

  const sites = [
    { 
      id: 'SITE-001', 
      name: 'City Medical Center', 
      location: 'New York, NY',
      investigator: 'Dr. Smith',
      subjects: 45,
      status: 'Active' 
    },
    { 
      id: 'SITE-002', 
      name: 'Regional Hospital', 
      location: 'Los Angeles, CA',
      investigator: 'Dr. Johnson',
      subjects: 38,
      status: 'Active' 
    },
    { 
      id: 'SITE-003', 
      name: 'University Medical', 
      location: 'Chicago, IL',
      investigator: 'Dr. Williams',
      subjects: 52,
      status: 'Active' 
    }
  ];

  const milestones = [
    { name: 'First Patient In', date: '2024-02-01', status: 'Completed' },
    { name: '25% Enrollment', date: '2024-06-01', status: 'Completed' },
    { name: '50% Enrollment', date: '2024-10-01', status: 'In Progress' },
    { name: 'Last Patient In', date: '2025-06-01', status: 'Planned' },
    { name: 'Database Lock', date: '2025-12-01', status: 'Planned' }
  ];

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Study Details
      </Typography>

      {/* Study Overview */}
      <Paper sx={{ p: 3, mb: 3 }}>
        <Typography variant="h5" gutterBottom>
          {study.name}
        </Typography>
        <Grid container spacing={2}>
          <Grid item xs={12} md={3}>
            <Typography color="textSecondary">Protocol</Typography>
            <Typography variant="body1">{study.protocol}</Typography>
          </Grid>
          <Grid item xs={12} md={3}>
            <Typography color="textSecondary">Phase</Typography>
            <Typography variant="body1">Phase {study.phase}</Typography>
          </Grid>
          <Grid item xs={12} md={3}>
            <Typography color="textSecondary">Sponsor</Typography>
            <Typography variant="body1">{study.sponsor}</Typography>
          </Grid>
          <Grid item xs={12} md={3}>
            <Typography color="textSecondary">Status</Typography>
            <Chip 
              label={study.status} 
              color="success" 
              size="small"
            />
          </Grid>
        </Grid>

        {/* Enrollment Progress */}
        <Box sx={{ mt: 3 }}>
          <Typography variant="h6" gutterBottom>
            Enrollment Progress
          </Typography>
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
            <Box sx={{ width: '100%', mr: 1 }}>
              <LinearProgress 
                variant="determinate" 
                value={study.enrollment.percentage} 
                sx={{ height: 10, borderRadius: 5 }}
              />
            </Box>
            <Box sx={{ minWidth: 35 }}>
              <Typography variant="body2" color="text.secondary">
                {study.enrollment.percentage}%
              </Typography>
            </Box>
          </Box>
          <Typography variant="body2" color="textSecondary">
            {study.enrollment.current} of {study.enrollment.target} subjects enrolled
          </Typography>
        </Box>
      </Paper>

      {/* Tabs Section */}
      <Paper sx={{ mb: 3 }}>
        <Tabs value={tabValue} onChange={handleTabChange}>
          <Tab icon={<LocationOn />} label="Sites" />
          <Tab icon={<People />} label="Team" />
          <Tab icon={<Schedule />} label="Milestones" />
          <Tab icon={<Assignment />} label="Documents" />
        </Tabs>

        <TabPanel value={tabValue} index={0}>
          {/* Sites Tab */}
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Site ID</TableCell>
                  <TableCell>Name</TableCell>
                  <TableCell>Location</TableCell>
                  <TableCell>Principal Investigator</TableCell>
                  <TableCell>Subjects</TableCell>
                  <TableCell>Status</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {sites.map((site) => (
                  <TableRow key={site.id}>
                    <TableCell>{site.id}</TableCell>
                    <TableCell>{site.name}</TableCell>
                    <TableCell>{site.location}</TableCell>
                    <TableCell>{site.investigator}</TableCell>
                    <TableCell>{site.subjects}</TableCell>
                    <TableCell>
                      <Chip 
                        label={site.status} 
                        color="success" 
                        size="small"
                      />
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </TabPanel>

        <TabPanel value={tabValue} index={1}>
          {/* Team Tab */}
          <Grid container spacing={2}>
            <Grid item xs={12} md={6}>
              <Card>
                <CardContent>
                  <Typography variant="h6">Study Manager</Typography>
                  <Typography>Sarah Johnson</Typography>
                  <Typography color="textSecondary">sarah.johnson@sponsor.com</Typography>
                </CardContent>
              </Card>
            </Grid>
            <Grid item xs={12} md={6}>
              <Card>
                <CardContent>
                  <Typography variant="h6">Medical Monitor</Typography>
                  <Typography>Dr. Michael Chen</Typography>
                  <Typography color="textSecondary">m.chen@sponsor.com</Typography>
                </CardContent>
              </Card>
            </Grid>
            <Grid item xs={12} md={6}>
              <Card>
                <CardContent>
                  <Typography variant="h6">Data Manager</Typography>
                  <Typography>Alex Thompson</Typography>
                  <Typography color="textSecondary">a.thompson@sponsor.com</Typography>
                </CardContent>
              </Card>
            </Grid>
            <Grid item xs={12} md={6}>
              <Card>
                <CardContent>
                  <Typography variant="h6">Statistician</Typography>
                  <Typography>Dr. Emily Davis</Typography>
                  <Typography color="textSecondary">e.davis@sponsor.com</Typography>
                </CardContent>
              </Card>
            </Grid>
          </Grid>
        </TabPanel>

        <TabPanel value={tabValue} index={2}>
          {/* Milestones Tab */}
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Milestone</TableCell>
                  <TableCell>Target Date</TableCell>
                  <TableCell>Status</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {milestones.map((milestone, index) => (
                  <TableRow key={index}>
                    <TableCell>{milestone.name}</TableCell>
                    <TableCell>{milestone.date}</TableCell>
                    <TableCell>
                      <Chip
                        label={milestone.status}
                        color={
                          milestone.status === 'Completed' ? 'success' :
                          milestone.status === 'In Progress' ? 'warning' :
                          'default'
                        }
                        size="small"
                        icon={
                          milestone.status === 'Completed' ? <CheckCircle /> :
                          milestone.status === 'In Progress' ? <Warning /> :
                          undefined
                        }
                      />
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </TabPanel>

        <TabPanel value={tabValue} index={3}>
          {/* Documents Tab */}
          <Grid container spacing={2}>
            <Grid item xs={12}>
              <Button variant="contained" sx={{ mr: 2 }}>
                Protocol v1.2
              </Button>
              <Button variant="contained" sx={{ mr: 2 }}>
                ICF Template
              </Button>
              <Button variant="contained" sx={{ mr: 2 }}>
                CRF Specifications
              </Button>
              <Button variant="contained">
                Study Manual
              </Button>
            </Grid>
          </Grid>
        </TabPanel>
      </Paper>

      {/* Quick Actions */}
      <Paper sx={{ p: 3 }}>
        <Typography variant="h6" gutterBottom>
          Quick Actions
        </Typography>
        <Box sx={{ display: 'flex', gap: 2 }}>
          <Button variant="outlined">Add Site</Button>
          <Button variant="outlined">Export Data</Button>
          <Button variant="outlined">Generate Report</Button>
          <Button variant="outlined">View Metrics</Button>
        </Box>
      </Paper>
    </Box>
  );
};

export default StudyDetails;
