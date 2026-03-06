import React from 'react';
import {
  Box,
  Paper,
  Typography,
  Card,
  CardContent,
  Button,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Grid
} from '@mui/material';
import {
  GetApp,
  PictureAsPdf,
  TableChart,
  Assessment,
  Schedule,
  TrendingUp
} from '@mui/icons-material';

const Reports: React.FC = () => {
  const [reportType, setReportType] = React.useState('enrollment');
  const [format, setFormat] = React.useState('pdf');

  // Sample report data
  const availableReports = [
    {
      id: 'enrollment',
      name: 'Enrollment Report',
      description: 'Subject enrollment status by site',
      icon: <TrendingUp />,
      lastGenerated: '2024-11-24',
      frequency: 'Weekly'
    },
    {
      id: 'queries',
      name: 'Query Report',
      description: 'Open queries and resolution times',
      icon: <Assessment />,
      lastGenerated: '2024-11-23',
      frequency: 'Daily'
    },
    {
      id: 'monitoring',
      name: 'Monitoring Report',
      description: 'Site monitoring visit summary',
      icon: <Schedule />,
      lastGenerated: '2024-11-20',
      frequency: 'Monthly'
    },
    {
      id: 'data_quality',
      name: 'Data Quality Report',
      description: 'Data completeness and accuracy metrics',
      icon: <TableChart />,
      lastGenerated: '2024-11-22',
      frequency: 'Weekly'
    }
  ];

  const recentReports = [
    { name: 'Enrollment Report - Week 47', date: '2024-11-24', size: '2.3 MB', format: 'PDF' },
    { name: 'Query Report - Nov 23', date: '2024-11-23', size: '1.1 MB', format: 'Excel' },
    { name: 'Data Quality Metrics', date: '2024-11-22', size: '3.5 MB', format: 'PDF' },
    { name: 'Site Monitor Visit - SITE-001', date: '2024-11-20', size: '5.2 MB', format: 'PDF' },
    { name: 'Subject Status Summary', date: '2024-11-19', size: '1.8 MB', format: 'CSV' }
  ];

  const metrics = [
    { label: 'Total Reports Generated', value: '245', change: '+12 this week' },
    { label: 'Scheduled Reports', value: '15', change: '3 pending' },
    { label: 'Custom Reports', value: '8', change: '2 in progress' },
    { label: 'Export Queue', value: '0', change: 'All completed' }
  ];

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Reports
      </Typography>

      {/* Metrics Overview */}
      <Grid container spacing={2} sx={{ mb: 3 }}>
        {metrics.map((metric, index) => (
          <Grid item xs={12} sm={6} md={3} key={index}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h5" gutterBottom>
                {metric.value}
              </Typography>
              <Typography color="textSecondary" variant="body2">
                {metric.label}
              </Typography>
              <Typography color="primary" variant="caption">
                {metric.change}
              </Typography>
            </Paper>
          </Grid>
        ))}
      </Grid>

      {/* Report Generation */}
      <Paper sx={{ p: 3, mb: 3 }}>
        <Typography variant="h6" gutterBottom>
          Generate Report
        </Typography>
        <Grid container spacing={2} alignItems="center">
          <Grid item xs={12} md={4}>
            <FormControl fullWidth size="small">
              <InputLabel>Report Type</InputLabel>
              <Select
                value={reportType}
                label="Report Type"
                onChange={(e) => setReportType(e.target.value)}
              >
                {availableReports.map((report) => (
                  <MenuItem key={report.id} value={report.id}>
                    {report.name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={12} md={3}>
            <FormControl fullWidth size="small">
              <InputLabel>Format</InputLabel>
              <Select
                value={format}
                label="Format"
                onChange={(e) => setFormat(e.target.value)}
              >
                <MenuItem value="pdf">PDF</MenuItem>
                <MenuItem value="excel">Excel</MenuItem>
                <MenuItem value="csv">CSV</MenuItem>
                <MenuItem value="json">JSON</MenuItem>
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={12} md={2}>
            <Button
              variant="contained"
              fullWidth
              startIcon={format === 'pdf' ? <PictureAsPdf /> : <TableChart />}
            >
              Generate
            </Button>
          </Grid>
        </Grid>
      </Paper>

      {/* Available Reports */}
      <Grid container spacing={3} sx={{ mb: 3 }}>
        {availableReports.map((report) => (
          <Grid item xs={12} sm={6} md={3} key={report.id}>
            <Card>
              <CardContent>
                <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                  <Box sx={{ mr: 2, color: 'primary.main' }}>
                    {report.icon}
                  </Box>
                  <Typography variant="h6">
                    {report.name}
                  </Typography>
                </Box>
                <Typography color="textSecondary" variant="body2" sx={{ mb: 2 }}>
                  {report.description}
                </Typography>
                <Typography variant="caption" display="block">
                  Last generated: {report.lastGenerated}
                </Typography>
                <Typography variant="caption" display="block">
                  Frequency: {report.frequency}
                </Typography>
                <Button
                  size="small"
                  variant="outlined"
                  sx={{ mt: 2 }}
                  fullWidth
                >
                  Configure
                </Button>
              </CardContent>
            </Card>
          </Grid>
        ))}
      </Grid>

      {/* Recent Reports */}
      <Paper>
        <Box sx={{ p: 2, borderBottom: 1, borderColor: 'divider' }}>
          <Typography variant="h6">
            Recent Reports
          </Typography>
        </Box>
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Report Name</TableCell>
                <TableCell>Generated Date</TableCell>
                <TableCell>Size</TableCell>
                <TableCell>Format</TableCell>
                <TableCell>Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {recentReports.map((report, index) => (
                <TableRow key={index} hover>
                  <TableCell>{report.name}</TableCell>
                  <TableCell>{report.date}</TableCell>
                  <TableCell>{report.size}</TableCell>
                  <TableCell>{report.format}</TableCell>
                  <TableCell>
                    <Button
                      size="small"
                      startIcon={<GetApp />}
                      color="primary"
                    >
                      Download
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </Paper>
    </Box>
  );
};

export default Reports;
