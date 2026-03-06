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
  CircularProgress,
  Alert,
  Stack,
  Card,
  CardContent,
  Grid,
} from '@mui/material';
import {
  Visibility,
  Edit,
  Delete,
  Add,
  Refresh,
  Description,
  CheckCircle,
  Warning,
  Build,
  Science,
  TrendingUp,
  ArrowUpward,
} from '@mui/icons-material';
import { useNavigate } from 'react-router-dom';

interface SavedForm {
  id: string;
  name: string;
  description: string;
  category: string;
  version: string;
  status: string;
  created_at: string;
  updated_at: string;
}

const SavedForms: React.FC = () => {
  const [forms, setForms] = useState<SavedForm[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [selectedForm, setSelectedForm] = useState<SavedForm | null>(null);
  const navigate = useNavigate();

  // Fetch saved forms
  const fetchForms = async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch('/api/forms/list');
      const result = await response.json();
      
      if (result.status === 'success') {
        setForms(result.data || []);
      } else {
        setError(result.message || 'Failed to load forms');
      }
    } catch (err) {
      console.error('Fetch error:', err);
      setError('Failed to connect to server');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchForms();
  }, []);

  // Delete form
  const handleDelete = async () => {
    if (!selectedForm) return;

    try {
      const response = await fetch(`/api/forms/${selectedForm.id}`, {
        method: 'DELETE'
      });
      const result = await response.json();
      
      if (result.status === 'success') {
        setForms(forms.filter(f => f.id !== selectedForm.id));
        setDeleteDialogOpen(false);
        setSelectedForm(null);
      } else {
        setError(result.message || 'Failed to delete form');
      }
    } catch (err) {
      console.error('Delete error:', err);
      setError('Failed to delete form');
    }
  };

  // Get next status for promotion
  const getNextStatus = (currentStatus: string) => {
    switch (currentStatus?.toUpperCase()) {
      case 'DRAFT':
        return 'UAT_TESTING';
      case 'UAT_TESTING':
      case 'UAT':
        return 'PRODUCTION';
      default:
        return null;
    }
  };

  // Promote form status
  const handlePromote = async (form: SavedForm) => {
    const nextStatus = getNextStatus(form.status);
    if (!nextStatus) return;

    try {
      const response = await fetch(`/api/forms/${form.id}/promote`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ newStatus: nextStatus }),
      });

      const result = await response.json();

      if (result.status === 'success') {
        // Update the form in the list
        setForms(forms.map(f => 
          f.id === form.id 
            ? { ...f, status: nextStatus, updated_at: new Date().toISOString() }
            : f
        ));
      } else {
        setError(result.message || 'Failed to promote form status');
      }
    } catch (err) {
      console.error('Promote error:', err);
      setError('Failed to promote form status');
    }
  };

  // Get status color
  const getStatusColor = (status: string) => {
    switch (status?.toUpperCase()) {
      case 'PRODUCTION':
        return 'success';
      case 'UAT':
      case 'UAT_TESTING':
        return 'warning';
      case 'REVIEW':
        return 'info';
      case 'DRAFT':
      default:
        return 'default';
    }
  };

  // Get status icon
  const getStatusIcon = (status: string) => {
    switch (status?.toUpperCase()) {
      case 'PRODUCTION':
        return <CheckCircle fontSize="small" />;
      case 'UAT':
      case 'UAT_TESTING':
        return <Science fontSize="small" />;
      case 'REVIEW':
        return <Warning fontSize="small" />;
      case 'DRAFT':
      default:
        return <Build fontSize="small" />;
    }
  };

  // Get status display name
  const getStatusDisplayName = (status: string) => {
    switch (status?.toUpperCase()) {
      case 'UAT_TESTING':
        return 'UAT Testing';
      default:
        return status;
    }
  };

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="400px">
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Box>
      {/* Header */}
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <Box>
          <Typography variant="h4" gutterBottom>
            Saved Forms Library
          </Typography>
          <Typography variant="subtitle1" color="text.secondary">
            View and manage all saved clinical forms
          </Typography>
        </Box>
        <Stack direction="row" spacing={2}>
          <Button
            variant="outlined"
            startIcon={<Refresh />}
            onClick={fetchForms}
          >
            Refresh
          </Button>
          <Button
            variant="contained"
            startIcon={<Add />}
            onClick={() => navigate('/form-builder')}
          >
            Create New Form
          </Button>
        </Stack>
      </Box>

      {/* Error Alert */}
      {error && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
          {error}
        </Alert>
      )}

      {/* Statistics Cards */}
      <Grid container spacing={3} sx={{ mb: 3 }}>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="text.secondary" gutterBottom>
                Total Forms
              </Typography>
              <Typography variant="h4">
                {forms.length}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="text.secondary" gutterBottom>
                Production
              </Typography>
              <Typography variant="h4" color="success.main">
                {forms.filter(f => f.status === 'PRODUCTION').length}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="text.secondary" gutterBottom>
                UAT Testing
              </Typography>
              <Typography variant="h4" color="warning.main">
                {forms.filter(f => f.status === 'UAT_TESTING').length}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="text.secondary" gutterBottom>
                Draft
              </Typography>
              <Typography variant="h4" color="text.secondary">
                {forms.filter(f => f.status === 'DRAFT').length}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Forms Table */}
      <Paper elevation={2}>
        {forms.length === 0 ? (
          <Box p={4} textAlign="center">
            <Description sx={{ fontSize: 64, color: 'text.secondary', mb: 2 }} />
            <Typography variant="h6" gutterBottom>
              No Forms Saved Yet
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
              Create your first clinical form to get started
            </Typography>
            <Button
              variant="contained"
              startIcon={<Add />}
              onClick={() => navigate('/form-builder')}
            >
              Create Your First Form
            </Button>
          </Box>
        ) : (
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Form Name</TableCell>
                  <TableCell>Description</TableCell>
                  <TableCell>Version</TableCell>
                  <TableCell>Status</TableCell>
                  <TableCell>Category</TableCell>
                  <TableCell>Created</TableCell>
                  <TableCell>Updated</TableCell>
                  <TableCell align="center">Actions</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {forms.map((form) => (
                  <TableRow key={form.id} hover>
                    <TableCell>
                      <Typography variant="subtitle2" fontWeight={600}>
                        {form.name}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2" color="text.secondary">
                        {form.description || 'No description'}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Chip label={form.version} size="small" variant="outlined" />
                    </TableCell>
                    <TableCell>
                      <Chip
                        icon={getStatusIcon(form.status)}
                        label={getStatusDisplayName(form.status)}
                        color={getStatusColor(form.status)}
                        size="small"
                      />
                    </TableCell>
                    <TableCell>{form.category || 'Clinical'}</TableCell>
                    <TableCell>
                      {new Date(form.created_at).toLocaleDateString()}
                    </TableCell>
                    <TableCell>
                      {new Date(form.updated_at).toLocaleDateString()}
                    </TableCell>
                    <TableCell>
                      <Stack direction="row" spacing={1} justifyContent="center">
                        <Tooltip title="View Form">
                          <IconButton 
                            size="small" 
                            color="primary"
                            onClick={() => navigate(`/form-builder?id=${form.id}&view=true`)}
                          >
                            <Visibility />
                          </IconButton>
                        </Tooltip>
                        <Tooltip title="Edit Form">
                          <IconButton 
                            size="small" 
                            color="primary"
                            onClick={() => navigate(`/form-builder?id=${form.id}`)}
                          >
                            <Edit />
                          </IconButton>
                        </Tooltip>
                        {getNextStatus(form.status) && (
                          <Tooltip title={`Promote to ${getNextStatus(form.status)?.replace('_', ' ')}`}>
                            <IconButton
                              size="small"
                              color="success"
                              onClick={() => handlePromote(form)}
                            >
                              <ArrowUpward />
                            </IconButton>
                          </Tooltip>
                        )}
                        <Tooltip title="Delete Form">
                          <IconButton
                            size="small"
                            color="error"
                            onClick={() => {
                              setSelectedForm(form);
                              setDeleteDialogOpen(true);
                            }}
                          >
                            <Delete />
                          </IconButton>
                        </Tooltip>
                      </Stack>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        )}
      </Paper>

      {/* Delete Confirmation Dialog */}
      <Dialog open={deleteDialogOpen} onClose={() => setDeleteDialogOpen(false)}>
        <DialogTitle>Delete Form</DialogTitle>
        <DialogContent>
          <Typography>
            Are you sure you want to delete the form "{selectedForm?.name}"? 
            This action cannot be undone.
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteDialogOpen(false)}>Cancel</Button>
          <Button onClick={handleDelete} color="error" variant="contained">
            Delete
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default SavedForms;