import React, { useState } from 'react';
import {
  Box,
  Paper,
  Typography,
  Button,
  Chip,
  IconButton,
  TextField,
  InputAdornment,
  Menu,
  MenuItem,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TablePagination,
  Checkbox,
} from '@mui/material';
import {
  Add as AddIcon,
  Search as SearchIcon,
  FilterList as FilterIcon,
  MoreVert as MoreIcon,
  Launch as LaunchIcon,
} from '@mui/icons-material';

interface Study {
  id: string;
  protocolNumber: string;
  title: string;
  phase: 'STARTUP' | 'CONDUCT' | 'CLOSEOUT';
  sponsor: string;
  enrollmentTarget: number;
  currentEnrollment: number;
  sites: number;
  status: 'ACTIVE' | 'SUSPENDED' | 'COMPLETED';
  startDate: string;
  pi: string;
}

const studies: Study[] = [
  {
    id: '1',
    protocolNumber: 'COV-2021-001',
    title: 'COVID-19 Vaccine Efficacy Study',
    phase: 'CONDUCT',
    sponsor: 'PharmaCorp',
    enrollmentTarget: 500,
    currentEnrollment: 342,
    sites: 15,
    status: 'ACTIVE',
    startDate: '2021-03-15',
    pi: 'Dr. Jane Smith'
  },
  {
    id: '2',
    protocolNumber: 'DIA-2021-002',
    title: 'Type 2 Diabetes Management Trial',
    phase: 'STARTUP',
    sponsor: 'MedResearch Inc',
    enrollmentTarget: 200,
    currentEnrollment: 45,
    sites: 8,
    status: 'ACTIVE',
    startDate: '2021-06-01',
    pi: 'Dr. John Davis'
  },
  {
    id: '3',
    protocolNumber: 'HYP-2020-003',
    title: 'Hypertension Control Study',
    phase: 'CLOSEOUT',
    sponsor: 'CardioHealth',
    enrollmentTarget: 300,
    currentEnrollment: 298,
    sites: 12,
    status: 'COMPLETED',
    startDate: '2020-09-10',
    pi: 'Dr. Sarah Wilson'
  },
  {
    id: '4',
    protocolNumber: 'ONC-2021-004',
    title: 'Breast Cancer Immunotherapy Trial',
    phase: 'CONDUCT',
    sponsor: 'OncoTech',
    enrollmentTarget: 150,
    currentEnrollment: 89,
    sites: 10,
    status: 'ACTIVE',
    startDate: '2021-01-20',
    pi: 'Dr. Michael Chen'
  },
  {
    id: '5',
    protocolNumber: 'ALZ-2021-005',
    title: "Alzheimer's Disease Prevention Study",
    phase: 'STARTUP',
    sponsor: 'NeuroScience Ltd',
    enrollmentTarget: 400,
    currentEnrollment: 12,
    sites: 20,
    status: 'ACTIVE',
    startDate: '2021-11-05',
    pi: 'Dr. Emily Brown'
  }
];

const StudyList: React.FC = () => {
  const [searchText, setSearchText] = useState('');
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);

  const handleMenuOpen = (event: React.MouseEvent<HTMLElement>) => {
    setAnchorEl(event.currentTarget);
  };

  const handleMenuClose = () => {
    setAnchorEl(null);
  };

  const getPhaseColor = (phase: string) => {
    switch (phase) {
      case 'STARTUP': return 'warning';
      case 'CONDUCT': return 'success';
      case 'CLOSEOUT': return 'info';
      default: return 'default';
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'ACTIVE': return 'success';
      case 'SUSPENDED': return 'warning';
      case 'COMPLETED': return 'default';
      default: return 'default';
    }
  };

  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [selected, setSelected] = useState<string[]>([]);

  const handleChangePage = (event: unknown, newPage: number) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (event: React.ChangeEvent<HTMLInputElement>) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  const handleSelectAll = (event: React.ChangeEvent<HTMLInputElement>) => {
    if (event.target.checked) {
      setSelected(studies.map((s) => s.id));
    } else {
      setSelected([]);
    }
  };

  const handleSelectOne = (id: string) => {
    const selectedIndex = selected.indexOf(id);
    let newSelected: string[] = [];

    if (selectedIndex === -1) {
      newSelected = newSelected.concat(selected, id);
    } else if (selectedIndex === 0) {
      newSelected = newSelected.concat(selected.slice(1));
    } else if (selectedIndex === selected.length - 1) {
      newSelected = newSelected.concat(selected.slice(0, -1));
    } else if (selectedIndex > 0) {
      newSelected = newSelected.concat(
        selected.slice(0, selectedIndex),
        selected.slice(selectedIndex + 1),
      );
    }

    setSelected(newSelected);
  };

  const isSelected = (id: string) => selected.indexOf(id) !== -1;

  return (
    <Box>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <Typography variant="h4">Clinical Studies</Typography>
        <Button variant="contained" startIcon={<AddIcon />}>
          New Study
        </Button>
      </Box>

      <Paper sx={{ mb: 3, p: 2 }}>
        <Box display="flex" gap={2} alignItems="center">
          <TextField
            placeholder="Search studies..."
            variant="outlined"
            size="small"
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <SearchIcon />
                </InputAdornment>
              ),
            }}
            sx={{ flexGrow: 1, maxWidth: 400 }}
          />
          <Button startIcon={<FilterIcon />} variant="outlined">
            Filters
          </Button>
          <Chip label="Active: 4" color="success" />
          <Chip label="Completed: 1" />
        </Box>
      </Paper>

      <Paper>
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell padding="checkbox">
                  <Checkbox
                    indeterminate={selected.length > 0 && selected.length < studies.length}
                    checked={studies.length > 0 && selected.length === studies.length}
                    onChange={handleSelectAll}
                  />
                </TableCell>
                <TableCell>Protocol #</TableCell>
                <TableCell>Study Title</TableCell>
                <TableCell>Phase</TableCell>
                <TableCell>Sponsor</TableCell>
                <TableCell>Enrollment</TableCell>
                <TableCell align="center">Sites</TableCell>
                <TableCell>Status</TableCell>
                <TableCell>Principal Investigator</TableCell>
                <TableCell align="center">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {studies
                .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
                .map((study) => {
                  const isStudySelected = isSelected(study.id);
                  const percentage = (study.currentEnrollment / study.enrollmentTarget) * 100;
                  
                  return (
                    <TableRow
                      key={study.id}
                      hover
                      onClick={() => handleSelectOne(study.id)}
                      selected={isStudySelected}
                      sx={{ cursor: 'pointer' }}
                    >
                      <TableCell padding="checkbox">
                        <Checkbox checked={isStudySelected} />
                      </TableCell>
                      <TableCell>
                        <Button
                          size="small"
                          endIcon={<LaunchIcon fontSize="small" />}
                          onClick={(e) => {
                            e.stopPropagation();
                            console.log('Open study', study.protocolNumber);
                          }}
                        >
                          {study.protocolNumber}
                        </Button>
                      </TableCell>
                      <TableCell>{study.title}</TableCell>
                      <TableCell>
                        <Chip
                          label={study.phase}
                          color={getPhaseColor(study.phase)}
                          size="small"
                        />
                      </TableCell>
                      <TableCell>{study.sponsor}</TableCell>
                      <TableCell>
                        <Box>
                          <Typography variant="body2">
                            {study.currentEnrollment}/{study.enrollmentTarget}
                          </Typography>
                          <Typography variant="caption" color="textSecondary">
                            {percentage.toFixed(0)}% complete
                          </Typography>
                        </Box>
                      </TableCell>
                      <TableCell align="center">{study.sites}</TableCell>
                      <TableCell>
                        <Chip
                          label={study.status}
                          color={getStatusColor(study.status)}
                          size="small"
                          variant="outlined"
                        />
                      </TableCell>
                      <TableCell>{study.pi}</TableCell>
                      <TableCell align="center">
                        <IconButton 
                          size="small" 
                          onClick={(e) => {
                            e.stopPropagation();
                            handleMenuOpen(e);
                          }}
                        >
                          <MoreIcon />
                        </IconButton>
                      </TableCell>
                    </TableRow>
                  );
                })}
            </TableBody>
          </Table>
        </TableContainer>
        <TablePagination
          rowsPerPageOptions={[5, 10, 25]}
          component="div"
          count={studies.length}
          rowsPerPage={rowsPerPage}
          page={page}
          onPageChange={handleChangePage}
          onRowsPerPageChange={handleChangeRowsPerPage}
        />
      </Paper>

      <Menu
        anchorEl={anchorEl}
        open={Boolean(anchorEl)}
        onClose={handleMenuClose}
      >
        <MenuItem onClick={handleMenuClose}>View Details</MenuItem>
        <MenuItem onClick={handleMenuClose}>Edit Study</MenuItem>
        <MenuItem onClick={handleMenuClose}>Manage Sites</MenuItem>
        <MenuItem onClick={handleMenuClose}>Export Data</MenuItem>
        <MenuItem onClick={handleMenuClose}>Lock Study</MenuItem>
      </Menu>
    </Box>
  );
};

export default StudyList;