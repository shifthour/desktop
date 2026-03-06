import React, { useState } from 'react';
import {
  Box,
  Paper,
  TextField,
  Button,
  Typography,
  Alert,
  Chip,
  Stack,
  Divider,
  Card,
  CardContent,
  IconButton,
  InputAdornment,
} from '@mui/material';
import {
  Visibility,
  VisibilityOff,
  Science,
  Assignment,
  Lock,
} from '@mui/icons-material';
import axios from 'axios';

interface Props {
  setIsAuthenticated: (value: boolean) => void;
  setUserData?: (data: any) => void;
}

const Login: React.FC<Props> = ({ setIsAuthenticated, setUserData }) => {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [selectedLogin, setSelectedLogin] = useState<number | null>(null);

  const phaseLogins = [
    {
      phase: 'SETUP',
      username: 'startup_admin',
      password: 'Startup@2024',
      role: 'Study Administrator',
      color: 'primary',
      icon: <Science />,
      description: 'Study configuration, form design, UAT management',
      permissions: ['Create Studies', 'Design Forms', 'Setup Sites', 'Configure Workflows', 'Manage UAT', 'Promote Forms']
    },
    {
      phase: 'CONDUCT',
      username: 'conduct_crc',
      password: 'Conduct@2024',
      role: 'Doctor (CRC/PI)',
      color: 'success',
      icon: <Assignment />,
      description: 'Patient care, data entry, query response',
      permissions: ['Enroll Subjects', 'Enter Data', 'Respond to Queries', 'View Patient Data', 'E-Sign Forms']
    },
    {
      phase: 'CLOSEOUT',
      username: 'closeout_manager',
      password: 'Closeout@2024',
      role: 'Data Manager',
      color: 'warning',
      icon: <Lock />,
      description: 'Data review, query management, quality control',
      permissions: ['Review Data', 'Raise Queries', 'Monitor Quality', 'Export Reports', 'Manage Discrepancies']
    }
  ];

  const handleLogin = async () => {
    setError('');
    setLoading(true);

    try {
      const apiUrl = process.env.REACT_APP_API_URL || 'https://edcproject-cw03mgd6j-shifthourjobs-gmailcoms-projects.vercel.app';
      const response = await axios.post(`${apiUrl}/api/auth/login`, {
        username,
        password
      });

      if (response.data.status === 'success') {
        // Clear any existing data first
        localStorage.clear();
        
        // Set new data with session timestamp
        localStorage.setItem('token', response.data.token);
        localStorage.setItem('user', JSON.stringify(response.data.data.user));
        localStorage.setItem('studyData', JSON.stringify(response.data.data.studyData));
        localStorage.setItem('sessionTimestamp', Date.now().toString());
        
        if (setUserData) {
          setUserData(response.data.data.user);
        }
        
        setIsAuthenticated(true);
        
        // Force a page reload to ensure clean state
        window.location.href = '/';
      }
    } catch (err: any) {
      setError(err.response?.data?.message || 'Login failed. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  const handleQuickLogin = (index: number) => {
    const login = phaseLogins[index];
    setUsername(login.username);
    setPassword(login.password);
    setSelectedLogin(index);
    setError('');
  };

  return (
    <Box sx={{ 
      display: 'flex', 
      flexDirection: 'column',
      justifyContent: 'center', 
      alignItems: 'center', 
      minHeight: '100vh',
      bgcolor: 'background.default',
      p: 2
    }}>
      <Typography variant="h3" gutterBottom sx={{ mb: 1, fontWeight: 'bold', color: 'primary.main' }}>
        EDC Clinical Trials System
      </Typography>
      <Typography variant="subtitle1" sx={{ mb: 4, color: 'text.secondary' }}>
        21 CFR Part 11 Compliant Electronic Data Capture
      </Typography>

      <Paper sx={{ p: 4, width: '100%', maxWidth: 500, mb: 3 }}>
        <Typography variant="h5" gutterBottom sx={{ mb: 3, textAlign: 'center' }}>
          System Login
        </Typography>

        {error && (
          <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError('')}>
            {error}
          </Alert>
        )}

        <TextField
          fullWidth
          label="Username"
          margin="normal"
          value={username}
          onChange={(e) => setUsername(e.target.value)}
          disabled={loading}
          autoComplete="username"
        />
        
        <TextField
          fullWidth
          label="Password"
          type={showPassword ? 'text' : 'password'}
          margin="normal"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          disabled={loading}
          autoComplete="current-password"
          InputProps={{
            endAdornment: (
              <InputAdornment position="end">
                <IconButton
                  onClick={() => setShowPassword(!showPassword)}
                  edge="end"
                >
                  {showPassword ? <VisibilityOff /> : <Visibility />}
                </IconButton>
              </InputAdornment>
            ),
          }}
          onKeyPress={(e) => {
            if (e.key === 'Enter' && username && password) {
              handleLogin();
            }
          }}
        />
        
        <Button
          fullWidth
          variant="contained"
          sx={{ mt: 3, mb: 2, py: 1.5 }}
          onClick={handleLogin}
          disabled={!username || !password || loading}
        >
          {loading ? 'Logging in...' : 'Login'}
        </Button>

        {selectedLogin !== null && (
          <Alert severity="info" sx={{ mt: 2 }}>
            Selected: {phaseLogins[selectedLogin].phase} Phase - {phaseLogins[selectedLogin].role}
          </Alert>
        )}
      </Paper>

      <Paper sx={{ p: 3, width: '100%', maxWidth: 800 }}>
        <Typography variant="h6" gutterBottom sx={{ mb: 2 }}>
          Quick Access - Phase-Based Logins
        </Typography>
        <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
          Select a phase-specific login to auto-fill credentials
        </Typography>
        
        <Stack spacing={2}>
          {phaseLogins.map((login, index) => (
            <Card 
              key={index} 
              sx={{ 
                cursor: 'pointer',
                transition: 'all 0.3s',
                border: selectedLogin === index ? 2 : 1,
                borderColor: selectedLogin === index ? `${login.color}.main` : 'divider',
                '&:hover': {
                  transform: 'translateY(-2px)',
                  boxShadow: 3,
                }
              }}
              onClick={() => handleQuickLogin(index)}
            >
              <CardContent>
                <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                  <Box sx={{ mr: 2, color: `${login.color}.main` }}>
                    {login.icon}
                  </Box>
                  <Box sx={{ flexGrow: 1 }}>
                    <Typography variant="h6" sx={{ color: `${login.color}.main` }}>
                      {login.phase} PHASE
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      {login.role}
                    </Typography>
                  </Box>
                  <Chip 
                    label={`${login.username}`} 
                    size="small" 
                    color={login.color as any}
                    variant="outlined"
                  />
                </Box>
                
                <Typography variant="body2" sx={{ mb: 1 }}>
                  {login.description}
                </Typography>
                
                <Divider sx={{ my: 1 }} />
                
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mt: 1 }}>
                  {login.permissions.map((perm, i) => (
                    <Chip 
                      key={i} 
                      label={perm} 
                      size="small" 
                      variant="outlined"
                      sx={{ fontSize: '0.75rem' }}
                    />
                  ))}
                </Box>
              </CardContent>
            </Card>
          ))}
        </Stack>

        <Alert severity="info" sx={{ mt: 3 }}>
          <Typography variant="body2">
            <strong>Note:</strong> Each phase has specific permissions and data access. Select the appropriate 
            login based on your current study phase and role.
          </Typography>
        </Alert>
      </Paper>
    </Box>
  );
};

export default Login;