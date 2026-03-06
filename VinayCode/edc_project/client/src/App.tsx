import React, { useState, useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';
// LocalizationProvider removed - not using date pickers

import DashboardReports from './components/DashboardReports';
import Login from './components/Login';
import Studies from './components/Studies';
import SubjectList from './components/SubjectList';
import FormBuilder from './components/FormBuilder';
import SavedForms from './components/SavedForms';
import QueryManagement from './components/QueryManagement';
import AuditTrail from './components/AuditTrail';
import Layout from './components/Layout';
import DataEntry from './components/DataEntry';

const theme = createTheme({
  palette: {
    primary: {
      main: '#1976d2',
    },
    secondary: {
      main: '#dc004e',
    },
    background: {
      default: '#f5f5f5',
    },
  },
  typography: {
    h1: {
      fontSize: '2rem',
      fontWeight: 600,
    },
    h2: {
      fontSize: '1.5rem',
      fontWeight: 500,
    },
  },
});

function App() {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [userData, setUserData] = useState<any>(null);

  useEffect(() => {
    // Check if user is already logged in
    const token = localStorage.getItem('token');
    const user = localStorage.getItem('user');
    if (token && user) {
      try {
        const parsedUser = JSON.parse(user);
        setUserData(parsedUser);
        setIsAuthenticated(true);
      } catch (error) {
        console.error('Error parsing user data:', error);
        localStorage.clear();
        setIsAuthenticated(false);
      }
    }
  }, []);

  const handleLogout = () => {
    localStorage.clear(); // Clear all localStorage data
    setIsAuthenticated(false);
    setUserData(null);
    window.location.href = '/login'; // Force a full page reload to clear any cached state
  };

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Router>
          {!isAuthenticated ? (
            <Routes>
              <Route path="/login" element={<Login setIsAuthenticated={setIsAuthenticated} setUserData={setUserData} />} />
              <Route path="*" element={<Navigate to="/login" />} />
            </Routes>
          ) : (
            <Layout handleLogout={handleLogout} userData={userData}>
              <Routes>
                <Route path="/" element={<DashboardReports />} />
                <Route path="/dashboard" element={<DashboardReports />} />
                <Route path="/dashboards-reports" element={<DashboardReports />} />
                <Route path="/studies" element={<Studies />} />
                <Route path="/subjects" element={<SubjectList />} />
                <Route path="/form-builder" element={<FormBuilder />} />
                <Route path="/saved-forms" element={<SavedForms />} />
                <Route path="/queries" element={<QueryManagement />} />
                <Route path="/audit-trail" element={<AuditTrail />} />
                <Route path="/data-entry" element={<DataEntry />} />
                <Route path="/login" element={<Navigate to="/" />} />
              </Routes>
            </Layout>
          )}
      </Router>
    </ThemeProvider>
  );
}

export default App;
