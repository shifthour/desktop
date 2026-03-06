import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { AuthProvider } from './context/AuthContext';
import ProtectedRoute from './components/ProtectedRoute';
import Layout from './components/Layout';
import Login from './pages/Login';
import Dashboard from './pages/Dashboard';
import PGProperties from './pages/PGProperties';
import Rooms from './pages/Rooms';
import Tenants from './pages/Tenants';
import Payments from './pages/Payments';
import PaymentReview from './pages/PaymentReview';
import Expenses from './pages/Expenses';
import Complaints from './pages/Complaints';
import ChangePassword from './pages/ChangePassword';
import TenantLogin from './pages/TenantLogin';
import TenantChangePassword from './pages/TenantChangePassword';
import TenantDashboard from './pages/TenantDashboard';

function App() {
  return (
    <AuthProvider>
      <Router>
        <Routes>
          <Route path="/login" element={<Login />} />
          <Route
            path="/"
            element={
              <ProtectedRoute allowedRoles={['admin']}>
                <Layout>
                  <Dashboard />
                </Layout>
              </ProtectedRoute>
            }
          />
          <Route
            path="/pg-properties"
            element={
              <ProtectedRoute>
                <Layout>
                  <PGProperties />
                </Layout>
              </ProtectedRoute>
            }
          />
          <Route
            path="/rooms"
            element={
              <ProtectedRoute>
                <Layout>
                  <Rooms />
                </Layout>
              </ProtectedRoute>
            }
          />
          <Route
            path="/tenants"
            element={
              <ProtectedRoute>
                <Layout>
                  <Tenants />
                </Layout>
              </ProtectedRoute>
            }
          />
          <Route
            path="/payments"
            element={
              <ProtectedRoute>
                <Layout>
                  <Payments />
                </Layout>
              </ProtectedRoute>
            }
          />
          <Route
            path="/payment-review"
            element={
              <ProtectedRoute allowedRoles={['admin', 'supervisor']}>
                <Layout>
                  <PaymentReview />
                </Layout>
              </ProtectedRoute>
            }
          />
          <Route
            path="/expenses"
            element={
              <ProtectedRoute>
                <Layout>
                  <Expenses />
                </Layout>
              </ProtectedRoute>
            }
          />
          <Route
            path="/complaints"
            element={
              <ProtectedRoute>
                <Layout>
                  <Complaints />
                </Layout>
              </ProtectedRoute>
            }
          />
          <Route
            path="/change-password"
            element={
              <ProtectedRoute>
                <Layout>
                  <ChangePassword />
                </Layout>
              </ProtectedRoute>
            }
          />
          <Route path="/tenant/login" element={<TenantLogin />} />
          <Route path="/tenant/change-password" element={<TenantChangePassword />} />
          <Route path="/tenant/dashboard" element={<TenantDashboard />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </Router>
    </AuthProvider>
  );
}

export default App;
