import React from 'react';
import { Navigate, useLocation } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';

const ProtectedRoute = ({ children, allowedRoles }) => {
  const { user, loading } = useAuth();
  const location = useLocation();

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-gray-900 mx-auto"></div>
          <p className="mt-4 text-gray-600">Loading...</p>
        </div>
      </div>
    );
  }

  if (!user) {
    return <Navigate to="/login" replace />;
  }

  // Check role-based access
  if (allowedRoles && !allowedRoles.includes(user.role)) {
    // Redirect supervisors to PG Properties if they try to access admin-only pages
    if (user.role === 'supervisor') {
      return <Navigate to="/pg-properties" replace />;
    }
    return <Navigate to="/" replace />;
  }

  // Redirect supervisors from root to PG Properties
  if (user.role === 'supervisor' && location.pathname === '/') {
    return <Navigate to="/pg-properties" replace />;
  }

  return children;
};

export default ProtectedRoute;
