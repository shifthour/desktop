import React, { createContext, useState, useContext, useEffect } from 'react';

const AuthContext = createContext();

const STORAGE_KEY = 'physio_connect_user';
const TOKEN_KEY = 'physio_connect_token';

export const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(null);
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [isLoadingAuth, setIsLoadingAuth] = useState(true);

  useEffect(() => {
    // Try to restore session from token
    const token = localStorage.getItem(TOKEN_KEY);
    if (token) {
      fetch('/api/auth/me', {
        headers: { Authorization: `Bearer ${token}` },
      })
        .then((res) => (res.ok ? res.json() : Promise.reject()))
        .then((userData) => {
          setUser(userData);
          setIsAuthenticated(true);
          localStorage.setItem(STORAGE_KEY, JSON.stringify(userData));
        })
        .catch(() => {
          // Token expired or invalid — clear
          localStorage.removeItem(TOKEN_KEY);
          localStorage.removeItem(STORAGE_KEY);
        })
        .finally(() => setIsLoadingAuth(false));
    } else {
      // No token — check localStorage fallback
      try {
        const stored = localStorage.getItem(STORAGE_KEY);
        if (stored) {
          const parsed = JSON.parse(stored);
          setUser(parsed);
          setIsAuthenticated(true);
        }
      } catch {
        localStorage.removeItem(STORAGE_KEY);
      }
      setIsLoadingAuth(false);
    }
  }, []);

  const login = (userData) => {
    setUser(userData);
    setIsAuthenticated(true);
    localStorage.setItem(STORAGE_KEY, JSON.stringify(userData));
  };

  const logout = () => {
    setUser(null);
    setIsAuthenticated(false);
    localStorage.removeItem(STORAGE_KEY);
    localStorage.removeItem(TOKEN_KEY);
  };

  const navigateToLogin = () => {
    window.location.href = '/Login';
  };

  return (
    <AuthContext.Provider value={{
      user,
      isAuthenticated,
      isLoadingAuth,
      isLoadingPublicSettings: false,
      authError: null,
      appPublicSettings: null,
      login,
      logout,
      navigateToLogin,
      checkAppState: () => {},
    }}>
      {children}
    </AuthContext.Provider>
  );
};

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};
