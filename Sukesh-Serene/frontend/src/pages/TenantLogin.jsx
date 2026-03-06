import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import axios from 'axios';
import { ArrowLeft } from 'lucide-react';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5000/api';

const TenantLogin = () => {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [loading, setLoading] = useState(false);
  const [showResetPassword, setShowResetPassword] = useState(false);
  const [resetEmail, setResetEmail] = useState('');
  const navigate = useNavigate();

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setSuccess('');
    setLoading(true);

    try {
      const response = await axios.post(`${API_URL}/tenant-auth/login`, {
        email,
        password
      });

      const { token, tenant, firstLogin } = response.data;

      // Store token and tenant data in localStorage
      localStorage.setItem('tenantToken', token);
      localStorage.setItem('tenantData', JSON.stringify(tenant));

      // If first login, redirect to change password
      if (firstLogin) {
        navigate('/tenant/change-password');
      } else {
        navigate('/tenant/dashboard');
      }
    } catch (err) {
      setError(err.response?.data?.error || 'Login failed. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  const handleResetPassword = async (e) => {
    e.preventDefault();
    setError('');
    setSuccess('');
    setLoading(true);

    try {
      const response = await axios.post(`${API_URL}/tenant-auth/reset-password`, {
        email: resetEmail
      });

      setSuccess(response.data.message);
      setResetEmail('');
    } catch (err) {
      setError(err.response?.data?.error || 'Reset failed. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-teal-50 flex items-center justify-center p-4 relative">
      <div className="max-w-md w-full">
        <div className="bg-white rounded-2xl shadow-xl p-8">
          {/* Logo */}
          <div className="flex flex-col items-center mb-8">
            <div className="bg-white p-4 rounded-2xl mb-4">
              <img
                src="/assets/logo.png"
                alt="Serene Living Logo"
                className="h-32 w-auto object-contain"
              />
            </div>
            <h1 className="text-2xl font-bold text-gray-900">Tenant Portal</h1>
            <p className="text-gray-500 mt-1">Serene Living PG</p>
          </div>

          {/* Error/Success Messages */}
          {error && (
            <div className="bg-red-50 text-red-600 px-4 py-3 rounded-lg text-sm mb-4">
              {error}
            </div>
          )}
          {success && (
            <div className="bg-green-50 text-green-600 px-4 py-3 rounded-lg text-sm mb-4">
              {success}
            </div>
          )}

          {showResetPassword ? (
            /* Reset Password Form */
            <form onSubmit={handleResetPassword} className="space-y-6">
              <div>
                <button
                  type="button"
                  onClick={() => {
                    setShowResetPassword(false);
                    setError('');
                    setSuccess('');
                  }}
                  className="flex items-center text-gray-600 hover:text-gray-900 mb-4"
                >
                  <ArrowLeft size={18} className="mr-1" />
                  Back to Login
                </button>
                <h2 className="text-lg font-semibold text-gray-900 mb-2">Reset Password</h2>
                <p className="text-sm text-gray-500 mb-4">
                  Enter your email address and we'll reset your password to the default.
                </p>
              </div>

              <div>
                <label htmlFor="resetEmail" className="block text-sm font-medium text-gray-700 mb-2">
                  Email
                </label>
                <input
                  id="resetEmail"
                  type="email"
                  value={resetEmail}
                  onChange={(e) => setResetEmail(e.target.value)}
                  className="input-field"
                  placeholder="Enter your email"
                  required
                />
              </div>

              <button
                type="submit"
                disabled={loading}
                className="w-full bg-teal-600 text-white py-3 rounded-lg font-medium hover:bg-teal-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {loading ? 'Resetting...' : 'Reset Password'}
              </button>
            </form>
          ) : (
            /* Login Form */
            <form onSubmit={handleSubmit} className="space-y-6">
              <div>
                <label htmlFor="email" className="block text-sm font-medium text-gray-700 mb-2">
                  Email
                </label>
                <input
                  id="email"
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  className="input-field"
                  placeholder="Enter your email"
                  required
                />
              </div>

              <div>
                <label htmlFor="password" className="block text-sm font-medium text-gray-700 mb-2">
                  Password
                </label>
                <input
                  id="password"
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  className="input-field"
                  placeholder="Enter your password"
                  required
                />
              </div>

              <button
                type="submit"
                disabled={loading}
                className="w-full bg-teal-600 text-white py-3 rounded-lg font-medium hover:bg-teal-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {loading ? 'Logging in...' : 'Login'}
              </button>

              <div className="text-center">
                <button
                  type="button"
                  onClick={() => {
                    setShowResetPassword(true);
                    setError('');
                    setSuccess('');
                  }}
                  className="text-sm text-teal-600 hover:text-teal-700 hover:underline"
                >
                  Forgot Password?
                </button>
              </div>
            </form>
          )}
        </div>
      </div>
    </div>
  );
};

export default TenantLogin;
