import axios from 'axios';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:5000/api';

const api = axios.create({
  baseURL: API_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Add token to requests
api.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Handle responses and errors
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('token');
      localStorage.removeItem('user');
      window.location.href = '/login';
    }
    return Promise.reject(error);
  }
);

// Auth API
export const authAPI = {
  login: (credentials) => api.post('/auth/login', credentials),
  register: (userData) => api.post('/auth/register', userData),
  getCurrentUser: () => api.get('/auth/me'),
  changePassword: (data) => api.post('/auth/change-password', data),
};

// Dashboard API
export const dashboardAPI = {
  getStats: (params = {}) => {
    const queryParams = new URLSearchParams();
    if (params.pg_id) queryParams.append('pg_id', params.pg_id);
    const queryString = queryParams.toString();
    return api.get(`/dashboard/stats${queryString ? '?' + queryString : ''}`);
  },
  getMonthlyReport: (month, year) => api.get(`/dashboard/report?month=${month}&year=${year}`),
};

// Rooms API
export const roomsAPI = {
  getAll: () => api.get('/rooms'),
  getById: (id) => api.get(`/rooms/${id}`),
  create: (data) => api.post('/rooms', data),
  update: (id, data) => api.put(`/rooms/${id}`, data),
  delete: (id) => api.delete(`/rooms/${id}`),
  getAllBeds: () => api.get('/rooms/beds'),
};

// Tenants API
export const tenantsAPI = {
  getAll: () => api.get('/tenants'),
  getById: (id) => api.get(`/tenants/${id}`),
  create: (data) => api.post('/tenants', data),
  update: (id, data) => api.put(`/tenants/${id}`, data),
  delete: (id) => api.delete(`/tenants/${id}`),
  vacate: (id) => api.post(`/tenants/${id}/vacate`),
};

// Rent API
export const rentAPI = {
  getAll: (month, year) => {
    const params = new URLSearchParams();
    if (month) params.append('month', month);
    if (year) params.append('year', year);
    return api.get(`/rent?${params.toString()}`);
  },
  create: (data) => api.post('/rent', data),
  update: (id, data) => api.put(`/rent/${id}`, data),
  delete: (id) => api.delete(`/rent/${id}`),
  generateMonthly: (month, year) => api.post('/rent/generate', { month, year }),
};

// PG API
export const pgAPI = {
  getAll: () => api.get('/pgs'),
  getById: (id) => api.get(`/pgs/${id}`),
  create: (data) => api.post('/pgs', data),
  update: (id, data) => api.put(`/pgs/${id}`, data),
  delete: (id) => api.delete(`/pgs/${id}`),
};

export default api;
