// Drop-in replacement for base44Client
// Mirrors: api.entities.EntityName.filter/list/create/update
//          api.auth.me/login/register/logout/redirectToLogin
//          api.integrations.Core.UploadFile
//          api.appLogs.logUserInApp

const TOKEN_KEY = 'physio_connect_token';

function getToken() {
  return localStorage.getItem(TOKEN_KEY);
}

function setToken(token) {
  localStorage.setItem(TOKEN_KEY, token);
}

function clearToken() {
  localStorage.removeItem(TOKEN_KEY);
}

async function request(path, options = {}) {
  const token = getToken();
  const headers = { ...options.headers };

  if (!(options.body instanceof FormData)) {
    headers['Content-Type'] = 'application/json';
  }

  if (token) {
    headers['Authorization'] = `Bearer ${token}`;
  }

  const res = await fetch(path, { ...options, headers });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || res.statusText);
  }
  return res.json();
}

// Build an entity helper that matches the base44 SDK interface
function createEntity(endpoint) {
  return {
    // filter(query, sort, limit)  — e.g. filter({ status: "approved" }, "-rating", 8)
    async filter(query = {}, sort, limit) {
      const params = new URLSearchParams();
      for (const [k, v] of Object.entries(query)) {
        if (v !== undefined && v !== null) params.set(k, v);
      }
      if (sort) params.set('sort', sort);
      if (limit) params.set('limit', String(limit));
      return request(`${endpoint}?${params}`);
    },

    // list(sort)  — returns all, optionally sorted
    async list(sort) {
      const params = new URLSearchParams();
      if (sort) params.set('sort', sort);
      params.set('all', '1'); // bypass user-scoping for admin lists
      return request(`${endpoint}?${params}`);
    },

    // create(data)
    async create(data) {
      return request(endpoint, { method: 'POST', body: JSON.stringify(data) });
    },

    // update(id, data)
    async update(id, data) {
      return request(`${endpoint}/${id}`, { method: 'PATCH', body: JSON.stringify(data) });
    },

    // delete(id)
    async delete(id) {
      return request(`${endpoint}/${id}`, { method: 'DELETE' });
    },
  };
}

export const api = {
  entities: {
    Physiotherapist: createEntity('/api/physiotherapists'),
    Appointment: createEntity('/api/appointments'),
    Review: createEntity('/api/reviews'),
    BlockedSlot: createEntity('/api/blocked-slots'),
    AvailabilityOverride: createEntity('/api/availability-overrides'),
  },

  auth: {
    async login(email, password) {
      const data = await request('/api/auth/login', {
        method: 'POST',
        body: JSON.stringify({ email, password }),
      });
      setToken(data.token);
      return data;
    },

    async register(email, password, full_name, phone) {
      const data = await request('/api/auth/register', {
        method: 'POST',
        body: JSON.stringify({ email, password, full_name, phone }),
      });
      setToken(data.token);
      return data;
    },

    async me() {
      const token = getToken();
      if (!token) return null;
      return request('/api/auth/me');
    },

    logout() {
      clearToken();
      localStorage.removeItem('physio_connect_user');
      window.location.href = '/';
    },

    redirectToLogin() {
      window.location.href = '/Login';
    },

    getToken,
    setToken,
    clearToken,
  },

  integrations: {
    Core: {
      async UploadFile({ file }) {
        const formData = new FormData();
        formData.append('file', file);
        return request('/api/upload', { method: 'POST', body: formData });
      },
    },
  },

  appLogs: {
    async logUserInApp() {
      // No-op locally — can wire to analytics later
    },
  },
};

// Default export for backward-compatible `import { base44 } from ...` pattern
export const base44 = api;
export default api;
