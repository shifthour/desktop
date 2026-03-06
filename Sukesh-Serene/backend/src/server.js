import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import authRoutes from './routes/authRoutes.js';
import tenantAuthRoutes from './routes/tenantAuthRoutes.js';
import pgRoutes from './routes/pgRoutes.js';
import roomRoutes from './routes/roomRoutes.js';
import tenantRoutes from './routes/tenantRoutes.js';
import rentRoutes from './routes/rentRoutes.js';
import dashboardRoutes from './routes/dashboardRoutes.js';
import paymentSubmissionRoutes from './routes/paymentSubmissionRoutes.js';
import expenseRoutes from './routes/expenseRoutes.js';
import complaintRoutes from './routes/complaintRoutes.js';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors({
  origin: [
    'http://localhost:5173',
    'http://localhost:5174',
    'http://localhost:3000',
    'http://localhost:3002',
    'https://serene-living-r6p6tyf21-shifthourjobs-gmailcoms-projects.vercel.app',
    'https://serene-living-4kj2162ti-shifthourjobs-gmailcoms-projects.vercel.app',
    'https://serene-living-h2chpm61y-shifthourjobs-gmailcoms-projects.vercel.app',
    'https://serene-living-q99ng79mw-shifthourjobs-gmailcoms-projects.vercel.app',
    'https://serene-living-cz38x4v8h-shifthourjobs-gmailcoms-projects.vercel.app',
    'https://serene-living-ex8hp2xik-shifthourjobs-gmailcoms-projects.vercel.app',
    /https:\/\/.*\.vercel\.app$/
  ],
  credentials: true
}));
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Routes
app.use('/api/auth', authRoutes);
app.use('/api/tenant-auth', tenantAuthRoutes);
app.use('/api/pgs', pgRoutes);
app.use('/api/rooms', roomRoutes);
app.use('/api/tenants', tenantRoutes);
app.use('/api/rent', rentRoutes);
app.use('/api/dashboard', dashboardRoutes);
app.use('/api/payment-submissions', paymentSubmissionRoutes);
app.use('/api/expenses', expenseRoutes);
app.use('/api/complaints', complaintRoutes);

// Health check
app.get('/api/health', (req, res) => {
  res.json({ status: 'OK', message: 'Serene Living API is running' });
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({ error: 'Something went wrong!' });
});

// Start server
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
  console.log(`📊 Serene Living PG Management Portal API`);
});

export default app;
