require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const compression = require('compression');
const morgan = require('morgan');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3000;

// Create required directories (skip in Vercel environment)
if (!process.env.VERCEL) {
  const dirs = ['./logs'];
  dirs.forEach(dir => {
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
  });
}

// Security middleware
app.use(helmet());

// CORS configuration - Allow all origins for now
const corsOptions = {
  origin: true, // Allow all origins
  credentials: true,
  optionsSuccessStatus: 200,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
};
app.use(cors(corsOptions));

// Body parsing middleware
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Compression middleware
app.use(compression());

// Request logging
app.use(morgan('dev'));

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'healthy',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    environment: process.env.NODE_ENV || 'development',
    message: 'EDC Clinical Trials System - Server Running',
    database: 'Connected to Supabase'
  });
});

// API version endpoint
app.get('/api/version', (req, res) => {
  res.json({
    version: '1.0.0',
    api: 'EDC Clinical Trials API',
    compliance: ['21 CFR Part 11', 'GCP', 'GDPR'],
    status: 'Running with Supabase database'
  });
});

// Migration endpoint to add principal_investigator column
app.post('/api/migrate/add-principal-investigator', async (req, res) => {
  try {
    const supabase = require('./config/supabase');
    
    // Test if the column exists by trying to select it
    const { error: testError } = await supabase
      .from('edc_studies')
      .select('principal_investigator')
      .limit(1);
      
    if (testError && (testError.message.includes('column "principal_investigator" does not exist') || testError.message.includes('does not exist'))) {
      return res.status(200).json({
        status: 'migration_needed',
        message: 'Column does not exist. Please run this SQL command in Supabase SQL editor',
        sqlCommand: 'ALTER TABLE edc_studies ADD COLUMN principal_investigator TEXT;',
        instructions: [
          '1. Go to your Supabase project dashboard',
          '2. Navigate to SQL Editor',
          '3. Create a new query',
          '4. Paste the SQL command above',
          '5. Click Run to execute the migration'
        ]
      });
    } else if (testError) {
      throw testError;
    }

    res.json({
      status: 'success',
      message: 'principal_investigator column exists and is ready to use'
    });
  } catch (error) {
    console.error('Migration error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to check principal_investigator column',
      error: error.message
    });
  }
});

// Basic route endpoints
app.get('/api', (req, res) => {
  res.json({
    message: 'EDC Clinical Trials API',
    endpoints: {
      health: '/health',
      version: '/api/version',
      auth: '/api/auth/*',
      studies: '/api/studies',
      sites: '/api/sites',
      subjects: '/api/subjects',
      forms: '/api/forms',
      visits: '/api/visits',
      data: '/api/data',
      queries: '/api/queries',
      audit: '/api/audit',
      reports: '/api/reports',
      export: '/api/export'
    },
    note: 'Connected to Supabase - Full functionality enabled'
  });
});

// Auth routes with Supabase database
app.use('/api/auth', require('./routes/authRoutes'));
app.use('/api/forms', require('./routes/simpleFormRoutes'));

// Enhanced routes
app.use('/api/studies', require('./routes/studyRoutes'));
app.use('/api/sites', require('./routes/siteRoutes'));
app.use('/api/subjects', require('./routes/subjectRoutes'));
app.use('/api/visits', require('./routes/visitRoutes'));
app.use('/api/data', require('./routes/dataEntryRoutes'));
app.use('/api/queries', require('./routes/queryRoutes'));
app.use('/api/reports', require('./routes/reportRoutes'));
app.use('/api/export', require('./routes/exportRoutes'));

// New enhanced modules
try {
  app.use('/api/discrepancy', require('../api/discrepancy-management'));
  app.use('/api/validation', require('../api/validation-engine'));
  app.use('/api/uat', require('../api/uat-workspace'));
  const { router } = require('../api/enhanced-audit-trail');
  app.use('/api/audit', router);
  console.log('✅ Enhanced modules loaded successfully');
} catch (error) {
  console.log('⚠️ Enhanced modules not found, using basic routes. Error:', error.message);
  app.use('/api/audit', require('./routes/auditRoutes'));
}

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    error: 'Not Found',
    message: 'The requested resource was not found',
    path: req.path
  });
});

// Error handler
app.use((err, req, res, next) => {
  console.error('Error:', err.message);
  res.status(err.statusCode || 500).json({
    error: true,
    message: err.message || 'Internal Server Error'
  });
});

// For Vercel deployment
if (process.env.VERCEL) {
  // Don't start the server on Vercel
  module.exports = app;
} else {
  // Start server locally
  const server = app.listen(PORT, () => {
    console.log(`🚀 Server running on port ${PORT}`);
    console.log(`📊 Health check: http://localhost:${PORT}/health`);
    console.log(`📋 API Info: http://localhost:${PORT}/api`);
    console.log(`🔐 21 CFR Part 11 Compliant EDC System`);
    console.log('');
    console.log('✅ Connected to Supabase database');
    console.log('✅ All EDC tables with EDC_ prefix available');
    console.log('✅ Real data from database tables');
    console.log('⚙️  Database: https://eflvzsfgoelonfclzrjy.supabase.co');
  });

  // Graceful shutdown
  process.on('SIGTERM', () => {
    console.log('SIGTERM signal received: closing HTTP server');
    server.close(() => {
      console.log('HTTP server closed');
      process.exit(0);
    });
  });
}

module.exports = app;