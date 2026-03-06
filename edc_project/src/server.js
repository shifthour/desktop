require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const compression = require('compression');
const rateLimit = require('express-rate-limit');
const morgan = require('morgan');
const path = require('path');
const fs = require('fs');

const { syncDatabase } = require('./models');
const logger = require('./utils/logger');
const errorHandler = require('./middleware/errorHandler');

const app = express();
const PORT = process.env.PORT || 3000;

// Security middleware
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      styleSrc: ["'self'", "'unsafe-inline'"],
      scriptSrc: ["'self'"],
      imgSrc: ["'self'", "data:", "https:"],
    },
  },
}));

// CORS configuration
const corsOptions = {
  origin: process.env.ALLOWED_ORIGINS?.split(',') || ['http://localhost:3000'],
  credentials: true,
  optionsSuccessStatus: 200
};
app.use(cors(corsOptions));

// Body parsing middleware
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Compression middleware
app.use(compression());

// Request logging
const accessLogStream = fs.createWriteStream(
  path.join(__dirname, '../logs/access.log'),
  { flags: 'a' }
);
app.use(morgan('combined', { stream: accessLogStream }));
app.use(morgan('dev'));

// Rate limiting
const limiter = rateLimit({
  windowMs: (process.env.RATE_LIMIT_WINDOW || 15) * 60 * 1000,
  max: process.env.RATE_LIMIT_MAX_REQUESTS || 100,
  message: 'Too many requests from this IP, please try again later.',
  standardHeaders: true,
  legacyHeaders: false,
});
app.use('/api/', limiter);

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'healthy',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    environment: process.env.NODE_ENV
  });
});

// API version endpoint
app.get('/api/version', (req, res) => {
  res.json({
    version: '1.0.0',
    api: 'EDC Clinical Trials API',
    compliance: ['21 CFR Part 11', 'GCP', 'GDPR']
  });
});

// Routes (to be implemented)
// Note: These routes require database connection
// Uncomment after setting up PostgreSQL
// app.use('/api/auth', require('./routes/authRoutes'));
// app.use('/api/users', require('./routes/userRoutes'));
app.use('/api/studies', require('./routes/studyRoutes'));
app.use('/api/sites', require('./routes/siteRoutes'));
app.use('/api/subjects', require('./routes/subjectRoutes'));
app.use('/api/forms', require('./routes/formRoutes'));
app.use('/api/visits', require('./routes/visitRoutes'));
app.use('/api/data', require('./routes/dataEntryRoutes'));
app.use('/api/queries', require('./routes/queryRoutes'));
app.use('/api/audit', require('./routes/auditRoutes'));
app.use('/api/reports', require('./routes/reportRoutes'));
app.use('/api/export', require('./routes/exportRoutes'));

// Serve static files in production
if (process.env.NODE_ENV === 'production') {
  app.use(express.static(path.join(__dirname, '../client/build')));
  app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, '../client/build/index.html'));
  });
}

// Global error handler
app.use(errorHandler);

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    error: 'Not Found',
    message: 'The requested resource was not found',
    path: req.path
  });
});

// Start server
const startServer = async () => {
  try {
    // Create required directories
    const dirs = ['./uploads', './logs', './temp'];
    dirs.forEach(dir => {
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }
    });

    // Try to sync database, but don't fail if it's not available
    try {
      await syncDatabase();
      console.log('✅ Database connected successfully');
    } catch (dbError) {
      console.warn('⚠️  Database connection failed:', dbError.message);
      console.warn('⚠️  Server will run without database functionality');
      console.warn('⚠️  Please ensure PostgreSQL is running and databases are created:');
      console.warn('    CREATE DATABASE edc_clinical_trials;');
      console.warn('    CREATE DATABASE edc_audit_trails;');
    }

    // Start listening
    const server = app.listen(PORT, () => {
      logger.info(`Server running on port ${PORT} in ${process.env.NODE_ENV} mode`);
      console.log(`🚀 Server running on port ${PORT}`);
      console.log(`📊 Health check: http://localhost:${PORT}/health`);
      console.log(`🔐 21 CFR Part 11 Compliant EDC System`);
    });

    // Graceful shutdown
    process.on('SIGTERM', () => {
      logger.info('SIGTERM signal received: closing HTTP server');
      server.close(() => {
        logger.info('HTTP server closed');
        process.exit(0);
      });
    });

  } catch (error) {
    logger.error('Failed to start server:', error);
    console.error('Failed to start server:', error);
    process.exit(1);
  }
};

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
  logger.error('Uncaught Exception:', error);
  console.error('Uncaught Exception:', error);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  logger.error('Unhandled Rejection at:', promise, 'reason:', reason);
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
  process.exit(1);
});

startServer();

module.exports = app;