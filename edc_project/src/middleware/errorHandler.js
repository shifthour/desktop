const logger = require('../utils/logger');

class AppError extends Error {
  constructor(message, statusCode) {
    super(message);
    this.statusCode = statusCode;
    this.isOperational = true;
    Error.captureStackTrace(this, this.constructor);
  }
}

const errorHandler = (err, req, res, next) => {
  let { statusCode = 500, message, data } = err;

  // Log error
  logger.error({
    error: err,
    request: {
      method: req.method,
      url: req.url,
      body: req.body,
      params: req.params,
      query: req.query,
      user: req.user?.id
    }
  });

  // Handle Sequelize errors
  if (err.name === 'SequelizeValidationError') {
    statusCode = 400;
    message = 'Validation Error';
    data = err.errors?.map(e => ({
      field: e.path,
      message: e.message
    }));
  }

  if (err.name === 'SequelizeUniqueConstraintError') {
    statusCode = 400;
    message = 'Duplicate Entry';
    data = err.errors?.map(e => ({
      field: e.path,
      value: e.value,
      message: `${e.path} must be unique`
    }));
  }

  if (err.name === 'SequelizeDatabaseError') {
    statusCode = 500;
    message = 'Database Error';
  }

  // Handle JWT errors
  if (err.name === 'JsonWebTokenError') {
    statusCode = 401;
    message = 'Invalid token';
  }

  if (err.name === 'TokenExpiredError') {
    statusCode = 401;
    message = 'Token expired';
  }

  // Handle multer errors
  if (err.code === 'LIMIT_FILE_SIZE') {
    statusCode = 400;
    message = 'File too large';
  }

  if (err.code === 'LIMIT_UNEXPECTED_FILE') {
    statusCode = 400;
    message = 'Unexpected file upload';
  }

  // Send error response
  res.status(statusCode).json({
    error: true,
    message,
    ...(process.env.NODE_ENV === 'development' && { 
      stack: err.stack,
      originalError: err.message,
      data 
    })
  });
};

module.exports = errorHandler;
module.exports.AppError = AppError;