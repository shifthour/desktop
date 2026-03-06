/// Application-wide constants
class AppConstants {
  // Shift Cancellation Rules
  static const int freeCancellationHours = 24;
  static const double cancellationPenaltyRate = 0.5; // 50% penalty
  
  // Pagination Limits
  static const int shiftsPageSize = 100;
  static const int jobsPageSize = 50;
  static const int todayShiftsLimit = 20;
  
  // OTP Configuration
  static const int otpLength = 6;
  static const int otpValidityMinutes = 10;
  static const int otpMinValue = 100000;
  static const int otpMaxValue = 999999;
  
  // Wallet Configuration
  static const double minWithdrawalAmount = 1.0;
  static const String defaultCurrency = 'INR';
  
  // Query Timeouts (in seconds)
  static const int authTimeoutSeconds = 20;
  static const int queryTimeoutSeconds = 30;
  static const int uploadTimeoutSeconds = 60;
  static const int paymentTimeoutSeconds = 45;
  
  // UI Constants
  static const int maxShiftsPerDay = 20;
  static const int skeletonCardCount = 3;
  
  // Error Messages
  static const String networkError = 'Network error. Please check your internet connection.';
  static const String genericError = 'An unexpected error occurred. Please try again.';
  static const String authError = 'Authentication failed. Please log in again.';
  static const String timeoutError = 'Request timed out. Please try again.';
}

/// Feature Flags
class FeatureFlags {
  static const bool enableDebugLogging = false; // Set to false for production
  static const bool enableAnalytics = true;
  static const bool enableCrashReporting = true;
}
