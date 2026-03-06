import 'package:flutter/foundation.dart';

/// Simple logger that only logs in debug mode
/// Use this instead of print() to avoid logging sensitive data in production
class AppLogger {
  static void log(String message) {
    if (kDebugMode) {
      debugPrint(message);
    }
  }
  
  static void error(String message, [dynamic error, StackTrace? stackTrace]) {
    if (kDebugMode) {
      debugPrint('ERROR: $message');
      if (error != null) debugPrint('Error details: $error');
      if (stackTrace != null) debugPrint('Stack trace: $stackTrace');
    }
  }
  
  static void info(String message) {
    if (kDebugMode) {
      debugPrint('INFO: $message');
    }
  }
  
  static void warning(String message) {
    if (kDebugMode) {
      debugPrint('WARNING: $message');
    }
  }
}
