import 'dart:async';

/// Extension to add timeout support to Future operations
extension FutureTimeout<T> on Future<T> {
  /// Adds a timeout to any Future operation with a custom error message
  Future<T> withTimeout({
    Duration timeout = const Duration(seconds: 30),
    String? timeoutMessage,
  }) {
    return this.timeout(
      timeout,
      onTimeout: () {
        throw TimeoutException(
          timeoutMessage ?? 'Operation timed out after ${timeout.inSeconds} seconds',
          timeout,
        );
      },
    );
  }
}

/// Constants for timeout durations
class TimeoutDurations {
  static const Duration short = Duration(seconds: 10);
  static const Duration medium = Duration(seconds: 30);
  static const Duration long = Duration(seconds: 60);
  
  // Specific operation timeouts
  static const Duration auth = Duration(seconds: 20);
  static const Duration query = Duration(seconds: 30);
  static const Duration upload = Duration(seconds: 60);
  static const Duration payment = Duration(seconds: 45);
}
