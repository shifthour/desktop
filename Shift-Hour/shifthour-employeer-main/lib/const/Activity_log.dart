import 'dart:convert';
import 'package:supabase_flutter/supabase_flutter.dart';

class ActivityLogger {
  static final _supabase = Supabase.instance.client;

  // Generic method to log an activity
  static Future<void> logActivity({
    required String activityType,
    required String title,
    required String description,
    Map<String, dynamic>? additionalData,
  }) async {
    try {
      // Get current user
      final user = _supabase.auth.currentUser;
      if (user == null) return;

      // Prepare additional data as JSONB
      final jsonbData =
          additionalData != null ? jsonEncode(additionalData) : null;

      // Call the PostgreSQL function
      await _supabase.rpc(
        'insert_recent_activity',
        params: {
          'p_user_id': user.id,
          'p_activity_type': activityType,
          'p_title': title,
          'p_description': description,
          'p_additional_data': jsonbData,
        },
      );
    } catch (e) {
      print('Error logging activity: $e');
    }
  }

  // Predefined methods for common activities
  static Future<void> logJobPosting(Map<String, dynamic> jobDetails) async {
    await logActivity(
      activityType: 'job_posted',
      title: 'New Shift Posted',
      description:
          'Posted ${jobDetails['job_title']} shift at ${jobDetails['company']}',
      additionalData: jobDetails,
    );
  }

  static Future<void> logWorkerAssignment(
    Map<String, dynamic> applicationDetails,
  ) async {
    await logActivity(
      activityType: 'worker_assigned',
      title: 'Worker Assigned',
      description:
          'Assigned ${applicationDetails['full_name']} to ${applicationDetails['job_title']}',
      additionalData: applicationDetails,
    );
  }

  static Future<void> logPaymentTransfer(
    Map<String, dynamic> activityDetails,
  ) async {
    try {
      // Validate required fields
      if (activityDetails['application_id'] == null ||
          activityDetails['worker_id'] == null ||
          activityDetails['employer_id'] == null) {
        print('Error: Missing required fields for payment transfer log');
        return;
      }

      // Prepare the log entry
      final logEntry = {
        'activity_type': 'payment_transfer',
        'application_id': activityDetails['application_id'],
        'shift_id': activityDetails['shift_id'],
        'worker_id': activityDetails['worker_id'],
        'employer_id': activityDetails['employer_id'],
        'amount': activityDetails['amount'] ?? 0.0,
        'job_title': activityDetails['job_title'] ?? 'Unnamed Job',
        'timestamp': DateTime.now().toIso8601String(),
        'additional_details': {
          // Add any additional context or metadata about the payment
          'payment_method': 'platform_transfer',
          'status': 'completed',
        },
      };

      // Insert the log entry into the activity log table
      await _supabase.from('activity_logs').insert(logEntry);

      print('Payment transfer logged successfully');
    } catch (e) {
      print('Error logging payment transfer: $e');
      // Optionally rethrow if you want calling code to handle the error
      rethrow;
    }
  }

  static Future<void> logWorkerCheckIn(
    Map<String, dynamic> checkInDetails,
  ) async {
    await logActivity(
      activityType: 'check_in',
      title: 'Worker Check-In',
      description:
          '${checkInDetails['full_name']} checked in for ${checkInDetails['job_title']}',
      additionalData: checkInDetails,
    );
  }

  static Future<void> logWorkerCheckOut(
    Map<String, dynamic> checkOutDetails,
  ) async {
    await logActivity(
      activityType: 'check_out',
      title: 'Worker Check-Out',
      description:
          '${checkOutDetails['full_name']} checked out from ${checkOutDetails['job_title']}',
      additionalData: checkOutDetails,
    );
  }

  // Added the method inside the class
  static Future<void> logShiftCancellation(
    Map<String, dynamic> application,
  ) async {
    await logActivity(
      activityType: 'shift_cancelled',
      title: 'Shift Cancellation',
      description: 'Cancelled ${application['job_title']} shift',
      additionalData: {
        'shift_id': application['shift_id'],
        'job_title': application['job_title'],
        'company': application['company'],
      },
    );
  }
}
