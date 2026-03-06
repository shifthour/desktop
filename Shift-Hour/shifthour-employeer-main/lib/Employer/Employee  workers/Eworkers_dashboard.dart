import 'dart:math';

import 'package:flutter/material.dart';
import 'package:get/get.dart';
import 'package:shifthour_employeer/Employer/employer_dashboard.dart';
import 'package:shifthour_employeer/const/Activity_log.dart';
import 'package:shifthour_employeer/const/Bottom_Navigation.dart';
import 'package:shifthour_employeer/const/Standard_Appbar.dart';
import 'package:shifthour_employeer/services/notification.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:intl/intl.dart';

class WorkerApplicationsController extends GetxController {
  // Reactive variables for state management

  final searchController = TextEditingController();
  final RxBool isStatusLoading = true.obs;
  final RxString statusFilter = "All".obs;
  final RxList<Map<String, dynamic>> applications =
      <Map<String, dynamic>>[].obs;
  final RxBool isLoading = true.obs;
  final RxString searchQuery = ''.obs;

  final supabase = Supabase.instance.client;
  final ScrollController _scrollController = ScrollController();
  ScrollController get scrollController => _scrollController;

  @override
  void onInit() {
    super.onInit();
    searchController.addListener(() {
      if (searchController.text != searchQuery.value) {
        searchQuery.value = searchController.text;
      }
    });
    fetchApplications().then((_) {
      // Fetch all statuses in one go after applications are loaded
      _preloadAllStatuses();
    });

    WidgetsBinding.instance.addPostFrameCallback((_) {
      // Get the navigation controller and set index without triggering navigation

      try {
        final navigationController = Get.find<NavigationController>();
        navigationController.setScrollController(_scrollController);
      } catch (e) {
        final navigationController = Get.put(
          NavigationController(),
          permanent: true,
        );
        navigationController.setScrollController(_scrollController);
      }
    });
  }

  @override
  void onClose() {
    searchController.dispose(); // Don't forget to dispose
    super.onClose();
  }

  List<Map<String, dynamic>> get filteredApplicationsByStatus {
    // First filter by search query
    final searchFiltered = filteredApplications;

    // If "All" is selected, return all applications filtered by search
    if (statusFilter.value == "All") {
      return searchFiltered;
    }

    // Otherwise, filter by status
    return searchFiltered.where((app) {
      final status = app['application_status']?.toString().toLowerCase() ?? '';
      return status == statusFilter.value.toLowerCase();
    }).toList();
  }

  void updateStatusFilter(String filter) {
    statusFilter.value = filter;
  }

  // Add these at the top of your WorkerApplicationsController class
  final RxMap<String, bool> checkInLoading = <String, bool>{}.obs;
  final RxMap<String, RxString> checkInStatuses = <String, RxString>{}.obs;
  Future<void> _preloadAllStatuses() async {
    try {
      isStatusLoading.value = true;

      // Skip if no applications
      if (applications.isEmpty) {
        isStatusLoading.value = false;
        return;
      }

      // Prepare a list of application IDs to batch fetch
      final appIds = applications.map((app) => app['id'].toString()).toList();

      // Query all statuses in one go
      final response = await supabase
          .from('worker_attendance')
          .select('application_id, status')
          .inFilter('application_id', appIds)
          .order('created_at', ascending: false);

      // Group by application_id, keeping only the most recent status
      final Map<String, String> latestStatuses = {};
      for (var record in response) {
        final appId = record['application_id'].toString();
        if (!latestStatuses.containsKey(appId)) {
          latestStatuses[appId] = record['status'];
        }
      }

      // Initialize all statuses at once
      for (var app in applications) {
        final appId = app['id'].toString();
        final status = latestStatuses[appId] ?? 'not_checked_in';

        if (!checkInStatuses.containsKey(appId)) {
          checkInStatuses[appId] = RxString(status);
        } else {
          checkInStatuses[appId]!.value = status;
        }
      }
    } catch (e) {
      print('Error preloading statuses: $e');
    } finally {
      isStatusLoading.value = false;
    }
  }

  // Complete implementation of checkIn method
  Future<void> checkIn(
    Map<String, dynamic> application,
    BuildContext context,
  ) async {
    final appId = application['id'].toString();

    try {
      final existingCheckIn = await _checkExistingCheckIn(application);

      if (existingCheckIn != null) {
        // For Check-Out flow, keep using loader inside dialog method
        await _showCheckOutDialog(context, application, existingCheckIn);
        await refreshApplications();
      } else {
        // ✅ Ask user for code
        final code = await _showCheckInCodeDialog(context, application);

        if (code != null && code.isNotEmpty && context.mounted) {
          // ✅ Full-screen loader
          showDialog(
            context: context,
            barrierDismissible: false,
            builder: (_) => const Center(child: CircularProgressIndicator()),
          );

          try {
            final isValid = await _verifyJobSeekerCode(
              application['full_name'],
              code,
            );

            if (!context.mounted) return;

            Navigator.of(context).pop(); // ❌ Close the loader here

            if (isValid) {
              await _performCheckIn(application);
              _showSuccessSnackbar(context, 'Check-in successful!');
            } else {
              _showErrorSnackbar(context, 'Invalid job seeker code');
            }
          } catch (e) {
            Navigator.of(context).pop(); // ❌ Close loader on failure too
            _showErrorSnackbar(context, 'Check-in error: ${e.toString()}');
          }
        }
      }
    } catch (e) {
      print('Error during check-in: $e');
      _showErrorSnackbar(context, 'Error during check-in: ${e.toString()}');
    }
  }

  // Complete implementation of getCheckInStatus
  Future<String> getCheckInStatus(Map<String, dynamic> application) async {
    final appId = application['id'].toString();

    // If we have a cached status that's not 'loading', return it immediately
    if (checkInStatuses.containsKey(appId) &&
        checkInStatuses[appId]!.value != 'loading') {
      return checkInStatuses[appId]!.value;
    }

    try {
      print('Checking Check-In Status for Application ID: $appId');

      final response =
          await supabase
              .from('worker_attendance')
              .select('status')
              .eq('application_id', appId)
              .order('created_at', ascending: false)
              .maybeSingle();

      print('Check-In Status Response: $response');

      // Get the status from response or default to not_checked_in
      final status = response?['status'] ?? 'not_checked_in';

      // Cache the result for future use
      if (!checkInStatuses.containsKey(appId)) {
        checkInStatuses[appId] = RxString(status);
      } else {
        checkInStatuses[appId]!.value = status;
      }

      return status;
    } catch (e) {
      print('Error checking check-in status: $e');

      // If there's a Supabase error, print more details
      if (e is PostgrestException) {
        print('Supabase Error Details:');
        print('Message: ${e.message}');
        print('Hint: ${e.hint}');
        print('Details: ${e.details}');
      }

      // Return cached status if available, otherwise default
      if (checkInStatuses.containsKey(appId)) {
        return checkInStatuses[appId]!.value;
      }
      return 'not_checked_in';
    }
  }

  DateTime _parseShiftDateTime(DateTime shiftDate, String startTimeStr) {
    int hour = 0;
    int minute = 0;

    // Clean up the time string
    startTimeStr = startTimeStr.trim().toUpperCase();

    if (startTimeStr.contains('AM') || startTimeStr.contains('PM')) {
      // Handle 12-hour format with AM/PM
      final parts = startTimeStr.split(' ');
      final timePart = parts[0];
      final amPm = parts[1];

      final timeSplit = timePart.split(':');
      hour = int.parse(timeSplit[0]);
      minute = int.parse(timeSplit[1]);

      // Convert to 24-hour format
      if (amPm == 'PM' && hour < 12) {
        hour += 12;
      } else if (amPm == 'AM' && hour == 12) {
        hour = 0;
      }
    } else {
      // Handle 24-hour format
      final timeSplit = startTimeStr.split(':');
      hour = int.parse(timeSplit[0]);
      minute = int.parse(timeSplit[1]);
    }

    // Create a DateTime with the shift date and parsed time
    return DateTime(
      shiftDate.year,
      shiftDate.month,
      shiftDate.day,
      hour,
      minute,
    );
  }

  Future<bool?> _showCancelConfirmationDialog(
    BuildContext context, {
    required Map application,
  }) {
    final payRate = (application['pay_rate'] as num?) ?? 0;
    final penaltyAmount = payRate * 0.5;

    // Get shift date to determine if within 24 hours
    DateTime? shiftDate;
    String? startTime;
    bool isWithin24Hours = false;

    try {
      if (application['date'] != null) {
        shiftDate = DateTime.parse(application['date'].toString());
        startTime = application['start_time'] ?? "00:00";

        // Parse the start time
        final shiftStartDateTime = _parseShiftDateTime(shiftDate, startTime!);

        // Get current time
        final now = DateTime.now();

        // Calculate time difference in hours
        final difference = shiftStartDateTime.difference(now).inHours;

        // If difference is less than 24 hours, apply penalty
        isWithin24Hours = difference < 24;
      }
    } catch (e) {
      print('Error calculating shift time difference in dialog: $e');
      // Default to no penalty if there's an error
      isWithin24Hours = false;
    }

    return showDialog<bool>(
      context: context,
      builder:
          (context) => Dialog(
            shape: RoundedRectangleBorder(
              borderRadius: BorderRadius.circular(16),
            ),
            elevation: 8,
            backgroundColor: Colors.white,
            child: Container(
              padding: const EdgeInsets.all(24),
              child: Column(
                mainAxisSize: MainAxisSize.min,
                children: [
                  // Warning Icon
                  Container(
                    width: 80,
                    height: 80,
                    decoration: BoxDecoration(
                      color: Colors.orange.shade50,
                      shape: BoxShape.circle,
                    ),
                    child: Icon(
                      Icons.warning_amber_rounded,
                      color: Colors.orange.shade600,
                      size: 48,
                    ),
                  ),
                  const SizedBox(height: 24),

                  // Title
                  Text(
                    'Shift Cancellation Notice',
                    style: TextStyle(
                      fontSize: 22,
                      fontWeight: FontWeight.bold,
                      color: Colors.grey.shade800,
                    ),
                    textAlign: TextAlign.center,
                  ),
                  const SizedBox(height: 16),

                  // Description with penalty info
                  Text(
                    isWithin24Hours
                        ? 'Cancelling this shift will deduct 50% (₹${penaltyAmount.toStringAsFixed(2)}) '
                            'from your employer wallet as the shift starts in less than 24 hours.'
                        : 'You are cancelling this shift more than 24 hours in advance. No penalty will be applied.',
                    style: TextStyle(fontSize: 16, color: Colors.grey.shade600),
                    textAlign: TextAlign.center,
                  ),
                  const SizedBox(height: 8),
                  Text(
                    'Do you want to proceed?',
                    style: TextStyle(
                      fontSize: 16,
                      color: Colors.grey.shade600,
                      fontWeight: FontWeight.bold,
                    ),
                    textAlign: TextAlign.center,
                  ),
                  const SizedBox(height: 24),

                  // Action Buttons
                  Row(
                    children: [
                      // Keep Shift
                      Expanded(
                        child: OutlinedButton(
                          onPressed: () => Navigator.of(context).pop(false),
                          style: OutlinedButton.styleFrom(
                            foregroundColor: Colors.grey.shade700,
                            side: BorderSide(color: Colors.grey.shade300),
                            padding: const EdgeInsets.symmetric(vertical: 12),
                            shape: RoundedRectangleBorder(
                              borderRadius: BorderRadius.circular(12),
                            ),
                          ),
                          child: const Text(
                            'No, Keep',
                            style: TextStyle(fontWeight: FontWeight.w600),
                          ),
                        ),
                      ),
                      const SizedBox(width: 16),

                      // Confirm Cancel
                      Expanded(
                        child: ElevatedButton(
                          onPressed: () => Navigator.of(context).pop(true),
                          style: ElevatedButton.styleFrom(
                            backgroundColor: Colors.red.shade600,
                            foregroundColor: Colors.white,
                            padding: const EdgeInsets.symmetric(vertical: 12),
                            shape: RoundedRectangleBorder(
                              borderRadius: BorderRadius.circular(12),
                            ),
                          ),
                          child: const Text(
                            'Yes, Cancel',
                            style: TextStyle(fontWeight: FontWeight.w600),
                          ),
                        ),
                      ),
                    ],
                  ),
                ],
              ),
            ),
          ),
    );
  }

  DateTime _parseTime(String timeString) {
    // Default to noon if parsing fails
    if (timeString == null || timeString.isEmpty) {
      return DateTime(2022, 1, 1, 12, 0);
    }

    try {
      // Handle time formats like "10:00 AM" or "14:00"
      timeString = timeString.trim().toUpperCase();

      int hour = 0;
      int minute = 0;

      if (timeString.contains('AM') || timeString.contains('PM')) {
        // Handle 12-hour format with AM/PM
        final parts = timeString.split(' ');
        final timePart = parts[0];
        final amPm = parts[1];

        final timeSplit = timePart.split(':');
        hour = int.parse(timeSplit[0]);
        minute = int.parse(timeSplit[1]);

        // Convert to 24-hour format
        if (amPm == 'PM' && hour < 12) {
          hour += 12;
        } else if (amPm == 'AM' && hour == 12) {
          hour = 0;
        }
      } else {
        // Handle 24-hour format
        final timeSplit = timeString.split(':');
        hour = int.parse(timeSplit[0]);
        minute = int.parse(timeSplit[1]);
      }

      return DateTime(2022, 1, 1, hour, minute);
    } catch (e) {
      print('Error parsing time: $e');
      return DateTime(2022, 1, 1, 12, 0); // Default to noon
    }
  }

  Future<void> _sendWorkerNotification({
    required String userId,
    required String title,
    required String message,
    required String shiftId,
    String userType = 'worker', // ✅ NEW
    String type = 'info',
    String notificationType = 'check_event',
    String androidChannelId = 'shifthour_general',
  }) async {
    final notificationId = NotificationService().generateUuid();
    final supabase = Supabase.instance.client;

    final body = {
      'user_id': userId,
      'notification_id': notificationId,
      'title': title,
      'message': message,
      'notification_type': notificationType,
      'type': type,
      'android_channel_id': androidChannelId,
      'additional_data': {'shift_id': shiftId},
    };

    // ✅ Save to DB
    try {
      await supabase.from('notifications').insert({
        'user_id': userId,
        'user_type': userType,
        'title': title,
        'message': message,
        'notification_type': notificationType,
        'type': type,
        'created_at': DateTime.now().toIso8601String(),
        'is_read': false,
      });

      print("📥 Notification stored in DB for $userId");
    } catch (e) {
      print("❌ Error saving notification to DB: $e");
    }

    // ✅ Send via Edge Function
    try {
      final response = await supabase.functions.invoke(
        'send-notification',
        body: body,
      );
      print("✅ Notification sent: ${response.data}");
    } catch (e) {
      print('❌ Error sending notification: $e');
    }
  }

  Future<void> sendCheckEventNotifications({
    required String workerUserId,
    required String employerUserId,
    required String shiftId,
    required bool isCheckIn,
  }) async {
    final isCheckInText = isCheckIn ? 'checked in' : 'checked out';
    final type = isCheckIn ? 'check_in' : 'check_out';

    final workerTitle =
        isCheckIn ? 'Check-In Successful' : 'Check-Out Completed';
    final workerMessage = 'You have $isCheckInText for Shift $shiftId';

    final employerTitle = 'Worker $isCheckInText';
    final employerMessage = 'A worker has $isCheckInText for Shift $shiftId';

    // Send to Worker
    // Send to Worker
    await _sendWorkerNotification(
      userId: workerUserId,
      title: workerTitle,
      message: workerMessage,
      notificationType: type,
      shiftId: shiftId,
      userType: 'worker', // ✅ Add this
    );

    // Send to Employer
    await _sendWorkerNotification(
      userId: employerUserId,
      title: employerTitle,
      message: employerMessage,
      notificationType: type,
      shiftId: shiftId,
      userType: 'employer', // ✅ Add this
    );
  }

  Future<void> _performCheckIn(Map<String, dynamic> application) async {
    final appId = application['id'].toString();
    final userId = Supabase.instance.client.auth.currentUser?.id;
    try {
      // First verify the worker exists in job_seekers table
      final workerId = application['worker_id'] ?? application['user_id'];
      await ActivityLogger.logWorkerCheckIn(application);
      final userId = Supabase.instance.client.auth.currentUser?.id;
      // Check if the worker exists in job_seekers table
      final workerExists =
          await supabase
              .from('job_seekers')
              .select('id')
              .eq('id', workerId)
              .maybeSingle();

      // Update application status to In Progress
      if (application['shift_id'] != null) {
        await supabase
            .from('worker_job_applications')
            .update({'application_status': 'In Progress'})
            .eq('employer_id', userId.toString())
            .eq('shift_id', application['shift_id'])
            .eq('id', application['id']);
      }

      // If worker doesn't exist in job_seekers, we need to handle this case
      if (workerExists == null) {
        print('Warning: Worker ID $workerId not found in job_seekers table');

        // Option 2: Use a different ID field like application ID
        final insertData = {
          //'id': userId,
          'worker_id': userId,
          'application_id': application['id'],
          'shift_id': application['shift_id'],
          // Use application ID instead of worker_id to avoid the foreign key issue
          // 'worker_id': workerId, // This is the problematic line
          'check_in_time': DateTime.now().toIso8601String(),
          'status': 'checked_in',
          'job_title': application['job_title'] ?? 'Unknown Job',
          'company': application['company'] ?? 'Unknown Company',
          'location': application['location'] ?? 'Unknown Location',
        };

        // Remove any null values
        insertData.removeWhere((key, value) => value == null);

        final response =
            await supabase
                .from('worker_attendance')
                .insert(insertData)
                .select();

        print('Check-In Insertion Response (without worker_id): $response');
      } else {
        // Worker exists in job_seekers table, proceed with normal check-in
        final insertData = {
          // 'id': userId,
          'worker_id': userId,
          'application_id': application['id'],
          'shift_id': application['shift_id'],
          'worker_id': workerId,
          'check_in_time': DateTime.now().toIso8601String(),
          'status': 'checked_in',
          'job_title': application['job_title'] ?? 'Unknown Job',
          'company': application['company'] ?? 'Unknown Company',
          'location': application['location'] ?? 'Unknown Location',
        };

        // Remove any null values
        insertData.removeWhere((key, value) => value == null);

        final response =
            await supabase
                .from('worker_attendance')
                .insert(insertData)
                .select();

        print('Check-In Insertion Response: $response');
      }
      await sendCheckEventNotifications(
        workerUserId: workerId.toString(),
        employerUserId: userId.toString(),
        shiftId: application['shift_id'].toString(),
        isCheckIn: true,
      );

      // After successful check-in, update the cached status immediately
      if (!checkInStatuses.containsKey(appId)) {
        checkInStatuses[appId] = RxString('checked_in');
      } else {
        checkInStatuses[appId]!.value = 'checked_in';
      }
    } catch (e) {
      print('Error performing check-in: $e');

      // If it's a Supabase error, print more details
      if (e is PostgrestException) {
        print('Supabase Error Details:');
        print('Message: ${e.message}');
        print('Hint: ${e.hint}');
        print('Details: ${e.details}');
      }

      rethrow;
    }
  } // Complete implementation of _performCheckOut

  Future<void> _performCheckOut(
    Map<String, dynamic>? checkInRecord,
    int onTimeRating,
    int performanceRating,
    String? feedback,
  ) async {
    try {
      // First, check if checkInRecord is null
      if (checkInRecord == null) {
        print('Error: checkInRecord is null');
        return;
      }

      // Check if id exists in the record
      if (checkInRecord['id'] == null) {
        print('Error: checkInRecord does not contain an id field');
        return;
      }

      // Get application ID and shift ID for status updating
      final applicationId = checkInRecord['application_id']?.toString();
      final shiftCustomId =
          checkInRecord['shift_id']?.toString(); // This is in format "SH-XXXX"

      // Create update data with proper null handling
      final updateData = {
        'check_out_time': DateTime.now().toIso8601String(),
        'status': 'checked_out',
        'on_time_rating': onTimeRating,
        'performance_rating': performanceRating,
      };

      // Only add feedback if it's not null or empty
      if (feedback != null && feedback.trim().isNotEmpty) {
        updateData['feedback'] = feedback;
      }

      // Initialize variables to store worker information
      String? workerEmail;
      String? workerName;
      String? shiftUuid;

      // Step 1: Get the UUID that corresponds to the shift's custom ID from shift_id_mapping table
      if (shiftCustomId != null) {
        try {
          print('Looking up UUID for shift ID: $shiftCustomId');
          final shiftMapResponse =
              await supabase
                  .from('shift_id_mapping')
                  .select('uuid')
                  .eq('custom_id', shiftCustomId)
                  .maybeSingle();

          if (shiftMapResponse != null && shiftMapResponse['uuid'] != null) {
            shiftUuid = shiftMapResponse['uuid'].toString();
            print('Found shift UUID: $shiftUuid for custom ID: $shiftCustomId');
          } else {
            print('No UUID found for shift ID: $shiftCustomId');
          }
        } catch (e) {
          print('Error looking up shift UUID: $e');
        }
      }

      // Step 2: Fetch worker details from the application record
      if (applicationId != null) {
        print('Fetching worker details from application record...');
        try {
          final applicationData =
              await supabase
                  .from('worker_job_applications')
                  .select('full_name, email')
                  .eq('id', applicationId)
                  .single();

          if (applicationData != null) {
            workerName = applicationData['full_name'];
            workerEmail = applicationData['email'];
            print(
              'Found worker details - Name: $workerName, Email: $workerEmail',
            );
          }
        } catch (e) {
          print('Error fetching application data: $e');
        }
      }

      // Log activity with enriched data
      if (checkInRecord != null) {
        Map<String, dynamic> logData = {...checkInRecord};
        if (workerName != null) {
          logData['full_name'] = workerName;
        }
        await ActivityLogger.logWorkerCheckOut(logData);
      }
      final userId = Supabase.instance.client.auth.currentUser?.id;
      // Update attendance record

      print(
        '➡️ Updating attendance record ID: ${checkInRecord['id']} with data: $updateData',
      );

      final updateResponse =
          await supabase
              .from('worker_attendance')
              .update(updateData)
              .eq('id', checkInRecord['id'])
              .select();

      print('✅ Update response: $updateResponse');

      await sendCheckEventNotifications(
        workerUserId: checkInRecord['worker_id'].toString(),
        employerUserId: userId.toString(),
        shiftId: checkInRecord['shift_id'].toString(),
        isCheckIn: false,
      );

      // Update worker job application status to Completed
      if (shiftCustomId != null) {
        await supabase
            .from('worker_job_applications')
            .update({'application_status': 'Completed'})
            .eq('shift_id', shiftCustomId)
            .eq('employer_id', userId.toString());
      }

      double shiftAmount = 0.0;
      bool isShiftHoldUpdated = false;

      // Update the shift_holds table with worker_email and batch_id
      if (shiftUuid != null && workerEmail != null) {
        try {
          // Generate a batch ID with timestamp
          final batchId = 'batch_${DateTime.now().millisecondsSinceEpoch}';

          print('Updating shift_holds with UUID: $shiftUuid');
          print('Setting worker_email to: $workerEmail');

          // First get the shift hold to determine the amount
          final shiftHoldData =
              await supabase
                  .from('shift_holds')
                  .select('*')
                  .eq('id', shiftUuid)
                  .maybeSingle();

          if (shiftHoldData != null && shiftHoldData['amount'] != null) {
            // Get the shift amount
            if (shiftHoldData['amount'] is int) {
              shiftAmount = shiftHoldData['amount'].toDouble();
            } else if (shiftHoldData['amount'] is double) {
              shiftAmount = shiftHoldData['amount'];
            } else if (shiftHoldData['amount'] is String) {
              shiftAmount = double.tryParse(shiftHoldData['amount']) ?? 0.0;
            }

            print('Found shift amount: $shiftAmount');
          } else {
            // Try finding by shift_id if not found by id
            final shiftHoldByShiftId =
                await supabase
                    .from('shift_holds')
                    .select('*')
                    .eq('shift_id', shiftUuid)
                    .maybeSingle();

            if (shiftHoldByShiftId != null &&
                shiftHoldByShiftId['amount'] != null) {
              if (shiftHoldByShiftId['amount'] is int) {
                shiftAmount = shiftHoldByShiftId['amount'].toDouble();
              } else if (shiftHoldByShiftId['amount'] is double) {
                shiftAmount = shiftHoldByShiftId['amount'];
              } else if (shiftHoldByShiftId['amount'] is String) {
                shiftAmount =
                    double.tryParse(shiftHoldByShiftId['amount']) ?? 0.0;
              }

              print('Found shift amount by shift_id: $shiftAmount');
            }
          }

          // Update shift_holds with worker_email
          final response =
              await supabase
                  .from('shift_holds')
                  .update({'worker_email': workerEmail, 'batch_id': batchId})
                  .eq('id', shiftUuid)
                  .select();

          print('Shift holds update response: $response');

          // If update was successful with ID
          if (response != null && response.isNotEmpty) {
            isShiftHoldUpdated = true;
          } else {
            // Try using shift_id instead
            print('No records updated by ID, trying shift_id column...');

            final responseAlt =
                await supabase
                    .from('shift_holds')
                    .update({'worker_email': workerEmail, 'batch_id': batchId})
                    .eq('shift_id', shiftUuid)
                    .select();

            print('Shift holds update response (by shift_id): $responseAlt');

            if (responseAlt != null && responseAlt.isNotEmpty) {
              isShiftHoldUpdated = true;
            } else {
              // Additional fallback methods (existing code)
              final allHolds = await supabase
                  .from('shift_holds')
                  .select('*')
                  .limit(20);

              // Last resort: try to find any column that matches our shift's value
              if (allHolds != null && allHolds.isNotEmpty) {
                for (final hold in allHolds) {
                  bool foundMatch = false;

                  // Check each field in the record
                  hold.forEach((key, value) {
                    if (value != null && value.toString() == shiftUuid) {
                      print('Found match on column $key = $value');
                      foundMatch = true;
                    }
                  });

                  // If we found a matching value, try to update by ID
                  if (foundMatch && hold['id'] != null) {
                    if (hold['amount'] != null) {
                      if (hold['amount'] is int) {
                        shiftAmount = hold['amount'].toDouble();
                      } else if (hold['amount'] is double) {
                        shiftAmount = hold['amount'];
                      } else if (hold['amount'] is String) {
                        shiftAmount = double.tryParse(hold['amount']) ?? 0.0;
                      }
                    }

                    final matchUpdate =
                        await supabase
                            .from('shift_holds')
                            .update({
                              'worker_email': workerEmail,
                              'batch_id': batchId,
                            })
                            .eq('id', hold['id'])
                            .select();

                    print('Update result for matching record: $matchUpdate');

                    if (matchUpdate != null && matchUpdate.isNotEmpty) {
                      isShiftHoldUpdated = true;
                    }
                  }
                }
              }
            }
          }
        } catch (e) {
          print('Error updating shift_holds: $e');

          // If it's a Supabase error, print more details
          if (e is PostgrestException) {
            print('Supabase Error Details:');
            print('Message: ${e.message}');
            print('Hint: ${e.hint}');
            print('Details: ${e.details}');
          }
        }
      } else {
        print('Cannot update shift_holds: missing shift UUID or worker email');
        if (shiftUuid == null) print('Shift UUID is null');
        if (workerEmail == null) print('Worker email is null');
      }

      // Process payment to worker wallet - 90% of shift amount
      if (isShiftHoldUpdated && workerEmail != null && shiftAmount > 0) {
        try {
          // Calculate 90% of the shift amount
          final workerAmount = shiftAmount * 0.9;
          print('Processing payment to worker wallet: $workerAmount');

          // Check if worker has a wallet
          final workerWallet =
              await supabase
                  .from('worker_wallet')
                  .select()
                  .eq('worker_email', workerEmail)
                  .maybeSingle();

          if (workerWallet != null) {
            // Worker wallet exists, update balance
            print('Updating existing worker wallet');

            // Get current balance
            double currentBalance = 0;
            if (workerWallet['balance'] != null) {
              if (workerWallet['balance'] is int) {
                currentBalance = workerWallet['balance'].toDouble();
              } else if (workerWallet['balance'] is double) {
                currentBalance = workerWallet['balance'];
              } else if (workerWallet['balance'] is String) {
                currentBalance =
                    double.tryParse(workerWallet['balance']) ?? 0.0;
              }
            }

            // Add payment to balance
            final newBalance = currentBalance + workerAmount;

            // Update wallet
            final walletUpdateResponse =
                await supabase
                    .from('worker_wallet')
                    .update({
                      'balance': newBalance,
                      'last_updated': DateTime.now().toIso8601String(),
                    })
                    .eq('id', workerWallet['id'])
                    .select();

            print('Wallet update response: $walletUpdateResponse');

            // Create transaction record
            await _createWalletTransaction(
              workerWallet['id'],
              workerAmount,
              'earning',
              'Payment for shift: $shiftCustomId',
            );
          } else {
            // Worker wallet doesn't exist, create one
            print('Creating new worker wallet');

            // Get worker_id from auth database
            String? workerId;
            try {
              final workerUserData =
                  await supabase
                      .from('job_seekers')
                      .select('id')
                      .eq('email', workerEmail)
                      .maybeSingle();

              if (workerUserData != null) {
                workerId = workerUserData['id'];
              }
            } catch (e) {
              print('Error getting worker ID: $e');
            }

            // Create wallet with initial balance
            final newWalletResponse =
                await supabase.from('worker_wallet').insert({
                  'worker_id': workerId,
                  'worker_email': workerEmail,
                  'balance': workerAmount,
                  'currency': 'INR',
                  'last_updated': DateTime.now().toIso8601String(),
                }).select();

            print('New wallet creation response: $newWalletResponse');

            // Create transaction record if wallet was created
            if (newWalletResponse != null && newWalletResponse.isNotEmpty) {
              await _createWalletTransaction(
                newWalletResponse[0]['id'],
                workerAmount,
                'earning',
                'Payment for shift: $shiftCustomId',
              );
            }
          }
        } catch (e) {
          print('Error processing payment to worker wallet: $e');

          // If it's a Supabase error, print more details
          if (e is PostgrestException) {
            print('Supabase Error Details:');
            print('Message: ${e.message}');
            print('Hint: ${e.hint}');
            print('Details: ${e.details}');
          }
        }
      }

      // After successful check-out, update the cached status
      if (applicationId != null) {
        if (!checkInStatuses.containsKey(applicationId)) {
          checkInStatuses[applicationId] = RxString('checked_out');
        } else {
          checkInStatuses[applicationId]!.value = 'checked_out';
        }
      }
    } catch (e) {
      print('Error performing check-out: $e');

      // If it's a Supabase error, print more details
      if (e is PostgrestException) {
        print('Supabase Error Details:');
        print('Message: ${e.message}');
        print('Hint: ${e.hint}');
        print('Details: ${e.details}');
      }

      rethrow;
    }
  }

  Future<bool> cancelShift(Map application, BuildContext context) async {
    try {
      // Show confirmation dialog
      final shouldCancel = await _showCancelConfirmationDialog(
        context,
        application: application,
      );
      if (shouldCancel != true) return false;

      final applicationId = application['id'];
      final shiftId = application['shift_id'];
      final employerId = application['user_id']; // from job listing
      final workerId = application['user_id']; // from application

      if (applicationId == null || shiftId == null || workerId == null) {
        throw Exception('Invalid application, shift, or worker ID');
      }

      // Show loading
      Get.dialog(
        const Center(child: CircularProgressIndicator()),
        barrierDismissible: false,
      );

      try {
        await ActivityLogger.logShiftCancellation(
          application as Map<String, dynamic>,
        );
      } catch (logError) {
        print('Error logging shift cancellation: $logError');
        // Continue execution
      }

      try {
        // Check for attendance
        print('Checking attendance for application: $applicationId');
        final attendanceResponse = await supabase
            .from('worker_attendance')
            .select('id')
            .eq('application_id', applicationId);

        if (attendanceResponse != null && attendanceResponse.isNotEmpty) {
          Get.back();
          Get.dialog(
            AlertDialog(
              title: Text('Cannot Cancel Shift'),
              content: Text(
                'This shift has already been checked in or is in progress. You cannot cancel it now.',
              ),
              actions: [
                TextButton(onPressed: () => Get.back(), child: Text('OK')),
              ],
            ),
          );
          return false;
        }
      } catch (attendanceError) {
        print('Error checking attendance: $attendanceError');
        // Continue execution
      }

      Map<String, dynamic>? shiftResponse;
      try {
        // Fetch full shift details before deleting
        print('Fetching shift details for ID: $applicationId');
        shiftResponse =
            await supabase
                .from('worker_job_applications')
                .select(
                  '*, worker_job_listings (supervisor_name, user_id, shift_id, supervisor_email, supervisor_phone, job_pincode, category, contact_name, contact_email)',
                )
                .eq('id', applicationId) // using applicationId directly
                .maybeSingle();

        if (shiftResponse == null) {
          Get.back();
          Get.dialog(
            AlertDialog(
              title: Text('Error'),
              content: Text('Shift not found for cancellation.'),
              actions: [
                TextButton(onPressed: () => Get.back(), child: Text('OK')),
              ],
            ),
          );
          return false;
        }
        print('Shift details found: ${shiftResponse['shift_id']}');
      } catch (shiftFetchError) {
        print('Error fetching shift details: $shiftFetchError');
        Get.back();
        Get.snackbar(
          'Error',
          'Failed to fetch shift details: ${shiftFetchError.toString()}',
          snackPosition: SnackPosition.BOTTOM,
          backgroundColor: Colors.red,
          colorText: Colors.white,
        );
        return false;
      }

      // Calculate if cancellation is within 24 hours of shift start
      bool isWithin24Hours = false;
      try {
        // Parse shift date and time
        final shiftDate = DateTime.parse(shiftResponse['date']);
        String startTime = shiftResponse['start_time'] ?? "00:00";

        // Parse the start time (handling various formats)
        final DateTime shiftStartDateTime = _parseShiftDateTime(
          shiftDate,
          startTime,
        );

        // Get current time
        final now = DateTime.now();

        // Calculate time difference in hours
        final difference = shiftStartDateTime.difference(now).inHours;

        // If difference is less than 24 hours, apply penalty
        isWithin24Hours = difference < 24;

        print('Shift start time: $shiftStartDateTime');
        print('Current time: $now');
        print('Hours until shift: $difference');
        print('Is within 24 hours: $isWithin24Hours');
      } catch (timeCalcError) {
        print('Error calculating shift time difference: $timeCalcError');
        // Default to no penalty if there's an error in date parsing
        isWithin24Hours = false;
      }

      print(
        'Debug - applicationId: $applicationId, shiftId: $shiftId, workerId: $workerId',
      );

      try {
        // Insert into cancelled_shifts
        print('Inserting into cancelled_shifts table...');
        final cancelledShiftData = {
          'shift_id': shiftResponse['shift_id'],
          'user_id': shiftResponse['user_id'],
          'job_title': shiftResponse['job_title'],
          'category': shiftResponse['worker_job_listings']?['category'],
          'company': shiftResponse['company'],
          'location': shiftResponse['location'],
          'job_pincode': shiftResponse['worker_job_listings']?['job_pincode'],
          'date': shiftResponse['date'],
          'start_time': shiftResponse['start_time'],
          'end_time': shiftResponse['end_time'],
          'pay_rate': shiftResponse['pay_rate'],
          'status': shiftResponse['application_status'],
          'check_in_time': shiftResponse['check_in_time'],
          'check_out_time': shiftResponse['check_out_time'],
          'shift_created_at': shiftResponse['created_at'],
          'employer_name':
              shiftResponse['worker_job_listings']?['contact_name'] ?? '',
          'employer_email':
              shiftResponse['worker_job_listings']?['contact_email'] ?? '',
          'supervisor_name':
              shiftResponse['worker_job_listings']?['supervisor_name'],
          'supervisor_phone':
              shiftResponse['worker_job_listings']?['supervisor_phone'],
          'supervisor_email':
              shiftResponse['worker_job_listings']?['supervisor_email'],
          'worker_name': shiftResponse['full_name'] ?? '',
          'worker_email': shiftResponse['email'] ?? '',
          'cancelled_by': 'employer',
          'cancelled_person_name':
              shiftResponse['worker_job_listings']?['contact_name'] ?? '',
          'remarks': isWithin24Hours ? 'Charge' : 'No Charge',
        };

        // Remove null values
        cancelledShiftData.removeWhere((key, value) => value == null);

        print('Cancellation data to insert: $cancelledShiftData');
        await supabase.from('cancelled_shifts').insert(cancelledShiftData);
        print('Successfully inserted into cancelled_shifts');
      } catch (insertError) {
        print('Error inserting into cancelled_shifts: $insertError');
        Get.back();
        Get.snackbar(
          'Error',
          'Failed to record cancellation: ${insertError.toString()}',
          snackPosition: SnackPosition.BOTTOM,
          backgroundColor: Colors.red,
          colorText: Colors.white,
        );
        return false;
      }

      // Handle refund and compensation based on cancellation time
      if (isWithin24Hours) {
        try {
          // Penalty calculations
          final payRate = (application['pay_rate'] as num?) ?? 0;
          final refundAmount = payRate * 0.5; // Refund 50% of the amount
          final workerCredit =
              payRate * 0.25; // Worker still gets 25% compensation
          print(
            'Cancellation calculation - Pay rate: $payRate, Refund to employer: $refundAmount, Worker credit: $workerCredit',
          );

          // Process employer refund - UPDATED LOGIC
          if (employerId != null) {
            try {
              final employerWallet =
                  await supabase
                      .from('employer_wallet')
                      .select()
                      .eq('employer_id', employerId)
                      .maybeSingle();

              if (employerWallet != null) {
                final currentBalance = employerWallet['balance'] as num? ?? 0;

                // Add 50% back to the employer's wallet instead of deducting
                await supabase
                    .from('employer_wallet')
                    .update({
                      'balance':
                          currentBalance + refundAmount, // REFUND 50% back
                      'last_updated': DateTime.now().toIso8601String(),
                    })
                    .eq('id', employerWallet['id']);

                // Log wallet transaction - UPDATED to use wallet_transactions table
                await supabase.from('wallet_transactions').insert({
                  'wallet_id': employerWallet['id'],
                  'amount': refundAmount,
                  'transaction_type': 'refund',
                  'description':
                      'Partial refund (50%) for cancelled shift: ${shiftResponse['shift_id']}',
                  'created_at': DateTime.now().toIso8601String(),
                });
                print('Employer partial refund applied successfully');
              } else {
                print('No employer wallet found for ID: $employerId');
              }
            } catch (employerRefundError) {
              print('Error applying employer refund: $employerRefundError');
              // Continue execution
            }
          }

          // Process worker compensation
          try {
            final workerEmail = shiftResponse['email'];
            if (workerEmail != null) {
              print('Processing worker compensation for email: $workerEmail');
              var workerWallet =
                  await supabase
                      .from('worker_wallet')
                      .select()
                      .eq('worker_email', workerEmail)
                      .maybeSingle();

              if (workerWallet != null) {
                final currentBalance = workerWallet['balance'] as num? ?? 0;
                final walletId = workerWallet['id'];

                // First update the wallet balance
                await supabase
                    .from('worker_wallet')
                    .update({
                      'balance': currentBalance + workerCredit,
                      'last_updated': DateTime.now().toIso8601String(),
                    })
                    .eq('id', walletId);

                // Create transaction data with simpler fields first
                final transactionData = {
                  'wallet_id': walletId,
                  'amount': workerCredit,
                  'transaction_type': 'compensation',
                  'description': 'Compensation for cancelled shift',
                  'status': 'completed',
                  'created_at': DateTime.now().toIso8601String(),
                };

                // Add shift_id as text (since column is text type)
                if (shiftResponse['shift_id'] != null) {
                  transactionData['shift_id'] =
                      shiftResponse['shift_id'].toString();
                }

                // Print the transaction data we're about to insert
                print(
                  'Attempting to insert transaction with data: $transactionData',
                );

                // Try inserting with just the essential fields first
                try {
                  await supabase
                      .from('worker_wallet_transactions')
                      .insert(transactionData);
                  print(
                    'Worker compensation transaction recorded successfully',
                  );
                } catch (insertError) {
                  print('Error inserting transaction: $insertError');

                  // If first attempt failed, try with minimal fields
                  try {
                    print('Trying minimal transaction insert');
                    await supabase.from('worker_wallet_transactions').insert({
                      'wallet_id': walletId,
                      'amount': workerCredit,
                      'transaction_type': 'compensation',
                      'status': 'completed',
                    });
                    print('Minimal transaction recorded successfully');
                  } catch (minimalError) {
                    print(
                      'Even minimal transaction insert failed: $minimalError',
                    );
                  }
                }

                print('Worker compensation applied to existing wallet');
              } else {
                // If no wallet exists, create a new one
                print('Creating new worker wallet for email: $workerEmail');
                final newWallet =
                    await supabase.from('worker_wallet').insert({
                      'worker_email': workerEmail,
                      'balance': workerCredit,
                      'currency': 'INR',
                      'last_updated': DateTime.now().toIso8601String(),
                    }).select();

                // If wallet was created successfully, also create the transaction record
                if (newWallet != null && newWallet.isNotEmpty) {
                  final walletId = newWallet[0]['id'];

                  // Create minimal transaction data
                  final transactionData = {
                    'wallet_id': walletId,
                    'amount': workerCredit,
                    'transaction_type': 'compensation',
                    'status': 'completed',
                  };

                  // Try inserting with minimal fields
                  try {
                    await supabase
                        .from('worker_wallet_transactions')
                        .insert(transactionData);
                    print('Transaction recorded for new wallet');
                  } catch (insertError) {
                    print(
                      'Error inserting transaction for new wallet: $insertError',
                    );
                  }
                }
              }
            } else {
              print('No worker email found in shift data');
            }
          } catch (workerCompError) {
            print('Error processing worker compensation: $workerCompError');
            // Continue execution
          }
        } catch (penaltyError) {
          print('Error in refund/compensation processing: $penaltyError');
          // Continue execution
        }
      } else {
        // For cancellations more than 24 hours before the shift
        // Refund 100% to the employer
        try {
          if (employerId != null) {
            final payRate = (application['pay_rate'] as num?) ?? 0;
            final fullRefundAmount = payRate; // Full refund

            final employerWallet =
                await supabase
                    .from('employer_wallet')
                    .select()
                    .eq('employer_id', employerId)
                    .maybeSingle();

            if (employerWallet != null) {
              final currentBalance = employerWallet['balance'] as num? ?? 0;

              // Add full amount back to employer wallet
              await supabase
                  .from('employer_wallet')
                  .update({
                    'balance': currentBalance + fullRefundAmount,
                    'last_updated': DateTime.now().toIso8601String(),
                  })
                  .eq('id', employerWallet['id']);

              // Log wallet transaction - UPDATED to use wallet_transactions table
              await supabase.from('wallet_transactions').insert({
                'wallet_id': employerWallet['id'],
                'amount': fullRefundAmount,
                'transaction_type': 'refund',
                'description':
                    'Full refund for cancelled shift: ${shiftResponse['shift_id']}',
                'created_at': DateTime.now().toIso8601String(),
              });
              print('Employer full refund applied successfully');
            }
          }
        } catch (refundError) {
          print('Error processing full refund: $refundError');
          // Continue execution
        }
      }
      final userId = Supabase.instance.client.auth.currentUser?.id;
      // Delete from both tables
      try {
        // Delete the job application record
        print(
          'Deleting from worker_saved_jobs with job_id linked to shift_id: $shiftId',
        );
        await supabase
            .from('worker_saved_jobs')
            .delete()
            .eq('shift_id', shiftId);
        print('Successfully deleted saved job references');

        // Delete the job application record
        print(
          'Deleting job application with shift_id: $shiftId and id: $applicationId',
        );

        print(
          'Deleting job application with shift_id: $shiftId and id: $applicationId',
        );
        await supabase
            .from('worker_job_applications')
            .delete()
            .eq('shift_id', shiftId)
            .eq('employer_id', userId.toString())
            .eq('id', applicationId);
        print('Successfully deleted job application');
        final shiftIdForNotification =
            shiftResponse['shift_id']?.toString() ?? '';
        print(
          '📤 Sending cancel notifications with shift_id: $shiftIdForNotification',
        );

        // Notify Worker
        final workerUserId = shiftResponse['user_id'];
        if (workerUserId != null && workerUserId is String) {
          await _sendWorkerNotification(
            userId: workerUserId,
            title: 'Shift Cancelled',
            message:
                'Your shift (ID: $shiftIdForNotification) scheduled on ${shiftResponse['date']} has been cancelled.',
            notificationType: 'shift_cancelled',
            shiftId: shiftIdForNotification,
            userType: 'worker', // ✅ Add this
          );
        } else {
          print('❌ Worker user_id is null or invalid: $workerUserId');
        }

        // Notify Employer
        final employerUserId = shiftResponse['worker_job_listings']?['user_id'];
        if (employerUserId != null && employerUserId is String) {
          await _sendWorkerNotification(
            userId: employerUserId,
            title: 'Shift Cancelled',
            message:
                'You cancelled the shift (ID: $shiftIdForNotification) scheduled for ${shiftResponse['date']}.',
            notificationType: 'shift_cancelled',
            shiftId: shiftIdForNotification,
            userType: 'employer', // ✅ Add this
          );
        } else {
          print("❌ Employer user_id is null or invalid: $employerUserId");
        }

        // Delete the listing from worker_job_listings
        print('Deleting job listing with shift_id: $shiftId');
        await supabase
            .from('worker_job_listings')
            .delete()
            .eq('shift_id', shiftId);
        print('Successfully deleted job listing');
      } catch (deleteError) {
        print('Error deleting records: $deleteError');
        Get.back();
        Get.snackbar(
          'Partial Success',
          'Shift was cancelled and refund processed, but deletion failed: ${deleteError.toString()}',
          snackPosition: SnackPosition.BOTTOM,
          backgroundColor: Colors.orange,
          colorText: Colors.white,
        );
        await refreshApplications();

        return true; // Return true as cancellation was recorded
      }

      Get.back(); // Close loading
      await refreshApplications(); // Refresh UI
      // Notify Worker

      // Show different success message based on whether penalty was applied
      Get.snackbar(
        'Shift Cancelled',
        isWithin24Hours
            ? 'Shift has been cancelled. You have been refunded 50% of the shift amount.'
            : 'Shift has been cancelled. You have been refunded the full amount.',
        snackPosition: SnackPosition.BOTTOM,
        backgroundColor: Colors.green,
        colorText: Colors.white,
        duration: Duration(seconds: 3),
      );

      return true;
    } catch (e) {
      if (Get.isDialogOpen == true) Get.back();
      print('Error cancelling shift: $e');
      Get.snackbar(
        'Error',
        'Cancellation failed: ${e.toString()}',
        snackPosition: SnackPosition.BOTTOM,
        backgroundColor: Colors.red,
        colorText: Colors.white,
      );
      return false;
    }
  }

  Future<void> _createWalletTransaction(
    String walletId,
    double amount,
    String transactionType,
    String description,
  ) async {
    try {
      print(
        'Creating wallet transaction - wallet_id: $walletId, amount: $amount, type: $transactionType',
      );

      // Create transaction data
      final transactionData = {
        'wallet_id': walletId,
        'amount': amount,
        'transaction_type': transactionType,
        'description': description,
        'status': 'completed', // Required field
        'created_at': DateTime.now().toIso8601String(),
      };

      // Add the transaction record
      final response = await supabase
          .from('worker_wallet_transactions')
          .insert(transactionData);

      print('Transaction created successfully');
    } catch (e) {
      print('Error creating wallet transaction: $e');

      // If it's a Supabase error, print more details
      if (e is PostgrestException) {
        print('Supabase Error Details:');
        print('Message: ${e.message}');
        print('Hint: ${e.hint}');
        print('Details: ${e.details}');
      }

      // Try with minimal fields as fallback
      try {
        print('Attempting minimal transaction insert');
        await supabase.from('worker_wallet_transactions').insert({
          'wallet_id': walletId,
          'amount': amount,
          'transaction_type': transactionType,
          'status': 'completed',
        });
        print('Minimal transaction created successfully');
      } catch (fallbackError) {
        print('Even minimal transaction insert failed: $fallbackError');
      }
    }
  }

  Widget _buildCheckButton(
    Map<String, dynamic> application,
    WorkerApplicationsController controller,
    BuildContext context,
  ) {
    final appId = application['id'].toString();
    final checkStatus =
        controller.checkInStatuses[appId]?.value ?? 'not_checked_in';
    final isLoading = controller.checkInLoading[appId] == true;
    final appStatus =
        application['application_status']?.toString().toLowerCase();

    if (appStatus == 'completed') return SizedBox.shrink();

    // 🔁 Loader shown for both In Progress & Upcoming
    if (isLoading) {
      return ElevatedButton(
        onPressed: null,
        style: ElevatedButton.styleFrom(
          minimumSize: const Size(double.infinity, 48),
          backgroundColor: const Color(0xFF3461FD),
          shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(8)),
        ),
        child: Row(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            SizedBox(
              width: 16,
              height: 16,
              child: CircularProgressIndicator(
                strokeWidth: 2,
                valueColor: AlwaysStoppedAnimation<Color>(Colors.white),
              ),
            ),
            SizedBox(width: 8),
            Text('Processing...'),
          ],
        ),
      );
    }

    if (appStatus == 'in progress') {
      return ElevatedButton(
        onPressed: () async {
          controller.checkInLoading[appId] = true; // ✅ trigger loader
          await controller.checkIn(application, context);
          await controller.refreshApplications();
          controller.checkInLoading[appId] = false; // ✅ turn off loader
        },
        style: ElevatedButton.styleFrom(
          minimumSize: const Size(double.infinity, 48),
          backgroundColor: Colors.red,
          foregroundColor: Colors.white,
          shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(8)),
        ),
        child: Text('Check Out'),
      );
    }

    if (appStatus == 'upcoming') {
      return Row(
        children: [
          Flexible(
            flex: 1,
            child: SizedBox(
              width: 130,
              child: OutlinedButton(
                onPressed: () {
                  controller.cancelShift(application, context);
                },
                style: OutlinedButton.styleFrom(
                  minimumSize: const Size(0, 48),
                  foregroundColor: Colors.red,
                  side: BorderSide(color: Colors.red.shade200),
                  shape: RoundedRectangleBorder(
                    borderRadius: BorderRadius.circular(8),
                  ),
                ),
                child: FittedBox(
                  fit: BoxFit.scaleDown,
                  child: Text('Cancel Shift'),
                ),
              ),
            ),
          ),
          const SizedBox(width: 12),
          Obx(() {
            final isLoading = controller.checkInLoading[appId] ?? false;

            return isLoading
                ? ElevatedButton(
                  onPressed: null,
                  style: ElevatedButton.styleFrom(
                    backgroundColor: const Color(0xFF3461FD),
                    minimumSize: const Size(double.infinity, 48),
                    shape: RoundedRectangleBorder(
                      borderRadius: BorderRadius.circular(8),
                    ),
                  ),
                  child: Row(
                    mainAxisAlignment: MainAxisAlignment.center,
                    children: const [
                      SizedBox(
                        width: 20,
                        height: 20,
                        child: CircularProgressIndicator(
                          strokeWidth: 2,
                          valueColor: AlwaysStoppedAnimation<Color>(
                            Colors.white,
                          ),
                        ),
                      ),
                      SizedBox(width: 8),
                      Text('Processing...'),
                    ],
                  ),
                )
                : ElevatedButton(
                  onPressed: () async {
                    await controller.checkIn(application, context);
                    controller.checkInLoading[appId] = false;
                  },
                  style: ElevatedButton.styleFrom(
                    backgroundColor: const Color(0xFF3461FD),
                    minimumSize: const Size(double.infinity, 48),
                    shape: RoundedRectangleBorder(
                      borderRadius: BorderRadius.circular(8),
                    ),
                  ),
                  child: const Text('Check In'),
                );
          }),
        ],
      );
    }

    return SizedBox.shrink();
  }

  bool isUpcomingShift(Map<String, dynamic> application) {
    try {
      // Get current date and time
      final now = DateTime.now();
      final today = DateTime(now.year, now.month, now.day);

      // Parse shift date from the application
      DateTime? shiftDate;
      if (application['date'] != null) {
        shiftDate = DateTime.parse(application['date'].toString());
      } else if (application['shift_date'] != null) {
        shiftDate = DateTime.parse(application['shift_date'].toString());
      }

      // If no date is available, we can't determine if it's today
      if (shiftDate == null) {
        return false;
      }

      // Convert shift date to just date component (no time)
      final shiftDay = DateTime(shiftDate.year, shiftDate.month, shiftDate.day);

      // Check if shift is scheduled for today (exact date match)
      if (shiftDay.isAtSameMomentAs(today)) {
        // For today's shifts, check if they're still in the future
        DateTime shiftTime;
        if (application['start_time'] != null) {
          shiftTime = _parseTime(application['start_time'].toString());
        } else if (application['shift_start_time'] != null) {
          shiftTime = _parseTime(application['shift_start_time'].toString());
        } else {
          // Default to start of day if no time specified
          shiftTime = DateTime(2022, 1, 1, 0, 0);
        }

        // Combine date and time
        final shiftDateTime = DateTime(
          shiftDate.year,
          shiftDate.month,
          shiftDate.day,
          shiftTime.hour,
          shiftTime.minute,
        );

        // Debug logs
        print('Current time: $now');
        print('Shift date/time for ${application['shift_id']}: $shiftDateTime');
        print('Is after: ${shiftDateTime.isAfter(now)}');

        // For today's shifts, check the specific time
        return shiftDateTime.isAfter(now);
      } else {
        // Not today, so not upcoming by our new definition
        return false;
      }
    } catch (e) {
      print('Error determining if shift is today: $e');
      return false;
    }
  }

  Future<void> refreshApplications() async {
    try {
      // Reset applications but keep search query
      applications.clear();

      // Show loading indicator during refresh
      isLoading.value = true;

      // Fetch new data
      await fetchApplications();

      // Show a success message or handle as needed
    } catch (e) {
      print('Error refreshing applications: $e');
      Get.snackbar(
        'Refresh Failed',
        'Could not update applications data',
        snackPosition: SnackPosition.BOTTOM,
        backgroundColor: Colors.red,
        colorText: Colors.white,
      );
    } finally {
      isLoading.value = false;
    }
  }

  Future<String?> _showCheckInCodeDialog(
    BuildContext context,
    Map<String, dynamic> application,
  ) async {
    final codeController = TextEditingController();

    final code = await showDialog<String>(
      context: context,
      builder: (BuildContext dialogContext) {
        return Dialog(
          shape: RoundedRectangleBorder(
            borderRadius: BorderRadius.circular(16.0),
          ),
          child: Container(
            padding: EdgeInsets.all(20.0),
            child: Column(
              mainAxisSize: MainAxisSize.min,
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Row(
                  children: [
                    Icon(Icons.shield, color: Color(0xFF3461FD), size: 30),
                    SizedBox(width: 10),
                    Text(
                      'Verify Check-In',
                      style: TextStyle(
                        fontSize: 20.0,
                        fontWeight: FontWeight.bold,
                        color: Color(0xFF3461FD),
                      ),
                    ),
                    Spacer(),
                    IconButton(
                      icon: Icon(Icons.close, color: Colors.grey),
                      onPressed: () => Navigator.of(dialogContext).pop(null),
                    ),
                  ],
                ),
                SizedBox(height: 16.0),
                Row(
                  children: [
                    Icon(Icons.info_outline, color: Colors.blue.shade300),
                    SizedBox(width: 10),
                    Expanded(
                      child: Text(
                        'Please enter the Shift Seeker Code to verify check-in:',
                        style: TextStyle(
                          fontSize: 16.0,
                          color: Colors.grey.shade700,
                        ),
                      ),
                    ),
                  ],
                ),
                SizedBox(height: 16.0),
                TextField(
                  controller: codeController,
                  decoration: InputDecoration(
                    hintText: 'Enter Shift Seeker Code',
                    prefixIcon: Icon(Icons.key, color: Colors.grey),
                    suffixIcon: IconButton(
                      icon: Icon(Icons.close, color: Colors.grey),
                      onPressed: () => codeController.clear(),
                    ),
                    filled: true,
                    fillColor: Colors.grey.shade100,
                    border: OutlineInputBorder(
                      borderRadius: BorderRadius.circular(12.0),
                      borderSide: BorderSide.none,
                    ),
                    focusedBorder: OutlineInputBorder(
                      borderRadius: BorderRadius.circular(12.0),
                      borderSide: BorderSide(color: Colors.purple, width: 2),
                    ),
                  ),
                  keyboardType: TextInputType.number,
                ),
                SizedBox(height: 8.0),
                Text(
                  'The code was provided to the shift seeker',
                  style: TextStyle(color: Colors.grey.shade600, fontSize: 12.0),
                ),
                SizedBox(height: 16.0),
                Row(
                  mainAxisAlignment: MainAxisAlignment.end,
                  children: [
                    TextButton(
                      onPressed: () => Navigator.of(dialogContext).pop(null),
                      child: Text(
                        'Cancel',
                        style: TextStyle(color: Colors.grey),
                      ),
                    ),
                    SizedBox(width: 10),
                    ElevatedButton.icon(
                      onPressed: () {
                        Navigator.of(dialogContext).pop(codeController.text);
                      },
                      style: ElevatedButton.styleFrom(
                        backgroundColor: Color(0xFF3461FD),
                        shape: RoundedRectangleBorder(
                          borderRadius: BorderRadius.circular(12.0),
                        ),
                      ),
                      icon: Icon(
                        Icons.check_circle_outline,
                        color: Colors.white,
                      ),
                      label: Text(
                        'Verify',
                        style: TextStyle(color: Colors.white),
                      ),
                    ),
                  ],
                ),
              ],
            ),
          ),
        );
      },
    );

    return code;
  }

  Future<void> _showCheckOutDialog(
    BuildContext context,
    Map<String, dynamic> application,
    Map<String, dynamic> checkInRecord,
  ) async {
    // Use RxInt to track the selected ratings
    final onTimeRating = RxInt(0);
    final performanceRating = RxInt(0);
    final feedbackController = TextEditingController();

    // Get screen size to help with responsiveness
    final screenSize = MediaQuery.of(context).size;
    final isSmallScreen = screenSize.width < 360;

    // Store the result of the dialog
    final result = await showDialog<Map<String, dynamic>>(
      context: context,
      builder: (BuildContext dialogContext) {
        return Dialog(
          shape: RoundedRectangleBorder(
            borderRadius: BorderRadius.circular(16.0),
          ),
          insetPadding: EdgeInsets.symmetric(
            horizontal: isSmallScreen ? 10.0 : 20.0,
            vertical: 24.0,
          ),
          child: Container(
            width: min(screenSize.width * 0.95, 500), // Limit max width
            padding: EdgeInsets.all(isSmallScreen ? 16.0 : 24.0),
            constraints: BoxConstraints(
              maxHeight: MediaQuery.of(dialogContext).size.height * 0.8,
            ),
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                // Header with title and close button
                Row(
                  children: [
                    Icon(
                      Icons.check_circle_outline,
                      color: Colors.blue,
                      size: 24,
                    ),
                    SizedBox(width: 8),
                    Expanded(
                      child: Text(
                        'Worker Check Out',
                        style: TextStyle(
                          fontSize: isSmallScreen ? 18.0 : 20.0,
                          fontWeight: FontWeight.bold,
                          color: Colors.blue.shade700,
                        ),
                        overflow: TextOverflow.ellipsis,
                      ),
                    ),
                    IconButton(
                      icon: Icon(Icons.close, color: Colors.grey),
                      padding: EdgeInsets.zero,
                      constraints: BoxConstraints(),
                      onPressed: () => Navigator.of(dialogContext).pop(null),
                    ),
                  ],
                ),

                Divider(height: 16, color: Colors.grey.shade200),

                // Make the main content scrollable
                Expanded(
                  child: SingleChildScrollView(
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        // On-Time Rating Section
                        Row(
                          children: [
                            Container(
                              padding: EdgeInsets.all(6),
                              decoration: BoxDecoration(
                                color: Colors.amber.shade100,
                                borderRadius: BorderRadius.circular(8),
                              ),
                              child: Icon(
                                Icons.timer,
                                color: Colors.amber.shade800,
                                size: isSmallScreen ? 16 : 20,
                              ),
                            ),
                            SizedBox(width: 8),
                            Expanded(
                              child: Text(
                                'On-Time Rating',
                                style: TextStyle(
                                  fontWeight: FontWeight.bold,
                                  fontSize: isSmallScreen ? 14 : 16,
                                  color: Colors.grey.shade800,
                                ),
                              ),
                            ),
                          ],
                        ),
                        SizedBox(height: 8),
                        Obx(
                          () => Container(
                            padding: EdgeInsets.symmetric(
                              vertical: 4,
                              horizontal: 8,
                            ),
                            decoration: BoxDecoration(
                              color: Colors.grey.shade100,
                              borderRadius: BorderRadius.circular(12),
                            ),
                            child: Row(
                              mainAxisAlignment: MainAxisAlignment.spaceEvenly,
                              children: List.generate(5, (index) {
                                return InkWell(
                                  onTap: () {
                                    onTimeRating.value = index + 1;
                                  },
                                  child: Container(
                                    padding: EdgeInsets.all(
                                      isSmallScreen ? 4 : 6,
                                    ),
                                    decoration: BoxDecoration(
                                      color:
                                          index < onTimeRating.value
                                              ? Colors.amber.shade100
                                              : Colors.transparent,
                                      borderRadius: BorderRadius.circular(8),
                                    ),
                                    child: Icon(
                                      index < onTimeRating.value
                                          ? Icons.star_rounded
                                          : Icons.star_outline_rounded,
                                      color:
                                          index < onTimeRating.value
                                              ? Colors.amber
                                              : Colors.grey.shade400,
                                      size: isSmallScreen ? 24 : 28,
                                    ),
                                  ),
                                );
                              }),
                            ),
                          ),
                        ),

                        SizedBox(height: 16),

                        // Performance Rating Section
                        Row(
                          children: [
                            Container(
                              padding: EdgeInsets.all(6),
                              decoration: BoxDecoration(
                                color: Colors.blue.shade100,
                                borderRadius: BorderRadius.circular(8),
                              ),
                              child: Icon(
                                Icons.trending_up,
                                color: Colors.blue.shade800,
                                size: isSmallScreen ? 16 : 20,
                              ),
                            ),
                            SizedBox(width: 8),
                            Expanded(
                              child: Text(
                                'Performance Rating',
                                style: TextStyle(
                                  fontWeight: FontWeight.bold,
                                  fontSize: isSmallScreen ? 14 : 16,
                                  color: Colors.grey.shade800,
                                ),
                              ),
                            ),
                          ],
                        ),
                        SizedBox(height: 8),
                        Obx(
                          () => Container(
                            padding: EdgeInsets.symmetric(
                              vertical: 4,
                              horizontal: 8,
                            ),
                            decoration: BoxDecoration(
                              color: Colors.grey.shade100,
                              borderRadius: BorderRadius.circular(12),
                            ),
                            child: Row(
                              mainAxisAlignment: MainAxisAlignment.spaceEvenly,
                              children: List.generate(5, (index) {
                                return InkWell(
                                  onTap: () {
                                    performanceRating.value = index + 1;
                                  },
                                  child: Container(
                                    padding: EdgeInsets.all(
                                      isSmallScreen ? 4 : 6,
                                    ),
                                    decoration: BoxDecoration(
                                      color:
                                          index < performanceRating.value
                                              ? Colors.blue.shade100
                                              : Colors.transparent,
                                      borderRadius: BorderRadius.circular(8),
                                    ),
                                    child: Icon(
                                      index < performanceRating.value
                                          ? Icons.star_rounded
                                          : Icons.star_outline_rounded,
                                      color:
                                          index < performanceRating.value
                                              ? Colors.blue.shade700
                                              : Colors.grey.shade400,
                                      size: isSmallScreen ? 24 : 28,
                                    ),
                                  ),
                                );
                              }),
                            ),
                          ),
                        ),

                        SizedBox(height: 16),

                        // Feedback TextField
                        Text(
                          'Additional Feedback',
                          style: TextStyle(
                            fontWeight: FontWeight.bold,
                            fontSize: isSmallScreen ? 14 : 16,
                            color: Colors.grey.shade800,
                          ),
                        ),
                        SizedBox(height: 8),
                        TextField(
                          controller: feedbackController,
                          decoration: InputDecoration(
                            hintText: 'How did the worker perform?',
                            border: OutlineInputBorder(
                              borderRadius: BorderRadius.circular(12),
                              borderSide: BorderSide(
                                color: Colors.grey.shade300,
                              ),
                            ),
                            focusedBorder: OutlineInputBorder(
                              borderRadius: BorderRadius.circular(12),
                              borderSide: BorderSide(
                                color: Colors.blue,
                                width: 2,
                              ),
                            ),
                            filled: true,
                            fillColor: Colors.grey.shade50,
                            contentPadding: EdgeInsets.symmetric(
                              horizontal: 12,
                              vertical: 10,
                            ),
                          ),
                          maxLines: 3,
                        ),

                        SizedBox(height: 8),
                      ],
                    ),
                  ),
                ),

                // Action Buttons - outside of scrollable area
                Padding(
                  padding: const EdgeInsets.only(top: 8.0),
                  child: Row(
                    mainAxisAlignment: MainAxisAlignment.end,
                    children: [
                      OutlinedButton(
                        onPressed: () => Navigator.of(dialogContext).pop(null),
                        style: OutlinedButton.styleFrom(
                          shape: RoundedRectangleBorder(
                            borderRadius: BorderRadius.circular(12),
                          ),
                          side: BorderSide(color: Colors.grey.shade300),
                          padding: EdgeInsets.symmetric(
                            horizontal: isSmallScreen ? 12 : 16,
                            vertical: 8,
                          ),
                        ),
                        child: Text(
                          'Cancel',
                          style: TextStyle(
                            color: Colors.grey.shade700,
                            fontSize: isSmallScreen ? 14 : 16,
                          ),
                        ),
                      ),
                      SizedBox(width: 8),
                      ElevatedButton(
                        onPressed: () {
                          if (onTimeRating.value < 1 ||
                              performanceRating.value < 1) {
                            ScaffoldMessenger.of(dialogContext).showSnackBar(
                              SnackBar(
                                content: Text(
                                  'Please rate both on-time and performance',
                                ),
                                backgroundColor: Colors.red,
                              ),
                            );
                            return;
                          }

                          // ✅ Close dialog and return result (do NOT show loader here)
                          Navigator.of(dialogContext).pop({
                            'onTimeRating': onTimeRating.value,
                            'performanceRating': performanceRating.value,
                            'feedback': feedbackController.text,
                          });
                        },
                        style: ElevatedButton.styleFrom(
                          backgroundColor: Colors.blue.shade700,
                          foregroundColor: Colors.white,
                          shape: RoundedRectangleBorder(
                            borderRadius: BorderRadius.circular(12),
                          ),
                          padding: EdgeInsets.symmetric(
                            horizontal: isSmallScreen ? 12 : 16,
                            vertical: 8,
                          ),
                        ),
                        child: Row(
                          mainAxisSize: MainAxisSize.min,
                          children: [
                            Icon(
                              Icons.check_circle_outline,
                              size: isSmallScreen ? 14 : 16,
                            ),
                            SizedBox(width: 6),
                            Text(
                              'Complete',
                              style: TextStyle(
                                fontSize: isSmallScreen ? 14 : 16,
                              ),
                            ),
                          ],
                        ),
                      ),
                    ],
                  ),
                ),
              ],
            ),
          ),
        );
      },
    );
    if (result != null && context.mounted) {
      showDialog(
        context: context,
        barrierDismissible: false,
        builder: (_) => const Center(child: CircularProgressIndicator()),
      );

      try {
        await _performCheckOut(
          checkInRecord,
          result['onTimeRating'],
          result['performanceRating'],
          result['feedback'],
        );
        if (context.mounted) {
          Navigator.of(context).pop(); // close loader
          _showSuccessSnackbar(context, 'Check-out successful!');
        }
      } catch (e) {
        if (context.mounted) {
          Navigator.of(context).pop(); // close loader
          _showErrorSnackbar(context, 'Check-out failed: ${e.toString()}');
        }
      } finally {
        await refreshApplications();
      }
    }
  }

  Future<Map<String, dynamic>?> _checkExistingCheckIn(
    Map<String, dynamic> application,
  ) async {
    try {
      final response =
          await supabase
              .from('worker_attendance')
              .select()
              .eq('application_id', application['id'])
              .eq('status', 'checked_in')
              .maybeSingle();

      return response;
    } catch (e) {
      print('Error checking existing check-in: $e');
      return null;
    }
  }

  Future<bool> _verifyJobSeekerCode(String fullName, String enteredCode) async {
    try {
      final response =
          await supabase
              .from('job_seekers')
              .select('id, job_seeker_code')
              .eq('full_name', fullName)
              .eq('job_seeker_code', enteredCode)
              .maybeSingle();

      print('Job Seeker Verification Response: $response');
      return response != null;
    } catch (e) {
      print('Error verifying job seeker code: $e');
      return false;
    }
  }

  void _showSuccessSnackbar(BuildContext context, String message) {
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(content: Text(message), backgroundColor: Colors.green),
    );
  }

  void _showErrorSnackbar(BuildContext context, String message) {
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(content: Text(message), backgroundColor: Colors.red),
    );
  }

  void viewDetails(Map<String, dynamic> application) {
    Get.dialog(
      Dialog(
        shape: RoundedRectangleBorder(
          borderRadius: BorderRadius.circular(16.0),
        ),
        elevation: 0,
        backgroundColor: Colors.white,
        child: Container(
          padding: EdgeInsets.all(24.0),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            mainAxisSize: MainAxisSize.min,
            children: [
              Row(
                mainAxisAlignment: MainAxisAlignment.spaceBetween,
                children: [
                  Text(
                    'Application Details',
                    style: TextStyle(
                      fontSize: 20.0,
                      fontWeight: FontWeight.bold,
                      color: Color(0xFF5B6BF8),
                      fontFamily: 'Inter',
                    ),
                  ),
                  InkWell(
                    onTap: () => Get.back(),
                    child: Container(
                      padding: EdgeInsets.all(4),
                      decoration: BoxDecoration(
                        color: Colors.grey.shade100,
                        shape: BoxShape.circle,
                      ),
                      child: Icon(
                        Icons.close,
                        size: 20,
                        color: Colors.grey.shade700,
                      ),
                    ),
                  ),
                ],
              ),
              SizedBox(height: 16.0),
              Divider(color: Colors.grey.shade200, thickness: 1),
              SizedBox(height: 16.0),
              _buildInfoRow('Full Name', application['full_name']),
              _buildInfoRow('Phone', application['phone_number']),
              _buildInfoRow('Email', application['email']),
              if (application['status'] != null)
                _buildStatusRow('Status', application['status']),
              if (application['submission_date'] != null)
                _buildInfoRow('Submitted', application['submission_date']),
              SizedBox(height: 24.0),
              Row(
                mainAxisAlignment: MainAxisAlignment.center,
                children: [
                  ElevatedButton(
                    onPressed: () => Get.back(),
                    style: ElevatedButton.styleFrom(
                      backgroundColor: Color(0xFF5B6BF8),
                      foregroundColor: Colors.white,
                      padding: EdgeInsets.symmetric(
                        horizontal: 32,
                        vertical: 12,
                      ),
                      shape: RoundedRectangleBorder(
                        borderRadius: BorderRadius.circular(10),
                      ),
                      elevation: 0,
                    ),
                    child: Text(
                      'Close',
                      style: TextStyle(
                        fontSize: 16,
                        fontWeight: FontWeight.w600,
                        fontFamily: 'Inter',
                      ),
                    ),
                  ),
                ],
              ),
            ],
          ),
        ),
      ),
    );
  }

  Widget _buildStatusRow(String label, String status) {
    final Color statusColor =
        status.toLowerCase() == 'approved'
            ? Colors.green
            : status.toLowerCase() == 'rejected'
            ? Colors.red
            : status.toLowerCase() == 'pending'
            ? Colors.amber.shade700
            : Colors.blue;

    return Padding(
      padding: const EdgeInsets.only(bottom: 16.0),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Text(
            label,
            style: TextStyle(
              color: Colors.grey.shade600,
              fontSize: 14,
              fontFamily: 'Inter',
            ),
          ),
          SizedBox(height: 4),
          Container(
            padding: EdgeInsets.symmetric(horizontal: 12, vertical: 6),
            decoration: BoxDecoration(
              color: statusColor.withOpacity(0.1),
              borderRadius: BorderRadius.circular(16),
            ),
            child: Text(
              status,
              style: TextStyle(
                fontSize: 14,
                fontWeight: FontWeight.w600,
                color: statusColor,
                fontFamily: 'Inter',
              ),
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildInfoRow(String label, String value) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 6.0),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          SizedBox(
            width: 100,
            child: Text(
              '$label:',
              style: TextStyle(
                fontWeight: FontWeight.w500,
                color: Colors.grey[700],
              ),
            ),
          ),
          Expanded(
            child: Text(
              value ?? 'Not provided',
              style: TextStyle(fontWeight: FontWeight.w400),
            ),
          ),
        ],
      ),
    );
  }

  Future<void> fetchApplications() async {
    isLoading.value = true;

    try {
      final employerId = supabase.auth.currentUser?.email;
      print('DEBUG: Current employer ID: $employerId');

      if (employerId == null) {
        print('DEBUG: No user ID found');
        applications.value = [];
        isLoading.value = false;
        return;
      }

      // Fetch job listings for the current employer
      final jobListingsResponse = await supabase
          .from('worker_job_listings')
          .select('*')
          .eq('contact_email', employerId);

      print(
        'DEBUG: Employer Job Listings Count: ${jobListingsResponse.length}',
      );

      // Extract shift IDs from employer's job listings
      final employerShiftIds =
          jobListingsResponse.map((job) => job['shift_id'].toString()).toList();

      print('DEBUG: Employer Shift IDs: $employerShiftIds');

      // Fetch worker job applications for these shift IDs
      final applicationsResponse = await supabase
          .from('worker_job_applications')
          .select('*')
          .inFilter('shift_id', employerShiftIds)
          .order('application_date', ascending: true);

      print(
        'DEBUG: Matching Applications Count: ${applicationsResponse.length}',
      );

      // Process applications
      List<Map<String, dynamic>> processedApplications = [];

      for (var app in applicationsResponse) {
        // Find matching job listing
        final matchingJob = jobListingsResponse.firstWhere(
          (job) => job['shift_id'].toString() == app['shift_id'].toString(),
          // orElse: () => null,
        );

        if (matchingJob != null) {
          processedApplications.add({
            ...app,
            ...matchingJob,
            'id': app['id'], // Ensure the original application ID is preserved
            'worker_id':
                app['user_id'] ??
                matchingJob['user_id'], // Ensure worker ID is present
            'job_title':
                matchingJob['job_title'] ?? app['job_title'] ?? 'Unknown Job',
            'company':
                matchingJob['company'] ?? app['company'] ?? 'Unknown Company',
            'location':
                matchingJob['location'] ??
                app['location'] ??
                'Unknown Location',
            'formatted_date': _formatDate(app['date']),
            'pay_rate': app['pay_rate'] ?? matchingJob['pay_rate'] ?? 0,
            'status': app['application_status'] ?? 'Applied',
          });
        }
      }

      applications.value = processedApplications;
      print('Final Processed Applications Count: ${applications.length}');
    } catch (e) {
      print('CRITICAL FETCH ERROR: $e');
      applications.value = [];
    } finally {
      isLoading.value = false;
    }
  }

  // Helper method for date formatting
  String _formatDate(dynamic dateInput) {
    if (dateInput == null) return 'No date specified';
    try {
      final date = DateTime.parse(dateInput.toString());
      return DateFormat('MMMM d, yyyy').format(date);
    } catch (e) {
      print('Date parsing error: $e');
      return 'Invalid date';
    }
  }

  List<Map<String, dynamic>> get filteredApplications {
    if (searchQuery.isEmpty) {
      return applications;
    }

    final query = searchQuery.toLowerCase();
    return applications.where((app) {
      // Search across multiple fields
      final shiftId = app['shift_id']?.toString().toLowerCase() ?? '';
      final fullName = app['full_name']?.toString().toLowerCase() ?? '';
      final jobTitle = app['job_title']?.toString().toLowerCase() ?? '';
      final location = app['location']?.toString().toLowerCase() ?? '';
      final email = app['email']?.toString().toLowerCase() ?? '';
      final category = app['category']?.toString().toLowerCase() ?? '';
      final phoneNumber = app['phone_number']?.toString().toLowerCase() ?? '';
      final company = app['company']?.toString().toLowerCase() ?? '';
      final status = app['application_status']?.toString().toLowerCase() ?? '';

      // Check if query matches any of the fields
      return shiftId.contains(query) ||
          fullName.contains(query) ||
          jobTitle.contains(query) ||
          location.contains(query) ||
          email.contains(query) ||
          category.contains(query) ||
          phoneNumber.contains(query) ||
          company.contains(query) ||
          status.contains(query);
    }).toList();
  }

  // Method to update search query
  void updateSearchQuery(String query) {
    searchQuery.value = query;
    if (searchController.text != query) {
      searchController.text = query;
    }
  }
}

class WorkerApplicationsScreen extends StatelessWidget {
  const WorkerApplicationsScreen({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    final WorkerApplicationsController controller = Get.put(
      WorkerApplicationsController(),
    );
    final theme = Theme.of(context);
    final isDark = Theme.of(context).brightness == Brightness.dark;
    final userId = Supabase.instance.client.auth.currentUser?.id;

    return Scaffold(
      appBar: StandardAppBar(title: 'Worker Applications', centerTitle: false),
      bottomNavigationBar: const ShiftHourBottomNavigation(),

      body: GestureDetector(
        behavior: HitTestBehavior.opaque,
        onTap: () {
          // This will unfocus any text field and dismiss the keyboard
          FocusManager.instance.primaryFocus?.unfocus();
        },

        child: Padding(
          padding: const EdgeInsets.all(16.0),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              // Search by Job ID
              Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Container(
                    decoration: BoxDecoration(
                      borderRadius: BorderRadius.circular(30),
                      color: Colors.white,
                      boxShadow: [
                        BoxShadow(
                          color: Colors.blue.withOpacity(0.2),
                          blurRadius: 8,
                          spreadRadius: 1,
                          offset: Offset(0, 3),
                        ),
                      ],
                    ),
                    child: TextField(
                      controller:
                          controller
                              .searchController, // Use the controller from your controller class
                      onChanged: controller.updateSearchQuery,
                      decoration: InputDecoration(
                        hintText: 'Search shifts',
                        prefixIcon: const Icon(
                          Icons.search,
                          color: Colors.grey,
                        ),
                        suffixIcon: Obx(() {
                          return controller.searchQuery.value.isNotEmpty
                              ? IconButton(
                                icon: const Icon(
                                  Icons.close,
                                  color: Colors.grey,
                                ),
                                onPressed: () {
                                  controller.updateSearchQuery('');
                                  controller.searchController
                                      .clear(); // Also clear the controller
                                },
                              )
                              : const SizedBox.shrink();
                        }),
                        border: InputBorder.none,
                        contentPadding: const EdgeInsets.symmetric(
                          vertical: 14,
                        ),
                      ),
                    ),
                  ),
                ],
              ),

              const SizedBox(height: 20),
              // In your WorkerApplicationsScreen class, replace the Expanded section in the build method with this:
              Obx(
                () => SingleChildScrollView(
                  scrollDirection: Axis.horizontal,
                  child: Row(
                    children: [
                      _buildFilterChip(
                        'All',
                        controller.statusFilter.value == 'All',
                        Colors.grey,
                        () => controller.updateStatusFilter('All'),
                      ),
                      const SizedBox(width: 8),
                      _buildFilterChip(
                        'Upcoming',
                        controller.statusFilter.value == 'Upcoming',
                        Color(0xFFF5A623),
                        () => controller.updateStatusFilter('Upcoming'),
                      ),
                      const SizedBox(width: 8),
                      _buildFilterChip(
                        'In Progress',
                        controller.statusFilter.value == 'In Progress',
                        Color(0xFF3461FD),
                        () => controller.updateStatusFilter('In Progress'),
                      ),
                      const SizedBox(width: 8),
                      _buildFilterChip(
                        'Completed',
                        controller.statusFilter.value == 'Completed',
                        Color(0xFF27AE60),
                        () => controller.updateStatusFilter('Completed'),
                      ),
                    ],
                  ),
                ),
              ),

              const SizedBox(height: 16),
              // Applications List
              Expanded(
                child: Obx(() {
                  if (controller.isLoading.value) {
                    final applications =
                        controller.filteredApplicationsByStatus;
                    return const Center(child: CircularProgressIndicator());
                  }

                  if (controller.applications.isEmpty) {
                    return RefreshIndicator(
                      onRefresh: controller.refreshApplications,
                      child: ListView(
                        // controller: controller._scrollController,
                        physics: const AlwaysScrollableScrollPhysics(),
                        children: [
                          SizedBox(
                            height: MediaQuery.of(context).size.height * 0.6,
                            child: Center(child: _buildEmptyState(theme)),
                          ),
                        ],
                      ),
                    );
                  }

                  final applications = controller.filteredApplicationsByStatus;

                  if (applications.isEmpty) {
                    return RefreshIndicator(
                      onRefresh: controller.refreshApplications,
                      child: ListView(
                        controller: controller.scrollController,
                        physics: const AlwaysScrollableScrollPhysics(),
                        children: [
                          SizedBox(
                            height: MediaQuery.of(context).size.height * 0.6,
                            child: Center(
                              child: Column(
                                mainAxisAlignment: MainAxisAlignment.center,
                                children: [
                                  Icon(
                                    Icons.search_off,
                                    size: 64,
                                    color: Colors.grey.shade400,
                                  ),
                                  const SizedBox(height: 16),
                                  Text(
                                    'No applications found with that Shift ID',
                                    style: theme.textTheme.titleMedium,
                                  ),
                                ],
                              ),
                            ),
                          ),
                        ],
                      ),
                    );
                  }
                  return RefreshIndicator(
                    onRefresh: controller.refreshApplications,
                    child: ListView.separated(
                      controller: controller.scrollController,
                      physics: const AlwaysScrollableScrollPhysics(),
                      itemCount: applications.length,
                      separatorBuilder:
                          (context, index) => const SizedBox(height: 12),
                      itemBuilder: (context, index) {
                        final application = applications[index];

                        // Check if worker is assigned
                        final bool isAssigned =
                            application['full_name'] != null &&
                            application['full_name'].toString().isNotEmpty;

                        // Create an Applicant object from the application data
                        // In the WorkerApplicationsScreen class, modify the Applicant creation in the build method:

                        final applicant = Applicant(
                          name:
                              application['full_name'] ?? 'No Worker Assigned',
                          role: application['company'] ?? '',
                          rating:
                              (application['worker_rating'] ?? 0).toDouble(),
                          jobTitle: application['job_title'] ?? '',
                          shiftId: application['shift_id'] ?? '',
                          category: application['category'] ?? 'Others',
                          startTime: application['start_time'] ?? '',
                          endTime: application['end_time'] ?? '',
                          location: application['location'] ?? '',
                          phoneNumber: application['phone_number'] ?? '',
                          email: application['email'] ?? '',
                          avatarText:
                              (application['full_name'] != null &&
                                      application['full_name']
                                          .toString()
                                          .isNotEmpty)
                                  ? application['full_name']
                                      .toString()
                                      .substring(0, 1)
                                      .toUpperCase()
                                  : 'NA',
                          avatarColor: isAssigned ? Colors.blue : Colors.grey,
                          isAssigned: isAssigned,
                          appstatus:
                              application['application_status'] ?? 'Applied',

                          pincode: application['job_pincode']?.toString() ?? '',
                          date: application['date']?.toString() ?? '',
                          // Fix here - use "N/A" instead of "null"
                        );

                        // Then in the _buildApplicantCard method, modify how you display the pincode:

                        return _buildApplicantCard(
                          applicant,
                          application,
                          context,
                          controller,
                        );
                      },
                    ),
                  );
                }),
              ),
            ],
          ),
        ),
      ),
    );
  }

  Widget _buildFilterChip(
    String label,
    bool isSelected,
    Color color,
    VoidCallback onTap,
  ) {
    return GestureDetector(
      onTap: onTap,
      child: Container(
        padding: EdgeInsets.symmetric(horizontal: 16, vertical: 8),
        decoration: BoxDecoration(
          color: isSelected ? color.withOpacity(0.2) : Colors.white,
          borderRadius: BorderRadius.circular(20),
          border: Border.all(
            color: isSelected ? color : Colors.grey.shade300,
            width: 1.5,
          ),
        ),
        child: Row(
          mainAxisSize: MainAxisSize.min,
          children: [
            if (isSelected)
              Padding(
                padding: const EdgeInsets.only(right: 6),
                child: Icon(Icons.check_circle, size: 14, color: color),
              ),
            Text(
              label,
              style: TextStyle(
                color: isSelected ? color : Colors.grey.shade700,
                fontWeight: isSelected ? FontWeight.w600 : FontWeight.normal,
              ),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildEmptyState(ThemeData theme) {
    return Center(
      child: Column(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          Icon(Icons.work_off_outlined, size: 64, color: Colors.grey.shade400),
          const SizedBox(height: 16),
          Text('No applications found', style: theme.textTheme.titleMedium),
          const SizedBox(height: 8),
        ],
      ),
    );
  }

  Widget _buildCheckButton(
    Map<String, dynamic> application,
    WorkerApplicationsController controller,
    BuildContext context,
  ) {
    final appId = application['id'].toString();
    final checkStatus =
        controller.checkInStatuses[appId]?.value ?? 'not_checked_in';
    final isLoading = controller.checkInLoading[appId] == true;
    final appStatus =
        application['application_status']?.toString().toLowerCase();

    // Get current date
    final now = DateTime.now();
    final today = DateTime(now.year, now.month, now.day);

    // Parse shift date from application
    DateTime? shiftDate;
    if (application['date'] != null) {
      shiftDate = DateTime.parse(application['date'].toString());
    } else if (application['shift_date'] != null) {
      shiftDate = DateTime.parse(application['shift_date'].toString());
    }

    // If no date or not today's date, don't show Check In button
    final isTodayShift =
        shiftDate != null &&
        DateTime(
          shiftDate.year,
          shiftDate.month,
          shiftDate.day,
        ).isAtSameMomentAs(today);

    // For Completed status - hide all buttons
    if (appStatus == 'completed') {
      return SizedBox.shrink();
    }

    // For "In Progress" - show Check Out button
    if (appStatus == 'in progress') {
      if (isLoading) {
        return Container(
          alignment: Alignment.centerRight,
          child: ElevatedButton(
            onPressed: null,
            style: ElevatedButton.styleFrom(
              fixedSize: Size(130, 48),
              backgroundColor: Colors.red,
              padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 12),
              shape: RoundedRectangleBorder(
                borderRadius: BorderRadius.circular(8),
              ),
            ),
            child: Row(
              mainAxisSize: MainAxisSize.min,
              mainAxisAlignment: MainAxisAlignment.center,
              children: [
                SizedBox(
                  width: 16,
                  height: 16,
                  child: CircularProgressIndicator(
                    strokeWidth: 2,
                    valueColor: AlwaysStoppedAnimation<Color>(Colors.white),
                  ),
                ),
                SizedBox(width: 8),
                Text('Processing...'),
              ],
            ),
          ),
        );
      }

      return Container(
        alignment: Alignment.centerRight,
        child: ElevatedButton.icon(
          icon: Icon(Icons.logout_outlined, size: 18),
          label: Text('Check Out'),
          onPressed: () async {
            await controller.checkIn(application, context);
            await controller.refreshApplications();
          },
          style: ElevatedButton.styleFrom(
            fixedSize: Size(130, 48),
            backgroundColor: Colors.red,
            foregroundColor: Colors.white,
            padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 12),
            shape: RoundedRectangleBorder(
              borderRadius: BorderRadius.circular(8),
            ),
          ),
        ),
      );
    }

    // For "Upcoming" - show buttons based on date
    if (appStatus == 'upcoming') {
      return Row(
        mainAxisAlignment: MainAxisAlignment.end, // Align to the right
        children: [
          // Cancel Shift button - always visible, fixed width
          Container(
            width: 130, // Fixed width to match original layout
            child: OutlinedButton(
              onPressed: () {
                controller.cancelShift(application, context);
              },
              style: OutlinedButton.styleFrom(
                minimumSize: Size(0, 48),
                foregroundColor: Colors.red,
                side: BorderSide(color: Colors.red.shade200),
                shape: RoundedRectangleBorder(
                  borderRadius: BorderRadius.circular(8),
                ),
              ),
              child: Text('Cancel Shift'),
            ),
          ),

          // Only show Check In button for today's shifts
          if (isTodayShift) ...[
            const SizedBox(width: 12),
            Expanded(
              child: Obx(() {
                final appId = application['id'].toString();
                final isLoading = controller.checkInLoading[appId] ?? false;

                return isLoading
                    ? ElevatedButton(
                      onPressed: null,
                      style: ElevatedButton.styleFrom(
                        minimumSize: Size(double.infinity, 48),
                        backgroundColor: const Color(0xFF3461FD),
                        shape: RoundedRectangleBorder(
                          borderRadius: BorderRadius.circular(8),
                        ),
                      ),
                      child: Row(
                        mainAxisAlignment: MainAxisAlignment.center,
                        children: [
                          SizedBox(
                            height: 20,
                            width: 20,
                            child: CircularProgressIndicator(
                              strokeWidth: 2,
                              valueColor: AlwaysStoppedAnimation<Color>(
                                Colors.white,
                              ),
                            ),
                          ),
                          SizedBox(width: 8),
                          Text('Processing...'),
                        ],
                      ),
                    )
                    : ElevatedButton(
                      onPressed: () async {
                        controller.checkInLoading[appId] = true; // ✅ Trigger UI
                        await controller.checkIn(application, context);
                        await controller.refreshApplications();
                        controller.checkInLoading[appId] =
                            false; // ✅ Reset loader
                      },
                      style: ElevatedButton.styleFrom(
                        minimumSize: Size(double.infinity, 48),
                        backgroundColor: const Color(0xFF3461FD),
                        foregroundColor: Colors.white,
                        shape: RoundedRectangleBorder(
                          borderRadius: BorderRadius.circular(8),
                        ),
                      ),
                      child: Text('Check In'),
                    );
              }),
            ),
          ],
        ],
      );
    }

    // Fallback - hide button for any unexpected status
    return SizedBox.shrink();
  }

  String _formatJobTime(String? timeString) {
    if (timeString == null) return '';

    final time = TimeOfDay.fromDateTime(
      DateTime.tryParse('2000-01-01 $timeString')!,
    );
    final period = time.period == DayPeriod.am ? 'AM' : 'PM';
    final hour = time.hourOfPeriod == 0 ? 12 : time.hourOfPeriod;
    final minute = time.minute.toString().padLeft(2, '0');

    return '$hour:$minute $period';
  }

  String _calculateShiftDuration(String? startTime, String? endTime) {
    if (startTime == null || endTime == null) return 'N/A';

    try {
      // Parse start and end times
      final startParts = startTime.split(':');
      final endParts = endTime.split(':');

      if (startParts.length < 2 || endParts.length < 2) return 'N/A';

      final startHour = int.parse(startParts[0]);
      final startMinute = int.parse(startParts[1]);
      final endHour = int.parse(endParts[0]);
      final endMinute = int.parse(endParts[1]);

      // Calculate duration in minutes
      int durationMinutes =
          (endHour * 60 + endMinute) - (startHour * 60 + startMinute);

      // Handle overnight shifts
      if (durationMinutes < 0) {
        durationMinutes += 24 * 60;
      }

      // Convert to hours and minutes
      final hours = durationMinutes ~/ 60;
      final minutes = durationMinutes % 60;

      // Format duration string
      if (hours > 0 && minutes > 0) {
        return '$hours hr $minutes min';
      } else if (hours > 0) {
        return '$hours hr';
      } else {
        return '$minutes min';
      }
    } catch (e) {
      return 'N/A';
    }
  }

  double _calculateHourlyRate(String duration, double originalPayRate) {
    // Parse duration to extract total hours
    final hours = _extractHoursFromDuration(duration);

    // If no hours, return the original pay rate
    return hours > 0 ? originalPayRate / hours : originalPayRate;
  }

  double _extractHoursFromDuration(String duration) {
    double totalHours = 0.0;

    // Extract hours
    final hourMatch = RegExp(r'(\d+)\s*hr').firstMatch(duration);
    if (hourMatch != null) {
      totalHours += double.parse(hourMatch.group(1)!);
    }

    // Extract minutes
    final minuteMatch = RegExp(r'(\d+)\s*min').firstMatch(duration);
    if (minuteMatch != null) {
      totalHours += double.parse(minuteMatch.group(1)!) / 60;
    }

    return totalHours;
  }

  String _getMonthName(int month) {
    const months = [
      'January',
      'February',
      'March',
      'April',
      'May',
      'June',
      'July',
      'August',
      'September',
      'October',
      'November',
      'December',
    ];
    return months[month - 1];
  }

  String _formatJobDatee(String? dateString) {
    if (dateString == null) return '';

    final date = DateTime.tryParse(dateString);
    if (date == null) return '';

    return '${_getMonthName(date.month)} ${date.day} ${date.year}';
  }

  Widget _buildApplicantCard(
    Applicant applicant,
    Map<String, dynamic> application,
    BuildContext context,
    WorkerApplicationsController controller,
  ) {
    String duration = _calculateShiftDuration(
      applicant.startTime,
      applicant.endTime,
    );
    // Your card building code...
    Color getStatusColor(appstatus) {
      switch (appstatus.toLowerCase()) {
        case 'in progress':
          return Color(0xFF3461FD); // Blue
        case 'upcoming':
          return Color(0xFFF5A623); // Yellow/Orange
        case 'completed':
          return Color(0xFF27AE60); // Green
        default:
          return Color(0xFF2563EB); // Default blue
      }
    }

    Color getStatusBackgroundColor(appstatus) {
      switch (appstatus.toLowerCase()) {
        case 'in progress':
          return Color(0x1A3461FD); // Light Blue
        case 'upcoming':
          return Color(0x1AF5A623); // Light Yellow/Orange
        case 'completed':
          return Color(0x1A27AE60); // Light Green
        default:
          return Color(0x1A2563EB); // Default light blue
      }
    }

    return Container(
      width: double.infinity,
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(12),
        boxShadow: [
          BoxShadow(
            blurRadius: 4,
            color: Color(0x0D000000),
            offset: Offset(0, 2),
          ),
        ],
      ),
      child: Padding(
        padding: EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            // Job Title and Status Row
            Container(
              width: double.infinity,
              decoration: BoxDecoration(
                color: getStatusBackgroundColor(applicant.appstatus),
                borderRadius: BorderRadius.circular(12),
              ),
              child: Padding(
                padding: EdgeInsets.all(12),
                child: Row(
                  mainAxisAlignment: MainAxisAlignment.spaceBetween,
                  children: [
                    Expanded(
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          Text(
                            'Title: ${applicant.jobTitle}',
                            style: TextStyle(
                              fontSize: 16,
                              fontWeight: FontWeight.w600,
                            ),
                          ),
                          Text(
                            'Category: ${applicant.category}',
                            style: TextStyle(
                              fontSize: 14,
                              // color: Colors.grey.shade600,
                              fontWeight: FontWeight.w500,
                              //fontWeight: FontWeight.w600,
                            ),
                          ),
                          Text(
                            'Shift ID: ${applicant.shiftId}',
                            style: TextStyle(
                              color: Colors.grey.shade600,
                              fontSize: 12,
                            ),
                          ),
                        ],
                      ),
                    ),
                    Container(
                      padding: EdgeInsets.symmetric(
                        horizontal: 12,
                        vertical: 6,
                      ),
                      decoration: BoxDecoration(
                        color: getStatusColor(applicant.appstatus),
                        borderRadius: BorderRadius.circular(16),
                      ),
                      child: Text(
                        applicant.appstatus ?? '',
                        style: TextStyle(color: Colors.white, fontSize: 12),
                      ),
                    ),
                  ],
                ),
              ),
            ),

            // Shift ID line

            // Time and Location
            SizedBox(height: 12),

            Row(
              mainAxisAlignment: MainAxisAlignment.spaceBetween,
              children: [
                Row(
                  children: [
                    Icon(Icons.business, size: 18, color: Colors.black),
                    SizedBox(width: 8),
                    Text(
                      'Company: ${applicant.role}', // Using role as company
                      style: TextStyle(
                        fontSize: 15,
                        color: Colors.grey.shade800,
                      ),
                      maxLines: 4,
                      overflow: TextOverflow.ellipsis,
                    ),
                  ],
                ),
              ],
            ),
            // Time and date row
            Row(
              children: [
                Icon(
                  Icons.calendar_month,
                  size: 18,
                  color: Colors.orange.shade700,
                ),
                const SizedBox(width: 8),
                Text(
                  'Date: '
                  '${_formatJobDatee(applicant.date)}',
                  style: TextStyle(fontSize: 15, color: Colors.grey.shade800),
                ),
              ],
            ),
            Row(
              children: [
                Icon(
                  Icons.access_time_rounded,
                  size: 18,
                  color: Colors.grey.shade700,
                ),
                const SizedBox(width: 8),
                Text(
                  'Time: '
                  '${_formatJobTime(applicant.startTime)} - ${_formatJobTime(applicant.endTime)}',
                  style: TextStyle(fontSize: 15, color: Colors.grey.shade800),
                ),
              ],
            ),

            Row(
              mainAxisAlignment: MainAxisAlignment.spaceBetween,
              children: [
                // Duration
                Row(
                  children: [
                    Icon(Icons.timer, size: 18, color: Colors.orange),
                    SizedBox(width: 8),
                    Text(
                      'Duration: $duration',
                      style: TextStyle(
                        color: Colors.grey.shade700,
                        fontSize: 15,
                      ),
                    ),
                  ],
                ),
              ],
            ),
            // Location row
            Row(
              children: [
                Icon(Icons.location_on_outlined, size: 18, color: Colors.green),
                const SizedBox(width: 8),
                Expanded(
                  child: Text(
                    'Location: ${applicant.location}',
                    style: TextStyle(fontSize: 15, color: Colors.grey.shade800),
                    maxLines: 4,
                    overflow: TextOverflow.ellipsis,
                  ),
                ),
              ],
            ),
            Row(
              children: [
                Icon(Icons.pin_drop_outlined, size: 18, color: Colors.red),
                SizedBox(width: 8),
                Text(
                  'Pincode: ${applicant.pincode == 'N/A' ? 'Not Available' : applicant.pincode}',
                  style: TextStyle(color: Colors.grey.shade700, fontSize: 15),
                ),
              ],
            ),

            Divider(height: 24, thickness: 1, color: Colors.grey.shade200),

            // Worker Assigned Section
            if (applicant.isAssigned) ...[
              Text(
                'Worker Assigned',
                style: TextStyle(fontSize: 14, fontWeight: FontWeight.w600),
              ),
              SizedBox(height: 12),
              Row(
                children: [
                  FutureBuilder<String?>(
                    future: Supabase.instance.client
                        .from('documents')
                        .select('photo_image_url')
                        .eq('email', applicant.email)
                        .maybeSingle()
                        .then((value) => value?['photo_image_url'] as String?),
                    builder: (context, snapshot) {
                      if (snapshot.connectionState == ConnectionState.waiting) {
                        return CircleAvatar(
                          radius: 25,
                          backgroundColor: Colors.grey.shade300,
                          child: CircularProgressIndicator(strokeWidth: 2),
                        );
                      }

                      if (snapshot.hasData &&
                          snapshot.data != null &&
                          snapshot.data!.isNotEmpty) {
                        return GestureDetector(
                          onTap:
                              () =>
                                  _showFullImageDialog(context, snapshot.data!),
                          child: CircleAvatar(
                            radius: 25,
                            backgroundImage: NetworkImage(snapshot.data!),
                          ),
                        );
                      }

                      return CircleAvatar(
                        radius: 25,
                        backgroundColor: applicant.avatarColor,
                        child: Text(
                          applicant.avatarText,
                          style: TextStyle(color: Colors.white, fontSize: 20),
                        ),
                      );
                    },
                  ),

                  SizedBox(width: 12),

                  // Worker Details
                  Expanded(
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        Text(
                          applicant.name,
                          style: TextStyle(
                            fontSize: 16,
                            fontWeight: FontWeight.w600,
                          ),
                        ),
                        SizedBox(height: 4),
                        Row(
                          children: [
                            Icon(Icons.star, size: 16, color: Colors.amber),
                            SizedBox(width: 4),
                            Text(
                              applicant.rating.toString(),
                              style: TextStyle(fontSize: 14),
                            ),
                          ],
                        ),
                        SizedBox(height: 8),
                        Row(
                          children: [
                            Icon(Icons.phone, size: 16, color: Colors.blue),
                            SizedBox(width: 4),
                            Text(applicant.phoneNumber),
                          ],
                        ),
                        SizedBox(height: 4),
                        Row(
                          children: [
                            Icon(Icons.email, size: 16, color: Colors.blue),
                            SizedBox(width: 4),
                            Text(applicant.email),
                          ],
                        ),
                      ],
                    ),
                  ),
                ],
              ),

              // Action buttons below worker info
              SizedBox(height: 16),
              // Action buttons below worker info
              SizedBox(height: 16),
              if (applicant.appstatus.toLowerCase() != "completed") ...[
                _buildCheckButton(application, controller, context),
              ],
            ] else ...[
              // No worker assigned message
              Container(
                padding: EdgeInsets.all(12),
                decoration: BoxDecoration(
                  color: Colors.grey.shade100,
                  borderRadius: BorderRadius.circular(8),
                ),
                child: Row(
                  children: [
                    Icon(Icons.info_outline, color: Colors.grey.shade600),
                    SizedBox(width: 8),
                    Text(
                      'No worker assigned yet.',
                      style: TextStyle(
                        color: Colors.grey.shade600,
                        fontSize: 14,
                      ),
                    ),
                  ],
                ),
              ),
            ],
          ],
        ),
      ),
    );
  }

  void _showFullImageDialog(BuildContext context, String imageUrl) {
    showDialog(
      context: context,
      builder:
          (_) => Dialog(
            backgroundColor: Colors.transparent,
            child: GestureDetector(
              onTap: () => Navigator.pop(context),
              child: InteractiveViewer(
                child: ClipRRect(
                  borderRadius: BorderRadius.circular(12),
                  child: Image.network(imageUrl),
                ),
              ),
            ),
          ),
    );
  }
}
