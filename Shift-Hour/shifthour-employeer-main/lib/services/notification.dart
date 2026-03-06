import 'dart:async';
import 'dart:math';
import 'package:flutter/material.dart';
import 'package:onesignal_flutter/onesignal_flutter.dart';
import 'package:supabase_flutter/supabase_flutter.dart';

class NotificationService {
  // Singleton pattern
  static final NotificationService _instance = NotificationService._internal();
  factory NotificationService() => _instance;
  NotificationService._internal();

  // Your OneSignal App ID from the configuration page
  final String _oneSignalAppId = '7d9ea1e5-74a4-4db6-ba2b-f94a22e686d0';

  // Initialize the OneSignal SDK
  Future<void> initializeOneSignal() async {
    try {
      // Enable logging for debugging (remove in production)
      OneSignal.Debug.setLogLevel(OSLogLevel.verbose);

      // Initialize OneSignal
      OneSignal.initialize(_oneSignalAppId);

      // Request permission to send notifications
      OneSignal.Notifications.requestPermission(true);

      // Set notification handlers
      _setupNotificationHandlers();

      // When user is authenticated, associate their Supabase ID with OneSignal
      _setupUserIdentification();

      print('OneSignal initialization complete');
    } catch (e) {
      print('Error during OneSignal initialization: $e');
    }
  }

  void _setupNotificationHandlers() {
    // Handle notifications when app is in foreground
    OneSignal.Notifications.addForegroundWillDisplayListener((event) {
      final notification = event.notification;
      print("Foreground notification received: ${notification.title}");

      // Handle custom data if needed
      if (notification.additionalData != null) {
        final data = notification.additionalData!;
        print("Additional data: $data");

        // Process specific notification types
        if (data['notification_type'] == 'shift_posted') {
          // Show custom UI or update state
          print("Shift posted notification received");
        }
      }
    });

    // Handle when a notification is opened
    OneSignal.Notifications.addClickListener((event) {
      print("Notification clicked: ${event.notification.title}");

      // Handle navigation based on notification type
      if (event.notification.additionalData != null) {
        final data = event.notification.additionalData!;

        if (data['notification_type'] == 'shift_posted') {
          // Navigate to relevant screen
          print("Navigating to posted shifts screen");
        }
      }
    });
  }

  void _setupUserIdentification() {
    // Listen for auth state changes in Supabase
    Supabase.instance.client.auth.onAuthStateChange.listen((data) {
      final AuthChangeEvent event = data.event;
      final Session? session = data.session;

      if (event == AuthChangeEvent.signedIn && session != null) {
        // User signed in, associate their ID with OneSignal
        _setExternalUserId(session.user.id);
      } else if (event == AuthChangeEvent.signedOut) {
        // User signed out, remove association
        _removeExternalUserId();
      }
    });

    // Check if user is already signed in
    final currentUser = Supabase.instance.client.auth.currentUser;
    if (currentUser != null) {
      _setExternalUserId(currentUser.id);
    }
  }

  Future<void> logNotificationToDatabase({
    required String userId,
    required String userType,
    required String title,
    required String message,
    required String notificationType,
    String type = 'info',
  }) async {
    final supabase = Supabase.instance.client;

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

      print("📥 Notification saved in DB for $userId");
    } catch (e) {
      print("❌ Failed to store notification in DB: $e");
    }
  }

  // Set external user ID in OneSignal (associate with Supabase user ID)
  Future<void> _setExternalUserId(String userId) async {
    try {
      // Get the subscription status - this is how we access user ID in the new API
      final bool? deviceOptedIn = OneSignal.User.pushSubscription.optedIn;

      // Check if deviceOptedIn is not null and true
      if (deviceOptedIn == true) {
        await OneSignal.login(userId);
        print("Set external user ID: $userId");

        // Update user tags for segmentation if needed
        await OneSignal.User.addTags({
          'user_type': 'employer',
          'app_version': '1.0.0',
        });
      } else {
        print(
          "User has not opted in to notifications, cannot login with OneSignal",
        );
      }
    } catch (e) {
      print("Error setting external user ID: $e");
    }
  }

  // Remove external user ID association
  Future<void> _removeExternalUserId() async {
    await OneSignal.logout();
    print("Removed external user ID");
  }

  // Method to get the OneSignal player ID (device ID)
  Future<String?> getPlayerId() async {
    // In the new API, the player ID is accessed via the subscription ID
    final userId = OneSignal.User.pushSubscription.id;
    return userId;
  }

  // Send a shift posted confirmation notification to the employer
  Future<void> sendShiftPostedNotification({
    required String shiftTitle,
    required String shiftId,
    required int numberOfPositions,
  }) async {
    try {
      final supabase = Supabase.instance.client;
      final employerId = supabase.auth.currentUser?.id;

      if (employerId == null) {
        print("Cannot send notification: No authenticated user");
        return;
      }

      // Generate a unique notification ID
      final notificationId = generateUuid();

      // Prepare notification data
      final notificationData = {
        'user_id': employerId,
        'notification_id': notificationId, // ✅ Add this
        'title': 'Shift Posted Successfully!',
        'message':
            'Your shift "$shiftTitle" and ShiftId "$shiftId" with $numberOfPositions position(s) is now live!',
        'notification_type': 'shift_posted',
        'android_channel_id': 'shifthour_general',
      };

      // If edge function isn't available yet, store directly in database
      try {
        // Insert into notifications table directly
        await supabase.from('notifications').insert({
          'user_id': employerId,
          'user_type': 'employer', // ✅ REQUIRED
          'title': 'Shift Posted Successfully!',
          'message':
              'Your shift "$shiftTitle" with $numberOfPositions position(s) is now live!',
          'notification_type': 'shift_posted',
          'type': 'info', // ✅ Optional if column exists
          'created_at': DateTime.now().toIso8601String(),
          'is_read': false,
        });

        print("Notification stored in database");
      } catch (dbError) {
        print("Error storing notification in database: $dbError");
      }

      // Call the Supabase Edge Function to send the notification
      try {
        final response = await supabase.functions.invoke(
          'send-notification',
          body: notificationData,
        );

        print("Notification sent response: ${response.data}");
      } catch (fnError) {
        print("Error calling send-notification function: $fnError");
        // This is expected if the function isn't deployed yet
      }
    } catch (e) {
      print("Error sending shift posted notification: $e");
    }
  }

  // Helper method to generate a UUID
  String generateUuid() {
    // Simple UUID v4 generator
    final random = Random();
    final hexDigits = '0123456789abcdef';
    final uuid = List<String>.filled(36, '');

    for (var i = 0; i < 36; i++) {
      if (i == 8 || i == 13 || i == 18 || i == 23) {
        uuid[i] = '-';
      } else if (i == 14) {
        uuid[i] = '4'; // Version 4 UUID
      } else if (i == 19) {
        uuid[i] = hexDigits[(random.nextInt(4) | 8)]; // Variant
      } else {
        uuid[i] = hexDigits[random.nextInt(16)];
      }
    }

    return uuid.join('');
  }
}
