import 'package:flutter/material.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:get/get.dart';

class NotificationBadge extends StatelessWidget {
  const NotificationBadge({
    Key? key,
    required this.child,
    this.showZero = false,
  }) : super(key: key);

  final Widget child;
  final bool showZero;

  @override
  Widget build(BuildContext context) {
    return FutureBuilder<int>(
      future: _getUnreadCount(),
      builder: (context, snapshot) {
        final unreadCount = snapshot.data ?? 0;

        if (unreadCount == 0 && !showZero) {
          return child;
        }

        return Stack(
          clipBehavior: Clip.none,
          children: [
            child,
            Positioned(
              top: -6,
              right: -6,
              child: Container(
                padding: const EdgeInsets.all(4),
                decoration: BoxDecoration(
                  color: Colors.red,
                  shape: BoxShape.circle,
                  border: Border.all(color: Colors.white, width: 1.5),
                ),
                constraints: const BoxConstraints(minWidth: 16, minHeight: 16),
                child: Text(
                  unreadCount.toString(),
                  style: const TextStyle(
                    color: Colors.white,
                    fontSize: 10,
                    fontWeight: FontWeight.bold,
                  ),
                  textAlign: TextAlign.center,
                ),
              ),
            ),
          ],
        );
      },
    );
  }

  Future<int> _getUnreadCount() async {
    try {
      final supabase = Supabase.instance.client;

      final response =
          await supabase
              .from('unread_notification_counts')
              .select('unread_count')
              .single();

      return response['unread_count'] ?? 0;
    } catch (e) {
      print('Error fetching unread count: $e');
      return 0;
    }
  }
}

// Add this extension to be used in the dashboard for the notification button
extension NotificationsIconButton on IconButton {
  Widget withNotificationBadge() {
    return NotificationBadge(child: this);
  }
}

// Usage example for EmployerDashboard:
/*
  // In your app bar actions:
  actions: [
    IconButton(
      icon: const Icon(Icons.notifications),
      onPressed: () => Get.toNamed('/notifications'),
    ).withNotificationBadge(),
    // Other actions...
  ],
*/
