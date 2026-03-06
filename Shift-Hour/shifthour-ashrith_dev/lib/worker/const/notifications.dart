import 'package:flutter/material.dart';
import 'package:get/get.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:intl/intl.dart';

class NotificationsScreen extends StatefulWidget {
  const NotificationsScreen({Key? key}) : super(key: key);

  @override
  State<NotificationsScreen> createState() => _NotificationsScreenState();
}

class _NotificationsScreenState extends State<NotificationsScreen> {
  final supabase = Supabase.instance.client;

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Notifications'),
        actions: [
          IconButton(
            icon: const Icon(Icons.done_all),
            tooltip: 'Mark all as read',
            onPressed: _markAllAsRead,
          ),
        ],
      ),
      body: FutureBuilder<List<Map<String, dynamic>>>(
        future: _getNotifications(),
        builder: (context, snapshot) {
          if (snapshot.connectionState == ConnectionState.waiting) {
            return const Center(child: CircularProgressIndicator());
          }

          if (snapshot.hasError) {
            return Center(child: Text('Error: ${snapshot.error}'));
          }

          final notifications = snapshot.data ?? [];

          if (notifications.isEmpty) {
            return const Center(
              child: Text(
                'No notifications yet',
                style: TextStyle(fontSize: 16, color: Colors.grey),
              ),
            );
          }

          return ListView.builder(
            itemCount: notifications.length,
            itemBuilder: (context, index) {
              final notification = notifications[index];
              final date =
                  DateTime.tryParse(notification['created_at'] ?? '') ??
                  DateTime.now();
              final formattedDate = DateFormat('MMM d, h:mm a').format(date);

              final additionalData =
                  notification['data'] ?? notification['additional_data'];
              final shiftId = additionalData?['shift_id']?.toString();

              return Card(
                margin: const EdgeInsets.symmetric(horizontal: 12, vertical: 6),
                child: ListTile(
                  leading: CircleAvatar(
                    backgroundColor: Colors.blue.shade100,
                    child: const Icon(Icons.notifications, color: Colors.blue),
                  ),
                  title: Text(
                    notification['title'] ?? 'New notification',
                    style: TextStyle(
                      fontWeight:
                          notification['is_read'] == true
                              ? FontWeight.normal
                              : FontWeight.bold,
                    ),
                  ),
                  subtitle: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text(notification['message'] ?? ''),
                      if (shiftId != null && shiftId.isNotEmpty) ...[
                        const SizedBox(height: 4),
                        Text(
                          'Shift ID: $shiftId',
                          style: const TextStyle(
                            fontSize: 12,
                            color: Colors.grey,
                          ),
                        ),
                      ],
                      const SizedBox(height: 4),
                      Text(
                        formattedDate,
                        style: const TextStyle(
                          fontSize: 12,
                          color: Colors.grey,
                        ),
                      ),
                    ],
                  ),
                  trailing:
                      notification['is_read'] == true
                          ? null
                          : Container(
                            width: 12,
                            height: 12,
                            decoration: const BoxDecoration(
                              color: Colors.blue,
                              shape: BoxShape.circle,
                            ),
                          ),
                  onTap: () {
                    _markAsRead(notification['id']);
                    Get.to(
                      () => const NotificationDetailsScreen(),
                      arguments: notification['id'],
                    );
                  },
                ),
              );
            },
          );
        },
      ),
    );
  }

  Future<void> _markAllAsRead() async {
    final userId = supabase.auth.currentUser?.id;
    if (userId != null) {
      await supabase
          .from('notifications')
          .update({'is_read': true})
          .eq('user_id', userId)
          .eq('is_read', false);
      setState(() {});
    }
  }

  Future<void> _markAsRead(String notificationId) async {
    await supabase
        .from('notifications')
        .update({'is_read': true})
        .eq('id', notificationId);
    setState(() {});
  }

  Future<List<Map<String, dynamic>>> _getNotifications() async {
    final userId = supabase.auth.currentUser?.id;
    if (userId == null) return [];

    final response = await supabase
        .from('notifications')
        .select()
        .eq('user_id', userId)
        .order('created_at', ascending: false);

    return List<Map<String, dynamic>>.from(response);
  }
}

class NotificationDetailsScreen extends StatelessWidget {
  const NotificationDetailsScreen({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    final notificationId = Get.arguments as String?;

    return Scaffold(
      appBar: AppBar(title: const Text('Notification Details')),
      body: Center(
        child: Text(
          notificationId != null
              ? 'Notification ID: $notificationId'
              : 'No ID provided',
        ),
      ),
    );
  }
}
