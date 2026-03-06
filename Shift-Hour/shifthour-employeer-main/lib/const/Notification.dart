import 'package:flutter/material.dart';
import 'package:intl/intl.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:get/get.dart';

class NotificationsScreen extends StatefulWidget {
  const NotificationsScreen({Key? key}) : super(key: key);

  @override
  State<NotificationsScreen> createState() => _NotificationsScreenState();
}

class _NotificationsScreenState extends State<NotificationsScreen> {
  final _supabase = Supabase.instance.client;
  bool _isLoading = true;
  List<Map<String, dynamic>> _notifications = [];
  int _unreadCount = 0;

  @override
  void initState() {
    super.initState();
    _fetchNotifications();
  }

  Future<void> _fetchNotifications() async {
    setState(() => _isLoading = true);

    try {
      final response = await _supabase
          .from('notifications')
          .select('*')
          .order('created_at', ascending: false)
          .limit(50);

      setState(() {
        _notifications = List<Map<String, dynamic>>.from(response);
        _unreadCount =
            _notifications.where((n) => n['is_read'] == false).length;
        _isLoading = false;
      });
    } catch (e) {
      print('Error fetching notifications: $e');
      setState(() => _isLoading = false);
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('Failed to load notifications'),
          backgroundColor: Colors.red,
        ),
      );
    }
  }

  Future<void> _markAsRead(String notificationId) async {
    try {
      await _supabase.rpc(
        'mark_notification_read',
        params: {'p_notification_id': notificationId},
      );

      setState(() {
        final index = _notifications.indexWhere(
          (n) => n['notification_id'] == notificationId,
        );
        if (index != -1) {
          _notifications[index]['is_read'] = true;
          _unreadCount =
              _notifications.where((n) => n['is_read'] == false).length;
        }
      });
    } catch (e) {
      print('Error marking notification as read: $e');
    }
  }

  Future<void> _markAllAsRead() async {
    try {
      await _supabase.rpc('mark_all_notifications_read');

      setState(() {
        for (var notification in _notifications) {
          notification['is_read'] = true;
        }
        _unreadCount = 0;
      });

      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('All notifications marked as read'),
          backgroundColor: Colors.green,
        ),
      );
    } catch (e) {
      print('Error marking all notifications as read: $e');
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('Failed to mark notifications as read'),
          backgroundColor: Colors.red,
        ),
      );
    }
  }

  String _formatTimeAgo(String dateTimeString) {
    final now = DateTime.now();
    final dateTime = DateTime.parse(dateTimeString);
    final difference = now.difference(dateTime);

    if (difference.inSeconds < 60) return 'Just now';
    if (difference.inMinutes < 60) return '${difference.inMinutes}m ago';
    if (difference.inHours < 24) return '${difference.inHours}h ago';
    if (difference.inDays < 7) return '${difference.inDays}d ago';
    return DateFormat('MMM d, y').format(dateTime);
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: const Color(0xFFF8FAFF),
      appBar: AppBar(
        title: const Text(
          'Notifications',
          style: TextStyle(color: Colors.white, fontWeight: FontWeight.bold),
        ),
        actions: [
          if (_unreadCount > 0)
            TextButton.icon(
              onPressed: _markAllAsRead,
              icon: const Icon(Icons.check_circle_outline, color: Colors.white),
              label: const Text(
                'Mark all read',
                style: TextStyle(color: Colors.white),
              ),
            ),
          IconButton(
            icon: const Icon(Icons.refresh, color: Colors.white),
            onPressed: _fetchNotifications,
          ),
        ],
      ),
      body:
          _isLoading
              ? const Center(child: CircularProgressIndicator())
              : _notifications.isEmpty
              ? _buildEmptyState()
              : _buildNotificationsList(),
    );
  }

  Widget _buildEmptyState() {
    return Center(
      child: Column(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          Icon(
            Icons.notifications_off_outlined,
            size: 64,
            color: Colors.grey.shade400,
          ),
          const SizedBox(height: 16),
          Text(
            'No notifications yet',
            style: TextStyle(
              fontSize: 18,
              fontWeight: FontWeight.w500,
              color: Colors.grey.shade700,
            ),
          ),
          const SizedBox(height: 8),
          Text(
            'You\'ll be notified about important updates here',
            style: TextStyle(fontSize: 14, color: Colors.grey.shade600),
          ),
        ],
      ),
    );
  }

  Widget _buildNotificationsList() {
    return RefreshIndicator(
      onRefresh: _fetchNotifications,
      child: ListView.separated(
        padding: const EdgeInsets.all(12),
        itemCount: _notifications.length,
        separatorBuilder: (context, index) => const Divider(height: 1),
        itemBuilder: (context, index) {
          final notification = _notifications[index];
          final bool isRead = notification['is_read'] ?? false;
          final String formattedTime = _formatTimeAgo(
            notification['created_at'],
          );
          final String type = notification['notification_type'] ?? 'general';

          IconData notificationIcon;
          Color iconColor;

          switch (type) {
            case 'shift_posted':
              notificationIcon = Icons.event_available;
              iconColor = Colors.green;
              break;
            case 'shift_applied':
              notificationIcon = Icons.person_add;
              iconColor = Colors.blue;
              break;
            case 'payment_successful':
              notificationIcon = Icons.payment;
              iconColor = Colors.purple;
              break;
            case 'check_in':
              notificationIcon = Icons.login;
              iconColor = Colors.orange;
              break;
            case 'check_out':
              notificationIcon = Icons.logout;
              iconColor = Colors.deepOrange;
              break;
            case 'check_event':
              notificationIcon = Icons.access_time;
              iconColor = Colors.indigo;
              break;
            case 'shift_cancelled':
              notificationIcon = Icons.cancel;
              iconColor = Colors.redAccent;
              break;

            default:
              notificationIcon = Icons.notifications;
              iconColor = Colors.blue.shade700;
          }

          return InkWell(
            onTap: () {
              if (!isRead) {
                _markAsRead(notification['notification_id']);
              }
            },
            child: Container(
              color: isRead ? Colors.white : Colors.blue.shade50,
              padding: const EdgeInsets.symmetric(vertical: 12, horizontal: 8),
              child: Row(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Container(
                    margin: const EdgeInsets.only(top: 4),
                    padding: const EdgeInsets.all(10),
                    decoration: BoxDecoration(
                      color: iconColor.withOpacity(0.1),
                      shape: BoxShape.circle,
                    ),
                    child: Icon(notificationIcon, color: iconColor, size: 22),
                  ),
                  const SizedBox(width: 12),
                  Expanded(
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        Row(
                          children: [
                            Expanded(
                              child: Text(
                                notification['title'] ?? 'Notification',
                                style: TextStyle(
                                  fontSize: 16,
                                  fontWeight:
                                      isRead
                                          ? FontWeight.w500
                                          : FontWeight.bold,
                                  color: Colors.black87,
                                ),
                                maxLines: 1,
                                overflow: TextOverflow.ellipsis,
                              ),
                            ),
                            Text(
                              formattedTime,
                              style: TextStyle(
                                fontSize: 12,
                                color: Colors.grey.shade600,
                              ),
                            ),
                          ],
                        ),
                        const SizedBox(height: 4),
                        Text(
                          notification['message'] ?? '',
                          style: TextStyle(
                            fontSize: 14,
                            color: Colors.black87,
                            height: 1.3,
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
      ),
    );
  }
}
