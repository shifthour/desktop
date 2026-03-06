import 'package:flutter/material.dart';
import '../../config/app_theme.dart';
import '../../models/user_model.dart';
import '../../services/supabase_service.dart';

class UsersScreen extends StatefulWidget {
  const UsersScreen({super.key});

  @override
  State<UsersScreen> createState() => _UsersScreenState();
}

class _UsersScreenState extends State<UsersScreen> {
  List<UserModel> _users = [];
  bool _isLoading = true;

  @override
  void initState() {
    super.initState();
    _loadUsers();
  }

  Future<void> _loadUsers() async {
    setState(() => _isLoading = true);

    final users = await SupabaseService.getUsers();

    if (mounted) {
      setState(() {
        _users = users;
        _isLoading = false;
      });
    }
  }

  Color _getRoleColor(String role) {
    switch (role) {
      case 'super_admin':
        return Colors.purple;
      case 'ADMIN':
        return Colors.blue;
      case 'SALES_MANAGER':
        return Colors.orange;
      case 'AGENT':
        return Colors.green;
      case 'CHANNEL_PARTNER':
        return Colors.teal;
      default:
        return Colors.grey;
    }
  }

  IconData _getRoleIcon(String role) {
    switch (role) {
      case 'super_admin':
        return Icons.shield;
      case 'ADMIN':
        return Icons.admin_panel_settings;
      case 'SALES_MANAGER':
        return Icons.manage_accounts;
      case 'AGENT':
        return Icons.person;
      case 'CHANNEL_PARTNER':
        return Icons.handshake;
      default:
        return Icons.person_outline;
    }
  }

  @override
  Widget build(BuildContext context) {
    return RefreshIndicator(
      onRefresh: _loadUsers,
      child: _isLoading
          ? const Center(child: CircularProgressIndicator())
          : _users.isEmpty
              ? Center(
                  child: Column(
                    mainAxisAlignment: MainAxisAlignment.center,
                    children: [
                      Icon(
                        Icons.people_outline,
                        size: 64,
                        color: AppTheme.textMuted,
                      ),
                      const SizedBox(height: 16),
                      Text(
                        'No users found',
                        style: TextStyle(
                          color: AppTheme.textSecondary,
                          fontSize: 16,
                        ),
                      ),
                    ],
                  ),
                )
              : ListView.builder(
                  padding: const EdgeInsets.all(16),
                  itemCount: _users.length,
                  itemBuilder: (context, index) {
                    return _buildUserCard(_users[index]);
                  },
                ),
    );
  }

  Widget _buildUserCard(UserModel user) {
    final roleColor = _getRoleColor(user.role);
    final roleIcon = _getRoleIcon(user.role);

    return Card(
      margin: const EdgeInsets.only(bottom: 12),
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Row(
          children: [
            // Avatar
            CircleAvatar(
              radius: 25,
              backgroundColor: roleColor.withOpacity(0.1),
              child: Icon(roleIcon, color: roleColor, size: 24),
            ),
            const SizedBox(width: 16),

            // User Info
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    user.name,
                    style: const TextStyle(
                      fontSize: 16,
                      fontWeight: FontWeight.w600,
                    ),
                  ),
                  const SizedBox(height: 4),
                  Text(
                    user.email,
                    style: TextStyle(
                      fontSize: 13,
                      color: AppTheme.textSecondary,
                    ),
                  ),
                  if (user.phone != null) ...[
                    const SizedBox(height: 2),
                    Text(
                      user.phone!,
                      style: TextStyle(
                        fontSize: 12,
                        color: AppTheme.textMuted,
                      ),
                    ),
                  ],
                ],
              ),
            ),

            // Role Badge
            Container(
              padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 4),
              decoration: BoxDecoration(
                color: roleColor.withOpacity(0.1),
                borderRadius: BorderRadius.circular(20),
                border: Border.all(color: roleColor.withOpacity(0.3)),
              ),
              child: Text(
                user.role.replaceAll('_', ' '),
                style: TextStyle(
                  fontSize: 11,
                  color: roleColor,
                  fontWeight: FontWeight.w600,
                ),
              ),
            ),
          ],
        ),
      ),
    );
  }
}
