import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import '../../config/app_theme.dart';
import '../../providers/auth_provider.dart';
import '../../services/supabase_service.dart';
import '../leads/add_lead_screen.dart';
import '../leads/today_followups_screen.dart';
import '../main_shell.dart';

class DashboardScreen extends StatefulWidget {
  const DashboardScreen({super.key});

  @override
  State<DashboardScreen> createState() => _DashboardScreenState();
}

class _DashboardScreenState extends State<DashboardScreen> {
  Map<String, int> _stats = {};
  bool _isLoading = true;

  @override
  void initState() {
    super.initState();
    _loadStats();
  }

  Future<void> _loadStats() async {
    setState(() => _isLoading = true);

    // Always show total stats for all leads (no user filter)
    final stats = await SupabaseService.getDashboardStats();

    if (mounted) {
      setState(() {
        _stats = stats;
        _isLoading = false;
      });
    }
  }

  @override
  Widget build(BuildContext context) {
    final user = context.watch<AuthProvider>().user;

    return RefreshIndicator(
      onRefresh: _loadStats,
      child: _isLoading
          ? const Center(child: CircularProgressIndicator())
          : SingleChildScrollView(
              physics: const AlwaysScrollableScrollPhysics(),
              padding: const EdgeInsets.all(16),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  // Welcome message
                  Text(
                    'Welcome, ${user?.name ?? 'User'}',
                    style: Theme.of(context).textTheme.titleMedium?.copyWith(
                          color: AppTheme.textSecondary,
                        ),
                  ),
                  const SizedBox(height: 16),

                  // Stats Grid
                  _buildStatsGrid(),
                  const SizedBox(height: 24),

                  // Quick Actions
                  Text(
                    'Quick Actions',
                    style: Theme.of(context).textTheme.titleMedium?.copyWith(
                          fontWeight: FontWeight.bold,
                        ),
                  ),
                  const SizedBox(height: 12),
                  _buildQuickActions(),
                ],
              ),
            ),
    );
  }

  Widget _buildStatsGrid() {
    return GridView.count(
      crossAxisCount: 2,
      shrinkWrap: true,
      physics: const NeverScrollableScrollPhysics(),
      mainAxisSpacing: 12,
      crossAxisSpacing: 12,
      childAspectRatio: 1.5,
      children: [
        _buildStatCard(
          title: 'Total Leads',
          value: _stats['totalLeads']?.toString() ?? '0',
          icon: Icons.people_outline,
          color: AppTheme.primaryColor,
        ),
        _buildStatCard(
          title: 'New Leads',
          value: _stats['newLeads']?.toString() ?? '0',
          icon: Icons.person_add_outlined,
          color: Colors.blue,
        ),
        _buildStatCard(
          title: 'Contacted',
          value: _stats['contactedLeads']?.toString() ?? '0',
          icon: Icons.phone_outlined,
          color: Colors.orange,
        ),
        _buildStatCard(
          title: 'Qualified',
          value: _stats['qualifiedLeads']?.toString() ?? '0',
          icon: Icons.verified_outlined,
          color: Colors.green,
        ),
        _buildStatCard(
          title: 'Site Visits',
          value: _stats['siteVisitsCompleted']?.toString() ?? '0',
          icon: Icons.location_on_outlined,
          color: Colors.purple,
        ),
        _buildStatCard(
          title: 'Booked',
          value: _stats['booked']?.toString() ?? '0',
          icon: Icons.check_circle_outline,
          color: AppTheme.successColor,
        ),
      ],
    );
  }

  Widget _buildStatCard({
    required String title,
    required String value,
    required IconData icon,
    required Color color,
  }) {
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          mainAxisAlignment: MainAxisAlignment.spaceBetween,
          children: [
            Row(
              mainAxisAlignment: MainAxisAlignment.spaceBetween,
              children: [
                Container(
                  padding: const EdgeInsets.all(8),
                  decoration: BoxDecoration(
                    color: color.withOpacity(0.1),
                    borderRadius: BorderRadius.circular(8),
                  ),
                  child: Icon(icon, color: color, size: 20),
                ),
              ],
            ),
            Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(
                  value,
                  style: Theme.of(context).textTheme.headlineSmall?.copyWith(
                        fontWeight: FontWeight.bold,
                        color: AppTheme.textPrimary,
                      ),
                ),
                Text(
                  title,
                  style: Theme.of(context).textTheme.bodySmall?.copyWith(
                        color: AppTheme.textSecondary,
                      ),
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildQuickActions() {
    return Column(
      children: [
        _buildActionTile(
          icon: Icons.person_add,
          title: 'Add New Lead',
          subtitle: 'Create a new lead entry',
          color: AppTheme.primaryColor,
          onTap: () {
            Navigator.push(
              context,
              MaterialPageRoute(
                builder: (context) => const AddLeadScreen(),
              ),
            ).then((result) {
              if (result == true) {
                _loadStats();
              }
            });
          },
        ),
        const SizedBox(height: 8),
        _buildActionTile(
          icon: Icons.calendar_today,
          title: 'Today\'s Follow-ups',
          subtitle: 'View scheduled follow-ups',
          color: Colors.orange,
          onTap: () {
            Navigator.push(
              context,
              MaterialPageRoute(
                builder: (context) => const TodayFollowupsScreen(),
              ),
            );
          },
        ),
        const SizedBox(height: 8),
        _buildActionTile(
          icon: Icons.location_on,
          title: 'Site Visits',
          subtitle: 'Manage site visits',
          color: Colors.purple,
          onTap: () {
            // Navigate to Site Visits tab (index 3)
            MainShell.of(context)?.navigateToTab(3);
          },
        ),
      ],
    );
  }

  Widget _buildActionTile({
    required IconData icon,
    required String title,
    required String subtitle,
    required Color color,
    required VoidCallback onTap,
  }) {
    return Card(
      child: ListTile(
        leading: Container(
          padding: const EdgeInsets.all(10),
          decoration: BoxDecoration(
            color: color.withOpacity(0.1),
            borderRadius: BorderRadius.circular(10),
          ),
          child: Icon(icon, color: color),
        ),
        title: Text(
          title,
          style: const TextStyle(fontWeight: FontWeight.w600),
        ),
        subtitle: Text(
          subtitle,
          style: TextStyle(color: AppTheme.textSecondary, fontSize: 12),
        ),
        trailing: Icon(
          Icons.chevron_right,
          color: AppTheme.textMuted,
        ),
        onTap: onTap,
      ),
    );
  }
}
