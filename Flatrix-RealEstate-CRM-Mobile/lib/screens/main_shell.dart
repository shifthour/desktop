import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import '../config/app_theme.dart';
import '../providers/auth_provider.dart';
import 'dashboard/dashboard_screen.dart';
import 'leads/leads_screen.dart';
import 'deals/deals_screen.dart';
import 'site_visits/site_visits_screen.dart';
import 'bookings/bookings_screen.dart';
import 'users/users_screen.dart';
import 'commissions/commissions_screen.dart';
import 'reports/reports_screen.dart';

class MainShell extends StatefulWidget {
  const MainShell({super.key});

  static _MainShellState? of(BuildContext context) {
    return context.findAncestorStateOfType<_MainShellState>();
  }

  @override
  State<MainShell> createState() => _MainShellState();
}

class _MainShellState extends State<MainShell> {
  int _currentIndex = 0;
  final GlobalKey<ScaffoldState> _scaffoldKey = GlobalKey<ScaffoldState>();

  void navigateToTab(int index) {
    setState(() => _currentIndex = index);
  }

  List<_NavItem> _getNavItems(bool isSuperAdmin) {
    final items = <_NavItem>[
      _NavItem(
        icon: Icons.dashboard_outlined,
        activeIcon: Icons.dashboard,
        label: 'Dashboard',
        screen: const DashboardScreen(),
      ),
      _NavItem(
        icon: Icons.people_outline,
        activeIcon: Icons.people,
        label: 'Leads',
        screen: const LeadsScreen(),
      ),
      _NavItem(
        icon: Icons.handshake_outlined,
        activeIcon: Icons.handshake,
        label: 'Deals',
        screen: const DealsScreen(),
      ),
      _NavItem(
        icon: Icons.location_on_outlined,
        activeIcon: Icons.location_on,
        label: 'Site Visits',
        screen: const SiteVisitsScreen(),
      ),
      _NavItem(
        icon: Icons.event_available_outlined,
        activeIcon: Icons.event_available,
        label: 'Bookings',
        screen: const BookingsScreen(),
      ),
    ];

    if (isSuperAdmin) {
      items.addAll([
        _NavItem(
          icon: Icons.manage_accounts_outlined,
          activeIcon: Icons.manage_accounts,
          label: 'Users',
          screen: const UsersScreen(),
          superAdminOnly: true,
        ),
        _NavItem(
          icon: Icons.payments_outlined,
          activeIcon: Icons.payments,
          label: 'Commissions',
          screen: const CommissionsScreen(),
          superAdminOnly: true,
        ),
        _NavItem(
          icon: Icons.bar_chart_outlined,
          activeIcon: Icons.bar_chart,
          label: 'Reports',
          screen: const ReportsScreen(),
          superAdminOnly: true,
        ),
      ]);
    }

    return items;
  }

  void _showLogoutDialog() {
    showDialog(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text('Logout'),
        content: const Text('Are you sure you want to logout?'),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context),
            child: const Text('Cancel'),
          ),
          TextButton(
            onPressed: () {
              Navigator.pop(context);
              context.read<AuthProvider>().logout();
            },
            child: const Text(
              'Logout',
              style: TextStyle(color: AppTheme.errorColor),
            ),
          ),
        ],
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    final user = context.watch<AuthProvider>().user;
    final isSuperAdmin = user?.isSuperAdmin ?? false;
    final navItems = _getNavItems(isSuperAdmin);

    // Ensure current index is valid
    if (_currentIndex >= navItems.length) {
      _currentIndex = 0;
    }

    return Scaffold(
      key: _scaffoldKey,
      appBar: AppBar(
        leading: IconButton(
          icon: const Icon(Icons.menu),
          onPressed: () => _scaffoldKey.currentState?.openDrawer(),
        ),
        title: Text(navItems[_currentIndex].label),
        actions: [
          IconButton(
            icon: const Icon(Icons.logout),
            tooltip: 'Logout',
            onPressed: _showLogoutDialog,
          ),
        ],
      ),
      body: IndexedStack(
        index: _currentIndex,
        children: navItems.map((item) => item.screen).toList(),
      ),
      bottomNavigationBar: isSuperAdmin
          ? null // Use drawer for super admin (too many items)
          : BottomNavigationBar(
              currentIndex: _currentIndex,
              onTap: (index) {
                setState(() => _currentIndex = index);
              },
              items: navItems
                  .map((item) => BottomNavigationBarItem(
                        icon: Icon(item.icon),
                        activeIcon: Icon(item.activeIcon),
                        label: item.label,
                      ))
                  .toList(),
            ),
      drawer: Drawer(
        child: SafeArea(
          child: Column(
            children: [
              // User Header
              Container(
                width: double.infinity,
                padding: const EdgeInsets.all(20),
                color: isSuperAdmin ? Colors.purple : AppTheme.primaryColor,
                child: Consumer<AuthProvider>(
                  builder: (context, auth, child) {
                    return Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        CircleAvatar(
                          radius: 30,
                          backgroundColor: Colors.white,
                          child: Icon(
                            isSuperAdmin ? Icons.shield : Icons.person,
                            size: 30,
                            color: isSuperAdmin ? Colors.purple : AppTheme.primaryColor,
                          ),
                        ),
                        const SizedBox(height: 12),
                        Text(
                          auth.user?.name ?? 'User',
                          style: const TextStyle(
                            color: Colors.white,
                            fontSize: 18,
                            fontWeight: FontWeight.bold,
                          ),
                        ),
                        Text(
                          auth.user?.email ?? '',
                          style: TextStyle(
                            color: Colors.white.withOpacity(0.8),
                            fontSize: 14,
                          ),
                        ),
                        const SizedBox(height: 4),
                        Container(
                          padding: const EdgeInsets.symmetric(
                              horizontal: 8, vertical: 2),
                          decoration: BoxDecoration(
                            color: Colors.white.withOpacity(0.2),
                            borderRadius: BorderRadius.circular(4),
                          ),
                          child: Row(
                            mainAxisSize: MainAxisSize.min,
                            children: [
                              if (isSuperAdmin)
                                const Icon(Icons.shield, color: Colors.white, size: 14),
                              if (isSuperAdmin) const SizedBox(width: 4),
                              Text(
                                auth.user?.role?.replaceAll('_', ' ').toUpperCase() ?? 'AGENT',
                                style: const TextStyle(
                                  color: Colors.white,
                                  fontSize: 12,
                                  fontWeight: FontWeight.w600,
                                ),
                              ),
                            ],
                          ),
                        ),
                      ],
                    );
                  },
                ),
              ),

              // Menu Items
              Expanded(
                child: ListView(
                  padding: EdgeInsets.zero,
                  children: [
                    ...navItems.asMap().entries.map((entry) {
                      final index = entry.key;
                      final item = entry.value;
                      return ListTile(
                        leading: Icon(
                          _currentIndex == index ? item.activeIcon : item.icon,
                          color: item.superAdminOnly ? Colors.purple : null,
                        ),
                        title: Text(
                          item.label,
                          style: TextStyle(
                            color: item.superAdminOnly ? Colors.purple : null,
                            fontWeight: _currentIndex == index
                                ? FontWeight.w600
                                : FontWeight.normal,
                          ),
                        ),
                        selected: _currentIndex == index,
                        selectedTileColor: (item.superAdminOnly
                                ? Colors.purple
                                : AppTheme.primaryColor)
                            .withOpacity(0.1),
                        onTap: () {
                          setState(() => _currentIndex = index);
                          Navigator.pop(context);
                        },
                      );
                    }),

                    if (isSuperAdmin) ...[
                      const Divider(),
                      Padding(
                        padding: const EdgeInsets.symmetric(
                            horizontal: 16, vertical: 8),
                        child: Text(
                          'SUPER ADMIN',
                          style: TextStyle(
                            fontSize: 12,
                            color: Colors.purple.withOpacity(0.7),
                            fontWeight: FontWeight.w600,
                          ),
                        ),
                      ),
                    ],
                  ],
                ),
              ),

              const Divider(),

              // Logout
              ListTile(
                leading: const Icon(Icons.logout, color: AppTheme.errorColor),
                title: const Text(
                  'Logout',
                  style: TextStyle(color: AppTheme.errorColor),
                ),
                onTap: () {
                  Navigator.pop(context);
                  _showLogoutDialog();
                },
              ),
              const SizedBox(height: 16),
            ],
          ),
        ),
      ),
    );
  }
}

class _NavItem {
  final IconData icon;
  final IconData activeIcon;
  final String label;
  final Widget screen;
  final bool superAdminOnly;

  _NavItem({
    required this.icon,
    required this.activeIcon,
    required this.label,
    required this.screen,
    this.superAdminOnly = false,
  });
}
