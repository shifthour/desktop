import 'package:flutter/material.dart';
import 'package:shifthour/Employer/Manage%20Jobs/manage_jobs_dashboard.dart';

import 'package:shifthour/login.dart';
import 'package:shifthour/main.dart';

class EmployerDashboard extends StatefulWidget {
  const EmployerDashboard({Key? key}) : super(key: key);

  @override
  State<EmployerDashboard> createState() => _EmployerDashboardState();
}

class _EmployerDashboardState extends State<EmployerDashboard> {
  final scaffoldKey = GlobalKey<ScaffoldState>();

  @override
  Widget build(BuildContext context) {
    return GestureDetector(
      onTap: () {
        FocusScope.of(context).unfocus();
        FocusManager.instance.primaryFocus?.unfocus();
      },
      child: Scaffold(
        key: scaffoldKey,
        backgroundColor: const Color(0xFFF0F5FF),
        // Add drawer to the scaffold
        drawer: _buildDrawer(context),
        // Add app bar with menu icon
        appBar: AppBar(
          backgroundColor: Colors.transparent,
          elevation: 0,
          leading: IconButton(
            icon: const Icon(Icons.menu, color: Color(0xFF5B6BF8), size: 28),
            onPressed: () {
              scaffoldKey.currentState?.openDrawer();
            },
          ),
          automaticallyImplyLeading: false,
        ),
        body: SafeArea(
          top: true,
          child: Column(
            mainAxisSize: MainAxisSize.max,
            children: [
              // Header
              Padding(
                padding: const EdgeInsets.fromLTRB(24, 16, 24, 16),
                child: Container(
                  width: double.infinity,
                  decoration: BoxDecoration(
                    gradient: const LinearGradient(
                      colors: [Color(0xFF5B6BF8), Color(0xFF8B65D9)],
                      begin: Alignment.topLeft,
                      end: Alignment.bottomRight,
                    ),
                    borderRadius: BorderRadius.circular(12),
                    boxShadow: [
                      BoxShadow(
                        blurRadius: 4,
                        color: const Color(0x10000000),
                        offset: const Offset(0, 2),
                      ),
                    ],
                  ),
                  child: Padding(
                    padding: const EdgeInsets.all(16),
                    child: Row(
                      mainAxisSize: MainAxisSize.max,
                      mainAxisAlignment: MainAxisAlignment.spaceBetween,
                      crossAxisAlignment: CrossAxisAlignment.center,
                      children: [
                        // Logo and App Name
                        Row(
                          children: [
                            Container(
                              width: 40,
                              height: 40,
                              decoration: const BoxDecoration(
                                color: Colors.white,
                                shape: BoxShape.circle,
                              ),
                              child: const Icon(
                                Icons.calendar_today,
                                color: Color(0xFF5B6BF8),
                                size: 24,
                              ),
                            ),
                            const SizedBox(width: 12),
                            const Text(
                              'ShiftHour',
                              style: TextStyle(
                                fontFamily: 'Inter Tight',
                                color: Colors.white,
                                fontSize: 22,
                                fontWeight: FontWeight.bold,
                              ),
                            ),
                          ],
                        ),
                        // User info and avatar
                        Row(
                          children: [
                            Column(
                              crossAxisAlignment: CrossAxisAlignment.end,
                              children: const [
                                Text(
                                  'Welcome back,',
                                  style: TextStyle(
                                    fontFamily: 'Inter',
                                    color: Colors.white70,
                                    fontSize: 12,
                                  ),
                                ),
                                Text(
                                  'Sarah Johnson',
                                  style: TextStyle(
                                    fontFamily: 'Inter',
                                    color: Colors.white,
                                    fontSize: 16,
                                    fontWeight: FontWeight.bold,
                                  ),
                                ),
                              ],
                            ),
                            const SizedBox(width: 12),
                            Container(
                              width: 45,
                              height: 45,
                              decoration: BoxDecoration(
                                color: Colors.white,
                                shape: BoxShape.circle,
                                border: Border.all(
                                  color: Colors.white,
                                  width: 2,
                                ),
                                image: const DecorationImage(
                                  fit: BoxFit.cover,
                                  image: NetworkImage(
                                    'https://source.unsplash.com/200x200/?portrait,woman',
                                  ),
                                ),
                              ),
                            ),
                          ],
                        ),
                      ],
                    ),
                  ),
                ),
              ),

              // Verification Alert
              Padding(
                padding: const EdgeInsets.fromLTRB(24, 0, 24, 16),
                child: Container(
                  width: double.infinity,
                  decoration: BoxDecoration(
                    color: Colors.amber.shade100,
                    borderRadius: BorderRadius.circular(8),
                  ),
                  child: Padding(
                    padding: const EdgeInsets.all(16),
                    child: Row(
                      mainAxisAlignment: MainAxisAlignment.spaceBetween,
                      children: [
                        Row(
                          children: [
                            Icon(
                              Icons.warning_amber_rounded,
                              color: Colors.amber.shade800,
                              size: 24,
                            ),
                            const SizedBox(width: 12),
                            const Text(
                              'Your business verification is incomplete',
                              style: TextStyle(
                                fontFamily: 'Inter',
                                fontWeight: FontWeight.w600,
                              ),
                            ),
                          ],
                        ),
                        ElevatedButton(
                          onPressed: () {
                            print('Complete verification pressed');
                          },
                          style: ElevatedButton.styleFrom(
                            backgroundColor: Colors.white,
                            foregroundColor: Colors.amber.shade800,
                            elevation: 0,
                            shape: RoundedRectangleBorder(
                              borderRadius: BorderRadius.circular(20),
                            ),
                            padding: const EdgeInsets.symmetric(horizontal: 16),
                            minimumSize: const Size(0, 36),
                          ),
                          child: const Text(
                            'Complete Now',
                            style: TextStyle(
                              fontFamily: 'Inter',
                              fontWeight: FontWeight.w600,
                            ),
                          ),
                        ),
                      ],
                    ),
                  ),
                ),
              ),

              // Main Content
              Expanded(
                child: Padding(
                  padding: const EdgeInsets.fromLTRB(24, 0, 24, 24),
                  child: SingleChildScrollView(
                    child: Column(
                      mainAxisSize: MainAxisSize.max,
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        const Text(
                          'Dashboard Overview',
                          style: TextStyle(
                            fontFamily: 'Inter Tight',
                            fontSize: 24,
                            fontWeight: FontWeight.bold,
                          ),
                        ),
                        const SizedBox(height: 16),

                        // First Row of Stats
                        Row(
                          children: [
                            // Total Jobs
                            Expanded(
                              child: _buildStatCard(
                                context,
                                icon: Icons.work_rounded,
                                iconColor: const Color(0xFF5B6BF8),
                                value: '24',
                                label: 'Total Jobs',
                                growthValue: '+10%',
                                growthColor: const Color(0xFF5B6BF8),
                              ),
                            ),
                            const SizedBox(width: 16),
                            // Active Workers
                            Expanded(
                              child: _buildStatCard(
                                context,
                                icon: Icons.people_rounded,
                                iconColor: Colors.teal,
                                value: '142',
                                label: 'Active Workers',
                                growthValue: '+12%',
                                growthColor: Colors.teal,
                              ),
                            ),
                          ],
                        ),
                        const SizedBox(height: 16),

                        // Second Row of Stats
                        Row(
                          children: [
                            // Hours Tracked
                            Expanded(
                              child: _buildStatCard(
                                context,
                                icon: Icons.timer_rounded,
                                iconColor: Colors.orange,
                                value: '1,248',
                                label: 'Hours Tracked',
                                growthValue: '+19%',
                                growthColor: Colors.orange,
                              ),
                            ),
                            const SizedBox(width: 16),
                            // Pending Payments
                            Expanded(
                              child: _buildStatCard(
                                context,
                                icon: Icons.payments_rounded,
                                iconColor: Colors.green,
                                value: '\$4,325',
                                label: 'Pending Payments',
                                growthValue: '+7%',
                                growthColor: Colors.green,
                              ),
                            ),
                          ],
                        ),
                        const SizedBox(height: 24),

                        // Recent Activity
                        Container(
                          width: double.infinity,
                          decoration: BoxDecoration(
                            color: Colors.white,
                            borderRadius: BorderRadius.circular(12),
                            boxShadow: [
                              BoxShadow(
                                blurRadius: 4,
                                color: const Color(0x10000000),
                                offset: const Offset(0, 2),
                              ),
                            ],
                          ),
                          child: Padding(
                            padding: const EdgeInsets.all(16),
                            child: Column(
                              crossAxisAlignment: CrossAxisAlignment.start,
                              children: [
                                Row(
                                  mainAxisAlignment:
                                      MainAxisAlignment.spaceBetween,
                                  children: [
                                    const Text(
                                      'Recent Activity',
                                      style: TextStyle(
                                        fontFamily: 'Inter Tight',
                                        fontSize: 18,
                                        fontWeight: FontWeight.bold,
                                      ),
                                    ),
                                    TextButton(
                                      onPressed: () {
                                        print('View All pressed');
                                      },
                                      style: TextButton.styleFrom(
                                        backgroundColor: const Color(
                                          0xFFE6EEFF,
                                        ),
                                        shape: RoundedRectangleBorder(
                                          borderRadius: BorderRadius.circular(
                                            20,
                                          ),
                                        ),
                                        padding: const EdgeInsets.symmetric(
                                          horizontal: 16,
                                          vertical: 8,
                                        ),
                                      ),
                                      child: Text(
                                        'View All',
                                        style: TextStyle(
                                          color: Colors.blue.shade700,
                                          fontFamily: 'Inter',
                                          fontWeight: FontWeight.w600,
                                        ),
                                      ),
                                    ),
                                  ],
                                ),
                                const SizedBox(height: 16),
                                _buildActivityItem(
                                  context,
                                  icon: Icons.login_rounded,
                                  iconBackgroundColor: const Color(0xFFE6EEFF),
                                  iconColor: const Color(0xFF5B6BF8),
                                  title: 'Worker Check-in',
                                  description:
                                      'John Smith checked in at Downtown Cafe',
                                  timeAgo: '2h ago',
                                ),
                                const SizedBox(height: 16),
                                _buildActivityItem(
                                  context,
                                  icon: Icons.description_outlined,
                                  iconBackgroundColor: const Color(0xFFE0F8F3),
                                  iconColor: Colors.teal,
                                  title: 'Job Application',
                                  description:
                                      '5 new applications for Barista position',
                                  timeAgo: '4h ago',
                                ),
                                const SizedBox(height: 16),
                                _buildActivityItem(
                                  context,
                                  icon: Icons.check_circle_outline,
                                  iconBackgroundColor: const Color(0xFFFFF1E6),
                                  iconColor: Colors.orange,
                                  title: 'Shift Completion',
                                  description:
                                      'Maria Garcia completed her shift at Main Street Store',
                                  timeAgo: '6h ago',
                                ),
                                const SizedBox(height: 16),
                                _buildActivityItem(
                                  context,
                                  icon: Icons.payments_outlined,
                                  iconBackgroundColor: const Color(0xFFE8F9E8),
                                  iconColor: Colors.green,
                                  title: 'Payment Processing',
                                  description:
                                      'Weekly payroll processed for 32 workers',
                                  timeAgo: 'Yesterday',
                                ),
                              ],
                            ),
                          ),
                        ),
                        const SizedBox(height: 24),

                        // Quick Actions
                        Container(
                          width: double.infinity,
                          decoration: BoxDecoration(
                            color: Colors.white,
                            borderRadius: BorderRadius.circular(12),
                            boxShadow: [
                              BoxShadow(
                                blurRadius: 4,
                                color: const Color(0x10000000),
                                offset: const Offset(0, 2),
                              ),
                            ],
                          ),
                          child: Padding(
                            padding: const EdgeInsets.all(16),
                            child: Column(
                              crossAxisAlignment: CrossAxisAlignment.start,
                              children: [
                                const Text(
                                  'Quick Actions',
                                  style: TextStyle(
                                    fontFamily: 'Inter Tight',
                                    fontSize: 18,
                                    fontWeight: FontWeight.bold,
                                  ),
                                ),
                                const SizedBox(height: 16),
                                GridView.count(
                                  crossAxisCount: 3,
                                  mainAxisSpacing: 16,
                                  crossAxisSpacing: 16,
                                  shrinkWrap: true,
                                  physics: const NeverScrollableScrollPhysics(),
                                  children: [
                                    _buildActionCard(
                                      context,
                                      icon: Icons.add_circle_outline,
                                      title: 'Post a New Job',
                                      color: const Color(0xFF5B6BF8),
                                    ),
                                    _buildActionCard(
                                      context,
                                      icon: Icons.event_note,
                                      title: 'Schedule Interviews',
                                      color: Colors.teal,
                                    ),
                                    _buildActionCard(
                                      context,
                                      icon: Icons.people_alt_outlined,
                                      title: 'View Candidates',
                                      color: Colors.orange,
                                    ),
                                    _buildActionCard(
                                      context,
                                      icon:
                                          Icons.account_balance_wallet_outlined,
                                      title: 'Manage Payments',
                                      color: Colors.green,
                                    ),
                                    _buildActionCard(
                                      context,
                                      icon: Icons.bar_chart,
                                      title: 'Generate Reports',
                                      color: Colors.amber,
                                    ),
                                    _buildActionCard(
                                      context,
                                      icon: Icons.support_agent,
                                      title: 'Contact Support',
                                      color: Colors.red,
                                    ),
                                  ],
                                ),
                              ],
                            ),
                          ),
                        ),
                        const SizedBox(height: 24),

                        // Upcoming Interviews
                        Container(
                          width: double.infinity,
                          decoration: BoxDecoration(
                            color: Colors.white,
                            borderRadius: BorderRadius.circular(12),
                            boxShadow: [
                              BoxShadow(
                                blurRadius: 4,
                                color: const Color(0x10000000),
                                offset: const Offset(0, 2),
                              ),
                            ],
                          ),
                          child: Padding(
                            padding: const EdgeInsets.all(16),
                            child: Column(
                              crossAxisAlignment: CrossAxisAlignment.start,
                              children: [
                                Row(
                                  mainAxisAlignment:
                                      MainAxisAlignment.spaceBetween,
                                  children: [
                                    const Text(
                                      'Upcoming Interviews',
                                      style: TextStyle(
                                        fontFamily: 'Inter Tight',
                                        fontSize: 18,
                                        fontWeight: FontWeight.bold,
                                      ),
                                    ),
                                    TextButton(
                                      onPressed: () {
                                        print('Schedule More pressed');
                                      },
                                      style: TextButton.styleFrom(
                                        foregroundColor: const Color(
                                          0xFF5B6BF8,
                                        ),
                                      ),
                                      child: const Text(
                                        'Schedule More',
                                        style: TextStyle(
                                          fontFamily: 'Inter',
                                          fontWeight: FontWeight.w600,
                                        ),
                                      ),
                                    ),
                                  ],
                                ),
                                const SizedBox(height: 16),
                                _buildInterviewCard(
                                  context,
                                  name: 'David Wilson',
                                  position: 'Barista',
                                  date: 'Today',
                                  time: '2:30 PM',
                                  imageUrl:
                                      'https://source.unsplash.com/200x200/?portrait,man',
                                ),
                                const SizedBox(height: 16),
                                _buildInterviewCard(
                                  context,
                                  name: 'Emily Rodriguez',
                                  position: 'Cashier',
                                  date: 'Tomorrow',
                                  time: '10:00 AM',
                                  imageUrl:
                                      'https://source.unsplash.com/200x200/?portrait,woman,2',
                                ),
                              ],
                            ),
                          ),
                        ),
                      ],
                    ),
                  ),
                ),
              ),

              // Bottom Navigation
              Container(
                width: double.infinity,
                height: 80,
                decoration: BoxDecoration(
                  color: Colors.white,
                  boxShadow: [
                    BoxShadow(
                      blurRadius: 4,
                      color: const Color(0x10000000),
                      offset: const Offset(0, -2),
                    ),
                  ],
                ),
                child: Row(
                  mainAxisAlignment: MainAxisAlignment.spaceAround,
                  children: [
                    _buildNavItem(
                      context,
                      icon: Icons.dashboard_rounded,
                      label: 'Dashboard',
                      isSelected: true,
                    ),
                    _buildNavItem(
                      context,
                      icon: Icons.work_outline_rounded,
                      label: 'Jobs',
                    ),
                    _buildNavItem(
                      context,
                      icon: Icons.people_outline_rounded,
                      label: 'Workers',
                    ),
                    _buildNavItem(
                      context,
                      icon: Icons.account_balance_wallet_outlined,
                      label: 'Payments',
                    ),
                    _buildNavItem(
                      context,
                      icon: Icons.person_outline_rounded,
                      label: 'Profile',
                    ),
                  ],
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }

  Widget _buildDrawer(BuildContext context) {
    return Drawer(
      child: Container(
        color: Colors.white,
        child: ListView(
          padding: EdgeInsets.zero,
          children: [
            DrawerHeader(
              decoration: const BoxDecoration(
                gradient: LinearGradient(
                  colors: [Color(0xFF5B6BF8), Color(0xFF8B65D9)],
                  begin: Alignment.topLeft,
                  end: Alignment.bottomRight,
                ),
              ),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Row(
                    children: [
                      Container(
                        width: 60,
                        height: 60,
                        decoration: BoxDecoration(
                          color: Colors.white,
                          shape: BoxShape.circle,
                          border: Border.all(color: Colors.white, width: 2),
                          image: const DecorationImage(
                            fit: BoxFit.cover,
                            image: NetworkImage(
                              'https://source.unsplash.com/200x200/?portrait,woman',
                            ),
                          ),
                        ),
                      ),
                      const SizedBox(width: 12),
                      Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: const [
                          Text(
                            'Sarah Johnson',
                            style: TextStyle(
                              color: Colors.white,
                              fontSize: 18,
                              fontWeight: FontWeight.bold,
                            ),
                          ),
                          Text(
                            'Business Owner',
                            style: TextStyle(
                              color: Colors.white70,
                              fontSize: 14,
                            ),
                          ),
                        ],
                      ),
                    ],
                  ),
                  const SizedBox(height: 16),
                  Row(
                    children: const [
                      Icon(Icons.business, color: Colors.white, size: 16),
                      SizedBox(width: 8),
                      Text(
                        'Acme Coffee Shop',
                        style: TextStyle(color: Colors.white, fontSize: 14),
                      ),
                    ],
                  ),
                ],
              ),
            ),
            _buildDrawerItem(
              context,
              icon: Icons.dashboard_rounded,
              title: 'Dashboard',
              isSelected: true,
              onTap: () {
                Navigator.pop(context); // Close the drawer
              },
            ),
            _buildDrawerItem(
              context,
              icon: Icons.work_outline_rounded,
              title: 'Manage Jobs',
              onTap: () {
                Navigator.pop(context);
                Navigator.push(
                  context,
                  MaterialPageRoute(
                    builder: (context) => const Manage_Jobs_HomePage(),
                  ),
                ); // Close the drawer
                // Navigate to jobs page
              },
            ),
            _buildDrawerItem(
              context,
              icon: Icons.people_outline_rounded,
              title: 'Workers',
              onTap: () {
                // Navigator.pop(context);
                // Close the drawer
                // Navigate to workers page
              },
            ),
            _buildDrawerItem(
              context,
              icon: Icons.payments_outlined,
              title: 'Payments',
              onTap: () {
                Navigator.pop(context); // Close the drawer
                // Navigate to payments page
              },
            ),
            _buildDrawerItem(
              context,
              icon: Icons.person_outline,
              title: 'Company Profile',
              onTap: () {
                Navigator.pop(context); // Close the drawer
                // Navigate to profile page
              },
            ),
            _buildDrawerItem(
              context,
              icon: Icons.settings_outlined,
              title: 'Settings',
              onTap: () {
                Navigator.pop(context); // Close the drawer
                // Navigate to settings page
              },
            ),
            const Divider(thickness: 1),
            _buildDrawerItem(
              context,
              icon: Icons.help_outline,
              title: 'Help & Support',
              onTap: () {
                Navigator.pop(context); // Close the drawer
                // Navigate to help page
              },
            ),
            _buildDrawerItem(
              context,
              icon: Icons.logout,
              title: 'Logout',
              onTap: () {
                // Show confirmation dialog
                showDialog(
                  context: context,
                  builder: (BuildContext context) {
                    return AlertDialog(
                      title: const Text('Logout'),
                      content: const Text('Are you sure you want to logout?'),
                      actions: [
                        TextButton(
                          onPressed: () {
                            Navigator.of(context).pop(); // Close dialog
                          },
                          child: const Text('Cancel'),
                        ),
                        TextButton(
                          onPressed: () {
                            Navigator.of(context).pop(); // Close dialog
                            // Navigate to login screen
                            Navigator.of(context).pushAndRemoveUntil(
                              MaterialPageRoute(
                                builder: (context) => const WorkerLoginPage(),
                              ),
                              (Route<dynamic> route) => false,
                            );

                            // Alternatively, you could use pushReplacement if you're not using named routes:
                            // Navigator.of(context).pushAndRemoveUntil(
                            //   MaterialPageRoute(builder: (context) => LoginScreen()),
                            //   (Route<dynamic> route) => false,
                            // );
                          },
                          style: TextButton.styleFrom(
                            foregroundColor: Colors.red,
                          ),
                          child: const Text('Logout'),
                        ),
                      ],
                    );
                  },
                );
              },
            ),
          ],
        ),
      ),
    );
  }

  // Helper method to build drawer items
  Widget _buildDrawerItem(
    BuildContext context, {
    required IconData icon,
    required String title,
    bool isSelected = false,
    required VoidCallback onTap,
  }) {
    return ListTile(
      leading: Icon(
        icon,
        color: isSelected ? const Color(0xFF5B6BF8) : Colors.grey.shade700,
        size: 24,
      ),
      title: Text(
        title,
        style: TextStyle(
          fontFamily: 'Inter',
          fontSize: 16,
          fontWeight: isSelected ? FontWeight.w600 : FontWeight.normal,
          color: isSelected ? const Color(0xFF5B6BF8) : Colors.grey.shade800,
        ),
      ),
      onTap: onTap,
      tileColor: isSelected ? const Color(0xFFEEF1FF) : null,
      shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(8)),
      contentPadding: const EdgeInsets.symmetric(horizontal: 20, vertical: 4),
    );
  }

  Widget _buildNavItem(
    BuildContext context, {
    required IconData icon,
    required String label,
    bool isSelected = false,
  }) {
    return InkWell(
      onTap: () {
        print('$label tab pressed');
      },
      child: Column(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          Icon(
            icon,
            color: isSelected ? const Color(0xFF5B6BF8) : Colors.grey.shade600,
            size: 24,
          ),
          const SizedBox(height: 4),
          Text(
            label,
            style: TextStyle(
              fontFamily: 'Inter',
              fontSize: 12,
              color:
                  isSelected ? const Color(0xFF5B6BF8) : Colors.grey.shade600,
              fontWeight: isSelected ? FontWeight.w600 : FontWeight.normal,
            ),
          ),
        ],
      ),
    );
  }

  // Helper methods to build UI components

  Widget _buildStatCard(
    BuildContext context, {
    required IconData icon,
    required Color iconColor,
    required String value,
    required String label,
    required String growthValue,
    required Color growthColor,
  }) {
    return Container(
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(12),
        boxShadow: [
          BoxShadow(
            blurRadius: 4,
            color: const Color(0x10000000),
            offset: const Offset(0, 2),
          ),
        ],
      ),
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              mainAxisAlignment: MainAxisAlignment.spaceBetween,
              children: [
                Icon(icon, color: iconColor, size: 32),
                Container(
                  padding: const EdgeInsets.symmetric(
                    horizontal: 8,
                    vertical: 4,
                  ),
                  decoration: BoxDecoration(
                    color: growthColor.withOpacity(0.1),
                    borderRadius: BorderRadius.circular(16),
                  ),
                  child: Text(
                    growthValue,
                    style: TextStyle(
                      color: growthColor,
                      fontFamily: 'Inter',
                      fontSize: 12,
                      fontWeight: FontWeight.w600,
                    ),
                  ),
                ),
              ],
            ),
            const SizedBox(height: 12),
            Text(
              value,
              style: const TextStyle(
                fontFamily: 'Inter Tight',
                fontSize: 24,
                fontWeight: FontWeight.bold,
              ),
            ),
            const SizedBox(height: 4),
            Text(
              label,
              style: TextStyle(
                fontFamily: 'Inter',
                color: Colors.grey.shade600,
              ),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildActivityItem(
    BuildContext context, {
    required IconData icon,
    required Color iconBackgroundColor,
    required Color iconColor,
    required String title,
    required String description,
    required String timeAgo,
  }) {
    return Container(
      decoration: BoxDecoration(
        color: const Color(0xFFF5F8FF),
        borderRadius: BorderRadius.circular(8),
      ),
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Row(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Container(
              width: 40,
              height: 40,
              decoration: BoxDecoration(
                color: iconBackgroundColor,
                shape: BoxShape.circle,
              ),
              child: Icon(icon, color: iconColor, size: 20),
            ),
            const SizedBox(width: 12),
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Row(
                    mainAxisAlignment: MainAxisAlignment.spaceBetween,
                    children: [
                      Text(
                        title,
                        style: const TextStyle(
                          fontFamily: 'Inter',
                          fontSize: 16,
                          fontWeight: FontWeight.w600,
                        ),
                      ),
                      Text(
                        timeAgo,
                        style: TextStyle(
                          fontFamily: 'Inter',
                          fontSize: 12,
                          color: Colors.grey.shade600,
                        ),
                      ),
                    ],
                  ),
                  const SizedBox(height: 4),
                  Text(
                    description,
                    style: TextStyle(
                      fontFamily: 'Inter',
                      color: Colors.grey.shade600,
                    ),
                  ),
                ],
              ),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildActionCard(
    BuildContext context, {
    required IconData icon,
    required String title,
    required Color color,
  }) {
    return InkWell(
      onTap: () {
        print('$title pressed');
      },
      child: Container(
        decoration: BoxDecoration(
          color: Colors.white,
          border: Border.all(color: Colors.grey.shade200),
          borderRadius: BorderRadius.circular(8),
        ),
        child: Padding(
          padding: const EdgeInsets.all(8),
          child: Column(
            mainAxisAlignment: MainAxisAlignment.center,
            children: [
              Icon(icon, color: color, size: 32),
              const SizedBox(height: 8),
              Text(
                title,
                textAlign: TextAlign.center,
                style: const TextStyle(
                  fontFamily: 'Inter',
                  fontSize: 12,
                  fontWeight: FontWeight.w600,
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }

  Widget _buildInterviewCard(
    BuildContext context, {
    required String name,
    required String position,
    required String date,
    required String time,
    required String imageUrl,
  }) {
    return Container(
      decoration: BoxDecoration(
        color: const Color(0xFFF5F8FF),
        borderRadius: BorderRadius.circular(8),
      ),
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Row(
          children: [
            // Candidate Image
            Container(
              width: 50,
              height: 50,
              decoration: BoxDecoration(
                shape: BoxShape.circle,
                image: DecorationImage(
                  fit: BoxFit.cover,
                  image: NetworkImage(imageUrl),
                ),
              ),
            ),
            const SizedBox(width: 16),
            // Candidate Info
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    name,
                    style: const TextStyle(
                      fontFamily: 'Inter',
                      fontSize: 16,
                      fontWeight: FontWeight.w600,
                    ),
                  ),
                  Text(
                    'Applying for $position',
                    style: TextStyle(
                      fontFamily: 'Inter',
                      color: Colors.grey.shade600,
                    ),
                  ),
                ],
              ),
            ),
            // Interview Time
            Column(
              crossAxisAlignment: CrossAxisAlignment.end,
              children: [
                Container(
                  padding: const EdgeInsets.symmetric(
                    horizontal: 8,
                    vertical: 4,
                  ),
                  decoration: BoxDecoration(
                    color:
                        date == 'Today'
                            ? Colors.green.shade100
                            : Colors.blue.shade100,
                    borderRadius: BorderRadius.circular(16),
                  ),
                  child: Text(
                    date,
                    style: TextStyle(
                      fontFamily: 'Inter',
                      fontSize: 12,
                      fontWeight: FontWeight.w600,
                      color:
                          date == 'Today'
                              ? Colors.green.shade700
                              : Colors.blue.shade700,
                    ),
                  ),
                ),
                const SizedBox(height: 4),
                Text(
                  time,
                  style: const TextStyle(
                    fontFamily: 'Inter',
                    fontWeight: FontWeight.w600,
                  ),
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }
}
