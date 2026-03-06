import 'dart:async';
import 'dart:math';

import 'package:flutter/material.dart';
import 'package:intl/intl.dart';
import 'package:shifthour/utils/app_constants.dart';
import 'package:shifthour/utils/supabase_extensions.dart';
import 'package:shifthour/worker/Earnings/earnings_dashboard.dart';
import 'package:shifthour/worker/Find%20Jobs/Find_Jobs.dart';
import 'package:shifthour/worker/const/Botton_Navigation.dart';
import 'package:shifthour/worker/const/Standard_Appbar.dart';
import 'package:shifthour/worker/my_profile.dart';
import 'package:shifthour/worker/services/services.dart';
import 'package:shifthour/worker/shifts/view_shifts.dart' show ShiftDetailsPage;
import 'package:shifthour/worker/worker_dashboard.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:url_launcher/url_launcher.dart';

class MyShiftsPage extends StatefulWidget {
  final dynamic initialTab; // Change to dynamic to catch any unexpected type

  const MyShiftsPage({
    Key? key,
    this.initialTab = 0, // Default to 0 if not specified
  }) : super(key: key);

  @override
  State<MyShiftsPage> createState() => _MyShiftsPageState();
}

class _MyShiftsPageState extends State<MyShiftsPage>
    with SingleTickerProviderStateMixin, NavigationMixin {
  late TabController _tabController;
  final TextEditingController _searchController = TextEditingController();
  String _dateFilter = 'All Dates';

  // Dynamic data counts and lists
  int _upcomingCount = 0;
  int _completedCount = 0;
  int _cancelledCount = 0;
  List<Map<String, dynamic>> _upcomingShifts = [];
  List<Map<String, dynamic>> _completedShifts = [];
  List<Map<String, dynamic>> _cancelledShifts = [];

  // Filtered lists for search
  List<Map<String, dynamic>> _filteredUpcomingShifts = [];
  List<Map<String, dynamic>> _filteredCompletedShifts = [];
  List<Map<String, dynamic>> _filteredCancelledShifts = [];
  List<Map<String, dynamic>> _inProgressShifts = [];
  List<Map<String, dynamic>> _filteredInProgressShifts = [];

  // Loading and error states
  bool _isLoading = true;
  String _errorMessage = '';

  final ScrollController _scrollController = ScrollController();
  final GlobalKey<NavigatorState> _navigatorKey = GlobalKey<NavigatorState>();

  @override
  void initState() {
    super.initState();
    setCurrentTab(2);
    // Register the scroll controller with the navigation system
    setPageScrollController(_scrollController);
    // Extensive logging and type checking
    print('Initial tab received: ${widget.initialTab}');
    print('Initial tab type: ${widget.initialTab.runtimeType}');

    // Safe conversion with extensive error handling
    int validInitialTab = 0;
    try {
      // Handle different possible input types
      if (widget.initialTab == null) {
        validInitialTab = 0;
      } else if (widget.initialTab is int) {
        validInitialTab = widget.initialTab as int;
      } else if (widget.initialTab is String) {
        validInitialTab = int.tryParse(widget.initialTab) ?? 0;
      } else {
        // Fallback to 0 for any unexpected type
        validInitialTab = 0;
      }

      // Ensure tab is within valid range
      validInitialTab =
          (validInitialTab >= 0 && validInitialTab < 4) ? validInitialTab : 0;
    } catch (e) {
      print('Error processing initial tab: $e');
      validInitialTab = 0;
    }

    print('Validated initial tab: $validInitialTab');

    // Create TabController with 4 tabs
    _tabController = TabController(
      length: 3, // Changed from 3 to 4
      vsync: this,
      initialIndex: validInitialTab,
    );

    _fetchShifts();
  }

  Future<void> _fetchShifts() async {
    setState(() {
      _isLoading = true;
      _errorMessage = '';
    });

    try {
      // Get the current user
      final user = Supabase.instance.client.auth.currentUser;
      if (user == null) {
        throw Exception('No authenticated user found');
      }

      // Fetch shifts with a proper join (with pagination for performance)
      final response = await Supabase.instance.client
          .from('worker_job_applications')
          .select('''
        *,
        worker_job_listings (
          id,
          supervisor_name,
          supervisor_phone,
          supervisor_email,
           job_pincode,
           category
        )
      ''')
          .eq('user_id', user.id)
          .order('date', ascending: true)
          .limit(100) // Limit to 100 most recent shifts for performance
          .withTimeout(timeout: TimeoutDurations.query);

      // Print out detailed debug information
      print('DEBUG: Raw Response Length: ${response.length}');
      response.forEach((shift) {
        print('DEBUG: Full Shift Details:');
        print('Shift Data: $shift');
        print('Application Status: ${shift['application_status']}');
        print('Worker Job Listings: ${shift['worker_job_listings']}');
      });

      // Categorize shifts
      final shifts = List<Map<String, dynamic>>.from(response);
      setState(() {
        // Enhanced status categorization with lowercase checks
        _upcomingShifts =
            shifts.where((shift) {
              final status = (shift['application_status'] ?? '').toLowerCase();
              return status == 'upcoming' ||
                  status == 'confirmed' ||
                  status == 'pending';
            }).toList();

        _inProgressShifts =
            shifts.where((shift) {
              final status = (shift['application_status'] ?? '').toLowerCase();
              return status == 'in progress';
            }).toList();

        _completedShifts =
            shifts.where((shift) {
              final status = (shift['application_status'] ?? '').toLowerCase();
              return status == 'completed';
            }).toList();

        // _cancelledShifts =
        //   shifts.where((shift) {
        //   final status = (shift['application_status'] ?? '').toLowerCase();
        // return status == 'cancelled';
        //}).toList();

        // Initialize filtered lists
        _filteredUpcomingShifts = _upcomingShifts;
        _filteredInProgressShifts = _inProgressShifts;
        _filteredCompletedShifts = _completedShifts;
        // _filteredCancelledShifts = _cancelledShifts;

        _upcomingCount = _upcomingShifts.length;
        _completedCount = _completedShifts.length;
        // _cancelledCount = _cancelledShifts.length;
        _isLoading = false;
      });
    } on PostgrestException catch (e) {
      setState(() {
        _errorMessage = 'Database error: ${e.message}. Please try again.';
        _isLoading = false;
      });
      print('Supabase error fetching shifts: ${e.message}');
    } on TimeoutException catch (e) {
      setState(() {
        _errorMessage = 'Request timed out. Please check your connection.';
        _isLoading = false;
      });
      print('Timeout error fetching shifts: $e');
    } catch (e) {
      setState(() {
        _errorMessage = 'Failed to load shifts. Please try again.';
        _isLoading = false;
      });
      print('Error fetching shifts: $e');
    }
  }

  void _filterShifts(String query) {
    setState(() {
      _filteredUpcomingShifts =
          _upcomingShifts.where((shift) {
            final jobTitle = (shift['job_title'] ?? '').toLowerCase();
            final shiftId = (shift['shift_id'] ?? '').toLowerCase();
            final category =
                (shift['worker_job_listings']?['category'] ?? '').toLowerCase();
            final company = (shift['company'] ?? '').toLowerCase();
            final searchQuery = query.toLowerCase();
            return jobTitle.contains(searchQuery) ||
                company.contains(searchQuery) ||
                shiftId.contains(searchQuery) ||
                category.contains(searchQuery);
          }).toList();

      _filteredCompletedShifts =
          _completedShifts.where((shift) {
            final jobTitle = (shift['job_title'] ?? '').toLowerCase();
            final shiftId = (shift['shift_id'] ?? '').toLowerCase();
            final category =
                (shift['worker_job_listings']?['category'] ?? '').toLowerCase();
            final company = (shift['company'] ?? '').toLowerCase();
            final searchQuery = query.toLowerCase();
            return jobTitle.contains(searchQuery) ||
                company.contains(searchQuery) ||
                shiftId.contains(searchQuery) ||
                category.contains(searchQuery);
          }).toList();

      // Add filtering for In Progress shifts
      _filteredInProgressShifts =
          _inProgressShifts.where((shift) {
            final jobTitle = (shift['job_title'] ?? '').toLowerCase();
            final shiftId = (shift['shift_id'] ?? '').toLowerCase();
            final category =
                (shift['worker_job_listings']?['category'] ?? '').toLowerCase();
            final company = (shift['company'] ?? '').toLowerCase();
            final searchQuery = query.toLowerCase();
            return jobTitle.contains(searchQuery) ||
                company.contains(searchQuery) ||
                shiftId.contains(searchQuery) ||
                category.contains(searchQuery);
          }).toList();
    });
  } // New method for header

  // Build In Progress Shifts
  Widget _buildInProgressShifts() {
    if (_filteredInProgressShifts.isEmpty) {
      return ListView(
        controller: _scrollController,
        physics: const AlwaysScrollableScrollPhysics(),
        children: [
          SizedBox(height: MediaQuery.of(context).size.height * 0.15),
          _buildEmptyState(
            icon: Icons.hourglass_empty,
            title: 'No shifts in progress',
            description: 'Shifts in progress will appear here',
          ),
        ],
      );
    }

    return ListView(
      physics: const AlwaysScrollableScrollPhysics(),
      padding: const EdgeInsets.fromLTRB(16, 0, 16, 24),
      children:
          _filteredInProgressShifts.map((shift) {
            return Padding(
              padding: const EdgeInsets.only(bottom: 16),
              child: _buildShiftCard(
                jobTitle: shift['job_title'] ?? 'Unknown Job',
                category: shift['worker_job_listings']?['category'] ?? 'Other',
                company: shift['company'] ?? 'Unknown Company',
                status: shift['application_status'] ?? 'In Progress',
                statusColor: _getStatusColor(shift['status']),
                location: shift['location'] ?? 'Unknown Location',
                date: _formatDate(shift['date']),
                time: _formatTime(shift['start_time'], shift['end_time']),
                payRate: '${shift['pay_rate'].toStringAsFixed(2)}',
                checkInTime: 'In Progress',
                showCheckInButton: false,
                companyLogo: _getCompanyInitials(shift['company']),
                logoColor: _getLogoColor(shift['logo_color']),
                shiftId: shift['shift_id']?.toString() ?? 'unknown',
                supervisorName:
                    shift['worker_job_listings']?['supervisor_name'],
                supervisorPhoneNumber:
                    shift['worker_job_listings']?['supervisor_phone'],
                supervisorEmail:
                    shift['worker_job_listings']?['supervisor_email'],
                pincode:
                    shift['worker_job_listings']?['job_pincode']?.toString() ??
                    'N/A',
              ),
            );
          }).toList(),
    );
  }

  // New method for search and filter
  Widget buildSearchBar() {
    return Container(
      margin: const EdgeInsets.symmetric(horizontal: 16, vertical: 12),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(30),
        boxShadow: [
          BoxShadow(
            color: Colors.black.withOpacity(0.05),
            blurRadius: 10,
            spreadRadius: 0,
            offset: const Offset(0, 2),
          ),
        ],
      ),
      child: TextField(
        controller: _searchController,
        onChanged: _filterShifts,
        decoration: InputDecoration(
          hintText: 'Search shifts',
          hintStyle: TextStyle(color: Colors.grey[500], fontSize: 16),
          prefixIcon: Icon(Icons.search, color: Colors.grey[400], size: 22),
          suffixIcon:
              _searchController.text.isNotEmpty
                  ? IconButton(
                    icon: Icon(Icons.close, color: Colors.grey[400], size: 20),
                    onPressed: () {
                      _searchController.clear();
                      _filterShifts('');
                    },
                  )
                  : null,
          border: InputBorder.none,
          contentPadding: const EdgeInsets.symmetric(
            vertical: 14,
            horizontal: 8,
          ),
        ),
      ),
    );
  }

  // New method for tabs

  String selectedTab = 'upcoming';

  Widget _buildTabs() {
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: 16.0, vertical: 8.0),
      child: Row(
        children: [
          _buildTabButton('Upcoming', 'upcoming', Colors.orange),
          const SizedBox(width: 8),
          _buildTabButton('In Progress', 'inProgress', Colors.blue),
          const SizedBox(width: 8),
          _buildTabButton('Completed', 'completed', Colors.green),
          //  const SizedBox(width: 8),
          // _buildTabButton('Cancelled', 'cancelled', Colors.red),
        ],
      ),
    );
  }

  Widget _buildTabButton(String title, String value, Color color) {
    final isDark = Theme.of(context).brightness == Brightness.dark;
    final isSelected = selectedTab == value;

    return Expanded(
      child: GestureDetector(
        onTap: () {
          setState(() {
            selectedTab = value;
            // Synchronize the tab controller with the selected tab
            switch (value) {
              case 'upcoming':
                _tabController.animateTo(0);
                break;
              case 'inProgress':
                _tabController.animateTo(1);
                break;
              case 'completed':
                _tabController.animateTo(2);
                break;
              // case 'cancelled':
              //   _tabController.animateTo(3);
              //  break;
            }
          });
        },
        child: Container(
          padding: const EdgeInsets.symmetric(vertical: 10),
          decoration: BoxDecoration(
            color: Colors.transparent,
            borderRadius: BorderRadius.circular(50),
            border: Border.all(
              color: isSelected ? color : Colors.grey.shade300,
              width: 2,
            ),
          ),
          child: Center(
            child: Text(
              title,
              style: TextStyle(
                color: isDark ? Colors.white : Colors.black,
                fontWeight: isSelected ? FontWeight.bold : FontWeight.normal,
              ),
            ),
          ),
        ),
      ),
    );
  } // Helper methods to format and style shift data

  Color _getStatusColor(String? status) {
    switch (status) {
      case 'Confirmed':
        return Colors.green;
      case 'Pending':
        return Colors.amber;
      //  case 'Cancelled':
      //  return Colors.grey;
      case 'Completed':
        return Colors.blue;
      default:
        return Colors.blue;
    }
  }

  String _formatDate(String? dateString) {
    if (dateString == null) return 'Unknown Date';
    try {
      final date = DateTime.parse(dateString);
      return '${_getMonthName(date.month)} ${date.day}, ${date.year}';
    } catch (e) {
      return 'Invalid Date';
    }
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

  String _formatTime(String? startTime, String? endTime) {
    if (startTime == null || endTime == null) return 'Unknown Time';
    return '$startTime - $endTime';
  }

  String _getCompanyInitials(String? company) {
    // Handle null or empty input
    if (company == null || company.isEmpty) return '?';

    // Trim and split the company name
    final trimmedCompany = company.trim();

    // If still empty after trimming
    if (trimmedCompany.isEmpty) return '?';

    // Split words and get initials
    try {
      return trimmedCompany
          .split(' ')
          .where((word) => word.isNotEmpty) // Ensure non-empty words
          .map((word) => word[0].toUpperCase())
          .take(2)
          .join();
    } catch (e) {
      print('Error getting company initials: $e');
      return '?';
    }
  }

  Color _getLogoColor(String? colorCode) {
    try {
      return colorCode != null
          ? Color(int.parse(colorCode.replaceFirst('#', '0xFF')))
          : Colors.grey;
    } catch (e) {
      return Colors.grey;
    }
  }

  // Build Upcoming Shifts
  Widget _buildUpcomingShifts() {
    if (_filteredUpcomingShifts.isEmpty) {
      // When empty, we need to return a scrollable widget for RefreshIndicator to work
      return ListView(
        controller: _scrollController,
        physics: const AlwaysScrollableScrollPhysics(),
        children: [
          SizedBox(height: MediaQuery.of(context).size.height * 0.15),
          _buildEmptyState(
            icon: Icons.calendar_today,
            title: 'No upcoming shifts',
            description: 'Find Shifts to start booking shifts',
            buttonText: 'Find Shifts',
            onButtonPressed: () {
              Navigator.push(
                context,
                MaterialPageRoute(builder: (context) => const FindJobsPage()),
              );
            },
          ),
        ],
      );
    }

    return ListView(
      controller: _scrollController,
      physics:
          const AlwaysScrollableScrollPhysics(), // Important for RefreshIndicator
      padding: const EdgeInsets.fromLTRB(16, 0, 16, 24),
      children:
          _filteredUpcomingShifts.map((shift) {
            return Padding(
              padding: const EdgeInsets.only(bottom: 16),
              child: _buildShiftCard(
                jobTitle: shift['job_title'] ?? 'Unknown Job',
                category: shift['worker_job_listings']?['category'] ?? 'Other',
                company: shift['company'] ?? 'Unknown Company',
                status: shift['application_status'] ?? 'Pending',
                statusColor: _getStatusColor(shift['status']),
                location: shift['location'] ?? 'Unknown Location',
                date: _formatDate(shift['date']),
                time: _formatTime(shift['start_time'], shift['end_time']),
                payRate: '${shift['pay_rate'].toStringAsFixed(2)}',
                checkInTime: 'Check-in at ${shift['check_in_time']}',
                showCheckInButton: shift['status'] == 'Confirmed',
                companyLogo: _getCompanyInitials(shift['company']),
                logoColor: _getLogoColor(shift['logo_color']),
                shiftId: shift['shift_id']?.toString() ?? 'unknown',
                supervisorName:
                    shift['worker_job_listings']?['supervisor_name'],
                supervisorPhoneNumber:
                    shift['worker_job_listings']?['supervisor_phone'],
                supervisorEmail:
                    shift['worker_job_listings']?['supervisor_email'],
                showCancelButton:
                    shift['application_status'] == 'Upcoming', // Add this line
                onCancelPressed: () {
                  // TODO: Implement cancel shift functionality
                  ScaffoldMessenger.of(context).showSnackBar(
                    const SnackBar(
                      content: Text('Cancel shift functionality coming soon'),
                    ),
                  );
                },
                pincode:
                    shift['worker_job_listings']?['job_pincode']?.toString() ??
                    'N/A',
              ),
            );
          }).toList(),
    );
  }

  // Build Completed Shifts
  Widget _buildCompletedShifts() {
    if (_filteredCompletedShifts.isEmpty) {
      return ListView(
        controller: _scrollController,
        physics: const AlwaysScrollableScrollPhysics(),
        children: [
          SizedBox(height: MediaQuery.of(context).size.height * 0.15),
          _buildEmptyState(
            icon: Icons.check_circle,
            title: 'No completed shifts',
            description: 'Your completed shifts will appear here',
          ),
        ],
      );
    }

    return ListView(
      physics: const AlwaysScrollableScrollPhysics(),
      padding: const EdgeInsets.fromLTRB(16, 0, 16, 24),
      children:
          _filteredCompletedShifts.map((shift) {
            return Padding(
              padding: const EdgeInsets.only(bottom: 16),
              child: _buildShiftCard(
                jobTitle: shift['job_title'] ?? 'Unknown Job',
                category: shift['worker_job_listings']?['category'] ?? 'Other',
                company: shift['company'] ?? 'Unknown Company',
                status: shift['status'] ?? 'Completed',
                statusColor: _getStatusColor(shift['status']),
                location: shift['location'] ?? 'Unknown Location',
                date: _formatDate(shift['date']),
                time: _formatTime(shift['start_time'], shift['end_time']),
                payRate: '${shift['pay_rate'].toStringAsFixed(2)}',
                checkInTime: 'Checked in at ${shift['check_in_time']}',
                showCheckInButton: false,
                companyLogo: _getCompanyInitials(shift['company']),
                logoColor: _getLogoColor(shift['logo_color']),
                shiftId: shift['shift_id']?.toString() ?? 'unknown',
                supervisorName:
                    shift['worker_job_listings']?['supervisor_name'],
                supervisorPhoneNumber:
                    shift['worker_job_listings']?['supervisor_phone'],
                supervisorEmail:
                    shift['worker_job_listings']?['supervisor_email'],
                pincode:
                    shift['worker_job_listings']?['job_pincode']?.toString() ??
                    'N/A',
              ),
            );
          }).toList(),
    );
  }

  String _formatJobDatee(String? dateString) {
    print('DEBUG: Input date string: $dateString');

    if (dateString == null || dateString.isEmpty) {
      print('DEBUG: Date is null or empty');
      return '';
    }

    DateTime? date;
    try {
      // Try parsing in multiple formats
      date = DateTime.tryParse(dateString);

      // If parsing fails, try manual parsing
      if (date == null) {
        // Check if it's in 'YYYY-MM-DD' format
        final parts = dateString.toString().split('-');
        if (parts.length == 3) {
          date = DateTime(
            int.parse(parts[0]),
            int.parse(parts[1]),
            int.parse(parts[2]),
          );
        }
      }
    } catch (e) {
      print('DEBUG: Error parsing date: $e');
      return '';
    }

    if (date == null) {
      print('DEBUG: Could not parse date');
      return '';
    }

    final formattedDate =
        '${_getMonthName(date.month)} ${date.day} ${date.year}';
    print('DEBUG: Formatted date: $formattedDate');

    return formattedDate;
  }

  // Build Cancelled Shifts
  /*Widget _buildCancelledShifts() {
    if (_filteredCancelledShifts.isEmpty) {
      return ListView(
        physics: const AlwaysScrollableScrollPhysics(),
        children: [
          SizedBox(height: MediaQuery.of(context).size.height * 0.15),
          _buildEmptyState(
            icon: Icons.cancel,
            title: 'No cancelled shifts',
            description: 'Your cancelled shifts will appear here',
          ),
        ],
      );
    }

    return ListView(
      physics: const AlwaysScrollableScrollPhysics(),
      padding: const EdgeInsets.fromLTRB(16, 0, 16, 24),
      children:
          _filteredCancelledShifts.map((shift) {
            return Padding(
              padding: const EdgeInsets.only(bottom: 16),
              child: _buildShiftCard(
                jobTitle: shift['job_title'] ?? 'Unknown Job',
                company: shift['company'] ?? 'Unknown Company',
                status: shift['application_status'] ?? 'Cancelled',
                statusColor: _getStatusColor(shift['status']),
                location: shift['location'] ?? 'Unknown Location',
                date: _formatDate(shift['date']),
                time: _formatTime(shift['start_time'], shift['end_time']),
                payRate: '${shift['pay_rate'].toStringAsFixed(2)}/Day',
                checkInTime: 'Cancelled on ${shift['updated_at']}',
                showCheckInButton: false,
                companyLogo: _getCompanyInitials(shift['company']),
                logoColor: _getLogoColor(shift['logo_color']),
                shiftId: shift['id']?.toString() ?? 'unknown',
                pincode:
                    shift['worker_job_listings']?['job_pincode']?.toString() ??
                    'N/A',
              ),
            );
          }).toList(),
    );
  }*/
  // Calculate hourly rate
  double calculateHourlyRate(String? duration, String payRate) {
    // Remove the currency symbol and parse the pay rate
    final numericPayRate =
        double.tryParse(payRate.replaceAll(RegExp(r'[^\d.]'), '')) ?? 0.0;

    // Parse duration to extract total hours
    double totalHours = 0.0;

    // Check if duration is null or empty
    if (duration == null || duration == 'N/A') {
      return numericPayRate; // Return original pay rate if duration can't be calculated
    }

    // Extract hours
    final hourMatch = RegExp(r'(\d+)\s*hr').firstMatch(duration);
    if (hourMatch != null && hourMatch.group(1) != null) {
      totalHours += double.tryParse(hourMatch.group(1)!) ?? 0;
    }

    // Extract minutes
    final minuteMatch = RegExp(r'(\d+)\s*min').firstMatch(duration);
    if (minuteMatch != null && minuteMatch.group(1) != null) {
      totalHours += (double.tryParse(minuteMatch.group(1)!) ?? 0) / 60;
    }

    // Prevent division by zero
    if (totalHours <= 0) {
      return numericPayRate;
    }

    // Calculate hourly rate
    return numericPayRate / totalHours;
  }

  // In the _buildShiftCard method, replace the existing hourly rate calculation with:

  double _extractHoursFromDuration(String duration) {
    double totalHours = 0.0;

    // Extract hours
    final hourMatch = RegExp(r'(\d+)\s*hr').firstMatch(duration);
    if (hourMatch != null && hourMatch.group(1) != null) {
      totalHours += double.tryParse(hourMatch.group(1)!) ?? 0;
    }

    // Extract minutes
    final minuteMatch = RegExp(r'(\d+)\s*min').firstMatch(duration);
    if (minuteMatch != null && minuteMatch.group(1) != null) {
      totalHours += (double.tryParse(minuteMatch.group(1)!) ?? 0) / 60;
    }

    return totalHours;
  }

  Widget _buildShiftCard({
    required String jobTitle,
    required String category,
    required String company,
    required String status,
    required Color statusColor,
    required String location,
    required String date,
    required String time,
    required String payRate,
    required String checkInTime,
    required bool showCheckInButton,
    required String companyLogo,
    required Color logoColor,
    required String shiftId,
    // New parameters for supervisor details
    String? supervisorName,
    String? supervisorPhoneNumber,
    String? supervisorEmail,
    bool showCancelButton = false, // Add this line
    VoidCallback? onCancelPressed,
    required String pincode,
  }) {
    // Status color functions
    Color getStatusColor(String status) {
      switch (status.toLowerCase()) {
        case 'in progress':
          return Color(0xFF3461FD); // Blue
        case 'upcoming':
          return Color(0xFFF5A623); // Yellow/Orange
        case 'completed':
          return Color(0xFF27AE60); // Green
        // case 'cancelled':
        //   return Color(0xFFEF4444); // Red
        default:
          return Color(0xFF8B0000); // Default red
      }
    }

    Color getStatusBackgroundColor(String status) {
      switch (status.toLowerCase()) {
        case 'in progress':
          return Color(0x1A3461FD); // Light Blue
        case 'upcoming':
          return Color(0x1AF5A623); // Light Yellow/Orange
        case 'completed':
          return Color(0x1A27AE60); // Light Green
        // case 'cancelled':
        //   return Color(0x1AEF4444); // Light Red
        default:
          return Color(0x1AEF4444); // Default light red
      }
    }

    // Calculate shift duration (you'll need to implement this method)
    String duration = _calculateShiftDuration(time);
    double hourlyRateValue = calculateHourlyRate(duration, payRate);
    String hourlyRate = '\₹${hourlyRateValue.toStringAsFixed(2)}/hr';

    return Container(
      width: double.infinity,
      decoration: BoxDecoration(
        color: Colors.white,
        boxShadow: [
          BoxShadow(
            blurRadius: 4,
            color: Color(0x0D000000),
            offset: Offset(0, 2),
          ),
        ],
        borderRadius: BorderRadius.circular(12),
      ),
      child: Padding(
        padding: EdgeInsets.all(16),
        child: Column(
          children: [
            // Shift Header
            Container(
              width: double.infinity,
              decoration: BoxDecoration(
                color: getStatusBackgroundColor(status),
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
                            'Title: $jobTitle',
                            style: TextStyle(
                              fontSize: 16,
                              fontWeight: FontWeight.w600,
                            ),
                          ),
                          Text(
                            'Category: $category',
                            style: TextStyle(
                              fontSize: 14,
                              fontWeight: FontWeight.w500,
                            ),
                          ),
                          Text(
                            'Shift ID: $shiftId',
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
                        color: getStatusColor(status),
                        borderRadius: BorderRadius.circular(16),
                      ),
                      child: Text(
                        status,
                        style: TextStyle(color: Colors.white, fontSize: 12),
                      ),
                    ),
                  ],
                ),
              ),
            ),

            // Shift Details
            Padding(
              padding: EdgeInsets.symmetric(vertical: 12),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  // Company row
                  Row(
                    mainAxisAlignment: MainAxisAlignment.spaceBetween,
                    children: [
                      Row(
                        children: [
                          Icon(Icons.business, size: 18, color: Colors.black),
                          SizedBox(width: 8),
                          Text(
                            'Company: $company',
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
                        'Date:'
                        '${_formatJobDate(date)}',
                        style: TextStyle(
                          fontSize: 15,
                          color: Colors.grey.shade800,
                        ),
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
                        '${_formatJobTime(time.split(' - ')[0])} - ${_formatJobTime(time.split(' - ')[1])}',
                        style: TextStyle(
                          fontSize: 15,
                          color: Colors.grey.shade800,
                        ),
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
                            'Duration: ${_calculateShiftDuration(time)}',
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
                      Icon(
                        Icons.location_on_outlined,
                        size: 18,
                        color: Colors.green,
                      ),
                      const SizedBox(width: 8),
                      Expanded(
                        child: Text(
                          'location: ${location ?? 'Location'}',
                          style: TextStyle(
                            fontSize: 15,
                            color: Colors.grey.shade800,
                          ),
                          maxLines: 4,
                          overflow: TextOverflow.ellipsis,
                        ),
                      ),
                      const SizedBox(width: 4),
                      InkWell(
                        onTap: () {
                          final address = ' ${location ?? ''}';
                          final encoded = Uri.encodeComponent(address);
                          final googleMapsUrl =
                              'https://www.google.com/maps/search/?api=1&query=$encoded';
                          launchUrl(
                            Uri.parse(googleMapsUrl),
                            mode: LaunchMode.externalApplication,
                          );
                        },
                        borderRadius: BorderRadius.circular(20),
                        child: Padding(
                          padding: const EdgeInsets.all(4.0),
                          child: Icon(
                            Icons.pin_drop,
                            color: Colors.blue.shade600,
                            size: 22,
                          ),
                        ),
                      ),
                    ],
                  ),
                  Row(
                    children: [
                      Icon(
                        Icons.pin_drop_outlined,
                        size: 18,
                        color: Colors.red,
                      ),
                      SizedBox(width: 8),
                      Text(
                        'Pincode: $pincode',
                        style: TextStyle(
                          color: Colors.grey.shade700,
                          fontSize: 15,
                        ),
                      ),
                    ],
                  ),
                  Row(
                    children: [
                      Icon(Icons.credit_card, size: 18, color: Colors.red),
                      SizedBox(width: 8),
                      Text(
                        'Pay Rate per Hour: $hourlyRate',
                        style: TextStyle(
                          color: Colors.grey.shade700,
                          fontSize: 15,
                        ),
                      ),
                    ],
                  ),
                  Row(
                    children: [
                      Icon(Icons.credit_card, size: 18, color: Colors.red),
                      SizedBox(width: 8),
                      Text(
                        'Pay Rate per Shift: $payRate',
                        style: TextStyle(
                          color: Colors.grey.shade700,
                          fontSize: 15,
                        ),
                      ),
                    ],
                  ),
                  if (showCancelButton)
                    if (showCancelButton)
                      Row(
                        mainAxisAlignment: MainAxisAlignment.end,
                        children: [
                          OutlinedButton(
                            onPressed: () {
                              _showCancelShiftDialog(context, shiftId);
                            },
                            style: OutlinedButton.styleFrom(
                              minimumSize: Size(0, 48),
                              foregroundColor: Colors.red,
                              side: BorderSide(color: Colors.red.shade200),
                              padding: const EdgeInsets.symmetric(
                                horizontal: 16,
                                vertical: 12,
                              ),
                              shape: RoundedRectangleBorder(
                                borderRadius: BorderRadius.circular(8),
                              ),
                            ),
                            child: Text('Cancel Shift'),
                          ),
                        ],
                      ),

                  // Action Buttons (if needed)
                ],
              ),
            ),
          ],
        ),
      ),
    );
  }

  void _showCancelShiftDialog(BuildContext context, String shiftId) async {
    try {
      // First, fetch the complete shift details
      final user = Supabase.instance.client.auth.currentUser;
      if (user == null) {
        _showErrorDialog(context, 'No authenticated user found');
        return;
      }

      // Fetch full shift details
      final response =
          await Supabase.instance.client
              .from('worker_job_applications')
              .select('''
        *,
        worker_job_listings (
          id,
          supervisor_name, 
          supervisor_phone, 
          supervisor_email,
          job_pincode,
          category
        )
      ''')
              .eq('shift_id', shiftId)
              .eq('user_id', user.id)
              .single();

      if (response == null) {
        _showErrorDialog(context, 'Shift details not found');
        return;
      }

      // Extract shift date and start time
      final shiftDate =
          response['date'] != null
              ? DateTime.parse(response['date'].toString())
              : null;
      final shiftStartTime = response['start_time']?.toString();
      final payRate = response['pay_rate'] as num? ?? 0;

      if (shiftDate == null || shiftStartTime == null) {
        _showErrorDialog(context, 'Invalid shift details');
        return;
      }

      // Parse shift start time
      final shiftDateTime = DateTime(
        shiftDate.year,
        shiftDate.month,
        shiftDate.day,
        int.parse(shiftStartTime.split(':')[0]),
        int.parse(shiftStartTime.split(':')[1]),
      );

      // Current time
      final now = DateTime.now();

      // Calculate time difference
      final timeDifference = shiftDateTime.difference(now);
      final canCancelFree = timeDifference.inHours >= AppConstants.freeCancellationHours;

      if (canCancelFree) {
        // Free cancellation (more than 24 hours before shift)
        _proceedWithCancellation(context, shiftId);
      } else {
        // Show penalty cancellation dialog
        _showPenaltyCancellationDialog(context, response, shiftDateTime);
      }
    } catch (e) {
      _showErrorDialog(
        context,
        'Error checking shift details: ${e.toString()}',
      );
    }
  }

  void _showPenaltyCancellationDialog(
    BuildContext context,
    Map<String, dynamic> shift,
    DateTime shiftDateTime,
  ) {
    final payRate = shift['pay_rate'] as num? ?? 0;
    final penaltyAmount = payRate * AppConstants.cancellationPenaltyRate;

    showDialog(
      context: context,
      builder: (BuildContext context) {
        return Dialog(
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
                    Icons.warning_rounded,
                    color: Colors.orange.shade400,
                    size: 48,
                  ),
                ),
                const SizedBox(height: 24),

                // Title
                Text(
                  'Cancellation Penalty',
                  style: TextStyle(
                    fontSize: 22,
                    fontWeight: FontWeight.bold,
                    color: Colors.grey.shade800,
                  ),
                  textAlign: TextAlign.center,
                ),
                const SizedBox(height: 16),

                // Description
                Text(
                  'This shift is less than 24 hours away. '
                  'Cancelling now will result in a 50% penalty of \₹$penaltyAmount. '
                  'Do you still want to proceed?',
                  style: TextStyle(fontSize: 16, color: Colors.grey.shade600),
                  textAlign: TextAlign.center,
                ),
                const SizedBox(height: 24),

                // Action Buttons
                Row(
                  children: [
                    // Cancel Button
                    Expanded(
                      child: OutlinedButton(
                        onPressed: () {
                          Navigator.of(context).pop();
                        },
                        style: OutlinedButton.styleFrom(
                          foregroundColor: Colors.grey.shade700,
                          side: BorderSide(color: Colors.grey.shade300),
                          padding: const EdgeInsets.symmetric(vertical: 12),
                          shape: RoundedRectangleBorder(
                            borderRadius: BorderRadius.circular(12),
                          ),
                        ),
                        child: const Text(
                          'Keep Shift',
                          style: TextStyle(fontWeight: FontWeight.w600),
                        ),
                      ),
                    ),
                    const SizedBox(width: 16),

                    // Confirm Cancel Button
                    Expanded(
                      child: ElevatedButton(
                        onPressed: () async {
                          Navigator.of(context).pop(); // Close penalty dialog
                          await _processPenaltyCancellation(
                            context,
                            shift,
                            penaltyAmount,
                          );
                        },
                        style: ElevatedButton.styleFrom(
                          backgroundColor: Colors.orange.shade600,
                          foregroundColor: Colors.white,
                          padding: const EdgeInsets.symmetric(vertical: 12),
                          shape: RoundedRectangleBorder(
                            borderRadius: BorderRadius.circular(12),
                          ),
                        ),
                        child: const Text(
                          'Accept Penalty',
                          style: TextStyle(fontWeight: FontWeight.w600),
                        ),
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
  }

  Future<void> _processPenaltyCancellation(
    BuildContext context,
    Map<String, dynamic> shift,
    num penaltyAmount,
  ) async {
    final shiftId = shift['shift_id'];

    print("====== CANCELLATION PROCESS STARTED ======");
    print("Shift ID: $shiftId");
    print("Penalty Amount: $penaltyAmount");

    // Store a copy of the scaffold messenger before any async operations
    final scaffoldMessenger = ScaffoldMessenger.of(context);

    // Show loading first
    setState(() {
      _isLoading = true;
      print("Set _isLoading = true");
    });

    try {
      print("Getting current user...");
      final user = Supabase.instance.client.auth.currentUser;
      if (user == null || user.email == null) {
        print("ERROR: No authenticated user found");
        throw Exception('No authenticated user found');
      }
      final useremail = user.email!; // Safe to use ! here since we checked above
      print("User found: ${user.id}");

      // Fetch user details for transaction record
      final userDetailsResponse =
          await Supabase.instance.client
              .from(
                'job_seekers',
              ) // Adjust if you're using a different table for user details
              .select('full_name, phone, email')
              .eq('email', user.email as Object)
              .single()
              .withTimeout(timeout: TimeoutDurations.query);

      final userName = userDetailsResponse?['full_name'] ?? 'Unknown User';
      final userPhone = userDetailsResponse?['phone'] ?? '';
      final userEmail = userDetailsResponse?['email'] ?? '';

      print("User details: Name=$userName, Phone=$userPhone, Email=$userEmail");

      // Fetch wallet information
      print("Fetching wallet information...");
      final walletResponse = await Supabase.instance.client
          .from('worker_wallet')
          .select()
          .eq('worker_email', useremail)
          .withTimeout(timeout: TimeoutDurations.query);

      print("Wallet response received: ${walletResponse?.length ?? 0} items");

      if (walletResponse == null || walletResponse.isEmpty) {
        print("ERROR: No wallet found for user");
        throw Exception('No wallet found for user');
      }

      // ⚠️ RACE CONDITION WARNING:
      // This read-modify-write pattern has a race condition if multiple
      // cancellations happen simultaneously. Both will read the same balance,
      // calculate their penalty, and one update may be lost.
      //
      // RECOMMENDED FIX: Use a Supabase RPC function with database-level
      // transaction or use SQL: UPDATE wallets SET balance = balance - penalty
      // WHERE user_id = $1 to ensure atomic updates.

      final walletId = walletResponse[0]['id']; // Get wallet ID for reference
      final currentBalance = walletResponse[0]['balance'] as num? ?? 0;
      print("Current balance: $currentBalance");

      final newBalance = currentBalance - penaltyAmount;
      print("New balance will be: $newBalance");

      // First create a transaction record
      print("Creating wallet transaction record...");
      final transactionData = {
        'wallet_id': walletId,
        'amount': penaltyAmount,
        'transaction_type': 'DEBIT',
        'description':
            'Cancellation penalty for shift: ${shift['job_title'] ?? 'Unknown'} (ID: $shiftId)',
        'status': 'Cancelled',
        //'shift_id': shiftId,
        //'reference_id': referenceId, // Use the generated UUID
        'created_at': DateTime.now().toIso8601String(),
      };

      print("Transaction data prepared: $transactionData");

      // Use a database transaction for consistency
      // Start of atomic operations
      print("Beginning database operations...");

      // 1. Insert transaction record
      final transactionResponse =
          await Supabase.instance.client
              .from('worker_wallet_transactions')
              .insert(transactionData)
              .select()
              .withTimeout(timeout: TimeoutDurations.query);

      print(
        "Transaction record created: ${transactionResponse?.length ?? 0} items",
      );

      // 2. Update wallet balance
      print("Updating wallet balance...");
      final updateResponse =
          await Supabase.instance.client
              .from('worker_wallet')
              .update({
                'balance': newBalance,
                'last_updated': DateTime.now().toIso8601String(),
                // 'last_transaction': 'Penalty deduction for shift cancellation',
              })
              .eq('id', walletId)
              .select()
              .withTimeout(timeout: TimeoutDurations.query);

      print("Wallet update response: $updateResponse");

      // Continue with the rest of the cancellation logic as before
      final shiftResponse =
          await Supabase.instance.client
              .from('worker_job_applications')
              .select(
                '*, worker_job_listings (supervisor_name,user_id, supervisor_email, job_pincode, category, contact_name, contact_email)',
              )
              .eq('shift_id', shiftId)
              .eq('user_id', user.id)
              .single();

      if (shiftResponse == null) {
        Navigator.of(context).pop();
        _showErrorDialog(context, 'Shift not found for cancellation.');
        return;
      }

      // Insert into cancelled_shifts
      await Supabase.instance.client.from('cancelled_shifts').insert({
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
        // 'penalty_amount': penaltyAmount, // Add penalty amount to the record
        // 'wallet_transaction_id':
        //   transactionResponse?[0]?['id'] ?? null, // Link to the transaction
        // New fields
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
        'worker_name': userName,
        'worker_email': userEmail,
        'cancelled_by': 'worker',
        'cancelled_person_name': userName,
        'remarks': 'Charge',
      });

      print("Added entry to cancelled_shifts table");

      // Delete from worker_job_applications
      print("Deleting shift with ID: $shiftId...");
      await Supabase.instance.client
          .from('worker_job_applications')
          .delete()
          .eq('shift_id', shiftId)
          .eq('user_id', user.id);
      print("Shift deletion complete");

      print("All database operations completed successfully");
      await sendUserNotification(
        userId:
            shiftResponse['worker_job_listings']?['user_id'], // or employer's user ID if available
        title: 'Shift Cancelled',
        message: 'A worker has cancelled the Shift $shiftId',
        shiftId: shiftId,
        userType: 'employer',
        notificationType: 'worker_cancelled',
      );

      await sendUserNotification(
        userId: user.id, // the worker’s user ID
        title: 'Shift Cancelled',
        message: 'You have cancelled the Shift $shiftId',
        shiftId: shiftId,
        userType: 'worker',
        notificationType: 'cancelled',
      );

      // Use post-frame callback to show success snackbar
      WidgetsBinding.instance.addPostFrameCallback((_) async {
        scaffoldMessenger.showSnackBar(
          SnackBar(
            content: Text(
              'Shift cancelled. ₹$penaltyAmount deducted from your wallet.',
            ),
            backgroundColor: Colors.green,
          ),
        );

        await _fetchShifts(); // Refresh shifts
        setState(() {}); // Force rebuild after shifts are refreshed
      });
    } on PostgrestException catch (e) {
      print("SUPABASE ERROR during cancellation: ${e.message}");

      // Use post-frame callback to show error snackbar
      WidgetsBinding.instance.addPostFrameCallback((_) {
        print("Showing database error message via post-frame callback");
        scaffoldMessenger.showSnackBar(
          SnackBar(
            content: Text('Database error: ${e.message}. Please try again.'),
            backgroundColor: Colors.red,
            duration: Duration(seconds: 5),
          ),
        );
      });
    } on TimeoutException catch (e) {
      print("TIMEOUT ERROR during cancellation: $e");

      WidgetsBinding.instance.addPostFrameCallback((_) {
        scaffoldMessenger.showSnackBar(
          SnackBar(
            content: Text('Request timed out. Please check your connection and try again.'),
            backgroundColor: Colors.orange,
            duration: Duration(seconds: 5),
          ),
        );
      });
    } catch (e) {
      print("CRITICAL ERROR during cancellation: $e");

      // Use post-frame callback to show error snackbar
      WidgetsBinding.instance.addPostFrameCallback((_) {
        print("Showing error message via post-frame callback");
        scaffoldMessenger.showSnackBar(
          SnackBar(
            content: Text('Error: ${e.toString()}'),
            backgroundColor: Colors.red,
            duration: Duration(seconds: 5),
          ),
        );
      });
    } finally {
      // Ensure we reset loading state
      if (mounted) {
        setState(() {
          _isLoading = false;
          print("Set _isLoading = false");
        });
      }
      print("====== CANCELLATION PROCESS COMPLETED ======");
    }
  }

  void _proceedWithCancellation(BuildContext context, String shiftId) async {
    try {
      showDialog(
        context: context,
        barrierDismissible: false,
        builder: (context) => const Center(child: CircularProgressIndicator()),
      );

      final user = Supabase.instance.client.auth.currentUser;
      if (user == null) {
        throw Exception('No authenticated user found');
      }

      // 🆕 Fetch shift details first
      final shiftResponse =
          await Supabase.instance.client
              .from('worker_job_applications')
              .select(
                '*, worker_job_listings (supervisor_name, supervisor_email, job_pincode, category, contact_name, contact_email)',
              )
              .eq('shift_id', shiftId)
              .eq('user_id', user.id)
              .single();

      if (shiftResponse == null) {
        Navigator.of(context).pop();
        _showErrorDialog(context, 'Shift not found for cancellation.');
        return;
      }

      // 🆕 Insert into cancelled_shifts
      await Supabase.instance.client.from('cancelled_shifts').insert({
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

        // New fields
        'employer_name':
            shiftResponse['worker_job_listings']?['contact_name'] ??
            '', // You might need to adjust
        'employer_email':
            shiftResponse['worker_job_listings']?['contact_email'] ?? '',
        'supervisor_phone':
            shiftResponse['worker_job_listings']?['supervisor_phone'],
        'supervisor_name':
            shiftResponse['worker_job_listings']?['supervisor_name'],
        'supervisor_email':
            shiftResponse['worker_job_listings']?['supervisor_email'],
        'worker_name': shiftResponse['full_name'] ?? '',
        'worker_email': shiftResponse['email'] ?? '',
        'cancelled_by': 'worker',
        'cancelled_person_name': shiftResponse['full_name'] ?? '',
        'remarks': 'Nocharge',
      });

      // ✅ After insert, now delete
      await Supabase.instance.client
          .from('worker_job_applications')
          .delete()
          .eq('shift_id', shiftId)
          .eq('user_id', user.id);

      Navigator.of(context).pop();
      _showSuccessDialog(context, 'Shift cancelled successfully.');
      await _fetchShifts();
    } on PostgrestException catch (e) {
      Navigator.of(context).pop();
      _showErrorDialog(context, 'Database error: ${e.message}. Please try again.');
      print('Supabase error during free cancellation: ${e.message}');
    } on TimeoutException catch (e) {
      Navigator.of(context).pop();
      _showErrorDialog(context, 'Request timed out. Please check your connection and try again.');
      print('Timeout error during free cancellation: $e');
    } catch (e) {
      Navigator.of(context).pop();
      _showErrorDialog(context, 'Error: ${e.toString()}');
      print('Error during free cancellation: $e');
    }
  }

  Future<void> sendWorkerNotification({
    required String userId,
    required String title,
    required String message,
    required String shiftId,
    String type = 'info',
    String notificationType = 'applied',
    String androidChannelId = 'shifthour_general',
  }) async {
    final supabase = Supabase.instance.client;
    final notificationId = generateUuid(); // Reuse your existing UUID method

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

    try {
      // Store to DB
      await supabase.from('notifications').insert({
        'id': notificationId,
        'user_id': userId,
        'user_type': 'worker',
        'title': title,
        'message': message,
        'notification_type': notificationType,
        'type': type,
        'created_at': DateTime.now().toIso8601String(),
        'is_read': false,
      });

      // Send notification via edge function
      final response = await supabase.functions.invoke(
        'send-notification',
        body: body,
      );

      print("✅ Notification sent: ${response.data}");
    } catch (e) {
      print("❌ Error sending notification: $e");
    }
  }

  String generateUuid() {
    final random = Random();
    final hexDigits = '0123456789abcdef';
    final uuid = List<String>.filled(36, '');

    for (var i = 0; i < 36; i++) {
      if (i == 8 || i == 13 || i == 18 || i == 23) {
        uuid[i] = '-';
      } else if (i == 14) {
        uuid[i] = '4';
      } else if (i == 19) {
        uuid[i] = hexDigits[(random.nextInt(4) | 8)];
      } else {
        uuid[i] = hexDigits[random.nextInt(16)];
      }
    }

    return uuid.join('');
  }

  Future<void> sendUserNotification({
    required String userId,
    required String title,
    required String message,
    required String shiftId,
    required String userType, // 'employer' or 'worker'
    String type = 'info',
    String notificationType = 'cancelled',
    String androidChannelId = 'shifthour_general',
  }) async {
    final supabase = Supabase.instance.client;
    final notificationId = generateUuid();

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

    try {
      // ✅ Store in notifications table
      await supabase.from('notifications').insert({
        'id': notificationId,
        'user_id': userId,
        'user_type': userType, // ✅ IMPORTANT: worker or employer
        'title': title,
        'message': message,
        'notification_type': notificationType,
        'type': type,
        'created_at': DateTime.now().toIso8601String(),
        'is_read': false,
      });

      // ✅ Send via edge function
      final response = await supabase.functions.invoke(
        'send-notification',
        body: body,
      );

      print("✅ Notification sent: ${response.data}");
    } catch (e) {
      print("❌ Error sending notification: $e");
    }
  }

  void _showSuccessDialog(BuildContext context, String message) {
    showDialog(
      context: context,
      builder: (BuildContext context) {
        return Dialog(
          shape: RoundedRectangleBorder(
            borderRadius: BorderRadius.circular(16),
          ),
          child: Container(
            padding: const EdgeInsets.all(24),
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                Container(
                  width: 80,
                  height: 80,
                  decoration: BoxDecoration(
                    color: Colors.green.shade50,
                    shape: BoxShape.circle,
                  ),
                  child: Icon(
                    Icons.check_circle_rounded,
                    color: Colors.green.shade400,
                    size: 48,
                  ),
                ),
                const SizedBox(height: 24),
                Text(
                  'Shift Cancelled',
                  style: TextStyle(
                    fontSize: 22,
                    fontWeight: FontWeight.bold,
                    color: Colors.grey.shade800,
                  ),
                ),
                const SizedBox(height: 16),
                Text(
                  message,
                  style: TextStyle(fontSize: 16, color: Colors.grey.shade600),
                  textAlign: TextAlign.center,
                ),
                const SizedBox(height: 24),
                ElevatedButton(
                  onPressed: () {
                    Navigator.of(context).pop();
                  },
                  style: ElevatedButton.styleFrom(
                    backgroundColor: Colors.green.shade600,
                    foregroundColor: Colors.white,
                    padding: const EdgeInsets.symmetric(
                      horizontal: 32,
                      vertical: 12,
                    ),
                    shape: RoundedRectangleBorder(
                      borderRadius: BorderRadius.circular(12),
                    ),
                  ),
                  child: const Text(
                    'Okay',
                    style: TextStyle(fontWeight: FontWeight.w600),
                  ),
                ),
              ],
            ),
          ),
        );
      },
    );
  }

  void _showErrorDialog(BuildContext context, String message) {
    showDialog(
      context: context,
      builder: (BuildContext context) {
        return Dialog(
          shape: RoundedRectangleBorder(
            borderRadius: BorderRadius.circular(16),
          ),
          child: Container(
            padding: const EdgeInsets.all(24),
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                Container(
                  width: 80,
                  height: 80,
                  decoration: BoxDecoration(
                    color: Colors.red.shade50,
                    shape: BoxShape.circle,
                  ),
                  child: Icon(
                    Icons.error_rounded,
                    color: Colors.red.shade400,
                    size: 48,
                  ),
                ),
                const SizedBox(height: 24),
                Text(
                  'Oops! Something Went Wrong',
                  style: TextStyle(
                    fontSize: 22,
                    fontWeight: FontWeight.bold,
                    color: Colors.grey.shade800,
                  ),
                ),
                const SizedBox(height: 16),
                Text(
                  message,
                  style: TextStyle(fontSize: 16, color: Colors.grey.shade600),
                  textAlign: TextAlign.center,
                ),
                const SizedBox(height: 24),
                ElevatedButton(
                  onPressed: () {
                    Navigator.of(context).pop();
                  },
                  style: ElevatedButton.styleFrom(
                    backgroundColor: Colors.red.shade600,
                    foregroundColor: Colors.white,
                    padding: const EdgeInsets.symmetric(
                      horizontal: 32,
                      vertical: 12,
                    ),
                    shape: RoundedRectangleBorder(
                      borderRadius: BorderRadius.circular(12),
                    ),
                  ),
                  child: const Text(
                    'Dismiss',
                    style: TextStyle(fontWeight: FontWeight.w600),
                  ),
                ),
              ],
            ),
          ),
        );
      },
    );
  }

  String _formatJobTime(String? timeString) {
    if (timeString == null) return '';

    try {
      final parsedDateTime = DateTime.tryParse('2000-01-01 $timeString');
      if (parsedDateTime == null) {
        return timeString; // Return original if parsing fails
      }

      final time = TimeOfDay.fromDateTime(parsedDateTime);
      final period = time.period == DayPeriod.am ? 'AM' : 'PM';
      final hour = time.hourOfPeriod == 0 ? 12 : time.hourOfPeriod;
      final minute = time.minute.toString().padLeft(2, '0');

      return '$hour:$minute $period';
    } catch (e) {
      return timeString; // Return original string on error
    }
  }

  String _formatJobDate(String? dateString) {
    if (dateString == null || dateString.isEmpty) {
      return 'Unknown Date';
    }

    // If it's already in the desired format, return as-is
    if (_isAlreadyFormattedDate(dateString)) {
      return dateString;
    }

    try {
      // Try standard parsing
      DateTime? date = DateTime.tryParse(dateString);

      if (date == null) {
        // Try parsing with alternative formats
        final formats = [
          'yyyy-MM-dd',
          'MM/dd/yyyy',
          'dd/MM/yyyy',
          'yyyy/MM/dd',
        ];

        for (var format in formats) {
          try {
            date = _parseDate(dateString, format);
            if (date != null) break;
          } catch (_) {}
        }
      }

      if (date == null) {
        print('Could not parse date: $dateString');
        return 'Invalid Date';
      }

      return '${_getMonthName(date.month)} ${date.day}, ${date.year}';
    } catch (e) {
      print('Unexpected error parsing date: $e');
      return 'Invalid Date';
    }
  }

  // Helper method to check if date is already in desired format
  bool _isAlreadyFormattedDate(String dateString) {
    final regex = RegExp(r'^[A-Z][a-z]+ \d{1,2}, \d{4}$');
    return regex.hasMatch(dateString);
  }

  // Helper method for parsing with specific format
  DateTime? _parseDate(String dateString, String format) {
    try {
      return DateFormat(format).parse(dateString);
    } catch (_) {
      return null;
    }
  }

  Future<bool> cancelShift(String shiftId) async {
    try {
      // Get the current user
      final user = Supabase.instance.client.auth.currentUser;
      if (user == null) {
        throw Exception('No authenticated user found');
      }

      // Delete the shift record
      await Supabase.instance.client
          .from('worker_job_applications')
          .delete()
          .eq('id', shiftId)
          .eq('user_id', user.id);

      return true;
    } catch (e) {
      print('Error cancelling shift: $e');
      return false;
    }
  }

  String _calculateShiftDuration(String? time) {
    if (time == null) return 'N/A';

    try {
      // Split the time string into start and end times
      final parts = time.split(' - ');

      if (parts.length != 2) return 'N/A';

      final startTime = parts[0];
      final endTime = parts[1];

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
  } // Month name helper

  Widget _buildInfoItem(IconData icon, String text) {
    return IntrinsicWidth(
      child: Row(
        mainAxisSize: MainAxisSize.min,
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Icon(icon, size: 16, color: Colors.grey),
          const SizedBox(width: 8),
          Flexible(
            child: Text(
              text,
              style: const TextStyle(
                fontSize: 13,
                overflow: TextOverflow.ellipsis,
              ),
            ),
          ),
        ],
      ),
    );
  }

  // Empty State Widget
  Widget _buildEmptyState({
    required IconData icon,
    required String title,
    required String description,
    String? buttonText,
    VoidCallback? onButtonPressed,
  }) {
    return Center(
      child: Padding(
        padding: const EdgeInsets.all(32),
        child: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            Container(
              width: 80,
              height: 80,
              decoration: BoxDecoration(
                color: Colors.grey.shade100,
                shape: BoxShape.circle,
              ),
              child: Icon(icon, size: 40, color: Colors.grey.shade400),
            ),
            const SizedBox(height: 24),
            Text(
              title,
              style: const TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
            ),
            const SizedBox(height: 8),
            Text(
              description,
              textAlign: TextAlign.center,
              style: TextStyle(color: Colors.grey.shade600),
            ),
            if (buttonText != null && onButtonPressed != null) ...[
              const SizedBox(height: 24),
              ElevatedButton(
                onPressed: onButtonPressed,
                style: ElevatedButton.styleFrom(
                  backgroundColor: Colors.indigo,
                  foregroundColor: Colors.white,
                  padding: const EdgeInsets.symmetric(
                    horizontal: 24,
                    vertical: 12,
                  ),
                  shape: RoundedRectangleBorder(
                    borderRadius: BorderRadius.circular(12),
                  ),
                ),
                child: Text(buttonText),
              ),
            ],
          ],
        ),
      ),
    );
  }

  // Error State Widget
  Widget _buildErrorState() {
    return ListView(
      physics: const AlwaysScrollableScrollPhysics(),
      children: [
        SizedBox(height: MediaQuery.of(context).size.height * 0.3),
        Center(
          child: Padding(
            padding: const EdgeInsets.all(24),
            child: Column(
              mainAxisAlignment: MainAxisAlignment.center,
              children: [
                const Icon(Icons.error_outline, color: Colors.red, size: 60),
                const SizedBox(height: 16),
                Text(
                  _errorMessage,
                  style: const TextStyle(color: Colors.red, fontSize: 16),
                  textAlign: TextAlign.center,
                ),
                const SizedBox(height: 16),
                ElevatedButton(
                  onPressed: _fetchShifts,
                  child: const Text('Retry'),
                  style: ElevatedButton.styleFrom(
                    backgroundColor: Colors.indigo,
                    foregroundColor: Colors.white,
                    shape: RoundedRectangleBorder(
                      borderRadius: BorderRadius.circular(8),
                    ),
                  ),
                ),
              ],
            ),
          ),
        ),
      ],
    );
  }

  @override
  void dispose() {
    _searchController.dispose();
    _tabController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: StandardAppBar(title: 'Myshifts', centerTitle: true),
      backgroundColor: const Color(0xFFF4F8FD),
      body: GestureDetector(
        behavior: HitTestBehavior.opaque,
        onTap: () {
          // This will unfocus any text field and dismiss the keyboard
          FocusManager.instance.primaryFocus?.unfocus();
        },
        child: SafeArea(
          child:
              _isLoading
                  ? const Center(child: CircularProgressIndicator())
                  : _errorMessage.isNotEmpty
                  ? _buildErrorState()
                  : Column(
                    children: [
                      Container(child: buildSearchBar()),

                      _buildTabs(),
                      Expanded(
                        child: TabBarView(
                          controller: _tabController,
                          children: [
                            RefreshIndicator(
                              onRefresh: _fetchShifts,
                              color: Colors.indigo,
                              child: _buildUpcomingShifts(),
                            ),
                            RefreshIndicator(
                              onRefresh: _fetchShifts,
                              color: Colors.indigo,
                              child: _buildInProgressShifts(), // New method
                            ),
                            RefreshIndicator(
                              onRefresh: _fetchShifts,
                              color: Colors.indigo,
                              child: _buildCompletedShifts(),
                            ),
                            // RefreshIndicator(
                            // onRefresh: _fetchShifts,
                            //color: Colors.indigo,
                            //child: _buildCancelledShifts(),
                            //),
                          ],
                        ),
                      ),
                    ],
                  ),
        ),
      ),
      bottomNavigationBar: const ShiftHourBottomNavigation(),
    );
  }
}
