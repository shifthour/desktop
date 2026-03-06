import 'dart:async';

import 'package:flutter/material.dart';
import 'package:get/get.dart';
import 'package:get/get_core/src/get_main.dart';
import 'package:shifthour/utils/supabase_extensions.dart';
import 'package:shifthour/worker/Find%20Jobs/Find_Jobs.dart';
import 'package:shifthour/worker/Earnings/earnings_dashboard.dart';
import 'package:shifthour/worker/const/Botton_Navigation.dart';
import 'package:shifthour/worker/const/kyc.dart';
import 'package:shifthour/worker/const/notifications.dart';
import 'package:shifthour/worker/my_profile.dart';
import 'package:shifthour/worker/shifts/My_Shifts_Dashboard.dart';
import 'package:shifthour/worker/shifts/view_shifts.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:intl/intl.dart';
import 'package:url_launcher/url_launcher.dart';

class WorkerDashboard extends StatefulWidget {
  const WorkerDashboard({Key? key}) : super(key: key);

  @override
  State<WorkerDashboard> createState() => _WorkerDashboardState();
}

class _WorkerDashboardState extends State<WorkerDashboard>
    with NavigationMixin {
  String _userName = "Alex";
  String _usercode = ''; // Default name
  String? _userImageUrl;
  int _jobsCompleted = 32;
  double _totalEarnings = 0;
  List<Map<String, dynamic>> _upcomingShifts = [];
  bool _loadingShifts = true;
  int _completedShiftsCount = 0;
  bool _loadingShiftCounts = true;
  int _upcomingShiftsCount = 0;
  int _currentIndex = 0;
  bool _isLoading = true; // Keep this as true
  bool _isKycLoading = true; // Add this new variable
  String _isKycVerified = 'Inactive';
  final ScrollController _scrollController = ScrollController();

  @override
  void initState() {
    super.initState();
    setCurrentTab(0);
    setPageScrollController(_scrollController);

    // Start with KYC loading as true
    setState(() {
      _isKycLoading = true;
    });

    _loadUserData();
    _loadUpcomingShifts();
    _fetchShiftCounts();
    _loadkyc(); // This will set _isKycLoading to false when complete
  }

  Future<void> _fetchShiftCounts() async {
    setState(() {
      _loadingShiftCounts = true;
    });

    try {
      // Get the current user
      final user = Supabase.instance.client.auth.currentUser;
      if (user == null) {
        throw Exception('No authenticated user found');
      }

      // Fetch wallet data to get the balance
      final walletResponse =
          await Supabase.instance.client
              .from('worker_wallet')
              .select()
              .eq('worker_email', user.email as Object)
              .maybeSingle();

      // Fetch confirmed shifts count for the current user
      final confirmedResponse = await Supabase.instance.client
          .from('worker_job_applications')
          .select()
          .eq('user_id', user.id)
          .eq('application_status', 'Upcoming');

      // Fetch completed shifts count
      final completedResponse = await Supabase.instance.client
          .from('worker_job_applications')
          .select('pay_rate')
          .eq('user_id', user.id)
          .eq('application_status', 'Completed');

      // Parse balance from wallet data
      double totalBalance = 0.0;
      if (walletResponse != null && walletResponse['balance'] != null) {
        final balanceValue = walletResponse['balance'];
        if (balanceValue is int) {
          totalBalance = balanceValue.toDouble();
        } else if (balanceValue is double) {
          totalBalance = balanceValue;
        } else if (balanceValue is String) {
          totalBalance = double.tryParse(balanceValue) ?? 0.0;
        }
      }

      // Calculate total earnings from completed shifts
      double totalEarnings = 0.0;
      for (var shift in completedResponse) {
        final payRate = shift['pay_rate'];
        if (payRate != null) {
          totalEarnings += double.tryParse(payRate.toString()) ?? 0.0;
        }
      }

      setState(() {
        _upcomingShiftsCount = confirmedResponse.length;
        _completedShiftsCount = completedResponse.length;
        _totalEarnings =
            totalBalance; // Use wallet balance instead of calculating from shifts
        _loadingShiftCounts = false;
      });
    } on PostgrestException catch (e) {
      print('Supabase error fetching shift counts: ${e.message}');
      setState(() {
        _loadingShiftCounts = false;
        _totalEarnings = 0.0;
      });
    } on TimeoutException catch (e) {
      print('Timeout error fetching shift counts: $e');
      setState(() {
        _loadingShiftCounts = false;
        _totalEarnings = 0.0;
      });
    } catch (e) {
      print('Error fetching shift counts and earnings: $e');
      setState(() {
        _loadingShiftCounts = false;
        _totalEarnings = 0.0;
      });
    }
  }

  Future<void> _loadUserData() async {
    try {
      setState(() => _isLoading = true);

      // Get current user's info
      final user = Supabase.instance.client.auth.currentUser;
      if (user == null || user.email == null) {
        throw Exception('User not logged in or email not available');
      }

      final email = user.email!;

      try {
        // Get user data from database
        final response =
            await Supabase.instance.client
                .from('job_seekers')
                .select('*')
                .eq('email', email)
                .single();

        // Update state with user data
        setState(() {
          _userName = response['full_name'] ?? 'Alex';
          _usercode = response['job_seeker_code'] ?? 'N/A';
          _userImageUrl = response['profile_photo_url'];
          // You could also fetch these from your metrics table
          // _jobsCompleted = ...
          // _totalEarnings = ...
        });
      } catch (e) {
        print('ERROR: $e');
      }
    } catch (e) {
      print('Error loading user data: $e');
    } finally {
      setState(() => _isLoading = false);
    }
  }

  Future<void> _loadUpcomingShifts() async {
    try {
      setState(() => _loadingShifts = true);

      // Get current user
      final user = Supabase.instance.client.auth.currentUser;
      if (user == null) {
        throw Exception('No authenticated user found');
      }

      // Get today's date in yyyy-MM-dd format
      final today = DateFormat('yyyy-MM-dd').format(DateTime.now());

      try {
        // Fetch today's shifts with supervisor details
        final response = await Supabase.instance.client
            .from('worker_job_applications')
            .select('''
            *,
            worker_job_listings(
              supervisor_name,
              supervisor_phone,
              supervisor_email,
              job_pincode,
              category
            )
          ''')
            .eq('user_id', user.id)
            //.eq('application_status', 'Upcoming') // Uncomment if needed
            .eq('date', today)
            .order('start_time', ascending: true)
            .limit(20) // Safety limit - unlikely to have >20 shifts in one day
            .withTimeout(timeout: TimeoutDurations.query);

        print('DEBUG: Raw Response - ${response.length} shifts found');

        // Print out details of each shift
        response.forEach((shift) {
          print('DEBUG: Full Shift Details:');
          print(shift);
          print('Supervisor Details: ${shift['worker_job_listings']}');
        });

        setState(() {
          _upcomingShifts = List<Map<String, dynamic>>.from(response);
          _loadingShifts = false;
        });

        print('DEBUG: Loaded ${_upcomingShifts.length} shifts for today');
      } on PostgrestException catch (e) {
        print('DEBUG: Supabase error in shift query: ${e.message}');
        setState(() {
          _upcomingShifts = [];
          _loadingShifts = false;
        });
      } on TimeoutException catch (e) {
        print('DEBUG: Timeout error in shift query: $e');
        setState(() {
          _upcomingShifts = [];
          _loadingShifts = false;
        });
      } catch (e) {
        print('DEBUG: Detailed Error in shift query: $e');
        setState(() {
          _upcomingShifts = [];
          _loadingShifts = false;
        });
      }
    } on PostgrestException catch (e) {
      print('Supabase error loading shifts: ${e.message}');
      setState(() {
        _upcomingShifts = [];
        _loadingShifts = false;
      });
    } on TimeoutException catch (e) {
      print('Timeout error loading shifts: $e');
      setState(() {
        _upcomingShifts = [];
        _loadingShifts = false;
      });
    } catch (e) {
      print('Error loading shifts: $e');
      setState(() {
        _upcomingShifts = [];
        _loadingShifts = false;
      });
    }
  }

  String _formatCurrency(double amount) {
    return '\₹${amount.toStringAsFixed(0)}';
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: Colors.white,
      appBar: AppBar(
        elevation: 0,
        backgroundColor: Colors.white,
        title: const Text(
          'Dashboard',
          style: TextStyle(
            color: Colors.black,
            fontSize: 24,
            fontWeight: FontWeight.bold,
          ),
        ),
        centerTitle: true,
        actions: [
          IconButton(
            icon: const Icon(Icons.notifications_outlined, color: Colors.black),
            onPressed: () {
              Navigator.push(
                context,
                MaterialPageRoute(
                  builder: (context) => const NotificationsScreen(),
                ),
              );
              // Handle notifications
            },
          ),
        ],
      ),
      body:
          (_isLoading || _isKycLoading)
              ? const Center(child: CircularProgressIndicator())
              : RefreshIndicator(
                onRefresh: () async {
                  await _loadUserData();
                  await _loadUpcomingShifts();
                  await _fetchShiftCounts();
                  await _loadkyc();
                },
                child: SingleChildScrollView(
                  controller: _scrollController,
                  physics: const AlwaysScrollableScrollPhysics(),
                  child: Padding(
                    padding: const EdgeInsets.all(16.0),
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        // Greeting section

                        // Business verification banner
                        if (_isKycLoading)
                          const SizedBox.shrink()
                        else if (_isKycVerified == 'Inactive')
                          Padding(
                            padding: const EdgeInsets.only(bottom: 16),
                            child: Container(
                              width: double.infinity,
                              decoration: BoxDecoration(
                                color: Colors.amber.shade100,
                                borderRadius: BorderRadius.circular(8),
                              ),
                              child: Padding(
                                padding: const EdgeInsets.all(10),
                                child: Column(
                                  mainAxisAlignment:
                                      MainAxisAlignment.spaceBetween,
                                  children: [
                                    Row(
                                      children: [
                                        Icon(
                                          Icons.warning_amber_rounded,
                                          color: Colors.amber.shade800,
                                          size: 24,
                                        ),
                                        const SizedBox(width: 12),
                                        const Expanded(
                                          child: Text(
                                            'Your KYC verification is incomplete',
                                            style: TextStyle(
                                              fontFamily: 'Inter',
                                              fontWeight: FontWeight.w300,
                                            ),
                                          ),
                                        ),
                                      ],
                                    ),
                                    SizedBox(height: 8),

                                    ElevatedButton(
                                      onPressed: _openKycVerificationForm,
                                      style: ElevatedButton.styleFrom(
                                        backgroundColor: Colors.white,
                                        foregroundColor: Colors.amber.shade800,
                                        elevation: 0,
                                        shape: RoundedRectangleBorder(
                                          borderRadius: BorderRadius.circular(
                                            20,
                                          ),
                                        ),
                                        padding: const EdgeInsets.symmetric(
                                          horizontal: 16,
                                        ),
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
                        _buildGreeting(),
                        const SizedBox(height: 24),
                        // Stats cards
                        Row(
                          children: [
                            Expanded(
                              child: _buildStatsCard(
                                'Shifts Completed',
                                _loadingShiftCounts
                                    ? '...'
                                    : _completedShiftsCount.toString(),
                                Icons.work,
                                Colors.blue.shade100,
                              ),
                            ),
                            const SizedBox(width: 16),
                            Expanded(
                              child: _buildStatsCard(
                                'Earnings',
                                _formatCurrency(_totalEarnings),
                                Icons.account_balance_wallet_outlined,
                                Colors.green.shade100,
                              ),
                            ),
                          ],
                        ),
                        const SizedBox(height: 24),

                        // Quick access buttons
                        _buildQuickAccessButtons(),
                        const SizedBox(height: 24),

                        // Upcoming shifts section
                        _buildUpcomingShiftsSection(),
                      ],
                    ),
                  ),
                ),
              ),

      bottomNavigationBar: const ShiftHourBottomNavigation(),
    );
  }

  Future<void> _loadkyc() async {
    try {
      setState(() => _isKycLoading = true); // Use separate loading state

      // Get current user's email
      final user = Supabase.instance.client.auth.currentUser;
      if (user == null || user.email == null) {
        throw Exception('User not logged in or email not available');
      }

      final email = user.email!;

      try {
        final response =
            await Supabase.instance.client
                .from('documents')
                .select('*')
                .eq('email', email)
                .maybeSingle();

        if (response == null) {
          print('No verification record found for email: $email');
          setState(() {
            _isKycVerified = 'Inactive';
          });
          return;
        }

        print('DEBUG: BUSINESS VERIFICATION DATA:');
        response.forEach((key, value) {
          print('$key: $value');
        });

        setState(() {
          _isKycVerified = response['status'] ?? 'Inactive';
        });
      } catch (e) {
        print('ERROR in _loadkyc: $e');
        setState(() {
          _isKycVerified = 'Inactive';
        });
      }
    } catch (e) {
      print('Error loading KYC data: $e');
      setState(() {
        _isKycVerified = 'Inactive';
      });
    } finally {
      setState(() => _isKycLoading = false); // Set KYC loading to false
    }
  }

  void _openKycVerificationForm() {
    // Show the form as a full-screen dialog
    showDialog(
      context: context,
      barrierDismissible: false,
      builder: (BuildContext context) {
        return Dialog(
          insetPadding: EdgeInsets.zero,
          clipBehavior: Clip.antiAliasWithSaveLayer,
          child: Container(
            width: double.infinity,
            height: double.infinity,
            child: StandaloneVerificationForm(
              onComplete: () {
                // After verification, immediately refresh the verification status
                setState(() {
                  _isLoading = true; // Show loading state while refreshing
                });

                // Focus on refreshing just the KYC status first
                _loadkyc().then((_) {
                  // Then load the rest of the data
                  Future.wait([_fetchShiftCounts(), _loadUserData()]).then((_) {
                    if (mounted) {
                      setState(() {
                        _isLoading = false;
                      });
                    }
                  });
                });
              },
            ),
          ),
        );
      },
    );
  }

  Widget _buildGreeting() {
    // Get current date
    final now = DateTime.now();
    final formatter = DateFormat('EEEE, MMMM d');
    final formattedDate = formatter.format(now);

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Row(
          mainAxisAlignment: MainAxisAlignment.start,
          children: [
            // Status dot
            Container(
              width: 12,
              height: 12,
              margin: const EdgeInsets.only(right: 8),
              decoration: BoxDecoration(
                shape: BoxShape.circle,
                color: _getStatusColor(_isKycVerified),
              ),
            ),
            // Greeting text with date aligned below
            Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(
                  'Hello, $_userName!',
                  style: const TextStyle(
                    fontSize: 28,
                    fontWeight: FontWeight.bold,
                    color: Colors.black,
                  ),
                ),
                Text(
                  formattedDate,
                  style: TextStyle(fontSize: 18, color: Colors.grey.shade600),
                ),
                const SizedBox(height: 8),
                Container(
                  padding: const EdgeInsets.symmetric(
                    horizontal: 12,
                    vertical: 6,
                  ),
                  decoration: BoxDecoration(
                    color: Colors.blue.shade50,
                    borderRadius: BorderRadius.circular(20),
                    border: Border.all(color: Colors.blue.shade200),
                  ),
                  child: Text(
                    'Check In Code: $_usercode',
                    style: const TextStyle(
                      fontSize: 12,
                      color: Colors.blue,
                      fontWeight: FontWeight.w500,
                    ),
                  ),
                ),
              ],
            ),
          ],
        ),
      ],
    );
  }

  // Add this helper method to your class
  Color _getStatusColor(String status) {
    switch (status) {
      case 'Inactive':
      case 'Inactive': // Handle both spellings
        return Colors.red;
      case 'Active':
        return Colors.green;
      case 'InProgress':
      case 'Inprogress': // Handle both spellings
        return Colors.orange.shade400;
      default:
        return Colors.grey;
    }
  }

  Widget _buildStatsCard(
    String label,
    String value,
    IconData icon,
    Color bgColor, {
    VoidCallback? onTap,
  }) {
    return GestureDetector(
      onTap: onTap,
      child: Container(
        height: 150, // 🔒 Fixed height for uniform cards
        padding: const EdgeInsets.all(16),
        decoration: BoxDecoration(
          color: Colors.white,
          borderRadius: BorderRadius.circular(16),
          boxShadow: [
            BoxShadow(
              color: Colors.grey.withOpacity(0.1),
              spreadRadius: 1,
              blurRadius: 6,
              offset: const Offset(0, 2),
            ),
          ],
        ),
        child: Column(
          mainAxisAlignment: MainAxisAlignment.spaceBetween, // 🔄 Equal spacing
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Container(
              padding: const EdgeInsets.all(10),
              decoration: BoxDecoration(
                color: bgColor,
                borderRadius: BorderRadius.circular(12),
              ),
              child: Icon(
                icon,
                size: 24, // 🔧 Explicit size for consistency
                color:
                    bgColor == Colors.blue.shade100
                        ? Colors.blue
                        : Colors.green,
              ),
            ),
            Text(
              value,
              style: const TextStyle(
                fontSize: 24, // 🔧 Reduced for better fit
                fontWeight: FontWeight.bold,
                color: Colors.black,
              ),
              maxLines: 1,
              overflow: TextOverflow.ellipsis,
            ),
            Text(
              label,
              style: TextStyle(fontSize: 14, color: Colors.grey.shade600),
              maxLines: 1,
              overflow: TextOverflow.ellipsis,
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildQuickAccessButton(
    String label,
    IconData icon,
    Color iconColor,
    VoidCallback onTap,
  ) {
    return GestureDetector(
      onTap: () async {
        // Check if the user has completed KYC verification
        final user = Supabase.instance.client.auth.currentUser;
        if (user == null) {
          // Handle unauthenticated user
          ScaffoldMessenger.of(context).showSnackBar(
            SnackBar(
              content: Text('Please log in first'),
              backgroundColor: Colors.red,
            ),
          );
          return;
        }

        // Query the documents table for KYC status
        final verificationResponse =
            await Supabase.instance.client
                .from('documents')
                .select()
                .eq('email', user.email!)
                .maybeSingle();

        // Debug logging
        print('DEBUG: Verification Response: $verificationResponse');
        print('DEBUG: User Email: ${user.email}');

        final String status = verificationResponse?['status'] ?? 'Inactive';

        // More debug logging
        print('DEBUG: Extracted Status: "$status"');
        print('DEBUG: Status == Approved? ${status == 'Approved'}');
        print('DEBUG: Status != Approved? ${status != 'Approved'}');

        if (status != 'Approved' &&
            status != 'Verified' &&
            status != 'Active') {
          // Debug what's causing the dialog to show
          print('DEBUG: Showing dialog because status is: "$status"');

          // Determine title and message based on status
          String dialogTitle;
          String dialogMessage;
          IconData dialogIcon;

          switch (status) {
            case 'Pending':
            case 'Inprogress':
              dialogTitle = 'KYC Verification In Progress';
              dialogMessage =
                  'Your KYC verification is currently being reviewed. This process usually takes 1-2 hours. You will be notified once it\'s completed.';
              dialogIcon = Icons.pending_outlined;
              break;
            case 'Rejected':
              dialogTitle = 'KYC Verification Rejected';
              dialogMessage =
                  'Your KYC verification was rejected. Please review your documents and submit them again.';
              dialogIcon = Icons.error_outline;
              break;
            case 'Inactive':
            default:
              dialogTitle = 'KYC Verification Required';
              dialogMessage =
                  'You need to complete KYC verification before accessing this feature. This helps us maintain security and trust.';
              dialogIcon = Icons.verified_user_outlined;
          }

          showDialog(
            context: context,
            barrierDismissible: false,
            builder: (BuildContext context) {
              return Dialog(
                shape: RoundedRectangleBorder(
                  borderRadius: BorderRadius.circular(16),
                ),
                child: Container(
                  padding: const EdgeInsets.all(24),
                  decoration: BoxDecoration(
                    color: Colors.white,
                    borderRadius: BorderRadius.circular(16),
                    boxShadow: [
                      BoxShadow(
                        color: Colors.grey.shade300,
                        blurRadius: 10,
                        offset: const Offset(0, 4),
                      ),
                    ],
                  ),
                  child: Column(
                    mainAxisSize: MainAxisSize.min,
                    crossAxisAlignment: CrossAxisAlignment.center,
                    children: [
                      Container(
                        width: 80,
                        height: 80,
                        decoration: BoxDecoration(
                          color: iconColor.withOpacity(0.1),
                          shape: BoxShape.circle,
                        ),
                        child: Center(
                          child: Icon(dialogIcon, color: iconColor, size: 40),
                        ),
                      ),
                      const SizedBox(height: 16),
                      Text(
                        dialogTitle,
                        style: TextStyle(
                          fontSize: 18,
                          fontWeight: FontWeight.bold,
                          color: iconColor,
                          fontFamily: 'Inter',
                        ),
                        textAlign: TextAlign.center,
                      ),
                      const SizedBox(height: 12),
                      Text(
                        dialogMessage,
                        style: TextStyle(
                          color: Colors.grey.shade700,
                          fontSize: 14,
                          fontFamily: 'Inter',
                        ),
                        textAlign: TextAlign.center,
                      ),
                      const SizedBox(height: 24),
                      Row(
                        children: [
                          Expanded(
                            child: OutlinedButton(
                              onPressed: () => Navigator.pop(context),
                              style: OutlinedButton.styleFrom(
                                side: BorderSide(
                                  color: iconColor.withOpacity(0.2),
                                ),
                                foregroundColor: iconColor,
                                padding: const EdgeInsets.symmetric(
                                  vertical: 12,
                                ),
                                shape: RoundedRectangleBorder(
                                  borderRadius: BorderRadius.circular(12),
                                ),
                              ),
                              child: const Text('Cancel'),
                            ),
                          ),
                          const SizedBox(width: 16),
                          Expanded(
                            child: ElevatedButton(
                              onPressed: () {
                                Navigator.pop(context); // Close dialog
                                if (status == 'Pending' ||
                                    status == 'In Progress') {
                                  // Don't navigate to form if already in progress
                                  ScaffoldMessenger.of(context).showSnackBar(
                                    SnackBar(
                                      content: Text(
                                        'Your verification is already being processed.',
                                      ),
                                      backgroundColor: Colors.blue,
                                    ),
                                  );
                                } else {
                                  // Open the KYC verification form
                                  showDialog(
                                    context: context,
                                    barrierDismissible: false,
                                    builder: (BuildContext context) {
                                      return Dialog(
                                        insetPadding: EdgeInsets.zero,
                                        clipBehavior:
                                            Clip.antiAliasWithSaveLayer,
                                        child: Container(
                                          width: double.infinity,
                                          height: double.infinity,
                                          child: StandaloneVerificationForm(
                                            onComplete: () {
                                              // After verification, refresh the KYC status
                                              setState(() {
                                                _isLoading = true;
                                              });

                                              _loadkyc().then((_) {
                                                setState(() {
                                                  _isLoading = false;
                                                });
                                              });
                                            },
                                          ),
                                        ),
                                      );
                                    },
                                  );
                                }
                              },
                              style: ElevatedButton.styleFrom(
                                backgroundColor: iconColor,
                                foregroundColor: Colors.white,
                                padding: const EdgeInsets.symmetric(
                                  vertical: 12,
                                ),
                                shape: RoundedRectangleBorder(
                                  borderRadius: BorderRadius.circular(12),
                                ),
                                elevation: 2,
                              ),
                              child: Text(
                                status == 'Pending' || status == 'In Progress'
                                    ? 'Okay'
                                    : status == 'Rejected'
                                    ? 'Resubmit'
                                    : 'Verify Now',
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
        } else {
          // User is verified, proceed with the action
          print('DEBUG: KYC is verified, executing original onTap');
          onTap();
        }
      },
      child: Column(
        children: [
          Container(
            width: 64,
            height: 64,
            decoration: BoxDecoration(
              color: iconColor.withOpacity(0.1),
              borderRadius: BorderRadius.circular(16),
            ),
            child: Icon(icon, color: iconColor, size: 28),
          ),
          const SizedBox(height: 8),
          Text(
            label,
            style: const TextStyle(fontSize: 14, fontWeight: FontWeight.w500),
          ),
        ],
      ),
    );
  }

  Widget _buildQuickAccessButtons() {
    return Row(
      mainAxisAlignment: MainAxisAlignment.center,
      children: [
        _buildQuickAccessButton(
          'Find Shifts',
          Icons.search,
          Colors.blue,
          () => Get.to(() => FindJobsPage()),
        ),
        const SizedBox(
          width: 40,
        ), // Add 40 pixels of horizontal spacing between buttons
        _buildQuickAccessButton(
          'My Shifts',
          Icons.calendar_today,
          Colors.orange,
          () => Get.to(() => MyShiftsPage(initialTab: 0)),
        ),
        const SizedBox(width: 40),
        _buildQuickAccessButton(
          'Wallet',
          Icons.account_balance_wallet_outlined,
          Colors.green,
          () => Get.to(() => WorkerWalletScreen()),
        ),
      ],
    );
  }

  // Helper method to format time from "19:30:00" to "7:30 PM"
  String _formatTimeString(String timeString) {
    try {
      // Parse the time string
      final timeParts = timeString.split(':');
      if (timeParts.length < 2) return timeString; // Return original if invalid

      // Extract hours and minutes
      int hours = int.tryParse(timeParts[0]) ?? 0;
      final minutes = timeParts[1];

      // Determine AM/PM
      final period = hours >= 12 ? 'PM' : 'AM';

      // Convert to 12-hour format
      hours = hours > 12 ? hours - 12 : hours;
      hours = hours == 0 ? 12 : hours; // Handle midnight (0) as 12 AM

      // Return formatted time
      return '$hours:$minutes $period';
    } catch (e) {
      print('Error formatting time: $e');
      return timeString; // Return original in case of error
    }
  }

  // Add this boolean as a class member in _WorkerDashboardState
  bool _showAllShifts = false;

  Widget _buildUpcomingShiftsSection() {
    // Define the number of shifts to show initially
    const int initialShiftsToShow = 2;

    // Determine how many shifts to display
    final displayCount =
        _showAllShifts
            ? _upcomingShifts.length
            : (_upcomingShifts.length < initialShiftsToShow
                ? _upcomingShifts.length
                : initialShiftsToShow);

    return Column(
      children: [
        Row(
          mainAxisAlignment: MainAxisAlignment.spaceBetween,
          children: [
            const Text(
              'Today Shifts',
              style: TextStyle(fontSize: 20, fontWeight: FontWeight.bold),
            ),
            if (_upcomingShifts.isNotEmpty &&
                _upcomingShifts.length > initialShiftsToShow)
              TextButton(
                onPressed: () {
                  // Toggle between showing all shifts or just the initial count
                  setState(() {
                    _showAllShifts = !_showAllShifts;
                  });
                },
                child: Text(
                  _showAllShifts ? 'Show Less' : 'View All',
                  style: const TextStyle(fontSize: 16, color: Colors.blue),
                ),
              ),
          ],
        ),
        const SizedBox(height: 16),
        if (_loadingShifts)
          const Center(child: CircularProgressIndicator())
        else if (_upcomingShifts.isEmpty)
          _buildNoShiftsCard()
        else
          Column(
            children: List.generate(displayCount, (index) {
              try {
                // Get the shift at this index (safely)
                final shift = _upcomingShifts[index];

                // Extract data from each shift entry with null safety
                final title = shift['job_title'] ?? 'Position';
                final company = shift['company'] ?? 'Company';
                final location = shift['location'] ?? 'Location not specified';

                // Format the start and end times to 12-hour format
                final startTime = _formatTimeString(
                  shift['start_time'] ?? '9:00 AM',
                );
                final endTime = _formatTimeString(
                  shift['end_time'] ?? '5:00 PM',
                );

                final rate = (shift['pay_rate'] ?? 0.0).toDouble();

                // Determine if the shift is today or tomorrow
                String dateLabel = 'Upcoming';
                if (shift['date'] != null) {
                  try {
                    final shiftDate = DateTime.parse(shift['date'].toString());
                    final today = DateTime.now();
                    final tomorrow = DateTime(
                      today.year,
                      today.month,
                      today.day + 1,
                    );

                    if (shiftDate.year == today.year &&
                        shiftDate.month == today.month &&
                        shiftDate.day == today.day) {
                      dateLabel = 'Today';
                    } else if (shiftDate.year == tomorrow.year &&
                        shiftDate.month == tomorrow.month &&
                        shiftDate.day == tomorrow.day) {
                      dateLabel = 'Tomorrow';
                    }
                  } catch (e) {
                    print('Error parsing date: $e');
                  }
                }

                // Choose an icon based on job title
                IconData jobIcon = Icons.work;
                Color iconColor = Colors.blue;

                if (title.toLowerCase().contains('barista') ||
                    title.toLowerCase().contains('cafe') ||
                    title.toLowerCase().contains('coffee')) {
                  jobIcon = Icons.coffee;
                  iconColor = Colors.blue;
                } else if (title.toLowerCase().contains('server') ||
                    title.toLowerCase().contains('restaurant') ||
                    title.toLowerCase().contains('waiter')) {
                  jobIcon = Icons.restaurant;
                  iconColor = Colors.green;
                } else if (title.toLowerCase().contains('retail') ||
                    title.toLowerCase().contains('cashier') ||
                    title.toLowerCase().contains('sales')) {
                  jobIcon = Icons.shopping_bag;
                  iconColor = Colors.purple;
                } else if (title.toLowerCase().contains('driver') ||
                    title.toLowerCase().contains('delivery')) {
                  jobIcon = Icons.delivery_dining;
                  iconColor = Colors.orange;
                }

                // Check if this job might include tips
                bool hasTips =
                    shift['tips_included'] == true ||
                    title.toLowerCase().contains('server') ||
                    title.toLowerCase().contains('waiter') ||
                    title.toLowerCase().contains('bartender');
                print('Shift data: $shift');

                return Padding(
                  padding: const EdgeInsets.only(bottom: 16),
                  child: _buildShiftCard(
                    jobTitle: title,
                    category:
                        shift['worker_job_listings']?['category']?.toString() ??
                        'Other',
                    shiftId: shift['shift_id']?.toString() ?? 'N/A',

                    appstatus: shift['application_status'] ?? 'N/A',
                    role: company,
                    date: shift['date']?.toString() ?? '',
                    startTime: shift['start_time']?.toString() ?? '',
                    endTime: shift['end_time']?.toString() ?? '',
                    location: location,
                    pincode:
                        shift['worker_job_listings']?['job_pincode']
                            ?.toString() ??
                        'N/A',
                    isAssigned:
                        shift['worker_job_listings'] != null &&
                        shift['worker_job_listings']['supervisor_name'] != null,
                    supervisorName:
                        shift['worker_job_listings']?['supervisor_name'],
                    supervisorPhoneNumber:
                        shift['worker_job_listings']?['supervisor_phone'],
                    supervisorEmail:
                        shift['worker_job_listings']?['supervisor_email'],
                    supervisorAvatarColor: iconColor,
                    supervisorAvatarText:
                        shift['worker_job_listings']?['supervisor_name'] != null &&
                                (shift['worker_job_listings']['supervisor_name'] as String).isNotEmpty
                            ? (shift['worker_job_listings']['supervisor_name'] as String)[0]
                                .toUpperCase()
                            : '?',
                  ),
                );
              } catch (e) {
                print('Error building shift card: $e');
                // Return an error card instead of crashing
                return Padding(
                  padding: const EdgeInsets.only(bottom: 16),
                  child: Container(
                    padding: const EdgeInsets.all(16),
                    decoration: BoxDecoration(
                      border: Border.all(color: Colors.red.shade200),
                      borderRadius: BorderRadius.circular(16),
                    ),
                    child: const Text(
                      'Error loading shift details',
                      style: TextStyle(color: Colors.red),
                    ),
                  ),
                );
              }
            }),
          ),
      ],
    );
  }

  Widget _buildNoShiftsCard() {
    return Container(
      padding: const EdgeInsets.all(24),
      decoration: BoxDecoration(
        color: Colors.grey.shade100,
        borderRadius: BorderRadius.circular(16),
      ),
      child: Center(
        child: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            Icon(Icons.event_busy, size: 48, color: Colors.grey.shade400),
            const SizedBox(height: 16),
            Text(
              'No upcoming shifts',
              style: TextStyle(
                fontSize: 16,
                fontWeight: FontWeight.w500,
                color: Colors.grey.shade700,
              ),
            ),
            const SizedBox(height: 8),
            ElevatedButton(
              onPressed: () => Get.to(() => const FindJobsPage()),
              child: const Text('Find Shifts'),
              style: ElevatedButton.styleFrom(
                backgroundColor: Colors.blue,
                foregroundColor: Colors.white,
              ),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildShiftCard({
    required String jobTitle,
    required String category,
    required String shiftId,
    required String appstatus,
    required String role,
    required String date,
    required String startTime,
    required String endTime,
    required String location,
    required String pincode,
    bool isAssigned = false,
    String? supervisorName,
    String? supervisorPhoneNumber,
    String? supervisorEmail,
    Color? supervisorAvatarColor,
    String? supervisorAvatarText,
    double? supervisorRating,
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
        default:
          return Color(0x1AEF4444); // Default light red
      }
    }

    // Calculate shift duration
    String duration = _calculateShiftDuration(startTime, endTime);

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
                color: getStatusBackgroundColor(appstatus),
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
                        color: getStatusColor(appstatus),
                        borderRadius: BorderRadius.circular(16),
                      ),
                      child: Text(
                        appstatus,
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
                            'Company: $role',
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
                        '${_formatJobDatee(date)}',
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
                        '${_formatJobTime(startTime)} - ${_formatJobTime(endTime)}',
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
                          final address = '${location ?? ''}';
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
                  Divider(
                    height: 24,
                    thickness: 1,
                    color: Colors.grey.shade200,
                  ),

                  // Supervisor Assigned Section
                  if (isAssigned && supervisorName != null) ...[
                    Text(
                      'Contact Details',
                      style: TextStyle(
                        fontSize: 14,
                        fontWeight: FontWeight.w600,
                      ),
                    ),
                    SizedBox(height: 12),
                    Row(
                      children: [
                        CircleAvatar(
                          radius: 25,
                          backgroundColor: supervisorAvatarColor ?? Colors.blue,
                          child: Text(
                            supervisorAvatarText ??
                                supervisorName[0].toUpperCase(),
                            style: TextStyle(color: Colors.white, fontSize: 20),
                          ),
                        ),
                        SizedBox(width: 12),
                        Expanded(
                          child: Column(
                            crossAxisAlignment: CrossAxisAlignment.start,
                            children: [
                              Text(
                                supervisorName,
                                style: TextStyle(
                                  fontSize: 16,
                                  fontWeight: FontWeight.w600,
                                ),
                              ),
                              SizedBox(height: 4),
                              if (supervisorRating != null)
                                Row(
                                  children: [
                                    Icon(
                                      Icons.star,
                                      size: 16,
                                      color: Colors.amber,
                                    ),
                                    SizedBox(width: 4),
                                    Text(
                                      supervisorRating.toString(),
                                      style: TextStyle(fontSize: 14),
                                    ),
                                  ],
                                ),
                              SizedBox(height: 8),
                              Row(
                                children: [
                                  Icon(
                                    Icons.phone,
                                    size: 16,
                                    color: Colors.blue,
                                  ),
                                  SizedBox(width: 4),
                                  Text(supervisorPhoneNumber ?? 'N/A'),
                                ],
                              ),
                              SizedBox(height: 4),
                              Row(
                                children: [
                                  Icon(
                                    Icons.email,
                                    size: 16,
                                    color: Colors.blue,
                                  ),
                                  SizedBox(width: 4),
                                  Text(supervisorEmail ?? 'N/A'),
                                ],
                              ),
                            ],
                          ),
                        ),
                      ],
                    ),
                    // Add this section for Call and Message buttons
                    SizedBox(height: 16),
                    Row(
                      children: [
                        SizedBox(width: 12),
                        Expanded(
                          child: OutlinedButton.icon(
                            icon: Icon(Icons.message, size: 18),
                            label: Text('Message'),
                            onPressed: () async {
                              final Uri smsUri = Uri(
                                scheme: 'sms',
                                path: supervisorPhoneNumber,
                              );
                              if (await canLaunchUrl(smsUri)) {
                                await launchUrl(smsUri);
                              } else {
                                ScaffoldMessenger.of(context).showSnackBar(
                                  SnackBar(
                                    content: Text(
                                      'Could not open messaging app',
                                    ),
                                  ),
                                );
                              }
                            },
                            style: OutlinedButton.styleFrom(
                              foregroundColor: Colors.blue,
                              side: BorderSide(color: Colors.blue.shade200),
                              padding: EdgeInsets.symmetric(vertical: 12),
                            ),
                          ),
                        ),
                        Expanded(
                          child: ElevatedButton.icon(
                            icon: Icon(Icons.call, size: 18),
                            label: Text('Call'),
                            onPressed:
                                supervisorPhoneNumber != null
                                    ? () async {
                                      final Uri telUri = Uri(
                                        scheme: 'tel',
                                        path: supervisorPhoneNumber,
                                      );
                                      if (await canLaunchUrl(telUri)) {
                                        await launchUrl(telUri);
                                      } else {
                                        ScaffoldMessenger.of(
                                          context,
                                        ).showSnackBar(
                                          SnackBar(
                                            content: Text(
                                              'Could not launch dialer',
                                            ),
                                          ),
                                        );
                                      }
                                    }
                                    : null,
                            style: ElevatedButton.styleFrom(
                              backgroundColor: Colors.blue,
                              foregroundColor: Colors.white,
                              padding: EdgeInsets.symmetric(vertical: 12),
                            ),
                          ),
                        ),
                      ],
                    ),
                  ] else ...[
                    // Clear message if no supervisor assigned
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
                            'No supervisor assigned yet.',
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
          ],
        ),
      ),
    );
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

  // Month name helper
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
}
