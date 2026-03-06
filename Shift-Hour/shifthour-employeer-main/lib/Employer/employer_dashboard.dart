import 'dart:async';

import 'package:flutter/material.dart';
import 'package:get/get.dart';
import 'package:intl/intl.dart';
import 'package:shifthour_employeer/Employer/payments/employeer_payments_dashboard.dart';
import 'package:shifthour_employeer/Employer/post%20jobs/post_job.dart';
import 'package:shifthour_employeer/const/Notification.dart';
import 'package:shifthour_employeer/const/Notification_badge.dart';
import 'package:shifthour_employeer/utils/supabase_extensions.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
// Import your specific files
import 'package:shifthour_employeer/Employer/Business%20Kyc/kyc.dart';
import 'package:shifthour_employeer/Employer/Employee%20%20workers/Eworkers_dashboard.dart';
import 'package:shifthour_employeer/Employer/Manage%20Jobs/manage_jobs_dashboard.dart';
import 'package:shifthour_employeer/Employer/payments/payments.model.dart';
import 'package:shifthour_employeer/const/Bottom_Navigation.dart'
    show NavigationController, NavigationMixin, ShiftHourBottomNavigation;
import 'package:shifthour_employeer/login.dart';
import 'package:url_launcher/url_launcher.dart';

class EmployerDashboard extends StatefulWidget {
  const EmployerDashboard({Key? key}) : super(key: key);

  @override
  State<EmployerDashboard> createState() => _EmployerDashboardState();
}

class _EmployerDashboardState extends State<EmployerDashboard> {
  final scaffoldKey = GlobalKey<ScaffoldState>();
  String _isBusinessVerified = 'Inactive';

  bool _isLoading = true;
  String _userName = "";
  String _usercompany = "";
  String _contactemail = "";
  int _totalShifts = 0;
  int _activeWorkers = 0;
  int _shiftsToday = 0;
  bool _isRefreshing = false;
  bool _showAllShifts = false;

  List<Applicant> _applicants = []; // Default value for shifts today
  final ScrollController _scrollController = ScrollController();
  final PaymentsController _paymentsController = Get.put(PaymentsController());

  @override
  void initState() {
    super.initState();
    // Set current tab to Dashboard (index 0) when this page loads
    WidgetsBinding.instance.addPostFrameCallback((_) {
      // Get the navigation controller and set index without triggering navigation
      try {
        final navigationController = Get.find<NavigationController>();
        navigationController.setScrollController(_scrollController);
      } catch (e) {
        final navigationController = Get.put(
          NavigationController(),
          permanent: true,
        );
        navigationController.setScrollController(_scrollController);
      }

      // Load employer data
      _loadUserData();
      _loadProfileData();
      _loadkyc();
      _loadAllData();
      _fetchApplicants();
    });
  }

  Future<void> _fetchApplicants() async {
    try {
      final user = Supabase.instance.client.auth.currentUser;
      if (user == null) {
        setState(() => _applicants = []);
        return;
      }

      final today = DateTime.now().toIso8601String().substring(
        0,
        10,
      ); // 'YYYY-MM-DD'

      // Step 1: Fetch today's job listings posted by current user
      final jobListings = await Supabase.instance.client
          .from('worker_job_listings')
          .select(
            'id, job_title, start_time, end_time, location, shift_id, job_pincode,date,category,company',
          )
          .eq('contact_email', user.email as Object)
          .eq('date', today)
          .withTimeout(timeout: TimeoutDurations.query);

      final jobIds = jobListings.map((j) => j['id'] as String).toList();

      if (jobIds.isEmpty) {
        setState(() => _applicants = []);
        return;
      }

      // Step 2: Fetch applications only for those job IDs
      final applications = await Supabase.instance.client
          .from('worker_job_applications')
          .select()
          .filter('job_id', 'in', jobIds)
          .withTimeout(timeout: TimeoutDurations.query);

      // Step 3: Merge listings + applications
      final List<Applicant> finalList = [];

      for (final job in jobListings) {
        final matchingApp = applications.firstWhere(
          (app) => app['job_id'] == job['id'],
          orElse: () => <String, dynamic>{},
        );

        // Fix this line to check if the map is not empty
        final bool isAssigned = matchingApp.isNotEmpty;

        final applicant = Applicant(
          name:
              isAssigned
                  ? matchingApp['full_name'] ?? 'Worker'
                  : 'No Worker Assigned',
          role: job['company'] ?? '',
          rating:
              isAssigned ? (matchingApp['worker_rating'] ?? 0).toDouble() : 0.0,
          jobTitle: job['job_title'] ?? '',
          category: job['category'] ?? 'Others',
          shiftId: job['shift_id'],
          startTime: job['start_time'] ?? '',
          appstatus:
              isAssigned
                  ? matchingApp['application_status'] ?? 'Pending'
                  : 'Pending',
          endTime: job['end_time'] ?? '',
          location: job['location'] ?? '',
          phoneNumber: isAssigned ? matchingApp['phone_number'] ?? '' : '',
          email: isAssigned ? matchingApp['email'] ?? '' : '',
          avatarText:
              isAssigned
                  ? (matchingApp['worker_name'] ?? 'W')
                      .substring(0, 1)
                      .toUpperCase()
                  : 'NA',
          avatarColor: isAssigned ? Colors.blue : Colors.grey,
          isAssigned: isAssigned,
          pincode: job['job_pincode']?.toString() ?? '',
          date: job['date']?.toString() ?? '',
        );

        finalList.add(applicant);
      }

      setState(() => _applicants = finalList);
    } on PostgrestException catch (e) {
      print('Supabase error fetching applicants: ${e.message}');
      setState(() => _applicants = []);
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(
            content: Text('Failed to load today\'s shifts. Please try again.'),
            backgroundColor: Colors.red,
          ),
        );
      }
    } on TimeoutException catch (e) {
      print('Timeout error fetching applicants: $e');
      setState(() => _applicants = []);
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(
            content: Text('Request timed out. Please check your connection.'),
            backgroundColor: Colors.orange,
          ),
        );
      }
    } catch (e) {
      print('Error fetching applicants: $e');
      setState(() => _applicants = []);
    }
  }

  Future<String?> _getWorkerPhotoUrl(String email) async {
    try {
      final response =
          await Supabase.instance.client
              .from('documents')
              .select('photo_image_url')
              .eq('email', email)
              .maybeSingle();

      return response?['photo_image_url'];
    } catch (e) {
      print('Error fetching worker photo: $e');
      return null;
    }
  }

  Future<void> _loadAllData() async {
    setState(() => _isLoading = true);

    try {
      await Future.wait([
        _loadUserData(),
        _loadProfileData(),
        _loadkyc(),
        _fetchApplicants(),
      ]);
    } catch (e) {
      print('Error loading data: $e');
    } finally {
      if (mounted) {
        setState(() => _isLoading = false);
      }
    }
  }

  Future<void> _onRefresh() async {
    setState(() => _isRefreshing = true);

    try {
      await _loadAllData();
    } catch (e) {
      print('Error refreshing data: $e');
    } finally {
      if (mounted) {
        setState(() => _isRefreshing = false);
      }
    }
  }

  Future<void> _loadUserData() async {
    try {
      setState(() => _isLoading = true);

      final user = Supabase.instance.client.auth.currentUser;
      if (user == null || user.email == null) {
        throw Exception('User not logged in or email not available');
      }

      final email = user.email!;
      final today = DateTime.now().toIso8601String().substring(
        0,
        10,
      ); // 'YYYY-MM-DD'

      try {
        // Fetch total shifts count from worker_job_listings
        // First get all worker_job_listings for the user
        // Get all shift_ids from worker_job_listings for the current user
        final userListings = await Supabase.instance.client
            .from('worker_job_listings')
            .select('shift_id')
            .eq('contact_email', email)
            .withTimeout(timeout: TimeoutDurations.query);

        // Extract the list of shift_ids
        final userShiftIds =
            userListings.map((listing) => listing['shift_id']).toList();

        print('User has ${userShiftIds.length} total shifts');

        // Get completed applications that match the user's shift_ids
        final completedApplications = await Supabase.instance.client
            .from('worker_job_applications')
            .select('shift_id')
            .eq('application_status', 'Completed')
            .inFilter('shift_id', userShiftIds)
            .withTimeout(timeout: TimeoutDurations.query);

        // Extract the shift_ids from completed applications
        final completedShiftIds =
            completedApplications.map((app) => app['shift_id']).toSet();

        print('Found ${completedShiftIds.length} completed shifts');

        // Calculate non-completed shifts by removing completed shift_ids
        final nonCompletedShiftIds =
            userShiftIds
                .where((shiftId) => !completedShiftIds.contains(shiftId))
                .toList();

        final nonCompletedCount = nonCompletedShiftIds.length;
        print('Number of non-completed shifts: $nonCompletedCount');

        // Set the state variable
        setState(() {
          _totalShifts = nonCompletedCount;
        });

        // Fetch active workers count by joining worker_job_listings and worker_job_applications
        final activeWorkersResponse =
            await Supabase.instance.client
                .from('worker_job_listings')
                .select('worker_job_applications!inner(*)')
                .eq('contact_email', email)
                .eq('worker_job_applications.application_status', 'In Progress')
                .withTimeout(timeout: TimeoutDurations.query)
                .count();

        // Fetch shifts for today
        final shiftsForTodayResponse =
            await Supabase.instance.client
                .from('worker_job_listings')
                .select()
                .eq('contact_email', email)
                .eq('date', today)
                .withTimeout(timeout: TimeoutDurations.query)
                .count();

        if (mounted) {
          setState(() {
            _totalShifts = _totalShifts ?? 0;
            _activeWorkers = activeWorkersResponse.count ?? 0;
            _shiftsToday = shiftsForTodayResponse.count ?? 0;
          });
        }
      } on PostgrestException catch (e) {
        print('Supabase error loading user data: ${e.message}');
      } on TimeoutException catch (e) {
        print('Timeout error loading user data: $e');
      } catch (e) {
        print('ERROR: $e');
      }
    } on PostgrestException catch (e) {
      print('Supabase error loading user data: ${e.message}');
    } on TimeoutException catch (e) {
      print('Timeout error loading user data: $e');
    } catch (e) {
      print('Error loading user data: $e');
    } finally {
      if (mounted) {
        setState(() => _isLoading = false);
      }
    }
  }

  Future<void> _loadProfileData() async {
    try {
      setState(() => _isLoading = true);

      // Get current user's email
      final user = Supabase.instance.client.auth.currentUser;
      if (user == null || user.email == null) {
        throw Exception('User not logged in or email not available');
      }

      final email = user.email!;

      try {
        // Check what data exists in job_seekers table
        final response =
            await Supabase.instance.client
                .from('employers')
                .select('*')
                .eq('contact_email', email)
                .withTimeout(timeout: TimeoutDurations.query)
                .single();

        // Log all key-value pairs
        print('DEBUG: JOB SEEKER DATA:');
        response.forEach((key, value) {
          print('$key: $value');
        });

        // Then update state with confirmed field name
        setState(() {
          _userName = response['contact_name'] ?? 'Employer';
          _usercompany = response['company_name'];
          _contactemail = response['contact_email'];
        });
      } on PostgrestException catch (e) {
        print('Supabase error loading profile: ${e.message}');
      } on TimeoutException catch (e) {
        print('Timeout error loading profile: $e');
      } catch (e) {
        print('ERROR: $e');
      }
    } on PostgrestException catch (e) {
      print('Supabase error loading profile: ${e.message}');
    } on TimeoutException catch (e) {
      print('Timeout error loading profile: $e');
    } catch (e) {
      print('Error loading user data: $e');
      // Keep default values
    } finally {
      // We don't set _isLoading to false here as it's done in _loadUserData
    }
  }

  Future<void> _loadkyc() async {
    try {
      setState(() => _isLoading = true);

      // Get current user's email
      final user = Supabase.instance.client.auth.currentUser;
      if (user == null || user.email == null) {
        throw Exception('User not logged in or email not available');
      }

      final email = user.email!;

      try {
        // Use .select() without .single() to handle no rows
        final response =
            await Supabase.instance.client
                .from('business_verifications')
                .select('*')
                .eq('email', email)
                .withTimeout(timeout: TimeoutDurations.query)
                .maybeSingle(); // Use maybeSingle instead of single

        // Check if response is null
        if (response == null) {
          print('No verification record found for email: $email');
          setState(() {
            _isBusinessVerified = 'Inactive';
          });
          return;
        }

        // Log all key-value pairs
        print('DEBUG: BUSINESS VERIFICATION DATA:');
        response.forEach((key, value) {
          print('$key: $value');
        });

        // Update state with verification status
        setState(() {
          _isBusinessVerified = response['status'] ?? '';
        });
      } catch (e) {
        print('ERROR in _loadkyc: $e');
        setState(() {
          _isBusinessVerified = 'Inactive';
        });
      }
    } catch (e) {
      print('Error loading KYC data: $e');
      setState(() {
        _isBusinessVerified = 'Inactive';
      });
    } finally {
      setState(() => _isLoading = false);
    }
  }

  void _openBusinessVerificationForm() {
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
                  Future.wait([_loadProfileData(), _loadUserData()]).then((_) {
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

  @override
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
                color: _getStatusColor(_isBusinessVerified),
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
        return Colors.yellow;
      default:
        return Colors.grey;
    }
  }

  Widget build(BuildContext context) {
    return GestureDetector(
      onTap: () {
        FocusScope.of(context).unfocus();
        FocusManager.instance.primaryFocus?.unfocus();
      },
      child: Scaffold(
        key: scaffoldKey,
        backgroundColor: Color(0xFFF9FAFB),

        body: SafeArea(
          top: true,
          child: Column(
            mainAxisSize: MainAxisSize.max,
            children: [
              // App Bar
              AppBar(
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
                  Padding(
                    padding: const EdgeInsets.only(
                      right: 26.0,
                    ), // Adjust this as needed
                    child:
                        IconButton(
                          icon: const Icon(
                            Icons.notifications,
                            color: Colors.black,
                          ),
                          onPressed: () => Get.toNamed('/notifications'),
                        ).withNotificationBadge(),
                  ),
                ],
              ), // Business Verification Alert (if needed)
              if (_isLoading)
                const SizedBox.shrink()
              else if (_isBusinessVerified == 'Inactive')
                Padding(
                  padding: const EdgeInsets.fromLTRB(16, 10, 16, 16),
                  child: Container(
                    width: double.infinity,
                    decoration: BoxDecoration(
                      color: Colors.amber.shade100,
                      borderRadius: BorderRadius.circular(8),
                    ),
                    child: Padding(
                      padding: const EdgeInsets.all(10),
                      child: Column(
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
                              const Expanded(
                                child: Text(
                                  'Your business verification is incomplete',
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
                            onPressed: _openBusinessVerificationForm,
                            style: ElevatedButton.styleFrom(
                              backgroundColor: Colors.white,
                              foregroundColor: Colors.amber.shade800,
                              elevation: 0,
                              shape: RoundedRectangleBorder(
                                borderRadius: BorderRadius.circular(20),
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

              // Content
              Expanded(
                child: RefreshIndicator(
                  onRefresh: _onRefresh,
                  child: Padding(
                    padding: EdgeInsets.all(16),
                    child:
                        _isLoading
                            ? Center(child: CircularProgressIndicator())
                            : SingleChildScrollView(
                              controller: _scrollController,
                              physics:
                                  AlwaysScrollableScrollPhysics(), // Important for pull-to-refresh
                              child: Column(
                                mainAxisSize: MainAxisSize.max,
                                crossAxisAlignment: CrossAxisAlignment.start,
                                children: [
                                  _buildGreeting(),
                                  const SizedBox(height: 24),
                                  // Stats Row
                                  Padding(
                                    padding: EdgeInsetsDirectional.fromSTEB(
                                      0,
                                      0,
                                      0,
                                      24,
                                    ),
                                    child: Row(
                                      mainAxisSize: MainAxisSize.max,
                                      children: [
                                        // Open Shifts
                                        Expanded(
                                          child: _buildStatCard(
                                            icon: Icons.work,
                                            iconColor: Color(0xFF2563EB),
                                            iconBgColor: Color(0x1A2563EB),
                                            value: '$_totalShifts',
                                            label: 'Open\nShifts',
                                          ),
                                        ),

                                        // Active Workers
                                        Expanded(
                                          child: _buildStatCard(
                                            icon: Icons.people,
                                            iconColor: Color(0xFF10B981),
                                            iconBgColor: Color(0x1A10B981),
                                            value: '$_activeWorkers',
                                            label: 'Active\nWorkers',
                                          ),
                                        ),

                                        // Shifts Today
                                        Expanded(
                                          child: _buildStatCard(
                                            icon: Icons.today,
                                            iconColor: Color(0xFFF59E0B),
                                            iconBgColor: Color(0x1AF59E0B),
                                            value: '$_shiftsToday',
                                            label: 'Today\nShifts',
                                          ),
                                        ),
                                      ],
                                    ),
                                  ),

                                  // Quick Actions
                                  Padding(
                                    padding: EdgeInsetsDirectional.fromSTEB(
                                      0,
                                      0,
                                      0,
                                      24,
                                    ),
                                    child: Row(
                                      mainAxisSize: MainAxisSize.max,
                                      mainAxisAlignment:
                                          MainAxisAlignment.spaceBetween,
                                      children: [
                                        // Find the _buildQuickAction method in EmployerDashboard and modify the onTap function for "Post Shifts"
                                        _buildQuickAction(
                                          icon: Icons.add_circle,
                                          label: 'Post Shifts',
                                          color: Color(0xFF2563EB),
                                          bgColor: Color(0x1A2563EB),

                                          onTap: () async {
                                            // Check if the user has completed business verification
                                            final user =
                                                Supabase
                                                    .instance
                                                    .client
                                                    .auth
                                                    .currentUser;
                                            if (user == null) {
                                              ScaffoldMessenger.of(
                                                context,
                                              ).showSnackBar(
                                                SnackBar(
                                                  content: Text(
                                                    'Please log in first',
                                                  ),
                                                  backgroundColor: Colors.red,
                                                ),
                                              );
                                              return;
                                            }
                                            final color = Color(0xFF2563EB);
                                            final bgColor = Color(0x1A2563EB);

                                            // Query the business_verifications table
                                            final verificationResponse =
                                                await Supabase.instance.client
                                                    .from(
                                                      'business_verifications',
                                                    )
                                                    .select()
                                                    .eq('email', user.email!)
                                                    .maybeSingle(); // Remove the .eq('status', 'Active') filter

                                            // Check if the status is Active
                                            final status =
                                                verificationResponse?['status']
                                                    ?.toString()
                                                    .trim() ??
                                                '';

                                            if (status.toLowerCase() !=
                                                'active') {
                                              // Show dialog if not active
                                              Get.dialog(
                                                Dialog(
                                                  shape: RoundedRectangleBorder(
                                                    borderRadius:
                                                        BorderRadius.circular(
                                                          16,
                                                        ),
                                                  ),
                                                  child: Container(
                                                    padding:
                                                        const EdgeInsets.all(
                                                          24,
                                                        ),
                                                    decoration: BoxDecoration(
                                                      color: Colors.white,
                                                      borderRadius:
                                                          BorderRadius.circular(
                                                            16,
                                                          ),
                                                      boxShadow: [
                                                        BoxShadow(
                                                          color:
                                                              Colors
                                                                  .grey
                                                                  .shade300,
                                                          blurRadius: 10,
                                                          offset: const Offset(
                                                            0,
                                                            4,
                                                          ),
                                                        ),
                                                      ],
                                                    ),
                                                    child: Column(
                                                      mainAxisSize:
                                                          MainAxisSize.min,
                                                      crossAxisAlignment:
                                                          CrossAxisAlignment
                                                              .center,
                                                      children: [
                                                        Container(
                                                          width: 80,
                                                          height: 80,
                                                          decoration:
                                                              BoxDecoration(
                                                                color: bgColor
                                                                    .withOpacity(
                                                                      0.1,
                                                                    ),
                                                                shape:
                                                                    BoxShape
                                                                        .circle,
                                                              ),
                                                          child: Center(
                                                            child: Icon(
                                                              Icons
                                                                  .verified_outlined,
                                                              color: color,
                                                              size: 40,
                                                            ),
                                                          ),
                                                        ),
                                                        const SizedBox(
                                                          height: 16,
                                                        ),
                                                        Text(
                                                          'Business Verification Required',
                                                          style: TextStyle(
                                                            fontSize: 18,
                                                            fontWeight:
                                                                FontWeight.bold,
                                                            color: color,
                                                            fontFamily: 'Inter',
                                                          ),
                                                          textAlign:
                                                              TextAlign.center,
                                                        ),
                                                        const SizedBox(
                                                          height: 12,
                                                        ),
                                                        Text(
                                                          'You need to complete business verification before posting a shift. This helps us maintain the quality and authenticity of job listings.',
                                                          style: TextStyle(
                                                            color:
                                                                Colors
                                                                    .grey
                                                                    .shade700,
                                                            fontSize: 14,
                                                            fontFamily: 'Inter',
                                                          ),
                                                          textAlign:
                                                              TextAlign.center,
                                                        ),
                                                        const SizedBox(
                                                          height: 24,
                                                        ),
                                                        Row(
                                                          children: [
                                                            Expanded(
                                                              child: OutlinedButton(
                                                                onPressed:
                                                                    () =>
                                                                        Get.back(),
                                                                style: OutlinedButton.styleFrom(
                                                                  side: BorderSide(
                                                                    color: color
                                                                        .withOpacity(
                                                                          0.2,
                                                                        ),
                                                                  ),
                                                                  foregroundColor:
                                                                      color,
                                                                  padding:
                                                                      const EdgeInsets.symmetric(
                                                                        vertical:
                                                                            12,
                                                                      ),
                                                                  shape: RoundedRectangleBorder(
                                                                    borderRadius:
                                                                        BorderRadius.circular(
                                                                          12,
                                                                        ),
                                                                  ),
                                                                ),
                                                                child:
                                                                    const Text(
                                                                      'Cancel',
                                                                    ),
                                                              ),
                                                            ),
                                                            const SizedBox(
                                                              width: 16,
                                                            ),
                                                            Expanded(
                                                              child: ElevatedButton(
                                                                onPressed: () {
                                                                  Get.back(); // Close dialog
                                                                  Get.to(
                                                                    () =>
                                                                        const StandaloneVerificationForm(),
                                                                  ); // Navigate to verification form
                                                                },
                                                                style: ElevatedButton.styleFrom(
                                                                  backgroundColor:
                                                                      color,
                                                                  foregroundColor:
                                                                      Colors
                                                                          .white,
                                                                  padding:
                                                                      const EdgeInsets.symmetric(
                                                                        vertical:
                                                                            12,
                                                                      ),
                                                                  shape: RoundedRectangleBorder(
                                                                    borderRadius:
                                                                        BorderRadius.circular(
                                                                          12,
                                                                        ),
                                                                  ),
                                                                  elevation: 2,
                                                                ),
                                                                child: const Text(
                                                                  'Verify Now',
                                                                ),
                                                              ),
                                                            ),
                                                          ],
                                                        ),
                                                      ],
                                                    ),
                                                  ),
                                                ),
                                                barrierDismissible: false,
                                              );
                                            } else {
                                              // User is verified, proceed to post shift
                                              final result = await Get.to(
                                                () => const PostShiftScreen(),
                                              );
                                              if (result == true) {
                                                await _loadAllData();
                                                ScaffoldMessenger.of(
                                                  context,
                                                ).showSnackBar(
                                                  SnackBar(
                                                    content: Text(
                                                      'Shift posted successfully!',
                                                    ),
                                                    backgroundColor:
                                                        Colors.green,
                                                    duration: Duration(
                                                      seconds: 2,
                                                    ),
                                                  ),
                                                );
                                              }
                                            }
                                          },
                                        ),

                                        _buildQuickAction(
                                          icon: Icons.work,
                                          label: 'Shifts',
                                          color: Color(0xFF2563EB),
                                          bgColor: Color(0x1A2563EB),
                                          onTap: () {
                                            // Navigate to shifts page
                                            Get.to(
                                              () => Manage_Jobs_HomePage(),
                                            );
                                          },
                                        ),
                                        _buildQuickAction(
                                          icon: Icons.people,
                                          label: 'Workers',
                                          color: Color(0xFF10B981),
                                          bgColor: Color(0x1A10B981),
                                          onTap: () {
                                            // Navigate to workers page
                                            Get.to(
                                              () =>
                                                  const WorkerApplicationsScreen(),
                                            );
                                          },
                                        ),
                                        _buildQuickAction(
                                          icon: Icons.account_balance_wallet,
                                          label: 'Wallets',
                                          color: Color(0xFFEF4444),
                                          bgColor: Color(0x1AEF4444),
                                          onTap: () {
                                            // Use direct navigation instead of named route
                                            Get.to(() => const WalletScreen());
                                            // Or update your routes registration to properly handle named routes
                                          },
                                        ),
                                      ],
                                    ),
                                  ),

                                  // Today's Shifts Section
                                  Column(
                                    mainAxisSize: MainAxisSize.max,
                                    crossAxisAlignment:
                                        CrossAxisAlignment.start,
                                    children: [
                                      // Section Header
                                      Padding(
                                        padding: const EdgeInsets.only(
                                          bottom: 16,
                                        ),
                                        child: Row(
                                          mainAxisAlignment:
                                              MainAxisAlignment.spaceBetween,
                                          children: [
                                            Text(
                                              'Today\'s Shifts',
                                              style: TextStyle(
                                                fontFamily: 'Inter Tight',
                                                fontSize: 18,
                                                fontWeight: FontWeight.w600,
                                              ),
                                            ),
                                            if (_applicants.length > 3)
                                              GestureDetector(
                                                onTap: () {
                                                  setState(() {
                                                    _showAllShifts =
                                                        !_showAllShifts;
                                                  });
                                                },
                                                child: Text(
                                                  _showAllShifts
                                                      ? 'Show Less'
                                                      : 'View All',
                                                  style: TextStyle(
                                                    fontFamily: 'Inter',
                                                    color: Color(0xFF2563EB),
                                                    fontSize: 14,
                                                    fontWeight: FontWeight.w500,
                                                  ),
                                                ),
                                              ),
                                          ],
                                        ),
                                      ),
                                      ListView.builder(
                                        shrinkWrap: true,
                                        physics: NeverScrollableScrollPhysics(),
                                        itemCount:
                                            _showAllShifts
                                                ? _applicants.length
                                                : (_applicants.length > 3
                                                    ? 3
                                                    : _applicants.length),
                                        padding: EdgeInsets.zero,
                                        itemBuilder: (context, index) {
                                          final applicant = _applicants[index];
                                          return Padding(
                                            padding: const EdgeInsets.only(
                                              bottom: 16,
                                            ),
                                            child: _buildApplicantCard(
                                              applicant,
                                            ),
                                          );
                                        },
                                      ),
                                    ],
                                  ),
                                ],
                              ),
                            ),
                  ),
                ),

                // Bottom Navigation
              ),
              ShiftHourBottomNavigation(),
            ],
          ),
        ),
      ),
    );
  }

  // Helper method to build a drawer
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
                  colors: [Colors.blue, Colors.lightBlueAccent],
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
                        ),
                        child: Center(
                          child: Text(
                            _userName.isNotEmpty ? _userName[0] : 'E',
                            style: TextStyle(
                              color: Colors.blue.shade700,
                              fontSize: 24,
                              fontWeight: FontWeight.bold,
                            ),
                          ),
                        ),
                      ),
                      const SizedBox(width: 12),
                      Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          Text(
                            _userName.isNotEmpty ? _userName : 'Employer',
                            style: const TextStyle(
                              color: Colors.white,
                              fontSize: 18,
                              fontWeight: FontWeight.bold,
                            ),
                            overflow: TextOverflow.ellipsis,
                          ),
                          const Text(
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
                    children: [
                      const Icon(Icons.business, color: Colors.white, size: 16),
                      const SizedBox(width: 8),
                      Expanded(
                        child: Text(
                          _usercompany.isNotEmpty
                              ? _usercompany
                              : 'Your Company',
                          style: const TextStyle(
                            color: Colors.white,
                            fontSize: 14,
                          ),
                          overflow: TextOverflow.ellipsis,
                        ),
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
                Get.back(); // Close the drawer
              },
            ),
            _buildDrawerItem(
              context,
              icon: Icons.work_outline_rounded,
              title: 'Shifts',
              onTap: () {
                Get.back();
                Get.to(() => Manage_Jobs_HomePage());
              },
            ),
            _buildDrawerItem(
              context,
              icon: Icons.people_outline_rounded,
              title: 'Workers',
              onTap: () {
                Get.back();
                Get.toNamed('/workers');
              },
            ),
            _buildDrawerItem(
              context,
              icon: Icons.payments_outlined,
              title: 'Payments',
              onTap: () {
                Navigator.pop(context);
                // Navigate to payments page
              },
            ),
            _buildDrawerItem(
              context,
              icon: Icons.person_outline,
              title: 'Company Profile',
              onTap: () {
                Get.back();
                Get.toNamed('/employeer');
              },
            ),
            _buildDrawerItem(
              context,
              icon: Icons.settings_outlined,
              title: 'Settings',
              onTap: () {
                Navigator.pop(context);
                // Navigate to settings page
              },
            ),
            const Divider(thickness: 1),
            _buildDrawerItem(
              context,
              icon: Icons.help_outline,
              title: 'Help & Support',
              onTap: () {
                Navigator.pop(context);
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
                          onPressed: () async {
                            Navigator.of(context).pop(); // Close dialog

                            try {
                              showDialog(
                                context: context,
                                barrierDismissible: false,
                                builder: (BuildContext dialogContext) {
                                  return const Center(
                                    child: CircularProgressIndicator(),
                                  );
                                },
                              );

                              // Clear Supabase session
                              await Supabase.instance.client.auth.signOut();

                              // Check if context is still valid before using Navigator
                              if (Navigator.canPop(context)) {
                                Navigator.of(
                                  context,
                                ).pop(); // Close loading indicator

                                // Navigate to login screen
                                Navigator.of(context).pushAndRemoveUntil(
                                  MaterialPageRoute(
                                    builder:
                                        (context) => const EmployerLoginPage(),
                                  ),
                                  (Route<dynamic> route) => false,
                                );
                              }
                            } catch (e) {
                              // Handle any errors
                              print('Logout error: $e');

                              // Check if context is still valid before showing SnackBar
                              if (context.mounted) {
                                Navigator.of(
                                  context,
                                ).pop(); // Close loading indicator
                                ScaffoldMessenger.of(context).showSnackBar(
                                  SnackBar(
                                    content: Text('Error during logout: $e'),
                                  ),
                                );
                              }
                            }
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

  // Helper method to build a stat card
  Widget _buildStatCard({
    required IconData icon,
    required Color iconColor,
    required Color iconBgColor,
    required String value,
    required String label,
  }) {
    return Padding(
      padding: EdgeInsets.all(8),
      child: Container(
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
            mainAxisSize: MainAxisSize.max,
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Container(
                width: 36,
                height: 36,
                decoration: BoxDecoration(
                  color: iconBgColor,
                  borderRadius: BorderRadius.circular(8),
                ),
                child: Icon(icon, color: iconColor, size: 24),
              ),
              Padding(
                padding: EdgeInsetsDirectional.fromSTEB(0, 12, 0, 4),
                child: Text(
                  value,
                  style: TextStyle(
                    fontFamily: 'Inter Tight',
                    fontSize: 24,
                    fontWeight: FontWeight.bold,
                  ),
                ),
              ),
              Text(
                label,
                style: TextStyle(
                  fontFamily: 'Inter',
                  color: Color(0x991F2937),
                  fontSize: 12,
                ),
              ),
            ],
          ),
        ),
      ),
    );
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

  String _formatJobDatee(String? dateString) {
    if (dateString == null) return '';

    final date = DateTime.tryParse(dateString);
    if (date == null) return '';

    return '${_getMonthName(date.month)} ${date.day} ${date.year}';
  }

  Widget _buildQuickAction({
    required IconData icon,
    required String label,
    required Color color,
    required Color bgColor,
    required VoidCallback onTap,
  }) {
    return InkWell(
      onTap: () async {
        // Check if the user has completed business verification
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

        // Query the business_verifications table
        final verificationResponse =
            await Supabase.instance.client
                .from('business_verifications')
                .select()
                .eq('email', user.email!)
                .maybeSingle();

        final String rawStatus = verificationResponse?['status'] ?? 'Inactive';
        // Normalize status to handle case inconsistencies in database
        final String status = rawStatus.trim().toLowerCase();

        if (status != 'active') {

          // Determine title and message based on status
          String dialogTitle;
          String dialogMessage;
          IconData dialogIcon;

          switch (status) {
            case 'inprogress':
              dialogTitle = 'Business Verification In Progress';
              dialogMessage =
                  'Your business verification is currently being reviewed. This process usually takes 1-2 hours. You will be notified once it\'s completed.';
              dialogIcon = Icons.pending_outlined;
              break;
            case 'inactive':
              dialogTitle = 'Business Verification Required';
              dialogMessage =
                  'You need to complete business verification before posting a shift. This helps us maintain the quality and authenticity of job listings.';
              dialogIcon = Icons.verified_outlined;
              break;
            default:
              dialogTitle = 'Business Verification Needed';
              dialogMessage =
                  'Your business verification status is unclear. Please complete the verification process to access all features.';
              dialogIcon = Icons.info_outlined;
          }

          Get.dialog(
            Dialog(
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
                        color: bgColor.withOpacity(0.1),
                        shape: BoxShape.circle,
                      ),
                      child: Center(
                        child: Icon(dialogIcon, color: color, size: 40),
                      ),
                    ),
                    const SizedBox(height: 16),
                    Text(
                      dialogTitle,
                      style: TextStyle(
                        fontSize: 18,
                        fontWeight: FontWeight.bold,
                        color: color,
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
                            onPressed: () => Get.back(),
                            style: OutlinedButton.styleFrom(
                              side: BorderSide(color: color.withOpacity(0.2)),
                              foregroundColor: color,
                              padding: const EdgeInsets.symmetric(vertical: 12),
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
                              Get.back(); // Close dialog
                              if (status == 'inprogress') {
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
                                Get.to(
                                  () => const StandaloneVerificationForm(),
                                );
                              }
                            },
                            style: ElevatedButton.styleFrom(
                              backgroundColor: color,
                              foregroundColor: Colors.white,
                              padding: const EdgeInsets.symmetric(vertical: 12),
                              shape: RoundedRectangleBorder(
                                borderRadius: BorderRadius.circular(12),
                              ),
                              elevation: 2,
                            ),
                            child: Text(
                              status == 'inprogress'
                                  ? 'Okay'
                                  : 'Verify Now',
                            ),
                          ),
                        ),
                      ],
                    ),
                  ],
                ),
              ),
            ),
            barrierDismissible: false,
          );
        } else {
          // User is verified, proceed with the action
          print('DEBUG: Status is Active, executing original onTap');
          onTap();
        }
      },
      child: Column(
        mainAxisSize: MainAxisSize.max,
        children: [
          Container(
            width: 56,
            height: 56,
            decoration: BoxDecoration(
              color: bgColor,
              borderRadius: BorderRadius.circular(16),
            ),
            child: Icon(icon, color: color, size: 24),
          ),
          Padding(
            padding: EdgeInsetsDirectional.fromSTEB(0, 8, 0, 0),
            child: Text(
              label,
              style: TextStyle(
                fontFamily: 'Inter',
                fontSize: 12,
                fontWeight: FontWeight.w500,
              ),
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildApplicantCard(Applicant applicant) {
    // Now use the properties of the Applicant class directly
    String duration = _calculateShiftDuration(
      applicant.startTime,
      applicant.endTime,
    );

    // Get status colors
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
                color: getStatusBackgroundColor(applicant.appstatus),
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
                            'Title: ${applicant.jobTitle}',
                            style: TextStyle(
                              fontSize: 16,
                              fontWeight: FontWeight.w600,
                            ),
                          ),
                          Text(
                            'Category: ${applicant.category}',
                            style: TextStyle(
                              fontSize: 14,
                              // color: Colors.grey.shade600,
                              fontWeight: FontWeight.w500,
                              //fontWeight: FontWeight.w600,
                            ),
                          ),
                          Text(
                            'Shift ID: ${applicant.shiftId}',
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
                        color: getStatusColor(applicant.appstatus),
                        borderRadius: BorderRadius.circular(16),
                      ),
                      child: Text(
                        applicant.appstatus,
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
                  // Company row if available in Applicant class
                  Row(
                    mainAxisAlignment: MainAxisAlignment.spaceBetween,
                    children: [
                      Row(
                        children: [
                          Icon(Icons.business, size: 18, color: Colors.black),
                          SizedBox(width: 8),
                          Text(
                            'Company: ${applicant.role}', // Using role as company
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
                        '${_formatJobDatee(applicant.date)}',
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
                        '${_formatJobTime(applicant.startTime)} - ${_formatJobTime(applicant.endTime)}',
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
                          'Location: ${applicant.location}',
                          style: TextStyle(
                            fontSize: 15,
                            color: Colors.grey.shade800,
                          ),
                          maxLines: 4,
                          overflow: TextOverflow.ellipsis,
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
                        'Pincode: ${applicant.pincode}',
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

                  // Worker Assigned Section
                  if (applicant.isAssigned) ...[
                    Text(
                      'Worker Assigned',
                      style: TextStyle(
                        fontSize: 14,
                        fontWeight: FontWeight.w600,
                      ),
                    ),
                    SizedBox(height: 12),
                    Row(
                      children: [
                        FutureBuilder<String?>(
                          future: _getWorkerPhotoUrl(applicant.email),
                          builder: (context, snapshot) {
                            Widget avatar;

                            if (snapshot.connectionState ==
                                ConnectionState.waiting) {
                              avatar = CircleAvatar(
                                radius: 25,
                                backgroundColor: Colors.grey.shade300,
                                child: CircularProgressIndicator(
                                  strokeWidth: 2,
                                ),
                              );
                            } else if (snapshot.hasData &&
                                snapshot.data != null &&
                                snapshot.data!.isNotEmpty) {
                              avatar = CircleAvatar(
                                radius: 25,
                                backgroundImage: NetworkImage(snapshot.data!),
                              );
                            } else {
                              avatar = CircleAvatar(
                                radius: 25,
                                backgroundColor: applicant.avatarColor,
                                child: Text(
                                  applicant.avatarText,
                                  style: TextStyle(
                                    color: Colors.white,
                                    fontSize: 20,
                                  ),
                                ),
                              );
                            }

                            return GestureDetector(
                              onTap: () {
                                if (snapshot.hasData &&
                                    snapshot.data != null &&
                                    snapshot.data!.isNotEmpty) {
                                  showDialog(
                                    context: context,
                                    builder:
                                        (_) => Dialog(
                                          shape: RoundedRectangleBorder(
                                            borderRadius: BorderRadius.circular(
                                              12,
                                            ),
                                          ),
                                          child: Container(
                                            padding: EdgeInsets.all(10),
                                            decoration: BoxDecoration(
                                              borderRadius:
                                                  BorderRadius.circular(12),
                                              color: Colors.white,
                                            ),
                                            child: Image.network(
                                              snapshot.data!,
                                              fit: BoxFit.contain,
                                              errorBuilder:
                                                  (
                                                    context,
                                                    error,
                                                    stackTrace,
                                                  ) => Text(
                                                    "Failed to load image",
                                                  ),
                                            ),
                                          ),
                                        ),
                                  );
                                }
                              },
                              child: avatar,
                            );
                          },
                        ),
                        SizedBox(width: 12),
                        Expanded(
                          child: Column(
                            crossAxisAlignment: CrossAxisAlignment.start,
                            children: [
                              Text(
                                applicant.name,
                                style: TextStyle(
                                  fontSize: 16,
                                  fontWeight: FontWeight.w600,
                                ),
                              ),
                              SizedBox(height: 4),
                              Row(
                                children: [
                                  Icon(
                                    Icons.star,
                                    size: 16,
                                    color: Colors.amber,
                                  ),
                                  SizedBox(width: 4),
                                  Text(
                                    applicant.rating.toString(),
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
                                  Text(applicant.phoneNumber),
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
                                  Text(applicant.email),
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
                        Expanded(
                          child: OutlinedButton.icon(
                            icon: Icon(Icons.message, size: 18),
                            label: Text('Message'),
                            onPressed: () async {
                              final Uri smsUri = Uri(
                                scheme: 'sms',
                                path: applicant.phoneNumber,
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
                        SizedBox(width: 12),
                        Expanded(
                          child: ElevatedButton.icon(
                            icon: Icon(Icons.call, size: 18),
                            label: Text('Call'),
                            onPressed: () async {
                              final Uri telUri = Uri(
                                scheme: 'tel',
                                path: applicant.phoneNumber,
                              );
                              if (await canLaunchUrl(telUri)) {
                                await launchUrl(telUri);
                              } else {
                                ScaffoldMessenger.of(context).showSnackBar(
                                  SnackBar(
                                    content: Text('Could not launch dialer'),
                                  ),
                                );
                              }
                            },
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
                    // Clear message if no worker assigned
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
                            'No worker assigned yet.',
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

  double _calculateHourlyRate(String duration, double originalPayRate) {
    // Parse duration to extract total hours
    final hours = _extractHoursFromDuration(duration);

    // If no hours, return the original pay rate
    return hours > 0 ? originalPayRate / hours : originalPayRate;
  }

  double _extractHoursFromDuration(String duration) {
    double totalHours = 0.0;

    // Extract hours
    final hourMatch = RegExp(r'(\d+)\s*hr').firstMatch(duration);
    if (hourMatch != null) {
      totalHours += double.parse(hourMatch.group(1)!);
    }

    // Extract minutes
    final minuteMatch = RegExp(r'(\d+)\s*min').firstMatch(duration);
    if (minuteMatch != null) {
      totalHours += double.parse(minuteMatch.group(1)!) / 60;
    }

    return totalHours;
  }
}

class Applicant {
  final String name;
  final String role;
  final double rating;
  final String jobTitle;
  final String category;
  final String shiftId;
  final String appstatus;
  final String startTime;
  final String endTime;
  final String location;
  final String phoneNumber;
  final String email;
  final String avatarText;
  final Color avatarColor;
  final bool isAssigned;
  final String pincode;
  final String date; // ← Add this line

  Applicant({
    required this.name,
    required this.role,
    required this.rating,
    required this.jobTitle,
    required this.category,
    required this.shiftId,
    required this.appstatus,
    required this.startTime,
    required this.endTime,
    required this.location,
    required this.phoneNumber,
    required this.email,
    required this.avatarText,
    required this.avatarColor,
    required this.isAssigned,
    required this.pincode,
    required this.date, // ← Add this in constructor
  });

  factory Applicant.fromJson(Map<String, dynamic> json) {
    return Applicant(
      name: json['full_name'] ?? '',
      role: json['company'] ?? '',
      rating: json['rating']?.toDouble() ?? 0.0,
      jobTitle: json['job_title'] ?? '',
      category: json['category'] ?? 'Others',
      shiftId: json['shift_id']?.toString() ?? '',
      appstatus: json['application_status'] ?? '',
      startTime: json['start_time'] ?? '',
      endTime: json['end_time'] ?? '',
      location: json['location'] ?? '',
      phoneNumber: json['phone_number'] ?? '',
      email: json['email'] ?? '',
      avatarText:
          (json['full_name'] as String? ?? '').isNotEmpty
              ? (json['full_name'] as String).substring(0, 1).toUpperCase()
              : '',
      avatarColor: Color(int.parse(json['avatar_color'] ?? '0xFF60A5FA')),
      isAssigned: json['has_application'] ?? false,
      pincode: json['job_pincode'] ?? '',
      date: json['date'] ?? '',
    );
  }
}
