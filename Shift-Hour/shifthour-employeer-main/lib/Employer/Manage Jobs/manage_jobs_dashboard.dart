import 'dart:async';
import 'dart:math';
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:get/get.dart';
import 'package:shifthour_employeer/Employer/Business%20Kyc/kyc.dart';
import 'package:shifthour_employeer/Employer/employer_dashboard.dart';
import 'package:shifthour_employeer/const/Bottom_Navigation.dart';
import 'package:shifthour_employeer/const/Standard_Appbar.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:shifthour_employeer/Employer/post%20jobs/post_job.dart';
import 'package:intl/intl.dart';
import 'package:url_launcher/url_launcher.dart';
import 'package:shifthour_employeer/utils/supabase_extensions.dart';

class Manage_Jobs_HomePage extends StatefulWidget {
  const Manage_Jobs_HomePage({Key? key}) : super(key: key);

  @override
  _ManageHomePageState createState() => _ManageHomePageState();
}

class _ManageHomePageState extends State<Manage_Jobs_HomePage> {
  String searchQuery = "";
  String sortBy = "Recent";
  String activeTab = "all";
  bool isLoading = true;
  List<Map<String, dynamic>> jobs = [];
  final supabase = Supabase.instance.client;
  String mainTab = "all"; // To track the main tab (all, assigned, unassigned)
  String assignedSubTab = "all";
  final List<String> sortOptions = ["Recent", "Date", "Pay Rate"];
  final ScrollController _scrollController = ScrollController();

  @override
  void initState() {
    super.initState();
    fetchJobs();
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
    });
  } // Add this at the beginning of your fetchJobs method to analyze the structure

  // Replace excessive print statements with conditional logging
  void debugLog(String message) {
    if (kDebugMode) {
      print(message);
    }
  }

  Future<void> refreshJobs() async {
    try {
      // Clear the jobs list but keep the search query and filter settings
      setState(() {
        jobs.clear();
        isLoading = true;
      });

      // Fetch new data
      await fetchJobs();

      // Optional: Show a success message
    } catch (e) {
      debugLog('Error refreshing Shifts: $e');
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('Failed to refresh Shifts'),
          backgroundColor: Colors.red,
          duration: Duration(seconds: 2),
        ),
      );
    } finally {
      setState(() {
        isLoading = false;
      });
    }
  }

  Future<void> fetchJobs() async {
    setState(() {
      isLoading = true;
    });

    try {
      final email = supabase.auth.currentUser?.email;
      if (email == null) {
        setState(() {
          isLoading = false;
          jobs = [];
        });
        return;
      }

      // Comprehensive query to fetch jobs with potential worker details
      final response = await supabase
          .from('worker_job_listings')
          .select('''
        *,
        worker_job_applications (
          full_name,
          phone_number,
          email,
          application_status,
          date,
          worker_rating
        )
      ''')
          .eq('contact_email', email)
          .order('created_at', ascending: false)
          .limit(50)
          .withTimeout(timeout: TimeoutDurations.query);

      // Process the jobs with joined data
      final processedJobs =
          response.map<Map<String, dynamic>>((job) {
            // Extract worker details
            String workerName = 'No worker assigned';
            String phoneNumber = '';
            String email = '';
            String applicationStatus = '';
            bool hasWorkerAssigned = false;
            int workerRating = 0;

            // Check if there are associated applications
            final applications = job['worker_job_applications'];
            if (applications != null && applications.isNotEmpty) {
              final firstApplication = applications[0];
              workerName = firstApplication['full_name'] ?? workerName;
              phoneNumber = firstApplication['phone_number'] ?? '';
              email = firstApplication['email'] ?? '';
              applicationStatus = firstApplication['application_status'] ?? '';
              hasWorkerAssigned = true;
              workerRating = firstApplication['worker_rating'] ?? 0.0;

              // Debug info to help diagnose issues
              print('Job ${job['job_title']} has worker: $workerName');
              print('Application status: $applicationStatus');
            } else {
              print('Job ${job['job_title']} has no assigned workers');
            }

            // Existing job processing logic
            DateTime? jobDate;
            try {
              print('DEBUG: Raw date from job: ${job['date']}');

              // Try parsing in multiple formats
              if (job['date'] != null) {
                // First, try standard DateTime.parse
                jobDate = DateTime.tryParse(job['date']);

                // If that fails, try manual parsing
                if (jobDate == null) {
                  // Check if it's a string in 'YYYY-MM-DD' format
                  final parts = job['date'].toString().split('-');
                  if (parts.length == 3) {
                    jobDate = DateTime(
                      int.parse(parts[0]),
                      int.parse(parts[1]),
                      int.parse(parts[2]),
                    );
                  }
                }
              }

              print('DEBUG: Parsed jobDate: $jobDate');
            } catch (e) {
              print('Error parsing date: ${job['date']}');
              print('Detailed error: $e');
              jobDate = null;
            }

            // Determine status and color
            Color statusColor;
            String status = job['status'] ?? "Active";
            bool isUrgent = false;

            // Set the appropriate status color based on the status
            switch (status.toLowerCase()) {
              case 'active':
                statusColor = Colors.green;
                break;
              case 'completed':
                statusColor = Colors.blue;
                break;
              case 'cancelled':
                statusColor = Colors.red;
                break;
              case 'upcoming':
                statusColor = Colors.orange;
                break;
              case 'unassigned':
                statusColor = Colors.grey;
                break;
              default:
                statusColor = Colors.blueGrey;
            }

            // Check if job is marked as urgent
            if (job['urgent'] == true) {
              isUrgent = true;
            }

            return {
              ...job, // Keep all original fields
              'statusColor': statusColor,
              'status': status,
              'urgent': isUrgent,
              'time':
                  job['start_time'] != null && job['end_time'] != null
                      ? '${job['start_time']} - ${job['end_time']}'
                      : 'No time specified',
              'date': job['date'],
              'workers': '1/1', // Assuming the application is for one worker
              'pay': '₹${job['pay_rate'] ?? 0}/hr',
              'job_title': job['job_title'] ?? 'Untitled Job',
              'company': job['company'] ?? 'No Company',
              'category': job['category'] ?? 'Others',
              'location': job['location'] ?? 'No Location',
              'description': job['description'] ?? 'No description',
              'number_of_positions':
                  job['number_of_positions'] ?? job['position_number'] ?? 1,
              'worker_name': workerName,
              'phone_number': phoneNumber,
              'email': email,
              'application_status': applicationStatus,
              'has_worker_assigned':
                  hasWorkerAssigned, // Add this flag for easy checking
              'position_number': job['position_number'] ?? 3,
              'shift_id': job['shift_id'] ?? job['id'] ?? "",
              'worker_rating': workerRating,
            };
          }).toList();

      setState(() {
        jobs = processedJobs;
        isLoading = false;
      });

      // Debug summary
      print('Fetched ${jobs.length} jobs');
      print(
        'Jobs with workers: ${jobs.where((job) => job['has_worker_assigned'] == true).length}',
      );
    } on PostgrestException catch (e) {
      print('Supabase error fetching jobs: ${e.message}');
      setState(() {
        jobs = [];
        isLoading = false;
      });
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(
            content: Text('Failed to load shifts. Please try again.'),
            backgroundColor: Colors.red,
          ),
        );
      }
    } on TimeoutException catch (e) {
      print('Timeout error fetching jobs: $e');
      setState(() {
        jobs = [];
        isLoading = false;
      });
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(
            content: Text('Request timed out. Please check your connection.'),
            backgroundColor: Colors.orange,
          ),
        );
      }
    } catch (e) {
      print('Error fetching jobs: $e');
      setState(() {
        jobs = [];
        isLoading = false;
      });
    }
  }

  List<Map<String, dynamic>> get filteredJobs {
    final today = DateTime.now();
    final formattedToday = DateFormat('yyyy-MM-dd').format(today);

    return jobs.where((job) {
      final query = searchQuery.toLowerCase();

      // 🔍 First apply the search filter
      if (searchQuery.isNotEmpty) {
        final title = job["job_title"]?.toString().toLowerCase() ?? "";
        final company = job["company"]?.toString().toLowerCase() ?? "";
        final category = job["category"]?.toString().toLowerCase() ?? "";

        final location = job["location"]?.toString().toLowerCase() ?? "";
        final shiftId =
            (job['shift_id'] ?? job['id'] ?? "").toString().toLowerCase();

        final matches =
            title.contains(query) ||
            company.contains(query) ||
            category.contains(query) ||
            location.contains(query) ||
            shiftId.contains(query);

        if (!matches) return false;
      }

      // ✅ Now apply mainTab + assignedSubTab filtering
      if (mainTab == "all") return true;

      if (mainTab == "assigned") {
        if (job["has_worker_assigned"] != true) return false;

        if (assignedSubTab == "all") return true;
        if (assignedSubTab == "in-progress") {
          return job["application_status"]?.toString() == "In Progress";
        }
        if (assignedSubTab == "completed") {
          return job["application_status"]?.toString() == "Completed";
        }
        if (assignedSubTab == "upcoming") {
          return job["application_status"]?.toString() == "Upcoming";
        }
      }

      if (mainTab == "unassigned") {
        return job["has_worker_assigned"] != true;
      }

      return true;
    }).toList();
  }

  TextField _buildSearchTextField(BuildContext context) {
    final isDark = Theme.of(context).brightness == Brightness.dark;
    return TextField(
      onChanged: (value) {
        setState(() {
          searchQuery = value;
        });
      },
      decoration: InputDecoration(
        hintText:
            'Search Shifts by title, location, company,category, or Shift ID...',
        prefixIcon: const Icon(Icons.search),
        fillColor:
            isDark
                ? const Color(0xFF1E293B)
                : const Color(0xFFF8FAFC), // slate-50
        filled: true,
        border: OutlineInputBorder(
          borderRadius: BorderRadius.circular(8),
          borderSide: BorderSide(
            color:
                isDark
                    ? const Color(0xFF334155)
                    : const Color(0xFFE2E8F0), // slate-200
          ),
        ),
        contentPadding: const EdgeInsets.symmetric(vertical: 10),
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

  @override
  Widget build(BuildContext context) {
    final isDark = Theme.of(context).brightness == Brightness.dark;
    // Get screen width for responsive design
    final screenWidth = MediaQuery.of(context).size.width;
    final isTablet = screenWidth > 600;
    final isDesktop = screenWidth > 1024;

    return Scaffold(
      appBar: StandardAppBar(title: 'Shift Management', centerTitle: false),
      bottomNavigationBar: const ShiftHourBottomNavigation(),
      body: OrientationBuilder(
        builder: (context, orientation) {
          return GestureDetector(
            behavior: HitTestBehavior.opaque,
            onTap: () {
              // This will unfocus any text field and dismiss the keyboard
              FocusManager.instance.primaryFocus?.unfocus();
            },
            child: Column(
              children: [
                // Header with gradient

                // Main Content
                Expanded(
                  child: Padding(
                    padding: const EdgeInsets.all(16.0),
                    child: Column(
                      children: [
                        // Search and Filter Card
                        Column(
                          children: [
                            LayoutBuilder(
                              builder: (context, constraints) {
                                return constraints.maxWidth > 700
                                    ? _buildWideSearchFilter()
                                    : _buildNarrowSearchFilter();
                              },
                            ),
                          ],
                        ),

                        const SizedBox(height: 16),

                        // Tabs
                        Container(
                          decoration: BoxDecoration(
                            color:
                                isDark
                                    ? const Color(0xFF1E293B)
                                    : const Color(0xFFF1F5F9), // slate-100
                            borderRadius: BorderRadius.circular(12),
                          ),
                          padding: const EdgeInsets.all(4),
                          child:
                              orientation == Orientation.portrait
                                  ? _buildTabsPortrait()
                                  : _buildTabsLandscape(),
                        ),

                        const SizedBox(height: 16),

                        // Job List or Loading Indicator
                        Expanded(
                          child:
                              isLoading
                                  ? const Center(
                                    child: CircularProgressIndicator(),
                                  )
                                  : filteredJobs.isEmpty
                                  ? _buildEmptyState(isDark)
                                  : LayoutBuilder(
                                    builder: (context, constraints) {
                                      // Decide on grid or list view based on available width
                                      if (constraints.maxWidth > 900) {
                                        // Grid view for larger screens
                                        return RefreshIndicator(
                                          onRefresh: refreshJobs,
                                          child: GridView.builder(
                                            gridDelegate:
                                                SliverGridDelegateWithFixedCrossAxisCount(
                                                  crossAxisCount:
                                                      constraints.maxWidth >
                                                              1200
                                                          ? 3
                                                          : 2,
                                                  childAspectRatio: 1.2,
                                                  crossAxisSpacing: 16,
                                                  mainAxisSpacing: 16,
                                                ),
                                            itemCount: filteredJobs.length,
                                            itemBuilder: (context, index) {
                                              return JobCard(
                                                job: filteredJobs[index],
                                                isDark: isDark,
                                              );
                                            },
                                          ),
                                        );
                                      } else {
                                        // List view for smaller screens
                                        return RefreshIndicator(
                                          onRefresh: refreshJobs,
                                          child: ListView.builder(
                                            controller: _scrollController,
                                            itemCount: filteredJobs.length,
                                            itemBuilder: (context, index) {
                                              return Padding(
                                                padding: const EdgeInsets.only(
                                                  bottom: 16,
                                                ),
                                                child: JobCard(
                                                  job: filteredJobs[index],
                                                  isDark: isDark,
                                                ),
                                              );
                                            },
                                          ),
                                        );
                                      }
                                    },
                                  ),
                        ),
                      ],
                    ),
                  ),
                ),
              ],
            ),
          );
        },
      ),

      // FAB
      floatingActionButton: Container(
        decoration: BoxDecoration(
          color: Colors.blue.shade100, // Light background (you can tweak this)
          borderRadius: BorderRadius.circular(20), // Smooth corner radius
          boxShadow: [
            BoxShadow(
              color: Colors.black.withOpacity(0.05),
              blurRadius: 8,
              offset: Offset(0, 3),
            ),
          ],
        ),
        padding: const EdgeInsets.all(12), // Spacing around the button
        child: FloatingActionButton(
          onPressed: () async {
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
                    .eq('is_verified', true)
                    .withTimeout(timeout: TimeoutDurations.query)
                    .maybeSingle();

            if (verificationResponse == null) {
              // No verified business verification found
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
                            color: Colors.blue.shade50,
                            shape: BoxShape.circle,
                          ),
                          child: Center(
                            child: Icon(
                              Icons.verified_outlined,
                              color: Colors.blue.shade600,
                              size: 40,
                            ),
                          ),
                        ),
                        const SizedBox(height: 16),
                        Text(
                          'Business Verification Required',
                          style: TextStyle(
                            fontSize: 18,
                            fontWeight: FontWeight.bold,
                            color: Colors.blue.shade700,
                          ),
                          textAlign: TextAlign.center,
                        ),
                        const SizedBox(height: 12),
                        Text(
                          'You need to complete business verification before posting a shift. This helps us maintain the quality and authenticity of shift listings.',
                          style: TextStyle(
                            color: Colors.grey.shade700,
                            fontSize: 14,
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
                                  side: BorderSide(color: Colors.blue.shade200),
                                  foregroundColor: Colors.blue.shade600,
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
                                  Get.back(); // Close dialog
                                  Get.to(
                                    () => const StandaloneVerificationForm(),
                                  ); // Navigate to verification form
                                },
                                style: ElevatedButton.styleFrom(
                                  backgroundColor: Colors.blue.shade600,
                                  foregroundColor: Colors.white,
                                  padding: const EdgeInsets.symmetric(
                                    vertical: 12,
                                  ),
                                  shape: RoundedRectangleBorder(
                                    borderRadius: BorderRadius.circular(12),
                                  ),
                                  elevation: 2,
                                ),
                                child: const Text('Verify Now'),
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
              Get.to(() => const PostShiftScreen())?.then((_) => fetchJobs());
            }
          },
          backgroundColor: Colors.blue,
          elevation: 0,
          child: const Icon(Icons.add, color: Colors.white),
          shape: const CircleBorder(),
        ),
      ),
    );
  }

  Widget _buildWideSearchFilter() {
    final isDark = Theme.of(context).brightness == Brightness.dark;
    return Row(
      children: [
        Expanded(
          flex: 3,
          child: TextField(
            onChanged: (value) {
              setState(() {
                searchQuery = value;
              });
            },
            decoration: InputDecoration(
              hintText: 'Search Shifts by title, location, or category...',
              prefixIcon: const Icon(Icons.search),
              fillColor:
                  isDark
                      ? const Color(0xFF1E293B)
                      : const Color(0xFFF8FAFC), // slate-50
              filled: true,
              border: OutlineInputBorder(
                borderRadius: BorderRadius.circular(8),
                borderSide: BorderSide(
                  color:
                      isDark
                          ? const Color(0xFF334155)
                          : const Color(0xFFE2E8F0), // slate-200
                ),
              ),
              contentPadding: const EdgeInsets.symmetric(vertical: 10),
            ),
          ),
        ),
        const SizedBox(width: 8),

        // Sort Dropdown
        Flexible(
          child: PopupMenuButton<String>(
            onSelected: (value) {
              setState(() {
                sortBy = value;
              });
            },
            child: Container(
              padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
              decoration: BoxDecoration(
                border: Border.all(
                  color:
                      isDark
                          ? const Color(0xFF334155)
                          : const Color(0xFFE2E8F0), // slate-200
                ),
                borderRadius: BorderRadius.circular(8),
              ),
              child: Row(
                children: [
                  Expanded(
                    child: FittedBox(
                      fit: BoxFit.scaleDown,
                      alignment: Alignment.centerLeft,
                      child: Text('Sort: $sortBy'),
                    ),
                  ),
                  const SizedBox(width: 4),
                  const Icon(Icons.keyboard_arrow_down, size: 16),
                ],
              ),
            ),
            itemBuilder:
                (context) =>
                    sortOptions
                        .map(
                          (option) => PopupMenuItem<String>(
                            value: option,
                            child: Text(option),
                          ),
                        )
                        .toList(),
          ),
        ),

        const SizedBox(width: 8),

        // Filter Button
        AspectRatio(
          aspectRatio: 1,
          child: Container(
            decoration: BoxDecoration(
              border: Border.all(
                color:
                    isDark
                        ? const Color(0xFF334155)
                        : const Color(0xFFE2E8F0), // slate-200
              ),
              borderRadius: BorderRadius.circular(8),
            ),
            child: IconButton(
              icon: const Icon(Icons.filter_list),
              onPressed: () {},
            ),
          ),
        ),
      ],
    );
  }

  Widget _buildNarrowSearchFilter() {
    final isDark = Theme.of(context).brightness == Brightness.dark;

    return Column(
      children: [
        Container(
          decoration: BoxDecoration(
            borderRadius: BorderRadius.circular(30),
            color: Colors.white,
            boxShadow: [
              BoxShadow(
                color: Colors.blue.withOpacity(0.3),
                spreadRadius: 1,
                blurRadius: 10,
                offset: Offset(0, 3),
              ),
            ],
          ),
          child: TextField(
            onChanged: (value) {
              setState(() {
                searchQuery = value;
              });
            },
            controller: TextEditingController.fromValue(
              TextEditingValue(
                text: searchQuery,
                selection: TextSelection.collapsed(offset: searchQuery.length),
              ),
            ),
            decoration: InputDecoration(
              hintText: 'Search ...',
              prefixIcon: Icon(Icons.search, color: Colors.grey),
              suffixIcon:
                  searchQuery.isNotEmpty
                      ? IconButton(
                        icon: Icon(Icons.close, color: Colors.grey),
                        onPressed: () {
                          setState(() {
                            searchQuery = '';
                          });
                        },
                      )
                      : null,
              border: InputBorder.none,
              contentPadding: EdgeInsets.symmetric(vertical: 14),
            ),
          ),
        ),
        const SizedBox(height: 8),
      ],
    );
  }

  Widget _buildTabsPortrait() {
    return Column(
      children: [
        // Main tabs
        Row(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            _buildMainTab('All Shifts', 'all'),
            const SizedBox(width: 8),
            _buildMainTab('Assigned', 'assigned'),
            const SizedBox(width: 8),
            _buildMainTab('Unassigned', 'unassigned'),
          ],
        ),

        // Sub-tabs for Assigned (only show when assigned tab is selected)
        if (mainTab == "assigned")
          Padding(
            padding: const EdgeInsets.only(top: 8.0),
            child: Row(
              children: [
                _buildSubTab('All', 'all'),
                _buildSubTab('In Progress', 'in-progress'),
                _buildSubTab('Completed', 'completed'),
                _buildSubTab('Upcoming', 'upcoming'),
              ],
            ),
          ),
      ],
    );
  }

  Widget _buildTabsLandscape() {
    return Column(
      children: [
        // Main tabs
        Row(
          children: [
            _buildMainTab('All Shifts', 'all'),
            _buildMainTab('Assigned', 'assigned'),
            _buildMainTab('Unassigned', 'unassigned'),
          ],
        ),

        // Sub-tabs for Assigned (only show when assigned tab is selected)
        if (mainTab == "assigned")
          Padding(
            padding: const EdgeInsets.only(top: 8.0),
            child: Row(
              children: [
                _buildSubTab('All', 'all'),
                _buildSubTab('In Progress', 'in-progress'),
                _buildSubTab('Completed', 'completed'),
                _buildSubTab('Upcoming', 'upcoming'),
              ],
            ),
          ),
      ],
    );
  }

  Widget _buildMainTab(String title, String value) {
    final isDark = Theme.of(context).brightness == Brightness.dark;

    // Define border color based on tab type
    Color getTabBorderColor() {
      switch (value) {
        case 'all':
          return Colors.blue;
        case 'assigned':
          return Colors.green;
        case 'unassigned':
          return Colors.red;
        default:
          return Colors.grey;
      }
    }

    return Expanded(
      child: GestureDetector(
        onTap: () {
          setState(() {
            mainTab = value;
            if (value != "assigned") {
              assignedSubTab = "all";
            }
          });
        },
        child: Container(
          padding: const EdgeInsets.symmetric(vertical: 10),
          decoration: BoxDecoration(
            color: Colors.transparent,
            borderRadius: BorderRadius.circular(50),
            border: Border.all(
              color:
                  mainTab == value ? getTabBorderColor() : Colors.grey.shade300,
              width: 2,
            ),
          ),
          child: Center(
            child: Text(
              title,
              style: TextStyle(
                color: isDark ? Colors.white : Colors.black,
                fontWeight:
                    mainTab == value ? FontWeight.bold : FontWeight.normal,
              ),
            ),
          ),
        ),
      ),
    );
  }

  Widget _buildSubTab(String title, String value) {
    final isDark = Theme.of(context).brightness == Brightness.dark;

    // Define colors for different sub-tabs
    Color getSubTabColor() {
      switch (value) {
        case 'in-progress':
          return Colors.yellow.shade50;
        case 'completed':
          return Colors.green.shade50;
        case 'upcoming':
          return Colors.orange.shade50;
        case 'all':
          return Colors.blue.shade50;
        default:
          return Colors.grey.shade50;
      }
    }

    // Define text colors for different sub-tabs
    Color getSubTabTextColor() {
      switch (value) {
        case 'in-progress':
          return Colors.yellow.shade700;
        case 'completed':
          return Colors.green.shade700;
        case 'upcoming':
          return Colors.orange.shade700;
        case 'all':
          return Colors.blue.shade700;
        default:
          return Colors.grey.shade700;
      }
    }

    return Expanded(
      child: GestureDetector(
        onTap: () {
          setState(() {
            assignedSubTab = value;
          });
        },
        child: Container(
          padding: const EdgeInsets.symmetric(vertical: 8),
          decoration: BoxDecoration(
            color:
                assignedSubTab == value ? getSubTabColor() : Colors.transparent,
            borderRadius: BorderRadius.circular(8),
            border:
                assignedSubTab == value
                    ? Border.all(
                      color: getSubTabTextColor().withOpacity(0.5),
                      width: 1,
                    )
                    : null,
          ),
          child: Text(
            title,
            textAlign: TextAlign.center,
            style: TextStyle(
              fontSize: 12,
              fontWeight:
                  assignedSubTab == value ? FontWeight.bold : FontWeight.normal,
              color:
                  assignedSubTab == value
                      ? getSubTabTextColor()
                      : (isDark ? Colors.white70 : Colors.black54),
            ),
          ),
        ),
      ),
    );
  }

  Widget _buildTabItem(String title, String value) {
    return FractionallySizedBox(
      widthFactor:
          title == 'All Shifts' || title == 'In Progress' ? 0.33 : 0.32,
      child: GestureDetector(
        onTap: () {
          setState(() {
            activeTab = value;
          });
        },
        child: Container(
          margin: const EdgeInsets.symmetric(vertical: 2),
          padding: const EdgeInsets.symmetric(vertical: 12),
          decoration: BoxDecoration(
            color:
                activeTab == value
                    ? Theme.of(context).brightness == Brightness.dark
                        ? const Color(0xFF0F172A) // slate-900
                        : Colors.white
                    : Colors.transparent,
            borderRadius: BorderRadius.circular(8),
          ),
          child: Text(
            title,
            textAlign: TextAlign.center,
            style: TextStyle(
              fontWeight:
                  activeTab == value ? FontWeight.bold : FontWeight.normal,
            ),
          ),
        ),
      ),
    );
  }

  Widget _buildTab(String title, String value) {
    return Expanded(
      child: GestureDetector(
        onTap: () {
          setState(() {
            activeTab = value;
          });
        },
        child: Container(
          padding: const EdgeInsets.symmetric(vertical: 12),
          decoration: BoxDecoration(
            color:
                activeTab == value
                    ? Theme.of(context).brightness == Brightness.dark
                        ? const Color(0xFF0F172A) // slate-900
                        : Colors.white
                    : Colors.transparent,
            borderRadius: BorderRadius.circular(8),
          ),
          child: Text(
            title,
            textAlign: TextAlign.center,
            style: TextStyle(
              fontWeight:
                  activeTab == value ? FontWeight.bold : FontWeight.normal,
            ),
          ),
        ),
      ),
    );
  }

  Widget _buildEmptyState(bool isDark) {
    return Center(
      child: Column(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          Container(
            width: 64,
            height: 64,
            decoration: BoxDecoration(
              color:
                  isDark
                      ? const Color(0xFF1E293B)
                      : const Color(0xFFF1F5F9), // slate-100
              shape: BoxShape.circle,
            ),
            child: Icon(
              Icons.work_outline,
              size: 32,
              color:
                  isDark
                      ? const Color(0xFF94A3B8)
                      : const Color(0xFF94A3B8), // slate-400
            ),
          ),
          const SizedBox(height: 16),
          const Text(
            'No Shifts found',
            style: TextStyle(fontSize: 18, fontWeight: FontWeight.w500),
          ),
          const SizedBox(height: 8),
          Text(
            activeTab != 'all'
                ? 'No ${activeTab.replaceAll('-', ' ')} Shift found'
                : 'Post your  Shift by clicking the + button',
            textAlign: TextAlign.center,
            style: const TextStyle(color: Colors.grey),
          ),
          const SizedBox(height: 24),
        ],
      ),
    );
  }
}

class JobCard extends StatelessWidget {
  final Map<String, dynamic> job;
  final bool isDark;

  const JobCard({Key? key, required this.job, required this.isDark})
    : super(key: key);

  Color _getStatusColor(status) {
    switch (status.toLowerCase()) {
      case 'in progress':
        return Color(0xFF3461FD); // Blue
      case 'upcoming':
        return Color(0xFFF5A623); // Yellow/Orange
      case 'completed':
        return Color(0xFF27AE60); // Green
      default:
        return Color(0xFF8B0000); // Default blue
    }
  }

  Color getStatusBackgroundColor(status) {
    switch (status.toLowerCase()) {
      case 'in progress':
        return Color(0x1A3461FD); // Light Blue
      case 'upcoming':
        return Color(0x1AF5A623); // Light Yellow/Orange
      case 'completed':
        return Color(0x1A27AE60); // Light Green
      default:
        return Color(0x1AEF4444); // Default light blue
    }
  }

  @override
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

  Widget build(BuildContext context) {
    final theme = Theme.of(context);

    // Get worker assignment status
    final bool hasWorkerAssigned = job['has_worker_assigned'] == true;
    final String rating = job['worker_rating']?.toString() ?? '0.0';

    // Extract worker details with fallback
    String workerName = 'No worker assigned';
    if (job['worker_name'] != null &&
        job['worker_name'].toString().trim().isNotEmpty) {
      workerName = job['worker_name'];
    } else {
      final nameFields = ['full_name', 'fullName', 'name'];
      for (var field in nameFields) {
        if (job[field] != null) {
          final nameStr = job[field].toString().trim();
          if (nameStr.isNotEmpty &&
              nameStr.toLowerCase() != 'null' &&
              nameStr.toLowerCase() != 'unknown') {
            workerName = nameStr;
            break;
          }
        }
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
      print('DEBUG: Input date string: $dateString');

      if (dateString == null || dateString.isEmpty) {
        print('DEBUG: Date is null or empty');
        return 'No Date';
      }

      DateTime? date;

      try {
        // Try parsing as YYYY-MM-DD first (original database format)
        date = DateTime.tryParse(dateString);

        // If that fails, use explicit parsing for month day, year format
        if (date == null) {
          final monthNames = {
            'January': 1,
            'February': 2,
            'March': 3,
            'April': 4,
            'May': 5,
            'June': 6,
            'July': 7,
            'August': 8,
            'September': 9,
            'October': 10,
            'November': 11,
            'December': 12,
          };

          // Split the string and parse manually
          final parts = dateString.split(' ');
          if (parts.length == 3) {
            final month = monthNames[parts[0]];
            final day = int.tryParse(parts[1].replaceAll(',', ''));
            final year = int.tryParse(parts[2]);

            if (month != null && day != null && year != null) {
              date = DateTime(year, month, day);
            }
          }
        }
      } catch (e) {
        print('DEBUG: Comprehensive error parsing date: $e');
        return 'Invalid Date';
      }

      if (date == null) {
        print('DEBUG: Could not parse date after all attempts');
        return 'Invalid Date';
      }

      final formattedDate =
          '${_getMonthName(date.month)} ${date.day} ${date.year}';
      print('DEBUG: Formatted date: $formattedDate');

      return formattedDate;
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

    // Get status details
    final String status =
        (job['application_status'] ?? job['status'] ?? 'Pending')
                .toString()
                .isNotEmpty
            ? (job['application_status'] ?? job['status'] ?? 'Pending')
                .toString()
            : 'Pending';
    final Color statusColor = _getStatusColor(status);

    // Extract other job details
    final String shiftId =
        job['shift_id']?.toString() ?? job['id']?.toString() ?? 'N/A';
    final String jobTitle = job['job_title'] ?? 'Untitled Job';
    final String category = job['category'] ?? 'Others';
    final String location = job['location'] ?? 'No Location';
    final String startTime = job['start_time'] ?? 'N/A';
    final String endTime = job['end_time'] ?? 'N/A';
    final String phoneNumber = job['phone_number'] ?? '';
    final String email = job['email'] ?? '';

    String duration = _calculateShiftDuration(
      job['start_time'],
      job['end_time'],
    );
    final pincode = job['job_pincode']?.toString();
    final date = job['job_pincode']?.toString();
    final payrate = job['pay_rate']?.toString();
    return Container(
      margin: const EdgeInsets.only(bottom: 16),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(8),
        boxShadow: [
          BoxShadow(
            color: Colors.grey.shade200,
            blurRadius: 4,
            offset: const Offset(0, 2),
          ),
        ],
      ),
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            // Job Title and Status Row
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
                              // color: Colors.grey.shade600,
                              fontWeight: FontWeight.w500,
                              //fontWeight: FontWeight.w600,
                            ),
                          ),
                          Text(
                            'Shift ID: ${shiftId}',
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
                        color: _getStatusColor(status),
                        borderRadius: BorderRadius.circular(16),
                      ),
                      child: Text(
                        status ?? 'Pending',
                        style: TextStyle(color: Colors.white, fontSize: 12),
                      ),
                    ),

                    // Instead of the PopupMenuButton, add these two buttons directly
                    // Add this at the end of your Row that contains the status badge
                    // Inside your JobCard class, modify the PopupMenuButton for unassigned shifts
                    if (!hasWorkerAssigned)
                      PopupMenuButton<String>(
                        icon: Icon(
                          Icons.more_vert,
                          color: Colors.grey.shade700,
                        ),
                        onSelected: (value) async {
                          if (value == 'edit') {
                            final result = await Get.to(
                              () => PostShiftScreen(jobId: job['shift_id']),
                            );
                            if (result == true) {
                              // Find the parent widget and refresh jobs
                              final manageJobsState =
                                  context
                                      .findAncestorStateOfType<
                                        _ManageHomePageState
                                      >();
                              if (manageJobsState != null) {
                                manageJobsState.fetchJobs();
                              }
                            }
                          } else if (value == 'cancel') {
                            // Show confirmation dialog
                            // Enhanced dialog design for shift cancellation
                            // Enhanced dialog design for shift cancellation
                            final shouldCancel = await showDialog<bool>(
                              context: context,
                              barrierDismissible: false,
                              builder: (BuildContext context) {
                                return Dialog(
                                  shape: RoundedRectangleBorder(
                                    borderRadius: BorderRadius.circular(16),
                                  ),
                                  child: Container(
                                    padding: const EdgeInsets.all(20),
                                    decoration: BoxDecoration(
                                      color: Colors.white,
                                      borderRadius: BorderRadius.circular(16),
                                      boxShadow: [
                                        BoxShadow(
                                          color: Colors.grey.shade200,
                                          blurRadius: 10,
                                          offset: const Offset(0, 4),
                                        ),
                                      ],
                                    ),
                                    child: Column(
                                      mainAxisSize: MainAxisSize.min,
                                      crossAxisAlignment:
                                          CrossAxisAlignment.center,
                                      children: [
                                        // Warning icon
                                        Container(
                                          width: 70,
                                          height: 70,
                                          decoration: BoxDecoration(
                                            color: Colors.red.shade50,
                                            shape: BoxShape.circle,
                                          ),
                                          child: Center(
                                            child: Icon(
                                              Icons.warning_amber_rounded,
                                              color: Colors.red.shade600,
                                              size: 40,
                                            ),
                                          ),
                                        ),
                                        const SizedBox(height: 20),

                                        // Dialog title
                                        Text(
                                          'Cancel Shift',
                                          style: TextStyle(
                                            fontSize: 20,
                                            fontWeight: FontWeight.bold,
                                            color: Colors.black,
                                            fontFamily: 'Inter',
                                          ),
                                          textAlign: TextAlign.center,
                                        ),
                                        const SizedBox(height: 12),

                                        // Dialog message
                                        Text(
                                          'Are you sure you want to cancel this shift? You will receive a full refund to your wallet.',
                                          textAlign: TextAlign.center,
                                          style: TextStyle(
                                            color: Colors.grey.shade700,
                                            fontSize: 14,
                                            fontFamily: 'Inter',
                                          ),
                                        ),

                                        // Information box about cancellation
                                        Container(
                                          margin: EdgeInsets.symmetric(
                                            vertical: 16,
                                          ),
                                          padding: EdgeInsets.all(12),
                                          decoration: BoxDecoration(
                                            color: Colors.blue.shade50,
                                            borderRadius: BorderRadius.circular(
                                              12,
                                            ),
                                            border: Border.all(
                                              color: Colors.blue.shade100,
                                            ),
                                          ),
                                          child: Row(
                                            children: [
                                              Icon(
                                                Icons.info_outline,
                                                color: Colors.blue.shade700,
                                                size: 20,
                                              ),
                                              SizedBox(width: 10),
                                              Expanded(
                                                child: Text(
                                                  'This action cannot be undone. The shift will be removed from active listings.',
                                                  style: TextStyle(
                                                    fontSize: 13,
                                                    color: Colors.blue.shade800,
                                                    fontFamily: 'Inter',
                                                  ),
                                                ),
                                              ),
                                            ],
                                          ),
                                        ),

                                        const SizedBox(height: 16),

                                        // Action buttons with enhanced styling
                                        Row(
                                          children: [
                                            Expanded(
                                              child: OutlinedButton(
                                                onPressed:
                                                    () => Navigator.pop(
                                                      context,
                                                      false,
                                                    ),
                                                style: OutlinedButton.styleFrom(
                                                  padding: EdgeInsets.symmetric(
                                                    vertical: 14,
                                                  ),
                                                  side: BorderSide(
                                                    color: Colors.grey.shade300,
                                                  ),
                                                  shape: RoundedRectangleBorder(
                                                    borderRadius:
                                                        BorderRadius.circular(
                                                          10,
                                                        ),
                                                  ),
                                                ),
                                                child: Text(
                                                  'Keep Shift',
                                                  style: TextStyle(
                                                    color: Colors.grey.shade700,
                                                    fontWeight: FontWeight.w500,
                                                    fontFamily: 'Inter',
                                                  ),
                                                ),
                                              ),
                                            ),
                                            SizedBox(width: 16),
                                            Expanded(
                                              child: ElevatedButton(
                                                onPressed:
                                                    () => Navigator.pop(
                                                      context,
                                                      true,
                                                    ),
                                                style: ElevatedButton.styleFrom(
                                                  backgroundColor: Colors.red,
                                                  foregroundColor: Colors.white,
                                                  padding: EdgeInsets.symmetric(
                                                    vertical: 14,
                                                  ),
                                                  elevation: 0,
                                                  shape: RoundedRectangleBorder(
                                                    borderRadius:
                                                        BorderRadius.circular(
                                                          10,
                                                        ),
                                                  ),
                                                ),
                                                child: Text(
                                                  'Cancel Shift',
                                                  style: TextStyle(
                                                    fontWeight: FontWeight.w600,
                                                    fontFamily: 'Inter',
                                                  ),
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
                            if (shouldCancel != true) return;

                            // Show loading indicator
                            Get.dialog(
                              Center(child: CircularProgressIndicator()),
                              barrierDismissible: false,
                            );

                            try {
                              final supabase = Supabase.instance.client;
                              final user = supabase.auth.currentUser;

                              if (user == null) {
                                Get.back(); // Close loading indicator
                                ScaffoldMessenger.of(context).showSnackBar(
                                  SnackBar(
                                    content: Text(
                                      'You must be logged in to cancel shifts',
                                    ),
                                    backgroundColor: Colors.red,
                                  ),
                                );
                                return;
                              }

                              final shiftId = job['shift_id'];
                              final employerId = job['user_id'];
                              final payRate = (job['pay_rate'] as num?) ?? 0;

                              await supabase
                                  .from('worker_job_listings')
                                  .delete()
                                  .eq('shift_id', shiftId)
                                  .withTimeout(timeout: TimeoutDurations.query);
                              // print('Successfully deleted job listing');

                              // Process refund - refund 100% to the employer
                              try {
                                if (employerId != null) {
                                  final employerWallet =
                                      await supabase
                                          .from('employer_wallet')
                                          .select()
                                          .eq('employer_id', employerId)
                                          .withTimeout(timeout: TimeoutDurations.query)
                                          .maybeSingle();

                                  if (employerWallet != null) {
                                    final currentBalance =
                                        employerWallet['balance'] as num? ?? 0;

                                    // ⚠️ RACE CONDITION WARNING:
                                    // This read-modify-write pattern has a race condition.
                                    // RECOMMENDED: Use Supabase RPC function with atomic updates.

                                    // Add full amount back to employer wallet
                                    await supabase
                                        .from('employer_wallet')
                                        .update({
                                          'balance': currentBalance + payRate,
                                          'last_updated':
                                              DateTime.now().toIso8601String(),
                                        })
                                        .eq('id', employerWallet['id'])
                                        .withTimeout(timeout: TimeoutDurations.query);

                                    // Log the transaction
                                    await supabase
                                        .from('wallet_transactions')
                                        .insert({
                                          'wallet_id': employerWallet['id'],
                                          'amount': payRate,
                                          'transaction_type': 'refund',
                                          'description':
                                              'Full refund for cancelled shift: $shiftId',
                                          'created_at':
                                              DateTime.now().toIso8601String(),
                                          'status': 'completed',
                                        })
                                        .withTimeout(timeout: TimeoutDurations.query);
                                  }
                                }
                              } catch (refundError) {
                                print('Error processing refund: $refundError');
                                // Continue execution
                              }

                              // Close loading dialog
                              Get.back();

                              // Show success message
                              ScaffoldMessenger.of(context).showSnackBar(
                                SnackBar(
                                  content: Text(
                                    'Shift cancelled successfully and funds returned to your wallet',
                                  ),
                                  backgroundColor: Colors.green,
                                ),
                              );

                              // Refresh the jobs list
                              final manageJobsState =
                                  context
                                      .findAncestorStateOfType<
                                        _ManageHomePageState
                                      >();
                              if (manageJobsState != null) {
                                manageJobsState.fetchJobs();
                              }
                            } catch (e) {
                              // Close loading dialog
                              Get.back();

                              // Show error message
                              ScaffoldMessenger.of(context).showSnackBar(
                                SnackBar(
                                  content: Text('Error cancelling shift: $e'),
                                  backgroundColor: Colors.red,
                                ),
                              );
                            }
                          }
                        },
                        itemBuilder:
                            (context) => [
                              // Edit option
                              PopupMenuItem<String>(
                                value: 'edit',
                                child: Row(
                                  children: [
                                    Icon(
                                      Icons.edit,
                                      color: Colors.amber.shade700,
                                      size: 18,
                                    ),
                                    const SizedBox(width: 8),
                                    Text(
                                      'Edit',
                                      style: TextStyle(
                                        color: Colors.amber.shade700,
                                        fontWeight: FontWeight.w500,
                                      ),
                                    ),
                                  ],
                                ),
                              ),

                              // Cancel option
                              PopupMenuItem<String>(
                                value: 'cancel',
                                child: Row(
                                  children: [
                                    Icon(
                                      Icons.cancel,
                                      color: Colors.red.shade700,
                                      size: 18,
                                    ),
                                    const SizedBox(width: 8),
                                    Text(
                                      'Cancel Shift',
                                      style: TextStyle(
                                        color: Colors.red.shade700,
                                        fontWeight: FontWeight.w500,
                                      ),
                                    ),
                                  ],
                                ),
                              ),
                            ],
                        shape: RoundedRectangleBorder(
                          borderRadius: BorderRadius.circular(8),
                        ),
                        color: Colors.white,
                        elevation: 4,
                        offset: const Offset(0, 8),
                      ),
                  ],
                ),
              ),
            ),
            // Time and Location
            const SizedBox(height: 12),

            Row(
              mainAxisAlignment: MainAxisAlignment.spaceBetween,
              children: [
                // Duration
                Row(
                  children: [
                    Icon(Icons.business, size: 18, color: Colors.black),
                    SizedBox(width: 8),
                    Text(
                      'Company: '
                      '${job['company'] ?? 'Company'}',
                      style: TextStyle(
                        fontSize: 15,
                        color: Colors.grey.shade800,
                      ),
                      maxLines: 2,
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
                Expanded(
                  child: Text(
                    'Date : '
                    '${_formatJobDatee(job['date'])}',
                    style: TextStyle(fontSize: 15, color: Colors.grey.shade800),
                    overflow: TextOverflow.ellipsis,
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
                Expanded(
                  child: Text(
                    'Time: '
                    '${_formatJobTime(job['start_time'])} - ${_formatJobTime(job['end_time'])}',
                    style: TextStyle(fontSize: 15, color: Colors.grey.shade800),
                    overflow: TextOverflow.ellipsis,
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
                Icon(Icons.location_on_outlined, size: 18, color: Colors.green),
                const SizedBox(width: 8),
                Expanded(
                  child: Text(
                    'location: '
                    '${job['company'] ?? 'Company'}, ${job['location'] ?? 'Location'}',
                    style: TextStyle(fontSize: 15, color: Colors.grey.shade800),
                    maxLines: 4,
                    overflow: TextOverflow.ellipsis,
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
                    Icon(Icons.pin_drop_outlined, size: 18, color: Colors.red),
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
              ],
            ),
            Row(
              children: [
                Icon(Icons.credit_card, size: 18, color: Colors.red),
                SizedBox(width: 8),
                Text(
                  'Pay Rate per Shift: $payrate',
                  style: TextStyle(color: Colors.grey.shade700, fontSize: 15),
                ),
              ],
            ),

            Divider(height: 24, thickness: 1, color: Colors.grey),

            // Worker Assigned Section
            if (hasWorkerAssigned) ...[
              Row(
                children: [
                  FutureBuilder<String?>(
                    future: Supabase.instance.client
                        .from('documents')
                        .select('photo_image_url')
                        .eq('email', email)
                        .withTimeout(timeout: TimeoutDurations.query)
                        .maybeSingle()
                        .then((value) => value?['photo_image_url'] as String?),
                    builder: (context, snapshot) {
                      if (snapshot.connectionState == ConnectionState.waiting) {
                        return CircleAvatar(
                          radius: 25,
                          backgroundColor: Colors.grey.shade300,
                          child: CircularProgressIndicator(strokeWidth: 2),
                        );
                      }

                      if (snapshot.hasData && snapshot.data != null) {
                        return GestureDetector(
                          onTap:
                              () =>
                                  _showFullImageDialog(context, snapshot.data!),
                          child: CircleAvatar(
                            radius: 25,
                            backgroundImage: NetworkImage(snapshot.data!),
                          ),
                        );
                      }

                      return CircleAvatar(
                        radius: 25,
                        backgroundColor: Colors.blue,
                        child: Text(workerName[0].toUpperCase()),
                      );
                    },
                  ),

                  const SizedBox(width: 12),
                  Expanded(
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        Text(
                          workerName,
                          style: const TextStyle(
                            fontSize: 16,
                            fontWeight: FontWeight.w600,
                            color: Colors.black87,
                          ),
                        ),
                        const SizedBox(height: 4),
                        Row(
                          children: [
                            const Icon(
                              Icons.star,
                              size: 16,
                              color: Colors.amber,
                            ),
                            const SizedBox(width: 4),
                            Text(
                              rating,
                              style: const TextStyle(
                                fontSize: 14,
                                color: Colors.black87,
                              ),
                            ),
                          ],
                        ),
                        const SizedBox(height: 8),
                        Row(
                          children: [
                            const Icon(
                              Icons.phone,
                              size: 16,
                              color: Colors.blue,
                            ),
                            const SizedBox(width: 4),
                            Text(
                              phoneNumber,
                              style: const TextStyle(color: Colors.black87),
                            ),
                          ],
                        ),
                        const SizedBox(height: 4),
                        Row(
                          children: [
                            const Icon(
                              Icons.email,
                              size: 16,
                              color: Colors.blue,
                            ),
                            const SizedBox(width: 4),
                            Text(
                              email,
                              style: const TextStyle(color: Colors.black87),
                            ),
                          ],
                        ),
                      ],
                    ),
                  ),
                ],
              ),

              // Action buttons below worker info
              const SizedBox(height: 16),
              Row(
                children: [
                  Expanded(
                    child: OutlinedButton.icon(
                      icon: Icon(Icons.message, size: 18),
                      label: Text('Message'),
                      onPressed: () async {
                        final Uri smsUri = Uri(
                          scheme: 'sms',
                          path: phoneNumber,
                        );
                        if (await canLaunchUrl(smsUri)) {
                          await launchUrl(smsUri);
                        } else {
                          ScaffoldMessenger.of(context).showSnackBar(
                            SnackBar(
                              content: Text('Could not open messaging app'),
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
                          path: phoneNumber,
                        );
                        if (await canLaunchUrl(telUri)) {
                          await launchUrl(telUri);
                        } else {
                          ScaffoldMessenger.of(context).showSnackBar(
                            SnackBar(content: Text('Could not launch dialer')),
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
              // No worker assigned message
              Container(
                padding: const EdgeInsets.all(12),
                decoration: BoxDecoration(
                  color: Colors.grey.shade100,
                  borderRadius: BorderRadius.circular(8),
                ),
                child: Row(
                  children: [
                    Icon(Icons.info_outline, color: Colors.grey.shade600),
                    const SizedBox(width: 8),
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
    );
  }

  void _showFullImageDialog(BuildContext context, String imageUrl) {
    showDialog(
      context: context,
      builder:
          (_) => Dialog(
            backgroundColor: Colors.transparent,
            child: GestureDetector(
              onTap: () => Navigator.pop(context),
              child: InteractiveViewer(
                child: ClipRRect(
                  borderRadius: BorderRadius.circular(12),
                  child: Image.network(imageUrl),
                ),
              ),
            ),
          ),
    );
  }
}
