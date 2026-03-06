import 'package:flutter/material.dart';
import 'package:get/get.dart';
import 'package:get/get_core/src/get_main.dart';
import 'package:shifthour/login.dart';
import 'package:shifthour/worker/const/Botton_Navigation.dart';
import 'package:shifthour/worker/const/Standard_Appbar.dart';
import 'package:shifthour/worker/const/kyc.dart';
import 'package:shifthour/worker/profile/document_management.dart';
import 'package:shifthour/worker/profile/privacy_policy.dart';
import 'package:shifthour/worker/settings.dart';
import 'package:shifthour/worker/worker_dashboard.dart';
import 'package:supabase_flutter/supabase_flutter.dart';

class ProfileScreen extends StatefulWidget {
  const ProfileScreen({Key? key}) : super(key: key);

  @override
  _ProfileScreenState createState() => _ProfileScreenState();
}

class _ProfileScreenState extends State<ProfileScreen> with NavigationMixin {
  final _formKey = GlobalKey<FormState>();
  bool _isEditMode = false;
  bool _isLoading = true;
  bool _saving = false;
  String? _photoUrl;
  String? _errorMessage;
  final ScrollController _scrollController = ScrollController();
  // Performance metrics
  int _completedTrainings = 0;
  double _averageRating = 0;
  int _onTimePercentage = 0;
  int _shiftsCompleted = 0;
  String _kycStatus = 'Inactive';
  String? _aadharNumber;
  String? _aadharImageUrl;
  String? _photoImageUrl;
  bool _isKycVerified = false;

  // Document verification status
  Map<String, bool> _documentVerification = {
    'Food Handler Certificate': true,
    'Driver\'s License': true,
  };

  // Form controllers
  final _fullNameController = TextEditingController();
  final _emailController = TextEditingController();
  final _phoneController = TextEditingController();
  final _addressController = TextEditingController();

  String? jobSeekerCode;

  // Collapsible sections state
  bool _isPromosExpanded = false;
  bool _isMyProfileExpanded = false;
  bool _isMyAccountExpanded = false;
  bool _isLegalExpanded = false;

  // Track dirty fields
  Map<String, bool> _dirtyFields = {};

  @override
  void initState() {
    super.initState();

    // Set up navigation controller and scroll controller here
    setCurrentTab(4); // Profile tab has index 4
    setPageScrollController(_scrollController);

    // Add listeners to track changes
    _fullNameController.addListener(() => _markFieldDirty('fullName'));
    _phoneController.addListener(() => _markFieldDirty('phone'));
    _addressController.addListener(() => _markFieldDirty('address'));

    // Fetch profile data after the widget is built
    WidgetsBinding.instance.addPostFrameCallback((_) {
      _fetchUserProfile();
      _fetchKycInformation(); // Add this line
    });
  }

  Future<void> _fetchKycInformation() async {
    try {
      final user = Supabase.instance.client.auth.currentUser;
      if (user == null || user.email == null) {
        throw Exception('User not logged in or email not available');
      }

      final email = user.email!;

      // Query the documents table for KYC information
      final response =
          await Supabase.instance.client
              .from('documents')
              .select()
              .eq('email', email)
              .maybeSingle();

      if (response != null) {
        setState(() {
          _aadharNumber = response['aadhar_number'] ?? '';
          _aadharImageUrl = response['aadhar_image_url'] ?? '';
          _photoImageUrl = response['photo_image_url'] ?? '';
          _kycStatus = response['status'] ?? 'Inactive';
          _isKycVerified =
              _kycStatus == 'Approved' ||
              _kycStatus == 'Verified' ||
              _kycStatus == 'Active';
        });
      } else {
        setState(() {
          _kycStatus = 'Inactive';
          _isKycVerified = false;
        });
      }
    } catch (e) {
      print('Error fetching KYC information: $e');
      setState(() {
        _kycStatus = 'Inactive';
        _isKycVerified = false;
      });
    }
  }

  void _showImagePopup(BuildContext context, String imageUrl, String title) {
    showDialog(
      context: context,
      builder: (BuildContext context) {
        return Dialog(
          backgroundColor: Colors.transparent,
          insetPadding: EdgeInsets.all(16),
          child: Container(
            constraints: BoxConstraints(
              maxWidth: MediaQuery.of(context).size.width,
              maxHeight: MediaQuery.of(context).size.height * 0.8,
            ),
            decoration: BoxDecoration(
              color: Colors.white,
              borderRadius: BorderRadius.circular(16),
            ),
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                // Header with title and close button
                Padding(
                  padding: const EdgeInsets.all(16.0),
                  child: Row(
                    mainAxisAlignment: MainAxisAlignment.spaceBetween,
                    children: [
                      Text(
                        title,
                        style: TextStyle(
                          fontSize: 18,
                          fontWeight: FontWeight.bold,
                        ),
                      ),
                      IconButton(
                        icon: Icon(Icons.close),
                        onPressed: () => Navigator.of(context).pop(),
                      ),
                    ],
                  ),
                ),
                Divider(),
                // Image container with scroll capability
                Flexible(
                  child: InteractiveViewer(
                    minScale: 0.5,
                    maxScale: 3.0,
                    child: Padding(
                      padding: const EdgeInsets.all(16.0),
                      child: Image.network(
                        imageUrl,
                        fit: BoxFit.contain,
                        loadingBuilder: (context, child, loadingProgress) {
                          if (loadingProgress == null) return child;
                          return Center(
                            child: CircularProgressIndicator(
                              value:
                                  loadingProgress.expectedTotalBytes != null
                                      ? loadingProgress.cumulativeBytesLoaded /
                                          loadingProgress.expectedTotalBytes!
                                      : null,
                            ),
                          );
                        },
                        errorBuilder: (context, error, stackTrace) {
                          return Center(
                            child: Column(
                              mainAxisAlignment: MainAxisAlignment.center,
                              children: [
                                Icon(
                                  Icons.error_outline,
                                  color: Colors.red,
                                  size: 48,
                                ),
                                SizedBox(height: 16),
                                Text(
                                  "Failed to load image",
                                  style: TextStyle(color: Colors.red),
                                ),
                              ],
                            ),
                          );
                        },
                      ),
                    ),
                  ),
                ),
              ],
            ),
          ),
        );
      },
    );
  }

  // Add this helper method for status colors
  Color _getStatusColor(String status) {
    switch (status) {
      case 'Active':
      case 'Approved':
      case 'Verified':
        return Colors.green;
      case 'Inactive':
        return Colors.red;
      case 'Pending':
      case 'Inprogress':
        return Colors.yellow;
      default:
        return Colors.grey;
    }
  }

  Future<void> _fetchAttendanceRatings(String jobSeekerId, String email) async {
    try {
      print(
        'DEBUG: Fetching attendance ratings for job seeker ID: $jobSeekerId and email: $email',
      );

      // Step 1: Get all application IDs from worker_job_applications for this user's email
      final applications = await Supabase.instance.client
          .from('worker_job_applications')
          .select('id')
          .eq('email', email);

      print('DEBUG: Found ${applications?.length ?? 0} job applications');

      if (applications == null || applications.isEmpty) {
        print('DEBUG: No job applications found, using default values');
        setState(() {
          _onTimePercentage = 95; // Default value
          _averageRating = 0.0; // Default value
          _shiftsCompleted = 0;
        });
        return;
      }

      // Extract application IDs
      List<String> applicationIds =
          applications.map<String>((app) => app['id'].toString()).toList();
      print('DEBUG: Application IDs: $applicationIds');

      // Step 2: Query worker_attendance for each application ID individually
      // This avoids the issue with the in_ operator and UUID formatting
      List<Map<String, dynamic>> allAttendanceRecords = [];

      for (String appId in applicationIds) {
        print('DEBUG: Querying for application ID: $appId');
        try {
          final attendanceForApp = await Supabase.instance.client
              .from('worker_attendance')
              .select('on_time_rating, performance_rating')
              .eq('application_id', appId);

          if (attendanceForApp != null && attendanceForApp.isNotEmpty) {
            allAttendanceRecords.addAll(attendanceForApp);
          }
        } catch (e) {
          print(
            'DEBUG: Error querying for application $appId: ${e.toString()}',
          );
        }
      }

      print(
        'DEBUG: Found ${allAttendanceRecords.length} total attendance records',
      );

      if (allAttendanceRecords.isNotEmpty) {
        // Calculate average ratings
        double onTimeRatingSum = 0;
        double performanceRatingSum = 0;
        int validOnTimeRatings = 0;
        int validPerformanceRatings = 0;

        for (var record in allAttendanceRecords) {
          // Handle on_time_rating separately
          if (record['on_time_rating'] != null) {
            onTimeRatingSum += record['on_time_rating'];
            validOnTimeRatings++;
          }

          // Handle performance_rating separately
          if (record['performance_rating'] != null) {
            performanceRatingSum += record['performance_rating'];
            validPerformanceRatings++;
          }
        }

        // Update state with calculated values
        setState(() {
          // Calculate on-time percentage if there are valid ratings
          if (validOnTimeRatings > 0) {
            _onTimePercentage =
                ((onTimeRatingSum / validOnTimeRatings) * 20)
                    .round(); // Scale from 1-5 to percentage
          } else {
            _onTimePercentage = 00; // Default value if no ratings
          }

          // Calculate average performance rating if there are valid ratings
          if (validPerformanceRatings > 0) {
            _averageRating = double.parse(
              (performanceRatingSum / validPerformanceRatings).toStringAsFixed(
                1,
              ),
            );
          } else {
            _averageRating = 0.0; // Default value if no ratings
          }

          // Set shifts completed to the number of attendance records
          _shiftsCompleted = allAttendanceRecords.length;
        });

        print(
          'DEBUG: Calculated ratings - On-time: $_onTimePercentage%, Performance: $_averageRating, Shifts: $_shiftsCompleted',
        );
      } else {
        // No attendance records found, using default values
        setState(() {
          _onTimePercentage = 00; // Default value
          _averageRating = 0.0; // Default value
          _shiftsCompleted = 0;
        });
        print('DEBUG: No attendance records found, using default values');
      }
    } catch (e) {
      print('DEBUG: Error fetching attendance ratings: ${e.toString()}');
      // If there's an error, keep default values
      setState(() {
        _onTimePercentage = 00; // Default value
        _averageRating = 0.0; // Default value
      });
    }
  }

  // Make sure to update your _fetchUserProfile method to call this updated function
  Future<void> _fetchUserProfile() async {
    setState(() => _isLoading = true);

    try {
      // Get current user
      final user = Supabase.instance.client.auth.currentUser;
      if (user == null || user.email == null) {
        throw Exception('User not logged in or email not available');
      }

      final email = user.email!;
      print('DEBUG: Fetching profile for email: $email');

      // Fetch profile data
      final response =
          await Supabase.instance.client
              .from('job_seekers')
              .select()
              .eq('email', email)
              .maybeSingle();

      print('DEBUG: JOB SEEKER DATA: ${response?.toString()}');

      if (response != null) {
        // Store the job seeker's ID to use for attendance queries
        final jobSeekerId = response['id'];

        setState(() {
          // Set controllers with data
          _fullNameController.text = response['full_name'] ?? '';
          _emailController.text = response['email'] ?? '';
          _phoneController.text = response['phone'] ?? '';
          _addressController.text = response['location'] ?? '';

          jobSeekerCode = response['job_seeker_code'] ?? 'N/A';

          // Set photo if available
          _photoUrl = response['profile_photo_url'];
        });

        // Fetch attendance data for ratings - pass both ID and email
        await _fetchAttendanceRatings(jobSeekerId, email);
      } else {
        setState(
          () =>
              _errorMessage = 'Profile not found. Please set up your profile.',
        );
      }
    } catch (e) {
      print('DEBUG: Error in _fetchUserProfile: ${e.toString()}');
      setState(() => _errorMessage = 'Error loading profile: ${e.toString()}');
    } finally {
      setState(() => _isLoading = false);
    }
  } // Update the fetch profile method to pass email to the attendance ratings method

  // Alternative approach: Direct query with a join if your Supabase setup supports it
  Future<void> _fetchAttendanceRatingsDirect(String email) async {
    try {
      print('DEBUG: Attempting direct join query for email: $email');

      // This is a more advanced query that tries to join the tables directly
      // Note: This might need adjustment based on your exact Supabase setup and permissions
      final result = await Supabase.instance.client.rpc(
        'get_worker_ratings',
        params: {'worker_email': email},
      );

      print('DEBUG: Join query result: $result');
      // Process the results here
    } catch (e) {
      print('DEBUG: Error in direct join query: ${e.toString()}');
      // Fall back to default values
    }
  } // Add this method to handle applications with the correct column name

  Future<void> _fetchUserApplications() async {
    try {
      final user = Supabase.instance.client.auth.currentUser;
      if (user == null) return;

      print('DEBUG: Fetching job applications for user ID: ${user.id}');

      // Use the correct column name: application_status instead of status
      final applications = await Supabase.instance.client
          .from('worker_job_applications')
          .select(
            'id, job_title, company, application_status, application_date',
          )
          .eq('user_id', user.id);

      // Process applications as needed
      print('DEBUG: Found ${applications.length} job applications');

      // You can use this data to show application history if needed
    } catch (e) {
      print('DEBUG: Error in _fetchUserApplications: ${e.toString()}');
    }
  } // Add a refresh method that can be called after completing shifts

  Future<void> refreshProfileData() async {
    await _fetchUserProfile();
  }

  void _markFieldDirty(String fieldName) {
    setState(() {
      _dirtyFields[fieldName] = true;
    });
  }

  @override
  void dispose() {
    _fullNameController.dispose();
    _emailController.dispose();
    _phoneController.dispose();
    _addressController.dispose();

    super.dispose();
  }

  String _getStatusText(String status) {
    switch (status.toLowerCase()) {
      case 'approved':
      case 'verified':
      case 'active':
        return 'Active';
      case 'pending':
      case 'inprogress':
      case 'in progress':
        return 'Pending';
      case 'rejected':
        return 'Rejected';
      case 'inactive':
      default:
        return 'Inactive';
    }
  }

  void _handlePhotoUpload() {
    // TODO: Implement photo upload with image picker and Supabase storage
    // For this example, we'll just set a placeholder
    setState(() {
      _photoUrl = 'https://via.placeholder.com/150';
    });
  }

  void _toggleEditMode() {
    setState(() {
      _isEditMode = !_isEditMode;
      if (!_isEditMode) {
        // Reset dirty fields when exiting edit mode without saving
        _dirtyFields = {};
      }
    });
  }

  Future<void> _saveProfile() async {
    if (_formKey.currentState!.validate()) {
      setState(() {
        _saving = true;
      });

      try {
        final user = Supabase.instance.client.auth.currentUser;
        if (user == null || user.email == null) {
          throw Exception('User not logged in or email not available');
        }

        final email = user.email!;

        // Prepare data for update
        final updateData = {
          'full_name': _fullNameController.text,
          'phone': _phoneController.text,
          'location': _addressController.text,
          'job_seeker_code': jobSeekerCode,
          'updated_at': DateTime.now().toIso8601String(),
        };

        // Add profile photo if available
        if (_photoUrl != null) {
          updateData['profile_photo_url'] = _photoUrl!;
        }

        // Update the job_seekers table
        await Supabase.instance.client
            .from('job_seekers')
            .update(updateData)
            .eq('email', email);

        // Show success message
        if (mounted) {
          ScaffoldMessenger.of(context).showSnackBar(
            SnackBar(
              content: Row(
                children: const [
                  Icon(Icons.check_circle, color: Colors.white),
                  SizedBox(width: 8),
                  Text('Profile Updated Successfully'),
                ],
              ),
              backgroundColor: Colors.green.shade700,
              duration: const Duration(seconds: 3),
              behavior: SnackBarBehavior.floating,
              shape: RoundedRectangleBorder(
                borderRadius: BorderRadius.circular(10),
              ),
            ),
          );
        }

        // Reset dirty fields and exit edit mode
        setState(() {
          _dirtyFields = {};
          _isEditMode = false;
        });
      } catch (e) {
        // Show error message
        if (mounted) {
          ScaffoldMessenger.of(context).showSnackBar(
            SnackBar(
              content: Text('Error updating profile: ${e.toString()}'),
              backgroundColor: Colors.red.shade700,
              duration: const Duration(seconds: 3),
              behavior: SnackBarBehavior.floating,
              shape: RoundedRectangleBorder(
                borderRadius: BorderRadius.circular(10),
              ),
            ),
          );
        }
      } finally {
        if (mounted) {
          setState(() {
            _saving = false;
          });
        }
      }
    }
  }

  Future<void> _signOut() async {
    try {
      await Supabase.instance.client.auth.signOut();
      // Navigate to login screen after successful signout
      Get.offAllNamed('/login');
    } catch (e) {
      print('Error signing out: $e');
      // Show error message
    }
  }

  void _showDeleteAccountDialog() {
    showDialog(
      context: context,
      builder: (BuildContext context) {
        return AlertDialog(
          title: const Text('Delete Account'),
          content: const Text(
            'Are you sure you want to delete your account? This action cannot be undone.',
          ),
          actions: [
            TextButton(
              onPressed: () => Navigator.of(context).pop(),
              child: const Text('Cancel'),
            ),
            TextButton(
              onPressed: () {
                // TODO: Implement account deletion
                Navigator.of(context).pop();
                ScaffoldMessenger.of(context).showSnackBar(
                  SnackBar(
                    content: const Text(
                      'Account deletion is not implemented in this demo',
                    ),
                    backgroundColor: Colors.orange.shade700,
                  ),
                );
              },
              child: const Text('Delete', style: TextStyle(color: Colors.red)),
            ),
          ],
        );
      },
    );
  }

  void showPrivacyPolicy(BuildContext context) {
    showDialog(
      context: context,
      builder: (context) => const PrivacyPolicyPage(),
    );
  }

  @override
  Widget build(BuildContext context) {
    final isDarkMode = Theme.of(context).brightness == Brightness.dark;

    return Scaffold(
      appBar: StandardAppBar(
        title: 'My Profile',
        isDarkMode: false,

        actions: [
          if (_isEditMode)
            IconButton(
              icon: Icon(Icons.close, color: Colors.red.shade400),
              onPressed: _toggleEditMode,
              tooltip: 'Cancel',
            ),
          const SizedBox(width: 8),
          ElevatedButton.icon(
            onPressed:
                _saving ? null : (_isEditMode ? _saveProfile : _toggleEditMode),
            icon:
                _saving
                    ? SizedBox(
                      width: 16,
                      height: 16,
                      child: CircularProgressIndicator(
                        strokeWidth: 2,
                        color: Colors.white,
                      ),
                    )
                    : Icon(_isEditMode ? Icons.save : Icons.edit, size: 16),
            label: Text(
              _saving
                  ? 'Saving...'
                  : (_isEditMode ? 'Save Profile' : 'Edit Profile'),
            ),
            style: ElevatedButton.styleFrom(
              backgroundColor:
                  _isEditMode ? Colors.green.shade600 : Colors.blue.shade600,
              foregroundColor: Colors.white,
              padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
              shape: RoundedRectangleBorder(
                borderRadius: BorderRadius.circular(8),
              ),
              elevation: 3,
            ),
          ),
          const SizedBox(width: 12),
          _buildDropdownMenu(context),
        ],
      ),
      bottomNavigationBar: const ShiftHourBottomNavigation(),
      body: Container(
        decoration: BoxDecoration(
          gradient: LinearGradient(
            begin: Alignment.topCenter,
            end: Alignment.bottomCenter,
            colors:
                isDarkMode
                    ? [Colors.grey.shade900, Colors.black]
                    : [Colors.grey.shade50, Colors.grey.shade100],
          ),
        ),
        child: SafeArea(
          child: Column(
            children: [
              // App Bar
              // Loading indicator or error message
              if (_isLoading)
                Expanded(
                  child: Center(
                    child: Column(
                      mainAxisAlignment: MainAxisAlignment.center,
                      children: [
                        CircularProgressIndicator(),
                        SizedBox(height: 16),
                        Text('Loading profile data...'),
                      ],
                    ),
                  ),
                )
              else if (_errorMessage != null)
                Expanded(
                  child: Center(
                    child: Column(
                      mainAxisAlignment: MainAxisAlignment.center,
                      children: [
                        Icon(Icons.error_outline, size: 48, color: Colors.red),
                        SizedBox(height: 16),
                        Text(
                          _errorMessage!,
                          style: TextStyle(color: Colors.red),
                          textAlign: TextAlign.center,
                        ),
                        SizedBox(height: 24),
                        ElevatedButton(
                          onPressed: _fetchUserProfile,
                          child: Text('Retry'),
                        ),
                      ],
                    ),
                  ),
                )
              else
                // Scrollable content
                Expanded(
                  child: SingleChildScrollView(
                    controller: _scrollController,
                    padding: const EdgeInsets.all(16),
                    child: Form(
                      key: _formKey,
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          // Profile header with user info
                          Container(
                            decoration: BoxDecoration(
                              color:
                                  isDarkMode
                                      ? Colors.grey.shade800
                                      : Colors.white,
                              borderRadius: BorderRadius.circular(16),
                              boxShadow: [
                                BoxShadow(
                                  color: Colors.black.withOpacity(0.05),
                                  spreadRadius: 1,
                                  blurRadius: 5,
                                ),
                              ],
                            ),
                            child: Column(
                              children: [
                                Stack(
                                  clipBehavior: Clip.none,
                                  alignment: Alignment.center,
                                  children: [
                                    // Cover background
                                    Container(
                                      height: 100,
                                      width: double.infinity,
                                      decoration: BoxDecoration(
                                        borderRadius: BorderRadius.only(
                                          topLeft: Radius.circular(16),
                                          topRight: Radius.circular(16),
                                        ),
                                        gradient: LinearGradient(
                                          begin: Alignment.topLeft,
                                          end: Alignment.bottomRight,
                                          colors: [
                                            Color(0xFF4F46E5),
                                            Color(0xFF6366F1),
                                          ],
                                        ),
                                        image: const DecorationImage(
                                          image: NetworkImage(
                                            'https://raw.githubusercontent.com/shadcn-ui/ui/main/apps/www/public/card-pattern.png',
                                          ),
                                          fit: BoxFit.cover,
                                          opacity: 0.3,
                                        ),
                                      ),
                                    ),

                                    // Profile photo
                                    Positioned(
                                      top: 50,
                                      child: Stack(
                                        children: [
                                          Container(
                                            width: 100,
                                            height: 100,
                                            decoration: BoxDecoration(
                                              color:
                                                  isDarkMode
                                                      ? Colors.grey.shade800
                                                      : Colors.white,
                                              shape: BoxShape.circle,
                                              boxShadow: [
                                                BoxShadow(
                                                  color: Colors.black
                                                      .withOpacity(0.1),
                                                  spreadRadius: 1,
                                                  blurRadius: 10,
                                                  offset: const Offset(0, 5),
                                                ),
                                              ],
                                            ),
                                            padding: const EdgeInsets.all(4),
                                            child:
                                                (_photoImageUrl != null &&
                                                        _photoImageUrl!
                                                            .isNotEmpty)
                                                    ? ClipRRect(
                                                      borderRadius:
                                                          BorderRadius.circular(
                                                            50,
                                                          ),
                                                      child: Image.network(
                                                        _photoImageUrl!,
                                                        fit: BoxFit.cover,
                                                        width: 100,
                                                        height: 100,
                                                        errorBuilder: (
                                                          context,
                                                          error,
                                                          stackTrace,
                                                        ) {
                                                          return _buildAvatarPlaceholder();
                                                        },
                                                      ),
                                                    )
                                                    : _buildAvatarPlaceholder(),
                                          ),
                                          if (_isEditMode)
                                            Positioned(
                                              bottom: 0,
                                              right: 0,
                                              child: Container(
                                                decoration: BoxDecoration(
                                                  color:
                                                      isDarkMode
                                                          ? Colors.grey.shade800
                                                          : Colors.white,
                                                  shape: BoxShape.circle,
                                                  boxShadow: [
                                                    BoxShadow(
                                                      color: Colors.black
                                                          .withOpacity(0.1),
                                                      spreadRadius: 1,
                                                      blurRadius: 5,
                                                      offset: const Offset(
                                                        0,
                                                        2,
                                                      ),
                                                    ),
                                                  ],
                                                ),
                                                child: IconButton(
                                                  onPressed: _handlePhotoUpload,
                                                  icon: const Icon(
                                                    Icons.camera_alt,
                                                    size: 20,
                                                  ),
                                                  color: Colors.blue.shade600,
                                                  splashRadius: 20,
                                                ),
                                              ),
                                            ),
                                        ],
                                      ),
                                    ),
                                  ],
                                ),

                                // User name and status
                                Padding(
                                  padding: const EdgeInsets.only(
                                    top: 60,
                                    bottom: 4,
                                    left: 16,
                                    right: 16,
                                  ),
                                  child: Column(
                                    children: [
                                      Text(
                                        _fullNameController.text,
                                        style: TextStyle(
                                          fontSize: 24,
                                          fontWeight: FontWeight.bold,
                                        ),
                                        textAlign: TextAlign.center,
                                      ),
                                      const SizedBox(height: 4),
                                      Container(
                                        padding: EdgeInsets.symmetric(
                                          horizontal: 12,
                                          vertical: 4,
                                        ),
                                        decoration: BoxDecoration(
                                          color: _getStatusColor(
                                            _kycStatus,
                                          ).withOpacity(0.1),
                                          borderRadius: BorderRadius.circular(
                                            16,
                                          ),
                                          border: Border.all(
                                            color: _getStatusColor(_kycStatus),
                                            width: 1,
                                          ),
                                        ),
                                        child: Row(
                                          mainAxisSize: MainAxisSize.min,
                                          children: [
                                            Container(
                                              width: 8,
                                              height: 8,
                                              decoration: BoxDecoration(
                                                color: _getStatusColor(
                                                  _kycStatus,
                                                ),
                                                shape: BoxShape.circle,
                                              ),
                                            ),
                                            const SizedBox(width: 6),
                                            if (jobSeekerCode != null &&
                                                jobSeekerCode!.isNotEmpty) ...[
                                              Text(
                                                jobSeekerCode!,
                                                style: TextStyle(
                                                  fontSize: 14,
                                                  color: Colors.grey.shade600,
                                                  fontWeight: FontWeight.w500,
                                                ),
                                              ),
                                              const SizedBox(width: 6),
                                            ],
                                            Text(
                                              _getStatusText(_kycStatus),
                                              style: TextStyle(
                                                color: _getStatusColor(
                                                  _kycStatus,
                                                ),
                                                fontSize: 14,
                                                fontWeight: FontWeight.w500,
                                              ),
                                            ),
                                          ],
                                        ),
                                      ),

                                      // Add this helper method to convert KYC status to display text:
                                    ],
                                  ),
                                ),

                                // Performance metrics
                                // Instead of using Padding + GridView.count
                                Padding(
                                  padding: const EdgeInsets.symmetric(
                                    horizontal: 16,
                                  ), // Remove padding from bottom and top
                                  child: LayoutBuilder(
                                    builder: (context, constraints) {
                                      // Calculate dynamic childAspectRatio based on available width
                                      double width = constraints.maxWidth;
                                      double itemWidth =
                                          (width - 12) /
                                          2; // 12 is your crossAxisSpacing
                                      double aspectRatio =
                                          itemWidth /
                                          90; // Adjust the height (90) as needed

                                      return GridView.count(
                                        crossAxisCount: 2,
                                        crossAxisSpacing: 12,
                                        mainAxisSpacing: 12,
                                        shrinkWrap: true,
                                        childAspectRatio: aspectRatio,
                                        physics: NeverScrollableScrollPhysics(),
                                        children: [
                                          _buildMetricCard(
                                            title: 'Average Rating',
                                            value: '$_averageRating',
                                            icon: Icons.star,
                                            color: Colors.amber,
                                            isDarkMode: isDarkMode,
                                          ),
                                          _buildMetricCard(
                                            title: 'On-time Rate',
                                            value: '$_onTimePercentage%',
                                            icon: Icons.timer,
                                            color: Colors.green,
                                            isDarkMode: isDarkMode,
                                          ),
                                          _buildMetricCard(
                                            title: 'Shifts Completed',
                                            value: '$_shiftsCompleted',
                                            icon: Icons.work,
                                            color: Colors.purple,
                                            isDarkMode: isDarkMode,
                                          ),
                                        ],
                                      );
                                    },
                                  ),
                                ),
                              ],
                            ),
                          ),

                          const SizedBox(height: 20),

                          // Personal Information
                          Container(
                            decoration: BoxDecoration(
                              color:
                                  isDarkMode
                                      ? Colors.grey.shade800
                                      : Colors.white,
                              borderRadius: BorderRadius.circular(16),
                              boxShadow: [
                                BoxShadow(
                                  color: Colors.black.withOpacity(0.05),
                                  spreadRadius: 1,
                                  blurRadius: 5,
                                ),
                              ],
                            ),
                            padding: const EdgeInsets.all(20),
                            child: Column(
                              crossAxisAlignment: CrossAxisAlignment.start,
                              children: [
                                Text(
                                  'Personal Information',
                                  style: TextStyle(
                                    fontSize: 18,
                                    fontWeight: FontWeight.bold,
                                  ),
                                ),
                                const Divider(height: 32),
                                _isEditMode
                                    ? Column(
                                      children: [
                                        _buildFormField(
                                          controller: _fullNameController,
                                          label: 'Full Name',
                                          hint: 'Enter your full name',
                                          icon: Icons.person,
                                          validator: (value) {
                                            if (value == null ||
                                                value.isEmpty) {
                                              return 'Please enter your name';
                                            }
                                            return null;
                                          },
                                          isDirty:
                                              _dirtyFields['fullName'] ?? false,
                                        ),
                                        const SizedBox(height: 20),
                                        _buildFormField(
                                          controller: _emailController,
                                          label: 'Email Address',
                                          hint: 'Enter your email address',
                                          icon: Icons.email,
                                          validator: (value) {
                                            if (value == null ||
                                                value.isEmpty) {
                                              return 'Please enter your email';
                                            } else if (!RegExp(
                                              r'^[\w-\.]+@([\w-]+\.)+[\w-]{2,4}$',
                                            ).hasMatch(value)) {
                                              return 'Please enter a valid email';
                                            }
                                            return null;
                                          },
                                          enabled: false,
                                          fillColor:
                                              isDarkMode
                                                  ? Colors.grey.shade900
                                                  : Colors.grey.shade50,
                                        ),
                                        const SizedBox(height: 20),
                                        _buildFormField(
                                          controller: _phoneController,
                                          label: 'Phone Number',
                                          hint: 'Enter your phone number',
                                          icon: Icons.phone,
                                          keyboardType: TextInputType.phone,
                                          validator: (value) {
                                            if (value == null ||
                                                value.isEmpty) {
                                              return 'Please enter your phone number';
                                            }
                                            return null;
                                          },
                                          isDirty:
                                              _dirtyFields['phone'] ?? false,
                                        ),
                                        const SizedBox(height: 20),
                                        _buildFormField(
                                          controller: _addressController,
                                          label: 'Address',
                                          hint: 'Enter your address',
                                          icon: Icons.location_on,
                                          maxLines: 3,
                                          isDirty:
                                              _dirtyFields['address'] ?? false,
                                        ),
                                        const SizedBox(height: 20),
                                      ],
                                    )
                                    : Column(
                                      crossAxisAlignment:
                                          CrossAxisAlignment.start,
                                      children: [
                                        _buildInfoItem(
                                          icon: Icons.email,
                                          title: 'Email',
                                          value: _emailController.text,
                                        ),
                                        const SizedBox(height: 16),
                                        _buildInfoItem(
                                          icon: Icons.phone,
                                          title: 'Phone',
                                          value: _phoneController.text,
                                        ),
                                        const SizedBox(height: 16),
                                        _buildInfoItem(
                                          icon: Icons.location_on,
                                          title: 'Address',
                                          value: _addressController.text,
                                        ),
                                        const SizedBox(height: 16),
                                      ],
                                    ),
                              ],
                            ),
                          ),

                          const SizedBox(height: 20),

                          // KYC Information Section
                          _buildKycInfoCard(context),

                          // App version
                          Padding(
                            padding: const EdgeInsets.symmetric(vertical: 24),
                            child: Center(
                              child: Text(
                                'ShiftHour v1.0.0',
                                style: TextStyle(
                                  color:
                                      isDarkMode
                                          ? Colors.grey.shade400
                                          : Colors.grey.shade600,
                                  fontSize: 14,
                                ),
                              ),
                            ),
                          ),
                        ],
                      ),
                    ),
                  ),
                ),
            ],
          ),
        ),
      ),
    );
  }

  Widget _buildDropdownMenu(BuildContext context) {
    return Container(
      margin: EdgeInsets.only(right: 16),
      child: PopupMenuButton(
        offset: Offset(0, 10),
        shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
        color: Colors.blue.shade50, // Mint green background
        constraints: BoxConstraints(minWidth: 60, maxWidth: 80),
        icon: Container(
          padding: EdgeInsets.all(8),
          decoration: BoxDecoration(
            color: Colors.blue.shade700,
            borderRadius: BorderRadius.circular(8),
          ),
          child: Icon(Icons.menu, color: Colors.white, size: 20),
        ),
        itemBuilder:
            (context) => [
              // Profile option

              // Settings option
              PopupMenuItem(
                height: 60,
                padding: EdgeInsets.zero,
                child: Container(
                  alignment: Alignment.center,
                  child: Container(
                    padding: EdgeInsets.all(10),
                    decoration: BoxDecoration(
                      color: Colors.blue.shade700, // Mint green
                      borderRadius: BorderRadius.circular(8),
                    ),
                    child: Icon(
                      Icons.settings_outlined,
                      color: Colors.white,
                      size: 24,
                    ),
                  ),
                ),
                onTap: () {
                  Get.to(SettingsScreen());
                },
              ),

              // ID Card option

              // Logout option
              PopupMenuItem(
                height: 60,
                padding: EdgeInsets.zero,
                child: Container(
                  alignment: Alignment.center,
                  child: Container(
                    padding: EdgeInsets.all(10),
                    decoration: BoxDecoration(
                      color: Colors.blue.shade700,
                      borderRadius: BorderRadius.circular(8),
                    ),
                    child: Icon(
                      Icons.logout_outlined,
                      color: Colors.white,
                      size: 24,
                    ),
                  ),
                ),
                onTap: () {
                  Future.delayed(Duration.zero, () {
                    _handleLogout(context);
                  });
                },
              ),
            ],
      ),
    );
  }

  Future<void> _handleLogout(BuildContext context) async {
    final confirm = await showDialog<bool>(
      context: context,
      builder: (BuildContext dialogContext) {
        return Dialog(
          shape: RoundedRectangleBorder(
            borderRadius: BorderRadius.circular(16),
          ),
          child: Container(
            padding: EdgeInsets.all(24),
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                // Logout Icon
                Container(
                  width: 80,
                  height: 80,
                  decoration: BoxDecoration(
                    color: Colors.red.shade50,
                    shape: BoxShape.circle,
                  ),
                  child: Center(
                    child: Icon(
                      Icons.logout_outlined,
                      color: Colors.red,
                      size: 40,
                    ),
                  ),
                ),
                SizedBox(height: 24),

                // Title
                Text(
                  'Logout',
                  style: TextStyle(
                    fontSize: 24,
                    fontWeight: FontWeight.bold,
                    color: Colors.black87,
                  ),
                ),
                SizedBox(height: 16),

                // Content
                Text(
                  'Are you sure you want to logout?',
                  style: TextStyle(fontSize: 16, color: Colors.grey[700]),
                  textAlign: TextAlign.center,
                ),
                SizedBox(height: 24),

                // Action Buttons
                Row(
                  children: [
                    // Cancel Button
                    Expanded(
                      child: OutlinedButton(
                        onPressed: () => Navigator.of(dialogContext).pop(false),
                        style: OutlinedButton.styleFrom(
                          padding: EdgeInsets.symmetric(vertical: 12),
                          side: BorderSide(color: Colors.grey.shade300),
                          shape: RoundedRectangleBorder(
                            borderRadius: BorderRadius.circular(12),
                          ),
                        ),
                        child: Text(
                          'Cancel',
                          style: TextStyle(
                            color: Colors.grey[700],
                            fontWeight: FontWeight.w500,
                          ),
                        ),
                      ),
                    ),
                    SizedBox(width: 16),

                    // Logout Button
                    Expanded(
                      child: ElevatedButton(
                        onPressed: () => Navigator.of(dialogContext).pop(true),
                        style: ElevatedButton.styleFrom(
                          backgroundColor: Colors.red,
                          padding: EdgeInsets.symmetric(vertical: 12),
                          shape: RoundedRectangleBorder(
                            borderRadius: BorderRadius.circular(12),
                          ),
                        ),
                        child: Text(
                          'Logout',
                          style: TextStyle(
                            color: Colors.white,
                            fontWeight: FontWeight.w600,
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

    if (confirm == true) {
      try {
        // Show loading indicator
        showDialog(
          context: context,
          barrierDismissible: false,
          builder: (_) => const Center(child: CircularProgressIndicator()),
        );

        // Perform Supabase sign out
        await Supabase.instance.client.auth.signOut();

        // Close loading
        if (Navigator.canPop(context)) {
          Navigator.of(context).pop();
        }

        // Navigate to login screen
        Navigator.of(context).pushAndRemoveUntil(
          MaterialPageRoute(builder: (_) => const WorkerLoginPage()),
          (route) => false,
        );
      } catch (e) {
        print('Logout error: $e');
        if (context.mounted) {
          Navigator.of(context).pop(); // Close loading
          ScaffoldMessenger.of(
            context,
          ).showSnackBar(SnackBar(content: Text('Error during logout: $e')));
        }
      }
    }
  }

  Widget _buildKycInfoCard(BuildContext context) {
    final isDarkMode = Theme.of(context).brightness == Brightness.dark;

    return Container(
      decoration: BoxDecoration(
        color: isDarkMode ? Colors.grey.shade800 : Colors.white,
        borderRadius: BorderRadius.circular(16),
        boxShadow: [
          BoxShadow(
            color: Colors.black.withOpacity(0.05),
            spreadRadius: 1,
            blurRadius: 5,
          ),
        ],
      ),
      padding: const EdgeInsets.all(20),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          // Header with status badge
          Row(
            mainAxisAlignment: MainAxisAlignment.spaceBetween,
            children: [
              Row(
                children: [
                  Icon(Icons.verified_rounded, color: Colors.blue, size: 22),
                  SizedBox(width: 8),
                  Text(
                    'KYC Information',
                    style: TextStyle(
                      fontSize: 18,
                      fontWeight: FontWeight.bold,
                      color: isDarkMode ? Colors.white : Colors.black,
                    ),
                  ),
                ],
              ),
              Container(
                padding: EdgeInsets.symmetric(horizontal: 12, vertical: 6),
                decoration: BoxDecoration(
                  color:
                      _isKycVerified
                          ? Color(0xFFEDF7ED) // Light green
                          : Color(0xFFFFF8E6), // Light yellow
                  borderRadius: BorderRadius.circular(50),
                ),
                child: Text(
                  _isKycVerified ? 'Verified' : 'Pending',
                  style: TextStyle(
                    color:
                        _isKycVerified
                            ? Color(0xFF1E8E3E) // Dark green
                            : Color(0xFF7A5C00), // Dark yellow/amber
                    fontWeight: FontWeight.w500,
                    fontSize: 14,
                  ),
                ),
              ),
            ],
          ),
          SizedBox(height: 20),

          // Aadhar Number
          if (_aadharNumber != null && _aadharNumber!.isNotEmpty) ...[
            Text(
              'Aadhar Number',
              style: TextStyle(
                fontSize: 14,
                fontWeight: FontWeight.w500,
                color: isDarkMode ? Colors.grey.shade300 : Colors.grey.shade700,
              ),
            ),
            SizedBox(height: 8),
            Text(
              _aadharNumber ?? 'Not provided',
              style: TextStyle(
                fontSize: 16,
                fontWeight: FontWeight.w600,
                color: isDarkMode ? Colors.white : Colors.black,
              ),
            ),
            SizedBox(height: 20),
          ],

          // Documents section
          Row(
            children: [
              Expanded(
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(
                      'Aadhar Card',
                      style: TextStyle(
                        fontSize: 14,
                        fontWeight: FontWeight.w500,
                        color:
                            isDarkMode
                                ? Colors.grey.shade300
                                : Colors.grey.shade700,
                      ),
                    ),
                    SizedBox(height: 8),
                    _buildDocumentThumbnail(
                      context,
                      _aadharImageUrl ?? '',
                      'Aadhar Card',
                      Icons.credit_card,
                      () => _showImagePopup(
                        context,
                        _aadharImageUrl ?? '',
                        'Aadhar Card',
                      ),
                    ),
                  ],
                ),
              ),
              SizedBox(width: 16),
              Expanded(
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(
                      'Photo ID',
                      style: TextStyle(
                        fontSize: 14,
                        fontWeight: FontWeight.w500,
                        color:
                            isDarkMode
                                ? Colors.grey.shade300
                                : Colors.grey.shade700,
                      ),
                    ),
                    SizedBox(height: 8),
                    _buildDocumentThumbnail(
                      context,
                      _photoImageUrl ?? '',
                      'Photo ID',
                      Icons.person,
                      () => _showImagePopup(
                        context,
                        _photoImageUrl ?? '',
                        'Photo ID',
                      ),
                    ),
                  ],
                ),
              ),
            ],
          ),

          // Verification button if not verified
          if (!_isKycVerified) ...[
            SizedBox(height: 20),
            Center(
              child: ElevatedButton.icon(
                onPressed: () {
                  // Navigate to verification form
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
                              _fetchKycInformation();
                            },
                          ),
                        ),
                      );
                    },
                  );
                },
                icon: Icon(Icons.verified_user),
                label: Text('Complete Verification'),
                style: ElevatedButton.styleFrom(
                  backgroundColor: Colors.blue,
                  foregroundColor: Colors.white,
                  padding: EdgeInsets.symmetric(horizontal: 20, vertical: 12),
                  shape: RoundedRectangleBorder(
                    borderRadius: BorderRadius.circular(10),
                  ),
                ),
              ),
            ),
          ],
        ],
      ),
    );
  }

  Widget _buildDocumentThumbnail(
    BuildContext context,
    String url,
    String title,
    IconData fallbackIcon,
    VoidCallback onTap,
  ) {
    final isDarkMode = Theme.of(context).brightness == Brightness.dark;

    return GestureDetector(
      onTap: url.isNotEmpty ? onTap : null,
      child: Container(
        height: 120,
        decoration: BoxDecoration(
          color: isDarkMode ? Colors.grey.shade700 : Colors.grey.shade100,
          borderRadius: BorderRadius.circular(8),
          border: Border.all(
            color: isDarkMode ? Colors.grey.shade600 : Colors.grey.shade300,
          ),
        ),
        child:
            url.isNotEmpty
                ? Stack(
                  children: [
                    ClipRRect(
                      borderRadius: BorderRadius.circular(8),
                      child: Image.network(
                        url,
                        width: double.infinity,
                        height: double.infinity,
                        fit: BoxFit.cover,
                        errorBuilder: (context, error, stackTrace) {
                          return Center(
                            child: Icon(
                              fallbackIcon,
                              size: 40,
                              color:
                                  isDarkMode
                                      ? Colors.grey.shade400
                                      : Colors.grey.shade400,
                            ),
                          );
                        },
                      ),
                    ),
                    Positioned.fill(
                      child: Container(
                        decoration: BoxDecoration(
                          borderRadius: BorderRadius.circular(8),
                          color: Colors.black.withOpacity(0.1),
                        ),
                        child: Center(
                          child: Icon(
                            Icons.remove_red_eye,
                            color: Colors.white,
                            size: 24,
                          ),
                        ),
                      ),
                    ),
                  ],
                )
                : Center(
                  child: Column(
                    mainAxisAlignment: MainAxisAlignment.center,
                    children: [
                      Icon(
                        fallbackIcon,
                        size: 36,
                        color:
                            isDarkMode
                                ? Colors.grey.shade400
                                : Colors.grey.shade400,
                      ),
                      SizedBox(height: 8),
                      Text(
                        'No document',
                        style: TextStyle(
                          color:
                              isDarkMode
                                  ? Colors.grey.shade400
                                  : Colors.grey.shade600,
                          fontSize: 12,
                        ),
                      ),
                    ],
                  ),
                ),
      ),
    );
  }

  Widget _buildAvatarPlaceholder() {
    final nameParts = _fullNameController.text.trim().split(' ');
    String initials = '?';

    if (nameParts.length == 1 && nameParts[0].isNotEmpty) {
      initials = nameParts[0][0].toUpperCase();
    } else if (nameParts.length >= 2) {
      initials = nameParts[0][0].toUpperCase() + nameParts[1][0].toUpperCase();
    }

    return Container(
      decoration: BoxDecoration(
        color: Colors.blue.shade600,
        shape: BoxShape.circle,
      ),
      child: Center(
        child: Text(
          initials,
          style: TextStyle(
            color: Colors.white,
            fontSize: 36,
            fontWeight: FontWeight.bold,
          ),
        ),
      ),
    );
  }

  Widget _buildMetricCard({
    required String title,
    required String value,
    required IconData icon,
    required Color color,
    required bool isDarkMode,
  }) {
    return Container(
      decoration: BoxDecoration(
        color: isDarkMode ? Colors.grey.shade700 : Colors.white,
        borderRadius: BorderRadius.circular(12),
        border: Border.all(
          color: isDarkMode ? Colors.grey.shade600 : Colors.grey.shade200,
          width: 1,
        ),
      ),
      padding: const EdgeInsets.all(12),
      child: Column(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          Row(
            mainAxisSize: MainAxisSize.min,
            mainAxisAlignment: MainAxisAlignment.center,
            children: [
              Icon(icon, color: color, size: 18),
              const SizedBox(width: 4),
              Flexible(
                child: Text(
                  title,
                  overflow: TextOverflow.ellipsis,
                  maxLines: 1,
                  textAlign: TextAlign.center,
                  style: TextStyle(
                    fontSize: 12,
                    fontWeight: FontWeight.w500,
                    color:
                        isDarkMode
                            ? Colors.grey.shade300
                            : Colors.grey.shade700,
                  ),
                ),
              ),
            ],
          ),
          const SizedBox(height: 8),
          Text(
            value,
            style: TextStyle(
              fontSize: 22,
              fontWeight: FontWeight.bold,
              color: color,
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildInfoItem({
    required IconData icon,
    required String title,
    required String value,
  }) {
    return Row(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Icon(icon, size: 20, color: Colors.blue.shade600),
        const SizedBox(width: 12),
        Expanded(
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text(
                title,
                style: TextStyle(
                  fontSize: 14,
                  color: Colors.grey.shade600,
                  fontWeight: FontWeight.w500,
                ),
              ),
              const SizedBox(height: 4),
              Text(value, style: TextStyle(fontSize: 16)),
            ],
          ),
        ),
      ],
    );
  }

  Widget _buildFormField({
    required TextEditingController controller,
    required String label,
    required String hint,
    required IconData icon,
    bool enabled = true,
    Color? fillColor,
    TextInputType keyboardType = TextInputType.text,
    String? Function(String?)? validator,
    bool isDirty = false,
    int maxLines = 1,
  }) {
    final isDarkMode = Theme.of(context).brightness == Brightness.dark;

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Row(
          mainAxisAlignment: MainAxisAlignment.spaceBetween,
          children: [
            Text(
              label,
              style: TextStyle(
                fontSize: 14,
                fontWeight: FontWeight.w500,
                color: isDarkMode ? Colors.grey.shade300 : Colors.grey.shade800,
              ),
            ),
            if (isDirty)
              Row(
                children: [
                  Icon(
                    Icons.check_circle,
                    size: 12,
                    color: Colors.green.shade500,
                  ),
                  const SizedBox(width: 4),
                  Text(
                    'Updated',
                    style: TextStyle(
                      fontSize: 12,
                      color: Colors.green.shade500,
                    ),
                  ),
                ],
              ),
          ],
        ),
        const SizedBox(height: 8),
        TextFormField(
          controller: controller,
          enabled: enabled,
          keyboardType: keyboardType,
          validator: validator,
          maxLines: maxLines,
          decoration: InputDecoration(
            hintText: hint,
            prefixIcon: Icon(icon, size: 20),
            filled: true,
            fillColor:
                fillColor ??
                (isDarkMode
                    ? Colors.grey.shade900.withOpacity(0.5)
                    : Colors.grey.shade50),
            contentPadding: const EdgeInsets.symmetric(
              vertical: 16,
              horizontal: 16,
            ),
            border: OutlineInputBorder(
              borderRadius: BorderRadius.circular(12),
              borderSide: BorderSide(
                color: isDarkMode ? Colors.grey.shade700 : Colors.grey.shade200,
              ),
            ),
            enabledBorder: OutlineInputBorder(
              borderRadius: BorderRadius.circular(12),
              borderSide: BorderSide(
                color:
                    isDirty
                        ? Colors.green.shade300
                        : (isDarkMode
                            ? Colors.grey.shade700
                            : Colors.grey.shade200),
              ),
            ),
            focusedBorder: OutlineInputBorder(
              borderRadius: BorderRadius.circular(12),
              borderSide: BorderSide(
                color: isDirty ? Colors.green.shade500 : Colors.blue.shade500,
                width: 2,
              ),
            ),
            errorBorder: OutlineInputBorder(
              borderRadius: BorderRadius.circular(12),
              borderSide: BorderSide(color: Colors.red.shade300),
            ),
            focusedErrorBorder: OutlineInputBorder(
              borderRadius: BorderRadius.circular(12),
              borderSide: BorderSide(color: Colors.red.shade500, width: 2),
            ),
          ),
        ),
      ],
    );
  }

  Widget _buildCollapsibleSection({
    required String title,
    required IconData icon,
    required bool isExpanded,
    required VoidCallback onToggle,
    required List<Widget> children,
  }) {
    final isDarkMode = Theme.of(context).brightness == Brightness.dark;

    return Container(
      decoration: BoxDecoration(
        color: isDarkMode ? Colors.grey.shade800 : Colors.white,
        borderRadius: BorderRadius.circular(16),
        boxShadow: [
          BoxShadow(
            color: Colors.black.withOpacity(0.05),
            spreadRadius: 1,
            blurRadius: 5,
          ),
        ],
      ),
      child: Column(
        children: [
          InkWell(
            onTap: onToggle,
            borderRadius: BorderRadius.circular(16),
            child: Padding(
              padding: const EdgeInsets.all(16),
              child: Row(
                children: [
                  Icon(icon, color: Colors.blue.shade600),
                  const SizedBox(width: 16),
                  Expanded(
                    child: Text(
                      title,
                      style: TextStyle(
                        fontSize: 16,
                        fontWeight: FontWeight.w600,
                      ),
                    ),
                  ),
                  Icon(
                    isExpanded
                        ? Icons.keyboard_arrow_up
                        : Icons.keyboard_arrow_down,
                    color:
                        isDarkMode
                            ? Colors.grey.shade400
                            : Colors.grey.shade700,
                  ),
                ],
              ),
            ),
          ),
          AnimatedCrossFade(
            firstChild: Container(height: 0),
            secondChild: Column(
              children: [
                const Divider(height: 1),
                Padding(
                  padding: const EdgeInsets.symmetric(
                    horizontal: 16,
                    vertical: 8,
                  ),
                  child: Column(children: children),
                ),
              ],
            ),
            crossFadeState:
                isExpanded
                    ? CrossFadeState.showSecond
                    : CrossFadeState.showFirst,
            duration: const Duration(milliseconds: 300),
          ),
        ],
      ),
    );
  }

  Widget _buildSettingsItem({
    required IconData icon,
    required String title,
    required VoidCallback onTap,
  }) {
    final isDarkMode = Theme.of(context).brightness == Brightness.dark;

    return InkWell(
      onTap: onTap,
      borderRadius: BorderRadius.circular(8),
      child: Padding(
        padding: const EdgeInsets.symmetric(vertical: 12, horizontal: 8),
        child: Row(
          children: [
            Icon(
              icon,
              size: 20,
              color: isDarkMode ? Colors.grey.shade300 : Colors.grey.shade700,
            ),
            const SizedBox(width: 16),
            Expanded(child: Text(title, style: TextStyle(fontSize: 16))),
            Icon(
              Icons.chevron_right,
              size: 20,
              color: isDarkMode ? Colors.grey.shade400 : Colors.grey.shade400,
            ),
          ],
        ),
      ),
    );
  }
}
