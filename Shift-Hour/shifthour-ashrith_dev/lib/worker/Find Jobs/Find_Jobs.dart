import 'dart:math';

import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:geocoding/geocoding.dart';
import 'package:geolocator/geolocator.dart';
import 'package:permission_handler/permission_handler.dart';
import 'package:shifthour/worker/const/Botton_Navigation.dart';
import 'package:shifthour/worker/const/Standard_Appbar.dart';
import 'package:shifthour/worker/const/kyc.dart';
import 'package:shifthour/worker/worker_dashboard.dart';
import 'dart:async';
import 'dart:io';

import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:url_launcher/url_launcher.dart';

class FindJobsPage extends StatefulWidget {
  const FindJobsPage({Key? key}) : super(key: key);

  @override
  State<FindJobsPage> createState() => _FindJobsPageState();
}

class _FindJobsPageState extends State<FindJobsPage>
    with SingleTickerProviderStateMixin, NavigationMixin {
  late TabController _tabController;
  final ScrollController _scrollController = ScrollController();
  final TextEditingController _searchController = TextEditingController();

  // State variables
  bool _isLoading = false;
  bool _isScrolled = false;
  String _errorMessage = '';
  String _sortBy = 'Distance (Closest)';

  // Job data lists
  List<Map<String, dynamic>> _allJobs = [];
  List<Map<String, dynamic>> _filteredJobs = [];
  List<Map<String, dynamic>> _recommendedJobs = [];
  List<Map<String, dynamic>> _savedJobs = [];
  List<Map<String, dynamic>> _appliedJobs = [];

  // Track which jobs the user has already applied to
  Set<String> _appliedJobIds = {};
  Set<String> _savedJobIds = {};
  bool _isSavingJob = false;
  List<String> _selectedCategories = [];
  List<String> _availableCities = [];
  String? _selectedCity;
  Position? _currentPosition;
  String _currentPincode = '';
  bool _isLocationLoading = false;
  Map<String, double> _jobDistances = {};
  bool _hasUserSelectedClosest = false;
  List<String> _availableCategories = [];

  @override
  void initState() {
    setCurrentTab(1);
    // Register the scroll controller with the navigation system
    setPageScrollController(_scrollController);

    super.initState();

    // Initialize tab controller with 2 tabs instead of 4
    _tabController = TabController(length: 2, vsync: this);

    // Add listener to tab controller to fetch data when changing tabs
    _tabController.addListener(_handleTabChange);

    // Setup scroll listener
    _scrollController.addListener(_onScroll);

    // Fetch initial data with delay to ensure widget is mounted
    WidgetsBinding.instance.addPostFrameCallback((_) {
      _getCurrentLocation().then((_) {
        _fetchSavedJobs().then((_) {
          // Only fetch applied jobs and other data if saved jobs fetch is successful
          _fetchAppliedJobs().then((_) {
            _fetchJobs().then((_) {
              _fetchAvailableCities();
              _fetchAvailableCategories();
            });
          });
        });
      });
    });
  }

  Future<bool> _checkKycStatus() async {
    try {
      final user = Supabase.instance.client.auth.currentUser;
      if (user == null || user.email == null) {
        return false;
      }

      final response =
          await Supabase.instance.client
              .from('documents')
              .select()
              .eq('email', user.email!)
              .maybeSingle();

      if (response == null) {
        return false;
      }

      final String status = response['status'] ?? 'Inactive';
      return status == 'Approved' || status == 'Verified' || status == 'Active';
    } catch (e) {
      debugPrint('Error checking KYC status: $e');
      return false;
    }
  }

  // Add this method to show KYC dialog
  void _showKycRequiredDialog() {
    showDialog(
      context: context,
      builder:
          (context) => Dialog(
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
                      color: Colors.blue.withOpacity(0.1),
                      shape: BoxShape.circle,
                    ),
                    child: Center(
                      child: Icon(
                        Icons.verified_user_outlined,
                        color: Colors.blue,
                        size: 40,
                      ),
                    ),
                  ),
                  const SizedBox(height: 16),
                  Text(
                    'KYC Verification Required',
                    style: TextStyle(
                      fontSize: 18,
                      fontWeight: FontWeight.bold,
                      color: Colors.blue,
                      fontFamily: 'Inter',
                    ),
                    textAlign: TextAlign.center,
                  ),
                  const SizedBox(height: 12),
                  Text(
                    'You need to complete KYC verification before applying for shifts. This helps us maintain security and trust.',
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
                              color: Colors.blue.withOpacity(0.2),
                            ),
                            foregroundColor: Colors.blue,
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
                            Navigator.pop(context); // Close dialog
                            // Open the KYC verification form
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
                                        // Refresh after completion
                                        setState(() {});
                                      },
                                    ),
                                  ),
                                );
                              },
                            );
                          },
                          style: ElevatedButton.styleFrom(
                            backgroundColor: Colors.blue,
                            foregroundColor: Colors.white,
                            padding: const EdgeInsets.symmetric(vertical: 12),
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
    );
  }

  Future<void> _fetchAvailableCategories() async {
    try {
      setState(() {
        // Show loading state if needed
      });

      final supabase = Supabase.instance.client;

      // Fetch distinct categories from your job listings table
      final response = await supabase
          .from('worker_job_listings')
          .select('category')
          .not('category', 'is', null)
          .limit(50);

      // Extract unique categories
      final Set<String> uniqueCategories = {};

      for (final item in response) {
        final category = item['category']?.toString() ?? '';
        if (category.isNotEmpty) {
          uniqueCategories.add(category);
        }
      }

      // Sort the categories alphabetically
      final List<String> sortedCategories = uniqueCategories.toList()..sort();

      // Add an "Other" category at the end if it doesn't exist
      if (!sortedCategories.contains('Other')) {
        sortedCategories.add('Other');
      }

      setState(() {
        // Replace hardcoded categories with fetched ones
        _availableCategories = sortedCategories;
      });

      debugPrint(
        'Fetched ${_availableCategories.length} categories from database',
      );
    } catch (e) {
      debugPrint('Error fetching categories: $e');
      // Fallback to default categories on error
      setState(() {
        _availableCategories = [
          'Hospitality',
          'Retail',
          'Food Service',
          'Customer Service',
          'Warehouse',
          'Office Admin',
          'Healthcare',
          'Event Staff',
          'Other',
        ];
      });
    }
  }

  Future<void> _getCurrentLocation() async {
    setState(() {
      _isLocationLoading = true;
    });

    try {
      // Check location permissions
      var status = await Permission.location.status;

      if (status.isDenied) {
        // Request location permission
        status = await Permission.location.request();
        if (status.isDenied) {
          throw Exception('Location permission denied');
        }
      }

      if (status.isPermanentlyDenied) {
        // The user opted to never again see the permission request dialog
        _showOpenAppSettingsDialog();
        throw Exception('Location permission permanently denied');
      }

      // Get current position
      _currentPosition = await Geolocator.getCurrentPosition(
        desiredAccuracy: LocationAccuracy.high,
      );

      debugPrint(
        'Current position: ${_currentPosition?.latitude}, ${_currentPosition?.longitude}',
      );

      // Get pincode from current position
      if (_currentPosition != null) {
        await _getPincodeFromPosition(_currentPosition!);
      }
    } catch (e) {
      debugPrint('Error getting location: $e');
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('Failed to get location: ${e.toString()}'),
          backgroundColor: Colors.red,
        ),
      );
    } finally {
      setState(() {
        _isLocationLoading = false;
      });
    }
  }

  // Add method to get pincode from position
  Future<void> _getPincodeFromPosition(Position position) async {
    try {
      List<Placemark> placemarks = await placemarkFromCoordinates(
        position.latitude,
        position.longitude,
      );

      if (placemarks.isNotEmpty) {
        final placemark = placemarks.first;
        setState(() {
          _currentPincode = placemark.postalCode ?? '';
        });
        debugPrint('Current pincode: $_currentPincode');
      }
    } catch (e) {
      debugPrint('Error getting pincode: $e');
    }
  }

  // Add method to show app settings dialog
  void _showOpenAppSettingsDialog() {
    showDialog(
      context: context,
      builder:
          (context) => AlertDialog(
            title: const Text('Location Permission Required'),
            content: const Text(
              'This app needs location permission to sort jobs by distance. '
              'Please enable location in app settings.',
            ),
            actions: [
              TextButton(
                onPressed: () => Navigator.of(context).pop(),
                child: const Text('Cancel'),
              ),
              TextButton(
                onPressed: () {
                  Navigator.of(context).pop();
                  openAppSettings();
                },
                child: const Text('Open Settings'),
              ),
            ],
          ),
    );
  }

  // Add method to calculate distance between pincodes
  double _calculateDistance(String? jobPincode) {
    if (_currentPincode.isEmpty || jobPincode == null || jobPincode.isEmpty) {
      return double.infinity;
    }

    // For exact match, return 0
    if (_currentPincode == jobPincode) {
      return 0;
    }

    // Uses prefix matching, which is not a true geographic distance
    int commonPrefix = 0;
    for (int i = 0; i < _currentPincode.length && i < jobPincode.length; i++) {
      if (_currentPincode[i] == jobPincode[i]) {
        commonPrefix++;
      } else {
        break;
      }
    }

    // Returns a value between 0 and 100 based on prefix match
    return 100 - (commonPrefix * 20);
  }

  void _handleTabChange() {
    // Trigger setState to rebuild with the new tab
    if (mounted) {
      setState(() {});
    }

    // If the selected tab is the All Shifts tab (index 0)
    if (_tabController.index == 0) {
      // If jobs haven't been loaded yet and we're not currently loading
      if (_allJobs.isEmpty && !_isLoading && _errorMessage.isEmpty) {
        _fetchJobs();
      }
    }
    // If the selected tab is the Saved tab, refresh saved jobs
    else if (_tabController.index == 1) {
      _fetchSavedJobs();
    }
    // If the selected tab is the Applied tab, refresh applied jobs
  }

  Future<void> _refreshAllData() async {
    debugPrint('Refreshing all job data...');

    // Clear existing data first
    setState(() {
      _allJobs = [];
      _filteredJobs = [];
      _recommendedJobs = [];
      _isLoading = true;
      _errorMessage = '';
    });

    // Fetch data in the proper sequence
    try {
      await _fetchSavedJobs();
      await _fetchAppliedJobs();
      await _fetchJobs();
      _fetchAvailableCities();

      // Show success message
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(
          content: Text('Job listings refreshed successfully'),
          backgroundColor: Color(0xFF059669), // green-600
          duration: Duration(seconds: 2),
        ),
      );
    } catch (e) {
      debugPrint('Error refreshing data: $e');
      setState(() {
        _errorMessage = 'Failed to refresh. Please try again.';
        _isLoading = false;
      });
    }
  }

  void _fetchAvailableCities() {
    debugPrint('Fetching Available Cities');
    debugPrint('Total Jobs: ${_allJobs.length}');

    // Extract unique cities, handling different location formats
    _availableCities =
        _allJobs
            .map((job) {
              final locationStr = job['location']?.toString() ?? '';
              // Split by comma and take the first part
              final locationParts = locationStr.split(',');
              return locationParts.isNotEmpty ? locationParts[0].trim() : '';
            })
            .whereType<String>()
            .where((location) => location.isNotEmpty)
            .toSet()
            .toList();

    debugPrint('Available Cities Count: ${_availableCities.length}');
    debugPrint('Available Cities: $_availableCities');

    // Sort cities alphabetically
    _availableCities.sort((a, b) => a.compareTo(b));
  }

  // Method to show city selection bottom sheet
  void _showCitySelectionBottomSheet() {
    // Create a list to store filtered cities
    List<String> filteredCities = List.from(_availableCities);

    if (_availableCities.isEmpty) {
      // Show a message if no cities are available
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: const Text(
            'No cities found. Please check your internet connection or try refreshing.',
          ),
          action: SnackBarAction(
            label: 'Refresh',
            onPressed: () {
              _fetchJobs(); // Attempt to fetch jobs again
            },
          ),
        ),
      );
      return;
    }

    showModalBottomSheet(
      context: context,
      isScrollControlled: true,
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.vertical(top: Radius.circular(20)),
      ),
      builder: (context) {
        return StatefulBuilder(
          builder: (BuildContext context, StateSetter setModalState) {
            return DraggableScrollableSheet(
              initialChildSize: 0.7,
              minChildSize: 0.5,
              maxChildSize: 0.9,
              expand: false,
              builder: (context, scrollController) {
                return Container(
                  padding: const EdgeInsets.symmetric(
                    horizontal: 16,
                    vertical: 20,
                  ),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      // Header
                      Row(
                        mainAxisAlignment: MainAxisAlignment.spaceBetween,
                        children: [
                          const Text(
                            'Select City',
                            style: TextStyle(
                              fontSize: 20,
                              fontWeight: FontWeight.bold,
                            ),
                          ),
                          IconButton(
                            icon: const Icon(Icons.close),
                            onPressed: () => Navigator.of(context).pop(),
                          ),
                        ],
                      ),
                      const SizedBox(height: 16),

                      // Search Bar for Cities
                      GestureDetector(
                        behavior: HitTestBehavior.opaque,
                        onTap: () {
                          // Dismiss the keyboard and remove focus
                          FocusScope.of(context).unfocus();
                        },
                        child: TextField(
                          decoration: InputDecoration(
                            hintText: 'Search cities...',
                            prefixIcon: const Icon(Icons.search),
                            border: OutlineInputBorder(
                              borderRadius: BorderRadius.circular(12),
                              borderSide: BorderSide(
                                color: Colors.grey.shade300,
                              ),
                            ),
                          ),
                          onChanged: (value) {
                            setModalState(() {
                              // Filter cities based on search input
                              filteredCities =
                                  _availableCities
                                      .where(
                                        (city) => city.toLowerCase().contains(
                                          value.toLowerCase(),
                                        ),
                                      )
                                      .toList();
                            });
                          },
                        ),
                      ),
                      const SizedBox(height: 16),

                      // All Cities Option
                      ListTile(
                        title: const Text('All Cities'),
                        trailing:
                            _selectedCity == null
                                ? const Icon(
                                  Icons.check_circle,
                                  color: Colors.blue, // indigo-600
                                )
                                : null,
                        onTap: () {
                          setState(() {
                            _selectedCity = null;
                          });
                          _filterJobsByCity();
                          Navigator.of(context).pop();
                        },
                      ),
                      const Divider(),

                      // City List
                      Expanded(
                        child: ListView.builder(
                          controller: scrollController,
                          itemCount: filteredCities.length,
                          itemBuilder: (context, index) {
                            final city = filteredCities[index];
                            return ListTile(
                              title: Text(city),
                              trailing:
                                  _selectedCity == city
                                      ? const Icon(
                                        Icons.check_circle,
                                        color: Colors.blue, // indigo-600
                                      )
                                      : null,
                              onTap: () {
                                setState(() {
                                  _selectedCity = city;
                                });
                                _filterJobsByCity();
                                Navigator.of(context).pop();
                              },
                            );
                          },
                        ),
                      ),
                    ],
                  ),
                );
              },
            );
          },
        );
      },
    );
  }

  void _filterJobsByCity() {
    setState(() {
      // If no city is selected (All Cities), reset to all jobs
      if (_selectedCity == null) {
        _filteredJobs = List.from(_allJobs);
      } else {
        // Filter jobs by selected city with more comprehensive matching
        _filteredJobs =
            _allJobs.where((job) {
              // Extract full location string
              final locationStr =
                  (job['location'] ?? '').toString().toLowerCase();

              // Normalize selected city
              final selectedCityLower = _selectedCity!.toLowerCase().trim();

              // Comprehensive city matching
              bool exactMatch = locationStr == selectedCityLower;
              bool startsWithMatch = locationStr.startsWith(
                '$selectedCityLower,',
              );
              bool containsMatch =
                  locationStr.contains(', $selectedCityLower,') ||
                  locationStr.contains(', $selectedCityLower\$');
              bool partialMatch = locationStr.contains(selectedCityLower);

              bool isMatch =
                  exactMatch ||
                  startsWithMatch ||
                  containsMatch ||
                  partialMatch;

              // Debug prints for troubleshooting
              debugPrint('Location: $locationStr');
              debugPrint('Selected City: $selectedCityLower');
              debugPrint('Exact Match: $exactMatch');
              debugPrint('Starts With Match: $startsWithMatch');
              debugPrint('Contains Match: $containsMatch');
              debugPrint('Partial Match: $partialMatch');
              debugPrint('Final Match: $isMatch');

              return isMatch;
            }).toList();

        // If no jobs match, show a message
        if (_filteredJobs.isEmpty) {
          _showNoJobsInCityMessage();
        }
      }

      // Apply additional text search and category filters
      _filterJobs(_searchController.text);
    });
  }

  void _showNoJobsInCityMessage() {
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        content: Text('No shifts found in ${_selectedCity ?? 'selected city'}'),
        backgroundColor: Colors.orange,
        duration: const Duration(seconds: 2),
      ),
    );
  }

  Future<void> _fetchSavedJobs() async {
    debugPrint('Fetching saved jobs...');

    try {
      // Check internet connectivity
      try {
        final result = await InternetAddress.lookup('google.com');
        if (result.isEmpty || result[0].rawAddress.isEmpty) {
          _handleNoInternet();
          return;
        }
      } on SocketException catch (_) {
        _handleNoInternet();
        return;
      }

      setState(() {
        if (_tabController.index == 1) {
          _isLoading = true;
        }
        _errorMessage = '';
      });

      // Get current user
      final supabase = Supabase.instance.client;
      final user = supabase.auth.currentUser;

      if (user == null) {
        setState(() {
          _errorMessage = 'Please log in to view saved jobs';
          _savedJobIds = {};
          // Do NOT set _isLoading = false here
        });
        return;
      } // Get just the date part in YYYY-MM-DD format
      final today = DateTime.now().toUtc();
      final todayDateOnly =
          "${today.year}-${today.month.toString().padLeft(2, '0')}-${today.day.toString().padLeft(2, '0')}";

      final response = await supabase
          .from('worker_saved_jobs')
          .select('''
      id,
      job_id,
      job_title,
      company,
      location,
      date,
      start_time,
      end_time,
      pay_rate,
      pay_currency,
      match_percentage,
      saved_date,
      shift_id,
      batch_id,
      position_number,
      job_pincode
    ''')
          .gte('date', todayDateOnly) // This will compare just the date part
          .eq('user_id', user.id)
          .order('saved_date', ascending: false);
      debugPrint('Saved jobs response: ${response.length} items');

      // Update the saved job IDs set
      final savedIds = <String>{};
      for (final job in response) {
        final jobId = job['job_id']?.toString();
        if (jobId != null) {
          savedIds.add(jobId);
        }

        // Add is_saved flag to each job
        job['is_saved'] = true;
      }

      setState(() {
        _savedJobs = List<Map<String, dynamic>>.from(response);
        _savedJobIds = savedIds;

        // Only set _isLoading to false if we're on the saved jobs tab
        if (_tabController.index == 1) {
          _isLoading = false;
        }
      });

      debugPrint('Saved job IDs: $_savedJobIds');

      // Trigger job fetching if needed
      if (_allJobs.isEmpty) {
        await _fetchJobs();
      }
    } catch (e) {
      debugPrint('Error fetching saved jobs: $e');
      setState(() {
        _errorMessage = 'Failed to load saved jobs: ${e.toString()}';
        // Only set _isLoading to false if we're on the saved jobs tab
        if (_tabController.index == 1) {
          _isLoading = false;
        }
      });
    }
  }

  Future<bool> _validateJobExists(String jobId) async {
    try {
      final supabase = Supabase.instance.client;

      // Check if the job exists in worker_job_listings
      final jobResult = await supabase
          .from('worker_job_listings')
          .select('id')
          .eq('id', jobId)
          .limit(1);

      return jobResult != null && jobResult.isNotEmpty;
    } catch (e) {
      debugPrint('Error validating job existence: $e');
      return false;
    }
  }

  Future<void> toggleSaveJob(Map<String, dynamic> jobDetails) async {
    // Don't allow multiple save operations at once
    if (_isSavingJob) return;

    setState(() {
      _isSavingJob = true;
    });

    try {
      // 1. Check if user is authenticated
      final supabase = Supabase.instance.client;
      final user = supabase.auth.currentUser;

      if (user == null) {
        _showAuthRequiredDialog();
        setState(() {
          _isSavingJob = false;
        });
        return;
      }

      // Debug print to see the exact job details
      debugPrint('Job Details for Saving: $jobDetails');
      debugPrint('Current tab index: ${_tabController.index}');

      // Ensure we have a valid job ID
      String? jobIdNullable;

      // First check for job_id (this will work for saved jobs)
      if (jobDetails['job_id'] != null &&
          jobDetails['job_id'].toString().isNotEmpty) {
        jobIdNullable = jobDetails['job_id'].toString();
      }
      // Then check for id (this will work for all jobs)
      else if (jobDetails['id'] != null &&
          jobDetails['id'].toString().isNotEmpty) {
        jobIdNullable = jobDetails['id'].toString();
      }

      if (jobIdNullable == null || jobIdNullable.isEmpty) {
        debugPrint('Invalid job ID in details: $jobDetails');
        _showErrorMessage('Invalid job details: No job ID found');
        setState(() {
          _isSavingJob = false;
        });
        return;
      }

      // Convert nullable String to non-nullable String
      final String jobId = jobIdNullable;

      debugPrint('Using job ID: $jobId for save/unsave operation');

      // 2. Check if job is already saved
      if (_savedJobIds.contains(jobId)) {
        // Job is already saved, so unsave it
        debugPrint('Job is already saved. Attempting to unsave...');

        try {
          final deleteResponse = await supabase
              .from('worker_saved_jobs')
              .delete()
              .eq('user_id', user.id)
              .eq('job_id', jobId);

          debugPrint('Delete response: $deleteResponse');

          setState(() {
            _savedJobIds.remove(jobId);

            // Update all job lists to reflect the change
            for (final jobList in [_allJobs, _filteredJobs, _recommendedJobs]) {
              for (final job in jobList) {
                if ((job['id']?.toString() == jobId) ||
                    (job['job_id']?.toString() == jobId)) {
                  job['is_saved'] = false;
                }
              }
            }

            // Remove from saved jobs list if we're on that tab
            _savedJobs.removeWhere(
              (job) =>
                  job['job_id']?.toString() == jobId ||
                  job['id']?.toString() == jobId,
            );

            _isSavingJob = false;
          });

          _showSuccessMessage('Job removed from saved list');
          return; // EXIT HERE AFTER SUCCESSFUL UNSAVE
        } catch (deleteError) {
          debugPrint('Error deleting saved job: $deleteError');
          _showErrorMessage(
            'Error removing job from saved list: ${deleteError.toString()}',
          );
          setState(() {
            _isSavingJob = false;
          });
          return; // EXIT HERE ON ERROR
        }
      } else {
        // 3. Job is not saved, so save it
        debugPrint('Job is not saved. Attempting to save...');

        // NEW: First validate that the job actually exists in worker_job_listings
        // Skip validation if we're on the saved jobs tab (tab index 1) since these jobs might not exist anymore
        if (_tabController.index != 1) {
          bool jobExists = await _validateJobExists(jobId);
          if (!jobExists) {
            _showErrorMessage('This job is no longer available for saving.');
            setState(() {
              _isSavingJob = false;
            });
            return;
          }
        }

        try {
          // Ensure all required fields are present
          String jobTitle = jobDetails['job_title'] ?? 'Unknown Job';
          String company = jobDetails['company'] ?? 'Unknown Company';
          String location = jobDetails['location'] ?? 'Unknown Location';

          // Get shift_id and batch_id values directly
          String shiftId = jobDetails['shift_id']?.toString() ?? '';
          String batchId = jobDetails['batch_id']?.toString() ?? '';

          // Prepare the save data with essential fields
          final saveData = {
            'job_id': jobId, // Use job_id as primary identifier
            'user_id': user.id,
            'job_title': jobTitle,
            'company': company,
            'location': location,
            'date': jobDetails['date'],
            'start_time': jobDetails['start_time'],
            'end_time': jobDetails['end_time'],
            'pay_rate': jobDetails['pay_rate'] ?? 0,
            'pay_currency': jobDetails['pay_currency'] ?? 'INR',
            'match_percentage': jobDetails['match_percentage'] ?? 0,
            'saved_date': DateTime.now().toIso8601String(),
            'job_pincode': jobDetails['job_pincode'] ?? 0,
          };

          // Add shift_id and batch_id if available
          if (shiftId.isNotEmpty) {
            saveData['shift_id'] = shiftId;
          }

          if (batchId.isNotEmpty) {
            saveData['batch_id'] = batchId;
          }

          // Only add position_number if it exists and is an integer
          if (jobDetails['position_number'] != null) {
            final positionNumber = jobDetails['position_number'];
            // Check if it's an integer
            if (positionNumber is int) {
              saveData['position_number'] = positionNumber;
            } else if (positionNumber is String) {
              // Try to parse it as an integer
              final parsedPosition = int.tryParse(positionNumber);
              if (parsedPosition != null) {
                saveData['position_number'] = parsedPosition;
              }
            }
          }

          // Remove any null values
          saveData.removeWhere((key, value) => value == null);

          // Debug print the save data
          debugPrint('Save Data to be inserted: $saveData');

          // Insert into Supabase
          final response = await supabase
              .from('worker_saved_jobs')
              .insert(saveData);
          debugPrint('Insert response: $response');

          setState(() {
            _savedJobIds.add(jobId);

            // Update UI state for all job lists
            for (final jobList in [_allJobs, _filteredJobs, _recommendedJobs]) {
              for (final job in jobList) {
                if ((job['id']?.toString() == jobId) ||
                    (job['job_id']?.toString() == jobId)) {
                  job['is_saved'] = true;
                }
              }
            }

            // Add to _savedJobs if we're on that tab
            if (_tabController.index == 1 &&
                !_savedJobs.any(
                  (job) =>
                      job['job_id']?.toString() == jobId ||
                      job['id']?.toString() == jobId,
                )) {
              final savedJob = Map<String, dynamic>.from(jobDetails);
              savedJob['job_id'] = jobId;
              savedJob['is_saved'] = true;
              savedJob['saved_date'] = DateTime.now().toIso8601String();
              _savedJobs.insert(0, savedJob);
            }

            _isSavingJob = false;
          });

          _showSuccessMessage('Job saved successfully');
        } catch (insertError) {
          debugPrint('Error saving job: $insertError');
          debugPrint('Error type: ${insertError.runtimeType}');

          if (insertError is PostgrestException) {
            debugPrint('PostgrestException details: ${insertError.message}');
            debugPrint('PostgrestException code: ${insertError.code}');
            debugPrint('PostgrestException details: ${insertError.details}');

            String errorMessage = 'Failed to save job. Please try again.';

            if (insertError.message.contains('foreign key constraint')) {
              errorMessage = 'This job is no longer available in the system.';
            } else if (insertError.message.contains(
              'invalid input syntax for type',
            )) {
              errorMessage =
                  'There was an issue with the job data format. Please contact support.';
            } else if (insertError.message.contains('duplicate key value')) {
              errorMessage = 'This job is already saved.';
            }

            _showErrorMessage(errorMessage);
          } else {
            _showErrorMessage('Failed to save job. Please try again.');
          }

          setState(() {
            _isSavingJob = false;
          });
        }
      }
    } catch (e) {
      debugPrint('General error toggling save job: $e');
      debugPrint('Error type: ${e.runtimeType}');
      _showErrorMessage('Failed to save job. Please try again.');
      setState(() {
        _isSavingJob = false;
      });
    }
  }

  String _generateUuid() {
    // Generate a timestamp-based UUID
    final now = DateTime.now();
    final timeMs = now.millisecondsSinceEpoch;
    final random =
        100000 + (DateTime.now().microsecond % 900000); // 6-digit random number

    // Create a UUID v4 format (using random values)
    final String uuidSegment1 = _randomHex(8);
    final String uuidSegment2 = _randomHex(4);
    final String uuidSegment3 = '4' + _randomHex(3); // Version 4 UUID
    final String uuidSegment4 = _randomHex(4);
    final String uuidSegment5 = _randomHex(12);

    return '$uuidSegment1-$uuidSegment2-$uuidSegment3-$uuidSegment4-$uuidSegment5';
  }

  // Helper method to generate random hex strings
  String _randomHex(int length) {
    final random = DateTime.now().microsecond + DateTime.now().millisecond;
    final chars = '0123456789abcdef';
    final buffer = StringBuffer();

    for (var i = 0; i < length; i++) {
      final index = (random + i) % chars.length;
      buffer.write(chars[index]);
    }

    return buffer.toString();
  }

  void _showErrorMessage(String message) {
    // Log the error message for debugging
    debugPrint('Showing error message: $message');

    // Extract a more user-friendly message
    String userMessage = message;

    // Check for common Supabase errors and make them more user-friendly
    if (message.contains('duplicate key value violates unique constraint')) {
      userMessage = 'This job has already been saved.';
    } else if (message.contains('not-found')) {
      userMessage = 'The job information could not be found.';
    } else if (message.contains('permission denied')) {
      userMessage =
          'You do not have permission to save this job. Please login again.';
    } else if (message.length > 100) {
      // If the error message is too long, provide a simpler message
      userMessage =
          'Could not save job due to a server error. Please try again later.';
    }

    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        content: Text(userMessage),
        backgroundColor: Colors.red,
        behavior: SnackBarBehavior.floating,
        shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(8)),
        duration: const Duration(seconds: 3),
        action: SnackBarAction(
          label: 'OK',
          textColor: Colors.white,
          onPressed: () => ScaffoldMessenger.of(context).hideCurrentSnackBar(),
        ),
      ),
    );
  }

  void _showSuccessMessage(String message) {
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        content: Text(message),
        backgroundColor: const Color(0xFF059669), // green-600
        behavior: SnackBarBehavior.floating,
        shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(8)),
        duration: const Duration(seconds: 2),
        action: SnackBarAction(
          label: 'OK',
          textColor: Colors.white,
          onPressed: () => ScaffoldMessenger.of(context).hideCurrentSnackBar(),
        ),
      ),
    );
  }

  void _onScroll() {
    final scrolled = _scrollController.offset > 10;
    if (scrolled != _isScrolled) {
      setState(() {
        _isScrolled = scrolled;
      });
    }
  }

  Future<void> _fetchJobs() async {
    // Check internet connectivity
    if (!_isLoading) {
      setState(() {
        _isLoading = true;
        _errorMessage = '';
        _allJobs = [];
        _filteredJobs = [];
      });
    }
    try {
      final result = await InternetAddress.lookup('google.com');
      if (result.isEmpty || result[0].rawAddress.isEmpty) {
        _handleNoInternet();
        return;
      }
    } on SocketException catch (_) {
      _handleNoInternet();
      return;
    }

    try {
      // Verify Supabase client is initialized
      final supabase = Supabase.instance.client;
      if (supabase == null) {
        throw Exception('Supabase client is not initialized');
      }

      // FIRST: Get all existing job applications to check occupied positions
      final allApplicationsResponse = await supabase
          .from('worker_job_applications')
          .select('shift_id, batch_id, position_number');

      // Create a Set of unique position identifiers
      final Set<String> takenPositions = {};
      for (final app in allApplicationsResponse) {
        final shiftId = app['shift_id']?.toString() ?? '';
        final batchId = app['batch_id']?.toString() ?? '';
        final positionNumber = app['position_number']?.toString() ?? '';

        // Create a unique key for this position
        final key = '$shiftId|$batchId|$positionNumber';
        takenPositions.add(key);
      }

      debugPrint('Found ${takenPositions.length} taken positions');

      final today = DateTime.now().toUtc().toIso8601String();
      // Start with base query - UPDATED to include batch-related fields
      var response = await supabase
          .from('worker_job_listings')
          .select('''
        id,
        job_title,
        company,
        location,
        date,
        start_time,
        end_time,
        pay_rate,
        pay_currency,
        company_logo,
        logo_color,
        status,
        created_at,
        category,
        job_pincode,
        number_of_positions,
        shift_id,
        batch_id,
        position_number,
        start_date_time,
        end_date_time,
        user_id
      ''')
          .gte('date', today)
          .order('created_at', ascending: false)
          .limit(50);

      debugPrint('Fetched ${response.length} jobs before filtering');

      // Filter out jobs where position is already taken
      response =
          response.where((job) {
            final shiftId = job['shift_id']?.toString() ?? '';
            final batchId = job['batch_id']?.toString() ?? '';
            final positionNumber = job['position_number']?.toString() ?? '';

            // Create the unique key for this job position
            final key = '$shiftId|$batchId|$positionNumber';

            // Debug log for taken positions
            if (takenPositions.contains(key)) {
              debugPrint(
                'Filtering out job: ${job['job_title']} with position key: $key',
              );
            }

            // Exclude this job if the position is already taken
            return !takenPositions.contains(key);
          }).toList();

      debugPrint('After filtering, ${response.length} jobs remain');

      // If categories are selected, filter the results
      if (_selectedCategories.isNotEmpty) {
        response =
            response.where((job) {
              final jobCategory = job['category'] ?? '';
              return _selectedCategories.contains(jobCategory);
            }).toList();
      }

      // Convert and process jobs (empty list is valid, will show empty state)
      final jobs = List<Map<String, dynamic>>.from(response);

      // Mark jobs that are saved or applied
      for (final job in jobs) {
        final jobId = job['id']?.toString();
        if (jobId != null) {
          job['is_saved'] = _savedJobIds.contains(jobId);
          job['is_applied'] = _appliedJobIds.contains(jobId);
        }

        // Check if this job is part of a batch that has been applied to
        final batchId = job['batch_id']?.toString();
        if (batchId != null &&
            batchId.isNotEmpty &&
            _appliedBatchIds != null &&
            _appliedBatchIds.contains(batchId)) {
          job['batch_applied'] = true;
        }
      }

      // Calculate distance for each job if we have current pincode
      if (_currentPincode.isNotEmpty) {
        debugPrint('Calculating distances based on pincode: $_currentPincode');
        _jobDistances.clear();

        for (final job in jobs) {
          final jobId = job['id']?.toString();
          final jobPincode = job['job_pincode']?.toString();

          if (jobId != null) {
            final distance = _calculateDistance(jobPincode);
            _jobDistances[jobId] = distance;
            job['distance'] =
                distance; // Add distance to job object for easier access
          }
        }

        // Log the number of jobs with distance info
        final jobsWithDistance =
            jobs.where((job) => job['distance'] != null).length;
        debugPrint('Jobs with distance info: $jobsWithDistance/${jobs.length}');
      }

      // Group jobs by batch_id for easier debugging
      final batchGroups = <String, List<Map<String, dynamic>>>{};
      for (final job in jobs) {
        final batchId = job['batch_id']?.toString();
        if (batchId != null && batchId.isNotEmpty) {
          batchGroups[batchId] ??= [];
          batchGroups[batchId]!.add(job);
        }
      }

      // Log batch information for debugging
      if (batchGroups.isNotEmpty) {
        debugPrint('Found ${batchGroups.length} job batches:');
        batchGroups.forEach((batchId, batchJobs) {
          debugPrint('Batch $batchId: ${batchJobs.length} positions');
          for (final job in batchJobs) {
            debugPrint(
              '  - ${job['job_title']}, Position #${job['position_number']}',
            );
          }
        });
      }

      // Log successful fetch
      debugPrint('Successfully fetched ${jobs.length} jobs');
      setState(() {
        _allJobs = jobs;
        _filteredJobs = List.from(_allJobs);

        // Recommendation logic

        // Clear any previous error messages since fetch was successful
        _errorMessage = '';

        _isLoading = false;

        // If sort by distance is selected, apply it immediately
        if (_sortBy == 'Distance (Closest)') {
          _sortJobs('Distance (Closest)');
        }
      });
    } on PostgrestException catch (e) {
      // Handle Supabase-specific errors
      setState(() {
        _errorMessage = 'Database error: ${e.message}';
        _isLoading = false;
      });
      debugPrint('Supabase Error: $e');
    } catch (e) {
      // Comprehensive error handling
      _handleGenericError(e);
    }
  }

  void _handleNoInternet() {
    setState(() {
      _errorMessage = 'No internet connection. Please check your network.';
      _isLoading = false;
    });
  }

  void _handleSupabaseError(dynamic error) {
    String errorMessage = 'Supabase error occurred.';

    if (error is PostgrestException) {
      errorMessage = 'Database error: ${error.message}';
    }

    setState(() {
      _errorMessage = errorMessage;
      _isLoading = false;
    });

    debugPrint('Supabase Error: $error');
  }

  // Add this variable to your class
  // Updated applyToJob method with proper batch handling
  bool _isApplying = false;

  Future<void> applyToJob(Map<String, dynamic> jobDetails) async {
    // Prevent multiple simultaneous applications
    if (_isApplying) return;

    setState(() {
      _isApplying = true;
    });

    try {
      // 1. Check if user is authenticated
      final supabase = Supabase.instance.client;
      final user = supabase.auth.currentUser;

      if (user == null) {
        _showAuthRequiredDialog();
        setState(() {
          _isApplying = false;
        });
        return;
      }

      // Safely handle nullable email
      final userEmail = user.email ?? '';
      if (userEmail.isEmpty) {
        _showAuthRequiredDialog();
        setState(() {
          _isApplying = false;
        });
        return;
      }
      bool isKycVerified = await _checkKycStatus();
      if (!isKycVerified) {
        _showKycRequiredDialog();
        setState(() {
          _isApplying = false;
        });
        return;
      }

      final jobId = jobDetails['id']?.toString();
      final jobTitle = jobDetails['job_title']?.toString() ?? '';
      final company = jobDetails['company']?.toString() ?? '';
      final employer_id = jobDetails['user_id']?.toString();

      // Critical fields for batch handling
      final shiftId = jobDetails['shift_id']?.toString();
      final batchId = jobDetails['batch_id']?.toString();
      final positionNumber = jobDetails['position_number'];

      // Job date and time details
      final jobDate = jobDetails['date'];
      final jobStartTime = jobDetails['start_time'];
      final jobEndTime = jobDetails['end_time'];

      if (jobId == null) {
        _showErrorMessage('Invalid job details');
        setState(() {
          _isApplying = false;
        });
        return;
      }

      // 3. Check for conflicting shifts
      // First, define the _isDateTimeOverlap function

      // Parse the job's datetime fields - add this before checking for conflicts
      DateTime startDateTime;
      DateTime endDateTime;
      DateTime adjustedEndDateTime;

      // Initialize these variables properly
      if (jobDetails['start_date_time'] != null &&
          jobDetails['end_date_time'] != null) {
        // If the job already has datetime fields, use them directly
        startDateTime = DateTime.parse(jobDetails['start_date_time']);
        endDateTime = DateTime.parse(jobDetails['end_date_time']);
      } else {
        // Otherwise, construct datetime objects from separate date and time fields
        final jobDate = DateTime.parse(jobDetails['date']);

        // Parse time strings to get hours and minutes
        final startTimeParts = jobStartTime.split(':');
        final endTimeParts = jobEndTime.split(':');

        // Create datetime objects
        startDateTime = DateTime(
          jobDate.year,
          jobDate.month,
          jobDate.day,
          int.parse(startTimeParts[0]),
          int.parse(startTimeParts[1]),
        );

        endDateTime = DateTime(
          jobDate.year,
          jobDate.month,
          jobDate.day,
          int.parse(endTimeParts[0]),
          int.parse(endTimeParts[1]),
        );
      }

      // Handle overnight shifts (if end time is earlier than start time)
      adjustedEndDateTime =
          endDateTime.isBefore(startDateTime)
              ? endDateTime.add(Duration(days: 1))
              : endDateTime;

      // Your existing overlap checking function
      bool _isDateTimeOverlap(
        DateTime newStart,
        DateTime newEnd,
        DateTime existingStart,
        DateTime existingEnd,
      ) {
        return (newStart.isAfter(existingStart) &&
                newStart.isBefore(existingEnd)) ||
            (newEnd.isAfter(existingStart) && newEnd.isBefore(existingEnd)) ||
            (newStart.isBefore(existingStart) && newEnd.isAfter(existingEnd)) ||
            (existingStart.isAfter(newStart) &&
                existingStart.isBefore(newEnd)) ||
            newStart.isAtSameMomentAs(existingStart) ||
            newEnd.isAtSameMomentAs(existingEnd);
      }

      if (shiftId != null) {
        try {
          // First check if the job exists in saved jobs
          final savedJobResult = await supabase
              .from('worker_saved_jobs')
              .select('id')
              .eq('shift_id', shiftId);

          // If found, delete it
          if (savedJobResult != null && savedJobResult.isNotEmpty) {
            await supabase
                .from('worker_saved_jobs')
                .delete()
                .eq('user_id', user.id)
                .eq('shift_id', shiftId);

            debugPrint('Removed job with shift_id: $shiftId from saved jobs');
          }
        } catch (e) {
          // Just log the error, don't interrupt the application process
          debugPrint('Error removing job from saved jobs: $e');
        }
      }
      // Now check for conflicts
      final conflictingJobsResponse = await supabase
          .from('worker_job_applications')
          .select(
            'job_title, date, start_time, end_time, start_date_time, end_date_time',
          )
          .eq('user_id', user.id)
          .eq('date', jobDate)
          .filter('application_status', 'in', '(Upcoming,Ongoing)');

      bool hasConflictingShift = conflictingJobsResponse.any((existingJob) {
        if (existingJob['start_date_time'] != null &&
            existingJob['end_date_time'] != null) {
          final existingStartDateTime = DateTime.parse(
            existingJob['start_date_time'],
          );
          final existingEndDateTime = DateTime.parse(
            existingJob['end_date_time'],
          );

          // Now we can use the previously defined variables
          return _isDateTimeOverlap(
            startDateTime,
            adjustedEndDateTime,
            existingStartDateTime,
            existingEndDateTime,
          );
        } else {
          final existingStartTime = existingJob['start_time'];
          final existingEndTime = existingJob['end_time'];

          return _isTimeOverlap(
            jobStartTime,
            jobEndTime,
            existingStartTime,
            existingEndTime,
          );
        }
      });
      if (hasConflictingShift) {
        // Show conflict dialog
        _showShiftConflictDialog(jobTitle, jobDate, jobStartTime, jobEndTime);
        setState(() {
          _isApplying = false;
        });
        return;
      }

      // 2. Extract job details for application checks
      if (jobId == null) {
        _showErrorMessage('Invalid job details');
        setState(() {
          _isApplying = false;
        });
        return;
      }

      // 3. Check if user has already applied to this specific job ID
      final existingByJobId = await supabase
          .from('worker_job_applications')
          .select('id')
          .eq('user_id', user.id)
          .eq('job_id', jobId);

      if (existingByJobId != null && existingByJobId.isNotEmpty) {
        _showAlreadyAppliedDialog(jobTitle);
        setState(() {
          _isApplying = false;
        });
        return;
      }

      // 4. Check if user has applied to any position in the same batch
      if (batchId != null && batchId.isNotEmpty) {
        final batchApplications = await supabase
            .from('worker_job_applications')
            .select('id, position_number, shift_id')
            .eq('user_id', user.id)
            .eq('batch_id', batchId);

        if (batchApplications != null && batchApplications.isNotEmpty) {
          // Show a more informative dialog about the batch application
          final appliedPositionNumber = batchApplications[0]['position_number'];
          final appliedShiftId = batchApplications[0]['shift_id'];

          _showAlreadyAppliedToBatchDialog(
            jobTitle,
            batchId,
            appliedPositionNumber,
            appliedShiftId,
          );

          setState(() {
            _isApplying = false;
          });
          return;
        }
      }

      // 5. Check if this specific position is already taken by ANYONE else
      final existingPositionResponse = await supabase
          .from('worker_job_applications')
          .select('id')
          .eq('shift_id', shiftId!)
          .eq('batch_id', batchId!)
          .eq('position_number', positionNumber);

      if (existingPositionResponse.isNotEmpty) {
        _showErrorMessage(
          'This position has already been filled by another user',
        );
        setState(() {
          _isApplying = false;
        });
        return;
      }

      // 6. Fetch job seeker details and ratings
      // 6. Fetch job seeker details
      final jobSeekerResponse =
          await supabase
              .from('job_seekers')
              .select('full_name, phone, location')
              .eq('email', userEmail)
              .single();

      debugPrint('Job seeker data: ${jobSeekerResponse.toString()}');

      // Calculate worker's average rating by fetching attendance records
      double workerRating = 0.0;
      try {
        // Get application IDs for this worker
        final applications = await supabase
            .from('worker_job_applications')
            .select('id')
            .eq('email', userEmail);

        if (applications != null && applications.isNotEmpty) {
          // Extract application IDs
          List<String> applicationIds =
              applications.map<String>((app) => app['id'].toString()).toList();

          // Collect all attendance records
          List<Map<String, dynamic>> allAttendanceRecords = [];
          for (String appId in applicationIds) {
            final attendanceForApp = await supabase
                .from('worker_attendance')
                .select('performance_rating')
                .eq('application_id', appId);

            if (attendanceForApp != null && attendanceForApp.isNotEmpty) {
              allAttendanceRecords.addAll(attendanceForApp);
            }
          }

          // Calculate average performance rating
          if (allAttendanceRecords.isNotEmpty) {
            double performanceRatingSum = 0;
            int validPerformanceRatings = 0;

            for (var record in allAttendanceRecords) {
              if (record['performance_rating'] != null) {
                performanceRatingSum += record['performance_rating'];
                validPerformanceRatings++;
              }
            }

            if (validPerformanceRatings > 0) {
              workerRating = performanceRatingSum / validPerformanceRatings;
            }
          }
        }
      } catch (e) {
        debugPrint('Error calculating worker rating: $e');
        // Use default rating of 0.0 if there's an error
      }

      debugPrint('Worker average rating: $workerRating');

      debugPrint('Worker average rating: $workerRating');

      // 7. Prepare application data - including worker_rating
      final applicationData = {
        'job_id': jobId,
        'employer_id': employer_id,
        'user_id': user.id,
        'job_title': jobTitle,
        'company': company,
        'location': jobDetails['location'],
        'start_date_time': jobDetails['start_date_time'],
        'end_date_time': jobDetails['end_date_time'],
        'application_date': DateTime.now().toIso8601String(),
        'application_status': 'Upcoming',
        'match_percentage': jobDetails['match_percentage'] ?? 0,
        'date': jobDetails['date'],
        'start_time': jobDetails['start_time'],
        'end_time': jobDetails['end_time'],
        'pay_rate': jobDetails['pay_rate'],
        'pay_currency': jobDetails['pay_currency'],

        // Include the worker's average rating
        'worker_rating': workerRating.toInt(),

        // Include all batch-related fields
        'shift_id': shiftId,
        'batch_id': batchId,
        'position_number': positionNumber,

        // User details from job_seekers table
        'full_name': jobSeekerResponse['full_name'],
        'phone_number': jobSeekerResponse['phone'],
        'email': userEmail,
      };

      // 8. Insert application to Supabase
      await supabase.from('worker_job_applications').insert(applicationData);
      setState(() {
        // Track applied shift using shift_id + position_number
        if (shiftId != null && positionNumber != null) {
          _appliedJobIds.add('$shiftId|$positionNumber');
        }

        // Track batch ID if available
        if (batchId != null && batchId.isNotEmpty) {
          _appliedBatchIds ??= {};
          _appliedBatchIds.add(batchId);
        }

        // Update UI state for job lists
        for (final jobList in [_allJobs, _filteredJobs, _recommendedJobs]) {
          for (final job in jobList) {
            if (job['id']?.toString() == jobId) {
              job['is_applied'] = true;
            }

            if (batchId != null && job['batch_id']?.toString() == batchId) {
              job['batch_applied'] = true;
            }
          }
        }
      });
      await sendUserNotification(
        userId: employer_id!,
        title: 'Worker Assigned',
        message: 'A worker has been assigned to Shift $shiftId',
        shiftId: shiftId!,
        userType: 'employer',
        notificationType: 'worker_assigned',
      );

      await sendUserNotification(
        userId: user.id,
        title: 'Shift Assigned',
        message: 'You have been assigned to Shift $shiftId',
        shiftId: shiftId,
        userType: 'worker',
        notificationType: 'assigned',
      );

      // 10. Handle successful application
      _showSuccessDialog(jobTitle, shiftId, positionNumber, batchId);

      // 11. Refresh applied jobs list
      _fetchAppliedJobs();
    } catch (e) {
      // Check for duplicate key error
      if (e.toString().contains(
            'duplicate key value violates unique constraint',
          ) ||
          e.toString().contains('unique constraint')) {
        _showAlreadyAppliedDialog(jobDetails['job_title'] ?? 'this job');
      } else {
        _handleApplicationError(e);
      }
    } finally {
      setState(() {
        _isApplying = false;
      });
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

  Future<void> sendUserNotification({
    required String userId,
    required String title,
    required String message,
    required String shiftId,
    required String userType, // 'employer' or 'worker'
    String type = 'info',
    String notificationType = 'applied',
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

  Future<void> applyToSavedJob(Map<String, dynamic> jobDetails) async {
    // Prevent multiple simultaneous applications
    if (_isApplying) return;
    // Get job_id (check both id and job_id fields)
    // Get job_id (check both id and job_id fields)

    setState(() {
      _isApplying = true;
    });

    try {
      // 1. Check if user is authenticated
      final supabase = Supabase.instance.client;
      final user = supabase.auth.currentUser;

      if (user == null) {
        _showAuthRequiredDialog();
        setState(() {
          _isApplying = false;
        });
        return;
      }

      // Safely handle nullable email
      final userEmail = user.email ?? '';
      if (userEmail.isEmpty) {
        _showAuthRequiredDialog();
        setState(() {
          _isApplying = false;
        });
        return;
      }

      final jobId = jobDetails['job_id']?.toString();
      final jobTitle = jobDetails['job_title']?.toString() ?? '';
      final company = jobDetails['company']?.toString() ?? '';
      final employer_id = jobDetails['employer_id']?.toString() ?? '';

      // Critical fields for batch handling
      final shiftId = jobDetails['shift_id']?.toString();
      final batchId = jobDetails['batch_id']?.toString();
      final positionNumber = jobDetails['position_number'];

      // Job date and time details
      final jobDate = jobDetails['date'];
      final jobStartTime = jobDetails['start_time'];
      final jobEndTime = jobDetails['end_time'];

      if (jobId == null) {
        _showErrorMessage('Invalid job details');
        setState(() {
          _isApplying = false;
        });
        return;
      }

      // 3. Check for conflicting shifts
      // First, define the _isDateTimeOverlap function

      // Parse the job's datetime fields - add this before checking for conflicts
      DateTime startDateTime;
      DateTime endDateTime;
      DateTime adjustedEndDateTime;

      // Initialize these variables properly
      if (jobDetails['start_date_time'] != null &&
          jobDetails['end_date_time'] != null) {
        // If the job already has datetime fields, use them directly
        startDateTime = DateTime.parse(jobDetails['start_date_time']);
        endDateTime = DateTime.parse(jobDetails['end_date_time']);
      } else {
        // Otherwise, construct datetime objects from separate date and time fields
        final jobDate = DateTime.parse(jobDetails['date']);

        // Parse time strings to get hours and minutes
        final startTimeParts = jobStartTime.split(':');
        final endTimeParts = jobEndTime.split(':');

        // Create datetime objects
        startDateTime = DateTime(
          jobDate.year,
          jobDate.month,
          jobDate.day,
          int.parse(startTimeParts[0]),
          int.parse(startTimeParts[1]),
        );

        endDateTime = DateTime(
          jobDate.year,
          jobDate.month,
          jobDate.day,
          int.parse(endTimeParts[0]),
          int.parse(endTimeParts[1]),
        );
      }

      // Handle overnight shifts (if end time is earlier than start time)
      adjustedEndDateTime =
          endDateTime.isBefore(startDateTime)
              ? endDateTime.add(Duration(days: 1))
              : endDateTime;

      // Your existing overlap checking function
      bool _isDateTimeOverlap(
        DateTime newStart,
        DateTime newEnd,
        DateTime existingStart,
        DateTime existingEnd,
      ) {
        return (newStart.isAfter(existingStart) &&
                newStart.isBefore(existingEnd)) ||
            (newEnd.isAfter(existingStart) && newEnd.isBefore(existingEnd)) ||
            (newStart.isBefore(existingStart) && newEnd.isAfter(existingEnd)) ||
            (existingStart.isAfter(newStart) &&
                existingStart.isBefore(newEnd)) ||
            newStart.isAtSameMomentAs(existingStart) ||
            newEnd.isAtSameMomentAs(existingEnd);
      }

      // Now check for conflicts
      final conflictingJobsResponse = await supabase
          .from('worker_job_applications')
          .select(
            'job_title, date, start_time, end_time, start_date_time, end_date_time',
          )
          .eq('user_id', user.id)
          .eq('date', jobDate)
          .filter('application_status', 'in', '(Upcoming,Ongoing)');

      bool hasConflictingShift = conflictingJobsResponse.any((existingJob) {
        if (existingJob['start_date_time'] != null &&
            existingJob['end_date_time'] != null) {
          final existingStartDateTime = DateTime.parse(
            existingJob['start_date_time'],
          );
          final existingEndDateTime = DateTime.parse(
            existingJob['end_date_time'],
          );

          // Now we can use the previously defined variables
          return _isDateTimeOverlap(
            startDateTime,
            adjustedEndDateTime,
            existingStartDateTime,
            existingEndDateTime,
          );
        } else {
          final existingStartTime = existingJob['start_time'];
          final existingEndTime = existingJob['end_time'];

          return _isTimeOverlap(
            jobStartTime,
            jobEndTime,
            existingStartTime,
            existingEndTime,
          );
        }
      });
      if (hasConflictingShift) {
        // Show conflict dialog
        _showShiftConflictDialog(jobTitle, jobDate, jobStartTime, jobEndTime);
        setState(() {
          _isApplying = false;
        });
        return;
      }

      // 2. Extract job details for application checks
      if (jobId == null) {
        _showErrorMessage('Invalid job details');
        setState(() {
          _isApplying = false;
        });
        return;
      }

      // 3. Check if user has already applied to this specific job ID
      final existingByJobId = await supabase
          .from('worker_job_applications')
          .select('id')
          .eq('user_id', user.id)
          .eq('job_id', jobId);

      if (existingByJobId != null && existingByJobId.isNotEmpty) {
        _showAlreadyAppliedDialog(jobTitle);
        setState(() {
          _isApplying = false;
        });
        return;
      }

      // 4. Check if user has applied to any position in the same batch
      if (batchId != null && batchId.isNotEmpty) {
        final batchApplications = await supabase
            .from('worker_job_applications')
            .select('id, position_number, shift_id')
            .eq('user_id', user.id)
            .eq('batch_id', batchId);

        if (batchApplications != null && batchApplications.isNotEmpty) {
          // Show a more informative dialog about the batch application
          final appliedPositionNumber = batchApplications[0]['position_number'];
          final appliedShiftId = batchApplications[0]['shift_id'];

          _showAlreadyAppliedToBatchDialog(
            jobTitle,
            batchId,
            appliedPositionNumber,
            appliedShiftId,
          );

          setState(() {
            _isApplying = false;
          });
          return;
        }
      }

      // 5. Check if this specific position is already taken by ANYONE else
      final existingPositionResponse = await supabase
          .from('worker_job_applications')
          .select('id')
          .eq('shift_id', shiftId!)
          .eq('batch_id', batchId!)
          .eq('position_number', positionNumber);

      if (existingPositionResponse.isNotEmpty) {
        _showErrorMessage(
          'This position has already been filled by another user',
        );
        setState(() {
          _isApplying = false;
        });
        return;
      }
      if (shiftId != null) {
        try {
          // First check if the job exists in saved jobs
          final savedJobResult = await supabase
              .from('worker_saved_jobs')
              .select('id')
              .eq('shift_id', shiftId);

          // If found, delete it
          if (savedJobResult != null && savedJobResult.isNotEmpty) {
            await supabase
                .from('worker_saved_jobs')
                .delete()
                .eq('user_id', user.id)
                .eq('shift_id', shiftId);

            debugPrint('Removed job with shift_id: $shiftId from saved jobs');
          }
        } catch (e) {
          // Just log the error, don't interrupt the application process
          debugPrint('Error removing job from saved jobs: $e');
        }
      }
      final jobSeekerResponse =
          await supabase
              .from('job_seekers')
              .select('full_name, phone, location')
              .eq('email', userEmail)
              .single();

      debugPrint('Job seeker data: ${jobSeekerResponse.toString()}');

      // Calculate worker's average rating by fetching attendance records
      double workerRating = 0.0;
      try {
        // Get application IDs for this worker
        final applications = await supabase
            .from('worker_job_applications')
            .select('id')
            .eq('email', userEmail);

        if (applications != null && applications.isNotEmpty) {
          // Extract application IDs
          List<String> applicationIds =
              applications.map<String>((app) => app['id'].toString()).toList();

          // Collect all attendance records
          List<Map<String, dynamic>> allAttendanceRecords = [];
          for (String appId in applicationIds) {
            final attendanceForApp = await supabase
                .from('worker_attendance')
                .select('performance_rating')
                .eq('application_id', appId);

            if (attendanceForApp != null && attendanceForApp.isNotEmpty) {
              allAttendanceRecords.addAll(attendanceForApp);
            }
          }

          // Calculate average performance rating
          if (allAttendanceRecords.isNotEmpty) {
            double performanceRatingSum = 0;
            int validPerformanceRatings = 0;

            for (var record in allAttendanceRecords) {
              if (record['performance_rating'] != null) {
                performanceRatingSum += record['performance_rating'];
                validPerformanceRatings++;
              }
            }

            if (validPerformanceRatings > 0) {
              workerRating = performanceRatingSum / validPerformanceRatings;
            }
          }
        }
      } catch (e) {
        debugPrint('Error calculating worker rating: $e');
        // Use default rating of 0.0 if there's an error
      }

      debugPrint('Worker average rating: $workerRating');

      debugPrint('Worker average rating: $workerRating');

      // 7. Prepare application data - including worker_rating
      final applicationData = {
        'job_id': jobId,
        'user_id': user.id,
        'employer_id': employer_id,
        'job_title': jobTitle,
        'company': company,
        'location': jobDetails['location'],
        'start_date_time': jobDetails['start_date_time'],
        'end_date_time': jobDetails['end_date_time'],
        'application_date': DateTime.now().toIso8601String(),
        'application_status': 'Upcoming',
        'match_percentage': jobDetails['match_percentage'] ?? 0,
        'date': jobDetails['date'],
        'start_time': jobDetails['start_time'],
        'end_time': jobDetails['end_time'],
        'pay_rate': jobDetails['pay_rate'],
        'pay_currency': jobDetails['pay_currency'],

        // Include the worker's average rating
        'worker_rating': workerRating.toInt(),

        // Include all batch-related fields
        'shift_id': shiftId,
        'batch_id': batchId,
        'position_number': positionNumber,

        // User details from job_seekers table
        'full_name': jobSeekerResponse['full_name'],
        'phone_number': jobSeekerResponse['phone'],
        'email': userEmail,
      };

      // 8. Insert application to Supabase
      await supabase.from('worker_job_applications').insert(applicationData);

      // 9. Update tracking mechanisms
      setState(() {
        // Track by ID
        if (jobId != null) {
          _appliedJobIds.add(jobId);
        }

        // Track batch ID if it exists
        if (batchId != null && batchId.isNotEmpty) {
          _appliedBatchIds ??= {};
          _appliedBatchIds.add(batchId);
        }

        // Update UI state for job lists
        // First, update the specific job that was applied to
        for (final jobList in [_allJobs, _filteredJobs, _recommendedJobs]) {
          for (final job in jobList) {
            if (job['id']?.toString() == jobId) {
              job['is_applied'] = true;
            }

            // If this job is part of the same batch, mark it as batch_applied
            final jobBatchId = job['batch_id']?.toString();
            if (batchId != null &&
                batchId.isNotEmpty &&
                jobBatchId != null &&
                jobBatchId == batchId) {
              job['batch_applied'] = true;
            }
          }
        }
      });

      // 10. Handle successful application
      _showSuccessDialog(jobTitle, shiftId, positionNumber, batchId);

      // 11. Refresh applied jobs list
      _fetchAppliedJobs();
    } catch (e) {
      // Check for duplicate key error
      if (e.toString().contains(
            'duplicate key value violates unique constraint',
          ) ||
          e.toString().contains('unique constraint')) {
        _showAlreadyAppliedDialog(jobDetails['job_title'] ?? 'this job');
      } else {
        _handleApplicationError(e);
      }
    } finally {
      setState(() {
        _isApplying = false;
      });
    }
  }

  // Helper method to check time overlap
  bool _isTimeOverlap(
    String newStartTime,
    String newEndTime,
    String existingStartTime,
    String existingEndTime,
  ) {
    // Convert times to minutes since midnight
    int newStartMinutes = _timeToMinutes(newStartTime);
    int newEndMinutes = _timeToMinutes(newEndTime);
    int existingStartMinutes = _timeToMinutes(existingStartTime);
    int existingEndMinutes = _timeToMinutes(existingEndTime);

    // Check for overlap
    return (newStartMinutes < existingEndMinutes &&
        newEndMinutes > existingStartMinutes);
  }

  // Helper method to convert time string to minutes
  int _timeToMinutes(String timeStr) {
    final parts = timeStr.split(':');
    if (parts.length < 2) return 0;

    final hours = int.parse(parts[0]);
    final minutes = int.parse(parts[1]);

    return hours * 60 + minutes;
  }

  void _showShiftConflictDialog(
    String jobTitle,
    String jobDate,
    String jobStartTime,
    String jobEndTime,
  ) {
    // Format start and end times to AM/PM
    final formattedStartTime = _formatTimeToAmPm(jobStartTime);
    final formattedEndTime = _formatTimeToAmPm(jobEndTime);

    showDialog(
      context: context,
      builder:
          (context) => Dialog(
            shape: RoundedRectangleBorder(
              borderRadius: BorderRadius.circular(16),
            ),
            child: Padding(
              padding: const EdgeInsets.all(16),
              child: Column(
                mainAxisSize: MainAxisSize.min,
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  // Dialog Title
                  Row(
                    children: [
                      Icon(Icons.warning, color: Colors.orange, size: 24),
                      SizedBox(width: 8),
                      Text(
                        'Shift Conflict',
                        style: TextStyle(
                          fontSize: 20,
                          fontWeight: FontWeight.bold,
                        ),
                      ),
                    ],
                  ),
                  SizedBox(height: 16),

                  // Conflict Message
                  RichText(
                    text: TextSpan(
                      style: TextStyle(color: Colors.black, fontSize: 16),
                      children: [
                        TextSpan(
                          text: 'You already have a shift scheduled on ',
                        ),
                        TextSpan(
                          text: jobDate,
                          style: TextStyle(fontWeight: FontWeight.bold),
                        ),
                        TextSpan(text: ' from '),
                        TextSpan(
                          text: formattedStartTime,
                          style: TextStyle(fontWeight: FontWeight.bold),
                        ),
                        TextSpan(text: ' to '),
                        TextSpan(
                          text: formattedEndTime,
                          style: TextStyle(fontWeight: FontWeight.bold),
                        ),
                        TextSpan(text: '.'),
                      ],
                    ),
                  ),
                  SizedBox(height: 16),

                  // Action Buttons
                  Row(
                    mainAxisAlignment: MainAxisAlignment.end,
                    children: [
                      TextButton(
                        onPressed: () => Navigator.of(context).pop(),
                        child: Text('Cancel'),
                      ),
                      SizedBox(width: 8),
                      ElevatedButton(
                        onPressed: () {
                          Navigator.of(context).pop();
                          // Navigate to job search screen or perform any desired action
                        },
                        style: ElevatedButton.styleFrom(
                          foregroundColor: Colors.white,
                          backgroundColor: Colors.blue,
                          shape: RoundedRectangleBorder(
                            borderRadius: BorderRadius.circular(8),
                          ),
                        ),
                        child: Text('Find Another Shift'),
                      ),
                    ],
                  ),
                ],
              ),
            ),
          ),
    );
  }

  // Helper method to format time to AM/PM
  String _formatTimeToAmPm(String time) {
    final hour = int.parse(time.split(':')[0]);
    final minute = int.parse(time.split(':')[1]);

    final period = hour >= 12 ? 'PM' : 'AM';
    final formattedHour = hour % 12 == 0 ? 12 : hour % 12;
    final formattedMinute = minute.toString().padLeft(2, '0');

    return '$formattedHour:$formattedMinute $period';
  }

  void _showSuccessDialog(
    String jobTitle,
    String? shiftId,
    dynamic positionNumber,
    String? batchId,
  ) {
    showDialog(
      context: context,
      builder:
          (context) => Dialog(
            shape: RoundedRectangleBorder(
              borderRadius: BorderRadius.circular(20),
            ),
            child: Container(
              padding: const EdgeInsets.all(24),
              decoration: BoxDecoration(
                color: Colors.white,
                borderRadius: BorderRadius.circular(20),
                boxShadow: [
                  BoxShadow(
                    color: Colors.black.withOpacity(0.1),
                    blurRadius: 15,
                    offset: const Offset(0, 5),
                  ),
                ],
              ),
              child: Column(
                mainAxisSize: MainAxisSize.min,
                children: [
                  // Success Animation Container
                  Container(
                    width: 100,
                    height: 100,
                    decoration: BoxDecoration(
                      color: Colors.green.shade50,
                      shape: BoxShape.circle,
                    ),
                    child: Center(
                      child: Icon(
                        Icons.check_circle,
                        color: Colors.green.shade600,
                        size: 72,
                      ),
                    ),
                  ),
                  const SizedBox(height: 24),

                  // Title
                  Text(
                    'Application Successful',
                    style: TextStyle(
                      fontSize: 22,
                      fontWeight: FontWeight.bold,
                      color: Colors.green.shade800,
                    ),
                    textAlign: TextAlign.center,
                  ),
                  const SizedBox(height: 16),

                  // Job Details
                  RichText(
                    textAlign: TextAlign.center,
                    text: TextSpan(
                      style: TextStyle(
                        color: Colors.grey.shade700,
                        fontSize: 16,
                      ),
                      children: [
                        const TextSpan(
                          text: 'You have successfully applied for ',
                        ),
                        TextSpan(
                          text: '"$jobTitle"',
                          style: TextStyle(
                            fontWeight: FontWeight.bold,
                            color: Colors.green.shade700,
                          ),
                        ),
                        const TextSpan(text: ' shift.'),
                      ],
                    ),
                  ),
                  const SizedBox(height: 24), // Action Buttons
                  Row(
                    children: [
                      Expanded(
                        child: ElevatedButton(
                          onPressed: () => Navigator.pop(context),
                          style: ElevatedButton.styleFrom(
                            backgroundColor: Colors.green.shade600,
                            foregroundColor: Colors.white,
                            padding: const EdgeInsets.symmetric(vertical: 12),
                            shape: RoundedRectangleBorder(
                              borderRadius: BorderRadius.circular(12),
                            ),
                          ),
                          child: const Text(
                            'OK, Got It',
                            style: TextStyle(
                              fontSize: 16,
                              fontWeight: FontWeight.bold,
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
    );
  }

  // Helper method to build detail rows
  Widget _buildDetailRow(String label, String value) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 4),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.spaceBetween,
        children: [
          Text(
            label,
            style: TextStyle(
              color: Colors.grey.shade700,
              fontWeight: FontWeight.w600,
            ),
          ),
          Text(
            value,
            style: TextStyle(
              color: Colors.green.shade700,
              fontWeight: FontWeight.bold,
            ),
          ),
        ],
      ),
    );
  }

  void _showAlreadyAppliedToBatchDialog(
    String jobTitle,
    String batchId,
    dynamic appliedPositionNumber,
    String? appliedShiftId,
  ) {
    showDialog(
      context: context,
      builder:
          (context) => Dialog(
            shape: RoundedRectangleBorder(
              borderRadius: BorderRadius.circular(20),
            ),
            child: Container(
              padding: const EdgeInsets.all(24),
              decoration: BoxDecoration(
                color: Colors.white,
                borderRadius: BorderRadius.circular(20),
                boxShadow: [
                  BoxShadow(
                    color: Colors.black.withOpacity(0.1),
                    blurRadius: 15,
                    offset: const Offset(0, 5),
                  ),
                ],
              ),
              child: Column(
                mainAxisSize: MainAxisSize.min,
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  // Warning Icon Container
                  Container(
                    width: 100,
                    height: 100,
                    decoration: BoxDecoration(
                      color: Colors.orange.shade50,
                      shape: BoxShape.circle,
                    ),
                    child: Center(
                      child: Icon(
                        Icons.warning_amber_rounded,
                        color: Colors.orange.shade600,
                        size: 72,
                      ),
                    ),
                  ),
                  const SizedBox(height: 24),

                  // Title
                  Text(
                    'Position Already Applied',
                    style: TextStyle(
                      fontSize: 22,
                      fontWeight: FontWeight.bold,
                      color: Colors.orange.shade800,
                    ),
                    textAlign: TextAlign.center,
                  ),
                  const SizedBox(height: 16),

                  // Detailed Message
                  RichText(
                    textAlign: TextAlign.center,
                    text: TextSpan(
                      style: TextStyle(
                        color: Colors.grey.shade700,
                        fontSize: 16,
                      ),
                      children: [
                        const TextSpan(
                          text:
                              'You have already applied to another position in the job batch for ',
                        ),
                        TextSpan(
                          text: '"$jobTitle"',
                          style: TextStyle(
                            fontWeight: FontWeight.bold,
                            color: Colors.orange.shade700,
                          ),
                        ),
                        const TextSpan(text: '.'),
                      ],
                    ),
                  ),
                  const SizedBox(height: 8),

                  // Application Details
                  Text(
                    'Applied Position: #$appliedPositionNumber${appliedShiftId != null ? ' (Shift ID: $appliedShiftId)' : ''}',
                    style: TextStyle(
                      color: Colors.grey.shade600,
                      fontSize: 14,
                      fontStyle: FontStyle.italic,
                    ),
                    textAlign: TextAlign.center,
                  ),
                  const SizedBox(height: 16),

                  // Restriction Message
                  Text(
                    'You cannot apply to multiple positions in the same job batch.',
                    style: TextStyle(
                      color: Colors.red.shade600,
                      fontSize: 14,
                      fontWeight: FontWeight.bold,
                    ),
                    textAlign: TextAlign.center,
                  ),
                  const SizedBox(height: 24),

                  // Action Buttons
                  Row(
                    children: [
                      // Dismiss Button
                      Expanded(
                        child: OutlinedButton(
                          onPressed: () => Navigator.pop(context),
                          style: OutlinedButton.styleFrom(
                            foregroundColor: Colors.grey.shade600,
                            side: BorderSide(color: Colors.grey.shade300),
                            padding: const EdgeInsets.symmetric(vertical: 12),
                            shape: RoundedRectangleBorder(
                              borderRadius: BorderRadius.circular(12),
                            ),
                          ),
                          child: const Text(
                            'Dismiss',
                            style: TextStyle(
                              fontSize: 16,
                              fontWeight: FontWeight.bold,
                            ),
                          ),
                        ),
                      ),
                      const SizedBox(width: 16),

                      // View Applications Button
                      Expanded(
                        child: ElevatedButton(
                          onPressed: () {
                            Navigator.pop(context);
                            _tabController.animateTo(3); // Go to Applied tab
                          },
                          style: ElevatedButton.styleFrom(
                            backgroundColor: Colors.blue.shade600,
                            foregroundColor: Colors.white,
                            padding: const EdgeInsets.symmetric(vertical: 12),
                            shape: RoundedRectangleBorder(
                              borderRadius: BorderRadius.circular(12),
                            ),
                          ),
                          child: const Text(
                            'View Applications',
                            style: TextStyle(
                              fontSize: 16,
                              fontWeight: FontWeight.bold,
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
    );
  } // Add this method to handle batch application conflicts

  // You'll need to also add this field to track applied batch IDs
  Set<String> _appliedBatchIds = {};
  void _showCategorySelectionBottomSheet() {
    // Ensure categories are populated
    if (_availableCategories.isEmpty) {
      _availableCategories = [
        'Hospitality',
        'Retail',
        'Food Service',
        'Customer Service',
        'Warehouse',
        'Office Admin',
        'Healthcare',
        'Event Staff',
        'Other',
      ];
    }

    // Create a temporary list to track selections
    List<String> tempSelectedCategories = List.from(_selectedCategories);

    showModalBottomSheet(
      context: context,
      isScrollControlled: true,
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.vertical(top: Radius.circular(20)),
      ),
      builder: (context) {
        return StatefulBuilder(
          builder: (BuildContext context, StateSetter setModalState) {
            return Container(
              padding: const EdgeInsets.all(24),
              child: Column(
                mainAxisSize: MainAxisSize.min,
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  // Header
                  Row(
                    mainAxisAlignment: MainAxisAlignment.spaceBetween,
                    children: [
                      const Text(
                        'Select Categories',
                        style: TextStyle(
                          fontSize: 20,
                          fontWeight: FontWeight.bold,
                        ),
                      ),
                      IconButton(
                        icon: const Icon(Icons.close),
                        onPressed: () => Navigator.of(context).pop(),
                      ),
                    ],
                  ),
                  const SizedBox(height: 16),

                  // Categories List
                  Wrap(
                    spacing: 8,
                    runSpacing: 8,
                    children:
                        _availableCategories.map((category) {
                          final isSelected = tempSelectedCategories.contains(
                            category,
                          );
                          return FilterChip(
                            label: Text(category),
                            selected: isSelected,
                            onSelected: (selected) {
                              setModalState(() {
                                if (selected) {
                                  tempSelectedCategories.add(category);
                                } else {
                                  tempSelectedCategories.remove(category);
                                }
                              });
                            },
                            backgroundColor: Colors.grey.shade100,
                            selectedColor: Colors.blue.withOpacity(0.2),
                            checkmarkColor: Colors.blue,
                            shape: RoundedRectangleBorder(
                              borderRadius: BorderRadius.circular(20),
                              side: BorderSide(
                                color:
                                    isSelected
                                        ? Colors.blue
                                        : Colors.grey.shade300,
                              ),
                            ),
                          );
                        }).toList(),
                  ),
                  const SizedBox(height: 24),

                  // Action Buttons
                  Row(
                    children: [
                      // Clear Button
                      Expanded(
                        child: OutlinedButton(
                          onPressed: () {
                            setModalState(() {
                              tempSelectedCategories.clear();
                            });
                          },
                          style: OutlinedButton.styleFrom(
                            padding: const EdgeInsets.symmetric(vertical: 12),
                            shape: RoundedRectangleBorder(
                              borderRadius: BorderRadius.circular(8),
                            ),
                          ),
                          child: const Text('Clear All'),
                        ),
                      ),
                      const SizedBox(width: 12),
                      // Apply Button
                      Expanded(
                        child: ElevatedButton(
                          onPressed: () {
                            // Update the state for ALL jobs
                            setState(() {
                              _selectedCategories = tempSelectedCategories;

                              // Apply category filtering to ALL jobs
                              _filterJobsByCategory();
                            });

                            Navigator.of(context).pop();
                          },
                          style: ElevatedButton.styleFrom(
                            backgroundColor: Colors.blue,
                            foregroundColor: Colors.white,
                            padding: const EdgeInsets.symmetric(vertical: 12),
                            shape: RoundedRectangleBorder(
                              borderRadius: BorderRadius.circular(8),
                            ),
                          ),
                          child: const Text('Apply'),
                        ),
                      ),
                    ],
                  ),
                ],
              ),
            );
          },
        );
      },
    );
  }

  void _filterJobsByCategory() {
    setState(() {
      if (_selectedCategories.isEmpty) {
        // If no categories selected, show all jobs
        _filteredJobs = List.from(_allJobs);
      } else {
        // Filter jobs based on selected categories
        _filteredJobs =
            _allJobs.where((job) {
              final jobCategory = (job['category'] ?? '').toString();
              return _selectedCategories.contains(jobCategory);
            }).toList();

        // Show message if no jobs match the selected categories
        if (_filteredJobs.isEmpty) {
          _showNoJobsInCategoryMessage();
        }
      }

      // Re-apply any existing search filter
      if (_searchController.text.isNotEmpty) {
        _filterJobs(_searchController.text);
      }
    });
  }

  void _showNoJobsInCategoryMessage() {
    final categoriesText =
        _selectedCategories.length == 1
            ? _selectedCategories.first
            : '${_selectedCategories.length} selected categories';

    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        content: Text('No Shifts found in $categoriesText'),
        action: SnackBarAction(
          label: 'Clear Filter',
          onPressed: () {
            setState(() {
              _selectedCategories = [];
              _filteredJobs = List.from(_allJobs);
              _filterJobs(_searchController.text);
            });
          },
        ),
      ),
    );
  }

  // Add this method for unauthenticated users
  void _showAuthRequiredDialog() {
    showDialog(
      context: context,
      builder:
          (context) => AlertDialog(
            title: const Text('Authentication Required'),
            content: const Text('Please log in to apply for jobs.'),
            actions: [
              TextButton(
                onPressed: () => Navigator.of(context).pop(),
                child: const Text('Cancel'),
              ),
              TextButton(
                onPressed: () {
                  Navigator.of(context).pop();
                  // Navigate to login page
                  // Navigator.of(context).pushNamed('/login');
                },
                child: const Text('Log In'),
              ),
            ],
          ),
    );
  }

  Future<void> _fetchAppliedJobs() async {
    debugPrint('Fetching applied jobs...');

    try {
      // Check internet connectivity
      try {
        final result = await InternetAddress.lookup('google.com');
        if (result.isEmpty || result[0].rawAddress.isEmpty) {
          _handleNoInternet();
          return;
        }
      } on SocketException catch (_) {
        _handleNoInternet();
        return;
      }

      setState(() {
        if (_tabController.index == 3) {
          _isLoading = true;
        }
        _errorMessage = '';
      });

      // Get current user
      final supabase = Supabase.instance.client;
      final user = supabase.auth.currentUser;

      if (user == null) {
        setState(() {
          _errorMessage = 'Please log in to view applied jobs';
          _isLoading = false;
          _appliedJobIds = {};
        });
        return;
      }

      // Fetch applied jobs with job details
      final response = await supabase
          .from('worker_job_applications')
          .select('''
    id,
    job_id,
    shift_id,
    position_number,
    job_title,
    company,
    location,
    date,
    start_time,
    end_time,
    pay_rate,
    pay_currency,
    application_status,
    application_date
  ''')
          .eq('user_id', user.id)
          .order('application_date', ascending: false);

      // Log the response for debugging
      debugPrint('Applied jobs response: ${response.length} items');

      if (response.isEmpty) {
        debugPrint('No applied jobs found');
      } else {
        debugPrint('First applied job: ${response[0]}');
      }

      // Update the applied job IDs set
      final appliedIds = <String>{};
      for (final job in response) {
        final shiftId = job['shift_id']?.toString();
        final positionNumber = job['position_number']?.toString();
        if (shiftId != null && positionNumber != null) {
          appliedIds.add('$shiftId|$positionNumber');
        }
      }

      setState(() {
        _appliedJobs = List<Map<String, dynamic>>.from(response);
        _appliedJobIds = appliedIds;
        _isLoading = false;
      });

      debugPrint('Applied job IDs: $_appliedJobIds');
    } catch (e) {
      debugPrint('Error fetching applied jobs: $e');
      setState(() {
        _errorMessage = 'Failed to load applied jobs: ${e.toString()}';
        _isLoading = false;
      });
    }
  }

  // Helper method to show already applied dialog
  void _showAlreadyAppliedDialog(String jobTitle) {
    showDialog(
      context: context,
      builder:
          (context) => AlertDialog(
            title: const Text('Already Applied'),
            content: Text('You have already applied to $jobTitle.'),
            actions: [
              TextButton(
                onPressed: () => Navigator.of(context).pop(),
                child: const Text('OK'),
              ),
            ],
          ),
    );
  }

  String _formatJobDate(String? date, String? startTime, String? endTime) {
    if (date == null || startTime == null || endTime == null) {
      return 'Unknown Date';
    }

    final parsedDate = DateTime.tryParse(date);
    if (parsedDate == null) return 'Invalid Date';

    final now = DateTime.now();
    String dateText;

    if (parsedDate.year == now.year &&
        parsedDate.month == now.month &&
        parsedDate.day == now.day) {
      dateText = 'Today';
    } else if (parsedDate.year == now.year &&
        parsedDate.month == now.month &&
        parsedDate.day == now.day + 1) {
      dateText = 'Tomorrow';
    } else {
      // Format date as "April 12"
      dateText = '${_getMonthName(parsedDate.month)} ${parsedDate.day}';
    }

    // Convert times to 12-hour format
    String formatTime(String time) {
      final timeParts = time.split(':');
      if (timeParts.length < 2) return time;

      int hour = int.parse(timeParts[0]);
      final minute = timeParts[1];
      String period = 'AM';

      if (hour >= 12) {
        period = 'PM';
        if (hour > 12) {
          hour -= 12;
        }
      }

      if (hour == 0) hour = 12;

      return '$hour:$minute $period';
    }

    final formattedStartTime = formatTime(startTime);
    final formattedEndTime = formatTime(endTime);

    return '$dateText\n$formattedStartTime - $formattedEndTime';
  }

  String _formatDate(String? date) {
    if (date == null) {
      return 'Unknown Date';
    }

    final parsedDate = DateTime.tryParse(date);
    if (parsedDate == null) return 'Invalid Date';

    final now = DateTime.now();

    if (parsedDate.year == now.year &&
        parsedDate.month == now.month &&
        parsedDate.day == now.day) {
      return 'Today';
    } else if (parsedDate.year == now.year &&
        parsedDate.month == now.month &&
        parsedDate.day == now.day + 1) {
      return 'Tomorrow';
    } else {
      // Format date as "April 12"
      return '${_getMonthName(parsedDate.month)} ${parsedDate.day}';
    }
  }

  String _formatTime(String? time) {
    if (time == null) {
      return 'Unknown Time';
    }

    final timeParts = time.split(':');
    if (timeParts.length < 2) return time;

    int hour = int.parse(timeParts[0]);
    final minute = timeParts[1];
    String period = 'AM';

    if (hour >= 12) {
      period = 'PM';
      if (hour > 12) {
        hour -= 12;
      }
    }

    if (hour == 0) hour = 12;

    return '$hour:$minute $period';
  }

  /// Formats the time range between start and end times
  String _formatTimeRange(String? startTime, String? endTime) {
    if (startTime == null || endTime == null) {
      return 'Unknown Time Range';
    }

    final formattedStartTime = _formatTime(startTime);
    final formattedEndTime = _formatTime(endTime);

    return '$formattedStartTime - $formattedEndTime';
  }

  // Helper function for month names (assumed to be defined elsewhere)
  String _getMonthName(int month) {
    final monthNames = [
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

    if (month >= 1 && month <= 12) {
      return monthNames[month - 1];
    }
    return 'Unknown Month';
  }

  // Error handling method
  void _handleApplicationError(dynamic error) {
    // Log the detailed error
    debugPrint('Job Application Error: $error');
    debugPrint('Error Type: ${error.runtimeType}');

    // More detailed error message
    String title = 'Application Failed';
    String errorMessage = 'Unable to submit application. Please try again.';
    IconData iconData = Icons.error_outline;
    Color iconColor = Colors.red;

    // Handle specific error types
    if (error is PostgrestException) {
      // Check for unique constraint violation (shift already taken)
      if (error.message.contains('unique constraint') ||
          error.message.toLowerCase().contains('already applied')) {
        title = 'Shift Already Taken';
        errorMessage =
            'Oops! This shift has already been claimed by another worker. '
            'Please check other available shifts.';
        iconData = Icons.person_off_rounded;
        iconColor = Colors.orange;
      } else {
        errorMessage = 'Database error: ${error.message}';
      }
    } else if (error is AuthException) {
      errorMessage = 'Authentication error. Please log in again.';
      iconData = Icons.lock_outline;
    } else if (error is TimeoutException) {
      errorMessage =
          'Connection timeout. Please check your internet connection.';
      iconData = Icons.signal_wifi_off;
    }

    // Show error dialog
    showDialog(
      context: context,
      builder:
          (context) => Dialog(
            shape: RoundedRectangleBorder(
              borderRadius: BorderRadius.circular(16),
            ),
            child: Container(
              padding: const EdgeInsets.all(24),
              child: Column(
                mainAxisSize: MainAxisSize.min,
                children: [
                  // Error Icon
                  Container(
                    width: 80,
                    height: 80,
                    decoration: BoxDecoration(
                      color: iconColor.withOpacity(0.1),
                      shape: BoxShape.circle,
                    ),
                    child: Icon(iconData, color: iconColor, size: 48),
                  ),
                  const SizedBox(height: 24),

                  // Title
                  Text(
                    title,
                    style: TextStyle(
                      fontSize: 22,
                      fontWeight: FontWeight.bold,
                      color: Colors.grey.shade800,
                    ),
                    textAlign: TextAlign.center,
                  ),
                  const SizedBox(height: 16),

                  // Error Message
                  Text(
                    errorMessage,
                    style: TextStyle(fontSize: 16, color: Colors.grey.shade600),
                    textAlign: TextAlign.center,
                  ),
                  const SizedBox(height: 24),

                  // Action Button
                  SizedBox(
                    width: double.infinity,
                    child: ElevatedButton(
                      onPressed: () {
                        Navigator.of(context).pop();

                        // Optional: Navigate to find jobs or refresh list
                        Navigator.push(
                          context,
                          MaterialPageRoute(
                            builder: (context) => const FindJobsPage(),
                          ),
                        );
                      },
                      style: ElevatedButton.styleFrom(
                        backgroundColor: iconColor,
                        foregroundColor: Colors.white,
                        padding: const EdgeInsets.symmetric(vertical: 12),
                        shape: RoundedRectangleBorder(
                          borderRadius: BorderRadius.circular(12),
                        ),
                      ),
                      child: const Text(
                        'Find Other Shifts',
                        style: TextStyle(fontWeight: FontWeight.w600),
                      ),
                    ),
                  ),
                ],
              ),
            ),
          ),
    );
  }

  void _showSortOptionsBottomSheet() {
    final sortOptions = [
      'Date (Newest)',
      'Date (Oldest)',
      'Pay (Highest)',
      'Pay (Lowest)',
      'Distance (Closest)',
      'Category',
    ];

    showModalBottomSheet(
      context: context,
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.vertical(top: Radius.circular(20)),
      ),
      builder: (context) {
        return Container(
          padding: const EdgeInsets.all(24),
          child: Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              // Header
              Row(
                mainAxisAlignment: MainAxisAlignment.spaceBetween,
                children: [
                  const Text(
                    'Sort By',
                    style: TextStyle(fontSize: 20, fontWeight: FontWeight.bold),
                  ),
                  IconButton(
                    icon: const Icon(Icons.close),
                    onPressed: () => Navigator.of(context).pop(),
                  ),
                ],
              ),
              const SizedBox(height: 16),

              // Sort Options
              ...sortOptions.map((option) {
                return ListTile(
                  title: Text(option),
                  trailing:
                      _sortBy == option
                          ? const Icon(Icons.check_circle, color: Colors.blue)
                          : null,
                  onTap: () {
                    setState(() {
                      _sortBy = option;
                    });
                    _sortJobs(option);
                    Navigator.of(context).pop();
                  },
                );
              }).toList(),
            ],
          ),
        );
      },
    );
  }

  void _sortJobs(String sortOption) {
    print('Sorting jobs by: $sortOption');

    // First update the sort option state
    setState(() {
      _sortBy = sortOption;

      // Now apply the actual sorting logic
      switch (sortOption) {
        case 'Distance (Closest)':
          if (_currentPincode.isNotEmpty) {
            print('Sorting by distance with pincode: $_currentPincode');

            // Debug distances
            if (_jobDistances.isNotEmpty) {
              print('Job distances available: ${_jobDistances.length}');
            } else {
              print('No job distances available');
              // If distances aren't calculated yet, calculate them now
              _calculateAllJobDistances();
            }

            // Sort by distance
            _filteredJobs.sort((a, b) {
              final aId = a['id']?.toString();
              final bId = b['id']?.toString();
              final aDistance =
                  aId != null
                      ? (_jobDistances[aId] ?? double.infinity)
                      : double.infinity;
              final bDistance =
                  bId != null
                      ? (_jobDistances[bId] ?? double.infinity)
                      : double.infinity;
              return aDistance.compareTo(
                bDistance,
              ); // Ascending (closest first)
            });

            print(
              'Jobs sorted by distance. First job: ${_filteredJobs.isNotEmpty ? _filteredJobs.first['job_title'] : "none"}',
            );
          } else {
            print('No current pincode available for distance sorting');
            // If no location, default to newest
            _filteredJobs.sort((a, b) {
              final aDate = a['created_at']?.toString() ?? '';
              final bDate = b['created_at']?.toString() ?? '';
              return bDate.compareTo(aDate); // Descending (newest first)
            });

            // Show message about location needed
            ScaffoldMessenger.of(context).showSnackBar(
              SnackBar(
                content: const Text(
                  'Location access is required to sort by distance. Sorting by date instead.',
                ),
                action: SnackBarAction(
                  label: 'Enable',
                  onPressed: () {
                    // Ask for location permission and get current location
                    _getCurrentLocation().then((_) {
                      // After getting location, re-sort by distance
                      if (_sortBy == 'Distance (Closest)') {
                        _sortJobs('Distance (Closest)');
                      }
                    });
                  },
                ),
              ),
            );
          }
          break;

        // Other sort options...
        case 'Date (Newest)':
          _filteredJobs.sort((a, b) {
            final aDate = a['created_at']?.toString() ?? '';
            final bDate = b['created_at']?.toString() ?? '';
            return bDate.compareTo(aDate); // Descending (newest first)
          });
          break;
        case 'Category':
          _filteredJobs.sort((a, b) {
            final categoryA = (a['category'] ?? '').toLowerCase();
            final categoryB = (b['category'] ?? '').toLowerCase();
            return categoryA.compareTo(categoryB);
          });
          break;
        // Add other cases here...
      }
    });

    // For debugging purposes
    print('After sorting, _sortBy = $_sortBy');
  }

  void _calculateAllJobDistances() {
    if (_currentPincode.isEmpty) {
      print('Cannot calculate distances: No current pincode available');
      return;
    }

    print(
      'Calculating distances for all jobs based on pincode: $_currentPincode',
    );
    _jobDistances.clear();

    for (final job in _allJobs) {
      final jobId = job['id']?.toString();
      final jobPincode = job['job_pincode']?.toString();

      if (jobId != null) {
        final distance = _calculateDistance(jobPincode);
        _jobDistances[jobId] = distance;
        job['distance'] =
            distance; // Add distance to job object for easier access
      }
    }

    print('Calculated distances for ${_jobDistances.length} jobs');
  }

  void _handleGenericError(dynamic e) {
    String errorMessage = 'An unexpected error occurred.';

    if (e is TimeoutException) {
      errorMessage = 'Connection timeout. Please try again.';
    } else if (e is SocketException) {
      errorMessage = 'Network error. Check your connection.';
    }

    setState(() {
      _errorMessage = errorMessage;
      _isLoading = false;
    });

    // Detailed error logging
    debugPrint('Job Fetch Error: $e');
    debugPrint('Error Type: ${e.runtimeType}');
  }

  String? _selectedPincode;
  void _filterJobs(String query) {
    setState(() {
      // Start with all jobs
      _filteredJobs = List.from(_allJobs);

      // Apply text search filter
      if (query.isNotEmpty) {
        _filteredJobs =
            _filteredJobs.where((job) {
              // Safely convert values to strings and lowercase
              final jobTitle =
                  (job['job_title']?.toString() ?? '').toLowerCase();
              final company = (job['company']?.toString() ?? '').toLowerCase();
              final shiftId = (job['shift_id']?.toString() ?? '').toLowerCase();
              final category =
                  (job['category']?.toString() ?? '').toLowerCase();
              final location =
                  (job['location']?.toString() ?? '').toLowerCase();

              // Explicitly get pincode as string and handle potential null or non-string values
              final pincode = (job['job_pincode']?.toString() ?? '');
              final searchQuery = query.toLowerCase();

              return jobTitle.contains(searchQuery) ||
                  company.contains(searchQuery) ||
                  shiftId.contains(searchQuery) ||
                  category.contains(searchQuery) ||
                  location.contains(searchQuery) ||
                  // Use startsWith for pincode to match from the beginning
                  pincode.startsWith(searchQuery);
            }).toList();
      }

      // Debug printing
      debugPrint('Search query: "$query"');
      debugPrint('Filtered jobs count: ${_filteredJobs.length}');
    });
  }

  @override
  void dispose() {
    _searchController.dispose();
    _tabController.dispose();
    _scrollController.dispose();
    super.dispose();
  }

  void _handleBack() {
    Navigator.pushReplacement(
      context,
      MaterialPageRoute(builder: (context) => WorkerDashboard()),
    );
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: StandardAppBar(title: 'Available Shifts', centerTitle: true),
      body: GestureDetector(
        behavior: HitTestBehavior.opaque,
        onTap: () {
          // This will unfocus any text field and dismiss the keyboard
          FocusManager.instance.primaryFocus?.unfocus();
        },
        child: TabBarView(
          controller: _tabController,
          children: [
            // All Jobs (with city selection)
            _buildTabContent(0),

            // Saved Jobs (no city selection)
            _buildTabContent(1),
          ],
        ),
      ),
      bottomNavigationBar: const ShiftHourBottomNavigation(),
    );
  }

  Widget buildShiftsSegmentControl() {
    return Container(
      margin: const EdgeInsets.only(left: 16, right: 16, top: 12, bottom: 0),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(30),
        boxShadow: [
          BoxShadow(
            color: Colors.black.withOpacity(0.05),
            blurRadius: 8,
            spreadRadius: 1,
            offset: const Offset(0, 2),
          ),
        ],
      ),

      child: Padding(
        padding: const EdgeInsets.all(6.0),

        child: Row(
          children: [
            _buildSegmentButton('All Shifts', 0, Colors.blue),
            const SizedBox(width: 8),
            _buildSegmentButton('Saved', 1, Colors.green),
          ],
        ),
      ),
    );
  }

  Widget _buildSegmentButton(String title, int tabIndex, Color color) {
    final isDark = Theme.of(context).brightness == Brightness.dark;
    final isSelected = _tabController.index == tabIndex;

    return Expanded(
      child: GestureDetector(
        onTap: () {
          setState(() {
            _tabController.animateTo(tabIndex);
          });
        },
        child: Container(
          padding: const EdgeInsets.symmetric(vertical: 10),
          decoration: BoxDecoration(
            color: Colors.transparent, // Removed the opacity effect
            borderRadius: BorderRadius.circular(20),
            border: Border.all(
              color: isSelected ? color : Colors.grey.shade300,
              width: 1.5,
            ),
          ),
          child: Center(
            child: Text(
              title,
              style: TextStyle(
                color:
                    isSelected
                        ? color
                        : (isDark ? Colors.white : Colors.black54),
                fontWeight: isSelected ? FontWeight.bold : FontWeight.normal,
                fontSize: 14,
              ),
            ),
          ),
        ),
      ),
    );
  } // Modify _buildTabContent to include city selection for first two tabs

  // Refactored method to build tab content
  Widget _buildTabContent(int tabIndex) {
    print('Building tab content, current _sortBy = $_sortBy');
    return NestedScrollView(
      controller: _scrollController,
      headerSliverBuilder: (context, innerBoxIsScrolled) {
        return [
          // Search Bar
          SliverToBoxAdapter(child: _buildSearchBar()),
          SliverToBoxAdapter(
            child: Container(
              padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
              child: Row(
                children: [
                  // City Filter
                  // Categories Filter
                  Expanded(
                    child: GestureDetector(
                      onTap: _showCategorySelectionBottomSheet,
                      child: Container(
                        padding: const EdgeInsets.symmetric(
                          vertical: 8,
                          horizontal: 12,
                        ),
                        decoration: BoxDecoration(
                          color: Colors.white,
                          borderRadius: BorderRadius.circular(24),
                          border: Border.all(color: Colors.grey.shade300),
                        ),
                        child: Row(
                          mainAxisAlignment: MainAxisAlignment.center,
                          children: [
                            Icon(
                              Icons.category_outlined,
                              size: 16,
                              color: Colors.blue,
                            ),
                            const SizedBox(width: 4),
                            Text('Categories', style: TextStyle(fontSize: 13)),
                          ],
                        ),
                      ),
                    ),
                  ),
                  const SizedBox(width: 8),

                  Expanded(
                    child: InkWell(
                      onTap: () {
                        print('DEBUG: Closest button tapped');

                        // Toggle the selection state
                        setState(() {
                          // Toggle between selected and not selected
                          if (_sortBy == 'Distance (Closest)' &&
                              _hasUserSelectedClosest) {
                            _sortBy =
                                'Date (Newest)'; // Change to default sort if already selected
                            _hasUserSelectedClosest = false;
                            print(
                              'DEBUG: Deselected Closest, new _sortBy = $_sortBy',
                            );
                          } else {
                            _sortBy = 'Distance (Closest)';
                            _hasUserSelectedClosest = true;
                            print(
                              'DEBUG: Selected Closest, new _sortBy = $_sortBy',
                            );
                          }
                        });

                        // Apply the current sort method
                        _sortJobs(_sortBy);
                      },
                      child: Container(
                        padding: const EdgeInsets.symmetric(
                          vertical: 8,
                          horizontal: 12,
                        ),
                        decoration: BoxDecoration(
                          color: Colors.white,
                          borderRadius: BorderRadius.circular(24),
                          border: Border.all(
                            color:
                                (_sortBy == 'Distance (Closest)' &&
                                        _hasUserSelectedClosest)
                                    ? Colors.blue
                                    : Colors.grey.shade500,
                            width:
                                (_sortBy == 'Distance (Closest)' &&
                                        _hasUserSelectedClosest)
                                    ? 2
                                    : 0,
                          ),
                        ),
                        child: Row(
                          mainAxisAlignment: MainAxisAlignment.center,
                          children: [
                            // Conditional icon based on selection state
                            (_sortBy == 'Distance (Closest)' &&
                                    _hasUserSelectedClosest)
                                ? Icon(
                                  Icons.near_me, // Filled icon
                                  size: 16,
                                  color: Colors.blue,
                                )
                                : Icon(
                                  Icons.near_me_outlined, // Outlined icon
                                  size: 16,
                                  color: Colors.teal,
                                ),
                            const SizedBox(width: 4),
                            Text(
                              'Closest',
                              style: TextStyle(
                                fontSize: 13,
                                fontWeight:
                                    (_sortBy == 'Distance (Closest)' &&
                                            _hasUserSelectedClosest)
                                        ? FontWeight.bold
                                        : FontWeight.normal,
                                color:
                                    (_sortBy == 'Distance (Closest)' &&
                                            _hasUserSelectedClosest)
                                        ? Colors.blue
                                        : Colors.black,
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

          // Location and Filter Chips (for All Jobs and Recommended tabs)
          if (tabIndex < 2)
            // Segment Control
            if (tabIndex < 2)
              SliverToBoxAdapter(
                child: Container(
                  color: Colors.white,
                  margin: EdgeInsets.zero,
                  child: buildShiftsSegmentControl(),
                ),
              ),

          // Segment Control

          // Filters (only for All Jobs and Recommended tabs)
        ];
      },
      body: _buildTabBody(tabIndex),
    );
  }

  Widget _buildTab(String title, int index) {
    final isSelected = _tabController.index == index;

    return Expanded(
      child: GestureDetector(
        onTap: () {
          _tabController.animateTo(index);
        },
        child: Container(
          padding: const EdgeInsets.symmetric(vertical: 10),
          decoration: BoxDecoration(
            color: isSelected ? Color(0xFF5B6BF8) : Colors.transparent,
            borderRadius: BorderRadius.circular(50),
          ),
          child: Center(
            child: Text(
              title,
              style: TextStyle(
                color: isSelected ? Colors.white : Color(0xFF6B7280),
                fontWeight: isSelected ? FontWeight.bold : FontWeight.normal,
              ),
            ),
          ),
        ),
      ),
    );
  }

  // Filter Chip
  Widget _buildFilterChip({
    required IconData icon,
    required String text,
    required VoidCallback onTap,
  }) {
    return GestureDetector(
      onTap: onTap,
      child: Container(
        padding: EdgeInsets.symmetric(horizontal: 12, vertical: 8),
        decoration: BoxDecoration(
          color: Colors.white,
          borderRadius: BorderRadius.circular(8),
          border: Border.all(color: Colors.grey.shade300),
        ),
        child: Row(
          mainAxisSize: MainAxisSize.min,
          children: [
            Icon(icon, size: 16, color: Colors.blue),
            SizedBox(width: 4),
            Text(text, style: TextStyle(color: Colors.black87, fontSize: 12)),
          ],
        ),
      ),
    );
  }

  // Sort Dropdown
  Widget _buildSortDropdown() {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(8),
        border: Border.all(color: Colors.grey.shade300),
      ),
      child: DropdownButton<String>(
        value: _sortBy,
        icon: const Icon(Icons.arrow_drop_down),
        elevation: 1,
        underline: Container(height: 0),
        isDense: true,
        style: const TextStyle(color: Colors.black87, fontSize: 14),
        onChanged: (String? newValue) {
          if (newValue != null) {
            setState(() {
              _sortBy = newValue;
            });
            _sortJobs(newValue);
          }
        },
        items:
            <String>[
              'Date (Newest)',
              'Pay (Highest)',
              'Distance (Closest)',
              'Match (Best)',
            ].map<DropdownMenuItem<String>>((String value) {
              return DropdownMenuItem<String>(value: value, child: Text(value));
            }).toList(),
        hint: const Text('Sort By'),
      ),
    );
  }

  // Filters Button
  Widget _buildFiltersButton() {
    return Container(
      decoration: BoxDecoration(
        borderRadius: BorderRadius.circular(8),
        border: Border.all(color: Colors.grey.shade300),
      ),
      child: IconButton(
        icon: Icon(Icons.filter_list, color: Colors.black87),
        onPressed: () {
          // _showCategorySelectionBottomSheet();
        },
        tooltip:
            _selectedCategories.isEmpty
                ? 'Filters'
                : 'Filters (${_selectedCategories.length})',
      ),
    );
  }

  // Short label for sort options
  String _getSortByShortLabel() {
    switch (_sortBy) {
      case 'Date (Newest)':
        return 'Newest';
      case 'Pay (Highest)':
        return 'Highest Pay';
      case 'Distance (Closest)':
        return 'Closest';
      case 'Match (Best)':
        return 'Best Match';
      default:
        return 'Sort';
    }
  }

  void _applyFilters() {
    setState(() {
      // If no categories selected, show all jobs
      if (_selectedCategories.isEmpty) {
        _filteredJobs = List.from(_allJobs);
      } else {
        // Filter jobs based on selected categories
        _filteredJobs =
            _allJobs.where((job) {
              final jobCategory = (job['category'] ?? '');
              return _selectedCategories.contains(jobCategory);
            }).toList();
      }
    });
  }

  // Refactored method to build tab body content
  Widget _buildTabBody(int tabIndex) {
    // Define which refresh method to call based on the tab
    Future<void> onRefresh() async {
      setState(() {
        _isLoading = true; // Add this line to show skeleton during refresh
      });
      switch (tabIndex) {
        case 0: // All Jobs
          await _fetchJobs(); // This already has loading logic
          break;
        case 1: // Saved
          await _fetchSavedJobs();
          break;
      }
      setState(() {
        _isLoading = false; // Add this line to show skeleton during refresh
      });
    }

    // Wrap everything with RefreshIndicator
    return RefreshIndicator(
      onRefresh: onRefresh,
      color: Colors.blue.shade700,
      backgroundColor: Colors.white,
      child: Builder(
        builder: (context) {
          // Handle loading state
          if (_isLoading) {
            return ListView.builder(
              physics: const AlwaysScrollableScrollPhysics(),
              itemCount: 3,
              itemBuilder: (context, index) => _buildSkeletonCard(),
            );
          }

          // Handle error state
          if (_errorMessage.isNotEmpty) {
            return ListView(
              physics: const AlwaysScrollableScrollPhysics(),
              children: [
                SizedBox(
                  height: MediaQuery.of(context).size.height * 0.4,
                  child: Center(
                    child: Column(
                      mainAxisAlignment: MainAxisAlignment.center,
                      children: [
                        const Icon(
                          Icons.error_outline,
                          color: Colors.red,
                          size: 60,
                        ),
                        const SizedBox(height: 16),
                        Text(
                          _errorMessage,
                          style: const TextStyle(
                            color: Colors.red,
                            fontSize: 16,
                          ),
                          textAlign: TextAlign.center,
                        ),
                        const SizedBox(height: 16),
                        ElevatedButton(
                          onPressed: onRefresh,
                          style: ElevatedButton.styleFrom(
                            backgroundColor: Colors.blue.shade600,
                            foregroundColor: Colors.white,
                            padding: const EdgeInsets.symmetric(
                              horizontal: 24,
                              vertical: 12,
                            ),
                            shape: RoundedRectangleBorder(
                              borderRadius: BorderRadius.circular(8),
                            ),
                          ),
                          child: const Text('Retry'),
                        ),
                      ],
                    ),
                  ),
                ),
              ],
            );
          }

          // Determine which list to show based on active tab
          List<Map<String, dynamic>> currentTabJobs;
          String emptyMessage;
          Widget emptyStateIcon;
          String emptyStateActionText;
          VoidCallback emptyStateAction;

          switch (tabIndex) {
            case 1: // Saved
              currentTabJobs = _savedJobs;
              emptyMessage =
                  'No saved jobs found\nSave jobs to view them later';
              emptyStateIcon = const Icon(
                Icons.bookmark_border,
                color: Colors.grey,
                size: 60,
              );
              emptyStateActionText = 'Browse Shifts';
              emptyStateAction = () => _tabController.animateTo(0);
              break;
            case 2: // Applied
              currentTabJobs = _appliedJobs;
              emptyMessage =
                  'No applied jobs found\nApply to jobs to track your applications';
              emptyStateIcon = const Icon(
                Icons.work_off,
                color: Colors.grey,
                size: 60,
              );
              emptyStateActionText = 'Browse Shifts';
              emptyStateAction = () => _tabController.animateTo(0);
              break;
            default: // All Jobs
              currentTabJobs = _filteredJobs;
              emptyMessage = 'No Shifts found';
              emptyStateIcon = const Icon(
                Icons.search_off,
                color: Colors.grey,
                size: 60,
              );
              emptyStateActionText = 'Refresh';
              emptyStateAction = _fetchJobs;
          }

          // Handle empty state
          if (currentTabJobs.isEmpty) {
            return ListView(
              physics: const AlwaysScrollableScrollPhysics(),
              children: [
                SizedBox(
                  height: MediaQuery.of(context).size.height * 0.4,
                  child: Center(
                    child: Padding(
                      padding: const EdgeInsets.all(24.0),
                      child: Column(
                        mainAxisAlignment: MainAxisAlignment.center,
                        children: [
                          emptyStateIcon,
                          const SizedBox(height: 16),
                          Text(
                            emptyMessage,
                            style: const TextStyle(
                              fontSize: 16,
                              color: Colors.grey,
                            ),
                            textAlign: TextAlign.center,
                          ),
                          const SizedBox(height: 20),
                          ElevatedButton(
                            onPressed: emptyStateAction,
                            style: ElevatedButton.styleFrom(
                              backgroundColor: Colors.blue.shade600,
                              foregroundColor: Colors.white,
                              padding: const EdgeInsets.symmetric(
                                horizontal: 24,
                                vertical: 12,
                              ),
                              shape: RoundedRectangleBorder(
                                borderRadius: BorderRadius.circular(8),
                              ),
                            ),
                            child: Text(emptyStateActionText),
                          ),
                        ],
                      ),
                    ),
                  ),
                ),
              ],
            );
          }

          // Display the jobs list
          return ListView.builder(
            physics: const AlwaysScrollableScrollPhysics(),
            itemCount: currentTabJobs.length,
            itemBuilder: (context, index) {
              final job = currentTabJobs[index];
              // Return the appropriate card based on tab
              switch (tabIndex) {
                case 1: // Saved Jobs tab
                  return _buildSavedJobCard(job);
                default: // All Jobs and Recommended tabs
                  return _buildJobCard(job);
              }
            },
          );
        },
      ),
    );
  }

  Widget _buildEventCard() {
    return Card(
      margin: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      elevation: 1,
      shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
      child: Column(
        children: [
          Padding(
            padding: const EdgeInsets.all(16),
            child: Row(
              mainAxisAlignment: MainAxisAlignment.spaceBetween,
              children: [
                Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: const [
                    Text(
                      'Event Staff',
                      style: TextStyle(
                        fontSize: 18,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                    SizedBox(height: 4),
                    Text(
                      'New opportunities coming soon',
                      style: TextStyle(
                        fontSize: 14,
                        color: Color(0xFF64748B), // slate-500
                      ),
                    ),
                  ],
                ),
                Container(
                  decoration: BoxDecoration(
                    color: Colors.white,
                    shape: BoxShape.circle,
                    boxShadow: [
                      BoxShadow(
                        color: Colors.black.withOpacity(0.1),
                        blurRadius: 4,
                        offset: const Offset(0, 2),
                      ),
                    ],
                  ),
                  child: IconButton(
                    icon: const Icon(
                      Icons.chevron_right,
                      color: Color(0xFF4F46E5), // indigo-600
                    ),
                    onPressed: () {},
                  ),
                ),
              ],
            ),
          ),
          // Gradient bar at bottom
          Container(
            height: 4,
            decoration: const BoxDecoration(
              gradient: LinearGradient(
                colors: [
                  Color(0xFF818CF8), // indigo-400
                  Color(0xFFA855F7), // purple-500
                  Color(0xFFEC4899), // pink-500
                ],
              ),
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildSkeletonCard() {
    return Container(
      margin: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(16),
        border: Border.all(color: Colors.grey.shade200),
        boxShadow: [
          BoxShadow(
            color: Colors.grey.withOpacity(0.1),
            blurRadius: 4,
            offset: const Offset(0, 2),
          ),
        ],
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          // Top section skeleton
          Container(
            padding: const EdgeInsets.all(16),
            decoration: BoxDecoration(
              color: Colors.blue.shade50,
              borderRadius: const BorderRadius.only(
                topLeft: Radius.circular(16),
                topRight: Radius.circular(16),
              ),
            ),
            child: Row(
              mainAxisAlignment: MainAxisAlignment.spaceBetween,
              children: [
                Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Container(
                      width: 120,
                      height: 20,
                      decoration: BoxDecoration(
                        color: Colors.grey.shade200,
                        borderRadius: BorderRadius.circular(4),
                      ),
                    ),
                    const SizedBox(height: 8),
                    Container(
                      width: 80,
                      height: 16,
                      decoration: BoxDecoration(
                        color: Colors.grey.shade200,
                        borderRadius: BorderRadius.circular(4),
                      ),
                    ),
                  ],
                ),
                Container(
                  width: 60,
                  height: 28,
                  decoration: BoxDecoration(
                    color: Colors.grey.shade200,
                    borderRadius: BorderRadius.circular(16),
                  ),
                ),
              ],
            ),
          ),

          // Content skeleton
          Padding(
            padding: const EdgeInsets.all(16),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                // Time row
                Row(
                  children: [
                    Container(
                      width: 18,
                      height: 18,
                      decoration: BoxDecoration(
                        color: Colors.grey.shade200,
                        shape: BoxShape.circle,
                      ),
                    ),
                    const SizedBox(width: 8),
                    Container(
                      width: 150,
                      height: 16,
                      decoration: BoxDecoration(
                        color: Colors.grey.shade200,
                        borderRadius: BorderRadius.circular(4),
                      ),
                    ),
                  ],
                ),
                const SizedBox(height: 12),

                // Location row
                Row(
                  children: [
                    Container(
                      width: 18,
                      height: 18,
                      decoration: BoxDecoration(
                        color: Colors.grey.shade200,
                        shape: BoxShape.circle,
                      ),
                    ),
                    const SizedBox(width: 8),
                    Container(
                      width: 200,
                      height: 16,
                      decoration: BoxDecoration(
                        color: Colors.grey.shade200,
                        borderRadius: BorderRadius.circular(4),
                      ),
                    ),
                  ],
                ),

                const SizedBox(height: 16),
                Container(height: 1, color: Colors.grey.shade200),
                const SizedBox(height: 16),

                // Bottom row
                Row(
                  mainAxisAlignment: MainAxisAlignment.spaceBetween,
                  children: [
                    Container(
                      width: 80,
                      height: 20,
                      decoration: BoxDecoration(
                        color: Colors.grey.shade200,
                        borderRadius: BorderRadius.circular(4),
                      ),
                    ),

                    Row(
                      children: [
                        Container(
                          width: 32,
                          height: 32,
                          decoration: BoxDecoration(
                            color: Colors.grey.shade200,
                            shape: BoxShape.circle,
                          ),
                        ),
                        const SizedBox(width: 8),
                        Container(
                          width: 80,
                          height: 32,
                          decoration: BoxDecoration(
                            color: Colors.grey.shade200,
                            borderRadius: BorderRadius.circular(20),
                          ),
                        ),
                      ],
                    ),
                  ],
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildJobCard(Map<String, dynamic> job) {
    String duration = _calculateShiftDuration(
      job['start_time'],
      job['end_time'],
    );
    double originalPayRate =
        double.tryParse(job['pay_rate']?.toString() ?? '0') ?? 0;

    // Use original pay rate as total payment
    double totalPayment = originalPayRate;
    // Get original pay rate
    double hourlyRate = _calculateHourlyRate(duration, originalPayRate);

    // Check if this job has been applied for
    final jobId = job['id']?.toString();
    final batchId = job['batch_id']?.toString();
    final pincode = job['job_pincode']?.toString();
    final hasApplied = _appliedJobIds.contains(
      '${job['shift_id']}|${job['position_number']}',
    );

    return Container(
      margin: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(16),
        border: Border.all(color: Colors.grey.shade200),
        boxShadow: [
          BoxShadow(
            color: Colors.grey.withOpacity(0.1),
            blurRadius: 4,
            offset: const Offset(0, 2),
          ),
        ],
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          // Top section with job title and status
          Container(
            padding: const EdgeInsets.all(16),
            decoration: BoxDecoration(
              color: Colors.blue.shade600,
              borderRadius: const BorderRadius.only(
                topLeft: Radius.circular(16),
                topRight: Radius.circular(16),
              ),
            ),
            child: Row(
              mainAxisAlignment: MainAxisAlignment.spaceBetween,
              children: [
                Expanded(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text(
                        'Title: ${job['job_title'] ?? 'Position'}',
                        style: const TextStyle(
                          fontSize: 18,
                          fontWeight: FontWeight.bold,
                          color: Colors.white,
                        ),
                      ),
                      Text(
                        'Category: ${job['category'] ?? 'Other'}',
                        style: TextStyle(
                          fontSize: 14,
                          color: Colors.white,
                          fontWeight: FontWeight.w500,
                        ),
                      ),
                      const SizedBox(height: 4),
                      Text(
                        'Shift ID: ${job['shift_id'] ?? 'N/A'}',
                        style: TextStyle(fontSize: 14, color: Colors.white),
                      ),
                    ],
                  ),
                ),
              ],
            ),
          ),

          // Time and location info
          Padding(
            padding: const EdgeInsets.all(16),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
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
                      Icons.access_time_rounded,
                      size: 18,
                      color: Colors.orange.shade700,
                    ),
                    const SizedBox(width: 8),
                    Text(
                      'Date: '
                      '${_formatJobDatee(job['date'])}',
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
                      '${_formatJobTime(job['start_time'])} - ${_formatJobTime(job['end_time'])}',
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
                        'location: ${job['location'] ?? 'Location'}',
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
                        final address = '${job['location'] ?? ''}';
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
                  mainAxisAlignment: MainAxisAlignment.spaceBetween,
                  children: [
                    // Duration
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
                  ],
                ),

                SizedBox(height: 16),
                //const Divider(thickness: 5),
                // Payment Details
                // Payment Details
                Padding(
                  padding: const EdgeInsets.symmetric(horizontal: 16),
                  child: Container(
                    width: double.infinity,
                    padding: EdgeInsets.all(12),
                    decoration: BoxDecoration(
                      color: Color(0xFFF5F7FF),
                      borderRadius: BorderRadius.circular(12),
                      border: Border.all(color: Color(0xFFE0E6FF), width: 1),
                    ),
                    child: Row(
                      mainAxisAlignment: MainAxisAlignment.spaceBetween,
                      children: [
                        Row(
                          children: [
                            Icon(
                              Icons.currency_rupee,
                              color: Colors.blue,
                              size: 18,
                            ),
                            SizedBox(width: 8),
                            Column(
                              crossAxisAlignment: CrossAxisAlignment.start,
                              children: [
                                Text(
                                  'Hourly Rate',
                                  style: TextStyle(
                                    color: Color(0x8A000000),
                                    fontSize: 12,
                                    fontWeight: FontWeight.w500,
                                  ),
                                ),
                                Text(
                                  'INR ${hourlyRate.toStringAsFixed(2)}',
                                  style: TextStyle(
                                    color: Color(0xDD000000),
                                    fontSize: 14,
                                    fontWeight: FontWeight.w600,
                                  ),
                                ),
                              ],
                            ),
                          ],
                        ),
                        Container(
                          width: 1,
                          height: 40,
                          color: Color(0xFFE0E6FF),
                        ),
                        Row(
                          children: [
                            Icon(
                              Icons.account_balance_wallet,
                              color: Colors.blue,
                              size: 18,
                            ),
                            SizedBox(width: 8),
                            Column(
                              crossAxisAlignment: CrossAxisAlignment.start,
                              children: [
                                Text(
                                  'Total Pay',
                                  style: TextStyle(
                                    color: Color(0x8A000000),
                                    fontSize: 12,
                                    fontWeight: FontWeight.w500,
                                  ),
                                ),
                                Text(
                                  'INR ${originalPayRate.toStringAsFixed(2)}',
                                  style: TextStyle(
                                    color: Color(0xDD000000),
                                    fontSize: 14,
                                    fontWeight: FontWeight.w600,
                                  ),
                                ),
                              ],
                            ),
                          ],
                        ),
                      ],
                    ),
                  ),
                ),
                const SizedBox(height: 16),
                //  const Divider(),
                const SizedBox(height: 16),

                // Pay rate and action buttons
                Row(
                  mainAxisAlignment: MainAxisAlignment.end,
                  crossAxisAlignment: CrossAxisAlignment.end,
                  children: [
                    // Apply/Save buttons
                    Row(
                      children: [
                        // Save button
                        IconButton(
                          icon: Icon(
                            job['is_saved'] == true
                                ? Icons.bookmark
                                : Icons.bookmark_outline,
                            color:
                                job['is_saved'] == true
                                    ? Colors.blue.shade700
                                    : Colors.grey.shade700,
                          ),
                          onPressed:
                              _isSavingJob
                                  ? null
                                  : () => toggleSaveJob(
                                    job,
                                  ), // Pass the entire job object
                        ),

                        // Apply button
                        hasApplied
                            ? ElevatedButton.icon(
                              onPressed: () {
                                // Navigate to Applied tab to see application
                                _tabController.animateTo(3);
                              },
                              icon: const Icon(Icons.check_circle, size: 16),
                              label: const Text('Applied'),
                              style: ElevatedButton.styleFrom(
                                backgroundColor: Colors.green.shade600,
                                foregroundColor: Colors.white,
                                elevation: 0,
                                shape: RoundedRectangleBorder(
                                  borderRadius: BorderRadius.circular(20),
                                ),
                              ),
                            )
                            : ElevatedButton(
                              onPressed:
                                  _isApplying
                                      ? null
                                      : () => applyToJob(
                                        job,
                                      ), // Pass the entire job object
                              style: ElevatedButton.styleFrom(
                                backgroundColor: Colors.blue.shade600,
                                foregroundColor: Colors.white,
                                elevation: 0,
                                shape: RoundedRectangleBorder(
                                  borderRadius: BorderRadius.circular(20),
                                ),
                              ),
                              child:
                                  _isApplying
                                      ? const SizedBox(
                                        width: 16,
                                        height: 16,
                                        child: CircularProgressIndicator(
                                          strokeWidth: 2,
                                          valueColor:
                                              AlwaysStoppedAnimation<Color>(
                                                Colors.white,
                                              ),
                                        ),
                                      )
                                      : const Text('Apply'),
                            ),
                      ],
                    ),
                  ],
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }

  String _formatJobDatee(String? dateString) {
    if (dateString == null) return '';

    final date = DateTime.tryParse(dateString);
    if (date == null) return '';

    return '${_getMonthName(date.month)} ${date.day} ${date.year}';
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

  Widget _buildSearchBar() {
    return Container(
      margin: const EdgeInsets.symmetric(horizontal: 16, vertical: 12),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(30),
        boxShadow: [
          BoxShadow(
            color: Colors.black.withOpacity(0.05),
            blurRadius: 8,
            spreadRadius: 0,
            offset: const Offset(0, 2),
          ),
        ],
      ),
      child: Row(
        children: [
          Padding(
            padding: const EdgeInsets.only(left: 16.0),
            child: Icon(Icons.search, color: Colors.grey.shade500),
          ),
          Expanded(
            child: TextField(
              controller: _searchController,
              decoration: InputDecoration(
                hintText: 'Search shifts',
                hintStyle: TextStyle(color: Colors.grey.shade400),
                border: InputBorder.none,
                contentPadding: const EdgeInsets.symmetric(
                  horizontal: 16,
                  vertical: 12,
                ),
              ),
              style: const TextStyle(fontSize: 16),
              onChanged: _filterJobs,
            ),
          ),
          if (_searchController.text.isNotEmpty)
            IconButton(
              icon: Icon(Icons.close, color: Colors.grey.shade500),
              onPressed: () {
                _searchController.clear();
                _filterJobs('');
              },
            ),
        ],
      ),
    );
  }

  // Helper methods for date formatting

  // Month name helper
  Widget _buildSavedJobCard(Map<String, dynamic> job) {
    String duration = _calculateShiftDuration(
      job['start_time'],
      job['end_time'],
    );
    double originalPayRate =
        double.tryParse(job['pay_rate']?.toString() ?? '0') ?? 0;

    // Use original pay rate as total payment
    double totalPayment = originalPayRate;
    // Get original pay rate
    double hourlyRate = _calculateHourlyRate(duration, originalPayRate);

    // Check if this job has been applied for
    final jobId = job['id']?.toString();
    final batchId = job['batch_id']?.toString();
    final pincode = job['job_pincode']?.toString();
    final hasApplied =
        (jobId != null &&
            (_appliedJobIds.contains(jobId) || job['is_applied'] == true)) ||
        (batchId != null &&
            batchId.isNotEmpty &&
            _appliedBatchIds.contains(batchId)) ||
        job['batch_applied'] == true;

    return Container(
      margin: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(16),
        border: Border.all(color: Colors.grey.shade200),
        boxShadow: [
          BoxShadow(
            color: Colors.grey.withOpacity(0.1),
            blurRadius: 4,
            offset: const Offset(0, 2),
          ),
        ],
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          // Top section with job title and status
          Container(
            padding: const EdgeInsets.all(16),
            decoration: BoxDecoration(
              color: Colors.green.shade600,
              borderRadius: const BorderRadius.only(
                topLeft: Radius.circular(16),
                topRight: Radius.circular(16),
              ),
            ),
            child: Row(
              mainAxisAlignment: MainAxisAlignment.spaceBetween,
              children: [
                Expanded(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text(
                        'Title: ${job['job_title'] ?? 'Position'}',
                        style: const TextStyle(
                          fontSize: 18,
                          fontWeight: FontWeight.bold,
                          color: Colors.white,
                        ),
                      ),
                      Text(
                        'Category: ${job['category'] ?? 'Other'}',
                        style: TextStyle(
                          fontSize: 14,
                          color: Colors.white,
                          fontWeight: FontWeight.w500,
                        ),
                      ),
                      const SizedBox(height: 4),
                      Text(
                        'Shift ID: ${job['shift_id'] ?? 'N/A'}',
                        style: TextStyle(fontSize: 14, color: Colors.white),
                      ),
                    ],
                  ),
                ),
                Container(
                  padding: const EdgeInsets.all(8),
                  decoration: BoxDecoration(
                    color: Colors.white.withOpacity(0.3),
                    shape: BoxShape.circle,
                  ),
                  child: Icon(Icons.bookmark, color: Colors.white, size: 16),
                ),
              ],
            ),
          ),

          // Time and location info
          Padding(
            padding: const EdgeInsets.all(16),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
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
                      Icons.access_time_rounded,
                      size: 18,
                      color: Colors.orange.shade700,
                    ),
                    const SizedBox(width: 8),
                    Text(
                      'Date: '
                      '${_formatDate(job['date'])}',
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
                      '${_formatTimeRange(job['start_time'], job['end_time'])}',
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
                        'location: '
                        '${job['company'] ?? 'Company'}, ${job['location'] ?? 'Location'}',
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
                  mainAxisAlignment: MainAxisAlignment.spaceBetween,
                  children: [
                    // Duration
                    Row(
                      children: [
                        Icon(
                          Icons.pin_drop_outlined,
                          size: 18,
                          color: Colors.red,
                        ),
                        SizedBox(width: 8),
                        Text(
                          'Pincode: ${pincode ?? 'N/A'}',
                          style: TextStyle(
                            color: Colors.grey.shade700,
                            fontSize: 15,
                          ),
                        ),
                      ],
                    ),
                  ],
                ),

                // Saved date
                if (job['saved_date'] != null) ...[
                  const SizedBox(height: 12),
                  Row(
                    children: [
                      Icon(
                        Icons.history,
                        size: 18,
                        color: Colors.grey.shade700,
                      ),
                      const SizedBox(width: 8),
                      Text(
                        'Saved ${_formatSavedDate(job['saved_date'])}',
                        style: TextStyle(
                          fontSize: 14,
                          color: Colors.grey.shade600,
                        ),
                      ),
                    ],
                  ),
                ],

                SizedBox(height: 16),
                //const Divider(thickness: 5),
                // Payment Details
                Padding(
                  padding: const EdgeInsets.symmetric(horizontal: 16),
                  child: Container(
                    width: double.infinity,
                    padding: EdgeInsets.all(12),
                    decoration: BoxDecoration(
                      color: Color(0xFFF5F7FF),
                      borderRadius: BorderRadius.circular(12),
                      border: Border.all(color: Color(0xFFE0E6FF), width: 1),
                    ),
                    child: Row(
                      mainAxisAlignment: MainAxisAlignment.spaceBetween,
                      children: [
                        Row(
                          children: [
                            Icon(
                              Icons.currency_rupee,
                              color: Colors.blue,
                              size: 18,
                            ),
                            SizedBox(width: 8),
                            Column(
                              crossAxisAlignment: CrossAxisAlignment.start,
                              children: [
                                Text(
                                  'Hourly Rate',
                                  style: TextStyle(
                                    color: Color(0x8A000000),
                                    fontSize: 12,
                                    fontWeight: FontWeight.w500,
                                  ),
                                ),
                                Text(
                                  'INR ${hourlyRate.toStringAsFixed(2)}',
                                  style: TextStyle(
                                    color: Color(0xDD000000),
                                    fontSize: 14,
                                    fontWeight: FontWeight.w600,
                                  ),
                                ),
                              ],
                            ),
                          ],
                        ),
                        Container(
                          width: 1,
                          height: 40,
                          color: Color(0xFFE0E6FF),
                        ),
                        Row(
                          children: [
                            Icon(
                              Icons.account_balance_wallet,
                              color: Colors.blue,
                              size: 18,
                            ),
                            SizedBox(width: 8),
                            Column(
                              crossAxisAlignment: CrossAxisAlignment.start,
                              children: [
                                Text(
                                  'Total Pay',
                                  style: TextStyle(
                                    color: Color(0x8A000000),
                                    fontSize: 12,
                                    fontWeight: FontWeight.w500,
                                  ),
                                ),
                                Text(
                                  'INR ${originalPayRate.toStringAsFixed(2)}',
                                  style: TextStyle(
                                    color: Color(0xDD000000),
                                    fontSize: 14,
                                    fontWeight: FontWeight.w600,
                                  ),
                                ),
                              ],
                            ),
                          ],
                        ),
                      ],
                    ),
                  ),
                ),
                const SizedBox(height: 16),
                //  const Divider(),
                const SizedBox(height: 16),

                // Pay rate and action buttons
                Row(
                  mainAxisAlignment: MainAxisAlignment.end,
                  crossAxisAlignment: CrossAxisAlignment.end,
                  children: [
                    // Action buttons
                    Row(
                      children: [
                        IconButton(
                          icon: Icon(
                            Icons
                                .bookmark, // Always filled since it's already saved
                            color: Colors.blue.shade700,
                          ),
                          onPressed:
                              _isSavingJob ? null : () => toggleSaveJob(job),
                        ),
                        hasApplied
                            ? ElevatedButton.icon(
                              onPressed: () {
                                // Navigate to Applied tab to see application
                                _tabController.animateTo(3);
                              },
                              icon: const Icon(Icons.check_circle, size: 16),
                              label: const Text('Applied'),
                              style: ElevatedButton.styleFrom(
                                backgroundColor: Colors.green.shade600,
                                foregroundColor: Colors.white,
                                elevation: 0,
                                shape: RoundedRectangleBorder(
                                  borderRadius: BorderRadius.circular(20),
                                ),
                              ),
                            )
                            : ElevatedButton(
                              onPressed:
                                  _isApplying
                                      ? null
                                      : () => applyToSavedJob(
                                        job,
                                      ), // Pass the entire job object
                              style: ElevatedButton.styleFrom(
                                backgroundColor: Colors.blue.shade600,
                                foregroundColor: Colors.white,
                                elevation: 0,
                                shape: RoundedRectangleBorder(
                                  borderRadius: BorderRadius.circular(20),
                                ),
                              ),
                              child:
                                  _isApplying
                                      ? const SizedBox(
                                        width: 16,
                                        height: 16,
                                        child: CircularProgressIndicator(
                                          strokeWidth: 2,
                                          valueColor:
                                              AlwaysStoppedAnimation<Color>(
                                                Colors.white,
                                              ),
                                        ),
                                      )
                                      : const Text('Apply'),
                            ),
                      ],
                    ),
                  ],
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }

  String _formatSavedDate(String? dateString) {
    if (dateString == null) return 'Recently';

    final date = DateTime.tryParse(dateString);
    if (date == null) return 'Recently';

    final now = DateTime.now();
    final difference = now.difference(date);

    if (difference.inDays == 0) {
      if (difference.inHours == 0) {
        return '${difference.inMinutes} minutes ago';
      }
      return '${difference.inHours} hours ago';
    } else if (difference.inDays == 1) {
      return 'Yesterday';
    } else if (difference.inDays < 7) {
      return '${difference.inDays} days ago';
    } else {
      return '${_getMonthName(date.month)} ${date.day}, ${date.year}';
    }
  }

  // Format application date helper
  String _formatApplicationDate(String? dateString) {
    if (dateString == null) return 'Unknown date';

    final date = DateTime.tryParse(dateString);
    if (date == null) return 'Invalid date';

    final now = DateTime.now();
    final difference = now.difference(date);

    if (difference.inDays == 0) {
      if (difference.inHours == 0) {
        return '${difference.inMinutes} minutes ago';
      }
      return '${difference.inHours} hours ago';
    } else if (difference.inDays == 1) {
      return 'Yesterday';
    } else if (difference.inDays < 7) {
      return '${difference.inDays} days ago';
    } else {
      return '${_getMonthName(date.month)} ${date.day}, ${date.year}';
    }
  }

  // Show application details dialog
  void _showApplicationDetailsDialog(Map<String, dynamic> job) {
    // Helper method to format status
    Widget _buildStatusStep(
      String status,
      String label,
      bool isActive,
      bool isCompleted,
    ) {
      return Row(
        children: [
          Container(
            width: 24,
            height: 24,
            decoration: BoxDecoration(
              shape: BoxShape.circle,
              color:
                  isCompleted
                      ? const Color(0xFF4F46E5) // indigo-600
                      : isActive
                      ? Colors.white
                      : const Color(0xFFE2E8F0), // slate-200
              border:
                  isActive && !isCompleted
                      ? Border.all(color: const Color(0xFF4F46E5), width: 2)
                      : null,
            ),
            child:
                isCompleted
                    ? const Icon(Icons.check, size: 16, color: Colors.white)
                    : null,
          ),
          const SizedBox(width: 8),
          Text(
            label,
            style: TextStyle(
              fontWeight:
                  isActive || isCompleted ? FontWeight.bold : FontWeight.normal,
              color: isActive || isCompleted ? Colors.black : Colors.grey,
            ),
          ),
        ],
      );
    }

    // Current application status
    final currentStatus = job['status']?.toLowerCase() ?? 'applied';

    showDialog(
      context: context,
      builder:
          (context) => Dialog(
            shape: RoundedRectangleBorder(
              borderRadius: BorderRadius.circular(16),
            ),
            child: Padding(
              padding: const EdgeInsets.all(16),
              child: Column(
                mainAxisSize: MainAxisSize.min,
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  // Header
                  Text(
                    job['job_title'] ?? 'Job Application',
                    style: const TextStyle(
                      fontSize: 20,
                      fontWeight: FontWeight.bold,
                    ),
                  ),
                  Text(
                    job['company'] ?? 'Unknown Company',
                    style: const TextStyle(
                      fontSize: 16,
                      color: Color(0xFF64748B), // slate-500
                    ),
                  ),
                  const SizedBox(height: 16),

                  // Application Status
                  const Text(
                    'Application Status',
                    style: TextStyle(fontSize: 16, fontWeight: FontWeight.bold),
                  ),
                  const SizedBox(height: 12),

                  // Status Timeline
                  Column(
                    children: [
                      _buildStatusStep(
                        'applied',
                        'Applied',
                        currentStatus == 'applied',
                        [
                          'applied',
                          'screening',
                          'interview',
                          'accepted',
                        ].contains(currentStatus),
                      ),
                      Container(
                        margin: const EdgeInsets.only(left: 12),
                        height: 20,
                        width: 1,
                        color: const Color(0xFFE2E8F0), // slate-200
                      ),
                      _buildStatusStep(
                        'screening',
                        'Screening',
                        currentStatus == 'screening',
                        [
                          'screening',
                          'interview',
                          'accepted',
                        ].contains(currentStatus),
                      ),
                      Container(
                        margin: const EdgeInsets.only(left: 12),
                        height: 20,
                        width: 1,
                        color: const Color(0xFFE2E8F0), // slate-200
                      ),
                      _buildStatusStep(
                        'interview',
                        'Interview',
                        currentStatus == 'interview',
                        ['interview', 'accepted'].contains(currentStatus),
                      ),
                      Container(
                        margin: const EdgeInsets.only(left: 12),
                        height: 20,
                        width: 1,
                        color: const Color(0xFFE2E8F0), // slate-200
                      ),
                      _buildStatusStep(
                        'accepted',
                        'Accepted',
                        currentStatus == 'accepted',
                        currentStatus == 'accepted',
                      ),
                    ],
                  ),

                  const SizedBox(height: 16),

                  // Job Details
                  const Text(
                    'Job Details',
                    style: TextStyle(fontSize: 16, fontWeight: FontWeight.bold),
                  ),
                  const SizedBox(height: 8),

                  // Location
                  Row(
                    children: [
                      const Icon(
                        Icons.location_on,
                        size: 16,
                        color: Colors.grey,
                      ),
                      const SizedBox(width: 8),
                      Text(
                        job['location'] ?? 'Unknown Location',
                        style: const TextStyle(fontSize: 14),
                      ),
                    ],
                  ),
                  const SizedBox(height: 4),

                  // Date
                  Row(
                    children: [
                      const Icon(
                        Icons.calendar_today,
                        size: 16,
                        color: Colors.grey,
                      ),
                      const SizedBox(width: 8),
                      Text(
                        _formatJobDate(
                          job['date'],
                          job['start_time'],
                          job['end_time'],
                        ),
                        style: const TextStyle(fontSize: 14),
                      ),
                    ],
                  ),
                  const SizedBox(height: 4),

                  // Pay
                  Row(
                    children: [
                      const Icon(Icons.payments, size: 16, color: Colors.grey),
                      const SizedBox(width: 8),
                      Text(
                        '₹${job['pay_rate']?.toStringAsFixed(2) ?? '0'}/day',
                        style: const TextStyle(fontSize: 14),
                      ),
                    ],
                  ),

                  const SizedBox(height: 16),

                  // Actions
                  Row(
                    mainAxisAlignment: MainAxisAlignment.end,
                    children: [
                      TextButton(
                        onPressed: () => Navigator.of(context).pop(),
                        child: const Text('Close'),
                      ),
                    ],
                  ),
                ],
              ),
            ),
          ),
    );
  }
}

class _SliverTabBarDelegate extends SliverPersistentHeaderDelegate {
  _SliverTabBarDelegate(this._tabBar);

  final TabBar _tabBar;

  @override
  double get minExtent => _tabBar.preferredSize.height;

  @override
  double get maxExtent => _tabBar.preferredSize.height;

  @override
  Widget build(
    BuildContext context,
    double shrinkOffset,
    bool overlapsContent,
  ) {
    return Container(color: Colors.white, child: _tabBar);
  }

  @override
  bool shouldRebuild(_SliverTabBarDelegate oldDelegate) {
    return false;
  }
}

// Delegate for filter bar
class _SliverFilterBarDelegate extends SliverPersistentHeaderDelegate {
  _SliverFilterBarDelegate(this._filterBar);

  final Widget _filterBar;

  @override
  double get minExtent => 60;

  @override
  double get maxExtent => 60;

  @override
  Widget build(
    BuildContext context,
    double shrinkOffset,
    bool overlapsContent,
  ) {
    return _filterBar;
  }

  @override
  bool shouldRebuild(_SliverFilterBarDelegate oldDelegate) {
    return false;
  }
}

class CustomSegmentControl extends StatefulWidget {
  final TabController controller;
  final List<CustomTab> tabs;

  const CustomSegmentControl({
    Key? key,
    required this.controller,
    required this.tabs,
  }) : super(key: key);

  @override
  _CustomSegmentControlState createState() => _CustomSegmentControlState();
}

class _CustomSegmentControlState extends State<CustomSegmentControl> {
  @override
  void initState() {
    super.initState();
    widget.controller.addListener(_onTabChanged);
  }

  void _onTabChanged() {
    setState(() {});
  }

  @override
  void dispose() {
    widget.controller.removeListener(_onTabChanged);
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Row(
      children:
          widget.tabs.map((tab) {
            final index = widget.tabs.indexOf(tab);
            final isSelected = widget.controller.index == index;

            return Expanded(
              child: GestureDetector(
                onTap: () => widget.controller.animateTo(index),
                child: Container(
                  decoration: BoxDecoration(
                    color: isSelected ? tab.selectedColor : Colors.transparent,
                    border: Border.all(
                      color:
                          isSelected
                              ? (tab.selectedColor ?? Colors.blue.shade100)
                              : Colors.grey.shade300,
                      width: 1,
                    ),
                    borderRadius: BorderRadius.horizontal(
                      left:
                          index == 0 ? const Radius.circular(20) : Radius.zero,
                      right:
                          index == widget.tabs.length - 1
                              ? const Radius.circular(20)
                              : Radius.zero,
                    ),
                  ),
                  padding: const EdgeInsets.symmetric(
                    vertical: 10,
                    horizontal: 12,
                  ),
                  child: Center(
                    child: Text(
                      tab.text,
                      style: TextStyle(
                        color:
                            isSelected
                                ? (tab.selectedTextColor ??
                                    Colors.blue.shade700)
                                : Colors.grey,
                        fontWeight:
                            isSelected ? FontWeight.bold : FontWeight.normal,
                        fontSize: 14,
                      ),
                    ),
                  ),
                ),
              ),
            );
          }).toList(),
    );
  }
}

// Custom Tab class
class CustomTab {
  final String text;
  final Color? selectedColor;
  final Color? selectedTextColor;

  const CustomTab({
    required this.text,
    this.selectedColor,
    this.selectedTextColor,
  });
}

// Delegate for the segment control
class _SliverSegmentControlDelegate extends SliverPersistentHeaderDelegate {
  final Widget _segmentControl;

  _SliverSegmentControlDelegate(this._segmentControl);

  @override
  double get minExtent => 60;

  @override
  double get maxExtent => 60;

  @override
  Widget build(
    BuildContext context,
    double shrinkOffset,
    bool overlapsContent,
  ) {
    return Container(color: Colors.white, child: _segmentControl);
  }

  @override
  bool shouldRebuild(_SliverSegmentControlDelegate oldDelegate) {
    return false;
  }
}
