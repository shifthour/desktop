import 'package:flutter/material.dart';
import 'package:shifthour/worker/services/services.dart';
import 'package:shifthour/worker/worker_dashboard.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:url_launcher/url_launcher.dart';

// Import the database service we created

class ShiftDetailsPage extends StatefulWidget {
  const ShiftDetailsPage({Key? key, this.shiftId, this.positionNumber})
    : super(key: key);

  final String? shiftId;
  final int? positionNumber;

  @override
  State<ShiftDetailsPage> createState() => _ShiftDetailsPageState();
}

class _ShiftDetailsPageState extends State<ShiftDetailsPage>
    with SingleTickerProviderStateMixin {
  late TabController _tabController;
  String activeTab = 'details';

  // Create instance of DatabaseService
  final DatabaseService _databaseService = DatabaseService();

  // State variables
  bool _isLoading = true;
  Map<String, dynamic>? shift;
  String? _errorMessage;

  @override
  void initState() {
    super.initState();
    _tabController = TabController(length: 3, vsync: this);
    _tabController.addListener(() {
      if (_tabController.indexIsChanging) {
        setState(() {
          switch (_tabController.index) {
            case 0:
              activeTab = 'details';
              break;
            case 1:
              activeTab = 'timeline';
              break;
            case 2:
              activeTab = 'feedback';
              break;
          }
        });
      }
    });

    // Fetch shift data
    _fetchShiftData();
  }

  void _showCancelShiftDialog(BuildContext context) {
    showDialog(
      context: context,
      builder: (BuildContext context) {
        return AlertDialog(
          title: const Text('Cancel Shift'),
          content: const Text(
            'Are you sure you want to cancel this shift? '
            'This action can only be done more than 24 hours before the shift starts.',
          ),
          actions: [
            TextButton(
              onPressed: () {
                Navigator.of(context).pop(); // Close dialog
              },
              child: const Text('No'),
            ),
            ElevatedButton(
              onPressed: () async {
                try {
                  // Get the shift_id from the current shift
                  // final shiftId = shift!['shift_id'];

                  // Call cancel method
                  final cancelled = await _databaseService.cancelShift(
                    widget.shiftId.toString(),
                  );

                  if (cancelled) {
                    // Show success message
                    ScaffoldMessenger.of(context).showSnackBar(
                      const SnackBar(
                        content: Text('Shift successfully cancelled'),
                        backgroundColor: Colors.green,
                      ),
                    );

                    // Navigate back to dashboard or shifts page
                    Navigator.of(context).pushAndRemoveUntil(
                      MaterialPageRoute(
                        builder: (context) => WorkerDashboard(),
                      ),
                      (Route<dynamic> route) => false,
                    );
                  }
                } catch (e) {
                  // Show error message
                  ScaffoldMessenger.of(context).showSnackBar(
                    SnackBar(
                      content: Text('Failed to cancel shift: ${e.toString()}'),
                      backgroundColor: Colors.red,
                    ),
                  );

                  // Close the dialog
                  Navigator.of(context).pop();
                }
              },
              style: ElevatedButton.styleFrom(
                backgroundColor: Colors.red,
                foregroundColor: Colors.white,
              ),
              child: const Text('Yes, Cancel Shift'),
            ),
          ],
        );
      },
    );
  }

  Future<void> _fetchShiftData() async {
    if (widget.shiftId == null) {
      setState(() {
        _isLoading = false;
        _errorMessage = "Shift ID is missing";
      });
      return;
    }

    print('Attempting to fetch shift with ID: ${widget.shiftId}');

    try {
      final shiftData = await _databaseService.getShiftById(widget.shiftId!);

      setState(() {
        if (shiftData == null) {
          _isLoading = false;
          _errorMessage =
              "Shift not found. Please check the shift ID.${widget.shiftId}";
          print('No shift found for ID: ${widget.shiftId}');
        } else {
          shift = shiftData;
          _isLoading = false;
          _errorMessage = null;
          print('Shift successfully loaded: $shift');
        }
      });
    } catch (e) {
      setState(() {
        _isLoading = false;
        _errorMessage = "Error loading shift: ${e.toString()}";
      });
      print('Detailed error in _fetchShiftData: $e');
    }
  }

  void openGoogleMapsNavigation(BuildContext context, String location) async {
    // Detailed location parsing and handling
    final fullLocation = location;

    // Prepare multiple URL strategies
    final mapUrls = [
      // Google Maps search URLs
      Uri.parse(
        'https://www.google.com/maps/search/?api=1&query=${Uri.encodeComponent(fullLocation)}',
      ),
      Uri.parse(
        'https://maps.google.com/maps?q=${Uri.encodeComponent(fullLocation)}',
      ),

      // Geo URI with full address
      Uri.parse('geo:0,0?q=${Uri.encodeComponent(fullLocation)}'),

      // Specific coordinates for ITPB (if known)
      Uri.parse('geo:12.9850,77.7100?q=International+Tech+Park+Bangalore'),

      // Google Maps app URL
      Uri.parse('comgooglemaps://?q=${Uri.encodeComponent(fullLocation)}'),
    ];

    bool urlLaunched = false;

    // Try launching each URL
    for (final url in mapUrls) {
      try {
        print('Attempting to launch map URL: $url');

        // Check if the URL can be launched
        final canLaunch = await canLaunchUrl(url);
        print('Can launch URL: $canLaunch');

        if (canLaunch) {
          // Attempt to launch the URL
          final launched = await launchUrl(
            url,
            mode: LaunchMode.externalApplication,
          );

          print('URL launched successfully: $launched');

          if (launched) {
            urlLaunched = true;
            break;
          }
        }
      } catch (e) {
        print('Error launching map URL: $e');
      }
    }

    // If no URL could be launched
    if (!urlLaunched) {
      print('Failed to launch any map URL');

      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('Unable to open maps for location: $fullLocation'),
          backgroundColor: Colors.orange,
          duration: Duration(seconds: 3),
        ),
      );
    }
  }

  @override
  void dispose() {
    _tabController.dispose();
    _feedbackController.dispose();
    _tabController.dispose();

    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: const Color(0xFFF4F8FD), // Light background
      body: SafeArea(
        child:
            _isLoading
                ? const Center(child: CircularProgressIndicator())
                : _errorMessage != null
                ? Center(child: Text(_errorMessage!))
                : Column(
                  children: [
                    _buildHeader(),
                    Expanded(
                      child: SingleChildScrollView(
                        child: Padding(
                          padding: const EdgeInsets.fromLTRB(16, 16, 16, 24),
                          child: Column(
                            crossAxisAlignment: CrossAxisAlignment.start,
                            children: [
                              _buildJobInfo(),
                              const SizedBox(height: 24),
                              _buildTabs(),
                              _buildTabContent(),
                            ],
                          ),
                        ),
                      ),
                    ),
                  ],
                ),
      ),
    );
  }

  Widget _buildHeader() {
    final statusColor =
        shift!['status'] == 'Upcoming'
            ? shift!['statusColor'] as Color
            : shift!['status'] == 'In Progress'
            ? const Color(0xFFF59E0B) // amber-500
            : shift!['status'] == 'Completed'
            ? const Color(0xFF10B981) // emerald-500
            : Colors.grey; // slate-500

    return Container(
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: Colors.white.withOpacity(0.8),
        border: Border(
          bottom: BorderSide(color: Colors.grey.shade200, width: 1),
        ),
      ),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.spaceBetween,
        children: [
          // Back button with text
          GestureDetector(
            onTap: () {
              Navigator.pushReplacement(
                context,
                MaterialPageRoute(builder: (context) => WorkerDashboard()),
              );
            },

            child: Row(
              children: [
                Container(
                  padding: const EdgeInsets.all(8),
                  decoration: BoxDecoration(
                    color: Colors.grey.shade100,
                    shape: BoxShape.circle,
                  ),
                  child: Icon(
                    Icons.arrow_back,
                    size: 20,
                    color: Colors.grey.shade700,
                  ),
                ),
                const SizedBox(width: 12),
                Text(
                  'Back to My Shifts',
                  style: TextStyle(
                    fontSize: 16,
                    fontWeight: FontWeight.w500,
                    color: Colors.grey.shade600,
                  ),
                ),
              ],
            ),
          ),

          // Status Badge
          Container(
            padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 6),
            decoration: BoxDecoration(
              color: statusColor,
              borderRadius: BorderRadius.circular(50),
            ),
            child: Text(
              shift!['applied_status'] as String,
              style: const TextStyle(
                color: Colors.white,
                fontSize: 13,
                fontWeight: FontWeight.w500,
              ),
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildJobInfo() {
    // Check if shift is null before accessing any properties
    if (shift == null) {
      return const SizedBox.shrink(); // Return an empty widget if no shift data
    }

    // Safely extract values with null checks and type conversions
    final String companyLogo = (shift!['companyLogo'] ?? 'SC').toString();
    final Color logoColor = _getColorFromHex(
      (shift!['logoColor'] ?? '#F59E0B').toString(),
    );
    final String jobTitle = (shift!['jobTitle'] ?? 'Untitled Job').toString();
    final String company = (shift!['company'] ?? 'Unknown Company').toString();

    return Row(
      children: [
        // Company Logo
        Container(
          width: 64,
          height: 64,
          decoration: BoxDecoration(
            color: logoColor,
            borderRadius: BorderRadius.circular(16),
          ),
          child: Center(
            child: Text(
              companyLogo,
              style: const TextStyle(
                color: Colors.white,
                fontSize: 24,
                fontWeight: FontWeight.bold,
              ),
            ),
          ),
        ),
        const SizedBox(width: 16),
        // Job Title and Company
        Expanded(
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text(
                jobTitle,
                style: const TextStyle(
                  fontSize: 24,
                  fontWeight: FontWeight.bold,
                  color: Color(0xFF1F2937), // slate-800
                ),
              ),
              Text(
                company,
                style: TextStyle(fontSize: 18, color: Colors.grey.shade600),
              ),
            ],
          ),
        ),
      ],
    );
  }

  Color _getColorFromHex(String hexColor) {
    try {
      hexColor = hexColor.replaceAll('#', '');
      if (hexColor.length == 6) {
        hexColor = 'FF$hexColor';
      }
      return Color(int.parse('0x$hexColor'));
    } catch (e) {
      return const Color(0xFFF59E0B); // Default amber color
    }
  }

  Widget _buildTabs() {
    return Container(
      margin: const EdgeInsets.only(bottom: 24),
      decoration: BoxDecoration(
        color: Colors.grey.shade100,
        borderRadius: BorderRadius.circular(16),
      ),
      child: TabBar(
        controller: _tabController,
        labelColor: Colors.black87,
        unselectedLabelColor: Colors.grey.shade600,
        indicatorSize: TabBarIndicatorSize.tab,
        indicator: BoxDecoration(
          color: Colors.white,
          borderRadius: BorderRadius.circular(12),
          boxShadow: [
            BoxShadow(
              color: Colors.black.withOpacity(0.05),
              blurRadius: 2,
              offset: const Offset(0, 1),
            ),
          ],
        ),
        padding: const EdgeInsets.all(4),
        labelPadding: const EdgeInsets.symmetric(horizontal: 0, vertical: 12),
        tabs: const [
          Tab(text: 'Details'),
          Tab(text: 'Timeline'),
          Tab(text: 'Feedback'),
        ],
      ),
    );
  }

  Widget _buildTabContent() {
    switch (activeTab) {
      case 'timeline':
        return _buildTimelineTab();
      case 'feedback':
        return _buildFeedbackTab();
      case 'details':
      default:
        return _buildDetailsTab();
    }
  }

  Widget _buildDetailsTab() {
    return Column(
      children: [
        // Key Information Card
        _buildCard(
          title: 'Shift Information',
          content: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Row(
                children: [
                  Expanded(
                    child: _buildInfoItem(
                      icon: Icons.calendar_today,
                      title: 'Date',
                      value: shift!['date'] as String,
                    ),
                  ),
                  Expanded(
                    child: _buildInfoItem(
                      icon: Icons.access_time,
                      title: 'Time',
                      value:
                          '${shift!['startTime']} - ${shift!['endTime']} (${shift!['duration']})',
                    ),
                  ),
                ],
              ),
              const SizedBox(height: 16),
              _buildInfoItem(
                icon: Icons.location_on,
                title: 'Location',
                value:
                    (shift!['location'] as Map<String, dynamic>)['address']
                        as String,
              ),
              const SizedBox(height: 16),
              Row(
                children: [
                  Expanded(
                    child: _buildInfoItem(
                      icon: Icons.credit_card,
                      title: 'Pay Rate',
                      value: shift!['payRate'] as String,
                    ),
                  ),
                  Expanded(
                    child: _buildInfoItem(
                      icon: Icons.work,
                      title: 'Job Category',
                      value: shift!['jobCategory'] as String,
                    ),
                  ),
                ],
              ),
              const SizedBox(height: 16),
              _buildInfoItem(
                icon: Icons.person,
                title: 'Supervisor',
                value:
                    '${(shift!['supervisor'] as Map<String, dynamic>)['name']} • ${(shift!['supervisor'] as Map<String, dynamic>)['phone']}',
              ),
            ],
          ),
        ),
        const SizedBox(height: 24),

        // Check-In Information Card
        _buildCard(
          title: 'Check-In Information',
          content: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              _buildInfoItem(
                icon: Icons.check_circle,
                title: 'Check-In Time',
                value: shift!['checkInTime'] as String,
              ),
              const SizedBox(height: 16), // Add spacing
              Row(
                // Use a Row for the buttons
                mainAxisAlignment: MainAxisAlignment.start,
                children: [
                  const SizedBox(width: 8),
                  OutlinedButton.icon(
                    icon: const Icon(Icons.qr_code, size: 18),
                    label: const Text('Scan QR Code'),
                    onPressed: () {
                      // QR Code scanning functionality would go here
                    },
                    style: OutlinedButton.styleFrom(
                      padding: const EdgeInsets.symmetric(
                        horizontal: 16,
                        vertical: 12,
                      ),
                      side: BorderSide(color: Colors.grey.shade300),
                      shape: RoundedRectangleBorder(
                        borderRadius: BorderRadius.circular(12),
                      ),
                    ),
                  ),
                ],
              ),
            ],
          ),
        ),
        const SizedBox(height: 24),

        // Job Details Card
        _buildCard(
          title: 'Job Details',
          content: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              // Description
              const Text(
                'Description',
                style: TextStyle(
                  fontWeight: FontWeight.w600,
                  fontSize: 16,
                  color: Color(0xFF1F2937), // slate-800
                ),
              ),
              const SizedBox(height: 8),
              Text(
                shift!['description'] as String,
                style: TextStyle(color: Colors.grey.shade600),
              ),
              const SizedBox(height: 20),

              // Required Skills
              const Text(
                'Required Skills',
                style: TextStyle(
                  fontWeight: FontWeight.w600,
                  fontSize: 16,
                  color: Color(0xFF1F2937), // slate-800
                ),
              ),
              const SizedBox(height: 8),
              Wrap(
                spacing: 8,
                runSpacing: 8,
                children:
                    (shift!['requiredSkills'] as List).map((skill) {
                      return Container(
                        padding: const EdgeInsets.symmetric(
                          horizontal: 12,
                          vertical: 6,
                        ),
                        decoration: BoxDecoration(
                          color: Colors.grey.shade100,
                          borderRadius: BorderRadius.circular(50),
                        ),
                        child: Text(
                          skill.toString(),
                          style: TextStyle(
                            color: Colors.grey.shade700,
                            fontSize: 14,
                          ),
                        ),
                      );
                    }).toList(),
              ),
              const SizedBox(height: 20),

              // Dress Code and Break Info
              Row(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Expanded(
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        _buildIconHeader(
                          icon: Icons.checkroom,
                          title: 'Dress Code',
                        ),
                        const SizedBox(height: 4),
                        Text(
                          shift!['dressCode'] as String,
                          style: TextStyle(color: Colors.grey.shade600),
                        ),
                      ],
                    ),
                  ),
                  const SizedBox(width: 10),
                  Expanded(
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        _buildIconHeader(icon: Icons.coffee, title: 'Break'),
                        const SizedBox(height: 4),
                        Text(
                          shift!['breakInfo'] as String,
                          style: TextStyle(color: Colors.grey.shade600),
                        ),
                      ],
                    ),
                  ),
                ],
              ),
              const SizedBox(height: 20),

              // Additional Information
              const Text(
                'Additional Information',
                style: TextStyle(
                  fontWeight: FontWeight.w600,
                  fontSize: 16,
                  color: Color(0xFF1F2937), // slate-800
                ),
              ),
              const SizedBox(height: 8),
              _buildInfoItem(
                icon: Icons.shield,
                title: 'Safety Instructions',
                value: shift!['safetyInstructions'] as String,
              ),
              const SizedBox(height: 12),
              _buildInfoItem(
                icon: Icons.info,
                title: 'Special Notes',
                value: shift!['specialNotes'] as String,
              ),
            ],
          ),
        ),
        const SizedBox(height: 24),

        // Actions Card
        _buildCard(
          title: 'Actions',
          content: Container(
            width: double.infinity,
            height: 150, // Explicit fixed height for the entire container
            child: Column(
              children: [
                Expanded(
                  child: Row(
                    children: [
                      Expanded(
                        child: Padding(
                          padding: const EdgeInsets.all(4.0),
                          child: _buildActionButton(
                            icon: Icons.navigation,
                            label: 'Get Directions',
                            color: const Color(0xFF4F46E5),
                            onPressed: () {
                              openGoogleMapsNavigation(
                                context,
                                (shift!['location']
                                        as Map<String, dynamic>)['address']
                                    as String,
                              );
                            },
                          ),
                        ),
                      ),
                      Expanded(
                        child: Padding(
                          padding: const EdgeInsets.all(4.0),
                          child: _buildActionButton(
                            icon: Icons.phone,
                            label: 'Contact Employer',
                            color: const Color(0xFF10B981),
                            onPressed: () {
                              // Call supervisor
                            },
                          ),
                        ),
                      ),
                    ],
                  ),
                ),
                Expanded(
                  child: Row(
                    children: [
                      Expanded(
                        child: Padding(
                          padding: const EdgeInsets.all(4.0),
                          child: _buildActionButton(
                            icon: Icons.report_problem,
                            label: 'Report Issue',
                            color: const Color(0xFFF59E0B),
                            onPressed: () {
                              // Show issue report form
                            },
                          ),
                        ),
                      ),
                      Expanded(
                        child: Padding(
                          padding: const EdgeInsets.all(4.0),
                          child: _buildActionButton(
                            icon: Icons.cancel,
                            label: 'Cancel Shift',
                            color: const Color(0xFFEF4444),
                            onPressed: () {
                              // Show confirmation dialog
                              _showCancelShiftDialog(context);
                            },
                          ),
                        ),
                      ),
                    ],
                  ),
                ),
              ],
            ),
          ),
        ),
        const SizedBox(height: 24),

        // Map Card
        _buildCard(
          title: 'Location',
          padding: EdgeInsets.zero,
          content: Column(
            children: [
              Container(
                height: 300,
                color: Colors.grey.shade200,
                child: Center(
                  child: Stack(
                    alignment: Alignment.center,
                    children: [
                      Icon(
                        Icons.location_on,
                        size: 48,
                        color: const Color(
                          0xFF4F46E5,
                        ).withOpacity(0.8), // indigo-600
                      ),
                      Text(
                        'Interactive map would be displayed here',
                        style: TextStyle(color: Colors.grey.shade600),
                      ),
                    ],
                  ),
                ),
              ),
              Container(
                padding: const EdgeInsets.all(16),
                decoration: BoxDecoration(
                  color: Colors.grey.shade50,
                  border: Border(top: BorderSide(color: Colors.grey.shade200)),
                ),
                child: SizedBox(
                  width: double.infinity,
                  child: ElevatedButton.icon(
                    icon: const Icon(Icons.navigation),
                    label: const Text('Get Directions'),
                    onPressed: () {
                      openGoogleMapsNavigation(
                        context,
                        (shift!['location'] as Map<String, dynamic>)['address']
                            as String,
                      );
                    },
                    style: ElevatedButton.styleFrom(
                      backgroundColor: const Color(0xFF4F46E5), // indigo-600
                      foregroundColor: Colors.white,
                      padding: const EdgeInsets.symmetric(vertical: 12),
                      shape: RoundedRectangleBorder(
                        borderRadius: BorderRadius.circular(12),
                      ),
                    ),
                  ),
                ),
              ),
            ],
          ),
        ),
      ],
    );
  }

  Widget _buildTimelineTab() {
    final shiftTimeline = shift!['timeline'] as List;

    return Column(
      children: [
        // Timeline Card
        _buildCard(
          title: 'Shift Timeline',
          subtitle: 'Chronological events related to this shift',
          content:
              shiftTimeline.isEmpty
                  ? _buildEmptyState(
                    icon: Icons.timeline,
                    title: 'No Timeline Events',
                    description:
                        'This shift does not have any timeline events yet.',
                  )
                  : Padding(
                    padding: const EdgeInsets.only(left: 16),
                    child: Column(
                      children: [
                        for (var i = 0; i < shiftTimeline.length; i++)
                          _buildTimelineItem(
                            event: shiftTimeline[i]['event'] as String,
                            time: shiftTimeline[i]['time'] as String,
                            icon: _getIconForTimeline(
                              shiftTimeline[i]['icon'] as String,
                            ),
                            isUpcoming:
                                shiftTimeline[i]['upcoming'] as bool? ?? false,
                            isLast: i == shiftTimeline.length - 1,
                          ),
                      ],
                    ),
                  ),
        ),
      ],
    );
  }

  int _userRating = 0;
  final TextEditingController _feedbackController = TextEditingController();
  bool _isSavingFeedback = false;
  Widget _buildFeedbackTab() {
    // Null check
    if (shift == null) {
      return Center(
        child: Text(
          'Error: No shift data available',
          style: TextStyle(color: Colors.red),
        ),
      );
    }

    // Status checks
    final applicationStatus =
        (shift!['applied_status'] ??
                shift!['application_status'] ??
                shift!['status'] ??
                '')
            .toString()
            .trim()
            .toLowerCase();

    print('Current shift status: $applicationStatus');

    // Define completed statuses
    final completedStatuses = [
      'completed',
      'done',
      'finish',
      'closed',
      'ended',
    ];

    // Check if completed
    final isCompleted =
        completedStatuses.contains(applicationStatus) ||
        applicationStatus == 'completed';

    print('Is shift completed? $isCompleted');

    // If not completed, show message
    if (!isCompleted) {
      return Column(
        children: [
          _buildEmptyState(
            icon: Icons.message,
            title: 'Feedback Not Available',
            description:
                'Feedback and ratings will be available only after you complete this shift.',
          ),
        ],
      );
    }

    // Check if user has already submitted feedback for this shift
    return FutureBuilder<Map<String, dynamic>?>(
      future: _checkExistingFeedback(),
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.waiting) {
          return Center(child: CircularProgressIndicator());
        }

        // If there's existing feedback, return a thank you widget instead of showing the form
        if (snapshot.hasData && snapshot.data != null) {
          final feedbackData = snapshot.data!;
          final rating = feedbackData['rating'] ?? 0;
          final comments = feedbackData['comments'] ?? '';

          return Column(
            children: [
              _buildCard(
                title: 'Your Feedback',
                subtitle: 'You have already submitted feedback for this shift',
                content: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    const Text(
                      'Your Rating',
                      style: TextStyle(
                        fontWeight: FontWeight.w600,
                        fontSize: 16,
                        color: Color(0xFF1F2937),
                      ),
                    ),
                    const SizedBox(height: 8),
                    Row(
                      children: List.generate(5, (index) {
                        return Icon(
                          Icons.star,
                          size: 28,
                          color:
                              index < rating
                                  ? const Color(0xFFFCD34D)
                                  : Colors.grey.shade300,
                        );
                      }),
                    ),
                    if (comments.isNotEmpty) ...[
                      const SizedBox(height: 16),
                      const Text(
                        'Your Comments',
                        style: TextStyle(
                          fontWeight: FontWeight.w600,
                          fontSize: 16,
                          color: Color(0xFF1F2937),
                        ),
                      ),
                      const SizedBox(height: 8),
                      Container(
                        width: double.infinity,
                        padding: const EdgeInsets.all(12),
                        decoration: BoxDecoration(
                          color: Colors.grey.shade50,
                          borderRadius: BorderRadius.circular(8),
                          border: Border.all(color: Colors.grey.shade200),
                        ),
                        child: Text(
                          comments,
                          style: TextStyle(color: Colors.grey.shade700),
                        ),
                      ),
                    ],
                    const SizedBox(height: 16),
                    Text(
                      'Thank you for your feedback! Your input helps us improve our services.',
                      style: TextStyle(
                        color: Colors.grey.shade600,
                        fontStyle: FontStyle.italic,
                      ),
                    ),
                  ],
                ),
              ),
            ],
          );
        }

        // If no existing feedback, show the feedback form
        return Column(
          children: [
            _buildCard(
              title: 'Shift Feedback',
              subtitle: 'Rate your experience with this employer',
              content: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  const Text(
                    'Rate This Employer',
                    style: TextStyle(
                      fontWeight: FontWeight.w600,
                      fontSize: 16,
                      color: Color(0xFF1F2937),
                    ),
                  ),
                  const SizedBox(height: 8),
                  Row(
                    children: List.generate(5, (index) {
                      return IconButton(
                        icon: Icon(
                          Icons.star,
                          size: 32,
                          color:
                              index < _userRating
                                  ? const Color(0xFFFCD34D)
                                  : Colors.grey.shade300,
                        ),
                        onPressed: () {
                          setState(() {
                            _userRating = index + 1;
                          });
                        },
                      );
                    }),
                  ),
                  const SizedBox(height: 16),
                  const Text(
                    'Comments (Optional)',
                    style: TextStyle(
                      fontWeight: FontWeight.w600,
                      fontSize: 16,
                      color: Color(0xFF1F2937),
                    ),
                  ),
                  const SizedBox(height: 8),
                  TextField(
                    controller: _feedbackController,
                    decoration: InputDecoration(
                      hintText: 'Share your experience with this employer...',
                      border: OutlineInputBorder(
                        borderRadius: BorderRadius.circular(12),
                      ),
                    ),
                    maxLines: 3,
                  ),
                ],
              ),
              footer: SizedBox(
                width: double.infinity,
                child: ElevatedButton(
                  onPressed:
                      _isSavingFeedback
                          ? null
                          : () async {
                            if (_userRating == 0) {
                              ScaffoldMessenger.of(context).showSnackBar(
                                const SnackBar(
                                  content: Text('Please select a rating'),
                                  backgroundColor: Colors.orange,
                                ),
                              );
                              return;
                            }

                            setState(() {
                              _isSavingFeedback = true;
                            });

                            try {
                              final user =
                                  Supabase.instance.client.auth.currentUser;
                              if (user == null) {
                                throw Exception('No authenticated user found');
                              }

                              // Get the appropriate job_id for feedback
                              String? jobListingId;

                              if (shift!.containsKey('job_id') &&
                                  shift!['job_id'] != null) {
                                jobListingId = shift!['job_id'].toString();
                                print(
                                  'Using job_id from shift data: $jobListingId',
                                );
                              } else {
                                jobListingId =
                                    shift!['id'] as String? ?? widget.shiftId;
                                print(
                                  'Using ID from shift data or widget: $jobListingId',
                                );
                              }

                              if (jobListingId == null ||
                                  jobListingId.isEmpty) {
                                throw Exception(
                                  'Could not determine job listing ID for feedback',
                                );
                              }

                              // New submission logic
                              try {
                                final success = await _databaseService
                                    .submitFeedback(
                                      jobListingId: jobListingId,
                                      userId: user.id,
                                      rating: _userRating,
                                      comments: _feedbackController.text,
                                    );

                                if (success) {
                                  ScaffoldMessenger.of(context).showSnackBar(
                                    const SnackBar(
                                      content: Text(
                                        'Feedback submitted successfully',
                                      ),
                                      backgroundColor: Colors.green,
                                    ),
                                  );

                                  // Refresh the feedback tab to show the submitted feedback
                                  setState(() {});
                                }
                              } catch (e) {
                                String errorMessage =
                                    'Failed to submit feedback';

                                // Check for the specific error message about already submitted feedback
                                if (e.toString().contains(
                                  'already submitted',
                                )) {
                                  errorMessage =
                                      'You have already submitted feedback for this job';

                                  // Refresh the feedback tab to show the existing feedback
                                  setState(() {});
                                }

                                ScaffoldMessenger.of(context).showSnackBar(
                                  SnackBar(
                                    content: Text(errorMessage),
                                    backgroundColor: Colors.red,
                                  ),
                                );
                              }
                            } catch (e) {
                              print('Submission preparation error: $e');
                              ScaffoldMessenger.of(context).showSnackBar(
                                SnackBar(
                                  content: Text(
                                    'Failed to prepare feedback: ${e.toString()}',
                                  ),
                                  backgroundColor: Colors.red,
                                ),
                              );
                            } finally {
                              setState(() {
                                _isSavingFeedback = false;
                              });
                            }
                          },
                  style: ElevatedButton.styleFrom(
                    backgroundColor: const Color(0xFF4F46E5), // indigo-600
                    foregroundColor: Colors.white,
                    padding: const EdgeInsets.symmetric(vertical: 12),
                    shape: RoundedRectangleBorder(
                      borderRadius: BorderRadius.circular(12),
                    ),
                  ),
                  child:
                      _isSavingFeedback
                          ? const SizedBox(
                            height: 20,
                            width: 20,
                            child: CircularProgressIndicator(
                              strokeWidth: 2,
                              valueColor: AlwaysStoppedAnimation<Color>(
                                Colors.white,
                              ),
                            ),
                          )
                          : const Text('Submit Rating'),
                ),
              ),
            ),
          ],
        );
      },
    );
  }

  Future<Map<String, dynamic>?> _checkExistingFeedback() async {
    try {
      final user = Supabase.instance.client.auth.currentUser;
      if (user == null) {
        return null;
      }

      // Get the job listing ID using the same logic as in the submit method
      String? jobListingId;

      if (shift!.containsKey('job_id') && shift!['job_id'] != null) {
        jobListingId = shift!['job_id'].toString();
      } else {
        jobListingId = shift!['id'] as String? ?? widget.shiftId;
      }

      if (jobListingId == null || jobListingId.isEmpty) {
        return null;
      }

      // Check for existing feedback
      final existingFeedback = await _databaseService.getUserFeedback(
        jobListingId,
        user.id,
      );

      return existingFeedback;
    } catch (e) {
      print('Error checking existing feedback: $e');
      return null;
    }
  }

  Widget _buildCard({
    required String title,
    String? subtitle,
    required Widget content,
    Widget? footer,
    EdgeInsets padding = const EdgeInsets.all(16),
  }) {
    return Container(
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(16),
        boxShadow: [
          BoxShadow(
            color: Colors.black.withOpacity(0.05),
            blurRadius: 10,
            offset: const Offset(0, 2),
          ),
        ],
        border: Border.all(color: Colors.grey.shade200),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          // Header
          Container(
            padding: const EdgeInsets.all(16),
            decoration: BoxDecoration(
              color: Colors.grey.shade50,
              borderRadius: const BorderRadius.only(
                topLeft: Radius.circular(16),
                topRight: Radius.circular(16),
              ),
              border: Border(bottom: BorderSide(color: Colors.grey.shade200)),
            ),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(
                  title,
                  style: const TextStyle(
                    fontSize: 18,
                    fontWeight: FontWeight.bold,
                    color: Color(0xFF1F2937), // slate-800
                  ),
                ),
                if (subtitle != null) ...[
                  const SizedBox(height: 4),
                  Text(
                    subtitle,
                    style: TextStyle(fontSize: 14, color: Colors.grey.shade600),
                  ),
                ],
              ],
            ),
          ),
          // Content
          if (padding != EdgeInsets.zero) ...[
            Padding(padding: padding, child: content),
          ] else ...[
            content,
          ],
          // Footer if provided
          if (footer != null) ...[
            Container(
              padding: const EdgeInsets.all(16),
              decoration: BoxDecoration(
                color: Colors.grey.shade50,
                borderRadius: const BorderRadius.only(
                  bottomLeft: Radius.circular(16),
                  bottomRight: Radius.circular(16),
                ),
                border: Border(top: BorderSide(color: Colors.grey.shade200)),
              ),
              child: footer,
            ),
          ],
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
        Icon(icon, size: 20, color: Colors.grey.shade400),
        const SizedBox(width: 12),
        Expanded(
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text(
                title,
                style: TextStyle(
                  fontWeight: FontWeight.w600,
                  color: Colors.grey.shade700,
                ),
              ),
              const SizedBox(height: 2),
              Text(value, style: TextStyle(color: Colors.grey.shade600)),
            ],
          ),
        ),
      ],
    );
  }

  Widget _buildIconHeader({required IconData icon, required String title}) {
    return Row(
      children: [
        Icon(icon, size: 20, color: Colors.grey.shade400),
        const SizedBox(width: 8),
        Text(
          title,
          style: TextStyle(
            fontWeight: FontWeight.w400,
            color: Colors.grey.shade700,
          ),
        ),
      ],
    );
  }

  Widget _buildActionButton({
    required IconData icon,
    required String label,
    required Color color,
    required VoidCallback? onPressed,
  }) {
    return OutlinedButton(
      onPressed: onPressed,
      style: OutlinedButton.styleFrom(
        padding: const EdgeInsets.symmetric(
          horizontal: 8,
          vertical: 6,
        ), // Reduced padding
        side: BorderSide(color: Colors.grey.shade300),
        shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(12)),
      ),
      child: FittedBox(
        // Force content to fit available space
        fit: BoxFit.scaleDown,
        child: Column(
          mainAxisSize: MainAxisSize.min, // Minimize height
          children: [
            Icon(
              icon,
              color: onPressed == null ? Colors.grey : color,
              size: 20,
            ), // Smaller icon
            const SizedBox(height: 2), // Reduced space
            Text(
              label,
              maxLines: 1,
              overflow: TextOverflow.ellipsis,
              style: TextStyle(
                color: onPressed == null ? Colors.grey : Colors.black87,
                fontSize: 12, // Smaller text
                height: 1.0, // Tighter line height
              ),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildTimelineItem({
    required String event,
    required String time,
    required IconData icon,
    required bool isUpcoming,
    required bool isLast,
  }) {
    return Row(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        // Timeline dot and line
        Column(
          children: [
            Container(
              width: 28,
              height: 28,
              decoration: BoxDecoration(
                color:
                    isUpcoming ? Colors.grey.shade300 : const Color(0xFF4F46E5),
                shape: BoxShape.circle,
              ),
              child: Icon(
                icon,
                size: 14,
                color: isUpcoming ? Colors.grey.shade700 : Colors.white,
              ),
            ),
            if (!isLast)
              Container(
                width: 2,
                height: 50,
                color: Colors.grey.shade300,
                margin: const EdgeInsets.only(left: 13),
              ),
          ],
        ),
        const SizedBox(width: 16),
        // Event details
        Expanded(
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text(
                event,
                style: TextStyle(
                  fontWeight: FontWeight.w500,
                  color:
                      isUpcoming
                          ? Colors.grey.shade600
                          : const Color(0xFF1F2937),
                ),
              ),
              const SizedBox(height: 4),
              Text(
                time,
                style: TextStyle(fontSize: 13, color: Colors.grey.shade500),
              ),
              SizedBox(height: isLast ? 0 : 24),
            ],
          ),
        ),
      ],
    );
  }

  Widget _buildEarningsRow({
    required String label,
    required String amount,
    bool isTotal = false,
  }) {
    return Row(
      mainAxisAlignment: MainAxisAlignment.spaceBetween,
      children: [
        Text(
          label,
          style: TextStyle(
            color: isTotal ? Colors.black87 : Colors.grey.shade600,
            fontWeight: isTotal ? FontWeight.w600 : FontWeight.normal,
          ),
        ),
        Text(
          amount,
          style: TextStyle(
            color: Colors.black87,
            fontWeight: isTotal ? FontWeight.bold : FontWeight.w500,
            fontSize: isTotal ? 18 : 15,
          ),
        ),
      ],
    );
  }

  Widget _buildPerformanceMetric({
    required String label,
    required String value,
    required int percentage,
  }) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Row(
          mainAxisAlignment: MainAxisAlignment.spaceBetween,
          children: [
            Text(
              label,
              style: TextStyle(fontSize: 14, color: Colors.grey.shade600),
            ),
            Text(
              value,
              style: const TextStyle(
                fontSize: 14,
                fontWeight: FontWeight.w500,
                color: Color(0xFF1F2937), // slate-800
              ),
            ),
          ],
        ),
        const SizedBox(height: 6),
        ClipRRect(
          borderRadius: BorderRadius.circular(2),
          child: LinearProgressIndicator(
            value: percentage / 100,
            backgroundColor: Colors.grey.shade200,
            color: const Color(0xFF4F46E5), // indigo-600
            minHeight: 8,
          ),
        ),
      ],
    );
  }

  Widget _buildEmptyState({
    required IconData icon,
    required String title,
    required String description,
  }) {
    return Container(
      padding: const EdgeInsets.all(32),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(16),
        border: Border.all(color: Colors.grey.shade200),
        boxShadow: [
          BoxShadow(
            color: Colors.black.withOpacity(0.05),
            blurRadius: 10,
            offset: const Offset(0, 2),
          ),
        ],
      ),
      child: Column(
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
            style: const TextStyle(
              fontSize: 18,
              fontWeight: FontWeight.bold,
              color: Color(0xFF1F2937), // slate-800
            ),
          ),
          const SizedBox(height: 8),
          Text(
            description,
            textAlign: TextAlign.center,
            style: TextStyle(color: Colors.grey.shade600),
          ),
        ],
      ),
    );
  }

  IconData _getIconForTimeline(String iconName) {
    switch (iconName) {
      case 'CheckSquare':
        return Icons.check_box;
      case 'Clock':
        return Icons.access_time;
      case 'CreditCard':
        return Icons.credit_card;
      default:
        return Icons.info;
    }
  }
}
