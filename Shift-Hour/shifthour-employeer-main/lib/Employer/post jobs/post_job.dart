import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'package:flutter/material.dart';
import 'package:flutter/rendering.dart';
import 'package:get/get.dart';
import 'package:get/get_core/src/get_main.dart';
import 'package:http/http.dart' as http;
import 'package:intl/intl.dart';
import 'package:shifthour_employeer/Employer/payments/employeer_payments_dashboard.dart';
import 'package:shifthour_employeer/const/Activity_log.dart';
import 'package:shifthour_employeer/services/notification.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:flutter/material.dart';
import 'package:flutter/foundation.dart' show kDebugMode;
import 'package:flutter_typeahead/flutter_typeahead.dart'; // Keep only this one

class PostShiftScreen extends StatefulWidget {
  const PostShiftScreen({Key? key, this.jobId}) : super(key: key);
  final String? jobId;
  @override
  State<PostShiftScreen> createState() => _PostShiftScreenState();
}

class _PostShiftScreenState extends State<PostShiftScreen> {
  final _formKey = GlobalKey<FormState>();
  bool _formSubmitted = false;
  final _scrollController = ScrollController();
  final Map<String, GlobalKey> _fieldKeys = {};
  final FocusNode _cityFocusNode = FocusNode();
  final FocusNode _locationFocusNode = FocusNode();
  final LayerLink _cityLayerLink = LayerLink();
  final LayerLink _locationLayerLink = LayerLink();
  OverlayEntry? _cityOverlayEntry;
  OverlayEntry? _locationOverlayEntry;
  // Form controllers
  final TextEditingController _ShiftTitleController = TextEditingController();
  final TextEditingController _companyController = TextEditingController();
  TextEditingController _locationController = TextEditingController();
  final TextEditingController _dateController = TextEditingController();
  final TextEditingController _startTimeController = TextEditingController();
  final TextEditingController _endTimeController = TextEditingController();
  final TextEditingController _payRateController = TextEditingController();
  final TextEditingController _positionsController = TextEditingController();
  final TextEditingController _supervisorNameController =
      TextEditingController();
  final TextEditingController _supervisorPhoneController =
      TextEditingController();
  final TextEditingController _supervisorEmailController =
      TextEditingController();
  final TextEditingController _ShiftPincodeController = TextEditingController();
  final TextEditingController _websiteController = TextEditingController();
  final TextEditingController _dressCodeController = TextEditingController();
  final TextEditingController _cityController = TextEditingController();
  PlaceModel? _selectedCity;
  String? selectedCategory;
  final List<String> categoryOptions = [
    'Retail Operations – Shopping Mall',
    'Warehouse & Inventory',
    'Billing & Accounting',
    'Events',
    'Others',
  ];
  final List<int> hourOptions = [
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
  ]; // Shift duration options
  int? selectedHours;
  DateTime? selectedDate;
  TimeOfDay? selectedStartTime;
  TimeOfDay? selectedEndTime;
  bool _isLoading = false;
  final TextEditingController _hourlyRateController = TextEditingController();
  bool _isEditMode = false;
  final userId = Supabase.instance.client.auth.currentUser?.id;
  PlaceModel? _selectedPlace;
  @override
  void initState() {
    super.initState();

    _fieldKeys['shiftTitle'] = GlobalKey(debugLabel: 'shiftTitle');
    _fieldKeys['company'] = GlobalKey(debugLabel: 'company');
    _fieldKeys['city'] = GlobalKey(debugLabel: 'city');
    _fieldKeys['location'] = GlobalKey(debugLabel: 'location');
    _fieldKeys['pincode'] = GlobalKey(debugLabel: 'pincode');
    _fieldKeys['date'] = GlobalKey(debugLabel: 'date');
    _fieldKeys['startTime'] = GlobalKey(debugLabel: 'startTime');
    _fieldKeys['hours'] = GlobalKey(debugLabel: 'hours');
    _fieldKeys['endTime'] = GlobalKey(debugLabel: 'endTime');
    _fieldKeys['positions'] = GlobalKey(debugLabel: 'positions');
    _fieldKeys['hourlyRate'] = GlobalKey(debugLabel: 'hourlyRate');
    _fieldKeys['totalPay'] = GlobalKey(debugLabel: 'totalPay');
    _fieldKeys['supervisorName'] = GlobalKey(debugLabel: 'supervisorName');
    _fieldKeys['supervisorPhone'] = GlobalKey(debugLabel: 'supervisorPhone');
    _fieldKeys['supervisorEmail'] = GlobalKey(debugLabel: 'supervisorEmail');

    _cityFocusNode.addListener(() {
      if (!_cityFocusNode.hasFocus) {
        _hideAllOverlays();
      }
    });

    _locationFocusNode.addListener(() {
      if (!_locationFocusNode.hasFocus) {
        _hideAllOverlays();
      }
    });
    // Check if we're in edit mode
    if (widget.jobId != null) {
      _isEditMode = true;
      _fetchJobDetails();
    }

    // Add listener to hourly rate to calculate total pay automatically
    _hourlyRateController.addListener(() {
      if (selectedHours != null) {
        final hourlyRate = double.tryParse(_hourlyRateController.text) ?? 0;
        final totalPayRate = hourlyRate * selectedHours!;
        _payRateController.text = totalPayRate.toStringAsFixed(2);
      }
    });
  }

  void dispose() {
    _scrollController.dispose();
    _ShiftTitleController.dispose();
    _companyController.dispose();
    _locationController.dispose();
    _dateController.dispose();
    _startTimeController.dispose();
    _endTimeController.dispose();
    _payRateController.dispose();
    _supervisorNameController.dispose();
    _supervisorPhoneController.dispose();
    _supervisorEmailController.dispose();
    _ShiftPincodeController.dispose();
    _websiteController.dispose();
    _dressCodeController.dispose();
    _positionsController.dispose();
    _cityController.dispose();
    _hideAllOverlays();
    super.dispose();
  }

  void _hideAllOverlays() {
    _cityOverlayEntry?.remove();
    _cityOverlayEntry = null;
    _locationOverlayEntry?.remove();
    _locationOverlayEntry = null;
  }

  // Simplified method to scroll to the first error
  Future<void> _sendShiftPostedNotification({
    required String shiftTitle,
    required String shiftId,
    required int numberOfPositions,
  }) async {
    try {
      // Use the notification service to send the notification
      await NotificationService().sendShiftPostedNotification(
        shiftTitle: shiftTitle,
        shiftId: shiftId,
        numberOfPositions: numberOfPositions,
      );
      print('Shift posted notification sent successfully');
    } catch (e) {
      print('Error sending shift posted notification: $e');
      // Don't show error to user, this is a non-critical feature
    }
  }

  void _handleParseError(String startTime, String endTime, dynamic error) {
    // Log error details for debugging
    print(
      'Error parsing job times - start: $startTime, end: $endTime, error: $error',
    );

    // Show user-friendly error message
    final errorMessage =
        'The start time or end time for this job appears to be in an '
        'invalid format. Please check the job data to ensure the times '
        'are entered correctly in HH:MM format.';
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        content: Text(errorMessage),
        backgroundColor: Colors.red,
        duration: Duration(seconds: 30),
        action: SnackBarAction(label: 'Retry', onPressed: _fetchJobDetails),
      ),
    );

    // Populate with raw values as fallback
    _startTimeController.text = startTime;
    _endTimeController.text = endTime;
  }

  Future<void> _fetchJobDetails() async {
    setState(() {
      _isLoading = true;
    });

    try {
      final supabase = Supabase.instance.client;
      final response =
          await supabase
              .from('worker_job_listings')
              .select('*')
              .eq('shift_id', widget.jobId as Object)
              .single();

      // Populate form controllers with fetched data
      _ShiftTitleController.text = response['job_title'] ?? '';
      _companyController.text = response['company'] ?? '';
      _locationController.text = response['location'] ?? '';
      _dateController.text = response['date'] ?? '';
      _supervisorNameController.text = response['supervisor_name'] ?? '';
      _cityController.text = response['city'] ?? '';
      selectedHours =
          response['duration'] != null
              ? (response['duration'] as num).toInt()
              : null;

      if (response['start_time'] != null) {
        final timeParts = response['start_time'].split(':');
        selectedStartTime = TimeOfDay(
          hour: int.parse(timeParts[0]),
          minute: int.parse(timeParts[1]),
        );
      }

      _supervisorPhoneController.text = response['supervisor_phone'] ?? '';
      _supervisorEmailController.text = response['supervisor_email'] ?? '';
      _ShiftPincodeController.text =
          (response['job_pincode']?.toString() ?? '');
      _websiteController.text = response['website'] ?? '';
      _dressCodeController.text = response['dress_code'] ?? '';
      String fetchedCategory = response['category'] ?? '';
      selectedCategory =
          categoryOptions.contains(fetchedCategory)
              ? fetchedCategory
              : categoryOptions.last;

      // Set selected hours if available
      try {
        if (response['start_time'] != null && response['end_time'] != null) {
          // Parse times in 24-hour format (HH:MM:SS)
          final startTime = TimeOfDay.fromDateTime(
            DateFormat.Hms().parse(response['start_time']),
          );
          final endTime = TimeOfDay.fromDateTime(
            DateFormat.Hms().parse(response['end_time']),
          );

          // Calculate hours
          final startMinutes = startTime.hour * 60 + startTime.minute;
          final endMinutes = endTime.hour * 60 + endTime.minute;
          final hoursDiff = (endMinutes - startMinutes) / 60;

          selectedHours =
              hourOptions.contains(hoursDiff.round())
                  ? hoursDiff.round()
                  : null;

          // Set start and end time controllers
          _startTimeController.text = startTime.format(context);
          _endTimeController.text = endTime.format(context);
        }
      } catch (formatError) {
        _handleParseError(
          response['start_time'] ?? '',
          response['end_time'] ?? '',
          formatError,
        );
      }

      // Update pay rate and positions
      if (selectedHours != null && response['pay_rate'] != null) {
        final totalPay =
            double.tryParse(response['pay_rate'].toString()) ?? 0.0;
        if (selectedHours! > 0) {
          // Calculate hourly rate from total pay
          final hourlyRate = totalPay / selectedHours!;
          _hourlyRateController.text = hourlyRate.toStringAsFixed(2);
        }
      }

      _positionsController.text =
          (response['number_of_positions']?.toString() ?? '1');

      // Update state
      setState(() {
        _isLoading = false;
      });
    } catch (e) {
      // Handle fetch error
      print('Error fetching job details: $e');
      setState(() {
        _isLoading = false;
      });
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('Failed to load job details. Please try again later.'),
          backgroundColor: Colors.red,
        ),
      );
    }
  } // Time format validation method

  Widget _buildCategoryDropdown() {
    return Padding(
      key: _fieldKeys['category'],
      padding: const EdgeInsets.only(bottom: 20),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Text(
            'Category',
            style: TextStyle(
              fontSize: 14,
              fontWeight: FontWeight.w600,
              color: Colors.black87,
              fontFamily: 'Inter',
            ),
          ),
          const SizedBox(height: 8),
          DropdownButtonFormField<String>(
            isExpanded: true,
            value: selectedCategory,
            decoration: InputDecoration(
              hintText: 'Select Category',
              prefixIcon: Icon(
                Icons.category_outlined,
                color: Color(0xFF5B6BF8),
              ),
              border: OutlineInputBorder(
                borderRadius: BorderRadius.circular(10),
                borderSide: BorderSide(color: Colors.grey.shade300),
              ),
              enabledBorder: OutlineInputBorder(
                borderRadius: BorderRadius.circular(10),
                borderSide: BorderSide(color: Colors.grey.shade300),
              ),
              focusedBorder: OutlineInputBorder(
                borderRadius: BorderRadius.circular(10),
                borderSide: const BorderSide(color: Color(0xFF5B6BF8)),
              ),
              filled: true,
              fillColor: Colors.white,
              contentPadding: const EdgeInsets.symmetric(
                horizontal: 16,
                vertical: 12,
              ),
            ),
            items:
                categoryOptions.map((category) {
                  return DropdownMenuItem<String>(
                    value: category,
                    child: Text(category),
                  );
                }).toList(),
            onChanged: (value) {
              setState(() {
                selectedCategory = value;
              });
            },
            validator: (value) {
              if (value == null || value.isEmpty) {
                return 'Please select a category';
              }
              return null;
            },
          ),
        ],
      ),
    );
  }

  bool _isValidTimeFormat(String time) {
    // Example simple format check: HH:MM AM/PM
    return RegExp(r'^\d{1,2}:\d{2} (AM|PM)$').hasMatch(time);
  }

  Future<void> _selectDate(BuildContext context) async {
    final DateTime today = DateTime.now();

    // Set the initial date to display in the picker
    // If we already have a selected date, use that, otherwise use today
    final initialDate = selectedDate ?? today;

    final DateTime? picked = await showDatePicker(
      context: context,
      initialDate: initialDate,
      firstDate: today, // This ensures we can only select today or future dates
      lastDate: DateTime(2030),
      builder: (context, child) {
        return Theme(
          data: Theme.of(context).copyWith(
            colorScheme: const ColorScheme.light(primary: Color(0xFF5B6BF8)),
          ),
          child: child!,
        );
      },
    );

    if (picked != null && picked != selectedDate) {
      setState(() {
        selectedDate = picked;
        _dateController.text = DateFormat('yyyy-MM-dd').format(picked);

        // If date changes, we should reset time-related fields
        // (especially important if changing from a future date to today)
        selectedStartTime = null;
        selectedEndTime = null;
        selectedHours = null;
        _startTimeController.clear();
        _endTimeController.clear();
      });
    }
  }

  void _selectStartTime(BuildContext context) async {
    // Get the current time
    final TimeOfDay currentTime = TimeOfDay.now();

    // Check if the selected date is today
    final bool isToday =
        selectedDate != null &&
        DateTime(selectedDate!.year, selectedDate!.month, selectedDate!.day)
                .difference(
                  DateTime(
                    DateTime.now().year,
                    DateTime.now().month,
                    DateTime.now().day,
                  ),
                )
                .inDays ==
            0;

    // If today is selected, we'll use a custom time picker dialog
    if (isToday) {
      // Show a custom time picker dialog
      final TimeOfDay? picked = await _showCustomTimePicker(
        context,
        currentTime,
      );

      if (picked != null && picked != selectedStartTime) {
        setState(() {
          selectedStartTime = picked;
          _startTimeController.text = picked.format(context);

          // Reset end time and hours when start time changes
          selectedEndTime = null;
          _endTimeController.clear();
          selectedHours = null;
        });
      }
    } else {
      // For future dates, use the standard time picker
      final initialTime = selectedStartTime ?? TimeOfDay(hour: 9, minute: 0);

      final TimeOfDay? picked = await showTimePicker(
        context: context,
        initialTime: initialTime,
        builder: (context, child) {
          return Theme(
            data: Theme.of(context).copyWith(
              colorScheme: const ColorScheme.light(primary: Color(0xFF5B6BF8)),
            ),
            child: child!,
          );
        },
      );

      if (picked != null && picked != selectedStartTime) {
        setState(() {
          selectedStartTime = picked;
          _startTimeController.text = picked.format(context);

          // Reset end time and hours when start time changes
          selectedEndTime = null;
          _endTimeController.clear();
          selectedHours = null;
        });
      }
    }
  }

  // Custom time picker dialog that only shows future times for today
  Future<TimeOfDay?> _showCustomTimePicker(
    BuildContext context,
    TimeOfDay currentTime,
  ) async {
    // Calculate the current time in minutes since midnight
    final int currentMinutes = currentTime.hour * 60 + currentTime.minute;

    // Round up to the nearest 5 minutes and add 5 more minutes
    // to ensure we start with a time definitely in the future
    int suggestedMinutes = ((currentMinutes + 4) ~/ 5) * 5 + 5;
    int suggestedHour = suggestedMinutes ~/ 60;
    int suggestedMinute = suggestedMinutes % 60;

    // Handle day overflow
    if (suggestedHour >= 24) {
      suggestedHour = suggestedHour % 24;
    }

    TimeOfDay suggestedTime = TimeOfDay(
      hour: suggestedHour,
      minute: suggestedMinute,
    );

    // Create a list of available time slots (every 15 minutes) starting from the suggested time
    List<TimeOfDay> availableTimes = [];

    // Generate times from suggested time until midnight
    for (int minutes = suggestedMinutes; minutes < 24 * 60; minutes += 5) {
      int hour = minutes ~/ 60;
      int minute = minutes % 60;
      availableTimes.add(TimeOfDay(hour: hour, minute: minute));
    }

    // Show a bottom sheet with the available times
    return showModalBottomSheet<TimeOfDay>(
      context: context,
      shape: RoundedRectangleBorder(
        borderRadius: BorderRadius.vertical(top: Radius.circular(16)),
      ),
      builder: (context) {
        return Container(
          height: MediaQuery.of(context).size.height * 0.6,
          padding: EdgeInsets.symmetric(vertical: 16),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Padding(
                padding: EdgeInsets.symmetric(horizontal: 16, vertical: 8),
                child: Row(
                  children: [
                    Icon(Icons.access_time, color: Color(0xFF5B6BF8)),
                    SizedBox(width: 12),
                    Text(
                      'Select a future time',
                      style: TextStyle(
                        fontSize: 18,
                        fontWeight: FontWeight.bold,
                        fontFamily: 'Inter',
                      ),
                    ),
                    Spacer(),
                    IconButton(
                      icon: Icon(Icons.close),
                      onPressed: () => Navigator.pop(context),
                    ),
                  ],
                ),
              ),
              Divider(),
              Padding(
                padding: EdgeInsets.symmetric(horizontal: 16, vertical: 8),
                child: Text(
                  'Current time: ${currentTime.format(context)}',
                  style: TextStyle(
                    fontSize: 14,
                    color: Colors.grey[600],
                    fontStyle: FontStyle.italic,
                  ),
                ),
              ),
              Expanded(
                child: ListView.builder(
                  itemCount: availableTimes.length,
                  itemBuilder: (context, index) {
                    final time = availableTimes[index];
                    return ListTile(
                      leading: Icon(Icons.schedule, color: Color(0xFF5B6BF8)),
                      title: Text(
                        time.format(context),
                        style: TextStyle(fontSize: 16, fontFamily: 'Inter'),
                      ),
                      onTap: () => Navigator.pop(context, time),
                    );
                  },
                ),
              ),
            ],
          ),
        );
      },
    );
  }

  void _calculateEndTime() {
    if (selectedStartTime != null && selectedHours != null) {
      // Convert start time to minutes
      int startMinutes =
          selectedStartTime!.hour * 60 + selectedStartTime!.minute;

      // Add selected hours
      int endMinutes = startMinutes + (selectedHours! * 60);

      // Convert back to TimeOfDay
      int endHour = endMinutes ~/ 60;
      int endMinute = endMinutes % 60;

      // Adjust for day overflow
      endHour = endHour % 24;

      final endTime = TimeOfDay(hour: endHour, minute: endMinute);

      setState(() {
        selectedEndTime = endTime;
        _endTimeController.text = endTime.format(context);
      });
    }
  }

  Widget _buildTimeSelectionRow() {
    return LayoutBuilder(
      builder: (context, constraints) {
        // Check if we have enough width for a row layout
        if (constraints.maxWidth > 600) {
          return Row(
            children: [
              // Start Time
              Expanded(
                child: _buildTextField(
                  controller: _startTimeController,
                  label: 'Start Time',
                  hint: 'Select',
                  icon: Icons.access_time,
                  readOnly: true,
                  onTap: () => _selectStartTime(context),
                  validator: (value) {
                    if (value == null || value.isEmpty) {
                      return 'Required';
                    }
                    return null;
                  },
                  fieldKey: 'startTime',
                ),
              ),
              const SizedBox(width: 16),
              // Hours Dropdown
              Expanded(child: _buildHoursDropdown()),
              const SizedBox(width: 16),
              // End Time
              Expanded(
                child: _buildTextField(
                  controller: _endTimeController,
                  label: 'End Time',
                  hint: 'Calculated',
                  icon: Icons.access_time,
                  readOnly: true,
                  validator: (value) {
                    if (value == null || value.isEmpty) {
                      return 'Required';
                    }
                    return null;
                  },
                  fieldKey: 'endTime',
                ),
              ),
            ],
          );
        } else {
          // Vertical layout for smaller screens
          return Column(
            children: [
              // Start Time
              _buildTextField(
                controller: _startTimeController,
                label: 'Start Time',
                hint: 'Select',
                icon: Icons.access_time,
                readOnly: true,
                onTap: () => _selectStartTime(context),
                validator: (value) {
                  if (value == null || value.isEmpty) {
                    return 'Required';
                  }
                  return null;
                },
                fieldKey: 'startTime',
              ),
              const SizedBox(height: 16),
              // Hours Dropdown
              _buildHoursDropdown(),
              const SizedBox(height: 16),
              // End Time
              _buildTextField(
                controller: _endTimeController,
                label: 'End Time',
                hint: 'Calculated',
                icon: Icons.access_time,
                readOnly: true,
                validator: (value) {
                  if (value == null || value.isEmpty) {
                    return 'Required';
                  }
                  return null;
                },
              ),
            ],
          );
        }
      },
    );
  }

  Widget _buildHoursDropdown() {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(
          'Shift Duration',
          style: TextStyle(
            fontSize: 14,
            fontWeight: FontWeight.w600,
            color: Colors.black87,
            fontFamily: 'Inter',
          ),
        ),
        const SizedBox(height: 8),
        DropdownButtonFormField<int>(
          isExpanded: true,
          value: selectedHours,
          decoration: InputDecoration(
            hintText:
                selectedStartTime == null
                    ? 'Start Time Required'
                    : 'Select Hours',
            hintStyle: TextStyle(
              color: selectedStartTime == null ? Colors.red : Colors.grey,
            ),
            prefixIcon: Icon(
              Icons.timer_outlined,
              color: selectedStartTime == null ? Colors.red : Color(0xFF5B6BF8),
            ),
            border: OutlineInputBorder(
              borderRadius: BorderRadius.circular(10),
              borderSide: BorderSide(
                color:
                    selectedStartTime == null
                        ? Colors.red
                        : Colors.grey.shade300,
              ),
            ),
            enabledBorder: OutlineInputBorder(
              borderRadius: BorderRadius.circular(10),
              borderSide: BorderSide(
                color:
                    selectedStartTime == null
                        ? Colors.red
                        : Colors.grey.shade300,
              ),
            ),
            focusedBorder: OutlineInputBorder(
              borderRadius: BorderRadius.circular(10),
              borderSide: const BorderSide(color: Color(0xFF5B6BF8)),
            ),
            filled: true,
            fillColor: Colors.white,
            contentPadding: const EdgeInsets.symmetric(
              horizontal: 16,
              vertical: 12,
            ),
          ),
          items:
              selectedStartTime == null
                  ? []
                  : hourOptions.map((hours) {
                    return DropdownMenuItem<int>(
                      value: hours,
                      child: Text('$hours Hours'),
                    );
                  }).toList(),
          onChanged:
              selectedStartTime == null
                  ? null
                  : (value) {
                    setState(() {
                      selectedHours = value;
                      // Clear previous pay rate when hours change
                      _payRateController.clear();
                      _hourlyRateController.clear();
                    });

                    _calculateEndTime();
                  },
          validator: (value) {
            if (selectedStartTime == null) {
              return 'Select start time first';
            }
            if (value == null) {
              return 'Select shift duration';
            }
            return null;
          },
        ),
      ],
    );
  }

  Future<void> _selectEndTime(BuildContext context) async {
    final TimeOfDay? picked = await showTimePicker(
      context: context,
      initialTime: selectedEndTime ?? TimeOfDay.now(),
      builder: (context, child) {
        return Theme(
          data: Theme.of(context).copyWith(
            colorScheme: const ColorScheme.light(primary: Color(0xFF5B6BF8)),
          ),
          child: child!,
        );
      },
    );

    if (picked != null && picked != selectedEndTime) {
      setState(() {
        selectedEndTime = picked;
        _endTimeController.text = picked.format(context);
      });
    }
  }

  Future<String> generateSequentialShiftId() async {
    final supabase = Supabase.instance.client;

    final response = await supabase.rpc('increment_shift_counter');

    if (response == null || response is! int) {
      throw Exception('Failed to generate shift ID');
    }

    final padded = response.toString().padLeft(5, '0');
    return 'SH-$padded';
  }

  Future<Map<String, String>> _getOrCreateEmployerInfo() async {
    final supabase = Supabase.instance.client;
    final currentUser = supabase.auth.currentUser;

    if (currentUser == null) {
      throw Exception('User is not logged in');
    }

    try {
      // First, try to find existing employer record by email
      final existingEmployer =
          await supabase
              .from('employers')
              .select('id, contact_name') // Select both id and contact_name
              .eq('contact_email', currentUser.email as Object)
              .maybeSingle();

      // If employer exists, return its ID and contact_name
      if (existingEmployer != null) {
        return {
          'id': existingEmployer['id'],
          'contactName': existingEmployer['contact_name'],
        };
      }

      // If no employer record exists, create a new one
      final nowIso = DateTime.now().toIso8601String();
      final contactName = currentUser.email?.split('@').first ?? 'Unknown';
      final newEmployer =
          await supabase
              .from('employers')
              .insert({
                'contact_email': currentUser.email,
                'contact_name': contactName,
                'company_name':
                    _companyController.text.isNotEmpty
                        ? _companyController.text
                        : 'My Company',
                'created_at': nowIso,
                'updated_at': nowIso,
                // Add other necessary fields with default or collected values
              })
              .select('id, contact_name') // Select both fields
              .single();

      // After creating employer, also create a wallet
      try {
        // Try using RPC function to create wallet
        await supabase.rpc(
          'create_employer_wallet',
          params: {'p_employer_id': newEmployer['id']},
        );
      } catch (rpcError) {
        print('RPC error, creating wallet directly: $rpcError');

        // Fallback: Create wallet directly if RPC fails
        await supabase.from('employer_wallet').insert({
          'employer_id': newEmployer['id'],
          'balance': 0,
          'currency': 'INR',
          'created_at': nowIso,
          'last_updated': nowIso,
        });
      }

      return {
        'id': newEmployer['id'],
        'contactName': newEmployer['contact_name'],
      };
    } catch (e) {
      print('Error finding/creating employer record: $e');
      rethrow;
    }
  }

  DateTime _parseTimeWithFormat(DateTime baseDate, String timeString) {
    try {
      // Clean up the time string
      timeString = timeString.trim().toUpperCase();

      int hour = 0;
      int minute = 0;

      if (timeString.contains('AM') || timeString.contains('PM')) {
        // Handle 12-hour format with AM/PM
        final parts = timeString.split(' ');
        final timePart = parts[0];
        final amPm = parts[1];

        final timeSplit = timePart.split(':');
        hour = int.parse(timeSplit[0]);
        minute = int.parse(timeSplit[1]);

        // Convert to 24-hour format
        if (amPm == 'PM' && hour < 12) {
          hour += 12;
        } else if (amPm == 'AM' && hour == 12) {
          hour = 0;
        }
      } else {
        // Handle 24-hour format
        final timeSplit = timeString.split(':');
        hour = int.parse(timeSplit[0]);
        minute = int.parse(timeSplit[1]);
      }

      // Create a DateTime with the job date and parsed time
      return DateTime(
        baseDate.year,
        baseDate.month,
        baseDate.day,
        hour,
        minute,
      );
    } catch (e) {
      print('Error parsing time: $e for time string: $timeString');
      throw FormatException('Could not parse time: $timeString');
    }
  }

  void _postJob() async {
    setState(() {
      _formSubmitted = true;
    });

    if (_locationController.text.trim().isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('Please enter a valid location'),
          backgroundColor: Colors.red,
        ),
      );
      //  _scrollToFirstError();
      //return;
    }

    if (_formKey.currentState!.validate()) {
      setState(() {
        _isLoading = true;
      });

      try {
        print('Inserting job with user_id: $userId');

        final supabase = Supabase.instance.client;
        final currentUser = supabase.auth.currentUser;

        if (currentUser == null) {
          throw Exception('User is not logged in');
        }

        final employerInfo = await _getOrCreateEmployerInfo();
        final String employerId = employerInfo['id']!;
        final String contactName = employerInfo['contactName']!;
        final jobDate = DateTime.parse(_dateController.text);

        // Use the robust parser for both start and end times
        final startDateTime = _parseTimeWithFormat(
          jobDate,
          _startTimeController.text,
        );
        final endDateTime = _parseTimeWithFormat(
          jobDate,
          _endTimeController.text,
        );

        // If end time is earlier than start time, it means the shift ends the next day
        final adjustedEndDateTime =
            endDateTime.isBefore(startDateTime)
                ? endDateTime.add(Duration(days: 1))
                : endDateTime;

        // Format as ISO strings for database
        final startDateTimeString = startDateTime.toIso8601String();
        final endDateTimeString = adjustedEndDateTime.toIso8601String();

        print('Calculated start date time: $startDateTimeString');
        print('Calculated end date time: $endDateTimeString');

        // Continue with your existing code...

        if (_isEditMode && widget.jobId != null) {
          // Get the current job data to compare amounts
          final currentJobData =
              await supabase
                  .from('worker_job_listings')
                  .select('pay_rate, number_of_positions')
                  .eq('shift_id', widget.jobId as Object)
                  .eq('user_id', userId.toString())
                  .single();

          final double currentTotalAmount =
              (currentJobData['pay_rate'] as num).toDouble() *
              (currentJobData['number_of_positions'] as int);

          final double newPayRate = double.parse(_payRateController.text);
          final int newPositions = int.parse(_positionsController.text);
          final double newTotalAmount = newPayRate * newPositions;

          final double amountDifference = newTotalAmount - currentTotalAmount;

          // If amount increased, need to check wallet balance and deduct
          if (amountDifference > 0) {
            // Check wallet balance
            final walletData =
                await supabase
                    .from('employer_wallet')
                    .select('id, balance, currency')
                    .eq('employer_id', employerId)
                    .maybeSingle();

            if (walletData == null) {
              _showNoWalletDialog();
              return;
            }

            // Parse balance
            final dynamic balanceValue = walletData['balance'];
            double walletBalance = 0.0;

            if (balanceValue is int) {
              walletBalance = balanceValue.toDouble();
            } else if (balanceValue is double) {
              walletBalance = balanceValue;
            } else if (balanceValue is String) {
              walletBalance = double.tryParse(balanceValue) ?? 0.0;
            }

            // Check if there's enough balance for the increase
            if (walletBalance < amountDifference) {
              _showInsufficientBalanceDialog(amountDifference, walletBalance);
              return;
            }

            // Show confirmation dialog for balance deduction
            final bool shouldProceed =
                await _showEditDeductionConfirmationDialog(
                  currentTotalAmount,
                  newTotalAmount,
                  amountDifference,
                );

            if (!shouldProceed) {
              setState(() {
                _isLoading = false;
              });
              return;
            }

            // Deduct the difference from wallet
            try {
              final newBalance = walletBalance - amountDifference;
              final timestamp = DateTime.now().toIso8601String();

              // Update wallet balance
              await supabase
                  .from('employer_wallet')
                  .update({'balance': newBalance, 'last_updated': timestamp})
                  .eq('id', walletData['id']);

              final user = supabase.auth.currentUser;
              // Record the transaction
              await supabase.from('wallet_transactions').insert({
                'user_id': user!.id,
                'wallet_id': walletData['id'],
                'amount': -amountDifference,
                'transaction_type': 'adjustment',
                'description':
                    'Additional amount for editing shift ${widget.jobId} - increased from ₹$currentTotalAmount to ₹$newTotalAmount',
                'reference_id': _generateUuid(),
                'created_at': timestamp,
                'status': 'completed',
              });

              print('Additional amount deducted from wallet for job edit');
            } catch (e) {
              print('Error in wallet update during edit: $e');
              throw e;
            }
          } else if (amountDifference < 0) {
            // If amount decreased, refund the difference
            final walletData =
                await supabase
                    .from('employer_wallet')
                    .select('id, balance, currency')
                    .eq('employer_id', employerId)
                    .single();

            try {
              final dynamic balanceValue = walletData['balance'];
              double walletBalance = 0.0;

              if (balanceValue is int) {
                walletBalance = balanceValue.toDouble();
              } else if (balanceValue is double) {
                walletBalance = balanceValue;
              } else if (balanceValue is String) {
                walletBalance = double.tryParse(balanceValue) ?? 0.0;
              }

              final newBalance =
                  walletBalance +
                  (-amountDifference); // Add back the difference
              final timestamp = DateTime.now().toIso8601String();

              // Update wallet balance
              await supabase
                  .from('employer_wallet')
                  .update({'balance': newBalance, 'last_updated': timestamp})
                  .eq('id', walletData['id']);
              final user = supabase.auth.currentUser;
              // Record the refund transaction
              await supabase.from('wallet_transactions').insert({
                'user_id': user!.id,
                'wallet_id': walletData['id'],
                'amount':
                    -amountDifference, // Positive amount since we're adding back
                'transaction_type': 'refund',
                'description':
                    'Refund for editing shift ${widget.jobId} - reduced from ₹$currentTotalAmount to ₹$newTotalAmount',
                'reference_id': _generateUuid(),
                'created_at': timestamp,
                'status': 'completed',
              });

              print('Amount refunded to wallet for job edit');
            } catch (e) {
              print('Error in wallet refund during edit: $e');
              throw e;
            }
          }
          print('Inserting job with user_id: $userId');

          // Continue with the normal update...
          final jobData = {
            'user_id': userId,
            'job_title': _ShiftTitleController.text,
            'company': _companyController.text,
            'location':
                _locationController.text.isNotEmpty
                    ? _locationController.text
                    : 'Unspecified Location',
            'date': _dateController.text,
            'start_time': _startTimeController.text,
            'end_time': _endTimeController.text,
            'pay_rate': newPayRate, // Use newPayRate instead of parsing again
            'pay_currency': 'INR',
            'number_of_positions':
                newPositions, // Use newPositions instead of parsing again
            'supervisor_name': _supervisorNameController.text,
            'supervisor_phone': _supervisorPhoneController.text,
            'supervisor_email': _supervisorEmailController.text,
            'job_pincode': int.parse(_ShiftPincodeController.text),
            'website':
                _websiteController.text.isEmpty
                    ? null
                    : _websiteController.text,
            'dress_code': _dressCodeController.text,
            'updated_at': DateTime.now().toIso8601String(),
            'category': selectedCategory ?? 'Other',
            'user_id': employerId,
            'city':
                _cityController.text.isNotEmpty
                    ? _cityController.text
                    : 'Unspecified Location',
            'contact_email': currentUser.email,
            'contact_name': contactName,
            'start_date_time': startDateTimeString,
            'end_date_time': endDateTimeString,
            'duration': selectedHours,
          };

          await supabase
              .from('worker_job_listings')
              .update(jobData)
              .eq('shift_id', widget.jobId as Object);

          ScaffoldMessenger.of(context).showSnackBar(
            SnackBar(
              content: Text(
                amountDifference > 0
                    ? 'Shift updated successfully! ₹${amountDifference.toStringAsFixed(2)} deducted from wallet.'
                    : amountDifference < 0
                    ? 'Shift updated successfully! ₹${(-amountDifference).toStringAsFixed(2)} refunded to wallet.'
                    : 'Shift updated successfully!',
              ),
              backgroundColor: Colors.green,
            ),
          );

          Navigator.pop(context, true);
          return;
        }

        // For new job posting
        // Get the number of positions and calculate required balance
        final int numberOfPositions = int.parse(_positionsController.text);
        final double requiredBalance =
            numberOfPositions *
            (double.tryParse(_payRateController.text) ?? 0.0);

        // Check wallet balance
        final walletData =
            await supabase
                .from('employer_wallet')
                .select('id, balance, currency')
                .eq('employer_id', employerId)
                .maybeSingle();

        if (walletData == null) {
          // No wallet exists
          _showNoWalletDialog();
          return;
        }

        // Parse balance (handling different types)
        final dynamic balanceValue = walletData['balance'];
        double walletBalance = 0.0;

        if (balanceValue is int) {
          walletBalance = balanceValue.toDouble();
        } else if (balanceValue is double) {
          walletBalance = balanceValue;
        } else if (balanceValue is String) {
          walletBalance = double.tryParse(balanceValue) ?? 0.0;
        }

        // Check if there's enough balance
        if (walletBalance < requiredBalance) {
          _showInsufficientBalanceDialog(requiredBalance, walletBalance);
          return;
        }

        // Show confirmation dialog for balance deduction
        final bool shouldProceed = await _showDeductionConfirmationDialog(
          requiredBalance,
        );

        if (!shouldProceed) {
          setState(() {
            _isLoading = false;
          });
          return;
        }

        // Create a batch ID to group related positions
        final batchId = 'B-${DateTime.now().millisecondsSinceEpoch}';
        final List<String> createdShiftIds = [];

        // Update the wallet first
        try {
          // SIMPLEST APPROACH - JUST DEDUCT THE BALANCE
          final newBalance = walletBalance - requiredBalance;
          final timestamp = DateTime.now().toIso8601String();

          // Update wallet balance only
          await supabase
              .from('employer_wallet')
              .update({'balance': newBalance, 'last_updated': timestamp})
              .eq('id', walletData['id']);
          final user = supabase.auth.currentUser;
          // Record the transaction
          await supabase.from('wallet_transactions').insert({
            'user_id': user!.id,
            'wallet_id': walletData['id'],
            'amount': -requiredBalance,
            'transaction_type': 'hold',
            'description':
                'Amount debited for posting $numberOfPositions position(s) for ${_ShiftTitleController.text}',
            'reference_id':
                _generateUuid(), // Generate a new UUID for reference_id
            'created_at': timestamp,
            'status': 'completed',
          });

          print('Amount deducted from wallet for job posting');
        } catch (e) {
          print('Error in wallet update: $e');
          throw e; // Re-throw to be caught by the outer catch
        }
        print('Job Title: ${_ShiftTitleController.text}');
        print('Company: ${_companyController.text}');
        print('Location: ${_locationController.text}');
        print('Date: ${_dateController.text}');
        print('Start Time: ${_startTimeController.text}');
        print('End Time: ${_endTimeController.text}');
        print('Pay Rate: ${_payRateController.text}');
        print('Number of Positions: $numberOfPositions');
        // Create a batch ID to group related positions

        // FIXED: Use only one loop to create job listings and shift holds
        for (int i = 0; i < numberOfPositions; i++) {
          // Generate a unique custom job ID for each position
          final shiftId = await generateSequentialShiftId();

          // Prepare job data with the custom ID in shift_id
          final jobData = {
            'user_id': userId,
            'shift_id': shiftId,
            'batch_id': batchId,
            'position_number': i + 1, // Track position number within batch
            'job_title': _ShiftTitleController.text,
            'company': _companyController.text,
            'number_of_positions': 1, // Each record represents 1 position
            'location':
                _locationController.text.isNotEmpty
                    ? _locationController.text
                    : 'Unspecified Location',
            'date': _dateController.text,
            'start_time': _startTimeController.text,
            'end_time': _endTimeController.text,
            'pay_rate': double.parse(_payRateController.text),
            'pay_currency': 'INR',
            'supervisor_name': _supervisorNameController.text,
            'supervisor_phone': _supervisorPhoneController.text,
            'supervisor_email': _supervisorEmailController.text,
            'job_pincode': int.parse(_ShiftPincodeController.text),
            'website':
                _websiteController.text.isEmpty
                    ? null
                    : _websiteController.text,
            'dress_code': _dressCodeController.text,
            'status': 'Active',
            'created_at': DateTime.now().toIso8601String(),
            'updated_at': DateTime.now().toIso8601String(),
            'category': selectedCategory ?? 'Other',
            'user_id': employerId,
            'city':
                _cityController.text.isNotEmpty
                    ? _cityController.text
                    : 'Unspecified Location',
            'contact_email': currentUser.email,
            'contact_name': contactName,
            'start_date_time': startDateTimeString, // Add this new field
            'end_date_time': endDateTimeString,
            'duration': selectedHours,
          };

          // Insert the job listing with our custom shift_id
          final jobResponse =
              await supabase
                  .from('worker_job_listings')
                  .insert(jobData)
                  .select('id, shift_id')
                  .single();

          print('Job Insertion Response:');
          print('Job ID: ${jobResponse['id']}');
          print('Shift ID: ${jobResponse['shift_id']}');

          print(
            'Job created with ID: ${jobResponse['id']} and shift_id: ${jobResponse['shift_id']}',
          );

          // Now create the shift hold record using the same customJobId
          try {
            // First, convert the customJobId to a UUID
            final shiftUuid = _generateUuid();

            // Create mapping between custom ID and UUID
            await supabase.from('shift_id_mapping').insert({
              'custom_id': shiftId,
              'uuid': shiftUuid,
              'created_at': DateTime.now().toIso8601String(),
            });

            // Create shift hold using UUID
            await supabase.from('shift_holds').insert({
              'employer_id': employerId,
              'shift_id': shiftUuid, // Use UUID here, not customJobId
              'amount': double.parse(_payRateController.text),
              'status': 'active',
              'created_at': DateTime.now().toIso8601String(),
            });

            print(
              'Shift hold created for shift ID: $shiftId with UUID: $shiftUuid',
            );
          } catch (e) {
            print('Error creating shift hold: $e');
            // Continue with the loop even if the hold creation fails
          }

          createdShiftIds.add(jobResponse['shift_id']);
        }

        // Log activity
        await ActivityLogger.logJobPosting({
          'job_title': _ShiftTitleController.text,
          'company': _companyController.text,
          'location': _locationController.text,
          'pay_rate': double.parse(_payRateController.text),
          'number_of_positions': numberOfPositions,
        });

        // Add this code to send notification:
        for (final shiftId in createdShiftIds) {
          await _sendShiftPostedNotification(
            shiftTitle: _ShiftTitleController.text,
            shiftId: shiftId,
            numberOfPositions: 1, // one per shift
          );
        }

        showSuccessSnackBar(
          context,
          numberOfPositions: numberOfPositions,

          requiredBalance: requiredBalance,
        );

        Navigator.pop(context, true);
      } catch (e) {
        // _scrollToFirstError();
        // Show error message
        if (mounted) {
          ScaffoldMessenger.of(context).showSnackBar(
            SnackBar(
              content: Text(
                'Error ${_isEditMode ? 'updating' : 'posting'} job: $e',
              ),
              backgroundColor: Colors.red,
            ),
          );
        }
        print('Error in job ${_isEditMode ? 'update' : 'posting'} process: $e');
      } finally {
        if (mounted) {
          setState(() {
            _isLoading = false;
          });
        }
      }
    }
  }

  // UUID generator function
  String _generateUuid() {
    // Simple UUID v4 generator
    final random = Random();
    final hexDigits = '0123456789abcdef';
    final uuid = List<String>.filled(36, '');

    for (var i = 0; i < 36; i++) {
      if (i == 8 || i == 13 || i == 18 || i == 23) {
        uuid[i] = '-';
      } else if (i == 14) {
        uuid[i] = '4'; // Version 4 UUID
      } else if (i == 19) {
        uuid[i] = hexDigits[(random.nextInt(4) | 8)]; // Variant
      } else {
        uuid[i] = hexDigits[random.nextInt(16)];
      }
    }

    return uuid.join('');
  }
  // Add this UUID generator function if you don't already have one

  void _scrollToFirstError() {
    // Create a list of all the GlobalKeys we want to check
    final fieldsToCheck = [
      _fieldKeys['shiftTitle'],
      _fieldKeys['company'],
      _fieldKeys['location'],
      _fieldKeys['pincode'],
      _fieldKeys['date'],
      _fieldKeys['startTime'],
      _fieldKeys['hours'],
      _fieldKeys['endTime'],
      _fieldKeys['positions'],
      _fieldKeys['hourlyRate'],
      _fieldKeys['supervisorName'],
      _fieldKeys['supervisorPhone'],
    ];

    // Find the first field with an error
    for (final key in fieldsToCheck) {
      if (key?.currentContext != null) {
        // Check if the field has a validation error
        final renderBox = key!.currentContext!.findRenderObject() as RenderBox?;
        if (renderBox != null) {
          // Calculate the position of the field
          final position = renderBox.localToGlobal(Offset.zero);
          final scrollOffset =
              position.dy -
              MediaQuery.of(context).padding.top -
              AppBar().preferredSize.height -
              100; // Additional offset to ensure the field is not at the very top

          // Scroll to the position
          _scrollController.animateTo(
            scrollOffset,
            duration: const Duration(milliseconds: 300),
            curve: Curves.easeInOut,
          );

          // Optional: Add a slight delay to ensure scroll completes before focus
          Future.delayed(const Duration(milliseconds: 350), () {
            // Optionally focus on the field to show keyboard
            FocusScope.of(context).requestFocus(
              (key.currentContext!.findRenderObject() as RenderBox).attached
                  ? FocusNode()
                  : null,
            );
          });

          break;
        }
      }
    }
  }

  // Updated showSuccessSnackBar method to reflect hold instead of deduction
  void showSuccessSnackBar(
    BuildContext context, {
    required int numberOfPositions,

    required double requiredBalance,
  }) {
    final snackBar = SnackBar(
      elevation: 0,
      behavior: SnackBarBehavior.floating,
      backgroundColor: Colors.transparent,
      content: Container(
        padding: const EdgeInsets.all(16),
        decoration: BoxDecoration(
          boxShadow: [
            BoxShadow(
              blurRadius: 8,
              color: Color(0x26000000),
              offset: Offset(0, 4),
            ),
          ],
          gradient: LinearGradient(
            colors: [Color(0xFF43A047), Color(0xFF2E7D32)],
            begin: AlignmentDirectional.topEnd,
            end: AlignmentDirectional.bottomStart,
          ),
          borderRadius: BorderRadius.circular(16),
        ),
        child: Row(
          children: [
            Container(
              width: 40,
              height: 40,
              decoration: BoxDecoration(
                color: Color(0x40FFFFFF),
                shape: BoxShape.circle,
              ),
              child: const Icon(Icons.check, color: Colors.white, size: 24),
            ),
            const SizedBox(width: 16),
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                mainAxisSize: MainAxisSize.min,
                children: [
                  Text(
                    'Success!',
                    style: TextStyle(
                      fontFamily: 'Inter Tight',
                      color: Colors.white,
                      fontWeight: FontWeight.bold,
                      fontSize: 16,
                    ),
                  ),
                  const SizedBox(height: 4),
                  Text(
                    'Posted $numberOfPositions positions.',
                    style: TextStyle(
                      fontFamily: 'Inter',
                      color: Colors.white,
                      fontSize: 14,
                    ),
                  ),
                  const SizedBox(height: 4),
                  Text(
                    '₹${requiredBalance.toStringAsFixed(2)} held from wallet',
                    style: TextStyle(
                      fontFamily: 'Inter',
                      color: Colors.white,
                      fontSize: 14,
                    ),
                  ),
                ],
              ),
            ),
          ],
        ),
      ),
      duration: const Duration(seconds: 4),
    );

    ScaffoldMessenger.of(context).showSnackBar(snackBar);
  }

  Future<bool> _showEditDeductionConfirmationDialog(
    double currentAmount,
    double newAmount,
    double difference,
  ) async {
    final formatter = NumberFormat.currency(locale: 'en_IN', symbol: '₹');

    final result = await showDialog<bool>(
      context: context,
      barrierDismissible: false,
      builder:
          (context) => Dialog(
            shape: RoundedRectangleBorder(
              borderRadius: BorderRadius.circular(24),
            ),
            child: SingleChildScrollView(
              child: Container(
                width:
                    MediaQuery.of(context).size.width > 340
                        ? 340
                        : MediaQuery.of(context).size.width * 0.9,
                padding: EdgeInsets.all(20),
                child: Column(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    // Icon at the top
                    Container(
                      width: 60,
                      height: 60,
                      decoration: BoxDecoration(
                        color: Color(0xFFEEF1FF),
                        shape: BoxShape.circle,
                      ),
                      child: Center(
                        child: Icon(
                          Icons.edit_notifications_outlined,
                          color: Color(0xFF5B6BF8),
                          size: 28,
                        ),
                      ),
                    ),
                    SizedBox(height: 16),

                    // Title
                    Text(
                      'Confirm Shift Edit',
                      style: TextStyle(
                        fontSize: 20,
                        fontWeight: FontWeight.bold,
                        color: Colors.black,
                      ),
                      textAlign: TextAlign.center,
                    ),
                    SizedBox(height: 12),

                    // Description
                    Text(
                      'The shift amount has increased. An additional amount will be deducted from your wallet.',
                      style: TextStyle(
                        fontSize: 14,
                        color: Colors.black87,
                        height: 1.4,
                      ),
                      textAlign: TextAlign.center,
                    ),
                    SizedBox(height: 16),

                    // Breakdown container
                    Container(
                      padding: EdgeInsets.all(14),
                      decoration: BoxDecoration(
                        color: Color(0xFFF8F9FA),
                        borderRadius: BorderRadius.circular(12),
                      ),
                      child: Column(
                        children: [
                          // Breakdown header
                          Row(
                            children: [
                              Icon(
                                Icons.receipt_outlined,
                                size: 18,
                                color: Colors.black54,
                              ),
                              SizedBox(width: 8),
                              Text(
                                'Amount Change',
                                style: TextStyle(
                                  fontSize: 15,
                                  fontWeight: FontWeight.w600,
                                  color: Colors.black54,
                                ),
                              ),
                            ],
                          ),
                          SizedBox(height: 12),

                          // Current amount
                          Row(
                            mainAxisAlignment: MainAxisAlignment.spaceBetween,
                            children: [
                              Text(
                                'Current total amount',
                                style: TextStyle(
                                  fontSize: 14,
                                  color: Colors.black54,
                                ),
                              ),
                              Text(
                                formatter.format(currentAmount),
                                style: TextStyle(
                                  fontSize: 14,
                                  fontWeight: FontWeight.w500,
                                ),
                              ),
                            ],
                          ),
                          SizedBox(height: 8),

                          // New amount
                          Row(
                            mainAxisAlignment: MainAxisAlignment.spaceBetween,
                            children: [
                              Text(
                                'New total amount',
                                style: TextStyle(
                                  fontSize: 14,
                                  color: Colors.black54,
                                ),
                              ),
                              Text(
                                formatter.format(newAmount),
                                style: TextStyle(
                                  fontSize: 14,
                                  fontWeight: FontWeight.w500,
                                ),
                              ),
                            ],
                          ),
                          SizedBox(height: 12),

                          Divider(color: Colors.black12, height: 1),
                          SizedBox(height: 12),

                          // Additional deduction
                          Row(
                            mainAxisAlignment: MainAxisAlignment.spaceBetween,
                            children: [
                              Text(
                                'Additional deduction',
                                style: TextStyle(
                                  fontSize: 15,
                                  fontWeight: FontWeight.bold,
                                  color: Colors.black87,
                                ),
                              ),
                              Text(
                                formatter.format(difference),
                                style: TextStyle(
                                  fontSize: 15,
                                  fontWeight: FontWeight.bold,
                                  color: Color(0xFF5B6BF8),
                                ),
                              ),
                            ],
                          ),
                        ],
                      ),
                    ),
                    SizedBox(height: 20),

                    // Action buttons
                    Row(
                      children: [
                        // Cancel button
                        Expanded(
                          child: OutlinedButton(
                            onPressed: () => Navigator.pop(context, false),
                            style: OutlinedButton.styleFrom(
                              padding: EdgeInsets.symmetric(vertical: 12),
                              foregroundColor: Colors.black54,
                              side: BorderSide(color: Colors.black12),
                              shape: RoundedRectangleBorder(
                                borderRadius: BorderRadius.circular(10),
                              ),
                            ),
                            child: Text(
                              'Cancel',
                              style: TextStyle(
                                fontSize: 14,
                                fontWeight: FontWeight.w500,
                              ),
                            ),
                          ),
                        ),
                        SizedBox(width: 12),

                        // Confirm button
                        Expanded(
                          child: ElevatedButton(
                            onPressed: () => Navigator.pop(context, true),
                            style: ElevatedButton.styleFrom(
                              padding: EdgeInsets.symmetric(vertical: 12),
                              backgroundColor: Color(0xFF5B6BF8),
                              foregroundColor: Colors.white,
                              elevation: 0,
                              shape: RoundedRectangleBorder(
                                borderRadius: BorderRadius.circular(10),
                              ),
                            ),
                            child: Text(
                              'Confirm',
                              style: TextStyle(
                                fontSize: 14,
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
            ),
          ),
    );

    return result ?? false;
  }

  Future<PlaceModel> _getPlaceDetails(String placeId) async {
    try {
      final apiKey = 'YOUR_GOOGLE_PLACES_API_KEY';
      final url = Uri.parse(
        'https://maps.googleapis.com/maps/api/place/details/json?'
        'place_id=$placeId&key=$apiKey&fields=name,formatted_address,geometry',
      );

      final response = await http.get(url);

      if (response.statusCode == 200) {
        final result = json.decode(response.body);

        if (result['status'] == 'OK') {
          final placeDetails = result['result'];

          // Extract formatted address
          final formattedAddress = placeDetails['formatted_address'];

          // Extract location coordinates
          final location = placeDetails['geometry']['location'];
          final latitude = location['lat'];
          final longitude = location['lng'];

          // Update pincode if possible
          final components = placeDetails['address_components'] ?? [];
          for (var component in components) {
            final types = component['types'];
            if (types.contains('postal_code')) {
              _ShiftPincodeController.text = component['long_name'];
              break;
            }
          }

          return PlaceModel(
            placeId: placeId,
            description: placeDetails['name'] ?? formattedAddress,
            formattedAddress: formattedAddress,
            latitude: latitude,
            longitude: longitude,
          );
        }
      }
    } catch (e) {
      print('Error fetching place details: $e');
    }

    // Fallback
    return PlaceModel(placeId: placeId, description: _locationController.text);
  }

  Future<List<PlaceModel>> _getPlaceSuggestions(String input) async {
    if (input.trim().length < 1) {
      return [];
    }

    try {
      final apiKey =
          'AIzaSyC7eH8S98TXVYSSpGa5HvaDRaCD_YgRJk0'; // Replace with your actual API key
      final url = Uri.parse(
        'https://maps.googleapis.com/maps/api/place/autocomplete/json?'
        'input=$input&key=$apiKey'
        '&types=geocode' // This will include various geocode types including pincodes
        '&components=country:in' // Restrict to India
        '&language=en',
      );

      final response = await http
          .get(url)
          .timeout(
            const Duration(seconds: 10),
            onTimeout: () => http.Response('Timeout', 408),
          );

      if (kDebugMode) {
        print('🌐 Autocomplete Input: $input');
        print('🌐 Request URL: $url');
        print('🌐 Response Status: ${response.statusCode}');
        print('🌐 Response Body: ${response.body}');
      }

      if (response.statusCode == 200) {
        final result = json.decode(response.body);

        if (result['status'] == 'OK') {
          final suggestions =
              (result['predictions'] as List)
                  .take(2)
                  .map<PlaceModel>(
                    (p) => PlaceModel(
                      placeId: p['place_id'],
                      description: p['description'],
                    ),
                  )
                  .toList();

          if (kDebugMode) {
            print('✅ Suggestions Found: ${suggestions.length}');
            suggestions.forEach((suggestion) {
              print('📍 Suggestion: ${suggestion.description}');
            });
          }

          return suggestions;
        } else {
          print('❌ API Error: ${result['status']}');
          print('❌ Error Details: ${result['error_message'] ?? 'No details'}');
          return [];
        }
      } else {
        print('❌ HTTP Error: ${response.statusCode}');
        return [];
      }
    } catch (e) {
      print('❌ Unexpected Error: $e');
      return [];
    }
  }

  Future<bool> _checkInternetConnection() async {
    try {
      final result = await InternetAddress.lookup('google.com');
      return result.isNotEmpty && result[0].rawAddress.isNotEmpty;
    } on SocketException catch (_) {
      return false;
    }
  }

  // Custom autocomplete with overlay positioning directly above the field
  Widget _buildCityAutocomplete() {
    // Controller and focus node to manage overlay

    // Function to show suggestions overlay
    void _showCitySuggestions(List<PlaceModel> suggestions) {
      // Remove existing overlay if any
      _cityOverlayEntry?.remove();

      // Create new overlay entry positioned above the text field
      _cityOverlayEntry = OverlayEntry(
        builder:
            (context) => Positioned(
              width: MediaQuery.of(context).size.width - 32,
              child: CompositedTransformFollower(
                link: _cityLayerLink,
                showWhenUnlinked: false,
                offset: Offset(
                  0.0,
                  -120.0,
                ), // adjust as needed for field height
                // Position above with appropriate height
                child: Material(
                  elevation: 4.0,
                  borderRadius: BorderRadius.circular(12),
                  child: Container(
                    height: min(300, suggestions.length * 60.0),
                    decoration: BoxDecoration(
                      color: Colors.white,
                      borderRadius: BorderRadius.circular(12),
                      boxShadow: [
                        BoxShadow(
                          color: Colors.grey.shade300,
                          blurRadius: 10,
                          offset: Offset(0, -4),
                        ),
                      ],
                    ),
                    child:
                        suggestions.isEmpty
                            ? Center(
                              child: Padding(
                                padding: const EdgeInsets.all(16.0),
                                child: Text(
                                  'No cities found',
                                  style: TextStyle(
                                    color: Colors.grey.shade600,
                                    fontStyle: FontStyle.italic,
                                  ),
                                ),
                              ),
                            )
                            : ListView.separated(
                              shrinkWrap: true,
                              padding: EdgeInsets.symmetric(vertical: 8),
                              itemCount: suggestions.length,
                              separatorBuilder:
                                  (context, index) => Divider(
                                    height: 1,
                                    color: Colors.grey.shade200,
                                    indent: 16,
                                    endIndent: 16,
                                  ),
                              itemBuilder: (context, index) {
                                final option = suggestions[index];
                                return ListTile(
                                  contentPadding: EdgeInsets.symmetric(
                                    horizontal: 16,
                                    vertical: 4,
                                  ),
                                  leading: Container(
                                    padding: EdgeInsets.all(8),
                                    decoration: BoxDecoration(
                                      color: Color(0xFF5B6BF8).withOpacity(0.1),
                                      shape: BoxShape.circle,
                                    ),
                                    child: Icon(
                                      Icons.location_city,
                                      color: Color(0xFF5B6BF8),
                                      size: 24,
                                    ),
                                  ),
                                  title: Text(
                                    option.description,
                                    style: TextStyle(
                                      fontSize: 16,
                                      fontWeight: FontWeight.w500,
                                      color: Colors.black87,
                                      fontFamily: 'Inter',
                                    ),
                                  ),
                                  onTap: () {
                                    print(
                                      'City option selected via tap: ${option.description}',
                                    );
                                    setState(() {
                                      _cityController.text = option.description;
                                      _selectedCity = option;
                                      // Clear location when city changes
                                      _locationController.text = '';
                                      _selectedPlace = null;
                                    });
                                    _cityOverlayEntry?.remove();
                                    _cityOverlayEntry = null;
                                  },
                                  trailing: Icon(
                                    Icons.arrow_forward_ios,
                                    color: Colors.grey.shade400,
                                    size: 16,
                                  ),
                                );
                              },
                            ),
                  ),
                ),
              ),
            ),
      );

      // Add overlay to the screen
      Overlay.of(context).insert(_cityOverlayEntry!);
    }

    // Function to hide suggestions
    void _hideCitySuggestions() {
      _cityOverlayEntry?.remove();
      _cityOverlayEntry = null;
    }

    // Handle focus changes
    _cityFocusNode.addListener(() {
      if (!_cityFocusNode.hasFocus) {
        _hideCitySuggestions();
      }
    });

    return Padding(
      padding: const EdgeInsets.only(bottom: 20),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Text(
            'City',
            style: const TextStyle(
              fontSize: 14,
              fontWeight: FontWeight.w600,
              color: Colors.black87,
              fontFamily: 'Inter',
            ),
          ),
          const SizedBox(height: 8),
          // Use CompositedTransformTarget to link the text field position
          CompositedTransformTarget(
            link: _cityLayerLink,
            child: TextField(
              controller: _cityController,
              focusNode: _cityFocusNode,
              decoration: InputDecoration(
                hintText: 'Enter city name',
                prefixIcon: Icon(Icons.location_city, color: Color(0xFF5B6BF8)),
                suffixIcon:
                    _cityController.text.isNotEmpty
                        ? IconButton(
                          icon: Icon(Icons.clear, color: Colors.grey),
                          onPressed: () {
                            print('City field clear button pressed');
                            setState(() {
                              _cityController.clear();
                              _selectedCity = null;
                              // Also clear location when city is cleared
                              _locationController.text = '';
                              _selectedPlace = null;
                              print('City and location cleared');
                            });
                          },
                        )
                        : null,
                border: OutlineInputBorder(
                  borderRadius: BorderRadius.circular(10),
                  borderSide: BorderSide(color: Colors.grey.shade300),
                ),
                enabledBorder: OutlineInputBorder(
                  borderRadius: BorderRadius.circular(10),
                  borderSide: BorderSide(color: Colors.grey.shade300),
                ),
                focusedBorder: OutlineInputBorder(
                  borderRadius: BorderRadius.circular(10),
                  borderSide: const BorderSide(color: Color(0xFF5B6BF8)),
                ),
                errorBorder: OutlineInputBorder(
                  borderRadius: BorderRadius.circular(10),
                  borderSide: BorderSide(color: Colors.red),
                ),
                filled: true,
                fillColor: Colors.white,
                contentPadding: const EdgeInsets.symmetric(
                  horizontal: 16,
                  vertical: 12,
                ),
              ),
              style: const TextStyle(fontSize: 16, fontFamily: 'Inter'),
              onChanged: (value) async {
                final input = value.trim();
                print('City search input: "$input"');

                if (input.length < 1) {
                  _hideCitySuggestions();
                  return;
                }

                try {
                  print(
                    'Making API call for city suggestions with input: "$input"',
                  );
                  final apiKey = 'AIzaSyC7eH8S98TXVYSSpGa5HvaDRaCD_YgRJk0';
                  final url = Uri.parse(
                    'https://maps.googleapis.com/maps/api/place/autocomplete/json?'
                    'input=${Uri.encodeComponent(input)}&key=$apiKey'
                    '&types=(cities)'
                    '&components=country:in'
                    '&language=en',
                  );

                  final response = await http.get(url);

                  if (response.statusCode == 200) {
                    final result = json.decode(response.body);

                    if (result['status'] == 'OK') {
                      final predictions = result['predictions'] as List;
                      print(
                        'City API returned ${predictions.length} suggestions',
                      );

                      final suggestions =
                          predictions
                              .take(2)
                              .map<PlaceModel>(
                                (p) => PlaceModel(
                                  placeId: p['place_id'],
                                  description: p['description'],
                                ),
                              )
                              .toList();

                      print(
                        'City suggestions parsed: ${suggestions.map((s) => s.description).join(', ')}',
                      );

                      // Show suggestions above the text field
                      _showCitySuggestions(suggestions);
                    }
                  }
                } catch (e) {
                  print('Error in city autocomplete: $e');
                }
              },
            ),
          ),
        ],
      ),
    );
  }

  // Similar approach for location widget
  Widget _buildPlacesAutocomplete() {
    // Controller and focus node to manage overlay

    // Function to show suggestions overlay
    void _showLocationSuggestions(List<PlaceModel> suggestions) {
      // Remove existing overlay if any
      _locationOverlayEntry?.remove();

      // Create new overlay entry positioned above the text field
      _locationOverlayEntry = OverlayEntry(
        builder:
            (context) => Positioned(
              width: MediaQuery.of(context).size.width - 32,
              child: CompositedTransformFollower(
                link: _locationLayerLink,
                showWhenUnlinked: false,
                offset: Offset(
                  0.0,
                  -120.0,
                ), // adjust as needed for field height
                // Position above with appropriate height
                child: Material(
                  elevation: 4.0,
                  borderRadius: BorderRadius.circular(12),
                  child: Container(
                    height: min(300, suggestions.length * 60.0),
                    decoration: BoxDecoration(
                      color: Colors.white,
                      borderRadius: BorderRadius.circular(12),
                      boxShadow: [
                        BoxShadow(
                          color: Colors.grey.shade300,
                          blurRadius: 10,
                          offset: Offset(0, -4),
                        ),
                      ],
                    ),
                    child:
                        suggestions.isEmpty
                            ? Center(
                              child: Padding(
                                padding: const EdgeInsets.all(16.0),
                                child: Text(
                                  'No locations found in ${_cityController.text.split(',').first}',
                                  style: TextStyle(
                                    color: Colors.grey.shade600,
                                    fontStyle: FontStyle.italic,
                                  ),
                                ),
                              ),
                            )
                            : ListView.separated(
                              shrinkWrap: true,
                              padding: EdgeInsets.symmetric(vertical: 8),
                              itemCount: suggestions.length,
                              separatorBuilder:
                                  (context, index) => Divider(
                                    height: 1,
                                    color: Colors.grey.shade200,
                                    indent: 16,
                                    endIndent: 16,
                                  ),
                              itemBuilder: (context, index) {
                                final option = suggestions[index];
                                return ListTile(
                                  contentPadding: EdgeInsets.symmetric(
                                    horizontal: 16,
                                    vertical: 4,
                                  ),
                                  leading: Container(
                                    padding: EdgeInsets.all(8),
                                    decoration: BoxDecoration(
                                      color: Color(0xFF5B6BF8).withOpacity(0.1),
                                      shape: BoxShape.circle,
                                    ),
                                    child: Icon(
                                      Icons.location_on_outlined,
                                      color: Color(0xFF5B6BF8),
                                      size: 24,
                                    ),
                                  ),
                                  title: Text(
                                    option.description,
                                    style: TextStyle(
                                      fontSize: 16,
                                      fontWeight: FontWeight.w500,
                                      color: Colors.black87,
                                      fontFamily: 'Inter',
                                    ),
                                  ),
                                  onTap: () {
                                    print(
                                      'Location option selected via tap: ${option.description}',
                                    );
                                    setState(() {
                                      _locationController.text =
                                          option.description;
                                      _selectedPlace = option;

                                      // Try to extract pincode if available
                                      final pincodeMatch = RegExp(
                                        r'\b\d{6}\b',
                                      ).firstMatch(option.description);

                                      if (pincodeMatch != null) {
                                        final pincode =
                                            pincodeMatch.group(0) ?? '';
                                        _ShiftPincodeController.text = pincode;
                                      }
                                    });
                                    _locationOverlayEntry?.remove();
                                    _locationOverlayEntry = null;
                                  },
                                  trailing: Icon(
                                    Icons.arrow_forward_ios,
                                    color: Colors.grey.shade400,
                                    size: 16,
                                  ),
                                );
                              },
                            ),
                  ),
                ),
              ),
            ),
      );

      // Add overlay to the screen
      Overlay.of(context).insert(_locationOverlayEntry!);
    }

    // Function to hide suggestions
    void _hideLocationSuggestions() {
      _locationOverlayEntry?.remove();
      _locationOverlayEntry = null;
    }

    // Handle focus changes
    _locationFocusNode.addListener(() {
      if (!_locationFocusNode.hasFocus) {
        _hideLocationSuggestions();
      }
    });

    return Padding(
      padding: const EdgeInsets.only(bottom: 20),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Text(
                'Location',
                style: const TextStyle(
                  fontSize: 14,
                  fontWeight: FontWeight.w600,
                  color: Colors.black87,
                  fontFamily: 'Inter',
                ),
              ),
              if (_cityController.text.isNotEmpty)
                Expanded(
                  child: Padding(
                    padding: const EdgeInsets.only(left: 8.0),
                    child: Text(
                      'in ${_cityController.text.split(',').first}',
                      style: const TextStyle(
                        fontSize: 14,
                        fontWeight: FontWeight.w500,
                        color: Color(0xFF5B6BF8),
                        fontFamily: 'Inter',
                        fontStyle: FontStyle.italic,
                      ),
                      overflow: TextOverflow.ellipsis,
                    ),
                  ),
                ),
            ],
          ),
          const SizedBox(height: 8),
          CompositedTransformTarget(
            link: _locationLayerLink,
            child: TextField(
              controller: _locationController,
              focusNode: _locationFocusNode,
              enabled: _cityController.text.isNotEmpty,
              decoration: InputDecoration(
                hintText:
                    _cityController.text.isNotEmpty
                        ? 'Enter location in ${_cityController.text.split(',').first}'
                        : 'Select a city first',
                prefixIcon: Icon(
                  Icons.location_on_outlined,
                  color:
                      _cityController.text.isNotEmpty
                          ? Color(0xFF5B6BF8)
                          : Colors.grey,
                ),
                suffixIcon:
                    _locationController.text.isNotEmpty &&
                            _cityController.text.isNotEmpty
                        ? IconButton(
                          icon: Icon(Icons.clear, color: Colors.grey),
                          onPressed: () {
                            print('Location field clear button pressed');
                            setState(() {
                              _locationController.clear();
                              _selectedPlace = null;
                            });
                          },
                        )
                        : _cityController.text.isNotEmpty
                        ? null
                        : Icon(
                          Icons.lock_outline,
                          color: Colors.grey,
                          size: 18,
                        ),
                border: OutlineInputBorder(
                  borderRadius: BorderRadius.circular(10),
                  borderSide: BorderSide(color: Colors.grey.shade300),
                ),
                enabledBorder: OutlineInputBorder(
                  borderRadius: BorderRadius.circular(10),
                  borderSide: BorderSide(color: Colors.grey.shade300),
                ),
                focusedBorder: OutlineInputBorder(
                  borderRadius: BorderRadius.circular(10),
                  borderSide: const BorderSide(color: Color(0xFF5B6BF8)),
                ),
                errorBorder: OutlineInputBorder(
                  borderRadius: BorderRadius.circular(10),
                  borderSide: BorderSide(color: Colors.red),
                ),
                filled: true,
                fillColor:
                    _cityController.text.isNotEmpty
                        ? Colors.white
                        : Colors.grey.shade100,
                disabledBorder: OutlineInputBorder(
                  borderRadius: BorderRadius.circular(10),
                  borderSide: BorderSide(color: Colors.grey.shade200),
                ),
                contentPadding: const EdgeInsets.symmetric(
                  horizontal: 16,
                  vertical: 12,
                ),
              ),
              style: const TextStyle(fontSize: 16, fontFamily: 'Inter'),
              onChanged: (value) async {
                final input = value.trim();
                print('Location search input: "$input"');

                // Check if city is selected
                if (_cityController.text.isEmpty) {
                  print('No city selected for location search');
                  return;
                }

                // Don't search if input is too short
                if (input.length < 1) {
                  _hideLocationSuggestions();
                  return;
                }

                try {
                  // Get the city name without state/country
                  final cityName = _cityController.text.split(',').first.trim();
                  print('Searching for locations in city: "$cityName"');

                  final apiKey = 'AIzaSyC7eH8S98TXVYSSpGa5HvaDRaCD_YgRJk0';
                  final searchQuery = "$input in $cityName";
                  print('Location search query: "$searchQuery"');

                  final url = Uri.parse(
                    'https://maps.googleapis.com/maps/api/place/autocomplete/json?'
                    'input=${Uri.encodeComponent(searchQuery)}&key=$apiKey'
                    '&components=country:in'
                    '&language=en',
                  );

                  final response = await http.get(url);

                  if (response.statusCode == 200) {
                    final result = json.decode(response.body);

                    if (result['status'] == 'OK') {
                      final predictions = result['predictions'] as List;
                      print(
                        'Location API returned ${predictions.length} raw suggestions',
                      );

                      final suggestions =
                          predictions
                              .take(2)
                              .map<PlaceModel>(
                                (p) => PlaceModel(
                                  placeId: p['place_id'],
                                  description: p['description'],
                                ),
                              )
                              .toList();

                      print(
                        'All suggestions before filtering: ${suggestions.map((s) => s.description).join(' | ')}',
                      );

                      final filteredSuggestions =
                          suggestions
                              .where(
                                (model) => model.description
                                    .toLowerCase()
                                    .contains(cityName.toLowerCase()),
                              )
                              .toList();

                      print(
                        'Filtered suggestions (${filteredSuggestions.length}): ${filteredSuggestions.map((s) => s.description).join(' | ')}',
                      );

                      // Show suggestions above the text field
                      _showLocationSuggestions(filteredSuggestions);
                    }
                  }
                } catch (e) {
                  print('Error in location autocomplete: $e');
                }
              },
            ),
          ),
        ],
      ),
    );
  }

  void _showNoWalletDialog() {
    setState(() {
      _isLoading = false;
    });

    showDialog(
      context: context,
      builder:
          (context) => AlertDialog(
            title: const Text('Wallet Required'),
            content: const Text(
              'You need to set up a wallet to post Shifts. Please add funds to your wallet to continue.',
            ),
            actions: [
              TextButton(
                onPressed: () => Navigator.pop(context),
                child: const Text('Cancel'),
              ),
              ElevatedButton(
                onPressed: () {
                  Navigator.pop(context);
                  // Navigate to wallet setup screen
                  // Replace with your actual wallet screen navigation
                  Get.to(() => WalletScreen());
                },
                style: ElevatedButton.styleFrom(
                  backgroundColor: Theme.of(context).primaryColor,
                ),
                child: const Text('Set Up Wallet'),
              ),
            ],
          ),
    );
  }

  // Show a dialog when balance is insufficient
  void _showInsufficientBalanceDialog(double required, double available) {
    setState(() {
      _isLoading = false;
    });

    final formatter = NumberFormat.currency(locale: 'en_IN', symbol: '₹');

    showDialog(
      context: context,
      builder:
          (context) => AlertDialog(
            title: const Text('Insufficient Balance'),
            content: Text(
              'You need ${formatter.format(required)} to post this Shift, but your wallet only has ${formatter.format(available)}. Please add funds to your wallet to continue.',
            ),
            actions: [
              TextButton(
                onPressed: () => Navigator.pop(context),
                child: const Text('Cancel'),
              ),
              ElevatedButton(
                onPressed: () {
                  Navigator.pop(context);
                  // Navigate to add funds screen
                  // Replace with your actual wallet screen navigation
                  Get.to(() => WalletScreen());
                },
                style: ElevatedButton.styleFrom(
                  backgroundColor: Theme.of(context).primaryColor,
                ),
                child: const Text('Add Funds'),
              ),
            ],
          ),
    );
  }

  // Updated confirmation dialog to reflect hold instead of deduction
  Future<bool> _showDeductionConfirmationDialog(double amount) async {
    // Get screen size
    final screenSize = MediaQuery.of(context).size;
    setState(() {
      _isLoading = false;
    });

    final formatter = NumberFormat.currency(locale: 'en_IN', symbol: '₹');
    final int positions = int.parse(_positionsController.text);
    final perPositionCharge = double.parse(
      _payRateController.text.replaceAll('₹', '').trim(),
    );

    final result = await showDialog<bool>(
      context: context,
      barrierDismissible: false,
      builder:
          (context) => Dialog(
            shape: RoundedRectangleBorder(
              borderRadius: BorderRadius.circular(24),
            ),
            child: SingleChildScrollView(
              // Add SingleChildScrollView to make the content scrollable
              child: Container(
                width:
                    MediaQuery.of(context).size.width > 340
                        ? 340
                        : MediaQuery.of(context).size.width *
                            0.9, // Responsive width
                padding: EdgeInsets.all(
                  20,
                ), // Slightly reduce padding to save space
                child: Column(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    // Icon at the top
                    Container(
                      width: 60, // Smaller icon container
                      height: 60,
                      decoration: BoxDecoration(
                        color: Color(0xFFEEF1FF),
                        shape: BoxShape.circle,
                      ),
                      child: Center(
                        child: Icon(
                          Icons.confirmation_num_outlined,
                          color: Color(0xFF5B6BF8),
                          size: 28,
                        ),
                      ),
                    ),
                    SizedBox(height: 16),

                    // Title
                    Text(
                      'Confirm Shift Posting',
                      style: TextStyle(
                        fontSize: 20, // Slightly smaller font
                        fontWeight: FontWeight.bold,
                        color: Colors.black,
                      ),
                      textAlign: TextAlign.center,
                    ),
                    SizedBox(height: 12),

                    // Description
                    RichText(
                      textAlign: TextAlign.center,
                      text: TextSpan(
                        style: TextStyle(
                          fontSize: 14, // Smaller font
                          color: Colors.black87,
                          height: 1.4,
                        ),
                        children: [
                          TextSpan(text: 'You will have '),
                          TextSpan(
                            text: formatter.format(amount),
                            style: TextStyle(
                              fontWeight: FontWeight.bold,
                              color: Color(0xFF5B6BF8),
                            ),
                          ),
                          TextSpan(
                            text:
                                ' held from your wallet for posting this Shift with ',
                          ),
                          TextSpan(
                            text: '$positions position(s)',
                            style: TextStyle(fontWeight: FontWeight.bold),
                          ),
                          TextSpan(text: '.'),
                        ],
                      ),
                    ),
                    SizedBox(height: 8),

                    // Add explanation about holds
                    Container(
                      padding: EdgeInsets.all(10),
                      decoration: BoxDecoration(
                        color: Colors.amber.shade50,
                        borderRadius: BorderRadius.circular(8),
                        border: Border.all(color: Colors.amber.shade200),
                      ),
                      child: Column(
                        children: [
                          Row(
                            children: [
                              Icon(
                                Icons.info_outline,
                                color: Colors.amber.shade800,
                                size: 16,
                              ),
                              SizedBox(width: 8),
                              Expanded(
                                child: Text(
                                  'What is a hold?',
                                  style: TextStyle(
                                    fontWeight: FontWeight.bold,
                                    fontSize: 13,
                                    color: Colors.amber.shade800,
                                  ),
                                ),
                              ),
                            ],
                          ),
                          SizedBox(height: 6),
                          Text(
                            'This amount will be held in your wallet until the shift is completed or cancelled. Held funds cannot be used for other purposes while the shift is active.',
                            style: TextStyle(
                              fontSize: 12,
                              color: Colors.grey.shade800,
                            ),
                          ),
                        ],
                      ),
                    ),
                    SizedBox(height: 16),

                    // Breakdown container
                    Container(
                      padding: EdgeInsets.all(14),
                      decoration: BoxDecoration(
                        color: Color(0xFFF8F9FA),
                        borderRadius: BorderRadius.circular(12),
                      ),
                      child: Column(
                        children: [
                          // Breakdown header with icon
                          Row(
                            children: [
                              Icon(
                                Icons.receipt_outlined,
                                size: 18,
                                color: Colors.black54,
                              ),
                              SizedBox(width: 8),
                              Text(
                                'Breakdown',
                                style: TextStyle(
                                  fontSize: 15,
                                  fontWeight: FontWeight.w600,
                                  color: Colors.black54,
                                ),
                              ),
                            ],
                          ),
                          SizedBox(height: 12),

                          // Per position charge
                          Row(
                            mainAxisAlignment: MainAxisAlignment.spaceBetween,
                            children: [
                              Text(
                                'Per position charge',
                                style: TextStyle(
                                  fontSize: 14,
                                  color: Colors.black54,
                                ),
                              ),
                              Text(
                                '₹${perPositionCharge.toStringAsFixed(2)}',
                                style: TextStyle(
                                  fontSize: 14,
                                  fontWeight: FontWeight.w500,
                                ),
                              ),
                            ],
                          ),
                          SizedBox(height: 8),

                          // Number of positions
                          Row(
                            mainAxisAlignment: MainAxisAlignment.spaceBetween,
                            children: [
                              Text(
                                'Number of positions',
                                style: TextStyle(
                                  fontSize: 14,
                                  color: Colors.black54,
                                ),
                              ),
                              Text(
                                '$positions',
                                style: TextStyle(
                                  fontSize: 14,
                                  fontWeight: FontWeight.w500,
                                ),
                              ),
                            ],
                          ),
                          SizedBox(height: 12),

                          Divider(color: Colors.black12, height: 1),
                          SizedBox(height: 12),

                          // Total amount
                          Row(
                            mainAxisAlignment: MainAxisAlignment.spaceBetween,
                            children: [
                              Text(
                                'Total hold amount',
                                style: TextStyle(
                                  fontSize: 15,
                                  fontWeight: FontWeight.bold,
                                  color: Colors.black87,
                                ),
                              ),
                              Text(
                                formatter.format(amount),
                                style: TextStyle(
                                  fontSize: 15,
                                  fontWeight: FontWeight.bold,
                                  color: Color(0xFF5B6BF8),
                                ),
                              ),
                            ],
                          ),
                        ],
                      ),
                    ),
                    SizedBox(height: 16),

                    // Confirmation question
                    Text(
                      'Do you want to proceed?', // Shorter text
                      style: TextStyle(
                        fontSize: 15,
                        fontWeight: FontWeight.w500,
                        color: Colors.black87,
                      ),
                      textAlign: TextAlign.center,
                    ),
                    SizedBox(height: 20),

                    // Action buttons
                    Row(
                      children: [
                        // Cancel button
                        Expanded(
                          child: OutlinedButton(
                            onPressed: () => Navigator.pop(context, false),
                            style: OutlinedButton.styleFrom(
                              padding: EdgeInsets.symmetric(vertical: 12),
                              foregroundColor: Colors.black54,
                              side: BorderSide(color: Colors.black12),
                              shape: RoundedRectangleBorder(
                                borderRadius: BorderRadius.circular(10),
                              ),
                            ),
                            child: Text(
                              'Cancel',
                              style: TextStyle(
                                fontSize: 14,
                                fontWeight: FontWeight.w500,
                              ),
                            ),
                          ),
                        ),
                        SizedBox(width: 12),

                        // Confirm button
                        Expanded(
                          child: ElevatedButton(
                            onPressed: () => Navigator.pop(context, true),
                            style: ElevatedButton.styleFrom(
                              padding: EdgeInsets.symmetric(vertical: 12),
                              backgroundColor: Color(0xFF5B6BF8),
                              foregroundColor: Colors.white,
                              elevation: 0,
                              shape: RoundedRectangleBorder(
                                borderRadius: BorderRadius.circular(10),
                              ),
                            ),
                            child: Text(
                              'Confirm', // Shorter text
                              style: TextStyle(
                                fontSize: 14,
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
            ),
          ),
    );

    setState(() {
      _isLoading = true;
    });

    return result ?? false;
  }

  void _calculatePayRate() {
    if (selectedHours != null) {
      // Show a dialog to input hourly rate
      showDialog(
        context: context,
        builder: (BuildContext context) {
          final hourlyRateController = TextEditingController();

          return AlertDialog(
            title: Text('Enter Hourly Rate'),
            content: Column(
              mainAxisSize: MainAxisSize.min,
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(
                  'You selected ${selectedHours} hours',
                  style: TextStyle(
                    fontWeight: FontWeight.bold,
                    color: Colors.grey[700],
                  ),
                ),
                SizedBox(height: 16),
                TextField(
                  controller: hourlyRateController,
                  keyboardType: TextInputType.number,
                  decoration: InputDecoration(
                    hintText: 'Enter hourly rate in ₹',
                    prefixText: '₹ ',
                    border: OutlineInputBorder(),
                  ),
                ),
              ],
            ),
            actions: [
              TextButton(
                onPressed: () => Navigator.pop(context),
                child: Text('Cancel'),
              ),
              ElevatedButton(
                onPressed: () {
                  // Validate hourly rate
                  final hourlyRate = double.tryParse(hourlyRateController.text);

                  if (hourlyRate != null && hourlyRate > 0) {
                    // Calculate total pay rate
                    final totalPayRate = hourlyRate * selectedHours!;

                    setState(() {
                      _payRateController.text = totalPayRate.toStringAsFixed(2);
                    });

                    Navigator.pop(context);
                  } else {
                    // Show error if invalid input
                    ScaffoldMessenger.of(context).showSnackBar(
                      SnackBar(
                        content: Text('Please enter a valid hourly rate'),
                        backgroundColor: Colors.red,
                      ),
                    );
                  }
                },
                child: Text('Calculate Total Pay'),
              ),
            ],
          );
        },
      );
    } else {
      // Show a snackbar to select hours first
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('Please select shift duration first'),
          backgroundColor: Colors.red,
        ),
      );
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: const Color(0xFFF8FAFF),
      appBar: AppBar(
        title: Text(
          _isEditMode ? 'Edit Shift' : 'Post a Shift',
          style: const TextStyle(
            color: Colors.black87,
            fontWeight: FontWeight.bold,
          ),
        ),
        backgroundColor: Colors.white,
        elevation: 1,
        iconTheme: const IconThemeData(color: Color(0xFF5B6BF8)),
      ),

      body: GestureDetector(
        behavior: HitTestBehavior.opaque,
        onTap: () {
          _hideAllOverlays();
          // This will unfocus any text field and dismiss the keyboard
          FocusManager.instance.primaryFocus?.unfocus();
        },
        child:
            _isLoading
                ? const Center(
                  child: CircularProgressIndicator(color: Color(0xFF5B6BF8)),
                )
                : GestureDetector(
                  onTap: () => FocusScope.of(context).unfocus(),
                  child: Form(
                    key: _formKey,
                    autovalidateMode:
                        _formSubmitted
                            ? AutovalidateMode.always
                            : AutovalidateMode.disabled,
                    child: SingleChildScrollView(
                      controller: _scrollController,
                      padding: const EdgeInsets.all(16),
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          _buildSectionHeader('Shift Information'),
                          _buildCard([
                            _buildTextField(
                              controller: _ShiftTitleController,
                              label: 'Shift Title',
                              hint:
                                  'e.g., Sales Executive, Supervisor, Cashier',
                              icon: Icons.work_outline,
                              validator: _required,
                              fieldKey: 'shiftTitle',
                            ),
                            _buildTextField(
                              controller: _companyController,
                              label: 'Company Name',
                              hint: 'e.g., Asia mall, Restaurant ...',
                              icon: Icons.business,
                              validator: _required,
                              fieldKey: 'company',
                            ),
                            _buildCategoryDropdown(),
                            _buildCityAutocomplete(),
                            _buildPlacesAutocomplete(),
                            _buildTextField(
                              controller: _ShiftPincodeController,
                              label: 'Pincode',
                              hint: 'e.g., 400001',
                              icon: Icons.location_on,
                              keyboardType: TextInputType.number,
                              validator: _validatePincode,
                              fieldKey: 'pincode',
                            ),
                          ]),

                          _buildSectionHeader('Shift Details'),
                          _buildCard([
                            _buildTextField(
                              controller: _dateController,
                              label: 'Date',
                              hint: 'Select Date',
                              icon: Icons.calendar_today,
                              readOnly: true,
                              onTap: () => _selectDate(context),
                              validator: _required,
                              fieldKey: 'date',
                            ),
                            _buildTimeSelectionRow(),

                            _buildTextField(
                              controller: _positionsController,
                              label: 'Number of Positions',
                              hint: 'e.g., 1, 2, 3',
                              icon: Icons.groups_outlined,
                              keyboardType: TextInputType.number,
                              validator: _isEditMode ? null : _validateNumber,
                              readOnly:
                                  _isEditMode, // Make read-only in edit mode
                              fieldKey: 'positions',
                            ),
                          ]),

                          _buildSectionHeader('Payment Details'),
                          _buildCard([
                            _buildTextField(
                              controller: _hourlyRateController,
                              label: 'Hourly Rate (₹)',
                              hint: 'e.g., 200',
                              icon: Icons.currency_rupee,
                              keyboardType: TextInputType.number,
                              validator: _validatePay,
                              fieldKey: 'hourlyRate',
                            ),
                            _buildTextField(
                              controller: _payRateController,
                              label: 'Total Pay Per 1 Position (₹)',
                              hint: 'Calculated based on hours',
                              icon: Icons.currency_rupee,
                              readOnly: true,
                              fieldKey: 'totalPay',
                            ),
                          ]),

                          _buildSectionHeader('Supervisor Information'),
                          _buildCard([
                            _buildTextField(
                              controller: _supervisorNameController,
                              label: 'Supervisor Name',
                              hint: 'Full Name',
                              icon: Icons.person,
                              validator: _required,
                              fieldKey: 'supervisorName',
                            ),
                            _buildTextField(
                              controller: _supervisorPhoneController,
                              label: 'Supervisor Phone',
                              hint: 'e.g., 9876543210',
                              icon: Icons.phone,
                              keyboardType: TextInputType.phone,
                              validator: _validatePhone,
                              fieldKey: 'supervisorPhone',
                            ),
                            _buildTextField(
                              controller: _supervisorEmailController,
                              label: 'Supervisor Email',
                              hint: 'e.g., supervisor@company.com',
                              icon: Icons.email,
                              keyboardType: TextInputType.emailAddress,
                              validator: _validateEmail,
                              fieldKey: 'supervisorEmail',
                            ),
                          ]),

                          _buildSectionHeader('Additional Information'),
                          _buildCard([
                            _buildTextField(
                              controller: _websiteController,
                              label: 'Website (Optional)',
                              hint: 'e.g., www.shfthour.com',
                              icon: Icons.language,
                            ),
                            _buildTextField(
                              controller: _dressCodeController,
                              label: 'Dress Code',
                              hint: 'e.g., Black Shirt, Jeans',
                              icon: Icons.checkroom_outlined,
                              maxLines: 3,
                              // validator: _required,
                            ),
                          ]),

                          const SizedBox(height: 24),
                          SizedBox(
                            width: double.infinity,
                            height: 50,
                            child: ElevatedButton.icon(
                              onPressed: _postJob,
                              icon: const Icon(Icons.post_add),
                              label: Text(
                                _isEditMode ? 'Update Shift' : 'Post Shift',
                                style: const TextStyle(
                                  fontSize: 16,
                                  fontWeight: FontWeight.bold,
                                  fontFamily: 'Inter',
                                ),
                              ),
                              style: ElevatedButton.styleFrom(
                                backgroundColor: const Color(0xFF5B6BF8),
                                shape: RoundedRectangleBorder(
                                  borderRadius: BorderRadius.circular(10),
                                ),
                              ),
                            ),
                          ),
                        ],
                      ),
                    ),
                  ),
                ),
      ),
    );
  }

  Widget _buildSectionHeader(String title) {
    return Padding(
      padding: const EdgeInsets.only(bottom: 12),
      child: Text(
        title,
        style: const TextStyle(
          fontSize: 18,
          fontWeight: FontWeight.bold,
          fontFamily: 'Inter Tight',
        ),
      ),
    );
  }

  Widget _buildCard(List<Widget> children) {
    return Container(
      margin: const EdgeInsets.only(bottom: 20),
      padding: const EdgeInsets.all(20),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(12),
        boxShadow: const [
          BoxShadow(
            blurRadius: 4,
            color: Color(0x10000000),
            offset: Offset(0, 2),
          ),
        ],
      ),
      child: Column(children: children),
    );
  } // Required field validator

  String? _required(String? value) {
    if (value == null || value.trim().isEmpty) {
      return 'This field is required';
    }
    return null;
  }

  // Pincode validation
  String? _validatePincode(String? value) {
    if (value == null || value.isEmpty) {
      return 'Pincode is required';
    }
    if (!RegExp(r'^\d{6}$').hasMatch(value)) {
      return 'Enter a valid 6-digit pincode';
    }
    return null;
  }

  // Number validator (for positions, etc.)
  String? _validateNumber(String? value) {
    if (value == null || value.isEmpty) {
      return 'This field is required';
    }
    final number = int.tryParse(value);
    if (number == null || number <= 0) {
      return 'Enter a valid positive number';
    }
    return null;
  }

  // Pay rate validator
  String? _validatePay(String? value) {
    if (value == null || value.isEmpty) {
      return 'Hourly rate is required';
    }
    final rate = double.tryParse(value);
    if (rate == null || rate <= 0) {
      return 'Enter a valid hourly rate';
    }
    return null;
  }

  // Phone number validator
  String? _validatePhone(String? value) {
    if (value == null || value.isEmpty) {
      return 'Phone number is required';
    }
    if (!RegExp(r'^\d{10}$').hasMatch(value)) {
      return 'Enter a valid 10-digit phone number';
    }
    // Additional check to prevent all-zero numbers
    if (RegExp(r'^0+$').hasMatch(value)) {
      return 'Invalid phone number';
    }
    // Optional: Add more specific checks for Indian mobile numbers
    if (!value.startsWith(RegExp(r'[6-9]'))) {
      return 'Phone number must start with 6-9';
    }
    return null;
  }

  // Email validator

  String? _validateEmail(String? value) {
    if (value == null || value.isEmpty) {
      return 'Email is required';
    }

    // Use a proper email regex pattern
    if (!RegExp(r'^[\w-\.]+@([\w-]+\.)+[\w-]{2,4}$').hasMatch(value)) {
      return 'Enter a valid email address';
    }

    return null;
  }

  // Helper method to build consistent text fields
  Widget _buildTextField({
    required TextEditingController controller,
    required String label,
    required String hint,
    required IconData icon,
    bool readOnly = false,
    VoidCallback? onTap,
    int maxLines = 1,
    String? prefixText,
    TextInputType keyboardType = TextInputType.text,
    String? Function(String?)? validator,
    void Function(String)? onChanged,
    String? fieldKey,
  }) {
    // Check if this is the number of positions field and we're in edit mode

    final key =
        fieldKey != null
            ? _fieldKeys[fieldKey]
            : GlobalKey(
              debugLabel: 'textField_${DateTime.now().millisecondsSinceEpoch}',
            );

    // Check if this is the number of positions field and we're in edit mode

    return Padding(
      key: key,
      padding: const EdgeInsets.only(bottom: 20),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Text(
            label,
            style: const TextStyle(
              fontSize: 14,
              fontWeight: FontWeight.w600,
              color: Colors.black87,
              fontFamily: 'Inter',
            ),
          ),
          const SizedBox(height: 8),
          TextFormField(
            controller: controller,
            decoration: InputDecoration(
              hintText: hint,
              prefixIcon: Icon(icon, color: const Color(0xFF5B6BF8)),
              prefixText: prefixText,
              border: OutlineInputBorder(
                borderRadius: BorderRadius.circular(10),
                borderSide: BorderSide(color: Colors.grey.shade300),
              ),
              enabledBorder: OutlineInputBorder(
                borderRadius: BorderRadius.circular(10),
                borderSide: BorderSide(color: Colors.grey.shade300),
              ),
              focusedBorder: OutlineInputBorder(
                borderRadius: BorderRadius.circular(10),
                borderSide: const BorderSide(color: Color(0xFF5B6BF8)),
              ),
              errorBorder: OutlineInputBorder(
                borderRadius: BorderRadius.circular(10),
                borderSide: const BorderSide(color: Colors.red),
              ),
              filled: true,
              fillColor: readOnly ? Colors.grey.shade100 : Colors.white,
              contentPadding: const EdgeInsets.symmetric(
                horizontal: 16,
                vertical: 12,
              ),
            ),
            style: const TextStyle(fontSize: 16, fontFamily: 'Inter'),
            readOnly: readOnly,
            onTap: onTap,
            maxLines: maxLines,
            keyboardType: keyboardType,
            validator: validator,
            onChanged: onChanged,
          ),
        ],
      ),
    );
  }
}

class PlaceModel {
  final String placeId;
  final String description;
  String? formattedAddress;
  double? latitude;
  double? longitude;

  PlaceModel({
    required this.placeId,
    required this.description,
    this.formattedAddress,
    this.latitude,
    this.longitude,
  });

  Map<String, dynamic> toJson() {
    return {
      'placeId': placeId,
      'description': description,
      'formattedAddress': formattedAddress,
      'latitude': latitude,
      'longitude': longitude,
    };
  }
}
