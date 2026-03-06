import 'package:flutter/material.dart';
import 'package:get/get.dart';
import 'package:shifthour_employeer/Employer/Business%20Kyc/kyc.dart';
import 'package:shifthour_employeer/Employer/employer_dashboard.dart';
import 'package:shifthour_employeer/const/Bottom_Navigation.dart';
import 'package:shifthour_employeer/const/Standard_Appbar.dart';

import 'package:shifthour_employeer/const/settings.dart';
import 'package:shifthour_employeer/login.dart';
import 'package:supabase_flutter/supabase_flutter.dart';

// Model class for Employer data
class Employer {
  final String id;
  final String companyName;
  final String website;
  final String contactName;
  final String contactEmail;
  final String contactPhone;
  final DateTime createdAt;
  final DateTime updatedAt;

  Employer({
    required this.id,
    required this.companyName,
    required this.website,
    required this.contactName,
    required this.contactEmail,
    required this.contactPhone,
    required this.createdAt,
    required this.updatedAt,
  });

  factory Employer.fromJson(Map<String, dynamic> json) {
    return Employer(
      id: json['id'] ?? '',
      companyName: json['company_name'] ?? '',
      website: json['website'] ?? '',
      contactName: json['contact_name'] ?? '',
      contactEmail: json['contact_email'] ?? '',
      contactPhone: json['contact_phone']?.toString() ?? '',
      createdAt:
          json['created_at'] != null
              ? DateTime.parse(json['created_at'])
              : DateTime.now(),
      updatedAt:
          json['updated_at'] != null
              ? DateTime.parse(json['updated_at'])
              : DateTime.now(),
    );
  }
}

class EmployerProfileController extends GetxController {
  // Get Supabase client
  final supabase = Supabase.instance.client;

  // Observable to hold the employer data
  final employer = Rx<Employer?>(null);

  // Observable for loading state
  final isLoading = true.obs;
  final isEditing = false.obs;
  final hasError = false.obs;
  final errorMessage = ''.obs;
  final isEditingCompanyName = false.obs;
  final isEditingContactName = false.obs;
  final isEditingPhoneNumber = false.obs;
  final companyNameController = TextEditingController();
  final websiteController = TextEditingController();
  final contactNameController = TextEditingController();
  final phoneNumberController = TextEditingController();
  // Observable variables for company information
  final companyName = ''.obs;
  final companyDescription = ''.obs;

  final phoneNumber = ''.obs;
  final website = ''.obs;
  final emailAddress = ''.obs;
  final contactName = ''.obs;
  final alternateContact = 'Slack: @techvision'.obs;
  final panNumber = ''.obs;
  final panPhotoUrl = ''.obs;
  final incorporationCertificateUrl = ''.obs;
  final isVerified = false.obs;
  // Verification Status
  final verificationProgress = 0.7.obs;
  final businessVerificationStatus = 'Inactive'.obs;

  @override
  void onInit() {
    super.onInit();
    loadEmployerFromSupabase();
    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (Get.isRegistered<NavigationController>()) {
        final navController = Get.find<NavigationController>();
        // Only update if the controller is still active in memory
        try {
          navController.currentIndex.value = 4;
        } catch (e) {
          // Prevent setState after dispose error
          debugPrint('NavigationController already disposed: $e');
        }
      }
    });
  }

  Future<void> updateEmployerInSupabase() async {
    try {
      await supabase
          .from('employers')
          .update({
            'company_name': companyName.value,
            'contact_name': contactName.value,
            'contact_phone': phoneNumber.value,
            'website': website.value,
          })
          .eq('id', employer.value!.id);

      print('Employer updated successfully');
    } catch (e) {
      print('Error updating employer: $e');
    }
  }

  Future<void> loadKycInformation(String userId) async {
    try {
      final response =
          await supabase
              .from('business_verifications')
              .select()
              .eq('user_id', userId)
              .maybeSingle();

      if (response != null) {
        panNumber.value = response['pan_number'] ?? '';
        panPhotoUrl.value = response['pan_photo_url'] ?? '';
        incorporationCertificateUrl.value =
            response['incorporation_certificate_url'] ?? '';
        isVerified.value = response['is_verified'] ?? false;

        // Add this line to get the status
        businessVerificationStatus.value = response['status'] ?? 'Inactive';
      }
    } catch (e) {
      print('Error loading KYC information: $e');
    }
  }

  Color getStatusColor(String status) {
    switch (status) {
      case 'Active':
        return Colors.green;
      case 'Inactive':
      case 'InActive':
        return Colors.red;
      case 'InProgress':
      case 'Inprogress':
        return Colors.yellow;
      default:
        return Colors.grey;
    }
  }

  Future<void> loadEmployerFromSupabase() async {
    try {
      isLoading.value = true;

      // Get current user
      final currentUser = supabase.auth.currentUser;
      if (currentUser == null || currentUser.email == null) {
        throw Exception('No authenticated user found or user has no email');
      }

      final email = currentUser.email;
      print('Trying to load employer with email: $email');

      // Query the employers table from Supabase
      final response =
          await supabase
              .from('employers')
              .select()
              .eq('contact_email', email!) // Use contact_email not email
              .maybeSingle(); // Use maybeSingle instead of single

      if (response == null) {
        throw Exception('No employer found with email: $email');
      }

      // Convert the response to an Employer object
      final employerData = Employer.fromJson(response);
      employer.value = employerData;

      // Update UI observables with the loaded data
      updateUIFromEmployer(employerData);

      // Load KYC information
      await loadKycInformation(currentUser.id);
    } catch (e) {
      hasError.value = true;
      errorMessage.value = e.toString();
      print('Error loading employer from Supabase: $e');

      // If there's an error, try to load all employers as fallback
      loadAllEmployers();
    } finally {
      isLoading.value = false;
    }
  }

  Future<void> loadAllEmployers() async {
    try {
      // Query all records from the employers table
      final response = await supabase
          .from('employers')
          .select()
          .limit(1); // Just get the first employer as fallback

      if (response != null && response.isNotEmpty) {
        final employerData = Employer.fromJson(response[0]);
        employer.value = employerData;

        // Update UI observables
        updateUIFromEmployer(employerData);
      } else {
        throw Exception('No employer records found');
      }
    } catch (e) {
      print('Error in fallback loading: $e');
      // Use sample data as last resort
      loadSampleData();
    }
  }

  // Method to load sample data if all else fails
  void loadSampleData() {
    try {
      // Sample data from the CSV analysis
      final sampleData = {
        'id': '9367f1d8-4441-42e9-a783-cb073fe96434',
        'company_name': 'safestorage',
        'website': 'https://www.safestorage.in',
        'contact_name': 'kushal',
        'contact_email': 'kushal@safestorage.in',
        'contact_phone': '9036140106',
        'created_at': '2025-04-05 12:38:22.377939+00',
        'updated_at': '2025-04-05 12:38:22.380751+00',
      };

      final employerData = Employer.fromJson(sampleData);
      employer.value = employerData;

      // Update UI observable
      updateUIFromEmployer(employerData);
    } catch (e) {
      print('Error loading sample data: $e');
    }
  }

  // Helper method to get current employer ID (implement based on your auth system)
  String getCurrentEmployerId() {
    // In a real app, you would get this from your authentication system
    // For example: return supabase.auth.currentUser?.id ?? '';

    // For demo purposes, return a sample ID
    return '9367f1d8-4441-42e9-a783-cb073fe96434';
  }

  Widget _buildKycInformation(
    BuildContext context,
    EmployerProfileController controller,
  ) {
    final theme = Theme.of(context);
    return Container(
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(12),
        boxShadow: [
          BoxShadow(
            color: Colors.grey.shade300,
            blurRadius: 4,
            offset: const Offset(0, 2),
          ),
        ],
      ),
      padding: const EdgeInsets.all(16),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            mainAxisAlignment: MainAxisAlignment.spaceBetween,
            children: [
              Text('KYC Information', style: theme.textTheme.titleMedium),
              Obx(
                () => Container(
                  padding: const EdgeInsets.symmetric(
                    horizontal: 8,
                    vertical: 4,
                  ),
                  decoration: BoxDecoration(
                    color:
                        controller.isVerified.value
                            ? Colors.green.shade100
                            : Colors.orange.shade100,
                    borderRadius: BorderRadius.circular(8),
                  ),
                  child: Text(
                    controller.isVerified.value ? 'Verified' : 'Pending',
                    style: TextStyle(
                      color:
                          controller.isVerified.value
                              ? Colors.green.shade800
                              : Colors.orange.shade800,
                      fontWeight: FontWeight.bold,
                    ),
                  ),
                ),
              ),
            ],
          ),
          const SizedBox(height: 12),

          // PAN Number
          Row(
            children: [
              Expanded(
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(
                      'PAN Number',
                      style: theme.textTheme.bodySmall?.copyWith(
                        color: Colors.grey.shade600,
                      ),
                    ),
                    Obx(
                      () => Text(
                        controller.panNumber.value.isEmpty
                            ? 'Not provided'
                            : controller.panNumber.value,
                        style: theme.textTheme.bodyMedium,
                      ),
                    ),
                  ],
                ),
              ),
            ],
          ),

          const SizedBox(height: 16),

          // Document previews
          Row(
            children: [
              // PAN Card Preview
              Expanded(
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(
                      'PAN Card',
                      style: theme.textTheme.bodySmall?.copyWith(
                        color: Colors.grey.shade600,
                      ),
                    ),
                    const SizedBox(height: 8),
                    Obx(
                      () =>
                          controller.panPhotoUrl.value.isNotEmpty
                              ? Container(
                                height: 100,
                                decoration: BoxDecoration(
                                  borderRadius: BorderRadius.circular(8),
                                  border: Border.all(
                                    color: Colors.grey.shade300,
                                  ),
                                  image: DecorationImage(
                                    image: NetworkImage(
                                      controller.panPhotoUrl.value,
                                    ),
                                    fit: BoxFit.cover,
                                  ),
                                ),
                              )
                              : Container(
                                height: 100,
                                decoration: BoxDecoration(
                                  color: Colors.grey.shade100,
                                  borderRadius: BorderRadius.circular(8),
                                  border: Border.all(
                                    color: Colors.grey.shade300,
                                  ),
                                ),
                                child: Center(
                                  child: Text(
                                    'No document',
                                    style: TextStyle(
                                      color: Colors.grey.shade600,
                                    ),
                                  ),
                                ),
                              ),
                    ),
                  ],
                ),
              ),

              const SizedBox(width: 16),

              // Incorporation Certificate Preview
              Expanded(
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(
                      'Incorporation Certificate',
                      style: theme.textTheme.bodySmall?.copyWith(
                        color: Colors.grey.shade600,
                      ),
                    ),
                    const SizedBox(height: 8),
                    Obx(
                      () =>
                          controller
                                  .incorporationCertificateUrl
                                  .value
                                  .isNotEmpty
                              ? Container(
                                height: 100,
                                decoration: BoxDecoration(
                                  borderRadius: BorderRadius.circular(8),
                                  border: Border.all(
                                    color: Colors.grey.shade300,
                                  ),
                                  image: DecorationImage(
                                    image: NetworkImage(
                                      controller
                                          .incorporationCertificateUrl
                                          .value,
                                    ),
                                    fit: BoxFit.cover,
                                  ),
                                ),
                              )
                              : Container(
                                height: 100,
                                decoration: BoxDecoration(
                                  color: Colors.grey.shade100,
                                  borderRadius: BorderRadius.circular(8),
                                  border: Border.all(
                                    color: Colors.grey.shade300,
                                  ),
                                ),
                                child: Center(
                                  child: Text(
                                    'No document',
                                    style: TextStyle(
                                      color: Colors.grey.shade600,
                                    ),
                                  ),
                                ),
                              ),
                    ),
                  ],
                ),
              ),
            ],
          ),

          // Verify Button (if not verified)
          Obx(
            () =>
                controller.isVerified.value
                    ? SizedBox()
                    : Padding(
                      padding: const EdgeInsets.only(top: 16.0),
                      child: Center(
                        child: ElevatedButton.icon(
                          onPressed: () {
                            // Navigate to verification form
                            Navigator.push(
                              context,
                              MaterialPageRoute(
                                builder:
                                    (context) => StandaloneVerificationForm(
                                      onComplete:
                                          () =>
                                              controller
                                                  .loadEmployerFromSupabase(),
                                    ),
                              ),
                            );
                          },
                          icon: Icon(Icons.verified_user),
                          label: Text('Complete Verification'),
                          style: ElevatedButton.styleFrom(
                            backgroundColor: Colors.blue,
                            foregroundColor: Colors.white,
                            padding: EdgeInsets.symmetric(
                              horizontal: 16,
                              vertical: 12,
                            ),
                          ),
                        ),
                      ),
                    ),
          ),
        ],
      ),
    );
  }

  void onClose() {
    // Dispose the text editing controllers when the controller is closed
    companyNameController.dispose();
    websiteController.dispose();
    contactNameController.dispose();
    phoneNumberController.dispose();
    super.onClose();
  }

  // Method to update UI variables from loaded employer data
  void updateUIFromEmployer(Employer employerData) {
    companyName.value = employerData.companyName.trim();
    website.value = employerData.website;
    phoneNumber.value = employerData.contactPhone;
    contactName.value = employerData.contactName;
    emailAddress.value = employerData.contactEmail;
    // Update the text editing controllers
    companyNameController.text = employerData.companyName.trim();
    websiteController.text = employerData.website;
    contactNameController.text = employerData.contactName;
    phoneNumberController.text = employerData.contactPhone;
  }

  // Team Members
  final teamMembers =
      <TeamMember>[
        TeamMember(
          name: 'John Doe',
          email: 'john.doe@techvision.com',
          role: 'Owner',
          roleColor: Colors.blue,
          initials: 'JD',
          initialsColor: Colors.blue.shade100,
        ),
        TeamMember(
          name: 'Sarah Johnson',
          email: 'sarah.j@techvision.com',
          role: 'Manager',
          roleColor: Colors.green,
          initials: 'SJ',
          initialsColor: Colors.green.shade100,
        ),
        TeamMember(
          name: 'Robert Lee',
          email: 'robert.l@techvision.com',
          role: 'Recruiter',
          roleColor: Colors.orange,
          initials: 'RL',
          initialsColor: Colors.orange.shade100,
        ),
      ].obs;

  // Account Statistics
  final accountAge = '7 years'.obs;
  final jobsPosted = 248.obs;
  final workersHired = 183.obs;
  final averageRating = 4.8.obs;
}

class TeamMember {
  final String name;
  final String email;
  final String role;
  final Color roleColor;
  final String initials;
  final Color initialsColor;

  TeamMember({
    required this.name,
    required this.email,
    required this.role,
    required this.roleColor,
    required this.initials,
    required this.initialsColor,
  });
}

// Note: EmployerProfileController and model classes remain unchanged

class EmployerProfileScreen extends StatelessWidget {
  const EmployerProfileScreen({Key? key}) : super(key: key);

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
          MaterialPageRoute(builder: (_) => const EmployerLoginPage()),
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

  @override
  Widget build(BuildContext context) {
    final controller = Get.put(EmployerProfileController());

    // ✅ Define reactive edit button outside AppBar
    final editButton = Obx(() {
      final isEditing = controller.isEditing.value;
      return Container(
        decoration: BoxDecoration(
          color: isEditing ? Colors.green.shade700 : Colors.blue.shade700,
          borderRadius: BorderRadius.circular(12),
        ),
        child: InkWell(
          onTap: () {
            if (isEditing) {
              controller.updateEmployerInSupabase();
            }
            controller.isEditing.toggle();
          },
          child: Padding(
            padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 10),
            child: Row(
              mainAxisSize: MainAxisSize.min,
              children: [
                Icon(
                  isEditing ? Icons.check : Icons.edit,
                  color: Colors.white,
                  size: 16,
                ),
                SizedBox(width: 4),
                Text(
                  isEditing ? 'Save' : 'Edit',
                  style: TextStyle(
                    color: Colors.white,
                    fontWeight: FontWeight.w600,
                    fontSize: 14,
                  ),
                ),
              ],
            ),
          ),
        ),
      );
    });

    return Scaffold(
      backgroundColor: const Color(0xFFF6F7FB),
      appBar: StandardAppBar(
        title: 'Employer Profile',
        centerTitle: false,
        actions: [
          editButton, // ✅ Use prebuilt reactive widget here
          _buildDropdownMenu(context),
        ],
      ),
      bottomNavigationBar: const ShiftHourBottomNavigation(),
      body: Obx(() {
        if (controller.isLoading.value) {
          return const Center(child: CircularProgressIndicator());
        }

        if (controller.hasError.value) {
          return Center(
            child: Column(
              mainAxisAlignment: MainAxisAlignment.center,
              children: [
                Text('Error: \${controller.errorMessage.value}'),
                const SizedBox(height: 16),
                ElevatedButton(
                  onPressed: () => controller.loadEmployerFromSupabase(),
                  child: const Text('Retry Loading Data'),
                ),
              ],
            ),
          );
        }

        return SingleChildScrollView(
          child: Column(
            children: [
              Container(
                padding: const EdgeInsets.symmetric(horizontal: 20),
                child: Column(
                  children: [
                    const SizedBox(height: 20),
                    _buildCompanyHeader(context, controller),
                    const SizedBox(height: 20),
                    _buildCompanyInfoCard(context, controller),
                    const SizedBox(height: 20),
                    _buildContactInfoCard(context, controller),
                    const SizedBox(height: 20),
                    _buildKycInfoCard(context, controller),
                    const SizedBox(height: 20),
                  ],
                ),
              ),
            ],
          ),
        );
      }),
    );
  }

  Widget _buildCompanyHeader(
    BuildContext context,
    EmployerProfileController controller,
  ) {
    return Obx(() {
      return Container(
        margin: const EdgeInsets.symmetric(horizontal: 8),
        decoration: BoxDecoration(
          borderRadius: BorderRadius.circular(20),
          color: Colors.white,
          boxShadow: [
            BoxShadow(
              color: Colors.black12,
              blurRadius: 10,
              offset: Offset(0, 4),
            ),
          ],
        ),
        child: Column(
          children: [
            Stack(
              alignment: Alignment.center,
              clipBehavior: Clip.none,
              children: [
                // Top gradient
                Container(
                  height: 100,
                  decoration: const BoxDecoration(
                    borderRadius: BorderRadius.only(
                      topLeft: Radius.circular(20),
                      topRight: Radius.circular(20),
                    ),
                    gradient: LinearGradient(
                      colors: [Color(0xFF6366F1), Color(0xFF8B5CF6)],
                      begin: Alignment.topLeft,
                      end: Alignment.bottomRight,
                    ),
                  ),
                ),

                // Profile avatar
                Positioned(
                  bottom: -40,
                  child: Container(
                    padding: const EdgeInsets.all(3),
                    decoration: BoxDecoration(
                      shape: BoxShape.circle,
                      color: Colors.white,
                    ),
                    child: CircleAvatar(
                      radius: 40,
                      backgroundColor: Colors.grey.shade400,
                      child: Text(
                        controller.companyName.value.isNotEmpty
                            ? controller.companyName.value[0].toUpperCase()
                            : 'S',
                        style: const TextStyle(
                          fontSize: 28,
                          fontWeight: FontWeight.bold,
                          color: Colors.white,
                        ),
                      ),
                    ),
                  ),
                ),
              ],
            ),
            const SizedBox(height: 50),

            // Company name
            Text(
              controller.companyName.value,
              style: const TextStyle(fontSize: 20, fontWeight: FontWeight.bold),
            ),
            const SizedBox(height: 8),

            // ID and status badge
            Container(
              padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 6),
              decoration: BoxDecoration(
                border: Border.all(color: Colors.green),
                borderRadius: BorderRadius.circular(20),
              ),
              child: Row(
                mainAxisSize: MainAxisSize.min,
                children: [
                  const Icon(Icons.circle, size: 10, color: Colors.green),
                  const SizedBox(width: 6),
                  Text(
                    "${controller.getCurrentEmployerId().substring(0, 4)} ",
                    style: const TextStyle(
                      color: Colors.green,
                      fontWeight: FontWeight.bold,
                    ),
                  ),
                  const Text(
                    "Active",
                    style: TextStyle(
                      color: Colors.green,
                      fontWeight: FontWeight.bold,
                    ),
                  ),
                ],
              ),
            ),
            const SizedBox(height: 12),

            // Website
            Text(
              controller.website.value,
              style: const TextStyle(color: Colors.grey, fontSize: 14),
            ),
            const SizedBox(height: 16),
          ],
        ),
      );
    });
  }

  Widget _buildInfoField(
    BuildContext context,
    String label,
    String value,
    IconData icon, {
    bool isLink = false,
    bool isGrey = false,
    bool isSmallScreen = false,
  }) {
    return Container(
      width: double.infinity,
      padding: EdgeInsets.all(isSmallScreen ? 10 : 12),
      decoration: BoxDecoration(
        color: Color(0xFFF9FAFC),
        borderRadius: BorderRadius.circular(8),
        border: Border.all(color: Colors.grey.shade200),
      ),
      child: Row(
        children: [
          Icon(icon, color: Colors.blue, size: isSmallScreen ? 18 : 20),
          SizedBox(width: isSmallScreen ? 10 : 12),
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(
                  label,
                  style: TextStyle(
                    fontSize: isSmallScreen ? 13 : 14,
                    color: Colors.grey.shade600,
                    fontWeight: FontWeight.w500,
                  ),
                ),
                SizedBox(height: 4),
                Text(
                  value,
                  style: TextStyle(
                    fontSize: isSmallScreen ? 14 : 16,
                    fontWeight: FontWeight.w500,
                    color:
                        isLink
                            ? Colors.blue
                            : (isGrey ? Colors.grey.shade600 : Colors.black87),
                  ),
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildContactInfoCard(
    BuildContext context,
    EmployerProfileController controller,
  ) {
    return Container(
      width: double.infinity,
      padding: EdgeInsets.all(20),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(16),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          // Header with icon
          Row(
            children: [
              Icon(Icons.contacts_rounded, color: Colors.blue, size: 22),
              SizedBox(width: 8),
              Text(
                'Contact Information',
                style: TextStyle(
                  fontSize: 18,
                  fontWeight: FontWeight.bold,
                  color: Colors.black,
                ),
              ),
            ],
          ),
          SizedBox(height: 20),

          // Contact name field
          Obx(() {
            return controller.isEditing.value
                ? TextField(
                  controller: controller.contactNameController,
                  onChanged: (value) => controller.contactName.value = value,
                  decoration: InputDecoration(
                    labelText: 'Contact Name',
                    contentPadding: EdgeInsets.symmetric(
                      horizontal: 12,
                      vertical: 8,
                    ),
                    border: OutlineInputBorder(),
                  ),
                )
                : Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(
                      'Contact Name',
                      style: TextStyle(
                        fontSize: 14,
                        color: Colors.grey.shade600,
                      ),
                    ),
                    SizedBox(height: 4),
                    Text(
                      controller.contactName.value,
                      style: TextStyle(fontSize: 16),
                    ),
                  ],
                );
          }),
          SizedBox(height: 16),
          // Email field
          _buildInfoField(
            context,
            'Email Address',
            controller.emailAddress.value,
            Icons.email_rounded,
          ),
          SizedBox(height: 16),
          SizedBox(height: 16), SizedBox(height: 16),

          // Phone number field
          Obx(() {
            return controller.isEditing.value
                ? TextField(
                  controller: controller.phoneNumberController,
                  onChanged: (value) => controller.phoneNumber.value = value,
                  decoration: InputDecoration(
                    labelText: 'Phone Number',
                    contentPadding: EdgeInsets.symmetric(
                      horizontal: 12,
                      vertical: 8,
                    ),
                    border: OutlineInputBorder(),
                  ),
                )
                : Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(
                      'Phone Number',
                      style: TextStyle(
                        fontSize: 14,
                        color: Colors.grey.shade600,
                      ),
                    ),
                    SizedBox(height: 4),
                    Text(
                      controller.phoneNumber.value,
                      style: TextStyle(fontSize: 16),
                    ),
                  ],
                );
          }),
        ],
      ),
    );
  }

  Widget _buildCompanyInfoCard(
    BuildContext context,
    EmployerProfileController controller,
  ) {
    return Container(
      width: double.infinity,
      padding: EdgeInsets.all(20),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(16),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          // Header with icon
          Row(
            children: [
              Icon(Icons.business_rounded, color: Colors.blue, size: 22),
              SizedBox(width: 8),
              Text(
                'Company Information',
                style: TextStyle(
                  fontSize: 18,
                  fontWeight: FontWeight.bold,
                  color: Colors.black,
                ),
              ),
            ],
          ),
          SizedBox(height: 20),
          _buildInfoField(
            context,
            'Company Name',
            controller.companyName.value,
            Icons.description_outlined,
            isGrey: true,
          ),
          SizedBox(height: 16),
          // Website field
          _buildInfoField(
            context,
            'Website',
            controller.website.value,
            Icons.language_rounded,
            isLink: true,
          ),

          // Company description field
        ],
      ),
    );
  }

  Widget _buildKycInfoCard(
    BuildContext context,
    EmployerProfileController controller,
  ) {
    return Container(
      width: double.infinity,
      padding: EdgeInsets.all(20),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(16),
      ),
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
                      color: Colors.black,
                    ),
                  ),
                ],
              ),
              Obx(
                () => Container(
                  padding: EdgeInsets.symmetric(horizontal: 12, vertical: 6),
                  decoration: BoxDecoration(
                    color:
                        controller.isVerified.value
                            ? Color(0xFFEDF7ED) // Light green
                            : Color(0xFFFFF8E6), // Light yellow
                    borderRadius: BorderRadius.circular(50),
                  ),
                  child: Text(
                    controller.isVerified.value ? 'Verified' : 'Pending',
                    style: TextStyle(
                      color:
                          controller.isVerified.value
                              ? Color(0xFF1E8E3E) // Dark green
                              : Color(0xFF7A5C00), // Dark yellow/amber
                      fontWeight: FontWeight.w500,
                      fontSize: 14,
                    ),
                  ),
                ),
              ),
            ],
          ),
          SizedBox(height: 20),

          // Documents section
          Row(
            children: [
              Expanded(
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(
                      'PAN Card',
                      style: TextStyle(
                        fontSize: 14,
                        fontWeight: FontWeight.w500,
                        color: Colors.grey.shade700,
                      ),
                    ),
                    SizedBox(height: 8),
                    Obx(
                      () => _buildDocumentThumbnail(
                        context,
                        controller.panPhotoUrl.value,
                        'PAN Card',
                        Icons.image,
                        () => _showImagePopup(
                          context,
                          controller.panPhotoUrl.value,
                          'PAN Card',
                        ),
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
                      'Incorporation Certificate',
                      style: TextStyle(
                        fontSize: 14,
                        fontWeight: FontWeight.w500,
                        color: Colors.grey.shade700,
                      ),
                    ),
                    SizedBox(height: 8),
                    Obx(
                      () => _buildDocumentThumbnail(
                        context,
                        controller.incorporationCertificateUrl.value,
                        'Incorporation Certificate',
                        Icons.description,
                        () => _showImagePopup(
                          context,
                          controller.incorporationCertificateUrl.value,
                          'Incorporation Certificate',
                        ),
                      ),
                    ),
                  ],
                ),
              ),
            ],
          ),

          // Verification button if not verified
          Obx(
            () =>
                controller.isVerified.value
                    ? SizedBox.shrink()
                    : Padding(
                      padding: const EdgeInsets.only(top: 20.0),
                      child: Center(
                        child: ElevatedButton.icon(
                          onPressed: () {
                            Navigator.push(
                              context,
                              MaterialPageRoute(
                                builder:
                                    (context) => StandaloneVerificationForm(
                                      onComplete:
                                          () =>
                                              controller
                                                  .loadEmployerFromSupabase(),
                                    ),
                              ),
                            );
                          },
                          icon: Icon(Icons.verified_user),
                          label: Text('Complete Verification'),
                          style: ElevatedButton.styleFrom(
                            backgroundColor: Colors.blue,
                            foregroundColor: Colors.white,
                            padding: EdgeInsets.symmetric(
                              horizontal: 20,
                              vertical: 12,
                            ),
                            shape: RoundedRectangleBorder(
                              borderRadius: BorderRadius.circular(10),
                            ),
                          ),
                        ),
                      ),
                    ),
          ),
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
    return GestureDetector(
      onTap: url.isNotEmpty ? onTap : null,
      child: Container(
        height: 120,
        decoration: BoxDecoration(
          color: Colors.grey.shade100,
          borderRadius: BorderRadius.circular(8),
          border: Border.all(color: Colors.grey.shade300),
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
                              color: Colors.grey.shade400,
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
                      Icon(fallbackIcon, size: 36, color: Colors.grey.shade400),
                      SizedBox(height: 8),
                      Text(
                        'No document',
                        style: TextStyle(
                          color: Colors.grey.shade600,
                          fontSize: 12,
                        ),
                      ),
                    ],
                  ),
                ),
      ),
    );
  }
}
