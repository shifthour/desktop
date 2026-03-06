import 'package:flutter/material.dart';
import 'package:shifthour_employeer/Employer/employer_dashboard.dart';
import 'package:supabase_flutter/supabase_flutter.dart';

class EmployerProfileSetupScreen extends StatefulWidget {
  final String? initialEmail;
  const EmployerProfileSetupScreen({Key? key, this.initialEmail})
    : super(key: key);

  @override
  State<EmployerProfileSetupScreen> createState() =>
      _EmployerProfileSetupScreenState();
}

class _EmployerProfileSetupScreenState
    extends State<EmployerProfileSetupScreen> {
  final _formKey = GlobalKey<FormState>();
  final _scrollController = ScrollController();
  bool _showScrollToTopButton = false;

  // Employer form controllers
  final companyNameController = TextEditingController();
  final companyWebsiteController = TextEditingController();
  final contactPersonController = TextEditingController();
  final contactEmailController = TextEditingController();
  final contactPhoneController = TextEditingController();

  bool _isLoading = false;

  // Map to keep track of field focus nodes
  final Map<String, FocusNode> _focusNodes = {};

  @override
  void initState() {
    super.initState();
    _scrollController.addListener(_scrollListener);

    // If an initial email is provided, set it in the email controller
    if (widget.initialEmail != null) {
      contactEmailController.text = widget.initialEmail!;
    }

    // Initialize focus nodes for each field
    _focusNodes['companyName'] = FocusNode();
    _focusNodes['companyWebsite'] = FocusNode();
    _focusNodes['contactPerson'] = FocusNode();
    _focusNodes['contactEmail'] = FocusNode();
    _focusNodes['contactPhone'] = FocusNode();

    // Add listeners to focus nodes to scroll into view when focused
    _focusNodes.forEach((key, focusNode) {
      focusNode.addListener(() {
        if (focusNode.hasFocus) {
          _ensureFieldIsVisible(key);
        }
      });
    });
  }

  void _ensureFieldIsVisible(String fieldKey) {
    // Wait for keyboard to appear
    Future.delayed(const Duration(milliseconds: 300), () {
      if (!mounted) return;

      double position;

      // Set scroll position based on which field has focus
      switch (fieldKey) {
        case 'companyName':
          position = 300; // Adjusted for header content
          break;
        case 'companyWebsite':
          position = 400; // Adjusted for header content
          break;
        case 'contactPerson':
          position = 500; // Adjusted for header content
          break;
        case 'contactEmail':
          position = 600; // Adjusted for header content
          break;
        case 'contactPhone':
          position = 700; // Adjusted for header content
          break;
        default:
          position = 300; // Adjusted for header content
      }

      // Scroll to the position if controller is attached
      if (_scrollController.hasClients) {
        _scrollController.animateTo(
          position,
          duration: const Duration(milliseconds: 300),
          curve: Curves.easeInOut,
        );
      }
    });
  }

  @override
  void dispose() {
    _scrollController.removeListener(_scrollListener);
    _scrollController.dispose();
    companyNameController.dispose();
    companyWebsiteController.dispose();
    contactPersonController.dispose();
    contactEmailController.dispose();
    contactPhoneController.dispose();

    // Dispose all focus nodes
    _focusNodes.values.forEach((focusNode) {
      focusNode.dispose();
    });

    super.dispose();
  }

  void _scrollListener() {
    if (_scrollController.offset >= 200 && !_showScrollToTopButton) {
      setState(() {
        _showScrollToTopButton = true;
      });
    } else if (_scrollController.offset < 200 && _showScrollToTopButton) {
      setState(() {
        _showScrollToTopButton = false;
      });
    }
  }

  void _scrollToTop() {
    _scrollController.animateTo(
      0,
      duration: const Duration(milliseconds: 500),
      curve: Curves.easeInOut,
    );
  }

  @override
  Widget build(BuildContext context) {
    // Check if keyboard is visible
    final isKeyboardVisible = MediaQuery.of(context).viewInsets.bottom > 0;

    return GestureDetector(
      onTap: () {
        // Dismiss keyboard when tapping outside of text fields
        FocusScope.of(context).unfocus();
        FocusManager.instance.primaryFocus?.unfocus();
      },
      child: Scaffold(
        backgroundColor: const Color(0xFFF8FAFF),
        resizeToAvoidBottomInset: true, // Resize for keyboard
        appBar: AppBar(
          backgroundColor: const Color(0xFF5B6BF8),
          automaticallyImplyLeading: false,
          actions: [],
          centerTitle: false,
          elevation: 0,
        ),
        body: SafeArea(
          bottom: false, // Don't pad bottom to allow full scroll
          child: Scrollbar(
            controller: _scrollController,
            child: SingleChildScrollView(
              controller: _scrollController,
              keyboardDismissBehavior: ScrollViewKeyboardDismissBehavior.onDrag,
              child: Padding(
                padding: EdgeInsets.fromLTRB(
                  24,
                  24,
                  24,
                  isKeyboardVisible ? 80 : 24,
                ),
                child: Column(
                  mainAxisSize: MainAxisSize.max,
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    // Logo and Header - now part of the scrollable area
                    Center(
                      child: Column(
                        children: [
                          Container(
                            width: 50,
                            height: 50,
                            decoration: const BoxDecoration(
                              shape: BoxShape.circle,
                              gradient: LinearGradient(
                                colors: [Color(0xFF5B6BF8), Color(0xFF8B65D9)],
                                begin: Alignment.topLeft,
                                end: Alignment.bottomRight,
                              ),
                            ),
                            child: const Icon(
                              Icons.calendar_today,
                              color: Colors.white,
                              size: 25,
                            ),
                          ),
                          const SizedBox(height: 10),
                          Row(
                            mainAxisAlignment: MainAxisAlignment.center,
                            children: [
                              Text(
                                'Shift',
                                style: TextStyle(
                                  fontSize: 20,
                                  fontWeight: FontWeight.bold,
                                  color: Colors.blue.shade700,
                                ),
                              ),
                              Text(
                                'Hour',
                                style: TextStyle(
                                  fontSize: 20,
                                  fontWeight: FontWeight.bold,
                                  color: Colors.purple.shade500,
                                ),
                              ),
                            ],
                          ),
                          const SizedBox(height: 12),
                          Text(
                            'Complete your employer profile',
                            style: const TextStyle(
                              fontSize: 16,
                              fontWeight: FontWeight.w600,
                            ),
                          ),
                          const SizedBox(height: 4),
                          Text(
                            'These details will help you find matching shifts',
                            style: TextStyle(
                              fontSize: 14,
                              color: Colors.grey.shade600,
                            ),
                            textAlign: TextAlign.center,
                          ),
                        ],
                      ),
                    ),
                    const SizedBox(height: 24),

                    // Only show this when keyboard is not visible
                    if (!isKeyboardVisible) ...[
                      const Padding(
                        padding: EdgeInsets.only(bottom: 16),
                        child: Text(
                          'Company Information',
                          style: TextStyle(
                            fontFamily: 'Inter Tight',
                            fontSize: 20,
                            fontWeight: FontWeight.bold,
                          ),
                        ),
                      ),

                      // Info banner
                      Padding(
                        padding: const EdgeInsets.only(top: 10),
                        child: Container(
                          width: double.infinity,
                          decoration: BoxDecoration(
                            color: const Color(0xFFEBF2FF),
                            borderRadius: BorderRadius.circular(12),
                          ),
                        ),
                      ),
                    ],

                    // Form Card
                    Padding(
                      padding: EdgeInsets.only(top: isKeyboardVisible ? 8 : 24),
                      child: Form(
                        key: _formKey,
                        autovalidateMode: AutovalidateMode.disabled,
                        child: Container(
                          width: double.infinity,
                          decoration: BoxDecoration(
                            color: Colors.white,
                            boxShadow: const [
                              BoxShadow(
                                blurRadius: 15,
                                color: Color(0x0F000000),
                                offset: Offset(0, 5),
                              ),
                            ],
                            borderRadius: BorderRadius.circular(20),
                          ),
                          child: Padding(
                            padding: const EdgeInsets.all(16),
                            child: Column(
                              mainAxisSize: MainAxisSize.max,
                              crossAxisAlignment: CrossAxisAlignment.start,
                              children: [
                                // Company Name Field
                                _buildFormField(
                                  key: 'companyName',
                                  label: 'Company Name',
                                  hintText: 'e.g. Acme Corp',
                                  controller: companyNameController,
                                  icon: Icons.business,
                                  focusNode: _focusNodes['companyName']!,
                                ),
                                const SizedBox(height: 20),

                                // Company Website Field
                                _buildFormField(
                                  key: 'companyWebsite',
                                  label: 'Company Website',
                                  hintText: 'e.g. https://acme.com',
                                  controller: companyWebsiteController,
                                  keyboardType: TextInputType.url,
                                  icon: Icons.language,
                                  focusNode: _focusNodes['companyWebsite']!,
                                ),
                                const SizedBox(height: 20),

                                // Contact Person Field
                                _buildFormField(
                                  key: 'contactPerson',
                                  label: 'Contact Person Name',
                                  hintText: 'e.g. Ravi Kumar',
                                  controller: contactPersonController,
                                  icon: Icons.person,
                                  focusNode: _focusNodes['contactPerson']!,
                                ),
                                const SizedBox(height: 20),

                                // Contact Email Field
                                _buildFormField(
                                  key: 'contactEmail',
                                  label: 'Contact Email',
                                  hintText: 'e.g. ravi.kumar@gmail.com',
                                  controller: contactEmailController,
                                  keyboardType: TextInputType.emailAddress,
                                  focusNode: _focusNodes['contactEmail']!,
                                  readOnly: true,
                                ),
                                const SizedBox(height: 20),

                                // Contact Phone Field
                                _buildFormField(
                                  key: 'contactPhone',
                                  label: 'Contact Phone',
                                  hintText: 'e.g. +91 98765-43210',
                                  controller: contactPhoneController,
                                  keyboardType: TextInputType.phone,
                                  icon: Icons.phone,
                                  focusNode: _focusNodes['contactPhone']!,
                                ),
                                const SizedBox(height: 30),

                                // Submit Button
                                Container(
                                  width: double.infinity,
                                  height: 55,
                                  decoration: BoxDecoration(
                                    borderRadius: BorderRadius.circular(28),
                                    boxShadow: [
                                      BoxShadow(
                                        color: const Color(
                                          0xFF5B6BF8,
                                        ).withOpacity(0.3),
                                        blurRadius: 10,
                                        offset: const Offset(0, 5),
                                      ),
                                    ],
                                  ),
                                  child: ElevatedButton(
                                    onPressed:
                                        _isLoading
                                            ? null
                                            : () {
                                              if (_formKey.currentState!
                                                  .validate()) {
                                                setState(() {
                                                  _isLoading = true;
                                                });
                                                _saveEmployerProfile(
                                                  companyName:
                                                      companyNameController
                                                          .text,
                                                  companyWebsite:
                                                      companyWebsiteController
                                                          .text,
                                                  contactPerson:
                                                      contactPersonController
                                                          .text,
                                                  contactEmail:
                                                      contactEmailController
                                                          .text,
                                                  contactPhone:
                                                      contactPhoneController
                                                          .text,
                                                );
                                              }
                                            },
                                    style: ElevatedButton.styleFrom(
                                      backgroundColor: const Color(0xFF5B6BF8),
                                      foregroundColor: Colors.white,
                                      elevation: 0,
                                      shape: RoundedRectangleBorder(
                                        borderRadius: BorderRadius.circular(28),
                                      ),
                                      padding: const EdgeInsets.symmetric(
                                        vertical: 14,
                                      ),
                                    ),
                                    child:
                                        _isLoading
                                            ? const SizedBox(
                                              height: 24,
                                              width: 24,
                                              child: CircularProgressIndicator(
                                                color: Colors.white,
                                                strokeWidth: 2.5,
                                              ),
                                            )
                                            : const Text(
                                              'Complete Profile',
                                              style: TextStyle(
                                                fontSize: 16,
                                                fontWeight: FontWeight.bold,
                                                letterSpacing: 0.5,
                                                fontFamily: 'Inter Tight',
                                              ),
                                            ),
                                  ),
                                ),

                                // Add extra bottom padding when keyboard is visible
                                if (isKeyboardVisible)
                                  const SizedBox(height: 100),
                              ],
                            ),
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
        // Show a floating action button to scroll form when keyboard is visible
        floatingActionButton:
            _showScrollToTopButton
                ? FloatingActionButton(
                  mini: true,
                  backgroundColor: const Color(0xFF5B6BF8),
                  onPressed: _scrollToTop,
                  child: const Icon(
                    Icons.keyboard_arrow_up,
                    color: Colors.white,
                  ),
                )
                : null,
      ),
    );
  }

  Widget _buildFormField({
    required String key,
    required String label,
    required String hintText,
    required TextEditingController controller,
    required FocusNode focusNode,
    TextInputType keyboardType = TextInputType.text,
    IconData? icon,
    bool readOnly = false,
  }) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Padding(
          padding: const EdgeInsets.only(left: 4, bottom: 8),
          child: Text(
            label,
            style: const TextStyle(
              fontSize: 14,
              fontFamily: 'Inter',
              fontWeight: FontWeight.w600,
              color: Color(0xFF4A4A4A),
            ),
          ),
        ),
        TextFormField(
          controller: controller,
          keyboardType: keyboardType,
          readOnly: readOnly,
          focusNode: focusNode,
          decoration: InputDecoration(
            hintText: hintText,
            hintStyle: const TextStyle(
              color: Color(0xFFBDBDBD),
              fontSize: 14,
              fontFamily: 'Inter',
            ),
            filled: true,
            fillColor: const Color(0xFFF9F9F9),
            contentPadding: const EdgeInsets.symmetric(
              vertical: 16,
              horizontal: 20,
            ),
            border: OutlineInputBorder(
              borderRadius: BorderRadius.circular(16),
              borderSide: BorderSide.none,
            ),
            enabledBorder: OutlineInputBorder(
              borderRadius: BorderRadius.circular(16),
              borderSide: const BorderSide(color: Color(0xFFE8E8E8)),
            ),
            focusedBorder: OutlineInputBorder(
              borderRadius: BorderRadius.circular(16),
              borderSide: const BorderSide(
                color: Color(0xFF5B6BF8),
                width: 1.5,
              ),
            ),
            errorBorder: OutlineInputBorder(
              borderRadius: BorderRadius.circular(16),
              borderSide: const BorderSide(color: Color(0xFFEF5350)),
            ),
            prefixIcon:
                icon != null
                    ? Padding(
                      padding: const EdgeInsets.only(left: 12, right: 8),
                      child: Icon(
                        icon,
                        color: const Color(0xFF5B6BF8),
                        size: 22,
                      ),
                    )
                    : null,
            prefixIconConstraints: const BoxConstraints(minWidth: 50),
            errorStyle: const TextStyle(color: Color(0xFFEF5350)),
          ),
          style: TextStyle(
            fontFamily: 'Inter',
            color:
                readOnly ? Colors.grey[700] : null, // Grayed out if read-only
          ),
          cursorColor: const Color(0xFF5B6BF8),
          validator:
              readOnly
                  ? null // Skip validation for read-only fields
                  : (value) =>
                      value == null || value.isEmpty
                          ? 'This field is required'
                          : null,
        ),
      ],
    );
  }

  Future<void> _saveEmployerProfile({
    required String companyName,
    required String companyWebsite,
    required String contactPerson,
    required String contactEmail,
    required String contactPhone,
  }) async {
    final supabase = Supabase.instance.client;

    try {
      print('Starting employer profile save...');
      final userId = Supabase.instance.client.auth.currentUser?.id;
      print('Current user ID: $userId');

      // First save the employer profile
      final employerResponse =
          await supabase.from('employers').insert({
            'id': userId,
            'company_name': companyName,
            'website': companyWebsite,
            'contact_name': contactPerson,
            'contact_email': contactEmail,
            'contact_phone': contactPhone,
            'created_at': DateTime.now().toIso8601String(),
            'updated_at': DateTime.now().toIso8601String(),
          }).select();

      print('Employer insert response: $employerResponse');

      String employerId;

      if (employerResponse != null && employerResponse.isNotEmpty) {
        employerId = employerResponse[0]['id'].toString();
        print('Employer ID from insert response: $employerId');
      } else {
        print('No ID in insert response, querying by email...');
        final employerQuery =
            await supabase
                .from('employers')
                .select('id')
                .eq('contact_email', contactEmail)
                .single();

        employerId = employerQuery['id'].toString();
        print('Employer ID from fallback query: $employerId');
      }

      // Try to create the wallet
      print('Creating wallet for employerId: $employerId');
      await _createEmployerWallet(employerId);
      print('Wallet created successfully.');

      if (mounted) {
        setState(() {
          _isLoading = false;
        });

        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(
            content: Row(
              children: const [
                Icon(Icons.check_circle, color: Colors.white),
                SizedBox(width: 12),
                Text(
                  'Profile saved and wallet created successfully!',
                  style: TextStyle(fontWeight: FontWeight.w500),
                ),
              ],
            ),
            backgroundColor: Colors.green.shade600,
            behavior: SnackBarBehavior.floating,
            shape: RoundedRectangleBorder(
              borderRadius: BorderRadius.circular(10),
            ),
            margin: const EdgeInsets.all(12),
            duration: const Duration(seconds: 4),
          ),
        );

        Navigator.of(context).pushAndRemoveUntil(
          MaterialPageRoute(builder: (context) => const EmployerDashboard()),
          (route) => false,
        );
      }
    } catch (e) {
      print('Error saving employer profile: $e');

      if (mounted) {
        setState(() {
          _isLoading = false;
        });

        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(
            content: Row(
              children: [
                const Icon(Icons.error_outline, color: Colors.white),
                const SizedBox(width: 12),
                Expanded(
                  child: Text(
                    'Error: $e',
                    style: const TextStyle(fontWeight: FontWeight.w500),
                    maxLines: 2,
                    overflow: TextOverflow.ellipsis,
                  ),
                ),
              ],
            ),
            backgroundColor: Colors.red.shade600,
            behavior: SnackBarBehavior.floating,
            shape: RoundedRectangleBorder(
              borderRadius: BorderRadius.circular(10),
            ),
            margin: const EdgeInsets.all(12),
            duration: const Duration(seconds: 4),
          ),
        );
      }
    }
  }

  Future<bool> _createEmployerWallet(String employerId) async {
    final supabase = Supabase.instance.client;

    try {
      // Check if wallet already exists
      final existingWallet =
          await supabase
              .from('employer_wallet')
              .select()
              .eq('employer_id', employerId)
              .maybeSingle();

      if (existingWallet != null) {
        // Wallet already exists, no need to create
        print('Employer wallet already exists');
        return true;
      }

      // Try using the RPC function first
      try {
        await supabase.rpc(
          'create_employer_wallet',
          params: {'p_employer_id': employerId},
        );
        print('Employer wallet created via RPC');
        return true;
      } catch (rpcError) {
        print('RPC error, creating wallet directly: $rpcError');

        // Fallback: Create wallet directly if RPC fails
        final nowIso = DateTime.now().toIso8601String();
        await supabase.from('employer_wallet').insert({
          'employer_id': employerId,
          'balance': 0,
          'currency': 'INR',
          'created_at': nowIso,
          'last_updated': nowIso,
        });

        print('Employer wallet created directly');
        return true;
      }
    } catch (e) {
      print('Error creating employer wallet: $e');

      // If it's a PostgrestException, print more details
      if (e is PostgrestException) {
        print('Supabase Error Details:');
        print('Message: ${e.message}');
        print('Hint: ${e.hint}');
        print('Details: ${e.details}');
      }

      // Rethrow to be handled by the caller
      rethrow;
    }
  }
}
