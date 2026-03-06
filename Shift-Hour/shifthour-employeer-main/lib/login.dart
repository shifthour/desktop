import 'dart:async';
import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:flutter_dotenv/flutter_dotenv.dart';
import 'package:get/get.dart';
import 'package:get/get_core/src/get_main.dart';
import 'package:google_sign_in/google_sign_in.dart';
import 'package:http/http.dart' as http;
import 'package:motion_toast/motion_toast.dart';
import 'package:onesignal_flutter/onesignal_flutter.dart';
import 'package:shifthour_employeer/Employer/employer_dashboard.dart';
import 'package:shifthour_employeer/const/otp.dart';
import 'package:shifthour_employeer/const/otp_helper.dart';
import 'package:shifthour_employeer/main.dart';
import 'package:shifthour_employeer/profile.dart';
import 'package:shifthour_employeer/signup.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import 'dart:convert';
import 'package:crypto/crypto.dart';

class EmployerLoginPage extends StatefulWidget {
  const EmployerLoginPage({Key? key}) : super(key: key);

  @override
  State<EmployerLoginPage> createState() => _EmployerLoginPageState();
}

class _EmployerLoginPageState extends State<EmployerLoginPage> {
  // Form Controllers
  final TextEditingController _emailController = TextEditingController();
  final FocusNode _emailFocusNode = FocusNode();

  // State Variables
  bool _isLoading = false;
  bool _isGoogleSignInInProgress = false;

  // Google Sign-In Configuration

  // Authentication Subscription
  StreamSubscription<AuthState>? _authSubscription;

  @override
  void initState() {
    super.initState();
  }

  Future<void> _handleSignedIn(String email) async {
    // Use try-catch with comprehensive error handling
    try {
      // Check mounted state before async operation
      if (!mounted) return;

      // Navigate based on user profile
      await _navigateBasedOnProfile(email);
      Future.delayed(const Duration(seconds: 2), () async {
        final userId = Supabase.instance.client.auth.currentUser?.id;
        final pushSubscription = OneSignal.User.pushSubscription;

        print('🚀 PUSH SUBSCRIPTION ID after login: ${pushSubscription.id}');
        print(
          '🚀 PUSH SUBSCRIPTION TOKEN after login: ${pushSubscription.token}',
        );

        if (userId != null &&
            pushSubscription.id != null &&
            pushSubscription.token != null) {
          await registerDeviceForUser(userId);
        } else {
          print('❌ Push subscription or userId not ready after login delay.');
        }
      });
    } catch (e) {
      // Log the error for debugging
      print('Authentication verification error: $e');

      // Show user-friendly error message
      if (mounted) {
        _showErrorMessage('Unable to complete sign-in. Please try again.');
      }
    } finally {
      // Ensure state is updated only if widget is still mounted
      if (mounted) {
        setState(() {
          _isLoading = false;
          _isGoogleSignInInProgress = false;
        });
      }
    }
  }

  Future<void> _navigateBasedOnProfile(String email) async {
    final supabase = Supabase.instance.client;
    try {
      final employerData =
          await supabase
              .from('employers')
              .select('id, contact_email')
              .eq('contact_email', email)
              .maybeSingle();

      if (!mounted) return;

      Navigator.pop(context, true);

      if (employerData != null) {
        Get.offAll(() => const EmployerDashboard());
      } else {
        Get.offAll(() => EmployerProfileSetupScreen(initialEmail: email));
      }
    } catch (e) {
      print('❌ Error loading employer from Supabase: $e');
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(
            content: Text('Unable to verify profile. Try again.'),
            backgroundColor: Colors.red,
          ),
        );
        // setState(() => _isVerifying = false);
      }
    }
  }

  // Helper method to generate user-friendly error messages
  String _getErrorMessage(dynamic error) {
    String errorMessage = 'An unexpected error occurred. Please try again.';

    // Check for specific error types and provide more informative messages
    if (error.toString().contains('auth')) {
      errorMessage = 'Network error. Please check your internet connection.';
    } else if (error.toString().contains('timeout')) {
      errorMessage = 'Request timed out. Please try again.';
    } else if (error.toString().contains('invalid')) {
      errorMessage = 'Invalid email address. Please check and try again.';
    } else if (error.toString().contains('rate limit')) {
      errorMessage = 'Too many attempts. Please wait and try again later.';
    }

    return errorMessage;
  }

  void _showErrorMessage(String message) {
    if (mounted) {
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text(message),
          backgroundColor: Colors.red,
          behavior: SnackBarBehavior.floating,
          duration: const Duration(seconds: 3),
        ),
      );
    }
  }

  // Handle sign-out
  void _handleSignedOut() {
    setState(() {
      _isLoading = false;
      _isGoogleSignInInProgress = false;
    });
  }

  GoogleSignIn? _googleSignIn;

  GoogleSignIn get googleSignIn {
    _googleSignIn ??= GoogleSignIn(
      scopes: [
        'email',
        'profile',
        'https://www.googleapis.com/auth/userinfo.profile',
        'openid',
      ],
      serverClientId:
          '947621058450-121ov3cmg40fpkmc8hh5lv7f7ja15bef.apps.googleusercontent.com',
    );
    return _googleSignIn!;
  }

  // Add these helper methods for detailed logging
  void _logAuthStep(String step, [String? details]) {
    final message = details != null ? '$step: $details' : step;
    print('AUTH STEP: $message');
  }

  void _logAuthError(String step, dynamic error) {
    print('AUTH ERROR at $step: $error');
    if (error is Exception) {
      print('Exception type: ${error.runtimeType}');
    }
    print('Stack trace: ${StackTrace.current}');
  }

  Future<void> _handleGoogleLogin() async {
    if (_isGoogleSignInInProgress) return;

    setState(() {
      _isGoogleSignInInProgress = true;
      _isLoading = true;
    });

    _logAuthStep('Started Google Sign-In process');

    try {
      // Ensure previous sessions are cleared
      _logAuthStep('Signing out from previous Google sessions');
      await googleSignIn.signOut();
      _logAuthStep('Successfully signed out from previous session');

      // Try silent sign-in first
      _logAuthStep('Attempting silent sign-in');
      GoogleSignInAccount? googleUser = await googleSignIn.signInSilently();

      if (googleUser == null) {
        _logAuthStep(
          'Silent sign-in returned null, trying interactive sign-in',
        );
        _logAuthStep('Initiating interactive Google Sign-In UI');
        googleUser = await googleSignIn.signIn();
      }

      if (googleUser == null) {
        _logAuthStep('User canceled the sign-in process');
        _handleLoginCancellation();
        return;
      }

      _logAuthStep('Google account selected: ${googleUser.email}');

      // Get authentication details
      _logAuthStep('Requesting authentication tokens');
      final GoogleSignInAuthentication googleAuth =
          await googleUser.authentication;
      _logAuthStep('Authentication tokens received');

      if (googleAuth.idToken == null) {
        _logAuthError('Missing ID Token', 'ID token is null');
        _handleLoginError('Authentication failed. Missing ID token.');
        return;
      }

      _logAuthStep('Tokens validated successfully');

      // Supabase Sign-In
      _logAuthStep('Authenticating with Supabase');
      final response = await Supabase.instance.client.auth.signInWithIdToken(
        provider: OAuthProvider.google,
        idToken: googleAuth.idToken!,
        accessToken: googleAuth.accessToken,
      );

      // Handle successful sign-in
      if (response.user != null) {
        _logAuthStep(
          'Supabase authentication successful: ${response.user!.email}',
        );
        _logAuthStep('Verifying employer profile for: ${response.user!.email}');
        await _verifyEmployerProfile(response.user!.email);
      } else {
        _logAuthError('Supabase Auth Failed', 'Failed to create session');
        _handleLoginError('Failed to create user session');
      }
    } catch (e, stackTrace) {
      _logAuthError('Google Sign-In Process', e);
      print('Stack Trace: $stackTrace');

      String errorMessage = 'Sign-in failed';

      // More specific error messages based on error type
      if (e.toString().contains('network_error')) {
        errorMessage = 'Network error. Please check your connection.';
      } else if (e.toString().contains('canceled')) {
        errorMessage = 'Sign-in was canceled.';
      } else if (e.toString().contains('10:')) {
        errorMessage = 'Developer configuration error. Please contact support.';
      }

      _handleLoginError(errorMessage);
    } finally {
      _logAuthStep('Reset loading state');
      if (mounted) {
        setState(() {
          _isLoading = false;
          _isGoogleSignInInProgress = false;
        });
      }
    }
  }

  void _handleLoginError(String errorMessage) {
    debugPrint('Google Sign-In Error: $errorMessage');

    if (mounted) {
      // Use MotionToast for more visually appealing error presentation
      MotionToast.error(
        title: const Text(
          'Sign-In Failed',
          style: TextStyle(fontWeight: FontWeight.bold),
        ),
        description: Text(errorMessage),
        position: MotionToastPosition.top,
        animationType: AnimationType.slideInFromBottom,
      ).show(context);

      setState(() {
        _isLoading = false;
        _isGoogleSignInInProgress = false;
      });
    }
  }

  void _handleLoginCancellation() {
    if (mounted) {
      setState(() {
        _isLoading = false;
        _isGoogleSignInInProgress = false;
      });
      _showErrorMessage('Google sign-in cancelled');
    }
  }

  Future<void> _verifyEmployerProfile(String? email) async {
    if (email == null) {
      _showErrorMessage('Email not available');
      return;
    }

    try {
      // Check employer profile in database
      final employerData =
          await Supabase.instance.client
              .from('employers')
              .select('id, contact_email')
              .eq('contact_email', email)
              .maybeSingle();

      if (mounted) {
        if (employerData == null) {
          // No profile found - navigate to profile setup
          Get.offAll(() => EmployerProfileSetupScreen(initialEmail: email));
        } else {
          // Existing employer - navigate to dashboard
          Get.offAll(() => const EmployerDashboard());
        }
      }
    } catch (e) {
      if (mounted) {
        _showErrorMessage('Error verifying profile: ${e.toString()}');
        // Optional: Sign out if verification fails
        await Supabase.instance.client.auth.signOut();
        Get.offAll(() => const EmployerLoginPage());
      }
    }
  }

  Future<void> _handleFacebookLogin() async {
    _showErrorMessage('Facebook login not implemented yet');
  }

  Future<void> _handleSendOTP() async {
    final email = _emailController.text.trim();
    print('📱 Starting OTP flow for email: $email');

    // Validate email
    if (!_validateEmail(email)) {
      print('❌ Email validation failed');
      return;
    }

    setState(() => _isLoading = true);
    print('📱 Setting loading state to true');

    try {
      print('📱 Calling getOtp function');
      final otpCode = await getOtp(email, context);
      print('✅ OTP sent successfully');

      if (mounted) {
        setState(() => _isLoading = false);

        // Show OTP dialog if running on web
        if (otpCode.isNotEmpty) {
          await _showOTPDialog(otpCode);
        }

        // 👉 Navigate to full-screen OTP screen
        final result = await Navigator.push<bool>(
          context,
          MaterialPageRoute(
            builder: (_) => OtpVerificationScreen(email: email),
          ),
        );

        if (result == true) {
          await _handleSignedIn(email);
        }
      }
    } catch (e) {
      print('❌ Error in OTP flow: $e');
      if (mounted) {
        setState(() => _isLoading = false);
        MotionToast.error(
          title: const Text(
            'OTP Send Failed',
            style: TextStyle(fontWeight: FontWeight.bold),
          ),
          description: Text(
            _getErrorMessage(e),
            style: const TextStyle(fontSize: 12),
          ),
          position: MotionToastPosition.center,
          animationType: AnimationType.slideInFromTop,
          width: MediaQuery.of(context).size.width * 0.9,
        ).show(context);
      }
    }
  }

  Future<void> _showOTPDialog(String otpCode) async {
    return showDialog<void>(
      context: context,
      barrierDismissible: false,
      builder: (BuildContext dialogContext) => AlertDialog(
        title: const Row(
          children: [
            Icon(Icons.info_outline, color: Color(0xFF5B52FE)),
            SizedBox(width: 8),
            Text('Your OTP Code'),
          ],
        ),
        content: Column(
          mainAxisSize: MainAxisSize.min,
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            const Text(
              'For web testing, your OTP is:',
              style: TextStyle(fontSize: 14),
            ),
            const SizedBox(height: 16),
            Container(
              padding: const EdgeInsets.all(16),
              decoration: BoxDecoration(
                color: const Color(0xFF5B52FE).withOpacity(0.1),
                borderRadius: BorderRadius.circular(8),
                border: Border.all(color: const Color(0xFF5B52FE)),
              ),
              child: Center(
                child: Text(
                  otpCode,
                  style: const TextStyle(
                    fontSize: 32,
                    fontWeight: FontWeight.bold,
                    letterSpacing: 8,
                    color: Color(0xFF5B52FE),
                  ),
                ),
              ),
            ),
            const SizedBox(height: 16),
            const Text(
              '⚠️ In production, use a backend API or Supabase Edge Function to send emails.',
              style: TextStyle(fontSize: 12, color: Colors.grey),
            ),
          ],
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(dialogContext),
            child: const Text('OK, Got it'),
          ),
        ],
      ),
    );
  }

  bool _validateEmail(String email) {
    if (email.isEmpty) {
      _showErrorMessage('Please enter your email');
      return false;
    }

    final emailRegex = RegExp(r'^[^@]+@[^@]+\.[^@]+$');
    if (!emailRegex.hasMatch(email)) {
      _showErrorMessage('Please enter a valid email address');
      return false;
    }

    return true;
  }

  @override
  void dispose() {
    _emailController.dispose();
    _emailFocusNode.dispose();
    _authSubscription?.cancel();

    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final colorScheme = Theme.of(context).colorScheme;
    final textTheme = Theme.of(context).textTheme;

    return GestureDetector(
      onTap: () => FocusScope.of(context).unfocus(),
      child: Scaffold(
        backgroundColor: Colors.grey[100],
        body: GestureDetector(
          onTap: () => FocusScope.of(context).unfocus(),
          child: SafeArea(
            child: Column(
              children: [
                Expanded(
                  child: Align(
                    alignment: Alignment.center,
                    child: Padding(
                      padding: const EdgeInsets.symmetric(horizontal: 24),
                      child: SingleChildScrollView(
                        child: Column(
                          mainAxisAlignment: MainAxisAlignment.center,
                          children: [
                            // Logo
                            Padding(
                              padding: const EdgeInsets.symmetric(vertical: 40),
                              child: Image.asset(
                                'assets/logo.png',
                                width: 220,
                                height: 200,
                                fit: BoxFit.contain,
                                errorBuilder:
                                    (context, error, stackTrace) => Icon(
                                      Icons.business,
                                      size: 100,
                                      color: colorScheme.primary,
                                    ),
                              ),
                            ),

                            // Welcome text
                            Text(
                              'Welcome Back',
                              textAlign: TextAlign.center,
                              style: textTheme.headlineMedium?.copyWith(
                                fontWeight: FontWeight.bold,
                              ),
                            ),
                            const SizedBox(height: 8),

                            Text(
                              'Sign in to continue using the app',
                              textAlign: TextAlign.center,
                              style: textTheme.bodyMedium?.copyWith(
                                color: Colors.black54,
                              ),
                            ),
                            const SizedBox(height: 32),

                            // Email input card
                            Container(
                              width: double.infinity,
                              margin: const EdgeInsets.all(24),
                              decoration: BoxDecoration(
                                color: Colors.white,
                                boxShadow: [
                                  BoxShadow(
                                    blurRadius: 4,
                                    color: Colors.black.withOpacity(0.1),
                                    offset: const Offset(0, 2),
                                  ),
                                ],
                                borderRadius: BorderRadius.circular(12),
                              ),
                              child: Padding(
                                padding: const EdgeInsets.all(16),
                                child: Column(
                                  children: [
                                    Text(
                                      'Enter Email Id',
                                      style: textTheme.titleMedium?.copyWith(
                                        fontWeight: FontWeight.w600,
                                      ),
                                    ),
                                    const SizedBox(height: 16),

                                    // Email field
                                    TextFormField(
                                      controller: _emailController,
                                      focusNode: _emailFocusNode,
                                      autofocus: false,
                                      autofillHints: const [
                                        AutofillHints.email,
                                      ],
                                      textInputAction: TextInputAction.done,
                                      keyboardType: TextInputType.emailAddress,
                                      decoration: InputDecoration(
                                        hintText: 'Your email address',
                                        hintStyle: textTheme.bodyMedium
                                            ?.copyWith(color: Colors.grey[600]),
                                        enabledBorder: OutlineInputBorder(
                                          borderSide: BorderSide(
                                            color: Colors.grey[300]!,
                                            width: 1,
                                          ),
                                          borderRadius: BorderRadius.circular(
                                            8,
                                          ),
                                        ),
                                        focusedBorder: OutlineInputBorder(
                                          borderSide: BorderSide(
                                            color: colorScheme.primary,
                                            width: 1,
                                          ),
                                          borderRadius: BorderRadius.circular(
                                            8,
                                          ),
                                        ),
                                        errorBorder: OutlineInputBorder(
                                          borderSide: BorderSide(
                                            color: Colors.red,
                                            width: 1,
                                          ),
                                          borderRadius: BorderRadius.circular(
                                            8,
                                          ),
                                        ),
                                        focusedErrorBorder: OutlineInputBorder(
                                          borderSide: BorderSide(
                                            color: Colors.red,
                                            width: 1,
                                          ),
                                          borderRadius: BorderRadius.circular(
                                            8,
                                          ),
                                        ),
                                        filled: true,
                                        fillColor: Colors.white,
                                        prefixIcon: Icon(
                                          Icons.email_outlined,
                                          color: Colors.grey[600],
                                        ),
                                        contentPadding:
                                            const EdgeInsets.symmetric(
                                              horizontal: 16,
                                              vertical: 14,
                                            ),
                                      ),
                                      style: textTheme.bodyMedium,
                                    ),
                                    const SizedBox(height: 16),

                                    // Send OTP button
                                    SizedBox(
                                      width: double.infinity,
                                      height: 50,
                                      child: ElevatedButton(
                                        onPressed:
                                            _isLoading
                                                ? null
                                                : () {
                                                  // Dismiss the keyboard
                                                  FocusScope.of(
                                                    context,
                                                  ).unfocus();

                                                  // Call the OTP sending method
                                                  _handleSendOTP();
                                                },
                                        style: ElevatedButton.styleFrom(
                                          backgroundColor: const Color.fromARGB(
                                            255,
                                            82,
                                            122,
                                            254,
                                          ),
                                          foregroundColor: Colors.white,
                                          elevation: 0,
                                          shape: RoundedRectangleBorder(
                                            borderRadius: BorderRadius.circular(
                                              8,
                                            ),
                                          ),
                                          padding: const EdgeInsets.all(8),
                                          disabledBackgroundColor:
                                              Colors.grey[300],
                                        ),
                                        child:
                                            _isLoading
                                                ? SizedBox(
                                                  width: 24,
                                                  height: 24,
                                                  child:
                                                      CircularProgressIndicator(
                                                        strokeWidth: 2,
                                                        color: Colors.white,
                                                      ),
                                                )
                                                : Text(
                                                  'Send OTP',
                                                  style: textTheme.titleSmall
                                                      ?.copyWith(
                                                        color: Colors.white,
                                                      ),
                                                ),
                                      ),
                                    ),
                                  ],
                                ),
                              ),
                            ),

                            // Or continue with divider
                            Padding(
                              padding: const EdgeInsets.only(top: 32),
                              child: Row(
                                mainAxisAlignment: MainAxisAlignment.center,
                                children: [
                                  Expanded(
                                    child: Container(
                                      height: 1,
                                      color: Colors.grey[300],
                                    ),
                                  ),
                                  Padding(
                                    padding: const EdgeInsets.symmetric(
                                      horizontal: 8,
                                    ),
                                    child: Text(
                                      'Or continue with',
                                      style: textTheme.bodyMedium?.copyWith(
                                        color: Colors.grey[600],
                                      ),
                                    ),
                                  ),
                                  Expanded(
                                    child: Container(
                                      height: 1,
                                      color: Colors.grey[300],
                                    ),
                                  ),
                                ],
                              ),
                            ),

                            // Social login buttons
                            Padding(
                              padding: const EdgeInsets.only(
                                top: 24,
                                bottom: 10,
                              ),
                              child: Row(
                                mainAxisAlignment: MainAxisAlignment.center,
                                children: [
                                  // Google button with image asset
                                  _socialLoginButton(
                                    imageAsset:
                                        'assets/google_logo.png', // Using image asset instead of icon
                                    color: const Color(0xFF4285F4),
                                    onPressed: _handleGoogleLogin,
                                  ),
                                  const SizedBox(width: 24),

                                  // Facebook button
                                  /// _socialLoginButton(
                                  //icon: Icons.facebook,
                                  //color: const Color(0xFF1877F2),
                                  //onPressed: _handleFacebookLogin,
                                  //),
                                ],
                              ),
                            ),

                            // Sign up link
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
    );
  }

  Widget _socialLoginButton({
    String? imageAsset,
    IconData? icon,
    required Color color,
    required VoidCallback onPressed,
  }) {
    return Container(
      width: 60,
      height: 60,
      decoration: BoxDecoration(
        color: Colors.white,
        boxShadow: [
          BoxShadow(
            blurRadius: 4,
            color: Colors.black.withOpacity(0.1),
            offset: const Offset(0, 2),
          ),
        ],
        borderRadius: BorderRadius.circular(12),
      ),
      child: Material(
        color: Colors.transparent,
        borderRadius: BorderRadius.circular(12),
        child: InkWell(
          borderRadius: BorderRadius.circular(12),
          onTap: onPressed,
          child: Padding(
            padding: const EdgeInsets.all(12),
            child:
                imageAsset != null
                    ? Image.asset(
                      imageAsset,
                      width: 24,
                      height: 24,
                      fit: BoxFit.contain,
                      errorBuilder:
                          (context, error, stackTrace) => Icon(
                            Icons.image_not_supported,
                            color: color,
                            size: 24,
                          ),
                    )
                    : Icon(icon, color: color, size: 32),
          ),
        ),
      ),
    );
  }
}

Future<bool> confirmOtp(String token, String email) async {
  final supabase = Supabase.instance.client;
  print('📱 confirmOtp called with token: $token, email: $email');
  try {
    final response = await supabase.auth.verifyOTP(
      type: OtpType.magiclink,
      token: token,
      email: email,
    );

    print('✅ OTP verified. Session: ${response.session}');
    return response.session != null;
  } catch (error) {
    print('❌ Error during OTP verification: $error');
    return false;
  }
}

final resendApiKey = dotenv.env['RESEND_API_KEY'];
final fromEmail = dotenv.env['FROM_EMAIL'];
String _hashOtp(String otp, String email) {
  final bytes = utf8.encode(otp + email); // Salt with email
  final digest = sha256.convert(bytes);
  return digest.toString();
}

Future<String> getOtp(String email, BuildContext context) async {
  final supabase = Supabase.instance.client;

  print('📥 getOtp called for: $email');

  // Step 1: Generate OTP
  final otp =
      (100000 + (DateTime.now().millisecondsSinceEpoch % 900000)).toString();
  final otpHash = _hashOtp(otp, email);

  // Step 2: Upsert OTP using email as ID (or unique key)
  await supabase.from('email_otps').upsert({
    'email': email,
    'otp_hash': otpHash,
    'expires_at':
        DateTime.now().add(const Duration(minutes: 10)).toIso8601String(),
    'created_at': DateTime.now().toIso8601String(),
  }, onConflict: 'email'); // assumes 'email' is unique or primary key

  print('📨 OTP upserted for email: $email, now sending email...');

  // Step 3: Use SupabaseFunctions helper to send email
  final otpForWeb = await SupabaseFunctions.sendResendEmail(email, otp);
  print('✅ OTP email sent successfully to $email');

  return otpForWeb; // Return OTP if on web, empty string if email sent
}

Widget buildOtpMessage(String otp) {
  return Center(
    child: Container(
      margin: const EdgeInsets.all(16),
      padding: const EdgeInsets.all(20),
      constraints: const BoxConstraints(maxWidth: 600),
      decoration: BoxDecoration(
        border: Border.all(color: Colors.grey.shade300),
        borderRadius: BorderRadius.circular(10),
        color: Colors.white,
      ),
      child: Column(
        mainAxisSize: MainAxisSize.min,
        children: [
          // Header with gradient background and OTP
          Container(
            width: double.infinity,
            padding: const EdgeInsets.all(20),
            decoration: const BoxDecoration(
              gradient: LinearGradient(
                colors: [Color(0xFF5B6BF8), Color(0xFF8B65D9)],
              ),
              borderRadius: BorderRadius.only(
                topLeft: Radius.circular(10),
                topRight: Radius.circular(10),
              ),
            ),
            child: Text(
              otp,
              textAlign: TextAlign.center,
              style: const TextStyle(
                color: Colors.white,
                fontSize: 32,
                fontWeight: FontWeight.bold,
                letterSpacing: 2,
              ),
            ),
          ),

          const SizedBox(height: 20),

          // Main content
          Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: const [
              Text('Hello,', style: TextStyle(fontSize: 16)),
              SizedBox(height: 12),
              Text(
                'Please enter the above OTP in your mobile application to log in. '
                'The code is valid for 5 minutes.',
                style: TextStyle(fontSize: 14),
              ),
              SizedBox(height: 30),
              Text('Thank you for using our platform!'),
              Text('The ShiftHour Team'),
            ],
          ),

          const SizedBox(height: 24),

          // Footer
          Container(
            width: double.infinity,
            padding: const EdgeInsets.all(15),
            decoration: BoxDecoration(
              color: Colors.grey.shade100,
              borderRadius: const BorderRadius.only(
                bottomLeft: Radius.circular(10),
                bottomRight: Radius.circular(10),
              ),
            ),
            child: const Column(
              children: [
                Text(
                  'This is an automated message. Please do not reply to this email.',
                  textAlign: TextAlign.center,
                  style: TextStyle(fontSize: 12, color: Colors.grey),
                ),
                SizedBox(height: 4),
                Text(
                  '© 2025 ShiftHour. All rights reserved.',
                  textAlign: TextAlign.center,
                  style: TextStyle(fontSize: 12, color: Colors.grey),
                ),
              ],
            ),
          ),
        ],
      ),
    ),
  );
}

Future<bool> checkAuth() async {
  final supabase = Supabase.instance.client;
  final session = Supabase.instance.client.auth.currentSession;
  print('📱 New session after login: $session');

  print('📱 checkAuth: ${session != null ? 'Session exists' : 'No session'}');
  if (session != null) {
    print('📱 Session user: ${session.user.email}');
  }
  return session != null;
}
