import 'dart:async';
import 'dart:convert';
import 'dart:math';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:flutter_dotenv/flutter_dotenv.dart';
import 'package:get/get.dart';
import 'package:get/get_core/src/get_main.dart';
import 'package:google_sign_in/google_sign_in.dart';
import 'package:motion_toast/motion_toast.dart';
import 'package:onesignal_flutter/onesignal_flutter.dart';
import 'package:shifthour/main.dart';
import 'package:shifthour/profile.dart' as profile;
import 'package:shifthour/signup.dart';
import 'package:shifthour/worker/const/otp.dart';
import 'package:shifthour/worker/worker_dashboard.dart';
import 'package:http/http.dart' as http;
import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:crypto/crypto.dart';

class WorkerLoginPage extends StatefulWidget {
  const WorkerLoginPage({Key? key}) : super(key: key);

  @override
  State<WorkerLoginPage> createState() => _WorkerLoginPageState();
}

class _WorkerLoginPageState extends State<WorkerLoginPage> {
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
    try {
      if (!mounted) return;

      // Navigate to profile or dashboard
      await _navigateBasedOnProfile(email);

      // 🔥 After navigation, wait a bit and register the device
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
      print('Authentication verification error: $e');
      if (mounted) {
        _showErrorMessage('Unable to complete sign-in. Please try again.');
      }
    } finally {
      if (mounted) {
        setState(() {
          _isLoading = false;
          _isGoogleSignInInProgress = false;
        });
      }
    }
  }

  Future<void> _navigateBasedOnProfile(String email) async {
    // Check if widget is still mounted before async operation
    if (!mounted) return;

    try {
      // Perform database query to check employer profile
      final employerData =
          await Supabase.instance.client
              .from('job_seekers')
              .select('id, email')
              .eq('email', email)
              .maybeSingle();

      // Double-check mounted state after async operation
      if (!mounted) return;

      // Determine navigation based on profile existence
      if (employerData != null && employerData['email'] == email) {
        // Existing employer - navigate to dashboard
        Get.offAll(() => const WorkerDashboard());
      } else {
        // No profile found - navigate to profile setup
        Navigator.of(context).push(
          MaterialPageRoute(
            builder:
                (context) => profile.ProfileSetupScreen(
                  isEmployer: false,
                  initialEmail: email,
                ),
          ),
        );
      }
    } catch (e) {
      // Log the error for debugging
      print('Error checking employer profile: $e');

      // Show user-friendly error message
      _showErrorMessage('Unable to verify your profile. Please try again.');
    }
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

  final GoogleSignIn _googleSignIn = GoogleSignIn(
    scopes: [
      'email',
      'profile',
      'https://www.googleapis.com/auth/userinfo.profile',
      'openid',
    ],
    serverClientId:
        '825127993257-6jjq58pk3g3oq3rdshetu72qfirvd6lm.apps.googleusercontent.com',
  );
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

  // Replace your existing _handleGoogleLogin method with this one
  Future<void> _handleGoogleLogin() async {
    // Prevent multiple simultaneous login attempts
    if (_isLoading || _isGoogleSignInInProgress) {
      _logAuthStep('Login prevented - already in progress');
      return;
    }

    setState(() {
      _isLoading = true;
      _isGoogleSignInInProgress = true;
    });
    _logAuthStep('Started Google Sign-In process');

    try {
      // Force sign out first to ensure a clean state
      _logAuthStep('Signing out from previous Google sessions');
      bool wasSignedIn = await _googleSignIn.isSignedIn();
      if (wasSignedIn) {
        await _googleSignIn.signOut();
        _logAuthStep('Successfully signed out from previous session');
      }

      // Begin sign-in process with silent sign-in attempt first
      _logAuthStep('Attempting silent sign-in');
      GoogleSignInAccount? googleUser;

      try {
        googleUser = await _googleSignIn.signInSilently();
        if (googleUser != null) {
          _logAuthStep('Silent sign-in successful', googleUser.email);
        } else {
          _logAuthStep(
            'Silent sign-in returned null, trying interactive sign-in',
          );
        }
      } catch (e) {
        _logAuthError('Silent sign-in failed', e);
      }

      // If silent sign-in failed, try interactive sign-in
      if (googleUser == null) {
        _logAuthStep('Initiating interactive Google Sign-In UI');
        try {
          googleUser = await _googleSignIn.signIn();
        } catch (e) {
          _logAuthError('Interactive sign-in error', e);
          throw e;
        }
      }

      if (googleUser == null) {
        _logAuthStep('Sign-in canceled by user');
        _handleLoginCancellation();
        return;
      }

      _logAuthStep('Google account selected', googleUser.email);

      // Get authentication details
      _logAuthStep('Requesting authentication tokens');
      final GoogleSignInAuthentication googleAuth;

      try {
        googleAuth = await googleUser.authentication;
        _logAuthStep('Authentication tokens received');
      } catch (e) {
        _logAuthError('Failed to get authentication tokens', e);
        throw e;
      }

      // Validate tokens
      if (googleAuth.idToken == null) {
        _logAuthError('Token Validation', 'ID Token is null');
        _handleLoginError('Failed to get valid authentication token');
        return;
      }

      _logAuthStep('Tokens validated successfully');

      // Show user that we're processing
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(
            content: Text('Signing in...'),
            duration: Duration(seconds: 2),
          ),
        );
      }

      // Authenticate with Supabase
      _logAuthStep('Authenticating with Supabase');
      try {
        final response = await Supabase.instance.client.auth.signInWithIdToken(
          provider: OAuthProvider.google,
          idToken: googleAuth.idToken!,
          accessToken: googleAuth.accessToken,
        );

        final email = response.user?.email;

        if (email == null) {
          _logAuthError('Supabase Authentication', 'No email returned');
          _showErrorMessage('Could not retrieve email from your account');
          return;
        }

        _logAuthStep('Supabase authentication successful', email);

        // Check if employer profile exists
        _logAuthStep('Verifying employer profile for', email);
        await _verifyEmployerProfile(email);
      } on AuthException catch (authError) {
        _logAuthError('Supabase Authentication', authError);
        _handleLoginError('Authentication failed: ${authError.message}');
      }
    } catch (e) {
      _logAuthError('Google Sign-In Process', e);

      // Provide more specific error messages based on common error patterns
      String errorMessage = 'Google sign-in failed. Please try again.';

      if (e.toString().contains('network')) {
        errorMessage = 'Network error. Please check your internet connection.';
      } else if (e.toString().contains('canceled')) {
        errorMessage = 'Sign-in was canceled.';
      } else if (e.toString().contains('credential')) {
        errorMessage =
            'Authentication problem. Please try again or use another sign-in method.';
      }

      _handleLoginError(errorMessage);
    } finally {
      if (mounted) {
        setState(() {
          _isLoading = false;
          _isGoogleSignInInProgress = false;
        });
        _logAuthStep('Reset loading state');
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
              .from('job_seekers')
              .select('id, email')
              .eq('email', email)
              .maybeSingle();

      if (mounted) {
        if (employerData == null) {
          // No profile found - navigate to profile setup
          Navigator.of(context).push(
            MaterialPageRoute(
              builder:
                  (context) => profile.ProfileSetupScreen(
                    isEmployer: false,
                    initialEmail: email,
                  ),
            ),
          );
        } else {
          // Existing employer - navigate to dashboard
          Get.offAll(() => const WorkerDashboard());
        }
      }
    } catch (e) {
      if (mounted) {
        _showErrorMessage('Error verifying profile: ${e.toString()}');
        // Optional: Sign out if verification fails
        await Supabase.instance.client.auth.signOut();
        Get.offAll(() => const WorkerLoginPage());
      }
    }
  }

  //Future<void> _handleFacebookLogin() async {
  //  _showErrorMessage('Facebook login not implemented yet');
  //}

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
      await getOtp(email);
      print('✅ OTP sent successfully');

      if (mounted) {
        setState(() => _isLoading = false);

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
        body: SafeArea(
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
                                    autofillHints: const [AutofillHints.email],
                                    textInputAction: TextInputAction.done,
                                    keyboardType: TextInputType.emailAddress,
                                    decoration: InputDecoration(
                                      hintText: 'Your email address',
                                      hintStyle: textTheme.bodyMedium?.copyWith(
                                        color: Colors.grey[600],
                                      ),
                                      enabledBorder: OutlineInputBorder(
                                        borderSide: BorderSide(
                                          color: Colors.grey[300]!,
                                          width: 1,
                                        ),
                                        borderRadius: BorderRadius.circular(8),
                                      ),
                                      focusedBorder: OutlineInputBorder(
                                        borderSide: BorderSide(
                                          color: colorScheme.primary,
                                          width: 1,
                                        ),
                                        borderRadius: BorderRadius.circular(8),
                                      ),
                                      errorBorder: OutlineInputBorder(
                                        borderSide: BorderSide(
                                          color: Colors.red,
                                          width: 1,
                                        ),
                                        borderRadius: BorderRadius.circular(8),
                                      ),
                                      focusedErrorBorder: OutlineInputBorder(
                                        borderSide: BorderSide(
                                          color: Colors.red,
                                          width: 1,
                                        ),
                                        borderRadius: BorderRadius.circular(8),
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
                            padding: const EdgeInsets.only(top: 24, bottom: 10),
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
                                //  _socialLoginButton(
                                //  icon: Icons.facebook,
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

Future<void> getOtp(String email) async {
  final supabase = Supabase.instance.client;

  print('📥 getOtp called for: $email');

  // Step 1: Generate OTP using cryptographically secure random
  final random = Random.secure();
  final otp = (100000 + random.nextInt(900000)).toString();
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

  // Step 3: Send Email
  final htmlContent = '''
  <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #e0e0e0; border-radius: 10px;">
    <div style="background: linear-gradient(to right, #5B6BF8, #8B65D9); padding: 20px; border-radius: 10px 10px 0 0; text-align: center;">
      <h1 style="color: white; margin: 0;">$otp</h1>
    </div>
    <div style="padding: 20px;">
      <p>Hello,</p>
      <p>Please enter above OTP in your mobile application to log in. The code is valid for 5 minutes.</p>
      <p style="margin-top: 30px;">Thank you for using our platform!</p>
      <p>The ShiftHour Team</p>
    </div>
    <div style="background-color: #f5f5f5; padding: 15px; border-radius: 0 0 10px 10px; text-align: center; font-size: 12px; color: #666;">
      <p>This is an automated message. Please do not reply to this email.</p>
      <p>© 2025 ShiftHour. All rights reserved.</p>
    </div>
  </div>
''';

  final response = await http.post(
    Uri.parse('https://api.resend.com/emails'),
    headers: {
      'Authorization': 'Bearer $resendApiKey',
      'Content-Type': 'application/json',
    },
    body: json.encode({
      'from': fromEmail,
      'to': email,
      'subject': 'Your Login OTP',
      'html': htmlContent,
    }),
  );

  if (response.statusCode >= 400) {
    throw Exception('❌ Failed to send OTP email: ${response.body}');
  }

  print('✅ OTP email sent successfully to $email');
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
