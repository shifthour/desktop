// Improved OTP Verification Class
// File: lib/auth/otp_verification_screen.dart

import 'dart:async';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:get/get.dart';
import 'package:shifthour_employeer/Employer/employer_dashboard.dart';
import 'package:shifthour_employeer/const/otp_helper.dart';
import 'package:shifthour_employeer/profile.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:crypto/crypto.dart'; // Add this dependency
import 'dart:convert';

class OtpVerificationScreen extends StatefulWidget {
  final String email;

  const OtpVerificationScreen({Key? key, required this.email})
    : super(key: key);

  @override
  State<OtpVerificationScreen> createState() => _OtpVerificationScreenState();
}

class _OtpVerificationScreenState extends State<OtpVerificationScreen> {
  final List<TextEditingController> _controllers = List.generate(
    6,
    (_) => TextEditingController(),
  );
  final List<FocusNode> _focusNodes = List.generate(6, (_) => FocusNode());

  bool _isVerifying = false;
  bool _isResending = false;
  String? _errorMessage;
  // Track verification attempts for rate limiting

  Timer? _timer;
  int _timerSeconds = 60;
  bool _canResend = false;

  @override
  void initState() {
    super.initState();
    _startTimer();
  }

  void _startTimer() {
    setState(() {
      _timerSeconds = 60;
      _canResend = false;
    });
    _timer?.cancel();
    _timer = Timer.periodic(const Duration(seconds: 1), (timer) {
      if (mounted) {
        setState(() {
          if (_timerSeconds > 0) {
            _timerSeconds--;
          } else {
            _canResend = true;
            timer.cancel();
          }
        });
      }
    });
  }

  @override
  void dispose() {
    for (var controller in _controllers) {
      controller.dispose();
    }
    for (var focusNode in _focusNodes) {
      focusNode.dispose();
    }
    _timer?.cancel();
    super.dispose();
  }

  String get _token => _controllers.map((c) => c.text).join();
  Future<void> _verifyOTP() async {
    if (_token.length != 6) {
      setState(() => _errorMessage = 'Please enter all 6 digits');
      return;
    }

    setState(() {
      _isVerifying = true;
      _errorMessage = null;
    });

    try {
      final isValid = await _verifyOTPFromDatabase(widget.email, _token);

      if (isValid) {
        print('✅ OTP is valid. Proceeding to auth session creation...');
        await _createAuthSession(); // ← Add this line
      } else {
        setState(() {
          _errorMessage = 'Invalid or expired code. Try again.';
          _isVerifying = false;
        });
      }
    } catch (e) {
      print('❌ Error during OTP check: $e');
      setState(() {
        _errorMessage = 'Error verifying code. Try again.';
        _isVerifying = false;
      });
    }
  }

  Future<bool> _verifyOTPFromDatabase(String email, String token) async {
    final supabase = Supabase.instance.client;

    final response =
        await supabase
            .from('email_otps')
            .select()
            .eq('email', email)
            .order('created_at', ascending: false)
            .limit(1)
            .maybeSingle();

    if (response == null || response['otp_hash'] == null) {
      print('❌ No OTP found for $email');
      return false;
    }

    if (DateTime.parse(response['expires_at']).isBefore(DateTime.now())) {
      print('⏰ OTP expired for $email');
      return false;
    }

    final hashedInput = _hashOtp(token, email);
    if (hashedInput != response['otp_hash']) {
      print('❌ OTP mismatch');
      return false;
    }

    print('✅ OTP verified for $email');
    return true;
  }

  Future<void> _createAuthSession() async {
    final supabase = Supabase.instance.client;
    const dummyPassword = 'use-only-with-otp-flow';

    try {
      await supabase.auth.signUp(email: widget.email, password: dummyPassword);
      print('✅ User signed up successfully');
    } on AuthException catch (e) {
      print('⚠️ AuthException during signUp: ${e.message}');
      if (e.message.contains('User already registered')) {
        print('🔄 User already exists, attempting to sign in...');
        final login = await supabase.auth.signInWithPassword(
          email: widget.email,
          password: dummyPassword,
        );

        if (login.user != null) {
          print('✅ Sign in successful for: ${login.user!.email}');
        } else {
          setState(() {
            _errorMessage = 'Login failed. Please try again.';
            _isVerifying = false;
          });
          return;
        }
      } else {
        setState(() {
          _errorMessage = 'Signup error: ${e.message}';
          _isVerifying = false;
        });
        return;
      }
    }

    // Success path
    print('🚀 Navigating based on profile...');
    await _navigateBasedOnProfile();
  }

  // Securely hash OTP to compare with stored hash
  String _hashOtp(String otp, String email) {
    final bytes = utf8.encode(otp + widget.email); // Salt with email
    final digest = sha256.convert(bytes);
    return digest.toString();
  }

  // Generate a secure random password for auth
  String _generateSecurePassword() {
    final random = DateTime.now().millisecondsSinceEpoch.toString();
    final bytes = utf8.encode(random + widget.email);
    final digest = sha256.convert(bytes);
    return digest.toString().substring(0, 16); // Use first 16 chars
  }

  Future<void> _navigateBasedOnProfile() async {
    final supabase = Supabase.instance.client;
    try {
      final employerData =
          await supabase
              .from('employers')
              .select('id, contact_email')
              .eq('contact_email', widget.email)
              .maybeSingle();

      if (!mounted) return;

      Navigator.pop(context, true);

      if (employerData != null) {
        Get.offAll(() => const EmployerDashboard());
      } else {
        Get.offAll(
          () => EmployerProfileSetupScreen(initialEmail: widget.email),
        );
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
        setState(() => _isVerifying = false);
      }
    }
  }

  // Improved resend OTP with rate limiting
  Future<void> _resendOTP() async {
    if (!_canResend) return;

    setState(() {
      _isResending = true;
      _errorMessage = null;
    });

    try {
      await _generateAndSendOtp(widget.email);

      if (mounted) {
        // Reset inputs
        for (var c in _controllers) {
          c.clear();
        }
        _focusNodes[0].requestFocus();
        _startTimer();
        // Reset attempt counter

        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(
            content: Text('Verification code sent successfully.'),
            backgroundColor: Color(0xFF5B52FE),
          ),
        );
      }
    } catch (e) {
      if (mounted) {
        setState(() {
          _errorMessage = 'Failed to send verification code. Please try again.';
        });
      }
    } finally {
      if (mounted) {
        setState(() => _isResending = false);
      }
    }
  }

  Future<void> _generateAndSendOtp(String email) async {
    final supabase = Supabase.instance.client;

    // Step 1: Generate OTP
    final otp =
        (100000 + (DateTime.now().millisecondsSinceEpoch % 900000)).toString();
    final otpHash = _hashOtp(otp, email);

    // Step 2: Check if record exists
    try {
      final existing =
          await supabase
              .from('email_otps')
              .select('email')
              .eq('email', email)
              .maybeSingle();

      if (existing != null) {
        // Update
        await supabase
            .from('email_otps')
            .update({
              'otp_hash': otpHash,
              'expires_at':
                  DateTime.now()
                      .add(const Duration(minutes: 5))
                      .toIso8601String(),
              'created_at': DateTime.now().toIso8601String(),
            })
            .eq('email', email);
      } else {
        // Insert
        await supabase.from('email_otps').insert({
          'email': email,
          'otp_hash': otpHash,
          'expires_at':
              DateTime.now().add(const Duration(minutes: 5)).toIso8601String(),
          'created_at': DateTime.now().toIso8601String(),
        });
      }

      print('📨 OTP upserted for $email');

      // Step 3: Send Email
      await SupabaseFunctions.sendResendEmail(email, otp);
      print('✅ Resent OTP email sent to $email');
    } catch (e) {
      print('❌ Resend failed: $e');
      throw Exception('Failed to resend OTP');
    }
  }
  // Helper to send OTP email

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Verification'),
        backgroundColor: const Color(0xFF5B52FE),
      ),
      body: Center(
        child: SingleChildScrollView(
          child: Container(
            constraints: const BoxConstraints(maxWidth: 400),
            padding: const EdgeInsets.all(24),
            decoration: BoxDecoration(
              color: Colors.white,
              borderRadius: BorderRadius.circular(16),
              boxShadow: [
                BoxShadow(
                  color: Colors.black.withOpacity(0.1),
                  blurRadius: 10,
                  offset: const Offset(0, 4),
                ),
              ],
            ),
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                const Text(
                  'Enter Verification Code',
                  style: TextStyle(fontSize: 24, fontWeight: FontWeight.bold),
                ),
                const SizedBox(height: 12),
                Text(
                  'A 6-digit code has been sent to ${widget.email}',
                  textAlign: TextAlign.center,
                  style: TextStyle(color: Colors.grey[600]),
                ),
                const SizedBox(height: 40),
                Row(
                  mainAxisAlignment: MainAxisAlignment.spaceEvenly,
                  children: List.generate(6, _buildOTPField),
                ),
                if (_errorMessage != null)
                  Padding(
                    padding: const EdgeInsets.only(top: 16),
                    child: Text(
                      _errorMessage!,
                      style: const TextStyle(color: Colors.red),
                      textAlign: TextAlign.center,
                    ),
                  ),
                const SizedBox(height: 24),
                _isResending
                    ? const CircularProgressIndicator()
                    : _canResend
                    ? TextButton(
                      onPressed: _resendOTP,
                      child: const Text("Resend verification code"),
                    )
                    : Text("Resend in $_timerSeconds seconds"),
                const SizedBox(height: 24),
                Row(
                  mainAxisAlignment: MainAxisAlignment.spaceBetween,
                  children: [
                    OutlinedButton(
                      onPressed: () => Navigator.pop(context, false),
                      style: OutlinedButton.styleFrom(
                        padding: const EdgeInsets.symmetric(
                          horizontal: 24,
                          vertical: 12,
                        ),
                      ),
                      child: const Text('Cancel'),
                    ),
                    ElevatedButton(
                      onPressed: _isVerifying ? null : _verifyOTP,
                      style: ElevatedButton.styleFrom(
                        backgroundColor: const Color(0xFF5B52FE),
                        foregroundColor: Colors.white,
                        padding: const EdgeInsets.symmetric(
                          horizontal: 24,
                          vertical: 12,
                        ),
                      ),
                      child:
                          _isVerifying
                              ? const SizedBox(
                                width: 20,
                                height: 20,
                                child: CircularProgressIndicator(
                                  strokeWidth: 2,
                                  color: Colors.white,
                                ),
                              )
                              : const Text('Verify Code'),
                    ),
                  ],
                ),
              ],
            ),
          ),
        ),
      ),
    );
  }

  Widget _buildOTPField(int index) {
    return SizedBox(
      width: 48,
      height: 60,
      child: TextField(
        controller: _controllers[index],
        focusNode: _focusNodes[index],
        maxLength: 1,
        keyboardType: TextInputType.number,
        textAlign: TextAlign.center,
        inputFormatters: [FilteringTextInputFormatter.digitsOnly],
        style: const TextStyle(fontSize: 20, fontWeight: FontWeight.bold),
        decoration: InputDecoration(
          counterText: '',
          border: OutlineInputBorder(
            borderRadius: BorderRadius.circular(12),
            borderSide: BorderSide(color: Colors.grey.shade300),
          ),
          focusedBorder: OutlineInputBorder(
            borderRadius: BorderRadius.circular(12),
            borderSide: const BorderSide(color: Color(0xFF5B52FE), width: 2),
          ),
          filled: true,
          fillColor: Colors.white,
        ),
        onChanged: (value) {
          if (value.isNotEmpty && index < 5) {
            _focusNodes[index + 1].requestFocus();
          } else if (value.isEmpty && index > 0) {
            _focusNodes[index - 1].requestFocus();
          }

          // Auto-verify if all fields are filled
          if (index == 5 && value.isNotEmpty) {
            if (_token.length == 6) {
              // Don't use the return value of _verifyOTP here
              _verifyOTP();
            }
          }
        },
      ),
    );
  }
}
