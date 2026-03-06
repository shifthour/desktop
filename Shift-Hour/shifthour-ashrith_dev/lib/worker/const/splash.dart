import 'package:flutter/material.dart';
import 'package:get/get.dart';
import 'package:supabase_flutter/supabase_flutter.dart';

class SplashScreen extends StatefulWidget {
  const SplashScreen({Key? key}) : super(key: key);

  @override
  State<SplashScreen> createState() => _SplashScreenState();
}

class _SplashScreenState extends State<SplashScreen> {
  @override
  void initState() {
    super.initState();
    _checkAuthState();
  }

  Future<void> _checkAuthState() async {
    // Show splash screen for minimum duration
    await Future.delayed(const Duration(seconds: 2));

    if (!mounted) return;

    try {
      // Get the current session
      final session = Supabase.instance.client.auth.currentSession;
      final user = Supabase.instance.client.auth.currentUser;

      // Check if we have a valid session
      if (session != null &&
          !session.isExpired &&
          user != null &&
          user.email != null) {
        print("User is signed in: ${user.email}");

        // Check if job seeker profile exists (worker app)
        final userData =
            await Supabase.instance.client
                .from('job_seekers')
                .select('id, email')
                .eq('email', user.email!)
                .maybeSingle();

        if (!mounted) return;

        if (userData != null) {
          // Job seeker profile exists, navigate to worker dashboard
          Get.offAllNamed('/worker_dashboard');
        } else {
          // No profile exists, redirect to profile setup
          Get.offAllNamed('/profile_setup');
        }
      } else {
        // No valid session, try to restore from refresh token
        try {
          if (session?.refreshToken != null) {
            final response =
                await Supabase.instance.client.auth.refreshSession();
            if (response.session != null) {
              // Session restored successfully
              _checkAuthState(); // Recursively check again
              return;
            }
          }
        } catch (e) {
          print('Failed to restore session: $e');
        }

        // Go to login if no session can be restored
        Get.offAllNamed('/login');
      }
    } catch (e) {
      print('Error checking auth state: $e');
      if (mounted) {
        Get.offAllNamed('/login');
      }
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: const Color(0xFFF0F5FF),
      body: Center(
        child: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            // Logo
            Image.asset(
              'assets/logo.png',
              width: 200,
              height: 200,
              fit: BoxFit.contain,
              errorBuilder: (context, error, stackTrace) {
                return const Icon(
                  Icons.error_outline,
                  size: 60,
                  color: Colors.red,
                );
              },
            ),
            const SizedBox(height: 20),
            // App name with gradient text
            ShaderMask(
              shaderCallback:
                  (bounds) => LinearGradient(
                    colors: [Colors.blue.shade600, Colors.indigo.shade700],
                    begin: Alignment.topLeft,
                    end: Alignment.bottomRight,
                  ).createShader(bounds),
              child: const Text(
                'ShiftHour',
                style: TextStyle(
                  fontSize: 36,
                  fontWeight: FontWeight.bold,
                  color: Colors.white,
                ),
              ),
            ),
            const SizedBox(height: 50),
            // Loading indicator
            CircularProgressIndicator(
              valueColor: AlwaysStoppedAnimation<Color>(Colors.blue.shade700),
            ),
          ],
        ),
      ),
    );
  }
}
