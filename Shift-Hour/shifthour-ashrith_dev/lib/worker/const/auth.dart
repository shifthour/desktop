import 'dart:async';
import 'package:flutter/material.dart';
import 'package:get/get.dart';
import 'package:onesignal_flutter/onesignal_flutter.dart';
import 'package:shifthour/main.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:shared_preferences/shared_preferences.dart';

class AuthWrapper extends StatefulWidget {
  final Widget child;

  const AuthWrapper({Key? key, required this.child}) : super(key: key);

  @override
  State<AuthWrapper> createState() => _AuthWrapperState();
}

class _AuthWrapperState extends State<AuthWrapper> with WidgetsBindingObserver {
  late StreamSubscription<AuthState> _authSubscription;
  bool _isAuthenticating = true;

  @override
  void initState() {
    super.initState();
    WidgetsBinding.instance.addObserver(this);
    _checkAndRestoreAuth();
    _setupAuthListener();
  }

  Future<void> _checkAndRestoreAuth() async {
    try {
      final session = Supabase.instance.client.auth.currentSession;

      if (session == null || session.isExpired) {
        // No valid session, go to login
        setState(() => _isAuthenticating = false);
        if (mounted) {
          Get.offAllNamed('/login');
        }
        return;
      }

      if (session.refreshToken != null) {
        // Try to refresh the session
        try {
          final response = await Supabase.instance.client.auth.refreshSession();

          if (response.session != null) {
            setState(() => _isAuthenticating = false);
            // Check user profile to determine where to navigate
            _checkUserProfile();
          } else {
            // No valid session after refresh attempt
            setState(() => _isAuthenticating = false);
            if (mounted) {
              Get.offAllNamed('/login');
            }
          }
        } catch (e) {
          print('Failed to refresh session: $e');
          setState(() => _isAuthenticating = false);
          if (mounted) {
            Get.offAllNamed('/login');
          }
        }
      } else {
        // Session exists but no refresh token
        setState(() => _isAuthenticating = false);
        if (mounted) {
          Get.offAllNamed('/login');
        }
      }
    } catch (e) {
      print('Auth restoration error: $e');
      setState(() => _isAuthenticating = false);
      if (mounted) {
        Get.offAllNamed('/login');
      }
    }
  }

  void _setupAuthListener() {
    _authSubscription = Supabase.instance.client.auth.onAuthStateChange.listen((
      data,
    ) {
      final AuthChangeEvent event = data.event;
      final Session? session = data.session;

      if (event == AuthChangeEvent.signedOut) {
        _clearLocalSession();
        Get.offAllNamed('/login');
      } else if (event == AuthChangeEvent.tokenRefreshed) {
        print('Session refreshed');
      } else if (event == AuthChangeEvent.signedIn && session != null) {
        _checkUserProfile();
      }
    });
  }

  Future<void> _checkUserProfile() async {
    final user = Supabase.instance.client.auth.currentUser;
    if (user == null || user.email == null) {
      Get.offAllNamed('/login');
      return;
    }

    try {
      // First check if user is a job seeker
      final jobSeekerData =
          await Supabase.instance.client
              .from('job_seekers')
              .select('id, email')
              .eq('email', user.email!)
              .maybeSingle();

      // If user exists as a job seeker
      if (jobSeekerData != null) {
        await registerDeviceForUser(user.id);

        // Navigate to worker dashboard if on a public route
        final currentRoute = Get.currentRoute;
        if (currentRoute == '/login' ||
            currentRoute == '/splash' ||
            currentRoute == '/') {
          Get.offAllNamed('/worker_dashboard');
        }
        return;
      }

      // If not a job seeker, check if user is an employer
      final employerData =
          await Supabase.instance.client
              .from('employers') // Adjust table name if different
              .select('id, email')
              .eq('email', user.email!)
              .maybeSingle();

      if (employerData != null) {
        await registerDeviceForUser(user.id);

        // Navigate to employer dashboard if on a public route
        final currentRoute = Get.currentRoute;
        if (currentRoute == '/login' ||
            currentRoute == '/splash' ||
            currentRoute == '/') {
          Get.offAllNamed('/employer_dashboard');
        }
        return;
      }

      // If user is neither worker nor employer, they need to create a profile
      // But we should only redirect to profile setup if they're coming from login
      final currentRoute = Get.currentRoute;
      if (currentRoute == '/login') {
        // This ensures we only redirect to profile setup from login
        // and not on fresh app install
        Get.offAllNamed('/profile_setup');
      } else {
        // For fresh installs, go to login page
        Get.offAllNamed('/login');
      }
    } catch (e) {
      print('Error checking profile: $e');
      // If there's an error, default to login
      Get.offAllNamed('/login');
    }
  }

  Future<void> _clearLocalSession() async {
    try {
      final prefs = await SharedPreferences.getInstance();
      await prefs.remove('supabase_session');
    } catch (e) {
      print('Error clearing session: $e');
    }
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    if (state == AppLifecycleState.resumed) {
      _checkAndRestoreAuth();
    }
  }

  @override
  void dispose() {
    _authSubscription.cancel();
    WidgetsBinding.instance.removeObserver(this);
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    if (_isAuthenticating) {
      return const Scaffold(body: Center(child: CircularProgressIndicator()));
    }
    return widget.child;
  }

  static Future<void> logout() async {
    try {
      await Supabase.instance.client.auth.signOut();
      final prefs = await SharedPreferences.getInstance();
      await prefs.remove('supabase_session');
      Get.offAllNamed('/login');
    } catch (e) {
      print('Logout error: $e');
      Get.offAllNamed('/login');
    }
  }
}
