import 'dart:io';

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:get/get.dart';
import 'package:onesignal_flutter/onesignal_flutter.dart';
import 'package:shifthour/Employer/employer_dashboard.dart';
import 'package:shifthour/login.dart';
import 'package:shifthour/profile.dart';
import 'package:shifthour/worker/const/notifications.dart';
import 'package:shifthour/worker/const/otp.dart';
import 'package:shifthour/worker/worker_dashboard.dart';
import 'package:shifthour/worker/const/auth.dart';
import 'package:shifthour/worker/const/splash.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:flutter_dotenv/flutter_dotenv.dart';

void main() async {
  WidgetsFlutterBinding.ensureInitialized();

  // ✅ Load .env first before using any dotenv.env[] values
  await dotenv.load();

  await SystemChrome.setPreferredOrientations([
    DeviceOrientation.portraitUp,
    DeviceOrientation.portraitDown,
  ]);

  // ✅ Now safe to initialize OneSignal
  OneSignal.Debug.setLogLevel(OSLogLevel.verbose);
  OneSignal.initialize(dotenv.env['ONESIGNAL_APP_ID']!);
  OneSignal.Notifications.requestPermission(true);

  // ✅ Now initialize Supabase
  await Supabase.initialize(
    url: dotenv.env['SUPABASE_URL']!,
    anonKey: dotenv.env['SUPABASE_ANON_KEY']!,
    debug: true,
  );

  // Setup notification handlers
  setupOneSignalHandlers();

  // Check if user is logged in and register for notifications
  final currentUser = Supabase.instance.client.auth.currentUser;
  if (currentUser != null) {
    registerDeviceForUser(currentUser.id);
  }

  verifyOneSignalSetup();
  checkNetworkConnectivity();

  runApp(const MyApp());
}

Future<void> checkNetworkConnectivity() async {
  try {
    final result = await InternetAddress.lookup(
      'awpczipxzeurlpmmrzid.supabase.co',
    );
    if (result.isNotEmpty && result[0].rawAddress.isNotEmpty) {
      print('✅ Supabase Host Reachable');
    }
  } on SocketException catch (e) {
    print('❌ Network Error: ${e.message}');
    print('Possible Issues:');
    print('- No internet connection');
    print('- DNS resolution failed');
    print('- Firewall blocking');
  }
}

void validateSupabaseConnection() async {
  try {
    final supabase = Supabase.instance.client;
    final currentUser = supabase.auth.currentUser;

    if (currentUser != null) {
      // User is authenticated - removed sensitive email logging

      // Attempt a simple query to test connection
      final response = await supabase
          .from('job_seekers')
          .select()
          .eq('email', currentUser.email as Object)
          .limit(1);

      print('Query Result: ${response.length} records found');
    }
  } catch (e) {
    print('❌ Supabase Connection Error: $e');
  }
}

// Remove the hardcoded ID, we'll get it dynamically
// String onesignalId = '483003617488';
void setupOneSignalHandlers() {
  // Listen for push subscription changes
  OneSignal.User.pushSubscription.addObserver((state) async {
    print("Push subscription changed:");
    print("Opted in: ${OneSignal.User.pushSubscription.optedIn}");
    print("ID: ${OneSignal.User.pushSubscription.id}");
    print("Token: ${OneSignal.User.pushSubscription.token}");

    // Only when subscription and token are ready
    if (OneSignal.User.pushSubscription.id != null &&
        OneSignal.User.pushSubscription.token != null) {
      final currentUser = Supabase.instance.client.auth.currentUser;
      if (currentUser != null) {
        await registerDeviceForUser(currentUser.id);
      } else {
        print('⚠️ No user logged in to register device.');
      }
    } else {
      print('⚠️ Push Subscription ID/Token not ready yet.');
    }
  });
}

Future<void> registerDeviceForUser(String userId) async {
  final pushSubscription = OneSignal.User.pushSubscription;
  final subscriptionId = pushSubscription.id;
  final token = pushSubscription.token;

  print(
    "Registering device for user $userId with OneSignal ID: $subscriptionId",
  );

  if (subscriptionId == null || token == null) {
    print(
      "❌ Cannot register device - OneSignal subscription ID or token is null",
    );
    return;
  }

  try {
    // Identify the user in OneSignal (Important: use OneSignal.login(userId) before this)
    OneSignal.login(userId);

    final deviceType = GetPlatform.isIOS ? 'iOS' : 'Android';

    // Store correctly in Supabase
    await Supabase.instance.client.from('device_tokens').upsert({
      'user_id': userId, // Must be equal to supabase.auth.currentUser!.id
      'token': token,
      'platform': deviceType,
      'player_id': subscriptionId,
      'updated_at': DateTime.now().toIso8601String(),
    }, onConflict: 'user_id');
    // optional if you want to update if user already exists

    print('✅ Device registered successfully for notifications');
  } catch (e) {
    print('❌ Error registering device: $e');
  }
}

void verifyOneSignalSetup() {
  print('OneSignal Setup Verification:');

  // Check initialization - removed hardcoded App ID logging

  // Verify current user
  final currentUser = Supabase.instance.client.auth.currentUser;
  if (currentUser != null) {
    // User authenticated - removed sensitive user data logging

    // Try to log in with current user ID
    try {
      OneSignal.login(currentUser.id);
      print('✅ OneSignal User Login Successful');
    } catch (e) {
      print('❌ OneSignal User Login Failed: $e');
    }
  } else {
    print('⚠️ No Authenticated User');
  }

  // Check push subscription
  final pushSubscription = OneSignal.User.pushSubscription;
  print('Push Subscription:');
  print('Subscription ID: ${pushSubscription.id}');
  print('Subscription Token: ${pushSubscription.token}');
  print('Opted In: ${pushSubscription.optedIn}');
}

void storePlayerIdOnAuth() {
  final userId = Supabase.instance.client.auth.currentUser?.id;
  if (userId != null) {
    registerDeviceForUser(userId);
  } else {
    print("❌ Cannot store player ID - User not logged in");
  }
}

final GlobalKey<NavigatorState> _navigatorKey = GlobalKey<NavigatorState>();

class MyApp extends StatelessWidget {
  const MyApp({super.key});

  @override
  Widget build(BuildContext context) {
    return GetMaterialApp(
      title: 'ShiftHour',
      debugShowCheckedModeBanner: false,
      theme: ThemeData(
        primarySwatch: Colors.blue,
        scaffoldBackgroundColor: const Color(0xFFF0F5FF),
        fontFamily: 'Inter',
        appBarTheme: AppBarTheme(
          backgroundColor: Colors.blue.shade700,
          elevation: 0,
          iconTheme: const IconThemeData(color: Colors.white),
          titleTextStyle: const TextStyle(
            color: Colors.white,
            fontSize: 20,
            fontWeight: FontWeight.bold,
            fontFamily: 'Inter Tight',
          ),
        ),
        elevatedButtonTheme: ElevatedButtonThemeData(
          style: ElevatedButton.styleFrom(
            backgroundColor: Colors.blue.shade700,
            foregroundColor: Colors.white,
            shape: RoundedRectangleBorder(
              borderRadius: BorderRadius.circular(8),
            ),
          ),
        ),
      ),
      initialRoute: '/splash',
      getPages: [
        // 🔓 Public routes
        GetPage(name: '/splash', page: () => const SplashScreen()),
        GetPage(name: '/login', page: () => const WorkerLoginPage()),
        GetPage(
          name: '/otp',
          page: () => const OtpVerificationScreen(email: ''),
        ),

        // 🔐 Authenticated routes - Each wrapped with AuthWrapper
        GetPage(
          name: '/worker_dashboard',
          page: () => AuthWrapper(child: const WorkerDashboard()),
        ),
        GetPage(
          name: '/employer_dashboard',
          page: () => AuthWrapper(child: const EmployerDashboard()),
        ),
        GetPage(
          name: '/profile_setup',
          page:
              () => AuthWrapper(
                child: const ProfileSetupScreen(isEmployer: false),
              ),
        ),
        GetPage(
          name: '/notifications',
          page: () => AuthWrapper(child: const NotificationsScreen()),
        ),
        GetPage(
          name: '/notification_details',
          page: () => AuthWrapper(child: const NotificationDetailsScreen()),
        ),
      ],
    );
  }
}
