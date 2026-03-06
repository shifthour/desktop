import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:flutter_dotenv/flutter_dotenv.dart';
import 'package:get/get.dart';
import 'package:shifthour_employeer/Employer/Employee%20%20workers/Eworkers_dashboard.dart';
import 'package:shifthour_employeer/Employer/employeer_profile.dart';
import 'package:shifthour_employeer/Employer/employer_dashboard.dart';
import 'package:shifthour_employeer/Employer/payments/employeer_payments_dashboard.dart';
import 'package:shifthour_employeer/Employer/post%20jobs/post_job.dart';
import 'package:shifthour_employeer/const/Notification.dart';
import 'package:shifthour_employeer/const/auth.dart';
import 'package:shifthour_employeer/const/otp.dart';
import 'package:shifthour_employeer/login.dart';
import 'package:shifthour_employeer/profile.dart';
import 'package:shifthour_employeer/services/notification.dart';
import 'package:shifthour_employeer/splash.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:onesignal_flutter/onesignal_flutter.dart';

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

  // ✅ Register external user ID for OneSignal
  final userId = Supabase.instance.client.auth.currentUser?.id;
  if (userId != null) {
    await OneSignal.login(userId);
    print("🔗 OneSignal externalUserId set: $userId");
  }

  runApp(const MyApp());
}

Future<void> _initializeOneSignal() async {
  try {
    await NotificationService().initializeOneSignal();
    print('OneSignal initialized successfully');
  } catch (e) {
    print('Error initializing OneSignal: $e');
  }
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
        // 🔓 Public Routes
        GetPage(name: '/splash', page: () => const SplashScreen()),
        GetPage(name: '/login', page: () => const EmployerLoginPage()),
        GetPage(
          name: '/otp',
          page: () => const OtpVerificationScreen(email: ''),
        ),

        // 🔐 Authenticated Routes - All wrapped with AuthWrapper
        GetPage(
          name: '/employer_dashboard',
          page: () => AuthWrapper(child: const EmployerDashboard()),
        ),
        GetPage(
          name: '/profile_setup',
          page: () => AuthWrapper(child: const EmployerProfileSetupScreen()),
        ),
        GetPage(
          name: '/notifications',
          page: () => AuthWrapper(child: const NotificationsScreen()),
        ),
        GetPage(
          name: '/workers',
          page: () => AuthWrapper(child: const WorkerApplicationsScreen()),
        ),
        GetPage(
          name: '/employeer',
          page: () => AuthWrapper(child: const EmployerProfileScreen()),
        ),
        GetPage(
          name: '/wallet',
          page: () => AuthWrapper(child: const WalletScreen()),
        ),
        GetPage(
          name: '/postshifts',
          page: () => AuthWrapper(child: const PostShiftScreen()),
        ),
      ],
    );
  }
}
