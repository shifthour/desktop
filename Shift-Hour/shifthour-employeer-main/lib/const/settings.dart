import 'package:flutter/material.dart';
import 'package:get/get.dart';
import 'package:http/http.dart';
import 'package:shifthour_employeer/const/Standard_Appbar.dart';
import 'package:shifthour_employeer/const/Bottom_Navigation.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:shifthour_employeer/const/Terms_and_conditions.dart';
import 'package:shifthour_employeer/const/privacy_policy.dart';
import 'package:url_launcher/url_launcher.dart';
import 'package:package_info_plus/package_info_plus.dart';

class SettingsController extends GetxController {
  final RxBool isDarkMode = false.obs;
  final RxBool notificationsEnabled = true.obs;
  final RxString appVersion = "".obs;
  final RxString buildNumber = "".obs;

  @override
  void onInit() {
    super.onInit();
    loadSettings();
    getAppInfo();
  }

  Future<void> getAppInfo() async {
    try {
      PackageInfo packageInfo = await PackageInfo.fromPlatform();
      appVersion.value = packageInfo.version;
      buildNumber.value = packageInfo.buildNumber;
    } catch (e) {
      print('Error getting app info: $e');
      appVersion.value = "1.0.0";
      buildNumber.value = "1";
    }
  }

  Future<void> loadSettings() async {
    final prefs = await SharedPreferences.getInstance();
    isDarkMode.value = prefs.getBool('darkMode') ?? false;
    notificationsEnabled.value = prefs.getBool('notifications') ?? true;
  }

  Future<void> toggleDarkMode(bool value) async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.setBool('darkMode', value);
    isDarkMode.value = value;

    // Apply theme change
    Get.changeThemeMode(value ? ThemeMode.dark : ThemeMode.light);
  }

  Future<void> toggleNotifications(bool value) async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.setBool('notifications', value);
    notificationsEnabled.value = value;

    // Implement your notification logic here
    if (value) {
      // Enable notifications
      print('Notifications enabled');
    } else {
      // Disable notifications
      print('Notifications disabled');
    }
  }

  Future<void> openTermsOfService() async {
    const url = 'https://yourcompany.com/terms-of-service';
    if (await canLaunch(url)) {
      await launch(url);
    } else {
      Get.snackbar(
        'Error',
        'Could not open terms of service',
        snackPosition: SnackPosition.BOTTOM,
        backgroundColor: Colors.red,
        colorText: Colors.white,
      );
    }
  }
}

class SettingsScreen extends StatelessWidget {
  const SettingsScreen({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    final SettingsController controller = Get.put(SettingsController());
    final isDark = Theme.of(context).brightness == Brightness.dark;

    return Scaffold(
      appBar: StandardAppBar(title: 'Settings', centerTitle: false),
      bottomNavigationBar: const ShiftHourBottomNavigation(),
      body: Obx(
        () => ListView(
          padding: const EdgeInsets.all(16.0),
          children: [
            // App Appearance Section
            _buildSectionHeader(context, 'Appearance'),
            _buildSettingCard(
              context,
              leading: Icon(
                Icons.dark_mode,
                color: isDark ? Colors.blue : Colors.amber,
              ),
              title: 'Dark Theme',
              subtitle: 'Enable dark mode for the app',
              trailing: Switch(
                value: controller.isDarkMode.value,
                onChanged: (value) => controller.toggleDarkMode(value),
                activeColor: Colors.blue,
              ),
            ),
            const SizedBox(height: 8),

            // Notifications Section
            _buildSectionHeader(context, 'Notifications'),
            _buildSettingCard(
              context,
              leading: Icon(Icons.notifications, color: Colors.green),
              title: 'Push Notifications',
              subtitle: 'Receive alerts for new shifts and updates',
              trailing: Switch(
                value: controller.notificationsEnabled.value,
                onChanged: (value) => controller.toggleNotifications(value),
                activeColor: Colors.blue,
              ),
            ),
            const SizedBox(height: 8),

            // Legal Section
            _buildSectionHeader(context, 'Legal'),
            _buildSettingCard(
              context,
              leading: Icon(Icons.privacy_tip, color: Colors.purple),
              title: 'Privacy Policy',
              subtitle: 'View our privacy policy',
              trailing: Icon(Icons.chevron_right),
              onTap: () => Get.to(() => PrivacyPolicyScreen()),
            ),
            const SizedBox(height: 4),
            _buildSettingCard(
              context,
              leading: Icon(Icons.description, color: Colors.blue),
              title: 'Terms of Service',
              subtitle: 'View our terms of service',
              trailing: Icon(Icons.chevron_right),
              onTap: () => Get.to(() => EmployerTermsPage()),
            ),
            const SizedBox(height: 8),

            _buildSectionHeader(context, 'About'),
            _buildSettingCard(
              context,
              leading: Icon(Icons.info, color: Colors.cyan),
              title: 'App Version',
              subtitle:
                  'Version ${controller.appVersion.value} (Build ${controller.buildNumber.value})',
            ),
            const SizedBox(height: 16),

            // Contact & Support Section
            _buildSectionHeader(context, 'Contact & Support'),
            _buildSettingCard(
              context,
              leading: Icon(Icons.phone, color: Colors.teal),
              title: 'Customer Support',
              subtitle: 'Call us at 7204265777',
              trailing: Icon(Icons.call),
              onTap: () async {
                const phoneNumber = 'tel:+917204265777';
                if (await canLaunch(phoneNumber)) {
                  await launch(phoneNumber);
                } else {
                  Get.snackbar(
                    'Error',
                    'Could not launch dialer',
                    snackPosition: SnackPosition.BOTTOM,
                    backgroundColor: Colors.red,
                    colorText: Colors.white,
                  );
                }
              },
            ),
            const SizedBox(height: 24),

            // Logout Button
          ],
        ),
      ),
    );
  }

  Widget _buildSectionHeader(BuildContext context, String title) {
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: 16.0, vertical: 8.0),
      child: Text(
        title,
        style: TextStyle(
          color: Theme.of(context).colorScheme.primary,
          fontSize: 14,
          fontWeight: FontWeight.bold,
        ),
      ),
    );
  }

  Widget _buildSettingCard(
    BuildContext context, {
    required Widget leading,
    required String title,
    required String subtitle,
    Widget? trailing,
    VoidCallback? onTap,
  }) {
    return Card(
      elevation: 0,
      shape: RoundedRectangleBorder(
        borderRadius: BorderRadius.circular(12),
        side: BorderSide(
          color: Theme.of(context).dividerColor.withOpacity(0.1),
        ),
      ),
      child: ListTile(
        contentPadding: const EdgeInsets.symmetric(
          horizontal: 16.0,
          vertical: 8.0,
        ),
        leading: Container(
          padding: const EdgeInsets.all(8),
          decoration: BoxDecoration(
            color:
                Theme.of(context).brightness == Brightness.dark
                    ? Colors.grey.shade800
                    : Colors.grey.shade100,
            borderRadius: BorderRadius.circular(8),
          ),
          child: leading,
        ),
        title: Text(
          title,
          style: TextStyle(fontWeight: FontWeight.w600, fontSize: 16),
        ),
        subtitle: Text(
          subtitle,
          style: TextStyle(
            fontSize: 14,
            color: Theme.of(context).textTheme.bodySmall?.color,
          ),
        ),
        trailing: trailing,
        onTap: onTap,
      ),
    );
  }
}
