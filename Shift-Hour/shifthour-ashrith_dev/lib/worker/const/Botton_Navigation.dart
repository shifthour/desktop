import 'package:flutter/material.dart';
import 'package:get/get.dart';
import 'package:get/get_core/src/get_main.dart';
import 'package:get/get_rx/src/rx_types/rx_types.dart';
import 'package:get/get_state_manager/src/simple/get_controllers.dart';
import 'package:shifthour/Employer/employer_dashboard.dart';
import 'package:shifthour/worker/Earnings/earnings_dashboard.dart';
import 'package:shifthour/worker/Find%20Jobs/Find_Jobs.dart';
import 'package:shifthour/worker/my_profile.dart' show ProfileScreen;
import 'package:shifthour/worker/shifts/My_Shifts_Dashboard.dart';
import 'package:shifthour/worker/worker_dashboard.dart';

// GetX controller for navigation with permanent flag
class NavigationController extends GetxController {
  // Make this controller permanent to avoid recreation
  static NavigationController get to => Get.find();

  // Observable for current index
  final RxInt currentIndex = 0.obs;

  // Flag to control if navigation should happen
  final RxBool enableNavigation = true.obs;

  // Scroll controller for handling scroll to top
  ScrollController? _scrollController;

  // Method to set scroll controller
  void setScrollController(ScrollController controller) {
    _scrollController = controller;
  }

  // Method to change the selected index
  void changePage(int index) {
    // If tapping the current page's icon, scroll to top
    if (index == currentIndex.value && _scrollController != null) {
      _scrollController!.animateTo(
        0.0,
        duration: const Duration(milliseconds: 300),
        curve: Curves.easeInOut,
      );
      return;
    }

    // Update the index
    currentIndex.value = index;

    // Only navigate if navigation is enabled
    if (enableNavigation.value) {
      navigateToPage(index);
    }
  }

  // Method to handle navigation based on index
  // Method to handle navigation based on index
  void navigateToPage(int index) {
    switch (index) {
      case 0:
        Get.offAll(() => const WorkerDashboard());
        break;
      case 1:
        Get.offAll(() => const FindJobsPage());
        break;
      case 2:
        Get.offAll(() => const MyShiftsPage());
        break;
      case 3:
        Get.offAll(() => const WorkerWalletScreen());
        break;
      case 4:
        Get.offAll(() => const ProfileScreen());
        break;
    }
  }

  // Helper method to safely set current tab without navigation
  void setTabWithoutNavigation(int index) {
    // Temporarily disable navigation
    enableNavigation.value = false;

    // Set the index
    currentIndex.value = index;

    // Re-enable navigation after a short delay
    Future.delayed(const Duration(milliseconds: 100), () {
      enableNavigation.value = true;
    });
  }
}

// This is the shared bottom navigation component using GetX
class ShiftHourBottomNavigation extends StatelessWidget {
  const ShiftHourBottomNavigation({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    // Put the controller with permanent flag
    final NavigationController controller = Get.put(
      NavigationController(),
      permanent: true, // This makes it permanent
    );

    return Container(
      decoration: BoxDecoration(
        color: Colors.white,
        boxShadow: [
          BoxShadow(
            color: Colors.grey.withOpacity(0.2),
            spreadRadius: 0,
            blurRadius: 10,
            offset: const Offset(0, -2),
          ),
        ],
      ),
      child: Obx(
        () => BottomNavigationBar(
          currentIndex: controller.currentIndex.value,
          onTap: controller.changePage,
          backgroundColor: Colors.white,
          type: BottomNavigationBarType.fixed,
          selectedItemColor: Colors.blue.shade700,
          unselectedItemColor: Colors.grey.shade600,
          selectedLabelStyle: const TextStyle(
            fontFamily: 'Inter',
            fontSize: 12,
            fontWeight: FontWeight.w600,
          ),
          unselectedLabelStyle: const TextStyle(
            fontFamily: 'Inter',
            fontSize: 12,
          ),
          items: const [
            BottomNavigationBarItem(
              icon: Icon(Icons.home_outlined),
              activeIcon: Icon(Icons.home),
              label: 'Home',
            ),
            BottomNavigationBarItem(
              icon: Icon(Icons.search_outlined),
              activeIcon: Icon(Icons.search),
              label: 'Find Shifts',
            ),
            BottomNavigationBarItem(
              icon: Icon(Icons.calendar_today),
              activeIcon: Icon(Icons.calendar_today),
              label: 'My Shifts',
            ),
            BottomNavigationBarItem(
              icon: Icon(Icons.account_balance_wallet_outlined),
              activeIcon: Icon(Icons.account_balance_wallet_outlined),
              label: 'Wallet',
            ),
            BottomNavigationBarItem(
              icon: Icon(Icons.person_outline),
              activeIcon: Icon(Icons.person),
              label: 'Profile',
            ),
          ],
          elevation: 0,
        ),
      ),
    );
  }
}

mixin NavigationMixin {
  // Method to set the current tab when entering a page
  void setCurrentTab(int index) {
    try {
      // Try to find the existing controller
      final controller = Get.find<NavigationController>();
      // Use the safer method instead of directly setting the value
      controller.setTabWithoutNavigation(index);
    } catch (e) {
      // If not found, put a new permanent one
      final controller = Get.put(NavigationController(), permanent: true);
      controller.setTabWithoutNavigation(index);
    }
  }

  // Method to set scroll controller for the page
  void setPageScrollController(ScrollController controller) {
    try {
      // Try to find the existing controller
      final navigationController = Get.find<NavigationController>();
      // Set the scroll controller
      navigationController.setScrollController(controller);
    } catch (e) {
      // If not found, put a new permanent one
      final navigationController = Get.put(
        NavigationController(),
        permanent: true,
      );
      navigationController.setScrollController(controller);
    }
  }
}
