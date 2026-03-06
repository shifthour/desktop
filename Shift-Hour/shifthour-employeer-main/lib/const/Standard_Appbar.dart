import 'package:flutter/material.dart';
import 'package:lucide_icons/lucide_icons.dart';

class StandardAppBar extends StatelessWidget implements PreferredSizeWidget {
  final String title;
  final bool centerTitle;
  final Function()? onBackPressed;
  final List<Widget>? actions;
  final bool isScrolled;
  final bool isDarkMode;
  final Widget? customTitle;
  final String? subtitle;
  final bool useSliverAppBar;
  final bool pinned;
  final bool floating;

  const StandardAppBar({
    Key? key,
    required this.title,
    this.centerTitle = true,
    this.onBackPressed,
    this.actions,
    this.isScrolled = false,
    this.isDarkMode = false,
    this.customTitle,
    this.subtitle,
    this.useSliverAppBar = false,
    this.pinned = true,
    this.floating = false,
  }) : super(key: key);

  @override
  Size get preferredSize => const Size.fromHeight(kToolbarHeight);

  @override
  Widget build(BuildContext context) {
    final backgroundColor = isDarkMode ? Colors.grey.shade800 : Colors.white;
    final textColor = isDarkMode ? Colors.white : Colors.black;
    final subtitleColor = isDarkMode ? Colors.grey.shade300 : Colors.grey;

    // Create styled back button
    final backButton =
        onBackPressed != null || Navigator.of(context).canPop()
            ? Container(
              height: 36,
              width: 36,
              decoration: BoxDecoration(
                color: isDarkMode ? Colors.grey.shade700 : Colors.grey.shade100,
                borderRadius: BorderRadius.circular(8),
                border: Border.all(
                  color: isDarkMode ? Colors.transparent : Colors.grey.shade200,
                ),
                boxShadow: [
                  BoxShadow(
                    color: Colors.black.withOpacity(0.03),
                    blurRadius: 5,
                    offset: const Offset(0, 2),
                  ),
                ],
              ),
              child: IconButton(
                icon: Icon(LucideIcons.arrowLeft, size: 16, color: textColor),
                padding: EdgeInsets.zero,
                onPressed: onBackPressed ?? () => Navigator.of(context).pop(),
              ),
            )
            : null;

    final titleWidget =
        customTitle ??
        Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          mainAxisSize: MainAxisSize.min,
          children: [
            Text(
              title,
              style: TextStyle(
                fontSize: 20,
                fontWeight: FontWeight.w600,
                color: textColor,
              ),
            ),
            if (subtitle != null) ...[
              const SizedBox(height: 2),
              Text(
                subtitle!,
                style: TextStyle(fontSize: 13, color: subtitleColor),
              ),
            ],
          ],
        );

    if (useSliverAppBar) {
      return SliverAppBar(
        pinned: pinned,
        floating: floating,
        elevation: isScrolled ? 4 : 0,
        backgroundColor: backgroundColor,
        // Always show back button styled consistently with the rest of the app
        leading:
            backButton != null
                ? Padding(
                  padding: const EdgeInsets.only(left: 8.0),
                  child: backButton,
                )
                : null,
        title: titleWidget,
        centerTitle: centerTitle,
        actions: actions,
      );
    }

    return AppBar(
      backgroundColor: backgroundColor,
      elevation: isScrolled ? 4 : 0,
      automaticallyImplyLeading: false, // We handle back button ourselves
      centerTitle: centerTitle,
      // If title is not centered and we have a back button, show both in a row
      title:
          !centerTitle && backButton != null
              ? Row(
                children: [
                  backButton,
                  const SizedBox(width: 12),
                  Expanded(child: titleWidget),
                ],
              )
              : titleWidget,
      // If title is centered and we have a back button, show in leading position
      leading: centerTitle && backButton != null ? backButton : null,
      actions: actions,
      flexibleSpace: Container(
        decoration: BoxDecoration(
          boxShadow:
              isScrolled
                  ? [
                    BoxShadow(
                      color: Colors.black.withOpacity(0.05),
                      spreadRadius: 1,
                      blurRadius: 5,
                      offset: const Offset(0, 2),
                    ),
                  ]
                  : null,
          borderRadius: const BorderRadius.only(
            bottomLeft: Radius.circular(16),
            bottomRight: Radius.circular(16),
          ),
        ),
      ),
    );
  }
}

// Example usage with notifications and job icon
class ExampleActions {
  static List<Widget> getStandardActions({
    bool showJobIcon = true,
    bool showNotifications = true,
    Function()? onNotificationTap,
    int notificationCount = 0,
  }) {
    return [
      if (showJobIcon)
        Container(
          width: 32,
          height: 32,
          margin: const EdgeInsets.only(right: 8),
          decoration: const BoxDecoration(
            color: Color(0xFFEEF2FF), // indigo-100
            shape: BoxShape.circle,
          ),
          child: const Icon(
            Icons.work_outline,
            size: 16,
            color: Color(0xFF4F46E5), // indigo-600
          ),
        ),
      if (showNotifications)
        Stack(
          children: [
            IconButton(
              icon: const Icon(
                Icons.notifications_none,
                color: Colors.black,
                size: 20,
              ),
              onPressed: onNotificationTap ?? () {},
            ),
            if (notificationCount > 0)
              Positioned(
                top: 12,
                right: 12,
                child: Container(
                  width: 8,
                  height: 8,
                  decoration: const BoxDecoration(
                    color: Colors.red,
                    shape: BoxShape.circle,
                  ),
                ),
              ),
          ],
        ),
    ];
  }
}
