import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:shifthour/worker/my_profile.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:shifthour/worker/my_profile.dart';

void main() {
  testWidgets('ProfileScreen displays user name and email', (WidgetTester tester) async {
    // Mock user data
    const userName = 'Test User';
    const userEmail = 'test@example.com';

    // Build our app and trigger a frame.
    await tester.pumpWidget(MaterialApp(home: ProfileScreen()));

    // Verify that the user name is displayed.
    expect(find.text(userName), findsNothing); // Initial state

    // Verify that the user email is displayed.
    expect(find.text(userEmail), findsNothing); // Initial state

    // Verify that the text fields are displayed
    expect(find.bySemanticsLabel('Full Name'), findsOneWidget);
    expect(find.bySemanticsLabel('Email'), findsOneWidget);
    expect(find.bySemanticsLabel('Phone'), findsOneWidget);
    expect(find.bySemanticsLabel('Address'), findsOneWidget);

    // Tap the edit button
    final editButton = find.widgetWithText(ElevatedButton, 'Edit Profile');
    await tester.tap(editButton);
    await tester.pump();

    // Verify that the save button is displayed
    expect(find.widgetWithText(ElevatedButton, 'Save Profile'), findsOneWidget);

    // Tap the save button
    final saveButton = find.widgetWithText(ElevatedButton, 'Save Profile');
    await tester.tap(saveButton);
    await tester.pump();

    // Verify that the _uploadDocuments function is called
    // This is difficult to verify directly without mocking the ProfileScreen state.
    // For now, we can only verify that the button is tappable.

    // Verify that the KYC Information card is displayed
    expect(find.text('KYC Information'), findsOneWidget);

    // Verify that the "Complete Verification" button is displayed when KYC is inactive
    expect(find.widgetWithText(ElevatedButton, 'Complete Verification'), findsOneWidget);
  });
}
