import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:shifthour_employeer/main.dart' as app;

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  group('Shift Scheduling Flow Tests', () {
    testWidgets('Create a new shift and verify it appears in schedule', (
      WidgetTester tester,
    ) async {
      // Start the app
      app.main();
      await tester.pumpAndSettle();

      // Login (assuming we start at login screen)
      await tester.enterText(
        find.byKey(const Key('email_field')),
        'admin@shifthour.com',
      );
      await tester.enterText(
        find.byKey(const Key('password_field')),
        'admin123',
      );
      await tester.tap(find.byType(ElevatedButton));
      await tester.pumpAndSettle();

      // Navigate to Schedule section
      await tester.tap(find.text('Schedule'));
      await tester.pumpAndSettle();

      // Tap on Add Shift button
      await tester.tap(find.byIcon(Icons.add));
      await tester.pumpAndSettle();

      // Select employee (assuming dropdown exists)
      await tester.tap(find.byKey(const Key('employee_dropdown')));
      await tester.pumpAndSettle();
      await tester.tap(find.text('John Doe').last);
      await tester.pumpAndSettle();

      // Select position
      await tester.tap(find.byKey(const Key('position_dropdown')));
      await tester.pumpAndSettle();
      await tester.tap(find.text('Cashier').last);
      await tester.pumpAndSettle();

      // Set date and time (this is simplified, actual date/time picker interaction would be more complex)
      await tester.tap(find.byKey(const Key('date_picker')));
      await tester.pumpAndSettle();
      await tester.tap(find.text('15')); // Selecting 15th day of month
      await tester.tap(find.text('OK'));
      await tester.pumpAndSettle();

      // Set start time
      await tester.tap(find.byKey(const Key('start_time_picker')));
      await tester.pumpAndSettle();
      await tester.tap(find.text('9'));
      await tester.tap(find.text('00'));
      await tester.tap(find.text('OK'));
      await tester.pumpAndSettle();

      // Set end time
      await tester.tap(find.byKey(const Key('end_time_picker')));
      await tester.pumpAndSettle();
      await tester.tap(find.text('5'));
      await tester.tap(find.text('00'));
      await tester.tap(find.text('OK'));
      await tester.pumpAndSettle();

      // Save the shift
      await tester.tap(find.text('Save'));
      await tester.pumpAndSettle();

      // Verify the shift appears in the schedule
      expect(find.text('John Doe'), findsAtLeastNWidgets(1));
      expect(find.text('9:00 AM - 5:00 PM'), findsOneWidget);
      expect(find.text('Cashier'), findsAtLeastNWidgets(1));
    });

    testWidgets('Update shift status to completed', (
      WidgetTester tester,
    ) async {
      // Start the app
      app.main();
      await tester.pumpAndSettle();

      // Login
      await tester.enterText(
        find.byKey(const Key('email_field')),
        'admin@shifthour.com',
      );
      await tester.enterText(
        find.byKey(const Key('password_field')),
        'admin123',
      );
      await tester.tap(find.byType(ElevatedButton));
      await tester.pumpAndSettle();

      // Navigate to Schedule section
      await tester.tap(find.text('Schedule'));
      await tester.pumpAndSettle();

      // Find and tap on an existing shift
      await tester.tap(find.text('John Doe').first);
      await tester.pumpAndSettle();

      // Change status to completed
      await tester.tap(find.byKey(const Key('status_dropdown')));
      await tester.pumpAndSettle();
      await tester.tap(find.text('Completed').last);
      await tester.pumpAndSettle();

      // Save changes
      await tester.tap(find.text('Save'));
      await tester.pumpAndSettle();

      // Verify status changed (assuming completed shifts have a specific indicator or text)
      expect(find.byIcon(Icons.check_circle), findsAtLeastNWidgets(1));
    });
  });
}
