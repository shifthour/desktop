import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:shifthour_employeer/main.dart' as app;

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  group('Employee Management Flow Tests', () {
    testWidgets('Add new employee and verify in employee list', (
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

      // Navigate to Employees section
      await tester.tap(find.text('Employees'));
      await tester.pumpAndSettle();

      // Tap on Add Employee button
      await tester.tap(find.byIcon(Icons.add));
      await tester.pumpAndSettle();

      // Fill employee details
      await tester.enterText(find.byKey(const Key('name_field')), 'Jane Smith');
      await tester.enterText(
        find.byKey(const Key('email_field')),
        'jane@example.com',
      );
      await tester.enterText(find.byKey(const Key('phone_field')), '555-5678');
      await tester.enterText(
        find.byKey(const Key('position_field')),
        'Cashier',
      );
      await tester.enterText(
        find.byKey(const Key('hourly_rate_field')),
        '18.50',
      );

      // Save the new employee
      await tester.tap(find.text('Save'));
      await tester.pumpAndSettle();

      // Verify the employee appears in the list
      expect(find.text('Jane Smith'), findsOneWidget);
      expect(find.text('Cashier'), findsOneWidget);
    });

    testWidgets('Edit existing employee details', (WidgetTester tester) async {
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

      // Navigate to Employees section
      await tester.tap(find.text('Employees'));
      await tester.pumpAndSettle();

      // Find and tap on an existing employee (assuming John Doe exists)
      await tester.tap(find.text('John Doe'));
      await tester.pumpAndSettle();

      // Tap edit button
      await tester.tap(find.byIcon(Icons.edit));
      await tester.pumpAndSettle();

      // Update position
      await tester.enterText(
        find.byKey(const Key('position_field')),
        'Senior Manager',
      );

      // Save the changes
      await tester.tap(find.text('Save'));
      await tester.pumpAndSettle();

      // Verify the updated information
      expect(find.text('Senior Manager'), findsOneWidget);
    });
  });
}
