import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:shifthour/login.dart';

void main() {
  testWidgets('WorkerLoginPage has a title and email input', (
    WidgetTester tester,
  ) async {
    // Build our app and trigger a frame.
    await tester.pumpWidget(MaterialApp(home: WorkerLoginPage()));

    // Verify that the login page has a title.
    expect(find.text('Welcome Back'), findsOneWidget);

    // Verify that there is an email input field.
    expect(find.bySemanticsLabel('Your email address'), findsOneWidget);
  });

  testWidgets('WorkerLoginPage can enter email and tap send OTP', (
    WidgetTester tester,
  ) async {
    // Build our app and trigger a frame.
    await tester.pumpWidget(MaterialApp(home: WorkerLoginPage()));

    // Find the email input field.
    final emailField = find.bySemanticsLabel('Your email address');

    // Enter 'test@example.com' into the email field.
    await tester.enterText(emailField, 'test@example.com');

    // Verify that the text field has the correct value.
    expect(find.text('test@example.com'), findsOneWidget);

    // Find the 'Send OTP' button.
    final sendOtpButton = find.widgetWithText(ElevatedButton, 'Send OTP');

    // Tap the 'Send OTP' button.
    await tester.tap(sendOtpButton);
    await tester.pump();

    // You can add more expectations here to verify that the OTP sending process is initiated.
    // For example, you can check if a loading indicator appears or if a navigation event occurs.
  });
}
