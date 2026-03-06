import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:shifthour/worker/privacy_policy.dart';

void main() {
  testWidgets('PrivacyPolicyScreen has a title', (WidgetTester tester) async {
    // Build our app and trigger a frame.
    await tester.pumpWidget(MaterialApp(home: PrivacyPolicyScreen()));

    // Verify that the PrivacyPolicyScreen has a title.
    expect(find.text('Privacy Policy'), findsOneWidget);
  });
}
