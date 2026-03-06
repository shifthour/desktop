import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:shifthour/worker/Help&Support.dart';

void main() {
  testWidgets('HelpAndSupportPage has a title', (WidgetTester tester) async {
    // Build our app and trigger a frame.
    await tester.pumpWidget(MaterialApp(home: HelpAndSupportPage()));

    // Verify that the HelpAndSupportPage has a title.
    expect(find.text('Help & Support'), findsOneWidget);
  });
}
