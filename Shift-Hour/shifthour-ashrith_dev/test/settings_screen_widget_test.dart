import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:shifthour/worker/settings.dart';

void main() {
  testWidgets('SettingsScreen has a title', (WidgetTester tester) async {
    // Build our app and trigger a frame.
    await tester.pumpWidget(MaterialApp(home: SettingsScreen()));

    // Verify that the SettingsScreen has a title.
    expect(find.text('Settings'), findsOneWidget);
  });
}
