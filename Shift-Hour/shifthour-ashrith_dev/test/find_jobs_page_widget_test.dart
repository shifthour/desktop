import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:shifthour/worker/Find Jobs/Find_Jobs.dart';

void main() {
  testWidgets('FindJobsPage has a title', (WidgetTester tester) async {
    // Build our app and trigger a frame.
    await tester.pumpWidget(MaterialApp(home: FindJobsPage()));

    // Verify that the FindJobsPage has a title.
    expect(find.text('Find Jobs'), findsOneWidget);
  });
}
