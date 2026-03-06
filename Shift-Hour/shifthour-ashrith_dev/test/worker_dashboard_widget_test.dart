import 'package:flutter/material.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:shifthour/worker/worker_dashboard.dart';

void main() {
  testWidgets('WorkerDashboard has a title', (WidgetTester tester) async {
    // Build our app and trigger a frame.
    await tester.pumpWidget(MaterialApp(home: WorkerDashboard()));

    // Verify that the worker dashboard has a title.
    expect(find.text('Dashboard'), findsOneWidget);
  });

  testWidgets('WorkerDashboard displays greeting', (WidgetTester tester) async {
    // Build our app and trigger a frame.
    await tester.pumpWidget(MaterialApp(home: WorkerDashboard()));

    // Verify that the worker dashboard displays a greeting.
    expect(find.textContaining('Hello,'), findsOneWidget);
  });

  // You can add more tests here to verify other widgets and functionalities of the WorkerDashboard.
}
