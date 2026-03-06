import 'package:flutter/material.dart';

class WorkerTermsPage extends StatelessWidget {
  const WorkerTermsPage({super.key});

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Worker Terms & Conditions'),
        centerTitle: true,
        backgroundColor: Colors.blue,
      ),
      body: SingleChildScrollView(
        padding: const EdgeInsets.all(16.0),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: const [
            Text(
              '1. Eligibility to Work',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
            Text(
              'You must be an Indian citizen above 18 years of age and legally eligible to work in India. KYC verification is mandatory.',
            ),
            SizedBox(height: 16),

            Text(
              '2. Shift Attendance',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
            Text(
              'Workers must arrive on time and complete the assigned shift hours. Repeated absenteeism may lead to account suspension.',
            ),
            SizedBox(height: 16),

            Text(
              '3. Code of Conduct',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
            Text(
              'Professional behavior is expected at all times. Harassment, theft, or misconduct will result in a permanent ban and legal action.',
            ),
            SizedBox(height: 16),

            Text(
              '4. Payment and Payouts',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
            Text(
              'Wages will be transferred to your verified bank account or wallet as per the payment cycle of the platform. Ensure KYC and account details are accurate.',
            ),
            SizedBox(height: 16),

            Text(
              '5. Shift Cancellations',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
            Text(
              'If you cannot attend a shift, cancel at least 12 hours in advance. Frequent cancellations may affect your profile visibility.',
            ),
            SizedBox(height: 16),

            Text(
              '6. Platform Usage',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
            Text(
              'The app must not be used for fraudulent activities. Accounts found misusing the platform will be reported to authorities.',
            ),
            SizedBox(height: 16),

            Text(
              '7. Ratings and Feedback',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
            Text(
              'Employers can rate your performance. Low ratings due to repeated issues may reduce job recommendations.',
            ),
            SizedBox(height: 16),

            Text(
              '8. Taxes and Compliance',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
            Text(
              'You are responsible for your income tax liabilities under Indian tax laws. TDS may be deducted where applicable.',
            ),
            SizedBox(height: 16),

            Text(
              '9. Insurance and Safety',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
            Text(
              'Some shifts may include accidental coverage or insurance. Please read job details carefully before applying.',
            ),
            SizedBox(height: 16),

            Text(
              '10. Changes to Terms',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
            Text(
              'We reserve the right to update these terms at any time. Continued usage of the app implies your agreement with the latest terms.',
            ),
          ],
        ),
      ),
    );
  }
}
