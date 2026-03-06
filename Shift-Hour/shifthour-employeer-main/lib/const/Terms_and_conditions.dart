import 'package:flutter/material.dart';

class EmployerTermsPage extends StatelessWidget {
  const EmployerTermsPage({super.key});

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Employer Terms & Conditions'),
        centerTitle: true,
        backgroundColor: Colors.blue,
      ),
      body: SingleChildScrollView(
        padding: const EdgeInsets.all(16.0),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: const [
            Text(
              '1. Eligibility',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
            Text(
              'Employers must be registered Indian entities (e.g., Pvt Ltd, LLP, Proprietorship) or authorized individuals responsible for hiring.',
            ),
            SizedBox(height: 16),

            Text(
              '2. Job Posting Guidelines',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
            Text(
              'All job posts must be legal, ethical, and free of discrimination. Misleading or fraudulent job listings are strictly prohibited.',
            ),
            SizedBox(height: 16),

            Text(
              '3. Worker Compensation',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
            Text(
              'Employers must provide fair compensation as agreed during job posting. Delayed or non-payment can result in account suspension.',
            ),
            SizedBox(height: 16),

            Text(
              '4. Verification Requirements',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
            Text(
              'Employers must verify their KYC and company information before posting shifts or hiring workers.',
            ),
            SizedBox(height: 16),

            Text(
              '5. Cancellation & Penalties',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
            Text(
              'Repeated cancellation of posted shifts may result in penalties or temporary suspension of account.',
            ),
            SizedBox(height: 16),

            Text(
              '6. Worker Conduct & Feedback',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
            Text(
              'Employers should treat workers respectfully. Abuse or harassment may lead to legal consequences and platform ban.',
            ),
            SizedBox(height: 16),

            Text(
              '7. Platform Fee & Charges',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
            Text(
              'Employers agree to any service charges or commission as mentioned during shift posting or hiring. Fee structure may change with prior notice.',
            ),
            SizedBox(height: 16),

            Text(
              '8. Legal Compliance',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
            Text(
              'Employers are responsible for compliance with Indian labor laws, PF/ESI if applicable, and any local hiring regulations.',
            ),
            SizedBox(height: 16),

            Text(
              '9. Data Usage',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
            Text(
              'Employer data will be used in accordance with our Privacy Policy and will not be shared with unauthorized third parties.',
            ),
            SizedBox(height: 16),

            Text(
              '10. Agreement Changes',
              style: TextStyle(fontWeight: FontWeight.bold),
            ),
            Text(
              'We reserve the right to update these terms at any time. Continued use of the platform implies acceptance of the revised terms.',
            ),
          ],
        ),
      ),
    );
  }
}
