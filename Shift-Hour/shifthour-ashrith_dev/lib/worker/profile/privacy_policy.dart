import 'package:flutter/material.dart';

class PrivacyPolicyPage extends StatelessWidget {
  const PrivacyPolicyPage({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Dialog(
      shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
      elevation: 8,
      child: Container(
        width: MediaQuery.of(context).size.width * 0.9,
        height: MediaQuery.of(context).size.height * 0.8,
        decoration: BoxDecoration(
          borderRadius: BorderRadius.circular(16),
          color: Colors.white,
        ),
        child: Column(
          children: [
            // App Bar
            Container(
              decoration: BoxDecoration(
                gradient: LinearGradient(
                  colors: [Colors.blue.shade600, Colors.indigo.shade700],
                  begin: Alignment.topLeft,
                  end: Alignment.bottomRight,
                ),
                borderRadius: BorderRadius.only(
                  topLeft: Radius.circular(16),
                  topRight: Radius.circular(16),
                ),
              ),
              padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 12),
              child: Row(
                mainAxisAlignment: MainAxisAlignment.spaceBetween,
                children: [
                  Text(
                    'Privacy Policy',
                    style: TextStyle(
                      color: Colors.white,
                      fontSize: 18,
                      fontWeight: FontWeight.bold,
                    ),
                  ),
                  IconButton(
                    icon: Icon(Icons.close, color: Colors.white),
                    onPressed: () => Navigator.of(context).pop(),
                  ),
                ],
              ),
            ),

            // Scrollable Privacy Policy Content
            Expanded(
              child: SingleChildScrollView(
                padding: const EdgeInsets.all(16),
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(
                      'Privacy Policy for ShiftHour',
                      style: TextStyle(
                        fontSize: 20,
                        fontWeight: FontWeight.bold,
                        color: Colors.blue.shade700,
                      ),
                    ),
                    SizedBox(height: 16),
                    Text(
                      'Last updated: April 04, 2025',
                      style: TextStyle(
                        fontStyle: FontStyle.italic,
                        color: Colors.grey.shade600,
                      ),
                    ),
                    SizedBox(height: 16),
                    _buildSectionTitle('Overview'),
                    _buildParagraph(
                      'This Privacy Policy describes our policies and procedures on the collection, use, and disclosure of Your information when You use the ShiftHour Service.',
                    ),
                    SizedBox(height: 16),
                    _buildSectionTitle('Information We Collect'),
                    _buildBulletPoint('Email address'),
                    _buildBulletPoint('First and last name'),
                    _buildBulletPoint('Phone number'),
                    _buildBulletPoint('Address details'),
                    SizedBox(height: 16),
                    _buildSectionTitle('How We Use Your Information'),
                    _buildBulletPoint('Provide and maintain our Service'),
                    _buildBulletPoint('Manage Your Account'),
                    _buildBulletPoint('Contact You about updates'),
                    _buildBulletPoint('Improve our Service'),
                    SizedBox(height: 16),
                    _buildSectionTitle('Contact Us'),
                    _buildParagraph(
                      'If you have any questions about this Privacy Policy, You can contact us:',
                    ),
                    _buildBulletPoint('Email: shifthour.jobs@gmail.com'),
                    _buildBulletPoint('Phone: 8088848484'),
                  ],
                ),
              ),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildSectionTitle(String title) {
    return Padding(
      padding: const EdgeInsets.only(bottom: 8),
      child: Text(
        title,
        style: TextStyle(
          fontSize: 16,
          fontWeight: FontWeight.bold,
          color: Colors.blue.shade700,
        ),
      ),
    );
  }

  Widget _buildParagraph(String text) {
    return Text(
      text,
      style: TextStyle(fontSize: 14, color: Colors.grey.shade800, height: 1.5),
    );
  }

  Widget _buildBulletPoint(String text) {
    return Padding(
      padding: const EdgeInsets.only(bottom: 8),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Padding(
            padding: const EdgeInsets.only(top: 6, right: 8),
            child: Container(
              width: 6,
              height: 6,
              decoration: BoxDecoration(
                color: Colors.blue.shade700,
                shape: BoxShape.circle,
              ),
            ),
          ),
          Expanded(
            child: Text(
              text,
              style: TextStyle(
                fontSize: 14,
                color: Colors.grey.shade800,
                height: 1.5,
              ),
            ),
          ),
        ],
      ),
    );
  }
}
