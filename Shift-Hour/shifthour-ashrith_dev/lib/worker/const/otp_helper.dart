import 'dart:convert';
import 'package:flutter_dotenv/flutter_dotenv.dart';
import 'package:http/http.dart' as http;

class SupabaseFunctions {
  static Future<void> sendResendEmail(String toEmail, String otp) async {
    final resendApiKey = dotenv.env['RESEND_API_KEY'];
    final fromEmail = dotenv.env['FROM_EMAIL'];

    if (resendApiKey == null || fromEmail == null) {
      throw Exception('RESEND_API_KEY or FROM_EMAIL is not set in .env file');
    }

    final url = Uri.parse('https://api.resend.com/emails');
    final headers = {
      'Authorization': 'Bearer $resendApiKey',
      'Content-Type': 'application/json',
    };

    final htmlContent = '''
      <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #e0e0e0; border-radius: 10px;">
        <div style="background: linear-gradient(to right, #5B6BF8, #8B65D9); padding: 20px; border-radius: 10px 10px 0 0; text-align: center;">
          <h1 style="color: white; margin: 0;">$otp</h1>
        </div>
        <div style="padding: 20px;">
          <p>Hello,</p>
          <p>Please enter the above OTP in your mobile application to log in. The code is valid for 5 minutes.</p>
          <p style="margin-top: 30px;">Thank you for using our platform!</p>
          <p>The ShiftHour Team</p>
        </div>
        <div style="background-color: #f5f5f5; padding: 15px; border-radius: 0 0 10px 10px; text-align: center; font-size: 12px; color: #666;">
          <p>This is an automated message. Please do not reply to this email.</p>
          <p>© 2025 ShiftHour. All rights reserved.</p>
        </div>
      </div>
    ''';

    final body = jsonEncode({
      'from': fromEmail,
      'to': toEmail,
      'subject': 'Login OTP',
      'html': htmlContent,
    });

    final response = await http.post(url, headers: headers, body: body);

    if (response.statusCode != 200 && response.statusCode != 202) {
      throw Exception('Failed to send OTP email: ${response.body}');
    }
  }
}
