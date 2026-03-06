import 'dart:convert';
import 'dart:math' as Math;
import 'dart:typed_data';
import 'package:encrypt/encrypt.dart' as Prefix;
import 'package:fast_rsa/fast_rsa.dart';
import 'package:http/http.dart' as http;
import 'package:intl/intl.dart';
import 'package:pointycastle/asn1/asn1_parser.dart';
import 'package:pointycastle/asn1/primitives/asn1_bit_string.dart';
import 'package:pointycastle/asn1/primitives/asn1_integer.dart';
import 'package:pointycastle/asn1/primitives/asn1_sequence.dart';
import 'package:pointycastle/export.dart';
import 'package:flutter/services.dart';

class CashfreePayoutsService {
  // Cashfree API credentials
  static const String clientId = "CF87207D0MQD4V8CQAC73E79F7G";
  static const String clientSecret =
      "cfsk_ma_prod_01cb2cf58cb3cf88e8ea83f614d42755_83a2a0cd";

  // API endpoints for test environment
  // Update these constants in your service class
  // Update these constants in your service class
  static const String baseUrl = "https://payout-api.cashfree.com";
  static const String authorizeEndpoint = "$baseUrl/payout/v1/authorize";
  static const String createCashgramEndpoint =
      "$baseUrl/payout/v1/createCashgram";

  // Public key for RSA encryption in PEM format - THIS SHOULD BE YOUR DOWNLOADED PUBLIC KEY
  // The key below is just a placeholder - you need to replace it with your actual public key
  // that you downloaded from Cashfree Dashboard > Developers > Payouts > Two-Factor Authentication > Public Key
  static const String publicKey = '''-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtGJexVkOVMiO8lmr7XUV
egUmHHfOKwuPBk9rekG41oAgsQi72ac4RglcnpWKKuwT90fOx4sL9Vn21K2jIw5M
aO4BbK38sQ5jM6C1fd+9MyHs92HqHYWtFIGwwCAOBCvTEGeOrCuKOn0dUwZn3XPP
nhSi0hMRb6FINpmceQLEv1F4ugv/GRjR/xpFTBZXmA1gasIkNwHgQZd4A2xS80PY
DHHEDz1rBHZ5LK+y7XtQjY0qEdqqMKSm/gRg89rkHxkzUbTeFJ+OTPYs01UDOt6D
g8ubOObkqGFU9tvztUCWzlqcMzBwEpM3zCzevAXiKygljmRQ0BGzkN205FhR0fUZ
NQIDAQAB
-----END PUBLIC KEY-----''';

  // Cache token to avoid unnecessary API calls
  static String? _authToken;
  static DateTime? _tokenExpiry;
  static Future<String?> generateSignature() async {
    try {
      final clientId = "CF87207D0MQD4V8CQAC73E79F7G";
      final timestamp = (DateTime.now().millisecondsSinceEpoch / 1000).floor();
      final encodedData = "$clientId.$timestamp";

      print("🛠️ Starting signature generation");
      print("🧾 Data to encrypt: $encodedData");

      // Encrypt using RSA with OAEP padding
      final encrypted = await RSA.encryptOAEP(
        encodedData,
        "", // Label (empty)
        Hash.SHA1, // Use SHA1 to match your original implementation
        publicKey,
      );

      print("👉 Generated signature: $encrypted");

      return encrypted;
    } catch (e) {
      print('❌ Error generating signature: $e');
      print('❌ Error stack trace: ${e is Exception ? e.toString() : ""}');
      return null; // Match Python's approach of returning None on error
    }
  }

  static BigInt extractBigIntFromBytes(Uint8List bytes) {
    BigInt result = BigInt.zero;
    for (int i = 0; i < bytes.length; i++) {
      result = (result << 8) | BigInt.from(bytes[i]);
    }
    return result;
  }

  static BigInt decodeBigInt(Uint8List bytes) {
    BigInt result = BigInt.zero;
    for (int i = 0; i < bytes.length; i++) {
      result = (result << 8) | BigInt.from(bytes[i]);
    }
    return result;
  }

  static Future<String> getAuthToken() async {
    if (_authToken != null &&
        _tokenExpiry != null &&
        DateTime.now().isBefore(_tokenExpiry!)) {
      print("👉 Using cached token: $_authToken");
      return _authToken!;
    }

    try {
      // Get signature for authentication
      final signatureData = await generateSignature();

      // Check if signature is null
      if (signatureData == null) {
        throw Exception('Failed to generate signature');
      }

      // Get current timestamp in seconds
      final timestamp =
          (DateTime.now().millisecondsSinceEpoch / 1000).floor().toString();

      print(
        "👉 Making authorize API call with signature: ${signatureData.substring(0, Math.min(20, signatureData.length))}...",
      );

      // Make the authorize API call
      final response = await http.post(
        Uri.parse(authorizeEndpoint),
        headers: {
          'X-Client-Id': clientId,
          'X-Client-Secret': clientSecret,
          'X-Cf-Signature': signatureData,
          'X-Cf-Timestamp': timestamp,
          'Content-Type': 'application/json',
        },
      );
      print("🔑 Requesting new auth token");
      print("📅 Timestamp used: $timestamp");
      print("🔐 Signature: ${signatureData?.substring(0, 30)}...");
      print("👉 Auth response status: ${response.statusCode}");
      print("👉 Auth response body: ${response.body}");

      if (response.statusCode == 200) {
        final responseJson = json.decode(response.body);
        if (responseJson['status'] == 'SUCCESS') {
          _authToken = responseJson['data']['token'];
          _tokenExpiry = DateTime.now().add(Duration(minutes: 9));
          print(
            "👉 Auth token obtained: ${_authToken!.substring(0, Math.min(20, _authToken!.length))}...",
          );

          return _authToken!;
          print("✅ Auth response status: ${response.statusCode}");
          print("✅ Auth response body: ${response.body}");
        } else {
          print("❌ Auth failed: ${responseJson['message']}");
          throw Exception('Authentication failed: ${responseJson['message']}');
        }
      } else {
        print("❌ Auth failed with status: ${response.statusCode}");
        throw Exception(
          'Authentication failed: ${response.statusCode}, ${response.body}',
        );
      }
    } catch (e) {
      print('❌ Error getting auth token: $e');
      throw Exception('Failed to authenticate with Cashfree: $e');
    }
  }

  static Future<Map<String, dynamic>> createCashgram({
    required String cashgramId,
    required String name,
    required String email,
    required String phone,
    required double amount,
    String? linkExpiry,
    String? remarks,
  }) async {
    try {
      // Validate inputs
      if (cashgramId.isEmpty ||
          name.isEmpty ||
          email.isEmpty ||
          phone.isEmpty ||
          amount <= 0) {
        return {'status': 'ERROR', 'message': 'Invalid input parameters'};
      }

      print("👉 Starting Cashgram creation process");

      // Get authentication token
      String token;
      try {
        token = await getAuthToken();
        print("👉 Successfully obtained auth token: $token");
      } catch (e) {
        print("❌ Failed to obtain auth token: $e");
        return {'status': 'ERROR', 'message': 'Authentication failed: $e'};
      }

      // Prepare payload with notifyCustomer as int instead of bool
      final payload = {
        'cashgramId': cashgramId,
        'amount': amount, // Keep as double
        'name': name,
        'email': email,
        'phone': phone,
        'linkExpiry': linkExpiry ?? _getFormattedExpiry(),
        'remarks': remarks ?? 'sample cashgram',
        'validateAccount': 0,
        'payoutType': "Refund",
        'description': "Test",
        'notifyCustomer': 1, // Changed from true to 1
      };

      print("🚀 Starting Cashgram creation");
      print("📤 Payload: ${json.encode(payload)}");

      final response = await http.post(
        Uri.parse(createCashgramEndpoint),
        headers: {
          'Content-Type': 'application/json',
          'accept': 'application/json',
          'Authorization': 'Bearer $token',
        },
        body: json.encode(payload),
      );
      print("📥 Response Status: ${response.statusCode}");
      print("📥 Response Body: ${response.body}");
      print(
        "👉 Request headers: ${{'Content-Type': 'application/json', 'accept': 'application/json', 'Authorization': 'Bearer $token'}}",
      );
      print("👉 Cashgram API response status: ${response.statusCode}");
      print("👉 Cashgram API response body: ${response.body}");

      // Parse and return the response
      final responseJson = json.decode(response.body);

      if (response.statusCode == 200 && responseJson['status'] == 'SUCCESS') {
        return {
          'status': 'SUCCESS',
          'message': 'Cashgram created successfully',
          'cashgramLink': responseJson['data']['cashgramLink'],
          'cashgramId': cashgramId,
          'responseData': responseJson['data'],
        };
      } else {
        return {
          'status': 'ERROR',
          'message': responseJson['message'] ?? 'Failed to create Cashgram',
          'subCode': responseJson['subCode'] ?? '',
          'responseData': responseJson,
        };
      }
    } catch (e) {
      print('❌ Error creating Cashgram: $e');
      return {
        'status': 'ERROR',
        'message': 'An unexpected error occurred: ${e.toString()}',
      };
    }
  } // Helper for date formatting

  static String _getFormattedExpiry() {
    final expiryDate = DateTime.now().add(Duration(days: 7));
    return "${expiryDate.year}/${expiryDate.month.toString().padLeft(2, '0')}/${expiryDate.day.toString().padLeft(2, '0')}";
  }

  static Future<Map<String, dynamic>> getCashgramStatus(
    String cashgramId,
  ) async {
    try {
      // Get authentication token
      final token = await getAuthToken();

      // Make the API call
      final response = await http.get(
        Uri.parse('$baseUrl/v1/getCashgramStatus?cashgramId=$cashgramId'),
        headers: {
          'Content-Type': 'application/json',
          'Authorization': 'Bearer $token',
        },
      );

      // Parse and return the response
      final responseJson = json.decode(response.body);

      if (response.statusCode == 200 && responseJson['status'] == 'SUCCESS') {
        return {'status': 'SUCCESS', 'data': responseJson['data']};
      } else {
        return {
          'status': 'ERROR',
          'message': responseJson['message'] ?? 'Failed to get Cashgram status',
          'subCode': responseJson['subCode'] ?? '',
        };
      }
    } catch (e) {
      print('Error getting Cashgram status: $e');
      return {
        'status': 'ERROR',
        'message': 'An unexpected error occurred: ${e.toString()}',
      };
    }
  }

  // Generate link expiry date (7 days from now)
  static String _calculateLinkExpiry() {
    final expiryDate = DateTime.now().add(Duration(days: 7));
    return DateFormat('yyyy-MM-dd').format(expiryDate);
  }

  // Utility method to validate email format
  static bool isValidEmail(String email) {
    final emailRegExp = RegExp(r'^[^@]+@[^@]+\.[^@]+$');
    return emailRegExp.hasMatch(email);
  }

  // Utility method to validate phone format (Indian format)
  static bool isValidPhone(String phone) {
    final phoneRegExp = RegExp(r'^[6-9]\d{9}$');
    return phoneRegExp.hasMatch(phone);
  }
}
