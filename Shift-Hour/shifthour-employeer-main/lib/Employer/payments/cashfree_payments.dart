import 'dart:math';

import 'package:flutter/material.dart';
import 'package:flutter_cashfree_pg_sdk/api/cferrorresponse/cferrorresponse.dart';
import 'package:flutter_cashfree_pg_sdk/api/cfpayment/cfwebcheckoutpayment.dart';
import 'package:flutter_cashfree_pg_sdk/api/cfpaymentgateway/cfpaymentgatewayservice.dart';
import 'package:flutter_cashfree_pg_sdk/api/cfsession/cfsession.dart';
import 'package:flutter_cashfree_pg_sdk/utils/cfenums.dart';
import 'package:flutter_cashfree_pg_sdk/utils/cfexceptions.dart';
import 'package:get/get.dart';
import 'package:shifthour_employeer/signup.dart';

import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:intl/intl.dart';
import 'package:http/http.dart' as http;
import 'dart:convert';

// Cashfree Service
class CashfreeService {
  // API Credentials - Should be stored securely or fetched from backend
  static const String _clientId =
      "87207aaaf09bc7e5721a4dd9370278"; // Replace with actual client ID
  static const String _clientSecret =
      "0f5992672990f923d9d4f926aeb2ee37efe9bda2"; // Replace with actual client secret
  static const String _apiVersion = "2023-08-01";

  // Environment selection - Change to Cashfree.Environment.PRODUCTION for live
  static const CFEnvironment environment = CFEnvironment.PRODUCTION;

  // Cashfree payment gateway service instance
  final CFPaymentGatewayService _cfPaymentGatewayService =
      CFPaymentGatewayService();

  // Callback functions for payment results
  Function(String)? onPaymentSuccess;
  Function(CFErrorResponse, String)? onPaymentError;

  CashfreeService() {
    _initializeCashfree();
  }

  // Initialize Cashfree service and set up callbacks
  void _initializeCashfree() {
    _cfPaymentGatewayService.setCallback(
      _handlePaymentSuccess,
      _handlePaymentError,
    );
  }

  // Set external callbacks
  void setCallbacks({
    required Function(String) onSuccess,
    required Function(CFErrorResponse, String) onError,
  }) {
    onPaymentSuccess = onSuccess;
    onPaymentError = onError;
  }

  void _handlePaymentSuccess(String orderId) async {
    try {
      print('Payment success callback received for order: $orderId');

      // Verify payment with Cashfree
      final verificationResult = await verifyPayment(orderId);
      print('Verification result: $verificationResult');

      if (verificationResult['success'] == true &&
          (verificationResult['order_status'] == 'PAID' ||
              verificationResult['payment_status'] == 'PAID')) {
        // If payment is verified, call the success callback if set
        if (onPaymentSuccess != null) {
          onPaymentSuccess!(orderId);
        }
      } else {
        print('Payment verification failed or payment not marked as PAID');

        // Handle payment verification failure
        if (onPaymentError != null) {
          final errorResponse = CFErrorResponse(
            'FAILED',
            'Payment verification failed',
            'VERIFICATION_ERROR',
            'PAYMENT',
          );
          onPaymentError!(errorResponse, orderId);
        }
      }
    } catch (e) {
      print('Error processing payment success: $e');

      // Handle any unexpected errors
      if (onPaymentError != null) {
        final errorResponse = CFErrorResponse(
          'ERROR',
          'Unexpected error occurred',
          'UNEXPECTED_ERROR',
          'SYSTEM',
        );
        onPaymentError!(errorResponse, orderId);
      }
    }
  }

  void _handlePaymentError(CFErrorResponse errorResponse, String orderId) {
    print("Payment error for order: $orderId - ${errorResponse.getMessage()}");
    if (onPaymentError != null) {
      onPaymentError!(errorResponse, orderId);
    }
  }

  Future<Map<String, dynamic>> createPaymentLink({
    required String paymentLinkId,
    required double amount,
    required Map<String, dynamic> customerDetails,
    Map<String, dynamic>? linkDetails,
  }) async {
    final Uri url = Uri.parse('https://api.cashfree.com/pg/links');

    try {
      // Get access token first
      final tokenResponse = await getAccessToken();
      if (!tokenResponse['success']) {
        return {'success': false, 'message': 'Failed to get access token'};
      }
      final accessToken = tokenResponse['access_token'];

      // Prepare payment link request body
      final Map<String, dynamic> body = {
        "link_id": paymentLinkId,
        "link_amount": amount.toStringAsFixed(2),
        "link_currency": "INR",
        "customer_details": {
          "customer_id": customerDetails['customer_id'],
          "customer_name": customerDetails['customer_name'],
          "customer_email": customerDetails['customer_email'],
          "customer_phone": customerDetails['customer_phone'],
        },
        "link_notify": {"send_sms": true, "send_email": true},
        "link_purpose": linkDetails?['link_purpose'] ?? "Payment",
        "link_meta": linkDetails?['link_meta'] ?? {},
        "link_expiry_time":
            DateTime.now()
                .toUtc()
                .add(Duration(days: 7))
                .toIso8601String(), // Link valid for 7 days
      };

      // Make payment link creation request
      final response = await http.post(
        url,
        headers: {
          'Content-Type': 'application/json',
          'Authorization': 'Bearer $accessToken',
          'x-api-version': _apiVersion,
        },
        body: jsonEncode(body),
      );

      final responseData = jsonDecode(response.body);

      // Check payment link creation status
      if (response.statusCode == 200) {
        return {
          'success': true,
          'payment_link_id': responseData['link_id'],
          'payment_link': responseData['link_url'],
          'short_url': responseData['short_url'],
          'response': responseData,
        };
      } else {
        return {
          'success': false,
          'message': responseData['message'] ?? 'Failed to create payment link',
          'response': responseData,
        };
      }
    } catch (e) {
      print('Error creating payment link: $e');
      return {'success': false, 'message': 'Error creating payment link: $e'};
    }
  }

  Future<Map<String, dynamic>> getAccessToken() async {
    final Uri url = Uri.parse('https://api.cashfree.com/pg/auth');

    try {
      final response = await http.post(
        url,
        headers: {'Content-Type': 'application/json'},
        body: jsonEncode({
          'client_id': _clientId, // 🔁 Use production client_id
          'client_secret': _clientSecret, // 🔁 Use production client_secret
        }),
      );

      final responseData = jsonDecode(response.body);

      if (response.statusCode == 200) {
        return {'success': true, 'access_token': responseData['access_token']};
      } else {
        return {
          'success': false,
          'message': responseData['message'] ?? 'Failed to get access token',
        };
      }
    } catch (e) {
      print('❌ Error getting access token: $e');
      return {'success': false, 'message': 'Error getting access token: $e'};
    }
  }

  Future<Map<String, dynamic>> createOrder({
    required double amount,
    required String orderId,
    required String customerId,
    required String customerName,
    required String customerEmail,
    required String customerPhone,
    String? orderNote,
  }) async {
    final Uri url = Uri.parse('https://api.cashfree.com/pg/orders');

    // ✅ Correct ISO8601 format (UTC) for expiry time
    final String expiryTime =
        DateTime.now()
            .toUtc()
            .add(Duration(hours: 1))
            .toIso8601String(); // automatically ends with 'Z'

    final Map<String, dynamic> body = {
      "order_id": orderId,
      "order_amount": amount.toStringAsFixed(2),
      "order_currency": "INR",
      "customer_details": {
        "customer_id": customerId,
        "customer_name": customerName,
        "customer_email": customerEmail,
        "customer_phone": customerPhone,
      },
      "order_meta": {
        "return_url":
            "https://yourapp.com/return?order_id={order_id}", // ✅ FIXED
      },
      "order_note": orderNote ?? "Adding funds to wallet",
      "order_expiry_time":
          DateTime.now()
              .toUtc()
              .add(Duration(hours: 1))
              .toIso8601String(), // ✅ valid ISO 8601
    };
    final Map<String, String> headers = {
      'x-client-id': _clientId,
      'x-client-secret': _clientSecret,
      'x-api-version': _apiVersion,
      'Content-Type': 'application/json',
    };

    try {
      final response = await http.post(
        url,
        headers: headers,
        body: jsonEncode(body),
      );

      final responseData = jsonDecode(response.body);

      if (response.statusCode == 200) {
        return {
          'success': true,
          'order_id': responseData['order_id'],
          'payment_session_id': responseData['payment_session_id'],
          'response': responseData,
        };
      } else {
        return {
          'success': false,
          'message': responseData['message'] ?? 'Failed to create order',
          'response': responseData,
        };
      }
    } catch (e) {
      return {'success': false, 'message': 'Error creating order: $e'};
    }
  }

  // Process payment using Cashfree WebCheckout
  Future<bool> processPayment({
    required String orderId,
    required String paymentSessionId,
  }) async {
    try {
      // Create Cashfree session
      final CFSession? session = _createSession(orderId, paymentSessionId);

      if (session == null) {
        print("Failed to create Cashfree session");
        return false;
      }

      // Create web checkout payment object
      final cfWebCheckout =
          CFWebCheckoutPaymentBuilder().setSession(session).build();

      // Initiate payment
      _cfPaymentGatewayService.doPayment(cfWebCheckout);
      return true;
    } catch (e) {
      print("Error processing payment: $e");
      return false;
    }
  }

  // Create Cashfree session
  CFSession? _createSession(String orderId, String paymentSessionId) {
    try {
      return CFSessionBuilder()
          .setEnvironment(environment)
          .setOrderId(orderId)
          .setPaymentSessionId(paymentSessionId)
          .build();
    } on CFException catch (e) {
      print("Error creating Cashfree session: ${e.message}");
      return null;
    }
  }

  // Verify payment status
  Future<Map<String, dynamic>> verifyPayment(String orderId) async {
    final Uri url = Uri.parse('https://api.cashfree.com/pg/orders/$orderId');

    try {
      // Get access token
      final tokenResponse = await getAccessToken();
      if (!tokenResponse['success']) {
        return {'success': false, 'message': 'Failed to get access token'};
      }
      final accessToken = tokenResponse['access_token'];

      // Make GET request to verify payment
      final response = await http.get(
        url,
        headers: {
          'Authorization': 'Bearer $accessToken',
          'x-api-version': _apiVersion,
          'Content-Type': 'application/json',
        },
      );

      final responseData = jsonDecode(response.body);

      if (response.statusCode == 200) {
        return {
          'success': true,
          'order_status': responseData['order_status'],
          'payment_status': responseData['payment_status'],
          'response': responseData,
        };
      } else {
        return {
          'success': false,
          'message': responseData['message'] ?? 'Failed to verify payment',
          'response': responseData,
        };
      }
    } catch (e) {
      print('Error verifying payment: $e');
      return {'success': false, 'message': 'Error verifying payment: $e'};
    }
  }
}

// Enhanced Wallet Controller with Cashfree Integration
class CashfreeWalletController extends GetxController {
  final SupabaseClient _supabaseClient = Supabase.instance.client;
  final RxBool isLoading = false.obs;
  final RxString error = ''.obs;

  // Wallet data
  final Rx<Map<String, dynamic>?> employerWallet = Rx<Map<String, dynamic>?>(
    null,
  );
  final RxList<Map<String, dynamic>> transactions =
      RxList<Map<String, dynamic>>([]);
  final RxList<Map<String, dynamic>> holdTransactions =
      RxList<Map<String, dynamic>>([]);

  // Cashfree service
  final CashfreeService _cashfreeService = CashfreeService();

  // Wallet and user IDs for caching
  String? _employerWalletId;
  String? _employerId;
  @override
  void onInit() {
    super.onInit();

    // Get the current authenticated user
    final currentUser = _supabaseClient.auth.currentUser;

    if (currentUser != null) {
      // Find the employer ID associated with the current user's email
      _findEmployerIdByEmail(currentUser.email).then((employerId) {
        _employerId = employerId;
        fetchEmployerWallet();
      });
    }

    // Set up Cashfree callbacks
    _cashfreeService.setCallbacks(
      onSuccess: _handlePaymentSuccess,
      onError: _handlePaymentError,
    );
  }

  Future<String?> _findEmployerIdByEmail(String? email) async {
    if (email == null) return null;

    try {
      final employerData =
          await _supabaseClient
              .from('employers')
              .select('id')
              .eq('contact_email', email)
              .maybeSingle();

      return employerData?['id'];
    } catch (e) {
      print('Error finding employer ID: $e');
      return null;
    }
  }

  // Handle payment error
  void _handlePaymentError(CFErrorResponse errorResponse, String orderId) {
    error.value = 'Payment failed: ${errorResponse.getMessage()}';
  }

  Future<Map<String, dynamic>?> _getTransactionDetailsByOrderId(
    String orderId,
  ) async {
    try {
      final response =
          await _supabaseClient
              .from('cashfree_transactions')
              .select()
              .eq('order_id', orderId)
              .maybeSingle();

      return response;
    } catch (e) {
      print('Error getting transaction details: $e');
      return null;
    }
  }

  Future<bool> createEmployerWalletIfNotExists() async {
    isLoading.value = true;
    error.value = '';

    try {
      if (_employerId == null) {
        error.value = 'User not authenticated';
        return false;
      }

      // Check if wallet already exists
      final existingWallet =
          await _supabaseClient
              .from('employer_wallet')
              .select()
              .eq('employer_id', _employerId!)
              .maybeSingle();

      if (existingWallet != null) {
        // Wallet already exists
        employerWallet.value = existingWallet;
        _employerWalletId = existingWallet['id'];
        await fetchTransactions();
        return true;
      }

      // Try using the RPC function
      try {
        await _supabaseClient.rpc(
          'create_employer_wallet',
          params: {'p_employer_id': _employerId},
        );
      } catch (rpcError) {
        print('RPC error, creating wallet directly: $rpcError');

        // Fallback: Create wallet directly if RPC fails
        final nowIso = DateTime.now().toIso8601String();
        await _supabaseClient.from('employer_wallet').insert({
          'employer_id': _employerId,
          'balance': 0,
          'currency': 'INR',
          'created_at': nowIso,
          'last_updated': nowIso,
        });
      }

      await fetchEmployerWallet();
      return true;
    } catch (e) {
      error.value = 'Failed to create wallet: $e';
      print('Error creating wallet: $e');
      return false;
    } finally {
      isLoading.value = false;
    }
  }

  // Helper method to generate UUID
  String _generateUuid() {
    // Simple UUID generator
    final random = Random();
    final hexDigits = '0123456789abcdef';
    final uuid = List<String>.filled(36, '');

    for (var i = 0; i < 36; i++) {
      if (i == 8 || i == 13 || i == 18 || i == 23) {
        uuid[i] = '-';
      } else if (i == 14) {
        uuid[i] = '4'; // Version 4 UUID
      } else if (i == 19) {
        uuid[i] = hexDigits[(random.nextInt(4) | 8)]; // Variant
      } else {
        uuid[i] = hexDigits[random.nextInt(16)];
      }
    }

    return uuid.join('');
  }

  Future<void> fetchEmployerWallet() async {
    isLoading.value = true;
    error.value = '';

    try {
      if (_employerId == null) {
        error.value = 'User not authenticated';
        return;
      }

      // First, try to fetch the employer record to ensure it exists
      final employerData =
          await _supabaseClient
              .from('employers')
              .select('id, contact_email')
              .eq(
                'contact_email',
                _supabaseClient.auth.currentUser?.email as Object,
              )
              .maybeSingle();

      if (employerData == null) {
        // No employer record found - this might require creating one
        error.value = 'Employer profile not found';
        employerWallet.value = null;
        _employerWalletId = null;
        return;
      }

      // Now check if the wallet exists
      final response =
          await _supabaseClient
              .from('employer_wallet')
              .select()
              .eq('employer_id', employerData['id'])
              .maybeSingle();

      if (response != null) {
        employerWallet.value = response;
        _employerWalletId = response['id'];

        // After fetching wallet, also fetch transactions
        await fetchTransactions();
      } else {
        // Wallet doesn't exist yet - attempt to create it
        try {
          // Try using RPC function to create wallet
          await _supabaseClient.rpc(
            'create_employer_wallet',
            params: {'p_employer_id': employerData['id']},
          );
        } catch (rpcError) {
          print('RPC error, creating wallet directly: $rpcError');

          // Fallback: Create wallet directly if RPC fails
          final nowIso = DateTime.now().toIso8601String();
          await _supabaseClient.from('employer_wallet').insert({
            'employer_id': employerData['id'],
            'balance': 0,
            'currency': 'INR',
            'created_at': nowIso,
            'last_updated': nowIso,
          });
        }

        // Refetch the wallet after creation
        final newWalletResponse =
            await _supabaseClient
                .from('employer_wallet')
                .select()
                .eq('employer_id', employerData['id'])
                .maybeSingle();

        if (newWalletResponse != null) {
          employerWallet.value = newWalletResponse;
          _employerWalletId = newWalletResponse['id'];
          await fetchTransactions();
        } else {
          // Wallet creation failed
          employerWallet.value = null;
          _employerWalletId = null;
          error.value = 'Failed to create wallet';
        }
      }
    } catch (e) {
      print('Error fetching wallet: $e');
      error.value = 'Failed to load wallet data: $e';
      employerWallet.value = null;
      _employerWalletId = null;
    } finally {
      isLoading.value = false;
    }
  }

  Future<bool> addFundsThroughCashfree(double amount) async {
    isLoading.value = true;
    error.value = '';

    try {
      final currentUser = _supabaseClient.auth.currentUser;
      if (currentUser == null) {
        error.value = 'User not authenticated';
        return false;
      }

      // Ensure email is not null and handle potential null case
      final userEmail = currentUser.email;
      if (userEmail == null) {
        error.value = 'No email associated with the account';
        return false;
      }

      // Find or create employer record
      final employerData =
          await _supabaseClient
              .from('employers')
              .select('id')
              .eq('contact_email', userEmail)
              .maybeSingle();

      String employerId;
      if (employerData == null) {
        // Create new employer record if not exists
        final nowIso = DateTime.now().toIso8601String();
        final newEmployer =
            await _supabaseClient
                .from('employers')
                .insert({
                  'contact_email': userEmail,
                  'contact_name': userEmail.split('@').first,
                  'created_at': nowIso,
                  'updated_at': nowIso,
                })
                .select('id')
                .single();

        employerId = newEmployer['id'];

        // Create wallet for new employer
        try {
          await _supabaseClient.from('employer_wallet').insert({
            'employer_id': employerId,
            'balance': 0,
            'currency': 'INR',
            'created_at': nowIso,
            'last_updated': nowIso,
            'hold_balance': 0,
          });
        } catch (walletError) {
          print('Error creating wallet: $walletError');
          error.value = 'Failed to create wallet';
          return false;
        }
      } else {
        employerId = employerData['id'];
      }

      // Generate unique order ID
      final String orderId =
          'WALLET_${employerId.substring(0, 8)}_${DateTime.now().millisecondsSinceEpoch}';
      final now = DateTime.now().toIso8601String();

      // Insert initial transaction record
      await _supabaseClient.from('cashfree_transactions').insert({
        'user_id': currentUser.id,
        'user_type': 'employer',
        'order_id': orderId,
        'amount': amount,
        'status': 'PENDING',
        'payment_method': 'Web Checkout',
        'created_at': now,
      });

      // Create order via Cashfree
      final createOrderResult = await _cashfreeService.createOrder(
        amount: amount,
        orderId: orderId,
        customerId: employerId,
        customerName: 'Employer',
        customerEmail: userEmail,
        customerPhone: '9999999999',
      );

      if (createOrderResult['success'] != true) {
        error.value = createOrderResult['message'] ?? 'Failed to create order';
        await _supabaseClient
            .from('cashfree_transactions')
            .update({'status': 'FAILED'})
            .eq('order_id', orderId);
        return false;
      }

      // Update transaction with payment session details
      await _supabaseClient
          .from('cashfree_transactions')
          .update({
            'payment_session_id': createOrderResult['payment_session_id'],
            'cashfree_order_data': createOrderResult['response'],
          })
          .eq('order_id', orderId);

      final paymentInitiated = await _cashfreeService.processPayment(
        orderId: orderId,
        paymentSessionId: createOrderResult['payment_session_id'],
      );

      if (!paymentInitiated) {
        error.value = 'Failed to initiate payment';
        await _supabaseClient
            .from('cashfree_transactions')
            .update({'status': 'FAILED'})
            .eq('order_id', orderId);
        return false;
      }

      // Final return to ensure method always returns a boolean
      return true;
    } catch (e) {
      print('Error in addFundsThroughCashfree: $e');
      error.value = 'Error adding funds: $e';
      return false;
    } finally {
      isLoading.value = false;
    }
  }

  Future<void> fetchHoldTransactions() async {
    try {
      if (_employerId == null) {
        return;
      }

      final response = await _supabaseClient
          .from('shift_holds')
          .select('*') // Select all columns
          .eq('employer_id', _employerId!)
          .order('created_at', ascending: false);

      if (response != null) {
        holdTransactions.value = List<Map<String, dynamic>>.from(response);
      }
    } catch (e) {
      print('Error Fetching Hold Transactions:');
      print('Error Type: ${e.runtimeType}');
      print('Error Message: $e');
    }
  } // In payment success handler

  void _handlePaymentSuccess(String orderId) async {
    try {
      print('Payment success callback received for order: $orderId');

      // Verify payment with Cashfree
      final verificationResult = await _cashfreeService.verifyPayment(orderId);
      print('Verification result: $verificationResult');

      if (verificationResult['success'] == true &&
          (verificationResult['order_status'] == 'PAID' ||
              verificationResult['payment_status'] == 'PAID')) {
        // Get transaction details
        final transactionDetails = await _getTransactionDetailsByOrderId(
          orderId,
        );
        print('Transaction details: $transactionDetails');

        if (transactionDetails != null) {
          final amount = transactionDetails['amount'] as num;

          // Update transaction status first
          await _supabaseClient
              .from('cashfree_transactions')
              .update({'status': 'PAID'})
              .eq('order_id', orderId);

          // Ensure we have the wallet ID
          if (_employerWalletId == null) {
            await fetchEmployerWallet();
          }

          try {
            // Try RPC function first
            final walletResult = await _supabaseClient.rpc(
              'add_funds_to_employer_wallet',
              params: {
                'p_employer_id': _employerId,
                'p_amount': amount,
                'p_order_id': orderId,
              },
            );
            print('RPC result: $walletResult');

            // If RPC succeeds but doesn't handle transaction recording
            if (walletResult['success'] == true) {
              await _recordTransactionForSuccessfulPayment(orderId, amount);
            }
          } catch (rpcError) {
            print('RPC error, falling back to direct update: $rpcError');
            // Use the helper method for wallet update and transaction recording
            await _updateWalletAndRecordTransaction(amount, orderId);
          }

          // Show success message
          Get.snackbar(
            'Payment Successful',
            'Your wallet has been funded with ₹${amount.toStringAsFixed(2)}',
            snackPosition: SnackPosition.BOTTOM,
            backgroundColor: Colors.green.shade100,
            colorText: Colors.green.shade800,
          );
        }
      } else {
        print('Payment verification failed or payment not marked as PAID');
        error.value = 'Payment verification failed';
      }
    } catch (e) {
      print('Error processing payment success: $e');
      error.value = 'Error processing payment: $e';
    } finally {
      // Always refresh wallet data at the end
      await fetchEmployerWallet();
    }
  }

  Future<void> _updateWalletAndRecordTransaction(
    num amount,
    String orderId,
  ) async {
    try {
      final nowIso = DateTime.now().toIso8601String();

      // Ensure we have employer ID
      if (_employerId == null) {
        throw Exception('Employer ID not found');
      }

      // Get current wallet data
      final wallet =
          await _supabaseClient
              .from('employer_wallet')
              .select()
              .eq('employer_id', _employerId!)
              .maybeSingle();

      if (wallet != null) {
        // Set wallet ID
        _employerWalletId = wallet['id'];

        // Update wallet balance (no total_deposited field)
        final currentBalance = wallet['balance'] ?? 0;
        final updatedBalance = (currentBalance as num) + amount;

        await _supabaseClient
            .from('employer_wallet')
            .update({'balance': updatedBalance, 'last_updated': nowIso})
            .eq('employer_id', _employerId!);

        // Generate a proper UUID for the transaction
        final transactionId = _generateUuid();
        final user = supabase.auth.currentUser;
        // Insert transaction record
        final transactionData = {
          'id': transactionId,
          'user_id': user!.id,
          'wallet_id': _employerWalletId,
          'amount': amount,
          'transaction_type': 'deposit',
          'description': 'Wallet funding via Cashfree - Order: $orderId',
          'reference_id':
              _generateUuid(), // Generate another UUID for reference
          'created_at': nowIso,
          'status': 'completed',
        };

        await _supabaseClient
            .from('wallet_transactions')
            .insert(transactionData);

        print('Transaction recorded successfully');
      } else {
        throw Exception('Wallet not found for employer $_employerId');
      }
    } catch (e) {
      print('Error updating wallet and recording transaction: $e');
      error.value = 'Error updating wallet: $e';
      throw e;
    }
  }

  // Also fix the _recordTransactionForSuccessfulPayment method
  Future<void> _recordTransactionForSuccessfulPayment(
    String orderId,
    num amount,
  ) async {
    try {
      // Ensure we have wallet ID
      if (_employerWalletId == null) {
        if (_employerId == null) {
          throw Exception('Employer ID not found');
        }

        final wallet =
            await _supabaseClient
                .from('employer_wallet')
                .select('id')
                .eq('employer_id', _employerId!)
                .maybeSingle();

        if (wallet != null) {
          _employerWalletId = wallet['id'];
        } else {
          throw Exception('Wallet not found for employer $_employerId');
        }
      }

      // Create transaction record
      final nowIso = DateTime.now().toIso8601String();
      final transactionId = _generateUuid();
      final supabase = Supabase.instance.client;
      final user = supabase.auth.currentUser;
      final transactionData = {
        'id': transactionId,
        'user_id': user!.id,
        'wallet_id': _employerWalletId,
        'amount': amount,
        'transaction_type': 'deposit',
        'description': 'Wallet funding via Cashfree - Order: $orderId',
        'reference_id': _generateUuid(), // Generate a UUID for reference_id
        'created_at': nowIso,
        'status': 'completed',
      };

      print('Creating transaction record with data: $transactionData');

      await _supabaseClient.from('wallet_transactions').insert(transactionData);

      print('Transaction record created successfully');
    } catch (e) {
      print('Failed to record transaction: $e');
      throw e;
    }
  }

  Future<void> fetchTransactions() async {
    try {
      if (_employerWalletId == null) {
        return;
      }

      final response = await _supabaseClient
          .from('wallet_transactions')
          .select()
          .eq('wallet_id', _employerWalletId!)
          .order('created_at', ascending: false);

      if (response != null) {
        transactions.value = List<Map<String, dynamic>>.from(response);
      }

      // Also fetch hold transactions (for shifts)
      await fetchHoldTransactions();
    } catch (e) {
      print('Error fetching transactions: $e');
    }
  }

  // Hold amount for shift
  Future<Map<String, dynamic>> holdAmountForShift(
    String shiftId,
    double amount,
  ) async {
    isLoading.value = true;
    error.value = '';

    try {
      if (_employerId == null) {
        error.value = 'User not authenticated';
        return {'success': false, 'message': 'User not authenticated'};
      }

      // Check if wallet exists and has sufficient balance
      if (employerWallet.value == null) {
        error.value = 'Wallet not found';
        return {'success': false, 'message': 'Wallet not found'};
      }

      final balance = employerWallet.value!['balance'];
      final numBalance =
          balance is int ? balance.toDouble() : balance as double;

      if (numBalance < amount) {
        error.value = 'Insufficient balance';
        return {'success': false, 'message': 'Insufficient balance'};
      }

      final result = await _supabaseClient.rpc(
        'hold_amount_for_shift',
        params: {
          'p_employer_id': _employerId,
          'p_shift_id': shiftId,
          'p_amount': amount,
        },
      );

      if (result['success'] == true) {
        await fetchEmployerWallet(); // Refresh wallet data
        return result;
      } else {
        error.value = result['message'] ?? 'Failed to hold amount';
        return {'success': false, 'message': error.value};
      }
    } catch (e) {
      error.value = 'Error holding amount: $e';
      print('Error holding amount: $e');
      return {'success': false, 'message': error.value};
    } finally {
      isLoading.value = false;
    }
  }

  // Transfer amount to worker (checkout)
  Future<Map<String, dynamic>> transferAmountToWorker(
    String shiftId,
    String workerId,
  ) async {
    isLoading.value = true;
    error.value = '';

    try {
      if (_employerId == null) {
        error.value = 'User not authenticated';
        return {'success': false, 'message': 'User not authenticated'};
      }

      final result = await _supabaseClient.rpc(
        'transfer_amount_to_worker',
        params: {
          'p_employer_id': _employerId,
          'p_worker_id': workerId,
          'p_shift_id': shiftId,
        },
      );

      if (result['success'] == true) {
        await fetchEmployerWallet(); // Refresh wallet data
        return result;
      } else {
        error.value = result['message'] ?? 'Failed to transfer amount';
        return {'success': false, 'message': error.value};
      }
    } catch (e) {
      error.value = 'Error transferring amount: $e';
      print('Error transferring amount: $e');
      return {'success': false, 'message': error.value};
    } finally {
      isLoading.value = false;
    }
  }

  // Cancel shift as employer
  Future<Map<String, dynamic>> cancelShiftAsEmployer(
    String shiftId,
    DateTime cancelDeadline,
  ) async {
    isLoading.value = true;
    error.value = '';

    try {
      if (_employerId == null) {
        error.value = 'User not authenticated';
        return {'success': false, 'message': 'User not authenticated'};
      }

      final result = await _supabaseClient.rpc(
        'employer_cancel_shift',
        params: {
          'p_employer_id': _employerId,
          'p_shift_id': shiftId,
          'p_cancel_deadline': cancelDeadline.toIso8601String(),
        },
      );

      if (result['success'] == true) {
        await fetchEmployerWallet(); // Refresh wallet data
        return result;
      } else {
        error.value = result['message'] ?? 'Failed to cancel shift';
        return {'success': false, 'message': error.value};
      }
    } catch (e) {
      error.value = 'Error cancelling shift: $e';
      print('Error cancelling shift: $e');
      return {'success': false, 'message': error.value};
    } finally {
      isLoading.value = false;
    }
  }

  // Format currency
  String formatCurrency(double amount) {
    return NumberFormat.currency(
      locale: 'en_IN',
      symbol: '₹',
      decimalDigits: 2,
    ).format(amount);
  }

  // Get formatted wallet balance
  String getFormattedWalletBalance() {
    if (employerWallet.value == null) return '₹0.00';

    final balance = employerWallet.value!['balance'];
    final numBalance = balance is int ? balance.toDouble() : balance as double;

    return formatCurrency(numBalance);
  }
}

// Example UI for adding funds
class AddFundsDialog extends StatelessWidget {
  final CashfreeWalletController controller =
      Get.find<CashfreeWalletController>();
  final TextEditingController amountController = TextEditingController();

  AddFundsDialog({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Dialog(
      child: Container(
        padding: EdgeInsets.all(16),
        child: Column(
          mainAxisSize: MainAxisSize.min,
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              'Add Funds',
              style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
            ),
            SizedBox(height: 16),
            TextField(
              controller: amountController,
              keyboardType: TextInputType.numberWithOptions(decimal: true),
              decoration: InputDecoration(
                labelText: 'Amount (₹)',
                border: OutlineInputBorder(),
                prefixIcon: Icon(Icons.currency_rupee),
              ),
            ),
            SizedBox(height: 24),
            Row(
              mainAxisAlignment: MainAxisAlignment.end,
              children: [
                TextButton(onPressed: () => Get.back(), child: Text('Cancel')),
                SizedBox(width: 8),
                ElevatedButton(
                  onPressed: () {
                    if (amountController.text.isEmpty) {
                      Get.snackbar(
                        'Error',
                        'Please enter an amount.',
                        snackPosition: SnackPosition.BOTTOM,
                      );
                      return;
                    }

                    final amount = double.tryParse(amountController.text);
                    if (amount == null || amount <= 0) {
                      Get.snackbar(
                        'Error',
                        'Please enter a valid amount.',
                        snackPosition: SnackPosition.BOTTOM,
                      );
                      return;
                    }

                    Get.back();

                    // Use Cashfree for payment processing
                    controller.addFundsThroughCashfree(amount);
                  },
                  child: Text('Add Funds'),
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }
}
