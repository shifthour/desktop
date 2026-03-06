import 'package:flutter/material.dart';
import 'package:get/get.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:intl/intl.dart';

class Payment {
  final String id;
  final String workerName;
  final String workerId;
  final double amount;
  final DateTime dueDate;
  final PaymentStatus status;
  final String? jobReference;
  final double? hoursWorked;
  final String? paymentMethod;
  final String? transactionId;

  Payment({
    required this.id,
    required this.workerName,
    required this.workerId,
    required this.amount,
    required this.dueDate,
    required this.status,
    this.jobReference,
    this.hoursWorked,
    this.paymentMethod,
    this.transactionId,
  });
}

enum PaymentStatus { pending, processing, completed, failed }

extension PaymentStatusExtension on PaymentStatus {
  String get name {
    switch (this) {
      case PaymentStatus.pending:
        return 'Pending';
      case PaymentStatus.processing:
        return 'Processing';
      case PaymentStatus.completed:
        return 'Completed';
      case PaymentStatus.failed:
        return 'Failed';
    }
  }

  Color get color {
    switch (this) {
      case PaymentStatus.pending:
        return Colors.amber.shade700;
      case PaymentStatus.processing:
        return Colors.blue.shade700;
      case PaymentStatus.completed:
        return Colors.green.shade600;
      case PaymentStatus.failed:
        return Colors.red.shade600;
    }
  }

  static PaymentStatus fromString(String status) {
    switch (status.toLowerCase()) {
      case 'pending':
        return PaymentStatus.pending;
      case 'processing':
        return PaymentStatus.processing;
      case 'completed':
        return PaymentStatus.completed;
      case 'failed':
        return PaymentStatus.failed;
      default:
        return PaymentStatus.pending;
    }
  }
}

class PaymentsController extends GetxController {
  final SupabaseClient _supabaseClient = Supabase.instance.client;
  final RxList<Payment> payments = <Payment>[].obs;
  final RxBool isLoading = false.obs;
  final RxString error = ''.obs;
  final Rx<PaymentStatus> selectedStatus = PaymentStatus.pending.obs;

  // Wallet-related properties
  final Rx<Map<String, dynamic>?> walletData = Rx<Map<String, dynamic>?>(null);
  final RxList<Map<String, dynamic>> transactions =
      RxList<Map<String, dynamic>>([]);
  final RxList<Map<String, dynamic>> paymentMethods =
      RxList<Map<String, dynamic>>([]);

  // Payment stats
  final RxDouble totalPayments = 0.0.obs;
  final RxDouble pendingPayments = 0.0.obs;
  final RxDouble monthlyPayments = 0.0.obs;

  // Wallet ID (cache this for faster access)
  String? _walletId;

  @override
  void onInit() {
    super.onInit();
    fetchWalletData().then((_) {
      fetchPayments();
      fetchTransactions();
      fetchPaymentMethods();
    });
  }

  // Fetch payments from database
  // Assumption: You have a payments table in your Supabase DB
  // If not, this will need to be modified to match your actual data structure
  Future<void> fetchPayments() async {
    isLoading.value = true;
    error.value = '';

    try {
      // Try to fetch from the payments table
      // Adjust the table name and structure as needed
      final response = await _supabaseClient
          .from('payments') // Change this to your actual table name
          .select(
            '*, worker:workers(name)',
          ) // Assuming you have a workers table with worker profiles
          .order('due_date', ascending: true);

      // If the database fetch works, use that data
      if (response != null && response is List) {
        List<Payment> fetchedPayments = [];

        for (var row in response) {
          fetchedPayments.add(
            Payment(
              id: row['id'],
              workerName: row['worker']['name'] ?? 'Unknown Worker',
              workerId: row['worker_id'] ?? '',
              amount:
                  (row['amount'] is int)
                      ? (row['amount'] as int).toDouble()
                      : row['amount'] as double,
              dueDate: DateTime.parse(row['due_date']),
              status: PaymentStatusExtension.fromString(
                row['status'] ?? 'pending',
              ),
              jobReference: row['job_reference'],
              hoursWorked:
                  row['hours_worked'] != null
                      ? (row['hours_worked'] is int)
                          ? (row['hours_worked'] as int).toDouble()
                          : row['hours_worked'] as double
                      : null,
              paymentMethod: row['payment_method'],
              transactionId: row['transaction_id'],
            ),
          );
        }

        payments.value = fetchedPayments;
      } else {
        // If database fetch doesn't work, fall back to mock data
        payments.value = _getMockPayments();
      }

      // Calculate payment stats
      calculatePaymentStats();
    } catch (e) {
      print('Error fetching payments: $e');
      // Fall back to mock data if there's an error
      payments.value = _getMockPayments();
      calculatePaymentStats();
    } finally {
      isLoading.value = false;
    }
  }

  // Mock payments as a fallback if the database table isn't set up yet
  List<Payment> _getMockPayments() {
    return [
      Payment(
        id: 'PAY-001',
        workerName: 'Sarah Johnson',
        workerId: 'W-12345',
        amount: 450.00,
        dueDate: DateTime(2025, 5, 15),
        status: PaymentStatus.pending,
        jobReference: 'JOB-123',
        hoursWorked: 30.0,
        paymentMethod: 'Bank Transfer',
      ),
      Payment(
        id: 'PAY-002',
        workerName: 'Michael Chen',
        workerId: 'W-67890',
        amount: 720.00,
        dueDate: DateTime(2025, 5, 17),
        status: PaymentStatus.pending,
        jobReference: 'JOB-124',
        hoursWorked: 48.0,
        paymentMethod: 'Bank Transfer',
      ),
      Payment(
        id: 'PAY-003',
        workerName: 'Emily Rodriguez',
        workerId: 'W-54321',
        amount: 325.50,
        dueDate: DateTime(2025, 5, 20),
        status: PaymentStatus.pending,
        jobReference: 'JOB-125',
        hoursWorked: 21.7,
        paymentMethod: 'PayPal',
      ),
    ];
  }

  // Get payments by status
  List<Payment> getPaymentsByStatus(PaymentStatus status) {
    return payments.where((payment) => payment.status == status).toList();
  }

  // Change selected status
  void changeStatus(PaymentStatus status) {
    selectedStatus.value = status;
  }

  // Calculate payment stats
  void calculatePaymentStats() {
    totalPayments.value = payments.fold(
      0,
      (sum, payment) => sum + payment.amount,
    );
    pendingPayments.value = getPaymentsByStatus(
      PaymentStatus.pending,
    ).fold(0, (sum, payment) => sum + payment.amount);

    final now = DateTime.now();
    final startOfMonth = DateTime(now.year, now.month, 1);
    final endOfMonth = DateTime(now.year, now.month + 1, 0);

    monthlyPayments.value = payments
        .where(
          (payment) =>
              payment.dueDate.isAfter(startOfMonth) &&
              payment.dueDate.isBefore(endOfMonth),
        )
        .fold(0, (sum, payment) => sum + payment.amount);
  }

  // Format currency
  String formatCurrency(double amount) {
    return NumberFormat.currency(
      locale: 'en_IN',
      symbol: '₹',
      decimalDigits: 2,
    ).format(amount);
  }

  // Format date
  String formatDate(DateTime date) {
    return 'Due: ${date.day} ${_getMonthName(date.month)}, ${date.year}';
  }

  String _getMonthName(int month) {
    const months = [
      'Jan',
      'Feb',
      'Mar',
      'Apr',
      'May',
      'Jun',
      'Jul',
      'Aug',
      'Sep',
      'Oct',
      'Nov',
      'Dec',
    ];
    return months[month - 1];
  }

  Future<bool> createInitialWalletIfNotExists() async {
    final currentUser = _supabaseClient.auth.currentUser;
    if (currentUser == null) {
      error.value = 'User not authenticated';
      return false;
    }

    try {
      final existingWallet =
          await _supabaseClient
              .from('employer_wallet')
              .select()
              .eq('employer_id', currentUser.id)
              .maybeSingle();

      if (existingWallet != null) {
        return false; // Already exists
      }

      final newWallet =
          await _supabaseClient
              .from('employer_wallet')
              .insert({
                'employer_id': currentUser.id,
                'balance': 500,
                'currency': 'INR',
                'created_at': DateTime.now().toIso8601String(),
                'last_updated': DateTime.now().toIso8601String(),
              })
              .select()
              .single();

      walletData.value = newWallet;
      return true;
    } catch (e) {
      error.value = 'Failed to create wallet: $e';
      return false;
    }
  }

  Future<void> fetchWalletData() async {
    isLoading.value = true;
    error.value = '';

    final currentUser = Supabase.instance.client.auth.currentUser;

    print('Current User: $currentUser');

    if (currentUser == null) {
      error.value = 'User not authenticated';
      isLoading.value = false;
      return;
    }

    try {
      final response =
          await _supabaseClient
              .from('employer_wallet')
              .select()
              .eq('employer_id', currentUser.id)
              .single();

      walletData.value = response;
      _walletId = response['id']; // Cache the wallet ID
    } catch (e) {
      error.value = 'Failed to load wallet data: $e';
      walletData.value = null;
      _walletId = null;
      print(error.value);
    } finally {
      isLoading.value = false;
    }
  }

  // Fetch transactions with optional filter
  // Fixed fetchTransactions method
  Future<void> fetchTransactions({String? status, int limit = 20}) async {
    if (_walletId == null) {
      await fetchWalletData();
      if (_walletId == null) {
        error.value = 'No wallet found';
        return;
      }
    }

    isLoading.value = true;
    error.value = '';

    try {
      // Create a filter query object with explicitly typed Map<String, Object>
      Map<String, Object> filterObj = {'wallet_id': _walletId!};

      // Add status filter if provided
      if (status != null) {
        filterObj['status'] = status;
      }

      // Execute the query with the filter
      final response = await _supabaseClient
          .from('wallet_transactions')
          .select()
          .match(filterObj) // Now this will accept the Map<String, Object>
          .order('created_at', ascending: false)
          .limit(limit);

      if (response != null) {
        transactions.value = List<Map<String, dynamic>>.from(response);
      }
    } catch (e) {
      error.value = 'Failed to load transactions: $e';
      print(error.value);
    } finally {
      isLoading.value = false;
    }
  }

  Future<bool> setDefaultPaymentMethod(String paymentMethodId) async {
    try {
      // First, set all payment methods to non-default
      await _supabaseClient
          .from('payment_methods')
          .update({'is_default': false})
          .match({}); // Match all records

      // Then set the selected payment method as default
      await _supabaseClient
          .from('payment_methods')
          .update({'is_default': true})
          .match({'id': paymentMethodId}); // Use match instead of eq

      // Refresh payment methods
      await fetchPaymentMethods();
      return true;
    } catch (e) {
      error.value = 'Error setting default payment method: $e';
      print(error.value);
      return false;
    }
  }

  // Fix the payment method delete query
  Future<bool> deletePaymentMethod(String paymentMethodId) async {
    try {
      await _supabaseClient.from('payment_methods').delete().match({
        'id': paymentMethodId,
      }); // Use match instead of eq

      // Refresh payment methods
      await fetchPaymentMethods();
      return true;
    } catch (e) {
      error.value = 'Error deleting payment method: $e';
      print(error.value);
      return false;
    }
  }

  // Fetch payment methods
  Future<void> fetchPaymentMethods() async {
    isLoading.value = true;
    error.value = '';

    try {
      final response = await _supabaseClient
          .from('payment_methods')
          .select()
          .order('is_default', ascending: false);

      if (response != null) {
        paymentMethods.value = List<Map<String, dynamic>>.from(response);
      }
    } catch (e) {
      error.value = 'Failed to load payment methods: $e';
      print(error.value);
    } finally {
      isLoading.value = false;
    }
  }

  // Process payment
  Future<bool> processPayment(String paymentId) async {
    try {
      // Get the payment details
      final payment = payments.firstWhere((p) => p.id == paymentId);

      // Check if wallet has sufficient balance
      if (walletData.value == null ||
          walletData.value!['balance'] < payment.amount) {
        error.value = 'Insufficient balance in wallet';
        return false;
      }

      // Use the stored procedure to process payment
      try {
        final result = await _supabaseClient.rpc(
          'process_payment_from_wallet',
          params: {
            'wallet_id': _walletId,
            'amount': payment.amount,
            'description':
                'Payment to ${payment.workerName} for ${payment.jobReference ?? "services"}',
            'recipient_id': payment.workerId,
          },
        );

        // Payment was successful, update the local payment status
        final index = payments.indexWhere((p) => p.id == paymentId);
        if (index >= 0) {
          final updatedPayment = Payment(
            id: payment.id,
            workerName: payment.workerName,
            workerId: payment.workerId,
            amount: payment.amount,
            dueDate: payment.dueDate,
            status: PaymentStatus.processing, // Set to processing initially
            jobReference: payment.jobReference,
            hoursWorked: payment.hoursWorked,
            paymentMethod: payment.paymentMethod,
            transactionId:
                result != null && result['id'] != null
                    ? result['id']
                    : 'TRX-${DateTime.now().millisecondsSinceEpoch}',
          );

          payments[index] = updatedPayment;
          payments.refresh();

          // Update the payment record in the database if available
          try {
            await _supabaseClient
                .from('payments') // Change to your actual table name
                .update({
                  'status': 'processing',
                  'transaction_id':
                      result != null && result['id'] != null
                          ? result['id']
                          : 'TRX-${DateTime.now().millisecondsSinceEpoch}',
                })
                .eq('id', paymentId);
          } catch (e) {
            print('Error updating payment status: $e');
            // Continue even if this fails
          }

          // Refresh wallet data and transactions
          await fetchWalletData();
          await fetchTransactions();

          // Recalculate payment stats
          calculatePaymentStats();

          return true;
        }

        return false;
      } catch (e) {
        // If the RPC call fails (e.g., function doesn't exist yet), do the update locally
        print('RPC error: $e, fallback to local update');
        return _processPaymentLocally(payment);
      }
    } catch (e) {
      error.value = 'Error processing payment: $e';
      print(error.value);
      return false;
    }
  }

  // Process payment locally as a fallback
  Future<bool> _processPaymentLocally(Payment payment) async {
    try {
      if (walletData.value == null) {
        error.value = 'No wallet found';
        return false;
      }

      // Check if wallet has sufficient balance
      final balance = walletData.value!['balance'];
      final numBalance =
          balance is int ? balance.toDouble() : balance as double;

      if (numBalance < payment.amount) {
        error.value = 'Insufficient balance';
        return false;
      }

      // Update the wallet balance locally
      walletData.value = {
        ...walletData.value!,
        'balance': numBalance - payment.amount,
        'last_updated': DateTime.now().toIso8601String(),
      };

      // Create a mock transaction
      final transactionId = 'TRX-${DateTime.now().millisecondsSinceEpoch}';
      transactions.insert(0, {
        'id': transactionId,
        'wallet_id': _walletId,
        'amount': -payment.amount,
        'transaction_type': 'payment',
        'description':
            'Payment to ${payment.workerName} for ${payment.jobReference ?? "services"}',
        'status': 'completed',
        'created_at': DateTime.now().toIso8601String(),
      });

      // Update the payment status
      final index = payments.indexWhere((p) => p.id == payment.id);
      if (index >= 0) {
        final updatedPayment = Payment(
          id: payment.id,
          workerName: payment.workerName,
          workerId: payment.workerId,
          amount: payment.amount,
          dueDate: payment.dueDate,
          status: PaymentStatus.processing,
          jobReference: payment.jobReference,
          hoursWorked: payment.hoursWorked,
          paymentMethod: payment.paymentMethod,
          transactionId: transactionId,
        );

        payments[index] = updatedPayment;
        payments.refresh();

        // Recalculate payment stats
        calculatePaymentStats();

        return true;
      }

      return false;
    } catch (e) {
      error.value = 'Error processing payment locally: $e';
      print(error.value);
      return false;
    }
  }

  Future<bool> addFundsToWallet(double amount, String paymentMethodId) async {
    try {
      // First, check if a wallet exists for the current employer
      final currentUser = Supabase.instance.client.auth.currentUser;
      if (currentUser == null) {
        error.value = 'User not authenticated';
        return false;
      }

      // Try to find existing wallet or create a new one if it doesn't exist
      Map<String, dynamic>? existingWallet;
      try {
        existingWallet =
            await Supabase.instance.client
                .from('employer_wallet')
                .select()
                .eq('employer_id', currentUser.id)
                .single();
      } catch (e) {
        // No existing wallet found, create a new one
        existingWallet = null;
      }

      if (existingWallet == null) {
        // Create new wallet
        final newWalletResponse =
            await Supabase.instance.client
                .from('employer_wallet')
                .insert({
                  'employer_id': currentUser.id,
                  'balance': amount,
                  'currency': 'INR', // Default to Indian Rupee
                  'last_updated': DateTime.now().toIso8601String(),
                  'created_at': DateTime.now().toIso8601String(),
                })
                .select()
                .single();

        // Update local wallet data
        walletData.value = newWalletResponse;
        _walletId = newWalletResponse['id'];
      } else {
        // Update existing wallet balance
        final currentBalance =
            existingWallet['balance'] is int
                ? (existingWallet['balance'] as int).toDouble()
                : (existingWallet['balance'] as double);

        final updatedWalletResponse =
            await Supabase.instance.client
                .from('employer_wallet')
                .update({
                  'balance': currentBalance + amount,
                  'last_updated': DateTime.now().toIso8601String(),
                })
                .eq('id', existingWallet['id'])
                .select()
                .single();

        // Update local wallet data
        walletData.value = updatedWalletResponse;
        _walletId = updatedWalletResponse['id'];
      }

      // Record the transaction - remove payment_method_id column
      await Supabase.instance.client.from('wallet_transactions').insert({
        'wallet_id': _walletId,
        'amount': amount,
        'transaction_type': 'deposit',
        'description': 'Funds added via app',
        'status': 'completed',
        'created_at': DateTime.now().toIso8601String(),
        // Remove payment_method_id column
      });

      // Refresh transactions
      await fetchTransactions();

      return true;
    } catch (e) {
      error.value = 'Error adding funds: $e';
      print(error.value);
      return false;
    }
  }

  // Also update the fetchTransactions method to remove reference to payment_method_id
  // Add funds locally as a fallback
  Future<bool> _addFundsLocally(double amount) async {
    try {
      if (walletData.value == null) {
        error.value = 'No wallet found';
        return false;
      }

      // Update the wallet balance locally
      final balance = walletData.value!['balance'];
      final numBalance =
          balance is int ? balance.toDouble() : balance as double;

      walletData.value = {
        ...walletData.value!,
        'balance': numBalance + amount,
        'last_updated': DateTime.now().toIso8601String(),
      };

      // Create a mock transaction
      transactions.insert(0, {
        'id': 'mock-${DateTime.now().millisecondsSinceEpoch}',
        'wallet_id': _walletId,
        'amount': amount,
        'transaction_type': 'deposit',
        'description': 'Funds added via app',
        'status': 'completed',
        'created_at': DateTime.now().toIso8601String(),
      });

      return true;
    } catch (e) {
      error.value = 'Error adding funds locally: $e';
      print(error.value);
      return false;
    }
  }

  // Withdraw funds from wallet
  Future<bool> withdrawFunds(double amount, String paymentMethodId) async {
    if (_walletId == null) {
      error.value = 'No wallet found';
      return false;
    }

    try {
      // Check for sufficient balance
      if (walletData.value == null) {
        error.value = 'No wallet found';
        return false;
      }

      final balance = walletData.value!['balance'];
      final numBalance =
          balance is int ? balance.toDouble() : balance as double;

      if (numBalance < amount) {
        error.value = 'Insufficient balance';
        return false;
      }

      // Try to use the stored procedure
      try {
        await _supabaseClient.rpc(
          'withdraw_funds_from_wallet',
          params: {
            'wallet_id': _walletId,
            'amount': amount,
            'description': 'Withdrawal via app',
            'payment_method_id': paymentMethodId,
          },
        );

        // Refresh wallet data and transactions
        await fetchWalletData();
        await fetchTransactions();
        return true;
      } catch (e) {
        // If the RPC call fails, do the update locally
        print('RPC error: $e, fallback to local update');
        return _withdrawFundsLocally(amount);
      }
    } catch (e) {
      error.value = 'Error withdrawing funds: $e';
      print(error.value);
      return false;
    }
  }

  // Withdraw funds locally as a fallback
  Future<bool> _withdrawFundsLocally(double amount) async {
    try {
      if (walletData.value == null) {
        error.value = 'No wallet found';
        return false;
      }

      // Check for sufficient balance
      final balance = walletData.value!['balance'];
      final numBalance =
          balance is int ? balance.toDouble() : balance as double;

      if (numBalance < amount) {
        error.value = 'Insufficient balance';
        return false;
      }

      // Update the wallet balance locally
      walletData.value = {
        ...walletData.value!,
        'balance': numBalance - amount,
        'last_updated': DateTime.now().toIso8601String(),
      };

      // Create a mock transaction
      transactions.insert(0, {
        'id': 'mock-${DateTime.now().millisecondsSinceEpoch}',
        'wallet_id': _walletId,
        'amount': -amount,
        'transaction_type': 'withdrawal',
        'description': 'Withdrawal via app',
        'status': 'completed',
        'created_at': DateTime.now().toIso8601String(),
      });

      return true;
    } catch (e) {
      error.value = 'Error withdrawing funds locally: $e';
      print(error.value);
      return false;
    }
  }

  // Add payment method
  Future<bool> addPaymentMethod({
    required String methodType,
    required String displayName,
    required String lastFour,
    String? expiryDate,
    required String provider,
    bool isDefault = false,
  }) async {
    try {
      // If this is set as default, update all other payment methods
      if (isDefault) {
        await _supabaseClient.from('payment_methods').update({
          'is_default': false,
        });
      }

      await _supabaseClient.from('payment_methods').insert({
        'method_type': methodType,
        'display_name': displayName,
        'last_four': lastFour,
        'expiry_date': expiryDate,
        'provider': provider,
        'is_default': isDefault,
      });

      // Refresh payment methods
      await fetchPaymentMethods();
      return true;
    } catch (e) {
      error.value = 'Error adding payment method: $e';
      print(error.value);
      return false;
    }
  }

  // Get formatted wallet balance
  String getFormattedWalletBalance() {
    if (walletData.value == null) return '₹0.00';

    final balance = walletData.value!['balance'];
    final numBalance = balance is int ? balance.toDouble() : balance as double;

    return formatCurrency(numBalance);
  }

  // Check if wallet exists
  bool hasWallet() {
    return walletData.value != null;
  }

  // Check if payment methods exist
  bool hasPaymentMethods() {
    return paymentMethods.isNotEmpty;
  }

  // Get default payment method id
  String? getDefaultPaymentMethodId() {
    final defaultMethod = paymentMethods.firstWhereOrNull(
      (method) => method['is_default'] == true,
    );
    return defaultMethod?['id'];
  }

  // Create a new payment (for future implementation)
  Future<bool> createPayment({
    required String workerName,
    required String workerId,
    required double amount,
    required DateTime dueDate,
    String? jobReference,
    double? hoursWorked,
    String? paymentMethod,
  }) async {
    try {
      // This assumes you have a payments table
      // Adjust as needed for your actual database schema
      await _supabaseClient.from('payments').insert({
        'worker_id': workerId,
        'amount': amount,
        'due_date': dueDate.toIso8601String(),
        'status': 'pending',
        'job_reference': jobReference,
        'hours_worked': hoursWorked,
        'payment_method': paymentMethod,
      });

      // Refresh payments
      await fetchPayments();
      return true;
    } catch (e) {
      error.value = 'Error creating payment: $e';
      print(error.value);
      return false;
    }
  }
}
