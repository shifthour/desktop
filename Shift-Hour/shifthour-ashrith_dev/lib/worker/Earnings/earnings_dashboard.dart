import 'package:flutter/material.dart';
import 'package:get/get.dart';
import 'package:shifthour/worker/Earnings/cashgram_withdrawal_dialog.dart';
import 'package:shifthour/worker/const/Botton_Navigation.dart';
import 'package:shifthour/worker/const/Standard_Appbar.dart';
import 'package:uuid/uuid.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:intl/intl.dart';
import 'package:url_launcher/url_launcher.dart';

import 'cashfree_payouts.dart';

// Transfer ID Generator
class TransferIdGenerator {
  static String generate({String? prefix}) {
    final timestamp = DateTime.now().millisecondsSinceEpoch;
    final uuid = const Uuid().v4();
    return '${prefix ?? 'TRF'}_$timestamp\_${uuid.split('-').last}';
  }
}

class WorkerWalletController extends GetxController {
  final SupabaseClient _supabase = Supabase.instance.client;
  final RxBool isLoading = false.obs;
  final RxBool hasWallet = false.obs;
  final Rx<Map<String, dynamic>> walletData = Rx<Map<String, dynamic>>({});
  final RxDouble balance = 0.0.obs;
  final RxString currency = 'INR'.obs;
  final RxString walletId = ''.obs;
  final RxString error = ''.obs;
  final RxList<Map<String, dynamic>> transactions =
      <Map<String, dynamic>>[].obs;
  final RxBool isWithdrawing = false.obs;
  final RxString withdrawalError = ''.obs;
  final RxString withdrawalStatus = ''.obs;

  @override
  void onInit() {
    super.onInit();
    checkWalletExists();
  }

  @override
  void onClose() {
    // Clean up observables to prevent memory leaks
    isLoading.close();
    hasWallet.close();
    walletData.close();
    balance.close();
    currency.close();
    walletId.close();
    error.close();
    transactions.close();
    isWithdrawing.close();
    withdrawalError.close();
    withdrawalStatus.close();
    super.onClose();
  }

  // Method to withdraw funds via Cashgram
  Future<bool> withdrawViaCashgram({
    required double amount,
    required String name,
    required String email,
    required String phone,
  }) async {
    isWithdrawing.value = true;
    withdrawalError.value = '';
    withdrawalStatus.value = '';

    try {
      if (amount < 1) {
        withdrawalError.value = 'Minimum withdrawal amount is ₹1';
        return false;
      }

      if (amount > balance.value) {
        withdrawalError.value = 'Insufficient balance';
        return false;
      }

      // Validate inputs
      if (!CashfreePayoutsService.isValidEmail(email)) {
        withdrawalError.value = 'Invalid email format';
        return false;
      }

      if (!CashfreePayoutsService.isValidPhone(phone)) {
        withdrawalError.value =
            'Invalid phone number (must be 10 digits starting with 6-9)';
        return false;
      }

      final cashgramId = TransferIdGenerator.generate(prefix: 'CSG');

      // Use the new Cashfree API integration
      final cashgramResponse = await CashfreePayoutsService.createCashgram(
        cashgramId: cashgramId,
        name: name,
        email: email,
        phone: phone,
        amount: amount,
        remarks: "Wallet withdrawal",
      );

      if (cashgramResponse['status'] == 'SUCCESS') {
        // Update balance locally
        balance.value -= amount;

        // Record transaction in database
        await _recordCashgramTransaction(
          amount,
          cashgramId,
          cashgramResponse['cashgramLink'],
        );

        // ✅ Update balance in worker_wallet table
        await _supabase
            .from('worker_wallet')
            .update({
              'balance': balance.value,
              'last_updated': DateTime.now().toIso8601String(),
            })
            .eq('id', walletId.value);

        withdrawalStatus.value = 'Cashgram Created Successfully';
        await fetchTransactions();
        return true;
      } else {
        withdrawalError.value =
            cashgramResponse['message'] ?? 'Cashgram creation failed';
        return false;
      }
    } catch (e) {
      withdrawalError.value = 'An unexpected error occurred: ${e.toString()}';
      return false;
    } finally {
      isWithdrawing.value = false;
    }
  }

  // Method to record Cashgram transaction
  Future<void> _recordCashgramTransaction(
    double amount,
    String cashgramId,
    String? paymentLink,
  ) async {
    try {
      final userId = _supabase.auth.currentUser?.id;

      final uuid = const Uuid().v4();

      final result =
          await _supabase.from('worker_wallet_transactions').insert({
            'id': uuid, // ✅ set primary key manually
            'wallet_id': walletId.value,
            'amount': -amount,
            'transaction_type': 'cashgram',
            'status': 'initiated',
            'transfer_id': cashgramId,
            'payment_method': 'cashgram',
            'description': 'withdrawal',
            'payment_link': paymentLink,
            'created_at': DateTime.now().toIso8601String(),
          }).select();

      print("✅ Inserted cashgram transaction: $result");
    } catch (e) {
      print('❌ Error recording cashgram transaction: $e');
    }
  }

  Future<void> checkWalletExists() async {
    isLoading.value = true;
    error.value = '';
    try {
      final userId = _supabase.auth.currentUser?.id;
      if (userId == null) {
        throw Exception('User not authenticated');
      }

      final userEmail = _supabase.auth.currentUser?.email;
      if (userEmail != null) {
        final walletByEmail =
            await _supabase
                .from('worker_wallet')
                .select()
                .eq('worker_email', userEmail)
                .maybeSingle();

        if (walletByEmail != null) {
          _handleWalletData(walletByEmail);
          await fetchTransactions();
          isLoading.value = false;
          return;
        }
      }

      final walletResponse =
          await _supabase
              .from('worker_wallet')
              .select()
              .eq('worker_id', userId)
              .maybeSingle();

      if (walletResponse != null) {
        _handleWalletData(walletResponse);
        await fetchTransactions();
      } else {
        hasWallet.value = false;
        walletData.value = {};
        transactions.clear();
      }
    } catch (e) {
      print('Error checking wallet: $e');
      error.value = e.toString();
      hasWallet.value = false;
    } finally {
      isLoading.value = false;
    }
  }

  Future<void> fetchTransactions() async {
    if (!hasWallet.value || walletId.value.isEmpty) return;

    try {
      final response = await _supabase
          .from('worker_wallet_transactions')
          .select()
          .eq('wallet_id', walletId.value)
          .order('created_at', ascending: false);

      if (response != null) {
        transactions.value = List<Map<String, dynamic>>.from(response);
      } else {
        transactions.clear();
      }
    } catch (e) {
      print('Error fetching transactions: $e');
      transactions.clear();
    }
  }

  void _handleWalletData(Map<String, dynamic> data) {
    hasWallet.value = true;
    walletData.value = data;
    walletId.value = data['id']?.toString() ?? '';

    if (data['balance'] != null) {
      if (data['balance'] is int) {
        balance.value = (data['balance'] as int).toDouble();
      } else if (data['balance'] is double) {
        balance.value = data['balance'];
      } else if (data['balance'] is String) {
        balance.value = double.tryParse(data['balance']) ?? 0.0;
      }
    }

    currency.value = data['currency']?.toString() ?? 'INR';
  }

  Future<bool> createWallet() async {
    isLoading.value = true;
    error.value = '';
    try {
      final userId = _supabase.auth.currentUser?.id;
      final userEmail = _supabase.auth.currentUser?.email;

      if (userId == null) {
        throw Exception('User not authenticated');
      }

      if (userEmail == null) {
        throw Exception('User email not available');
      }

      final response =
          await _supabase.from('worker_wallet').insert({
            'worker_id': userId,
            'worker_email': userEmail,
            'balance': 0,
            'currency': 'INR',
            'last_updated': DateTime.now().toIso8601String(),
          }).select();

      if (response != null && response.isNotEmpty) {
        _handleWalletData(response[0]);
        await fetchTransactions();
        return true;
      }
      return false;
    } catch (e) {
      print('Error creating wallet: $e');
      error.value = e.toString();
      return false;
    } finally {
      isLoading.value = false;
    }
  }

  String getFormattedBalance() {
    return formatCurrency(balance.value);
  }

  String formatCurrency(double amount) {
    return NumberFormat.currency(
      locale: 'en_IN',
      symbol: '₹',
      decimalDigits: 2,
    ).format(amount);
  }
}

class WorkerWalletScreen extends StatefulWidget {
  const WorkerWalletScreen({Key? key}) : super(key: key);

  @override
  State<WorkerWalletScreen> createState() => _WorkerWalletScreenState();
}

class _WorkerWalletScreenState extends State<WorkerWalletScreen>
    with NavigationMixin {
  final ScrollController _scrollController = ScrollController();
  final WorkerWalletController _walletController = Get.put(
    WorkerWalletController(),
  );
  void initState() {
    super.initState();
    setCurrentTab(3); // Profile tab has index 3
    // setPageScrollController(_scrollController);
  }

  @override
  Widget build(BuildContext context) {
    final screenWidth = MediaQuery.of(context).size.width;
    final isSmallScreen = screenWidth < 600;

    return Scaffold(
      appBar: StandardAppBar(title: 'Wallet', centerTitle: true),

      bottomNavigationBar: const ShiftHourBottomNavigation(),

      body: Padding(
        padding: const EdgeInsets.all(16.0),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            // Header
            Text(
              'Wallet Overview',
              style: Theme.of(
                context,
              ).textTheme.headlineSmall?.copyWith(fontFamily: 'Inter Tight'),
            ),
            const SizedBox(height: 16),

            // Wallet Content
            Expanded(child: _buildWalletContent(context, isSmallScreen)),
          ],
        ),
      ),
    );
  }

  // Main wallet content
  Widget _buildWalletContent(BuildContext context, bool isSmallScreen) {
    return SingleChildScrollView(
      controller: _scrollController,
      child: Obx(() {
        if (_walletController.isLoading.value) {
          return Center(
            child: Padding(
              padding: const EdgeInsets.symmetric(vertical: 100.0),
              child: CircularProgressIndicator(),
            ),
          );
        }

        if (_walletController.error.value.isNotEmpty) {
          return _buildErrorState();
        }

        if (!_walletController.hasWallet.value) {
          return _buildNoWalletState();
        }

        return Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            // Wallet Balance Card
            _buildWalletBalanceCard(),
            SizedBox(height: 24),

            // Recent Transactions Section
            _buildTransactionsSection(),
          ],
        );
      }),
    );
  }

  Widget _buildTransactionsSection() {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(
          'Recent Transactions',
          style: TextStyle(
            fontSize: 18,
            fontWeight: FontWeight.bold,
            fontFamily: 'Inter Tight',
          ),
        ),
        SizedBox(height: 16),

        // Transaction Items
        if (_walletController.transactions.isEmpty)
          _buildNoTransactionsCard()
        else
          Column(
            children:
                _walletController.transactions
                    .take(5)
                    .map((transaction) => _buildTransactionItem(transaction))
                    .toList(),
          ),

        SizedBox(height: 24),

        // View All Transactions Button
        Center(
          child: TextButton(
            onPressed: () {
              // Refresh transactions before showing dialog
              _walletController.fetchTransactions();
              _showAllTransactionsDialog();
            },
            child: Text(
              'View All Transactions',
              style: TextStyle(
                color: Colors.blue.shade700,
                fontWeight: FontWeight.bold,
              ),
            ),
          ),
        ),
      ],
    );
  }

  // Error state
  Widget _buildErrorState() {
    return Center(
      child: Padding(
        padding: const EdgeInsets.symmetric(vertical: 100.0, horizontal: 24.0),
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            Icon(Icons.error_outline, size: 48, color: Colors.red.shade400),
            SizedBox(height: 16),
            Text(
              'Error Loading Wallet',
              style: TextStyle(fontSize: 20, fontWeight: FontWeight.bold),
            ),
            SizedBox(height: 8),
            Text(
              _walletController.error.value,
              textAlign: TextAlign.center,
              style: TextStyle(color: Colors.grey.shade700),
            ),
            SizedBox(height: 24),
            ElevatedButton.icon(
              onPressed: () => _walletController.checkWalletExists(),
              icon: Icon(Icons.refresh),
              label: Text('Retry'),
            ),
          ],
        ),
      ),
    );
  }

  // No wallet state
  Widget _buildNoWalletState() {
    return Center(
      child: Padding(
        padding: const EdgeInsets.symmetric(vertical: 100.0, horizontal: 24.0),
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            Icon(
              Icons.account_balance_wallet_outlined,
              size: 48,
              color: Colors.blue.shade400,
            ),
            SizedBox(height: 16),
            Text(
              'No Wallet Found',
              style: TextStyle(fontSize: 20, fontWeight: FontWeight.bold),
            ),
            SizedBox(height: 8),
            Text(
              'You need to create a wallet to manage your earnings.',
              textAlign: TextAlign.center,
              style: TextStyle(color: Colors.grey.shade700),
            ),
            SizedBox(height: 24),
            ElevatedButton.icon(
              onPressed: _handleCreateWallet,
              icon: Icon(Icons.add),
              label: Text('Create Wallet'),
            ),
          ],
        ),
      ),
    );
  }

  // Wallet Balance Card
  Widget _buildWalletBalanceCard() {
    return Container(
      width: double.infinity,
      padding: EdgeInsets.all(24),
      decoration: BoxDecoration(
        gradient: LinearGradient(
          colors: [Color(0xFF5B6BF8), Color(0xFF8B65D9)],
          begin: Alignment.topLeft,
          end: Alignment.bottomRight,
        ),
        borderRadius: BorderRadius.circular(16),
        boxShadow: [
          BoxShadow(
            color: Colors.blue.withOpacity(0.2),
            blurRadius: 10,
            offset: Offset(0, 4),
          ),
        ],
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            mainAxisAlignment: MainAxisAlignment.spaceBetween,
            children: [
              Row(
                children: [
                  Icon(
                    Icons.account_balance_wallet,
                    color: Colors.white,
                    size: 28,
                  ),
                  SizedBox(width: 12),
                  Text(
                    'Personal Wallet',
                    style: TextStyle(
                      color: Colors.white,
                      fontSize: 18,
                      fontWeight: FontWeight.bold,
                      fontFamily: 'Inter',
                    ),
                  ),
                ],
              ),
              IconButton(
                icon: Icon(Icons.refresh, color: Colors.white),
                onPressed: () => _walletController.checkWalletExists(),
                tooltip: 'Refresh wallet data',
              ),
            ],
          ),
          SizedBox(height: 24),
          Text(
            'Available Balance',
            style: TextStyle(
              color: Colors.white.withOpacity(0.8),
              fontSize: 14,
              fontFamily: 'Inter',
            ),
          ),
          SizedBox(height: 8),
          Text(
            _walletController.getFormattedBalance(),
            style: TextStyle(
              color: Colors.white,
              fontSize: 32,
              fontWeight: FontWeight.bold,
              fontFamily: 'Inter Tight',
            ),
          ),
          SizedBox(height: 24),
          Row(
            mainAxisAlignment: MainAxisAlignment.spaceBetween,
            children: [
              _buildWalletButton(
                icon: Icons.history,
                label: 'History',
                onPressed: () => _showAllTransactionsDialog(),
              ),
              _buildWalletButton(
                icon: Icons.arrow_outward,
                label: 'Withdraw',
                onPressed: () => _showWithdrawDialog(),
              ),
              _buildWalletButton(
                icon: Icons.info_outline,
                label: 'Help',
                onPressed: () => _showHelpDialog(),
              ),
            ],
          ),
        ],
      ),
    );
  }

  // No transactions card
  Widget _buildNoTransactionsCard() {
    return Container(
      width: double.infinity,
      padding: EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: Colors.grey.shade100,
        borderRadius: BorderRadius.circular(8),
        border: Border.all(color: Colors.grey.shade300),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Text(
            'No Transactions',
            style: TextStyle(fontSize: 16, fontWeight: FontWeight.bold),
          ),
          SizedBox(height: 8),
          Text(
            'Your transaction history will appear here.',
            style: TextStyle(color: Colors.grey.shade700, fontSize: 14),
          ),
        ],
      ),
    );
  }

  // Transaction item
  Widget _buildTransactionItem(Map<String, dynamic> transaction) {
    final amount = transaction['amount'] as num;

    final transactionType = transaction['transaction_type'] ?? '';

    // Force DEBIT transactions to be shown as negative
    final isCredit = transactionType == 'DEBIT' ? false : amount > 0;

    final formattedAmount =
        isCredit
            ? '+${_walletController.formatCurrency(amount.toDouble().abs())}'
            : '-${_walletController.formatCurrency(amount.toDouble().abs())}';

    final createdAt = DateTime.parse(transaction['created_at']);
    final formattedDate = DateFormat('MMM dd, yyyy').format(createdAt);

    String title =
        transaction['description'] ??
        transactionType.toString().capitalizeFirst!;

    // Get appropriate icon based on transaction type
    IconData transactionIcon;
    if (transactionType == 'deposit' || transactionType == 'earning') {
      transactionIcon = Icons.add_circle_outline;
    } else if (transactionType == 'pending') {
      transactionIcon = Icons.hourglass_top_outlined;
    } else if (transactionType == 'release' || transactionType == 'refund') {
      transactionIcon = Icons.undo_outlined;
    } else if (transactionType == 'fee') {
      transactionIcon = Icons.money_off_outlined;
    } else if (transactionType == 'withdrawal') {
      transactionIcon = Icons.arrow_outward;
    } else if (transactionType == 'cashgram') {
      transactionIcon = Icons.money;
    } else if (transactionType == 'DEBIT') {
      transactionIcon = Icons.money_off_outlined;
    } else {
      transactionIcon =
          isCredit ? Icons.add_circle_outline : Icons.remove_circle_outline;
    }

    // Get color based on transaction type
    Color iconColor;
    if (transactionType == 'pending') {
      iconColor = Colors.amber.shade700;
    } else if (transactionType == 'fee') {
      iconColor = Colors.red.shade700;
    } else {
      iconColor = isCredit ? Colors.green.shade700 : Colors.red.shade700;
    }

    // Cashgram Link
    Widget? cashgramLink;
    if (transactionType == 'cashgram' && transaction['payment_link'] != null) {
      cashgramLink = GestureDetector(
        onTap: () => launch(transaction['payment_link']),
        child: Text(
          'View Cashgram',
          style: TextStyle(
            color: Colors.blue,
            fontWeight: FontWeight.bold,
            fontSize: 14,
          ),
        ),
      );
    }

    return Container(
      margin: EdgeInsets.only(bottom: 12),
      padding: EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(8),
        border: Border.all(color: Colors.grey.shade300),
        boxShadow: [
          BoxShadow(
            color: Colors.black.withOpacity(0.05),
            blurRadius: 5,
            offset: Offset(0, 2),
          ),
        ],
      ),
      child: Row(
        children: [
          Container(
            padding: EdgeInsets.all(8),
            decoration: BoxDecoration(
              color: iconColor.withOpacity(0.1),
              borderRadius: BorderRadius.circular(8),
            ),
            child: Icon(transactionIcon, color: iconColor, size: 24),
          ),
          SizedBox(width: 16),
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(
                  title,
                  style: TextStyle(fontWeight: FontWeight.bold, fontSize: 16),
                ),
                Text(
                  formattedDate,
                  style: TextStyle(color: Colors.grey.shade600, fontSize: 14),
                ),
                if (cashgramLink != null) ...[
                  SizedBox(height: 8),
                  cashgramLink,
                ],
              ],
            ),
          ),
          Text(
            formattedAmount,
            style: TextStyle(
              fontWeight: FontWeight.bold,
              fontSize: 16,
              color: iconColor,
            ),
          ),
        ],
      ),
    );
  }

  // Wallet action button
  Widget _buildWalletButton({
    required IconData icon,
    required String label,
    required VoidCallback onPressed,
  }) {
    return Material(
      color: Colors.transparent,
      child: InkWell(
        onTap: onPressed,
        borderRadius: BorderRadius.circular(8),
        child: Container(
          padding: EdgeInsets.symmetric(vertical: 8, horizontal: 12),
          child: Column(
            children: [
              Container(
                padding: EdgeInsets.all(8),
                decoration: BoxDecoration(
                  color: Colors.white.withOpacity(0.2),
                  borderRadius: BorderRadius.circular(8),
                ),
                child: Icon(icon, color: Colors.white, size: 18),
              ),
              SizedBox(height: 8),
              Text(
                label,
                style: TextStyle(
                  color: Colors.white,
                  fontSize: 12,
                  fontFamily: 'Inter',
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }

  // Show all transactions dialog
  void _showAllTransactionsDialog() {
    Get.dialog(
      Dialog(
        child: Container(
          width: double.infinity,
          padding: EdgeInsets.all(16),
          child: SingleChildScrollView(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              mainAxisSize: MainAxisSize.min,
              children: [
                Row(
                  children: [
                    Text(
                      'Transaction History',
                      style: TextStyle(
                        fontSize: 18,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                    Spacer(),
                    IconButton(
                      icon: Icon(Icons.close),
                      onPressed: () => Get.back(),
                    ),
                  ],
                ),
                SizedBox(height: 16),
                if (_walletController.transactions.isEmpty)
                  _buildNoTransactionsCard()
                else
                  Container(
                    constraints: BoxConstraints(maxHeight: 500),
                    child: ListView.builder(
                      shrinkWrap: true,
                      itemCount: _walletController.transactions.length,
                      itemBuilder: (context, index) {
                        return _buildTransactionItem(
                          _walletController.transactions[index],
                        );
                      },
                    ),
                  ),
              ],
            ),
          ),
        ),
      ),
    );
  }

  void _showWithdrawDialog() {
    final walletBalance = _walletController.balance.value;

    if (walletBalance <= 0) {
      Get.snackbar(
        'Insufficient Balance',
        'You need to have funds in your wallet to withdraw.',
        snackPosition: SnackPosition.BOTTOM,
      );
      return;
    }

    if (walletBalance < 1) {
      Get.snackbar(
        'Minimum Withdrawal',
        'The minimum withdrawal amount is ₹1.',
        snackPosition: SnackPosition.BOTTOM,
      );
      return;
    }

    // Show withdrawal dialog
    Get.dialog(
      CashgramWithdrawalDialog(controller: _walletController),
      barrierDismissible: false, // Prevent closing by tapping outside
    );
  }

  // Show help dialog
  void _showHelpDialog() {
    Get.dialog(
      Dialog(
        shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
        child: Container(
          padding: EdgeInsets.all(24),
          child: Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Row(
                children: [
                  Icon(Icons.help_outline, color: Colors.blue),
                  SizedBox(width: 12),
                  Text(
                    'Wallet Help',
                    style: TextStyle(fontSize: 20, fontWeight: FontWeight.bold),
                  ),
                  Spacer(),
                  IconButton(
                    icon: Icon(Icons.close),
                    onPressed: () => Get.back(),
                  ),
                ],
              ),
              SizedBox(height: 16),
              Text(
                'About Your Wallet',
                style: TextStyle(fontSize: 16, fontWeight: FontWeight.bold),
              ),
              SizedBox(height: 8),
              Text(
                'Your wallet automatically receives payments for completed shifts. The funds will be available for withdrawal after the shift has been completed and approved by the employer.',
              ),
              SizedBox(height: 16),
              Text(
                'Withdrawals',
                style: TextStyle(fontSize: 16, fontWeight: FontWeight.bold),
              ),
              SizedBox(height: 8),
              Text(
                'To withdraw funds, you need to have a minimum balance of ₹1. Withdrawals are processed within 1-3 business days.',
              ),
              SizedBox(height: 24),
              Align(
                alignment: Alignment.center,
                child: ElevatedButton(
                  onPressed: () => Get.back(),
                  child: Text('Got it'),
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }

  // Handle creating wallet
  void _handleCreateWallet() async {
    if (_walletController.hasWallet.value) {
      Get.snackbar(
        'Wallet Exists',
        'You already have a wallet.',
        snackPosition: SnackPosition.BOTTOM,
        backgroundColor: Colors.blue.shade100,
        colorText: Colors.blue.shade800,
      );
      return;
    }

    final created = await _walletController.createWallet();

    if (created) {
      Get.snackbar(
        'Wallet Created',
        'Your wallet has been successfully created. You will receive payments for completed shifts here.',
        snackPosition: SnackPosition.BOTTOM,
        backgroundColor: Colors.green.shade100,
        colorText: Colors.green.shade800,
      );
    } else {
      Get.snackbar(
        'Error',
        _walletController.error.value.isEmpty
            ? 'Failed to create wallet. Please try again.'
            : _walletController.error.value,
        snackPosition: SnackPosition.BOTTOM,
        backgroundColor: Colors.red.shade100,
        colorText: Colors.red.shade700,
      );
    }
  }
}
