import 'package:flutter/material.dart';
import 'package:get/get.dart';
import 'package:intl/intl.dart';
import 'package:shifthour_employeer/Employer/payments/cashfree_payments.dart';
import 'package:shifthour_employeer/const/Bottom_Navigation.dart';
import 'package:shifthour_employeer/const/Standard_Appbar.dart';

class WalletScreen extends StatefulWidget {
  const WalletScreen({Key? key}) : super(key: key);

  @override
  State<WalletScreen> createState() => _WalletScreenState();
}

class _WalletScreenState extends State<WalletScreen> {
  final ScrollController _scrollController = ScrollController();
  final CashfreeWalletController _walletController = Get.put(
    CashfreeWalletController(),
  );

  @override
  void initState() {
    super.initState();

    // Initialize navigation controller
    WidgetsBinding.instance.addPostFrameCallback((_) {
      try {
        final navigationController = Get.find<NavigationController>();
        navigationController.setScrollController(_scrollController);
      } catch (e) {
        final navigationController = Get.put(
          NavigationController(),
          permanent: true,
        );
        navigationController.setScrollController(_scrollController);
      }
    });
  }

  @override
  Widget build(BuildContext context) {
    final screenWidth = MediaQuery.of(context).size.width;
    final isSmallScreen = screenWidth < 600;

    return Scaffold(
      appBar: StandardAppBar(
        title: 'Wallet',
        centerTitle: false,
        actions: [
          IconButton(
            icon: Icon(Icons.account_balance_wallet_outlined),
            tooltip: 'Create Wallet',
            onPressed: _handleCreateWallet,
            color: Colors.black,
          ),
        ],
      ),

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
          return Center(child: _buildNoWalletState());
        }

        if (_walletController.error.value.isNotEmpty) {
          return _buildErrorState();
        }

        if (_walletController.employerWallet.value == null) {
          return _buildNoWalletState();
        }
        // In _buildWalletContent, update your return Column like this:
        return Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            // Wallet Balance Card
            _buildWalletBalanceCard(),
            SizedBox(height: 24),

            // Pending Holds Section
            _buildPendingHoldsSection(),
            SizedBox(height: 24),

            // Recent Transactions Section - KEEP ONLY ONE OF THESE TWO OPTIONS:
            // OPTION 1: Use the dedicated section builder
            _buildTransactionsSection(),

            // OPTION 2: OR use the inline transactions list (remove this if using option 1)
            /*
    Text(
      'Recent Transactions',
      style: TextStyle(
        fontSize: 18,
        fontWeight: FontWeight.bold,
        fontFamily: 'Inter Tight',
      ),
    ),
    SizedBox(height: 16),
    _walletController.transactions.isEmpty
        ? _buildNoTransactionsCard()
        : Column(
            children: _walletController.transactions
              .take(3)
              .map((transaction) => _buildTransactionItem(transaction))
              .toList(),
          ),
    */

            // Add test buttons
          ],
        );
      }),
    );
  }

  Widget _buildTransactionsSection() {
    print(
      'Building transactions section. Count: ${_walletController.transactions.length}',
    );

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

        // Transaction Items with debugging print statements
        if (_walletController.transactions.isEmpty)
          _buildNoTransactionsCard()
        else
          Column(
            children:
                _walletController.transactions
                    .take(5) // Show more transactions for testing
                    .map((transaction) {
                      print(
                        'Rendering transaction: ${transaction['id']} - ${transaction['transaction_type']} - ${transaction['amount']}',
                      );
                      return _buildTransactionItem(transaction);
                    })
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
              onPressed: () => _walletController.fetchEmployerWallet(),
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
    // Trigger a refresh automatically after a small delay
    Future.delayed(Duration(minutes: 10), () {
      _walletController.fetchEmployerWallet();
    });

    return Center(
      child: Padding(
        padding: const EdgeInsets.all(24.0),
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            // Dummy loading skeleton box
            Container(
              height: 120,
              width: double.infinity,
              decoration: BoxDecoration(
                color: Colors.grey.shade200,
                borderRadius: BorderRadius.circular(16),
              ),
            ),
            const SizedBox(height: 16),
            Container(
              height: 20,
              width: 150,
              decoration: BoxDecoration(
                color: Colors.grey.shade300,
                borderRadius: BorderRadius.circular(8),
              ),
            ),
            const SizedBox(height: 8),
            Container(
              height: 20,
              width: 200,
              decoration: BoxDecoration(
                color: Colors.grey.shade300,
                borderRadius: BorderRadius.circular(8),
              ),
            ),
            const SizedBox(height: 32),
            CircularProgressIndicator(),
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
                    'Business Wallet',
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
                onPressed: () => _walletController.fetchEmployerWallet(),
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
            _walletController.getFormattedWalletBalance(),
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
                icon: Icons.add,
                label: 'Add Funds',
                onPressed: () => _showAddFundsDialog(),
              ),

              _buildWalletButton(
                icon: Icons.history,
                label: 'History',
                onPressed: () => _showAllTransactionsDialog(),
              ),
            ],
          ),
        ],
      ),
    );
  }

  // Pending Holds Section
  Widget _buildPendingHoldsSection() {
    final pendingHolds =
        _walletController.holdTransactions
            .where((hold) => hold['status'] == 'held')
            .toList();

    if (pendingHolds.isEmpty) {
      return SizedBox.shrink();
    }

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(
          'Pending Holds',
          style: TextStyle(
            fontSize: 18,
            fontWeight: FontWeight.bold,
            fontFamily: 'Inter Tight',
          ),
        ),
        SizedBox(height: 16),
        Container(
          padding: EdgeInsets.all(16),

          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text(
                'Funds currently on hold for pending shifts',
                style: TextStyle(
                  color: Colors.amber.shade800,
                  fontWeight: FontWeight.w500,
                ),
              ),
              SizedBox(height: 12),
              ...pendingHolds.map((hold) {
                final shiftData = hold['shift'] ?? {};
                final shiftTitle = shiftData['title'] ?? 'Shift';
                final amount = hold['amount'] as num;
                final formattedAmount = _walletController.formatCurrency(
                  amount.toDouble(),
                );
                final createdAt = DateTime.parse(hold['created_at']);
                final formattedDate = DateFormat(
                  'MMM dd, yyyy',
                ).format(createdAt);

                return Padding(
                  padding: const EdgeInsets.only(bottom: 8),
                  child: Row(
                    mainAxisAlignment: MainAxisAlignment.spaceBetween,
                    children: [
                      Expanded(
                        child: Column(
                          crossAxisAlignment: CrossAxisAlignment.start,
                          children: [
                            Text(
                              shiftTitle,
                              style: TextStyle(fontWeight: FontWeight.bold),
                              maxLines: 1,
                              overflow: TextOverflow.ellipsis,
                            ),
                            Text(
                              formattedDate,
                              style: TextStyle(
                                color: Colors.grey.shade700,
                                fontSize: 12,
                              ),
                            ),
                          ],
                        ),
                      ),
                      Text(
                        formattedAmount,
                        style: TextStyle(
                          fontWeight: FontWeight.bold,
                          color: Colors.amber.shade800,
                        ),
                      ),
                    ],
                  ),
                );
              }).toList(),
            ],
          ),
        ),
      ],
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
    final isCredit = amount > 0;
    final formattedAmount =
        isCredit
            ? '+${_walletController.formatCurrency(amount.toDouble())}'
            : _walletController.formatCurrency(amount.toDouble());

    final createdAt = DateTime.parse(transaction['created_at']);
    final formattedDate = DateFormat('MMM dd, yyyy').format(createdAt);

    final transactionType = transaction['transaction_type'] ?? '';
    String title =
        transaction['description'] ??
        transactionType.toString().capitalizeFirst!;

    // Get appropriate icon based on transaction type
    IconData transactionIcon;
    if (transactionType == 'deposit') {
      transactionIcon = Icons.add_circle_outline;
    } else if (transactionType == 'hold') {
      transactionIcon = Icons.arrow_upward;
    } else if (transactionType == 'release' || transactionType == 'refund') {
      transactionIcon = Icons.undo_outlined;
    } else if (transactionType == 'penalty') {
      transactionIcon = Icons.money_off_outlined;
    } else {
      transactionIcon =
          isCredit ? Icons.add_circle_outline : Icons.remove_circle_outline;
    }

    // Get color based on transaction type
    Color iconColor;
    if (transactionType == 'hold') {
      iconColor = Colors.red.shade700;
    } else if (transactionType == 'penalty') {
      iconColor = Colors.red.shade700;
    } else {
      iconColor = isCredit ? Colors.green.shade700 : Colors.red.shade700;
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

  // Show add funds dialog
  void _showAddFundsDialog() {
    Get.dialog(AddFundsDialog());
  }

  // Show withdraw dialog

  // Handle creating wallet
  void _handleCreateWallet() async {
    final created = await _walletController.createEmployerWalletIfNotExists();

    if (created) {
      Get.snackbar(
        'Wallet Created',
        'Your wallet has been successfully created. Add funds to start posting shifts.',
        snackPosition: SnackPosition.BOTTOM,
        backgroundColor: Colors.green.shade100,
        colorText: Colors.green.shade800,
      );

      // Prompt to add funds
      Future.delayed(Duration(seconds: 2), () {
        Get.dialog(AddFundsDialog());
      });
    } else if (_walletController.employerWallet.value != null) {
      Get.snackbar(
        'Wallet Exists',
        'You already have a wallet.',
        snackPosition: SnackPosition.BOTTOM,
        backgroundColor: Colors.blue.shade100,
        colorText: Colors.blue.shade800,
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

// Worker Wallet Screen
