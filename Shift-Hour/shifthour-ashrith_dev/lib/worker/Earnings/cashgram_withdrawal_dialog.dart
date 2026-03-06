import 'package:flutter/material.dart';
import 'package:get/get.dart';
import 'package:shifthour/worker/Earnings/earnings_dashboard.dart';
import 'package:supabase_flutter/supabase_flutter.dart';

import 'cashfree_payouts.dart';

class CashgramWithdrawalDialog extends StatefulWidget {
  final WorkerWalletController controller;

  const CashgramWithdrawalDialog({Key? key, required this.controller})
    : super(key: key);

  @override
  _CashgramWithdrawalDialogState createState() =>
      _CashgramWithdrawalDialogState();
}

class _CashgramWithdrawalDialogState extends State<CashgramWithdrawalDialog> {
  final _formKey = GlobalKey<FormState>();
  final TextEditingController _amountController = TextEditingController();
  final TextEditingController _nameController = TextEditingController();
  final TextEditingController _emailController = TextEditingController();
  final TextEditingController _phoneController = TextEditingController();

  // Pre-populate with user's data if available
  @override
  void initState() {
    super.initState();
    _prefillUserData();
  }

  void _prefillUserData() {
    try {
      final user = Supabase.instance.client.auth.currentUser;
      if (user != null) {
        if (user.email != null && user.email!.isNotEmpty) {
          _emailController.text = user.email!;
        }

        if (user.userMetadata != null) {
          final metadata = user.userMetadata!;
          if (metadata['full_name'] != null) {
            _nameController.text = metadata['full_name'];
          } else if (metadata['name'] != null) {
            _nameController.text = metadata['name'];
          }

          if (metadata['phone'] != null) {
            _phoneController.text = metadata['phone'];
          }
        }
      }
    } catch (e) {
      print('Error prefilling user data: $e');
    }
  }

  @override
  Widget build(BuildContext context) {
    return AlertDialog(
      title: Row(
        children: [
          Icon(Icons.account_balance_wallet, color: Colors.blue.shade600),
          SizedBox(width: 10),
          Text('Withdraw'),
        ],
      ),
      content: SingleChildScrollView(
        child: Form(
          key: _formKey,
          child: Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              // Information about Cashgram
              Container(
                padding: EdgeInsets.all(12),
                decoration: BoxDecoration(
                  color: Colors.blue.shade50,
                  borderRadius: BorderRadius.circular(8),
                ),
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(
                      'What is a Cashgram?',
                      style: TextStyle(
                        fontSize: 16,
                        fontWeight: FontWeight.bold,
                        color: Colors.blue.shade700,
                      ),
                    ),
                    SizedBox(height: 8),
                    Text(
                      'A Cashgram is a payment link that allows you to withdraw money directly to your bank account. You\'ll receive a link via email and SMS where you can enter your bank details securely.',
                      style: TextStyle(fontSize: 14),
                    ),
                  ],
                ),
              ),
              SizedBox(height: 20),

              // Available Balance Display
              Text(
                'Available Balance: ${widget.controller.getFormattedBalance()}',
                style: TextStyle(
                  fontSize: 16,
                  fontWeight: FontWeight.bold,
                  color: Colors.green.shade700,
                ),
              ),
              SizedBox(height: 16),

              // Amount Input
              TextFormField(
                controller: _amountController,
                keyboardType: TextInputType.numberWithOptions(decimal: true),
                decoration: InputDecoration(
                  labelText: 'Amount',
                  prefixText: '₹ ',
                  border: OutlineInputBorder(),
                  hintText: 'Minimum ₹1',
                ),
                validator: (value) {
                  if (value == null || value.isEmpty) {
                    return 'Please enter an amount';
                  }
                  final amount = double.tryParse(value);
                  if (amount == null || amount <= 0) {
                    return 'Please enter a valid amount';
                  }
                  if (amount > widget.controller.balance.value) {
                    return 'Insufficient balance';
                  }
                  if (amount <= 1) {
                    return 'Minimum withdrawal is ₹1';
                  }
                  return null;
                },
              ),
              SizedBox(height: 16),

              // Name Input
              TextFormField(
                controller: _nameController,
                decoration: InputDecoration(
                  labelText: 'Recipient Name',
                  border: OutlineInputBorder(),
                  hintText: 'Enter full name as in bank account',
                ),
                validator: (value) {
                  if (value == null || value.isEmpty) {
                    return 'Please enter recipient name';
                  }
                  return null;
                },
              ),
              SizedBox(height: 16),

              // Email Input
              TextFormField(
                controller: _emailController,
                keyboardType: TextInputType.emailAddress,
                decoration: InputDecoration(
                  labelText: 'Recipient Email',
                  border: OutlineInputBorder(),
                  hintText: 'You\'ll receive the cashgram link here',
                ),
                validator: (value) {
                  if (value == null || value.isEmpty) {
                    return 'Please enter recipient email';
                  }
                  if (!CashfreePayoutsService.isValidEmail(value)) {
                    return 'Please enter a valid email';
                  }
                  return null;
                },
              ),
              SizedBox(height: 16),

              // Phone Input
              TextFormField(
                controller: _phoneController,
                keyboardType: TextInputType.phone,
                decoration: InputDecoration(
                  labelText: 'Recipient Phone',
                  border: OutlineInputBorder(),
                  hintText: '10-digit mobile number',
                  prefixText: '+91 ',
                ),
                validator: (value) {
                  if (value == null || value.isEmpty) {
                    return 'Please enter recipient phone';
                  }
                  if (!CashfreePayoutsService.isValidPhone(value)) {
                    return 'Please enter a valid 10-digit number starting with 6-9';
                  }
                  return null;
                },
              ),

              // Error message display
              Obx(() {
                if (widget.controller.withdrawalError.value.isNotEmpty) {
                  return Padding(
                    padding: const EdgeInsets.only(top: 16.0),
                    child: Container(
                      padding: EdgeInsets.all(8),
                      decoration: BoxDecoration(
                        color: Colors.red.shade50,
                        borderRadius: BorderRadius.circular(4),
                        border: Border.all(color: Colors.red.shade200),
                      ),
                      child: Text(
                        widget.controller.withdrawalError.value,
                        style: TextStyle(color: Colors.red.shade800),
                      ),
                    ),
                  );
                }
                return SizedBox.shrink();
              }),

              // Success message display
              Obx(() {
                if (widget.controller.withdrawalStatus.value.isNotEmpty) {
                  return Padding(
                    padding: const EdgeInsets.only(top: 16.0),
                    child: Container(
                      padding: EdgeInsets.all(8),
                      decoration: BoxDecoration(
                        color: Colors.green.shade50,
                        borderRadius: BorderRadius.circular(4),
                        border: Border.all(color: Colors.green.shade200),
                      ),
                      child: Text(
                        widget.controller.withdrawalStatus.value,
                        style: TextStyle(color: Colors.green.shade800),
                      ),
                    ),
                  );
                }
                return SizedBox.shrink();
              }),
            ],
          ),
        ),
      ),
      actions: [
        TextButton(onPressed: () => Get.back(), child: Text('Cancel')),
        Obx(() {
          return ElevatedButton(
            style: ElevatedButton.styleFrom(
              backgroundColor: Colors.blue.shade600,
              foregroundColor: Colors.white,
            ),
            onPressed:
                widget.controller.isWithdrawing.value
                    ? null
                    : () async {
                      if (_formKey.currentState!.validate()) {
                        final amount = double.parse(_amountController.text);
                        final name = _nameController.text;
                        final email = _emailController.text;
                        final phone = _phoneController.text;

                        final success = await widget.controller
                            .withdrawViaCashgram(
                              amount: amount,
                              name: name,
                              email: email,
                              phone: phone,
                            );

                        if (success) {
                          Get.back(); // Close dialog
                          Get.snackbar(
                            'Success',
                            'Payment created successfully. The recipient will receive the payment link.',
                            snackPosition: SnackPosition.BOTTOM,
                            backgroundColor: Colors.green.shade100,
                            colorText: Colors.green.shade800,
                            duration: Duration(seconds: 5),
                            icon: Icon(
                              Icons.check_circle,
                              color: Colors.green.shade800,
                            ),
                          );
                        }
                      }
                    },
            child:
                widget.controller.isWithdrawing.value
                    ? SizedBox(
                      height: 20,
                      width: 20,
                      child: CircularProgressIndicator(
                        color: Colors.white,
                        strokeWidth: 2,
                      ),
                    )
                    : Text('Create Withdraw'),
          );
        }),
      ],
    );
  }
}
