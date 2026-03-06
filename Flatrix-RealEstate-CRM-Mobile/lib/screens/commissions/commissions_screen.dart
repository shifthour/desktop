import 'package:flutter/material.dart';
import 'package:intl/intl.dart';
import '../../config/app_theme.dart';
import '../../models/commission_model.dart';
import '../../services/supabase_service.dart';

class CommissionsScreen extends StatefulWidget {
  const CommissionsScreen({super.key});

  @override
  State<CommissionsScreen> createState() => _CommissionsScreenState();
}

class _CommissionsScreenState extends State<CommissionsScreen> {
  List<CommissionModel> _commissions = [];
  Map<String, dynamic> _stats = {};
  bool _isLoading = true;
  String _selectedStatus = 'All';

  final List<String> _statusFilters = [
    'All',
    'PENDING',
    'APPROVED',
    'PAID',
    'CANCELLED',
  ];

  final currencyFormat = NumberFormat.currency(locale: 'en_IN', symbol: '\u20B9');

  @override
  void initState() {
    super.initState();
    _loadData();
  }

  Future<void> _loadData() async {
    setState(() => _isLoading = true);

    final commissions = await SupabaseService.getCommissions(
      status: _selectedStatus == 'All' ? null : _selectedStatus,
    );
    final stats = await SupabaseService.getCommissionStats();

    if (mounted) {
      setState(() {
        _commissions = commissions;
        _stats = stats;
        _isLoading = false;
      });
    }
  }

  Color _getStatusColor(String status) {
    switch (status) {
      case 'PENDING':
        return Colors.orange;
      case 'APPROVED':
        return Colors.blue;
      case 'PAID':
        return Colors.green;
      case 'CANCELLED':
        return Colors.red;
      default:
        return Colors.grey;
    }
  }

  @override
  Widget build(BuildContext context) {
    return RefreshIndicator(
      onRefresh: _loadData,
      child: _isLoading
          ? const Center(child: CircularProgressIndicator())
          : SingleChildScrollView(
              physics: const AlwaysScrollableScrollPhysics(),
              child: Column(
                children: [
                  // Stats Cards
                  _buildStatsSection(),

                  // Status Filter
                  Container(
                    padding: const EdgeInsets.all(16),
                    color: AppTheme.surfaceColor,
                    child: SizedBox(
                      height: 36,
                      child: ListView.separated(
                        scrollDirection: Axis.horizontal,
                        itemCount: _statusFilters.length,
                        separatorBuilder: (_, __) => const SizedBox(width: 8),
                        itemBuilder: (context, index) {
                          final status = _statusFilters[index];
                          final isSelected = _selectedStatus == status;
                          return FilterChip(
                            label: Text(status),
                            selected: isSelected,
                            onSelected: (selected) {
                              setState(() => _selectedStatus = status);
                              _loadData();
                            },
                            selectedColor: AppTheme.primaryColor.withOpacity(0.2),
                            checkmarkColor: AppTheme.primaryColor,
                            labelStyle: TextStyle(
                              color: isSelected
                                  ? AppTheme.primaryColor
                                  : AppTheme.textSecondary,
                              fontWeight:
                                  isSelected ? FontWeight.w600 : FontWeight.normal,
                            ),
                          );
                        },
                      ),
                    ),
                  ),

                  // Commissions List
                  _commissions.isEmpty
                      ? Padding(
                          padding: const EdgeInsets.all(48),
                          child: Column(
                            children: [
                              Icon(
                                Icons.payments_outlined,
                                size: 64,
                                color: AppTheme.textMuted,
                              ),
                              const SizedBox(height: 16),
                              Text(
                                'No commissions found',
                                style: TextStyle(
                                  color: AppTheme.textSecondary,
                                  fontSize: 16,
                                ),
                              ),
                            ],
                          ),
                        )
                      : ListView.builder(
                          shrinkWrap: true,
                          physics: const NeverScrollableScrollPhysics(),
                          padding: const EdgeInsets.all(16),
                          itemCount: _commissions.length,
                          itemBuilder: (context, index) {
                            return _buildCommissionCard(_commissions[index]);
                          },
                        ),
                ],
              ),
            ),
    );
  }

  Widget _buildStatsSection() {
    return Container(
      padding: const EdgeInsets.all(16),
      child: Column(
        children: [
          Row(
            children: [
              Expanded(
                child: _buildStatCard(
                  title: 'Total Commissions',
                  value: currencyFormat.format(_stats['totalAmount'] ?? 0),
                  icon: Icons.account_balance_wallet,
                  color: AppTheme.primaryColor,
                ),
              ),
            ],
          ),
          const SizedBox(height: 12),
          Row(
            children: [
              Expanded(
                child: _buildStatCard(
                  title: 'Pending',
                  value: currencyFormat.format(_stats['pendingAmount'] ?? 0),
                  subtitle: '${_stats['pendingCount'] ?? 0} commissions',
                  icon: Icons.pending,
                  color: Colors.orange,
                ),
              ),
              const SizedBox(width: 12),
              Expanded(
                child: _buildStatCard(
                  title: 'Paid',
                  value: currencyFormat.format(_stats['paidAmount'] ?? 0),
                  subtitle: '${_stats['paidCount'] ?? 0} commissions',
                  icon: Icons.check_circle,
                  color: Colors.green,
                ),
              ),
            ],
          ),
        ],
      ),
    );
  }

  Widget _buildStatCard({
    required String title,
    required String value,
    String? subtitle,
    required IconData icon,
    required Color color,
  }) {
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                Container(
                  padding: const EdgeInsets.all(8),
                  decoration: BoxDecoration(
                    color: color.withOpacity(0.1),
                    borderRadius: BorderRadius.circular(8),
                  ),
                  child: Icon(icon, color: color, size: 20),
                ),
                const Spacer(),
              ],
            ),
            const SizedBox(height: 12),
            Text(
              value,
              style: const TextStyle(
                fontSize: 18,
                fontWeight: FontWeight.bold,
              ),
            ),
            const SizedBox(height: 4),
            Text(
              title,
              style: TextStyle(
                fontSize: 12,
                color: AppTheme.textSecondary,
              ),
            ),
            if (subtitle != null) ...[
              const SizedBox(height: 2),
              Text(
                subtitle,
                style: TextStyle(
                  fontSize: 11,
                  color: AppTheme.textMuted,
                ),
              ),
            ],
          ],
        ),
      ),
    );
  }

  Widget _buildCommissionCard(CommissionModel commission) {
    final statusColor = _getStatusColor(commission.status);

    return Card(
      margin: const EdgeInsets.only(bottom: 12),
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            // Header
            Row(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                // Icon
                Container(
                  padding: const EdgeInsets.all(10),
                  decoration: BoxDecoration(
                    color: AppTheme.primaryColor.withOpacity(0.1),
                    borderRadius: BorderRadius.circular(10),
                  ),
                  child: Icon(
                    Icons.payments,
                    color: AppTheme.primaryColor,
                    size: 24,
                  ),
                ),
                const SizedBox(width: 12),

                // Info
                Expanded(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text(
                        commission.channelPartnerName ?? 'Channel Partner',
                        style: const TextStyle(
                          fontSize: 16,
                          fontWeight: FontWeight.w600,
                        ),
                      ),
                      if (commission.projectName != null)
                        Text(
                          commission.projectName!,
                          style: TextStyle(
                            fontSize: 13,
                            color: AppTheme.primaryColor,
                          ),
                        ),
                      Text(
                        'Lead: ${commission.leadName ?? 'N/A'}',
                        style: TextStyle(
                          fontSize: 12,
                          color: AppTheme.textSecondary,
                        ),
                      ),
                    ],
                  ),
                ),

                // Status Badge
                Container(
                  padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 4),
                  decoration: BoxDecoration(
                    color: statusColor.withOpacity(0.1),
                    borderRadius: BorderRadius.circular(20),
                  ),
                  child: Text(
                    commission.status,
                    style: TextStyle(
                      fontSize: 11,
                      color: statusColor,
                      fontWeight: FontWeight.w600,
                    ),
                  ),
                ),
              ],
            ),

            const SizedBox(height: 16),
            const Divider(height: 1),
            const SizedBox(height: 16),

            // Amount Section
            Row(
              mainAxisAlignment: MainAxisAlignment.spaceBetween,
              children: [
                Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(
                      'Commission Amount',
                      style: TextStyle(
                        fontSize: 12,
                        color: AppTheme.textMuted,
                      ),
                    ),
                    Text(
                      currencyFormat.format(commission.amount),
                      style: const TextStyle(
                        fontSize: 20,
                        fontWeight: FontWeight.bold,
                        color: AppTheme.successColor,
                      ),
                    ),
                  ],
                ),
                Column(
                  crossAxisAlignment: CrossAxisAlignment.end,
                  children: [
                    Text(
                      'Percentage',
                      style: TextStyle(
                        fontSize: 12,
                        color: AppTheme.textMuted,
                      ),
                    ),
                    Text(
                      '${commission.percentage}%',
                      style: const TextStyle(
                        fontSize: 18,
                        fontWeight: FontWeight.w600,
                      ),
                    ),
                  ],
                ),
              ],
            ),

            if (commission.createdAt != null) ...[
              const SizedBox(height: 12),
              Row(
                children: [
                  Icon(Icons.calendar_today, size: 14, color: AppTheme.textMuted),
                  const SizedBox(width: 4),
                  Text(
                    'Created: ${DateFormat('MMM d, yyyy').format(commission.createdAt!)}',
                    style: TextStyle(
                      fontSize: 12,
                      color: AppTheme.textSecondary,
                    ),
                  ),
                ],
              ),
            ],

            if (commission.paidDate != null) ...[
              const SizedBox(height: 4),
              Row(
                children: [
                  Icon(Icons.check_circle, size: 14, color: Colors.green),
                  const SizedBox(width: 4),
                  Text(
                    'Paid: ${DateFormat('MMM d, yyyy').format(commission.paidDate!)}',
                    style: TextStyle(
                      fontSize: 12,
                      color: Colors.green,
                    ),
                  ),
                ],
              ),
            ],
          ],
        ),
      ),
    );
  }
}
