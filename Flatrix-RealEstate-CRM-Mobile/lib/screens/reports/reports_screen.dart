import 'package:flutter/material.dart';
import 'package:intl/intl.dart';
import '../../config/app_theme.dart';
import '../../services/supabase_service.dart';

class ReportsScreen extends StatefulWidget {
  const ReportsScreen({super.key});

  @override
  State<ReportsScreen> createState() => _ReportsScreenState();
}

class _ReportsScreenState extends State<ReportsScreen> {
  Map<String, dynamic> _stats = {};
  bool _isLoading = true;

  final currencyFormat = NumberFormat.currency(locale: 'en_IN', symbol: '\u20B9');

  @override
  void initState() {
    super.initState();
    _loadStats();
  }

  Future<void> _loadStats() async {
    setState(() => _isLoading = true);

    final stats = await SupabaseService.getReportStats();

    if (mounted) {
      setState(() {
        _stats = stats;
        _isLoading = false;
      });
    }
  }

  @override
  Widget build(BuildContext context) {
    return RefreshIndicator(
      onRefresh: _loadStats,
      child: _isLoading
          ? const Center(child: CircularProgressIndicator())
          : SingleChildScrollView(
              physics: const AlwaysScrollableScrollPhysics(),
              padding: const EdgeInsets.all(16),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  // Overview Stats
                  _buildSectionTitle('Overview'),
                  const SizedBox(height: 12),
                  _buildOverviewStats(),

                  const SizedBox(height: 24),

                  // Leads by Status
                  _buildSectionTitle('Leads by Status'),
                  const SizedBox(height: 12),
                  _buildLeadsByStatus(),

                  const SizedBox(height: 24),

                  // Leads by Source
                  _buildSectionTitle('Leads by Source'),
                  const SizedBox(height: 12),
                  _buildLeadsBySource(),

                  const SizedBox(height: 24),

                  // Conversion Stats
                  _buildSectionTitle('Conversion Stats'),
                  const SizedBox(height: 12),
                  _buildConversionStats(),
                ],
              ),
            ),
    );
  }

  Widget _buildSectionTitle(String title) {
    return Text(
      title,
      style: const TextStyle(
        fontSize: 18,
        fontWeight: FontWeight.bold,
      ),
    );
  }

  Widget _buildOverviewStats() {
    return GridView.count(
      crossAxisCount: 2,
      shrinkWrap: true,
      physics: const NeverScrollableScrollPhysics(),
      mainAxisSpacing: 12,
      crossAxisSpacing: 12,
      childAspectRatio: 1.5,
      children: [
        _buildStatCard(
          title: 'Total Leads',
          value: '${_stats['totalLeads'] ?? 0}',
          icon: Icons.people,
          color: AppTheme.primaryColor,
        ),
        _buildStatCard(
          title: 'Total Deals',
          value: '${_stats['totalDeals'] ?? 0}',
          icon: Icons.handshake,
          color: Colors.orange,
        ),
        _buildStatCard(
          title: 'Site Visits',
          value: '${_stats['siteVisitsCompleted'] ?? 0}',
          icon: Icons.location_on,
          color: Colors.purple,
        ),
        _buildStatCard(
          title: 'Bookings',
          value: '${_stats['bookings'] ?? 0}',
          icon: Icons.check_circle,
          color: Colors.green,
        ),
      ],
    );
  }

  Widget _buildStatCard({
    required String title,
    required String value,
    required IconData icon,
    required Color color,
  }) {
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          mainAxisAlignment: MainAxisAlignment.spaceBetween,
          children: [
            Container(
              padding: const EdgeInsets.all(8),
              decoration: BoxDecoration(
                color: color.withOpacity(0.1),
                borderRadius: BorderRadius.circular(8),
              ),
              child: Icon(icon, color: color, size: 20),
            ),
            Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(
                  value,
                  style: const TextStyle(
                    fontSize: 24,
                    fontWeight: FontWeight.bold,
                  ),
                ),
                Text(
                  title,
                  style: TextStyle(
                    fontSize: 12,
                    color: AppTheme.textSecondary,
                  ),
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildLeadsByStatus() {
    final leadsByStatus = _stats['leadsByStatus'] as Map<String, int>? ?? {};

    if (leadsByStatus.isEmpty) {
      return Card(
        child: Padding(
          padding: const EdgeInsets.all(24),
          child: Center(
            child: Text(
              'No data available',
              style: TextStyle(color: AppTheme.textMuted),
            ),
          ),
        ),
      );
    }

    final total = leadsByStatus.values.fold(0, (a, b) => a + b);

    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          children: leadsByStatus.entries.map((entry) {
            final percentage = total > 0 ? (entry.value / total * 100) : 0.0;
            final color = AppTheme.leadStatusColors[entry.key] ?? Colors.grey;

            return Padding(
              padding: const EdgeInsets.only(bottom: 12),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Row(
                    mainAxisAlignment: MainAxisAlignment.spaceBetween,
                    children: [
                      Row(
                        children: [
                          Container(
                            width: 12,
                            height: 12,
                            decoration: BoxDecoration(
                              color: color,
                              borderRadius: BorderRadius.circular(2),
                            ),
                          ),
                          const SizedBox(width: 8),
                          Text(
                            entry.key.replaceAll('_', ' '),
                            style: const TextStyle(fontSize: 14),
                          ),
                        ],
                      ),
                      Text(
                        '${entry.value} (${percentage.toStringAsFixed(1)}%)',
                        style: TextStyle(
                          fontSize: 14,
                          fontWeight: FontWeight.w600,
                          color: AppTheme.textSecondary,
                        ),
                      ),
                    ],
                  ),
                  const SizedBox(height: 6),
                  ClipRRect(
                    borderRadius: BorderRadius.circular(4),
                    child: LinearProgressIndicator(
                      value: percentage / 100,
                      backgroundColor: Colors.grey[200],
                      valueColor: AlwaysStoppedAnimation<Color>(color),
                      minHeight: 8,
                    ),
                  ),
                ],
              ),
            );
          }).toList(),
        ),
      ),
    );
  }

  Widget _buildLeadsBySource() {
    final leadsBySource = _stats['leadsBySource'] as Map<String, int>? ?? {};

    if (leadsBySource.isEmpty) {
      return Card(
        child: Padding(
          padding: const EdgeInsets.all(24),
          child: Center(
            child: Text(
              'No data available',
              style: TextStyle(color: AppTheme.textMuted),
            ),
          ),
        ),
      );
    }

    // Sort by count descending
    final sortedEntries = leadsBySource.entries.toList()
      ..sort((a, b) => b.value.compareTo(a.value));

    final colors = [
      Colors.blue,
      Colors.green,
      Colors.orange,
      Colors.purple,
      Colors.teal,
      Colors.pink,
      Colors.indigo,
      Colors.amber,
    ];

    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          children: sortedEntries.asMap().entries.map((mapEntry) {
            final index = mapEntry.key;
            final entry = mapEntry.value;
            final color = colors[index % colors.length];

            return Padding(
              padding: const EdgeInsets.only(bottom: 8),
              child: Row(
                children: [
                  Container(
                    width: 12,
                    height: 12,
                    decoration: BoxDecoration(
                      color: color,
                      borderRadius: BorderRadius.circular(2),
                    ),
                  ),
                  const SizedBox(width: 12),
                  Expanded(
                    child: Text(
                      entry.key,
                      style: const TextStyle(fontSize: 14),
                      overflow: TextOverflow.ellipsis,
                    ),
                  ),
                  Container(
                    padding: const EdgeInsets.symmetric(
                      horizontal: 10,
                      vertical: 4,
                    ),
                    decoration: BoxDecoration(
                      color: color.withOpacity(0.1),
                      borderRadius: BorderRadius.circular(12),
                    ),
                    child: Text(
                      '${entry.value}',
                      style: TextStyle(
                        fontSize: 13,
                        fontWeight: FontWeight.w600,
                        color: color,
                      ),
                    ),
                  ),
                ],
              ),
            );
          }).toList(),
        ),
      ),
    );
  }

  Widget _buildConversionStats() {
    final totalLeads = _stats['totalLeads'] ?? 0;
    final siteVisits = _stats['siteVisitsCompleted'] ?? 0;
    final bookings = _stats['bookings'] ?? 0;
    final totalDealValue = _stats['totalDealValue'] ?? 0.0;

    final siteVisitRate = totalLeads > 0 ? (siteVisits / totalLeads * 100) : 0.0;
    final bookingRate = siteVisits > 0 ? (bookings / siteVisits * 100) : 0.0;

    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          children: [
            // Total Deal Value
            Container(
              width: double.infinity,
              padding: const EdgeInsets.all(16),
              decoration: BoxDecoration(
                color: AppTheme.successColor.withOpacity(0.1),
                borderRadius: BorderRadius.circular(12),
              ),
              child: Column(
                children: [
                  Text(
                    'Total Deal Value',
                    style: TextStyle(
                      fontSize: 14,
                      color: AppTheme.textSecondary,
                    ),
                  ),
                  const SizedBox(height: 4),
                  Text(
                    currencyFormat.format(totalDealValue),
                    style: const TextStyle(
                      fontSize: 28,
                      fontWeight: FontWeight.bold,
                      color: AppTheme.successColor,
                    ),
                  ),
                ],
              ),
            ),

            const SizedBox(height: 16),

            // Conversion Rates
            Row(
              children: [
                Expanded(
                  child: _buildRateCard(
                    title: 'Site Visit Rate',
                    rate: siteVisitRate,
                    subtitle: '$siteVisits / $totalLeads leads',
                    color: Colors.purple,
                  ),
                ),
                const SizedBox(width: 12),
                Expanded(
                  child: _buildRateCard(
                    title: 'Booking Rate',
                    rate: bookingRate,
                    subtitle: '$bookings / $siteVisits visits',
                    color: Colors.green,
                  ),
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildRateCard({
    required String title,
    required double rate,
    required String subtitle,
    required Color color,
  }) {
    return Container(
      padding: const EdgeInsets.all(12),
      decoration: BoxDecoration(
        border: Border.all(color: Colors.grey[200]!),
        borderRadius: BorderRadius.circular(8),
      ),
      child: Column(
        children: [
          Text(
            '${rate.toStringAsFixed(1)}%',
            style: TextStyle(
              fontSize: 24,
              fontWeight: FontWeight.bold,
              color: color,
            ),
          ),
          const SizedBox(height: 4),
          Text(
            title,
            style: const TextStyle(
              fontSize: 12,
              fontWeight: FontWeight.w600,
            ),
          ),
          Text(
            subtitle,
            style: TextStyle(
              fontSize: 10,
              color: AppTheme.textMuted,
            ),
          ),
        ],
      ),
    );
  }
}
