import 'package:flutter/material.dart';
import 'package:intl/intl.dart';
import 'package:url_launcher/url_launcher.dart';
import 'package:font_awesome_flutter/font_awesome_flutter.dart';
import '../../config/app_theme.dart';
import '../../models/deal_model.dart';
import '../../models/lead_model.dart';
import '../../services/supabase_service.dart';

class BookingsScreen extends StatefulWidget {
  const BookingsScreen({super.key});

  @override
  State<BookingsScreen> createState() => _BookingsScreenState();
}

class _BookingsScreenState extends State<BookingsScreen> {
  List<Map<String, dynamic>> _bookings = [];
  bool _isLoading = true;
  final _searchController = TextEditingController();

  @override
  void initState() {
    super.initState();
    _loadBookings();
  }

  @override
  void dispose() {
    _searchController.dispose();
    super.dispose();
  }

  Future<void> _loadBookings() async {
    setState(() => _isLoading = true);

    final bookings = await SupabaseService.getBookings();

    if (mounted) {
      setState(() {
        _bookings = bookings;
        _isLoading = false;
      });
    }
  }

  List<Map<String, dynamic>> get _filteredBookings {
    final searchTerm = _searchController.text.toLowerCase();
    if (searchTerm.isEmpty) return _bookings;

    return _bookings.where((booking) {
      final lead = booking['lead'] as Map<String, dynamic>?;
      if (lead == null) return false;

      final name = (lead['name'] ?? '').toString().toLowerCase();
      final phone = (lead['phone'] ?? '').toString().toLowerCase();
      final email = (lead['email'] ?? '').toString().toLowerCase();
      final project = (lead['project_name'] ?? '').toString().toLowerCase();
      final dealNumber = (booking['deal_number'] ?? '').toString().toLowerCase();

      return name.contains(searchTerm) ||
          phone.contains(searchTerm) ||
          email.contains(searchTerm) ||
          project.contains(searchTerm) ||
          dealNumber.contains(searchTerm);
    }).toList();
  }

  Future<void> _makePhoneCall(String phone) async {
    final uri = Uri.parse('tel:$phone');
    if (await canLaunchUrl(uri)) {
      await launchUrl(uri);
    }
  }

  Future<void> _openWhatsApp(String phone) async {
    final cleanPhone = phone.replaceAll(RegExp(r'[^0-9]'), '');
    // Add country code if not present (assuming India +91)
    final phoneWithCode = cleanPhone.startsWith('91') ? cleanPhone : '91$cleanPhone';
    final uri = Uri.parse('https://wa.me/$phoneWithCode');
    try {
      await launchUrl(uri, mode: LaunchMode.externalApplication);
    } catch (e) {
      // Fallback to intent URL for Android
      final intentUri = Uri.parse('whatsapp://send?phone=$phoneWithCode');
      await launchUrl(intentUri);
    }
  }

  String _formatCurrency(double? value) {
    if (value == null) return '-';
    if (value >= 10000000) {
      return '${(value / 10000000).toStringAsFixed(2)} Cr';
    } else if (value >= 100000) {
      return '${(value / 100000).toStringAsFixed(2)} L';
    }
    return NumberFormat.currency(locale: 'en_IN', symbol: '₹', decimalDigits: 0).format(value);
  }

  @override
  Widget build(BuildContext context) {
    return Column(
      children: [
        // Search Bar
        Container(
          padding: const EdgeInsets.all(16),
          color: AppTheme.surfaceColor,
          child: TextField(
            controller: _searchController,
            decoration: InputDecoration(
              hintText: 'Search bookings...',
              prefixIcon: const Icon(Icons.search),
              suffixIcon: _searchController.text.isNotEmpty
                  ? IconButton(
                      icon: const Icon(Icons.clear),
                      onPressed: () {
                        _searchController.clear();
                        setState(() {});
                      },
                    )
                  : null,
            ),
            onChanged: (_) => setState(() {}),
          ),
        ),

        // Stats Summary
        Container(
          padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 12),
          color: AppTheme.successColor.withOpacity(0.1),
          child: Row(
            mainAxisAlignment: MainAxisAlignment.spaceAround,
            children: [
              _buildStatItem(
                'Total Bookings',
                _bookings.length.toString(),
                Icons.check_circle,
              ),
              _buildStatItem(
                'Total Value',
                _formatCurrency(
                  _bookings.fold<double>(
                    0,
                    (sum, b) => sum + ((b['deal_value'] as num?)?.toDouble() ?? 0),
                  ),
                ),
                Icons.currency_rupee,
              ),
            ],
          ),
        ),

        // Bookings List
        Expanded(
          child: RefreshIndicator(
            onRefresh: _loadBookings,
            child: _isLoading
                ? const Center(child: CircularProgressIndicator())
                : _filteredBookings.isEmpty
                    ? Center(
                        child: Column(
                          mainAxisAlignment: MainAxisAlignment.center,
                          children: [
                            Icon(
                              Icons.event_available_outlined,
                              size: 64,
                              color: AppTheme.textMuted,
                            ),
                            const SizedBox(height: 16),
                            Text(
                              'No bookings found',
                              style: TextStyle(
                                color: AppTheme.textSecondary,
                                fontSize: 16,
                              ),
                            ),
                          ],
                        ),
                      )
                    : ListView.builder(
                        padding: const EdgeInsets.all(16),
                        itemCount: _filteredBookings.length,
                        itemBuilder: (context, index) {
                          return _buildBookingCard(_filteredBookings[index]);
                        },
                      ),
          ),
        ),
      ],
    );
  }

  Widget _buildStatItem(String label, String value, IconData icon) {
    return Row(
      children: [
        Icon(icon, color: AppTheme.successColor, size: 20),
        const SizedBox(width: 8),
        Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              value,
              style: const TextStyle(
                fontSize: 16,
                fontWeight: FontWeight.bold,
              ),
            ),
            Text(
              label,
              style: TextStyle(
                fontSize: 12,
                color: AppTheme.textSecondary,
              ),
            ),
          ],
        ),
      ],
    );
  }

  Widget _buildBookingCard(Map<String, dynamic> booking) {
    final lead = booking['lead'] as Map<String, dynamic>?;
    final dealValue = (booking['deal_value'] as num?)?.toDouble();
    final bookingDate = booking['booking_date'] != null
        ? DateTime.parse(booking['booking_date'])
        : null;
    final closingDate = booking['closing_date'] != null
        ? DateTime.parse(booking['closing_date'])
        : null;

    final name = lead?['name'] ?? 'Unknown';
    final phone = lead?['phone'];
    final email = lead?['email'];
    final projectName = lead?['project_name'];
    final dealNumber = booking['deal_number'] ?? '';

    return Card(
      margin: const EdgeInsets.only(bottom: 12),
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            // Header Row
            Row(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                // Avatar with checkmark
                Stack(
                  children: [
                    CircleAvatar(
                      backgroundColor: AppTheme.successColor.withOpacity(0.1),
                      child: Text(
                        name[0].toUpperCase(),
                        style: TextStyle(
                          color: AppTheme.successColor,
                          fontWeight: FontWeight.bold,
                        ),
                      ),
                    ),
                    Positioned(
                      right: 0,
                      bottom: 0,
                      child: Container(
                        padding: const EdgeInsets.all(2),
                        decoration: const BoxDecoration(
                          color: AppTheme.successColor,
                          shape: BoxShape.circle,
                        ),
                        child: const Icon(
                          Icons.check,
                          size: 10,
                          color: Colors.white,
                        ),
                      ),
                    ),
                  ],
                ),
                const SizedBox(width: 12),

                // Name & Details
                Expanded(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      if (projectName != null)
                        Text(
                          projectName,
                          style: TextStyle(
                            fontSize: 12,
                            color: AppTheme.primaryColor,
                            fontWeight: FontWeight.w600,
                          ),
                        ),
                      Text(
                        name,
                        style: const TextStyle(
                          fontSize: 16,
                          fontWeight: FontWeight.w600,
                        ),
                      ),
                      Text(
                        'Deal #$dealNumber',
                        style: TextStyle(
                          fontSize: 12,
                          color: AppTheme.textSecondary,
                        ),
                      ),
                    ],
                  ),
                ),

                // Booked Badge
                Container(
                  padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 4),
                  decoration: BoxDecoration(
                    color: AppTheme.successColor.withOpacity(0.1),
                    borderRadius: BorderRadius.circular(20),
                  ),
                  child: Row(
                    mainAxisSize: MainAxisSize.min,
                    children: [
                      const Icon(
                        Icons.check_circle,
                        size: 14,
                        color: AppTheme.successColor,
                      ),
                      const SizedBox(width: 4),
                      const Text(
                        'BOOKED',
                        style: TextStyle(
                          fontSize: 11,
                          color: AppTheme.successColor,
                          fontWeight: FontWeight.w600,
                        ),
                      ),
                    ],
                  ),
                ),
              ],
            ),

            const SizedBox(height: 16),

            // Deal Value
            if (dealValue != null)
              Container(
                padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
                decoration: BoxDecoration(
                  color: Colors.green.withOpacity(0.05),
                  borderRadius: BorderRadius.circular(8),
                  border: Border.all(color: Colors.green.withOpacity(0.2)),
                ),
                child: Row(
                  mainAxisAlignment: MainAxisAlignment.center,
                  children: [
                    const Icon(Icons.currency_rupee, size: 18, color: Colors.green),
                    const SizedBox(width: 4),
                    Text(
                      _formatCurrency(dealValue),
                      style: const TextStyle(
                        fontSize: 18,
                        fontWeight: FontWeight.bold,
                        color: Colors.green,
                      ),
                    ),
                  ],
                ),
              ),

            const SizedBox(height: 12),

            // Dates Row
            Row(
              children: [
                if (bookingDate != null)
                  Expanded(
                    child: _buildDateInfo(
                      'Booking Date',
                      DateFormat('dd MMM yyyy').format(bookingDate),
                      Icons.event_available,
                    ),
                  ),
                if (closingDate != null)
                  Expanded(
                    child: _buildDateInfo(
                      'Closing Date',
                      DateFormat('dd MMM yyyy').format(closingDate),
                      Icons.event_note,
                    ),
                  ),
              ],
            ),

            const SizedBox(height: 12),
            const Divider(height: 1),
            const SizedBox(height: 12),

            // Action Buttons
            Row(
              mainAxisAlignment: MainAxisAlignment.spaceEvenly,
              children: [
                if (phone != null) ...[
                  _buildActionButton(
                    icon: Icons.phone,
                    label: 'Call',
                    color: Colors.blue,
                    onTap: () => _makePhoneCall(phone),
                  ),
                  _buildActionButton(
                    icon: FontAwesomeIcons.whatsapp,
                    label: 'WhatsApp',
                    color: const Color(0xFF25D366),
                    onTap: () => _openWhatsApp(phone),
                  ),
                ],
                if (email != null)
                  _buildActionButton(
                    icon: Icons.email,
                    label: 'Email',
                    color: Colors.orange,
                    onTap: () async {
                      final uri = Uri.parse('mailto:$email');
                      if (await canLaunchUrl(uri)) {
                        await launchUrl(uri);
                      }
                    },
                  ),
              ],
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildDateInfo(String label, String value, IconData icon) {
    return Row(
      children: [
        Icon(icon, size: 14, color: AppTheme.textMuted),
        const SizedBox(width: 6),
        Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              label,
              style: TextStyle(
                fontSize: 10,
                color: AppTheme.textMuted,
              ),
            ),
            Text(
              value,
              style: const TextStyle(
                fontSize: 12,
                fontWeight: FontWeight.w500,
              ),
            ),
          ],
        ),
      ],
    );
  }

  Widget _buildActionButton({
    required IconData icon,
    required String label,
    required Color color,
    required VoidCallback onTap,
  }) {
    return InkWell(
      onTap: onTap,
      borderRadius: BorderRadius.circular(8),
      child: Container(
        padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
        decoration: BoxDecoration(
          color: color.withOpacity(0.1),
          borderRadius: BorderRadius.circular(8),
        ),
        child: Row(
          children: [
            Icon(icon, color: color, size: 18),
            const SizedBox(width: 6),
            Text(
              label,
              style: TextStyle(
                color: color,
                fontWeight: FontWeight.w500,
                fontSize: 13,
              ),
            ),
          ],
        ),
      ),
    );
  }
}
