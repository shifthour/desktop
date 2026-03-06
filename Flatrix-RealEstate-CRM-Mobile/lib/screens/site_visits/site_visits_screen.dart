import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'package:url_launcher/url_launcher.dart';
import 'package:intl/intl.dart';
import 'package:font_awesome_flutter/font_awesome_flutter.dart';
import '../../config/app_theme.dart';
import '../../models/lead_model.dart';
import '../../models/deal_model.dart';
import '../../providers/auth_provider.dart';
import '../../services/supabase_service.dart';

class SiteVisitsScreen extends StatefulWidget {
  const SiteVisitsScreen({super.key});

  @override
  State<SiteVisitsScreen> createState() => _SiteVisitsScreenState();
}

class _SiteVisitsScreenState extends State<SiteVisitsScreen> {
  List<Map<String, dynamic>> _siteVisits = [];
  bool _isLoading = true;
  String _selectedStatus = 'SCHEDULED';

  final List<String> _statusFilters = [
    'All',
    'SCHEDULED',
    'COMPLETED',
    'CANCELLED',
  ];

  @override
  void initState() {
    super.initState();
    _loadSiteVisits();
  }

  Future<void> _loadSiteVisits() async {
    setState(() => _isLoading = true);

    final authProvider = context.read<AuthProvider>();

    // Get qualified leads
    final leads = await SupabaseService.getLeads(
      assignedToId:
          authProvider.user?.isAdmin == true ? null : authProvider.user?.id,
      status: 'QUALIFIED',
    );

    // Get deals with site visits
    List<Map<String, dynamic>> siteVisits = [];
    for (final lead in leads) {
      final deal = await SupabaseService.getDealByLeadId(lead.id);
      if (deal != null &&
          deal.siteVisitStatus != null &&
          deal.siteVisitStatus != 'NOT_VISITED') {
        // Filter by status
        if (_selectedStatus == 'All' ||
            deal.siteVisitStatus == _selectedStatus) {
          siteVisits.add({
            'lead': lead,
            'deal': deal,
          });
        }
      }
    }

    // Sort by site visit date
    siteVisits.sort((a, b) {
      final dateA = (a['deal'] as DealModel).siteVisitDate;
      final dateB = (b['deal'] as DealModel).siteVisitDate;
      if (dateA == null && dateB == null) return 0;
      if (dateA == null) return 1;
      if (dateB == null) return -1;
      return dateB.compareTo(dateA);
    });

    if (mounted) {
      setState(() {
        _siteVisits = siteVisits;
        _isLoading = false;
      });
    }
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

  Future<void> _updateSiteVisitStatus(DealModel deal, String newStatus) async {
    final success = await SupabaseService.updateDeal(deal.id, {
      'site_visit_status': newStatus,
    });

    if (success) {
      _loadSiteVisits();
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('Status updated successfully')),
        );
      }
    }
  }

  void _showStatusUpdateDialog(DealModel deal) {
    showModalBottomSheet(
      context: context,
      builder: (context) => Container(
        padding: const EdgeInsets.all(20),
        child: Column(
          mainAxisSize: MainAxisSize.min,
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            const Text(
              'Update Site Visit Status',
              style: TextStyle(
                fontSize: 18,
                fontWeight: FontWeight.bold,
              ),
            ),
            const SizedBox(height: 16),
            ..._statusFilters
                .where((s) => s != 'All')
                .map(
                  (status) => ListTile(
                    leading: Container(
                      width: 12,
                      height: 12,
                      decoration: BoxDecoration(
                        color: AppTheme.siteVisitStatusColors[status],
                        shape: BoxShape.circle,
                      ),
                    ),
                    title: Text(status.replaceAll('_', ' ')),
                    trailing: deal.siteVisitStatus == status
                        ? const Icon(Icons.check, color: AppTheme.successColor)
                        : null,
                    onTap: () {
                      Navigator.pop(context);
                      _updateSiteVisitStatus(deal, status);
                    },
                  ),
                ),
            const SizedBox(height: 8),
          ],
        ),
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    return Column(
        children: [
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
                    label: Text(status.replaceAll('_', ' ')),
                    selected: isSelected,
                    onSelected: (selected) {
                      setState(() => _selectedStatus = status);
                      _loadSiteVisits();
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

          // Site Visits List
          Expanded(
            child: RefreshIndicator(
              onRefresh: _loadSiteVisits,
              child: _isLoading
                  ? const Center(child: CircularProgressIndicator())
                  : _siteVisits.isEmpty
                      ? Center(
                          child: Column(
                            mainAxisAlignment: MainAxisAlignment.center,
                            children: [
                              Icon(
                                Icons.location_off_outlined,
                                size: 64,
                                color: AppTheme.textMuted,
                              ),
                              const SizedBox(height: 16),
                              Text(
                                'No site visits found',
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
                          itemCount: _siteVisits.length,
                          itemBuilder: (context, index) {
                            final data = _siteVisits[index];
                            return _buildSiteVisitCard(
                              data['lead'] as LeadModel,
                              data['deal'] as DealModel,
                            );
                          },
                        ),
            ),
          ),
        ],
      );
  }

  Widget _buildSiteVisitCard(LeadModel lead, DealModel deal) {
    final siteVisitColor =
        AppTheme.siteVisitStatusColors[deal.siteVisitStatus] ?? Colors.grey;
    final conversionColor =
        AppTheme.conversionStatusColors[deal.conversionStatus] ?? Colors.grey;

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
                CircleAvatar(
                  backgroundColor: AppTheme.primaryColor.withOpacity(0.1),
                  child: Text(
                    lead.displayName[0].toUpperCase(),
                    style: TextStyle(
                      color: AppTheme.primaryColor,
                      fontWeight: FontWeight.bold,
                    ),
                  ),
                ),
                const SizedBox(width: 12),
                Expanded(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      if (lead.projectName != null)
                        Text(
                          lead.projectName!,
                          style: TextStyle(
                            fontSize: 12,
                            color: AppTheme.primaryColor,
                            fontWeight: FontWeight.w600,
                          ),
                        ),
                      Text(
                        lead.displayName,
                        style: const TextStyle(
                          fontSize: 16,
                          fontWeight: FontWeight.w600,
                        ),
                      ),
                      if (lead.phone != null)
                        Text(
                          lead.phone!,
                          style: TextStyle(
                            fontSize: 13,
                            color: AppTheme.textSecondary,
                          ),
                        ),
                    ],
                  ),
                ),
                // Status Badge (Tappable)
                InkWell(
                  onTap: () => _showStatusUpdateDialog(deal),
                  borderRadius: BorderRadius.circular(20),
                  child: Container(
                    padding:
                        const EdgeInsets.symmetric(horizontal: 10, vertical: 4),
                    decoration: BoxDecoration(
                      color: siteVisitColor.withOpacity(0.1),
                      borderRadius: BorderRadius.circular(20),
                      border: Border.all(color: siteVisitColor.withOpacity(0.3)),
                    ),
                    child: Row(
                      mainAxisSize: MainAxisSize.min,
                      children: [
                        Text(
                          (deal.siteVisitStatus ?? 'NOT_VISITED')
                              .replaceAll('_', ' '),
                          style: TextStyle(
                            fontSize: 11,
                            color: siteVisitColor,
                            fontWeight: FontWeight.w600,
                          ),
                        ),
                        const SizedBox(width: 4),
                        Icon(Icons.edit, size: 12, color: siteVisitColor),
                      ],
                    ),
                  ),
                ),
              ],
            ),

            const SizedBox(height: 12),

            // Site Visit Date & Time
            if (deal.siteVisitDate != null)
              Container(
                padding:
                    const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
                decoration: BoxDecoration(
                  color: Colors.blue.withOpacity(0.05),
                  borderRadius: BorderRadius.circular(8),
                ),
                child: Row(
                  children: [
                    const Icon(Icons.calendar_today,
                        size: 16, color: Colors.blue),
                    const SizedBox(width: 8),
                    Text(
                      DateFormat('EEEE, MMM d, yyyy').format(deal.siteVisitDate!),
                      style: const TextStyle(
                        fontSize: 13,
                        fontWeight: FontWeight.w500,
                      ),
                    ),
                    const SizedBox(width: 12),
                    const Icon(Icons.access_time, size: 16, color: Colors.blue),
                    const SizedBox(width: 4),
                    Text(
                      DateFormat('h:mm a').format(deal.siteVisitDate!),
                      style: const TextStyle(
                        fontSize: 13,
                        fontWeight: FontWeight.w500,
                      ),
                    ),
                  ],
                ),
              ),

            const SizedBox(height: 12),

            // Info Row
            Row(
              children: [
                // Conversion Status
                Expanded(
                  child: Row(
                    children: [
                      Text(
                        'Conversion: ',
                        style: TextStyle(
                          fontSize: 12,
                          color: AppTheme.textMuted,
                        ),
                      ),
                      Container(
                        padding: const EdgeInsets.symmetric(
                            horizontal: 6, vertical: 2),
                        decoration: BoxDecoration(
                          color: conversionColor.withOpacity(0.1),
                          borderRadius: BorderRadius.circular(4),
                        ),
                        child: Text(
                          (deal.conversionStatus ?? 'PENDING')
                              .replaceAll('_', ' '),
                          style: TextStyle(
                            fontSize: 11,
                            color: conversionColor,
                            fontWeight: FontWeight.w600,
                          ),
                        ),
                      ),
                    ],
                  ),
                ),
                // Attended By
                if (deal.attendedBy != null)
                  Row(
                    children: [
                      Icon(Icons.person, size: 14, color: AppTheme.textMuted),
                      const SizedBox(width: 4),
                      Text(
                        deal.attendedBy!,
                        style: TextStyle(
                          fontSize: 12,
                          color: AppTheme.textSecondary,
                        ),
                      ),
                    ],
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
                if (lead.phone != null) ...[
                  _buildActionButton(
                    icon: Icons.phone,
                    label: 'Call',
                    color: Colors.blue,
                    onTap: () => _makePhoneCall(lead.phone!),
                  ),
                  _buildActionButton(
                    icon: FontAwesomeIcons.whatsapp,
                    label: 'WhatsApp',
                    color: const Color(0xFF25D366),
                    onTap: () => _openWhatsApp(lead.phone!),
                  ),
                ],
                _buildActionButton(
                  icon: Icons.directions,
                  label: 'Navigate',
                  color: Colors.purple,
                  onTap: () async {
                    // Open Google Maps for navigation
                    final uri = Uri.parse(
                        'https://maps.google.com/?q=${lead.preferredLocation ?? 'site visit'}');
                    if (await canLaunchUrl(uri)) {
                      await launchUrl(uri,
                          mode: LaunchMode.externalApplication);
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
