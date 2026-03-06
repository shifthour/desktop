import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'package:url_launcher/url_launcher.dart';
import 'package:intl/intl.dart';
import '../../config/app_theme.dart';
import '../../models/lead_model.dart';
import '../../models/deal_model.dart';
import '../../providers/auth_provider.dart';
import '../../services/supabase_service.dart';

class DealsScreen extends StatefulWidget {
  const DealsScreen({super.key});

  @override
  State<DealsScreen> createState() => _DealsScreenState();
}

class _DealsScreenState extends State<DealsScreen> {
  List<Map<String, dynamic>> _dealsWithLeads = [];
  bool _isLoading = true;
  String _selectedStatus = 'All';

  final List<String> _statusFilters = [
    'All',
    'NOT_VISITED',
    'SCHEDULED',
    'COMPLETED',
  ];

  @override
  void initState() {
    super.initState();
    _loadDeals();
  }

  Future<void> _loadDeals() async {
    setState(() => _isLoading = true);

    final authProvider = context.read<AuthProvider>();

    // Get qualified leads
    final leads = await SupabaseService.getLeads(
      assignedToId:
          authProvider.user?.isAdmin == true ? null : authProvider.user?.id,
      status: 'QUALIFIED',
    );

    // Get deals for each lead
    List<Map<String, dynamic>> dealsWithLeads = [];
    for (final lead in leads) {
      final deal = await SupabaseService.getDealByLeadId(lead.id);
      if (deal != null) {
        // Filter by site visit status
        if (_selectedStatus == 'All' ||
            deal.siteVisitStatus == _selectedStatus) {
          dealsWithLeads.add({
            'lead': lead,
            'deal': deal,
          });
        }
      }
    }

    if (mounted) {
      setState(() {
        _dealsWithLeads = dealsWithLeads;
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
    final uri = Uri.parse('https://wa.me/$cleanPhone');
    if (await canLaunchUrl(uri)) {
      await launchUrl(uri, mode: LaunchMode.externalApplication);
    }
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
                      _loadDeals();
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

          // Deals List
          Expanded(
            child: RefreshIndicator(
              onRefresh: _loadDeals,
              child: _isLoading
                  ? const Center(child: CircularProgressIndicator())
                  : _dealsWithLeads.isEmpty
                      ? Center(
                          child: Column(
                            mainAxisAlignment: MainAxisAlignment.center,
                            children: [
                              Icon(
                                Icons.handshake_outlined,
                                size: 64,
                                color: AppTheme.textMuted,
                              ),
                              const SizedBox(height: 16),
                              Text(
                                'No deals found',
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
                          itemCount: _dealsWithLeads.length,
                          itemBuilder: (context, index) {
                            final data = _dealsWithLeads[index];
                            return _buildDealCard(
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

  Widget _buildDealCard(LeadModel lead, DealModel deal) {
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
              ],
            ),

            const SizedBox(height: 16),

            // Status Row
            Row(
              children: [
                Expanded(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text(
                        'Site Visit',
                        style: TextStyle(
                          fontSize: 11,
                          color: AppTheme.textMuted,
                        ),
                      ),
                      const SizedBox(height: 4),
                      Container(
                        padding: const EdgeInsets.symmetric(
                            horizontal: 8, vertical: 4),
                        decoration: BoxDecoration(
                          color: siteVisitColor.withOpacity(0.1),
                          borderRadius: BorderRadius.circular(4),
                        ),
                        child: Text(
                          (deal.siteVisitStatus ?? 'NOT_VISITED')
                              .replaceAll('_', ' '),
                          style: TextStyle(
                            fontSize: 11,
                            color: siteVisitColor,
                            fontWeight: FontWeight.w600,
                          ),
                        ),
                      ),
                    ],
                  ),
                ),
                Expanded(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text(
                        'Conversion',
                        style: TextStyle(
                          fontSize: 11,
                          color: AppTheme.textMuted,
                        ),
                      ),
                      const SizedBox(height: 4),
                      Container(
                        padding: const EdgeInsets.symmetric(
                            horizontal: 8, vertical: 4),
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
              ],
            ),

            // Site Visit Date
            if (deal.siteVisitDate != null) ...[
              const SizedBox(height: 12),
              Row(
                children: [
                  Icon(Icons.calendar_today,
                      size: 14, color: AppTheme.textMuted),
                  const SizedBox(width: 6),
                  Text(
                    'Site Visit: ${DateFormat('MMM d, yyyy h:mm a').format(deal.siteVisitDate!)}',
                    style: TextStyle(
                      fontSize: 12,
                      color: AppTheme.textSecondary,
                    ),
                  ),
                ],
              ),
            ],

            // Attended By
            if (deal.attendedBy != null) ...[
              const SizedBox(height: 6),
              Row(
                children: [
                  Icon(Icons.person, size: 14, color: AppTheme.textMuted),
                  const SizedBox(width: 6),
                  Text(
                    'Attended by: ${deal.attendedBy}',
                    style: TextStyle(
                      fontSize: 12,
                      color: AppTheme.textSecondary,
                    ),
                  ),
                ],
              ),
            ],

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
                    color: Colors.blue,
                    onTap: () => _makePhoneCall(lead.phone!),
                  ),
                  _buildActionButton(
                    icon: Icons.chat,
                    color: Colors.green,
                    onTap: () => _openWhatsApp(lead.phone!),
                  ),
                ],
              ],
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildActionButton({
    required IconData icon,
    required Color color,
    required VoidCallback onTap,
  }) {
    return InkWell(
      onTap: onTap,
      borderRadius: BorderRadius.circular(8),
      child: Container(
        padding: const EdgeInsets.all(10),
        decoration: BoxDecoration(
          color: color.withOpacity(0.1),
          borderRadius: BorderRadius.circular(8),
        ),
        child: Icon(icon, color: color, size: 20),
      ),
    );
  }
}
