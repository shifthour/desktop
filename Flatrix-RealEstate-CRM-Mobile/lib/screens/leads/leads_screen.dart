import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'package:url_launcher/url_launcher.dart';
import 'package:font_awesome_flutter/font_awesome_flutter.dart';
import '../../config/app_theme.dart';
import '../../models/lead_model.dart';
import '../../providers/auth_provider.dart';
import '../../services/supabase_service.dart';
import 'lead_detail_screen.dart';

class LeadsScreen extends StatefulWidget {
  const LeadsScreen({super.key});

  @override
  State<LeadsScreen> createState() => _LeadsScreenState();
}

class _LeadsScreenState extends State<LeadsScreen> {
  List<LeadModel> _leads = [];
  bool _isLoading = true;
  String _selectedStatus = 'All';
  final _searchController = TextEditingController();

  final List<String> _statusFilters = [
    'All',
    'NEW',
    'CONTACTED',
    'QUALIFIED',
    'FOLLOW_UP',
    'LOST',
  ];

  @override
  void initState() {
    super.initState();
    _loadLeads();
  }

  @override
  void dispose() {
    _searchController.dispose();
    super.dispose();
  }

  Future<void> _loadLeads() async {
    setState(() => _isLoading = true);

    final authProvider = context.read<AuthProvider>();
    final leads = await SupabaseService.getLeads(
      assignedToId: authProvider.user?.isAdmin == true
          ? null
          : authProvider.user?.id,
      status: _selectedStatus == 'All' ? null : _selectedStatus,
    );

    if (mounted) {
      setState(() {
        _leads = leads;
        _isLoading = false;
      });
    }
  }

  List<LeadModel> get _filteredLeads {
    final searchTerm = _searchController.text.toLowerCase();
    if (searchTerm.isEmpty) return _leads;

    return _leads.where((lead) {
      return lead.displayName.toLowerCase().contains(searchTerm) ||
          (lead.phone?.toLowerCase().contains(searchTerm) ?? false) ||
          (lead.email?.toLowerCase().contains(searchTerm) ?? false) ||
          (lead.projectName?.toLowerCase().contains(searchTerm) ?? false);
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

  @override
  Widget build(BuildContext context) {
    return Column(
        children: [
          // Search & Filters
          Container(
            padding: const EdgeInsets.all(16),
            color: AppTheme.surfaceColor,
            child: Column(
              children: [
                // Search Bar
                TextField(
                  controller: _searchController,
                  decoration: InputDecoration(
                    hintText: 'Search leads...',
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
                const SizedBox(height: 12),

                // Status Filter Chips
                SizedBox(
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
                          _loadLeads();
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
              ],
            ),
          ),

          // Leads List
          Expanded(
            child: RefreshIndicator(
              onRefresh: _loadLeads,
              child: _isLoading
                  ? const Center(child: CircularProgressIndicator())
                  : _filteredLeads.isEmpty
                      ? Center(
                          child: Column(
                            mainAxisAlignment: MainAxisAlignment.center,
                            children: [
                              Icon(
                                Icons.people_outline,
                                size: 64,
                                color: AppTheme.textMuted,
                              ),
                              const SizedBox(height: 16),
                              Text(
                                'No leads found',
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
                          itemCount: _filteredLeads.length,
                          itemBuilder: (context, index) {
                            return _buildLeadCard(_filteredLeads[index]);
                          },
                        ),
            ),
          ),
        ],
      );
  }

  Widget _buildLeadCard(LeadModel lead) {
    final statusColor = AppTheme.leadStatusColors[lead.status] ?? Colors.grey;

    return Card(
      margin: const EdgeInsets.only(bottom: 12),
      child: InkWell(
        onTap: () {
          Navigator.push(
            context,
            MaterialPageRoute(
              builder: (context) => LeadDetailScreen(lead: lead),
            ),
          ).then((_) => _loadLeads());
        },
        borderRadius: BorderRadius.circular(12),
        child: Padding(
          padding: const EdgeInsets.all(16),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              // Header Row
              Row(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  // Avatar
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

                  // Name & Project
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

                  // Status Badge
                  Container(
                    padding:
                        const EdgeInsets.symmetric(horizontal: 10, vertical: 4),
                    decoration: BoxDecoration(
                      color: statusColor.withOpacity(0.1),
                      borderRadius: BorderRadius.circular(20),
                    ),
                    child: Text(
                      lead.status.replaceAll('_', ' '),
                      style: TextStyle(
                        fontSize: 11,
                        color: statusColor,
                        fontWeight: FontWeight.w600,
                      ),
                    ),
                  ),
                ],
              ),

              const SizedBox(height: 12),

              // Info Row
              Row(
                children: [
                  if (lead.source != null) ...[
                    Icon(Icons.source_outlined,
                        size: 14, color: AppTheme.textMuted),
                    const SizedBox(width: 4),
                    Text(
                      lead.source!,
                      style: TextStyle(
                          fontSize: 12, color: AppTheme.textSecondary),
                    ),
                    const SizedBox(width: 16),
                  ],
                  if (lead.assignedToName != null) ...[
                    Icon(Icons.person_outline,
                        size: 14, color: AppTheme.textMuted),
                    const SizedBox(width: 4),
                    Text(
                      lead.assignedToName!,
                      style: TextStyle(
                          fontSize: 12, color: AppTheme.textSecondary),
                    ),
                  ],
                ],
              ),

              // Notes/Comments Section
              if (lead.notes != null && lead.notes!.isNotEmpty) ...[
                const SizedBox(height: 12),
                Container(
                  width: double.infinity,
                  padding: const EdgeInsets.all(10),
                  decoration: BoxDecoration(
                    color: Colors.amber.withOpacity(0.08),
                    borderRadius: BorderRadius.circular(8),
                    border: Border.all(
                      color: Colors.amber.withOpacity(0.3),
                      width: 1,
                    ),
                  ),
                  child: Row(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Icon(
                        Icons.note_outlined,
                        size: 16,
                        color: Colors.amber[700],
                      ),
                      const SizedBox(width: 8),
                      Expanded(
                        child: Text(
                          lead.notes!,
                          style: TextStyle(
                            fontSize: 12,
                            color: AppTheme.textSecondary,
                            height: 1.4,
                          ),
                          maxLines: 2,
                          overflow: TextOverflow.ellipsis,
                        ),
                      ),
                    ],
                  ),
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
                      icon: FontAwesomeIcons.whatsapp,
                      color: const Color(0xFF25D366),
                      onTap: () => _openWhatsApp(lead.phone!),
                    ),
                  ],
                  if (lead.email != null)
                    _buildActionButton(
                      icon: Icons.email,
                      color: Colors.orange,
                      onTap: () async {
                        final uri = Uri.parse('mailto:${lead.email}');
                        if (await canLaunchUrl(uri)) {
                          await launchUrl(uri);
                        }
                      },
                    ),
                  _buildActionButton(
                    icon: Icons.edit,
                    color: Colors.grey,
                    onTap: () {
                      Navigator.push(
                        context,
                        MaterialPageRoute(
                          builder: (context) => LeadDetailScreen(lead: lead),
                        ),
                      ).then((_) => _loadLeads());
                    },
                  ),
                ],
              ),
            ],
          ),
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
