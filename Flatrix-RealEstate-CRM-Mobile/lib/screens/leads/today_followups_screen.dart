import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'package:url_launcher/url_launcher.dart';
import 'package:intl/intl.dart';
import 'package:font_awesome_flutter/font_awesome_flutter.dart';
import '../../config/app_theme.dart';
import '../../models/lead_model.dart';
import '../../providers/auth_provider.dart';
import '../../services/supabase_service.dart';
import 'lead_detail_screen.dart';

class TodayFollowupsScreen extends StatefulWidget {
  const TodayFollowupsScreen({super.key});

  @override
  State<TodayFollowupsScreen> createState() => _TodayFollowupsScreenState();
}

class _TodayFollowupsScreenState extends State<TodayFollowupsScreen> {
  List<LeadModel> _leads = [];
  bool _isLoading = true;

  @override
  void initState() {
    super.initState();
    _loadTodayFollowups();
  }

  Future<void> _loadTodayFollowups() async {
    setState(() => _isLoading = true);

    final authProvider = context.read<AuthProvider>();
    final today = DateTime.now();

    final leads = await SupabaseService.getLeads(
      assignedToId: authProvider.user?.isAdmin == true
          ? null
          : authProvider.user?.id,
      followUpDate: today,
    );

    if (mounted) {
      setState(() {
        _leads = leads;
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

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text("Today's Follow-ups"),
      ),
      body: RefreshIndicator(
        onRefresh: _loadTodayFollowups,
        child: _isLoading
            ? const Center(child: CircularProgressIndicator())
            : _leads.isEmpty
                ? Center(
                    child: Column(
                      mainAxisAlignment: MainAxisAlignment.center,
                      children: [
                        Icon(
                          Icons.event_available,
                          size: 64,
                          color: AppTheme.successColor.withOpacity(0.5),
                        ),
                        const SizedBox(height: 16),
                        Text(
                          'No follow-ups scheduled for today',
                          style: TextStyle(
                            color: AppTheme.textSecondary,
                            fontSize: 16,
                          ),
                        ),
                        const SizedBox(height: 8),
                        Text(
                          DateFormat('EEEE, MMMM d, yyyy').format(DateTime.now()),
                          style: TextStyle(
                            color: AppTheme.textMuted,
                            fontSize: 14,
                          ),
                        ),
                      ],
                    ),
                  )
                : Column(
                    children: [
                      // Header
                      Container(
                        padding: const EdgeInsets.all(16),
                        color: Colors.orange.withOpacity(0.1),
                        child: Row(
                          children: [
                            const Icon(Icons.calendar_today, color: Colors.orange),
                            const SizedBox(width: 12),
                            Expanded(
                              child: Column(
                                crossAxisAlignment: CrossAxisAlignment.start,
                                children: [
                                  Text(
                                    DateFormat('EEEE, MMMM d, yyyy').format(DateTime.now()),
                                    style: const TextStyle(
                                      fontWeight: FontWeight.w600,
                                      fontSize: 16,
                                    ),
                                  ),
                                  Text(
                                    '${_leads.length} follow-up${_leads.length == 1 ? '' : 's'} scheduled',
                                    style: TextStyle(
                                      color: AppTheme.textSecondary,
                                      fontSize: 13,
                                    ),
                                  ),
                                ],
                              ),
                            ),
                          ],
                        ),
                      ),

                      // List
                      Expanded(
                        child: ListView.builder(
                          padding: const EdgeInsets.all(16),
                          itemCount: _leads.length,
                          itemBuilder: (context, index) {
                            return _buildLeadCard(_leads[index]);
                          },
                        ),
                      ),
                    ],
                  ),
      ),
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
          ).then((_) => _loadTodayFollowups());
        },
        borderRadius: BorderRadius.circular(12),
        child: Padding(
          padding: const EdgeInsets.all(16),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Row(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  CircleAvatar(
                    backgroundColor: Colors.orange.withOpacity(0.1),
                    child: Text(
                      lead.displayName[0].toUpperCase(),
                      style: const TextStyle(
                        color: Colors.orange,
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
                  Container(
                    padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 4),
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

              if (lead.nextFollowupDate != null) ...[
                const SizedBox(height: 12),
                Container(
                  padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
                  decoration: BoxDecoration(
                    color: Colors.orange.withOpacity(0.05),
                    borderRadius: BorderRadius.circular(8),
                  ),
                  child: Row(
                    children: [
                      const Icon(Icons.access_time, size: 16, color: Colors.orange),
                      const SizedBox(width: 8),
                      Text(
                        'Follow-up at ${DateFormat('h:mm a').format(lead.nextFollowupDate!)}',
                        style: const TextStyle(
                          fontSize: 13,
                          fontWeight: FontWeight.w500,
                          color: Colors.orange,
                        ),
                      ),
                    ],
                  ),
                ),
              ],

              const SizedBox(height: 12),
              const Divider(height: 1),
              const SizedBox(height: 12),

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
                  _buildActionButton(
                    icon: Icons.edit,
                    color: Colors.grey,
                    onTap: () {
                      Navigator.push(
                        context,
                        MaterialPageRoute(
                          builder: (context) => LeadDetailScreen(lead: lead),
                        ),
                      ).then((_) => _loadTodayFollowups());
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
