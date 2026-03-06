import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'package:url_launcher/url_launcher.dart';
import 'package:intl/intl.dart';
import 'package:font_awesome_flutter/font_awesome_flutter.dart';
import '../../config/app_theme.dart';
import '../../models/lead_model.dart';
import '../../models/comment_model.dart';
import '../../providers/auth_provider.dart';
import '../../services/supabase_service.dart';

class LeadDetailScreen extends StatefulWidget {
  final LeadModel lead;

  const LeadDetailScreen({super.key, required this.lead});

  @override
  State<LeadDetailScreen> createState() => _LeadDetailScreenState();
}

class _LeadDetailScreenState extends State<LeadDetailScreen> {
  late LeadModel _lead;
  bool _isUpdating = false;
  List<CommentModel> _comments = [];
  bool _isLoadingComments = true;
  final _commentController = TextEditingController();
  bool _isAddingComment = false;

  final List<String> _statusOptions = [
    'NEW',
    'CONTACTED',
    'QUALIFIED',
    'FOLLOW_UP',
    'LOST',
  ];

  @override
  void initState() {
    super.initState();
    _lead = widget.lead;
    _loadComments();
  }

  @override
  void dispose() {
    _commentController.dispose();
    super.dispose();
  }

  Future<void> _loadComments() async {
    setState(() => _isLoadingComments = true);
    // Parse comments from lead's notes field
    final comments = SupabaseService.parseNotesToComments(_lead.notes, _lead.id);
    if (mounted) {
      setState(() {
        _comments = comments;
        _isLoadingComments = false;
      });
    }
  }

  Future<void> _refreshLead() async {
    // Refresh lead data to get updated notes
    final updatedLead = await SupabaseService.getLeadById(_lead.id);
    if (updatedLead != null && mounted) {
      setState(() {
        _lead = updatedLead;
      });
      _loadComments();
    }
  }

  Future<void> _addComment() async {
    final commentText = _commentController.text.trim();
    if (commentText.isEmpty) return;

    setState(() => _isAddingComment = true);

    final success = await SupabaseService.addCommentToLead(
      leadId: _lead.id,
      comment: commentText,
    );

    if (success && mounted) {
      _commentController.clear();
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('Comment added successfully')),
      );
      // Refresh lead to get updated notes
      await _refreshLead();
    } else if (mounted) {
      final errorMsg = SupabaseService.lastCommentError ?? 'Unknown error';
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('Failed to add comment: $errorMsg'),
          duration: const Duration(seconds: 5),
        ),
      );
    }

    setState(() => _isAddingComment = false);
  }

  Future<void> _updateStatus(String newStatus) async {
    setState(() => _isUpdating = true);

    final success = await SupabaseService.updateLead(_lead.id, {
      'status': newStatus,
    });

    if (success) {
      setState(() {
        _lead = _lead.copyWith(status: newStatus);
      });
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('Status updated successfully')),
        );
      }
    } else {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('Failed to update status')),
        );
      }
    }

    setState(() => _isUpdating = false);
  }

  Future<void> _makePhoneCall() async {
    if (_lead.phone == null) return;
    final uri = Uri.parse('tel:${_lead.phone}');
    try {
      await launchUrl(uri);
    } catch (e) {
      print('Error making phone call: $e');
    }
  }

  Future<void> _openWhatsApp() async {
    if (_lead.phone == null) return;
    final cleanPhone = _lead.phone!.replaceAll(RegExp(r'[^0-9]'), '');
    final phoneWithCode = cleanPhone.startsWith('91') ? cleanPhone : '91$cleanPhone';
    final uri = Uri.parse('https://wa.me/$phoneWithCode');
    try {
      await launchUrl(uri, mode: LaunchMode.externalApplication);
    } catch (e) {
      final intentUri = Uri.parse('whatsapp://send?phone=$phoneWithCode');
      await launchUrl(intentUri);
    }
  }

  Future<void> _sendEmail() async {
    if (_lead.email == null) return;
    final uri = Uri.parse('mailto:${_lead.email}');
    try {
      await launchUrl(uri);
    } catch (e) {
      print('Error sending email: $e');
    }
  }

  @override
  Widget build(BuildContext context) {
    final statusColor =
        AppTheme.leadStatusColors[_lead.status] ?? Colors.grey;

    return Scaffold(
      appBar: AppBar(
        title: const Text('Lead Details'),
        actions: [
          if (_isUpdating)
            const Padding(
              padding: EdgeInsets.all(16),
              child: SizedBox(
                width: 20,
                height: 20,
                child: CircularProgressIndicator(strokeWidth: 2),
              ),
            ),
        ],
      ),
      body: SingleChildScrollView(
        child: Column(
          children: [
            // Header Card
            Container(
              width: double.infinity,
              padding: const EdgeInsets.all(20),
              color: AppTheme.surfaceColor,
              child: Column(
                children: [
                  // Avatar
                  CircleAvatar(
                    radius: 40,
                    backgroundColor: AppTheme.primaryColor.withOpacity(0.1),
                    child: Text(
                      _lead.displayName[0].toUpperCase(),
                      style: TextStyle(
                        fontSize: 32,
                        color: AppTheme.primaryColor,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                  ),
                  const SizedBox(height: 12),

                  // Project Name
                  if (_lead.projectName != null)
                    Text(
                      _lead.projectName!,
                      style: TextStyle(
                        fontSize: 14,
                        color: AppTheme.primaryColor,
                        fontWeight: FontWeight.w600,
                      ),
                    ),

                  // Name
                  Text(
                    _lead.displayName,
                    style: const TextStyle(
                      fontSize: 24,
                      fontWeight: FontWeight.bold,
                    ),
                  ),
                  const SizedBox(height: 8),

                  // Status Badge
                  Container(
                    padding: const EdgeInsets.symmetric(
                        horizontal: 16, vertical: 6),
                    decoration: BoxDecoration(
                      color: statusColor.withOpacity(0.1),
                      borderRadius: BorderRadius.circular(20),
                    ),
                    child: Text(
                      _lead.status.replaceAll('_', ' '),
                      style: TextStyle(
                        color: statusColor,
                        fontWeight: FontWeight.w600,
                      ),
                    ),
                  ),
                  const SizedBox(height: 20),

                  // Action Buttons
                  Row(
                    mainAxisAlignment: MainAxisAlignment.center,
                    children: [
                      if (_lead.phone != null) ...[
                        _buildCircleButton(
                          icon: Icons.phone,
                          color: Colors.blue,
                          onTap: _makePhoneCall,
                        ),
                        const SizedBox(width: 16),
                        _buildCircleButton(
                          icon: FontAwesomeIcons.whatsapp,
                          color: const Color(0xFF25D366),
                          onTap: _openWhatsApp,
                        ),
                        const SizedBox(width: 16),
                      ],
                      if (_lead.email != null)
                        _buildCircleButton(
                          icon: Icons.email,
                          color: Colors.orange,
                          onTap: _sendEmail,
                        ),
                    ],
                  ),
                ],
              ),
            ),

            const SizedBox(height: 8),

            // Details Section
            Container(
              padding: const EdgeInsets.all(16),
              color: AppTheme.surfaceColor,
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    'Contact Information',
                    style: Theme.of(context).textTheme.titleMedium?.copyWith(
                          fontWeight: FontWeight.bold,
                        ),
                  ),
                  const SizedBox(height: 16),
                  _buildDetailRow(Icons.phone, 'Phone', _lead.phone ?? 'N/A'),
                  _buildDetailRow(Icons.email, 'Email', _lead.email ?? 'N/A'),
                  _buildDetailRow(
                      Icons.source, 'Source', _lead.source ?? 'N/A'),
                  _buildDetailRow(Icons.person, 'Assigned To',
                      _lead.assignedToName ?? 'Unassigned'),
                ],
              ),
            ),

            const SizedBox(height: 8),

            // Status Update Section
            Container(
              padding: const EdgeInsets.all(16),
              color: AppTheme.surfaceColor,
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    'Update Status',
                    style: Theme.of(context).textTheme.titleMedium?.copyWith(
                          fontWeight: FontWeight.bold,
                        ),
                  ),
                  const SizedBox(height: 16),
                  Wrap(
                    spacing: 8,
                    runSpacing: 8,
                    children: _statusOptions.map((status) {
                      final isSelected = _lead.status == status;
                      final color =
                          AppTheme.leadStatusColors[status] ?? Colors.grey;
                      return ChoiceChip(
                        label: Text(status.replaceAll('_', ' ')),
                        selected: isSelected,
                        onSelected: (selected) {
                          if (selected && !_isUpdating) {
                            _updateStatus(status);
                          }
                        },
                        selectedColor: color.withOpacity(0.2),
                        labelStyle: TextStyle(
                          color: isSelected ? color : AppTheme.textSecondary,
                          fontWeight: isSelected
                              ? FontWeight.w600
                              : FontWeight.normal,
                        ),
                      );
                    }).toList(),
                  ),
                ],
              ),
            ),

            const SizedBox(height: 8),

            // Comments Section
            Container(
              padding: const EdgeInsets.all(16),
              color: AppTheme.surfaceColor,
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Row(
                    mainAxisAlignment: MainAxisAlignment.spaceBetween,
                    children: [
                      Text(
                        'Comments',
                        style: Theme.of(context).textTheme.titleMedium?.copyWith(
                              fontWeight: FontWeight.bold,
                            ),
                      ),
                      Text(
                        '${_comments.length}',
                        style: TextStyle(
                          color: AppTheme.textSecondary,
                          fontWeight: FontWeight.w600,
                        ),
                      ),
                    ],
                  ),
                  const SizedBox(height: 16),

                  // Add Comment Input
                  Row(
                    crossAxisAlignment: CrossAxisAlignment.end,
                    children: [
                      Expanded(
                        child: TextField(
                          controller: _commentController,
                          decoration: InputDecoration(
                            hintText: 'Add a comment...',
                            contentPadding: const EdgeInsets.symmetric(
                              horizontal: 16,
                              vertical: 12,
                            ),
                            border: OutlineInputBorder(
                              borderRadius: BorderRadius.circular(12),
                            ),
                          ),
                          maxLines: 3,
                          minLines: 1,
                        ),
                      ),
                      const SizedBox(width: 12),
                      SizedBox(
                        height: 48,
                        child: ElevatedButton(
                          onPressed: _isAddingComment ? null : _addComment,
                          style: ElevatedButton.styleFrom(
                            shape: RoundedRectangleBorder(
                              borderRadius: BorderRadius.circular(12),
                            ),
                          ),
                          child: _isAddingComment
                              ? const SizedBox(
                                  width: 20,
                                  height: 20,
                                  child: CircularProgressIndicator(
                                    strokeWidth: 2,
                                    color: Colors.white,
                                  ),
                                )
                              : const Icon(Icons.send),
                        ),
                      ),
                    ],
                  ),

                  const SizedBox(height: 16),

                  // Comments List
                  if (_isLoadingComments)
                    const Center(
                      child: Padding(
                        padding: EdgeInsets.all(20),
                        child: CircularProgressIndicator(),
                      ),
                    )
                  else if (_comments.isEmpty)
                    Container(
                      padding: const EdgeInsets.all(20),
                      alignment: Alignment.center,
                      child: Column(
                        children: [
                          Icon(
                            Icons.chat_bubble_outline,
                            size: 40,
                            color: AppTheme.textMuted,
                          ),
                          const SizedBox(height: 8),
                          Text(
                            'No comments yet',
                            style: TextStyle(
                              color: AppTheme.textSecondary,
                            ),
                          ),
                        ],
                      ),
                    )
                  else
                    ListView.separated(
                      shrinkWrap: true,
                      physics: const NeverScrollableScrollPhysics(),
                      itemCount: _comments.length,
                      separatorBuilder: (_, __) => const Divider(height: 24),
                      itemBuilder: (context, index) {
                        return _buildCommentItem(_comments[index]);
                      },
                    ),
                ],
              ),
            ),

            const SizedBox(height: 8),

            // Additional Info
            if (_lead.preferredType != null ||
                _lead.budgetMin != null ||
                _lead.preferredLocation != null)
              Container(
                padding: const EdgeInsets.all(16),
                color: AppTheme.surfaceColor,
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(
                      'Preferences',
                      style: Theme.of(context).textTheme.titleMedium?.copyWith(
                            fontWeight: FontWeight.bold,
                          ),
                    ),
                    const SizedBox(height: 16),
                    if (_lead.preferredType != null)
                      _buildDetailRow(Icons.home, 'Preferred Type',
                          _lead.preferredType!),
                    if (_lead.budgetMin != null)
                      _buildDetailRow(Icons.currency_rupee, 'Budget',
                          '₹${NumberFormat('#,##,###').format(_lead.budgetMin)}'),
                    if (_lead.preferredLocation != null)
                      _buildDetailRow(Icons.location_on, 'Preferred Location',
                          _lead.preferredLocation!),
                  ],
                ),
              ),

            const SizedBox(height: 8),

            // Notes Section
            if (_lead.notes != null && _lead.notes!.isNotEmpty)
              Container(
                padding: const EdgeInsets.all(16),
                color: AppTheme.surfaceColor,
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(
                      'Notes',
                      style: Theme.of(context).textTheme.titleMedium?.copyWith(
                            fontWeight: FontWeight.bold,
                          ),
                    ),
                    const SizedBox(height: 12),
                    Container(
                      width: double.infinity,
                      padding: const EdgeInsets.all(12),
                      decoration: BoxDecoration(
                        color: Colors.grey[100],
                        borderRadius: BorderRadius.circular(8),
                      ),
                      child: Text(
                        _lead.notes!,
                        style: TextStyle(
                          color: AppTheme.textSecondary,
                          height: 1.5,
                        ),
                      ),
                    ),
                  ],
                ),
              ),

            const SizedBox(height: 8),

            // Dates Section
            Container(
              padding: const EdgeInsets.all(16),
              color: AppTheme.surfaceColor,
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    'Timeline',
                    style: Theme.of(context).textTheme.titleMedium?.copyWith(
                          fontWeight: FontWeight.bold,
                        ),
                  ),
                  const SizedBox(height: 16),
                  _buildDetailRow(
                    Icons.calendar_today,
                    'Created',
                    _lead.createdAt != null
                        ? DateFormat('MMM d, yyyy h:mm a')
                            .format(_lead.createdAt!)
                        : 'N/A',
                  ),
                  if (_lead.lastContactedAt != null)
                    _buildDetailRow(
                      Icons.phone_callback,
                      'Last Contacted',
                      DateFormat('MMM d, yyyy').format(_lead.lastContactedAt!),
                    ),
                  if (_lead.nextFollowupDate != null)
                    _buildDetailRow(
                      Icons.event,
                      'Next Follow-up',
                      DateFormat('MMM d, yyyy h:mm a')
                          .format(_lead.nextFollowupDate!),
                    ),
                ],
              ),
            ),

            const SizedBox(height: 24),
          ],
        ),
      ),
    );
  }

  Widget _buildCommentItem(CommentModel comment) {
    return Row(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Container(
          width: 36,
          height: 36,
          decoration: BoxDecoration(
            color: AppTheme.primaryColor.withOpacity(0.1),
            borderRadius: BorderRadius.circular(8),
          ),
          child: Icon(
            Icons.comment_outlined,
            color: AppTheme.primaryColor,
            size: 18,
          ),
        ),
        const SizedBox(width: 12),
        Expanded(
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              if (comment.createdAt != null)
                Text(
                  DateFormat('MMM d, yyyy • h:mm a').format(comment.createdAt!),
                  style: TextStyle(
                    color: AppTheme.textMuted,
                    fontSize: 12,
                    fontWeight: FontWeight.w500,
                  ),
                ),
              const SizedBox(height: 4),
              Text(
                comment.comment,
                style: TextStyle(
                  color: AppTheme.textPrimary,
                  height: 1.4,
                  fontSize: 14,
                ),
              ),
            ],
          ),
        ),
      ],
    );
  }

  Widget _buildCircleButton({
    required IconData icon,
    required Color color,
    required VoidCallback onTap,
  }) {
    return InkWell(
      onTap: onTap,
      borderRadius: BorderRadius.circular(30),
      child: Container(
        padding: const EdgeInsets.all(14),
        decoration: BoxDecoration(
          color: color.withOpacity(0.1),
          shape: BoxShape.circle,
        ),
        child: Icon(icon, color: color, size: 24),
      ),
    );
  }

  Widget _buildDetailRow(IconData icon, String label, String value) {
    return Padding(
      padding: const EdgeInsets.only(bottom: 12),
      child: Row(
        children: [
          Icon(icon, size: 20, color: AppTheme.textMuted),
          const SizedBox(width: 12),
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(
                  label,
                  style: TextStyle(
                    fontSize: 12,
                    color: AppTheme.textMuted,
                  ),
                ),
                Text(
                  value,
                  style: const TextStyle(
                    fontSize: 15,
                    fontWeight: FontWeight.w500,
                  ),
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }
}
