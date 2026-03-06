import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import '../../config/app_theme.dart';
import '../../providers/auth_provider.dart';
import '../../services/supabase_service.dart';
import '../../models/user_model.dart';

class AddLeadScreen extends StatefulWidget {
  const AddLeadScreen({super.key});

  @override
  State<AddLeadScreen> createState() => _AddLeadScreenState();
}

class _AddLeadScreenState extends State<AddLeadScreen> {
  final _formKey = GlobalKey<FormState>();
  bool _isLoading = false;
  List<UserModel> _users = [];

  final _nameController = TextEditingController();
  final _phoneController = TextEditingController();
  final _emailController = TextEditingController();
  final _projectController = TextEditingController();
  final _notesController = TextEditingController();
  final _locationController = TextEditingController();
  final _budgetMinController = TextEditingController();
  final _budgetMaxController = TextEditingController();

  String _selectedSource = 'Website';
  String _selectedStatus = 'NEW';
  String? _selectedAssignedTo;

  final List<String> _sources = [
    'Website',
    'Facebook',
    'Instagram',
    'Google Ads',
    'Referral',
    'Walk-in',
    'Phone Inquiry',
    'Other',
  ];

  final List<String> _statuses = [
    'NEW',
    'CONTACTED',
    'QUALIFIED',
    'FOLLOW_UP',
    'LOST',
  ];

  @override
  void initState() {
    super.initState();
    _loadUsers();
  }

  @override
  void dispose() {
    _nameController.dispose();
    _phoneController.dispose();
    _emailController.dispose();
    _projectController.dispose();
    _notesController.dispose();
    _locationController.dispose();
    _budgetMinController.dispose();
    _budgetMaxController.dispose();
    super.dispose();
  }

  Future<void> _loadUsers() async {
    final users = await SupabaseService.getUsers();
    if (mounted) {
      setState(() => _users = users);
    }
  }

  Future<void> _saveLead() async {
    if (!_formKey.currentState!.validate()) return;

    setState(() => _isLoading = true);

    final authProvider = context.read<AuthProvider>();

    final leadData = {
      'name': _nameController.text.trim(),
      'phone': _phoneController.text.trim(),
      'email': _emailController.text.trim().isEmpty ? null : _emailController.text.trim(),
      'project_name': _projectController.text.trim().isEmpty ? null : _projectController.text.trim(),
      'source': _selectedSource,
      'status': _selectedStatus,
      'notes': _notesController.text.trim().isEmpty ? null : _notesController.text.trim(),
      'preferred_location': _locationController.text.trim().isEmpty ? null : _locationController.text.trim(),
      'budget_min': _budgetMinController.text.trim().isEmpty ? null : double.tryParse(_budgetMinController.text.trim()),
      'budget_max': _budgetMaxController.text.trim().isEmpty ? null : double.tryParse(_budgetMaxController.text.trim()),
      'assigned_to_id': _selectedAssignedTo ?? authProvider.user?.id,
    };

    final result = await SupabaseService.createLead(leadData);

    setState(() => _isLoading = false);

    if (result != null && mounted) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(
          content: Text('Lead created successfully!'),
          backgroundColor: AppTheme.successColor,
        ),
      );
      Navigator.pop(context, true);
    } else if (mounted) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(
          content: Text('Failed to create lead'),
          backgroundColor: AppTheme.errorColor,
        ),
      );
    }
  }

  @override
  Widget build(BuildContext context) {
    final isAdmin = context.read<AuthProvider>().user?.isAdmin ?? false;

    return Scaffold(
      appBar: AppBar(
        title: const Text('Add New Lead'),
        actions: [
          TextButton(
            onPressed: _isLoading ? null : _saveLead,
            child: _isLoading
                ? const SizedBox(
                    width: 20,
                    height: 20,
                    child: CircularProgressIndicator(strokeWidth: 2),
                  )
                : const Text('Save'),
          ),
        ],
      ),
      body: Form(
        key: _formKey,
        child: SingleChildScrollView(
          padding: const EdgeInsets.all(16),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              // Basic Info Section
              _buildSectionHeader('Basic Information'),
              const SizedBox(height: 12),

              TextFormField(
                controller: _nameController,
                decoration: const InputDecoration(
                  labelText: 'Name *',
                  prefixIcon: Icon(Icons.person_outline),
                ),
                validator: (value) {
                  if (value == null || value.trim().isEmpty) {
                    return 'Name is required';
                  }
                  return null;
                },
              ),
              const SizedBox(height: 16),

              TextFormField(
                controller: _phoneController,
                decoration: const InputDecoration(
                  labelText: 'Phone *',
                  prefixIcon: Icon(Icons.phone_outlined),
                ),
                keyboardType: TextInputType.phone,
                validator: (value) {
                  if (value == null || value.trim().isEmpty) {
                    return 'Phone is required';
                  }
                  return null;
                },
              ),
              const SizedBox(height: 16),

              TextFormField(
                controller: _emailController,
                decoration: const InputDecoration(
                  labelText: 'Email',
                  prefixIcon: Icon(Icons.email_outlined),
                ),
                keyboardType: TextInputType.emailAddress,
              ),
              const SizedBox(height: 24),

              // Project & Source Section
              _buildSectionHeader('Lead Details'),
              const SizedBox(height: 12),

              TextFormField(
                controller: _projectController,
                decoration: const InputDecoration(
                  labelText: 'Project Name',
                  prefixIcon: Icon(Icons.apartment_outlined),
                ),
              ),
              const SizedBox(height: 16),

              DropdownButtonFormField<String>(
                value: _selectedSource,
                decoration: const InputDecoration(
                  labelText: 'Source',
                  prefixIcon: Icon(Icons.source_outlined),
                ),
                items: _sources.map((source) {
                  return DropdownMenuItem(value: source, child: Text(source));
                }).toList(),
                onChanged: (value) {
                  if (value != null) setState(() => _selectedSource = value);
                },
              ),
              const SizedBox(height: 16),

              DropdownButtonFormField<String>(
                value: _selectedStatus,
                decoration: const InputDecoration(
                  labelText: 'Status',
                  prefixIcon: Icon(Icons.flag_outlined),
                ),
                items: _statuses.map((status) {
                  return DropdownMenuItem(
                    value: status,
                    child: Text(status.replaceAll('_', ' ')),
                  );
                }).toList(),
                onChanged: (value) {
                  if (value != null) setState(() => _selectedStatus = value);
                },
              ),
              const SizedBox(height: 24),

              // Assignment Section (Admin only)
              if (isAdmin && _users.isNotEmpty) ...[
                _buildSectionHeader('Assignment'),
                const SizedBox(height: 12),
                DropdownButtonFormField<String>(
                  value: _selectedAssignedTo,
                  decoration: const InputDecoration(
                    labelText: 'Assign To',
                    prefixIcon: Icon(Icons.assignment_ind_outlined),
                  ),
                  items: _users.map((user) {
                    return DropdownMenuItem(
                      value: user.id,
                      child: Text(user.name),
                    );
                  }).toList(),
                  onChanged: (value) {
                    setState(() => _selectedAssignedTo = value);
                  },
                ),
                const SizedBox(height: 24),
              ],

              // Budget Section
              _buildSectionHeader('Budget'),
              const SizedBox(height: 12),

              Row(
                children: [
                  Expanded(
                    child: TextFormField(
                      controller: _budgetMinController,
                      decoration: const InputDecoration(
                        labelText: 'Min Budget',
                        prefixIcon: Icon(Icons.currency_rupee),
                      ),
                      keyboardType: TextInputType.number,
                    ),
                  ),
                  const SizedBox(width: 16),
                  Expanded(
                    child: TextFormField(
                      controller: _budgetMaxController,
                      decoration: const InputDecoration(
                        labelText: 'Max Budget',
                        prefixIcon: Icon(Icons.currency_rupee),
                      ),
                      keyboardType: TextInputType.number,
                    ),
                  ),
                ],
              ),
              const SizedBox(height: 24),

              // Location & Notes Section
              _buildSectionHeader('Additional Info'),
              const SizedBox(height: 12),

              TextFormField(
                controller: _locationController,
                decoration: const InputDecoration(
                  labelText: 'Preferred Location',
                  prefixIcon: Icon(Icons.location_on_outlined),
                ),
              ),
              const SizedBox(height: 16),

              TextFormField(
                controller: _notesController,
                decoration: const InputDecoration(
                  labelText: 'Notes',
                  prefixIcon: Icon(Icons.notes_outlined),
                  alignLabelWithHint: true,
                ),
                maxLines: 3,
              ),
              const SizedBox(height: 32),

              // Save Button
              SizedBox(
                width: double.infinity,
                child: ElevatedButton(
                  onPressed: _isLoading ? null : _saveLead,
                  style: ElevatedButton.styleFrom(
                    padding: const EdgeInsets.symmetric(vertical: 16),
                  ),
                  child: _isLoading
                      ? const SizedBox(
                          width: 20,
                          height: 20,
                          child: CircularProgressIndicator(strokeWidth: 2),
                        )
                      : const Text('Create Lead'),
                ),
              ),
              const SizedBox(height: 16),
            ],
          ),
        ),
      ),
    );
  }

  Widget _buildSectionHeader(String title) {
    return Text(
      title,
      style: TextStyle(
        fontSize: 14,
        fontWeight: FontWeight.w600,
        color: AppTheme.primaryColor,
      ),
    );
  }
}
