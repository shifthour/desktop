// employeer_model.dart - Ensure this file exists and has the following:

import 'package:supabase_flutter/supabase_flutter.dart';

class Employer {
  final String id;
  final String companyName;
  final String website;
  final String contactName;
  final String contactEmail;
  final String contactPhone;
  final DateTime? createdAt;
  final DateTime? updatedAt;
  final String? profileImageUrl;

  Employer({
    required this.id,
    required this.companyName,
    required this.website,
    required this.contactName,
    required this.contactEmail,
    required this.contactPhone,
    this.createdAt,
    this.updatedAt,
    this.profileImageUrl,
  });

  factory Employer.fromJson(Map<String, dynamic> json) {
    return Employer(
      id: json['id'] ?? '',
      companyName: json['company_name'] ?? '',
      website: json['website'] ?? '',
      contactName: json['contact_name'] ?? '',
      contactEmail: json['contact_email'] ?? '',
      contactPhone: json['contact_phone'] ?? '',
      createdAt:
          json['created_at'] != null
              ? DateTime.parse(json['created_at'])
              : null,
      updatedAt:
          json['updated_at'] != null
              ? DateTime.parse(json['updated_at'])
              : null,
      profileImageUrl: json['profile_image_url'],
    );
  }
}

class EmployerService {
  final SupabaseClient _supabase;

  EmployerService(this._supabase);

  // Get employer data for the currently logged in user
  Future<Employer?> getCurrentEmployer() async {
    try {
      final user = _supabase.auth.currentUser;
      if (user == null) return null;

      // First try to find the employer by user_id
      var response =
          await _supabase
              .from('employers')
              .select()
              .eq('user_id', user.id)
              .maybeSingle();

      // If not found, try by the email
      if (response == null && user.email != null) {
        response =
            await _supabase
                .from('employers')
                .select()
                .eq('contact_email', user.email as Object)
                .maybeSingle();
      }

      if (response != null) {
        return Employer.fromJson(response);
      }

      // Fallback to default data for demo purposes
      // In a production app, you would return null or handle differently
      print(
        'No employer profile found for user ${user.id}. Using fallback data.',
      );
      return null;
    } catch (e) {
      print('Error fetching employer data: $e');
      return null;
    }
  }

  Future<Employer?> getEmployerById(String id) async {
    try {
      final response =
          await _supabase.from('employers').select().eq('id', id).single();

      return Employer.fromJson(response);
    } catch (e) {
      print('Error fetching employer by ID: $e');
      return null;
    }
  }

  Future<Employer?> getEmployerByContactName(String name) async {
    try {
      final response =
          await _supabase
              .from('employers')
              .select()
              .eq('contact_name', name)
              .maybeSingle();

      if (response != null) {
        return Employer.fromJson(response);
      }
      return null;
    } catch (e) {
      print('Error fetching employer by contact name: $e');
      return null;
    }
  }

  Future<Employer?> getEmployerByEmail(String email) async {
    try {
      final response =
          await _supabase
              .from('employers')
              .select()
              .eq('contact_email', email)
              .maybeSingle();

      if (response != null) {
        return Employer.fromJson(response);
      }
      return null;
    } catch (e) {
      print('Error fetching employer by email: $e');
      return null;
    }
  }

  // Create or update employer profile
  Future<Employer?> createOrUpdateEmployer(Map<String, dynamic> data) async {
    try {
      final user = _supabase.auth.currentUser;
      if (user == null) throw Exception('User not authenticated');

      // Add user_id to data if not present
      if (!data.containsKey('user_id')) {
        data['user_id'] = user.id;
      }

      // Check if employer exists
      final existingEmployer = await getCurrentEmployer();

      if (existingEmployer != null) {
        // Update existing employer
        final response =
            await _supabase
                .from('employers')
                .update(data)
                .eq('id', existingEmployer.id)
                .single();

        return Employer.fromJson(response);
      } else {
        // Create new employer
        final response =
            await _supabase.from('employers').insert(data).select().single();

        return Employer.fromJson(response);
      }
    } catch (e) {
      print('Error creating/updating employer: $e');
      return null;
    }
  }
}
