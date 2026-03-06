import 'dart:ui';
import 'package:flutter/material.dart';
import 'package:supabase_flutter/supabase_flutter.dart';

class DatabaseService {
  final SupabaseClient _supabase = Supabase.instance.client;
  Future<Map<String, dynamic>?> getShiftById(String shiftId) async {
    try {
      print('Attempting to fetch shift with Shift ID: $shiftId');

      // Directly query using shift_id column
      final response =
          await _supabase
              .from('worker_job_applications')
              .select('*')
              .eq('shift_id', shiftId)
              .single();

      if (response == null) {
        print('No shift found with Shift ID: $shiftId');
        return null;
      }

      print('Shift found: $response'); // Debug print

      final Map<String, dynamic> shiftData = {
        'id': response['id'] ?? '',
        'jobTitle': response['job_title'] ?? 'Untitled Job',
        'company': response['company'] ?? 'Unknown Company',
        'companyLogo':
            response['company_logo'] ??
            (response['company']?.toString().substring(0, 2) ?? 'SC'),
        'logoColor': response['logo_color'] ?? '#F59E0B',
        'status': response['status'] ?? 'Upcoming',
        'statusColor': _getStatusColor(response['status']),
        'location': {
          'address': response['location'] ?? 'No location provided',
          'coordinates': {'lat': 0, 'lng': 0},
        },
        'date': response['date']?.toString() ?? 'Date not specified',
        'startTime': _formatTime(response['start_time']),
        'endTime': _formatTime(response['end_time']),
        'duration':
            response['duration'] ??
            _calculateDuration(response['start_time'], response['end_time']),
        'payRate': '₹${response['pay_rate']?.toStringAsFixed(2) ?? '0.00'}/Day',
        'jobCategory': response['job_category'] ?? 'Unspecified',
        'supervisor': {
          'name': response['supervisor_name'] ?? 'Not assigned',
          'phone': response['supervisor_phone'] ?? 'Not provided',
        },
        'description': response['description'] ?? 'No description available',
        'requiredSkills': _parseSkills(response['required_skills']),
        'applied_status':
            response['application_status'] ??
            response['application_status'] ??
            'Pending',
        'checkInTime':
            _formatTime(response['check_in_time']) ?? 'Not checked in',
        'checkInStatus': response['check_in_status'] ?? 'Not Started',
        'dressCode': response['dress_code'] ?? 'Professional attire',
        'breakInfo': response['break_info'] ?? 'Standard break periods',
        'safetyInstructions':
            response['safety_instructions'] ??
            'Follow workplace safety guidelines',
        'specialNotes': response['special_notes'] ?? '',
        'timeline': [], // You might want to fetch this separately if needed
      };

      return shiftData;
    } catch (e) {
      print('Detailed error in getShiftById: $e');
      return null;
    }
  }

  List<String> _parseSkills(dynamic skills) {
    if (skills == null) return [];

    if (skills is String) {
      // If skills are stored as a comma-separated string
      return skills.split(',').map((skill) => skill.trim()).toList();
    }

    if (skills is List) {
      return skills.map((skill) => skill.toString()).toList();
    }

    return [];
  }

  // Existing helper methods remain the same
  String _formatTime(dynamic timeString) {
    if (timeString == null || timeString.toString().isEmpty) return 'Not set';
    try {
      final time = DateTime.parse('2000-01-01 $timeString');
      return '${time.hour}:${time.minute.toString().padLeft(2, '0')} ${time.hour >= 12 ? 'PM' : 'AM'}';
    } catch (e) {
      return timeString.toString();
    }
  }

  String _calculateDuration(dynamic startTime, dynamic endTime) {
    if (startTime == null || endTime == null) return 'N/A';
    try {
      final start = DateTime.parse('2000-01-01 $startTime');
      final end = DateTime.parse('2000-01-01 $endTime');
      final difference = end.difference(start);
      return '${difference.inHours} hours';
    } catch (e) {
      return 'N/A';
    }
  }

  Color _getStatusColor(String? status) {
    switch (status) {
      case 'Upcoming':
      case 'Confirmed':
        return const Color(0xFF4F46E5); // indigo-600
      case 'In Progress':
        return const Color(0xFFF59E0B); // amber-500
      case 'Completed':
        return const Color(0xFF10B981); // emerald-500
      case 'Cancelled':
        return Colors.grey; // slate-500
      default:
        return const Color(0xFF4F46E5);
    }
  }

  String _generateCompanyLogo(dynamic company) {
    if (company == null) return 'SC';
    return company.toString().length >= 2
        ? company.toString().substring(0, 2).toUpperCase()
        : company.toString().toUpperCase();
  }

  // Null-safe pay rate formatting
  String _formatPayRate(dynamic payRate) {
    try {
      return '₹${double.parse(payRate.toString()).toStringAsFixed(2)}/Day';
    } catch (e) {
      return '₹0.00/hr';
    }
  }

  Future<bool> cancelShift(String shiftId) async {
    try {
      // Get current user
      final user = Supabase.instance.client.auth.currentUser;
      if (user == null) {
        throw Exception('No authenticated user found');
      }

      // Check if shift can be cancelled (before 24 hours)
      final shiftResponse =
          await Supabase.instance.client
              .from('worker_job_applications')
              .select('date, start_time')
              .eq('shift_id', shiftId)
              .eq('user_id', user.id)
              .single();

      if (shiftResponse == null) {
        throw Exception('Shift not found');
      }

      // Parse shift date and time
      final shiftDate = DateTime.parse(shiftResponse['date']);
      final shiftTime = _parseTime(shiftResponse['start_time']);
      final shiftDateTime = DateTime(
        shiftDate.year,
        shiftDate.month,
        shiftDate.day,
        shiftTime.hour,
        shiftTime.minute,
      );

      // Check if shift is more than 24 hours away
      final now = DateTime.now();
      final difference = shiftDateTime.difference(now);

      if (difference.inHours < 24) {
        throw Exception('Cannot cancel shift less than 24 hours before start');
      }

      // Delete the shift record
      await Supabase.instance.client
          .from('worker_job_applications')
          .delete()
          .eq('shift_id', shiftId)
          .eq('user_id', user.id);

      return true;
    } catch (e) {
      print('Error cancelling shift: $e');
      rethrow;
    }
  }

  // Helper method to parse time
  TimeOfDay _parseTime(String timeString) {
    try {
      final parts = timeString.split(':');
      return TimeOfDay(
        hour: int.parse(parts[0]),
        minute: int.parse(parts[1].split(' ')[0]),
      );
    } catch (e) {
      print('Error parsing time: $e');
      return TimeOfDay.now();
    }
  }

  // Generate description with null safety
  String _generateDescription(Map<String, dynamic> response) {
    final company = response['company'] ?? 'Unknown Company';
    final jobTitle = response['job_title'] ?? 'Job';
    return '$jobTitle position at $company';
  }

  Future<bool> submitFeedback({
    required String jobListingId,
    required String userId,
    required int rating,
    String? comments,
  }) async {
    try {
      print('Attempting to submit feedback:');
      print('Job ID: $jobListingId');
      print('User ID: $userId');
      print('Rating: $rating');
      print('Comments: $comments');

      // Check for empty values
      if (jobListingId.isEmpty) {
        print('Error: Empty job listing ID');
        throw Exception('Invalid job listing ID');
      }

      if (userId.isEmpty) {
        print('Error: Empty user ID');
        throw Exception('Invalid user ID');
      }

      // Check for existing feedback first
      final existingFeedback =
          await _supabase
              .from('job_feedback')
              .select()
              .eq('job_listing_id', jobListingId)
              .eq('user_id', userId)
              .maybeSingle();

      // If feedback already exists, prevent submission
      if (existingFeedback != null) {
        print('Feedback already exists for this job and user');
        throw Exception('You have already submitted feedback for this job');
      }

      // Validate if job listing exists
      final jobListingExists =
          await _supabase
              .from('worker_job_listings')
              .select('id')
              .eq('id', jobListingId)
              .maybeSingle();

      if (jobListingExists == null) {
        // Try to find correct job_id from applications table
        final jobApplication =
            await _supabase
                .from('worker_job_applications')
                .select('job_id')
                .eq('id', jobListingId)
                .maybeSingle();

        if (jobApplication != null && jobApplication['job_id'] != null) {
          jobListingId = jobApplication['job_id'].toString();
        } else {
          print('Could not find a valid job listing');
          throw Exception('Invalid job listing');
        }
      }

      // Insert new feedback
      await _supabase.from('job_feedback').insert({
        'job_listing_id': jobListingId,
        'user_id': userId,
        'rating': rating,
        'comments': comments ?? '',
        'created_at': DateTime.now().toIso8601String(),
        'updated_at': DateTime.now().toIso8601String(),
      });

      print('Feedback submitted successfully');
      return true;
    } catch (e) {
      print('Error submitting feedback: $e');
      rethrow;
    }
  }

  Future<Map<String, dynamic>?> getUserFeedback(
    String jobListingId,
    String userId,
  ) async {
    try {
      print('Checking for existing feedback:');
      print('Job ID: $jobListingId');
      print('User ID: $userId');

      // First, try direct match
      final feedback =
          await _supabase
              .from('job_feedback')
              .select()
              .eq('job_listing_id', jobListingId)
              .eq('user_id', userId)
              .maybeSingle();

      if (feedback != null) {
        print('Existing feedback found with direct match');
        return feedback;
      }

      // If not found, try to get the job_id from worker_job_applications
      final jobApplication =
          await _supabase
              .from('worker_job_applications')
              .select('job_id')
              .eq('id', jobListingId)
              .maybeSingle();

      if (jobApplication != null && jobApplication['job_id'] != null) {
        final alternativeJobId = jobApplication['job_id'].toString();

        // Try again with the alternative job ID
        final altFeedback =
            await _supabase
                .from('job_feedback')
                .select()
                .eq('job_listing_id', alternativeJobId)
                .eq('user_id', userId)
                .maybeSingle();

        if (altFeedback != null) {
          print('Existing feedback found with alternative job ID');
          return altFeedback;
        }
      }

      print('No existing feedback found');
      return null;
    } catch (e) {
      print('Error fetching user feedback: $e');
      return null;
    }
  }
}
