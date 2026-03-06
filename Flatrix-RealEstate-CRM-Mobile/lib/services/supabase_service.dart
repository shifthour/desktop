import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:dbcrypt/dbcrypt.dart';
import '../config/supabase_config.dart';
import '../models/user_model.dart';
import '../models/lead_model.dart';
import '../models/deal_model.dart';
import '../models/commission_model.dart';
import '../models/comment_model.dart';

class SupabaseService {
  static SupabaseClient get client => Supabase.instance.client;

  static Future<void> initialize() async {
    await Supabase.initialize(
      url: SupabaseConfig.supabaseUrl,
      anonKey: SupabaseConfig.supabaseAnonKey,
    );
  }

  // ==================== AUTH ====================

  static String? lastLoginError;

  // Temporary hardcoded passwords (fallback when database password_hash is not set)
  // TODO: Remove this once all users have password_hash in the database
  static const Map<String, String> _fallbackPasswords = {
    'superadmin@flatrix.com': 'superadmin123',
    'admin@flatrix.com': 'admin123',
    'dinki@flatrix.com': 'dinki123',
    'prakash@flatrix.com': 'prakash123',
    'kusuma@flatrix.com': 'Kusuma@123',
  };

  /// Authenticates user against server-side stored credentials.
  /// Falls back to hardcoded passwords if password_hash is not set in database.
  static Future<UserModel?> login(String email, String password) async {
    lastLoginError = null;
    try {
      final emailLower = email.trim().toLowerCase();

      // Query user from flatrix_users table
      final response = await client
          .from('flatrix_users')
          .select()
          .eq('email', emailLower)
          .maybeSingle();

      if (response == null) {
        lastLoginError = 'User not found';
        return null;
      }

      // Try to validate against bcrypt hash in database first
      final storedHash = response['password_hash'] as String?;
      if (storedHash != null && storedHash.isNotEmpty) {
        final bcrypt = DBCrypt();
        final isValidPassword = bcrypt.checkpw(password, storedHash);
        if (!isValidPassword) {
          lastLoginError = 'Incorrect password';
          return null;
        }
      } else {
        // Fallback to hardcoded passwords if no hash in database
        final expectedPassword = _fallbackPasswords[emailLower];
        if (expectedPassword == null || password != expectedPassword) {
          lastLoginError = 'Incorrect password';
          return null;
        }
      }

      return UserModel.fromJson(response);
    } catch (e) {
      lastLoginError = 'Error: $e';
      return null;
    }
  }

  // ==================== LEADS ====================

  static Future<List<LeadModel>> getLeads({
    String? assignedToId,
    String? status,
    int limit = 10000,
    int offset = 0,
    DateTime? followUpDate,
  }) async {
    try {
      PostgrestFilterBuilder query = client
          .from('flatrix_leads')
          .select('*, assigned_to:flatrix_users!assigned_to_id(name)');

      if (assignedToId != null) {
        query = query.eq('assigned_to_id', assignedToId);
      }

      if (status != null) {
        query = query.eq('status', status);
      }

      // Filter by follow-up date if provided
      if (followUpDate != null) {
        final startOfDay = DateTime(followUpDate.year, followUpDate.month, followUpDate.day);
        final endOfDay = startOfDay.add(const Duration(days: 1));
        query = query.gte('next_followup_date', startOfDay.toIso8601String());
        query = query.lt('next_followup_date', endOfDay.toIso8601String());
      }

      final response = await query
          .order('created_at', ascending: false)
          .range(offset, offset + limit - 1);

      return (response as List)
          .map((json) => LeadModel.fromJson(json))
          .toList();
    } catch (e) {
      print('Error fetching leads: $e');
      return [];
    }
  }

  /// Get all bookings (deals with conversion_status = BOOKED)
  static Future<List<Map<String, dynamic>>> getBookings() async {
    try {
      final response = await client
          .from('flatrix_deals')
          .select('*, lead:flatrix_leads!lead_id(*)')
          .eq('conversion_status', 'BOOKED')
          .order('booking_date', ascending: false);

      return List<Map<String, dynamic>>.from(response);
    } catch (e) {
      print('Error fetching bookings: $e');
      return [];
    }
  }

  static Future<LeadModel?> getLeadById(String id) async {
    try {
      final response = await client
          .from('flatrix_leads')
          .select('*, assigned_to:flatrix_users!assigned_to_id(name)')
          .eq('id', id)
          .single();

      return LeadModel.fromJson(response);
    } catch (e) {
      print('Error fetching lead: $e');
      return null;
    }
  }

  static Future<bool> updateLead(String id, Map<String, dynamic> data) async {
    try {
      data['updated_at'] = DateTime.now().toIso8601String();
      await client.from('flatrix_leads').update(data).eq('id', id);
      return true;
    } catch (e) {
      print('Error updating lead: $e');
      return false;
    }
  }

  static Future<LeadModel?> createLead(Map<String, dynamic> data) async {
    try {
      data['created_at'] = DateTime.now().toIso8601String();
      data['updated_at'] = DateTime.now().toIso8601String();

      final response = await client
          .from('flatrix_leads')
          .insert(data)
          .select()
          .single();

      return LeadModel.fromJson(response);
    } catch (e) {
      print('Error creating lead: $e');
      return null;
    }
  }

  // ==================== DEALS ====================

  static Future<List<DealModel>> getDeals({
    String? userId,
    String? status,
    int limit = 100,
    int offset = 0,
  }) async {
    try {
      PostgrestFilterBuilder query = client
          .from('flatrix_deals')
          .select();

      if (userId != null) {
        query = query.eq('user_id', userId);
      }

      if (status != null) {
        query = query.eq('status', status);
      }

      final response = await query
          .order('created_at', ascending: false)
          .range(offset, offset + limit - 1);

      return (response as List)
          .map((json) => DealModel.fromJson(json))
          .toList();
    } catch (e) {
      print('Error fetching deals: $e');
      return [];
    }
  }

  static Future<DealModel?> getDealByLeadId(String leadId) async {
    try {
      final response = await client
          .from('flatrix_deals')
          .select()
          .eq('lead_id', leadId)
          .maybeSingle();

      if (response == null) return null;
      return DealModel.fromJson(response);
    } catch (e) {
      print('Error fetching deal: $e');
      return null;
    }
  }

  static Future<bool> updateDeal(String id, Map<String, dynamic> data) async {
    try {
      data['updated_at'] = DateTime.now().toIso8601String();
      await client.from('flatrix_deals').update(data).eq('id', id);
      return true;
    } catch (e) {
      print('Error updating deal: $e');
      return false;
    }
  }

  // ==================== STATS ====================

  /// Helper to get count using Supabase's count feature (no 1000 row limit)
  static Future<int> _getLeadsCount({String? status, String? assignedToId}) async {
    try {
      PostgrestFilterBuilder query = client.from('flatrix_leads').select('id');

      if (status != null) {
        query = query.eq('status', status);
      }

      if (assignedToId != null) {
        query = query.eq('assigned_to_id', assignedToId);
      }

      // Use count() to get accurate count without 1000 limit
      final response = await query.count(CountOption.exact);
      return response.count;
    } catch (e) {
      print('Error getting leads count: $e');
      return 0;
    }
  }

  static Future<int> _getDealsCount({String? siteVisitStatus, String? conversionStatus}) async {
    try {
      PostgrestFilterBuilder query = client.from('flatrix_deals').select('id');

      if (siteVisitStatus != null) {
        // Use ilike for case-insensitive matching
        query = query.ilike('site_visit_status', siteVisitStatus);
      }

      if (conversionStatus != null) {
        // Use ilike for case-insensitive matching
        query = query.ilike('conversion_status', conversionStatus);
      }

      final response = await query.count(CountOption.exact);
      return response.count;
    } catch (e) {
      print('Error getting deals count: $e');
      return 0;
    }
  }

  static Future<Map<String, int>> getDashboardStats({String? assignedToId}) async {
    try {
      // Use count queries to avoid the 1000 row limit
      // Run all count queries in parallel for better performance
      final results = await Future.wait([
        // Total leads
        _getLeadsCount(assignedToId: assignedToId),
        // New leads
        _getLeadsCount(status: 'NEW', assignedToId: assignedToId),
        // Contacted leads
        _getLeadsCount(status: 'CONTACTED', assignedToId: assignedToId),
        // Qualified leads
        _getLeadsCount(status: 'QUALIFIED', assignedToId: assignedToId),
        // Lost leads
        _getLeadsCount(status: 'LOST', assignedToId: assignedToId),
        // Site visits completed - filter by status COMPLETED
        _getDealsCount(siteVisitStatus: 'COMPLETED'),
        // Site visits scheduled
        _getDealsCount(siteVisitStatus: 'SCHEDULED'),
        // Booked
        _getDealsCount(conversionStatus: 'BOOKED'),
      ]);

      return {
        'totalLeads': results[0],
        'newLeads': results[1],
        'contactedLeads': results[2],
        'qualifiedLeads': results[3],
        'lostLeads': results[4],
        'siteVisitsCompleted': results[5],
        'siteVisitsScheduled': results[6],
        'booked': results[7],
      };
    } catch (e) {
      print('Error fetching stats: $e');
      return {};
    }
  }

  // ==================== USERS ====================

  static Future<List<UserModel>> getUsers() async {
    try {
      final response = await client
          .from('flatrix_users')
          .select()
          .order('name', ascending: true);

      return (response as List)
          .map((json) => UserModel.fromJson(json))
          .toList();
    } catch (e) {
      print('Error fetching users: $e');
      return [];
    }
  }

  // ==================== COMMISSIONS ====================

  static Future<List<CommissionModel>> getCommissions({
    String? status,
    int limit = 100,
    int offset = 0,
  }) async {
    try {
      PostgrestFilterBuilder query = client
          .from('flatrix_commissions')
          .select('*, channel_partner:flatrix_users!channel_partner_id(name)');

      if (status != null) {
        query = query.eq('status', status);
      }

      final response = await query
          .order('created_at', ascending: false)
          .range(offset, offset + limit - 1);

      return (response as List)
          .map((json) => CommissionModel.fromJson(json))
          .toList();
    } catch (e) {
      print('Error fetching commissions: $e');
      return [];
    }
  }

  static Future<Map<String, dynamic>> getCommissionStats() async {
    try {
      final response = await client
          .from('flatrix_commissions')
          .select('status, amount');

      final commissions = response as List;

      double totalAmount = 0;
      double pendingAmount = 0;
      double paidAmount = 0;
      int totalCount = commissions.length;
      int pendingCount = 0;
      int paidCount = 0;

      for (final c in commissions) {
        final amount = (c['amount'] ?? 0).toDouble();
        totalAmount += amount;

        if (c['status'] == 'PENDING' || c['status'] == 'APPROVED') {
          pendingAmount += amount;
          pendingCount++;
        } else if (c['status'] == 'PAID') {
          paidAmount += amount;
          paidCount++;
        }
      }

      return {
        'totalAmount': totalAmount,
        'pendingAmount': pendingAmount,
        'paidAmount': paidAmount,
        'totalCount': totalCount,
        'pendingCount': pendingCount,
        'paidCount': paidCount,
      };
    } catch (e) {
      print('Error fetching commission stats: $e');
      return {};
    }
  }

  static Future<bool> updateCommission(String id, Map<String, dynamic> data) async {
    try {
      data['updated_at'] = DateTime.now().toIso8601String();
      await client.from('flatrix_commissions').update(data).eq('id', id);
      return true;
    } catch (e) {
      print('Error updating commission: $e');
      return false;
    }
  }

  // ==================== REPORTS ====================

  static Future<Map<String, dynamic>> getReportStats() async {
    try {
      // Get leads stats
      final leadsResponse = await client
          .from('flatrix_leads')
          .select('status, source, created_at');

      final leads = leadsResponse as List;

      // Get deals stats
      final dealsResponse = await client
          .from('flatrix_deals')
          .select('site_visit_status, conversion_status, deal_value, created_at');

      final deals = dealsResponse as List;

      // Calculate stats
      Map<String, int> leadsByStatus = {};
      Map<String, int> leadsBySource = {};

      for (final lead in leads) {
        final status = lead['status'] ?? 'UNKNOWN';
        leadsByStatus[status] = (leadsByStatus[status] ?? 0) + 1;

        final source = lead['source'] ?? 'Unknown';
        leadsBySource[source] = (leadsBySource[source] ?? 0) + 1;
      }

      int siteVisitsCompleted = 0;
      int bookings = 0;
      double totalDealValue = 0;

      for (final deal in deals) {
        if (deal['site_visit_status'] == 'COMPLETED') {
          siteVisitsCompleted++;
        }
        if (deal['conversion_status'] == 'BOOKED') {
          bookings++;
          totalDealValue += (deal['deal_value'] ?? 0).toDouble();
        }
      }

      return {
        'totalLeads': leads.length,
        'totalDeals': deals.length,
        'leadsByStatus': leadsByStatus,
        'leadsBySource': leadsBySource,
        'siteVisitsCompleted': siteVisitsCompleted,
        'bookings': bookings,
        'totalDealValue': totalDealValue,
      };
    } catch (e) {
      print('Error fetching report stats: $e');
      return {};
    }
  }

  // ==================== COMMENTS ====================

  /// Parse notes string into list of comments (matching web version format)
  /// Format: [MM/DD/YY, HH:MM AM/PM]:\nComment content
  static List<CommentModel> parseNotesToComments(String? notes, String leadId) {
    if (notes == null || notes.trim().isEmpty) return [];

    final List<CommentModel> comments = [];
    // Regex to match timestamp format: [MM/DD/YY, HH:MM AM/PM]:
    final regex = RegExp(r'\[(\d{1,2}/\d{1,2}/\d{2,4},?\s*\d{1,2}:\d{2}\s*[AP]M)\]:\s*');

    final matches = regex.allMatches(notes).toList();

    for (int i = 0; i < matches.length; i++) {
      final match = matches[i];
      final timestamp = match.group(1) ?? '';
      final startIndex = match.end;
      final endIndex = i + 1 < matches.length ? matches[i + 1].start : notes.length;
      final commentText = notes.substring(startIndex, endIndex).trim();

      if (commentText.isNotEmpty) {
        DateTime? createdAt;
        try {
          // Parse timestamp like "1/15/24, 3:30 PM" or "01/15/2024, 3:30 PM"
          final parts = timestamp.split(',');
          if (parts.length >= 2) {
            final datePart = parts[0].trim();
            final timePart = parts[1].trim();
            final dateParts = datePart.split('/');
            if (dateParts.length == 3) {
              int month = int.parse(dateParts[0]);
              int day = int.parse(dateParts[1]);
              int year = int.parse(dateParts[2]);
              if (year < 100) year += 2000;

              // Parse time
              final timeRegex = RegExp(r'(\d{1,2}):(\d{2})\s*(AM|PM)', caseSensitive: false);
              final timeMatch = timeRegex.firstMatch(timePart);
              if (timeMatch != null) {
                int hour = int.parse(timeMatch.group(1)!);
                int minute = int.parse(timeMatch.group(2)!);
                final ampm = timeMatch.group(3)!.toUpperCase();
                if (ampm == 'PM' && hour != 12) hour += 12;
                if (ampm == 'AM' && hour == 12) hour = 0;
                createdAt = DateTime(year, month, day, hour, minute);
              }
            }
          }
        } catch (e) {
          print('Error parsing timestamp: $e');
        }

        comments.add(CommentModel(
          id: '${leadId}_$i',
          leadId: leadId,
          comment: commentText,
          createdAt: createdAt,
          userName: null,
        ));
      }
    }

    // If no timestamped comments found, treat entire notes as one comment
    if (comments.isEmpty && notes.trim().isNotEmpty) {
      comments.add(CommentModel(
        id: '${leadId}_0',
        leadId: leadId,
        comment: notes.trim(),
        createdAt: null,
        userName: null,
      ));
    }

    // Return newest first
    return comments.reversed.toList();
  }

  static String? lastCommentError;

  /// Add a comment by appending to the notes field (matching web version format)
  static Future<bool> addCommentToLead({
    required String leadId,
    required String comment,
  }) async {
    lastCommentError = null;
    try {
      // Get current notes
      final leadResponse = await client
          .from('flatrix_leads')
          .select('notes')
          .eq('id', leadId)
          .single();

      String currentNotes = leadResponse['notes'] ?? '';

      // Create timestamp in web format: [MM/DD/YY, HH:MM AM/PM]:
      final now = DateTime.now();
      final hour = now.hour > 12 ? now.hour - 12 : (now.hour == 0 ? 12 : now.hour);
      final ampm = now.hour >= 12 ? 'PM' : 'AM';
      final timestamp = '${now.month}/${now.day}/${now.year.toString().substring(2)}, $hour:${now.minute.toString().padLeft(2, '0')} $ampm';

      // Append new comment with timestamp
      final newNote = '[$timestamp]:\n${comment.trim()}';
      final updatedNotes = currentNotes.isEmpty
          ? newNote
          : '$currentNotes\n\n$newNote';

      // Update lead with new notes
      await client
          .from('flatrix_leads')
          .update({
            'notes': updatedNotes,
            'updated_at': DateTime.now().toIso8601String(),
          })
          .eq('id', leadId);

      print('Comment added successfully');
      return true;
    } catch (e) {
      lastCommentError = e.toString();
      print('Error adding comment: $e');
      return false;
    }
  }
}
