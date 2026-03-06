import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:shared_preferences/shared_preferences.dart';
import '../models/user_model.dart';
import '../services/supabase_service.dart';

class AuthProvider extends ChangeNotifier {
  UserModel? _user;
  bool _isLoading = false;
  String? _error;

  UserModel? get user => _user;
  bool get isLoading => _isLoading;
  bool get isLoggedIn => _user != null;
  String? get error => _error;

  AuthProvider() {
    _loadUser();
  }

  Future<void> _loadUser() async {
    _isLoading = true;
    notifyListeners();

    try {
      final prefs = await SharedPreferences.getInstance();
      final userJson = prefs.getString('flatrix_user');

      if (userJson != null) {
        _user = UserModel.fromJson(jsonDecode(userJson));
      }
    } catch (e) {
      print('Error loading user: $e');
    }

    _isLoading = false;
    notifyListeners();
  }

  Future<bool> login(String email, String password) async {
    _isLoading = true;
    _error = null;
    notifyListeners();

    try {
      final user = await SupabaseService.login(email, password);

      if (user != null) {
        _user = user;
        _error = null;

        // Save to local storage
        final prefs = await SharedPreferences.getInstance();
        await prefs.setString('flatrix_user', jsonEncode(user.toJson()));

        _isLoading = false;
        notifyListeners();
        return true;
      } else {
        // Show detailed error for debugging
        _error = SupabaseService.lastLoginError ?? 'Invalid email or password';
        _isLoading = false;
        notifyListeners();
        return false;
      }
    } catch (e) {
      _error = 'Login failed. Please try again.';
      _isLoading = false;
      notifyListeners();
      return false;
    }
  }

  Future<void> logout() async {
    _user = null;
    _error = null;

    final prefs = await SharedPreferences.getInstance();
    await prefs.remove('flatrix_user');

    notifyListeners();
  }

  void clearError() {
    _error = null;
    notifyListeners();
  }
}
