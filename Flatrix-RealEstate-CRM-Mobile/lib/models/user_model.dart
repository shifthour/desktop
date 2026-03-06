class UserModel {
  final String id;
  final String email;
  final String name;
  final String role;
  final String? phone;
  final DateTime? createdAt;

  UserModel({
    required this.id,
    required this.email,
    required this.name,
    required this.role,
    this.phone,
    this.createdAt,
  });

  factory UserModel.fromJson(Map<String, dynamic> json) {
    return UserModel(
      id: json['id'] ?? '',
      email: json['email'] ?? '',
      name: json['name'] ?? json['email'] ?? '',
      role: json['role'] ?? 'AGENT',
      phone: json['phone'],
      createdAt: json['created_at'] != null
          ? DateTime.parse(json['created_at'])
          : null,
    );
  }

  Map<String, dynamic> toJson() {
    return {
      'id': id,
      'email': email,
      'name': name,
      'role': role,
      'phone': phone,
      'created_at': createdAt?.toIso8601String(),
    };
  }

  bool get isAdmin => role.toLowerCase() == 'admin' || role.toLowerCase() == 'super_admin';
  bool get isSuperAdmin => role.toLowerCase() == 'super_admin';
}
