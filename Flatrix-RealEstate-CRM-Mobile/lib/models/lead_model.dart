class LeadModel {
  final String id;
  final String? name;
  final String? firstName;
  final String? lastName;
  final String? email;
  final String? phone;
  final String? projectName;
  final String? source;
  final String status;
  final String? notes;
  final String? managementNotes;
  final String? preferredLocation;
  final String? preferredType;
  final double? budgetMin;
  final double? budgetMax;
  final String? assignedToId;
  final String? assignedToName;
  final String? registered;
  final DateTime? registeredDate;
  final DateTime? createdAt;
  final DateTime? updatedAt;
  final DateTime? lastContactedAt;
  final DateTime? nextFollowupDate;
  final List<String>? interestedIn;

  LeadModel({
    required this.id,
    this.name,
    this.firstName,
    this.lastName,
    this.email,
    this.phone,
    this.projectName,
    this.source,
    required this.status,
    this.notes,
    this.managementNotes,
    this.preferredLocation,
    this.preferredType,
    this.budgetMin,
    this.budgetMax,
    this.assignedToId,
    this.assignedToName,
    this.registered,
    this.registeredDate,
    this.createdAt,
    this.updatedAt,
    this.lastContactedAt,
    this.nextFollowupDate,
    this.interestedIn,
  });

  String get displayName {
    if (name != null && name!.isNotEmpty) return name!;
    final parts = [firstName, lastName].where((p) => p != null && p.isNotEmpty);
    if (parts.isNotEmpty) return parts.join(' ');
    return 'Unknown';
  }

  factory LeadModel.fromJson(Map<String, dynamic> json) {
    return LeadModel(
      id: json['id'] ?? '',
      name: json['name'],
      firstName: json['first_name'],
      lastName: json['last_name'],
      email: json['email'],
      phone: json['phone'],
      projectName: json['project_name'],
      source: json['source'],
      status: json['status'] ?? 'NEW',
      notes: json['notes'],
      managementNotes: json['management_notes'],
      preferredLocation: json['preferred_location'],
      preferredType: json['preferred_type'],
      budgetMin: json['budget_min']?.toDouble(),
      budgetMax: json['budget_max']?.toDouble(),
      assignedToId: json['assigned_to_id'],
      assignedToName: json['assigned_to']?['name'],
      registered: json['registered'],
      registeredDate: json['registered_date'] != null
          ? DateTime.parse(json['registered_date'])
          : null,
      createdAt: json['created_at'] != null
          ? DateTime.parse(json['created_at'])
          : null,
      updatedAt: json['updated_at'] != null
          ? DateTime.parse(json['updated_at'])
          : null,
      lastContactedAt: json['last_contacted_at'] != null
          ? DateTime.parse(json['last_contacted_at'])
          : null,
      nextFollowupDate: json['next_followup_date'] != null
          ? DateTime.parse(json['next_followup_date'])
          : null,
      interestedIn: json['interested_in'] != null
          ? List<String>.from(json['interested_in'])
          : null,
    );
  }

  Map<String, dynamic> toJson() {
    return {
      'id': id,
      'name': name,
      'first_name': firstName,
      'last_name': lastName,
      'email': email,
      'phone': phone,
      'project_name': projectName,
      'source': source,
      'status': status,
      'notes': notes,
      'management_notes': managementNotes,
      'preferred_location': preferredLocation,
      'preferred_type': preferredType,
      'budget_min': budgetMin,
      'budget_max': budgetMax,
      'assigned_to_id': assignedToId,
      'registered': registered,
      'registered_date': registeredDate?.toIso8601String(),
      'last_contacted_at': lastContactedAt?.toIso8601String(),
      'next_followup_date': nextFollowupDate?.toIso8601String(),
      'interested_in': interestedIn,
    };
  }

  LeadModel copyWith({
    String? id,
    String? name,
    String? firstName,
    String? lastName,
    String? email,
    String? phone,
    String? projectName,
    String? source,
    String? status,
    String? notes,
    String? managementNotes,
    String? preferredLocation,
    String? preferredType,
    double? budgetMin,
    double? budgetMax,
    String? assignedToId,
    String? assignedToName,
    String? registered,
    DateTime? registeredDate,
    DateTime? createdAt,
    DateTime? updatedAt,
    DateTime? lastContactedAt,
    DateTime? nextFollowupDate,
    List<String>? interestedIn,
  }) {
    return LeadModel(
      id: id ?? this.id,
      name: name ?? this.name,
      firstName: firstName ?? this.firstName,
      lastName: lastName ?? this.lastName,
      email: email ?? this.email,
      phone: phone ?? this.phone,
      projectName: projectName ?? this.projectName,
      source: source ?? this.source,
      status: status ?? this.status,
      notes: notes ?? this.notes,
      managementNotes: managementNotes ?? this.managementNotes,
      preferredLocation: preferredLocation ?? this.preferredLocation,
      preferredType: preferredType ?? this.preferredType,
      budgetMin: budgetMin ?? this.budgetMin,
      budgetMax: budgetMax ?? this.budgetMax,
      assignedToId: assignedToId ?? this.assignedToId,
      assignedToName: assignedToName ?? this.assignedToName,
      registered: registered ?? this.registered,
      registeredDate: registeredDate ?? this.registeredDate,
      createdAt: createdAt ?? this.createdAt,
      updatedAt: updatedAt ?? this.updatedAt,
      lastContactedAt: lastContactedAt ?? this.lastContactedAt,
      nextFollowupDate: nextFollowupDate ?? this.nextFollowupDate,
      interestedIn: interestedIn ?? this.interestedIn,
    );
  }
}
