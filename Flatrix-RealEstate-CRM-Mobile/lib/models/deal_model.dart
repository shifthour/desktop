class DealModel {
  final String id;
  final String dealNumber;
  final String leadId;
  final String? propertyId;
  final String? userId;
  final String? channelPartnerId;
  final double? dealValue;
  final String status;
  final String? siteVisitStatus;
  final DateTime? siteVisitDate;
  final String? conversionStatus;
  final DateTime? conversionDate;
  final String? attendedBy;
  final String? notes;
  final String? managementNotes;
  final DateTime? bookingDate;
  final DateTime? closingDate;
  final DateTime? createdAt;
  final DateTime? updatedAt;

  DealModel({
    required this.id,
    required this.dealNumber,
    required this.leadId,
    this.propertyId,
    this.userId,
    this.channelPartnerId,
    this.dealValue,
    required this.status,
    this.siteVisitStatus,
    this.siteVisitDate,
    this.conversionStatus,
    this.conversionDate,
    this.attendedBy,
    this.notes,
    this.managementNotes,
    this.bookingDate,
    this.closingDate,
    this.createdAt,
    this.updatedAt,
  });

  factory DealModel.fromJson(Map<String, dynamic> json) {
    return DealModel(
      id: json['id'] ?? '',
      dealNumber: json['deal_number'] ?? '',
      leadId: json['lead_id'] ?? '',
      propertyId: json['property_id'],
      userId: json['user_id'],
      channelPartnerId: json['channel_partner_id'],
      dealValue: json['deal_value']?.toDouble(),
      status: json['status'] ?? 'ACTIVE',
      siteVisitStatus: json['site_visit_status'],
      siteVisitDate: json['site_visit_date'] != null
          ? DateTime.parse(json['site_visit_date'])
          : null,
      conversionStatus: json['conversion_status'],
      conversionDate: json['conversion_date'] != null
          ? DateTime.parse(json['conversion_date'])
          : null,
      attendedBy: json['attended_by'],
      notes: json['notes'],
      managementNotes: json['management_notes'],
      bookingDate: json['booking_date'] != null
          ? DateTime.parse(json['booking_date'])
          : null,
      closingDate: json['closing_date'] != null
          ? DateTime.parse(json['closing_date'])
          : null,
      createdAt: json['created_at'] != null
          ? DateTime.parse(json['created_at'])
          : null,
      updatedAt: json['updated_at'] != null
          ? DateTime.parse(json['updated_at'])
          : null,
    );
  }

  Map<String, dynamic> toJson() {
    return {
      'id': id,
      'deal_number': dealNumber,
      'lead_id': leadId,
      'property_id': propertyId,
      'user_id': userId,
      'channel_partner_id': channelPartnerId,
      'deal_value': dealValue,
      'status': status,
      'site_visit_status': siteVisitStatus,
      'site_visit_date': siteVisitDate?.toIso8601String(),
      'conversion_status': conversionStatus,
      'conversion_date': conversionDate?.toIso8601String(),
      'attended_by': attendedBy,
      'notes': notes,
      'management_notes': managementNotes,
      'booking_date': bookingDate?.toIso8601String(),
      'closing_date': closingDate?.toIso8601String(),
    };
  }

  DealModel copyWith({
    String? id,
    String? dealNumber,
    String? leadId,
    String? propertyId,
    String? userId,
    String? channelPartnerId,
    double? dealValue,
    String? status,
    String? siteVisitStatus,
    DateTime? siteVisitDate,
    String? conversionStatus,
    DateTime? conversionDate,
    String? attendedBy,
    String? notes,
    String? managementNotes,
    DateTime? bookingDate,
    DateTime? closingDate,
    DateTime? createdAt,
    DateTime? updatedAt,
  }) {
    return DealModel(
      id: id ?? this.id,
      dealNumber: dealNumber ?? this.dealNumber,
      leadId: leadId ?? this.leadId,
      propertyId: propertyId ?? this.propertyId,
      userId: userId ?? this.userId,
      channelPartnerId: channelPartnerId ?? this.channelPartnerId,
      dealValue: dealValue ?? this.dealValue,
      status: status ?? this.status,
      siteVisitStatus: siteVisitStatus ?? this.siteVisitStatus,
      siteVisitDate: siteVisitDate ?? this.siteVisitDate,
      conversionStatus: conversionStatus ?? this.conversionStatus,
      conversionDate: conversionDate ?? this.conversionDate,
      attendedBy: attendedBy ?? this.attendedBy,
      notes: notes ?? this.notes,
      managementNotes: managementNotes ?? this.managementNotes,
      bookingDate: bookingDate ?? this.bookingDate,
      closingDate: closingDate ?? this.closingDate,
      createdAt: createdAt ?? this.createdAt,
      updatedAt: updatedAt ?? this.updatedAt,
    );
  }
}
