class CommissionModel {
  final String id;
  final String dealId;
  final String channelPartnerId;
  final double amount;
  final double percentage;
  final String status;
  final DateTime? approvedDate;
  final DateTime? paidDate;
  final String? paymentMethod;
  final String? transactionRef;
  final String? notes;
  final DateTime? createdAt;
  final DateTime? updatedAt;

  // Joined fields
  final String? channelPartnerName;
  final String? leadName;
  final String? projectName;

  CommissionModel({
    required this.id,
    required this.dealId,
    required this.channelPartnerId,
    required this.amount,
    required this.percentage,
    required this.status,
    this.approvedDate,
    this.paidDate,
    this.paymentMethod,
    this.transactionRef,
    this.notes,
    this.createdAt,
    this.updatedAt,
    this.channelPartnerName,
    this.leadName,
    this.projectName,
  });

  factory CommissionModel.fromJson(Map<String, dynamic> json) {
    return CommissionModel(
      id: json['id'] ?? '',
      dealId: json['deal_id'] ?? '',
      channelPartnerId: json['channel_partner_id'] ?? '',
      amount: (json['amount'] ?? 0).toDouble(),
      percentage: (json['percentage'] ?? 0).toDouble(),
      status: json['status'] ?? 'PENDING',
      approvedDate: json['approved_date'] != null
          ? DateTime.parse(json['approved_date'])
          : null,
      paidDate: json['paid_date'] != null
          ? DateTime.parse(json['paid_date'])
          : null,
      paymentMethod: json['payment_method'],
      transactionRef: json['transaction_ref'],
      notes: json['notes'],
      createdAt: json['created_at'] != null
          ? DateTime.parse(json['created_at'])
          : null,
      updatedAt: json['updated_at'] != null
          ? DateTime.parse(json['updated_at'])
          : null,
      channelPartnerName: json['channel_partner']?['name'],
      leadName: json['deal']?['lead']?['name'] ?? json['deal']?['lead']?['first_name'],
      projectName: json['deal']?['lead']?['project_name'],
    );
  }

  Map<String, dynamic> toJson() {
    return {
      'id': id,
      'deal_id': dealId,
      'channel_partner_id': channelPartnerId,
      'amount': amount,
      'percentage': percentage,
      'status': status,
      'approved_date': approvedDate?.toIso8601String(),
      'paid_date': paidDate?.toIso8601String(),
      'payment_method': paymentMethod,
      'transaction_ref': transactionRef,
      'notes': notes,
      'created_at': createdAt?.toIso8601String(),
      'updated_at': updatedAt?.toIso8601String(),
    };
  }
}
