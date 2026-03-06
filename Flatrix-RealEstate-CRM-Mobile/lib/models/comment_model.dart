class CommentModel {
  final String id;
  final String leadId;
  final String? userId;
  final String? userName;
  final String comment;
  final DateTime? createdAt;

  CommentModel({
    required this.id,
    required this.leadId,
    this.userId,
    this.userName,
    required this.comment,
    this.createdAt,
  });

  factory CommentModel.fromJson(Map<String, dynamic> json) {
    return CommentModel(
      id: json['id'] ?? '',
      leadId: json['lead_id'] ?? '',
      userId: json['user_id'],
      userName: json['user']?['name'] ?? json['user_name'],
      comment: json['comment'] ?? json['content'] ?? '',
      createdAt: json['created_at'] != null
          ? DateTime.parse(json['created_at'])
          : null,
    );
  }

  Map<String, dynamic> toJson() {
    return {
      'id': id,
      'lead_id': leadId,
      'user_id': userId,
      'comment': comment,
      'created_at': createdAt?.toIso8601String(),
    };
  }
}
