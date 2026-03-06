import 'dart:math';
import 'package:uuid/uuid.dart';

class TransferIdGenerator {
  // Generate a unique transfer ID
  static String generate({String? prefix}) {
    // Use current timestamp as part of the ID
    final timestamp = DateTime.now().millisecondsSinceEpoch;

    // Generate a random UUID
    final uuid = const Uuid().v4();

    // Create a unique identifier combining timestamp and UUID
    final uniqueId = '${prefix ?? 'TRF'}_$timestamp\_${uuid.split('-').last}';

    return uniqueId;
  }

  // Optional: Generate a shorter, numeric-based transfer ID
  static String generateNumeric({int length = 12}) {
    final random = Random();
    final buffer = StringBuffer();

    // Ensure first digit is non-zero
    buffer.write(random.nextInt(9) + 1);

    // Fill the rest with random digits
    for (int i = 1; i < length; i++) {
      buffer.write(random.nextInt(10));
    }

    return buffer.toString();
  }
}
