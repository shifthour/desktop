import 'package:bcrypt/bcrypt.dart';

void main() {
  final storedHash = '\$2a\$12\$/RDtHjZyij0Fu88IcpPWyevVvs6SAYdxujIipbhUZIaDjpbmKKRIa';
  final password = 'admin123';

  print('Stored hash: $storedHash');
  print('Password to test: $password');

  try {
    final result = BCrypt.checkpw(password, storedHash);
    print('BCrypt.checkpw result: $result');
  } catch (e) {
    print('Error: $e');
  }
}
