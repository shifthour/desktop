import 'package:shared_preferences_platform_interface/shared_preferences_platform_interface.dart';
import 'package:plugin_platform_interface/plugin_platform_interface.dart';

class SharedPreferencesMock extends Fake implements SharedPreferencesPlatform {
  SharedPreferencesMock(this.store);

  SharedPreferencesMock.empty() : store = <String, Object>{};

  final Map<String, Object> store;

  @override
  Future<bool> clear() async {
    store.clear();
    return true;
  }

  @override
  Future<bool> remove(String key) async {
    store.remove(key);
    return true;
  }

  @override
  Future<bool> setBool(String key, bool value) async {
    store[key] = value;
    return true;
  }

  @override
  Future<bool> setDouble(String key, double value) async {
    store[key] = value;
    return true;
  }

  @override
  Future<bool> setInt(String key, int value) async {
    store[key] = value;
    return true;
  }

  @override
  Future<bool> setString(String key, String value) async {
    store[key] = value;
    return true;
  }

  @override
  Future<bool> setStringList(String key, List<String> value) async {
    store[key] = value;
    return true;
  }

  @override
  Object? get(String key) {
    return store[key];
  }

  @override
  bool? getBool(String key) {
    return store[key] as bool?;
  }

  @override
  double? getDouble(String key) {
    return store[key] as double?;
  }

  @override
  int? getInt(String key) {
    return store[key] as int?;
  }

  @override
  String? getString(String key) {
    return store[key] as String?;
  }

  @override
  List<String>? getStringList(String key) {
    return store[key] as List<String>?;
  }

  @override
  Future<Map<String, Object>> getAll() async {
    return store;
  }

  @override
  bool containsKey(String key) {
    return store.containsKey(key);
  }
}

class Fake implements Mock {
  const Fake();
}

class Mock extends Object with MockPlatformInterfaceMixin {}
