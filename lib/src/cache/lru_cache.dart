library document_db.lru_cache;

import 'dart:async';
//import 'dart:typed_data';

import 'package:quiver/collection.dart' show LruMap;
import 'document_cache.dart';

class LruCache implements DocumentCache {
  final LruMap<String, String> _lru;

  LruCache({int maximumSize}) : _lru = new LruMap(maximumSize: maximumSize);

  Future clear() async {
    _lru.clear();
    return null;
  }

  Future get(String key) async {
    if (!_lru.containsKey(key)) {
      return null;
    }
    return _lru[key];
  }

  Future<Map<String, String>> getAll(Iterable<String> keys) async {
    var result = {};
    for (var key in keys) {
      if (!_lru.containsKey(key)) {
        return null;
      }
      result[key] = _lru[key];
    }
    return result;
  }

  Future remove(String key) async => _lru.remove(key);

  Future removeAll(Iterable<String> keys) async {
    for (var key in keys) {
      _lru.remove(key);
    }
  }

  Future set(String key, String value,
      {SetAction action: SetAction.SET}) async {
    switch (action) {
      case SetAction.ADD:
        if (!_lru.containsKey(key)) {
          _lru[key] = value;
        }
        break;
      case SetAction.REPLACE:
        if (_lru.containsKey(key)) {
          _lru[key] = value;
        }
        break;
      case SetAction.SET:
        _lru[key] = value;
        break;
    }
  }

  Future setAll(Map<String, String> keysAndValues,
      {SetAction action: SetAction.SET}) async {
    for (var key in keysAndValues.keys) {
      var value = keysAndValues[key];
      switch (action) {
        case SetAction.ADD:
          if (!_lru.containsKey(key)) {
            _lru[key] = value;
          }
          break;
        case SetAction.REPLACE:
          if (_lru.containsKey(key)) {
            _lru[key] = value;
          }
          break;
        case SetAction.SET:
          _lru[key] = value;
          break;
      }
    }
  }

  toString() => _lru.toString();
}
