library document_db.memcache;

import 'dart:async';

import 'package:memcache/memcache.dart' as memcache;
import 'package:memcache/memcache_raw.dart';
import 'package:memcache/src/memcache_impl.dart';
import 'document_cache.dart';

const _actions = const {
  SetAction.ADD: memcache.SetAction.ADD,
  SetAction.REPLACE: memcache.SetAction.REPLACE,
  SetAction.SET: memcache.SetAction.SET
};

class DocumentMemcache implements DocumentCache {
  final memcache.Memcache _memcache;

  DocumentMemcache(RawMemcache raw) : _memcache = new MemCacheImpl(raw);

  Future clear() => _memcache.clear();

  Future<String> get(String key) => _memcache.get(key, asBinary: false);

  Future<Map<String, String>> getAll(Iterable<String> keys) =>
      _memcache.getAll(keys, asBinary: false);

  Future remove(String key) => _memcache.remove(key);

  Future removeAll(Iterable<String> keys) => _memcache.removeAll(keys);

  Future set(String key, String value, {SetAction action: SetAction.SET}) {
    return _memcache.set(key, value, action: _actions[action]);
  }

  Future setAll(Map keysAndValues, {SetAction action: SetAction.SET}) {
    return _memcache.setAll(keysAndValues, action: _actions[action]);
  }
}
