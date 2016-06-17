library document_db.cached_db;

import 'dart:async';
import 'dart:convert' show JSON;

import '../document_db.dart';
import 'document_cache.dart';

class CachedDocumentStore<K> implements DocumentStore<K> {
  final DocumentStore<K> _store;
  final DocumentCache _cache;

  CachedDocumentStore(this._store, this._cache);

  static String _encode(value) {
    if (value == null) {
      return null;
    }
    return JSON.encode(value);
  }

  Future<K> add(Map doc) async {
    var key = _store.add(doc);
    await _cache.set(_encode(key), _encode(doc));
    return key;
  }

  Future<K> put(Map doc) async {
    var key = _store.put(doc);
    await _cache.set(_encode(key), _encode(doc));
    return key;
  }

  Future delete(K key) async {
    var result = await _store.delete(key);
    await _cache.remove(_encode(key));
    return result;
  }

  Future<Map> get(K key) async {
    var value = await _cache.get(_encode(key));
    if (value != null) {
      return value;
    }
    return await _store.get(key);
  }

  Stream<Entry<K>> addAll(Iterable<Map> docs) {
    var stream = _store.addAll(docs);
    stream.toList().then((entries) {
      var kv = new Map.fromIterable(entries,
          key: (Entry<K> entry) => entry.key,
          value: (Entry<K> entry) => entry.doc);
      return _cache.setAll(kv);
    });
    return stream;
  }

  Stream<Entry<K>> putAll(Iterable<Map> docs) {
    var stream = _store.putAll(docs);
    stream.toList().then((entries) {
      var kv = new Map.fromIterable(entries,
          key: (Entry<K> entry) => entry.key,
          value: (Entry<K> entry) => entry.doc);
      return _cache.setAll(kv);
    });
    return stream;
  }

  Future deleteAll(Iterable<K> keys) async {
    var result = await _store.deleteAll(keys);
    await _cache.removeAll(keys.map(_encode));
    return result;
  }

  Stream<Entry<K>> getAll(Iterable<K> keys) {}

  Future clear() async {
    var result = await _store.clear();
    await _cache.clear();
    return result;
  }

  Future<Map> find(String name, value) => _store.find(name, value);

  Stream<Map> findAll(String name, value, {int offset, int limit}) {
    return _store.findAll(name, value, offset: offset, limit: limit);
  }

  Future<K> findKey(String name, value) => _store.findKey(name, value);

  Stream<K> findAllKeys(String name, value, {int offset, int limit}) {
    return _store.findAllKeys(name, value, offset: offset, limit: limit);
  }

  Future<int> count([String name, value]) => _store.count(name, value);
}
