library pb.db.document_db;

import 'dart:async';

typedef Future<DocumentDb> OpenDb(String uri,
    [List<Store> schema, int version]);

class Store {
  final String name;
  final List<Index> indexes;

  Store(this.name, [List<Index> indexes]) : indexes = indexes ?? [];
}

class Index {
  final String name;
  final bool unique;
  final String field;

  Index(String field, {String name, this.unique: false})
      : this.field = field,
        this.name = name ?? field;
}

abstract class DocumentDb<K> {
  Future close();

  Future deleteAndClose();

  String get name;

  Future<List<String>> storeNames();

  DocumentStore<K> store(String name);
}

abstract class DocumentStore<K> {
  static const KEY = '_id';

  Future<K> add(Map doc);

  Future<K> put(Map doc);

  Future delete(K key);

  Future<Map> get(K key);

  Stream<Entry<K>> addAll(Iterable<Map> docs);

  Stream<Entry<K>> putAll(Iterable<Map> docs);

  Future deleteAll(Iterable<K> keys);

  Stream<Entry<K>> getAll(Iterable<K> keys);

  Future clear();

  Future<Map> find(String name, value);

  Stream<Map> findAll(String name, value, {int offset, int limit});

  Future<K> findKey(String name, value);

  Stream<K> findAllKeys(String name, value, {int offset, int limit});

  Future<int> count([String name, value]);
}

class Entry<K> {
  final K key;
  final Map doc;
  Entry(this.key, this.doc);
}
