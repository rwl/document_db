library document_store.indexed_db;

import 'dart:html';
import 'dart:indexed_db';
import 'dart:async';

import 'package:bson/bson.dart' show ObjectId;

import 'document_db.dart' as ddb;

Future<ddb.DocumentDb> open(String uri,
    [List<ddb.Store> schema, int version]) async {
  Database _db;

  if (schema == null) {
    _db = await window.indexedDB.open(uri);
  } else {
    var stores = <ObjectStore>[];
    _db = await window.indexedDB.open(uri, version: version ?? 1,
        onUpgradeNeeded: (VersionChangeEvent event) {
      var request = event.target as Request;
      Database db = request.result;

      var names = schema.map((ss) => ss.name).toSet();
      for (var name in db.objectStoreNames) {
        if (!names.contains(name)) {
          db.deleteObjectStore(name);
        }
      }

      for (var spec in schema) {
        ObjectStore store;
        if (db.objectStoreNames.contains(spec.name)) {
          store = request.transaction.objectStore(spec.name);
        } else {
          store =
              db.createObjectStore(spec.name, keyPath: ddb.DocumentStore.KEY);
          stores.add(store);
        }

        spec.indexes.forEach((index) {
          if (!store.indexNames.contains(index.name)) {
            store.createIndex(index.name, index.field, unique: index.unique);
          }
        });

        var indexNames = spec.indexes.map((idx) => idx.name).toSet();
        for (var indexName in store.indexNames) {
          if (!indexNames.contains(indexName)) {
            store.deleteIndex(indexName);
          }
        }
      }
    });
//    return Future
//        .wait(stores.map((store) => store.transaction.onComplete.first))
//        .then((_) => _db);
  }
  return new IndexedDocumentDb._(_db);
}

class IndexedDocumentDb<K> implements ddb.DocumentDb<K> {
  final Database _db;

  IndexedDocumentDb._(this._db);

  Future close() async {
    _db.close();
  }

  Future deleteAndClose() {
    _db.close();
    return window.indexedDB.deleteDatabase(_db.name);
  }

  String get name => _db.name;

  Future<List<String>> storeNames() async => _db.objectStoreNames;

  ddb.DocumentStore<K> store(String name) {
    return new IndexedDbDocumentStore<K>._(_db, name);
  }
}

class IndexedDbDocumentStore<K> implements ddb.DocumentStore<K> {
  final Database _db;
  final String _name;

  IndexedDbDocumentStore._(this._db, this._name);

  static dynamic _generateKey() => new ObjectId().toHexString();

  Future<K> add(Map doc) {
    var transaction = _db.transaction(_name, 'readwrite');
    var store = transaction.objectStore(_name);
    if (doc[ddb.DocumentStore.KEY] == null) {
      doc = new Map.from(doc);
      doc[ddb.DocumentStore.KEY] = _generateKey();
    }
    return store.add(doc);
  }

  Future<K> put(Map doc) {
    var transaction = _db.transaction(_name, 'readwrite');
    var store = transaction.objectStore(_name);
    if (doc[ddb.DocumentStore.KEY] == null) {
      doc = new Map.from(doc);
      doc[ddb.DocumentStore.KEY] = _generateKey();
    }
    return store.put(doc);
  }

  Future delete(K key) {
    var transaction = _db.transaction(_name, 'readwrite');
    var store = transaction.objectStore(_name);
    return store.delete(key);
  }

  Future<Map> get(K key) async {
    var transaction = _db.transaction(_name, 'readwrite'); // FIXME: 'readonly'
    var store = transaction.objectStore(_name);
    return store.getObject(key);
  }

  Stream<ddb.Entry<K>> addAll(Iterable<Map> docs) {
    var ctrl = new StreamController<ddb.Entry<K>>();
    var transaction = _db.transaction(_name, 'readwrite');
    transaction.onComplete.listen((_) {
      return ctrl.close();
    });
    var store = transaction.objectStore(_name);
    for (var doc in docs) {
      if (doc[ddb.DocumentStore.KEY] == null) {
        doc = new Map.from(doc);
        doc[ddb.DocumentStore.KEY] = _generateKey();
      }
      store.add(doc).then((key) {
        ctrl.add(new ddb.Entry<K>(key, doc));
      });
    }
    return ctrl.stream;
  }

  Stream<ddb.Entry<K>> putAll(Iterable<Map> docs) {
    var ctrl = new StreamController<ddb.Entry<K>>();
    var transaction = _db.transaction(_name, 'readwrite');
    transaction.onComplete.listen((_) {
      return ctrl.close();
    });
    var store = transaction.objectStore(_name);
    for (var doc in docs) {
      if (doc[ddb.DocumentStore.KEY] == null) {
        doc = new Map.from(doc);
        doc[ddb.DocumentStore.KEY] = _generateKey();
      }
      store.put(doc).then((putted) {
        ctrl.add(new ddb.Entry<K>(putted, doc));
      });
    }
    return ctrl.stream;
  }

  Future deleteAll(Iterable keys) async {
    var transaction = _db.transaction(_name, 'readwrite');
    var store = transaction.objectStore(_name);
    for (var key in keys) {
      await store.delete(key);
    }
  }

  Stream<ddb.Entry<K>> getAll(Iterable keys) {
    var ctrl = new StreamController<ddb.Entry<K>>();
    var transaction = _db.transaction(_name, 'readwrite'); // FIXME: 'readonly'
    transaction.onComplete.listen((_) {
      return ctrl.close();
    });
    var store = transaction.objectStore(_name);
    for (var key in keys) {
      store.getObject(key).then((got) {
        ctrl.add(new ddb.Entry<K>(key, got));
      });
    }
    return ctrl.stream;
  }

  Future clear() {
    var transaction = _db.transaction(_name, 'readwrite');
    var store = transaction.objectStore(_name);
    return store.clear();
  }

  Future<Map> find(String name, value) => _find(name, value, false);

  Future<K> findKey(String name, value) => _find(name, value, true);

  Future _find(String name, value, bool wantKey) async {
    var transaction = _db.transaction(_name, 'readwrite'); // FIXME: 'readonly'
    var store = transaction.objectStore(_name);

    if (name == ddb.DocumentStore.KEY) {
      return wantKey ? value : store.getObject(value);
    }

    var index = _keyPaths(store)[name];
    if (index != null) {
      return await (wantKey ? index.getKey(value) : index.get(value));
    }

    window.console.warn('ObjectStore "${_name}" has no index for "$name"');

    var completer = new Completer();
    store.openCursor().listen((cursor) {
      var doc = cursor.value;
      if (doc is Map && doc[name] == value) {
        completer.complete(wantKey ? doc[ddb.DocumentStore.KEY] : doc);
      } else {
        cursor.next();
      }
    });
    return completer.future;
  }

  Stream<Map> findAll(String name, value, {int offset, int limit}) =>
      _findAll(name, value, offset, limit, false);

  Stream<K> findAllKeys(String name, value, {int offset, int limit}) =>
      _findAll(name, value, offset, limit, true);

  Stream _findAll(String name, value, int offset, int limit, bool justKey) {
    var ctrl = new StreamController();

    var transaction = _db.transaction(_name, 'readwrite'); // FIXME: 'readonly'
    var store = transaction.objectStore(_name);

    int counter = 0;

    if (name == ddb.DocumentStore.KEY) {
      if (justKey) {
        ctrl.add(value);
        ctrl.close();
        return ctrl.stream;
      } else {
        store.openCursor(key: value).listen((cursor) {
          if (offset != null && counter < offset) {
            counter = offset;
            cursor.advance(offset);
            return;
          }
          var doc = cursor.value;
          if (limit == null || counter < (offset ?? 0) + limit) {
            ctrl.add(doc);
          }
          counter++;
          cursor.next(); // FIXME: not calling next should close the stream
        }, onDone: () {
          return ctrl.close();
        });
      }
    } else {
      var index = _keyPaths(store)[name];
      if (index != null) {
        Stream<Cursor> cursors;
        if (justKey) {
          cursors = index.openKeyCursor(key: value);
        } else {
          cursors = index.openCursor(key: value);
        }
        cursors.listen((cursor) {
          if (offset != null && counter < offset) {
            counter = offset;
            cursor.advance(offset);
            return;
          }
          if (limit == null || counter < (offset ?? 0) + limit) {
            ctrl.add(
                cursor is CursorWithValue ? cursor.value : cursor.primaryKey);
          }
          counter++;
          cursor.next(); // FIXME: not calling next should close the stream
        }, onDone: () {
          return ctrl.close();
        });
        // openKeyCursor
      } else {
        window.console
            .warn('ObjectStore "${store.name}" has no index for "$name"');

        store.openCursor().listen((cursor) {
          if (offset != null && counter < offset) {
            counter = offset;
            cursor.advance(offset);
            return;
          }
          var doc = cursor.value;
          if (doc is Map && doc[name] == value) {
            if (limit == null || counter < (offset ?? 0) + limit) {
              ctrl.add(justKey ? doc[ddb.DocumentStore.KEY] : doc);
            }
          }
          counter++;
          cursor.next(); // FIXME: not calling next should close the stream
        }, onDone: () {
          return ctrl.close();
        });
      }
    }

    return ctrl.stream;
  }

  Future<int> count([String name, value]) {
    var transaction = _db.transaction(_name, 'readwrite'); // FIXME: 'readonly'
    var store = transaction.objectStore(_name);

    if (name == null || name == ddb.DocumentStore.KEY) {
      return store.count(value);
    }

    var index = _keyPaths(store)[name];
    if (index != null) {
      return index.count(value);
    }

    window.console.warn('ObjectStore "${_name}" has no index for "$name"');

    var completer = new Completer<int>();
    int counter = 0;
    store.openCursor(autoAdvance: true).listen((cursor) {
      var doc = cursor.value;
      if (doc is Map) {
        if (value == null) {
          if (doc.containsKey(name)) {
            counter++;
          }
        } else if (doc[name] == value) {
          counter++;
        }
      }
    }, onDone: () {
      completer.complete(counter);
    });
    return completer.future;
  }

  /// Returns a map of `keyPath` to [Index] for all indexes in [store].
  static Map<String, Index> _keyPaths(ObjectStore store) {
    var indexes = <String, Index>{};
    for (var indexName in store.indexNames) {
      var idx = store.index(indexName);
      var keyPath = idx.keyPath;
      if (keyPath is String) {
        indexes[keyPath] = idx;
      }
    }
    return indexes;
  }
}
