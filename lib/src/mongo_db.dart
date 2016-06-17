library document_store.mongo_db;

import 'dart:async';
import 'package:mongo_dart/mongo_dart.dart';
import 'document_db.dart' as ddb;

const String _INTERNAL = '_internal';

Future<ddb.DocumentDb> open(String uri,
    [List<ddb.Store> schema, int version]) async {
  var db = new Db(uri);
  await db.open();

  if (schema != null && await _upgradeNeeded(db, version ?? 1)) {
    var names = schema.map((ss) => ss.name).toSet();
    var collectionNames = await db.getCollectionNames();
    for (var name in collectionNames) {
      if (!names.contains(name)) {
        db.dropCollection(name);
      }
    }

    for (var spec in schema) {
      var coll = await _createCollection(db, spec.name);

      spec.indexes.forEach((index) {
        db.createIndex(spec.name,
            name: index.name, unique: index.unique, key: index.field);
      });

      var indexNames = spec.indexes.map((idx) => idx.name).toSet();
      for (var index in await coll.getIndexes()) {
        if (!indexNames.contains(index['name'])) {
          //coll.dropIndex(index['name']); FIXME: dropIndex
        }
      }
    }
  }
  await _saveVersion(db, version ?? 1);

  return new MongoDocumentDb._(db);
}

Future<bool> _upgradeNeeded(Db db, int version) async {
  var coll = db.collection(_INTERNAL);
  var config = await coll.findOne();
  bool needed = true;
  if (config != null) {
    int currentVerson = config['version'] ?? 1;
    needed = version > currentVerson;
  }
  return needed;
}

_saveVersion(Db db, int version) async {
  var coll = db.collection(_INTERNAL);
  Map config = await coll.findOne();
  if (config != null) {
    int currentVerson = config['version'] ?? 1;
    if (version > currentVerson) {
      config['version'] = version;
    }
  } else {
    config = {'version': version};
  }
  await coll.save(config);
}

Future<DbCollection> _createCollection(Db db, String name) async {
  var coll = db.collection(name);

  if (!(await db.getCollectionNames()).contains(name)) {
    // A DbCollection is created lazily. This will force creation of an
    // empty collection, ensuring it will be listed by `storeNames`.
    var id = new ObjectId();
    await coll.insert({ddb.DocumentStore.KEY: id});
    await coll.remove(where.id(id));
  }
  return coll;
}

class MongoDocumentDb<K> implements ddb.DocumentDb<K> {
  final Db _db;

  MongoDocumentDb._(this._db);

  Future close() => _db.close();

  Future deleteAndClose() async {
    await _db.drop();
    return _db.close();
  }

  String get name => _db.databaseName;

  Future<List<String>> storeNames() async {
    return (await _db.getCollectionNames()).map((String name) {
      // MongoDb 2.4 returns `databaseName.colletionName`
      if (name.startsWith(_db.databaseName)) {
        name = name.substring(_db.databaseName.length + 1);
      }
      return name;
    }).toList();
  }

  ddb.DocumentStore<K> store(String name) =>
      new MongoDbDocumentStore._(_db.collection(name));
}

class MongoDbDocumentStore<K> implements ddb.DocumentStore<K> {
  final DbCollection _collecton;

  MongoDbDocumentStore._(this._collecton);

  static dynamic _generateKey() => new ObjectId();

  Future<K> add(Map doc) async {
    if (doc[ddb.DocumentStore.KEY] == null) {
      doc = new Map.from(doc);
      doc[ddb.DocumentStore.KEY] = _generateKey();
    }
    await _collecton.insert(doc);
    return doc[ddb.DocumentStore.KEY];
  }

  Future<K> put(Map doc) async {
    if (doc[ddb.DocumentStore.KEY] == null) {
      doc = new Map.from(doc);
      doc[ddb.DocumentStore.KEY] = _generateKey();
    }
    await _collecton.save(doc);
    return doc[ddb.DocumentStore.KEY];
  }

  Future delete(K key) async {
    return _collecton
        .remove(where.eq(ddb.DocumentStore.KEY, key))
        .then((_) => null);
  }

  Future<Map> get(K key) =>
      _collecton.findOne(where.eq(ddb.DocumentStore.KEY, key));

  Stream<ddb.Entry<K>> addAll(Iterable<Map> docs) {
    var ctrl = new StreamController<ddb.Entry<K>>();
    docs = docs.map((doc) {
      if (doc[ddb.DocumentStore.KEY] == null) {
        doc = new Map.from(doc);
        doc[ddb.DocumentStore.KEY] = _generateKey();
      }
      return doc;
    });
    _collecton.insertAll(docs.toList()).then((_) {
      for (var doc in docs) {
        ctrl.add(new ddb.Entry<K>(doc.remove(ddb.DocumentStore.KEY), doc));
      }
    }).then((_) {
      return ctrl.close();
    });
    return ctrl.stream;
  }

  Stream<ddb.Entry<K>> putAll(Iterable<Map> docs) {
    var ctrl = new StreamController<ddb.Entry<K>>();
    Future.wait(docs.map((doc) {
      if (doc[ddb.DocumentStore.KEY] == null) {
        doc = new Map.from(doc);
        doc[ddb.DocumentStore.KEY] = _generateKey();
      }
      return _collecton.save(doc).then((_) {
        ctrl.add(new ddb.Entry<K>(doc.remove(ddb.DocumentStore.KEY), doc));
      });
    })).then((_) {
      return ctrl.close();
    });
    return ctrl.stream;
  }

  Future deleteAll(Iterable keys) {
    return _collecton
        .remove(where.oneFrom(ddb.DocumentStore.KEY, keys.toList()))
        .then((_) => null);
  }

  Stream<ddb.Entry<K>> getAll(Iterable keys) {
    return _collecton
        .find(where.oneFrom(ddb.DocumentStore.KEY, keys.toList()))
        .map((doc) => new ddb.Entry<K>(doc.remove(ddb.DocumentStore.KEY), doc));
  }

  Future clear() => _collecton.remove({}).then((_) => null);

  Future<Map> find(String name, value) =>
      _collecton.findOne(where.eq(name, value));

  Stream<Map> findAll(String name, value, {int offset, int limit}) {
    SelectorBuilder s = where.eq(name, value);
    if (offset != null) {
      s.skip(offset);
    }
    if (limit != null) {
      s.limit(limit);
    }
    return _collecton.find(s);
  }

  Future<K> findKey(String name, value) => _collecton
      .findOne(where.eq(name, value).returnKey())
      .then((d) => d[ddb.DocumentStore.KEY]);

  Stream<K> findAllKeys(String name, value, {int offset, int limit}) {
    SelectorBuilder s = where.eq(name, value);
    if (offset != null) {
      s.skip(offset);
    }
    if (limit != null) {
      s.limit(limit);
    }
    return _collecton.find(s.returnKey()).map((d) => d[ddb.DocumentStore.KEY]);
  }

  Future<int> count([String name, value]) {
    if (name == null) {
      return _collecton.count();
    }
    if (value == null) {
      return _collecton.count(where.exists(name));
    }
    return _collecton.count(where.eq(name, value));
  }
}
