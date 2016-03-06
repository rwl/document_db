library pb.db.elastic_search;

import 'dart:async';
import 'package:elastic_dart/browser_client.dart';
import 'document_db.dart';

Future<DocumentDb> open(
    [String uri = 'http://127.0.0.1:9200',
    List<Store> schema,
    int version]) async {
  var es = new Elasticsearch(uri);
  return new ElasticSearchDb(es);
}

class ElasticSearchDb<K> implements DocumentDb<K> {
  final Elasticsearch es;

  ElasticSearchDb(this.es);

  Future close() {}

  Future deleteAndClose() {}

  String get name => 'Elastic';

  Future<List<String>> storeNames() {
    es.getIndex('_all');
  }

  DocumentStore<K> store(String name) => new ElasticSearchIndex(es, name);
}

class ElasticSearchIndex<K> implements DocumentStore<K> {
  final Elasticsearch es;
  final String _name;

  ElasticSearchIndex(this.es, this._name);

  Future<K> add(Map doc) {
    es.bulk([
      {
        "create": {
          "_index": _name,
          "_type": _name,
          "_id": doc[DocumentStore.KEY]
        }
      },
      doc
    ]);
  }

  Future<K> put(Map doc) {
    es.bulk([
      {
        "index": {
          "_index": _name,
          "_type": _name,
          "_id": doc[DocumentStore.KEY]
        }
      },
      doc
    ]);
  }

  Future delete(K key) {
    es.bulk([
      {
        "delete": {"_index": _name, "_type": _name, "_id": key}
      }
    ]);
  }

  Future<Map> get(K key) {}

  Stream<K> addAll(Iterable<Map> docs) {
    var mapList = [];
    for (var doc in docs) {
      mapList.add({
        "create": {
          "_index": _name,
          "_type": _name,
          "_id": doc[DocumentStore.KEY]
        }
      });
      mapList.add(doc);
    }
    es.bulk(mapList);
  }

  Stream<K> putAll(Iterable<Map> docs) {
    var mapList = [];
    for (var doc in docs) {
      mapList.add({
        "index": {
          "_index": _name,
          "_type": _name,
          "_id": doc[DocumentStore.KEY]
        }
      });
      mapList.add(doc);
    }
    es.bulk(mapList);
  }

  Future deleteAll(Iterable<K> keys) {
    var mapList = [];
    for (var key in keys) {
      mapList.add({
        "delete": {"_index": _name, "_type": _name, "_id": key}
      });
    }
    es.bulk(mapList);
  }

  Stream<Map> getAll(Iterable<K> keys) {}

  Future clear() {
    /*es.deleteByQuery({
      "query": {"match_all": {}}
    });*/
  }

  Future<Map> find(String name, value) {
    es.search(index: _name, query: {
      "query": {
        "match": {name: value}
      },
      "size": 1
    });
  }

  Stream<Map> findAll(String name, value, {int offset, int limit}) {
    es.search(index: _name, query: {
      "query": {
        "match": {name: value}
      },
      "from": offset,
      "size": limit
    });
  }

  Future<K> findKey(String name, value) {
    es.search(index: _name, query: {
      "query": {
        "match": {name: value}
      },
      "size": 1,
      "fields": []
    });
  }

  Stream<K> findAllKeys(String name, value, {int offset, int limit}) {
    es.search(index: _name, query: {
      "query": {
        "match": {name: value}
      },
      "from": offset,
      "size": limit,
      "fields": []
    });
  }

  Future<int> count([String name, value]) {
    /*es.count(index: _name, query: {
      "query": {
        "match": {name: value}
      },
    });*/
  }
}
