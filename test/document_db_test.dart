library document_db.test.document_db;

import 'dart:math';
import 'package:test/test.dart';
import 'package:document_db/document_db.dart';

int rint() => new Random().nextInt(9999);

dbTest(OpenDb open, String uri) {
  group('db', () {
    test('open', () async {
      var spec = new Store('store1');
      var db = await open(uri, [spec]);
      expect(await db.storeNames(), contains(spec.name));

      await db.deleteAndClose();

//      var db1 = await open('db test');
//      expect(await db.storeNames(), contains(spec.name));
//
//      await db1.close();
//
//      var spec2 = new StoreSpec('store2');
//      var db2 = await open('db test', [spec2], 2);
//      expect(await db2.storeNames(), isNot(contains(spec.name)));
//
//      await db2.close();
    });
  });

  group('store', () {
    Map doc;
    List<Map> docs;
    DocumentDb db;
    DocumentStore store;

    setUp(() async {
      doc = {'name': 'Alice', 'age': 21};
      docs = [
        {'title': 'Blog A', 'posted': new DateTime(2011, 01, 01)},
        {'title': 'Blog A', 'posted': new DateTime(2011, 01, 02)},
        {'title': 'Blog B', 'posted': new DateTime(2011, 01, 02)},
        {'title': 'Blog C', 'posted': new DateTime(2011, 01, 03)},
        {
          'title': 'Blog D',
          'posted': new DateTime(2011, 01, 04),
          'promoted': true
        },
        {'posted': new DateTime(2011, 01, 05)},
        {
          'title': 'Blog F',
          'posted': new DateTime(2011, 01, 06),
          'promoted': true
        }
      ];
      var spec = new Store('store3', [new Index('title'), new Index('name')]);
      db = await open(uri, [spec]);
      store = db.store(spec.name);
    });

    tearDown(() {
      db.deleteAndClose();
    });

    test('add', () async {
      var key = await store.add(doc);
      expect(doc, isNot(contains(DocumentStore.KEY)));
      expect(key, isNotNull);
    });

    test('add with key', () async {
      doc[DocumentStore.KEY] = 'foo';
      var key = await store.add(doc);
      expect(key, equals('foo'));
    });

    test('get', () async {
      var key = await store.add(doc);
      var got = await store.get(key);
      expect(got..remove(DocumentStore.KEY), equals(doc));
    });

    test('get missing', () async {
      var got = await store.get('bar'); // TODO: onError?
      expect(got, isNull);
    });

    test('put without key', () async {
      var key = await store.put(doc);
      expect(doc, isNot(contains(DocumentStore.KEY)));
      expect(key, isNotNull);
    });

    test('put with key', () async {
      doc[DocumentStore.KEY] = 'foo';
      var key = await store.put(doc);
      expect(key, equals('foo'));
    });

    test('put', () async {
      doc[DocumentStore.KEY] = 'foo';
      await store.put(doc);
      doc['age'] = 25;
      await store.put(doc);
      var got = await store.get('foo');
      expect(got['age'], equals(25));
    });

    test('delete', () async {
      var key = await store.add(doc);
      expect(await store.delete(key), isNull);
      var got = await store.get(key);
      expect(got, isNull);
    });

    test('delete missing', () async {
      expect(await store.delete('baz'), isNull);
    });

    test('addAll', () async {
      var results = await store.addAll(docs);
      var keys = await results.toList();
      expect(keys, hasLength(equals(docs.length)));
      expect(keys.every((key) => key != null), isTrue);
    });

    test('addAll with key', () async {
      docs.asMap().forEach((i, doc) {
        doc[DocumentStore.KEY] = i + 1;
      });
      var results = await store.addAll(docs);
      var keys = await results.toList();
      expect(keys, hasLength(equals(docs.length)));
      keys.asMap().forEach((i, key) {
        expect(key, equals(i + 1));
      });
    });

    test('getAll', () async {
      var results = await store.addAll(docs);
      var keys = await results.toList();
      results = await store.getAll(keys.sublist(3));
      var got = await results.toList();
      expect(got.map((g) => g..remove(DocumentStore.KEY)),
          equals(docs.sublist(3)));
    });

    test('getAll missing', () async {
      var results = await store.getAll(['abc', 'def']);
      var got = await results.toList();
      expect(got.every((g) => g == null), isTrue);
    });

    test('putAll', () async {
      docs.asMap().forEach((i, doc) {
        doc[DocumentStore.KEY] = i * 10;
      });
      await store.putAll(docs);

      var dur = const Duration(days: 7);
      for (var doc in docs) {
        doc['posted'] = doc['posted'].add(dur);
      }

      var results = await store.putAll(docs);
      var putted = await results.toList();

      var got = await store.get(putted[2]);
      expect(got, equals(docs[2]));
    });

    test('deleteAll', () async {
      var results = await store.addAll(docs);
      var keys = await results.toList();

      var deleted = await store.deleteAll(keys);
      expect(deleted, isNull);

      results = await store.getAll(keys);
      var got = await results.toList();
      expect(got.every((g) => g == null), isTrue);
    });

    test('clear', () async {
      var key = await store.add(doc);
      var cleared = await store.clear();
      expect(cleared, isNull);
      expect(await store.get(key), isNull);
    });

    test('find', () async {
      await store.addAll(docs);
      var found = await store.find('title', 'Blog C');
      expect(found['posted'], equals(new DateTime(2011, 01, 03)));
    });

    test('find no index', () async {
      await store.addAll(docs);
      var found = await store.find('posted', new DateTime(2011, 01, 03));
      expect(found['title'], equals('Blog C'));
    });

    test('findAll', () async {
      await store.addAll(docs);
      var result = await store.findAll('title', 'Blog A');
      var found = await result.toList();
      expect(found, hasLength(2));
    });

    test('findAll no index', () async {
      await store.addAll(docs);
      var result = await store.findAll('posted', new DateTime(2011, 01, 02));
      var found = await result.toList();
      expect(found, hasLength(2));
    });

    test('findAll offset', () async {
      await store.addAll(docs);
      var result = await store.findAll('title', 'Blog A', offset: 1);
      var found = await result.toList();
      expect(found, hasLength(1));
      expect(found[0]['posted'], equals(new DateTime(2011, 01, 02)));
    });

    test('findAll limit', () async {
      await store.addAll(docs);
      var result = await store.findAll('title', 'Blog A', limit: 1);
      var found = await result.toList();
      expect(found, hasLength(1));
      expect(found[0]['posted'], equals(new DateTime(2011, 01, 01)));
    });

    group('find keys', () {
      setUp(() {
        docs.asMap().forEach((i, doc) {
          doc[DocumentStore.KEY] = i;
        });
      });
      test('findKey', () async {
        await store.addAll(docs);
        var found = await store.findKey('title', 'Blog C');
        expect(found, equals(3));
      });

      test('findKey no index', () async {
        await store.addAll(docs);
        var found = await store.findKey('posted', new DateTime(2011, 01, 03));
        expect(found, equals(3));
      });

      test('findAllKeys', () async {
        await store.addAll(docs);
        var result = await store.findAllKeys('title', 'Blog A');
        var found = await result.toList();
        expect(found, [0, 1]);
      });

      test('findAllKeys no index', () async {
        await store.addAll(docs);
        var result =
            await store.findAllKeys('posted', new DateTime(2011, 01, 02));
        var found = await result.toList();
        expect(found, [1, 2]);
      });

      test('findAllKeys offset', () async {
        await store.addAll(docs);
        var result = await store.findAllKeys('title', 'Blog A', offset: 1);
        var found = await result.toList();
        expect(found, equals([1]));
      });

      test('findAllKeys limit', () async {
        await store.addAll(docs);
        var result = await store.findAllKeys('title', 'Blog A', limit: 1);
        var found = await result.toList();
        expect(found, equals([0]));
      });
    });

    test('count', () async {
      await store.addAll(docs);
      var count = await store.count();
      expect(count, equals(docs.length));
    });

    test('count exists', () async {
      await store.addAll(docs);
      var count = await store.count('promoted');
      expect(count, equals(2));
    });

    test('count indexed exists', () async {
      await store.addAll(docs);
      var count = await store.count('title');
      expect(count, equals(6));
    });

    test('count indexed', () async {
      await store.addAll(docs);
      var count = await store.count('title', 'Blog A');
      expect(count, equals(2));
    });

    test('count no index', () async {
      await store.addAll(docs);
      var count = await store.count('posted', new DateTime(2011, 01, 02));
      expect(count, equals(2));
    });
  });
}
