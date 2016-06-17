library document_db.test.document_cache;

import 'package:test/test.dart';
import 'package:document_db/document_db.dart';

documentCacheTest(DocumentCache mkCache()) {
  group('DocumentCache', () {
    String key, value1;
    DocumentCache cache;
    setUp(() {
      cache = mkCache();
      key = 'abc123';
      value1 = 'Alice';
    });
    test('set', () {
      expect(cache.set(key, value1), completes);
    });
    test('get', () async {
      await cache.set(key, value1);
      var got = await cache.get(key);
      expect(got, equals(value1));
    });
    test('remove', () async {
      await cache.set(key, value1);
      await cache.remove(key);
      expect(await cache.get(key), isNull);
    });
    test('clear', () async {
      await cache.set(key, value1);
      await cache.clear();
      expect(await cache.get(key), isNull);
    });
  });
}

main() => documentCacheTest(() => new LruCache());
