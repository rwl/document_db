import 'package:test/test.dart';
import 'package:document_db/document_db.dart';
import 'package:document_db/indexed_db.dart';
import 'document_db_test.dart';

main() async {
  var name = 'test${rint()}';
  group('indexeddb', () => dbTest(open, name));
  group('indexeddb lru', dbTest(open, name, () => new LruCache()));
}
