import 'package:test/test.dart';
import 'package:document_db/document_db.dart';
import 'package:document_db/mongo_db.dart';
import 'document_db_test.dart';

main() async {
  var uri = 'mongodb://localhost:27017/test${rint()}';
  group('mongodb', () => dbTest(open, uri));
  group('mongodb lru', () => dbTest(open, uri, () => new LruCache()));
}
