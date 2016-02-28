import 'package:document_db/mongo_db.dart';
import 'document_db_test.dart';

main() async {
  dbTest(open, 'mongodb://localhost:27017/test${rint()}');
}
